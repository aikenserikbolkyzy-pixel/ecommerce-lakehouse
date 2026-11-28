"""
Phase 3 / Job 5 - Purchase prediction with Spark MLlib (binary classification).

Question: when a customer puts a product into the cart, will that product be
bought in the same session?

Unit of prediction : the FIRST cart event of a (user, session, product)
Label              : 1 if the same product is purchased later in the same session, else 0
Features (no look-ahead, only information available at cart time):
  price (log), price / average price of its category, hour of day, day of week,
  weekend flag, events / views / carts earlier in the session, views of this product
  earlier in the session, seconds since session start, user's history from previous
  sessions (sessions, views, carts, purchases), main_category and brand
  (StringIndexer + OneHotEncoder), all assembled with VectorAssembler.
Imbalance          : class weights in weightCol (balanced: n / (2 * n_class)).
Split              : 80 / 20 by user (hash of user_id), so one customer's carts never
                     appear in both train and test.
Data quality       : days with carts but zero purchases (2019-11-15 in this dataset)
                     are excluded, otherwise their carts would be labelled 0 wrongly.

Distributed design: one shuffle co-locates every session (user_id, user_session);
all session- and product-level window features and the label are computed there.
User history is a second, much smaller shuffle over ~23 M session rows.
The feature table is stored in Iceberg (local.ecommerce.ml_cart_features), then
the Spark ML pipeline trains on it.

Outputs:
  ./results/logreg_metrics.json            ROC-AUC, PR-AUC, accuracy, confusion matrix, ...
  ./results/logreg_coefficients.csv        model coefficients (feature importance)
  ./results/logreg_curve_points.csv        ROC / PR curve points for plotting
  hdfs:///models/logreg_cart_purchase      saved PipelineModel

Usage: spark-submit ... 05_ml_purchase_prediction.py [--sample 0.05] [--tag _dev]
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "phase2"))

from pyspark.ml import Pipeline  # noqa: E402
from pyspark.ml.classification import LogisticRegression  # noqa: E402
from pyspark.ml.evaluation import BinaryClassificationEvaluator  # noqa: E402
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler  # noqa: E402
from pyspark.ml.functions import vector_to_array  # noqa: E402
from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

import common as C  # noqa: E402

NUMERIC_FEATURES = [
    "log_price", "price_rel_category", "event_hour", "day_of_week", "is_weekend",
    "sess_events_before", "sess_views_before", "sess_carts_before", "product_views_before",
    "log_sec_since_session_start",
    "log_hist_sessions", "log_hist_views", "log_hist_carts", "log_hist_purchases",
    "has_purchase_history",
]
TOP_BRANDS = 300          # rarer brands are grouped as "other"
MODEL_DIR = "hdfs://master:9000/models"


# --------------------------------------------------------------------------- features
def zero_purchase_days(spark):
    """Days that have carts but no purchases at all (missing purchase data)."""
    try:
        src = spark.table(C.GOLD_HOURLY)
    except Exception:
        src = (spark.table(C.SILVER).groupBy("event_date")
               .agg(F.sum((F.col("event_type") == "cart").cast("long")).alias("carts"),
                    F.sum((F.col("event_type") == "purchase").cast("long")).alias("purchases")))
    rows = (src.groupBy("event_date")
            .agg(F.sum("carts").alias("c"), F.sum("purchases").alias("p"))
            .filter((F.col("c") > 0) & (F.col("p") == 0))
            .orderBy("event_date").collect())
    return [r["event_date"] for r in rows]


def user_sample(df, fraction):
    if fraction >= 1.0:
        return df
    return df.filter(F.pmod(F.xxhash64("user_id"), F.lit(1000)) < int(fraction * 1000))


def build_features(spark, sample, bad_days):
    s = (spark.table(C.SILVER)
         .select("event_time", "event_date", "event_hour", "event_type", "product_id",
                 "category_id", "main_category", "brand", "price", "user_id", "user_session",
                 "event_seq", "session_start"))
    s = user_sample(s, sample)
    for t in ("view", "cart", "purchase"):
        s = s.withColumn(f"is_{t}", (F.col("event_type") == t).cast("int"))

    # ONE shuffle: every session on one task; all windows below reuse this partitioning
    s = s.repartition("user_id", "user_session")

    sess = Window.partitionBy("user_id", "user_session").orderBy("event_seq")
    sess_before = sess.rowsBetween(Window.unboundedPreceding, -1)
    prod = Window.partitionBy("user_id", "user_session", "product_id").orderBy("event_seq")
    prod_before = prod.rowsBetween(Window.unboundedPreceding, -1)
    prod_upto = prod.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    prod_after = prod.rowsBetween(Window.currentRow, Window.unboundedFollowing)

    ev = (s
          .withColumn("sess_views_before", F.coalesce(F.sum("is_view").over(sess_before), F.lit(0)))
          .withColumn("sess_carts_before", F.coalesce(F.sum("is_cart").over(sess_before), F.lit(0)))
          .withColumn("product_views_before", F.coalesce(F.sum("is_view").over(prod_before), F.lit(0)))
          .withColumn("cart_no", F.sum("is_cart").over(prod_upto))
          # label: is this product purchased at or after this event, in this session?
          .withColumn("label", F.max("is_purchase").over(prod_after)))
    carts = ev.filter((F.col("is_cart") == 1) & (F.col("cart_no") == 1))

    # user history from PREVIOUS sessions only (no information from the future)
    sess_agg = (s.groupBy("user_id", "user_session")
                .agg(F.min("event_time").alias("s_start"),
                     F.sum("is_view").alias("s_views"),
                     F.sum("is_cart").alias("s_carts"),
                     F.sum("is_purchase").alias("s_purchases")))
    uw = (Window.partitionBy("user_id").orderBy("s_start", "user_session")
          .rowsBetween(Window.unboundedPreceding, -1))
    hist = sess_agg.select(
        "user_id", "user_session",
        F.count(F.lit(1)).over(uw).alias("hist_sessions"),
        F.coalesce(F.sum("s_views").over(uw), F.lit(0)).alias("hist_views"),
        F.coalesce(F.sum("s_carts").over(uw), F.lit(0)).alias("hist_carts"),
        F.coalesce(F.sum("s_purchases").over(uw), F.lit(0)).alias("hist_purchases"))

    # average price per category (map-side partial aggregation -> tiny shuffle)
    cat_avg = spark.table(C.SILVER).groupBy("category_id").agg(F.avg("price").alias("cat_avg_price"))

    f = (carts.join(hist, ["user_id", "user_session"], "left")
         .join(F.broadcast(cat_avg), "category_id", "left"))
    if bad_days:
        f = f.filter(~F.col("event_date").isin(bad_days))

    def lg(c):
        return F.log1p(F.coalesce(F.col(c), F.lit(0)).cast("double"))

    return f.select(
        "user_id", "user_session", "product_id", "event_time", "event_date",
        "main_category", "brand",
        F.col("label").cast("double").alias("label"),
        F.log1p("price").alias("log_price"),
        F.coalesce(F.try_divide(F.col("price"), F.col("cat_avg_price")), F.lit(1.0)).alias("price_rel_category"),
        F.col("event_hour").cast("double").alias("event_hour"),
        F.dayofweek("event_time").cast("double").alias("day_of_week"),
        F.dayofweek("event_time").isin(1, 7).cast("double").alias("is_weekend"),
        (F.col("event_seq") - 1).cast("double").alias("sess_events_before"),
        F.col("sess_views_before").cast("double").alias("sess_views_before"),
        F.col("sess_carts_before").cast("double").alias("sess_carts_before"),
        F.col("product_views_before").cast("double").alias("product_views_before"),
        F.log1p((F.unix_timestamp("event_time") - F.unix_timestamp("session_start")).cast("double"))
         .alias("log_sec_since_session_start"),
        lg("hist_sessions").alias("log_hist_sessions"),
        lg("hist_views").alias("log_hist_views"),
        lg("hist_carts").alias("log_hist_carts"),
        lg("hist_purchases").alias("log_hist_purchases"),
        (F.coalesce(F.col("hist_purchases"), F.lit(0)) > 0).cast("double").alias("has_purchase_history"),
        # 80/20 split by user, deterministic
        F.when(F.pmod(F.xxhash64("user_id", F.lit(42)), F.lit(10)) < 8, F.lit("train"))
         .otherwise(F.lit("test")).alias("split"),
    )


# --------------------------------------------------------------------------- model
def build_pipeline():
    si_cat = StringIndexer(inputCol="main_category", outputCol="cat_idx", handleInvalid="keep")
    si_brand = StringIndexer(inputCol="brand_grp", outputCol="brand_idx", handleInvalid="keep")
    ohe = OneHotEncoder(inputCols=["cat_idx", "brand_idx"], outputCols=["cat_vec", "brand_vec"],
                        handleInvalid="keep")
    asm = VectorAssembler(inputCols=NUMERIC_FEATURES + ["cat_vec", "brand_vec"], outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="label", weightCol="weight",
                            maxIter=100, regParam=0.01, elasticNetParam=0.0, standardization=True)
    return Pipeline(stages=[si_cat, si_brand, ohe, asm, lr])


def labels_of(indexer_model):
    try:
        return list(indexer_model.labelsArray[0])
    except Exception:
        return list(indexer_model.labels)


def feature_names(model):
    """Names for every coefficient: numeric features, then one-hot slots of main_category and brand.

    One-hot size per column = categorySize (+1 if handleInvalid='keep') (-1 if dropLast);
    with StringIndexer(handleInvalid='keep') the last slot is the 'unseen value' bucket.
    """
    ohe = model.stages[2]
    keep = ohe.getHandleInvalid() == "keep"
    drop_last = ohe.getDropLast()
    names = list(NUMERIC_FEATURES)
    for col, idx_model, size in (("main_category", model.stages[0], ohe.categorySizes[0]),
                                 ("brand", model.stages[1], ohe.categorySizes[1])):
        out = size + (1 if keep else 0) - (1 if drop_last else 0)
        labels = labels_of(idx_model)
        slots = [f"{col}={x}" for x in labels][:out]
        slots += [f"{col}=<unseen>"] * (out - len(slots))
        names += slots
    return names


def coefficient_table(model):
    lr = model.stages[-1]
    coefs = lr.coefficients.toArray().tolist()
    names = feature_names(model)
    if len(names) != len(coefs):
        C.log(f"  WARN: {len(names)} names for {len(coefs)} coefficients -> generic names")
        names = [f"f{i}" for i in range(len(coefs))]
    rows = sorted(zip(names, coefs), key=lambda x: -abs(x[1]))
    return rows, float(lr.intercept)


def curve_points(pred, bins=100):
    """ROC and PR curve points from a histogram of predicted probabilities (one Spark pass)."""
    hist = (pred.withColumn("b", F.least(F.floor(F.col("p1") * bins), F.lit(bins - 1)).cast("int"))
            .groupBy("b").agg(F.sum("label").alias("pos"), F.count(F.lit(1)).alias("n"))
            .collect())
    pos = {r["b"]: float(r["pos"]) for r in hist}
    tot = {r["b"]: float(r["n"]) for r in hist}
    P = sum(pos.values())
    N = sum(tot.values()) - P
    out, tp, fp = [], 0.0, 0.0
    for b in range(bins - 1, -1, -1):           # thresholds from high to low
        tp += pos.get(b, 0.0)
        fp += tot.get(b, 0.0) - pos.get(b, 0.0)
        out.append({"threshold": round(b / bins, 4),
                    "tpr_recall": round(tp / P, 5) if P else None,
                    "fpr": round(fp / N, 5) if N else None,
                    "precision": round(tp / (tp + fp), 5) if tp + fp else None})
    return out


def write_csv(rows, header, name):
    import csv
    os.makedirs(C.RESULTS_DIR, exist_ok=True)
    path = os.path.join(C.RESULTS_DIR, name)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    C.log(f"  wrote {path}")
    return os.path.abspath(path)


# --------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=float, default=1.0, help="fraction of users (dev runs)")
    ap.add_argument("--tag", default="", help="suffix for tables / model (e.g. _dev)")
    args = ap.parse_args()

    spark = C.get_spark("phase3-05-ml-purchase-prediction")
    run = C.Run(spark, f"05_ml_purchase_prediction{args.tag}")
    run.put("sample_fraction", args.sample)
    feat_table = C.tbl(f"ml_cart_features{args.tag}")
    C.banner(f"PURCHASE PREDICTION  {C.SILVER} -> {feat_table} -> LogisticRegression")

    bad_days = zero_purchase_days(spark)
    run.put("excluded_zero_purchase_days", [str(d) for d in bad_days])
    C.log(f"  excluded days with carts but no purchases: {[str(d) for d in bad_days]}")

    with run.step("feature engineering -> Iceberg feature table"):
        feats = build_features(spark, args.sample, bad_days)
        snap = C.replace_table(spark, feats, feat_table,
                               comment="ML features: one row per first cart of a product in a session")
    run.put("feature_table", {"name": feat_table, **snap})

    data = spark.table(feat_table)
    with run.step("dataset statistics + brand grouping + class weights"):
        top = (data.filter(F.col("split") == "train").groupBy("brand").count()
               .orderBy(F.desc("count")).limit(TOP_BRANDS).collect())
        top_brands = [r["brand"] for r in top]
        data = data.withColumn("brand_grp", F.when(F.col("brand").isin(top_brands), F.col("brand"))
                               .otherwise(F.lit("other")))
        stats = {(r["split"], int(r["label"])): int(r["n"]) for r in
                 data.groupBy("split", "label").agg(F.count(F.lit(1)).alias("n")).collect()}
        n_tr_pos, n_tr_neg = stats.get(("train", 1), 0), stats.get(("train", 0), 0)
        n_te_pos, n_te_neg = stats.get(("test", 1), 0), stats.get(("test", 0), 0)
        n_tr = n_tr_pos + n_tr_neg
        w_pos, w_neg = n_tr / (2.0 * n_tr_pos), n_tr / (2.0 * n_tr_neg)
    run.put("dataset", {
        "train_rows": n_tr, "train_positive": n_tr_pos, "train_positive_rate": round(n_tr_pos / n_tr, 4),
        "test_rows": n_te_pos + n_te_neg, "test_positive": n_te_pos,
        "test_positive_rate": round(n_te_pos / (n_te_pos + n_te_neg), 4),
        "class_weight_positive": round(w_pos, 4), "class_weight_negative": round(w_neg, 4),
        "brands_kept": len(top_brands),
    })
    C.log(f"  train {n_tr:,} rows ({n_tr_pos / n_tr:.2%} positive) | test {n_te_pos + n_te_neg:,} rows "
          f"| weights pos {w_pos:.3f} neg {w_neg:.3f}")

    train = (data.filter(F.col("split") == "train")
             .withColumn("weight", F.when(F.col("label") == 1.0, F.lit(w_pos)).otherwise(F.lit(w_neg)))
             .cache())
    test = data.filter(F.col("split") == "test")

    with run.step("train Pipeline (indexers + one-hot + assembler + weighted LogisticRegression)"):
        model = build_pipeline().fit(train)
    lr_model = model.stages[-1]
    try:
        iterations = lr_model.summary.totalIterations
    except Exception:
        iterations = None
    run.put("training", {"iterations": iterations, "num_features": lr_model.numFeatures})
    train.unpersist()

    with run.step("evaluate on test set"):
        pred = (model.transform(test)
                .withColumn("p1", vector_to_array("probability")[1])
                .select("label", "prediction", "rawPrediction", "p1")
                .cache())
        ev = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction")
        roc_auc = ev.setMetricName("areaUnderROC").evaluate(pred)
        pr_auc = ev.setMetricName("areaUnderPR").evaluate(pred)
        cm = {(int(r["label"]), int(r["prediction"])): int(r["n"]) for r in
              pred.groupBy("label", "prediction").agg(F.count(F.lit(1)).alias("n")).collect()}
        tp, fp = cm.get((1, 1), 0), cm.get((0, 1), 0)
        tn, fn = cm.get((0, 0), 0), cm.get((1, 0), 0)
        total = tp + fp + tn + fn
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        curve = curve_points(pred)

    metrics = {
        "model": "LogisticRegression (class-weighted), threshold 0.5",
        "task": "first cart of a product in a session -> purchased in the same session?",
        "roc_auc": round(roc_auc, 4),
        "pr_auc": round(pr_auc, 4),
        "pr_auc_random_baseline": round(n_te_pos / (n_te_pos + n_te_neg), 4),
        "accuracy": round((tp + tn) / total, 4),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "confusion_matrix": {"tp": tp, "fp": fp, "tn": tn, "fn": fn,
                             "layout": "rows = actual (0, 1), cols = predicted (0, 1)",
                             "matrix": [[tn, fp], [fn, tp]]},
        **{k: run.metrics[k] for k in ("dataset", "training", "excluded_zero_purchase_days")},
        "features_numeric": NUMERIC_FEATURES,
        "features_categorical": ["main_category (one-hot)", f"brand (top {TOP_BRANDS} + other, one-hot)"],
        "split": "80/20 by user_id hash (no customer in both sets)",
        "params": {"maxIter": 100, "regParam": 0.01, "elasticNetParam": 0.0},
    }

    coefs, intercept = coefficient_table(model)
    metrics["intercept"] = round(intercept, 4)
    metrics["top_coefficients"] = [{"feature": n, "coef": round(c, 4)} for n, c in coefs[:15]]

    os.makedirs(C.RESULTS_DIR, exist_ok=True)
    mpath = os.path.join(C.RESULTS_DIR, f"logreg_metrics{args.tag}.json")
    with open(mpath, "w") as f:
        json.dump(metrics, f, indent=2, default=str)
    C.log(f"  wrote {mpath}")
    write_csv([(n, round(c, 6)) for n, c in coefs], ["feature", "coefficient"],
              f"logreg_coefficients{args.tag}.csv")
    write_csv([(p["threshold"], p["fpr"], p["tpr_recall"], p["precision"]) for p in curve],
              ["threshold", "fpr", "tpr_recall", "precision"], f"logreg_curve_points{args.tag}.csv")

    with run.step("save model to HDFS"):
        model_path = f"{MODEL_DIR}/logreg_cart_purchase{args.tag}"
        model.write().overwrite().save(model_path)
    run.put("model_path", model_path)

    C.banner("RESULTS")
    print(f"  ROC-AUC  {metrics['roc_auc']}")
    print(f"  PR-AUC   {metrics['pr_auc']}   (random baseline = positive rate {metrics['pr_auc_random_baseline']})")
    print(f"  Accuracy {metrics['accuracy']}   Precision {metrics['precision']}   "
          f"Recall {metrics['recall']}   F1 {metrics['f1']}")
    print("  Confusion matrix (rows actual 0/1, cols predicted 0/1):")
    print(f"      [[{tn:>9,} {fp:>9,}]\n       [{fn:>9,} {tp:>9,}]]")
    print("  Top coefficients:")
    for n, c in coefs[:10]:
        print(f"      {c:+.4f}  {n}")
    run.put("metrics", {k: metrics[k] for k in ("roc_auc", "pr_auc", "accuracy", "precision", "recall", "f1")})
    run.save()
    spark.stop()


if __name__ == "__main__":
    main()
