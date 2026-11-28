"""
Phase 3 / Job 6 - Product recommender with Spark MLlib ALS (implicit feedback).

1. Interaction strength per (user, product): view = 1, cart = 3, purchase = 5, summed
   over all events (distributed groupBy over the Silver table), stored in Iceberg
   (local.ecommerce.ml_user_item_interactions). ALS confidence uses log1p(score).
2. Pruning for a laptop cluster: products with >= --min-item-users users, users with
   between --min-user-items and --max-user-items products (very long histories are
   usually bots). --sample keeps a deterministic fraction of users (hash of user_id).
3. 80/20 random split of the (user, product) pairs; ALS(implicitPrefs=True) on train.
4. recommendForAllUsers(K) -> products the user already had in TRAIN are removed ->
   Top-10 per user. Stored in Iceberg local.ecommerce.gold_user_recommendations with
   product category / brand / price.
5. Evaluation on the held-out 20 %:
   Precision@10, Recall@10, NDCG@10 (RankingEvaluator) against
     - all held-out interactions, and
     - only held-out carts / purchases ("intent"),
   compared with a most-popular baseline (same filtering).
   RMSE of the predicted preference on held-out pairs (target preference = 1).
   For implicit ALS the ranking metrics are the meaningful ones; RMSE is a sanity check.

Outputs:
  ./results/als_metrics.json
  ./results/als_sample_recommendations.csv   10 users x 10 recommendations (small)
  hdfs:///models/als_implicit                saved ALSModel
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "phase2"))

from pyspark import StorageLevel  # noqa: E402
from pyspark.ml.evaluation import RankingEvaluator, RegressionEvaluator  # noqa: E402
from pyspark.ml.recommendation import ALS, ALSModel  # noqa: E402
from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

import common as C  # noqa: E402

WEIGHTS = {"view": 1.0, "cart": 3.0, "purchase": 5.0}
MODEL_DIR = "hdfs://master:9000/models"
CHECKPOINT_DIR = "hdfs://master:9000/tmp/als-checkpoints"


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=float, default=0.2,
                    help="fraction of users (hash of user_id); 1.0 = all users")
    ap.add_argument("--tag", default="", help="suffix for tables / model / results (e.g. _dev)")
    ap.add_argument("--rank", type=int, default=20)
    ap.add_argument("--max-iter", type=int, default=10)
    ap.add_argument("--reg", type=float, default=0.1)
    ap.add_argument("--alpha", type=float, default=10.0)
    ap.add_argument("--min-item-users", type=int, default=20)
    ap.add_argument("--min-user-items", type=int, default=3)
    ap.add_argument("--max-user-items", type=int, default=500)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--candidates", type=int, default=50,
                    help="N candidates per user; already-seen products are removed, then top k kept")
    ap.add_argument("--eval-users", type=int, default=20000,
                    help="recommend for this many evaluation users (recommendForUserSubset); "
                         "0 = every user (recommendForAllUsers, very slow on laptops)")
    ap.add_argument("--reuse", action="store_true",
                    help="reuse the interactions table and the saved ALS model from a previous run")
    return ap.parse_args()


# --------------------------------------------------------------------------- data
def build_interactions(spark, a):
    s = spark.table(C.SILVER).select("user_id", "product_id", "event_type")
    if a.sample < 1.0:
        s = s.filter(F.pmod(F.xxhash64("user_id"), F.lit(1000)) < int(a.sample * 1000))
    w = (F.when(F.col("event_type") == "view", WEIGHTS["view"])
         .when(F.col("event_type") == "cart", WEIGHTS["cart"])
         .when(F.col("event_type") == "purchase", WEIGHTS["purchase"])
         .otherwise(0.0))
    pairs = (s.groupBy("user_id", "product_id")
             .agg(F.sum(w).alias("score"),
                  F.count(F.lit(1)).alias("events"),
                  F.max((F.col("event_type") == "cart").cast("int")).alias("carted"),
                  F.max((F.col("event_type") == "purchase").cast("int")).alias("purchased")))

    items = (pairs.groupBy("product_id").agg(F.count(F.lit(1)).alias("item_users"))
             .filter(F.col("item_users") >= a.min_item_users).select("product_id"))
    pairs = pairs.join(items, "product_id", "left_semi")
    users = (pairs.groupBy("user_id").agg(F.count(F.lit(1)).alias("user_items"))
             .filter(F.col("user_items").between(a.min_user_items, a.max_user_items))
             .select("user_id"))
    pairs = pairs.join(users, "user_id", "left_semi")

    return pairs.select(
        F.col("user_id").cast("int").alias("user"),          # ALS needs int ids (all ids < 2^31)
        F.col("product_id").cast("int").alias("item"),
        "score", "events", "carted", "purchased",
        F.log1p("score").cast("float").alias("rating"),
        F.when(F.rand(seed=42) < 0.8, F.lit("train")).otherwise(F.lit("test")).alias("split"),
    )


def product_dim(spark):
    return (spark.table(C.SILVER)
            .groupBy("product_id")
            .agg(F.first("main_category", ignorenulls=True).alias("main_category"),
                 F.first("brand", ignorenulls=True).alias("brand"),
                 F.round(F.avg("price"), 2).alias("avg_price"))
            .select(F.col("product_id").cast("int").alias("item"), "main_category", "brand", "avg_price"))


def table_exists(spark, name):
    try:
        spark.table(name).limit(0).collect()
        return True
    except Exception:
        return False


def model_exists(path):
    try:
        ALSModel.load(path)
        return True
    except Exception:
        return False


# --------------------------------------------------------------------------- ranking helpers
def top_k_unseen(cands, train_pairs, k):
    """cands(user, item, score) -> top-k per user after removing items seen in train."""
    unseen = cands.join(train_pairs.select("user", "item"), ["user", "item"], "left_anti")
    w = Window.partitionBy("user").orderBy(F.desc("score"), F.asc("item"))
    return unseen.withColumn("rank", F.row_number().over(w)).filter(F.col("rank") <= k)


def as_lists(topk, test_rel):
    """Ordered prediction list + relevant list per user, as array<double> for RankingEvaluator."""
    pred = (topk.groupBy("user")
            .agg(F.sort_array(F.collect_list(F.struct("rank", "item"))).alias("ri"))
            .select("user", F.transform("ri", lambda x: x["item"].cast("double")).alias("prediction")))
    rel = (test_rel.groupBy("user")
           .agg(F.collect_list(F.col("item").cast("double")).alias("label")))
    return pred.join(rel, "user")


def ranking_metrics(df, k):
    df = df.persist(StorageLevel.MEMORY_AND_DISK)
    ev = RankingEvaluator(predictionCol="prediction", labelCol="label", k=k)
    out = {
        f"precision_at_{k}": round(ev.setMetricName("precisionAtK").evaluate(df), 5),
        f"recall_at_{k}": round(ev.setMetricName("recallAtK").evaluate(df), 5),
        f"ndcg_at_{k}": round(ev.setMetricName("ndcgAtK").evaluate(df), 5),
        "users_evaluated": df.count(),
    }
    df.unpersist()
    return out


# --------------------------------------------------------------------------- main
def main():
    a = parse_args()
    spark = C.get_spark("phase3-06-ml-recommender-als")
    spark.sparkContext.setCheckpointDir(CHECKPOINT_DIR)
    run = C.Run(spark, f"06_ml_recommender_als{a.tag}")
    run.put("params", vars(a))
    inter_table = C.tbl(f"ml_user_item_interactions{a.tag}")
    recs_table = C.tbl(f"gold_user_recommendations{a.tag}")
    C.banner(f"ALS RECOMMENDER  {C.SILVER} -> {inter_table} -> ALS -> {recs_table}")

    model_path = f"{MODEL_DIR}/als_implicit{a.tag}"
    reuse = a.reuse and table_exists(spark, inter_table) and model_exists(model_path)
    if a.reuse and not reuse:
        C.log("  --reuse requested but table or model missing -> full run")
    if reuse:
        C.log(f"  reusing {inter_table} and model {model_path}")
        snap = C.snapshot_info(spark, inter_table)
    else:
        with run.step("build user-product interactions -> Iceberg"):
            snap = C.replace_table(spark, build_interactions(spark, a), inter_table,
                                   comment="Implicit feedback: view=1, cart=3, purchase=5 per user and product")
    run.put("interactions_table", {"name": inter_table, **snap})

    pairs = spark.table(inter_table)
    train = pairs.filter(F.col("split") == "train").persist(StorageLevel.MEMORY_AND_DISK)
    test = pairs.filter(F.col("split") == "test")
    with run.step("dataset statistics"):
        st = pairs.groupBy("split").agg(F.count(F.lit(1)).alias("pairs"),
                                        F.count_distinct("user").alias("users"),
                                        F.count_distinct("item").alias("items")).collect()
        ds = {r["split"]: {"pairs": int(r["pairs"]), "users": int(r["users"]), "items": int(r["items"])}
              for r in st}
        tr = ds.get("train", {})
        ds["train_density"] = round(tr.get("pairs", 0) / max(tr.get("users", 1) * tr.get("items", 1), 1), 8)
    run.put("dataset", ds)
    C.log(f"  {ds}")

    als = ALS(userCol="user", itemCol="item", ratingCol="rating",
              implicitPrefs=True, rank=a.rank, maxIter=a.max_iter, regParam=a.reg, alpha=a.alpha,
              coldStartStrategy="drop", nonnegative=False, seed=42,
              numUserBlocks=10, numItemBlocks=10, checkpointInterval=5)
    if reuse:
        model = ALSModel.load(model_path)
    else:
        with run.step(f"train ALS (rank {a.rank}, {a.max_iter} iterations)"):
            model = als.fit(train)
        with run.step("save model to HDFS"):           # saved right away: later steps can --reuse it
            model.write().overwrite().save(model_path)

    with run.step("RMSE on held-out pairs (predicted preference vs 1.0)"):
        scored = model.transform(test.withColumn("preference", F.lit(1.0)))
        rmse = RegressionEvaluator(metricName="rmse", labelCol="preference",
                                   predictionCol="prediction").evaluate(scored)

    # users that appear in both train and test can be evaluated
    both = test.select("user").distinct().join(train.select("user").distinct(), "user")
    if a.eval_users > 0:
        rec_users = both.orderBy(F.xxhash64("user")).limit(a.eval_users).persist(StorageLevel.MEMORY_AND_DISK)
        api = f"recommendForUserSubset({a.eval_users:,} users, {a.candidates})"
    else:
        rec_users = both.persist(StorageLevel.MEMORY_AND_DISK)
        api = f"recommendForAllUsers({a.candidates})"
    n_rec_scope = rec_users.count()
    with run.step(f"{api} -> top {a.k} unseen"):
        raw = (model.recommendForUserSubset(rec_users, a.candidates) if a.eval_users > 0
               else model.recommendForAllUsers(a.candidates))
        cands = (raw
                 .select("user", F.posexplode("recommendations").alias("pos", "r"))
                 .select("user", F.col("r.item").alias("item"), F.col("r.rating").alias("score")))
        topk = top_k_unseen(cands, train, a.k).persist(StorageLevel.MEMORY_AND_DISK)
        n_rec_users = topk.select("user").distinct().count()

    with run.step("ranking metrics vs most-popular baseline"):
        test_all = test.select("user", "item")
        test_intent = test.filter((F.col("carted") == 1) | (F.col("purchased") == 1)).select("user", "item")

        als_all = ranking_metrics(as_lists(topk, test_all), a.k)
        als_intent = ranking_metrics(as_lists(topk, test_intent), a.k)

        popular = (train.groupBy("item").agg(F.count(F.lit(1)).alias("n"))
                   .orderBy(F.desc("n")).limit(a.candidates)
                   .withColumn("score", F.col("n").cast("double")).select("item", "score"))
        pop_topk = top_k_unseen(rec_users.crossJoin(F.broadcast(popular)), train, a.k)
        pop_all = ranking_metrics(as_lists(pop_topk, test_all), a.k)
        pop_intent = ranking_metrics(as_lists(pop_topk, test_intent), a.k)

    with run.step("write gold_user_recommendations"):
        recs = (topk.join(product_dim(spark), "item", "left")
                .select(F.col("user").cast("long").alias("user_id"), "rank",
                        F.col("item").cast("long").alias("product_id"),
                        F.round("score", 5).alias("score"), "main_category", "brand", "avg_price",
                        F.current_timestamp().alias("generated_at")))
        rsnap = C.replace_table(spark, recs, recs_table,
                                comment=f"Top-{a.k} ALS recommendations per user (products not seen in training)")
    run.put("recommendations_table", {"name": recs_table, **rsnap})

    with run.step("export sample"):
        sample_users = (spark.table(recs_table).select("user_id").distinct()
                        .orderBy(F.xxhash64("user_id")).limit(10))
        sample = spark.table(recs_table).join(sample_users, "user_id").drop("generated_at")
        exp = C.export_csv(sample, f"als_sample_recommendations{a.tag}", order_by=["user_id", "rank"])

    metrics = {
        "model": "ALS implicitPrefs=True",
        "interaction_weights": WEIGHTS,
        "rating": "log1p(sum of weights) per (user, product)",
        "params": {"rank": a.rank, "maxIter": a.max_iter, "regParam": a.reg, "alpha": a.alpha,
                   "user_sample_fraction": a.sample, "min_item_users": a.min_item_users,
                   "user_items_range": [a.min_user_items, a.max_user_items]},
        "dataset": ds,
        "rmse_preference_heldout": round(rmse, 5),
        "rmse_note": "implicit ALS predicts preference (0..1), so RMSE is measured against 1.0 on "
                     "held-out interactions; ranking metrics below are the main quality measure",
        "users_with_recommendations": n_rec_users,
        "recommendation_api": api,
        "evaluation_users_scope": (f"deterministic sample of {n_rec_scope:,} users present in train and test"
                                   if a.eval_users > 0 else f"all {n_rec_scope:,} users in train and test"),
        "model_reused": reuse,
        "evaluation": f"top-{a.k} recommendations excluding products seen in train, vs held-out 20 %",
        "als": {"all_interactions": als_all, "carts_and_purchases": als_intent},
        "popularity_baseline": {"all_interactions": pop_all, "carts_and_purchases": pop_intent},
        "model_path": model_path,
        "sample_csv": exp["path"],
    }
    k = a.k
    metrics[f"precision_at_{k}"] = als_all[f"precision_at_{k}"]
    metrics[f"recall_at_{k}"] = als_all[f"recall_at_{k}"]
    metrics["lift_precision_vs_popularity"] = (
        round(als_all[f"precision_at_{k}"] / pop_all[f"precision_at_{k}"], 2)
        if pop_all[f"precision_at_{k}"] else None)

    os.makedirs(C.RESULTS_DIR, exist_ok=True)
    mpath = os.path.join(C.RESULTS_DIR, f"als_metrics{a.tag}.json")
    with open(mpath, "w") as f:
        json.dump(metrics, f, indent=2, default=str)
    C.log(f"  wrote {mpath}")

    C.banner("RESULTS")
    print(f"  pairs train/test: {ds.get('train', {}).get('pairs')} / {ds.get('test', {}).get('pairs')}")
    print(f"  RMSE (preference, held-out): {metrics['rmse_preference_heldout']}")
    for name, m in (("ALS        ", als_all), ("Popularity ", pop_all)):
        print(f"  {name} all interactions : P@{k} {m[f'precision_at_{k}']}  R@{k} {m[f'recall_at_{k}']}  "
              f"NDCG@{k} {m[f'ndcg_at_{k}']}  users {m['users_evaluated']:,}")
    for name, m in (("ALS        ", als_intent), ("Popularity ", pop_intent)):
        print(f"  {name} carts/purchases  : P@{k} {m[f'precision_at_{k}']}  R@{k} {m[f'recall_at_{k}']}  "
              f"NDCG@{k} {m[f'ndcg_at_{k}']}  users {m['users_evaluated']:,}")
    print(f"  lift of ALS over popularity (P@{k}): {metrics['lift_precision_vs_popularity']}")
    spark.table(recs_table).filter(F.col("rank") <= 5).orderBy("user_id", "rank").show(10, truncate=False)

    run.put("metrics", {kk: metrics[kk] for kk in (f"precision_at_{k}", f"recall_at_{k}",
                                                    "rmse_preference_heldout", "lift_precision_vs_popularity")})
    run.save()
    topk.unpersist()
    train.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
