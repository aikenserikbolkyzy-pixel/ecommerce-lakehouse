"""
Phase 2 / Job 4 - Gold layer.

local.ecommerce.silver_events ->
  gold_funnel_metrics       view -> cart -> purchase funnel per main_category x brand,
                            per main_category and in total (session-based + event counts)
  gold_rfm_segments         one row per buying customer: Recency, Frequency, Monetary,
                            R/F/M scores 1-5 and a named segment
  gold_rfm_segment_summary  customers / revenue per segment (+ browsers without purchase)
  gold_rfm_rf_grid          customers per (R score, F score) - for a heatmap
  gold_hourly_trends        events, revenue, conversion and active users per day x hour

All heavy work is distributed Spark (groupBy shuffles, percentile_approx, HyperLogLog).
Only the small aggregated tables are collected and written to ./results/gold_*.csv.
The per-customer RFM table (~hundreds of thousands of rows) stays in Iceberg only.
"""
from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField,
                               StructType)

import common as C


def flag(t):
    return (F.col("event_type") == t).cast("long")


def rate(num, den):
    return F.round(F.try_divide(F.col(num).cast("double"), F.col(den).cast("double")), 4)


# --------------------------------------------------------------------------- funnel
FUNNEL_SUMS = ["views", "carts", "purchases", "revenue"]
FUNNEL_FLAGS = ["s_view", "s_cart", "s_purchase"]


def funnel_level(df, keys):
    return (df.groupBy(*keys)
            .agg(*[F.sum(c).alias(c) for c in FUNNEL_SUMS],
                 F.sum("s_view").alias("view_sessions"),
                 F.sum("s_cart").alias("cart_sessions"),
                 F.sum("s_purchase").alias("purchase_sessions")))


def build_funnel(silver):
    # 1 row per (category, brand, session): what happened in that session for that brand
    per_session = (silver
                   .groupBy("main_category", "brand", "user_session")
                   .agg(F.sum(flag("view")).alias("views"),
                        F.sum(flag("cart")).alias("carts"),
                        F.sum(flag("purchase")).alias("purchases"),
                        F.sum(F.when(F.col("event_type") == "purchase", F.col("price"))
                              .otherwise(F.lit(0.0))).alias("revenue"),
                        F.max(flag("view")).alias("s_view"),
                        F.max(flag("cart")).alias("s_cart"),
                        F.max(flag("purchase")).alias("s_purchase"))
                   .persist(StorageLevel.MEMORY_AND_DISK))

    def collapse(keys):
        # re-aggregate to a coarser session grain so a session is counted once per level
        return (per_session.groupBy(*keys, "user_session")
                .agg(*[F.sum(c).alias(c) for c in FUNNEL_SUMS],
                     *[F.max(c).alias(c) for c in FUNNEL_FLAGS]))

    brand_level = (funnel_level(per_session, ["main_category", "brand"])
                   .withColumn("level", F.lit("category_brand")))
    cat_level = (funnel_level(collapse(["main_category"]), ["main_category"])
                 .withColumn("brand", F.lit("ALL")).withColumn("level", F.lit("category")))
    total_level = (funnel_level(collapse([]), [])
                   .withColumn("main_category", F.lit("ALL")).withColumn("brand", F.lit("ALL"))
                   .withColumn("level", F.lit("total")))

    funnel = (brand_level.unionByName(cat_level).unionByName(total_level)
              .withColumn("revenue", F.round("revenue", 2))
              .withColumn("view_to_cart_rate", rate("cart_sessions", "view_sessions"))
              .withColumn("cart_to_purchase_rate", rate("purchase_sessions", "cart_sessions"))
              .withColumn("view_to_purchase_rate", rate("purchase_sessions", "view_sessions"))
              .withColumn("event_cart_per_view", rate("carts", "views"))
              .withColumn("event_purchase_per_view", rate("purchases", "views")))
    cols = ["level", "main_category", "brand", "views", "carts", "purchases", "revenue",
            "view_sessions", "cart_sessions", "purchase_sessions",
            "view_to_cart_rate", "cart_to_purchase_rate", "view_to_purchase_rate",
            "event_cart_per_view", "event_purchase_per_view"]
    return funnel.select(*cols), per_session


# --------------------------------------------------------------------------- RFM
def score_up(col, cuts):
    """1..5, higher value -> higher score; cuts = 20/40/60/80 % quantiles."""
    c = F.col(col)
    return (F.when(c <= cuts[0], 1).when(c <= cuts[1], 2).when(c <= cuts[2], 3)
            .when(c <= cuts[3], 4).otherwise(5))


def segment_expr():
    r, f, m = F.col("r_score"), F.col("f_score"), F.col("m_score")
    return (F.when((r >= 4) & (f >= 4), "Champions")
            .when((r >= 3) & (f >= 3), "Loyal Customers")
            .when((r <= 2) & (f >= 4), "Can't Lose Them")
            .when((r <= 2) & ((f >= 2) | (m >= 4)), "At Risk")
            .when((r >= 4) & (m >= 4), "Promising Big Spenders")
            .when(r >= 4, "Recent Customers")
            .when(r == 3, "Need Attention")
            .otherwise("Hibernating"))


def build_rfm(spark, silver, run):
    ref_date = silver.agg(F.date_add(F.to_date(F.max("event_time")), 1).alias("d")).first()["d"]
    run.put("rfm_reference_date", str(ref_date))

    customers = (silver.filter(F.col("event_type") == "purchase")
                 .groupBy("user_id")
                 .agg(F.max("event_time").alias("last_purchase_ts"),
                      F.count_distinct("user_session").alias("frequency"),
                      F.count(F.lit(1)).alias("purchase_events"),
                      F.round(F.sum("price"), 2).alias("monetary"))
                 .withColumn("recency_days",
                             F.datediff(F.lit(ref_date), F.to_date("last_purchase_ts")))
                 .persist(StorageLevel.MEMORY_AND_DISK))

    q = customers.agg(
        F.percentile_approx("recency_days", [0.2, 0.4, 0.6, 0.8], 10000).alias("r"),
        F.percentile_approx("monetary", [0.2, 0.4, 0.6, 0.8], 10000).alias("m"),
    ).first()
    r_cuts, m_cuts = list(q["r"]), list(q["m"])
    run.put("rfm_quantile_cuts", {"recency_days": r_cuts, "monetary": m_cuts,
                                  "frequency": "fixed bins 1 | 2 | 3 | 4-5 | 6+"})
    C.log(f"  RFM cuts: recency {r_cuts}, monetary {m_cuts}")

    fr = F.col("frequency")
    rfm = (customers
           .withColumn("r_score", 6 - score_up("recency_days", r_cuts))   # recent = 5
           .withColumn("f_score", F.when(fr >= 6, 5).when(fr >= 4, 4).when(fr == 3, 3)
                       .when(fr == 2, 2).otherwise(1))
           .withColumn("m_score", score_up("monetary", m_cuts))
           .withColumn("rfm_cell", F.concat_ws("", *[F.col(c).cast("string")
                                                      for c in ("r_score", "f_score", "m_score")]))
           .withColumn("rfm_total", F.col("r_score") + F.col("f_score") + F.col("m_score"))
           .withColumn("segment", segment_expr())
           .select("user_id", "last_purchase_ts", "recency_days", "frequency", "purchase_events",
                   "monetary", "r_score", "f_score", "m_score", "rfm_cell", "rfm_total", "segment"))
    return rfm, customers


SUMMARY_SCHEMA = StructType([
    StructField("segment", StringType()),
    StructField("customers", LongType()),
    StructField("share_of_users", DoubleType()),
    StructField("share_of_buyers", DoubleType()),
    StructField("revenue", DoubleType()),
    StructField("share_of_revenue", DoubleType()),
    StructField("avg_recency_days", DoubleType()),
    StructField("avg_frequency", DoubleType()),
    StructField("avg_monetary", DoubleType()),
])


def build_rfm_summary(spark, silver, rfm):
    seg = (spark.table(C.GOLD_RFM).groupBy("segment")
           .agg(F.count(F.lit(1)).alias("customers"),
                F.sum("monetary").alias("revenue"),
                F.avg("recency_days").alias("avg_recency_days"),
                F.avg("frequency").alias("avg_frequency"),
                F.avg("monetary").alias("avg_monetary"))
           .collect())
    all_users = int(silver.agg(F.count_distinct("user_id").alias("n")).first()["n"])
    buyers = sum(int(r["customers"]) for r in seg)
    revenue = sum(float(r["revenue"]) for r in seg) or 1.0

    rows = [(r["segment"], int(r["customers"]),
             round(int(r["customers"]) / all_users, 4),
             round(int(r["customers"]) / buyers, 4),
             round(float(r["revenue"]), 2),
             round(float(r["revenue"]) / revenue, 4),
             round(float(r["avg_recency_days"]), 2),
             round(float(r["avg_frequency"]), 3),
             round(float(r["avg_monetary"]), 2)) for r in seg]
    rows.sort(key=lambda x: -x[4])
    rows.append(("Browsers (no purchase)", all_users - buyers,
                 round((all_users - buyers) / all_users, 4), None, 0.0, 0.0, None, None, None))
    return spark.createDataFrame(rows, SUMMARY_SCHEMA), all_users, buyers


# --------------------------------------------------------------------------- hourly
def build_hourly(silver):
    return (silver.groupBy("event_date", "event_hour")
            .agg(F.sum(flag("view")).alias("views"),
                 F.sum(flag("cart")).alias("carts"),
                 F.sum(flag("purchase")).alias("purchases"),
                 F.round(F.sum(F.when(F.col("event_type") == "purchase", F.col("price"))
                               .otherwise(F.lit(0.0))), 2).alias("revenue"),
                 F.approx_count_distinct("user_id", 0.02).alias("active_users_approx"),
                 F.approx_count_distinct("user_session", 0.02).alias("sessions_approx"))
            .withColumn("day_of_week", F.date_format("event_date", "E"))
            .withColumn("cart_per_view", rate("carts", "views"))
            .withColumn("purchase_per_view", rate("purchases", "views"))
            .select("event_date", "day_of_week", "event_hour", "views", "carts", "purchases",
                    "revenue", "active_users_approx", "sessions_approx",
                    "cart_per_view", "purchase_per_view"))


# --------------------------------------------------------------------------- main
def main():
    spark = C.get_spark("phase2-04-gold-aggregations")
    run = C.Run(spark, "04_gold_aggregations")
    C.banner(f"GOLD  {C.SILVER}  ->  gold_* tables + {C.RESULTS_DIR}/gold_*.csv")
    cols = ["event_time", "event_date", "event_hour", "event_type", "main_category", "brand",
            "price", "user_id", "user_session"]
    silver = spark.table(C.SILVER).select(*cols)
    exports, snaps = {}, {}

    with run.step("gold_funnel_metrics"):
        funnel, per_session = build_funnel(silver)
        snaps["gold_funnel_metrics"] = C.replace_table(
            spark, funnel, C.GOLD_FUNNEL, comment="Gold: view->cart->purchase funnel")
        per_session.unpersist()
    spark.sql(f"""SELECT main_category, views, view_sessions, cart_sessions, purchase_sessions,
                         view_to_cart_rate, cart_to_purchase_rate, view_to_purchase_rate, revenue
                  FROM {C.GOLD_FUNNEL} WHERE level IN ('category','total')
                  ORDER BY revenue DESC""").show(20, truncate=False)

    with run.step("gold_rfm_segments"):
        rfm, customers = build_rfm(spark, silver, run)
        snaps["gold_rfm_segments"] = C.replace_table(
            spark, rfm, C.GOLD_RFM, comment="Gold: RFM scores and segment per buying customer")
        customers.unpersist()

    with run.step("gold_rfm_segment_summary + rf grid"):
        summary, all_users, buyers = build_rfm_summary(spark, silver, rfm)
        snaps["gold_rfm_segment_summary"] = C.replace_table(
            spark, summary, C.GOLD_RFM_SUMMARY, comment="Gold: customers and revenue per RFM segment")
        grid = (spark.table(C.GOLD_RFM).groupBy("r_score", "f_score")
                .agg(F.count(F.lit(1)).alias("customers"),
                     F.round(F.sum("monetary"), 2).alias("revenue"),
                     F.round(F.avg("monetary"), 2).alias("avg_monetary")))
        snaps["gold_rfm_rf_grid"] = C.replace_table(
            spark, grid, C.GOLD_RFM_GRID, comment="Gold: customers per R and F score")
    run.put("users_total", all_users)
    run.put("buyers_total", buyers)
    spark.table(C.GOLD_RFM_SUMMARY).show(truncate=False)

    with run.step("gold_hourly_trends"):
        snaps["gold_hourly_trends"] = C.replace_table(
            spark, build_hourly(silver), C.GOLD_HOURLY,
            comment="Gold: activity, revenue and conversion per day and hour (UTC)")
    run.put("snapshots", snaps)

    with run.step("export small gold tables to CSV"):
        exports["gold_funnel_metrics"] = C.export_csv(
            spark.table(C.GOLD_FUNNEL), "gold_funnel_metrics",
            order_by=[F.col("level").desc(), F.col("revenue").desc()])
        exports["gold_rfm_segments_summary"] = C.export_csv(
            spark.table(C.GOLD_RFM_SUMMARY), "gold_rfm_segments_summary",
            order_by=[F.col("revenue").desc()])
        exports["gold_rfm_rf_grid"] = C.export_csv(
            spark.table(C.GOLD_RFM_GRID), "gold_rfm_rf_grid", order_by=["r_score", "f_score"])
        exports["gold_hourly_trends"] = C.export_csv(
            spark.table(C.GOLD_HOURLY), "gold_hourly_trends", order_by=["event_date", "event_hour"])
    run.put("exports", exports)

    total = spark.table(C.GOLD_FUNNEL).filter("level = 'total'").first()
    if total:
        C.log(f"  overall: {int(total['view_sessions']):,} sessions with a view, "
              f"{int(total['purchase_sessions']):,} with a purchase "
              f"(view->purchase {total['view_to_purchase_rate']}), revenue {total['revenue']:,}")
    C.log(f"  users {all_users:,}, buyers {buyers:,} ({buyers / all_users:.2%})")
    run.save()
    spark.stop()


if __name__ == "__main__":
    main()
