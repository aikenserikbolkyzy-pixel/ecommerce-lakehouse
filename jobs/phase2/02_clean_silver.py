"""
Phase 2 / Job 2 - Silver layer.

local.ecommerce.bronze_events  ->  local.ecommerce.silver_events  (partitioned by days(event_time))

1. Data-quality profile of Bronze in ONE distributed pass (counts per rule).
2. Filter invalid records: price <= 0 or NULL, user_id NULL, user_session NULL,
   unparseable event_time, unknown event_type.
3. Remove exact duplicate rows (all 9 original columns equal).
4. Impute brand / category_code NULL -> 'unknown'; main_category = first token of
   category_code ("electronics.smartphone" -> "electronics").
5. Sessionization with window functions over (user_id, user_session) ordered by
   event_time: event_seq, seconds since previous event, session start/end/duration,
   events per session, purchases per session, session_has_purchase,
   session_ended_with_purchase.

Distributed design: the data is hash-partitioned ONCE by (user_id, user_session).
That single shuffle satisfies both the de-duplication (its key set contains
user_id, user_session) and the session window, so Spark does not shuffle again
before the window. The plan is saved to the evidence folder.
"""
import os

from pyspark.sql import Window
from pyspark.sql import functions as F

import common as C

VALID_TYPES = ["view", "cart", "remove_from_cart", "purchase"]
SESSION_KEYS = ["user_id", "user_session"]

SILVER_PROPS = {
    "write.distribution-mode": "hash",          # one writer per day partition
    # row-level changes on Silver (demo in job 03) write delete files instead of
    # rewriting whole Parquet files
    "write.delete.mode": "merge-on-read",
    "write.update.mode": "merge-on-read",
    "write.merge.mode": "merge-on-read",
}


def valid_condition():
    return (F.col("event_time").isNotNull()
            & F.col("price").isNotNull() & (F.col("price") > 0)
            & F.col("user_id").isNotNull()
            & F.col("user_session").isNotNull()
            & F.col("event_type").isNotNull()
            & F.col("event_type").isin(VALID_TYPES))


def profile(bronze):
    def cnt(cond):
        return F.sum(F.when(cond, 1).otherwise(0))
    row = bronze.agg(
        F.count(F.lit(1)).alias("bronze_rows"),
        cnt(F.col("event_time").isNull()).alias("null_event_time"),
        cnt(F.col("price").isNull()).alias("null_price"),
        cnt(F.col("price") <= 0).alias("price_le_0"),
        cnt(F.col("user_id").isNull()).alias("null_user_id"),
        cnt(F.col("user_session").isNull()).alias("null_user_session"),
        cnt(~F.col("event_type").isin(VALID_TYPES) | F.col("event_type").isNull()).alias("bad_event_type"),
        cnt(F.col("brand").isNull()).alias("null_brand"),
        cnt(F.col("category_code").isNull()).alias("null_category_code"),
        cnt(~valid_condition()).alias("invalid_rows"),
    ).first()
    return {k: int(v) for k, v in row.asDict().items()}


def unknown_if_blank(col):
    c = F.trim(F.col(col))
    return F.when(F.col(col).isNull() | (c == ""), F.lit("unknown")).otherwise(c)


def build_silver(bronze):
    valid = bronze.filter(valid_condition()).select(*C.DATA_COLS)

    # ONE shuffle: co-locate every session on one task, then de-duplicate there
    dedup = valid.repartition(*SESSION_KEYS).dropDuplicates(C.DATA_COLS)

    cleaned = (dedup
               .withColumn("brand", unknown_if_blank("brand"))
               .withColumn("category_code", unknown_if_blank("category_code"))
               .withColumn("main_category",
                           F.when(F.col("category_code") == "unknown", F.lit("unknown"))
                           .otherwise(F.substring_index("category_code", ".", 1)))
               .withColumn("event_date", F.to_date("event_time"))
               .withColumn("event_hour", F.hour("event_time")))

    # deterministic order for events with the same timestamp
    ev_rank = (F.when(F.col("event_type") == "view", 1)
               .when(F.col("event_type") == "cart", 2)
               .when(F.col("event_type") == "remove_from_cart", 3)
               .otherwise(4))
    w = Window.partitionBy(*SESSION_KEYS).orderBy("event_time", "_ev_rank", "product_id")
    w_all = w.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    is_purchase = (F.col("event_type") == "purchase").cast("int")

    s = (cleaned
         .withColumn("_ev_rank", ev_rank)
         .withColumn("event_seq", F.row_number().over(w))
         .withColumn("_prev_ts", F.lag("event_time").over(w))
         .withColumn("session_start", F.min("event_time").over(w_all))
         .withColumn("session_end", F.max("event_time").over(w_all))
         .withColumn("session_event_count", F.count(F.lit(1)).over(w_all))
         .withColumn("session_purchase_count", F.sum(is_purchase).over(w_all))
         .withColumn("_last_type", F.last("event_type").over(w_all)))

    return s.select(
        "event_time", "event_date", "event_hour", "event_type",
        "product_id", "category_id", "category_code", "main_category", "brand", "price",
        "user_id", "user_session",
        "event_seq",
        (F.unix_timestamp("event_time") - F.unix_timestamp("_prev_ts")).alias("sec_since_prev_event"),
        "session_start", "session_end",
        (F.unix_timestamp("session_end") - F.unix_timestamp("session_start")).alias("session_duration_sec"),
        "session_event_count",
        F.col("session_purchase_count").cast("int").alias("session_purchase_count"),
        (F.col("session_purchase_count") > 0).alias("session_has_purchase"),
        (F.col("_last_type") == "purchase").alias("session_ended_with_purchase"),
    )


def main():
    spark = C.get_spark("phase2-02-clean-silver")
    run = C.Run(spark, "02_clean_silver")
    C.banner(f"SILVER  {C.BRONZE}  ->  {C.SILVER}")
    bronze = spark.table(C.BRONZE)

    with run.step("data-quality profile of bronze (1 pass)"):
        dq = profile(bronze)
    run.put("data_quality", dq)
    for k, v in dq.items():
        print(f"    {k:<22}{v:>15,}")

    silver = build_silver(bronze)
    plan = C.explain_text(silver)
    n_shuffles = C.count_shuffles(silver)
    run.put("shuffles_before_write", n_shuffles)
    os.makedirs(C.EVIDENCE_DIR, exist_ok=True)
    plan_path = os.path.join(C.EVIDENCE_DIR, f"02_silver_plan_{C.NAMESPACE}.txt")
    with open(plan_path, "w") as f:
        f.write(plan)
    C.log(f"  physical plan: {n_shuffles} shuffle(s) before the write -> {plan_path}")

    with run.step("filter + dedup + clean + sessionize + write silver"):
        snap = C.replace_table(
            spark, silver, C.SILVER,
            partitioned_by=["days(event_time)"],
            props=SILVER_PROPS,
            comment="Silver: cleaned, de-duplicated, sessionized events",
        )
    run.put("snapshot", snap)

    valid_rows = dq["bronze_rows"] - dq["invalid_rows"]
    silver_rows = snap.get("total_records")
    run.put("valid_rows", valid_rows)
    run.put("silver_rows", silver_rows)
    if silver_rows is not None:
        run.put("duplicates_removed", valid_rows - silver_rows)
        C.log(f"  bronze {dq['bronze_rows']:,} -> invalid {dq['invalid_rows']:,} -> "
              f"valid {valid_rows:,} -> duplicates {valid_rows - silver_rows:,} -> silver {silver_rows:,}")

    with run.step("session statistics"):
        t = spark.table(C.SILVER)
        sess = t.filter(F.col("event_seq") == 1).agg(
            F.count(F.lit(1)).alias("sessions"),
            F.sum(F.col("session_has_purchase").cast("int")).alias("sessions_with_purchase"),
            F.sum(F.col("session_ended_with_purchase").cast("int")).alias("sessions_ending_in_purchase"),
            F.round(F.avg("session_event_count"), 2).alias("avg_events_per_session"),
            F.round(F.avg("session_duration_sec"), 1).alias("avg_session_duration_sec"),
            F.percentile_approx("session_duration_sec", 0.5).alias("median_session_duration_sec"),
        ).first().asDict()
        sess = {k: (float(v) if v is not None else None) for k, v in sess.items()}
        sess["session_conversion_rate"] = (round(sess["sessions_with_purchase"] / sess["sessions"], 4)
                                           if sess["sessions"] else None)
        run.put("sessions", sess)
        for k, v in sess.items():
            print(f"    {k:<30}{v}")
        run.put("layout", C.table_layout(spark, C.SILVER))

        print("  sample session:")
        sample_session = t.filter(
            (F.col("session_purchase_count") > 0) & (F.col("session_event_count").between(4, 8))
        ).select("user_session").limit(1).collect()
        if sample_session:
            (t.filter(F.col("user_session") == sample_session[0][0])
             .select("event_seq", "event_time", "event_type", "main_category", "brand", "price",
                     "sec_since_prev_event", "session_duration_sec", "session_has_purchase",
                     "session_ended_with_purchase")
             .orderBy("event_seq").show(truncate=False))

    run.save()
    spark.stop()


if __name__ == "__main__":
    main()
