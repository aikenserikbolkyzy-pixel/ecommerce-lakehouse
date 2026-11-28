"""
Phase 2 / Job 1 - Bronze layer.

Raw CSVs in HDFS  ->  typed Iceberg table local.ecommerce.bronze_events
                      partitioned by days(event_time), event_type

* explicit schema (no inferSchema pass over 14 GB)
* event_time "2019-11-01 00:00:00 UTC" -> TimestampType (UTC)
* lineage columns: source_file, ingested_at
* no shuffle: the CSVs are already ordered by time, so every read task (one HDFS
  block) only touches 1-2 days. Iceberg's fanout writer writes those partitions
  directly from the task that holds the block (data-local, no network transfer).

Usage:
  spark-submit ... 01_ingest_bronze.py [--sample 0.01]
"""
import argparse

from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField,
                               StructType)

import common as C

RAW_SCHEMA = StructType([
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", LongType(), True),
    StructField("category_id", LongType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_session", StringType(), True),
])

BRONZE_PROPS = {
    # input is time-ordered -> write without a shuffle, one writer per open partition
    "write.distribution-mode": "none",
    "write.spark.fanout.enabled": "true",
}


def read_raw(spark, sample):
    df = (spark.read
          .schema(RAW_SCHEMA)
          .option("header", "true")
          .option("mode", "PERMISSIVE")      # malformed fields -> NULL, counted below
          .option("pathGlobFilter", "*.csv")
          .csv(C.RAW_DIR))
    df = df.withColumn("source_file", F.col("_metadata.file_name"))
    if sample < 1.0:
        df = df.sample(fraction=sample, seed=42)
    return df


def to_bronze(raw):
    # "2019-11-01 00:00:00 UTC" -> timestamp. try_to_timestamp returns NULL instead of
    # failing the job (Spark 4 runs in ANSI mode by default).
    ts = F.try_to_timestamp(F.regexp_replace("event_time", r"\s*UTC$", ""),
                            F.lit("yyyy-MM-dd HH:mm:ss"))
    return raw.select(
        ts.alias("event_time"),
        F.lower(F.trim("event_type")).alias("event_type"),
        "product_id", "category_id", "category_code", "brand", "price",
        "user_id", "user_session",
        "source_file",
        F.current_timestamp().alias("ingested_at"),
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=float, default=1.0,
                    help="fraction of rows to keep (dev runs), default 1.0 = all")
    args = ap.parse_args()

    spark = C.get_spark("phase2-01-ingest-bronze")
    run = C.Run(spark, "01_ingest_bronze")
    run.put("sample_fraction", args.sample)
    C.banner(f"BRONZE  {C.RAW_DIR}  ->  {C.BRONZE}")

    bronze = to_bronze(read_raw(spark, args.sample))
    plan = C.explain_text(bronze)
    run.put("shuffles_in_plan", C.count_shuffles(bronze))
    log_plan = plan if len(plan) < 4000 else plan[:4000] + "\n..."
    print(log_plan)

    with run.step("write bronze (CSV parse + Iceberg write)"):
        snap = C.replace_table(
            spark, bronze, C.BRONZE,
            partitioned_by=["days(event_time)", "event_type"],
            props=BRONZE_PROPS,
            comment="Bronze: raw e-commerce events, typed, partitioned by day and event type",
        )
    run.put("snapshot", snap)
    C.log(f"  snapshot {snap.get('snapshot_id')}: {snap.get('total_records')} records, "
          f"{snap.get('total_data_files')} data files")

    with run.step("verify bronze"):
        layout = C.table_layout(spark, C.BRONZE)
        run.put("layout", layout)
        C.log(f"  layout: {layout}")

        stats = spark.table(C.BRONZE).agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(F.when(F.col("event_time").isNull(), 1).otherwise(0)).alias("unparsed_event_time"),
            F.min("event_time").alias("min_ts"),
            F.max("event_time").alias("max_ts"),
        ).first()
        run.put("rows", int(stats["rows"]))
        run.put("unparsed_event_time", int(stats["unparsed_event_time"]))
        run.put("time_range_utc", [str(stats["min_ts"]), str(stats["max_ts"])])

        by_type = spark.table(C.BRONZE).groupBy("event_type").count().orderBy(F.desc("count"))
        by_type.show(truncate=False)
        run.put("rows_by_event_type", {r["event_type"]: int(r["count"]) for r in by_type.collect()})

        # partition pruning demo: 1 day of purchases only reads that partition
        one_day = spark.sql(f"""
            SELECT count(*) AS n FROM {C.BRONZE}
            WHERE event_time >= TIMESTAMP '2019-11-17 00:00:00'
              AND event_time <  TIMESTAMP '2019-11-18 00:00:00'
              AND event_type = 'purchase'
        """).first()["n"]
        run.put("purchases_2019_11_17", int(one_day))

        spark.sql(f"""
            SELECT partition, record_count, file_count
            FROM {C.BRONZE}.partitions ORDER BY partition LIMIT 8
        """).show(truncate=False)

    expected = C.manifest_total_rows() if args.sample >= 1.0 else None
    run.put("manifest_rows", expected)
    if expected is not None:
        ok = expected == int(stats["rows"])
        run.put("row_count_matches_manifest", ok)
        C.log(f"[{'PASS' if ok else 'FAIL'}] bronze rows {int(stats['rows']):,} vs manifest {expected:,}")
    C.log(f"[{'PASS' if int(stats['unparsed_event_time']) == 0 else 'WARN'}] "
          f"unparsed event_time rows: {int(stats['unparsed_event_time']):,}")

    run.save()
    spark.stop()


if __name__ == "__main__":
    main()
