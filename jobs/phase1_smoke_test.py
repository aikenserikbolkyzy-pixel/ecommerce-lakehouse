"""
Phase 1 smoke test — run with spark-submit on the Standalone cluster.

Proves, before any real processing starts, that:
  1. Spark tasks execute on all 3 laptops (master, worker1, worker2);
  2. Spark reads the raw CSVs from HDFS in parallel (~1 input partition per 128 MB block);
  3. the row counts in HDFS equal the local manifest written at unzip time;
  4. the Iceberg HadoopCatalog "lake" works on HDFS: namespaces bronze/silver/gold
     are created, and a throw-away table supports insert, snapshots and time travel.

Usage (on master):
  spark-submit jobs/phase1_smoke_test.py --evidence-dir /bigdata/evidence/phase1
"""
import argparse
import json
import math
import os
import socket
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Raw schema exactly as in the Kaggle CSV. event_time stays a string here
# ("2019-11-01 00:00:00 UTC"); it is parsed to a timestamp in the Bronze job (Phase 2).
RAW_SCHEMA = T.StructType([
    T.StructField("event_time", T.StringType(), True),
    T.StructField("event_type", T.StringType(), True),
    T.StructField("product_id", T.LongType(), True),
    T.StructField("category_id", T.LongType(), True),
    T.StructField("category_code", T.StringType(), True),
    T.StructField("brand", T.StringType(), True),
    T.StructField("price", T.DoubleType(), True),
    T.StructField("user_id", T.LongType(), True),
    T.StructField("user_session", T.StringType(), True),
])

EXPECTED_HOSTS = {"master", "worker1", "worker2"}
LAYERS = ("bronze", "silver", "gold")
CATALOG = "lake"


def parse_args():
    p = argparse.ArgumentParser(description="Phase 1 cluster + HDFS + Iceberg smoke test")
    p.add_argument("--raw", default="/data/raw/ecommerce/*.csv", help="HDFS glob of raw CSVs")
    p.add_argument("--manifest", default="/data/meta/ingest/manifest.csv", help="HDFS manifest from 07/08")
    p.add_argument("--evidence-dir", default="/bigdata/evidence/phase1", help="local dir on the driver")
    return p.parse_args()


def check(results, name, passed, detail):
    results["checks"].append({"check": name, "passed": bool(passed), "detail": detail})
    print(f"[{'PASS' if passed else 'FAIL'}] {name}: {detail}", flush=True)


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("phase1-smoke-test").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    results = {
        "run_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "spark_version": spark.version,
        "master": sc.master,
        "default_parallelism": sc.defaultParallelism,
        "checks": [],
    }

    # ------------------------------------------------------------------
    # 1. Which machines run tasks?
    # ------------------------------------------------------------------
    n = max(sc.defaultParallelism * 4, 24)
    hosts = sorted(
        sc.parallelize(range(n), n).map(lambda _: socket.gethostname().split(".")[0]).distinct().collect()
    )
    results["task_hosts"] = hosts
    check(results, "tasks run on all 3 laptops", EXPECTED_HOSTS.issubset(hosts), f"hosts={hosts}")

    # ------------------------------------------------------------------
    # 2. Parallel read of the raw CSVs from HDFS
    # ------------------------------------------------------------------
    raw = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(RAW_SCHEMA)
        .csv(args.raw)
        .withColumn("source_file", F.col("_metadata.file_name"))
    )
    partitions = raw.rdd.getNumPartitions()
    results["input_partitions"] = partitions

    t0 = time.time()
    by_file_type = (
        raw.groupBy("source_file", "event_type").count().collect()   # one distributed pass
    )
    scan_seconds = round(time.time() - t0, 1)
    results["full_scan_seconds"] = scan_seconds

    rows_per_file, rows_per_type = {}, {}
    for r in by_file_type:
        rows_per_file[r["source_file"]] = rows_per_file.get(r["source_file"], 0) + r["count"]
        rows_per_type[r["event_type"]] = rows_per_type.get(r["event_type"], 0) + r["count"]
    total = sum(rows_per_file.values())
    results.update(rows_per_file=rows_per_file, rows_per_event_type=rows_per_type, total_rows=total)
    print(f"Input partitions: {partitions}   full scan: {scan_seconds}s   rows: {total:,}", flush=True)
    for k, v in sorted(rows_per_type.items(), key=lambda kv: -kv[1]):
        print(f"    {k:<18}{v:>14,}  ({100 * v / total:5.2f}%)")

    # Expected splits ~= sum(ceil(file_size / 128 MB))
    manifest = {r["file"]: r for r in spark.read.option("header", True).csv(args.manifest).collect()}
    expected_splits = sum(math.ceil(int(m["bytes"]) / 134217728) for m in manifest.values())
    check(results, "input partitions ~ HDFS blocks", abs(partitions - expected_splits) <= 2,
          f"partitions={partitions}, blocks={expected_splits}")

    for fname, m in sorted(manifest.items()):
        got, want = rows_per_file.get(fname, 0), int(m["rows"])
        check(results, f"row count {fname}", got == want, f"spark={got:,} manifest={want:,}")

    # ------------------------------------------------------------------
    # 3. Iceberg HadoopCatalog on HDFS
    # ------------------------------------------------------------------
    for layer in LAYERS:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{layer}")
    namespaces = sorted(r[0] for r in spark.sql(f"SHOW NAMESPACES IN {CATALOG}").collect())
    results["iceberg_namespaces"] = namespaces
    check(results, "Iceberg namespaces", set(LAYERS).issubset(namespaces), f"{CATALOG}: {namespaces}")

    t = f"{CATALOG}.bronze.phase1_smoke_test"
    spark.sql(f"DROP TABLE IF EXISTS {t} PURGE")
    spark.sql(f"""
        CREATE TABLE {t} (id BIGINT, event_type STRING, ts TIMESTAMP)
        USING iceberg
        PARTITIONED BY (days(ts))
    """)
    spark.sql(f"INSERT INTO {t} VALUES (1, 'view', TIMESTAMP '2019-11-01 00:00:00'), "
              f"(2, 'cart', TIMESTAMP '2019-11-01 00:00:01')")
    spark.sql(f"INSERT INTO {t} VALUES (3, 'purchase', TIMESTAMP '2019-11-02 10:00:00')")

    snaps = spark.sql(f"SELECT snapshot_id FROM {t}.snapshots ORDER BY committed_at").collect()
    first_snapshot = snaps[0]["snapshot_id"]
    now_count = spark.table(t).count()
    old_count = spark.sql(f"SELECT count(*) AS c FROM {t} VERSION AS OF {first_snapshot}").first()["c"]
    data_file = spark.sql(f"SELECT file_path FROM {t}.files LIMIT 1").first()["file_path"]

    check(results, "Iceberg snapshots", len(snaps) == 2, f"{len(snaps)} snapshots")
    check(results, "Iceberg time travel", (now_count, old_count) == (3, 2),
          f"current={now_count} rows, VERSION AS OF first snapshot={old_count} rows")
    check(results, "Iceberg data stored in HDFS warehouse",
          data_file.startswith("hdfs://master:9000/warehouse/iceberg/bronze/phase1_smoke_test/"), data_file)
    spark.sql(f"DROP TABLE {t} PURGE")

    # ------------------------------------------------------------------
    # Evidence
    # ------------------------------------------------------------------
    results["all_passed"] = all(c["passed"] for c in results["checks"])
    os.makedirs(args.evidence_dir, exist_ok=True)
    out = os.path.join(args.evidence_dir,
                       f"smoke_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(out, "w") as fh:
        json.dump(results, fh, indent=2, default=str)
    print(f"\nEvidence written to {out}")
    print("RESULT:", "ALL CHECKS PASSED" if results["all_passed"] else "SOME CHECKS FAILED")

    spark.stop()
    sys.exit(0 if results["all_passed"] else 1)


if __name__ == "__main__":
    main()
