"""
Shared helpers for the Phase 2 jobs (Spark processing + Iceberg lakehouse).

Every job imports this module. It
  * builds the SparkSession with the Iceberg catalog `local` (Hadoop catalog on HDFS),
  * names all tables in one place,
  * times each step and labels it in the Spark UI,
  * writes Iceberg tables and small CSV exports,
  * saves a JSON "evidence" file per run for the report.

Environment overrides (used by scripts/11_run_phase2.sh --dev):
  LAKE_NS        Iceberg namespace           (default: ecommerce)
  RESULTS_DIR    local CSV export directory  (default: ./results)
  EVIDENCE_DIR   JSON evidence directory     (default: /bigdata/evidence/phase2)
"""
import contextlib
import csv
import io
import json
import os
import time
from datetime import datetime, timezone

# Driver-side Python must agree with Spark's session time zone (UTC), otherwise
# timestamps collected to Python would be shifted to the laptop's local time.
os.environ["TZ"] = "UTC"
time.tzset()

from pyspark.sql import SparkSession  # noqa: E402

# --------------------------------------------------------------------------- names
CATALOG = os.environ.get("LAKE_CATALOG", "local")
NAMESPACE = os.environ.get("LAKE_NS", "ecommerce")
WAREHOUSE = os.environ.get("LAKE_WAREHOUSE", "hdfs://master:9000/warehouse/iceberg")
RAW_DIR = os.environ.get("RAW_DIR", "hdfs://master:9000/data/raw/ecommerce")
MANIFEST = os.environ.get("MANIFEST", "/bigdata/staging/ecommerce/manifest.csv")
RESULTS_DIR = os.environ.get("RESULTS_DIR", "./results")
EVIDENCE_DIR = os.environ.get("EVIDENCE_DIR", "/bigdata/evidence/phase2")


def tbl(name):
    return f"{CATALOG}.{NAMESPACE}.{name}"


BRONZE = tbl("bronze_events")
SILVER = tbl("silver_events")
DAILY_SUMMARY = tbl("daily_category_summary")      # MERGE INTO demo table
GOLD_FUNNEL = tbl("gold_funnel_metrics")
GOLD_RFM = tbl("gold_rfm_segments")
GOLD_RFM_SUMMARY = tbl("gold_rfm_segment_summary")
GOLD_RFM_GRID = tbl("gold_rfm_rf_grid")
GOLD_HOURLY = tbl("gold_hourly_trends")

# Original 9 CSV columns (used for exact-duplicate detection)
DATA_COLS = ["event_time", "event_type", "product_id", "category_id",
             "category_code", "brand", "price", "user_id", "user_session"]

# Iceberg table properties shared by all tables
BASE_PROPS = {
    "format-version": "2",
    "write.parquet.compression-codec": "zstd",
    "write.target-file-size-bytes": str(128 * 1024 * 1024),
    # smaller row groups = less writer memory on 8 GB laptops
    "write.parquet.row-group-size-bytes": str(64 * 1024 * 1024),
}


# --------------------------------------------------------------------------- session
def get_spark(app_name):
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
        # always read the latest table metadata (matters for the DELETE/rollback demo)
        .config(f"spark.sql.catalog.{CATALOG}.cache-enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    log(f"Spark {spark.version} | master={spark.sparkContext.master} | "
        f"namespace={CATALOG}.{NAMESPACE} | warehouse={WAREHOUSE}")
    return spark


def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


def banner(title):
    line = "=" * 78
    print(f"\n{line}\n  {title}\n{line}", flush=True)


# --------------------------------------------------------------------------- run / evidence
class Run:
    """Times steps, labels them in the Spark UI and saves a JSON evidence file."""

    def __init__(self, spark, job):
        self.spark = spark
        self.job = job
        self.t0 = time.time()
        self.metrics = {
            "job": job,
            "namespace": f"{CATALOG}.{NAMESPACE}",
            "started_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "spark_version": spark.version,
            "default_parallelism": spark.sparkContext.defaultParallelism,
            "steps_sec": {},
        }

    @contextlib.contextmanager
    def step(self, name):
        sc = self.spark.sparkContext
        sc.setJobDescription(f"{self.job}: {name}")
        log(f"> {name}")
        start = time.time()
        try:
            yield
        finally:
            dt = round(time.time() - start, 1)
            self.metrics["steps_sec"][name] = dt
            log(f"  {name}: {dt}s")
            sc.setJobDescription(None)

    def put(self, key, value):
        self.metrics[key] = value

    def save(self):
        self.metrics["total_sec"] = round(time.time() - self.t0, 1)
        os.makedirs(EVIDENCE_DIR, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        path = os.path.join(EVIDENCE_DIR, f"{self.job}_{NAMESPACE}_{stamp}.json")
        with open(path, "w") as f:
            json.dump(self.metrics, f, indent=2, default=str)
        log(f"Evidence written to {path}  (total {self.metrics['total_sec']}s)")
        return path


def explain_text(df, mode="formatted"):
    """Return df.explain() output as a string (for counting shuffles in the report)."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode=mode)
    return buf.getvalue()


def count_shuffles(df):
    """Number of shuffle (Exchange) operators in the physical plan of df."""
    import re
    simple = explain_text(df, mode="simple")   # one line per operator, e.g. "Exchange hashpartitioning(..)"
    return len(re.findall(r"\bExchange (hashpartitioning|rangepartitioning|RoundRobinPartitioning|SinglePartition)",
                          simple))


# --------------------------------------------------------------------------- Iceberg helpers
def _ddl(schema):
    return ",\n  ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in schema.fields)


def replace_table(spark, df, table, partitioned_by=None, props=None, comment=None):
    """
    (Re)create an Iceberg table with an explicit DDL and append `df` into it.

    Appending (instead of CTAS) guarantees Spark applies Iceberg's requested write
    distribution (e.g. hash by partition), so partitioned writes never fail with
    "records not clustered by partition".
    """
    all_props = {**BASE_PROPS, **(props or {})}
    part_sql = f"PARTITIONED BY ({', '.join(partitioned_by)})" if partitioned_by else ""
    props_sql = ", ".join(f"'{k}'='{v}'" for k, v in all_props.items())
    comment_sql = f"COMMENT '{comment}'" if comment else ""
    body = f"(\n  {_ddl(df.schema)}\n) USING iceberg {part_sql} {comment_sql} TBLPROPERTIES ({props_sql})"
    try:
        spark.sql(f"CREATE OR REPLACE TABLE {table} {body}")
    except Exception as exc:  # fallback if the catalog refuses REPLACE
        log(f"  REPLACE not possible ({type(exc).__name__}); dropping and recreating {table}")
        spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
        spark.sql(f"CREATE TABLE {table} {body}")
    df.writeTo(table).append()
    return snapshot_info(spark, table)


def current_snapshot_id(spark, table):
    row = spark.sql(
        f"SELECT snapshot_id FROM {table}.history ORDER BY made_current_at DESC LIMIT 1"
    ).first()
    return None if row is None else int(row["snapshot_id"])


def snapshot_info(spark, table, snapshot_id=None):
    sid = snapshot_id if snapshot_id is not None else current_snapshot_id(spark, table)
    if sid is None:
        return {}
    row = spark.sql(f"""
        SELECT snapshot_id,
               date_format(committed_at, 'yyyy-MM-dd HH:mm:ss.SSS') AS committed_at,
               operation, summary
        FROM {table}.snapshots WHERE snapshot_id = {sid}
    """).first()
    summary = dict(row["summary"] or {})
    return {
        "snapshot_id": int(row["snapshot_id"]),
        "committed_at_utc": row["committed_at"],
        "operation": row["operation"],
        "total_records": _int(summary.get("total-records")),
        "total_data_files": _int(summary.get("total-data-files")),
        "total_delete_files": _int(summary.get("total-delete-files")),
        "added_records": _int(summary.get("added-records")),
        "deleted_records": _int(summary.get("deleted-records")),
        "total_files_size_bytes": _int(summary.get("total-files-size")),
    }


def table_layout(spark, table):
    """Files / partitions / size of the current snapshot (from Iceberg metadata tables)."""
    files = spark.sql(f"""
        SELECT count(*) AS n, coalesce(sum(file_size_in_bytes), 0) AS bytes
        FROM {table}.files WHERE content = 0
    """).first()
    parts = spark.sql(f"SELECT count(*) AS n FROM {table}.partitions").first()
    return {"data_files": int(files["n"]),
            "data_bytes": int(files["bytes"]),
            "data_mb": round(int(files["bytes"]) / 1024 / 1024, 1),
            "partitions": int(parts["n"])}


def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- CSV export
def export_csv(df, name, order_by=None):
    """Collect a SMALL DataFrame to the driver and write ./results/<name>.csv (no pandas)."""
    if order_by:
        df = df.orderBy(*order_by)
    rows = df.collect()
    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, f"{name}.csv")
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(df.columns)
        for r in rows:
            w.writerow(["" if v is None else v for v in r])
    log(f"  exported {len(rows):,} rows -> {path}")
    return {"path": os.path.abspath(path), "rows": len(rows)}


def manifest_total_rows(path=MANIFEST):
    """Total row count from Phase 1 manifest.csv, or None if it cannot be read."""
    try:
        with open(path) as f:
            return sum(int(r["rows"]) for r in csv.DictReader(f))
    except Exception:
        return None
