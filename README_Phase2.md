# Distributed Clickstream Lakehouse — Phase 2

Distributed Spark processing (Task 3) and the Apache Iceberg lakehouse (Task 4).

```
HDFS raw CSV ──01──> bronze_events ──02──> silver_events ──04──> gold_* tables ──> ./results/gold_*.csv
                     (typed)               (clean, dedup,          (funnel, RFM,
                                            sessions)               hourly trends)
                                  03: Iceberg features on silver_events + daily_category_summary
```

All tables live in the Iceberg catalog **`local`** (Hadoop catalog, `hdfs://master:9000/warehouse/iceberg`),
namespace **`ecommerce`**. `common.get_spark()` registers the catalog, so the jobs work with the
Phase 1 `spark-defaults.conf` as is. The Phase 1 catalog `lake` points to the same warehouse.

## Files

```
jobs/phase2/
├── common.py                     # SparkSession + catalog, table names, timing, Iceberg/CSV helpers
├── 01_ingest_bronze.py           # CSV (HDFS) -> local.ecommerce.bronze_events
├── 02_clean_silver.py            # bronze -> local.ecommerce.silver_events
├── 03_iceberg_features_demo.py   # time travel, schema evolution, MERGE INTO
└── 04_gold_aggregations.py       # silver -> gold_* tables + ./results/gold_*.csv
scripts/11_run_phase2.sh          # runs 1-4 with spark-submit, logs to /bigdata/evidence/phase2/logs
```

## Run (on master, from the project root)

The cluster must be running (`./scripts/06_start_cluster.sh`, `./scripts/cluster_status.sh` → 3/3).

**1. Dev run first (1 % sample, separate namespace `local.ecommerce_dev`, a few minutes).** It catches
errors before the long run and does not touch the real tables.

```bash
./scripts/11_run_phase2.sh --dev
```

**2. Full run (all ~110 M rows).** Over Wi-Fi this takes a while (expect tens of minutes; every shuffle
crosses the network). Keep all three laptops awake.

```bash
./scripts/11_run_phase2.sh            # all four jobs
./scripts/11_run_phase2.sh 3 4        # or only some jobs, e.g. after a failure
```

### Exact spark-submit commands (what the runner executes)

```bash
cd ~/ecommerce-lakehouse
export LAKE_NS=ecommerce RESULTS_DIR=./results EVIDENCE_DIR=/bigdata/evidence/phase2
SUBMIT="$SPARK_HOME/bin/spark-submit --master spark://master:7077 --py-files jobs/phase2/common.py"

$SUBMIT --name phase2-01_ingest_bronze        jobs/phase2/01_ingest_bronze.py --sample 1.0
$SUBMIT --name phase2-02_clean_silver         jobs/phase2/02_clean_silver.py
$SUBMIT --name phase2-03_iceberg_features_demo jobs/phase2/03_iceberg_features_demo.py
$SUBMIT --name phase2-04_gold_aggregations    jobs/phase2/04_gold_aggregations.py
```

Executor memory, cores and the Iceberg jar come from `conf/spark/spark-defaults.conf` (Phase 1).

## What each job does

### 01 — Bronze (`bronze_events`, partitioned by `days(event_time), event_type`)
* Explicit 9-column schema (no `inferSchema` pass over 14 GB).
* `"2019-11-01 00:00:00 UTC"` → `TimestampType` with `try_to_timestamp` (Spark 4 runs in ANSI mode;
  bad values become NULL and are counted instead of failing the job).
* Lineage columns `source_file`, `ingested_at`.
* **No shuffle:** the CSVs are ordered by time, so each read task (one 128 MB HDFS block) touches only
  1–2 days. Iceberg's fanout writer writes those partitions straight from the task that read the block
  (`write.distribution-mode=none`). `shuffles_in_plan` in the evidence JSON shows 0.
* Checks: row count = Phase 1 manifest (109,950,743), unparsed timestamps, rows per event type,
  partition pruning query (one day of purchases).

### 02 — Silver (`silver_events`, partitioned by `days(event_time)`)
1. Data-quality profile of Bronze in **one** distributed aggregation (count per rule).
2. Filter: `price <= 0` or NULL, `user_id` NULL, `user_session` NULL, unparsed `event_time`, unknown `event_type`.
3. Exact-duplicate removal over the 9 original columns.
4. `brand`, `category_code` NULL/blank → `'unknown'`; `main_category` = first token of `category_code`.
5. Sessionization with window functions over `(user_id, user_session)` ordered by `event_time`:
   `event_seq`, `sec_since_prev_event`, `session_start/end`, `session_duration_sec`,
   `session_event_count`, `session_purchase_count`, `session_has_purchase`, `session_ended_with_purchase`.

**One shuffle for steps 3 + 5:** the data is hash-partitioned once by `(user_id, user_session)`. That
partitioning also satisfies the de-duplication (its key contains both columns) and the window, so Spark
does not shuffle again. The physical plan is saved to `/bigdata/evidence/phase2/02_silver_plan_*.txt`
(`shuffles_before_write` = 1). The Iceberg write adds one more shuffle (hash by day), so each day is
written by one task into a few large files.

### 03 — Iceberg features (Task 4 asks for at least two; three are shown)

| Feature | What the job does | Proof printed / saved |
|---|---|---|
| **Snapshots + time travel** | `DELETE` one real customer from Silver (merge-on-read → position-delete files, no data file rewritten), then reads the old state with `VERSION AS OF <id>` and `TIMESTAMP AS OF '<ts>'`, then `CALL local.system.rollback_to_snapshot` | `snapshots`, `history`, `files` metadata tables; customer rows now = 0, as-of = original, after rollback = original |
| **Schema evolution** | `ALTER TABLE silver_events ADD COLUMN is_high_value_item BOOLEAN` | same snapshot id, identical set of data files, old rows read NULL, new metadata version in `metadata_log_entries` |
| **ACID upsert (`MERGE INTO`)** | `daily_category_summary` loaded until 2019-11-15 12:00; an incremental batch from 2019-11-15 00:00 is staged in the Iceberg table `daily_category_increment`, then one `MERGE INTO` updates the partial day and inserts the new days in one commit | rows before/after (and via time travel), snapshot summary, result = full recomputation (0 mismatches) |

The Silver rollback means job 04 always aggregates the complete, unchanged Silver data.

### 04 — Gold
| Table | Grain | CSV |
|---|---|---|
| `gold_funnel_metrics` | `level` = category_brand / category / total; event counts, sessions with view / cart / purchase, conversion rates, revenue | `results/gold_funnel_metrics.csv` |
| `gold_rfm_segments` | one row per buying customer: recency, frequency (purchase sessions), monetary, R/F/M scores 1–5, segment | not exported (too large) |
| `gold_rfm_segment_summary` | per segment: customers, revenue share, average R/F/M; plus "Browsers (no purchase)" | `results/gold_rfm_segments_summary.csv` |
| `gold_rfm_rf_grid` | customers per (R score, F score) for a heatmap | `results/gold_rfm_rf_grid.csv` |
| `gold_hourly_trends` | day × hour (UTC): views, carts, purchases, revenue, active users and sessions (HyperLogLog) | `results/gold_hourly_trends.csv` |

RFM scoring: R and M use distributed `percentile_approx` quintiles; F uses fixed bins
(1 | 2 | 3 | 4–5 | 6+ purchase sessions) because most customers buy once, so quintiles collapse.
Funnel conversion is session-based: a session counts once per level, even if it viewed several brands.

`.gitignore` excludes `*.csv`; to commit the results: `git add -f results/*.csv`.

## Evidence for the report

`/bigdata/evidence/phase2/` (on master):
* `01_ingest_bronze_*.json` … `04_gold_aggregations_*.json`: step timings, row counts, data-quality
  counts, snapshot ids, files / partitions / MB per table, Iceberg feature checks (`pass: true`)
* `02_silver_plan_ecommerce.txt`: Silver physical plan (shuffles)
* `logs/*.log`: full console output of each spark-submit

Screenshots worth taking: Spark UI (`http://192.168.100.47:4040` while a job runs, or the History
Server on `:18080`). Look at the Jobs page (labels like `02_clean_silver: filter + dedup …`), Stages →
shuffle read/write, Executors (work on all 3 laptops), SQL tab → plan with `Exchange` nodes.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `ExecutorLostFailure` / `OutOfMemoryError` | Lower parallel tasks per executor: add `--conf spark.executor.cores=1` to the spark-submit, or raise `spark.executor.memory` if the laptop has RAM to spare |
| Job very slow, tasks waiting on shuffle | Expected over Wi-Fi; check all laptops are awake and on the same network |
| `No space left on device` on worker1 | `df -h /bigdata` on worker1: Bronze + Silver are replicated ×3, and Spark spills shuffle data to `/bigdata/spark/local` |
| `Table ... not found` in job 02/03/04 | Run the earlier job first (same mode: `--dev` or full) |
| Re-running a job | Safe: every table is recreated with `CREATE OR REPLACE TABLE`, and job 03 restores Silver with a rollback |
| Remove the dev tables after the full run works | `hdfs dfs -rm -r /warehouse/iceberg/ecommerce_dev` |
