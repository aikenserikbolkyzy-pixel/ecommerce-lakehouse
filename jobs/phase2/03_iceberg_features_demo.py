"""
Phase 2 / Job 3 - Apache Iceberg data-management features (Task 4).

A. Snapshots + time travel + rollback  (on silver_events)
   - list snapshots / history metadata tables
   - row-level DELETE of one customer ("right to be forgotten"), stored as
     merge-on-read position-delete files (no Parquet data file is rewritten)
   - read the table before the delete with VERSION AS OF and TIMESTAMP AS OF
   - CALL system.rollback_to_snapshot -> Silver is restored for the Gold job

B. Schema evolution  (on silver_events)
   - ALTER TABLE ... ADD COLUMN is_high_value_item BOOLEAN
   - proof that it is metadata-only: same snapshot, same data files, old rows read NULL

C. ACID upsert with MERGE INTO  (on daily_category_summary)
   - initial load up to 2019-11-15 12:00 (simulates a partially loaded day)
   - incremental batch from 2019-11-15 00:00: the partial day is UPDATEd,
     the new days are INSERTed, in one atomic commit
   - correctness check against a full recomputation + time travel on the summary
"""
from pyspark.sql import functions as F

import common as C

FEATURE_COL = "is_high_value_item"
CUTOFF = "2019-11-15 12:00:00"           # end of the "initial load"
INCREMENT_FROM = "2019-11-15 00:00:00"   # start of the "incremental batch"


def show(spark, sql, n=20):
    df = spark.sql(sql)
    df.show(n, truncate=False)
    return df


STAGE = C.tbl("daily_category_increment")   # staging table for the MERGE source


def silver_ident():
    # fully qualified (catalog.namespace.table): avoids a "catalog not found" warning
    return C.SILVER


# --------------------------------------------------------------------------- A
def demo_time_travel(spark, run):
    C.banner("A. SNAPSHOTS, TIME TRAVEL, ROW-LEVEL DELETE, ROLLBACK")
    S = C.SILVER

    show(spark, f"""
        SELECT snapshot_id, date_format(committed_at,'yyyy-MM-dd HH:mm:ss') AS committed_utc,
               operation, summary['added-records'] AS added_records,
               summary['total-records'] AS total_records
        FROM {S}.snapshots ORDER BY committed_at
    """)

    s0 = C.snapshot_info(spark, S)
    sid0, t0 = s0["snapshot_id"], s0["committed_at_utc"]
    C.log(f"  current snapshot s0 = {sid0} committed {t0} UTC")

    # pick one real customer deterministically (partition-pruned lookup)
    row = spark.sql(f"""
        SELECT user_id FROM {S}
        WHERE event_time >= TIMESTAMP '2019-11-17 00:00:00'
          AND event_time <  TIMESTAMP '2019-11-18 00:00:00'
          AND event_type = 'purchase'
        ORDER BY user_id LIMIT 1
    """).first() or spark.sql(f"SELECT min(user_id) AS user_id FROM {S}").first()
    user = int(row["user_id"])

    def user_rows(suffix=""):
        return int(spark.sql(
            f"SELECT count(*) AS n FROM {S} {suffix} WHERE user_id = {user}").first()["n"])

    total_before = int(spark.sql(f"SELECT count(*) AS n FROM {S}").first()["n"])
    before = user_rows()
    C.log(f"  customer {user}: {before} events, table total {total_before:,}")

    deleted = False
    try:
        with run.step("A1 DELETE one customer (merge-on-read)"):
            spark.sql(f"DELETE FROM {S} WHERE user_id = {user}")
            deleted = True
        s1 = C.snapshot_info(spark, S)
        C.log(f"  new snapshot s1 = {s1['snapshot_id']} ({s1['operation']}), "
              f"delete files total = {s1['total_delete_files']}")
        show(spark, f"""
            SELECT CASE content WHEN 0 THEN 'data' WHEN 1 THEN 'position deletes'
                                ELSE 'equality deletes' END AS file_type,
                   count(*) AS files, sum(record_count) AS records
            FROM {S}.files GROUP BY content ORDER BY content
        """)

        with run.step("A2 time travel queries"):
            now_rows = user_rows()
            version_rows = user_rows(f"VERSION AS OF {sid0}")
            ts_rows = user_rows(f"TIMESTAMP AS OF '{t0}'")
            total_now = int(spark.sql(f"SELECT count(*) AS n FROM {S}").first()["n"])
        C.log(f"  customer rows: current={now_rows}  VERSION AS OF s0={version_rows}  "
              f"TIMESTAMP AS OF '{t0}'={ts_rows}")
        C.log(f"  table rows:    current={total_now:,}  before={total_before:,}  "
              f"(diff {total_before - total_now})")
        run.put("time_travel", {
            "customer_user_id": user, "rows_before": before,
            "snapshot_before": sid0, "snapshot_after_delete": s1["snapshot_id"],
            "rows_after_delete": now_rows,
            "rows_version_as_of_before": version_rows,
            "rows_timestamp_as_of_before": ts_rows,
            "delete_files_after_delete": s1["total_delete_files"],
            "table_rows_before": total_before, "table_rows_after_delete": total_now,
            "pass": now_rows == 0 and version_rows == before and ts_rows == before,
        })
    finally:
        if deleted:
            with run.step("A3 rollback_to_snapshot"):
                spark.sql(f"CALL {C.CATALOG}.system.rollback_to_snapshot('{silver_ident()}', {sid0})")
            restored = user_rows()
            C.log(f"  after rollback: customer rows = {restored} (expected {before})")
            run.metrics.setdefault("time_travel", {})["rows_after_rollback"] = restored
            run.metrics["time_travel"]["rollback_pass"] = restored == before

    show(spark, f"""
        SELECT date_format(made_current_at,'yyyy-MM-dd HH:mm:ss') AS made_current_utc,
               snapshot_id, parent_id, is_current_ancestor
        FROM {S}.history ORDER BY made_current_at
    """)


# --------------------------------------------------------------------------- B
def data_file_paths(spark, table):
    return {r["file_path"] for r in spark.sql(f"SELECT file_path FROM {table}.files WHERE content = 0").collect()}


def demo_schema_evolution(spark, run):
    C.banner("B. SCHEMA EVOLUTION (metadata-only ADD COLUMN)")
    S = C.SILVER

    if FEATURE_COL in spark.table(S).columns:          # make the demo re-runnable
        C.log(f"  {FEATURE_COL} exists from a previous run -> DROP COLUMN (also metadata-only)")
        spark.sql(f"ALTER TABLE {S} DROP COLUMN {FEATURE_COL}")

    files_before = data_file_paths(spark, S)
    snap_before = C.current_snapshot_id(spark, S)
    meta_before = spark.sql(f"SELECT count(*) AS n FROM {S}.metadata_log_entries").first()["n"]

    with run.step("B1 ALTER TABLE ADD COLUMN"):
        spark.sql(f"""
            ALTER TABLE {S} ADD COLUMN {FEATURE_COL} BOOLEAN
            COMMENT 'Flag for expensive items; added by schema evolution, NULL for existing rows'
        """)

    files_after = data_file_paths(spark, S)
    snap_after = C.current_snapshot_id(spark, S)
    meta_after = spark.sql(f"SELECT count(*) AS n FROM {S}.metadata_log_entries").first()["n"]

    null_rows = spark.sql(f"""
        SELECT count(*) AS total, count({FEATURE_COL}) AS non_null
        FROM {S} WHERE event_time < TIMESTAMP '2019-10-02 00:00:00'
    """).first()

    spark.sql(f"DESCRIBE TABLE {S}").filter(
        F.col("col_name").isin("price", "brand", FEATURE_COL)).show(truncate=False)
    show(spark, f"SELECT event_time, brand, price, {FEATURE_COL} FROM {S} LIMIT 5")
    show(spark, f"""
        SELECT date_format(timestamp,'yyyy-MM-dd HH:mm:ss') AS ts_utc, latest_snapshot_id,
               latest_schema_id, file
        FROM {S}.metadata_log_entries ORDER BY timestamp DESC LIMIT 3
    """)

    res = {
        "column": FEATURE_COL,
        "data_files_before": len(files_before), "data_files_after": len(files_after),
        "same_data_files": files_before == files_after,
        "snapshot_before": snap_before, "snapshot_after": snap_after,
        "same_snapshot": snap_before == snap_after,
        "metadata_versions_before": int(meta_before), "metadata_versions_after": int(meta_after),
        "old_rows_checked": int(null_rows["total"]), "old_rows_non_null": int(null_rows["non_null"]),
    }
    res["pass"] = res["same_data_files"] and res["same_snapshot"] and res["old_rows_non_null"] == 0
    run.put("schema_evolution", res)
    C.log(f"  [{'PASS' if res['pass'] else 'FAIL'}] no data file rewritten "
          f"({len(files_before)} files before / {len(files_after)} after), "
          f"snapshot unchanged, new metadata version {meta_before} -> {meta_after}")


# --------------------------------------------------------------------------- C
def daily_summary(df, batch_label):
    is_t = lambda t: (F.col("event_type") == t).cast("long")  # noqa: E731
    return (df.groupBy("event_date", "main_category")
            .agg(F.sum(is_t("view")).alias("views"),
                 F.sum(is_t("cart")).alias("carts"),
                 F.sum(is_t("purchase")).alias("purchases"),
                 F.round(F.sum(F.when(F.col("event_type") == "purchase", F.col("price"))
                               .otherwise(F.lit(0.0))), 2).alias("revenue"),
                 F.count_distinct(F.when(F.col("event_type") == "purchase", F.col("user_id"))).alias("buyers"))
            .withColumn("load_batch", F.lit(batch_label))
            .withColumn("last_updated", F.current_timestamp()))


def demo_merge(spark, run):
    C.banner("C. ACID UPSERT WITH MERGE INTO")
    T = C.DAILY_SUMMARY
    silver = spark.table(C.SILVER)

    with run.step("C1 initial load (until 2019-11-15 12:00)"):
        initial = daily_summary(silver.filter(F.col("event_time") < F.expr(f"TIMESTAMP '{CUTOFF}'")),
                                "initial")
        s_init = C.replace_table(spark, initial, T,
                                 props={"write.merge.mode": "copy-on-write"},
                                 comment="Daily KPIs per main_category, maintained with MERGE INTO")
    C.log(f"  initial snapshot {s_init['snapshot_id']}: {s_init['total_records']} rows")

    # The incremental batch lands in a small Iceberg staging table, like a real
    # micro-batch. (MERGE from a temp view over an Iceberg DataFrame hits a Spark 4.1
    # planner bug: "No plan for TableReference".)
    with run.step("C1b stage incremental batch"):
        src = daily_summary(silver.filter(F.col("event_time") >= F.expr(f"TIMESTAMP '{INCREMENT_FROM}'")),
                            "incremental")
        C.replace_table(spark, src, STAGE, comment="Staging: incremental batch for the MERGE demo")
    src_rows = int(spark.sql(f"SELECT count(*) AS n FROM {STAGE}").first()["n"])
    matched = spark.sql(f"""
        SELECT count(*) AS n FROM {STAGE} s JOIN {T} t
        ON t.event_date = s.event_date AND t.main_category = s.main_category
    """).first()["n"]
    C.log(f"  incremental batch: {src_rows} rows -> expect {matched} UPDATE, {src_rows - matched} INSERT")

    print("  2019-11-15 BEFORE merge (partial day):")
    show(spark, f"""SELECT event_date, main_category, views, carts, purchases, revenue, load_batch
                    FROM {T} WHERE event_date = DATE '2019-11-15' ORDER BY views DESC LIMIT 5""")

    with run.step("C2 MERGE INTO"):
        spark.sql(f"""
            MERGE INTO {T} t
            USING {STAGE} s
            ON t.event_date = s.event_date AND t.main_category = s.main_category
            WHEN MATCHED THEN UPDATE SET
                t.views = s.views, t.carts = s.carts, t.purchases = s.purchases,
                t.revenue = s.revenue, t.buyers = s.buyers,
                t.load_batch = s.load_batch, t.last_updated = s.last_updated
            WHEN NOT MATCHED THEN INSERT *
        """)
    s_merge = C.snapshot_info(spark, T)
    C.log(f"  merge snapshot {s_merge['snapshot_id']} ({s_merge['operation']}): "
          f"total rows {s_merge['total_records']}, added {s_merge['added_records']}, "
          f"deleted(rewritten) {s_merge['deleted_records']}")

    print("  2019-11-15 AFTER merge (full day):")
    show(spark, f"""SELECT event_date, main_category, views, carts, purchases, revenue, load_batch
                    FROM {T} WHERE event_date = DATE '2019-11-15' ORDER BY views DESC LIMIT 5""")
    print("  same rows via time travel to the snapshot BEFORE the merge:")
    show(spark, f"""SELECT event_date, main_category, views, carts, purchases, revenue, load_batch
                    FROM {T} VERSION AS OF {s_init['snapshot_id']}
                    WHERE event_date = DATE '2019-11-15' ORDER BY views DESC LIMIT 5""")

    with run.step("C3 verify against full recomputation"):
        full = daily_summary(silver, "full")
        keys = ["event_date", "main_category"]
        a = spark.table(T).select(*keys, *[F.col(c).alias("t_" + c) for c in
                                           ["views", "carts", "purchases", "revenue", "buyers"]])
        j = a.join(full, keys, "full_outer")
        mismatches = j.filter(
            F.col("t_views").isNull() | F.col("views").isNull()
            | (F.col("t_views") != F.col("views")) | (F.col("t_carts") != F.col("carts"))
            | (F.col("t_purchases") != F.col("purchases")) | (F.col("t_buyers") != F.col("buyers"))
            | (F.abs(F.col("t_revenue") - F.col("revenue")) > 0.01)
        ).count()
        batches = {r["load_batch"]: int(r["n"]) for r in
                   spark.sql(f"SELECT load_batch, count(*) AS n FROM {T} GROUP BY load_batch").collect()}

    res = {"initial_snapshot": s_init, "merge_snapshot": s_merge,
           "source_rows": int(src_rows), "expected_updates": int(matched),
           "expected_inserts": int(src_rows - matched),
           "rows_by_load_batch": batches, "mismatches_vs_full_recompute": int(mismatches)}
    res["pass"] = mismatches == 0 and batches.get("incremental", 0) == src_rows
    run.put("merge_into", res)
    C.log(f"  [{'PASS' if res['pass'] else 'FAIL'}] MERGE result equals full recomputation "
          f"({mismatches} mismatching rows), rows by batch {batches}")


def main():
    spark = C.get_spark("phase2-03-iceberg-features")
    run = C.Run(spark, "03_iceberg_features_demo")
    demo_time_travel(spark, run)
    demo_schema_evolution(spark, run)
    demo_merge(spark, run)

    C.banner("SUMMARY")
    for k in ("time_travel", "schema_evolution", "merge_into"):
        m = run.metrics.get(k, {})
        ok = m.get("pass") and (k != "time_travel" or m.get("rollback_pass"))
        print(f"  [{'PASS' if ok else 'FAIL'}] {k}")
    run.save()
    spark.stop()


if __name__ == "__main__":
    main()
