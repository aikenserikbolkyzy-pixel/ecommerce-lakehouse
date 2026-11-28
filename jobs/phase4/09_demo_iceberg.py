"""
Phase 4 / Job 9 - LIVE Iceberg demo for the teacher (about 1-2 minutes incl. Spark start-up).

Works on the small table local.ecommerce.daily_category_summary (854 rows), so every
step is instant, and it leaves the table exactly as it was.

  1. Tables of the lakehouse (Bronze / Silver / Gold / ML)
  2. Snapshot history of daily_category_summary (initial load -> MERGE INTO)
  3. Time travel: 2019-11-15 before the MERGE (VERSION AS OF) vs now
  4. LIVE row-level DELETE of one day -> the old snapshot still has it -> rollback
  5. Silver's history: the "right to be forgotten" deletes and their rollbacks from job 03

Run:  ./jobs/phase4/09_demo.sh
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "phase2"))

import common as C  # noqa: E402

T = C.DAILY_SUMMARY
DEMO_DAY = "2019-11-20"


def step(n, text):
    print(f"\n\033[1m[{n}] {text}\033[0m", flush=True)


def show(spark, sql, n=20):
    spark.sql(sql).show(n, truncate=False)


def count_day(spark, suffix=""):
    return int(spark.sql(f"SELECT count(*) AS n FROM {T} {suffix} WHERE event_date = DATE '{DEMO_DAY}'")
               .first()["n"])


def main():
    spark = C.get_spark("phase4-live-iceberg-demo")

    step(1, f"Tables in {C.CATALOG}.{C.NAMESPACE} (Iceberg on HDFS {C.WAREHOUSE})")
    show(spark, f"SHOW TABLES IN {C.CATALOG}.{C.NAMESPACE}", 40)

    step(2, f"Snapshot history of {T}")
    show(spark, f"""
        SELECT h.snapshot_id, date_format(s.committed_at,'yyyy-MM-dd HH:mm:ss') AS committed_utc,
               s.operation, s.summary['added-records'] AS added, s.summary['deleted-records'] AS deleted,
               s.summary['total-records'] AS total, h.is_current_ancestor
        FROM {T}.history h JOIN {T}.snapshots s ON h.snapshot_id = s.snapshot_id
        ORDER BY h.made_current_at DESC LIMIT 6""")
    cur = C.current_snapshot_id(spark, T)
    parent = spark.sql(f"SELECT parent_id FROM {T}.snapshots WHERE snapshot_id = {cur}").first()["parent_id"]

    step(3, "Time travel: 2019-11-15 before the MERGE (only half the day was loaded) vs now")
    cols = "event_date, main_category, views, carts, purchases, load_batch"
    if parent is not None:
        print(f"  VERSION AS OF {parent}  (before MERGE INTO)")
        show(spark, f"""SELECT {cols} FROM {T} VERSION AS OF {parent}
                        WHERE event_date = DATE '2019-11-15' ORDER BY views DESC LIMIT 3""")
    print(f"  current snapshot {cur}  (after MERGE INTO)")
    show(spark, f"""SELECT {cols} FROM {T} WHERE event_date = DATE '2019-11-15' ORDER BY views DESC LIMIT 3""")

    step(4, f"LIVE: DELETE all rows of {DEMO_DAY}, read the old snapshot, roll back")
    before_id = cur
    before = count_day(spark)
    print(f"  rows for {DEMO_DAY} now: {before}")
    deleted = False
    try:
        spark.sql(f"DELETE FROM {T} WHERE event_date = DATE '{DEMO_DAY}'")
        deleted = True
        new_id = C.current_snapshot_id(spark, T)
        print(f"  DELETE committed as snapshot {new_id}")
        print(f"  rows for {DEMO_DAY} now:                         {count_day(spark)}")
        print(f"  rows for {DEMO_DAY} VERSION AS OF {before_id}: "
              f"{count_day(spark, f'VERSION AS OF {before_id}')}")
    finally:
        if deleted:
            spark.sql(f"CALL {C.CATALOG}.system.rollback_to_snapshot('{T}', {before_id})")
            print(f"  rollback_to_snapshot({before_id}) -> rows for {DEMO_DAY}: {count_day(spark)}"
                  f"  (expected {before})")

    step(5, f"History of {C.SILVER}: customer deletes (job 03) and their rollbacks")
    show(spark, f"""
        SELECT date_format(h.made_current_at,'yyyy-MM-dd HH:mm:ss') AS made_current_utc, h.snapshot_id,
               s.operation, h.is_current_ancestor
        FROM {C.SILVER}.history h JOIN {C.SILVER}.snapshots s ON h.snapshot_id = s.snapshot_id
        ORDER BY h.made_current_at DESC LIMIT 6""")
    print("\nDemo finished - the table is back to its original snapshot.")
    spark.stop()


if __name__ == "__main__":
    main()
