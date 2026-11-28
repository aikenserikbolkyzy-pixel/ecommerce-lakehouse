"""
Phase 4 / Job 7 - reproducible benchmarks (Task 6).

Three modes (07_run_benchmark.sh drives them):

  --mode scaling    ONE measurement of the scalability workload at p cores.
                    Workload on local.ecommerce.silver_events (date range, default 2019-11-01..14):
                      1. filter events (date range -> Iceberg partition pruning, price > 0)
                      2. shuffle-heavy session aggregation: groupBy(user_id, user_session)
                         -> per-session counts / revenue / duration -> cluster-wide summary
                      3. category revenue: purchases groupBy(main_category)
                    The runner submits it with --total-executor-cores p, 1-core executors of
                    700 MB (same memory per core for every p) and spark.sql.shuffle.partitions = 4p.
                    Timing starts after all p executors have registered.

  --mode storage    Raw CSV (HDFS) vs Iceberg Parquet (bronze_events), same results checked:
                      Q1 "one day of purchases"  -> Iceberg reads 1 partition (pruning),
                                                    CSV must parse all 14.7 GB
                      Q2 "revenue by event type over all rows" -> no pruning possible; Iceberg
                                                    reads only 2 Parquet columns (columnar)
                    Runs interleaved (csv, iceberg, csv, iceberg ...) --runs times.

  --mode summarize  pure Python (no Spark): raw JSON lines -> ./results/benchmark_results.csv
                    with raw rows + summary rows (median T_p, S_p = T_1 / T_p, E_p = S_p / p,
                    storage speed-up = CSV / Iceberg).

Every Spark measurement appends one JSON line to --raw (default ./results/benchmark_raw.jsonl).
"""
import argparse
import csv
import json
import os
import statistics
import sys
import time
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "phase2"))


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def append_raw(path, rec):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(rec, default=str) + "\n")


# --------------------------------------------------------------------------- Spark helpers
def executor_hosts(spark):
    """(number of executors, {host: executors}) - the driver is not counted."""
    sc = spark.sparkContext
    jsc = sc._jsc.sc()
    try:
        n = int(jsc.getExecutorIds().size())
    except Exception:
        n = int(jsc.getExecutorMemoryStatus().size()) - 1
    hosts = {}
    try:
        for i in jsc.statusTracker().getExecutorInfos():
            hosts[i.host()] = hosts.get(i.host(), 0) + 1
        dh = sc.getConf().get("spark.driver.host", "")
        if hosts.get(dh):                       # the list includes the driver once
            hosts[dh] -= 1
            if hosts[dh] == 0:
                del hosts[dh]
    except Exception:
        hosts = {}
    return n, hosts


def wait_for_executors(spark, n, timeout=180):
    t0 = time.time()
    got, hosts = executor_hosts(spark)
    while got < n and time.time() - t0 < timeout:
        time.sleep(1)
        got, hosts = executor_hosts(spark)
    return got, hosts


def timed(fn):
    t0 = time.perf_counter()
    out = fn()
    return out, round(time.perf_counter() - t0, 3)


# --------------------------------------------------------------------------- scaling
def scaling(args):
    import common as C
    from pyspark.sql import functions as F

    spark = C.get_spark(f"phase4-bench-scaling-p{args.p}-run{args.run}")
    sc = spark.sparkContext
    n_exec, hosts = wait_for_executors(spark, args.p)
    C.log(f"  p={args.p} run={args.run}: {n_exec} executors on {hosts}, "
          f"shuffle.partitions={spark.conf.get('spark.sql.shuffle.partitions')}")

    ev = (spark.table(C.SILVER)
          .where((F.col("event_time") >= F.expr(f"TIMESTAMP '{args.start} 00:00:00'"))
                 & (F.col("event_time") < F.expr(f"TIMESTAMP '{args.end} 00:00:00'")))
          .where(F.col("price") > 0)
          .select("user_id", "user_session", "event_type", "price", "main_category", "event_time"))

    def sessions():
        is_t = lambda t: (F.col("event_type") == t).cast("long")  # noqa: E731
        s = (ev.groupBy("user_id", "user_session")
             .agg(F.count(F.lit(1)).alias("events"),
                  F.sum(is_t("view")).alias("views"),
                  F.sum(is_t("cart")).alias("carts"),
                  F.sum(is_t("purchase")).alias("purchases"),
                  F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0.0)).alias("revenue"),
                  (F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time")))
                  .alias("duration_s")))
        return s.agg(F.count(F.lit(1)).alias("sessions"),
                     F.sum("events").alias("events"),
                     F.round(F.avg("events"), 3).alias("avg_events"),
                     F.sum((F.col("purchases") > 0).cast("long")).alias("buying_sessions"),
                     F.round(F.avg("duration_s"), 1).alias("avg_duration_s"),
                     F.round(F.sum("revenue"), 2).alias("revenue")).first().asDict()

    def categories():
        return (ev.filter(F.col("event_type") == "purchase")
                .groupBy("main_category")
                .agg(F.round(F.sum("price"), 2).alias("revenue"), F.count(F.lit(1)).alias("purchases"))
                .collect())

    sc.setJobDescription(f"benchmark p={args.p}: session aggregation")
    sess, t_sess = timed(sessions)
    sc.setJobDescription(f"benchmark p={args.p}: category revenue")
    cats, t_cat = timed(categories)
    total = round(t_sess + t_cat, 3)

    rec = {"benchmark": "scaling", "config": f"p{args.p}", "p": args.p, "run": args.run,
           "seconds": total, "t_sessions": t_sess, "t_categories": t_cat,
           "executors": n_exec, "executor_hosts": hosts,
           "shuffle_partitions": int(spark.conf.get("spark.sql.shuffle.partitions")),
           "date_range": [args.start, args.end], "events": int(sess["events"]),
           "sessions": int(sess["sessions"]), "categories": len(cats),
           "warmup": args.warmup, "utc": now()}
    if not args.warmup:
        append_raw(args.raw, rec)
    print(f"RESULT scaling p={args.p} run={args.run} seconds={total} "
          f"(sessions {t_sess}s, categories {t_cat}s) events={sess['events']:,} "
          f"sessions={sess['sessions']:,} executors={n_exec} {'[warm-up, not recorded]' if args.warmup else ''}",
          flush=True)
    spark.stop()


# --------------------------------------------------------------------------- storage
RAW_COLS = [("event_time", "string"), ("event_type", "string"), ("product_id", "long"),
            ("category_id", "long"), ("category_code", "string"), ("brand", "string"),
            ("price", "double"), ("user_id", "long"), ("user_session", "string")]


def hdfs_size(spark, path):
    try:
        jvm = spark._jvm
        p = jvm.org.apache.hadoop.fs.Path(path)
        fs = p.getFileSystem(spark._jsc.hadoopConfiguration())
        return int(fs.getContentSummary(p).getLength())
    except Exception:
        return None


def storage(args):
    import common as C
    from pyspark.sql import functions as F

    spark = C.get_spark("phase4-bench-storage")
    n_exec, hosts = wait_for_executors(spark, args.expect_executors, timeout=90)
    C.log(f"  executors: {n_exec} on {hosts}")
    day = args.day
    nxt = (datetime.strptime(day, "%Y-%m-%d").date().toordinal() + 1)
    day_end = datetime.fromordinal(nxt).strftime("%Y-%m-%d")

    schema = ", ".join(f"{n} {t}" for n, t in RAW_COLS)
    csv_df = (spark.read.schema(schema).option("header", "true")
              .option("pathGlobFilter", "*.csv").csv(C.RAW_DIR))
    ice_df = spark.table(C.BRONZE)

    def agg(df):
        rows = (df.groupBy("event_type")
                .agg(F.count(F.lit(1)).alias("n"), F.sum("price").alias("revenue")).collect())
        return sorted((r["event_type"], int(r["n"]), round(float(r["revenue"] or 0), 2)) for r in rows)

    def q1_csv():
        return agg(csv_df.filter((F.col("event_type") == "purchase") & F.col("event_time").startswith(day)))

    def q1_ice():
        return agg(ice_df.filter((F.col("event_type") == "purchase")
                                 & (F.col("event_time") >= F.expr(f"TIMESTAMP '{day} 00:00:00'"))
                                 & (F.col("event_time") < F.expr(f"TIMESTAMP '{day_end} 00:00:00'"))))

    def q2(df):
        return lambda: agg(df)

    queries = {
        "q1_one_day_purchases": {"csv": q1_csv, "iceberg": q1_ice,
                                 "what": f"count + revenue of purchases on {day}"},
        "q2_revenue_by_event_type_full_scan": {"csv": q2(csv_df), "iceberg": q2(ice_df),
                                               "what": "count + revenue per event_type over all rows"},
    }

    sizes = {"csv_total_bytes": hdfs_size(spark, C.RAW_DIR)}
    try:
        sizes["iceberg_total_bytes"] = int(spark.sql(
            f"SELECT coalesce(sum(file_size_in_bytes),0) AS b FROM {C.BRONZE}.files WHERE content = 0"
        ).first()["b"])
        sizes["iceberg_q1_partition_bytes"] = int(spark.sql(f"""
            SELECT coalesce(sum(file_size_in_bytes),0) AS b FROM {C.BRONZE}.files
            WHERE content = 0 AND partition.event_time_day = DATE '{day}'
              AND partition.event_type = 'purchase'""").first()["b"])
    except Exception as exc:
        C.log(f"  (could not read Iceberg file sizes: {type(exc).__name__})")
    C.log(f"  sizes: {sizes}")

    answers = {}
    for run in range(1, args.runs + 1):
        for q, spec in queries.items():
            for fmt in ("csv", "iceberg"):
                spark.sparkContext.setJobDescription(f"storage {q} {fmt} run {run}")
                out, secs = timed(spec[fmt])
                answers.setdefault(q, {})[fmt] = out
                rec = {"benchmark": "storage", "config": f"{q}:{fmt}", "query": q, "format": fmt,
                       "run": run, "seconds": secs, "what": spec["what"], **sizes,
                       "executors": n_exec, "utc": now()}
                append_raw(args.raw, rec)
                print(f"RESULT storage {q} {fmt} run={run} seconds={secs}", flush=True)

    def same_result(x, y):   # counts equal, revenue equal up to floating-point summation order
        return (x is not None and y is not None and len(x) == len(y)
                and all(a[0] == b[0] and a[1] == b[1] and abs(a[2] - b[2]) <= 1.0 for a, b in zip(x, y)))

    for q, a in answers.items():
        same = same_result(a.get("csv"), a.get("iceberg"))
        print(f"[{'PASS' if same else 'CHECK'}] {q}: CSV and Iceberg give "
              f"{'the same' if same else 'DIFFERENT'} result\n    csv     {a.get('csv')}\n    iceberg {a.get('iceberg')}")
    spark.stop()


# --------------------------------------------------------------------------- summarize (no Spark)
def summarize(args):
    if not os.path.exists(args.raw):
        sys.exit(f"no raw results at {args.raw}")
    recs = [json.loads(line) for line in open(args.raw) if line.strip()]
    rows = []
    for r in recs:
        rows.append({"record_type": "raw", "benchmark": r["benchmark"], "config": r["config"],
                     "p": r.get("p", ""), "run": r["run"], "seconds": r["seconds"],
                     "median_seconds": "", "speedup": "", "efficiency": "",
                     "detail": json.dumps({k: v for k, v in r.items()
                                           if k not in ("benchmark", "config", "p", "run", "seconds")},
                                          default=str)})

    # scaling: median T_p, S_p = T_1 / T_p, E_p = S_p / p
    sc = {}
    for r in recs:
        if r["benchmark"] == "scaling":
            sc.setdefault(int(r["p"]), []).append(float(r["seconds"]))
    if sc:
        # baseline = smallest p (normally p = 1); if p = 1 is missing, S_p is scaled so S_base = base
        base = min(sc)
        t1 = statistics.median(sc[base]) * base
        print("\nScalability (median of runs)")
        print(f"  {'p':>3} {'runs':>4} {'T_p [s]':>9} {'S_p':>6} {'E_p':>6}")
        for p in sorted(sc):
            tp = statistics.median(sc[p])
            s, e = t1 / tp, (t1 / tp) / p
            rows.append({"record_type": "summary", "benchmark": "scaling", "config": f"p{p}", "p": p,
                         "run": len(sc[p]), "seconds": "", "median_seconds": round(tp, 3),
                         "speedup": round(s, 3), "efficiency": round(e, 3),
                         "detail": json.dumps({"runs_seconds": sc[p], "min": min(sc[p]), "max": max(sc[p])})})
            print(f"  {p:>3} {len(sc[p]):>4} {tp:>9.1f} {s:>6.2f} {e:>6.2f}")

    # storage: median per (query, format), speed-up = CSV / Iceberg
    st = {}
    for r in recs:
        if r["benchmark"] == "storage":
            st.setdefault(r["query"], {}).setdefault(r["format"], []).append(float(r["seconds"]))
    if st:
        print("\nStorage format (median of runs)")
        for q, fm in st.items():
            med = {f: statistics.median(v) for f, v in fm.items()}
            ratio = med["csv"] / med["iceberg"] if med.get("iceberg") else None
            for f, v in fm.items():
                rows.append({"record_type": "summary", "benchmark": "storage", "config": f"{q}:{f}", "p": "",
                             "run": len(v), "seconds": "", "median_seconds": round(med[f], 3),
                             "speedup": round(ratio, 2) if (f == "iceberg" and ratio) else "",
                             "efficiency": "", "detail": json.dumps({"runs_seconds": v})})
            print(f"  {q:<40} CSV {med.get('csv', 0):8.1f}s   Iceberg {med.get('iceberg', 0):7.1f}s   "
                  f"-> Iceberg {ratio:.1f}x faster" if ratio else f"  {q}: incomplete")

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    cols = ["record_type", "benchmark", "config", "p", "run", "seconds",
            "median_seconds", "speedup", "efficiency", "detail"]
    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)
    print(f"\nwrote {args.out} ({len(rows)} rows)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["scaling", "storage", "summarize"], required=True)
    ap.add_argument("--p", type=int, default=1)
    ap.add_argument("--run", type=int, default=1)
    ap.add_argument("--warmup", action="store_true")
    ap.add_argument("--start", default="2019-11-01")
    ap.add_argument("--end", default="2019-11-15", help="exclusive")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--day", default="2019-11-17")
    ap.add_argument("--expect-executors", type=int, default=5)
    ap.add_argument("--raw", default=os.path.join(os.environ.get("RESULTS_DIR", "./results"), "benchmark_raw.jsonl"))
    ap.add_argument("--out", default=os.path.join(os.environ.get("RESULTS_DIR", "./results"), "benchmark_results.csv"))
    args = ap.parse_args()
    {"scaling": scaling, "storage": storage, "summarize": summarize}[args.mode](args)


if __name__ == "__main__":
    main()
