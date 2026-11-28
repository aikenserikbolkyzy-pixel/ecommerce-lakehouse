"""
Phase 4 / Job 7b - WHY the speed-up looks the way it does: shuffle traffic per benchmark run.

Plain Python (standard library only, no Spark). Reads the Spark History Server REST API
(event logs of every benchmark application) and, for each scaling run, sums over its stages:
  shuffle write bytes, shuffle bytes read LOCALLY vs over the NETWORK (other laptops),
  shuffle fetch wait time (tasks waiting for remote blocks), executor run time,
plus the laptop each executor ran on.

    python3 jobs/phase4/07b_shuffle_analysis.py [--history http://localhost:18080]

Writes ./results/benchmark_shuffle.csv (one row per run + one summary row per p) and prints a table.
Chart 11 of 08_visualize_results.py is drawn from it.
"""
import argparse
import csv
import json
import os
import re
import statistics
import urllib.request

NAME = re.compile(r"bench-scaling-p(\d+)-run(\d+)")


def get(url):
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.loads(r.read().decode())


def app_base(history, app):
    att = (app.get("attempts") or [{}])[0]
    base = f"{history}/api/v1/applications/{app['id']}"
    return f"{base}/{att['attemptId']}" if att.get("attemptId") else base


def analyse(history, app):
    base = app_base(history, app)
    stages = get(f"{base}/stages?status=complete")
    execs = [e for e in get(f"{base}/allexecutors") if e.get("id") != "driver"]
    s = lambda k: sum(int(st.get(k) or 0) for st in stages)  # noqa: E731
    remote, local = s("shuffleRemoteBytesRead"), s("shuffleLocalBytesRead")
    hosts = {}
    for e in execs:
        h = e.get("hostPort", "?").rsplit(":", 1)[0]
        hosts[h] = hosts.get(h, 0) + 1
    return {
        "shuffle_write_mb": round(s("shuffleWriteBytes") / 2**20, 1),
        "shuffle_read_local_mb": round(local / 2**20, 1),
        "shuffle_read_remote_mb": round(remote / 2**20, 1),
        "remote_share": round(remote / (remote + local), 3) if remote + local else 0.0,
        "fetch_wait_s": round(s("shuffleFetchWaitTime") / 1000, 1),
        "executor_run_s": round(s("executorRunTime") / 1000, 1),
        "spill_disk_mb": round(s("diskBytesSpilled") / 2**20, 1),
        "executor_hosts": json.dumps(hosts, sort_keys=True),
        "laptops_used": len(hosts),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--history", default="http://localhost:18080")
    ap.add_argument("--out", default=os.path.join(os.environ.get("RESULTS_DIR", "./results"),
                                                  "benchmark_shuffle.csv"))
    a = ap.parse_args()

    apps = get(f"{a.history}/api/v1/applications?limit=1000")
    rows = []
    for app in apps:
        m = NAME.search(app.get("name", ""))
        if not m or int(m.group(2)) == 0:          # run 0 = warm-up
            continue
        att = (app.get("attempts") or [{}])[0]
        if not att.get("completed", True):
            continue
        p, run = int(m.group(1)), int(m.group(2))
        try:
            r = analyse(a.history, app)
        except Exception as exc:
            print(f"  skip {app['id']} ({app['name']}): {exc}")
            continue
        rows.append({"record_type": "raw", "p": p, "run": run, "app_id": app["id"],
                     "app_seconds": round(int(att.get("duration") or 0) / 1000, 1), **r})

    if not rows:
        raise SystemExit("no benchmark applications found in the History Server "
                         "(is it running on :18080? were the runs started with 07_run_benchmark.sh?)")

    # if the benchmark was run more than once, keep the newest app per (p, run)
    newest = {}
    for r in sorted(rows, key=lambda r: r["app_id"]):
        newest[(r["p"], r["run"])] = r
    rows = sorted(newest.values(), key=lambda r: (r["p"], r["run"]))

    num = ["app_seconds", "shuffle_write_mb", "shuffle_read_local_mb", "shuffle_read_remote_mb",
           "remote_share", "fetch_wait_s", "executor_run_s", "spill_disk_mb", "laptops_used"]
    summary = []
    for p in sorted({r["p"] for r in rows}):
        rs = [r for r in rows if r["p"] == p]
        summary.append({"record_type": "summary", "p": p, "run": len(rs), "app_id": "", "executor_hosts": "",
                        **{k: round(statistics.median(float(r[k]) for r in rs), 3) for k in num}})

    cols = ["record_type", "p", "run", "app_id", *num, "executor_hosts"]
    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    with open(a.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows + summary)

    print(f"\n{'p':>2} {'run':>3} {'laptops':>7} {'remote MB':>10} {'local MB':>9} {'remote':>7} "
          f"{'fetch wait':>10} {'spill MB':>9}  executors per laptop")
    for r in rows:
        print(f"{r['p']:>2} {r['run']:>3} {r['laptops_used']:>7} {r['shuffle_read_remote_mb']:>10} "
              f"{r['shuffle_read_local_mb']:>9} {r['remote_share']:>7.0%} {r['fetch_wait_s']:>9}s "
              f"{r['spill_disk_mb']:>9}  {r['executor_hosts']}")
    print("\nmedian per p:")
    for s in summary:
        print(f"  p={s['p']}: {s['remote_share']:.0%} of shuffle data over Wi-Fi, "
              f"fetch wait {s['fetch_wait_s']} s, spill {s['spill_disk_mb']} MB, {s['laptops_used']:.0f} laptop(s)")
    print(f"\nwrote {a.out}")


if __name__ == "__main__":
    main()
