#!/usr/bin/env bash
# Phase 4 - reproducible benchmarks (Task 6). Run on master from the project root.
#
#   ./jobs/phase4/07_run_benchmark.sh              # warm-up + scaling (p = 1 2 4 8, 3 runs) + storage + summary
#   ./jobs/phase4/07_run_benchmark.sh scaling      # only the scalability experiment
#   ./jobs/phase4/07_run_benchmark.sh storage      # only CSV vs Iceberg
#   ./jobs/phase4/07_run_benchmark.sh summarize    # only rebuild results/benchmark_results.csv
#   FRESH=1 ./jobs/phase4/07_run_benchmark.sh      # delete earlier raw measurements first
#
# Settings (environment): RUNS=3  PS="1 2 4 8"  START=2019-11-01  END=2019-11-15 (exclusive)
#
# Fairness rules of the scalability experiment
#   * --total-executor-cores p, 1-core executors with 700 MB each -> the SAME memory per core
#     for every p (master fits 2 such executors, each worker 4 -> up to 10 cores)
#   * spark.sql.shuffle.partitions = 4 * p, AQE partition coalescing off (fixed task count)
#   * one fresh spark-submit per measurement; timing starts after all p executors registered
#   * a warm-up run first (not recorded), then the runs interleaved: run1 p1 p2 p4 p8, run2 ...
#   * HDFS replication = 3, so every laptop holds every block: data locality does not depend on p
set -euo pipefail
cd "$(dirname "$0")/../.."

WHAT=${1:-all}
RUNS=${RUNS:-3}
read -r -a PS <<< "${PS:-1 2 4 8}"
START=${START:-2019-11-01}
END=${END:-2019-11-15}

export LAKE_NS=ecommerce RESULTS_DIR=./results EVIDENCE_DIR=/bigdata/evidence/phase4
RAW=$RESULTS_DIR/benchmark_raw.jsonl
OUT=$RESULTS_DIR/benchmark_results.csv
LOG_DIR=$EVIDENCE_DIR/logs
JOB=jobs/phase4/07_benchmark_workload.py
SPARK_SUBMIT="${SPARK_HOME:-/opt/spark}/bin/spark-submit"
mkdir -p "$LOG_DIR" "$RESULTS_DIR"
[[ "${FRESH:-0}" == 1 ]] && rm -f "$RAW" && echo "removed old $RAW"

run_spark() {   # run_spark <log-name> <spark-submit args...>
  local name=$1; shift
  local log="$LOG_DIR/${name}_$(date +%Y%m%d_%H%M%S).log"
  if ! "$SPARK_SUBMIT" --master spark://master:7077 --py-files jobs/phase2/common.py "$@" > "$log" 2>&1; then
    echo "[FAIL] $name - last lines of $log:"; tail -40 "$log"; exit 1
  fi
  grep -E "^RESULT|^\[PASS\]|^\[CHECK\]|^    (csv|iceberg)" "$log" || true
}

scaling_one() {   # scaling_one <p> <run> [--warmup]
  local p=$1 run=$2; shift 2
  run_spark "scaling_p${p}_run${run}" \
    --name "bench-scaling-p${p}-run${run}" \
    --total-executor-cores "$p" \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=700m \
    --conf spark.sql.shuffle.partitions=$((4 * p)) \
    --conf spark.sql.adaptive.coalescePartitions.enabled=false \
    --conf spark.dynamicAllocation.enabled=false \
    "$JOB" --mode scaling --p "$p" --run "$run" --start "$START" --end "$END" --raw "$RAW" "$@"
}

do_scaling() {
  echo "=== Scalability: p = ${PS[*]}, $RUNS runs each, events $START .. $END (exclusive)"
  local t0=$SECONDS
  scaling_one 8 0 --warmup
  for run in $(seq 1 "$RUNS"); do
    for p in "${PS[@]}"; do scaling_one "$p" "$run"; done
  done
  echo "=== scaling done in $(( (SECONDS - t0) / 60 )) min"
}

do_storage() {
  echo "=== Storage: raw CSV vs Iceberg (bronze_events), $RUNS runs, all executors"
  local t0=$SECONDS
  run_spark storage --name bench-storage "$JOB" --mode storage --runs "$RUNS" --raw "$RAW"
  echo "=== storage done in $(( (SECONDS - t0) / 60 )) min"
}

do_summary() {
  python3 "$JOB" --mode summarize --raw "$RAW" --out "$OUT"
}

case $WHAT in
  all)       do_scaling; do_storage; do_summary ;;
  scaling)   do_scaling; do_summary ;;
  storage)   do_storage; do_summary ;;
  summarize) do_summary ;;
  *) echo "usage: $0 [all|scaling|storage|summarize]"; exit 1 ;;
esac
