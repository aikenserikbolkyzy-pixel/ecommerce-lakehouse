#!/usr/bin/env bash
# Phase 2 runner: Bronze -> Silver -> Iceberg features -> Gold, on the Spark Standalone cluster.
#
#   ./scripts/11_run_phase2.sh             # full data, namespace local.ecommerce, CSVs in ./results
#   ./scripts/11_run_phase2.sh --dev       # 1% sample, namespace local.ecommerce_dev, ./results_dev
#   ./scripts/11_run_phase2.sh 3 4         # only jobs 3 and 4 (full)
#   ./scripts/11_run_phase2.sh --dev 2     # only job 2 (dev)
#
# Run on master from the project root. Logs: /bigdata/evidence/phase2/logs/
set -euo pipefail
cd "$(dirname "$0")/.."

MODE=full
if [[ "${1:-}" == "--dev" ]]; then MODE=dev; shift; fi
STEPS=("$@"); [[ ${#STEPS[@]} -eq 0 ]] && STEPS=(1 2 3 4)

if [[ $MODE == dev ]]; then
  export LAKE_NS=ecommerce_dev RESULTS_DIR=./results_dev
  SAMPLE=0.01
else
  export LAKE_NS=ecommerce RESULTS_DIR=./results
  SAMPLE=1.0
fi
export EVIDENCE_DIR=/bigdata/evidence/phase2

SPARK_SUBMIT="${SPARK_HOME:-/opt/spark}/bin/spark-submit"
JOBS=jobs/phase2
LOG_DIR=$EVIDENCE_DIR/logs
mkdir -p "$LOG_DIR"

submit() {   # submit <job-file> [args...]
  local job=$1; shift
  local log="$LOG_DIR/${job%.py}_${LAKE_NS}_$(date +%Y%m%d_%H%M%S).log"
  echo
  echo "################  $job  ($MODE, namespace local.$LAKE_NS)  ################"
  local t0=$SECONDS
  "$SPARK_SUBMIT" \
    --master spark://master:7077 \
    --name "phase2-${job%.py}" \
    --py-files "$JOBS/common.py" \
    "$JOBS/$job" "$@" 2>&1 | tee "$log"
  echo "################  $job finished in $((SECONDS - t0)) s  -> log: $log"
}

for s in "${STEPS[@]}"; do
  case $s in
    1) submit 01_ingest_bronze.py --sample "$SAMPLE" ;;
    2) submit 02_clean_silver.py ;;
    3) submit 03_iceberg_features_demo.py ;;
    4) submit 04_gold_aggregations.py ;;
    *) echo "unknown step $s (use 1-4)"; exit 1 ;;
  esac
done

echo
echo "[ OK ] Phase 2 ($MODE) done. Evidence: $EVIDENCE_DIR   CSVs: $RESULTS_DIR"
ls -lh "$RESULTS_DIR" 2>/dev/null || true
