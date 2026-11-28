#!/usr/bin/env bash
# Phase 3 runner: Spark MLlib jobs on the Spark Standalone cluster.
#
#   ./scripts/12_run_phase3.sh             # full: LogReg on all users, ALS on ALS_SAMPLE of users
#   ./scripts/12_run_phase3.sh --dev       # quick test: 5 % / 2 % of users, *_dev tables, ./results_dev
#   ./scripts/12_run_phase3.sh 6           # only the recommender (full)
#   ALS_SAMPLE=0.5 ./scripts/12_run_phase3.sh 6   # ALS on 50 % of users
#   ./scripts/12_run_phase3.sh 5b          # name the coefficients of the saved LogReg model (no retraining)
#   ALS_EXTRA="--reuse" ./scripts/12_run_phase3.sh 6   # reuse saved ALS model + interactions (skip training)
#
# Reads local.ecommerce.silver_events (Phase 2). Run on master from the project root.
set -euo pipefail
cd "$(dirname "$0")/.."

MODE=full
if [[ "${1:-}" == "--dev" ]]; then MODE=dev; shift; fi
STEPS=("$@"); [[ ${#STEPS[@]} -eq 0 ]] && STEPS=(5 6)

export LAKE_NS=ecommerce EVIDENCE_DIR=/bigdata/evidence/phase3
if [[ $MODE == dev ]]; then
  export RESULTS_DIR=./results_dev
  LR_ARGS=(--sample 0.05 --tag _dev)
  TAG_ARGS=(--tag _dev)
  ALS_ARGS=(--sample 0.02 --tag _dev ${ALS_EXTRA:-})
else
  export RESULTS_DIR=./results
  LR_ARGS=(--sample 1.0)
  TAG_ARGS=()
  ALS_ARGS=(--sample "${ALS_SAMPLE:-0.2}" ${ALS_EXTRA:-})
fi

SPARK_SUBMIT="${SPARK_HOME:-/opt/spark}/bin/spark-submit"
LOG_DIR=$EVIDENCE_DIR/logs
mkdir -p "$LOG_DIR"

submit() {   # submit <job-file> [args...]
  local job=$1; shift
  local log="$LOG_DIR/${job%.py}_${MODE}_$(date +%Y%m%d_%H%M%S).log"
  echo
  echo "################  $job  ($MODE: $*)  ################"
  local t0=$SECONDS
  "$SPARK_SUBMIT" \
    --master spark://master:7077 \
    --name "phase3-${job%.py}" \
    --py-files jobs/phase2/common.py \
    "jobs/phase3/$job" "$@" 2>&1 | tee "$log"
  echo "################  $job finished in $((SECONDS - t0)) s  -> log: $log"
}

for s in "${STEPS[@]}"; do
  case $s in
    5) submit 05_ml_purchase_prediction.py "${LR_ARGS[@]}" ;;
    5b) submit 05b_name_coefficients.py "${TAG_ARGS[@]}" ;;
    6) submit 06_ml_recommender_als.py "${ALS_ARGS[@]}" ;;
    *) echo "unknown step $s (use 5, 5b or 6)"; exit 1 ;;
  esac
done

echo
echo "[ OK ] Phase 3 ($MODE) done. Evidence: $EVIDENCE_DIR   Results: $RESULTS_DIR"
ls -lh "$RESULTS_DIR" 2>/dev/null || true
