#!/usr/bin/env bash
# =====================================================================
# 10_run_smoke_test.sh — run on MASTER. End-of-Phase-1 acceptance test.
#   ./scripts/10_run_smoke_test.sh          Spark Standalone + HDFS + Iceberg
#   ./scripts/10_run_smoke_test.sh --yarn   also run the MapReduce "pi" job on YARN
# =====================================================================
source "$(dirname "$0")/lib.sh"
require_host master

if [[ "${1:-}" == "--yarn" ]]; then
  log "YARN check: MapReduce pi on 3 NodeManagers"
  yarn jar "$HADOOP_HOME"/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar pi 3 1000
fi

log "Spark + HDFS + Iceberg smoke test (full scan of ~110M rows: a few minutes)"
spark-submit \
  --master spark://master:7077 \
  --name phase1-smoke-test \
  "$PROJECT_DIR/jobs/phase1_smoke_test.py" \
  --raw "$HDFS_RAW_DIR/*.csv" \
  --manifest "$HDFS_META_DIR/manifest.csv" \
  --evidence-dir "$EVIDENCE_DIR"
