#!/usr/bin/env bash
# Live Iceberg demo (Phase 4 / job 9). Run on master from anywhere.
set -euo pipefail
cd "$(dirname "$0")/../.."
export LAKE_NS=ecommerce
"${SPARK_HOME:-/opt/spark}/bin/spark-submit" \
  --master spark://master:7077 --name live-iceberg-demo \
  --conf spark.ui.showConsoleProgress=false \
  --py-files jobs/phase2/common.py jobs/phase4/09_demo_iceberg.py 2>&1 \
  | grep -v -E "^(WARNING: Using incubator|Using Spark's default log4j)| (INFO|WARN) "
