#!/usr/bin/env bash
# stop_cluster.sh — run on MASTER. Stops Spark, YARN, HDFS (reverse start order).
source "$(dirname "$0")/lib.sh"
require_host master

"$SPARK_HOME/sbin/stop-history-server.sh" || true
"$SPARK_HOME/sbin/stop-workers.sh"        || true
"$SPARK_HOME/sbin/stop-master.sh"         || true
stop-yarn.sh || true
stop-dfs.sh  || true
ok "Cluster stopped"
