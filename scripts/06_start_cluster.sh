#!/usr/bin/env bash
# =====================================================================
# 06_start_cluster.sh — run on MASTER. Starts, in order:
#   HDFS (NameNode, SecondaryNameNode, 3 DataNodes)
#   YARN (ResourceManager, 3 NodeManagers)
#   Spark Standalone (Master, 3 Workers) + Spark History Server
# then creates the base HDFS directories and checks every node is alive.
# Safe to re-run: daemons that are already up are left running.
# =====================================================================
source "$(dirname "$0")/lib.sh"
require_cluster_env
require_host master

log "Starting HDFS"
start-dfs.sh || warn "start-dfs.sh reported an error (often: a daemon is already running) — checking state"
log "Waiting for NameNode to leave safe mode"
hdfs dfsadmin -safemode wait

log "Waiting for 3 live DataNodes"
for _ in $(seq 1 30); do
  live="$(hdfs dfsadmin -report -live 2>/dev/null | sed -nE 's/^Live datanodes \(([0-9]+)\):.*/\1/p')"
  [[ "${live:-0}" -ge 3 ]] && break
  sleep 2
done
[[ "${live:-0}" -ge 3 ]] || die "Only ${live:-0}/3 DataNodes are live. See README §4 (clusterID / firewall / 127.0.1.1)."
ok "HDFS up: $live live DataNodes"

log "Starting YARN"
start-yarn.sh || warn "start-yarn.sh reported an error (often: a daemon is already running)"

log "Creating base HDFS directories"
hdfs dfs -mkdir -p "$HDFS_RAW_DIR" "$HDFS_META_DIR" "$HDFS_WAREHOUSE" "$HDFS_SPARK_LOGS" /tmp
hdfs dfs -chmod 1777 /tmp "$HDFS_SPARK_LOGS"

log "Starting Spark Standalone master + workers + history server"
"$SPARK_HOME/sbin/start-master.sh"         || warn "Spark master may already be running"
"$SPARK_HOME/sbin/start-workers.sh"        || warn "some Spark workers may already be running"
"$SPARK_HOME/sbin/start-history-server.sh" || warn "history server may already be running"

sleep 8
"$PROJECT_DIR/scripts/cluster_status.sh"
