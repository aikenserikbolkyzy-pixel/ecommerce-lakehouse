#!/usr/bin/env bash
# =====================================================================
# 05_format_namenode.sh — run ONCE on MASTER, before the first start.
#   ./scripts/05_format_namenode.sh           first-time format (refuses if already formatted)
#   ./scripts/05_format_namenode.sh --force   wipe ALL HDFS data on all 3 nodes and re-format
# Why --force wipes DataNodes too: a re-formatted NameNode gets a new
# clusterID, and old DataNode dirs then fail with "Incompatible clusterIDs".
# =====================================================================
source "$(dirname "$0")/lib.sh"
require_cluster_env
require_host master

NN_DIR="$LOCAL_ROOT/hadoop/hdfs/namenode"
DN_DIR="$LOCAL_ROOT/hadoop/hdfs/datanode"

if [[ -f "$NN_DIR/current/VERSION" ]]; then
  if [[ "${1:-}" != "--force" ]]; then
    die "NameNode already formatted ($NN_DIR/current/VERSION). Use --force to wipe HDFS and start over."
  fi
  warn "This DELETES every file in HDFS (raw data + Iceberg tables) on all 3 nodes."
  read -r -p "Type WIPE to continue: " answer
  [[ "$answer" == "WIPE" ]] || die "aborted"
  "$PROJECT_DIR/scripts/stop_cluster.sh" || true
  rm -rf "${NN_DIR:?}"/* "$LOCAL_ROOT"/hadoop/hdfs/namesecondary/* "$LOCAL_ROOT"/hadoop/tmp/*
  for n in "${ALL_NODES[@]}"; do
    ssh "$n" "rm -rf ${DN_DIR:?}/* $LOCAL_ROOT/hadoop/tmp/*" && ok "wiped DataNode dir on $n"
  done
fi

log "Formatting NameNode"
hdfs namenode -format -nonInteractive -clusterId "CID-ecommerce-lakehouse"
ok "NameNode formatted: $(grep clusterID "$NN_DIR/current/VERSION")"
