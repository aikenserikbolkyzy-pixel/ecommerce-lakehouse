#!/usr/bin/env bash
# =====================================================================
# cluster_status.sh — run on MASTER any time (good first slide of the demo).
# Shows the Java daemons on every node and checks HDFS, YARN and Spark.
# =====================================================================
source "$(dirname "$0")/lib.sh"
require_host master
fail=0

log "Java daemons per node (jps)"
for n in "${ALL_NODES[@]}"; do
  echo "--- $n"
  # jps lives next to the node's own java binary (needs the JDK, not only the JRE)
  ssh "$n" 'J=$(dirname "$(readlink -f "$(command -v java)")"); "$J/jps" 2>/dev/null || echo "0 jps-missing:install-openjdk-jdk-headless"' \
    | { grep -v -E ' Jps$|^$' || true; } | sort -k2 | sed 's/^/    /'
done

live="$(hdfs dfsadmin -report -live 2>/dev/null | sed -nE 's/^Live datanodes \(([0-9]+)\):.*/\1/p')"
if [[ "${live:-0}" -eq 3 ]]; then ok "HDFS: 3/3 DataNodes live"; else warn "HDFS: ${live:-0}/3 DataNodes live"; fail=1; fi

ynodes="$(yarn node -list 2>/dev/null | sed -nE 's/.*Total Nodes:([0-9]+).*/\1/p')"
if [[ "${ynodes:-0}" -eq 3 ]]; then ok "YARN: 3/3 NodeManagers running"; else warn "YARN: ${ynodes:-0}/3 NodeManagers"; fail=1; fi

sworkers="$(curl -s --max-time 5 http://master:8080/json/ \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(sum(w["state"]=="ALIVE" for w in d["workers"]), d["cores"])' 2>/dev/null || echo "0 0")"
read -r alive cores <<<"$sworkers"
if [[ "$alive" -eq 3 ]]; then ok "Spark: 3/3 workers ALIVE, $cores cores total"; else warn "Spark: $alive/3 workers ALIVE"; fail=1; fi

cat <<EOF

Web UIs (open from any of the 3 laptops):
  HDFS NameNode      http://master:9870
  YARN RM            http://master:8088
  Spark Master       http://master:8080
  Spark History      http://master:18080
  Running app (UI)   http://master:4040
EOF
exit "$fail"
