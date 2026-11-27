#!/usr/bin/env bash
# =====================================================================
# 09_verify_hdfs.sh — run on MASTER. Proves the raw data is distributed:
#   * 3 live DataNodes                       (hdfs dfsadmin -report)
#   * replication = 3, block size = 128 MB   (hdfs getconf, hdfs dfs -stat)
#   * HEALTHY, 0 under-replicated/missing    (hdfs fsck -files -blocks -locations)
#   * blocks per file = ceil(size / 128 MB)
#   * every DataNode holds a replica of every block
# Saves the full outputs to $EVIDENCE_DIR for the report and exits non-zero
# if any check fails.
# =====================================================================
source "$(dirname "$0")/lib.sh"
require_host master

TS="$(date +%Y%m%d_%H%M%S)"
OUT="$EVIDENCE_DIR/hdfs_verify_$TS"
mkdir -p "$OUT"
pass=0; fail=0
check() {   # check "<message>" <test command...>   (never aborts the script)
  local msg="$1"; shift
  if "$@"; then ok "$msg"; pass=$((pass+1)); else warn "FAILED: $msg"; fail=$((fail+1)); fi
}

# ---------- 1. Cluster report ----------
log "hdfs dfsadmin -report"
hdfs dfsadmin -report > "$OUT/dfsadmin_report.txt"
sed -n '1,/^-------------------------------------------------$/p' "$OUT/dfsadmin_report.txt" | head -20
live="$(sed -nE 's/^Live datanodes \(([0-9]+)\):.*/\1/p' "$OUT/dfsadmin_report.txt")"
check "live DataNodes = ${live:-0} (expected 3)" [ "${live:-0}" -eq 3 ]
echo "  Per DataNode:"
awk '/^Name:/{n=$2} /^Hostname:/{h=$2} /^DFS Used:/{u=$0; sub(/^DFS Used: /,"",u)} /^DFS Remaining%:/{printf "    %-10s %-22s used %-28s remaining %s\n", h, n, u, $3}' \
  "$OUT/dfsadmin_report.txt"

# ---------- 2. Effective configuration ----------
rep_conf="$(hdfs getconf -confKey dfs.replication)"
bs_conf="$(hdfs getconf -confKey dfs.blocksize)"
check "dfs.replication = $rep_conf"            [ "$rep_conf" = "3" ]
check "dfs.blocksize   = $bs_conf (128 MB)"  [ "$bs_conf" = "134217728" ]

# ---------- 3. Per-file metadata ----------
log "hdfs dfs -stat (name, replication, block size, bytes)"
hdfs dfs -stat "%n  replication=%r  blocksize=%o  bytes=%b" "$HDFS_RAW_DIR/*.csv" | tee "$OUT/stat.txt"
hdfs dfs -cat "$HDFS_META_DIR/manifest.csv" > "$OUT/manifest.csv"

# ---------- 4. fsck with block locations ----------
log "hdfs fsck $HDFS_RAW_DIR/ -files -blocks -locations"
hdfs fsck "$HDFS_RAW_DIR/" -files -blocks -locations > "$OUT/fsck_files_blocks_locations.txt" 2>&1 || true
F="$OUT/fsck_files_blocks_locations.txt"
echo "  First block lines:"; { grep -m3 -E '^[0-9]+\. BP-' "$F" || true; } | cut -c1-200 | sed 's/^/    /'
echo "  Summary:"
{ grep -E 'Total size|Total files|Total blocks|Minimally replicated|Under-replicated|Mis-replicated|Default replication|Average block replication|Missing blocks|Corrupt blocks|Number of data-nodes' "$F" || true; } | sed 's/^/    /'

check "fsck status HEALTHY" grep -q "is HEALTHY" "$F"
avg="$(sed -nE 's/.*Average block replication:[[:space:]]*([0-9.]+).*/\1/p' "$F" | head -1)"
check "average block replication = ${avg:-?}" [ "${avg:-0}" = "3.0" ]
under="$(sed -nE 's/.*Under-replicated blocks:[[:space:]]*([0-9]+).*/\1/p' "$F" | head -1)"
check "under-replicated blocks = ${under:-?}" [ "${under:-1}" = "0" ]
missing="$(sed -nE 's/.*Missing blocks:[[:space:]]*([0-9]+).*/\1/p' "$F" | head -1)"
check "missing blocks = ${missing:-?}" [ "${missing:-1}" = "0" ]

# ---------- 5. Per-file block counts vs. expectation ----------
log "Blocks per file"
total_blocks=0
for f in "${DATA_FILES[@]}"; do
  bytes="$(hdfs dfs -stat %b "$HDFS_RAW_DIR/$f")"
  rep="$(hdfs dfs -stat %r "$HDFS_RAW_DIR/$f")"
  bsz="$(hdfs dfs -stat %o "$HDFS_RAW_DIR/$f")"
  mbytes="$(awk -F, -v f="$f" '$1==f{print $2}' "$OUT/manifest.csv")"
  expected=$(( (bytes + BLOCK_SIZE - 1) / BLOCK_SIZE ))
  actual="$(grep -E "^$HDFS_RAW_DIR/$f " "$F" | grep -oE '[0-9]+ block\(s\)' | grep -oE '^[0-9]+' || true)"
  total_blocks=$(( total_blocks + ${actual:-0} ))
  printf '    %-14s %6s MB  expected %3d blocks  fsck %3s blocks  replication %s\n' \
    "$f" "$((bytes/1048576))" "$expected" "${actual:-?}" "$rep"
  check "$f size in HDFS equals local manifest ($bytes bytes)" [ "$bytes" = "${mbytes:-x}" ]
  check "$f replication=3, blocksize=128MB" [ "$rep" = "3" -a "$bsz" = "134217728" ]
  check "$f has $expected blocks"          [ "${actual:-0}" = "$expected" ]
done

# ---------- 6. Replica placement per DataNode ----------
log "Block replicas stored on each DataNode"
{ grep -oE 'DatanodeInfoWithStorage\[[0-9.]+' "$F" || true; } | sed 's/.*\[//' | sort | uniq -c > "$OUT/replicas_per_datanode.txt"
nodes_ok=0
while read -r cnt ip; do
  host="$(getent hosts "$ip" | awk '{print $2}' || true)"
  printf '    %-10s %-16s %4d replicas\n' "${host:-?}" "$ip" "$cnt"
  if [ "$cnt" = "$total_blocks" ]; then nodes_ok=$((nodes_ok+1)); fi
done < "$OUT/replicas_per_datanode.txt"
check "each of the 3 DataNodes holds all $total_blocks blocks (3 x $total_blocks = $((3*total_blocks)) replicas)" \
  [ "$nodes_ok" -eq 3 -a "$total_blocks" -gt 0 ]

# ---------- Result ----------
echo
log "Evidence saved in $OUT"
ls -1 "$OUT" | sed 's/^/    /'
if (( fail == 0 )); then ok "ALL $pass CHECKS PASSED"; else die "$fail check(s) failed, $pass passed"; fi
