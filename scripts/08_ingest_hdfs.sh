#!/usr/bin/env bash
# =====================================================================
# 08_ingest_hdfs.sh — run on MASTER. Uploads the staged CSVs into the HDFS
# raw zone with replication=3 and 128 MB blocks, waits until every block
# has 3 replicas, and records timing/throughput for the report.
# Idempotent: a file already in HDFS with the same size is skipped.
# Usage: ./scripts/08_ingest_hdfs.sh
# =====================================================================
source "$(dirname "$0")/lib.sh"
require_host master

MANIFEST="$STAGING_DIR/manifest.csv"
[[ -f "$MANIFEST" ]] || die "Run 07_stage_dataset.sh first (no $MANIFEST)"
LOG_CSV="$EVIDENCE_DIR/ingest_log.csv"
mkdir -p "$EVIDENCE_DIR"
[[ -f "$LOG_CSV" ]] || echo "timestamp_utc,file,bytes,seconds,mb_per_s,replication,blocksize" > "$LOG_CSV"

hdfs dfs -mkdir -p "$HDFS_RAW_DIR" "$HDFS_META_DIR"

for f in "${DATA_FILES[@]}"; do
  src="$STAGING_DIR/$f"
  dst="$HDFS_RAW_DIR/$f"
  bytes=$(stat -c %s "$src")

  if hdfs dfs -test -e "$dst"; then
    hbytes=$(hdfs dfs -stat %b "$dst")
    if [[ "$hbytes" == "$bytes" ]]; then ok "$f already in HDFS ($bytes bytes) — skipped"; continue; fi
    warn "$f exists with a different size ($hbytes vs $bytes) — replacing"
    hdfs dfs -rm -skipTrash "$dst"
  fi

  log "Uploading $f ($(numfmt --to=iec "$bytes")) -> $dst"
  t0=$(date +%s.%N)
  # -D overrides make replication and block size explicit for this write,
  # independent of client-side defaults.
  hdfs dfs -D dfs.replication="$REPLICATION" -D dfs.blocksize="$BLOCK_SIZE" -put "$src" "$dst"
  t1=$(date +%s.%N)
  secs=$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.1f", b-a}')
  mbps=$(awk -v s="$secs" -v b="$bytes" 'BEGIN{printf "%.1f", b/1048576/s}')
  echo "$(date -u +%FT%TZ),$f,$bytes,$secs,$mbps,$REPLICATION,$BLOCK_SIZE" >> "$LOG_CSV"
  ok "$f uploaded in ${secs}s (${mbps} MB/s, written 3x across the cluster)"
done

log "Waiting until every block has $REPLICATION replicas"
hdfs dfs -setrep -w "$REPLICATION" "$HDFS_RAW_DIR"

# Store the manifest + ingest log next to the data (metadata zone)
hdfs dfs -put -f "$MANIFEST" "$HDFS_META_DIR/manifest.csv"
hdfs dfs -put -f "$LOG_CSV"  "$HDFS_META_DIR/ingest_log.csv"

log "HDFS raw zone"
hdfs dfs -ls -h "$HDFS_RAW_DIR"
hdfs dfs -du -s -h "$HDFS_RAW_DIR"     # columns: logical size | space used incl. 3 replicas
column -s, -t "$LOG_CSV" 2>/dev/null || cat "$LOG_CSV"
ok "Ingestion done. Next: ./scripts/09_verify_hdfs.sh"
