#!/usr/bin/env bash
# =====================================================================
# 07_stage_dataset.sh — run on MASTER. Unzips the Kaggle archive into the
# local staging dir, validates the header, and writes manifest.csv with
# exact byte sizes and row counts (the reference that every later phase
# checks against).
# Usage: ./scripts/07_stage_dataset.sh /bigdata/staging/ecommerce-archive.zip
# =====================================================================
source "$(dirname "$0")/lib.sh"
require_host master

ZIP="${1:-$LOCAL_ROOT/staging/ecommerce-archive.zip}"
[[ -f "$ZIP" ]] || die "Archive not found: $ZIP (copy it from Windows first, README step 10)"
command -v unzip >/dev/null || die "unzip missing: sudo apt-get install -y unzip"

log "Archive contents"
unzip -l "$ZIP"
ZIP_LIST="$(unzip -Z1 "$ZIP")"
for f in "${DATA_FILES[@]}"; do
  grep -qE "(^|/)$f\$" <<<"$ZIP_LIST" || die "$f not found inside $ZIP"
done

# Space check: uncompressed size + 2 GB margin
need=$(unzip -l "$ZIP" | tail -1 | awk '{print $1}')
avail=$(df --output=avail -B1 "$STAGING_DIR" | tail -1)
(( avail > need + 2*1024**3 )) || die "Need $((need/1024**3)) GB + 2 GB free in $STAGING_DIR, have $((avail/1024**3)) GB"

log "Unzipping into $STAGING_DIR (several minutes)"
# -j: ignore folders inside the zip, -o: overwrite on re-run
unzip -o -j "$ZIP" "${DATA_FILES[@]/#/*}" -d "$STAGING_DIR"

MANIFEST="$STAGING_DIR/manifest.csv"
echo "file,bytes,rows,sha256_first_64mb" > "$MANIFEST"
for f in "${DATA_FILES[@]}"; do
  p="$STAGING_DIR/$f"
  header="$(head -1 "$p" | tr -d '\r')"
  [[ "$header" == "$EXPECTED_HEADER" ]] || die "$f: unexpected header: $header"
  bytes=$(stat -c %s "$p")
  log "Counting rows in $f"
  rows=$(( $(wc -l < "$p") - 1 ))           # minus header
  fp=$(head -c 67108864 "$p" | sha256sum | cut -c1-16)   # cheap fingerprint
  echo "$f,$bytes,$rows,$fp" >> "$MANIFEST"
  ok "$f: $(numfmt --to=iec "$bytes"), $rows rows"
  head -3 "$p" | sed 's/^/    /'
done

total=$(awk -F, 'NR>1{s+=$3} END{print s}' "$MANIFEST")
ok "Manifest: $MANIFEST  (total rows: $total)"
column -s, -t "$MANIFEST" 2>/dev/null || cat "$MANIFEST"
