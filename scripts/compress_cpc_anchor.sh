#!/usr/bin/env bash
# Compress the Manic Miner .dsk from TOSEC into a committed anchor
# fixture for Amstrad CPC EDSK + AMSDOS-via-amstrad_data cross-check.
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="${CPC_SAMPLE_SRC:-/mnt/c/Users/spam/AppData/Local/Temp/cpc_tosec/picks/dsk/ManicMiner.dsk}"
[ -f "$SRC" ] || { echo "missing: $SRC" >&2; exit 1; }
OUT="$REPO_ROOT/tests/fixtures/anchor_cpc_ManicMiner.dsk.zst"
zstd -19 -f "$SRC" -o "$OUT"
echo "wrote $OUT ($(stat -c%s "$OUT") B; raw $(stat -c%s "$SRC") B)"
