#!/usr/bin/env bash
# One-shot: compress the MiSTer-pulled anchor fixtures into tests/fixtures/.
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="${MISTER_FIXTURE_SRC:-/mnt/c/Users/spam/AppData/Local/Temp/mister_fixtures}"
for entry in BLANK_disk_X68000.D88 GamesCart.mdv crazy.mdv; do
    src="$SRC/$entry"
    [ -f "$src" ] || { echo "missing: $src" >&2; exit 1; }
    safe="${entry// /_}"
    out="$REPO_ROOT/tests/fixtures/anchor_mister_${safe}.zst"
    zstd -19 -f "$src" -o "$out"
    echo "wrote $out ($(stat -c%s "$out") B)"
done
