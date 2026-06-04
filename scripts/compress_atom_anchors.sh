#!/usr/bin/env bash
# Compress the sample .atm files extracted from the Atom Software
# Archive V13.00 into committed anchor fixtures.
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="${ATOM_SAMPLE_SRC:-/mnt/c/Users/spam/AppData/Local/Temp/atom_archive/atm_samples}"
for entry in FROGGER INVADER; do
    src="$SRC/$entry"
    [ -f "$src" ] || { echo "missing: $src" >&2; exit 1; }
    out="$REPO_ROOT/tests/fixtures/anchor_atom_${entry}.atm.zst"
    zstd -19 -f "$src" -o "$out"
    echo "wrote $out ($(stat -c%s "$out") B)"
done
