#!/usr/bin/env bash
# Build a fresh, writable Cedar 6.1 Pilot work volume for the Dorado emulator.
#
# Usage:
#   scripts/build-cedar-work-volume.sh /path/to/CedarDorado-work.pdi [volume-label]
#
# The image is deliberately not populated from a kitchen-sink volume.  It has
# the matched germ, BasicCedar boot world, an empty writable client directory,
# and roughly 64K free Pilot pages.  Add software only after this base image
# has cold-booted and Cedar has successfully made a local file.
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <out.pdi> [volume-label]" >&2
  exit 2
fi

RUSTY_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DORADO_ROOT="$(cd "$RUSTY_ROOT/../.." && pwd)"
OUT="$1"
LABEL="${2:-CedarWork}"
GERM="${CEDAR_GERM:-$DORADO_ROOT/chm/cedar/germ-alt/Dorado.germ-6.1.6}"
BOOT="${CEDAR_BOOT:-$DORADO_ROOT/chm/cedar/cedar6.1/BasicCedarDorado.boot!22}"

[[ -f "$GERM" ]] || { echo "missing Cedar 6.1 germ: $GERM" >&2; exit 1; }
[[ -f "$BOOT" ]] || { echo "missing BasicCedar boot file: $BOOT" >&2; exit 1; }
[[ ! -e "$OUT" ]] || { echo "refusing to overwrite existing image: $OUT" >&2; exit 1; }
mkdir -p "$(dirname "$OUT")"
# `pilot_probe` runs after this script changes into the Rusty checkout, so
# resolve a caller-supplied relative output path while still in the caller's
# directory.
OUT="$(cd "$(dirname "$OUT")" && pwd)/$(basename "$OUT")"

cd "$RUSTY_ROOT"
cargo run --release --example pilot_probe -- new 65535 cedar "$LABEL" "$OUT"
cargo run --release --example pilot_probe -- install-boot "$OUT" germ "$GERM"
cargo run --release --example pilot_probe -- install-boot "$OUT" bootfile "$BOOT"

# A zero-entry B-tree is intentional: Cedar can allocate names into its reserved
# free B-tree pages, while the rest of the volume remains free for local files.
cargo run --release --example pilot_probe -- set-dir "$OUT"
cargo run --release --example pilot_probe -- verify "$OUT" \
  "germ=$GERM" "bootfile=$BOOT"

if [[ -x "$DORADO_ROOT/dorado/build/pdidump" ]]; then
  "$DORADO_ROOT/dorado/build/pdidump" "$OUT"
else
  echo "note: build dorado/build/pdidump, then inspect: $DORADO_ROOT/dorado/build/pdidump $OUT" >&2
fi

echo ""
echo "Built $OUT"
echo "Next acceptance test: cold-boot it with CedarDorado.eb!6, log in, and create a local file."
