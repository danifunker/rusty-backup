#!/usr/bin/env bash
# Build every PowerPC family bundle back to back, unattended.
#
# The first family runs the full transpile (the expensive single-threaded
# mrustc leg); every later family is seeded from it and runs codegen + link +
# package only, because the emitted C is identical across families. See
# docs/build-ppc-mrustc.md "Transpile once across CPU families".
#
# Usage:
#   PPC_HOST=admin@g5.local scripts/build-all-ppc.sh          # 750 7400 970
#   PPC_HOST=... scripts/build-all-ppc.sh 750 7400            # a subset
# Logs land in $PPC_ALL_LOGDIR (default /tmp/ppc-all), one file per stage.
set -euo pipefail

RB_DIR="${RB_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
MRUSTC_DIR="${MRUSTC_DIR:-$HOME/repos/mrustc}"
LOGDIR="${PPC_ALL_LOGDIR:-/tmp/ppc-all}"
[ -n "${PPC_HOST:-}" ] || { echo "PPC_HOST is not set (ssh dest of the PowerPC Mac)" >&2; exit 1; }
# The G5 has 2 cores; more jobs just queue on it.
export PPC_JOBS="${PPC_JOBS:-2}"
# The shipped bundles were built without debug_assertions. The cfg changes the
# emitted C and minicargo cannot detect a cfg change, so it must stay constant
# across every family (and every incremental rebuild of these trees).
export MINICARGO_NO_DEBUG_ASSERTIONS=1

if [ "$#" -gt 0 ]; then FAMILIES=("$@"); else FAMILIES=(750 7400 970); fi
mkdir -p "$LOGDIR"
cd "$RB_DIR"

run_stage() {  # $1 = cpu, $2 = label, $3.. = stage + args
  local cpu="$1" label="$2"; shift 2
  echo "--- $label/$1 $(date -u +%H:%M:%SZ) ---"
  if PPC_CPU="$cpu" ./scripts/build-ppc.sh "$@" > "$LOGDIR/$label-$1.log" 2>&1; then
    echo "    $label/$1 OK $(date -u +%H:%M:%SZ)"
  else
    echo "    $label/$1 FAILED $(date -u +%H:%M:%SZ) -- see $LOGDIR/$label-$1.log"
    tail -20 "$LOGDIR/$label-$1.log" | sed 's/^/      /'
    return 1
  fi
}

echo "=== started $(date -u +%H:%M:%SZ), families: ${FAMILIES[*]} ==="
first_label=""
for cpu in "${FAMILIES[@]}"; do
  label="$(PPC_CPU="$cpu" ./scripts/build-ppc.sh label)"
  echo ""
  echo "########## $label (PPC_CPU=$cpu) started $(date -u +%H:%M:%SZ) ##########"
  ok=1
  engine_c=""
  if [ -n "$first_label" ]; then
    run_stage "$cpu" "$label" seed "$first_label" || ok=0
    engine_c="$(ls "$MRUSTC_DIR/output-rb-ppc-$label"/librusty_backup-*.rlib.c 2>/dev/null | head -1)"
  fi
  mtime_before="$([ -n "$engine_c" ] && stat -c %Y "$engine_c" || echo "")"
  if [ "$ok" = 1 ]; then
    for stage in ppclibs ppc dist; do
      run_stage "$cpu" "$label" "$stage" || { ok=0; break; }
    done
  fi
  # A seeded family must never re-transpile the engine; if the .c moved, the
  # seed failed to convince minicargo and an hour was silently spent.
  if [ -n "$engine_c" ] && [ -n "$mtime_before" ] && [ -e "$engine_c" ]; then
    if [ "$(stat -c %Y "$engine_c")" != "$mtime_before" ]; then
      echo "    WARNING: $label re-transpiled the engine despite seeding -- the"
      echo "    build is still correct, but check the seed exclusions/mtimes."
    fi
  fi
  if [ "$ok" = 1 ] && [ -f "$RB_DIR/dist/rb-cli-ppc-$label.tar.gz" ]; then
    echo "########## $label DONE -> dist/rb-cli-ppc-$label.tar.gz ($(stat -c%s "$RB_DIR/dist/rb-cli-ppc-$label.tar.gz") bytes) ##########"
    [ -z "$first_label" ] && first_label="$label"
  else
    echo "########## $label INCOMPLETE ##########"
  fi
done

echo ""
echo "=== finished $(date -u +%H:%M:%SZ) ==="
ls -la "$RB_DIR/dist/"rb-cli-ppc-*.tar.gz 2>/dev/null || true
