#!/usr/bin/env bash
# One-host full regression: build, validate, run every case, produce every
# artifact, then verify them with every oracle this host has (third-party
# tools and the scripted emulators). No other machine is involved; for the
# cross-OS sweep see regress-all.sh.
#
#   regression-tests/scripts/regress-local.sh [--no-gui-deps] [--skip-run] [--skip-verify]
#
# Environment:
#   RB_BUILD_DIR    cargo target dir (default: <repo>/target). Put it on a big disk.
#   RB_SCRATCH      scratch root for cases (default: regression-tests/scratch)
#   RB_FEATURES     rb-cli features when --no-gui-deps is given
#                   (default: every default feature except `audio`, which turns on
#                   `gui` and so needs GTK headers)
#
# Exit code: 0 when every case passed or is a known failure and no oracle
# reported FAIL; 1 otherwise; 2 when the harness itself could not run.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGRESS="$(cd "$HERE/.." && pwd)"
REPO="$(cd "$REGRESS/.." && pwd)"

NO_GUI_DEPS=0
SKIP_RUN=0
SKIP_VERIFY=0
for a in "$@"; do
    case "$a" in
        --no-gui-deps) NO_GUI_DEPS=1 ;;
        --skip-run)    SKIP_RUN=1 ;;
        --skip-verify) SKIP_VERIFY=1 ;;
        *) echo "unknown argument: $a" >&2; exit 2 ;;
    esac
done

BUILD="${RB_BUILD_DIR:-$REPO/target}"
SCRATCH="${RB_SCRATCH:-$REGRESS/scratch}"
FEATURES="${RB_FEATURES:-chd,optical,native-zlib,native-zstd,seekable-zstd,remote,tui,crypto,yaml}"
RB_CLI="$BUILD/release/rb-cli"
RB="$BUILD/runner/release/rb-regress"

step() { printf '\n=== %s  (%s)\n' "$*" "$(date '+%H:%M:%S')"; }
die()  { printf 'ERROR: %s\n' "$*" >&2; exit 2; }

FAILED=0
cd "$REPO" || die "no repo at $REPO"
step "HEAD $(git rev-parse --short HEAD), $(git status --porcelain | wc -l | tr -d ' ') uncommitted change(s)"

step "build rb-cli"
if [ "$NO_GUI_DEPS" = 1 ]; then
    CARGO_TARGET_DIR="$BUILD" cargo build --release --bin rb-cli \
        --no-default-features --features "$FEATURES" || die "rb-cli build failed"
else
    CARGO_TARGET_DIR="$BUILD" cargo build --release --bin rb-cli || die "rb-cli build failed"
fi

step "build rb-regress"
( cd "$REGRESS/runner" && CARGO_TARGET_DIR="$BUILD/runner" cargo build --release ) \
    || die "runner build failed"

step "validate manifests and the bug list"
"$RB" validate || die "manifests or bug list are invalid"

step "detect oracles on this host"
"$RB" oracles --detect --no-prompt || true

if [ "$SKIP_RUN" = 0 ]; then
    step "run every case"
    mkdir -p "$SCRATCH"
    "$RB" run --rb-cli "$RB_CLI" --require-clean --scratch-root "$SCRATCH" || FAILED=1
fi

if [ "$SKIP_VERIFY" = 0 ]; then
    step "produce every artifact"
    "$RB" produce --rb-cli "$RB_CLI" || FAILED=1
    step "verify the artifacts with this host's oracles"
    # Script oracles that drive rb-cli themselves (motion) find it through RB_CLI.
    RB_CLI="$RB_CLI" "$RB" verify || FAILED=1
fi

step "done: $([ "$FAILED" = 0 ] && echo 'nothing to look at' || echo 'something needs a look - see above')"
exit "$FAILED"
