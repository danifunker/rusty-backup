#!/usr/bin/env bash
# Build a synthetic Sharp `.d88` fixture for tests/d88_e2e.rs.
#
# Approach: format a 720 KB FAT12 floppy with mkfs.fat (standard PC
# geometry: 80 cyl × 2 sides × 9 sec × 512 B), seed two files via
# mtools, then wrap the flat image in a `.d88` container via our
# in-tree `examples/d88_encode` helper.
#
# This is a SYNTHETIC fixture per the test-fixture policy: real TOSEC
# `.d88` dumps are for scouting (e.g. `anchor_mister_BLANK_disk_X68000`
# in tests/fixtures, which we validated decoder behaviour against and
# left as a future Human68k consumer), but the committed fixture is
# our own clean-room build with known seed content.
#
# Why 720 KB DD instead of X68000's native 2HD 1232 KB? The Human68k
# engine accepts any FAT-compatible BPB — 2DD is the simplest mkfs.fat
# can produce without sector-size gymnastics, and the D88 wrapper
# round-trips identically for both. The X68000 boot-test path uses
# the 2HD shape; the decoder regression test pins the wrapper format.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FIXTURES="$REPO_ROOT/tests/fixtures"

# Use a tempdir under target/ so the Windows-built d88_encode helper
# (a .exe in target/release/examples/) can still resolve the same path.
# Linux mktemp /tmp/... isn't visible to Windows binaries via WSL.
TMPROOT="$REPO_ROOT/target/d88-fixture-tmp"
mkdir -p "$TMPROOT"
TMPDIR="$(mktemp -d -p "$TMPROOT")"
trap 'rm -rf "$TMPDIR"' EXIT

mkdir -p "$FIXTURES"
echo "Repo root : $REPO_ROOT"
echo "Fixtures  : $FIXTURES"
echo "Tempdir   : $TMPDIR"
echo

for t in mkfs.fat mcopy mdir zstd dd sha256sum; do
    if ! command -v "$t" >/dev/null; then
        echo "ERROR: required tool '$t' not on PATH" >&2
        exit 1
    fi
done

# The d88_encode helper must be pre-built on the host. We prefer the
# release artifact because it's small and fast; fall back to debug if
# the user has only run `cargo build --example d88_encode`.
D88_ENCODE=""
for candidate in \
    "$REPO_ROOT/target/release/examples/d88_encode" \
    "$REPO_ROOT/target/release/examples/d88_encode.exe" \
    "$REPO_ROOT/target/debug/examples/d88_encode" \
    "$REPO_ROOT/target/debug/examples/d88_encode.exe"; do
    if [ -x "$candidate" ]; then
        D88_ENCODE="$candidate"
        break
    fi
done
if [ -z "$D88_ENCODE" ]; then
    echo "ERROR: d88_encode helper not built. Run:" >&2
    echo "  cargo build --release --example d88_encode" >&2
    echo "from the host then re-run this script." >&2
    exit 1
fi

# 720 KB DD floppy: 80 cyl × 2 sides × 9 sec × 512 B.
CYLS=80
HEADS=2
SPT=9
SEC=512
TOTAL=$(( CYLS * HEADS * SPT * SEC ))

FLAT="$TMPDIR/flat.img"
dd if=/dev/zero of="$FLAT" bs=512 count=$(( TOTAL / 512 )) status=none

echo "=== Formatting FAT12 (Human68k-compatible BPB) ==="
mkfs.fat -F 12 -S "$SEC" -s 1 -h 0 -R 1 -n "X68KSAMPLE" "$FLAT"

# Use our own Human68k EditableFilesystem to write the seed files, so
# directory bytes 12-21 are zeroed (Human68k's "no extended name" idiom)
# instead of holding FAT12 control bytes that mtools' mcopy would put
# there. Without this, the Human68k parser concatenates the FAT-control
# bytes into the displayed name.
H68K_BUILD=""
for candidate in \
    "$REPO_ROOT/target/release/examples/build_human68k_fixture" \
    "$REPO_ROOT/target/release/examples/build_human68k_fixture.exe" \
    "$REPO_ROOT/target/debug/examples/build_human68k_fixture" \
    "$REPO_ROOT/target/debug/examples/build_human68k_fixture.exe"; do
    if [ -x "$candidate" ]; then
        H68K_BUILD="$candidate"
        break
    fi
done
if [ -z "$H68K_BUILD" ]; then
    echo "ERROR: build_human68k_fixture helper not built. Run:" >&2
    echo "  cargo build --release --example build_human68k_fixture" >&2
    exit 1
fi

OUT_FLAT="$TMPDIR/flat-h68k.img"
if [[ "$H68K_BUILD" == *.exe ]] && command -v wslpath >/dev/null; then
    "$H68K_BUILD" "$(wslpath -w "$FLAT")" "$(wslpath -w "$OUT_FLAT")"
else
    "$H68K_BUILD" "$FLAT" "$OUT_FLAT"
fi
FLAT="$OUT_FLAT"

echo
echo "=== mtools view of post-Human68k floppy (cross-check FAT compatibility) ==="
mdir -i "$FLAT" -/ :: || echo "(mtools may complain about bytes 12-21 zeros; harmless)"
echo "SHA256 (flat): $(sha256sum "$FLAT" | awk '{print $1}')"

# Wrap in D88. d88_encode may be a Windows .exe (when WSL invokes the
# host-built target/release/examples/d88_encode.exe); in that case we
# have to translate WSL paths to Windows paths before passing them in.
OUT_D88="$TMPDIR/x68k.d88"
if [[ "$D88_ENCODE" == *.exe ]] && command -v wslpath >/dev/null; then
    FLAT_WIN="$(wslpath -w "$FLAT")"
    OUT_WIN="$(wslpath -w "$OUT_D88")"
    "$D88_ENCODE" "$FLAT_WIN" "$OUT_WIN" "$CYLS" "$HEADS" "$SPT" "$SEC" 2dd
else
    "$D88_ENCODE" "$FLAT" "$OUT_D88" "$CYLS" "$HEADS" "$SPT" "$SEC" 2dd
fi

echo
echo "=== d88 wrapper info ==="
ls -la "$OUT_D88"
echo "SHA256 (d88): $(sha256sum "$OUT_D88" | awk '{print $1}')"

# Commit.
zstd -19 -f "$OUT_D88" -o "$FIXTURES/test_x68000_human68k_2dd.d88.zst"
chmod 644 "$FIXTURES/test_x68000_human68k_2dd.d88.zst"

echo
echo "=== Committed ==="
ls -la "$FIXTURES/test_x68000_human68k_2dd.d88.zst"
echo
echo "Wrapper:   Sharp .d88 (2DD, $CYLS cyl × $HEADS heads × $SPT sec × $SEC B)"
echo "Filesystem: FAT12 with Human68k-compatible BPB; readable by both"
echo "            our Human68k engine and standard FAT12 dispatch."
