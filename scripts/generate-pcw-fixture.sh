#!/usr/bin/env bash
# Build a cpmtools-cross-checked Amstrad PCW Format A fixture for
# tests/pcw_e2e.rs.
#
# Format A is the dominant PCW floppy shape (single-sided 40-track,
# 9 × 512 B sectors, one reserved boot track) — what TOSEC's PCW
# Applications and Games archives all ship. cpmtools' `pcw` diskdef
# (libdsk `pcw180`) emits the exact same geometry our AMSTRAD_PCW DPB
# expects, so the round-trip is byte-identical.
#
# This is a SYNTHETIC (clean-room) fixture per the test-fixture policy:
# real TOSEC disks are for scouting only; the committed fixture is a
# fresh disk we built ourselves with known seed content.
#
# Output: tests/fixtures/test_pcw_format_a.dsk.zst (flat, ~180 KB raw)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FIXTURES="$REPO_ROOT/tests/fixtures"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

mkdir -p "$FIXTURES"
echo "Repo root : $REPO_ROOT"
echo "Fixtures  : $FIXTURES"
echo "Tempdir   : $TMPDIR"
echo

for t in mkfs.cpm cpmls cpmcp zstd dd sha256sum; do
    if ! command -v "$t" >/dev/null; then
        echo "ERROR: required tool '$t' not on PATH" >&2
        exit 1
    fi
done

# Build a fresh Format A floppy: 40 trk × 9 sec × 512 B = 184320 B.
DISK="$TMPDIR/pcw.dsk"
dd if=/dev/zero of="$DISK" bs=512 count=360 status=none

# Apply the pcw DPB via cpmtools' shipped diskdef.
echo "=== Formatting CP/M (pcw = Amstrad PCW Format A) ==="
mkfs.cpm -f pcw "$DISK"

# Seed two files with known content. Filenames stay 8.3 uppercase to
# match the AMSDOS / PCW BIOS convention; cpmtools lower-cases on
# display but the on-disk bytes are upper.
echo -n "Hello from PCW Format A fixture." > "$TMPDIR/hello.txt"
echo -n "Second file, different content."  > "$TMPDIR/note.txt"
cpmcp -f pcw "$DISK" "$TMPDIR/hello.txt" 0:hello.txt
cpmcp -f pcw "$DISK" "$TMPDIR/note.txt"  0:note.txt
sync

echo
echo "=== cpmtools view of seeded disk ==="
cpmls -f pcw "$DISK"
echo "SHA256: $(sha256sum "$DISK" | awk '{print $1}')"

# Commit.
zstd -19 -f "$DISK" -o "$FIXTURES/test_pcw_format_a.dsk.zst"
chmod 644 "$FIXTURES/test_pcw_format_a.dsk.zst"

echo
echo "=== Committed ==="
ls -la "$FIXTURES/test_pcw_format_a.dsk.zst"
echo
echo "DPB:  Amstrad PCW Format A (off=1 reserved boot track, 180 KB,"
echo "      1024 B allocation blocks; cpmtools name 'pcw', our preset"
echo "      name 'amstrad_pcw')."
