#!/usr/bin/env bash
# Build a cpmtools-cross-checked CP/M fixture for tests/cpm_e2e.rs.
#
# Uses mkfs.cpm + cpmcp from the apt `cpmtools` package (verified
# installed in WSL Ubuntu-24.04). The fixture is an Amstrad-data-format
# 180 KB CP/M floppy with two seeded files. The same disk reads back
# clean through both our engine and cpmls — the strongest oracle we
# get on any Wave-2 core.
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

# Build a fresh Amstrad-data CP/M 180 KB floppy.
DISK="$TMPDIR/cpm.dsk"
dd if=/dev/zero of="$DISK" bs=512 count=360 status=none

# Apply the cpcdata DPB via cpmtools' shipped diskdef.
echo "=== Formatting CP/M (cpcdata = Amstrad data format) ==="
mkfs.cpm -f cpcdata "$DISK"

# Seed two files with known content.
echo -n "Hello from CP/M cpmtools fixture." > "$TMPDIR/hello.txt"
echo -n "Second file, different content." > "$TMPDIR/note.txt"
cpmcp -f cpcdata "$DISK" "$TMPDIR/hello.txt" 0:hello.txt
cpmcp -f cpcdata "$DISK" "$TMPDIR/note.txt"  0:note.txt
sync

echo
echo "=== cpmtools view of seeded disk ==="
cpmls -f cpcdata "$DISK"
echo "SHA256: $(sha256sum "$DISK" | awk '{print $1}')"

# Commit.
zstd -19 -f "$DISK" -o "$FIXTURES/test_cpm_amstrad_data.dsk.zst"
chmod 644 "$FIXTURES/test_cpm_amstrad_data.dsk.zst"

echo
echo "=== Committed ==="
ls -la "$FIXTURES/test_cpm_amstrad_data.dsk.zst"
echo
echo "DPB:  Amstrad data format (no reserved tracks, 180 KB, 1024 B blocks)"
echo "Note: cpmtools labels this 'data'; our preset name is 'amstrad_data'."
