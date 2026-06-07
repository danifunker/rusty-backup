#!/usr/bin/env bash
# Build the AtariST `.msa` floppy + raw `.st` fixtures used by
# `tests/atarist_e2e.rs`. Run from any clone (the script auto-locates
# the repo root from its own path).
#
# Requires (verified WSL Ubuntu-24.04 toolchain, see OPEN-WORK §8):
# - mkfs.fat / mtools (build a known-content FAT12 floppy)
# - hmsa (Hatari's reference MSA encoder, ships with the `hatari` package)
# - zstd (compress the committed fixtures)
#
# The AHDI HDD fixture has its own generator — kept separate so each
# phase can be regenerated independently when its spec evolves.
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

# --- Tool sanity ---
for t in mkfs.fat mcopy hmsa zstd dd sha256sum; do
    if ! command -v "$t" >/dev/null; then
        echo "ERROR: required tool '$t' not on PATH" >&2
        exit 1
    fi
done

# --- Build a known-content 720K FAT12 floppy ---
# 720K = 9 spt × 80 trk × 2 sides × 512 B. That's the canonical
# Atari ST DD floppy geometry that MSA always wraps.
echo "=== Building 720K FAT12 floppy ==="
ST_RAW="$TMPDIR/atarist_floppy.st"
dd if=/dev/zero of="$ST_RAW" bs=1024 count=720 status=none

# Format with mkfs.fat. -F 12 forces FAT12; -n labels the volume.
# Note: stock mkfs.fat sets a non-Atari OEM ID but the BPB is
# spec-correct and our FAT driver picks the geometry from the BPB.
mkfs.fat -F 12 -n "ATARI_ST" -i "DECAF811" "$ST_RAW" >/dev/null

# Seed known content: a top-level file + one nested directory + file.
# Each file has stable bytes so the integration test can hash-compare.
echo -n "AtariST floppy hello via MSA round-trip." > "$TMPDIR/hello.txt"
echo -n "Nested under PROG." > "$TMPDIR/nested.txt"
mcopy -i "$ST_RAW" "$TMPDIR/hello.txt" "::HELLO.TXT"
mmd -i "$ST_RAW" "::PROG"
mcopy -i "$ST_RAW" "$TMPDIR/nested.txt" "::PROG/NESTED.TXT"
sync

# Record the raw image hash + listing for the test assertion.
ST_SHA="$(sha256sum "$ST_RAW" | awk '{print $1}')"
echo ".st flat raw : $ST_SHA"
mdir -i "$ST_RAW" "::" "::PROG"

# --- Encode to MSA via hmsa (Hatari's reference encoder) ---
echo
echo "=== Encoding to MSA via hmsa ==="
MSA_OUT="$TMPDIR/atarist_floppy.msa"
# hmsa writes its output next to the input with the extension swapped.
# Copy the `.st` into a sibling path so the output lands where we want.
#
# NOTE: the Ubuntu 24.04 `hatari` package's `hmsa` exits with code 1 even
# on a successful conversion (upstream bug). We verify by checking the
# output file existed and is non-empty, not by the exit code.
cp "$ST_RAW" "$TMPDIR/hmsa_in.st"
hmsa "$TMPDIR/hmsa_in.st" || true
if [[ ! -s "$TMPDIR/hmsa_in.msa" ]]; then
    echo "ERROR: hmsa did not produce $TMPDIR/hmsa_in.msa" >&2
    exit 1
fi
mv "$TMPDIR/hmsa_in.msa" "$MSA_OUT"
echo ".msa size    : $(stat -c%s "$MSA_OUT") bytes"
echo ".msa sha256  : $(sha256sum "$MSA_OUT" | awk '{print $1}')"

# --- Cross-check: hmsa decodes back to byte-identical `.st` ---
echo
echo "=== hmsa round-trip cross-check ==="
cp "$MSA_OUT" "$TMPDIR/rt.msa"
hmsa "$TMPDIR/rt.msa" || true
if [[ ! -s "$TMPDIR/rt.st" ]]; then
    echo "ERROR: hmsa did not produce $TMPDIR/rt.st" >&2
    exit 1
fi
RT_SHA="$(sha256sum "$TMPDIR/rt.st" | awk '{print $1}')"
if [[ "$RT_SHA" != "$ST_SHA" ]]; then
    echo "ERROR: hmsa round-trip mismatch — fixture is bogus" >&2
    echo "  original  : $ST_SHA"
    echo "  round-tripped: $RT_SHA"
    exit 1
fi
echo "hmsa round-trip OK — $ST_SHA"

# --- Commit ---
zstd -19 -f "$ST_RAW"   -o "$FIXTURES/test_atarist_floppy.st.zst"
zstd -19 -f "$MSA_OUT"  -o "$FIXTURES/test_atarist_floppy.msa.zst"
chmod 644 "$FIXTURES/test_atarist_floppy".*.zst

echo
echo "=== Committed ==="
ls -la "$FIXTURES/test_atarist_floppy".*
echo
echo "SHA256 of decoded flat .st (what tests assert on): $ST_SHA"
