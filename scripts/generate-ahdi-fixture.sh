#!/usr/bin/env bash
# Build the AHDI HDD fixture used by `tests/atarist_e2e.rs`. Run from any
# clone (auto-locates repo root from its own path).
#
# Layout (matches `src/partition/atari.rs::AhdiTable` shape):
#   sector 0           AHDI root sector (4 primary slots, big-endian)
#   sectors 2..16385   Partition 0 — GEM, 8 MiB FAT12  (mkfs.fat -F 12)
#   sectors 16386..49153  Partition 1 — BGM, 16 MiB FAT16 (mkfs.fat -F 16)
#   total              65536 sectors = 32 MiB
#
# Requires (verified WSL Ubuntu-24.04):
# - mkfs.fat, mtools (build FAT12/16 partition images + verify)
# - python3 (stamp the AHDI root sector with 0x1234 word-sum checksum)
# - zstd (compress the committed fixture)
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

for t in mkfs.fat mcopy mdir mmd python3 zstd dd sha256sum; do
    if ! command -v "$t" >/dev/null; then
        echo "ERROR: required tool '$t' not on PATH" >&2
        exit 1
    fi
done

# --- Partition layout (sectors of 512 B) ---
# Reserve sector 0 for the AHDI root. Partitions sit at hardcoded LBAs
# below; if these change, update src/partition/atari.rs::tests and
# tests/atarist_e2e.rs in lockstep.
PART0_START=2          # GEM, FAT12
PART0_COUNT=16384      # 8 MiB
PART1_START=16386      # BGM, FAT16
PART1_COUNT=32768      # 16 MiB
DISK_SECTORS=65536     # 32 MiB

DISK="$TMPDIR/atarist_ahdi.img"
P0="$TMPDIR/p0_gem_fat12.img"
P1="$TMPDIR/p1_bgm_fat16.img"

echo "=== Building 8 MiB FAT12 partition (GEM) ==="
dd if=/dev/zero of="$P0" bs=512 count=$PART0_COUNT status=none
mkfs.fat -F 12 -n "GEM_P0" -i "AABBCCDD" "$P0" >/dev/null
echo -n "GEM partition 0 hello." > "$TMPDIR/p0_hello.txt"
echo -n "P0 nested via mtools." > "$TMPDIR/p0_nested.txt"
mcopy -i "$P0" "$TMPDIR/p0_hello.txt" "::HELLO.TXT"
mmd -i "$P0" "::SUB"
mcopy -i "$P0" "$TMPDIR/p0_nested.txt" "::SUB/NESTED.TXT"
sync
mdir -i "$P0" "::"

echo
echo "=== Building 16 MiB FAT16 partition (BGM) ==="
dd if=/dev/zero of="$P1" bs=512 count=$PART1_COUNT status=none
mkfs.fat -F 16 -n "BGM_P1" -i "11223344" "$P1" >/dev/null
echo -n "BGM partition 1 hello." > "$TMPDIR/p1_hello.txt"
echo -n "P1 nested via mtools." > "$TMPDIR/p1_nested.txt"
mcopy -i "$P1" "$TMPDIR/p1_hello.txt" "::HELLO.TXT"
mmd -i "$P1" "::SUB"
mcopy -i "$P1" "$TMPDIR/p1_nested.txt" "::SUB/NESTED.TXT"
sync
mdir -i "$P1" "::"

# --- Build the disk: zero-fill, then splice partitions, then stamp root ---
echo
echo "=== Assembling AHDI disk ==="
dd if=/dev/zero of="$DISK" bs=512 count=$DISK_SECTORS status=none
dd if="$P0" of="$DISK" bs=512 seek=$PART0_START count=$PART0_COUNT conv=notrunc status=none
dd if="$P1" of="$DISK" bs=512 seek=$PART1_START count=$PART1_COUNT conv=notrunc status=none

# --- Stamp the AHDI root sector via python ---
python3 - "$DISK" "$PART0_START" "$PART0_COUNT" "$PART1_START" "$PART1_COUNT" <<'PY'
import sys, struct

path = sys.argv[1]
p0_start = int(sys.argv[2])
p0_count = int(sys.argv[3])
p1_start = int(sys.argv[4])
p1_count = int(sys.argv[5])

# AHDI root sector layout (matches src/partition/atari.rs):
#   0x000..0x1C6  bootstrap (zeroed)
#   0x1C6..0x1F6  4x 12-byte entries, big-endian
#       flags(1) id(3 ASCII) start(BE u32) blocks(BE u32)
#   0x1F6..0x1FA  disk size in sectors (BE u32)
#   0x1FA..0x1FE  bad-sector list start (BE u32, 0 = none)
#   0x1FE..0x200  checksum (BE u16; big-endian word-sum must equal 0x1234)
sec = bytearray(512)

# Slot 0: GEM partition (FAT12)
off = 0x1C6
sec[off] = 0x01  # exists
sec[off+1:off+4] = b"GEM"
struct.pack_into(">I", sec, off+4, p0_start)
struct.pack_into(">I", sec, off+8, p0_count)

# Slot 1: BGM partition (FAT16)
off = 0x1C6 + 12
sec[off] = 0x01
sec[off+1:off+4] = b"BGM"
struct.pack_into(">I", sec, off+4, p1_start)
struct.pack_into(">I", sec, off+8, p1_count)

# Slots 2 and 3: empty (already zeroed)

# Disk size field (sectors)
struct.pack_into(">I", sec, 0x1F6, p0_start + p0_count + p1_count)  # nominal, not used by detection

# Bad-sector list: 0 (none)
struct.pack_into(">I", sec, 0x1FA, 0)

# Checksum: make the BE word-sum of all 256 u16 words = 0x1234.
total = 0
for i in range(0, 0x1FE, 2):
    total = (total + ((sec[i] << 8) | sec[i+1])) & 0xFFFFFFFF
cksum = (0x1234 - total) & 0xFFFF
struct.pack_into(">H", sec, 0x1FE, cksum)

# Sanity self-check.
verify = 0
for i in range(0, 0x200, 2):
    verify = (verify + ((sec[i] << 8) | sec[i+1])) & 0xFFFFFFFF
assert (verify & 0xFFFF) == 0x1234, f"checksum self-check failed: got 0x{verify & 0xFFFF:04X}"
print(f"AHDI root sector checksum stamped: 0x{cksum:04X} (verified)")

with open(path, "r+b") as f:
    f.seek(0)
    f.write(sec)
PY

# --- Verify via mtools that each partition is readable at its declared offset ---
echo
echo "=== mtools cross-check at partition offsets ==="
# mtools can read a partition inside a disk image by extracting the bytes
# first (dd) and feeding the slice as if it were a flat image.
dd if="$DISK" of="$TMPDIR/p0_check.img" bs=512 skip=$PART0_START count=$PART0_COUNT status=none
mdir -i "$TMPDIR/p0_check.img" "::" "::SUB"
dd if="$DISK" of="$TMPDIR/p1_check.img" bs=512 skip=$PART1_START count=$PART1_COUNT status=none
mdir -i "$TMPDIR/p1_check.img" "::" "::SUB"

# Confirm post-splice byte-for-byte identity (sanity).
DISK_SHA="$(sha256sum "$DISK" | awk '{print $1}')"
echo
echo "Disk size : $(stat -c%s "$DISK") bytes"
echo "Disk sha  : $DISK_SHA"

# --- Commit ---
zstd -19 -f "$DISK" -o "$FIXTURES/test_atarist_ahdi.img.zst"
chmod 644 "$FIXTURES/test_atarist_ahdi.img.zst"

echo
echo "=== Committed ==="
ls -la "$FIXTURES/test_atarist_ahdi.img.zst"
echo
echo "Layout summary:"
echo "  disk sectors  : $DISK_SECTORS  (32 MiB)"
echo "  GEM (FAT12)   : sectors $PART0_START..$((PART0_START + PART0_COUNT - 1))  ($((PART0_COUNT/2048)) MiB)"
echo "  BGM (FAT16)   : sectors $PART1_START..$((PART1_START + PART1_COUNT - 1))  ($((PART1_COUNT/2048)) MiB)"
