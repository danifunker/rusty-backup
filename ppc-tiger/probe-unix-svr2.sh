#!/bin/sh
# probe-unix-svr2.sh — quick & dirty probe of an Apple_UNIX_SVR2 slot.
# Tells you whether it's ext2/3/4, Linux swap, UFS, or something else.
#
# Usage:  sudo ./probe-unix-svr2.sh /dev/disk0s4
#
# No deps beyond dd / od / sh. Safe to run on Tiger PPC.

set -u

DEV="${1:-}"
if [ -z "$DEV" ]; then
    echo "Usage: sudo $0 /dev/diskNsM"
    exit 1
fi

if [ ! -r "$DEV" ]; then
    echo "Cannot read $DEV — try with sudo." >&2
    exit 1
fi

echo "=== Probing $DEV ==="

# Read first 64 KiB into a temp file (covers ext sb + biggest swap page)
TMP=$(mktemp -t probe.XXXXXX) || exit 1
trap 'rm -f "$TMP"' EXIT

dd if="$DEV" of="$TMP" bs=1024 count=64 2>/dev/null
if [ ! -s "$TMP" ]; then
    echo "  Read failed."
    exit 1
fi

# Hex dump at a byte offset, N bytes; prints one space-separated line.
hex_at() {
    off=$1; len=$2
    dd if="$TMP" bs=1 skip="$off" count="$len" 2>/dev/null | od -An -tx1 | tr -d ' \n'
}
ascii_at() {
    off=$1; len=$2
    dd if="$TMP" bs=1 skip="$off" count="$len" 2>/dev/null | tr -c '\40-\176' '.'
}

# ----- ext2/3/4 -----------------------------------------------------
# Superblock at byte 1024; magic 0xEF53 (LE) at offset 0x38 of sb.
ext_magic=$(hex_at $((1024 + 0x38)) 2)
if [ "$ext_magic" = "53ef" ]; then
    echo "  Match: ext2/3/4 superblock magic at +0x438"
    # block size = 1024 << s_log_block_size (LE u32 at sb+0x18)
    log_bs_hex=$(hex_at $((1024 + 0x18)) 4)
    # Convert little-endian hex to decimal via printf
    b0=$(printf '%d' "0x${log_bs_hex%??????}")
    log_bs=$b0
    block_size=$((1024 << log_bs))
    echo "  Block size:    $block_size"

    # s_blocks_count_lo at sb+0x04
    bc_hex=$(hex_at $((1024 + 0x04)) 4)
    # Little-endian -> decimal (handles up to 4 GiB blocks; good enough)
    bc=$(printf '%d' "0x${bc_hex:6:2}${bc_hex:4:2}${bc_hex:2:2}${bc_hex:0:2}")
    echo "  Total blocks:  $bc"
    # Rough size: bc * block_size, in MiB
    size_mib=$(( bc / 1024 * block_size / 1024 ))
    echo "  Approx size:   ${size_mib} MiB"

    # Feature flags hint family
    fc_hex=$(hex_at $((1024 + 0x5C)) 4)
    fi_hex=$(hex_at $((1024 + 0x60)) 4)
    echo "  feature_compat:   0x$fc_hex (LE)"
    echo "  feature_incompat: 0x$fi_hex (LE)"

    # Volume label at sb+0x78, 16 bytes
    label=$(ascii_at $((1024 + 0x78)) 16 | sed 's/\.*$//')
    echo "  Volume label:  '$label'"

    # UUID at sb+0x68, 16 bytes
    uuid_raw=$(hex_at $((1024 + 0x68)) 16)
    echo "  UUID:          ${uuid_raw:0:8}-${uuid_raw:8:4}-${uuid_raw:12:4}-${uuid_raw:16:4}-${uuid_raw:20:12}"
    exit 0
fi

# ----- Linux swap ---------------------------------------------------
# Magic "SWAPSPACE2" or "SWAP-SPACE" in the last 10 bytes of the first page.
# Try 4K, 8K, 16K, 32K, 64K page sizes.
for psz in 4096 8192 16384 32768 65536; do
    off=$((psz - 10))
    [ $off -ge $((64 * 1024)) ] && continue
    tail=$(ascii_at $off 10)
    case "$tail" in
        SWAPSPACE2)
            echo "  Match: Linux swap v1 (page size = $psz)"
            ver_hex=$(hex_at 1024 4)
            lp_hex=$(hex_at 1028 4)
            uuid_raw=$(hex_at $((1024 + 12)) 16)
            label=$(ascii_at $((1024 + 28)) 16 | sed 's/\.*$//')
            echo "  Version:       0x$ver_hex (LE)"
            echo "  Last page:     0x$lp_hex (LE)"
            echo "  Volume label:  '$label'"
            echo "  UUID:          ${uuid_raw:0:8}-${uuid_raw:8:4}-${uuid_raw:12:4}-${uuid_raw:16:4}-${uuid_raw:20:12}"
            exit 0
            ;;
        SWAP-SPACE)
            echo "  Match: Linux swap (legacy v0, page size = $psz)"
            exit 0
            ;;
    esac
done

# ----- BSD UFS / A/UX UFS ------------------------------------------
# Classic UFS superblock at offset 8192; magic 0x011954 (LE: 54 19 01 00)
# at offset 1372 (0x55C) into the superblock => abs offset 9564 (0x255C).
ufs_magic=$(hex_at 9564 4)
if [ "$ufs_magic" = "54190100" ]; then
    echo "  Match: BSD UFS / A/UX UFS (magic 0x011954 at +0x255C)"
    exit 0
fi

# ----- HFS-ish (sanity) --------------------------------------------
hfs_magic=$(hex_at 1024 2)
case "$hfs_magic" in
    4244) echo "  Match: classic HFS (BD magic at +1024)"; exit 0 ;;
    482b) echo "  Match: HFS+ (H+ magic at +1024)";        exit 0 ;;
    4858) echo "  Match: HFSX (HX magic at +1024)";        exit 0 ;;
esac

# ----- Fallback raw dump -------------------------------------------
echo "  No known signature matched. Raw dumps for human eyeballs:"
echo
echo "  --- first 32 bytes ---"
dd if="$TMP" bs=32 count=1 2>/dev/null | od -An -c | head -2
echo "  --- bytes 1024..1088 (would-be ext/HFS sb start) ---"
dd if="$TMP" bs=1 skip=1024 count=64 2>/dev/null | od -An -c | head -4
echo "  --- last 32 bytes of 4K page ---"
dd if="$TMP" bs=1 skip=4064 count=32 2>/dev/null | od -An -c | head -2
echo "  --- last 32 bytes of 64K page ---"
dd if="$TMP" bs=1 skip=65504 count=32 2>/dev/null | od -An -c | head -2
exit 2
