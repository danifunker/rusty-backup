#!/bin/sh
# scripts/capture-apm-disk.sh
#
# Capture the exact on-disk layout of an Apple Partition Map disk — the shape a
# real Mac wrote — without copying its data. Run this on the machine the disk is
# attached to (over ssh is fine); it produces a few MB you copy back and feed to
# `rb-cli show partmap`, or to scripts/assemble-apm-skeleton.sh.
#
# STRICTLY READ-ONLY. Every access is `dd if=DEVICE`; nothing writes to the disk.
#
#   sudo sh capture-apm-disk.sh /dev/sdb   out-dir
#   sudo sh capture-apm-disk.sh /dev/rdisk4 out-dir     # macOS: prefer /dev/rdiskN
#
# What it collects:
#   head.bin        first HEAD_MIB of the disk — DDR, the whole partition map,
#                   and the Apple driver / patch partitions that live just after
#   tail.bin        last EDGE_MIB of the physical disk
#   part-NN-head.bin / part-NN-tail.bin
#                   first and last EDGE_MIB of each partition — HFS boot blocks,
#                   the MDB / volume header, and the alternate copy at the end
#   layout.txt      the partition map decoded by this script (od + awk), so it
#   layout.tsv      is ground truth independent of rusty-backup's own parser
#   os-report.txt   what the host OS and its native tools say about the disk
#   SHA256SUMS      so the capture can be verified after it moves
#
# Env overrides: HEAD_MIB (default 8), EDGE_MIB (default 16).
#
# EDGE_MIB has to clear the *embedded* volume header of an HFS-wrapped HFS+
# partition, which sits drAlBlSt + 5 * drAlBlkSiz into the partition — about
# 6 MB on a 78 GB volume, and it scales with volume size. 2 MiB was not enough
# on a real Mac OS 9 disk; raise it further for volumes much past 100 GB.

set -eu

DEV=${1-}
OUT=${2-apm-capture}
HEAD_MIB=${HEAD_MIB:-8}
EDGE_MIB=${EDGE_MIB:-16}

if [ -z "$DEV" ]; then
    cat >&2 <<'USAGE'
usage: capture-apm-disk.sh DEVICE [OUTDIR]

Find the device first:
  Linux   lsblk -o NAME,SIZE,MODEL,TRAN          -> /dev/sdb
  macOS   diskutil list                          -> /dev/disk4 (use /dev/rdisk4)
  *BSD    dmesg | tail                           -> /dev/da0

Run it as root; raw device reads need privileges. It never writes to DEVICE.
USAGE
    exit 2
fi

[ -e "$DEV" ] || { echo "capture-apm-disk: $DEV does not exist" >&2; exit 1; }
[ -r "$DEV" ] || { echo "capture-apm-disk: $DEV is not readable — run as root" >&2; exit 1; }

mkdir -p "$OUT"
LOG="$OUT/capture.log"
: > "$LOG"

say() { echo "$@"; echo "$@" >> "$LOG"; }

# ---------------------------------------------------------------------------
# Big-endian readers. `od` on a regular file has no alignment constraints, which
# is why everything below parses head.bin rather than the device itself — a raw
# character device on macOS refuses unaligned or short reads.
# ---------------------------------------------------------------------------
# Accumulate across every field od emits: it wraps long output onto several
# lines and some od builds add a trailing blank one, which a positional
# `$1*256+$2` silently mis-reads.
bytes_be() {
    od -A n -t u1 -j "$2" -N "$3" "$1" |
        awk '{for (i = 1; i <= NF; i++) v = v * 256 + $i} END {printf "%.0f", v}'
}
be16() { bytes_be "$1" "$2" 2; }
be32() { bytes_be "$1" "$2" 4; }
# Fixed-width C string field, non-printables stripped.
cstr() {
    dd if="$1" bs=1 skip="$2" count="$3" 2>/dev/null |
        LC_ALL=C tr -d '\000' | LC_ALL=C tr -cd '[:print:]'
}

sha_of() {
    if command -v sha256sum >/dev/null 2>&1; then sha256sum "$@"
    elif command -v shasum >/dev/null 2>&1; then shasum -a 256 "$@"
    else echo "(no sha256 tool available)"; fi
}

# ---------------------------------------------------------------------------
# 0. Provenance
# ---------------------------------------------------------------------------
{
    echo "captured:  $(date -u '+%Y-%m-%dT%H:%M:%SZ') UTC"
    echo "device:    $DEV"
    echo "host:      $(uname -a)"
    echo "head_mib:  $HEAD_MIB"
    echo "edge_mib:  $EDGE_MIB"
} > "$OUT/capture-info.txt"

# ---------------------------------------------------------------------------
# 1. What the host OS thinks, via its own tools — an independent cross-check on
#    everything this script and rusty-backup later decode from the raw bytes.
# ---------------------------------------------------------------------------
DISK_BYTES=""
{
    echo "=== uname ==="; uname -a
    case "$(uname -s)" in
    Linux)
        echo; echo "=== lsblk ==="
        lsblk -b -o NAME,SIZE,MODEL,SERIAL,TRAN "$DEV" 2>&1 || true
        echo; echo "=== blockdev ==="
        blockdev --getsize64 --getss --getpbsz "$DEV" 2>&1 || true
        echo; echo "=== parted print ==="
        parted -s "$DEV" unit s print 2>&1 || true
        echo; echo "=== mac-fdisk ==="
        mac-fdisk -l "$DEV" 2>&1 || echo "(mac-fdisk not installed — apt install mac-fdisk)"
        echo; echo "=== fdisk ==="
        fdisk -l "$DEV" 2>&1 || true
        ;;
    Darwin)
        echo; echo "=== diskutil list ==="; diskutil list "$DEV" 2>&1 || true
        echo; echo "=== diskutil info ==="; diskutil info "$DEV" 2>&1 || true
        echo; echo "=== pdisk dump ==="
        pdisk "$DEV" -dump 2>&1 || echo "(pdisk not present on this macOS)"
        ;;
    *)
        echo; echo "(no native partition tooling wired up for $(uname -s))"
        ;;
    esac
} > "$OUT/os-report.txt" 2>&1

case "$(uname -s)" in
Linux)  DISK_BYTES=$(blockdev --getsize64 "$DEV" 2>/dev/null || echo "") ;;
Darwin) DISK_BYTES=$(diskutil info "$DEV" 2>/dev/null |
            sed -n 's/.*(\([0-9][0-9]*\) Bytes).*/\1/p' | head -1) ;;
esac
# Pointed at an image file rather than a device (handy for testing this script).
if [ -z "$DISK_BYTES" ] && [ -f "$DEV" ]; then
    DISK_BYTES=$(wc -c < "$DEV" | tr -d ' ')
fi

# ---------------------------------------------------------------------------
# 2. The head: DDR + partition map + driver partitions, in one aligned read.
# ---------------------------------------------------------------------------
say "Reading the first ${HEAD_MIB} MiB of $DEV ..."
dd if="$DEV" of="$OUT/head.bin" bs=512 count=$((HEAD_MIB * 2048)) 2>>"$LOG"

# ---------------------------------------------------------------------------
# 3. Decode the Driver Descriptor Record (block 0).
# ---------------------------------------------------------------------------
DDR_SIG=$(be16 "$OUT/head.bin" 0)
BLKSIZE=$(be16 "$OUT/head.bin" 2)
BLKCOUNT=$(be32 "$OUT/head.bin" 4)

if [ "$DDR_SIG" -ne 17746 ]; then          # 0x4552 'ER'
    say "WARNING: block 0 signature is 0x$(printf '%04X' "$DDR_SIG"), not 0x4552 ('ER')."
    say "         This disk may not be APM. Capturing what is there anyway."
    [ "$BLKSIZE" -gt 0 ] 2>/dev/null || BLKSIZE=512
fi
[ "$BLKSIZE" -gt 0 ] 2>/dev/null || BLKSIZE=512

{
    echo "Driver Descriptor Record (block 0)"
    printf '  sbSig        0x%04X %s\n' "$DDR_SIG" \
        "$([ "$DDR_SIG" -eq 17746 ] && echo "('ER', valid)" || echo "(NOT 'ER')")"
    echo "  sbBlkSize    $BLKSIZE"
    echo "  sbBlkCount   $BLKCOUNT   (= $(awk -v b="$BLKCOUNT" -v s="$BLKSIZE" \
        'BEGIN{printf "%.0f", b*s}') bytes the formatter believed the disk to be)"
    echo "  sbDevType    $(be16 "$OUT/head.bin" 8)"
    echo "  sbDevId      $(be16 "$OUT/head.bin" 10)"
    echo "  sbData       $(be32 "$OUT/head.bin" 12)"
    echo "  sbDrvrCount  $(be16 "$OUT/head.bin" 16)"
    ndrv=$(be16 "$OUT/head.bin" 16)
    i=0
    while [ "$i" -lt "$ndrv" ] && [ "$i" -lt 61 ]; do
        off=$((18 + i * 8))
        echo "    driver[$i] ddBlock=$(be32 "$OUT/head.bin" $off)" \
             "ddSize=$(be16 "$OUT/head.bin" $((off + 4)))" \
             "ddType=$(be16 "$OUT/head.bin" $((off + 6)))"
        i=$((i + 1))
    done
    echo
    echo "Host OS reports: ${DISK_BYTES:-unknown} bytes"
    if [ -n "$DISK_BYTES" ]; then
        awk -v ddr="$(awk -v b="$BLKCOUNT" -v s="$BLKSIZE" 'BEGIN{printf "%.0f", b*s}')" \
            -v os="$DISK_BYTES" 'BEGIN{
                if (ddr == os) print "  -> matches the DDR exactly"
                else printf "  -> DIFFERS from the DDR by %.0f bytes (%.2f GB)\n", \
                     os-ddr, (os-ddr)/1000000000
            }'
    fi
    echo
    echo "Partition map (block 1 onward, ${BLKSIZE}-byte blocks)"
} > "$OUT/layout.txt"

printf 'idx\tstart_block\tblock_count\tstart_bytes\tsize_bytes\ttype\tname\tstatus\tdata_start\tdata_count\tboot_start\tboot_size\tprocessor\n' \
    > "$OUT/layout.tsv"

# ---------------------------------------------------------------------------
# 4. Walk the partition map, decode each entry, and grab that partition's ends.
# ---------------------------------------------------------------------------
EDGE_BLOCKS=$((EDGE_MIB * 1024 * 1024 / BLKSIZE))
COUNT=0
IDX=0
while : ; do
    OFF=$(( (1 + IDX) * BLKSIZE ))
    # Stop if the entry would fall outside what we captured.
    [ $((OFF + BLKSIZE)) -le $((HEAD_MIB * 1024 * 1024)) ] || break

    SIG=$(be16 "$OUT/head.bin" "$OFF")
    [ "$SIG" -eq 20557 ] || break            # 0x504D 'PM'

    MAPCNT=$(be32 "$OUT/head.bin" $((OFF + 4)))
    START=$(be32 "$OUT/head.bin" $((OFF + 8)))
    BLOCKS=$(be32 "$OUT/head.bin" $((OFF + 12)))
    NAME=$(cstr "$OUT/head.bin" $((OFF + 16)) 32)
    PTYPE=$(cstr "$OUT/head.bin" $((OFF + 48)) 32)
    DSTART=$(be32 "$OUT/head.bin" $((OFF + 80)))
    DCOUNT=$(be32 "$OUT/head.bin" $((OFF + 84)))
    STATUS=$(be32 "$OUT/head.bin" $((OFF + 88)))
    BSTART=$(be32 "$OUT/head.bin" $((OFF + 92)))
    BSIZE=$(be32 "$OUT/head.bin" $((OFF + 96)))
    PROC=$(cstr "$OUT/head.bin" $((OFF + 120)) 16)

    [ "$COUNT" -eq 0 ] && COUNT=$MAPCNT
    NN=$(printf '%02d' "$IDX")
    SB=$((START * BLKSIZE))
    SZ=$((BLOCKS * BLKSIZE))

    {
        printf '  [%02d] %-24s %-20s\n' "$IDX" "$PTYPE" "$NAME"
        printf '       start block %-12s (%s bytes)\n' "$START" "$SB"
        printf '       blocks      %-12s (%s bytes, %.2f GB)\n' "$BLOCKS" "$SZ" \
            "$(awk -v v="$SZ" 'BEGIN{printf "%.2f", v/1000000000}')"
        printf '       pmMapBlkCnt %s   pmPartStatus 0x%08X\n' "$MAPCNT" "$STATUS"
        printf '       pmLgDataStart %s  pmDataCnt %s\n' "$DSTART" "$DCOUNT"
        printf '       pmLgBootStart %s  pmBootSize %s  pmProcessor "%s"\n' \
            "$BSTART" "$BSIZE" "$PROC"
    } >> "$OUT/layout.txt"

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t0x%08X\t%s\t%s\t%s\t%s\t%s\n' \
        "$IDX" "$START" "$BLOCKS" "$SB" "$SZ" "$PTYPE" "$NAME" "$STATUS" \
        "$DSTART" "$DCOUNT" "$BSTART" "$BSIZE" "$PROC" >> "$OUT/layout.tsv"

    # Both ends of the partition. Apple_Free holds nothing; skip it. A partition
    # smaller than two edges is captured whole instead of twice over.
    if [ "$PTYPE" != "Apple_Free" ] && [ "$BLOCKS" -gt 0 ]; then
        if [ "$BLOCKS" -le $((EDGE_BLOCKS * 2)) ]; then
            dd if="$DEV" of="$OUT/part-$NN-whole.bin" bs="$BLKSIZE" \
                skip="$START" count="$BLOCKS" 2>>"$LOG" || true
        else
            dd if="$DEV" of="$OUT/part-$NN-head.bin" bs="$BLKSIZE" \
                skip="$START" count="$EDGE_BLOCKS" 2>>"$LOG" || true
            dd if="$DEV" of="$OUT/part-$NN-tail.bin" bs="$BLKSIZE" \
                skip=$((START + BLOCKS - EDGE_BLOCKS)) count="$EDGE_BLOCKS" 2>>"$LOG" || true
        fi
    fi

    IDX=$((IDX + 1))
    [ "$COUNT" -gt 0 ] && [ "$IDX" -ge "$COUNT" ] && break
done

say "Decoded $IDX partition map entries (pmMapBlkCnt says $COUNT)."

# ---------------------------------------------------------------------------
# 5. The physical tail — is there anything past the last partition, and does the
#    disk end where the DDR claims?
# ---------------------------------------------------------------------------
if [ -n "$DISK_BYTES" ]; then
    TAIL_BLOCKS=$((EDGE_MIB * 1024 * 1024 / 512))
    TOTAL_512=$((DISK_BYTES / 512))
    say "Reading the last ${EDGE_MIB} MiB of the physical disk ..."
    dd if="$DEV" of="$OUT/tail.bin" bs=512 \
        skip=$((TOTAL_512 - TAIL_BLOCKS)) count="$TAIL_BLOCKS" 2>>"$LOG" || true
    echo "physical_disk_bytes: $DISK_BYTES" >> "$OUT/capture-info.txt"
else
    say "Could not learn the physical disk size; skipping the tail capture."
fi

echo "apm_block_size:  $BLKSIZE"  >> "$OUT/capture-info.txt"
echo "apm_block_count: $BLKCOUNT" >> "$OUT/capture-info.txt"
echo "map_entries:     $IDX"      >> "$OUT/capture-info.txt"

# ---------------------------------------------------------------------------
# 6. Seal it.
# ---------------------------------------------------------------------------
( cd "$OUT" && sha_of ./*.bin ./*.txt ./*.tsv > SHA256SUMS 2>/dev/null ) || true

ARCHIVE="$OUT.tar.gz"
tar czf "$ARCHIVE" "$OUT" 2>/dev/null || ARCHIVE=""

say ""
say "Captured to $OUT ($(du -sh "$OUT" 2>/dev/null | awk '{print $1}'))"
[ -n "$ARCHIVE" ] && say "Archive: $ARCHIVE ($(du -h "$ARCHIVE" | awk '{print $1}'))"
say "Read layout.txt for the decoded map. Copy it back with:"
say "    scp USER@HOST:$(pwd)/${ARCHIVE:-$OUT} ."
