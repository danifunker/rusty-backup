#!/bin/sh
# scripts/assemble-apm-skeleton.sh
#
# Rebuild a capture from scripts/capture-apm-disk.sh into a sparse disk image of
# the original's exact size, with every captured region at its true offset. The
# file reports as (say) 120 GB but occupies only the few MB that were captured,
# so `rb-cli show partmap` / `inspect` / the GUI see a faithful disk without
# anyone hauling the real one over the network.
#
#   sh assemble-apm-skeleton.sh apm-capture/ mac9-120gb-skeleton.img
#
# Everything not captured reads back as zeros. That is the point: the layout is
# exact, the bulk data is absent, and anything the tools get wrong about the
# *shape* of the disk shows up immediately.

set -eu

IN=${1-}
IMG=${2-}

if [ -z "$IN" ] || [ -z "$IMG" ]; then
    echo "usage: assemble-apm-skeleton.sh CAPTURE_DIR OUTPUT.img" >&2
    exit 2
fi
[ -d "$IN" ] || { echo "assemble: $IN is not a directory" >&2; exit 1; }
[ -f "$IN/head.bin" ] || { echo "assemble: $IN/head.bin missing" >&2; exit 1; }
[ -f "$IN/layout.tsv" ] || { echo "assemble: $IN/layout.tsv missing" >&2; exit 1; }

info() { sed -n "s/^$1: *//p" "$IN/capture-info.txt" | head -1; }

BLKSIZE=$(info apm_block_size)
BLKCOUNT=$(info apm_block_count)
DISK_BYTES=$(info physical_disk_bytes)
[ -n "$BLKSIZE" ] || BLKSIZE=512

# Prefer the size the host OS measured; fall back to what the DDR claimed.
if [ -z "$DISK_BYTES" ]; then
    DISK_BYTES=$(awk -v b="$BLKCOUNT" -v s="$BLKSIZE" 'BEGIN{printf "%.0f", b*s}')
    echo "note: no host-measured size in the capture; using the DDR's $DISK_BYTES bytes"
fi

if [ -e "$IMG" ]; then
    echo "assemble: $IMG already exists — remove it first" >&2
    exit 1
fi

echo "Creating a sparse $DISK_BYTES-byte image at $IMG ..."
if command -v truncate >/dev/null 2>&1; then
    truncate -s "$DISK_BYTES" "$IMG"
else
    dd if=/dev/zero of="$IMG" bs=1 count=0 seek="$DISK_BYTES" 2>/dev/null
fi

place() {   # file, byte offset
    [ -f "$1" ] || return 0
    dd if="$1" of="$IMG" bs="$BLKSIZE" seek=$(($2 / BLKSIZE)) conv=notrunc 2>/dev/null
    echo "  placed $(basename "$1") at byte $2"
}

place "$IN/head.bin" 0

# Per-partition regions, positioned from the captured map.
tail -n +2 "$IN/layout.tsv" | while IFS="$(printf '\t')" read -r idx start blocks sb sz rest; do
    nn=$(printf '%02d' "$idx")
    place "$IN/part-$nn-whole.bin" "$sb"
    place "$IN/part-$nn-head.bin" "$sb"
    if [ -f "$IN/part-$nn-tail.bin" ]; then
        tsz=$(wc -c < "$IN/part-$nn-tail.bin" | tr -d ' ')
        place "$IN/part-$nn-tail.bin" $((sb + sz - tsz))
    fi
done

if [ -f "$IN/tail.bin" ]; then
    tsz=$(wc -c < "$IN/tail.bin" | tr -d ' ')
    place "$IN/tail.bin" $((DISK_BYTES - tsz))
fi

echo
echo "Skeleton ready: $IMG"
echo "  apparent size $DISK_BYTES bytes, actually using $(du -h "$IMG" | awk '{print $1}')"
echo
echo "Now compare our parse against the capture's own (layout.txt):"
echo "    rb-cli show partmap $IMG"
echo "    rb-cli inspect $IMG"
