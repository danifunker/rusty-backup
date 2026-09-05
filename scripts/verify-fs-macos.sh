#!/bin/bash
# Let Mac OS judge rb-cli's HFS / HFS+ / MFS edits: rb-cli formats and edits
# the images, hdiutil attaches them, and fsck_hfs -n plus the kernel's own HFS+
# driver give the verdict. Written for the 2026-09-01 audit (docs/Regression_Bugs.md,
# "macOS verification"); rerun it whenever the HFS writers change.
#   bash scripts/verify-fs-macos.sh [-o H1,H7] [-r target/debug/rb-cli] [-w /tmp/dir]
set -u
RB="$(pwd)/target/debug/rb-cli"
WORK="${TMPDIR:-/tmp}/rb-verify-fs-macos"
ONLY=""
while getopts "o:r:w:" opt; do
    case $opt in
        o) ONLY=$OPTARG ;;
        r) RB=$OPTARG ;;
        w) WORK=$OPTARG ;;
        *) echo "usage: $0 [-o IDs] [-r rb-cli] [-w workdir]" >&2; exit 2 ;;
    esac
done
mkdir -p "$WORK"
LOG="$WORK/results.log"
: > "$LOG"
# Devices attached so far, in a file: the attach helpers run inside $(...)
# subshells, where a shell array would never reach the exit trap.
ATTACHED_LIST="$WORK/attached.txt"
: > "$ATTACHED_LIST"

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "$LOG"; }
# rb-cli with its output logged; returns its exit code.
rb() {
    log "rb-cli $*"
    "$RB" "$@" >>"$LOG" 2>&1
    local code=$?
    [ $code -ne 0 ] && log "  rb-cli exit $code"
    return $code
}
rbq() { "$RB" "$@" >>"$LOG" 2>&1; }
# Attach an image without mounting; prints the whole-disk device.
attach_raw() {
    local dev
    dev=$(hdiutil attach -nomount -imagekey diskimage-class=CRawDiskImage "$1" 2>>"$LOG" | awk 'NR==1{print $1}')
    [ -n "$dev" ] || { log "  attach failed for $1"; return 1; }
    echo "$dev" >> "$ATTACHED_LIST"
    echo "$dev"
}
# Attach and mount read-only through the kernel driver; prints the mount point.
attach_mount() {
    local line dev mnt
    line=$(hdiutil attach -readonly -imagekey diskimage-class=CRawDiskImage "$1" 2>>"$LOG" | tail -1)
    dev=$(echo "$line" | awk '{print $1}')
    mnt=$(echo "$line" | awk -F'\t' '{print $3}' | sed 's/^ *//;s/ *$//')
    [ -d "$mnt" ] || { log "  mount failed for $1: $line"; return 1; }
    echo "${dev%s[0-9]*}" >> "$ATTACHED_LIST"
    echo "$mnt"
}
detach() {
    hdiutil detach "$1" >>"$LOG" 2>&1 || hdiutil detach -force "$1" >>"$LOG" 2>&1
}
detach_all() {
    while read -r d; do [ -n "$d" ] && detach "$d"; done < "$ATTACHED_LIST"
    : > "$ATTACHED_LIST"
}
trap detach_all EXIT
# fsck_hfs -n on a raw device; passes only on "appears to be OK".
mac_fsck() {
    local dev=${1/\/dev\/disk//dev/rdisk} tag=$2 out
    out=$(fsck_hfs -n "$dev" 2>&1)
    printf '%s\n' "$out" >>"$LOG"
    if echo "$out" | grep -q "appears to be OK"; then
        log "  fsck_hfs -n $dev ($tag): OK"
        return 0
    fi
    log "  fsck_hfs -n $dev ($tag): NOT CLEAN"
    echo "$out" | grep -v "^\*\* \|^   Executing" | head -5 | sed 's/^/    /' | tee -a "$LOG"
    return 1
}
# Leaf record count of the extents-overflow B-tree of a bare HFS / HFS+ image.
xt_leaf_records() {
    python3 - "$1" "$2" <<'EOF'
import struct, sys
img, fs = sys.argv[1], sys.argv[2]
f = open(img, 'rb'); f.seek(1024); hdr = f.read(512)
if fs == 'hfsplus':
    bs = struct.unpack('>I', hdr[0x28:0x2c])[0]
    start = struct.unpack('>I', hdr[0xD0:0xD4])[0]
    off = start * bs
else:
    albs = struct.unpack('>I', hdr[0x14:0x18])[0]
    alst = struct.unpack('>H', hdr[0x1c:0x1e])[0]
    start = struct.unpack('>H', hdr[0x86:0x88])[0]
    off = alst * 512 + start * albs
f.seek(off); node = f.read(512)
print(struct.unpack('>I', node[20:24])[0])
EOF
}
RESULTS=()
result() {
    local id=$1 pass=$2 note=$3 verdict
    [ "$pass" = 1 ] && verdict=PASS || verdict=FAIL
    RESULTS+=("$id $verdict $note")
    log "RESULT $id $verdict: $note"
}
wanted() { [ -z "$ONLY" ] || echo ",$ONLY," | grep -q ",$1,"; }
# A tree of 1500 small unique files: enough catalog records to split a 4 KiB
# HFS+ node dozens of times and a 512-byte HFS node hundreds of times.
make_tree() {
    local dir=$1
    rm -rf "$dir"; mkdir -p "$dir"
    for i in $(seq -w 1 1500); do printf 'file %s %s\n' "$i" "$(printf 'x%.0s' $(seq 1 $((10#$i % 37 + 1))))" > "$dir/f$i.txt"; done
}
# Every file under $2 must read back byte-identical from $1 (a mount point).
compare_tree() {
    local mnt=$1 dir=$2 bad=0 n=0
    for f in "$dir"/*; do
        n=$((n + 1))
        cmp -s "$f" "$mnt/$(basename "$f")" || { bad=$((bad + 1)); [ $bad -le 3 ] && log "  differs: $(basename "$f")"; }
    done
    log "  $n files compared through the kernel driver, $bad differ"
    [ $bad -eq 0 ]
}
compare_tree_via_rb() {
    local img=$1 dir=$2 out="$WORK/get-$(basename "$img")" bad=0 n=0
    rm -rf "$out"; mkdir -p "$out"
    rbq get -r "$img" / "$out" || { log "  rb-cli get -r failed"; return 1; }
    for f in "$dir"/*; do
        n=$((n + 1))
        cmp -s "$f" "$out/$(basename "$f")" || { bad=$((bad + 1)); [ $bad -le 3 ] && log "  differs: $(basename "$f")"; }
    done
    log "  $n files compared through rb-cli get, $bad differ"
    [ $bad -eq 0 ]
}

log "rb-cli: $RB  work: $WORK"
make_tree "$WORK/tree"

# ---------------------------------------------------------------- H1 / H2 / H4 / H9 / H11
# ee07cf4: B-tree index consistency, first-child descent, bounds, clamped dates.
# Fill until the catalog splits several times, delete from the middle, add again.
for fs in hfsplus hfs; do
    id="H1-$fs"; wanted "$id" || continue
    img="$WORK/h1-$fs.img"
    ok=1
    rb new volume "$fs" "$img" --size 64M --name "H1$fs" || ok=0
    rb import "$img" "$WORK/tree" || ok=0
    rb rm "$img" 'f0[56]??.txt' || ok=0
    rb import --skip-existing "$img" "$WORK/tree" || ok=0
    rbok=1; rb fsck --checkonly "$img" || rbok=0
    dev=$(attach_raw "$img") && mac_fsck "$dev" "$id" || ok=0
    if [ "$fs" = hfsplus ]; then
        detach "$dev"
        mnt=$(attach_mount "$img") && compare_tree "$mnt" "$WORK/tree" || ok=0
        detach_all
    else
        # macOS 10.15+ no longer mounts classic HFS: read back through rb-cli.
        detach_all
        compare_tree_via_rb "$img" "$WORK/tree" || ok=0
    fi
    result "$id" $ok "1500 files, 200 deleted from the middle and re-added, rb_fsck_clean=$rbok"
done

# ---------------------------------------------------------------- H3
# 67ab9f2: deleting a file whose resource fork spilled into the extents
# overflow file frees those extents. The volume is filled, every other file is
# removed, and a 1 MB resource fork has to spread over the holes.
for fs in hfsplus hfs; do
    id="H3-$fs"; wanted "$id" || continue
    img="$WORK/h3-$fs.img"
    ok=1
    rb new volume "$fs" "$img" --size 8M --name "H3$fs" || ok=0
    head -c 65536 /dev/urandom > "$WORK/chunk.bin"
    n=0
    for i in $(seq -w 1 200); do rbq put "$img" "$WORK/chunk.bin" "f$i.bin" || break; n=$i; done
    log "  filled with $n files"
    for i in $(seq -w 1 2 "$n"); do rbq rm "$img" "f$i.bin"; done
    head -c 100000 /dev/urandom > "$WORK/big.bin"
    head -c 1000000 /dev/urandom > "$WORK/big.bin.rsrc"
    if ! rb put "$img" "$WORK/big.bin" big.bin; then
        result "$id" 0 "put of the fragmented fork refused (see log)"
        continue
    fi
    before=$(xt_leaf_records "$img" "$fs")
    log "  extents-overflow leaf records with the fork present: $before"
    [ "$before" -gt 0 ] || ok=0
    dev=$(attach_raw "$img") && mac_fsck "$dev" "$id before delete" || ok=0
    detach_all
    # Kept for verify-hfs-snow.sh, which judges the fork before and after the delete.
    cp "$img" "${img%.img}-before.img"
    rb rm "$img" big.bin || ok=0
    after=$(xt_leaf_records "$img" "$fs")
    log "  extents-overflow leaf records after the delete: $after"
    [ "$after" -eq 0 ] || ok=0
    rbok=1; rb fsck --checkonly "$img" || rbok=0
    dev=$(attach_raw "$img") && mac_fsck "$dev" "$id after delete" || ok=0
    detach_all
    result "$id" $ok "overflow records $before -> $after, rb_fsck_clean=$rbok"
done

# ---------------------------------------------------------------- H5
# 3a53254: fsck checks key order across leaves and index separators; a volume
# Mac OS itself formatted and filled must not draw "keys out of order".
if wanted H5; then
    img="$WORK/h5-good.dmg"
    ok=1
    rm -f "$img"
    hdiutil create -size 64m -fs HFS+ -layout NONE -volname GoodHFS "$img" >>"$LOG" 2>&1 || ok=0
    line=$(hdiutil attach -imagekey diskimage-class=CRawDiskImage "$img" 2>>"$LOG" | tail -1)
    mnt=$(echo "$line" | awk -F'\t' '{print $3}' | sed 's/^ *//;s/ *$//')
    if [ -d "$mnt" ]; then
        cp "$WORK/tree"/* "$mnt/" && rm "$mnt"/f0[56]??.txt && cp "$WORK/tree"/f0[56]??.txt "$mnt/"
        sync
        detach "$(echo "$line" | awk '{print $1}')"
    else
        log "  could not mount $img: $line"; ok=0
    fi
    out=$("$RB" fsck --checkonly "$img" 2>&1); printf '%s\n' "$out" >>"$LOG"
    echo "$out" | grep -qi "out of order" && { ok=0; log "  keys out of order reported"; }
    echo "$out" | grep -q "^fsck:" || { ok=0; log "  rb-cli fsck did not finish clean"; }
    result H5 $ok "macOS-formatted HFS+ with 1500 files, 200 replaced: $(echo "$out" | tail -1)"
fi

# ---------------------------------------------------------------- H6
# a1f7558: MFS physical fork lengths in whole allocation blocks. The judge is
# Disk First Aid inside Mini vMac; this only builds the image and checks it.
if wanted H6; then
    img="$WORK/h6-mfs.img"
    ok=1
    rb new floppy mfs "$img" --name "H6 MFS" || ok=0
    for i in 1 2 3; do head -c $((1000 * i + 7)) /dev/urandom > "$WORK/mfs$i.bin"; rb put "$img" "$WORK/mfs$i.bin" "File $i" || ok=0; done
    rb rm "$img" "File 2" || ok=0
    rb put "$img" "$WORK/tree/f0001.txt" "f0001.txt" || ok=0
    rb fsck --checkonly "$img" || ok=0
    result H6 $ok "image built at $img; open it in Mini vMac and run Disk First Aid"
fi

# ---------------------------------------------------------------- H7
# 9cb5383: the alternate MDB / volume header sits 1024 bytes before the
# partition end. A volume smaller than its APM partition, then edited and grown.
# Classic HFS must fill its partition (R-058): the fill grows it, which needs
# spare bits in the fixed volume bitmap, so that volume is 28800K (57600
# blocks, 15 bitmap sectors = 61440 bits, enough for 30M) rather than 24M.
for fs in hfs hfsplus; do
    id="H7-$fs"; wanted "$id" || continue
    img="$WORK/h7-$fs.img"
    vol="$WORK/h7-$fs-vol.img"
    ok=1
    if [ "$fs" = hfs ]; then size=28800K; else size=24M; fi
    rb new volume "$fs" "$vol" --size "$size" --name "H7$fs" || ok=0
    rb new hd apm "$img" --size 40M --partition "30M:Apple_HFS:H7" --partition "rest:Apple_HFS:Rest" --fill "1=$vol" || ok=0
    rb put "$img@1" "$WORK/tree/f0001.txt" f0001.txt || ok=0
    dev=$(attach_raw "$img") && mac_fsck "${dev}s2" "$id $size volume poured into a 30M partition" || ok=0
    detach_all
    if [ "$fs" = hfsplus ]; then
        rb resize "$img@1" --size 30M || ok=0
    fi
    rb put "$img@1" "$WORK/tree/f0002.txt" f0002.txt || ok=0
    rbok=1; rb fsck --checkonly "$img@1" || rbok=0
    dev=$(attach_raw "$img") && mac_fsck "${dev}s2" "$id at the partition size" || ok=0
    detach_all
    result "$id" $ok "alternate header accepted before and after the grow, rb_fsck_clean=$rbok"
done

log "---- summary ----"
for r in "${RESULTS[@]}"; do log "$r"; done
