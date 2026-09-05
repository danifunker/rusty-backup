#!/bin/bash
# Let Mac OS 7 judge rb-cli's classic HFS and MFS volumes: Disk First Aid 7.2
# runs on System 7.1 inside the Snow emulator (a headless Macintosh II), the
# Finder mounts, copies and opens files, and the framebuffer is the verdict.
# Companion to verify-fs-macos.sh, whose judge is fsck_hfs -n (and which is
# the only judge for HFS+: Disk First Aid 7.x does not know HFS+, and the Mac
# OS 8.1 that does needs a 68040 Snow does not emulate).
#
#   bash scripts/verify-hfs-snow.sh leg3 -w DIR        # the H1 / H6 / H7 volumes verify-fs-macos.sh -w DIR left behind
#   bash scripts/verify-hfs-snow.sh dfa DISK.hda [-i N] [-k KEYS] -o OUT
#                                                      # Disk First Aid "Verify" on the N-th volume icon of a SCSI disk
#   bash scripts/verify-hfs-snow.sh dfa-floppy IMG [-i N] -o OUT
#   bash scripts/verify-hfs-snow.sh finder-copy IMG -o OUT
#                                                      # the Finder copies every root file of a floppy to the boot disk
#   bash scripts/verify-hfs-snow.sh open DISK.hda NAME... -o OUT
#                                                      # Find + Open: TeachText shows the named files
#   bash scripts/verify-hfs-snow.sh h3-real -w DIR     # the Finder fragments a fork on h3-hfs.img; rb-cli rm; both judges
#   bash scripts/verify-hfs-snow.sh mac-formatted -w DIR
#                                                      # System 7.1 formats a partition; rb-cli edits it; both judges + the Finder
#   bash scripts/verify-hfs-snow.sh wrap VOL.img OUT.hda [SIZE]
#                                                      # bare HFS volume -> APM disk with an Apple SCSI driver
#
# Pieces (override with the environment):
#   SNOW_HARNESS  the MacAtrium harness built inside Snow's testrunner crate:
#                 cp scripts/snow/macatrium_harness.rs ~/repos/snow/testrunner/src/bin/
#                 (cd ~/repos/snow && cargo build -r -p testrunner --bin macatrium_harness --features snow_core/mmap)
#   MACII_ROM     Macintosh II FDHD ROM; MDC_ROM the Display Card 8-24 ROM (3410868.bin from MAME's nb_mdc824.zip)
#   BOOT_HDA      a System 7.1 BlueSCSI disk that carries Disk First Aid under /Applications/Disk Utilities
#   RB            rb-cli
#   DRIVER_HDA    any Apple-formatted .hda whose Apple_Driver43 partition `wrap` may copy
#
# Timing is in CPU cycles (the harness runs uncapped, ~45 M cycles/s here):
# the desktop and a Startup Item are up by 2.0 G; clicks land at 2.3 G+.
set -u
SNOW_HARNESS="${SNOW_HARNESS:-$HOME/repos/snow/target/release/macatrium_harness}"
MACII_ROM="${MACII_ROM:-$HOME/repos/lbmactwo_MiSTer/releases/MacIIFDHD.rom}"
MDC_ROM="${MDC_ROM:-$HOME/Library/Application Support/Ample/roms/3410868.bin}"
BOOT_HDA="${BOOT_HDA:-$HOME/Documents/MacOS_SampleDisks/MacLC_7-1.hda}"
RB="${RB:-$(pwd)/target/debug/rb-cli}"
DRIVER_HDA="${DRIVER_HDA:-$HOME/Downloads/EmptyHFS.hda}"
DFA_PATH="/Applications/Disk Utilities/Disk First Aid"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { log "$*"; exit 1; }
need() { [ -e "$1" ] || die "missing $2: $1"; }

# Disk First Aid 7.2 window geometry at 640x480 (the window opens centred).
dfa_icon_x() { echo $((147 + $1 * 116)); }   # volume icons, 116 px apart
DFA_ICON_Y=150
DFA_VERIFY="130,254"
# "This is not a Macintosh disk: Do you want to initialize it?" -> Cancel.
INIT_CANCEL="229,176"

# One boot disk per run: a copy of BOOT_HDA with Disk First Aid in Startup
# Items (auto-launched, so no Finder navigation) or without it (Finder runs).
make_boot() {
    local out=$1 with_dfa=$2
    cp "$BOOT_HDA" "$out" || return 1
    [ "$with_dfa" = 1 ] || return 0
    local hqx="$out.dfa.hqx"
    "$RB" get-binhex "$BOOT_HDA" "$DFA_PATH" "$hqx" >/dev/null 2>&1 || die "Disk First Aid not found at $DFA_PATH on $BOOT_HDA"
    "$RB" put-binhex "$out" "$hqx" --dst-dir "/System Folder/Startup Items" >/dev/null 2>&1 || die "put-binhex into Startup Items failed"
    rm -f "$hqx"
}

# The MDC 8-24 ROM ships zipped in MAME's nb_mdc824.zip; unpack it beside the zip once.
ensure_mdc_rom() {
    [ -e "$MDC_ROM" ] && return 0
    local zip; zip="$(dirname "$MDC_ROM")/nb_mdc824.zip"
    [ -e "$zip" ] && unzip -o -q "$zip" 3410868.bin -d "$(dirname "$MDC_ROM")"
    need "$MDC_ROM" "MDC 8-24 ROM (3410868.bin from MAME's nb_mdc824.zip)"
}

# harness OUT BOOT MAX_CYCLES KEYS [extra harness args...]
harness() {
    local out=$1 boot=$2 max=$3 keys=$4; shift 4
    need "$SNOW_HARNESS" "harness (see the header)"; need "$MACII_ROM" "Mac II ROM"; ensure_mdc_rom
    rm -rf "$out"; mkdir -p "$out"
    local args=("$MACII_ROM" "$MDC_ROM" "$boot" "$out" "$max" --snap-every 500000000 --wall-secs 900 "$@")
    [ -n "$keys" ] && args+=(--keys "$keys")
    "$SNOW_HARNESS" "${args[@]}" > "$out/harness.log" 2>&1 || { tail -5 "$out/harness.log"; die "harness failed, see $out/harness.log"; }
    [ -f "$out/final.png" ] || die "no final.png in $out"
}

# The results pane of Disk First Aid, read from the final frame's PBM: the
# verdict line is matched against reference glyph rows kept in scripts/snow/.
# Prints OK / NOT-HFS / PROBLEM / UNKNOWN.
dfa_verdict() {
    python3 - "$1" "$SCRIPT_DIR/snow" <<'EOF'
import sys, os
pbm, refdir = sys.argv[1], sys.argv[2]
def load(path):
    data = open(path, 'rb').read()
    assert data[:2] == b'P4'
    parts = data.split(maxsplit=3)
    w, h = int(parts[1]), int(parts[2])
    raw = parts[3]
    stride = (w + 7) // 8
    rows = []
    for y in range(h):
        line = raw[y * stride:(y + 1) * stride]
        rows.append(''.join('1' if (line[x // 8] >> (7 - x % 8)) & 1 else '0' for x in range(w)))
    return w, h, rows
w, h, rows = load(pbm)
# The pane: x 205..560, y 232..405. Search each reference bitmap in it.
def find(ref):
    rw, rh, rrows = load(ref)
    for y in range(232, 406 - rh):
        for x in range(205, 560 - rw):
            if all(rows[y + j][x:x + rw] == rrows[j] for j in range(rh)):
                return True
    return False
for name, verdict in (("dfa-ok.pbm", "OK"), ("dfa-not-hfs.pbm", "NOT-HFS"), ("dfa-problem.pbm", "PROBLEM")):
    ref = os.path.join(refdir, name)
    if os.path.exists(ref) and find(ref):
        print(verdict); sys.exit(0)
print("UNKNOWN")
EOF
}

# crop_pbm IN.pbm OUT.pbm X Y W H  -- cut a reference bitmap out of a frame.
crop_pbm() {
    python3 - "$@" <<'EOF'
import sys
src, dst, x0, y0, cw, ch = sys.argv[1], sys.argv[2], *map(int, sys.argv[3:7])
data = open(src, 'rb').read(); parts = data.split(maxsplit=3)
w, h, raw = int(parts[1]), int(parts[2]), parts[3]; stride = (w + 7) // 8
ostride = (cw + 7) // 8; out = bytearray(ostride * ch)
for j in range(ch):
    for i in range(cw):
        x, y = x0 + i, y0 + j
        if (raw[y * stride + x // 8] >> (7 - x % 8)) & 1:
            out[j * ostride + i // 8] |= 0x80 >> (i % 8)
open(dst, 'wb').write(b'P4\n%d %d\n' % (cw, ch) + bytes(out))
EOF
}

cmd_wrap() {
    local vol=$1 out=$2 size=${3:-} kib
    need "$vol" "volume image"
    if [ -z "$size" ]; then
        # The volume rounded up to a whole MiB; the fill grows it to the
        # partition (R-058), so classic HFS fills what Mac OS expects it to.
        kib=$(( ( $(stat -f %z "$vol") + 1048575 ) / 1048576 * 1024 ))
    else
        case $size in
            *[Mm]) kib=$(( ${size%[Mm]} * 1024 )) ;;
            *[Kk]) kib=${size%[Kk]} ;;
            *) die "wrap: SIZE needs a K or M suffix" ;;
        esac
    fi
    size="${kib}K"
    local disk="$(( kib + 1024 ))K"
    rm -f "$out"
    "$RB" new hd apm "$out" --size "$disk" --partition "$size:Apple_HFS:$(basename "${vol%.*}")" --fill "1=$vol" >/dev/null || die "new hd apm failed"
    "$RB" mac-scsi-bless "$out" --driver-from "$DRIVER_HDA" >/dev/null || die "mac-scsi-bless failed"
    log "wrapped $vol -> $out ($size partition, SCSI driver from $(basename "$DRIVER_HDA"))"
}

# dfa DISK.hda [-i ICON] [-k EXTRA_KEYS] -o OUT
cmd_dfa() {
    local disk=$1; shift
    local icon=0 extra="" out=""
    local OPTIND=1
    while getopts "i:k:o:" opt; do case $opt in i) icon=$OPTARG;; k) extra=$OPTARG;; o) out=$OPTARG;; *) die "usage";; esac; done
    [ -n "$out" ] || die "dfa: -o OUT required"
    need "$disk" "SCSI disk"
    mkdir -p "$out"
    make_boot "$out/boot.hda" 1 || die "boot disk"
    cp "$disk" "$out/disk.hda"
    local keys="${extra:+$extra;}2600000000:click@$(dfa_icon_x "$icon"),$DFA_ICON_Y;2800000000:click@$DFA_VERIFY"
    harness "$out/run" "$out/boot.hda" 4800000000 "$keys" --disk2 "$out/disk.hda"
    local v; v=$(dfa_verdict "$out/run/final.pbm")
    log "Disk First Aid on $(basename "$disk") icon $icon: $v  ($out/run/final.png)"
    [ "$v" = OK ]
}

# dfa-floppy IMG [-i ICON] -o OUT  -- the floppy goes in after the desktop is up
cmd_dfa_floppy() {
    local img=$1; shift
    local icon=1 out=""
    local OPTIND=1
    while getopts "i:o:" opt; do case $opt in i) icon=$OPTARG;; o) out=$OPTARG;; *) die "usage";; esac; done
    [ -n "$out" ] || die "dfa-floppy: -o OUT required"
    need "$img" "floppy image"
    mkdir -p "$out"
    make_boot "$out/boot.hda" 1 || die "boot disk"
    cp "$img" "$out/floppy.img"
    local keys="2800000000:click@$(dfa_icon_x "$icon"),145;3000000000:click@$DFA_VERIFY"
    harness "$out/run" "$out/boot.hda" 4800000000 "$keys" --floppy "$out/floppy.img@2300000000"
    local v; v=$(dfa_verdict "$out/run/final.pbm")
    log "Disk First Aid on floppy $(basename "$img") icon $icon: $v  ($out/run/final.png)"
    [ "$v" = OK ]
}

# finder-copy IMG -o OUT: the Finder opens the floppy, selects all, drags the
# selection onto the boot disk; then every root file of the floppy must read
# back byte-identical from the boot disk's root through rb-cli get.
cmd_finder_copy() {
    local img=$1; shift
    local out=""
    local OPTIND=1
    while getopts "o:" opt; do case $opt in o) out=$OPTARG;; *) die "usage";; esac; done
    [ -n "$out" ] || die "finder-copy: -o OUT required"
    need "$img" "floppy image"
    mkdir -p "$out"
    make_boot "$out/boot.hda" 0 || die "boot disk"
    cp "$img" "$out/floppy.img"
    # Floppy icon (600,100) -> Open, Select All, drag the first icon of the
    # window (38,95) onto the boot disk icon (600,55).
    local keys="2600000000:click@600,100;2800000000:cmd-o;3100000000:cmd-a;3300000000:drag@38,95,600,55"
    harness "$out/run" "$out/boot.hda" 5000000000 "$keys" --floppy "$out/floppy.img@2300000000"
    local names bad=0 n=0
    names=$("$RB" ls "$img" / 2>/dev/null | awk '$1=="FILE"{ $1=""; $2=""; $3=""; $4=""; sub(/^ +/, ""); print }')
    rm -rf "$out/from-floppy" "$out/from-boot"; mkdir -p "$out/from-floppy" "$out/from-boot"
    while IFS= read -r name; do
        [ -n "$name" ] || continue
        n=$((n + 1))
        "$RB" get "$img" "/$name" "$out/from-floppy/$name" >/dev/null 2>&1
        if ! "$RB" get "$out/boot.hda" "/$name" "$out/from-boot/$name" >/dev/null 2>&1; then
            log "  $name: not on the boot disk after the Finder copy"; bad=$((bad + 1)); continue
        fi
        cmp -s "$out/from-floppy/$name" "$out/from-boot/$name" || { log "  $name: differs"; bad=$((bad + 1)); }
    done <<< "$names"
    log "Finder copy of $(basename "$img"): $n files, $bad missing or different  ($out/run/final.png)"
    [ $n -gt 0 ] && [ $bad -eq 0 ]
}

# open DISK.hda NAME... -o OUT: Find (cmd-f) each name and Open it; the
# final frame shows the last one in TeachText, the snapshots the others.
cmd_open() {
    local disk=$1; shift
    local names=() out=""
    while [ $# -gt 0 ]; do
        case $1 in -o) out=$2; shift 2;; *) names+=("$1"); shift;; esac
    done
    [ -n "$out" ] || die "open: -o OUT required"
    [ ${#names[@]} -gt 0 ] || die "open: no file names"
    need "$disk" "SCSI disk"
    mkdir -p "$out"
    make_boot "$out/boot.hda" 0 || die "boot disk"
    cp "$disk" "$out/disk.hda"
    # A window of 1500 icons takes the Finder a while to draw: 3.4 G per file.
    local keys="" at=2600000000
    for n in "${names[@]}"; do
        keys+="${keys:+;}$at:cmd-f;$((at + 300000000)):type@$n;$((at + 700000000)):return;$((at + 2600000000)):cmd-o;$((at + 3100000000)):cmd-q"
        at=$((at + 3400000000))
    done
    # Stop just before the last Quit, so final.png still shows the last file.
    harness "$out/run" "$out/boot.hda" "$((at - 300000000))" "$keys" --disk2 "$out/disk.hda" --snap-every 200000000
    log "opened ${names[*]} from $(basename "$disk"); TeachText frames: the snapshots ~2.7 G after each Find, and $out/run/final.png"
}

# Leaf record count of a classic HFS extents-overflow tree at byte OFFSET of IMG.
xt_leaf_records_at() {
    python3 - "$1" "$2" <<'EOF'
import struct, sys
f = open(sys.argv[1], 'rb'); base = int(sys.argv[2]); f.seek(base + 1024); hdr = f.read(512)
albs = struct.unpack('>I', hdr[0x14:0x18])[0]; alst = struct.unpack('>H', hdr[0x1c:0x1e])[0]
start = struct.unpack('>H', hdr[0x86:0x88])[0]
f.seek(base + alst * 512 + start * albs); node = f.read(512)
print(struct.unpack('>I', node[20:24])[0])
EOF
}
# fsck_hfs -n on the BYTES-long volume at LBA 2048 of DISK.hda; prints its last line.
fsck_hfs_wrapped() {
    local disk=$1 bytes=$2 tmp dev line
    tmp="${disk%.hda}-vol.img"
    dd if="$disk" of="$tmp" bs=512 skip=2048 count=$(( bytes / 512 )) 2>/dev/null
    dev=$(hdiutil attach -nomount -imagekey diskimage-class=CRawDiskImage "$tmp" | awk 'NR==1{print $1}')
    line=$(fsck_hfs -n "${dev/disk/rdisk}" 2>&1 | tail -1)
    hdiutil detach "$dev" >/dev/null 2>&1
    rm -f "$tmp"
    echo "$line"
}

# h3-real -w DIR: Mac OS itself fragments a fork. The h3-hfs.img that
# verify-fs-macos.sh -o H3-hfs leaves (every other 64 KiB file deleted) goes
# in over SCSI, the Finder copies a 1 MB file onto it, so System 7's File
# Manager spreads that fork over the holes and writes the overflow records;
# then rb-cli rm removes the file and both judges look before and after.
cmd_h3_real() {
    local work="" out
    local OPTIND=1
    while getopts "w:" opt; do case $opt in w) work=$OPTARG;; *) die "usage";; esac; done
    [ -n "$work" ] || die "h3-real: -w DIR (the verify-fs-macos.sh work dir) required"
    local img="$work/h3-hfs.img"; need "$img" "h3-hfs.img (run verify-fs-macos.sh -o H3-hfs first)"
    out="$work/snow/h3-real"; rm -rf "$out"; mkdir -p "$out"
    cmd_wrap "$img" "$out/disk.hda"
    make_boot "$out/boot.hda" 0 || die "boot disk"
    head -c 1000000 /dev/urandom > "$out/big.bin"
    { "$RB" mkdir "$out/boot.hda" /Frag && "$RB" put "$out/boot.hda" "$out/big.bin" /Frag/big.bin; } >/dev/null 2>&1 || die "put big.bin on the boot disk"
    # Find the Frag folder and open it (its one icon lands at 38,95), then drag
    # that icon onto the second disk's desktop icon (600,105); the copy takes ~2 G.
    local keys="2600000000:cmd-f;2900000000:type@frag;3300000000:return;3800000000:cmd-o;4300000000:drag@38,95,600,105"
    harness "$out/copy" "$out/boot.hda" 7500000000 "$keys" --disk2 "$out/disk.hda"
    "$RB" ls "$out/disk.hda@1" 2>/dev/null | grep -q "big.bin" || die "the Finder did not copy big.bin (see $out/copy/final.png)"
    local ok=1 before after f1 f2
    before=$(xt_leaf_records_at "$out/disk.hda" 1048576)
    [ "$before" -gt 0 ] || { log "  Mac OS wrote no overflow records; the copy did not fragment"; ok=0; }
    f1=$(fsck_hfs_wrapped "$out/disk.hda" "$(stat -f %z "$img")")
    cp "$out/disk.hda" "$out/disk-before.hda"
    "$RB" rm "$out/disk.hda@1" big.bin >/dev/null 2>&1 || { log "  rb-cli rm failed"; ok=0; }
    after=$(xt_leaf_records_at "$out/disk.hda" 1048576)
    [ "$after" -eq 0 ] || ok=0
    f2=$(fsck_hfs_wrapped "$out/disk.hda" "$(stat -f %z "$img")")
    log "  overflow records written by Mac OS: $before, after rb-cli rm: $after"
    log "  fsck_hfs before: $f1"; log "  fsck_hfs after:  $f2"
    echo "$f1$f2" | grep -q "OK.*OK" || ok=0
    cmd_dfa "$out/disk-before.hda" -i 0 -o "$out/dfa-before" || ok=0
    cmd_dfa "$out/disk.hda" -i 0 -o "$out/dfa-after" || ok=0
    [ $ok = 1 ] && log "H3-real PASS" || log "H3-real FAIL"
    [ $ok = 1 ]
}

# mac-formatted -w DIR: a volume Mac OS itself formatted, edited by rb-cli.
# A blank 5 MiB Apple_HFS partition goes in over SCSI, the System 7.1 Finder
# offers to initialize it (Initialize, Erase, a name), and rb-cli then edits
# the result: put, mkdir, mv, rm, setrsrc, put-binhex. fsck_hfs -n and Disk
# First Aid judge it; the Finder opens the text file in TeachText and
# launches the Disk First Aid copy rb-cli wrote (its resource fork intact).
cmd_mac_formatted() {
    local work="" out
    local OPTIND=1
    while getopts "w:" opt; do case $opt in w) work=$OPTARG;; *) die "usage";; esac; done
    [ -n "$work" ] || die "mac-formatted: -w DIR required"
    out="$work/snow/mac-formatted"; rm -rf "$out"; mkdir -p "$out"
    { "$RB" new hd apm "$out/disk.hda" --size 6M --partition 5M:Apple_HFS:Blank && "$RB" mac-scsi-bless "$out/disk.hda" --driver-from "$DRIVER_HDA"; } >/dev/null 2>&1 || die "building the blank disk"
    make_boot "$out/boot.hda" 0 || die "boot disk"
    # "This is not a Macintosh disk" -> Initialize (410,176); "will erase all
    # information" -> Erase (same spot); "Please name this disk" -> a name, OK.
    local keys="2300000000:click@410,176;2700000000:click@410,176;3400000000:type@snow71;3800000000:return"
    harness "$out/format" "$out/boot.hda" 6000000000 "$keys" --disk2 "$out/disk.hda"
    "$RB" ls "$out/disk.hda@1" >/dev/null 2>&1 || die "Mac OS did not format the partition (see $out/format/final.png)"
    local ok=1 d="$out/disk.hda@1" f0 f1
    f0=$(fsck_hfs_wrapped "$out/disk.hda" 5242880); log "  fsck_hfs on the Mac-formatted volume: $f0"
    printf 'Hello from rb-cli on a volume System 7.1 formatted.\r' > "$out/hello.txt"
    head -c 100000 /dev/urandom > "$out/blob.bin"; head -c 20000 /dev/urandom > "$out/fake.rsrc"
    "$RB" get-binhex "$BOOT_HDA" "$DFA_PATH" "$out/dfa.hqx" >/dev/null 2>&1 || die "get-binhex Disk First Aid"
    { "$RB" put "$d" "$out/hello.txt" Hello.txt && "$RB" put "$d" "$out/blob.bin" blob.bin \
        && "$RB" put "$d" "$out/blob.bin" gone.bin && "$RB" mkdir "$d" /Folder \
        && "$RB" put "$d" "$out/blob.bin" /Folder/inner.bin && "$RB" mv "$d" /Hello.txt /Hi.txt \
        && "$RB" rm "$d" gone.bin && "$RB" setrsrc "$d" /blob.bin --from-file "$out/fake.rsrc" \
        && "$RB" put-binhex "$d" "$out/dfa.hqx" && "$RB" mv "$d" "/Disk First Aid" "/DFA from rb-cli"; } >>"$out/edits.log" 2>&1 \
        || { log "  an rb-cli edit failed, see $out/edits.log"; ok=0; }
    "$RB" fsck --checkonly "$d" >>"$out/edits.log" 2>&1 || { log "  rb-cli fsck not clean, see $out/edits.log"; ok=0; }
    f1=$(fsck_hfs_wrapped "$out/disk.hda" 5242880); log "  fsck_hfs after the rb-cli edits: $f1"
    echo "$f1" | grep -q "OK" || ok=0
    cmd_dfa "$out/disk.hda" -i 0 -o "$out/dfa" || ok=0
    cmd_open "$out/disk.hda" Hi.txt "DFA from rb-cli" -o "$out/open"
    [ $ok = 1 ] && log "mac-formatted PASS" || log "mac-formatted FAIL"
    [ $ok = 1 ]
}

# leg3 -w DIR: the classic-HFS and MFS volumes verify-fs-macos.sh -w DIR built.
cmd_leg3() {
    local work=""
    local OPTIND=1
    while getopts "w:" opt; do case $opt in w) work=$OPTARG;; *) die "usage";; esac; done
    [ -n "$work" ] || die "leg3: -w DIR (the verify-fs-macos.sh work dir) required"
    local snow="$work/snow"; mkdir -p "$snow"
    local results=()
    if [ -f "$work/h1-hfs.img" ]; then
        cmd_wrap "$work/h1-hfs.img" "$snow/h1.hda"
        cmd_dfa "$snow/h1.hda" -i 0 -o "$snow/h1" && results+=("H1-hfs PASS") || results+=("H1-hfs FAIL")
        cmd_open "$snow/h1.hda" f0550 f1500 -o "$snow/h1-open" && results+=("H1-hfs open: read $snow/h1-open/run/final.png")
    fi
    # H3: the spilled fork before and after rb-cli deleted it.
    for stage in before after; do
        img="$work/h3-hfs-before.img"; [ "$stage" = after ] && img="$work/h3-hfs.img"
        [ -f "$img" ] || continue
        cmd_wrap "$img" "$snow/h3-$stage.hda"
        cmd_dfa "$snow/h3-$stage.hda" -i 0 -o "$snow/h3-$stage" && results+=("H3-hfs $stage delete PASS") || results+=("H3-hfs $stage delete FAIL")
    done
    if [ -f "$work/h3-hfs.img" ]; then
        cmd_h3_real -w "$work" && results+=("H3-real PASS") || results+=("H3-real FAIL")
    fi
    if [ -f "$work/h7-hfs.img" ]; then
        cp "$work/h7-hfs.img" "$snow/h7.hda"
        "$RB" mac-scsi-bless "$snow/h7.hda" --driver-from "$DRIVER_HDA" >/dev/null || die "mac-scsi-bless h7"
        # Its empty second partition draws the initialize dialog: Cancel it first.
        cmd_dfa "$snow/h7.hda" -i 0 -k "2300000000:click@$INIT_CANCEL" -o "$snow/h7" && results+=("H7-hfs PASS") || results+=("H7-hfs FAIL")
    fi
    if [ -f "$work/h6-mfs.img" ]; then
        cmd_dfa_floppy "$work/h6-mfs.img" -i 1 -o "$snow/h6-dfa"
        results+=("H6 Disk First Aid: $(dfa_verdict "$snow/h6-dfa/run/final.pbm") (MFS: it declines; the Finder check is the judge)")
        cmd_finder_copy "$work/h6-mfs.img" -o "$snow/h6" && results+=("H6 Finder copy PASS") || results+=("H6 Finder copy FAIL")
    fi
    cmd_mac_formatted -w "$work" && results+=("mac-formatted PASS") || results+=("mac-formatted FAIL")
    log "---- summary ----"
    for r in "${results[@]}"; do log "$r"; done
}

case "${1:-}" in
    wrap) shift; cmd_wrap "$@" ;;
    dfa) shift; cmd_dfa "$@" ;;
    dfa-floppy) shift; cmd_dfa_floppy "$@" ;;
    finder-copy) shift; cmd_finder_copy "$@" ;;
    open) shift; cmd_open "$@" ;;
    leg3) shift; cmd_leg3 "$@" ;;
    h3-real) shift; cmd_h3_real "$@" ;;
    mac-formatted) shift; cmd_mac_formatted "$@" ;;
    crop) shift; crop_pbm "$@" ;;
    verdict) shift; dfa_verdict "$@" ;;
    *) sed -n '2,20p' "$0"; exit 2 ;;
esac
