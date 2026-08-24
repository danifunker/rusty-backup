#!/usr/bin/env bash
# SGI EFS v1 emulation oracle: boot a real IRIX 3.7 on a disk we wrote.
#
# The Motion emulator (https://github.com/.../motion, checked out at
# $MOTION_ROOT) runs an IRIS 3130 well enough to boot IRIX 3.7 / GL2-W3.6 to a
# shell, with the serial console on stdout. It opens the disk **read-only**, so
# it is a pure read oracle — which is exactly what our write side needs: the
# period kernel is the only thing that can say whether what we wrote is what
# IRIX would have written.
#
# `prove` is the interesting one. It takes the reference IRIS 3130 disk, has
# rb-cli
#   * rewrite and GROW /etc/rc.s0 (1282 -> ~1600 bytes),
#   * create a new file, a new directory and a file inside it,
#   * allocate a fresh 256 KiB file,
# then boots it and checks the console for what the added rc.s0 lines print.
# Passing means IRIX itself executed our rewritten script, read our new file and
# directory, `sum`-ed our allocated file to the right checksum, and mounted the
# second EFS v1 filesystem off md0c.
#
# Usage:
#   scripts/sgi-efs-v1-oracle.sh baseline        # boot the stock disk, print the console
#   scripts/sgi-efs-v1-oracle.sh boot <img>      # boot any image, print the console
#   scripts/sgi-efs-v1-oracle.sh prove           # write with rb-cli, boot, assert
#
# Environment:
#   MOTION_ROOT  Motion checkout (default ~/repos/motion)
#   IRIS_DISK    Reference IRIS 3130 image (default ~/3130.img)
#   BOOT_SECONDS How long to let the guest run (default 120)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MOTION_ROOT=${MOTION_ROOT:-$HOME/repos/motion}
MOTION_BIN=${MOTION_BIN:-$MOTION_ROOT/build/output/RelWithDebInfo}
IRIS_DISK=${IRIS_DISK:-$HOME/3130.img}
BOOT_SECONDS=${BOOT_SECONDS:-120}
RB=${RB:-$ROOT/target/debug/rb-cli}

need() {
  [ -x "$MOTION_BIN/motion" ] || {
    echo "skip: no Motion build at $MOTION_BIN/motion (set MOTION_ROOT)" >&2
    exit 77
  }
  [ -f "$IRIS_DISK" ] || {
    echo "skip: no IRIS 3130 image at $IRIS_DISK (set IRIS_DISK)" >&2
    exit 77
  }
  [ -x "$RB" ] || { echo "no rb-cli at $RB; run cargo build --bin rb-cli" >&2; exit 1; }
}

# Boot `img` in a scratch run directory and print just the guest's serial
# console. Motion needs assets/roms/profile beside its binary, so the run
# directory is symlinks plus a private copy of the disk.
boot() {  # boot <img> <logfile>
  local img=$1 log=$2
  local run; run=$(mktemp -d)
  trap 'rm -rf "$run"' RETURN
  ln -s "$MOTION_BIN/assets" "$run/assets"
  ln -s "$MOTION_BIN/roms" "$run/roms"
  ln -s "$MOTION_BIN/motion" "$run/motion"
  mkdir -p "$run/profile"
  cp "$img" "$run/profile/3130.img"
  [ -f "$MOTION_BIN/profile/ip2_sram.bin" ] && cp "$MOTION_BIN/profile/ip2_sram.bin" "$run/profile/"
  ( cd "$run" && timeout "$BOOT_SECONDS" ./motion +set skipLauncher 1 +set startPaused 0 ) \
    </dev/null >"$run/raw.log" 2>&1 || true
  # The guest's console lines are tagged; everything else is host tracing.
  sed -n 's/.*\[Emulation - Serial\].*\[line 1\] //p' "$run/raw.log" >"$log"
}

case "${1:-help}" in
  baseline)
    need
    log=$(mktemp); boot "$IRIS_DISK" "$log"; cat "$log"; rm -f "$log"
    ;;

  boot)
    need
    [ -n "${2:-}" ] || { echo "usage: $0 boot <img>" >&2; exit 1; }
    log=$(mktemp); boot "$2" "$log"; cat "$log"; rm -f "$log"
    ;;

  prove)
    need
    work=$(mktemp -d); trap 'rm -rf "$work"' EXIT
    img=$work/proof.img
    cp "$IRIS_DISK" "$img"

    "$RB" -q get "$img@1" /etc/rc.s0 "$work/rcs0" >/dev/null
    cat >>"$work/rcs0" <<'SH'

# Added by scripts/sgi-efs-v1-oracle.sh: mount /usr, then read back what
# rb-cli wrote into this filesystem.
/etc/mount /dev/md0c /usr
echo "RBPROOF-BEGIN"
echo "RBPROOF-NEWFILE:"
/bin/cat /etc/rbnewfile
echo "RBPROOF-NEWDIR:"
/bin/cat /rbdir/inner
echo "RBPROOF-BIG:"
/bin/sum /rbbig.dat
echo "RBPROOF-USR:"
/bin/ls /usr/bin | /bin/head -3
echo "RBPROOF-END"
SH
    echo 'rusty-backup wrote this file into a real IRIS 3130 root filesystem.' >"$work/newfile"
    echo 'and this one lives in a directory rusty-backup created.' >"$work/inner"
    # 256 KiB of 'A': 262144 * 65 folds to a System V `sum` of exactly 260.
    head -c 262144 /dev/zero | tr '\0' 'A' >"$work/big"

    "$RB" -q put "$img@1" "$work/rcs0" /etc/rc.s0 --force >/dev/null
    "$RB" -q chmod "$img@1" /etc/rc.s0 755 >/dev/null
    "$RB" -q put "$img@1" "$work/newfile" /etc/rbnewfile >/dev/null
    "$RB" -q mkdir "$img@1" /rbdir >/dev/null
    "$RB" -q put "$img@1" "$work/inner" /rbdir/inner >/dev/null
    "$RB" -q put "$img@1" "$work/big" /rbbig.dat >/dev/null
    "$RB" -q fsck "$img@1" >/dev/null
    "$RB" -q fsck "$img@3" >/dev/null

    log=$work/console.log
    boot "$img" "$log"

    fail=0
    check() {  # check <label> <pattern>
      if grep -qF "$2" "$log"; then
        echo "  ok    $1"
      else
        echo "  FAIL  $1 (no '$2' on the console)"
        fail=1
      fi
    }
    echo "IRIX 3.7 read back, from its own console:"
    check "rewritten+grown /etc/rc.s0 ran" "RBPROOF-BEGIN"
    check "new file /etc/rbnewfile"        "rusty-backup wrote this file"
    check "new directory /rbdir"           "lives in a directory rusty-backup created"
    check "256 KiB /rbbig.dat checksums"   "260 256 /rbbig.dat"
    check "/usr mounted off md0c"          "RBPROOF-USR"
    check "script ran to completion"       "RBPROOF-END"
    if [ "$fail" = 0 ]; then
      echo "PASS"
    else
      echo "FAIL -- full console:"; cat "$log"; exit 1
    fi
    ;;

  *)
    sed -n '2,32p' "$0" | sed 's/^# \{0,1\}//'
    ;;
esac
