#!/usr/bin/env bash
# XFS repair + creation oracle harness.
#
# Two halves. The **v4 repair** half builds a small Docker image with xfsprogs
# 4.9.0 (the last series that creates v4 XFS cleanly), uses it to mkfs a v4
# image, then cross-checks our verifier against the real `xfs_repair -n`. The
# **v5 creation** half checks images from our own creator
# (`src/fs/xfs/format.rs`) against a modern xfsprogs over SSH — mkfs.xfs 6.x
# refuses filesystems under 300 MB, so it can only ever be a reference layout,
# never the thing under test.
#
# All xfsprogs tools operate on a plain image file: no root, no mount, no
# kernel xfs module.
#
# Usage:
#   scripts/xfs-oracle.sh build           # build the rusty-xfs-oracle image
#   scripts/xfs-oracle.sh mkfs <img> [MB] # create a clean v4 image (default 128 MiB)
#   scripts/xfs-oracle.sh repair <img>    # run `xfs_repair -n` (the oracle)
#   scripts/xfs-oracle.sh db <img> <cmds> # run arbitrary xfs_db commands
#   scripts/xfs-oracle.sh check <img>     # run OUR verifier (cargo example)
#   scripts/xfs-oracle.sh our-repair <img> # run OUR conservative repair in place
#   scripts/xfs-oracle.sh verify <img>    # run both and report agreement
#   scripts/xfs-oracle.sh ours <img> [MB] # format with OUR v5 creator
#   scripts/xfs-oracle.sh remote-mkfs <img> [MB]  # v5 reference from modern mkfs.xfs
#   scripts/xfs-oracle.sh remote-check <img>      # `xfs_repair -n` on the remote box
#   scripts/xfs-oracle.sh sweep [MB...]   # format at each size and check every one
set -euo pipefail

IMAGE=rusty-xfs-oracle
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# A Linux box with a modern xfsprogs, for the v5 creation half.
XFS_ORACLE_SSH=${XFS_ORACLE_SSH:-m900}
# The conservative feature set our reader speaks, and what format.rs emits.
MKFS_V5_FEATURES="-m crc=1,finobt=0,rmapbt=0,reflink=0,bigtime=0,inobtcount=0 -i sparse=0,nrext64=0"

dock() { docker run --rm -v "$(cd "$(dirname "$1")" && pwd):/w" -w /w "$IMAGE" "${@:2}"; }

# Run a modern xfsprogs tool on the remote box, uploading first and pulling the
# image back for the ones that mutate it.
remote_tool() {  # remote_tool <name> <img> [args...]
  local name=$1 img=$2; shift 2
  local base; base=$(basename "$img") rc=0
  rsync -qz --sparse -e "ssh -o BatchMode=yes" "$img" "$XFS_ORACLE_SSH:/tmp/$base"
  ssh -o BatchMode=yes "$XFS_ORACLE_SSH" "$name $* /tmp/$base" || rc=$?
  [ "$name" = mkfs.xfs ] && rsync -qz --sparse -e "ssh -o BatchMode=yes" "$XFS_ORACLE_SSH:/tmp/$base" "$img"
  return $rc
}

our_mkfs() {  # our_mkfs <img> <MB>
  cargo run -q --manifest-path "$ROOT/Cargo.toml" --example xfs_mkfs -- \
    "$1" "$(( $2 * 1024 * 1024 ))" rbtest
}

cmd=${1:-help}
case "$cmd" in
  build)
    docker build -q -t "$IMAGE" - <<'EOF'
FROM ubuntu:18.04
RUN apt-get update -qq && apt-get install -y -qq xfsprogs && rm -rf /var/lib/apt/lists/*
EOF
    echo "built $IMAGE ($(docker run --rm "$IMAGE" mkfs.xfs -V))"
    ;;
  mkfs)
    img=$2; mb=${3:-128}; base=$(basename "$img")
    dock "$img" mkfs.xfs -m crc=0 -d "file=1,name=$base,size=${mb}m" -L RUSTYV4
    ;;
  repair)
    img=$2; base=$(basename "$img")
    dock "$img" xfs_repair -n "$base"
    ;;
  db)
    img=$2; base=$(basename "$img"); shift 2
    args=(); for c in "$@"; do args+=( -c "$c" ); done
    dock "$img" xfs_db -x "${args[@]}" "$base"
    ;;
  check)
    cargo run -q --manifest-path "$ROOT/Cargo.toml" --example xfs_check -- "$2"
    ;;
  our-repair)
    cargo run -q --manifest-path "$ROOT/Cargo.toml" --example xfs_check -- --repair "$2"
    ;;
  ours)
    our_mkfs "$2" "${3:-512}"
    ;;
  remote-mkfs)
    img=$2; mb=${3:-512}
    rm -f "$img"; truncate -s "${mb}m" "$img"
    # shellcheck disable=SC2086
    remote_tool mkfs.xfs "$img" -f -L rbtest $MKFS_V5_FEATURES
    ;;
  remote-check)
    remote_tool xfs_repair "$2" -n
    ;;
  sweep)
    shift
    sizes=("$@"); [ ${#sizes[@]} -eq 0 ] && sizes=(32 64 128 300 512 1024 4096)
    tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
    fail=0
    for mb in "${sizes[@]}"; do
      our_mkfs "$tmp/x.img" "$mb" >/dev/null
      if remote_tool xfs_repair "$tmp/x.img" -n >"$tmp/log" 2>&1; then
        echo "${mb}M: CLEAN"
      else
        echo "${mb}M: DIRTY"
        grep -v 'host filesystem geometry\|sector size mismatch\|Repair may fail\|the image and the host' "$tmp/log" | head -20
        fail=1
      fi
    done
    exit $fail
    ;;
  verify)
    img=$2; base=$(basename "$img")
    echo "=== oracle: xfs_repair -n ==="
    if dock "$img" xfs_repair -n "$base" >/tmp/xfs_oracle.log 2>&1; then
      oracle=clean; else oracle=dirty; fi
    tail -3 /tmp/xfs_oracle.log; echo "oracle: $oracle"
    echo "=== ours: xfs_check ==="
    if cargo run -q --manifest-path "$ROOT/Cargo.toml" --example xfs_check -- "$img"; then
      ours=clean; else ours=dirty; fi
    echo "ours: $ours"
    [ "$oracle" = "$ours" ] && echo "AGREE ($oracle)" || { echo "DISAGREE: oracle=$oracle ours=$ours"; exit 1; }
    ;;
  *)
    sed -n '2,28p' "$0"; exit 1;;
esac
