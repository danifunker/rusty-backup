#!/usr/bin/env bash
# XFS v4 repair oracle harness.
#
# Builds a small Docker image with xfsprogs 4.9.0 (the last series that
# creates v4 XFS cleanly), uses it to mkfs a v4 image, then cross-checks our
# verifier against the real `xfs_repair -n`. Works on macOS/Linux with Docker;
# all xfsprogs tools operate on a plain image file (no root, no mount).
#
# Usage:
#   scripts/xfs-oracle.sh build           # build the rusty-xfs-oracle image
#   scripts/xfs-oracle.sh mkfs <img> [MB] # create a clean v4 image (default 128 MiB)
#   scripts/xfs-oracle.sh repair <img>    # run `xfs_repair -n` (the oracle)
#   scripts/xfs-oracle.sh db <img> <cmds> # run arbitrary xfs_db commands
#   scripts/xfs-oracle.sh check <img>     # run OUR verifier (cargo example)
#   scripts/xfs-oracle.sh verify <img>    # run both and report agreement
set -euo pipefail

IMAGE=rusty-xfs-oracle
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

dock() { docker run --rm -v "$(cd "$(dirname "$1")" && pwd):/w" -w /w "$IMAGE" "${@:2}"; }

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
    sed -n '2,20p' "$0"; exit 1;;
esac
