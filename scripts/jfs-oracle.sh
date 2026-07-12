#!/usr/bin/env bash
# JFS2 check/repair oracle harness.
#
# Cross-checks our JFS verifier + repair against the real jfsutils
# (mkfs.jfs / fsck.jfs). Like xfsprogs, jfsutils operate on a plain image
# file with no mount and no root — so the oracle needs only the binaries, no
# kernel jfs module.
#
# Three backends (pick with JFS_ORACLE_BACKEND, default auto):
#
#   local   — run mkfs.jfs / fsck.jfs straight off PATH. Use this when the
#             harness runs ON a Linux box that has jfsutils installed
#             (`sudo apt install jfsutils`). No container, no SSH — the
#             fastest loop for developing the JFS write path on the box.
#
#   docker  — build a tiny ubuntu+jfsutils image and run the tools in it.
#             Works on macOS/Linux with any Docker-compatible runtime
#             (Docker Desktop, colima, podman via DOCKER=podman).
#
#   ssh     — run the tools on a remote Linux box over SSH. No root needed:
#             the setup extracts the jfsutils .deb into $HOME (see the
#             `remote-setup` verb). Point at the host with JFS_ORACLE_SSH
#             (default: m900) and the extracted sbin with JFS_ORACLE_SBIN
#             (default: \$HOME/jfsoracle/root/sbin on the remote).
#
# Populating files needs a mounted fs (kernel jfs), which this harness does
# NOT do — the in-repo tests forge structural conditions (orphans, bad
# counters) directly in the image bytes, exactly like the fsck unit tests.
# The oracle's job is to confirm mkfs-clean and repair-clean images survive
# the real `fsck.jfs`.
#
# Usage:
#   scripts/jfs-oracle.sh remote-setup     # (ssh backend) install jfsutils into remote $HOME, no sudo
#   scripts/jfs-oracle.sh build            # (docker backend) build the rusty-jfs-oracle image
#   scripts/jfs-oracle.sh mkfs <img> [MB]  # create a clean JFS2 image (default 16 MiB; 16 is the min)
#   scripts/jfs-oracle.sh fsck <img>       # run the real `fsck.jfs -f -n` (the oracle; read-only)
#   scripts/jfs-oracle.sh check <img>      # run OUR verifier (cargo example)
#   scripts/jfs-oracle.sh verify <img>     # run both and report agreement
set -euo pipefail

IMAGE=rusty-jfs-oracle
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOCKER=${DOCKER:-docker}
JFS_ORACLE_SSH=${JFS_ORACLE_SSH:-m900}
JFS_ORACLE_SBIN=${JFS_ORACLE_SBIN:-'$HOME/jfsoracle/root/sbin'}

# Backend selection: explicit override, else local jfsutils if present, else
# docker if usable, else ssh.
pick_backend() {
  if [ -n "${JFS_ORACLE_BACKEND:-}" ]; then echo "$JFS_ORACLE_BACKEND"; return; fi
  if command -v fsck.jfs >/dev/null 2>&1; then echo local; return; fi
  if "$DOCKER" info >/dev/null 2>&1; then echo docker; else echo ssh; fi
}
BACKEND=$(pick_backend)

dock() { "$DOCKER" run --rm -v "$(cd "$(dirname "$1")" && pwd):/w" -w /w "$IMAGE" "${@:2}"; }

# Run mkfs.jfs/fsck.jfs on `img` and print output. Uploads to the remote,
# runs, and (for mkfs) downloads the formatted image back.
remote_tool() {  # remote_tool <tool> <img> [extra-args...]
  local tool=$1 img=$2; shift 2
  local base; base=$(basename "$img")
  scp -q -o BatchMode=yes "$img" "$JFS_ORACLE_SSH:/tmp/$base"
  ssh -o BatchMode=yes "$JFS_ORACLE_SSH" "$JFS_ORACLE_SBIN/$tool $* /tmp/$base"
  local rc=$?
  # mkfs / repair mutate the image — pull it back.
  case "$tool" in
    mkfs.jfs|fsck.jfs) scp -q -o BatchMode=yes "$JFS_ORACLE_SSH:/tmp/$base" "$img" 2>/dev/null || true ;;
  esac
  return $rc
}

cmd=${1:-help}
case "$cmd" in
  remote-setup)
    # Install jfsutils into the remote $HOME with no sudo: download the .deb
    # from the apt mirror and extract it with dpkg-deb -x.
    ssh -o BatchMode=yes "$JFS_ORACLE_SSH" 'bash -s' <<'REOF'
set -e
cd "$HOME"; mkdir -p jfsoracle && cd jfsoracle
url=$(apt-get download --print-uris jfsutils 2>/dev/null | head -1 | awk '{print $1}' | tr -d "'")
echo "downloading $url"
wget -q -O jfsutils.deb "$url"
dpkg-deb -x jfsutils.deb root
./root/sbin/fsck.jfs -V 2>&1 | head -1
./root/sbin/mkfs.jfs -V 2>&1 | head -1
echo "jfsutils ready under $HOME/jfsoracle/root/sbin"
REOF
    ;;
  build)
    "$DOCKER" build -q -t "$IMAGE" - <<'EOF'
FROM ubuntu:22.04
RUN apt-get update -qq && apt-get install -y -qq jfsutils && rm -rf /var/lib/apt/lists/*
EOF
    echo "built $IMAGE ($("$DOCKER" run --rm "$IMAGE" fsck.jfs -V 2>&1 | head -1))"
    ;;
  mkfs)
    img=$2; mb=${3:-16}; base=$(basename "$img")
    dd if=/dev/zero of="$img" bs=1m count="$mb" status=none 2>/dev/null \
      || dd if=/dev/zero of="$img" bs=1M count="$mb" 2>/dev/null
    case "$BACKEND" in
      local)  mkfs.jfs -q -L jfs_test "$img" ;;
      docker) dock "$img" mkfs.jfs -q -L jfs_test "$base" ;;
      *)      remote_tool mkfs.jfs "$img" -q -L jfs_test ;;
    esac
    ;;
  fsck)
    img=$2; base=$(basename "$img")
    # `-n` read-only check; `-f` force full check even if SB says clean.
    case "$BACKEND" in
      local)  fsck.jfs -f -n "$img" ;;
      docker) dock "$img" fsck.jfs -f -n "$base" ;;
      *)      remote_tool fsck.jfs "$img" -f -n ;;
    esac
    ;;
  check)
    cargo run -q --manifest-path "$ROOT/Cargo.toml" --example jfs_check -- "$2"
    ;;
  verify)
    img=$2
    echo "=== oracle: fsck.jfs -f -n (backend: $BACKEND) ==="
    if "$0" fsck "$img" >/tmp/jfs_oracle.log 2>&1; then oracle=clean; else oracle=dirty; fi
    tail -5 /tmp/jfs_oracle.log; echo "oracle: $oracle"
    echo "=== ours: jfs_check ==="
    if cargo run -q --manifest-path "$ROOT/Cargo.toml" --example jfs_check -- "$img"; then
      ours=clean; else ours=dirty; fi
    echo "ours: $ours"
    [ "$oracle" = "$ours" ] && echo "AGREE ($oracle)" || { echo "DISAGREE: oracle=$oracle ours=$ours"; exit 1; }
    ;;
  *)
    sed -n '2,35p' "$0"; exit 1;;
esac
