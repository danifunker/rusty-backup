#!/usr/bin/env bash
#
# ppc-smoke.sh -- does a vintage build agree with the desktop build?
#
# The bar for a vintage port is not "it runs" but "it produces the same
# answers": identical `inspect` / `ls` / `fsck` output on the same images, and a
# backup whose payload and checksum are byte-identical, with `metadata.json`
# differing only in its `created` timestamp. That comparison had been run by
# hand every time, which is why it kept being run *narrowly* - two filesystems
# and one format, then a conclusion about the whole engine.
#
# Usage:
#   PPC_HOST=admin@g5.local scripts/ppc-smoke.sh [remote-rb-cli]
#   SOL9_HOST=user@192.168.99.176 RB_SMOKE_SSH_AUTH_SOCK=/run/user/$(id -u)/gcr/ssh \
#     scripts/ppc-smoke.sh ./rb-cli
#
# `remote-rb-cli` defaults to /Users/admin/rb-cli-dev. The local binary is
# target/release/rb-cli; build it first. Nothing here needs privileges: every
# subject is a scratch image in a temp directory.
#
# Exit status is the number of mismatches, so this can gate a build.

set -uo pipefail

# The target host is named by whichever variable fits the machine: PPC_HOST for
# the PowerPC Macs, SOL9_HOST for the Sun Blade, RB_SMOKE_HOST for anything
# else. Nothing below this line is target-specific -- every assertion is on
# bytes the two hosts produce, and the remote side is plain POSIX sh.
SMOKE_HOST="${RB_SMOKE_HOST:-${PPC_HOST:-${SOL9_HOST:-}}}"
REMOTE_BIN="${1:-${RB_SMOKE_BIN:-/Users/admin/rb-cli-dev}}"
LOCAL_BIN="${LOCAL_BIN:-target/release/rb-cli}"
REMOTE_DIR="/tmp/rb-smoke.$$"

# SunSSH on the Blade wants a SHA-1 RSA signature the inherited gnome-keyring
# agent refuses to make, and fails as "Permission denied (publickey)" -- which
# reads like a missing key and is not. Point at the gcr agent instead:
#   RB_SMOKE_SSH_AUTH_SOCK=/run/user/$(id -u)/gcr/ssh
[ -n "${RB_SMOKE_SSH_AUTH_SOCK:-}" ] && export SSH_AUTH_SOCK="$RB_SMOKE_SSH_AUTH_SOCK"

[ -n "$SMOKE_HOST" ] || { echo "no target host: set PPC_HOST, SOL9_HOST or RB_SMOKE_HOST" >&2; exit 2; }
[ -x "$LOCAL_BIN" ] || { echo "$LOCAL_BIN missing - cargo build --release --bin rb-cli" >&2; exit 2; }

LOCAL_BIN="$(cd "$(dirname "$LOCAL_BIN")" && pwd)/$(basename "$LOCAL_BIN")"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"; ssh "$SMOKE_HOST" "rm -rf $REMOTE_DIR" 2>/dev/null' EXIT

fails=0
pass() { printf '  \033[32mOK\033[0m    %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; fails=$((fails + 1)); }

echo "== building subjects =="
# One per filesystem family the engine treats differently: FAT (cluster chains),
# HFS (classic Mac B-trees), ext (Unix inodes). Small: the point is agreement,
# not throughput.
"$LOCAL_BIN" new floppy fat "$WORK/fat.img" >/dev/null 2>&1 || { echo "new fat failed" >&2; exit 2; }
"$LOCAL_BIN" new floppy hfs "$WORK/hfs.img" >/dev/null 2>&1 || { echo "new hfs failed" >&2; exit 2; }
"$LOCAL_BIN" new volume ext3 "$WORK/ext.img" --size 32M >/dev/null 2>&1 || { echo "new ext failed" >&2; exit 2; }
printf 'rusty-backup smoke subject\n' > "$WORK/payload.txt"
"$LOCAL_BIN" put "$WORK/fat.img" "$WORK/payload.txt" /HELLO.TXT >/dev/null 2>&1
"$LOCAL_BIN" put "$WORK/hfs.img" "$WORK/payload.txt" /hello.txt >/dev/null 2>&1
"$LOCAL_BIN" put "$WORK/ext.img" "$WORK/payload.txt" /hello.txt >/dev/null 2>&1
echo "  fat.img hfs.img ext.img"

echo "== shipping to $SMOKE_HOST =="
ssh "$SMOKE_HOST" "mkdir -p $REMOTE_DIR" || exit 2
scp -q "$WORK"/*.img "$SMOKE_HOST:$REMOTE_DIR/" || exit 2

echo "== read-only verbs must agree =="
for img in fat hfs ext; do
  for verb in "inspect $img.img" "ls $img.img /" "fsck $img.img"; do
    # Capture the two streams separately and label them. Merging with 2>&1
    # compares nothing useful over ssh: the remote merge interleaves stdout and
    # stderr on one channel with different buffering than a local pipe, so
    # byte-identical output reports as a diff (a lost newline between the
    # stderr banner and the stdout body). Cost a real debugging session once.
    local_out="$(cd "$WORK" && { "$LOCAL_BIN" $verb >"$WORK/.o" 2>"$WORK/.e"; \
        printf '<<<out>>>\n'; cat "$WORK/.o"; printf '<<<err>>>\n'; cat "$WORK/.e"; })"
    remote_out="$(ssh "$SMOKE_HOST" "cd $REMOTE_DIR && { $REMOTE_BIN $verb >.o 2>.e; \
        printf '<<<out>>>\\n'; cat .o; printf '<<<err>>>\\n'; cat .e; }")"
    if [ "$local_out" = "$remote_out" ]; then
      pass "rb-cli $verb"
    else
      fail "rb-cli $verb"
      diff <(printf '%s\n' "$local_out") <(printf '%s\n' "$remote_out") | head -6 | sed 's/^/        /'
    fi
  done
done

echo "== backup must be byte-identical (payload + checksum) =="
for img in fat hfs ext; do
  (cd "$WORK" && "$LOCAL_BIN" backup "$img.img" "local-$img" --format zstd --checksum sha256 >/dev/null 2>&1)
  ssh "$SMOKE_HOST" "cd $REMOTE_DIR && $REMOTE_BIN backup $img.img remote-$img --format zstd --checksum sha256" >/dev/null 2>&1

  local_dir="$(find "$WORK/local-$img" -mindepth 1 -maxdepth 1 -type d | head -1)"
  remote_name="$(ssh "$SMOKE_HOST" "ls $REMOTE_DIR/remote-$img | head -1")"
  [ -n "$local_dir" ] && [ -n "$remote_name" ] || { fail "backup $img produced nothing"; continue; }

  mismatch=0
  for f in $(cd "$local_dir" && ls); do
    case "$f" in
      metadata.json) continue ;;   # `created` differs by construction
    esac
    scp -q "$SMOKE_HOST:$REMOTE_DIR/remote-$img/$remote_name/$f" "$WORK/fetched" 2>/dev/null
    if cmp -s "$local_dir/$f" "$WORK/fetched"; then
      :
    else
      fail "backup $img: $f differs ($(stat -c%s "$local_dir/$f" 2>/dev/null) vs $(stat -c%s "$WORK/fetched" 2>/dev/null) bytes)"
      mismatch=1
    fi
  done

  # metadata.json must differ *only* in `created`.
  ssh "$SMOKE_HOST" "cat $REMOTE_DIR/remote-$img/$remote_name/metadata.json" > "$WORK/remote-meta-$img.json" 2>/dev/null
  if diff <(grep -v '"created"' "$local_dir/metadata.json") \
          <(grep -v '"created"' "$WORK/remote-meta-$img.json") >/dev/null 2>&1; then
    [ $mismatch -eq 0 ] && pass "backup $img (payload, checksum, metadata)"
  else
    fail "backup $img: metadata.json differs beyond \`created\`"
    diff <(grep -v '"created"' "$local_dir/metadata.json") \
         <(grep -v '"created"' "$WORK/remote-meta-$img.json") | head -6 | sed 's/^/        /'
  fi
done

echo
if [ $fails -eq 0 ]; then
  printf '\033[32m%s agrees with the desktop build on every subject.\033[0m\n' "$SMOKE_HOST"
else
  printf '\033[31m%d mismatch(es).\033[0m\n' "$fails"
fi
exit $fails
