#!/usr/bin/env bash
#
# ppc-newcode-smoke.sh -- does a vintage build agree with the desktop build on
# the *write* verbs added since the last PPC binary?
#
# ppc-smoke.sh covers the read side and whole-image backup. This covers the
# verbs that mutate a file's bytes or its metadata in place - `edit`, `chmeta`,
# `put --force` - none of which had ever run on a big-endian host. That gap is
# not theoretical: a mrustc mis-lowering of `leading_zeros` on u8/u16 made every
# PowerPC ext/UFS/JFS/AFFS backup ~7x too large, and it was invisible until a
# real backup was compared byte-for-byte.
#
# The assertions are deliberately on bytes, never on log lines. A log line
# reading `Preserving type/creator TEXT/MSWD` is exactly how an earlier bug hid.
# Each test runs the identical command on both hosts and compares three things:
# the exit status, the extracted file's bytes, and the set of image offsets the
# command changed (via scripts/imgdiff.py). A byte-order bug shows up as the
# same offset holding different bytes.
#
# Usage:
#   PPC_HOST=admin@g5.local scripts/ppc-newcode-smoke.sh [remote-rb-cli]
#   SOL9_HOST=user@192.168.99.176 RB_SMOKE_SSH_AUTH_SOCK=/run/user/$(id -u)/gcr/ssh \
#     scripts/ppc-newcode-smoke.sh ./rb-cli
#
# Exit status is the number of mismatches, so this can gate a build.

set -uo pipefail

# Target host: PPC_HOST, SOL9_HOST or RB_SMOKE_HOST. Nothing below is target-specific.
SMOKE_HOST="${RB_SMOKE_HOST:-${PPC_HOST:-${SOL9_HOST:-}}}"
REMOTE_BIN="${1:-${RB_SMOKE_BIN:-/Users/admin/rb-cli-dev}}"
LOCAL_BIN="${LOCAL_BIN:-target/release/rb-cli}"
REMOTE_DIR="/tmp/rb-newcode.$$"

# SunSSH needs a SHA-1 RSA signature; RB_SMOKE_SSH_AUTH_SOCK picks an agent that will make one.
[ -n "${RB_SMOKE_SSH_AUTH_SOCK:-}" ] && export SSH_AUTH_SOCK="$RB_SMOKE_SSH_AUTH_SOCK"

[ -n "$SMOKE_HOST" ] || { echo "no target host: set PPC_HOST, SOL9_HOST or RB_SMOKE_HOST" >&2; exit 2; }
[ -x "$LOCAL_BIN" ] || { echo "$LOCAL_BIN missing - cargo build --release --bin rb-cli" >&2; exit 2; }

LOCAL_BIN="$(cd "$(dirname "$LOCAL_BIN")" && pwd)/$(basename "$LOCAL_BIN")"
IMGDIFF="$(cd "$(dirname "$0")" && pwd)/imgdiff.py"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"; ssh "$SMOKE_HOST" "rm -rf $REMOTE_DIR" 2>/dev/null' EXIT

fails=0
pass() { printf '  \033[32mOK\033[0m    %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; fails=$((fails + 1)); }
info() { printf '        %s\n' "$1"; }

# ---------------------------------------------------------------- fixtures --
# perl, not printf: /bin/sh's printf has no \xNN escape and would write the literal text.
echo "== building subjects =="
perl -e 'print "REM \xB3 box drawing\r\nSET X=1\r\n\xB3\xB3\xB3\r\n"' > "$WORK/dostext.txt"
perl -e 'print "alpha\nbeta\r\ngamma\ndelta\r\nomega\n"'              > "$WORK/mixed.txt"
printf 'attribute subject\n'  > "$WORK/attr.txt"
printf 'protection subject\n' > "$WORK/prot.txt"
printf 'metadata subject\n'   > "$WORK/meta.txt"
printf 'replacement bytes\n'  > "$WORK/replace.txt"
# Pin fixture permissions: the two hosts have different umasks, which reads like an engine bug.
chmod 644 "$WORK"/*.txt

# Editors as shell scripts taking the temp file as $1; octal escapes only, \xNN is not POSIX.
cat > "$WORK/ed-noop.sh" <<'EOF'
#!/bin/sh
exit 0
EOF
cat > "$WORK/ed-emdash.sh" <<'EOF'
#!/bin/sh
printf 'tail \342\200\224 dash\n' >> "$1"
EOF
cat > "$WORK/ed-ascii.sh" <<'EOF'
#!/bin/sh
printf 'APPENDED LINE\n' >> "$1"
EOF
chmod +x "$WORK"/ed-*.sh

"$LOCAL_BIN" new floppy fat  "$WORK/fat.img"                >/dev/null 2>&1 || { echo "new fat failed"  >&2; exit 2; }
"$LOCAL_BIN" new floppy hfs  "$WORK/hfs.img"                >/dev/null 2>&1 || { echo "new hfs failed"  >&2; exit 2; }
# AFFS is a `new volume` filesystem, not a `new floppy` one.
"$LOCAL_BIN" new volume affs "$WORK/affs.img" --size 880K   >/dev/null 2>&1 || { echo "new affs failed" >&2; exit 2; }
"$LOCAL_BIN" new volume ext3 "$WORK/ext.img"  --size 32M    >/dev/null 2>&1 || { echo "new ext failed"  >&2; exit 2; }

"$LOCAL_BIN" put "$WORK/fat.img"  "$WORK/dostext.txt" /DOSTEXT.TXT >/dev/null 2>&1
"$LOCAL_BIN" put "$WORK/fat.img"  "$WORK/mixed.txt"   /MIXED.TXT   >/dev/null 2>&1
"$LOCAL_BIN" put "$WORK/fat.img"  "$WORK/attr.txt"    /ATTR.TXT    >/dev/null 2>&1
"$LOCAL_BIN" put "$WORK/affs.img" "$WORK/prot.txt"    /prot.txt    >/dev/null 2>&1
"$LOCAL_BIN" put "$WORK/hfs.img"  "$WORK/meta.txt"    /meta.txt --type TEXT --creator MSWD >/dev/null 2>&1
"$LOCAL_BIN" put "$WORK/ext.img"  "$WORK/meta.txt"    /meta.txt --mode 750 --uid 1234 --gid 5678 >/dev/null 2>&1
echo "  fat.img affs.img hfs.img ext.img"

# Pristine copies both sides; every test resets from these.
for i in fat affs hfs ext; do cp "$WORK/$i.img" "$WORK/$i.pristine"; done

echo "== shipping to $SMOKE_HOST =="
ssh "$SMOKE_HOST" "mkdir -p $REMOTE_DIR" || exit 2
scp -q "$WORK"/*.img "$WORK"/*.txt "$WORK"/ed-*.sh "$SMOKE_HOST:$REMOTE_DIR/" || exit 2
ssh "$SMOKE_HOST" "cd $REMOTE_DIR && chmod +x ed-*.sh && chmod 644 *.txt && for i in fat affs hfs ext; do cp \$i.img \$i.pristine; done" || exit 2

# run_both NAME IMG CMD...: same command both hosts, compares status, changed offsets and bytes.
run_both() {
  local name="$1" img="$2"; shift 2
  local cmd="$*"

  cp "$WORK/$img.pristine" "$WORK/$img.img"
  ssh "$SMOKE_HOST" "cd $REMOTE_DIR && cp $img.pristine $img.img" 2>/dev/null

  local lout lrc rout rrc
  lout="$(cd "$WORK" && eval "\"$LOCAL_BIN\" $cmd" 2>&1)"; lrc=$?
  rout="$(ssh "$SMOKE_HOST" "cd $REMOTE_DIR && $REMOTE_BIN $cmd" 2>&1)"; rrc=$?

  scp -q "$SMOKE_HOST:$REMOTE_DIR/$img.img" "$WORK/$img-remote.img" 2>/dev/null

  # A second local run at a different instant, so differencing the two identifies the clock bytes.
  # The sleep is load-bearing: one-second granularity means back-to-back runs record the same time.
  sleep 2
  cp "$WORK/$img.pristine" "$WORK/$img-local2.img"
  (cd "$WORK" && eval "\"$LOCAL_BIN\" ${cmd/$img.img/$img-local2.img}" >/dev/null 2>&1)

  local ok=1
  if [ "$lrc" != "$rrc" ]; then
    fail "$name: exit status differs (local=$lrc ppc=$rrc)"
    info "local: $(printf '%s' "$lout" | tail -2 | tr '\n' ' ')"
    info "ppc  : $(printf '%s' "$rout" | tail -2 | tr '\n' ' ')"
    ok=0
  fi

  local mask="--mask-pair $WORK/$img.img $WORK/$img-local2.img"
  python3 "$IMGDIFF" "$WORK/$img.pristine" "$WORK/$img.img" $mask \
    > "$WORK/$name.ldiff" 2>&1
  python3 "$IMGDIFF" "$WORK/$img.pristine" "$WORK/$img-remote.img" $mask \
    > "$WORK/$name.rdiff" 2>&1
  if ! diff -q "$WORK/$name.ldiff" "$WORK/$name.rdiff" >/dev/null 2>&1; then
    fail "$name: the two builds changed different bytes"
    diff "$WORK/$name.ldiff" "$WORK/$name.rdiff" | head -8 | sed 's/^/        /'
    ok=0
  fi

  LAST_LOUT="$lout"; LAST_ROUT="$rout"; LAST_LRC="$lrc"
  [ $ok -eq 1 ] && return 0 || return 1
}

# extract_both NAME IMG PATH -- pull the file out of each image, compare bytes
extract_both() {
  local name="$1" img="$2" path="$3"
  (cd "$WORK" && "$LOCAL_BIN" get "$img.img"    "$path" "$WORK/$name.lfile" >/dev/null 2>&1)
  (cd "$WORK" && "$LOCAL_BIN" get "$img-remote.img" "$path" "$WORK/$name.rfile" >/dev/null 2>&1)
  if [ ! -s "$WORK/$name.lfile" ] || [ ! -s "$WORK/$name.rfile" ]; then
    fail "$name: could not extract $path from one of the images"; return 1
  fi
  if cmp -s "$WORK/$name.lfile" "$WORK/$name.rfile"; then return 0; fi
  fail "$name: extracted $path differs between builds"
  info "local: $(od -An -tx1 "$WORK/$name.lfile" | head -2 | tr -s ' ')"
  info "ppc  : $(od -An -tx1 "$WORK/$name.rfile" | head -2 | tr -s ' ')"
  return 1
}

echo
echo "== edit: CP437 + CRLF must survive a round trip =="
# The appending editor, not a no-op one: a no-op skips the write and the encode path never runs.
if run_both edit-roundtrip fat "edit fat.img /DOSTEXT.TXT --editor ./ed-ascii.sh"; then
  if extract_both edit-roundtrip fat /DOSTEXT.TXT; then
    # The original bytes, with one ASCII line appended in the file's own form.
    perl -e 'print "REM \xB3 box drawing\r\nSET X=1\r\n\xB3\xB3\xB3\r\nAPPENDED LINE\r\n"' \
      > "$WORK/dostext.expected"
    if cmp -s "$WORK/edit-roundtrip.rfile" "$WORK/dostext.expected"; then
      pass "edit re-encoded to CP437 + CRLF byte-for-byte (0xB3 intact)"
    else
      fail "edit round trip altered the file on $SMOKE_HOST"
      info "want: $(od -An -tx1 "$WORK/dostext.expected"      | head -3 | tr -s ' ')"
      info "got : $(od -An -tx1 "$WORK/edit-roundtrip.rfile"  | head -3 | tr -s ' ')"
    fi
  fi
fi

echo
echo "== edit: a no-op edit must write nothing at all =="
if run_both edit-noop fat "edit fat.img /DOSTEXT.TXT --editor ./ed-noop.sh"; then
  if [ "$(head -1 "$WORK/edit-noop.rdiff")" = "IDENTICAL" ]; then
    pass "no-op edit left the image untouched"
  else
    fail "no-op edit rewrote the image on $SMOKE_HOST"
    head -4 "$WORK/edit-noop.rdiff" | sed 's/^/        /'
  fi
fi

echo
echo "== edit: an em-dash must be refused by position, with nothing written =="
if run_both edit-emdash fat "edit fat.img /DOSTEXT.TXT --editor ./ed-emdash.sh"; then
  if [ "$LAST_LRC" -eq 0 ]; then
    fail "edit accepted an em-dash into a CP437 file (expected refusal)"
  # Match `line N, col N` exactly: the informational first line contains those words anyway.
  elif ! printf '%s' "$LAST_ROUT" | grep -qiE 'line [0-9]+, col(umn)? [0-9]+'; then
    fail "edit refused the em-dash but did not name a position"
    info "ppc: $(printf '%s' "$LAST_ROUT" | tail -3 | tr '\n' ' ')"
  elif [ "$(head -1 "$WORK/edit-emdash.rdiff")" != "IDENTICAL" ]; then
    fail "edit refused the em-dash but still wrote to the image"
    head -4 "$WORK/edit-emdash.rdiff" | sed 's/^/        /'
  else
    pass "em-dash refused by position, image untouched"
    info "ppc: $(printf '%s' "$LAST_ROUT" | grep -iE 'line [0-9]+, col(umn)? [0-9]+' | head -1)"
  fi
fi

echo
echo "== edit: an ASCII edit must land identically on both builds =="
if run_both edit-ascii fat "edit fat.img /ATTR.TXT --editor ./ed-ascii.sh"; then
  extract_both edit-ascii fat /ATTR.TXT && \
    grep -q 'APPENDED LINE' "$WORK/edit-ascii.rfile" && \
    pass "edit writes an ASCII change identically" || \
    fail "edit ASCII change did not land"
fi

echo
echo "== edit --line-endings crlf --no-edit on a mixed-ending file =="
if run_both edit-crlf fat "edit fat.img /MIXED.TXT --line-endings crlf --no-edit"; then
  if extract_both edit-crlf fat /MIXED.TXT; then
    lf=$(od -An -tx1 "$WORK/edit-crlf.rfile" | tr -s ' ' '\n' | grep -c '^0a$')
    cr=$(od -An -tx1 "$WORK/edit-crlf.rfile" | tr -s ' ' '\n' | grep -c '^0d$')
    if [ "$lf" -eq 5 ] && [ "$cr" -eq 5 ]; then
      pass "all five lines converted to CRLF (5x 0d, 5x 0a)"
    else
      fail "line-ending conversion wrong on $SMOKE_HOST: ${cr}x 0d, ${lf}x 0a (want 5 and 5)"
    fi
  fi
fi

echo
echo "== chmeta --attrs on FAT =="
if run_both chmeta-ro fat "chmeta fat.img /ATTR.TXT --attrs +readonly"; then
  pass "chmeta --attrs +readonly agrees byte-for-byte"
fi
if run_both chmeta-hide fat "chmeta fat.img /ATTR.TXT --attrs -hidden"; then
  pass "chmeta --attrs -hidden agrees byte-for-byte"
fi

echo
echo "== chmeta --protection rwd on AFFS (active-low: rwd must land as 00000002) =="
if run_both chmeta-prot affs "chmeta affs.img /prot.txt --protection rwd"; then
  # Big-endian canary: active-low RWED means rwd must land as 00 00 00 02, the 0x02 in the last byte.
  if grep -qE 'new=02$' "$WORK/chmeta-prot.rdiff"; then
    pass "protection longword written big-endian as 00000002 (E denied)"
    info "$(grep -E 'new=' "$WORK/chmeta-prot.rdiff" | tr '\n' ' ')"
  else
    fail "protection byte is not 02 in the low byte - suspect byte order"
    head -5 "$WORK/chmeta-prot.rdiff" | sed 's/^/        /'
  fi
fi

echo
echo "== put --force metadata preservation =="
if run_both put-preserve-ext ext "put ext.img replace.txt /meta.txt --force"; then
  lo="$(cd "$WORK" && "$LOCAL_BIN" ls ext-remote.img / -o 2>&1 | grep meta)"
  if printf '%s' "$lo" | grep -q '1234:5678' && printf '%s' "$lo" | grep -q 'rwxr-x---'; then
    pass "put --force preserved mode 750 and owner 1234:5678"
  else
    fail "put --force did not preserve mode/owner on $SMOKE_HOST"
    info "$(printf '%s' "$lo" | head -3 | tr '\n' ' ')"
  fi
fi
# On Unix this does not reset mode/owner (a desktop bug in put.rs), so only agreement is checked.
if run_both put-fresh-ext ext "put ext.img replace.txt /meta.txt --force --no-preserve-meta"; then
  ll="$(cd "$WORK" && "$LOCAL_BIN" ls ext.img    / -o 2>&1 | grep meta)"
  lr="$(cd "$WORK" && "$LOCAL_BIN" ls ext-remote.img / -o 2>&1 | grep meta)"
  if [ "$ll" = "$lr" ]; then
    pass "put --no-preserve-meta agrees on both builds"
    printf '%s' "$lr" | grep -q '1234:5678' && \
      info "both builds keep owner 1234:5678 here - known desktop bug, not a port regression"
  else
    fail "put --no-preserve-meta differs between builds"
    info "local: $ll"
    info "ppc  : $lr"
  fi
fi
if run_both put-preserve-hfs hfs "put hfs.img replace.txt /meta.txt --force"; then
  lo="$(cd "$WORK" && "$LOCAL_BIN" ls hfs-remote.img / 2>&1)"
  if printf '%s' "$lo" | grep -q 'TEXT' && printf '%s' "$lo" | grep -q 'MSWD'; then
    pass "put --force preserved type/creator TEXT/MSWD"
  else
    fail "put --force lost type/creator on $SMOKE_HOST (the BINA bug's shape)"
    info "$(printf '%s' "$lo" | head -4 | tr '\n' ' ')"
  fi
fi
# type/creator does go through the preserved-meta path, so the opt-out works here.
if run_both put-fresh-hfs hfs "put hfs.img replace.txt /meta.txt --force --no-preserve-meta"; then
  lo="$(cd "$WORK" && "$LOCAL_BIN" ls hfs-remote.img / 2>&1)"
  if printf '%s' "$lo" | grep -q 'MSWD'; then
    fail "--no-preserve-meta still kept creator MSWD"
    info "$(printf '%s' "$lo" | head -4 | tr '\n' ' ')"
  else
    pass "put --no-preserve-meta gave the file a fresh creator"
  fi
fi

echo
if [ $fails -eq 0 ]; then
  printf '\033[32m%s agrees with the desktop build on every write verb.\033[0m\n' "$SMOKE_HOST"
else
  printf '\033[31m%d mismatch(es).\033[0m\n' "$fails"
fi
exit $fails
