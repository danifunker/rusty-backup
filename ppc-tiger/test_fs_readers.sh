#!/bin/sh
# test_fs_readers.sh - Validate the ext2/3/4 + exFAT read-only browse
# (ls / get) on PowerPC/Tiger. This is the big-endian acceptance test:
# the readers were only smoke-tested on little-endian hardware, so this
# run is what actually exercises the read_le* byteswap paths.
#
# Self-contained: the fixture images ship gzip-compressed in
# ppc-tiger/fixtures/ (a few KB each since they are mostly zeros) and
# are decompressed to a temp dir automatically. Just build, then run:
#
#   ./test_fs_readers.sh
#
# To test against your own raw images instead, pass them explicitly:
#   ./test_fs_readers.sh /path/ext2.img /path/exfat.img /path/efs.img /path/sgi.img
#
# Set SKIP_BUILD=1 to reuse an existing ./rusty-backup-ppc.

set -u

BIN="./rusty-backup-ppc"
FIXTURES="./fixtures"
TMP="${TMPDIR:-/tmp}/rbfs.$$"
mkdir -p "$TMP"

# Resolve a fixture image: use the explicit arg if given, else gunzip the
# committed .gz into $TMP. Echoes the usable path (empty if unavailable).
resolve_fixture() {  # <explicit-arg> <name>
    if [ -n "$1" ]; then echo "$1"; return; fi
    if [ -f "$FIXTURES/$2.gz" ]; then
        gunzip -c "$FIXTURES/$2.gz" > "$TMP/$2" 2>/dev/null && echo "$TMP/$2"
    fi
}

EXT2_IMG=`resolve_fixture "${1:-}" ext2.img`
EXFAT_IMG=`resolve_fixture "${2:-}" exfat.img`
EFS_IMG=`resolve_fixture "${3:-}" efs.img`
SGI_IMG=`resolve_fixture "${4:-}" sgi.img`

PASS=0
FAIL=0

ok()   { echo "  PASS: $1"; PASS=`expr $PASS + 1`; }
bad()  { echo "  FAIL: $1"; FAIL=`expr $FAIL + 1`; }

# check_contains <file> <substring> <label>
check_contains() {
    if grep "$2" "$1" >/dev/null 2>&1; then ok "$3"; else
        bad "$3 (did not find '$2' in:)"; sed 's/^/      /' "$1"; fi
}

# check_text <extracted-file> <expected-string> <label>
check_text() {
    got=`cat "$1" 2>/dev/null`
    if [ "$got" = "$2" ]; then ok "$3"; else
        bad "$3 (got '$got', expected '$2')"; fi
}

# check_fill <extracted-file> <expected-size> <fill-char> <label>
# Verifies the file is exactly <size> bytes and contains ONLY <fill-char>.
check_fill() {
    sz=`wc -c < "$1" 2>/dev/null | tr -d ' '`
    leftover=`tr -d "$3" < "$1" 2>/dev/null | wc -c | tr -d ' '`
    if [ "$sz" = "$2" ] && [ "$leftover" = "0" ]; then ok "$4"; else
        bad "$4 (size=$sz expected=$2, non-'$3' bytes=$leftover)"; fi
}

echo "=== Build ==="
# Always rebuild from source so a stale binary can't silently mask new
# code (an old binary lacks ext2/exFAT detection and fails confusingly).
# Set SKIP_BUILD=1 to reuse the existing $BIN instead.
if [ "${SKIP_BUILD:-0}" = "1" ]; then
    echo "  SKIP_BUILD=1 set, reusing $BIN"
else
    ./build.sh || { echo "BUILD FAILED"; exit 2; }
fi
[ -x "$BIN" ] || { echo "ERROR: $BIN not found - build it first"; exit 2; }
"$BIN" --version
echo ""

# ---------------------------------------------------------------
echo "=== ext2 ($EXT2_IMG) ==="
if [ ! -f "$EXT2_IMG" ]; then
    echo "  SKIP: $EXT2_IMG not found"
else
    "$BIN" ls "$EXT2_IMG" /         > "$TMP/e_root.txt" 2>&1
    "$BIN" ls "$EXT2_IMG" /subdir   > "$TMP/e_sub.txt"  2>&1
    echo "  --- ls / ---"; sed 's/^/    /' "$TMP/e_root.txt"

    check_contains "$TMP/e_root.txt" "hello.txt"        "ext2: root lists hello.txt"
    check_contains "$TMP/e_root.txt" "big.bin"          "ext2: root lists big.bin"
    check_contains "$TMP/e_root.txt" "subdir/"          "ext2: root lists subdir/"
    check_contains "$TMP/e_root.txt" "inline_tiny.txt"  "ext2: root lists inline_tiny.txt"
    check_contains "$TMP/e_root.txt" "l.*symlink_fast"  "ext2: symlink_fast tagged 'l' in ls"
    check_contains "$TMP/e_sub.txt"  "inner.txt"        "ext2: subdir lists inner.txt"

    "$BIN" get "$EXT2_IMG" /hello.txt        "$TMP/e_hello"  >/dev/null 2>&1
    "$BIN" get "$EXT2_IMG" /subdir/inner.txt "$TMP/e_inner"  >/dev/null 2>&1
    "$BIN" get "$EXT2_IMG" /big.bin          "$TMP/e_big"    >/dev/null 2>&1
    "$BIN" get "$EXT2_IMG" /inline_tiny.txt  "$TMP/e_inline" >/dev/null 2>&1
    # symlink_fast: get must REFUSE with the target hint on stderr.
    "$BIN" get "$EXT2_IMG" /symlink_fast     "$TMP/e_sym"    >"$TMP/e_sym_out" 2>"$TMP/e_sym_err"
    sym_rc=$?

    check_text "$TMP/e_hello"  "Hello, ext2 world!"   "ext2: get hello.txt content"
    check_text "$TMP/e_inner"  "nested file contents" "ext2: get nested inner.txt content"
    check_fill "$TMP/e_big" 5000 A "ext2: get big.bin (5000 bytes, all 'A' = indirect map)"
    check_text "$TMP/e_inline" "INLINE FILE!"         "ext2: get inline_tiny.txt content (INLINE_DATA)"
    if [ "$sym_rc" != "0" ]; then ok "ext2: get on symlink exits non-zero"
    else bad "ext2: get on symlink exits non-zero (got rc=$sym_rc)"; fi
    check_contains "$TMP/e_sym_err" "is a symlink to 'target_path'" \
        "ext2: get on symlink prints target hint"
    if [ ! -s "$TMP/e_sym" ]; then ok "ext2: get on symlink does not write output file"
    else bad "ext2: get on symlink wrote $(wc -c < "$TMP/e_sym") bytes to output"; fi
fi
echo ""

# ---------------------------------------------------------------
echo "=== exFAT ($EXFAT_IMG) ==="
if [ ! -f "$EXFAT_IMG" ]; then
    echo "  SKIP: $EXFAT_IMG not found"
else
    "$BIN" ls "$EXFAT_IMG" /        > "$TMP/x_root.txt" 2>&1
    "$BIN" ls "$EXFAT_IMG" /subdir  > "$TMP/x_sub.txt"  2>&1
    echo "  --- ls / (macOS ._ and .fseventsd entries are expected noise) ---"
    sed 's/^/    /' "$TMP/x_root.txt"

    check_contains "$TMP/x_root.txt" "hello.txt" "exFAT: root lists hello.txt"
    check_contains "$TMP/x_root.txt" "big.bin"   "exFAT: root lists big.bin"
    check_contains "$TMP/x_root.txt" "subdir/"   "exFAT: root lists subdir/"
    check_contains "$TMP/x_sub.txt"  "inner.txt" "exFAT: subdir lists inner.txt"

    "$BIN" get "$EXFAT_IMG" /hello.txt        "$TMP/x_hello" >/dev/null 2>&1
    "$BIN" get "$EXFAT_IMG" /subdir/inner.txt "$TMP/x_inner" >/dev/null 2>&1
    "$BIN" get "$EXFAT_IMG" /big.bin          "$TMP/x_big"   >/dev/null 2>&1

    check_text "$TMP/x_hello" "Hello, exFAT world!" "exFAT: get hello.txt content"
    check_text "$TMP/x_inner" "nested exfat file"   "exFAT: get nested inner.txt content"
    check_fill "$TMP/x_big" 200000 B "exFAT: get big.bin (200000 bytes, all 'B' = cluster chain)"
fi
echo ""

# ---------------------------------------------------------------
echo "=== EFS / bare superfloppy ($EFS_IMG) ==="
if [ ! -f "$EFS_IMG" ]; then
    echo "  SKIP: $EFS_IMG not found"
else
    "$BIN" ls "$EFS_IMG" /        > "$TMP/f_root.txt" 2>&1
    "$BIN" ls "$EFS_IMG" /subdir  > "$TMP/f_sub.txt"  2>&1
    echo "  --- ls / ---"; sed 's/^/    /' "$TMP/f_root.txt"

    check_contains "$TMP/f_root.txt" "hello.txt" "EFS: root lists hello.txt"
    check_contains "$TMP/f_root.txt" "big.bin"   "EFS: root lists big.bin"
    check_contains "$TMP/f_root.txt" "subdir/"   "EFS: root lists subdir/"
    check_contains "$TMP/f_sub.txt"  "inner.txt" "EFS: subdir lists inner.txt"

    "$BIN" get "$EFS_IMG" /hello.txt        "$TMP/f_hello" >/dev/null 2>&1
    "$BIN" get "$EFS_IMG" /subdir/inner.txt "$TMP/f_inner" >/dev/null 2>&1
    "$BIN" get "$EFS_IMG" /big.bin          "$TMP/f_big"   >/dev/null 2>&1

    check_text "$TMP/f_hello" "Hello, EFS world!" "EFS: get hello.txt content"
    check_text "$TMP/f_inner" "nested efs file"   "EFS: get nested inner.txt content"
    check_fill "$TMP/f_big" 307200 E "EFS: get big.bin (307200 bytes, all 'E' = multi-extent)"
fi
echo ""

# ---------------------------------------------------------------
echo "=== SGI volume header ($SGI_IMG) ==="
if [ ! -f "$SGI_IMG" ]; then
    echo "  SKIP: $SGI_IMG not found"
else
    "$BIN" show partmap "$SGI_IMG" > "$TMP/s_pm.txt" 2>&1
    echo "  --- show partmap ---"; sed 's/^/    /' "$TMP/s_pm.txt"
    check_contains "$TMP/s_pm.txt" "SGI" "SGI: partition table identified as SGI"
    check_contains "$TMP/s_pm.txt" "EFS" "SGI: EFS partition listed"

    # EFS partition is entry @1 inside the volume header.
    "$BIN" ls "$SGI_IMG@1" /        > "$TMP/s_root.txt" 2>&1
    check_contains "$TMP/s_root.txt" "hello.txt" "SGI: ls @1 lists hello.txt"
    check_contains "$TMP/s_root.txt" "subdir/"   "SGI: ls @1 lists subdir/"

    "$BIN" get "$SGI_IMG@1" /subdir/inner.txt "$TMP/s_inner" >/dev/null 2>&1
    check_text "$TMP/s_inner" "nested efs file" "SGI: get @1 nested file content"
fi
echo ""

# ---------------------------------------------------------------
echo "=== Summary ==="
echo "  PASS=$PASS  FAIL=$FAIL"
rm -rf "$TMP"
[ "$FAIL" = "0" ]
