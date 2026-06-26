#!/bin/sh
# Build WATT-32 FROM SOURCE for DJGPP, targeting i386/i486 instructions only, so
# the networked CRUSTYBK runs on a 486. The prebuilt DJGPP package (fetched by
# the older fetch-watt32.sh) was compiled with 686 `cmov`, which faults on a 486;
# building from source uses the makefile's default `-march=i386` (cmov/fcmov-free
# -- verified) while `cpumodel.S` still runtime-detects the CPU and falls back to
# the 8254 PIT timer instead of `rdtsc` on pre-Pentium hardware.
#
# Output layout is IDENTICAL to fetch-watt32.sh (net/watt32/{inc,lib}) so the
# Makefile's -I/-L paths are unchanged. Needs: the DJGPP cross toolchain (via
# BIN_PREFIX), nasm (the packet-stub assembler), make, curl, tar.
#
# License note: WATT-32 is BSD (compatible with AGPL-3.0 via §7); the license
# travels with the headers (copying.bsd).
#
# Usage:  sh crusty-backup/net/build-watt32-src.sh
# Override: WATT32_SRC_URL=... / WATT32_BIN_PREFIX=...
set -e

HERE=$(CDPATH= cd "$(dirname "$0")" && pwd)
DL="$HERE/dl"
OUT="$HERE/watt32"
SRCDIR="$DL/watt32-src"
PREFIX="${WATT32_BIN_PREFIX:-i586-pc-msdosdjgpp-}"
# Pinned to a validated commit for reproducible CI builds.
WATT32_REF="${WATT32_REF:-c727114d209138e3057c122134cf7c3f3e67c374}"
WATT32_SRC_URL="${WATT32_SRC_URL:-https://github.com/gvanem/Watt-32/archive/${WATT32_REF}.tar.gz}"

mkdir -p "$DL" "$OUT"

if [ ! -f "$SRCDIR/src/configur.sh" ]; then
    echo "Fetching WATT-32 source: $WATT32_SRC_URL"
    curl -fsSL -o "$DL/watt32-src.tar.gz" "$WATT32_SRC_URL"
    rm -rf "$SRCDIR"; mkdir -p "$SRCDIR"
    tar -xzf "$DL/watt32-src.tar.gz" -C "$SRCDIR" --strip-components=1
fi

cd "$SRCDIR/src"

# Generate the djgpp target makefile + build dir.
sh ./configur.sh djgpp

# The cross-config deliberately skips the two error tables because they come
# from a DOS-only tool that reads the target libc. Supply vendored copies: a
# djgpp.err (the WATT-32 extended socket-error strings) and a syserr.c that
# defines _w32_sys_errlist[]/_w32_sys_nerr (which neterr.c references).
cp "$HERE/watt32-djgpp.err" ../inc/sys/djgpp.err
mkdir -p build/djgpp
cp "$HERE/watt32-syserr.c" build/djgpp/syserr.c

make -f djgpp.mak BIN_PREFIX="$PREFIX"

# Stage headers + library exactly where fetch-watt32.sh put them.
rm -rf "$OUT/inc" "$OUT/lib"
mkdir -p "$OUT/lib"
cp -R ../inc "$OUT/inc"
cp ../lib/libwatt.a "$OUT/lib/libwatt.a"
[ -f ../inc/copying.bsd ] && cp ../inc/copying.bsd "$OUT/COPYING.BSD"

echo "WATT-32 (i386/i486, from source) built:"
echo "  $OUT/lib/libwatt.a  ($(wc -c < "$OUT/lib/libwatt.a") bytes)"
