#!/bin/sh
# Fetch zlib source and cross-build a DJGPP static lib (libz.a + headers) into
# crusty-backup/deps/zlib/, so the cb-dos backup engine (cbbackup) can link
# gzip/DEFLATE + crc32. zlib compiles unmodified under DJGPP (docs/cb_dos.md §1);
# we build it from source rather than chase a prebuilt DJGPP package so the
# version is pinned and reproducible. Gitignored like net/watt32.
#
# Usage:  sh crusty-backup/deps/fetch-zlib.sh
# Override the version/URL with ZLIB_URL=... if needed.
set -e

HERE=$(CDPATH= cd "$(dirname "$0")" && pwd)
DJGPP="${DJGPP:-$HOME/djgpp}"
CC="$DJGPP/bin/i586-pc-msdosdjgpp-gcc"
AR="$DJGPP/bin/i586-pc-msdosdjgpp-ar"
ZLIB_URL="${ZLIB_URL:-https://github.com/madler/zlib/releases/download/v1.3.1/zlib-1.3.1.tar.gz}"
DL="$HERE/dl"
OUT="$HERE/zlib"

[ -x "$CC" ] || { echo "DJGPP cross-gcc not found at $CC"; exit 1; }
mkdir -p "$DL" "$OUT/lib" "$OUT/inc"
TGZ="$DL/$(basename "$ZLIB_URL")"
[ -f "$TGZ" ] || { echo "Fetching zlib: $ZLIB_URL"; curl -fsSL -o "$TGZ" "$ZLIB_URL"; }

TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
tar xzf "$TGZ" -C "$TMP"
SRC=$(echo "$TMP"/zlib-*)

# Compile the library .c files (the tarball's top level is exactly the lib
# sources + headers — no autoconf needed; the shipped zconf.h is usable as-is).
( cd "$SRC" && "$CC" -O2 -c \
    adler32.c compress.c crc32.c deflate.c gzclose.c gzlib.c gzread.c \
    gzwrite.c infback.c inffast.c inflate.c inftrees.c trees.c uncompr.c \
    zutil.c && "$AR" rcs libz.a *.o )

cp "$SRC/libz.a" "$OUT/lib/"
cp "$SRC/zlib.h" "$SRC/zconf.h" "$OUT/inc/"
echo "zlib ready in $OUT :"
echo "  header: $OUT/inc/zlib.h"
echo "  library: $OUT/lib/libz.a"
