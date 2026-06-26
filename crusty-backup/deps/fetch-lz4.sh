#!/bin/sh
# Fetch LZ4 source and cross-build a DJGPP static lib (liblz4.a + headers) into
# crusty-backup/deps/lz4/, so the cb-dos backup engine can offer the LZ4 frame
# codec (`/CODEC:LZ4`) alongside gzip. Like zlib, LZ4 compiles unmodified under
# DJGPP -- it's portable C with no platform assumptions. We use the frame API
# (lz4frame.c) so the on-disk `.lz4` is the standard LZ4 frame format, byte-for-
# byte interchangeable with the desktop's lz4_flex. Built from source (pinned
# version, reproducible) and gitignored like zlib / net/watt32.
#
# Usage:  sh crusty-backup/deps/fetch-lz4.sh
# Override the version/URL with LZ4_URL=... if needed.
set -e

HERE=$(CDPATH= cd "$(dirname "$0")" && pwd)
DJGPP="${DJGPP:-$HOME/djgpp}"
CC="$DJGPP/bin/i586-pc-msdosdjgpp-gcc"
AR="$DJGPP/bin/i586-pc-msdosdjgpp-ar"
LZ4_URL="${LZ4_URL:-https://github.com/lz4/lz4/releases/download/v1.10.0/lz4-1.10.0.tar.gz}"
DL="$HERE/dl"
OUT="$HERE/lz4"

[ -x "$CC" ] || { echo "DJGPP cross-gcc not found at $CC"; exit 1; }
mkdir -p "$DL" "$OUT/lib" "$OUT/inc"
TGZ="$DL/$(basename "$LZ4_URL")"
[ -f "$TGZ" ] || { echo "Fetching LZ4: $LZ4_URL"; curl -fsSL -o "$TGZ" "$LZ4_URL"; }

TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
tar xzf "$TGZ" -C "$TMP"
SRC=$(echo "$TMP"/lz4-*)/lib

# The library sources live in lib/. lz4.c (block codec), lz4hc.c (high-compression
# -- pulled in by lz4frame), lz4frame.c (the frame format), xxhash.c (frame
# content checksum). Headers are self-contained; no autoconf.
( cd "$SRC" && "$CC" -O2 -march=i486 -mtune=i586 -DXXH_NAMESPACE=LZ4_ -c \
    lz4.c lz4hc.c lz4frame.c xxhash.c && "$AR" rcs liblz4.a *.o )

cp "$SRC/liblz4.a" "$OUT/lib/"
cp "$SRC/lz4.h" "$SRC/lz4hc.h" "$SRC/lz4frame.h" "$SRC/lz4frame_static.h" \
   "$SRC/xxhash.h" "$OUT/inc/"
echo "LZ4 ready in $OUT :"
echo "  headers: $OUT/inc/lz4frame.h ..."
echo "  library: $OUT/lib/liblz4.a"
