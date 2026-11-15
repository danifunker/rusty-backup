#!/bin/sh
# Fails when the built app imports a symbol from libSystem, libc++, libobjc or libiconv that
# Mac OS X 10.7 does not export, which would make dyld abort on Lion when it binds it.
# lion/exports.txt.gz was taken from a 10.7.5 Mac:
#   nm -gU /usr/lib/libSystem.B.dylib /usr/lib/system/*.dylib /usr/lib/libiconv.2.dylib \
#       /usr/lib/libc++.1.dylib /usr/lib/libc++abi.dylib /usr/lib/libobjc.A.dylib
# Framework C calls from the engine are not covered; the Objective-C host is held to 10.7 by
# -Werror=unguarded-availability instead.
#
#   frontends-vintage/cocoa/check-lion-imports.sh "path/to/Rusty Backup.app/Contents/MacOS/Rusty Backup"
set -eu
HERE="$(cd "$(dirname "$0")" && pwd)"
BIN="$1"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
nm -m -u "$BIN" | grep -E '\(from (libSystem|libc\+\+|libc\+\+abi|libobjc|libiconv)\)' \
    | sed -E 's/^.*external //; s/ \(from .*$//' | sort -u > "$TMP/imports"
gzip -dc "$HERE/lion/exports.txt.gz" > "$TMP/exports"
MISSING="$(comm -23 "$TMP/imports" "$TMP/exports")"
if [ -n "$MISSING" ]; then
    echo "Symbols Mac OS X 10.7 does not export (the app would crash on Lion; see rb-cli-vintage/lion_compat.cpp):" >&2
    echo "$MISSING" >&2
    exit 1
fi
echo "Lion imports: $(wc -l < "$TMP/imports" | tr -d ' ') system symbols, all present on 10.7"
