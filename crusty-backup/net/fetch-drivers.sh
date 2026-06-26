#!/bin/sh
# Fetch the Crynwr packet-driver collection (GPLv2) into crusty-backup/net/drivers/
# so mkmedia.sh can stage packet drivers on the cb-dos media.
#
# A packet driver is a tiny per-NIC TSR that presents the card to the TCP/IP
# stack on a software interrupt (we use 60h). crustybk's networked backup
# (`backup rb://...`, WATT-32) and the mTCP CD suite both need one for your card.
# The Crynwr library is the classic GPL set covering the ISA-era cards (NE2000,
# 3C5xx, WD80x3, ...) plus PCNTPK (AMD PCnet) and RTSPKT (Realtek RTL8139).
#
# These .com drivers are VENDORED (committed) -- this script is how they were
# obtained / how to refresh them, and it satisfies GPLv2 by recording the source.
# The GPL text travels with them as drivers/CRYNWR-GPL.txt.
#
# Usage:  sh crusty-backup/net/fetch-drivers.sh
# Override the source with DRIVERS_URL=... (e.g. a newer FreeDOS net repo mirror).
set -e

HERE=$(CDPATH= cd "$(dirname "$0")" && pwd)
DRIVERS_URL="${DRIVERS_URL:-http://www.ibiblio.org/pub/micro/pc-stuff/freedos/files/distributions/1.1/repos/net/crynwr.zip}"
DL="$HERE/dl"
OUT="$HERE/drivers"

mkdir -p "$DL" "$OUT"
ZIP="$DL/crynwr.zip"

if [ ! -f "$ZIP" ]; then
    echo "Fetching Crynwr packet drivers: $DRIVERS_URL"
    curl -fsSL -o "$ZIP" "$DRIVERS_URL"
fi

# The zip scatters the .com drivers (and Crynwr utilities) across subdirs along
# with source + docs. Pull every .com out flat, lower-cased for tidy 8.3 names.
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
unzip -oq "$ZIP" -d "$TMP"
n=0
for f in $(find "$TMP" -iname "*.com"); do
    base=$(basename "$f" | tr 'A-Z' 'a-z')
    cp "$f" "$OUT/$base"
    n=$((n + 1))
done

# Keep the GPL text alongside the binaries (GPLv2 requires the license travel
# with them; source is the crynwr.zip above).
gpl=$(find "$TMP" -iname "gpl.doc" -o -iname "copying*" | head -1)
[ -n "$gpl" ] && cp "$gpl" "$OUT/CRYNWR-GPL.txt"

echo "Staged $n packet driver(s) into $OUT"
echo "Refresh / extend the set by editing net/drivers/ and rebuilding the media."
