#!/bin/sh
# Fetch the WATT-32 TCP/IP library (BSD-licensed, by Gisle Vanem / Erick Engelke)
# as a prebuilt DJGPP package, into crusty-backup/net/watt32/ so the cb-dos
# networked client (CRUSTYBK / net_hello) can link it.
#
# WATT-32 gives DJGPP a BSD-sockets API (socket/connect/send/recv) over a
# *packet driver* (which you supply for your NIC — see drivers/DRIVERS.TXT).
# We use the prebuilt DJGPP binary package (libwatt.a + headers) rather than
# cross-building from source — no Watcom, no toolchain mixing. See
# docs/cb_dos_network_and_state.md (§1b: WATT-32 chosen over mTCP for the
# DJGPP/C client).
#
# License note: WATT-32 is BSD ("Regents of California"); the advertising-
# acknowledgment clause is a permitted additional term under AGPL-3.0 §7, so it
# is compatible with this project's AGPL-3.0 license. The license file is copied
# alongside the library.
#
# Usage:  sh crusty-backup/net/fetch-watt32.sh
# Override the version/URL with WATT32_URL=... if a newer build is out.
set -e

HERE=$(CDPATH= cd "$(dirname "$0")" && pwd)
WATT32_URL="${WATT32_URL:-https://www.delorie.com/pub/djgpp/current/v2tk/wat3211b.zip}"
DL="$HERE/dl"
OUT="$HERE/watt32"

mkdir -p "$DL" "$OUT"
ZIP="$DL/$(basename "$WATT32_URL")"

if [ ! -f "$ZIP" ]; then
    echo "Fetching WATT-32 (DJGPP binary package): $WATT32_URL"
    curl -fsSL -o "$ZIP" "$WATT32_URL"
fi

# The package lays the library out under net/watt/{inc,lib}; lift those two
# trees up to watt32/{inc,lib} so the Makefile's -I/-L paths are short.
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
unzip -o -q "$ZIP" "net/watt/inc/*" "net/watt/lib/*" -d "$TMP"
rm -rf "$OUT/inc" "$OUT/lib"
cp -R "$TMP/net/watt/inc" "$OUT/inc"
cp -R "$TMP/net/watt/lib" "$OUT/lib"
# Keep the BSD license alongside the library (it must travel with it).
[ -f "$OUT/inc/copying.bsd" ] && cp "$OUT/inc/copying.bsd" "$OUT/COPYING.BSD"

echo "WATT-32 ready in $OUT :"
echo "  headers: $OUT/inc   (tcp.h, sys/socket.h, netinet/, arpa/)"
echo "  library: $OUT/lib/libwatt.a"
echo
echo "Build the networked client:  make -C crusty-backup net"
echo "On the DOS box you still need: a packet driver for your NIC + WATTCP.CFG"
echo "(DHCP or a static IP). See docs/cb_dos_networking.md."
