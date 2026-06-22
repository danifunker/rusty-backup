#!/bin/sh
# Fetch the mTCP DOS TCP/IP applications (GPLv3, by Michael Brutman) into
# crusty-backup/net/mtcp/ so mkmedia.sh can drop them onto the cb-dos CD.
#
# mTCP gives the DOS box DHCP + FTP/FTPSRV + ping over a *packet driver* (which
# you supply for your NIC — see drivers/DRIVERS.TXT). See docs/cb_dos_networking.md.
#
# Usage:  sh crusty-backup/net/fetch-mtcp.sh
# Override the version/URL with MTCP_URL=... if a newer build is out.
set -e

HERE=$(CDPATH= cd "$(dirname "$0")" && pwd)
MTCP_URL="${MTCP_URL:-https://www.brutman.com/mTCP/download/mTCP_2025-01-10.zip}"
DL="$HERE/dl"
OUT="$HERE/mtcp"

mkdir -p "$DL" "$OUT"
ZIP="$DL/$(basename "$MTCP_URL")"

if [ ! -f "$ZIP" ]; then
    echo "Fetching mTCP: $MTCP_URL"
    curl -fsSL -o "$ZIP" "$MTCP_URL"
fi

# The zip holds the .exe apps plus docs/samples; we only want the small set the
# appliance uses (8.3 names already). Pull them out flat into mtcp/.
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
unzip -o -q "$ZIP" -d "$TMP"
for app in DHCP.EXE FTP.EXE FTPSRV.EXE PING.EXE NC.EXE SNTP.EXE PKTTOOL.EXE; do
    f=$(find "$TMP" -iname "$app" | head -1)
    [ -n "$f" ] && cp "$f" "$OUT/$(basename "$app")"
done
# Keep the license alongside the binaries (GPLv3 requires it travels with them).
lic=$(find "$TMP" -iname "COPYING*" -o -iname "LICENSE*" | head -1)
[ -n "$lic" ] && cp "$lic" "$OUT/COPYING.TXT"

echo "mTCP apps in $OUT :"
ls -1 "$OUT"
echo
echo "Next: drop your NIC's packet driver into net/drivers/ (see DRIVERS.TXT),"
echo "then rebuild the media — mkmedia.sh will add a \\NET dir to the CD."
