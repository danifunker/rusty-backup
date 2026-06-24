#!/bin/bash
# install.sh - install rb-daemon (the Rusty Backup network daemon) on a MiSTer.
#
# Run this from inside the unpacked rb-cli-mini release tarball, on the MiSTer
# itself (or over SSH). It drops two files into /media/fat/Scripts:
#
#   rb-cli         - the program (no .sh extension -> NOT shown in the Scripts menu)
#   rb-daemon.sh   - the Scripts-menu entry that opens the daemon setup console
#
# It does NOT enable the daemon by itself - open "rb-daemon" from the Scripts
# menu (or run `rb-cli serve service install`) to start it and turn on autostart.

set -e

SCRIPTS="/media/fat/Scripts"
SRC="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"

# The binary ships as `rb-cli-mini` in the release tarball; install it as
# `rb-cli` so it stays out of the Scripts menu (which lists only *.sh files).
BIN_SRC=""
for cand in "$SRC/rb-cli-mini" "$SRC/rb-cli"; do
    if [ -f "$cand" ]; then
        BIN_SRC="$cand"
        break
    fi
done
if [ -z "$BIN_SRC" ]; then
    echo "Could not find rb-cli-mini (or rb-cli) next to install.sh."
    echo "Run this script from inside the unpacked release tarball."
    exit 1
fi

if [ ! -f "$SRC/rb-daemon.sh" ]; then
    echo "Could not find rb-daemon.sh next to install.sh."
    exit 1
fi

mkdir -p "$SCRIPTS"
cp "$BIN_SRC" "$SCRIPTS/rb-cli"
cp "$SRC/rb-daemon.sh" "$SCRIPTS/rb-daemon.sh"
chmod +x "$SCRIPTS/rb-cli" "$SCRIPTS/rb-daemon.sh"

echo "Installed:"
echo "  $SCRIPTS/rb-cli        (the program; not shown in the Scripts menu)"
echo "  $SCRIPTS/rb-daemon.sh  (the Scripts-menu entry)"
echo
echo "Next: open 'rb-daemon' from the MiSTer Scripts menu to start the daemon"
echo "and enable it on boot. To do it now from the shell:"
echo "  $SCRIPTS/rb-cli serve service install"
