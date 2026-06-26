#!/bin/bash
# rb-daemon.sh - Rusty Backup daemon (rb-daemon) control console for MiSTer.
#
# This is the ONLY entry the MiSTer "Scripts" menu shows for the daemon. Opening
# it brings up the interactive setup console where you can start/stop the daemon,
# enable or disable it on boot, and read the IP:port other machines connect to.
#
# The actual program is the `rb-cli` binary sitting next to this script. A bare
# `rb-cli` has no `.sh` extension, so the Scripts menu does not list it as a
# second entry - exactly one menu item, one binary.
#
# At boot, user-startup.sh launches `rb-cli serve service start` directly, so
# this shim is normally only the interactive front door. It still forwards any
# arguments through to `rb-cli serve` (so `rb-daemon.sh service start` works too).

set -e

DIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"
RB_CLI="$DIR/rb-cli"

# When delivered by MiSTer Downloader (a custom database), the binary can arrive
# without the executable bit set. install.sh chmods it, but the Downloader path
# skips install.sh, so set it here too - this keeps the menu entry working with
# no manual chmod regardless of how rb-cli got onto the card.
if [ -f "$RB_CLI" ] && [ ! -x "$RB_CLI" ]; then
    chmod +x "$RB_CLI" 2>/dev/null || true
fi

if [ ! -x "$RB_CLI" ]; then
    echo "rb-cli was not found next to rb-daemon.sh:"
    echo "  $RB_CLI"
    echo
    echo "Re-run the rb-daemon installer (install.sh), or copy the MiSTer rb-cli"
    echo "binary to $DIR/rb-cli and try again."
    echo
    echo "Press Enter to close."
    read -r _ || true
    exit 1
fi

if [ "$#" -gt 0 ]; then
    # Boot / scripted path: `rb-daemon.sh service start`, `... service status`, etc.
    exec "$RB_CLI" serve "$@"
fi

# Interactive path (launched from the Scripts menu): open the setup console.
exec "$RB_CLI" serve setup
