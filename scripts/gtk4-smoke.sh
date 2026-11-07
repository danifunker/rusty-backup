#!/bin/bash
# Tier B smoke for the GTK4 shell. Run inside `dbus-run-session -- xvfb-run -a`; needs
# at-spi2-core, python3-gi with the Atspi typelib, dosfstools and mtools.
# Usage: scripts/gtk4-smoke.sh <path to rusty-backup-gtk>
set -euo pipefail

BIN=${1:?path to rusty-backup-gtk}
WORK=$(mktemp -d)
trap 'kill "${APP:-0}" 2>/dev/null || true; rm -rf "$WORK"' EXIT

# A scratch HOME keeps the user's config and recent files out of it.
mkdir -p "$WORK/home"
echo "hello" > "$WORK/hello.txt"
mkfs.fat -C "$WORK/fixture.img" 1440 >/dev/null
mcopy -i "$WORK/fixture.img" "$WORK/hello.txt" ::HELLO.TXT

/usr/libexec/at-spi-bus-launcher --launch-immediately &
sleep 1

HOME="$WORK/home" GTK_A11Y=atspi GDK_BACKEND=x11 GDK_DEBUG=no-portals NO_AT_BRIDGE=0 \
  "$BIN" "$WORK/fixture.img" > "$WORK/app.log" 2>&1 &
APP=$!

status=0
python3 -I "$(dirname "$0")/gtk4-smoke.py" rusty || status=$?
if [ "$status" -ne 0 ]; then
  echo "--- app log ---"
  cat "$WORK/app.log"
fi
exit "$status"
