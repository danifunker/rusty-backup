#!/bin/bash
# GUI regression suite for the GTK4 shell (scripts/gtk4-gui-suite.py). Run inside
# `dbus-run-session -- xvfb-run -a`; needs at-spi2-core, python3-gi with the Atspi typelib,
# xdotool, dosfstools and mtools; the screen must be at least 1600x2300.
# Usage: scripts/gtk4-gui-suite.sh <path to rusty-backup-gtk> [case ...]
set -euo pipefail

BIN=${1:?path to rusty-backup-gtk}
shift
WORK=$(mktemp -d)
trap 'kill "${APP:-0}" 2>/dev/null || true; rm -rf "$WORK"' EXIT

# A scratch HOME keeps the user's config and recent files out of it.
mkdir -p "$WORK/home"
echo "hello" > "$WORK/hello.txt"
mkfs.fat -C "$WORK/fixture.img" 1440 >/dev/null
mcopy -i "$WORK/fixture.img" "$WORK/hello.txt" ::HELLO.TXT

# The folder the CD cases build from: a file, its macOS ._ sidecar and a tar archive.
mkdir -p "$WORK/cdsrc" "$WORK/tarsrc"
echo "hello" > "$WORK/cdsrc/hello.txt"
python3 -I -c 'import struct,sys; open(sys.argv[1],"wb").write(struct.pack(">II16sH",0x00051607,0x00020000,bytes(16),0))' \
  "$WORK/cdsrc/._hello.txt"
echo "inside" > "$WORK/tarsrc/inner.txt"
tar -C "$WORK/tarsrc" --format=ustar -cf "$WORK/cdsrc/pack.tar" inner.txt
# The folder the Convert cases read: the fixture, and a copy in a subfolder.
mkdir -p "$WORK/convsrc/sub"
cp "$WORK/fixture.img" "$WORK/convsrc/a.img"
cp "$WORK/fixture.img" "$WORK/convsrc/sub/b.img"
# Pickers answer from this file instead of opening a dialog; the suite rewrites it per case.
: > "$WORK/picks.tsv"

/usr/libexec/at-spi-bus-launcher --launch-immediately &
sleep 1

HOME="$WORK/home" GTK_A11Y=atspi GDK_BACKEND=x11 GDK_DEBUG=no-portals NO_AT_BRIDGE=0 \
  RUSTY_BACKUP_TEST_PICKS="$WORK/picks.tsv" \
  "$BIN" "$WORK/fixture.img" > "$WORK/app.log" 2>&1 &
APP=$!

status=0
python3 -I "$(dirname "$0")/gtk4-gui-suite.py" "$WORK" "$@" || status=$?
if [ "$status" -ne 0 ]; then
  echo "--- app log ---"
  cat "$WORK/app.log"
fi
exit "$status"
