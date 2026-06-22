#!/bin/sh
# Run a cb-dos POC binary under DOSBox-X (macOS).
#
#   ./run-dosbox.sh                 # runs build/CRUSTYBK.EXE
#   ./run-dosbox.sh DISKSPK.EXE     # runs another POC binary
#
# Override the emulator path with: DOSBOXX=/path/to/dosbox-x ./run-dosbox.sh
set -e

DOSBOXX="${DOSBOXX:-/Applications/dosbox-x.app/Contents/MacOS/dosbox-x}"
HERE="$(cd "$(dirname "$0")" && pwd)"
BUILD="$HERE/build"
EXE="${1:-CRUSTYBK.EXE}"

if [ ! -x "$DOSBOXX" ]; then
    echo "dosbox-x not found at: $DOSBOXX" >&2
    echo "Set DOSBOXX=/path/to/dosbox-x and retry." >&2
    exit 1
fi
if [ ! -d "$BUILD" ]; then
    echo "No build/ dir. Run 'make' first." >&2
    exit 1
fi

# DOSBox-X provides a built-in DPMI host, so DJGPP exes run without CWSDPMI.
exec "$DOSBOXX" -fastlaunch \
    -c "MOUNT C \"$BUILD\"" \
    -c "C:" \
    -c "$EXE"
