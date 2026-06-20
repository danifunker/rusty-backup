#!/bin/sh
# Build the cb-dos deployable media from a stock FreeDOS boot floppy + the cb-dos
# .exe's: a bootable FreeDOS floppy (cb-dos tools + banner) and an El-Torito CD
# ISO wrapping it. Outputs land in crusty-backup/dist/ (gitignored).
#
# Needs the cb-dos tools built first (crusty-backup/build/*.exe — `make` here, or
# docker/cb-dos.Dockerfile), and a FreeDOS 1.4 boot floppy as the base:
#   FDBASE=/path/to/x86BOOT.img ./mkmedia.sh
# (FreeDOS 1.4 "FloppyEdition" 144m/x86BOOT.img; for CI, fetch it from the
# FreeDOS release. It's GPL/redistributable but not vendored here.)
#
# mtools + genisoimage run in a throwaway Debian container, so the host needs
# only Docker.
set -e

HERE="$(cd "$(dirname "$0")" && pwd)"
FDBASE="${FDBASE:?set FDBASE=/path/to/FreeDOS x86BOOT.img}"
BUILD="$HERE/build"
DIST="$HERE/dist"
AUTOEXEC="$HERE/media/cbdos-autoexec.bat"

for e in tui_poc disk_spike lfn_test; do
    [ -f "$BUILD/$e.exe" ] || { echo "missing $BUILD/$e.exe (run make / cb-dos.Dockerfile first)"; exit 1; }
done

mkdir -p "$DIST"
cp "$FDBASE" "$DIST/cbdos-freedos.img"

docker run --rm \
    -v "$DIST/cbdos-freedos.img":/fd.img \
    -v "$BUILD":/exe:ro \
    -v "$AUTOEXEC":/in/fdauto.bat:ro \
    debian:bookworm-slim sh -c '
        apt-get update >/dev/null 2>&1
        apt-get install -y mtools genisoimage >/dev/null 2>&1
        export MTOOLS_SKIP_CHECK=1
        sed "s/\$/\r/" /in/fdauto.bat > /tmp/fdauto.bat   # CRLF for DOS
        mcopy -o -i /fd.img /tmp/fdauto.bat     ::FDAUTO.BAT
        mcopy -o -i /fd.img /exe/tui_poc.exe    ::TUI_POC.EXE
        mcopy -o -i /fd.img /exe/disk_spike.exe ::DISKSPK.EXE
        mcopy -o -i /fd.img /exe/lfn_test.exe   ::LFNTEST.EXE
        # El-Torito CD that boots the same floppy (floppy emulation).
        mkdir -p /isoroot/boot && cp /fd.img /isoroot/boot/cbdos.img
        genisoimage -quiet -o /cbdos.iso -V CBDOS -b boot/cbdos.img -c boot/boot.cat /isoroot
        cp /cbdos.iso /fd.img.iso 2>/dev/null || true
    '
# genisoimage wrote inside the container; redo the ISO with the dist mount bound.
docker run --rm -v "$DIST":/out debian:bookworm-slim sh -c '
    apt-get update >/dev/null 2>&1; apt-get install -y genisoimage >/dev/null 2>&1
    mkdir -p /isoroot/boot && cp /out/cbdos-freedos.img /isoroot/boot/cbdos.img
    genisoimage -quiet -o /out/cbdos.iso -V CBDOS -b boot/cbdos.img -c boot/boot.cat /isoroot
'

echo "cb-dos media written to crusty-backup/dist/ :"
ls -lh "$DIST"/cbdos-freedos.img "$DIST"/cbdos.iso
