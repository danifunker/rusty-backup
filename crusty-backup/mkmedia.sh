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
# Stage a \NET directory for the CD (DOS networking via mTCP + your packet
# driver), if mTCP has been fetched — `sh crusty-backup/net/fetch-mtcp.sh`. The
# boot floppy is too small for it, so networking rides on the CD only. DOS text
# files get CRLF line endings. See docs/cb_dos_networking.md.
NETDIR="$HERE/net"
NETSTAGE="$DIST/.netstage"
rm -rf "$NETSTAGE"; mkdir -p "$NETSTAGE"
if ls "$NETDIR"/mtcp/*.EXE >/dev/null 2>&1; then
    mkdir -p "$NETSTAGE/DRIVERS"
    cp "$NETDIR"/mtcp/*.EXE "$NETSTAGE/"
    [ -f "$NETDIR/mtcp/COPYING.TXT" ] && cp "$NETDIR/mtcp/COPYING.TXT" "$NETSTAGE/"
    for c in "$NETDIR"/drivers/*.COM; do [ -f "$c" ] && cp "$c" "$NETSTAGE/DRIVERS/"; done
    sed 's/$/\r/' "$NETDIR/drivers/DRIVERS.TXT" > "$NETSTAGE/DRIVERS/DRIVERS.TXT"
    sed 's/$/\r/' "$NETDIR/MTCP.CFG"            > "$NETSTAGE/MTCP.CFG"
    sed 's/$/\r/' "$NETDIR/NET.BAT"             > "$NETSTAGE/NET.BAT"
    drv=$(ls "$NETDIR"/drivers/*.COM 2>/dev/null | wc -l | tr -d ' ')
    echo "Including \\NET on the CD: mTCP + ${drv} packet driver(s)"
else
    echo "No mTCP in net/mtcp/ — CD has no \\NET (run net/fetch-mtcp.sh to add DOS networking)."
fi

# genisoimage wrote inside the container; redo the ISO with the dist mount bound
# (and the staged \NET dir, when present).
docker run --rm -v "$DIST":/out -v "$NETSTAGE":/netstage:ro debian:bookworm-slim sh -c '
    apt-get update >/dev/null 2>&1; apt-get install -y genisoimage >/dev/null 2>&1
    mkdir -p /isoroot/boot && cp /out/cbdos-freedos.img /isoroot/boot/cbdos.img
    [ -n "$(ls -A /netstage 2>/dev/null)" ] && cp -r /netstage /isoroot/NET
    genisoimage -quiet -o /out/cbdos.iso -V CBDOS -b boot/cbdos.img -c boot/boot.cat /isoroot
'
rm -rf "$NETSTAGE"

echo "cb-dos media written to crusty-backup/dist/ :"
ls -lh "$DIST"/cbdos-freedos.img "$DIST"/cbdos.iso
