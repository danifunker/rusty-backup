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
DOSLFN="$HERE/media/DOSLFN.COM"     # long-filename TSR (vendored; see media/DOSLFN-ATTRIBUTION.md)
WATTCFG="$HERE/net/WATTCP.CFG"      # WATT-32 config for crustybk's networked backup
NETDRV="$HERE/net/drivers"          # user packet-driver .COM(s) for the floppy's \NET\DRIVERS

# Only CRUSTYBK ships on the media now (the disk_spike / lfn_test POCs are dev
# diagnostics — see the floppy staging below). It links WATT-32 + zlib + lz4 and
# is the one that must exist.
[ -f "$BUILD/crustybk.exe" ] || { echo "missing $BUILD/crustybk.exe (run make / cb-dos.Dockerfile first)"; exit 1; }
[ -f "$DOSLFN" ]    || { echo "missing $DOSLFN (vendored long-filename TSR)"; exit 1; }
[ -f "$WATTCFG" ]   || { echo "missing $WATTCFG (WATT-32 config for networked backup)"; exit 1; }

mkdir -p "$DIST"
cp "$FDBASE" "$DIST/cbdos-freedos.img"

# DPMI host. The cb-dos tools are DJGPP (32-bit protected mode) and need a DPMI
# provider at runtime. DOSBox-X has one built in, but a real FreeDOS boot disk
# does not — without this they fail with "Load error: no DPMI". CWSDPMI is
# freely redistributable, and the DJGPP stub auto-loads CWSDPMI.EXE from the
# program's own directory (A:\), so just shipping it on the floppy is enough.
DL="$HERE/dl"
CSDPMI_URL="${CSDPMI_URL:-http://www.delorie.com/pub/djgpp/current/v2misc/csdpmi7b.zip}"
mkdir -p "$DL"
if [ ! -f "$DL/CWSDPMI.EXE" ]; then
    curl -fsSL -o "$DL/csdpmi7b.zip" "$CSDPMI_URL"
    ( cd "$DL" && unzip -o -j -q csdpmi7b.zip bin/CWSDPMI.EXE )
fi

# UPX (Ultimate Packer for eXecutables). CRUSTYBK links the WATT-32 TCP/IP stack
# and is ~1 MB -- too big for a 1.44 MB floppy alongside FreeDOS -- so we pack it
# (it self-extracts into RAM at launch). UPX is not in Debian's apt repos, so
# grab the official static build matching the mtools container's arch (no
# --platform on that container, so it runs at the host arch).
UPX_VER="${UPX_VER:-4.2.4}"
case "$(uname -m)" in
    x86_64|amd64)  UPX_ARCH=amd64 ;;
    arm64|aarch64) UPX_ARCH=arm64 ;;
    *) echo "unsupported arch for UPX: $(uname -m)"; exit 1 ;;
esac
if [ ! -x "$DL/upx" ]; then
    curl -fsSL -o "$DL/upx.tar.xz" \
        "https://github.com/upx/upx/releases/download/v${UPX_VER}/upx-${UPX_VER}-${UPX_ARCH}_linux.tar.xz"
    tar -xf "$DL/upx.tar.xz" -C "$DL" "upx-${UPX_VER}-${UPX_ARCH}_linux/upx"
    mv "$DL/upx-${UPX_VER}-${UPX_ARCH}_linux/upx" "$DL/upx"
    rmdir "$DL/upx-${UPX_VER}-${UPX_ARCH}_linux" 2>/dev/null || true
    chmod +x "$DL/upx"
fi

docker run --rm \
    -v "$DIST/cbdos-freedos.img":/fd.img \
    -v "$BUILD":/exe:ro \
    -v "$DL/CWSDPMI.EXE":/dpmi/CWSDPMI.EXE:ro \
    -v "$DOSLFN":/dpmi/DOSLFN.COM:ro \
    -v "$AUTOEXEC":/in/fdauto.bat:ro \
    -v "$WATTCFG":/in/WATTCP.CFG:ro \
    -v "$NETDRV":/in/drivers:ro \
    -v "$DL/upx":/usr/local/bin/upx:ro \
    debian:bookworm-slim sh -c '
        set -e
        apt-get update >/dev/null 2>&1
        apt-get install -y mtools genisoimage >/dev/null 2>&1
        export MTOOLS_SKIP_CHECK=1
        sed "s/\$/\r/" /in/fdauto.bat > /tmp/fdauto.bat   # CRLF for DOS

        # CRUSTYBK statically links the WATT-32 TCP/IP stack (networked backup)
        # and weighs ~1 MB -- too big for a 1.44 MB floppy alongside FreeDOS, so
        # mcopy used to silently fail "Disk full" and ship a floppy with no
        # backup tool. UPX packs it to ~600 KB; the stub unpacks it into RAM at
        # launch, so networking is fully preserved. --best (NRV) decompresses far
        # faster than --lzma on the 486-class CPUs cb-dos targets.
        cp /exe/crustybk.exe /tmp/CRUSTYBK.EXE
        upx --best -q /tmp/CRUSTYBK.EXE >/dev/null

        mcopy -o -i /fd.img /tmp/fdauto.bat     ::FDAUTO.BAT
        mcopy -o -i /fd.img /dpmi/CWSDPMI.EXE   ::CWSDPMI.EXE
        mcopy -o -i /fd.img /dpmi/DOSLFN.COM    ::DOSLFN.COM
        mcopy -o -i /fd.img /tmp/CRUSTYBK.EXE   ::CRUSTYBK.EXE

        # Networking for "backup rb://...": WATT-32 reads WATTCP.CFG (DHCP by
        # default) from the root next to CRUSTYBK.EXE; \NET\DRIVERS holds the
        # packet driver(s) you drop into net/drivers/ for your NIC.
        sed "s/\$/\r/" /in/WATTCP.CFG > /tmp/WATTCP.CFG
        mcopy -o -i /fd.img /tmp/WATTCP.CFG     ::WATTCP.CFG
        mmd -i /fd.img ::NET         2>/dev/null || true
        mmd -i /fd.img ::NET/DRIVERS 2>/dev/null || true
        sed "s/\$/\r/" /in/drivers/DRIVERS.TXT > /tmp/DRIVERS.TXT
        mcopy -o -i /fd.img /tmp/DRIVERS.TXT    ::NET/DRIVERS/DRIVERS.TXT
        # The full Crynwr set (~600 KB) only fits the CD; the floppy carries a
        # curated set covering the common retro NICs, the cards the big
        # emulators (QEMU / VirtualBox / 86Box / DOSBox-X) present, and the
        # ITX-Llama onboard Vortex86/RDC R6040 (r6040pd). The CD has all of
        # them under \NET\DRIVERS.
        for d in ne2000 ne1000 pcntpk rtspkt 3c509 3c503 smc_wd e100bpkt r6040pd 3c90xpd; do
            [ -f "/in/drivers/$d.com" ] && mcopy -o -i /fd.img "/in/drivers/$d.com" ::NET/DRIVERS/ || true
        done

        # Drop the FreeDOS installer (irrelevant to cb-dos, frees ~39 KB). The
        # disk_spike / lfn_test POC diagnostics are intentionally NOT shipped --
        # the room goes to CRUSTYBK + networking.
        mdel -i /fd.img ::SETUP.BAT 2>/dev/null || true

        # Guard against the silent-disk-full regression: mtools mcopy can print
        # "Disk full" yet still exit 0, so set -e alone is not enough -- assert
        # the tool actually landed.
        mdir -i /fd.img ::CRUSTYBK.EXE >/dev/null 2>&1 || {
            echo "ERROR: CRUSTYBK.EXE is not on the floppy (out of space?). Aborting." >&2
            exit 1
        }

        # El-Torito CD that boots the same floppy (floppy emulation).
        mkdir -p /isoroot/boot && cp /fd.img /isoroot/boot/cbdos.img
        genisoimage -quiet -o /cbdos.iso -V CBDOS -b boot/cbdos.img -c boot/boot.cat /isoroot
        cp /cbdos.iso /fd.img.iso 2>/dev/null || true
    '
# Stage the CD's \NET directory. Unlike the floppy (which only has room for a
# handful of common drivers), the CD carries the FULL packet-driver set plus the
# WATT-32 config, and — when fetched — the mTCP FTP suite. DOS text files get
# CRLF line endings. See docs/cb_dos_networking.md.
NETDIR="$HERE/net"
NETSTAGE="$DIST/.netstage"
rm -rf "$NETSTAGE"; mkdir -p "$NETSTAGE/DRIVERS"
# Full packet-driver collection (the floppy only gets the curated few above).
for c in "$NETDIR"/drivers/*.com "$NETDIR"/drivers/*.COM; do [ -f "$c" ] && cp "$c" "$NETSTAGE/DRIVERS/"; done
sed 's/$/\r/' "$NETDIR/drivers/DRIVERS.TXT" > "$NETSTAGE/DRIVERS/DRIVERS.TXT"
[ -f "$NETDIR/drivers/CRYNWR-GPL.txt" ] && sed 's/$/\r/' "$NETDIR/drivers/CRYNWR-GPL.txt" > "$NETSTAGE/DRIVERS/CRYNWR-GPL.txt"
# Third-party driver licensing/attribution (Crynwr GPL + the 3Com proprietary
# 3C90x driver) travels with the binaries.
[ -f "$NETDIR/drivers/ATTRIBUTION.md" ] && sed 's/$/\r/' "$NETDIR/drivers/ATTRIBUTION.md" > "$NETSTAGE/DRIVERS/ATTRIB.TXT"
# WATT-32 config for crustybk's networked backup, alongside the drivers.
sed 's/$/\r/' "$WATTCFG" > "$NETSTAGE/WATTCP.CFG"
drv=$(ls "$NETDIR"/drivers/*.com "$NETDIR"/drivers/*.COM 2>/dev/null | wc -l | tr -d ' ')
# Optional mTCP FTP suite (move a finished backup file off the box over FTP).
if ls "$NETDIR"/mtcp/*.EXE >/dev/null 2>&1; then
    cp "$NETDIR"/mtcp/*.EXE "$NETSTAGE/"
    [ -f "$NETDIR/mtcp/COPYING.TXT" ] && cp "$NETDIR/mtcp/COPYING.TXT" "$NETSTAGE/"
    sed 's/$/\r/' "$NETDIR/MTCP.CFG" > "$NETSTAGE/MTCP.CFG"
    sed 's/$/\r/' "$NETDIR/NET.BAT"  > "$NETSTAGE/NET.BAT"
    echo "Including \\NET on the CD: ${drv} packet driver(s) + mTCP FTP suite"
else
    echo "Including \\NET on the CD: ${drv} packet driver(s) (no mTCP — run net/fetch-mtcp.sh for the FTP suite)"
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
