#!/bin/sh
# Package the rusty-backup appliance as a PXE network-boot bundle:
#   bzImage + initrd + a sample pxelinux.cfg + a README, tarred up ready to
#   drop onto a TFTP / dnsmasq server. Good for a permanent diskless backup
#   *station* (overkill for a one-off — use the ISO for that).
#
# Inputs (from a finished Buildroot build): buildroot/output/{bzImage, and the
# initramfs — either buildroot/output/rootfs.cpio or /boot/initrd inside the
# ISO}. Output: buildroot/output/rusty-backup-appliance-pxe-<version>.tar.gz
#
# Usage:  sh buildroot/package-pxe.sh [VERSION]
set -e

HERE=$(CDPATH= cd "$(dirname "$0")" && pwd)
OUT="$HERE/output"
VER="${1:-${RELEASE_VERSION:-dev}}"
BZIMAGE="$OUT/bzImage"
ISO="$OUT/rusty-backup-appliance.iso"

[ -f "$BZIMAGE" ] || { echo "ERROR: $BZIMAGE not found — build the appliance first"; exit 1; }

# Resolve the initramfs: prefer a copied-out rootfs.cpio, else pull /boot/initrd
# out of the ISO (works with isoinfo, bsdtar, or xorriso — whichever is present).
INITRD="$OUT/initrd"
if [ -f "$OUT/rootfs.cpio" ]; then
    # Preferred path: build.sh copies the rootfs cpio out next to the ISO, so we
    # never need an ISO-extraction tool on the host.
    cp "$OUT/rootfs.cpio" "$INITRD"
elif [ -f "$ISO" ]; then
    # Fallback: dig /boot/initrd out of the ISO. Try every tool we know in turn
    # and accept the first that yields a non-empty file (tools/versions vary on
    # which ISO path spelling they accept), instead of trusting one blindly.
    rm -f "$INITRD"
    if command -v isoinfo >/dev/null 2>&1; then
        isoinfo -i "$ISO" -x '/boot/initrd.;1' > "$INITRD" 2>/dev/null || true
    fi
    if [ ! -s "$INITRD" ] && command -v bsdtar >/dev/null 2>&1; then
        bsdtar -xOf "$ISO" boot/initrd > "$INITRD" 2>/dev/null || true
    fi
    if [ ! -s "$INITRD" ] && command -v xorriso >/dev/null 2>&1; then
        xorriso -osirrox on -indev "$ISO" -extract /boot/initrd "$INITRD" >/dev/null 2>&1 || true
    fi
    if [ ! -s "$INITRD" ]; then
        echo "ERROR: could not extract /boot/initrd from $ISO (need isoinfo, bsdtar, or xorriso)."
        echo "       Easiest fix: rebuild so build.sh emits buildroot/output/rootfs.cpio."
        exit 1
    fi
else
    echo "ERROR: no rootfs.cpio and no ISO to source the initramfs from"; exit 1
fi
[ -s "$INITRD" ] || { echo "ERROR: initrd is empty"; exit 1; }

STAGE="$OUT/pxe-stage/rusty-backup-appliance-pxe-${VER}"
rm -rf "$OUT/pxe-stage"
mkdir -p "$STAGE/pxelinux.cfg"
cp "$BZIMAGE" "$STAGE/bzImage"
cp "$INITRD"  "$STAGE/initrd"

cat > "$STAGE/pxelinux.cfg/default" <<'CFG'
# rusty-backup appliance — PXELINUX boot menu.
# Serial console mirrored to COM1 so the station can be driven headless.
DEFAULT rusty-backup
PROMPT 0
TIMEOUT 30
SERIAL 0 115200
LABEL rusty-backup
    KERNEL bzImage
    # The whole rootfs is the initramfs (in RAM), so no root= is needed.
    APPEND initrd=initrd console=tty0 console=ttyS0,115200
CFG

cat > "$STAGE/README.txt" <<EOF
rusty-backup appliance — PXE network-boot bundle (version ${VER})
=================================================================

A diskless way to boot the rusty-backup backup/restore appliance over the
network. The whole appliance (kernel + rootfs-in-RAM) loads via TFTP; nothing
is installed on the target machine's disk.

Contents
--------
  bzImage              the appliance kernel
  initrd               the appliance rootfs as an initramfs (loaded into RAM)
  pxelinux.cfg/default a sample PXELINUX config pointing at the two files above

Quick setup (PXELINUX + dnsmasq)
--------------------------------
1. Put pxelinux.0 + ldlinux.c32 (from the 'syslinux' package) and the two
   files above into your TFTP root, keeping pxelinux.cfg/default in place.
2. Point dnsmasq at them, e.g.:

       dhcp-range=192.168.50.50,192.168.50.150,12h
       dhcp-boot=pxelinux.0
       enable-tftp
       tftp-root=/srv/tftp

3. Set the target's BIOS to network/PXE boot. It pulls the kernel + initrd and
   boots straight into the rb-cli backup/restore menu (on the VGA screen and on
   the serial console).

Notes
-----
* The rootfs is held in RAM, so the station needs RAM >= the initrd size plus
  working room (>= 128 MB is comfortable).
* Networking inside the appliance is still the deferred branch — today the
  *backup destination* is a 2nd disk or a USB stick on the target, even though
  the boot itself came over the network. Mount it on /mnt and use rb-cli as
  the boot banner describes.
* Same kernel/driver set as the ISO — see docs/appliance_hardware_support.md.
EOF

TARBALL="$OUT/rusty-backup-appliance-pxe-${VER}.tar.gz"
rm -f "$TARBALL"
( cd "$OUT/pxe-stage" && tar -czf "$TARBALL" "rusty-backup-appliance-pxe-${VER}" )
rm -rf "$OUT/pxe-stage" "$INITRD"

echo "PXE bundle: $TARBALL"
ls -lh "$TARBALL"
