#!/bin/sh
# Build the rusty-backup Buildroot appliance image.
# Runs inside docker/buildroot-appliance.Dockerfile with the repo bind-mounted
# at /work. Produces buildroot/output/{bzImage,rootfs.ext2} on the host.
set -e

BR=/opt/buildroot
WORK=/work
OVERLAY="$WORK/buildroot/overlay"
# Static rb-cli (musl) — runs regardless of the appliance's own libc.
RBCLI="$WORK/target-cross/i586-unknown-linux-musl/release/rb-cli"
OUT="$WORK/buildroot/output"

if [ ! -f "$RBCLI" ]; then
    echo "ERROR: static rb-cli not found at $RBCLI"
    echo "Build it first:  docker run --rm -v \"\$PWD\":/src rb-cross-i586-musl"
    exit 1
fi

# Stage the binary into the rootfs overlay.
install -D -m755 "$RBCLI" "$OVERLAY/usr/bin/rb-cli"
chmod +x "$OVERLAY/etc/init.d/S99appliance" "$OVERLAY/usr/bin/appliance-menu"
chmod 644 "$OVERLAY/etc/inittab"

cd "$BR"
export FORCE_UNSAFE_CONFIGURE=1
export DL_DIR="$WORK/buildroot/dl"     # cache downloads on the host across runs
mkdir -p "$DL_DIR"

# Start from the known-bootable qemu_x86 config, then layer our customisations.
make qemu_x86_defconfig
cat >> .config <<EOF
BR2_ROOTFS_OVERLAY="$OVERLAY"
BR2_TARGET_GENERIC_HOSTNAME="rusty-backup"
# Boot straight into the backup/restore menu, like an installer CD. The
# authoritative getty setup is the overlay's /etc/inittab, which runs
# /usr/bin/appliance-menu on BOTH the VGA console (tty1) and the serial port
# (ttyS0) — Buildroot copies the overlay after its own inittab fixups, so the
# overlay wins. This GETTY_OPTIONS line is a harmless fallback in case the
# overlay inittab is ever removed.
BR2_TARGET_GENERIC_GETTY_OPTIONS="-n -l /usr/bin/appliance-menu"
# Networking is deferred to a later branch; drop the auto-eth0 DHCP client so
# the boot doesn't stall ~15 s in wait_iface looking for a NIC that isn't
# configured yet (that delay made early boots look hung). Re-enable when the
# network branch lands.
BR2_SYSTEM_DHCP=""
BR2_LINUX_KERNEL_CONFIG_FRAGMENT_FILES="$WORK/buildroot/kernel.fragment"
# Destination-medium tooling (a 2nd disk / USB the backup is written to).
BR2_PACKAGE_DOSFSTOOLS=y
BR2_PACKAGE_E2FSPROGS=y
BR2_PACKAGE_UTIL_LINUX=y
BR2_PACKAGE_UTIL_LINUX_BINARIES=y
# Bootable image: a hybrid ISO9660 (isolinux) — one file boots from a CD-ROM
# *and* from a USB stick / CF card (dd it to the device). Covers both the CD and
# the .img deployables with a single artifact. Our isolinux.cfg adds a serial
# console (see buildroot/isolinux.cfg) so the boot is visible/drivable headless.
BR2_TARGET_SYSLINUX=y
BR2_TARGET_SYSLINUX_ISOLINUX=y
BR2_TARGET_ROOTFS_ISO9660=y
BR2_TARGET_ROOTFS_ISO9660_ISOLINUX=y
BR2_TARGET_ROOTFS_ISO9660_BOOT_MENU="$WORK/buildroot/isolinux.cfg"
BR2_TARGET_ROOTFS_ISO9660_HYBRID=y
EOF
make olddefconfig
make -j"$(nproc)"

mkdir -p "$OUT"
cp -v output/images/bzImage output/images/rootfs.ext2 "$OUT/"
# The bootable hybrid ISO (name it for humans).
if [ -f output/images/rootfs.iso9660 ]; then
    cp -v output/images/rootfs.iso9660 "$OUT/rusty-backup-appliance.iso"
fi
echo
echo "Appliance images written to buildroot/output/ :"
ls -lh "$OUT"
