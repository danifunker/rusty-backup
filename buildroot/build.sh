#!/bin/sh
# Build the rusty-backup Buildroot appliance image.
# Runs inside docker/buildroot-appliance.Dockerfile with the repo bind-mounted
# at /work. Produces buildroot/output/{bzImage,rootfs.ext2} on the host.
set -e

BR=/opt/buildroot
WORK=/work
OVERLAY="$WORK/buildroot/overlay"
OUT="$WORK/buildroot/output"

# CPU baseline for the appliance kernel + BusyBox userland. The qemu_x86
# defconfig uses Buildroot's *internal* toolchain (it sets no external one), so
# retargeting is just a config flip — the toolchain rebuilds gcc/glibc for the
# chosen arch. The static rb-cli baked in must match the floor:
#   i686 (default) -> Pentium Pro+; bundles the i586 static rb-cli (runs on i686)
#   i586           -> Pentium / 5x86 / Vortex86 (e.g. ITX-Llama) — the floor for
#                     the Linux appliance. Covers everything except a literal 486.
# A true 486 is NOT a Linux-appliance target: it ships as cb-dos (the DOS lane,
# tiny + BIOS int 13h) instead — see docs/cb_dos.md. (A 486 Linux rb-cli is an
# open toolchain item, docs/linux_486_build.md, but low-RAM 486s want cb-dos.)
#
# BR2_CPU sets the toolchain/userland arch; KCPU_FRAG sets the *kernel*'s
# processor family (the qemu_x86 kernel config pins M686=i686, so without this
# the kernel keeps emitting CMOV and faults on a real Pentium even when the
# userland is retargeted).
APPLIANCE_ARCH="${APPLIANCE_ARCH:-i686}"
case "$APPLIANCE_ARCH" in
    i686) RB_TRIPLE="i586-unknown-linux-musl"; BR2_CPU=""; KCPU_FRAG="" ;;   # defconfig default (M686)
    i586) RB_TRIPLE="i586-unknown-linux-musl"; BR2_CPU="BR2_x86_i586"; KCPU_FRAG="$WORK/buildroot/kernel-i586.fragment" ;;
    i486) echo "ERROR: a true 486 ships as cb-dos (the DOS lane), not a Linux appliance — see docs/cb_dos.md."; exit 1 ;;
    *) echo "ERROR: APPLIANCE_ARCH must be i686 or i586 (got '$APPLIANCE_ARCH'); a 486 uses cb-dos"; exit 1 ;;
esac
echo "Building the appliance for CPU floor: $APPLIANCE_ARCH (rb-cli: $RB_TRIPLE)"

# Static rb-cli (musl) — runs regardless of the appliance's own libc.
RBCLI="$WORK/target-cross/$RB_TRIPLE/release/rb-cli"
if [ ! -f "$RBCLI" ]; then
    echo "ERROR: static rb-cli for $APPLIANCE_ARCH not found at $RBCLI"
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
BR2_LINUX_KERNEL_CONFIG_FRAGMENT_FILES="$WORK/buildroot/kernel.fragment $KCPU_FRAG"
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

# Retarget the CPU floor when asked (i686 is the defconfig default, so no-op).
if [ -n "$BR2_CPU" ]; then
    cat >> .config <<EOF
# BR2_x86_pentiumpro is not set
$BR2_CPU=y
EOF
fi
make olddefconfig
make -j"$(nproc)"

mkdir -p "$OUT"
cp -v output/images/bzImage output/images/rootfs.ext2 "$OUT/"
# The rootfs as an initramfs cpio. This IS the ISO's /boot/initrd and the PXE
# bundle's initrd; copy it out so package-pxe.sh uses it directly instead of
# digging it back out of the ISO (which needs a working isoinfo/bsdtar/xorriso
# on the host and was the fragile part in CI).
if [ -f output/images/rootfs.cpio ]; then
    cp -v output/images/rootfs.cpio "$OUT/rootfs.cpio"
fi
# The bootable hybrid ISO (name it for humans).
if [ -f output/images/rootfs.iso9660 ]; then
    cp -v output/images/rootfs.iso9660 "$OUT/rusty-backup-appliance.iso"
fi
echo
echo "Appliance images written to buildroot/output/ :"
ls -lh "$OUT"
