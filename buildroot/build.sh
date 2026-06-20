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

cd "$BR"
export FORCE_UNSAFE_CONFIGURE=1
export DL_DIR="$WORK/buildroot/dl"     # cache downloads on the host across runs
mkdir -p "$DL_DIR"

# Start from the known-bootable qemu_x86 config, then layer our customisations.
make qemu_x86_defconfig
cat >> .config <<EOF
BR2_ROOTFS_OVERLAY="$OVERLAY"
BR2_TARGET_GENERIC_HOSTNAME="rusty-backup"
# Boot straight into the backup/restore menu, like an installer CD: the console
# getty runs /usr/bin/appliance-menu (which launches `rb-cli menu`, then drops to
# a shell on quit) instead of prompting for login.
BR2_TARGET_GENERIC_GETTY_OPTIONS="-n -l /usr/bin/appliance-menu"
BR2_LINUX_KERNEL_CONFIG_FRAGMENT_FILES="$WORK/buildroot/kernel.fragment"
# Destination-medium tooling (a 2nd disk / USB the backup is written to).
BR2_PACKAGE_DOSFSTOOLS=y
BR2_PACKAGE_E2FSPROGS=y
BR2_PACKAGE_UTIL_LINUX=y
BR2_PACKAGE_UTIL_LINUX_BINARIES=y
EOF
make olddefconfig
make -j"$(nproc)"

mkdir -p "$OUT"
cp -v output/images/bzImage output/images/rootfs.ext2 "$OUT/"
echo
echo "Appliance images written to buildroot/output/ :"
ls -lh "$OUT"
