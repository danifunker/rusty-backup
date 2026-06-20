#!/bin/sh
# Build a 1.44 MB GRUB boot floppy that loads the rusty-backup appliance off the
# CD (or USB). For ancient 486-class BIOSes that predate El Torito and can't
# boot a CD directly: GRUB fits on a floppy and can read ISO9660, so the floppy
# boots, finds /boot/bzImage on the CD, and chainloads the appliance from there.
#
# Needs GRUB's i386-pc tools (Debian/Ubuntu: `grub-pc-bin`) — grub-mkimage plus
# the i386-pc module/boot.img dir. Run it on Linux (CI) or inside a container:
#   docker run --rm -v "$PWD":/work debian:bookworm sh -c \
#     'apt-get update -qq && apt-get install -y -qq grub-pc-bin xorriso >/dev/null && sh /work/buildroot/make-grub-floppy.sh'
#
# Output: buildroot/output/rusty-backup-grub-floppy.img
set -e

HERE=$(CDPATH= cd "$(dirname "$0")" && pwd)
OUT="$HERE/output"
FLOPPY="$OUT/rusty-backup-grub-floppy.img"

command -v grub-mkimage >/dev/null 2>&1 || { echo "ERROR: grub-mkimage not found (install grub-pc-bin)"; exit 1; }

# Find the i386-pc support dir (boot.img + modules).
GRUBLIB=""
for d in /usr/lib/grub/i386-pc /usr/lib/grub2/i386-pc /lib/grub/i386-pc; do
    [ -f "$d/boot.img" ] && GRUBLIB="$d" && break
done
[ -n "$GRUBLIB" ] || { echo "ERROR: GRUB i386-pc dir (boot.img) not found"; exit 1; }

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# The embedded config runs at GRUB startup, before "normal" mode — so it uses
# plain commands (no `menuentry { }` blocks, which need the normal module). It
# searches every drive except the floppy for the appliance kernel, then boots it
# from there, so it works whether the appliance is on a CD or a USB stick. No
# filesystem is needed on the floppy itself.
cat > "$WORK/grub-early.cfg" <<'CFG'
serial --unit=0 --speed=115200
terminal_input console serial
terminal_output console serial
echo "rusty-backup GRUB floppy -- drives GRUB can see:"
ls
echo "Searching for the appliance (/boot/bzImage) on CD / USB ..."
search --no-floppy --file --set=root /boot/bzImage
echo "Booting the rusty-backup appliance from (${root}) ..."
linux /boot/bzImage root=/dev/sr0 console=tty0 console=ttyS0,115200
initrd /boot/initrd
boot
echo "Boot failed -- dropping to the GRUB prompt. '/boot/bzImage' was not found"
echo "on any drive above; check that the appliance CD/USB is attached."
CFG

# Core image: boot.img (sector 0) chainloads core.img (sector 1+). The modules
# are what GRUB needs to read the CD/USB and boot Linux. Crucially this includes
# GRUB's *native* ATA (`ata`/`ahci`) and USB (`usbms`) drivers, not just
# `biosdisk`: a pre-El-Torito BIOS won't expose the CD-ROM over int 13h, so GRUB
# has to drive the IDE/SATA/USB controller itself to read the appliance media.
grub-mkimage -O i386-pc -c "$WORK/grub-early.cfg" -o "$WORK/core.img" -p "(fd0)/boot/grub" \
    biosdisk ata ahci pata usb uhci ohci ehci usbms \
    iso9660 part_msdos part_gpt fat ext2 configfile linux normal \
    search search_fs_file echo cat test serial terminal halt reboot ls

mkdir -p "$OUT"
# A blank 1.44 MB floppy, then boot.img + core.img laid down at its start.
dd if=/dev/zero of="$FLOPPY" bs=512 count=2880 status=none
dd if="$GRUBLIB/boot.img" of="$FLOPPY" bs=512 count=1 conv=notrunc status=none
dd if="$WORK/core.img"    of="$FLOPPY" bs=512 seek=1 conv=notrunc status=none

CORE_SECTORS=$(( ($(wc -c < "$WORK/core.img") + 511) / 512 ))
echo "GRUB floppy: $FLOPPY   (boot.img + ${CORE_SECTORS}-sector core.img on a 2880-sector floppy)"
ls -lh "$FLOPPY"
