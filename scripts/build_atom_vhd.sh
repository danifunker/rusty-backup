#!/usr/bin/env bash
# Build a 100 MB FAT-formatted VHD for the MiSTer AcornAtom core and
# stuff every .atm file from the Atom Software Archive into it.
#
# Mirrors the convention from the MiSTer AcornAtom README:
#   "You need a boot.vhd file formatted as FAT and around 100MB.
#    Attach the boot.vhd and copy software onto it. Detach the VHD
#    and place the file in the games/AcornAtom folder on your SD card."
set -euo pipefail

SRC="${ATOM_ARCHIVE_SRC:-/tmp/atom_archive/staging}"
OUT="${OUT:-/tmp/atom_archive/AtomSoftwareArchive_V13.vhd}"
SIZE_MB="${SIZE_MB:-100}"

[ -d "$SRC" ] || { echo "missing source dir $SRC" >&2; exit 1; }
for t in mkfs.fat mcopy mmd; do
    command -v "$t" >/dev/null || { echo "missing $t" >&2; exit 1; }
done

# Create the VHD as a 100 MB raw file. The MiSTer AcornAtom core
# treats it as a flat FAT image — no Microsoft VHD footer needed (the
# blank.zip shipping with the core is also footer-less).
dd if=/dev/zero of="$OUT" bs=1M count="$SIZE_MB" status=none
mkfs.fat -F 16 -n "ATOMV13" "$OUT" >/dev/null

# Copy the archive tree in. Atom filenames are upper-case, often
# limited to 7 chars (Atom's original BBC-DOS / AtomMMC limit). mtools
# handles long names via VFAT, so the directory layout from the zip
# round-trips intact.
echo "Copying $(find "$SRC" -type f | wc -l) files into the VHD..."
mcopy -s -i "$OUT" "$SRC"/* ::
sync

echo
echo "=== VHD overview ==="
ls -la "$OUT"
echo
echo "Root entries:"
mdir -i "$OUT" :: | head -20
echo
echo "Total file count:"
mdir -s -i "$OUT" :: 2>/dev/null | grep -cE '^[A-Z0-9]' || true
