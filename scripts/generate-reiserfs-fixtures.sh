#!/usr/bin/env bash
# Generate the ReiserFS v3.6 fixture for docs/need_fixtures.md.
#
# Runs inside WSL Ubuntu (or any Linux box). Setup the once-only host bits
# before the first run:
#   sudo apt install -y reiserfsprogs libguestfs-tools \
#                       linux-image-virtual \
#                       linux-modules-extra-$(uname -r)
#   sudo dpkg-statoverride --update --add root root 0644 /boot/vmlinuz-*
#
# The WSL kernel has no reiserfs module, so we mount via libguestfs's QEMU
# appliance (which uses the host's /boot/vmlinuz + /lib/modules/*).
#
# Why only v3.6:
#   Modern Linux kernels refuse to mount v3.5 ReiserFS volumes ("Structure
#   needs cleaning" — the kernel rejects the legacy on-disk version, and
#   `mount -o conv` no longer auto-upgrades). The R.1/R.2/R.3a unit tests
#   in src/fs/reiserfs.rs already cover v3.5 superblock + key format
#   end-to-end against synthetic bytes (both V1 sentinel-u32 and V2 packed
#   layouts), which is sufficient for the math the parser does. Real-image
#   v3.5 validation would require an older kernel (~5.x) and is parked.
#
# Output:
#   tests/fixtures/test_reiserfs_v3_6.img.zst
#
# Layout per docs/need_fixtures.md (mirrors the ext2/ext4 fixtures so the
# test code can share patterns):
#   /hello.txt          -> "Hello, ReiserFS!"
#   /subdir/nested.txt  -> "nested file"
#   /link.txt           -> symlink to hello.txt
#   /large.bin          -> 24 KiB deterministic data (forces indirect items)
#   /tiny.txt           -> 10 bytes (forces tail-packing / direct item)

set -euo pipefail

WORK=$(mktemp -d /tmp/reiserfs_fixtures.XXXXXX)
trap 'rm -rf "$WORK"' EXIT
cd "$WORK"

# Project root: from scripts/, go up one to the repo, mapping a WSL path.
FIXTURES_DIR=/mnt/c/Temp/mistercore/rusty-backup/tests/fixtures

# Allow guestfish to use the kernel without permission complaints.
export LIBGUESTFS_BACKEND=direct

build_one() {
    local ver=$1       # 3.5 or 3.6
    local label=$2
    local raw=$3       # raw image filename (no compression)

    echo "=== building reiserfs $ver ($label) ==="

    # 64 MiB blank file, then format on the host (mkfs.reiserfs runs in
    # userspace and doesn't need the kernel module).
    dd if=/dev/zero of="$raw" bs=1M count=64 status=none
    mkfs.reiserfs -f -f -q --format "$ver" -l "$label" "$raw"

    # Populate via guestfish (its appliance kernel has reiserfs).
    guestfish --rw -a "$raw" <<'EOF'
run
mount /dev/sda /
write /hello.txt "Hello, ReiserFS!"
mkdir /subdir
write /subdir/nested.txt "nested file"
ln-s hello.txt /link.txt
write /tiny.txt "tiny bytes"
EOF

    # Write a 24 KiB "large" file separately (binary content -> heredoc
    # would mangle bytes). guestfish 'upload' takes a host path.
    python3 -c "
import os, struct
data = bytearray()
for i in range(24 * 1024):
    data.append((i * 37 + 11) & 0xFF)
open('$WORK/large.bin', 'wb').write(data)
"
    guestfish --rw -a "$raw" <<EOF
run
mount /dev/sda /
upload $WORK/large.bin /large.bin
EOF

    # Sanity check: list and stat the entries we just wrote.
    guestfish --ro -a "$raw" <<'EOF'
run
mount /dev/sda /
ll /
ls /subdir
EOF

    # Compress and copy to the project fixtures directory.
    zstd -q -f -19 "$raw" -o "${raw}.zst"
    cp "${raw}.zst" "$FIXTURES_DIR/"
    echo "wrote $FIXTURES_DIR/${raw}.zst ($(stat -c %s ${raw}.zst) bytes)"
}

build_one 3.6 reiser36_test test_reiserfs_v3_6.img

ls -la "$FIXTURES_DIR"/test_reiserfs_*.img.zst
