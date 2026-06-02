#!/usr/bin/env bash
# Generate the JFS2 fixture for docs/need_fixtures.md.
#
# Runs inside WSL Ubuntu (or any Linux box). Setup the once-only host bits
# before the first run:
#   sudo apt install -y jfsutils libguestfs-tools \
#                       linux-image-virtual \
#                       linux-modules-extra-$(uname -r)
#   sudo dpkg-statoverride --update --add root root 0644 /boot/vmlinuz-*
#
# The WSL kernel may not have the jfs module loaded; we mount via
# libguestfs's QEMU appliance (which uses the host's /boot/vmlinuz +
# /lib/modules/*).
#
# Why JFS2 only:
#   AIX JFS1 (the older sibling) uses a different on-disk format-id and is
#   not relevant on Linux — the kernel only ever shipped JFS2 (`s_version
#   = 2`). The parser rejects v1 with a clear error.
#
# Output:
#   tests/fixtures/test_jfs.img.zst
#
# Layout per docs/need_fixtures.md (mirrors the ext / reiserfs / ufs
# fixtures):
#   /hello.txt          -> "Hello, JFS!"
#   /subdir/nested.txt  -> "nested file"
#   /link.txt           -> symlink to hello.txt
#   /large.bin          -> 24 KiB deterministic data (exercises xtree leaf)
#   /tiny.txt           -> 10 bytes

set -euo pipefail

WORK=$(mktemp -d /tmp/jfs_fixtures.XXXXXX)
trap 'rm -rf "$WORK"' EXIT
cd "$WORK"

# Project root: from scripts/, go up one to the repo, mapping a WSL path.
FIXTURES_DIR=/mnt/c/Temp/mistercore/rusty-backup/tests/fixtures

# Allow guestfish to use the kernel without permission complaints.
export LIBGUESTFS_BACKEND=direct

build_one() {
    local raw=$1

    echo "=== building jfs2 ==="

    # 16 MiB is the JFS minimum (any smaller and mkfs.jfs refuses).
    dd if=/dev/zero of="$raw" bs=1M count=16 status=none
    # `-q` quiet, `-f` force (overwrite existing FS), `-L` label.
    mkfs.jfs -q -L jfs_test "$raw"

    # Populate via guestfish (its appliance kernel has jfs).
    guestfish --rw -a "$raw" <<'EOF'
run
mount /dev/sda /
write /hello.txt "Hello, JFS!"
mkdir /subdir
write /subdir/nested.txt "nested file"
ln-s hello.txt /link.txt
write /tiny.txt "tiny bytes"
EOF

    # Write a 24 KiB "large" file separately (binary content -> heredoc
    # would mangle bytes). guestfish 'upload' takes a host path.
    python3 -c "
import os
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

    # Quick byte-level probe so the fixture-driven tests can pin
    # absolute offsets without guessing.
    echo "--- aggregate superblock @ byte 32768 ---"
    xxd -s 32768 -l 64 "$raw" | head -4

    # Compress and copy to the project fixtures directory.
    zstd -q -f -19 "$raw" -o "${raw}.zst"
    cp "${raw}.zst" "$FIXTURES_DIR/"
    echo "wrote $FIXTURES_DIR/${raw}.zst ($(stat -c %s ${raw}.zst) bytes)"
}

build_one test_jfs.img

ls -la "$FIXTURES_DIR"/test_jfs.img.zst
