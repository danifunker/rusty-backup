#!/usr/bin/env bash
# Generate the UFS1 + UFS2 fixtures for docs/need_fixtures.md.
#
# Runs inside WSL Ubuntu (or any Linux box). Setup the once-only host bits
# before the first run:
#   sudo apt install -y makefs zstd
#
# `makefs` is the NetBSD tool that builds an FFS/UFS image straight from a
# host directory tree — no kernel-side UFS write support, no mounting, no
# QEMU appliance. Lives in Ubuntu universe (24.04+).
#
# Why makefs and not mkfs.ufs / newfs:
#   `mkfs.ufs` / `newfs` ship with FreeBSD; neither is packaged for Ubuntu.
#   `ufstool` (sergev's port) would have to be built from source. makefs
#   does the job in one userspace pass.
#
# Output:
#   tests/fixtures/test_ufs1.img.zst   (UFS1: superblock at byte 8192,
#                                       magic 0x011954 at offset 1372)
#   tests/fixtures/test_ufs2.img.zst   (UFS2: superblock at byte 65536,
#                                       magic 0x19540119 at offset 1372)
#
# Layout per docs/need_fixtures.md (mirrors the reiserfs + ext fixtures):
#   /hello.txt          -> "Hello, UFS!"
#   /subdir/nested.txt  -> "nested file"
#   /link.txt           -> symlink to hello.txt
#   /large.bin          -> 24 KiB deterministic (forces indirect-block walk)
#   /tiny.txt           -> 10 bytes

set -euo pipefail

WORK=$(mktemp -d /tmp/ufs_fixtures.XXXXXX)
trap 'rm -rf "$WORK"' EXIT
cd "$WORK"

FIXTURES_DIR=/mnt/c/Temp/mistercore/rusty-backup/tests/fixtures

build_source_tree() {
    local src=$1
    mkdir -p "$src"
    printf 'Hello, UFS!' > "$src/hello.txt"
    mkdir -p "$src/subdir"
    printf 'nested file' > "$src/subdir/nested.txt"
    ln -sf hello.txt "$src/link.txt"
    printf 'tiny bytes' > "$src/tiny.txt"
    python3 -c "
import sys
data = bytearray()
for i in range(24 * 1024):
    data.append((i * 37 + 11) & 0xFF)
open('$src/large.bin', 'wb').write(data)
"
}

build_one() {
    local ver=$1       # 1 or 2
    local raw=$2       # raw image filename

    echo "=== building ufs$ver ==="
    local src="$WORK/src_ufs$ver"
    build_source_tree "$src"

    # 16 MiB image, little-endian (amd64-compatible UFS).
    # `-s 16m` sets the image size; makefs rounds up to a multiple of bsize.
    # `-o version=$ver` picks UFS1 (default) or UFS2.
    makefs -t ffs -B le -s 16m -o "version=$ver" "$raw" "$src"

    # Quick sanity: confirm the magic landed at the documented offset.
    if [ "$ver" = "1" ]; then
        # UFS1 superblock at byte 8192, magic at +1372 = absolute byte 9564.
        magic=$(dd if="$raw" bs=1 skip=9564 count=4 status=none | xxd -p)
        echo "ufs1 magic at byte 9564: $magic (expected: 54190100)"
    else
        # UFS2 superblock at byte 65536, magic at +1372 = absolute byte 66908.
        magic=$(dd if="$raw" bs=1 skip=66908 count=4 status=none | xxd -p)
        echo "ufs2 magic at byte 66908: $magic (expected: 19015419)"
    fi

    zstd -q -f -19 "$raw" -o "${raw}.zst"
    cp "${raw}.zst" "$FIXTURES_DIR/"
    echo "wrote $FIXTURES_DIR/${raw}.zst ($(stat -c %s ${raw}.zst) bytes)"
}

build_one 1 test_ufs1.img
build_one 2 test_ufs2.img

ls -la "$FIXTURES_DIR"/test_ufs*.img.zst
