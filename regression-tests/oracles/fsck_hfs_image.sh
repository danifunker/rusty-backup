#!/bin/sh
# Run macOS `fsck_hfs` against a disk *image* file.
#
# fsck_hfs needs a block device and reports "Can't get device block size
# (Inappropriate ioctl for device)" when handed a plain file, so the image has
# to be attached first. `-nomount` keeps the volume from appearing in Finder
# and `-readonly` means a check can never modify the artifact it is judging.
#
# Exits with fsck_hfs's own status, so the harness needs no special handling.
# The device is always detached, including on failure — leaking attached
# devices across a run would eventually exhaust them and turn every later check
# into a mysterious failure.
#
# Named as the `program` for the fsck_hfs oracle in data/oracles.toml.

set -u

IMAGE="${1:-}"
if [ -z "$IMAGE" ]; then
    echo "usage: fsck_hfs_image.sh <image>" >&2
    exit 2
fi
if [ ! -f "$IMAGE" ]; then
    echo "no such image: $IMAGE" >&2
    exit 2
fi

# CRawDiskImage forces the flat-image reader. Without it hdiutil sniffs the
# file, and a bare HFS volume with no partition map is exactly the shape it
# guesses wrong on.
DEV=$(hdiutil attach -nomount -readonly \
        -imagekey diskimage-class=CRawDiskImage "$IMAGE" 2>&1 |
      awk '/^\/dev\// { print $1; exit }')

if [ -z "$DEV" ]; then
    echo "hdiutil could not attach $IMAGE" >&2
    exit 2
fi

# The raw device is markedly faster for a whole-volume read than the buffered
# one, and fsck only ever reads here.
RDEV=$(echo "$DEV" | sed 's|/dev/disk|/dev/rdisk|')

fsck_hfs -n "$RDEV"
STATUS=$?

hdiutil detach "$DEV" >/dev/null 2>&1

exit $STATUS
