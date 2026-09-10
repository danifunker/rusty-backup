#!/bin/sh
# Build a Solaris sysroot for the sparcv9-sun-solaris* cross targets.
#
#   mksysroot.sh disk   IMAGE [SLICE]   from a Solaris disk image (9 or 10) -- preferred
#   mksysroot.sh media9 ISO             from Solaris 9 install media
#
# The disk method uses rb-cli and nothing else, so it runs anywhere rb-cli does.
# The media method needs xorriso, bunzip2 and cpio, and is Solaris 9 only: Solaris 10
# media compresses package payloads with 7z rather than bzip2.
#
# Notes/SolarisSysroot.md in the mrustc tree explains the why, including the two traps
# in the media path.
set -eu

RB="${RB:-rb-cli}"
OUT="${OUT:-$(cd "$(dirname "$0")" && pwd)}"
ROOT="$OUT/sysroot-build/root"
TARBALL="$OUT/sysroot.tar.gz"

usage() { sed -n '2,12p' "$0" >&2; exit 2; }
[ $# -ge 2 ] || usage
MODE="$1"; SRC="$2"

rm -rf "$OUT/sysroot-build"; mkdir -p "$ROOT"

case "$MODE" in
disk)
    SLICE="${3:-1}"
    command -v "$RB" >/dev/null || { echo "error: rb-cli not on PATH (set RB=)" >&2; exit 1; }
    # A real filesystem already has the development symlinks and needs no package
    # selection, so this is three copies rather than nine packages plus a pkgmap replay.
    echo "==> Reading $SRC slice $SLICE"
    # /lib matters on Solaris 10, where the core libraries live there and /usr/lib holds
    # symlinks pointing back; on Solaris 9 /lib is itself a symlink to usr/lib and this
    # copy finds nothing. Take both and let the trailing check decide which happened.
    # `get -r` lays a directory source out *under* the destination, so pass the parent:
    # naming the directory itself would give usr/include/include.
    ERR="$OUT/sysroot-build/get.err"; LINKS="$OUT/sysroot-build/symlinks.tsv"; : > "$LINKS"
    for d in usr/include usr/lib usr/ccs/lib lib; do
        echo "    $d"
        parent="$ROOT/$(dirname "$d")"
        mkdir -p "$parent"
        "$RB" get "$SRC@$SLICE" "/$d" "$parent" -r >/dev/null 2>"$ERR" || \
            echo "    (skipped $d -- not present on this install)" >&2
        # rb-cli writes a symlink as a text file holding its target, and names each one it
        # did that to; collect them so they can be turned back into links below.
        sed -n 's/^  symlink as text: \(.*\) -> \(.*\) (use platform tools.*$/\1\t\2/p' \
            "$ERR" >> "$LINKS"
    done

    # Without this every symlink is a plain file, so `-lc` finds a 12-byte text libc.so
    # and the link fails far from the cause.
    if [ -s "$LINKS" ]; then
        echo "==> Recreating $(wc -l < "$LINKS" | tr -d " ") symlinks"
        while IFS="$(printf "\t")" read -r dst target; do
            [ -n "$dst" ] && [ -n "$target" ] || continue
            rm -f "$dst" && ln -s "$target" "$dst"
        done < "$LINKS"
    fi
    ;;
media9)
    command -v xorriso >/dev/null || { echo "error: xorriso is required" >&2; exit 1; }
    # SUNWlibm carries the math *headers*, SUNWlibms the math *libraries*: taking only
    # the latter loses floatingpoint.h and fails inside math.h, far from the cause.
    PKGS="SUNWhea SUNWlibm SUNWcsl SUNWcslx SUNWlibms SUNWlmsx SUNWarc SUNWarcx SUNWtoo"
    W="$OUT/sysroot-build/pkgs"; mkdir -p "$W"
    echo "==> Extracting packages"
    set -- -osirrox on -indev "$SRC"
    for p in $PKGS; do set -- "$@" -extract "/Solaris_9/Product/$p" "$W/$p"; done
    xorriso "$@" >/dev/null 2>&1
    echo "==> Unpacking payloads"
    for p in $PKGS; do
        [ -f "$W/$p/archive/none.bz2" ] || { echo "error: $p has no archive/none.bz2 (Solaris 10 media uses none.7z)" >&2; exit 1; }
        bunzip2 -c "$W/$p/archive/none.bz2" | ( cd "$ROOT" && cpio -idmu --quiet )
    done
    # SVR4 keeps symlinks in the pkgmap, not the payload; pkgadd makes them on install.
    echo "==> Replaying symlinks from pkgmaps"
    python3 - "$W" "$ROOT" <<'PY'
import os, sys, glob
work, root = sys.argv[1], sys.argv[2]
n = 0
for pm in glob.glob(os.path.join(work, "*", "pkgmap")):
    for line in open(pm, errors="replace"):
        f = line.split()
        if len(f) >= 4 and f[1] == "s" and "=" in f[3]:
            path, target = f[3].split("=", 1)
            dst = os.path.join(root, path)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            if not os.path.lexists(dst):
                try:
                    os.symlink(target, dst); n += 1
                except OSError:
                    pass
print("    %d symlinks" % n)
PY
    ;;
*) usage ;;
esac

# Only invent lib -> usr/lib when /lib did not come across as a real directory: doing
# it unconditionally would replace Solaris 10's actual libraries with a dangling link.
[ -d "$ROOT/lib" ] || ln -sfn usr/lib "$ROOT/lib"

[ -f "$ROOT/usr/include/stdio.h" ] || { echo "error: no usr/include/stdio.h" >&2; exit 1; }
# Solaris 9 keeps the 64-bit libc under usr/lib, Solaris 10 under lib. Accept either,
# and resolve through the symlink so a dangling one is caught here rather than at link time.
if [ ! -s "$ROOT/usr/lib/sparcv9/libc.so.1" ] && [ ! -s "$ROOT/lib/sparcv9/libc.so.1" ]; then
    echo "error: no readable 64-bit libc under usr/lib/sparcv9 or lib/sparcv9" >&2
    exit 1
fi

echo "==> Writing $TARBALL"
tar czf "$TARBALL" -C "$ROOT" usr lib 2>/dev/null || tar czf "$TARBALL" -C "$ROOT" usr
echo "    $(du -h "$TARBALL" | cut -f1)"
