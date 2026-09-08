#!/bin/sh
# Build a Solaris 9 sysroot from the install DVD, with no Solaris machine involved.
#
#   ./mksysroot-from-iso.sh /path/to/sol-9-905hw-ga-sparc-dvd.iso [outdir]
#
# Needs only xorriso, bunzip2 and cpio. Produces sysroot.tar.gz next to this script,
# ready for build.sh. See Notes/SolarisSysroot.md in the mrustc tree for the why.
set -eu

ISO="${1:?usage: mksysroot-from-iso.sh ISO [outdir]}"
OUT="${2:-$(dirname "$0")/sysroot-build}"
WORK="$OUT/pkgs"
ROOT="$OUT/root"

# Enough for a C toolchain: headers, the 32- and 64-bit core/math libraries and their
# archive counterparts. SUNWlibm carries the math *headers* (floatingpoint.h), which
# SUNWlibms does not -- omitting it fails much later, inside math.h.
PKGS="SUNWhea SUNWlibm SUNWcsl SUNWcslx SUNWlibms SUNWlmsx SUNWarc SUNWarcx SUNWtoo"

command -v xorriso >/dev/null || { echo "error: xorriso is required" >&2; exit 1; }
rm -rf "$WORK" "$ROOT"; mkdir -p "$WORK" "$ROOT"

echo "==> Extracting packages"
set -- -osirrox on -indev "$ISO"
for p in $PKGS; do set -- "$@" -extract "/Solaris_9/Product/$p" "$WORK/$p"; done
xorriso "$@" >/dev/null 2>&1

echo "==> Unpacking payloads"
for p in $PKGS; do
    [ -f "$WORK/$p/archive/none.bz2" ] || { echo "error: $p has no archive/none.bz2" >&2; exit 1; }
    bunzip2 -c "$WORK/$p/archive/none.bz2" | ( cd "$ROOT" && cpio -idmu --quiet )
done

# SVR4 keeps symlinks in the pkgmap, not the cpio payload: pkgadd creates them. Without
# this the development symlinks (libm.so -> libm.so.1) are absent and every -l fails.
echo "==> Replaying symlinks from pkgmaps"
python3 - "$WORK" "$ROOT" <<'PY'
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

ln -sfn usr/lib "$ROOT/lib"
[ -f "$ROOT/usr/include/stdio.h" ] || { echo "error: no usr/include/stdio.h" >&2; exit 1; }
[ -e "$ROOT/usr/lib/sparcv9/libc.so.1" ] || { echo "error: no 64-bit libc" >&2; exit 1; }

TARBALL="$(cd "$(dirname "$0")" && pwd)/sysroot.tar.gz"
echo "==> Writing $TARBALL"
tar czf "$TARBALL" -C "$ROOT" usr lib
echo "    $(du -h "$TARBALL" | cut -f1)"
