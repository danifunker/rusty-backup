#!/bin/sh
# Build the sparcv9-sun-solaris2.9 cross-toolchain image (mrustc-sol9-cross).
#
# Needs a Solaris 9 sysroot, which is the one part of this that cannot be fetched from a
# public source: it is a copy of a licensed Solaris 9 install's headers and libraries, and
# Solaris 9 predates OpenSolaris by three years so no free substitute exists. Supply one of:
#
#   ./mksysroot-from-iso.sh DVD.iso  build one from the install DVD -- no Solaris box needed
#   SOL9_SPARC64_SYSROOT_URL=https://...     download a prepared tarball (what CI uses; keep it private)
#   SOL9_HOST=user@host              pull one off a live Solaris 9 SPARC install
#   ./sysroot.tar.gz                 drop your own next to this script
#
# Building one from scratch: Notes/SolarisSysroot.md in the mrustc tree.
# The tarball is rooted at /, holding usr/include, usr/ccs/lib and usr/lib.
set -eu

SOL9_HOST="${SOL9_HOST:-}"
SOL9_SPARC64_SYSROOT_URL="${SOL9_SPARC64_SYSROOT_URL:-}"
IMAGE="${IMAGE:-mrustc-sol9-cross}"
TAR="${TAR:-/opt/csw/bin/gtar}"
cd "$(dirname "$0")"

if [ ! -f sysroot.tar.gz ]; then
    if [ -n "$SOL9_SPARC64_SYSROOT_URL" ]; then
        echo "==> Downloading sysroot"
        curl -fsSL "$SOL9_SPARC64_SYSROOT_URL" -o sysroot.tar.gz.tmp
        mv sysroot.tar.gz.tmp sysroot.tar.gz
    elif [ -n "$SOL9_HOST" ]; then
        echo "==> Pulling sysroot from $SOL9_HOST"
        # sudo: parts of /usr/lib are not world-readable, and a partial sysroot fails late.
        ssh "$SOL9_HOST" "sudo $TAR czf - -C / usr/include usr/ccs/lib usr/lib" > sysroot.tar.gz.tmp
        mv sysroot.tar.gz.tmp sysroot.tar.gz
    else
        echo "error: no sysroot.tar.gz, and neither SOL9_SPARC64_SYSROOT_URL nor SOL9_HOST is set." >&2
        exit 1
    fi
fi

# A truncated or HTML-error download fails deep inside the GCC build otherwise.
gzip -t sysroot.tar.gz || { echo "error: sysroot.tar.gz is not a valid gzip file" >&2; exit 1; }
# Accept both `usr/...` and `./usr/...`: `tar -C / -c usr` gives the first, `tar -c .` the
# second, and both unpack identically. Naming the member exactly would reject the second.
listing="$(tar tzf sysroot.tar.gz)"
if ! printf '%s\n' "$listing" | grep -qE '^(\./)?usr/include/stdio\.h$'; then
    echo "error: sysroot.tar.gz has no usr/include/stdio.h; is it rooted at / ?" >&2
    echo "       its top-level entries are:" >&2
    printf '%s\n' "$listing" | sed 's|^\./||' | awk -F/ 'NF>1{print $1"/"$2}' | sort -u | head -10 >&2
    exit 1
fi

echo "==> Building $IMAGE"
exec docker build -t "$IMAGE" "$@" .
