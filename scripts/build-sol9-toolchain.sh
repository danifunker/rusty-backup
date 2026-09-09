#!/usr/bin/env bash
#
# build-sol9-toolchain.sh -- build the sparcv9-sun-solaris2.9 cross toolchain natively.
#
# No container. GitHub's ubuntu-latest runners and this machine are both Ubuntu 24.04 with
# GCC 13.3, and GCC 4.9.4 builds under that given the flags below -- so the toolchain can
# be built on the same OS that will use it, and packed as a seed for CI.
#
#   scripts/build-sol9-toolchain.sh SYSROOT.tar.gz [PREFIX]
#
# PREFIX defaults to ~/sol9-toolchain/opt, matching scripts/build-sol9.sh's own default.
# The sysroot tarball is rooted at / and holds usr/include, usr/ccs/lib and usr/lib.
#
# Why 4.9.4: GCC obsoleted Solaris 9 in 4.9 (buildable only with --enable-obsolete) and
# deleted the port in the next release, which shipped as GCC 5.
set -euo pipefail

SYSROOT_TGZ="${1:?usage: $0 SYSROOT.tar.gz [PREFIX]}"
PREFIX="${2:-$HOME/sol9-toolchain/opt}"
SYSROOT="${SOL9_SYSROOT:-$(dirname "$PREFIX")/sysroot}"

BINUTILS_VERSION="${BINUTILS_VERSION:-2.35.2}"
GCC_VERSION="${GCC_VERSION:-4.9.4}"
TARGET="${TARGET:-sparcv9-sun-solaris2.9}"
SRC="${SRC:-${TMPDIR:-/tmp}/sol9-toolchain-src}"
JOBS="${JOBS:-$(nproc)}"

say() { printf '\033[1;36m==> %s\033[0m\n' "$*"; }

[ -f "$SYSROOT_TGZ" ] || { echo "error: no sysroot tarball at $SYSROOT_TGZ" >&2; exit 1; }
tar tzf "$SYSROOT_TGZ" | grep -qE '^(\./)?usr/include/stdio\.h$' \
  || { echo "error: $SYSROOT_TGZ has no usr/include/stdio.h; is it rooted at / ?" >&2; exit 1; }

say "Unpacking the sysroot into $SYSROOT"
mkdir -p "$SYSROOT"
tar xzf "$SYSROOT_TGZ" -C "$SYSROOT"
# Solaris 9 keeps its libraries only in /usr/lib; Solaris 10 has the real ones in /lib with
# symlinks in /usr/lib, so linking unconditionally would destroy them.
[ -d "$SYSROOT/lib" ] || ln -sfn usr/lib "$SYSROOT/lib"

mkdir -p "$SRC"; cd "$SRC"
if [ ! -d "binutils-$BINUTILS_VERSION" ]; then
    say "Fetching binutils $BINUTILS_VERSION"
    curl -fsSLO "https://ftp.gnu.org/gnu/binutils/binutils-$BINUTILS_VERSION.tar.xz"
    tar xJf "binutils-$BINUTILS_VERSION.tar.xz"; rm "binutils-$BINUTILS_VERSION.tar.xz"
fi
if [ ! -d "gcc-$GCC_VERSION" ]; then
    say "Fetching gcc $GCC_VERSION"
    curl -fsSLO "https://ftp.gnu.org/gnu/gcc/gcc-$GCC_VERSION/gcc-$GCC_VERSION.tar.bz2"
    tar xjf "gcc-$GCC_VERSION.tar.bz2"; rm "gcc-$GCC_VERSION.tar.bz2"
fi

# GNU as/ld: Solaris' own are not available to a cross build, and mrustc's target spec
# already avoids the GNU-only linker options Solaris ld lacks.
say "Building binutils -> $PREFIX"
rm -rf "$SRC/build-binutils"; mkdir -p "$SRC/build-binutils"; cd "$SRC/build-binutils"
"$SRC/binutils-$BINUTILS_VERSION/configure" \
    --target="$TARGET" --prefix="$PREFIX" --with-sysroot="$SYSROOT" \
    --disable-nls --disable-werror >/dev/null
make -j"$JOBS" >/dev/null
make install-strip >/dev/null

# -std=gnu++98 -fpermissive: GCC 4.9's own sources predate C++11-strict compilers, which
# every current host compiler is. libsanitizer does not build against modern glibc headers
# and is of no use to a Rust codegen backend.
say "Building gcc $GCC_VERSION -> $PREFIX (the slow part, ~30 min)"
rm -rf "$SRC/build-gcc"; mkdir -p "$SRC/build-gcc"; cd "$SRC/build-gcc"
PATH="$PREFIX/bin:$PATH" "$SRC/gcc-$GCC_VERSION/configure" \
    --target="$TARGET" --prefix="$PREFIX" \
    --with-sysroot="$SYSROOT" --with-build-sysroot="$SYSROOT" \
    --enable-obsolete --enable-languages=c --enable-threads=posix \
    --with-gnu-as --with-gnu-ld \
    --with-as="$PREFIX/bin/$TARGET-as" --with-ld="$PREFIX/bin/$TARGET-ld" \
    --disable-nls --disable-libssp --disable-libgomp --disable-libatomic \
    --disable-libitm --disable-libsanitizer --disable-libquadmath --disable-libvtv \
    --disable-libcilkrts \
    MAKEINFO=missing \
    CFLAGS="-O2 -w" CXXFLAGS="-O2 -w -std=gnu++98 -fpermissive" >/dev/null
PATH="$PREFIX/bin:$PATH" make -j"$JOBS" >/dev/null
make install-strip >/dev/null

# Solaris 9's <sys/int_limits.h> defines INTPTR_MAX and UINTPTR_MAX as *empty* macros --
# pre-C99 they were existence flags, and C99 later gave them values. GCC copies the header
# into include-fixed without fixing this, so C99 code testing them fails to compile.
H="$PREFIX/lib/gcc/$TARGET/$GCC_VERSION/include-fixed/sys/int_limits.h"
if [ -f "$H" ] && grep -qE '^#define[[:space:]]*U?INTPTR_MAX[[:space:]]*$' "$H"; then
    say "Repairing empty INTPTR_MAX/UINTPTR_MAX in include-fixed"
    sed -i -e 's|^#define[ \t]*INTPTR_MAX[ \t]*$|#define\tINTPTR_MAX\t__INTPTR_MAX__|' \
           -e 's|^#define[ \t]*UINTPTR_MAX[ \t]*$|#define\tUINTPTR_MAX\t__UINTPTR_MAX__|' "$H"
    grep -q '__UINTPTR_MAX__' "$H"
fi

say "Checking it works"
echo 'int main(void){return 0;}' > "$SRC/t.c"
"$PREFIX/bin/$TARGET-gcc" -m64 -mcpu=v9 "$SRC/t.c" -o "$SRC/t"
file "$SRC/t" | grep -q 'SPARC V9' || { echo "error: not a SPARC V9 binary" >&2; exit 1; }
rm -rf "$SRC/build-binutils" "$SRC/build-gcc"
say "Toolchain ready: $("$PREFIX/bin/$TARGET-gcc" --version | head -1)"
