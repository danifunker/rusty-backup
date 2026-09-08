# Cross toolchain: x86_64 Linux -> 64-bit SPARC Solaris 9 (sparcv9-sun-solaris2.9)
#
# Why 4.9.4, and why an old base image:
#   GCC obsoleted Solaris 9 in 4.9 (buildable only with --enable-obsolete) and deleted
#   the port in the next release, which shipped as GCC 5. So 4.9.4 is the newest GCC
#   that can target Solaris 9 at all. GCC 4.9's own sources predate C++11-strict compilers
#   and need -std=gnu++98 -fpermissive; with those it does build under a current host GCC
#   (verified on 13.3), so the Debian 11 (GCC 10) pin is for reproducibility, not necessity.
#
#   GCC 4.9 has <stdatomic.h> but not __builtin_{add,sub,mul}_overflow, which arrived in
#   GCC 5. mrustc covers that gap by emitting its own helpers -- see the
#   `emulate-overflow-intrinsics` target flag used by the sparcv9-sun-solaris2.9 target.
#
# The sysroot is not downloadable: Sun's headers and libraries are not redistributable.
# Supply sysroot.tar.gz in the build context, as ./build.sh does. See README.md.

ARG BASE=debian:11-slim

FROM ${BASE} AS builder

ARG BINUTILS_VERSION=2.35.2
ARG GCC_VERSION=4.9.4
ARG TARGET=sparcv9-sun-solaris2.9
ARG PREFIX=/opt/sol9

ENV SYSROOT=${PREFIX}/sysroot

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential ca-certificates curl xz-utils bzip2 file \
        libgmp-dev libmpfr-dev libmpc-dev m4 texinfo \
    && rm -rf /var/lib/apt/lists/*

# Solaris 9 ships no crt1.o/crti.o/crtn.o -- on Solaris those come with the compiler,
# not the OS -- so only headers and libraries are needed here. GCC builds its own crt
# files from libgcc/config/sparc/sol2-c1.S.
COPY sysroot.tar.gz /tmp/
RUN mkdir -p "${SYSROOT}" \
    && tar xzf /tmp/sysroot.tar.gz -C "${SYSROOT}" \
    && rm /tmp/sysroot.tar.gz \
    && ln -sfn usr/lib "${SYSROOT}/lib"

WORKDIR /src
RUN curl -fsSLO "https://ftp.gnu.org/gnu/binutils/binutils-${BINUTILS_VERSION}.tar.xz" \
    && tar xJf "binutils-${BINUTILS_VERSION}.tar.xz" \
    && rm "binutils-${BINUTILS_VERSION}.tar.xz"
RUN curl -fsSLO "https://ftp.gnu.org/gnu/gcc/gcc-${GCC_VERSION}/gcc-${GCC_VERSION}.tar.bz2" \
    && tar xjf "gcc-${GCC_VERSION}.tar.bz2" \
    && rm "gcc-${GCC_VERSION}.tar.bz2"

# GNU as/ld: Solaris' own are not available to a cross build, and mrustc's target spec
# already avoids the GNU-only linker options that Solaris ld lacks.
RUN mkdir -p /build/binutils && cd /build/binutils \
    && "/src/binutils-${BINUTILS_VERSION}/configure" \
        --target="${TARGET}" --prefix="${PREFIX}" --with-sysroot="${SYSROOT}" \
        --disable-nls --disable-werror \
    && make -j"$(nproc)" && make install-strip \
    && cd / && rm -rf /build/binutils

ENV PATH=${PREFIX}/bin:${PATH}

# -std=gnu++98 -fpermissive: GCC 4.9's own sources do not compile under a C++11-or-later
# default. libsanitizer is disabled because it does not build against modern glibc headers
# and is of no use to a Rust codegen backend.
RUN mkdir -p /build/gcc && cd /build/gcc \
    && "/src/gcc-${GCC_VERSION}/configure" \
        --target="${TARGET}" --prefix="${PREFIX}" \
        --with-sysroot="${SYSROOT}" --with-build-sysroot="${SYSROOT}" \
        --enable-obsolete \
        --enable-languages=c \
        --enable-threads=posix \
        --with-gnu-as --with-gnu-ld \
        --with-as="${PREFIX}/bin/${TARGET}-as" --with-ld="${PREFIX}/bin/${TARGET}-ld" \
        --disable-nls --disable-libssp --disable-libgomp --disable-libatomic \
        --disable-libitm --disable-libsanitizer --disable-libquadmath --disable-libvtv \
        --disable-libcilkrts \
        MAKEINFO=missing \
        CFLAGS="-O2 -w" CXXFLAGS="-O2 -w -std=gnu++98 -fpermissive" \
    && make -j"$(nproc)" && make install-strip \
    && cd / && rm -rf /build/gcc /src

# Solaris 9's <sys/int_limits.h> defines INTPTR_MAX and UINTPTR_MAX as *empty*
# macros -- pre-C99 they were existence flags ("a pointer fits in an integral
# type"), and C99 later gave them values. GCC copies the header into include-fixed
# without fixing this, so C99 code doing `#if UINTPTR_MAX == ...` fails with
# "operator '==' has no left operand" (mbedTLS, among others). GCC's own
# __INTPTR_MAX__/__UINTPTR_MAX__ are per-multilib, so this is right for both.
RUN H="${PREFIX}/lib/gcc/${TARGET}/${GCC_VERSION}/include-fixed/sys/int_limits.h" \
    && sed -i -e 's|^#define[ \t]*INTPTR_MAX[ \t]*$|#define\tINTPTR_MAX\t__INTPTR_MAX__|' \
              -e 's|^#define[ \t]*UINTPTR_MAX[ \t]*$|#define\tUINTPTR_MAX\t__UINTPTR_MAX__|' "$H" \
    && grep -q '__UINTPTR_MAX__' "$H"


FROM ${BASE}

ARG TARGET=sparcv9-sun-solaris2.9
ARG PREFIX=/opt/sol9

# build-essential/python3/curl/patch are mrustc's own build and run dependencies, so this
# one image can build mrustc and then cross-compile its C output.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential ca-certificates curl git patch python3 zlib1g-dev file \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder ${PREFIX} ${PREFIX}

ENV PATH=${PREFIX}/bin:${PATH}
# mrustc looks for `<target>-gcc`, overridable via CC_<target with - replaced by _>
ENV CC_sparcv9_sun_solaris2_9=${TARGET}-gcc

WORKDIR /work
