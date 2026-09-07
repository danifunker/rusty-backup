# Build rb-cli (CLI + TUI) for Solaris 9 on SPARC, via mrustc.
# See docs/build-sol9-mrustc.md and rb-cli-sol9/.
#
# This is the one vintage target that cross-builds end to end on Linux, so
# unlike the PowerPC Mac build (which needs real PowerPC hardware to compile its
# C) it can run in the release pipeline unattended. That is the whole point of
# this image.
#
# PREREQUISITE: the base image. It carries a GCC 4.9.4 that can target Solaris 9
# plus a Solaris 9 sysroot, and it CANNOT be published -- Sun's headers and
# libraries are not redistributable. Build it once from the mrustc tree:
#
#   cd ~/repos/mrustc/docker/sol9-cross && SOL9_HOST=user@<blade> ./build.sh
#
# Then build this image (bakes in mrustc + the Solaris stdlib, so each later
# build is only the engine), and build the artifact with the repo bind-mounted:
#
#   docker build -t rb-sol9 - < docker/sol9.Dockerfile
#   docker run --rm -v "$PWD":/src rb-sol9
#   # -> ./dist/rb-cli-sol9.tar.gz
#
# Override the stage to do less than the default full run:
#   docker run --rm -v "$PWD":/src rb-sol9 scripts/build-sol9.sh check

ARG BASE=mrustc-sol9-cross
FROM ${BASE}

ARG MRUSTC_REPO=https://github.com/danifunker/mrustc
# The Solaris 9 target lives on this branch: the target spec itself, the
# emitted-overflow-helper support GCC 4.9 needs, and the CC_${TRIPLE} name
# sanitisation that a triple with dots in it requires.
ARG MRUSTC_BRANCH=sparc-solaris-10
ARG RUSTC_VERSION=1.74.0
ARG TARGET=sparcv9-sun-solaris2.9

# A modern cargo, for `cargo vendor` only -- nothing here is built with it. The
# engine is compiled by mrustc, whose language mode is pinned to 1.74.
RUN apt-get update && apt-get install -y --no-install-recommends \
        cargo rsync \
    && rm -rf /var/lib/apt/lists/*

# mrustc, and the rustc source it bootstraps its standard library from.
RUN git clone --branch "${MRUSTC_BRANCH}" --single-branch "${MRUSTC_REPO}" /opt/mrustc
WORKDIR /opt/mrustc
RUN make -j"$(nproc)" && make -f minicargo.mk bin/minicargo RUSTC_VERSION="${RUSTC_VERSION}"
RUN make RUSTCSRC RUSTC_VERSION="${RUSTC_VERSION}"

# The host standard library, then the Solaris 9 one. Baking both in is what
# makes a pipeline run cost only the engine transpile.
#
# OVERRIDE_SUFFIX is picked from the *host* OS by minicargo.mk, so a cross build
# from Linux would silently take the -linux build-script overrides.
# STD_ENV_ARCH is needed because std::env::consts::ARCH is otherwise derived
# from the first triple component, which is `sparcv9` rather than `sparc64`.
RUN make -f minicargo.mk LIBS RUSTC_VERSION="${RUSTC_VERSION}" -j"$(nproc)"
RUN make -f minicargo.mk LIBS \
        RUSTC_VERSION="${RUSTC_VERSION}" \
        MRUSTC_TARGET="${TARGET}" \
        OVERRIDE_SUFFIX=-solaris \
        STD_ENV_ARCH=sparc64 \
        PARLEVEL="$(nproc)" \
    && ls "output-${RUSTC_VERSION}-${TARGET}/libstd.rlib"

# scripts/build-sol9.sh reads every path from the environment, so the image only
# has to say where things ended up.
ENV MRUSTC_DIR=/opt/mrustc \
    RB_DIR=/src \
    SOL9_BIN=/opt/sol9/bin \
    SOL9_SYSROOT=/opt/sol9/sysroot \
    RUSTC_VERSION=1.74.0

WORKDIR /src
CMD ["scripts/build-sol9.sh"]
