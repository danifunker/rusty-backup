# Build the cb-dos (crusty-backup) DOS tools with the DJGPP cross-compiler.
# See docs/cb_dos.md and crusty-backup/.
#
# DJGPP's prebuilt binaries are x86_64 Linux, so this image is amd64 (it runs
# under qemu on an arm64 host — Docker Desktop handles that transparently).
# gcc 12.2.0 matches the local ~/djgpp toolchain.
#
# Build the image:
#   docker build --platform linux/amd64 -t rb-cb-dos - < docker/cb-dos.Dockerfile
# Build the DOS .exe's into crusty-backup/build/ on the host (one-liner — the
# default command runs `make size`):
#   docker run --rm --platform linux/amd64 -v "$PWD":/src rb-cb-dos
#
# Output: build/crustybk.exe (+ diagnostics). The default command fetches the
# gitignored cross-built deps (zlib + lz4 + WATT-32 — CRUSTYBK links all three:
# gzip, LZ4, and networked `backup rb://...`) then builds, so a fresh checkout
# in CI builds without the deps pre-staged on the host.

FROM --platform=linux/amd64 debian:bookworm-slim

# Prebuilt DJGPP (gcc 12.2.0) from the build-djgpp project + make. `unzip` is
# needed by net/fetch-watt32.sh (the WATT-32 DJGPP package ships as a .zip);
# curl/tar cover the zlib/lz4 source fetches. `libfl2` provides libfl.so.2,
# which the DJGPP v3.4 binutils binaries (i586-pc-msdosdjgpp-ar) are linked
# against — without it `ar` dies with "error while loading shared libraries".
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl bzip2 make unzip libfl2 \
    && curl -fsSL \
        https://github.com/andrewwutw/build-djgpp/releases/download/v3.4/djgpp-linux64-gcc1220.tar.bz2 \
        -o /tmp/djgpp.tar.bz2 \
    && tar -xjf /tmp/djgpp.tar.bz2 -C /opt \
    && rm /tmp/djgpp.tar.bz2 \
    && rm -rf /var/lib/apt/lists/*

# The crusty-backup Makefile reads $DJGPP and calls $(DJGPP)/bin/i586-pc-msdosdjgpp-gcc.
ENV DJGPP=/opt/djgpp \
    PATH="/opt/djgpp/bin:${PATH}"

WORKDIR /src/crusty-backup
# Fetch the cross-built deps (idempotent — each skips if already present), then
# build. zlib/lz4 cross-compile under DJGPP; WATT-32 is a prebuilt DJGPP package.
CMD ["sh", "-c", "sh deps/fetch-zlib.sh && sh deps/fetch-lz4.sh && sh net/fetch-watt32.sh && make size"]
