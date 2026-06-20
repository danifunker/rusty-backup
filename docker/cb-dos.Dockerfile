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
# Output: build/tui_poc.exe, build/disk_spike.exe, build/lfn_test.exe
# (run them under DOSBox-X on the host — see crusty-backup/run-dosbox.sh).

FROM --platform=linux/amd64 debian:bookworm-slim

# Prebuilt DJGPP (gcc 12.2.0) from the build-djgpp project + make.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl bzip2 make \
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
CMD ["make", "size"]
