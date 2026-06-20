# Cross-build rb-cli as a STATIC i586 (Pentium-class) binary.
# See docs/linux_486_build.md.
#
# Uses the prebuilt i586-unknown-linux-musl target (stable; bundles musl libc +
# CRT + libunwind), so there's no build-std and no toolchain fiddling — only the
# 32-bit x86 cross-gcc is needed to drive the link (the default `cc` on a non-x86
# host rejects -m32). Works from any host arch incl. Apple-Silicon arm64.
#
# The result is a single static binary with no shared-library dependencies —
# ideal as a drop-on-a-rootfs backup appliance for Pentium-class machines.
#
# NOTE: this is Pentium (i586) codegen, so it uses CMPXCHG8B and will fault
# (illegal instruction) on a bare 486. For a true 486 use cross-i486.Dockerfile
# (i486 codegen) with an i486 rootfs — see docs/linux_486_build.md.
#
# Build the image:
#   docker build -t rb-cross-i586-musl - < docker/cross-i586-musl.Dockerfile
# Cross-build (one-liner — default command builds the static binary to
# ./target-cross/i586-unknown-linux-musl/release/rb-cli):
#   docker run --rm -v "$PWD":/src rb-cross-i586-musl
# Run it under an emulated Pentium (static — no rootfs needed):
#   docker run --rm -v "$PWD":/src rb-cross-i586-musl \
#     qemu-i386 -cpu pentium \
#       /src/target-cross/i586-unknown-linux-musl/release/rb-cli --version

FROM rust:bookworm

# 32-bit x86 cross-gcc to drive the link (-m32) + qemu to run/test the result.
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc-i686-linux-gnu qemu-user \
    && rm -rf /var/lib/apt/lists/*

RUN rustup target add i586-unknown-linux-musl

ENV CARGO_TARGET_I586_UNKNOWN_LINUX_MUSL_LINKER=i686-linux-gnu-gcc

WORKDIR /src
CMD ["cargo", "build", "--release", "--bin", "rb-cli", \
     "--no-default-features", "--features", "pure-zstd", \
     "--target", "i586-unknown-linux-musl", "--target-dir", "/src/target-cross"]
