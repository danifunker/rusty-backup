# Cross-build rb-cli for 486-class Linux (i586-unknown-linux-gnu), the Linux
# sibling of cb-dos. See docs/linux_486_build.md.
#
# Works from any host arch (incl. Apple-Silicon arm64): rustc runs natively and
# emits i586 object code; the i686-linux-gnu cross-gcc does the 32-bit x86 link,
# so no emulation is needed for the build.
#
# Build the toolchain image:
#   docker build -t rb-cross-i586 - < docker/cross-i586.Dockerfile
# Cross-build (one-liner — the default command builds; binary lands in
# ./target-cross/i586-unknown-linux-gnu/release/rb-cli on the host):
#   docker run --rm -v "$PWD":/src rb-cross-i586
#
# Note: Debian's 32-bit glibc is i686-baseline, so the result runs on Pentium+.
# A *true* 486 needs an i486 sysroot (musl / Buildroot) — see the doc.

FROM rust:bookworm

# i686 cross toolchain = 32-bit x86 linker + glibc sysroot (+ libatomic, which
# the i486 tier's lowered 64-bit atomics call into).
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc-i686-linux-gnu \
        libc6-dev-i386-cross \
    && rm -rf /var/lib/apt/lists/*

# Prebuilt i586 std (tier-2 target); no build-std needed for this tier.
RUN rustup target add i586-unknown-linux-gnu

# Use the cross-gcc to link the i586 target.
ENV CARGO_TARGET_I586_UNKNOWN_LINUX_GNU_LINKER=i686-linux-gnu-gcc

WORKDIR /src
CMD ["cargo", "build", "--release", "--bin", "rb-cli", \
     "--no-default-features", "--features", "pure-zstd,remote", \
     "--target", "i586-unknown-linux-gnu", "--target-dir", "/src/target-cross"]
