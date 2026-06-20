# Cross-build rb-cli for a true i486 (i486-unknown-linux-gnu), Tier 2.
# See docs/linux_486_build.md.
#
# The i486 has no CMPXCHG8B, so this uses a custom target spec with
# max-atomic-width=32 (64-bit atomics -> libatomic libcalls) and builds std
# from source (-Z build-std) since there is no prebuilt i486 std.
#
# CAVEAT: links against Debian's 32-bit glibc, which is i686-baseline, so the
# resulting binary needs a Pentium-class glibc at runtime. Our *code* is
# i486-clean; for a binary that runs on bare 486 hardware use the static-musl
# image (cross-i486-musl.Dockerfile) instead.
#
# Build the image:
#   docker build -t rb-cross-i486 - < docker/cross-i486.Dockerfile
# Cross-build (one-liner — default command builds to
# ./target-cross/i486-unknown-linux-gnu/release/rb-cli):
#   docker run --rm -v "$PWD":/src rb-cross-i486

FROM rust:bookworm

# i686 cross toolchain: 32-bit x86 linker + glibc sysroot + libatomic (the
# lowered 64-bit atomics call into it).
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc-i686-linux-gnu \
        libc6-dev-i386-cross \
    && rm -rf /var/lib/apt/lists/*

# Nightly + std source for -Z build-std (no prebuilt std for a custom target).
RUN rustup toolchain install nightly --profile minimal \
    && rustup component add rust-src --toolchain nightly

ENV CARGO_TARGET_I486_UNKNOWN_LINUX_GNU_LINKER=i686-linux-gnu-gcc \
    RUSTFLAGS="-C link-arg=-latomic"

WORKDIR /src
CMD ["cargo", "+nightly", "build", "--release", "--bin", "rb-cli", \
     "--no-default-features", "--features", "pure-zstd", \
     "-Z", "build-std=std,panic_abort", "-Z", "json-target-spec", \
     "--target", "/src/targets/i486-unknown-linux-gnu.json", \
     "--target-dir", "/src/target-cross"]
