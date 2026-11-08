# Ubuntu 20.04 build image for the x64 egui Linux packages (deb, rpm, Arch) and rb-cli.
# Its glibc (2.31) is their floor. CI builds it each run and runs the build with `docker run`,
# so no GitHub action executes in here; see scripts/ci/build-linux-packages.sh.
# When focal's apt mirror goes away this stops building: that is the cue to retire the 20.04 floor.
FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential ca-certificates curl git file cmake pkg-config \
      libgtk-3-dev libasound2-dev libssl-dev liblzma-dev dpkg-dev rpm zstd \
 && rm -rf /var/lib/apt/lists/*

# Pinned so a rebuild gives the same compiler; bump it deliberately.
ENV RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/usr/local/cargo PATH=/usr/local/cargo/bin:$PATH
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path --profile minimal --default-toolchain 1.99.1
RUN cargo install --locked cargo-deb@3.8.0 cargo-generate-rpm@0.21.1 \
 && rm -rf "$CARGO_HOME/registry" "$CARGO_HOME/git"

# libchdman-rs's default x86_64 prebuilt needs glibc 2.35; this picks its 20.04-built one.
ENV LIBCHDMAN_GLIBC=2.31

WORKDIR /src
CMD ["scripts/ci/build-linux-packages.sh", "x86_64-unknown-linux-gnu", "x64", "dev"]
