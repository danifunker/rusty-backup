#!/usr/bin/env bash
# Test the workspace and build rb-cli for one Linux target, as out/rb-cli-linux-<arch>-<version>.tar.gz.
# Usage: build-linux-packages.sh <rust-target> <arch-label> <version>
# CI runs it on the arm64 host, and for x64 inside docker/linux-packages-focal.Dockerfile.
set -euo pipefail

target=$1
arch=$2
version=$3
bin="target/${target}/release"
mkdir -p out

cargo test --workspace --release --target "$target"
cargo build --release --target "$target" --bin rb-cli

tar -C "$bin" -czf "out/rb-cli-linux-${arch}-${version}.tar.gz" rb-cli
ls -l out
