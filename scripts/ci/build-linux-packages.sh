#!/usr/bin/env bash
# Test, build and package the egui GUI + rb-cli for one Linux target into out/.
# Usage: build-linux-packages.sh <rust-target> <arch-label> <version>
# CI runs it on the arm64 host, and for x64 inside docker/linux-packages-focal.Dockerfile.
set -euo pipefail

target=$1
arch=$2
version=$3
name="Rusty-Backup-linux-${arch}-${version}"
# Package managers order dotted numbers; the tag's dashes would read as a Debian revision.
pkgver="${version//-/.}"
bin="target/${target}/release"
mkdir -p out

cargo test --workspace --release --target "$target" --features gui
cargo build --release --target "$target" --features gui

# -o: cargo-deb only mirrors into target/<triple>/debian as a deprecated back-compat copy.
cargo deb --target "$target" --no-build --deb-version "$pkgver" -o "out/${name}.deb"
[ "$(dpkg-deb -f "out/${name}.deb" Version)" = "$pkgver" ] || { echo "deb version is not $pkgver" >&2; exit 1; }

rm -rf "target/${target}/generate-rpm"
cargo generate-rpm --target "$target" --set-metadata "version = \"$pkgver\""
mv "target/${target}/generate-rpm/"*.rpm "out/${name}.rpm"
[ "$(rpm -qp --qf '%{VERSION}' "out/${name}.rpm")" = "$pkgver" ] || { echo "rpm version is not $pkgver" >&2; exit 1; }

# The Arch package is made from this .deb on the runner (scripts/ci/build-arch-package.sh).

tar -C "$bin" -czf "out/rb-cli-linux-${arch}-${version}.tar.gz" rb-cli
ls -l out
