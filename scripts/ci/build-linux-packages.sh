#!/usr/bin/env bash
# Test, build and package the egui GUI + rb-cli for one Linux target into out/.
# Usage: build-linux-packages.sh <rust-target> <arch-label> <version>
# CI runs it on the arm64 host, and for x64 inside docker/linux-packages-focal.Dockerfile.
set -euo pipefail

target=$1
arch=$2
version=$3
name="Rusty-Backup-linux-${arch}-${version}"
bin="target/${target}/release"
mkdir -p out

cargo test --workspace --release --target "$target"
cargo build --release --target "$target"

# -o: cargo-deb only mirrors into target/<triple>/debian as a deprecated back-compat copy.
cargo deb --target "$target" --no-build -o "out/${name}.deb"

rm -rf "target/${target}/generate-rpm"
cargo generate-rpm --target "$target"
mv "target/${target}/generate-rpm/"*.rpm "out/${name}.rpm"

root=$(mktemp -d)
install -Dm755 "${bin}/rusty-backup" "${root}/usr/bin/rusty-backup"
install -Dm644 rusty-backup.desktop -t "${root}/usr/share/applications"
for n in 16 32 48 64 128 256; do
  install -Dm644 "assets/icons/icon-${n}.png" "${root}/usr/share/icons/hicolor/${n}x${n}/apps/rusty-backup.png"
done
# Pipe through zstd: 20.04's tar (1.30) has no --zstd.
tar -C "$root" -cf - . | zstd -q -f -19 -T0 -o "out/${name}.pkg.tar.zst"
rm -rf "$root"

tar -C "$bin" -czf "out/rb-cli-linux-${arch}-${version}.tar.gz" rb-cli
ls -l out
