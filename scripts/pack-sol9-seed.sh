#!/usr/bin/env bash
#
# pack-sol9-seed.sh -- pack a prebuilt Solaris 9 build environment for CI.
#
# CI has no business rebuilding a 2016 GCC on every run, and a container image carries a
# whole Debian plus the rustc source purely to redo work already done here. This packs only
# what scripts/build-sol9.sh actually reads: the cross toolchain and its sysroot, mrustc's
# two binaries, and the two prebuilt standard libraries.
#
#   scripts/pack-sol9-seed.sh [OUT.tar.gz]
#
# Paths are staged absolute, at /opt/sol9 and /opt/mrustc, because GCC bakes its sysroot
# path in at configure time -- the tree has to land where it was configured. Unpack with
# `sudo tar xzf sol9-seed.tar.gz -C /`.
#
# TOOLCHAIN_SRC is a toolchain prefix holding bin/, sysroot/ and <target>/, as
# scripts/build-sol9-toolchain.sh produces. It defaults to /opt/sol9, which is where the
# seed unpacks, so re-packing an already-seeded host needs no arguments.
#
# The result contains Sun's headers and libraries, so it is NOT redistributable -- host it
# privately, exactly as for the sysroot it is built from.
set -euo pipefail

OUT="${1:-$PWD/sol9-seed.tar.gz}"
MRUSTC_DIR="${MRUSTC_DIR:-$HOME/repos/mrustc}"
RUSTC_VERSION="${RUSTC_VERSION:-1.74.0}"
TARGET="${SOL9_TARGET:-sparcv9-sun-solaris2.9}"
TOOLCHAIN_SRC="${TOOLCHAIN_SRC:-/opt/sol9}"

say() { printf '\033[1;36m==> %s\033[0m\n' "$*"; }
need() { [ -e "$1" ] || { echo "error: missing $1 -- $2" >&2; exit 1; }; }

need "$MRUSTC_DIR/bin/mrustc" "build mrustc first"
need "$MRUSTC_DIR/bin/minicargo" "build minicargo first"
need "$MRUSTC_DIR/output-$RUSTC_VERSION/libstd.rlib" "build the host stdlib first"
need "$MRUSTC_DIR/output-$RUSTC_VERSION-$TARGET/libstd.rlib" "run build-sol9.sh sol9libs first"

STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
mkdir -p "$STAGE/opt"

say "Taking the toolchain from $TOOLCHAIN_SRC"
need "$TOOLCHAIN_SRC/bin/$TARGET-gcc" "not a toolchain prefix -- run scripts/build-sol9-toolchain.sh"
cp -a "$TOOLCHAIN_SRC" "$STAGE/opt/sol9"
need "$STAGE/opt/sol9/sysroot/usr/include/stdio.h" "the toolchain carries no sysroot"

say "Staging mrustc and the prebuilt standard libraries"
mkdir -p "$STAGE/opt/mrustc/bin"
cp -a "$MRUSTC_DIR/bin/mrustc" "$MRUSTC_DIR/bin/minicargo" "$STAGE/opt/mrustc/bin/"
cp -a "$MRUSTC_DIR/output-$RUSTC_VERSION" "$STAGE/opt/mrustc/"
cp -a "$MRUSTC_DIR/output-$RUSTC_VERSION-$TARGET" "$STAGE/opt/mrustc/"

# The emitted C and mrustc's debug dumps are regenerated on demand and are most of the
# weight. Keep .rlib.hir: the .rlib itself is a 0-byte marker, and .hir is where the crate
# metadata actually lives -- without it every dependent crate fails with "Unable to
# deserialise crate metadata".
say "Dropping regenerable intermediates"
find "$STAGE/opt/mrustc" \( -name '*_dbg.txt' -o -name '*.rlib.c' \) -delete

# Store root ownership rather than whoever packed it: the seed unpacks at / on another
# machine, and preserving a local uid hands /opt/mrustc to whichever user happens to hold
# that number there.
say "Writing $OUT"
tar czf "$OUT" --owner=0 --group=0 --numeric-owner -C "$STAGE" opt
say "Seed ready: $(du -h "$OUT" | cut -f1)"
cat <<TXT
    Unpack on the runner with:  sudo tar xzf $(basename "$OUT") -C /
    Then build with:
      SOL9_BIN=/opt/sol9/bin SOL9_SYSROOT=/opt/sol9/sysroot \\
      SOL9_LIBGCC=/opt/sol9/$TARGET/lib/sparcv9/libgcc_s.so.1 \\
      MRUSTC_DIR=/opt/mrustc scripts/build-sol9.sh
TXT
