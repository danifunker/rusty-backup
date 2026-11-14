#!/bin/sh
# Builds "Rusty Backup.app" for Mac OS X 10.7+ (x86_64): the Rust half on Rust 1.73, the
# Objective-C host with clang, linked into one executable. See docs/cocoa_vintage_shell_plan.md.
#
#   frontends-vintage/cocoa/build.sh [debug|release] [x86_64|arm64]
#       -> frontends-vintage/cocoa/build/<arch>/Rusty Backup.app
# x86_64 is the shipped 10.7 build; arm64 (11.0 floor) only smoke-tests the host on a Mac without Rosetta.
set -eu

PROFILE="${1:-release}"
ARCH="${2:-x86_64}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
HERE="$ROOT/frontends-vintage/cocoa"
OUT="$HERE/build/$ARCH"
FEATURES="native-zstd,remote,rust173-polyfill,yaml,tui"
case "$ARCH" in
    x86_64) TARGET=x86_64-apple-darwin; MIN=10.7 ;;
    arm64) TARGET=aarch64-apple-darwin; MIN=11.0 ;;
    *) echo "unknown arch $ARCH" >&2; exit 2 ;;
esac
export MACOSX_DEPLOYMENT_TARGET=$MIN

# A Homebrew rustc on PATH shadows rustup's; pin both halves of the 1.73 toolchain (CI has no shadow).
TC="$HOME/.rustup/toolchains/1.73.0-$(uname -m | sed 's/arm64/aarch64/')-apple-darwin"
if [ -x "$TC/bin/cargo" ]; then
    CARGO="$TC/bin/cargo"; export RUSTC="$TC/bin/rustc"
else
    CARGO="cargo +1.73.0"
fi

PFLAG=""; [ "$PROFILE" = release ] && PFLAG="--release"
$CARGO build $PFLAG -p rusty-backup-cocoa \
    --manifest-path "$ROOT/rb-cli-vintage/Cargo.toml" \
    --target "$TARGET" --no-default-features --features "$FEATURES" \
    --ignore-rust-version --locked
LIB="$ROOT/rb-cli-vintage/target/$TARGET/$PROFILE/librb_cocoa.a"

# APIs newer than the floor are errors; deprecated ones are allowed, since 10.4 has only the old names. No blocks in our code (gcc 4.0 on PPC has none).
CFLAGS="-arch $ARCH -mmacosx-version-min=$MIN -fno-objc-arc -O2 -Wall -Werror -Wno-deprecated-declarations -Werror=unguarded-availability -Werror=objc-method-access"
APP="$OUT/Rusty Backup.app"
rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources" "$OUT/obj"
for m in main host; do clang $CFLAGS -c "$HERE/host/$m.m" -o "$OUT/obj/$m.o"; done
# The frameworks the engine's device layer and std need (rustc --print native-static-libs).
clang -arch $ARCH -mmacosx-version-min=$MIN -o "$APP/Contents/MacOS/Rusty Backup" \
    "$OUT/obj/main.o" "$OUT/obj/host.o" "$LIB" \
    -framework Cocoa -framework IOKit -framework DiskArbitration -framework CoreFoundation \
    -framework Security -liconv -Wl,-dead_strip

# The version the engine bakes in (build.rs): RELEASE_VERSION in CI, else the vintage manifest's.
VERSION="${RELEASE_VERSION:-$(sed -n 's/^version = "\(.*\)"/\1/p' "$ROOT/rb-cli-vintage/Cargo.toml" | head -1)}"
sed "s/@VERSION@/$VERSION/g" "$HERE/host/Info.plist" > "$APP/Contents/Info.plist"
printf 'APPL????' > "$APP/Contents/PkgInfo"
cp "$ROOT/assets/icons/icon.icns" "$APP/Contents/Resources/icon.icns"
# The toolbar's screen icons and the listing's row icons (Tabler).
cp "$ROOT"/assets/icons/toolbar/png/*-48.png "$APP/Contents/Resources/"
for i in file file-plus file-minus folder folder-plus folder-minus folder-open; do
    cp "$ROOT/assets/icons/rows/png/$i-32.png" "$APP/Contents/Resources/"
done
codesign -s - --force "$APP" 2>/dev/null || true

otool -l "$APP/Contents/MacOS/Rusty Backup" | grep -A3 -E 'LC_VERSION_MIN_MACOSX|LC_BUILD_VERSION' | grep -E 'version|minos' | head -2
echo "Built $APP ($VERSION)"
