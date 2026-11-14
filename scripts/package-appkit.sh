#!/bin/bash
# Package the AppKit shell (frontends/appkit) as its own .app and DMG (docs/appkit_shell_plan.md,
# slice 9). CI's build-appkit job runs it; it works locally too.
#
#   scripts/package-appkit.sh <binary> <version> <arch> [out-dir]
#
# The DMG is Rusty-Backup-AppKit-macos-<arch>-<version>.dmg, the name the in-app updater looks for
# (src/update.rs, mac::dmg_prefix). The bundle id is io.github.danifunker.rusty-backup, so it
# installs beside the egui app. Its document types come from DISK_IMAGE_EXTS at the Alternate
# rank: Finder's Open With lists it, and it never takes over a file type by itself.
#
# Signing: set SIGN_IDENTITY to a Developer ID Application identity to sign with it (hardened
# runtime); otherwise the bundle is signed ad hoc. Notarization stays with the caller.

set -euo pipefail

BIN=${1:?the rusty-backup-appkit binary}
VERSION=${2:?the version}
ARCH=${3:?arm64 or x64}
OUT=${4:-.}

ROOT=$(cd "$(dirname "$0")/.." && pwd)
APP_NAME="Rusty Backup AppKit"
BUNDLE="$OUT/$APP_NAME.app"
DMG="$OUT/Rusty-Backup-AppKit-macos-$ARCH-$VERSION.dmg"

rm -rf "$BUNDLE"
mkdir -p "$BUNDLE/Contents/MacOS" "$BUNDLE/Contents/Resources"
cp "$BIN" "$BUNDLE/Contents/MacOS/rusty-backup-appkit"
chmod +x "$BUNDLE/Contents/MacOS/rusty-backup-appkit"
if [ -f "$ROOT/assets/icons/icon.icns" ]; then
    cp "$ROOT/assets/icons/icon.icns" "$BUNDLE/Contents/Resources/AppIcon.icns"
fi

python3 - "$ROOT/src/model/file_types.rs" "$BUNDLE/Contents/Info.plist" "$APP_NAME" "$VERSION" <<'PY'
import plistlib, re, sys
src, out, name, version = sys.argv[1:5]
text = open(src, encoding="utf-8").read()
body = text.split("pub const DISK_IMAGE_EXTS: &[&str] = &[", 1)[1].split("];", 1)[0]
body = "\n".join(l.split("//", 1)[0] for l in body.splitlines())
exts = sorted({e.lower() for e in re.findall(r'"([^"]+)"', body)})
plist = {
    "CFBundleName": name,
    "CFBundleDisplayName": name,
    "CFBundleIdentifier": "io.github.danifunker.rusty-backup",
    "CFBundleVersion": version,
    "CFBundleShortVersionString": version,
    "CFBundleExecutable": "rusty-backup-appkit",
    "CFBundleIconFile": "AppIcon.icns",
    "CFBundlePackageType": "APPL",
    "NSHighResolutionCapable": True,
    "LSMinimumSystemVersion": "10.13",
    "CFBundleDocumentTypes": [{
        "CFBundleTypeName": "Disk Image",
        "CFBundleTypeRole": "Viewer",
        "LSHandlerRank": "Alternate",
        "CFBundleTypeExtensions": exts,
    }],
}
with open(out, "wb") as f:
    plistlib.dump(plist, f)
print(f"Info.plist: {len(exts)} document extensions")
PY

if [ -n "${SIGN_IDENTITY:-}" ]; then
    codesign --force --options runtime --timestamp --sign "$SIGN_IDENTITY" "$BUNDLE"
else
    echo "No SIGN_IDENTITY: signing ad hoc"
    codesign --force --deep --sign - "$BUNDLE"
fi
codesign --verify --deep --strict "$BUNDLE"

# Explicit size: hdiutil's own estimate can run out of room while copying (see release.yml).
STAGE=$(mktemp -d)
cp -R "$BUNDLE" "$STAGE/"
ln -s /Applications "$STAGE/Applications"
STAGE_KB=$(du -sk "$STAGE" | cut -f1)
SIZE_KB=$(( STAGE_KB * 14 / 10 + 204800 ))
RW="$OUT/appkit-rw.dmg"
rm -f "$RW" "$DMG"
hdiutil create -volname "$APP_NAME" -srcfolder "$STAGE" -fs HFS+ -format UDRW \
    -size "${SIZE_KB}k" -ov "$RW" >/dev/null
hdiutil convert "$RW" -format UDZO -ov -o "$DMG" >/dev/null
rm -rf "$RW" "$STAGE"
if [ -n "${SIGN_IDENTITY:-}" ]; then
    codesign --force --sign "$SIGN_IDENTITY" "$DMG"
fi
echo "Built $DMG"
