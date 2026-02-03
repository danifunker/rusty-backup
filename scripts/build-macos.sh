#!/bin/bash
# Script to build and package Rusty Backup for macOS with daemon included

set -e

# Detect architecture
ARCH=$(uname -m)
if [ "$ARCH" = "arm64" ]; then
    TARGET="aarch64-apple-darwin"
elif [ "$ARCH" = "x86_64" ]; then
    TARGET="x86_64-apple-darwin"
else
    echo "Unsupported architecture: $ARCH"
    exit 1
fi

echo "Building for macOS ($ARCH)..."

# Build main app
echo "Building main application..."
cargo build --release --target "$TARGET"

# Build daemon
echo "Building privileged helper daemon..."
cargo build --release --target "$TARGET" --bin rusty-backup-helper --features macos-daemon

# Create app bundle
APP_NAME="Rusty Backup"
BUNDLE_NAME="Rusty Backup.app"
VERSION=$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[0].version')

echo "Creating app bundle: $BUNDLE_NAME"

# Clean old bundle if exists
rm -rf "$BUNDLE_NAME"

# Create structure
mkdir -p "${BUNDLE_NAME}/Contents/MacOS"
mkdir -p "${BUNDLE_NAME}/Contents/Resources"
mkdir -p "${BUNDLE_NAME}/Contents/Library/LaunchDaemons"

# Copy main binary
echo "Copying main binary..."
cp "target/$TARGET/release/rusty-backup" "${BUNDLE_NAME}/Contents/MacOS/"
chmod +x "${BUNDLE_NAME}/Contents/MacOS/rusty-backup"

# Copy daemon binary
echo "Copying daemon binary..."
cp "target/$TARGET/release/rusty-backup-helper" "${BUNDLE_NAME}/Contents/Library/LaunchDaemons/com.rustybackup.helper"
chmod +x "${BUNDLE_NAME}/Contents/Library/LaunchDaemons/com.rustybackup.helper"

# Copy daemon plist
echo "Copying daemon plist..."
cp "assets/com.rustybackup.helper.plist" "${BUNDLE_NAME}/Contents/Resources/"

# Copy icon if available
if [ -f "assets/icons/icon.icns" ]; then
    echo "Copying icon..."
    cp "assets/icons/icon.icns" "${BUNDLE_NAME}/Contents/Resources/AppIcon.icns"
fi

# Create Info.plist
echo "Creating Info.plist..."
cat > "${BUNDLE_NAME}/Contents/Info.plist" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleName</key>
    <string>${APP_NAME}</string>
    <key>CFBundleDisplayName</key>
    <string>${APP_NAME}</string>
    <key>CFBundleIdentifier</key>
    <string>io.github.dani.rusty-backup</string>
    <key>CFBundleVersion</key>
    <string>${VERSION}</string>
    <key>CFBundleShortVersionString</key>
    <string>${VERSION}</string>
    <key>CFBundleExecutable</key>
    <string>rusty-backup</string>
    <key>CFBundleIconFile</key>
    <string>AppIcon.icns</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>NSHighResolutionCapable</key>
    <true/>
    <key>LSMinimumSystemVersion</key>
    <string>10.13</string>
</dict>
</plist>
EOF

# Ad-hoc code sign
echo "Code signing..."
codesign --force --deep --sign - "${BUNDLE_NAME}"

echo ""
echo "✅ Build complete!"
echo ""
echo "App bundle: $BUNDLE_NAME"
echo "You can now run: open $BUNDLE_NAME"
echo ""
echo "Bundle contents:"
tree "$BUNDLE_NAME" || find "$BUNDLE_NAME" -type f
