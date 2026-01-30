#!/bin/bash
# scripts/generate-icon.sh
# Converts icon-original.png to all required formats for cross-platform releases

set -e

ORIGINAL="assets/icon-original.png"
ASSETS_DIR="assets/icons"

# Check if ImageMagick is installed
if command -v magick &> /dev/null; then
    MAGICK_CMD="magick"
elif command -v convert &> /dev/null; then
    MAGICK_CMD="convert"
else
    echo "Error: ImageMagick is not installed."
    echo "Install it with:"
    echo "  macOS: brew install imagemagick"
    echo "  Linux: sudo apt-get install imagemagick"
    exit 1
fi

# Check if original icon exists
if [ ! -f "$ORIGINAL" ]; then
    echo "Error: $ORIGINAL not found"
    exit 1
fi

# Check if original has alpha channel
echo "Checking original icon..."
if $MAGICK_CMD identify -format "%[channels]" "$ORIGINAL" | grep -q "a"; then
    echo "✓ Original icon has alpha channel (transparency)"
else
    echo "⚠ Warning: Original icon does not have alpha channel"
    echo "  The icon may not be transparent"
fi

# Create assets directory
mkdir -p "$ASSETS_DIR"

echo "Converting $ORIGINAL to multiple formats..."

# Generate PNG icons at various sizes (preserving transparency)
SIZES=(16 32 48 64 128 256 512 1024)
for size in "${SIZES[@]}"; do
    echo "Creating ${size}x${size} PNG..."
    $MAGICK_CMD "$ORIGINAL" -background none -alpha on -resize ${size}x${size} -extent ${size}x${size} PNG32:"$ASSETS_DIR/icon-${size}.png"
done

# Generate Windows ICO file (multi-size with transparency)
echo "Creating Windows ICO file..."
$MAGICK_CMD "$ORIGINAL" \
    \( -clone 0 -background none -resize 16x16 -extent 16x16 \) \
    \( -clone 0 -background none -resize 32x32 -extent 32x32 \) \
    \( -clone 0 -background none -resize 48x48 -extent 48x48 \) \
    \( -clone 0 -background none -resize 64x64 -extent 64x64 \) \
    \( -clone 0 -background none -resize 128x128 -extent 128x128 \) \
    \( -clone 0 -background none -resize 256x256 -extent 256x256 \) \
    -delete 0 -colors 256 "$ASSETS_DIR/icon.ico"

# Generate macOS ICNS file (requires additional tools on macOS)
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "Creating macOS ICNS file..."
    
    # Create iconset directory
    ICONSET="$ASSETS_DIR/icon.iconset"
    mkdir -p "$ICONSET"
    
    # Generate all required sizes for ICNS (with transparency)
    $MAGICK_CMD "$ORIGINAL" -background none -resize 16x16 -extent 16x16 "$ICONSET/icon_16x16.png"
    $MAGICK_CMD "$ORIGINAL" -background none -resize 32x32 -extent 32x32 "$ICONSET/icon_16x16@2x.png"
    $MAGICK_CMD "$ORIGINAL" -background none -resize 32x32 -extent 32x32 "$ICONSET/icon_32x32.png"
    $MAGICK_CMD "$ORIGINAL" -background none -resize 64x64 -extent 64x64 "$ICONSET/icon_32x32@2x.png"
    $MAGICK_CMD "$ORIGINAL" -background none -resize 128x128 -extent 128x128 "$ICONSET/icon_128x128.png"
    $MAGICK_CMD "$ORIGINAL" -background none -resize 256x256 -extent 256x256 "$ICONSET/icon_128x128@2x.png"
    $MAGICK_CMD "$ORIGINAL" -background none -resize 256x256 -extent 256x256 "$ICONSET/icon_256x256.png"
    $MAGICK_CMD "$ORIGINAL" -background none -resize 512x512 -extent 512x512 "$ICONSET/icon_256x256@2x.png"
    $MAGICK_CMD "$ORIGINAL" -background none -resize 512x512 -extent 512x512 "$ICONSET/icon_512x512.png"
    $MAGICK_CMD "$ORIGINAL" -background none -resize 1024x1024 -extent 1024x1024 "$ICONSET/icon_512x512@2x.png"
    
    # Convert to ICNS
    iconutil -c icns "$ICONSET"
    
    # Clean up
    rm -rf "$ICONSET"
else
    echo "Skipping ICNS generation (macOS only)"
    echo "Note: The workflow will use PNG icons for macOS"
fi

# Create a simple AppImage-ready icon structure
echo "Creating AppImage icon structure..."
mkdir -p "$ASSETS_DIR/hicolor/256x256/apps"
cp "$ASSETS_DIR/icon-256.png" "$ASSETS_DIR/hicolor/256x256/apps/rusty-backup.png"

# Copy main icon for easy reference
cp "$ASSETS_DIR/icon-256.png" "$ASSETS_DIR/icon.png"

echo ""
echo "✓ Icon conversion complete!"
echo ""
echo "Generated files:"
ls -lh "$ASSETS_DIR"
echo ""
echo "Icon files are ready for:"
echo "  • Windows: $ASSETS_DIR/icon.ico"
echo "  • macOS: $ASSETS_DIR/icon.png (or icon.icns if on macOS)"
echo "  • Linux AppImage: $ASSETS_DIR/hicolor/256x256/apps/rusty-backup.png"
echo "  • Snap: $ASSETS_DIR/icon.png"