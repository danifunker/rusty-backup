#!/bin/bash
# scripts/add-transparency.sh
# Makes white/light background transparent in the original icon

set -e

ORIGINAL="assets/icon-original.png"
OUTPUT="assets/icon-original-transparent.png"

# Check if ImageMagick is installed
if command -v magick &> /dev/null; then
    MAGICK_CMD="magick"
elif command -v convert &> /dev/null; then
    MAGICK_CMD="convert"
else
    echo "Error: ImageMagick is not installed."
    exit 1
fi

# Check if original exists
if [ ! -f "$ORIGINAL" ]; then
    echo "Error: $ORIGINAL not found"
    exit 1
fi

echo "Adding transparency to icon..."
echo ""
echo "This will make white/light backgrounds transparent."
echo "Adjust the -fuzz percentage if needed (higher = more aggressive)"
echo ""

# Make white background transparent
# -fuzz 10% allows slight variations in white color
# Adjust this value if your background isn't pure white
$MAGICK_CMD "$ORIGINAL" -fuzz 10% -transparent white "$OUTPUT"

echo "✓ Created transparent version: $OUTPUT"
echo ""
echo "Check the output file. If it looks good:"
echo "  mv assets/icon-original-transparent.png assets/icon-original.png"
echo "  ./scripts/generate-icon.sh"
echo ""
echo "If too much was made transparent, try a lower -fuzz value (e.g., 5%)"
echo "If not enough, try a higher value (e.g., 15% or 20%)"
