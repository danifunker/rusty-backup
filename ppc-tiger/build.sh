#!/bin/sh
# build.sh — Build rusty-backup for Mac OS X Tiger (PowerPC)
#
# Requirements:
#   - Mac OS X Tiger 10.4.x with Xcode 2.x (gcc-4.0)
#   - Or any PowerPC Mac with gcc-4.0 installed
#
# This produces a ~58KB native Mach-O PPC binary.
#
# Usage:
#   ./build.sh              Build CLI only
#   ./build.sh --gui        Build CLI + Carbon GUI
#   ./build.sh --app        Build CLI + GUI + .app bundle

set -e

CC="${CC:-gcc-4.0}"
CFLAGS="-std=c99 -O2"

# If root disk is full, set TMPDIR to a volume with space
if [ -z "$TMPDIR" ]; then
    for vol in "/Volumes/Macintosh HD" "/Volumes/Macintosh Hard Drve" "/tmp"; do
        if [ -d "$vol" ] && df "$vol" 2>/dev/null | tail -1 | awk '{exit ($4 < 100000)}'; then
            export TMPDIR="$vol"
            echo "Using TMPDIR=$TMPDIR"
            break
        fi
    done
fi

BUILDDIR="$(pwd)"

echo "=== Rusty Backup PPC Build ==="
echo "Compiler: $CC"
echo ""

# CRC32C stub (referenced by runtime but not needed for CLI)
cat > /tmp/crc32c_stub.c << 'EOF'
unsigned int crc32c(unsigned int crc, const void *buf, int len) {
    (void)buf; (void)len; return crc;
}
EOF

echo "Compiling rust_cli_real.c..."
$CC $CFLAGS -c rust_cli_real.c -o rust_cli_real.o

echo "Compiling rust_runtime_v2.c..."
$CC $CFLAGS -c rust_runtime_v2.c -o rust_runtime_v2.o

echo "Compiling crc32c stub..."
$CC $CFLAGS -c /tmp/crc32c_stub.c -o crc32c_stub.o

echo "Linking rusty-backup-ppc..."
/usr/bin/ld -o rusty-backup-ppc \
    /usr/lib/crt1.o \
    -L/usr/lib \
    -L/usr/lib/gcc/powerpc-apple-darwin8/4.0.1 \
    -lSystem -lSystemStubs -lgcc -lz \
    -read_only_relocs suppress \
    rust_cli_real.o rust_runtime_v2.o crc32c_stub.o

echo ""
echo "Built: rusty-backup-ppc ($(wc -c < rusty-backup-ppc | tr -d ' ') bytes)"
./rusty-backup-ppc --version

# GUI build
if [ "$1" = "--gui" ] || [ "$1" = "--app" ]; then
    echo ""
    echo "Compiling Carbon GUI..."
    $CC $CFLAGS -c rusty_backup_gui.c -o rusty_backup_gui.o

    echo "Linking rusty-backup-gui..."
    /usr/bin/ld -o rusty-backup-gui \
        /usr/lib/crt1.o \
        -L/usr/lib \
        -L/usr/lib/gcc/powerpc-apple-darwin8/4.0.1 \
        -lSystem -lSystemStubs -lgcc \
        -framework Carbon -lpthread \
        -read_only_relocs suppress \
        rusty_backup_gui.o

    echo "Built: rusty-backup-gui ($(wc -c < rusty-backup-gui | tr -d ' ') bytes)"
fi

# .app bundle
if [ "$1" = "--app" ]; then
    echo ""
    echo "Creating RustyBackup.app bundle..."
    APP="RustyBackup.app"
    rm -rf "$APP"
    mkdir -p "$APP/Contents/MacOS"
    mkdir -p "$APP/Contents/Resources"
    cp Info.plist "$APP/Contents/"
    cp rusty-backup-gui "$APP/Contents/MacOS/RustyBackup"
    cp rusty-backup-ppc "$APP/Contents/Resources/"
    echo "APPL????" > "$APP/Contents/PkgInfo"
    echo "Built: $APP"
fi

echo ""
echo "Done."
