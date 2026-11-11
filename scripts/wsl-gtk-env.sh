#!/bin/bash
# Set up the GTK flow tools under WSL (or any Ubuntu 24.04) without root, into ~/gtkenv.
# Needs GTK 4 dev packages, python3-gi and dbus already installed; adds what the GTK driver
# needs on top (AT-SPI typelib, Xvfb, xauth, xkbcomp, xdotool). Then run flows through
# scripts/wsl-gtk-run.sh. See regression-tests/FLOWS.md § "GTK flows under WSL".
set -euo pipefail

ENV_DIR=${GTKENV:-$HOME/gtkenv}
mkdir -p "$ENV_DIR/debs"
cd "$ENV_DIR/debs"
apt-get download gir1.2-atspi-2.0 xvfb xdotool libxdo3 xauth xserver-common libxfont2 \
  libfontenc1 x11-xkb-utils xkb-data libxkbfile1
for d in ./*.deb; do dpkg-deb -x "$d" "$ENV_DIR/root"; done

R=$ENV_DIR/root
cat > "$ENV_DIR/env.sh" <<ENV
export PATH=$R/usr/bin:\$PATH
export LD_LIBRARY_PATH=$R/usr/lib/x86_64-linux-gnu
export GI_TYPELIB_PATH=$R/usr/lib/x86_64-linux-gnu/girepository-1.0
ENV

. "$ENV_DIR/env.sh"
python3 -c 'import gi; gi.require_version("Atspi", "2.0"); from gi.repository import Atspi'
echo "gtkenv ready in $ENV_DIR"
