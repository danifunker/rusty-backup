# ARM64 Self-Hosted Runner Dependencies

## Required System Packages

### Core Build Tools
```bash
sudo apt-get update
sudo apt-get install -y \
  build-essential \
  pkg-config \
  curl \
  wget \
  git
```

### Rust Dependencies
```bash
# GTK3 (for egui/eframe GUI)
sudo apt-get install -y \
  libgtk-3-dev \
  libglib2.0-dev \
  libcairo2-dev \
  libpango1.0-dev \
  libgdk-pixbuf2.0-dev \
  libatk1.0-dev

# OpenSSL (for reqwest HTTPS)
sudo apt-get install -y \
  libssl-dev

# Additional X11/graphics libraries (may be needed by egui)
sudo apt-get install -y \
  libx11-dev \
  libxcursor-dev \
  libxrandr-dev \
  libxi-dev
```

### AppImage Creation
```bash
# No special packages needed - appimagetool is downloaded as AppImage
# We extract it to avoid FUSE requirement
# Just need wget (already in core tools above)
```

### Debian Package Creation (cargo-deb)
```bash
# These should already be present on Ubuntu
sudo apt-get install -y \
  dpkg-dev \
  liblzma-dev
```

### RPM Package Creation (cargo-generate-rpm)
```bash
sudo apt-get install -y \
  rpm
```

### Arch Package Creation (cargo-aur)
```bash
# cargo-aur can build packages without pacman/makepkg
# But for maximum compatibility, install:
sudo apt-get install -y \
  libarchive-tools \
  zstd
```

## Rust Toolchain

```bash
# Install rustup if not already installed
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Ensure latest stable
rustup update stable
rustup default stable

# Add ARM64 target (should already match host)
rustup target add aarch64-unknown-linux-gnu
```

## Cargo Package Tools

```bash
# Install packaging tools
cargo install cargo-deb
cargo install cargo-generate-rpm
cargo install cargo-aur
```

## Complete One-Liner Installation

```bash
# System packages
sudo apt-get update && sudo apt-get install -y \
  build-essential pkg-config curl wget git \
  libgtk-3-dev libglib2.0-dev libcairo2-dev libpango1.0-dev \
  libgdk-pixbuf2.0-dev libatk1.0-dev libssl-dev \
  libx11-dev libxcursor-dev libxrandr-dev libxi-dev \
  dpkg-dev liblzma-dev rpm libarchive-tools zstd

# Cargo tools (after Rust is installed)
cargo install cargo-deb cargo-generate-rpm cargo-aur
```

## Verification

After installation, verify everything is available:

```bash
# Check build tools
gcc --version
pkg-config --version
cargo --version

# Check GTK3
pkg-config --modversion gtk+-3.0

# Check packaging tools
cargo deb --version
cargo generate-rpm --version
cargo aur --version

# Check RPM tools
rpm --version

# Check compression tools
zstd --version
```

## Notes

- **GitHub Actions Cache**: The self-hosted runner will benefit from cargo caching between runs
- **Disk Space**: Ensure adequate space for build artifacts (recommend 20GB+ free)
- **Permissions**: Runner user should NOT require sudo for builds (only for initial setup)
- **Network**: Runner needs internet access to download dependencies during first build
