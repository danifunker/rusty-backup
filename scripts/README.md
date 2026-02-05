# Build Scripts

This directory contains scripts for building and packaging Rusty Backup for different platforms.

## macOS

### build-macos.sh

Builds a complete macOS .app bundle using sudo elevation for disk access.

**Usage:**
```bash
./scripts/build-macos.sh
```

**What it does:**
1. Detects your Mac architecture (arm64 or x86_64)
2. Builds the main Rusty Backup application (release mode)
3. Creates a `.app` bundle with proper structure:
   - `Contents/MacOS/rusty-backup` - Main application
   - `Contents/Resources/AppIcon.icns` - Application icon
   - `Contents/Info.plist` - Bundle metadata
4. Code signs the bundle (ad-hoc signature for local use)

**Output:**
- `Rusty Backup.app` - Ready to use application bundle

**Running the app:**
```bash
open 'Rusty Backup.app'
```

Or drag it to `/Applications` for permanent installation. The app uses sudo for disk access and will prompt for your password when needed.

## CI/CD

The GitHub Actions workflow (`.github/workflows/release.yml`) automatically builds for all platforms:
- **Windows**: x86 and x64 builds (ZIP archives)
- **macOS**: arm64 and x64 builds (DMG images)
- **Linux**: AppImage, deb, rpm, and Arch packages

Release artifacts are automatically created on push to main/master branch.
