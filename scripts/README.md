# Build Scripts

This directory contains scripts for building and packaging Rusty Backup for different platforms.

## macOS

### build-macos.sh

Builds a complete macOS .app bundle with the privileged helper daemon included.

**Usage:**
```bash
./scripts/build-macos.sh
```

**What it does:**
1. Detects your Mac architecture (arm64 or x86_64)
2. Builds the main Rusty Backup application (release mode)
3. Builds the privileged helper daemon (`rusty-backup-helper`)
4. Creates a `.app` bundle with proper structure:
   - `Contents/MacOS/rusty-backup` - Main application
   - `Contents/Library/LaunchDaemons/rusty-backup-helper` - Privileged daemon
   - `Contents/Resources/com.rustybackup.helper.plist` - Daemon configuration
   - `Contents/Resources/AppIcon.icns` - Application icon
   - `Contents/Info.plist` - Bundle metadata
5. Code signs the bundle (ad-hoc signature for local use)

**Output:**
- `Rusty-Backup.app` - Ready to use application bundle

**Running the app:**
```bash
open Rusty-Backup.app
```

Or drag it to `/Applications` for permanent installation.

**Testing daemon installation:**
1. Open the app
2. Click "Install Helper" in the top bar
3. Enter your admin password when prompted
4. The daemon will be installed to `/Library/PrivilegedHelperTools/`
5. Check Settings to see daemon status

## CI/CD

The GitHub Actions workflow (`.github/workflows/release.yml`) automatically builds for all platforms:
- **Windows**: x86 and x64 builds (ZIP archives)
- **macOS**: arm64 and x64 builds (DMG images with daemon included)
- **Linux**: AppImage, deb, rpm, and Arch packages

Release artifacts are automatically created on push to main/master branch.
