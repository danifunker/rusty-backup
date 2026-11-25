#!/usr/bin/env bash
# Writes the release page's Downloads section to stdout: one table row per downloadable file,
# grouped by operating system. Usage: release-downloads.sh VERSION OWNER/REPO [ASSET_DIR]
# With ASSET_DIR, every file the page links to is checked there and a missing one is warned
# about (a CI annotation), without failing the release.
set -euo pipefail
VER=$1
REPO=$2
ASSETS=${3:-}
BASE="https://github.com/${REPO}/releases/download/v${VER}"
DB_URL="https://github.com/${REPO}/releases/latest/download/rusty-backup.json.zip"
WIKI="https://github.com/${REPO}/wiki/PPC-Download"
DOCS="https://github.com/${REPO}/blob/main/docs"

# row LABEL ARCH RUNS-ON FILE: one download.
row() {
  local file=$4
  if [ -n "$ASSETS" ] && [ ! -f "$ASSETS/$file" ]; then
    echo "::warning::release asset missing from the downloads page: $file" >&2
  fi
  printf '| [%s](%s/%s) | %s | %s |\n' "$1" "$BASE" "$file" "$2" "$3"
}
table() {
  printf '| Download | Architecture | Runs on |\n|---|---|---|\n'
}

cat <<EOF
## Downloads

**Most people want:** Windows -> the **x64 installer** · Mac -> the **Apple Silicon** or **Intel** \`.dmg\` · Linux -> the **x64 AppImage**.

Each platform has the desktop app first, then \`rb-cli\`, the command-line tool (with its full-screen \`rb-cli tui\` terminal UI). Every \`rb-cli\` download holds a single \`rb-cli\` executable (\`rb-cli.exe\` on Windows).

### Windows

EOF
table
row "Windows app — installer" "x64" "Windows 11 / 10" "Rusty-Backup-Win32-Setup-windows-x64-${VER}.exe"
row "Windows app — portable zip" "x64" "Windows 11 / 10" "Rusty-Backup-Win32-windows-x64-${VER}.zip"
row "Windows app — installer" "ARM64" "Windows 11" "Rusty-Backup-Win32-Setup-windows-arm64-${VER}.exe"
row "Windows app — portable zip" "ARM64" "Windows 11" "Rusty-Backup-Win32-windows-arm64-${VER}.zip"
row "Windows app — installer" "x86 (32-bit)" "Windows 10" "Rusty-Backup-Win32-Setup-windows-x86-${VER}.exe"
row "Windows app — portable zip" "x86 (32-bit)" "Windows 10" "Rusty-Backup-Win32-windows-x86-${VER}.zip"
row "rb-cli" "x64" "Windows 11 / 10" "rb-cli-windows-x64-${VER}.zip"
row "rb-cli" "ARM64" "Windows 11" "rb-cli-windows-arm64-${VER}.zip"
row "rb-cli" "x86 (32-bit)" "Windows 10" "rb-cli-windows-x86-${VER}.zip"
row "rb-cli (vintage)" "x64" "Windows 7 / 8 / 8.1" "rb-cli-win7-x64-${VER}.zip"
row "rb-cli (vintage)" "x86 (32-bit)" "Windows 7 / 8 / 8.1" "rb-cli-win7-x86-${VER}.zip"
cat <<EOF

<sub>The Windows app is the native Win32 one; x86 is for 32-bit Windows 10, the last Windows with 32-bit support. The Windows 7 / 8 \`rb-cli\` is built with Rust 1.73 (the last toolchain that targets Windows 7) with the CRT linked in, so it runs on a stock Windows 7; it has no CHD or physical-optical support.</sub>

### macOS

EOF
table
row "Mac app (AppKit) — .dmg" "Apple Silicon (arm64)" "macOS 10.13 High Sierra and later" "Rusty-Backup-AppKit-macos-arm64-${VER}.dmg"
row "Mac app (AppKit) — .dmg" "Intel (x64)" "macOS 10.13 High Sierra and later" "Rusty-Backup-AppKit-macos-x64-${VER}.dmg"
row "Mac app (Cocoa, preview) — .dmg" "Intel (x64)" "Mac OS X 10.7 Lion and later (made for 10.7 – 10.11)" "Rusty-Backup-Cocoa-macos-vintage-x64-${VER}.dmg"
row "rb-cli" "Apple Silicon (arm64)" "macOS 10.13 and later" "rb-cli-macos-arm64-${VER}.zip"
row "rb-cli" "Intel (x64)" "macOS 10.13 and later" "rb-cli-macos-x64-${VER}.zip"
row "rb-cli (vintage)" "Intel (x64)" "Mac OS X 10.7 and later" "rb-cli-macos-vintage-x64-${VER}.zip"
cat <<EOF
| [Mac OS X on PowerPC — wiki page](${WIKI}) | PowerPC (G3 / G4 / G5) | Mac OS X 10.4 Tiger – 10.5 Leopard |

<sub>The AppKit app is the native Mac app. The Cocoa app is its preview for Macs too old for it (10.7 – 10.11). The vintage \`rb-cli\` and the Cocoa app are built with Rust 1.73, are ad-hoc signed and not notarized (run \`xattr -dr com.apple.quarantine\` on them first), and have no CHD or physical-optical support. The PowerPC Mac OS X builds come from separate hardware and update less often, so they live on the wiki.</sub>

### Linux

EOF
table
row "Linux app — AppImage" "x64" "any distribution (GTK4 bundled)" "Rusty-Backup-GTK-linux-x64-${VER}.AppImage"
row "Linux app — AppImage" "arm64" "any distribution (GTK4 bundled)" "Rusty-Backup-GTK-linux-arm64-${VER}.AppImage"
row "Linux app — Debian / Ubuntu package (.deb)" "x64" "Ubuntu 24.04+, Debian 13" "Rusty-Backup-GTK-linux-x64-${VER}.deb"
row "Linux app — Debian / Ubuntu package (.deb)" "arm64" "Ubuntu 24.04+, Debian 13" "Rusty-Backup-GTK-linux-arm64-${VER}.deb"
row "Linux app — Debian / Ubuntu package (.deb)" "RISC-V 64 (riscv64gc)" "Ubuntu 24.04+, Debian 13" "Rusty-Backup-GTK-linux-riscv64-${VER}.deb"
row "Linux app — Fedora / RHEL / openSUSE package (.rpm)" "x64" "Fedora 40+" "Rusty-Backup-GTK-linux-x64-${VER}.rpm"
row "Linux app — Fedora / RHEL / openSUSE package (.rpm)" "arm64" "Fedora 40+" "Rusty-Backup-GTK-linux-arm64-${VER}.rpm"
row "Linux app — Fedora / RHEL / openSUSE package (.rpm)" "RISC-V 64 (riscv64gc)" "Fedora 40+" "Rusty-Backup-GTK-linux-riscv64-${VER}.rpm"
row "Linux app — Arch Linux package (.pkg.tar.zst)" "x64" "Arch Linux" "Rusty-Backup-GTK-linux-x64-${VER}.pkg.tar.zst"
row "Linux app — Arch Linux package (.pkg.tar.zst)" "arm64" "Arch Linux (aarch64)" "Rusty-Backup-GTK-linux-arm64-${VER}.pkg.tar.zst"
row "Linux app — tarball (.tar.gz)" "RISC-V 64 (riscv64gc)" "GTK 4.14+ and libadwaita 1.5+" "Rusty-Backup-GTK-linux-riscv64-${VER}.tar.gz"
row "rb-cli" "x64" "glibc 2.31+ (Ubuntu 20.04+, Debian 11+, Fedora 32+, RHEL 9+)" "rb-cli-linux-x64-${VER}.tar.gz"
row "rb-cli" "arm64" "glibc 2.39+ (Ubuntu 24.04+)" "rb-cli-linux-arm64-${VER}.tar.gz"
row "rb-cli" "RISC-V 64 (riscv64gc)" "glibc 2.39+ (Ubuntu 24.04+)" "rb-cli-linux-riscv64-${VER}.tar.gz"
row "rb-cli" "PowerPC 64, big-endian (G5 / POWER)" "glibc 2.39+" "rb-cli-linux-ppc64-${VER}.tar.gz"
row "rb-cli" "PowerPC 32, big-endian (G3 / G4)" "glibc 2.39+" "rb-cli-linux-ppc32-${VER}.tar.gz"
row "rb-cli (static)" "x64" "any Linux, any glibc" "rb-cli-linux-musl-x64-${VER}.tar.gz"
row "rb-cli (static)" "arm64" "any Linux, any glibc" "rb-cli-linux-musl-arm64-${VER}.tar.gz"
row "rb-cli (static)" "i686 (Pentium Pro and up)" "any 32-bit x86 Linux" "rb-cli-linux-musl-i686-${VER}.tar.gz"
row "rb-cli (static)" "i586 (Pentium, K6, Cyrix and up)" "any 32-bit x86 Linux" "rb-cli-linux-musl-i586-${VER}.tar.gz"
row "rb-cli (static)" "PowerPC 64, big-endian (G5 / POWER)" "any Linux, any glibc" "rb-cli-linux-musl-ppc64-${VER}.tar.gz"
row "rb-cli (static)" "PowerPC 32, big-endian (G3 / G4)" "any Linux, any glibc" "rb-cli-linux-musl-ppc32-${VER}.tar.gz"
cat <<EOF

<sub>The Linux app is the native GTK4 + libadwaita one, installed as \`rusty-backup-gtk\`. The AppImage bundles GTK4 and runs on older distributions; the packages and the RISC-V tarball need GTK 4.14 and libadwaita 1.5 or newer. The static \`rb-cli\` builds have no dynamic dependencies at all, for systems whose glibc is too old for the others; the x64 and arm64 ones read and write CHD, the 32-bit and PowerPC ones don't (they and the glibc PowerPC and RISC-V builds use the slim feature set: no CHD or optical, but every filesystem and \`.zst\` / \`.raw\` / \`.vhd\` / \`.img\`). ppc32 is a best-effort build of a Rust tier-3 target.</sub>

### Other systems

EOF
table
row "rb-cli-mini" "armv7 hardfloat (Cortex-A9)" "MiSTer FPGA (glibc 2.31)" "rb-cli-mini-armv7-linux-${VER}.tar.gz"
row "rb-cli" "SPARC64" "Solaris 9" "rb-cli-solaris9-sparc64-${VER}.tar.gz"
cat <<EOF

<sub>\`rb-cli-mini\` is a slim \`rb-cli\` for the MiSTer's Cyclone V SoC, with CHD, optical and the TUI; its tarball's \`install.sh\` puts it at \`/media/fat/Scripts/rb-cli\`.</sub>

#### Install on a MiSTer FPGA (auto-updating)

Add Rusty Backup to the **MiSTer Downloader** as a custom database and it installs, and keeps itself updated, from the Scripts / Update menu. Add this to \`/media/fat/downloader.ini\` on the SD card:

\`\`\`ini
[danifunker/rusty-backup]
db_url = '${DB_URL}'
\`\`\`

Then run **Scripts > update** (or **Downloader**). It puts \`rb-cli\` and \`rb-daemon.sh\` in \`/media/fat/Scripts\`; open **rb-daemon** from the Scripts menu to start the network daemon and enable it at boot.

### Boot the vintage machine itself

For 486 / Pentium-class PCs too old for the desktop app: boot the Rusty Backup backup / restore menu on the machine itself (VGA or serial console), with a broad set of vintage network, SCSI and IDE drivers. Pick **i586** unless the machine is a Pentium Pro or newer; a true 486 uses **cb-dos**.

| Download | Architecture | Boots from |
|---|---|---|
EOF
boot() {
  local file=$4
  if [ -n "$ASSETS" ] && [ ! -f "$ASSETS/$file" ]; then
    echo "::warning::release asset missing from the downloads page: $file" >&2
  fi
  printf '| [%s](%s/%s) | %s | %s |\n' "$1" "$BASE" "$file" "$2" "$3"
}
boot "Appliance ISO" "i586 (Pentium and up)" "CD-ROM, or USB / CF (write the same file with \`dd\`)" "rusty-backup-appliance-i586-${VER}.iso"
boot "Appliance ISO" "i686 (Pentium Pro and up)" "CD-ROM, or USB / CF (write the same file with \`dd\`)" "rusty-backup-appliance-i686-${VER}.iso"
boot "PXE bundle" "i586 (Pentium and up)" "the network (TFTP / PXE), for a diskless backup station" "rusty-backup-appliance-pxe-i586-${VER}.tar.gz"
boot "PXE bundle" "i686 (Pentium Pro and up)" "the network (TFTP / PXE), for a diskless backup station" "rusty-backup-appliance-pxe-i686-${VER}.tar.gz"
boot "GRUB boot floppy" "i586 (Pentium and up)" "a floppy that loads the appliance off the CD (BIOSes that can't boot a CD)" "rusty-backup-grub-floppy-i586-${VER}.img"
boot "GRUB boot floppy" "i686 (Pentium Pro and up)" "a floppy that loads the appliance off the CD (BIOSes that can't boot a CD)" "rusty-backup-grub-floppy-i686-${VER}.img"
boot "cb-dos floppy" "x86 (486 class)" "a FreeDOS floppy: no Linux, for a true 486 or where Linux is too heavy" "cbdos-freedos-${VER}.img"
boot "cb-dos CD" "x86 (486 class)" "a FreeDOS CD: no Linux, for a true 486 or where Linux is too heavy" "cbdos-${VER}.iso"
cat <<EOF

<sub>Which cards work and how to add more: [\`docs/appliance_hardware_support.md\`](${DOCS}/appliance_hardware_support.md). cb-dos background: [\`docs/cb_dos.md\`](${DOCS}/cb_dos.md).</sub>

---
EOF
# Files the release ships that the table doesn't link: the MiSTer Downloader's own.
if [ -n "$ASSETS" ]; then
  for f in rb-cli-mini-armv7 rb-daemon.sh rusty-backup.json.zip; do
    [ -f "$ASSETS/$f" ] || echo "::warning::release asset missing: $f" >&2
  done
fi
