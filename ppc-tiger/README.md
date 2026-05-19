# Rusty Backup — PowerPC Tiger Port

A native PowerPC port of [rusty-backup](https://github.com/danifunker/rusty-backup) for Mac OS X Tiger (10.4.x). Reimplements the core backup/restore engine in C, producing a ~58KB Mach-O binary that runs on G3/G4/G5 Macs.

## Features

All core rusty-backup functionality, running natively on Tiger:

- **Partition table support**: MBR (with EBR chain for logical partitions), APM, Superfloppy
- **Gzip compression**: `--compression gzip` via zlib (ships with Tiger)
- **Checksums**: CRC32 (`--checksum crc32`) — same value rb-cli accepts; strong-hash variants (`sha1`/`sha256`) are intentionally not built into the PPC port
- **FAT compaction**: Automatic for FAT12/16/32 — only backs up allocated clusters
- **Metadata**: Compatible `metadata.json` format
- **Carbon GUI**: Native Aqua frontend with progress bars and file pickers

## Requirements

- Mac OS X Tiger 10.4.x (or Leopard 10.5.x)
- Xcode 2.x with gcc-4.0
- PowerPC G3, G4, or G5 processor

## Build

```bash
# CLI only
./build.sh

# CLI + Carbon GUI
./build.sh --gui

# CLI + GUI + .app bundle
./build.sh --app
```

If your root disk is full, set `TMPDIR` to a volume with space:
```bash
export TMPDIR="/Volumes/Macintosh HD"
./build.sh
```

## Usage

The CLI grammar follows the main `rb-cli` surface: positional `<SOURCE>` /
`<DEST>` / `<BACKUP_DIR>` / `<TARGET>` arguments, `show devices` for
device enumeration, `optical rip` for disc ripping. The pre-alignment
flag forms (`--source`, `--dest`, `--backup-dir`, `--target`,
`--compression`, top-level `list-devices`, top-level `rip`) are still
accepted as deprecated aliases.

```bash
# List available disks
./rusty-backup-ppc show devices

# Back up a disk with gzip compression and CRC32 checksums
sudo ./rusty-backup-ppc backup /dev/rdisk2 /Volumes/Backup \
    --name my-backup \
    --format gzip \
    --checksum crc32

# Back up with FAT compaction disabled (sector-by-sector)
sudo ./rusty-backup-ppc backup /dev/rdisk1 /Volumes/Backup \
    --name full-image \
    --sector-by-sector

# Inspect a backup folder (reads metadata.json — PPC-specific; rb-cli's
# `inspect` operates on a live disk image instead)
./rusty-backup-ppc inspect /Volumes/Backup/my-backup

# Restore a backup (handles .gz files automatically)
sudo ./rusty-backup-ppc restore /Volumes/Backup/my-backup /dev/rdisk2

# Rip an optical disc
sudo ./rusty-backup-ppc optical rip \
    --device /dev/disk1 \
    --output /Volumes/Backup/disc.iso
```

### Checksum support

PPC only exposes `--checksum {none,crc32}`. CRC32 is shared with the
desktop `rb-cli`, so PPC-produced backups can be verified by either
tool when CRC32 is selected. Strong hashes (SHA-1, SHA-256) are
deliberately not built into the PPC port — pass `--checksum sha1` or
`--checksum sha256` and the CLI will error out rather than silently
producing an incompatible sidecar.

## FAT Compaction

For FAT12/16/32 partitions, the backup automatically skips unallocated clusters. This can dramatically reduce backup size:

```
Backing up partition 0 (FAT16, 4.0 MiB)...
  [compact] FAT12, 2020 clusters, 2048 bytes/cluster
  [compact] 7/2020 clusters allocated (99.7% savings)
  [compact] Output: 32256 bytes (was 4162048, saved 4129792)
```

The compacted image is a valid FAT filesystem with remapped cluster chains, patched directory entries, and updated BPB fields. Use `--sector-by-sector` to disable compaction for bit-perfect copies.

## Files

| File | Lines | Purpose |
|------|-------|---------|
| `rust_cli_real.c` | 2,251 | Complete CLI — all 5 commands |
| `rusty_backup_gui.c` | 853 | Carbon GUI frontend |
| `rust_runtime_v2.c` | 182 | Runtime symbol stubs |
| `Info.plist` | 28 | .app bundle metadata |
| `malloc_wrapper.c` | 16 | PIC relocation fix |
| `build.sh` | 88 | Build script |

## How This Port Was Made

This port was created using [rust-ppc-tiger](https://github.com/Scottcjn/rust-ppc-tiger), a Rust-to-PowerPC transpiler. The original Rust source was analyzed and the core backup/restore logic was reimplemented in C99, targeting GCC 4.0.1 on Tiger.

Key adaptations for Tiger:
- `_NSGetArgc()`/`_NSGetArgv()` from `<crt_externs.h>` for argument access
- `rdisk` (raw character devices) for ioctl device size detection
- `statfs()` fallback for mounted partition sizes
- zlib for gzip compression and CRC32 (ships with Tiger)
- Carbon HIToolbox for the native GUI
- Navigation Services for file/folder pickers

## Tested On

- **Dual G4 Power Mac** running Mac OS X Tiger 10.4.11
  - 4 internal disks (APM + HFS+)
  - Backup, restore, inspect, list-devices all verified
  - FAT compaction verified with test images
  - Gzip round-trip verified (backup → restore → MD5 match)

## License

Same as rusty-backup — AGPL-3.0.
