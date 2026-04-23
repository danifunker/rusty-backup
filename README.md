# Rusty Backup

Cross-platform GUI tool for backing up, restoring, and inspecting vintage
computer disk images. The name is a play on words — both referencing the Rust
programming language and the rusty vintage machines we're preserving.

Primary users are retro computing enthusiasts archiving CF/SD cards, IDE/SCSI
drives, and floppy images from DOS, Windows 9x, early Linux, classic Mac OS,
and Apple II / IIgs systems.

Licensed under **AGPL-3.0**. See `PROJECT-SPEC.md` for the full design
specification and `docs/` for deep-dive topics.

## Installation

Rusty Backup ships as a single self-contained binary per platform.

1. Grab the latest build for your OS from the
   [GitHub Releases page](https://github.com/danifunker/rusty-backup/releases).
2. Drop the binary where you want it:
   - **Windows** — extract the ZIP and run `rusty-backup.exe`.
   - **macOS** — open the DMG and drag `Rusty Backup.app` into `/Applications`.
   - **Linux** — `chmod +x rusty-backup-*.AppImage` and launch it.
3. Raw physical disks require elevated privileges (admin on Windows, root on
   Linux, an authorisation prompt on macOS). Working with image files on disk
   does not.

To build from source: `cargo build --release`. See `CLAUDE.md` for the full
build matrix.

## Usage

The app has three tabs:

- **Backup** — pick a source (physical device or image file), choose a
  destination folder, pick an output format and checksum type, and start.
  Each backup is written as a folder containing `metadata.json`, the partition
  table (`mbr.bin`/`mbr.json` or `gpt.json`), and one compressed file per
  partition with its checksum sidecar.
- **Restore** — pick a backup folder and a target device or image file.
  Partition sizes can be left at original, shrunk to the filesystem minimum,
  or set to a custom value; the filesystem is expanded in place when the
  restored partition is larger than the minimum. Alignment (DOS/cylinder,
  1 MB, or custom) is preserved from the source by default.
- **Inspect** — pick any supported source and browse the partition table,
  filesystem info, and file listings without modifying anything.

VHD export is available from the Inspect tab: produce either a whole-disk
`.vhd` (partition table plus all partitions with their gaps) or per-partition
`.vhd` files, ready to mount in VirtualBox, Hyper-V, or QEMU. See
`docs/vhd-export.md`.

## Compatibility

### Image / backup formats

| Format         | Extension       | Read as source | Write as backup | Notes |
|----------------|-----------------|----------------|-----------------|-------|
| Raw            | `.img`, `.raw`, `.hda` | Yes     | Yes             | Sparse zero-skipping; optional splitting |
| Fixed VHD      | `.vhd`          | Yes            | Yes             | 512-byte footer; also used for VHD export |
| Zstd stream    | `.zst`          | Yes            | Yes             | Good general compression, splittable |
| CHD (MAME)     | `.chd`          | Yes            | Yes             | Requires `chdman` on `PATH` |
| Apple 2MG      | `.2mg`          | Yes            | No              | Apple II / IIgs disk images |
| Disk Copy 4.2  | `.dc42`, `.image` | Yes          | No              | Classic Mac floppy images |
| Apple DMG      | `.dmg`          | Yes (raw/UDRW) | No              | Uncompressed DMGs only |
| WOZ            | `.woz`          | Yes            | No              | Apple II 5.25" and 3.5" flux images |
| Raw physical disk | —            | Yes            | Yes (restore target) | CF/SD/USB/HDD/SSD — see below |

### Filesystems

All listed filesystems support browsing in the **Inspect** tab and are
preserved intact on backup/restore. "Shrink" means the filesystem can be
safely compacted to its minimum size during backup and re-expanded during
restore or VHD export.

| Filesystem     | Browse | Shrink / expand on restore | Shrink / expand on VHD export | Notes |
|----------------|:------:|:--------------------------:|:-----------------------------:|-------|
| FAT12          | Yes    | Yes                        | Yes                           | Apple II SuperDrive, DOS floppies |
| FAT16          | Yes    | Yes                        | Yes                           | DOS / Windows 3.x / 9x |
| FAT32          | Yes    | Yes                        | Yes                           | Windows 95 OSR2+ through XP, vintage Linux |
| exFAT          | Yes    | Yes                        | Yes                           | Modern removable media |
| NTFS           | Yes    | Yes                        | Yes                           | Windows NT / 2000 / XP |
| ext2 / ext3 / ext4 | Yes | Yes                       | Yes                           | Early Linux installs onward |
| HFS (Mac OS Standard) | Yes | Yes                     | Yes                           | Classic Mac OS 68k / early PowerPC |
| HFS+ (Mac OS Extended) | Yes | Yes                    | Yes                           | Mac OS 8.1 onward, non-case-sensitive |
| btrfs          | Yes    | Yes                        | Yes                           | Modern Linux; read-only browse |
| ProDOS         | Yes    | Yes                        | No (planned)                  | Apple II / IIgs |

### What works well vs. what to watch out for

- **FAT12 / FAT16 / FAT32 → VHD** is the best-exercised path. Whole-disk and
  per-partition export, shrink, grow, and in-place FAT table patching are all
  covered by tests.
- **exFAT / NTFS / HFS / HFS+ / ext / btrfs → VHD** will write a correctly
  sized VHD with the filesystem patched in place, but less of this path is
  test-covered; verify the exported image by mounting it before trusting a
  restore from it. Some docs in `docs/` predate full coverage — the code
  now wires resize for every filesystem in the table.
- **ProDOS → VHD** is not implemented yet; restore to raw / CHD / Zstd /
  physical disk works.
- **Raw → raw** restore always works regardless of filesystem; only the
  shrink/expand paths depend on filesystem-specific code.
- **Write-back to source formats** (2MG, DC42, DMG, WOZ) is not supported;
  read those to back up, and restore to raw / VHD / CHD / Zstd or a physical
  disk.

### Physical drive compatibility

Rusty Backup talks to whatever the OS exposes as a raw block device. Anything
that shows up via the platform enumerator (Disk Management / `diskutil` /
`/sys/block`) is a candidate.

| Media                                   | Backup from physical | Restore to physical | Notes |
|-----------------------------------------|:--------------------:|:-------------------:|-------|
| CompactFlash (via USB/PCMCIA reader)    | Yes                  | Yes                 | Primary use case |
| SD / microSD / MMC                      | Yes                  | Yes                 |       |
| USB flash drives                        | Yes                  | Yes                 |       |
| USB-attached IDE / SATA (HDD, SSD)      | Yes                  | Yes                 | Docks, toasters, bridges |
| Internal SATA / NVMe drives             | Yes                  | Yes                 | Requires elevation; verify target |
| USB floppy drives (1.44 MB, 720 KB)     | Yes (as block device)| Yes                 | Treated as a regular block device; no copy-protection support |
| 5.25" / 3.5" floppies via Kryoflux / Greaseweazle / Applesauce | No (use their tools) | No | Dump to `.woz` / `.dc42` / `.2mg` and feed that image in |
| Optical media (CD/DVD/BD)               | No                   | No                  | Use `chdman` or dedicated tools |
| Tape drives                             | No                   | No                  |       |

Physical floppies are supported only through the OS block-device layer, which
covers standard MFM formats (PC 1.44 MB, 720 KB). Copy-protected, variable
speed, or GCR-encoded Apple floppies must be dumped with a flux-level tool
first and then ingested as a `.woz` / `.dc42` / `.2mg` image.

## Further reading

- `PROJECT-SPEC.md` — full design document.
- `CONTRIBUTING.md` — contributor guide.
- `docs/` — per-feature deep dives (VHD export, alignment, code signing,
  Apple II floppy formats, …).

## License

AGPL-3.0 — see `LICENSE`.
