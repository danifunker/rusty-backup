# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Rusty Backup is a cross-platform GUI application (Rust, egui/eframe) for backing up and restoring vintage computer hard disk images with partition resizing and CHD compression. Target users are retro computing enthusiasts backing up CF/SD cards from DOS, Windows 9x, and early Linux systems.

Licensed under AGPL-3.0. The full specification lives in `PROJECT-SPEC.md`.

## Coding Style
Read CONTRIBUTING.md prior to making code changes in this repository.

## Build Commands

```bash
cargo build                    # Debug build (version: x.y.z-dev)
cargo build --release          # Release build (version from Cargo.toml or RELEASE_VERSION env)
cargo run                      # Run (debug)
cargo run --release            # Run (release)
cargo test                     # All tests
cargo test test_name           # Single test
cargo test --lib               # Unit tests only
cargo test --test '*'          # Integration tests only
cargo fmt --check              # Check formatting
cargo fmt                      # Auto-format
cargo clippy                   # Lint
```

## Build Infrastructure

### Versioning
- Version is set at compile time via `APP_VERSION` environment variable in `build.rs`
- Debug builds: `{version}-dev` (e.g., `0.1.0-dev`)
- Release builds: `RELEASE_VERSION` env var or falls back to `CARGO_PKG_VERSION`
- CI/CD uses date-based versioning: `YYYYMMDDHHMM` format

### Cross-Platform Icons
- Icons are embedded at compile time from `assets/icons/`
- Windows: `icon.ico` embedded in exe resources via winres
- macOS: `icon.icns` included in .app bundle
- Linux: `icon-256.png` for window and desktop integration
- Icon generation script: `scripts/generate-icon.sh` (requires ImageMagick)

### Platform-Specific Builds
- Windows: Console window hidden in release builds (`windows_subsystem = "windows"`)
- macOS: Built as .app bundle with Info.plist, packaged as DMG
- Linux: AppImage format with .desktop file integration

### CI/CD Pipeline
- GitHub Actions workflow: `.github/workflows/release.yml`
- Builds for: Windows (x86/x64), macOS (arm64/x64), Linux (x64 AppImage)
- Auto-release on push to main/master with generated version tag
- Artifacts: ZIP (Windows), DMG (macOS), AppImage (Linux)

### Update Checking
- Optional automatic update detection from GitHub releases
- Configure via `config.json` (see `config.json.example`)
- GUI shows update notification banner when newer version available
- Module: `src/update.rs`

## Architecture

The codebase separates GUI from core logic with these major modules:

- **`gui/`** - egui/eframe UI layer with tabs (Backup, Restore, Inspect), progress indicators, log panel, version display, and update notifications. Long-running disk operations run on separate threads to keep the UI responsive.
- **`partition/`** - MBR and GPT partition table parsing, export, alignment detection, `PartitionSizeOverride` for resize/restore, and `patch_mbr_entries`/`lba_to_chs` for MBR patching.
- **`fs/`** - Trait-based filesystem abstraction (`Filesystem` trait in `filesystem.rs`, `FileEntry` in `entry.rs`). FAT12/16/32 implementation in `fat.rs` includes browsing, `CompactFatReader` for smart compaction, and in-place resize/validation/BPB patching. Factory functions in `mod.rs` route by partition type byte. See `src/fs/README.md`.
- **`rbformats/`** - Output format handlers (VHD, CHD, Zstd, Raw) with compress/decompress APIs, plus `reconstruct_disk_from_backup` for restore and VHD export. Each format in its own file. See `src/rbformats/README.md`.
- **`backup/`** - Backup orchestration (`run_backup`), `CompressionType` enum, folder structure management, JSON metadata serialization, and checksum verification (CRC32 or SHA256).
- **`restore/`** - Restore orchestration with flexible partition sizing (original, minimum, custom) and alignment preservation.
- **`update/`** - Update checking against GitHub releases, configuration via `config.json`, and update notification UI.
- **`error.rs`** - Centralized error types using `thiserror`, with `anyhow::Result` for propagation.

### Backup Format

Each backup is a folder. Two layouts depending on the chosen output:

**Per-partition** (Zstd / Raw / per-partition VHD):
- `metadata.json` - partition info, alignment data, checksums, bad sectors
- `mbr.bin` / `mbr.json` or `gpt.json` - partition table exports
- `partition-N.<ext>` - one compressed file per partition (`.zst`, `.raw`, `.vhd`)
- Per-file checksum files (`.sha256` or `.crc32`)

**Single-file CHD** (CHD output):
- `metadata.json` with `layout: "single-file-chd"` and per-partition
  `offset_in_disk` byte ranges instead of per-file references
- `mbr.json` / `gpt.json` / `apm.json` - parsed partition-table sidecar (raw
  bytes live inside the CHD)
- `<backup-name>.chd` - one disk image with table at sector 0, partitions
  at their declared offsets, gaps zero-filled. `chdman info` opens it,
  MAME loads it.

CHD output never produces per-partition CHDs — the single-file layout is
the only CHD shape rusty-backup writes.

### Key Design Patterns

- **Filesystem trait** for extensible filesystem support - each filesystem module implements detection from superblock bytes, metadata reading, and minimum size calculation independently.
- **Partition alignment preservation** - backups record detected alignment (DosTraditional, Modern1MB, Custom, None) so restores can preserve original geometry for vintage OS compatibility.
- **Flexible restore** - users can configure partition sizes during restore (entire disk, minimum, or custom per-partition), with automatic filesystem expansion.

### Amiga Support

Amiga images (`.adf`, `.hdf`, `.adz`, `.hdz`, and CHD-wrapped HDFs) and the three filesystems that matter on Classic Amiga hardware (AFFS, PFS3, SFS) are first-class:

- **Partition table**: `PartitionTable::Rdb` parses RDSK + PART chain (`src/partition/rdb.rs`). Each `PartitionInfo` carries the 4-byte `DosType` as `partition_type_string` (e.g. `"DOS\\3"`, `"PFS\\3"`, `"SFS\\0"`).
- **Filesystem dispatch**: extend the `partition_type_string` matchers in `src/fs/mod.rs` (`fs_name_for`, `is_layout_preserving_fs`, `is_expensive_minimum`, `open_filesystem_by_string`, `open_editable_filesystem_by_string`, `compact_partition_reader_by_string`). Helpers `is_amiga_dos_type` / `is_amiga_pfs3_type` / `is_amiga_sfs_type` group the relevant strings.
- **Filesystems**:
  - **AFFS** (`src/fs/affs.rs`) — DOS\0..DOS\7, read + edit + Disk Validator fsck. Files inherit `amiga_protection` (offset 0x140) and `amiga_comment` (BSTR at 0x148) via `CreateFileOptions`; the same fields surface on `FileEntry` for the browse view.
  - **PFS3** (`src/fs/pfs3.rs`) — PFS\3, PDS\3, muFS. Read + edit (allocator + EditableFilesystem with snapshot/rollback). `create_blank_pfs3` formats a blank volume for tests.
  - **SFS** (`src/fs/sfs.rs`) — SFS\0, SFS\2. Read + edit (single-leaf btree only); journal blocks (TRST/TRFA) are not maintained — we use sync-boundary atomicity instead. `create_blank_sfs` formats a blank volume.
- **Bitmap convention**: All three filesystems use **set bit = free** (opposite of most other FS we deal with). Easy bug source.
- **Endianness**: every multi-byte field on Amiga disks is big-endian. Easy bug source #2.
- **`.adz` / `.hdz`**: the GUI helper `gui::materialize_amiga_image_path` decompresses gzip-wrapped images to a tempfile at file-pick time. Each tab keeps a `tempfile::TempDir` guard alive for the lifetime of `image_file_path`.

Reference C sources for the three filesystems live at `~/repos/amigasources/{ADFlib,amitools,pfs3aio,smartfilesystem}`. Read + edit + in-place resize all shipped; any new Amiga work lives under `src/fs/{affs,pfs3,sfs}.rs` and follows the existing patterns there.

### BasiliskII HFV Support

`.hfv` / `.HFV` files are flat, partition-less raw images of a single **classic HFS** volume (boot blocks at sector 0, MDB at byte 1024, no MBR/GPT/APM) used by the BasiliskII / SheepShaver 68k Mac emulators. They flow through the existing **superfloppy** path: `partition::detect_superfloppy` finds the MDB at offset 1024 → `PartitionTable::None { fs_hint: "HFS" }` → `open_filesystem(.., 0, 0, None)` auto-detects HFS at offset 0. So read / inspect / browse / fsck / edit / backup / restore all work with no HFV-specific code — only the file-picker extensions and a few superfloppy-HFS gating helpers (`is_superfloppy_hfs`) needed touching.

The **write** side lives in `src/fs/hfv.rs`, the single source of truth for the HFV limits: **classic HFS only** (never HFS+) and **≤ 2047 MB** (the 2 GiB signed-32-bit boundary classic Mac OS won't cross). `build_blank_hfv` / `clone_into_hfv` produce a flat HFS image with **no** APM wrapper (an HFV is the bare volume). Creation is `rb-cli new --fs hfv`; conversion/resize to HFV is `rb-cli expand --to-hfv` (and the GUI "Expand HFS Volume…" dialog's "Flat HFV" output mode), which reuse the HFS clone path and skip `emit_apm_disk_with_hfs`.

### Platform-Specific Concerns

Device paths differ by platform: `/dev/sdX` (Linux), `/dev/diskX` (macOS), `\\.\PhysicalDriveX` (Windows). Raw disk access requires elevated privileges. The `nix` crate handles Unix-specific operations; `windows` crate handles Windows.

All OS-specific code lives in `src/os/`:
- **`windows.rs`** - Windows device enumeration, elevation requests, volume locking/dismounting
- **`macos.rs`** - macOS device enumeration via DiskArbitration, unmounting, elevated access via dd
- **`linux.rs`** - Linux device enumeration from /sys/block, unmounting via MNT_DETACH
- **`mod.rs`** - Unified interface, `SectorAlignedWriter` for raw device writes

## Development Guidelines

- Separate business logic from GUI code; core modules should be independently testable.
- **GUI / CLI feature parity.** When adding a user-facing feature, consider whether it belongs in the CLI (`rb-cli`, formerly `rusty-backup-cli`) as well. The CLI mirrors the GUI's scriptable surface — backup, restore, inspect, browse-view editing, optical, resize/expand, etc. Rule of thumb: if it's a one-shot operation a user might want to script, it should land in both. Check `rb-cli --help` and `docs/cli-reference.md` for the canonical grammar before adding a new verb or flag so naming stays consistent. Interactive-only dialogs (Settings, elevation prompts, free-form review modals) stay GUI-only — those are explicitly out of scope for the CLI.
- **Shared business logic between GUI and CLI.** When implementing a feature, put the logic in a core module (e.g. `src/backup/`, `src/fs/`, `src/model/`) and have both `src/gui/` and `src/cli.rs` call into it. Don't reimplement the same operation twice. Preflight checks (free-space projection, duplicate-name detection, type/creator validation) especially need lifting into shared modules.
- Use streaming I/O with large blocks (64KB-1MB chunks) for disk operations; never load entire partitions into RAM.
- Disk I/O must be sector-aligned (512 bytes or 4KB).
- Disable UI controls during active operations to prevent conflicts.
- Use `Result<T>` and `?` for error propagation throughout.
- **No Unicode glyphs in UI text or log messages.** The default egui font does not include emoji or symbol glyphs (e.g. `✓ ✗ ⚠ ← → • ✖ ℹ`), so they render as blank boxes on the user's screen. Use plain ASCII alternatives instead — `OK`, `Skipped`, `Warning`, `Back`, `->`, `-`, `X`, `Info:` — for log lines, button labels, dialog text, and any other user-visible string. The only exception is content read directly from a filesystem (e.g. symlink target arrows in the optical browse view), which we render verbatim.

### Pre-commit documentation sync

**Before staging a commit for any user-facing change, run a quick parity pass over three surfaces and update them in the same commit if they drifted.** These have all gone stale in real-world commits — adding a filesystem driver but forgetting the README table, shipping a new container format but leaving the picker filter behind, adding a CP/M DPB but never updating the per-core matrix. The sync is cheap (usually one extra section in the diff); diagnosing the drift from a user-reported issue is expensive.

The three surfaces:

1. **[README.md](README.md)** — user-facing feature lists.
   - **Image / backup formats table** (§ Compatibility) — every new wrapper / container format gets a row with extension, read/write columns, and a one-line note.
   - **Filesystems table** — every new fs gets a row covering Browse / Edit / Shrink-expand / fsck / Notes.
   - **MiSTer FPGA cores table** — when a new fs / container makes a previously-Partial core go end-to-end, the cell moves to **Yes** and the media-path column gains the new extensions.
   - **Inspect tab bullet list** — when a new dialog ships (e.g. "Convert Floppy Container..."), add the bullet.

2. **[docs/full_MiSTer_support_status.md](docs/full_MiSTer_support_status.md)** — per-core status matrix.
   - The intro § "What Rusty Backup supports today" lists filesystems / containers / partition tables; add the new entry there.
   - The per-core table further down maps each MiSTer computer core to its FS shape and Yes/Partial/No status. When you ship support for a new filesystem (or a new DPB on a multi-DPB driver like CP/M), walk this table and re-grade every core that uses that FS. The intro and table have drifted multiple times — most recently CP/M DPBs and Apple DOS 3.3 were in the intro but the per-core matrix still marked seven cores as "No".

3. **`DISK_IMAGE_EXTS` in [src/model/file_types.rs](src/model/file_types.rs)** — the single source of truth for picker extension filters and OS file-association registration.
   - Every container format the engine knows how to open should appear here so the GUI's "Open File…" picker dropdown shows it, and so Windows Explorer / macOS Finder / Linux MIME handlers route double-clicks at us.
   - Lowercase only for new entries (the existing `GHO` / `HFV` / `GHS` uppercase variants cover a long-standing rfd quirk; we don't grow that pattern).
   - Add a row to the `floppy_container_family_present`-style regression test so a future cleanup that drops the extension has to do it deliberately.

The quick checklist when finishing a feature:

- [ ] Does this add or change a filesystem / container / partition format the user can open?
- [ ] If yes — README Image-formats / Filesystems / MiSTer-cores table updated?
- [ ] If yes — `docs/full_MiSTer_support_status.md` intro + per-core matrix walked?
- [ ] If a new picker-visible file extension — `DISK_IMAGE_EXTS` updated + regression test extended?

If the answer to all four is "no, nothing user-visible changed," commit as normal. Otherwise the doc / picker updates land in the same commit as the code change so reviewers see the intent.
