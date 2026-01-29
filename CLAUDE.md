# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Rusty Backup is a cross-platform GUI application (Rust, egui/eframe) for backing up and restoring vintage computer hard disk images with partition resizing and CHD compression. Target users are retro computing enthusiasts backing up CF/SD cards from DOS, Windows 9x, and early Linux systems.

Licensed under AGPL-3.0. The full specification lives in `PROJECT-SPEC.md`.

## Build Commands

```bash
cargo build                    # Debug build
cargo build --release          # Release build
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

## Architecture

The codebase separates GUI from core logic with these major modules:

- **`gui/`** - egui/eframe UI layer with tabs (Backup, Restore, Inspect), progress indicators, and a log panel. Long-running disk operations run on separate threads to keep the UI responsive.
- **`partition/`** - MBR and GPT partition table parsing, export, and alignment detection. Alignment patterns matter for vintage systems (DOS traditional LBA 63/cylinder boundaries vs modern 1MB alignment).
- **`filesystem/`** - Trait-based filesystem support (`Filesystem` trait with `detect`, `get_info`, `calculate_min_size`, `validate`). Implementations for FAT16/32, NTFS, and ext2/3/4.
- **`resize/`** - Filesystem-specific resize logic. Determines minimum partition size by analyzing allocated clusters/blocks.
- **`chd/`** - CHD (MAME standard) compression wrapper with file splitting support (default ~4GB chunks for FAT32 media compatibility).
- **`backup/`** - Backup folder structure management, JSON metadata serialization, and checksum verification (user-selectable CRC32 or SHA256).
- **`error.rs`** - Centralized error types using `thiserror`, with `anyhow::Result` for propagation.

### Backup Format

Each backup is a folder containing:
- `metadata.json` - partition info, alignment data, checksums, bad sectors
- `mbr.bin` / `mbr.json` or `gpt.json` - partition table exports
- `partition-N.chd` - compressed partition images
- Per-file checksum files (`.sha256` or `.crc32`)

### Key Design Patterns

- **Filesystem trait** for extensible filesystem support - each filesystem module implements detection from superblock bytes, metadata reading, and minimum size calculation independently.
- **Partition alignment preservation** - backups record detected alignment (DosTraditional, Modern1MB, Custom, None) so restores can preserve original geometry for vintage OS compatibility.
- **Flexible restore** - users can configure partition sizes during restore (entire disk, minimum, or custom per-partition), with automatic filesystem expansion.

### Platform-Specific Concerns

Device paths differ by platform: `/dev/sdX` (Linux), `/dev/diskX` (macOS), `\\.\PhysicalDriveX` (Windows). Raw disk access requires elevated privileges. The `nix` crate handles Unix-specific operations; `winapi` handles Windows.

## Development Guidelines

- Separate business logic from GUI code; core modules should be independently testable.
- Use streaming I/O with large blocks (64KB-1MB chunks) for disk operations; never load entire partitions into RAM.
- Disk I/O must be sector-aligned (512 bytes or 4KB).
- Disable UI controls during active operations to prevent conflicts.
- Use `Result<T>` and `?` for error propagation throughout.
