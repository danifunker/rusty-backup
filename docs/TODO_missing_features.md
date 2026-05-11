# Missing Features

A running log of feature gaps surfaced during cleanup work in `docs/codecleanup.md`. Items here are *adds*, not refactors — they represent on-disk state or capabilities the codebase doesn't yet expose.

Conventions:
- Group by area (filesystem, GUI, etc.)
- Each item: what's missing + why we know it should exist + suggested shape
- Not prioritized; pull from this list when adding features

---

## Filesystems

### ProDOS — access-bit setter
- **Gap:** ProDOS directory entries carry an *access byte* at offset 30 (read/write/destroy/rename/backup-needed flags; `$C3` unlocked, `$21` locked). The codebase reads and writes this byte (`src/fs/prodos.rs:602, 637, 664`) but the `EditableFilesystem` trait offers no way to change it after creation.
- **Why surfaced:** §2 of `codecleanup.md` — `set_permissions(mode: u32)` is Unix-shaped and only ext implements it. ProDOS access bits are an 8-bit format with different semantics, so they can't piggy-back on `set_permissions`.
- **Suggested shape:** Mirror `set_prodos_type` — add a ProDOS-specific method:
  ```rust
  fn set_prodos_access(&mut self, entry: &FileEntry, access: u8) -> Result<(), FilesystemError>
  ```
  or a higher-level `set_locked(entry, bool)` if a single locked/unlocked toggle covers the GUI case.
- **GUI hookup:** new `StagedEdit::SetProdosAccess` variant in `src/gui/browse_view.rs` (parallel to `SetProdosType`); button gated on `fs_type == "ProDOS"`.

### HFS+ — editable surface
- **Gap:** HFS+ (`src/fs/hfsplus.rs`, 2549 lines) implements `Filesystem` (browse / read) but not `EditableFilesystem`. HFS gained a full editing surface (`create_file`, `create_directory`, `delete_entry`, `delete_recursive`, `sync_metadata`, `set_finder_info`, `write_resource_fork`, `set_blessed_folder`) plus snapshot/rollback (Step 2 of `docs/hfs_write_tweaks.md`); HFS+ has none of it. The GUI's edit-mode toolbar disables Add File / New Folder / Delete on HFS+ partitions.
- **Why surfaced:** §8 of `codecleanup.md` — flagged as "confirm intentionally read-only or queue an edit-mode track."
- **Suggested shape:** mirror the HFS edit-mode plan in `docs/hfs_write_tweaks.md`:
  - Allocation-bitmap alloc/free helpers (volume bitmap + free-block tracking).
  - Catalog B-tree insert/remove (HFS+ already has the read side; `btree_split_leaf_with_insert` is shared via §1 work).
  - Extents-overflow B-tree write side (read-only today).
  - Snapshot/rollback wrapper around mutating ops, like HFS Step 2.
  - `EditableFilesystem` impl: `create_file`, `create_directory`, `delete_entry`, `delete_recursive`, `sync_metadata`, plus the HFS-shared methods (`set_finder_info`, `write_resource_fork`, `set_blessed_folder`).
- **GUI hookup:** the existing browse-view edit toolbar already drives HFS via `EditableFilesystem`; flipping HFS+ on is a one-line dispatch change in `open_editable_filesystem` once the impl exists.
- **Scale:** rough estimate 1500–2500 lines added to `hfsplus.rs`, comparable to the HFS edit-mode work.

## Partitioning

### Superfloppy -> partitioned-disk export ("wrap with synthetic MBR/GPT")
- **Gap:** when the source is a superfloppy (no partition table — e.g. a partition-only `dd` of an NTFS C: drive like `Dell Optiplex Kens office 2008.img`), backup and restore preserve the flat layout: sector 0 is the filesystem VBR, no MBR. That's correct for fidelity but useless if the user wants to feed the result to an emulator/VM that expects an MBR + partition table at sector 0 (XP boot drive, MAME PC drivers, most CHD HDD consumers).
- **Why surfaced:** confirmed against a real ~6.4 GB NTFS partition dump (commit adding `test_detect_superfloppy_ntfs`). `chdman createhd` will wrap the raw image but doesn't synthesize a table — the resulting CHD still has no MBR. `src/backup/single_file_chd.rs:1046` explicitly bails on superfloppy sources, so today there's no rusty-backup -> CHD path for these images at all.
- **Suggested shape (restore-time option):**
  - Detect when the target disk size > source partition size AND the source is a superfloppy: surface a UI prompt "source has no partition table; create one?"
  - Offer MBR / GPT / APM with a *suggested default* (see heuristic below). Always let the user override.
  - On confirmation, on restore:
    1. Allocate alignment gap (LBA 63 for DosTraditional, 2048 for Modern1MB, 34 + padding for GPT).
    2. Write a synthetic MBR (or GPT) with one entry covering the partition. Type byte from the source fs (`0x07` for NTFS/exFAT, `0x0C`/`0x0B` for FAT32, etc.).
    3. Shift the filesystem image forward by the gap.
    4. Patch the VBR `hidden_sectors` field at offset 0x1C to match the new partition LBA — NTFS/FAT/exFAT all refuse to mount if this disagrees with the partition table.
  - Reverse direction (collapse partitioned -> superfloppy) is *not* in scope; this is one-way.
- **Suggested default heuristic (single-partition source only):**
  1. If target disk size > 2 TiB: force GPT (MBR can't address it).
  2. Else read VBR `hidden_sectors` at offset 0x1C:
     - `63` -> suggest MBR + DosTraditional (legacy XP-era alignment; matches existing field, no VBR patch needed).
     - `2048` -> suggest MBR + Modern1MB (Vista+ alignment; matches existing field).
     - `0` or other -> suggest MBR + Modern1MB and patch the VBR field.
  3. HFS/HFS+ source -> suggest APM.
  4. Show the detected `hidden_sectors`/media descriptor/CHS fields in the prompt so the user can sanity-check the suggestion.
- **Worked example:** the Dell Optiplex .img has `hidden_sectors=63`, sectors_per_track=63, heads=255, media=0xF8 — classic BIOS CHS, unambiguous MBR/DOS-traditional, XP-era. Suggested default would be MBR + DosTraditional with the NTFS volume starting at LBA 63.

---

### HFS — extending a raw partition image
- **Gap:** investigate how to extend `~/Documents/partition-0.img` (a raw single-partition HFS image with no APM wrapper). The current expand-HFS path runs through `emit_apm_disk_with_hfs` in `src/fs/hfs_clone.rs`, which assumes an APM source. Surfaced during the hfs_fsck Phase 4 split — not investigated yet.
- **Suggested shape:** TBD. First step is to reproduce and confirm whether raw-image expand ever worked or has always been APM-only; then either add a non-APM emit path (just write the cloned HFS image to the output) or document the APM-only constraint and offer to wrap the source first.
