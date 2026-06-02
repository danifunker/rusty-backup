# Export & Backup Pipeline Map

A navigation aid for "where do I touch the code when I want to change behavior
X for output format Y?" The pipelines look similar at a glance but fan out
into separate per-format implementations, partition-table dispatch, and
filesystem-specific hooks. This doc maps the call graph so you can jump
straight to the right file instead of re-deriving it via grep.

**When to consult this:**
- Adding a new output format that should honor resize plans.
- Adding a defragmenting writer for a new filesystem.
- Tracking down where the "Compact Space" UI checkbox is gated.
- Wiring CHD / VHD / Raw to a new clone path you just built.
- Debugging "I resized partition X and the export pipeline ignored it."

The fanout from one user action into per-table + per-format + per-filesystem
dispatch is the kind of thing that's easy to forget between sessions and
hard to rediscover by grep alone. The 12-step recipe at the bottom is the
canonical procedure for "add a defragmenting writer for filesystem X"
— it has been validated against the PFS3 work and catches the wiring sites
that are easy to miss (the CHD path was forgotten initially during that
work and required a follow-up; it's now an explicit step in the recipe).

Reference implementations in tree:
- `src/fs/affs.rs`, `src/fs/pfs3.rs`, `src/fs/sfs.rs`, `src/fs/pfs3_clone.rs` — Amiga in-place resize + PFS3 defragmenting clone.
- `src/fs/hfsplus_defrag.rs`, `src/fs/hfsplus_clone.rs` — HFS+ defrag clone planner + streamer.

---

## The four user-facing actions

| User action | GUI tab | Entry point (function) | Lives in |
|---|---|---|---|
| **Back up to folder** | Backup tab | `backup::run_backup` | `src/backup/mod.rs` |
| **Back up to single-file CHD** | Backup tab (CHD output) | `backup::single_file_chd::run_export` | `src/backup/single_file_chd.rs` |
| **Restore from backup** | Restore tab | `restore::run_restore` → `rbformats::reconstruct_disk_from_backup` | `src/restore/mod.rs`, `src/rbformats/mod.rs` |
| **Export disk image** | Inspect tab → "Export Disk Image…" | `rbformats::export::export_whole_disk` (and per-format siblings) | `src/rbformats/export.rs` |

Each entry point branches by partition-table type (MBR / GPT / APM / RDB /
None) and by output format (VHD / Raw / 2MG / WOZ / DC42 / CHD / Zstd /
backup-folder).

---

## Per-partition-table dispatch (the reconstruct layer)

When a partition resize is requested AND the source has a structural
partition table that needs patching, the export/restore paths route through
one of these reconstructors (all in `src/rbformats/mod.rs`):

| Source table | Reconstructor | Patches | Per-partition fixup |
|---|---|---|---|
| MBR/GPT (backup folder source) | `reconstruct_disk_from_backup` | MBR primary + EBR chain, GPT primary/backup | Per-FS resize (FAT/NTFS/exFAT/HFS/HFS+/ext/btrfs/ProDOS) |
| APM | `reconstruct_raw_apm_disk` | DDR + partition map entries | Per-FS resize (Apple_HFS, Apple_HFS+, Apple_UNIX_SVR2) |
| RDB | `reconstruct_raw_rdb_disk` | RDSK header + PART chain via `Rdb::patch_for_restore` | Per-FS resize (AFFS, SFS, PFS3 — see below for PFS3 clone branch) |

Each reconstructor follows the same shape:

1. Copy the partition-table region (sectors 0..N) verbatim.
2. Overlay the patched table blocks.
3. Copy each partition's bytes from source_lba to dest_lba.
4. Call `fs::resize_filesystem_for` per resized partition (the per-FS
   in-place trim hook).
5. Pad to declared target size.

**PFS3 exception (step 3+4 short-circuit):** when `reconstruct_raw_rdb_disk`
sees a PFS3 partition with `export_size < copy_size` (a real shrink) AND a
`pfs3_clone_source` path was passed in, it skips the verbatim copy and
runs `fs::pfs3_clone::stream_defragmented_pfs3` directly into the
destination writer at `dest_offset`. That partition is then excluded from
the step-4 fixup loop. Reason: PFS3's `rovingPointer` allocator scatters
blocks throughout the volume, so `resize_pfs3_in_place` typically refuses
real shrinks (`highest_allocated_user_sector >= target sectors`). The clone
rebuilds the volume packed and achieves the defragmented floor.

---

## Per-output-format dispatch

Each `export_whole_disk_*` function in `src/rbformats/export.rs` and friends
calls the appropriate reconstructor (when needed) and then either writes
the raw bytes, appends a format-specific footer, or pipes through a
compressor:

| Format | Function | Path |
|---|---|---|
| Raw (`.img`) | `export::export_whole_disk` (default branch) | reconstructor → flat file |
| VHD (`.vhd`) | `vhd::export_whole_disk_vhd` | reconstructor → flat file + 512-byte footer |
| 2MG (`.2mg`) | `export::export_whole_disk` (TwoMg branch) | reconstructor → file with 2MG header |
| WOZ / DC42 | `export::export_whole_disk` (Woz/Dc42 branch) | reconstruct to RAM buffer → encoder |
| CHD HD (`.chd`) | `export::export_whole_disk_chd` | reconstructor → tempfile → `chd::compress_chd` |
| CHD CD / DVD | `export::export_whole_disk_chd_cd`, `export_whole_disk_bincue` | bypasses partition logic (optical tab) |
| Backup-folder Zstd / Raw / VHD / CHD | `backup::run_backup` per-partition | direct fs-specific CompactReader or defragmenting writer |

**To add resize handling to a new output format:** detect whether the source
has a structural partition table (MBR/GPT/APM/RDB) and route through the
matching reconstructor first, writing to either the destination directly
(Read+Write+Seek sinks) or a tempfile that's then fed into the
format-specific encoder.

---

## Per-filesystem defragmenting writer dispatch

When the user enables shrink-to-minimum AND the filesystem has a true
defragmenting writer (not just an in-place trim), a parallel pipeline kicks
in via `fs::DefragCloneShape`:

| Shape | Detected by | Streamer | When fires |
|---|---|---|---|
| `Flat` (HFS+/HFSX) | `fs::probe_hfsplus_signature` | `fs::hfsplus_defrag::stream_defragmented_hfsplus` | Backup pipeline (backup/mod.rs:1015) AND single-file CHD pipeline (backup/single_file_chd.rs:1513) |
| `Wrapped` (HFS+ inside HFS shell) | `fs::hfsplus_wrapper_clone::detect_wrapped_hfsplus_at` | `fs::hfsplus_wrapper_clone::stream_wrapped_defragmented_hfsplus` | Same call sites as Flat |
| `Pfs3` | partition type string `PFS\3` / `PDS\3` / `muFS` + rootblock magic check | `fs::pfs3_clone::stream_defragmented_pfs3` | Backup pipeline, single-file CHD pipeline, AND raw-RDB export reconstructor |

Unified detection: `fs::detect_defrag_clone_shape(reader, offset,
partition_type_string)` returns the right shape or an error reason.

Capability gate: `fs::has_defragmenting_writer(type_byte, type_string)`
returns true for HFS+/HFSX/PFS3. The GUI's "Compact Space" checkbox is
enabled iff this returns true. `fs::pick_shrink_target(...)` decides whether
to use the in-place floor or the defragmented floor based on this.

**To add a new defragmenting writer for filesystem X:**

1. Implement `clone_X_volume(source, target)` modeled on
   `src/fs/pfs3_clone.rs::clone_pfs3_volume`.
2. Implement `stream_defragmented_X(source, target_size, dst)` — usually
   a thin wrapper that formats blank → clones → drains tempfile.
3. Override `defragmented_minimum_size` on `XFilesystem` to return the
   used-bytes floor (default returns `last_data_byte`).
4. Add a variant to `fs::DefragCloneShape`.
5. Extend `fs::detect_defrag_clone_shape` to recognize X.
6. Add X to `fs::has_defragmenting_writer`.
7. Wire the new shape into the three producer-thread switch sites
   (`backup/mod.rs`, `backup/single_file_chd.rs`,
   `rbformats/mod.rs::reconstruct_raw_rdb_disk` for matching table types).
8. Wire format-specific outputs (CHD, VHD, Raw) by detecting source-has-X
   in `export_whole_disk_chd` / `export_whole_disk` / `export_whole_disk_vhd`
   when a resize is requested.

---

## Per-filesystem in-place resize dispatch

Layout-preserving filesystems route resize requests through
`fs::resize_filesystem_for(writer, partition_offset, new_size, log_cb)`,
which dispatches by reading the volume's magic and calling one of:

| Filesystem | Resize function | Notes |
|---|---|---|
| FAT12/16/32 | `fs::fat::resize_fat_in_place` | Updates BPB, FAT chain, FSInfo |
| NTFS | `fs::ntfs::resize_ntfs_in_place` | MFT bitmap rewrite |
| exFAT | `fs::exfat::resize_exfat_in_place` | Volume bitmap + BR checksum |
| HFS | `fs::hfs::resize_hfs_in_place` | MDB + alt-MDB |
| HFS+/HFSX | `fs::hfsplus::resize_hfsplus_in_place` | VH + alt-VH + allocation file trim |
| ext2/3/4 | `fs::ext::resize_ext_in_place` | Block group descriptor update |
| btrfs | `fs::btrfs::resize_btrfs_in_place` | Chunk tree trim |
| ProDOS | `fs::prodos::resize_prodos_in_place` | Volume bitmap |
| AFFS | `fs::affs::resize_affs_in_place` | Root-at-midpoint relocation |
| SFS | `fs::sfs::resize_sfs_in_place` | Phase S.1/S.2 — shrink to floor only |
| PFS3 | `fs::pfs3::resize_pfs3_in_place` | Shrink to free-tail floor only — usually refuses real shrinks; see clone path |

`resize_filesystem_for` skips silently when the magic at `partition_offset`
isn't a filesystem it recognizes — so e.g. RDB partitions with a non-Amiga
FS just fall through harmlessly.

---

## GUI state for resize sizing

`gui::inspect_tab::InspectTab` and `gui::backup_tab::BackupTab` both keep:

- `partition_min_sizes: HashMap<usize, u64>` — in-place floor per partition.
- `partition_defrag_min_sizes: HashMap<usize, u64>` — defragmented floor for
  filesystems that have a true defragmenting writer.

Populated asynchronously via `model::min_size_runner::spawn` which calls
`fs::partition_minimum_size`. Display layer chooses between the two via
`fs::pick_shrink_target`:

- **Packing FS** (FAT/NTFS/exFAT) → defragmented (CompactReader emits packed).
- **HFS+/HFSX, PFS3** → defragmented (clone pipeline emits packed).
- **All other layout-preserving** (HFS / ext / btrfs / ProDOS / AFFS / SFS)
  → in-place (no clone path yet).

The "Export Disk Image" dialog's `init_export_configs` passes BOTH maps into
`model::export_runner::build_partition_configs`, which routes through
`pick_shrink_target` to pick the right floor per partition.

---

## What to grep for when you need to find these

- **"Wire a new clone path"** → grep `DefragCloneShape::` and follow each match.
- **"Add a new output format with resize"** → grep `reconstruct_raw_rdb_disk\|reconstruct_raw_apm_disk\|reconstruct_disk_from_backup` for call sites.
- **"Change in-place resize for filesystem X"** → grep `resize_X_in_place\|resize_filesystem_for`.
- **"Find where minimum size gets displayed"** → grep `partition_min_sizes\|partition_defrag_min_sizes`.
- **"Find where Compact Space checkbox is gated"** → grep `has_defragmenting_writer`.

---

## Adding a "new special thing" — a worked example

Scenario: you want to make AFFS partitions emit a defragmenting clone (today
they only trim trailing free blocks). Following the recipe:

1. **Read side already exists** (`fs::affs::AffsFilesystem` implements
   `Filesystem`). ✓
2. **Write side** — `fs::affs::AffsFilesystem` implements
   `EditableFilesystem`. ✓
3. **Blank-volume creator** — `fs::affs::create_blank_affs(size, name)`
   already exists. ✓
4. **Write `fs::affs_clone::clone_affs_volume(source, target)`** modeled on
   `pfs3_clone::clone_pfs3_volume`. Walk source, replay onto target,
   preserve `amiga_protection` / `amiga_comment` / `amiga_date` (all already
   round-trip via `CreateFileOptions`).
5. **Write `stream_defragmented_affs(source, target_size, dst)`** — thin
   wrapper, tempfile-backed.
6. **Override `AffsFilesystem::defragmented_minimum_size`** to return
   `used_blocks * block_size + margin`.
7. **Add `DefragCloneShape::Affs`** in `src/fs/mod.rs`.
8. **Extend `detect_defrag_clone_shape`** to recognize AFFS partition type
   strings.
9. **Add AFFS to `has_defragmenting_writer`**.
10. **Wire the new shape** at three producer sites: `backup/mod.rs`,
    `backup/single_file_chd.rs`, `rbformats/mod.rs::reconstruct_raw_rdb_disk`.
11. **Add limitations banner** in `gui/inspect_tab.rs` Export Disk Image
    dialog (mirror the existing PFS3 banner near line 1267).
12. **Tests** in `src/fs/affs_clone.rs::tests`.

The same 12-step recipe applies for SFS and any future Linux/BSD FS we want
to defragment-clone.
