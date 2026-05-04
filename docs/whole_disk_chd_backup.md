# Single-file CHD backup format

End state: when the user picks `CompressionType::Chd` or `CompressionType::Dvd`
as the backup output, the backup folder contains **one CHD file** that is a
real disk image — `chdman info` opens it, MAME loads it, and any other CHD-
aware tool can read its contents. Other compression types (zstd, raw) keep
today's per-partition layout — this change is scoped to CHD output only.

## Design

The CHD is always a coherent disk image: partition table at sector 0, each
partition at its declared offset, gaps and post-last-partition tail
zero-filled. The user controls *what goes inside* via three orthogonal
options at backup time, plus a re-resize override at restore time.

### Read mode (how source bytes reach the CHD)

1. **Smart compaction (default).** Each selected partition is read through
   `compact_partition_reader()` — only used clusters touch disk; unused
   regions emit zeros on the fly. CHD's zero-hunk compression collapses
   them to ~nothing.

2. **Sector-by-sector copy.** Bypasses `compact_partition_reader` entirely.
   Every sector inside each partition is read from the source. Useful for
   forensics, unrecognised filesystems, or paranoid byte-equal preservation.
   Slower + larger output. Works for any partition type, including ones we
   can't smart-compact.

Read mode is per-backup, exposed as a radio in the backup tab when CHD/DVD
is selected.

### Partition sizing (how big each partition is in the output)

Same controls and code path as today's restore-time resize, lifted into the
backup tab when CHD/DVD is selected:

- **Original** — same size as on the source disk.
- **Minimum + 20%** (default) — filesystem's computed minimum, plus 20%
  headroom for future adds. Falls back to "Original" for partition types we
  can't compute a minimum for.
- **Custom** — user types a value per partition.
- **Skip** — partition not included; gets zero-filled in the CHD output.

This sub-pane is the same widget as the restore-resize pane, populated from
the same `partition_min_sizes` cache, gated by the same expensive-min-size
deferral. We're not duplicating code; we're reusing the resize widget for a
new producer.

### Restore (how the CHD becomes a target disk)

Two paths, picked by a radio in the restore tab when the source backup is a
CHD:

- **As-is** (default) — `io::copy` the CHD's logical bytes to the target.
  Partition table + sizes are baked in; the target disk must be at least as
  large as the CHD's logical size.
- **Re-resize** — same per-partition sizing controls as today. Extract each
  partition's byte range from the CHD via `ChdReader::seek`, run through the
  existing fs-resize pipeline, write at new offsets on the target. Lets the
  user shrink, grow, or rearrange after the fact.

For per-partition (non-CHD) backups, restore behaves exactly as today.

### Re-export

A finished CHD is a regular disk image. To change its partition sizes
later, the user has three paths:

1. **Inspect → Export Disk Image → CHD** with new sizes. Same backup-time
   sizing controls applied to a CHD source instead of a live device. (This
   is how Stage 9's bulk-convert-to-CHD already works; we extend it to
   accept the size pane.)
2. **Restore to a larger/smaller disk** with the re-resize controls.
3. **Edit mode** to add/remove files within the current partition sizes.

## Splitting

`chdman`, MAME, and `libchdman-rs` have no concept of split CHDs.
`split_size` is disabled for CHD/DVD output (silently ignored if a config
sets one; UI hides the control). Other compression types keep splitting
unchanged.

## Metadata

`metadata.json` gains:

```json
{
  "format_version": 2,
  "layout": "per-partition" | "single-file-chd",
  "container": "disk.chd",                  // single-file-chd only
  "container_logical_size": 137438953472,
  "container_sha1": "…",
  "read_mode": "smart" | "sector-by-sector",
  "size_policy": "original" | "minimum-plus-20" | "custom",
  "partitions": [
    { "index": 0, "offset_in_disk": 1048576, "imaged_size": …,
      "checksum": "…", "fs_type": "…", "alignment": "…", … }
  ]
}
```

- `layout` defaults to `"per-partition"` via `#[serde(default)]` so existing
  backup folders parse unchanged.
- `partitions[i].file` becomes optional; for `single-file-chd` it's absent
  and `offset_in_disk` is the partition's offset *inside the CHD's logical
  stream* (i.e. inside the disk image).
- `read_mode` and `size_policy` are recorded for traceability and to drive
  the restore tab's defaults if the user picks "re-resize".

## Stages

Each stage builds + tests green and is one session-sized chunk.

---

### Stage 1 — metadata schema + serde compat ✅

- [x] Added `BackupLayout::{PerPartition, SingleFileChd}` enum (kebab-case
      serde, `Default = PerPartition`) and `SizePolicy::{Original, MinPlus20,
      Custom}` enum to `backup/metadata.rs`.
- [x] Skipped a separate `ReadMode` enum — the existing
      `BackupMetadata::sector_by_sector: bool` already encodes the same
      smart-vs-byte-for-byte distinction; no need to duplicate.
- [x] Extended `BackupMetadata` with `layout`, `container`,
      `container_logical_size`, `container_sha1`, `size_policy`. All new fields
      use `#[serde(default)]` (and `skip_serializing_if = "Option::is_none"`
      where appropriate) so old metadata.json files round-trip unchanged.
- [x] No format-version bump needed — the layout field is the discriminator,
      and `version: u32` stays as a stamped value the writer fills.
- [x] Updated the three `BackupMetadata { ... }` call sites (one in
      `backup/mod.rs`, two in test fixtures) to set the new fields.
- [x] Two new round-trip tests:
      `test_legacy_metadata_loads_with_default_layout` (parses a pre-layout
      JSON blob and verifies the defaults) and
      `test_single_file_chd_metadata_round_trip` (verifies kebab-case
      `single-file-chd` / `min-plus20` serialization and round-trip).
- [x] `cargo test --lib backup::metadata`: 4 pass. `cargo build` + `cargo
      clippy --lib` clean (no new warnings).

**Done when:** schema parses both shapes; no behavior change yet. ✅

---

### Stage 2 — disk-image stream builder

New module `src/backup/disk_image_stream.rs`. Builds a `Read + Seek` over a
synthesised whole disk that:

1. Emits a *new* MBR or GPT at sector 0, generated from the user's resize
   plan (not copied from source — partition offsets and sizes change when
   the user resizes).
2. For each partition slot:
   - Read mode = Smart + supported FS: `compact_partition_reader()` cropped
     or padded to `imaged_size_after_resize`.
   - Read mode = Smart + unsupported FS: raw passthrough cropped/padded.
   - Read mode = SectorBySector: raw passthrough only.
3. Zero-fills gaps between partitions and the post-last-partition tail to
   the target disk-image size.
4. The total logical size is `last_partition_end + tail_padding` rounded
   up to the CHD unit boundary.

Reuses the partition-table builder code from `restore::reconstruct_disk_from_backup`
(MBR patching + GPT primary/backup header generation). No new partition-table
logic; we're piping the existing builder into a different consumer.

- Unit tests: synthetic 16 MiB MBR disk with two FAT partitions; verify
  the stream's bytes equal `[mbr][part0_compacted_padded][gap_zeros][part1_compacted_padded][tail_zeros]`,
  and that the embedded partition table parses to the resized layout.

**Done when:** `cargo test backup::disk_image_stream` green; no callers yet.

---

### Stage 3 — backup tab UI for CHD output

Reuse the existing resize widget (currently in the restore tab) inside the
backup tab. Show it only when the output is CHD/DVD.

- Lift the resize widget into a shared `gui::partition_size_picker` module
  if it isn't already shared. (Check first — most of this state is in
  `resize_popup.rs` and may already be reusable.)
- Backup tab gains a "Read mode" radio (Smart compaction / Sector-by-sector)
  and the partition size picker (with `MinPlus20` as the default).
- Hide the split-size control when CHD/DVD is selected.
- Persist last-used read-mode + default-size-policy in `UpdateConfig`.

**Done when:** the backup tab shows the right controls for CHD output and
their values plumb through to a `BackupConfig` extension.

---

### Stage 4 — backup orchestration branch

`backup::run_backup` adds a pre-loop branch:

```rust
match config.compression_type {
    CompressionType::Chd | CompressionType::Dvd => run_single_file_chd_backup(...),
    _ => existing_per_partition_loop(...),
}
```

`run_single_file_chd_backup`:
- Builds the partition list + alignment as today.
- Builds the user's resize plan (same data shape as restore-time resize).
- Constructs `DiskImageStream` (Stage 2) wrapping the source with the resize
  plan + read mode.
- Calls `compress_chd` / `compress_chd_dvd` once with `logical_size = stream
  total size`, `split_size = None` (warn if config set one).
- After write, opens `disk.chd` via `ChdReader`, computes per-partition
  checksums by seeking + streaming each partition's byte range.
- Writes `metadata.json` with `layout = single-file-chd`, `container`,
  `read_mode`, `size_policy`, partitions array with `offset_in_disk`.
- Skips `mbr.bin` (the CHD has it inline). Keeps `mbr.json` / `gpt.json`
  parsed sidecars for fast inspect.

**Done when:** running a CHD backup produces a folder containing `disk.chd`
+ `metadata.json` (+ parsed-table JSON), and `chdman info disk.chd` reports
a sensible drive with the right SHA1.

---

### Stage 5 — restore branch

`restore::reconstruct_disk_from_backup` reads metadata.json, branches on
`layout`:

- `PerPartition`: existing logic, unchanged.
- `SingleFileChd`:
  - **As-is** path (default): open `disk.chd` via `ChdReader`, `io::copy`
    its logical bytes to the target writer.
  - **Re-resize** path: for each partition in the user's plan, seek to its
    `offset_in_disk` in the CHD, read `imaged_size` bytes through the
    existing fs-resize pipeline, write at the new partition offset on
    target. Build new partition table from the resize plan, write to the
    target's MBR/GPT region.

Restore tab gains an "As-is" / "Re-resize" radio when the source backup is
a CHD. Re-resize defaults to the original sizes (i.e. preserve what the
backup baked in) and lets the user override.

VHD-export and other "export disk image" flows that go through
`reconstruct_disk_from_backup` pick this up for free.

**Done when:** CHD backup → restore as-is is byte-equal; CHD backup →
restore re-resize produces a valid resized target with passing fs check.

---

### Stage 6 — inspect + browse on single-file-chd backups

When inspect tab opens a backup folder with `layout = single-file-chd`:
- Set `chd_image_path = Some(folder/disk.chd)`.
- Reuse the existing CHD-on-image-file inspect path. Most plumbing exists
  from "inspect a raw .chd image".
- Browse view: per-partition browse opens `ChdReader`, seeks to the
  partition offset, slices to `imaged_size`, and feeds that to
  `open_filesystem`. Same as today's "browse a partition inside a CHD on
  disk image".
- The "CHD Info" button shows up automatically because `chd_image_path`
  is set.

**Done when:** opening a single-file-chd backup in inspect tab shows the
partition list + lets the user browse + extract files.

---

### Stage 7 — edit mode on single-file-chd backups

`chd_edit::ChdEditSession` already takes a parent CHD + diff path:
- Edit mode opens a session on `disk.chd` with diff
  `disk.edit-diff.chd`.
- All edits flow through the existing chd_edit flatten-on-save path.
- On save, recompute per-partition checksums in `metadata.json` for any
  partition whose byte range was touched (derivable from the diff's hunk
  map; conservatively recompute all if the map is hard to inspect).

**Done when:** edit mode on a single-file-chd backup adds/removes files in
one partition and saves correctly, with metadata checksums updated.

---

### Stage 8 — bulk-convert / re-export

The bulk-convert dialog already produces CHDs from arbitrary disk images
(Stage 9 of the chdman-removal plan). Extend it to accept the same backup-
time size picker + read-mode radio when the destination is HD CHD or DVD
CHD. This gives users a "re-export this CHD with new sizes" workflow
without leaving the app.

- Bulk-convert dialog: when destination is HD CHD or DVD CHD, embed the
  partition size picker (sourced from each input's parsed partition table)
  and the read-mode radio.
- `export_whole_disk_chd` extends to accept an optional resize plan; falls
  back to "Original" sizing when none is given (current behavior).

**Done when:** users can drop a CHD on bulk-convert, change partition
sizes, and emit a new CHD with the new layout. Same for non-CHD inputs.

---

### Stage 9 — UI cleanup + per-CHD split disablement

- Backup tab: split-size dropdown hidden when CHD/DVD selected.
- `BackupConfig::split_size` ignored for CHD/DVD with a logged warning if
  set.
- Backup tab: small note next to the CHD/DVD radio: "Output: single CHD file
  (chdman/MAME compatible)."

**Done when:** UI no longer offers nonsensical options; warnings cover any
config-file edge case.

---

### Stage 10 — docs + e2e tests

- This file kept up to date as stages land.
- Update `PROJECT-SPEC.md` if it documents the per-partition layout as the
  only option.
- Update any README that mentions backup format.
- E2E tests:
  - synthetic disk → smart-compact + min+20% → CHD backup → as-is restore
    → byte-equal target
  - same source → sector-by-sector + original → CHD backup → as-is restore
    → byte-equal target (and confirm output stream length = source disk
    size exactly)
  - smart-compact CHD backup → re-resize restore (shrink one partition) →
    fs check passes on the result
  - inspect + browse the CHD backup, extract a file, SHA256 vs source
  - cross-tool spot-check: `chdman info disk.chd` reports sensible drive +
    SHA1 for both read modes (manual; not CI)

**Done when:** PR opens, docs in shape, tests green.

---

## Open questions / risks

- **Bad-sector handling.** Today bad sectors get recorded in
  `metadata.json` and zero-filled in the per-partition stream. Same
  approach works inside the disk-image stream: read full extent, zero out
  bad ranges, record them in metadata. Sector-by-sector mode treats bad
  sectors the same way (read attempt → zero on failure → record).

- **GPT alternate header.** Stream builder must emit the GPT backup header
  at the synthesised disk's last 33 LBAs. `Gpt::patch_for_restore` already
  handles this when given the new disk size.

- **Sector-by-sector + resize.** Sector-by-sector reads the source byte
  for byte, which assumes the partition's `imaged_size` equals its source
  size. If the user picks sector-by-sector and *also* requests a smaller
  size, we should reject the combination at config-validate time (or
  fall back to truncate, with a loud warning that data past the new end
  is dropped). Likewise growing a partition with sector-by-sector zero-pads
  the tail — fine, but worth surfacing.

- **APM disks.** Apple Partition Map sources work the same way — partition
  table at sector 0 instead of MBR/GPT. The existing APM emission code
  (e.g. `emit_apm_disk_with_hfs`) plugs into the disk-image stream the
  same way as MBR.

- **Migration.** Out of scope. Old per-partition CHD backups keep working
  forever (layout discriminator defaults to `per-partition`). Users can
  re-back-up if they want the new format.
