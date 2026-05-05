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

### Stage 2 — disk-image stream composer ✅

Scope shrunk from the original draft: Stage 2 is the **pure sparse-stream
composer** that takes ordered `(offset, length, Read)` segments and emits
a `Read` over the synthesised disk, zero-filling gaps. The orchestration
that builds those segments from a real source + partition table + resize
plan moves to Stage 4 (backup orchestration). This split keeps Stage 2
fully unit-testable without any filesystem I/O.

- [x] New module `src/backup/disk_image_stream.rs` with
      `DiskImageStreamBuilder` + `DiskImageStream`. Forward-only `Read` —
      `Seek` isn't needed because `compress_chd` only reads forward.
- [x] Builder enforces non-overlapping, in-order segments at registration
      time so a misconfigured caller fails fast rather than producing a
      corrupt CHD.
- [x] Source readers shorter than their declared `length` get zero-padded;
      readers longer than declared get truncated. Both behaviors tested.
- [x] 11 inline unit tests covering: empty stream, single segment at
      origin, segment in middle, two segments with gap, source-short
      zero-pad, source-long truncate, out-of-order rejection, overlap
      rejection, past-end rejection, zero-length rejection,
      touching-segments-no-gap.
- [x] Module declared in `src/backup/mod.rs`. No production callers yet —
      Stage 4 wires it into the backup orchestrator.
- [x] `cargo fmt` clean; `cargo build --all-targets` zero warnings;
      `cargo test --lib` 638 passing.

**Done when:** `cargo test backup::disk_image_stream` green; no callers yet. ✅

---

### Stage 3 — backup tab UI for CHD output ✅

**Stage 3b — picker UI + plumbing ✅**

- [x] Backup tab embeds the shared `size_mode_row` widget inline when
      CHD output is selected. Default selection per row is `MinPlus20`
      when the partition's minimum is known and below source size, else
      `Original`. The deferred Calc-min button + spinner is wired through
      the existing `pending_min_size_calcs` map.
- [x] Split-size control is hidden for CHD and DVD output (chdman /
      MAME load a single file). VHD continues to suppress it as before.
- [x] `BackupConfig` gained `size_policy: Option<SizePolicy>` and
      `partition_target_sizes: Option<Vec<(usize, u64)>>`. The values
      land in `metadata.json` for traceability.
- [x] **Resize execution itself is Stage 4b**: when the picker selects
      anything other than Original, `run_backup` logs a warning and the
      CHD body still uses source sizes. Recording the user's intent
      keeps the next stage's data flow intact without misleading users.
- [x] Last-used read-mode + size-policy persistence in `UpdateConfig`
      is deferred to Stage 9 (UI polish).
- [x] `cargo build --all-targets` zero new warnings; `cargo test --lib`
      647 passing.

**Done when:** the backup tab shows the right controls for CHD output and
their values plumb through to a `BackupConfig` extension. ✅

---

### Stage 4 — backup orchestration branch ✅ (initial slice)

Initial slice landed: MBR sources only, source layout preserved (no
backup-time resize yet), smart compaction = layout-preserving compact
reader where available with raw passthrough fallback. GPT/APM/superfloppy
sources fall back to the legacy per-partition layout with a logged
explanation. `split_size_mib` paired with CHD output logs a warning and
is ignored.

- [x] `src/backup/single_file_chd.rs` — `SingleFileChdInputs`,
      `SingleFileChdResult`, `is_supported`, `run`, plus end-to-end
      round-trip test (synthetic 4 MiB MBR disk → CHD → byte-equal
      readback via `ChdReader`).
- [x] `run_backup` branches when `compression == Chd|Dvd` and the table
      type is supported, calling `single_file_chd::run` and writing a
      `single-file-chd` `metadata.json`. Returns early; per-partition
      loop is skipped.
- [x] Restore stub: `run_restore` rejects `BackupLayout::SingleFileChd`
      with a clear error pointing the user at chdman/MAME for now —
      keeps the user out of a corrupt-restore footgun until Stage 5.
- [x] Inspect-tab redirect: when the user opens a single-file-chd
      backup folder, swap the source to `folder/disk.chd` and run the
      existing CHD-image inspect/browse path. Gives users immediate
      browsability of their backup without new GUI plumbing.
- [x] `cargo fmt` clean; `cargo build --all-targets` zero warnings;
      `cargo test --lib` 644 passing.

Follow-ups tracked under their own stages:

- Stage 4b ✅ — backup-time resize. `single_file_chd::run` now accepts
  `resize_targets` + `alignment_sectors`; non-trivial plans take the
  sparse-temp-file pipeline. `run_backup` drops its prior warning and
  records actual resized sizes/offsets in `metadata.json` (with
  `resized: true` per partition that moved or changed size). Round-trip
  test (`resize_plan_round_trip_shrinks_mbr_partition`) verifies a 4 MiB
  MBR disk's 0x83 partition halves cleanly: patched MBR entry, body at
  declared offset, dropped tail zero-filled.

  **Approach (decided):** sparse temp-file scratch. When the resize plan is
  non-trivial, `single_file_chd::run` bypasses `DiskImageStream` and
  assembles the synthesised disk into a sparse temp file: patched
  MBR/GPT/APM at sector 0, each partition body written at its new offset
  via the same compact-or-raw reader logic as today, then
  `resize_*_in_place` and `patch_hidden_sectors_for` run against the temp
  file. Finally the temp file is handed to `compress_chd` as a `Read`. The
  Original-sized path keeps the streaming composer. Rationale: code reuse
  with `reconstruct_disk_from_backup`'s resize+patch pipeline outweighs
  the temporary disk cost (sparse files mean only the actually-used
  regions hit disk), and FS resize functions need `Read + Write + Seek`
  which the forward-only stream can't provide.
- Stage 4c ✅ — GPT + APM source support landed.
- Stage 4d ✅ — layout-preserving FAT/NTFS/exFAT readers. New module
  `src/fs/layout_preserving.rs` houses a generic `LayoutPreservingReader<R>`
  that streams source bytes verbatim except inside caller-supplied zero
  ranges. Each of `CompactFatReader` / `CompactNtfsReader` /
  `CompactExfatReader` gains an `into_layout_preserving()` method that
  reuses the already-parsed allocation state to derive free-cluster byte
  ranges and hand back a `LayoutPreservingReader` + `CompactResult` with
  `compacted_size == original_size`. New `fs::layout_preserving_partition_reader()`
  dispatcher routes FAT/NTFS/exFAT through the new path and forwards the
  HFS/HFS+/ext/btrfs/ProDOS readers (already layout-preserving) unchanged.
  `single_file_chd::build_partition_reader` calls the new dispatcher
  before falling back to raw passthrough. Round-trip test
  (`layout_preserving_fat_zeros_free_clusters`) verifies a synthetic
  FAT12 partition with garbage-filled free clusters round-trips with
  metadata + allocated regions byte-equal and free regions zeroed.

**Stage 4c — GPT + APM source support ✅**

- [x] `is_supported` accepts `Gpt { .. }` and `Apm(_)` (in addition to
      MBR). Superfloppy still falls back.
- [x] `build_head_segments` selects the right head (and, for GPT, tail)
      shape per table type:
      - **MBR**: 512-byte sector verbatim from caller-provided buffer.
      - **GPT**: protective MBR (verbatim) + freshly-built primary GPT
        (33 sectors at LBA 1–33) at the head; freshly-built backup GPT
        at the last 33 LBAs.
      - **APM**: bytes 0..first_partition_offset read eagerly from the
        source. Eager-read sidesteps a `try_clone`/`dup` fd-offset race
        with the partition-loop seeks.
- [x] `run_backup` skips the raw `mbr.bin` / `gpt.bin` / `apm.bin`
      sidecars on the single-file CHD path for GPT and APM (parallels
      the MBR behavior already in place); the JSON sidecars stay for
      fast inspect.
- [x] Round-trip unit tests: `end_to_end_round_trip_gpt` (4 MiB GPT
      disk → CHD → re-detect as GPT after readback) and
      `end_to_end_round_trip_apm` (DDR + APM map + HFS-shaped data
      partition → CHD → head verbatim, partition body byte-equal,
      re-detects as APM).
- [x] `cargo build --all-targets` zero new warnings;
      `cargo test --lib` 647 passing.

**Original spec preserved below for reference.**

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

### Stage 5 — restore branch ✅

**Stage 5a — as-is restore ✅**

- [x] `run_restore` branches on `BackupLayout::SingleFileChd` and calls
      `run_single_file_chd_restore_as_is`, replacing the prior
      "not implemented" bail.
- [x] Helper opens `disk.chd` via `ChdReader`, pre-checks
      `target_size >= container_logical_size`, and `read_exact` /
      `write_all` 1 MiB chunks through the existing `SectorAlignedWriter`.
      Cancel polled at chunk boundaries; progress driven off bytes written.
- [x] Per-partition size choices coming from the restore tab are logged
      as ignored on this path — re-resize is Stage 5b.
- [x] Round-trip unit test (`single_file_chd_as_is_restore_round_trips`)
      builds a synthetic CHD backup folder, runs `run_restore`, and
      asserts the restored image is byte-equal to the source disk.
- [x] `cargo build --all-targets` zero new warnings; `cargo test --lib`
      645 passing.

**Stage 5b — re-resize restore ✅**

- [x] `run_restore` routes `BackupLayout::SingleFileChd` to
      `run_single_file_chd_restore_resize` when any per-partition size
      choice is non-`Original`; otherwise stays on the as-is path.
- [x] New helper extracts each partition body from `disk.chd` via
      `ChdReader` (seek to in-CHD offset + read `imaged_size_bytes`),
      writes it at the user-chosen new offset on the target, zero-pads
      grow regions, then runs `fs::resize_filesystem_for` +
      `patch_hidden_sectors_for` for FS metadata fixups. Patched MBR /
      primary GPT / APM head land at sector 0; backup GPT lands at the
      target's last 33 LBAs for GPT restores. Logical / extended-MBR
      backups bail with a clear error (the EBR rebuild from the
      per-partition path isn't wired in yet).
- [x] `RestoreSizeChoice::Original` on this path means "as-stored in the
      CHD" (i.e. `imaged_size_bytes`), since the CHD is the source of
      truth post-Stage-4b. The function pre-swaps
      `original_size_bytes := imaged_size_bytes` before calling
      `calculate_restore_layout` so the existing validator (which
      forbids shrink-below-imaged) keeps its meaning.
- [x] Round-trip test
      (`single_file_chd_re_resize_restore_grows_partition`) restores a
      4 MiB MBR-disk CHD with a Custom-grown partition and verifies the
      patched MBR + imaged region match + zero-padded grow region.
- [x] `cargo test --lib` 649 passing; `cargo fmt --check` + `cargo
      clippy --lib` clean (no new warnings).

**Original spec preserved below for reference.**

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

### Stage 6 — inspect + browse on single-file-chd backups ✅

Delivered via the Stage 4 redirect: opening a single-file-chd backup
folder in inspect tab swaps the source to `<folder>/<container>` and re-
runs inspect on the CHD directly. Browse, per-partition extract, and the
"CHD Info" button all come for free from the existing CHD-image inspect
path. The metadata.json sidecars (`mbr.json` / `gpt.json` / `apm.json`)
remain in the folder for users who want to peek at the recorded layout.

Trade-off accepted: the backup-folder framing (size_policy,
container_sha1, recorded checksums) doesn't surface in the inspect view —
users see the CHD as a disk image, not as a backup. Acceptable because
the CHD is the source of truth and any in-CHD partition data is reachable
through the standard inspect/browse flow.

Container filename: `<backup_name>/<backup_name>.chd` (the backup folder's
own name, not the literal `disk.chd`). `metadata.container` records the
actual filename so old backups written with `disk.chd` continue to load.

**Done when:** opening a single-file-chd backup in inspect tab shows the
partition list + lets the user browse + extract files. ✅

---

### Stage 7 — edit mode on single-file-chd backups ✅

The chd_edit flow (compressed-CHD diff-against-parent + flatten-on-save)
already covers the editing side. The Stage 6 redirect means the user is
editing the backup's `<container>` directly, so no new edit plumbing was
needed. What Stage 7 added:

- New helper `backup::single_file_chd::refresh_metadata_after_edit(folder, log_cb)`
  reads `metadata.json`, re-opens the container, and conservatively
  recomputes every partition's SHA-256 (over `[start_lba * 512,
  +imaged_size_bytes)`) plus the container's own SHA-1, then writes the
  metadata back. Conservative recompute beats parsing the diff's hunk
  map and is fast enough for typical backup sizes.
- `BrowseView` gained `single_file_chd_backup_folder: Option<PathBuf>`
  + `set_single_file_chd_backup_folder(folder)`. After a successful
  `chd_edit::flatten_to_parent`, `poll_chd_flatten` calls the helper
  when the field is set; failures surface as a warning suffix on the
  "Changes saved successfully." message rather than rolling back the
  on-disk save (the bytes are already correct).
- `InspectTab` mirrors the field. The Stage 6 redirect now records the
  original backup folder *after* `run_inspect` (which would otherwise
  clear it via `clear_results`); `open_browse`'s raw-image branch
  forwards it into `BrowseView` whenever it's set.

Round-trip test: `refresh_metadata_after_edit_recomputes_checksums`
builds a real backup, writes a `metadata.json` with stale partition + container
checksums, and asserts the helper restores both to the live values.

**Done when:** edit mode on a single-file-chd backup adds/removes files in
one partition and saves correctly, with metadata checksums updated. ✅

---

### Stage 8 — re-export with resize (inspect-tab Export Disk Image) ✅

**Stage 8a — backend (`run_export` + estimator + free-space helper) ✅**

- [x] `os::available_space(path) -> Option<u64>` cross-platform helper
      (libc `statvfs` on macOS/Linux, `GetDiskFreeSpaceExW` on Windows).
      Returns `None` rather than `0` when the query fails so callers
      can distinguish "couldn't tell" from "really full".
- [x] `single_file_chd::ExportDiskEstimate` + `estimate_export_disk_usage`.
      Disk-usage math per the in-thread analysis: sparse temp scratch
      consumes ~`sum(partition imaged_size_bytes) + 1 MiB head/tail
      allowance` (grow regions cost ~0 because they're sparse holes that
      compress_chd encodes as zero-hunks); output `.chd` upper bound is
      the source CHD's existing file size when re-exporting from CHD,
      else the source data size for raw images.
- [x] `single_file_chd::SingleFileChdExportInputs` + `run_export`. The
      no-resize path streams source → `compress_chd` directly via
      `wrap_image_reader` (works for raw, VHD, 2MG, *and* CHD sources —
      no scratch needed). The resize path needs raw `File` semantics, so
      CHD sources are decompressed to a temp `.img` next to the dest
      first (cleaned up by RAII guard); then the existing `run`
      machinery drives the temp-file resize pipeline. Output filename
      derived from `dest_path.with_extension("")`.
- [x] Three new tests: `run_export_no_resize_round_trips_raw_to_chd`,
      `run_export_with_resize_shrinks_partition`, and
      `estimate_export_disk_usage_matches_partition_layout`. 661 lib
      tests passing.

**Stage 8b — UI + sector-by-sector warning ✅**

- [x] Inspect-tab Export Disk Image popup keeps the existing per-
      partition `size_mode_row` picker (already shown there for VHD
      per-partition / whole-disk exports). For CHD output the picker
      values now flow through to `single_file_chd::run_export` instead
      of being ignored.
- [x] `start_native_whole_disk_chd` gained a
      `partition_context: Option<NativeWholeDiskChdPartitionContext>`
      parameter. When provided (and format is `Chd` / `ChdDvd`), the
      worker routes through `run_export`; otherwise it falls back to
      the legacy raw-stream `export_whole_disk_chd` (no resize).
- [x] Inspect-tab `build_chd_partition_context` reads sector 0 from the
      source (`ChdReader` for `.chd` sources, `File::open` otherwise),
      copies the parsed table + partitions + alignment, derives
      `resize_targets` from the picker (filters out no-op entries — an
      empty Vec keeps the no-scratch fast path inside `run_export`).
- [x] On-screen workflow banner + disk-usage estimate: visible in the
      popup whenever CHD output is selected for whole-disk export. Picks
      one of four workflow strings depending on (CHD vs raw source) ×
      (resize vs not), then shows scratch + output + total estimates in
      MiB sourced from `estimate_export_disk_usage`. Final free-space
      check runs at Export-click time (because we don't know the dest
      directory until then), comparing `os::available_space(dest_dir)`
      against `estimate.required_total()` — blocks the export with an
      error message if insufficient and measurable.
- [x] Sector-by-sector source warning: triggered when the user is
      re-exporting *with resize* and the source is the body of a
      single-file-CHD backup whose `metadata.json` records
      `sector_by_sector: true`. Surfaces as an amber notice that the
      byte-for-byte preservation of free space + unrecognized FS
      regions only carries over when re-exporting at original sizes.

**Done when:** the Export Disk Image popup lets the user re-export an
opened source (raw image or `.chd`) to a CHD with new partition sizes,
shows the workflow + disk-usage estimate, blocks on insufficient free
space, and warns when resizing a sector-by-sector source. ✅

---



The inspect-tab "Export Disk Image..." popup already emits whole-disk CHDs
from raw image / device sources. Extend just that flow with the backup-
time size picker so users can re-export an opened source (including a
loaded `.chd`) with new partition sizes — a "re-resize this disk" workflow
without leaving the app.

Bulk-convert is intentionally out of scope: it batches many heterogeneous
inputs and a per-input partition picker doesn't fit that UI.

- Inspect-tab Export Disk Image popup: when format is CHD (Hard Disk),
  embed the shared `size_mode_row` widget per partition (sourced from the
  inspect tab's already-parsed partition table + min-size cache, same
  plumbing the backup tab uses in Stage 3b).
- `export_whole_disk_chd` (or its single-file-CHD-aware sibling) accepts
  an optional resize plan; falls back to "Original" when none is given
  (current behavior preserved).
- Read-mode radio is *not* added here — inspect-tab export is always
  smart-compact today, and sector-by-sector re-export from an already-
  imaged source is meaningless.

**Done when:** with a raw image or `.chd` open in inspect, the user can
pick new partition sizes in Export Disk Image and emit a CHD with the new
layout. Bulk-convert remains unchanged.

---

### Stage 9 — UI cleanup + per-CHD split disablement ✅

- [x] Backup tab: split-archives checkbox + split-size DragValue are now
      hidden entirely when CHD/DVD output is selected (previously rendered
      disabled with an explanatory label). VHD continues to render the
      checkbox disabled with a "(not available for VHD)" note since users
      may still want to see why splitting isn't on offer for that path.
- [x] `BackupConfig::split_size_mib` was already ignored for CHD/DVD with a
      `LogLevel::Warning` ("Split-size set with CHD output: splitting is
      incompatible with chdman/MAME single-file CHDs and is ignored.")
      emitted from `run_backup` — see `src/backup/mod.rs:463`. Stage 9
      verified the path; no code change needed beyond the UI hide.
- [x] Small note rendered immediately below the Output Format row when CHD
      is selected: "Output: single CHD file (chdman/MAME compatible)."
      Sits above the CHD codec / hunk-size knobs so it lands next to the
      radio without crowding the format row itself.
- [x] `cargo build --all-targets` clean.

**Done when:** UI no longer offers nonsensical options; warnings cover any
config-file edge case. ✅

---

### Stage 10 — docs + e2e tests ✅

- [x] `PROJECT-SPEC.md` Phase 4 now documents both backup layouts. The
      legacy per-partition tree is presented next to a single-file-CHD
      tree (`metadata.json` + table sidecars + `<backup-name>.chd`), with
      a paragraph on how `metadata.partitions[i].offset_in_disk` records
      byte ranges inside the CHD's logical stream and why splitting is
      disabled for CHD output.
- [x] `README.md` Backup-tab description updated: each backup folder
      contains either one compressed file per partition (Zstd / Raw /
      per-partition VHD) or a single `<backup-name>.chd` disk image that
      `chdman info` opens and MAME loads directly. Notes that edit mode
      flows changes back into the CHD so any CHD-aware tool still reads
      it.
- [x] E2E coverage. Most of the matrix was already in place from earlier
      stages; Stage 10 added the missing sector-by-sector full round-trip
      through restore. Final inventory:
      - `end_to_end_round_trip_single_partition_chd` /
        `end_to_end_round_trip_gpt` / `end_to_end_round_trip_apm` —
        smart-compact backup → CHD readback byte-equal for MBR, GPT,
        APM heads (sector_by_sector flag exercised on the APM case so
        the raw-passthrough branch is covered at backup level too).
      - `resize_plan_round_trip_shrinks_mbr_partition` — smart-compact
        + custom shrink → CHD body matches new size with patched MBR.
      - `single_file_chd_as_is_restore_round_trips` — smart-compact
        backup → as-is restore byte-equal target.
      - `single_file_chd_sector_by_sector_round_trips_through_restore`
        (NEW) — sector-by-sector + Original → CHD backup → as-is
        restore byte-equal target. Asserts
        `container_logical_size == source_size` per the Stage 10 spec.
      - `single_file_chd_re_resize_restore_grows_partition` — smart-
        compact CHD → re-resize restore growing one partition. (Shrink
        variant deferred: requires a real-FS source where backup-time
        compaction makes `imaged_size_bytes < new_size`, since the
        validator forbids restore-shrink past `imaged_size_bytes`.
        Backup-time shrink is already covered by
        `resize_plan_round_trip_shrinks_mbr_partition` and the grow
        test exercises the same restore-resize plumbing in the other
        direction.)
      - `layout_preserving_fat_zeros_free_clusters` — synthetic FAT12
        partition with garbage in free clusters round-trips with
        metadata + allocated regions byte-equal and free regions
        zeroed (covers the inspect / browse / extract chain at the
        layout-preserving reader level; full BrowseView extraction
        exercised via the editing test below).
      - `refresh_metadata_after_edit_recomputes_checksums` — edit-mode
        metadata helper recomputes container SHA-1 + per-partition
        SHA-256 against the live CHD.
      - `run_export_no_resize_round_trips_raw_to_chd` /
        `run_export_with_resize_shrinks_partition` — inspect-tab
        re-export to CHD with and without resize.
- [x] Cross-tool spot-check (`chdman info disk.chd` recognises both
      read modes) flagged as manual, not in CI — kept out per the
      original spec.
- [x] `cargo test --lib`: 662 passing.

**Done when:** PR opens, docs in shape, tests green. ✅

---

## Open questions / risks (resolved)

- **Bad-sector handling.** Parity with the per-partition path: neither
  populates `metadata.bad_sectors` in production today (the field is
  defined but no code path writes to it). Read errors propagate as IO
  errors and abort the backup in both layouts. Wiring up real bad-sector
  recording is a separate cross-cutting effort, not specific to this
  plan; tracking it under `docs/TODO_missing_features.md` if/when it
  matters.

- **GPT alternate header.** Resolved in Stage 4c — `build_head_segments`
  emits a freshly-built primary GPT at LBA 1–33 and a backup GPT at the
  last 33 LBAs of the synthesised disk; `Gpt::patch_for_restore` is used
  on the resize path so both copies stay consistent.

- **Sector-by-sector + resize.** Resolved by disallowing the combination
  outright. Sector-by-sector backups are point-in-time forensic copies
  that preserve free space and unrecognized filesystem regions; resizing
  at backup time would defeat the property. Enforced in two places:
  the backup tab hides the per-partition size picker when sector-by-
  sector is checked (with an explanatory note), and `single_file_chd::run`
  bails defensively if a caller still hands it `resize_targets` with
  changes. Re-export via Inspect (Stage 8) remains the supported way to
  change sizes after the fact, with a warning that the byte-for-byte
  property is lost in the re-exported file.

- **APM disks.** Resolved in Stage 4c — `build_head_segments` reads the
  pre-first-partition region (DDR + map + Apple_Driver* partitions)
  verbatim from the source. APM-aware emission already existed
  (`emit_apm_disk_with_hfs`) but the single-file-CHD path doesn't need
  it: copying the source's head bytes preserves bootability without re-
  synthesising the map.
