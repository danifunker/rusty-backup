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

### Stage 3 — backup tab UI for CHD output

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
- Stage 4d — layout-preserving FAT/NTFS/exFAT compact readers so smart
  mode benefits more filesystems.

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

### Stage 5 — restore branch

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
