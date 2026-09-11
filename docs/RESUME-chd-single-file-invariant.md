# Resume: CHD is a whole disk, and restore must be able to rewrite any label

Read `CONTRIBUTING.md` and `CLAUDE.md` in full first. This doc is the single
place to resume from; it supersedes its own earlier version (commit `e7449c3`).

## The rule (from the user, 2026-09-10)

> A CHD contains a full disk, not a partition. We should never have multiple
> per-partition CHD files. A partitionless disk is fine, because there the
> partition *is* the disk.

Everything below follows from that. A source the single-file layout cannot
assemble is **refused**, never downgraded to `partition-N.chd`.

## Status

| Item | State |
|---|---|
| Task A: partitionless volumes (floppy, HFV, bare HDF) as one whole-disk CHD | **Shipped** (this branch). See "Task A" below. |
| Restore-side resize of a label-scheme CHD backup corrupts silently | **Open, live bug.** Stage 0 below stops it. |
| X68k and DSD still leak `partition-N.chd` | **Open.** Stage 0 refuses them. |
| Label rewrite on restore (Sun / NeXT / SGI / SGI-DkLabel / RDB / AHDI / X68k) | **Planned.** Stages 1-4 below. |

Baseline on `30d4f9b`: preflight green, 3,164 lib tests, `rb-regress` 381/381.
After Task A: 3,165 lib tests (one added), the three new tier-5 cases pass.

## History, so nobody laps this track again

The repo changed its mind about superfloppy + CHD three times. Each commit
was locally right and none wrote the rule down where the code enforces it.

| Date | Commit | What it did |
|---|---|---|
| 2026-05-05 | `5f6378c` | Wrote the CLAUDE.md invariant. Forced superfloppy + CHD to a raw `.img`, silently. Deleted the log line that admitted other tables fall back to per-partition CHD, but not the fallback. |
| 2026-06-04, 07-17 | `f55415f`, `a6fe080` | Added the X68k and DSD tables without touching the CHD gate. Both leaked per-partition CHDs from day one. |
| 2026-08-02 | `5ab770a` | Made the superfloppy forcing warn. |
| 2026-08-02 | `bfbc736` | Removed the forcing entirely to fix a real zstd bug ("honour --format"). Its rationale treated CHD as one more per-partition codec, which reopened the superfloppy leak. |
| 2026-09-10 | `8d814b7`..`30d4f9b` | Routed the disk-label schemes through the single-file layout; per-partition layouts refused (`LABEL_BACKUP_NEEDS_CHD`). Restore side left scheme-blind. |
| 2026-09-10 | `e7449c3` | First version of this doc. It misread the stale comment as "never implemented". |

The two mental models in conflict were "CHD is a codec" and "CHD is a disk
image". The rule above settles it.

## Task A (shipped)

- `single_file_chd::is_supported` accepts `PartitionTable::None`;
  `build_patched_head_segments` emits no head for it (the body starts at
  byte 0). The `!is_superfloppy` term left the gate in `run_backup_inner`.
- A compacted FAT/NTFS/exFAT body sits *shrunk* inside its full extent in
  every single-file CHD (that is the padded-packed design, and it is what an
  MBR disk does today: a 15 MB FAT16 partition comes back with a 8 MB BPB
  after an as-is restore). For a partitionless volume that would have meant
  a 1.44 MB floppy whose BPB says 205 KB, while the per-partition path grows
  it back. So `run_single_file_chd_restore_as_is` now grows a compacted
  partitionless FAT/NTFS/exFAT volume to its extent, and nothing else. The
  MBR/GPT/APM as-is behaviour is unchanged and is a separate decision (see
  "Decisions for the user").
- Pinned by `run_via_staging_round_trip_superfloppy_no_resize`,
  `tests/superfloppy_compression.rs::superfloppy_chd_is_one_whole_disk_container`,
  and `regression-tests/cases/tier5/roundtrip-partitionless.toml` (floppy,
  300 MB HFV, 500 MB SFS HDF). `roundtrip.format.chd` in
  `tier5/roundtrip.toml` was already a superfloppy CHD round trip and still
  passes.
- Old per-partition folders still restore: restore dispatches on
  `metadata.layout`, verified against a folder made before the change.

## The live bug behind "B2"

The old doc could not explain why an X68k prototype produced `Unrecognized
media` on `restore --size minimum` while the resize refusal in
`build_patched_head_segments` never fired. The mechanism:

1. That refusal is **backup-time only**. Restore never calls it.
2. `run_single_file_chd_restore_resize` (`src/restore/mod.rs`, search the
   name) has arms for `"None"`, `"GPT"`, `"APM"` and *everything else is
   MBR*. Sun, NeXT, both SGI schemes, RDB, AHDI and X68k take the MBR arm:
   `patch_mbr_entries` finds no matching entry, sector 0 is written back
   verbatim, and the bodies move anyway.
3. `clear_gpt_structures` zeroes LBAs 1-33 unconditionally. That wipes an
   RDB's PART/FSHD chain, one of the four NeXT label copies, and the X68k
   table at byte 0x800 (the `Unrecognized media`).

Reproduced on a Sun label with a FAT slice (`new hd sun --fill`): exit 0,
slice 1's body moves from LBA 67536 to 34272, the label still says 67536.
The tier-5 label cases never pass `--size`, so nothing catches it.
`rb-cli restore --size minimum` and any non-Original choice in the GUI
restore tab reach this path.

## Plan: label-aware restore

Scoped with two full inventories on 2026-09-10 (every `PartitionTable`
variant's writer/patcher/sidecar, every write-side dispatch site, every test
that pins the current shape). The short version of the inventory:

**Assets that exist.** `provision::write_table` has a from-scratch writer
for all 11 schemes (`src/partition/provision.rs`, `WRITABLE_TABLES`), with
`reserved_head` / `reserved_tail` / `size_granularity` /
`uses_cylinder_geometry` per scheme. `editor::apply_edits` patches NeXT,
Solaris x86, SGI and SGI-DkLabel labels in place. Patchers taking
`PartitionSizeOverride` exist for MBR (`patch_mbr_entries`), GPT
(`Gpt::patch_for_restore`), APM (`Apm::patch_for_restore`), RDB
(`Rdb::patch_for_restore`, an overlay of RDSK/PART blocks) and X68k
(`patch_x68k_entries`). Every scheme's parsed table has serde and a JSON
sidecar (`sun.json`, `next.json`, `sgi.json`, `sgi_dklabel.json`,
`rdb.json`, `ahdi.json`, `x68k.json`).

**Gaps.** Sun has no serializer at all. NeXT, Solaris x86 and AHDI build
from a spec (`AhdiTable::root_to_bytes` zeros the 454-byte bootstrap), not
from parsed bytes. `PartitionSizeOverride` and `calculate_restore_layout`
are 512-byte-LBA only, with a `(255, 63)` CHS fallback that fires for every
label scheme because `detect_alignment` records `(0, 0)` heads/sectors for
them even though the sidecars carry the geometry. `compute_resize_plan`
ignores `PartitionInfo::start_byte` (X68k SASI, 256-byte sectors).
`rbformats::load_table_sidecars` knows only GPT and APM.
`export_whole_disk` / `export_whole_disk_vhd` silently drop size overrides
for anything that is not MBR/APM/RDB. `resize_filesystem_for` has no UFS
resizer, so a Sun or NeXT slice can only ever be restored at its original
size. No regression case resizes a label-scheme backup.

### Stage 0 — stop the bleeding (small; ship first, one commit)

Only changes behaviour where today's behaviour is corruption or an
invariant violation. Each item gets a regression case.

- **0a. Refuse CHD/DVD for tables `is_supported` rejects** (X68k, DSD) in
  `run_backup_inner`, right after the gate, with a message naming
  `--format zstd`. X68k works per-partition with zstd, and its
  resize-on-restore works through the X68k arm of
  `reconstruct_disk_from_backup` (pinned by
  `tests/resize_suite/x68000_resize.rs`, which does not use CHD). Delete the
  two-line comment above the split-size check that describes the leak.
- **0b. Refuse `backup` of a `.dsd` in every format.** Its restore already
  fails ("no MBR data available", `src/rbformats/mod.rs`, the MBR fallback
  of `reconstruct_disk_from_backup`), so nothing that works is lost. See
  "DSD" below for the alternative.
- **0c. Refuse the resize restore for label schemes.** In `run_restore`'s
  single-file dispatch and in `calculate_restore_layout`, bail for any
  `partition_table_type` outside `MBR / GPT / APM / None` when any size
  choice is not Original: "resizing a Sun disk on restore is not supported
  yet; restore at Original size". The GUI restore tab calls
  `calculate_restore_layout` for its fit projection, so the message shows
  there too; also force the size-mode column to Original for those schemes.
- **0d. `clear_gpt_structures` only for MBR.** Its job is "we are writing an
  MBR disk over a possibly-GPT target". GPT writes its own structures; a
  label scheme must not have LBAs 1-33 zeroed.
- Regression: tier-5 `restore --size minimum` on each label fixture exits 1
  with the message, and Original still round-trips; X68k `--format chd`
  exits 1 naming zstd; X68k zstd + `--size minimum` still shrinks 32 -> 16 MiB.

### Stage 1 — one head patcher per scheme (additive, no call-site change)

Add `partition::restore_patch::patch_head_for_restore(table, head: &mut
[u8], overrides, target_size) -> Result<()>` with an exhaustive match, the
same shape as `editor::apply_edits`. It patches **only** start/size fields
and checksums on the verbatim head bytes, so boot code, driver chains and
bootstraps survive. Per scheme:

| Scheme | Patcher | Work |
|---|---|---|
| MBR | `patch_mbr_entries` | exists |
| GPT | `patch_for_restore` + `build_primary_gpt` / `build_backup_gpt` | exists (head + tail) |
| APM | `patch_for_restore` + overlay `build_apm_blocks` | exists (already the overlay pattern) |
| RDB | `Rdb::patch_for_restore` overlay, adapted from `Read+Seek` source to the head buffer | small |
| X68k | `patch_x68k_entries` at `X68K_TABLE_OFFSET(_SASI)`, units = sector size, refresh `disk_size_field` (mirror the X68k arm of `reconstruct_disk_from_backup`) | small |
| Sun | **new**: 8 slots at byte 444 (`cyl u32 BE`, `nblocks u32 BE`), unit = cylinder (`ntrks * nsect`), XOR-16 checksum at 510; slice 2 untouched | ~60 lines |
| NeXT | in-buffer mutators exist (`set_partition_extent`, `stamp_checksum`, `write_copies` in `src/partition/next.rs`); units are 1024-byte sectors past `d_front`; restamp all four copies | ~40 lines glue |
| SGI volhdr | `SgiVolumeHeader::to_bytes` (checksum recomputed); overlapping-slot `fx` disks are already whole-disk bodies, so no override reaches them | small |
| SGI-DkLabel | `write_into` + `apply_byte_order`; must honour the detected word order | small |
| AHDI | **new**: 4 entries at 0x1C6 (flag, id[3], start BE u32, size BE u32), recompute the 0x1234 word-sum; XGM disks are already whole-disk bodies | ~50 lines |
| Solaris x86 | defer: the VTOC lives in the Solaris partition's second sector with slice offsets relative to it; today it is backed up as MBR + `solaris_x86.json`. Refuse resizing the Solaris partition until `write_label` is wired | refuse |
| None | no-op | — |
| DSD | bail | — |

Tests per scheme: build with `provision::write_table`, parse, patch with an
override, re-parse, assert the new start/size, a valid checksum, and that
every byte outside the entries is unchanged (boot code, FSHD chain,
bootstrap). `every_writable_table_writes_and_reparses` in `provision.rs` is
the pattern.

### Stage 2 — layout rules per scheme (additive)

- A `SchemeLayoutRules { granularity, reserved_head, reserved_tail,
  cylinder_bytes: Option<u64>, fixed_slots }` derived from the sidecar JSON,
  reusing `provision::{reserved_head, reserved_tail, size_granularity,
  uses_cylinder_geometry}`. `calculate_restore_layout` consults it instead
  of the MBR assumptions (`first_partition_lba`, EBR gaps, `(255, 63)`).
  Fixed slots: Sun slice 2 (whole disk), the SGI volume-header slots, the
  RDB reserved blocks through `rdb_blk_hi`.
- Record geometry for label schemes in `AlignmentMetadata` at backup time
  (`detect_alignment` returns `(0, 0)` today; the labels carry
  `ntrks/nsect`, `ntracks/nsectors/front_porch`, `heads/sectors`,
  `surfaces/blk_per_trk`). Additive; old backups fall back to the sidecar.
- `compute_resize_plan` works in bytes (`byte_offset()`), so X68k SASI plans
  correctly.
- Ask `in_place_resize_support` before accepting a non-Original size: a
  UFS/FFS slice reports `Unsupported`, and the layout refuses instead of
  moving a body it cannot shrink. The min-size runner probes the filesystem,
  so the GUI "Minimum" for such a slice must collapse to Original.

### Stage 3 — wire it in, one scheme at a time

- `run_single_file_chd_restore_resize`: read the **head region** from the
  CHD (bytes before the first partition, same rule as
  `read_label_head_region`), call the patcher, write head + bodies; GPT tail
  as today; `clear_gpt_structures` only for MBR (Stage 0d).
- `build_patched_head_segments`: replace the verbatim arm's refusal with the
  patcher for schemes that have one. `is_verbatim_head_scheme` keeps meaning
  "the head is carried as the base bytes"; the refusal keys on patcher
  availability. Update `is_supported_covers_the_disk_label_schemes` and the
  `end_to_end_round_trip_*` assertions accordingly.
- `reconstruct_disk_from_backup`'s MBR fallback bails with a scheme-named
  message instead of "no MBR data available". Per-partition layouts for
  label schemes stay refused at backup time (RDB's FSHD/LSEG chain needs the
  verbatim head, which only the CHD carries).
- `rbformats::load_table_sidecars` learns every sidecar.
- Regression: tier-5 `restore --size minimum` for Sun (FAT slice), NeXT,
  SGI, SGI-DkLabel, RDB (the PFS3 compacted fixture), AHDI and X68k: inspect
  shows the shrunk layout, fsck clean, an extracted file byte-identical.
  Stage 0c's refusal cases flip to success one scheme at a time.

### Stage 4 — exports and GUI

- `export_whole_disk` / `export_whole_disk_vhd` route through the patcher or
  refuse; today they drop overrides silently for non-MBR/APM/RDB.
- `inspect_tab::build_chd_partition_context` returns `None` for an
  unsupported table and the export silently loses its resize; surface it.
- Restore tab: size-mode column enabled per scheme by patcher availability
  and `in_place_resize_support`.

### Stage 5 (optional) — X68k as a single-file CHD

With the X68k patcher (Stage 1) and byte-based planning (Stage 2), add X68k
to `is_supported`. Three things from the old doc still apply: the
`packed_partition_reader_padded` early return for type-string partitions
packs Human68k without padding (a follow-up chip covers it); there is no
real SASI/SCSI HDD fixture (only `.d88` floppies; `rb-cli new hd x68k` makes
the 512-byte synthetic shape); and the boot region ahead of the table is
not just boot code: `X68SCSI1` at byte 0 selects the 1024-byte sector unit,
so it must ride verbatim, which the single-file layout does and today's
per-partition restore (zero-fill) does not. Until then Stage 0a's refusal
stands.

### No-break guarantees

- `metadata.layout` dispatch is never touched; per-partition folders are
  unaffected by every stage.
- Stages 1-2 add code and tests only. Stage 3 flips call sites behind
  patcher availability; the tier-5 Original-size cases guard the verbatim
  path throughout.
- The single-file as-is restore stays a byte copy for every partitioned
  scheme.

## DSD, and floppy containers generally

Inventory result: of every container the engine decodes (`.d88`, `.woz`,
`.moof`, `.g64`, `.msa`, `.atr`, `.dim`, `.dc42` Twiggy, `.od`, `.adz`,
Apple II `.do/.po`, ...), **only `.dsd`** has the property "the reader
transforms the bytes *and* the table splits into more than one piece". All
the others decode to one flat volume, land as `PartitionTable::None`, and
back up fine under Task A; restore emits a flat `.img` (documented one-way,
`restore/superfloppy_wrap.rs`). There is no wider "floppy formats conflict
with CHD" class to exclude. The CHD exclusion list is exactly
`!is_supported`, which already holds `Dsd` and `X68k`; what was missing is
the refusal (Stage 0a).

For `.dsd` itself:

- Its backup is already unrestorable in every format (see Stage 0b).
- A de-interleaved `side0 || side1` image is not a format anything reads.
- Recommended: refuse `backup` of a `.dsd` (Stage 0b), keep read / edit /
  `convert`. A `.dsd` is a 200-400 KB file; copying it is its backup.
- Alternative, if "back up the file bytes" is wanted: route `Dsd` through
  `whole_disk_partition` (the raw interleaved container as one body, restore
  a byte copy) and give it the same no-head arm as `None`. That yields a
  faithful single-file CHD of the container, but MAME will not load a
  floppy CHD, so it buys nothing over copying the file.

## Decisions for the user

1. **DSD**: refuse (recommended) or whole-container body. See above.
2. **Compacted FAT/NTFS/exFAT inside an MBR/GPT/APM single-file CHD**: after
   an as-is restore the volume stays shrunk inside its full partition (a
   15 MB partition with an 8 MB FAT16). The per-partition layout grows it
   back. Task A grows it back for partitionless volumes only. Extending the
   grow to every table is a one-block change in
   `run_single_file_chd_restore_as_is`, mirrors the per-partition semantics,
   and would make the two layouts agree; it changes what a restored MBR
   backup looks like, so it is called out rather than done.
3. **Stage 0c** refuses something that "works" today only in the sense of
   exiting 0. Confirm that refusing is acceptable until Stage 3 lands.

## Ground truth (measured 2026-09-10; re-verify before relying on it)

| Source | `backup --format chd` before Task A | after |
|---|---|---|
| Superfloppy (`PartitionTable::None`) | `partition-0.chd`, logical size 8,704 for a 1.44 MB floppy | `<name>.chd`, logical 1,474,560 |
| DSD | `partition-1.chd` and `partition-2.chd` | unchanged (Stage 0b) |
| X68k | `partition-0.chd` | unchanged (Stage 0a) |

```bash
rb-cli new floppy fat /tmp/fl.img --size 1440K
rb-cli backup /tmp/fl.img /tmp/bk --name job --format chd && ls /tmp/bk/job
rb-cli inspect /tmp/bk/job/job.chd      # Logical size: 1,474,560 bytes
```

CHD needs no partition table: `compress_chd` takes a logical size, a hunk
size and codecs; `GDDD` geometry is derived from the size inside
`libchdman-rs`. Do not infer a table for a partitionless volume.

## Code map

| Location | What it is |
|---|---|
| `src/backup/mod.rs`, `single_file_chd_planned` | the gate: CHD/DVD, no split, `is_supported` |
| `src/backup/mod.rs`, `LABEL_BACKUP_NEEDS_CHD` | per-partition refusal for the label schemes |
| `src/backup/single_file_chd.rs`, `is_supported` | the scheme allowlist; also gates the GUI Inspect-tab CHD export |
| `src/backup/single_file_chd.rs`, `is_verbatim_head_scheme` | the list Stage 3 re-keys on patcher availability |
| `src/backup/single_file_chd.rs`, `build_patched_head_segments` | backup-time head patching; the None arm and the label refusal live here |
| `src/backup/single_file_chd.rs`, `build_partition_reader` | the `debug_assert_eq!` that hides in release |
| `src/restore/mod.rs`, `run_single_file_chd_restore_as_is` | byte copy; Task A's partitionless grow |
| `src/restore/mod.rs`, `run_single_file_chd_restore_resize` | the scheme-blind resize path (Stage 0c / 3) |
| `src/restore/mod.rs`, `calculate_restore_layout` | 512-LBA layout with MBR assumptions (Stage 2) |
| `src/restore/mod.rs`, `clear_gpt_structures` | zeroes LBAs 1-33 (Stage 0d) |
| `src/rbformats/mod.rs`, `reconstruct_disk_from_backup` | per-partition restore; X68k arm; MBR fallback |
| `src/partition/provision.rs`, `write_table` | the complete set of from-scratch writers to borrow from |
| `src/partition/editor.rs`, `apply_edits` | the in-place label editors (NeXT, Solaris, SGI, DkLabel) |
| `src/fs/mod.rs`, `packed_partition_reader_padded` | padding; the type-string early return is a follow-up chip |
| `src/fs/mod.rs`, `in_place_resize_support` | ask before moving a body whose filesystem cannot shrink |

## Traps

- **Do not infer a partition table for a partitionless volume.** CHD has
  no such concept.
- **Restore keys off `metadata.layout`.** A scheme-aware arm in the
  single-file resize path cannot touch a per-partition folder. Verify with
  a folder made before your change anyway.
- **`debug_assert` hides in release.** `build_partition_reader` panics in
  debug and degrades to raw passthrough in release with one log line.
- **Do not widen `LABEL_BACKUP_NEEDS_CHD` to partitionless volumes.** They
  back up in every format; only their CHD shape was wrong.
- `--split-size` + CHD is already refused by `validate_backup_config`; the
  `split_size_mib.is_none()` term in the gate is belt and braces.
- The old per-partition X68k CHD restore zero-fills the boot region; a real
  SCSI disk's `X68SCSI1` signature selects the sector unit, so that path is
  a fidelity bug, not a nicety. Stage 5 is the fix; do not "improve" the
  zero-fill.

## Verification

```bash
cargo test --lib
cargo test --test superfloppy_compression
scripts/preflight.sh
./regression-tests/runner/target/release/rb-regress run --tiers 5 --filter roundtrip
```
