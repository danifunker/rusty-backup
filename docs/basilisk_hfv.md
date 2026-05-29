# BasiliskII HFV support — implementation plan

Living plan + progress tracker for full read/write support of **HFV** disk
images (the flat HFS volume images used by the BasiliskII / SheepShaver 68k Mac
emulators).

Update the checkboxes and the **Progress log** at the bottom as work lands. Keep
each step small enough to be one commit (see CONTRIBUTING.md "Adding a feature:
the playbook" — engine layer first, then model, then view, then tests).

Status legend: `[ ]` not started · `[~]` in progress · `[x]` done · `[-]` dropped/out of scope

---

## 1. What an HFV actually is

An `.hfv` (a.k.a. `.HFV`) file is a **flat, partition-less raw image of a single
classic HFS volume**:

- Sector 0–1: HFS boot blocks (signature `0x4C4B` = `"LK"`; all-zero on a
  non-bootable volume).
- Sector 2 (byte offset **1024**): HFS Master Directory Block, signature
  `0x4244` = `"BD"`.
- No MBR, no GPT, no APM, no Apple driver descriptor. The HFS volume *is* the
  whole file, starting at byte 0.

Confirmed against three local samples:

| File | Size | Boot blocks | MDB | Volume name |
|------|------|-------------|-----|-------------|
| `~/Downloads/THINKCDEV.hfv` | 30 MB | zeroed (non-boot) | `BD` @1024 | "THINK C dev" |
| `~/Downloads/HFV Files/Mac OS 8.1.HFV` | 300 MB | `LK` (bootable) | `BD` @1024 | "Mac HDD" |
| `~/Downloads/HFV Files/Starterdisk.hfv` | 500 MB | `LK` (bootable) | `BD` @1024 | "Starter Disk" |

BasiliskII treats it exactly this way: `src/disk.cpp` `find_hfs_partition()`
scans the first 64 blocks for an `Apple_HFS` APM entry, fails (there is none),
and falls back to a **flat image** with `num_blocks = filesize / 512`
(disk.cpp:203). The `.hfv` extension is registered in `extfs_unix.cpp:216`
(`FOURCC('D','D','i','m')` / `FOURCC('d','d','s','k')`).

### How this maps onto rusty-backup today

The flat-HFS-at-offset-0 shape already flows through the existing **superfloppy**
path with **no new detection code**:

- `partition::detect_superfloppy()` (src/partition/mod.rs:99-215) reads offset
  1024, matches `0x4244` → `fs_hint = "HFS"`, returns
  `PartitionTable::None { size_bytes, fs_hint: "HFS" }`.
- `PartitionTable::partitions()` (src/partition/mod.rs:515-550) emits one
  `PartitionInfo { index: 0, type_name: "HFS", partition_type_byte: 0,
  start_lba: 0, partition_type_string: None }`.
- `fs::open_filesystem(reader, offset=0, type_byte=0, …)` auto-detects
  (src/fs/mod.rs:980-1031) → `detect_filesystem_type` finds `"hfs"` at offset
  1024 → `hfs::HfsFilesystem::open(reader, 0)` (src/fs/hfs.rs:437).
- `is_browsable_superfloppy()` (src/gui/inspect_tab.rs:4720) already lists
  `"HFS"`, so the browse button lights up — **once the picker accepts `.hfv`**.

So **inspect + browse are ~one extension-list change away**. The substantial
work is the **write side** (create / export / expand to HFV) plus extending the
**fsck / expand / edit** gating, which today is keyed to `0xAF` / APM
`Apple_HFS` and skips superfloppy (type-byte-0) HFS.

---

## 2. The 2047 MB / HFS-only limitation (must enforce on the write side)

This is a hard constraint on anything we *create, expand, or export* as HFV:

1. **HFS only — never HFS+.** A flat HFV is mounted by the 68k ROM's classic
   HFS driver. HFS+ in a flat image needs Mac OS 8.1+ plus a driver shim and is
   explicitly out of scope (the user's "HFS-only"). We will *read/browse* an
   HFS+ flat image if one is handed to us, but we never **write** one.

2. **Total size ≤ 2047 MB.** Classic HFS uses a **16-bit allocation-block count**
   (`drNmAlBlks`, max 65,535) and classic Mac OS File Manager does signed 32-bit
   byte arithmetic on volume sizes; a volume ≥ 2 GiB (2^31 bytes) wraps negative
   and won't mount / corrupts. Apple's own formatters historically refused HFS
   > 2 GB. The safe, conventionally-stated ceiling is **2047 MB**.

3. **Allocation-block-size floor follows from (2).** With 512-byte sectors a
   2047 MB volume is 4,192,256 sectors; to keep `drNmAlBlks ≤ 65,535` the
   allocation block size must be **≥ 32 KiB** at the top end. `create_blank_hfs`
   already iterates block size to satisfy this; we just need to clamp the
   target and let it pick.

These three rules get one shared validator (Phase 3) that every write entry
point calls, so the limit lives in exactly one place.

---

## 3. Phased plan

Ordered by value: Phases 1–2 deliver read/inspect/browse/fsck for existing HFV
files (small, high-value). Phases 3–6 are the write side. Phase 7 is
backup/restore round-trip. Phase 8 is CLI parity. Phase 9 is docs + real-Mac
validation.

### Phase 1 — Read / inspect / browse existing HFVs  ✅ DONE (commit 7dd3b61)
- [x] 1.1 Add `"hfv"`, `"HFV"` to the **inspect** picker filter
  (src/gui/inspect_tab.rs:349-356).
- [x] 1.2 Add the same to the **backup-source** picker
  (src/gui/backup_tab.rs:250-256) and both **restore-source** pickers
  (src/gui/restore_tab.rs:746-754, 1677-1685).
- [x] 1.3 Confirm `prepare_disk_image_path` (src/gui/mod.rs:80) passes `.hfv`
  through untouched (no decompression) — it does; added `hfv_passes_through_unchanged`
  unit test (covers `.hfv` and `.HFV`).
- [x] 1.4 Verified via `rb-cli inspect` + `rb-cli ls` against `THINKCDEV.hfv`
  (non-bootable) and `Mac OS 8.1.HFV` (bootable): each detects as one HFS
  superfloppy row at LBA 0 with correct size, and the root tree browses.
- [x] 1.5 Sizing display routes through the same HFS code as APM HFS (the row
  is a normal `PartitionInfo` with `partition_type_byte == 0`); inspect shows
  correct size for both samples.

### Phase 2 — fsck + edit for superfloppy HFS  ✅ DONE
The classic-HFS fsck and editable paths are entirely offset-driven and already
work at offset 0 (verified: `rb-cli fsck THINKCDEV.hfv@1` → "813 files / 90
dirs checked"). The only gap was the **GUI gating** keyed to type-byte `0xAF` /
APM `Apple_HFS`, which skipped superfloppy (type-byte-0) HFS rows.
- [x] 2.1 Added `is_superfloppy_hfs(ptype, type_name)` helper
  (src/gui/inspect_tab.rs) — `ptype == 0 && type_name == "HFS"`.
- [x] 2.2 ORed it into the inspect-tab **Check** gate; `run_fsck` already opens
  via `open_filesystem(.., 0, None)` auto-detect, so no engine change. CLI fsck
  confirmed working on `THINKCDEV.hfv`.
- [x] 2.3 **Edit mode** needs no gating change: the browse button uses
  `is_browsable_superfloppy` (already lists HFS) and `session.open_editable()`
  is a dynamic `open_editable_filesystem(.., 0, None)` probe — auto-detects HFS.
- [x] 2.4 Integration test `test_hfv_flat_hfs_detect_open_fsck_edit`
  (tests/filesystem_e2e.rs): builds a real blank HFS via `create_blank_hfs`,
  asserts superfloppy detect → auto-detect factory open → fsck clean →
  add-file round-trip persists & reads back byte-identical.

  Note: the inspect-tab **Expand…** gate (`is_classic_hfs`) is intentionally
  *not* widened here — it's deferred to Phase 6, which also adds the flat-HFV
  output mode + 2047 MB clamp the expand path needs.

### Phase 3 — HFV constraints + flat-HFS emit (engine, no UI)  ✅ DONE
New `src/fs/hfv.rs` is the single source of truth for the limits + the flat-HFS
build/clone primitives every write feature reuses.
- [x] 3.1 `src/fs/hfv.rs`:
  - `HFV_MAX_BYTES = 2047 * 1024 * 1024`.
  - `HfvVolumeKind { Hfs, HfsPlus }` + `from_fs_type()`.
  - `validate_hfv_target(size, kind) -> Result<(), FilesystemError>` — rejects
    HFS+/HFSX and `size > HFV_MAX_BYTES`, ASCII-only messages.
  - `suggest_block_size(size)` / `max_volume_for_block_size` / `BLOCK_SIZE_CHOICES`
    moved here as the canonical home (smallest block size keeping
    `drNmAlBlks ≤ 65,535`); `model::hfs_expand_runner` now re-exports them, so
    existing GUI/CLI imports keep resolving and there's no duplication.
- [x] 3.2 `build_blank_hfv(size, block_size, name) -> Vec<u8>` — validated thin
  wrapper over `create_blank_hfs_sized` (no APM wrapper). An HFV *is*
  `create_blank_hfs`'s output.
- [x] 3.3 `clone_into_hfv(source_hfs, target_size, block_size, name) ->
  (Vec<u8>, CloneReport)` — builds a blank target sized to the source's
  catalog/extents B-trees, opens it in memory, replays via `clone_hfs_volume`
  (copies boot blocks → bootable source stays bootable), **skips**
  `emit_apm_disk_with_hfs`. Refuses non-HFS sources.
- [x] 3.4 6 unit tests in `fs::hfv`: validate accept/reject matrix (incl. exact
  cap boundary), `suggest_block_size` range (2047 MB → 32 KiB), `build_blank_hfv`
  re-detects as superfloppy HFS + fscks clean, oversize/HFS+ refusal, and a
  `clone_into_hfv` file round-trip.

### Phase 4 — Create a blank HFV  ✅ DONE (CLI); GUI deferred by design
- [x] 4.3 CLI: added `FsKind::Hfv` to the `new` verb —
  `rb-cli new out.hfv --fs hfv --size 100M --name "Mac HD"`. Routes through
  `fs::hfv::build_blank_hfv` (validates the 2047 MB cap) with the block size
  auto-floored via `suggest_block_size` unless `--block-size` is given.
  Verified end-to-end: a 100 MB `--fs hfv` image inspects as a single HFS
  superfloppy and fscks clean; `--size 2048M` is refused with the cap message.
- [-] 4.1/4.2 Standalone model runner + "New blank HFV…" GUI dialog **deferred
  by design**: the GUI has no general "create blank image" surface for *any*
  filesystem today, so a create dialog would be net-new UI scaffolding
  orthogonal to HFV. GUI-side HFV creation is delivered instead through
  **Export to HFV** (Phase 5) and **Expand to HFV** (Phase 6), which hang off
  existing inspect-tab buttons. Revisit if a general File→New surface is added.
  A model runner will be added then (or for Phase 6, which does need one).

### Phases 5 & 6 — Export / Expand to a flat HFV  ✅ DONE (unified)
"Export an HFS volume to HFV" and "resize/re-floor an HFV" are the **same**
operation — clone a classic-HFS source into a freshly-sized flat HFS volume —
which is exactly what the existing **Expand HFS Volume…** flow does before its
APM-emit step. So rather than bolt `ExportFormat::Hfv` onto the whole-disk
*streaming* export pipeline (a poor fit — HFV output is a volume re-layout, not
a byte stream), both goals are delivered by adding a **flat-HFV output mode** to
the expand path.
- [x] 6.4 `model::hfs_expand_runner`: new `ExpandOutput { ApmDisk, FlatHfv }`
  threaded through `start_hfs_expand` / `run_expand`. The clone + fsck-verify
  chain is shared; `FlatHfv` writes `target_buf` verbatim (no
  `emit_apm_disk_with_hfs`) and returns `emit_report = None`. Early
  `validate_hfv_target` enforces the 2047 MB cap for HFV output.
- [x] 6.1 `is_classic_hfs` (inspect_tab) widened via `is_superfloppy_hfs`, so an
  HFV row now offers **Expand…** — that's the GUI "export/convert to HFV" and
  "resize HFV" entry point.
- [x] 6.2/6.3 `expand_hfs_dialog.rs`: **Output format** radio (APM disk / Flat
  HFV), defaulting to HFV when the source is itself a `.hfv`. Size slider
  clamps to 2047 MB and the Save-As dialog defaults to `.hfv` with an HFV
  filter when HFV output is selected.
- [x] 5.4 CLI: `rb-cli expand IMG@N --size … --to-hfv --output out.hfv` —
  writes a flat HFV instead of an APM disk; rejects `--size > 2047 MB`.
- [x] 5.x/6.5 Verified end-to-end: `expand THINKCDEV.hfv@1 --size 50M --to-hfv`
  clones all 813 files into a 50 MB flat HFV that re-detects + fscks clean +
  lists correctly; `--size 2048M` is refused. Model-layer test
  `test_expand_runner_writes_flat_hfv` drives the real worker with
  `ExpandOutput::FlatHfv` and asserts a valid HFV + no `EmitReport`.

  Note: `ExportFormat::Hfv` in the inspect-tab Export dialog is intentionally
  **not** added — see rationale above; the Expand button is the correct home.

### Phase 7 — Backup / restore round-trip
- [ ] 7.1 Confirm backing up an HFV produces a sane per-partition backup
  (superfloppy `partition_table_type == "None"`, HFS compaction is
  layout-preserving). Record the original container as HFV in metadata if a hint
  is needed for restore defaulting.
- [ ] 7.2 Restore path: a `None`-table HFS backup should be restorable **as a
  flat `.hfv`** (not forced through `superfloppy_wrap`, which adds an MBR/GPT —
  that's the wrong shape for Basilisk). Verify `reconstruct_disk_from_backup` /
  the superfloppy restore branch (src/restore/mod.rs:764, 1460, 1468) can emit
  the bare volume, and offer ".hfv" as the restore output.
- [ ] 7.3 Round-trip test: HFV → backup → restore → byte-equivalent (or
  fsck-equivalent + same file tree) HFV.

### Phase 8 — CLI parity sweep
- [ ] 8.1 Ensure `inspect`, `browse`, `backup`, `restore`, `export`, and any
  resize verbs accept `.hfv` inputs/outputs. Cross-check naming against
  docs/cli-todo.md.
- [ ] 8.2 CLI examples/cookbook entries (docs/cli-examples.md,
  docs/cli-cookbook.md).

### Phase 9 — Docs + real-Mac validation
- [ ] 9.1 Update CLAUDE.md (formats list) and src/rbformats/README.md /
  src/fs/README.md as touched.
- [ ] 9.2 Validate created/exported/expanded HFVs **boot or mount** in
  BasiliskII (and/or MAME) — the only true test of the bootability/limit work.
  Use the bootable samples (`Mac OS 8.1.HFV`, `Starterdisk.hfv`) as ground
  truth; compare layouts the way `make_blank_apm_hfs` was A/B-tested against the
  Apple HD SC reference.
- [ ] 9.3 Confirm a 2047 MB created HFV mounts and a 2048 MB one is refused by
  our validator (we never hand Basilisk a volume it would corrupt).

---

## 4. Open questions / decisions
- Restore-as-HFV vs restore-as-raw: should a `None`-table HFS backup default its
  restore output to `.hfv`? (Phase 7.2.) Leaning yes when the source was an HFV.
- Do we want a "convert APM/`.hda` HFS disk → HFV" one-shot? Falls out of Phase 5
  export almost for free; decide whether to surface it explicitly.

## 5. Key references
- BasiliskII: `../macemu/BasiliskII/src/disk.cpp` (`find_hfs_partition`,
  flat-image fallback), `src/Unix/extfs_unix.cpp:216` (`.hfv` registration).
- Existing flat-HFS machinery: `src/fs/hfs.rs` (`create_blank_hfs`,
  `create_blank_hfs_sized`, boot-block get/set), `src/fs/hfs_clone.rs`
  (`clone_hfs_volume`, `emit_apm_disk_with_hfs` — the APM path we deliberately
  *bypass* for HFV).
- Superfloppy detection/emit: `src/partition/mod.rs` (`detect_superfloppy`,
  `partitions`), `src/restore/superfloppy_wrap.rs` (MBR/GPT wrap — *not* used
  for HFV; HFV stays bare).
- Gating helpers: `src/gui/inspect_tab.rs` (`is_browsable_superfloppy`,
  `is_checkable_type`, `is_classic_hfs`).
- Prior HFS bring-up lore (bootability, block size, B-tree): docs/hfs_expand_block_size.md,
  docs/hfs_btree_capacity.md, and the MEMORY.md "Classic-HFS Bootability" notes.

---

## 6. Progress log
_Add a dated line per session as steps land._

- 2026-05-29 — Plan created. Recon done: confirmed HFV = flat HFS @ offset 0
  (3 samples), mapped the existing superfloppy→HFS read path, identified the
  write-side gaps (extension wiring, fsck/expand/edit gating for type-byte-0
  HFS, flat-HFS emit, 2047 MB/HFS-only validator). Nothing implemented yet.
