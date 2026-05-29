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

### Phase 1 — Read / inspect / browse existing HFVs  ✅ smallest valuable slice
- [ ] 1.1 Add `"hfv"`, `"HFV"` to the **inspect** picker filter
  (src/gui/inspect_tab.rs:349-356).
- [ ] 1.2 Add the same to the **backup-source** picker
  (src/gui/backup_tab.rs:250-256) and both **restore-source** pickers
  (src/gui/restore_tab.rs:746-754, 1677-1685).
- [ ] 1.3 Confirm `prepare_disk_image_path` (src/gui/mod.rs:80) passes `.hfv`
  through untouched (no decompression) — it already does; add a unit-test
  assertion so a future edit can't regress it.
- [ ] 1.4 Manual: open all three sample HFVs in Inspect; confirm one HFS row,
  correct volume name, block size, used/free; open Browse and walk the tree.
- [ ] 1.5 Verify `partition_minimum_size`/sizing display is sane for a
  superfloppy HFS row (it routes through the same HFS code as APM HFS).

### Phase 2 — fsck + edit for superfloppy HFS
The classic-HFS fsck and editable paths exist but their **GUI gating** is keyed
to type-byte `0xAF` / APM `Apple_HFS`. Extend it to recognize superfloppy
(type-byte-0) HFS so HFVs get the same Check / Edit affordances.
- [ ] 2.1 Extend `is_checkable_type` (src/gui/inspect_tab.rs:4754) to return
  true for a superfloppy HFS row (`ptype == 0 && type_name == "HFS"`). Consider
  a small `is_superfloppy_hfs(ptype, type_name)` helper reused by 2.x and 6.x.
- [ ] 2.2 Confirm the per-partition **Check** button runs HFS fsck on an HFV
  (the checker is offset-driven via `HfsFilesystem::open(reader, 0)`; no engine
  change expected). Test against `THINKCDEV.hfv`.
- [ ] 2.3 Confirm **Edit mode** opens for a superfloppy HFS: trace
  `open_editable_filesystem(type_byte=0)` auto-detect and the browse-view edit
  gating. Add a file, Apply Edits, reopen, verify it persists and the volume
  still mounts (Phase 9 validation).
- [ ] 2.4 Unit test: open a small synthetic flat-HFS image (built via
  `create_blank_hfs`) with no partition table, assert detect → HFS, fsck clean,
  add-file round-trips.

### Phase 3 — HFV constraints + flat-HFS emit (engine, no UI)
The single source of truth for the limits, plus the "write a flat HFS volume to
a file" primitive that every write feature reuses.
- [ ] 3.1 New `src/fs/hfv.rs` (engine layer). Define:
  - `const HFV_MAX_BYTES: u64 = 2047 * 1024 * 1024;`
  - `fn validate_hfv_target(size_bytes, fs_kind) -> Result<(), HfvError>` —
    rejects HFS+/HFSX and any `size > HFV_MAX_BYTES`; returns a clear,
    ASCII-only message ("HFV volumes must be classic HFS and at most 2047 MB").
  - `fn suggested_block_size(size_bytes) -> u32` — smallest standard block size
    keeping `drNmAlBlks ≤ 65,535` (delegates to / mirrors `create_blank_hfs`'s
    existing iteration).
- [ ] 3.2 `fn write_blank_hfv(path/writer, size_bytes, block_size, name)` —
  thin wrapper over `create_blank_hfs` that writes the flat volume with **no**
  APM wrapper. (An HFV is literally `create_blank_hfs`'s output; this is mostly
  a named, validated entry point.)
- [ ] 3.3 `fn write_hfv_from_source(source_hfs, target_writer, target_size,
  block_size, name)` — `create_blank_hfs` then `clone_hfs_volume(source,
  target)`, no `emit_apm_disk_with_hfs`. Boot blocks copy via the existing
  `read_boot_blocks`/`set_boot_blocks`, so a bootable source stays bootable
  without DDR/APM.
- [ ] 3.4 Unit tests: `validate_hfv_target` accept/reject matrix;
  `write_blank_hfv` produces a file that re-detects as superfloppy HFS, passes
  fsck, and has `drNmAlBlks ≤ 65,535`; clone round-trip preserves files.

### Phase 4 — Create a blank HFV (model + GUI + CLI)
- [ ] 4.1 Model runner `src/model/*` (or extend an existing one) wrapping
  `write_blank_hfv` with the progress/Status pattern (CONTRIBUTING
  "Background work").
- [ ] 4.2 GUI action: "New blank HFV…" (size slider clamped to
  `[~800 KB .. 2047 MB]`, name field, block-size auto). Save dialog defaults to
  `.hfv`. Reuse `expand_hfs_dialog.rs` widgets where sensible.
- [ ] 4.3 CLI verb (see docs/cli-todo.md before naming) e.g.
  `rb-cli create-hfv --size 100M --name "Mac HDD" out.hfv`.

### Phase 5 — Export to HFV
Export an existing classic-HFS source (live image / backup / another HFS
partition) to a flat `.hfv`, gated to HFS-only ≤ 2047 MB.
- [ ] 5.1 Add `ExportFormat::Hfv` (src/rbformats/export.rs): extension `"hfv"`,
  description "BasiliskII HFV", dialog filter `("BasiliskII HFV", &["hfv"])`.
- [ ] 5.2 Export path delegates to `write_hfv_from_source` (Phase 3.3), **not**
  the generic raw streamer — we re-lay-out into a freshly-sized HFS so the
  result respects the block-size floor and ≤2047 MB cap.
- [ ] 5.3 Gate the radio option: only offer HFV when the selected partition is
  classic HFS and ≤ 2047 MB; otherwise disable with a tooltip explaining the
  limit. Tie into `is_superfloppy_hfs` / `is_classic_hfs`.
- [ ] 5.4 CLI: `rb-cli export --format hfv …` with the same gate.
- [ ] 5.5 Tests: export an APM `Apple_HFS` partition and a superfloppy HFS to
  HFV; both re-detect, fsck clean. Assert rejection of an HFS+ source and a
  >2047 MB source.

### Phase 6 — Expand / resize an HFV
The existing "Expand HFS Volume…" flow currently emits an **APM-wrapped** disk
(`emit_apm_disk_with_hfs`). Add a flat-HFV output mode and let HFVs use it.
- [ ] 6.1 Extend `is_classic_hfs` (or the expand-dialog gate) so superfloppy
  HFS rows offer "Expand…".
- [ ] 6.2 In `src/gui/expand_hfs_dialog.rs`, add an output-shape choice:
  **APM disk (.hda)** (existing) vs **flat HFV (.hfv)** (new, via Phase 3.3).
  When the source is an HFV, default to flat HFV.
- [ ] 6.3 Clamp the target-size slider to **≤ 2047 MB** when the flat-HFV output
  is selected (the APM path keeps its current ceiling). Output filter includes
  `"hfv"` (src/gui/expand_hfs_dialog.rs:224).
- [ ] 6.4 Reuse the existing worker chain (`create_blank_hfs` → `clone_hfs_volume`
  → fsck verify) but skip `emit_apm_disk_with_hfs` for the flat path.
- [ ] 6.5 Tests: expand a 30 MB HFV to 100 MB flat HFV, verify file count and
  fsck; assert the slider/validator rejects 2048 MB+.

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
