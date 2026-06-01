# Virtualization & Legacy Image Format Support — Implementation Plan

Status: **CLOSED** (2026-06-01; opened 2026-05-23). Owner: Dani. The shipped
work merges to main with this branch; the unfinished items are deferred (see
the closeout below).

This plan adds support for modern virtual-machine disk-image containers
(VHD dynamic, QCOW2, VMDK) and read-only import of vintage/legacy backup
imaging formats (GHO, EnCase E01, Partimage, WinImage IMZ, Teledisk/
ImageDisk floppy). VHDX is deferred; PQI is out of scope (§ Phase 5).

## Closeout (2026-06-01)

**Shipped:**

- **Phases 1–4 complete** — VHD dynamic, QCOW2, VMDK flat, VMDK sparse: each
  with convert-from (reader), convert-to (`ExportFormat`), and in-place edit,
  wired into GUI + CLI.
- **Phase 5 (legacy import) — IMZ and Norton Ghost shipped, and well past the
  original scope:** WinImage IMZ (incl. password decryption); GHO container +
  record-stream + Fast-LZ + zlib, SECTOR-mode, `.GHS`/`.NNN` span chains,
  password decryption, **and full file-aware reconstruction for both FAT and
  NTFS** (5.6-fa, originally scoped FAT-only and marked "deferred"). GHO/IMZ
  were also refactored from decode-to-temp onto CHD-style streaming
  `Read + Seek` readers (no tempfile).

**Deferred indefinitely** (no work started; revisit only on concrete demand):

- **5.2 Partimage** — parked. Reference + a phased plan live in
  `docs/additional_nix_filesystems.md` (Partimage decodes used-blocks of a
  Unix FS, so it composes with the ReiserFS/XFS readers tracked there).
- **5.3 Teledisk / ImageDisk (TD0 / IMD)** — **superseded:** these floppy
  containers are owned by the MiSTer plan,
  `docs/mister_filesystem_implementation_plan.md` §3.2 (port the decoders from
  `fluxfox`, MIT). Do not implement standalone here — it would duplicate that
  decoder.
- **5.4 EnCase E01 / EWF** — deferred indefinitely. No other plan covers it; it
  has no floppy/retro-core relevance, so it stays an orphaned follow-up until a
  real need appears.
- **5.8 corrupt/truncated recovery** (always a stretch goal) — deferred
  indefinitely.
- **Phase 6 VHDX** — deferred indefinitely (unchanged from the original plan).
- **PQI** — out of scope (unchanged).

The per-session ✅ markers below are the historical record. The status column
of the unfinished rows is annotated **DEFERRED** with the pointer above.

Each numbered session below is sized to fit **one Claude Code session** and
ends in **one commit**. Context is cleared between sessions. Sessions within a
phase are ordered so each makes the next easier (engine first, then wiring),
per `CONTRIBUTING.md` "Adding a feature: the playbook".

---

## Goals & non-goals

- **Modern formats (VHD dynamic, QCOW2, VMDK flat, VMDK sparse):** convert-from
  (import/inspect/browse), convert-to (export), and in-place edit.
- **Legacy formats (GHO, PQI, E01, Partimage, IMZ, TD0/IMD):** convert-**from**
  only. No edit, no convert-to. The reader feeds the existing inspect /
  `reconstruct_disk_from_backup` / modern-writer paths so a user can turn an
  ancient image into a modern one.
- **VHDX:** deferred — modern Hyper-V only, zero retro demand, highest cost
  (region table + metadata + log/journal replay). Revisit on concrete need.

### Why these, and why this order

- **QCOW2 is the headline target.** UTM (the popular macOS front-end for QEMU)
  stores its disks as QCOW2, *including classic Mac OS* guests on QEMU's PPC
  machines. Those QCOW2 files contain APM + HFS/HFS+, which the existing HFS
  stack already masters end-to-end — so QCOW2 read/write is the one new
  container that round-trips classic-Mac disks through UTM.
- **VHD dynamic is the warmup.** It exercises the same sparse-allocation and
  allocate-on-write edit machinery QCOW2 needs, but in its simplest form (one
  BAT entry + a sector bitmap guarding a flat run). It also reuses the existing
  `src/rbformats/vhd.rs` fixed-VHD footer + CHS geometry code. Doing it first
  lets us extract a reusable `sparse_alloc` helper before tackling QCOW2.
- **VMDK flat** is cheap (raw data + a text descriptor) and slots in after the
  hard work. **VMDK sparse** reuses the QCOW2-era two-level allocation pattern.

### Already covered (no work needed)

- Raw `.img` / `.raw` / `.bin` / `.dd` and **fixed VHD** — full read/write/edit.
- DOSBox / DOSBox-X hard disks (flat raw) and SheepShaver / Basilisk II
  hardfiles (flat raw HFS/HFS+, e.g. `.hfv`). Watch the headerless raw-HFS
  case (volume header at offset 1024, no APM) in inspect/browse.

---

## Architecture mapping (per CONTRIBUTING.md)

| Capability | Layer | Shape |
|---|---|---|
| convert-from / browse | `src/rbformats/<fmt>.rs` | `Read + Seek` reader (random-access formats) **or** decode-to-temp materializer (whole-image-compressed formats), mirroring `ChdReader` / `materialize_amiga_image_path` |
| convert-to / export | `src/rbformats/<fmt>.rs` | `compress_*`-style streamer; `ExportFormat` variant in `export.rs` |
| in-place edit | `src/rbformats/<fmt>.rs` | `Read + Write + Seek` with allocate-on-write; hosted by `open_editable_filesystem` |
| progress / threading | `src/model/*_runner.rs` | runner owns `Arc<Mutex<XxxStatus>>`; leaf I/O takes `progress_cb` / `cancel_check` / `log_cb` |
| view | `src/gui/<tab>.rs` | polls Status; export dropdown + file-picker filters; ~10–20 line handlers |
| CLI parity | `src/cli/verbs/convert.rs` | every one-shot convert path mirrored in the CLI |

**Random-access vs. stream-compressed.** E01, VHD-dynamic, QCOW2, and VMDK are
random-access (offset/allocation tables) → native `Read + Seek`, enabling
browse without a full extract. GHO, PQI, Partimage, IMZ, and TD0/IMD are
whole-image-compressed streams → decode to a raw tempfile first (reuse the
`tempfile::TempDir` guard pattern already used for `.adz`/`.hdz`), then the
normal inspect/convert paths apply. Convert-from for those is a streaming
transcode: decode legacy → feed a modern writer.

### Shared scaffolding

The VHD-dynamic warmup (session 1.2) extracts a small `sparse_alloc` helper
(allocation-table append + "is this block all-zero, skip it" logic) that QCOW2
and VMDK-sparse reuse. We do **not** introduce a unified VM-format trait — per
`CONTRIBUTING.md` "Patterns to avoid", premature trait splits cost more than
they save. Per-format readers + the existing `CompactResult` boundary stay.

### Per-session definition of done

Every session must end green on: `cargo build --all-targets` (zero warnings),
`cargo test --lib`, `cargo clippy` (no new warnings on touched files),
`cargo fmt` (pre-commit hook). New engine logic ships with inline
`#[cfg(test)]` round-trip tests. UI strings are ASCII-only. One commit,
message explaining the *why*.

---

## Integration seams (exact code touch-points)

Every format in this plan hooks the codebase at the same four seams. Read this
once; the per-phase sections refer back to it rather than repeating it.

### Seam A — convert-from / browse: `ImageFormat` + detection + reader factory

All three live in `src/rbformats/mod.rs`:

1. **`pub enum ImageFormat`** (~line 1002). Add one variant per new container,
   carrying whatever the reader factory needs (a `PathBuf`, a parsed header, a
   `logical_size`). Follow the `Chd { path, logical_size }` precedent for
   path-opened formats and `Vhd { data_size }` for in-place section readers.
   Extend the `describe()` match too (used by the inspect summary).
2. **`detect_image_format_with_path(file, path)`** (~line 1086). A magic-sniff
   ladder, most-specific first. Add a branch that seeks to the signature offset,
   matches the magic, and returns the new `ImageFormat` variant. **Ordering
   matters** — put strong fixed-magic checks (QCOW2 `QFI\xFB`, EWF
   `EVF\x09\x0D\x0A\xFF\x00`) before weak/heuristic ones. The VHD cookie check
   at line 1157 reads the *last* 512 bytes; dynamic-VHD detection extends that
   same branch (read footer `disk type` field at footer offset 60, big-endian:
   2 = fixed, 3 = dynamic, 4 = differencing — reject 4 for now).
3. **`wrap_image_reader(file, format)` → `(BoxReadSeek, u64)`** (~line 1234).
   Add a match arm that constructs the reader and returns it boxed with the
   logical disk length. `BoxReadSeek = Box<dyn Read + Seek + Send>`. For
   path-opened formats, `drop(file)` and open by path (see the `Chd` arm). For
   whole-image-compressed legacy formats, this arm materializes to a tempfile
   (see Seam A′).

Once a format flows through these three, **inspect, browse, partition parsing,
and `reconstruct_disk_from_backup` all work for free** — they consume
`BoxReadSeek`, not a concrete type.

### Seam A′ — decode-to-temp for whole-image-compressed legacy formats

GHO, Partimage, IMZ, and TD0/IMD are not randomly seekable. They reuse the
`materialize_amiga_image_path` pattern (`src/gui/mod.rs`): decode the whole
image into a `tempfile::NamedTempFile`/`TempDir`, then hand back a `File`
reader over the decoded raw. The `ImageFormat` variant carries the decoded
temp path and the GUI tab keeps the `TempDir` guard alive for the lifetime of
the open image (exactly as the `.adz`/`.hdz` tabs already do). Decoding takes a
`progress_cb`/`cancel_check`/`log_cb` and runs on a worker thread (Seam C) for
anything that isn't near-instant.

### Seam B — convert-to / export: `ExportFormat`

`src/rbformats/export.rs`, `pub enum ExportFormat` (~line 22). Add a variant and
extend the five `impl` methods: `extension()`, `description()`,
`default_filename()` (free if extension is set), `dialog_filter()`, and
`is_floppy_only()` (→ `false`). Hard-disk-class formats route through the
format's own `compress_*`/`export_*` streamer; wire that into the export
dispatch alongside `export_whole_disk_vhd`. The writer takes
`progress_cb`/`cancel_check` and never buffers a whole partition (CONTRIBUTING
I/O rule: 64 KiB–1 MiB chunks).

### Seam C — model runner + GUI + CLI

- **Runner:** reuse the existing export/convert runner and its
  `Arc<Mutex<XxxStatus>>` (e.g. `BulkConvertStatus`); add Status fields only if
  a new progress dimension appears. Leaf I/O takes callbacks; the runner owns
  the mutex (`docs/progress_pattern.md`).
- **GUI:** the export format dropdown and the open-file picker filters. Convert
  is already a GUI surface; new formats are new dropdown entries + filter
  extensions. No new background-spawn code in the view.
- **CLI:** `src/cli/verbs/convert.rs`. Each convert-to format becomes a valid
  `--to` target; each convert-from format is auto-detected via Seam A so `--from`
  is usually unnecessary (parity rule in CLAUDE.md / `docs/cli-todo.md`).

### Seam D — in-place edit: `Read + Write + Seek` host

`open_editable_filesystem(reader, …)` needs `R: Read + Write + Seek`. A
container supports edit when its reader type also implements `Write + Seek` with
allocate-on-write. The browse-view edit toggle already routes CHD through a
lighter in-place path and WOZ through decompress→edit→recompress
(`src/gui/browse_view.rs`); new sparse containers get an in-place
`Read+Write+Seek` impl, with the archive-edit expand→edit→re-emit flow as the
documented fallback if in-place edit slips.

---

## Phase 1 — VHD dynamic (warmup)

Reference: Microsoft "Virtual Hard Disk Image Format Specification" (public).
Layout: data lives in blocks (default 2 MB) indexed by a Block Allocation Table
(BAT); each block is preceded by a 512-byte sector bitmap; a 1024-byte dynamic
disk header precedes the BAT; the 512-byte VHD footer (already implemented) is
at the end (and mirrored at the front).

| # | Session | Scope & deliverable |
|---|---|---|
| 1.1 ✅ | **Reader** | `DynamicVhdReader` (`Read + Seek`) parsing dynamic header + BAT + per-block bitmaps; unallocated blocks read as zeros. Detect dynamic-vs-fixed via footer `disk type` field (3 = dynamic, 2 = fixed). Wire detection into the file-open path so inspect/browse/convert-from work. Round-trip unit test against a hand-built dynamic VHD. Commit `d1f94bf`. |
| 1.2 ✅ | **Writer (convert-to)** | Stream a disk image → dynamic VHD: emit header/footer, build BAT, allocate a block only when its data is non-zero, write the bitmap + block. New `ExportFormat::VhdDynamic`. **Extract `sparse_alloc` helper** (block-append + zero-skip) for downstream reuse. Tests asserting all-zero input produces a near-empty file and that fixed-VHD output is byte-identical to before (no regression). Commit `f236325`. |
| 1.3 ✅ | **In-place edit** | `Read + Write + Seek` with allocate-on-write: writing into an unallocated block appends a new block, sets its BAT entry + bitmap; writing into an allocated block updates in place. Host it through `open_editable_filesystem`. Tests: open → mutate a file via the FS layer → reopen with the reader → verify. Commit `8691bad`. |
| 1.4 ✅ | **Integration** | GUI export radio entry ("VHD (Dynamic)") on Inspect tab + Bulk Convert dialog; CLI `--format vhd-dynamic` on `rb-cli convert`. Per-partition export auto-forced to whole-disk (sparse layout wraps a whole disk). File-picker filter shares `vhd`/`hda` with fixed VHD; detection on read distinguishes via the footer's disk_type field. `src/rbformats/README.md` format table and this doc updated. |

### On-disk layout

```
+0                  Copy of 512-byte footer (mirror of the one at EOF)
+512                Dynamic Disk Header (1024 bytes)
+1536               BAT (Block Allocation Table): N * u32 (big-endian), padded to 512
...                 Data blocks, each = 512-byte sector bitmap + (block_size) data
EOF-512             512-byte footer (already implemented: build_vhd_footer)
```

All multi-byte fields are **big-endian** (VHD is a Connectix/Microsoft format).

**Footer (already have `build_vhd_footer`, `VHD_COOKIE = b"conectix"`):** the
field this phase cares about is **disk type at footer offset 60 (u32 BE)**:
`2` = fixed, `3` = dynamic, `4` = differencing. `build_vhd_footer` currently
hard-codes 2 (see the `disk_type == 2` assert at `vhd.rs:1110`); session 1.2
parameterizes it. Footer offset 16 (`data_offset`, u64 BE) is `0xFFFFFFFF...`
for fixed and points at the dynamic header (512) for dynamic.

**Dynamic Disk Header (1024 B), all fields big-endian:**

| Offset | Size | Field | Value |
|---|---|---|---|
| 0 | 8 | cookie | `cxsparse` |
| 8 | 8 | data offset | `0xFFFFFFFFFFFFFFFF` (none) |
| 16 | 8 | table offset | BAT location (= 1536) |
| 24 | 4 | header version | `0x00010000` |
| 28 | 4 | max table entries | BAT entry count = ceil(disk_size / block_size) |
| 32 | 4 | block size | default `0x00200000` (2 MiB) |
| 36 | 4 | checksum | ones-complement sum of all 1024 bytes with this field zeroed |
| 40 | 16 | parent unique id | 0 (non-differencing) |
| 56 | 4 | parent timestamp | 0 |
| 60 | 4 | reserved | 0 |
| 64 | 512 | parent unicode name | 0 |
| 576 | 8×24 | parent locator entries | 0 |

The checksum algorithm is identical to the footer's (sum bytes, ones-complement)
— factor a shared `vhd_checksum(&[u8])` helper in 1.2.

**BAT:** `max_table_entries` u32 big-endian entries. Each entry is the **sector
offset** (×512) of that block's sector-bitmap, or `0xFFFFFFFF` if the block is
unallocated (reads as zeros). Block index = `byte_offset / block_size`;
intra-block offset = `byte_offset % block_size`.

**Block:** a sector bitmap (ceil(block_size/512/8) bytes, padded up to a
512-byte boundary) followed by the block data. Bit set = that 512-byte sector
has been written. For our purposes (whole-disk import/export) we set all bitmap
bits when we allocate a block.

### Session 1.1 — `DynamicVhdReader` (Read + Seek)

- Parse footer (reuse existing parse path), branch on disk type. New struct
  `DynamicVhdReader<R: Read + Seek>` holding the BAT (`Vec<u32>`), `block_size`,
  `bitmap_size`, `disk_size`, and a current logical position.
- `read`: locate block via BAT; if entry == `0xFFFFFFFF`, emit zeros for the
  span up to the next block boundary; else seek to `bat[i]*512 + bitmap_size +
  intra` and read. Never read across a block boundary in one inner read.
- `seek`: pure arithmetic on the logical position; no I/O.
- **Seam A wiring:** extend the VHD branch in `detect_image_format_with_path`
  (line 1157) to read disk type; add `ImageFormat::VhdDynamic { … }`; add the
  `wrap_image_reader` arm.
  - While here, add a one-paragraph comment at the top of the detection ladder
    documenting the **insertion convention** (strong fixed-magic checks first,
    weak/heuristic/size-based checks last; note the offset each magic lives at).
    Every later convert-from session edits this ladder; a stated convention
    keeps the ordering correct without re-abstracting it into a table (we
    deliberately keep the explicit ladder — see CONTRIBUTING "patterns to
    avoid").
- Tests: build a 2-block dynamic VHD by hand (one allocated, one sparse) in a
  `Vec<u8>`/`Cursor`; assert reads return written data + zeros; assert seek+read
  at a block boundary is correct.

### Session 1.2 — writer + `sparse_alloc` helper

- `export_whole_disk_vhd_dynamic(reader, out, block_size, progress, cancel)`:
  stream the source in `block_size` chunks; for each chunk, if `is_all_zeros`
  (already in `mod.rs`) leave BAT entry `0xFFFFFFFF`; else append a block
  (bitmap all-ones + data) and record the sector offset. Finally write footer
  mirror, dynamic header, BAT, then the appended blocks — or stream blocks
  first and backfill the BAT (two-pass over a temp, or hold BAT in RAM: a
  64 GiB disk at 2 MiB blocks = 32768 entries = 128 KiB, trivially in RAM).
- **Extract `sparse_alloc`** into `src/rbformats/sparse.rs` (or a `mod sparse`
  in `mod.rs`): a tiny helper owning "next free host offset" + "append this
  block, return its offset" + the zero-skip predicate. QCOW2 (clusters) and
  VMDK-sparse (grains) reuse it with different unit sizes.
  - **Design it for both allocation modes up front**, because the same logic is
    needed by every edit session later (1.3, 2.3/2.4, 4.3): (a) *one-shot
    streaming append* during export, and (b) *incremental allocation into an
    existing file* during in-place edit ("append a unit at EOF / first free
    slot, hand back its offset, mark it allocated"). Keep it unit-size-agnostic
    (caller passes the unit size) and table-agnostic (caller owns the BAT/L2/GT
    and writes the returned offset into it). Getting this boundary right in 1.2
    makes 1.3 / 2.3 / 2.4 / 4.3 thin wrappers instead of three re-derivations of
    the same bookkeeping.
- Parameterize `build_vhd_footer` with disk type (default keeps 2 → no
  regression; assert fixed-VHD bytes unchanged).
- `ExportFormat::VhdDynamic` (Seam B); extension `vhd`, description
  "Dynamic VHD".
- Tests: all-zero 64 MiB input → output << input; round-trip through
  `DynamicVhdReader`; fixed-VHD golden bytes unchanged.

### Session 1.3 — in-place edit (Read + Write + Seek)

- Impl `Write + Seek` on a `DynamicVhdWriter<R: Read + Write + Seek>` (or extend
  the reader): `write` into an allocated block updates in place; into an
  unallocated block, append a new block at EOF-512 (in front of the footer —
  i.e. move the footer down, or keep footer mirror at front and append before
  the trailing footer), set the BAT entry, rewrite the BAT region, set bitmap
  bits. Keep the trailing footer last.
- Host via `open_editable_filesystem` (Seam D).
- Tests: open the 1.2 output read-write, mutate a file via the FS layer
  (create/delete), `sync_metadata`, reopen with `DynamicVhdReader`, verify FS
  contents + that a previously-sparse region got allocated.

---

## Phase 2 — QCOW2 (priority)

Reference: QEMU `docs/interop/qcow2.txt` (public spec). v1 scope: **header
versions 2 and 3** (qcow2 `compat=0.10` and `compat=1.1`), uncompressed, no
snapshots, no backing file, no encryption, no external data file, no extended
L2 entries (subclusters). Reject/clearly-error anything outside that on open
and on import. **Reject qcow1** (`.qcow`, magic `QFI\xFB` with version==1) —
different on-disk format (no refcounts, single-level table) and effectively
extinct in the wild. **The writer emits v3 only** (no reason to downgrade).
Reader accepts v2 so we can ingest 2009–2013-era VM images
(default QEMU output until 1.7, Dec 2013) — exactly the retro convert-from
case Phases 1–5 exist for.
Test fixtures: a real UTM-exported classic-Mac QCOW2 (APM + HFS, v3) plus a
`qemu-img`-generated PC image. **Add a v2 fixture**
(`qemu-img create -f qcow2 -o compat=0.10 …`) for session 2.1.

Layout: header → L1 table → L2 tables → data clusters (default 64 KB);
allocation tracked by a refcount table → refcount blocks.

| # | Session | Scope & deliverable |
|---|---|---|
| 2.1 ✅ | **Reader** | `Qcow2Reader` (`Read + Seek`): parse header, walk L1 → L2 → cluster offset; unallocated/zero clusters read as zeros. Detect & reject out-of-scope feature bits with a precise error. Wire into file-open. Tests against fixtures; assert a UTM classic-Mac image inspects as APM + HFS. |
| 2.2 ✅ | **Writer (convert-to)** | Disk → QCOW2 v3: build L1/L2 + refcount table/blocks, allocate clusters on non-zero data (reuse `sparse_alloc`). New `ExportFormat::Qcow2`. Tests: round-trip via `Qcow2Reader`; verify `qemu-img check` passes if `qemu-img` is available (gated, skipped otherwise). |
| 2.3 ✅ | **In-place edit (a)** | `Read + Write + Seek`: cluster allocation, refcount increment, L2-table growth (allocate a new L2 when an L1 slot is empty). Host through `open_editable_filesystem`. Tests for the L2-growth path. |
| 2.4 ✅ | **In-place edit (b) + integration** | Refcount-**table** growth edge cases (refcount block full), free-cluster reuse, host-cluster bookkeeping. GUI/CLI wiring. **Acceptance: round-trip a UTM classic-Mac APM+HFS disk** (import → edit a file → export → reopen in UTM/`qemu-img check`). Docs. |

### On-disk layout

All fields **big-endian**. The file is an array of fixed-size **clusters**
(`cluster_size = 1 << cluster_bits`, default 65536). Everything — header, L1,
L2 tables, refcount structures, data — is cluster-aligned.

**Header (v2 = 72 B, v3 ≥ 104 B + 8 B padding = 112 B written), all big-endian.
Offsets verified against `qemu.org/docs/master/interop/qcow2.html`:**

| Offset | Size | Field | Notes |
|---|---|---|---|
| 0 | 4 | magic | `QFI\xFB` (`0x514649FB`) — Seam A detection |
| 4 | 4 | version | **accept 2 or 3**; reject 1 (qcow1) and anything ≥ 4 |
| 8 | 8 | backing_file_offset | **must be 0** (reject backing files) |
| 16 | 4 | backing_file_size | — (paired with the offset above) |
| 20 | 4 | cluster_bits | typically 16 |
| 24 | 8 | size | virtual disk size in bytes |
| 32 | 4 | crypt_method | **must be 0** (reject encryption) |
| 36 | 4 | l1_size | number of L1 entries |
| 40 | 8 | l1_table_offset | cluster-aligned |
| 48 | 8 | refcount_table_offset | — |
| 56 | 4 | refcount_table_clusters | — |
| 60 | 4 | nb_snapshots | **must be 0** (reject snapshots) |
| 64 | 8 | snapshots_offset | v2 header ends after this field at byte 72 |
| 72 | 8 | incompatible_features | **v3 only**. Reject if any **unknown** bit set; defined bits: 0 = dirty, 1 = corrupt, 2 = external data file (**reject**), 3 = compression type (**reject** — implies non-zlib), 4 = extended L2 entries (**reject** — subclusters). |
| 80 | 8 | compatible_features | **v3 only**. Bit 0 = lazy refcounts; safe to ignore on read, clear on writeable open. |
| 88 | 8 | autoclear_features | **v3 only**. Mask off bits we don't understand when opening for write. |
| 96 | 4 | refcount_order | **v3 only**. Refcount bit width = `1 << refcount_order`. v1 supports only `4` (16-bit refcounts, the QEMU default); reject other values. v2 has no field — assume 4. |
| 100 | 4 | header_length | **v3 only**. Length of the header in bytes (≥ 104). Bounds-check before reading header extensions, which start at `header_length`. |
| 104 | 1 | compression_type | **v3 only** (since QEMU 5.1). 0 = zlib (default); reject any non-zero value (we reject the compression-type incompatible bit anyway). |
| 105 | 7 | header_end_padding | zero |

**v2 vs v3 deltas the reader must handle:**

- **Header length:** v2 is exactly 72 B and has none of the feature words; treat
  the missing fields as zero. `refcount_order` is implicitly 4 on v2.
- **Header extensions:** v3 may carry a TLV chain after byte `header_length`
  (backing-file format hint, feature-name table, bitmaps, etc.). v1 scope
  reads them only to detect "unknown required extension" and reject; we don't
  consume any extension data.
- **QCOW_OFLAG_ZERO** (L2 entry bit 0 on a *standard* entry → read as zeros):
  **v3 only**. On v2 the bit is reserved/unused; an unallocated L2 entry (= 0)
  is still "read as zeros" via the normal sparse path, so v2 just lacks the
  explicit zero-cluster optimization.
- **Extended L2 entries (subclusters):** v3 only and gated by an incompatible
  feature bit. Reject when set — v1 scope is standard L2 only.

Writer always emits v3 with `header_length = 104` (the spec minimum), one byte
of `compression_type = 0` plus 7 zero padding bytes to a 16-byte boundary at
offset 112 before any header-extension TLVs (we emit none), `refcount_order = 4`,
all feature words zero. (`header_length = 112` is also valid and what `qemu-img`
typically writes, since it includes the compression_type byte + padding inside
the declared length; either is acceptable per spec — pick whichever matches the
fixture we round-trip in test.)

**Address translation** (read path): for a guest byte offset,
`cluster = off / cluster_size`; `l2_entries = cluster_size / 8`;
`l1_index = cluster / l2_entries`; `l2_index = cluster % l2_entries`.
Read `l1[l1_index]` (u64): bit 63 = COPIED (refcount==1), low bits (mask
`0x00FFFFFFFFFFFE00`) = host offset of the L2 table; `0` = unallocated → zeros.
Read `l2[l2_index]` (u64): bit 63 COPIED, **bit 62 = compressed** (reject for
v1 — or support read-only zlib later), bit 0 of a *standard* entry via the
QCOW_OFLAG_ZERO (`bit 0`) = read as zeros; otherwise low bits = host cluster
offset. `0` = unallocated → zeros.

**Refcounts:** a refcount **table** (array of u64 cluster offsets) points at
refcount **blocks** (arrays of `refcount_bits`-wide entries, default 16-bit).
Each host cluster has a refcount; allocation = find a free (refcount 0) cluster
or extend the file, set refcount 1, mark L2 COPIED. For v1 export we only ever
write refcount 1, which keeps this simple.

### Session 2.1 — `Qcow2Reader` (Read + Seek)

- Parse header; **accept version 2 or 3**, **reject out-of-scope** (version∉{2,3},
  backing file, encryption, snapshots>0, external-data-file / extended-L2 /
  unknown incompatible bits, `refcount_order ≠ 4`) with a precise
  `FilesystemError`/`anyhow` message. On v2 the feature words and
  `refcount_order` are absent — treat as zero / implicit 4. Cache L1 in RAM (`l1_size`×8 ≤ tens of
  KiB). Lazily read + cache L2 tables (single-entry LRU like `ChdReader`'s
  hunk cache is enough).
- `read`/`seek`: address translation above; unallocated/ZERO clusters emit
  zeros; never cross a cluster boundary per inner read.
- **Seam A:** `ImageFormat::Qcow2 { path, logical_size }`; detect by magic at
  offset 0 (put it high in the ladder); `wrap_image_reader` opens by path.
- Tests: hand-built minimal QCOW2 (`Cursor`) for **both v2 and v3** headers;
  `qemu-img convert`-produced fixtures if available (gated) — at least one
  `compat=0.10` (v2) and one `compat=1.1` (v3). **Assert a UTM classic-Mac
  qcow2 (v3) inspects as APM + HFS, and a v2 PC image inspects with the
  expected partition table.**

### Session 2.2 — writer (convert-to)

- `export_qcow2(reader, out, cluster_bits, progress, cancel)`: precompute
  `l1_size` from disk size; stream source in clusters; for each non-zero
  cluster, `sparse_alloc` a host cluster, write data, set its L2 entry (COPIED),
  bump refcount; lazily allocate L2 tables and refcount blocks as needed; write
  header + L1 + refcount structures. Holding L1/refcounts in RAM and writing
  data clusters as we go (backfilling tables at the end) is simplest.
- `ExportFormat::Qcow2` (Seam B); extension `qcow2`, description "QCOW2".
- Tests: round-trip via `Qcow2Reader`; gated `qemu-img check` if present
  (skip cleanly otherwise).
  - **Build the gating helper once here** (a test util that locates `qemu-img`
    on `PATH` and returns `None`/skips when absent). Session 4.2 reuses it for
    its own `qemu-img check`, so put it somewhere shared (e.g. a
    `#[cfg(test)]` helper module) rather than inline.

### Sessions 2.3 / 2.4 — in-place edit

- 2.3: `Read + Write + Seek` with cluster allocation + refcount increment +
  **L2-table growth** (empty L1 slot → allocate a fresh L2 cluster, set L1).
  Host via `open_editable_filesystem` (Seam D).
- 2.4: refcount-**table** growth (refcount block full → allocate a new refcount
  block, extend the refcount table; and the recursive case where allocating the
  refcount block itself needs a refcount entry), free-cluster reuse. Wire Seam
  C. Acceptance test = UTM classic-Mac APM+HFS round-trip + `qemu-img check`.

> If in-place edit (2.3/2.4) proves too deep for two sessions, fall back to the
> archive-edit expand→edit→re-emit flow and file the in-place work as a
> follow-up — convert-from/to (2.1/2.2) are the must-haves.

---

## Phase 3 — VMDK flat

Reference: VMware "Virtual Disk Format" spec (public). Two on-disk shapes:
split (`name.vmdk` text descriptor + `name-flat.vmdk` raw data) and embedded
(descriptor header in front of flat data). Data is raw; the descriptor is a
small ASCII file (`# Disk DescriptorFile`, `createType`, an extent line like
`RW <sectors> FLAT "name-flat.vmdk" 0`).

| # | Session | Scope & deliverable |
|---|---|---|
| 3.1 ✅ | **Reader + writer** | `VmdkFlatReader` parses the ASCII descriptor (standalone or embedded), resolves `FLAT`/`ZERO` extents, exposes them as a flat `Read + Seek` stream. `export_vmdk_flat` writes a single-extent `monolithicFlat` (descriptor + `<base>-flat.vmdk`). `ImageFormat::VmdkFlat` + Seam A detection on `# Disk DescriptorFile`. `ExportFormat::VmdkFlat` wired into `export_whole_disk` (backup-folder + raw-source paths). Sparse `KDMV` images detected and rejected pending Phase 4. Tests: round-trip split flat, multi-extent concatenation (FLAT + ZERO + FLAT), descriptor parsing edge cases (quoted/spaced filenames, SPARSE rejection, ZERO extents). |
| 3.2 ✅ | **Edit + integration** | `vmdk::open_flat_extent_for_edit(descriptor)` resolves the single FLAT extent and returns it `R+W` (rejects multi-extent / ZERO / sparse — convert to monolithicFlat first). GUI radio "VMDK (Flat)" on Inspect + Bulk Convert dialogs; per-partition force-disabled (whole-disk only) for the same reason as dynamic VHD / QCOW2. CLI `rb-cli convert --format vmdk-flat`. Round-trip edit test: descriptor + flat extent → mutate via the helper → reopen via `VmdkFlatReader` → verify mutation visible. |

### Descriptor format

The descriptor is plain ASCII. Minimal example for a split flat disk:

```
# Disk DescriptorFile
version=1
CID=fffffffe
parentCID=ffffffff
createType="monolithicFlat"

# Extent description
RW 2097152 FLAT "disk-flat.vmdk" 0

# The Disk Data Base
ddb.virtualHWVersion = "4"
ddb.geometry.cylinders = "1024"
ddb.geometry.heads = "16"
ddb.geometry.sectors = "63"
ddb.adapterType = "ide"
```

- **Extent line:** `RW <size_in_sectors> FLAT "<filename>" <offset_in_sectors>`.
  `monolithicFlat` = single extent. There can be multiple `RW` lines
  (`twoGbMaxExtentFlat` splits into 2 GiB chunks) — parse them all and present
  one logical disk (concatenate extents by offset).
- **Two on-disk shapes:** split (`disk.vmdk` text + `disk-flat.vmdk` raw) and
  the rarer embedded/`monolithicFlat`-in-one-file. Detection: a `.vmdk` whose
  first bytes are ASCII `# Disk DescriptorFile` is a descriptor; a `.vmdk`
  starting with sparse magic `KDMV` is Phase 4; otherwise a `-flat.vmdk` is raw
  data referenced by a sibling descriptor.
- **CID/parentCID:** random-ish content IDs; `parentCID=ffffffff` means no
  parent. We emit `parentCID=ffffffff` and a fresh CID.
- **Geometry:** derive C/H/S from disk size (reuse `vhd_chs_geometry` logic — or
  a sibling helper; VMware uses the same 16-heads/63-sectors convention for
  IDE).

### Session 3.1 — reader + writer

- Reader: parse descriptor; build a `Read+Seek` over the (possibly multi-)flat
  extent(s) — a thin `SectionReader`-style concatenation. Most flat VMDKs are a
  single extent → just a `SectionReader` over `disk-flat.vmdk` at offset 0.
- Writer `export_vmdk_flat(reader, out_base, …)`: write `<base>-flat.vmdk` as a
  raw copy (reuse the raw streamer) + `<base>.vmdk` descriptor with the computed
  size/geometry. `ExportFormat::VmdkFlat`, extension `vmdk`.
- **Seam A:** `ImageFormat::VmdkFlat { … }` (carry the resolved flat-extent path
  + size); detection branch matches the descriptor magic / `-flat.vmdk`
  sibling. **Seam B:** `ExportFormat::VmdkFlat`.
- Tests: write split flat, reopen via reader, round-trip; parse a descriptor
  with two extents.

### Session 3.2 — edit + integration

- In-place edit is trivial: the flat extent is raw, so a `Read+Write+Seek` over
  `-flat.vmdk` (plus leaving the descriptor untouched) is enough. Host via
  Seam D.
- Seam C wiring, file-picker filter `vmdk`, docs.

---

## Phase 4 — VMDK sparse (`monolithicSparse`)

Reference: same VMware spec. Layout: sparse header → grain directory (GD) →
grain tables (GT) → grains (default 64 KB). Conceptually the QCOW2 two-level
scheme, so 4.x reuses the patterns proven in Phase 2.

| # | Session | Scope & deliverable |
|---|---|---|
| 4.1 ✅ | **Reader** | `VmdkSparseReader` (`Read + Seek`): parses `KDMV` `SparseExtentHeader`, loads GD into RAM, lazy GT load with single-entry LRU, walks GD → GT → grain for reads, unallocated grains return zeros. Compressed / marker (`streamOptimized`) variants rejected at open. RGD ignored on read (primary GD is authoritative). `ImageFormat::VmdkSparse` + Seam-A detection on `KDMV` magic (placed before flat-descriptor sniff). Tests: hand-built sparse extents covering grain-boundary reads, unallocated zero-fill, seek-then-read, and compressed-flag rejection. |
| 4.2 ✅ | **Writer (convert-to)** | `export_vmdk_sparse` streams a whole-disk source into a single `monolithicSparse` file: header @ sector 0, embedded descriptor (10 KiB), GD, GTs, grain data; allocates grains on non-zero data via `SparseAllocator`, leaves zero-grain GT entries == 0. `overHead` aligned up to grain boundary. Writer emits v3 header (`version=3`, no RGD, no markers). `ExportFormat::VmdkSparse` + `export_whole_disk_vmdk_sparse` dispatcher (backup-folder + raw-source paths). Round-trip + all-zero-input tests; `qemu-img check` gated test passes on installed `qemu-img 11.0.0`. |
| 4.3 ✅ | **Edit + integration** | `Read + Write + Seek` on `VmdkSparseReader<R>`: writes into an unallocated grain append a fresh grain at EOF (rounded up to grain boundary), zero-fill it, set `GT[gt_index][gte_index]` to the new host sector both on disk and in the cached GT, bump `host_end`. Allocated-grain writes update in place. Since the writer pre-allocates every GT (one per GD entry) and never emits RGD, the "grow a GT" / "keep RGD in sync" branches of the QCOW2 editor don't arise. GUI "VMDK (Sparse)" radio on Inspect + Bulk Convert dialogs (whole-disk only, same hover-text rationale as flat); CLI `rb-cli convert --format vmdk-sparse`. Edit test: zero-source → mutate two grains via the editor → reopen via reader → confirm bytes intact + `qemu-img check` clean. |

### On-disk layout

All fields **little-endian** (unlike VHD/QCOW2). The sparse extent embeds its
descriptor.

**SparseExtentHeader (512 B), key fields:**

| Offset | Size | Field | Notes |
|---|---|---|---|
| 0 | 4 | magicNumber | `KDMV` (`0x564d444b`) — Seam A detection |
| 4 | 4 | version | 1–3 |
| 8 | 4 | flags | bit 0 = valid newline test; bit 1 = use RGD |
| 12 | 8 | capacity | sectors |
| 20 | 8 | grainSize | sectors per grain (default 128 = 64 KiB) |
| 28 | 8 | descriptorOffset | sectors (embedded descriptor) |
| 36 | 8 | descriptorSize | sectors |
| 44 | 4 | numGTEsPerGT | grain-table entries per grain table (default 512) |
| 48 | 8 | rgdOffset | redundant grain directory (sectors) |
| 56 | 8 | gdOffset | grain directory (sectors) |
| 64 | 8 | overHead | sectors before grain data starts |

**Two-level map:** the **grain directory (GD)** is an array of u32 sector
offsets, one per grain table. Each **grain table (GT)** has `numGTEsPerGT` u32
entries, each the sector offset of a grain (0 = unallocated → zeros). For a
guest sector `s`: `grain = s / grainSize`; `gt_index = grain / numGTEsPerGT`;
`gte_index = grain % numGTEsPerGT`. `grain_offset_sectors = GT[gt_index][gte] `;
intra-grain = `s % grainSize`. This is structurally the QCOW2 L1/L2 scheme with
u32 sector offsets instead of u64 byte offsets — so the reader/writer mirror
Phase 2, and `sparse_alloc` is reused with grain-sized units.

**RGD:** an optional second copy of the directory + tables (flags bit 1). On
read, tolerate/ignore it; on write, either skip it (set flag off) or keep it in
sync. v1 may emit GD only (no RGD) for simplicity — `qemu-img`/VMware accept
that.

### Session 4.1 — `VmdkSparseReader`

- Parse header (reject compressed grains / markers for v1), read GD into RAM,
  lazily read+cache GTs. `read`/`seek` via the map above; unallocated grains →
  zeros.
- **Reuse, don't re-abstract:** the GD/GT walk + single-entry table cache is the
  same algorithm as QCOW2's L1/L2 reader (session 2.1). This is the *second*
  occurrence, so now is the time to lift a small shared **helper** (the
  table-index / entry-index / intra-unit arithmetic + cache) out of 2.1 and
  reuse it here — but a plain helper struct, **not** a generic trait (CONTRIBUTING
  rejected a unified reader trait; the genuinely shared state is tiny). If the
  shapes diverge more than expected, keep them separate — don't force it.
- **Seam A:** `ImageFormat::VmdkSparse { path, logical_size }`; magic `KDMV`.
- Tests: hand-built minimal sparse extent; round-trip.

### Session 4.2 — writer

- `export_vmdk_sparse(reader, out, grain_size, …)`: lay out header → embedded
  descriptor → GD → GTs → grain data; allocate a grain only for non-zero data
  (`sparse_alloc`). `ExportFormat::VmdkSparse`, extension `vmdk` (disambiguate
  from flat on description / dialog).
- Tests: round-trip via reader; gated `qemu-img check`.

### Session 4.3 — edit + integration

- `Read+Write+Seek` allocate-on-write: append grain at EOF, set GT entry,
  grow/allocate a GT when its GD slot is empty, keep RGD in sync if emitted.
  Seam D + Seam C, docs.

> **Fallback for sparse edit (Phases 2 & 4):** if an in-place edit session
> overruns, the existing archive-edit **expand-to-raw → edit → re-emit** flow is
> the safety net — don't block a phase shipping convert-from/to on in-place
> edit. File the in-place edit as a follow-up session.

---

## Phase 5 — Legacy convert-from (read-only import)

> **Clean-room / provenance rule (PQI, GHO especially).** Derive these formats
> ONLY from (a) bytes of sample files we legitimately own and (b) independently
> reverse-engineered public specs or clean-room implementations under a known
> open license. The 2012 Symantec breach (Lords of Dharmaraja) exposed
> pcAnywhere / Norton AV / SystemWorks source; Ghost and PowerQuest/DriveImage
> were NOT on the confirmed list, and no PQI source leak is known. Regardless:
> **do not consult any leaked Symantec/PowerQuest source** — it would taint the
> implementation (copyright/trade-secret) and poison a clean-room defense.
> `nyarime/gho` is MIT-licensed and itself a clean-room RE → a legitimate
> reference. (PQI was dropped from scope for this reason among others — see the
> note after the table.)

All Phase-5 readers are **convert-from only**. Whole-image-compressed formats
decode to a raw tempfile (reuse the `.adz`/`.hdz` materialize-to-tempfile
pattern); E01 is random-access and gets a native `Read + Seek` reader.
Convert-from = decode legacy → existing inspect/browse, or → a Phase 1–4 modern
writer for "turn this ancient image into a modern one".

**Session order within Phase 5:** lead with the simplest decode-to-temp format
(IMZ) so the **Seam A′ plumbing** (materialize-to-tempfile, `TempDir`-guard
lifetime, worker-thread decode + progress) is built and tested once, then
Partimage / TD0-IMD / GHO inherit it. E01 is the lone *random-access* format
(native `Read + Seek`, no decode-to-temp), so it shares none of that plumbing
and can land anywhere; it's placed after the decode-to-temp group here.

| # | Session | Format | Scope & deliverable | Reference / risk |
|---|---|---|---|---|
| 5.1 ✅ | **WinImage IMZ** | `.imz` reader | **Establishes Seam A′.** `src/rbformats/imz.rs::materialize_imz_to_temp` unwraps a `.imz` ZIP into a fresh `tempfile::TempDir`, picks the first floppy-image entry (`.ima`/`.img`/`.dsk`/`.flp`/`.vfd`), and returns the temp path + guard. Password-protected entries (legacy ZipCrypto, detected via `ZipFile::encrypted()`) are rejected with a precise error. Wired through `gui::materialize_amiga_image_path` alongside `.adz`/`.hdz` so all three formats share one file-pick → materialize → detect-as-Raw path; GUI file-picker filters in Backup/Inspect/Restore tabs accept `imz`. Tests: 6 unit + 2 real-fixture-gated, including byte-for-byte round-trip of `w98_boot_nopassword.imz` vs `w98_boot.ima` and clean rejection of `w99_boot_withpassword.imz`. Worker-thread + progress-cb plumbing **deferred** — IMZ floppies decode in <1 ms; later legacy sessions (5.2 / 5.6) add it as needed since their payloads are MB–GB scale. |
| 5.2 **DEFERRED** | **Partimage** | `.000` reader | **Parked → see `docs/additional_nix_filesystems.md`.** Decode-to-temp (reuses 5.1 plumbing): header + bitmap of used blocks + gzip/bzip2 stream. | partimage source / format doc. **Med.** |
| 5.3 **DEFERRED** | **Teledisk / ImageDisk** | `.TD0` + `.IMD` reader | **Superseded → `docs/mister_filesystem_implementation_plan.md` §3.2** owns TD0/IMD (port from `fluxfox`, MIT). Do not implement standalone here. | Dunfield IMD spec; TD0 RE notes. **Med.** |
| 5.4 **DEFERRED** | **E01 / EWF** | EnCase EWF reader | **Deferred indefinitely — no other plan covers it.** Native `Read + Seek` over chunked EWF segments (per-segment offset table, zlib chunks, CRC per chunk); multi-segment (`.E01`, `.E02`, …). | libewf / libyal docs; Forensic7z confirms it's clean. **Low–Med.** |
| 5.5a ✅ | **GHO container spike** | Norton Ghost | `src/rbformats/gho.rs`: container-header parser (12-byte prefix + optional 16-byte password verifier + optional Pascal description at 0xFF), `GhoCompression` / `GhoImageType` enums, `materialize_gho_to_temp` stub that dispatches by header and produces precise per-case errors (password-protected, SECTOR mode, unknown compression byte, unknown container version). 13 tests, including a corpus-walk that parses 11 real fixtures from `~/new-fixtures/gho/` and asserts the (compression, image_type, password) tuple matches the documented table. **Deferred:** inner record stream (5.5b), Fast-LZ decoder (5.5b), full decode-to-temp + `.GHS` span chain (5.6), SECTOR-mode decoding (5.6b). |
| 5.5b ✅ | **GHO record-stream parser** | Norton Ghost | `GhoRecordHeader` (corrected layout: `u16 type | u16 marker | u32 magic | u16 body_len`, 10 bytes total — the plan's "u32 type" was the type + marker fields run together), `find_inner_stream_start` (scan-forward, absorbs the 7.5 vs 11.5 padding diff: 7.5 starts at sector 6, 11.5 at sector 11), `GhoRecordIter` walker. **Key 5.5b finding:** record stream is structurally identical between Ghost 7.5 and 11.5 — same type + body_len sequence record-for-record over the first 128 records (0 type mismatches, 0 body-len mismatches). Marker differs: 7.5 is always `0x0000`; 11.5 is mostly `0x0000` with ~10% `0x95FD`. 9 new tests including the 7.5-vs-11.5 fixture diff. **Fast-LZ decoder split out to 5.5c** for slice size. |
| 5.5c ✅ | **GHO Fast-LZ decoder** | Norton Ghost | `fast_lz_decompress(data, comp_len, dst)` + `fast_lz_hash` ported from MIT `nyarime/gho/fastlz.go` (4096-entry hash, 16-bit control words, min 3-byte match, sentinel literal `"123456789012345678"`). `data[0] == 1` path forwards a verbatim uncompressed block; otherwise the LZ77 token stream drives literal + match-copy expansion with the documented hash. Covered by 6 synthetic tests (hash spot-check, uncompressed round-trip, literal-only LZ77 token stream, truncation / zero-length / too-short rejections) plus an `#[ignore]`d fixture-gated brute-force scan kept as a hook for 5.6 — that test cannot locate true block boundaries from byte-level scanning since they live in the record framing, so a non-match there is expected, not a decoder regression. No decode-to-temp yet (5.6 wires it). | `github.com/nyarime/gho` (MIT). **Med.** |
| 5.6 ✅ | **GHO SECTOR-mode reader** | Norton Ghost | `materialize_gho_to_temp` decodes **SECTOR-mode** (raw sector-by-sector) backups to a raw disk image in a tempdir, wired into the GUI via `materialize_amiga_image_path` (`.gho`/`.ghs` join `.adz`/`.hdz`/`.imz`). Three compression paths: None (verbatim sector copy), High (zlib block stream), Fast (Fast-LZ block stream). Data starts at the sector after the last `FE EF` sub-header (`find_sector_data_start`); compressed chunks are `[u16 stored_len][block]` where stored_len includes itself, terminated by a record header (peek for record magic at +4). Verified end-to-end against the real `secthigh.GHO` (641 MB zlib → 8.5 GB FAT32 disk, valid boot sig) and `SECTOR.GHO` (uncompressed). **File-aware mode deferred** — the fixture corpus revealed file-aware backups interleave boot-sector (0x0017) / FAT-entry (0x0004) / dir (0x0102-4) metadata with 0x0002 cluster-data records and need a full filesystem rebuilder, not a stream concat; the plan's "file-aware first" assumption (from the Go reference's record model, which does NOT match our corpus's actual record taxonomy) was wrong. The walker (`parse_gho_image`) + `decode_data_blocks_to` are kept as scaffolding. `.GHS` span chain still pending. 4 synthetic tests + 2 `#[ignore]`d real-fixture tests. | as above. |
| 5.6-fa ✅ | **GHO file-aware rebuild** | Norton Ghost | **Shipped — and beyond the original FAT-only scope.** Reconstructs a mountable volume from the file-aware record stream: typed body parsers, directory-tree walker, FAT image emitter (`922a035` unified into an in-RAM virtual FAT, no tempfile), Ghost 7.5 full-disk records, **plus a full NTFS file-aware subsystem** (LCN mapping, compressed/zlib NTFS, fragmented `$MFT`, `$Bitmap`/`$Boot`/`$MFTMirr` synthesis for bootable export). See `docs/gho_file_aware.md`. | `nyarime/gho` for the record model; our own corpus for the actual taxonomy. **High.** |
| 5.6-span ✅ | **GHO `.GHS` span chain** | Norton Ghost | `discover_gho_span_set(picked)` walks the picked file's directory and returns the ordered span set (primary + numbered spans), handling all three corpus naming patterns: stem-prefix `.GHS` siblings (incl. 8.3 truncation like `SECTOR.GHO` + `SECTO00N.GHS`), hyphenated stem-prefix (`gh11-spl.GHO` + `gh11-00N.GHS`), and `.GHO.NNN` numeric suffix (`Win7_86xAMB.GHO.001` ... `.066`). `SpanReader: Read + Seek` virtualises the chain — exposes the primary verbatim, skips the 512-byte container header on every continuation file, so the SECTOR-mode decoder reads one continuous data stream without realising it crosses files. Wired through `materialize_gho_to_temp`: the user can pick the primary OR any span sibling and the whole set decodes. Verified against the real 7.5 `SECTOR.GHO` 4-file set (primary + 3 `.GHS` → 8.5 GiB FAT32 disk with valid boot signature). 6 synthetic tests (naming-pattern parsing, sibling discovery for both patterns, singleton fallback, multi-file reader concatenation + seek) + 1 `#[ignore]`d real-fixture test. Span discovery confirmed against all 6 multi-file corpus sets (4–66 files) via a one-off probe. | `nyarime/gho/span.go` for the .GHS skip-header convention. **Med.** |
| 5.7 ◑ | **Legacy integration** | all of Phase 5 | **Done for the formats that shipped (GHO/IMZ): magic detection, GUI file-picker filters, CLI convert, streaming readers. The remainder is moot — the only unintegrated formats are the DEFERRED 5.2/5.3/5.4 above, so there is nothing left to wire until one of those is built.** | — |

> **PQI (PowerQuest DriveImage) is out of scope.** Dropped 2026-05-23: no
> public spec, no open/clean-room reference (Aaru #400 unimplemented), and
> black-box RE of a proprietary compressed+optionally-encrypted format is too
> much implementation risk for the payoff. Revisit only if a clean-room spec or
> reference implementation appears.

### 5.1 — WinImage IMZ (decode-to-temp) — establishes Seam A′

IMZ is essentially a **zip/zlib-wrapped raw image** (a compressed `.ima`).
Unwrap (the `flate2`/`zip` crate, already in the tree for zstd/zlib use) to a
raw tempfile, then treat as raw. **Low.** Detection: zip local-file-header
magic `PK\x03\x04` on a `.imz` extension (or gzip magic for the gzip variant).

Because it's the simplest decode-to-temp format, **build the shared Seam A′
plumbing here**: the materialize-to-tempfile step, the `tempfile::TempDir`
guard whose lifetime the GUI tab holds (mirroring `.adz`/`.hdz`), and the
worker-thread decode with `progress_cb`/`cancel_check`/`log_cb`. Sessions 5.2,
5.3, and 5.6 reuse this plumbing rather than re-inventing it.

### 5.2 — Partimage (decode-to-temp)

`.000` (+ `.001`… volumes). Header (magic `PaRtImAgE-VoLuMe`) → "info" block →
**used-block bitmap** → the data stream of only the used blocks, optionally
gzip or bzip2 compressed (recorded in the header). Decode = walk the bitmap,
placing each stored block at its filesystem offset and zero-filling the gaps,
into a raw tempfile (Seam A′, built in 5.1). Tolerate the multi-volume split.
**Med.** Reference: partimage source / on-disk-format doc.

### 5.3 — Teledisk TD0 + ImageDisk IMD (decode-to-temp, floppy)

Both are floppy-preservation formats; decode to a raw sector image (tempfile,
Seam A′ from 5.1), then the existing floppy paths (FAT12, interleave) handle it.

- **IMD** (`.IMD`): ASCII header line ending `0x1A`, then per-track records
  (mode, cyl, head, sector count, sector map, optional cylinder/head maps, then
  per-sector data with a type byte: normal / compressed-via-single-byte-fill).
  Straightforward — Dave Dunfield's IMD spec is the reference. **Low–Med.**
- **TD0** (`.TD0`): signature `TD` (normal) or `td` (advanced/LZHUF-compressed
  whole file). Header → optional comment block → per-track/sector records, each
  sector optionally RLE-compressed. The advanced variant needs an **LZHUF**
  (LZSS + adaptive Huffman) decompressor over the whole file first — port a
  known clean implementation. **Med.**

### 5.4 — EnCase E01 / EWF (random-access, native Read + Seek)

Independent of the Seam A′ group — the only Phase-5 format that is randomly
seekable, so it gets a real reader (no decode-to-temp) and can land in any
order. EWF1 layout: a file header then a chain of typed **sections**
(`header`, `header2`, `volume`/`disk`, then repeating `sectors` + `table` +
`table2`, ending `next`/`done`).

- **Signature:** `EVF\x09\x0D\x0A\xFF\x00` (8 bytes) — Seam A, high in the ladder.
- **`volume`/`disk` section:** chunk_count, sectors_per_chunk (commonly 64),
  bytes_per_sector → gives the geometry and chunk size.
- **`table` section:** array of u32 offsets into the segment file, one per
  chunk; high bit set = chunk is zlib-compressed, clear = stored. Each chunk
  decompresses to `sectors_per_chunk * bytes_per_sector` bytes, with a trailing
  Adler-32/CRC.
- **Multi-segment:** `.E01`, `.E02`, … (then `.EAA`…). Open the set; the
  `table`/`sectors` sections continue across segments. The reader maps a guest
  offset → (segment, chunk, intra-chunk) and caches the last decompressed chunk
  (like `ChdReader`).
- Reference: libewf / libyal documentation (the canonical clean public spec).
  **Low–Med.** Scope v1 to EWF1 uncompressed+zlib, no encryption (EWF2/Ex01
  later).

### 5.5 / 5.6 — Norton Ghost GHO (decode-to-temp)

Port the format model from the MIT-licensed clean-room parser
[`nyarime/gho`](https://github.com/nyarime/gho) (do **not** consult any leaked
Symantec source — see the provenance rule above).

**Two layers to keep straight:** there is the *container* header (the first
~512 bytes of every `.gho`/`.ghs` file — same across Ghost 7.5 and 11.5 in
our fixture corpus) and the *inner record stream* (what `nyarime/gho`
documents). The container header was decoded from our own fixtures
(`~/new-fixtures/gho/`) in conversation 2026-05-25; both 7.5 and 11.5 use the
identical 12-byte container layout, so the version split lives **inside the
record stream**, not in the wrapper.

**Container header (verified across 12 fixtures, Ghost 7.5 + 11.5):**

| Offset | Size | Field | Notes |
|---|---|---|---|
| 0x00 | 2 | magic | `FE EF` |
| 0x02 | 1 | container version | `01` for every fixture we have (both 7.5 and 11.5) |
| 0x03 | 1 | compression | `00` = none, `02` = fast, `03` = high |
| 0x04 | 4 | serial / CRC | varies per backup; not yet decoded — confirm in 5.5 spike |
| 0x08 | 2 | flags | constant `01 01` in our corpus; semantics unknown |
| 0x0A | 1 | **image type** | `00` = file-aware (used-data only), `01` = SECTOR (raw sector-by-sector) |
| 0x0B | 1 | password flag | `00` = none, `01` = password set |
| 0x0C | 16 | password verifier | only present (non-zero) when password flag = `01` |

Confirmation table (excerpt from the 2026-05-25 hex-dump pass):

| Fixture | comp | type | pwd |
|---|---|---|---|
| `7.5/FULLDISK/FULLDISK.GHO` | 00 | 00 | 00 |
| `7.5/PART/PART.GHO` | 00 | 00 | 00 |
| `7.5/SECTOR/SECTOR.GHO` | 00 | **01** | 00 |
| `7.5/FD-FAST/FAST.GHO` | 02 | 00 | 00 |
| `7.5/FD-HIGH/FD-HIGH.GHO` | 03 | 00 | 00 |
| `11.5/GH11/fulldisk.GHS` | 00 | 00 | 00 |
| `11.5/GH11-hicompression/High.GHO` | 03 | 00 | 00 |
| `11.5/GH11-password/GH11PW.GHO` | 00 | 00 | 01 |
| `11.5/Gh11-sect compression high/secthigh.GHO` | 03 | **01** | 00 |
| legacy `HPVectra95C.gho` (Ghost ~7.x) | 02 | 00 | 00 |

A Pascal-style description string can appear at offset `0xFF` (length byte +
ASCII, e.g. `"PartitionBackup no compression"`). Present in some 7.5 fixtures,
absent in the 11.5 ones — treat as optional metadata, parse if length byte is
non-zero and offset+length stays within the first sector.

**Image-type variants the reader must handle distinctly:**

- **`0x00` file-aware ("truncated full backup"):** Ghost walks the filesystem
  and stores only used clusters. This is the mode `nyarime/gho`'s record-stream
  parser targets. **Start here for 5.5 / 5.6** — it's the common case and the
  reference parser maps directly.
- **`0x01` SECTOR:** sector-by-sector raw image with the container header in
  front. Simpler in principle — no FS walk, no per-partition record stream — but
  uses a different inner layout (raw + split-marker scheme). Both an uncompressed
  fixture (`7.5/SECTOR/SECTOR.GHO`, ~6.4 GB) and a compressed one
  (`11.5/Gh11-sect compression high/secthigh.GHO`, comp=03, ~641 MB) are in the
  corpus as of 2026-05-25. Defer SECTOR decoder to **5.6b**; spike work in
  5.5 should *detect* and *cleanly reject* SECTOR mode pending its own
  sub-session.

**Inner record stream (from `nyarime/gho`, applies to file-aware mode):**

- **Records (10-byte header):** `[u32 type][u32 magic 0x012F18D8][u16 body_len]`.
  Types: Track0 `0x0006` (MBR + boot sectors), Partition `0x0603` (descriptor),
  Continuation `0x0703` (additional data spans), End `0x0023`.
- **Blocks:** 32 KiB decompressed each, preceded by a control byte — `0x01`
  = stored (uncompressed); other values = Fast-LZ compressed.
- **Compression modes:** Z0 none; **Z1 Fast-LZ** (custom LZ77, 4096-entry hash,
  16-bit control words marking literals vs. matches, min 3-byte match, hash
  `h = ((-24993 * (b2 ^ (16*(b1 ^ (16*b0))))) >> 4) & 0xFFF`); Z3–Z9 zlib.
  Cross-check against our container's compression byte (offset 0x03) — they
  must agree.
- **Spanning:** `.ghs` continuation files (one logical image split across
  multiple files due to CD/DVD/media size limits). The reference parser marks
  span-file **reading** as supported (writing is incomplete — we don't need
  write), so the span-chain logic ports largely for free. **5.5 spike must
  confirm** whether the chain is auto-followed (given `disk.gho`, locate
  `disk.ghs` / `.001` / `.002` …) or the caller supplies each segment, and
  whether a single job ever images *multiple physical disks* (a rarer layering
  = repeated Track0 + partition record sets) vs. the common single-disk split.
  In our 7.5 SECTOR fixture the split files are `SECTOR.GHO` + `SECTO001.GHS`
  + `SECTO002.GHS` + `SECTO003.GHS` (~2 GiB chunks); 11.5 uses smaller
  ~100 MiB chunks with a `disk-001.GHS` / `disk-002.GHS` … naming pattern, and
  `disk.GHO` is the primary.
- **encryption:** CRC-16 cipher on the body when container password flag (offset
  0x0B) is set. Tolerate/skip on first pass, or reject password-protected
  images cleanly with a precise error. The 16-byte verifier at 0x0C is the
  password hash — do not attempt to brute-force; just refuse with "password
  required, not supported" until we wire UI prompting.
- Versions: Ghost 11.x–12.x (what the reference covers) plus 7.5 confirmed
  identical at the container level. Document the supported set; reject unknown
  header variants with a clear message.

**5.5 (spike) deliverables (updated 2026-05-25):**
1. Container-header parser (the 12-byte + optional password-verifier layout
   above), tested against every fixture in `~/new-fixtures/gho/`.
2. Image-type dispatch: file-aware vs SECTOR; SECTOR returns a clear
   "not yet supported" error.
3. Rust skeleton + record-header parsing against `11.5/GH11/fulldisk.GHS`
   (uncompressed file-aware) — confirms `nyarime/gho`'s record model holds in
   our corpus and that the inner stream begins at the expected offset past the
   container header.
4. Diff inner records of `7.5/PART/PART.GHO` vs `11.5/GH11/fulldisk.GHS` to
   confirm the record stream is identical between Ghost 7.5 and 11.5 (or note
   the deltas).
5. Fast-LZ decoder as a standalone tested unit (oracle: round-trip a known
   buffer via a reference Fast-LZ in Go/Python, or — simpler — against a real
   `02`-compressed `.gho` once we can locate a block in it).
6. Password-protected fixtures (`GH11PW.GHO`, `hipwd.GHO`) must open far enough
   to read the container header and produce the "password required" error
   without panicking.

**5.6 (reader) deliverables:**
- Wire decode-to-temp (Track0 → MBR at LBA 0, partitions at their offsets, gaps
  zero-filled) for file-aware mode.
- `.ghs` span handling (auto-follow when given the primary file).
- zlib path for compression byte `0x03`.
- Tests against the 5.5 fixtures plus the new compressed-SECTOR sample once
  the user provides it.
- Seam A′ + `ImageFormat::Gho { … }`.

**SECTOR-mode reader:** scheduled as a follow-up sub-session (call it 5.6b) once
a compressed-SECTOR fixture is in hand. Not a blocker for shipping
file-aware-mode convert-from.

### 5.7 — Legacy integration

Wire all Phase-5 readers through Seam A/A′ (detection + reader/materializer),
add CLI `convert --from`/auto-detect, GUI open-file filters
(`E01`/`Exx`, `000`, `imz`, `imd`, `td0`, `gho`/`ghs`), and update the README
format table. Convert-from a legacy image to any Phase 1–4 modern format is then
just "materialize → existing export path".

### 5.8 (STRETCH, optional) — Partial recovery of corrupt / truncated archives

> **DEFERRED INDEFINITELY (2026-06-01).** Always a stretch goal; not started.
> The design below is preserved as the spec for whenever a damaged-archive
> recovery need actually arises.

Not required for normal convert-from; this is a separate **resilience layer**
for "I have a damaged multi-file Ghost backup and want to salvage what I can."
The `nyarime/gho` reference is happy-path only (aborts on the first invalid
structure), so none of this comes for free — it is entirely our own work and is
explicitly a stretch goal, scheduled only after 5.6 ships clean.

Design (GHO-specific but the pattern generalizes):

- **Resync, don't abort.** GHO records carry a fixed magic (`0x012F18D8`). A
  recovery reader, on a failed block CRC / malformed record / unexpected EOF,
  logs the damaged byte range, **zero-fills the lost span** of the output disk,
  then scans forward for the next valid record magic and resumes. The output is
  a raw image with holes where data was unrecoverable.
- **Tolerate a broken span chain.** If a `.ghs` segment is missing or truncated,
  recover the segments that are present, zero-fill the gap for the absent one,
  and continue with the next available segment rather than failing the whole
  job.
- **Recovery report.** Emit a structured summary (mirroring the existing
  fsck/`RepairReport` style): bytes recovered vs. zero-filled, which partitions
  / disk offsets are intact vs. damaged, and which `.ghs` segments were
  missing. Surface it in the log and as a returned struct.
- **Partition-level partial restore.** Because Track0 + per-partition records
  let us map damaged ranges back to partitions, a partition whose data survived
  intact can be restored even if a sibling partition's blocks are corrupt.
  Hook this into `reconstruct_disk_from_backup` so the user can choose
  "restore the recoverable partitions".
- **Gating:** behind an explicit `--recover` / "attempt recovery" opt-in — the
  normal path stays strict (a CRC failure is an error), so we never silently
  hand back a partially-zeroed image as if it were good.

This sizes to 1–2 sessions on its own and depends on 5.6. Treat it as a
post-MVP follow-up, not part of the Phase 5 baseline.

---

## Phase 6 — VHDX (deferred indefinitely)

> **DEFERRED INDEFINITELY (2026-06-01).** Never scheduled; modern Hyper-V only,
> zero retro demand, highest cost of any format here. Spec preserved below.

Only if a concrete Hyper-V need appears. Region table + metadata region + BAT +
**log/journal replay-on-open and write-on-edit** for crash consistency, plus
1 MB+ block alignment and CRC32C. Estimated 3–4 sessions (reader / writer /
edit-with-log / integration). Not scheduled.

---

## Session inventory (final status)

| Phase | Sessions | Status |
|---|---|---|
| 1 — VHD dynamic (warmup) | 1.1 reader, 1.2 writer + `sparse_alloc`, 1.3 edit, 1.4 integration | ✅ complete |
| 2 — QCOW2 (priority) | 2.1 reader, 2.2 writer, 2.3 edit(a), 2.4 edit(b)+integration | ✅ complete |
| 3 — VMDK flat | 3.1 reader+writer, 3.2 edit+integration | ✅ complete |
| 4 — VMDK sparse | 4.1 reader, 4.2 writer, 4.3 edit+integration | ✅ complete |
| 5 — legacy convert-from | 5.1 IMZ ✅, 5.5/5.6 GHO (container/records/Fast-LZ/SECTOR/span/file-aware FAT+NTFS/password) ✅ | ✅ for IMZ + GHO |
| 5 — deferred | 5.2 Partimage → `additional_nix_filesystems.md`; 5.3 TD0/IMD → MiSTer plan §3.2; 5.4 E01; 5.8 recovery | **DEFERRED** |
| 6 — VHDX | reader / writer / edit / integration | **DEFERRED INDEFINITELY** |

Recommended execution order: **1 → 2 → 3 → 4 → 5**. Within Phase 5, do **5.1
(IMZ) first** to build the Seam A′ decode-to-temp plumbing that 5.2/5.3/5.6
reuse; 5.4 (E01) is independent (native reader) and can land anywhere.

## Reference fixtures to gather before starting

- UTM classic-Mac QCOW2 (APM + HFS) — Phase 2 acceptance.
- `qemu-img`-generated QCOW2 + VMDK (flat & sparse) + dynamic VHD — round-trip oracles.
- A real `.E01` set, a Partimage `.000`, a WinImage `.imz`, an `.IMD`/`.TD0` floppy.
- Sample `.gho` (+ `.ghs` span) for the GHO sessions.

## External references

- VHD: Microsoft VHD Image Format Specification.
- QCOW2: QEMU `docs/interop/qcow2.txt`.
- VMDK: VMware Virtual Disk Format spec.
- E01/EWF: libewf / libyal documentation.
- GHO: https://github.com/nyarime/gho (MIT) — Rust port reference.
- Aaru (`aaru-dps/Aaru`): broad cross-reference for several legacy formats.
</content>
