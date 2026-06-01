# MiSTer Filesystem Support — Implementation Plan

Companion to [`full_MiSTer_support_status.md`](full_MiSTer_support_status.md),
which surveys *what* is outstanding. This document is the *how*: a concrete,
multi-platform (macOS / Windows / Linux) implementation plan for every
outstanding MiSTer computer-core filesystem, written against the architecture
and conventions in [`../CONTRIBUTING.md`](../CONTRIBUTING.md).

Survey date: 2026-05-31. Toolchain baseline: edition 2021, latest stable Rust.

---

## Progress tracker (live state — START HERE each session)

**This section is the single source of truth for progress.** It is committed to
git, so it travels between machines: **pull before a session, update + commit
after.** Do not track status anywhere else (not in chat, not in memory). §0–§8
below are the reference/design; this tracker is the live state.

**Status legend:** `[ ]` not started · `[~]` in progress · `[x]` done ·
`[!]` blocked (add a note).

**Current position:** Planning complete and technically verified. No
implementation started. Next action: Wave-1 shared infra + AtariST.

**Session log** (newest first; one line per session — date, what moved, what's next):
- 2026-05-31 — Plan authored; tech assumptions verified (AHDI ✓, MSA ✓, a2kit
  CP/M DPB ✓ flexible; fluxfox import ✗ → port decoders from source). Net-new
  deps = 0. No code yet. Next: §3.1 + container framework, then AtariST.

### Per-format task spine

Every core advances through these stages (skip stage 7 for floppy-only formats).
A core is **done** only when every applicable stage is `[x]`.

1. **Prereqs** — container decoder(s) + partition table, if the format needs them
2. **Detect + inspect** — dispatch hint registered in `fs/mod.rs`; volume summary
3. **Browse + extract** — list dirs, read files, checksum verify
4. **Reference cross-check** — extracted bytes match a reference tool (§6)
5. **Add/Delete** — `EditableFilesystem` write path with snapshot/rollback
6. **Write verified** — re-read + reference tool + emulator/core boot (joint step)
7. **Resize + compaction reader** — HDD media only
8. **GUI wiring**
9. **CLI parity** (`rb-cli`)
10. **Unit tests + fixture committed**

### Shared infrastructure (built lazily; first wave that needs it)

- [ ] §3.1 non-512 logical-sector accessor + `src/fs/README.md` note
- [ ] §3.2 container framework `src/rbformats/containers/` (`open_container` dispatch)
- [ ] §3.2 decoders: [ ] MSA · [ ] EDSK · [ ] TD0 (port) · [ ] IMD (port) · [ ] GCR `.g64/.g71` · [ ] `.nib` · [ ] `.d88`
- [ ] §3.3 partitionless / extension-dispatch framework
- [ ] §3.4 convention docs (endianness, bitmap polarity, write-safety)

### Wave 1 — near-complete dual-media cores

- [ ] **AtariST** — prereqs [ ] MSA [ ] AHDI table · [ ] inspect · [ ] extract · [ ] ref · [ ] add/del · [ ] write-verified · [ ] resize (FAT/HDD) · [ ] gui · [ ] cli · [ ] tests
- [ ] **MacPlus** (MFS 400K) · [ ] inspect · [ ] extract · [ ] ref · [ ] add/del · [ ] write-verified · [ ] gui · [ ] cli · [ ] tests
- [ ] **Apple-II** (DOS 3.3) — prereq [ ] sector-order container · [ ] inspect · [ ] extract · [ ] ref · [ ] add/del · [ ] write-verified · [ ] gui · [ ] cli · [ ] tests

### Wave 2 — new dual-media cores (all carry the full spine incl. resize unless noted)

- [ ] **X68000** (Human68k) — prereqs [ ] `.d88` container [ ] X68k partition · [ ] inspect · [ ] extract · [ ] ref · [ ] add/del · [ ] write-verified · [ ] resize · [ ] gui · [ ] cli · [ ] tests
- [ ] **Archie** (ADFS/FileCore) — prereq [ ] `.hdf` header handling · [ ] inspect · [ ] extract · [ ] ref · [ ] add/del · [ ] write-verified · [ ] resize · [ ] gui · [ ] cli · [ ] tests
- [ ] **QL** (QDOS) — prereqs [ ] `.mdv` [ ] `QXL.WIN` containers · [ ] inspect · [ ] extract · [ ] ref · [ ] add/del · [ ] write-verified · [ ] resize · [ ] gui · [ ] cli · [ ] tests
- [ ] **Altair8800 / CP/M** — prereqs [ ] DPB registry [ ] EDSK · [ ] inspect · [ ] extract · [ ] ref · [ ] add/del · [ ] write-verified · [ ] resize (CF/IDE) · [ ] gui · [ ] cli · [ ] tests
- [ ] **BK0011M** (ANDOS) · [ ] inspect · [ ] extract · [ ] ref · [ ] add/del · [ ] write-verified · [ ] resize (stretch) · [ ] gui · [ ] cli · [ ] tests

### Wave 3 — floppy-only long tail (full spine, no resize)

- [ ] **Commodore** (CBM: C64/128/16/VIC20/PET) — prereq [ ] GCR container; port `cbm` · [ ] inspect · [ ] extract · [ ] ref · [ ] add/del · [ ] write-verified · [ ] gui · [ ] cli · [ ] tests
- [ ] **CP/M floppy cores** (Amstrad, PCW, Einstein, SVI328, MultiComp, ZX+3) — reuse CP/M engine; per core: [ ] DPB preset · [ ] verify
- [ ] **Atari800** (Atari DOS) — full spine
- [ ] **CoCo2/3** (RS-DOS/DragonDOS + OS-9 RBF — two FS) — full spine ×2
- [ ] **Oric** (Sedoric) — full spine
- [ ] **PC88** (N88-BASIC) — full spine
- [ ] **TRS-80** (TRSDOS/LDOS) — full spine
- [ ] **SharpMZ** — full spine
- [ ] **TI-99_4A** — full spine
- [ ] **ColecoAdam** (EOS; extract-focused, core is RO) — spine through stage 6
- [ ] **Vector-06C** (MicroDOS) — full spine
- [ ] **Specialist** (Specialist-MX) — full spine

> Rough effort (soft): infra + Wave 1 ≈ 15–24 sessions; through Wave 2 ≈ 50–80
> cumulative (covers all high-value dual-media cores); full plan incl. Wave 3 ≈
> 100–150. Recalibrate after the first two cores land.

---

## 0. Guiding principles

1. **The product is disk manipulation, not archival.** Rusty Backup exists to
   do two things the host OS cannot: (a) **restore a backup to a
   different-sized disk** (resize-on-restore), and (b) **add / extract files
   to-and-from an image whose filesystem macOS / Windows / Linux can't mount**.
   Plain whole-image copy is table stakes; these two capabilities are the
   reason the tool exists. Every filesystem decision below is judged against
   them.
2. **Multi-platform, pure Rust, no new C toolchain.** We already pay for one
   native build (`libchdman-rs`). Every filesystem here is byte manipulation —
   it must compile and run identically on macOS, Windows, and Linux with **no
   FFI, no C build step, no platform `cfg`**. This single rule kills importing
   `a2kit` wholesale (it drags in `tree-sitter`, a C build).
3. **Engine-first, per CONTRIBUTING.** Each filesystem lands in `src/fs/` with
   inline unit tests, wired through the `src/fs/mod.rs` routing helpers, before
   any GUI or CLI work. No `match fs_type { … }` ladders outside `fs/mod.rs`.
4. **Import vs port is a per-crate decision** driven by dependency hygiene
   (§2), not convenience. Ported code carries an attribution header (all three
   source crates are MIT / MIT-OR-Apache, compatible with our AGPL-3.0).

### The two capability axes

We scope each filesystem by **capability**, not by a single linear tier,
because the two product goals are independent and apply to different cores:

- **Axis 1 — File manipulation (read → extract → add/delete).** Needed to get
  files in and out of an OS-unmountable image. Applies to **every** format,
  most of all the floppy ones (the host OS literally cannot read a D64 / ATR /
  ADFS / CP/M disk). The **full Add/Delete capability is the target for every
  format** — we are not shipping read-only support and calling a format done.
  Implementation order within a format:
  - **Extract** (first milestone) — detect + inspect + browse (list) + read-out
    files + verify. Read-only, cannot corrupt anything; lands first.
  - **Add/Delete** (the goal) — `EditableFilesystem`: allocate in the free map,
    write directory entries, delete. This is the real payoff for OS-unsupported
    disks, and it is *most* of the per-FS work (the allocator + directory
    writer are the hard, quirky parts).
  - **fsck/repair** — opportunistic; only where the structure makes it
    tractable and a real corruption mode exists.
- **Axis 2 — Resize-on-restore.** Grow/shrink the volume to a different target
  size: relocate structures, rewrite the free map, patch geometry, plus a
  compaction reader for the backup side. Only **meaningful on hard-disk media**
  where target size can vary. Floppies are fixed-geometry — resize is a
  non-concept there, so floppy-only formats never get Axis 2.

`Add/Delete` is the target for **every** format; `Extract` is just the first
milestone toward it. `Resize` is layered on top for HDD-capable cores. fsck is
opportunistic.

---

## 1. Capability target per outstanding filesystem

"File manip" gives the Axis-1 target; "Resize" is Axis-2 (HDD only).

| Filesystem | Cores | Media | File manip (Axis 1) | Resize (Axis 2) | Notes |
|---|---|---|---|---|---|
| CBM DOS | C64, C128, C16, VIC20, PET | Floppy | Add/Delete | — | `cbm` already does write; flat dir |
| Apple DOS 3.3 | Apple-II | Floppy | Add/Delete | — | a2kit does write; complements ProDOS |
| CP/M (parameterized) | Amstrad, PCW, Einstein, SVI328, Altair, MultiComp, ZX+3 | Floppy + Altair CF/IDE | Add/Delete | HDD only (Altair) | a2kit does write; DPB registry |
| Human68k | X68000 | Floppy + SASI HDD | Add/Delete | **Yes** | FAT-derived; resize via FAT machinery |
| ADFS / FileCore | Archie, BBC/Electron | 800K floppy + HDD | Add/Delete | **Yes** (HDD) | Ground-up |
| QDOS | QL | Microdrive + QXL.WIN HDD | Add/Delete | **Yes** (HDD) | Ground-up |
| Atari DOS | Atari800 | Floppy | Add/Delete | — | Ground-up |
| OS-9 RBF / RS-DOS / DragonDOS | CoCo2, CoCo3 | Floppy | Add/Delete | — | Two FS families |
| TRS-80 (TRSDOS/LDOS) | TRS-80 | Floppy | Add/Delete | — | Variant-heavy |
| Sedoric / Oric DOS | Oric | Floppy | Add/Delete | — | Niche |
| N88-BASIC | PC88 | Floppy | Add/Delete | — | Niche |
| Sharp MZ FD | SharpMZ | Floppy | Add/Delete | — | Niche |
| EOS | ColecoAdam | Floppy | Add/Delete | — | Core is RO; write still implemented for completeness |
| TI-99 FS | TI-99_4A | Floppy | Add/Delete | — | Niche |
| ANDOS/CSIDOS | BK0011M | Floppy + VHD | Add/Delete | **Yes** (HDD) | Only Soviet HDD core |
| MicroDOS / Specialist-MX | Vector-06C, Specialist | Floppy | Add/Delete | — | Niche |

**Full support per format is the goal:** every filesystem targets Add/Delete
(read + extract + add + delete), and every HDD-capable format additionally
targets Axis-2 resize. Within a single format we still *implement* extract
before the write path (read can't corrupt, and it's the natural first
milestone), but no format is considered done until its write path lands.
fsck/repair remains opportunistic (added where a real corruption mode and a
tractable structure exist).

Near-complete touch-ups (already mostly supported, listed for sequencing):
**Atari ST AHDI partition table** (AtariST HDD — enables Axis-2 resize of its
FAT16 partitions), **BPB-less FAT detection** (Atari ST `.st`, MSX `.dsk`),
**MFS** (MacPlus 400K floppy, extract).

---

## 2. Dependency analysis and import-vs-port decisions

Checked each candidate crate's manifest against our `Cargo.toml`
(`zip 8`, `flate2 1` w/ zlib-ng, `sha2 0.11`, `aes 0.8`, `thiserror 2`,
`env_logger 0.11`, `eframe 0.34`, edition 2021).

### 2.1 `fluxfox` (dbalsom/fluxfox) — **PORT decoders, do NOT import** (revised after build test 2026-05-31)

The original plan was to import fluxfox lean. **A real build test killed that:**

- **crates.io is stale and broken.** Published release is `fluxfox v0.1.0`
  (the active repo is far ahead). `cargo add fluxfox --no-default-features`
  then `cargo build` **fails to compile** (9 errors — feature-gated code paths
  leave functions return-less). With default features v0.1.0 *does* build but
  pulls a heavy tree and a **future-incompat transitive dep `typemap v0.3.3`**
  (slated for rejection by a future rustc).
- **git HEAD (v0.2.0) can't build lean either.** `--no-default-features`
  fails (13 errors — `rand` is declared optional but used unconditionally in
  `track/metasector.rs`). So lean is simply not a supported configuration.
- **git HEAD with default features builds, but drags ~187 packages**,
  including `rhai` (an embedded scripting engine), `tar`, `zstd-sys` (a second
  C build on top of the one we already have), `xattr`, and `wasm-bindgen` /
  `web-sys`. That directly violates guiding principle #2 (lean, pure-Rust, no
  extra C builds), and we couldn't trim it because lean is broken.
- **Decision:** **do not add fluxfox as a dependency.** Treat it like `cbm` /
  `a2kit` — a **port reference**. Lift only the specific container decoders we
  need (TD0, IMD, and `.d88` if present) from its MIT source into
  `src/rbformats/containers/`, taking the small pure-Rust helpers they need
  (`binrw` is the only notable one, and only if we choose to keep it). Hand-
  write the rest (EDSK, GCR, MSA, `.nib`) — they are small.
- **Consequence:** the container-decode layer (§3.2) is **ported source, not a
  crate**. This also removes the only proposed net-new dependency (§2.4).

### 2.2 `cbm` (simmons/cbm) — **PORT the disk modules**

- Edition 2021, but deps are stale: `memmap 0.6.2` (**deprecated**,
  RUSTSEC-2020-0077 — successor is `memmap2`) and `clap 2.34` (the CLI binary
  only).
- The D64/D71/D81 logic is small, self-contained byte manipulation; `memmap`
  is a convenience for the file backing and `clap` is only the bundled CLI.
- **Decision:** port the `disk` / image modules into `src/fs/cbm.rs`, feeding
  them our own `Read + Write + Seek`. **Drop `memmap` and `clap 2` entirely.**
  Small port (a few files), removes both flagged deps. MIT/Apache attribution
  header.

### 2.3 `a2kit` (dfgordon/a2kit) — **PORT the disk/FS modules only**

- Edition **2024** (needs rustc ≥ 1.85). Dependency surface is dominated by the
  *language-services* half we don't want: `tree-sitter 0.25` + `tree-sitter-
  applesoft` / `-integerbasic` / `-merlin6502` (a **C build**), `lsp-server`,
  `lsp-types`, `fluent-uri`, `colored`, `math-parse`, plus `atty 0.2`
  (**unmaintained**, RUSTSEC-2021-0145).
- The disk/FS half (DOS 3.3, ProDOS, CP/M, Pascal, and the
  2MG/DSK/DO/PO/IMD/NIB/WOZ image readers) realistically only needs `binrw`,
  `bit-vec`, `num-traits`/`num-derive`, and the author's `a2kit_macro` /
  `retrocompressor` helpers.
- **Module layout verified (2026-05-31, repo clone):** `src/fs/` is cleanly
  modular — `dos3x/`, `cpm/`, `prodos/`, `pascal/`, `fat/`, `fimg/` each in
  their own directory. **But the modules are not self-contained:** `dos3x` and
  `cpm` import `a2kit_macro::DiskStruct` (a byte-(de)serialization derive) and
  lean on a2kit-internal `img` / `Block` / `FileImage` / `DiskFormat`
  abstractions. So the port is **"reimplement against our `Filesystem` trait,
  using a2kit's logic + on-disk constants as the reference,"** not a verbatim
  file copy. Budget glue accordingly.
- **CP/M DPB verified flexible (good news for B3).** `src/bios/dpb.rs` defines
  a fully parameterized `DiskParameterBlock` (all standard fields: `spt`,
  `bsh`, `blm`, `exm`, `dsm`, `drm`, `al0`, `al1`, `cks`, `off`, `psh`, `phm`)
  plus a preset table (`A2_525`, `CPM1`, `SSSD_525`, `SSDD_525_OFF1`,
  `TRS80_M2`, …) it tries heuristically. This is **exactly** our DPB-registry
  design — porting it means adding Amstrad / PCW / Einstein / SVI328 presets as
  new consts. B3 de-risked.
- **Decision:** **do not import.** Reimplement the filesystem modules we need
  (DOS 3.3 first; CP/M with our DPB presets; Pascal later) against our trait,
  using a2kit as reference. This avoids forcing `tree-sitter`'s C toolchain and
  the `atty` advisory onto our Windows/macOS/Linux CI. MIT attribution header.
  For TD0 decompression, port from fluxfox source (§2.1) so we have one TD0
  decoder rather than pulling a2kit's `retrocompressor`.

### 2.4 Net new dependencies after this plan

**Zero.** The build test (§2.1) ruled out importing fluxfox, and `cbm` / `a2kit`
were always ports. All filesystem, container, and partition-table code is
ported or hand-written source under `src/fs/`, `src/rbformats/containers/`, and
`src/partition/`. No new crates, no new C build steps, no advisory-flagged or
deprecated dependencies enter the tree. (If we later choose to keep `binrw` for
the ported parsers, that is the *only* candidate addition, and it is optional —
the existing `byteorder` covers our needs.)

---

## 3. Cross-cutting infrastructure (must land first)

These are shared prerequisites. Each is its own slice/commit.

### 3.1 Non-512-byte logical sectors

Many of these filesystems use 128 B (Atari SD), 256 B (Acorn, CBM, Apple,
OS-9), or 1024 B (X68000, PC-98) sectors. Our FS implementations already take
`R: Read + Seek` and compute their own offsets, so a new FS can use any sector
size internally. The constraint is at the **raw-device write boundary**
(`SectorAlignedWriter`, 512/4 KiB) — but these are **file-backed image**
formats, not raw `\\.\PhysicalDrive` targets, so they flow through the
file-restore path, not the device path. Action: document this in
`src/fs/README.md` and add a `logical_sector_size()` accessor to the volume
summary so inspect/GUI display the real geometry. **No change to
`SectorAlignedWriter`.** (Effort: S)

### 3.2 Container-decode layer — `src/rbformats/containers/`

Several image formats wrap sectors with per-track geometry / compression:
`.d88`, EDSK, `.g64/.g71` (GCR), `.msa`, `.dim`, `.vdk/.jvc`, `.fdd/.edd`,
`.odi`, `.nib`, TD0, IMD. A container is conceptually "decode wrapper → flat
sector stream", consumed *before* partition detection.

**Placement decision: this lives in `src/rbformats/`, not a new top-level
module.** It's the same category as the existing `ChdReader`
(`src/rbformats/chd.rs`) — a `Read + Seek` wrapper that decodes a wrapped
format on the fly and hands flat bytes to the partition/FS layer — and VHD
(also a container) already lives there. Put the decoders in
`src/rbformats/containers/`.

- Expose `fn open_container(path) -> Result<Box<dyn ReadSeek + Send>>` that
  sniffs the wrapper and returns a flat-sector view. Raw/already-flat images
  pass through untouched.
- **Ported, not a dependency** (§2.1 build test ruled out importing fluxfox).
  Port the TD0 and IMD decoders from fluxfox's MIT source; hand-write the
  others (EDSK, GCR `.g64/.g71`, `.nib`, `.d88`, MSA) — each is a small, well-
  documented format. We control the code and pull no heavy tree.
- **Dependency direction stays clean:** the *open path* composes
  `container-reader → partition-detect` (exactly how `browse_view` wraps a file
  in `ChdReader` today), so `partition/` never depends on `rbformats/` — no
  cycle. The container reader is constructed by the caller, then its flat
  stream is fed to detection.
- Tests: round-trip a known sector dump through each wrapper decoder.
- (Effort: M — TD0/IMD ported from fluxfox source; the rest hand-written.)

### 3.3 Partitionless / extension dispatch for exotic images

Most of these have no MBR/GPT/APM. Follow the **BasiliskII HFV pattern**
(`detect_superfloppy → PartitionTable::None { fs_hint }`, see `CLAUDE.md`):

- Extend `partition::detect_superfloppy` (and a new
  `fs_hint_from_extension_and_magic`) so a `.d64` → `fs_hint:"cbmdos"`,
  `.atr` → `"ataridos"`, `.adf`(Acorn 800K) → `"adfs"`, etc., when no partition
  table is present.
- Add the new hint strings to the `fs/mod.rs` dispatch helpers exactly as
  Amiga did: `fs_name_for`, `is_browsable_type`, `is_layout_preserving_fs`,
  `is_expensive_minimum`, `open_filesystem_by_string`,
  `open_editable_filesystem_by_string`, `compact_partition_reader_by_string`.
- Because `.adf` collides between Amiga (880K) and Acorn (800K) and `.dsk`
  collides across a dozen systems, dispatch must be **content-magic first,
  size/extension second** — never extension alone. Reuse the NTFS-vs-exFAT
  precedent (type byte shared, OEM magic disambiguates).
- (Effort: S per filesystem to register; the framework is one M slice.)

### 3.4 Reused conventions

- **Endianness:** Acorn, QDOS, Mac/MFS, and Amiga are big-endian; X68000
  Human68k FAT is little-endian. Use `byteorder` (already a dep) explicitly per
  field; never assume.
- **Bitmap polarity:** CBM BAM, Amiga (already handled), and some others use
  "set bit = free". This already bit us on Amiga and IRIX — call it out in each
  allocator's doc-comment.
- **fsck shared types:** any filesystem that grows an fsck/repair path
  produces the shared `FsckResult` / `FsckIssue` / `RepairReport` from
  `src/fs/fsck.rs` (see `docs/fsck.md`), never bespoke result types.
- **Write safety (Axis 1 Add/Delete):** every editable filesystem follows the
  existing snapshot/rollback + staged-edit pattern (`docs/editing.md`) so a
  failed allocation or directory write rolls back rather than leaving a
  half-mutated vintage image. Round-trip each write against an external
  reference tool (§6) before enabling it by default.

---

## 4. Per-filesystem implementation plans

This section is the **building-block catalog**: the per-filesystem detail that
the per-core roadmap in §5 draws on. A single core may pull several blocks from
here (a filesystem + a container decoder + a partition table); §5 is what
orders their delivery, core by core.

Each entry: code placement → on-disk structure → reference source → dispatch
wiring → tests → capability/effort. All follow the CONTRIBUTING playbook
(engine → model → view → CLI), so only filesystem-specific notes are given.
"Capability" states the Axis-1 target and whether Axis-2 resize applies.

### Group A — FAT-adjacent quick wins

#### A1. BPB-less FAT detection (port from TotalImage)
- **Cores helped:** AtariST `.st`, MSX `.dsk`, odd PC floppies (already
  supported FS, hardening only).
- **Placement:** extend `src/fs/fat.rs::open` with a fallback chain mirroring
  TotalImage `FatFactory.cs`: infer geometry from image size + media byte +
  dual-FAT validation; try BPB at 0x04 (Zenith Z-100) and 0x50 (Apricot);
  special-case Acorn-DOS-800K and Victor 9000. Port the geometry table from
  `FloppyGeometry.cs`.
- **Tests:** detection unit tests for each geometry from synthetic images.
- **Capability:** hardens existing FAT (extract + add + resize already exist) ·
  **Effort:** S · MIT attribution.

#### A2. Atari ST AHDI partition table
- **Core:** AtariST HDD (`.vhd`-renamed images; FAT16 inside).
- **Placement:** `src/partition/atari.rs` — parse the AHDI root sector
  (bootable + up to 4 entries, big-endian, `GEM`/`BGM` IDs). Add
  `PartitionTable::Ahdi` variant; route partitions to existing FAT.
- **Tests:** parse a known AHDI layout; round-trip serialize.
- **Capability:** unlocks Axis-2 resize of the FAT16 partitions (FAT does the
  rest) · **Effort:** S.

#### A3. Human68k (X68000)
- **Cores:** X68000 floppy (`.d88`/`.dim`/`.xdf`) + SASI HDD (`.hdf`).
- **Structure:** FAT12/16-derived but **18.3 filenames, Shift-JIS, case-
  sensitive**, X68k partition scheme on HDD. Reuse FAT cluster/FAT-table logic;
  write a Human68k directory-entry layer on top.
- **Placement:** `src/fs/human68k.rs`; floppy containers via `src/rbformats/containers/`
  (§3.2). Decode Shift-JIS via a small table (no new dep; or `encoding_rs` if
  we accept one well-maintained dep — **prefer a static table**).
- **Capability:** Add/Delete + Axis-2 resize on HDD (reuses FAT cluster/resize
  machinery) · **Effort:** M.

### Group B — crate-backed (port or import)

#### B1. CBM DOS (port `cbm`)
- **Cores:** C64, C128, C16, VIC20 (D64/D71/D81); PET (D80/D82) is an
  extension.
- **Placement:** port `cbm`'s image modules → `src/fs/cbm.rs`; feed our
  `Read+Write+Seek`; drop `memmap`/`clap`. Flat directory on track 18 (D64);
  BAM is "set bit = free". GCR `.g64/.g71` decode via `src/rbformats/containers/`.
- **Dispatch:** `fs_hint:"cbmdos"` (§3.3). PET D80/D82 add a geometry variant.
- **Capability:** Add/Delete (cbm already does write/delete/rename, so the
  expensive part is done); no Axis-2 (floppy) · **Effort:** M · MIT/Apache
  attribution.

#### B2. Apple DOS 3.3 (port from `a2kit`)
- **Core:** Apple-II (`.dsk`/`.do` sector-order; ProDOS already done).
- **Placement:** port a2kit's DOS 3.3 module → `src/fs/apple_dos.rs`. Handle
  the `.do` vs `.po` physical/logical sector-skew (a2kit encodes this) via the
  `src/rbformats/containers/` layer or an order-translating reader.
- **Capability:** Add/Delete (a2kit does write); no Axis-2 (floppy) ·
  **Effort:** S-M · MIT attribution.

#### B3. CP/M (port from `a2kit`, parameterized)
- **Cores:** Amstrad, AmstradPCW, TatungEinstein, SVI328, Altair8800,
  MultiComp, and ZX-Spectrum +3DOS.
- **Structure:** CP/M needs a **per-machine Disk Parameter Block (DPB)** — the
  single biggest design point. Build a small DPB registry (à la `cpmtools`
  `diskdefs`) keyed by detected geometry / core. **Verify a2kit's CP/M accepts
  arbitrary DPBs**; if it's Apple-CP/M-centric, port the directory/extent logic
  and supply our own DPB table.
- **Placement:** `src/fs/cpm.rs` + `src/fs/cpm_diskdefs.rs`. EDSK floppies via
  `src/rbformats/containers/`.
- **Capability:** Add/Delete across all ~7 cores; Axis-2 resize only on
  Altair's CF/IDE (floppies fixed) · **Effort:** L (one impl covers ~7 cores) ·
  MIT attribution.

#### B4. Container formats (port from `fluxfox` source — NOT a dependency)
- Covered by §3.2. Port TD0/IMD from fluxfox's MIT source; check whether that
  source includes a `.d88` decoder to port, else hand-write the RIFF-like chunk
  reader (small). See §2.1 for why fluxfox is a port reference, not a crate.

### Group C — ground-up (no Rust prior art)

For each: implement `Filesystem` (extract floor), inline tests from a real
image fixture, register the hint in `fs/mod.rs`. Add the write path
(`EditableFilesystem`) per the capability column. References are C/C++/spec
only. "Effort" is for the extract floor; "+write" notes the add/delete
increment; "+resize" the Axis-2 increment where applicable.

| FS / module | Cores | On-disk notes | Reference | Capability + effort |
|---|---|---|---|---|
| `src/fs/adfs.rs` (FileCore) | Archie, BBC/Electron | 256 B blocks, free-space map, FileCore vs old-map; HDD + 800K E-format | OpenAcornExplorer, Linux `adfs`, RISC OS PRM | extract M-L; +write M; +resize (HDD) M |
| `src/fs/qdos.rs` | QL | QXL.WIN container, QDOS dir, big-endian | QPC/SMSQ docs, `qltools` | extract M; +write M; +resize (HDD) M |
| `src/fs/ataridos.rs` | Atari800 | VTOC@360, dir 361-368, DOS 2.0/2.5, SD/ED/DD | `atari-tools` (C), atrfs spec | extract S-M; +write S-M |
| `src/fs/os9.rs` (RBF) | CoCo3 | LSN0 ident + LSN1 bitmap, 256 B, Unix-like | OS-9 RBF tech manual, NitrOS-9 | extract M; +write M |
| `src/fs/rsdos.rs` (Disk BASIC/DragonDOS) | CoCo2/3 | granule table on track 17 ("FAT" in name only) | Sub-Etha CoCo docs | extract S-M; +write S-M |
| `src/fs/trsdos.rs` | TRS-80 | TRSDOS/LDOS/NEWDOS variants on JV1 | Tim Mann dskspec | extract M; write deferred |
| `src/fs/sedoric.rs` | Oric | Sedoric dir; EDSK container | Oric emulator docs | extract S-M; write deferred |
| `src/fs/n88basic.rs` | PC88 | N88-BASIC Disk BASIC on `.d88` | PC-88 emulator docs | extract M; write deferred |
| `src/fs/sharpmz.rs` | SharpMZ | per-machine FD format | Sharp MZ docs | extract S-M; write deferred |
| `src/fs/eos.rs` | ColecoAdam | block dir @ block 1 (magic 55 AA 00 FF), contiguous | Just Solve EOS | extract S-M (core is RO; no write needed) |
| `src/fs/ti99.rs` | TI-99_4A | VIB@0 + FDIR 1-2 | TI disk docs | extract M; write deferred |
| `src/fs/andos.rs`, `microdos.rs`, `specialist.rs` | BK0011M, Vector-06C, Specialist | Soviet formats; niche | emulator sources | extract M each; write/resize deferred (low priority) |
| `src/fs/mfs.rs` | MacPlus 400K | flat MFS (pre-HFS) | IM:F, our HFS code | extract S (write deferred) |

### Group D — already-supported confirmations (no work, sequencing only)

ao486, PCXT, MSX/MSX1, ZXNext, TSConf, Minimig-AGA, plus the FAT/HFS/ProDOS
paths of MacPlus / AtariST / Apple-II / ZX-Spectrum.

---

## 5. Per-core delivery roadmap

**The delivery unit is a core, not a filesystem.** A core is "done" when every
medium it supports has full file manipulation (extract milestone → add/delete),
and HDD media additionally support resize-on-restore. Within a core, extract
lands before the write path; a core isn't done until its write path is verified
(§6).

**Ordering rule (your call): cores that need both floppy *and* hard-disk file
support come first**, because that's where the tool delivers both capabilities
at once. Each core below lists the building blocks it pulls from §2–§4 — FS
module(s), container decoder(s) (§3.2), partition table (§3.3), and whether HDD
resize applies.

Shared infrastructure (§3.1 non-512 sectors, the §3.2 container framework —
ported decoders, no crate — the §3.3 partitionless-dispatch framework, §3.4
conventions) is **built the first time a wave needs it**, not as a big up-front
phase — it rides in with the first core that requires each piece.

### Wave 1 — finish near-complete dual-media cores (cheap both-media wins)

These already support one medium; a small piece completes the other, so the
core flips to full floppy+HDD support fast.

| Core | Floppy | HDD | Building blocks still needed | Effort |
|---|---|---|---|---|
| **AtariST** | FAT12 (`.st` done) | FAT16 (done) | `.msa` container decoder (floppy); **AHDI partition table** (HDD); then HDD resize works via existing FAT resize | S + S |
| **MacPlus** | HFS 800K (done) | HFS (done) | **MFS** for 400K floppy (extract) | S |
| **Apple-II** | DOS 3.3 (new) | ProDOS (done) | **DOS 3.3** FS (port a2kit) + sector-order (`.do`/`.po`) container | S-M |

### Wave 2 — new dual-media cores

Each needs its filesystem built from scratch (or ported) plus its
container/partition pieces, but delivers floppy **and** HDD together.

| Core | Media + FS | Building blocks | HDD resize | Effort |
|---|---|---|---|---|
| **X68000** | Floppy + SASI HDD, Human68k | `human68k.rs` (A3, reuses FAT); `.d88`/`.dim`/`.xdf` container; X68k SASI partition scheme | Yes (FAT machinery) | M |
| **Archie** | 800K floppy + HDD, ADFS/FileCore | `adfs.rs` (ground-up); `.adf` 800K + `.hdf` (strip Arculator header) | Yes | M-L |
| **QL** | Microdrive + QXL.WIN HDD, QDOS | `qdos.rs` (ground-up); `.mdv` (QLAY) + `QXL.WIN` containers | Yes | M |
| **Altair8800** | Floppy + CF/IDE, CP/M | `cpm.rs` + DPB registry (B3); `.dsk` + raw CF | Yes (CF/IDE) | L |
| **BK0011M** | Floppy + `.vhd` HDD, ANDOS | `andos.rs` (ground-up, niche) | Stretch | M |

**Spillover:** building CP/M for Altair (Wave 2) immediately unlocks the
floppy-only CP/M cores in Wave 3 (Amstrad, PCW, Einstein, SVI328, MultiComp,
ZX +3DOS) — they just need a DPB entry + EDSK container, not a new FS.

### Wave 3 — single-medium (floppy-only) cores

No HDD, so file manipulation only (no resize). Sequenced by leverage.

| Core(s) | FS | Building blocks | Effort |
|---|---|---|---|
| C64, C128, C16, VIC20, PET | CBM DOS | port `cbm` → `cbm.rs`; `.d64/.d71/.d81` raw + `.g64/.g71` GCR container; PET adds `.d80/.d82` geometry | M |
| Amstrad, AmstradPCW, TatungEinstein, SVI328, MultiComp, ZX+3 | CP/M | **free from Wave 2** — DPB entries + EDSK container | S each |
| Atari800 | Atari DOS | `ataridos.rs` | S-M |
| CoCo2, CoCo3 | RS-DOS/DragonDOS + OS-9 RBF | `rsdos.rs`, `os9.rs`; `.jvc/.vdk` container | M |
| Oric | Sedoric | `sedoric.rs`; EDSK container | S-M |
| PC88 | N88-BASIC | `n88basic.rs`; `.d88` container (shared w/ X68000) | M |
| TRS-80 | TRSDOS/LDOS | `trsdos.rs` | M |
| SharpMZ | Sharp MZ FD | `sharpmz.rs` | S-M |
| TI-99_4A | TI-99 FS | `ti99.rs` | M |
| ColecoAdam | EOS | `eos.rs` (extract; core is RO) | S-M |
| Vector-06C, Specialist | MicroDOS, Specialist-MX | `microdos.rs`, `specialist.rs`; `.fdd/.edd`, `.odi` containers | M each (last) |

---

## 6. Testing & multi-platform validation

**Every format must be verified end-to-end before it's considered done** — no
format ships on unit tests alone. Verification is a shared activity: automated
tests catch regressions, but each format's read *and* write paths get confirmed
against an external reference and, where it matters, a real emulator/core run
(done together — see "Verification workflow" below).

- **Engine unit tests** inline per `src/fs/*.rs` (`#[cfg(test)]`), built from
  small real-image fixtures checked into `tests/fixtures/` (keep them tiny —
  floppy images are ≤ 1 MB). Assert detect → list → extract → checksum
  round-trips, the write-then-reread round-trip, and the compact-reader size
  invariant where applicable.
- **Container round-trips:** decode wrapper → flat sectors → re-detect FS.
- **Cross-platform CI:** the existing GitHub Actions matrix (Win x86/x64,
  macOS arm64/x64, Linux) must stay green. Because everything new is **ported
  or hand-written pure Rust with zero new dependencies** (§2.4), there is no
  new-dependency CI risk — the code compiles with the toolchain we already use.
- **External validators** (the cross-check that our bytes are *correct*, not
  just self-consistent): CiderPress2 for Apple, `c1541`/VICE for CBM, `cpmtools`
  for CP/M, `atari-tools` for Atari DOS, OpenAcornExplorer for ADFS, `qltools`
  for QDOS, real emulators (and MiSTer cores) for the rest. A write is not
  trusted until a reference tool / emulator reads it back cleanly.

### Verification workflow (per format)

1. **Read/extract:** open a real image, list + extract files, diff extracted
   bytes against the same files pulled by the reference tool.
2. **Write/add+delete:** add and delete a file, then (a) re-read with our own
   code, (b) open the mutated image in the reference tool, and (c) where the
   format drives a MiSTer core, boot it in the emulator/core to confirm the OS
   still mounts the disk and sees the change. **This step is done together** —
   the author wires up the test image and reference output; emulator/core
   confirmation is a joint check before the write path is enabled by default.
3. **Resize (HDD formats):** restore to a larger and a smaller target, then
   verify the volume mounts and all files survive in both the reference tool
   and the emulator.

- **No raw-device tests required** — these are file-image formats.

---

## 7. Per-PR checklist (from CONTRIBUTING)

Every slice/commit must satisfy:

1. `cargo build --all-targets` → **zero warnings**.
2. `cargo test --lib` green (add tests with the FS).
3. `cargo fmt` (pre-commit hook; no `--no-verify`).
4. **No Unicode glyphs** in any user-visible string (log lines, GUI, CLI).
5. New FS code is in `src/fs/`, dispatched only through `src/fs/mod.rs`
   helpers — no type-byte/hint `match` ladders elsewhere.
6. Background work (compaction, fsck, resize) uses the callback-at-leaf +
   Status-at-runner pattern (`docs/progress_pattern.md`), never blocks the egui
   frame, and honors `cancel_check`.
7. `Err(FilesystemError::Unsupported)` for unimplemented trait methods — never
   silent no-ops.
8. **GUI / CLI parity:** every scriptable operation (inspect, browse-extract,
   backup, restore, and resize where applicable) is exposed in `rb-cli` per
   `docs/cli-todo.md`. Shared logic lives in the engine/model layer, called by
   both.
9. Ported code (from `cbm`, `a2kit`, `fluxfox` — all MIT / MIT-OR-Apache)
   carries a source + license attribution header. No new crate is added (§2.4),
   so there is no NOTICE entry to make unless we later opt to keep `binrw`.
10. On-disk structs preserve every field for faithful round-trip writes
    (CONTRIBUTING "On-disk format struct fidelity").

---

## 8. Risks & open questions

### Resolved by the product framing (§0)

- **Is floppy-only in scope?** Yes — via Axis 1. The OS-can't-mount-it angle
  makes file extract/add the differentiator even with no resize.
- **Read-only vs editable bar?** **Full support per format** — every format
  targets Add/Delete; extract is just the first implementation milestone, not
  the ship bar. Resize is HDD-only; fsck is opportunistic.
- **How conservative on the write path?** We don't refuse-to-write; instead
  **every format's write path is verified before it's enabled by default** —
  reference-tool read-back plus emulator/core confirmation, done together (§6
  "Verification workflow"). All formats get verified, read and write.

### Verified 2026-05-31 (was "open — technical")

- **fluxfox importability** — ✗ **ruled out by build test.** crates.io v0.1.0
  is stale + fails `--no-default-features` + has a future-incompat dep; git HEAD
  fails lean and only builds with a ~187-package default tree (rhai/tar/
  zstd-sys/wasm). Decision flipped to **port decoders from source** (§2.1).
- **a2kit CP/M DPB flexibility** — ✓ **flexible.** `src/bios/dpb.rs` has a fully
  parameterized `DiskParameterBlock` + a heuristic preset table; matches our
  DPB-registry design. B3 = port + add MiSTer-core presets (§2.3).
- **a2kit module layout** — ✓ modular (`dos3x`/`cpm`/`prodos`/`pascal`/`fat`),
  but each leans on `a2kit_macro` + internal `img`/`Block` abstractions → port
  = reimplement against our trait using a2kit as reference (§2.3).
- **AHDI partition table** — ✓ **confirmed** (DrCoolZic *Atari HD File System
  Reference Guide*): LBA-0 root sector = boot code + 4 big-endian slots; types
  GEM / BGM / XGM; XGM = extended linked-list of further root sectors. A2 plan
  and S effort stand.
- **MSA format** — ✓ **confirmed/fully spec'd**: header (`$0E0F` id, SPT, sides,
  start/end track), per-track `[len:u16][data]`, RLE with `$E5` marker
  (`$E5 <byte> <runlen:u16>`, literal `$E5` as `$E5 E5 0001`). Small decoder, S.

### Still open — technical, I can verify

- **fluxfox `.d88` source** — does fluxfox's source include a `.d88` decoder to
  port, or do we hand-write it? Affects PC88 / X68000 (§3.2 / A3, B4). Small
  either way.
- **Sector-size assumptions** — audit `backup/sizes.rs` and any 512-hardcoded
  arithmetic before Wave 1 / Wave 3; most paths take the FS's own geometry but
  confirm.

### Still open — design, my call but worth a sanity check

- **`src/rbformats/containers/` shape** — `Box<dyn ReadSeek + Send>` wrapper
  alongside `ChdReader`; confirm against CONTRIBUTING's layer rules.
- **`.adf` / `.dsk` extension collisions** — dispatch must be
  content-magic-first (§3.3). Risk: bare sector dumps (CBM `.d64`, Atari `.atr`
  body) have weak/no magic, so disambiguation may fall back to exact image
  size. Real correctness risk; needs a per-format signature table.
- **Shift-JIS for Human68k** — prefer a static table over adding `encoding_rs`.

### Standing principle

- **Scope discipline** — resist Axis-2 creep onto floppy-only cores; they get
  full file manipulation (add/delete), not resize.
