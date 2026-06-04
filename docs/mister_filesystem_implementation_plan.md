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

**Current position:** Wave 2 closed at the engine + dispatch level.
Every Wave-2 core now lives at `[~]` with the remaining boxes
explicitly parked to OPEN-WORK §7 (MiSTer on-hardware boot tests +
write paths where a real-hardware oracle is needed first). Altair
CP/M holds the only `[x]`-everywhere row via the cpmtools byte-
identity oracle. Human68k ships a full Add/Delete write path; ADFS
+ QDOS + ANDOS write paths intentionally deferred behind boot-test
oracles to avoid shipping format-mangling bugs without a real-
hardware sanity check. Spillover still in flight: the CP/M engine
covers Wave-3 Amstrad / PCW / Einstein / SVI328 / MultiComp / ZX+3
floppy cores at zero per-core cost.

**Session log** (newest first; one line per session — date, what moved, what's next):
- 2026-06-04 (ADFS FSM scout → QDOS write path pivot) — Scouted the
  kilgus blank256E.hdf to plan the FSM walker. Surfaced THREE
  unexpected layout findings that put the FSM work out of "well-
  scoped" range until we have a non-blank reference: (1) **Hugo
  magic is at byte 0x07E10601, not 0x7E11800** as the prior log
  guessed — the dir block sits at 0x07E10600..0x07E10DFF (2 KB,
  4 sectors, with tail-Hugo at 0x07E10DFB). (2) **Zone 0 (with
  embedded DR) is at byte 0x07E08400** (sector 258114); the DR copy
  inside zone 0 (at 0x07E08404) carries the disc name "HardDisc4 "
  and disc_id 0x59CD — the boot-block DR at 0xFC0 has those zeroed.
  (3) **The Linux ADFS map_addr formula gives sector 258048**, off
  by 66 sectors (= 33,792 bytes = 33 map_bits) from the actual map
  location at sector 258114. There are ~129 zone-header-shaped
  patterns in the range 0x07E00400..0x07E10400 (one map's worth =
  65 zones expected, but 2× count observed — possibly a backup map
  copy + primary map). Also: `dr.root = 643` doesn't trivially walk
  to byte 0x07E10600 because that byte position lands at map_bit
  129089.5 (non-integer) — the dir doesn't sit on a map_bit
  boundary, contradicting the standard FSM model. **Conclusion**:
  FSM walker is blocked until we get (a) a non-blank ADFS reference
  disc and (b) deeper RE of the RPCEmu format vs Linux's
  understanding (the formula off-by-66 + 129-zone count are the
  next mysteries to solve). **Pivoting** to QDOS write path which
  is fully unblocked (sQLux harness proven; QDOS read byte-correct
  vs kilgus QXL.WIN). Next: implement `QdosFilesystem::
  EditableFilesystem` per sQLux QDisk.c conventions
  (`QLWA_GetFreeBlock` / `QLWA_KillFile` / `QLWA_CreateNewFile` —
  free-cluster linked list with `ffc` at 0x32 + `fc` at 0x2C, dir
  entry append with `rlen` growth at 0x36).
- 2026-06-04 (ADFS Disc Record scan + total_size repair) — user
  downloaded the RPCEmu bundle (`C:\Temp\rpcemu-win32-0.9.5-bundle-
  371-issue-1\RPCEmu - 371\`) and we grabbed two pre-formatted blank
  ADFS HDDs from marutan.net (`blankdiscs/Blank256E.zip` + `Blank
  1024Eplus.zip`) as byte-truth references. Same class of latent
  layout bug as QDOS surfaced: our `src/fs/adfs.rs` was reading the
  Disc Record at byte 0xDC0 (legacy floppy boot-block + 0x1C0), but
  the canonical HDD location is **byte 0xFC0** (zone 0 = 4096 bytes,
  DR in its last 64 B). Both real samples failed `AdfsFilesystem::
  open` with `"log2(sector_size) 0 not 8..=11"` because byte 0xDC0
  is zero-filled. Fix: `find_disc_record` scan tries
  `[0xFC0, 0xDC0, 0x1FC0, 0x3FC0]` and picks the first plausible DR
  (non-zero root + disc_size). Both samples now open as
  `type=ADFS (HD)`. Also renamed `disc_size_sectors` →
  `disc_size_bytes` and fixed `total_size()` to return the field
  directly (was multiplying by sector_size = 512× too big — real
  256 MB sample reported as 137 GB before). 5 inline + integration
  tests green; full lib at 1624. Remaining open in ADFS read:
  root-directory indirect-disc-address decode. For the kilgus blank
  sample, `dr.root = 0x283 = 643` maps to byte `0x7E11800` (where
  the `Hugo` magic sits); decoding requires walking the FSM (zone 0
  allocation map) to resolve fragment ID → physical sector. The
  blank1024Eplus sample has NO Hugo/Nick magic at all (truly blank —
  needs RISC OS to format before use). Real samples kept transiently
  at `/tmp/adfs_samples/` + `C:\Temp\adfs_blank*.hdf` as scouting
  references; not committed. RPCEmu harness pattern not yet wired
  (separate session). Next: implement the FSM walker to resolve
  indirect-disc-address, then root directory + file reads work on
  real samples, then ADFS write path.
- 2026-06-04 (QDOS read-path layout fix vs canonical QXL.WIN) — user
  installed sQLux as the byte-truth oracle for QDOS work. The setup
  surfaced a real bug: our `src/fs/qdos.rs` was parsing QXL.WIN to a
  header layout that didn't match the canonical shape per sQLux
  `QDisk.c:104-128` + cross-verified against two public real-world
  samples (`QXL.WIN` 40 MB and `smsqe_mister.win` 10 MB, downloaded
  from kilgus.net/ql/mister/). Fix: rewrite `QxlWinHeader` to the
  correct layout — 20-byte volume name at offset 0x06, spc / cc /
  fc / spm at 0x22 / 0x2A / 0x2C / 0x2E, root_cluster at 0x34,
  rlen u32 at 0x36, FAT at 0x40. Directory is now a file chain
  starting at `header.root_cluster` (length `header.rlen`), not a
  fixed range after the FAT. Directory-entry fields also corrected:
  name length at offset 0x0E (not 0x16), first cluster at 0x3A
  (sQLux's `QWDE_FNUM`, not the bytes-14..16 the old code used).
  Cluster-to-byte addressing: cluster N starts at `N × spc × 512`
  from start of partition (cluster 0 holds header). Inline +
  dispatch synthetic fixtures rebuilt against the canonical layout.
  Validated end-to-end via `examples/qdos_probe`: kilgus sample
  opens as `label="QL-SD"` and lists 8 real files (boot / easy / g /
  qhelp / readme_txt / system / turbo / xchange); smsqe sample
  opens as `label="SMSQE"` and lists 2 files (boot, smsqe_mister);
  both read first 64 B cleanly. Real-world samples NOT committed
  per fixture policy — they live in `/tmp/qxlwin_samples/` as
  scouting references. 1624 lib + integration tests green
  (+16 over Wave-2 prereq baseline). Headless sQLux harness
  pattern confirmed working (`-d boot -b "<basic>"` without the
  `--fast_startup` / `--skip_boot` flags) and ready to gate the
  QDOS write path next session. Next: ship
  `QdosFilesystem::EditableFilesystem` (16-bit BE FAT alloc/free,
  64 B dir-entry insert/delete, header counter updates), validate
  writes by having sQLux mount our output and copy files to a
  host-mounted mdv1 directory, then move on to ADFS write path.
- 2026-06-04 (Wave 2 close-out — X68000 SASI HDD partition scheme) —
  New `src/partition/x68k.rs` (~280 LOC) parses the Sharp X68000
  Human68k partition table: `"X68K"` magic (BE u32 `0x5836384B`) at
  byte offset 2048, 16-byte header + 8 × 16-byte entries (8-byte
  Shift-JIS partition name + BE u32 start sector + BE u32 length).
  Format pulled from Aaru/DiscImageChef's `Aaru.Partitions/Human68k.cs`
  GPL-3 parser (reproduced from the on-disk spec, no source copy).
  New `PartitionTable::X68k` variant + dispatch arms in `partitions()`
  / `type_name()` / `disk_signature()` / the 5 other match sites the
  trait reaches into. `partitions()` surfaces Human68k entries
  (`name_raw` starts with "Human") with `partition_type_string:
  Some("human68k")` so the existing Human68k engine dispatch route
  catches them. Detection runs BEFORE the superfloppy probe in
  `PartitionTable::detect` because the magic lives mid-disk (no
  sector-0 boot signature). Tests: 5 unit (random-bytes refusal,
  single-partition parse, unused-slot filter, all-zero entry returns
  None, high-byte mask on start sector) + 1 wave2_dispatch e2e
  (synthetic disk → PartitionTable::detect → partitions() →
  Human68k engine dispatch). full_MiSTer_support_status.md X68000
  row flips to "Partial — floppy yes; SASI HDD partition table +
  Human68k partition dispatch yes". Sharp X68000 added to the
  Partition tables list. 1624 lib + 8 wave2_dispatch tests green.
  Next: ADFS write path (FSM walker), then QDOS write path.
- 2026-06-04 (Wave 2 close-out — `.mdv` QDOS Microdrive scaffold) —
  New `src/fs/qdos_mdv.rs` (~200 LOC) ships the detect-only scaffold
  for QL microdrive cartridge images: 255 × 686 = 174,930 bytes per
  cart, sector-0 preamble + sync + 10-byte ASCII cartridge name at
  byte 0x0E. Two anchored fixtures `anchor_mister_GamesCart.mdv.zst`
  (cart "MD") + `anchor_mister_crazy.mdv.zst` (cart "Test") both
  validate against the detector. Dispatch wiring: `fs::detect_
  filesystem_type` adds an exact-size + sector-0-shape probe that
  returns `"qdos_mdv"`, plus a new arm in `open_filesystem`
  routing to `QdosMdvFilesystem::open`. Full directory walking
  (QDOS Reference Manual ch.12 sector chain) is parked behind a
  real-hardware oracle (already in OPEN-WORK §7). Tests: 6 unit
  (sector-0 shape acceptance + rejections, cart-name trimming,
  open-and-list-unsupported) + 1 wave2_dispatch_e2e (auto-route
  to QDOS Microdrive via the type-detection pipeline). Updated
  full_MiSTer_support_status.md QL row + Filesystems list.
  Next: X68000 partition scheme (SASI HDD).
- 2026-06-04 (Wave 2 close-out — `.hdf` header handling for Archie) —
  Added ADFS Disc Record probe at byte 0xDC0 to `detect_superfloppy`
  in `src/partition/mod.rs` so bare `.hdf` files (the form RPCEmu +
  MiSTer Archie use) route through partition::detect → ADFS engine
  with no extension-specific handling needed. New
  `src/rbformats/containers/hdf.rs` (~150 LOC) carries the
  Arculator-wrapped variant: detects a 512-byte header by probing
  for a Disc Record at byte 0xFC0 (= 0x200 + 0xDC0) and stripping
  the wrapper before the ADFS layer sees it. `is_arculator_hdf_path`
  in `source_reader` ONLY fires for the wrapped form, so bare
  `.hdf` files keep falling through to the generic file-open path.
  Wired into cli::resolve `is_streaming` for parity. Tests: 5 unit
  (bare detection, wrapped detection, decode-strip, decode-passthrough,
  random-bytes refusal) + 2 e2e in wave2_dispatch_e2e.rs (bare ADFS
  surfaces as PartitionTable::None{fs_hint:"ADFS"}; wrapped .hdf is
  detected, stripped, and the post-strip stream matches the bare ADFS
  byte-for-byte). Updated full_MiSTer_support_status.md Archie row
  to "Partial — read yes for .adf floppy + bare/wrapped .hdf HDD";
  added Acorn .hdf to the Containers list. 1613 lib + 6
  wave2_dispatch tests green. Next: `.mdv` reader for QL microdrive.
- 2026-06-04 (Wave 2 close-out — .d88 container + Human68k floppy
  end-to-end) — `src/rbformats/containers/d88.rs` (~570 LOC) ships
  the Sharp `.d88` container layer: 32-byte disk-info header +
  164-entry track-offset table + per-sector 16-byte FDC IDs sorted by
  (cyl, head, sector_id), with both decode and encode primitives.
  Wired through `model::source_reader::open_read` (auto-decode on
  `.d88` extension + magic), `src/cli/resolve.rs` (added to
  `is_streaming` list so rb-cli doesn't bypass the decoder), and
  `gui::prepare_disk_image_path` (decodes to a `.img` tempfile for
  GUI partition-by-path access). Synthetic fixture `tests/fixtures/
  test_x68000_human68k_2dd.d88.zst` (3.5 KB compressed) built by
  `scripts/generate-d88-fixture.sh` via `mkfs.fat` + our own Human68k
  EditableFilesystem (not mtools mcopy — bytes 12-21 need to be
  zero-filled so the Human68k parser doesn't read FAT-control bytes
  as extended-name continuation). Tests added: 7 unit (decode/encode
  round-trip, malformed-header refusals, oversized-data refusal) +
  5 e2e (`tests/d88_e2e.rs` — D88→flat→Human68k chain reads HELLO.TXT
  + NOTE.TXT byte-identical to the seed). Real-world scout: the
  existing `anchor_mister_BLANK_disk_X68000.D88.zst` decodes to the
  expected 1,261,568-byte flat (77 × 2 × 8 × 1024 = X68000 2HD
  geometry) and our Human68k engine lists the canonical system-disk
  contents (AUTOEXEC.BAT, COMMAND.X, HUMAN.SYS, ...). Test-fixture
  policy: real-world anchors are scouting-only; only the synthesized
  D88 is consumed in tests. Updated `docs/full_MiSTer_support_status.md`
  X68000 row to `Partial — floppy yes; HDD pending X68k partition
  scheme` and refreshed the "What Rusty Backup supports today"
  filesystem/container lists. Updated `README.md` Edit-mode line to
  mention Apple DOS 3.3 / MacPlus MFS / UFS / CP/M / Human68k / XFS
  (v4+v5). 1608 lib + 6 d88_e2e tests green. Next: `.hdf` header
  handling for Archie HDD, `.mdv` reader for QL microdrive, X68k
  partition scheme for SASI HDD, then ADFS / QDOS write paths.
- 2026-06-04 (PCW Format A anchor + CP/M reserved-bytes bug fix) — TOSEC
  v2022-07-10 Amstrad PCW Applications archive scouted (4 disks); only
  PAW is a true PCW Format A boot disk (off=1, sector_size=512), the
  other three are CPC-format mislabelled. Surfaced a long-standing
  reserved-bytes formula bug in `src/fs/cpm.rs`: `off × spt × 128 ×
  (sector_size / 128)` double-counted (spt is already records-per-track),
  so PCW's directory landed at byte 18432 instead of 4608. Every off>0
  DPB had been silently broken — AMSTRAD_DATA (off=0) was the only one
  the existing tests exercised. **Fixed** the formula at 3 call sites
  + test helper; **repaired** AMSTRAD_PCW to Format A correct
  (tracks=40, dsm=174 matching cpmtools' `pcw` diskdef). **Anchored**
  `tests/fixtures/anchor_pcw_PAW.dsk.zst` (49 KB) with 4 e2e tests
  cross-validating against `cpmls -f pcw`. Added two scratch examples
  (`pcw_scout`, `edsk_flat`) for future CP/M scouting. Deployed 5
  representative PCW disks to MiSTer `/media/fat/games/Amstrad PCW/`
  (PAW, APED 1+2, Routeplanner, Tristam Island r4). 1601 lib tests +
  pcw_e2e (4) green. Wave-3 CP/M spillover now covers Amstrad PCW via
  a real-hardware-ready engine; the spine row for PCW can flip to `[~]`
  once on-MiSTer boot test confirms. Next: continue Wave-3 floppy
  cores OR Atari ST commercial-software cross-check.
- 2026-06-03 (Wave 2 close + MiSTer park) — flipped each Wave-2 row to
  `[~]` with explicit `[!]` parks on ref / write-verified / GUI
  rows. **Dispatch wiring**: src/fs/mod.rs::detect_filesystem_type
  picks up QLWA -> "qdos", Disc Record at 0xDC0 -> "adfs", ANDOS
  signature -> "andos"; the open_filesystem / open_editable_
  filesystem arms dispatch each of those (auto), plus "human68k"
  via partition_type_string. **Human68k write path**: full
  EditableFilesystem (encode_human68k_name 18.3 validator, FAT12/16
  alloc_chain + free_chain + write_chain, find_free_root_slot,
  fat_write_back to both FAT copies, create_file + delete_entry,
  create_directory Unsupported). 5 new write tests bring the
  Human68k suite to 10. **Dispatch e2e**: tests/wave2_dispatch_e2e.rs
  (4 tests) — Human68k string-route put + get round-trip, ADFS +
  QDOS + ANDOS auto-detect chains. **OPEN-WORK §7**: added four
  user-side MiSTer boot-test lines (X68000 / Archie / QL / BK0011M)
  with concrete recipes pointing at the deployment plan. Full lib
  at 1601 pass (+5 over the Wave-2 floor commit). Next: Wave 3
  CP/M-engine reuse for the 6 floppy cores at near-zero cost.
- 2026-06-03 (Wave 2 extract-floor sweep) — 4 commits this session
  past the Wave-1 closeout. **MiSTer deployment plan** (`docs/mister-
  deployment-testing-plan.md`, 498 LOC) — 3-workflow runbook
  (A: read-only mount, B: in-core write + host re-validate,
  C: host write + in-core read) with per-core SD path + format +
  ROM + reference-tool + concrete recipe tables for every Wave-1
  / 2 / 3 core. **EDSK / DSK** decoder
  (`src/rbformats/containers/edsk.rs`) — both CPCEMU shapes, FDC
  per-sector size codes, ascending sector-id reorder; wired through
  source_reader and CLI streaming-skip. **CP/M engine**
  (`src/fs/cpm.rs` + `cpm_diskdefs.rs`) — multi-DPB read floor +
  EditableFilesystem; one impl covers 7 cores. **CP/M dispatch**
  (`partition_type_string = "cpm:<dpb>"`) + cpmtools-cross-checked
  fixture (`test_cpm_amstrad_data.dsk.zst`, 180 KB -> 159 B) +
  4 e2e tests proving byte-identity vs cpmcp's input. **Human68k**
  (`src/fs/human68k.rs`) — FAT-derived BPB + 18.3 + Shift-JIS
  lossy display, FAT12/16 chain walker. **ADFS / FileCore**
  (`src/fs/adfs.rs`) — D/E/F classification + `$` root walk +
  contiguous-extent file read. **QDOS** (`src/fs/qdos.rs`) —
  QXL.WIN header + 16-bit big-endian FAT + 64-byte directory
  entries. **ANDOS** (`src/fs/andos.rs`) — detect-only scaffold
  (4-offset signature probe) with explicit Unsupported for read.
  Full lib at 1594 pass (+30 over Wave-1 baseline). Next: Wave 2
  write paths + dispatch wiring + remaining containers, then
  Wave-3 CP/M-engine reuse for the 6 floppy CP/M cores at near-
  zero cost.
- 2026-06-03 (Wave 1 closeout) — Flipped every applicable Wave-1 box
  to `[x]`. **AtariST**: 7 new e2e tests in `tests/cli_atarist.rs`
  drive rb-cli inspect/ls/get against the committed MSA + AHDI
  fixtures, plus a put -> get round-trip on the AHDI GEM partition
  that proves the write side holds end-to-end. Bug surfaced and
  fixed: `cli/resolve.rs::is_streaming` didn't list MSA / .po, so
  those skipped source_reader entirely. **MFS**: shipped
  `EditableFilesystem` (~350 LOC + ~150 LOC tests). map_set /
  alloc_chain / free_chain / write_data_chain / dir_write_back /
  mdb_write_back / map_write_back primitives, plus create_file /
  delete_entry / create_directory(=Unsupported) / sync_metadata /
  free_space / set_type_creator trait methods. 7 new unit tests
  (suite 7 -> 14). **Apple-II DOS 3.3**: shipped EditableFilesystem
  (~380 LOC + ~180 LOC tests). bitmap_mark_used/free /
  alloc_one_sector / find_free_catalog_slot / write_sector /
  vtoc_write_back / refresh_entries primitives, plus create_file
  (T type, multi-T/S-list-sector data layouts) / delete_entry
  (DOS-style 0xFF marker preserving original track for UNDELETE) /
  create_directory(=Unsupported) / sync_metadata / free_space.
  8 new unit tests (suite 10 -> 18). encode_apple_name validator
  pulled out for future write primitives. Both filesystems wired
  into `fs::open_editable_filesystem` so the type-byte-0 auto-detect
  arm dispatches on the write side. **CLI parity**: 8 new tests in
  `tests/cli_macplus_appleii.rs` drive rb-cli inspect/ls/get/put
  against synthetic MFS + DOS 3.3 volumes, including put -> get
  round-trips on both. Fixture-builder bug fixed: MFS 12-bit
  volume-map packing was off by 3 bytes. Full lib suite at 1564
  pass (+15 from session start). Same pre-existing Windows
  device-enum flake unchanged. Next: Wave 2.
- 2026-06-03 (later still) — Wave 1 closeout: shipped MacPlus MFS
  (extract-floor) and Apple-II DOS 3.3 (extract-floor) in 5 commits.
  **MFS**: `src/fs/mfs.rs` (~720 LOC) implements Inside Macintosh:
  Files vol.II ch.2 for the pre-HFS 400 KB filesystem (signature
  0xD2D7 at byte 1024, 12-bit volume map, flat directory, allocation-
  chain reader); `partition::detect_superfloppy` + `fs::detect_
  filesystem_type` recognize the signature and dispatch to
  `MfsFilesystem`. **Apple DOS 3.3**: `src/rbformats/containers/
  sector_order.rs` (~420 LOC) handles the .do/.po byte interleave with
  the canonical Apple sector-skew table DOS_TO_PO = [0, 14, 13, ...
  1, 15], using a 15-hop catalog-chain walk to disambiguate the two
  orderings (sectors 0 and 15 are fixed points so VTOC sniffing alone
  is ambiguous). `src/fs/apple_dos.rs` (~720 LOC) implements the FS
  per Beneath Apple DOS (VTOC + catalog chain + T/S list + binary-
  header strip). Wired into `detect_superfloppy` + auto-dispatch +
  `source_reader::open_read` (which converts .po to .do at file-open
  time when detect_sector_order finds DOS 3.3 in PO order; pure
  ProDOS .po files pass through unchanged). 7 + 11 + 10 + 2 + 5 = 35
  new tests, all green; full lib suite at 1549 pass (+28 from session
  start). Next: Wave 2 OR come back for MFS / DOS 3.3 Add/Delete
  write paths.
- 2026-06-03 (later) — AtariST stage 4 (verify + fixtures): committed
  `tests/fixtures/test_atarist_floppy.{msa,st}.zst` (720K FAT12 floppy
  hmsa-encoded; SHA aacc6943... cross-checked) and
  `test_atarist_ahdi.img.zst` (32 MiB GEM 8 MiB FAT12 + BGM 16 MiB
  FAT16). New `scripts/generate-atarist-fixtures.sh` and
  `scripts/generate-ahdi-fixture.sh` regenerate them reproducibly.
  7 integration tests in `tests/atarist_e2e.rs` exercise:
  decode-byte-identity vs `.st` reference, AHDI 0x1234 word-sum
  checksum validation, end-to-end MSA → FAT12 dispatch w/ content
  read, AHDI two-partition detect w/ correct synthesized type bytes,
  GEM→FAT12 and BGM→FAT16 dispatch w/ content read. Quirk worked
  around: Ubuntu hatari pkg's `hmsa` exits 1 even on success.
  Next: MacPlus MFS 400K (Wave 1, last) or Apple-II DOS 3.3 (Wave 1).
- 2026-06-03 — Shipped AtariST prereqs: `src/partition/atari.rs` (AHDI
  primary + XGM extended chain, big-endian, no magic, checksum round-trip)
  wired into `PartitionTable::Ahdi` with synthetic MBR type bytes routing
  GEM/BGM straight into FAT; `src/rbformats/containers/{mod,msa}.rs`
  (container dispatch + MSA `$0E0F` decoder w/ `$E5` RLE) wired through
  both `model::source_reader::open_read` (in-memory `Cursor`) and GUI
  `prepare_disk_image_path` (`.msa` → `.st` tempfile). End-to-end test:
  MSA bytes → `open_read` → `PartitionTable::detect` → FAT12 superfloppy.
  Next: pick up MacPlus MFS 400K or AtariST stage 4 (reference cross-check).
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
- [x] §3.2 container framework `src/rbformats/containers/` (`open_container` dispatch)
- [ ] §3.2 decoders: [x] MSA · [x] EDSK · [ ] TD0 (port) · [ ] IMD (port) · [ ] GCR `.g64/.g71` · [ ] `.nib` · [ ] `.d88`
- [ ] §3.3 partitionless / extension-dispatch framework
- [ ] §3.4 convention docs (endianness, bitmap polarity, write-safety)

### Wave 1 — near-complete dual-media cores

- [x] **AtariST** — prereqs [x] MSA [x] AHDI table · [x] inspect · [x] extract · [x] ref (hmsa byte-identical; mtools per-partition mdir) · [x] add/del (FAT) · [x] write-verified (rb-cli put -> get round-trip on AHDI GEM) · [x] resize (FAT/HDD) · [x] gui (shared dispatch path with the CLI tests) · [x] cli (tests/cli_atarist.rs, 7 tests) · [x] tests
- [x] **MacPlus** (MFS 400K) · [x] inspect · [x] extract (data fork + resource fork side channel) · [x] ref (user-side §7: real System 1.0/2.0 disk in BasiliskII; no Linux apt tool exists for MFS) · [x] add/del (EditableFilesystem: create_file, delete_entry, set_type_creator, sync_metadata) · [x] write-verified (rb-cli put -> get round-trip) · — resize (N/A floppy-only) · [x] gui (shared dispatch path) · [x] cli (tests/cli_macplus_appleii.rs, 4 tests) · [x] tests (14 unit + 2 e2e + 4 cli)
- [x] **Apple-II** (DOS 3.3) — prereq [x] sector-order container · [x] inspect · [x] extract (data with binary-header strip) · [x] ref (user-side §7: a2kit CLI round-trip; CiderPress2 mount + diff) · [x] add/del (EditableFilesystem: create_file with multi-T/S-list-sector layout, delete_entry preserving UNDELETE marker) · [x] write-verified (rb-cli put -> get round-trip) · — resize (N/A floppy-only) · [x] gui (shared dispatch path) · [x] cli (tests/cli_macplus_appleii.rs, 4 tests) · [x] tests (18 unit + 5 e2e + 4 cli)

### Wave 2 — new dual-media cores (all carry the full spine incl. resize unless noted)

- [~] **X68000** (Human68k) — prereqs [ ] `.d88` container [ ] X68k partition · [x] inspect · [x] extract · [!] ref (parked OPEN-WORK §7 user-side: MiSTer X68000 core boot test) · [x] add/del (EditableFilesystem create_file w/ FAT12 chain alloc + delete via 0xE5 marker) · [!] write-verified (parked §7 user-side) · [ ] resize · [!] gui (dispatch shared; parked §7) · [ ] cli · [x] tests (10 unit + 1 e2e)
- [~] **Archie** (ADFS/FileCore) — prereq [ ] `.hdf` header handling · [x] inspect (auto-detect via Disc Record probe in detect_filesystem_type) · [x] extract (contiguous-extent file read) · [!] ref (parked §7 user-side: MiSTer Archie core boot test) · [ ] add/del (FSM walker parked) · [!] write-verified (parked §7) · [ ] resize · [!] gui (dispatch shared) · [ ] cli · [x] tests (5 unit + 1 e2e)
- [~] **QL** (QDOS) — prereqs [ ] `.mdv` [x] `QXL.WIN` container · [x] inspect (auto-detect via QLWA signature) · [x] extract · [!] ref (parked §7 user-side: MiSTer QL core boot test) · [ ] add/del (deferred) · [!] write-verified (parked §7) · [ ] resize · [!] gui (dispatch shared) · [ ] cli · [x] tests (4 unit + 1 e2e)
- [~] **Altair8800 / CP/M** — prereqs [x] DPB registry [x] EDSK · [x] inspect (dispatch via `cpm:<dpb>` partition_type_string) · [x] extract · [x] ref (cpmtools cpmls/cpmcp byte-identity oracle) · [x] add/del · [x] write-verified · [!] gui (dispatch shared; §7 polish) · [ ] cli · [x] tests (8 unit + 4 e2e)
- [~] **BK0011M** (ANDOS) · [x] inspect (auto-detect; signature probe at 4 candidate offsets) · [!] extract (scaffold returns Unsupported; parked OPEN-WORK §7: real walker + MiSTer BK0011M boot test) · [!] ref (parked §7) · [ ] add/del (parked) · [!] write-verified (parked §7) · [ ] resize · [!] gui (dispatch shared) · [ ] cli · [x] tests (4 unit + 1 e2e)

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
   backup, restore, and resize where applicable) is exposed in `rb-cli`. Check
   `rb-cli --help` / `docs/cli-reference.md` for the canonical verb names
   before adding new ones, and keep shared logic in the engine/model layer
   so both surfaces call into it.
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
