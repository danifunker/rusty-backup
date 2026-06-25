# crusty-backup (cb-dos) — remaining-work checklist

The **living backlog**: the single place for *what's left*. Tick items off as they
land; each box is sized to roughly one commit (or a small handful). Ordered by the
agreed priority. Context + history live in [`cb_dos.md`](cb_dos.md) (phased plan +
progress log) and [`cb_dos_resume.md`](cb_dos_resume.md) (hand-off + the
"done & proven on qemu" table). **Resume** from `cb_dos_resume.md`; **work** from
here, and update both as items land.

Legend: `[ ]` todo · `[~]` in progress · `[x]` done · `[-]` dropped

## Now (priority order)

- [x] **Phase 5 — file-level repack/defrag (FAT, boot-aware). DONE (2026-06-25).**
  `backup /DEFRAG` reorders files contiguously inside the emitted partition → a
  defragmented `partition-N.gz` (same format, just packed). New `cbdefrag.{h,c}`.
  - [x] Walk the FAT dir tree + cluster chains (own read-only walk over the
        `cbdisk` FAT primitives; LFN + deleted-slot safe).
  - [x] Compute a packed layout; keep `IO.SYS` / `MSDOS.SYS` / `IBMBIO.COM` /
        `IBMDOS.COM` / `KERNEL.SYS` **first + contiguous** at the data-area start
        (the `SYS`-command rule) so the disk still boots.
  - [x] Rewrite FAT + dir entries (incl. `.` / `..`) + relocate clusters into the
        gzip stream; FAT32 root repointed to cluster 2 + FSInfo invalidated.
  - [x] Expose it as a `/DEFRAG` flag on `backup`. (`clone /DEFRAG` is a deferred
        follow-up — see *Later / optional*.)
  - **Done when (all met on real FreeDOS/qemu):** FAT16 imaged 25.2 MB → 1.2 MB
    and FAT32 72.7 MB → 2.7 MB; **every file byte-identical** (desktop *and*
    on-DOS cb-dos restore); a SYS'd bootable FreeDOS disk defragged + restored
    **boots** (AUTOEXEC marker written); an unclean FS (lost cluster) **declines**
    to plain compaction (no data loss).

- [x] **Phase 6 — LZ4 codec. DONE (2026-06-25).** Faster on slow CPUs; gzip
  stays the default.
  - [x] Desktop: `CompressionType::Lz4` + `src/rbformats/lz4.rs` (mirror
        `gzip.rs`, `lz4_flex` frame) + the three `compress.rs` dispatch arms;
        `--format lz4`.
  - [x] DOS: cross-build liblz4 (`deps/fetch-lz4.sh`); `cbcodec.{h,c}` wraps
        gzip (zlib) + lz4 (LZ4F); `/CODEC:LZ4` on `backup`, auto-detected on
        restore from the member extension / metadata `compression_type`. (Clone
        has no compressed artifact, so `/CODEC` doesn't apply there.)
  - **Done when (met on FreeDOS/qemu):** lz4 round-trips **all three directions**
    byte-identical (DOS↔desktop, both ways, + DOS↔DOS) and composes with
    `/DEFRAG`. Size note: on a mostly-incompressible source lz4 ran ~7% larger
    than gzip (the expected ratio-for-speed trade); the win is CPU time on a 486.

- [x] **Bug — `backup` mbr.bin corruption under stdout redirection. FIXED
  (2026-06-25).** Root cause: `setvbuf(stdout, NULL, _IONBF, 0)` — **unbuffered**
  stdout, when redirected on FreeDOS, bled its final writes into a recently-closed
  DOS file's clusters at the matching offset (the "wrote metadata.json…" banner
  landed in `mbr.bin` at byte 158). Bisected: removing `_IONBF` makes it clean.
  Fix: switch all six `cmd_*` to **`_IOLBF`** (line-buffered) — writes go out in
  buffered chunks like the clean non-redirected path, while per-line + the
  progress line's explicit `fflush` keep output prompt. Verified on FreeDOS/qemu:
  redirected `backup` (mbr.bin == source) **and** redirected `get` (extracted file
  intact) are both clean, and the non-redirected round-trip still works.

- [~] **Net 7b–7i — networked backup/restore** (the path to "both" local + net).
  **7a–7g done + qemu-verified — the core network feature is complete**: the
  backup↔restore loop is closed, resumable, file-aware/idempotent,
  boot-fingerprinted, and swap-aware. Only optional 7h/7i remain. Full
  sub-checklist + design in
  [`cb_dos_network_and_state.md`](cb_dos_network_and_state.md) §9:
  - [x] 7b chunk wire protocol  - [x] 7c block-level backup over wire
  - [x] 7d resume (journal + fingerprint)  - [x] 7e restore over wire
  - [x] 7f manifest + idempotency  - [x] 7g boot hashes + swap exclusion
  - [ ] 7h incremental backup *(opt)*  - [~] 7i level-2 swap dealloc + desktop swap parity *(desktop parity DONE; level-2 + CHD swap remain)*

- [ ] **Real-486 hardware validation.** Everything so far is qemu/emulator. Run
  the full matrix (backup / restore / clone / browse, FAT + NTFS,
  extended/logical, live progress) on a real 486 + CF card.

## Later / optional

- [x] **`clone /DEFRAG`. DONE (2026-06-25).** Wired the cbdefrag planner into
  `clone`: refactored the emit to a **sink** (backup → compressed writer, clone →
  target disk) + extracted a shared `defrag_plan_build`, then added
  `defrag_clone_fat` (relocates straight onto the target, same-size, zero-fills the
  free tail) and a `/DEFRAG` flag on `clone` (primary + logical FAT; declines →
  plain clone). Verified on FreeDOS/qemu: a fragmented FAT16 cloned `/DEFRAG`
  produces a target with files byte-identical and defragged (IO.SYS at cluster 2,
  TAIL relocated, all contiguous). Defrag + `/SIZE` resize together is still a
  future combo (defrag forces same-size).
- [x] **lz4 browse. DONE (2026-06-25).** `ls`/`get` from an `.lz4` backup. Added an
  lz4 backend to the browse engine (`cbbrowse.h` / `cmd_browse.c`): since LZ4 frames
  aren't seekable, `vol_read_at` reaches an offset by decompressing forward from the
  current position (reopening from the start on a backward seek) — O(off), like a
  gzseek rewind. `cbk_open_vol` prefers `.gz`, falls back to `.lz4`. Verified on
  FreeDOS/qemu: `ls` lists the tree and `get` extracts a root file, an LFN file, and
  a deep 300 KB multi-cluster file from a `partition-0.lz4`, all byte-identical.
- [x] **Desktop defrag parity. DONE (2026-06-25).** `rb-cli backup --defrag`
  repacks FAT partitions so each file's clusters are contiguous (boot files first),
  the desktop sibling of cb-dos `backup /DEFRAG`. Reused `CompactFatReader` (which
  already renumbers + remaps clusters) by adding a `new_defrag` mode that orders
  clusters file-by-file (tree walk, boot-pinned, lost clusters preserved) instead
  of ascending source order — same output *size*, just contiguous. Verified: a
  unit test proves a fragmented file comes out contiguous (plain packing leaves it
  fragmented) + byte-exact, and an `rb-cli backup --defrag` → restore round-trip is
  byte-identical. GUI checkbox parity is a possible follow-up (CLI ships now).
- [x] **Lazy-reader packer re-chunking. DONE (2026-06-25).** Solved without the
  feared round-trip break: backups emit `partition-N.gz` as **source-span
  multi-member** gzip (a fresh member every 4 MiB uncompressed) + a regenerable
  `partition-N.gz.idx` seek layout (`src/rbformats/gz_index.rs`), pre-cached during
  backup *and* edit. The `.cbk` packer splits the member into per-span chunks
  carrying the existing `src_offset`, and `CbkLazyReader` seeks to the covering
  chunk — O(one span) forward *and* backward. The chunk payloads concatenated are
  byte-identical to the `.gz`, so `cbk unpack` reproduces it exactly and checksums
  stay valid (the original round-trip worry). A `gz_index_matches` guard + the
  stale-`.idx` cleanup keep a mismatched layout from ever mis-splitting. Verified:
  unit tests + `rb-cli` (2-chunk `.cbk`, byte-identical unpack, edit re-chunks
  2→9, deep `get` correct) and **on FreeDOS/qemu cb-dos restores + `gzseek`-browses
  a desktop multi-member backup byte-identically** (zlib reads concatenated
  members transparently). Small partitions stay single-member (byte-identical to
  the old output, no `.idx`). Possible follow-up: use the `.idx` for fast seeks
  when browsing a *folder* backup too (today only the `.cbk` path uses it).
- [ ] **Boot-media driver profiles** — the FreeDOS floppy/CD ship today with the
  plain IDE/CF path only; add CONFIG.SYS boot-menu entries for CD-ROM / USB
  mass-storage (see `cb_dos.md` *Distribution* → "Bundled drivers").

## Dropped (out of scope, by decision)

- [-] **exFAT** as a backup source — no DOS-era OS mounts it.
- [-] **ext2/3** — those users would run a Linux build.
- [-] **CD burning on DOS** + any built-in non-TCP network transport (superseded
  by the 7b TCP path).

## Done

Phases 1–4e, live progress, the lazy `.cbk` reader, the FreeDOS floppy + CD in CI,
**Phase 5 (boot-aware FAT defrag, `backup /DEFRAG`)**, and **Phase 6 (LZ4 codec,
`--format lz4` / `/CODEC:LZ4`)** are all shipped and qemu-verified — see the
"done & proven" table in [`cb_dos_resume.md`](cb_dos_resume.md) and the progress
log in [`cb_dos.md`](cb_dos.md).
