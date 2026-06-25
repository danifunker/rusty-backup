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

- [ ] **Bug — `backup` mbr.bin corruption under stdout redirection** (low-pri).
  Redirecting `CRUSTYBK BACKUP`'s stdout to a file on the *same drive* bleeds its
  "wrote metadata.json" banner into `mbr.bin`'s boot-code area. Likely a DTA /
  FILE-buffer aliasing in `cmd_backup.c` (gotcha #3).
  - [ ] Root-cause + fix; `get` writes a DOS file too — check the same path.
  - **Done when:** `CRUSTYBK BACKUP … > C:\LOG.TXT` leaves `mbr.bin` clean.

- [ ] **Net 7b–7i — networked backup/restore** (the path to "both" local + net).
  Only **7a** (binary handshake) is done; the `.cbk` container is frozen, so this
  is mostly wire framing + resume. Full sub-checklist + design in
  [`cb_dos_network_and_state.md`](cb_dos_network_and_state.md) §9:
  - [ ] 7b chunk wire protocol  - [ ] 7c whole-folder over wire
  - [ ] 7d resume (`.idx` sidecar)  - [ ] 7e restore over wire
  - [ ] 7f manifest + idempotency  - [ ] 7g boot section + swap exclusion
  - [ ] 7h incremental backup *(opt)*  - [ ] 7i level-2 swap dealloc *(opt)*

- [ ] **Real-486 hardware validation.** Everything so far is qemu/emulator. Run
  the full matrix (backup / restore / clone / browse, FAT + NTFS,
  extended/logical, live progress) on a real 486 + CF card.

## Later / optional

- [ ] **`clone /DEFRAG`** — wire the cbdefrag planner into `clone` too (emit the
  relocated image straight to the target disk instead of gzip; share the planner
  via a write-sink). Same-size only at first (defrag + `/SIZE` resize is a later
  combo). `backup /DEFRAG` shipped; this is the remaining "and maybe clone" half.
- [ ] **Desktop defrag parity** — the desktop backup could optionally repack FAT
  partitions the same way (it already has the FAT machinery). Not in the cb-dos
  scope; a separate GUI/CLI feature if wanted.
- [ ] **Lazy-reader follow-up — packer re-chunking** for intra-partition random
  access: re-chunk `pack_folder_to_cbk` into ~1–4 MB source-span gzip members so a
  deep seek decompresses only its chunk. Not "free" — it re-frames
  `partition-N.gz` and needs a recomputed per-partition CRC32.
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
