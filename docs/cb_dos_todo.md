# crusty-backup (cb-dos) — remaining-work checklist

The **living backlog**: the single place for *what's left*. Tick items off as they
land; each box is sized to roughly one commit (or a small handful). Ordered by the
agreed priority. Context + history live in [`cb_dos.md`](cb_dos.md) (phased plan +
progress log) and [`cb_dos_resume.md`](cb_dos_resume.md) (hand-off + the
"done & proven on qemu" table). **Resume** from `cb_dos_resume.md`; **work** from
here, and update both as items land.

Legend: `[ ]` todo · `[~]` in progress · `[x]` done · `[-]` dropped

## Now (priority order)

- [ ] **Phase 5 — file-level repack/defrag (FAT, boot-aware).** Reorder files
  contiguously inside the emitted partition before zeroing the now-contiguous free
  tail → a defragmented `partition-N.gz` (same format, just packed). Optional
  mode; ships after the round-trip is proven (it is).
  - [ ] Walk the FAT dir tree + cluster chains (reuse `cbbrowse.h`'s FAT reader).
  - [ ] Compute a packed layout; keep `IO.SYS` / `MSDOS.SYS` **first + contiguous**
        at the data-area start (the `SYS`-command rule) so the disk still boots.
  - [ ] Rewrite FAT + dir entries + relocate clusters into the output image.
  - [ ] Expose it as a `/DEFRAG` flag on `backup` (and maybe `clone`).
  - **Done when:** defrag a fragmented FAT disk on qemu → boots, every file
    byte-identical, `.gz` smaller; desktop restore agrees.

- [ ] **Phase 6 — LZ4 codec** (faster on slow CPUs; gzip stays the default).
  - [ ] Desktop: `CompressionType::Lz4` + `src/rbformats/lz4.rs` (mirror
        `gzip.rs`) + the three `compress.rs` dispatch arms; `--format lz4`.
  - [ ] DOS: link an LZ4 (de)compressor; `/CODEC:LZ4` on `backup`/`clone`,
        auto-detected on restore from metadata `compression_type: "lz4"`.
  - **Done when:** an lz4 backup round-trips on qemu; size/speed vs gzip noted.

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

Phases 1–4e, live progress, the lazy `.cbk` reader, and the FreeDOS floppy + CD in
CI are all shipped and qemu-verified — see the "done & proven" table in
[`cb_dos_resume.md`](cb_dos_resume.md) and the progress log in [`cb_dos.md`](cb_dos.md).
