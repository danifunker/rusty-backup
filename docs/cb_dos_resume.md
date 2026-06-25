# crusty-backup (`cb-dos`) — resume prompt

Hand-off for continuing the crusty-backup / `.cbk` work. Read this first, then
[`cb_dos.md`](cb_dos.md) (local backup/restore plan + progress log) and
[`cb_dos_network_and_state.md`](cb_dos_network_and_state.md) (the `.cbk` container
+ network plan, §2). Everything below was verified on **real FreeDOS in qemu**.

**The remaining-work checklist lives in [`cb_dos_todo.md`](cb_dos_todo.md)** — the
single tick-it-off backlog. Resume here for context; work from there.

## Where we are (2026-06-25)

Branch **`cbdos`** (off `main`), 42 commits ahead of `main`, all verified, tree
clean:

```
6aa3eef feat(cb-dos): lz4 browse — ls/get from a partition-N.lz4 backup
53ca69e feat(cb-dos): clone /DEFRAG — repack FAT volumes contiguously onto the target
4c847ed fix(cb-dos): mbr.bin corruption under stdout redirection (_IONBF -> _IOLBF)
a900b24 feat(cb-dos): Phase 6 — LZ4 codec on DOS (backup /CODEC:LZ4 + restore)
9f036c8 feat(rbformats): LZ4 codec (Phase 6 desktop half) — CompressionType::Lz4
23a74d9 feat(cb-dos): Phase 5 — boot-aware FAT file-level defrag (backup /DEFRAG)
e45bf49 perf(cbk): lazy .cbk disk reader (skip the whole-disk reconstruct)
dbcdfc7 feat(cb-dos): extended / logical partition backup, restore, and clone
6ffe918 fix(restore): don't double-count logical partitions in the restore pre-flight
e3284d6 feat(cb-dos): live progress (%, speed, ETA) for backup/restore/clone
7a8c246 feat(cb-dos): Phase 4d — NTFS backup/restore/clone via $Bitmap compaction
13b32cf feat(cb-dos): Phase 4c-c — browse a live disk (ls/get/TUI over read_lba)
c59cd83 feat(cb-dos): Phase 4c-b — TUI browse/mark/extract (single file, multi, folders)
54322ab feat(cb-dos): Phase 4c-a — browse + extract single files from a backup (ls/get)
7a2c603 feat(cb-dos): Stage 2 — real TUI front-end (live enum, drives the cmd_* engine)
7c8245c refactor(cb-dos): consolidate into one CRUSTYBK.EXE (cbdisk + cmd_* + dispatcher)
93e5c2c feat(cb-dos): Phase 4b — direct disk-to-disk clone (cbclone)
7a8830d docs(cb-dos): mark desktop --partitions off-by-one resolved in resume next-work
37b9b70 fix(cli): make backup --partitions actually 1-based (first partition selectable)
3109e50 feat(cb-dos): Phase 4 — per-partition selective backup/restore (/PARTS)
028b3c1 feat(cb-dos): on-DOS FAT resize in cbrestore — /SIZE entire/minimum/custom
ec66eef fix(cb-dos): root-cause + fix the CWSDPMI termination hang (DAP buffer overrun)
61dfb64 fix(cbk): make PathBuf import unconditional so no-chd builds compile
ba93140 feat(cbk): edit a partition inside a .cbk (materialize -> edit -> repack)
acc7daf feat(cbk): native .cbk as a first-class image — inspect/browse/extract/restore
5d3e562 feat(cbk): chunked .cbk backup container — pack/unpack + native restore
2351f99 feat(cb-dos): Phase 3 restore engine — write the native folder back to a disk on DOS
c55211e feat(cb-dos): Phase 2 backup engine — image a FAT disk on DOS to the native folder
b55db82 feat(backup): add Gzip codec — the shared format with crusty-backup (cb-dos)
ee3ac3e docs(cb-dos): mark net Phase 7a complete — handshake verified on FreeDOS/qemu
```

**Done and proven end-to-end:**

| Area | State |
|------|-------|
| Net **7a** (binary Family-B handshake) | `NETHELLO` ↔ `rb-cli serve` round-trips on FreeDOS/qemu |
| **Phase 1** — desktop `Gzip` codec (`.gz`) | `rb-cli backup --format gzip`; restore/resize reuse it 100% |
| **Phase 2** — `cbbackup` (DOS) | images a FAT disk → native folder; desktop restores it |
| **Phase 3** — `cbrestore` (DOS) | folder → disk on DOS; **byte-identical** to source |
| **Phase 3 resize** — `cbrestore /SIZE` | bidirectional FAT16/32 resize on DOS (entire/minimum/custom); grow+shrink verified on FreeDOS/qemu |
| **Phase 4** — `/PARTS:i,j` selective | per-partition backup *and* restore (0-based MBR slot indices); verified on a 2-partition disk on FreeDOS/qemu |
| **Phase 4b** — `cbclone` (DOS) | direct disk-to-disk clone, no staging file; compact + `/SIZE` resize + `/PARTS`; verified grow/shrink/subset on FreeDOS/qemu |
| **One exe** — `CRUSTYBK.EXE` | backup/restore/clone/inspect subcommands **+ a real text UI** (live int13h enum; F2/F3/F4 drive the cmd_* engine) over the shared `cbdisk` engine; both verified on FreeDOS/qemu |
| **Phase 4c-a** — `ls` / `get` (DOS) | browse + extract single files from a backup `partition-N.gz` (FAT dir reader + LFN over `gzseek`), no scratch/no full restore; verified incl. LFN + nested + multi-cluster on FreeDOS/qemu |
| **Phase 4c-b** — TUI browse (DOS) | F6 Browse: interactive file browser, mark files+folders, F2 extract (folders recurse); verified extracting a file + a folder tree byte-identical on FreeDOS/qemu |
| **Phase 4c-c** — live-disk browse (DOS) | same `ls`/`get`/TUI-browse against a *mounted/attached* disk (no imaging first): `vol_read_at` dispatches gzseek **or** int13h `read_lba`; CLI `@HH` + MBR slot, TUI F6 on a FAT partition row; read-only on source; verified byte-identical on FAT32 + FAT16 (CLI + TUI) on FreeDOS/qemu |
| **Phase 4d** — NTFS (DOS) | `backup`/`clone` image NTFS (type 0x07) via `$Bitmap` compaction (`cbntfs.{h,c}`); restore same-size on DOS (resize via desktop `resize_ntfs_in_place`); verified on FreeDOS/qemu — 63 MB random-filled NTFS → 0.4 MB gz, restore + clone `ntfsfix`-clean with files byte-identical, desktop restore cross-checked |
| **Live progress** (DOS) | backup/restore/clone show a live `\r` line — percent, transferred/total MiB, MiB/s, ETA (`progress_t` in `cbdisk`, BIOS-tick timing, `isatty`-gated); shared engine so **CLI + TUI both** show it; screendump-verified mid-op on FreeDOS/qemu |
| **Phase 4e** — extended/logical (DOS) | backup/restore/clone follow the **EBR chain** of an extended container (`walk_ebr_chain`/`write_ebr_chain` in `cbdisk`, ports of parse/build_ebr_chain); logicals (index 4+) imaged with `is_logical` + an `extended_container` block; same-size on DOS (resize via desktop); verified on FreeDOS/qemu — primary FAT16 + ext { FAT16, NTFS } round-trips byte-identical via cb-dos restore, cb-dos clone, **and** desktop restore (after fixing a desktop pre-flight double-count) |
| **Phase 5** — FAT defrag (DOS) | `backup /DEFRAG` **and `clone /DEFRAG`** repack a FAT12/16/32 volume's files+dirs into contiguous runs (boot files pinned first) — read-only on the source; backup → smaller `imaged_size`, clone → defragged target (same-size), both via the shared `cbdefrag` planner (sink = compressed writer / target disk). Declines safely on an unclean FS. Verified on FreeDOS/qemu — backup FAT16 25.2→1.2 MB / FAT32 72.7→2.7 MB, byte-identical (desktop **and** on-DOS restore), a SYS'd bootable disk defragged+restored **boots**, lost-cluster disk declines; clone /DEFRAG byte-identical + defragged target |
| **Phase 6** — LZ4 codec (both) | a second shared codec: desktop `rb-cli backup --format lz4` (`lz4_flex` frame, `src/rbformats/lz4.rs`) + DOS `CRUSTYBK backup /CODEC:LZ4` (liblz4 frame via `cbcodec.{h,c}`; restore auto-detects). Cheaper than gzip on a 486 at a lower ratio; gzip stays default. Standard LZ4 frame so members interchange. Verified on FreeDOS/qemu — byte-identical all three ways (DOS↔desktop, DOS↔DOS) + composes with `/DEFRAG`. **Browse** (`ls`/`get`) reads `.lz4` too (seek-by-decode) |
| **Fix** — redirected backup | the long-standing "redirecting `BACKUP` corrupts `mbr.bin`" gotcha is fixed: `_IONBF`→`_IOLBF` in all `cmd_*`. Redirected `backup`/`get` verified clean on qemu |
| **`.cbk`** container (frozen v1) | `cbk pack`/`unpack`; **native** inspect/ls/get/fsck/restore; **edit** (put/rm/mkdir via materialize→edit→repack) |
| **Lazy `.cbk` reader** (desktop) | `CbkLazyReader` reads a `.cbk` as a disk without the whole-disk reconstruct — structural sectors eager (MBR+CHS, EBR), partition bytes decompressed on demand; engages only when the `hidden_sectors` patch is a provable no-op, else falls back; byte-identical test + `rb-cli`-verified |
| **Distribution** (CI) | release pipeline builds + ships a bootable **FreeDOS floppy + CD** (`build-cb-dos` job → `mkmedia.sh` → `cbdos-freedos-<ver>.img` / `cbdos-<ver>.iso`) |
| **CWSDPMI exit-hang** | root-caused (DAP buffer overrun) + fixed; tools exit cleanly |

## Next work — see [`cb_dos_todo.md`](cb_dos_todo.md)

The prioritized, tick-it-off backlog now lives in **[`cb_dos_todo.md`](cb_dos_todo.md)**
(one source of truth; update it as items land). Top of the queue, in order:

1. **Net 7b–7i** — networked backup/restore (only 7a/handshake done). The local
   foundation + the frozen `.cbk` are all in place; this is the next big phase.
2. **Real-486 hardware** validation (everything so far is qemu) — once the rig is
   fully set up.
3. *(optional, bigger)* **Desktop defrag parity** (repack FAT on the desktop too),
   **lazy-reader packer re-chunking** (source-span gzip members + recomputed CRC),
   **boot-media driver profiles** (CD-ROM / USB CONFIG.SYS menu entries).

Done since the last refresh: the mbr.bin-under-redirection bug (fixed), `clone
/DEFRAG`, and lz4 browse. Dropped by decision: exFAT backup-source, ext2/3.

(Resolved 2026-06-24: the desktop `--partitions` off-by-one — `parse_indices`
now subtracts 1, so the flag is genuinely 1-based and matches `img@N`; commit
`37b9b70`.)

## Key files

**Desktop (Rust):**
- `src/backup/mod.rs` — `CompressionType::Gzip` / `CompressionType::Lz4`.
- `src/rbformats/gzip.rs` — gzip codec; `compress.rs` — `Gzip` arm + `"gzip"`
  decode (`MultiGzDecoder`).
- `src/rbformats/lz4.rs` — LZ4 frame codec (`lz4_flex`); `compress.rs` — `Lz4` arm
  + `"lz4"` decode (`FrameDecoder`); `BackupFormat::Lz4` / `--format lz4` in
  `cli/verbs/backup.rs`. `Cargo.toml` `lz4_flex` dep.
- `src/rbformats/cbk.rs` — **the `.cbk` format** (RBKC chunks / RBKI index / RBKF
  footer), `pack_folder_to_cbk`, `materialize_cbk_to_folder`, `is_cbk`.
- `src/cli/verbs/cbk.rs` — `rb-cli cbk pack|unpack`.
- `src/model/source_reader.rs` — `CbkTempReader` + `open_read_dispatch` arm (native
  read); `browse_session.rs` — `.cbk` browse arm; `file_types.rs` — `"cbk"` in
  `DISK_IMAGE_EXTS`.
- `src/cli/resolve.rs` — `RwCommit::Cbk` (edit-then-repack); `cli/backup_edit.rs`
  — `gzip` in the editable-codec whitelist.

**DOS (C) — `crusty-backup/src/`:** one tool, `CRUSTYBK.EXE` (TUI on bare run,
subcommands for scripting), built from:
- `cbdisk.{h,c}` — **the shared engine**: int13h r/w (LBA+CHS), geometry, AH=48h
  `drive_total_sectors`, `parse_fatlay`/`fat_entry`/`is_fat_part_type`, the
  bidirectional `fat_resize` (C port of `resize_fat_in_place`) + its helpers
  (`shift_region_forward`/`_backward`, `compute_fat_sectors`,
  `max_fat_window`/`min_fat_window` cluster cap/floor, `set_clean_flags`,
  `reset_fsinfo`), and the arg helpers (`switch_val`/`eq_ci`/`round_up_512`/
  `parse_parts`, `rd64`), the **EBR chain** helpers (`walk_ebr_chain`/
  `write_ebr_chain` + `is_extended_type` + `logical_t`, ports of the desktop's
  parse/build_ebr_chain for extended/logical partitions), and the **live transfer
  progress** meter (`progress_t` + `progress_begin`/`progress_update`/
  `progress_finish`: `\r` line with %, MiB, MiB/s, ETA from the BIOS tick counter,
  `isatty`-gated). The single source of truth — no more triplicated primitives.
- `cbntfs.{h,c}` — the **NTFS `$Bitmap` reader** (no driver): `ntfs_parse` (BPB),
  `ntfs_load_bitmap` (MFT #6 `$Bitmap` + fixup + `$DATA` run decode → RAM bitmap,
  set bit = used), `ntfs_cluster_used`, `ntfs_is_ntfs` (OEM-id check vs exFAT/HPFS).
  Lifted from the `disk_spike.c` probe. Used by `backup`/`clone` for NTFS compaction.
- `cbcodec.{h,c}` — the **compressed-stream abstraction** (gzip + LZ4): a writer
  (`cbw_open`/`cbw_write`/`cbw_close`) and reader (`cbr_open`/`cbr_read`/
  `cbr_close`) over zlib `gzFile` (gzip) and liblz4 `LZ4F` streaming frame (lz4),
  with a fixed bounce buffer so any partition size streams. `cbr_read` mirrors
  `gzread`'s contract (bytes / 0 EOF / -1 err). Used by `cmd_backup` (`/CODEC:LZ4`),
  `cbdefrag` (emit), `cmd_restore` (codec auto-detected from the member ext), and
  `cmd_browse` (the lz4 read backend). liblz4 is cross-built by `deps/fetch-lz4.sh`.
- `cbdefrag.{h,c}` — the **FAT file-level defragmenter** for `backup /DEFRAG`
  (`defrag_backup_fat`) **and `clone /DEFRAG`** (`defrag_clone_fat`). A shared
  `defrag_plan_build` + a sink-based emit (sink = the compressed writer for backup,
  the target disk for clone). Walks the dir tree **read-only** on the source,
  assigns each file/subdir a contiguous run of new clusters (boot files
  `IO.SYS`/`MSDOS.SYS`/`IBMBIO.COM`/`IBMDOS.COM`/`KERNEL.SYS` pinned first),
  builds a fresh FAT, and streams the relocated image out — dir entries
  (incl. `.`/`..`) repointed via the relocation map, LFN entries verbatim, FAT32
  root → cluster 2 + FSInfo reset. **Declines** (returns 1, gz untouched) on a
  not-provably-clean FS (lost/bad/cross-linked clusters, OOM) → caller images
  with plain compaction. Source never written, so a bug can only yield a bad
  backup, never corrupt the source.
- `cmd_backup.c` — `backup` (image FAT/NTFS disk → native folder, smart-compact +
  gzip, `/PARTS`, `/DEFRAG`): FAT compacts from its FAT (or, with `/DEFRAG`,
  repacks via `cbdefrag` then falls back to plain compaction if it declines),
  NTFS (`backup_ntfs_partition`) from the `$Bitmap` (full window, free clusters
  zeroed); **extended containers** are walked and their **logical** volumes
  imaged (`is_logical`, index 4+, + `extended_container` metadata). `cmd_restore.c` — `restore` (folder → disk,
  `/SIZE` resize + `/PARTS`, the metadata.json scanner + gzip stream; **FS-agnostic**
  so NTFS restores same-size; logicals same-size + the **EBR chain rebuilt** via
  `write_ebr_chain`; resize gated on 512-byte FAT, primary growth clamped at the
  extended container). `cmd_clone.c` — `clone` (direct disk-to-disk, no staging
  file, `/SIZE` + `/PARTS`; FAT resizes, NTFS clones same-size via
  `clone_ntfs_partition`; **logicals cloned same-size + EBRs copied verbatim**).
  `cmd_inspect.c` —
  `inspect` (list BIOS drives + partitions). `cmd_browse.c` + `cbbrowse.h` —
  `ls` / `get` **and the shared browse engine** (`fatvol_t`, `cbk_open_vol`/
  `cbk_open_vol_live`/`cbk_list_dir`/`cbk_extract`/`cbk_extract_tree`): a
  FAT12/16/32 directory reader with LFN reassembly + a file/tree extractor over a
  single dispatched read primitive (`vol_read_at`) with **three backends** — a
  backup `partition-N.gz` (`gzseek`), a backup `partition-N.lz4` (seek by forward
  decode, reopen on backward seek — frames aren't seekable), **or** a live FAT
  partition on a BIOS drive (int13h `read_lba`); all finish through
  `vol_finish_open`. `cbk_open_vol` prefers `.gz`, falls back to `.lz4`. No scratch, no full
  restore. CLI source is a backup folder or `@HH` (live drive 0xHH, `N`=MBR slot;
  `open_browse_src`/`live_part_lba`). Each command exposes `int cmd_X(argc,argv)`.
- `crustybk.c` — `main()` dispatches the subcommands or launches the **text UI**
  (`tui_main`): `scan_disks` does live int13h enumeration; F2/F3/F4 gather params
  (`read_line`/`pick_size`/`confirm_erase`) and call `cmd_*()` on a plain screen;
  **F6 Browse** runs the shared `browse_loop` (mark files+folders, F2 extracts,
  folders recurse) over `cbbrowse.h` — on a highlighted **FAT partition row** it
  browses that partition **live** (`do_browse_live`), otherwise it prompts for a
  backup folder (`do_browse_backup`). `disk_spike.c` — disk/FS spike.
  `net_hello.c` — WATT-32 handshake client. `lfn_test.c` — raw LFN-API probe.
- `Makefile` targets: `make crustybk` (the tool; links zlib + liblz4 once) /
  `make all` (+ diagnostics) / `make net` / `make size`.
- `deps/fetch-zlib.sh`, `deps/fetch-lz4.sh`, `net/fetch-watt32.sh` — cross-built
  deps (gitignored). Run both `fetch-*` once before `make crustybk`.

## Build + unit test

```bash
cargo build --bin rb-cli          # desktop CLI
cargo test --lib                  # 2090+ tests; clippy --all-targets -D warnings via pre-commit
sh crusty-backup/deps/fetch-zlib.sh && sh crusty-backup/deps/fetch-lz4.sh  # once
make -C crusty-backup crustybk      # the unified CRUSTYBK.EXE -> crusty-backup/build/
make -C crusty-backup all net       # + diagnostics (disk_spike/lfn_test) + nethello
```

## The qemu test rig (the scratch harnesses are EPHEMERAL — rebuild as below)

Persistent on this machine: **`~/djgpp`** (cross-gcc), **`~/FD14FULL.img`**
(FreeDOS 1.4 FULL installer, FAT32 partition @ sector 63 / byte offset 32256),
`qemu-system-i386`, `mtools`. Gitignored-but-present (regenerable):
`crusty-backup/deps/zlib`, `net/watt32`, `net/drivers/NE2000.COM`. **Not
persistent — refetch:** `CWSDPMI.EXE`:

```bash
curl -fsSL -o /tmp/csdpmi.zip https://www.delorie.com/pub/djgpp/current/v2misc/csdpmi7b.zip
unzip -o -j /tmp/csdpmi.zip bin/CWSDPMI.EXE -d /tmp     # -> /tmp/CWSDPMI.EXE
```

**Recipe to run a cb-dos tool headless and read its output** (the verified
pattern — works because int13h writes commit immediately and the tools now exit
cleanly so `FDAPM POWEROFF` ends qemu):

```bash
IMG=/tmp/base.img ; OFF=32256 ; AT="$IMG@@$OFF"
cp ~/FD14FULL.img "$IMG"                                   # throwaway boot disk
printf '@echo off\r\nSET PATH=\\FREEDOS\\BIN\r\nDOSLFN\r\nSET LFN=Y\r\n'\
'C:\r\nmd C:\\BK\r\nCD \\CB\r\nCRUSTYBK BACKUP C:\\BK 81\r\nFDAPM POWEROFF\r\n' \
  | mcopy -o -i "$AT" - ::/FDAUTO.BAT                      # autorun (LFN!)
mmd -i "$AT" ::/CB 2>/dev/null
mcopy -o -i "$AT" crusty-backup/build/crustybk.exe ::/CB/CRUSTYBK.EXE
mcopy -o -i "$AT" /tmp/CWSDPMI.EXE                  ::/CB/CWSDPMI.EXE
# hda = boot disk; hdb (0x81) = the FAT disk to back up:
qemu-system-i386 -m 64 -display none -no-reboot \
  -drive file="$IMG",format=raw,if=ide,index=0 \
  -drive file=/path/to/source-fat.img,format=raw,if=ide,index=1
mcopy -s -n -i "$AT" ::/BK /tmp/out                        # pull the backup folder off
```

Everything runs through the one binary now: `CRUSTYBK backup|restore|clone|inspect`
(case-insensitive; bare `CRUSTYBK` would open the TUI). A blank target for restore
tests: `dd if=/dev/zero of=/tmp/tgt.img bs=1M count=48`, attach as `index=1`
(0x81), and `CRUSTYBK RESTORE C:\BK 81 /Y`. Build an MBR FAT16 test disk with
`mformat -i disk.img@@1048576 ...` after writing an MBR partition entry (type 0x06,
start LBA 2048) — see the Phase-1 history in `cb_dos.md`.

**Resize tests** (`/SIZE:{ORIGINAL|MINIMUM|ENTIRE|CUSTOM}`, `/CUSTOM:<bytes>`):
attach several blank targets (`index=1..3` → `0x81..0x83`) of *different* sizes
and resize into each, e.g.
`CRUSTYBK RESTORE C:\BK 82 /Y /SIZE:ENTIRE` (grow to fill the disk),
`CRUSTYBK RESTORE C:\BK 83 /Y /SIZE:CUSTOM /CUSTOM:33554432`. To exercise both the
**grow** (forward-shift + FAT extend, capped at the FAT16 cluster ceiling) *and*
**shrink** (backward-shift + FAT truncate, floored at a valid FAT16) paths, test
with **both** producers: a `CRUSTYBK BACKUP` folder (gz keeps the original
full-size BPB → MINIMUM/CUSTOM actually shrink) and a desktop `rb-cli backup
--format gzip` folder (gz is pre-minimized → ORIGINAL/ENTIRE grow it). Pull each
target with `mcopy -i tgt.img@@1048576 ::/FILE out` and diff checksums; the
desktop's `rb-cli ls tgt.img@1` is a good independent FAT-reader cross-check.
**Don't redirect `CRUSTYBK BACKUP`** when making the folder (gotcha #3 corrupts
`mbr.bin`).

**Clone tests** (`CRUSTYBK CLONE`, no staging file): attach the **source** as one
drive and a blank **target** as another (e.g. source `index=1`/`0x81`, target
`index=2`/`0x82`), then `CRUSTYBK CLONE 81 82 /Y [/SIZE:mode] [/PARTS:i,j]`. Clone
writes nothing to DOS files, so **redirecting its stdout is safe** (unlike backup)
— `CRUSTYBK CLONE 81 82 /Y /SIZE:ENTIRE > C:\OUT.TXT`. Verify the target the same
way (`mcopy ... tgt.img@@1048576`, `rb-cli ls`). It refuses `src == tgt`.

**Browse/extract tests** (`ls`/`get`, Phase 4c-a): stage a backup folder on a DOS
drive (e.g. `C:\BK` with `partition-N.gz`). `CRUSTYBK ls C:\BK` lists the root,
`CRUSTYBK ls C:\BK \DOCS` a subdir; `CRUSTYBK get C:\BK \DOCS\FILE.TXT C:\OUT.TXT`
extracts one file. `ls` is safe to redirect; `get` writes a DOS file so **don't
redirect `get`** on the same drive (gotcha #3). Verify by `mcopy`-ing the
extracted file off and diffing checksums vs the source. N is the 0-based partition
index (defaults to the first `partition-N.gz` present); paths use `\` or `/`.

**Live-disk browse tests** (`ls`/`get` with `@HH`, Phase 4c-c): attach a FAT disk
as a second drive (`index=1` → `0x81`) and point the *same* commands at it with
the `@HH` source — `CRUSTYBK ls @81`, `CRUSTYBK ls @81 0 \DOCS`,
`CRUSTYBK get @81 \DOCS\F.TXT C:\OUT.TXT` (`@HH` = BIOS drive 0xHH, the trailing
`N` is the **MBR slot**, default first FAT slot). No imaging first — it reads the
live volume over int13h. Build the source disk exactly like the clone/PARTS tests
(own exact-size FS file → `dd` into a slot with a type-0x06 MBR entry; gotcha #7),
attach it as `0x81`, run the commands writing output to **C:** (a different
physical drive than the source, so `get` is safe), pull the output off the boot
disk and diff. Source is **read-only** (no int13h writes). The **TUI** path:
highlight the live FAT partition row in the menu and press **F6** (a backup folder
is still reachable by pressing F6 on a disk row / when nothing FAT is selected).
Verified on both FAT32 (root-cluster) and FAT16 (fixed-root) attached disks.

**Defrag tests** (`backup /DEFRAG`, Phase 5): the win is only visible on a
**fragmented** disk, and mtools writes contiguously, so *manufacture* fragmentation:
copy the real files first (low clusters), then a pile of filler files, then a small
`TAIL.BIN` (lands at the end), then `mdel` the filler — now `TAIL.BIN` is parked at
a high cluster with a free gap before it. Build the FS in its own exact-size file
(gotcha #7), `dd` into a type-0x06/0x0C MBR slot, attach as `0x81`. Run **both**
`CRUSTYBK BACKUP C:\BKD 81 /DEFRAG` and a plain `CRUSTYBK BACKUP C:\BKP 81`: the
proof is `imaged_size_bytes` in `metadata.json` — plain images up to `TAIL`
(tens of MB), `/DEFRAG` packs to ~the used-data size (the `.gz` is only slightly
smaller because plain already zeroes+gzips the gap). Restore the `/DEFRAG` folder
(desktop `rb-cli restore` *and* a cb-dos `RESTORE`), `mcopy` every file off and
`md5` vs source; a tiny FAT-reader script confirms each file is contiguous, the
boot file sits at cluster 2, and FAT32 `..`=root(2) / FSInfo reset. **Boot test:**
`SYS` a fresh FAT16 (own MBR boot code + active flag) inside qemu, put an
`AUTOEXEC.BAT` that writes a `C:\IBOOTED.TXT` marker + `FDAPM POWEROFF`,
`/DEFRAG`-back-it-up, restore to a blank disk, boot **that disk alone** — the
marker file appearing proves the chain booted. **Decline test:** patch a free FAT
cluster to `0xFFFF` in both copies (a lost cluster); `/DEFRAG` must fall back to
plain (large `imaged_size`), never silently drop it.

**LZ4 tests** (`backup /CODEC:LZ4`, Phase 6): prove the **standard frame**
interoperates both ways. (a) `CRUSTYBK BACKUP C:\BL 81 /CODEC:LZ4` writes
`partition-0.lz4` + `compression_type: "lz4"`; pull it off and `rb-cli restore`
it on the desktop (DOS liblz4 → desktop `lz4_flex`). (b) `rb-cli backup … --format
lz4` on the host, stage the folder on a DOS drive, `CRUSTYBK RESTORE` it (desktop
`lz4_flex` → DOS liblz4). (c) `CRUSTYBK RESTORE C:\BL 82 /Y` (DOS → DOS). All three
must be byte-identical. Also confirm `/DEFRAG /CODEC:LZ4` composes (defragged lz4,
IO.SYS at cluster 2). `restore` picks the codec from the member extension, so no
extra flag is needed. Browsing (`ls`/`get`) an lz4 backup is **not** supported
(frames aren't seekable) — restore it first.

## Gotchas learned the hard way (do not relearn these)

1. **No LFN on the FD14FULL kernel.** `71A0h` returns ax `0x7100`. You **must**
   `DOSLFN` (at `\FREEDOS\BIN\DOSLFN.COM`) before running cb-dos tools, or
   `partition-0.gz` truncates to `PARTITIO.GZ` and the `.gz` / `.gz.crc32` 8.3
   names collide. (`SET LFN=Y` alone is not enough — the kernel API must exist.)
2. **DAP/DOS-buffer sizing.** `xfer_init` allocates `XFER_BYTES + 16` because
   `read_lba`/`write_lba` put the 16-byte int13h Disk Address Packet at offset
   `XFER_BYTES`. Allocating exactly `XFER_BYTES` overruns the next MCB and hangs
   the process **at exit** (only after an AH=42 read). This bit us for a while —
   don't reintroduce it when copying the disk primitives into new tools.
3. **FreeCOM redirection quirks.** `2>` / `2>&1` are mis-parsed (the `2` becomes a
   program argument — silently sent `NETHELLO` to *port 2*); `>>` append is
   unreliable. Use a single `>` and pass args explicitly. **(RESOLVED 2026-06-25)**
   The old "redirecting `CRUSTYBK BACKUP`'s stdout corrupts `mbr.bin`" bug is
   **fixed**: the root cause was `setvbuf(stdout, NULL, _IONBF, 0)` — unbuffered
   stdout, when redirected on FreeDOS, bled its final writes into a just-closed DOS
   file's clusters (the banner landed at `mbr.bin` byte 158). All `cmd_*` now use
   `_IOLBF` (line-buffered), so `CRUSTYBK BACKUP … > C:\LOG.TXT` (and a redirected
   `get`) are clean — verified on qemu. The other two parts of this gotcha (the
   `2>` mis-parse and `>>` flakiness) are FreeCOM quirks, still true.
4. **int13h writes are immediate** (not DOS-file-cached), so a restore lands on
   disk even if the process were killed mid-run — handy for headless tests.
5. **`.cbk` v1 is frozen** (`cbk.rs` doc-comment). The future DOS network producer
   (7b) must emit the same bytes; the desktop packer emits one chunk/member, the
   network producer will emit many — both valid.
6. **CWSDPMI vs DOSBox-X.** DOSBox-X supplies its own DPMI host and masked the DAP
   overrun; always validate exit behavior under **CWSDPMI on real FreeDOS** (qemu).
7. **`mformat -i img@@OFF` sizes the FS to EOF, not the partition count.** Building
   a multi-partition test disk by `mformat`-ing each `@@offset` makes every FS span
   from its offset to end-of-file → overlapping volumes whose BPB `total_sectors`
   ignore the MBR entry. Build each partition FS in its **own exact-size file**
   (`dd ... count=<sectors>; mformat -i p.img ::`) and `dd` it into the disk at the
   slot offset, so each BPB total matches its MBR count (needed for a clean
   ORIGINAL/`/PARTS` round-trip).

## Doc-sync reminder (CLAUDE.md)

`.cbk` is already in the README image/backup-formats table and `DISK_IMAGE_EXTS`
(+ a `file_types` regression test). When a new fs/container makes a MiSTer core go
end-to-end, also walk `docs/full_MiSTer_support_status.md`. The cb-dos plan docs
(`cb_dos.md`, `cb_dos_network_and_state.md`) carry the living checkboxes + progress
logs — update them as work lands.
