# crusty-backup (`cb-dos`) — resume prompt

Hand-off for continuing the crusty-backup / `.cbk` work. Read this first, then
[`cb_dos.md`](cb_dos.md) (local backup/restore plan + progress log) and
[`cb_dos_network_and_state.md`](cb_dos_network_and_state.md) (the `.cbk` container
+ network plan, §2). Everything below was verified on **real FreeDOS in qemu**.

## Where we are (2026-06-25)

Branch **`cbdos`** (off `main`), 20 commits ahead of `main`, all verified, tree
clean:

```
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
| **`.cbk`** container (frozen v1) | `cbk pack`/`unpack`; **native** inspect/ls/get/fsck/restore; **edit** (put/rm/mkdir via materialize→edit→repack) |
| **CWSDPMI exit-hang** | root-caused (DAP buffer overrun) + fixed; tools exit cleanly |

## Next work (prioritized — pick up here)

1. **Browse a *live disk* (not just a backup).** The browse engine (`cbbrowse.h`)
   is parameterized only by how it reads bytes — front it with `read_lba` instead
   of `gzseek` and the same `ls`/`get`/TUI-browse work against a mounted/attached
   disk too (recover a file from a card without imaging it first). Small, high-
   value. *Recommended next.*
2. **Lazy `.cbk` reader (perf).** Today `CbkTempReader` reconstructs the whole disk
   to a tempfile at open (fine for small backups, slow for multi-GB). Re-chunk the
   packer (`pack_folder_to_cbk`) into ~1–4 MB source-span gzip members (the v1
   format already supports multiple chunks/member) and make the reader decompress
   only the members a seek touches. No format change. (Also makes `get`'s backward
   gzseeks cheap — today they rewind to the gz start.)
3. **Net 7b** — the `.cbk` chunk **wire** protocol (the container is frozen, so
   this is mainly framing + the incremental `.idx` resume sidecar). See
   `cb_dos_network_and_state.md` §2c/§3 and §9 (7b–7i).
4. **`backup` mbr.bin corruption under stdout redirection (bug, low-priority).**
   Redirecting `CRUSTYBK BACKUP`'s stdout to a file on the *same drive* it writes
   the backup folder to bleeds its "wrote metadata.json" banner into `mbr.bin`'s
   boot-code area (corrupting restores from that folder). A FreeCOM/DOS file-handle
   quirk (gotcha #3), not a restore bug — run backup without `>` and it's clean.
   `get` also writes a DOS file, so the same caution applies; don't redirect it on
   the same drive. Worth root-causing (likely a DTA/FILE-buffer aliasing).
5. **Real-486 hardware** validation (everything so far is qemu/emulator).
6. **Phase 5 — file-level repack/defrag** (boot-file aware), and the **NTFS /
   logical-partition** backup gaps — the remaining FS coverage on DOS.

(Resolved 2026-06-24: the desktop `--partitions` off-by-one — `parse_indices`
now subtracts 1, so the flag is genuinely 1-based and matches `img@N`; commit
`37b9b70`.)

## Key files

**Desktop (Rust):**
- `src/backup/mod.rs` — `CompressionType::Gzip`.
- `src/rbformats/gzip.rs` — gzip codec; `compress.rs` — `Gzip` arm + `"gzip"`
  decode (`MultiGzDecoder`).
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
  `parse_parts`). The single source of truth — no more triplicated primitives.
- `cmd_backup.c` — `backup` (image FAT disk → native folder, smart-compact +
  gzip, `/PARTS`). `cmd_restore.c` — `restore` (folder → disk, `/SIZE` resize +
  `/PARTS`, the metadata.json scanner + gzip stream). `cmd_clone.c` — `clone`
  (direct disk-to-disk, no staging file, `/SIZE` + `/PARTS`). `cmd_inspect.c` —
  `inspect` (list BIOS drives + partitions). `cmd_browse.c` + `cbbrowse.h` —
  `ls` / `get` **and the shared browse engine** (`fatvol_t`, `cbk_open_vol`/
  `cbk_list_dir`/`cbk_extract`/`cbk_extract_tree`): a FAT12/16/32 directory reader
  with LFN reassembly + a file/tree extractor over `gzseek` random access (no
  scratch, no full restore). Each command exposes `int cmd_X(argc,argv)`.
- `crustybk.c` — `main()` dispatches the subcommands or launches the **text UI**
  (`tui_main`): `scan_disks` does live int13h enumeration; F2/F3/F4 gather params
  (`read_line`/`pick_size`/`confirm_erase`) and call `cmd_*()` on a plain screen;
  **F6 `do_browse`** opens an interactive file browser over `cbbrowse.h` (mark
  files+folders, F2 extracts the selection, folders recurse). `disk_spike.c` —
  disk/FS spike.
  `net_hello.c` — WATT-32 handshake client. `lfn_test.c` — raw LFN-API probe.
- `Makefile` targets: `make crustybk` (the tool; links zlib once) / `make all`
  (+ diagnostics) / `make net` / `make size`.
- `deps/fetch-zlib.sh`, `net/fetch-watt32.sh` — cross-built deps (gitignored).

## Build + unit test

```bash
cargo build --bin rb-cli          # desktop CLI
cargo test --lib                  # 2086 tests; clippy --all-targets -D warnings via pre-commit
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
   unreliable. Use a single `>` and pass args explicitly. **Also:** redirecting
   `CRUSTYBK BACKUP`'s stdout to a file on the *same drive* it writes the backup
   folder to corrupts `mbr.bin` (the "wrote metadata.json" banner bleeds into the
   boot-code area) → restores from that folder get a garbled MBR. Run the backup
   subcommand **without** `>`; restore/clone/inspect write nothing to DOS files so
   redirecting those is safe. Don't trust a folder for a byte-identical check if
   backup was redirected. Likely a `cmd_backup.c` DTA/FILE-buffer aliasing bug.
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
