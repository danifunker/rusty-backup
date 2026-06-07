# Open Work — Master Plan

The single source of truth for what's left to do on Rusty Backup. Each
section carries enough design detail to be actionable without external
references. The MiSTer plan
([`mister_filesystem_implementation_plan.md`](mister_filesystem_implementation_plan.md))
is the one other living plan and is treated as a single line item here;
everything else has been consolidated into this file.

Last reconciled against the code: 2026-06-03 (later — AtariST MiSTer prereqs).

When an item lands, remove its block. When new work surfaces, add a block
here.

---

## 1. Filesystem engine — Unix priority track

**Closed 2026-06-02.** ReiserFS (v3.5 + v3.6), UFS (UFS1 + UFS2), and JFS
(JFS2) all read end-to-end through the trait: inspect, layout-preserving
backup compactor, browse, file read, symlinks, recursive directory
descent. UFS U.4 (fsck + edit + repair) shipped 2026-06-03; see §10
for the per-FS one-liners. JFS J.4a (fsck read-only verifier),
multi-IAG dispatch covering >4k inodes, and the multi-page external
dtree walker (>127-entry directories) all shipped 2026-06-03 late. The
JFS fixture was regenerated in WSL Ubuntu-24.04 to include
`/bigdir/file_001..200` for the dtree-walker regression. **J.4b
(edit-side write primitives) is in §9 explicitly excluded** —
`repair()`-driven OrphanInode adoption is the only thing it would
unlock, real-world JFS orphans are rare, and Linux `fsck.jfs` is the
fallback when one surfaces. The multi-level dmapctl walker (>32 GiB
JFS aggregates) is also in §9 — vintage hardware doesn't address that
scale. ReiserFS is intentionally read-only forever (filesystem leaving
the kernel; investment caps at "read what exists").

---

## 2. Filesystem engine — multi-session existing tracks

### 2.1 XFS — close the v4-edit holes and add v5 write

XFS read + edit + fsck shipped on the `xfs-efs-fsck` track (~10k lines).
Oracle: `xfs_repair -n` clean on every write. Two oracle Docker images
already documented elsewhere: `rusty-xfs-oracle` (xfsprogs 4.9.0, v4, v2
inodes) and `rusty-xfs-oracle-v1` (xfsprogs 3.1.9, V1-inode IRIX disks).
For v5 you need a third image with recent xfsprogs (e.g. ubuntu:22.04).

Tools: `scripts/xfs-oracle.sh`; `examples/xfs_check.rs` (`--repair`),
`xfs_mkdir.rs`, `xfs_mkfile.rs`; and `rb-cli mkdir|put|rm|ls|get IMG[@N]`.
Real disk to test against: `~/Documents/scsi2.raw` (SGI, V1 inodes, XFS at
byte 2097152 / `@1`, partition slot 7). Always operate on a copy.

Open holes in v4 edit (suggested order — easiest/most reusable first):

- **(A) new-chunk inode allocation** — **shipped 2026-06-02** (single-leaf
  + multi-level grow; see §10).

- **(B) block → short-form dir re-compaction** — **shipped 2026-06-02**
  (see §10).

- **(C) leaf/node (multi-block) directories** — **block→leaf conversion
  shipped 2026-06-02** (v1: 2-data-block leaf form on overflow; see §10).
  Follow-ups still open: leaf-form INSERT (grow beyond 2 data blocks),
  leaf-form REMOVE, leaf→block recompaction, node form for very large
  dirs. v1 is enough to take a single-block dir past 127 entries; further
  inserts return `Unsupported` with a clear message.

- **(D) bmap-btree file forks** — **shipped 2026-06-02** (single-leaf
  bmbt — multi-leaf parked for follow-up; see §10). The R2 abort-on-btree
  gate is also relaxed: R2/R5 now account bmbt blocks via
  `collect_bmbt_blocks`.

- **(E) v5/CRC editing** — **shipped 2026-06-03** across nine slices
  (E.1 / E.2 / E.3 / E.4 / E.5a / E.5b / E.5c / E.5d). End-to-end v5
  file creation works on the modern `mkfs.xfs` fixture, `run_fsck`
  stays clean across alloc + dir-grow + bmbt + repair paths. See §10
  for per-slice audit. **R2 (freespace rebuild) + R3 (inobt rebuild)
  remain v4-only** — both can write a structurally correct CRC tree,
  but v5 layouts may carry `finobt` / `rmapbt` / `refcountbt`
  ro-compat metadata that R2's block-completeness map and R3's
  AGI-summary recompute don't model. Parked in §8 behind those
  side-trees actually being needed; the existing read path is
  already finobt-tolerant.

### 2.2 XFS shrink via clone-into-fresh

Edit shipped, so the prerequisites for a shrink path are in tree. Same
shape as the HFS+ defrag-on-backup pipeline (`hfsplus_defrag.rs` /
`hfsplus_clone.rs`) and the proposed btrfs scratch-recreate.

```
source XFS ──► read every file + xattr via XfsFilesystem
            ──► create_blank_xfs(target_size, label) → fresh mountable v4 image
            ──► replay dirs/files into it via edit.rs
            ──► stream the target image into the compressor
            ──► metadata.json records shrunk=true so restore knows to grow back
```

Restore: write cloned bytes into the (larger) target partition, then run an
in-tree `xfs_growfs`-equivalent (or document the manual `xfs_growfs`
post-restore step the way today's "Add free space" flow already does).

Each slice oracle-validated against `xfs_repair -n`:

| # | Slice |
|---|---|
| XR.0 | **Decision**: v4-only-first vs gate behind hole (E) v5 editing. v4 is enough to land the feature end-to-end on IRIX disks and old Linux; v5 follows when (E) does. |
| XR.1 | `create_blank_xfs(size_bytes, label, sector_size, ag_count) -> Vec<u8>` — emit a minimum-sized fresh v4 XFS that mounts clean and passes `xfs_repair -n`. Mirrors `create_blank_hfsplus` / `create_blank_efs`. |
| XR.2 | `clone_xfs_volume(source, target_size) -> Result<Vec<u8>>` — BFS-replay dirs + files (xattrs once that edit primitive lands; ignore for now or refuse with a clear error). Reuses `edit.rs` write primitives. |
| XR.3 | `stream_xfs_shrunk_to_writer` — peer of `stream_defragmented_hfsplus`: clone into a temp/in-memory target, then stream into the compressor with no double-disk-pass. |
| XR.4 | Pre-flight refusal: v5 sources (until XR.0+E), realtime volumes, multi-device XFS, images with xattrs we can't emit. |
| XR.5 | Backup-engine `shrink_to_minimum` flag wiring for XFS partitions, GUI checkbox + tooltip. Mirrors HFS+ Step 22f-i. |
| XR.6 | End-to-end backup → restore round-trip; restore-side post-write `xfs_growfs`-equivalent or document the manual step. |

Until XR.0 is decided, XFS backups continue through the existing
layout-preserving compactor (output size is fine — zeros compress
trivially; only wall-clock CPU is hurt).

### 2.3 HFS+ journal — Step 27 transactional writes

Journal parse + replay + dirty-volume recovery shipped (Steps 24-26 of the
HFS+ enhancements track). The Step 4 "refuse journaled" relaxation also
landed — `prepare_for_edit` now accepts a journaled volume when its journal
is clean (empty, `start == end`) and still refuses dirty ones.

**What's still open**: route every `do_sync_metadata` block write through
`TransactionBuilder.record_*` so edit-mode mutations land *inside* the
journal rather than writing directly to disk, plus the
`replay_dirty_journal`-on-open hook for `open_editable_filesystem`. The
recorder (`TransactionBuilder` in `src/fs/hfsplus_journal.rs:847`) and
`replay_journal` are in tree from steps 25/26; the plumbing below hasn't
been picked up.

Touch sites in `src/fs/hfsplus.rs`:
- `do_sync_metadata` becomes the natural transaction boundary. Every
  block-level write inside it (`write_fork_data`, B-tree node writes,
  bitmap writes, VH + alt-VH) goes through `pending_tx.record_*` instead
  of writing to disk directly.
- After in-memory mutations succeed, `pending_tx.commit(...)` runs once at
  the end of `do_sync_metadata`. A failure rolls back via the Step 5
  snapshot AND skips the commit, so the journal stays untouched.
- Touch-date stamping (Step 8) and counter updates ride inside the
  transaction.
- `open_editable_filesystem` for HFS+: when `vh.attributes &
  kHFSVolumeJournaledBit != 0`:
  - Clean (`kHFSVolumeUnmountedBit` set) — proceed; transactional writes
    preserve journaling.
  - Dirty + `replay_dirty_journal` on — call `replay_journal` first, then
    proceed.
  - Dirty + not authorized — keep refusing with the Step 4 message.
- Multi-megabyte mutations (defrag-clone in place) stay routed through
  `clone_hfsplus_volume` writing to a **fresh** target with no journal;
  only edit-mode mutations get journaled.

Tests:
- End-to-end edit on a clean journaled HFS+ image: create a file,
  `do_sync_metadata`, reopen with macOS-style replay, assert the file is
  present and the journal is empty.
- End-to-end edit on a dirty journaled image with `replay_dirty_journal=
  true`: replay applies pending transaction, then our own transaction
  lands on top, both visible after a second replay-and-reopen.
- Snapshot rollback during a journaled mutation: forced `DiskFull` →
  assert the journal is byte-identical to before the mutation (no partial
  transaction written).

### 2.4 btrfs — scratch-recreate (gated on writer)

The earlier "trim at last chunk end" idea was abandoned: real-world
layouts have metadata/system chunks placed mid-device, so last-chunk-end
is often close to `total_bytes` even on mostly-free volumes; trimming
past the 64 MiB / 256 GiB superblock mirrors drops redundancy; and
restore-side `resize_btrfs_in_place` would corrupt the volume if any
chunk lies past the trim point (chunk tree still references original
physical offsets).

**Recommended path**: clone the live source into a freshly formatted,
sized-to-fit target btrfs during backup. Same pattern as the HFS+
defrag-on-backup pipeline.

**Hard prerequisite**: a pure-Rust btrfs writer. We have none today —
`src/fs/btrfs.rs` is read-only: superblock + chunk tree + root tree + FS
tree + extents → `Filesystem` reads. No `EditableFilesystem`, no
allocator, no B-tree insert, no chunk tree builder, no CRC stamping for
tree blocks.

Building one is genuinely large (pessimistically larger than the HFS+
edit-mode work) because btrfs is COW with cross-linked B-trees:
chunk tree, extent tree, FS tree, root tree, free-space tree (post-v4.5
default), plus crc32c on every metadata block, plus subvolume layout at
minimum. Multi-month. **No alternative ships it faster** — shelling out
to `mkfs.btrfs` + `btrfs send/receive` is Linux-only and requires root,
losing the Windows/macOS coverage the project's "pure Rust, no platform
cfg" rule (CLAUDE.md) protects.

| # | Slice |
|---|---|
| B.0 | **Decision** — do we want to invest in a btrfs writer at all, or close as "wontfix — back up btrfs verbatim and let users shrink in-OS"? **Needs user input before any code work.** |
| B.1 | `create_blank_btrfs(size, label) -> Vec<u8>` — emit a fresh mountable btrfs. Bootstrap chunk array, system chunk, single data + metadata chunks, empty root/extent/fs trees, free-space tree. Validate with `btrfs check`. |
| B.2 | Single-file write: given a blank target and a `(path, bytes)`, allocate a data extent, write the file, insert INODE_ITEM/DIR_ITEM/DIR_INDEX/EXTENT_DATA, update extent tree back-refs, CRC everything. |
| B.3 | Recursive directory replay: walk source via `BtrfsFilesystem::list_directory` → emit into target. Handle nested dirs + many-file directories (B-tree split). |
| B.4 | `clone_into_blank_btrfs(source, target_size) -> Vec<u8>` + `stream_btrfs_to_target` for streaming into the compressor with no multi-GB temp file. |
| B.5 | Pre-flight refusal for subvolumes / snapshots / compressed extents we can't yet emit, with a clear error. |
| B.6 | Backup engine `shrink_to_minimum` wiring for btrfs partitions (parallel to HFS+ flag); GUI checkbox + tooltip + log polish. |
| B.7 | Restore-side: post-resize `resize_btrfs_in_place` already shipped; verify end-to-end. |

Until B.0 is decided, btrfs backups continue through the existing
layout-preserving compactor.

---

## 3. Filesystem engine — small to medium

(Section currently empty — §3.1 HFS raw-image expand closed 2026-06-03,
see §10. Reopen when new small-to-medium filesystem work surfaces.)

---

## 4. Clonezilla import — remaining gaps

GPT support shipped. The remaining gaps are LVM and RAID. Both are real
but rare in the project's use case (vintage hardware retro restores);
the motivating workflows are CF/SD cards and single-disk vintage
drives, not modern Linux servers. Open because a future user might hit
them; they sit behind everything in the MiSTer plan and §1-§3 above.

### 4.1 LVM (Logical Volume Manager)

Clonezilla preserves LVM2 metadata in `lvm_*.conf` / `lvm_logv.list` /
`lvm_vg_dev.list` sidecar files plus a `<disk>-<part>.<fs>-ptcl-img.*`
partclone image of the **physical volume** (the partition that backs
the PV header). Today we ignore the LVM sidecars entirely; the
partclone image is treated as raw bytes — restore writes the PV back
verbatim. The LV layout inside the PV is invisible to inspect/browse,
and restore-time resize leaves the VG metadata stale.

Work:
1. Parse `lvm_*.conf` (text format — UUIDs, PV/VG/LV map, extent
   counts).
2. Map each `lv_*` payload partclone image to its parent VG/LV.
3. Surface LVs as logical partitions in inspect (one row per LV inside
   the PV partition).
4. On restore, rewrite PV header `pe_count` and the VG metadata text
   to match the new partition size when the user resizes.

Reference: Clonezilla `ocs-functions` LVM block + `lvmdump` output
format.

### 4.2 RAID (mdadm / Linux software RAID)

Clonezilla images of Linux software RAID arrays write the array members
as separate partitions with type byte `0xFD` (Linux RAID Autodetect) and
a metadata sidecar describing the array shape. Today we detect the
`0xFD` partition type byte in `src/partition/mbr.rs:107` for display
("Linux RAID") but do nothing with the RAID metadata. Restore writes
each member partition back verbatim — fine for RAID-1, meaningless for
RAID-0/5/6 without reassembling the array first.

Work:
1. Parse the RAID sidecar (level, chunk size, member UUIDs, member
   order).
2. Refuse single-member restore for RAID-0/5/6 with a clear "this is
   part of an array; restore needs all N members at once" message.
3. Optional: synthesize an mdadm superblock on restore so the OS
   re-assembles the array on boot.

Reference: Linux `md/raid*.c` superblock format; Clonezilla
`ocs-functions` mdadm dump block.

---

## 5. MiSTer computer-core filesystems

The MiSTer FS plan lives in its own file because of its size (Wave 1 +
Wave 2 + Wave 3, ~100-150 sessions estimated):
[`mister_filesystem_implementation_plan.md`](mister_filesystem_implementation_plan.md).
Companion status survey:
[`full_MiSTer_support_status.md`](full_MiSTer_support_status.md).

Treat as a single tracked initiative here; progress is on that doc's
own per-format spine.

Gaps to reconcile back into the plan when it gets picked up:
- **BBCMicro / AcornElectron** — Acorn DFS (the flat catalog FS) not in
  the plan; only ADFS is covered via Archie.
- **AcornAtom** (Atom DOS) — niche, omitted from §1 + §5 of the plan.
- **SAM-Coupe** (SAM DOS / MasterDOS) — omitted.
- **ZX-Spectrum native FS** (TR-DOS / G+DOS) — only FAT + +3DOS are
  covered. The native floppy FS isn't in scope yet.
- **PET2001 D80/D82** — included in CBM Wave 3 but the `cbm` crate
  doesn't cover those formats; needs an extra slice.
- **§0 vs §4 reconciliation** — §0 says "Add/Delete is the target for
  every format" but §4 marks ~9 floppy cores as "write deferred". Decide
  which is right (only ColecoAdam EOS has a legitimate read-only reason
  — the core itself is RO).

---

## 6. CLI / GUI

(Section currently empty — Mac archives GUI polish closed 2026-06-02,
see §10. Reopen when new CLI / GUI work surfaces.)


---

## 7. User-side verification (not coding)

### 7.NH — Need help (user research blocking, surfaced 2026-06-05 MiSTer session)

Two MiSTer Wave-2 manual-verify attempts each hit a wall that **isn't a
rusty-backup issue** but needs the user to source / research the right
real-hardware-side software before the engine work we shipped can be
demonstrated in-emulator. Both blockers are well-scoped; both have
proven engine paths host-side that can substitute as evidence for the
spine rows. Captured here so the next session knows what's pending the
user's research.

**X68000 — need a Human68k system with the SCSI driver loaded**

- The MiSTer X68000 core boots from `boot3.vhd` (16 KB stub on the
  SD; valid SASI signature `\x82w68000W` but no real Human68k
  install). With FDD0 = `BLANK_disk_X68000.D88` the floppy boots
  Human68k v3.0 fine — but the floppy's CONFIG.SYS doesn't include
  a SCSI device driver line (typically `DEVICE = \SYS\SCSIROM.SYS`
  or similar), so even with a valid SCSI HDF mounted in the SASI
  slot, Human68k can't see it as a drive letter.
- Tried `hd0.hds` (the real 100 MB Sharp SCSI image from
  `C:\Temp\hd0.zip`): byte-0 has `X68SCSI1` signature so the SASI
  BIOS *should* recognise it, but without a SCSI driver in CONFIG.SYS
  there's still no drive letter.
- Tried `Bomberman.HDF` (real 10 MB Sharp self-bootable game disc):
  IPL menu auto-boots straight into the game; no shell stop.
- **What the user needs to source:** either (a) a Human68k system
  floppy whose CONFIG.SYS already loads the SCSI driver — the
  `Human 68k v3.02 (1993)(Sharp - Hudson)[a].dim` in `C:\Temp\hd0.zip`
  is a strong candidate (matched-pair release with `hd0.hds`, would
  need `.dim → .D88` conversion); or (b) a known-good HDF with
  Human68k + SCSI driver pre-installed and editable from outside.
- **What rusty-backup already validates** without this:
  `tests/cli_x68000.rs` round-trip on the synthetic D88 fixture +
  `tests/x68000_resize.rs` reconstruct → resize → re-open + the
  Wave-2 spine close-out commit. Engine surface is complete.
- Separate but related: the OPEN-WORK §8 entry "X68000 SASI / SCSI
  HDD format gaps" captures the *code-side* follow-ups
  (signature detection, sector-size derivation, IPL menu builder).
  Closing those would land a self-bootable HDF that **doesn't need**
  a working CONFIG.SYS workflow — a separate path to the same end.

**QL — need a QXL-driver-capable system ROM**

- The MiSTer QL setup ships `boot.rom` (stock Sinclair JS) and
  `minerva+qlsd_ql.rom` (Minerva + Marcel Kilgus's QLSD SD-card
  interface). QLSD exposes `sdc1_` for SD-card-image format,
  **NOT** `win1_` for QXL.WIN.
- Our `rb-cli put`-emitted file is a canonical QXL.WIN container —
  byte 0 = `QLWA`, header layout matches `ql-filer`'s
  `WinHeader.java` (`DISC_IDENTIFIER = {'Q','L','W','A'}`,
  `SECTOR_LENGTH = 512`, `HEADER_LENGTH = 64`) and our
  `src/fs/qdos.rs` documentation byte-exact. Three independent
  references confirm: sQLux byte-truth oracle (passing host-side),
  `ql-filer` `WinHeader.java`, our own engine. Format is canonical.
- For the MiSTer QL core to surface our file as `win1_` it needs a
  ROM with a QXL / Gold Card / Trump Card / Super GoldCard driver —
  not what `minerva+qlsd_ql.rom` provides. With QLSD loaded, the
  user types `DIR win1_` and gets "not found" because no driver
  bound that device prefix; under `sdc1_` it might attach but as an
  SD-card-image format (not QLWA).
- **What the user needs to source:** a QXL.WIN-aware QL system ROM
  for MiSTer. Candidates from QL community archives:
  Trump Card + Minerva, Super GoldCard ROM, or kilgus's
  `qlmister.rom` variant that bundles the QXL driver
  (referenced in `docs/mister_filesystem_implementation_plan.md`
  Wave-2 session log but not committed in tree).
- **What rusty-backup already validates** without this:
  sQLux byte-truth oracle (the kilgus headless harness has
  `rb-cli put → SuperBASIC COPY → host file diff`, byte-exact, per
  the Wave-2 close-out commit). Engine + container format are
  complete.

**Common shape**: both gaps are "MiSTer real-hardware setup needs
software rusty-backup doesn't and shouldn't ship" — the
ROMs/drivers in question are proprietary or community-distributed
and live outside our scope. The user-side ref / write-verified parks
on the Wave-2 spine rows stay `[!]` until the user runs these tests
with the right setup; the engine-level surface is independently
proven via the host-side oracles.

Move these to the right §X8 sub-bucket once the user has gathered
research or sourced the needed ROMs/disks.

---

### 7.0 — Original user-side verification list (pre-2026-06-05)

- **HFV in BasiliskII / MAME** — boot/mount our blank + cloned HFVs
  against the bootable samples (`Mac OS 8.1.HFV`, `Starterdisk.hfv`).
  Our blank/cloned volumes fsck clean and round-trip byte-identically,
  but in-emulator mount is unverified.
- **HFV restore filename default** — optional nicety: default the
  restore output filename to `.hfv` when the backup was a `None`-table
  HFS. Round-trip works without it.
- **CD CHD ISO9660 browse** — open a CD CHD (game ISO ripped to CHD) in
  Browse, navigate the ISO9660 tree, extract a file, SHA256-match
  against the same file extracted via mounting the source ISO.
  Implementation is in tree (`CdCookedReader` in `src/rbformats/chd.rs`,
  `ImageFormat::ChdCdCooked` routing in `src/rbformats/mod.rs`); just a
  user-side sanity check.
- **MacPlus MFS in BasiliskII** — boot a real System 1.0 / 2.0 MFS
  floppy from a public archive (macintoshrepository.org has many)
  using BasiliskII + a Mac Plus ROM, then round-trip via `rb-cli get`
  + `rb-cli put` on the same image and confirm BasiliskII still mounts
  it cleanly with our changes. No Linux apt tool produces MFS, so this
  is the canonical reference cross-check. Engine + write-path are
  shipped (14 unit + 4 cli tests, all green).
- **Apple-II DOS 3.3 cross-check via a2kit** — `cargo install a2kit`
  (or vendor as a workspace dev-dep), produce a known disk via a2kit's
  CLI, round-trip files through our `rb-cli get` / `put` and confirm
  byte-for-byte identity with a2kit's view. Also useful: open one of
  our `rb-cli`-written images in CiderPress2 (Windows) to confirm the
  Apple-II-era tools accept our output. Engine + write-path are
  shipped (18 unit + 5 e2e + 4 cli tests, all green).
- **MiSTer X68000 core boot test** — workflow A+C+B per
  `docs/mister-deployment-testing-plan.md` §3.4. Build a 1.2 MB
  Human68k floppy via `rb-cli put` against the synthetic disk
  (engine ships full Add/Delete), `scp` to
  `/media/fat/games/X68000/`, mount in the core, `dir A:` lists
  the written file, `TYPE foo.txt` reads back the seeded bytes.
  Reverse (workflow B) — `save` from Human68k, power down, pull
  SD, `rb-cli ls / get` agrees. Engine surface is shipped (10 unit
  + 1 e2e dispatch test); only the real-hardware mount + boot is
  outstanding.
- **MiSTer Archie / ADFS core boot test** — workflow A+C per
  `docs/mister-deployment-testing-plan.md` §3.5. Build a 800 KB
  E-format ADFS disc with the synthetic-fixture pattern, mount as
  `:0`, `*CAT :0` lists the file, `*Type HELLO` reads back the
  seed. Engine surface shipped (5 unit + 1 e2e + 3 cli); only
  real-hardware boot outstanding. **ADFS read-side FSM walker
  landed 2026-06-04** — the formula was the Linux kernel's
  published `adfs_map_layout` macro applied to the *correctly*
  parsed disc record. Previous-session puzzles unwound:
  - **dr.root split was wrong.** The kernel's `__adfs_block_map`
    (`fs/adfs/adfs.h`) splits indaddr as
    `frag_id = indaddr >> 8`, `block += ((indaddr & 0xFF) - 1) <<
    log2sharesize`. CROS42's `dr.root = 0x243` is therefore
    `(frag_id = 2, indaddr_lo = 0x43)` — `ADFS_ROOT_FRAG = 2`,
    the same constant E-format uses. Not "frag 579 zone 2".
  - **`log2bpmb` was misnamed.** Disc-record byte 0x05 is
    log2(bytes per map bit) (kernel `log2bpmb`), not
    `log2_map_bits`. `map2blk = log2bpmb - log2_secsize` is the
    shift that converts a fragment's map-bit count to a sector
    count. For CROS42 log2bpmb=12, sector=9, map2blk=+3 (1 map
    bit = 8 sectors = 4096 B).
  - **FSM is at middle of disc (kernel formula was right).**
    `map_addr_sec = signed_asl((nzones>>1) * zone_size_bits -
    ((nzones>1) ? 480 : 0), map2blk)`. For CROS42 (33 zones,
    zone_size_bits=3986, map2blk=+3): map_addr_sec = 506368 =
    byte 0xF740000. Matches.
  - **Resolved end-to-end:** `frag 2`, zone 16, start_bit 40 →
    result = 40-32+63296 = 63304 → sector 506432 → byte
    0xF748000 → +secoff (2) → **byte 0xF748400**, where the
    Hugo magic sits exactly. Same formula resolves ICEBIRD's
    root and arc-04's E-format root (`dr.root=0x203` →
    `(frag_id=2, indaddr_lo=3)` → bit 528, length 32 →
    sector 2 → byte 0x800 Nick). Cross-verified by
    `examples/adfs_fsm_probe.rs`.
  - **Walker shipped** (`src/fs/adfs.rs::AdfsFsm`): full DR
    parser (60 bytes, including `log2bpmb`, `log2sharesize`,
    `nzones_high`, `disc_size_high`, `format_version`,
    `root_size`), `map_lookup(indaddr, block)` mirroring
    `__adfs_block_map` + `adfs_map_lookup`, `free_bytes()`
    mirroring `adfs_map_statfs`. Wired into `list_directory`
    (root + subdirs) + `read_file` for E/F/HD; D-format keeps
    direct-byte addressing (old-map). Hugo + Nick directory
    magics treated interchangeably (the layout is identical
    for 26-byte-entry F-format dirs on this read path).
  - **End-to-end byte-truth confirmed (read side):**
    `rb-cli ls 'C:\Temp\CROS42.hdf'` returns the full root
    (!Boot / Apps / Comms / Develop / Documents / Emulator /
    Emulators / Files / Games / Media / Printing / ReadMe
    (FILE 607 B) / Swap / Utilities / Utils — exact match to
    the 2026-06-04 RPCEmu-mount ground truth). Subdirs descend
    too: `rb-cli ls 'C:\Temp\CROS42.hdf' Apps/!Clock` lists
    !Help / !Run / !RunImage / !Sprites / !Sprites22 /
    !Sprites23 / Messages. File extraction byte-exact:
    `rb-cli get 'C:\Temp\CROS42.hdf' Apps/!Clock/!Help` pulls
    the 363-byte help text verbatim. ICEBIRD lists
    ClassicROS / Demos / Diskmags / Tools; arc-04 lists
    1_DataComm.
  - **End-to-end byte-truth confirmed (write side):**
    `rb-cli put cros42.hdf foo.txt MyTest` adds the file via
    `EditableFilesystem`; `rb-cli get` round-trips byte-exact;
    `rb-cli rm` removes it cleanly; existing files
    (Apps/!Clock/!Help) stay byte-identical across the
    put+rm cycle. Implementation honours the kernel
    `scan_free_map` chain: bit-8 freelink delta, predecessor
    pointer rewriting on carve.
  - **Still open:**
    1. **HDD resize.** Same-nzones grow is bounded by FSM
       geometry — CROS42 can grow 512 MB → 537 MB without
       crossing nzones (33 zones × ~15.6 MB each = 514 MB
       theoretical, with some clamp slack). Anything past
       that requires **FSM relocation**: `map_addr` lives at
       `(nzones>>1) * zone_size_bits` shifted by `map2blk`,
       so growing nzones moves the physical FSM forward by
       up to several MB. The bytes at the new map_addr are
       part of the existing data area, so resize would need
       to walk every fragment, identify allocations that
       overlap the new map_addr region, and relocate them
       (updating dir entries' indaddrs). No byte-truth
       oracle exists (Linux's `ADFS_FS_RW` flags itself
       DANGEROUS / experimental, and RISC OS's resize tool
       runs only inside the OS). **Deferred** until a
       maintainer with an RPCEmu / real-Archie boot loop is
       willing to validate. Workaround for grow-CF-card:
       restore byte-for-byte, then `*AddDrive` inside
       RISC OS.
    2. **Variable-size F+ directories** (`format_version != 0`,
       `root_size` from DR). Walker handles size, but the
       big-dir entry format (`adfs_bigdirentry`, 8-byte
       indaddr, longer names) needs its own parser. Not
       observed yet on our samples.
    3. **Old-map (D-format) FSM** still uses the
       "indaddr = byte offset" fallback. No D-format real
       sample to validate against. Reopen if one surfaces.
    4. **Multi-deep freelink chain.** Today `find_chain_head`
       only carves from the chain root. CROS42 / ICEBIRD
       zones we've inspected carry a single chain entry per
       zone, but a heavily-used disc could grow longer
       chains; allocating from inner entries would need the
       walker to thread the predecessor pointer through the
       chain. Not blocking for the current MiSTer
       distribution workflow.
  - **Samples on disc** (all confirmed in the 2026-06-04 probe):
    - `C:\Temp\CROS42.hdf` (512 MB populated HD-format, TOSEC
      Acorn) — dr.root=0x243, frag 2 zone 16 bit 40 → byte
      0xF748400 (Hugo). Verified via probe.
    - `C:\Temp\ICEBIRD.hdf` (512 MB populated HD-format) —
      identical DR shape to CROS42; same address resolves.
    - `C:\Temp\adfs_arc04_e_orig.adf` (800 K E-format
      populated, from 8bs.com/pool/arc/arc-04.zip) — frag 2
      zone 0 → byte 0x800 (Nick). Verified via probe.
    - `C:\Temp\adfs_blank256E.hdf` (256 MB blank E-format HD).
    - `C:\Temp\adfs_blank1024Eplus.hdf` (1 GB blank E+ format HD).
- **MiSTer QL core boot test** — workflow A+C per
  `docs/mister-deployment-testing-plan.md` §3.6. Build a QXL.WIN
  hard-disk image, place at `/media/fat/games/QL/win1_`, boot
  the QL core, `DIR win1_`, `LOAD win1_HELLO` reads the seed.
  Engine surface shipped (20 unit + 2 e2e + 5 cli — including
  write); the write path passes a headless sQLux byte-truth oracle
  (rb-cli put → sQLux SuperBASIC COPY → host file round-trips
  byte-exact, per-file 64-byte QDOS header convention honoured).
  Only the real-hardware mount + boot is outstanding. `.mdv`
  microdrive container (per-sector 60-byte records) deferred.
- **MiSTer BK0011M ANDOS core boot test** — workflow A only per
  `docs/mister-deployment-testing-plan.md` §3.8. Mount one of our
  ANDOS-scaffolded volumes and visually confirm the core boots
  (full read path is parked behind the boot-test oracle).
- **`.d88` container decoder** — `tests/fixtures/anchor_mister_
  BLANK_disk_X68000.D88.zst` is a real Sharp `.d88` from the MiSTer
  X68000 distribution waiting for a consumer. The format is a 32-byte
  header + per-track LE-offset table + per-sector blocks. Once landed
  in `src/rbformats/containers/d88.rs`, the existing Human68k engine
  (read + EditableFilesystem write, all shipped) validates against
  the BLANK byte-for-byte. Probably a 1-commit slice. Same decoder
  also unlocks the X68000 ZIP (1.5 GB of real disks already on the
  MiSTer SD card, not committed).
- **`.mdv` QDOS microdrive reader** — `tests/fixtures/anchor_mister_
  {GamesCart,crazy}.mdv.zst` are real QL microdrive cartridges from
  the MiSTer QL distribution. The format is per-sector record
  headers (visible in the first sector as `MD` / `Test` 8-byte cart
  names) — completely separate from the QXL.WIN HDD format
  `src/fs/qdos.rs` currently implements. Lives in *QDOS Reference
  Manual* ch. 12; ~300 LOC engine + tests against the two anchored
  fixtures.
- **AcornAtom — already MiSTer-ready via existing FAT**
  (reconciles the §5 plan gap noted as "Atom DOS niche / omitted").
  The MiSTer AcornAtom core uses a 100 MB FAT-formatted .vhd as the
  SD-card backing, not native Atom DOS. We **already fully support
  that**: `scripts/build_atom_vhd.sh` packs the full
  `hoglet67/AtomSoftwareArchive` V13.00 (5311 `.atm` files, 16 MB
  raw) into a fresh FAT16 VHD via `mkfs.fat` + `mtools`, and our
  existing FAT engine reads any `.atm` from it byte-for-byte
  identical to the source. The packed VHD is on the user's MiSTer at
  `/media/fat/games/AcornAtom/AtomSoftwareArchive_V13.vhd`. **No
  Atom DOS engine work is required for MiSTer scope.** A future
  `src/fs/atom_file.rs` for `.atm` metadata decoding (load/exec
  addresses, AGD detection) would be a content-aware nicety but is
  not on the spine.

Items that have a real shape but no schedule. Surface them here so they
aren't lost.

**WSL toolchain — verified 2026-06-03 on `Ubuntu-24.04`:** to avoid
re-litigating setup, the following is confirmed working end-to-end
(appliance smoke-tested, not just `dpkg -l` presence checked):
`mkfs.jfs` (jfsutils 1.1.15), `guestfish` (libguestfs 1.52.0),
`linux-image-virtual` + `linux-modules-extra-6.8.0-124-generic`,
`/boot/vmlinuz-*` 0644 via `dpkg-statoverride`, `zstd`, `mkfs.xfs` /
`xfs_repair` / `xfs_db` (xfsprogs 6.6.0). The minimal-image appliance
boots and round-trips reads/writes; `xfs_repair -n` runs clean on the
bundled v5 fixture. **Implication for the entries below:** any park
reason that names "tooling unavailable" or "no oracle" is wrong —
both fixtures and oracles are accessible from this host.

### X68000 SASI / SCSI HDD format gaps (scoped 2026-06-05 via real-disc probes + IPL ROM disasm)

Today's MiSTer manual-verify session surfaced concrete gaps between
our `partition/x68k.rs` + `fs/human68k.rs` and real Sharp X68000 HDDs.
All five anchored against probed bytes and the disassembled IPL ROM —
no hand-waving.

**Reference material in tree** (sibling repos, already cloned):
- `../x68kd11s/iplrom/iplrom30.s` — disassembled X68030 IPL ROM. Two key citations:
  - **Line 4761 `Lff09e8`**: SASI HDD descriptor template starting
    `\x82w68000W\x00` followed by 31 bytes of geometry / partition
    pointers. Exact field layout we'd map.
  - **Line 16420 `Lff9488`**: SCSI signature check —
    `cmpi.l #$53435349,(-$0014,a0)` (= `'SCSI'` at offset -0x14 from
    boot-device pointer). `X68SCSI1` at byte 0 of `hd0.hds` matches.
- `../dis68k/lib/libfat-human68k/ff.c` — Human68k-port of ChaN FatFs:
  - **Line 2160**: `0xAA55` boot signature check commented out.
  - **Lines 421–432**: standard BPB field offsets (still 8-byte OEM —
    Hudson's 18-byte OEM is achieved by stuffing the extra 10 bytes
    into the JMP/code area before the BPB fields).
  - `_MAX_SS` / `_MIN_SS` + `SS(fs)` macro: variable sector size when
    `_MAX_SS != _MIN_SS`.
- `../X68000_MiSTer/Doc/diskemu.txt` + `rtl/diskemu/sasidev.vhd` —
  describes the core's emulation architecture; the signature-checking
  firmware runs on a Nios II soft-CPU compiled into
  `diskemu_mainmem.hex` (binary, not source).

**Real-image anchors (locally available, byte-probed today):**
- `/c/Temp/Bomberman/games/X68000/media/Bomberman/Bomberman.hdf`
  (10,441,728 B = 40788 × 256). Byte 0: `\x60\x00\x00\xca` (68000
  BSR.W boot-IPL opcode) + `\x1b[6;32H X68000 HARD DISK IPL MENU` ASCII
  at byte 0x40. X68K partition table at byte 0x400 with one Human68k
  entry at start_lba 33. Sector size **256 B**. Custom Hudson Soft
  game-data format inside the partition (NOT readable FAT).
- `/c/Users/spam/AppData/Local/Temp/mister-tests/hd0.hds`
  (104,857,600 B = 102400 × 1024). Byte 0: `X68SCSI1\x02\x00\x00\x03\x1f\xff\x01\x00`
  + ASCII `Human68K SCSI-DISK by Keisoku Giken`. IPL boot code at byte
  0x400, X68K partition table at byte 0x800 (= sector 2 in 1024-B
  units). Sector size **1024 B**. Real Human68k FAT12 partition at
  byte 0x8000, BPB starts with `\x60\x24SHARP/KG    1.` (18-byte OEM).
- `/media/fat/games/X68000/boot3.vhd` original stub
  (16,384 B). Byte 0: `\x82w68000W\x00\xc0\x00\x00\x00\xfe\x4f\xfc`.
  Real SASI signature + 8 bytes of geometry / capacity descriptor.

**Concrete code gaps (in dependency order):**

1. **`partition/detect_superfloppy`** (`src/partition/mod.rs:139`)
   accepts FAT VBR JMP byte `0xEB` / `0xE9` only. X68000 floppies
   start with `0x60` (68000 BSR.S). Result: `rb-cli inspect/ls` on a
   raw X68000 `.D88` decode fails detection even though the BPB is
   valid (integration tests pass because they call the engine
   directly).

2. **[RESOLVED for SCSI read]** `partition/x68k.rs` hardcoded 512-byte
   sectors when computing `partition_offset = start_lba *
   SECTOR_SIZE`. Reality: SASI HDDs use 256-B sectors, SCSI HDDs use
   1024-B sectors. With the old math: probed offsets are off by 2×
   (Bomberman) or 0.5× (hd0.hds) from where the FAT BPB actually lives,
   causing `bytes_per_sector 0 not in {256,512,1024,2048}` on
   `rb-cli ls hd0.hds@1`. **Fixed**: `x68k::detect_sector_size` reads
   the boot signature and `PartitionTable::X68k` now carries
   `sector_size`; `to_partition_info` normalizes `start_lba` /
   `size_bytes` into 512-byte LBA units so every `start_lba * 512`
   consumer lands on the real partition. Verified read/browse/extract
   end-to-end against the BlueSCSI SxSI v3.02 `HD10/20/30/40_512.hda`
   set (1024-B SCSI). **Resolved (slice 4 + follow-up, 2026-06-07).**
   Partition-table side: `patch_x68k_entries` takes the disk's logical
   `sector_size`, and the `rbformats/mod.rs` X68k reconstruct branch
   derives it from metadata to fix `new_sectors`, the `disk_size_field`,
   and the table offset (0x400 SASI / 0x800 SCSI). Partition-*data* side:
   `PartitionMetadata` now persists `start_byte` (the true non-512-aligned
   byte offset), the backup engine reads at `PartitionInfo::byte_offset()`,
   and the reconstruct loop + restore sites write/resize at
   `PartitionMetadata::byte_offset()`. Verified on real 256-B SASI game
   disks (Populous / Lemmings / SSF2 / Votoms) — backup→restore now lands
   the partition region byte-identical at byte 8448, and in-place
   `rb-cli resize` is byte-exact on SASI too.

3. **[RESOLVED for read]** `partition/x68k.rs` geometry detection.
   `X68kPartitionTable::detect_with_geometry` now probes both table
   offsets and derives the sector size: `X68SCSI1` -> (0x800, 1024);
   `\x82w68000W` -> (0x400, 256); otherwise it tries (0x800, 512) then
   (0x400, 256). The table *location* is the reliable SASI marker —
   real SASI game disks (Bomberman) carry a custom IPL with no Sharp
   signature but put the `X68K` table at 0x400 with 256-B sectors. The
   non-512-aligned partition offset that 256-B sectors produce is
   carried through `PartitionInfo::start_byte` / `byte_offset()`.
   Verified read/browse/extract against `Bomberman.hdf` and the full
   `~/Downloads/X68000-fixtures` set (BlueSCSI / ZuluSCSI / HDS /
   Henkan Bancho / SCSI2SD `.ima`). *Still open*: we don't reject
   non-Sharp HDDs that coincidentally carry the `X68K` magic. (The SASI
   256-B partition-*data* offset across backup / restore / reconstruct is
   now handled via the persisted `PartitionMetadata::start_byte` —
   resolved 2026-06-07.)

4. **[RESOLVED]** `fs/human68k.rs` FAT BPB parser now handles the Sharp
   / Keisoku Giken SCSI-HDD BPB: a 2-byte BRA.S + 16-byte OEM
   ("SHARP/KG    1.00") with **big-endian** fields at offset 0x12.
   Layout verified byte-for-byte against HD10/HD20 (see
   `Human68kBpb::parse_sharp_kg`). The standard little-endian FAT BPB
   path is preserved for floppies. Note `dis68k/lib/libfat-human68k`
   sidesteps this entirely by replacing sector 0 with a synthetic
   standard boot sector (`FS_REPLACE_SECTOR0` / `fake_bootsect`); we
   parse the real on-disk BPB instead.

5. **No self-bootable IPL builder** in `examples/build_x68k_hdd.rs` —
   the example produces a partition-table-only HDD that requires
   booting from FDD0. Real-world workflow is HDD-bootable. The IPL
   menu pattern (Bomberman: BSR.W → ANSI-positioned menu → boot
   selected partition) is short — ~200 bytes of 68000 assembly.

**Implementation estimate**: ~500 LOC + tests against `hd0.hds` and
`Bomberman.HDF` as committed anchors. Phases: (a) signature detector +
sector-size derivation in `partition/x68k.rs`; (b) extended-OEM BPB
parser in `fs/human68k.rs`; (c) `examples/build_x68k_hdd.rs` upgrade
to emit a SCSI-format bootable HDD with IPL menu; (d) `rb-cli put`
round-trip against an injected `hd0.hds` clone on a MiSTer with
Human68k v3.02 system disk.

**What's blocked behind this**: closing the X68000 row's `[!] ref` +
`[!] write-verified` user-side parks via real-hardware MiSTer
verification. Today the engine-level surface is proven; the
in-emulator surface needs the SCSI/SASI format work above.

### 8a. Full read/write support — progress + remaining plan (2026-06-07)

Driven by the real `~/Downloads/X68000-fixtures` set (BlueSCSI / ZuluSCSI
/ HDS / Henkan Bancho `.hda`/`.hds`, `Bomberman.hdf` SASI, SCSI2SD `.ima`).
Goal stated by the user: create / delete / extract files, change partition
sizes, and clone the filesystem — **full support**.

**DONE (uncommitted on `main`):**
- **Read** — every fixture inspects / browses / extracts. SHARP-KG
  big-endian BPB parser (`Human68kBpb::parse_sharp_kg`), geometry
  detection (`X68kPartitionTable::detect_with_geometry`), and the
  `PartitionInfo::start_byte` / `byte_offset()` accessor for the
  non-512-aligned SASI offset.
- **🐛 Big-endian FAT fix** — the FAT table is big-endian on real X68000
  disks (dir entries stay LE). `Human68kBpb::fat_big_endian` gates
  `fat_lookup` / `fat_set` (FAT16). Was silently truncating every file
  > 1 cluster on *both* SCSI and SASI. Regression test
  `reads_big_endian_fat_chain_across_clusters`.
- **Slices 1–3 (file management)** — `create_file` / `create_directory` /
  `delete_entry` are subdir-aware (helpers `dir_slot_offsets`,
  `find_free_slot_in_dir`, `grow_dir_chain`, `find_entry_slot_in_dir`,
  `dir_is_empty`); recursive delete uses the trait default over the new
  subdir-aware `delete_entry`. Verified via CLI `mkdir`/`put`/`get`/`rm`
  on a Bomberman copy + multi-cluster byte-exact round-trip.
- Picker: `.hds` + `.ima` added to `DISK_IMAGE_EXTS`. Docs (README
  formats + per-core tables, `full_MiSTer_support_status.md`) updated.
- Unrelated build-blocker fixed: `completions::bin_stem` (Windows-path
  basename failed on Unix).

**Slice 4 — resize — DONE (2026-06-07):** `resize_human68k_in_place`
(`src/fs/human68k.rs`) is the Human68k peer of `fs::resize_fat_in_place`:
it accepts the `0x60` BRA.S jump, reads the big-endian SHARP/KG BPB at
`0x12` via `Human68kBpb::parse`, and reads/writes the FAT big-endian
(FAT16). Grow extends the FAT (shifting root+data forward) and bumps
`total_sectors`; shrink keeps the FAT size so every cluster keeps its byte
offset (existing files stay byte-exact) and refuses to drop below the FAT16
floor. Wired into `fs::resize_filesystem_for` (so `rb-cli resize` + GUI +
the reconstruct-time call all route there) and the backup-folder restore
site in `src/restore/mod.rs`. `patch_x68k_entries` now takes the disk's
logical `sector_size` (1024 SCSI / 256 SASI / 512 synthetic), and the
`rbformats/mod.rs` X68k reconstruct branch derives that sector size from
metadata (`original_size_bytes / length_sectors`) to fix the `new_sectors`
length math, the `disk_size_field`, and the table offset (0x400 SASI vs
0x800 SCSI). Verified byte-exact on the BlueSCSI HD10 fixture across a
grow→shrink round-trip (multi-cluster `COMMAND.X` with `HU` header +
`HUMAN.SYS` + Japanese filenames all survive).

**256-byte-SASI partition-data offset — DONE (follow-up, 2026-06-07).**
Once real 256-byte SASI game disks with readable Human68k FAT volumes
landed in the fixture set (Populous / Lemmings / SSF2 / Votoms — partition
at sector 33 = byte 8448, non-512-aligned, with a Sharp/KG big-endian BPB
declaring 1024-byte FS sectors), the engine paths were converted off the
floored `start_lba * 512`: `PartitionMetadata` gained a persisted
`start_byte`, the backup engine reads at `PartitionInfo::byte_offset()`,
and the reconstruct loop + restore sites write/resize at
`PartitionMetadata::byte_offset()` (falling back to `start_lba * 512`
when `start_byte` is absent, so old backups round-trip unchanged).
Verified: in-place `rb-cli resize` grow+shrink byte-exact on SASI, and
backup→restore lands the partition region byte-identical at byte 8448
(`tests/x68000_resize.rs::x68k_sasi_256byte_partition_round_trips_at_true_byte_offset`).

**Slice 5 — defragmenting clone — DONE (2026-06-07):**
`create_blank_human68k` + `Human68kFormatTemplate` + `format_template()`
in `src/fs/human68k.rs` format a fresh FAT16 volume matching a source's
conventions (sector size, FAT count, root entries, endianness, OEM/boot
stub), re-patching only total-sectors + sectors-per-FAT. The new
`src/fs/human68k_clone.rs` hosts `clone_human68k_volume` (DFS walk +
contiguous replay, peer of `clone_pfs3_volume`) and
`stream_defragmented_human68k` (tempfile-backed streaming wrapper).
FAT16-only (FAT12 floppies use the floppy-container converter);
read-only attr / mtimes aren't carried over (same as the interactive
edit path) and surface as a warning. Wired into the CLI as `rb-cli
repack IMG[@N] [--size]` (in-place defragment; defaults to the FS's own
declared size, not the partition-table length, so 256-byte-SASI disks
work) and into the GUI as a per-partition "Defragment…" button (Inspect
tab, image-file sources only) backed by `model::repack_runner`. The
in-place resizer can only trim trailing free space (it keeps cluster
byte-offsets); the clone reclaims holes left by deleted/relocated files.
Validated end-to-end on the real `Populous.hdf` 256-byte-SASI fixture
(tree identical, POP.X / LOAD.DAT / HUMAN.SYS byte-exact, X68k table
intact). **This closes the X68000 full read/write/resize/clone goal.**

- **GUI/CLI parity:** the write slices flow through `cli/resolve.rs`
  (`byte_offset()`) and `gui/inspect_tab.rs` (converted) — both work for
  SASI today. Confirm the GUI browse-edit toolbar exposes mkdir / add /
  delete for Human68k partitions.

Reference fixtures live at `~/Downloads/X68000-fixtures`; reference C
sources at `~/repos/dis68k/lib/libfat-human68k` (FatFs port; fakes
sector 0), `~/repos/x68kd11s` (Human68k 3.02 source). See memory note
`x68000_scsi_hdd.md`.

---

## 9. Explicitly excluded

Out, not parked. Listed so the question doesn't get re-litigated.

- **StuffIt X / `.sitx`** — licensing.
- **SIT methods 6 / 8 / 14** — XADMaster itself flags as "interesting /
  partial".
- **SIT method 13 compressor (writer)** — stored / RLE90 covers
  preservation; no need for the compressor side.
- **Resume support for partial backups/restores** — explicitly deferred.
- **Partition-scoped device restore** (`/dev/disk3s2` style) —
  explicitly deferred.
- **Disk-expansion v1 out-of-scope**: shrinking via partition-table
  edits, shifting non-trailing partitions, RDB expand, per-partition
  free space.
- **In-place XFS / btrfs shrink** — `xfs_repair`-scale risk; the
  clone-into-fresh paths replace them.
- **23 N/A MiSTer cores** — tape / cartridge / ROM-only; no filesystem
  to support. See `full_MiSTer_support_status.md`.
- **`a2kit` / `cbm` / `fluxfox` as dependencies** — see the MiSTer plan
  §2 for the per-crate rationale; all are reference ports, not crates.
- **In-place btrfs balance / shrink** — replaced by §2.4
  scratch-recreate (gated on B.0 decision).
- **JFS J.4b — edit-side write primitives** (multi-month surface:
  xtree-write + dtree-write + dmap-write + IAG alloc/free). J.4a
  fsck shipped, so the read-side audit + OrphanInode reporting are
  in tree. The only thing J.4b would unlock is `repair()`-driven
  adoption of OrphanInode findings into `/lost+found/`. Real-world
  JFS orphans are rare (single-user vintage volumes don't accumulate
  the dangling-inode patterns big-multi-user systems do), and on
  the rare hit the user can route through Linux `fsck.jfs` to
  resolve. Investment / payoff ratio doesn't justify the multi-
  month surface. **Closed indefinitely** — reopen only if a real
  user workflow surfaces meaningful orphan volumes that fsck.jfs
  can't handle.
- **XFS rmapbt rebuild / refcountbt rebuild** (R2 / R3 fully safe
  on rmapbt-enabled v5 images). Slices 1–6 of the v5 unlock shipped
  (see §10): finobt + rmapbt + refcountbt parsers/walkers, R2 v5
  gate lift, R3 finobt resync. R2 / R3 silently skip on
  `has_rmapbt() || has_reflink()` to avoid leaving side-tree records
  stale. Making rmapbt-rebuild itself work is a separate ~500–800
  LOC slice: walk rmapbt to find records over old tree blocks,
  emit new records pointing at the new ones, update AGF rmap
  counters. Same shape for refcountbt when reflinks are actually
  in use. **Closed indefinitely** — the vintage / single-disk
  retro-restore target rarely hits `--repair` on modern v5 images
  with these features, and when it does the user can route through
  `xfs_repair` in WSL. Reopen only if a real workflow needs in-app
  v5+rmapbt repair coverage that the WSL fallback can't provide.
- **JFS multi-level dmapctl walker** (read-side, BMAP at
  `MAX_BMAP_DEPTH_1_BLOCKS` = 32 GiB cap). Vintage hardware doesn't
  address a multi-TiB JFS aggregate; the rusty-backup target is
  CF / SD card / single-disk retro-restore workflows where 32 GiB
  is already an outlier. `walk_bmap` returns `Ok(None)` past the
  cap and falls back to the conservative full-partition extent —
  backups are still correct, only compaction efficiency is hurt.
  Lift would require implementing the L0/L1/L2 dmapctl hierarchy
  walk (separate from the xtree walker that already covers BMAP's
  internal-xtree pointers). Closed-by-design rather than parked.
- **External `chdman`** — replaced by the in-tree MAME CHD core via
  `libchdman-rs`. The Windows-host elevation rework removed the
  separate `chdman.exe` dependency.
- **BK0011M ANDOS write / extract / resize** — kept as **detect-only
  forever**. The Soviet-era ANDOS filesystem on the BK0011M MiSTer
  core has no English-language layout spec, no community-maintained
  reader / writer to validate byte-truth against, and a vanishingly
  small target audience among retro enthusiasts using rusty-backup.
  Our scaffold detects an ANDOS-shaped volume (the four canonical
  sector-0 signatures landed during CLI parity work — see
  `detect_superfloppy`) and routes it to the engine, which surfaces
  a clean `Unsupported` for read / write / resize. That's enough
  for `rb-cli inspect` to report "ANDOS detected" without crashing.
  Spine row 5 (BK0011M) is therefore complete at the detect floor.
  Reopen only if (a) a credible spec turns up AND (b) someone steps
  forward as the maintainer for the format.

---

## 10. Recently closed (2026-06-02 sweep + follow-ups)

Audit trail. Each was either shipped, closed-by-design, or moved into
the structure above before its source plan doc was deleted in the
docs-consolidation pass.

- **X68000 floppy-container any-to-any conversion (XDF / HDM / DIM / D88
  — Wave 2 prereq, 2026-06-06)** — closes the third-party-tool gap for
  the X68000 row's floppy workflow. `src/rbformats/containers/floppy_geom.rs`
  hosts the four-element media set (1.2 MB / 1.44 MB 2HD, 720 KB / 640 KB
  2DD) with size-unique geometries so size-based inference is unambiguous;
  `xdf.rs` / `hdm.rs` are raw-flat passthroughs delegating to the shared
  helper; `dim.rs` handles DIFC (256-byte signed header + track-existence
  bitmap, zero-fills missing tracks on decode, all-tracks-present on encode)
  and a generic "256-byte header + flat payload" fallback for non-DIFC
  `.dim` variants (e.g. IBM XDF DIM); DIFC has no 640 KB media byte so
  DIM-encode rejects that geometry with a clear pointer to the other
  three formats. The pre-existing `d88.rs` `encode_d88_bytes` /
  `decode_d88_bytes` already round-trip-tested; the new
  `convert_floppy_container` engine in `containers/mod.rs` is the shared
  any-to-any entry point used by every surface. Surfaces: `rb-cli floppy
  convert <in> <out>` (extension-driven), `rb-cli floppy convert <dir>
  <dir> --to <fmt> [--recursive]` (bulk), `rb-cli floppy info <path>`
  (geometry probe), `FloppyConvertDialog` in the Inspect tab (single-file
  GUI), and the existing GUI Bulk Convert dialog gets `Xdf` / `Hdm` /
  `Dim` / `D88` entries via four new `ExportFormat` variants intercepted
  in `bulk_convert_runner` and routed through the same engine. The
  inspect-tab materializer (`gui::prepare_disk_image_path`) now decodes
  `.xdf` / `.hdm` / `.dim` to tempfile so the inner FAT / Human68k
  filesystem is browsable directly. Tests: 65 container unit + 3
  integration cells (`tests/floppy_container_roundtrip.rs` covers the
  16-cell matrix, asserts DIM-DD640 rejection, and feeds a 3-file
  mixed-format batch through `start_bulk_convert` to confirm runner
  routing). Drops the `xdf2d88` Linux-tooling note from
  `docs/mister-deployment-testing-plan.md` §3.4. Unblocks the X68000
  Wave-2 spine row's floppy column.

- **AtariST MiSTer-core prereqs (§5 MiSTer plan → §10)** — first slice
  of the MiSTer-core spine. `src/partition/atari.rs` parses AHDI root
  sectors (4 big-endian 12-byte slots at 0x1C6, GEM / BGM / XGM / RAW
  IDs, optional disk-size + bad-sector + 0x1234 word-sum checksum) and
  walks XGM extended chains MBR-EBR-style — slot 0 (logical) is
  relative to the current extended root sector, slot 1 (XGM link)
  relative to the outermost container; cycle + hop + total-partition
  guards. Wires through a new `PartitionTable::Ahdi` variant gated by
  the magic-less `looks_like_ahdi_root` probe (rejects MBR signatures,
  requires at least one in-bounds entry with a recognized ID), inserted
  between RDB and superfloppy probes. `partitions()` emits synthetic
  MBR type bytes (GEM→0x01, BGM→0x06, XGM→0x05) so existing FAT12/16
  dispatch routes the partitions directly; backup emits `ahdi.json`
  sidecar; editor / single-file CHD assemble bail with clear "not yet
  implemented" messages. `src/rbformats/containers/{mod,msa}.rs` adds
  the container-decode layer (per §3.2 of the MiSTer plan): pure-Rust
  `ContainerKind` enum + `detect_container_kind` magic-first sniffer +
  `open_container_bytes` / `open_container_reader` returning a
  `Box<dyn ReadSeek>` view; MSA decoder handles the 10-byte `$0E0F`
  header + per-track payloads (uncompressed or `$E5`-RLE-compressed,
  including the `$E5 $E5 $00 $01` literal-marker encoding); strict
  spt/sides/track-range bounds + 4 MiB decoded-size cap. Wired into
  `model::source_reader::open_read` (in-memory Cursor) and GUI
  `prepare_disk_image_path` (`.msa` → `.st` tempfile) so both the
  streaming + path-based open paths see flat sectors. End-to-end test
  in `model/source_reader.rs` round-trips a FAT12-shaped 720K MSA
  through `open_read` → `PartitionTable::detect` and asserts the
  detector lands on `PartitionTable::None { fs_hint: "FAT" }`. Tests:
  11 AHDI unit + 2 partition::tests integration + 10 container/MSA +
  2 source_reader (is_msa_path + end-to-end MSA → FAT). Closes the
  AtariST prereqs row in `docs/mister_filesystem_implementation_plan.md`
  Wave 1; AtariST is now `[~]` in progress (every applicable stage
  except reference cross-check + CLI parity flips on the FAT
  inheritance). v4 only.
- **XFS R3 finobt resync (slice 5) (§8 parked → §10)** — `repair_inobt_
  leaf` now uses `sb.sblock_hdr_len()` (was hardcoded 16-byte v4
  header) and re-stamps sblock-CRC on v5, returns per-record
  `InobtRecordChange` so the caller can propagate into finobt.
  New `sync_finobt_for_changes` walks the AG's finobt with the
  generalised `walk_short_btree_blocks`, finds each change's
  start_agino key, rewrites the matching record's mask + freecount
  in place, re-stamps the CRC. Pure-update transitions only;
  0-boundary transitions refuse cleanly with `Unsupported` so the
  caller surfaces a per-AG failure entry. R3 driver's
  `has_finobt()` early skip comes down (rmapbt still gates).
  Tests added (2): direct call of `sync_finobt_for_changes` on
  the v5 fixture's AG 0 finobt verifying mask propagation + CRC
  re-stamp + on-disk bytes; refusal-path test asserting the
  zero-boundary `Unsupported` error.
- **XFS v5 side-tree walks (slices 1+2+3+4+6) (§8 parked → §10)** —
  Modern xfsprogs 6.6.0's bundled v5 fixture has
  `features_ro_compat = 0xF` (FINOBT + RMAPBT + REFLINK +
  INOBTCNT). All three side-trees now parsed and walked into the
  block-completeness map. (1) finobt: `XFS_FIBT_MAGIC` +
  `XFS_FIBT_CRC_MAGIC` constants, `XfsAgi` reads `agi_free_root` /
  `agi_free_level` at offsets 328/332, `XfsSuperblock::has_finobt()`
  /` finobt_magic()` helpers, `walk_short_btree_blocks` generalised
  from `walk_inobt` so the same descent shape drives finobt walks
  too. (2) refcountbt: parses `agf_refcount_root` / `_level` at
  offsets 88/92. On-disk magic is `R3FC` (0x52334643), not `R3C `
  (0x52334320) as older kernel headers list — verified via probe
  of the bundled fixture. (3) rmapbt: parses `agf_rmap_root` /
  `_level` at offsets 24/36 (= `agf_roots[2]` / `agf_levels[2]`).
  (4) `check_allocations` in fsck.rs now walks each side-tree per
  AG when its corresponding ro_compat bit is on; the
  `if !sb.is_v5()` gate around `UnaccountedBlocks` comes down. (6)
  R2 silently skips on `has_rmapbt() || has_reflink()` and R3
  silently skips on `has_rmapbt()` — preserves the "no-op on clean
  volume" promise without claiming to safely rebuild trees whose
  side-tree records would go stale. Tests added (2):
  `opens_modern_xfs_v5_fixture` extended to assert all three
  feature bits AND that AG 0's three side-tree roots are populated
  in `XfsAgf` / `XfsAgi`; `repair_r6_drops_dangling_shortform_
  entry_on_v5_fixture` tolerates the now-active `UnaccountedBlocks`
  finding (R6 zeroes an inode without freeing its blocks; the
  completeness check correctly surfaces those blocks as leaked).
- **JFS multi-page dtree walker (§8 parked → §10)** —
  `parse_inline_dtree` (name kept for API compat; now handles
  external dtrees too) descends BT_INTERNAL dtroots into off-disk
  dtpages. CRITICAL finding: kernel jfs_dtree.h declares dtpage's
  `nextindex`/`freecnt`/`freelist` as `__le16`, but on-disk truth
  (confirmed via probe of `/bigdir` in the regenerated fixture) is
  u8 — matching dtroot's layout. Walker uses on-disk layout: header
  is 32 bytes (one slot), u8 fields at offsets 17–21, self_pxd at
  24. Recursive descent with `DTREE_MAX_DEPTH = 16` +
  `DTREE_MAX_PAGES_VISITED = 65536` cycle/budget guards. Shared
  `parse_dtree_ldtentry` decoder + continuation-slot closure works
  for both inline-dtroot (continuation in dtroot bytes) and
  external-dtpage (continuation in same page) slot pools. Fixture
  regenerated via WSL Ubuntu-24.04's
  `scripts/generate-jfs-fixtures.sh` to add `/bigdir/file_001..200`
  past the inline 8-slot cap, forcing JFS to convert /bigdir to an
  external dtree (1 internal dtroot + 2 leaf dtpages chained via
  sibling next pointers). Tests added (2 + 6 re-pinned): full walk
  emits all 200 unique names; `read_file` round-trips file_001's
  "line 001" content after walker enumerated it; bmap / pmap / root-
  inode / used-size assertions re-pinned for new fixture geometry
  (pmap[0]=0xFFFFFFFF, mapsize=3788 nfree=3514 alloc=274,
  root.size=48 nlink=4); orphan-detection test moved to fino 34
  (extent 1) since fino 8 is now a real bigdir/file_001 entry.
  J.4a fsck `ExternalDtreeNotWalked` warning path remains as
  defence-in-depth for future fixtures with even-deeper dtrees but
  is dormant on the current fixture.
- **JFS J.4a — fsck (read-only verifier) (§8 parked → §10)** — new
  `src/fs/jfs_fsck.rs` (~370 LOC) closes the read-side half of the §8
  J.4 entry. Mirrors `ufs_fsck.rs` / `efs_fsck.rs` in shape (Builder
  + ordered passes) but adapts to JFS's AIT + IAG + BMAP layout.
  Passes: (1) geometry re-validation (version / bsize / pbsize); (2)
  AIT walk via `read_ait_inode` for AGGREGATE_I / BMAP_I / LOG_I /
  BADBLOCK_I / FILESYSTEM_I — `dinode.di_number == inum` plus
  size-non-zero on BMAP_I + FILESYSTEM_I (LOG_I points to SB
  logpxd, BADBLOCK_I empty on clean volume); (3) IAG walk via
  `read_fileset_iag_at` over `(FILESYSTEM_I.di_size / PSIZE) - 1`
  IAGs, with the CORRECTED pmap interpretation (`pmap[i]` is the u32
  word covering 32 inodes inside extent `i`, MSB-first → bit
  `31 - in_extent`) — an extent with length=0 + non-zero pmap word
  fires `IagPmapInoextSkew`; per-bit walk builds the
  allocated-fileset-inum set with `FIRST_USER_FILESET_INO = 4` skip
  (fino 0 dummy / 1 badblock / 2 root / 3 root-ACL kernel-reserved);
  (4) BMAP walk via `require_bmap_walk` surfaces stats + cross-checks
  dbmap.nfree against pmap-walk-derived nfree; (5) connectivity BFS
  from FILESET_ROOT_INO through inline dtrees, external dtrees emit
  `ExternalDtreeNotWalked` warning + skip. Orphans = pmap-allocated
  ∖ reachable; `PmapAllocatedInodeIsZero` fires for the pmap/dinode
  skew variant. Wired into `Filesystem::fsck` so the trait method
  returns `Some(Ok(result))` for JFS volumes.
  `OrphanInode` is the only repairable code today — a repair driver
  needs the J.4b edit-side write primitives. Constants promoted to
  pub(crate) for the sibling module. Tests (3 added; JFS suite 57
  → 60): `fixture_fsck_clean` is_clean() round-trip on the real
  fixture, `fsck_flags_pmap_inoext_skew` sets the MSB of `pmap[10]`
  (an unallocated-extent slot) and asserts the code surfaces,
  `fsck_flags_orphan_inode` forges an orphan at fino 8 by setting
  pmap bit 23 of `pmap[0]` + stamping mode=0o100644 + di_number=8 at
  fino 8's dinode byte offset, asserts OrphanInode + orphaned_entries
  + `repairable` flag.
- **UFS U.4 — `repair()` (§8 parked → §10)** — closes the §8 parked
  driver. `repair()` lives on the `EditableFilesystem` impl at
  `src/fs/ufs.rs` and delegates to `repair_ufs`, mirroring `repair_efs`
  in shape: re-runs `fsck_ufs`, switches on the repairable issue codes,
  returns a `RepairReport`. Three branches: (1) `ReplicaSb*Mismatch` —
  reads the primary SB byte image once via the new
  `read_primary_sb_bytes`, parses the CG out of every replica issue's
  message, then rewrites each affected CG's replica slot via the new
  `write_replica_sb_bytes(cg, &bytes)` (refuses cg=0 and length !=
  SB_READ_SIZE); (2) `BitmapMissingAllocation` — groups fragments by
  CG so each CG's free-bitmap reads + writes once, parses fragment
  number from each issue's message, clears the stray free bit (UFS
  polarity: set=FREE so clearing = mark allocated), bumps cg_cs.nffree
  down by the cleared count via `update_cg_cs`; (3) `OrphanInode` —
  via the new `adopt_orphans_into_lost_found_ufs` helper: looks up or
  creates `/lost+found` under root inum 2 through the trait
  `create_directory`, then for each orphan re-reads the inode (to source
  the actual d_type via `mode_to_dirent_type`, not assume), generates
  `ino_<inum>` with `_<n>` suffix retry on collision (capped at 1000
  retries before failed-fix bookkeeping), calls `dir_insert` + writes
  the lost+found inode back. Unrepairable codes (geometry damage,
  double-allocation, out-of-range pointers, indirect-read failures)
  flow into `unrepairable_count`. Tests (`src/fs/ufs.rs::tests`, 3
  added; UFS suite 62 → 65): `repair_clears_bitmap_missing_allocation
  _and_refscks_clean` flips a root-claimed fragment's bitmap bit on
  the real UFS1 fixture and asserts `is_clean()` post-repair;
  `repair_adopts_orphan_inode_into_lost_found_and_refscks_clean`
  forges an orphan via `alloc_inode` + `write_inode` with no parent
  link, repairs, asserts `/lost+found/ino_<inum>` exists with matching
  inum and the volume re-fscks clean;
  `repair_rewrites_replica_sb_from_primary` corrupts CG 1's replica SB
  magic + bsize on a synthetic 2-CG UFS2 image and asserts every
  `ReplicaSb*Mismatch` is gone post-repair (geometry warnings from the
  hand-rolled image tolerated).
- **JFS multi-IAG dispatch (§8 parked → §10)** — lifts the §8
  refusal in `read_fileset_iag` for FILESYSTEM_I sized past 2 × PSIZE.
  The old cap topped out at EXTSPERIAG * INOSPEREXT = 4096 fileset
  inodes; the new dispatch routes lookups to whichever IAG holds the
  requested fino via a cached HashMap. Surface (`src/fs/jfs.rs`):
  `read_fileset_iag_at(iag_no)` is the new primitive (logical page
  `1 + iag_no` of FILESYSTEM_I — page 0 is the dinomap header),
  `iag_for_fino(fino)` is the cached lookup (returns `Clone`d
  FilesetIag so callers don't carry a `&mut self` borrow across the
  next reader seek), `read_fileset_inode_global(fino)` is the trait-
  layer entry point. The old `read_fileset_inode(fino, &iag)`
  validates that the supplied iag's iagnum matches
  `iag_no_for_fino(fino)` — silent mismatches now surface as a clear
  `Parse` error instead of garbage. The `di_size > 2 * PSIZE` gate is
  replaced by `iag_no >= (di_size / PSIZE) - 1` so any IAG covered by
  the on-disk fileset inode is now reachable. JfsFilesystem gains an
  `iag_cache: HashMap<u32, FilesetIag>` field (lazy, never cleared —
  read-only mode, xtree stable). All three trait callers (root /
  list_directory / read_file) moved to `read_fileset_inode_global`;
  the list_directory per-child loop benefits most because a directory
  whose children straddle IAGs now pays one logical-page read per
  IAG, not per inum. Tests (`src/fs/jfs.rs::tests`, 4 added; JFS
  suite 53 → 57): `iag_no_for_fino_splits_every_4096_inodes` —
  pure arithmetic; `iag_cache_short_circuits_second_lookup_for_same_
  iag` — first hit populates, second hit reuses, third hit on a
  different inum in the same IAG stays at len=1;
  `read_fileset_inode_global_matches_iag_supplied_form` — dispatch
  result is byte-identical to the explicit `(fino, &iag)` form on
  the single-IAG fixture; `read_fileset_inode_rejects_iag_mismatch`
  — passing IAG 0 for inum 4096 returns the new "belongs in IAG 1"
  Parse error.
- **UFS U.4 — fsck + write + EditableFilesystem (§8)** — shipped
  across three slices on 2026-06-03. **fsck** (`src/fs/ufs_fsck.rs`,
  ~500 LOC) mirrors `efs_fsck.rs`: 5 ordered passes covering geometry
  re-validation, per-CG header + replica SB cross-check (every CG > 0,
  comparing magic/bsize/fsize/frag/ncg/fpg/ipg/sblkno/cblkno/iblkno/
  total_frags), inode-table walk with full direct + single/double/
  triple indirect descent (`walk_inode_blocks` helper returns both
  data + indirect-block fragments so a thorough bitmap check
  accounts for both), bitmap consistency (every inode-claimed
  fragment's bit must be CLEAR — UFS bitmap is set=FREE, opposite of
  every Linux FS), connectivity BFS from root inum 2. Repairable
  codes: `ReplicaSb*Mismatch` (11 fields), `BitmapMissingAllocation`,
  `OrphanInode`. **Write primitives**: `write_frag_run`,
  `write_inode` (version-aware UFS1 128 B / UFS2 256 B encoding),
  `read_cg_iused_bitmap` / `write_cg_iused_bitmap` /
  `write_cg_free_bitmap` (named by effect — `alloc_*` / `free_*` —
  so callers don't have to remember which bitmap uses which
  polarity), `read_cg_cs` / `update_cg_cs` for the 4-field
  cylinder-group summary, `alloc_inode` / `free_inode` /
  `alloc_frag_run` / `free_frag_run`. **EditableFilesystem**:
  `create_file` (direct extents only, ≤ 12 × bsize), `create_
  directory` (single-block initial layout with `.` / `..` stamped
  via DIRENT2 reclen + d_type, parent nlink bumped + CG ndir
  counter incremented), `delete_entry` (empty-dir scan, unlink-
  first-then-free crash-survivability, parent nlink + ndir
  decremented on dir delete), `sync_metadata` (no-op — every
  primitive flushes synchronously), `free_space` (per-CG nffree
  sum × fsize). Tests: 62 UFS tests total — fixture round-trips
  (create + list_directory + read_file + fsck-clean), edit failure
  modes (delete non-empty dir, oversize file rejection), bitmap +
  inode primitive smoke tests, replica-SB-magic-mismatch detection
  on a hand-rolled 2-CG synthetic UFS2 image. `repair()` itself —
  applying the repairable codes — is the only piece left of the §8
  UFS line, parked there with a ~200-300 LOC scope estimate.
- **JFS multi-level xtpage walker (§8)** — `parse_xtpage_buf` /
  `read_xtpage_at` / `read_xtree_logical_page` / `walk_xtree_leaves`
  primitives on `JfsFilesystem`. The header layout is identical for
  inline xtroot (288 B) and external xtpage (4 KiB); the parser
  validates `nextindex` against the buffer's actual capacity so
  corruption surfaces as `Parse` rather than slice OOB. Descent is
  iterative with a 16-level cycle guard and a 1M-node-visit budget.
  Refusals lifted: `read_file_data` now walks every leaf XAD
  (sorted by logical offset, sparse gaps zero-filled);
  `walk_bmap` accepts BMAP control inode's internal xtree
  (`read_bmap_logical_page` descends via `read_xtree_logical_page`);
  `read_fileset_iag` accepts FILESYSTEM_I internal xtree (with a
  separate multi-IAG gate on `di_size > 2 * PSIZE`). Tests: 4 new
  on a synthetic 2-level xtree built on a `Cursor<Vec<u8>>` —
  parse round-trip, DFS leaf walk, descent to correct physical
  block via sentinel bytes, page-uncovered error. 53 JFS tests
  pass total. Multi-IAG dispatch + dtree walker + dmapctl walker
  remain parked in §8.
- **HFS — extending a raw partition image (§3.1)** — `emit_apm_disk_with_hfs`
  in `src/fs/hfs_clone.rs` now takes `Apm::parse` as optional: a non-APM
  source (a raw single-partition HFS image like `partition-0.img`, no DDR at
  sector 0) is a valid input. Drivers come up empty, no Apple_HFS self-entry
  is mimicked, and the output is a fresh APM-wrapped disk with one
  `Apple_HFS` partition at block 64. The fallback DDR mirrors what
  `build_minimal_apm` produces. GUI default `output_hfv` now keys on
  `partition_offset == 0` (covers both `.hfv` superfloppies and any raw
  single-partition HFS image), and the "Flat HFV" radio + Save As dialog
  broaden to "Bare HFS image (.hfv / .img)" so the cap-at-2047-MB framing
  reads as a classic-HFS limit rather than BasiliskII-specific. CLI `expand`
  about-string updated to advertise raw-source support. Tests: unit
  `emit_apm_from_non_apm_source` in `src/fs/hfs_clone.rs` (asserts 0 drivers
  copied, HFS at block 64, output parses + fsck-clean) and integration
  `test_expand_runner_wraps_non_apm_source_in_apm` in
  `tests/filesystem_e2e.rs` (end-to-end runner from `partition-0.img`-style
  source to APM-wrapped output with file round-trip).
- **XFS hole (E.1) — v5/CRC primitives** — new `src/fs/xfs/v5_crc.rs`
  with `crc32c` (Castagnoli, init=0xFFFFFFFF, xorout=0xFFFFFFFF, matched
  to `xfs_start_cksum_safe`), `stamp_crc(buf, crc_off)` that zeroes
  the field before computing, per-block-type stampers (`stamp_inode_v3`
  / `stamp_dir3_blk_hdr` / `stamp_da3_blkinfo` /
  `stamp_sblock_crc_header` / `stamp_lblock_crc_header` /
  `stamp_agf` / `stamp_agi` / `stamp_agfl` / `stamp_superblock`), and
  the field-offset constants every later slice keys off. New `sb_uuid`
  / `sb_meta_uuid` fields on `XfsSuperblock` plus a `meta_uuid()`
  accessor that routes through the META_UUID incompat bit. A fixture
  cross-check (`v5_crc_primitive_matches_on_disk_root_inode_and_ag_headers`)
  verifies our CRC of mkfs.xfs's root inode + AGF + AGI + SB matches
  the on-disk values — if the crc32c crate's params ever drifted from
  what `xfs_repair` expects, that test fires.
- **XFS hole (E.2) — v3 inode core stamping** — `write_inode_region`
  (the read-modify-write helper that every edit / repair path funnels
  through) calls `stamp_inode_v3` over the freshly-copied inode bytes
  on v5, just before the sector span commits. `init_free_inode_chunk`
  stamps each of the 64 fresh slots in a new chunk with its own
  `di_ino`. Upstream alloc/repair gates still reject v5 because the
  AGI / SB / sblock write paths haven't landed yet — when E.5 lifts
  them, inode writes Just Work. Verified by a v5-fixture round-trip
  test that mutates `di_atime`, writes back through `write_inode_region`,
  and re-validates `di_crc`.
- **XFS hole (E.3) — dir3 block / data / leaf1 builders** — three dir
  builders parameterized on `is_v5`: `build_block_dir` emits XDB3 with
  64-byte hdr + bestfree at offset 48; `build_leaf_data_block` emits
  XDD3 (same layout); `build_leaf1_block_v4` → `build_leaf1_block`
  emits XD3F with 56-byte `xfs_da3_blkinfo` + magic 0x3DF1 at byte 8.
  CRC tuple is left zero in the builder; call sites
  (`create_block_dir_from_shortform`, `block_insert_entry`,
  `convert_block_dir_to_leaf_form` over 3 blocks,
  `dir_remove_entry`) stamp via `stamp_dir3_blk_hdr` /
  `stamp_da3_blkinfo` once the destination fsblock is known. The
  bests-extraction in the conversion path adapts (v4: bytes 6..8, v5:
  bytes 50..52). New `v5_crc::fsblock_to_daddr` helper translates a
  filesystem block to the 512-byte-basic-block disk address
  (`fsbno << (blocklog - 9)`) every `blkno` stamp needs. Three unit
  tests in `edit.rs::tests` round-trip each builder through the
  existing dir3-aware reader.
- **XFS hole (E.4) — sblock-crc btree builder** — `btree_build`'s
  `capacities` / `blocks_needed[_for]` thread an `is_v5` flag (v5
  shrinks `max_leaf` / `max_intern` by the 40-byte CRC overhead),
  and `build_sblock_btree` / `build_alloc_btree` take
  `Option<&XfsSuperblock>`. When `Some(sb)`, every produced block
  (leaf + internal node) carries the standard 16-byte v4 short header
  followed by the CRC-header tuple at bytes 16..56
  (`bb_blkno`/`bb_lsn`/`bb_uuid`/`bb_owner`/`bb_crc`) via the new
  `stamp_v5_sblock` helper. `freespace_rebuild` picks v5 magics
  (`XFS_ABTB_CRC_MAGIC` / `XFS_ABTC_CRC_MAGIC`) and passes
  `Some(sb)` through `build_alloc_btree` on v5 volumes; inobt
  rebuild (R3) and `swap_inobt_blocks_in_ag` stay v4-only because
  their upstream gates still reject v5. Two builder-level tests verify
  the round-trip and assert every emitted block's CRC validates via
  `crc_valid`.
- **XFS hole (E.5a) — AGF/AGI/AGFL/SB stampers** — new per-sector
  stamp+write helpers (`write_agi_sector` / `write_agf_sector` /
  `write_agfl_sector` / `write_sb_primary`) on `XfsFilesystem`. Each
  branches on `sb.is_v5()`, stamps the v5 CRC tuple (uuid/lsn/crc),
  and writes through. Every existing AGI / AGF / SB write site funnels
  through these — `alloc_inode_slot`, `alloc_new_inode_chunk`,
  `grow_inobt_with_new_record`, `free_inode`, R3
  `resync_sb_inode_counts`, R4 secondary-SB fixup, R4b summary
  rewrites, R2 `write_agf_fields` + `resync_sb_fdblocks`,
  `bump_sb_ifree` / `bump_sb_inode_counters`. AGFL helper stays in
  tree (`#[allow(dead_code)]`) for future allocation refills.
- **XFS hole (E.5b) — splice / grow inobt v5 + lift inode alloc** —
  `walk_inobt` reader accepts both v4 (`IABT`) and v5 (`IAB3`)
  magics + the 56-byte CRC header. `splice_inobt_single_leaf_record`
  and the single-leaf splice path in `try_claim_slot_from_existing_
  chunks` use `sblock_hdr_len(sb.is_v5())` to pack records and
  re-stamp the leaf's sblock-crc before write. `grow_inobt_with_new_
  record` selects `XFS_IBT_CRC_MAGIC` + `Some(sb)` on v5 so every
  emitted node carries a stamped CRC. `free_inode` mirrors the
  splice-write stamp. Top-level v5 gates on `alloc_inode_slot` /
  `alloc_new_inode_chunk` come down. Integration test
  `alloc_new_inode_chunk_on_v5_stamps_every_block_and_stays_clean`
  drives a fresh-chunk allocation on the modern fixture and asserts:
  AGI/AGF/SB CRCs re-verify, the inobt leaf's sblock-crc re-verifies,
  AGI/SB counters bump by +64, every fresh dinode carries di_version=3
  + di_ino + di_uuid + valid di_crc, and `run_fsck` stays clean.
- **XFS hole (E.5c) — bmbt v5 + fork_offset sweep + create_file** —
  `build_bmbt_leaf_v4` → `build_bmbt_leaf` parameterized on `is_v5`
  (72-byte CRC header, `XFS_BMAP_CRC_MAGIC`). `do_create_file` stamps
  the leaf's `xfs_btree_block_lhdr` CRC via
  `v5_crc::stamp_lblock_crc_header` (owner = new file inode) before
  the write. `fork_offset(false)` hardcoded at 10 sites in `edit.rs`
  is swept to `fork_offset(sb.is_v5())` so the literal area starts at
  byte 176 on v5. `init_file_inode` zeroes bytes 100..176 on v5 so a
  recycled slot doesn't inherit stale v3 fields. Integration test
  `create_file_on_v5_fixture_round_trips_through_fsck` creates a new
  file on the modern fixture, re-opens read-only, lists / reads the
  file back, and asserts `run_fsck` stays clean — the verifier walks
  every metadata block, so a stale CRC anywhere along the alloc /
  init / insert path would surface as a failure.
- **XFS hole (E.5d) — repair gates + verify** — R5 (`run_inode_core_
  repair`), R6 (`run_dir_repair`), R7 nlink (`run_nlink_repair`), and
  R7 orphan reconnect (`run_orphan_reconnect`) all lift their v5
  early-returns now that `write_inode_region`, `dir_insert_entry`,
  `init_empty_shortform_dir`, and the inobt-walk traversal are v5-
  aware. R5/R6 inobt-leaf iteration in their own helpers picks up
  `XFS_BTREE_SBLOCK_CRC_LEN` (56 B) when v5. **R2 (freespace rebuild)
  and R3 (inobt repair) stay v4-only**: both could write a CRC-
  correct tree, but v5 layouts may carry `finobt` / `rmapbt` /
  `refcountbt` ro-compat metadata that R2's block-completeness map
  and R3's AGI-summary recompute don't model — parked in §8. Stale
  `"v5 not supported"` comment in `src/fs/mod.rs` updated to reflect
  full v5 read+edit+fsck support. Integration test
  `repair_on_clean_v5_fixture_runs_passes_and_stays_clean` runs the
  full repair pass on a clean v5 volume: no fixes applied, no
  failures, and `run_fsck` stays clean post-repair. 114 xfs tests
  pass.

- **Windows install + self-update** — Phases 1-9 + elevation A-H all
  done (Setup.exe via Inno, in-app `self_update` + `self_replace`,
  ARP `DisplayVersion` refresh, per-user file-association registrar,
  asInvoker elevation rework, Windows-VM runtime validated).
- **BasiliskII HFV support** — Phases 1-8 done; 4.1/4.2 standalone
  "New blank HFV" dialog closed-by-design (folded into Expand /
  Export-to-HFV).
- **HFS+ editable surface** — `impl EditableFilesystem for
  HfsPlusFilesystem` at `src/fs/hfsplus.rs:3703` with full
  create/delete/sync_metadata + Finder info + resource fork + blessed
  folder + xattr B-tree + hardlinks + HFSX case-sensitivity + journal
  parse + replay + history view + defrag-on-backup.
- **Superfloppy → partitioned-disk export** — `src/restore/
  superfloppy_wrap.rs` ships MBR + GPT synthesis with alignment
  variants and VBR `hidden_sectors` patching.
- **Clonezilla GPT parsing** — `<disk>-gpt-1st` + `<disk>-gpt-2nd`
  raw-sector readers wired through `src/restore/mod.rs:2048-2080` via
  the same CRC-validated `Gpt::parse` path native GPT restores use.
- **Earlier btrfs "trim at last chunk end" plan** — abandoned (real-
  world layouts don't have a useful last-chunk-end; superblock mirrors
  matter; restore-side resize would corrupt). Replaced by §2.4.
- **XFS gap-fill** — v5 read shipped, fsck shipped (R1-R8 pipeline +
  multi-level inobt rebuild + freespace btree rebuild), edit shipped
  (v4 + snapshot/rollback + wired into `open_editable_filesystem`),
  AGF walk shipped via `freespace_rebuild.rs`. Open: holes A-E in §2.1
  and shrink in §2.2.
- **JFS + UFS** — promoted from parked (`additional_nix_filesystems.md`
  "JFS / UFS parked, noted for completeness") to scheduled in §1.
- **`PLAN-ext-btrfs.md` Task 16** — `tests/filesystem_e2e.rs` ships 89
  ext2/ext4/btrfs browse + read + symlink + permissions + nested-dir +
  compaction round-trip + resize + detection-routing tests, with
  committed fixtures.
- **Disk expansion** — Phases 0-5 all shipped (PartitionBar widget, XFS
  recognizer, Mode A free-space, partition-table writers for
  MBR/GPT/APM/SGI, Mode B last-partition extend, Add-Partition flow).
- **Amiga Phase 10** — `resize_affs_in_place` / `resize_sfs_in_place` /
  `resize_pfs3_in_place` + their validators + RDB PART-entry fixup all
  shipped. Standard "restore an Amiga RDB image to a larger CF card
  and grow to fill" works end-to-end.
- **HFS+ defrag-on-backup Steps 22g + 23** — `shrink_to_minimum` wired
  via the "Resize partitions to minimum size" GUI checkbox and the
  per-partition Defrag toggle; `"Defrag emit: ..."` log line in
  `src/backup/mod.rs:1109-1121`.
- **HFS+ journal Steps 24-26 + 28-29** — parse + replay + transaction
  recorder + history viewer + fsck journal phase. §2.3 covers what's
  left.
- **SEA detection** — `find_sea_archive` + extract.rs routing classifies
  `.sea` as SIT-over-Mac-app data fork.
- **Native Mac archives reader/writer** — BinHex 4.0 + StuffIt classic
  + SIT5 + SEA, CLI verbs, Archives tab in GUI, all shipped. §6.1
  covers the GUI import / auto-unwrap / folder-emit / file-picker
  remainder.
- **`docs/codecleanup.md`** — every actionable item closed; survey
  retired.
- **`docs/virtualization-formats.md`** — Phases 1-5 closed; modern-VM
  containers (VHD dynamic / QCOW2 / VMDK flat + sparse) and legacy
  imports (GHO + IMZ with password decryption + file-aware FAT+NTFS
  reconstruction) all shipped.
- **`docs/efs_resize_and_edit.md`** — EFS read + edit + fsck + in-place
  grow + conservative shrink + aggressive shrink all shipped.
- **`docs/chdman_replacement.md`** — Stages 1-10 shipped; no external
  `chdman` binary required. The user-side CD CHD browse verify in §7
  is the only loose end.
- **Unix-FS read track (§1 ReiserFS / UFS / JFS)** — three filesystems
  shipped end-to-end. **ReiserFS** (v3.5 `ReIsErFs` + v3.6 `ReIsEr2Fs`,
  reiser4 rejected): superblock at byte 65536 + bitmap-driven
  layout-preserving `CompactReiserFsReader` (set bit = allocated) + S+tree
  walker (`collect_leaf_block_numbers` DFS with cycle / level / child=0
  guards) + `read_statdata` / `read_symlink_target` / `list_directory`
  (DIR_ENTRY decode, hidden-filter) / `read_file` (SD/IND/DRCT
  stitching with sparse + tail-pad handling). Fixture
  `tests/fixtures/test_reiserfs_v3_6.img.zst`. **UFS** (UFS1 4.2BSD/FFS +
  UFS2 — SunOS / Solaris / *BSD): superblock probe at 65536 then 8192
  with both endians, SU+J dirty refused, cylinder-group bitmap walker
  (set = FREE — opposite polarity of every other Linux FS, matching
  primitive `BitmapReader::highest_clear_bit`), `CompactUfsReader`,
  `UfsInode` (UFS1 128 B / UFS2 256 B), `resolve_logical_block` (direct
  → single → double → triple indirect with sparse handling), DIRENT2
  decode with `d_reclen` alignment validation, inline-symlink decode at
  the version-specific cutoffs. **JFS** (JFS2 only — AIX JFS1 rejected
  via the `"JFS1"` ASCII magic gate paired with version `1..=2`):
  Aggregate Superblock at byte 32768, AIT bootstrap at
  `AIMAP_OFF + 2 * SIZE_OF_MAP_PAGE = 0xB000`, BMAP B+tree walker that
  tightens `last_data_byte` to the pmap-derived highest allocated block
  (still maxed against `logpxd`/`fsckpxd` so the inline log rides out
  verbatim), `CompactJfsReader`, IAG-based dinode locator with pmap
  sanity check, inline-dtree walker (stbl iteration through slots 1..8 +
  dtslot continuation chain for names > 11 chars), inline-xtree XAD walker
  with sparse gap-fill. Parked Tier B+ follow-ups (UFS U.4 / JFS J.4
  fsck+edit, JFS J.3 multi-page B+tree walkers) live in §8.
- **ProDOS access-bit setter** —
  `EditableFilesystem::set_prodos_access(entry, access)` patches the
  access byte at directory-entry offset 30 in place. **Lock** ($21 = read
  + backup) / **Unlock** ($C3 = read + write + destroy + rename +
  backup) buttons on the browse-view edit toolbar; `StagedEdit::
  SetProdosAccess { entry, access }` queues through the same
  replace-prior + pending-query helpers as the rest of the staged-edit
  pipeline.
- **NTFS file-aware GHO — compressed path** — `NtfsCompressedState`
  in `src/rbformats/gho.rs` carries per-block zlib metadata + a lazy
  scan cursor; `open_ntfs_compressed_mft_only` decompresses just the
  $MFT run to bootstrap the index, then `extend_ntfs_compressed_index`
  decompresses the rest on demand as `GhoReader` reads progress. The
  scan result rides through `ntfs_scan_cache_store` so re-opening the
  same `.GHO` skips the multi-second decompress pass. Compressed +
  uncompressed file-aware NTFS GHO backups now round-trip identically.
- **`rb-cli fsck --format json|yaml`** — `FsckResult` / `FsckIssue`
  / `FsckStats` / `OrphanedEntry` / `RepairReport` all carry
  `#[derive(Serialize)]`. The verb mirrors the structured-output flag
  every other read-only verb already has, with a `clean: bool` top-level
  envelope for scripted branching and a `repair: { applied / failed /
  unrepairable }` sub-object when `--repair` runs. Unsupported
  filesystems emit `status.error: true, status.code: GENERIC_FAILURE,
  result: null`. Process exit code stays issue-driven so `$?` branching
  works in either text or structured mode.
- **`rb-cli get` globbing** — accepts the same glob /
  `--exclude` / `--ignore-case` / `--case-sensitive` syntax as `ls` /
  `rm`. Adds `-r` / `--recursive` (literal-directory source without it
  errors with a clear pointer to the flag; glob-matched directories
  without it skip with a warning), `--force` (overwrite existing host
  files), `--skip-existing` (skip silently). Destination semantics
  follow cp/rsync: literal-file source treats DST as the target path
  (or `DST/<basename>` when DST exists as a directory or has a trailing
  separator); literal-directory + `-r` mirrors the tree under
  `DST/<source-basename>/`; glob lays matches out relative to the
  longest non-glob prefix of the pattern. Symlinks ride out as plain
  text files containing the target (lossy but cross-platform safe);
  specials skip with a one-line note.
- **XFS hole (C) — block→leaf dir conversion (v1: 2 data blocks)** —
  `block_insert_entry` falls through to `convert_block_dir_to_leaf_form`
  when `build_block_dir` reports `DiskFull` after appending the new entry.
  The conversion sorts every entry by hash (matching the reader's
  `dir_hashname`), splits at the byte midpoint between block 0 (with
  synthetic `.` / `..`) and block 1, allocates two new dir blocks (data
  block 1 + the XD2F leaf1 index block at file offset
  `XFS_DIR2_LEAF_OFFSET = 2^32 / blocksize`), builds and writes all three
  blocks via `build_leaf_data_block` (XD2D — header+bestfree+tag entries
  packed against the head, free record at the tail recorded in
  `bestfree[0]`) and `build_leaf1_block_v4` (`xfs_da_blkinfo` +
  `xfs_dir2_leaf_hdr` + sorted entries[hashval(4) + address(4)] + bests
  array at the tail), then rewrites the inode via
  `write_dir_inode_multi_extent` (`di_format=Extents`,
  `di_size=2*dirblksize`, `di_nblocks=3*blocks_per_dir`, `di_nextents=3`,
  3-record inline data fork — well within the 9-extent inline cap).
  Address encoding: `(file_block * dirblksize + byte_off) / 8` —
  block 0 entries get dataptrs < 512, block 1 entries 512..1024 (at
  4 KiB dirblksize). `dir_can_insert` pre-flights the split fit and
  refuses leaf-form sources before do_create_file allocates an inode
  (avoids orphans). `block_remove_entry` rejects leaf-form sources with
  a clear error — remove + leaf→block recompaction is a follow-up slice.
  Reader path unchanged (still `walk_dir2_data_blocks`). Test
  `dir_overflow_converts_block_to_leaf_form` adds files until the
  inserter rejects with the leaf-form-not-implemented message, verifies
  > 100 adds absorbed by the conversion, `di_format=Extents`, `nextents
  >= 3`, every original + every successful add lists back, and `run_fsck`
  stays clean. v4 only.
- **XFS hole (D) — bmap-btree file forks (single-leaf)** —
  `do_create_file` no longer caps at `MAX_SINGLE_EXTENT_BLOCKS` blocks or
  `max_inline` extents: `alloc_extents` returns arbitrary runs (capped at
  `bmbt_leaf_max = (bs - hdr) / 16`), `runs_to_extent_records` splits any
  run whose count exceeds the 21-bit on-disk `blockcount` field into
  multiple consecutive records, and `init_file_inode` dispatches between
  inline-extents and btree formats. When the record count exceeds
  `max_inline`, do_create_file allocates one block for the bmbt leaf,
  writes the leaf via `build_bmbt_leaf_v4` (XFS_BMAP_MAGIC, level 0, sibling
  pointers NULLFSBLOCK, extents packed after the long-form header), and
  `init_file_inode` writes the in-inode root via `write_bmbt_root_to_leaf`
  (level 1, numrecs 1, key = first extent's startoff, ptr = leaf fsblock,
  remaining (key, ptr) slots zeroed). `di_format = 3`, `di_nblocks`
  includes the leaf block. Multi-leaf bmbt (>= bmbt_leaf_max records) is
  refused with a clear error after rolling back the data allocation. R2
  freespace rebuild's `mark_inode_blocks` now walks bmbt-format inodes via
  the new `collect_bmbt_blocks` helper (mirrors `walk_bmbt`'s descent +
  rightsib chain, returns block numbers) and marks the bmbt blocks as
  owned alongside the extent blocks — the previous abort on `di_format==3`
  is gone. R5 inode-nblocks repair also picks up bmbt blocks. Reader path
  unchanged (still `walk_bmbt`). Test
  `create_file_writes_bmbt_format_when_extents_exceed_inline_cap` forges
  both AGs' bnobt/cntbt into 12 free fragments of 100 blocks (gaps of 100)
  so the fast-path single-extent alloc fails, then creates a 1100-block
  file whose 11 carved runs blow past the 9-extent inline cap and force
  btree format. Assertions: `DiFormat::Btree`, `nextents > 9`,
  `nblocks = data + 1` (one leaf), byte-for-byte data round-trip via the
  standard `read_file`. v4 only.
- **XFS hole (B) — block → short-form dir re-compaction** —
  `dir_remove_entry` calls `try_recompact_block_dir_to_shortform` after
  `block_remove_entry` rewrites the data block. The helper bails (no-op,
  leaves dir in block form) when the dir is multi-extent / not at file
  offset 0 / any inode > `u32::MAX` (8-byte short-form not implemented) /
  the surviving entries don't fit the inode literal area. Otherwise: build
  the short-form fork bytes (count + i8count=0 + parent + per-entry
  `namelen(1) + offset(2) + name + [ftype] + ino(4)`, with offset cookies
  matching the notional dir2 data-block positions starting after `.` /
  `..`), `free_blocks` the directory's data block(s), and rewrite the
  inode: `di_format=Local`, `di_size=sf_len`, `di_nblocks=0`,
  `di_nextents=0`, literal area zeroed before the fork bytes are written
  so stale block-fork bytes can't leak through. Mirrors
  `xfs_dir2_block_to_sf`. v4 only. Test
  `block_dir_recompacts_to_shortform_on_shrink` adds 20 files (forces
  short-form → block conversion), deletes all 20 (last delete trips the
  re-compaction), then verifies the root is back to `DiFormat::Local`
  with `nblocks=0`/`nextents=0`, originals intact, free space fully
  reclaimed, and the volume fsck-clean.
- **XFS hole (A) — new-chunk inode allocation (single-leaf + multi-level
  grow)** — `alloc_inode_slot` lifted into
  `try_claim_slot_from_existing_chunks`; the new `alloc_new_inode_chunk`
  reads `di_version` from the root inode, carves `blocks_per_chunk`
  contiguous fsblocks aligned to `sb_inoalignmt` (or `blocks_per_chunk`
  when ALIGN is off) via the new `alloc_blocks_aligned` +
  `carve_aligned_from_largest` primitives, writes 64 free dinodes (magic
  + version + `di_mode = 0` + `di_next_unlinked = NULLAGINO`), then
  dispatches between two splice strategies. **Single-leaf splice**
  (`splice_inobt_single_leaf_record`) — when the AGI root is a one-block
  level-1 leaf with at least one open slot, sort + rewrite the leaf in
  place; AGI root/level unchanged. **Multi-level grow**
  (`grow_inobt_with_new_record`) — when the existing tree is multi-level
  or its only leaf is full, gather every existing record from every
  leaf, add the new all-free chunk, key-sort, allocate `blocks_needed_for`
  contiguous AG-local blocks for a fresh tree AND reclaim the old tree
  blocks in one freespace rebuild via the new `swap_inobt_blocks_in_ag`
  helper (alloc + free fused; the old blocks coalesce into the AG's
  current-full-free set, then `n` blocks carve from the largest extent,
  and `rebuild_ag_freespace` writes one set of new bno/cnt trees),
  re-build the inobt via R3's `build_sblock_btree`, write every new tree
  block, then relocate AGI root + level. AGI `count`/`freecount` + sb
  `sb_icount`/`sb_ifree` bump by 64 once after dispatch — both paths
  share the same counter update. Slot search re-runs and claims slot 0
  of the new chunk via the existing -1 accounting. v4 only (v5/CRC is
  hole E). New helpers: `collect_inobt_all_blocks` in `inobt_repair.rs`
  (shared `walk_inobt` traversal returns leaves + all blocks).
  Tests: 5 alignment-carve unit tests +
  `alloc_new_inode_chunk_extends_inobt_and_keeps_volume_clean` end-to-end
  against the v4 fixture (counter deltas, sorted record + all-free
  shape, every dinode initialized, our verifier stays clean) +
  `alloc_new_inode_chunk_promotes_full_leaf_to_multi_level` (forges 254
  phantom records to fill the root leaf, then verifies the new tree is
  level >= 2, AGI root moved, all max_leaf+1 records survive in
  per-leaf-sorted form, sb counters bumped). Oracle helper:
  `examples/xfs_fill_chunks.rs`.
- **Native Mac archives GUI polish (Workflows A / B / C / D / E)** —
  full sweep. Detection is now magic-driven via
  `macarchive::detect::detect_mac_archive` (six `MacArchiveKind`
  variants — BinHexSingleFile, BinHexOverSit, BinHexOverSea, Sit, Sit5,
  Sea); filename extension is hint-only. `MacArchiveKind::label()` keeps
  user-visible strings ASCII-only (CLAUDE.md egui-font rule).
  `detect_mountable_image` sniffs DiskCopy 4.2 / raw HFS / raw HFS+ for
  the auto-unwrap "Mount in new Inspect tab" affordance.
  **D.4**: inspect / restore / backup tab pickers gained a "Mac
  archives" filter group alongside Disk Images. **D.1**:
  `extract::open_bytes` routes through `detect_mac_archive` and
  synthesizes a one-entry archive (with proper `crc16_arc` stamping) for
  loose BinHex single-file inputs, so the Archives tab and every
  downstream extract path handle `.hqx` end-to-end without any special
  case. **D.2**: Archives tab caches a `MountablePayload` on load when
  the archive has exactly one entry whose data fork sniffs as a disk
  image; "Mount in new Inspect tab (<kind>)" button writes the bytes to
  a tempfile, hands path + tempdir guard to the Inspect tab via the new
  `InspectTab::load_image_with_tempdir`, and the RustyBackupApp update
  loop drains `take_pending_inspect_open` + switches `active_tab`.
  **D.3**: per-entry checkbox column on the Archives tab grid; new
  `extract::extract_filtered(keep: impl FnMut(usize) -> bool)`
  (folder markers always extract); "Extract Selected... (N)" button
  alongside Extract All. **A**: browse-view `stage_host_file` intercepts
  any picked file that sniffs as a Mac archive when the target is HFS /
  HFS+; queues a `PendingArchiveImport`; modal pops with the per-kind
  three-action set ("Convert" / "Expand" / "Convert and expand" +
  "Convert only" for HQX-over / "Add as-is" + Cancel). Extract path
  dumps to an AppleDouble tree under a BrowseView-owned
  `archive_import_tempdir`, then re-stages the resulting children
  through the existing `stage_host_directory` / `stage_host_file` flow
  so resource forks come back through `detect_resource_fork`. Tempdir
  cleaned up on Apply / Discard / close. **B**: three new "Export Mac
  archive" buttons under the existing Extract row — BinHex (single file
  only), StuffIt (any selection — folders walk recursively), and
  StuffIt-over-BinHex (any selection). `walk_fs_to_input_nodes` reads
  data + resource fork + type/creator from the FileEntry / Filesystem
  trait; `build_archive_tree` + optional `binhex::build_binhex` wrap
  produces the output bytes. **C**: a "Save as Mac archive..." button on
  selected files inside the disk image sniffs the bytes and pops a
  Save-as-is / Decode-and-save modal with an inline ForkFormat dropdown
  (BinHex / MacBinary / AppleDouble / Raw) for the decode path. **E**:
  a "Browse archive..." button opens a floating read-only viewer window
  with the same shape as the standalone Archives tab — entry list with
  per-entry checkboxes, fork-format dropdown, Extract All / Extract
  Selected. Edit operations inside the archive (would need repacking)
  are deferred. Modal wording lives in
  `docs/mac-archives-gui-wording.md` (kept as the spec source of truth).
  Tests: 17 detect-tests (6 kinds + 3 mountable + edge cases) + 4
  extract-tests (open_bytes synth, extract_filtered subset).
