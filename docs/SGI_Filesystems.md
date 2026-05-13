# SGI Filesystem Support — Implementation Plan

Read-only browse + per-file extract for **EFS** and **XFS v1/v2** on SGI disks
(IRIX systems), working from raw images, CHD images, and physical disks. This
is the staged plan referenced from `CLAUDE.md`. It is browse-first by design;
backup, compaction, resize, and edit are explicitly deferred.

The plan is written so each numbered step in "Implementation steps" is a
self-contained session: open the repo, do the step, run tests, open a PR.
Steps are ordered by dependency — do not skip ahead.

---

## Prerequisites: software to install on the implementation machine

```bash
# Rust toolchain (already required by the project).
rustup install stable
rustup default stable

# CHD tooling — extracts raw bytes from .chd images for development fixtures.
brew install rom-tools          # provides chdman (macOS)
# or: apt install mame-tools    # provides chdman (Debian/Ubuntu)

# XFS userspace — ground-truth dumps of test images during development.
brew install xfsprogs           # macOS
# or: apt install xfsprogs      # Linux

# Optional but very useful for hand-decoding fixtures.
brew install hexyl xxd binwalk
```

You also want a **Linux VM** (any modern distro) handy for one specific task:
generating tiny EFS test fixtures with `mkfs_efs`. macOS has no EFS userspace
and `xfsprogs` does not include EFS. See "Test fixtures" below.

**Source for dir1 (XFS v1 directory format):** the Linux kernel removed dir1
years ago, so we need the historical SGI XFS source. Grab this single RPM:

```
kernel-source-2.4.2-SGI_XFS_1.0.i386.rpm   (22.5 MB)
```

Despite the `i386` suffix it's a noarch source-tree package. Extract with:

```bash
rpm2cpio kernel-source-2.4.2-SGI_XFS_1.0.i386.rpm | cpio -idmv
# look in usr/src/linux-2.4.2/fs/xfs/
#   xfs_dir.[ch]        — dir1 entry points
#   xfs_dir_leaf.[ch]   — dir1 leaf format
#   xfs_dir_sf.h        — dir1 shortform layout
#   xfs_da_btree.[ch]   — shared B+tree code (used by both dir1 and dir2)
```

The other RPMs in that release directory (`kernel-2.4.2-…i386/i586/i686`,
`kernel-smp-…`, `kernel-enterprise-…`, `kernel-headers-…`) are compiled
x86 binaries or trimmed headers — **skip them**. The `.src.rpm` (25.3 MB) is
a fallback only if the `kernel-source` RPM is unavailable.

**Reference disk images** (do **not** commit to the repo):
- `~/Downloads/IRIX6.5/irix65.chd` — 5.3 GiB compressed / 93 GiB logical,
  uncompressed CHD. Used for initial reconnaissance and large-scale
  verification.
- `~/Downloads/SGI_imaged-001.sample.hda` — 5 MiB head of a real SGI hard
  disk. Confirmed dir1-format XFS v4 (see "Known images on hand" below).
  Excellent for verifying the partition table, XFS superblock detection,
  AG0 header reads, root-inode lookup, and end-of-file truncation
  handling. Use it as a local-only fixture.

If continuing work on a different machine, copy these images over or
re-acquire from the same sources. We will craft and commit small synthetic
fixtures (Step 1) to stand in for them in CI.

---

## Reference sources: is the Linux kernel enough?

**Short answer: the latest kernel covers XFS and the SGI partition table, but
NOT EFS — EFS was deleted from staging in 2022. Plan to clone an older tag.**

| Component                     | In latest Linux? | Where to look                                                    |
|-------------------------------|------------------|------------------------------------------------------------------|
| SGI Volume Header partitions  | Yes              | `block/partitions/sgi.c`, `block/partitions/sgi.h`               |
| XFS modern (v5/CRC)           | Yes              | `fs/xfs/libxfs/` — especially `xfs_format.h`, `xfs_sb.c`, `xfs_dir2_*.c`, `xfs_bmap*.c`, `xfs_inode_buf.c` |
| XFS v1/v2 sb compat paths     | Yes (gated)      | Same files — search for `XFS_SB_VERSION_1` / `_2`, `xfs_sb_version_*` predicates |
| **XFS dir1 (v1 directory)**   | **Removed**      | Gone before v2.6.x. Use `kernel-source-2.4.2-SGI_XFS_1.0.i386.rpm` `fs/xfs/xfs_dir*.[ch]`. |
| EFS (read-only)               | **Removed**      | Last present in `fs/efs/` through kernel **v5.17** (Mar 2022). Removed from staging in v5.18+. |

**Recommended sources on the implementation machine:**

```bash
# Modern kernel: XFS v2/v4 + SGI partitions (shallow clone is fine).
git clone --depth 1 https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git
# Older kernel containing fs/efs/. v5.15 is an LTS and definitely has it.
git clone --depth 1 --branch v5.15 https://git.kernel.org/pub/scm/linux/kernel/git/stable/linux.git linux-5.15-efs
# Historical SGI XFS source (dir1) — see "Software prep" above.
rpm2cpio kernel-source-2.4.2-SGI_XFS_1.0.i386.rpm | cpio -idmv
```

`fs/efs/` in v5.15 is small (~1.5 KLOC, read-only) and is the cleanest
existing reference for the EFS on-disk format. The 2.4.2-SGI source tree's
`fs/xfs/xfs_dir*.[ch]` is the only readable reference for the dir1 format
on disks predating dir2 (~IRIX 6.4). SGI's original `efs(4)` and `efs(5)`
man pages are also worth a read if you can find an IRIX manpage mirror —
they document the cylinder-group layout in plain English.

**Beyond the kernel, you will also want:**

1. **"XFS Algorithms & Data Structures", 3rd edition (SGI / Silicon Graphics)** —
   the canonical XFS spec. Freely mirrored as a PDF (search the title). Covers
   superblock, AGs, btrees, inode formats, dir2/dir3, extent records. This is
   the bible; the kernel is the cross-check.
2. **`xfsprogs` source** — `git://git.kernel.org/pub/scm/fs/xfs/xfsprogs-dev.git`.
   `db/`, `repair/`, and `libxfs/` are the read-only reference implementation.
   `xfs_db -r <image>` is what you'll use to verify our parser against ground
   truth during development.
3. **2.4.2-SGI_XFS_1.0 kernel source** (`kernel-source-2.4.2-SGI_XFS_1.0.i386.rpm`)
   — required for dir1. It's the only readable reference for the XFS v1
   directory format, which real IRIX disks still use (confirmed against a
   sampled real disk — `versionnum & DIRV2BIT == 0`). Modern Linux removed
   dir1 long before the v5.15 baseline.
4. **SGI IRIX XFS GPL drop (May 2000)** — older still. Only chase if
   2.4.2-SGI proves to be missing some IRIX-specific quirk; otherwise skip.
4. **Indy/Indigo² hardware service manuals** — only relevant if you end up
   debugging physical-disk reads on real SGI hardware. Skip unless needed.

**Bottom line:** clone modern Linus's tree **and** v5.15 stable. Grab the XFS
PDF. Don't bother with the IRIX historical tarball unless something breaks
that the modern kernel can't explain.

---

## Architectural constraints (from `CONTRIBUTING.md`)

These shape every step below; re-read CONTRIBUTING.md §"Where code lives" and
§"Adding a feature: the playbook" before starting.

- **Engine layer first.** Land `fs/efs.rs` and `fs/xfs/` with unit tests
  before touching `model/` or `gui/`.
- **No new traits, capability bitsets, or sub-traits.** Both filesystems
  implement the existing `Filesystem` trait verbatim. GUI gating goes through
  the existing `is_browsable_type` helpers in `fs/mod.rs`.
- **Type-byte routing.** SGI partitions don't have an MBR type byte, so we
  introduce **synthetic** internal type bytes (`0xA0` = SGI-XFS, `0xA1` =
  SGI-EFS) emitted only by `PartitionTable::Sgi`. Existing routers stay
  unchanged.
- **Sector-aligned I/O.** Raw character devices on macOS (`/dev/rdiskN`)
  silently EINVAL on sub-sector seeks/reads — same trap that bit HFS+
  detection. Both readers must round to 512-byte boundaries via a bounce
  buffer for every raw-device read.
- **No Unicode glyphs** in any user-visible string. Plain ASCII only.
- **Streaming I/O**, 64 KiB – 1 MiB chunks. Never load a whole partition.
- **Tests** in the same file under `#[cfg(test)]`.
- **`cargo build --all-targets`** must produce zero warnings on every PR.

---

## Test fixtures

Commit small synthetic fixtures under `tests/fixtures/sgi/`:

| File                             | Size      | How to produce                                                          |
|----------------------------------|-----------|-------------------------------------------------------------------------|
| `irix_volhdr.bin`                | 4 KiB     | `chdman extractraw -i irix65.chd -o /tmp/h.bin -ib 4096`, take first 4 KiB. Committed raw (no compression). |
| `efs_small.img.zst`              | ~550 KiB  | Slice a real EFS volume from IRIX 5.3 install media (we used `ULTRA64_2GIG_SCSI_IRIX53_confirmed2.img`). Parse the SGI volhdr, locate the type-7 (EFS) partition, `dd bs=512 skip=<first> count=8192` to take a 4 MiB head, then `zstd -19`. See "Known images on hand" for the exact offsets used. **`mkfs_efs` does NOT exist on Linux** — the v5.15 EFS module is read-only by design; the only mkfs implementation lives in IRIX userspace. Don't go hunting for it. |
| `xfs_v2_dir2_small.img.zst`      | ~17 KiB   | `truncate -s 512M xfs.img && mkfs.xfs -f -m crc=0,finobt=0 -i nrext64=0 -d agcount=2 -b size=4096 -L RUSTYTEST -p proto xfs.img`, then `zstd -19`. v4 (no CRC) with dir2 (versionnum & DIRV2 set). Populate via `-p proto` file (no sudo / loop mount needed) — modern `mkfs.xfs` rejects agcount=2 below ~64 MiB so the fixture is a 512 MiB sparse image that compresses to ~17 KiB. The 16 MiB target listed in earlier drafts is unreachable with current xfsprogs. |
| `xfs_v2_dir1_small.bin`          | ≤64 KiB   | Hand-crafted byte fixture: superblock with `versionnum & DIRV2 == 0`, one AG, one shortform-dir1 root inode with three entries, one extent-format file inode. Constructed in code (a builder helper under `#[cfg(test)]`) rather than via a real `mkfs` — no modern tool produces dir1. |

**Real-world disks are not committed** (per project preference). Reference
images live locally on the implementation machine (`~/xfs-efs/irix65.chd`,
`~/xfs-efs/SGI_imaged-001.sample.hda`, `~/xfs-efs/ULTRA64_2GIG_SCSI_IRIX53_confirmed2.img`).
When we need a "real-disk parity"
test, we'll craft a small synthetic image from the parameters captured in
"Known images on hand" rather than committing source material.

## Known images on hand

Used for manual verification at the relevant steps. **Do not commit.**

### `~/Downloads/IRIX6.5/irix65.chd`
Uncompressed CHD, 5.3 GiB / 93 GiB logical. SGI Volume Header. Partition 0
is XFS at LBA `0x41000`. Confirmed parameters (extracted via
`chdman extractraw -isb 136314880 -ib 4096`):

- XFS superblock `versionnum=0x30b4`: v4 + ATTR + NLINK + ALIGN + EXTFLG +
  **DIRV2 = true** → dir2 directory format.
- `blocksize=4096`, `inodesize=256`, `inopblock=16`, `agcount=24`,
  `agblocks=1,048,576` (4 GiB each), `dblocks=24,542,720` (~93.5 GiB
  partition), `rootino=128`, `inopblog=4`, `agblklog=20`, `blocklog=12`,
  no realtime, no CRC/v5.

This is the canonical **dir2** verification target.

### `~/Downloads/SGI_imaged-001.sample.hda`
First 5 MiB of a real SGI hard disk. Confirmed parameters (do not depend on
this file in CI — these values are documented here so a synthetic
equivalent can be built later):

- SGI Volume Header (magic `0x0BE5A941`); `bootfile = "/unix"`,
  `root_part_num = 0`, `swap_part_num = 1`.
- Volume directory entries: `sgilabel`, `ide`, `sash`.
- Partitions: `[0]` XFS first=4360 blocks=16,309,640; `[1]` RAW (swap)
  first=16,314,000 blocks=1,536,000; `[8]` VOLHDR first=0 blocks=4360;
  `[10]` VOLUME first=0 blocks=17,850,000.
- XFS superblock at LBA 4360 (byte `0x221000`): `magic=XFSB`,
  `versionnum=0x1094` (v4 + ATTR + ALIGN + EXTFLG; **DIRV2 = false** →
  dir1 directory format), `blocksize=4096`, `sectsize=512`,
  `inodesize=256`, `inopblock=16`, `agcount=8`, `agblocks=254839`,
  `dblocks=2,038,705` (~7.96 GiB), `rootino=128`,
  `inopblog=4`, `agblklog=18`, `blocklog=12`, no realtime, no CRC/v5.
- Root inode at byte offset `0x229000` from start of image (well within
  the 5 MiB sample), so superblock + AG0 headers + root inode are all
  reachable for testing.

This image is the canonical **dir1** verification target.

### `~/xfs-efs/ULTRA64_2GIG_SCSI_IRIX53_confirmed2.img`
A 14.8 GiB raw dump of an Ultra64 2 GiB SCSI disk holding an IRIX 5.3
installation. SGI Volume Header (magic `0x0BE5A941`). Confirmed parameters:

- Partitions: `[0]` EFS first=32130 blocks=7,486,290 (~3655 MiB); `[1]` RAW
  first=7,518,420 blocks=80,325; `[8]` VOLHDR first=0 blocks=32,130; `[10]`
  VOLUME first=0 blocks=7,598,745.
- EFS superblock at byte `0xFB0600` (partition first-block + 512): magic
  `0x00072959` (old-magic) at sb+28, `fs_size=7,486,242`, `fs_firstcg=1830`,
  `fs_cgfsize=95,954`, `fs_cgisize=2460`, `fs_sectors=63`, `fs_heads=10`,
  `fs_ncg=78`, volume name "noname", pack "nopack".

This is the canonical **EFS** verification target. The first 4 MiB of the
EFS partition (offset 0xFB0400 onward) is shipped as
`tests/fixtures/sgi/efs_small.img.zst` for CI.

**EFS superblock alignment gotcha:** `struct efs_super` in
`fs/efs/efs_fs_sb.h` is laid out without `__attribute__((packed))`, so the
C compiler inserts 2 bytes of padding between `fs_dirty` (`__be16` at +20)
and `fs_time` (`__be32` at +24). This shifts `fs_magic` to **sb+28**, not
sb+26. Step 3's reader must read by explicit byte offset, not by mirroring
the struct as `#[repr(C)]` — the on-disk layout depends on the compiler's
natural-alignment behaviour, which is fragile. Field offsets in bytes:

| Offset | Type      | Field         |
|--------|-----------|---------------|
| 0      | __be32    | fs_size       |
| 4      | __be32    | fs_firstcg    |
| 8      | __be32    | fs_cgfsize    |
| 12     | __be16    | fs_cgisize    |
| 14     | __be16    | fs_sectors    |
| 16     | __be16    | fs_heads      |
| 18     | __be16    | fs_ncg        |
| 20     | __be16    | fs_dirty      |
| 22     | (pad ×2)  | —             |
| 24     | __be32    | fs_time       |
| 28     | __be32    | fs_magic      |
| 32     | char[6]   | fs_fname      |
| 38     | char[6]   | fs_fpack      |
| 44     | __be32    | fs_bmsize     |
| 48     | __be32    | fs_tfree      |
| 52     | __be32    | fs_tinode     |
| 56     | __be32    | fs_bmblock    |
| 60     | __be32    | fs_replsb     |
| 64     | __be32    | fs_lastialloc |
| 68     | char[20]  | fs_spare      |
| 88     | __be32    | fs_checksum   |

---

## Implementation steps

Each step is one session: branch, code, tests, PR. Step N+1 assumes step N
has merged.

### Step 1 — Reconnaissance & fixture capture

**Goal:** produce committed fixtures and document the IRIX disk's exact
on-disk parameters before writing any code.

**Branch:** `add-sgi-fixtures`

**Tasks:**
1. `chdman extractraw -i ~/Downloads/IRIX6.5/irix65.chd -o /tmp/irix_head.bin -ib 1048576`
2. Slice the first 4 KiB into `tests/fixtures/sgi/irix_volhdr.bin`.
3. Extract partition 0 (XFS) from CHD: read the SGI volume header, find
   partition 0's first/blocks fields, `chdman extractraw` that range to
   `/tmp/irix_xfs_part0.img` (don't commit — too large).
4. Run `xfs_db -r /tmp/irix_xfs_part0.img` and dump `sb 0`. Save output to
   `docs/SGI_Filesystems_irix_sb.txt` (committed) so future maintainers have
   a known-good superblock reference.
5. Build the `xfs_v2_small.img` fixture (recipe above), commit it.
6. On a Linux VM: build the `efs_small.img` fixture, commit it.

**Acceptance:** the three fixtures are checked in; `irix_volhdr.bin` parses
by hand to magic `0x0BE5A941`, partition 0 type=10 (XFS), first-block
`0x00041000`. No code changes — this is a fixture-only PR.

---

### Step 2 — SGI Volume Header partition table

**Goal:** detect SGI partitions ahead of MBR/GPT/APM. Surface them in the
partition list with correct names. No filesystem support yet.

**Branch:** `add-sgi-partition-table`

**Files:**
- `src/partition/sgi.rs` — new
- `src/partition/mod.rs` — add `Sgi(SgiVolumeHeader)` variant; detection ahead of MBR
- (no `fs/` changes yet)

**On-disk shape (all big-endian, sector 0):**
- `0x000` magic `0x0BE5A941` (u32)
- `0x004` `root_part_num` (u16), `swap_part_num` (u16)
- `0x008` `bootfile[16]` — null-terminated path of standalone boot program
- `0x018` 48 unused bytes
- `0x048` 15 × 16-byte volume directory entries (`name[8]`, `block_num`, `bytes`)
- `0x138` 16 × 12-byte partition entries (`blocks`, `first`, `type`) — note: `0x138`, not `0x130`. Off-by-eight here will silently mis-decode.
- `0x1F8` checksum (u32) — verify but don't reject on mismatch (log a warning); some images have stale checksums.
- `0x1FC` 4-byte pad

**SGI partition type → display name mapping:**
```
0  VOLHDR     3  RAW       7  EFS      10 XFS       13 XVM
1  TRKREPL    4  BSD       8  LVOL     11 XFSLOG
2  SECREPL    5  SYSV      9  RLVOL    12 XLV
                6  VOLUME
```

**Tasks:**
1. Define `SgiVolumeHeader`, `SgiPartitionEntry`, `SgiVolumeDirEntry` structs.
2. Implement `parse(buf: &[u8; 4096]) -> Result<SgiVolumeHeader, ...>`.
3. Add `PartitionTable::Sgi(...)` variant. Update detection in
   `partition/mod.rs::PartitionTable::detect` to try SGI magic before MBR.
4. Map SGI type bytes onto `PartitionInfo`:
   - Skip `VOLHDR`, `VOLUME`, `XFSLOG`, `XLV`, `XVM`, `RAW` for browse iteration.
   - For `XFS`, set `partition_type_byte = 0xA0`, `type_name = "SGI XFS"`.
   - For `EFS`, set `partition_type_byte = 0xA1`, `type_name = "SGI EFS"`.
5. Update every exhaustive match on `PartitionTable` (compiler will list them).
   Mostly mechanical; a few will need real handling (backup iteration,
   inspect tab partition list).
6. Volume directory entries: parse and store, but don't surface in the UI
   yet (those are standalone executables — a later concern, not browse).

**Tests** (in `partition/sgi.rs`):
- Parse `tests/fixtures/sgi/irix_volhdr.bin`; assert magic, partition 0
  is XFS at first-block `0x00041000`, blocks `0x0BB3F000`.
- Round-trip a synthetic header.
- Reject obviously bad inputs (wrong magic, partition entries past disk end).

**Acceptance:** opening `irix65.chd` in `cargo run` shows one partition row
labelled "SGI XFS" at the right offset/size. Browsing it still fails (no
filesystem support yet) — that's expected.

---

### Step 3 — EFS read-only browse: superblock + root directory

**Goal:** detect EFS, list the root directory of `efs_small.img`. No file
reads yet.

**Branch:** `add-efs-step1-detect-and-root`

**Files:**
- `src/fs/efs.rs` — new
- `src/fs/mod.rs` — register synthetic byte `0xA1`

**Reference:** `fs/efs/` from the `linux-5.15-efs` clone. Read
`super.c`, `inode.c`, `dir.c` once before starting.

**On-disk shape (big-endian):**
- Superblock at byte offset 1×512 of partition. Magic is **at sb+28**, not
  at sb+0 — the kernel `struct efs_super` is not packed and the compiler
  inserts 2 bytes of natural-alignment padding before `fs_time`. See the
  full field-offset table under "Known images on hand" for IRIX 5.3. Read
  by explicit offset, not via `#[repr(C)]`. Magic value is `0x00072959`
  (old) or `0x0007295A` (new).
- Inode 2 = root directory. Inode size = 128 bytes.
- 12 inline extents in the inode: each is 16 bytes packed as
  `(magic:8, bn:24, offset:8, length:8)` — `bn` is the start block,
  `length` is in 512-byte blocks.
- Directory blocks are 512 bytes: header (`firstused:16, slots:8, magic:16`),
  slot offsets, then variable-length entries `(inumber:32, namelen:8, name[])`.

**Tasks:**
1. `EfsSuperblock` struct + `parse()`.
2. `EfsInode` struct + `read_inode(inode_num)` — translate inum to
   `(cylinder_group, offset)` using `s_cgisize`/`s_cgfsize`/`s_firstcg`,
   read the 128-byte inode.
3. `read_directory_block()` — parse one 512-byte slotted block.
4. `EfsFilesystem::root()` returns inode 2 as a `FileEntry` with type `Dir`.
5. `EfsFilesystem::list_directory()` walks the root's 12 extents, parses
   each 512-byte block, emits `FileEntry`s.
6. `volume_label()` from `s_fname` + `s_fpack` (Latin-1, trim).
7. `fs_type()` returns `"EFS"`.
8. `total_size()` = `s_size × 512`. `used_size()` = `total_size()` for now.
9. `last_data_byte()` = `total_size()` (no trim yet).
10. Wire `0xA1` into `fs/mod.rs::open_filesystem` and `is_browsable_type`.

**Tests:**
- Parse superblock from `efs_small.img`.
- `list_directory(root)` returns the expected entries (whatever you put on
  the fixture).
- Bad-magic rejection.
- Inode-number → CG translation: hand-compute one case, assert.

**Acceptance:** `cargo test --lib efs` green. Manual: open an EFS-only image
in the GUI, browse to `/`, see entries.

---

### Step 4 — EFS file reads + recursive browse

**Goal:** read file contents; descend into subdirectories.

**Branch:** `add-efs-step2-read-files`

**Files:**
- `src/fs/efs.rs` — extend

**Tasks:**
1. `read_file(entry, max_bytes)`: walk the 12 extents, read each contiguous
   range into a `Vec<u8>` capped at `max_bytes`. EFS has no indirect blocks,
   so file size is bounded by 12 × 16 MiB = 192 MiB per file.
2. `write_file_to(entry, writer)`: stream the same extents in 1 MiB chunks
   directly into the writer (no full buffering).
3. **Sector-aligned read helper.** Add `read_at_aligned(file, off, len)` —
   rounds to 512, reads to bounce buffer, slices out window. Use it for every
   read. Critical for raw-device support on macOS (see the architectural
   note).
4. `list_directory()` already works; just verify it descends correctly into
   subdirectories from step 3.
5. Symlinks: EFS stores the target as the file contents (mode bit 0o120000).
   Surface as `EntryType::Symlink` with `target = read_file(entry).
   to_string()`.

**Tests:**
- Read a known small file from the fixture, byte-compare.
- Read a file that crosses an extent boundary (build a fixture file with a
  hole-filled middle block).
- Read a symlink, assert target.
- `write_file_to` produces identical bytes to `read_file`.

**Acceptance:** EFS browse + extract round-trip works in the GUI for both an
image and (manually verified) a USB-attached EFS disk.

---

### Step 5 — XFS superblock & inode lookup (no directory parsing yet)

**Goal:** open an XFS partition, read the root inode, report its mode/size.
No directory listing or file reads yet — this step verifies the lookup math
is correct before building anything on top.

**Branch:** `add-xfs-step1-sb-and-inode`

**Files:**
- `src/fs/xfs/mod.rs` — new submodule
- `src/fs/xfs/sb.rs`
- `src/fs/xfs/types.rs` — shared on-disk structs
- `src/fs/xfs/ag.rs`
- `src/fs/xfs/inode.rs`
- `src/fs/mod.rs` — register synthetic byte `0xA0`

**Reference:** "XFS Algorithms & Data Structures" PDF chapters 1–5; cross-
check against `fs/xfs/libxfs/xfs_format.h` and `xfs_sb.c`. `xfs_db -r
xfs_v2_small.img -c 'sb 0' -c 'p'` for ground truth.

**Hard guards in `sb.rs`:**
- Magic `XFSB` (`0x58465342`).
- Reject `sb_versionnum & XFS_SB_VERSION_NUMBITS == 5` (CRC/v5) — log a
  clear "v5 XFS not supported" message and bail. IRIX won't have it.
- Reject `sb_rextents > 0` (realtime device) — out of scope.
- Validate `sb_blocksize` is a power of two in [512, 65536].
- Validate `sb_sectsize` ≥ 512.

**Directory-format detection (must surface on `XfsFilesystem`):**
The `DIRV2` bit (`0x2000`) of `sb_versionnum` selects dir1 vs dir2. Both
formats exist in the wild on IRIX disks (the sampled real disk is dir1;
modern `mkfs.xfs` always produces dir2). Decode this bit, store it on the
`XfsFilesystem` struct as a `dir_format: DirFormat` enum, and:
- log it at open time via `log_cb` (e.g. `XFS dir format: dir1` / `dir2`),
- have `list_directory` and `read_file` (when reading a directory) dispatch
  on it. In Step 6 only the dir2 arm is implemented; the dir1 arm returns
  `Unsupported("XFS dir1 not yet supported")` and is filled in by Step 6b.

**Graceful EOF requirement:** all reads in `xfs/` and `efs/` must treat a
short read past the end of the underlying file/device as
`FilesystemError::Io(UnexpectedEof)` with the partition byte offset, never
panic. Truncated samples (e.g. the 5 MiB SGI sample) and damaged disks both
produce this case; tests must cover it.

**Tasks:**
1. `types.rs`: struct definitions for `XfsDsb`, `XfsAgf`, `XfsAgi`,
   `XfsAgfl`, `XfsDinodeCore`. Big-endian throughout.
2. `sb.rs::parse()` with the guards above. Extract `blocksize`,
   `sectsize`, `dblocks`, `agcount`, `agblocks`, `inopblock`, `rootino`,
   `inodesize`, `agblklog`, `inopblog`, `dirblklog`, `versionnum`,
   `features2`, `features_compat/incompat/ro_compat`, `fname[12]`.
3. `ag.rs`: read AGF/AGI for a given AG number. (We don't use AGF yet but
   parse it now for completeness.)
4. `inode.rs::read_inode(ino)`:
   - `agno = ino >> (sb.agblklog + sb.inopblog)`
   - `agbno = (ino >> sb.inopblog) & ((1 << sb.agblklog) - 1)`
   - `offset = ino & ((1 << sb.inopblog) - 1)`
   - byte offset = `(agno × sb.agblocks + agbno) × sb.blocksize + offset × sb.inodesize`
   - read `sb.inodesize` bytes, parse `XfsDinodeCore` (96 bytes).
5. `XfsFilesystem::open()` — parse sb, store `Arc<Mutex<File>>`.
6. `XfsFilesystem::root()` returns a stub `FileEntry` representing
   `sb.rootino` with the mode/size from the inode core. `list_directory`
   and `read_file` return `Unsupported` until step 6.
7. `fs_type()` → `"XFS v1"` or `"XFS v2"` based on `versionnum & 0x0F`.
8. `volume_label()`, `total_size()`, `used_size()`, `last_data_byte()`
   as in EFS step.
9. **Sector-aligned read helper** — same shape as EFS, factor into a
   small util shared by both if convenient.
10. Wire `0xA0` into `fs/mod.rs::open_filesystem` and `is_browsable_type`.

**Tests:**
- Parse superblock from `xfs_v2_small.img`.
- Inode-number arithmetic: hand-compute the byte offset of `sb.rootino`,
  assert `read_inode` reads from there.
- v5/CRC rejection.
- Realtime-device rejection.

**Acceptance:** `cargo test --lib xfs::sb` green; opening an XFS partition
in the GUI shows correct fs_type and volume label, root listing fails with
a clear "not yet implemented" error.

---

### Step 6 — XFS dir2 shortform + block directories, extent-list reader

**Goal:** list and read files from an XFS **dir2** partition where every
directory fits in shortform or one block, and every file uses extent-list
format (`di_format == 2`). Covers `xfs_v2_dir2_small.img` and any IRIX disk
with the `DIRV2` feature bit set. dir1 disks (the sampled real disk) are
covered in Step 6b.

**Branch:** `add-xfs-step2-browse`

**Files:**
- `src/fs/xfs/dir2.rs`
- `src/fs/xfs/bmap.rs`
- `src/fs/xfs/symlink.rs`
- `src/fs/xfs/inode.rs` — extend
- `src/fs/xfs/mod.rs` — wire into `Filesystem` trait

**Tasks:**
1. **`bmap.rs::decode_extent(rec: &[u8; 16]) -> XfsBmbtIrec`.**
   The packed format (big-endian, 128 bits):
   - bit 0: flag (preallocated/unwritten)
   - bits 1..54: file offset in fs blocks
   - bits 55..106: start block (52 bits)
   - bits 107..127: count (21 bits)
   Verify against the PDF and against `libxfs/xfs_bmap_btree.c`. Test on
   three hand-constructed records (one with flag set, one with large offset,
   one with large block).
2. **`inode.rs`** dispatch on `di_format`:
   - 1 (local) — fork data is the file/symlink contents inline.
   - 2 (extents) — `di_nextents` extent records inline in the fork.
   - 3 (btree) — return `Unsupported` for now (deferred to step 7).
3. **`dir2.rs::parse_shortform(fork: &[u8])`** — header (`count`, `i8count`,
   `parent`) + entries `(namelen, offset, name, inumber)`. Inumber width
   depends on `i8count > 0`. List of `FileEntry`s.
4. **`dir2.rs::parse_block(buf: &[u8])`** — single-block dir2. Magic
   `XD2B` / `XDB3`. Header + data entries (forward) + leaf entries
   (backward) + tail. We only need forward iteration for browse.
5. **`symlink.rs`**: local fork → inline string; extents fork → read first
   extent (always single-block for symlinks).
6. **`XfsFilesystem::list_directory()`** — dispatch on root inode's
   `di_format`:
   - local → shortform
   - extents with `di_size <= sb.dirblksize` → block dir
   - otherwise → return `Unsupported` (step 7)
7. **`XfsFilesystem::read_file()` / `write_file_to()`** — dispatch on
   `di_format`. For extents: walk the inline records, translate file
   blocks to disk blocks, stream in 1 MiB chunks via the aligned helper.

**Tests:**
- Shortform with 3 entries, 8 entries, one long name.
- Block directory parse from a fixture-built dir.
- Extent-list bmap on three hand-constructed records.
- Read a small file (local format), a medium file (single extent), a
  fragmented file (multiple extents).
- Local symlink + extent symlink.
- Integration: open `xfs_v2_small.img`, list `/`, descend two levels,
  read a known file, compare bytes.

**Acceptance:** GUI browse + extract works on `xfs_v2_dir2_small.img` and on
the IRIX 6.5 disk (assuming dir2; verify in Step 1) for any directory that
fits this subset. Files with btree-format extents and dirs in leaf/node
format show a clear error. dir1 disks still fail with a clear `Unsupported`
message.

---

### Step 6b — XFS dir1 directories (real-IRIX support)

**Goal:** list and read files from XFS partitions whose `DIRV2` bit is
clear — i.e., disks using the original IRIX dir1 directory format. The
sampled SGI disk (`SGI_imaged-001.sample.hda`) is the canonical target;
some IRIX 6.x systems will also use dir1.

**Branch:** `add-xfs-step2b-dir1`

**Reference:** `kernel-source-2.4.2-SGI_XFS_1.0/fs/xfs/`:
- `xfs_dir.h`, `xfs_dir.c` — dir1 entry points and dispatcher
- `xfs_dir_sf.h`, `xfs_dir_sf.c` — shortform-dir1 layout (different from dir2)
- `xfs_dir_leaf.h`, `xfs_dir_leaf.c` — leaf-dir1 layout
- `xfs_da_btree.[ch]` — shared B+tree code (same as dir2 uses)

The XFS Algorithms PDF does **not** document dir1; this RPM is the only
readable spec.

**Files:**
- `src/fs/xfs/dir1.rs` — new, parallel to `dir2.rs`
- `src/fs/xfs/mod.rs` — extend the `dir_format` dispatch added in Step 5

**On-disk shape (highlights — full layout in the 2.4.2 sources):**
- **Shortform-dir1 (`xfs_dir_shortform_t`):** header is `xfs_dir_sf_hdr_t`
  with parent inumber + `count` (u8). Each entry is
  `xfs_dir_sf_entry_t`: `inumber` (8 bytes), `namelen` (u8), `name[]`.
  Note these are NOT the same fields as dir2 shortform (no `i8count`,
  fixed 8-byte inumber width). Differentiate carefully.
- **Leaf-dir1 (`xfs_dir_leafblock_t`):** magic `0xfeeb`. Header
  (`xfs_da_blkinfo_t` + counts), leaf entries, name-list region. Naming
  conventions differ from dir2 (e.g. `xfs_dir_leaf_entry_t`).
- **Node-dir1:** uses the shared `xfs_da_intnode_t` (same code as dir2's
  node walk — the `xfs_da_btree.c` from 2.4.2-SGI is structurally
  unchanged). For browse we again only need to enumerate child blocks in
  order, not perform hash lookups.

**Tasks:**
1. Decode `xfs_dir_sf_hdr_t` + `xfs_dir_sf_entry_t` for `di_format=local`
   when the parent FS is dir1.
2. Decode leaf-dir1 blocks for `di_format=extents` when the parent FS is
   dir1 and `di_size <= sb.dirblksize`.
3. Decode node-dir1 (chained leaves) for larger directories. Like dir2 we
   walk by data-block order rather than the hash btree.
4. Wire dispatch in `xfs/mod.rs::list_directory` so dir1 cases route here
   and dir2 cases route to `dir2.rs`. File reads (extent-list / btree)
   are format-independent and reuse Steps 6 / 7 code.

**Tests** (alongside `dir1.rs`):
- Hand-built shortform-dir1 buffers: 0 entries, 1 entry, 5 entries, one
  with a 255-char name, parent inumber preserved on round-trip.
- Hand-built leaf-dir1 block (the `xfs_v2_dir1_small.bin` synthetic
  fixture covers this end-to-end).
- Integration: open `xfs_v2_dir1_small.bin` (or, locally, the `.hda`
  sample), list `/`, read a known file.

**Acceptance:** the SGI sample disk lists and extracts files cleanly. The
dir1 detection log line appears on open. dir2 path remains unaffected.

---

### Step 7 — XFS leaf/node dir2 directories + bmap btree (conditional)

**Goal:** handle the remaining XFS formats — leaf/node directories (dir2
only; dir1 leaf/node landed in Step 6b) and btree-format extents in files.
**Run `xfs_db` against `irix65.chd` first** to determine whether this is
needed for browseable files in IRIX 6.5. If not, this step can be deferred
indefinitely.

**Branch:** `add-xfs-step3-leaf-node-and-btree`

Decision check: `xfs_db -r /tmp/irix_xfs_part0.img -c 'inode <root>' -c 'p'`
recursively, looking for any inode with `core.format = 3` (btree). For each
directory: `core.format = 2` and `core.nextents > 1` → leaf format.

**Files:**
- `src/fs/xfs/dir2.rs` — extend
- `src/fs/xfs/bmap.rs` — extend with btree walker

**Tasks:**
1. **Leaf directories** (magic `XD2D` / `XD2L`): parse data block(s) +
   single leaf block. Iterate data entries by walking each data block in
   order — leaf is only needed for hash-based lookup, which we don't do for
   `list_directory`. Easy win.
2. **Node directories** (magic `XD2N`): multi-level. Walk free-space block
   to find data blocks, list them all in order. Skip the hash btree.
3. **BMBT walker** (`bmap.rs`): inode fork → btree root header → walk to
   leaf level → iterate `xfs_bmbt_rec_t` records exactly like the
   extent-list case. Use it from `read_file` when `di_format == 3`.

**Tests:**
- A fixture XFS image with a directory of 1000 entries (forces leaf or
  node format depending on dir block size).
- A fixture file with > ~20 extents (forces btree format).
- Integration: same browse test as step 6, but on a fixture that exercises
  both new paths.

**Acceptance:** every browseable inode on `irix65.chd` listable / readable.

---

### Step 7b — Modern XFS (v5 / CRC / dir3) support

**Goal:** read XFS volumes produced by modern `mkfs.xfs` — the v5/CRC
superblock format. This is the format any disk created on a current Linux
kernel uses; v4 is now IRIX-only. Routing for MBR 0x83 / GPT
"Linux Filesystem" GUID landed earlier (see `fs/mod.rs` — Step A), so once
the v5 parsers compile, browse should "just work" against `xfs_volume.chd`
and any modern-Linux XFS partition on MBR or GPT.

**Branch:** `add-xfs-v5-support`

**Reference:** `~/xfs-efs/refs/xfs-modern/xfs/libxfs/` — same tree we
already use for v4. v5 is what those files were written for; v4 is the
fallback path. The "XFS Algorithms & Data Structures" PDF covers v5/dir3
in chapters 5–9.

**Verification target:** `~/xfs-efs/xfs_volume.chd` (extracted to
`/tmp/xfs_modern.img` via `chdman extractraw`). Confirmed via
`xfs_db -r -c 'sb 0' -c 'p'`:
- `versionnum=0xb4b5`, lower nibble = 5 → v5/CRC.
- `blocksize=4096`, `inodesize=512`, `inopblock=8`, `agcount=4`,
  `agblocks=19200`, `rootino=128`, `agblklog=15`, `inopblog=3`.
- features bits typical of modern mkfs defaults (CRC, nrext64=0, FTYPE).

This is a bare XFS volume — no partition table — so the existing
superfloppy / auto-detect path (type byte 0) is what routes here once the
v5 sb is accepted.

**On-disk differences from v4 (all already documented in `xfs_format.h`):**
- **Superblock** grows past the v4 `sb_bad_features2` field. New fields:
  `sb_features_compat`, `sb_features_ro_compat`, `sb_features_incompat`,
  `sb_features_log_incompat`, `sb_crc`, `sb_spino_align`, `sb_pquotino`,
  `sb_lsn`, `sb_meta_uuid`. Total v5 sb ≈ 264 bytes (vs. v4's 208).
- **Inode core** grows from 100 bytes to 176 bytes. New fields after
  `di_next_unlinked`: `di_crc` (LE32), `di_changecount`, `di_lsn`,
  `di_flags2`, `di_cowextsize`, `di_pad2[12]`, `di_crtime`, `di_ino`,
  `di_uuid`. Fork data starts at offset **176**, not 100.
- **Directory blocks** use the "dir3" magic family and a 56-byte
  `xfs_dir3_blk_hdr` (magic + crc + blkno + lsn + uuid + owner) in
  place of the 16-byte v4 `xfs_dir2_data_hdr`. Data entries inside
  shift by +40 bytes. Magics:
  - `XDB3` (`0x58444233`) — single-block dir3 (was `XD2B`)
  - `XDD3` (`0x58444433`) — data block in larger dir3 (was `XD2D`)
  - `XDL3` (`0x58444C33`) — leaf-1 dir3 (was `XD2L` / `XD2F`)
  - `XDN3` (`0x5844334E`) — node dir3 (was `XD2N`)
- **Extent records** unchanged (same `xfs_bmbt_disk_get_all` decode).
- **Shortform** layout unchanged (`xfs_dir2_sf_*` is dir2/dir3 agnostic;
  FTYPE handling already implemented).
- **Realtime / log** unchanged; we already reject realtime.

**CRCs are not verified.** We read past them; integrity is the user's
responsibility (they can run `xfs_repair -n` separately).

**Files:**
- `src/fs/xfs/sb.rs` — accept v5; parse the trailing feature words; reject
  realtime + unsupported incompat bits (`SPINODES`, `META_UUID`, `REFLINK`,
  `RMAPBT`, `FINOBT`, `META_DIR` if they affect read paths).
- `src/fs/xfs/inode.rs` — add a `core_size` accessor (100 for v4, 176 for
  v5) so `data_fork()` knows where the fork starts.
- `src/fs/xfs/dir2.rs` — accept `XDB3` magic in `parse_block`, skip the
  extra 40 bytes of v5 header, and feed the data-entry walker the same
  layout as v4 (entries themselves are byte-identical post-header).
- `src/fs/xfs/types.rs` — extend `DiFormat` if v5 ever introduces new
  format values we care about (currently no).

**Tasks:**
1. **sb.rs**: switch from a hard "v5 reject" to a "v5 accept with feature-
   bit triage". Allow CRC, FTYPE, NREXT64=0, ATTR2, FREE_INODE_BTREE.
   Reject (with a clear message) NREXT64=1, REFLINK, RMAPBT, RT inodes.
2. **inode.rs**: switch the fork-offset constant to a method that returns
   100 or 176 based on the parsed sb version. Inode magic is unchanged
   (`IN`); the core parser already stops at offset 96 so it's
   version-agnostic — only `data_fork()` needs the new offset.
3. **dir2.rs**: in `parse_block`, accept `XDB3`. After matching, skip the
   first 56 bytes of header instead of 16 before walking entries. Keep
   the v4 path untouched.
4. **mod.rs**: thread the sb version through to `data_fork`; flip the
   "XFS dir format" log to also report v4 vs v5 ("XFS v5 / dir3" vs
   "XFS v4 / dir2" vs "XFS v4 / dir1").

**Tests:**
- New unit: v5 sb parse + dir3 block parse on hand-built buffers.
- Integration: open `/tmp/xfs_modern.img`, list `/`, read a known file
  byte-for-byte against `xfs_db -c 'inode N' -c 'dblock M'` output.
- Regression: every v4 test still passes (dir2 fixture, dir1 synthetic,
  SGI sample disk).

**Acceptance:** `xfs_volume.chd` lists `/` and extracts a known file.
The MBR-0x83 and GPT-Linux-Filesystem routing tests added in Step A
continue to pass — and now reach a successful open instead of a v5
rejection.

---

### Step 8 — Physical-disk verification & sector-alignment audit — DONE (skipped)

Marked done without execution. Rationale: the aligned-read helper
(`read_at_aligned` in `src/fs/xfs/mod.rs` and `src/fs/efs.rs`) is already
used throughout both modules, and SGI users in practice clone their SCSI
drives with a BlueSCSI rather than attaching the physical disk to a
modern host, so the raw-`/dev/rdiskN` path is not a realistic workflow
for this audience. Bounce-buffer regression test and physical-disk
matrix intentionally skipped.

### Step 8 (original plan, for reference)

**Goal:** prove the implementation works end-to-end on a real (or
emulated-as-physical) SGI disk. This is mostly verification with a small
hardening pass.

**Branch:** `sgi-fs-physical-disk-pass`

**Tasks:**
1. **Audit every `seek_read`** in `efs.rs` and `xfs/*.rs`. Every read on
   the underlying file must go through the aligned helper. Look for raw
   `seek + read` calls and replace.
2. Add an integration test that wraps the fixture file in a reader which
   panics on sub-sector reads, run the full browse test against it. This
   catches regressions automatically.
3. **Manual verification matrix** (document results in PR description):
   - `irix65.chd` opened directly via the existing CHD source.
   - `irix_xfs_part0.img` (raw extracted file).
   - USB-attached SGI disk if available, via macOS `/dev/rdiskN` and Linux
     `/dev/sdX`.
   - Same on Windows `\\.\PhysicalDriveN` if a Windows machine is handy.
4. Add `src/fs/xfs/README.md` and a section in `src/fs/README.md` table
   documenting EFS and XFS support level.

**Acceptance:** the IRIX disk browses cleanly from CHD and from raw device
on at least macOS + Linux. The bounce-buffer test passes.

---

### Step 9 — Inspect/browse GUI surface verification — DONE

Manually verified by Dani: `irix65.chd` and an EFS image both walk
cleanly through the partition list, Inspect tab, Browse view, and file
extraction. No GUI changes required.

### Step 9 (original plan, for reference)

**Goal:** confirm the GUI works without GUI code changes (which is the
point — type-byte routing should mean it Just Works). Add only the
display-string adjustments that are necessary.

**Branch:** `sgi-fs-gui-polish`

**Tasks:**
1. Run `cargo run`, open `irix65.chd`, walk through:
   - Partition list shows "SGI XFS" with correct size.
   - Inspect tab opens and lists root.
   - Browse view descends.
   - File extraction writes correct bytes.
2. Repeat for an EFS image.
3. Fix any plain-ASCII / wording issues found. **No new dialogs or
   widgets.**
4. Update `docs/TODO_missing_features.md` to record what's NOT supported
   yet (backup compaction, resize, edit) so the gap is documented.

**Acceptance:** clean walkthrough on both filesystems, on both image and
physical disk, with no GUI regressions elsewhere.

---

## Out of scope (future PRs, not part of this plan)

- **Backup compaction.** Layout-preserving compact reader for both
  filesystems. Requires walking AGF free-extent btrees (XFS) or
  cylinder-group summaries (EFS). One PR each, after browse ships.
- **Smart `last_data_byte()`.** Same dependency as compaction.
- **Resize / restore.** XFS shrink is structurally hard (inode
  relocation); IRIX users likely want byte-for-byte restore anyway. EFS
  shrink is plausible. Defer until requested.
- **Editing.** Out of scope indefinitely.
- **Volume directory standalone executables** (`sash`, `ide`, `/unix`)
  in the SGI volume header — surface in the inspect tab as virtual
  entries someday. Not browse-critical.

---

## References

- **`CONTRIBUTING.md`** — architecture & playbook. Re-read before each step.
- **`CLAUDE.md`** — top-level pointers; add a one-line entry for SGI fs
  once step 9 lands.
- **`src/fs/README.md`** — update the supported-partition table when EFS
  and XFS land.
- **`docs/progress_pattern.md`** — only relevant if/when we add background
  work for backup compaction (out of scope here).
- **XFS Algorithms & Data Structures**, 3rd ed. (SGI). External PDF.
- **Linux kernel** `fs/xfs/libxfs/` (modern) and `fs/efs/` (≤ v5.17).
- **xfsprogs** `db/`, `repair/`, `libxfs/`. Use `xfs_db` for ground truth.
