# Additional Unix Filesystems — ReiserFS + XFS gap-fill

Status: **proposed / parking-lot** (2026-06-01). Owner: Dani.

This is a follow-up roadmap split out of `virtualization-formats.md` so that
branch (`new-file-formats`) can close and merge with the modern VM-container
work done. It captures two threads:

1. **ReiserFS** — add read support before it disappears. ReiserFS is on the
   Linux kernel's removal track (marked obsolete; scheduled for deletion), so
   images of it become "legacy" — exactly the convert-from / inspect case this
   project exists to serve. **Priority.**
2. **XFS gap-fill** — we already ship a read-only XFS reader (`src/fs/xfs/`);
   this records the real holes in it so we don't re-discover them later.

Partimage (5.2 in `virtualization-formats.md`) is **parked**. Its source
(`~/Downloads/partimage-0.6.9`, GPLv2-or-later) is, however, the best
clean-room reference for the *sizing/bitmap tier* of several Unix filesystems —
see "What partimage actually gives us" below.

---

## Two capability tiers (read this first)

When we say "support filesystem X" there are two very different amounts of work:

- **Tier A — detect + size + compact.** Parse the superblock (magic, block
  size, total/free blocks) and the **free-space bitmap**. This is enough for:
  filesystem detection, the inspect tab's total/used/free display, smart
  compaction (`compact_partition_reader`), and minimum-size queries
  (`partition_minimum_size`). **No directory or file contents.**
- **Tier B — browse + read.** Parse the directory structure, inode/stat
  records, and file extents so the GUI browse view and `read_file` work. This
  is the bulk of the effort for tree-structured filesystems.

**Partimage only ever implemented Tier A.** Its per-FS handlers
(`src/client/fs/fs_*.cpp`) read the superblock and the allocation bitmap and
nothing else; `fsck()` is a no-op stub in every one. So a partimage port buys
us Tier A cheaply and contributes **nothing** to Tier B.

This split drives the phasing below: Tier A first (small, high-value, partimage
is the reference), Tier B as a separate, much larger effort with its own
references (reiserfsprogs / kernel sources).

---

## What partimage actually gives us

`~/Downloads/partimage-0.6.9/src/client/fs/` has handlers for ext2, FAT, HFS,
HPFS, JFS, NTFS, ReiserFS, UFS, XFS, AFS. Each is a `CFSBase` subclass with
three meaningful methods:

- `readSuperBlock()` — magic check, block size, total/used/free block counts.
- `readBitmap()` — locate and load the free-space bitmap into memory.
- `fsck()` — **empty stub** (no integrity checking despite the name).

For example `fs_reiser.cpp` (160 lines total):

- Superblock at fixed offset **64 KiB** (`REISERFS_DISK_OFFSET_IN_BYTES`).
- Magics: `"ReIsErFs"` → v3.5, `"ReIsEr2Fs"` → v3.6, `"ReIsEr4"` → reiser4
  (partimage does **not** handle reiser4; we won't either).
- Fields used: `s_block_count`, `s_free_blocks`, `s_blocksize`, `s_bmap_nr`
  (bitmap block count). Bitmaps live at computed block positions: the first
  right after the superblock, then one every `blocksize * 8` blocks.
- `fsck()` is empty.

So porting `fs_reiser.cpp` gives us ReiserFS **Tier A** and nothing more.

**License:** partimage is GPLv2 **"or (at your option) any later version"**
(GPLv2+), which is compatible with this project's AGPL-3.0. A port is legally
clean; credit partimage in the module header. (Provenance is also clean: it's
public open source, unlike the GHO/PQI leak concern.)

The top-level `~/Downloads/partimage-0.6.9/FORMAT` file documents the
*container* layout, not the per-FS bitmap layouts — those live in the headers
(`fs_*.h`), which carry the on-disk superblock structs we need.

---

## Architecture mapping (per CLAUDE.md / the Amiga + EFS playbook)

Adding a filesystem here is the standard `src/fs/` job — pure-Rust on-disk
parsing, **no kernel drivers, no mounting** (that's why we run on Windows and
macOS). The reference precedent is **EFS** (`src/fs/efs.rs` +
`src/fs/efs_fsck.rs` + `src/fs/efs_resize.rs` + `src/fs/unix_common/`): a
Unix-style inode/bitmap filesystem we brought up read → fsck → edit → resize.

Touch-points for any new FS, all in `src/fs/mod.rs` dispatch:

1. **`pub mod reiserfs;`** (or `xfs` already present).
2. **Detection** — ReiserFS lives in MBR type **`0x83`** (Linux native), same
   as ext/btrfs/xfs. Extend `probe_0x83_fs_type()` (and the read path at the
   `0x83` arms ~lines 381 / 1069) to seek to 64 KiB and match the ReiserFS
   magic **before** the ext/xfs checks.
3. **`fs_name_for` / `is_layout_preserving_fs` / `is_expensive_minimum` /
   `open_filesystem_by_string`** — register the new `"reiserfs"` string, mirror
   how `"xfs"` / `"efs"` are wired.
4. **`compact_partition_reader_by_string`** — Tier A compaction reader once the
   bitmap is parsed.
5. **`partition_minimum_size`** — `is_expensive_minimum` = true for any FS that
   needs a volume walk; ReiserFS Tier A (bitmap read only) is **cheap** like
   FAT/NTFS, so it can compute inline without the "Calc min" gate.

The `Filesystem` trait (`src/fs/filesystem.rs`) is what each FS implements;
`EditableFilesystem` is the optional write extension. ReiserFS and XFS edit are
**out of scope** (see below).

---

## ReiserFS plan (priority)

References, in order of trust:
- **`~/Downloads/partimage-0.6.9/src/client/fs/fs_reiser.{cpp,h}`** — Tier A
  superblock + bitmap (port directly).
- **reiserfsprogs** (`debugreiserfs`, `reiserfsck` sources, GPL) — Tier B tree
  format: the canonical clean reference for the S+tree, item heads, stat data,
  and directory items.
- **Linux `fs/reiserfs/`** (in kernel git history even after removal) — the
  authoritative on-disk structs (`reiserfs_fs.h`).

Scope: **ReiserFS v3.5 and v3.6 only** (the `ReIsErFs` / `ReIsEr2Fs` magics).
**Reject reiser4** — wholly different format, effectively extinct, partimage
never handled it. **Read-only** — no edit, no fsck, no resize (it's a
disappearing filesystem; investment caps at "read what exists").

| # | Session | Tier | Scope & deliverable |
|---|---|---|---|
| R.1 | **Detection + superblock + sizing** | A | `src/fs/reiserfs.rs`: superblock struct + parse at offset 64 KiB, magic dispatch (v3.5/v3.6, reject reiser4 with a precise error), block size / total / used / free. Wire `probe_0x83_fs_type` + `fs_name_for("reiserfs")` + `open_filesystem_by_string`. Inspect tab shows ReiserFS with correct sizes. Port from `fs_reiser.cpp`. Unit test against a hand-built superblock; gated test against a real `mkfs.reiserfs` fixture. |
| R.2 | **Free-space bitmap → compact + min-size** | A | Load the bitmap blocks (positions per `fs_reiser.cpp`: first after the superblock, then every `blocksize*8` blocks). Implement `compact_partition_reader_by_string` (stream used blocks) + `last_data_byte` so `partition_minimum_size` returns a real floor. `is_expensive_minimum` = false (bitmap read only). Round-trip compaction test. **Bitmap convention: confirm set-bit meaning** — most Linux FS use set = allocated; verify against a real image (cf. the Amiga/EFS "set = free" trap noted in CLAUDE.md). |
| R.3 | **S+tree browse + read** (the big one) | B | `read_file` + `list_directory` + `root`. Walk the single balanced S+tree: internal nodes → leaf nodes → item heads; parse stat-data (v1/v2), directory items (entry headers + name area), and indirect/direct items for file bodies (incl. **tail packing** — small file tails packed into a direct item). Read-only. Reference reiserfsprogs `debugreiserfs`. High effort — its own multi-session sub-project; **defer past the merge.** |

R.1 + R.2 (Tier A) are small and land the high-value "detect + size + back up an
existing ReiserFS partition byte-for-byte (or compacted)" capability. R.3 (Tier
B / browse) is the large piece and is explicitly a post-merge follow-up.

---

## XFS gap-fill

We already have a read-only XFS reader (`src/fs/xfs/`, ~3.2k lines): superblock,
inode lookup, dir1 (shortform + leaf/node), dir2 (shortform + block + leaf +
node), extent-list and bmap-btree file reads, symlinks, `root` /
`list_directory` / `read_file` / `write_file_to` / sizing. It is **already more
capable than partimage's `fs_xfs.cpp`** (which is Tier-A bitmap-only), so
partimage contributes nothing to XFS — these are the real holes:

| Hole | Severity | Notes |
|---|---|---|
| **XFS v5 (CRC) format** | **High** | The reader is **v4 only** (`src/fs/mod.rs` returns a "v5 not supported" error). XFS v5 (CRC-checksummed metadata, `XFS_SB_VERSION_5`) has been the **mkfs.xfs default since ~2016** — virtually every modern XFS image is v5. v5 adds CRC + UUID + owner/log-seq fields to block headers and uses the **v3 inode** (`di_version == 3`, 176-byte core with `di_crtime`, `di_ino`, `di_uuid`). Bringing up v5 = extend the superblock version parse, accept v3 dinodes, and skip/ignore the CRC trailers on the dir/bmap block headers we already walk. Largest and most worthwhile XFS item. |
| **fsck** | Low | No `fsck()` impl — `Filesystem::fsck` returns `None`. EFS has one (`efs_fsck.rs`) as the pattern if we want it; low demand for read-only inspect. |
| **Edit / resize** | Out of scope | No `EditableFilesystem`; read-only by design. XFS in-place edit is a large undertaking (free-space btrees, AGF/AGI, log) with little retro demand. Not planned. |
| **Accurate used-size / compaction** | Medium | XFS tracks free space with **per-AG free-space B+trees (AGF: bno + cnt btrees)**, not a flat bitmap, so Tier-A "used blocks" needs an AGF btree walk rather than a partimage-style bitmap read. Today sizing leans on `last_data_byte`; a proper AGF walk would tighten compaction/min-size. Medium effort, after v5. |

| # | Session | Scope & deliverable |
|---|---|---|
| X.1 | **XFS v5 read** | Parse `XFS_SB_VERSION_5` superblocks; accept v3 (176-byte) dinodes; tolerate CRC/UUID block-header trailers in dir2 + bmap-btree walks. Replace the "v5 not supported" rejection. Gated test against a default `mkfs.xfs` (v5) fixture; assert browse + read_file work. **Highest-value XFS item.** |
| X.2 | **AGF free-space walk** (optional) | Walk the AGF bno/cnt B+trees per allocation group for an exact used-block count → tighter `used_size` + compaction + min-size. After X.1. |
| X.3 | **fsck** (optional) | XFS verifier mirroring `efs_fsck.rs`. Low priority. |

Reference source is already vendored: `~/xfs-efs/refs/xfs-modern/xfs/libxfs/`
(`xfs_format.h`, `xfs_da_format.h`, `xfs_bmap_btree.c`) and
`docs/SGI_Filesystems.md`.

---

## JFS / UFS (parked, noted for completeness)

Partimage also ships `fs_jfs.cpp` (688 lines) and `fs_ufs.cpp` (227 lines) —
both Tier-A (superblock + bitmap). If demand appears:

- **UFS** — BSD/Solaris Fast File System, classic cylinder-group + inode
  layout, very well documented, and `src/fs/unix_common/` already has the
  inode/bitmap scaffolding (built for EFS) a UFS reader would reuse. Most
  tractable of the remaining Unix filesystems for a full Tier-B reader.
- **JFS** — IBM JFS2, B+tree-based; jfsutils is a clean reference. More moving
  parts than UFS.

Neither is scheduled. They are recorded here so the partimage source's value is
not forgotten when this checkout is gone.

---

## Fixture strategy (clean, self-generated)

Same approach as the GHO corpus and the rest of `src/fs/` — **we generate our
own fixtures**, so provenance is clean and we control the format matrix. Build
small loopback images in a Linux VM:

- **ReiserFS:** `mkfs.reiserfs` (reiserfsprogs — still packaged in current
  distros, but grab it *now* while it's around). A few MB, both `-v 3.5` and
  default v3.6; write a couple of files + a subdir; capture one with `notail`
  and one with tail-packing for the R.3 tail case.
- **XFS v5:** `mkfs.xfs` default → v5. Also keep a `-m crc=0` v4 image so the
  existing v4 path stays tested.

Commit only small images as in-tree regression guards (cf. GHO `6e72255`); gate
larger / generated ones behind `#[ignore]` + an env path like the existing
fixture-gated tests.

---

## How this closes the branch

`new-file-formats` merges with: Phases 1–4 (modern VM containers) + GHO/IMZ
legacy import **done**; Partimage, TD0/IMD, and E01 **not started**. TD0/IMD is
better owned by the MiSTer plan (`docs/mister_filesystem_implementation_plan.md`
§3.2, fluxfox port); Partimage is parked here; E01 remains a standalone
`virtualization-formats.md` follow-up.

This document is the parking-lot roadmap for the Unix-filesystem thread. If any
pre-merge work is wanted, **ReiserFS R.1 + R.2** (Tier A) is the smallest
high-value slice — it lands "detect + size + back up a ReiserFS partition"
before the kernel drops the filesystem. Everything else (R.3 browse, XFS v5,
JFS/UFS) is an explicit post-merge follow-up.
