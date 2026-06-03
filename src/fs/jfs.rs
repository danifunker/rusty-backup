//! JFS2 (IBM Journaled Filesystem, Linux generation) read-only support.
//!
//! Implements the [`Filesystem`] trait far enough to detect a JFS2 partition,
//! surface its on-disk version, label, and total/used/free sizes in the
//! inspect tab, and back the partition up byte-for-byte through the
//! existing layout-preserving pipeline. Tier B (browse + read via AIT +
//! xtree) and a true BMAP-B+tree compactor are follow-ups — see
//! `docs/OPEN-WORK.md` §1.3.
//!
//! # Scope (from §1.3 of `docs/OPEN-WORK.md`)
//!
//!  * **JFS2** — the only on-disk format Linux ever shipped. The kernel
//!    constant `JFS_VERSION` is `2`, but mkfs.jfs and the on-disk
//!    `s_version` field carry `1`; both 1 and 2 are accepted by the
//!    kernel and by this parser. AIX JFS1 is a wholly different
//!    on-disk format with a different magic byte and is rejected
//!    implicitly by the `"JFS1"` ASCII-magic gate.
//!
//! # Superblock location
//!
//! JFS2's primary aggregate superblock lives at byte 32768 (`SUPER1_OFF =
//! 0x8000`) — the first 32 KiB of the partition is reserved for boot/OEM
//! data. A secondary superblock lives at byte `SUPER2_OFF = 0x808000`
//! (8 MiB in) on aggregates large enough to host it.
//!
//! # Endianness
//!
//! JFS2 is **little-endian on disk**, period — the Linux implementation
//! always wrote LE regardless of host architecture (per
//! `linux/fs/jfs/jfs_filsys.h` `le32_to_cpu`-everywhere). Big-endian
//! AIX JFS1 was a different file format. We read every field as LE
//! without runtime endian dispatch.
//!
//! # References
//!
//! * `jfsutils/include/jfs_superblock.h` — authoritative on-disk struct.
//! * `jfsutils/include/jfs_filsys.h` — version + magic + flag constants.
//! * Linux `fs/jfs/jfs_superblock.h` — kernel mirror of the same struct.
//! * `partimage-0.6.9/src/client/fs/fs_jfs.cpp` — Tier-A reference port.

use std::io::{Read, Seek, SeekFrom};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};
use super::unix_common::compact::{CompactLayout, CompactSection, CompactStreamReader};
use crate::fs::CompactResult;

// ---- Constants ----

/// Byte offset of the primary aggregate superblock. Matches
/// `SUPER1_OFF = 0x8000` from `jfs_filsys.h`.
const SB_OFFSET: u64 = 0x8000;
/// Bytes read for the superblock probe. The on-disk struct is 256-ish
/// bytes; 512 covers it with sector alignment and gives us room for the
/// label + uuid block at offset 152.
const SB_READ_SIZE: usize = 512;

/// `"JFS1"` ASCII magic at SB byte offset 0.
const JFS_MAGIC: &[u8; 4] = b"JFS1";

/// Maximum `s_version` we accept. The kernel constant is `2`; mkfs.jfs
/// writes `1`. Both round-trip through the in-tree fsck and mount, so
/// we accept either. Anything outside [1, 2] is rejected as either a
/// future format or a corrupted SB.
const JFS_VERSION_MAX: u32 = 2;
const JFS_VERSION_MIN: u32 = 1;

// On-disk superblock field offsets, validated against
// `jfsutils/include/jfs_superblock.h` and cross-checked against the
// makefs-built fixture (`scripts/probe-jfs-sb.py`).
const OFF_MAGIC: usize = 0; // s_magic       u8[4]
const OFF_VERSION: usize = 4; // s_version    u32
const OFF_SIZE: usize = 8; // s_size        i64 (aggregate size in hardware blocks)
const OFF_BSIZE: usize = 16; // s_bsize       i32 (aggregate block size)
const OFF_L2BSIZE: usize = 20; // s_l2bsize     i16
const OFF_PBSIZE: usize = 24; // s_pbsize      i32 (hardware/LVM block size)
const OFF_AGSIZE: usize = 32; // s_agsize      u32 (alloc-group size in agg blocks)
const OFF_FLAG: usize = 36; // s_flag        u32
const OFF_STATE: usize = 40; // s_state       u32 (mount/recovery state)
const OFF_LOGPXD: usize = 72; // s_logpxd      pxd_t (inline log extent)
const OFF_FSCKPXD: usize = 80; // s_fsckpxd     pxd_t (inline fsck workspace)
const OFF_LABEL: usize = 152; // s_label       u8[16]

/// `pxd_t` (physical extent descriptor) is 8 bytes on disk:
///   * bytes 0..3 — low 24 bits = length, high 8 bits = addr-high
///   * bytes 4..7 — low 32 bits of address
///
/// Both length and address are measured in aggregate blocks
/// (`s_bsize`-sized units).
const PXD_LEN: usize = 8;

const LABEL_LEN: usize = 16;

// Block-size sanity limits. JFS supports 512..4096 byte aggregate
// blocks; anything outside this range is a corrupt or non-JFS image.
const MIN_BSIZE: u64 = 512;
const MAX_BSIZE: u64 = 4096;

// ---- s_state bits we care about ----

/// `FM_DIRTY` (file system was mounted and not cleanly unmounted).
/// `0` = clean. We refuse non-zero state at open and ask the user to
/// run `fsck.jfs -p` first — same shape as the UFS SU+J dirty refusal.
const FM_CLEAN: u32 = 0;

// ---- J.2 / J.3: AIT + dinode + xtree + BMAP constants ----
//
// All confirmed against `linux/fs/jfs/jfs_filsys.h` + `jfs_dinode.h` +
// `jfs_xtree.h` + `jfs_dmap.h` + a probe of the real fixture
// (`scripts/probe-jfs-{ait,bmap}.py`).

/// Page size. JFS uses 4 KiB pages for all metadata structures
/// regardless of the aggregate block size — even on a 512-byte-block
/// volume, the inode/xtree/dmap pages are 4 KiB.
const PSIZE: u64 = 4096;
/// `AIMAP_OFF = SUPER1_OFF + SIZE_OF_SUPER` — primary aggregate inode
/// allocation map. Right after the primary superblock.
const AIMAP_OFF: u64 = SB_OFFSET + PSIZE;
/// `AITBL_OFF = AIMAP_OFF + (SIZE_OF_MAP_PAGE << 1)` — primary
/// Aggregate Inode Table. Starts at byte 0xB000 with `INOSPEREXT = 32`
/// inodes packed into one extent (`INODE_EXTENT_SIZE = 16 KiB`).
const AITBL_OFF: u64 = AIMAP_OFF + 2 * PSIZE;

/// `DISIZE` — bytes per on-disk inode.
const DISIZE: u64 = 512;
/// Aggregate's reserved system-inode numbers.
#[allow(dead_code)] // J.3 consumes AGGREGATE_I for the AIT-self xtree walk
const AGGREGATE_I: u32 = 1; // self-describes the AIT
const BMAP_I: u32 = 2; // block allocation map control inode
#[allow(dead_code)] // surfaced for completeness; J.2/J.3 don't consult LOG_I
const LOG_I: u32 = 3;
#[allow(dead_code)]
const BADBLOCK_I: u32 = 4;
const FILESYSTEM_I: u32 = 16; // first fileset (user data)
/// Fileset-internal root directory inode number. Reserved like UFS's
/// inode 2: the first fileset inode (`fino=0`/`fino=1`) is reserved
/// and the root dir always lives at `fino=2`.
const FILESET_ROOT_INO: u32 = 2;
/// Maximum aggregate inode number we read via the first AIT extent.
/// FILESYSTEM_I = 16 is the highest one we care about; the kernel
/// reserves slots up to 31.
const AIT_INODES: u32 = 32;

// Dinode field byte offsets (`struct dinode` in `jfs_dinode.h`).
const DI_OFF_INOSTAMP: usize = 0; // u32
const DI_OFF_FILESET: usize = 4; // u32
const DI_OFF_NUMBER: usize = 8; // u32
#[allow(dead_code)]
const DI_OFF_GEN: usize = 12; // u32
const DI_OFF_IXPXD: usize = 16; // pxd_t (8 B)
const DI_OFF_SIZE: usize = 24; // u64
const DI_OFF_NBLOCKS: usize = 32; // u64
const DI_OFF_NLINK: usize = 40; // u32
const DI_OFF_UID: usize = 44; // u32
const DI_OFF_GID: usize = 48; // u32
const DI_OFF_MODE: usize = 52; // u32 (POSIX type in low 16, JFS flags in high)
const DI_OFF_MTIME: usize = 72; // timestruc_t (8 B; we read the seconds half)
/// Start of the type-specific area (`u._dir._dtroot` / `u._file._u2._xtroot`
/// / `u._file._u2._special.{_fastsymlink, _rdev}`).
const DI_OFF_TYPE_AREA: usize = 224;
/// Start of `di_fastsymlink` / `di_rdev` within the dinode for the
/// special variant. Consumed by J.3 for inline-symlink decoding.
const DI_OFF_FASTSYMLINK: usize = 256;
/// `di_fastsymlink` capacity in bytes.
const DI_FASTSYMLINK_LEN: usize = 128;

// ---- J.3: fileset + iag + dtree constants ----

/// `INOSPEREXT` — dinodes per inode extent (32 × 512 B = 16 KiB =
/// 4 aggregate blocks at bsize=4096).
const INOSPEREXT: u32 = 32;
/// `EXTSPERIAG` — inode-extent slots per IAG (each iag describes 128
/// possible inode extents = 4096 fileset inodes).
const EXTSPERIAG: usize = 128;
/// Byte offset of `inoext[]` (pxd_t[EXTSPERIAG]) within a 4096-byte
/// iag. Layout per `jfs_imap.h`:
///   header (72) + pad (1976) + wmap (512) + pmap (512) + inoext (1024) = 4096
const IAG_INOEXT_OFF: usize = 72 + 1976 + 512 + 512; // 3072
/// Byte offset of `pmap[]` (u32[EXTSPERIAG]) within an iag.
const IAG_PMAP_OFF: usize = 72 + 1976 + 512; // 2560

/// `dtroot` lives at dinode offset 224 and is 288 bytes (9 × 32-byte
/// slots, with slot[0] overlapping the header in the kernel's union
/// type). Active leaf entries live at slot indices 1..=8; slot 0 is
/// the header in disguise.
const DTROOT_SIZE: usize = 288;
const DT_SLOT_SIZE: usize = 32;
/// Header field offsets within the 288-byte dtroot.
#[allow(dead_code)]
const DTROOT_OFF_DASD: usize = 0; // 16 bytes (DASD quota descriptor — unused by browse)
const DTROOT_OFF_FLAG: usize = 16; // u8
const DTROOT_OFF_NEXTINDEX: usize = 17; // u8 — number of active entries
#[allow(dead_code)]
const DTROOT_OFF_FREECNT: usize = 18; // s8
#[allow(dead_code)]
const DTROOT_OFF_FREELIST: usize = 19; // s8
#[allow(dead_code)]
const DTROOT_OFF_IDOTDOT: usize = 20; // u32 — parent inode #
const DTROOT_OFF_STBL: usize = 24; // s8[8] — sorted entry index table

/// Maximum directory size we accept inline (i.e. inside the dinode's
/// dtroot). When `di_size > DT_INLINE_CAP`, the dtree has spilled to
/// off-disk leaf/internal pages — not supported in this J.3 first
/// slice. Empirically the kernel keeps a directory inline while its
/// total entry count fits in the 8 free slots (~7 short-name entries),
/// so the cap below is generous.
const DT_INLINE_CAP: u64 = 4096;

// Mode bits. The low 16 bits hold POSIX mode (S_IFMT + perms); high bits
// hold JFS attribute flags (IFJOURNAL, IDIRECTORY, etc.).
const S_IFMT: u32 = 0o170000;
const S_IFREG: u32 = 0o100000;
const S_IFDIR: u32 = 0o040000;
const S_IFLNK: u32 = 0o120000;
const S_IFBLK: u32 = 0o060000;
const S_IFCHR: u32 = 0o020000;
const S_IFIFO: u32 = 0o010000;
const S_IFSOCK: u32 = 0o140000;

// xtree (extent btree) constants.
//
// xtroot lives inline in a file/symlink dinode at offset 224 and is 288
// bytes total = 18 slots × 16 bytes. xtheader takes 2 slots (32 B),
// leaving 16 XAD entries.
const XAD_SIZE: usize = 16;
const XTHEADER_SIZE: usize = 32;
const XTROOT_SIZE: usize = 288;
const XTROOT_MAX_XAD: usize = (XTROOT_SIZE - XTHEADER_SIZE) / XAD_SIZE; // 16
#[allow(dead_code)]
const XTPAGE_SIZE: usize = 4096;

// xtheader field offsets.
const XTH_OFF_FLAG: usize = 16; // u8 — XT_INTERNAL / XT_LEAF / XT_ROOT
const XTH_OFF_NEXTINDEX: usize = 18; // u16 — slot index of next free entry
#[allow(dead_code)]
const XTH_OFF_MAXENTRY: usize = 20; // u16

// Btree node flags (`jfs_btree.h`). The high bit (0x80) is the "first
// time written" marker we observed on the BMAP fixture; we strip it
// when classifying nodes.
const BT_ROOT_FLAG: u8 = 0x01;
const BT_LEAF_FLAG: u8 = 0x02;
const BT_INTERNAL_FLAG: u8 = 0x04;
const BT_TYPE_MASK: u8 = BT_ROOT_FLAG | BT_LEAF_FLAG | BT_INTERNAL_FLAG;

// XAD (eXtent Address Descriptor) field offsets within a 16-byte slot.
#[allow(dead_code)]
const XAD_OFF_FLAG: usize = 0; // u8
const XAD_OFF_OFF1: usize = 3; // u8 (high 8 bits of 40-bit logical offset)
const XAD_OFF_OFF2: usize = 4; // u32 LE (low 32 bits of logical offset)
const XAD_OFF_LOC: usize = 8; // pxd_t (length + physical addr)

// BMAP (block allocation map) constants.
//
// Each dmap leaf page covers `BPERDMAP = 8192` aggregate blocks via a
// pmap[256]+wmap[256] pair of bitmaps at the page tail.
const L2BPERDMAP: u32 = 13;
const BPERDMAP: u64 = 1 << L2BPERDMAP; // 8192
const LPERDMAP: usize = 256; // BPERDMAP / 32 bits per word
/// Byte offset of `pmap[]` within a dmap page. The page is 4096 bytes;
/// `pmap` and `wmap` each take `LPERDMAP * 4 = 1024` bytes at the tail.
const DMAP_PMAP_OFF: usize = PSIZE as usize - LPERDMAP * 4;
/// Byte offset of `wmap[]` within a dmap page.
#[allow(dead_code)]
const DMAP_WMAP_OFF: usize = DMAP_PMAP_OFF - LPERDMAP * 4;

// dmap header field offsets.
const DMAP_OFF_NBLOCKS: usize = 0; // u32 — # blocks this dmap covers
#[allow(dead_code)]
const DMAP_OFF_NFREE: usize = 4; // u32 — # free blocks in this dmap
const DMAP_OFF_START: usize = 8; // u64 — first agg block covered

// dbmap_disk (BMAP control header at logical page 0) field offsets.
const DBMAP_OFF_MAPSIZE: usize = 0; // u64
const DBMAP_OFF_NFREE: usize = 8; // u64
const DBMAP_OFF_L2NBPERPAGE: usize = 16; // u32
#[allow(dead_code)]
const DBMAP_OFF_NUMAG: usize = 20; // u32

/// Maximum aggregate size we walk the BMAP for. Aggregates that span
/// more than this hit the `dmapctl` multi-level walker — not
/// implemented yet. `BPERDMAP * LPERCTL = 8192 * 1024 = 8 MiB blocks`
/// = 32 GiB at `bsize=4096` is the cap. Vintage hardware never has
/// volumes this big.
const MAX_BMAP_DEPTH_1_BLOCKS: u64 = BPERDMAP * 1024;

// ---- Filesystem ----

/// In-memory representation of a parsed `pxd_t`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pxd {
    /// Length in aggregate blocks.
    pub length: u32,
    /// Starting address in aggregate blocks.
    pub address: u64,
}

impl Pxd {
    /// Parse 8 on-disk bytes (always little-endian on JFS2).
    fn parse(buf: &[u8]) -> Self {
        let word0 = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let word1 = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
        // Low 24 bits of word0 = length, high 8 bits = addr1 (high byte
        // of a 40-bit address).
        let length = word0 & 0x00FF_FFFF;
        let addr_hi = ((word0 >> 24) & 0xFF) as u64;
        let addr_lo = word1 as u64;
        let address = (addr_hi << 32) | addr_lo;
        Pxd { length, address }
    }

    /// Byte offset of the **end** of this extent within the aggregate.
    /// `(address + length) * bsize`.
    pub fn end_byte(&self, bsize: u64) -> u64 {
        (self.address + self.length as u64) * bsize
    }
}

// `bsize` and `pbsize` are unused by J.1 itself once `total_size` is
// computed, but every JFS-aware follow-up consumes them — keep them.
#[allow(dead_code)]
pub struct JfsFilesystem<R> {
    pub(crate) reader: R,
    pub(crate) partition_offset: u64,
    pub(crate) version: u32,
    /// Aggregate size in hardware/LVM blocks (`s_pbsize`-sized units).
    pub(crate) size_in_pblocks: u64,
    /// Aggregate block size in bytes.
    pub(crate) bsize: u64,
    /// Hardware/LVM block size in bytes.
    pub(crate) pbsize: u64,
    pub(crate) agsize: u32,
    pub(crate) state: u32,
    pub(crate) flag: u32,
    pub(crate) logpxd: Pxd,
    pub(crate) fsckpxd: Pxd,
    /// Last byte offset we need to capture to faithfully back up the
    /// partition. Equal to `max(aggregate_end, logpxd_end, fsckpxd_end)`
    /// — the inline log lives **past** the aggregate end on most
    /// volumes, so a naive `s_size * s_pbsize` truncation would silently
    /// lose journal data. Returned by [`Filesystem::last_data_byte`].
    pub(crate) total_byte_extent: u64,
    label: Option<String>,
    /// Cached BMAP walk result. Lazily populated on the first call to
    /// `last_data_byte` / `used_size` / the compactor. `None` on open;
    /// `Some` after a successful walk; `Some(_)` is retained even
    /// across multiple calls. We never clear it — the BMAP doesn't
    /// change under us in read-only mode.
    bmap_walk_cache: Option<BmapWalk>,
}

impl<R: Read + Seek + Send> JfsFilesystem<R> {
    /// Open a JFS2 filesystem at the given partition offset. Probes only
    /// the primary aggregate SB at byte 32768 — the secondary at byte
    /// 8 MiB is used by fsck for repair and isn't consulted by Tier A.
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        // Read the SB block.
        reader.seek(SeekFrom::Start(partition_offset + SB_OFFSET))?;
        let mut sb = vec![0u8; SB_READ_SIZE];
        reader.read_exact(&mut sb)?;

        // Magic.
        if &sb[OFF_MAGIC..OFF_MAGIC + 4] != JFS_MAGIC {
            return Err(FilesystemError::Parse(format!(
                "jfs: bad magic at byte {} (expected \"JFS1\", got {:?})",
                partition_offset + SB_OFFSET,
                &sb[OFF_MAGIC..OFF_MAGIC + 4]
            )));
        }

        // Version. Linux mkfs.jfs writes 1; the kernel constant is 2.
        // Anything outside [1, 2] is either an AIX-era format we don't
        // support or a corrupt SB.
        let version = read_u32(&sb, OFF_VERSION);
        if !(JFS_VERSION_MIN..=JFS_VERSION_MAX).contains(&version) {
            return Err(FilesystemError::Unsupported(format!(
                "jfs: unsupported version {version} (expected 1 or 2; AIX JFS1 / future JFS \
                 generations are out of scope)"
            )));
        }

        // Geometry.
        let size_in_pblocks = read_i64(&sb, OFF_SIZE);
        if size_in_pblocks <= 0 {
            return Err(FilesystemError::Parse(format!(
                "jfs: s_size {size_in_pblocks} is not positive"
            )));
        }
        let size_in_pblocks = size_in_pblocks as u64;

        let bsize = read_i32(&sb, OFF_BSIZE) as i64;
        if bsize <= 0 {
            return Err(FilesystemError::Parse(format!(
                "jfs: s_bsize {bsize} is not positive"
            )));
        }
        let bsize = bsize as u64;
        if !(MIN_BSIZE..=MAX_BSIZE).contains(&bsize) || !bsize.is_power_of_two() {
            return Err(FilesystemError::Parse(format!(
                "jfs: implausible aggregate block size {bsize} (expected power of two in \
                 [{MIN_BSIZE}, {MAX_BSIZE}])"
            )));
        }

        let l2bsize = read_i16(&sb, OFF_L2BSIZE);
        if l2bsize <= 0 || (1u64 << l2bsize as u32) != bsize {
            return Err(FilesystemError::Parse(format!(
                "jfs: s_l2bsize {l2bsize} doesn't match s_bsize {bsize}"
            )));
        }

        let pbsize = read_i32(&sb, OFF_PBSIZE) as i64;
        if pbsize <= 0 {
            return Err(FilesystemError::Parse(format!(
                "jfs: s_pbsize {pbsize} is not positive"
            )));
        }
        let pbsize = pbsize as u64;
        if pbsize > bsize {
            return Err(FilesystemError::Parse(format!(
                "jfs: hardware block size {pbsize} larger than aggregate block size {bsize}"
            )));
        }

        let agsize = read_u32(&sb, OFF_AGSIZE);
        let flag = read_u32(&sb, OFF_FLAG);
        let state = read_u32(&sb, OFF_STATE);

        // Refuse dirty volumes. JFS2's mount path replays the inline
        // log; reading a dirty aggregate without that replay risks
        // silently dropping in-flight metadata updates. Same shape as
        // the UFS SU+J dirty refusal — defer to `fsck.jfs -p` first.
        if state != FM_CLEAN {
            return Err(FilesystemError::Unsupported(format!(
                "JFS: aggregate is not cleanly unmounted (s_state = 0x{state:08X}); \
                 run `fsck.jfs -p` on the source first"
            )));
        }

        // Inline-log + fsck-workspace extents. Both can sit *past* the
        // FS-claimed aggregate end on real volumes; we need the maximum
        // of all three for a faithful backup extent.
        let logpxd = Pxd::parse(&sb[OFF_LOGPXD..OFF_LOGPXD + PXD_LEN]);
        let fsckpxd = Pxd::parse(&sb[OFF_FSCKPXD..OFF_FSCKPXD + PXD_LEN]);

        // Volume label. `mkfs.jfs -L name` writes here.
        let label_bytes = &sb[OFF_LABEL..OFF_LABEL + LABEL_LEN];
        let label = parse_label(label_bytes);

        // Compute the total byte extent we need for backup. The
        // aggregate's bookkeeping is in hardware blocks (s_size *
        // s_pbsize); the inline log / fsck workspace are in aggregate
        // blocks (s_bsize). They may extend past the aggregate end —
        // typical mkfs layouts place the log at the tail of the
        // partition immediately after the fsckpxd region.
        let aggregate_end = size_in_pblocks * pbsize;
        let log_end = logpxd.end_byte(bsize);
        let fsck_end = fsckpxd.end_byte(bsize);
        let total_byte_extent = aggregate_end.max(log_end).max(fsck_end);

        Ok(Self {
            reader,
            partition_offset,
            version,
            size_in_pblocks,
            bsize,
            pbsize,
            agsize,
            state,
            flag,
            logpxd,
            fsckpxd,
            total_byte_extent,
            label,
            bmap_walk_cache: None,
        })
    }

    /// Lazy-populate the BMAP cache. Returns `Ok(())` on success **or**
    /// on a "BMAP too large to walk in-tree" outcome (`walk_bmap`
    /// returns `Ok(None)`); only honest I/O / parse errors propagate.
    /// On error the cache stays `None`; callers fall back to the
    /// conservative log-aware extent computed at open time.
    fn ensure_bmap_cache(&mut self) -> Result<(), FilesystemError> {
        if self.bmap_walk_cache.is_some() {
            return Ok(());
        }
        if let Some(walk) = self.walk_bmap()? {
            self.bmap_walk_cache = Some(walk);
        }
        Ok(())
    }

    /// Force a BMAP walk and return a borrow of the cached result. Used
    /// by `CompactJfsReader::new` to drive the compactor.
    pub(crate) fn require_bmap_walk(&mut self) -> Result<&BmapWalk, FilesystemError> {
        self.ensure_bmap_cache()?;
        self.bmap_walk_cache.as_ref().ok_or_else(|| {
            FilesystemError::Unsupported(
                "JFS: aggregate exceeds the in-tree BMAP walker (≥ 2^23 blocks; \
                 dmapctl multi-level walker is a follow-up)"
                    .into(),
            )
        })
    }

    // ---- J.3: fileset + iag + dtree + per-user-inode read ----

    /// Read fileset 1's IAG (Inode Allocation Group) page 0 from the
    /// FILESYSTEM_I inode's data area. Returns the parsed inoext[]
    /// PXD array — slot `extent_idx` describes the disk extent that
    /// holds fileset inodes `extent_idx * INOSPEREXT .. +INOSPEREXT`.
    pub(crate) fn read_fileset_iag(&mut self) -> Result<FilesetIag, FilesystemError> {
        let fs_ino = self.read_ait_inode(FILESYSTEM_I)?;
        let xtroot = self.parse_inline_xtree_root(&fs_ino.raw)?;
        if xtroot.flag & BT_TYPE_MASK & BT_INTERNAL_FLAG != 0 {
            return Err(FilesystemError::Unsupported(
                "JFS: FILESYSTEM_I xtree has internal nodes — multi-IAG layouts \
                 (filesets with > 4096 inodes) not yet supported"
                    .into(),
            ));
        }
        if xtroot.xads.is_empty() {
            return Err(FilesystemError::Parse(
                "jfs: FILESYSTEM_I inode has no xtree entries".into(),
            ));
        }
        // FILESYSTEM_I data: page 0 = dinomap_disk header (we skip
        // it), page 1 = first IAG. The first XAD must cover both.
        let iag_page = self.read_inode_logical_page(&xtroot, 1)?;
        let agstart = u64::from_le_bytes(iag_page[0..8].try_into().unwrap());
        let iagnum = u32::from_le_bytes(iag_page[8..12].try_into().unwrap());
        if iagnum != 0 {
            return Err(FilesystemError::Parse(format!(
                "jfs: first IAG has iagnum {iagnum} != 0"
            )));
        }

        let mut pmap = [0u32; EXTSPERIAG];
        for (i, slot) in pmap.iter_mut().enumerate() {
            let off = IAG_PMAP_OFF + i * 4;
            *slot = u32::from_le_bytes(iag_page[off..off + 4].try_into().unwrap());
        }

        let mut inoext = Vec::with_capacity(EXTSPERIAG);
        for i in 0..EXTSPERIAG {
            let off = IAG_INOEXT_OFF + i * 8;
            inoext.push(Pxd::parse(&iag_page[off..off + 8]));
        }
        Ok(FilesetIag {
            agstart,
            iagnum,
            pmap,
            inoext,
        })
    }

    /// Read logical page `logical_page` from an inode's data area via
    /// the inline xtree root. Used by both BMAP and FILESYSTEM_I walks.
    /// Returns a 4 KiB page buffer.
    fn read_inode_logical_page(
        &mut self,
        xtroot: &XtreeRoot,
        logical_page: u64,
    ) -> Result<Vec<u8>, FilesystemError> {
        self.read_bmap_logical_page(xtroot, logical_page)
    }

    /// Read fileset inode `fino` (a user-fileset inode number) via the
    /// IAG's inoext[] array. The dinode lives at byte
    /// `inoext[fino/32].address * bsize + (fino % 32) * DISIZE`.
    pub(crate) fn read_fileset_inode(
        &mut self,
        fino: u32,
        iag: &FilesetIag,
    ) -> Result<JfsDinode, FilesystemError> {
        let extent_idx = (fino / INOSPEREXT) as usize;
        let in_extent = (fino % INOSPEREXT) as u64;
        if extent_idx >= EXTSPERIAG {
            return Err(FilesystemError::Unsupported(format!(
                "jfs: fileset inode {fino} would require IAG {} (only IAG 0 \
                 supported in this first slice)",
                fino / (INOSPEREXT * EXTSPERIAG as u32)
            )));
        }
        let pxd = iag.inoext[extent_idx];
        if pxd.length == 0 {
            return Err(FilesystemError::Parse(format!(
                "jfs: fileset inode {fino}: inoext[{extent_idx}] is empty \
                 (extent not allocated)"
            )));
        }
        // Verify the pmap bit is set — bit (31 - extent_idx%32) of
        // word extent_idx/32, MSB-first.
        let pmap_word = iag.pmap[extent_idx / 32];
        let pmap_bit_pos = 31 - (extent_idx % 32);
        if (pmap_word >> pmap_bit_pos) & 1 != 1 {
            return Err(FilesystemError::Parse(format!(
                "jfs: fileset inode {fino}: pmap says extent {extent_idx} is free"
            )));
        }

        let byte = self.partition_offset + pxd.address * self.bsize + in_extent * DISIZE;
        self.reader.seek(SeekFrom::Start(byte))?;
        let mut buf = [0u8; DISIZE as usize];
        self.reader.read_exact(&mut buf)?;
        JfsDinode::parse(&buf, fino)
    }

    /// Walk an inline dtroot and return one decoded entry per active
    /// stbl slot. Refuses non-inline dtrees (di_size > DT_INLINE_CAP)
    /// with a clear error — multi-page dtree walkers are a J.3
    /// follow-up.
    pub(crate) fn parse_inline_dtree(
        &self,
        dir_inode: &JfsDinode,
    ) -> Result<Vec<DtreeEntry>, FilesystemError> {
        if dir_inode.size > DT_INLINE_CAP {
            return Err(FilesystemError::Unsupported(format!(
                "JFS: directory inode {} has size {} > {} (multi-page \
                 dtree walker not yet implemented)",
                dir_inode.di_number, dir_inode.size, DT_INLINE_CAP
            )));
        }
        if dir_inode.raw.len() < DI_OFF_TYPE_AREA + DTROOT_SIZE {
            return Err(FilesystemError::Parse(format!(
                "jfs: directory inode {} buffer too small for dtroot",
                dir_inode.di_number
            )));
        }
        let dtroot = &dir_inode.raw[DI_OFF_TYPE_AREA..DI_OFF_TYPE_AREA + DTROOT_SIZE];
        let flag = dtroot[DTROOT_OFF_FLAG];
        if flag & BT_TYPE_MASK & BT_INTERNAL_FLAG != 0 {
            return Err(FilesystemError::Unsupported(format!(
                "JFS: directory inode {} dtree has internal nodes — multi-page \
                 dtree walker not yet implemented",
                dir_inode.di_number
            )));
        }
        let nextindex = dtroot[DTROOT_OFF_NEXTINDEX] as usize;
        if nextindex > 8 {
            return Err(FilesystemError::Parse(format!(
                "jfs: directory inode {} dtroot nextindex {nextindex} > 8",
                dir_inode.di_number
            )));
        }
        let stbl = &dtroot[DTROOT_OFF_STBL..DTROOT_OFF_STBL + 8];

        let mut entries = Vec::with_capacity(nextindex);
        for (i, raw_slot) in stbl.iter().enumerate().take(nextindex) {
            let slot_idx = *raw_slot as i8 as i32;
            if !(0..=8).contains(&slot_idx) {
                return Err(FilesystemError::Parse(format!(
                    "jfs: directory inode {} stbl[{i}] = {slot_idx} out of range",
                    dir_inode.di_number
                )));
            }
            let slot_off = slot_idx as usize * DT_SLOT_SIZE;
            let slot = &dtroot[slot_off..slot_off + DT_SLOT_SIZE];
            // ldtentry: inumber u32 + next s8 + namlen u8 + name[11] u16 + index u32
            let inumber = u32::from_le_bytes(slot[0..4].try_into().unwrap());
            let mut next = slot[4] as i8 as i32;
            let namlen = slot[5] as usize;
            // First 11 UCS-2 chars live in the entry slot.
            let mut name_buf: Vec<u16> = Vec::with_capacity(namlen);
            let take_first = namlen.min(11);
            for c in 0..take_first {
                let cp_off = 6 + c * 2;
                let cp = u16::from_le_bytes(slot[cp_off..cp_off + 2].try_into().unwrap());
                name_buf.push(cp);
            }
            // Walk the continuation chain for remaining chars.
            let mut remaining = namlen.saturating_sub(11);
            while remaining > 0 {
                if !(0..=8).contains(&next) {
                    return Err(FilesystemError::Parse(format!(
                        "jfs: directory inode {} name-continuation next slot {next} \
                         out of range (remaining={remaining})",
                        dir_inode.di_number
                    )));
                }
                let cont_off = next as usize * DT_SLOT_SIZE;
                let cont_slot = &dtroot[cont_off..cont_off + DT_SLOT_SIZE];
                // dtslot: next s8 + cnt s8 + name[15] u16
                let cont_next = cont_slot[0] as i8 as i32;
                let cont_cnt = cont_slot[1] as usize;
                let take = cont_cnt.min(15).min(remaining);
                for c in 0..take {
                    let cp_off = 2 + c * 2;
                    let cp = u16::from_le_bytes(cont_slot[cp_off..cp_off + 2].try_into().unwrap());
                    name_buf.push(cp);
                }
                remaining = remaining.saturating_sub(take);
                next = cont_next;
            }
            let name = String::from_utf16_lossy(&name_buf);
            entries.push(DtreeEntry { inumber, name });
        }
        Ok(entries)
    }

    /// Read a user file's data fork. Walks the inline xtree root only
    /// (no xtpage chase yet) — refuses files larger than ~64 KiB of
    /// data spread across more than 16 XADs with a clear error.
    pub(crate) fn read_file_data(
        &mut self,
        file_inode: &JfsDinode,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let xtroot = self.parse_inline_xtree_root(&file_inode.raw)?;
        if xtroot.flag & BT_TYPE_MASK & BT_INTERNAL_FLAG != 0 {
            return Err(FilesystemError::Unsupported(format!(
                "JFS: file inode {} has internal-node xtree (multi-level xtpage \
                 walker not yet implemented)",
                file_inode.di_number
            )));
        }
        let total = (file_inode.size as usize).min(max_bytes);
        let mut out: Vec<u8> = Vec::with_capacity(total);
        for xad in &xtroot.xads {
            if out.len() >= total {
                break;
            }
            let logical_start_byte = xad.logical_offset * self.bsize;
            // If the XAD's logical start is past where we are, the
            // gap is sparse — emit zeros up to it.
            if logical_start_byte as usize > out.len() {
                let gap = (logical_start_byte as usize - out.len()).min(total - out.len());
                out.resize(out.len() + gap, 0);
                if out.len() >= total {
                    break;
                }
            }
            let extent_bytes = xad.physical_length as usize * self.bsize as usize;
            let take = extent_bytes.min(total - out.len());
            let byte = self.partition_offset + xad.physical_address * self.bsize;
            self.reader.seek(SeekFrom::Start(byte))?;
            let start = out.len();
            out.resize(start + take, 0);
            self.reader.read_exact(&mut out[start..start + take])?;
        }
        // Pad with zeros to file_size if there's a sparse tail.
        if out.len() < total {
            out.resize(total, 0);
        }
        Ok(out)
    }

    /// Decode an inline symlink target from `di_fastsymlink`. For
    /// non-inline symlinks the target lives in the xtree-managed
    /// data fork — falls back to `read_file_data`.
    fn read_symlink_target(&mut self, link_inode: &JfsDinode) -> Result<String, FilesystemError> {
        let size = link_inode.size as usize;
        if size == 0 {
            return Ok(String::new());
        }
        if size <= DI_FASTSYMLINK_LEN {
            // Inline. di_fastsymlink lives at +256 in the dinode.
            let bytes = &link_inode.raw[DI_OFF_FASTSYMLINK..DI_OFF_FASTSYMLINK + size];
            return Ok(String::from_utf8_lossy(bytes).into_owned());
        }
        let bytes = self.read_file_data(link_inode, size)?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    /// Build a `FileEntry` from a fileset dinode and a name + parent.
    fn build_user_entry(
        &mut self,
        name: &str,
        parent_path: &str,
        child_dinode: &JfsDinode,
    ) -> Result<FileEntry, FilesystemError> {
        let path = if parent_path == "/" {
            format!("/{name}")
        } else {
            format!("{parent_path}/{name}")
        };
        let loc = child_dinode.di_number as u64;
        let mode = child_dinode.mode;
        let posix_type = mode & S_IFMT;
        let mut entry = match posix_type {
            S_IFDIR => FileEntry::new_directory(name.to_string(), path, loc),
            S_IFLNK => {
                let target = self.read_symlink_target(child_dinode)?;
                FileEntry::new_symlink(name.to_string(), path, child_dinode.size, loc, target)
            }
            S_IFREG => FileEntry::new_file(name.to_string(), path, child_dinode.size, loc),
            S_IFBLK => FileEntry::new_special(name.to_string(), path, loc, "block device".into()),
            S_IFCHR => FileEntry::new_special(name.to_string(), path, loc, "char device".into()),
            S_IFIFO => FileEntry::new_special(name.to_string(), path, loc, "fifo".into()),
            S_IFSOCK => FileEntry::new_special(name.to_string(), path, loc, "socket".into()),
            _ => FileEntry::new_file(name.to_string(), path, child_dinode.size, loc),
        };
        // Carry mode + uid + gid through to the browse view; only the
        // low 16 bits are the POSIX mode that `unix_mode_string`
        // understands.
        entry.mode = Some(mode & 0xFFFF);
        entry.uid = Some(child_dinode.uid);
        entry.gid = Some(child_dinode.gid);
        if child_dinode.mtime_seconds != 0 {
            entry.modified = Some(super::unix_common::inode::format_unix_timestamp(
                child_dinode.mtime_seconds as i64,
            ));
        }
        Ok(entry)
    }

    /// Read-back accessors.
    pub fn version(&self) -> u32 {
        self.version
    }

    pub fn aggregate_block_size(&self) -> u64 {
        self.bsize
    }

    pub fn hardware_block_size(&self) -> u64 {
        self.pbsize
    }

    pub fn alloc_group_size(&self) -> u32 {
        self.agsize
    }

    pub fn logpxd(&self) -> Pxd {
        self.logpxd
    }

    pub fn fsckpxd(&self) -> Pxd {
        self.fsckpxd
    }

    // ---- J.2: dinode + xtree + BMAP walk ----

    /// Byte offset (relative to partition start) of AIT inode `inum`.
    /// The first AIT extent holds inodes 0..31 packed back-to-back as
    /// 512-byte dinodes starting at byte 0xB000.
    pub(crate) fn ait_inode_byte_offset(&self, inum: u32) -> u64 {
        AITBL_OFF + inum as u64 * DISIZE
    }

    /// Read AIT inode `inum` (one of `AGGREGATE_I`, `BMAP_I`, `LOG_I`,
    /// `BADBLOCK_I`, `FILESYSTEM_I`, ...). Refuses inum > 31 (would
    /// require chasing the AIT's xtree to follow-on extents — not
    /// implemented yet; the first extent already covers every system
    /// inode JFS defines).
    pub(crate) fn read_ait_inode(&mut self, inum: u32) -> Result<JfsDinode, FilesystemError> {
        if inum >= AIT_INODES {
            return Err(FilesystemError::Unsupported(format!(
                "jfs: AIT inode {inum} requires walking a multi-extent AIT \
                 (not yet implemented; first extent holds inodes 0..{})",
                AIT_INODES - 1
            )));
        }
        let byte = self.partition_offset + self.ait_inode_byte_offset(inum);
        self.reader.seek(SeekFrom::Start(byte))?;
        let mut buf = [0u8; DISIZE as usize];
        self.reader.read_exact(&mut buf)?;
        JfsDinode::parse(&buf, inum)
    }

    /// Walk the inline xtree root of a dinode and return its XAD
    /// entries. The xtroot lives at offset 224 of the dinode; the
    /// header takes 2 slots (32 bytes) and the remaining 16 slots hold
    /// XADs. `nextindex` is the *slot* index of the next free entry —
    /// real XAD count = `nextindex - 2`.
    pub(crate) fn parse_inline_xtree_root(
        &self,
        dinode_buf: &[u8],
    ) -> Result<XtreeRoot, FilesystemError> {
        if dinode_buf.len() < DI_OFF_TYPE_AREA + XTROOT_SIZE {
            return Err(FilesystemError::Parse(format!(
                "jfs: dinode buffer too small for xtree root ({} < {})",
                dinode_buf.len(),
                DI_OFF_TYPE_AREA + XTROOT_SIZE
            )));
        }
        let root = &dinode_buf[DI_OFF_TYPE_AREA..DI_OFF_TYPE_AREA + XTROOT_SIZE];
        let flag = root[XTH_OFF_FLAG];
        let nextindex = u16::from_le_bytes([root[XTH_OFF_NEXTINDEX], root[XTH_OFF_NEXTINDEX + 1]]);
        // nextindex is in slot units (16 bytes/slot). Header takes 2
        // slots; real XADs start at slot 2.
        if nextindex < 2 {
            return Err(FilesystemError::Parse(format!(
                "jfs: xtree root nextindex {nextindex} < 2 (header takes 2 slots)"
            )));
        }
        let xad_count = (nextindex as usize) - 2;
        if xad_count > XTROOT_MAX_XAD {
            return Err(FilesystemError::Parse(format!(
                "jfs: xtree root has {xad_count} XADs (max {XTROOT_MAX_XAD})"
            )));
        }
        let mut xads = Vec::with_capacity(xad_count);
        for i in 0..xad_count {
            let xad_off = XTHEADER_SIZE + i * XAD_SIZE;
            xads.push(Xad::parse(&root[xad_off..xad_off + XAD_SIZE]));
        }
        Ok(XtreeRoot { flag, xads })
    }

    /// Walk the BMAP control inode (#2) and return per-page pmap data
    /// plus the highest-allocated aggregate block. Returns `None` if
    /// the volume's BMAP needs a multi-level dmapctl walker (≥ 2^23
    /// blocks; vintage hardware is never this large).
    fn walk_bmap(&mut self) -> Result<Option<BmapWalk>, FilesystemError> {
        let bmap_ino = self.read_ait_inode(BMAP_I)?;
        let xtroot = self.parse_inline_xtree_root(&bmap_ino.raw)?;
        // The xtroot must be a leaf with at least one XAD pointing at
        // the BMAP file's data. Internal-node xtrees would need a
        // multi-level xtree walker (xtpage chase) that hasn't shipped.
        let node_type = xtroot.flag & BT_TYPE_MASK;
        if node_type & BT_INTERNAL_FLAG != 0 {
            return Err(FilesystemError::Unsupported(
                "JFS: BMAP control inode's xtree has internal nodes (BMAP > 256 MiB \
                 not yet supported)"
                    .into(),
            ));
        }
        if node_type & BT_LEAF_FLAG == 0 {
            return Err(FilesystemError::Parse(format!(
                "jfs: BMAP xtree root flag 0x{:02X} has no LEAF bit",
                xtroot.flag
            )));
        }
        if xtroot.xads.is_empty() {
            return Err(FilesystemError::Parse(
                "jfs: BMAP control inode has no xtree entries".into(),
            ));
        }
        // Page 0 of BMAP file = dbmap_disk header. It must be the first
        // logical block of the first XAD.
        let dbmap = self.read_bmap_logical_page(&xtroot, 0)?;
        let mapsize = u64::from_le_bytes(
            dbmap[DBMAP_OFF_MAPSIZE..DBMAP_OFF_MAPSIZE + 8]
                .try_into()
                .unwrap(),
        );
        let nfree = u64::from_le_bytes(
            dbmap[DBMAP_OFF_NFREE..DBMAP_OFF_NFREE + 8]
                .try_into()
                .unwrap(),
        );
        let l2nbperpage = u32::from_le_bytes(
            dbmap[DBMAP_OFF_L2NBPERPAGE..DBMAP_OFF_L2NBPERPAGE + 4]
                .try_into()
                .unwrap(),
        );

        // Cross-check against the SB. `mapsize` from dbmap is in
        // *aggregate* blocks; `size_in_pblocks * pbsize / bsize` is the
        // same quantity from the SB side.
        let agg_blocks_sb = (self.size_in_pblocks * self.pbsize) / self.bsize;
        if mapsize != agg_blocks_sb {
            return Err(FilesystemError::Parse(format!(
                "jfs: BMAP mapsize {mapsize} != SB-derived agg-block count {agg_blocks_sb}"
            )));
        }

        // Refuse aggregates that span more than one dmapctl level.
        if mapsize >= MAX_BMAP_DEPTH_1_BLOCKS {
            return Ok(None);
        }

        // Each dmap covers BPERDMAP = 8192 blocks. Logical page index =
        // (agg_block >> L2BPERDMAP) + 4 — the "+4" accounts for the
        // dbmap header + 2 reserved L1 ctl pages + 1 reserved L2 ctl
        // page at the start of the BMAP file. (Per the `BLKTODMAP`
        // macro in jfs_dmap.h, this is valid for all aggregates strictly
        // smaller than 2^23 blocks.)
        let n_dmaps = mapsize.div_ceil(BPERDMAP);
        let mut highest_alloc: Option<u64> = None;
        let mut allocated_blocks: u64 = 0;
        let mut runs: Vec<BmapRun> = Vec::new();
        let mut cur_state: Option<bool> = None;
        let mut cur_start: u64 = 0;

        for dmap_idx in 0..n_dmaps {
            let logical_page = dmap_idx + 4;
            let dmap_page = self.read_bmap_logical_page(&xtroot, logical_page)?;
            // dmap header: nblocks + nfree + start.
            let dmap_start = u64::from_le_bytes(
                dmap_page[DMAP_OFF_START..DMAP_OFF_START + 8]
                    .try_into()
                    .unwrap(),
            );
            let dmap_nblocks = u32::from_le_bytes(
                dmap_page[DMAP_OFF_NBLOCKS..DMAP_OFF_NBLOCKS + 4]
                    .try_into()
                    .unwrap(),
            );
            // Sanity check: dmap.start should equal dmap_idx * BPERDMAP.
            // dmap.nblocks should match `min(BPERDMAP, mapsize - start)`.
            let expect_start = dmap_idx * BPERDMAP;
            if dmap_start != expect_start {
                return Err(FilesystemError::Parse(format!(
                    "jfs: dmap {dmap_idx} start={dmap_start}, expected {expect_start}"
                )));
            }
            let expect_nblocks = (mapsize - expect_start).min(BPERDMAP) as u32;
            if dmap_nblocks != expect_nblocks {
                return Err(FilesystemError::Parse(format!(
                    "jfs: dmap {dmap_idx} nblocks={dmap_nblocks}, expected {expect_nblocks}"
                )));
            }

            // Walk the pmap. Each word covers 32 blocks MSB-first.
            // Blocks past `mapsize` have their bits forced to 1 by the
            // kernel (out-of-bounds "always allocated"); we mask them
            // out before counting / scanning.
            for word_idx in 0..LPERDMAP {
                let word_off = DMAP_PMAP_OFF + word_idx * 4;
                let word =
                    u32::from_le_bytes(dmap_page[word_off..word_off + 4].try_into().unwrap());
                for bit_in_word in 0..32u64 {
                    let block_in_dmap = (word_idx as u64) * 32 + bit_in_word;
                    let global_block = dmap_start + block_in_dmap;
                    if global_block >= mapsize {
                        // Past the FS — skip both the run accounting
                        // and the highest-alloc test.
                        continue;
                    }
                    // MSB-first: bit `31 - bit_in_word` represents this block.
                    let allocated = (word >> (31 - bit_in_word)) & 1 == 1;
                    if allocated {
                        allocated_blocks += 1;
                        highest_alloc = Some(global_block);
                    }
                    // Accumulate into the current run.
                    match cur_state {
                        None => {
                            cur_state = Some(allocated);
                            cur_start = global_block;
                        }
                        Some(prev) if prev != allocated => {
                            runs.push(BmapRun {
                                allocated: prev,
                                start_block: cur_start,
                                length: global_block - cur_start,
                            });
                            cur_state = Some(allocated);
                            cur_start = global_block;
                        }
                        Some(_) => {}
                    }
                }
            }
        }

        // Flush the final run.
        if let Some(state) = cur_state {
            let end = mapsize;
            if end > cur_start {
                runs.push(BmapRun {
                    allocated: state,
                    start_block: cur_start,
                    length: end - cur_start,
                });
            }
        }

        Ok(Some(BmapWalk {
            mapsize,
            nfree,
            l2nbperpage,
            allocated_blocks,
            highest_alloc,
            runs,
        }))
    }

    /// Read logical page `logical_page` from the BMAP control file's
    /// data area, resolving it through `xtroot` (a single-leaf xtree
    /// is the only shape we accept for BMAP — the file is always
    /// contiguous on disk by construction).
    fn read_bmap_logical_page(
        &mut self,
        xtroot: &XtreeRoot,
        logical_page: u64,
    ) -> Result<Vec<u8>, FilesystemError> {
        // Find which XAD covers this logical page, then read.
        for xad in &xtroot.xads {
            if logical_page >= xad.logical_offset
                && logical_page < xad.logical_offset + xad.physical_length as u64
            {
                let within = logical_page - xad.logical_offset;
                let phys_block = xad.physical_address + within;
                let byte = self.partition_offset + phys_block * self.bsize;
                self.reader.seek(SeekFrom::Start(byte))?;
                let mut buf = vec![0u8; PSIZE as usize];
                self.reader.read_exact(&mut buf)?;
                return Ok(buf);
            }
        }
        Err(FilesystemError::Parse(format!(
            "jfs: BMAP logical page {logical_page} not covered by any inline-xtree XAD"
        )))
    }
}

// ---- J.3: fileset / dtree types ----

/// Decoded fileset Inode Allocation Group (IAG). Page 1 of the
/// FILESYSTEM_I inode's data fork.
#[derive(Debug, Clone)]
pub struct FilesetIag {
    pub agstart: u64,
    pub iagnum: u32,
    /// pmap[i]: bit-vector for the 32 inode extents covered by this
    /// word (MSB = extent 0). Set bit = extent allocated.
    pub pmap: [u32; EXTSPERIAG],
    /// `inoext[i]` describes the on-disk extent holding fileset
    /// inodes `i * INOSPEREXT .. +INOSPEREXT`. `length = 0` means
    /// "unused slot."
    pub inoext: Vec<Pxd>,
}

/// One dtree leaf entry: a child inode + UTF-8 name.
#[derive(Debug, Clone)]
pub struct DtreeEntry {
    pub inumber: u32,
    pub name: String,
}

// ---- J.2 / J.3: dinode + xtree types ----

/// Parsed JFS on-disk inode. Holds the 512-byte raw buffer plus the
/// commonly-used decoded fields. `raw` lets downstream callers slice
/// the type-specific area (xtroot / dtroot / di_fastsymlink / di_rdev)
/// without re-fetching from disk.
#[derive(Debug, Clone)]
pub struct JfsDinode {
    pub inum: u32,
    pub inostamp: u32,
    pub fileset: u32,
    pub di_number: u32,
    pub size: u64,
    pub nblocks: u64,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    /// Full mode word (POSIX type+perms in low 16, JFS attrs in high).
    pub mode: u32,
    pub mtime_seconds: i32,
    pub ixpxd: Pxd,
    /// Full 512-byte dinode bytes; slice DI_OFF_TYPE_AREA..+288 for the
    /// xtroot / dtroot, or DI_OFF_FASTSYMLINK..+128 for an inline symlink.
    pub raw: Vec<u8>,
}

impl JfsDinode {
    fn parse(buf: &[u8], inum: u32) -> Result<Self, FilesystemError> {
        if buf.len() < DISIZE as usize {
            return Err(FilesystemError::Parse(format!(
                "jfs: dinode buffer too small ({} bytes)",
                buf.len()
            )));
        }
        let inostamp = read_u32(buf, DI_OFF_INOSTAMP);
        let fileset = read_u32(buf, DI_OFF_FILESET);
        let di_number = read_u32(buf, DI_OFF_NUMBER);
        let ixpxd = Pxd::parse(&buf[DI_OFF_IXPXD..DI_OFF_IXPXD + 8]);
        let size = u64::from_le_bytes(buf[DI_OFF_SIZE..DI_OFF_SIZE + 8].try_into().unwrap());
        let nblocks =
            u64::from_le_bytes(buf[DI_OFF_NBLOCKS..DI_OFF_NBLOCKS + 8].try_into().unwrap());
        let nlink = read_u32(buf, DI_OFF_NLINK);
        let uid = read_u32(buf, DI_OFF_UID);
        let gid = read_u32(buf, DI_OFF_GID);
        let mode = read_u32(buf, DI_OFF_MODE);
        let mtime_seconds = read_i32(buf, DI_OFF_MTIME);

        // Sanity check inostamp -- AIT inodes share a single stamp set
        // at format time. We don't validate against the SB stamp here
        // (the SB doesn't surface it directly); just ensure the dinode
        // isn't all-zero garbage.
        if di_number == 0 && fileset == 0 && size == 0 && mode == 0 && inum != 0 {
            return Err(FilesystemError::Parse(format!(
                "jfs: AIT inode {inum} is empty (di_number=0, mode=0, size=0)"
            )));
        }

        Ok(Self {
            inum,
            inostamp,
            fileset,
            di_number,
            size,
            nblocks,
            nlink,
            uid,
            gid,
            mode,
            mtime_seconds,
            ixpxd,
            raw: buf[..DISIZE as usize].to_vec(),
        })
    }

    /// POSIX file type extracted from the low 16 bits of `mode`.
    pub fn posix_type(&self) -> u32 {
        self.mode & S_IFMT
    }

    /// True iff this dinode represents a regular file.
    pub fn is_regular(&self) -> bool {
        self.posix_type() == S_IFREG
    }

    /// True iff this dinode represents a directory.
    pub fn is_directory(&self) -> bool {
        self.posix_type() == S_IFDIR
    }

    /// True iff this dinode represents a symlink.
    pub fn is_symlink(&self) -> bool {
        self.posix_type() == S_IFLNK
    }
}

/// One on-disk XAD (`xad_t`) — 16 bytes describing an extent of the
/// containing inode's data.
#[derive(Debug, Clone, Copy)]
pub struct Xad {
    pub flag: u8,
    /// File-relative starting offset in **aggregate blocks** (40-bit).
    pub logical_offset: u64,
    /// On-disk starting block (40-bit).
    pub physical_address: u64,
    /// Extent length in aggregate blocks (24-bit).
    pub physical_length: u32,
}

impl Xad {
    fn parse(buf: &[u8]) -> Self {
        let flag = buf[XAD_OFF_FLAG];
        let off1 = buf[XAD_OFF_OFF1] as u64;
        let off2 =
            u32::from_le_bytes(buf[XAD_OFF_OFF2..XAD_OFF_OFF2 + 4].try_into().unwrap()) as u64;
        let logical_offset = (off1 << 32) | off2;
        let loc = Pxd::parse(&buf[XAD_OFF_LOC..XAD_OFF_LOC + 8]);
        Self {
            flag,
            logical_offset,
            physical_address: loc.address,
            physical_length: loc.length,
        }
    }
}

/// Decoded xtree root header + XAD list.
#[derive(Debug, Clone)]
pub struct XtreeRoot {
    pub flag: u8,
    pub xads: Vec<Xad>,
}

// ---- J.2: BMAP walk result types ----

/// One contiguous run of same-state blocks within the aggregate as
/// observed in the BMAP's pmap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BmapRun {
    pub allocated: bool,
    pub start_block: u64,
    pub length: u64,
}

/// Whole-aggregate BMAP walk result.
#[derive(Debug, Clone)]
pub struct BmapWalk {
    /// Total aggregate blocks (= s_size * s_pbsize / s_bsize).
    pub mapsize: u64,
    /// Free aggregate block count (from the dbmap header).
    pub nfree: u64,
    /// `dn_l2nbperpage` from the dbmap header. 0 means "1 page per
    /// agg block."
    #[allow(dead_code)]
    pub l2nbperpage: u32,
    /// Total set bits in pmap within [0, mapsize).
    pub allocated_blocks: u64,
    /// Highest agg block index with its pmap bit set, or None if no
    /// blocks are allocated.
    pub highest_alloc: Option<u64>,
    /// Mapped vs zero runs covering [0, mapsize).
    pub runs: Vec<BmapRun>,
}

// ---- Little-endian field readers ----
//
// JFS2 is always LE on disk regardless of host architecture, so no
// runtime endian dispatch is needed. Keep helpers as free functions
// for parity with the per-struct parse paths.

fn read_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}

fn read_i32(buf: &[u8], off: usize) -> i32 {
    read_u32(buf, off) as i32
}

fn read_i16(buf: &[u8], off: usize) -> i16 {
    i16::from_le_bytes([buf[off], buf[off + 1]])
}

fn read_i64(buf: &[u8], off: usize) -> i64 {
    i64::from_le_bytes([
        buf[off],
        buf[off + 1],
        buf[off + 2],
        buf[off + 3],
        buf[off + 4],
        buf[off + 5],
        buf[off + 6],
        buf[off + 7],
    ])
}

fn parse_label(bytes: &[u8]) -> Option<String> {
    // Stop at the first NUL; trim trailing ASCII whitespace; reject the
    // empty case. Matches the same pattern used by `src/fs/ufs.rs` +
    // `src/fs/reiserfs.rs`.
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    if end == 0 {
        return None;
    }
    let s = String::from_utf8_lossy(&bytes[..end]).trim().to_string();
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

// ---- Filesystem trait impl (Tier A — read-only metadata) ----

impl<R: Read + Seek + Send> Filesystem for JfsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        let iag = self.read_fileset_iag()?;
        let root_dinode = self.read_fileset_inode(FILESET_ROOT_INO, &iag)?;
        let mut entry = FileEntry::root();
        entry.location = FILESET_ROOT_INO as u64;
        entry.mode = Some(root_dinode.mode & 0xFFFF);
        entry.uid = Some(root_dinode.uid);
        entry.gid = Some(root_dinode.gid);
        if root_dinode.mtime_seconds != 0 {
            entry.modified = Some(super::unix_common::inode::format_unix_timestamp(
                root_dinode.mtime_seconds as i64,
            ));
        }
        Ok(entry)
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        let fino = entry.location as u32;
        let iag = self.read_fileset_iag()?;
        let dir_dinode = self.read_fileset_inode(fino, &iag)?;
        if !dir_dinode.is_directory() {
            return Err(FilesystemError::Parse(format!(
                "jfs: list_directory on inode {fino} which is not a directory \
                 (mode = 0o{:o})",
                dir_dinode.mode
            )));
        }
        let raw_entries = self.parse_inline_dtree(&dir_dinode)?;
        let mut out: Vec<FileEntry> = Vec::with_capacity(raw_entries.len());
        let parent_path = entry.path.as_str();
        for raw in raw_entries {
            let child_dinode = self.read_fileset_inode(raw.inumber, &iag)?;
            let child_entry = self.build_user_entry(&raw.name, parent_path, &child_dinode)?;
            out.push(child_entry);
        }
        Ok(out)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let fino = entry.location as u32;
        let iag = self.read_fileset_iag()?;
        let dinode = self.read_fileset_inode(fino, &iag)?;
        match dinode.posix_type() {
            S_IFREG => self.read_file_data(&dinode, max_bytes),
            S_IFLNK => {
                let target = self.read_symlink_target(&dinode)?;
                let bytes = target.into_bytes();
                let take = bytes.len().min(max_bytes);
                Ok(bytes[..take].to_vec())
            }
            S_IFDIR => Err(FilesystemError::Parse(format!(
                "jfs: read_file on directory inode {fino}"
            ))),
            _ => Err(FilesystemError::Unsupported(format!(
                "jfs: cannot read special file inode {fino} (mode 0o{:o})",
                dinode.mode
            ))),
        }
    }

    fn volume_label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    fn fs_type(&self) -> &str {
        "JFS2"
    }

    fn total_size(&self) -> u64 {
        // Aggregate-claimed bytes. Doesn't include inline-log and
        // fsck-workspace bytes that sit beyond the aggregate end —
        // those are reflected in `last_data_byte` instead.
        self.size_in_pblocks * self.pbsize
    }

    fn used_size(&self) -> u64 {
        // Best-effort: walk the BMAP for the actual (used = mapsize -
        // nfree) figure; on any error during the walk, fall back to
        // total_size. The `Filesystem::used_size` trait method is
        // `&self`, so cache the BMAP through a separate `&mut self`
        // call. For Tier A correctness we conservatively report
        // total_size when the cache is empty.
        match &self.bmap_walk_cache {
            Some(walk) => walk.allocated_blocks * self.bsize,
            None => self.total_size(),
        }
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        // Lazy-walk the BMAP on first call so the inspect tab + the
        // backup pipeline both get the tighter (highest-allocated)
        // extent rather than the full partition. On any walk error,
        // fall back to the conservative `total_byte_extent` (= J.1's
        // log-aware extent). Backups are always correct; the only
        // downside of the fallback is missed compaction opportunities.
        self.ensure_bmap_cache().ok();
        if let Some(walk) = &self.bmap_walk_cache {
            // last_data_byte must still capture the inline log + fsck
            // workspace even when the aggregate-internal highest is
            // small. The log on real images lives at the partition
            // tail past the aggregate.
            let agg_high_byte = walk
                .highest_alloc
                .map(|b| (b + 1) * self.bsize)
                .unwrap_or(0);
            return Ok(agg_high_byte
                .max(self.logpxd.end_byte(self.bsize))
                .max(self.fsckpxd.end_byte(self.bsize)));
        }
        Ok(self.total_byte_extent)
    }
}

// ---- J.2: compact reader ----

/// Streaming `Read` that emits a layout-preserving compacted JFS2
/// aggregate.
///
/// Driven by the BMAP's pmap: allocated blocks come straight from the
/// source image, free blocks become zero sections. The inline log +
/// fsck workspace (which may live past the aggregate end) are emitted
/// from disk verbatim so the resulting image still survives `fsck.jfs`
/// and a journal replay.
pub struct CompactJfsReader<R: Read + Seek> {
    inner: CompactStreamReader<R>,
}

impl<R: Read + Seek + Send> CompactJfsReader<R> {
    /// Walk the BMAP, build a section list covering [0, partition_extent),
    /// and return a streaming reader. The partition extent equals
    /// `max(aggregate_end, logpxd_end, fsckpxd_end)` — the same upper
    /// bound `last_data_byte` reports.
    pub fn new(reader: R, partition_offset: u64) -> Result<(Self, CompactResult), FilesystemError> {
        let mut fs = JfsFilesystem::open(reader, partition_offset)?;
        let bsize = fs.bsize;
        let agg_byte_end = fs.size_in_pblocks * fs.pbsize;
        let total_byte_extent = fs.total_byte_extent;
        let logpxd = fs.logpxd;
        let fsckpxd = fs.fsckpxd;

        let walk = fs.require_bmap_walk()?.clone();
        let allocated_blocks = walk.allocated_blocks;

        let mut sections: Vec<CompactSection> = Vec::new();
        let mut covered: u64 = 0;

        // 1) Reserved area: bytes [0, AITBL_OFF) — superblock + AIMAP +
        //    AIT region. All metadata we must capture verbatim.
        // We cover this as a single mapped extent of aggregate blocks.
        // The reserved area is the first few aggregate blocks of the
        // disk; sit them in front of the pmap-derived runs.
        //
        // Practical effect: the BMAP pmap already marks reserved blocks
        // (0..AGGR_RSVD_BLOCKS) as allocated, so they're picked up by
        // the run loop below. No extra emit needed here.

        // 2) Aggregate body — emit one section per BMAP run.
        for run in &walk.runs {
            let start_byte = run.start_block * bsize;
            let len_bytes = run.length * bsize;
            if start_byte > covered {
                // Shouldn't happen given the run loop emits adjacent
                // runs, but be defensive against probe bugs by zero-
                // filling any unexpected hole.
                sections.push(CompactSection::Zeros(start_byte - covered));
            }
            if run.allocated {
                let n_blocks_per_section = run.length as usize;
                let mut old_blocks = Vec::with_capacity(n_blocks_per_section);
                for b in run.start_block..run.start_block + run.length {
                    old_blocks.push(b);
                }
                sections.push(CompactSection::MappedBlocks { old_blocks });
            } else {
                sections.push(CompactSection::Zeros(len_bytes));
            }
            covered = start_byte + len_bytes;
        }

        // 3) Trailing aggregate area (between mapsize*bsize and
        //    agg_byte_end). The aggregate-end may sit beyond mapsize *
        //    bsize because s_size is in pblocks: agg_byte_end =
        //    size_in_pblocks * pbsize. Cover the gap with zeros.
        if covered < agg_byte_end {
            sections.push(CompactSection::Zeros(agg_byte_end - covered));
            covered = agg_byte_end;
        }

        // 4) Post-aggregate area: fsckpxd + logpxd. Emit verbatim. They
        //    may overlap or be out-of-order on disk; sort and merge.
        let mut tail_runs: Vec<(u64, u64)> = Vec::new();
        if fsckpxd.length > 0 {
            tail_runs.push((fsckpxd.address * bsize, fsckpxd.end_byte(bsize)));
        }
        if logpxd.length > 0 {
            tail_runs.push((logpxd.address * bsize, logpxd.end_byte(bsize)));
        }
        tail_runs.sort_by_key(|&(s, _)| s);

        for (start, end) in tail_runs {
            if start < covered {
                // Already inside the aggregate body. Skip the overlap.
                if end <= covered {
                    continue;
                }
                let trimmed_start = covered;
                let blocks_start = trimmed_start.div_ceil(bsize);
                let blocks_end = end / bsize;
                if blocks_end > blocks_start {
                    let old_blocks: Vec<u64> = (blocks_start..blocks_end).collect();
                    sections.push(CompactSection::MappedBlocks { old_blocks });
                    covered = blocks_end * bsize;
                }
                continue;
            }
            if start > covered {
                sections.push(CompactSection::Zeros(start - covered));
                covered = start;
            }
            let blocks_start = start / bsize;
            let blocks_end = end / bsize;
            if blocks_end > blocks_start {
                let old_blocks: Vec<u64> = (blocks_start..blocks_end).collect();
                sections.push(CompactSection::MappedBlocks { old_blocks });
                covered = end;
            }
        }

        // 5) Final padding so the stream length equals total_byte_extent.
        if covered < total_byte_extent {
            sections.push(CompactSection::Zeros(total_byte_extent - covered));
        }

        let layout = CompactLayout {
            sections,
            block_size: bsize as usize,
            source_data_start: 0,
            source_partition_offset: partition_offset,
        };
        let inner = CompactStreamReader::new(fs.reader, layout);
        let info = CompactResult {
            original_size: total_byte_extent,
            compacted_size: total_byte_extent, // layout-preserving
            data_size: allocated_blocks * bsize,
            clusters_used: allocated_blocks.min(u32::MAX as u64) as u32,
        };
        Ok((Self { inner }, info))
    }
}

impl<R: Read + Seek> Read for CompactJfsReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

// ---- Tests ----

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::entry::EntryType;
    use std::io::Cursor;

    // ---- Synthetic superblock construction ----

    /// Build an in-memory image whose SB at byte 32768 carries the
    /// supplied field values. Every field not enumerated here is
    /// zeroed.
    #[allow(clippy::too_many_arguments)]
    fn build_sb(
        version: u32,
        size_in_pblocks: i64,
        bsize: i32,
        l2bsize: i16,
        pbsize: i32,
        agsize: u32,
        state: u32,
        flag: u32,
        logpxd: Option<(u32, u64)>,  // (length, address) in agg blocks
        fsckpxd: Option<(u32, u64)>, // (length, address) in agg blocks
        label: &[u8],
    ) -> Vec<u8> {
        let total_bytes = (SB_OFFSET as usize) + SB_READ_SIZE;
        let mut img = vec![0u8; total_bytes.max(2 * 1024 * 1024)];
        let sb = &mut img[SB_OFFSET as usize..];
        sb[OFF_MAGIC..OFF_MAGIC + 4].copy_from_slice(JFS_MAGIC);
        sb[OFF_VERSION..OFF_VERSION + 4].copy_from_slice(&version.to_le_bytes());
        sb[OFF_SIZE..OFF_SIZE + 8].copy_from_slice(&size_in_pblocks.to_le_bytes());
        sb[OFF_BSIZE..OFF_BSIZE + 4].copy_from_slice(&bsize.to_le_bytes());
        sb[OFF_L2BSIZE..OFF_L2BSIZE + 2].copy_from_slice(&l2bsize.to_le_bytes());
        sb[OFF_PBSIZE..OFF_PBSIZE + 4].copy_from_slice(&pbsize.to_le_bytes());
        sb[OFF_AGSIZE..OFF_AGSIZE + 4].copy_from_slice(&agsize.to_le_bytes());
        sb[OFF_FLAG..OFF_FLAG + 4].copy_from_slice(&flag.to_le_bytes());
        sb[OFF_STATE..OFF_STATE + 4].copy_from_slice(&state.to_le_bytes());
        if let Some((len, addr)) = logpxd {
            write_pxd(&mut sb[OFF_LOGPXD..OFF_LOGPXD + PXD_LEN], len, addr);
        }
        if let Some((len, addr)) = fsckpxd {
            write_pxd(&mut sb[OFF_FSCKPXD..OFF_FSCKPXD + PXD_LEN], len, addr);
        }
        let take = label.len().min(LABEL_LEN);
        sb[OFF_LABEL..OFF_LABEL + take].copy_from_slice(&label[..take]);
        img
    }

    fn write_pxd(buf: &mut [u8], length: u32, address: u64) {
        // length (24 bits) + addr_hi (8 bits) in word 0.
        let addr_hi = ((address >> 32) & 0xFF) as u32;
        let word0 = (length & 0x00FF_FFFF) | (addr_hi << 24);
        let word1 = (address & 0xFFFF_FFFF) as u32;
        buf[0..4].copy_from_slice(&word0.to_le_bytes());
        buf[4..8].copy_from_slice(&word1.to_le_bytes());
    }

    fn default_sb() -> Vec<u8> {
        // 4 MiB image, 4096 bsize, 512 pbsize, clean, no label.
        build_sb(
            1,
            8192, // 8192 * 512 = 4 MiB
            4096,
            12,
            512,
            1024,
            0,
            0,
            Some((64, 7040)), // inline log: 64 agg blocks at agg block 7040
            Some((20, 7020)), // fsck workspace: 20 agg blocks at agg block 7020
            b"",
        )
    }

    // ---- Pxd parsing ----

    #[test]
    fn pxd_parses_length_and_split_address() {
        // length = 0x000F00 = 3840, addr_hi = 0x01, addr_lo = 0xDEAD_BEEF
        // → expected address = (0x01 << 32) | 0xDEAD_BEEF = 0x1_DEAD_BEEF
        let mut buf = [0u8; 8];
        write_pxd(&mut buf, 3840, 0x0001_DEAD_BEEF);
        let pxd = Pxd::parse(&buf);
        assert_eq!(pxd.length, 3840);
        assert_eq!(pxd.address, 0x0001_DEAD_BEEF);
    }

    #[test]
    fn pxd_end_byte_uses_aggregate_blocksize() {
        let pxd = Pxd {
            length: 256,
            address: 3840,
        };
        assert_eq!(pxd.end_byte(4096), (3840 + 256) * 4096);
    }

    // ---- Open-time detection ----

    #[test]
    fn open_accepts_default_synthetic_sb() {
        let img = default_sb();
        let fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        assert_eq!(fs.fs_type(), "JFS2");
        assert_eq!(fs.version(), 1);
        assert_eq!(fs.aggregate_block_size(), 4096);
        assert_eq!(fs.hardware_block_size(), 512);
        assert_eq!(fs.total_size(), 4 * 1024 * 1024);
    }

    #[test]
    fn open_accepts_version_2() {
        // The kernel constant is 2; we accept it for forward compatibility.
        let img = build_sb(2, 8192, 4096, 12, 512, 1024, 0, 0, None, None, b"");
        let fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        assert_eq!(fs.version(), 2);
    }

    #[test]
    fn open_rejects_bad_magic() {
        let mut img = default_sb();
        img[SB_OFFSET as usize..SB_OFFSET as usize + 4].copy_from_slice(b"XYZ1");
        let err = JfsFilesystem::open(Cursor::new(img), 0)
            .err()
            .expect("expected reject");
        match err {
            FilesystemError::Parse(msg) => assert!(msg.contains("bad magic"), "got: {msg}"),
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn open_rejects_future_version() {
        let img = build_sb(3, 8192, 4096, 12, 512, 1024, 0, 0, None, None, b"");
        let err = JfsFilesystem::open(Cursor::new(img), 0)
            .err()
            .expect("expected reject");
        match err {
            FilesystemError::Unsupported(msg) => {
                assert!(msg.contains("version 3"), "got: {msg}")
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn open_rejects_version_zero() {
        let img = build_sb(0, 8192, 4096, 12, 512, 1024, 0, 0, None, None, b"");
        let err = JfsFilesystem::open(Cursor::new(img), 0)
            .err()
            .expect("expected reject");
        assert!(matches!(err, FilesystemError::Unsupported(_)));
    }

    #[test]
    fn open_rejects_dirty_aggregate() {
        // s_state != 0 → not cleanly unmounted.
        let img = build_sb(
            1, 8192, 4096, 12, 512, 1024, 1, // state = dirty
            0, None, None, b"",
        );
        let err = JfsFilesystem::open(Cursor::new(img), 0)
            .err()
            .expect("expected reject");
        match err {
            FilesystemError::Unsupported(msg) => {
                assert!(msg.contains("not cleanly unmounted"), "got: {msg}")
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn open_rejects_bad_block_size() {
        // bsize = 256 (below MIN_BSIZE = 512).
        let img = build_sb(1, 8192, 256, 8, 256, 1024, 0, 0, None, None, b"");
        let err = JfsFilesystem::open(Cursor::new(img), 0)
            .err()
            .expect("expected reject");
        match err {
            FilesystemError::Parse(msg) => assert!(msg.contains("block size"), "got: {msg}"),
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn open_rejects_l2bsize_mismatch() {
        // bsize = 4096 (2^12) but l2bsize = 10 (2^10 = 1024) → reject.
        let img = build_sb(1, 8192, 4096, 10, 512, 1024, 0, 0, None, None, b"");
        let err = JfsFilesystem::open(Cursor::new(img), 0)
            .err()
            .expect("expected reject");
        match err {
            FilesystemError::Parse(msg) => {
                assert!(msg.contains("l2bsize"), "got: {msg}")
            }
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn open_rejects_pbsize_larger_than_bsize() {
        // pbsize = 8192 > bsize = 4096 → reject.
        let img = build_sb(1, 8192, 4096, 12, 8192, 1024, 0, 0, None, None, b"");
        let err = JfsFilesystem::open(Cursor::new(img), 0)
            .err()
            .expect("expected reject");
        assert!(matches!(err, FilesystemError::Parse(_)));
    }

    #[test]
    fn open_rejects_zero_size() {
        let img = build_sb(1, 0, 4096, 12, 512, 1024, 0, 0, None, None, b"");
        let err = JfsFilesystem::open(Cursor::new(img), 0)
            .err()
            .expect("expected reject");
        match err {
            FilesystemError::Parse(msg) => assert!(msg.contains("s_size"), "got: {msg}"),
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn open_parses_label() {
        let img = build_sb(
            1,
            8192,
            4096,
            12,
            512,
            1024,
            0,
            0,
            None,
            None,
            b"jfs_test\0\0\0\0\0\0\0\0",
        );
        let fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        assert_eq!(fs.volume_label(), Some("jfs_test"));
    }

    #[test]
    fn label_is_none_when_field_is_empty() {
        let img = default_sb();
        let fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        assert_eq!(fs.volume_label(), None);
    }

    #[test]
    fn last_data_byte_includes_logpxd_past_aggregate() {
        // Aggregate ends at 8192 pblocks * 512 = 4 MiB = 4194304 bytes.
        // Log lives at agg block 7040 (= 28835840 bytes) for 64 blocks
        // → ends at byte (7040 + 64) * 4096 = 7104 * 4096 = 29,097,984.
        // 29 MiB > 4 MiB aggregate, so the log dominates.
        let img = build_sb(
            1,
            8192,
            4096,
            12,
            512,
            1024,
            0,
            0,
            Some((64, 7040)),
            Some((20, 7020)),
            b"",
        );
        let mut fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        assert_eq!(fs.last_data_byte().unwrap(), (7040 + 64) * 4096);
    }

    #[test]
    fn last_data_byte_uses_aggregate_when_log_is_inside() {
        // Move log + fsck workspace fully inside the aggregate.
        let img = build_sb(
            1,
            8192,
            4096,
            12,
            512,
            1024,
            0,
            0,
            Some((64, 100)),
            Some((20, 200)),
            b"",
        );
        let mut fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        // 8192 * 512 = 4 MiB.
        assert_eq!(fs.last_data_byte().unwrap(), 4 * 1024 * 1024);
    }

    #[test]
    fn partition_offset_is_threaded_through_seek() {
        let raw = default_sb();
        let prefix = vec![0xCDu8; 4 * 1024 * 1024]; // 4 MiB of junk
        let mut wrapped = prefix.clone();
        wrapped.extend_from_slice(&raw);
        let fs = JfsFilesystem::open(Cursor::new(wrapped), prefix.len() as u64).expect("open");
        assert_eq!(fs.fs_type(), "JFS2");
        assert_eq!(fs.total_size(), 4 * 1024 * 1024);
    }

    // ---- Fixture-driven tests ----

    fn load_fixture(name: &str) -> Vec<u8> {
        let path = format!("tests/fixtures/{name}");
        let compressed =
            std::fs::read(&path).unwrap_or_else(|e| panic!("Failed to read fixture {path}: {e}"));
        let mut decoder = zstd::stream::read::Decoder::new(Cursor::new(compressed))
            .unwrap_or_else(|e| panic!("Failed to create zstd decoder for {path}: {e}"));
        let mut output = Vec::new();
        std::io::Read::read_to_end(&mut decoder, &mut output)
            .unwrap_or_else(|e| panic!("Failed to decompress {path}: {e}"));
        output
    }

    #[test]
    fn fixture_jfs_opens_with_expected_geometry() {
        // From `scripts/generate-jfs-fixtures.sh` + `mkfs.jfs -L jfs_test`.
        // probe-jfs-sb.py confirms: version=1, s_size=30304 pblocks,
        // s_bsize=4096, s_pbsize=512, agsize=8192, state=0, label="jfs_test".
        let img = load_fixture("test_jfs.img.zst");
        let fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        assert_eq!(fs.fs_type(), "JFS2");
        assert_eq!(fs.version(), 1);
        assert_eq!(fs.aggregate_block_size(), 4096);
        assert_eq!(fs.hardware_block_size(), 512);
        assert_eq!(fs.alloc_group_size(), 8192);
        // 30304 * 512 = 15515648 bytes = ~14.8 MiB aggregate.
        assert_eq!(fs.total_size(), 30304 * 512);
        assert_eq!(fs.volume_label(), Some("jfs_test"));
    }

    #[test]
    fn fixture_jfs_logpxd_points_at_image_tail() {
        // The mkfs.jfs default lays the inline log at the very end of
        // the partition. For our 16 MiB image we expect logpxd.address
        // = 3840 agg blocks (= byte 15728640) with length 256
        // (= 1 MiB), and the log byte-end == 16 MiB.
        let img = load_fixture("test_jfs.img.zst");
        let fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let log = fs.logpxd();
        assert_eq!(log.address, 3840);
        assert_eq!(log.length, 256);
        assert_eq!(log.end_byte(4096), 16 * 1024 * 1024);
    }

    #[test]
    fn fixture_jfs_fsckpxd_sits_between_aggregate_and_log() {
        // fsckpxd at agg block 3788 (= 15511552 bytes — exactly the
        // aggregate end) with length 52 blocks (= 212992 bytes).
        let img = load_fixture("test_jfs.img.zst");
        let fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let fsck = fs.fsckpxd();
        assert_eq!(fsck.address, 3788);
        assert_eq!(fsck.length, 52);
        // fsckpxd ends exactly where logpxd starts.
        assert_eq!(fsck.end_byte(4096), fs.logpxd().address * 4096);
    }

    #[test]
    fn fixture_jfs_last_data_byte_captures_inline_log() {
        // The naive truncation to `total_size` would lose the inline
        // log; `last_data_byte` must include it. For our fixture, the
        // log ends exactly at 16 MiB (the partition end).
        let img = load_fixture("test_jfs.img.zst");
        let mut fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let last = fs.last_data_byte().unwrap();
        // J.2 now BMAP-walks lazily; on our fixture the aggregate's
        // internal highest-allocated block is small (within a few
        // hundred KiB), but logpxd_end sits at exactly 16 MiB and
        // dominates the max(). So the post-J.2 result still hits 16 MiB.
        assert_eq!(last, 16 * 1024 * 1024);
        assert!(
            last > fs.total_size(),
            "last_data_byte ({last}) should exceed total_size ({}) on a volume \
             whose inline log lives past the aggregate end",
            fs.total_size()
        );
    }

    // ---- J.2 — dinode + xtree + BMAP walk ----

    #[test]
    fn ait_inode_byte_offset_matches_kernel_formula() {
        // AITBL_OFF = 0xB000; inum 1 (AGGREGATE_I) at 0xB200; inum 2
        // (BMAP_I) at 0xB400; inum 16 (FILESYSTEM_I) at 0xD000 — all
        // confirmed via scripts/probe-jfs-ait.py against the fixture.
        let img = load_fixture("test_jfs.img.zst");
        let fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        assert_eq!(fs.ait_inode_byte_offset(0), 0xB000);
        assert_eq!(fs.ait_inode_byte_offset(1), 0xB200);
        assert_eq!(fs.ait_inode_byte_offset(BMAP_I), 0xB400);
        assert_eq!(fs.ait_inode_byte_offset(FILESYSTEM_I), 0xD000);
    }

    #[test]
    fn ait_inode_above_31_is_rejected_for_now() {
        let img = load_fixture("test_jfs.img.zst");
        let mut fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let err = fs.read_ait_inode(32).expect_err("expected reject");
        match err {
            FilesystemError::Unsupported(msg) => assert!(msg.contains("multi-extent AIT")),
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn fixture_reads_bmap_control_inode() {
        let img = load_fixture("test_jfs.img.zst");
        let mut fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let bmap = fs.read_ait_inode(BMAP_I).expect("read BMAP inode");
        // From probe: di_number=2, fileset=1, nblocks=6, size=24576,
        // mode=0o00300000 (IFJOURNAL + S_IFREG).
        assert_eq!(bmap.di_number, BMAP_I);
        assert_eq!(bmap.fileset, 1);
        assert_eq!(bmap.nblocks, 6);
        assert_eq!(bmap.size, 24576);
        // POSIX type bits are S_IFREG (regular file).
        assert_eq!(bmap.posix_type(), S_IFREG);
        assert!(bmap.is_regular());
    }

    #[test]
    fn fixture_reads_filesystem_root_inode() {
        // FILESYSTEM_I (inum 16) describes the user fileset. Even
        // though J.3 isn't implemented yet, the AIT bootstrap should
        // surface its metadata correctly.
        let img = load_fixture("test_jfs.img.zst");
        let mut fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let fsino = fs.read_ait_inode(FILESYSTEM_I).expect("read FILESYSTEM_I");
        assert_eq!(fsino.di_number, FILESYSTEM_I);
        assert_eq!(fsino.nblocks, 2);
        assert_eq!(fsino.size, 8192);
    }

    #[test]
    fn fixture_xtree_root_decodes_bmap_extent() {
        let img = load_fixture("test_jfs.img.zst");
        let mut fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let bmap_ino = fs.read_ait_inode(BMAP_I).expect("read BMAP inode");
        let xtroot = fs
            .parse_inline_xtree_root(&bmap_ino.raw)
            .expect("parse xtree root");
        // From probe: nextindex=3 (header + 1 XAD), xad[0]=(len=6,
        // addr=16, logical_offset=0).
        assert_eq!(xtroot.xads.len(), 1);
        let xad = xtroot.xads[0];
        assert_eq!(xad.logical_offset, 0);
        assert_eq!(xad.physical_address, 16);
        assert_eq!(xad.physical_length, 6);
    }

    #[test]
    fn fixture_xad_offset_split_address() {
        // Synthetic XAD: flag=XAD_NEW, logical_offset = 0x1234_5678_9A,
        // length=10, address=0x42.
        let mut buf = [0u8; XAD_SIZE];
        buf[XAD_OFF_FLAG] = 0x01;
        buf[XAD_OFF_OFF1] = 0x12; // high 8 bits of logical_offset
        buf[XAD_OFF_OFF2..XAD_OFF_OFF2 + 4].copy_from_slice(&0x3456_789Au32.to_le_bytes());
        // pxd_t loc: length=10 in low 24 bits of word0, addr_hi=0, addr_lo=0x42.
        let word0 = 10u32 & 0x00FF_FFFF;
        let word1 = 0x42u32;
        buf[XAD_OFF_LOC..XAD_OFF_LOC + 4].copy_from_slice(&word0.to_le_bytes());
        buf[XAD_OFF_LOC + 4..XAD_OFF_LOC + 8].copy_from_slice(&word1.to_le_bytes());
        let xad = Xad::parse(&buf);
        assert_eq!(xad.flag, 0x01);
        assert_eq!(xad.logical_offset, 0x0012_3456_789A);
        assert_eq!(xad.physical_length, 10);
        assert_eq!(xad.physical_address, 0x42);
    }

    #[test]
    fn fixture_bmap_walk_matches_probe() {
        // From probe-jfs-bmap.py: mapsize=3788, nfree=3741, so
        // allocated_blocks should be 47. Highest pmap-allocated block
        // = 46 (= 47 - 1).
        let img = load_fixture("test_jfs.img.zst");
        let mut fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let walk = fs.require_bmap_walk().expect("walk BMAP").clone();
        assert_eq!(walk.mapsize, 3788);
        assert_eq!(walk.nfree, 3741);
        assert_eq!(walk.allocated_blocks, 47);
        assert_eq!(walk.highest_alloc, Some(46));
        // First run is allocated (blocks 0..47), then a single
        // free run to mapsize.
        assert_eq!(walk.runs.len(), 2);
        assert!(walk.runs[0].allocated);
        assert_eq!(walk.runs[0].start_block, 0);
        assert_eq!(walk.runs[0].length, 47);
        assert!(!walk.runs[1].allocated);
        assert_eq!(walk.runs[1].length, 3788 - 47);
    }

    #[test]
    fn fixture_used_size_after_bmap_walk_is_real_count() {
        let img = load_fixture("test_jfs.img.zst");
        let mut fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        // Trigger walk via last_data_byte.
        let _ = fs.last_data_byte().expect("last_data_byte");
        // 47 blocks × 4096 = 192512 bytes
        assert_eq!(fs.used_size(), 47 * 4096);
        // total_size unchanged (FS-claimed aggregate bytes).
        assert_eq!(fs.total_size(), 30304 * 512);
    }

    #[test]
    fn fixture_last_data_byte_still_includes_log_after_bmap_walk() {
        // Even though the BMAP says only ~192 KB of the aggregate is
        // allocated, last_data_byte must still capture the inline log
        // at 16 MiB (otherwise the journal would be lost on restore).
        let img = load_fixture("test_jfs.img.zst");
        let mut fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let last = fs.last_data_byte().expect("last_data_byte");
        assert_eq!(last, 16 * 1024 * 1024);
    }

    #[test]
    fn fixture_compact_reader_stream_length_matches_total_extent() {
        // CompactJfsReader is layout-preserving: stream length ==
        // total partition extent (16 MiB), allocated blocks come
        // from disk, free blocks are zeros.
        let img = load_fixture("test_jfs.img.zst");
        let (mut reader, info) =
            CompactJfsReader::new(Cursor::new(img.clone()), 0).expect("compactor");
        // total_byte_extent = 16 MiB.
        assert_eq!(info.original_size, 16 * 1024 * 1024);
        assert_eq!(info.compacted_size, 16 * 1024 * 1024);
        // data_size = 47 * 4096 = 192512 (aggregate allocated bytes)
        // — NOT counting the inline log + fsckpxd which are emitted
        // verbatim but not tracked as "user data."
        assert_eq!(info.data_size, 47 * 4096);

        let mut sink = Vec::new();
        std::io::copy(&mut reader, &mut sink).expect("drain");
        assert_eq!(sink.len() as u64, info.original_size);
    }

    #[test]
    fn fixture_compact_reader_preserves_allocated_blocks() {
        // The compactor must emit the original bytes of allocated
        // aggregate blocks at their original byte offsets. Compare
        // against the raw fixture bytes for blocks 0..47.
        let img = load_fixture("test_jfs.img.zst");
        let raw = img.clone();
        let (mut reader, _info) = CompactJfsReader::new(Cursor::new(img), 0).expect("compactor");
        let mut out = Vec::new();
        std::io::copy(&mut reader, &mut out).expect("drain");
        let bsize = 4096usize;
        for blk in 0..47 {
            let start = blk * bsize;
            assert_eq!(
                &out[start..start + bsize],
                &raw[start..start + bsize],
                "allocated block {blk} should round-trip byte-for-byte"
            );
        }
        // Free blocks 47..3788 → compactor emits zeros.
        for blk in 47..3788 {
            let start = blk * bsize;
            assert!(
                out[start..start + bsize].iter().all(|&b| b == 0),
                "free block {blk} should be zero-filled by compactor"
            );
        }
        // logpxd region: blocks 3840..4096 (1 MiB). Must round-trip
        // byte-for-byte since the inline log is needed for fsck.
        for blk in 3840..4096 {
            let start = blk * bsize;
            assert_eq!(
                &out[start..start + bsize],
                &raw[start..start + bsize],
                "logpxd block {blk} should round-trip byte-for-byte"
            );
        }
    }

    #[test]
    fn fixture_compact_reader_round_trips_through_parser() {
        // The compacted stream must itself parse as a valid JFS image.
        let img = load_fixture("test_jfs.img.zst");
        let (mut reader, _info) = CompactJfsReader::new(Cursor::new(img), 0).expect("compactor");
        let mut out = Vec::new();
        std::io::copy(&mut reader, &mut out).expect("drain");
        let fs = JfsFilesystem::open(Cursor::new(out), 0).expect("re-open compacted");
        assert_eq!(fs.fs_type(), "JFS2");
        assert_eq!(fs.total_size(), 30304 * 512);
    }

    // ---- Dinode parse synthetics ----

    #[test]
    fn dinode_parse_rejects_too_small() {
        let small = vec![0u8; 256];
        let err = JfsDinode::parse(&small, 2).expect_err("expected size error");
        match err {
            FilesystemError::Parse(msg) => assert!(msg.contains("too small")),
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn dinode_parse_rejects_empty_nonzero_inum() {
        // All-zero dinode for a non-zero inum (i.e. an inode slot
        // that's been wiped or never written). Must reject — the
        // bootstrap chain would walk into nonsense otherwise.
        let empty = vec![0u8; DISIZE as usize];
        let err = JfsDinode::parse(&empty, 2).expect_err("expected reject");
        assert!(matches!(err, FilesystemError::Parse(_)));
    }

    #[test]
    fn dinode_mode_helpers() {
        let mut buf = vec![0u8; DISIZE as usize];
        // Need to set di_number to non-zero so the "all empty" check
        // doesn't fire. Then test each posix_type path.
        buf[DI_OFF_NUMBER..DI_OFF_NUMBER + 4].copy_from_slice(&7u32.to_le_bytes());

        // Regular file
        buf[DI_OFF_MODE..DI_OFF_MODE + 4].copy_from_slice(&S_IFREG.to_le_bytes());
        let dino = JfsDinode::parse(&buf, 7).unwrap();
        assert!(dino.is_regular() && !dino.is_directory() && !dino.is_symlink());

        // Directory
        buf[DI_OFF_MODE..DI_OFF_MODE + 4].copy_from_slice(&S_IFDIR.to_le_bytes());
        let dino = JfsDinode::parse(&buf, 7).unwrap();
        assert!(dino.is_directory() && !dino.is_regular() && !dino.is_symlink());

        // Symlink
        buf[DI_OFF_MODE..DI_OFF_MODE + 4].copy_from_slice(&S_IFLNK.to_le_bytes());
        let dino = JfsDinode::parse(&buf, 7).unwrap();
        assert!(dino.is_symlink() && !dino.is_regular() && !dino.is_directory());
    }

    #[test]
    fn xtree_root_rejects_nextindex_below_2() {
        let fs_img = default_sb();
        let fs = JfsFilesystem::open(Cursor::new(fs_img), 0).expect("open");
        let mut dino = vec![0u8; DISIZE as usize];
        // nextindex at xtroot offset 18 = dinode offset 224 + 18 = 242.
        dino[242..244].copy_from_slice(&1u16.to_le_bytes());
        let err = fs
            .parse_inline_xtree_root(&dino)
            .expect_err("expected reject");
        match err {
            FilesystemError::Parse(msg) => assert!(msg.contains("nextindex 1")),
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    // ---- J.3 — fileset + iag + dtree + browse + read_file ----

    fn find_entry<'a>(entries: &'a [FileEntry], name: &str) -> &'a FileEntry {
        entries
            .iter()
            .find(|e| e.name == name)
            .unwrap_or_else(|| panic!("missing entry {name:?} in listing"))
    }

    #[test]
    fn fixture_reads_fileset_iag() {
        // Per probe-jfs-fileset.py: pmap[0] = 0xFF000000 (extents 0..7
        // allocated), inoext[0] = (len=4, addr=28).
        let img = load_fixture("test_jfs.img.zst");
        let mut fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let iag = fs.read_fileset_iag().expect("read IAG");
        assert_eq!(iag.iagnum, 0);
        assert_eq!(iag.inoext[0].length, 4);
        assert_eq!(iag.inoext[0].address, 28);
        assert_eq!(iag.inoext[1].length, 4);
        assert_eq!(iag.inoext[1].address, 36);
        // pmap[0] = 0xFF000000 → top 8 bits set → extents 0..7 allocated.
        assert_eq!(iag.pmap[0], 0xFF00_0000);
    }

    #[test]
    fn fixture_reads_fileset_root_inode() {
        let img = load_fixture("test_jfs.img.zst");
        let mut fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let iag = fs.read_fileset_iag().expect("read IAG");
        let root = fs
            .read_fileset_inode(FILESET_ROOT_INO, &iag)
            .expect("read root");
        assert_eq!(root.di_number, FILESET_ROOT_INO);
        // From probe: di_size=40, di_nlink=3, mode=0o240755 (S_IFDIR + 0o755).
        assert_eq!(root.size, 40);
        assert_eq!(root.nlink, 3);
        assert_eq!(root.mode & 0xFFFF, 0o40755);
        assert!(root.is_directory());
    }

    #[test]
    fn fixture_root_listing_returns_all_user_entries() {
        let img = load_fixture("test_jfs.img.zst");
        let mut fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list root");

        let names: Vec<_> = entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(
            names.len(),
            5,
            "expected 5 entries (hello.txt, large.bin, link.txt, subdir, tiny.txt); got {names:?}"
        );
        for expected in ["hello.txt", "large.bin", "link.txt", "subdir", "tiny.txt"] {
            assert!(
                names.contains(&expected),
                "missing {expected}; got {names:?}"
            );
        }

        let hello = find_entry(&entries, "hello.txt");
        assert_eq!(hello.entry_type, EntryType::File);
        assert_eq!(hello.size, 11);
        assert_eq!(hello.path, "/hello.txt");

        let subdir = find_entry(&entries, "subdir");
        assert_eq!(subdir.entry_type, EntryType::Directory);
        assert_eq!(subdir.path, "/subdir");
    }

    #[test]
    fn fixture_inline_symlink_target_decodes() {
        let img = load_fixture("test_jfs.img.zst");
        let mut fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list root");
        let link = find_entry(&entries, "link.txt");
        assert_eq!(link.entry_type, EntryType::Symlink);
        // 9 bytes ≤ 128 → inline. Target = "hello.txt".
        assert_eq!(link.symlink_target.as_deref(), Some("hello.txt"));
        assert_eq!(link.size, 9);
    }

    #[test]
    fn fixture_reads_small_file() {
        let img = load_fixture("test_jfs.img.zst");
        let mut fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list");
        let hello = find_entry(&entries, "hello.txt");
        let bytes = fs.read_file(hello, 1_000_000).expect("read");
        assert_eq!(bytes, b"Hello, JFS!");
    }

    #[test]
    fn fixture_reads_tiny_file() {
        let img = load_fixture("test_jfs.img.zst");
        let mut fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list");
        let tiny = find_entry(&entries, "tiny.txt");
        let bytes = fs.read_file(tiny, 1_000_000).expect("read");
        assert_eq!(bytes, b"tiny bytes");
    }

    #[test]
    fn fixture_reads_24kib_file_via_xtree() {
        // large.bin = 24 KiB = 6 aggregate blocks (bsize=4096); should
        // fit in 1-2 inline xtree XADs. Deterministic content per the
        // generator: data[i] = (i*37 + 11) & 0xFF.
        let img = load_fixture("test_jfs.img.zst");
        let mut fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list");
        let big = find_entry(&entries, "large.bin");
        assert_eq!(big.size, 24 * 1024);
        let bytes = fs.read_file(big, 24 * 1024).expect("read");
        assert_eq!(bytes.len(), 24 * 1024);
        for i in [0usize, 1, 100, 4095, 4096, 8191, 8192, 12345, 16384, 24575] {
            assert_eq!(
                bytes[i],
                ((i * 37 + 11) & 0xFF) as u8,
                "large.bin[{i}] mismatch"
            );
        }
    }

    #[test]
    fn fixture_read_file_honours_max_bytes() {
        let img = load_fixture("test_jfs.img.zst");
        let mut fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list");
        let big = find_entry(&entries, "large.bin");
        let bytes = fs.read_file(big, 5000).expect("read clamped");
        assert_eq!(bytes.len(), 5000);
        // First 5 KB still matches the deterministic content.
        for i in [0usize, 100, 4095, 4999] {
            assert_eq!(bytes[i], ((i * 37 + 11) & 0xFF) as u8);
        }
    }

    #[test]
    fn fixture_descends_subdir() {
        let img = load_fixture("test_jfs.img.zst");
        let mut fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list root");
        let subdir = find_entry(&entries, "subdir");
        let nested = fs.list_directory(subdir).expect("list subdir");
        assert_eq!(nested.len(), 1);
        assert_eq!(nested[0].name, "nested.txt");
        assert_eq!(nested[0].path, "/subdir/nested.txt");
        assert_eq!(nested[0].size, 11);
        let bytes = fs.read_file(&nested[0], 1_000_000).expect("read nested");
        assert_eq!(bytes, b"nested file");
    }

    #[test]
    fn fixture_read_file_on_directory_is_parse_error() {
        let img = load_fixture("test_jfs.img.zst");
        let mut fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let err = fs.read_file(&root, 64).expect_err("expected parse error");
        assert!(matches!(err, FilesystemError::Parse(_)));
    }

    #[test]
    fn fixture_read_file_on_symlink_returns_target_bytes() {
        let img = load_fixture("test_jfs.img.zst");
        let mut fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list");
        let link = find_entry(&entries, "link.txt");
        let bytes = fs.read_file(link, 1_000_000).expect("read symlink");
        assert_eq!(bytes, b"hello.txt");
    }

    #[test]
    fn list_directory_filesystem_metadata_carries_uid_gid_mtime() {
        let img = load_fixture("test_jfs.img.zst");
        let mut fs = JfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list");
        let hello = find_entry(&entries, "hello.txt");
        // The fixture was created with default uid/gid = 0 (running as
        // root inside guestfish). mtime is the build timestamp;
        // assert it's *some* string rather than pinning a value.
        assert_eq!(hello.uid, Some(0));
        assert_eq!(hello.gid, Some(0));
        assert!(hello.modified.is_some());
        // Mode string should start with '-' (regular file).
        let ms = hello.mode_string().unwrap();
        assert!(
            ms.starts_with('-'),
            "expected mode_string to begin with '-', got {ms}"
        );
    }
}
