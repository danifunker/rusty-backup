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
        })
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
        // Tier B (J.3 — AIT + fileset 1 root inode + xtree dir walker)
        // will replace this with a real inode lookup. For Tier A we
        // surface a stub root so the inspect tab renders.
        Ok(FileEntry::root())
    }

    fn list_directory(&mut self, _entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "JFS directory browse not yet implemented (Tier B / J.3 pending)".into(),
        ))
    }

    fn read_file(
        &mut self,
        _entry: &FileEntry,
        _max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "JFS file read not yet implemented (Tier B / J.3 pending)".into(),
        ))
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
        // Tier A has no BMAP B+tree walker yet; report the full size
        // as used so the inspect tab doesn't lie about free space.
        // Tier B / J.2 will replace this with the real count.
        self.total_size()
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        // Capture the aggregate **and** the inline log + fsck
        // workspace — the log sits at the tail of most JFS volumes,
        // past the aggregate end. Truncating to `total_size` would
        // silently drop journal state.
        Ok(self.total_byte_extent)
    }
}

// ---- Tests ----

#[cfg(test)]
mod tests {
    use super::*;
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
        assert_eq!(last, 16 * 1024 * 1024);
        assert!(
            last > fs.total_size(),
            "last_data_byte ({last}) should exceed total_size ({}) on a volume \
             whose inline log lives past the aggregate end",
            fs.total_size()
        );
    }
}
