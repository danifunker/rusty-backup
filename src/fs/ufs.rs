//! UFS1 / UFS2 (Berkeley Fast Filesystem) read-only support.
//!
//! Implements the [`Filesystem`] trait far enough to detect a UFS partition,
//! surface its on-disk version, label, and total/used/free sizes in the
//! inspect tab, back the partition up byte-for-byte through the existing
//! layout-preserving pipeline (Tier A — U.1 + U.2), and walk the directory
//! tree + read regular files via the dinode → DIRENT2 → direct/indirect
//! block-pointer chain (Tier B — U.3).
//!
//! # Scope (from §1.2 of `docs/OPEN-WORK.md`)
//!
//!  * **UFS1** — original 4.2BSD FFS: SunOS 4, Solaris 2, NetBSD, OpenBSD,
//!    FreeBSD ≤ 4. Magic `0x00011954` at offset 1372 within the superblock.
//!  * **UFS2** — FreeBSD 5+ extended layout with 64-bit block addresses,
//!    256-byte inodes, and per-extent timestamps. Magic `0x19540119` at
//!    the same offset.
//!
//! Softupdate-journaled (SU+J) "dirty" volumes are refused at open with a
//! clear error rather than risk corrupting in-flight recovery state. AIX
//! JFS1 (the older sibling to JFS2) is unrelated and handled separately
//! in `src/fs/` future `jfs.rs`.
//!
//! # Superblock location
//!
//! UFS1 superblock lives at byte 8192 (the BSD `SBLOCK_UFS1`); UFS2 may
//! live at byte 65536 (FreeBSD `newfs -O 2` default, `SBLOCK_UFS2`) **or**
//! byte 8192 (small images produced by NetBSD's `makefs -O 2`, which our
//! own test fixtures use). We probe both offsets in kernel `SBLOCKSEARCH`
//! order (UFS2's modern location first, then UFS1's) and accept whichever
//! magic matches first.
//!
//! # Endianness
//!
//! UFS is **host-endian**: a volume formatted on a big-endian machine
//! (SPARC, m68k Sun, PowerPC NetBSD) stores its superblock fields BE,
//! and on a little-endian machine (amd64, ARM) LE. We capture the endian
//! by probing the magic field in both orientations at open time and
//! dispatch every multi-byte field read through it.
//!
//! # Reference
//!
//! * FreeBSD `sys/ufs/ffs/fs.h` for the authoritative on-disk struct.
//! * `partimage-0.6.9/src/client/fs/fs_ufs.cpp` for the Tier-A
//!   superblock+bitmap shape we mirror.
//! * NetBSD `usr.sbin/makefs/ffs/ffs.c` for the writer that produces our
//!   test fixtures (`tests/fixtures/test_ufs{1,2}.img.zst`).

use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};
use super::unix_common::bitmap::{
    bitmap_clear_bit, bitmap_find_clear_bit, bitmap_set_bit, BitmapReader,
};
use super::unix_common::compact::{CompactLayout, CompactSection, CompactStreamReader};
use super::unix_common::inode::{format_unix_timestamp, unix_file_type, UnixFileType};
use crate::fs::CompactResult;

// ---- Constants ----

/// Byte offsets to try when locating a UFS superblock. Matches kernel
/// `SBLOCKSEARCH` order (`{SBLOCK_UFS2, SBLOCK_UFS1, …}`). `SBLOCK_FLOPPY`
/// (0) and `SBLOCK_PIGGY` (262144) are intentionally not in the list:
/// neither shape appears on any disk image we expect to encounter.
const SB_OFFSET_UFS2: u64 = 65536;
const SB_OFFSET_UFS1: u64 = 8192;

/// UFS1 magic — `0x00011954`, encoded little-endian as `54 19 01 00`.
pub(crate) const MAGIC_UFS1: u32 = 0x0001_1954;
/// UFS2 magic — `0x19540119`, encoded little-endian as `19 01 54 19`.
pub(crate) const MAGIC_UFS2: u32 = 0x1954_0119;

/// Offset of `fs_magic` within `struct fs` (FreeBSD `sys/ufs/ffs/fs.h`).
pub(crate) const MAGIC_OFF: usize = 1372;

/// How many bytes of the superblock we read in a single call. 2 KiB covers
/// every Tier-A field including the 64-bit UFS2 size at offset 1080 and
/// the magic at offset 1372.
pub(crate) const SB_READ_SIZE: usize = 2048;

// On-disk field offsets, validated against the FreeBSD struct definition
// and cross-checked against our makefs-built fixtures (see scripts/
// generate-ufs-fixtures.sh + scripts/probe-ufs-sb.py).
pub(crate) const OFF_SBLKNO: usize = 0x008; // fs_sblkno      i32 — SB address in frags
pub(crate) const OFF_CBLKNO: usize = 0x00C; // fs_cblkno      i32 — CG block addr in frags
pub(crate) const OFF_IBLKNO: usize = 0x010; // fs_iblkno      i32 — inode-block addr in frags
pub(crate) const OFF_OLD_SIZE: usize = 0x024; // fs_old_size    i32 — UFS1 total fragments
pub(crate) const OFF_NCG: usize = 0x02C; // fs_ncg         u32 — # cylinder groups
pub(crate) const OFF_BSIZE: usize = 0x030; // fs_bsize       i32 — block size in bytes
pub(crate) const OFF_FSIZE: usize = 0x034; // fs_fsize       i32 — fragment size in bytes
pub(crate) const OFF_FRAG: usize = 0x038; // fs_frag        i32 — frags per block
pub(crate) const OFF_IPG: usize = 0x0B8; // fs_ipg         u32 — inodes per CG
pub(crate) const OFF_FPG: usize = 0x0BC; // fs_fpg         i32 — fragments per CG
const OFF_VOLNAME: usize = 680; // fs_volname[32] — `u_char[MAXVOLLEN]`
pub(crate) const OFF_SIZE_UFS2: usize = 1080; // fs_size        i64 — UFS2 64-bit fragments
const OFF_MAXSYMLINKLEN: usize = 0x528; // fs_maxsymlinklen i32 — inline-symlink cutoff (60 for UFS1, 120 for UFS2)
const OFF_FLAGS2: usize = 0x35E; // fs_flags2      i32 (relevant journal bits live elsewhere too, but this is the canonical "ENABLED" word in modern UFS2)

const VOLNAME_LEN: usize = 32;

// ---- Dinode constants (`struct ufs1_dinode` / `struct ufs2_dinode`) ----

/// Reserved as "not an inode". Inode 1 is the historical bad-blocks file
/// (unused since BSD 4.2); inode 2 is the root directory.
pub(crate) const ROOT_INODE: u32 = 2;

const UFS_NDADDR: usize = 12; // direct disk-block pointers per inode
const UFS_NIADDR: usize = 3; // indirect disk-block pointers per inode

/// UFS1 on-disk dinode is exactly 128 bytes.
const DINODE1_SIZE: u64 = 128;
/// UFS2 on-disk dinode is exactly 256 bytes.
const DINODE2_SIZE: u64 = 256;

// UFS1 dinode field offsets (struct ufs1_dinode in FreeBSD `sys/ufs/ufs/dinode.h`).
const D1_OFF_MODE: usize = 0; // di_mode    u16
const D1_OFF_NLINK: usize = 2; // di_nlink   i16
const D1_OFF_SIZE: usize = 8; // di_size    u64
const D1_OFF_MTIME: usize = 24; // di_mtime   i32 (epoch seconds)
const D1_OFF_DB: usize = 40; // di_db[12]  i32
const D1_OFF_IB: usize = 88; // di_ib[3]   i32
const D1_OFF_UID: usize = 112; // di_uid     u32
const D1_OFF_GID: usize = 116; // di_gid     u32

// UFS2 dinode field offsets (struct ufs2_dinode). di_mode + di_nlink
// share offsets with UFS1 (offsets 0 + 2); only the version-specific
// constants below are unique to UFS2.
const D2_OFF_UID: usize = 4; // di_uid     u32
const D2_OFF_GID: usize = 8; // di_gid     u32
const D2_OFF_SIZE: usize = 16; // di_size    u64
const D2_OFF_MTIME: usize = 40; // di_mtime   i64
const D2_OFF_DB: usize = 112; // di_db[12]  i64
const D2_OFF_IB: usize = 208; // di_ib[3]   i64

/// Maximum directory size we'll consume in a single `list_directory`
/// call. Real-world directories rarely exceed a few KiB; this cap is the
/// safety net against an inode whose `di_size` was corrupted to claim
/// the whole disk.
const MAX_DIR_BYTES: usize = 16 * 1024 * 1024;

// DIRENT2 (`struct direct` in `sys/ufs/ufs/dir.h`) fixed header is 8 bytes:
//   d_ino      u32  (offset 0)
//   d_reclen   u16  (offset 4)
//   d_type     u8   (offset 6)
//   d_namlen   u8   (offset 7)
// Followed by `d_namlen` name bytes + NUL pad to a 4-byte boundary.
pub(crate) const DIRENT_HDR_LEN: usize = 8;

// ---- Cylinder-group on-disk header (`struct cg` in fs.h) ----

/// Magic number at offset 4 of every cylinder-group header. Matches
/// `CG_MAGIC` from FreeBSD `sys/ufs/ffs/fs.h`.
pub(crate) const CG_MAGIC: u32 = 0x0009_0255;
pub(crate) const CG_OFF_MAGIC: usize = 0x004;
pub(crate) const CG_OFF_CGX: usize = 0x00C; // cg_cgx          u32 — CG index
pub(crate) const CG_OFF_NDBLK: usize = 0x014; // cg_ndblk        u32 — # data frags in CG
pub(crate) const CG_OFF_IUSEDOFF: usize = 0x05C; // cg_iusedoff     u32 — inode-used bitmap offset
pub(crate) const CG_OFF_FREEOFF: usize = 0x060; // cg_freeoff      u32 — free-frag bitmap offset
pub(crate) const CG_OFF_NEXTFREEOFF: usize = 0x064; // cg_nextfreeoff  u32 — used to bound the bitmap

/// How many bytes of the CG header we read in a single sweep. The fixed
/// portion ends around byte 168, and the trailing free-frag bitmap can
/// be far larger (1 bit per fragment in the CG). We read the fixed
/// portion to learn `cg_freeoff` + `cg_ndblk`, then issue a second
/// targeted read for the bitmap itself.
const CG_HEADER_FIXED_BYTES: usize = 256;

/// Block-size sanity limits. Matches FreeBSD `MINBSIZE = 512` / `MAXBSIZE
/// = 65536`. Any value outside this range is a corrupt or non-UFS image.
const MIN_BSIZE: u64 = 512;
const MAX_BSIZE: u64 = 65536;

// ---- Version & endian ----

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UfsVersion {
    Ufs1,
    Ufs2,
}

impl UfsVersion {
    fn name(self) -> &'static str {
        match self {
            UfsVersion::Ufs1 => "UFS1",
            UfsVersion::Ufs2 => "UFS2",
        }
    }
}

/// On-disk byte order of the UFS superblock + every multi-byte field
/// inside it. Captured at open time by probing the magic in both
/// orientations and dispatched through all subsequent reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UfsEndian {
    Little,
    Big,
}

// ---- Dinode ----

/// Version-agnostic in-memory dinode. Both UFS1 and UFS2 decode into this
/// shape — pointer widths differ on disk (32-bit vs 64-bit) but every
/// pointer is sign-extended up to `u64` so the walk code is shared.
#[derive(Debug, Clone)]
pub struct UfsInode {
    pub inum: u32,
    pub mode: u32,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub mtime: i64,
    /// 12 direct disk-block pointers. Each is the starting *fragment*
    /// number of a block (block bytes = `bsize`); a value of 0 means
    /// "sparse hole — emit zeros."
    pub direct: [u64; UFS_NDADDR],
    /// 3 indirect disk-block pointers (single / double / triple). Same
    /// 0 = sparse convention.
    pub indirect: [u64; UFS_NIADDR],
    /// Inline payload area starting at `di_db`. Holds 60 bytes on UFS1
    /// and 120 bytes on UFS2 — exactly the slice the kernel uses for
    /// fast symlinks (`di_size <= maxsymlinklen`).
    pub inline_payload: Vec<u8>,
}

// ---- Filesystem ----

// `frag` is captured for parity with the FS struct but unused at runtime;
// all other fields see use across U.1/U.2/U.3.
#[allow(dead_code)]
pub struct UfsFilesystem<R> {
    pub(crate) reader: R,
    pub(crate) partition_offset: u64,
    /// Byte offset of the superblock relative to the partition start.
    /// Either `SB_OFFSET_UFS1` (8192) or `SB_OFFSET_UFS2` (65536).
    pub(crate) sb_offset: u64,
    pub(crate) version: UfsVersion,
    pub(crate) endian: UfsEndian,
    pub(crate) bsize: u64, // block size in bytes
    pub(crate) fsize: u64, // fragment size in bytes
    pub(crate) frag: u32,  // fragments per block
    pub(crate) ncg: u32,   // number of cylinder groups
    pub(crate) fpg: u32,   // fragments per cylinder group
    pub(crate) ipg: u32,   // inodes per cylinder group
    /// `fs_cblkno`: fragment offset of the cylinder-group header inside
    /// each CG region. CG `i`'s header lives at fragment `i * fpg + cblkno`.
    pub(crate) cblkno: u32,
    /// `fs_iblkno`: fragment offset of the inode-table region inside each
    /// CG. Inode `inum`'s dinode lives at `(cg * fpg + iblkno) * fsize +
    /// in_cg * dinode_size` for CG `cg = inum / ipg`.
    pub(crate) iblkno: u32,
    /// `fs_sblkno`: fragment offset of the SB replica region inside each
    /// CG. CG `i`'s replica SB lives at `(cg * fpg + sblkno) * fsize`. Used
    /// by `ufs_fsck` to check that every replica matches the primary.
    pub(crate) sblkno: u32,
    pub(crate) total_frags: u64,
    /// `fs_maxsymlinklen`: byte cutoff for inline (fast) symlinks. A
    /// symlink whose `di_size <= maxsymlinklen` stores its target
    /// directly in the dinode's pointer area starting at `di_db`; longer
    /// targets live in the first data block. 60 on UFS1, 120 on UFS2 by
    /// convention; 0 (legacy 4.2BSD) means "no inline symlinks ever."
    pub(crate) maxsymlinklen: u32,
    label: Option<String>,
}

impl<R: Read + Seek + Send> UfsFilesystem<R> {
    /// Open a UFS1 or UFS2 filesystem at the given partition offset.
    /// Probes both candidate superblock locations (UFS2's modern 65536
    /// first, then UFS1's 8192) and accepts whichever magic matches.
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        let candidates = [SB_OFFSET_UFS2, SB_OFFSET_UFS1];

        // Probe the magic at each candidate offset before reading the full
        // SB; the kernel does the same dance via SBLOCKSEARCH. We try LE
        // first since modern UFS volumes are overwhelmingly little-endian;
        // BE is a fallback for legacy SPARC / m68k / PowerPC images.
        let mut found: Option<(u64, UfsVersion, UfsEndian)> = None;
        let mut magic_buf = [0u8; 4];
        for &cand in &candidates {
            reader.seek(SeekFrom::Start(partition_offset + cand + MAGIC_OFF as u64))?;
            if reader.read_exact(&mut magic_buf).is_err() {
                continue;
            }
            let le = u32::from_le_bytes(magic_buf);
            let be = u32::from_be_bytes(magic_buf);
            if le == MAGIC_UFS1 {
                found = Some((cand, UfsVersion::Ufs1, UfsEndian::Little));
                break;
            } else if le == MAGIC_UFS2 {
                found = Some((cand, UfsVersion::Ufs2, UfsEndian::Little));
                break;
            } else if be == MAGIC_UFS1 {
                found = Some((cand, UfsVersion::Ufs1, UfsEndian::Big));
                break;
            } else if be == MAGIC_UFS2 {
                found = Some((cand, UfsVersion::Ufs2, UfsEndian::Big));
                break;
            }
        }
        let (sb_offset, version, endian) = found.ok_or_else(|| {
            FilesystemError::Parse(
                "ufs: no superblock magic found at byte 8192 or 65536 (UFS1/UFS2 + LE/BE all checked)"
                    .into(),
            )
        })?;

        // Read the full SB now that we know where it lives.
        reader.seek(SeekFrom::Start(partition_offset + sb_offset))?;
        let mut sb = vec![0u8; SB_READ_SIZE];
        reader.read_exact(&mut sb)?;

        // Re-confirm the magic — this catches readers that returned stale
        // bytes between the probe and the full read (e.g. a `try_clone`'d
        // fd whose seek offset got clobbered by a concurrent worker).
        let confirmed = read_u32(&sb, MAGIC_OFF, endian);
        let expected = match version {
            UfsVersion::Ufs1 => MAGIC_UFS1,
            UfsVersion::Ufs2 => MAGIC_UFS2,
        };
        if confirmed != expected {
            return Err(FilesystemError::Parse(format!(
                "ufs: magic mismatch on re-read (expected 0x{expected:08X}, got 0x{confirmed:08X})"
            )));
        }

        // Geometry.
        let bsize = read_i32(&sb, OFF_BSIZE, endian) as u64;
        let fsize = read_i32(&sb, OFF_FSIZE, endian) as u64;
        let frag = read_i32(&sb, OFF_FRAG, endian);
        let ncg = read_u32(&sb, OFF_NCG, endian);
        let fpg = read_i32(&sb, OFF_FPG, endian);
        let ipg = read_u32(&sb, OFF_IPG, endian);
        let cblkno = read_i32(&sb, OFF_CBLKNO, endian);
        let iblkno = read_i32(&sb, OFF_IBLKNO, endian);
        let sblkno = read_i32(&sb, OFF_SBLKNO, endian);
        // `fs_maxsymlinklen` may be negative on corrupt images; clamp to
        // zero and a generous upper bound (the dinode pointer area). The
        // upper clamp matches the kernel — a value larger than the inline
        // payload area would let the kernel scribble past `di_db`.
        let raw_msl = read_i32(&sb, OFF_MAXSYMLINKLEN, endian);
        let inline_cap = match version {
            UfsVersion::Ufs1 => 60u32,  // (12+3) * 4
            UfsVersion::Ufs2 => 120u32, // (12+3) * 8
        };
        let maxsymlinklen = if raw_msl < 0 {
            0
        } else {
            (raw_msl as u32).min(inline_cap)
        };

        // Sanity gates. A corrupt or non-UFS image trips here rather than
        // returning bogus sizes downstream.
        if !(MIN_BSIZE..=MAX_BSIZE).contains(&bsize) || !bsize.is_power_of_two() {
            return Err(FilesystemError::Parse(format!(
                "ufs: implausible block size {bsize} (expected power of two in [{MIN_BSIZE}, {MAX_BSIZE}])"
            )));
        }
        if fsize == 0 || !fsize.is_power_of_two() || fsize > bsize {
            return Err(FilesystemError::Parse(format!(
                "ufs: implausible fragment size {fsize} (block size = {bsize})"
            )));
        }
        if frag <= 0 || (frag as u64) * fsize != bsize {
            return Err(FilesystemError::Parse(format!(
                "ufs: frag={frag}, bsize={bsize}, fsize={fsize} inconsistent (frag * fsize must equal bsize)"
            )));
        }
        if fpg <= 0 {
            return Err(FilesystemError::Parse(format!(
                "ufs: implausible fragments-per-group {fpg}"
            )));
        }
        if ncg == 0 || ncg > 1_000_000 {
            return Err(FilesystemError::Parse(format!(
                "ufs: implausible cylinder-group count {ncg}"
            )));
        }
        if cblkno <= 0 {
            return Err(FilesystemError::Parse(format!(
                "ufs: implausible CG block offset {cblkno}"
            )));
        }
        if iblkno <= 0 || (iblkno as i64) <= (cblkno as i64) {
            return Err(FilesystemError::Parse(format!(
                "ufs: implausible inode-table offset iblkno={iblkno} (cblkno={cblkno})"
            )));
        }
        if sblkno < 0 {
            return Err(FilesystemError::Parse(format!(
                "ufs: implausible SB replica offset sblkno={sblkno}"
            )));
        }

        // Total fragment count. UFS1 stores it in the 32-bit `fs_old_size`
        // field at offset 36; UFS2 uses 64-bit `fs_size` at offset 1080.
        let total_frags = match version {
            UfsVersion::Ufs1 => {
                let v = read_i32(&sb, OFF_OLD_SIZE, endian);
                if v <= 0 {
                    return Err(FilesystemError::Parse(format!(
                        "ufs1: fs_old_size {v} is not positive"
                    )));
                }
                v as u64
            }
            UfsVersion::Ufs2 => {
                let v = read_i64(&sb, OFF_SIZE_UFS2, endian);
                if v <= 0 {
                    return Err(FilesystemError::Parse(format!(
                        "ufs2: fs_size {v} is not positive"
                    )));
                }
                v as u64
            }
        };

        // Refuse a softupdate-journaled volume that is also dirty. The
        // safe path on those is "let FreeBSD fsck_ffs replay the journal
        // first"; we don't want to risk silently dropping in-flight
        // updates by reading past the journal head. The `FS_INDEXDIRS` /
        // `FS_GJOURNAL` / `FS_SUJ` bits live in `fs_flags2` at offset
        // 0x35E; for Tier A we surface only the SU+J + dirty combination.
        // (`fs_clean` at byte 213 is the cheap is-dirty hint.)
        let su_j_enabled = (read_u32(&sb, OFF_FLAGS2, endian) & FS_SUJ) != 0;
        let fs_clean_byte = sb[213];
        if su_j_enabled && fs_clean_byte == FS_DIRTY {
            return Err(FilesystemError::Unsupported(
                "UFS: volume is softupdate-journaled (SU+J) and not cleanly unmounted; \
                 mount + sync on the source OS or run `fsck_ffs -y` before backing up"
                    .into(),
            ));
        }

        // Volume label. `tunefs -L name` writes it post-format; makefs and
        // the `newfs -L` flag both stamp this field. Empty (all-NUL) means
        // no label.
        let label_bytes = &sb[OFF_VOLNAME..OFF_VOLNAME + VOLNAME_LEN];
        let label = parse_label(label_bytes);

        Ok(Self {
            reader,
            partition_offset,
            sb_offset,
            version,
            endian,
            bsize,
            fsize,
            frag: frag as u32,
            ncg,
            fpg: fpg as u32,
            ipg,
            cblkno: cblkno as u32,
            iblkno: iblkno as u32,
            sblkno: sblkno as u32,
            total_frags,
            maxsymlinklen,
            label,
        })
    }

    /// Byte offset (relative to the partition start) of the cylinder-group
    /// header for CG index `i`. `cgbase(fs, i) = fpg * i` (FreeBSD's
    /// modern UFS layout, post-rotational-table removal); the CG header
    /// then lives at `cgbase + cblkno` fragments.
    pub(crate) fn cg_header_offset(&self, i: u32) -> u64 {
        ((i as u64) * (self.fpg as u64) + self.cblkno as u64) * self.fsize
    }

    /// Starting absolute fragment of CG `i`.
    pub(crate) fn cgbase_frag(&self, i: u32) -> u64 {
        (i as u64) * (self.fpg as u64)
    }

    /// Read CG `i`'s header (fixed portion + free-frag bitmap) and return
    /// the bitmap bytes plus the in-CG fragment count it covers.
    ///
    /// Validates the CG magic + cg_cgx index; refuses bitmaps that would
    /// extend past `cg_nextfreeoff` (the next-section sentinel) or past
    /// the CG block region in `fpg` fragments.
    pub(crate) fn read_cg_free_bitmap(
        &mut self,
        i: u32,
    ) -> Result<(Vec<u8>, u64), FilesystemError> {
        let cg_byte = self.partition_offset + self.cg_header_offset(i);

        // Fixed portion first — gives us cg_freeoff + cg_ndblk.
        self.reader.seek(SeekFrom::Start(cg_byte))?;
        let mut hdr = vec![0u8; CG_HEADER_FIXED_BYTES];
        self.reader.read_exact(&mut hdr)?;

        let magic = read_u32(&hdr, CG_OFF_MAGIC, self.endian);
        if magic != CG_MAGIC {
            return Err(FilesystemError::Parse(format!(
                "ufs CG {i}: bad magic 0x{magic:08X} (expected 0x{CG_MAGIC:08X}) at byte {cg_byte}"
            )));
        }
        let cgx = read_u32(&hdr, CG_OFF_CGX, self.endian);
        if cgx != i {
            return Err(FilesystemError::Parse(format!(
                "ufs CG {i}: cg_cgx says {cgx}, expected {i}"
            )));
        }
        let cg_ndblk = read_u32(&hdr, CG_OFF_NDBLK, self.endian);
        if cg_ndblk == 0 || cg_ndblk as u64 > self.fpg as u64 {
            return Err(FilesystemError::Parse(format!(
                "ufs CG {i}: implausible cg_ndblk {cg_ndblk} (fpg={})",
                self.fpg
            )));
        }
        let freeoff = read_u32(&hdr, CG_OFF_FREEOFF, self.endian) as u64;
        let nextfreeoff = read_u32(&hdr, CG_OFF_NEXTFREEOFF, self.endian) as u64;
        let iusedoff = read_u32(&hdr, CG_OFF_IUSEDOFF, self.endian) as u64;
        if freeoff < (CG_HEADER_FIXED_BYTES as u64).min(iusedoff) {
            // The bitmap can sit inside the first 256 bytes for tiny CG
            // layouts; guard only against the truly nonsensical case
            // where `cg_freeoff` lands before `cg_iusedoff`.
        }
        if nextfreeoff <= freeoff {
            return Err(FilesystemError::Parse(format!(
                "ufs CG {i}: cg_nextfreeoff {nextfreeoff} ≤ cg_freeoff {freeoff}"
            )));
        }

        // Compute bitmap length from cg_ndblk and confirm it fits the
        // declared CG region between freeoff and nextfreeoff.
        let expected_len = cg_ndblk.div_ceil(8) as u64;
        let region_len = nextfreeoff - freeoff;
        if expected_len > region_len {
            return Err(FilesystemError::Parse(format!(
                "ufs CG {i}: cg_ndblk {cg_ndblk} needs {expected_len} bitmap bytes, \
                 declared region [freeoff={freeoff}, nextfreeoff={nextfreeoff}) is {region_len}"
            )));
        }

        // Read the bitmap. We re-seek because freeoff might sit beyond
        // the 256-byte header read above.
        self.reader.seek(SeekFrom::Start(cg_byte + freeoff))?;
        let mut bitmap = vec![0u8; expected_len as usize];
        self.reader.read_exact(&mut bitmap)?;
        Ok((bitmap, cg_ndblk as u64))
    }

    // ---- U.4 (fsck) helpers ----

    /// Total addressable inode count across every CG (`ipg * ncg`). Inode 0
    /// and 1 are reserved; root lives at inum 2.
    pub(crate) fn total_inodes(&self) -> u64 {
        self.ipg as u64 * self.ncg as u64
    }

    /// Byte offset (relative to the partition start) of the replica
    /// superblock inside CG `cg`. Mirrors the kernel's `cgsblock(fs, c)`
    /// macro for layouts where `cgstart == cgbase` (UFS2 always; UFS1
    /// post-rotational-table-removal). Returns `None` if `cg >= ncg`.
    pub(crate) fn replica_sb_byte_offset(&self, cg: u32) -> Option<u64> {
        if cg >= self.ncg {
            return None;
        }
        Some((cg as u64 * self.fpg as u64 + self.sblkno as u64) * self.fsize)
    }

    /// Read the SB-sized prefix (`SB_READ_SIZE` bytes) at CG `cg`'s replica
    /// slot. Failures bubble up — callers (fsck) turn them into warnings.
    pub(crate) fn read_replica_sb_bytes(&mut self, cg: u32) -> Result<Vec<u8>, FilesystemError> {
        let off = self
            .replica_sb_byte_offset(cg)
            .ok_or_else(|| FilesystemError::Parse(format!("ufs: cg {cg} out of range")))?;
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + off))?;
        let mut buf = vec![0u8; SB_READ_SIZE];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Walk every disk fragment a regular-file / directory / symlink-with-
    /// out-of-line-target inode owns. Returns `(data_frags, indirect_frags)`
    /// — sparse-hole blocks (pointer = 0) are skipped. `indirect_frags`
    /// covers the indirect blocks themselves (which occupy disk space too)
    /// so a thorough bitmap consistency check can account for them.
    ///
    /// Special files (device / fifo / socket) and inline symlinks own no
    /// disk blocks; this returns `(vec![], vec![])` for those.
    pub(crate) fn walk_inode_blocks(
        &mut self,
        inode: &UfsInode,
    ) -> Result<(Vec<u64>, Vec<u64>), FilesystemError> {
        let mut data: Vec<u64> = Vec::new();
        let mut indirect: Vec<u64> = Vec::new();

        match unix_file_type(inode.mode) {
            UnixFileType::Regular
            | UnixFileType::Directory
            | UnixFileType::Unknown
            | UnixFileType::Symlink => {}
            _ => return Ok((data, indirect)),
        }

        // Inline symlinks live in the dinode payload area — no disk blocks
        // beyond the dinode itself, which the bitmap already accounts for.
        if matches!(unix_file_type(inode.mode), UnixFileType::Symlink)
            && self.maxsymlinklen > 0
            && (inode.size as u32) <= self.maxsymlinklen
        {
            return Ok((data, indirect));
        }

        // How many logical blocks does the file span? `bsize` is the block
        // size; the last block may be partial (smaller than `bsize`) which
        // is fine because we walk by logical-block index, not by byte.
        let total_bytes = inode.size;
        if total_bytes == 0 {
            return Ok((data, indirect));
        }
        let bs = self.bsize;
        let last_lbn = total_bytes.div_ceil(bs);

        // First 12 logical blocks live in `direct[]`; no indirect blocks
        // are touched until the file spans more than that.
        let ndaddr = UFS_NDADDR as u64;
        for lbn in 0..last_lbn.min(ndaddr) {
            let frag = inode.direct[lbn as usize];
            if frag != 0 {
                data.push(frag);
            }
        }
        if last_lbn <= ndaddr {
            return Ok((data, indirect));
        }

        let nindir = self.nindir();

        // Single indirect — one indirect block, `nindir` pointers.
        if inode.indirect[0] != 0 {
            indirect.push(inode.indirect[0]);
            let span = (last_lbn - ndaddr).min(nindir);
            for i in 0..span {
                let frag = self.read_indirect_pointer(inode.indirect[0], i)?;
                if frag != 0 {
                    data.push(frag);
                }
            }
        }
        if last_lbn <= ndaddr + nindir {
            return Ok((data, indirect));
        }

        // Double indirect — one top + nindir middle + nindir² leaves.
        let n2 = nindir.saturating_mul(nindir);
        if inode.indirect[1] != 0 {
            indirect.push(inode.indirect[1]);
            let span = (last_lbn - ndaddr - nindir).min(n2);
            let mid_count = span.div_ceil(nindir);
            for m in 0..mid_count {
                let mid = self.read_indirect_pointer(inode.indirect[1], m)?;
                if mid == 0 {
                    continue;
                }
                indirect.push(mid);
                let lo = m * nindir;
                let hi = ((m + 1) * nindir).min(span);
                for inner in lo..hi {
                    let frag = self.read_indirect_pointer(mid, inner - lo)?;
                    if frag != 0 {
                        data.push(frag);
                    }
                }
            }
        }
        if last_lbn <= ndaddr + nindir + n2 {
            return Ok((data, indirect));
        }

        // Triple indirect — one top + nindir mid + nindir² inner.
        let n3 = n2.saturating_mul(nindir);
        if inode.indirect[2] != 0 {
            indirect.push(inode.indirect[2]);
            let span = (last_lbn - ndaddr - nindir - n2).min(n3);
            let outer_count = span.div_ceil(n2);
            for o in 0..outer_count {
                let mid_top = self.read_indirect_pointer(inode.indirect[2], o)?;
                if mid_top == 0 {
                    continue;
                }
                indirect.push(mid_top);
                let outer_lo = o * n2;
                let outer_hi = ((o + 1) * n2).min(span);
                let outer_span = outer_hi - outer_lo;
                let inner_count = outer_span.div_ceil(nindir);
                for m in 0..inner_count {
                    let inner_idx = self.read_indirect_pointer(mid_top, m)?;
                    if inner_idx == 0 {
                        continue;
                    }
                    indirect.push(inner_idx);
                    let lo = m * nindir;
                    let hi = ((m + 1) * nindir).min(outer_span);
                    for leaf in lo..hi {
                        let frag = self.read_indirect_pointer(inner_idx, leaf - lo)?;
                        if frag != 0 {
                            data.push(frag);
                        }
                    }
                }
            }
        }
        Ok((data, indirect))
    }

    /// Read raw bytes from `inode`'s data fork into `buf`, used by fsck's
    /// directory walk so it can iterate DIRENT2 records without going
    /// through the `Filesystem::list_directory` path (which is recursive
    /// and would re-enter the trait).
    pub(crate) fn read_dir_bytes_raw(
        &mut self,
        inode: &UfsInode,
    ) -> Result<Vec<u8>, FilesystemError> {
        if inode.size as usize > MAX_DIR_BYTES {
            return Err(FilesystemError::Parse(format!(
                "ufs: directory inode {} claims size {} bytes (cap {MAX_DIR_BYTES})",
                inode.inum, inode.size
            )));
        }
        self.read_inode_data(inode, inode.size, inode.size as usize)
    }

    /// Endian-aware accessor for `ufs_fsck` so the helper can decode
    /// replica SB byte slices without re-exporting every per-field reader.
    pub(crate) fn endian_value(&self) -> UfsEndian {
        self.endian
    }

    // ---- U.3: dinode + directory + file walk ----

    /// On-disk byte offset (relative to the partition start) of inode
    /// `inum`'s dinode. Mirrors FreeBSD's `ino_to_fsba(fs, ino)` +
    /// `ino_to_fsbo(fs, ino)` macros, expressed in bytes directly.
    pub(crate) fn inode_byte_offset(&self, inum: u32) -> u64 {
        let dsize = self.dinode_size();
        let ipg = self.ipg as u64;
        let cg = inum as u64 / ipg;
        let in_cg = inum as u64 % ipg;
        (cg * self.fpg as u64 + self.iblkno as u64) * self.fsize + in_cg * dsize
    }

    fn dinode_size(&self) -> u64 {
        match self.version {
            UfsVersion::Ufs1 => DINODE1_SIZE,
            UfsVersion::Ufs2 => DINODE2_SIZE,
        }
    }

    fn pointer_size(&self) -> u64 {
        match self.version {
            UfsVersion::Ufs1 => 4,
            UfsVersion::Ufs2 => 8,
        }
    }

    /// Number of pointers per indirect block (`NINDIR(fs)` in the kernel).
    /// UFS1: bsize / 4; UFS2: bsize / 8.
    fn nindir(&self) -> u64 {
        self.bsize / self.pointer_size()
    }

    /// Read a single inode by number. Inode 0 and inode 1 are reserved
    /// and rejected; out-of-range inodes are too.
    pub(crate) fn read_inode(&mut self, inum: u32) -> Result<UfsInode, FilesystemError> {
        if inum < ROOT_INODE {
            return Err(FilesystemError::Parse(format!(
                "ufs: invalid inode number {inum} (must be >= {ROOT_INODE})"
            )));
        }
        let total_inodes = (self.ipg as u64).saturating_mul(self.ncg as u64);
        if inum as u64 >= total_inodes {
            return Err(FilesystemError::Parse(format!(
                "ufs: inode {inum} out of range (total inodes = {total_inodes})"
            )));
        }
        let dsize = self.dinode_size();
        let byte = self.partition_offset + self.inode_byte_offset(inum);
        self.reader.seek(SeekFrom::Start(byte))?;
        let mut buf = vec![0u8; dsize as usize];
        self.reader.read_exact(&mut buf)?;

        let endian = self.endian;
        let mode = read_u16(&buf, D1_OFF_MODE, endian) as u32;
        let nlink = read_i16(&buf, D1_OFF_NLINK, endian) as i32 as u32;

        let (uid, gid, size, mtime, direct, indirect, inline_payload) = match self.version {
            UfsVersion::Ufs1 => {
                let size = read_u64(&buf, D1_OFF_SIZE, endian);
                let mtime = read_i32(&buf, D1_OFF_MTIME, endian) as i64;
                let uid = read_u32(&buf, D1_OFF_UID, endian);
                let gid = read_u32(&buf, D1_OFF_GID, endian);
                let mut direct = [0u64; UFS_NDADDR];
                for (i, slot) in direct.iter_mut().enumerate() {
                    *slot = read_i32(&buf, D1_OFF_DB + i * 4, endian) as u32 as u64;
                }
                let mut indirect = [0u64; UFS_NIADDR];
                for (i, slot) in indirect.iter_mut().enumerate() {
                    *slot = read_i32(&buf, D1_OFF_IB + i * 4, endian) as u32 as u64;
                }
                // Inline area = bytes [40, 100) — covers di_db (48 B) + di_ib (12 B).
                let inline_payload = buf[D1_OFF_DB..D1_OFF_DB + 60].to_vec();
                (uid, gid, size, mtime, direct, indirect, inline_payload)
            }
            UfsVersion::Ufs2 => {
                let uid = read_u32(&buf, D2_OFF_UID, endian);
                let gid = read_u32(&buf, D2_OFF_GID, endian);
                let size = read_u64(&buf, D2_OFF_SIZE, endian);
                let mtime = read_i64(&buf, D2_OFF_MTIME, endian);
                let mut direct = [0u64; UFS_NDADDR];
                for (i, slot) in direct.iter_mut().enumerate() {
                    *slot = read_i64(&buf, D2_OFF_DB + i * 8, endian) as u64;
                }
                let mut indirect = [0u64; UFS_NIADDR];
                for (i, slot) in indirect.iter_mut().enumerate() {
                    *slot = read_i64(&buf, D2_OFF_IB + i * 8, endian) as u64;
                }
                // Inline area = bytes [112, 232) — covers di_db (96 B) + di_ib (24 B).
                let inline_payload = buf[D2_OFF_DB..D2_OFF_DB + 120].to_vec();
                (uid, gid, size, mtime, direct, indirect, inline_payload)
            }
        };

        Ok(UfsInode {
            inum,
            mode,
            nlink,
            uid,
            gid,
            size,
            mtime,
            direct,
            indirect,
            inline_payload,
        })
    }

    /// Resolve logical block index `lbn` of the given inode to a starting
    /// fragment number on disk. Sparse (hole) blocks return 0. Walks the
    /// 12-direct → single → double → triple indirect chain.
    pub(crate) fn resolve_logical_block(
        &mut self,
        inode: &UfsInode,
        lbn: u64,
    ) -> Result<u64, FilesystemError> {
        let nindir = self.nindir();
        let ndaddr = UFS_NDADDR as u64;

        if lbn < ndaddr {
            return Ok(inode.direct[lbn as usize]);
        }
        let lbn = lbn - ndaddr;

        // Single indirect: nindir blocks addressable.
        if lbn < nindir {
            let ib = inode.indirect[0];
            if ib == 0 {
                return Ok(0);
            }
            return self.read_indirect_pointer(ib, lbn);
        }
        let lbn = lbn - nindir;

        // Double indirect: nindir² blocks.
        let n2 = nindir.saturating_mul(nindir);
        if lbn < n2 {
            let ib = inode.indirect[1];
            if ib == 0 {
                return Ok(0);
            }
            let outer = lbn / nindir;
            let inner = lbn % nindir;
            let l1 = self.read_indirect_pointer(ib, outer)?;
            if l1 == 0 {
                return Ok(0);
            }
            return self.read_indirect_pointer(l1, inner);
        }
        let lbn = lbn - n2;

        // Triple indirect: nindir³ blocks. Past that, the file would
        // exceed the maximum addressable size — refuse rather than wrap.
        let n3 = n2.saturating_mul(nindir);
        if lbn < n3 {
            let ib = inode.indirect[2];
            if ib == 0 {
                return Ok(0);
            }
            let outer = lbn / n2;
            let mid_rem = lbn % n2;
            let middle = mid_rem / nindir;
            let inner = mid_rem % nindir;
            let l1 = self.read_indirect_pointer(ib, outer)?;
            if l1 == 0 {
                return Ok(0);
            }
            let l2 = self.read_indirect_pointer(l1, middle)?;
            if l2 == 0 {
                return Ok(0);
            }
            return self.read_indirect_pointer(l2, inner);
        }
        Err(FilesystemError::Parse(format!(
            "ufs: inode {} logical block index out of addressable range",
            inode.inum
        )))
    }

    /// Read `idx`-th pointer from the indirect block whose starting
    /// fragment is `block_frag`. Pointer size matches the FS version
    /// (UFS1: 4 bytes; UFS2: 8 bytes).
    fn read_indirect_pointer(&mut self, block_frag: u64, idx: u64) -> Result<u64, FilesystemError> {
        let ptr_size = self.pointer_size();
        let byte = self.partition_offset + block_frag * self.fsize + idx * ptr_size;
        self.reader.seek(SeekFrom::Start(byte))?;
        match self.version {
            UfsVersion::Ufs1 => {
                let mut buf = [0u8; 4];
                self.reader.read_exact(&mut buf)?;
                Ok(read_i32(&buf, 0, self.endian) as u32 as u64)
            }
            UfsVersion::Ufs2 => {
                let mut buf = [0u8; 8];
                self.reader.read_exact(&mut buf)?;
                Ok(read_u64(&buf, 0, self.endian))
            }
        }
    }

    /// Read up to `min(file_size, max_bytes)` bytes of an inode's data
    /// fork. Sparse holes (zero pointer) emit zeros without touching
    /// disk.
    pub(crate) fn read_inode_data(
        &mut self,
        inode: &UfsInode,
        file_size: u64,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let total = (file_size as usize).min(max_bytes);
        let mut out = Vec::with_capacity(total);
        let mut lbn: u64 = 0;
        while out.len() < total {
            let remaining = total - out.len();
            let want = (self.bsize as usize).min(remaining);
            let block_frag = self.resolve_logical_block(inode, lbn)?;
            if block_frag == 0 {
                out.extend(std::iter::repeat_n(0u8, want));
            } else {
                let byte = self.partition_offset + block_frag * self.fsize;
                self.reader.seek(SeekFrom::Start(byte))?;
                let start = out.len();
                out.resize(start + want, 0);
                self.reader.read_exact(&mut out[start..start + want])?;
            }
            lbn += 1;
        }
        Ok(out)
    }

    /// Decode a symlink's target. Inline (fast) symlinks live in the
    /// dinode's pointer area; non-inline symlinks live in the first
    /// data block.
    fn read_symlink_target(&mut self, inode: &UfsInode) -> Result<String, FilesystemError> {
        let size = inode.size as usize;
        if size == 0 {
            return Ok(String::new());
        }
        let bytes = if (inode.size as u32) <= self.maxsymlinklen
            && self.maxsymlinklen > 0
            && size <= inode.inline_payload.len()
        {
            inode.inline_payload[..size].to_vec()
        } else {
            self.read_inode_data(inode, inode.size, size)?
        };
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    /// Compose a `FileEntry` for `child_inode`, naming it `name` and
    /// rooting its path under `parent`.
    fn build_file_entry(
        &mut self,
        name: &str,
        parent: &FileEntry,
        child_inode: &UfsInode,
    ) -> Result<FileEntry, FilesystemError> {
        let parent_path = if parent.path == "/" {
            String::new()
        } else {
            parent.path.clone()
        };
        let path = format!("{parent_path}/{name}");
        let loc = child_inode.inum as u64;
        let ft = unix_file_type(child_inode.mode);
        let mut entry = match ft {
            UnixFileType::Directory => FileEntry::new_directory(name.to_string(), path, loc),
            UnixFileType::Symlink => {
                let target = self.read_symlink_target(child_inode)?;
                FileEntry::new_symlink(name.to_string(), path, child_inode.size, loc, target)
            }
            UnixFileType::BlockDevice
            | UnixFileType::CharDevice
            | UnixFileType::Fifo
            | UnixFileType::Socket => FileEntry::new_special(
                name.to_string(),
                path,
                loc,
                match ft {
                    UnixFileType::BlockDevice => "block device".into(),
                    UnixFileType::CharDevice => "char device".into(),
                    UnixFileType::Fifo => "fifo".into(),
                    UnixFileType::Socket => "socket".into(),
                    _ => unreachable!(),
                },
            ),
            UnixFileType::Regular | UnixFileType::Unknown => {
                FileEntry::new_file(name.to_string(), path, child_inode.size, loc)
            }
        };
        entry.mode = Some(child_inode.mode);
        entry.uid = Some(child_inode.uid);
        entry.gid = Some(child_inode.gid);
        if child_inode.mtime != 0 {
            entry.modified = Some(format_unix_timestamp(child_inode.mtime));
        }
        Ok(entry)
    }

    /// Read-back accessors so the compactor (U.2) and browse layer (U.3)
    /// don't need to refetch the SB.
    pub fn version(&self) -> UfsVersion {
        self.version
    }

    pub fn endian(&self) -> UfsEndian {
        self.endian
    }

    pub fn superblock_offset(&self) -> u64 {
        self.sb_offset
    }

    /// On-disk superblock byte offset for `sb_block` arithmetic (alias of
    /// [`Self::superblock_offset`] but expressed as a fragment index for
    /// callers that want it in the same units the SB itself uses).
    #[allow(dead_code)] // U.2 will consume this when scanning bitmaps
    pub fn sb_frag(&self) -> u64 {
        self.sb_offset / self.fsize
    }

    /// Endian-aware accessor used by `ufs.rs`-internal tests.
    #[cfg(test)]
    fn endian_for_test(&self) -> UfsEndian {
        self.endian
    }
}

// ---- U.4 write primitives ----
//
// Everything in this impl block requires `R: Read + Write + Seek + Send` and
// is consumed exclusively by the editable surface (`impl
// EditableFilesystem`, plus the `repair_ufs` companion). Mirrors the
// `efs.rs` split — read-only handles can't accidentally call write methods
// because the trait bounds don't include `Write`.
//
// **Bitmap polarity, again** — UFS keeps two bitmaps per cylinder group:
//   * **`cg_freeoff`** (free-fragment bitmap): **set bit = FREE**, clear =
//     in use. Opposite of every Linux FS we touch.
//   * **`cg_iusedoff`** (inode-used bitmap): **set bit = USED**, clear =
//     free. Standard convention, same as ext / xfs / etc.
//
// The `mark_frag_*` / `mark_inode_*` helpers below name the operation by
// effect (allocated vs free) so callers don't have to remember which
// polarity applies to which bitmap.

impl<R: Read + Write + Seek + Send> UfsFilesystem<R> {
    /// Write raw bytes at the start of fragment `start_frag`. The caller
    /// is responsible for sizing `data` to a multiple of `fsize` when
    /// aligning with the on-disk fragment grid is required (which is
    /// almost always — every block-level mutation lands here).
    pub(crate) fn write_frag_run(
        &mut self,
        start_frag: u64,
        data: &[u8],
    ) -> Result<(), FilesystemError> {
        let byte_len = data.len() as u64;
        let end_byte = start_frag * self.fsize + byte_len;
        let max_byte = self.total_frags * self.fsize;
        if end_byte > max_byte {
            return Err(FilesystemError::InvalidData(format!(
                "ufs write_frag_run: fragment {start_frag} + {byte_len} bytes \
                 exceeds total volume bytes {max_byte}"
            )));
        }
        let byte = self.partition_offset + start_frag * self.fsize;
        self.reader.seek(SeekFrom::Start(byte))?;
        self.reader.write_all(data)?;
        Ok(())
    }

    /// Encode `inode` into its on-disk dinode shape (UFS1: 128 B, UFS2:
    /// 256 B) and write it at `inum`'s slot. Fields the read path captures
    /// round-trip; fields the read path ignores (atime / ctime / generation
    /// / flags / blocks count / spare slots) are left zero — the kernel
    /// updates them on mount, and our oracle (`fsck.ufs -n`) doesn't fault
    /// on a zero-stamped generation.
    pub(crate) fn write_inode(
        &mut self,
        inum: u32,
        inode: &UfsInode,
    ) -> Result<(), FilesystemError> {
        if inum < ROOT_INODE {
            return Err(FilesystemError::InvalidData(format!(
                "ufs write_inode: invalid inum {inum} (must be >= {ROOT_INODE})"
            )));
        }
        if inum as u64 >= self.total_inodes() {
            return Err(FilesystemError::InvalidData(format!(
                "ufs write_inode: inum {inum} out of range (total inodes = {})",
                self.total_inodes()
            )));
        }
        let dsize = self.dinode_size() as usize;
        let mut buf = vec![0u8; dsize];
        let endian = self.endian;

        // di_mode + di_nlink share offsets across versions.
        write_u16(&mut buf, D1_OFF_MODE, inode.mode as u16, endian);
        write_i16(&mut buf, D1_OFF_NLINK, inode.nlink as i16, endian);

        match self.version {
            UfsVersion::Ufs1 => {
                write_u64(&mut buf, D1_OFF_SIZE, inode.size, endian);
                write_i32(&mut buf, D1_OFF_MTIME, inode.mtime as i32, endian);
                write_u32(&mut buf, D1_OFF_UID, inode.uid, endian);
                write_u32(&mut buf, D1_OFF_GID, inode.gid, endian);
                for (i, &slot) in inode.direct.iter().enumerate() {
                    write_i32(&mut buf, D1_OFF_DB + i * 4, slot as i32, endian);
                }
                for (i, &slot) in inode.indirect.iter().enumerate() {
                    write_i32(&mut buf, D1_OFF_IB + i * 4, slot as i32, endian);
                }
                // Inline payload (fast symlink target, etc.) overlays the
                // direct/indirect pointer area. Callers that want to write
                // an inline symlink should populate `inline_payload` AND
                // leave direct/indirect zero; the inline bytes win.
                if !inode.inline_payload.is_empty() {
                    let len = inode.inline_payload.len().min(60);
                    buf[D1_OFF_DB..D1_OFF_DB + len].copy_from_slice(&inode.inline_payload[..len]);
                }
            }
            UfsVersion::Ufs2 => {
                write_u32(&mut buf, D2_OFF_UID, inode.uid, endian);
                write_u32(&mut buf, D2_OFF_GID, inode.gid, endian);
                write_u64(&mut buf, D2_OFF_SIZE, inode.size, endian);
                write_i64(&mut buf, D2_OFF_MTIME, inode.mtime, endian);
                for (i, &slot) in inode.direct.iter().enumerate() {
                    write_i64(&mut buf, D2_OFF_DB + i * 8, slot as i64, endian);
                }
                for (i, &slot) in inode.indirect.iter().enumerate() {
                    write_i64(&mut buf, D2_OFF_IB + i * 8, slot as i64, endian);
                }
                if !inode.inline_payload.is_empty() {
                    let len = inode.inline_payload.len().min(120);
                    buf[D2_OFF_DB..D2_OFF_DB + len].copy_from_slice(&inode.inline_payload[..len]);
                }
            }
        }

        let byte = self.partition_offset + self.inode_byte_offset(inum);
        self.reader.seek(SeekFrom::Start(byte))?;
        self.reader.write_all(&buf)?;
        Ok(())
    }

    /// Read CG `cg`'s inode-used bitmap. Sibling to `read_cg_free_bitmap`;
    /// covers `ipg` bits (`SET = used`, opposite polarity to the free-frag
    /// bitmap which is `SET = free`).
    pub(crate) fn read_cg_iused_bitmap(&mut self, cg: u32) -> Result<Vec<u8>, FilesystemError> {
        let cg_byte = self.partition_offset + self.cg_header_offset(cg);
        self.reader.seek(SeekFrom::Start(cg_byte))?;
        let mut hdr = vec![0u8; CG_HEADER_FIXED_BYTES];
        self.reader.read_exact(&mut hdr)?;
        let magic = read_u32(&hdr, CG_OFF_MAGIC, self.endian);
        if magic != CG_MAGIC {
            return Err(FilesystemError::Parse(format!(
                "ufs CG {cg}: bad magic 0x{magic:08X} (expected 0x{CG_MAGIC:08X})"
            )));
        }
        let iusedoff = read_u32(&hdr, CG_OFF_IUSEDOFF, self.endian) as u64;
        let freeoff = read_u32(&hdr, CG_OFF_FREEOFF, self.endian) as u64;
        if iusedoff == 0 || iusedoff >= freeoff {
            return Err(FilesystemError::Parse(format!(
                "ufs CG {cg}: implausible iusedoff={iusedoff} freeoff={freeoff}"
            )));
        }
        let bytes = (self.ipg as u64).div_ceil(8);
        if iusedoff + bytes > freeoff {
            return Err(FilesystemError::Parse(format!(
                "ufs CG {cg}: iused bitmap [{iusedoff}, {}) overlaps freeoff {freeoff}",
                iusedoff + bytes
            )));
        }
        self.reader.seek(SeekFrom::Start(cg_byte + iusedoff))?;
        let mut bm = vec![0u8; bytes as usize];
        self.reader.read_exact(&mut bm)?;
        Ok(bm)
    }

    /// Write CG `cg`'s free-fragment bitmap back to disk. Caller-supplied
    /// `bm` must match the bitmap's on-disk length (`cg_ndblk` bits, padded
    /// to a byte boundary).
    pub(crate) fn write_cg_free_bitmap(
        &mut self,
        cg: u32,
        bm: &[u8],
    ) -> Result<(), FilesystemError> {
        let (cg_byte, freeoff) = self.cg_freeoff_byte(cg)?;
        self.reader.seek(SeekFrom::Start(cg_byte + freeoff))?;
        self.reader.write_all(bm)?;
        Ok(())
    }

    /// Write CG `cg`'s inode-used bitmap back to disk. Caller-supplied `bm`
    /// must hold `ipg` bits worth of bytes.
    pub(crate) fn write_cg_iused_bitmap(
        &mut self,
        cg: u32,
        bm: &[u8],
    ) -> Result<(), FilesystemError> {
        let (cg_byte, iusedoff) = self.cg_iusedoff_byte(cg)?;
        self.reader.seek(SeekFrom::Start(cg_byte + iusedoff))?;
        self.reader.write_all(bm)?;
        Ok(())
    }

    /// Read CG `cg`'s `cg_cs` summary (`ndir`, `nbfree`, `nifree`,
    /// `nffree` — each i32). Used by sync_metadata to propagate the +/-1
    /// delta from an inode allocation up to the per-CG counter.
    pub(crate) fn read_cg_cs(&mut self, cg: u32) -> Result<(i32, i32, i32, i32), FilesystemError> {
        let cg_byte = self.partition_offset + self.cg_header_offset(cg);
        self.reader.seek(SeekFrom::Start(cg_byte))?;
        let mut hdr = vec![0u8; CG_HEADER_FIXED_BYTES];
        self.reader.read_exact(&mut hdr)?;
        let endian = self.endian;
        Ok((
            read_i32(&hdr, 24, endian),
            read_i32(&hdr, 28, endian),
            read_i32(&hdr, 32, endian),
            read_i32(&hdr, 36, endian),
        ))
    }

    /// Update CG `cg`'s `cg_cs` summary by the given deltas.
    pub(crate) fn update_cg_cs(
        &mut self,
        cg: u32,
        d_ndir: i32,
        d_nbfree: i32,
        d_nifree: i32,
        d_nffree: i32,
    ) -> Result<(), FilesystemError> {
        let (ndir, nbfree, nifree, nffree) = self.read_cg_cs(cg)?;
        let new = [
            ndir.wrapping_add(d_ndir),
            nbfree.wrapping_add(d_nbfree),
            nifree.wrapping_add(d_nifree),
            nffree.wrapping_add(d_nffree),
        ];
        let mut buf = [0u8; 16];
        let endian = self.endian;
        write_i32(&mut buf, 0, new[0], endian);
        write_i32(&mut buf, 4, new[1], endian);
        write_i32(&mut buf, 8, new[2], endian);
        write_i32(&mut buf, 12, new[3], endian);
        let cg_byte = self.partition_offset + self.cg_header_offset(cg);
        self.reader.seek(SeekFrom::Start(cg_byte + 24))?;
        self.reader.write_all(&buf)?;
        Ok(())
    }

    /// Allocate a free inode. Walks CGs starting from `cg_hint` (typically
    /// the parent directory's CG so children land near their parent), sets
    /// the inum's bit in `cg_iusedoff`, and decrements `cs_nifree`. Returns
    /// the new inum (always ≥ `ROOT_INODE`).
    pub(crate) fn alloc_inode(&mut self, cg_hint: u32) -> Result<u32, FilesystemError> {
        let start = cg_hint % self.ncg;
        let inodes_per_cg = self.ipg as u64;
        for offset in 0..self.ncg {
            let cg = (start + offset) % self.ncg;
            let mut bm = self.read_cg_iused_bitmap(cg)?;
            // Inode bitmap is SET = USED — find a CLEAR bit.
            if let Some(in_cg) = bitmap_find_clear_bit(&bm, inodes_per_cg) {
                // CG 0 reserves the first 2 inums (0 = unused, 1 = legacy
                // bad-blocks); never hand out below ROOT_INODE.
                let global = cg as u64 * inodes_per_cg + in_cg;
                if global < ROOT_INODE as u64 {
                    continue;
                }
                bitmap_set_bit(&mut bm, in_cg);
                self.write_cg_iused_bitmap(cg, &bm)?;
                self.update_cg_cs(cg, 0, 0, -1, 0)?;
                return Ok(global as u32);
            }
        }
        Err(FilesystemError::DiskFull(
            "ufs alloc_inode: no free inode in any cylinder group".into(),
        ))
    }

    /// Free `inum`'s bit in its CG's `cg_iusedoff` bitmap and zero out the
    /// on-disk dinode. Caller is responsible for first freeing any
    /// fragments the inode owns.
    pub(crate) fn free_inode(&mut self, inum: u32) -> Result<(), FilesystemError> {
        if inum < ROOT_INODE || inum as u64 >= self.total_inodes() {
            return Err(FilesystemError::InvalidData(format!(
                "ufs free_inode: invalid inum {inum}"
            )));
        }
        let cg = inum / self.ipg;
        let in_cg = (inum % self.ipg) as u64;
        let mut bm = self.read_cg_iused_bitmap(cg)?;
        bitmap_clear_bit(&mut bm, in_cg);
        self.write_cg_iused_bitmap(cg, &bm)?;
        self.update_cg_cs(cg, 0, 0, 1, 0)?;

        // Zero the dinode so a future scan doesn't see stale metadata.
        let zero = vec![0u8; self.dinode_size() as usize];
        let byte = self.partition_offset + self.inode_byte_offset(inum);
        self.reader.seek(SeekFrom::Start(byte))?;
        self.reader.write_all(&zero)?;
        Ok(())
    }

    /// Allocate a contiguous run of `count` fragments somewhere on disk.
    /// Walks CGs starting from `cg_hint`, picks the first CG with a clear
    /// `count`-bit window in its free-fragment bitmap, marks the bits
    /// allocated (CLEAR), and returns the absolute starting fragment.
    ///
    /// `count` of 0 is rejected; `count > fpg` is rejected (we don't span
    /// CG boundaries).
    pub(crate) fn alloc_frag_run(
        &mut self,
        count: u64,
        cg_hint: u32,
    ) -> Result<u64, FilesystemError> {
        if count == 0 {
            return Err(FilesystemError::InvalidData(
                "ufs alloc_frag_run: count == 0".into(),
            ));
        }
        if count > self.fpg as u64 {
            return Err(FilesystemError::Unsupported(format!(
                "ufs alloc_frag_run: cross-CG run of {count} fragments not supported \
                 (fpg = {})",
                self.fpg
            )));
        }
        let start = cg_hint % self.ncg;
        for offset in 0..self.ncg {
            let cg = (start + offset) % self.ncg;
            let (mut bm, ndblk) = self.read_cg_free_bitmap(cg)?;
            // Find first clear-bit window. Bitmap polarity: SET = FREE,
            // so a "clear-bit window" in our search semantics means
            // "consecutive SET bits in the bitmap". Walk linearly.
            let cap = ndblk.min(self.fpg as u64);
            let mut run_start: Option<u64> = None;
            let mut run_len: u64 = 0;
            let mut found: Option<u64> = None;
            for bit in 0..cap {
                let byte = (bit / 8) as usize;
                let bit_idx = (bit % 8) as u8;
                let is_free = bm[byte] & (1u8 << bit_idx) != 0;
                if is_free {
                    if run_start.is_none() {
                        run_start = Some(bit);
                    }
                    run_len += 1;
                    if run_len >= count {
                        found = run_start;
                        break;
                    }
                } else {
                    run_start = None;
                    run_len = 0;
                }
            }
            if let Some(in_cg) = found {
                for i in 0..count {
                    bitmap_clear_bit(&mut bm, in_cg + i);
                }
                self.write_cg_free_bitmap(cg, &bm)?;
                // `nffree` tracks free fragments — drop by `count`. `nbfree`
                // (free *blocks*) would only change when a whole-block worth
                // of frags transitions; we leave it alone for sub-block
                // allocations (matches FreeBSD's `ffs_clusteracct` behavior
                // for the common case our edit path hits).
                self.update_cg_cs(cg, 0, 0, 0, -(count as i32))?;
                return Ok(cg as u64 * self.fpg as u64 + in_cg);
            }
        }
        Err(FilesystemError::DiskFull(format!(
            "ufs alloc_frag_run: no {count}-fragment free run in any cylinder group"
        )))
    }

    /// Free a previously-allocated run of `count` fragments starting at
    /// absolute fragment `start`. The run must lie inside a single CG.
    pub(crate) fn free_frag_run(&mut self, start: u64, count: u64) -> Result<(), FilesystemError> {
        if count == 0 {
            return Ok(());
        }
        let cg = start / self.fpg as u64;
        let in_cg = start % self.fpg as u64;
        if cg >= self.ncg as u64 {
            return Err(FilesystemError::InvalidData(format!(
                "ufs free_frag_run: start fragment {start} past total volume"
            )));
        }
        if in_cg + count > self.fpg as u64 {
            return Err(FilesystemError::Unsupported(format!(
                "ufs free_frag_run: run [{start}..{}) crosses a CG boundary",
                start + count
            )));
        }
        let (mut bm, _ndblk) = self.read_cg_free_bitmap(cg as u32)?;
        for i in 0..count {
            bitmap_set_bit(&mut bm, in_cg + i);
        }
        self.write_cg_free_bitmap(cg as u32, &bm)?;
        self.update_cg_cs(cg as u32, 0, 0, 0, count as i32)?;
        Ok(())
    }

    // ---- Internal byte-offset helpers ----

    /// Return `(cg_header_byte, freeoff)` where the free-frag bitmap of
    /// `cg` lives — header byte + freeoff = bitmap start byte.
    fn cg_freeoff_byte(&mut self, cg: u32) -> Result<(u64, u64), FilesystemError> {
        let cg_byte = self.partition_offset + self.cg_header_offset(cg);
        self.reader.seek(SeekFrom::Start(cg_byte))?;
        let mut hdr = [0u8; 8];
        self.reader.read_exact(&mut hdr)?;
        let magic = read_u32(&hdr, CG_OFF_MAGIC, self.endian);
        if magic != CG_MAGIC {
            return Err(FilesystemError::Parse(format!(
                "ufs CG {cg}: bad magic on write path"
            )));
        }
        self.reader
            .seek(SeekFrom::Start(cg_byte + CG_OFF_FREEOFF as u64))?;
        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf)?;
        let freeoff = read_u32(&buf, 0, self.endian) as u64;
        Ok((cg_byte, freeoff))
    }

    /// Return `(cg_header_byte, iusedoff)` where the inode-used bitmap of
    /// `cg` lives.
    fn cg_iusedoff_byte(&mut self, cg: u32) -> Result<(u64, u64), FilesystemError> {
        let cg_byte = self.partition_offset + self.cg_header_offset(cg);
        self.reader
            .seek(SeekFrom::Start(cg_byte + CG_OFF_IUSEDOFF as u64))?;
        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf)?;
        let iusedoff = read_u32(&buf, 0, self.endian) as u64;
        Ok((cg_byte, iusedoff))
    }

    /// Read `SB_READ_SIZE` bytes of the primary superblock. Used by the
    /// repair pass to source the byte image we copy into each CG's
    /// replica slot.
    pub(crate) fn read_primary_sb_bytes(&mut self) -> Result<Vec<u8>, FilesystemError> {
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + self.sb_offset))?;
        let mut buf = vec![0u8; SB_READ_SIZE];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Write `SB_READ_SIZE` bytes into CG `cg`'s replica SB slot. Refuses
    /// `cg == 0` (the primary occupies that slot — overwriting it via the
    /// replica path would be a no-op in the best case and a corruption in
    /// the worst). Refuses a `bytes` slice that isn't exactly
    /// `SB_READ_SIZE` long.
    pub(crate) fn write_replica_sb_bytes(
        &mut self,
        cg: u32,
        bytes: &[u8],
    ) -> Result<(), FilesystemError> {
        if cg == 0 {
            return Err(FilesystemError::InvalidData(
                "ufs write_replica_sb_bytes: refusing to rewrite CG 0 replica slot".into(),
            ));
        }
        if bytes.len() != SB_READ_SIZE {
            return Err(FilesystemError::InvalidData(format!(
                "ufs write_replica_sb_bytes: expected {SB_READ_SIZE} bytes, got {}",
                bytes.len()
            )));
        }
        let off = self
            .replica_sb_byte_offset(cg)
            .ok_or_else(|| FilesystemError::Parse(format!("ufs: cg {cg} out of range")))?;
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + off))?;
        self.reader.write_all(bytes)?;
        Ok(())
    }
}

// ---- U.4 directory + file-data helpers + EditableFilesystem ----
//
// The trait surface mirrors `efs.rs` (create_file / create_directory /
// delete_entry / sync_metadata / free_space). v1 scope is intentionally
// conservative:
//   - File data fork: direct extents only (≤ 12 × bsize). Indirect
//     blocks (> 96 KiB at the typical 8 KiB block size) are rejected
//     with `Unsupported` rather than corrupting the allocator with
//     a half-implemented multi-level walker. A follow-up slice can
//     lift this when needed.
//   - Directories: laid out in DIRBLKSIZ (512-byte) chunks within
//     direct slots. Each chunk has self-contained DIRENT2 records; no
//     entry crosses a chunk boundary. Growing past 12 × bsize hits
//     the same `Unsupported` gate.
//   - sync_metadata is a no-op: every primitive write is immediate
//     (CG bitmaps + cs counters + inode + frag data flush as they
//     happen). The trait still requires the method so callers don't
//     need to special-case UFS.

/// UFS directory record d_type values (`sys/ufs/ufs/dirent.h`).
const DT_DIR: u8 = 4;
const DT_REG: u8 = 8;
const DT_LNK: u8 = 10;

/// Directory record alignment + minimum-record gate. Matches
/// FreeBSD `DIRSIZ(fmt, dp)` rounding (`(8 + namlen + 1 + 3) & ~3`).
const DIRENT_ALIGN: usize = 4;

/// On-disk directory chunk size (`DIRBLKSIZ` = `DEV_BSIZE` in UFS).
/// Directory entries never cross a 512-byte boundary.
const DIRBLKSIZ: usize = 512;

/// Compute the minimum d_reclen needed to hold a directory entry with
/// the given name length. Header (8) + name + NUL pad up to 4-byte
/// alignment.
fn dirent_record_size(namlen: usize) -> usize {
    (DIRENT_HDR_LEN + namlen + 1 + (DIRENT_ALIGN - 1)) & !(DIRENT_ALIGN - 1)
}

/// Filename validation: non-empty, no embedded NUL, no `/`, length
/// within MAXNAMLEN (255 per FreeBSD `sys/ufs/ufs/dir.h`).
fn validate_name(name: &[u8]) -> Result<(), FilesystemError> {
    if name.is_empty() {
        return Err(FilesystemError::InvalidData("empty filename".into()));
    }
    if name.len() > 255 {
        return Err(FilesystemError::InvalidData(format!(
            "filename too long: {} bytes (max 255)",
            name.len()
        )));
    }
    if name.iter().any(|&b| b == 0 || b == b'/') {
        return Err(FilesystemError::InvalidData(
            "filename contains NUL or '/' byte".into(),
        ));
    }
    Ok(())
}

/// d_type for a file mode (used when stamping the directory entry).
fn mode_to_dirent_type(mode: u32) -> u8 {
    match unix_file_type(mode) {
        UnixFileType::Directory => DT_DIR,
        UnixFileType::Symlink => DT_LNK,
        _ => DT_REG,
    }
}

impl<R: Read + Write + Seek + Send> UfsFilesystem<R> {
    /// Look up `name` in `parent`'s directory data fork. Returns the
    /// (logical-byte-offset, d_reclen) of the matching DIRENT2 record,
    /// or `None`.
    fn dir_find(
        &mut self,
        parent: &UfsInode,
        name: &[u8],
    ) -> Result<Option<(usize, usize)>, FilesystemError> {
        let bytes = self.read_dir_bytes_raw(parent)?;
        let endian = self.endian;
        let mut off = 0;
        while off + DIRENT_HDR_LEN <= bytes.len() {
            let d_ino = read_u32(&bytes, off, endian);
            let d_reclen = read_u16(&bytes, off + 4, endian) as usize;
            let d_namlen = bytes[off + 7] as usize;
            if d_reclen == 0 || !d_reclen.is_multiple_of(DIRENT_ALIGN) {
                return Err(FilesystemError::Parse(format!(
                    "ufs dir_find: corrupt d_reclen {d_reclen} at off {off}"
                )));
            }
            if d_ino != 0
                && d_namlen == name.len()
                && &bytes[off + DIRENT_HDR_LEN..off + DIRENT_HDR_LEN + d_namlen] == name
            {
                return Ok(Some((off, d_reclen)));
            }
            off += d_reclen;
        }
        Ok(None)
    }

    /// Insert `name -> inum` into `parent`'s directory data fork. Splits
    /// the trailing-slack record in an existing 512-byte chunk when one
    /// can be found; otherwise appends a new 512-byte chunk to the next
    /// direct slot (allocating fragments as needed). Caller is
    /// responsible for writing the (possibly mutated) parent inode back.
    /// Refuses with `Unsupported` if growing past 12 × bsize.
    fn dir_insert(
        &mut self,
        parent: &mut UfsInode,
        name: &[u8],
        child_inum: u32,
        child_d_type: u8,
    ) -> Result<(), FilesystemError> {
        let bsize = self.bsize as usize;
        let frags_per_block = self.frag as u64;
        let dir_bytes = self.read_dir_bytes_raw(parent)?;
        let need = dirent_record_size(name.len());

        // Try to fit into an existing chunk by splitting the slack of
        // any record whose actual size (8 + namlen + alignment) plus
        // `need` fits within its d_reclen. We scan chunks in order so
        // an insert lands as close to the front as possible — matches
        // the kernel's `ufs_direnter` behavior.
        let endian = self.endian;
        for chunk_start in (0..dir_bytes.len()).step_by(DIRBLKSIZ) {
            let chunk_end = (chunk_start + DIRBLKSIZ).min(dir_bytes.len());
            let mut last_off: Option<usize> = None;
            let mut off = chunk_start;
            while off + DIRENT_HDR_LEN <= chunk_end {
                let d_reclen = read_u16(&dir_bytes, off + 4, endian) as usize;
                let d_namlen = dir_bytes[off + 7] as usize;
                if d_reclen == 0 || off + d_reclen > chunk_end {
                    break;
                }
                let actual = dirent_record_size(d_namlen);
                let slack = d_reclen.saturating_sub(actual);
                if slack >= need {
                    // Split: shrink this record to its actual size,
                    // place the new entry in the trailing slack.
                    let mut updated = dir_bytes.clone();
                    write_u16(&mut updated, off + 4, actual as u16, endian);
                    let new_off = off + actual;
                    write_u32(&mut updated, new_off, child_inum, endian);
                    write_u16(&mut updated, new_off + 4, slack as u16, endian);
                    updated[new_off + 6] = child_d_type;
                    updated[new_off + 7] = name.len() as u8;
                    let name_end = new_off + DIRENT_HDR_LEN + name.len();
                    updated[new_off + DIRENT_HDR_LEN..name_end].copy_from_slice(name);
                    // Pad to record length with zeros.
                    for b in &mut updated[name_end..new_off + slack] {
                        *b = 0;
                    }
                    self.write_dir_bytes(parent, &updated)?;
                    return Ok(());
                }
                last_off = Some(off);
                off += d_reclen;
            }
            let _ = last_off;
        }

        // No fit in any existing chunk — append a new DIRBLKSIZ chunk
        // at the current end of the dir. Grow within the current dir
        // block if there's still room; else allocate a new direct block.
        let cur_size = dir_bytes.len();
        if cur_size + DIRBLKSIZ > 12 * bsize {
            return Err(FilesystemError::Unsupported(format!(
                "ufs dir_insert: directory would exceed v1 cap of {} bytes (12 × bsize); \
                 indirect-block dir growth not yet implemented",
                12 * bsize
            )));
        }
        let new_size = cur_size + DIRBLKSIZ;

        // Build the new chunk holding just the new entry, padded out to
        // DIRBLKSIZ via a single trailing-slack record.
        let mut new_chunk = vec![0u8; DIRBLKSIZ];
        write_u32(&mut new_chunk, 0, child_inum, endian);
        write_u16(&mut new_chunk, 4, DIRBLKSIZ as u16, endian);
        new_chunk[6] = child_d_type;
        new_chunk[7] = name.len() as u8;
        new_chunk[DIRENT_HDR_LEN..DIRENT_HDR_LEN + name.len()].copy_from_slice(name);

        // Does cur_size already end on a bsize boundary? If yes, we need
        // a fresh direct block. If no, we extend the current block (the
        // bsize block at direct[cur_size / bsize - 1]).
        if cur_size % bsize == 0 {
            // Need a new direct slot.
            let slot = cur_size / bsize;
            if slot >= UFS_NDADDR {
                return Err(FilesystemError::Unsupported(format!(
                    "ufs dir_insert: directory exhausted all {UFS_NDADDR} direct slots"
                )));
            }
            // Allocate one full block (`frag` fragments).
            let cg_hint = (parent.inum / self.ipg) % self.ncg;
            let start_frag = self.alloc_frag_run(frags_per_block, cg_hint)?;
            parent.direct[slot] = start_frag;
            // Write the new chunk at the new block start, zero-pad the
            // rest of the bsize block.
            let mut full_block = vec![0u8; bsize];
            full_block[..DIRBLKSIZ].copy_from_slice(&new_chunk);
            self.write_frag_run(start_frag, &full_block)?;
        } else {
            // Append to the current trailing partial block. Read it,
            // splice the new chunk in, write back. The block's
            // remaining region (cur_size % bsize .. cur_size % bsize +
            // DIRBLKSIZ) gets the new chunk.
            let last_block_lbn = (cur_size - 1) / bsize;
            let last_frag = parent.direct[last_block_lbn];
            // Read the existing block.
            let byte = self.partition_offset + last_frag * self.fsize;
            self.reader.seek(SeekFrom::Start(byte))?;
            let mut block = vec![0u8; bsize];
            self.reader.read_exact(&mut block)?;
            let in_block = cur_size % bsize;
            block[in_block..in_block + DIRBLKSIZ].copy_from_slice(&new_chunk);
            self.write_frag_run(last_frag, &block)?;
        }
        parent.size = new_size as u64;
        Ok(())
    }

    /// Remove the dirent for `name` from `parent`. Returns the inum it
    /// referenced. The slot is merged into the previous record (i.e. the
    /// previous record's d_reclen grows to absorb it); if the removed
    /// entry is the first in its chunk, its `d_ino` is set to 0 to mark
    /// the slot vacant while preserving the chunk-traversal invariant.
    fn dir_remove(&mut self, parent: &UfsInode, name: &[u8]) -> Result<u32, FilesystemError> {
        let mut dir_bytes = self.read_dir_bytes_raw(parent)?;
        let endian = self.endian;
        // Find the target and the previous record within its chunk.
        let mut found_off: Option<usize> = None;
        let mut found_reclen: usize = 0;
        let mut prev_off: Option<usize> = None;
        for chunk_start in (0..dir_bytes.len()).step_by(DIRBLKSIZ) {
            let chunk_end = (chunk_start + DIRBLKSIZ).min(dir_bytes.len());
            let mut off = chunk_start;
            let mut prev_in_chunk: Option<usize> = None;
            while off + DIRENT_HDR_LEN <= chunk_end {
                let d_ino = read_u32(&dir_bytes, off, endian);
                let d_reclen = read_u16(&dir_bytes, off + 4, endian) as usize;
                let d_namlen = dir_bytes[off + 7] as usize;
                if d_reclen == 0 || off + d_reclen > chunk_end {
                    break;
                }
                if d_ino != 0
                    && d_namlen == name.len()
                    && &dir_bytes[off + DIRENT_HDR_LEN..off + DIRENT_HDR_LEN + d_namlen] == name
                {
                    found_off = Some(off);
                    found_reclen = d_reclen;
                    prev_off = prev_in_chunk;
                    break;
                }
                prev_in_chunk = Some(off);
                off += d_reclen;
            }
            if found_off.is_some() {
                break;
            }
        }
        let off = found_off
            .ok_or_else(|| FilesystemError::NotFound(String::from_utf8_lossy(name).into_owned()))?;
        let removed_inum = read_u32(&dir_bytes, off, endian);

        match prev_off {
            Some(prev) => {
                // Merge into previous record's d_reclen.
                let prev_reclen = read_u16(&dir_bytes, prev + 4, endian) as usize;
                write_u16(
                    &mut dir_bytes,
                    prev + 4,
                    (prev_reclen + found_reclen) as u16,
                    endian,
                );
            }
            None => {
                // First entry in its chunk — zero the d_ino slot. The
                // chunk's structural invariant (sum of d_reclen ==
                // DIRBLKSIZ) is preserved.
                write_u32(&mut dir_bytes, off, 0, endian);
            }
        }
        self.write_dir_bytes(parent, &dir_bytes)?;
        Ok(removed_inum)
    }

    /// Re-write the parent's directory data fork in place. The data fork
    /// length stays at `parent.size`; only contents change.
    fn write_dir_bytes(&mut self, parent: &UfsInode, bytes: &[u8]) -> Result<(), FilesystemError> {
        if bytes.len() as u64 != parent.size {
            return Err(FilesystemError::InvalidData(format!(
                "ufs write_dir_bytes: caller passed {} bytes, parent.size = {}",
                bytes.len(),
                parent.size
            )));
        }
        let bs = self.bsize as usize;
        let mut written = 0usize;
        for slot in 0..UFS_NDADDR {
            if written >= bytes.len() {
                break;
            }
            let frag = parent.direct[slot];
            if frag == 0 {
                return Err(FilesystemError::Parse(format!(
                    "ufs write_dir_bytes: parent inode {} has zero direct[{slot}] mid-dir",
                    parent.inum
                )));
            }
            let take = (bytes.len() - written).min(bs);
            // Whole-block writes round up to bsize; the last partial
            // write only updates `take` bytes (preserves anything past
            // the file's logical end).
            let byte = self.partition_offset + frag * self.fsize;
            self.reader.seek(SeekFrom::Start(byte))?;
            self.reader.write_all(&bytes[written..written + take])?;
            written += take;
        }
        if written < bytes.len() {
            return Err(FilesystemError::Unsupported(
                "ufs write_dir_bytes: parent dir spills past direct[] (indirect not yet supported)"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Write a regular file's data fork. Allocates one full block per
    /// occupied direct slot (`alloc_frag_run(frag, ...)`). Refuses with
    /// `Unsupported` if the file needs more than 12 direct blocks
    /// (indirect-block writes not yet implemented).
    fn write_file_data(
        &mut self,
        inode: &mut UfsInode,
        data: &mut dyn Read,
        data_len: u64,
    ) -> Result<(), FilesystemError> {
        let bs = self.bsize;
        let frags_per_block = self.frag as u64;
        let max_direct_bytes = bs * UFS_NDADDR as u64;
        if data_len > max_direct_bytes {
            return Err(FilesystemError::Unsupported(format!(
                "ufs write_file_data: file size {data_len} > direct cap {max_direct_bytes} \
                 (indirect-block writes not yet implemented)"
            )));
        }
        let cg_hint = (inode.inum / self.ipg) % self.ncg;
        let mut written: u64 = 0;
        let mut block_buf = vec![0u8; bs as usize];
        let mut slot = 0;
        while written < data_len {
            let remaining = data_len - written;
            let chunk = (bs).min(remaining);
            // Allocate one block (frag fragments) for this slot.
            let start = self.alloc_frag_run(frags_per_block, cg_hint)?;
            inode.direct[slot] = start;
            // Read the chunk from the source into the block buffer.
            let mut filled = 0usize;
            while filled < chunk as usize {
                let n = data.read(&mut block_buf[filled..chunk as usize])?;
                if n == 0 {
                    return Err(FilesystemError::InvalidData(format!(
                        "ufs write_file_data: source ran out at {} bytes, expected {data_len}",
                        written + filled as u64
                    )));
                }
                filled += n;
            }
            // Zero the trailing bytes (when chunk < bs) so the on-disk
            // block doesn't carry stale tail content.
            for b in &mut block_buf[chunk as usize..] {
                *b = 0;
            }
            self.write_frag_run(start, &block_buf)?;
            written += chunk;
            slot += 1;
        }
        inode.size = data_len;
        Ok(())
    }

    /// Free every disk fragment an inode owns (data + indirect blocks).
    /// Re-uses the `walk_inode_blocks` traversal so the indirect-block
    /// fragments are caught alongside the leaves.
    fn free_inode_blocks(&mut self, inode: &UfsInode) -> Result<(), FilesystemError> {
        let (data_frags, indirect_frags) = self.walk_inode_blocks(inode)?;
        // Group consecutive frags per CG so each free_frag_run call sees
        // a contiguous in-CG slice. Simplest: one frag at a time. With
        // v1's direct-only file layout each direct[i] is a `frag`-long
        // block, so we can collapse into runs trivially when neighbors
        // line up; the dumb path is correct and slow.
        for f in data_frags.into_iter().chain(indirect_frags) {
            if f == 0 {
                continue;
            }
            // For now, free one block (frag fragments) at a time — every
            // direct slot is exactly one block by construction in
            // write_file_data.
            let frags_per_block = self.frag as u64;
            // Guard against runs that cross CG boundaries (won't happen
            // for direct allocations from alloc_frag_run, which refuses
            // them, but defensive coding for future indirect support).
            let cg = f / self.fpg as u64;
            let in_cg = f % self.fpg as u64;
            let span = frags_per_block.min(self.fpg as u64 - in_cg);
            self.free_frag_run(f, span)?;
            let _ = cg;
        }
        Ok(())
    }
}

impl<R: Read + Write + Seek + Send> super::filesystem::EditableFilesystem for UfsFilesystem<R> {
    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn Read,
        data_len: u64,
        options: &super::filesystem::CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        if !parent.is_directory() {
            return Err(FilesystemError::NotADirectory(parent.path.clone()));
        }
        let name_bytes = name.as_bytes();
        validate_name(name_bytes)?;
        let parent_inum = parent.location as u32;

        let parent_inode = self.read_inode(parent_inum)?;
        if self.dir_find(&parent_inode, name_bytes)?.is_some() {
            return Err(FilesystemError::AlreadyExists(name.into()));
        }

        // Allocate the new inode first; if anything below fails we free
        // it back. Inum allocation is sticky across the create.
        let new_inum = self.alloc_inode(parent_inum / self.ipg)?;
        let mode = options.mode.unwrap_or(0o100644);
        let mut new_inode = UfsInode {
            inum: new_inum,
            mode,
            nlink: 1,
            uid: options.uid.unwrap_or(0),
            gid: options.gid.unwrap_or(0),
            size: 0,
            mtime: 0,
            direct: [0; UFS_NDADDR],
            indirect: [0; UFS_NIADDR],
            inline_payload: Vec::new(),
        };
        let create_result = (|| -> Result<FileEntry, FilesystemError> {
            self.write_file_data(&mut new_inode, data, data_len)?;
            self.write_inode(new_inum, &new_inode)?;

            let mut parent_inode = self.read_inode(parent_inum)?;
            self.dir_insert(
                &mut parent_inode,
                name_bytes,
                new_inum,
                mode_to_dirent_type(mode),
            )?;
            self.write_inode(parent_inum, &parent_inode)?;
            // Build the resulting FileEntry.
            let parent_path = if parent.path == "/" {
                String::new()
            } else {
                parent.path.clone()
            };
            let mut entry = FileEntry::new_file(
                name.to_string(),
                format!("{parent_path}/{name}"),
                new_inode.size,
                new_inum as u64,
            );
            entry.mode = Some(new_inode.mode);
            entry.uid = Some(new_inode.uid);
            entry.gid = Some(new_inode.gid);
            Ok(entry)
        })();

        if create_result.is_err() {
            // Roll back: free any allocated data blocks, free the inode.
            let _ = self.free_inode_blocks(&new_inode);
            let _ = self.free_inode(new_inum);
        }
        create_result
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        options: &super::filesystem::CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        if !parent.is_directory() {
            return Err(FilesystemError::NotADirectory(parent.path.clone()));
        }
        let name_bytes = name.as_bytes();
        validate_name(name_bytes)?;
        let parent_inum = parent.location as u32;

        let parent_inode_check = self.read_inode(parent_inum)?;
        if self.dir_find(&parent_inode_check, name_bytes)?.is_some() {
            return Err(FilesystemError::AlreadyExists(name.into()));
        }

        let bsize = self.bsize as usize;
        let frags_per_block = self.frag as u64;
        let new_inum = self.alloc_inode(parent_inum / self.ipg)?;

        let create_result = (|| -> Result<FileEntry, FilesystemError> {
            // Allocate one block for the new dir's initial DIRBLKSIZ
            // chunk holding `.` and `..`. The remaining bsize - DIRBLKSIZ
            // bytes are zero-padded — they'll be reused when files are
            // added via the same chunk-append logic in dir_insert.
            let cg_hint = parent_inum / self.ipg;
            let start_frag = self.alloc_frag_run(frags_per_block, cg_hint)?;
            let endian = self.endian;
            let mut block = vec![0u8; bsize];

            // `.` record: 12 bytes (8 hdr + 1 name + 3 pad).
            let dot_reclen = dirent_record_size(1);
            write_u32(&mut block, 0, new_inum, endian);
            write_u16(&mut block, 4, dot_reclen as u16, endian);
            block[6] = DT_DIR;
            block[7] = 1;
            block[DIRENT_HDR_LEN] = b'.';

            // `..` record: takes the rest of the first DIRBLKSIZ chunk.
            let dotdot_off = dot_reclen;
            let dotdot_reclen = DIRBLKSIZ - dot_reclen;
            write_u32(&mut block, dotdot_off, parent_inum, endian);
            write_u16(&mut block, dotdot_off + 4, dotdot_reclen as u16, endian);
            block[dotdot_off + 6] = DT_DIR;
            block[dotdot_off + 7] = 2;
            block[dotdot_off + DIRENT_HDR_LEN] = b'.';
            block[dotdot_off + DIRENT_HDR_LEN + 1] = b'.';

            self.write_frag_run(start_frag, &block)?;

            let mode = options.mode.unwrap_or(0o040755);
            let mut new_dir = UfsInode {
                inum: new_inum,
                mode,
                nlink: 2, // `.` is the second link
                uid: options.uid.unwrap_or(0),
                gid: options.gid.unwrap_or(0),
                size: DIRBLKSIZ as u64,
                mtime: 0,
                direct: [0; UFS_NDADDR],
                indirect: [0; UFS_NIADDR],
                inline_payload: Vec::new(),
            };
            new_dir.direct[0] = start_frag;
            self.write_inode(new_inum, &new_dir)?;
            // Tracking: the new directory bumps the per-CG ndir counter.
            self.update_cg_cs(new_inum / self.ipg, 1, 0, 0, 0)?;

            // Link into parent + bump parent's nlink for the back-link.
            let mut parent_inode = self.read_inode(parent_inum)?;
            self.dir_insert(&mut parent_inode, name_bytes, new_inum, DT_DIR)?;
            parent_inode.nlink = parent_inode.nlink.saturating_add(1);
            self.write_inode(parent_inum, &parent_inode)?;

            let parent_path = if parent.path == "/" {
                String::new()
            } else {
                parent.path.clone()
            };
            let mut entry = FileEntry::new_directory(
                name.to_string(),
                format!("{parent_path}/{name}"),
                new_inum as u64,
            );
            entry.mode = Some(new_dir.mode);
            entry.uid = Some(new_dir.uid);
            entry.gid = Some(new_dir.gid);
            Ok(entry)
        })();

        if create_result.is_err() {
            let _ = self.free_inode(new_inum);
        }
        create_result
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        if !parent.is_directory() {
            return Err(FilesystemError::NotADirectory(parent.path.clone()));
        }
        let parent_inum = parent.location as u32;
        let entry_inum = entry.location as u32;
        if entry_inum < ROOT_INODE {
            return Err(FilesystemError::InvalidData(format!(
                "ufs delete_entry: refusing to delete reserved inum {entry_inum}"
            )));
        }
        let target = self.read_inode(entry_inum)?;
        let is_dir = matches!(unix_file_type(target.mode), UnixFileType::Directory);
        if is_dir {
            // Empty-check: scan every chunk and reject if any DIRENT2
            // outside `.` / `..` references a non-zero inum.
            let bytes = self.read_dir_bytes_raw(&target)?;
            let endian = self.endian;
            let mut off = 0;
            while off + DIRENT_HDR_LEN <= bytes.len() {
                let d_ino = read_u32(&bytes, off, endian);
                let d_reclen = read_u16(&bytes, off + 4, endian) as usize;
                let d_namlen = bytes[off + 7] as usize;
                if d_reclen == 0 || off + d_reclen > bytes.len() {
                    break;
                }
                if d_ino != 0 && d_namlen > 0 {
                    let name = &bytes[off + DIRENT_HDR_LEN..off + DIRENT_HDR_LEN + d_namlen];
                    if name != b"." && name != b".." {
                        return Err(FilesystemError::InvalidData(format!(
                            "ufs delete_entry: directory '{}' not empty",
                            entry.path
                        )));
                    }
                }
                off += d_reclen;
            }
        }

        // Unlink from parent first (a crash after this point leaves an
        // orphan inode, recoverable; a crash before leaves a dangling
        // dirent, NOT recoverable cleanly).
        let parent_inode = self.read_inode(parent_inum)?;
        let removed = self.dir_remove(&parent_inode, entry.name.as_bytes())?;
        if removed != entry_inum {
            return Err(FilesystemError::InvalidData(format!(
                "ufs delete_entry: dirent inum {removed} differs from entry inum {entry_inum}"
            )));
        }

        // Free the inode's data blocks, then the inode itself.
        self.free_inode_blocks(&target)?;
        self.free_inode(entry_inum)?;
        if is_dir {
            // Account for the directory-count drop on the CG.
            self.update_cg_cs(entry_inum / self.ipg, -1, 0, 0, 0)?;
            // Parent loses its "back-link" from the deleted dir's `..`.
            let mut parent_inode = self.read_inode(parent_inum)?;
            parent_inode.nlink = parent_inode.nlink.saturating_sub(1);
            self.write_inode(parent_inum, &parent_inode)?;
        }
        Ok(())
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        // Every primitive write flushes synchronously — bitmaps, cs
        // counters, inodes, and frag data all land at the moment they're
        // mutated. sync_metadata is a no-op for callers that want to
        // batch (the trait contract allows it). When buffered SB-level
        // summary (`fs_old_cstotal` / `fs_cstotal`) propagation lands in
        // a follow-up, it'll flush here.
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        // Sum nffree across every CG and multiply by fsize. Modest cost
        // (one CG header read per CG) and correct under arbitrary edits
        // because each primitive keeps `cg_cs` in sync.
        let mut total: u64 = 0;
        for cg in 0..self.ncg {
            let (_, _, _, nffree) = self.read_cg_cs(cg)?;
            total = total.saturating_add(nffree.max(0) as u64);
        }
        Ok(total * self.fsize)
    }

    fn repair(&mut self) -> Result<super::fsck::RepairReport, FilesystemError> {
        repair_ufs(self)
    }
}

/// UFS repair driver. Re-runs the verifier and fixes the three repairable
/// findings:
///   * `ReplicaSb*Mismatch`  → rewrite the affected CG's replica slot from
///     the primary superblock byte image.
///   * `BitmapMissingAllocation` → clear the stray "free" bit (UFS bitmap
///     polarity is set=FREE, so clearing the bit marks it allocated to
///     match the inode that claims it) and bump `cg_cs.nffree` down by one
///     per cleared bit.
///   * `OrphanInode` → adopt every unreachable inode into `/lost+found/`
///     as `ino_<inum>` with collision-safe suffix retry.
///
/// Findings the verifier flags as unrepairable (geometry damage, double
/// allocation, out-of-range pointers, indirect-read failures) are counted
/// in `unrepairable_count` and left for a future pass.
fn repair_ufs<R: Read + Write + Seek + Send>(
    fs: &mut UfsFilesystem<R>,
) -> Result<super::fsck::RepairReport, FilesystemError> {
    use std::collections::{HashMap, HashSet};

    let mut report = super::fsck::RepairReport {
        fixes_applied: Vec::new(),
        fixes_failed: Vec::new(),
        unrepairable_count: 0,
    };

    let result = super::ufs_fsck::fsck_ufs(fs)?;
    if result.errors.is_empty() {
        return Ok(report);
    }

    // ----- Replica SB fixup -----
    // Every `ReplicaSb*Mismatch` finding names its CG up front:
    // "CG {cg} replica SB ...". Collect the unique CGs and rewrite each
    // replica slot from the primary byte image in one pass — the primary
    // is the source of truth (the fsck only compares the replicas against
    // it; mismatches by definition mean the replica drifted, not the
    // primary).
    let replica_errs: Vec<&super::fsck::FsckIssue> = result
        .errors
        .iter()
        .filter(|e| e.code.starts_with("ReplicaSb") && e.code.ends_with("Mismatch"))
        .collect();
    if !replica_errs.is_empty() {
        let primary = fs.read_primary_sb_bytes()?;
        let mut cgs: HashSet<u32> = HashSet::new();
        for issue in &replica_errs {
            match parse_replica_cg_from_msg(&issue.message) {
                Some(cg) if cg > 0 => {
                    cgs.insert(cg);
                }
                Some(_) => {
                    // CG 0's "replica" overlaps the primary slot; the
                    // verifier already skips it, so this is unreachable
                    // in practice — guard anyway.
                }
                None => report.fixes_failed.push(format!(
                    "Replica SB: could not parse CG from '{}'",
                    issue.message
                )),
            }
        }
        let mut rewritten = 0u32;
        for cg in cgs {
            match fs.write_replica_sb_bytes(cg, &primary) {
                Ok(()) => rewritten += 1,
                Err(e) => report
                    .fixes_failed
                    .push(format!("Replica SB CG {cg}: rewrite failed: {e}")),
            }
        }
        if rewritten > 0 {
            report.fixes_applied.push(format!(
                "Replica superblock: rewrote {rewritten} CG replica(s) from primary \
                 ({} field-mismatch(es) corrected)",
                replica_errs.len()
            ));
        }
    }

    // ----- Bitmap missing-allocation fixup -----
    // Group fixes by CG so each CG's bitmap reads + writes happen once.
    let mut bitmap_fixes_per_cg: HashMap<u32, Vec<u64>> = HashMap::new();
    for issue in result
        .errors
        .iter()
        .filter(|e| e.code == "BitmapMissingAllocation")
    {
        match parse_fragment_number_from_msg(&issue.message) {
            Some(frag) => {
                let cg = (frag / fs.fpg as u64) as u32;
                let in_cg = frag % fs.fpg as u64;
                bitmap_fixes_per_cg.entry(cg).or_default().push(in_cg);
            }
            None => report.fixes_failed.push(format!(
                "BitmapMissingAllocation: could not parse fragment from '{}'",
                issue.message
            )),
        }
    }
    let mut bitmap_fixes = 0u32;
    for (cg, in_cgs) in bitmap_fixes_per_cg {
        let (mut bm, ndblk) = match fs.read_cg_free_bitmap(cg) {
            Ok(pair) => pair,
            Err(e) => {
                report
                    .fixes_failed
                    .push(format!("CG {cg} bitmap read failed: {e}"));
                continue;
            }
        };
        let mut cleared = 0u32;
        for in_cg in in_cgs {
            if in_cg >= ndblk {
                report.fixes_failed.push(format!(
                    "BitmapMissingAllocation: CG {cg} fragment in_cg {in_cg} past cg_ndblk {ndblk}"
                ));
                continue;
            }
            let by = (in_cg / 8) as usize;
            let bb = (in_cg % 8) as u8;
            // set bit = free; mark allocated by CLEARING the bit.
            if bm[by] & (1 << bb) != 0 {
                bm[by] &= !(1 << bb);
                cleared += 1;
            }
        }
        if cleared > 0 {
            if let Err(e) = fs.write_cg_free_bitmap(cg, &bm) {
                report
                    .fixes_failed
                    .push(format!("CG {cg} bitmap write failed: {e}"));
                continue;
            }
            // Each cleared free bit means one fewer free fragment in this CG.
            if let Err(e) = fs.update_cg_cs(cg, 0, 0, 0, -(cleared as i32)) {
                report
                    .fixes_failed
                    .push(format!("CG {cg} cs nffree update failed: {e}"));
                continue;
            }
            bitmap_fixes += cleared;
        }
    }
    if bitmap_fixes > 0 {
        report.fixes_applied.push(format!(
            "Bitmap: cleared {bitmap_fixes} stray free bit(s) for inode-claimed fragments"
        ));
    }

    // ----- Orphan adoption into lost+found -----
    if !result.orphaned_entries.is_empty() {
        match adopt_orphans_into_lost_found_ufs(fs, &result.orphaned_entries, &mut report) {
            Ok(adopted) if adopted > 0 => report.fixes_applied.push(format!(
                "Orphans: adopted {adopted} entry/entries into lost+found/"
            )),
            Ok(_) => {}
            Err(e) => report
                .fixes_failed
                .push(format!("Orphan adoption setup failed: {e}")),
        }
    }

    report.unrepairable_count = result.errors.iter().filter(|e| !e.repairable).count();
    Ok(report)
}

/// Parse `"CG {N} ..."` out of a fsck replica-mismatch issue message.
/// The verifier emits a stable format so this is a controlled-input
/// parse.
fn parse_replica_cg_from_msg(msg: &str) -> Option<u32> {
    let tail = msg.strip_prefix("CG ")?;
    let num_str = tail.split_whitespace().next()?;
    num_str.parse().ok()
}

/// Parse `"fragment {N} ..."` out of a fsck BitmapMissingAllocation
/// message. Mirrors `parse_replica_cg_from_msg` in shape.
fn parse_fragment_number_from_msg(msg: &str) -> Option<u64> {
    let tail = msg.strip_prefix("fragment ")?;
    let num_str = tail.split_whitespace().next()?;
    num_str.parse().ok()
}

/// Ensure `/lost+found` exists under the root inode and link each orphan
/// into it as `ino_<inum>` (with `_<n>` suffix retry on collision).
/// Returns the number of orphans successfully adopted; failures are
/// pushed into `report.fixes_failed`.
fn adopt_orphans_into_lost_found_ufs<R: Read + Write + Seek + Send>(
    fs: &mut UfsFilesystem<R>,
    orphans: &[super::fsck::OrphanedEntry],
    report: &mut super::fsck::RepairReport,
) -> Result<u32, FilesystemError> {
    use super::entry::{EntryType, FileEntry};
    use super::filesystem::{CreateDirectoryOptions, EditableFilesystem};

    // Find or create lost+found under root.
    let root_inode = fs.read_inode(ROOT_INODE)?;
    let lf_inum = match fs.dir_find(&root_inode, b"lost+found")? {
        Some((_off, _reclen)) => {
            // dir_find returns the slot location; re-read the dirent to
            // pull the actual child inum. Simpler: list children and find
            // by name.
            let root_entry = FileEntry {
                name: "/".into(),
                path: "/".into(),
                entry_type: EntryType::Directory,
                size: 0,
                location: ROOT_INODE as u64,
                modified: None,
                type_code: None,
                creator_code: None,
                symlink_target: None,
                special_type: None,
                mode: Some(root_inode.mode),
                uid: Some(root_inode.uid),
                gid: Some(root_inode.gid),
                resource_fork_size: None,
                aux_type: None,
                link_target_cnid: None,
                amiga_protection: None,
                amiga_comment: None,
                amiga_date: None,
            };
            let children =
                (fs as &mut dyn super::filesystem::Filesystem).list_directory(&root_entry)?;
            let lf = children
                .into_iter()
                .find(|c| c.name == "lost+found")
                .ok_or_else(|| {
                    FilesystemError::Parse(
                        "lost+found dirent found by dir_find but missing in list_directory".into(),
                    )
                })?;
            lf.location as u32
        }
        None => {
            let root_entry = FileEntry {
                name: "/".into(),
                path: "/".into(),
                entry_type: EntryType::Directory,
                size: 0,
                location: ROOT_INODE as u64,
                modified: None,
                type_code: None,
                creator_code: None,
                symlink_target: None,
                special_type: None,
                mode: Some(root_inode.mode),
                uid: Some(root_inode.uid),
                gid: Some(root_inode.gid),
                resource_fork_size: None,
                aux_type: None,
                link_target_cnid: None,
                amiga_protection: None,
                amiga_comment: None,
                amiga_date: None,
            };
            let lf = fs.create_directory(
                &root_entry,
                "lost+found",
                &CreateDirectoryOptions::default(),
            )?;
            lf.location as u32
        }
    };

    let mut adopted: u32 = 0;
    for orphan in orphans {
        let inum = orphan.id as u32;
        // Pull the orphan's mode so we stamp the correct d_type into the
        // dirent — getting this wrong only matters for tools that trust
        // d_type without re-reading the inode, but the kernel does, so
        // mirror what `mode_to_dirent_type` produces during create.
        let orphan_inode = match fs.read_inode(inum) {
            Ok(i) => i,
            Err(e) => {
                report
                    .fixes_failed
                    .push(format!("Orphan inum {inum}: could not read inode: {e}"));
                continue;
            }
        };
        if orphan_inode.mode == 0 {
            // Inode was freed between fsck and adoption — nothing to link.
            continue;
        }
        let d_type = mode_to_dirent_type(orphan_inode.mode);

        let mut lf_inode = fs.read_inode(lf_inum)?;
        let mut name = format!("ino_{inum}");
        let mut suffix = 1u32;
        while fs.dir_find(&lf_inode, name.as_bytes())?.is_some() {
            name = format!("ino_{inum}_{suffix}");
            suffix += 1;
            if suffix > 1000 {
                report.fixes_failed.push(format!(
                    "Orphan inum {inum}: could not generate unique lost+found name"
                ));
                break;
            }
        }
        if suffix > 1000 {
            continue;
        }
        match fs.dir_insert(&mut lf_inode, name.as_bytes(), inum, d_type) {
            Ok(()) => {
                fs.write_inode(lf_inum, &lf_inode)?;
                adopted += 1;
            }
            Err(e) => report.fixes_failed.push(format!(
                "Orphan inum {inum}: could not adopt into lost+found: {e}"
            )),
        }
    }
    Ok(adopted)
}

// ---- `fs_flags2` bits we care about ----

/// `FS_SUJ` (softupdate journaling enabled). Defined as `1 << 3` in
/// FreeBSD's `sys/ufs/ffs/fs.h`.
const FS_SUJ: u32 = 1 << 3;

/// `fs_clean == FS_ISCLEAN (1)` means the volume was cleanly unmounted;
/// any other value (commonly `0` = FS_ISDIRTY) means the kernel was
/// killed before a sync.
const FS_DIRTY: u8 = 0;

// ---- Byte-order-aware field readers ----

pub(crate) fn read_u16(buf: &[u8], off: usize, endian: UfsEndian) -> u16 {
    let bytes = [buf[off], buf[off + 1]];
    match endian {
        UfsEndian::Little => u16::from_le_bytes(bytes),
        UfsEndian::Big => u16::from_be_bytes(bytes),
    }
}

pub(crate) fn read_i16(buf: &[u8], off: usize, endian: UfsEndian) -> i16 {
    read_u16(buf, off, endian) as i16
}

pub(crate) fn read_u32(buf: &[u8], off: usize, endian: UfsEndian) -> u32 {
    let bytes = [buf[off], buf[off + 1], buf[off + 2], buf[off + 3]];
    match endian {
        UfsEndian::Little => u32::from_le_bytes(bytes),
        UfsEndian::Big => u32::from_be_bytes(bytes),
    }
}

pub(crate) fn read_i32(buf: &[u8], off: usize, endian: UfsEndian) -> i32 {
    read_u32(buf, off, endian) as i32
}

pub(crate) fn read_u64(buf: &[u8], off: usize, endian: UfsEndian) -> u64 {
    let bytes = [
        buf[off],
        buf[off + 1],
        buf[off + 2],
        buf[off + 3],
        buf[off + 4],
        buf[off + 5],
        buf[off + 6],
        buf[off + 7],
    ];
    match endian {
        UfsEndian::Little => u64::from_le_bytes(bytes),
        UfsEndian::Big => u64::from_be_bytes(bytes),
    }
}

fn write_u16(buf: &mut [u8], off: usize, v: u16, endian: UfsEndian) {
    let bytes = match endian {
        UfsEndian::Little => v.to_le_bytes(),
        UfsEndian::Big => v.to_be_bytes(),
    };
    buf[off..off + 2].copy_from_slice(&bytes);
}

fn write_i16(buf: &mut [u8], off: usize, v: i16, endian: UfsEndian) {
    write_u16(buf, off, v as u16, endian);
}

fn write_u32(buf: &mut [u8], off: usize, v: u32, endian: UfsEndian) {
    let bytes = match endian {
        UfsEndian::Little => v.to_le_bytes(),
        UfsEndian::Big => v.to_be_bytes(),
    };
    buf[off..off + 4].copy_from_slice(&bytes);
}

fn write_i32(buf: &mut [u8], off: usize, v: i32, endian: UfsEndian) {
    write_u32(buf, off, v as u32, endian);
}

fn write_u64(buf: &mut [u8], off: usize, v: u64, endian: UfsEndian) {
    let bytes = match endian {
        UfsEndian::Little => v.to_le_bytes(),
        UfsEndian::Big => v.to_be_bytes(),
    };
    buf[off..off + 8].copy_from_slice(&bytes);
}

fn write_i64(buf: &mut [u8], off: usize, v: i64, endian: UfsEndian) {
    write_u64(buf, off, v as u64, endian);
}

pub(crate) fn read_i64(buf: &[u8], off: usize, endian: UfsEndian) -> i64 {
    let bytes = [
        buf[off],
        buf[off + 1],
        buf[off + 2],
        buf[off + 3],
        buf[off + 4],
        buf[off + 5],
        buf[off + 6],
        buf[off + 7],
    ];
    let v = match endian {
        UfsEndian::Little => u64::from_le_bytes(bytes),
        UfsEndian::Big => u64::from_be_bytes(bytes),
    };
    v as i64
}

fn parse_label(bytes: &[u8]) -> Option<String> {
    // Stop at the first NUL; trim trailing ASCII whitespace; reject the
    // empty case (matches reiserfs.rs's label parsing exactly).
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

// ---- Filesystem trait impl (Tier-A scope) ----

impl<R: Read + Seek + Send> Filesystem for UfsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        let mut entry = FileEntry::root();
        entry.location = ROOT_INODE as u64;
        let inode = self.read_inode(ROOT_INODE)?;
        entry.mode = Some(inode.mode);
        entry.uid = Some(inode.uid);
        entry.gid = Some(inode.gid);
        if inode.mtime != 0 {
            entry.modified = Some(format_unix_timestamp(inode.mtime));
        }
        Ok(entry)
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        let inum = entry.location as u32;
        let inode = self.read_inode(inum)?;
        if !matches!(unix_file_type(inode.mode), UnixFileType::Directory) {
            return Err(FilesystemError::Parse(format!(
                "ufs: inode {inum} is not a directory (mode = 0o{:o})",
                inode.mode
            )));
        }
        if inode.size as usize > MAX_DIR_BYTES {
            return Err(FilesystemError::Parse(format!(
                "ufs: directory inode {inum} claims size {} bytes (cap {MAX_DIR_BYTES})",
                inode.size
            )));
        }
        let dir_bytes = self.read_inode_data(&inode, inode.size, inode.size as usize)?;
        let endian = self.endian;
        let mut children: Vec<FileEntry> = Vec::new();
        let mut off = 0usize;
        while off + DIRENT_HDR_LEN <= dir_bytes.len() {
            let d_ino = read_u32(&dir_bytes, off, endian);
            let d_reclen = read_u16(&dir_bytes, off + 4, endian) as usize;
            let _d_type = dir_bytes[off + 6];
            let d_namlen = dir_bytes[off + 7] as usize;

            if d_reclen == 0 {
                return Err(FilesystemError::Parse(format!(
                    "ufs: directory {inum} has zero d_reclen at offset {off}"
                )));
            }
            if !d_reclen.is_multiple_of(4) {
                return Err(FilesystemError::Parse(format!(
                    "ufs: directory {inum} d_reclen {d_reclen} at offset {off} not 4-byte aligned"
                )));
            }
            if d_reclen < DIRENT_HDR_LEN || off + d_reclen > dir_bytes.len() {
                return Err(FilesystemError::Parse(format!(
                    "ufs: directory {inum} d_reclen {d_reclen} at offset {off} out of bounds (dir size = {})",
                    dir_bytes.len()
                )));
            }
            if d_namlen > d_reclen - DIRENT_HDR_LEN {
                return Err(FilesystemError::Parse(format!(
                    "ufs: directory {inum} d_namlen {d_namlen} at offset {off} exceeds slot ({d_reclen} - {DIRENT_HDR_LEN})"
                )));
            }

            if d_ino != 0 && d_namlen > 0 {
                let name_bytes = &dir_bytes[off + DIRENT_HDR_LEN..off + DIRENT_HDR_LEN + d_namlen];
                let name = String::from_utf8_lossy(name_bytes).into_owned();
                if name != "." && name != ".." {
                    let child_inode = self.read_inode(d_ino)?;
                    let child = self.build_file_entry(&name, entry, &child_inode)?;
                    children.push(child);
                }
            }
            off += d_reclen;
        }
        Ok(children)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let inum = entry.location as u32;
        let inode = self.read_inode(inum)?;
        match unix_file_type(inode.mode) {
            UnixFileType::Regular | UnixFileType::Unknown => {}
            UnixFileType::Symlink => {
                // Reading a symlink returns its target bytes — same
                // convention every other Unix-y FS in this codebase uses.
                let target = self.read_symlink_target(&inode)?;
                let bytes = target.into_bytes();
                let take = bytes.len().min(max_bytes);
                return Ok(bytes[..take].to_vec());
            }
            UnixFileType::Directory => {
                return Err(FilesystemError::Parse(format!(
                    "ufs: read_file on directory inode {inum}"
                )));
            }
            UnixFileType::BlockDevice
            | UnixFileType::CharDevice
            | UnixFileType::Fifo
            | UnixFileType::Socket => {
                return Err(FilesystemError::Unsupported(format!(
                    "ufs: cannot read special file inode {inum} (mode 0o{:o})",
                    inode.mode
                )));
            }
        }
        self.read_inode_data(&inode, inode.size, max_bytes)
    }

    fn volume_label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    fn fs_type(&self) -> &str {
        self.version.name()
    }

    fn total_size(&self) -> u64 {
        self.total_frags * self.fsize
    }

    fn used_size(&self) -> u64 {
        // Tier A places no upper bound on used space; the cheap "total
        // minus free-summary" read from `fs_old_cstotal` / `fs_cstotal`
        // is a follow-up under U.2 (alongside the bitmap walk that
        // drives `last_data_byte`).
        self.total_size()
    }

    fn fsck(&mut self) -> Option<Result<super::fsck::FsckResult, FilesystemError>> {
        Some(super::ufs_fsck::fsck_ufs(self))
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        // Walk every CG bitmap from the last index back to 0 and find the
        // highest CLEAR bit (= last allocated fragment) anywhere on the
        // volume. UFS bitmaps use **set bit = FREE** (BSD convention,
        // opposite of ext / ReiserFS); a clear bit means "this fragment
        // holds live data — the backup pipeline must capture it."
        for i in (0..self.ncg).rev() {
            let (bitmap, ndblk) = self.read_cg_free_bitmap(i)?;
            let bm = BitmapReader::new(&bitmap, ndblk);
            if let Some(top) = bm.highest_clear_bit() {
                let absolute_frag = self.cgbase_frag(i) + top;
                return Ok((absolute_frag + 1) * self.fsize);
            }
        }
        // No allocated fragments at all — the volume's bitmaps say
        // everything is free. Return total_size as the safe upper bound.
        Ok(self.total_size())
    }
}

// ---- Compact reader ----

/// A streaming `Read` that produces a compacted UFS partition image.
///
/// **Layout-preserving** — allocated fragments are read from their original
/// byte positions; free fragments emit zeros. UFS uses
/// **set bit = free** in the per-CG `cg_freeoff` bitmap, so we walk every
/// CG, group consecutive same-state bits into runs, and emit each run as
/// either a [`CompactSection::MappedBlocks`] (allocated) or
/// [`CompactSection::Zeros`] (free) section.
///
/// Per-CG metadata (CG header, SB copy, inode table) is **also** covered
/// by the bitmap and shows up as allocated — by design, since the backup
/// pipeline needs those bytes to faithfully reconstruct the volume.
///
/// The trailing reserved region between the last CG's end and
/// `total_frags` (sometimes present when `ncg * fpg < total_frags`) is
/// emitted as zeros so the stream length still equals `original_size`.
pub struct CompactUfsReader<R: Read + Seek> {
    inner: CompactStreamReader<R>,
}

impl<R: Read + Seek + Send> CompactUfsReader<R> {
    /// Create a new compacted UFS reader. Parses the SB, walks every CG
    /// bitmap, and builds the section list.
    pub fn new(reader: R, partition_offset: u64) -> Result<(Self, CompactResult), FilesystemError> {
        // Re-open the volume so we share SB validation + endian detection
        // with the read path.
        let mut fs = UfsFilesystem::open(reader, partition_offset)?;
        let fsize = fs.fsize;
        let total_frags = fs.total_frags;

        let mut sections: Vec<CompactSection> = Vec::new();
        let mut total_allocated: u64 = 0;
        let mut covered_frags: u64 = 0;

        for i in 0..fs.ncg {
            let cgbase = fs.cgbase_frag(i);
            // If the previous CG didn't end exactly where this one begins
            // (degenerate layouts), zero-pad the gap so absolute fragment
            // offsets in subsequent sections still match the source. The
            // `covered_frags` book-keeping is refreshed at the end of the
            // loop body so we only need the section emission here.
            if covered_frags < cgbase {
                sections.push(CompactSection::Zeros((cgbase - covered_frags) * fsize));
            }

            let (bitmap, ndblk) = fs.read_cg_free_bitmap(i)?;
            let bm = BitmapReader::new(&bitmap, ndblk);

            // Coalesce same-state runs into single sections.
            let mut bit: u64 = 0;
            while bit < ndblk {
                let is_set = bm.is_bit_set(bit);
                let mut run_end = bit + 1;
                while run_end < ndblk && bm.is_bit_set(run_end) == is_set {
                    run_end += 1;
                }
                let run_len = run_end - bit;

                if is_set {
                    // SET = free in UFS — emit zeros.
                    sections.push(CompactSection::Zeros(run_len * fsize));
                } else {
                    // CLEAR = allocated — map the actual fragments.
                    let old_blocks: Vec<u64> = (bit..run_end).map(|b| cgbase + b).collect();
                    total_allocated += run_len;
                    sections.push(CompactSection::MappedBlocks { old_blocks });
                }
                bit = run_end;
            }
            covered_frags = cgbase + ndblk;
        }

        // Trailing zero-pad. Some UFS layouts have `ncg * fpg < total_frags`
        // when the last CG was truncated; the residual area is never
        // allocated to user data, so emitting zeros matches the source.
        if covered_frags < total_frags {
            sections.push(CompactSection::Zeros((total_frags - covered_frags) * fsize));
        }

        let original_size = total_frags * fsize;
        let layout = CompactLayout {
            sections,
            block_size: fsize as usize,
            source_data_start: 0,
            source_partition_offset: partition_offset,
        };
        let inner = CompactStreamReader::new(fs.reader, layout);
        Ok((
            Self { inner },
            CompactResult {
                original_size,
                // Layout-preserving — stream length matches the source.
                compacted_size: original_size,
                data_size: total_allocated * fsize,
                // CompactResult::clusters_used is u32 for parity with FAT-
                // family bookkeeping; UFS fragment counts beyond u32 only
                // happen on multi-TiB volumes we don't expect here. Cap
                // at u32::MAX defensively.
                clusters_used: total_allocated.min(u32::MAX as u64) as u32,
            },
        ))
    }
}

impl<R: Read + Seek> Read for CompactUfsReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

// ---- Tests ----

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::entry::EntryType;
    use crate::fs::filesystem::{EditableFilesystem, Filesystem};
    use std::io::Cursor;

    // ---- Synthetic superblock construction ----

    /// Build a minimal in-memory UFS superblock image suitable for the
    /// parser. `partition_size_bytes` controls how large the surrounding
    /// volume claims to be (must be > sb_offset + SB_READ_SIZE).
    ///
    /// Fields written: magic, geometry (bsize/fsize/frag/ncg/fpg/ipg),
    /// total fragments (per version), volname, fs_clean=1, fs_flags2=0.
    /// Every other byte is zero — a real `newfs` image carries far more
    /// metadata, but Tier A doesn't depend on any of it.
    #[allow(clippy::too_many_arguments)] // every parameter is an independent
                                         // dimension we exercise across tests
    fn build_sb(
        version: UfsVersion,
        endian: UfsEndian,
        sb_offset: u64,
        bsize: u32,
        fsize: u32,
        ncg: u32,
        fpg: u32,
        ipg: u32,
        total_frags: u64,
        label: &[u8],
        fs_clean: u8,
        fs_flags2: u32,
    ) -> Vec<u8> {
        let needed = (sb_offset as usize) + SB_READ_SIZE;
        let mut img = vec![0u8; needed.max(8 * 1024 * 1024)];
        let sb = &mut img[sb_offset as usize..];

        let write_u32 = |sb: &mut [u8], off: usize, v: u32| match endian {
            UfsEndian::Little => sb[off..off + 4].copy_from_slice(&v.to_le_bytes()),
            UfsEndian::Big => sb[off..off + 4].copy_from_slice(&v.to_be_bytes()),
        };
        let write_i32 = |sb: &mut [u8], off: usize, v: i32| write_u32(sb, off, v as u32);
        let write_i64 = |sb: &mut [u8], off: usize, v: i64| match endian {
            UfsEndian::Little => sb[off..off + 8].copy_from_slice(&v.to_le_bytes()),
            UfsEndian::Big => sb[off..off + 8].copy_from_slice(&v.to_be_bytes()),
        };

        write_i32(sb, OFF_BSIZE, bsize as i32);
        write_i32(sb, OFF_FSIZE, fsize as i32);
        write_i32(sb, OFF_FRAG, (bsize / fsize) as i32);
        write_u32(sb, OFF_NCG, ncg);
        write_i32(sb, OFF_FPG, fpg as i32);
        write_u32(sb, OFF_IPG, ipg);
        write_u32(sb, OFF_FLAGS2, fs_flags2);
        // `fs_cblkno` + `fs_iblkno` — fragment offsets within each CG.
        // 24 / 32 are the makefs defaults for the small fixtures we
        // exercise; tests that walk CG headers seed the bitmap region
        // themselves. Inode-table walk tests use the same defaults so
        // the inode region lives at a known fragment.
        write_i32(sb, OFF_CBLKNO, 24);
        write_i32(sb, OFF_IBLKNO, 32);
        // `fs_maxsymlinklen` — version-default inline-symlink cutoff.
        let msl = if matches!(version, UfsVersion::Ufs1) {
            60
        } else {
            120
        };
        write_i32(sb, OFF_MAXSYMLINKLEN, msl);

        match version {
            UfsVersion::Ufs1 => {
                write_i32(sb, OFF_OLD_SIZE, total_frags as i32);
                write_u32(sb, MAGIC_OFF, MAGIC_UFS1);
            }
            UfsVersion::Ufs2 => {
                write_i64(sb, OFF_SIZE_UFS2, total_frags as i64);
                write_u32(sb, MAGIC_OFF, MAGIC_UFS2);
            }
        }

        // fs_clean is a single byte at superblock offset 213 in both versions.
        sb[213] = fs_clean;

        // Volume name.
        let take = label.len().min(VOLNAME_LEN);
        sb[OFF_VOLNAME..OFF_VOLNAME + take].copy_from_slice(&label[..take]);

        img
    }

    fn build_default_sb(version: UfsVersion, endian: UfsEndian, sb_offset: u64) -> Vec<u8> {
        build_sb(
            version, endian, sb_offset, 8192, 1024, 1, 16384, 64, 16384, b"", 1, 0,
        )
    }

    // ---- Open-time detection ----

    #[test]
    fn open_ufs1_at_8192_le() {
        let img = build_default_sb(UfsVersion::Ufs1, UfsEndian::Little, SB_OFFSET_UFS1);
        let fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        assert_eq!(fs.version(), UfsVersion::Ufs1);
        assert_eq!(fs.endian(), UfsEndian::Little);
        assert_eq!(fs.superblock_offset(), SB_OFFSET_UFS1);
        assert_eq!(fs.fs_type(), "UFS1");
    }

    #[test]
    fn open_ufs2_at_8192_le_makefs_layout() {
        // makefs default for small images: UFS2 with SB at byte 8192.
        let img = build_default_sb(UfsVersion::Ufs2, UfsEndian::Little, SB_OFFSET_UFS1);
        let fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        assert_eq!(fs.version(), UfsVersion::Ufs2);
        assert_eq!(fs.superblock_offset(), SB_OFFSET_UFS1);
        assert_eq!(fs.fs_type(), "UFS2");
    }

    #[test]
    fn open_ufs2_at_65536_le_newfs_layout() {
        // FreeBSD newfs default for non-tiny images: UFS2 at byte 65536.
        let img = build_default_sb(UfsVersion::Ufs2, UfsEndian::Little, SB_OFFSET_UFS2);
        let fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        assert_eq!(fs.version(), UfsVersion::Ufs2);
        assert_eq!(fs.superblock_offset(), SB_OFFSET_UFS2);
    }

    #[test]
    fn open_prefers_ufs2_at_65536_when_both_offsets_have_magic() {
        // A converted volume could carry a stale UFS1 SB at 8192 plus a
        // fresh UFS2 SB at 65536. Match the kernel's SBLOCKSEARCH order:
        // 65536 first.
        let mut img = build_default_sb(UfsVersion::Ufs1, UfsEndian::Little, SB_OFFSET_UFS1);
        let ufs2_img = build_default_sb(UfsVersion::Ufs2, UfsEndian::Little, SB_OFFSET_UFS2);
        if img.len() < ufs2_img.len() {
            img.resize(ufs2_img.len(), 0);
        }
        img[SB_OFFSET_UFS2 as usize..SB_OFFSET_UFS2 as usize + SB_READ_SIZE].copy_from_slice(
            &ufs2_img[SB_OFFSET_UFS2 as usize..SB_OFFSET_UFS2 as usize + SB_READ_SIZE],
        );
        let fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        assert_eq!(fs.version(), UfsVersion::Ufs2);
        assert_eq!(fs.superblock_offset(), SB_OFFSET_UFS2);
    }

    #[test]
    fn open_ufs1_big_endian() {
        let img = build_default_sb(UfsVersion::Ufs1, UfsEndian::Big, SB_OFFSET_UFS1);
        let fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open BE");
        assert_eq!(fs.endian(), UfsEndian::Big);
        assert_eq!(fs.version(), UfsVersion::Ufs1);
    }

    #[test]
    fn open_rejects_image_with_no_magic() {
        let img = vec![0u8; 256 * 1024];
        let err = UfsFilesystem::open(Cursor::new(img), 0)
            .err()
            .expect("expected open to fail");
        match err {
            FilesystemError::Parse(msg) => assert!(msg.contains("no superblock")),
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn open_rejects_implausible_block_size() {
        // Block size = 7 → not a power of two, outside [512, 65536].
        let img = build_sb(
            UfsVersion::Ufs1,
            UfsEndian::Little,
            SB_OFFSET_UFS1,
            7,
            1,
            1,
            16,
            16,
            16,
            b"",
            1,
            0,
        );
        let err = UfsFilesystem::open(Cursor::new(img), 0)
            .err()
            .expect("expected open to fail");
        match err {
            FilesystemError::Parse(msg) => assert!(msg.contains("block size")),
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn open_rejects_frag_inconsistent_with_bsize_fsize() {
        // bsize=8192, fsize=1024 → frag must be 8; we lie and say 4.
        let mut img = build_default_sb(UfsVersion::Ufs1, UfsEndian::Little, SB_OFFSET_UFS1);
        let sb_off = SB_OFFSET_UFS1 as usize;
        img[sb_off + OFF_FRAG..sb_off + OFF_FRAG + 4].copy_from_slice(&4i32.to_le_bytes());
        let err = UfsFilesystem::open(Cursor::new(img), 0)
            .err()
            .expect("expected open to fail");
        match err {
            FilesystemError::Parse(msg) => assert!(msg.contains("inconsistent")),
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn open_rejects_su_j_dirty_volume() {
        let img = build_sb(
            UfsVersion::Ufs2,
            UfsEndian::Little,
            SB_OFFSET_UFS1,
            8192,
            1024,
            1,
            16384,
            64,
            16384,
            b"",
            FS_DIRTY,
            FS_SUJ,
        );
        let err = UfsFilesystem::open(Cursor::new(img), 0)
            .err()
            .expect("expected open to fail");
        match err {
            FilesystemError::Unsupported(msg) => {
                assert!(msg.contains("softupdate-journaled") || msg.contains("SU+J"))
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn open_accepts_su_j_clean_volume() {
        // SU+J + clean is fine — journal is consistent.
        let img = build_sb(
            UfsVersion::Ufs2,
            UfsEndian::Little,
            SB_OFFSET_UFS1,
            8192,
            1024,
            1,
            16384,
            64,
            16384,
            b"",
            1,
            FS_SUJ,
        );
        let fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open SU+J clean");
        assert_eq!(fs.fs_type(), "UFS2");
    }

    // ---- Field decoding ----

    #[test]
    fn parses_label_when_present() {
        let img = build_sb(
            UfsVersion::Ufs1,
            UfsEndian::Little,
            SB_OFFSET_UFS1,
            8192,
            1024,
            1,
            16384,
            64,
            16384,
            b"ufs1_test\0\0",
            1,
            0,
        );
        let fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        assert_eq!(fs.volume_label(), Some("ufs1_test"));
    }

    #[test]
    fn label_is_none_when_volname_is_empty() {
        let img = build_default_sb(UfsVersion::Ufs1, UfsEndian::Little, SB_OFFSET_UFS1);
        let fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        assert_eq!(fs.volume_label(), None);
    }

    #[test]
    fn sizes_use_old_size_for_ufs1() {
        let img = build_sb(
            UfsVersion::Ufs1,
            UfsEndian::Little,
            SB_OFFSET_UFS1,
            8192,
            1024,
            1,
            16384,
            64,
            16384,
            b"",
            1,
            0,
        );
        let fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        // 16384 fragments × 1024 bytes = 16 MiB
        assert_eq!(fs.total_size(), 16 * 1024 * 1024);
    }

    #[test]
    fn sizes_use_size_for_ufs2() {
        let img = build_sb(
            UfsVersion::Ufs2,
            UfsEndian::Little,
            SB_OFFSET_UFS2,
            4096,
            512,
            2,
            32768,
            128,
            65536,
            b"",
            1,
            0,
        );
        let fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        // 65536 fragments × 512 bytes = 32 MiB
        assert_eq!(fs.total_size(), 32 * 1024 * 1024);
    }

    #[test]
    fn partition_offset_is_threaded_through_seek() {
        // Mount the same UFS image at a non-zero "partition" offset.
        // Open must still find the SB and produce identical metadata.
        let raw = build_default_sb(UfsVersion::Ufs1, UfsEndian::Little, SB_OFFSET_UFS1);
        let prefix = vec![0xCDu8; 4 * 1024 * 1024]; // 4 MiB of junk
        let mut wrapped = prefix.clone();
        wrapped.extend_from_slice(&raw);
        let fs = UfsFilesystem::open(Cursor::new(wrapped), prefix.len() as u64).expect("open");
        assert_eq!(fs.fs_type(), "UFS1");
        assert_eq!(fs.total_size(), 16 * 1024 * 1024);
    }

    // ---- Fixture-driven tests (real makefs-built images) ----

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
    fn fixture_ufs1_opens_with_expected_geometry() {
        // From `scripts/generate-ufs-fixtures.sh` + `makefs -t ffs -B le
        // -s 16m -o version=1`. makefs reported "1 cylinder group, 2048
        // blks, 64 inodes" with 8192-byte blocks and 1024-byte frags.
        let img = load_fixture("test_ufs1.img.zst");
        let fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open ufs1");
        assert_eq!(fs.fs_type(), "UFS1");
        assert_eq!(fs.endian(), UfsEndian::Little);
        assert_eq!(fs.bsize, 8192);
        assert_eq!(fs.fsize, 1024);
        assert_eq!(fs.frag, 8);
        assert_eq!(fs.ncg, 1);
        assert_eq!(fs.ipg, 64);
        // 16 MiB raw size — 16384 fragments × 1024 bytes.
        assert_eq!(fs.total_size(), 16 * 1024 * 1024);
        // makefs doesn't set a volume name, so we expect None.
        assert_eq!(fs.volume_label(), None);
    }

    #[test]
    fn fixture_ufs2_opens_with_expected_geometry() {
        // From `makefs -t ffs -B le -s 16m -o version=2`. makefs reported
        // "1 cylinder group, 2048 blks, 32 inodes" — UFS2 inodes are
        // 256 bytes so density per group is lower.
        let img = load_fixture("test_ufs2.img.zst");
        let fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open ufs2");
        assert_eq!(fs.fs_type(), "UFS2");
        assert_eq!(fs.endian(), UfsEndian::Little);
        assert_eq!(fs.bsize, 8192);
        assert_eq!(fs.fsize, 1024);
        assert_eq!(fs.frag, 8);
        assert_eq!(fs.ncg, 1);
        assert_eq!(fs.ipg, 32);
        // makefs places the UFS2 SB at byte 8192 for small images.
        assert_eq!(fs.superblock_offset(), SB_OFFSET_UFS1);
        assert_eq!(fs.total_size(), 16 * 1024 * 1024);
    }

    // ---- U.2 — CG walk + compact + last_data_byte ----

    fn corrupt_cg_magic(img: &mut [u8], fs: &UfsFilesystem<Cursor<Vec<u8>>>) {
        // CG 0 header lives at byte `cgbase_frag(0) + cblkno`*fsize.
        let cg_off = fs.cg_header_offset(0) as usize;
        img[cg_off + CG_OFF_MAGIC..cg_off + CG_OFF_MAGIC + 4]
            .copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
    }

    #[test]
    fn fixture_ufs1_last_data_byte_matches_probe() {
        // Independent probe (`scripts/probe-ufs-cg.py`) shows the
        // makefs-built fixture allocates up to fragment 71 within the
        // single CG. (71 + 1) * fsize = 72 * 1024 = 73728 bytes.
        let img = load_fixture("test_ufs1.img.zst");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        assert_eq!(fs.last_data_byte().expect("scan"), 73728);
    }

    #[test]
    fn fixture_ufs2_last_data_byte_matches_probe() {
        // Same allocation pattern as UFS1 since makefs uses the same
        // file set; the CG header layout differs (slightly different
        // freeoff / iusedoff offsets within the CG) but the highest
        // allocated frag still lands at 71.
        let img = load_fixture("test_ufs2.img.zst");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        assert_eq!(fs.last_data_byte().expect("scan"), 73728);
    }

    #[test]
    fn fixture_ufs1_cg_header_validates_magic() {
        let mut img = load_fixture("test_ufs1.img.zst");
        let fs = UfsFilesystem::open(Cursor::new(img.clone()), 0).expect("open");
        corrupt_cg_magic(&mut img, &fs);
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let err = fs.last_data_byte().expect_err("expected magic mismatch");
        match err {
            FilesystemError::Parse(msg) => {
                assert!(msg.contains("CG 0") && msg.contains("magic"), "got: {msg}")
            }
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn fixture_ufs1_compact_reader_stream_length_matches_total_size() {
        // CompactReader is layout-preserving: stream length == original
        // partition size. Free fragments emit zeros (compress to almost
        // nothing); allocated fragments are read from their original
        // positions.
        let img = load_fixture("test_ufs1.img.zst");
        let (mut reader, info) =
            CompactUfsReader::new(Cursor::new(img.clone()), 0).expect("compactor");
        assert_eq!(info.original_size, 16 * 1024 * 1024);
        assert_eq!(info.compacted_size, 16 * 1024 * 1024);
        // 70 allocated fragments × 1024 bytes = 70 KiB of "live" data.
        // (Frags 0–39 + 40–45 + 48–71 = 40 + 6 + 24 = 70; frags 46/47 sit
        // in a gap between the inode table and the user data run.)
        assert_eq!(info.data_size, 70 * 1024);

        // Drain the stream and confirm we get exactly the original size.
        let mut sink = Vec::new();
        std::io::copy(&mut reader, &mut sink).expect("drain");
        assert_eq!(sink.len() as u64, info.original_size);
    }

    #[test]
    fn fixture_ufs1_compact_reader_preserves_allocated_byte_offsets() {
        // The compactor must emit the original file bytes at their
        // original byte offsets. Read the raw fixture and the compact
        // stream side-by-side; allocated regions (frags 0-45, 48-71)
        // must match byte-for-byte. Free regions emit zeros from the
        // compactor and arbitrary content from the source.
        let img = load_fixture("test_ufs1.img.zst");
        let raw = img.clone();
        let (mut reader, info) = CompactUfsReader::new(Cursor::new(img), 0).expect("compactor");
        let mut out = Vec::with_capacity(info.original_size as usize);
        std::io::copy(&mut reader, &mut out).expect("drain");

        // Allocated frags 0..46 — the metadata + superblock region —
        // must round-trip identically. Skip frag 46/47 (free per probe)
        // and check frags 48..72 (the 24 KiB large.bin file).
        let fsize = 1024usize;
        for frag in 0..46 {
            let start = frag * fsize;
            assert_eq!(
                &out[start..start + fsize],
                &raw[start..start + fsize],
                "frag {frag} (allocated) round-trip mismatch"
            );
        }
        for frag in 48..72 {
            let start = frag * fsize;
            assert_eq!(
                &out[start..start + fsize],
                &raw[start..start + fsize],
                "frag {frag} (allocated, part of large.bin) round-trip mismatch"
            );
        }
        // Free frags (72..16384) — compactor emits zeros.
        for frag in 72..16384 {
            let start = frag * fsize;
            assert!(
                out[start..start + fsize].iter().all(|&b| b == 0),
                "frag {frag} (free) should be zero-filled by compactor"
            );
        }
    }

    #[test]
    fn fixture_ufs1_compact_reader_round_trips_through_parser() {
        // The compacted stream must itself be a valid UFS image — when we
        // feed it back into UfsFilesystem::open we should see identical
        // geometry.
        let img = load_fixture("test_ufs1.img.zst");
        let (mut reader, _info) = CompactUfsReader::new(Cursor::new(img), 0).expect("compactor");
        let mut out = Vec::new();
        std::io::copy(&mut reader, &mut out).expect("drain");
        let fs = UfsFilesystem::open(Cursor::new(out), 0).expect("re-open compacted");
        assert_eq!(fs.fs_type(), "UFS1");
        assert_eq!(fs.total_size(), 16 * 1024 * 1024);
        assert_eq!(fs.ncg, 1);
    }

    #[test]
    fn fixture_ufs2_compact_reader_round_trips_through_parser() {
        let img = load_fixture("test_ufs2.img.zst");
        let (mut reader, _info) = CompactUfsReader::new(Cursor::new(img), 0).expect("compactor");
        let mut out = Vec::new();
        std::io::copy(&mut reader, &mut out).expect("drain");
        let fs = UfsFilesystem::open(Cursor::new(out), 0).expect("re-open compacted");
        assert_eq!(fs.fs_type(), "UFS2");
        assert_eq!(fs.total_size(), 16 * 1024 * 1024);
        assert_eq!(fs.ncg, 1);
    }

    // ---- U.3 — dinode + directory + file walk ----

    fn find_child<'a>(children: &'a [FileEntry], name: &str) -> &'a FileEntry {
        children
            .iter()
            .find(|e| e.name == name)
            .unwrap_or_else(|| panic!("missing child {name:?} in listing"))
    }

    #[test]
    fn inode_byte_offset_matches_kernel_formula() {
        // From the fixture probe (scripts/probe-ufs-dinode.py):
        //   UFS1 root inode 2 lives at byte 33024.
        //   UFS2 root inode 2 lives at byte 33280.
        // Formula: (cg * fpg + iblkno) * fsize + in_cg * dinode_size
        //   = (0 * 16384 + 32) * 1024 + 2 * 128 = 32768 + 256 = 33024  (UFS1)
        //   = (0 * 16384 + 32) * 1024 + 2 * 256 = 32768 + 512 = 33280  (UFS2)
        let img1 = load_fixture("test_ufs1.img.zst");
        let fs1 = UfsFilesystem::open(Cursor::new(img1), 0).expect("open ufs1");
        assert_eq!(fs1.inode_byte_offset(ROOT_INODE), 33024);

        let img2 = load_fixture("test_ufs2.img.zst");
        let fs2 = UfsFilesystem::open(Cursor::new(img2), 0).expect("open ufs2");
        assert_eq!(fs2.inode_byte_offset(ROOT_INODE), 33280);
    }

    #[test]
    fn rejects_inode_below_root() {
        let img = load_fixture("test_ufs1.img.zst");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let err = fs.read_inode(0).expect_err("inode 0 reserved");
        assert!(matches!(err, FilesystemError::Parse(_)));
        let err = fs.read_inode(1).expect_err("inode 1 reserved (bad-blocks)");
        assert!(matches!(err, FilesystemError::Parse(_)));
    }

    #[test]
    fn rejects_inode_out_of_range() {
        let img = load_fixture("test_ufs1.img.zst");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        // ipg=64, ncg=1, so 64 is the first out-of-range inode.
        let err = fs.read_inode(64).expect_err("inode 64 out of range");
        match err {
            FilesystemError::Parse(msg) => assert!(msg.contains("out of range"), "got: {msg}"),
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn fixture_ufs1_reads_root_inode() {
        let img = load_fixture("test_ufs1.img.zst");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let inode = fs.read_inode(ROOT_INODE).expect("root inode");
        assert_eq!(inode.inum, ROOT_INODE);
        assert_eq!(inode.mode & 0o170000, 0o040000, "root must be a directory");
        assert_eq!(inode.mode & 0o777, 0o755);
        // makefs links: . + .. + subdir = 3 nlinks on the root.
        assert_eq!(inode.nlink, 3);
        // Single 1-fragment direct pointer to the dir block (frag 41).
        assert_eq!(inode.direct[0], 41);
        for i in 1..UFS_NDADDR {
            assert_eq!(inode.direct[i], 0);
        }
        for i in 0..UFS_NIADDR {
            assert_eq!(inode.indirect[i], 0);
        }
        // Root dir holds exactly one fragment (512 bytes).
        assert_eq!(inode.size, 512);
    }

    #[test]
    fn fixture_ufs2_reads_root_inode_64bit_fields() {
        let img = load_fixture("test_ufs2.img.zst");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let inode = fs.read_inode(ROOT_INODE).expect("root inode");
        // UFS2 stores uid/gid at offset 4/8 (vs UFS1's 112/116). Probe
        // showed uid=gid=1002, mtime=1780429315.
        assert_eq!(inode.uid, 1002);
        assert_eq!(inode.gid, 1002);
        assert_eq!(inode.mtime, 1780429315);
        assert_eq!(inode.direct[0], 41);
    }

    #[test]
    fn fixture_ufs1_lists_root() {
        let img = load_fixture("test_ufs1.img.zst");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let kids = fs.list_directory(&root).expect("list root");
        // . and .. filtered → 5 entries: hello.txt, large.bin, tiny.txt,
        // link.txt, subdir.
        let names: Vec<_> = kids.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names.len(), 5, "expected 5 entries, got: {names:?}");
        for expected in ["hello.txt", "large.bin", "tiny.txt", "link.txt", "subdir"] {
            assert!(
                names.contains(&expected),
                "missing {expected}; got: {names:?}"
            );
        }

        // Spot-check metadata.
        let hello = find_child(&kids, "hello.txt");
        assert_eq!(hello.entry_type, EntryType::File);
        assert_eq!(hello.size, 11);
        assert_eq!(hello.uid, Some(1002));
        assert_eq!(hello.gid, Some(1002));
        assert_eq!(hello.path, "/hello.txt");
        assert_eq!(hello.mode_string().unwrap().chars().next(), Some('-'));

        let subdir = find_child(&kids, "subdir");
        assert_eq!(subdir.entry_type, EntryType::Directory);
        assert_eq!(subdir.path, "/subdir");
    }

    #[test]
    fn fixture_ufs1_filters_dot_and_dotdot() {
        let img = load_fixture("test_ufs1.img.zst");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let kids = fs.list_directory(&root).expect("list root");
        assert!(!kids.iter().any(|e| e.name == "." || e.name == ".."));
    }

    #[test]
    fn fixture_ufs1_descends_subdir() {
        let img = load_fixture("test_ufs1.img.zst");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let kids = fs.list_directory(&root).expect("list root");
        let subdir = find_child(&kids, "subdir");
        let nested = fs.list_directory(subdir).expect("list subdir");
        // Just nested.txt — . and .. are filtered.
        assert_eq!(nested.len(), 1);
        assert_eq!(nested[0].name, "nested.txt");
        assert_eq!(nested[0].path, "/subdir/nested.txt");
        assert_eq!(nested[0].size, 11);
        assert_eq!(nested[0].entry_type, EntryType::File);
    }

    #[test]
    fn fixture_ufs1_inline_symlink_target() {
        let img = load_fixture("test_ufs1.img.zst");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let kids = fs.list_directory(&root).expect("list root");
        let link = find_child(&kids, "link.txt");
        assert_eq!(link.entry_type, EntryType::Symlink);
        // 9-byte target ("hello.txt") <= 60-byte UFS1 maxsymlinklen, so
        // inline — read out of the dinode's pointer area, not data block.
        assert_eq!(link.symlink_target.as_deref(), Some("hello.txt"));
        // size carries the symlink target length.
        assert_eq!(link.size, 9);
    }

    #[test]
    fn fixture_ufs2_inline_symlink_target() {
        let img = load_fixture("test_ufs2.img.zst");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let kids = fs.list_directory(&root).expect("list root");
        let link = find_child(&kids, "link.txt");
        assert_eq!(link.entry_type, EntryType::Symlink);
        // UFS2 maxsymlinklen=120; target still inline.
        assert_eq!(link.symlink_target.as_deref(), Some("hello.txt"));
    }

    #[test]
    fn fixture_ufs1_reads_small_file() {
        let img = load_fixture("test_ufs1.img.zst");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let kids = fs.list_directory(&root).expect("list");
        let hello = find_child(&kids, "hello.txt");
        let bytes = fs.read_file(hello, 1_000_000).expect("read");
        assert_eq!(bytes, b"Hello, UFS!");
    }

    #[test]
    fn fixture_ufs1_reads_tiny_file() {
        let img = load_fixture("test_ufs1.img.zst");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let kids = fs.list_directory(&root).expect("list");
        let tiny = find_child(&kids, "tiny.txt");
        let bytes = fs.read_file(tiny, 1_000_000).expect("read");
        // Should be exactly 10 bytes, padding stripped.
        assert_eq!(bytes, b"tiny bytes");
    }

    #[test]
    fn fixture_ufs1_reads_24kib_via_direct_blocks() {
        // large.bin = 24 KiB = 3 full blocks via direct[0..3]. Confirms
        // the multi-direct-block walk and the byte formula matches.
        let img = load_fixture("test_ufs1.img.zst");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let kids = fs.list_directory(&root).expect("list");
        let big = find_child(&kids, "large.bin");
        assert_eq!(big.size, 24 * 1024);
        let bytes = fs.read_file(big, 24 * 1024).expect("read");
        assert_eq!(bytes.len(), 24 * 1024);
        // Deterministic content: data[i] = (i*37 + 11) & 0xFF.
        for i in [0usize, 1, 100, 4095, 4096, 8191, 8192, 12345, 16384, 24575] {
            assert_eq!(
                bytes[i],
                ((i * 37 + 11) & 0xFF) as u8,
                "large.bin[{i}] mismatch"
            );
        }
    }

    #[test]
    fn fixture_ufs1_read_file_honours_max_bytes() {
        let img = load_fixture("test_ufs1.img.zst");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let kids = fs.list_directory(&root).expect("list");
        let big = find_child(&kids, "large.bin");
        let bytes = fs.read_file(big, 5000).expect("read clamped");
        assert_eq!(bytes.len(), 5000);
        // First 5 KB still matches the deterministic content.
        for i in [0usize, 100, 4095, 4999] {
            assert_eq!(bytes[i], ((i * 37 + 11) & 0xFF) as u8);
        }
    }

    #[test]
    fn fixture_ufs2_reads_small_and_large_files() {
        let img = load_fixture("test_ufs2.img.zst");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let kids = fs.list_directory(&root).expect("list");
        assert_eq!(
            fs.read_file(find_child(&kids, "hello.txt"), 1_000_000)
                .unwrap(),
            b"Hello, UFS!"
        );
        let big = fs
            .read_file(find_child(&kids, "large.bin"), 24 * 1024)
            .unwrap();
        assert_eq!(big.len(), 24 * 1024);
        assert_eq!(big[0], 11);
        assert_eq!(big[24575], ((24575 * 37 + 11) & 0xFF) as u8);
    }

    #[test]
    fn fixture_ufs1_read_file_on_directory_is_parse_error() {
        let img = load_fixture("test_ufs1.img.zst");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let err = fs.read_file(&root, 64).expect_err("expected parse error");
        assert!(matches!(err, FilesystemError::Parse(_)));
    }

    #[test]
    fn fixture_ufs1_read_file_on_symlink_returns_target_bytes() {
        let img = load_fixture("test_ufs1.img.zst");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        let kids = fs.list_directory(&root).expect("list");
        let link = find_child(&kids, "link.txt");
        let bytes = fs.read_file(link, 1_000_000).expect("read symlink");
        assert_eq!(bytes, b"hello.txt");
    }

    // ---- Synthetic indirect-block walk ----
    //
    // The makefs fixtures only exercise direct[0..3] (large.bin = 24 KiB
    // = 3 blocks). To prove the single-indirect walker independently, we
    // hand-construct a UFS1 image with a single-indirect block pointing
    // at one data block beyond the 12-direct ceiling.

    #[test]
    fn synthetic_single_indirect_walk() {
        // 1 MiB UFS1 image, bsize=8192, fsize=1024.
        let mut img = build_default_sb(UfsVersion::Ufs1, UfsEndian::Little, SB_OFFSET_UFS1);
        img.resize(2 * 1024 * 1024, 0);
        // Lay out a synthetic file at block 100 (frag 800) with 13 logical
        // blocks: the first 12 are direct (zero-filled is fine, we only
        // verify the 13th via the indirect block walker).
        //
        // Indirect block lives at fragment 200 (one full block, fragments
        // 200..208). It holds a single u32 pointing at fragment 800.
        let indirect_byte = 200 * 1024usize;
        // Pointer 0 of the indirect block → fragment 800.
        img[indirect_byte..indirect_byte + 4].copy_from_slice(&800u32.to_le_bytes());

        // Fragment 800 → bytes [800*1024 .. +8192) = [819200..827392). Fill
        // with a sentinel pattern.
        let data_byte = 800 * 1024usize;
        for (i, slot) in img[data_byte..data_byte + 8192].iter_mut().enumerate() {
            *slot = (i & 0xFF) as u8;
        }

        // Allocate inode 5 and patch its on-disk dinode: mode=file, size =
        // 13 blocks, direct[0..12] all pointing at irrelevant fragments
        // (zero is fine — the test only reads block 12 which falls into
        // the indirect range), indirect[0] = 200.
        let fs = UfsFilesystem::open(Cursor::new(img.clone()), 0).expect("open");
        let inode_off = fs.inode_byte_offset(5) as usize;
        let endian = fs.endian;
        // di_mode = regular (S_IFREG | 0644) = 0o100644.
        img[inode_off..inode_off + 2].copy_from_slice(&0o100644u16.to_le_bytes());
        // di_size = 13 * 8192 = 106496 bytes.
        let size: u64 = 13 * 8192;
        img[inode_off + D1_OFF_SIZE..inode_off + D1_OFF_SIZE + 8]
            .copy_from_slice(&size.to_le_bytes());
        // di_ib[0] = 200 (the indirect block we built above).
        img[inode_off + D1_OFF_IB..inode_off + D1_OFF_IB + 4]
            .copy_from_slice(&200u32.to_le_bytes());
        // Sanity: endian matches.
        assert_eq!(endian, UfsEndian::Little);

        // Re-open against the patched image.
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("re-open");
        let inode = fs.read_inode(5).expect("read inode 5");
        assert_eq!(inode.size, 106_496);
        assert_eq!(inode.indirect[0], 200);

        // Resolve logical block 12 — the first one past direct[] — should
        // walk into the indirect block and return fragment 800.
        let blk12 = fs.resolve_logical_block(&inode, 12).expect("resolve");
        assert_eq!(blk12, 800);

        // Read just block 12 (offset 12 * 8192 .. 13 * 8192) through the
        // public API. We do this by clipping the inode and reading from
        // offset 12*8192 — there's no offset-read API, so do a full read
        // (lots of zero direct blocks) and slice.
        let bytes = fs
            .read_file(
                &FileEntry::new_file("synth".into(), "/synth".into(), inode.size, 5),
                inode.size as usize,
            )
            .expect("read");
        assert_eq!(bytes.len(), 106_496);
        // Direct blocks (sparse) → zeros.
        assert!(bytes[..12 * 8192].iter().all(|&b| b == 0));
        // Indirect block 12 → the sentinel pattern.
        for i in 0..8192usize {
            assert_eq!(bytes[12 * 8192 + i], (i & 0xFF) as u8);
        }
    }

    // ---- U.4 write primitives ----

    /// Build a tiny synthetic UFS2 volume with one CG, valid CG header,
    /// inode-used bitmap, and free-fragment bitmap, all sized so the write
    /// primitives can round-trip without tripping the CG validators.
    fn build_writable_fixture() -> Vec<u8> {
        let bsize = 8192u32;
        let fsize = 1024u32;
        let fpg = 256u32; // small CG for fast iteration
        let ipg = 64u32;
        let ncg = 1u32;
        let total_frags = fpg as u64;
        let mut img = build_sb(
            UfsVersion::Ufs2,
            UfsEndian::Little,
            SB_OFFSET_UFS1,
            bsize,
            fsize,
            ncg,
            fpg,
            ipg,
            total_frags,
            b"",
            1,
            0,
        );
        // Grow to volume size.
        let needed = (total_frags as usize) * (fsize as usize);
        if img.len() < needed {
            img.resize(needed, 0);
        }
        // Stamp CG 0 header at fragment 24 (build_sb's default cblkno).
        let cg_byte = 24usize * fsize as usize;
        img[cg_byte + CG_OFF_MAGIC..cg_byte + CG_OFF_MAGIC + 4]
            .copy_from_slice(&CG_MAGIC.to_le_bytes());
        // cg_cgx = 0
        // cg_ndblk = fpg
        img[cg_byte + CG_OFF_NDBLK..cg_byte + CG_OFF_NDBLK + 4].copy_from_slice(&fpg.to_le_bytes());
        // cs counters: ndir=0, nbfree=0, nifree=ipg-2 (root + bad-blocks
        // marked used by the fixture), nffree=fpg
        let cs_off = cg_byte + 24;
        (img[cs_off..cs_off + 4]).copy_from_slice(&0i32.to_le_bytes());
        (img[cs_off + 4..cs_off + 8]).copy_from_slice(&0i32.to_le_bytes());
        (img[cs_off + 8..cs_off + 12]).copy_from_slice(&((ipg as i32) - 2).to_le_bytes());
        (img[cs_off + 12..cs_off + 16]).copy_from_slice(&(fpg as i32).to_le_bytes());
        // iusedoff = 256, freeoff = 264, nextfreeoff = freeoff + ndblk/8
        let iusedoff = 256u32;
        let freeoff = 264u32;
        let nextfreeoff = freeoff + fpg.div_ceil(8);
        img[cg_byte + CG_OFF_IUSEDOFF..cg_byte + CG_OFF_IUSEDOFF + 4]
            .copy_from_slice(&iusedoff.to_le_bytes());
        img[cg_byte + CG_OFF_FREEOFF..cg_byte + CG_OFF_FREEOFF + 4]
            .copy_from_slice(&freeoff.to_le_bytes());
        img[cg_byte + CG_OFF_NEXTFREEOFF..cg_byte + CG_OFF_NEXTFREEOFF + 4]
            .copy_from_slice(&nextfreeoff.to_le_bytes());
        // iused bitmap: first 2 bits set (inums 0 + 1 reserved, inum 2 root).
        // Actually mark inums 0, 1, 2 used so the next alloc returns 3.
        img[cg_byte + iusedoff as usize] = 0b0000_0111;
        // Free bitmap: all fragments free (0xFF, set=free).
        let free_bytes = fpg.div_ceil(8) as usize;
        for i in 0..free_bytes {
            img[cg_byte + freeoff as usize + i] = 0xFF;
        }
        img
    }

    #[test]
    fn write_inode_round_trips_through_read_inode() {
        let img = build_writable_fixture();
        let mut backing = img.clone();
        let mut fs = UfsFilesystem::open(Cursor::new(&mut backing), 0).expect("open");
        // Build a synthetic regular-file inode at inum 5.
        let mut inode = UfsInode {
            inum: 5,
            mode: 0o100644,
            nlink: 1,
            uid: 1000,
            gid: 100,
            size: 4096,
            mtime: 0x12345678,
            direct: [0; UFS_NDADDR],
            indirect: [0; UFS_NIADDR],
            inline_payload: Vec::new(),
        };
        inode.direct[0] = 64;
        inode.direct[1] = 72;
        inode.indirect[0] = 128;

        fs.write_inode(5, &inode).expect("write inode");
        let read_back = fs.read_inode(5).expect("read inode");
        assert_eq!(read_back.mode, inode.mode);
        assert_eq!(read_back.nlink, inode.nlink);
        assert_eq!(read_back.uid, inode.uid);
        assert_eq!(read_back.gid, inode.gid);
        assert_eq!(read_back.size, inode.size);
        assert_eq!(read_back.mtime, inode.mtime);
        assert_eq!(read_back.direct[0], 64);
        assert_eq!(read_back.direct[1], 72);
        assert_eq!(read_back.indirect[0], 128);
    }

    #[test]
    fn alloc_inode_marks_iused_and_decrements_nifree() {
        let img = build_writable_fixture();
        let mut backing = img;
        let mut fs = UfsFilesystem::open(Cursor::new(&mut backing), 0).expect("open");
        let (_ndir, _nbfree, nifree_before, _nffree) = fs.read_cg_cs(0).expect("cs before");
        let inum = fs.alloc_inode(0).expect("alloc inode");
        assert_eq!(inum, 3, "first free inum after reserved [0..3) is 3");
        // iused bit for inum 3 must now be set.
        let bm = fs.read_cg_iused_bitmap(0).expect("iused");
        assert_eq!(bm[0] & 0b0000_1000, 0b0000_1000);
        // nifree must drop by 1.
        let (_, _, nifree_after, _) = fs.read_cg_cs(0).expect("cs after");
        assert_eq!(nifree_after, nifree_before - 1);
    }

    #[test]
    fn free_inode_clears_iused_and_increments_nifree() {
        let img = build_writable_fixture();
        let mut backing = img;
        let mut fs = UfsFilesystem::open(Cursor::new(&mut backing), 0).expect("open");
        // Inum 2 is the root, marked used in the fixture. Free it.
        let (_, _, nifree_before, _) = fs.read_cg_cs(0).expect("cs before");
        fs.free_inode(2).expect("free inode");
        let bm = fs.read_cg_iused_bitmap(0).expect("iused");
        assert_eq!(bm[0] & 0b0000_0100, 0, "inum 2 bit must clear");
        let (_, _, nifree_after, _) = fs.read_cg_cs(0).expect("cs after");
        assert_eq!(nifree_after, nifree_before + 1);
    }

    #[test]
    fn alloc_frag_run_finds_contig_window_and_marks_allocated() {
        let img = build_writable_fixture();
        let mut backing = img;
        let mut fs = UfsFilesystem::open(Cursor::new(&mut backing), 0).expect("open");
        let (_, _, _, nffree_before) = fs.read_cg_cs(0).expect("cs before");
        let start = fs.alloc_frag_run(8, 0).expect("alloc 8 frags");
        // First clear-bit run starts at bit 0 (all 256 frags are free).
        assert_eq!(start, 0);
        let (bm, _) = fs.read_cg_free_bitmap(0).expect("re-read free bitmap");
        // Frags 0..8 must now be CLEAR (= allocated). Byte 0 should be 0.
        assert_eq!(bm[0], 0);
        let (_, _, _, nffree_after) = fs.read_cg_cs(0).expect("cs after");
        assert_eq!(nffree_after, nffree_before - 8);
    }

    #[test]
    fn free_frag_run_restores_bits() {
        let img = build_writable_fixture();
        let mut backing = img;
        let mut fs = UfsFilesystem::open(Cursor::new(&mut backing), 0).expect("open");
        let start = fs.alloc_frag_run(8, 0).expect("alloc");
        let (_, _, _, nffree_after_alloc) = fs.read_cg_cs(0).expect("cs alloc");
        fs.free_frag_run(start, 8).expect("free");
        let (bm, _) = fs.read_cg_free_bitmap(0).expect("re-read");
        // First 8 bits must be SET again (all free).
        assert_eq!(bm[0], 0xFF);
        let (_, _, _, nffree_after_free) = fs.read_cg_cs(0).expect("cs free");
        assert_eq!(nffree_after_free, nffree_after_alloc + 8);
    }

    #[test]
    fn write_frag_run_round_trips_through_read() {
        let img = build_writable_fixture();
        let mut backing = img;
        let mut fs = UfsFilesystem::open(Cursor::new(&mut backing), 0).expect("open");
        let payload: Vec<u8> = (0..1024).map(|i| (i & 0xFF) as u8).collect();
        // Write 1 fragment's worth at frag 100 (within bounds).
        fs.write_frag_run(100, &payload).expect("write frag");
        // Read back by seeking directly to the same byte offset.
        let byte = 100 * fs.fsize;
        fs.reader.seek(SeekFrom::Start(byte)).expect("seek");
        let mut buf = vec![0u8; 1024];
        fs.reader.read_exact(&mut buf).expect("read");
        assert_eq!(buf, payload);
        let _ = fs.endian_for_test(); // exercise the test-only accessor
    }

    #[test]
    fn alloc_frag_run_rejects_oversize_request() {
        let img = build_writable_fixture();
        let mut backing = img;
        let mut fs = UfsFilesystem::open(Cursor::new(&mut backing), 0).expect("open");
        // fpg = 256; ask for fpg + 1 — must refuse with Unsupported.
        let err = fs.alloc_frag_run(257, 0).expect_err("oversize must fail");
        assert!(
            matches!(err, FilesystemError::Unsupported(_)),
            "expected Unsupported, got {err:?}"
        );
    }

    #[test]
    fn alloc_inode_returns_disk_full_when_all_used() {
        let mut img = build_writable_fixture();
        // Saturate the iused bitmap so every inum is "used".
        // iused bitmap lives at CG 0 byte (cblkno=24, fsize=1024) + iusedoff (256).
        let iused_byte = 24 * 1024usize + 256usize;
        for i in 0..(64u32.div_ceil(8) as usize) {
            img[iused_byte + i] = 0xFF;
        }
        let mut backing = img;
        let mut fs = UfsFilesystem::open(Cursor::new(&mut backing), 0).expect("open");
        let err = fs.alloc_inode(0).expect_err("alloc when full must fail");
        assert!(
            matches!(err, FilesystemError::DiskFull(_)),
            "expected DiskFull, got {err:?}"
        );
    }

    // ---- EditableFilesystem (U.4 edit) ----

    /// Round-trip create_file + read_file against the real UFS1 makefs
    /// fixture. The fixture is already fsck-clean; we mutate it in
    /// memory, re-run fsck, and confirm the new file shows up via
    /// list_directory.
    #[test]
    fn create_file_round_trips_against_ufs1_fixture() {
        use super::super::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
        let img = load_fixture("test_ufs1.img.zst");
        let mut backing = img;
        let mut fs = UfsFilesystem::open(Cursor::new(&mut backing), 0).expect("open");
        let root = fs.root().expect("root");
        let payload = b"hello from ufs editable surface".to_vec();
        let mut src = payload.as_slice();
        let new_entry = fs
            .create_file(
                &root,
                "newfile.txt",
                &mut src,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create_file");
        assert_eq!(new_entry.size, payload.len() as u64);

        // Re-open to confirm everything is on disk.
        let mut fs2 = UfsFilesystem::open(Cursor::new(&mut backing), 0).expect("re-open");
        let root2 = fs2.root().expect("root2");
        let kids = fs2.list_directory(&root2).expect("list");
        let found = kids
            .iter()
            .find(|e| e.name == "newfile.txt")
            .expect("newfile.txt present");
        let bytes = fs2.read_file(found, 1024).expect("read");
        assert_eq!(bytes, payload);

        // fsck must stay clean after the mutation.
        let result = fs2.fsck().expect("supports fsck").expect("runs");
        assert!(
            result.is_clean(),
            "fsck reported errors after create_file: {:?}",
            result.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    /// `create_directory` writes `.` and `..` and bumps the parent's
    /// nlink. Listing the new dir returns no children (the trait
    /// hides `.` / `..`).
    #[test]
    fn create_directory_round_trips_against_ufs2_fixture() {
        use super::super::filesystem::{CreateDirectoryOptions, EditableFilesystem, Filesystem};
        let img = load_fixture("test_ufs2.img.zst");
        let mut backing = img;
        let mut fs = UfsFilesystem::open(Cursor::new(&mut backing), 0).expect("open");
        let root = fs.root().expect("root");
        let parent_nlink_before = fs
            .read_inode(root.location as u32)
            .expect("root inode")
            .nlink;

        let new_dir = fs
            .create_directory(&root, "newdir", &CreateDirectoryOptions::default())
            .expect("create_directory");
        assert!(new_dir.is_directory());

        // Re-open.
        let mut fs2 = UfsFilesystem::open(Cursor::new(&mut backing), 0).expect("re-open");
        let root2 = fs2.root().expect("root2");
        let kids = fs2.list_directory(&root2).expect("list");
        let found = kids
            .iter()
            .find(|e| e.name == "newdir")
            .expect("newdir present");
        assert!(found.is_directory());

        // Listing the new dir filters `.` / `..`, so it should be empty.
        let sub_kids = fs2.list_directory(found).expect("list sub");
        assert!(
            sub_kids.is_empty(),
            "fresh dir should list empty: {sub_kids:?}"
        );

        // Parent's nlink must have bumped (back-link from new dir's `..`).
        let parent_nlink_after = fs2.read_inode(root2.location as u32).expect("inode").nlink;
        assert_eq!(parent_nlink_after, parent_nlink_before + 1);

        let result = fs2.fsck().expect("supports fsck").expect("runs");
        assert!(
            result.is_clean(),
            "fsck reported errors after create_directory: {:?}",
            result.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    /// delete_entry frees the inode + data blocks; counters bump back.
    #[test]
    fn delete_entry_frees_inode_and_blocks() {
        use super::super::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
        let img = load_fixture("test_ufs1.img.zst");
        let mut backing = img;

        // Snapshot CG 0's counters BEFORE.
        let (nffree_before, nifree_before) = {
            let mut fs = UfsFilesystem::open(Cursor::new(&mut backing), 0).expect("open");
            let (_, _, nifree, nffree) = fs.read_cg_cs(0).expect("cs");
            (nffree, nifree)
        };

        let new_inum: u32;
        {
            let mut fs = UfsFilesystem::open(Cursor::new(&mut backing), 0).expect("open");
            let root = fs.root().expect("root");
            let payload = vec![0xABu8; 4096];
            let mut src = payload.as_slice();
            let new_entry = fs
                .create_file(
                    &root,
                    "tempfile.bin",
                    &mut src,
                    payload.len() as u64,
                    &CreateFileOptions::default(),
                )
                .expect("create");
            new_inum = new_entry.location as u32;

            // Delete it.
            fs.delete_entry(&root, &new_entry).expect("delete");
        }

        let mut fs = UfsFilesystem::open(Cursor::new(&mut backing), 0).expect("re-open");
        let root = fs.root().expect("root");
        let kids = fs.list_directory(&root).expect("list");
        assert!(
            !kids.iter().any(|e| e.name == "tempfile.bin"),
            "tempfile.bin must be gone"
        );
        // Inode slot must be zeroed (mode == 0).
        let zeroed = fs.read_inode(new_inum).expect("re-read inode");
        assert_eq!(zeroed.mode, 0);

        // Counters restored (modulo intermediate ops on counters from
        // create_directory's ndir bump — we don't make any here, so the
        // create+delete sequence should net to zero).
        let (_, _, nifree_after, nffree_after) = fs.read_cg_cs(0).expect("cs");
        assert_eq!(nifree_after, nifree_before);
        assert_eq!(nffree_after, nffree_before);

        let result = fs.fsck().expect("supports fsck").expect("runs");
        assert!(
            result.is_clean(),
            "fsck reported errors after delete: {:?}",
            result.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    /// delete_entry on a non-empty directory must error.
    #[test]
    fn delete_non_empty_dir_refuses() {
        use super::super::filesystem::{
            CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem,
        };
        let img = load_fixture("test_ufs2.img.zst");
        let mut backing = img;
        let mut fs = UfsFilesystem::open(Cursor::new(&mut backing), 0).expect("open");
        let root = fs.root().expect("root");
        let dir = fs
            .create_directory(&root, "notempty", &CreateDirectoryOptions::default())
            .expect("create dir");
        let payload = b"x".to_vec();
        let mut src = payload.as_slice();
        let _file = fs
            .create_file(
                &dir,
                "child",
                &mut src,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create child");

        let err = fs.delete_entry(&root, &dir).expect_err("must refuse");
        assert!(
            matches!(err, FilesystemError::InvalidData(_)),
            "expected InvalidData, got {err:?}"
        );
    }

    /// Files larger than 12 × bsize hit the indirect-block gate and
    /// return Unsupported. Confirms we don't silently accept oversized
    /// writes that would corrupt the volume.
    #[test]
    fn create_file_oversize_returns_unsupported() {
        use super::super::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
        let img = load_fixture("test_ufs1.img.zst");
        let mut backing = img;
        let mut fs = UfsFilesystem::open(Cursor::new(&mut backing), 0).expect("open");
        let root = fs.root().expect("root");
        let bsize = fs.bsize as usize;
        let oversize = vec![0u8; bsize * UFS_NDADDR + 1];
        let mut src = oversize.as_slice();
        let err = fs
            .create_file(
                &root,
                "toobig.bin",
                &mut src,
                oversize.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect_err("must refuse");
        assert!(
            matches!(err, FilesystemError::Unsupported(_)),
            "expected Unsupported, got {err:?}"
        );

        // The failed create rolls back: no `toobig.bin` in the listing,
        // and fsck stays clean (no orphan inode, no leaked frags).
        let kids = fs.list_directory(&root).expect("list");
        assert!(
            !kids.iter().any(|e| e.name == "toobig.bin"),
            "failed create must roll back"
        );
        let result = fs.fsck().expect("supports fsck").expect("runs");
        assert!(
            result.is_clean(),
            "fsck after failed create: {:?}",
            result.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    /// Two files in the same dir exercise dir_insert's slack-split path
    /// (the new entry should fit into the trailing slack of the existing
    /// `..` record without growing the dir).
    #[test]
    fn dir_insert_splits_trailing_slack_for_second_file() {
        use super::super::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
        let img = load_fixture("test_ufs2.img.zst");
        let mut backing = img;
        let mut fs = UfsFilesystem::open(Cursor::new(&mut backing), 0).expect("open");
        let root = fs.root().expect("root");
        let root_size_before = fs.read_inode(root.location as u32).expect("inode").size;

        for name in &["a.txt", "b.txt", "c.txt"] {
            let payload = name.as_bytes().to_vec();
            let mut src = payload.as_slice();
            fs.create_file(
                &root,
                name,
                &mut src,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create");
        }

        let kids = fs.list_directory(&root).expect("list");
        for name in &["a.txt", "b.txt", "c.txt"] {
            assert!(kids.iter().any(|e| e.name == *name), "{name} missing");
        }

        // Root size shouldn't grow: three short filenames fit easily in
        // the existing trailing slack of the first chunk.
        let root_size_after = fs.read_inode(root.location as u32).expect("inode").size;
        assert_eq!(
            root_size_after, root_size_before,
            "root dir grew unnecessarily"
        );

        let result = fs.fsck().expect("supports fsck").expect("runs");
        assert!(
            result.is_clean(),
            "fsck: {:?}",
            result.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    /// free_space sums per-CG nffree counts × fsize.
    #[test]
    fn free_space_aggregates_per_cg_nffree() {
        use super::super::filesystem::EditableFilesystem;
        let img = load_fixture("test_ufs2.img.zst");
        let mut backing = img;
        let mut fs = UfsFilesystem::open(Cursor::new(&mut backing), 0).expect("open");
        let space = fs.free_space().expect("free_space");
        // Sanity bound — must be > 0 and ≤ total_size.
        assert!(space > 0);
        assert!(space <= fs.total_size());
    }

    // ---- fsck (U.4) ----

    #[test]
    fn fixture_ufs1_fsck_clean() {
        let img = load_fixture("test_ufs1.img.zst");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open ufs1");
        let result = fs.fsck().expect("UFS implements fsck").expect("fsck runs");
        assert!(
            result.is_clean(),
            "fsck reported errors on a known-clean fixture: {:?}",
            result
                .errors
                .iter()
                .map(|e| (&e.code, &e.message))
                .collect::<Vec<_>>()
        );
        // Fixture has the standard makefs root structure (./../a.txt/...);
        // confirm the stats counters fired.
        assert!(result.stats.files_checked > 0 || result.stats.directories_checked > 0);
    }

    #[test]
    fn fixture_ufs2_fsck_clean() {
        let img = load_fixture("test_ufs2.img.zst");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open ufs2");
        let result = fs.fsck().expect("UFS implements fsck").expect("fsck runs");
        assert!(
            result.is_clean(),
            "fsck reported errors on a known-clean UFS2 fixture: {:?}",
            result
                .errors
                .iter()
                .map(|e| (&e.code, &e.message))
                .collect::<Vec<_>>()
        );
    }

    /// Corrupt CG 0's bitmap so a known-allocated fragment looks free.
    /// The fsck's inode-allocated cross-check must surface
    /// `BitmapMissingAllocation`.
    #[test]
    fn fsck_flags_bitmap_missing_allocation() {
        let mut img = load_fixture("test_ufs1.img.zst");

        // Find the root dir's first allocated data fragment by walking
        // it through the clean view, then flip its bitmap bit to "free".
        let frag = {
            let mut fs = UfsFilesystem::open(Cursor::new(&img), 0).expect("open");
            let root = fs.read_inode(ROOT_INODE).expect("read root inode");
            let (data, _) = fs.walk_inode_blocks(&root).expect("walk root");
            *data
                .iter()
                .find(|&&f| f != 0)
                .expect("root has at least one data fragment")
        };

        // Re-open to learn CG 0's bitmap byte offset.
        let (bitmap_byte, in_cg) = {
            let fs = UfsFilesystem::open(Cursor::new(&img), 0).expect("open");
            let cg = frag / fs.fpg as u64;
            assert_eq!(cg, 0, "test assumes root is in CG 0");
            let in_cg = frag % fs.fpg as u64;
            // The bitmap lives at `cg_freeoff` from the CG header start;
            // the simplest way to get the byte offset is to ask read_cg_*
            // for it. Use the canonical formula: cg_header_offset + freeoff,
            // but we only need the resulting bitmap-byte position. Walking
            // the same path as `read_cg_free_bitmap` would re-validate the
            // header; we don't need to repeat that. Read freeoff directly.
            let cg_byte = fs.cg_header_offset(cg as u32);
            // freeoff lives at offset CG_OFF_FREEOFF in the header.
            let mut buf = [0u8; 4];
            buf.copy_from_slice(
                &img[cg_byte as usize + CG_OFF_FREEOFF..cg_byte as usize + CG_OFF_FREEOFF + 4],
            );
            let freeoff = u32::from_le_bytes(buf) as u64;
            (cg_byte + freeoff + in_cg / 8, in_cg % 8)
        };

        // Flip the bit ON (= free in UFS convention) while the inode
        // still claims it.
        img[bitmap_byte as usize] |= 1u8 << in_cg as u8;

        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("re-open");
        let result = fs.fsck().expect("UFS implements fsck").expect("fsck runs");
        assert!(
            result
                .errors
                .iter()
                .any(|e| e.code == "BitmapMissingAllocation"),
            "expected BitmapMissingAllocation, got: {:?}",
            result.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
        assert!(result.repairable);
    }

    /// Corrupt CG 0's replica SB magic so the fsck flags
    /// `ReplicaSbMagicMismatch`. UFS1 fixture has 1 CG → no replica
    /// distinct from primary, so this test uses a multi-CG fixture path
    /// by hand-rolling a 2-CG image instead. (Real makefs fixtures we
    /// have are 1-CG; the synthetic SB builder + a tiny replica suffices.)
    #[test]
    fn fsck_flags_replica_sb_magic_mismatch() {
        // Build a 2-CG synthetic UFS2 image. fpg=64, ipg=64. Total frags
        // = 128. bsize=8192, fsize=1024. sblkno=8 (placed inside the CG
        // header region for simplicity).
        let bsize = 8192u32;
        let fsize = 1024u32;
        let fpg = 64u32;
        let ipg = 64u32;
        let ncg = 2u32;
        let total_frags = (fpg as u64) * (ncg as u64);
        let sblkno = 8i32;
        let mut img = build_sb(
            UfsVersion::Ufs2,
            UfsEndian::Little,
            SB_OFFSET_UFS1,
            bsize,
            fsize,
            ncg,
            fpg,
            ipg,
            total_frags,
            b"",
            1,
            0,
        );
        // Patch sblkno into the SB so the parser captures it.
        img[SB_OFFSET_UFS1 as usize + OFF_SBLKNO..SB_OFFSET_UFS1 as usize + OFF_SBLKNO + 4]
            .copy_from_slice(&sblkno.to_le_bytes());

        // Grow the buffer to the size the SB claims.
        let needed = (total_frags as usize) * (fsize as usize);
        if img.len() < needed {
            img.resize(needed, 0);
        }

        // Stamp a CG header at the start of each CG. The cg layout the
        // bitmap reader inspects requires CG_MAGIC + cg_cgx + cg_ndblk +
        // valid freeoff/nextfreeoff. We carve a tiny header: magic at
        // byte 4, cg_cgx at byte 12, cg_ndblk at byte 20, iusedoff at
        // byte 92, freeoff at byte 96, nextfreeoff at byte 100.
        for cg in 0..ncg {
            let cg_byte = (cg as u64 * fpg as u64 + 24/*cblkno default from build_sb*/) as usize
                * fsize as usize;
            // Magic at byte 4.
            img[cg_byte + CG_OFF_MAGIC..cg_byte + CG_OFF_MAGIC + 4]
                .copy_from_slice(&CG_MAGIC.to_le_bytes());
            // cg_cgx at byte 12.
            img[cg_byte + CG_OFF_CGX..cg_byte + CG_OFF_CGX + 4].copy_from_slice(&cg.to_le_bytes());
            // cg_ndblk at byte 20 = fpg.
            img[cg_byte + CG_OFF_NDBLK..cg_byte + CG_OFF_NDBLK + 4]
                .copy_from_slice(&fpg.to_le_bytes());
            // iusedoff = 256 (somewhere past the fixed header), freeoff = 264,
            // nextfreeoff = 264 + ndblk/8.
            let iusedoff = 256u32;
            let freeoff = 264u32;
            let nextfreeoff = freeoff + fpg.div_ceil(8);
            img[cg_byte + CG_OFF_IUSEDOFF..cg_byte + CG_OFF_IUSEDOFF + 4]
                .copy_from_slice(&iusedoff.to_le_bytes());
            img[cg_byte + CG_OFF_FREEOFF..cg_byte + CG_OFF_FREEOFF + 4]
                .copy_from_slice(&freeoff.to_le_bytes());
            img[cg_byte + CG_OFF_NEXTFREEOFF..cg_byte + CG_OFF_NEXTFREEOFF + 4]
                .copy_from_slice(&nextfreeoff.to_le_bytes());
            // Bitmap: mark all free except a few overhead frags at start.
            for i in (freeoff as usize)..(nextfreeoff as usize) {
                img[cg_byte + i] = 0xFF;
            }
        }

        // Write a valid replica SB at CG 1's sblkno (mirror primary).
        let cg1_sb_byte = (fpg as u64 + sblkno as u64) as usize * fsize as usize;
        let primary_sb_bytes: Vec<u8> =
            img[SB_OFFSET_UFS1 as usize..SB_OFFSET_UFS1 as usize + SB_READ_SIZE].to_vec();
        if cg1_sb_byte + SB_READ_SIZE > img.len() {
            img.resize(cg1_sb_byte + SB_READ_SIZE, 0);
        }
        img[cg1_sb_byte..cg1_sb_byte + SB_READ_SIZE].copy_from_slice(&primary_sb_bytes);

        // Sanity: clean fsck before corruption.
        {
            let mut fs = UfsFilesystem::open(Cursor::new(&img), 0).expect("open clean");
            let result = fs.fsck().expect("supports fsck").expect("runs");
            // Geometry warnings are OK (this is a hand-rolled tiny image);
            // we only assert no ReplicaSbMagicMismatch fires on a clean replica.
            assert!(
                !result
                    .errors
                    .iter()
                    .any(|e| e.code == "ReplicaSbMagicMismatch"),
                "expected no ReplicaSbMagicMismatch on clean replica, got: {:?}",
                result.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
            );
        }

        // Corrupt the replica SB's magic.
        img[cg1_sb_byte + MAGIC_OFF..cg1_sb_byte + MAGIC_OFF + 4]
            .copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());

        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open corrupted");
        let result = fs.fsck().expect("supports fsck").expect("runs");
        assert!(
            result
                .errors
                .iter()
                .any(|e| e.code == "ReplicaSbMagicMismatch"),
            "expected ReplicaSbMagicMismatch, got: {:?}",
            result.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
        assert!(result.repairable);
    }

    // ---- U.4 repair() ----

    /// Repair clears the stray "free" bit that fsck flagged as
    /// `BitmapMissingAllocation` and the volume re-fscks clean. Uses the
    /// real UFS1 fixture so the post-repair fsck has the same hard
    /// expectation (`is_clean()`) the original `fixture_ufs1_fsck_clean`
    /// test does.
    #[test]
    fn repair_clears_bitmap_missing_allocation_and_refscks_clean() {
        let mut img = load_fixture("test_ufs1.img.zst");

        // Mirror `fsck_flags_bitmap_missing_allocation`: locate the root
        // dir's first allocated data fragment, then SET its bitmap bit
        // ("free" in UFS polarity) while the inode still claims it.
        let (bitmap_byte, in_cg) = {
            let mut fs = UfsFilesystem::open(Cursor::new(&img), 0).expect("open clean");
            let root = fs.read_inode(ROOT_INODE).expect("read root inode");
            let (data, _) = fs.walk_inode_blocks(&root).expect("walk root");
            let frag = *data
                .iter()
                .find(|&&f| f != 0)
                .expect("root has at least one data fragment");
            let cg = frag / fs.fpg as u64;
            assert_eq!(cg, 0, "test assumes root is in CG 0");
            let in_cg = frag % fs.fpg as u64;
            let cg_byte = fs.cg_header_offset(cg as u32);
            let mut buf = [0u8; 4];
            buf.copy_from_slice(
                &img[cg_byte as usize + CG_OFF_FREEOFF..cg_byte as usize + CG_OFF_FREEOFF + 4],
            );
            let freeoff = u32::from_le_bytes(buf) as u64;
            (cg_byte + freeoff + in_cg / 8, in_cg % 8)
        };
        img[bitmap_byte as usize] |= 1u8 << in_cg as u8;

        // Sanity: fsck flags the corruption.
        {
            let mut fs = UfsFilesystem::open(Cursor::new(&img), 0).expect("re-open");
            let pre = fs.fsck().expect("supports fsck").expect("runs");
            assert!(
                pre.errors
                    .iter()
                    .any(|e| e.code == "BitmapMissingAllocation"),
                "expected BitmapMissingAllocation before repair"
            );
        }

        // Repair on a writable cursor, then re-fsck.
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("re-open for repair");
        let report = fs.repair().expect("repair runs");
        assert!(
            report.fixes_applied.iter().any(|s| s.contains("Bitmap:")),
            "expected bitmap fix to be applied, got: {:?}",
            report.fixes_applied
        );
        let post = fs.fsck().expect("supports fsck").expect("runs");
        assert!(
            post.is_clean(),
            "fsck not clean after repair: {:?}",
            post.errors
                .iter()
                .map(|e| (&e.code, &e.message))
                .collect::<Vec<_>>()
        );
    }

    /// Repair adopts an orphan inode into `/lost+found/` and the volume
    /// re-fscks clean. The orphan is forged by allocating an inum,
    /// writing a valid mode-0o100644 inode at it, and never linking it
    /// from any directory — exactly what crash-recovered FS state looks
    /// like.
    #[test]
    fn repair_adopts_orphan_inode_into_lost_found_and_refscks_clean() {
        let img = load_fixture("test_ufs1.img.zst");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");

        // Forge an orphan: allocate inum + write a minimal valid inode.
        let orphan_inum = fs.alloc_inode(0).expect("alloc inode");
        let orphan = UfsInode {
            inum: orphan_inum,
            mode: 0o100644,
            nlink: 1,
            uid: 0,
            gid: 0,
            size: 0,
            mtime: 0,
            direct: [0; UFS_NDADDR],
            indirect: [0; UFS_NIADDR],
            inline_payload: Vec::new(),
        };
        fs.write_inode(orphan_inum, &orphan).expect("write inode");

        // Sanity: fsck flags the orphan.
        let pre = fs.fsck().expect("supports fsck").expect("runs");
        assert!(
            pre.errors.iter().any(|e| e.code == "OrphanInode"),
            "expected OrphanInode before repair, got: {:?}",
            pre.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
        assert!(
            pre.orphaned_entries
                .iter()
                .any(|o| o.id == orphan_inum as u64),
            "orphaned_entries did not include forged inum {orphan_inum}"
        );

        // Repair adopts and the volume goes clean.
        let report = fs.repair().expect("repair runs");
        assert!(
            report.fixes_applied.iter().any(|s| s.contains("Orphans:")),
            "expected orphan adoption, got: {:?}",
            report.fixes_applied
        );
        let post = fs.fsck().expect("supports fsck").expect("runs");
        assert!(
            post.is_clean(),
            "fsck not clean after orphan adoption: {:?}",
            post.errors
                .iter()
                .map(|e| (&e.code, &e.message))
                .collect::<Vec<_>>()
        );

        // The adopted name lives under `/lost+found/` and its inum matches.
        let root_entry = fs.root().expect("root");
        let kids = fs.list_directory(&root_entry).expect("ls /");
        let lf = kids
            .iter()
            .find(|c| c.name == "lost+found")
            .expect("/lost+found exists post-repair");
        let lf_kids = fs.list_directory(lf).expect("ls /lost+found");
        let adopted = lf_kids
            .iter()
            .find(|c| c.name == format!("ino_{orphan_inum}"))
            .expect("adopted dirent named after orphan inum");
        assert_eq!(adopted.location as u32, orphan_inum);
    }

    /// Repair rewrites a corrupted replica superblock from the primary
    /// and the (synthetic 2-CG) volume re-fscks without
    /// `ReplicaSb*Mismatch` codes. This synthetic image carries
    /// geometry-related warnings that the hand-rolled CG headers don't
    /// resolve (intentionally — it's a minimum-viable layout); we only
    /// assert that every replica-mismatch error is gone after repair.
    #[test]
    fn repair_rewrites_replica_sb_from_primary() {
        // Build a 2-CG synthetic UFS2 image — same shape as
        // `fsck_flags_replica_sb_magic_mismatch`.
        let bsize = 8192u32;
        let fsize = 1024u32;
        let fpg = 64u32;
        let ipg = 64u32;
        let ncg = 2u32;
        let total_frags = (fpg as u64) * (ncg as u64);
        let sblkno = 8i32;
        let mut img = build_sb(
            UfsVersion::Ufs2,
            UfsEndian::Little,
            SB_OFFSET_UFS1,
            bsize,
            fsize,
            ncg,
            fpg,
            ipg,
            total_frags,
            b"",
            1,
            0,
        );
        img[SB_OFFSET_UFS1 as usize + OFF_SBLKNO..SB_OFFSET_UFS1 as usize + OFF_SBLKNO + 4]
            .copy_from_slice(&sblkno.to_le_bytes());
        let needed = (total_frags as usize) * (fsize as usize);
        if img.len() < needed {
            img.resize(needed, 0);
        }
        for cg in 0..ncg {
            let cg_byte = (cg as u64 * fpg as u64 + 24) as usize * fsize as usize;
            img[cg_byte + CG_OFF_MAGIC..cg_byte + CG_OFF_MAGIC + 4]
                .copy_from_slice(&CG_MAGIC.to_le_bytes());
            img[cg_byte + CG_OFF_CGX..cg_byte + CG_OFF_CGX + 4].copy_from_slice(&cg.to_le_bytes());
            img[cg_byte + CG_OFF_NDBLK..cg_byte + CG_OFF_NDBLK + 4]
                .copy_from_slice(&fpg.to_le_bytes());
            let iusedoff = 256u32;
            let freeoff = 264u32;
            let nextfreeoff = freeoff + fpg.div_ceil(8);
            img[cg_byte + CG_OFF_IUSEDOFF..cg_byte + CG_OFF_IUSEDOFF + 4]
                .copy_from_slice(&iusedoff.to_le_bytes());
            img[cg_byte + CG_OFF_FREEOFF..cg_byte + CG_OFF_FREEOFF + 4]
                .copy_from_slice(&freeoff.to_le_bytes());
            img[cg_byte + CG_OFF_NEXTFREEOFF..cg_byte + CG_OFF_NEXTFREEOFF + 4]
                .copy_from_slice(&nextfreeoff.to_le_bytes());
            for i in (freeoff as usize)..(nextfreeoff as usize) {
                img[cg_byte + i] = 0xFF;
            }
        }
        // CG 1's replica starts as a verbatim copy of the primary.
        let cg1_sb_byte = (fpg as u64 + sblkno as u64) as usize * fsize as usize;
        let primary_sb_bytes: Vec<u8> =
            img[SB_OFFSET_UFS1 as usize..SB_OFFSET_UFS1 as usize + SB_READ_SIZE].to_vec();
        if cg1_sb_byte + SB_READ_SIZE > img.len() {
            img.resize(cg1_sb_byte + SB_READ_SIZE, 0);
        }
        img[cg1_sb_byte..cg1_sb_byte + SB_READ_SIZE].copy_from_slice(&primary_sb_bytes);

        // Corrupt: clobber the replica's magic AND its bsize so we see
        // multiple ReplicaSb*Mismatch findings and confirm repair rewrites
        // the whole 2 KiB image, not just one field.
        img[cg1_sb_byte + MAGIC_OFF..cg1_sb_byte + MAGIC_OFF + 4]
            .copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
        img[cg1_sb_byte + OFF_BSIZE..cg1_sb_byte + OFF_BSIZE + 4]
            .copy_from_slice(&0u32.to_le_bytes());

        // Pre-repair fsck should flag both.
        {
            let mut fs = UfsFilesystem::open(Cursor::new(&img), 0).expect("open pre-repair");
            let pre = fs.fsck().expect("supports fsck").expect("runs");
            assert!(
                pre.errors
                    .iter()
                    .any(|e| e.code == "ReplicaSbMagicMismatch"),
                "expected ReplicaSbMagicMismatch pre-repair"
            );
        }

        // Repair.
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open for repair");
        let report = fs.repair().expect("repair runs");
        assert!(
            report
                .fixes_applied
                .iter()
                .any(|s| s.contains("Replica superblock:")),
            "expected replica fix entry, got: {:?}",
            report.fixes_applied
        );

        // Post-repair: every ReplicaSb*Mismatch must be gone (other
        // hand-rolled-image findings are tolerated).
        let post = fs.fsck().expect("supports fsck").expect("runs");
        assert!(
            !post
                .errors
                .iter()
                .any(|e| e.code.starts_with("ReplicaSb") && e.code.ends_with("Mismatch")),
            "expected no ReplicaSb*Mismatch after repair, got: {:?}",
            post.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    #[test]
    fn synthetic_sparse_block_emits_zeros() {
        let img = build_default_sb(UfsVersion::Ufs1, UfsEndian::Little, SB_OFFSET_UFS1);
        let mut img = img;
        img.resize(2 * 1024 * 1024, 0);
        let fs = UfsFilesystem::open(Cursor::new(img.clone()), 0).expect("open");
        let inode_off = fs.inode_byte_offset(5) as usize;
        // Build a 4 KiB file with direct[0] = 0 (sparse hole).
        img[inode_off..inode_off + 2].copy_from_slice(&0o100644u16.to_le_bytes());
        let size: u64 = 4096;
        img[inode_off + D1_OFF_SIZE..inode_off + D1_OFF_SIZE + 8]
            .copy_from_slice(&size.to_le_bytes());
        // direct[0] stays zero.

        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("re-open");
        let inode = fs.read_inode(5).expect("read inode 5");
        let bytes = fs.read_inode_data(&inode, size, 4096).expect("read");
        assert_eq!(bytes.len(), 4096);
        assert!(bytes.iter().all(|&b| b == 0));
    }
}
