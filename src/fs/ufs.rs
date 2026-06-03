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

use std::io::{Read, Seek, SeekFrom};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};
use super::unix_common::bitmap::BitmapReader;
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
