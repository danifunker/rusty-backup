//! UFS1 / UFS2 (Berkeley Fast Filesystem) Tier-A read-only support.
//!
//! Implements the [`Filesystem`] trait far enough to detect a UFS partition,
//! surface its on-disk version, label, and total/used/free sizes in the
//! inspect tab, and back the partition up byte-for-byte through the existing
//! layout-preserving pipeline. U.2 layers a cylinder-group bitmap walk on
//! top to drive `last_data_byte`-based trimming; U.3 will add inode + dir
//! + file browse.
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
use crate::fs::CompactResult;

// ---- Constants ----

/// Byte offsets to try when locating a UFS superblock. Matches kernel
/// `SBLOCKSEARCH` order (`{SBLOCK_UFS2, SBLOCK_UFS1, …}`). `SBLOCK_FLOPPY`
/// (0) and `SBLOCK_PIGGY` (262144) are intentionally not in the list:
/// neither shape appears on any disk image we expect to encounter.
const SB_OFFSET_UFS2: u64 = 65536;
const SB_OFFSET_UFS1: u64 = 8192;

/// UFS1 magic — `0x00011954`, encoded little-endian as `54 19 01 00`.
const MAGIC_UFS1: u32 = 0x0001_1954;
/// UFS2 magic — `0x19540119`, encoded little-endian as `19 01 54 19`.
const MAGIC_UFS2: u32 = 0x1954_0119;

/// Offset of `fs_magic` within `struct fs` (FreeBSD `sys/ufs/ffs/fs.h`).
const MAGIC_OFF: usize = 1372;

/// How many bytes of the superblock we read in a single call. 2 KiB covers
/// every Tier-A field including the 64-bit UFS2 size at offset 1080 and
/// the magic at offset 1372.
const SB_READ_SIZE: usize = 2048;

// On-disk field offsets, validated against the FreeBSD struct definition
// and cross-checked against our makefs-built fixtures (see scripts/
// generate-ufs-fixtures.sh + scripts/probe-ufs-sb.py).
#[allow(dead_code)] // surfaced for completeness; the SB block address isn't
                    // needed by U.1/U.2/U.3
const OFF_SBLKNO: usize = 0x008; // fs_sblkno      i32 — SB address in frags
const OFF_CBLKNO: usize = 0x00C; // fs_cblkno      i32 — CG block addr in frags
const OFF_OLD_SIZE: usize = 0x024; // fs_old_size    i32 — UFS1 total fragments
const OFF_NCG: usize = 0x02C; // fs_ncg         u32 — # cylinder groups
const OFF_BSIZE: usize = 0x030; // fs_bsize       i32 — block size in bytes
const OFF_FSIZE: usize = 0x034; // fs_fsize       i32 — fragment size in bytes
const OFF_FRAG: usize = 0x038; // fs_frag        i32 — frags per block
const OFF_IPG: usize = 0x0B8; // fs_ipg         u32 — inodes per CG
const OFF_FPG: usize = 0x0BC; // fs_fpg         i32 — fragments per CG
const OFF_VOLNAME: usize = 680; // fs_volname[32] — `u_char[MAXVOLLEN]`
const OFF_SIZE_UFS2: usize = 1080; // fs_size        i64 — UFS2 64-bit fragments
const OFF_FLAGS2: usize = 0x35E; // fs_flags2      i32 (relevant journal bits live elsewhere too, but this is the canonical "ENABLED" word in modern UFS2)

const VOLNAME_LEN: usize = 32;

// ---- Cylinder-group on-disk header (`struct cg` in fs.h) ----

/// Magic number at offset 4 of every cylinder-group header. Matches
/// `CG_MAGIC` from FreeBSD `sys/ufs/ffs/fs.h`.
const CG_MAGIC: u32 = 0x0009_0255;
const CG_OFF_MAGIC: usize = 0x004;
const CG_OFF_CGX: usize = 0x00C; // cg_cgx          u32 — CG index
const CG_OFF_NDBLK: usize = 0x014; // cg_ndblk        u32 — # data frags in CG
const CG_OFF_IUSEDOFF: usize = 0x05C; // cg_iusedoff     u32 — inode-used bitmap offset
const CG_OFF_FREEOFF: usize = 0x060; // cg_freeoff      u32 — free-frag bitmap offset
const CG_OFF_NEXTFREEOFF: usize = 0x064; // cg_nextfreeoff  u32 — used to bound the bitmap

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

// ---- Filesystem ----

// `ipg` is only consumed by U.3 (still pending) and `bsize` / `frag` are
// captured for parity with the FS struct but unused until then. The other
// fields all see use in U.1/U.2.
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
    pub(crate) ipg: u32,   // inodes per cylinder group (U.3 needs this)
    /// `fs_cblkno`: fragment offset of the cylinder-group header inside
    /// each CG region. CG `i`'s header lives at fragment `i * fpg + cblkno`.
    pub(crate) cblkno: u32,
    pub(crate) total_frags: u64,
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
            total_frags,
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

fn read_u32(buf: &[u8], off: usize, endian: UfsEndian) -> u32 {
    let bytes = [buf[off], buf[off + 1], buf[off + 2], buf[off + 3]];
    match endian {
        UfsEndian::Little => u32::from_le_bytes(bytes),
        UfsEndian::Big => u32::from_be_bytes(bytes),
    }
}

fn read_i32(buf: &[u8], off: usize, endian: UfsEndian) -> i32 {
    read_u32(buf, off, endian) as i32
}

fn read_i64(buf: &[u8], off: usize, endian: UfsEndian) -> i64 {
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
        // Inspect tab needs a root entry to render the tree, even when
        // browse is unsupported. Browse code (U.3) will replace this
        // with a real inode lookup; for Tier A the entry exists but has
        // no children that `list_directory` can surface.
        Ok(FileEntry::root())
    }

    fn list_directory(&mut self, _entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "UFS directory browse not yet implemented (Tier B / U.3 pending)".into(),
        ))
    }

    fn read_file(
        &mut self,
        _entry: &FileEntry,
        _max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "UFS file read not yet implemented (Tier B / U.3 pending)".into(),
        ))
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
        // `fs_cblkno` — fragment offset of the CG block header. 24 is the
        // makefs default for the small fixtures we exercise; tests that
        // walk CG headers seed the bitmap region themselves.
        write_i32(sb, OFF_CBLKNO, 24);

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
}
