//! XFS v4 superblock parser. v5/CRC and realtime-device filesystems are
//! rejected with a clear error per the Step 5 plan.

use byteorder::{BigEndian, ByteOrder};

use super::types::{
    DirFormat, XFS_LABEL_MAX, XFS_SB_MAGIC, XFS_SB_VERSION_5, XFS_SB_VERSION_DIRV2BIT,
    XFS_SB_VERSION_NUMBITS,
};
use crate::fs::filesystem::FilesystemError;

/// Parsed XFS superblock. Only the fields Step 5 needs to look up the root
/// inode (and a few cosmetic fields for `fs_type` / `volume_label`).
#[derive(Debug, Clone)]
pub struct XfsSuperblock {
    pub magicnum: u32,
    pub blocksize: u32,
    pub dblocks: u64,
    pub rblocks: u64,
    pub rextents: u64,
    pub rootino: u64,
    pub agblocks: u32,
    pub agcount: u32,
    pub versionnum: u16,
    pub sectsize: u16,
    pub inodesize: u16,
    pub inopblock: u16,
    pub fname: [u8; XFS_LABEL_MAX],
    pub blocklog: u8,
    pub sectlog: u8,
    pub inodelog: u8,
    pub inopblog: u8,
    pub agblklog: u8,
    pub dirblklog: u8,
    pub features2: u32,
    pub features_compat: u32,
    pub features_ro_compat: u32,
    pub features_incompat: u32,
}

impl XfsSuperblock {
    /// Parse from a buffer that holds at least the first 224 bytes of the
    /// XFS superblock (we read a 512-byte sector to stay raw-device safe).
    pub fn parse(buf: &[u8]) -> Result<Self, FilesystemError> {
        if buf.len() < 224 {
            return Err(FilesystemError::Parse(format!(
                "XFS superblock buffer too small: {} bytes",
                buf.len()
            )));
        }
        let magicnum = BigEndian::read_u32(&buf[0..4]);
        if magicnum != XFS_SB_MAGIC {
            return Err(FilesystemError::Parse(format!(
                "bad XFS magic: 0x{magicnum:08X} (expected 0x{XFS_SB_MAGIC:08X})"
            )));
        }
        let versionnum = BigEndian::read_u16(&buf[100..102]);
        let version = versionnum & XFS_SB_VERSION_NUMBITS;
        if version == XFS_SB_VERSION_5 {
            return Err(FilesystemError::Unsupported(
                "v5 XFS (CRC-enabled) not supported — IRIX disks are v4".into(),
            ));
        }
        let rextents = BigEndian::read_u64(&buf[24..32]);
        if rextents > 0 {
            return Err(FilesystemError::Unsupported(
                "realtime-device XFS volumes are out of scope".into(),
            ));
        }
        let blocksize = BigEndian::read_u32(&buf[4..8]);
        if !blocksize.is_power_of_two() || !(512..=65536).contains(&blocksize) {
            return Err(FilesystemError::Parse(format!(
                "XFS sb_blocksize {blocksize} is not a power of two in [512, 65536]"
            )));
        }
        let sectsize = BigEndian::read_u16(&buf[102..104]);
        if sectsize < 512 {
            return Err(FilesystemError::Parse(format!(
                "XFS sb_sectsize {sectsize} < 512"
            )));
        }

        let mut fname = [0u8; XFS_LABEL_MAX];
        fname.copy_from_slice(&buf[108..108 + XFS_LABEL_MAX]);

        // Features 2 lives at offset 200 (be32). v4 sbs zero the v5-only
        // feature-mask words; we still parse them so callers can log them
        // for diagnostics.
        let features2 = BigEndian::read_u32(&buf[200..204]);
        let features_compat = BigEndian::read_u32(&buf[208..212]);
        let features_ro_compat = BigEndian::read_u32(&buf[212..216]);
        let features_incompat = BigEndian::read_u32(&buf[216..220]);

        Ok(XfsSuperblock {
            magicnum,
            blocksize,
            dblocks: BigEndian::read_u64(&buf[8..16]),
            rblocks: BigEndian::read_u64(&buf[16..24]),
            rextents,
            rootino: BigEndian::read_u64(&buf[56..64]),
            agblocks: BigEndian::read_u32(&buf[84..88]),
            agcount: BigEndian::read_u32(&buf[88..92]),
            versionnum,
            sectsize,
            inodesize: BigEndian::read_u16(&buf[104..106]),
            inopblock: BigEndian::read_u16(&buf[106..108]),
            fname,
            blocklog: buf[120],
            sectlog: buf[121],
            inodelog: buf[122],
            inopblog: buf[123],
            agblklog: buf[124],
            dirblklog: buf[192],
            features2,
            features_compat,
            features_ro_compat,
            features_incompat,
        })
    }

    /// True iff the DIRV2 bit is set (dir2 layout).
    pub fn is_dir2(&self) -> bool {
        self.versionnum & XFS_SB_VERSION_DIRV2BIT != 0
    }

    pub fn dir_format(&self) -> DirFormat {
        if self.is_dir2() {
            DirFormat::Dir2
        } else {
            DirFormat::Dir1
        }
    }

    /// Block size of a directory block in bytes (`blocksize << dirblklog`).
    pub fn dirblksize(&self) -> u32 {
        self.blocksize << self.dirblklog
    }

    /// True iff the FTYPE feature is enabled in `sb_features2`. FTYPE adds a
    /// 1-byte file-type field to every directory entry — shortform entries
    /// store it right after the inumber, data-block entries store it right
    /// after the name (before the alignment padding and the trailing tag).
    /// Modern `mkfs.xfs` defaults to enabling FTYPE even on v4 superblocks.
    pub fn has_ftype(&self) -> bool {
        // XFS_SB_VERSION2_FTYPE — see libxfs/xfs_format.h.
        const XFS_SB_VERSION2_FTYPE: u32 = 0x0000_0200;
        (self.features2 & XFS_SB_VERSION2_FTYPE) != 0
    }

    /// Volume label, trimmed of trailing NULs.
    pub fn label(&self) -> String {
        let end = self
            .fname
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(self.fname.len());
        String::from_utf8_lossy(&self.fname[..end])
            .trim_matches(|c: char| c == ' ' || c == '\0')
            .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal v4 superblock buffer for the rejection-path tests.
    /// Returns 512 bytes (one sector). All required guards key off the first
    /// 224 bytes.
    fn build_minimal_sb(versionnum: u16, rextents: u64) -> [u8; 512] {
        let mut buf = [0u8; 512];
        BigEndian::write_u32(&mut buf[0..4], XFS_SB_MAGIC);
        BigEndian::write_u32(&mut buf[4..8], 4096); // blocksize
        BigEndian::write_u64(&mut buf[24..32], rextents);
        BigEndian::write_u16(&mut buf[100..102], versionnum);
        BigEndian::write_u16(&mut buf[102..104], 512); // sectsize
        BigEndian::write_u16(&mut buf[104..106], 256); // inodesize
        BigEndian::write_u16(&mut buf[106..108], 16); // inopblock
        buf[120] = 12; // blocklog
        buf[121] = 9; // sectlog
        buf[122] = 8; // inodelog
        buf[123] = 4; // inopblog
        buf[124] = 16; // agblklog
        buf
    }

    #[test]
    fn rejects_wrong_magic() {
        let mut buf = [0u8; 512];
        buf[0..4].copy_from_slice(&0xDEAD_BEEFu32.to_be_bytes());
        match XfsSuperblock::parse(&buf) {
            Err(FilesystemError::Parse(msg)) => assert!(msg.contains("magic")),
            other => panic!("expected Parse error, got {other:?}"),
        }
    }

    #[test]
    fn rejects_v5_crc_filesystem() {
        // versionnum low nibble = 5 → v5/CRC. DIRV2 bit set or not; doesn't matter.
        let buf = build_minimal_sb(0x0005, 0);
        match XfsSuperblock::parse(&buf) {
            Err(FilesystemError::Unsupported(msg)) => assert!(msg.contains("v5")),
            other => panic!("expected Unsupported v5 error, got {other:?}"),
        }
    }

    #[test]
    fn rejects_realtime_volume() {
        // Valid v4 + DIRV2 but rextents != 0.
        let buf = build_minimal_sb(0x2004, 1);
        match XfsSuperblock::parse(&buf) {
            Err(FilesystemError::Unsupported(msg)) => assert!(msg.contains("realtime")),
            other => panic!("expected Unsupported realtime error, got {other:?}"),
        }
    }

    #[test]
    fn accepts_minimal_v4_dir2() {
        // Valid v4 + DIRV2, rextents = 0.
        let buf = build_minimal_sb(0x2004, 0);
        let sb = XfsSuperblock::parse(&buf).expect("parse");
        assert_eq!(sb.magicnum, XFS_SB_MAGIC);
        assert_eq!(sb.blocksize, 4096);
        assert!(sb.is_dir2());
        assert_eq!(sb.dir_format(), DirFormat::Dir2);
        assert_eq!(sb.dirblksize(), 4096);
    }

    #[test]
    fn detects_dir1_format() {
        // versionnum = 4, DIRV2 clear → dir1.
        let buf = build_minimal_sb(0x0004, 0);
        let sb = XfsSuperblock::parse(&buf).expect("parse");
        assert!(!sb.is_dir2());
        assert_eq!(sb.dir_format(), DirFormat::Dir1);
    }
}
