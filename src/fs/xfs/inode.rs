//! XFS inode core parser and inode-number → byte-offset translation.
//!
//! Layout reference: `~/xfs-efs/refs/xfs-modern/xfs/libxfs/xfs_format.h`,
//! `struct xfs_dinode`. The first 96 bytes (the v1/v2 core: mode, size,
//! format, nextents, gen, ...) are byte-identical between v4 and v5/v3 inodes.
//! v5/v3 adds a 76-byte CRC trailer (crc, changecount, lsn, flags2,
//! cowextsize, pad, crtime, inumber, uuid) before the fork area, so v5 forks
//! start at offset 176 instead of 100. `fork_offset` returns the right value.

use byteorder::{BigEndian, ByteOrder};

use super::types::{DiFormat, XFS_DINODE_MAGIC};
use crate::fs::filesystem::FilesystemError;

/// Parsed v1/v2 inode core. Big-endian on disk.
#[derive(Debug, Clone)]
pub struct XfsDinodeCore {
    pub ino: u64,
    pub magic: u16,
    pub mode: u16,
    pub version: u8,
    pub format: DiFormat,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u32,
    pub size: u64,
    pub nblocks: u64,
    pub nextents: u32,
    pub forkoff: u8,
    pub aformat: u8,
    pub flags: u16,
    pub gen: u32,
}

impl XfsDinodeCore {
    /// Parse the first 96+ bytes of an on-disk inode.
    pub fn parse(ino: u64, buf: &[u8]) -> Result<Self, FilesystemError> {
        if buf.len() < 96 {
            return Err(FilesystemError::Parse(format!(
                "XFS dinode buffer too small: {} bytes",
                buf.len()
            )));
        }
        let magic = BigEndian::read_u16(&buf[0..2]);
        if magic != XFS_DINODE_MAGIC {
            return Err(FilesystemError::Parse(format!(
                "bad XFS dinode magic at ino {ino}: 0x{magic:04X} (expected 0x{XFS_DINODE_MAGIC:04X})"
            )));
        }
        Ok(XfsDinodeCore {
            ino,
            magic,
            mode: BigEndian::read_u16(&buf[2..4]),
            version: buf[4],
            format: DiFormat::from_raw(buf[5]),
            // bytes 6..8: di_metatype / former di_onlink — unused in v4.
            uid: BigEndian::read_u32(&buf[8..12]),
            gid: BigEndian::read_u32(&buf[12..16]),
            nlink: BigEndian::read_u32(&buf[16..20]),
            // bytes 20..32: projid, v2_pad, flushiter — diagnostic only.
            // bytes 32..56: atime/mtime/ctime — not surfaced yet.
            size: BigEndian::read_u64(&buf[56..64]),
            nblocks: BigEndian::read_u64(&buf[64..72]),
            // bytes 72..76: extsize.
            nextents: BigEndian::read_u32(&buf[76..80]),
            // bytes 80..82: anextents.
            forkoff: buf[82],
            aformat: buf[83],
            // bytes 84..88: dmevmask; 88..90: dmstate.
            flags: BigEndian::read_u16(&buf[90..92]),
            gen: BigEndian::read_u32(&buf[92..96]),
        })
    }

    pub fn is_dir(&self) -> bool {
        (self.mode & 0o170000) == 0o040000
    }

    pub fn is_regular(&self) -> bool {
        (self.mode & 0o170000) == 0o100000
    }

    pub fn is_symlink(&self) -> bool {
        (self.mode & 0o170000) == 0o120000
    }
}

/// Translate an XFS inode number into a byte offset relative to the start of
/// the partition.
///
/// `agno = ino >> (agblklog + inopblog)`
/// `agbno = (ino >> inopblog) & ((1 << agblklog) - 1)`
/// `offset = ino & ((1 << inopblog) - 1)`
/// `byte = (agno * agblocks + agbno) * blocksize + offset * inodesize`
pub fn inode_byte_offset(
    ino: u64,
    agblocks: u32,
    agblklog: u8,
    inopblog: u8,
    blocksize: u32,
    inodesize: u16,
) -> u64 {
    let agno = ino >> (agblklog + inopblog);
    let agmask = (1u64 << agblklog) - 1;
    let agbno = (ino >> inopblog) & agmask;
    let inomask = (1u64 << inopblog) - 1;
    let off = ino & inomask;
    (agno * agblocks as u64 + agbno) * blocksize as u64 + off * inodesize as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_inode_offset_matches_xfs_db() {
        // Fixture xfs_v2_dir2_small.img: rootino = 128, agblocks = 65536,
        // agblklog = 16, inopblog = 4, blocksize = 4096, inodesize = 256.
        //   agno = 128 >> 20 = 0
        //   agbno = (128 >> 4) & 0xFFFF = 8
        //   off = 128 & 0xF = 0
        //   byte = (0 * 65536 + 8) * 4096 + 0 * 256 = 32768 = 0x8000
        let off = inode_byte_offset(128, 65536, 16, 4, 4096, 256);
        assert_eq!(off, 0x8000);
    }

    #[test]
    fn sgi_real_disk_root_offset_matches_doc() {
        // SGI_imaged-001.sample.hda parameters: agblocks=254839, agblklog=18,
        // inopblog=4, blocksize=4096, inodesize=256, rootino=128.
        //   agno = 128 >> 22 = 0
        //   agbno = (128 >> 4) & 0x3FFFF = 8
        //   off = 0
        //   byte (within partition) = 8 * 4096 = 0x8000
        let off = inode_byte_offset(128, 254839, 18, 4, 4096, 256);
        assert_eq!(off, 0x8000);
    }
}
