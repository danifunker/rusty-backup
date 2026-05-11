//! SGI / IRIX XFS v4 read-only support — Step 5 (superblock + inode lookup).
//!
//! Step 5 surfaces only enough of the on-disk format to verify the inode
//! arithmetic against a real image: parse the superblock with v5/realtime
//! guards, log the dir1/dir2 directory format, read the root inode, and
//! report `fs_type`/`volume_label`/`total_size`. Directory listing and file
//! reads return `Unsupported` and are filled in by Step 6 (dir2) and Step
//! 6b (dir1).
//!
//! References:
//! - `~/xfs-efs/refs/xfs-modern/xfs/libxfs/xfs_format.h`
//! - "XFS Algorithms & Data Structures", 3rd ed. (SGI).
//! - `docs/SGI_Filesystems.md` Step 5.

pub mod ag;
pub mod inode;
pub mod sb;
pub mod types;

use std::io::{Read, Seek, SeekFrom};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{Filesystem, FilesystemError};
use inode::{inode_byte_offset, XfsDinodeCore};
use sb::XfsSuperblock;
use types::DirFormat;

const SECTOR: u64 = 512;

/// XFS filesystem reader. Holds the superblock and a seekable backing
/// reader; everything else is computed on demand.
pub struct XfsFilesystem<R: Read + Seek + Send> {
    reader: R,
    partition_offset: u64,
    sb: XfsSuperblock,
    dir_format: DirFormat,
    label: String,
    fs_type_name: &'static str,
}

impl<R: Read + Seek + Send> XfsFilesystem<R> {
    /// Open an XFS partition at `partition_offset` within `reader`.
    ///
    /// Parses the superblock at byte 0 of the partition, applies the v5/
    /// realtime/blocksize guards from the plan, decodes the dir1/dir2
    /// directory format, and logs which format was detected so it shows up
    /// in the inspect tab and CLI logs.
    pub fn open(reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        Self::open_with_log(reader, partition_offset, |_| {})
    }

    pub fn open_with_log<F: FnMut(&str)>(
        mut reader: R,
        partition_offset: u64,
        mut log_cb: F,
    ) -> Result<Self, FilesystemError> {
        // Read one sector at the partition start. The XFS superblock fits
        // inside the first sector for every sane sb_sectsize, and we only
        // consult the first 224 bytes during parsing.
        let mut sector = [0u8; SECTOR as usize];
        read_at_aligned(&mut reader, partition_offset, SECTOR, &mut sector)?;
        let sb = XfsSuperblock::parse(&sector)?;

        let dir_format = sb.dir_format();
        let fs_type_name = match sb.versionnum & types::XFS_SB_VERSION_NUMBITS {
            1 => "XFS v1",
            _ => "XFS v2",
        };
        log_cb(&format!(
            "XFS dir format: {}",
            match dir_format {
                DirFormat::Dir1 => "dir1",
                DirFormat::Dir2 => "dir2",
            }
        ));

        let label = sb.label();
        Ok(XfsFilesystem {
            reader,
            partition_offset,
            sb,
            dir_format,
            label,
            fs_type_name,
        })
    }

    /// Read an inode by inumber. Returns the parsed v1/v2 core. The caller
    /// is responsible for interpreting `format` and reading the fork.
    pub fn read_inode(&mut self, ino: u64) -> Result<XfsDinodeCore, FilesystemError> {
        let part_off = inode_byte_offset(
            ino,
            self.sb.agblocks,
            self.sb.agblklog,
            self.sb.inopblog,
            self.sb.blocksize,
            self.sb.inodesize,
        );
        // Round to the sector containing the inode, slice it out. Inodes are
        // always at least 256 bytes (sectsize >= 512 → inopblock <= 2), so a
        // single sector covers the core; for inodesize >= 512 we may need
        // more — read enough sectors to cover the inode.
        let isz = self.sb.inodesize as u64;
        let sector_start = (part_off / SECTOR) * SECTOR;
        let span_end = part_off + isz;
        let span_len = (span_end - sector_start).div_ceil(SECTOR) * SECTOR;
        let mut buf = vec![0u8; span_len as usize];
        read_at_aligned(
            &mut self.reader,
            self.partition_offset + sector_start,
            span_len,
            &mut buf,
        )?;
        let off_in = (part_off - sector_start) as usize;
        XfsDinodeCore::parse(ino, &buf[off_in..off_in + isz as usize])
    }

    pub fn dir_format(&self) -> DirFormat {
        self.dir_format
    }

    pub fn superblock(&self) -> &XfsSuperblock {
        &self.sb
    }
}

impl<R: Read + Seek + Send> Filesystem for XfsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        let rootino = self.sb.rootino;
        let ino = self.read_inode(rootino)?;
        if !ino.is_dir() {
            return Err(FilesystemError::InvalidData(format!(
                "XFS root inode {rootino} is not a directory (mode=0o{:o})",
                ino.mode
            )));
        }
        Ok(FileEntry {
            name: "/".into(),
            path: "/".into(),
            entry_type: EntryType::Directory,
            size: ino.size,
            location: rootino,
            modified: None,
            type_code: None,
            creator_code: None,
            symlink_target: None,
            special_type: None,
            mode: Some(ino.mode as u32),
            uid: Some(ino.uid),
            gid: Some(ino.gid),
            resource_fork_size: None,
            aux_type: None,
            link_target_cnid: None,
        })
    }

    fn list_directory(&mut self, _entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        Err(FilesystemError::Unsupported(match self.dir_format {
            DirFormat::Dir2 => "XFS dir2 listing not yet implemented (Step 6)".into(),
            DirFormat::Dir1 => "XFS dir1 listing not yet implemented (Step 6b)".into(),
        }))
    }

    fn read_file(
        &mut self,
        _entry: &FileEntry,
        _max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "XFS file reads not yet implemented (Step 6)".into(),
        ))
    }

    fn volume_label(&self) -> Option<&str> {
        if self.label.is_empty() {
            None
        } else {
            Some(&self.label)
        }
    }

    fn fs_type(&self) -> &str {
        self.fs_type_name
    }

    fn total_size(&self) -> u64 {
        self.sb.dblocks * self.sb.blocksize as u64
    }

    fn used_size(&self) -> u64 {
        self.total_size()
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.total_size())
    }
}

/// Sector-aligned read helper. Reads `len` bytes starting at byte offset
/// `byte_off` into `out`. Both `byte_off` and `len` must be 512-byte
/// aligned. A short read (truncated fixture, damaged disk) is reported as
/// `Io(UnexpectedEof)` so callers can convert it into a user-visible
/// "truncated" message rather than panicking.
fn read_at_aligned<R: Read + Seek>(
    reader: &mut R,
    byte_off: u64,
    len: u64,
    out: &mut [u8],
) -> Result<(), FilesystemError> {
    debug_assert!(byte_off.is_multiple_of(SECTOR));
    debug_assert!(len.is_multiple_of(SECTOR));
    let len = len as usize;
    if out.len() < len {
        return Err(FilesystemError::InvalidData(format!(
            "XFS read_at_aligned: output buffer {} < {len}",
            out.len()
        )));
    }
    reader.seek(SeekFrom::Start(byte_off))?;
    let mut filled = 0;
    while filled < len {
        let n = reader.read(&mut out[filled..len])?;
        if n == 0 {
            return Err(FilesystemError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("XFS short read at byte {byte_off}: got {filled} of {len}"),
            )));
        }
        filled += n;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn load_fixture() -> Vec<u8> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/sgi/xfs_v2_dir2_small.img.zst");
        let compressed = std::fs::read(&path).expect("fixture present");
        let mut decoder =
            zstd::stream::read::Decoder::new(Cursor::new(compressed)).expect("zstd decoder");
        let mut out = Vec::new();
        decoder.read_to_end(&mut out).expect("decompress");
        out
    }

    #[test]
    fn parses_dir2_fixture_superblock() {
        let img = load_fixture();
        let fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        // Cross-checked against `xfs_db -r ... -c 'sb 0' -c 'p'`.
        assert_eq!(fs.sb.blocksize, 4096);
        assert_eq!(fs.sb.dblocks, 131_072);
        assert_eq!(fs.sb.rootino, 128);
        assert_eq!(fs.sb.agblocks, 65_536);
        assert_eq!(fs.sb.agcount, 2);
        assert_eq!(fs.sb.versionnum, 0xb4a4);
        assert_eq!(fs.sb.sectsize, 512);
        assert_eq!(fs.sb.inodesize, 256);
        assert_eq!(fs.sb.inopblock, 16);
        assert_eq!(fs.sb.blocklog, 12);
        assert_eq!(fs.sb.inopblog, 4);
        assert_eq!(fs.sb.agblklog, 16);
        assert_eq!(fs.sb.dirblklog, 0);
        assert_eq!(fs.dir_format, DirFormat::Dir2);
        assert_eq!(fs.fs_type(), "XFS v2");
        assert_eq!(fs.volume_label(), Some("RUSTYTEST"));
        assert_eq!(fs.total_size(), 131_072u64 * 4096);
    }

    #[test]
    fn root_inode_is_directory() {
        let img = load_fixture();
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        let root = fs.root().expect("root");
        assert!(root.is_directory());
        assert_eq!(root.location, 128);
        // di_mode upper nibble = 4 → directory.
        let mode = root.mode.expect("mode");
        assert_eq!(mode & 0o170000, 0o040000);
    }

    #[test]
    fn list_directory_returns_unsupported_for_dir2() {
        let img = load_fixture();
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        let root = fs.root().expect("root");
        match fs.list_directory(&root) {
            Err(FilesystemError::Unsupported(msg)) => assert!(msg.contains("Step 6")),
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn truncated_image_produces_unexpected_eof() {
        // The plan requires short reads past EOF to surface as
        // Io(UnexpectedEof), not panics. Slice the fixture down so the
        // superblock read past byte 0 already fails.
        let img = load_fixture();
        let truncated: Vec<u8> = img[..256].to_vec();
        match XfsFilesystem::open(Cursor::new(truncated), 0) {
            Err(FilesystemError::Io(e)) => {
                assert_eq!(e.kind(), std::io::ErrorKind::UnexpectedEof);
            }
            Err(e) => panic!("expected UnexpectedEof, got error {e}"),
            Ok(_) => panic!("expected UnexpectedEof, got Ok"),
        }
    }

    #[test]
    fn rejects_v5_filesystem_in_open() {
        // Build a 512-byte sector with XFSB magic but versionnum lower nibble = 5.
        let mut img = vec![0u8; 4096];
        img[0..4].copy_from_slice(&0x5846_5342u32.to_be_bytes());
        img[4..8].copy_from_slice(&4096u32.to_be_bytes());
        img[100..102].copy_from_slice(&0x0005u16.to_be_bytes());
        img[102..104].copy_from_slice(&512u16.to_be_bytes());
        match XfsFilesystem::open(Cursor::new(img), 0) {
            Err(FilesystemError::Unsupported(msg)) => assert!(msg.contains("v5")),
            Err(e) => panic!("expected v5 rejection, got error {e}"),
            Ok(_) => panic!("expected v5 rejection, got Ok"),
        }
    }
}
