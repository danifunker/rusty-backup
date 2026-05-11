//! SGI / IRIX XFS v4 read-only support.
//!
//! Step 5 brought up superblock + inode lookup. Step 6 adds dir2 shortform
//! and single-block directory listing plus extent-list file reads — enough
//! to browse and extract `xfs_v2_dir2_small.img` and any IRIX dir2 disk
//! whose directories fit in shortform or one block. dir1 disks are covered
//! by Step 6b; leaf/node dir2 and btree-format extents are deferred to
//! Step 7.
//!
//! References:
//! - `~/xfs-efs/refs/xfs-modern/xfs/libxfs/xfs_format.h`
//! - `~/xfs-efs/refs/xfs-modern/xfs/libxfs/xfs_da_format.h`
//! - `~/xfs-efs/refs/xfs-modern/xfs/libxfs/xfs_bmap_btree.c`
//! - "XFS Algorithms & Data Structures", 3rd ed. (SGI).
//! - `docs/SGI_Filesystems.md` Steps 5 and 6.

pub mod ag;
pub mod bmap;
pub mod dir2;
pub mod inode;
pub mod sb;
pub mod symlink;
pub mod types;

use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{Filesystem, FilesystemError};
use bmap::{decode_extent, fsblock_to_partition_byte, XfsBmbtIrec};
use inode::{inode_byte_offset, XfsDinodeCore};
use sb::XfsSuperblock;
use types::{DiFormat, DirFormat};

const SECTOR: u64 = 512;

/// Offset of the data fork within a v1/v2 on-disk inode (= offsetof(di_crc)).
const V2_FORK_OFF: usize = 100;

/// 1 MiB streaming chunk size — matches the rest of the codebase.
const STREAM_CHUNK: usize = 1024 * 1024;

/// XFS filesystem reader.
pub struct XfsFilesystem<R: Read + Seek + Send> {
    reader: R,
    partition_offset: u64,
    sb: XfsSuperblock,
    dir_format: DirFormat,
    label: String,
    fs_type_name: &'static str,
}

impl<R: Read + Seek + Send> XfsFilesystem<R> {
    pub fn open(reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        Self::open_with_log(reader, partition_offset, |_| {})
    }

    pub fn open_with_log<F: FnMut(&str)>(
        mut reader: R,
        partition_offset: u64,
        mut log_cb: F,
    ) -> Result<Self, FilesystemError> {
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

    pub fn dir_format(&self) -> DirFormat {
        self.dir_format
    }

    pub fn superblock(&self) -> &XfsSuperblock {
        &self.sb
    }

    /// Read a full on-disk inode buffer (sized to `sb.inodesize`). Returns
    /// the parsed core and the raw bytes so callers can also slice into the
    /// fork without a second seek.
    fn read_inode_buf(&mut self, ino: u64) -> Result<(XfsDinodeCore, Vec<u8>), FilesystemError> {
        let part_off = inode_byte_offset(
            ino,
            self.sb.agblocks,
            self.sb.agblklog,
            self.sb.inopblog,
            self.sb.blocksize,
            self.sb.inodesize,
        );
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
        let inode_buf = buf[off_in..off_in + isz as usize].to_vec();
        let core = XfsDinodeCore::parse(ino, &inode_buf)?;
        Ok((core, inode_buf))
    }

    pub fn read_inode(&mut self, ino: u64) -> Result<XfsDinodeCore, FilesystemError> {
        Ok(self.read_inode_buf(ino)?.0)
    }

    /// Slice the data-fork bytes out of a full inode buffer. v1/v2 layout:
    ///   data fork starts at offset 100 (V2_FORK_OFF). If `di_forkoff > 0`,
    ///   data fork ends at offset `100 + (forkoff << 3)`; otherwise it
    ///   consumes the rest of the literal area.
    fn data_fork<'a>(&self, core: &XfsDinodeCore, inode_buf: &'a [u8]) -> &'a [u8] {
        let end = if core.forkoff > 0 {
            V2_FORK_OFF + (core.forkoff as usize) * 8
        } else {
            self.sb.inodesize as usize
        };
        let end = end.min(inode_buf.len());
        &inode_buf[V2_FORK_OFF..end]
    }

    /// Decode `di_nextents` inline extent records from a fork buffer, sorted
    /// by file offset.
    fn decode_inline_extents(
        &self,
        fork: &[u8],
        n: usize,
    ) -> Result<Vec<XfsBmbtIrec>, FilesystemError> {
        let need = n.saturating_mul(16);
        if need > fork.len() {
            return Err(FilesystemError::Parse(format!(
                "XFS extents fork too small: need {need}, have {}",
                fork.len()
            )));
        }
        let mut recs = Vec::with_capacity(n);
        for i in 0..n {
            let off = i * 16;
            let chunk: &[u8; 16] = fork[off..off + 16].try_into().expect("16-byte chunk");
            recs.push(decode_extent(chunk));
        }
        recs.sort_by_key(|e| e.startoff);
        Ok(recs)
    }

    /// Read the bytes of one filesystem block at `fsblock` into `out`.
    fn read_fsblock(&mut self, fsblock: u64, out: &mut [u8]) -> Result<(), FilesystemError> {
        let bs = self.sb.blocksize as u64;
        let part_byte = fsblock_to_partition_byte(
            fsblock,
            self.sb.agblocks,
            self.sb.agblklog,
            self.sb.blocksize,
        );
        read_at_aligned(&mut self.reader, self.partition_offset + part_byte, bs, out)
    }

    /// Build a `FileEntry` from a directory entry's name + child inode.
    fn child_entry(
        &mut self,
        parent_path: &str,
        name: String,
        ino: u64,
    ) -> Result<FileEntry, FilesystemError> {
        let path = if parent_path == "/" {
            format!("/{name}")
        } else {
            format!("{parent_path}/{name}")
        };
        let (core, inode_buf) = self.read_inode_buf(ino)?;
        let entry_type = if core.is_dir() {
            EntryType::Directory
        } else if core.is_symlink() {
            EntryType::Symlink
        } else if core.is_regular() {
            EntryType::File
        } else {
            EntryType::Special
        };
        let symlink_target = if core.is_symlink() {
            self.read_symlink_target(&core, &inode_buf).ok()
        } else {
            None
        };
        let mut size = core.size;
        if matches!(entry_type, EntryType::Directory) {
            size = 0;
        }
        Ok(FileEntry {
            name,
            path,
            entry_type,
            size,
            location: ino,
            modified: None,
            type_code: None,
            creator_code: None,
            symlink_target,
            special_type: None,
            mode: Some(core.mode as u32),
            uid: Some(core.uid),
            gid: Some(core.gid),
            resource_fork_size: None,
            aux_type: None,
            link_target_cnid: None,
        })
    }

    fn read_symlink_target(
        &mut self,
        core: &XfsDinodeCore,
        inode_buf: &[u8],
    ) -> Result<String, FilesystemError> {
        match core.format {
            DiFormat::Local => {
                let fork = self.data_fork(core, inode_buf);
                Ok(symlink::decode_local_target(fork, core.size))
            }
            DiFormat::Extents => {
                let fork = self.data_fork(core, inode_buf);
                let recs = self.decode_inline_extents(fork, core.nextents as usize)?;
                let Some(first) = recs.first() else {
                    return Ok(String::new());
                };
                let bs = self.sb.blocksize as usize;
                let mut buf = vec![0u8; bs];
                self.read_fsblock(first.startblock, &mut buf)?;
                Ok(symlink::decode_extent_target(&buf, core.size))
            }
            other => Err(FilesystemError::Unsupported(format!(
                "XFS symlink di_format {other:?} not supported"
            ))),
        }
    }

    /// Read the file's data into a `Vec<u8>` capped at `max_bytes`.
    fn read_extents_file(
        &mut self,
        core: &XfsDinodeCore,
        inode_buf: &[u8],
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let size = core.size as usize;
        let want = size.min(max_bytes);
        let mut out = Vec::with_capacity(want);
        if want == 0 {
            return Ok(out);
        }
        let fork = self.data_fork(core, inode_buf);
        let recs = self.decode_inline_extents(fork, core.nextents as usize)?;
        let bs = self.sb.blocksize as u64;
        let mut produced: u64 = 0;
        let want64 = want as u64;
        let mut block = vec![0u8; bs as usize];
        for rec in recs {
            // Zero-fill any hole between the running file offset and this extent.
            let extent_byte_off = rec.startoff * bs;
            if extent_byte_off > produced {
                let hole = (extent_byte_off - produced).min(want64 - produced);
                out.resize(out.len() + hole as usize, 0);
                produced += hole;
                if produced >= want64 {
                    break;
                }
            }
            for i in 0..rec.blockcount {
                if produced >= want64 {
                    break;
                }
                if rec.unwritten {
                    let remaining = want64 - produced;
                    let take = remaining.min(bs);
                    out.resize(out.len() + take as usize, 0);
                    produced += take;
                } else {
                    self.read_fsblock(rec.startblock + i, &mut block)?;
                    let remaining = want64 - produced;
                    let take = remaining.min(bs) as usize;
                    out.extend_from_slice(&block[..take]);
                    produced += take as u64;
                }
            }
            if produced >= want64 {
                break;
            }
        }
        // Final tail of holes past the last extent up to di_size (if any).
        if (out.len() as u64) < want64 {
            out.resize(want as usize, 0);
        }
        Ok(out)
    }

    /// Streaming variant of `read_extents_file`. Emits the file bytes via
    /// `writer`, 1 MiB at a time, and returns total bytes written.
    fn stream_extents_file(
        &mut self,
        core: &XfsDinodeCore,
        inode_buf: &[u8],
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let size = core.size;
        if size == 0 {
            return Ok(0);
        }
        let fork = self.data_fork(core, inode_buf);
        let recs = self.decode_inline_extents(fork, core.nextents as usize)?;
        let bs = self.sb.blocksize as u64;
        let mut written: u64 = 0;
        let mut block = vec![0u8; bs as usize];
        let zero_chunk = vec![0u8; STREAM_CHUNK];
        for rec in recs {
            let extent_byte_off = rec.startoff * bs;
            if extent_byte_off > written {
                let hole = (extent_byte_off - written).min(size - written);
                written += emit_zeros(writer, &zero_chunk, hole)?;
                if written >= size {
                    return Ok(written);
                }
            }
            for i in 0..rec.blockcount {
                if written >= size {
                    return Ok(written);
                }
                let remaining_file = size - written;
                let take = remaining_file.min(bs) as usize;
                if rec.unwritten {
                    written += emit_zeros(writer, &zero_chunk, take as u64)?;
                } else {
                    self.read_fsblock(rec.startblock + i, &mut block)?;
                    writer.write_all(&block[..take])?;
                    written += take as u64;
                }
            }
        }
        if written < size {
            written += emit_zeros(writer, &zero_chunk, size - written)?;
        }
        Ok(written)
    }
}

impl<R: Read + Seek + Send> Filesystem for XfsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        let rootino = self.sb.rootino;
        let (core, _) = self.read_inode_buf(rootino)?;
        if !core.is_dir() {
            return Err(FilesystemError::InvalidData(format!(
                "XFS root inode {rootino} is not a directory (mode=0o{:o})",
                core.mode
            )));
        }
        Ok(FileEntry {
            name: "/".into(),
            path: "/".into(),
            entry_type: EntryType::Directory,
            size: 0,
            location: rootino,
            modified: None,
            type_code: None,
            creator_code: None,
            symlink_target: None,
            special_type: None,
            mode: Some(core.mode as u32),
            uid: Some(core.uid),
            gid: Some(core.gid),
            resource_fork_size: None,
            aux_type: None,
            link_target_cnid: None,
        })
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if !entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        if self.dir_format != DirFormat::Dir2 {
            return Err(FilesystemError::Unsupported(
                "XFS dir1 listing not yet implemented (Step 6b)".into(),
            ));
        }
        let (core, inode_buf) = self.read_inode_buf(entry.location)?;
        if !core.is_dir() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        let dirblksize = self.sb.dirblksize() as u64;
        let raw_entries: Vec<dir2::Dir2Entry> = match core.format {
            DiFormat::Local => {
                let fork = self.data_fork(&core, &inode_buf);
                let (_parent, entries) = dir2::parse_shortform(fork, self.sb.has_ftype())?;
                entries
            }
            DiFormat::Extents => {
                if core.size > dirblksize {
                    return Err(FilesystemError::Unsupported(format!(
                        "XFS dir2 size {} > dirblksize {dirblksize} \
                         (leaf/node directory, Step 7)",
                        core.size
                    )));
                }
                let fork = self.data_fork(&core, &inode_buf);
                let recs = self.decode_inline_extents(fork, core.nextents as usize)?;
                let Some(first) = recs.first() else {
                    return Ok(Vec::new());
                };
                // Read `dirblksize / blocksize` consecutive blocks starting at
                // `first.startblock`. For the supported subset that's always 1
                // (dirblklog == 0).
                let blocks_per_dir = (dirblksize / self.sb.blocksize as u64).max(1) as usize;
                let mut buf = vec![0u8; dirblksize as usize];
                let bs = self.sb.blocksize as usize;
                for i in 0..blocks_per_dir {
                    let slot = &mut buf[i * bs..(i + 1) * bs];
                    self.read_fsblock(first.startblock + i as u64, slot)?;
                }
                dir2::parse_block(&buf, self.sb.has_ftype())?
            }
            DiFormat::Btree => {
                return Err(FilesystemError::Unsupported(
                    "XFS btree-format directory (Step 7)".into(),
                ));
            }
            DiFormat::Other(v) => {
                return Err(FilesystemError::Parse(format!(
                    "unknown XFS di_format {v} on directory inode {}",
                    core.ino
                )));
            }
        };
        let parent_path = entry.path.clone();
        let mut out = Vec::with_capacity(raw_entries.len());
        for de in raw_entries {
            match self.child_entry(&parent_path, de.name, de.inumber) {
                Ok(child) => out.push(child),
                Err(_) => {
                    // Damaged metadata: surface the entry as a plain file with
                    // size 0 so the user still sees the name.
                    let path = if parent_path == "/" {
                        format!("/<unreadable inode {}>", de.inumber)
                    } else {
                        format!("{parent_path}/<unreadable inode {}>", de.inumber)
                    };
                    out.push(FileEntry::new_file(
                        format!("<unreadable inode {}>", de.inumber),
                        path,
                        0,
                        de.inumber,
                    ));
                }
            }
        }
        Ok(out)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        if entry.is_directory() {
            return Err(FilesystemError::InvalidData(format!(
                "XFS read_file on directory: {}",
                entry.path
            )));
        }
        let (core, inode_buf) = self.read_inode_buf(entry.location)?;
        match core.format {
            DiFormat::Local => {
                let fork = self.data_fork(&core, &inode_buf);
                let want = (core.size as usize).min(max_bytes);
                let take = want.min(fork.len());
                Ok(fork[..take].to_vec())
            }
            DiFormat::Extents => self.read_extents_file(&core, &inode_buf, max_bytes),
            DiFormat::Btree => Err(FilesystemError::Unsupported(
                "XFS btree-format file extents (Step 7)".into(),
            )),
            DiFormat::Other(v) => Err(FilesystemError::Parse(format!(
                "unknown XFS di_format {v} on inode {}",
                core.ino
            ))),
        }
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        if entry.is_directory() {
            return Err(FilesystemError::InvalidData(format!(
                "XFS write_file_to on directory: {}",
                entry.path
            )));
        }
        let (core, inode_buf) = self.read_inode_buf(entry.location)?;
        match core.format {
            DiFormat::Local => {
                let fork = self.data_fork(&core, &inode_buf);
                let take = (core.size as usize).min(fork.len());
                writer.write_all(&fork[..take])?;
                Ok(take as u64)
            }
            DiFormat::Extents => self.stream_extents_file(&core, &inode_buf, writer),
            DiFormat::Btree => Err(FilesystemError::Unsupported(
                "XFS btree-format file extents (Step 7)".into(),
            )),
            DiFormat::Other(v) => Err(FilesystemError::Parse(format!(
                "unknown XFS di_format {v} on inode {}",
                core.ino
            ))),
        }
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

fn emit_zeros(
    writer: &mut dyn Write,
    zero_chunk: &[u8],
    count: u64,
) -> Result<u64, FilesystemError> {
    let mut remaining = count;
    while remaining > 0 {
        let take = remaining.min(zero_chunk.len() as u64) as usize;
        writer.write_all(&zero_chunk[..take])?;
        remaining -= take as u64;
    }
    Ok(count)
}

/// Sector-aligned read helper. Reads `len` bytes starting at byte offset
/// `byte_off` into `out`. Both `byte_off` and `len` must be 512-byte aligned.
/// A short read surfaces as `Io(UnexpectedEof)`.
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
        let mode = root.mode.expect("mode");
        assert_eq!(mode & 0o170000, 0o040000);
    }

    #[test]
    fn lists_root_directory_from_fixture() {
        // From `xfs_db -c 'inode 128' -c 'p u'` on /tmp/xfs.img:
        //   hello.txt -> 131 (file)
        //   readme.txt -> 132 (file)
        //   subdir -> 1310848 (directory, in AG1)
        //   link -> 133 (symlink)
        // FTYPE feature is on (features2 = 0x28a).
        let img = load_fixture();
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        assert!(fs.sb.has_ftype());
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list root");
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        for expected in ["hello.txt", "readme.txt", "subdir", "link"] {
            assert!(names.contains(&expected), "missing {expected} in {names:?}");
        }
        assert!(!names.contains(&"."));
        assert!(!names.contains(&".."));
        let link = entries.iter().find(|e| e.name == "link").unwrap();
        assert!(link.is_symlink());
        let subdir = entries.iter().find(|e| e.name == "subdir").unwrap();
        assert!(subdir.is_directory());
    }

    #[test]
    fn reads_known_files_from_fixture() {
        // hello.txt (131) and readme.txt (132) are extent-format regular
        // files. xfs_db doesn't tell us their contents directly, but the
        // file sizes are recorded on disk. We just verify the read round-
        // trips byte-for-byte through write_file_to.
        let img = load_fixture();
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list root");
        for name in ["hello.txt", "readme.txt"] {
            let entry = entries.iter().find(|e| e.name == name).expect(name);
            let direct = fs.read_file(entry, usize::MAX).expect("read");
            assert_eq!(direct.len() as u64, entry.size);
            let mut streamed = Vec::new();
            let n = fs.write_file_to(entry, &mut streamed).expect("stream");
            assert_eq!(n, entry.size);
            assert_eq!(streamed, direct);
        }
    }

    #[test]
    fn hello_txt_contents_match_ground_truth() {
        // Verified via `xxd -s 49152` on /tmp/xfs.img: hello.txt is 41 bytes
        // at fsblock 12 (AG0 block 12 = byte 0xC000).
        let img = load_fixture();
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list root");
        let hello = entries.iter().find(|e| e.name == "hello.txt").unwrap();
        let data = fs.read_file(hello, usize::MAX).expect("read");
        assert_eq!(data, b"hello from rusty-backup XFS dir2 fixture\n",);
    }

    #[test]
    fn descends_into_subdir_and_reads_nested_file() {
        // subdir (1310848) is shortform-local with one entry: nested.txt
        // → 1310849. Descending must work across AG boundaries (subdir
        // lives in AG1).
        let img = load_fixture();
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list root");
        let subdir = entries.iter().find(|e| e.name == "subdir").unwrap();
        assert!(subdir.is_directory());
        let children = fs.list_directory(subdir).expect("list subdir");
        let nested = children
            .iter()
            .find(|e| e.name == "nested.txt")
            .expect("nested.txt");
        assert_eq!(nested.path, "/subdir/nested.txt");
        // read_file should succeed (extents-format file).
        let data = fs.read_file(nested, usize::MAX).expect("read nested");
        assert_eq!(data.len() as u64, nested.size);
    }

    #[test]
    fn symlink_target_is_populated() {
        let img = load_fixture();
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list root");
        let link = entries.iter().find(|e| e.name == "link").unwrap();
        assert!(link.is_symlink());
        // Verified via xfs_db: u.symlink = "readme.txt".
        assert_eq!(link.symlink_target.as_deref(), Some("readme.txt"));
    }

    #[test]
    fn truncated_image_produces_unexpected_eof() {
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

    #[test]
    fn list_directory_returns_unsupported_for_dir1() {
        // Patch the fixture's versionnum to clear the DIRV2 bit, so the open
        // path reports dir1. list_directory must then refuse with a Step 6b
        // pointer; the underlying disk is still dir2 but the dispatch reads
        // off the parsed format.
        let mut img = load_fixture();
        // versionnum @ partition_offset + 100, big-endian u16. fixture is
        // 0xb4a4 → clear 0x2000 → 0x94a4.
        img[100..102].copy_from_slice(&0x94a4u16.to_be_bytes());
        let mut fs = XfsFilesystem::open(Cursor::new(img), 0).expect("open xfs");
        assert_eq!(fs.dir_format, DirFormat::Dir1);
        let root = fs.root().expect("root");
        match fs.list_directory(&root) {
            Err(FilesystemError::Unsupported(msg)) => assert!(msg.contains("Step 6b")),
            Err(e) => panic!("expected Unsupported, got {e}"),
            Ok(_) => panic!("expected Unsupported"),
        }
    }
}
