//! QNX 4 filesystem (QNX 4.x, and QNX Neutrino 6.0-6.3's `fs-qnx4`), read-only.
//!
//! Layout from the on-disk structures in Linux's `include/uapi/linux/qnx4_fs.h`
//! (used as format documentation only). Little-endian, 512-byte blocks.
//!
//! - Block 1 (byte 512) is the superblock: four 64-byte inode entries, the first
//!   being the root directory (named `/`), then `/.inodes`, `/.boot`, `/.altboot`.
//! - Inode entry, 64 bytes: 16-byte name +0, size +16, first extent `{u32 block,
//!   u32 count}` +20, extent-block chain +28, times +32..+48, extent count +48,
//!   mode +50, uid +52, gid +54, nlink +56, type +62, status +63 (`0x01` used,
//!   `0x08` link, `0x10` inode).
//! - Block numbers in extents and links are 1-based: physical block = n - 1.
//! - A directory is 512-byte blocks of eight 64-byte entries. An entry is either
//!   the file's inode in place, or (status `0x08`) a link: a 48-byte name, then the
//!   block and index of the inode elsewhere. `.`, `..` and the root's `/` entry
//!   are skipped.
//! - Further extents live in extent blocks: `{next, prev, u8 count, 3 spare, u32
//!   blocks, 60 x {block, count}, "IamXblk\0", first}`.

use std::io::{Read, Seek, SeekFrom, Write};

use disk_core::entry::FileEntry;
use disk_core::filesystem::{Filesystem, FilesystemError};

const BLOCK: u64 = 512;
const ENTRY: usize = 64;
const PER_BLOCK: usize = 8;
const ST_USED: u8 = 0x01;
const ST_LINK: u8 = 0x08;
const S_IFMT: u16 = 0o170000;
const S_IFDIR: u16 = 0o040000;
const S_IFLNK: u16 = 0o120000;
const XBLK_SIG: &[u8; 8] = b"IamXblk\0";
const MAX_XBLKS: usize = 1 << 16;
const CHUNK: usize = 64 * 1024;

fn le32(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}

fn le16(b: &[u8], o: usize) -> u16 {
    u16::from_le_bytes([b[o], b[o + 1]])
}

/// One decoded inode entry.
#[derive(Debug, Clone)]
struct Inode {
    size: u64,
    first: (u32, u32),
    xblk: u32,
    mtime: u32,
    mode: u16,
    uid: u16,
    gid: u16,
}

impl Inode {
    fn parse(b: &[u8]) -> Self {
        Self {
            size: le32(b, 16) as u64,
            first: (le32(b, 20), le32(b, 24)),
            xblk: le32(b, 28),
            mtime: le32(b, 36),
            mode: le16(b, 50),
            uid: le16(b, 52),
            gid: le16(b, 54),
        }
    }
}

/// Whether the superblock at block 1 names a `/` root with a plausible first extent.
pub fn looks_like_qnx4<R: Read + Seek>(reader: &mut R, offset: u64) -> bool {
    let mut sb = [0u8; BLOCK as usize];
    if reader.seek(SeekFrom::Start(offset + BLOCK)).is_err() || reader.read_exact(&mut sb).is_err()
    {
        return false;
    }
    let root = Inode::parse(&sb[..ENTRY]);
    &sb[0..2] == b"/\0"
        && sb[63] & ST_USED != 0
        && root.mode & S_IFMT == S_IFDIR
        && root.first.0 >= 2
        && root.first.1 >= 1
}

/// A QNX 4 volume, browsed read-only.
pub struct Qnx4Filesystem<R: Read + Seek + Send> {
    reader: R,
    offset: u64,
    root: Inode,
    total_blocks: u64,
}

impl<R: Read + Seek + Send> Qnx4Filesystem<R> {
    pub fn open(mut reader: R, offset: u64) -> Result<Self, FilesystemError> {
        if !looks_like_qnx4(&mut reader, offset) {
            return Err(FilesystemError::Parse("not a QNX 4 volume".into()));
        }
        let mut sb = [0u8; BLOCK as usize];
        reader.seek(SeekFrom::Start(offset + BLOCK))?;
        reader.read_exact(&mut sb)?;
        let end = reader.seek(SeekFrom::End(0))?;
        Ok(Self {
            reader,
            offset,
            root: Inode::parse(&sb[..ENTRY]),
            total_blocks: end.saturating_sub(offset) / BLOCK,
        })
    }

    fn read_block(&mut self, physical: u64) -> Result<Vec<u8>, FilesystemError> {
        let mut b = vec![0u8; BLOCK as usize];
        self.reader
            .seek(SeekFrom::Start(self.offset + physical * BLOCK))?;
        self.reader.read_exact(&mut b)?;
        Ok(b)
    }

    /// Every extent of `inode` as `(first physical block, block count)`.
    fn extents(&mut self, inode: &Inode) -> Result<Vec<(u64, u64)>, FilesystemError> {
        let mut out = Vec::new();
        if inode.first.0 != 0 {
            out.push((inode.first.0 as u64 - 1, inode.first.1 as u64));
        }
        let mut next = inode.xblk;
        while next != 0 && out.len() < MAX_XBLKS {
            let x = self.read_block(next as u64 - 1)?;
            if &x[496..504] != XBLK_SIG {
                return Err(FilesystemError::InvalidData(format!(
                    "qnx4: extent block {next} has no signature"
                )));
            }
            for i in 0..(x[8] as usize).min(60) {
                let (blk, len) = (le32(&x, 16 + i * 8), le32(&x, 20 + i * 8));
                if blk != 0 {
                    out.push((blk as u64 - 1, len as u64));
                }
            }
            next = le32(&x, 0);
        }
        Ok(out)
    }

    fn read_data(&mut self, inode: &Inode, max: usize) -> Result<Vec<u8>, FilesystemError> {
        let mut out = Vec::with_capacity((inode.size as usize).min(max));
        self.copy_data(inode, max as u64, &mut out)?;
        Ok(out)
    }

    /// Stream up to `max` bytes of `inode` into `w` in bounded chunks; bytes past the last extent are zeros.
    fn copy_data(
        &mut self,
        inode: &Inode,
        max: u64,
        w: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let want = inode.size.min(max);
        let mut buf = vec![0u8; CHUNK];
        let mut done = 0u64;
        for (start, len) in self.extents(inode)? {
            if done >= want {
                break;
            }
            let mut left = (len * BLOCK).min(want - done);
            self.reader
                .seek(SeekFrom::Start(self.offset + start * BLOCK))?;
            while left > 0 {
                let n = (CHUNK as u64).min(left) as usize;
                self.reader.read_exact(&mut buf[..n])?;
                w.write_all(&buf[..n])?;
                left -= n as u64;
                done += n as u64;
            }
        }
        buf.fill(0);
        while done < want {
            let n = (CHUNK as u64).min(want - done) as usize;
            w.write_all(&buf[..n])?;
            done += n as u64;
        }
        Ok(done)
    }

    /// `(name, inode, location)` for every live entry of `dir`; the location is
    /// `(physical block << 3) | index` of the entry's inode.
    fn read_dir(&mut self, dir: &Inode) -> Result<Vec<(String, Inode, u64)>, FilesystemError> {
        let mut out = Vec::new();
        let mut left = dir.size.div_ceil(ENTRY as u64) as usize;
        for (start, len) in self.extents(dir)? {
            for blk in start..start + len {
                if left == 0 {
                    return Ok(out);
                }
                let b = self.read_block(blk)?;
                for (slot, e) in b.chunks_exact(ENTRY).enumerate().take(left) {
                    let status = e[63];
                    let (raw, loc) = if status & ST_LINK != 0 {
                        let (ib, ndx) = (le32(e, 48) as u64, e[52] as u64);
                        if ib == 0 || ndx >= PER_BLOCK as u64 {
                            continue;
                        }
                        (&e[..48], ((ib - 1) << 3) | ndx)
                    } else if status & ST_USED != 0 {
                        (&e[..16], (blk << 3) | slot as u64)
                    } else {
                        continue;
                    };
                    let end = raw.iter().position(|&c| c == 0).unwrap_or(raw.len());
                    let name = String::from_utf8_lossy(&raw[..end]).into_owned();
                    if name.is_empty() || name == "." || name == ".." || name == "/" {
                        continue;
                    }
                    let inode = self.inode_at(loc)?;
                    out.push((name, inode, loc));
                }
                left = left.saturating_sub(PER_BLOCK);
            }
        }
        Ok(out)
    }

    /// The inode a `FileEntry` stands for: entries carry `(block << 3) | index` of their inode.
    fn inode_at(&mut self, loc: u64) -> Result<Inode, FilesystemError> {
        if loc == 0 {
            return Ok(self.root.clone());
        }
        let b = self.read_block(loc >> 3)?;
        let i = (loc & 7) as usize;
        Ok(Inode::parse(&b[i * ENTRY..(i + 1) * ENTRY]))
    }

    fn list(&mut self, parent: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        let dir = self.inode_at(parent.location)?;
        if dir.mode & S_IFMT != S_IFDIR {
            return Err(FilesystemError::NotADirectory(parent.path.clone()));
        }
        let names = self.read_dir(&dir)?;
        let base = parent.path.trim_end_matches('/').to_string();
        let mut out = Vec::new();
        for (name, inode, loc) in names {
            let path = format!("{base}/{name}");
            let mut e = match inode.mode & S_IFMT {
                S_IFDIR => FileEntry::new_directory(name, path, loc),
                _ => FileEntry::new_file(name, path, inode.size, loc),
            };
            if inode.mode & S_IFMT == S_IFLNK {
                let t = self.read_data(&inode, 4096)?;
                e.symlink_target = Some(String::from_utf8_lossy(&t).into_owned());
                e.entry_type = disk_core::entry::EntryType::Symlink;
            }
            e.mode = Some((inode.mode & 0o7777) as u32);
            e.uid = Some(inode.uid as u32);
            e.gid = Some(inode.gid as u32);
            e.modified_unix = Some(inode.mtime as u64);
            out.push(e);
        }
        Ok(out)
    }
}

impl<R: Read + Seek + Send> Filesystem for Qnx4Filesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::root())
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        self.list(entry)
    }

    fn read_file(&mut self, entry: &FileEntry, max: usize) -> Result<Vec<u8>, FilesystemError> {
        let inode = self.inode_at(entry.location)?;
        self.read_data(&inode, max)
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let inode = self.inode_at(entry.location)?;
        self.copy_data(&inode, u64::MAX, writer)
    }

    fn volume_label(&self) -> Option<&str> {
        None
    }

    fn fs_type(&self) -> &str {
        "QNX 4"
    }

    fn total_size(&self) -> u64 {
        self.total_blocks * BLOCK
    }

    fn used_size(&self) -> u64 {
        self.total_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn put_inode(
        img: &mut [u8],
        at: usize,
        name: &[u8],
        size: u32,
        first: (u32, u32),
        mode: u16,
        status: u8,
    ) {
        img[at..at + name.len()].copy_from_slice(name);
        img[at + 16..at + 20].copy_from_slice(&size.to_le_bytes());
        img[at + 20..at + 24].copy_from_slice(&first.0.to_le_bytes());
        img[at + 24..at + 28].copy_from_slice(&first.1.to_le_bytes());
        img[at + 50..at + 52].copy_from_slice(&mode.to_le_bytes());
        img[at + 63] = status;
    }

    /// Root dir at physical block 2 holds `/`, `.`, `..`, file `hello` (block 3) and a link `a-long-name-...` to it.
    fn sample() -> Vec<u8> {
        let mut img = vec![0u8; 8 * 512];
        put_inode(&mut img, 512, b"/", 512, (3, 1), S_IFDIR | 0o755, ST_USED);
        let d = 2 * 512;
        put_inode(
            &mut img,
            d,
            b"/",
            512,
            (3, 1),
            S_IFDIR | 0o755,
            ST_USED | 0x10,
        );
        put_inode(
            &mut img,
            d + 64,
            b".",
            512,
            (3, 1),
            S_IFDIR | 0o755,
            ST_USED,
        );
        put_inode(
            &mut img,
            d + 128,
            b"..",
            512,
            (3, 1),
            S_IFDIR | 0o755,
            ST_USED,
        );
        put_inode(&mut img, d + 192, b"hello", 6, (4, 1), 0o100644, ST_USED);
        let l = d + 256;
        let long = b"a-name-longer-than-sixteen-characters";
        img[l..l + long.len()].copy_from_slice(long);
        img[l + 48..l + 52].copy_from_slice(&3u32.to_le_bytes());
        img[l + 52] = 3;
        img[l + 63] = ST_LINK;
        img[3 * 512..3 * 512 + 6].copy_from_slice(b"hello\n");
        img
    }

    #[test]
    fn lists_inodes_in_place_and_links() {
        let img = sample();
        assert!(looks_like_qnx4(&mut Cursor::new(img.clone()), 0));
        let mut fs = Qnx4Filesystem::open(Cursor::new(img), 0).unwrap();
        let root = fs.root().unwrap();
        let list = fs.list_directory(&root).unwrap();
        let names: Vec<_> = list.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, ["hello", "a-name-longer-than-sixteen-characters"]);
        for e in &list {
            assert_eq!(fs.read_file(e, usize::MAX).unwrap(), b"hello\n");
        }
    }

    #[test]
    fn rejects_other_volumes() {
        assert!(!looks_like_qnx4(&mut Cursor::new(vec![0u8; 4096]), 0));
    }
}
