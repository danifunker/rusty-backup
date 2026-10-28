//! QNX 6 Power-Safe filesystem (`fs-qnx6`, QNX Neutrino 6.4 and later), read-only.
//!
//! Layout from the on-disk structures in Linux's `include/linux/qnx6_fs.h` (used
//! as format documentation only). Either byte order; the magic says which.
//!
//! - An 8 KiB boot area, then a 512-byte superblock (magic `0x68191122`, a
//!   CRC-32 of bytes 8-511, a serial) in a 4 KiB superblock area. Data blocks
//!   start right after it; a second superblock follows the last block. The valid
//!   copy with the higher serial is current.
//! - Superblock: block size +48, inode count +52, block count +60, free +64, then
//!   four 80-byte root nodes from +72: the inode table, the bitmap, the long
//!   file names, and one unused.
//! - A root node (and every inode) maps its data through 16 block pointers and
//!   a level count: with `levels` 0 they point at data, otherwise at blocks of
//!   further pointers. A pointer is relative to the first data block.
//! - Inode N (from 1) is 128 bytes at `(N - 1) * 128` of the inode table:
//!   size +0, uid +8, gid +12, times +16..+32, mode +32, 16 pointers +36, levels
//!   +100, status +101.
//! - A directory is 32-byte entries `{u32 inode, u8 length, name[27]}`; a name
//!   longer than 27 bytes is stored as the `lf_size`-prefixed block numbered by
//!   the entry's +8 field in the long-file-name tree.

#[cfg(feature = "rust173-polyfill")]
use crate::rust173_compat::OptionIsNoneOr as _;
use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};

pub const QNX6_MAGIC: u32 = 0x6819_1122;
const BOOT_AREA: u64 = 0x2000;
const SB_AREA: u64 = 0x1000;
const SB_SIZE: usize = 512;
const INODE: u64 = 128;
const DIRENT: usize = 32;
const SHORT_NAME: usize = 27;
const S_IFMT: u16 = 0o170000;
const S_IFDIR: u16 = 0o040000;
const S_IFLNK: u16 = 0o120000;
const NOPTR: u32 = 0xffff_ffff;

/// A root node: `size`, 16 pointers, and the indirection depth.
#[derive(Debug, Clone)]
struct Node {
    size: u64,
    ptrs: [u32; 16],
    levels: u8,
}

/// The superblock's byte order and whether its checksum holds.
fn sb_order(sb: &[u8]) -> Option<bool> {
    let m = [sb[0], sb[1], sb[2], sb[3]];
    if u32::from_le_bytes(m) == QNX6_MAGIC {
        Some(false)
    } else if u32::from_be_bytes(m) == QNX6_MAGIC {
        Some(true)
    } else {
        None
    }
}

/// Linux's `crc32_be(0, ..)`: MSB-first CRC-32, polynomial 0x04C11DB7, no inversion.
fn crc32_be(data: &[u8]) -> u32 {
    let mut crc = 0u32;
    for &b in data {
        crc ^= (b as u32) << 24;
        for _ in 0..8 {
            crc = if crc & 0x8000_0000 != 0 {
                (crc << 1) ^ 0x04C1_1DB7
            } else {
                crc << 1
            };
        }
    }
    crc
}

/// Whether a QNX 6 superblock sits after the boot area at `offset`.
pub fn looks_like_qnx6<R: Read + Seek>(reader: &mut R, offset: u64) -> bool {
    let mut sb = [0u8; SB_SIZE];
    reader.seek(SeekFrom::Start(offset + BOOT_AREA)).is_ok()
        && reader.read_exact(&mut sb).is_ok()
        && sb_order(&sb).is_some()
}

/// A QNX 6 Power-Safe volume, browsed read-only.
pub struct Qnx6Filesystem<R: Read + Seek + Send> {
    reader: R,
    offset: u64,
    big: bool,
    bsize: u64,
    blocks_off: u64,
    num_blocks: u64,
    free_blocks: u64,
    inodes: Node,
    longnames: Node,
}

impl<R: Read + Seek + Send> Qnx6Filesystem<R> {
    pub fn open(mut reader: R, offset: u64) -> Result<Self, FilesystemError> {
        let read_sb = |r: &mut R, at: u64| -> Option<Vec<u8>> {
            let mut sb = vec![0u8; SB_SIZE];
            r.seek(SeekFrom::Start(at)).ok()?;
            r.read_exact(&mut sb).ok()?;
            Some(sb)
        };
        let first = read_sb(&mut reader, offset + BOOT_AREA)
            .ok_or_else(|| FilesystemError::Parse("qnx6: no superblock".into()))?;
        let big =
            sb_order(&first).ok_or_else(|| FilesystemError::Parse("not a QNX 6 volume".into()))?;
        let mut fs = Self {
            reader,
            offset,
            big,
            bsize: 0,
            blocks_off: 0,
            num_blocks: 0,
            free_blocks: 0,
            inodes: Node {
                size: 0,
                ptrs: [0; 16],
                levels: 0,
            },
            longnames: Node {
                size: 0,
                ptrs: [0; 16],
                levels: 0,
            },
        };
        fs.bsize = fs.u32(&first, 48) as u64;
        if !(512..=65536).contains(&fs.bsize) || !fs.bsize.is_power_of_two() {
            return Err(FilesystemError::Parse(format!(
                "qnx6: block size {}",
                fs.bsize
            )));
        }
        fs.blocks_off = (BOOT_AREA + SB_AREA) / fs.bsize;
        fs.num_blocks = fs.u32(&first, 60) as u64;
        // The copy-on-write superblocks: take the valid one with the higher serial.
        let backup_at = offset + (fs.blocks_off + fs.num_blocks) * fs.bsize;
        let mut best: Option<Vec<u8>> = None;
        for sb in [Some(first), read_sb(&mut fs.reader, backup_at)]
            .into_iter()
            .flatten()
        {
            if sb_order(&sb) != Some(big) || fs.u32(&sb, 4) != crc32_be(&sb[8..]) {
                continue;
            }
            if best.as_ref().is_none_or(|b| fs.u64(&sb, 8) > fs.u64(b, 8)) {
                best = Some(sb);
            }
        }
        let sb = best.ok_or_else(|| {
            FilesystemError::Parse("qnx6: no superblock with a valid checksum".into())
        })?;
        fs.free_blocks = fs.u32(&sb, 64) as u64;
        fs.inodes = fs.node(&sb, 72);
        fs.longnames = fs.node(&sb, 72 + 2 * 80);
        Ok(fs)
    }

    fn u32(&self, b: &[u8], o: usize) -> u32 {
        let a = [b[o], b[o + 1], b[o + 2], b[o + 3]];
        if self.big {
            u32::from_be_bytes(a)
        } else {
            u32::from_le_bytes(a)
        }
    }

    fn u16(&self, b: &[u8], o: usize) -> u16 {
        let a = [b[o], b[o + 1]];
        if self.big {
            u16::from_be_bytes(a)
        } else {
            u16::from_le_bytes(a)
        }
    }

    fn u64(&self, b: &[u8], o: usize) -> u64 {
        let mut a = [0u8; 8];
        a.copy_from_slice(&b[o..o + 8]);
        if self.big {
            u64::from_be_bytes(a)
        } else {
            u64::from_le_bytes(a)
        }
    }

    fn node(&self, b: &[u8], o: usize) -> Node {
        let mut ptrs = [0u32; 16];
        for (i, p) in ptrs.iter_mut().enumerate() {
            *p = self.u32(b, o + 8 + i * 4);
        }
        Node {
            size: self.u64(b, o),
            ptrs,
            levels: b[o + 72],
        }
    }

    fn read_block(&mut self, rel: u32) -> Result<Vec<u8>, FilesystemError> {
        let mut buf = vec![0u8; self.bsize as usize];
        let at = self.offset + (rel as u64 + self.blocks_off) * self.bsize;
        self.reader.seek(SeekFrom::Start(at))?;
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// The relative block holding logical block `n` of `node`, or `None` for a hole.
    fn bmap(&mut self, node: &Node, n: u64) -> Result<Option<u32>, FilesystemError> {
        let bits = (self.bsize / 4).trailing_zeros() as u64;
        let mut shift = bits * node.levels as u64;
        let top = (n >> shift) as usize;
        if top >= 16 {
            return Ok(None);
        }
        let mut ptr = node.ptrs[top];
        for _ in 0..node.levels {
            if ptr == NOPTR {
                return Ok(None);
            }
            shift -= bits;
            let blk = self.read_block(ptr)?;
            ptr = self.u32(&blk, (((n >> shift) & ((1 << bits) - 1)) * 4) as usize);
        }
        Ok((ptr != NOPTR).then_some(ptr))
    }

    fn read_node(&mut self, node: &Node, max: usize) -> Result<Vec<u8>, FilesystemError> {
        let mut out = Vec::with_capacity((node.size as usize).min(max));
        self.copy_node(node, max as u64, &mut out)?;
        Ok(out)
    }

    /// Stream up to `max` bytes of `node` into `w` one block at a time; holes write zeros.
    fn copy_node(
        &mut self,
        node: &Node,
        max: u64,
        w: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let want = node.size.min(max);
        let mut buf = vec![0u8; self.bsize as usize];
        let mut done = 0u64;
        let mut n = 0u64;
        while done < want {
            let take = self.bsize.min(want - done) as usize;
            match self.bmap(node, n)? {
                Some(p) => {
                    let at = self.offset + (p as u64 + self.blocks_off) * self.bsize;
                    self.reader.seek(SeekFrom::Start(at))?;
                    self.reader.read_exact(&mut buf)?;
                }
                None => buf.fill(0),
            }
            w.write_all(&buf[..take])?;
            done += take as u64;
            n += 1;
        }
        Ok(done)
    }

    /// Inode `ino` (from 1) as `(node, mode, uid, gid, mtime)`.
    fn inode(&mut self, ino: u32) -> Result<(Node, u16, u32, u32, u32), FilesystemError> {
        let pos = (ino as u64)
            .checked_sub(1)
            .ok_or_else(|| FilesystemError::InvalidData("qnx6: inode 0".into()))?
            * INODE;
        let table = self.inodes.clone();
        let blk = self
            .bmap(&table, pos / self.bsize)?
            .ok_or_else(|| FilesystemError::NotFound(format!("qnx6 inode {ino}")))?;
        let b = self.read_block(blk)?;
        let raw = &b[(pos % self.bsize) as usize..][..INODE as usize];
        let mut ptrs = [0u32; 16];
        for (i, p) in ptrs.iter_mut().enumerate() {
            *p = self.u32(raw, 36 + i * 4);
        }
        let node = Node {
            size: self.u64(raw, 0),
            ptrs,
            levels: raw[100],
        };
        Ok((
            node,
            self.u16(raw, 32),
            self.u32(raw, 8),
            self.u32(raw, 12),
            self.u32(raw, 20),
        ))
    }

    fn long_name(&mut self, index: u32) -> Result<String, FilesystemError> {
        let ln = self.longnames.clone();
        let blk = self.bmap(&ln, index as u64)?.ok_or_else(|| {
            FilesystemError::InvalidData(format!("qnx6: long name {index} missing"))
        })?;
        let b = self.read_block(blk)?;
        let len = (self.u16(&b, 0) as usize).min(b.len() - 2);
        Ok(String::from_utf8_lossy(&b[2..2 + len]).into_owned())
    }
}

impl<R: Read + Seek + Send> Filesystem for Qnx6Filesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        let mut root = FileEntry::root();
        root.location = 1;
        Ok(root)
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        let (node, mode, ..) = self.inode(entry.location as u32)?;
        if mode & S_IFMT != S_IFDIR {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        let data = self.read_node(&node, 1 << 24)?;
        let base = entry.path.trim_end_matches('/').to_string();
        let mut out = Vec::new();
        for d in data.chunks_exact(DIRENT) {
            let ino = self.u32(d, 0);
            let len = d[4] as usize;
            if ino == 0 || len == 0 {
                continue;
            }
            let name = if len > SHORT_NAME {
                self.long_name(self.u32(d, 8))?
            } else {
                String::from_utf8_lossy(&d[5..5 + len]).into_owned()
            };
            if name == "." || name == ".." {
                continue;
            }
            let (child, cmode, uid, gid, mtime) = self.inode(ino)?;
            let path = format!("{base}/{name}");
            let mut e = if cmode & S_IFMT == S_IFDIR {
                FileEntry::new_directory(name, path, ino as u64)
            } else {
                FileEntry::new_file(name, path, child.size, ino as u64)
            };
            if cmode & S_IFMT == S_IFLNK {
                let t = self.read_node(&child, 4096)?;
                e.symlink_target = Some(String::from_utf8_lossy(&t).into_owned());
                e.entry_type = super::entry::EntryType::Symlink;
            }
            e.mode = Some((cmode & 0o7777) as u32);
            e.uid = Some(uid);
            e.gid = Some(gid);
            e.modified_unix = Some(mtime as u64);
            out.push(e);
        }
        Ok(out)
    }

    fn read_file(&mut self, entry: &FileEntry, max: usize) -> Result<Vec<u8>, FilesystemError> {
        let (node, ..) = self.inode(entry.location as u32)?;
        self.read_node(&node, max)
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let (node, ..) = self.inode(entry.location as u32)?;
        self.copy_node(&node, u64::MAX, writer)
    }

    fn volume_label(&self) -> Option<&str> {
        None
    }

    fn fs_type(&self) -> &str {
        "QNX 6 Power-Safe"
    }

    fn total_size(&self) -> u64 {
        self.num_blocks * self.bsize
    }

    fn used_size(&self) -> u64 {
        self.num_blocks.saturating_sub(self.free_blocks) * self.bsize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// A little-endian 512-byte-block volume: root (inode 1) with `hello` (inode 2) and a long-named copy.
    fn sample() -> Vec<u8> {
        let bs = 512usize;
        let data0 = (BOOT_AREA + SB_AREA) as usize;
        let mut img = vec![0u8; data0 + 16 * bs + 1024];
        let blk = |n: usize| data0 + n * bs;
        let sb = BOOT_AREA as usize;
        img[sb..sb + 4].copy_from_slice(&QNX6_MAGIC.to_le_bytes());
        img[sb + 8..sb + 16].copy_from_slice(&1u64.to_le_bytes());
        img[sb + 48..sb + 52].copy_from_slice(&(bs as u32).to_le_bytes());
        img[sb + 60..sb + 64].copy_from_slice(&16u32.to_le_bytes());
        let node = |img: &mut Vec<u8>, at: usize, size: u64, ptr0: u32| {
            img[at..at + 8].copy_from_slice(&size.to_le_bytes());
            for i in 0..16 {
                let p = if i == 0 { ptr0 } else { NOPTR };
                img[at + 8 + i * 4..at + 12 + i * 4].copy_from_slice(&p.to_le_bytes());
            }
        };
        node(&mut img, sb + 72, 2 * INODE, 0); // inode table at block 0
        node(&mut img, sb + 72 + 160, bs as u64, 3); // long names at block 3
        let ino = |img: &mut Vec<u8>, n: usize, size: u64, mode: u16, ptr0: u32| {
            let at = blk(0) + (n - 1) * INODE as usize;
            img[at..at + 8].copy_from_slice(&size.to_le_bytes());
            img[at + 32..at + 34].copy_from_slice(&mode.to_le_bytes());
            for i in 0..16 {
                let p = if i == 0 { ptr0 } else { NOPTR };
                img[at + 36 + i * 4..at + 40 + i * 4].copy_from_slice(&p.to_le_bytes());
            }
        };
        ino(&mut img, 1, 4 * DIRENT as u64, S_IFDIR | 0o755, 1);
        ino(&mut img, 2, 6, 0o100644, 2);
        let d = blk(1);
        let ent = |img: &mut Vec<u8>, at: usize, ino: u32, name: &[u8]| {
            img[at..at + 4].copy_from_slice(&ino.to_le_bytes());
            img[at + 4] = name.len() as u8;
            img[at + 5..at + 5 + name.len()].copy_from_slice(name);
        };
        ent(&mut img, d, 1, b".");
        ent(&mut img, d + 32, 1, b"..");
        ent(&mut img, d + 64, 2, b"hello");
        img[d + 96..d + 100].copy_from_slice(&2u32.to_le_bytes());
        img[d + 100] = 40;
        img[d + 104..d + 108].copy_from_slice(&0u32.to_le_bytes());
        let long = b"a-file-name-well-past-27-characters-long";
        img[blk(3)..blk(3) + 2].copy_from_slice(&(long.len() as u16).to_le_bytes());
        img[blk(3) + 2..blk(3) + 2 + long.len()].copy_from_slice(long);
        img[blk(2)..blk(2) + 6].copy_from_slice(b"hello\n");
        let crc = crc32_be(&img[sb + 8..sb + 512]);
        img[sb + 4..sb + 8].copy_from_slice(&crc.to_le_bytes());
        img
    }

    #[test]
    fn lists_short_and_long_names() {
        let img = sample();
        assert!(looks_like_qnx6(&mut Cursor::new(img.clone()), 0));
        let mut fs = Qnx6Filesystem::open(Cursor::new(img), 0).unwrap();
        let root = fs.root().unwrap();
        let list = fs.list_directory(&root).unwrap();
        let names: Vec<_> = list.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, ["hello", "a-file-name-well-past-27-characters-long"]);
        assert_eq!(fs.read_file(&list[0], usize::MAX).unwrap(), b"hello\n");
    }

    #[test]
    fn a_bad_checksum_is_refused() {
        let mut img = sample();
        img[BOOT_AREA as usize + 20] ^= 1;
        assert!(Qnx6Filesystem::open(Cursor::new(img), 0).is_err());
    }
}
