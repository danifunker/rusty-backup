//! QNX 2 filesystem (QNX 2.x, and the Unisys ICON's System Software and QDOS), read-only.
//!
//! The layout comes from Mitchell Schoenbrun's public-domain `q2files.c` /
//! `q2read.c` and was checked against the ICON floppies (800 KiB, 1600 blocks).
//! Everything is little-endian with 16-bit `int`s.
//!
//! - 512-byte blocks numbered from 1: block 1 is the first sector of the volume.
//!   Block 1 holds boot data and, at byte 68, the root directory's entry; the
//!   free-block bitmap is the root's `bitmap` file (one bit per block).
//! - Directory entry, 48 bytes: status (`0x40` = in use) +0, first extent +1,
//!   last extent +5, blocks held minus one +9, extent count +13, owner +15,
//!   group +16, unused bytes in the last block +17, seconds +19, type +23, group
//!   and world permissions +24 / +25, attributes +26 (`0x20` = directory), a
//!   two-word date +27, a 16-character name + NUL +31.
//! - A file is a chain of extents. Each extent's first block starts with
//!   `{u32 prev, u32 next, u32 size, u32 bound}`: `size` is the data bytes the
//!   extent holds (after the 16-byte header, running on into its following
//!   blocks) and `bound` the blocks it spans beyond the first. A file's length
//!   is the sum of its extents' `size`.
//! - A directory's data starts `{u16 parent_xtnt, u16 dir_index}` (plus a u16
//!   when `dir_index`'s top bit is set), then 48-byte entries.

use std::io::{Read, Seek, SeekFrom};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};

const BLOCK: u64 = 512;
const ROOT_AT: usize = 68;
const ENTRY: usize = 48;
const XTNT_HDR: u64 = 16;
const USED: u8 = 0x40;
const DIRECTORY: u8 = 0x20;
/// Longest chain a well-formed volume can hold; a loop or a wild pointer stops here.
const MAX_EXTENTS: usize = 1 << 16;

/// One decoded directory entry.
#[derive(Debug, Clone)]
struct Dirent {
    name: String,
    first: u32,
    owner: u8,
    group: u8,
    attr: u8,
}

impl Dirent {
    fn parse(b: &[u8]) -> Option<Self> {
        if b[0] & USED == 0 {
            return None;
        }
        let raw = &b[31..47];
        let end = raw.iter().position(|&c| c == 0).unwrap_or(raw.len());
        Some(Self {
            name: String::from_utf8_lossy(&raw[..end]).into_owned(),
            first: u32::from_le_bytes([b[1], b[2], b[3], b[4]]),
            owner: b[15],
            group: b[16],
            attr: b[26],
        })
    }

    fn is_dir(&self) -> bool {
        self.attr & DIRECTORY != 0
    }
}

/// Whether block 1 at `offset` carries a QNX 2 root entry pointing at a sane extent.
pub fn looks_like_qnx2<R: Read + Seek>(reader: &mut R, offset: u64) -> bool {
    let mut b1 = [0u8; BLOCK as usize];
    if reader.seek(SeekFrom::Start(offset)).is_err() || reader.read_exact(&mut b1).is_err() {
        return false;
    }
    let root = &b1[ROOT_AT..ROOT_AT + ENTRY];
    let first = u32::from_le_bytes([root[1], root[2], root[3], root[4]]) as u64;
    if root[0] & USED == 0
        || root[26] & DIRECTORY == 0
        || root[31] != 0
        || !(2..65536).contains(&first)
    {
        return false;
    }
    let mut hdr = [0u8; 16];
    if reader
        .seek(SeekFrom::Start(offset + (first - 1) * BLOCK))
        .is_err()
        || reader.read_exact(&mut hdr).is_err()
    {
        return false;
    }
    let word = |o: usize| u32::from_le_bytes([hdr[o], hdr[o + 1], hdr[o + 2], hdr[o + 3]]) as u64;
    let (prev, size, bound) = (word(0), word(8), word(12));
    prev == 0 && size >= 4 && bound < 4096 && size <= (bound + 1) * BLOCK - XTNT_HDR
}

/// A QNX 2 volume, browsed read-only.
pub struct Qnx2Filesystem<R: Read + Seek + Send> {
    reader: R,
    offset: u64,
    total_blocks: u64,
    free_blocks: u64,
    root_first: u32,
}

impl<R: Read + Seek + Send> Qnx2Filesystem<R> {
    pub fn open(mut reader: R, offset: u64) -> Result<Self, FilesystemError> {
        if !looks_like_qnx2(&mut reader, offset) {
            return Err(FilesystemError::Parse("not a QNX 2 volume".into()));
        }
        let mut b1 = [0u8; BLOCK as usize];
        reader.seek(SeekFrom::Start(offset))?;
        reader.read_exact(&mut b1)?;
        let root = Dirent::parse(&b1[ROOT_AT..ROOT_AT + ENTRY])
            .ok_or_else(|| FilesystemError::Parse("qnx2: root entry not in use".into()))?;
        let end = reader.seek(SeekFrom::End(0))?;
        let mut fs = Self {
            reader,
            offset,
            total_blocks: end.saturating_sub(offset) / BLOCK,
            free_blocks: 0,
            root_first: root.first,
        };
        // The bitmap file sizes the volume (one bit per block) and counts the free blocks.
        if let Ok(entries) = fs.read_dir(root.first) {
            if let Some(bm) = entries.iter().find(|e| e.name == "bitmap" && !e.is_dir()) {
                if let Ok(map) = fs.read_chain(bm.first, usize::MAX) {
                    fs.total_blocks = (map.len() as u64 * 8).min(fs.total_blocks.max(1));
                    fs.free_blocks = map
                        .iter()
                        .map(|b| b.count_zeros() as u64)
                        .sum::<u64>()
                        .min(fs.total_blocks);
                }
            }
        }
        Ok(fs)
    }

    /// Every extent of the chain starting at `first`, as `(block, data bytes)`.
    fn extents(&mut self, first: u32) -> Result<Vec<(u64, u64)>, FilesystemError> {
        let mut out = Vec::new();
        let mut at = first as u64;
        let mut seen = std::collections::HashSet::new();
        while at != 0 {
            if at > self.total_blocks.max(1) || !seen.insert(at) || out.len() >= MAX_EXTENTS {
                return Err(FilesystemError::InvalidData(format!(
                    "qnx2: extent chain breaks at block {at}"
                )));
            }
            let mut hdr = [0u8; 16];
            self.reader
                .seek(SeekFrom::Start(self.offset + (at - 1) * BLOCK))?;
            self.reader.read_exact(&mut hdr)?;
            let w =
                |o: usize| u32::from_le_bytes([hdr[o], hdr[o + 1], hdr[o + 2], hdr[o + 3]]) as u64;
            let (next, size, bound) = (w(4), w(8), w(12));
            out.push((at, size.min((bound + 1) * BLOCK - XTNT_HDR)));
            at = next;
        }
        Ok(out)
    }

    /// Up to `max` bytes of the file whose chain starts at `first`.
    fn read_chain(&mut self, first: u32, max: usize) -> Result<Vec<u8>, FilesystemError> {
        let mut out = Vec::new();
        for (block, size) in self.extents(first)? {
            if out.len() >= max {
                break;
            }
            let take = (size as usize).min(max - out.len());
            let start = out.len();
            out.resize(start + take, 0);
            self.reader.seek(SeekFrom::Start(
                self.offset + (block - 1) * BLOCK + XTNT_HDR,
            ))?;
            self.reader.read_exact(&mut out[start..])?;
        }
        Ok(out)
    }

    fn file_len(&mut self, first: u32) -> Result<u64, FilesystemError> {
        Ok(self.extents(first)?.iter().map(|e| e.1).sum())
    }

    fn read_dir(&mut self, first: u32) -> Result<Vec<Dirent>, FilesystemError> {
        let data = self.read_chain(first, 1 << 22)?;
        if data.len() < 4 {
            return Ok(Vec::new());
        }
        let index = u16::from_le_bytes([data[2], data[3]]);
        let start = if index & 0x8000 != 0 { 6 } else { 4 };
        Ok(data[start..]
            .chunks_exact(ENTRY)
            .filter_map(Dirent::parse)
            .filter(|e| !e.name.is_empty())
            .collect())
    }

    fn entry_for(&mut self, parent: &str, d: &Dirent) -> FileEntry {
        let path = format!("{}/{}", parent.trim_end_matches('/'), d.name);
        let mut e = if d.is_dir() {
            FileEntry::new_directory(d.name.clone(), path, d.first as u64)
        } else {
            let len = self.file_len(d.first).unwrap_or(0);
            FileEntry::new_file(d.name.clone(), path, len, d.first as u64)
        };
        e.uid = Some(d.owner as u32);
        e.gid = Some(d.group as u32);
        e
    }
}

impl<R: Read + Seek + Send> Filesystem for Qnx2Filesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        let mut root = FileEntry::root();
        root.location = self.root_first as u64;
        Ok(root)
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        let dirents = self.read_dir(entry.location as u32)?;
        Ok(dirents
            .iter()
            .map(|d| self.entry_for(&entry.path, d))
            .collect())
    }

    fn read_file(&mut self, entry: &FileEntry, max: usize) -> Result<Vec<u8>, FilesystemError> {
        self.read_chain(entry.location as u32, max)
    }

    fn volume_label(&self) -> Option<&str> {
        None
    }

    fn fs_type(&self) -> &str {
        "QNX 2"
    }

    fn total_size(&self) -> u64 {
        self.total_blocks * BLOCK
    }

    fn used_size(&self) -> u64 {
        self.total_blocks.saturating_sub(self.free_blocks) * BLOCK
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// A 16-block volume: root at block 3 holding `HELLO` (block 5, 6 bytes) and dir `sub` (block 4).
    fn sample() -> Vec<u8> {
        let mut img = vec![0u8; 16 * 512];
        let entry = |img: &mut Vec<u8>, at: usize, name: &[u8], first: u32, attr: u8| {
            img[at] = USED;
            img[at + 1..at + 5].copy_from_slice(&first.to_le_bytes());
            img[at + 26] = attr;
            img[at + 24] = 0x21;
            img[at + 25] = 0x01;
            img[at + 31..at + 31 + name.len()].copy_from_slice(name);
        };
        entry(&mut img, ROOT_AT, b"", 3, 0x25);
        let hdr = |img: &mut Vec<u8>, block: usize, size: u32| {
            let at = (block - 1) * 512;
            img[at + 8..at + 12].copy_from_slice(&size.to_le_bytes());
        };
        hdr(&mut img, 3, 4 + 2 * ENTRY as u32);
        let dir = 2 * 512 + 16 + 4;
        entry(&mut img, dir, b"HELLO", 5, 0x01);
        entry(&mut img, dir + ENTRY, b"sub", 4, 0x25);
        hdr(&mut img, 4, 4);
        hdr(&mut img, 5, 6);
        img[4 * 512 + 16..4 * 512 + 22].copy_from_slice(b"hello\n");
        img
    }

    #[test]
    fn detects_and_lists_a_volume() {
        let img = sample();
        assert!(looks_like_qnx2(&mut Cursor::new(img.clone()), 0));
        let mut fs = Qnx2Filesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().unwrap();
        let list = fs.list_directory(&root).unwrap();
        let names: Vec<_> = list
            .iter()
            .map(|e| (e.name.as_str(), e.is_directory()))
            .collect();
        assert_eq!(names, [("HELLO", false), ("sub", true)]);
        assert_eq!(list[0].size, 6);
        assert_eq!(fs.read_file(&list[0], usize::MAX).unwrap(), b"hello\n");
        assert!(fs.list_directory(&list[1]).unwrap().is_empty());
    }

    #[test]
    fn rejects_fat_and_blank_disks() {
        assert!(!looks_like_qnx2(&mut Cursor::new(vec![0u8; 8192]), 0));
        let mut fat = vec![0u8; 8192];
        fat[0..3].copy_from_slice(&[0xEB, 0x3C, 0x90]);
        assert!(!looks_like_qnx2(&mut Cursor::new(fat), 0));
    }

    #[test]
    fn a_looping_chain_is_an_error_not_a_hang() {
        let mut img = sample();
        img[4 * 512 + 4..4 * 512 + 8].copy_from_slice(&5u32.to_le_bytes());
        let mut fs = Qnx2Filesystem::open(Cursor::new(img), 0).unwrap();
        assert!(fs.read_chain(5, usize::MAX).is_err());
    }
}
