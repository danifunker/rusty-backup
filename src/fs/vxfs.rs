//! Veritas VxFS (HP-UX "JFS", Solaris / Linux VxFS), read-only, disk layout versions 2-4.
//!
//! Layout facts were taken from Linux's former `freevxfs` driver (used as format
//! documentation only) and checked against the root VxFS v4 of an HP-UX 11.11
//! disk. Every multi-byte field is in the volume's byte order: big-endian on
//! HP-UX (superblock at byte 8192), little-endian elsewhere (byte 1024). Both
//! carry the magic `0xA501FCF5`.
//!
//! The walk from the superblock to a file:
//!
//! 1. The superblock gives the block size (`vs_bsize`, +32), the volume size in
//!    blocks (+36), free blocks (+192), the 6-byte name (+348), and the object
//!    location table: its block (`vs_oltext`, +368) and length in blocks (+376).
//! 2. The OLT (magic `0xA504FCF5`) is a header of `olt_size` bytes, then
//!    `{type, size}` records: type 4 gives the initial inode list's block, type 2
//!    the fileset-header inode.
//! 3. Inodes are 256 bytes. "blkiget" reads inode N straight from a block run:
//!    the fileset-header inode comes from the initial list. Its file holds the
//!    structural fileset header in block 0 and the primary in block 1;
//!    `fsh_ilistino` (+48) names each fileset's inode-list inode.
//! 4. The structural inode list is read by blkiget. The primary inode list's
//!    inode is entry `fsh_ilistino` *of the structural list's file*. Every user
//!    inode N is then at byte `N * 256` of the primary inode-list file. The root is 2.
//!
//! Inode: `mode` +0, `nlink` +4, `uid` +8, `gid` +12, `size` (u64) +16, `atime`
//! +24, `mtime` +32, `ctime` +40, organisation type +49 (1 = direct extents,
//! 2 = immediate data, 3 = typed extents), `dotdot` / `rdev` +56, and a 96-byte
//! organisation area +80:
//!
//! - direct: `indsize` +4, `indir[2]` +8, then 10 `{extent, size}` pairs from +16;
//!   past them, `indir[0]` is a block of extent addresses, each `indsize` long.
//! - immediate: the file's bytes themselves.
//! - typed: six 16-byte `{u64 header, u32 block, u32 size}`; the header's top byte
//!   is 1 (indirect: `size` blocks of further typed entries) or 2 (data), and its
//!   low 56 bits the logical block the entry starts at.
//!
//! Directory data is a run of blocks. Each starts with `{u16 free, u16 nhash,
//! u16 hash[nhash]}`, followed by entries `{u32 ino, u16 reclen, u16 namelen,
//! u16 hashnext, name}`. A zero `reclen` ends the block, a zero `ino` is a hole.
//! `.` and `..` are not stored; `..` is the inode's `dotdot` field.

use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};

/// Superblock magic, in the volume's byte order.
pub const VXFS_MAGIC: u32 = 0xA501_FCF5;
const OLT_MAGIC: u32 = 0xA504_FCF5;
const INODE_SIZE: u64 = 256;
const ROOT_INO: u32 = 2;
/// Superblock byte offset on HP-UX (big-endian) and elsewhere (little-endian).
const SB_OFFSET_BE: u64 = 8192;
const SB_OFFSET_LE: u64 = 1024;
const OLT_FSHEAD: u32 = 2;
const OLT_ILIST: u32 = 4;
const ORG_EXT4: u8 = 1;
const ORG_IMMED: u8 = 2;
const ORG_TYPED: u8 = 3;
const TYPED_INDIRECT: u64 = 1;
const TYPED_DATA: u64 = 2;
const TYPED_OFFSET_MASK: u64 = 0x00FF_FFFF_FFFF_FFFF;
/// Typed-extent indirection seen in practice is one or two levels deep.
const MAX_INDIRECT_DEPTH: u32 = 8;

const S_IFMT: u32 = 0o170000;
const S_IFDIR: u32 = 0o040000;
const S_IFREG: u32 = 0o100000;
const S_IFLNK: u32 = 0o120000;

/// Where a superblock with VxFS magic sits, if any, and in which byte order.
pub fn detect<R: Read + Seek>(reader: &mut R, partition_offset: u64) -> Option<(u64, bool)> {
    for (off, big) in [(SB_OFFSET_BE, true), (SB_OFFSET_LE, false)] {
        let mut m = [0u8; 4];
        if reader
            .seek(SeekFrom::Start(partition_offset + off))
            .is_err()
            || reader.read_exact(&mut m).is_err()
        {
            continue;
        }
        let want = if big {
            VXFS_MAGIC.to_be_bytes()
        } else {
            VXFS_MAGIC.to_le_bytes()
        };
        if m == want {
            return Some((off, big));
        }
    }
    None
}

#[derive(Debug, Clone)]
struct Inode {
    mode: u32,
    uid: u32,
    gid: u32,
    size: u64,
    mtime: u32,
    org: u8,
    org_area: [u8; 96],
}

/// A read-only VxFS volume.
pub struct VxfsFilesystem<R: Read + Seek + Send> {
    reader: R,
    offset: u64,
    big: bool,
    version: u32,
    bsize: u64,
    total_blocks: u64,
    free_blocks: u64,
    label: String,
    /// The primary fileset's inode-list inode; user inode N is at N * 256 in its file.
    ilist: Inode,
    fs_type_name: String,
}

impl<R: Read + Seek + Send> VxfsFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        let (sb_off, big) = detect(&mut reader, partition_offset)
            .ok_or_else(|| FilesystemError::Parse("not a VxFS volume".into()))?;
        let mut sb = vec![0u8; 1024];
        reader.seek(SeekFrom::Start(partition_offset + sb_off))?;
        reader.read_exact(&mut sb)?;
        let mut fs = Self {
            reader,
            offset: partition_offset,
            big,
            version: 0,
            bsize: 0,
            total_blocks: 0,
            free_blocks: 0,
            label: String::new(),
            ilist: Inode::empty(),
            fs_type_name: String::new(),
        };
        fs.version = fs.u32_at(&sb, 4);
        fs.bsize = fs.u32_at(&sb, 32) as u64;
        if !(1024..=65536).contains(&fs.bsize) || !fs.bsize.is_power_of_two() {
            return Err(FilesystemError::Parse(format!(
                "vxfs: implausible block size {}",
                fs.bsize
            )));
        }
        if !(2..=4).contains(&fs.version) {
            return Err(FilesystemError::Unsupported(format!(
                "VxFS disk layout version {} is not supported yet (2-4 are)",
                fs.version
            )));
        }
        fs.total_blocks = fs.u32_at(&sb, 36) as u64;
        fs.free_blocks = fs.u32_at(&sb, 192) as u64;
        fs.label = String::from_utf8_lossy(&sb[348..354])
            .trim_end_matches('\0')
            .trim()
            .to_string();
        fs.fs_type_name = format!("VxFS (layout {})", fs.version);

        let (olt_block, olt_len) = (fs.u32_at(&sb, 368) as u64, fs.u32_at(&sb, 376) as u64);
        let olt = fs.read_blocks(olt_block, olt_len.max(1))?;
        if fs.u32_at(&olt, 0) != OLT_MAGIC {
            return Err(FilesystemError::Parse(
                "vxfs: bad object location table magic".into(),
            ));
        }
        let (mut fsh_ino, mut iext) = (None, None);
        let mut p = fs.u32_at(&olt, 4) as usize;
        while p + 12 <= olt.len() {
            let (kind, size) = (fs.u32_at(&olt, p), fs.u32_at(&olt, p + 4) as usize);
            if size == 0 {
                break;
            }
            match kind {
                OLT_FSHEAD => fsh_ino = Some(fs.u32_at(&olt, p + 8)),
                OLT_ILIST => iext = Some(fs.u32_at(&olt, p + 8) as u64),
                _ => {}
            }
            p += size;
        }
        let (fsh_ino, iext) = fsh_ino
            .zip(iext)
            .ok_or_else(|| FilesystemError::Parse("vxfs: OLT names no fileset header".into()))?;

        let fsh = fs.blk_inode(iext, fsh_ino)?;
        let mut heads = vec![0u8; 2 * fs.bsize as usize];
        fs.read_inode_range(&fsh, 0, &mut heads)?;
        let structural = fs.u32_at(&heads, 48);
        let primary = fs.u32_at(&heads, fs.bsize as usize + 48);
        let stilist = fs.blk_inode(iext, structural)?;
        let mut raw = [0u8; INODE_SIZE as usize];
        fs.read_inode_range(&stilist, primary as u64 * INODE_SIZE, &mut raw)?;
        fs.ilist = fs.decode_inode(&raw);
        Ok(fs)
    }

    fn u32_at(&self, b: &[u8], o: usize) -> u32 {
        let a = [b[o], b[o + 1], b[o + 2], b[o + 3]];
        if self.big {
            u32::from_be_bytes(a)
        } else {
            u32::from_le_bytes(a)
        }
    }

    fn u16_at(&self, b: &[u8], o: usize) -> u16 {
        let a = [b[o], b[o + 1]];
        if self.big {
            u16::from_be_bytes(a)
        } else {
            u16::from_le_bytes(a)
        }
    }

    fn u64_at(&self, b: &[u8], o: usize) -> u64 {
        let mut a = [0u8; 8];
        a.copy_from_slice(&b[o..o + 8]);
        if self.big {
            u64::from_be_bytes(a)
        } else {
            u64::from_le_bytes(a)
        }
    }

    fn read_blocks(&mut self, block: u64, count: u64) -> Result<Vec<u8>, FilesystemError> {
        let mut buf = vec![0u8; (count * self.bsize) as usize];
        self.reader
            .seek(SeekFrom::Start(self.offset + block * self.bsize))?;
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn decode_inode(&self, b: &[u8]) -> Inode {
        let mut org_area = [0u8; 96];
        org_area.copy_from_slice(&b[80..176]);
        Inode {
            mode: self.u32_at(b, 0),
            uid: self.u32_at(b, 8),
            gid: self.u32_at(b, 12),
            size: self.u64_at(b, 16),
            mtime: self.u32_at(b, 32),
            org: b[49],
            org_area,
        }
    }

    /// Inode `ino` read straight from the block run starting at `extent` ("blkiget").
    fn blk_inode(&mut self, extent: u64, ino: u32) -> Result<Inode, FilesystemError> {
        let mut b = [0u8; INODE_SIZE as usize];
        self.reader.seek(SeekFrom::Start(
            self.offset + extent * self.bsize + ino as u64 * INODE_SIZE,
        ))?;
        self.reader.read_exact(&mut b)?;
        Ok(self.decode_inode(&b))
    }

    /// A user inode, from the primary inode-list file.
    fn inode(&mut self, ino: u32) -> Result<Inode, FilesystemError> {
        let ilist = self.ilist.clone();
        let mut b = [0u8; INODE_SIZE as usize];
        self.read_inode_range(&ilist, ino as u64 * INODE_SIZE, &mut b)?;
        Ok(self.decode_inode(&b))
    }

    /// Physical block of logical block `bn` of `inode`, or `None` for a hole.
    fn bmap(&mut self, inode: &Inode, bn: u64) -> Result<Option<u64>, FilesystemError> {
        match inode.org {
            ORG_EXT4 => {
                let a = &inode.org_area;
                let mut rel = bn;
                for i in 0..10 {
                    let extent = self.u32_at(a, 16 + i * 8) as u64;
                    let size = self.u32_at(a, 20 + i * 8) as u64;
                    if rel < size {
                        return Ok(Some(extent + rel));
                    }
                    rel -= size;
                }
                let indsize = self.u32_at(a, 4) as u64;
                let indir = self.u32_at(a, 8) as u64;
                if indsize == 0 || indir == 0 {
                    return Ok(None);
                }
                let slot = rel / indsize;
                if slot >= self.bsize / 4 {
                    return Ok(None);
                }
                let table = self.read_blocks(indir, 1)?;
                let start = self.u32_at(&table, slot as usize * 4) as u64;
                Ok((start != 0).then_some(start + rel % indsize))
            }
            ORG_TYPED => {
                let entries: Vec<[u8; 16]> = inode
                    .org_area
                    .chunks_exact(16)
                    .map(|c| {
                        let mut e = [0u8; 16];
                        e.copy_from_slice(c);
                        e
                    })
                    .collect();
                self.bmap_typed(&entries, bn, 0)
            }
            _ => Ok(None),
        }
    }

    fn bmap_typed(
        &mut self,
        entries: &[[u8; 16]],
        bn: u64,
        depth: u32,
    ) -> Result<Option<u64>, FilesystemError> {
        if depth > MAX_INDIRECT_DEPTH {
            return Err(FilesystemError::InvalidData(
                "vxfs: typed extents nest too deep".into(),
            ));
        }
        for e in entries {
            let hdr = self.u64_at(e, 0);
            let (kind, off) = (hdr >> 56, hdr & TYPED_OFFSET_MASK);
            let (block, size) = (self.u32_at(e, 8) as u64, self.u32_at(e, 12) as u64);
            if bn < off {
                continue;
            }
            match kind {
                TYPED_DATA if bn - off < size => return Ok(Some(block + bn - off)),
                TYPED_INDIRECT if size > 0 => {
                    let raw = self.read_blocks(block, size)?;
                    let children: Vec<[u8; 16]> = raw
                        .chunks_exact(16)
                        .map(|c| {
                            let mut x = [0u8; 16];
                            x.copy_from_slice(c);
                            x
                        })
                        .collect();
                    if let Some(p) = self.bmap_typed(&children, bn - off, depth + 1)? {
                        return Ok(Some(p));
                    }
                }
                _ => {}
            }
        }
        Ok(None)
    }

    /// Fill `buf` from byte `pos` of `inode`'s data; holes and bytes past EOF read as zero.
    fn read_inode_range(
        &mut self,
        inode: &Inode,
        pos: u64,
        buf: &mut [u8],
    ) -> Result<(), FilesystemError> {
        if inode.org == ORG_IMMED {
            buf.fill(0);
            let area = &inode.org_area;
            let start = (pos as usize).min(area.len());
            let n = (area.len() - start).min(buf.len());
            buf[..n].copy_from_slice(&area[start..start + n]);
            return Ok(());
        }
        let mut done = 0usize;
        while done < buf.len() {
            let at = pos + done as u64;
            let (bn, within) = (at / self.bsize, at % self.bsize);
            let take = ((self.bsize - within) as usize).min(buf.len() - done);
            match self.bmap(inode, bn)? {
                Some(pb) => {
                    self.reader
                        .seek(SeekFrom::Start(self.offset + pb * self.bsize + within))?;
                    self.reader.read_exact(&mut buf[done..done + take])?;
                }
                None => buf[done..done + take].fill(0),
            }
            done += take;
        }
        Ok(())
    }

    fn entry_for(
        &mut self,
        name: String,
        path: String,
        ino: u32,
    ) -> Result<FileEntry, FilesystemError> {
        let inode = self.inode(ino)?;
        let kind = inode.mode & S_IFMT;
        let mut e = if kind == S_IFDIR {
            FileEntry::new_directory(name, path, ino as u64)
        } else {
            FileEntry::new_file(name, path, inode.size, ino as u64)
        };
        if kind == S_IFLNK {
            let mut t = vec![0u8; inode.size.min(4096) as usize];
            self.read_inode_range(&inode, 0, &mut t)?;
            e.symlink_target = Some(String::from_utf8_lossy(&t).into_owned());
            e.entry_type = super::entry::EntryType::Symlink;
        } else if kind != S_IFDIR && kind != S_IFREG {
            e.special_type = Some(format!("{:06o}", kind));
        }
        e.mode = Some(inode.mode & 0o7777);
        e.uid = Some(inode.uid);
        e.gid = Some(inode.gid);
        e.modified_unix = Some(inode.mtime as u64);
        Ok(e)
    }
}

impl Inode {
    fn empty() -> Self {
        Self {
            mode: 0,
            uid: 0,
            gid: 0,
            size: 0,
            mtime: 0,
            org: 0,
            org_area: [0; 96],
        }
    }
}

impl<R: Read + Seek + Send> Filesystem for VxfsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        let mut root = FileEntry::root();
        root.location = ROOT_INO as u64;
        Ok(root)
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        let dir = self.inode(entry.location as u32)?;
        if dir.mode & S_IFMT != S_IFDIR {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        let mut data = vec![0u8; dir.size as usize];
        self.read_inode_range(&dir, 0, &mut data)?;
        let bsize = self.bsize as usize;
        let mut found: Vec<(String, u32)> = Vec::new();
        let mut pos = 0usize;
        while pos + 10 <= data.len() {
            if pos % bsize < 4 {
                let nhash = self.u16_at(&data, pos + 2) as usize;
                pos += 4 + 2 * nhash;
                continue;
            }
            let ino = self.u32_at(&data, pos);
            let reclen = self.u16_at(&data, pos + 4) as usize;
            let namelen = self.u16_at(&data, pos + 6) as usize;
            if reclen == 0 {
                pos = (pos / bsize + 1) * bsize;
                continue;
            }
            if ino != 0 && pos + 10 + namelen <= data.len() {
                let name =
                    String::from_utf8_lossy(&data[pos + 10..pos + 10 + namelen]).into_owned();
                found.push((name, ino));
            }
            pos += reclen;
        }
        let base = entry.path.trim_end_matches('/').to_string();
        found
            .into_iter()
            .map(|(name, ino)| {
                let path = format!("{base}/{name}");
                self.entry_for(name, path, ino)
            })
            .collect()
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let inode = self.inode(entry.location as u32)?;
        let mut out = vec![0u8; (inode.size as usize).min(max_bytes)];
        self.read_inode_range(&inode, 0, &mut out)?;
        Ok(out)
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let inode = self.inode(entry.location as u32)?;
        let mut chunk = vec![0u8; 1 << 20];
        let mut pos = 0u64;
        while pos < inode.size {
            let n = ((inode.size - pos) as usize).min(chunk.len());
            self.read_inode_range(&inode, pos, &mut chunk[..n])?;
            writer.write_all(&chunk[..n])?;
            pos += n as u64;
        }
        Ok(inode.size)
    }

    fn volume_label(&self) -> Option<&str> {
        Some(self.label.as_str()).filter(|l| !l.is_empty())
    }

    fn fs_type(&self) -> &str {
        &self.fs_type_name
    }

    fn total_size(&self) -> u64 {
        self.total_blocks * self.bsize
    }

    fn used_size(&self) -> u64 {
        self.total_blocks.saturating_sub(self.free_blocks) * self.bsize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn detects_both_byte_orders() {
        let mut be = vec![0u8; 16384];
        be[8192..8196].copy_from_slice(&VXFS_MAGIC.to_be_bytes());
        assert_eq!(detect(&mut Cursor::new(be), 0), Some((8192, true)));
        let mut le = vec![0u8; 16384];
        le[1024..1028].copy_from_slice(&VXFS_MAGIC.to_le_bytes());
        assert_eq!(detect(&mut Cursor::new(le), 0), Some((1024, false)));
        assert_eq!(detect(&mut Cursor::new(vec![0u8; 16384]), 0), None);
    }

    #[test]
    fn rejects_unsupported_layout_versions() {
        let mut img = vec![0u8; 16384];
        img[8192..8196].copy_from_slice(&VXFS_MAGIC.to_be_bytes());
        img[8196..8200].copy_from_slice(&6u32.to_be_bytes());
        img[8224..8228].copy_from_slice(&8192u32.to_be_bytes());
        let err = VxfsFilesystem::open(Cursor::new(img), 0).err().unwrap();
        assert!(err.to_string().contains("version 6"), "{err}");
    }
}
