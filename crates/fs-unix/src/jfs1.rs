//! AIX JFS (the original "JFS1" of AIX 3.x-5.x), read-only.
//!
//! Not the Linux JFS (that is JFS2, `fs/jfs.rs`). This one has no open-source
//! reader. The superblock fields and reserved inodes are from IBM's AIX
//! `filsys.h` / `inode.h` documentation, and everything else (inode placement,
//! address encoding, indirect blocks, directory format) was worked out from an
//! AIX 4.3.3 rootvg and checked against the files it holds. Big-endian throughout.
//!
//! Superblock (byte 4096, copy at 31 * 4096): magic at 0 (`0x43218765` fsv3,
//! `0x65872143` fsv3p), fragments per allocation group at 8, size in 512-byte
//! units at 16, block size at 20, name at 24, pack at 30, version at 48, and for
//! fsv3p the fragment size at 52 and inodes per allocation group at 56 (fsv3:
//! 4096-byte fragments and one inode per fragment). Compression flag at 60.
//!
//! Inodes are 128 bytes. Allocation group 0's inodes start at byte 32 * 4096,
//! every other group's at the group's first fragment, so inode N lives in group
//! `N / inodes_per_group`. Inode fields: mode (u32) at 4, link count (u16) at 8,
//! uid at 12, gid at 16, size (u32) at 20, mtime at 28. The 48-byte area at 80
//! holds eight direct page addresses, then a virtual and a real indirect address
//! at 112 and 116. A symlink shorter than 48 bytes is stored inline there.
//!
//! Files are read in 4096-byte pages. A page address is a fragment number in its
//! low 28 bits. Up to 8 pages: the direct addresses. Up to 1024 pages: the real
//! indirect block lists every page. Beyond that it is a root block of 512
//! `{virtual, real}` pairs, each real one a block of 1024 page addresses.
//! Address 0 is a hole.
//!
//! Directories are 512-byte chunks of `{u32 ino, u16 reclen, u16 namelen,
//! name}`; a zero `reclen` ends the chunk and a zero `ino` is a hole. `.` and
//! `..` are stored.

use std::io::{Read, Seek, SeekFrom, Write};

use disk_core::entry::FileEntry;
use disk_core::filesystem::{Filesystem, FilesystemError};

const SB_OFFSET: u64 = 4096;
/// fsv3 and fsv3p superblock magics.
pub const JFS1_MAGIC_V3: u32 = 0x4321_8765;
pub const JFS1_MAGIC_V3P: u32 = 0x6587_2143;
const INODE_SIZE: u64 = 128;
const PAGE: u64 = 4096;
const AG0_INODES: u64 = 32 * 4096;
const ROOT_INO: u32 = 2;
const NDADDR: u64 = 8;
const PER_INDIRECT: u64 = 1024;
const ADDR_MASK: u32 = 0x0FFF_FFFF;
const DIRCHUNK: usize = 512;

const S_IFMT: u32 = 0o170000;
const S_IFDIR: u32 = 0o040000;
const S_IFREG: u32 = 0o100000;
const S_IFLNK: u32 = 0o120000;

/// Whether `partition_offset` holds an AIX JFS superblock.
pub fn detect<R: Read + Seek>(reader: &mut R, partition_offset: u64) -> bool {
    let mut m = [0u8; 4];
    reader
        .seek(SeekFrom::Start(partition_offset + SB_OFFSET))
        .and_then(|_| reader.read_exact(&mut m))
        .map(|_| matches!(u32::from_be_bytes(m), JFS1_MAGIC_V3 | JFS1_MAGIC_V3P))
        .unwrap_or(false)
}

#[derive(Debug, Clone)]
struct Inode {
    mode: u32,
    uid: u32,
    gid: u32,
    size: u64,
    mtime: u32,
    area: [u8; 48],
}

/// A read-only AIX JFS volume.
pub struct Jfs1Filesystem<R: Read + Seek + Send> {
    reader: R,
    offset: u64,
    frag_size: u64,
    ag_frags: u64,
    inodes_per_ag: u64,
    total_bytes: u64,
    compressed: bool,
    label: String,
    fs_type_name: String,
}

impl<R: Read + Seek + Send> Jfs1Filesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        let mut sb = [0u8; 64];
        reader.seek(SeekFrom::Start(partition_offset + SB_OFFSET))?;
        reader.read_exact(&mut sb)?;
        let magic = be32(&sb, 0);
        let fsv3p = match magic {
            JFS1_MAGIC_V3P => true,
            JFS1_MAGIC_V3 => false,
            _ => return Err(FilesystemError::Parse("not an AIX JFS volume".into())),
        };
        let ag_frags = be32(&sb, 8) as u64;
        let (frag_size, inodes_per_ag) = if fsv3p {
            (be32(&sb, 52) as u64, be32(&sb, 56) as u64)
        } else {
            (4096, ag_frags)
        };
        if !matches!(frag_size, 512 | 1024 | 2048 | 4096) || ag_frags == 0 || inodes_per_ag == 0 {
            return Err(FilesystemError::Parse(format!(
                "AIX JFS: implausible geometry (fragment {frag_size}, AG {ag_frags}, inodes/AG {inodes_per_ag})"
            )));
        }
        let name = String::from_utf8_lossy(&sb[24..30])
            .trim_end_matches('\0')
            .to_string();
        Ok(Self {
            reader,
            offset: partition_offset,
            frag_size,
            ag_frags,
            inodes_per_ag,
            total_bytes: be32(&sb, 16) as u64 * 512,
            compressed: be32(&sb, 60) as i32 > 0,
            label: name,
            fs_type_name: if fsv3p {
                "AIX JFS (fsv3p)".to_string()
            } else {
                "AIX JFS (fsv3)".to_string()
            },
        })
    }

    fn read_at(&mut self, pos: u64, buf: &mut [u8]) -> Result<(), FilesystemError> {
        self.reader.seek(SeekFrom::Start(self.offset + pos))?;
        self.reader.read_exact(buf)?;
        Ok(())
    }

    fn inode(&mut self, ino: u32) -> Result<Inode, FilesystemError> {
        let (ag, idx) = (
            ino as u64 / self.inodes_per_ag,
            ino as u64 % self.inodes_per_ag,
        );
        let base = if ag == 0 {
            AG0_INODES
        } else {
            ag * self.ag_frags * self.frag_size
        };
        let mut b = [0u8; INODE_SIZE as usize];
        self.read_at(base + idx * INODE_SIZE, &mut b)?;
        let mut area = [0u8; 48];
        area.copy_from_slice(&b[80..128]);
        Ok(Inode {
            mode: be32(&b, 4) & 0xFFFF,
            uid: be32(&b, 12),
            gid: be32(&b, 16),
            size: be32(&b, 20) as u64,
            mtime: be32(&b, 28),
            area,
        })
    }

    fn addr_bytes(&self, addr: u32) -> Option<u64> {
        let frag = (addr & ADDR_MASK) as u64;
        (frag != 0).then_some(frag * self.frag_size)
    }

    fn indirect_entry(
        &mut self,
        block_addr: u32,
        index: u64,
        stride: u64,
    ) -> Result<u32, FilesystemError> {
        let Some(at) = self.addr_bytes(block_addr) else {
            return Ok(0);
        };
        let mut w = [0u8; 4];
        self.read_at(at + index * stride, &mut w)?;
        Ok(u32::from_be_bytes(w))
    }

    /// Byte offset of page `page` of `inode`, or `None` for a hole.
    fn page_addr(&mut self, inode: &Inode, page: u64) -> Result<Option<u64>, FilesystemError> {
        let pages = inode.size.div_ceil(PAGE);
        let real_indirect = be32(&inode.area, 36);
        let addr = if pages <= NDADDR {
            be32(&inode.area, page as usize * 4)
        } else if pages <= PER_INDIRECT {
            self.indirect_entry(real_indirect, page, 4)?
        } else {
            // Root of {virtual, real} pairs: the real half (+4) names a block of page addresses.
            let second = self.indirect_entry(real_indirect, (page / PER_INDIRECT) * 2 + 1, 4)?;
            self.indirect_entry(second, page % PER_INDIRECT, 4)?
        };
        Ok(self.addr_bytes(addr))
    }

    /// Fill `buf` from byte `pos` of `inode`; holes read as zero.
    fn read_inode(
        &mut self,
        inode: &Inode,
        pos: u64,
        buf: &mut [u8],
    ) -> Result<(), FilesystemError> {
        let mut done = 0usize;
        while done < buf.len() {
            let at = pos + done as u64;
            let (page, within) = (at / PAGE, at % PAGE);
            let take = ((PAGE - within) as usize).min(buf.len() - done);
            match self.page_addr(inode, page)? {
                Some(p) => self.read_at(p + within, &mut buf[done..done + take])?,
                None => buf[done..done + take].fill(0),
            }
            done += take;
        }
        Ok(())
    }

    fn symlink_target(&mut self, inode: &Inode) -> Result<String, FilesystemError> {
        let n = inode.size.min(4096) as usize;
        let bytes = if n < 48 {
            inode.area[..n].to_vec()
        } else {
            let mut b = vec![0u8; n];
            self.read_inode(inode, 0, &mut b)?;
            b
        };
        Ok(String::from_utf8_lossy(&bytes).into_owned())
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
            e.symlink_target = Some(self.symlink_target(&inode)?);
            e.entry_type = disk_core::entry::EntryType::Symlink;
        } else if kind != S_IFDIR && kind != S_IFREG {
            e.special_type = Some(format!("{kind:06o}"));
        }
        e.mode = Some(inode.mode & 0o7777);
        e.uid = Some(inode.uid);
        e.gid = Some(inode.gid);
        e.modified_unix = Some(inode.mtime as u64);
        Ok(e)
    }

    fn refuse_compressed(&self) -> Result<(), FilesystemError> {
        if self.compressed {
            return Err(FilesystemError::Unsupported(
                "this AIX JFS volume uses compression, which is not supported yet (docs/stubs.md)"
                    .into(),
            ));
        }
        Ok(())
    }
}

fn be32(b: &[u8], o: usize) -> u32 {
    u32::from_be_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}

impl<R: Read + Seek + Send> Filesystem for Jfs1Filesystem<R> {
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
        self.read_inode(&dir, 0, &mut data)?;
        let mut found = Vec::new();
        let mut p = 0usize;
        while p + 8 <= data.len() {
            let ino = be32(&data, p);
            let reclen = u16::from_be_bytes([data[p + 4], data[p + 5]]) as usize;
            let namelen = u16::from_be_bytes([data[p + 6], data[p + 7]]) as usize;
            if reclen == 0 {
                p = (p / DIRCHUNK + 1) * DIRCHUNK;
                continue;
            }
            if ino != 0 && p + 8 + namelen <= data.len() {
                let name = String::from_utf8_lossy(&data[p + 8..p + 8 + namelen]).into_owned();
                if name != "." && name != ".." {
                    found.push((name, ino));
                }
            }
            p += reclen;
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
        self.refuse_compressed()?;
        let inode = self.inode(entry.location as u32)?;
        let mut out = vec![0u8; (inode.size as usize).min(max_bytes)];
        self.read_inode(&inode, 0, &mut out)?;
        Ok(out)
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        self.refuse_compressed()?;
        let inode = self.inode(entry.location as u32)?;
        let mut chunk = vec![0u8; 1 << 20];
        let mut pos = 0u64;
        while pos < inode.size {
            let n = ((inode.size - pos) as usize).min(chunk.len());
            self.read_inode(&inode, pos, &mut chunk[..n])?;
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
        self.total_bytes
    }

    fn used_size(&self) -> u64 {
        self.total_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// fsv3p, 4 KiB fragments, 64 inodes per AG; root dir + a 9-page file through a single indirect block.
    fn sample() -> Vec<u8> {
        let mut d = vec![0u8; 1 << 20];
        let sb = 4096;
        d[sb..sb + 4].copy_from_slice(&JFS1_MAGIC_V3P.to_be_bytes());
        d[sb + 8..sb + 12].copy_from_slice(&64u32.to_be_bytes());
        d[sb + 16..sb + 20].copy_from_slice(&2048u32.to_be_bytes());
        d[sb + 24..sb + 25].copy_from_slice(b"/");
        d[sb + 48..sb + 52].copy_from_slice(&1u32.to_be_bytes());
        d[sb + 52..sb + 56].copy_from_slice(&4096u32.to_be_bytes());
        d[sb + 56..sb + 60].copy_from_slice(&64u32.to_be_bytes());
        let ino = |d: &mut Vec<u8>, n: usize, mode: u32, size: u32, addrs: &[u32], rind: u32| {
            let o = 32 * 4096 + n * 128;
            d[o + 4..o + 8].copy_from_slice(&mode.to_be_bytes());
            d[o + 20..o + 24].copy_from_slice(&size.to_be_bytes());
            for (i, a) in addrs.iter().enumerate() {
                d[o + 80 + i * 4..o + 84 + i * 4].copy_from_slice(&a.to_be_bytes());
            }
            d[o + 116..o + 120].copy_from_slice(&rind.to_be_bytes());
        };
        ino(&mut d, 2, 0o040755, 512, &[40], 0);
        ino(
            &mut d,
            7,
            0o100644,
            9 * 4096,
            &[50, 51, 52, 53, 54, 55, 56, 57],
            60,
        );
        let dir = 40 * 4096;
        let mut put = |p: usize, i: u32, name: &[u8], reclen: u16| {
            d[dir + p..dir + p + 4].copy_from_slice(&i.to_be_bytes());
            d[dir + p + 4..dir + p + 6].copy_from_slice(&reclen.to_be_bytes());
            d[dir + p + 6..dir + p + 8].copy_from_slice(&(name.len() as u16).to_be_bytes());
            d[dir + p + 8..dir + p + 8 + name.len()].copy_from_slice(name);
        };
        put(0, 2, b".", 12);
        put(12, 2, b"..", 12);
        put(24, 7, b"big", 512 - 24);
        for (i, a) in (50u32..58).chain([58]).enumerate() {
            d[60 * 4096 + i * 4..60 * 4096 + i * 4 + 4].copy_from_slice(&a.to_be_bytes());
            d[a as usize * 4096] = i as u8 + 1;
        }
        d
    }

    #[test]
    fn lists_root_and_reads_through_the_indirect_block() {
        let mut fs = Jfs1Filesystem::open(Cursor::new(sample()), 0).expect("open");
        assert_eq!(fs.fs_type(), "AIX JFS (fsv3p)");
        let root = fs.root().unwrap();
        let kids = fs.list_directory(&root).unwrap();
        assert_eq!(kids.len(), 1, "dot entries are not listed");
        let data = fs.read_file(&kids[0], usize::MAX).unwrap();
        assert_eq!(data.len(), 9 * 4096);
        let firsts: Vec<u8> = (0..9).map(|p| data[p * 4096]).collect();
        assert_eq!(
            firsts,
            (1..=9).collect::<Vec<u8>>(),
            "page 9 comes from the indirect block"
        );
    }

    #[test]
    fn detects_only_jfs1_magics() {
        let mut d = vec![0u8; 8192];
        assert!(!detect(&mut Cursor::new(d.clone()), 0));
        d[4096..4100].copy_from_slice(&JFS1_MAGIC_V3.to_be_bytes());
        assert!(detect(&mut Cursor::new(d), 0));
    }
}
