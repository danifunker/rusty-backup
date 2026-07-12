//! Minix filesystem (V1 / V2 / V3) — read driver.
//!
//! The Minix FS is the small, thoroughly documented Unix filesystem shipped
//! with Minix and used by early Linux (it was Linux's only filesystem until
//! ext appeared). Layout, from block 0 (blocks are 1024 bytes on V1/V2; V3
//! stores its block size in the superblock):
//!
//! ```text
//!   block 0            boot block (unused by the fs)
//!   block 1            superblock
//!   next imap_blocks   inode bitmap   (bit set = inode in use; bit 0 reserved)
//!   next zmap_blocks   zone bitmap    (bit set = zone in use; bit 0 reserved)
//!   next N blocks      inode table    (inode 1 = root; inode 0 does not exist)
//!   firstdatazone..    data zones
//! ```
//!
//! Three on-disk generations, distinguished by the superblock magic:
//!
//! | Magic  | Version | Name len | Dirent | Inode size |
//! |--------|---------|----------|--------|------------|
//! | 0x137F | V1      | 14       | 16     | 32         |
//! | 0x138F | V1      | 30       | 32     | 32         |
//! | 0x2468 | V2      | 14       | 16     | 64         |
//! | 0x2478 | V2      | 30       | 32     | 64         |
//! | 0x4D5A | V3      | 60       | 64     | 64         |
//!
//! V1 inodes are 32 bytes with 16-bit zone pointers (7 direct + single +
//! double indirect). V2/V3 inodes are 64 bytes with 32-bit zone pointers
//! (7 direct + single + double + triple indirect). V3 also uses a distinct
//! superblock layout (32-bit `ninodes`, an explicit block-size field) and a
//! 32-bit dirent inode field.
//!
//! Everything is little-endian. All offsets below were verified byte-for-byte
//! against real `mkfs.minix` (util-linux 2.39.3) V1/V2/V3 images and
//! cross-checked with `fsck.minix`.
//!
//! This module is the read half of the Minix quartet (Browse). Edit, create,
//! and fsck build on it (`docs/filesystem_completion_plan.md` Part 2).

use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};

// Superblock magic numbers (little-endian u16).
const MAGIC_V1_14: u16 = 0x137F;
const MAGIC_V1_30: u16 = 0x138F;
const MAGIC_V2_14: u16 = 0x2468;
const MAGIC_V2_30: u16 = 0x2478;
const MAGIC_V3: u16 = 0x4D5A;

/// V1/V2 magic lives at superblock offset 16; V3 magic at offset 24.
const SB_MAGIC_OFF_V12: usize = 16;
const SB_MAGIC_OFF_V3: usize = 24;

const ROOT_INO: u32 = 1;
/// Superblock is block 1 (byte offset 1024) regardless of block size.
const SUPERBLOCK_OFFSET: u64 = 1024;
/// Number of direct zone pointers in every inode generation.
const DIRECT_ZONES: usize = 7;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MinixVersion {
    V1,
    V2,
    V3,
}

/// Detect whether the block-1 buffer holds a Minix superblock, returning the
/// display name. Used by `detect_filesystem_type`. `buf` must be at least 32
/// bytes (a superblock header); callers pass the sector-aligned 512-byte read
/// at offset 1024.
pub fn detect_magic(buf: &[u8]) -> Option<&'static str> {
    if buf.len() < 32 {
        return None;
    }
    let magic_v3 = u16::from_le_bytes([buf[SB_MAGIC_OFF_V3], buf[SB_MAGIC_OFF_V3 + 1]]);
    if magic_v3 == MAGIC_V3 {
        return Some("minix");
    }
    let magic_v12 = u16::from_le_bytes([buf[SB_MAGIC_OFF_V12], buf[SB_MAGIC_OFF_V12 + 1]]);
    match magic_v12 {
        MAGIC_V1_14 | MAGIC_V1_30 | MAGIC_V2_14 | MAGIC_V2_30 => Some("minix"),
        _ => None,
    }
}

#[derive(Clone, Debug)]
pub struct MinixSuperblock {
    pub version: MinixVersion,
    pub ninodes: u32,
    pub imap_blocks: u32,
    pub zmap_blocks: u32,
    pub firstdatazone: u32,
    pub log_zone_size: u32,
    pub max_size: u32,
    /// Total number of zones in the volume.
    pub zones: u32,
    pub block_size: u32,
    /// Directory name field length (14 / 30 / 60).
    pub name_len: usize,
    /// On-disk directory entry stride (16 / 32 / 64).
    pub dir_entry_size: usize,
    /// On-disk inode size (32 for V1, 64 for V2/V3).
    pub inode_size: u64,
    pub magic: u16,
}

impl MinixSuperblock {
    pub fn parse(buf: &[u8]) -> Result<Self, FilesystemError> {
        if buf.len() < 32 {
            return Err(FilesystemError::InvalidData(
                "Minix superblock buffer too small".into(),
            ));
        }
        let le16 = |o: usize| u16::from_le_bytes([buf[o], buf[o + 1]]);
        let le32 = |o: usize| u32::from_le_bytes([buf[o], buf[o + 1], buf[o + 2], buf[o + 3]]);

        let magic_v3 = le16(SB_MAGIC_OFF_V3);
        let (version, name_len, inode_size, magic);
        let ninodes;
        let imap_blocks;
        let zmap_blocks;
        let firstdatazone;
        let log_zone_size;
        let max_size;
        let zones;
        let block_size;

        if magic_v3 == MAGIC_V3 {
            // V3 superblock: 32-bit ninodes, explicit block size.
            //   ninodes@0(u32) pad@4 imap_blocks@6 zmap_blocks@8
            //   firstdatazone@10 log_zone_size@12 pad@14 max_size@16(u32)
            //   zones@20(u32) magic@24 pad@26 blocksize@28 disk_version@30
            version = MinixVersion::V3;
            name_len = 60;
            inode_size = 64;
            magic = magic_v3;
            ninodes = le32(0);
            imap_blocks = le16(6) as u32;
            zmap_blocks = le16(8) as u32;
            firstdatazone = le16(10) as u32;
            log_zone_size = le16(12) as u32;
            max_size = le32(16);
            zones = le32(20);
            block_size = le16(28) as u32;
        } else {
            // V1/V2 superblock:
            //   ninodes@0(u16) nzones@2(u16) imap_blocks@4 zmap_blocks@6
            //   firstdatazone@8 log_zone_size@10 max_size@12(u32) magic@16
            //   state@18 [V2: zones@20(u32)]
            let magic_v12 = le16(SB_MAGIC_OFF_V12);
            let (v, nl) = match magic_v12 {
                MAGIC_V1_14 => (MinixVersion::V1, 14),
                MAGIC_V1_30 => (MinixVersion::V1, 30),
                MAGIC_V2_14 => (MinixVersion::V2, 14),
                MAGIC_V2_30 => (MinixVersion::V2, 30),
                _ => {
                    return Err(FilesystemError::InvalidData(format!(
                        "not a Minix superblock (magic 0x{magic_v12:04X} / 0x{magic_v3:04X})"
                    )));
                }
            };
            version = v;
            name_len = nl;
            inode_size = if v == MinixVersion::V1 { 32 } else { 64 };
            magic = magic_v12;
            ninodes = le16(0) as u32;
            let nzones = le16(2) as u32;
            imap_blocks = le16(4) as u32;
            zmap_blocks = le16(6) as u32;
            firstdatazone = le16(8) as u32;
            log_zone_size = le16(10) as u32;
            max_size = le32(12);
            zones = if v == MinixVersion::V2 {
                le32(20)
            } else {
                nzones
            };
            block_size = 1024;
        }

        let dir_entry_size = if version == MinixVersion::V3 {
            4 + name_len
        } else {
            2 + name_len
        };

        // Geometry sanity — reject nonsense so a false magic hit on random data
        // fails to open rather than reading wild offsets.
        if ninodes == 0
            || imap_blocks == 0
            || zmap_blocks == 0
            || zones == 0
            || firstdatazone < 2
            || !matches!(block_size, 1024 | 2048 | 4096)
            || log_zone_size > 8
            || firstdatazone > zones
        {
            return Err(FilesystemError::InvalidData(format!(
                "implausible Minix geometry (ninodes={ninodes} imap={imap_blocks} \
                 zmap={zmap_blocks} firstdatazone={firstdatazone} zones={zones} \
                 block_size={block_size})"
            )));
        }

        Ok(MinixSuperblock {
            version,
            ninodes,
            imap_blocks,
            zmap_blocks,
            firstdatazone,
            log_zone_size,
            max_size,
            zones,
            block_size,
            name_len,
            dir_entry_size,
            inode_size,
            magic,
        })
    }

    /// Size of one zone in bytes (`block_size << log_zone_size`).
    fn zone_size(&self) -> u64 {
        (self.block_size as u64) << self.log_zone_size
    }

    /// Size of an on-disk zone pointer (2 bytes on V1, 4 on V2/V3).
    fn ptr_size(&self) -> u64 {
        if self.version == MinixVersion::V1 {
            2
        } else {
            4
        }
    }

    /// Zone pointers per indirect block.
    fn ptrs_per_zone(&self) -> u64 {
        self.zone_size() / self.ptr_size()
    }

    /// Number of zone slots in an inode (9 for V1, 10 for V2/V3).
    fn zones_per_inode(&self) -> usize {
        if self.version == MinixVersion::V1 {
            9
        } else {
            10
        }
    }

    /// First block of the inode table.
    fn inode_table_block(&self) -> u64 {
        2 + self.imap_blocks as u64 + self.zmap_blocks as u64
    }
}

/// A parsed Minix inode, normalized across V1 and V2/V3 layouts.
#[derive(Clone, Debug)]
pub struct MinixInode {
    pub ino: u32,
    pub mode: u16,
    pub nlinks: u16,
    pub uid: u16,
    pub gid: u16,
    pub size: u32,
    pub mtime: u32,
    /// Zone pointers: 9 slots on V1 (7 direct, single, double), 10 on V2/V3
    /// (7 direct, single, double, triple). Unused slots are 0.
    pub zones: [u32; 10],
}

impl MinixInode {
    fn parse(ino: u32, buf: &[u8], version: MinixVersion) -> Self {
        let le16 = |o: usize| u16::from_le_bytes([buf[o], buf[o + 1]]);
        let le32 = |o: usize| u32::from_le_bytes([buf[o], buf[o + 1], buf[o + 2], buf[o + 3]]);
        let mut zones = [0u32; 10];
        if version == MinixVersion::V1 {
            // 32-byte V1 inode: mode@0 uid@2 size@4 time@8 gid@12(u8)
            // nlinks@13(u8) zone[9]@14(u16 each).
            let mode = le16(0);
            let uid = le16(2);
            let size = le32(4);
            let mtime = le32(8);
            let gid = buf[12] as u16;
            let nlinks = buf[13] as u16;
            for (i, z) in zones.iter_mut().take(9).enumerate() {
                *z = le16(14 + i * 2) as u32;
            }
            MinixInode {
                ino,
                mode,
                nlinks,
                uid,
                gid,
                size,
                mtime,
                zones,
            }
        } else {
            // 64-byte V2/V3 inode: mode@0 nlinks@2 uid@4 gid@6 size@8(u32)
            // atime@12 mtime@16 ctime@20 zone[10]@24(u32 each).
            let mode = le16(0);
            let nlinks = le16(2);
            let uid = le16(4);
            let gid = le16(6);
            let size = le32(8);
            let mtime = le32(16);
            for (i, z) in zones.iter_mut().enumerate() {
                *z = le32(24 + i * 4);
            }
            MinixInode {
                ino,
                mode,
                nlinks,
                uid,
                gid,
                size,
                mtime,
                zones,
            }
        }
    }

    fn type_bits(&self) -> u16 {
        self.mode & 0xF000
    }
    pub fn is_dir(&self) -> bool {
        self.type_bits() == 0x4000
    }
    fn is_symlink(&self) -> bool {
        self.type_bits() == 0xA000
    }
    fn is_regular(&self) -> bool {
        self.type_bits() == 0x8000
    }

    /// A zeroed inode (all fields 0) — what a free inode slot must look like so
    /// the allocator reuses it.
    fn empty(ino: u32) -> Self {
        MinixInode {
            ino,
            mode: 0,
            nlinks: 0,
            uid: 0,
            gid: 0,
            size: 0,
            mtime: 0,
            zones: [0; 10],
        }
    }

    /// Serialize into the on-disk inode (32 bytes for V1, 64 for V2/V3).
    fn serialize(&self, version: MinixVersion) -> Vec<u8> {
        if version == MinixVersion::V1 {
            let mut buf = vec![0u8; 32];
            buf[0..2].copy_from_slice(&self.mode.to_le_bytes());
            buf[2..4].copy_from_slice(&self.uid.to_le_bytes());
            buf[4..8].copy_from_slice(&self.size.to_le_bytes());
            buf[8..12].copy_from_slice(&self.mtime.to_le_bytes());
            buf[12] = self.gid as u8;
            buf[13] = self.nlinks as u8;
            for (i, z) in self.zones.iter().take(9).enumerate() {
                buf[14 + i * 2..16 + i * 2].copy_from_slice(&(*z as u16).to_le_bytes());
            }
            buf
        } else {
            let mut buf = vec![0u8; 64];
            buf[0..2].copy_from_slice(&self.mode.to_le_bytes());
            buf[2..4].copy_from_slice(&self.nlinks.to_le_bytes());
            buf[4..6].copy_from_slice(&self.uid.to_le_bytes());
            buf[6..8].copy_from_slice(&self.gid.to_le_bytes());
            buf[8..12].copy_from_slice(&self.size.to_le_bytes());
            // atime / mtime / ctime — write the same stamp to all three.
            buf[12..16].copy_from_slice(&self.mtime.to_le_bytes());
            buf[16..20].copy_from_slice(&self.mtime.to_le_bytes());
            buf[20..24].copy_from_slice(&self.mtime.to_le_bytes());
            for (i, z) in self.zones.iter().enumerate() {
                buf[24 + i * 4..28 + i * 4].copy_from_slice(&z.to_le_bytes());
            }
            buf
        }
    }
}

pub struct MinixFilesystem<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    sb: MinixSuperblock,
    fs_type: &'static str,
    /// Allocated data zones, counted from the zone bitmap at open. Used by the
    /// `&self` `used_size`; refreshed by the edit path when writes land.
    used_zones: u32,
}

impl<R: Read + Seek> MinixFilesystem<R> {
    /// Open a Minix filesystem at the given byte offset within `reader`.
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        let mut sb_block = [0u8; 1024];
        reader.seek(SeekFrom::Start(partition_offset + SUPERBLOCK_OFFSET))?;
        reader.read_exact(&mut sb_block)?;
        let sb = MinixSuperblock::parse(&sb_block)?;
        let fs_type = match sb.version {
            MinixVersion::V1 => "Minix FS (V1)",
            MinixVersion::V2 => "Minix FS (V2)",
            MinixVersion::V3 => "Minix FS (V3)",
        };
        let mut fs = MinixFilesystem {
            reader,
            partition_offset,
            sb,
            fs_type,
            used_zones: 0,
        };
        fs.used_zones = fs.used_data_zones()?;
        Ok(fs)
    }

    /// Read `buf.len()` bytes at partition-relative byte `off`.
    fn read_at(&mut self, off: u64, buf: &mut [u8]) -> Result<(), FilesystemError> {
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + off))?;
        self.reader.read_exact(buf)?;
        Ok(())
    }

    /// Read the `zone_size`-byte zone `zno` into a fresh buffer. Zone 0 is the
    /// sparse/hole sentinel and yields zeros.
    fn read_zone(&mut self, zno: u32) -> Result<Vec<u8>, FilesystemError> {
        let zs = self.sb.zone_size() as usize;
        let mut buf = vec![0u8; zs];
        if zno != 0 {
            self.read_at(zno as u64 * self.sb.zone_size(), &mut buf)?;
        }
        Ok(buf)
    }

    fn read_inode(&mut self, ino: u32) -> Result<MinixInode, FilesystemError> {
        if ino == 0 || ino > self.sb.ninodes {
            return Err(FilesystemError::InvalidData(format!(
                "Minix inode {ino} out of range (1..={})",
                self.sb.ninodes
            )));
        }
        let byte = self.sb.inode_table_block() * self.sb.block_size as u64
            + (ino as u64 - 1) * self.sb.inode_size;
        // Read the whole containing block (keeps raw-device reads aligned) and
        // slice the inode out.
        let bs = self.sb.block_size as u64;
        let block_byte = (byte / bs) * bs;
        let mut block = vec![0u8; bs as usize];
        self.read_at(block_byte, &mut block)?;
        let in_block = (byte - block_byte) as usize;
        let sz = self.sb.inode_size as usize;
        Ok(MinixInode::parse(
            ino,
            &block[in_block..in_block + sz],
            self.sb.version,
        ))
    }

    /// Resolve a zone pointer read from an indirect block.
    fn read_ptr(&self, block: &[u8], idx: usize) -> u32 {
        if self.sb.version == MinixVersion::V1 {
            let o = idx * 2;
            u16::from_le_bytes([block[o], block[o + 1]]) as u32
        } else {
            let o = idx * 4;
            u32::from_le_bytes([block[o], block[o + 1], block[o + 2], block[o + 3]])
        }
    }

    /// Append physical data zones from an indirect zone of the given `level`
    /// (1 = single, 2 = double, 3 = triple) until `out` covers `needed`
    /// logical zones. A zero pointer is a sparse hole: it contributes
    /// `ptrs^level` zero-zones.
    fn collect_indirect(
        &mut self,
        zone: u32,
        level: u32,
        needed: usize,
        out: &mut Vec<u32>,
    ) -> Result<(), FilesystemError> {
        if out.len() >= needed {
            return Ok(());
        }
        let ptrs = self.sb.ptrs_per_zone() as usize;
        let span = ptrs.pow(level); // logical zones covered by this subtree
        if zone == 0 {
            for _ in 0..span {
                if out.len() >= needed {
                    break;
                }
                out.push(0);
            }
            return Ok(());
        }
        let block = self.read_zone(zone)?;
        for i in 0..ptrs {
            if out.len() >= needed {
                break;
            }
            let ptr = self.read_ptr(&block, i);
            if level == 1 {
                out.push(ptr);
            } else {
                self.collect_indirect(ptr, level - 1, needed, out)?;
            }
        }
        Ok(())
    }

    /// Physical zone numbers for the first `needed` logical zones of `inode`,
    /// walking direct then single/double/triple indirect pointers.
    fn collect_zones(
        &mut self,
        inode: &MinixInode,
        needed: usize,
    ) -> Result<Vec<u32>, FilesystemError> {
        let mut out = Vec::with_capacity(needed);
        for i in 0..DIRECT_ZONES {
            if out.len() >= needed {
                break;
            }
            out.push(inode.zones[i]);
        }
        // Slot 7 = single indirect, 8 = double, 9 = triple (V2/V3 only).
        let indirect_slots = self.sb.zones_per_inode();
        if out.len() < needed && indirect_slots > 7 {
            self.collect_indirect(inode.zones[7], 1, needed, &mut out)?;
        }
        if out.len() < needed && indirect_slots > 8 {
            self.collect_indirect(inode.zones[8], 2, needed, &mut out)?;
        }
        if out.len() < needed && indirect_slots > 9 {
            self.collect_indirect(inode.zones[9], 3, needed, &mut out)?;
        }
        out.truncate(needed);
        Ok(out)
    }

    /// Read up to `max_bytes` of an inode's data.
    fn read_inode_data(
        &mut self,
        inode: &MinixInode,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let want = (inode.size as usize).min(max_bytes);
        if want == 0 {
            return Ok(Vec::new());
        }
        let zs = self.sb.zone_size() as usize;
        let needed = want.div_ceil(zs);
        let zones = self.collect_zones(inode, needed)?;
        let mut out = Vec::with_capacity(want);
        for zno in zones {
            if out.len() >= want {
                break;
            }
            let data = self.read_zone(zno)?;
            let take = zs.min(want - out.len());
            out.extend_from_slice(&data[..take]);
        }
        Ok(out)
    }

    /// Zone bitmap → highest allocated data zone (for smart-compaction floor).
    /// Returns `firstdatazone` (end of metadata) when no data zone is used.
    fn highest_used_zone(&mut self) -> Result<u32, FilesystemError> {
        let bmap = self.read_zone_bitmap()?;
        let fdz = self.sb.firstdatazone;
        // Real data-zone bits are 1..=(zones - firstdatazone); bit 0 is the
        // reserved sentinel and bits past the last zone are mkfs padding
        // (set to 1) — clamp so padding never inflates the floor.
        let max_bit = (self.sb.zones - fdz) as usize;
        let mut highest = fdz;
        for bit in 1..=max_bit {
            if bit_set(&bmap, bit) {
                highest = fdz + bit as u32 - 1;
            }
        }
        Ok(highest)
    }

    /// Count of allocated data zones (set bits in the real range of the zone
    /// bitmap).
    fn used_data_zones(&mut self) -> Result<u32, FilesystemError> {
        let bmap = self.read_zone_bitmap()?;
        let max_bit = (self.sb.zones - self.sb.firstdatazone) as usize;
        let mut count = 0u32;
        for bit in 1..=max_bit {
            if bit_set(&bmap, bit) {
                count += 1;
            }
        }
        Ok(count)
    }

    fn read_zone_bitmap(&mut self) -> Result<Vec<u8>, FilesystemError> {
        let bs = self.sb.block_size as u64;
        let start = (2 + self.sb.imap_blocks as u64) * bs;
        let len = (self.sb.zmap_blocks as u64 * bs) as usize;
        let mut buf = vec![0u8; len];
        self.read_at(start, &mut buf)?;
        Ok(buf)
    }

    fn entry_from_inode(&mut self, name: &str, parent_path: &str, inode: &MinixInode) -> FileEntry {
        let path = join_path(parent_path, name);
        let (entry_type, special_type, symlink_target) = if inode.is_dir() {
            (EntryType::Directory, None, None)
        } else if inode.is_symlink() {
            let target = self
                .read_inode_data(inode, 4096)
                .ok()
                .map(|b| String::from_utf8_lossy(&b).into_owned());
            (EntryType::Symlink, None, target)
        } else if inode.is_regular() {
            (EntryType::File, None, None)
        } else {
            (
                EntryType::Special,
                Some(special_kind(inode.mode).into()),
                None,
            )
        };
        FileEntry {
            name: name.to_string(),
            path,
            entry_type,
            size: inode.size as u64,
            location: inode.ino as u64,
            modified: None,
            type_code: None,
            creator_code: None,
            symlink_target,
            special_type,
            mode: Some(inode.mode as u32),
            uid: Some(inode.uid as u32),
            gid: Some(inode.gid as u32),
            resource_fork_size: None,
            aux_type: None,
            link_target_cnid: None,
            amiga_protection: None,
            amiga_comment: None,
            amiga_date: None,
            dos_attributes: None,
            finder_flags: None,
            prodos_file_type: None,
            mac_dates: None,
        }
    }
}

/// LSB-first bit test within a byte array.
fn bit_set(bitmap: &[u8], bit: usize) -> bool {
    let byte = bit / 8;
    byte < bitmap.len() && (bitmap[byte] >> (bit % 8)) & 1 == 1
}

/// Read a directory entry's inode field (u16 on V1/V2, u32 on V3).
fn dirent_ino(chunk: &[u8], ino_field: usize) -> u32 {
    if ino_field == 4 {
        u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]])
    } else {
        u16::from_le_bytes([chunk[0], chunk[1]]) as u32
    }
}

/// Serialize one directory entry (inode field + NUL-padded name) into `buf`.
fn encode_dirent(buf: &mut [u8], ino_field: usize, ino: u32, name: &[u8]) {
    for b in buf.iter_mut() {
        *b = 0;
    }
    if ino_field == 4 {
        buf[0..4].copy_from_slice(&ino.to_le_bytes());
    } else {
        buf[0..2].copy_from_slice(&(ino as u16).to_le_bytes());
    }
    let n = name.len().min(buf.len() - ino_field);
    buf[ino_field..ino_field + n].copy_from_slice(&name[..n]);
}

fn special_kind(mode: u16) -> &'static str {
    match mode & 0xF000 {
        0x6000 => "block device",
        0x2000 => "char device",
        0x1000 => "fifo",
        0xC000 => "socket",
        _ => "special",
    }
}

fn join_path(parent: &str, name: &str) -> String {
    if parent.ends_with('/') {
        format!("{parent}{name}")
    } else {
        format!("{parent}/{name}")
    }
}

impl<R: Read + Seek + Send> Filesystem for MinixFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        let ino = self.read_inode(ROOT_INO)?;
        if !ino.is_dir() {
            return Err(FilesystemError::InvalidData(format!(
                "Minix root inode is not a directory (mode=0o{:o})",
                ino.mode
            )));
        }
        let mut entry = self.entry_from_inode("/", "", &ino);
        // entry_from_inode joins "" + "/" into "//"; the root path is just "/".
        entry.path = "/".into();
        Ok(entry)
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if !entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        let dir_ino = self.read_inode(entry.location as u32)?;
        if !dir_ino.is_dir() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        let data = self.read_inode_data(&dir_ino, dir_ino.size as usize)?;
        let stride = self.sb.dir_entry_size;
        let ino_field = if self.sb.version == MinixVersion::V3 {
            4
        } else {
            2
        };
        let parent_path = if entry.path == "/" { "" } else { &entry.path };

        let mut entries = Vec::new();
        for chunk in data.chunks(stride) {
            if chunk.len() < stride {
                break;
            }
            let child_ino = if ino_field == 4 {
                u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]])
            } else {
                u16::from_le_bytes([chunk[0], chunk[1]]) as u32
            };
            if child_ino == 0 {
                continue; // free / deleted slot
            }
            let name_bytes = &chunk[ino_field..stride];
            let end = name_bytes
                .iter()
                .position(|&b| b == 0)
                .unwrap_or(name_bytes.len());
            let name = String::from_utf8_lossy(&name_bytes[..end]).into_owned();
            if name == "." || name == ".." || name.is_empty() {
                continue;
            }
            // A damaged entry pointing past the inode table shouldn't abort the
            // whole listing — surface the name as a placeholder file instead.
            match self.read_inode(child_ino) {
                Ok(child) => entries.push(self.entry_from_inode(&name, parent_path, &child)),
                Err(_) => {
                    let path = join_path(parent_path, &name);
                    entries.push(FileEntry::new_file(name, path, 0, child_ino as u64));
                }
            }
        }
        Ok(entries)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        if entry.is_directory() {
            return Err(FilesystemError::InvalidData(format!(
                "Minix read_file on directory: {}",
                entry.path
            )));
        }
        let inode = self.read_inode(entry.location as u32)?;
        self.read_inode_data(&inode, max_bytes)
    }

    fn volume_label(&self) -> Option<&str> {
        // Minix has no volume label field.
        None
    }

    fn fs_type(&self) -> &str {
        self.fs_type
    }

    fn total_size(&self) -> u64 {
        self.sb.zones as u64 * self.sb.zone_size()
    }

    fn used_size(&self) -> u64 {
        // Metadata region (blocks 0..firstdatazone) is always in use; add the
        // allocated data zones counted at open.
        let meta = self.sb.firstdatazone as u64 * self.sb.zone_size();
        meta + self.used_zones as u64 * self.sb.zone_size()
    }

    fn allocation_unit(&self) -> Option<u64> {
        Some(self.sb.zone_size())
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        let zone = self.highest_used_zone()?;
        Ok((zone as u64 + 1) * self.sb.zone_size())
    }
}

// ---- Write primitives (edit stage) ----
//
// Minix has no free-count fields in the superblock — free inodes/zones are
// computed from the two bitmaps — so edits never touch the superblock. Bit
// convention: set = used, bit 0 reserved; inode N -> imap bit N; zone Z ->
// zmap bit `Z - firstdatazone + 1`. Bitmap edits are single-byte read/modify/
// write; the edit path operates on image files, so sub-block writes are fine.

impl<R: Read + Write + Seek> MinixFilesystem<R> {
    fn write_at(&mut self, off: u64, data: &[u8]) -> Result<(), FilesystemError> {
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + off))?;
        self.reader.write_all(data)?;
        Ok(())
    }

    fn write_zone(&mut self, zno: u32, data: &[u8]) -> Result<(), FilesystemError> {
        self.write_at(zno as u64 * self.sb.zone_size(), data)
    }

    fn write_inode(&mut self, inode: &MinixInode) -> Result<(), FilesystemError> {
        let raw = inode.serialize(self.sb.version);
        let off = self.sb.inode_table_block() * self.sb.block_size as u64
            + (inode.ino as u64 - 1) * self.sb.inode_size;
        self.write_at(off, &raw)
    }

    /// Flip one bit in a bitmap. `region_start` is the byte offset of the
    /// bitmap's first block; `bit` is the bit index within it.
    fn set_bitmap_bit(
        &mut self,
        region_start: u64,
        bit: usize,
        used: bool,
    ) -> Result<(), FilesystemError> {
        let byte_off = region_start + (bit / 8) as u64;
        let mut b = [0u8; 1];
        self.read_at(byte_off, &mut b)?;
        if used {
            b[0] |= 1 << (bit % 8);
        } else {
            b[0] &= !(1 << (bit % 8));
        }
        self.write_at(byte_off, &b)
    }

    fn imap_start(&self) -> u64 {
        2 * self.sb.block_size as u64
    }
    fn zmap_start(&self) -> u64 {
        (2 + self.sb.imap_blocks as u64) * self.sb.block_size as u64
    }

    /// Allocate a free inode (imap bit clear, 1..=ninodes) and mark it used.
    fn alloc_inode(&mut self) -> Result<u32, FilesystemError> {
        let bmap = self.read_inode_bitmap()?;
        for ino in 1..=self.sb.ninodes as usize {
            if !bit_set(&bmap, ino) {
                self.set_bitmap_bit(self.imap_start(), ino, true)?;
                return Ok(ino as u32);
            }
        }
        Err(FilesystemError::DiskFull("no free Minix inodes".into()))
    }

    fn free_inode(&mut self, ino: u32) -> Result<(), FilesystemError> {
        self.set_bitmap_bit(self.imap_start(), ino as usize, false)
    }

    /// Allocate a free data zone (zmap bit clear) and mark it used.
    fn alloc_zone(&mut self) -> Result<u32, FilesystemError> {
        let bmap = self.read_zone_bitmap()?;
        let max_bit = (self.sb.zones - self.sb.firstdatazone) as usize;
        for bit in 1..=max_bit {
            if !bit_set(&bmap, bit) {
                self.set_bitmap_bit(self.zmap_start(), bit, true)?;
                self.used_zones += 1;
                return Ok(self.sb.firstdatazone + bit as u32 - 1);
            }
        }
        Err(FilesystemError::DiskFull("no free Minix zones".into()))
    }

    fn free_zone(&mut self, zno: u32) -> Result<(), FilesystemError> {
        if zno < self.sb.firstdatazone || zno >= self.sb.zones {
            return Ok(());
        }
        let bit = (zno - self.sb.firstdatazone + 1) as usize;
        self.set_bitmap_bit(self.zmap_start(), bit, false)?;
        self.used_zones = self.used_zones.saturating_sub(1);
        Ok(())
    }

    fn read_inode_bitmap(&mut self) -> Result<Vec<u8>, FilesystemError> {
        let len = (self.sb.imap_blocks as u64 * self.sb.block_size as u64) as usize;
        let mut buf = vec![0u8; len];
        self.read_at(self.imap_start(), &mut buf)?;
        Ok(buf)
    }

    fn write_ptr(&self, block: &mut [u8], idx: usize, zone: u32) {
        if self.sb.version == MinixVersion::V1 {
            block[idx * 2..idx * 2 + 2].copy_from_slice(&(zone as u16).to_le_bytes());
        } else {
            block[idx * 4..idx * 4 + 4].copy_from_slice(&zone.to_le_bytes());
        }
    }

    /// Stream `len` bytes of `data` into freshly allocated zones and wire them
    /// into `inode` (direct + single/double/triple indirect), setting its size.
    fn write_file_data(
        &mut self,
        inode: &mut MinixInode,
        data: &mut dyn Read,
        len: u64,
    ) -> Result<(), FilesystemError> {
        let zs = self.sb.zone_size() as usize;
        let nzones = (len as usize).div_ceil(zs);
        let mut data_zones = Vec::with_capacity(nzones);
        let mut remaining = len as usize;
        for _ in 0..nzones {
            let take = zs.min(remaining);
            let mut buf = vec![0u8; zs];
            data.read_exact(&mut buf[..take])?;
            let z = self.alloc_zone()?;
            self.write_zone(z, &buf)?;
            data_zones.push(z);
            remaining -= take;
        }
        inode.zones = self.build_zone_tree(&data_zones)?;
        inode.size = len as u32;
        Ok(())
    }

    /// Build an inode's zone array from a flat list of data zones, writing
    /// indirect blocks as needed.
    fn build_zone_tree(&mut self, data_zones: &[u32]) -> Result<[u32; 10], FilesystemError> {
        let mut slots = [0u32; 10];
        let mut idx = 0usize;
        while idx < DIRECT_ZONES && idx < data_zones.len() {
            slots[idx] = data_zones[idx];
            idx += 1;
        }
        let per_inode = self.sb.zones_per_inode();
        for (slot, level) in [(7usize, 1u32), (8, 2), (9, 3)] {
            if idx >= data_zones.len() {
                break;
            }
            if slot >= per_inode {
                return Err(FilesystemError::DiskFull(
                    "file too large for Minix indirection".into(),
                ));
            }
            slots[slot] = self.build_indirect(level, data_zones, &mut idx)?;
        }
        if idx < data_zones.len() {
            return Err(FilesystemError::DiskFull(
                "file too large for Minix indirection".into(),
            ));
        }
        Ok(slots)
    }

    /// Allocate one indirect zone of `level` (1/2/3) and fill it with pointers
    /// consuming `data_zones[*idx..]`; returns the indirect zone number.
    fn build_indirect(
        &mut self,
        level: u32,
        data_zones: &[u32],
        idx: &mut usize,
    ) -> Result<u32, FilesystemError> {
        let zone = self.alloc_zone()?;
        let per = self.sb.ptrs_per_zone() as usize;
        let mut block = vec![0u8; self.sb.zone_size() as usize];
        for i in 0..per {
            if *idx >= data_zones.len() {
                break;
            }
            let child = if level == 1 {
                let z = data_zones[*idx];
                *idx += 1;
                z
            } else {
                self.build_indirect(level - 1, data_zones, idx)?
            };
            self.write_ptr(&mut block, i, child);
        }
        self.write_zone(zone, &block)?;
        Ok(zone)
    }

    /// Free every data zone and indirect block an inode references.
    fn free_inode_zones(&mut self, inode: &MinixInode) -> Result<(), FilesystemError> {
        for i in 0..DIRECT_ZONES {
            if inode.zones[i] != 0 {
                self.free_zone(inode.zones[i])?;
            }
        }
        let per_inode = self.sb.zones_per_inode();
        for (slot, level) in [(7usize, 1u32), (8, 2), (9, 3)] {
            if slot < per_inode && inode.zones[slot] != 0 {
                self.free_indirect(inode.zones[slot], level)?;
            }
        }
        Ok(())
    }

    fn free_indirect(&mut self, zone: u32, level: u32) -> Result<(), FilesystemError> {
        if zone == 0 {
            return Ok(());
        }
        let block = self.read_zone(zone)?;
        let per = self.sb.ptrs_per_zone() as usize;
        for i in 0..per {
            let ptr = self.read_ptr(&block, i);
            if ptr != 0 {
                if level == 1 {
                    self.free_zone(ptr)?;
                } else {
                    self.free_indirect(ptr, level - 1)?;
                }
            }
        }
        self.free_zone(zone)
    }

    fn ino_field(&self) -> usize {
        if self.sb.version == MinixVersion::V3 {
            4
        } else {
            2
        }
    }

    /// Return the child inode of `name` in `dir`, if present.
    fn dir_find(&mut self, dir: &MinixInode, name: &[u8]) -> Result<Option<u32>, FilesystemError> {
        let data = self.read_inode_data(dir, dir.size as usize)?;
        let stride = self.sb.dir_entry_size;
        let ino_field = self.ino_field();
        for chunk in data.chunks(stride) {
            if chunk.len() < stride {
                break;
            }
            let ino = dirent_ino(chunk, ino_field);
            if ino == 0 {
                continue;
            }
            let nb = &chunk[ino_field..stride];
            let end = nb.iter().position(|&b| b == 0).unwrap_or(nb.len());
            if &nb[..end] == name {
                return Ok(Some(ino));
            }
        }
        Ok(None)
    }

    /// Physical zone backing logical zone `lz` of a directory (direct slots
    /// only). Allocates and zeroes a new zone when `alloc` and the slot is
    /// empty; refuses beyond the 7 direct slots (indirect dir growth is out of
    /// scope — realistic directories stay well under 7 zones).
    fn dir_zone(
        &mut self,
        dir: &mut MinixInode,
        lz: usize,
        alloc: bool,
    ) -> Result<u32, FilesystemError> {
        if lz >= DIRECT_ZONES {
            return Err(FilesystemError::Unsupported(
                "Minix directory too large (indirect directory growth unsupported)".into(),
            ));
        }
        if dir.zones[lz] != 0 {
            return Ok(dir.zones[lz]);
        }
        if !alloc {
            return Err(FilesystemError::InvalidData(
                "missing directory zone".into(),
            ));
        }
        let z = self.alloc_zone()?;
        self.write_zone(z, &vec![0u8; self.sb.zone_size() as usize])?;
        dir.zones[lz] = z;
        Ok(z)
    }

    /// Insert `name -> child` into `dir`, reusing a free slot or appending
    /// (growing the directory by a zone at a zone boundary). Mutates `dir`
    /// (size / zone slots); the caller writes it back.
    fn dir_add(
        &mut self,
        dir: &mut MinixInode,
        name: &[u8],
        child: u32,
    ) -> Result<(), FilesystemError> {
        if name.len() > self.sb.name_len {
            return Err(FilesystemError::InvalidData(format!(
                "name too long for Minix (max {} chars)",
                self.sb.name_len
            )));
        }
        let stride = self.sb.dir_entry_size;
        let ino_field = self.ino_field();
        let zs = self.sb.zone_size() as usize;

        // Find a free slot (inode == 0) within the current size, else append.
        let data = self.read_inode_data(dir, dir.size as usize)?;
        let mut target = None;
        let mut off = 0;
        while off + stride <= data.len() {
            if dirent_ino(&data[off..], ino_field) == 0 {
                target = Some(off);
                break;
            }
            off += stride;
        }
        let appended = target.is_none();
        let target_off = target.unwrap_or(dir.size as usize);

        let phys = self.dir_zone(dir, target_off / zs, true)?;
        let mut ent = vec![0u8; stride];
        encode_dirent(&mut ent, ino_field, child, name);
        self.write_at(phys as u64 * zs as u64 + (target_off % zs) as u64, &ent)?;
        if appended {
            dir.size += stride as u32;
        }
        Ok(())
    }

    /// Remove `name` from `dir` by zeroing its dirent's inode field (Minix
    /// leaves the slot as a hole). Returns the removed child inode.
    fn dir_remove(&mut self, dir: &MinixInode, name: &[u8]) -> Result<u32, FilesystemError> {
        let stride = self.sb.dir_entry_size;
        let ino_field = self.ino_field();
        let zs = self.sb.zone_size() as usize;
        let data = self.read_inode_data(dir, dir.size as usize)?;
        let mut off = 0;
        while off + stride <= data.len() {
            let ino = dirent_ino(&data[off..], ino_field);
            if ino != 0 {
                let nb = &data[off + ino_field..off + stride];
                let end = nb.iter().position(|&b| b == 0).unwrap_or(nb.len());
                if &nb[..end] == name {
                    // Directory data lives in direct zones, so the backing zone
                    // is simply dir.zones[logical]. Zero the inode field.
                    let phys = dir.zones[off / zs];
                    let zero = vec![0u8; ino_field];
                    self.write_at(phys as u64 * zs as u64 + (off % zs) as u64, &zero)?;
                    return Ok(ino);
                }
            }
            off += stride;
        }
        Err(FilesystemError::NotFound(
            String::from_utf8_lossy(name).into_owned(),
        ))
    }

    /// Count free data zones (for `free_space`).
    fn free_data_zones(&mut self) -> Result<u32, FilesystemError> {
        let total = self.sb.zones - self.sb.firstdatazone;
        Ok(total.saturating_sub(self.used_zones))
    }
}

impl<R: Read + Write + Seek + Send> EditableFilesystem for MinixFilesystem<R> {
    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn Read,
        data_len: u64,
        options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        if !parent.is_directory() {
            return Err(FilesystemError::NotADirectory(parent.path.clone()));
        }
        self.validate_name(name)?;
        let parent_inum = parent.location as u32;
        let parent_inode = self.read_inode(parent_inum)?;
        if !options.skip_name_checks && self.dir_find(&parent_inode, name.as_bytes())?.is_some() {
            return Err(FilesystemError::AlreadyExists(name.into()));
        }

        let inum = self.alloc_inode()?;
        let mut ino = MinixInode::empty(inum);
        ino.mode = options.mode.map(|m| m as u16).unwrap_or(0o100644);
        ino.nlinks = 1;
        ino.uid = options.uid.unwrap_or(0) as u16;
        ino.gid = options.gid.unwrap_or(0) as u16;
        if options.skip_data_write {
            ino.size = data_len as u32;
        } else {
            self.write_file_data(&mut ino, data, data_len)?;
        }
        self.write_inode(&ino)?;

        // Re-read the parent — dir_add may have grown it — link, write back.
        let mut parent_inode = self.read_inode(parent_inum)?;
        self.dir_add(&mut parent_inode, name.as_bytes(), inum)?;
        self.write_inode(&parent_inode)?;

        Ok(self.entry_from_inode(name, &parent.path, &ino))
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        if !parent.is_directory() {
            return Err(FilesystemError::NotADirectory(parent.path.clone()));
        }
        self.validate_name(name)?;
        let parent_inum = parent.location as u32;
        let parent_inode = self.read_inode(parent_inum)?;
        if self.dir_find(&parent_inode, name.as_bytes())?.is_some() {
            return Err(FilesystemError::AlreadyExists(name.into()));
        }

        let inum = self.alloc_inode()?;
        let zone = self.alloc_zone()?;
        let stride = self.sb.dir_entry_size;
        let ino_field = self.ino_field();
        // Initial dir zone: "." -> self, ".." -> parent.
        let mut zbuf = vec![0u8; self.sb.zone_size() as usize];
        encode_dirent(&mut zbuf[0..stride], ino_field, inum, b".");
        encode_dirent(&mut zbuf[stride..stride * 2], ino_field, parent_inum, b"..");
        self.write_zone(zone, &zbuf)?;

        let mut dir = MinixInode::empty(inum);
        dir.mode = options.mode.map(|m| m as u16).unwrap_or(0o040755);
        dir.nlinks = 2; // "." plus the parent's link
        dir.uid = options.uid.unwrap_or(0) as u16;
        dir.gid = options.gid.unwrap_or(0) as u16;
        dir.size = (stride * 2) as u32;
        dir.zones[0] = zone;
        self.write_inode(&dir)?;

        let mut parent_inode = self.read_inode(parent_inum)?;
        self.dir_add(&mut parent_inode, name.as_bytes(), inum)?;
        // Parent gains a link (the new dir's "..").
        parent_inode.nlinks = parent_inode.nlinks.saturating_add(1);
        self.write_inode(&parent_inode)?;

        Ok(self.entry_from_inode(name, &parent.path, &dir))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        if !parent.is_directory() {
            return Err(FilesystemError::NotADirectory(parent.path.clone()));
        }
        let entry_inum = entry.location as u32;
        if entry_inum <= ROOT_INO {
            return Err(FilesystemError::InvalidData(format!(
                "refusing to delete reserved Minix inode {entry_inum}"
            )));
        }
        let target = self.read_inode(entry_inum)?;
        if target.is_dir() {
            // Must be empty (only "." and "..").
            let data = self.read_inode_data(&target, target.size as usize)?;
            let stride = self.sb.dir_entry_size;
            let ino_field = self.ino_field();
            for chunk in data.chunks(stride) {
                if chunk.len() < stride || dirent_ino(chunk, ino_field) == 0 {
                    continue;
                }
                let nb = &chunk[ino_field..stride];
                let end = nb.iter().position(|&b| b == 0).unwrap_or(nb.len());
                if &nb[..end] != b"." && &nb[..end] != b".." {
                    return Err(FilesystemError::InvalidData(format!(
                        "Minix directory '{}' not empty",
                        entry.path
                    )));
                }
            }
        }

        // Unlink from parent first (a crash then leaves a recoverable orphan
        // inode rather than a dangling dirent).
        let mut parent_inode = self.read_inode(parent.location as u32)?;
        let removed = self.dir_remove(&parent_inode, entry.name.as_bytes())?;
        if removed != entry_inum {
            return Err(FilesystemError::InvalidData(format!(
                "Minix delete: dirent inode {removed} != entry inode {entry_inum}"
            )));
        }
        self.free_inode_zones(&target)?;
        self.free_inode(entry_inum)?;
        self.write_inode(&MinixInode::empty(entry_inum))?;
        if target.is_dir() {
            parent_inode.nlinks = parent_inode.nlinks.saturating_sub(1);
            self.write_inode(&parent_inode)?;
        }
        Ok(())
    }

    fn rename(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
        new_name: &str,
    ) -> Result<(), FilesystemError> {
        if !parent.is_directory() {
            return Err(FilesystemError::NotADirectory(parent.path.clone()));
        }
        if new_name == entry.name {
            return Ok(());
        }
        self.validate_name(new_name)?;
        let parent_inum = parent.location as u32;
        let parent_inode = self.read_inode(parent_inum)?;
        if self.dir_find(&parent_inode, new_name.as_bytes())?.is_some() {
            return Err(FilesystemError::AlreadyExists(new_name.into()));
        }
        // Fixed-size dirents: remove the old entry (leaves a free slot) and add
        // the new name pointing at the same inode. Identity/data untouched.
        let removed = self.dir_remove(&parent_inode, entry.name.as_bytes())?;
        if removed != entry.location as u32 {
            return Err(FilesystemError::InvalidData(format!(
                "Minix rename: dirent inode {removed} != entry inode {}",
                entry.location
            )));
        }
        let mut parent_inode = self.read_inode(parent_inum)?;
        self.dir_add(&mut parent_inode, new_name.as_bytes(), removed)?;
        self.write_inode(&parent_inode)?;
        Ok(())
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.reader.flush()?;
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.free_data_zones()? as u64 * self.sb.zone_size())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::process::Command;

    /// Locate a util-linux tool, honoring PATH then common sbin locations.
    fn minix_tool(name: &str) -> Option<PathBuf> {
        if Command::new(name).arg("-V").output().is_ok() {
            return Some(PathBuf::from(name));
        }
        for dir in ["/usr/sbin", "/sbin"] {
            let p = PathBuf::from(dir).join(name);
            if p.exists() {
                return Some(p);
            }
        }
        None
    }

    /// Build a real Minix image with mkfs.minix at the given version (1/2/3).
    /// Returns None (skip) when the oracle isn't installed.
    fn mkfs_image(version: u8, blocks: u64) -> Option<Vec<u8>> {
        let mkfs = minix_tool("mkfs.minix")?;
        let dir = std::env::temp_dir();
        let img = dir.join(format!("rb_minix_v{version}_{}.img", std::process::id()));
        std::fs::write(&img, vec![0u8; (blocks * 1024) as usize]).ok()?;
        let ok = Command::new(&mkfs)
            .arg(format!("-{version}"))
            .arg(&img)
            .output()
            .ok()?
            .status
            .success();
        let bytes = std::fs::read(&img).ok();
        let _ = std::fs::remove_file(&img);
        if ok {
            bytes
        } else {
            None
        }
    }

    #[test]
    fn superblock_parse_v3_matches_mkfs_geometry() {
        let Some(img) = mkfs_image(3, 4096) else {
            eprintln!("skipping: mkfs.minix not available");
            return;
        };
        let sb = MinixSuperblock::parse(&img[1024..1024 + 1024]).expect("parse v3 sb");
        assert_eq!(sb.version, MinixVersion::V3);
        assert_eq!(sb.magic, MAGIC_V3);
        assert_eq!(sb.name_len, 60);
        assert_eq!(sb.dir_entry_size, 64);
        assert_eq!(sb.inode_size, 64);
        assert_eq!(sb.block_size, 1024);
        assert_eq!(sb.zones, 4096);
        assert!(sb.firstdatazone >= 2);
    }

    #[test]
    fn open_and_root_across_versions() {
        for v in [1u8, 2, 3] {
            let Some(img) = mkfs_image(v, 4096) else {
                eprintln!("skipping v{v}: mkfs.minix not available");
                continue;
            };
            let mut fs = MinixFilesystem::open(Cursor::new(img), 0).expect("open");
            let root = fs.root().expect("root");
            assert!(root.is_directory());
            assert_eq!(root.location, ROOT_INO as u64);
            // A freshly-made volume's root has only "." and ".." — both filtered.
            let listing = fs.list_directory(&root).expect("list root");
            assert!(
                listing.is_empty(),
                "fresh v{v} root should list empty, got {listing:?}"
            );
            // Total size = whole volume; used should be well under it.
            assert_eq!(fs.total_size(), 4096 * 1024);
            assert!(fs.used_size() <= fs.total_size());
        }
    }

    #[test]
    fn detect_magic_recognizes_all_generations() {
        // Synthesize just the magic fields at the right offsets.
        for (off, magic) in [
            (SB_MAGIC_OFF_V12, MAGIC_V1_14),
            (SB_MAGIC_OFF_V12, MAGIC_V1_30),
            (SB_MAGIC_OFF_V12, MAGIC_V2_14),
            (SB_MAGIC_OFF_V12, MAGIC_V2_30),
            (SB_MAGIC_OFF_V3, MAGIC_V3),
        ] {
            let mut buf = [0u8; 64];
            buf[off..off + 2].copy_from_slice(&magic.to_le_bytes());
            assert_eq!(detect_magic(&buf), Some("minix"), "magic 0x{magic:04X}");
        }
        assert_eq!(detect_magic(&[0u8; 64]), None);
    }

    #[test]
    fn parse_rejects_bad_geometry() {
        let mut buf = [0u8; 64];
        // V3 magic present but zones=0 → reject.
        buf[SB_MAGIC_OFF_V3..SB_MAGIC_OFF_V3 + 2].copy_from_slice(&MAGIC_V3.to_le_bytes());
        buf[28..30].copy_from_slice(&1024u16.to_le_bytes());
        assert!(MinixSuperblock::parse(&buf).is_err());
    }

    // ---- Synthetic V3 image: exercises the traversal paths the empty mkfs
    // oracle can't (file content across the direct→single-indirect boundary, a
    // subdirectory, a symlink). Byte offsets are the ones verified against real
    // mkfs.minix; we lay the image by hand and don't maintain fsck-perfect
    // bitmaps / link counts (the reader never consults them). ----

    const BS: usize = 1024;
    const FDZ: u32 = 8; // firstdatazone (inode table blocks 4..7, data from 8)
    const SYN_ZONES: u32 = 24;
    const S_IFDIR: u16 = 0x4000;
    const S_IFREG: u16 = 0x8000;
    const S_IFLNK: u16 = 0xA000;

    fn inode_table_off() -> usize {
        // 2 + imap_blocks(1) + zmap_blocks(1) = block 4.
        4 * BS
    }

    fn put_v3_inode(img: &mut [u8], ino: u32, mode: u16, nlinks: u16, size: u32, zones: &[u32]) {
        let off = inode_table_off() + (ino as usize - 1) * 64;
        img[off..off + 2].copy_from_slice(&mode.to_le_bytes());
        img[off + 2..off + 4].copy_from_slice(&nlinks.to_le_bytes());
        img[off + 8..off + 12].copy_from_slice(&size.to_le_bytes());
        for (i, z) in zones.iter().enumerate() {
            let zo = off + 24 + i * 4;
            img[zo..zo + 4].copy_from_slice(&z.to_le_bytes());
        }
    }

    fn put_v3_dirent(img: &mut [u8], zone: u32, slot: usize, ino: u32, name: &str) {
        let off = zone as usize * BS + slot * 64;
        img[off..off + 4].copy_from_slice(&ino.to_le_bytes());
        let nb = name.as_bytes();
        img[off + 4..off + 4 + nb.len()].copy_from_slice(nb);
    }

    fn fill_zone(img: &mut [u8], zone: u32, byte: u8) {
        let off = zone as usize * BS;
        img[off..off + BS].fill(byte);
    }

    /// Layout:
    ///   inode 1 root dir       -> zone 8   (., .., big.bin, sub, link)
    ///   inode 2 big.bin (7268) -> direct zones 9..=15, indirect zone 16 -> zone 17
    ///   inode 3 sub dir        -> zone 18  (., .., deep.txt)
    ///   inode 4 deep.txt       -> zone 19
    ///   inode 5 link (symlink) -> zone 20  ("big.bin")
    fn build_synthetic_v3() -> Vec<u8> {
        let mut img = vec![0u8; SYN_ZONES as usize * BS];
        // superblock (block 1)
        let sb = &mut img[BS..BS + 64];
        sb[0..4].copy_from_slice(&64u32.to_le_bytes()); // ninodes
        sb[6..8].copy_from_slice(&1u16.to_le_bytes()); // imap_blocks
        sb[8..10].copy_from_slice(&1u16.to_le_bytes()); // zmap_blocks
        sb[10..12].copy_from_slice(&(FDZ as u16).to_le_bytes()); // firstdatazone
        sb[16..20].copy_from_slice(&0x7fff_ffffu32.to_le_bytes()); // max_size
        sb[20..24].copy_from_slice(&SYN_ZONES.to_le_bytes()); // zones
        sb[24..26].copy_from_slice(&MAGIC_V3.to_le_bytes()); // magic
        sb[28..30].copy_from_slice(&(BS as u16).to_le_bytes()); // block_size

        // inode bitmap (block 2): inodes 1..=5 used + bit 0 sentinel, so the
        // edit allocator hands out inode 6 next.
        let imap = 2 * BS;
        for ino in 0..=5usize {
            img[imap + ino / 8] |= 1 << (ino % 8);
        }
        // zone bitmap (block 3): mark data zones 8..=20 used so used_size is
        // sane. zone z -> bit (z - firstdatazone + 1); plus bit 0 sentinel.
        let zmap = 3 * BS;
        for z in FDZ..=20 {
            let bit = (z - FDZ + 1) as usize;
            img[zmap + bit / 8] |= 1 << (bit % 8);
        }
        img[zmap] |= 1; // sentinel

        // big.bin content: distinct byte per direct zone + the indirect zone.
        for (i, z) in (9u32..=15).enumerate() {
            fill_zone(&mut img, z, 0xC0 + i as u8);
        }
        fill_zone(&mut img, 17, 0xD0); // logical zone 7 (via indirect)
                                       // indirect block (zone 16) points at zone 17 in slot 0.
        img[16 * BS..16 * BS + 4].copy_from_slice(&17u32.to_le_bytes());

        // deep.txt content and symlink target.
        let deep = b"deep content";
        img[19 * BS..19 * BS + deep.len()].copy_from_slice(deep);
        let target = b"big.bin";
        img[20 * BS..20 * BS + target.len()].copy_from_slice(target);

        // inodes
        put_v3_inode(&mut img, 1, S_IFDIR | 0o755, 3, 5 * 64, &[8]);
        put_v3_inode(
            &mut img,
            2,
            S_IFREG | 0o644,
            1,
            7 * BS as u32 + 100,
            &[9, 10, 11, 12, 13, 14, 15, 16, 0, 0],
        );
        put_v3_inode(&mut img, 3, S_IFDIR | 0o755, 2, 3 * 64, &[18]);
        put_v3_inode(&mut img, 4, S_IFREG | 0o644, 1, deep.len() as u32, &[19]);
        put_v3_inode(&mut img, 5, S_IFLNK | 0o777, 1, target.len() as u32, &[20]);

        // root dir (zone 8)
        put_v3_dirent(&mut img, 8, 0, 1, ".");
        put_v3_dirent(&mut img, 8, 1, 1, "..");
        put_v3_dirent(&mut img, 8, 2, 2, "big.bin");
        put_v3_dirent(&mut img, 8, 3, 3, "sub");
        put_v3_dirent(&mut img, 8, 4, 5, "link");
        // sub dir (zone 18)
        put_v3_dirent(&mut img, 18, 0, 3, ".");
        put_v3_dirent(&mut img, 18, 1, 1, "..");
        put_v3_dirent(&mut img, 18, 2, 4, "deep.txt");

        img
    }

    fn open_syn() -> MinixFilesystem<Cursor<Vec<u8>>> {
        MinixFilesystem::open(Cursor::new(build_synthetic_v3()), 0).expect("open synthetic")
    }

    #[test]
    fn synthetic_root_lists_all_entries() {
        let mut fs = open_syn();
        let root = fs.root().expect("root");
        let names: Vec<String> = fs
            .list_directory(&root)
            .expect("list")
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert_eq!(names, vec!["big.bin", "sub", "link"]);
    }

    #[test]
    fn synthetic_reads_file_across_indirect_boundary() {
        let mut fs = open_syn();
        let root = fs.root().unwrap();
        let big = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "big.bin")
            .expect("big.bin");
        assert_eq!(big.size, 7 * BS as u64 + 100);
        let data = fs.read_file(&big, usize::MAX).expect("read big.bin");
        assert_eq!(data.len(), 7 * BS + 100);
        // 7 direct zones carry 0xC0..0xC6, one per zone.
        for (i, chunk) in data.chunks(BS).take(7).enumerate() {
            assert!(
                chunk.iter().all(|&b| b == 0xC0 + i as u8),
                "direct zone {i} mismatched"
            );
        }
        // The 100 tail bytes come from the indirect-mapped zone (0xD0).
        assert!(data[7 * BS..].iter().all(|&b| b == 0xD0));
    }

    #[test]
    fn synthetic_reads_file_honours_max_bytes() {
        let mut fs = open_syn();
        let root = fs.root().unwrap();
        let big = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "big.bin")
            .unwrap();
        // 2000 bytes spans the first two direct zones only.
        let data = fs.read_file(&big, 2000).unwrap();
        assert_eq!(data.len(), 2000);
        assert!(data[..BS].iter().all(|&b| b == 0xC0));
        assert!(data[BS..].iter().all(|&b| b == 0xC1));
    }

    #[test]
    fn synthetic_descends_subdirectory() {
        let mut fs = open_syn();
        let root = fs.root().unwrap();
        let sub = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "sub")
            .expect("sub");
        assert!(sub.is_directory());
        let kids = fs.list_directory(&sub).expect("list sub");
        assert_eq!(kids.len(), 1);
        assert_eq!(kids[0].name, "deep.txt");
        assert_eq!(kids[0].path, "/sub/deep.txt");
        let data = fs.read_file(&kids[0], usize::MAX).unwrap();
        assert_eq!(&data, b"deep content");
    }

    #[test]
    fn synthetic_resolves_symlink_target() {
        let mut fs = open_syn();
        let root = fs.root().unwrap();
        let link = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "link")
            .expect("link");
        assert_eq!(link.entry_type, EntryType::Symlink);
        assert_eq!(link.symlink_target.as_deref(), Some("big.bin"));
    }

    #[test]
    fn synthetic_used_and_total_size() {
        let fs = open_syn();
        assert_eq!(fs.total_size(), SYN_ZONES as u64 * BS as u64);
        // Metadata (8 zones) + 13 marked data zones (8..=20).
        assert_eq!(fs.used_size(), (FDZ as u64 + 13) * BS as u64);
    }

    // ---- Edit ----

    #[test]
    fn synthetic_edit_create_file_reads_back() {
        // Deterministic (no oracle): create a file on the hand-laid image and
        // read it back through the reader.
        let mut fs = MinixFilesystem::open(Cursor::new(build_synthetic_v3()), 0).expect("open");
        let root = fs.root().unwrap();
        let content = vec![0x5Au8; 1500]; // spans 2 zones
        fs.create_file(
            &root,
            "new.txt",
            &mut &content[..],
            content.len() as u64,
            &CreateFileOptions::default(),
        )
        .expect("create_file");

        let entries = fs.list_directory(&root).unwrap();
        let e = entries
            .iter()
            .find(|e| e.name == "new.txt")
            .expect("new.txt");
        assert_eq!(e.size, 1500);
        let data = fs.read_file(e, usize::MAX).unwrap();
        assert_eq!(data.len(), 1500);
        assert!(data.iter().all(|&b| b == 0x5A));
        // Pre-existing entries survive the append.
        assert!(entries.iter().any(|e| e.name == "big.bin"));
    }

    /// mkfs.minix a real image, run a full edit sequence through our writer,
    /// then require `fsck.minix -f -n` to call it clean and read the tree back.
    fn oracle_edit_roundtrip(version: u8) {
        let (Some(mkfs), Some(fsck)) = (minix_tool("mkfs.minix"), minix_tool("fsck.minix")) else {
            eprintln!("skipping oracle_edit_roundtrip v{version}: util-linux not available");
            return;
        };
        let img = std::env::temp_dir().join(format!(
            "rb_minix_edit_v{version}_{}.img",
            std::process::id()
        ));
        std::fs::write(&img, vec![0u8; 2 * 1024 * 1024]).expect("write image");
        assert!(
            Command::new(&mkfs)
                .arg(format!("-{version}"))
                .arg(&img)
                .output()
                .expect("mkfs")
                .status
                .success(),
            "mkfs.minix -{version} failed"
        );

        let big = vec![0xABu8; 3000]; // ~3 zones (crosses zone boundaries)
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&img)
                .expect("open rw");
            let mut fs = MinixFilesystem::open(file, 0).expect("open editable");
            let root = fs.root().unwrap();
            let hello = fs
                .create_file(
                    &root,
                    "hello.txt",
                    &mut &big[..],
                    big.len() as u64,
                    &CreateFileOptions::default(),
                )
                .expect("create hello.txt");
            let sub = fs
                .create_directory(&root, "sub", &CreateDirectoryOptions::default())
                .expect("mkdir sub");
            let inner = b"inner data";
            fs.create_file(
                &sub,
                "inner.txt",
                &mut &inner[..],
                inner.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create inner.txt");
            // Create then delete a file (exercises the free path + orphan-free).
            let tmp = fs
                .create_file(
                    &root,
                    "tmp.bin",
                    &mut &b"x"[..],
                    1,
                    &CreateFileOptions::default(),
                )
                .expect("create tmp.bin");
            fs.delete_entry(&root, &tmp).expect("delete tmp.bin");
            fs.rename(&root, &hello, "renamed.txt").expect("rename");
            fs.sync_metadata().expect("sync");
        }

        // fsck.minix has no dry-run flag; `-f` forces a check and, with stdin
        // closed, any repair prompt reads EOF ("no") so an inconsistency exits
        // nonzero rather than being silently fixed. Exit 0 = clean.
        let out = Command::new(&fsck)
            .arg("-f")
            .arg(&img)
            .stdin(std::process::Stdio::null())
            .output()
            .expect("run fsck.minix");
        if !out.status.success() {
            let log = format!(
                "{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
            let _ = std::fs::remove_file(&img);
            panic!("fsck.minix flagged the edited v{version} image:\n{log}");
        }

        // Read the tree back through a fresh open.
        {
            let file = std::fs::File::open(&img).expect("reopen ro");
            let mut fs = MinixFilesystem::open(file, 0).expect("reopen");
            let root = fs.root().unwrap();
            let entries = fs.list_directory(&root).unwrap();
            let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
            assert!(names.contains(&"renamed.txt"), "v{version}: {names:?}");
            assert!(names.contains(&"sub"), "v{version}: {names:?}");
            assert!(!names.contains(&"hello.txt"), "old name lingers: {names:?}");
            assert!(!names.contains(&"tmp.bin"), "deleted lingers: {names:?}");

            let renamed = entries.iter().find(|e| e.name == "renamed.txt").unwrap();
            let data = fs.read_file(renamed, usize::MAX).unwrap();
            assert_eq!(data.len(), 3000, "v{version} content length");
            assert!(data.iter().all(|&b| b == 0xAB), "v{version} content bytes");

            let sub = entries.iter().find(|e| e.name == "sub").unwrap();
            let kids = fs.list_directory(sub).unwrap();
            assert_eq!(kids.len(), 1);
            assert_eq!(kids[0].name, "inner.txt");
            assert_eq!(fs.read_file(&kids[0], usize::MAX).unwrap(), b"inner data");
        }
        let _ = std::fs::remove_file(&img);
    }

    #[test]
    fn oracle_edit_roundtrip_v1() {
        oracle_edit_roundtrip(1);
    }
    #[test]
    fn oracle_edit_roundtrip_v2() {
        oracle_edit_roundtrip(2);
    }
    #[test]
    fn oracle_edit_roundtrip_v3() {
        oracle_edit_roundtrip(3);
    }

    /// Write a file large enough to need single- AND double-indirect zones,
    /// exercising the indirect-block write path (u16 pointers on V1, u32 on
    /// V2/V3). Verified fsck.minix-clean and byte-exact on read-back, with a
    /// per-zone content signature so out-of-order assembly would fail.
    fn oracle_large_file_roundtrip(version: u8) {
        let (Some(mkfs), Some(fsck)) = (minix_tool("mkfs.minix"), minix_tool("fsck.minix")) else {
            eprintln!("skipping oracle_large_file_roundtrip v{version}: util-linux not available");
            return;
        };
        let img = std::env::temp_dir().join(format!(
            "rb_minix_big_v{version}_{}.img",
            std::process::id()
        ));
        std::fs::write(&img, vec![0u8; 4 * 1024 * 1024]).expect("write image");
        assert!(Command::new(&mkfs)
            .arg(format!("-{version}"))
            .arg(&img)
            .output()
            .expect("mkfs")
            .status
            .success());

        // 600 KiB: 7 direct + single-indirect + into double-indirect on every
        // generation (V1 512 ptrs/zone, V2/V3 256). Each 1 KiB zone is filled
        // with its own index so a misordered read is caught.
        let nzones = 600usize;
        let mut content = vec![0u8; nzones * 1024];
        for (z, chunk) in content.chunks_mut(1024).enumerate() {
            chunk.fill((z & 0xFF) as u8);
        }
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&img)
                .expect("open rw");
            let mut fs = MinixFilesystem::open(file, 0).expect("open editable");
            let root = fs.root().unwrap();
            fs.create_file(
                &root,
                "big.dat",
                &mut &content[..],
                content.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create big.dat");
            fs.sync_metadata().unwrap();
        }

        let out = Command::new(&fsck)
            .arg("-f")
            .arg(&img)
            .stdin(std::process::Stdio::null())
            .output()
            .expect("fsck");
        if !out.status.success() {
            let log = String::from_utf8_lossy(&out.stdout).into_owned()
                + &String::from_utf8_lossy(&out.stderr);
            let _ = std::fs::remove_file(&img);
            panic!("fsck.minix flagged the large-file v{version} image:\n{log}");
        }

        {
            let file = std::fs::File::open(&img).expect("reopen");
            let mut fs = MinixFilesystem::open(file, 0).expect("reopen");
            let root = fs.root().unwrap();
            let big = fs
                .list_directory(&root)
                .unwrap()
                .into_iter()
                .find(|e| e.name == "big.dat")
                .expect("big.dat");
            let data = fs.read_file(&big, usize::MAX).unwrap();
            assert_eq!(data, content, "v{version} large-file content mismatch");
        }
        let _ = std::fs::remove_file(&img);
    }

    #[test]
    fn oracle_large_file_roundtrip_v1() {
        oracle_large_file_roundtrip(1);
    }
    #[test]
    fn oracle_large_file_roundtrip_v2() {
        oracle_large_file_roundtrip(2);
    }
    #[test]
    fn oracle_large_file_roundtrip_v3() {
        oracle_large_file_roundtrip(3);
    }
}
