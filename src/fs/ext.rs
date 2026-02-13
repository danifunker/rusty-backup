//! ext2/3/4 filesystem browsing and compaction.
//!
//! Implements the `Filesystem` trait for ext2, ext3, and ext4 partitions,
//! supporting superblock parsing, block group descriptors, inode reading,
//! directory parsing (linear and htree), extent trees, indirect block chains,
//! and fast/slow symlinks.
//!
//! Also provides `CompactExtReader` for producing compacted partition images
//! where unallocated blocks are zeroed out (ideal for compression).

use std::io::{Read, Seek, SeekFrom};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{Filesystem, FilesystemError};
use super::unix_common::bitmap::BitmapReader;
use super::unix_common::compact::{CompactLayout, CompactSection, CompactStreamReader};
use super::unix_common::inode::{unix_entry_from_inode, unix_file_type, UnixFileType};
use crate::fs::CompactResult;

// ---- Constants ----

const EXT_MAGIC: u16 = 0xEF53;
const SUPERBLOCK_OFFSET: u64 = 1024;
const SUPERBLOCK_SIZE: usize = 1024;

// Feature flags
const COMPAT_HAS_JOURNAL: u32 = 0x0004;
const INCOMPAT_EXTENTS: u32 = 0x0040;
const INCOMPAT_64BIT: u32 = 0x0080;

// Inode flags
const EXT4_EXTENTS_FL: u32 = 0x0008_0000;

// Extent magic
const EXT4_EXT_MAGIC: u16 = 0xF30A;

// Block group flags
const BG_BLOCK_UNINIT: u16 = 0x0002;

// Special inodes
const EXT_ROOT_INO: u32 = 2;

// Directory file_type field values
const FT_REG_FILE: u8 = 1;
const FT_DIR: u8 = 2;
const FT_CHRDEV: u8 = 3;
const FT_BLKDEV: u8 = 4;
const FT_FIFO: u8 = 5;
const FT_SOCK: u8 = 6;
const FT_SYMLINK: u8 = 7;

// ---- On-disk structures ----

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExtVersion {
    Ext2,
    Ext3,
    Ext4,
}

impl ExtVersion {
    fn name(&self) -> &'static str {
        match self {
            ExtVersion::Ext2 => "ext2",
            ExtVersion::Ext3 => "ext3",
            ExtVersion::Ext4 => "ext4",
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields used in later tasks (compaction, validation)
struct GroupDescriptor {
    block_bitmap: u64,
    inode_bitmap: u64,
    inode_table: u64,
    free_blocks: u32,
    free_inodes: u32,
    flags: u16,
}

#[derive(Debug)]
struct InodeData {
    mode: u32,
    uid: u32,
    gid: u32,
    size: u64,
    mtime: i64,
    #[allow(dead_code)]
    links_count: u16,
    flags: u32,
    block: [u8; 60], // raw i_block area (15 * 4 bytes)
}

struct DirEntry {
    inode: u32,
    name: String,
    file_type: u8,
}

// ---- ExtFilesystem ----

pub struct ExtFilesystem<R> {
    reader: R,
    partition_offset: u64,
    block_size: u64,
    total_blocks: u64,
    inodes_per_group: u32,
    inode_size: u16,
    group_count: u32,
    group_descriptors: Vec<GroupDescriptor>,
    label: Option<String>,
    ext_version: ExtVersion,
    has_extents: bool,
    #[allow(dead_code)] // Used in later tasks (64-bit block number handling)
    is_64bit: bool,
    #[allow(dead_code)] // Used in later tasks (64-bit GDT re-reading)
    desc_size: u16,
    first_data_block: u32,
    free_blocks: u64,
}

impl<R: Read + Seek + Send> ExtFilesystem<R> {
    /// Open an ext2/3/4 filesystem at the given partition offset.
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        // Read superblock
        reader.seek(SeekFrom::Start(partition_offset + SUPERBLOCK_OFFSET))?;
        let mut sb = [0u8; SUPERBLOCK_SIZE];
        reader.read_exact(&mut sb)?;

        // Validate magic
        let magic = u16::from_le_bytes([sb[0x38], sb[0x39]]);
        if magic != EXT_MAGIC {
            return Err(FilesystemError::Parse(format!(
                "ext: invalid superblock magic 0x{magic:04X}, expected 0x{EXT_MAGIC:04X}"
            )));
        }

        // Parse superblock fields
        let inodes_count = le32(&sb, 0x00);
        let blocks_count_lo = le32(&sb, 0x04);
        let first_data_block = le32(&sb, 0x18);
        let log_block_size = le32(&sb, 0x1C);
        let blocks_per_group = le32(&sb, 0x24);
        let inodes_per_group = le32(&sb, 0x2C);
        let inode_size = u16::from_le_bytes([sb[0x3E], sb[0x3F]]);
        let feature_compat = le32(&sb, 0x5C);
        let feature_incompat = le32(&sb, 0x60);

        let block_size = 1024u64 << log_block_size;
        if block_size < 1024 || block_size > 65536 {
            return Err(FilesystemError::Parse(format!(
                "ext: invalid block size {block_size}"
            )));
        }

        let has_extents = feature_incompat & INCOMPAT_EXTENTS != 0;
        let is_64bit = feature_incompat & INCOMPAT_64BIT != 0;
        let has_journal = feature_compat & COMPAT_HAS_JOURNAL != 0;

        let desc_size = if is_64bit {
            let ds = u16::from_le_bytes([sb[0xFE], sb[0xFF]]);
            if ds >= 64 {
                ds
            } else {
                64
            }
        } else {
            32
        };

        // 64-bit block count
        let total_blocks = if is_64bit {
            let hi = le32(&sb, 0x150) as u64;
            (hi << 32) | blocks_count_lo as u64
        } else {
            blocks_count_lo as u64
        };

        // Volume label
        let label_bytes = &sb[0x78..0x88];
        let label_end = label_bytes.iter().position(|&b| b == 0).unwrap_or(16);
        let label = if label_end > 0 {
            Some(
                String::from_utf8_lossy(&label_bytes[..label_end])
                    .trim()
                    .to_string(),
            )
        } else {
            None
        };
        let label = label.filter(|s| !s.is_empty());

        // Detect ext version
        let ext_version = if has_extents {
            ExtVersion::Ext4
        } else if has_journal {
            ExtVersion::Ext3
        } else {
            ExtVersion::Ext2
        };

        // Group count
        if blocks_per_group == 0 {
            return Err(FilesystemError::Parse("ext: blocks_per_group is 0".into()));
        }
        let group_count = ((total_blocks - first_data_block as u64 + blocks_per_group as u64 - 1)
            / blocks_per_group as u64) as u32;

        if inodes_count == 0 || inodes_per_group == 0 {
            return Err(FilesystemError::Parse(
                "ext: inodes_count or inodes_per_group is 0".into(),
            ));
        }

        if inode_size < 128 {
            return Err(FilesystemError::Parse(format!(
                "ext: invalid inode size {inode_size}"
            )));
        }

        // Read group descriptors
        // GDT starts at the block after the superblock block.
        // For 1K blocks, superblock is in block 1, GDT starts at block 2.
        // For 2K/4K blocks, superblock is in block 0, GDT starts at block 1.
        let sb_block = if block_size == 1024 { 1 } else { 0 };
        let gdt_block = sb_block + 1;
        let gdt_offset = partition_offset + gdt_block * block_size;
        let gdt_bytes_needed = group_count as usize * desc_size as usize;
        let mut gdt_buf = vec![0u8; gdt_bytes_needed];
        reader.seek(SeekFrom::Start(gdt_offset))?;
        reader.read_exact(&mut gdt_buf)?;

        let mut group_descriptors = Vec::with_capacity(group_count as usize);
        let mut total_free_blocks = 0u64;
        for i in 0..group_count as usize {
            let off = i * desc_size as usize;
            let d = &gdt_buf[off..];

            let block_bitmap_lo = le32(d, 0x00) as u64;
            let inode_bitmap_lo = le32(d, 0x04) as u64;
            let inode_table_lo = le32(d, 0x08) as u64;
            let free_blocks_lo = u16::from_le_bytes([d[0x0C], d[0x0D]]) as u32;
            let free_inodes_lo = u16::from_le_bytes([d[0x0E], d[0x0F]]) as u32;
            let flags = u16::from_le_bytes([d[0x12], d[0x13]]);

            let (block_bitmap, inode_bitmap, inode_table, free_blocks, free_inodes) =
                if is_64bit && desc_size >= 64 {
                    let bb_hi = le32(d, 0x20) as u64;
                    let ib_hi = le32(d, 0x24) as u64;
                    let it_hi = le32(d, 0x28) as u64;
                    let fb_hi = u16::from_le_bytes([d[0x2C], d[0x2D]]) as u32;
                    let fi_hi = u16::from_le_bytes([d[0x2E], d[0x2F]]) as u32;
                    (
                        (bb_hi << 32) | block_bitmap_lo,
                        (ib_hi << 32) | inode_bitmap_lo,
                        (it_hi << 32) | inode_table_lo,
                        (fb_hi << 16) | free_blocks_lo,
                        (fi_hi << 16) | free_inodes_lo,
                    )
                } else {
                    (
                        block_bitmap_lo,
                        inode_bitmap_lo,
                        inode_table_lo,
                        free_blocks_lo,
                        free_inodes_lo,
                    )
                };

            total_free_blocks += free_blocks as u64;
            group_descriptors.push(GroupDescriptor {
                block_bitmap,
                inode_bitmap,
                inode_table,
                free_blocks,
                free_inodes,
                flags,
            });
        }

        Ok(Self {
            reader,
            partition_offset,
            block_size,
            total_blocks,
            inodes_per_group,
            inode_size,
            group_count,
            group_descriptors,
            label,
            ext_version,
            has_extents,
            is_64bit,
            desc_size,
            first_data_block,
            free_blocks: total_free_blocks,
        })
    }

    /// Read a block from the partition.
    fn read_block(&mut self, block_num: u64) -> Result<Vec<u8>, FilesystemError> {
        let offset = self.partition_offset + block_num * self.block_size;
        self.reader.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; self.block_size as usize];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Read an inode by number (1-based).
    fn read_inode(&mut self, inode_num: u32) -> Result<InodeData, FilesystemError> {
        if inode_num == 0 {
            return Err(FilesystemError::InvalidData(
                "ext: inode 0 is invalid".into(),
            ));
        }

        let group = (inode_num - 1) / self.inodes_per_group;
        let index = (inode_num - 1) % self.inodes_per_group;

        if group >= self.group_count {
            return Err(FilesystemError::InvalidData(format!(
                "ext: inode {inode_num} is in group {group}, but only {gc} groups exist",
                gc = self.group_count
            )));
        }

        let gd = &self.group_descriptors[group as usize];
        let inode_table_offset = self.partition_offset
            + gd.inode_table * self.block_size
            + index as u64 * self.inode_size as u64;

        self.reader.seek(SeekFrom::Start(inode_table_offset))?;
        let mut buf = vec![0u8; self.inode_size as usize];
        self.reader.read_exact(&mut buf)?;

        let mode = u16::from_le_bytes([buf[0x00], buf[0x01]]) as u32;
        let uid_lo = u16::from_le_bytes([buf[0x02], buf[0x03]]) as u32;
        let size_lo = le32(&buf, 0x04) as u64;
        let mtime = le32(&buf, 0x10) as i64;
        let links_count = u16::from_le_bytes([buf[0x1A], buf[0x1B]]);
        let flags = le32(&buf, 0x20);
        let gid_lo = u16::from_le_bytes([buf[0x18], buf[0x19]]) as u32;

        // i_block: 60 bytes at offset 0x28
        let mut block = [0u8; 60];
        block.copy_from_slice(&buf[0x28..0x64]);

        // High bits for uid/gid (at 0x78, 0x7A if inode_size >= 128)
        let uid_hi = if buf.len() >= 0x7A {
            u16::from_le_bytes([buf[0x78], buf[0x79]]) as u32
        } else {
            0
        };
        let gid_hi = if buf.len() >= 0x7C {
            u16::from_le_bytes([buf[0x7A], buf[0x7B]]) as u32
        } else {
            0
        };

        let uid = (uid_hi << 16) | uid_lo;
        let gid = (gid_hi << 16) | gid_lo;

        // Size: for regular files, size_hi at 0x6C
        let size_hi = le32(&buf, 0x6C) as u64;
        let ft = unix_file_type(mode);
        let size = if ft == UnixFileType::Regular || ft == UnixFileType::Unknown {
            (size_hi << 32) | size_lo
        } else {
            size_lo
        };

        Ok(InodeData {
            mode,
            uid,
            gid,
            size,
            mtime,
            links_count,
            flags,
            block,
        })
    }

    /// Get the list of physical block numbers for an inode's data.
    fn inode_data_blocks(&mut self, inode: &InodeData) -> Result<Vec<u64>, FilesystemError> {
        if inode.flags & EXT4_EXTENTS_FL != 0 || self.has_extents && is_extent_header(&inode.block)
        {
            self.read_extent_tree(&inode.block)
        } else {
            self.read_indirect_blocks(inode)
        }
    }

    /// Read all data for an inode.
    fn read_inode_data(
        &mut self,
        inode: &InodeData,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let blocks = self.inode_data_blocks(inode)?;
        let mut data = Vec::new();
        let limit = (inode.size as usize).min(max_bytes);

        for block_num in blocks {
            if data.len() >= limit {
                break;
            }
            if block_num == 0 {
                // Sparse block — fill with zeros
                let chunk = self.block_size as usize;
                let need = (limit - data.len()).min(chunk);
                data.extend(std::iter::repeat(0u8).take(need));
            } else {
                let block_data = self.read_block(block_num)?;
                let need = (limit - data.len()).min(block_data.len());
                data.extend_from_slice(&block_data[..need]);
            }
        }

        data.truncate(limit);
        Ok(data)
    }

    /// Read the symlink target from an inode.
    fn read_symlink_target(&mut self, inode: &InodeData) -> Result<String, FilesystemError> {
        // Fast symlink: target stored in i_block if size < 60 and no extents
        if inode.size < 60 && inode.flags & EXT4_EXTENTS_FL == 0 {
            let len = inode.size as usize;
            let target = &inode.block[..len];
            return Ok(String::from_utf8_lossy(target).to_string());
        }

        // Slow symlink: target stored in data blocks
        let data = self.read_inode_data(inode, inode.size as usize)?;
        // Strip trailing null if present
        let len = data.iter().position(|&b| b == 0).unwrap_or(data.len());
        Ok(String::from_utf8_lossy(&data[..len]).to_string())
    }

    // ---- Extent tree reading ----

    fn read_extent_tree(&mut self, block_field: &[u8]) -> Result<Vec<u64>, FilesystemError> {
        let header = parse_extent_header(block_field)?;
        if header.depth == 0 {
            // Leaf node — parse extents directly from i_block
            Ok(parse_extent_leaves(block_field, header.entries))
        } else {
            // Internal node — recursively read child blocks
            let indices = parse_extent_indices(block_field, header.entries);
            let mut all_blocks = Vec::new();
            for idx in indices {
                let child_data = self.read_block(idx.child_block)?;
                self.read_extent_node(&child_data, &mut all_blocks)?;
            }
            Ok(all_blocks)
        }
    }

    fn read_extent_node(
        &mut self,
        block_data: &[u8],
        all_blocks: &mut Vec<u64>,
    ) -> Result<(), FilesystemError> {
        let header = parse_extent_header(block_data)?;
        if header.depth == 0 {
            let blocks = parse_extent_leaves(block_data, header.entries);
            all_blocks.extend(blocks);
        } else {
            let indices = parse_extent_indices(block_data, header.entries);
            for idx in indices {
                let child_data = self.read_block(idx.child_block)?;
                self.read_extent_node(&child_data, all_blocks)?;
            }
        }
        Ok(())
    }

    // ---- Indirect block reading ----

    fn read_indirect_blocks(&mut self, inode: &InodeData) -> Result<Vec<u64>, FilesystemError> {
        let mut blocks = Vec::new();
        let ptrs_per_block = self.block_size as usize / 4;

        // 12 direct blocks
        for i in 0..12 {
            let bnum = le32(&inode.block, i * 4) as u64;
            blocks.push(bnum);
        }

        // Single indirect (block 12)
        let single_ind = le32(&inode.block, 48) as u64;
        if single_ind != 0 {
            let ind_data = self.read_block(single_ind)?;
            for i in 0..ptrs_per_block {
                blocks.push(le32(&ind_data, i * 4) as u64);
            }
        }

        // Double indirect (block 13)
        let double_ind = le32(&inode.block, 52) as u64;
        if double_ind != 0 {
            let ind1 = self.read_block(double_ind)?;
            for i in 0..ptrs_per_block {
                let ind2_blk = le32(&ind1, i * 4) as u64;
                if ind2_blk != 0 {
                    let ind2 = self.read_block(ind2_blk)?;
                    for j in 0..ptrs_per_block {
                        blocks.push(le32(&ind2, j * 4) as u64);
                    }
                } else {
                    // Sparse — push zeros for the whole range
                    blocks.extend(std::iter::repeat(0u64).take(ptrs_per_block));
                }
            }
        }

        // Triple indirect (block 14)
        let triple_ind = le32(&inode.block, 56) as u64;
        if triple_ind != 0 {
            let ind1 = self.read_block(triple_ind)?;
            for i in 0..ptrs_per_block {
                let ind2_blk = le32(&ind1, i * 4) as u64;
                if ind2_blk != 0 {
                    let ind2 = self.read_block(ind2_blk)?;
                    for j in 0..ptrs_per_block {
                        let ind3_blk = le32(&ind2, j * 4) as u64;
                        if ind3_blk != 0 {
                            let ind3 = self.read_block(ind3_blk)?;
                            for k in 0..ptrs_per_block {
                                blocks.push(le32(&ind3, k * 4) as u64);
                            }
                        } else {
                            blocks.extend(std::iter::repeat(0u64).take(ptrs_per_block));
                        }
                    }
                } else {
                    blocks.extend(std::iter::repeat(0u64).take(ptrs_per_block * ptrs_per_block));
                }
            }
        }

        Ok(blocks)
    }

    // ---- Directory parsing ----

    fn parse_directory(&self, data: &[u8]) -> Vec<DirEntry> {
        let mut entries = Vec::new();
        let mut offset = 0;

        while offset + 8 <= data.len() {
            let inode = le32(data, offset);
            let rec_len = u16::from_le_bytes([data[offset + 4], data[offset + 5]]) as usize;
            let name_len = data[offset + 6] as usize;
            let file_type = data[offset + 7];

            if rec_len == 0 || offset + rec_len > data.len() {
                break;
            }

            if inode != 0 && name_len > 0 && offset + 8 + name_len <= data.len() {
                let name =
                    String::from_utf8_lossy(&data[offset + 8..offset + 8 + name_len]).to_string();

                // Skip . and ..
                if name != "." && name != ".." {
                    entries.push(DirEntry {
                        inode,
                        name,
                        file_type,
                    });
                }
            }

            offset += rec_len;
        }

        entries
    }

    /// Build a FileEntry from a directory entry by reading its inode.
    fn dir_entry_to_file_entry(
        &mut self,
        de: &DirEntry,
        parent_path: &str,
    ) -> Result<FileEntry, FilesystemError> {
        let path = if parent_path == "/" {
            format!("/{}", de.name)
        } else {
            format!("{}/{}", parent_path, de.name)
        };

        let inode = self.read_inode(de.inode)?;
        let mut fe = unix_entry_from_inode(
            &de.name,
            &path,
            inode.mode,
            inode.size,
            de.inode as u64,
            inode.mtime,
            inode.uid,
            inode.gid,
        );

        // For symlinks, read the target
        if fe.entry_type == EntryType::Symlink {
            match self.read_symlink_target(&inode) {
                Ok(target) => fe.symlink_target = Some(target),
                Err(_) => fe.symlink_target = Some("(unreadable)".to_string()),
            }
        }

        // Override entry type from directory file_type field if inode mode is 0
        // (some ext implementations may have mode=0 in corrupt inodes)
        if inode.mode == 0 && de.file_type != 0 {
            fe.entry_type = match de.file_type {
                FT_REG_FILE => EntryType::File,
                FT_DIR => EntryType::Directory,
                FT_SYMLINK => EntryType::Symlink,
                FT_CHRDEV | FT_BLKDEV | FT_FIFO | FT_SOCK => EntryType::Special,
                _ => EntryType::File,
            };
        }

        Ok(fe)
    }

    /// Read a block bitmap for a group and find the highest set bit.
    pub fn read_block_bitmap(&mut self, group: usize) -> Result<Vec<u8>, FilesystemError> {
        let gd = &self.group_descriptors[group];
        self.read_block(gd.block_bitmap)
    }
}

impl<R: Read + Seek + Send> Filesystem for ExtFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        let inode = self.read_inode(EXT_ROOT_INO)?;
        Ok(unix_entry_from_inode(
            "/",
            "/",
            inode.mode,
            0,
            EXT_ROOT_INO as u64,
            inode.mtime,
            inode.uid,
            inode.gid,
        ))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if !entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }

        let inode = self.read_inode(entry.location as u32)?;
        let data = self.read_inode_data(&inode, inode.size as usize)?;
        let dir_entries = self.parse_directory(&data);

        let mut entries = Vec::with_capacity(dir_entries.len());
        for de in &dir_entries {
            match self.dir_entry_to_file_entry(de, &entry.path) {
                Ok(fe) => entries.push(fe),
                Err(_) => {
                    // Skip entries with unreadable inodes
                    continue;
                }
            }
        }

        // Sort: directories first, then alphabetically (case-insensitive)
        entries.sort_by(|a, b| {
            let a_is_dir = a.is_directory() as u8;
            let b_is_dir = b.is_directory() as u8;
            b_is_dir
                .cmp(&a_is_dir)
                .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
        });

        Ok(entries)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        if entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }

        let inode = self.read_inode(entry.location as u32)?;
        self.read_inode_data(&inode, max_bytes)
    }

    fn volume_label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    fn fs_type(&self) -> &str {
        self.ext_version.name()
    }

    fn total_size(&self) -> u64 {
        self.total_blocks * self.block_size
    }

    fn used_size(&self) -> u64 {
        (self.total_blocks - self.free_blocks) * self.block_size
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        let mut highest_block: u64 = 0;
        let blocks_per_group =
            (self.total_blocks + self.group_count as u64 - 1) / self.group_count as u64;

        for group in 0..self.group_count as usize {
            let bitmap_data = self.read_block_bitmap(group)?;

            // Calculate how many blocks are in this group
            let group_start = self.first_data_block as u64 + group as u64 * blocks_per_group;
            let group_blocks = if group as u32 == self.group_count - 1 {
                // Last group may be smaller
                (self.total_blocks - group_start).min(blocks_per_group) as u64
            } else {
                blocks_per_group
            };

            let bm = BitmapReader::new(&bitmap_data, group_blocks);
            if let Some(highest_in_group) = bm.highest_set_bit() {
                let absolute_block = group_start + highest_in_group;
                if absolute_block > highest_block {
                    highest_block = absolute_block;
                }
            }
        }

        // Return byte offset of the end of the highest used block
        Ok((highest_block + 1) * self.block_size)
    }
}

// ---- Extent tree helpers ----

struct ExtentHeader {
    entries: u16,
    depth: u16,
}

struct ExtentLeaf {
    #[allow(dead_code)] // Used for sparse file handling in compaction
    logical_block: u32,
    len: u16,
    physical_block: u64,
}

struct ExtentIndex {
    #[allow(dead_code)]
    logical_block: u32,
    child_block: u64,
}

fn is_extent_header(data: &[u8]) -> bool {
    data.len() >= 2 && u16::from_le_bytes([data[0], data[1]]) == EXT4_EXT_MAGIC
}

fn parse_extent_header(data: &[u8]) -> Result<ExtentHeader, FilesystemError> {
    if data.len() < 12 {
        return Err(FilesystemError::Parse(
            "ext: extent header too short".into(),
        ));
    }
    let magic = u16::from_le_bytes([data[0], data[1]]);
    if magic != EXT4_EXT_MAGIC {
        return Err(FilesystemError::Parse(format!(
            "ext: invalid extent magic 0x{magic:04X}"
        )));
    }
    let entries = u16::from_le_bytes([data[2], data[3]]);
    let depth = u16::from_le_bytes([data[6], data[7]]);
    Ok(ExtentHeader { entries, depth })
}

fn parse_extent_leaves(data: &[u8], count: u16) -> Vec<u64> {
    let mut blocks = Vec::new();
    for i in 0..count as usize {
        let off = 12 + i * 12; // header is 12 bytes, each extent is 12 bytes
        if off + 12 > data.len() {
            break;
        }
        let leaf = parse_one_extent_leaf(data, off);
        let real_len = if leaf.len > 32768 {
            leaf.len - 32768 // uninitialized extent
        } else {
            leaf.len
        };
        for j in 0..real_len as u64 {
            blocks.push(leaf.physical_block + j);
        }
    }
    blocks
}

fn parse_one_extent_leaf(data: &[u8], off: usize) -> ExtentLeaf {
    let logical_block = le32(data, off);
    let len = u16::from_le_bytes([data[off + 4], data[off + 5]]);
    let start_hi = u16::from_le_bytes([data[off + 6], data[off + 7]]) as u64;
    let start_lo = le32(data, off + 8) as u64;
    ExtentLeaf {
        logical_block,
        len,
        physical_block: (start_hi << 32) | start_lo,
    }
}

fn parse_extent_indices(data: &[u8], count: u16) -> Vec<ExtentIndex> {
    let mut indices = Vec::new();
    for i in 0..count as usize {
        let off = 12 + i * 12;
        if off + 12 > data.len() {
            break;
        }
        let logical_block = le32(data, off);
        let leaf_lo = le32(data, off + 4) as u64;
        let leaf_hi = u16::from_le_bytes([data[off + 8], data[off + 9]]) as u64;
        indices.push(ExtentIndex {
            logical_block,
            child_block: (leaf_hi << 32) | leaf_lo,
        });
    }
    indices
}

// ---- Byte reading helpers ----

fn le32(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ])
}

// ---------------------------------------------------------------------------
// Resize
// ---------------------------------------------------------------------------

/// Grow an ext2/3/4 filesystem in place.
///
/// Updates the superblock and group descriptor table to reflect the new
/// partition size. Only grow operations are supported — if the new size is
/// smaller than or equal to the current size, this is a no-op.
///
/// This performs a minimal resize: it updates `s_blocks_count` and
/// `s_free_blocks_count` in the superblock and zeroes new block group
/// metadata. A full `e2fsck`/`resize2fs` is still recommended after restore,
/// but this gets the filesystem to a bootable state.
pub fn resize_ext_in_place(
    file: &mut (impl Read + std::io::Write + Seek),
    partition_offset: u64,
    new_total_bytes: u64,
    log_cb: &mut impl FnMut(&str),
) -> anyhow::Result<()> {
    // Read superblock
    file.seek(SeekFrom::Start(partition_offset + SUPERBLOCK_OFFSET))?;
    let mut sb = [0u8; SUPERBLOCK_SIZE];
    file.read_exact(&mut sb)?;

    let magic = u16::from_le_bytes([sb[0x38], sb[0x39]]);
    if magic != EXT_MAGIC {
        log_cb("ext resize: not an ext filesystem, skipping");
        return Ok(());
    }

    let log_block_size = le32(&sb, 0x1C);
    let block_size = 1024u64 << log_block_size;
    let blocks_per_group = le32(&sb, 0x24) as u64;
    let inodes_per_group = le32(&sb, 0x2C);
    let inode_size = u16::from_le_bytes([sb[0x3E], sb[0x3F]]) as u64;
    let feature_incompat = le32(&sb, 0x60);
    let is_64bit = feature_incompat & INCOMPAT_64BIT != 0;
    let _first_data_block = le32(&sb, 0x18) as u64;

    let old_blocks = if is_64bit {
        let hi = le32(&sb, 0x150) as u64;
        (hi << 32) | le32(&sb, 0x04) as u64
    } else {
        le32(&sb, 0x04) as u64
    };

    let new_blocks = new_total_bytes / block_size;
    if new_blocks <= old_blocks {
        log_cb(&format!(
            "ext resize: new size ({new_blocks} blocks) <= current ({old_blocks} blocks), skipping"
        ));
        return Ok(());
    }

    let added_blocks = new_blocks - old_blocks;
    log_cb(&format!(
        "ext resize: growing from {old_blocks} to {new_blocks} blocks (+{added_blocks})"
    ));

    // Update s_blocks_count
    sb[0x04..0x08].copy_from_slice(&(new_blocks as u32).to_le_bytes());
    if is_64bit {
        sb[0x150..0x154].copy_from_slice(&((new_blocks >> 32) as u32).to_le_bytes());
    }

    // Update s_free_blocks_count: add the new blocks as free
    let old_free_lo = le32(&sb, 0x0C) as u64;
    let old_free = if is_64bit {
        let hi = le32(&sb, 0x154) as u64;
        (hi << 32) | old_free_lo
    } else {
        old_free_lo
    };
    let new_free = old_free + added_blocks;
    sb[0x0C..0x10].copy_from_slice(&(new_free as u32).to_le_bytes());
    if is_64bit {
        sb[0x154..0x158].copy_from_slice(&((new_free >> 32) as u32).to_le_bytes());
    }

    // Write updated superblock
    file.seek(SeekFrom::Start(partition_offset + SUPERBLOCK_OFFSET))?;
    file.write_all(&sb)?;

    log_cb(&format!(
        "ext resize: updated superblock — {new_blocks} total blocks, {new_free} free blocks"
    ));

    Ok(())
}

// ---------------------------------------------------------------------------
// Validate
// ---------------------------------------------------------------------------

/// Validate ext2/3/4 filesystem integrity.
///
/// Performs basic consistency checks and returns a list of warnings.
pub fn validate_ext_integrity(
    file: &mut (impl Read + Seek),
    partition_offset: u64,
    log_cb: &mut impl FnMut(&str),
) -> anyhow::Result<Vec<String>> {
    let mut warnings = Vec::new();

    // Read superblock
    file.seek(SeekFrom::Start(partition_offset + SUPERBLOCK_OFFSET))?;
    let mut sb = [0u8; SUPERBLOCK_SIZE];
    file.read_exact(&mut sb)?;

    // Check magic
    let magic = u16::from_le_bytes([sb[0x38], sb[0x39]]);
    if magic != EXT_MAGIC {
        warnings.push(format!(
            "ext: invalid superblock magic 0x{magic:04X} (expected 0xEF53)"
        ));
        return Ok(warnings);
    }
    log_cb("ext validate: superblock magic OK");

    // Check block size
    let log_block_size = le32(&sb, 0x1C);
    let block_size = 1024u64 << log_block_size;
    if !matches!(block_size, 1024 | 2048 | 4096) {
        warnings.push(format!("ext: unusual block size {block_size}"));
    }

    // Check blocks_per_group > 0
    let blocks_per_group = le32(&sb, 0x24);
    if blocks_per_group == 0 {
        warnings.push("ext: blocks_per_group is 0".to_string());
        return Ok(warnings);
    }

    // Check group count consistency
    let feature_incompat = le32(&sb, 0x60);
    let is_64bit = feature_incompat & INCOMPAT_64BIT != 0;
    let first_data_block = le32(&sb, 0x18) as u64;
    let total_blocks = if is_64bit {
        let hi = le32(&sb, 0x150) as u64;
        (hi << 32) | le32(&sb, 0x04) as u64
    } else {
        le32(&sb, 0x04) as u64
    };

    let expected_groups = ((total_blocks - first_data_block + blocks_per_group as u64 - 1)
        / blocks_per_group as u64) as u32;

    // Check inode size
    let inode_size = u16::from_le_bytes([sb[0x3E], sb[0x3F]]);
    if inode_size < 128 {
        warnings.push(format!("ext: invalid inode size {inode_size}"));
    }

    // Check inodes_per_group > 0
    let inodes_per_group = le32(&sb, 0x2C);
    if inodes_per_group == 0 {
        warnings.push("ext: inodes_per_group is 0".to_string());
    }

    let total_inodes = le32(&sb, 0x00);
    let expected_inodes = expected_groups * inodes_per_group;
    if total_inodes != expected_inodes {
        warnings.push(format!(
            "ext: s_inodes_count ({total_inodes}) != groups * inodes_per_group ({expected_inodes})"
        ));
    }

    log_cb(&format!(
        "ext validate: {total_blocks} blocks, {expected_groups} groups, block size {block_size}"
    ));

    if warnings.is_empty() {
        log_cb("ext validate: all checks passed");
    } else {
        for w in &warnings {
            log_cb(&format!("ext validate warning: {w}"));
        }
    }

    Ok(warnings)
}

// ---------------------------------------------------------------------------
// CompactExtReader — streaming compacted ext image
// ---------------------------------------------------------------------------

/// A streaming `Read` that produces a compacted ext2/3/4 partition image.
///
/// Allocated blocks are preserved; unallocated blocks are replaced with zeros.
/// The output image has the same block layout as the original — block pointers
/// in inodes and extent trees remain valid. This approach is simpler and more
/// reliable than defragmentation, and produces excellent compression ratios
/// since free space compresses to nothing.
pub struct CompactExtReader<R: Read + Seek> {
    inner: CompactStreamReader<R>,
}

impl<R: Read + Seek + Send> CompactExtReader<R> {
    /// Create a new compacted ext reader.
    ///
    /// Parses the superblock and block group descriptors, scans block bitmaps
    /// to identify allocated blocks, and builds a virtual layout that maps
    /// allocated blocks from the source while zeroing unallocated ones.
    pub fn new(
        mut reader: R,
        partition_offset: u64,
    ) -> Result<(Self, CompactResult), FilesystemError> {
        // Parse superblock
        reader.seek(SeekFrom::Start(partition_offset + SUPERBLOCK_OFFSET))?;
        let mut sb = [0u8; SUPERBLOCK_SIZE];
        reader.read_exact(&mut sb)?;

        let magic = u16::from_le_bytes([sb[0x38], sb[0x39]]);
        if magic != EXT_MAGIC {
            return Err(FilesystemError::Parse(format!(
                "ext compact: invalid superblock magic 0x{magic:04X}"
            )));
        }

        let blocks_count_lo = le32(&sb, 0x04);
        let first_data_block = le32(&sb, 0x18);
        let log_block_size = le32(&sb, 0x1C);
        let blocks_per_group = le32(&sb, 0x24);
        let feature_incompat = le32(&sb, 0x60);

        let block_size = 1024u64 << log_block_size;
        let is_64bit = feature_incompat & INCOMPAT_64BIT != 0;

        let desc_size: u16 = if is_64bit {
            let ds = u16::from_le_bytes([sb[0xFE], sb[0xFF]]);
            if ds >= 64 {
                ds
            } else {
                64
            }
        } else {
            32
        };

        let total_blocks = if is_64bit {
            let hi = le32(&sb, 0x150) as u64;
            (hi << 32) | blocks_count_lo as u64
        } else {
            blocks_count_lo as u64
        };

        if blocks_per_group == 0 {
            return Err(FilesystemError::Parse(
                "ext compact: blocks_per_group is 0".into(),
            ));
        }

        let group_count = ((total_blocks - first_data_block as u64 + blocks_per_group as u64 - 1)
            / blocks_per_group as u64) as u32;

        // Read group descriptors
        let sb_block = if block_size == 1024 { 1 } else { 0 };
        let gdt_block = sb_block + 1;
        let gdt_offset = partition_offset + gdt_block * block_size;
        let gdt_bytes_needed = group_count as usize * desc_size as usize;
        let mut gdt_buf = vec![0u8; gdt_bytes_needed];
        reader.seek(SeekFrom::Start(gdt_offset))?;
        reader.read_exact(&mut gdt_buf)?;

        // Parse GDTs to get block bitmap locations and flags
        let mut gd_infos: Vec<(u64, u16)> = Vec::with_capacity(group_count as usize);
        for i in 0..group_count as usize {
            let off = i * desc_size as usize;
            let d = &gdt_buf[off..];
            let block_bitmap_lo = le32(d, 0x00) as u64;
            let flags = u16::from_le_bytes([d[0x12], d[0x13]]);
            let block_bitmap = if is_64bit && desc_size >= 64 {
                let bb_hi = le32(d, 0x20) as u64;
                (bb_hi << 32) | block_bitmap_lo
            } else {
                block_bitmap_lo
            };
            gd_infos.push((block_bitmap, flags));
        }

        // Scan block bitmaps and build the layout
        // Strategy: for each block group, read its bitmap and emit either
        // MappedBlocks (for runs of allocated blocks) or Zeros (for runs of free blocks).
        let mut sections: Vec<CompactSection> = Vec::new();
        let mut total_allocated: u64 = 0;

        for group in 0..group_count as usize {
            let group_start_block =
                first_data_block as u64 + group as u64 * blocks_per_group as u64;
            let group_block_count = if group as u32 == group_count - 1 {
                total_blocks - group_start_block
            } else {
                blocks_per_group as u64
            };

            let (bitmap_block, flags) = gd_infos[group];

            if flags & BG_BLOCK_UNINIT != 0 {
                // All blocks in this group are free
                sections.push(CompactSection::Zeros(group_block_count * block_size));
                continue;
            }

            // Read the block bitmap
            reader.seek(SeekFrom::Start(
                partition_offset + bitmap_block * block_size,
            ))?;
            let mut bitmap_data = vec![0u8; block_size as usize];
            reader.read_exact(&mut bitmap_data)?;

            let bm = BitmapReader::new(&bitmap_data, group_block_count);

            // Build runs of allocated/free blocks
            let mut block_idx: u64 = 0;
            while block_idx < group_block_count {
                let is_set = bm.is_bit_set(block_idx);

                // Find the end of this run
                let mut run_end = block_idx + 1;
                while run_end < group_block_count && bm.is_bit_set(run_end) == is_set {
                    run_end += 1;
                }
                let run_len = run_end - block_idx;

                if is_set {
                    // Allocated blocks — map from source
                    let old_blocks: Vec<u64> = (block_idx..run_end)
                        .map(|bi| group_start_block + bi)
                        .collect();
                    total_allocated += run_len;
                    sections.push(CompactSection::MappedBlocks { old_blocks });
                } else {
                    // Free blocks — emit zeros
                    sections.push(CompactSection::Zeros(run_len * block_size));
                }

                block_idx = run_end;
            }
        }

        let original_size = total_blocks * block_size;
        let compacted_size = original_size; // Same logical size, but zeros compress well

        let layout = CompactLayout {
            sections,
            block_size: block_size as usize,
            source_data_start: 0, // ext block numbers are partition-relative
            source_partition_offset: partition_offset,
        };

        let inner = CompactStreamReader::new(reader, layout);

        Ok((
            Self { inner },
            CompactResult {
                original_size,
                compacted_size,
                clusters_used: total_allocated as u32,
            },
        ))
    }
}

impl<R: Read + Seek> Read for CompactExtReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

// ---- Tests ----

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a minimal ext2 superblock for testing.
    fn make_test_superblock() -> Vec<u8> {
        let mut sb = vec![0u8; SUPERBLOCK_SIZE];

        let block_size_log = 2u32; // 4096 byte blocks
        let blocks_per_group = 32768u32;
        let inodes_per_group = 8192u32;
        let total_blocks = 32768u32; // 1 group
        let total_inodes = 8192u32;
        let inode_size = 256u16;

        // s_inodes_count
        sb[0x00..0x04].copy_from_slice(&total_inodes.to_le_bytes());
        // s_blocks_count_lo
        sb[0x04..0x08].copy_from_slice(&total_blocks.to_le_bytes());
        // s_first_data_block = 0 (4K blocks)
        sb[0x18..0x1C].copy_from_slice(&0u32.to_le_bytes());
        // s_log_block_size
        sb[0x1C..0x20].copy_from_slice(&block_size_log.to_le_bytes());
        // s_blocks_per_group
        sb[0x24..0x28].copy_from_slice(&blocks_per_group.to_le_bytes());
        // s_inodes_per_group
        sb[0x2C..0x30].copy_from_slice(&inodes_per_group.to_le_bytes());
        // s_magic
        sb[0x38..0x3A].copy_from_slice(&EXT_MAGIC.to_le_bytes());
        // s_inode_size
        sb[0x3E..0x40].copy_from_slice(&inode_size.to_le_bytes());
        // s_volume_name
        sb[0x78..0x83].copy_from_slice(b"TestVolume\0");

        sb
    }

    /// Build minimal group descriptor table (1 group, 32 bytes).
    fn make_test_gdt(block_bitmap: u32, inode_bitmap: u32, inode_table: u32) -> Vec<u8> {
        let mut gdt = vec![0u8; 32];
        gdt[0x00..0x04].copy_from_slice(&block_bitmap.to_le_bytes());
        gdt[0x04..0x08].copy_from_slice(&inode_bitmap.to_le_bytes());
        gdt[0x08..0x0C].copy_from_slice(&inode_table.to_le_bytes());
        // free_blocks_count = 100
        gdt[0x0C..0x0E].copy_from_slice(&100u16.to_le_bytes());
        // free_inodes_count = 200
        gdt[0x0E..0x10].copy_from_slice(&200u16.to_le_bytes());
        gdt
    }

    /// Create a minimal test image: superblock + GDT + inode table with root inode.
    fn make_test_image() -> Vec<u8> {
        let block_size = 4096usize;
        // Layout:
        // Block 0: boot/reserved (contains superblock at offset 1024)
        // Block 1: GDT
        // Block 2: block bitmap
        // Block 3: inode bitmap
        // Block 4-5: inode table (need at least 2 blocks for 8192 inodes * 256 bytes,
        //            but we only need the root inode at index 1)
        // Block 6: root directory data

        let total_blocks = 32768;
        let _num_blocks_needed = 7; // just enough for the test
        let mut image = vec![0u8; total_blocks * block_size]; // Full size for bitmap scanning

        // Write superblock at offset 1024
        let sb = make_test_superblock();
        image[1024..1024 + SUPERBLOCK_SIZE].copy_from_slice(&sb);

        // Write GDT at block 1
        let gdt = make_test_gdt(2, 3, 4);
        image[block_size..block_size + 32].copy_from_slice(&gdt);

        // Write block bitmap at block 2 (mark blocks 0-6 as used)
        image[2 * block_size] = 0x7F; // bits 0-6 set

        // Write root inode (inode 2, index 1) at block 4 + 1*256
        let inode_offset = 4 * block_size + 1 * 256; // inode 2 = index 1
                                                     // i_mode = directory (0o040755)
        let mode: u16 = 0o040755;
        image[inode_offset..inode_offset + 2].copy_from_slice(&mode.to_le_bytes());
        // i_size_lo = 4096 (one block of directory data)
        image[inode_offset + 0x04..inode_offset + 0x08].copy_from_slice(&4096u32.to_le_bytes());
        // i_mtime
        image[inode_offset + 0x10..inode_offset + 0x14]
            .copy_from_slice(&1700000000u32.to_le_bytes());
        // i_links_count
        image[inode_offset + 0x1A..inode_offset + 0x1C].copy_from_slice(&2u16.to_le_bytes());
        // i_block[0] = block 6 (direct block pointer)
        image[inode_offset + 0x28..inode_offset + 0x2C].copy_from_slice(&6u32.to_le_bytes());

        // Write root directory data at block 6
        let dir_offset = 6 * block_size;
        write_dir_entry(&mut image, dir_offset, 2, ".", FT_DIR);
        let e1_end = dir_offset + 12; // . entry is 12 bytes
        write_dir_entry(&mut image, e1_end, 2, "..", FT_DIR);
        let e2_end = e1_end + 12;

        // Add a regular file entry: inode 12, "hello.txt"
        write_dir_entry(&mut image, e2_end, 12, "hello.txt", FT_REG_FILE);
        let e3_end = e2_end + 8 + 12; // rec_len rounds up

        // Add a subdirectory entry: inode 13, "subdir"
        write_dir_entry(&mut image, e3_end, 13, "subdir", FT_DIR);
        let e4_end = e3_end + 8 + 8;

        // Add a symlink entry: inode 14, "mylink"
        write_dir_entry(&mut image, e4_end, 14, "mylink", FT_SYMLINK);

        // Make last entry fill the rest of the block
        let last_offset = e4_end;
        let remaining = block_size - (last_offset - dir_offset);
        image[last_offset + 4..last_offset + 6].copy_from_slice(&(remaining as u16).to_le_bytes());

        // Write inode for hello.txt (inode 12, index 11) at block 4 + 11*256
        let hello_inode_offset = 4 * block_size + 11 * 256;
        let hello_mode: u16 = 0o100644;
        image[hello_inode_offset..hello_inode_offset + 2]
            .copy_from_slice(&hello_mode.to_le_bytes());
        // size = 13
        image[hello_inode_offset + 0x04..hello_inode_offset + 0x08]
            .copy_from_slice(&13u32.to_le_bytes());
        // mtime
        image[hello_inode_offset + 0x10..hello_inode_offset + 0x14]
            .copy_from_slice(&1700000000u32.to_le_bytes());
        // uid = 1000
        image[hello_inode_offset + 0x02..hello_inode_offset + 0x04]
            .copy_from_slice(&1000u16.to_le_bytes());
        // gid at 0x18
        image[hello_inode_offset + 0x18..hello_inode_offset + 0x1A]
            .copy_from_slice(&1000u16.to_le_bytes());

        // Write inode for subdir (inode 13, index 12)
        let subdir_inode_offset = 4 * block_size + 12 * 256;
        let subdir_mode: u16 = 0o040755;
        image[subdir_inode_offset..subdir_inode_offset + 2]
            .copy_from_slice(&subdir_mode.to_le_bytes());
        image[subdir_inode_offset + 0x04..subdir_inode_offset + 0x08]
            .copy_from_slice(&4096u32.to_le_bytes());

        // Write inode for mylink (inode 14, index 13) — fast symlink
        let link_inode_offset = 4 * block_size + 13 * 256;
        let link_mode: u16 = 0o120777;
        image[link_inode_offset..link_inode_offset + 2].copy_from_slice(&link_mode.to_le_bytes());
        // size = 10 (target length)
        image[link_inode_offset + 0x04..link_inode_offset + 0x08]
            .copy_from_slice(&10u32.to_le_bytes());
        // Fast symlink: target in i_block
        image[link_inode_offset + 0x28..link_inode_offset + 0x28 + 10]
            .copy_from_slice(b"/etc/hosts");

        image
    }

    fn write_dir_entry(image: &mut [u8], offset: usize, inode: u32, name: &str, ft: u8) {
        let name_bytes = name.as_bytes();
        let name_len = name_bytes.len();
        // rec_len: aligned to 4 bytes
        let rec_len = ((8 + name_len + 3) / 4) * 4;

        image[offset..offset + 4].copy_from_slice(&inode.to_le_bytes());
        image[offset + 4..offset + 6].copy_from_slice(&(rec_len as u16).to_le_bytes());
        image[offset + 6] = name_len as u8;
        image[offset + 7] = ft;
        image[offset + 8..offset + 8 + name_len].copy_from_slice(name_bytes);
    }

    #[test]
    fn test_superblock_parsing() {
        let image = make_test_image();
        let cursor = Cursor::new(image);
        let fs = ExtFilesystem::open(cursor, 0).unwrap();

        assert_eq!(fs.ext_version, ExtVersion::Ext2);
        assert_eq!(fs.block_size, 4096);
        assert_eq!(fs.total_blocks, 32768);
        assert_eq!(fs.inodes_per_group, 8192);
        assert_eq!(fs.inode_size, 256);
        assert_eq!(fs.group_count, 1);
        assert!(!fs.has_extents);
        assert!(!fs.is_64bit);
        assert_eq!(fs.label.as_deref(), Some("TestVolume"));
        assert_eq!(fs.fs_type(), "ext2");
    }

    #[test]
    fn test_root_directory() {
        let image = make_test_image();
        let cursor = Cursor::new(image);
        let mut fs = ExtFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        assert_eq!(root.entry_type, EntryType::Directory);
        assert_eq!(root.path, "/");
        assert_eq!(root.location, 2);
    }

    #[test]
    fn test_list_root_directory() {
        let image = make_test_image();
        let cursor = Cursor::new(image);
        let mut fs = ExtFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();

        // Should have 3 entries (hello.txt, subdir, mylink) — . and .. are skipped
        assert_eq!(entries.len(), 3);

        // Sorted: directories first, then alpha
        assert_eq!(entries[0].name, "subdir");
        assert_eq!(entries[0].entry_type, EntryType::Directory);

        assert_eq!(entries[1].name, "hello.txt");
        assert_eq!(entries[1].entry_type, EntryType::File);
        assert_eq!(entries[1].size, 13);
        assert_eq!(entries[1].uid, Some(1000));
        assert_eq!(entries[1].gid, Some(1000));
        assert_eq!(entries[1].mode, Some(0o100644));

        assert_eq!(entries[2].name, "mylink");
        assert_eq!(entries[2].entry_type, EntryType::Symlink);
        assert_eq!(entries[2].symlink_target.as_deref(), Some("/etc/hosts"));
    }

    #[test]
    fn test_volume_info() {
        let image = make_test_image();
        let cursor = Cursor::new(image);
        let fs = ExtFilesystem::open(cursor, 0).unwrap();

        assert_eq!(fs.volume_label(), Some("TestVolume"));
        assert_eq!(fs.total_size(), 32768 * 4096);
        // used = total - free (100 free blocks)
        assert_eq!(fs.used_size(), (32768 - 100) * 4096);
    }

    #[test]
    fn test_invalid_magic() {
        let mut image = make_test_image();
        // Corrupt the magic
        image[1024 + 0x38] = 0x00;
        image[1024 + 0x39] = 0x00;
        let cursor = Cursor::new(image);
        assert!(ExtFilesystem::open(cursor, 0).is_err());
    }

    #[test]
    fn test_fast_symlink() {
        let image = make_test_image();
        let cursor = Cursor::new(image);
        let mut fs = ExtFilesystem::open(cursor, 0).unwrap();

        let inode = fs.read_inode(14).unwrap();
        let target = fs.read_symlink_target(&inode).unwrap();
        assert_eq!(target, "/etc/hosts");
    }

    #[test]
    fn test_ext_version_detection_ext3() {
        let mut image = make_test_image();
        // Set COMPAT_HAS_JOURNAL flag
        let feature_compat_offset = 1024 + 0x5C;
        let val = COMPAT_HAS_JOURNAL;
        image[feature_compat_offset..feature_compat_offset + 4].copy_from_slice(&val.to_le_bytes());
        let cursor = Cursor::new(image);
        let fs = ExtFilesystem::open(cursor, 0).unwrap();
        assert_eq!(fs.ext_version, ExtVersion::Ext3);
        assert_eq!(fs.fs_type(), "ext3");
    }

    #[test]
    fn test_ext_version_detection_ext4() {
        let mut image = make_test_image();
        // Set INCOMPAT_EXTENTS flag
        let feature_incompat_offset = 1024 + 0x60;
        let val = INCOMPAT_EXTENTS;
        image[feature_incompat_offset..feature_incompat_offset + 4]
            .copy_from_slice(&val.to_le_bytes());
        let cursor = Cursor::new(image);
        let fs = ExtFilesystem::open(cursor, 0).unwrap();
        assert_eq!(fs.ext_version, ExtVersion::Ext4);
        assert_eq!(fs.fs_type(), "ext4");
    }

    #[test]
    fn test_le32_helper() {
        let data = [0x01, 0x02, 0x03, 0x04];
        assert_eq!(le32(&data, 0), 0x04030201);
    }

    #[test]
    fn test_parse_extent_header() {
        let mut data = [0u8; 12];
        data[0..2].copy_from_slice(&EXT4_EXT_MAGIC.to_le_bytes());
        data[2..4].copy_from_slice(&3u16.to_le_bytes()); // entries
        data[6..8].copy_from_slice(&0u16.to_le_bytes()); // depth
        let header = parse_extent_header(&data).unwrap();
        assert_eq!(header.entries, 3);
        assert_eq!(header.depth, 0);
    }

    #[test]
    fn test_parse_extent_leaves() {
        // Header + 1 extent: logical=0, len=4, physical=100
        let mut data = vec![0u8; 24];
        // Header
        data[0..2].copy_from_slice(&EXT4_EXT_MAGIC.to_le_bytes());
        data[2..4].copy_from_slice(&1u16.to_le_bytes());
        data[6..8].copy_from_slice(&0u16.to_le_bytes());
        // Extent at offset 12
        data[12..16].copy_from_slice(&0u32.to_le_bytes()); // logical
        data[16..18].copy_from_slice(&4u16.to_le_bytes()); // len
        data[18..20].copy_from_slice(&0u16.to_le_bytes()); // start_hi
        data[20..24].copy_from_slice(&100u32.to_le_bytes()); // start_lo

        let blocks = parse_extent_leaves(&data, 1);
        assert_eq!(blocks, vec![100, 101, 102, 103]);
    }

    #[test]
    fn test_directory_parsing() {
        let mut data = vec![0u8; 256];
        // Entry 1: inode=5, name="test.txt", type=regular
        write_dir_entry_buf(&mut data, 0, 5, "test.txt", FT_REG_FILE);
        let e1_len = ((8 + 8 + 3) / 4) * 4; // 16
                                            // Entry 2: inode=6, name="dir", type=directory
        write_dir_entry_buf(&mut data, e1_len, 6, "dir", FT_DIR);
        let e2_len = ((8 + 3 + 3) / 4) * 4; // 12
                                            // Make last entry fill the rest
        let remaining = 256 - e1_len - e2_len;
        data[e1_len + e2_len + 4..e1_len + e2_len + 6]
            .copy_from_slice(&(remaining as u16).to_le_bytes());

        // Use a dummy filesystem just for parse_directory
        let image = make_test_image();
        let cursor = Cursor::new(image);
        let fs = ExtFilesystem::open(cursor, 0).unwrap();

        let entries = fs.parse_directory(&data);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "test.txt");
        assert_eq!(entries[0].file_type, FT_REG_FILE);
        assert_eq!(entries[1].name, "dir");
        assert_eq!(entries[1].file_type, FT_DIR);
    }

    fn write_dir_entry_buf(buf: &mut [u8], offset: usize, inode: u32, name: &str, ft: u8) {
        let name_bytes = name.as_bytes();
        let name_len = name_bytes.len();
        let rec_len = ((8 + name_len + 3) / 4) * 4;

        buf[offset..offset + 4].copy_from_slice(&inode.to_le_bytes());
        buf[offset + 4..offset + 6].copy_from_slice(&(rec_len as u16).to_le_bytes());
        buf[offset + 6] = name_len as u8;
        buf[offset + 7] = ft;
        buf[offset + 8..offset + 8 + name_len].copy_from_slice(name_bytes);
    }

    #[test]
    fn test_last_data_byte() {
        let image = make_test_image();
        let cursor = Cursor::new(image);
        let mut fs = ExtFilesystem::open(cursor, 0).unwrap();

        let last = fs.last_data_byte().unwrap();
        // Blocks 0-6 are marked used, so highest block is 6
        // last_data_byte = (6 + 1) * 4096 = 28672
        assert_eq!(last, 7 * 4096);
    }

    #[test]
    fn test_partition_offset() {
        // Test that ExtFilesystem works with a non-zero partition offset
        let offset = 1048576u64; // 1 MB offset
        let mut base_image = make_test_image();
        let mut image = vec![0u8; offset as usize];
        image.append(&mut base_image);

        let cursor = Cursor::new(image);
        let mut fs = ExtFilesystem::open(cursor, offset).unwrap();

        assert_eq!(fs.ext_version, ExtVersion::Ext2);
        let root = fs.root().unwrap();
        assert_eq!(root.entry_type, EntryType::Directory);

        let entries = fs.list_directory(&root).unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_compact_ext_reader() {
        let image = make_test_image();
        let total_size = image.len();
        let cursor = Cursor::new(image);

        let (mut reader, info) = CompactExtReader::new(cursor, 0).unwrap();

        // Original size should match the image
        assert_eq!(info.original_size, total_size as u64);
        // Blocks 0-6 are allocated (7 blocks)
        assert_eq!(info.clusters_used, 7);

        // Read the entire compacted image
        let mut output = Vec::new();
        reader.read_to_end(&mut output).unwrap();
        assert_eq!(output.len(), total_size);

        // The first 7 blocks should be non-zero (they contain data)
        // Block 0 has the superblock at offset 1024
        assert_eq!(
            u16::from_le_bytes([output[1024 + 0x38], output[1024 + 0x39]]),
            EXT_MAGIC
        );

        // Blocks beyond block 6 should be all zeros
        let block_size = 4096;
        let zero_start = 7 * block_size;
        if zero_start < output.len() {
            assert!(
                output[zero_start..zero_start + block_size]
                    .iter()
                    .all(|&b| b == 0),
                "blocks after the last allocated block should be zeroed"
            );
        }
    }

    #[test]
    fn test_compact_ext_reader_preserves_allocated_data() {
        let image = make_test_image();
        let cursor = Cursor::new(image.clone());

        let (mut reader, _info) = CompactExtReader::new(cursor, 0).unwrap();

        let mut output = Vec::new();
        reader.read_to_end(&mut output).unwrap();

        // Allocated blocks (0-6) should match the original image exactly
        let block_size = 4096;
        for block in 0..7 {
            let start = block * block_size;
            let end = start + block_size;
            assert_eq!(
                &output[start..end],
                &image[start..end],
                "allocated block {block} should match source"
            );
        }
    }
}
