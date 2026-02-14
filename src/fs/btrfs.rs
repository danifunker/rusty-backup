//! btrfs filesystem browsing.
//!
//! Implements the `Filesystem` trait for btrfs partitions, supporting
//! superblock parsing, chunk tree (logical-to-physical translation),
//! B-tree traversal, directory listing, file reading (inline + regular
//! extents), and symlink resolution.
//!
//! Only single-device images are supported (always uses stripe[0]).

use std::io::{Read, Seek, SeekFrom};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};
use super::unix_common::compact::{CompactLayout, CompactSection, CompactStreamReader};
use super::unix_common::inode::unix_entry_from_inode;
use crate::fs::CompactResult;

// ---- Constants ----

/// Superblock is always at byte offset 0x10000 (64 KiB) from partition start.
const SUPERBLOCK_OFFSET: u64 = 0x10000;
const BTRFS_MAGIC: &[u8; 8] = b"_BHRfS_M";

// Key type constants (objectid, type, offset form a 17-byte key)
const INODE_ITEM_KEY: u8 = 1;
#[allow(dead_code)]
const INODE_REF_KEY: u8 = 12;
#[allow(dead_code)]
const DIR_ITEM_KEY: u8 = 84;
const DIR_INDEX_KEY: u8 = 96;
const EXTENT_DATA_KEY: u8 = 108;
const ROOT_ITEM_KEY: u8 = 132;
const CHUNK_ITEM_KEY: u8 = 228;

// Well-known objectids
const BTRFS_FS_TREE_OBJECTID: u64 = 5;
const BTRFS_FIRST_FREE_OBJECTID: u64 = 256;

// btrfs_dir_item file types (d_type field)
#[allow(dead_code)]
const BTRFS_FT_REG_FILE: u8 = 1;
#[allow(dead_code)]
const BTRFS_FT_DIR: u8 = 2;
#[allow(dead_code)]
const BTRFS_FT_CHRDEV: u8 = 3;
#[allow(dead_code)]
const BTRFS_FT_BLKDEV: u8 = 4;
#[allow(dead_code)]
const BTRFS_FT_FIFO: u8 = 5;
#[allow(dead_code)]
const BTRFS_FT_SOCK: u8 = 6;
#[allow(dead_code)]
const BTRFS_FT_SYMLINK: u8 = 7;

// Extent data types
const BTRFS_FILE_EXTENT_INLINE: u8 = 0;
const BTRFS_FILE_EXTENT_REG: u8 = 1;
const BTRFS_FILE_EXTENT_PREALLOC: u8 = 2;

// Node header size: csum(32) + fsid(16) + bytenr(8) + flags(8) + chunk_tree_uuid(16) + generation(8) + owner(8) + nritems(4) + level(1) = 101
const NODE_HEADER_SIZE: usize = 101;

// ---- Helper functions ----

fn le64(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

fn le32(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(buf[off..off + 4].try_into().unwrap())
}

fn le16(buf: &[u8], off: usize) -> u16 {
    u16::from_le_bytes(buf[off..off + 2].try_into().unwrap())
}

// ---- On-disk key ----

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct BtrfsKey {
    objectid: u64,
    key_type: u8,
    offset: u64,
}

impl BtrfsKey {
    fn parse(buf: &[u8], off: usize) -> Self {
        Self {
            objectid: le64(buf, off),
            key_type: buf[off + 8],
            offset: le64(buf, off + 9),
        }
    }
}

// ---- Chunk mapping ----

#[derive(Debug, Clone)]
struct ChunkMapping {
    logical: u64,
    length: u64,
    physical: u64, // stripe[0] offset
}

// ---- Shared superblock info ----

struct BtrfsSuperblockInfo {
    total_bytes: u64,
    bytes_used: u64,
    node_size: u32,
    sector_size: u32,
    chunk_map: Vec<ChunkMapping>,
    chunk_root: u64,
    root_tree_root: u64,
    label: Option<String>,
}

/// Parse the btrfs superblock and bootstrap the chunk map from sys_chunk_array.
/// Does NOT read the full chunk tree (caller must do that with a reader that has
/// the chunk map bootstrapped).
fn parse_btrfs_superblock<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
) -> Result<BtrfsSuperblockInfo, FilesystemError> {
    reader.seek(SeekFrom::Start(partition_offset + SUPERBLOCK_OFFSET))?;
    let mut sb = [0u8; 4096];
    reader.read_exact(&mut sb)?;

    if &sb[0x40..0x48] != BTRFS_MAGIC {
        return Err(FilesystemError::Parse(format!(
            "btrfs: invalid superblock magic {:?}, expected {:?}",
            &sb[0x40..0x48],
            BTRFS_MAGIC
        )));
    }

    let root = le64(&sb, 0x50);
    let chunk_root = le64(&sb, 0x58);
    let total_bytes = le64(&sb, 0x70);
    let bytes_used = le64(&sb, 0x78);
    let sector_size = le32(&sb, 0x90);
    let node_size = le32(&sb, 0x94);
    let sys_chunk_array_size = le32(&sb, 0xA0);

    if !(4096..=65536).contains(&node_size) {
        return Err(FilesystemError::Parse(format!(
            "btrfs: invalid nodesize {node_size}"
        )));
    }

    let label_bytes = &sb[0x12B..0x12B + 256];
    let label_end = label_bytes.iter().position(|&b| b == 0).unwrap_or(256);
    let label = if label_end > 0 {
        let s = String::from_utf8_lossy(&label_bytes[..label_end])
            .trim()
            .to_string();
        if s.is_empty() {
            None
        } else {
            Some(s)
        }
    } else {
        None
    };

    let sys_chunk_data = &sb[0x32B..0x32B + sys_chunk_array_size as usize];
    let mut chunk_map = parse_sys_chunk_array(sys_chunk_data)?;
    chunk_map.sort_by_key(|c| c.logical);

    Ok(BtrfsSuperblockInfo {
        total_bytes,
        bytes_used,
        node_size,
        sector_size,
        chunk_map,
        chunk_root,
        root_tree_root: root,
        label,
    })
}

// ---- BtrfsFilesystem ----

pub struct BtrfsFilesystem<R> {
    reader: R,
    partition_offset: u64,
    node_size: u32,
    total_bytes: u64,
    bytes_used: u64,
    label: Option<String>,
    chunk_map: Vec<ChunkMapping>,
    root_tree_root: u64,
    fs_tree_root: u64,
}

impl<R: Read + Seek + Send> BtrfsFilesystem<R> {
    /// Open a btrfs filesystem at the given partition offset.
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        let sb_info = parse_btrfs_superblock(&mut reader, partition_offset)?;

        let mut fs = Self {
            reader,
            partition_offset,
            node_size: sb_info.node_size,
            total_bytes: sb_info.total_bytes,
            bytes_used: sb_info.bytes_used,
            label: sb_info.label,
            chunk_map: sb_info.chunk_map,
            root_tree_root: sb_info.root_tree_root,
            fs_tree_root: 0,
        };

        // Read full chunk tree for additional mappings
        fs.read_chunk_tree(sb_info.chunk_root)?;

        // Look up the FS_TREE root item in the root tree
        let fs_tree_key = BtrfsKey {
            objectid: BTRFS_FS_TREE_OBJECTID,
            key_type: ROOT_ITEM_KEY,
            offset: 0,
        };
        let root_item = fs.find_item_ge(sb_info.root_tree_root, &fs_tree_key)?;
        match root_item {
            Some((key, data))
                if key.objectid == BTRFS_FS_TREE_OBJECTID && key.key_type == ROOT_ITEM_KEY =>
            {
                if data.len() < 184 {
                    return Err(FilesystemError::Parse("btrfs: ROOT_ITEM too short".into()));
                }
                fs.fs_tree_root = le64(&data, 176);
            }
            _ => {
                return Err(FilesystemError::Parse(
                    "btrfs: FS_TREE ROOT_ITEM not found".into(),
                ));
            }
        }

        Ok(fs)
    }

    // ---- Chunk map ----

    /// Translate a logical address to physical.
    fn logical_to_physical(&self, logical: u64) -> Result<u64, FilesystemError> {
        // Binary search for the chunk containing this logical address
        let idx = match self.chunk_map.binary_search_by(|c| c.logical.cmp(&logical)) {
            Ok(i) => i,
            Err(0) => {
                return Err(FilesystemError::InvalidData(format!(
                    "btrfs: logical address {logical:#x} before first chunk"
                )));
            }
            Err(i) => i - 1,
        };

        let chunk = &self.chunk_map[idx];
        if logical >= chunk.logical && logical < chunk.logical + chunk.length {
            Ok(chunk.physical + (logical - chunk.logical))
        } else {
            Err(FilesystemError::InvalidData(format!(
                "btrfs: logical address {logical:#x} not in any chunk"
            )))
        }
    }

    /// Read a node (nodesize bytes) at a logical address.
    fn read_node(&mut self, logical: u64) -> Result<Vec<u8>, FilesystemError> {
        let physical = self.logical_to_physical(logical)?;
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + physical))?;
        let mut buf = vec![0u8; self.node_size as usize];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    // ---- Chunk tree reading ----

    /// Read the chunk tree and add all CHUNK_ITEM mappings to chunk_map.
    fn read_chunk_tree(&mut self, chunk_root_logical: u64) -> Result<(), FilesystemError> {
        let node = self.read_node(chunk_root_logical)?;
        let level = node[NODE_HEADER_SIZE - 1];
        let nritems = le32(&node, NODE_HEADER_SIZE - 5);

        if level == 0 {
            // Leaf — scan items for CHUNK_ITEM
            self.parse_chunk_leaf(&node, nritems)?;
        } else {
            // Internal node — recurse into children
            for i in 0..nritems as usize {
                let ptr_off = NODE_HEADER_SIZE + i * 33;
                if ptr_off + 33 > node.len() {
                    break;
                }
                let child_logical = le64(&node, ptr_off + 17);
                self.read_chunk_tree(child_logical)?;
            }
        }
        Ok(())
    }

    fn parse_chunk_leaf(&mut self, node: &[u8], nritems: u32) -> Result<(), FilesystemError> {
        for i in 0..nritems as usize {
            let item_off = NODE_HEADER_SIZE + i * 25;
            if item_off + 25 > node.len() {
                break;
            }
            let key = BtrfsKey::parse(node, item_off);
            let data_offset = le32(node, item_off + 17) as usize;
            let data_size = le32(node, item_off + 21) as usize;

            // Item data is at the end of the leaf, offset from leaf start
            let abs_offset = data_offset + NODE_HEADER_SIZE;
            if key.key_type == CHUNK_ITEM_KEY
                && abs_offset + data_size <= node.len()
                && data_size >= 48
            {
                let chunk_data = &node[abs_offset..abs_offset + data_size];
                let length = le64(chunk_data, 0);
                let _owner = le64(chunk_data, 8);
                let _stripe_len = le64(chunk_data, 16);
                let _type = le64(chunk_data, 24);
                let num_stripes = le16(chunk_data, 44) as usize;

                if num_stripes > 0 && data_size >= 48 + 32 {
                    // stripe[0]: devid(8) + offset(8) + dev_uuid(16)
                    let stripe_offset = le64(chunk_data, 48 + 8);
                    let logical = key.offset;

                    // Only add if not already present
                    let already = self.chunk_map.iter().any(|c| c.logical == logical);
                    if !already {
                        self.chunk_map.push(ChunkMapping {
                            logical,
                            length,
                            physical: stripe_offset,
                        });
                    }
                }
            }
        }
        // Re-sort after adding
        self.chunk_map.sort_by_key(|c| c.logical);
        Ok(())
    }

    // ---- B-tree traversal ----

    /// Find the first item with key >= search_key in the tree rooted at `tree_root`.
    fn find_item_ge(
        &mut self,
        tree_root: u64,
        search_key: &BtrfsKey,
    ) -> Result<Option<(BtrfsKey, Vec<u8>)>, FilesystemError> {
        let node = self.read_node(tree_root)?;
        let level = node[NODE_HEADER_SIZE - 1];
        let nritems = le32(&node, NODE_HEADER_SIZE - 5);

        if nritems == 0 {
            return Ok(None);
        }

        if level == 0 {
            // Leaf — scan items
            self.leaf_find_ge(&node, nritems, search_key)
        } else {
            // Internal node — find the right child to descend into
            let child = self.find_child_for_key(&node, nritems, search_key)?;
            self.find_item_ge(child, search_key)
        }
    }

    /// Leaf scan: find first item with key >= search_key.
    fn leaf_find_ge(
        &self,
        node: &[u8],
        nritems: u32,
        search_key: &BtrfsKey,
    ) -> Result<Option<(BtrfsKey, Vec<u8>)>, FilesystemError> {
        for i in 0..nritems as usize {
            let item_off = NODE_HEADER_SIZE + i * 25;
            if item_off + 25 > node.len() {
                break;
            }
            let key = BtrfsKey::parse(node, item_off);
            if key >= *search_key {
                let data_offset = le32(node, item_off + 17) as usize;
                let data_size = le32(node, item_off + 21) as usize;
                let abs_offset = data_offset + NODE_HEADER_SIZE;
                if abs_offset + data_size <= node.len() {
                    let data = node[abs_offset..abs_offset + data_size].to_vec();
                    return Ok(Some((key, data)));
                }
            }
        }
        Ok(None)
    }

    /// Find the child pointer to descend into for a given search key.
    fn find_child_for_key(
        &mut self,
        node: &[u8],
        nritems: u32,
        search_key: &BtrfsKey,
    ) -> Result<u64, FilesystemError> {
        // Key pointers in internal nodes: key(17) + blockptr(8) + generation(8) = 33 bytes each
        let mut child_idx = 0usize;
        for i in 1..nritems as usize {
            let ptr_off = NODE_HEADER_SIZE + i * 33;
            if ptr_off + 33 > node.len() {
                break;
            }
            let key = BtrfsKey::parse(node, ptr_off);
            if key > *search_key {
                break;
            }
            child_idx = i;
        }
        let ptr_off = NODE_HEADER_SIZE + child_idx * 33;
        Ok(le64(node, ptr_off + 17))
    }

    /// Find all items in range [min_key, max_key] in the tree rooted at `tree_root`.
    fn find_items_in_range(
        &mut self,
        tree_root: u64,
        min_key: &BtrfsKey,
        max_key: &BtrfsKey,
    ) -> Result<Vec<(BtrfsKey, Vec<u8>)>, FilesystemError> {
        let mut results = Vec::new();
        self.collect_items_in_range(tree_root, min_key, max_key, &mut results)?;
        Ok(results)
    }

    fn collect_items_in_range(
        &mut self,
        logical: u64,
        min_key: &BtrfsKey,
        max_key: &BtrfsKey,
        results: &mut Vec<(BtrfsKey, Vec<u8>)>,
    ) -> Result<(), FilesystemError> {
        let node = self.read_node(logical)?;
        let level = node[NODE_HEADER_SIZE - 1];
        let nritems = le32(&node, NODE_HEADER_SIZE - 5);

        if nritems == 0 {
            return Ok(());
        }

        if level == 0 {
            // Leaf — collect matching items
            for i in 0..nritems as usize {
                let item_off = NODE_HEADER_SIZE + i * 25;
                if item_off + 25 > node.len() {
                    break;
                }
                let key = BtrfsKey::parse(&node, item_off);
                if key > *max_key {
                    break;
                }
                if key >= *min_key {
                    let data_offset = le32(&node, item_off + 17) as usize;
                    let data_size = le32(&node, item_off + 21) as usize;
                    let abs_offset = data_offset + NODE_HEADER_SIZE;
                    if abs_offset + data_size <= node.len() {
                        let data = node[abs_offset..abs_offset + data_size].to_vec();
                        results.push((key, data));
                    }
                }
            }
        } else {
            // Internal node — descend into relevant children
            for i in 0..nritems as usize {
                let ptr_off = NODE_HEADER_SIZE + i * 33;
                if ptr_off + 33 > node.len() {
                    break;
                }
                let key = BtrfsKey::parse(&node, ptr_off);
                let child_logical = le64(&node, ptr_off + 17);

                // Check if this child could contain items in range.
                // The child contains items from key[i] to key[i+1]-1.
                // We need to descend if the child's range overlaps [min_key, max_key].
                let next_key = if i + 1 < nritems as usize {
                    let next_off = NODE_HEADER_SIZE + (i + 1) * 33;
                    if next_off + 17 <= node.len() {
                        Some(BtrfsKey::parse(&node, next_off))
                    } else {
                        None
                    }
                } else {
                    None
                };

                // Skip children that are entirely before our range
                if let Some(nk) = next_key {
                    if nk <= *min_key {
                        continue;
                    }
                }
                // Skip children that are entirely after our range
                if key > *max_key {
                    break;
                }

                self.collect_items_in_range(child_logical, min_key, max_key, results)?;
            }
        }

        Ok(())
    }

    // ---- Inode reading ----

    /// Read INODE_ITEM for an inode number in a given tree.
    fn read_inode_item(
        &mut self,
        tree_root: u64,
        inode_num: u64,
    ) -> Result<InodeData, FilesystemError> {
        let key = BtrfsKey {
            objectid: inode_num,
            key_type: INODE_ITEM_KEY,
            offset: 0,
        };
        match self.find_item_ge(tree_root, &key)? {
            Some((found_key, data))
                if found_key.objectid == inode_num && found_key.key_type == INODE_ITEM_KEY =>
            {
                parse_inode_item(&data)
            }
            _ => Err(FilesystemError::NotFound(format!(
                "btrfs: inode {inode_num} not found"
            ))),
        }
    }

    // ---- Directory listing ----

    /// List directory entries by scanning DIR_INDEX items for a directory inode.
    fn list_dir_entries(
        &mut self,
        tree_root: u64,
        dir_inode: u64,
    ) -> Result<Vec<BtrfsDirEntry>, FilesystemError> {
        let min_key = BtrfsKey {
            objectid: dir_inode,
            key_type: DIR_INDEX_KEY,
            offset: 0,
        };
        let max_key = BtrfsKey {
            objectid: dir_inode,
            key_type: DIR_INDEX_KEY,
            offset: u64::MAX,
        };

        let items = self.find_items_in_range(tree_root, &min_key, &max_key)?;
        let mut entries = Vec::new();

        for (_key, data) in &items {
            if let Some(de) = parse_dir_item(data) {
                // Skip . and ..
                if de.name != "." && de.name != ".." {
                    entries.push(de);
                }
            }
        }

        Ok(entries)
    }

    // ---- File data reading ----

    /// Read file data by collecting EXTENT_DATA items.
    fn read_file_data(
        &mut self,
        tree_root: u64,
        inode_num: u64,
        file_size: u64,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let min_key = BtrfsKey {
            objectid: inode_num,
            key_type: EXTENT_DATA_KEY,
            offset: 0,
        };
        let max_key = BtrfsKey {
            objectid: inode_num,
            key_type: EXTENT_DATA_KEY,
            offset: u64::MAX,
        };

        let items = self.find_items_in_range(tree_root, &min_key, &max_key)?;
        let limit = (file_size as usize).min(max_bytes);
        let mut result = vec![0u8; limit];

        for (key, data) in &items {
            if data.len() < 21 {
                continue;
            }

            let file_offset = key.offset as usize;
            if file_offset >= limit {
                break;
            }

            // generation(8) + ram_bytes(8) + compression(1) + encryption(1) + other_encoding(2) + type(1)
            let compression = data[16];
            let extent_type = data[20];

            match extent_type {
                BTRFS_FILE_EXTENT_INLINE => {
                    if compression != 0 {
                        // Can't decompress — return what we have so far
                        continue;
                    }
                    // Inline data starts at offset 21
                    let inline_data = &data[21..];
                    let copy_len = inline_data.len().min(limit - file_offset);
                    result[file_offset..file_offset + copy_len]
                        .copy_from_slice(&inline_data[..copy_len]);
                }
                BTRFS_FILE_EXTENT_REG | BTRFS_FILE_EXTENT_PREALLOC => {
                    if data.len() < 53 {
                        continue;
                    }
                    if compression != 0 {
                        continue;
                    }
                    // disk_bytenr(8) + disk_num_bytes(8) + offset(8) + num_bytes(8)
                    let disk_bytenr = le64(data, 21);
                    let _disk_num_bytes = le64(data, 29);
                    let extent_offset = le64(data, 37);
                    let num_bytes = le64(data, 45);

                    if disk_bytenr == 0 {
                        // Sparse extent (hole) — already zeros
                        continue;
                    }

                    let read_logical = disk_bytenr + extent_offset;
                    let read_len = (num_bytes as usize).min(limit - file_offset);

                    let physical = self.logical_to_physical(read_logical)?;
                    self.reader
                        .seek(SeekFrom::Start(self.partition_offset + physical))?;

                    let actual_read = read_len.min(result.len() - file_offset);
                    self.reader
                        .read_exact(&mut result[file_offset..file_offset + actual_read])?;
                }
                _ => {}
            }
        }

        result.truncate(limit);
        Ok(result)
    }

    /// Read symlink target — stored as inline extent data.
    fn read_symlink_target(
        &mut self,
        tree_root: u64,
        inode_num: u64,
        size: u64,
    ) -> Result<String, FilesystemError> {
        let data = self.read_file_data(tree_root, inode_num, size, size as usize)?;
        let len = data.iter().position(|&b| b == 0).unwrap_or(data.len());
        Ok(String::from_utf8_lossy(&data[..len]).to_string())
    }

    /// Build a FileEntry for a directory child, reading its inode from the appropriate tree.
    fn dir_entry_to_file_entry(
        &mut self,
        de: &BtrfsDirEntry,
        parent_path: &str,
        parent_tree_root: u64,
    ) -> Result<FileEntry, FilesystemError> {
        let path = if parent_path == "/" {
            format!("/{}", de.name)
        } else {
            format!("{}/{}", parent_path, de.name)
        };

        // If the location key type is ROOT_ITEM_KEY, this is a subvolume
        let (tree_root, inode_num) = if de.location_type == ROOT_ITEM_KEY {
            // Look up the subvolume's ROOT_ITEM to get its tree root
            let subvol_key = BtrfsKey {
                objectid: de.location_objectid,
                key_type: ROOT_ITEM_KEY,
                offset: 0,
            };
            match self.find_item_ge(self.root_tree_root, &subvol_key)? {
                Some((key, data))
                    if key.objectid == de.location_objectid
                        && key.key_type == ROOT_ITEM_KEY
                        && data.len() >= 184 =>
                {
                    let subvol_tree_root = le64(&data, 176);
                    (subvol_tree_root, BTRFS_FIRST_FREE_OBJECTID)
                }
                _ => {
                    return Err(FilesystemError::NotFound(format!(
                        "btrfs: subvolume {} ROOT_ITEM not found",
                        de.location_objectid
                    )));
                }
            }
        } else {
            (parent_tree_root, de.location_objectid)
        };

        let inode = self.read_inode_item(tree_root, inode_num)?;
        let mut fe = unix_entry_from_inode(
            &de.name,
            &path,
            inode.mode,
            inode.size,
            inode_num,
            inode.mtime,
            inode.uid,
            inode.gid,
        );

        // For subvolumes, force directory type even if inode detection fails
        if de.location_type == ROOT_ITEM_KEY {
            fe.entry_type = super::entry::EntryType::Directory;
            // Encode the subvolume tree root in the location for later traversal
            fe.location = de.location_objectid | (1u64 << 63);
        }

        // For symlinks, read the target
        if fe.entry_type == super::entry::EntryType::Symlink {
            match self.read_symlink_target(tree_root, inode_num, inode.size) {
                Ok(target) => fe.symlink_target = Some(target),
                Err(_) => fe.symlink_target = Some("(unreadable)".to_string()),
            }
        }

        Ok(fe)
    }

    /// Resolve a FileEntry location to (tree_root, inode_num).
    /// Subvolumes are encoded with bit 63 set and the objectid as the value.
    fn resolve_location(&mut self, entry: &FileEntry) -> Result<(u64, u64), FilesystemError> {
        if entry.location & (1u64 << 63) != 0 {
            // Subvolume — look up the ROOT_ITEM
            let subvol_id = entry.location & !(1u64 << 63);
            let subvol_key = BtrfsKey {
                objectid: subvol_id,
                key_type: ROOT_ITEM_KEY,
                offset: 0,
            };
            match self.find_item_ge(self.root_tree_root, &subvol_key)? {
                Some((key, data))
                    if key.objectid == subvol_id
                        && key.key_type == ROOT_ITEM_KEY
                        && data.len() >= 184 =>
                {
                    let tree_root = le64(&data, 176);
                    Ok((tree_root, BTRFS_FIRST_FREE_OBJECTID))
                }
                _ => Err(FilesystemError::NotFound(format!(
                    "btrfs: subvolume {subvol_id} not found"
                ))),
            }
        } else {
            Ok((self.fs_tree_root, entry.location))
        }
    }
}

// ---- Filesystem trait implementation ----

impl<R: Read + Seek + Send> Filesystem for BtrfsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        let inode = self.read_inode_item(self.fs_tree_root, BTRFS_FIRST_FREE_OBJECTID)?;
        Ok(unix_entry_from_inode(
            "/",
            "/",
            inode.mode,
            0,
            BTRFS_FIRST_FREE_OBJECTID,
            inode.mtime,
            inode.uid,
            inode.gid,
        ))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if !entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }

        let (tree_root, inode_num) = self.resolve_location(entry)?;
        let dir_entries = self.list_dir_entries(tree_root, inode_num)?;

        let mut entries = Vec::with_capacity(dir_entries.len());
        for de in &dir_entries {
            match self.dir_entry_to_file_entry(de, &entry.path, tree_root) {
                Ok(fe) => entries.push(fe),
                Err(_) => continue,
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

        let (tree_root, inode_num) = self.resolve_location(entry)?;
        let inode = self.read_inode_item(tree_root, inode_num)?;
        self.read_file_data(tree_root, inode_num, inode.size, max_bytes)
    }

    fn volume_label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    fn fs_type(&self) -> &str {
        "btrfs"
    }

    fn total_size(&self) -> u64 {
        self.total_bytes
    }

    fn used_size(&self) -> u64 {
        self.bytes_used
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        // btrfs doesn't have a simple "last used block" concept;
        // return total size since the allocator can place data anywhere.
        Ok(self.total_bytes)
    }
}

// ---- Compaction ----

/// Compact reader for btrfs partitions. Zeros out physical space that is not
/// part of any allocated chunk, allowing downstream compression (CHD/zstd)
/// to achieve much better ratios.
pub struct CompactBtrfsReader<R: Read + Seek> {
    inner: CompactStreamReader<R>,
}

impl<R: Read + Seek + Send> CompactBtrfsReader<R> {
    /// Create a new compacted btrfs reader.
    ///
    /// Parses the superblock and chunk tree to identify allocated physical ranges.
    /// Gaps between chunks become zero-filled sections; chunk ranges are mapped
    /// through as-is from the source.
    pub fn new(
        mut reader: R,
        partition_offset: u64,
    ) -> Result<(Self, CompactResult), FilesystemError> {
        let sb_info = parse_btrfs_superblock(&mut reader, partition_offset)?;

        // Build a temporary filesystem just to read the full chunk tree
        let mut temp = BtrfsFilesystem {
            reader,
            partition_offset,
            node_size: sb_info.node_size,
            total_bytes: sb_info.total_bytes,
            bytes_used: sb_info.bytes_used,
            label: sb_info.label,
            chunk_map: sb_info.chunk_map,
            root_tree_root: sb_info.root_tree_root,
            fs_tree_root: 0,
        };
        temp.read_chunk_tree(sb_info.chunk_root)?;

        let total_bytes = temp.total_bytes;
        let _bytes_used = temp.bytes_used;
        let chunk_map = temp.chunk_map;
        let sector_size = sb_info.sector_size.max(512) as u64;
        let reader = temp.reader;

        // Collect physical ranges from chunks and sort by physical offset
        let mut phys_ranges: Vec<(u64, u64)> = chunk_map
            .iter()
            .map(|c| (c.physical, c.physical + c.length))
            .collect();
        phys_ranges.sort_by_key(|&(start, _)| start);

        // Merge overlapping ranges
        let mut merged: Vec<(u64, u64)> = Vec::new();
        for (start, end) in phys_ranges {
            if let Some(last) = merged.last_mut() {
                if start <= last.1 {
                    last.1 = last.1.max(end);
                    continue;
                }
            }
            merged.push((start, end));
        }

        // Also include the superblock region (0x10000..0x11000) as allocated —
        // it must be preserved for the filesystem to be valid.
        // Insert it into the merged ranges.
        let sb_start = SUPERBLOCK_OFFSET;
        let sb_end = SUPERBLOCK_OFFSET + 4096;
        merged.push((sb_start, sb_end));
        merged.sort_by_key(|&(s, _)| s);

        // Re-merge after adding superblock
        let mut final_ranges: Vec<(u64, u64)> = Vec::new();
        for (start, end) in merged {
            if let Some(last) = final_ranges.last_mut() {
                if start <= last.1 {
                    last.1 = last.1.max(end);
                    continue;
                }
            }
            final_ranges.push((start, end));
        }

        // Build CompactLayout sections
        let mut sections: Vec<CompactSection> = Vec::new();
        let mut pos: u64 = 0;
        let mut total_allocated_sectors: u64 = 0;

        for &(range_start, range_end) in &final_ranges {
            // Clamp to total_bytes
            let range_start = range_start.min(total_bytes);
            let range_end = range_end.min(total_bytes);
            if range_start >= range_end {
                continue;
            }

            // Gap before this range → zeros
            if pos < range_start {
                sections.push(CompactSection::Zeros(range_start - pos));
            }

            // Allocated range → MappedBlocks
            let aligned_start = range_start / sector_size * sector_size;
            let aligned_end =
                ((range_end + sector_size - 1) / sector_size * sector_size).min(total_bytes);
            let num_sectors = (aligned_end - aligned_start) / sector_size;
            let old_blocks: Vec<u64> = (0..num_sectors)
                .map(|i| aligned_start / sector_size + i)
                .collect();
            total_allocated_sectors += num_sectors;
            sections.push(CompactSection::MappedBlocks { old_blocks });

            pos = aligned_end;
        }

        // Trailing gap after the last range
        if pos < total_bytes {
            sections.push(CompactSection::Zeros(total_bytes - pos));
        }

        let layout = CompactLayout {
            sections,
            block_size: sector_size as usize,
            source_data_start: 0,
            source_partition_offset: partition_offset,
        };

        let inner = CompactStreamReader::new(reader, layout);

        Ok((
            Self { inner },
            CompactResult {
                original_size: total_bytes,
                compacted_size: total_bytes,
                clusters_used: total_allocated_sectors as u32,
            },
        ))
    }
}

impl<R: Read + Seek> Read for CompactBtrfsReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

// ---- Parsing helpers ----

/// Parse an INODE_ITEM (160 bytes).
fn parse_inode_item(data: &[u8]) -> Result<InodeData, FilesystemError> {
    if data.len() < 160 {
        return Err(FilesystemError::Parse(format!(
            "btrfs: INODE_ITEM too short ({} bytes, need 160)",
            data.len()
        )));
    }
    // btrfs_inode_item layout:
    // 0: generation(8), 8: transid(8), 16: size(8), 24: nbytes(8),
    // 32: block_group(8), 40: nlink(4), 44: uid(4), 48: gid(4),
    // 52: mode(4), 56: rdev(8), 64: flags(8),
    // 72: sequence(8), 80: reserved[4](32), 112: atime(12), 124: ctime(12),
    // 136: mtime(12), 148: otime(12)
    let size = le64(data, 16);
    let uid = le32(data, 44);
    let gid = le32(data, 48);
    let mode = le32(data, 52);
    // mtime is a btrfs_timespec at offset 136: sec(8) + nsec(4)
    let mtime_sec = le64(data, 136) as i64;

    Ok(InodeData {
        mode,
        uid,
        gid,
        size,
        mtime: mtime_sec,
    })
}

struct InodeData {
    mode: u32,
    uid: u32,
    gid: u32,
    size: u64,
    mtime: i64,
}

/// A parsed directory entry from DIR_INDEX item.
struct BtrfsDirEntry {
    name: String,
    location_objectid: u64,
    location_type: u8,
    #[allow(dead_code)]
    d_type: u8,
}

/// Parse a btrfs_dir_item from item data.
/// Layout: location key(17) + transid(8) + data_len(2) + name_len(2) + type(1) = 30 bytes header, then name.
fn parse_dir_item(data: &[u8]) -> Option<BtrfsDirEntry> {
    if data.len() < 30 {
        return None;
    }
    let location_objectid = le64(data, 0);
    let location_type = data[8];
    let _location_offset = le64(data, 9);
    let _transid = le64(data, 17);
    let _data_len = le16(data, 25);
    let name_len = le16(data, 27) as usize;
    let d_type = data[29];

    if data.len() < 30 + name_len {
        return None;
    }

    let name = String::from_utf8_lossy(&data[30..30 + name_len]).to_string();
    Some(BtrfsDirEntry {
        name,
        location_objectid,
        location_type,
        d_type,
    })
}

/// Parse the sys_chunk_array from the superblock.
/// It's a packed array of (btrfs_disk_key(17) + btrfs_chunk(variable)) entries.
fn parse_sys_chunk_array(data: &[u8]) -> Result<Vec<ChunkMapping>, FilesystemError> {
    let mut chunks = Vec::new();
    let mut off = 0;

    while off + 17 < data.len() {
        // btrfs_disk_key: objectid(8) + type(1) + offset(8) = 17 bytes
        let _objectid = le64(data, off);
        let _key_type = data[off + 8];
        let logical = le64(data, off + 9);
        off += 17;

        // btrfs_chunk: length(8) + owner(8) + stripe_len(8) + type(8) + io_align(4) +
        //              io_width(4) + sector_size(4) + num_stripes(2) + sub_stripes(2) = 48 bytes base
        if off + 48 > data.len() {
            break;
        }
        let length = le64(data, off);
        let num_stripes = le16(data, off + 44) as usize;
        off += 48;

        // Each stripe: devid(8) + offset(8) + dev_uuid(16) = 32 bytes
        if num_stripes == 0 || off + 32 > data.len() {
            break;
        }
        // Use stripe[0]
        let _devid = le64(data, off);
        let physical = le64(data, off + 8);
        off += num_stripes * 32;

        chunks.push(ChunkMapping {
            logical,
            length,
            physical,
        });
    }

    Ok(chunks)
}

// ---- Unit Tests ----

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // ---- Test image builder ----

    /// Build a minimal btrfs test image with:
    /// - Superblock at 0x10000
    /// - One chunk mapping: logical 0x100000 → physical 0x100000, length 0x100000
    /// - Root tree leaf at logical 0x100000 with FS_TREE ROOT_ITEM
    /// - FS tree leaf at logical 0x101000 with root dir inode + DIR_INDEX entries + child inodes
    fn build_test_image() -> Vec<u8> {
        let node_size: u32 = 4096;
        // Image: 2 MiB (enough for superblock + nodes)
        let image_size = 2 * 1024 * 1024;
        let mut img = vec![0u8; image_size];

        let chunk_logical: u64 = 0x100000;
        let chunk_physical: u64 = 0x100000;
        let chunk_length: u64 = 0x100000;
        let root_tree_logical: u64 = chunk_logical;
        let fs_tree_logical: u64 = chunk_logical + node_size as u64;

        // ---- Superblock at 0x10000 ----
        let sb_off = SUPERBLOCK_OFFSET as usize;
        // magic at 0x40
        img[sb_off + 0x40..sb_off + 0x48].copy_from_slice(BTRFS_MAGIC);
        // generation at 0x48
        write_le64(&mut img, sb_off + 0x48, 1);
        // root at 0x50
        write_le64(&mut img, sb_off + 0x50, root_tree_logical);
        // chunk_root at 0x58 — point to a trivial empty node (we'll use sys_chunk_array only)
        write_le64(&mut img, sb_off + 0x58, root_tree_logical); // reuse root tree as chunk tree (it'll just find no CHUNK_ITEMs)
                                                                // total_bytes at 0x70
        write_le64(&mut img, sb_off + 0x70, image_size as u64);
        // bytes_used at 0x78
        write_le64(&mut img, sb_off + 0x78, 32768);
        // sectorsize at 0x90
        write_le32(&mut img, sb_off + 0x90, 4096);
        // nodesize at 0x94
        write_le32(&mut img, sb_off + 0x94, node_size);
        // sys_chunk_array_size at 0xA0
        let sys_chunk = build_sys_chunk_entry(chunk_logical, chunk_length, chunk_physical);
        write_le32(&mut img, sb_off + 0xA0, sys_chunk.len() as u32);
        // root_level at 0xC6
        img[sb_off + 0xC6] = 0; // leaf
                                // chunk_root_level at 0xC7
        img[sb_off + 0xC7] = 0; // leaf
                                // label at 0x12B
        let label = b"TestVol";
        img[sb_off + 0x12B..sb_off + 0x12B + label.len()].copy_from_slice(label);
        // sys_chunk_array at 0x32B
        img[sb_off + 0x32B..sb_off + 0x32B + sys_chunk.len()].copy_from_slice(&sys_chunk);

        // ---- Root tree leaf at chunk_physical ----
        // This leaf contains the FS_TREE ROOT_ITEM (objectid=5, type=132, offset=0)
        let root_tree_off = chunk_physical as usize;
        let root_item_data = build_root_item(fs_tree_logical);
        build_leaf_node(
            &mut img,
            root_tree_off,
            node_size,
            &[(
                BtrfsKey {
                    objectid: BTRFS_FS_TREE_OBJECTID,
                    key_type: ROOT_ITEM_KEY,
                    offset: 0,
                },
                &root_item_data,
            )],
        );

        // ---- FS tree leaf at chunk_physical + node_size ----
        let fs_tree_off = (chunk_physical + node_size as u64) as usize;

        // Build items for the FS tree:
        // 1. INODE_ITEM for inode 256 (root dir)
        let root_dir_inode = build_inode_item(0o040755, 0, 0, 0, 1700000000);
        // 2. DIR_INDEX for "hello.txt" pointing to inode 257
        let dir_index_hello = build_dir_item(257, INODE_ITEM_KEY, BTRFS_FT_REG_FILE, "hello.txt");
        // 3. DIR_INDEX for "subdir" pointing to inode 258
        let dir_index_subdir = build_dir_item(258, INODE_ITEM_KEY, BTRFS_FT_DIR, "subdir");
        // 4. DIR_INDEX for "link" pointing to inode 259
        let dir_index_link = build_dir_item(259, INODE_ITEM_KEY, BTRFS_FT_SYMLINK, "link");
        // 5. INODE_ITEM for inode 257 (hello.txt, 13 bytes)
        let hello_inode = build_inode_item(0o100644, 13, 1000, 1000, 1700000000);
        // 6. EXTENT_DATA (inline) for inode 257
        let hello_extent = build_inline_extent(b"Hello, btrfs!");
        // 7. INODE_ITEM for inode 258 (subdir)
        let subdir_inode = build_inode_item(0o040755, 0, 0, 0, 1700000000);
        // 8. INODE_ITEM for inode 259 (link, symlink to hello.txt)
        let link_inode = build_inode_item(0o120777, 9, 0, 0, 1700000000);
        // 9. EXTENT_DATA (inline) for inode 259 — symlink target "hello.txt"
        let link_extent = build_inline_extent(b"hello.txt");

        let items: Vec<(BtrfsKey, &[u8])> = vec![
            (
                BtrfsKey {
                    objectid: 256,
                    key_type: INODE_ITEM_KEY,
                    offset: 0,
                },
                &root_dir_inode,
            ),
            (
                BtrfsKey {
                    objectid: 256,
                    key_type: DIR_INDEX_KEY,
                    offset: 2,
                },
                &dir_index_hello,
            ),
            (
                BtrfsKey {
                    objectid: 256,
                    key_type: DIR_INDEX_KEY,
                    offset: 3,
                },
                &dir_index_subdir,
            ),
            (
                BtrfsKey {
                    objectid: 256,
                    key_type: DIR_INDEX_KEY,
                    offset: 4,
                },
                &dir_index_link,
            ),
            (
                BtrfsKey {
                    objectid: 257,
                    key_type: INODE_ITEM_KEY,
                    offset: 0,
                },
                &hello_inode,
            ),
            (
                BtrfsKey {
                    objectid: 257,
                    key_type: EXTENT_DATA_KEY,
                    offset: 0,
                },
                &hello_extent,
            ),
            (
                BtrfsKey {
                    objectid: 258,
                    key_type: INODE_ITEM_KEY,
                    offset: 0,
                },
                &subdir_inode,
            ),
            (
                BtrfsKey {
                    objectid: 259,
                    key_type: INODE_ITEM_KEY,
                    offset: 0,
                },
                &link_inode,
            ),
            (
                BtrfsKey {
                    objectid: 259,
                    key_type: EXTENT_DATA_KEY,
                    offset: 0,
                },
                &link_extent,
            ),
        ];

        build_leaf_node(&mut img, fs_tree_off, node_size, &items);

        img
    }

    /// Build a sys_chunk_array entry (key + chunk).
    fn build_sys_chunk_entry(logical: u64, length: u64, physical: u64) -> Vec<u8> {
        let mut buf = Vec::new();
        // btrfs_disk_key: objectid=256 (FIRST_CHUNK_TREE), type=CHUNK_ITEM, offset=logical
        buf.extend_from_slice(&256u64.to_le_bytes()); // objectid
        buf.push(CHUNK_ITEM_KEY); // type
        buf.extend_from_slice(&logical.to_le_bytes()); // offset

        // btrfs_chunk (48 bytes base + 32 bytes per stripe)
        buf.extend_from_slice(&length.to_le_bytes()); // length
        buf.extend_from_slice(&256u64.to_le_bytes()); // owner (FIRST_CHUNK_TREE)
        buf.extend_from_slice(&65536u64.to_le_bytes()); // stripe_len
        buf.extend_from_slice(&(1u64 << 2).to_le_bytes()); // type (DATA)
        buf.extend_from_slice(&4096u32.to_le_bytes()); // io_align
        buf.extend_from_slice(&4096u32.to_le_bytes()); // io_width
        buf.extend_from_slice(&4096u32.to_le_bytes()); // sector_size
        buf.extend_from_slice(&1u16.to_le_bytes()); // num_stripes
        buf.extend_from_slice(&0u16.to_le_bytes()); // sub_stripes

        // stripe[0]
        buf.extend_from_slice(&1u64.to_le_bytes()); // devid
        buf.extend_from_slice(&physical.to_le_bytes()); // offset
        buf.extend_from_slice(&[0u8; 16]); // dev_uuid

        buf
    }

    /// Build a ROOT_ITEM (at least 184 bytes) with the tree root bytenr at offset 176.
    fn build_root_item(tree_root: u64) -> Vec<u8> {
        let mut data = vec![0u8; 439]; // standard ROOT_ITEM size
                                       // bytenr at offset 176
        write_le64(&mut data, 176, tree_root);
        data
    }

    /// Build an INODE_ITEM (160 bytes).
    fn build_inode_item(mode: u32, size: u64, uid: u32, gid: u32, mtime_sec: u64) -> Vec<u8> {
        let mut data = vec![0u8; 160];
        write_le64(&mut data, 16, size);
        write_le32(&mut data, 44, uid);
        write_le32(&mut data, 48, gid);
        write_le32(&mut data, 52, mode);
        write_le64(&mut data, 136, mtime_sec);
        data
    }

    /// Build a DIR_INDEX/DIR_ITEM entry.
    fn build_dir_item(objectid: u64, loc_type: u8, d_type: u8, name: &str) -> Vec<u8> {
        let name_bytes = name.as_bytes();
        let mut data = vec![0u8; 30 + name_bytes.len()];
        // location key
        write_le64(&mut data, 0, objectid);
        data[8] = loc_type;
        write_le64(&mut data, 9, 0); // offset
                                     // transid
        write_le64(&mut data, 17, 1);
        // data_len
        write_le16(&mut data, 25, 0);
        // name_len
        write_le16(&mut data, 27, name_bytes.len() as u16);
        // type
        data[29] = d_type;
        // name
        data[30..30 + name_bytes.len()].copy_from_slice(name_bytes);
        data
    }

    /// Build an inline EXTENT_DATA item.
    fn build_inline_extent(content: &[u8]) -> Vec<u8> {
        // header: generation(8) + ram_bytes(8) + compression(1) + encryption(1) + other_encoding(2) + type(1) = 21 bytes
        let mut data = vec![0u8; 21 + content.len()];
        write_le64(&mut data, 0, 1); // generation
        write_le64(&mut data, 8, content.len() as u64); // ram_bytes
        data[16] = 0; // no compression
        data[17] = 0; // no encryption
        write_le16(&mut data, 18, 0); // other_encoding
        data[20] = BTRFS_FILE_EXTENT_INLINE; // type
        data[21..21 + content.len()].copy_from_slice(content);
        data
    }

    /// Build a leaf node with the given items.
    fn build_leaf_node(img: &mut [u8], offset: usize, node_size: u32, items: &[(BtrfsKey, &[u8])]) {
        // Node header (101 bytes)
        // nritems at NODE_HEADER_SIZE - 5 (byte 96)
        write_le32(img, offset + NODE_HEADER_SIZE - 5, items.len() as u32);
        // level at NODE_HEADER_SIZE - 1 (byte 100)
        img[offset + NODE_HEADER_SIZE - 1] = 0; // leaf

        // Item headers start at NODE_HEADER_SIZE
        // Item data is packed from the end of the node towards the start
        let mut data_end = node_size as usize; // next free byte (from end)
        for (i, (key, data)) in items.iter().enumerate() {
            let item_hdr_off = offset + NODE_HEADER_SIZE + i * 25;
            // Write key (17 bytes)
            write_le64(img, item_hdr_off, key.objectid);
            img[item_hdr_off + 8] = key.key_type;
            write_le64(img, item_hdr_off + 9, key.offset);

            // Data offset is relative to the start of the node data area
            // (i.e., past the node header). But the btrfs on-disk format stores
            // the offset relative to the end of the header.
            // Actually, btrfs item.offset is the offset from the start of the leaf
            // where the data begins, MINUS the header size. Let me re-check.
            //
            // In btrfs, btrfs_item.offset = byte offset of the item data
            // measured from the start of the leaf data area (after items array).
            // But our code uses: abs_offset = data_offset + NODE_HEADER_SIZE
            // So data_offset = abs_offset - NODE_HEADER_SIZE

            data_end -= data.len();
            let data_offset = data_end - NODE_HEADER_SIZE;
            write_le32(img, item_hdr_off + 17, data_offset as u32);
            write_le32(img, item_hdr_off + 21, data.len() as u32);

            // Write item data
            img[offset + data_end..offset + data_end + data.len()].copy_from_slice(data);
        }
    }

    fn write_le64(buf: &mut [u8], off: usize, val: u64) {
        buf[off..off + 8].copy_from_slice(&val.to_le_bytes());
    }

    fn write_le32(buf: &mut [u8], off: usize, val: u32) {
        buf[off..off + 4].copy_from_slice(&val.to_le_bytes());
    }

    fn write_le16(buf: &mut [u8], off: usize, val: u16) {
        buf[off..off + 2].copy_from_slice(&val.to_le_bytes());
    }

    // ---- Tests ----

    #[test]
    fn test_superblock_parsing() {
        let img = build_test_image();
        let cursor = Cursor::new(img);
        let fs = BtrfsFilesystem::open(cursor, 0).unwrap();
        assert_eq!(fs.fs_type(), "btrfs");
        assert_eq!(fs.volume_label(), Some("TestVol"));
        assert_eq!(fs.total_size(), 2 * 1024 * 1024);
        assert_eq!(fs.used_size(), 32768);
        assert_eq!(fs.node_size, 4096);
    }

    #[test]
    fn test_invalid_magic() {
        let mut img = build_test_image();
        // Corrupt magic
        img[SUPERBLOCK_OFFSET as usize + 0x40] = 0xFF;
        let cursor = Cursor::new(img);
        let result = BtrfsFilesystem::open(cursor, 0);
        assert!(result.is_err());
        let err = result.err().expect("expected error").to_string();
        assert!(err.contains("invalid superblock magic"), "got: {err}");
    }

    #[test]
    fn test_root_directory() {
        let img = build_test_image();
        let cursor = Cursor::new(img);
        let mut fs = BtrfsFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        assert!(root.is_directory());
        assert_eq!(root.name, "/");
        assert_eq!(root.mode, Some(0o040755));
    }

    #[test]
    fn test_list_root_directory() {
        let img = build_test_image();
        let cursor = Cursor::new(img);
        let mut fs = BtrfsFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();

        assert_eq!(entries.len(), 3);
        // Sorted: directories first, then alphabetically
        assert_eq!(entries[0].name, "subdir");
        assert!(entries[0].is_directory());
        assert_eq!(entries[1].name, "hello.txt");
        assert!(entries[1].is_file());
        assert_eq!(entries[1].size, 13);
        assert_eq!(entries[2].name, "link");
        assert!(entries[2].is_symlink());
    }

    #[test]
    fn test_read_inline_file() {
        let img = build_test_image();
        let cursor = Cursor::new(img);
        let mut fs = BtrfsFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let hello = entries.iter().find(|e| e.name == "hello.txt").unwrap();

        let data = fs.read_file(hello, 1024).unwrap();
        assert_eq!(&data, b"Hello, btrfs!");
    }

    #[test]
    fn test_symlink_target() {
        let img = build_test_image();
        let cursor = Cursor::new(img);
        let mut fs = BtrfsFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let link = entries.iter().find(|e| e.name == "link").unwrap();

        assert!(link.is_symlink());
        assert_eq!(link.symlink_target.as_deref(), Some("hello.txt"));
    }

    #[test]
    fn test_partition_offset() {
        // Build an image with a partition offset
        let offset = 1048576u64; // 1 MiB offset
        let inner = build_test_image();
        let mut img = vec![0u8; offset as usize + inner.len()];
        img[offset as usize..].copy_from_slice(&inner);

        let cursor = Cursor::new(img);
        let mut fs = BtrfsFilesystem::open(cursor, offset).unwrap();

        let root = fs.root().unwrap();
        assert!(root.is_directory());
        let entries = fs.list_directory(&root).unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_file_permissions() {
        let img = build_test_image();
        let cursor = Cursor::new(img);
        let mut fs = BtrfsFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let hello = entries.iter().find(|e| e.name == "hello.txt").unwrap();

        assert_eq!(hello.uid, Some(1000));
        assert_eq!(hello.gid, Some(1000));
        assert_eq!(hello.mode, Some(0o100644));
    }

    #[test]
    fn test_last_data_byte() {
        let img = build_test_image();
        let cursor = Cursor::new(img);
        let mut fs = BtrfsFilesystem::open(cursor, 0).unwrap();
        // last_data_byte returns total_bytes for btrfs
        assert_eq!(fs.last_data_byte().unwrap(), 2 * 1024 * 1024);
    }

    #[test]
    fn test_compact_btrfs_reader() {
        let img = build_test_image();
        let image_size = img.len();
        let cursor = Cursor::new(img.clone());

        let (mut compact, info) = CompactBtrfsReader::new(cursor, 0).unwrap();
        assert_eq!(info.original_size, image_size as u64);
        assert_eq!(info.compacted_size, image_size as u64);
        // clusters_used should be > 0 (at least superblock + chunk)
        assert!(info.clusters_used > 0);

        // Read the full compacted output
        let mut output = Vec::new();
        std::io::Read::read_to_end(&mut compact, &mut output).unwrap();
        assert_eq!(output.len(), image_size);

        // Superblock region must be preserved
        let sb_off = SUPERBLOCK_OFFSET as usize;
        assert_eq!(&output[sb_off + 0x40..sb_off + 0x48], BTRFS_MAGIC);

        // Allocated chunk region (0x100000..0x200000) must be preserved
        assert_eq!(
            &output[0x100000..0x100000 + 8],
            &img[0x100000..0x100000 + 8],
            "chunk data should be preserved"
        );

        // Unallocated region (e.g. 0..0x10000) should be zeros
        assert!(
            output[..0x10000].iter().all(|&b| b == 0),
            "unallocated space before superblock should be zeroed"
        );
    }

    #[test]
    fn test_compact_btrfs_preserves_data() {
        let img = build_test_image();
        let cursor = Cursor::new(img.clone());

        let (mut compact, _info) = CompactBtrfsReader::new(cursor, 0).unwrap();
        let mut output = Vec::new();
        std::io::Read::read_to_end(&mut compact, &mut output).unwrap();

        // The entire chunk region should be byte-identical to the original
        let chunk_start = 0x100000usize;
        let chunk_end = 0x200000usize;
        assert_eq!(
            &output[chunk_start..chunk_end],
            &img[chunk_start..chunk_end],
            "allocated chunk data must be byte-identical to original"
        );

        // Superblock (4096 bytes at 0x10000) should also be preserved
        let sb_start = SUPERBLOCK_OFFSET as usize;
        let sb_end = sb_start + 4096;
        assert_eq!(
            &output[sb_start..sb_end],
            &img[sb_start..sb_end],
            "superblock must be byte-identical to original"
        );
    }
}
