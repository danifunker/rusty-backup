use byteorder::{BigEndian, ByteOrder};
use std::cmp::Ordering;
use std::io::{Read, Seek, SeekFrom, Write};
use unicode_normalization::UnicodeNormalization;

use super::entry::{EntryType, FileEntry};
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};
use super::hfs_common::{
    self, bitmap_clear_bit_be, bitmap_find_clear_run_be, bitmap_set_bit_be, btree_free_node,
    btree_grow_root, btree_insert_into_index, btree_insert_record, btree_record_range,
    btree_remove_record, btree_split_leaf_with_insert, BTreeHeader,
};
use super::CompactResult;

const HFS_PLUS_SIGNATURE: u16 = 0x482B;
const HFSX_SIGNATURE: u16 = 0x4858;

/// HFS+ extent descriptor: start_block (u32) + block_count (u32).
#[derive(Debug, Clone, Copy)]
struct ExtentDescriptor {
    start_block: u32,
    block_count: u32,
}

impl ExtentDescriptor {
    fn parse(data: &[u8]) -> Self {
        ExtentDescriptor {
            start_block: BigEndian::read_u32(&data[0..4]),
            block_count: BigEndian::read_u32(&data[4..8]),
        }
    }

    fn is_empty(&self) -> bool {
        self.block_count == 0
    }

    fn serialize(&self, out: &mut [u8]) {
        BigEndian::write_u32(&mut out[0..4], self.start_block);
        BigEndian::write_u32(&mut out[4..8], self.block_count);
    }
}

/// HFS+ fork data (80 bytes).
#[derive(Debug, Clone)]
struct ForkData {
    logical_size: u64,
    // Preserved for VH round-trip; HFS+ on-disk fork data has a clump_size
    // slot at bytes [8..12]. We never tune it, but parsing/emitting the
    // named field keeps the serializer symmetric with the parser.
    #[allow(dead_code)]
    clump_size: u32,
    total_blocks: u32,
    extents: [ExtentDescriptor; 8],
}

impl ForkData {
    fn parse(data: &[u8]) -> Self {
        let mut extents = [ExtentDescriptor {
            start_block: 0,
            block_count: 0,
        }; 8];
        for i in 0..8 {
            extents[i] = ExtentDescriptor::parse(&data[16 + i * 8..24 + i * 8]);
        }
        ForkData {
            logical_size: BigEndian::read_u64(&data[0..8]),
            clump_size: BigEndian::read_u32(&data[8..12]),
            total_blocks: BigEndian::read_u32(&data[12..16]),
            extents,
        }
    }

    fn serialize(&self, out: &mut [u8]) {
        BigEndian::write_u64(&mut out[0..8], self.logical_size);
        BigEndian::write_u32(&mut out[8..12], self.clump_size);
        BigEndian::write_u32(&mut out[12..16], self.total_blocks);
        for i in 0..8 {
            self.extents[i].serialize(&mut out[16 + i * 8..24 + i * 8]);
        }
    }

    fn empty() -> Self {
        ForkData {
            logical_size: 0,
            clump_size: 0,
            total_blocks: 0,
            extents: [ExtentDescriptor {
                start_block: 0,
                block_count: 0,
            }; 8],
        }
    }
}

/// HFS+ Volume Header (512 bytes at partition_offset + 1024).
#[derive(Debug)]
#[allow(dead_code)]
struct HfsPlusVolumeHeader {
    signature: u16,
    version: u16,
    attributes: u32,
    last_mounted_version: u32,
    journal_info_block: u32,
    create_date: u32,
    modify_date: u32,
    backup_date: u32,
    checked_date: u32,
    file_count: u32,
    folder_count: u32,
    block_size: u32,
    total_blocks: u32,
    free_blocks: u32,
    next_allocation: u32,
    rsrc_clump_size: u32,
    data_clump_size: u32,
    next_catalog_id: u32,
    write_count: u32,
    encodings_bitmap: u64,
    finder_info: [u32; 8],
    allocation_file: ForkData,
    extents_file: ForkData,
    catalog_file: ForkData,
    attributes_file: ForkData,
    startup_file: ForkData,
}

impl HfsPlusVolumeHeader {
    fn parse(data: &[u8]) -> Result<Self, FilesystemError> {
        if data.len() < 512 {
            return Err(FilesystemError::Parse("volume header too short".into()));
        }
        let sig = BigEndian::read_u16(&data[0..2]);
        if sig != HFS_PLUS_SIGNATURE && sig != HFSX_SIGNATURE {
            return Err(FilesystemError::Parse(format!(
                "bad HFS+ volume header signature: 0x{sig:04X}"
            )));
        }

        let mut finder_info = [0u32; 8];
        for i in 0..8 {
            finder_info[i] = BigEndian::read_u32(&data[80 + i * 4..84 + i * 4]);
        }

        Ok(HfsPlusVolumeHeader {
            signature: sig,
            version: BigEndian::read_u16(&data[2..4]),
            attributes: BigEndian::read_u32(&data[4..8]),
            last_mounted_version: BigEndian::read_u32(&data[8..12]),
            journal_info_block: BigEndian::read_u32(&data[12..16]),
            create_date: BigEndian::read_u32(&data[16..20]),
            modify_date: BigEndian::read_u32(&data[20..24]),
            backup_date: BigEndian::read_u32(&data[24..28]),
            checked_date: BigEndian::read_u32(&data[28..32]),
            file_count: BigEndian::read_u32(&data[32..36]),
            folder_count: BigEndian::read_u32(&data[36..40]),
            block_size: BigEndian::read_u32(&data[40..44]),
            total_blocks: BigEndian::read_u32(&data[44..48]),
            free_blocks: BigEndian::read_u32(&data[48..52]),
            next_allocation: BigEndian::read_u32(&data[52..56]),
            rsrc_clump_size: BigEndian::read_u32(&data[56..60]),
            data_clump_size: BigEndian::read_u32(&data[60..64]),
            next_catalog_id: BigEndian::read_u32(&data[64..68]),
            write_count: BigEndian::read_u32(&data[68..72]),
            encodings_bitmap: BigEndian::read_u64(&data[72..80]),
            finder_info,
            allocation_file: ForkData::parse(&data[112..192]),
            extents_file: ForkData::parse(&data[192..272]),
            catalog_file: ForkData::parse(&data[272..352]),
            attributes_file: ForkData::parse(&data[352..432]),
            startup_file: ForkData::parse(&data[432..512]),
        })
    }

    fn serialize(&self) -> [u8; 512] {
        let mut out = [0u8; 512];
        BigEndian::write_u16(&mut out[0..2], self.signature);
        BigEndian::write_u16(&mut out[2..4], self.version);
        BigEndian::write_u32(&mut out[4..8], self.attributes);
        BigEndian::write_u32(&mut out[8..12], self.last_mounted_version);
        BigEndian::write_u32(&mut out[12..16], self.journal_info_block);
        BigEndian::write_u32(&mut out[16..20], self.create_date);
        BigEndian::write_u32(&mut out[20..24], self.modify_date);
        BigEndian::write_u32(&mut out[24..28], self.backup_date);
        BigEndian::write_u32(&mut out[28..32], self.checked_date);
        BigEndian::write_u32(&mut out[32..36], self.file_count);
        BigEndian::write_u32(&mut out[36..40], self.folder_count);
        BigEndian::write_u32(&mut out[40..44], self.block_size);
        BigEndian::write_u32(&mut out[44..48], self.total_blocks);
        BigEndian::write_u32(&mut out[48..52], self.free_blocks);
        BigEndian::write_u32(&mut out[52..56], self.next_allocation);
        BigEndian::write_u32(&mut out[56..60], self.rsrc_clump_size);
        BigEndian::write_u32(&mut out[60..64], self.data_clump_size);
        BigEndian::write_u32(&mut out[64..68], self.next_catalog_id);
        BigEndian::write_u32(&mut out[68..72], self.write_count);
        BigEndian::write_u64(&mut out[72..80], self.encodings_bitmap);
        for i in 0..8 {
            BigEndian::write_u32(&mut out[80 + i * 4..84 + i * 4], self.finder_info[i]);
        }
        self.allocation_file.serialize(&mut out[112..192]);
        self.extents_file.serialize(&mut out[192..272]);
        self.catalog_file.serialize(&mut out[272..352]);
        self.attributes_file.serialize(&mut out[352..432]);
        self.startup_file.serialize(&mut out[432..512]);
        out
    }

    fn is_hfsx(&self) -> bool {
        self.signature == HFSX_SIGNATURE
    }
}

/// B-tree node descriptor (14 bytes).
#[derive(Debug)]
struct BTreeNodeDescriptor {
    next: u32,
    #[allow(dead_code)]
    prev: u32,
    kind: i8,
    #[allow(dead_code)]
    height: u8,
    num_records: u16,
}

impl BTreeNodeDescriptor {
    fn parse(data: &[u8]) -> Self {
        BTreeNodeDescriptor {
            next: BigEndian::read_u32(&data[0..4]),
            prev: BigEndian::read_u32(&data[4..8]),
            kind: data[8] as i8,
            height: data[9],
            num_records: BigEndian::read_u16(&data[10..12]),
        }
    }
}

/// B-tree header record (after the node descriptor in node 0).
#[derive(Debug)]
#[allow(dead_code)]
struct BTreeHeaderRecord {
    depth: u16,
    root_node: u32,
    leaf_records: u32,
    first_leaf_node: u32,
    last_leaf_node: u32,
    node_size: u16,
    max_key_len: u16,
    total_nodes: u32,
    free_nodes: u32,
}

impl BTreeHeaderRecord {
    fn parse(data: &[u8]) -> Self {
        BTreeHeaderRecord {
            depth: BigEndian::read_u16(&data[0..2]),
            root_node: BigEndian::read_u32(&data[2..6]),
            leaf_records: BigEndian::read_u32(&data[6..10]),
            first_leaf_node: BigEndian::read_u32(&data[10..14]),
            last_leaf_node: BigEndian::read_u32(&data[14..18]),
            node_size: BigEndian::read_u16(&data[18..20]),
            max_key_len: BigEndian::read_u16(&data[20..22]),
            total_nodes: BigEndian::read_u32(&data[22..26]),
            free_nodes: BigEndian::read_u32(&data[26..30]),
        }
    }
}

/// Catalog record types.
const CATALOG_FOLDER: i16 = 1;
const CATALOG_FILE: i16 = 2;

/// Validate a name for a new file or directory on an HFS+/HFSX volume.
fn validate_hfsplus_create_name(name: &str) -> Result<(), FilesystemError> {
    if name.is_empty() {
        return Err(FilesystemError::InvalidData(
            "filename is empty — pick a non-blank name".into(),
        ));
    }
    if name.contains(':') {
        return Err(FilesystemError::InvalidData(
            "filename contains ':', which classic Mac OS uses as a path separator — \
             rename the file (try '-' or '_' instead)"
                .into(),
        ));
    }
    let nfd: String = name.nfd().collect();
    let utf16_len = nfd.encode_utf16().count();
    if utf16_len > 255 {
        return Err(FilesystemError::InvalidData(format!(
            "filename is too long ({utf16_len} UTF-16 units); HFS+ allows up to 255 — \
             shorten the name (note: some emoji and rare characters count as 2 units)"
        )));
    }
    Ok(())
}

/// Decode a 4-byte Mac OS type/creator code to a string.
fn decode_fourcc(data: &[u8]) -> String {
    data.iter()
        .map(|&b| {
            if b.is_ascii_graphic() || b == b' ' {
                b as char
            } else {
                '.'
            }
        })
        .collect()
}
#[allow(dead_code)]
const CATALOG_FOLDER_THREAD: i16 = 3;
#[allow(dead_code)]
const CATALOG_FILE_THREAD: i16 = 4;

/// A parsed HFS+ catalog entry.
#[derive(Debug, Clone)]
enum CatalogEntry {
    Folder {
        folder_id: u32,
        name: String,
    },
    File {
        file_id: u32,
        name: String,
        data_size: u64,
        data_fork: ForkData,
        rsrc_size: u64,
        rsrc_fork: ForkData,
        type_code: String,
        creator_code: String,
        /// Finder flags (userInfo.fdFlags) — bit 0x8000 is `kIsAlias`.
        finder_flags: u16,
    },
}

/// HFS+ filesystem implementation.
pub struct HfsPlusFilesystem<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    vh: HfsPlusVolumeHeader,
    /// Cached catalog B-tree file data.
    catalog_data: Vec<u8>,
    /// B-tree header for the catalog file.
    catalog_header: BTreeHeaderRecord,
    /// Volume label (from catalog root folder thread).
    label: String,
    /// Cached allocation bitmap (loaded on first write operation).
    bitmap: Option<Vec<u8>>,
}

impl<R: Read + Seek> HfsPlusFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        // Read volume header at offset + 1024
        reader.seek(SeekFrom::Start(partition_offset + 1024))?;
        let mut vh_buf = [0u8; 512];
        reader.read_exact(&mut vh_buf)?;
        let vh = HfsPlusVolumeHeader::parse(&vh_buf)?;

        if vh.block_size == 0 || !vh.block_size.is_power_of_two() {
            return Err(FilesystemError::Parse(format!(
                "invalid block size: {}",
                vh.block_size
            )));
        }

        // Read the catalog file through its fork extents
        let catalog_data = read_fork(
            &mut reader,
            partition_offset,
            vh.block_size,
            &vh.catalog_file,
        )?;

        // Parse B-tree header from node 0
        if catalog_data.len() < 14 + 106 {
            return Err(FilesystemError::Parse(
                "catalog file too small for B-tree header".into(),
            ));
        }
        let catalog_header = BTreeHeaderRecord::parse(&catalog_data[14..14 + 106]);

        // Try to find the volume label from the root folder thread
        let label = find_volume_label(&catalog_data, &catalog_header);

        Ok(HfsPlusFilesystem {
            reader,
            partition_offset,
            vh,
            catalog_data,
            catalog_header,
            label,
            bitmap: None,
        })
    }

    /// List all children of a given parent CNID.
    fn list_children(&self, parent_cnid: u32) -> Result<Vec<CatalogEntry>, FilesystemError> {
        let node_size = self.catalog_header.node_size as usize;
        if node_size == 0 {
            return Ok(vec![]);
        }

        let mut results = Vec::new();
        let mut node_idx = self.catalog_header.first_leaf_node;

        while node_idx != 0 {
            let offset = node_idx as usize * node_size;
            if offset + node_size > self.catalog_data.len() {
                break;
            }
            let node = &self.catalog_data[offset..offset + node_size];

            let desc = BTreeNodeDescriptor::parse(node);
            // kind -1 = leaf node
            if desc.kind != -1 {
                node_idx = desc.next;
                continue;
            }

            for i in 0..desc.num_records as usize {
                // Record offsets are stored at end of node, growing backward
                let offset_pos = node_size - 2 * (i + 1);
                if offset_pos + 2 > node.len() {
                    break;
                }
                let rec_offset = BigEndian::read_u16(&node[offset_pos..offset_pos + 2]) as usize;
                if rec_offset + 6 > node.len() {
                    continue;
                }

                // Parse catalog key
                let key_len = BigEndian::read_u16(&node[rec_offset..rec_offset + 2]) as usize;
                if key_len < 6 || rec_offset + 2 + key_len > node.len() {
                    continue;
                }
                let key_data = &node[rec_offset + 2..rec_offset + 2 + key_len];

                // Key: parent_id (4) + name_length (2) + name (UTF-16BE)
                let key_parent_id = BigEndian::read_u32(&key_data[0..4]);
                if key_parent_id != parent_cnid {
                    continue;
                }

                let name_length = BigEndian::read_u16(&key_data[4..6]) as usize;
                let name = if name_length > 0 && 6 + name_length * 2 <= key_data.len() {
                    decode_utf16be(&key_data[6..6 + name_length * 2])
                } else {
                    String::new()
                };

                // Record data follows key (aligned to even offset from node start)
                let mut rec_data_start = rec_offset + 2 + key_len;
                if rec_data_start % 2 != 0 {
                    rec_data_start += 1;
                }
                if rec_data_start + 2 > node.len() {
                    continue;
                }

                let record_type = BigEndian::read_i16(&node[rec_data_start..rec_data_start + 2]);
                let rec = &node[rec_data_start..];

                match record_type {
                    CATALOG_FOLDER => {
                        if rec.len() < 88 {
                            continue;
                        }
                        let folder_id = BigEndian::read_u32(&rec[8..12]);
                        results.push(CatalogEntry::Folder { folder_id, name });
                    }
                    CATALOG_FILE => {
                        if rec.len() < 248 {
                            continue;
                        }
                        let file_id = BigEndian::read_u32(&rec[8..12]);
                        // FileInfo at offset 48: fdType(4) + fdCreator(4) + fdFlags(2)
                        let type_code = decode_fourcc(&rec[48..52]);
                        let creator_code = decode_fourcc(&rec[52..56]);
                        let finder_flags = BigEndian::read_u16(&rec[56..58]);
                        // Data fork at offset 88 (80 bytes)
                        let data_fork = ForkData::parse(&rec[88..168]);
                        // Resource fork at offset 168 (80 bytes)
                        let rsrc_fork = ForkData::parse(&rec[168..248]);
                        results.push(CatalogEntry::File {
                            file_id,
                            name,
                            data_size: data_fork.logical_size,
                            data_fork,
                            rsrc_size: rsrc_fork.logical_size,
                            rsrc_fork,
                            type_code,
                            creator_code,
                            finder_flags,
                        });
                    }
                    _ => {}
                }
            }

            node_idx = desc.next;
        }

        Ok(results)
    }

    /// Find a file record by its file_id (CNID) in the catalog B-tree.
    /// Returns (data_fork, resource_fork).
    fn find_file_by_id(&self, file_id: u32) -> Option<(ForkData, ForkData)> {
        let node_size = self.catalog_header.node_size as usize;
        if node_size == 0 {
            return None;
        }

        let mut node_idx = self.catalog_header.first_leaf_node;
        while node_idx != 0 {
            let offset = node_idx as usize * node_size;
            if offset + node_size > self.catalog_data.len() {
                break;
            }
            let node = &self.catalog_data[offset..offset + node_size];
            let desc = BTreeNodeDescriptor::parse(node);
            if desc.kind != -1 {
                node_idx = desc.next;
                continue;
            }

            for i in 0..desc.num_records as usize {
                let offset_pos = node_size - 2 * (i + 1);
                if offset_pos + 2 > node.len() {
                    break;
                }
                let rec_offset = BigEndian::read_u16(&node[offset_pos..offset_pos + 2]) as usize;
                if rec_offset + 6 > node.len() {
                    continue;
                }
                let key_len = BigEndian::read_u16(&node[rec_offset..rec_offset + 2]) as usize;
                if key_len < 6 || rec_offset + 2 + key_len > node.len() {
                    continue;
                }
                let mut rec_data_start = rec_offset + 2 + key_len;
                if rec_data_start % 2 != 0 {
                    rec_data_start += 1;
                }
                if rec_data_start + 2 > node.len() {
                    continue;
                }
                let record_type = BigEndian::read_i16(&node[rec_data_start..rec_data_start + 2]);
                if record_type != CATALOG_FILE {
                    continue;
                }
                let rec = &node[rec_data_start..];
                if rec.len() < 248 {
                    continue;
                }
                let rec_file_id = BigEndian::read_u32(&rec[8..12]);
                if rec_file_id != file_id {
                    continue;
                }
                return Some((
                    ForkData::parse(&rec[88..168]),
                    ForkData::parse(&rec[168..248]),
                ));
            }
            node_idx = desc.next;
        }
        None
    }

    /// Read the allocation bitmap and return it.
    fn read_allocation_bitmap(&mut self) -> Result<Vec<u8>, FilesystemError> {
        let alloc_data = read_fork(
            &mut self.reader,
            self.partition_offset,
            self.vh.block_size,
            &self.vh.allocation_file,
        )?;
        Ok(alloc_data)
    }

    /// Ensure the allocation bitmap is cached in memory.
    fn ensure_bitmap(&mut self) -> Result<(), FilesystemError> {
        if self.bitmap.is_none() {
            self.bitmap = Some(self.read_allocation_bitmap()?);
        }
        Ok(())
    }

    /// Build HFS+ catalog key bytes: key_len(2) + parent_id(4) + name_length(2) + name(UTF-16BE NFD).
    fn build_catalog_key(parent_cnid: u32, name: &str) -> Vec<u8> {
        let nfd: String = name.nfd().collect();
        let utf16: Vec<u16> = nfd.encode_utf16().collect();
        let key_len = 4 + 2 + utf16.len() * 2;
        let mut key = Vec::with_capacity(2 + key_len);
        let mut buf = [0u8; 2];
        BigEndian::write_u16(&mut buf, key_len as u16);
        key.extend_from_slice(&buf);
        let mut buf4 = [0u8; 4];
        BigEndian::write_u32(&mut buf4, parent_cnid);
        key.extend_from_slice(&buf4);
        BigEndian::write_u16(&mut buf, utf16.len() as u16);
        key.extend_from_slice(&buf);
        for &ch in &utf16 {
            BigEndian::write_u16(&mut buf, ch);
            key.extend_from_slice(&buf);
        }
        key
    }

    /// Compare function for catalog B-tree records (compares key portion only).
    fn catalog_compare(a: &[u8], b: &[u8]) -> Ordering {
        // Both records start with: key_len(2) + parent_id(4) + name_len(2) + name(UTF-16BE)
        if a.len() < 8 || b.len() < 8 {
            return a.len().cmp(&b.len());
        }
        let parent_a = BigEndian::read_u32(&a[2..6]);
        let parent_b = BigEndian::read_u32(&b[2..6]);
        let name_len_a = BigEndian::read_u16(&a[6..8]) as usize;
        let name_len_b = BigEndian::read_u16(&b[6..8]) as usize;
        let name_a: Vec<u16> = a[8..8 + name_len_a.min((a.len() - 8) / 2) * 2]
            .chunks_exact(2)
            .map(|c| BigEndian::read_u16(c))
            .collect();
        let name_b: Vec<u16> = b[8..8 + name_len_b.min((b.len() - 8) / 2) * 2]
            .chunks_exact(2)
            .map(|c| BigEndian::read_u16(c))
            .collect();
        hfs_common::compare_hfsplus_keys(parent_a, &name_a, parent_b, &name_b, false)
    }

    /// Find a catalog record by (parent_cnid, name).
    /// Returns Some((node_idx, rec_idx, absolute_offset_in_catalog_data)) if found.
    /// Scans leaf nodes linearly (correct for all catalog sizes we encounter).
    fn find_catalog_record(&self, parent_cnid: u32, name: &str) -> Option<(u32, usize, usize)> {
        let search_key = Self::build_catalog_key(parent_cnid, name);
        let node_size = self.catalog_header.node_size as usize;
        if node_size == 0 {
            return None;
        }

        let mut node_idx = self.catalog_header.first_leaf_node;
        while node_idx != 0 {
            let offset = node_idx as usize * node_size;
            if offset + node_size > self.catalog_data.len() {
                break;
            }
            let node = &self.catalog_data[offset..offset + node_size];
            let kind = node[8] as i8;
            if kind != -1 {
                node_idx = BigEndian::read_u32(&node[0..4]);
                continue;
            }
            let num_records = BigEndian::read_u16(&node[10..12]) as usize;
            for i in 0..num_records {
                let (rec_start, rec_end) = btree_record_range(node, node_size, i);
                if rec_start >= rec_end || rec_end > node_size {
                    continue;
                }
                let rec = &node[rec_start..rec_end];
                if rec.len() < 8 {
                    continue;
                }
                // Compare key portion
                let key_len = BigEndian::read_u16(&rec[0..2]) as usize;
                let key_portion = &rec[..2 + key_len.min(rec.len() - 2)];
                if Self::catalog_compare(key_portion, &search_key) == Ordering::Equal {
                    return Some((node_idx, i, offset + rec_start));
                }
            }
            node_idx = BigEndian::read_u32(&node[0..4]);
        }
        None
    }

    /// Find a thread record by CNID (thread key: parent_id=cnid, name="").
    fn find_catalog_record_by_cnid(&self, cnid: u32) -> Option<(u32, usize, usize)> {
        self.find_catalog_record(cnid, "")
    }

    /// Update parent folder valence (child count) by delta.
    fn update_parent_valence(
        &mut self,
        parent_cnid: u32,
        delta: i32,
    ) -> Result<(), FilesystemError> {
        // Find parent's thread record to get its actual parent + name
        let (node_idx, _rec_idx, _offset) = self
            .find_catalog_record_by_cnid(parent_cnid)
            .ok_or_else(|| {
                FilesystemError::NotFound(format!("thread record for CNID {parent_cnid} not found"))
            })?;

        // Now scan for the actual folder record (not thread) with this CNID
        let node_size = self.catalog_header.node_size as usize;
        let mut scan_node = self.catalog_header.first_leaf_node;
        while scan_node != 0 {
            let offset = scan_node as usize * node_size;
            if offset + node_size > self.catalog_data.len() {
                break;
            }
            let num_records =
                BigEndian::read_u16(&self.catalog_data[offset + 10..offset + 12]) as usize;
            for i in 0..num_records {
                let (rec_start, rec_end) = btree_record_range(
                    &self.catalog_data[offset..offset + node_size],
                    node_size,
                    i,
                );
                let abs_start = offset + rec_start;
                let abs_end = offset + rec_end;
                if abs_end > self.catalog_data.len() || rec_end - rec_start < 8 {
                    continue;
                }
                let key_len =
                    BigEndian::read_u16(&self.catalog_data[abs_start..abs_start + 2]) as usize;
                let mut rec_data_start = abs_start + 2 + key_len;
                if rec_data_start % 2 != 0 {
                    rec_data_start += 1;
                }
                if rec_data_start + 12 > abs_end {
                    continue;
                }
                let record_type =
                    BigEndian::read_i16(&self.catalog_data[rec_data_start..rec_data_start + 2]);
                if record_type != CATALOG_FOLDER {
                    continue;
                }
                let folder_id = BigEndian::read_u32(
                    &self.catalog_data[rec_data_start + 8..rec_data_start + 12],
                );
                if folder_id == parent_cnid {
                    // Valence is at offset 4 in the folder record
                    let val_offset = rec_data_start + 4;
                    let old_val =
                        BigEndian::read_u32(&self.catalog_data[val_offset..val_offset + 4]);
                    let new_val = (old_val as i64 + delta as i64).max(0) as u32;
                    BigEndian::write_u32(
                        &mut self.catalog_data[val_offset..val_offset + 4],
                        new_val,
                    );
                    return Ok(());
                }
            }
            let next = BigEndian::read_u32(&self.catalog_data[offset..offset + 4]);
            scan_node = next;
        }
        // If we can't find it, that's not fatal (could be root with no visible record)
        let _ = node_idx; // suppress warning
        Ok(())
    }

    /// Insert a catalog record into the B-tree, handling splits and growth.
    fn insert_catalog_record(&mut self, key_record: &[u8]) -> Result<(), FilesystemError> {
        let header = BTreeHeader::read(&self.catalog_data);
        let node_size = header.node_size as usize;

        // Find the correct leaf node
        let (leaf_idx, parent_chain) = hfs_common::btree_find_insert_leaf(
            &self.catalog_data,
            &header,
            key_record,
            &Self::catalog_compare,
        );

        // Try to insert into the leaf
        let offset = leaf_idx as usize * node_size;
        let node = &mut self.catalog_data[offset..offset + node_size];
        match btree_insert_record(node, node_size, key_record, &Self::catalog_compare) {
            Ok(_) => {
                // Update header leaf_records
                let mut h = BTreeHeader::read(&self.catalog_data);
                h.leaf_records += 1;
                h.write(&mut self.catalog_data);
                self.catalog_header.leaf_records = h.leaf_records;
                Ok(())
            }
            Err(_) => {
                // Leaf full — atomic split+insert. Byte-based split point
                // tolerates uneven record sizes; avoids the
                // split-then-insert failure mode where the target half
                // can't accept the new record.
                let mut h = BTreeHeader::read(&self.catalog_data);
                let (new_idx, split_key) = btree_split_leaf_with_insert(
                    &mut self.catalog_data,
                    node_size,
                    leaf_idx,
                    &mut h,
                    key_record,
                    &Self::catalog_compare,
                )?;

                h.leaf_records += 1;

                // Insert separator into parent
                if h.depth == 1 {
                    // Root was a leaf — grow root
                    btree_grow_root(
                        &mut self.catalog_data,
                        node_size,
                        &mut h,
                        leaf_idx,
                        new_idx,
                        &split_key,
                    )?;
                } else {
                    // Find the parent index node for the leaf
                    if let Some(&(_, parent_idx)) =
                        parent_chain.iter().find(|&&(nidx, _)| nidx == leaf_idx)
                    {
                        btree_insert_into_index(
                            &mut self.catalog_data,
                            node_size,
                            parent_idx,
                            new_idx,
                            &split_key,
                            &mut h,
                            &Self::catalog_compare,
                            &parent_chain,
                        )?;
                    } else {
                        btree_grow_root(
                            &mut self.catalog_data,
                            node_size,
                            &mut h,
                            leaf_idx,
                            new_idx,
                            &split_key,
                        )?;
                    }
                }

                h.write(&mut self.catalog_data);
                self.catalog_header = BTreeHeaderRecord::parse(&self.catalog_data[14..14 + 106]);
                Ok(())
            }
        }
    }

    /// Remove a catalog record by (node_idx, rec_idx).
    fn remove_catalog_record(&mut self, node_idx: u32, rec_idx: usize) {
        let node_size = self.catalog_header.node_size as usize;
        let offset = node_idx as usize * node_size;
        btree_remove_record(
            &mut self.catalog_data[offset..offset + node_size],
            node_size,
            rec_idx,
        );

        // Check if leaf is now empty
        let num = BigEndian::read_u16(&self.catalog_data[offset + 10..offset + 12]);
        if num == 0 {
            // Free the node and update prev/next links
            let prev = BigEndian::read_u32(&self.catalog_data[offset + 4..offset + 8]);
            let next = BigEndian::read_u32(&self.catalog_data[offset..offset + 4]);
            if prev != 0 {
                let prev_off = prev as usize * node_size;
                BigEndian::write_u32(&mut self.catalog_data[prev_off..prev_off + 4], next);
            }
            if next != 0 {
                let next_off = next as usize * node_size;
                BigEndian::write_u32(&mut self.catalog_data[next_off + 4..next_off + 8], prev);
            }
            btree_free_node(&mut self.catalog_data, node_size, node_idx);

            let mut h = BTreeHeader::read(&self.catalog_data);
            h.free_nodes += 1;
            if h.first_leaf_node == node_idx {
                h.first_leaf_node = next;
            }
            if h.last_leaf_node == node_idx {
                h.last_leaf_node = prev;
            }
            h.write(&mut self.catalog_data);
        }

        // Update header leaf_records
        let mut h = BTreeHeader::read(&self.catalog_data);
        h.leaf_records = h.leaf_records.saturating_sub(1);
        h.write(&mut self.catalog_data);
        self.catalog_header = BTreeHeaderRecord::parse(&self.catalog_data[14..14 + 106]);
    }
}

// --- Write helpers (require R: Read + Write + Seek) ---

impl<R: Read + Write + Seek> HfsPlusFilesystem<R> {
    /// Write data to an allocation block.
    fn write_block(&mut self, block: u32, data: &[u8]) -> Result<(), FilesystemError> {
        let offset = self.partition_offset + block as u64 * self.vh.block_size as u64;
        self.reader.seek(SeekFrom::Start(offset))?;
        self.reader.write_all(data)?;
        Ok(())
    }

    /// Write the volume header to both primary (offset+1024) and backup (offset+total_size-1024).
    fn write_volume_header(&mut self) -> Result<(), FilesystemError> {
        let vh_bytes = self.vh.serialize();
        // Primary
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + 1024))?;
        self.reader.write_all(&vh_bytes)?;
        // Backup (last 1024 bytes of volume)
        let total_size = self.vh.total_blocks as u64 * self.vh.block_size as u64;
        if total_size > 1024 {
            self.reader
                .seek(SeekFrom::Start(self.partition_offset + total_size - 1024))?;
            self.reader.write_all(&vh_bytes)?;
        }
        Ok(())
    }

    /// Write the catalog B-tree data back to disk through the catalog_file fork extents.
    fn write_catalog(&mut self) -> Result<(), FilesystemError> {
        write_fork_data(
            &mut self.reader,
            self.partition_offset,
            self.vh.block_size,
            &self.vh.catalog_file,
            &self.catalog_data,
        )
    }

    /// Write the allocation bitmap back to disk.
    fn write_allocation_bitmap(&mut self) -> Result<(), FilesystemError> {
        if let Some(ref bm) = self.bitmap {
            write_fork_data(
                &mut self.reader,
                self.partition_offset,
                self.vh.block_size,
                &self.vh.allocation_file,
                bm,
            )?;
        }
        Ok(())
    }

    /// Allocate `count` contiguous blocks from the allocation bitmap.
    /// Returns the start block index.
    fn allocate_blocks(&mut self, count: u32) -> Result<u32, FilesystemError> {
        self.ensure_bitmap()?;
        let bitmap = self.bitmap.as_mut().unwrap();
        let start =
            bitmap_find_clear_run_be(bitmap, self.vh.total_blocks, count).ok_or_else(|| {
                FilesystemError::DiskFull(format!("cannot find {} contiguous free blocks", count))
            })?;
        for i in 0..count {
            bitmap_set_bit_be(bitmap, start + i);
        }
        self.vh.free_blocks -= count;
        Ok(start)
    }

    /// Free `count` blocks starting at `start`.
    fn free_blocks(&mut self, start: u32, count: u32) {
        self.ensure_bitmap().ok();
        if let Some(ref mut bitmap) = self.bitmap {
            for i in 0..count {
                bitmap_clear_bit_be(bitmap, start + i);
            }
        }
        self.vh.free_blocks += count;
    }

    /// Free all blocks referenced by a fork (inline extents only).
    fn free_fork_blocks(&mut self, fork: &ForkData) {
        for ext in &fork.extents {
            if ext.is_empty() {
                break;
            }
            self.free_blocks(ext.start_block, ext.block_count);
        }
    }

    /// Write file data to allocated blocks. Returns the ForkData describing the allocation.
    fn write_data_to_blocks(
        &mut self,
        data: &mut dyn std::io::Read,
        data_len: u64,
    ) -> Result<ForkData, FilesystemError> {
        if data_len == 0 {
            return Ok(ForkData::empty());
        }
        let block_size = self.vh.block_size as u64;
        let blocks_needed = ((data_len + block_size - 1) / block_size) as u32;
        let start_block = self.allocate_blocks(blocks_needed)?;

        // Write data block by block
        let mut buf = vec![0u8; block_size as usize];
        let mut remaining = data_len;
        for i in 0..blocks_needed {
            let to_read = remaining.min(block_size) as usize;
            buf[..to_read].fill(0);
            data.read_exact(&mut buf[..to_read]).map_err(|e| {
                FilesystemError::Io(std::io::Error::new(
                    e.kind(),
                    format!("reading file data: {e}"),
                ))
            })?;
            // Zero-pad the rest of the block
            if to_read < block_size as usize {
                buf[to_read..].fill(0);
            }
            self.write_block(start_block + i, &buf)?;
            remaining -= to_read as u64;
        }

        let mut fork = ForkData::empty();
        fork.logical_size = data_len;
        fork.total_blocks = blocks_needed;
        fork.extents[0] = ExtentDescriptor {
            start_block,
            block_count: blocks_needed,
        };
        Ok(fork)
    }

    /// Build a complete HFS+ file catalog record (248 bytes).
    fn build_file_record(
        file_id: u32,
        data_fork: &ForkData,
        rsrc_fork: &ForkData,
        type_code: &[u8; 4],
        creator_code: &[u8; 4],
    ) -> [u8; 248] {
        let mut rec = [0u8; 248];
        let now = hfs_common::hfs_now();
        BigEndian::write_i16(&mut rec[0..2], CATALOG_FILE);
        BigEndian::write_u32(&mut rec[8..12], file_id);
        BigEndian::write_u32(&mut rec[12..16], now); // createDate
        BigEndian::write_u32(&mut rec[16..20], now); // contentModDate
        BigEndian::write_u32(&mut rec[20..24], now); // attributeModDate
        BigEndian::write_u32(&mut rec[24..28], now); // accessDate
                                                     // FileInfo (userInfo): fdType at offset 48, fdCreator at offset 52
        rec[48..52].copy_from_slice(type_code);
        rec[52..56].copy_from_slice(creator_code);
        // dataFork at offset 88
        data_fork.serialize(&mut rec[88..168]);
        // resourceFork at offset 168
        rsrc_fork.serialize(&mut rec[168..248]);
        rec
    }

    /// Build a complete HFS+ folder catalog record (88 bytes).
    fn build_folder_record(folder_id: u32) -> [u8; 88] {
        let mut rec = [0u8; 88];
        let now = hfs_common::hfs_now();
        BigEndian::write_i16(&mut rec[0..2], CATALOG_FOLDER);
        // valence = 0 (offset 4)
        BigEndian::write_u32(&mut rec[8..12], folder_id);
        BigEndian::write_u32(&mut rec[12..16], now);
        BigEndian::write_u32(&mut rec[16..20], now);
        BigEndian::write_u32(&mut rec[20..24], now);
        BigEndian::write_u32(&mut rec[24..28], now);
        rec
    }

    /// Build a thread record. Thread key: (cnid, ""). Thread data: type(2) + reserved(2) + parentID(4) + name.
    fn build_thread_record(record_type: i16, parent_cnid: u32, name: &str) -> (Vec<u8>, Vec<u8>) {
        // Thread key: parent_id = cnid, name = empty
        // Actually, thread records are keyed by (cnid, "")
        // We need to build key + record separately

        // Key
        let key = Self::build_catalog_key(parent_cnid, "");
        // But wait — for thread records, the key uses the CNID as parent_id and empty name.
        // The parent_cnid here is actually the target CNID, not the actual parent.
        // Let me re-read: Thread key: key_len(2) + parent_id(4, =CNID) + name_len(2, =0)

        // Record data: type(2) + reserved(2) + parentID(4) + name_len(2) + name(UTF-16BE)
        let nfd: String = name.nfd().collect();
        let utf16: Vec<u16> = nfd.encode_utf16().collect();
        let mut rec = Vec::with_capacity(10 + utf16.len() * 2);
        let mut buf2 = [0u8; 2];
        let mut buf4 = [0u8; 4];
        BigEndian::write_i16(&mut buf2, record_type);
        rec.extend_from_slice(&buf2); // type
        rec.extend_from_slice(&[0, 0]); // reserved
        BigEndian::write_u32(&mut buf4, parent_cnid);
        rec.extend_from_slice(&buf4); // parentID (the actual parent of the entry)
        BigEndian::write_u16(&mut buf2, utf16.len() as u16);
        rec.extend_from_slice(&buf2); // name_len
        for &ch in &utf16 {
            BigEndian::write_u16(&mut buf2, ch);
            rec.extend_from_slice(&buf2);
        }
        (key, rec)
    }

    /// Sync metadata: write catalog + allocation bitmap + volume header + flush.
    fn do_sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.vh.modify_date = hfs_common::hfs_now();
        self.write_catalog()?;
        self.write_allocation_bitmap()?;
        self.write_volume_header()?;
        self.reader.flush()?;
        Ok(())
    }
}

impl<R: Read + Seek + Send> Filesystem for HfsPlusFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry {
            name: "/".into(),
            path: "/".into(),
            entry_type: EntryType::Directory,
            size: 0,
            location: 2, // HFS+ root directory CNID
            modified: None,
            type_code: None,
            creator_code: None,
            symlink_target: None,
            special_type: None,
            mode: None,
            uid: None,
            gid: None,
            resource_fork_size: None,
            aux_type: None,
        })
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        let parent_cnid = entry.location as u32;
        let children = self.list_children(parent_cnid)?;

        let mut entries = Vec::new();
        for child in children {
            match child {
                CatalogEntry::Folder { folder_id, name } => {
                    let path = if entry.path == "/" {
                        format!("/{name}")
                    } else {
                        format!("{}/{name}", entry.path)
                    };
                    entries.push(FileEntry::new_directory(name, path, folder_id as u64));
                }
                CatalogEntry::File {
                    file_id,
                    name,
                    data_size,
                    data_fork,
                    rsrc_size,
                    rsrc_fork,
                    type_code,
                    creator_code,
                    finder_flags,
                } => {
                    let path = if entry.path == "/" {
                        format!("/{name}")
                    } else {
                        format!("{}/{name}", entry.path)
                    };
                    let mut fe = FileEntry::new_file(name, path, data_size, file_id as u64);
                    // HFS+ UNIX symlink: type "slnk" / creator "rhap", data
                    // fork is the target path as UTF-8 (typically <256 bytes).
                    if type_code == super::mac_alias::SLNK_TYPE
                        && creator_code == super::mac_alias::RHAP_CREATOR
                        && data_size > 0
                        && data_size <= 4096
                    {
                        if let Ok(data) = read_fork(
                            &mut self.reader,
                            self.partition_offset,
                            self.vh.block_size,
                            &data_fork,
                        ) {
                            if let Ok(target) = std::str::from_utf8(&data) {
                                let trimmed = target.trim_end_matches('\0').trim();
                                if !trimmed.is_empty() {
                                    fe.symlink_target = Some(trimmed.to_string());
                                }
                            }
                        }
                    }
                    fe.type_code = Some(type_code);
                    fe.creator_code = Some(creator_code);
                    if rsrc_size > 0 {
                        fe.resource_fork_size = Some(rsrc_size);
                    }
                    // Classic-Mac alias: fdFlags bit 0x8000 set.
                    if finder_flags & super::mac_alias::IS_ALIAS_FLAG != 0
                        && rsrc_size > 0
                        && fe.symlink_target.is_none()
                    {
                        if let Ok(rsrc) = read_fork(
                            &mut self.reader,
                            self.partition_offset,
                            self.vh.block_size,
                            &rsrc_fork,
                        ) {
                            if let Some(target) = super::mac_alias::resolve_alias_target(&rsrc) {
                                fe.symlink_target = Some(target);
                            }
                        }
                    }
                    entries.push(fe);
                }
            }
        }

        entries.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
        Ok(entries)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let file_id = entry.location as u32;
        let (data_fork, _rsrc_fork) = self.find_file_by_id(file_id).ok_or_else(|| {
            FilesystemError::NotFound(format!("file id {file_id} not found in catalog"))
        })?;

        let mut data = read_fork(
            &mut self.reader,
            self.partition_offset,
            self.vh.block_size,
            &data_fork,
        )?;
        data.truncate(max_bytes);
        Ok(data)
    }

    fn volume_label(&self) -> Option<&str> {
        if self.label.is_empty() {
            None
        } else {
            Some(&self.label)
        }
    }

    fn fs_type(&self) -> &str {
        if self.vh.is_hfsx() {
            "HFSX"
        } else {
            "HFS+"
        }
    }

    fn validate_name(&self, name: &str) -> Result<(), FilesystemError> {
        validate_hfsplus_create_name(name)
    }

    fn total_size(&self) -> u64 {
        self.vh.total_blocks as u64 * self.vh.block_size as u64
    }

    fn used_size(&self) -> u64 {
        (self.vh.total_blocks - self.vh.free_blocks) as u64 * self.vh.block_size as u64
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        let bitmap = self.read_allocation_bitmap()?;
        // The HFS+ alternate volume header occupies the second-to-last 512-byte sector
        // of the partition.  For volumes whose allocation block size is ≤ 1024 bytes this
        // overlaps with the last TWO allocation blocks; for larger block sizes (4 KiB, etc.)
        // it falls within only the last block.  Either way the last 1–2 blocks are reserved
        // for VH/alternate-VH and are always marked allocated — if we include them in the
        // search, find_last_set_bit returns the very end of the partition, making trimming
        // impossible.  Exclude the final two blocks so that the trim point reflects the
        // last genuine user-data block instead.
        let search_up_to = self.vh.total_blocks.saturating_sub(2);
        let last = find_last_set_bit(&bitmap, search_up_to);
        match last {
            Some(block) => {
                let byte = (block as u64 + 1) * self.vh.block_size as u64;
                Ok(byte)
            }
            None => Ok(self.total_size()),
        }
    }

    fn write_resource_fork_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn std::io::Write,
    ) -> Result<u64, FilesystemError> {
        let file_id = entry.location as u32;
        let (_data_fork, rsrc_fork) = self.find_file_by_id(file_id).ok_or_else(|| {
            FilesystemError::NotFound(format!("file id {file_id} not found in catalog"))
        })?;
        if rsrc_fork.logical_size == 0 {
            return Ok(0);
        }
        let data = read_fork(
            &mut self.reader,
            self.partition_offset,
            self.vh.block_size,
            &rsrc_fork,
        )?;
        writer.write_all(&data)?;
        Ok(data.len() as u64)
    }

    fn resource_fork_size(&mut self, entry: &FileEntry) -> u64 {
        let file_id = entry.location as u32;
        self.find_file_by_id(file_id)
            .map(|(_d, r)| r.logical_size)
            .unwrap_or(0)
    }

    fn blessed_system_folder(&mut self) -> Option<(u64, String)> {
        // finderInfo[0] = Classic Mac OS System Folder CNID
        let cnid = self.vh.finder_info[0];
        if cnid == 0 {
            // Try finderInfo[5] = Mac OS X boot directory
            let cnid_x = self.vh.finder_info[5];
            if cnid_x == 0 {
                return None;
            }
            return self.lookup_folder_name(cnid_x);
        }
        self.lookup_folder_name(cnid)
    }
}

impl<R: Read + Seek + Send> HfsPlusFilesystem<R> {
    fn lookup_folder_name(&self, cnid: u32) -> Option<(u64, String)> {
        // Find the thread record for this CNID to get its name
        if let Some((_node, _rec, offset)) = self.find_catalog_record_by_cnid(cnid) {
            let node_size = self.catalog_header.node_size as usize;
            let key_len = BigEndian::read_u16(&self.catalog_data[offset..offset + 2]) as usize;
            let mut rec_data_start = offset + 2 + key_len;
            if rec_data_start % 2 != 0 {
                rec_data_start += 1;
            }
            // Thread record: type(2) + reserved(2) + parentID(4) + name_len(2) + name(UTF-16BE)
            if rec_data_start + 10 <= self.catalog_data.len() {
                let name_len = BigEndian::read_u16(
                    &self.catalog_data[rec_data_start + 8..rec_data_start + 10],
                ) as usize;
                let name_end = rec_data_start + 10 + name_len * 2;
                if name_end <= self.catalog_data.len() {
                    let name = decode_utf16be(&self.catalog_data[rec_data_start + 10..name_end]);
                    return Some((cnid as u64, name));
                }
            }
            let _ = node_size;
        }
        // CNID set but can't resolve name
        Some((cnid as u64, format!("CNID {}", cnid)))
    }
}

/// Read a fork's data through its extent descriptors.
fn read_fork<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    block_size: u32,
    fork: &ForkData,
) -> Result<Vec<u8>, FilesystemError> {
    let size = fork.logical_size as usize;
    let mut data = Vec::with_capacity(size);

    for ext in &fork.extents {
        if ext.is_empty() {
            break;
        }
        let offset = partition_offset + ext.start_block as u64 * block_size as u64;
        let len = ext.block_count as u64 * block_size as u64;
        reader.seek(SeekFrom::Start(offset))?;
        let read_len = len.min((size - data.len()) as u64) as usize;
        let mut buf = vec![0u8; read_len];
        reader.read_exact(&mut buf)?;
        data.extend_from_slice(&buf);
        if data.len() >= size {
            break;
        }
    }

    data.truncate(size);
    Ok(data)
}

/// Write data through a fork's extent descriptors.
fn write_fork_data<R: Write + Seek>(
    writer: &mut R,
    partition_offset: u64,
    block_size: u32,
    fork: &ForkData,
    data: &[u8],
) -> Result<(), FilesystemError> {
    let mut written = 0usize;
    for ext in &fork.extents {
        if ext.is_empty() || written >= data.len() {
            break;
        }
        let offset = partition_offset + ext.start_block as u64 * block_size as u64;
        let extent_len = ext.block_count as u64 * block_size as u64;
        let to_write = extent_len.min((data.len() - written) as u64) as usize;
        writer.seek(SeekFrom::Start(offset))?;
        writer.write_all(&data[written..written + to_write])?;
        written += to_write;
    }
    Ok(())
}

/// Decode a UTF-16BE byte slice to a String.
fn decode_utf16be(data: &[u8]) -> String {
    let chars: Vec<u16> = data
        .chunks_exact(2)
        .map(|c| BigEndian::read_u16(c))
        .collect();
    String::from_utf16_lossy(&chars)
}

/// Find the volume label from the catalog B-tree (root folder thread record).
fn find_volume_label(catalog_data: &[u8], header: &BTreeHeaderRecord) -> String {
    let node_size = header.node_size as usize;
    if node_size == 0 {
        return String::new();
    }

    // Search leaf nodes for the folder thread of CNID 2 (root)
    let mut node_idx = header.first_leaf_node;

    while node_idx != 0 {
        let offset = node_idx as usize * node_size;
        if offset + node_size > catalog_data.len() {
            break;
        }
        let node = &catalog_data[offset..offset + node_size];
        let desc = BTreeNodeDescriptor::parse(node);
        if desc.kind != -1 {
            node_idx = desc.next;
            continue;
        }

        for i in 0..desc.num_records as usize {
            let offset_pos = node_size - 2 * (i + 1);
            if offset_pos + 2 > node.len() {
                break;
            }
            let rec_offset = BigEndian::read_u16(&node[offset_pos..offset_pos + 2]) as usize;
            if rec_offset + 8 > node.len() {
                continue;
            }

            let key_len = BigEndian::read_u16(&node[rec_offset..rec_offset + 2]) as usize;
            if key_len < 6 || rec_offset + 2 + key_len > node.len() {
                continue;
            }
            let key_data = &node[rec_offset + 2..rec_offset + 2 + key_len];
            let parent_id = BigEndian::read_u32(&key_data[0..4]);

            // Thread records for CNID 2 have parent_id = 2 and name_length = 0
            if parent_id != 2 {
                continue;
            }
            let name_length = BigEndian::read_u16(&key_data[4..6]) as usize;
            if name_length != 0 {
                continue;
            }

            // This is a thread record for the root — get the record data
            let mut rec_data_start = rec_offset + 2 + key_len;
            if rec_data_start % 2 != 0 {
                rec_data_start += 1;
            }
            if rec_data_start + 10 > node.len() {
                continue;
            }

            let record_type = BigEndian::read_i16(&node[rec_data_start..rec_data_start + 2]);
            if record_type == 3 {
                // Folder thread: type(2) + reserved(2) + parentID(4) + nodeName(2+N*2)
                if rec_data_start + 10 > node.len() {
                    continue;
                }
                let thread_name_len =
                    BigEndian::read_u16(&node[rec_data_start + 8..rec_data_start + 10]) as usize;
                if rec_data_start + 10 + thread_name_len * 2 > node.len() {
                    continue;
                }
                return decode_utf16be(
                    &node[rec_data_start + 10..rec_data_start + 10 + thread_name_len * 2],
                );
            }
        }

        node_idx = desc.next;
    }

    String::new()
}

/// Find the index of the last set bit in a bitmap (MSB-first).
fn find_last_set_bit(bitmap: &[u8], max_bits: u32) -> Option<u32> {
    // Scan from the end for efficiency
    let last_byte = ((max_bits as usize).min(bitmap.len() * 8) + 7) / 8;
    for byte_idx in (0..last_byte.min(bitmap.len())).rev() {
        if bitmap[byte_idx] == 0 {
            continue;
        }
        // Find highest set bit in this byte
        for bit in 0..8 {
            let global_bit = byte_idx * 8 + bit;
            if global_bit >= max_bits as usize {
                continue;
            }
            let bit_val = 7 - bit; // MSB-first
            if (bitmap[byte_idx] >> bit_val) & 1 == 1 {
                // This byte has set bits — but we need the *last* one globally
                // Since we're scanning bytes from end, find last bit in this byte
                for b2 in (0..8).rev() {
                    let global_bit2 = byte_idx * 8 + (7 - b2);
                    if global_bit2 < max_bits as usize && (bitmap[byte_idx] >> b2) & 1 == 1 {
                        return Some(global_bit2 as u32);
                    }
                }
            }
        }
    }
    None
}

// --- EditableFilesystem implementation ---

impl<R: Read + Write + Seek + Send> EditableFilesystem for HfsPlusFilesystem<R> {
    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn std::io::Read,
        data_len: u64,
        options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let parent_cnid = parent.location as u32;

        validate_hfsplus_create_name(name)?;

        // Check for duplicates
        if self.find_catalog_record(parent_cnid, name).is_some() {
            return Err(FilesystemError::AlreadyExists(name.into()));
        }

        // Assign CNID
        let file_id = self.vh.next_catalog_id;
        self.vh.next_catalog_id += 1;

        // Determine type/creator: prefer caller-supplied, fill any missing
        // half from the extension dictionary so a partial FInfo from an
        // imported AppleDouble isn't thrown away.
        let ext = name.rsplit('.').next().unwrap_or("");
        let (dict_t, dict_c) =
            hfs_common::type_creator_for_extension(ext).unwrap_or(([0; 4], [0; 4]));
        let type_code = options
            .type_code
            .as_deref()
            .map(hfs_common::encode_fourcc)
            .unwrap_or(dict_t);
        let creator_code = options
            .creator_code
            .as_deref()
            .map(hfs_common::encode_fourcc)
            .unwrap_or(dict_c);

        // Allocate blocks and write data
        let data_fork = self.write_data_to_blocks(data, data_len)?;

        // Handle resource fork
        let rsrc_fork = if let Some(ref rsrc_src) = options.resource_fork {
            match rsrc_src {
                super::filesystem::ResourceForkSource::Data(rsrc_data) => {
                    let mut cursor = std::io::Cursor::new(rsrc_data);
                    self.write_data_to_blocks(&mut cursor, rsrc_data.len() as u64)?
                }
                super::filesystem::ResourceForkSource::File(path) => {
                    let mut f = std::fs::File::open(path)?;
                    let len = f.metadata()?.len();
                    self.write_data_to_blocks(&mut f, len)?
                }
            }
        } else {
            ForkData::empty()
        };

        // Build file record
        let file_rec =
            Self::build_file_record(file_id, &data_fork, &rsrc_fork, &type_code, &creator_code);

        // Build key + record for catalog insertion
        let key = Self::build_catalog_key(parent_cnid, name);
        let mut key_record = key.clone();
        // Pad to even boundary if needed
        if key_record.len() % 2 != 0 {
            key_record.push(0);
        }
        key_record.extend_from_slice(&file_rec);

        // Insert file record
        self.insert_catalog_record(&key_record)?;

        // Build and insert thread record
        let (thread_key, thread_data) =
            Self::build_thread_record(CATALOG_FILE_THREAD, parent_cnid, name);
        let mut thread_record = thread_key;
        if thread_record.len() % 2 != 0 {
            thread_record.push(0);
        }
        thread_record.extend_from_slice(&thread_data);
        // Thread key is (file_id, ""), but build_thread_record used parent_cnid...
        // Actually, thread records are keyed by (file_id, "")
        let actual_thread_key = Self::build_catalog_key(file_id, "");
        let mut actual_thread_record = actual_thread_key;
        if actual_thread_record.len() % 2 != 0 {
            actual_thread_record.push(0);
        }
        actual_thread_record.extend_from_slice(&thread_data);
        self.insert_catalog_record(&actual_thread_record)?;

        // Update parent valence
        self.update_parent_valence(parent_cnid, 1)?;

        // Update VH counts
        self.vh.file_count += 1;

        let path = if parent.path == "/" {
            format!("/{name}")
        } else {
            format!("{}/{name}", parent.path)
        };
        let mut fe = FileEntry::new_file(name.to_string(), path, data_len, file_id as u64);
        let tc_str = String::from_utf8_lossy(&type_code).to_string();
        let cc_str = String::from_utf8_lossy(&creator_code).to_string();
        if type_code != [0; 4] {
            fe.type_code = Some(tc_str);
            fe.creator_code = Some(cc_str);
        }
        if rsrc_fork.logical_size > 0 {
            fe.resource_fork_size = Some(rsrc_fork.logical_size);
        }
        Ok(fe)
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let parent_cnid = parent.location as u32;

        validate_hfsplus_create_name(name)?;

        // Check duplicates
        if self.find_catalog_record(parent_cnid, name).is_some() {
            return Err(FilesystemError::AlreadyExists(name.into()));
        }

        // Assign CNID
        let folder_id = self.vh.next_catalog_id;
        self.vh.next_catalog_id += 1;

        // Build folder record
        let folder_rec = Self::build_folder_record(folder_id);

        // Build key + record
        let key = Self::build_catalog_key(parent_cnid, name);
        let mut key_record = key.clone();
        if key_record.len() % 2 != 0 {
            key_record.push(0);
        }
        key_record.extend_from_slice(&folder_rec);

        self.insert_catalog_record(&key_record)?;

        // Thread record (type 3 = folder thread)
        let (_thread_key, thread_data) =
            Self::build_thread_record(CATALOG_FOLDER_THREAD, parent_cnid, name);
        let actual_thread_key = Self::build_catalog_key(folder_id, "");
        let mut actual_thread_record = actual_thread_key;
        if actual_thread_record.len() % 2 != 0 {
            actual_thread_record.push(0);
        }
        actual_thread_record.extend_from_slice(&thread_data);
        self.insert_catalog_record(&actual_thread_record)?;

        // Update parent valence
        self.update_parent_valence(parent_cnid, 1)?;

        // Update VH
        self.vh.folder_count += 1;

        let path = if parent.path == "/" {
            format!("/{name}")
        } else {
            format!("{}/{name}", parent.path)
        };
        Ok(FileEntry::new_directory(
            name.to_string(),
            path,
            folder_id as u64,
        ))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        let parent_cnid = parent.location as u32;
        let cnid = entry.location as u32;

        // Check directory is empty
        if entry.is_directory() {
            let children = self.list_children(cnid)?;
            if !children.is_empty() {
                return Err(FilesystemError::InvalidData(
                    "cannot delete non-empty directory".into(),
                ));
            }
        }

        // Find and remove the entry record
        let (node_idx, rec_idx, _offset) = self
            .find_catalog_record(parent_cnid, &entry.name)
            .ok_or_else(|| {
                FilesystemError::NotFound(format!("entry '{}' not found in catalog", entry.name))
            })?;

        // If it's a file, free its data and resource fork blocks
        if !entry.is_directory() {
            if let Some((data_fork, rsrc_fork)) = self.find_file_by_id(cnid) {
                self.free_fork_blocks(&data_fork);
                self.free_fork_blocks(&rsrc_fork);
            }
        }

        self.remove_catalog_record(node_idx, rec_idx);

        // Find and remove the thread record
        if let Some((t_node, t_rec, _)) = self.find_catalog_record_by_cnid(cnid) {
            self.remove_catalog_record(t_node, t_rec);
        }

        // Update parent valence
        self.update_parent_valence(parent_cnid, -1)?;

        // Update VH counts
        if entry.is_directory() {
            self.vh.folder_count = self.vh.folder_count.saturating_sub(1);
        } else {
            self.vh.file_count = self.vh.file_count.saturating_sub(1);
        }

        Ok(())
    }

    fn set_type_creator(
        &mut self,
        entry: &FileEntry,
        type_code: &str,
        creator_code: &str,
    ) -> Result<(), FilesystemError> {
        let cnid = entry.location as u32;

        // Find the thread to get parent + name
        let (_t_node, _t_rec, t_offset) =
            self.find_catalog_record_by_cnid(cnid).ok_or_else(|| {
                FilesystemError::NotFound(format!("thread for CNID {cnid} not found"))
            })?;

        // Read parent_cnid and name from thread data
        let node_size = self.catalog_header.node_size as usize;
        let key_len = BigEndian::read_u16(&self.catalog_data[t_offset..t_offset + 2]) as usize;
        let mut rec_data_start = t_offset + 2 + key_len;
        if rec_data_start % 2 != 0 {
            rec_data_start += 1;
        }
        let thread_parent =
            BigEndian::read_u32(&self.catalog_data[rec_data_start + 4..rec_data_start + 8]);
        let thread_name_len =
            BigEndian::read_u16(&self.catalog_data[rec_data_start + 8..rec_data_start + 10])
                as usize;
        let thread_name = decode_utf16be(
            &self.catalog_data[rec_data_start + 10..rec_data_start + 10 + thread_name_len * 2],
        );

        // Find the actual file record
        let (f_node, f_rec, f_offset) = self
            .find_catalog_record(thread_parent, &thread_name)
            .ok_or_else(|| {
                FilesystemError::NotFound(format!("file record for '{}' not found", thread_name))
            })?;

        // Compute the record data offset
        let fkey_len = BigEndian::read_u16(&self.catalog_data[f_offset..f_offset + 2]) as usize;
        let mut frec_start = f_offset + 2 + fkey_len;
        if frec_start % 2 != 0 {
            frec_start += 1;
        }

        // Write type and creator at offsets 48-52 and 52-56
        let tc = hfs_common::encode_fourcc(type_code);
        let cc = hfs_common::encode_fourcc(creator_code);
        self.catalog_data[frec_start + 48..frec_start + 52].copy_from_slice(&tc);
        self.catalog_data[frec_start + 52..frec_start + 56].copy_from_slice(&cc);

        let _ = (f_node, f_rec, node_size); // suppress warnings
        Ok(())
    }

    fn write_resource_fork(
        &mut self,
        entry: &FileEntry,
        data: &mut dyn std::io::Read,
        len: u64,
    ) -> Result<(), FilesystemError> {
        let cnid = entry.location as u32;

        // Free existing resource fork blocks
        if let Some((_data_fork, rsrc_fork)) = self.find_file_by_id(cnid) {
            self.free_fork_blocks(&rsrc_fork);
        }

        // Allocate and write new resource fork data
        let new_rsrc = self.write_data_to_blocks(data, len)?;

        // Find the file record and update the resource fork
        let (t_node, _t_rec, t_offset) =
            self.find_catalog_record_by_cnid(cnid).ok_or_else(|| {
                FilesystemError::NotFound(format!("thread for CNID {cnid} not found"))
            })?;

        let key_len = BigEndian::read_u16(&self.catalog_data[t_offset..t_offset + 2]) as usize;
        let mut rec_data_start = t_offset + 2 + key_len;
        if rec_data_start % 2 != 0 {
            rec_data_start += 1;
        }
        let thread_parent =
            BigEndian::read_u32(&self.catalog_data[rec_data_start + 4..rec_data_start + 8]);
        let thread_name_len =
            BigEndian::read_u16(&self.catalog_data[rec_data_start + 8..rec_data_start + 10])
                as usize;
        let thread_name = decode_utf16be(
            &self.catalog_data[rec_data_start + 10..rec_data_start + 10 + thread_name_len * 2],
        );

        let (_f_node, _f_rec, f_offset) = self
            .find_catalog_record(thread_parent, &thread_name)
            .ok_or_else(|| {
                FilesystemError::NotFound(format!("file record for '{}' not found", thread_name))
            })?;

        let fkey_len = BigEndian::read_u16(&self.catalog_data[f_offset..f_offset + 2]) as usize;
        let mut frec_start = f_offset + 2 + fkey_len;
        if frec_start % 2 != 0 {
            frec_start += 1;
        }

        // Write resource fork data (at offset 168 in file record)
        let mut rsrc_bytes = [0u8; 80];
        new_rsrc.serialize(&mut rsrc_bytes);
        self.catalog_data[frec_start + 168..frec_start + 248].copy_from_slice(&rsrc_bytes);

        let _ = t_node;
        Ok(())
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.do_sync_metadata()
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.vh.free_blocks as u64 * self.vh.block_size as u64)
    }

    fn set_blessed_folder(&mut self, entry: &FileEntry) -> Result<(), FilesystemError> {
        if !entry.is_directory() {
            return Err(FilesystemError::InvalidData(
                "can only bless a directory".into(),
            ));
        }
        self.vh.finder_info[0] = entry.location as u32;
        Ok(())
    }
}

// --- Compact reader ---

/// Compact reader for HFS+: layout-preserving image with zeros for unallocated blocks.
///
/// Outputs `total_blocks * block_size` bytes. Allocated blocks are read from the
/// source; unallocated blocks are emitted as zeros. This preserves the original
/// block layout so that `block_N` is always at byte offset `N * block_size`,
/// enabling correct filesystem browsing and reliable restore.
pub struct CompactHfsPlusReader<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    block_size: u32,
    total_blocks: u32,
    bitmap: Vec<u8>,
    /// Current allocation block being output.
    current_block: u32,
    /// Byte position within the current block.
    block_pos: u64,
    #[allow(dead_code)]
    original_size: u64,
}

impl<R: Read + Seek> CompactHfsPlusReader<R> {
    pub fn new(
        mut reader: R,
        partition_offset: u64,
    ) -> Result<(Self, CompactResult), FilesystemError> {
        // Read volume header
        eprintln!(
            "[HFS+ compact] seeking to VH at offset {}",
            partition_offset + 1024
        );
        reader.seek(SeekFrom::Start(partition_offset + 1024))?;
        let mut vh_buf = [0u8; 512];
        reader.read_exact(&mut vh_buf)?;
        let vh = HfsPlusVolumeHeader::parse(&vh_buf)?;
        eprintln!(
            "[HFS+ compact] VH ok: block_size={}, total_blocks={}, alloc_file extents[0]=(start={}, count={})",
            vh.block_size,
            vh.total_blocks,
            vh.allocation_file.extents[0].start_block,
            vh.allocation_file.extents[0].block_count,
        );

        // Read allocation file
        let alloc_data = read_fork(
            &mut reader,
            partition_offset,
            vh.block_size,
            &vh.allocation_file,
        )
        .map_err(|e| {
            eprintln!("[HFS+ compact] read_fork(alloc_file) failed: {e}");
            e
        })?;
        eprintln!(
            "[HFS+ compact] allocation bitmap read: {} bytes",
            alloc_data.len()
        );

        // Count allocated blocks
        let mut allocated = 0u32;
        for bit in 0..vh.total_blocks {
            let byte_idx = bit as usize / 8;
            let bit_idx = 7 - (bit % 8);
            if byte_idx < alloc_data.len() && (alloc_data[byte_idx] >> bit_idx) & 1 == 1 {
                allocated += 1;
            }
        }
        eprintln!(
            "[HFS+ compact] allocated={} / {} total blocks ({} free)",
            allocated,
            vh.total_blocks,
            vh.total_blocks - allocated,
        );

        let original_size = vh.total_blocks as u64 * vh.block_size as u64;
        // Layout-preserving: output size equals the original partition size.
        // Unallocated blocks are zeroed, so they compress extremely well.
        let compacted_size = original_size;
        // data_size: only allocated blocks require disk reads.
        let data_size = allocated as u64 * vh.block_size as u64;
        eprintln!(
            "[HFS+ compact] compacted_size={} data_size={} original_size={} (layout-preserving; free blocks -> zeros)",
            compacted_size, data_size, original_size
        );

        let result = CompactResult {
            original_size,
            compacted_size,
            data_size,
            clusters_used: allocated,
        };

        Ok((
            CompactHfsPlusReader {
                reader,
                partition_offset,
                block_size: vh.block_size,
                total_blocks: vh.total_blocks,
                bitmap: alloc_data,
                current_block: 0,
                block_pos: 0,
                original_size,
            },
            result,
        ))
    }

    fn is_block_allocated(&self, block: u32) -> bool {
        let byte_idx = block as usize / 8;
        let bit_idx = 7 - (block % 8);
        byte_idx < self.bitmap.len() && (self.bitmap[byte_idx] >> bit_idx) & 1 == 1
    }
}

impl<R: Read + Seek> Read for CompactHfsPlusReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.current_block >= self.total_blocks {
            return Ok(0);
        }

        let block_size = self.block_size as u64;
        let remaining_in_block = block_size - self.block_pos;
        let to_read = (remaining_in_block as usize).min(buf.len());

        let n = if self.is_block_allocated(self.current_block) {
            let offset =
                self.partition_offset + self.current_block as u64 * block_size + self.block_pos;
            self.reader.seek(SeekFrom::Start(offset))?;
            self.reader.read(&mut buf[..to_read])?
        } else {
            // Unallocated block — emit zeros so free space compresses to nothing.
            buf[..to_read].fill(0);
            to_read
        };

        self.block_pos += n as u64;
        if self.block_pos >= block_size {
            self.block_pos = 0;
            self.current_block += 1;
        }

        Ok(n)
    }
}

// --- Resize and validation ---

/// Resize an HFS+ filesystem in place.
pub fn resize_hfsplus_in_place(
    device: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    new_size_bytes: u64,
    log: &mut impl FnMut(&str),
) -> anyhow::Result<()> {
    // Read volume header
    device.seek(SeekFrom::Start(partition_offset + 1024))?;
    let mut vh_buf = [0u8; 512];
    device.read_exact(&mut vh_buf)?;

    let sig = BigEndian::read_u16(&vh_buf[0..2]);
    if sig != HFS_PLUS_SIGNATURE && sig != HFSX_SIGNATURE {
        log("HFS+ resize: not an HFS+ volume, skipping");
        return Ok(());
    }

    let block_size = BigEndian::read_u32(&vh_buf[40..44]);
    let old_total = BigEndian::read_u32(&vh_buf[44..48]);
    let old_free = BigEndian::read_u32(&vh_buf[48..52]);
    let used_blocks = old_total - old_free;

    let new_total = (new_size_bytes / block_size as u64) as u32;
    if new_total < used_blocks {
        anyhow::bail!(
            "HFS+ resize: new size {} blocks < used {} blocks",
            new_total,
            used_blocks
        );
    }

    let new_free = new_total - used_blocks;

    log(&format!(
        "HFS+ resize: {} -> {} blocks ({} free)",
        old_total, new_total, new_free
    ));

    // Update volume header fields
    BigEndian::write_u32(&mut vh_buf[44..48], new_total);
    BigEndian::write_u32(&mut vh_buf[48..52], new_free);

    // Write primary volume header at offset + 1024
    device.seek(SeekFrom::Start(partition_offset + 1024))?;
    device.write_all(&vh_buf)?;

    // Write backup volume header at offset + new_size - 1024
    if new_size_bytes > 1024 {
        device.seek(SeekFrom::Start(partition_offset + new_size_bytes - 1024))?;
        device.write_all(&vh_buf)?;
    }

    device.flush()?;
    Ok(())
}

/// Validate HFS+ filesystem integrity.
pub fn validate_hfsplus_integrity(
    device: &mut (impl Read + Seek),
    partition_offset: u64,
    log: &mut impl FnMut(&str),
) -> anyhow::Result<()> {
    device.seek(SeekFrom::Start(partition_offset + 1024))?;
    let mut vh_buf = [0u8; 512];
    device.read_exact(&mut vh_buf)?;

    let sig = BigEndian::read_u16(&vh_buf[0..2]);
    if sig != HFS_PLUS_SIGNATURE && sig != HFSX_SIGNATURE {
        log("HFS+ validate: not an HFS+ volume, skipping");
        return Ok(());
    }

    let block_size = BigEndian::read_u32(&vh_buf[40..44]);
    let total_blocks = BigEndian::read_u32(&vh_buf[44..48]);

    if !block_size.is_power_of_two() || block_size < 512 {
        anyhow::bail!("HFS+ validate: invalid block size {block_size}");
    }
    if total_blocks == 0 {
        anyhow::bail!("HFS+ validate: zero total blocks");
    }

    log(&format!(
        "HFS+ validate: OK ({total_blocks} blocks, {block_size} block size)"
    ));
    Ok(())
}

/// Patch HFS+ hidden sectors (no-op: HFS+ doesn't have a hidden sectors field).
pub fn patch_hfsplus_hidden_sectors(
    _device: &mut (impl Read + Seek),
    _partition_offset: u64,
    _new_lba: u64,
    _log: &mut impl FnMut(&str),
) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_volume_header_parse() {
        let mut data = [0u8; 512];
        BigEndian::write_u16(&mut data[0..2], HFS_PLUS_SIGNATURE);
        BigEndian::write_u16(&mut data[2..4], 4); // version
        BigEndian::write_u32(&mut data[40..44], 4096); // block_size
        BigEndian::write_u32(&mut data[44..48], 100000); // total_blocks
        BigEndian::write_u32(&mut data[48..52], 30000); // free_blocks

        let vh = HfsPlusVolumeHeader::parse(&data).unwrap();
        assert_eq!(vh.signature, HFS_PLUS_SIGNATURE);
        assert_eq!(vh.block_size, 4096);
        assert_eq!(vh.total_blocks, 100000);
        assert_eq!(vh.free_blocks, 30000);
        assert!(!vh.is_hfsx());
    }

    #[test]
    fn test_hfsx_signature() {
        let mut data = [0u8; 512];
        BigEndian::write_u16(&mut data[0..2], HFSX_SIGNATURE);
        BigEndian::write_u32(&mut data[40..44], 4096);
        BigEndian::write_u32(&mut data[44..48], 100000);
        BigEndian::write_u32(&mut data[48..52], 30000);

        let vh = HfsPlusVolumeHeader::parse(&data).unwrap();
        assert!(vh.is_hfsx());
    }

    #[test]
    fn test_decode_utf16be() {
        // "Hello" in UTF-16BE
        let data = [0x00, 0x48, 0x00, 0x65, 0x00, 0x6C, 0x00, 0x6C, 0x00, 0x6F];
        assert_eq!(decode_utf16be(&data), "Hello");
    }

    #[test]
    fn test_decode_utf16be_with_non_ascii() {
        // "Ä" (U+00C4) in UTF-16BE
        let data = [0x00, 0xC4];
        assert_eq!(decode_utf16be(&data), "Ä");
    }

    #[test]
    fn test_extent_descriptor() {
        let mut data = [0u8; 8];
        BigEndian::write_u32(&mut data[0..4], 100);
        BigEndian::write_u32(&mut data[4..8], 50);

        let ext = ExtentDescriptor::parse(&data);
        assert_eq!(ext.start_block, 100);
        assert_eq!(ext.block_count, 50);
        assert!(!ext.is_empty());
    }

    #[test]
    fn test_find_last_set_bit() {
        let bitmap = [0b10100000, 0b00001000]; // bits 0, 2, 12
        assert_eq!(find_last_set_bit(&bitmap, 16), Some(12));

        let empty = [0u8; 2];
        assert_eq!(find_last_set_bit(&empty, 16), None);
    }

    #[test]
    fn test_volume_header_serialize_roundtrip() {
        let mut data = [0u8; 512];
        BigEndian::write_u16(&mut data[0..2], HFS_PLUS_SIGNATURE);
        BigEndian::write_u16(&mut data[2..4], 4);
        BigEndian::write_u32(&mut data[40..44], 4096);
        BigEndian::write_u32(&mut data[44..48], 100);
        BigEndian::write_u32(&mut data[48..52], 50);
        BigEndian::write_u32(&mut data[64..68], 10); // next_catalog_id
        BigEndian::write_u32(&mut data[80..84], 42); // finder_info[0]

        let vh = HfsPlusVolumeHeader::parse(&data).unwrap();
        assert_eq!(vh.next_catalog_id, 10);
        assert_eq!(vh.finder_info[0], 42);

        let serialized = vh.serialize();
        let vh2 = HfsPlusVolumeHeader::parse(&serialized).unwrap();
        assert_eq!(vh2.block_size, 4096);
        assert_eq!(vh2.total_blocks, 100);
        assert_eq!(vh2.free_blocks, 50);
        assert_eq!(vh2.next_catalog_id, 10);
        assert_eq!(vh2.finder_info[0], 42);
    }

    #[test]
    fn test_fork_data_serialize_roundtrip() {
        let fork = ForkData {
            logical_size: 12345,
            clump_size: 0,
            total_blocks: 3,
            extents: [
                ExtentDescriptor {
                    start_block: 10,
                    block_count: 3,
                },
                ExtentDescriptor {
                    start_block: 0,
                    block_count: 0,
                },
                ExtentDescriptor {
                    start_block: 0,
                    block_count: 0,
                },
                ExtentDescriptor {
                    start_block: 0,
                    block_count: 0,
                },
                ExtentDescriptor {
                    start_block: 0,
                    block_count: 0,
                },
                ExtentDescriptor {
                    start_block: 0,
                    block_count: 0,
                },
                ExtentDescriptor {
                    start_block: 0,
                    block_count: 0,
                },
                ExtentDescriptor {
                    start_block: 0,
                    block_count: 0,
                },
            ],
        };
        let mut buf = [0u8; 80];
        fork.serialize(&mut buf);
        let fork2 = ForkData::parse(&buf);
        assert_eq!(fork2.logical_size, 12345);
        assert_eq!(fork2.total_blocks, 3);
        assert_eq!(fork2.extents[0].start_block, 10);
        assert_eq!(fork2.extents[0].block_count, 3);
    }

    /// Create a minimal valid in-memory HFS+ image for testing.
    /// Layout: 256 blocks × 4096 bytes = 1 MB
    /// - Block 0: unused (VH at byte 1024 within this block)
    /// - Block 1: allocation bitmap
    /// - Block 2-5: catalog B-tree (4 blocks = 16 KB = 4 nodes of 4096 bytes)
    ///   - Node 0: header node
    ///   - Node 1: leaf node with root folder record + thread
    ///   - Nodes 2-3: free
    /// - Blocks 6+: free for user data
    fn make_editable_hfsplus_image() -> Vec<u8> {
        let block_size = 4096u32;
        let total_blocks = 256u32;
        let image_size = total_blocks as usize * block_size as usize; // 1 MB
        let mut img = vec![0u8; image_size];

        // Allocation bitmap: blocks 0-5 allocated (VH area + bitmap + catalog)
        let bitmap_block = 1u32;
        let catalog_start_block = 2u32;
        let catalog_blocks = 4u32;
        let alloc_blocks = 1 + 1 + catalog_blocks as u32; // VH + bitmap + catalog = 6 blocks
        let bitmap_data = &mut img[bitmap_block as usize * block_size as usize
            ..(bitmap_block + 1) as usize * block_size as usize];
        // Set bits 0-5 (MSB-first): byte 0 = 0b11111100
        bitmap_data[0] = 0b11111100;

        // Build catalog B-tree in blocks 2-5 (4 nodes × 4096)
        let node_size = 4096usize;
        let catalog_offset = catalog_start_block as usize * block_size as usize;
        let catalog_size = catalog_blocks as usize * block_size as usize;

        // Node 0: Header node
        let hdr_off = catalog_offset;
        // Node descriptor: next=0, prev=0, kind=1(header), height=0, numRecords=3
        img[hdr_off + 8] = 1; // kind = header
        BigEndian::write_u16(&mut img[hdr_off + 10..hdr_off + 12], 3); // 3 records

        // B-tree header record (record 0, at offset 14)
        let hr = hdr_off + 14;
        BigEndian::write_u16(&mut img[hr..hr + 2], 1); // depth = 1
        BigEndian::write_u32(&mut img[hr + 2..hr + 6], 1); // root_node = 1
        BigEndian::write_u32(&mut img[hr + 6..hr + 10], 2); // leaf_records = 2 (folder + thread)
        BigEndian::write_u32(&mut img[hr + 10..hr + 14], 1); // first_leaf_node = 1
        BigEndian::write_u32(&mut img[hr + 14..hr + 18], 1); // last_leaf_node = 1
        BigEndian::write_u16(&mut img[hr + 18..hr + 20], node_size as u16); // node_size
        BigEndian::write_u16(&mut img[hr + 20..hr + 22], 516); // max_key_len
        BigEndian::write_u32(&mut img[hr + 22..hr + 26], 4); // total_nodes = 4
        BigEndian::write_u32(&mut img[hr + 26..hr + 30], 2); // free_nodes = 2

        // Record offsets for header node (3 records + free space offset)
        // Record 0: header record at offset 14
        // Record 1: user data record (128 bytes at offset 14+128=142)
        // Record 2: bitmap record (256 bytes at offset 142+128=270)
        // Free space offset at end
        let ot = hdr_off + node_size; // offset table at end of node
        BigEndian::write_u16(&mut img[ot - 2..ot], 14); // record 0 offset
        BigEndian::write_u16(&mut img[ot - 4..ot - 2], 142); // record 1 offset
        BigEndian::write_u16(&mut img[ot - 6..ot - 4], 270); // record 2 (bitmap)
        BigEndian::write_u16(&mut img[ot - 8..ot - 6], 526); // free space offset

        // Node bitmap (record 2): mark nodes 0 and 1 as allocated
        img[hdr_off + 270] = 0b11000000;

        // Node 1: Leaf node with root folder record + thread record
        let leaf_off = catalog_offset + node_size;
        // Node descriptor: next=0, prev=0, kind=-1(leaf), height=1, numRecords=2
        img[leaf_off + 8] = 0xFF; // kind = -1 (leaf)
        img[leaf_off + 9] = 1; // height = 1
        BigEndian::write_u16(&mut img[leaf_off + 10..leaf_off + 12], 2); // 2 records

        // Record 0: Root folder record (CNID 2)
        // Key: key_len(2) + parent_id(4, =1) + name_len(2, =0)  — root folder's parent is CNID 1
        let r0_off = leaf_off + 14;
        BigEndian::write_u16(&mut img[r0_off..r0_off + 2], 6); // key_len = 6
        BigEndian::write_u32(&mut img[r0_off + 2..r0_off + 6], 1); // parent_id = 1 (root parent)
        BigEndian::write_u16(&mut img[r0_off + 6..r0_off + 8], 0); // name_len = 0
                                                                   // Record data starts after key (at offset 8, even-aligned)
        let r0_data = r0_off + 8;
        BigEndian::write_i16(&mut img[r0_data..r0_data + 2], CATALOG_FOLDER); // type = folder
        BigEndian::write_u32(&mut img[r0_data + 8..r0_data + 12], 2); // folderID = 2
                                                                      // Folder record is 88 bytes total

        // Record 1: Thread record for root folder (CNID 2)
        // Thread key: parent_id = CNID = 2, name = empty
        let r1_off = r0_data + 88; // after folder record
        BigEndian::write_u16(&mut img[r1_off..r1_off + 2], 6); // key_len = 6
        BigEndian::write_u32(&mut img[r1_off + 2..r1_off + 6], 2); // parent_id = 2 (CNID)
        BigEndian::write_u16(&mut img[r1_off + 6..r1_off + 8], 0); // name_len = 0
                                                                   // Thread record data
        let r1_data = r1_off + 8;
        BigEndian::write_i16(&mut img[r1_data..r1_data + 2], 3); // type = folder thread
        BigEndian::write_u32(&mut img[r1_data + 4..r1_data + 8], 1); // parentID = 1
        BigEndian::write_u16(&mut img[r1_data + 8..r1_data + 10], 0); // name_len = 0
                                                                      // Thread record is variable length, but at minimum 10 bytes

        // Record offset table for leaf node (2 records + free space)
        let lot = leaf_off + node_size;
        BigEndian::write_u16(&mut img[lot - 2..lot], 14); // record 0 offset
        let r1_rel = (r1_off - leaf_off) as u16;
        BigEndian::write_u16(&mut img[lot - 4..lot - 2], r1_rel); // record 1 offset
        let free_rel = (r1_data + 10 - leaf_off) as u16;
        BigEndian::write_u16(&mut img[lot - 6..lot - 4], free_rel); // free space offset

        // Volume Header at byte 1024
        let mut vh = HfsPlusVolumeHeader {
            signature: HFS_PLUS_SIGNATURE,
            version: 4,
            attributes: 0,
            last_mounted_version: 0,
            journal_info_block: 0,
            create_date: hfs_common::hfs_now(),
            modify_date: hfs_common::hfs_now(),
            backup_date: 0,
            checked_date: 0,
            file_count: 0,
            folder_count: 1, // root folder
            block_size,
            total_blocks,
            free_blocks: total_blocks - alloc_blocks,
            next_allocation: alloc_blocks,
            rsrc_clump_size: 0,
            data_clump_size: 0,
            next_catalog_id: 16, // next available CNID
            write_count: 0,
            encodings_bitmap: 0,
            finder_info: [0u32; 8],
            allocation_file: ForkData {
                logical_size: block_size as u64,
                clump_size: 0,
                total_blocks: 1,
                extents: {
                    let mut e = [ExtentDescriptor {
                        start_block: 0,
                        block_count: 0,
                    }; 8];
                    e[0] = ExtentDescriptor {
                        start_block: bitmap_block,
                        block_count: 1,
                    };
                    e
                },
            },
            extents_file: ForkData::empty(),
            catalog_file: ForkData {
                logical_size: catalog_size as u64,
                clump_size: 0,
                total_blocks: catalog_blocks,
                extents: {
                    let mut e = [ExtentDescriptor {
                        start_block: 0,
                        block_count: 0,
                    }; 8];
                    e[0] = ExtentDescriptor {
                        start_block: catalog_start_block,
                        block_count: catalog_blocks,
                    };
                    e
                },
            },
            attributes_file: ForkData::empty(),
            startup_file: ForkData::empty(),
        };

        let vh_bytes = vh.serialize();
        img[1024..1024 + 512].copy_from_slice(&vh_bytes);

        // Backup VH at last 1024 bytes
        let backup_pos = image_size - 1024;
        img[backup_pos..backup_pos + 512].copy_from_slice(&vh_bytes);

        img
    }

    #[test]
    fn test_hfsplus_editable_open_and_free_space() {
        let img = make_editable_hfsplus_image();
        let cursor = std::io::Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).unwrap();
        assert_eq!(fs.fs_type(), "HFS+");
        let free = fs.free_space().unwrap();
        assert!(free > 0);
        // 250 free blocks × 4096 = 1,024,000
        assert_eq!(free, 250 * 4096);
    }

    #[test]
    fn test_hfsplus_editable_create_file_and_read() {
        let img = make_editable_hfsplus_image();
        let cursor = std::io::Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        let test_data = b"Hello, HFS+ World!";
        let mut data_reader = std::io::Cursor::new(test_data.as_slice());

        let options = CreateFileOptions::default();
        let fe = fs
            .create_file(
                &root,
                "test.txt",
                &mut data_reader,
                test_data.len() as u64,
                &options,
            )
            .unwrap();

        assert_eq!(fe.name, "test.txt");
        assert_eq!(fe.size, test_data.len() as u64);

        // Verify we can list and find it
        let entries = fs.list_directory(&root).unwrap();
        assert!(entries.iter().any(|e| e.name == "test.txt"));

        // Verify we can read it back
        let read_back = fs.read_file(&fe, 1024).unwrap();
        assert_eq!(&read_back, test_data);
    }

    #[test]
    fn test_hfsplus_editable_create_directory() {
        let img = make_editable_hfsplus_image();
        let cursor = std::io::Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        let options = CreateDirectoryOptions::default();
        let dir = fs.create_directory(&root, "TestDir", &options).unwrap();

        assert_eq!(dir.name, "TestDir");
        assert!(dir.is_directory());

        let entries = fs.list_directory(&root).unwrap();
        assert!(entries
            .iter()
            .any(|e| e.name == "TestDir" && e.is_directory()));
    }

    #[test]
    fn test_hfsplus_editable_delete_file() {
        let img = make_editable_hfsplus_image();
        let cursor = std::io::Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        let test_data = b"delete me";
        let mut data_reader = std::io::Cursor::new(test_data.as_slice());
        let options = CreateFileOptions::default();
        let fe = fs
            .create_file(
                &root,
                "gone.txt",
                &mut data_reader,
                test_data.len() as u64,
                &options,
            )
            .unwrap();

        let free_before = fs.free_space().unwrap();

        // Delete it
        fs.delete_entry(&root, &fe).unwrap();

        // Verify it's gone
        let entries = fs.list_directory(&root).unwrap();
        assert!(!entries.iter().any(|e| e.name == "gone.txt"));

        // Verify free space recovered
        let free_after = fs.free_space().unwrap();
        assert!(free_after > free_before);
    }

    #[test]
    fn test_hfsplus_editable_duplicate_name_rejected() {
        let img = make_editable_hfsplus_image();
        let cursor = std::io::Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        let test_data = b"first";
        let mut r1 = std::io::Cursor::new(test_data.as_slice());
        let options = CreateFileOptions::default();
        fs.create_file(&root, "dup.txt", &mut r1, 5, &options)
            .unwrap();

        // Second creation should fail
        let mut r2 = std::io::Cursor::new(test_data.as_slice());
        let result = fs.create_file(&root, "dup.txt", &mut r2, 5, &options);
        assert!(matches!(result, Err(FilesystemError::AlreadyExists(_))));
    }

    #[test]
    fn test_hfsplus_editable_blessed_folder() {
        let img = make_editable_hfsplus_image();
        let cursor = std::io::Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).unwrap();

        // Initially no blessed folder
        assert!(fs.blessed_system_folder().is_none());

        // Create a system folder
        let root = fs.root().unwrap();
        let options = CreateDirectoryOptions::default();
        let sys_dir = fs
            .create_directory(&root, "System Folder", &options)
            .unwrap();

        // Bless it
        fs.set_blessed_folder(&sys_dir).unwrap();

        // Verify it's blessed
        let blessed = fs.blessed_system_folder();
        assert!(blessed.is_some());
        let (cnid, name) = blessed.unwrap();
        assert_eq!(cnid, sys_dir.location);
        assert_eq!(name, "System Folder");
    }

    #[test]
    fn test_hfsplus_editable_type_creator_auto_detect() {
        let img = make_editable_hfsplus_image();
        let cursor = std::io::Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        let test_data = b"text content";
        let mut data_reader = std::io::Cursor::new(test_data.as_slice());
        let options = CreateFileOptions::default();
        let fe = fs
            .create_file(
                &root,
                "hello.txt",
                &mut data_reader,
                test_data.len() as u64,
                &options,
            )
            .unwrap();

        // "txt" extension should auto-detect to TEXT/ttxt
        assert!(fe.type_code.is_some());
        assert_eq!(fe.type_code.as_deref(), Some("TEXT"));
    }
}
