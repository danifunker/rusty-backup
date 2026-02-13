use byteorder::{BigEndian, ByteOrder};
use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{Filesystem, FilesystemError};
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
}

/// HFS+ fork data (80 bytes).
#[derive(Debug, Clone)]
struct ForkData {
    logical_size: u64,
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
}

/// HFS+ Volume Header (512 bytes at partition_offset + 1024).
#[derive(Debug)]
#[allow(dead_code)]
struct HfsPlusVolumeHeader {
    signature: u16,
    version: u16,
    attributes: u32,
    block_size: u32,
    total_blocks: u32,
    free_blocks: u32,
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

        Ok(HfsPlusVolumeHeader {
            signature: sig,
            version: BigEndian::read_u16(&data[2..4]),
            attributes: BigEndian::read_u32(&data[4..8]),
            block_size: BigEndian::read_u32(&data[40..44]),
            total_blocks: BigEndian::read_u32(&data[44..48]),
            free_blocks: BigEndian::read_u32(&data[48..52]),
            allocation_file: ForkData::parse(&data[112..192]),
            extents_file: ForkData::parse(&data[192..272]),
            catalog_file: ForkData::parse(&data[272..352]),
            attributes_file: ForkData::parse(&data[352..432]),
            startup_file: ForkData::parse(&data[432..512]),
        })
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
        #[allow(dead_code)]
        data_fork: ForkData,
        type_code: String,
        creator_code: String,
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
                        // FileInfo (Finder Info) at offset 48: fdType(4) + fdCreator(4)
                        let type_code = decode_fourcc(&rec[48..52]);
                        let creator_code = decode_fourcc(&rec[52..56]);
                        // Data fork at offset 88 (80 bytes)
                        let data_fork = ForkData::parse(&rec[88..168]);
                        results.push(CatalogEntry::File {
                            file_id,
                            name,
                            data_size: data_fork.logical_size,
                            data_fork,
                            type_code,
                            creator_code,
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
    fn find_file_by_id(&self, file_id: u32) -> Option<ForkData> {
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
                return Some(ForkData::parse(&rec[88..168]));
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
                    type_code,
                    creator_code,
                    ..
                } => {
                    let path = if entry.path == "/" {
                        format!("/{name}")
                    } else {
                        format!("{}/{name}", entry.path)
                    };
                    let mut fe = FileEntry::new_file(name, path, data_size, file_id as u64);
                    fe.type_code = Some(type_code);
                    fe.creator_code = Some(creator_code);
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
        let fork = self.find_file_by_id(file_id).ok_or_else(|| {
            FilesystemError::NotFound(format!("file id {file_id} not found in catalog"))
        })?;

        let mut data = read_fork(
            &mut self.reader,
            self.partition_offset,
            self.vh.block_size,
            &fork,
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

    fn total_size(&self) -> u64 {
        self.vh.total_blocks as u64 * self.vh.block_size as u64
    }

    fn used_size(&self) -> u64 {
        (self.vh.total_blocks - self.vh.free_blocks) as u64 * self.vh.block_size as u64
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        let bitmap = self.read_allocation_bitmap()?;
        let last = find_last_set_bit(&bitmap, self.vh.total_blocks);
        match last {
            Some(block) => {
                let byte = (block as u64 + 1) * self.vh.block_size as u64;
                Ok(byte)
            }
            None => Ok(self.total_size()),
        }
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

// --- Compact reader ---

/// Compact reader for HFS+: streams boot region + allocated blocks only.
pub struct CompactHfsPlusReader<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    block_size: u32,
    total_blocks: u32,
    bitmap: Vec<u8>,
    /// Current phase: 0=boot region (3 sectors), 1=allocated blocks, 2=done
    phase: u8,
    phase_pos: u64,
    current_block: u32,
    #[allow(dead_code)]
    compacted_size: u64,
    #[allow(dead_code)]
    original_size: u64,
}

impl<R: Read + Seek> CompactHfsPlusReader<R> {
    pub fn new(
        mut reader: R,
        partition_offset: u64,
    ) -> Result<(Self, CompactResult), FilesystemError> {
        // Read volume header
        reader.seek(SeekFrom::Start(partition_offset + 1024))?;
        let mut vh_buf = [0u8; 512];
        reader.read_exact(&mut vh_buf)?;
        let vh = HfsPlusVolumeHeader::parse(&vh_buf)?;

        // Read allocation file
        let alloc_data = read_fork(
            &mut reader,
            partition_offset,
            vh.block_size,
            &vh.allocation_file,
        )?;

        // Count allocated blocks
        let mut allocated = 0u32;
        for bit in 0..vh.total_blocks {
            let byte_idx = bit as usize / 8;
            let bit_idx = 7 - (bit % 8);
            if byte_idx < alloc_data.len() && (alloc_data[byte_idx] >> bit_idx) & 1 == 1 {
                allocated += 1;
            }
        }

        let boot_region = 3 * 512u64; // boot blocks + volume header
        let original_size = vh.total_blocks as u64 * vh.block_size as u64;
        let compacted_size = boot_region + allocated as u64 * vh.block_size as u64;

        let result = CompactResult {
            original_size,
            compacted_size,
            clusters_used: allocated,
        };

        Ok((
            CompactHfsPlusReader {
                reader,
                partition_offset,
                block_size: vh.block_size,
                total_blocks: vh.total_blocks,
                bitmap: alloc_data,
                phase: 0,
                phase_pos: 0,
                current_block: 0,
                compacted_size,
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
        if self.phase > 1 {
            return Ok(0);
        }

        let boot_region = 3 * 512u64;

        if self.phase == 0 {
            let remaining = boot_region - self.phase_pos;
            if remaining == 0 {
                self.phase = 1;
                self.phase_pos = 0;
                self.current_block = 0;
                return self.read(buf);
            }
            let to_read = (remaining as usize).min(buf.len());
            let offset = self.partition_offset + self.phase_pos;
            self.reader.seek(SeekFrom::Start(offset))?;
            let n = self.reader.read(&mut buf[..to_read])?;
            self.phase_pos += n as u64;
            return Ok(n);
        }

        // Phase 1: allocated blocks
        let block_size = self.block_size as u64;

        while self.current_block < self.total_blocks && !self.is_block_allocated(self.current_block)
        {
            self.current_block += 1;
        }
        if self.current_block >= self.total_blocks {
            self.phase = 2;
            return Ok(0);
        }

        let block_offset = self.partition_offset + self.current_block as u64 * block_size;
        let offset_in_block = self.phase_pos % block_size;
        let remaining_in_block = block_size - offset_in_block;
        let to_read = (remaining_in_block as usize).min(buf.len());

        self.reader
            .seek(SeekFrom::Start(block_offset + offset_in_block))?;
        let n = self.reader.read(&mut buf[..to_read])?;
        self.phase_pos += n as u64;

        if self.phase_pos % block_size == 0 {
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
}
