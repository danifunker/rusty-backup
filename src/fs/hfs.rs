use byteorder::{BigEndian, ByteOrder};
use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{Filesystem, FilesystemError};
use super::CompactResult;

const HFS_SIGNATURE: u16 = 0x4244;
const HFS_PLUS_EMBEDDED_SIGNATURE: u16 = 0x482B;

/// Mac Roman to Unicode lookup table for bytes 0x80-0xFF.
static MAC_ROMAN_TABLE: [char; 128] = [
    '\u{00C4}', '\u{00C5}', '\u{00C7}', '\u{00C9}', '\u{00D1}', '\u{00D6}', '\u{00DC}', '\u{00E1}',
    '\u{00E0}', '\u{00E2}', '\u{00E4}', '\u{00E3}', '\u{00E5}', '\u{00E7}', '\u{00E9}', '\u{00E8}',
    '\u{00EA}', '\u{00EB}', '\u{00ED}', '\u{00EC}', '\u{00EE}', '\u{00EF}', '\u{00F1}', '\u{00F3}',
    '\u{00F2}', '\u{00F4}', '\u{00F6}', '\u{00F5}', '\u{00FA}', '\u{00F9}', '\u{00FB}', '\u{00FC}',
    '\u{2020}', '\u{00B0}', '\u{00A2}', '\u{00A3}', '\u{00A7}', '\u{2022}', '\u{00B6}', '\u{00DF}',
    '\u{00AE}', '\u{00A9}', '\u{2122}', '\u{00B4}', '\u{00A8}', '\u{2260}', '\u{00C6}', '\u{00D8}',
    '\u{221E}', '\u{00B1}', '\u{2264}', '\u{2265}', '\u{00A5}', '\u{00B5}', '\u{2202}', '\u{2211}',
    '\u{220F}', '\u{03C0}', '\u{222B}', '\u{00AA}', '\u{00BA}', '\u{03A9}', '\u{00E6}', '\u{00F8}',
    '\u{00BF}', '\u{00A1}', '\u{00AC}', '\u{221A}', '\u{0192}', '\u{2248}', '\u{2206}', '\u{00AB}',
    '\u{00BB}', '\u{2026}', '\u{00A0}', '\u{00C0}', '\u{00C3}', '\u{00D5}', '\u{0152}', '\u{0153}',
    '\u{2013}', '\u{2014}', '\u{201C}', '\u{201D}', '\u{2018}', '\u{2019}', '\u{00F7}', '\u{25CA}',
    '\u{00FF}', '\u{0178}', '\u{2044}', '\u{20AC}', '\u{2039}', '\u{203A}', '\u{FB01}', '\u{FB02}',
    '\u{2021}', '\u{00B7}', '\u{201A}', '\u{201E}', '\u{2030}', '\u{00C2}', '\u{00CA}', '\u{00C1}',
    '\u{00CB}', '\u{00C8}', '\u{00CD}', '\u{00CE}', '\u{00CF}', '\u{00CC}', '\u{00D3}', '\u{00D4}',
    '\u{F8FF}', '\u{00D2}', '\u{00DA}', '\u{00DB}', '\u{00D9}', '\u{0131}', '\u{02C6}', '\u{02DC}',
    '\u{00AF}', '\u{02D8}', '\u{02D9}', '\u{02DA}', '\u{00B8}', '\u{02DD}', '\u{02DB}', '\u{02C7}',
];

/// Decode a Mac Roman byte string to UTF-8.
pub fn mac_roman_to_utf8(data: &[u8]) -> String {
    data.iter()
        .map(|&b| {
            if b < 0x80 {
                b as char
            } else {
                MAC_ROMAN_TABLE[(b - 0x80) as usize]
            }
        })
        .collect()
}

/// HFS extent descriptor: start_block (u16) + block_count (u16).
#[derive(Debug, Clone, Copy)]
struct HfsExtDescriptor {
    start_block: u16,
    block_count: u16,
}

impl HfsExtDescriptor {
    fn parse(data: &[u8]) -> Self {
        HfsExtDescriptor {
            start_block: BigEndian::read_u16(&data[0..2]),
            block_count: BigEndian::read_u16(&data[2..4]),
        }
    }
}

/// HFS Master Directory Block (MDB) — at partition_offset + 1024.
#[derive(Debug)]
#[allow(dead_code)]
struct HfsMasterDirectoryBlock {
    signature: u16,
    create_date: u32,
    modify_date: u32,
    total_blocks: u16,
    block_size: u32,
    free_blocks: u16,
    volume_name: String,
    volume_bitmap_block: u16,
    /// First allocation block's offset in 512-byte sectors from partition start.
    first_alloc_block: u16,
    catalog_file_size: u32,
    catalog_file_extents: [HfsExtDescriptor; 3],
    extents_file_size: u32,
    extents_file_extents: [HfsExtDescriptor; 3],
    embedded_signature: u16,
    embedded_start_block: u16,
    embedded_block_count: u16,
}

impl HfsMasterDirectoryBlock {
    fn parse(data: &[u8]) -> Result<Self, FilesystemError> {
        if data.len() < 162 {
            return Err(FilesystemError::Parse("MDB too short".into()));
        }
        let sig = BigEndian::read_u16(&data[0..2]);
        if sig != HFS_SIGNATURE {
            return Err(FilesystemError::Parse(format!(
                "bad MDB signature: 0x{sig:04X}"
            )));
        }

        // Volume name: Pascal string at offset 36 (length byte + up to 27 chars)
        let name_len = data[36] as usize;
        let name_bytes = &data[37..37 + name_len.min(27)];
        let volume_name = mac_roman_to_utf8(name_bytes);

        let mut extents_extents = [HfsExtDescriptor {
            start_block: 0,
            block_count: 0,
        }; 3];
        for i in 0..3 {
            extents_extents[i] = HfsExtDescriptor::parse(&data[134 + i * 4..138 + i * 4]);
        }

        let mut catalog_extents = [HfsExtDescriptor {
            start_block: 0,
            block_count: 0,
        }; 3];
        for i in 0..3 {
            catalog_extents[i] = HfsExtDescriptor::parse(&data[150 + i * 4..154 + i * 4]);
        }

        // Embedded HFS+ info at offsets 124-130
        let embedded_sig = BigEndian::read_u16(&data[124..126]);
        let embedded_start = BigEndian::read_u16(&data[126..128]);
        let embedded_count = BigEndian::read_u16(&data[128..130]);

        Ok(HfsMasterDirectoryBlock {
            signature: sig,
            create_date: BigEndian::read_u32(&data[2..6]),
            modify_date: BigEndian::read_u32(&data[6..10]),
            total_blocks: BigEndian::read_u16(&data[18..20]),
            block_size: BigEndian::read_u32(&data[20..24]),
            free_blocks: BigEndian::read_u16(&data[34..36]),
            volume_name,
            volume_bitmap_block: BigEndian::read_u16(&data[14..16]),
            first_alloc_block: BigEndian::read_u16(&data[28..30]),
            extents_file_size: BigEndian::read_u32(&data[130..134]),
            extents_file_extents: extents_extents,
            catalog_file_size: BigEndian::read_u32(&data[146..150]),
            catalog_file_extents: catalog_extents,
            embedded_signature: embedded_sig,
            embedded_start_block: embedded_start,
            embedded_block_count: embedded_count,
        })
    }

    /// True if this MDB wraps an embedded HFS+ volume.
    fn has_embedded_hfs_plus(&self) -> bool {
        self.embedded_signature == HFS_PLUS_EMBEDDED_SIGNATURE
    }
}

/// Catalog record types.
const CATALOG_DIR: i8 = 1;
const CATALOG_FILE: i8 = 2;

/// Decode a 4-byte Mac OS type/creator code to a string.
/// Non-printable bytes are replaced with '.'.
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

/// A parsed HFS catalog record.
#[derive(Debug, Clone)]
#[allow(dead_code)]
enum CatalogRecord {
    Directory {
        dir_id: u32,
        name: String,
        parent_id: u32,
    },
    File {
        file_id: u32,
        name: String,
        parent_id: u32,
        data_size: u32,
        data_extents: [HfsExtDescriptor; 3],
        rsrc_size: u32,
        rsrc_extents: [HfsExtDescriptor; 3],
        type_code: String,
        creator_code: String,
    },
}

/// Classic HFS filesystem implementation.
pub struct HfsFilesystem<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    mdb: HfsMasterDirectoryBlock,
    /// Cached catalog file data.
    catalog_data: Vec<u8>,
}

impl<R: Read + Seek> HfsFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        // Read MDB at offset + 1024 (sector 2)
        reader.seek(SeekFrom::Start(partition_offset + 1024))?;
        let mut mdb_buf = [0u8; 162];
        reader.read_exact(&mut mdb_buf)?;
        let mdb = HfsMasterDirectoryBlock::parse(&mdb_buf)?;

        if mdb.has_embedded_hfs_plus() {
            return Err(FilesystemError::Unsupported(
                "this HFS volume contains an embedded HFS+ volume; use HFS+ reader instead".into(),
            ));
        }

        // Read the catalog file
        let catalog_data = read_fork_data(
            &mut reader,
            partition_offset,
            &mdb,
            &mdb.catalog_file_extents,
            mdb.catalog_file_size as u64,
        )?;

        Ok(HfsFilesystem {
            reader,
            partition_offset,
            mdb,
            catalog_data,
        })
    }

    /// List all catalog records with a given parent_id.
    fn list_children(&self, parent_id: u32) -> Result<Vec<CatalogRecord>, FilesystemError> {
        if self.catalog_data.len() < 512 {
            return Ok(vec![]);
        }

        // Read B-tree header from node 0
        // Node descriptor: 14 bytes, then BTHeaderRec starts
        // BTHeaderRec: treeDepth(2) + rootNode(4) + leafRecords(4) + firstLeafNode(4) + ...
        let node_size = BigEndian::read_u16(&self.catalog_data[32..34]) as u32;
        if node_size == 0 || self.catalog_data.len() < node_size as usize {
            return Ok(vec![]);
        }
        let header_node = &self.catalog_data[0..node_size as usize];
        let first_leaf = BigEndian::read_u32(&header_node[24..28]);

        let mut results = Vec::new();
        let mut node_idx = first_leaf;

        while node_idx != 0 {
            let offset = node_idx as usize * node_size as usize;
            if offset + node_size as usize > self.catalog_data.len() {
                break;
            }
            let node = &self.catalog_data[offset..offset + node_size as usize];

            // Node descriptor
            let next_node = BigEndian::read_u32(&node[0..4]);
            let _kind = node[8] as i8; // should be -1 for leaf
            let num_records = BigEndian::read_u16(&node[10..12]);

            for i in 0..num_records as usize {
                // Record offset is stored at end of node, growing backward
                let offset_pos = node_size as usize - 2 * (i + 1);
                if offset_pos + 2 > node.len() {
                    break;
                }
                let rec_offset = BigEndian::read_u16(&node[offset_pos..offset_pos + 2]) as usize;
                if rec_offset + 6 > node.len() {
                    continue;
                }

                // Parse catalog key
                let key_len = node[rec_offset] as usize;
                if key_len < 6 || rec_offset + 1 + key_len > node.len() {
                    continue;
                }
                let key_data = &node[rec_offset + 1..rec_offset + 1 + key_len];
                // key_data: reserved(1) + parent_id(4) + name_len(1) + name(N)
                let rec_parent_id = BigEndian::read_u32(&key_data[1..5]);
                let name_len = key_data[5] as usize;
                let name = if name_len > 0 && 6 + name_len <= key_data.len() {
                    mac_roman_to_utf8(&key_data[6..6 + name_len])
                } else {
                    String::new()
                };

                // Record data follows key (aligned to even boundary)
                let mut rec_data_offset = rec_offset + 1 + key_len;
                if rec_data_offset % 2 != 0 {
                    rec_data_offset += 1;
                }
                if rec_data_offset + 2 > node.len() {
                    continue;
                }

                let record_type = node[rec_data_offset] as i8;

                if rec_parent_id != parent_id {
                    continue;
                }

                match record_type {
                    CATALOG_DIR => {
                        if rec_data_offset + 70 > node.len() {
                            continue;
                        }
                        let dir_id =
                            BigEndian::read_u32(&node[rec_data_offset + 6..rec_data_offset + 10]);
                        results.push(CatalogRecord::Directory {
                            dir_id,
                            name,
                            parent_id: rec_parent_id,
                        });
                    }
                    CATALOG_FILE => {
                        if rec_data_offset + 102 > node.len() {
                            continue;
                        }
                        let rec = &node[rec_data_offset..];
                        // Finder Info (FInfo) at offset 4: fdType(4) + fdCreator(4)
                        let type_code = decode_fourcc(&rec[4..8]);
                        let creator_code = decode_fourcc(&rec[8..12]);
                        // File ID (filFlNum) at offset 20
                        let file_id = BigEndian::read_u32(&rec[20..24]);
                        // Data fork: logical size at offset 26, first 3 extents at 74
                        let data_size = BigEndian::read_u32(&rec[26..30]);
                        let mut data_extents = [HfsExtDescriptor {
                            start_block: 0,
                            block_count: 0,
                        }; 3];
                        for j in 0..3 {
                            data_extents[j] = HfsExtDescriptor::parse(&rec[74 + j * 4..78 + j * 4]);
                        }
                        // Resource fork: logical size at offset 36, extents at 86
                        let rsrc_size = BigEndian::read_u32(&rec[36..40]);
                        let mut rsrc_extents = [HfsExtDescriptor {
                            start_block: 0,
                            block_count: 0,
                        }; 3];
                        for j in 0..3 {
                            rsrc_extents[j] = HfsExtDescriptor::parse(&rec[86 + j * 4..90 + j * 4]);
                        }
                        results.push(CatalogRecord::File {
                            file_id,
                            name,
                            parent_id: rec_parent_id,
                            data_size,
                            data_extents,
                            rsrc_size,
                            rsrc_extents,
                            type_code,
                            creator_code,
                        });
                    }
                    _ => {}
                }
            }

            node_idx = next_node;
        }

        Ok(results)
    }

    /// Find a file record by its file_id (CNID) in the catalog B-tree.
    /// Returns (data_size, data_extents, rsrc_size, rsrc_extents).
    fn find_file_by_id(
        &self,
        file_id: u32,
    ) -> Option<(u32, [HfsExtDescriptor; 3], u32, [HfsExtDescriptor; 3])> {
        if self.catalog_data.len() < 512 {
            return None;
        }
        let node_size = BigEndian::read_u16(&self.catalog_data[32..34]) as u32;
        if node_size == 0 || self.catalog_data.len() < node_size as usize {
            return None;
        }
        let header_node = &self.catalog_data[0..node_size as usize];
        let first_leaf = BigEndian::read_u32(&header_node[24..28]);

        let mut node_idx = first_leaf;
        while node_idx != 0 {
            let offset = node_idx as usize * node_size as usize;
            if offset + node_size as usize > self.catalog_data.len() {
                break;
            }
            let node = &self.catalog_data[offset..offset + node_size as usize];
            let next_node = BigEndian::read_u32(&node[0..4]);
            let num_records = BigEndian::read_u16(&node[10..12]);

            for i in 0..num_records as usize {
                let offset_pos = node_size as usize - 2 * (i + 1);
                if offset_pos + 2 > node.len() {
                    break;
                }
                let rec_offset = BigEndian::read_u16(&node[offset_pos..offset_pos + 2]) as usize;
                if rec_offset + 6 > node.len() {
                    continue;
                }
                let key_len = node[rec_offset] as usize;
                if key_len < 6 || rec_offset + 1 + key_len > node.len() {
                    continue;
                }
                let mut rec_data_offset = rec_offset + 1 + key_len;
                if rec_data_offset % 2 != 0 {
                    rec_data_offset += 1;
                }
                if rec_data_offset + 2 > node.len() {
                    continue;
                }
                let record_type = node[rec_data_offset] as i8;
                if record_type != CATALOG_FILE {
                    continue;
                }
                if rec_data_offset + 102 > node.len() {
                    continue;
                }
                let rec = &node[rec_data_offset..];
                // File ID (filFlNum) at offset 20
                let rec_file_id = BigEndian::read_u32(&rec[20..24]);
                if rec_file_id != file_id {
                    continue;
                }
                // Data fork: logical size at offset 26, extents at 74
                let data_size = BigEndian::read_u32(&rec[26..30]);
                let mut data_extents = [HfsExtDescriptor {
                    start_block: 0,
                    block_count: 0,
                }; 3];
                for j in 0..3 {
                    data_extents[j] = HfsExtDescriptor::parse(&rec[74 + j * 4..78 + j * 4]);
                }
                // Resource fork: logical size at offset 36, extents at 86
                let rsrc_size = BigEndian::read_u32(&rec[36..40]);
                let mut rsrc_extents = [HfsExtDescriptor {
                    start_block: 0,
                    block_count: 0,
                }; 3];
                for j in 0..3 {
                    rsrc_extents[j] = HfsExtDescriptor::parse(&rec[86 + j * 4..90 + j * 4]);
                }
                return Some((data_size, data_extents, rsrc_size, rsrc_extents));
            }
            node_idx = next_node;
        }
        None
    }

    /// Read the volume bitmap and return it.
    fn read_volume_bitmap(&mut self) -> Result<Vec<u8>, FilesystemError> {
        let bitmap_offset = self.partition_offset + self.mdb.volume_bitmap_block as u64 * 512;
        let bitmap_size = (self.mdb.total_blocks as u32).div_ceil(8) as usize;
        self.reader.seek(SeekFrom::Start(bitmap_offset))?;
        let mut bitmap = vec![0u8; bitmap_size];
        self.reader.read_exact(&mut bitmap)?;
        Ok(bitmap)
    }

    /// Return the byte offset of allocation block `n` from partition start.
    fn alloc_block_offset(&self, block: u32) -> u64 {
        self.mdb.first_alloc_block as u64 * 512 + block as u64 * self.mdb.block_size as u64
    }
}

impl<R: Read + Seek + Send> Filesystem for HfsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry {
            name: "/".into(),
            path: "/".into(),
            entry_type: EntryType::Directory,
            size: 0,
            location: 2, // HFS root directory CNID
            modified: None,
            type_code: None,
            creator_code: None,
            symlink_target: None,
            special_type: None,
            mode: None,
            uid: None,
            gid: None,
            resource_fork_size: None,
        })
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        let parent_id = entry.location as u32;
        let children = self.list_children(parent_id)?;

        let mut entries = Vec::new();
        for child in children {
            match child {
                CatalogRecord::Directory { dir_id, name, .. } => {
                    let path = if entry.path == "/" {
                        format!("/{name}")
                    } else {
                        format!("{}/{name}", entry.path)
                    };
                    entries.push(FileEntry::new_directory(name, path, dir_id as u64));
                }
                CatalogRecord::File {
                    file_id,
                    name,
                    data_size,
                    rsrc_size,
                    type_code,
                    creator_code,
                    ..
                } => {
                    let path = if entry.path == "/" {
                        format!("/{name}")
                    } else {
                        format!("{}/{name}", entry.path)
                    };
                    let mut fe = FileEntry::new_file(name, path, data_size as u64, file_id as u64);
                    fe.type_code = Some(type_code);
                    fe.creator_code = Some(creator_code);
                    if rsrc_size > 0 {
                        fe.resource_fork_size = Some(rsrc_size as u64);
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
        let (data_size, extents, _rsrc_size, _rsrc_extents) =
            self.find_file_by_id(file_id).ok_or_else(|| {
                FilesystemError::NotFound(format!("file id {file_id} not found in catalog"))
            })?;

        let mut data = read_fork_data(
            &mut self.reader,
            self.partition_offset,
            &self.mdb,
            &extents,
            data_size as u64,
        )?;
        data.truncate(max_bytes);
        Ok(data)
    }

    fn volume_label(&self) -> Option<&str> {
        if self.mdb.volume_name.is_empty() {
            None
        } else {
            Some(&self.mdb.volume_name)
        }
    }

    fn fs_type(&self) -> &str {
        "HFS"
    }

    fn total_size(&self) -> u64 {
        self.mdb.total_blocks as u64 * self.mdb.block_size as u64
    }

    fn used_size(&self) -> u64 {
        (self.mdb.total_blocks - self.mdb.free_blocks) as u64 * self.mdb.block_size as u64
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        let bitmap = self.read_volume_bitmap()?;
        let last_block = find_last_set_bit(&bitmap, self.mdb.total_blocks as u32);
        match last_block {
            Some(block) => {
                let byte = self.alloc_block_offset(block + 1);
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
        let (_data_size, _data_ext, rsrc_size, rsrc_extents) =
            self.find_file_by_id(file_id).ok_or_else(|| {
                FilesystemError::NotFound(format!("file id {file_id} not found in catalog"))
            })?;
        if rsrc_size == 0 {
            return Ok(0);
        }
        let data = read_fork_data(
            &mut self.reader,
            self.partition_offset,
            &self.mdb,
            &rsrc_extents,
            rsrc_size as u64,
        )?;
        writer.write_all(&data)?;
        Ok(data.len() as u64)
    }

    fn resource_fork_size(&mut self, entry: &FileEntry) -> u64 {
        let file_id = entry.location as u32;
        self.find_file_by_id(file_id)
            .map(|(_ds, _de, rs, _re)| rs as u64)
            .unwrap_or(0)
    }
}

/// Read fork data from the 3-extent descriptor array in the MDB.
fn read_fork_data<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    mdb: &HfsMasterDirectoryBlock,
    extents: &[HfsExtDescriptor; 3],
    size: u64,
) -> Result<Vec<u8>, FilesystemError> {
    let mut data = Vec::with_capacity(size as usize);
    let first_alloc_offset = partition_offset + mdb.first_alloc_block as u64 * 512;

    for ext in extents {
        if ext.block_count == 0 {
            break;
        }
        let offset = first_alloc_offset + ext.start_block as u64 * mdb.block_size as u64;
        let len = ext.block_count as u64 * mdb.block_size as u64;
        reader.seek(SeekFrom::Start(offset))?;
        let read_len = len.min(size - data.len() as u64) as usize;
        let mut buf = vec![0u8; read_len];
        reader.read_exact(&mut buf)?;
        data.extend_from_slice(&buf);
        if data.len() as u64 >= size {
            break;
        }
    }

    data.truncate(size as usize);
    Ok(data)
}

/// Find the index of the last set bit in a bitmap (MSB-first).
fn find_last_set_bit(bitmap: &[u8], max_bits: u32) -> Option<u32> {
    let mut last = None;
    for bit in 0..max_bits {
        let byte_idx = bit as usize / 8;
        let bit_idx = 7 - (bit % 8); // MSB-first
        if byte_idx < bitmap.len() && (bitmap[byte_idx] >> bit_idx) & 1 == 1 {
            last = Some(bit);
        }
    }
    last
}

/// Compact reader for classic HFS: layout-preserving image with zeros for free blocks.
///
/// Outputs the full pre-allocation region (boot blocks + MDB + volume bitmap,
/// `first_alloc_block * 512` bytes) followed by all allocation blocks in order,
/// with free blocks replaced by zeros. This preserves the original block layout:
/// allocation block N is always at byte offset `first_alloc_block*512 + N*block_size`,
/// enabling correct filesystem browsing and reliable restore.
pub struct CompactHfsReader<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    mdb: HfsMasterDirectoryBlock,
    bitmap: Vec<u8>,
    /// Current phase: 0=pre-alloc region, 1=allocation blocks, 2=done.
    phase: u8,
    /// Byte position within the pre-alloc region (phase 0).
    pre_alloc_pos: u64,
    /// Current allocation block index (phase 1).
    current_block: u32,
    /// Byte position within the current allocation block (phase 1).
    block_pos: u64,
    #[allow(dead_code)]
    original_size: u64,
    #[allow(dead_code)]
    allocated_blocks: u32,
}

impl<R: Read + Seek> CompactHfsReader<R> {
    pub fn new(
        mut reader: R,
        partition_offset: u64,
    ) -> Result<(Self, CompactResult), FilesystemError> {
        // Read MDB
        eprintln!(
            "[HFS compact] seeking to MDB at offset {}",
            partition_offset + 1024
        );
        reader.seek(SeekFrom::Start(partition_offset + 1024))?;
        let mut mdb_buf = [0u8; 162];
        reader.read_exact(&mut mdb_buf)?;
        let mdb = HfsMasterDirectoryBlock::parse(&mdb_buf).map_err(|e| {
            eprintln!("[HFS compact] MDB parse failed: {e}");
            e
        })?;
        eprintln!(
            "[HFS compact] MDB ok: block_size={}, total_blocks={}, first_alloc_block={}, volume_bitmap_block={}",
            mdb.block_size, mdb.total_blocks, mdb.first_alloc_block, mdb.volume_bitmap_block,
        );

        if mdb.has_embedded_hfs_plus() {
            eprintln!("[HFS compact] has embedded HFS+ — refusing to compact HFS wrapper");
            return Err(FilesystemError::Unsupported(
                "cannot compact HFS wrapper with embedded HFS+".into(),
            ));
        }

        // Read volume bitmap
        let bitmap_offset = partition_offset + mdb.volume_bitmap_block as u64 * 512;
        eprintln!("[HFS compact] reading bitmap at offset {bitmap_offset}");
        let bitmap_size = (mdb.total_blocks as u32).div_ceil(8) as usize;
        reader.seek(SeekFrom::Start(bitmap_offset))?;
        let mut bitmap = vec![0u8; bitmap_size];
        reader.read_exact(&mut bitmap)?;
        eprintln!("[HFS compact] bitmap read: {bitmap_size} bytes");

        // Count allocated blocks
        let mut allocated = 0u32;
        for bit in 0..mdb.total_blocks as u32 {
            let byte_idx = bit as usize / 8;
            let bit_idx = 7 - (bit % 8);
            if byte_idx < bitmap.len() && (bitmap[byte_idx] >> bit_idx) & 1 == 1 {
                allocated += 1;
            }
        }
        eprintln!(
            "[HFS compact] allocated={} / {} total blocks ({} free)",
            allocated,
            mdb.total_blocks,
            mdb.total_blocks as u32 - allocated,
        );

        // Full partition data: pre-alloc region + all allocation blocks.
        let pre_alloc_size = mdb.first_alloc_block as u64 * 512;
        let original_size = pre_alloc_size + mdb.total_blocks as u64 * mdb.block_size as u64;
        // Layout-preserving: compacted_size == original_size.
        // Free allocation blocks are zeroed, so they compress extremely well.
        let compacted_size = original_size;
        eprintln!(
            "[HFS compact] pre_alloc_size={}, compacted_size={} original_size={} (layout-preserving; free blocks → zeros)",
            pre_alloc_size, compacted_size, original_size
        );

        let result = CompactResult {
            original_size,
            compacted_size,
            clusters_used: allocated,
        };

        Ok((
            CompactHfsReader {
                reader,
                partition_offset,
                mdb,
                bitmap,
                phase: 0,
                pre_alloc_pos: 0,
                current_block: 0,
                block_pos: 0,
                original_size,
                allocated_blocks: allocated,
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

impl<R: Read + Seek> Read for CompactHfsReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.phase > 1 {
            return Ok(0);
        }

        if self.phase == 0 {
            // Phase 0: pre-alloc region (boot blocks, MDB, volume bitmap).
            // Covers sectors 0..first_alloc_block — always preserved verbatim.
            let pre_alloc_size = self.mdb.first_alloc_block as u64 * 512;
            let remaining = pre_alloc_size - self.pre_alloc_pos;
            if remaining == 0 {
                self.phase = 1;
                return self.read(buf);
            }
            let to_read = (remaining as usize).min(buf.len());
            let offset = self.partition_offset + self.pre_alloc_pos;
            self.reader
                .seek(SeekFrom::Start(offset))
                .map_err(std::io::Error::other)?;
            let n = self.reader.read(&mut buf[..to_read])?;
            self.pre_alloc_pos += n as u64;
            return Ok(n);
        }

        // Phase 1: allocation blocks 0..total_blocks.
        // Allocated blocks → real data; free blocks → zeros.
        let total_blocks = self.mdb.total_blocks as u32;
        if self.current_block >= total_blocks {
            self.phase = 2;
            return Ok(0);
        }

        let block_size = self.mdb.block_size as u64;
        let remaining_in_block = block_size - self.block_pos;
        let to_read = (remaining_in_block as usize).min(buf.len());

        let n = if self.is_block_allocated(self.current_block) {
            let offset = self.partition_offset
                + self.mdb.first_alloc_block as u64 * 512
                + self.current_block as u64 * block_size
                + self.block_pos;
            self.reader
                .seek(SeekFrom::Start(offset))
                .map_err(std::io::Error::other)?;
            self.reader.read(&mut buf[..to_read])?
        } else {
            // Free block — emit zeros so free space compresses to nothing.
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

// --- Resize and validation functions ---

/// Resize an HFS filesystem in place.
pub fn resize_hfs_in_place(
    device: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    new_size_bytes: u64,
    log: &mut impl FnMut(&str),
) -> anyhow::Result<()> {
    // Read MDB
    device.seek(SeekFrom::Start(partition_offset + 1024))?;
    let mut mdb_buf = [0u8; 162];
    device.read_exact(&mut mdb_buf)?;

    let sig = BigEndian::read_u16(&mdb_buf[0..2]);
    if sig != HFS_SIGNATURE {
        log("HFS resize: not an HFS volume, skipping");
        return Ok(());
    }

    let block_size = BigEndian::read_u32(&mdb_buf[20..24]);
    let old_total = BigEndian::read_u16(&mdb_buf[18..20]);
    let free_blocks = BigEndian::read_u16(&mdb_buf[34..36]);
    let first_alloc = BigEndian::read_u16(&mdb_buf[28..30]);
    let used_blocks = old_total - free_blocks;

    let overhead = first_alloc as u64 * 512;
    let new_total = ((new_size_bytes - overhead) / block_size as u64) as u16;

    if new_total < used_blocks {
        anyhow::bail!(
            "HFS resize: new size {} blocks < used {} blocks",
            new_total,
            used_blocks
        );
    }

    let new_free = new_total - used_blocks;

    log(&format!(
        "HFS resize: {} -> {} blocks ({} free)",
        old_total, new_total, new_free
    ));

    // Update MDB fields
    BigEndian::write_u16(&mut mdb_buf[18..20], new_total);
    BigEndian::write_u16(&mut mdb_buf[34..36], new_free);

    // Write primary MDB at offset + 1024
    device.seek(SeekFrom::Start(partition_offset + 1024))?;
    device.write_all(&mdb_buf)?;

    // Write backup MDB at offset + new_size - 1024
    if new_size_bytes > 1024 {
        device.seek(SeekFrom::Start(partition_offset + new_size_bytes - 1024))?;
        device.write_all(&mdb_buf)?;
    }

    device.flush()?;
    Ok(())
}

/// Validate HFS filesystem integrity.
pub fn validate_hfs_integrity(
    device: &mut (impl Read + Seek),
    partition_offset: u64,
    log: &mut impl FnMut(&str),
) -> anyhow::Result<()> {
    device.seek(SeekFrom::Start(partition_offset + 1024))?;
    let mut mdb_buf = [0u8; 162];
    device.read_exact(&mut mdb_buf)?;

    let sig = BigEndian::read_u16(&mdb_buf[0..2]);
    if sig != HFS_SIGNATURE {
        log("HFS validate: not an HFS volume, skipping");
        return Ok(());
    }

    let block_size = BigEndian::read_u32(&mdb_buf[20..24]);
    let total_blocks = BigEndian::read_u16(&mdb_buf[18..20]);

    // Basic sanity checks
    if !block_size.is_power_of_two() || block_size < 512 {
        anyhow::bail!("HFS validate: invalid block size {block_size}");
    }
    if total_blocks == 0 {
        anyhow::bail!("HFS validate: zero total blocks");
    }

    log(&format!(
        "HFS validate: OK ({total_blocks} blocks, {} block size)",
        block_size
    ));
    Ok(())
}

/// Patch HFS hidden sectors (no-op: HFS doesn't have a hidden sectors field like FAT/NTFS).
pub fn patch_hfs_hidden_sectors(
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
    use std::io::Cursor;

    #[test]
    fn test_mac_roman_to_utf8_ascii() {
        let data = b"Hello World";
        assert_eq!(mac_roman_to_utf8(data), "Hello World");
    }

    #[test]
    fn test_mac_roman_to_utf8_special() {
        // 0x80 = Ä, 0x81 = Å, 0x87 = á
        let data = [0x80, 0x81, 0x87];
        assert_eq!(mac_roman_to_utf8(&data), "ÄÅá");
    }

    #[test]
    fn test_mdb_parse() {
        let mut data = [0u8; 162];
        // Signature
        BigEndian::write_u16(&mut data[0..2], HFS_SIGNATURE);
        // total_blocks
        BigEndian::write_u16(&mut data[18..20], 1000);
        // block_size
        BigEndian::write_u32(&mut data[20..24], 4096);
        // free_blocks
        BigEndian::write_u16(&mut data[34..36], 200);
        // volume name: Pascal string "TestVol"
        data[36] = 7;
        data[37..44].copy_from_slice(b"TestVol");

        let mdb = HfsMasterDirectoryBlock::parse(&data).unwrap();
        assert_eq!(mdb.signature, HFS_SIGNATURE);
        assert_eq!(mdb.total_blocks, 1000);
        assert_eq!(mdb.block_size, 4096);
        assert_eq!(mdb.free_blocks, 200);
        assert_eq!(mdb.volume_name, "TestVol");
        assert!(!mdb.has_embedded_hfs_plus());
    }

    #[test]
    fn test_mdb_detect_embedded_hfs_plus() {
        let mut data = [0u8; 162];
        BigEndian::write_u16(&mut data[0..2], HFS_SIGNATURE);
        BigEndian::write_u16(&mut data[18..20], 1000);
        BigEndian::write_u32(&mut data[20..24], 4096);
        // Embedded HFS+ signature
        BigEndian::write_u16(&mut data[124..126], HFS_PLUS_EMBEDDED_SIGNATURE);
        BigEndian::write_u16(&mut data[126..128], 10); // start
        BigEndian::write_u16(&mut data[128..130], 500); // count

        let mdb = HfsMasterDirectoryBlock::parse(&data).unwrap();
        assert!(mdb.has_embedded_hfs_plus());
    }

    #[test]
    fn test_find_last_set_bit() {
        let bitmap = [0b10100000, 0b00001000]; // bits 0, 2, 12
        assert_eq!(find_last_set_bit(&bitmap, 16), Some(12));

        let bitmap_empty = [0u8; 2];
        assert_eq!(find_last_set_bit(&bitmap_empty, 16), None);

        let bitmap_first = [0b10000000]; // bit 0
        assert_eq!(find_last_set_bit(&bitmap_first, 8), Some(0));
    }
}
