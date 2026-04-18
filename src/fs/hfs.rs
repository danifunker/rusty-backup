use byteorder::{BigEndian, ByteOrder};
use std::cmp::Ordering;
use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};
use super::hfs_common::{
    self, bitmap_clear_bit_be, bitmap_find_clear_run_be, bitmap_set_bit_be, btree_free_node,
    btree_insert_record, btree_record_range, btree_remove_record, btree_split_leaf, BTreeHeader,
};
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

/// Convert a UTF-8 string to Mac Roman bytes.
/// Returns Err if any character cannot be represented in Mac Roman.
pub fn utf8_to_mac_roman(s: &str) -> Result<Vec<u8>, FilesystemError> {
    let mut result = Vec::with_capacity(s.len());
    for ch in s.chars() {
        if (ch as u32) < 0x80 {
            result.push(ch as u8);
        } else {
            // Search the Mac Roman table for this character
            let mut found = false;
            for (i, &table_char) in MAC_ROMAN_TABLE.iter().enumerate() {
                if table_char == ch {
                    result.push((i as u8) + 0x80);
                    found = true;
                    break;
                }
            }
            if !found {
                return Err(FilesystemError::InvalidData(format!(
                    "character '{}' (U+{:04X}) cannot be encoded in Mac Roman",
                    ch, ch as u32
                )));
            }
        }
    }
    Ok(result)
}

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

/// Returns `true` if the catalog B-tree header is all zeros (uninitialized).
/// Validate a name for a new file or directory on a classic HFS volume.
/// Returns the encoded Mac Roman bytes on success.
fn validate_hfs_create_name(name: &str) -> Result<Vec<u8>, FilesystemError> {
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
    let name_bytes = utf8_to_mac_roman(name).map_err(|_| {
        FilesystemError::InvalidData(
            "filename contains characters that can't be encoded in Mac Roman \
             (HFS predates Unicode) — rename using plain ASCII or common accented letters"
                .into(),
        )
    })?;
    if name_bytes.is_empty() {
        return Err(FilesystemError::InvalidData(
            "filename is empty — pick a non-blank name".into(),
        ));
    }
    if name_bytes.len() > 31 {
        return Err(FilesystemError::InvalidData(format!(
            "filename is too long ({} chars); classic HFS allows up to 31 — shorten the name",
            name_bytes.len()
        )));
    }
    Ok(name_bytes)
}

/// Some formatters (notably Disk Jockey's "Empty HFS" image) allocate the
/// catalog extents but leave the bytes zeroed, which would otherwise cause a
/// panic on the first insert because `node_size` parses as 0.
fn is_catalog_uninitialized(catalog_data: &[u8]) -> bool {
    if catalog_data.len() < 34 {
        return true;
    }
    // node_size field in the BTHeaderRec sits at catalog byte 32..34.
    BigEndian::read_u16(&catalog_data[32..34]) == 0
}

/// HFS extent descriptor: start_block (u16) + block_count (u16).
#[derive(Debug, Clone, Copy)]
pub(crate) struct HfsExtDescriptor {
    pub(crate) start_block: u16,
    pub(crate) block_count: u16,
}

impl HfsExtDescriptor {
    pub(crate) fn parse(data: &[u8]) -> Self {
        HfsExtDescriptor {
            start_block: BigEndian::read_u16(&data[0..2]),
            block_count: BigEndian::read_u16(&data[2..4]),
        }
    }

    fn serialize(&self, out: &mut [u8]) {
        BigEndian::write_u16(&mut out[0..2], self.start_block);
        BigEndian::write_u16(&mut out[2..4], self.block_count);
    }
}

/// HFS Master Directory Block (MDB) — at partition_offset + 1024.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct HfsMasterDirectoryBlock {
    pub(crate) signature: u16,
    pub(crate) create_date: u32,
    pub(crate) modify_date: u32,
    pub(crate) total_blocks: u16,
    pub(crate) block_size: u32,
    pub(crate) free_blocks: u16,
    pub(crate) volume_name: String,
    pub(crate) volume_name_raw: Vec<u8>,
    pub(crate) volume_bitmap_block: u16,
    /// First allocation block's offset in 512-byte sectors from partition start.
    pub(crate) first_alloc_block: u16,
    pub(crate) next_catalog_id: u32,
    pub(crate) file_count: u32,
    pub(crate) folder_count: u32,
    pub(crate) finder_info: [u32; 8],
    pub(crate) catalog_file_size: u32,
    pub(crate) catalog_file_extents: [HfsExtDescriptor; 3],
    pub(crate) extents_file_size: u32,
    pub(crate) extents_file_extents: [HfsExtDescriptor; 3],
    pub(crate) embedded_signature: u16,
    pub(crate) embedded_start_block: u16,
    pub(crate) embedded_block_count: u16,
    /// Raw 512-byte sector for serialization (preserve fields we don't explicitly parse).
    pub(crate) raw_sector: [u8; 512],
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
        let volume_name_raw = name_bytes.to_vec();

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

        // Finder info at offset 92 (32 bytes = 8 × u32)
        let mut finder_info = [0u32; 8];
        for i in 0..8 {
            finder_info[i] = BigEndian::read_u32(&data[92 + i * 4..96 + i * 4]);
        }

        // Preserve raw sector for faithful serialization
        let mut raw_sector = [0u8; 512];
        let copy_len = data.len().min(512);
        raw_sector[..copy_len].copy_from_slice(&data[..copy_len]);

        Ok(HfsMasterDirectoryBlock {
            signature: sig,
            create_date: BigEndian::read_u32(&data[2..6]),
            modify_date: BigEndian::read_u32(&data[6..10]),
            total_blocks: BigEndian::read_u16(&data[18..20]),
            block_size: BigEndian::read_u32(&data[20..24]),
            free_blocks: BigEndian::read_u16(&data[34..36]),
            volume_name,
            volume_name_raw,
            volume_bitmap_block: BigEndian::read_u16(&data[14..16]),
            first_alloc_block: BigEndian::read_u16(&data[28..30]),
            next_catalog_id: BigEndian::read_u32(&data[30..34]),
            file_count: BigEndian::read_u32(&data[84..88]),
            folder_count: BigEndian::read_u32(&data[88..92]),
            finder_info,
            extents_file_size: BigEndian::read_u32(&data[130..134]),
            extents_file_extents: extents_extents,
            catalog_file_size: BigEndian::read_u32(&data[146..150]),
            catalog_file_extents: catalog_extents,
            embedded_signature: embedded_sig,
            embedded_start_block: embedded_start,
            embedded_block_count: embedded_count,
            raw_sector,
        })
    }

    /// Serialize MDB back to a 512-byte sector.
    /// Uses the raw sector as a base and overwrites fields we may have modified.
    fn serialize_to_sector(&self) -> [u8; 512] {
        let mut out = self.raw_sector;
        BigEndian::write_u32(&mut out[6..10], self.modify_date);
        BigEndian::write_u16(&mut out[18..20], self.total_blocks);
        BigEndian::write_u16(&mut out[34..36], self.free_blocks);
        BigEndian::write_u32(&mut out[30..34], self.next_catalog_id);
        BigEndian::write_u32(&mut out[84..88], self.file_count);
        BigEndian::write_u32(&mut out[88..92], self.folder_count);
        for i in 0..8 {
            BigEndian::write_u32(&mut out[92 + i * 4..96 + i * 4], self.finder_info[i]);
        }
        BigEndian::write_u32(&mut out[146..150], self.catalog_file_size);
        for i in 0..3 {
            self.catalog_file_extents[i].serialize(&mut out[150 + i * 4..154 + i * 4]);
        }
        out
    }

    /// True if this MDB wraps an embedded HFS+ volume.
    fn has_embedded_hfs_plus(&self) -> bool {
        self.embedded_signature == HFS_PLUS_EMBEDDED_SIGNATURE
    }
}

/// Catalog record types.
pub(crate) const CATALOG_DIR: i8 = 1;
pub(crate) const CATALOG_FILE: i8 = 2;

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
        /// Finder flags (FInfo.fdFlags) — bit 0x8000 is `kIsAlias`.
        finder_flags: u16,
    },
}

/// Classic HFS filesystem implementation.
pub struct HfsFilesystem<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    mdb: HfsMasterDirectoryBlock,
    /// Cached catalog file data.
    catalog_data: Vec<u8>,
    /// Cached volume bitmap (lazy-loaded on first edit operation).
    bitmap: Option<Vec<u8>>,
}

impl<R: Read + Seek> HfsFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        // Read MDB at offset + 1024 (sector 2) — full 512-byte sector
        reader.seek(SeekFrom::Start(partition_offset + 1024))?;
        let mut mdb_buf = [0u8; 512];
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
            bitmap: None,
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
                        // Finder Info (FInfo) at offset 4: fdType(4) + fdCreator(4) + fdFlags(2)
                        let type_code = decode_fourcc(&rec[4..8]);
                        let creator_code = decode_fourcc(&rec[8..12]);
                        let finder_flags = BigEndian::read_u16(&rec[12..14]);
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
                            finder_flags,
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

    /// Ensure bitmap is loaded.
    fn ensure_bitmap(&mut self) -> Result<(), FilesystemError> {
        if self.bitmap.is_none() {
            let bm = self.read_volume_bitmap()?;
            self.bitmap = Some(bm);
        }
        Ok(())
    }

    /// Run filesystem integrity check. Loads bitmap if not already cached.
    pub fn fsck(&mut self) -> Result<super::fsck::FsckResult, FilesystemError> {
        self.ensure_bitmap()?;
        // Load extents overflow B-tree if it exists (for files with >3 extents)
        let extents_data = if self.mdb.extents_file_size > 0 {
            read_fork_data(
                &mut self.reader,
                self.partition_offset,
                &self.mdb,
                &self.mdb.extents_file_extents,
                self.mdb.extents_file_size as u64,
            )
            .ok()
        } else {
            None
        };
        // Read the alternate MDB — located at the sector immediately after
        // the last allocation block on the volume.
        let alt_mdb_sector = if self.mdb.block_size > 0 && self.mdb.total_blocks > 0 {
            let sectors_per_block = self.mdb.block_size as u64 / 512;
            let last_alloc_sector = self.mdb.first_alloc_block as u64
                + self.mdb.total_blocks as u64 * sectors_per_block;
            let alt_offset = self.partition_offset + last_alloc_sector * 512;
            let mut alt_buf = [0u8; 512];
            if self
                .reader
                .seek(SeekFrom::Start(alt_offset))
                .and_then(|_| self.reader.read_exact(&mut alt_buf))
                .is_ok()
            {
                Some(alt_buf)
            } else {
                None
            }
        } else {
            None
        };
        Ok(super::hfs_fsck::check_hfs_integrity(
            &self.mdb,
            &self.catalog_data,
            self.bitmap.as_ref().unwrap(),
            extents_data.as_deref(),
            alt_mdb_sector.as_ref(),
        ))
    }

    /// Build classic HFS catalog key: key_len(1) + reserved(1) + parent_id(4) + name_len(1) + name(Mac Roman).
    pub(crate) fn build_catalog_key(parent_id: u32, name: &[u8]) -> Vec<u8> {
        let key_len = 1 + 4 + 1 + name.len(); // reserved + parent_id + name_len + name
        let mut key = Vec::with_capacity(1 + key_len);
        key.push(key_len as u8); // key_len
        key.push(0); // reserved
        let mut buf4 = [0u8; 4];
        BigEndian::write_u32(&mut buf4, parent_id);
        key.extend_from_slice(&buf4);
        key.push(name.len() as u8);
        key.extend_from_slice(name);
        // Pad to even length if needed (HFS B-tree records are even-aligned)
        if key.len() % 2 != 0 {
            key.push(0);
        }
        key
    }

    /// Compare function for HFS catalog keys (key portion only).
    pub(crate) fn catalog_compare(a: &[u8], b: &[u8]) -> Ordering {
        // Format: key_len(1) + reserved(1) + parent_id(4) + name_len(1) + name(Mac Roman)
        if a.len() < 7 || b.len() < 7 {
            return a.len().cmp(&b.len());
        }
        let parent_a = BigEndian::read_u32(&a[2..6]);
        let parent_b = BigEndian::read_u32(&b[2..6]);
        let name_len_a = a[6] as usize;
        let name_len_b = b[6] as usize;
        let name_a = &a[7..7 + name_len_a.min(a.len() - 7)];
        let name_b = &b[7..7 + name_len_b.min(b.len() - 7)];
        hfs_common::compare_hfs_keys(parent_a, name_a, parent_b, name_b)
    }

    /// Find a catalog record by (parent_id, name).
    /// Returns Some((node_idx, rec_idx, absolute_offset_in_catalog_data)) if found.
    fn find_catalog_record_by_name(
        &self,
        parent_id: u32,
        name: &[u8],
    ) -> Option<(u32, usize, usize)> {
        let search_key = Self::build_catalog_key(parent_id, name);
        let node_size = BigEndian::read_u16(&self.catalog_data[32..34]) as usize;
        if node_size == 0 || self.catalog_data.len() < node_size {
            return None;
        }
        let first_leaf = BigEndian::read_u32(&self.catalog_data[24..28]);

        let mut node_idx = first_leaf;
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
                if rec.len() < 7 {
                    continue;
                }
                // Extract key portion (key_len byte + key data, possibly padded)
                let key_len = rec[0] as usize;
                let key_end = 1 + key_len;
                if key_end > rec.len() {
                    continue;
                }
                let key_portion = &rec[..key_end];
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
        self.find_catalog_record_by_name(cnid, &[])
    }

    /// Update parent folder valence (child count) by delta.
    fn update_parent_valence(&mut self, parent_id: u32, delta: i32) -> Result<(), FilesystemError> {
        let node_size = BigEndian::read_u16(&self.catalog_data[32..34]) as usize;
        if node_size == 0 {
            return Ok(());
        }
        let first_leaf = BigEndian::read_u32(&self.catalog_data[24..28]);

        let mut node_idx = first_leaf;
        while node_idx != 0 {
            let offset = node_idx as usize * node_size;
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
                if abs_end > self.catalog_data.len() || rec_end - rec_start < 7 {
                    continue;
                }
                let key_len = self.catalog_data[abs_start] as usize;
                let mut rec_data_offset = abs_start + 1 + key_len;
                if rec_data_offset % 2 != 0 {
                    rec_data_offset += 1;
                }
                if rec_data_offset + 10 > abs_end {
                    continue;
                }
                let record_type = self.catalog_data[rec_data_offset] as i8;
                if record_type != CATALOG_DIR {
                    continue;
                }
                // Dir record: type(1) + reserved(1) + flags(2) + valence(2) + dirID(4)
                let dir_id = BigEndian::read_u32(
                    &self.catalog_data[rec_data_offset + 6..rec_data_offset + 10],
                );
                if dir_id == parent_id {
                    // Valence at offset 4 (u16)
                    let val_off = rec_data_offset + 4;
                    let old_val = BigEndian::read_u16(&self.catalog_data[val_off..val_off + 2]);
                    let new_val = (old_val as i32 + delta).max(0) as u16;
                    BigEndian::write_u16(&mut self.catalog_data[val_off..val_off + 2], new_val);
                    return Ok(());
                }
            }
            let next = BigEndian::read_u32(&self.catalog_data[offset..offset + 4]);
            node_idx = next;
        }
        Err(FilesystemError::NotFound(format!(
            "parent directory CNID {} not found in catalog",
            parent_id
        )))
    }

    /// Insert a catalog record into the B-tree, handling splits and growth.
    fn insert_catalog_record(&mut self, key_record: &[u8]) -> Result<(), FilesystemError> {
        let header = BTreeHeader::read(&self.catalog_data);
        let node_size = header.node_size as usize;

        // Find the correct leaf node
        let (leaf_idx, _parent_chain) = hfs_common::btree_find_insert_leaf(
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
                let mut h = BTreeHeader::read(&self.catalog_data);
                h.leaf_records += 1;
                h.write(&mut self.catalog_data);
                Ok(())
            }
            Err(_) => {
                // Leaf full — split, insert into correct half, rebuild index
                let mut h = BTreeHeader::read(&self.catalog_data);
                let (new_idx, split_key) =
                    btree_split_leaf(&mut self.catalog_data, node_size, leaf_idx, &mut h)?;

                let target = if Self::catalog_compare(key_record, &split_key) == Ordering::Less {
                    leaf_idx
                } else {
                    new_idx
                };
                let t_offset = target as usize * node_size;
                let t_node = &mut self.catalog_data[t_offset..t_offset + node_size];
                btree_insert_record(t_node, node_size, key_record, &Self::catalog_compare)?;

                h.leaf_records += 1;
                h.write(&mut self.catalog_data);

                // Full index rebuild replaces incremental parent chain insertion
                let mut dummy_report = super::fsck::RepairReport {
                    fixes_applied: Vec::new(),
                    fixes_failed: Vec::new(),
                    unrepairable_count: 0,
                };
                super::hfs_fsck::rebuild_index_nodes(
                    &mut self.catalog_data,
                    node_size,
                    &mut dummy_report,
                );

                Ok(())
            }
        }
    }

    /// Remove a catalog record from a leaf node.
    fn remove_catalog_record(&mut self, node_idx: u32, rec_idx: usize) {
        let node_size = BigEndian::read_u16(&self.catalog_data[32..34]) as usize;
        if node_size == 0 {
            return;
        }
        let offset = node_idx as usize * node_size;
        if offset + node_size > self.catalog_data.len() {
            return;
        }

        let num_before = {
            let node = &self.catalog_data[offset..offset + node_size];
            BigEndian::read_u16(&node[10..12]) as usize
        };

        {
            let node = &mut self.catalog_data[offset..offset + node_size];
            btree_remove_record(node, node_size, rec_idx);
        }

        // If node is now empty, free it and fix links
        let node = &self.catalog_data[offset..offset + node_size];
        let num_after = BigEndian::read_u16(&node[10..12]) as usize;
        if num_after == 0 && num_before > 0 {
            let prev = BigEndian::read_u32(&self.catalog_data[offset + 4..offset + 8]);
            let next = BigEndian::read_u32(&self.catalog_data[offset..offset + 4]);

            if prev != 0 {
                let prev_off = prev as usize * node_size;
                if prev_off + 4 <= self.catalog_data.len() {
                    BigEndian::write_u32(&mut self.catalog_data[prev_off..prev_off + 4], next);
                }
            }
            if next != 0 {
                let next_off = next as usize * node_size;
                if next_off + 8 <= self.catalog_data.len() {
                    BigEndian::write_u32(&mut self.catalog_data[next_off + 4..next_off + 8], prev);
                }
            }

            let mut h = BTreeHeader::read(&self.catalog_data);
            if h.first_leaf_node == node_idx {
                h.first_leaf_node = next;
            }
            if h.last_leaf_node == node_idx {
                h.last_leaf_node = prev;
            }
            btree_free_node(&mut self.catalog_data, node_size, node_idx);
            h.free_nodes += 1;
            h.write(&mut self.catalog_data);

            // Rebuild index nodes to clean up stale separator keys
            let header = BTreeHeader::read(&self.catalog_data);
            if header.depth > 1 {
                let mut dummy_report = super::fsck::RepairReport {
                    fixes_applied: Vec::new(),
                    fixes_failed: Vec::new(),
                    unrepairable_count: 0,
                };
                super::hfs_fsck::rebuild_index_nodes(
                    &mut self.catalog_data,
                    node_size,
                    &mut dummy_report,
                );
            }
        }

        // Update leaf_records count
        let mut h = BTreeHeader::read(&self.catalog_data);
        h.leaf_records = h.leaf_records.saturating_sub(1);
        h.write(&mut self.catalog_data);
    }

    /// Capture a snapshot of all mutable in-memory state for rollback.
    fn snapshot(&self) -> (Vec<u8>, Option<Vec<u8>>, HfsMasterDirectoryBlock) {
        (
            self.catalog_data.clone(),
            self.bitmap.clone(),
            self.mdb.clone(),
        )
    }

    /// Restore in-memory state from a previously captured snapshot.
    fn restore_snapshot(&mut self, snap: (Vec<u8>, Option<Vec<u8>>, HfsMasterDirectoryBlock)) {
        self.catalog_data = snap.0;
        self.bitmap = snap.1;
        self.mdb = snap.2;
    }
}

/// Write helpers — require Read + Write + Seek.
impl<R: Read + Write + Seek> HfsFilesystem<R> {
    /// Write data to an allocation block.
    fn write_block(&mut self, block: u32, data: &[u8]) -> Result<(), FilesystemError> {
        let offset = self.partition_offset + self.alloc_block_offset(block);
        self.reader.seek(SeekFrom::Start(offset))?;
        self.reader.write_all(data)?;
        Ok(())
    }

    /// Write MDB to the primary location (offset+1024).
    /// The backup (alternate) MDB is intentionally not updated — its location
    /// depends on the exact partition size which may not be known. The primary
    /// MDB is authoritative; Mac OS only falls back to the alternate if the
    /// primary is corrupt, and the stale alternate remains structurally valid.
    fn write_mdb(&mut self) -> Result<(), FilesystemError> {
        let mdb_bytes = self.mdb.serialize_to_sector();
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + 1024))?;
        self.reader.write_all(&mdb_bytes)?;
        Ok(())
    }

    /// Write catalog B-tree data back to disk through the catalog_file extents.
    fn write_catalog(&mut self) -> Result<(), FilesystemError> {
        write_hfs_fork_data(
            &mut self.reader,
            self.partition_offset,
            &self.mdb,
            &self.mdb.catalog_file_extents,
            &self.catalog_data,
        )
    }

    /// Write the volume bitmap back to disk.
    fn write_volume_bitmap(&mut self) -> Result<(), FilesystemError> {
        if let Some(ref bm) = self.bitmap {
            let bitmap_offset = self.partition_offset + self.mdb.volume_bitmap_block as u64 * 512;
            self.reader.seek(SeekFrom::Start(bitmap_offset))?;
            self.reader.write_all(bm)?;
        }
        Ok(())
    }

    /// Allocate `count` contiguous blocks from the volume bitmap.
    /// Returns the start block index.
    fn allocate_blocks(&mut self, count: u32) -> Result<u32, FilesystemError> {
        self.ensure_bitmap()?;
        let bitmap = self.bitmap.as_mut().unwrap();
        let start = bitmap_find_clear_run_be(bitmap, self.mdb.total_blocks as u32, count)
            .ok_or_else(|| {
                FilesystemError::DiskFull(format!("cannot find {} contiguous free blocks", count))
            })?;
        for i in 0..count {
            bitmap_set_bit_be(bitmap, start + i);
        }
        self.mdb.free_blocks -= count as u16;
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
        self.mdb.free_blocks += count as u16;
    }

    /// Free all blocks referenced by an extent array.
    fn free_extent_blocks(&mut self, extents: &[HfsExtDescriptor; 3]) {
        for ext in extents {
            if ext.block_count == 0 {
                break;
            }
            self.free_blocks(ext.start_block as u32, ext.block_count as u32);
        }
    }

    /// Write file data to allocated blocks. Returns (start_block, block_count).
    fn write_data_to_blocks(
        &mut self,
        data: &mut dyn std::io::Read,
        data_len: u64,
    ) -> Result<(u16, u16), FilesystemError> {
        if data_len == 0 {
            return Ok((0, 0));
        }
        let block_size = self.mdb.block_size as u64;
        let blocks_needed = data_len.div_ceil(block_size) as u32;
        if blocks_needed > u16::MAX as u32 {
            return Err(FilesystemError::InvalidData(
                "file too large for classic HFS (>65535 blocks)".into(),
            ));
        }
        let start_block = self.allocate_blocks(blocks_needed)?;

        let mut buf = vec![0u8; block_size as usize];
        let mut remaining = data_len;
        for i in 0..blocks_needed {
            let to_read = remaining.min(block_size) as usize;
            buf.fill(0);
            data.read_exact(&mut buf[..to_read]).map_err(|e| {
                FilesystemError::Io(std::io::Error::new(
                    e.kind(),
                    format!("reading file data: {e}"),
                ))
            })?;
            self.write_block(start_block + i, &buf)?;
            remaining -= to_read as u64;
        }

        Ok((start_block as u16, blocks_needed as u16))
    }

    /// Build a classic HFS file record (102 bytes).
    #[allow(clippy::too_many_arguments)]
    fn build_file_record(
        file_id: u32,
        data_size: u32,
        data_start: u16,
        data_blocks: u16,
        rsrc_size: u32,
        rsrc_start: u16,
        rsrc_blocks: u16,
        type_code: &[u8; 4],
        creator_code: &[u8; 4],
        block_size: u32,
    ) -> [u8; 102] {
        let mut rec = [0u8; 102];
        let now = hfs_common::hfs_now();
        rec[0] = CATALOG_FILE as u8; // cdrType
                                     // rec[1] = reserved
                                     // FInfo at offset 4: fdType(4) + fdCreator(4)
        rec[4..8].copy_from_slice(type_code);
        rec[8..12].copy_from_slice(creator_code);
        // filFlNum at offset 20
        BigEndian::write_u32(&mut rec[20..24], file_id);
        // filStBlk at offset 24 (first alloc block of data fork)
        BigEndian::write_u16(&mut rec[24..26], data_start);
        // filLgLen at offset 26 (data fork logical size)
        BigEndian::write_u32(&mut rec[26..30], data_size);
        // filPyLen at offset 30 (data fork physical size)
        BigEndian::write_u32(&mut rec[30..34], data_blocks as u32 * block_size);
        // filRStBlk at offset 34
        BigEndian::write_u16(&mut rec[34..36], rsrc_start);
        // filRLgLen at offset 36 (rsrc fork logical size)
        BigEndian::write_u32(&mut rec[36..40], rsrc_size);
        // filRPyLen at offset 40 (rsrc fork physical size)
        BigEndian::write_u32(&mut rec[40..44], rsrc_blocks as u32 * block_size);
        // filCrDat at offset 44
        BigEndian::write_u32(&mut rec[44..48], now);
        // filMdDat at offset 48
        BigEndian::write_u32(&mut rec[48..52], now);
        // Data fork extents at offset 74 (3 × 4 bytes)
        if data_blocks > 0 {
            BigEndian::write_u16(&mut rec[74..76], data_start);
            BigEndian::write_u16(&mut rec[76..78], data_blocks);
        }
        // Resource fork extents at offset 86 (3 × 4 bytes)
        if rsrc_blocks > 0 {
            BigEndian::write_u16(&mut rec[86..88], rsrc_start);
            BigEndian::write_u16(&mut rec[88..90], rsrc_blocks);
        }
        rec
    }

    /// Build a classic HFS directory record (70 bytes).
    fn build_dir_record(dir_id: u32) -> [u8; 70] {
        let mut rec = [0u8; 70];
        let now = hfs_common::hfs_now();
        rec[0] = CATALOG_DIR as u8; // cdrType
                                    // dirFlags at offset 2 (u16) = 0
                                    // dirVal at offset 4 (u16) = 0 (child count)
                                    // dirDirID at offset 6
        BigEndian::write_u32(&mut rec[6..10], dir_id);
        // dirCrDat at offset 10
        BigEndian::write_u32(&mut rec[10..14], now);
        // dirMdDat at offset 14
        BigEndian::write_u32(&mut rec[14..18], now);
        rec
    }

    /// Build a thread record for classic HFS.
    /// Thread key: (cnid, ""). Thread data: type(1) + reserved(1) + reserved(8) + parentID(4) + name(Pascal string).
    pub(crate) fn build_thread_record(
        thread_type: i8,
        parent_id: u32,
        name: &[u8],
    ) -> (Vec<u8>, Vec<u8>) {
        // Key: (entry_cnid, "")  — built by caller with build_catalog_key(cnid, &[])
        // Record data: cdrType(1) + reserved(1) + reserved(8) + thdParID(4) + thdCName(Str31=32)
        // Total data = 46 bytes (per hfsutils: fixed Str31 field, not variable-length)
        let mut rec = Vec::with_capacity(46);
        rec.push(thread_type as u8); // cdrType
        rec.push(0); // reserved
        rec.extend_from_slice(&[0u8; 8]); // reserved (2 x LongInt)
        let mut buf4 = [0u8; 4];
        BigEndian::write_u32(&mut buf4, parent_id);
        rec.extend_from_slice(&buf4); // thdParID
                                      // Str31: 1 byte length + up to 31 bytes name + zero padding = 32 bytes total
        let name_len = name.len().min(31);
        let mut str31 = [0u8; 32];
        str31[0] = name_len as u8;
        str31[1..1 + name_len].copy_from_slice(&name[..name_len]);
        rec.extend_from_slice(&str31);
        // Key for the thread (not built here — caller uses build_catalog_key)
        (vec![], rec) // key is unused; caller builds key separately
    }

    /// Sync metadata: write catalog + bitmap + MDB + flush.
    fn do_sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.mdb.modify_date = hfs_common::hfs_now();
        self.write_catalog()?;
        self.write_volume_bitmap()?;
        self.write_mdb()?;
        self.reader.flush()?;
        Ok(())
    }

    /// If the catalog and/or extents-overflow B-trees are blank (e.g. Disk
    /// Jockey "Empty HFS" output where the MDB reserves extents for both
    /// B-trees but leaves the bytes zeroed), build fresh B-trees and commit
    /// them to disk. Subsequent edits can then use the normal insert paths
    /// instead of panicking on an empty B-tree header.
    fn ensure_catalog_initialized(&mut self) -> Result<(), FilesystemError> {
        let catalog_blank = is_catalog_uninitialized(&self.catalog_data);
        // Read the extents B-tree bytes so we can detect a blank one.
        let extents_data = if self.mdb.extents_file_size > 0 {
            read_fork_data(
                &mut self.reader,
                self.partition_offset,
                &self.mdb,
                &self.mdb.extents_file_extents,
                self.mdb.extents_file_size as u64,
            )
            .ok()
        } else {
            None
        };
        let extents_blank = extents_data
            .as_deref()
            .map(is_catalog_uninitialized)
            .unwrap_or(false);
        if !catalog_blank && !extents_blank {
            return Ok(());
        }
        self.initialize_empty_btrees(catalog_blank, extents_blank)
    }

    fn initialize_empty_btrees(
        &mut self,
        init_catalog: bool,
        init_extents: bool,
    ) -> Result<(), FilesystemError> {
        if init_catalog {
            let catalog_size = self.catalog_data.len();
            if catalog_size < 1024 {
                return Err(FilesystemError::InvalidData(format!(
                    "cannot initialize empty HFS catalog: region is only {catalog_size} bytes"
                )));
            }
            let buf = build_empty_hfs_catalog(catalog_size)?;
            self.catalog_data = buf;
            self.write_catalog()?;
        }

        if init_extents {
            let extents_size = self.mdb.extents_file_size as usize;
            if extents_size >= 512 {
                let buf = build_empty_hfs_extents_btree(extents_size)?;
                write_hfs_fork_data(
                    &mut self.reader,
                    self.partition_offset,
                    &self.mdb,
                    &self.mdb.extents_file_extents,
                    &buf,
                )?;
            }
        }

        // Rebuild the volume bitmap from scratch: only the blocks occupied by
        // the two B-trees should be marked allocated. This avoids trusting any
        // pre-existing garbage in the on-disk bitmap.
        let bitmap_size = (self.mdb.total_blocks as u32).div_ceil(8) as usize;
        let mut bitmap = vec![0u8; bitmap_size];
        let mut used_blocks = 0u32;
        for ext in self.mdb.catalog_file_extents.iter() {
            if ext.block_count == 0 {
                break;
            }
            for i in 0..ext.block_count {
                let bit = ext.start_block as u32 + i as u32;
                if bit < self.mdb.total_blocks as u32 {
                    hfs_common::bitmap_set_bit_be(&mut bitmap, bit);
                    used_blocks += 1;
                }
            }
        }
        for ext in self.mdb.extents_file_extents.iter() {
            if ext.block_count == 0 {
                break;
            }
            for i in 0..ext.block_count {
                let bit = ext.start_block as u32 + i as u32;
                if bit < self.mdb.total_blocks as u32 {
                    hfs_common::bitmap_set_bit_be(&mut bitmap, bit);
                    used_blocks += 1;
                }
            }
        }
        self.bitmap = Some(bitmap);
        self.mdb.free_blocks = (self.mdb.total_blocks as u32).saturating_sub(used_blocks) as u16;

        // Reset volume counters to reflect a truly-empty volume.
        self.mdb.file_count = 0;
        self.mdb.folder_count = 0;
        if self.mdb.next_catalog_id < 16 {
            self.mdb.next_catalog_id = 16;
        }
        self.mdb.modify_date = hfs_common::hfs_now();

        self.write_volume_bitmap()?;
        self.write_mdb()?;
        self.reader.flush()?;
        Ok(())
    }
}

/// Build a fresh classic HFS catalog B-tree with a header node plus a single
/// leaf node containing the root directory record and its thread record.
fn build_empty_hfs_catalog(catalog_size: usize) -> Result<Vec<u8>, FilesystemError> {
    let node_size = 512usize;
    let total_nodes = (catalog_size / node_size) as u32;
    if total_nodes < 2 {
        return Err(FilesystemError::InvalidData(format!(
            "cannot initialize empty HFS catalog: only {total_nodes} node(s) fit"
        )));
    }
    let mut buf = vec![0u8; catalog_size];

    // ---- Node 0: B-tree header node (kind=1, 3 records) ----
    buf[8] = hfs_common::BTREE_HEADER_NODE as u8;
    buf[9] = 0;
    BigEndian::write_u16(&mut buf[10..12], 3);

    // BTHeaderRec at offset 14
    BigEndian::write_u16(&mut buf[14..16], 1); // depth
    BigEndian::write_u32(&mut buf[16..20], 1); // rootNode = 1
    BigEndian::write_u32(&mut buf[20..24], 2); // leafRecords
    BigEndian::write_u32(&mut buf[24..28], 1); // firstLeafNode
    BigEndian::write_u32(&mut buf[28..32], 1); // lastLeafNode
    BigEndian::write_u16(&mut buf[32..34], node_size as u16); // nodeSize
    BigEndian::write_u16(&mut buf[34..36], 37); // maxKeyLen
    BigEndian::write_u32(&mut buf[36..40], total_nodes);
    BigEndian::write_u32(&mut buf[40..44], total_nodes - 2); // freeNodes

    // Record 1: user data at offset 120 (128 bytes zero)
    // Record 2: node-allocation bitmap at offset 248; mark nodes 0 and 1 used.
    let bitmap_rec_offset = 248usize;
    buf[bitmap_rec_offset] = 0b11000000;

    // Offset table for header node (stored bottom-up)
    let free_offset = (node_size - 8) as u16;
    BigEndian::write_u16(&mut buf[node_size - 2..node_size], 14);
    BigEndian::write_u16(&mut buf[node_size - 4..node_size - 2], 120);
    BigEndian::write_u16(
        &mut buf[node_size - 6..node_size - 4],
        bitmap_rec_offset as u16,
    );
    BigEndian::write_u16(&mut buf[node_size - 8..node_size - 6], free_offset);

    // ---- Node 1: Leaf node with root dir record + thread ----
    let leaf = node_size;
    buf[leaf + 8] = hfs_common::BTREE_LEAF_NODE as u8; // -1
    buf[leaf + 9] = 1; // height
    BigEndian::write_u16(&mut buf[leaf + 10..leaf + 12], 2); // 2 records

    // Record 0: Root directory (key: parent CNID 1, empty name)
    let r0_key = leaf + 14;
    buf[r0_key] = 6; // key_len = resv+parID+nameLen = 6
    buf[r0_key + 1] = 0;
    BigEndian::write_u32(&mut buf[r0_key + 2..r0_key + 6], 1);
    buf[r0_key + 6] = 0; // name_len
    buf[r0_key + 7] = 0; // pad to even
    let r0_data = r0_key + 8; // 22
    buf[r0_data] = CATALOG_DIR as u8;
    BigEndian::write_u32(&mut buf[r0_data + 6..r0_data + 10], 2); // dirDirID = root CNID 2
    let now = hfs_common::hfs_now();
    BigEndian::write_u32(&mut buf[r0_data + 10..r0_data + 14], now); // crDate
    BigEndian::write_u32(&mut buf[r0_data + 14..r0_data + 18], now); // mdDate
                                                                     // Remainder of the 70-byte CdrDirRec stays zero.

    // Record 1: Root directory thread (key: parent CNID 2, empty name)
    let r1_key = r0_data + 70; // 92
    buf[r1_key] = 6;
    buf[r1_key + 1] = 0;
    BigEndian::write_u32(&mut buf[r1_key + 2..r1_key + 6], 2);
    buf[r1_key + 6] = 0;
    buf[r1_key + 7] = 0;
    let r1_data = r1_key + 8; // 100
    buf[r1_data] = CATALOG_DIR_THREAD as u8;
    BigEndian::write_u32(&mut buf[r1_data + 10..r1_data + 14], 1); // thdParID = 1
                                                                   // Remaining bytes (Str31 name, 32 bytes) stay zero. Thread record = 46 bytes.

    let free_rel = (r1_data + 46 - leaf) as u16; // 146

    // Leaf offset table (2 records + free space)
    BigEndian::write_u16(&mut buf[leaf + node_size - 2..leaf + node_size], 14);
    BigEndian::write_u16(
        &mut buf[leaf + node_size - 4..leaf + node_size - 2],
        (r1_key - leaf) as u16,
    );
    BigEndian::write_u16(
        &mut buf[leaf + node_size - 6..leaf + node_size - 4],
        free_rel,
    );

    Ok(buf)
}

/// Build a fresh classic HFS extents-overflow B-tree containing only the
/// header node (no leaf records). This is the minimal valid shape for a
/// volume with no files that spill beyond three extents per fork.
fn build_empty_hfs_extents_btree(size: usize) -> Result<Vec<u8>, FilesystemError> {
    let node_size = 512usize;
    let total_nodes = (size / node_size) as u32;
    if total_nodes < 1 {
        return Err(FilesystemError::InvalidData(format!(
            "cannot initialize empty HFS extents B-tree: region is only {size} bytes"
        )));
    }
    let mut buf = vec![0u8; size];

    buf[8] = hfs_common::BTREE_HEADER_NODE as u8;
    buf[9] = 0;
    BigEndian::write_u16(&mut buf[10..12], 3);

    // BTHeaderRec: depth=0, root=0, no leaves.
    BigEndian::write_u16(&mut buf[14..16], 0); // depth
    BigEndian::write_u32(&mut buf[16..20], 0); // rootNode
    BigEndian::write_u32(&mut buf[20..24], 0); // leafRecords
    BigEndian::write_u32(&mut buf[24..28], 0); // firstLeafNode
    BigEndian::write_u32(&mut buf[28..32], 0); // lastLeafNode
    BigEndian::write_u16(&mut buf[32..34], node_size as u16); // nodeSize
    BigEndian::write_u16(&mut buf[34..36], 7); // maxKeyLen for extents key (keyLen+forkType+fileID+startBlock)
    BigEndian::write_u32(&mut buf[36..40], total_nodes);
    BigEndian::write_u32(&mut buf[40..44], total_nodes - 1); // only header node used

    // Record 1: user data (128 bytes, zero) at offset 120
    // Record 2: node bitmap at offset 248; mark only node 0 (header).
    let bitmap_rec_offset = 248usize;
    buf[bitmap_rec_offset] = 0b10000000;

    let free_offset = (node_size - 8) as u16;
    BigEndian::write_u16(&mut buf[node_size - 2..node_size], 14);
    BigEndian::write_u16(&mut buf[node_size - 4..node_size - 2], 120);
    BigEndian::write_u16(
        &mut buf[node_size - 6..node_size - 4],
        bitmap_rec_offset as u16,
    );
    BigEndian::write_u16(&mut buf[node_size - 8..node_size - 6], free_offset);

    Ok(buf)
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
            aux_type: None,
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
                    rsrc_extents,
                    type_code,
                    creator_code,
                    finder_flags,
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
                    // Classic-Mac alias: fdFlags bit 0x8000 set. Read the
                    // resource fork (small for aliases, typically <4KB) and
                    // parse the `alis` resource to surface the target.
                    if finder_flags & super::mac_alias::IS_ALIAS_FLAG != 0 && rsrc_size > 0 {
                        if let Ok(rsrc) = read_fork_data(
                            &mut self.reader,
                            self.partition_offset,
                            &self.mdb,
                            &rsrc_extents,
                            rsrc_size as u64,
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

    fn validate_name(&self, name: &str) -> Result<(), FilesystemError> {
        validate_hfs_create_name(name).map(|_| ())
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

    fn blessed_system_folder(&mut self) -> Option<(u64, String)> {
        let cnid = self.mdb.finder_info[0];
        if cnid == 0 {
            return None;
        }
        // Look up folder name via thread record
        if let Some((_node, _rec, offset)) = self.find_catalog_record_by_cnid(cnid) {
            let key_len = self.catalog_data[offset] as usize;
            let mut rec_data_start = offset + 1 + key_len;
            if rec_data_start % 2 != 0 {
                rec_data_start += 1;
            }
            // Thread record: type(1) + reserved(1) + reserved(8) + parentID(4) + name(Pascal)
            if rec_data_start + 15 <= self.catalog_data.len() {
                let name_len = self.catalog_data[rec_data_start + 14] as usize;
                if rec_data_start + 15 + name_len <= self.catalog_data.len() {
                    let name = mac_roman_to_utf8(
                        &self.catalog_data[rec_data_start + 15..rec_data_start + 15 + name_len],
                    );
                    return Some((cnid as u64, name));
                }
            }
        }
        Some((cnid as u64, format!("CNID {}", cnid)))
    }

    fn fsck(&mut self) -> Option<Result<super::fsck::FsckResult, FilesystemError>> {
        Some(self.fsck())
    }
}

/// Classic HFS thread record types.
const CATALOG_DIR_THREAD: i8 = 3;
const CATALOG_FILE_THREAD: i8 = 4;

impl<R: Read + Write + Seek + Send> EditableFilesystem for HfsFilesystem<R> {
    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn std::io::Read,
        data_len: u64,
        options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        self.ensure_catalog_initialized()?;
        let snap = self.snapshot();
        let result = (|| {
            let parent_id = parent.location as u32;

            let name_bytes = validate_hfs_create_name(name)?;

            // Check for duplicates
            if self
                .find_catalog_record_by_name(parent_id, &name_bytes)
                .is_some()
            {
                return Err(FilesystemError::AlreadyExists(name.into()));
            }

            // Assign CNID
            let file_id = self.mdb.next_catalog_id;
            self.mdb.next_catalog_id += 1;

            // Determine type/creator
            let ext = name.rsplit('.').next().unwrap_or("");
            let (type_code, creator_code) = if let (Some(tc), Some(cc)) =
                (options.type_code.as_ref(), options.creator_code.as_ref())
            {
                (hfs_common::encode_fourcc(tc), hfs_common::encode_fourcc(cc))
            } else {
                hfs_common::type_creator_for_extension(ext).unwrap_or(([0; 4], [0; 4]))
            };

            // Allocate blocks and write data
            let (data_start, data_blocks) = self.write_data_to_blocks(data, data_len)?;

            // Handle resource fork
            let (rsrc_start, rsrc_blocks, rsrc_size) =
                if let Some(ref rsrc_src) = options.resource_fork {
                    match rsrc_src {
                        super::filesystem::ResourceForkSource::Data(rsrc_data) => {
                            let mut cursor = std::io::Cursor::new(rsrc_data);
                            let (rs, rb) =
                                self.write_data_to_blocks(&mut cursor, rsrc_data.len() as u64)?;
                            (rs, rb, rsrc_data.len() as u32)
                        }
                        super::filesystem::ResourceForkSource::File(path) => {
                            let mut f = std::fs::File::open(path)?;
                            let len = f.metadata()?.len();
                            let (rs, rb) = self.write_data_to_blocks(&mut f, len)?;
                            (rs, rb, len as u32)
                        }
                    }
                } else {
                    (0, 0, 0)
                };

            // Build file record
            let file_rec = Self::build_file_record(
                file_id,
                data_len as u32,
                data_start,
                data_blocks,
                rsrc_size,
                rsrc_start,
                rsrc_blocks,
                &type_code,
                &creator_code,
                self.mdb.block_size,
            );

            // Build key + record for catalog insertion
            let key = Self::build_catalog_key(parent_id, &name_bytes);
            let mut key_record = key;
            key_record.extend_from_slice(&file_rec);

            self.insert_catalog_record(&key_record)?;

            // Build and insert thread record
            let (_, thread_data) =
                Self::build_thread_record(CATALOG_FILE_THREAD, parent_id, &name_bytes);
            let thread_key = Self::build_catalog_key(file_id, &[]);
            let mut thread_record = thread_key;
            thread_record.extend_from_slice(&thread_data);
            self.insert_catalog_record(&thread_record)?;

            // Update parent valence
            self.update_parent_valence(parent_id, 1)?;

            // Update MDB counts
            self.mdb.file_count += 1;

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
            if rsrc_size > 0 {
                fe.resource_fork_size = Some(rsrc_size as u64);
            }
            Ok(fe)
        })();
        if result.is_err() {
            self.restore_snapshot(snap);
        }
        result
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        self.ensure_catalog_initialized()?;
        let snap = self.snapshot();
        let result = (|| {
            let parent_id = parent.location as u32;

            let name_bytes = validate_hfs_create_name(name)?;

            // Check duplicates
            if self
                .find_catalog_record_by_name(parent_id, &name_bytes)
                .is_some()
            {
                return Err(FilesystemError::AlreadyExists(name.into()));
            }

            // Assign CNID
            let folder_id = self.mdb.next_catalog_id;
            self.mdb.next_catalog_id += 1;

            // Build folder record
            let folder_rec = Self::build_dir_record(folder_id);

            // Build key + record
            let key = Self::build_catalog_key(parent_id, &name_bytes);
            let mut key_record = key;
            key_record.extend_from_slice(&folder_rec);

            self.insert_catalog_record(&key_record)?;

            // Thread record
            let (_, thread_data) =
                Self::build_thread_record(CATALOG_DIR_THREAD, parent_id, &name_bytes);
            let thread_key = Self::build_catalog_key(folder_id, &[]);
            let mut thread_record = thread_key;
            thread_record.extend_from_slice(&thread_data);
            self.insert_catalog_record(&thread_record)?;

            // Update parent valence
            self.update_parent_valence(parent_id, 1)?;

            // Update MDB
            self.mdb.folder_count += 1;

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
        })();
        if result.is_err() {
            self.restore_snapshot(snap);
        }
        result
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        let snap = self.snapshot();
        let result = (|| {
            let parent_id = parent.location as u32;
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

            // Find the entry record
            let name_bytes = utf8_to_mac_roman(&entry.name)?;
            let (node_idx, rec_idx, _offset) = self
                .find_catalog_record_by_name(parent_id, &name_bytes)
                .ok_or_else(|| {
                    FilesystemError::NotFound(format!(
                        "entry '{}' not found in catalog",
                        entry.name
                    ))
                })?;

            // If it's a file, free its fork blocks
            if !entry.is_directory() {
                if let Some((_, data_extents, _, rsrc_extents)) = self.find_file_by_id(cnid) {
                    self.free_extent_blocks(&data_extents);
                    self.free_extent_blocks(&rsrc_extents);
                }
            }

            self.remove_catalog_record(node_idx, rec_idx);

            // Find and remove the thread record
            if let Some((t_node, t_rec, _)) = self.find_catalog_record_by_cnid(cnid) {
                self.remove_catalog_record(t_node, t_rec);
            }

            // Update parent valence
            self.update_parent_valence(parent_id, -1)?;

            // Update MDB counts
            if entry.is_directory() {
                self.mdb.folder_count = self.mdb.folder_count.saturating_sub(1);
            } else {
                self.mdb.file_count = self.mdb.file_count.saturating_sub(1);
            }

            Ok(())
        })();
        if result.is_err() {
            self.restore_snapshot(snap);
        }
        result
    }

    fn set_type_creator(
        &mut self,
        entry: &FileEntry,
        type_code: &str,
        creator_code: &str,
    ) -> Result<(), FilesystemError> {
        let snap = self.snapshot();
        let result = (|| {
            let cnid = entry.location as u32;

            // Find the thread to get parent + name
            let (_t_node, _t_rec, t_offset) =
                self.find_catalog_record_by_cnid(cnid).ok_or_else(|| {
                    FilesystemError::NotFound(format!("thread for CNID {cnid} not found"))
                })?;

            // Read parent_id and name from thread record data
            let key_len = self.catalog_data[t_offset] as usize;
            let mut rec_data_start = t_offset + 1 + key_len;
            if rec_data_start % 2 != 0 {
                rec_data_start += 1;
            }
            // Thread record: type(1) + reserved(1) + reserved(8) + parentID(4) + name(Pascal)
            let thread_parent =
                BigEndian::read_u32(&self.catalog_data[rec_data_start + 10..rec_data_start + 14]);
            let thread_name_len = self.catalog_data[rec_data_start + 14] as usize;
            let thread_name = self.catalog_data
                [rec_data_start + 15..rec_data_start + 15 + thread_name_len]
                .to_vec();

            // Find the actual file record
            let (_f_node, _f_rec, f_offset) = self
                .find_catalog_record_by_name(thread_parent, &thread_name)
                .ok_or_else(|| FilesystemError::NotFound("file record not found".into()))?;

            // Compute record data offset
            let fkey_len = self.catalog_data[f_offset] as usize;
            let mut frec_start = f_offset + 1 + fkey_len;
            if frec_start % 2 != 0 {
                frec_start += 1;
            }

            // Write type at offset 4, creator at offset 8 in the file record
            let tc = hfs_common::encode_fourcc(type_code);
            let cc = hfs_common::encode_fourcc(creator_code);
            self.catalog_data[frec_start + 4..frec_start + 8].copy_from_slice(&tc);
            self.catalog_data[frec_start + 8..frec_start + 12].copy_from_slice(&cc);

            Ok(())
        })();
        if result.is_err() {
            self.restore_snapshot(snap);
        }
        result
    }

    fn write_resource_fork(
        &mut self,
        entry: &FileEntry,
        data: &mut dyn std::io::Read,
        len: u64,
    ) -> Result<(), FilesystemError> {
        let snap = self.snapshot();
        let result = (|| {
            let cnid = entry.location as u32;

            // Free existing resource fork blocks
            if let Some((_, _, _, rsrc_extents)) = self.find_file_by_id(cnid) {
                self.free_extent_blocks(&rsrc_extents);
            }

            // Allocate and write new resource fork
            let (rsrc_start, rsrc_blocks) = self.write_data_to_blocks(data, len)?;

            // Find the file record via thread
            let (_t_node, _t_rec, t_offset) =
                self.find_catalog_record_by_cnid(cnid).ok_or_else(|| {
                    FilesystemError::NotFound(format!("thread for CNID {cnid} not found"))
                })?;

            let key_len = self.catalog_data[t_offset] as usize;
            let mut rec_data_start = t_offset + 1 + key_len;
            if rec_data_start % 2 != 0 {
                rec_data_start += 1;
            }
            let thread_parent =
                BigEndian::read_u32(&self.catalog_data[rec_data_start + 10..rec_data_start + 14]);
            let thread_name_len = self.catalog_data[rec_data_start + 14] as usize;
            let thread_name = self.catalog_data
                [rec_data_start + 15..rec_data_start + 15 + thread_name_len]
                .to_vec();

            let (_f_node, _f_rec, f_offset) = self
                .find_catalog_record_by_name(thread_parent, &thread_name)
                .ok_or_else(|| FilesystemError::NotFound("file record not found".into()))?;

            let fkey_len = self.catalog_data[f_offset] as usize;
            let mut frec_start = f_offset + 1 + fkey_len;
            if frec_start % 2 != 0 {
                frec_start += 1;
            }

            // Update rsrc fork fields in file record:
            // filRStBlk at offset 34
            BigEndian::write_u16(
                &mut self.catalog_data[frec_start + 34..frec_start + 36],
                rsrc_start,
            );
            // filRLgLen at offset 36
            BigEndian::write_u32(
                &mut self.catalog_data[frec_start + 36..frec_start + 40],
                len as u32,
            );
            // filRPyLen at offset 40
            BigEndian::write_u32(
                &mut self.catalog_data[frec_start + 40..frec_start + 44],
                rsrc_blocks as u32 * self.mdb.block_size,
            );
            // Rsrc extents at offset 86
            BigEndian::write_u16(
                &mut self.catalog_data[frec_start + 86..frec_start + 88],
                rsrc_start,
            );
            BigEndian::write_u16(
                &mut self.catalog_data[frec_start + 88..frec_start + 90],
                rsrc_blocks,
            );
            // Clear remaining rsrc extent slots
            self.catalog_data[frec_start + 90..frec_start + 98].fill(0);

            Ok(())
        })();
        if result.is_err() {
            self.restore_snapshot(snap);
        }
        result
    }

    fn repair(&mut self) -> Result<super::fsck::RepairReport, FilesystemError> {
        self.ensure_bitmap()?;
        let mut extents_data = if self.mdb.extents_file_size > 0 {
            read_fork_data(
                &mut self.reader,
                self.partition_offset,
                &self.mdb,
                &self.mdb.extents_file_extents,
                self.mdb.extents_file_size as u64,
            )
            .ok()
        } else {
            None
        };

        let report = super::hfs_fsck::repair_hfs(
            &mut self.mdb,
            &mut self.catalog_data,
            self.bitmap.as_mut().unwrap(),
            extents_data.as_mut(),
        );

        // Write back repaired extents overflow B-tree if modified
        if let Some(ref ext_data) = extents_data {
            if self.mdb.extents_file_size > 0 {
                let alloc_start = self.mdb.first_alloc_block as u64 * 512;
                for (i, extent) in self.mdb.extents_file_extents.iter().enumerate() {
                    if extent.block_count == 0 {
                        break;
                    }
                    let block_off = self.partition_offset
                        + alloc_start
                        + extent.start_block as u64 * self.mdb.block_size as u64;
                    let byte_len = extent.block_count as u64 * self.mdb.block_size as u64;
                    let data_start = if i == 0 {
                        0
                    } else {
                        self.mdb.extents_file_extents[..i]
                            .iter()
                            .map(|e| e.block_count as u64 * self.mdb.block_size as u64)
                            .sum::<u64>() as usize
                    };
                    let data_end = (data_start + byte_len as usize).min(ext_data.len());
                    if data_start < data_end {
                        self.reader.seek(std::io::SeekFrom::Start(block_off))?;
                        self.reader.write_all(&ext_data[data_start..data_end])?;
                    }
                }
            }
        }

        self.do_sync_metadata()?;
        Ok(report)
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.do_sync_metadata()
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.mdb.free_blocks as u64 * self.mdb.block_size as u64)
    }

    fn set_blessed_folder(&mut self, entry: &FileEntry) -> Result<(), FilesystemError> {
        let snap = self.snapshot();
        let result = (|| {
            if !entry.is_directory() {
                return Err(FilesystemError::InvalidData(
                    "can only bless a directory".into(),
                ));
            }
            self.mdb.finder_info[0] = entry.location as u32;
            Ok(())
        })();
        if result.is_err() {
            self.restore_snapshot(snap);
        }
        result
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

/// Write fork data to disk through extent descriptors.
fn write_hfs_fork_data<R: Write + Seek>(
    writer: &mut R,
    partition_offset: u64,
    mdb: &HfsMasterDirectoryBlock,
    extents: &[HfsExtDescriptor; 3],
    data: &[u8],
) -> Result<(), FilesystemError> {
    let first_alloc_offset = partition_offset + mdb.first_alloc_block as u64 * 512;
    let mut written = 0usize;
    for ext in extents {
        if ext.block_count == 0 || written >= data.len() {
            break;
        }
        let offset = first_alloc_offset + ext.start_block as u64 * mdb.block_size as u64;
        let extent_len = ext.block_count as u64 * mdb.block_size as u64;
        let to_write = extent_len.min((data.len() - written) as u64) as usize;
        writer.seek(SeekFrom::Start(offset))?;
        writer.write_all(&data[written..written + to_write])?;
        written += to_write;
    }
    Ok(())
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
        // data_size: pre-alloc is always read from disk; only allocated alloc blocks are read.
        let data_size = pre_alloc_size + allocated as u64 * mdb.block_size as u64;
        eprintln!(
            "[HFS compact] pre_alloc_size={}, data_size={}, compacted_size={} original_size={} (layout-preserving; free blocks → zeros)",
            pre_alloc_size, data_size, compacted_size, original_size
        );

        let result = CompactResult {
            original_size,
            compacted_size,
            data_size,
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
    // Read MDB sector (512-byte aligned I/O for raw device compatibility)
    // The MDB is at partition_offset + 1024 and is 162 bytes, but we read/write
    // a full 512-byte sector to satisfy raw device alignment requirements.
    device.seek(SeekFrom::Start(partition_offset + 1024))?;
    let mut sector = [0u8; 512];
    device.read_exact(&mut sector)?;

    let sig = BigEndian::read_u16(&sector[0..2]);
    if sig != HFS_SIGNATURE {
        log("HFS resize: not an HFS volume, skipping");
        return Ok(());
    }

    let block_size = BigEndian::read_u32(&sector[20..24]);
    let old_total = BigEndian::read_u16(&sector[18..20]);
    let free_blocks = BigEndian::read_u16(&sector[34..36]);
    let first_alloc = BigEndian::read_u16(&sector[28..30]);
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

    // Update MDB fields in the sector buffer
    BigEndian::write_u16(&mut sector[18..20], new_total);
    BigEndian::write_u16(&mut sector[34..36], new_free);

    // Write primary MDB sector at offset + 1024
    device.seek(SeekFrom::Start(partition_offset + 1024))?;
    device.write_all(&sector)?;

    // Write backup MDB sector at offset + new_size - 1024
    if new_size_bytes > 1024 {
        device.seek(SeekFrom::Start(partition_offset + new_size_bytes - 1024))?;
        device.write_all(&sector)?;
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
    // Read MDB sector (512-byte aligned I/O for raw device compatibility)
    device.seek(SeekFrom::Start(partition_offset + 1024))?;
    let mut sector = [0u8; 512];
    device.read_exact(&mut sector)?;

    let sig = BigEndian::read_u16(&sector[0..2]);
    if sig != HFS_SIGNATURE {
        log("HFS validate: not an HFS volume, skipping");
        return Ok(());
    }

    let block_size = BigEndian::read_u32(&sector[20..24]);
    let total_blocks = BigEndian::read_u16(&sector[18..20]);

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

    #[test]
    fn test_utf8_to_mac_roman_roundtrip() {
        let text = "Hello World";
        let mac = utf8_to_mac_roman(text).unwrap();
        assert_eq!(mac, b"Hello World");
        assert_eq!(mac_roman_to_utf8(&mac), text);
    }

    #[test]
    fn test_utf8_to_mac_roman_special() {
        let text = "Ä"; // U+00C4 => Mac Roman 0x80
        let mac = utf8_to_mac_roman(text).unwrap();
        assert_eq!(mac, &[0x80]);
        assert_eq!(mac_roman_to_utf8(&mac), text);
    }

    #[test]
    fn test_utf8_to_mac_roman_unencodable() {
        let text = "\u{4E2D}"; // Chinese character — not in Mac Roman
        assert!(utf8_to_mac_roman(text).is_err());
    }

    #[test]
    fn test_mdb_serialize_roundtrip() {
        let mut data = [0u8; 512];
        BigEndian::write_u16(&mut data[0..2], HFS_SIGNATURE);
        BigEndian::write_u16(&mut data[18..20], 100);
        BigEndian::write_u32(&mut data[20..24], 4096);
        BigEndian::write_u16(&mut data[34..36], 50);
        BigEndian::write_u32(&mut data[30..34], 10); // next_catalog_id
        BigEndian::write_u32(&mut data[92..96], 42); // finder_info[0]
        data[36] = 4;
        data[37..41].copy_from_slice(b"Test");

        let mdb = HfsMasterDirectoryBlock::parse(&data).unwrap();
        let serialized = mdb.serialize_to_sector();
        assert_eq!(BigEndian::read_u16(&serialized[18..20]), 100);
        assert_eq!(BigEndian::read_u32(&serialized[30..34]), 10);
        assert_eq!(BigEndian::read_u32(&serialized[92..96]), 42);
    }

    /// Create a minimal valid in-memory classic HFS image for testing.
    /// Layout: 128 blocks × 4096 bytes = 512 KB
    /// - Sectors 0-1: Boot blocks (1024 bytes)
    /// - Sector 2: MDB (1024 bytes)
    /// - Sectors 3-4: Volume bitmap (1024 bytes, covers 128 blocks = 16 bytes needed)
    /// - first_alloc_block = 5 (sectors), allocation starts at byte 2560
    /// - Block 0-3: catalog B-tree (4 blocks = 16 KB)
    ///   - Node 0: header node (4096 bytes)
    ///   - Node 1: leaf node with root folder + thread
    ///   - Nodes 2-3: free
    /// - Blocks 4+: free for user data
    fn make_editable_hfs_image() -> Vec<u8> {
        let block_size = 4096u32;
        let total_blocks = 128u16;
        let first_alloc_block = 5u16; // start at sector 5 (byte 2560)
        let alloc_start = first_alloc_block as usize * 512;
        let image_size = alloc_start + total_blocks as usize * block_size as usize;
        let mut img = vec![0u8; image_size];

        let catalog_start = 0u16; // first 4 allocation blocks
        let catalog_blocks = 4u16;
        let catalog_size = catalog_blocks as u32 * block_size;

        // Volume bitmap at sector 3 (byte 1536)
        let bitmap_sector = 3u16;
        let bitmap_off = bitmap_sector as usize * 512;
        // Mark blocks 0-3 as allocated (catalog)
        img[bitmap_off] = 0b11110000;

        // Build catalog B-tree
        let node_size = 4096usize;
        let catalog_offset = alloc_start; // block 0 of allocation area

        // Node 0: Header node
        let hdr_off = catalog_offset;
        img[hdr_off + 8] = 1; // kind = header node
        BigEndian::write_u16(&mut img[hdr_off + 10..hdr_off + 12], 3); // 3 records

        // B-tree header record (record 0, at offset 14)
        let hr = hdr_off + 14;
        BigEndian::write_u16(&mut img[hr..hr + 2], 1); // depth = 1
        BigEndian::write_u32(&mut img[hr + 2..hr + 6], 1); // root_node = 1
        BigEndian::write_u32(&mut img[hr + 6..hr + 10], 2); // leaf_records = 2
        BigEndian::write_u32(&mut img[hr + 10..hr + 14], 1); // first_leaf_node = 1
        BigEndian::write_u32(&mut img[hr + 14..hr + 18], 1); // last_leaf_node = 1
        BigEndian::write_u16(&mut img[hr + 18..hr + 20], node_size as u16); // node_size
        BigEndian::write_u16(&mut img[hr + 20..hr + 22], 37); // max_key_len
        BigEndian::write_u32(&mut img[hr + 22..hr + 26], 4); // total_nodes = 4
        BigEndian::write_u32(&mut img[hr + 26..hr + 30], 2); // free_nodes = 2

        // Record offset table for header node
        let ot = hdr_off + node_size;
        BigEndian::write_u16(&mut img[ot - 2..ot], 14); // record 0
        BigEndian::write_u16(&mut img[ot - 4..ot - 2], 142); // record 1 (user data)
        BigEndian::write_u16(&mut img[ot - 6..ot - 4], 270); // record 2 (bitmap)
        BigEndian::write_u16(&mut img[ot - 8..ot - 6], 526); // free space

        // Node bitmap (record 2): mark nodes 0 and 1 as allocated
        img[hdr_off + 270] = 0b11000000;

        // Node 1: Leaf node with root folder + thread
        let leaf_off = catalog_offset + node_size;
        img[leaf_off + 8] = 0xFF; // kind = -1 (leaf)
        img[leaf_off + 9] = 1; // height = 1
        BigEndian::write_u16(&mut img[leaf_off + 10..leaf_off + 12], 2); // 2 records

        // Record 0: Root folder record (CNID 2)
        // Key: key_len(1) + reserved(1) + parent_id(4, =1) + name_len(1) + name(0) = 7 bytes
        // Pad to even: 8 bytes total
        let r0_off = leaf_off + 14;
        img[r0_off] = 6; // key_len = 6 (reserved + parent_id + name_len)
        img[r0_off + 1] = 0; // reserved
        BigEndian::write_u32(&mut img[r0_off + 2..r0_off + 6], 1); // parent_id = 1
        img[r0_off + 6] = 0; // name_len = 0
                             // Pad to even
        img[r0_off + 7] = 0;
        // Record data at offset 8 (even-aligned)
        let r0_data = r0_off + 8;
        img[r0_data] = CATALOG_DIR as u8; // type = directory
                                          // dir record: type(1) + reserved(1) + flags(2) + valence(2) + dirID(4) + ...
        BigEndian::write_u32(&mut img[r0_data + 6..r0_data + 10], 2); // dirID = 2
                                                                      // Dir record = 70 bytes

        // Record 1: Thread record for root (CNID 2)
        let r1_off = r0_data + 70;
        img[r1_off] = 6; // key_len = 6
        img[r1_off + 1] = 0; // reserved
        BigEndian::write_u32(&mut img[r1_off + 2..r1_off + 6], 2); // parent_id = 2 (CNID)
        img[r1_off + 6] = 0; // name_len = 0
        img[r1_off + 7] = 0; // padding
        let r1_data = r1_off + 8;
        img[r1_data] = CATALOG_DIR_THREAD as u8; // type = dir thread
        img[r1_data + 1] = 0; // reserved
                              // reserved(8) at offset 2-9
        BigEndian::write_u32(&mut img[r1_data + 10..r1_data + 14], 1); // parentID = 1
                                                                       // Str31 name field: 32 bytes (1 length byte + 31 bytes name/padding)
        img[r1_data + 14] = 0; // name_len = 0 (rest of 32 bytes already zero)
                               // Thread data total = 2 (type+rsv) + 8 (rsv) + 4 (parentID) + 32 (Str31) = 46 bytes

        // Record offset table for leaf node
        let lot = leaf_off + node_size;
        BigEndian::write_u16(&mut img[lot - 2..lot], 14); // record 0
        let r1_rel = (r1_off - leaf_off) as u16;
        BigEndian::write_u16(&mut img[lot - 4..lot - 2], r1_rel); // record 1
        let free_rel = (r1_data + 46 - leaf_off) as u16;
        BigEndian::write_u16(&mut img[lot - 6..lot - 4], free_rel); // free space

        // MDB at byte 1024
        let mdb_off = 1024;
        let mut mdb = [0u8; 512];
        BigEndian::write_u16(&mut mdb[0..2], HFS_SIGNATURE);
        BigEndian::write_u32(&mut mdb[2..6], hfs_common::hfs_now()); // create date
        BigEndian::write_u32(&mut mdb[6..10], hfs_common::hfs_now()); // modify date
        BigEndian::write_u16(&mut mdb[14..16], bitmap_sector); // volume bitmap block
        BigEndian::write_u16(&mut mdb[18..20], total_blocks);
        BigEndian::write_u32(&mut mdb[20..24], block_size);
        BigEndian::write_u16(&mut mdb[28..30], first_alloc_block);
        BigEndian::write_u32(&mut mdb[30..34], 16); // next_catalog_id
        BigEndian::write_u16(&mut mdb[34..36], total_blocks - catalog_blocks); // free blocks
                                                                               // Volume name: "TestVol"
        mdb[36] = 7;
        mdb[37..44].copy_from_slice(b"TestVol");
        BigEndian::write_u32(&mut mdb[84..88], 0); // file_count
        BigEndian::write_u32(&mut mdb[88..92], 0); // folder_count (drDirCnt excludes root)
                                                   // Catalog file size
        BigEndian::write_u32(&mut mdb[146..150], catalog_size);
        // Catalog extents
        BigEndian::write_u16(&mut mdb[150..152], catalog_start);
        BigEndian::write_u16(&mut mdb[152..154], catalog_blocks);

        img[mdb_off..mdb_off + 512].copy_from_slice(&mdb);

        img
    }

    /// Create an "uninitialized" HFS image mimicking Disk Jockey's Empty HFS
    /// output: MDB present with catalog/extents extents reserved, but the
    /// catalog and extents-overflow regions are all zeros and the volume
    /// bitmap is blank.
    fn make_uninitialized_hfs_image() -> Vec<u8> {
        let block_size = 512u32;
        let total_blocks = 256u16;
        let first_alloc_block = 6u16;
        let alloc_start = first_alloc_block as usize * 512;
        let image_size = alloc_start + total_blocks as usize * block_size as usize;
        let mut img = vec![0u8; image_size];

        // Extents B-tree: blocks 0..3 (4 × 512 = 2048 bytes)
        let extents_start = 0u16;
        let extents_blocks = 4u16;
        // Catalog B-tree: blocks 4..7 (4 × 512 = 2048 bytes)
        let catalog_start = 4u16;
        let catalog_blocks = 4u16;

        // MDB at byte 1024, bitmap at sector 3 (byte 1536), both left blank
        // for the catalog and extents regions.
        let mdb_off = 1024usize;
        let mut mdb = [0u8; 512];
        BigEndian::write_u16(&mut mdb[0..2], HFS_SIGNATURE);
        BigEndian::write_u32(&mut mdb[2..6], hfs_common::hfs_now());
        BigEndian::write_u32(&mut mdb[6..10], hfs_common::hfs_now());
        BigEndian::write_u16(&mut mdb[14..16], 3); // vbm_block sector
        BigEndian::write_u16(&mut mdb[18..20], total_blocks);
        BigEndian::write_u32(&mut mdb[20..24], block_size);
        BigEndian::write_u16(&mut mdb[28..30], first_alloc_block);
        BigEndian::write_u32(&mut mdb[30..34], 16);
        BigEndian::write_u16(
            &mut mdb[34..36],
            total_blocks - extents_blocks - catalog_blocks,
        );
        // Volume name "Blank"
        mdb[36] = 5;
        mdb[37..42].copy_from_slice(b"Blank");
        // Extents file
        BigEndian::write_u32(&mut mdb[130..134], extents_blocks as u32 * block_size);
        BigEndian::write_u16(&mut mdb[134..136], extents_start);
        BigEndian::write_u16(&mut mdb[136..138], extents_blocks);
        // Catalog file
        BigEndian::write_u32(&mut mdb[146..150], catalog_blocks as u32 * block_size);
        BigEndian::write_u16(&mut mdb[150..152], catalog_start);
        BigEndian::write_u16(&mut mdb[152..154], catalog_blocks);

        img[mdb_off..mdb_off + 512].copy_from_slice(&mdb);
        img
    }

    #[test]
    fn test_hfs_empty_volume_auto_initialize_and_create_file() {
        // Blank catalog + extents regions should be auto-initialized on the
        // first mutating call instead of panicking in btree_find_insert_leaf.
        let img = make_uninitialized_hfs_image();
        let cursor = Cursor::new(img);
        let mut fs = HfsFilesystem::open(cursor, 0).unwrap();
        assert_eq!(fs.fs_type(), "HFS");

        // list_directory on a blank catalog returns an empty list (no panic).
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert!(entries.is_empty());

        // create_file triggers the auto-init path.
        let data = b"auto-init test";
        let mut cursor = Cursor::new(data.as_slice());
        let fe = fs
            .create_file(
                &root,
                "hello.txt",
                &mut cursor,
                data.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
        assert_eq!(fe.name, "hello.txt");
        fs.sync_metadata().unwrap();

        // The new file should be listed and readable.
        let entries = fs.list_directory(&root).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "hello.txt");
        let read_back = fs.read_file(&fe, 1024).unwrap();
        assert_eq!(&read_back, data);

        // fsck should report no errors or warnings on the initialized volume.
        let result = fs.fsck().unwrap();
        let err_msgs: Vec<_> = result.errors.iter().map(|e| e.message.clone()).collect();
        let warn_msgs: Vec<_> = result.warnings.iter().map(|w| w.message.clone()).collect();
        assert!(
            result.errors.is_empty(),
            "unexpected fsck errors: {err_msgs:?}"
        );
        assert!(
            result.warnings.is_empty(),
            "unexpected fsck warnings: {warn_msgs:?}"
        );
    }

    #[test]
    fn test_hfs_editable_open_and_free_space() {
        let img = make_editable_hfs_image();
        let cursor = Cursor::new(img);
        let mut fs = HfsFilesystem::open(cursor, 0).unwrap();
        assert_eq!(fs.fs_type(), "HFS");
        assert_eq!(fs.volume_label(), Some("TestVol"));
        let free = fs.free_space().unwrap();
        // 124 free blocks × 4096 = 507,904
        assert_eq!(free, 124 * 4096);
    }

    #[test]
    fn test_hfs_editable_create_file_and_read() {
        let img = make_editable_hfs_image();
        let cursor = Cursor::new(img);
        let mut fs = HfsFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        let test_data = b"Hello, HFS World!";
        let mut data_reader = Cursor::new(test_data.as_slice());
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

        // Verify listing
        let entries = fs.list_directory(&root).unwrap();
        assert!(entries.iter().any(|e| e.name == "test.txt"));

        // Read back
        let read_back = fs.read_file(&fe, 1024).unwrap();
        assert_eq!(&read_back, test_data);
    }

    #[test]
    fn test_hfs_editable_create_directory() {
        let img = make_editable_hfs_image();
        let cursor = Cursor::new(img);
        let mut fs = HfsFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        let options = CreateDirectoryOptions::default();
        let dir = fs.create_directory(&root, "NewDir", &options).unwrap();

        assert_eq!(dir.name, "NewDir");
        assert!(dir.is_directory());

        let entries = fs.list_directory(&root).unwrap();
        assert!(entries
            .iter()
            .any(|e| e.name == "NewDir" && e.is_directory()));
    }

    #[test]
    fn test_hfs_editable_delete_file() {
        let img = make_editable_hfs_image();
        let cursor = Cursor::new(img);
        let mut fs = HfsFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        let test_data = b"delete me";
        let mut data_reader = Cursor::new(test_data.as_slice());
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
        fs.delete_entry(&root, &fe).unwrap();

        let entries = fs.list_directory(&root).unwrap();
        assert!(!entries.iter().any(|e| e.name == "gone.txt"));

        let free_after = fs.free_space().unwrap();
        assert!(free_after > free_before);
    }

    #[test]
    fn test_hfs_editable_duplicate_name_rejected() {
        let img = make_editable_hfs_image();
        let cursor = Cursor::new(img);
        let mut fs = HfsFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        let test_data = b"first";
        let mut r1 = Cursor::new(test_data.as_slice());
        let options = CreateFileOptions::default();
        fs.create_file(&root, "dup.txt", &mut r1, 5, &options)
            .unwrap();

        let mut r2 = Cursor::new(test_data.as_slice());
        let result = fs.create_file(&root, "dup.txt", &mut r2, 5, &options);
        assert!(matches!(result, Err(FilesystemError::AlreadyExists(_))));
    }

    #[test]
    fn test_hfs_editable_name_too_long() {
        let img = make_editable_hfs_image();
        let cursor = Cursor::new(img);
        let mut fs = HfsFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        let long_name = "a".repeat(32); // 32 bytes > 31 limit
        let test_data = b"x";
        let mut r = Cursor::new(test_data.as_slice());
        let options = CreateFileOptions::default();
        let result = fs.create_file(&root, &long_name, &mut r, 1, &options);
        assert!(matches!(result, Err(FilesystemError::InvalidData(_))));
    }

    #[test]
    fn test_hfs_editable_blessed_folder() {
        let img = make_editable_hfs_image();
        let cursor = Cursor::new(img);
        let mut fs = HfsFilesystem::open(cursor, 0).unwrap();

        // Initially no blessed folder
        assert!(fs.blessed_system_folder().is_none());

        // Create a system folder
        let root = fs.root().unwrap();
        let options = CreateDirectoryOptions::default();
        let sys_dir = fs.create_directory(&root, "System", &options).unwrap();

        // Bless it
        fs.set_blessed_folder(&sys_dir).unwrap();

        // Verify
        let blessed = fs.blessed_system_folder();
        assert!(blessed.is_some());
        let (cnid, name) = blessed.unwrap();
        assert_eq!(cnid, sys_dir.location);
        assert_eq!(name, "System");
    }

    #[test]
    fn test_hfs_editable_type_creator_auto_detect() {
        let img = make_editable_hfs_image();
        let cursor = Cursor::new(img);
        let mut fs = HfsFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        let test_data = b"text content";
        let mut data_reader = Cursor::new(test_data.as_slice());
        let options = CreateFileOptions::default();
        let fe = fs
            .create_file(
                &root,
                "readme.txt",
                &mut data_reader,
                test_data.len() as u64,
                &options,
            )
            .unwrap();

        assert!(fe.type_code.is_some());
        assert_eq!(fe.type_code.as_deref(), Some("TEXT"));
    }

    /// Helper: run fsck on an HfsFilesystem and panic if any errors are found.
    fn assert_fsck_clean<R: Read + Seek>(fs: &mut HfsFilesystem<R>) {
        let result = fs.fsck().unwrap();
        if !result.is_clean() {
            let msgs: Vec<_> = result
                .errors
                .iter()
                .map(|e| format!("[{}] {}", e.code, e.message))
                .collect();
            panic!("fsck found {} error(s):\n{}", msgs.len(), msgs.join("\n"));
        }
    }

    #[test]
    #[ignore] // manual test — requires real HFS image
    fn test_fsck_real_hfs_image() {
        let path = std::path::Path::new(&std::env::var("HOME").unwrap())
            .join("Documents/HD20_512 20MB Mac II Data-FullyWorking.hda");
        if !path.exists() {
            eprintln!("Skipping: {:?} not found", path);
            return;
        }
        let file = std::fs::File::open(&path).unwrap();
        // APM disk: partition 3 (Apple_HFS) starts at sector 0x60 = 96
        let partition_offset = 96 * 512;
        let mut fs = HfsFilesystem::open(file, partition_offset).unwrap();
        let result = fs.fsck().unwrap();

        eprintln!("=== ERRORS ({}) ===", result.errors.len());
        for e in &result.errors {
            eprintln!("  [{}] {}", e.code, e.message);
        }
        eprintln!("=== WARNINGS ({}) ===", result.warnings.len());
        for w in &result.warnings {
            eprintln!("  [{}] {}", w.code, w.message);
        }
        eprintln!(
            "=== STATS: {} files, {} dirs ===",
            result.stats.files_checked, result.stats.directories_checked
        );
        assert!(
            result.is_clean(),
            "known-good image should be clean ({} errors, {} warnings)",
            result.errors.len(),
            result.warnings.len()
        );
    }

    #[test]
    #[ignore] // manual test — requires real HFS image
    fn test_fsck_pm6100_compare() {
        let paths = [
            (
                "/Volumes/Software/VintageSystemBackups/HD40_imagedPowerMac6100.hda",
                "ORIGINAL",
            ),
            (
                "/Volumes/Software/VintageSystemBackups/DiskWarriorFixed/HD50_pm6100 hdd.hda",
                "DISKWARRIOR FIXED",
            ),
        ];

        for (path, label) in &paths {
            let p = std::path::Path::new(path);
            if !p.exists() {
                eprintln!("Skipping {}: not found", path);
                continue;
            }
            eprintln!("\n========== {} ==========", label);
            eprintln!("File: {}", path);

            let file = std::fs::File::open(p).unwrap();
            let mut reader = std::io::BufReader::new(file);

            // Find Apple_HFS partition in APM
            let mut hfs_offset = 0u64;
            for i in 1..64u64 {
                use std::io::{Read, Seek};
                reader.seek(SeekFrom::Start(i * 512)).unwrap();
                let mut buf = [0u8; 512];
                if reader.read_exact(&mut buf).is_err() {
                    break;
                }
                if buf[0] != 0x50 || buf[1] != 0x4D {
                    break;
                }
                let start = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
                let count = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
                let tstr: String = buf[48..80]
                    .iter()
                    .take_while(|&&b| b != 0)
                    .map(|&b| b as char)
                    .collect();
                eprintln!(
                    "  Part {}: {:32} start={:>8} blocks={:>8}",
                    i, tstr, start, count
                );
                if tstr == "Apple_HFS" && hfs_offset == 0 {
                    hfs_offset = start as u64 * 512;
                }
            }
            eprintln!("  -> HFS partition at offset {}", hfs_offset);

            let file2 = std::fs::File::open(p).unwrap();
            let mut fs = HfsFilesystem::open(file2, hfs_offset).unwrap();
            let result = fs.fsck().unwrap();

            eprintln!("=== ERRORS ({}) ===", result.errors.len());
            for e in &result.errors {
                eprintln!("  [{}] {}", e.code, e.message);
            }
            eprintln!("=== WARNINGS ({}) ===", result.warnings.len());
            for w in &result.warnings {
                eprintln!("  [{}] {}", w.code, w.message);
            }
            eprintln!(
                "=== STATS: {} files, {} dirs ===",
                result.stats.files_checked, result.stats.directories_checked
            );
            for (k, v) in &result.stats.extra {
                eprintln!("  {} = {}", k, v);
            }
        }
    }

    #[test]
    fn test_fsck_clean_fresh_image() {
        let img = make_editable_hfs_image();
        let cursor = Cursor::new(img);
        let mut fs = HfsFilesystem::open(cursor, 0).unwrap();

        let result = fs.fsck().unwrap();
        let err_msgs: Vec<&str> = result.errors.iter().map(|e| e.message.as_str()).collect();
        assert!(
            result.is_clean(),
            "fresh image should be clean: {:?}",
            err_msgs
        );
        assert_eq!(result.stats.directories_checked, 1); // root only
        assert_eq!(result.stats.files_checked, 0);
    }

    #[test]
    fn test_fsck_clean_after_create_file() {
        let img = make_editable_hfs_image();
        let cursor = Cursor::new(img);
        let mut fs = HfsFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        let test_data = b"Hello, HFS World!";
        let mut data_reader = Cursor::new(test_data.as_slice());
        let options = CreateFileOptions::default();
        fs.create_file(
            &root,
            "test.txt",
            &mut data_reader,
            test_data.len() as u64,
            &options,
        )
        .unwrap();

        assert_fsck_clean(&mut fs);
    }

    #[test]
    fn test_fsck_clean_after_create_directory() {
        let img = make_editable_hfs_image();
        let cursor = Cursor::new(img);
        let mut fs = HfsFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        let options = CreateDirectoryOptions::default();
        fs.create_directory(&root, "NewDir", &options).unwrap();

        assert_fsck_clean(&mut fs);
    }

    #[test]
    fn test_fsck_clean_after_delete_file() {
        let img = make_editable_hfs_image();
        let cursor = Cursor::new(img);
        let mut fs = HfsFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        let test_data = b"delete me";
        let mut data_reader = Cursor::new(test_data.as_slice());
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

        assert_fsck_clean(&mut fs);
        fs.delete_entry(&root, &fe).unwrap();
        assert_fsck_clean(&mut fs);
    }

    #[test]
    fn test_fsck_stress_many_files() {
        let img = make_editable_hfs_image();
        let cursor = Cursor::new(img);
        let mut fs = HfsFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        let options = CreateFileOptions::default();

        // Create 30+ files to trigger B-tree splits
        for i in 0..35 {
            let name = format!("file_{:03}.txt", i);
            let data = format!("content of file {}", i);
            let mut reader = Cursor::new(data.as_bytes().to_vec());
            fs.create_file(&root, &name, &mut reader, data.len() as u64, &options)
                .unwrap();

            // Verify fsck passes after each creation
            assert_fsck_clean(&mut fs);
        }

        let result = fs.fsck().unwrap();
        assert!(result.is_clean());
        assert_eq!(result.stats.files_checked, 35);
        assert_eq!(result.stats.directories_checked, 1); // just root
    }

    #[test]
    fn test_fsck_clean_after_blessed_folder() {
        let img = make_editable_hfs_image();
        let cursor = Cursor::new(img);
        let mut fs = HfsFilesystem::open(cursor, 0).unwrap();

        let root = fs.root().unwrap();
        let options = CreateDirectoryOptions::default();
        let sys_dir = fs.create_directory(&root, "System", &options).unwrap();
        fs.set_blessed_folder(&sys_dir).unwrap();

        assert_fsck_clean(&mut fs);
    }

    /// Verify that rebuilt index records have proper even-alignment padding
    /// between the key and the node pointer, as required by Mac OS's B-tree
    /// traversal code. Without padding, Mac OS reads misaligned node pointers
    /// and crashes during volume mount.
    #[test]
    fn test_repair_index_records_are_even_aligned() {
        use super::hfs_common::{btree_record_range, BTreeHeader};

        let img = make_editable_hfs_image();
        let cursor = Cursor::new(img);
        let mut fs = HfsFilesystem::open(cursor, 0).unwrap();
        let root = fs.root().unwrap();
        let options = CreateFileOptions::default();

        // Create files with varying name lengths (even and odd) to exercise
        // both padded and unpadded key paths in index records
        for i in 0..40 {
            let name = if i % 2 == 0 {
                format!("f{:03}.txt", i) // 8 chars → key_len=14 (even) → needs pad
            } else {
                format!("fi{:03}.txt", i) // 9 chars → key_len=15 (odd) → no pad
            };
            let data = format!("data{}", i);
            let mut reader = Cursor::new(data.as_bytes().to_vec());
            fs.create_file(&root, &name, &mut reader, data.len() as u64, &options)
                .unwrap();
        }

        // Run repair (which rebuilds index nodes)
        let report = fs.repair().unwrap();
        assert!(
            report.fixes_failed.is_empty(),
            "repair failures: {:?}",
            report.fixes_failed
        );

        // Verify every index record has correct alignment
        let cat = &fs.catalog_data;
        let header = BTreeHeader::read(cat);
        let node_size = header.node_size as usize;
        let max_nodes = cat.len() / node_size;

        let mut index_recs_checked = 0u32;
        for n in 1..max_nodes {
            let off = n * node_size;
            if off + node_size > cat.len() {
                break;
            }
            let kind = cat[off + 8] as i8;
            if kind != 0 {
                continue;
            }
            let num_recs = BigEndian::read_u16(&cat[off + 10..off + 12]) as usize;
            for r in 0..num_recs {
                let (rec_start, rec_end) =
                    btree_record_range(&cat[off..off + node_size], node_size, r);
                if rec_start >= rec_end || rec_end > node_size {
                    continue;
                }
                let key_len = cat[off + rec_start] as usize;
                let rec_len = rec_end - rec_start;

                // Mac OS forces all catalog index keys to length 0x25 (37),
                // making every record exactly 42 bytes.
                assert_eq!(
                    key_len, 0x25,
                    "Index node {} rec {}: key_len={} but expected 0x25",
                    n, r, key_len
                );
                assert_eq!(
                    rec_len, 42,
                    "Index node {} rec {}: rec_len={} but expected 42",
                    n, r, rec_len
                );
                index_recs_checked += 1;
            }
        }
        assert!(
            index_recs_checked > 0,
            "no index records found — test needs more files"
        );

        // Verify that a post-repair integrity check finds no issues
        let result = fs.fsck().unwrap();
        let error_msgs: Vec<&str> = result.errors.iter().map(|e| e.message.as_str()).collect();
        assert!(
            result.errors.is_empty(),
            "post-repair check found errors: {:?}",
            error_msgs
        );
    }
}
