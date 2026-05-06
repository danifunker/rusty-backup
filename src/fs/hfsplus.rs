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
    btree_grow_root, btree_insert_into_index, btree_insert_record, btree_remove_record,
    btree_split_leaf_with_insert, BTreeHeader,
};
use super::CompactResult;

const HFS_PLUS_SIGNATURE: u16 = 0x482B;
const HFSX_SIGNATURE: u16 = 0x4858;

/// HFS+ reserved CNIDs (Inside Macintosh: Files / TN1150).
#[allow(dead_code)]
const HFSPLUS_EXTENTS_FILE_ID: u32 = 3;
const HFSPLUS_CATALOG_FILE_ID: u32 = 4;
const HFSPLUS_ALLOCATION_FILE_ID: u32 = 6;

/// Fork-type byte used in extents-overflow keys.
const HFSPLUS_FORK_DATA: u8 = 0x00;
#[allow(dead_code)]
const HFSPLUS_FORK_RESOURCE: u8 = 0xFF;

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
    /// Cached extents-overflow B-tree file data (None until first fragmented
    /// fork forces it to load; stays None on volumes with no overflow file).
    extents_overflow_data: Option<Vec<u8>>,
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

        // Sanity bound: the catalog and extents-overflow forks must fit
        // within the volume. A corrupt logical_size that says "50 GiB" on a
        // 58 GiB volume would otherwise drive `Vec::with_capacity` into swap.
        let volume_bytes = vh.total_blocks as u64 * vh.block_size as u64;
        if vh.catalog_file.logical_size > volume_bytes {
            return Err(FilesystemError::Parse(format!(
                "catalog file logical_size {} exceeds volume size {}",
                vh.catalog_file.logical_size, volume_bytes
            )));
        }
        if vh.extents_file.logical_size > volume_bytes {
            return Err(FilesystemError::Parse(format!(
                "extents-overflow file logical_size {} exceeds volume size {}",
                vh.extents_file.logical_size, volume_bytes
            )));
        }

        log::debug!(
            "[HFS+ open @ {partition_offset}] vh ok: block_size={}, total_blocks={} ({} bytes), \
             catalog={} bytes, extents_file={} bytes, alloc_file={} bytes",
            vh.block_size,
            vh.total_blocks,
            volume_bytes,
            vh.catalog_file.logical_size,
            vh.extents_file.logical_size,
            vh.allocation_file.logical_size,
        );

        // Eagerly load the extents-overflow B-tree (its own 8 inline extents
        // are authoritative — the overflow file can't have overflow records),
        // then read the catalog with overflow support. Volumes with hundreds
        // of thousands of files can have catalog forks that exceed 8 inline
        // extents; reading inline-only would silently truncate and lead to
        // corrupt walks (and, with no cycle detection, hangs).
        log::debug!("[HFS+ open] reading extents-overflow file...");
        let extents_overflow_data = if vh.extents_file.logical_size > 0 {
            Some(read_fork(
                &mut reader,
                partition_offset,
                vh.block_size,
                &vh.extents_file,
            )?)
        } else {
            None
        };
        log::debug!(
            "[HFS+ open] extents-overflow loaded: {} bytes",
            extents_overflow_data.as_ref().map(|d| d.len()).unwrap_or(0)
        );

        log::debug!("[HFS+ open] reading catalog file...");
        let catalog_data = read_fork_with_overflow(
            &mut reader,
            partition_offset,
            vh.block_size,
            &vh.catalog_file,
            HFSPLUS_CATALOG_FILE_ID,
            HFSPLUS_FORK_DATA,
            extents_overflow_data.as_deref(),
        )?;
        log::debug!("[HFS+ open] catalog loaded: {} bytes", catalog_data.len());

        // Parse B-tree header from node 0
        if catalog_data.len() < 14 + 106 {
            return Err(FilesystemError::Parse(
                "catalog file too small for B-tree header".into(),
            ));
        }
        let catalog_header = BTreeHeaderRecord::parse(&catalog_data[14..14 + 106]);
        log::debug!(
            "[HFS+ open] catalog btree: depth={}, root_node={}, first_leaf={}, last_leaf={}, \
             node_size={}, total_nodes={}, free_nodes={}",
            catalog_header.depth,
            catalog_header.root_node,
            catalog_header.first_leaf_node,
            catalog_header.last_leaf_node,
            catalog_header.node_size,
            catalog_header.total_nodes,
            catalog_header.free_nodes,
        );

        // Try to find the volume label from the root folder thread.
        log::debug!("[HFS+ open] scanning catalog for volume label...");
        let label = find_volume_label(&catalog_data, &catalog_header);
        log::debug!("[HFS+ open] volume label: {:?}", label);

        Ok(HfsPlusFilesystem {
            reader,
            partition_offset,
            vh,
            catalog_data,
            catalog_header,
            label,
            bitmap: None,
            extents_overflow_data,
        })
    }

    /// List all children of a given parent CNID.
    ///
    /// HFS+ catalog records are ordered by (parent_id, name), so all children
    /// of a given parent occupy a contiguous run in the leaf chain. We stop
    /// the walk as soon as we encounter a record with parent_id > target —
    /// otherwise listing the root of a 500k-file volume would scan every
    /// leaf node in the catalog. The `visited` set guards against cycles
    /// from corrupt `next` pointers.
    fn list_children(&self, parent_cnid: u32) -> Result<Vec<CatalogEntry>, FilesystemError> {
        let node_size = self.catalog_header.node_size as usize;
        if node_size == 0 {
            return Ok(vec![]);
        }

        let mut results = Vec::new();
        // Descend the catalog B-tree to find the leaf that would contain the
        // smallest key for this parent_cnid (i.e. (parent_cnid, "")). Walking
        // forward from there skips the leaves that hold smaller parent_cnids
        // — for high-CNID parents on a 24k-node catalog, that's the difference
        // between O(node_count) per call and O(depth + matches).
        let header = hfs_common::BTreeHeader::read(&self.catalog_data);
        let search_key = Self::build_catalog_key(parent_cnid, "");
        let (start_leaf, _chain) = hfs_common::btree_find_insert_leaf(
            &self.catalog_data,
            &header,
            &search_key,
            &Self::catalog_compare,
        );
        let mut node_idx = if start_leaf != 0 {
            start_leaf
        } else {
            self.catalog_header.first_leaf_node
        };
        let mut visited: std::collections::HashSet<u32> = std::collections::HashSet::new();
        let mut seen_target = false;

        'outer: while node_idx != 0 {
            if !visited.insert(node_idx) {
                break;
            }
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
                if key_parent_id == parent_cnid {
                    seen_target = true;
                } else if key_parent_id > parent_cnid {
                    // Catalog is sorted by (parent_id, name). Once we pass
                    // the target, no further records will match. If we
                    // already collected matches we're done; if not, the
                    // target may still appear in a later leaf only if we
                    // somehow started past it (shouldn't happen). Either
                    // way, stop scanning.
                    break 'outer;
                } else {
                    // key_parent_id < parent_cnid: not yet at the target.
                    if seen_target {
                        // (defensive) records out of order — bail.
                        break 'outer;
                    }
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
                if !rec_data_start.is_multiple_of(2) {
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
        let first_leaf = self.catalog_header.first_leaf_node;

        hfs_common::walk_leaf_records(
            &self.catalog_data,
            first_leaf,
            node_size,
            |_node_idx, _rec_idx, _abs_off, rec| {
                if rec.len() < 8 {
                    return None;
                }
                let key_len = BigEndian::read_u16(&rec[0..2]) as usize;
                if key_len < 6 || 2 + key_len > rec.len() {
                    return None;
                }
                let mut data_rel = 2 + key_len;
                if !data_rel.is_multiple_of(2) {
                    data_rel += 1;
                }
                if data_rel + 2 > rec.len() {
                    return None;
                }
                let record_type = BigEndian::read_i16(&rec[data_rel..data_rel + 2]);
                if record_type != CATALOG_FILE {
                    return None;
                }
                let body = &rec[data_rel..];
                if body.len() < 248 {
                    return None;
                }
                let rec_file_id = BigEndian::read_u32(&body[8..12]);
                if rec_file_id != file_id {
                    return None;
                }
                Some((
                    ForkData::parse(&body[88..168]),
                    ForkData::parse(&body[168..248]),
                ))
            },
        )
    }

    /// Read the allocation bitmap and return it.
    fn read_allocation_bitmap(&mut self) -> Result<Vec<u8>, FilesystemError> {
        // Allocation files on large volumes can run beyond 8 inline extents;
        // route through the overflow-aware reader.
        read_fork_with_overflow(
            &mut self.reader,
            self.partition_offset,
            self.vh.block_size,
            &self.vh.allocation_file,
            HFSPLUS_ALLOCATION_FILE_ID,
            HFSPLUS_FORK_DATA,
            self.extents_overflow_data.as_deref(),
        )
    }

    /// Read a user fork (data or resource) belonging to `file_id`, consulting
    /// the extents-overflow B-tree when the inline 8 extents are insufficient.
    fn read_user_fork(
        &mut self,
        file_id: u32,
        fork_type: u8,
        fork: &ForkData,
    ) -> Result<Vec<u8>, FilesystemError> {
        read_fork_with_overflow(
            &mut self.reader,
            self.partition_offset,
            self.vh.block_size,
            fork,
            file_id,
            fork_type,
            self.extents_overflow_data.as_deref(),
        )
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
            .map(BigEndian::read_u16)
            .collect();
        let name_b: Vec<u16> = b[8..8 + name_len_b.min((b.len() - 8) / 2) * 2]
            .chunks_exact(2)
            .map(BigEndian::read_u16)
            .collect();
        hfs_common::compare_hfsplus_keys(parent_a, &name_a, parent_b, &name_b, false)
    }

    /// Find a catalog record by (parent_cnid, name).
    /// Returns Some((node_idx, rec_idx, absolute_offset_in_catalog_data)) if found.
    /// Scans leaf nodes linearly (correct for all catalog sizes we encounter).
    fn find_catalog_record(&self, parent_cnid: u32, name: &str) -> Option<(u32, usize, usize)> {
        let search_key = Self::build_catalog_key(parent_cnid, name);
        let node_size = self.catalog_header.node_size as usize;
        let first_leaf = self.catalog_header.first_leaf_node;

        hfs_common::walk_leaf_records(
            &self.catalog_data,
            first_leaf,
            node_size,
            |node_idx, rec_idx, abs_off, rec| {
                if rec.len() < 8 {
                    return None;
                }
                let key_len = BigEndian::read_u16(&rec[0..2]) as usize;
                let key_portion = &rec[..2 + key_len.min(rec.len() - 2)];
                if Self::catalog_compare(key_portion, &search_key) == Ordering::Equal {
                    Some((node_idx, rec_idx, abs_off))
                } else {
                    None
                }
            },
        )
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

        // Now scan for the actual folder record (not thread) with this CNID.
        let node_size = self.catalog_header.node_size as usize;
        let first_leaf = self.catalog_header.first_leaf_node;
        let val_offset = hfs_common::walk_leaf_records(
            &self.catalog_data,
            first_leaf,
            node_size,
            |_node_idx, _rec_idx, abs_off, rec| {
                if rec.len() < 8 {
                    return None;
                }
                let key_len = BigEndian::read_u16(&rec[0..2]) as usize;
                let mut data_off = abs_off + 2 + key_len;
                if !data_off.is_multiple_of(2) {
                    data_off += 1;
                }
                let abs_end = abs_off + rec.len();
                if data_off + 12 > abs_end {
                    return None;
                }
                let record_type = BigEndian::read_i16(&self.catalog_data[data_off..data_off + 2]);
                if record_type != CATALOG_FOLDER {
                    return None;
                }
                let folder_id =
                    BigEndian::read_u32(&self.catalog_data[data_off + 8..data_off + 12]);
                if folder_id == parent_cnid {
                    Some(data_off + 4)
                } else {
                    None
                }
            },
        );
        if let Some(val_offset) = val_offset {
            let old_val = BigEndian::read_u32(&self.catalog_data[val_offset..val_offset + 4]);
            let new_val = (old_val as i64 + delta as i64).max(0) as u32;
            BigEndian::write_u32(&mut self.catalog_data[val_offset..val_offset + 4], new_val);
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
        let blocks_needed = data_len.div_ceil(block_size) as u32;
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
                    fe.type_code = Some(type_code);
                    fe.creator_code = Some(creator_code);
                    if rsrc_size > 0 {
                        fe.resource_fork_size = Some(rsrc_size);
                    }
                    // NOTE: symlink/alias *target resolution* used to happen
                    // here (read the data fork for `slnk`/`rhap` files; read
                    // the resource fork for entries with the IS_ALIAS finder
                    // flag). On a slow or forward-only source — zstd
                    // streaming backups, NAS-backed images — those reads can
                    // each take minutes because the source has to decompress
                    // gigabytes to reach the fork's extent. With Mac OS X
                    // root directories typically holding a handful of
                    // symlinks (etc → private/etc, var → private/var, etc.),
                    // listing the root would stall for tens of minutes.
                    //
                    // The target isn't load-bearing for navigation: the GUI
                    // file preview already reads the data fork on demand
                    // when the user selects the entry, and an `slnk`/`rhap`
                    // data fork is just the UTF-8 target path — it renders
                    // fine as a text preview. The Finder-alias path
                    // (`mac_alias::resolve_alias_target`) was inline-used
                    // by the directory listing only; nothing else depends
                    // on the alias having a resolved target. So we drop the
                    // eager resolution and let the preview path do the read
                    // when (and only when) the user actually clicks.
                    let _ = (data_fork, rsrc_fork, finder_flags);
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

        let mut data = self.read_user_fork(file_id, HFSPLUS_FORK_DATA, &data_fork)?;
        data.truncate(max_bytes);
        Ok(data)
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn std::io::Write,
    ) -> Result<u64, FilesystemError> {
        if entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        let file_id = entry.location as u32;
        let (data_fork, _rsrc_fork) = self.find_file_by_id(file_id).ok_or_else(|| {
            FilesystemError::NotFound(format!("file id {file_id} not found in catalog"))
        })?;
        write_fork_to(
            &mut self.reader,
            self.partition_offset,
            self.vh.block_size,
            &data_fork,
            writer,
        )
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
            if !rec_data_start.is_multiple_of(2) {
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

/// Stream a fork's data through its extent descriptors to a writer.
fn write_fork_to<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    block_size: u32,
    fork: &ForkData,
    writer: &mut dyn std::io::Write,
) -> Result<u64, FilesystemError> {
    let size = fork.logical_size;
    let mut written: u64 = 0;
    let mut buf = vec![0u8; 64 * 1024];
    for ext in &fork.extents {
        if ext.is_empty() || written >= size {
            break;
        }
        let offset = partition_offset + ext.start_block as u64 * block_size as u64;
        let extent_len = ext.block_count as u64 * block_size as u64;
        let to_emit = extent_len.min(size - written);
        reader.seek(SeekFrom::Start(offset))?;
        let mut left = to_emit;
        while left > 0 {
            let n = (buf.len() as u64).min(left) as usize;
            reader.read_exact(&mut buf[..n])?;
            writer.write_all(&buf[..n])?;
            left -= n as u64;
        }
        written += to_emit;
    }
    Ok(written)
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

    let extent_count = fork.extents.iter().filter(|e| !e.is_empty()).count();
    let t_total = std::time::Instant::now();
    log::debug!(
        "[HFS+ read_fork] start: logical_size={} bytes, inline_extents={}",
        size,
        extent_count
    );

    for (idx, ext) in fork.extents.iter().enumerate() {
        if ext.is_empty() {
            break;
        }
        let offset = partition_offset + ext.start_block as u64 * block_size as u64;
        let len = ext.block_count as u64 * block_size as u64;
        let read_len = len.min((size - data.len()) as u64) as usize;
        let t_ext = std::time::Instant::now();
        reader.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; read_len];
        reader.read_exact(&mut buf)?;
        let elapsed_ms = t_ext.elapsed().as_secs_f64() * 1000.0;
        let mb = read_len as f64 / (1024.0 * 1024.0);
        let mbps = if elapsed_ms > 0.0 {
            mb / (elapsed_ms / 1000.0)
        } else {
            f64::INFINITY
        };
        log::debug!(
            "[HFS+ read_fork] ext[{}]: offset={} bytes={} time={:.1}ms throughput={:.2} MB/s",
            idx,
            offset,
            read_len,
            elapsed_ms,
            mbps
        );
        data.extend_from_slice(&buf);
        if data.len() >= size {
            break;
        }
    }

    data.truncate(size);
    let total_ms = t_total.elapsed().as_secs_f64() * 1000.0;
    let total_mb = size as f64 / (1024.0 * 1024.0);
    let total_mbps = if total_ms > 0.0 {
        total_mb / (total_ms / 1000.0)
    } else {
        f64::INFINITY
    };
    log::debug!(
        "[HFS+ read_fork] done: {} bytes in {:.1}ms ({:.2} MB/s)",
        size,
        total_ms,
        total_mbps
    );
    Ok(data)
}

/// Walk the extents-overflow B-tree leaf chain and collect every extent
/// belonging to (`file_id`, `fork_type`) whose `startBlock` key is at or
/// past `min_start_block`.
///
/// The HFS+ extents-overflow record is:
/// ```text
/// HFSPlusExtentKey { keyLength: u16 = 10, forkType: u8, pad: u8,
///                    fileID: u32, startBlock: u32 }
/// HFSPlusExtentRecord { extents: [HFSPlusExtentDescriptor; 8] }
/// ```
/// Each `HFSPlusExtentDescriptor` is `{ startBlock: u32, blockCount: u32 }`.
fn collect_hfsplus_overflow_extents(
    extents_data: &[u8],
    file_id: u32,
    fork_type: u8,
    min_start_block: u32,
) -> Vec<ExtentDescriptor> {
    use super::hfs_common::walk_leaf_records;

    if extents_data.len() < 14 + 30 {
        return Vec::new();
    }
    let header = BTreeHeaderRecord::parse(&extents_data[14..14 + 30.min(extents_data.len() - 14)]);
    let node_size = header.node_size as usize;
    if node_size == 0 {
        return Vec::new();
    }

    let mut out: Vec<(u32, ExtentDescriptor)> = Vec::new();
    walk_leaf_records::<(), _>(
        extents_data,
        header.first_leaf_node,
        node_size,
        |_node, _rec_idx, _abs_off, rec| {
            if rec.len() < 12 {
                return None;
            }
            let key_len = BigEndian::read_u16(&rec[0..2]) as usize;
            // HFS+ extent key: forkType(1) pad(1) fileID(4) startBlock(4) = 10 bytes.
            if key_len < 10 || 2 + key_len > rec.len() {
                return None;
            }
            let rec_fork = rec[2];
            let rec_file = BigEndian::read_u32(&rec[4..8]);
            let rec_start_block = BigEndian::read_u32(&rec[8..12]);
            if rec_fork != fork_type || rec_file != file_id {
                return None;
            }
            if rec_start_block < min_start_block {
                return None;
            }
            // Record body is 8 × 8-byte HFSPlusExtentDescriptor immediately
            // after the key; HFS+ keys are 2-byte aligned by construction.
            let data_off = 2 + key_len;
            if data_off + 64 > rec.len() {
                return None;
            }
            for j in 0..8 {
                let ext = ExtentDescriptor::parse(&rec[data_off + j * 8..data_off + j * 8 + 8]);
                if ext.block_count > 0 {
                    out.push((rec_start_block, ext));
                }
            }
            None
        },
    );

    out.sort_by_key(|(sb, _)| *sb);
    out.into_iter().map(|(_, e)| e).collect()
}

/// Like `read_fork`, but consults the extents-overflow B-tree when the inline
/// 8 extents don't cover `fork.logical_size`.
///
/// `extents_overflow_data` is the bytes of `vh.extents_file` (loaded once via
/// `read_fork`; the extents-overflow file itself can't have overflow records
/// — its 8 inline extents are authoritative). Pass `None` if the volume has
/// no extents-overflow file or you've already verified the fork fits inline.
fn read_fork_with_overflow<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    block_size: u32,
    fork: &ForkData,
    file_id: u32,
    fork_type: u8,
    extents_overflow_data: Option<&[u8]>,
) -> Result<Vec<u8>, FilesystemError> {
    let mut data = read_fork(reader, partition_offset, block_size, fork)?;
    let target = fork.logical_size as usize;
    if data.len() >= target {
        return Ok(data);
    }
    let Some(ext_data) = extents_overflow_data else {
        return Err(FilesystemError::InvalidData(format!(
            "file {file_id} fork {fork_type:#x}: {} bytes requested but only \
             {} bytes in inline extents and no extents-overflow B-tree available",
            fork.logical_size,
            data.len()
        )));
    };
    let inline_blocks: u32 = fork.extents.iter().map(|e| e.block_count).sum();
    let overflow = collect_hfsplus_overflow_extents(ext_data, file_id, fork_type, inline_blocks);
    for ext in overflow {
        if data.len() >= target {
            break;
        }
        if ext.block_count == 0 {
            continue;
        }
        let offset = partition_offset + ext.start_block as u64 * block_size as u64;
        let extent_len = ext.block_count as u64 * block_size as u64;
        let to_read = extent_len.min((target - data.len()) as u64) as usize;
        reader.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; to_read];
        reader.read_exact(&mut buf)?;
        data.extend_from_slice(&buf);
    }
    if data.len() < target {
        return Err(FilesystemError::InvalidData(format!(
            "file {file_id} fork {fork_type:#x}: extents (inline + overflow) \
             cover {} of {} bytes",
            data.len(),
            fork.logical_size
        )));
    }
    Ok(data)
}

/// Translate a byte offset within a fork (file space) to a byte offset on
/// the underlying device, using the fork's inline 8 extents. Returns
/// `Some((device_offset, contiguous_bytes_remaining))` for the extent that
/// contains `fork_offset`, or `None` if `fork_offset` falls past the inline
/// extents (caller would need to consult the extents-overflow B-tree).
fn translate_fork_offset(
    fork: &ForkData,
    block_size: u32,
    partition_offset: u64,
    fork_offset: u64,
) -> Option<(u64, u64)> {
    let bs = block_size as u64;
    let mut cursor: u64 = 0;
    for ext in &fork.extents {
        if ext.block_count == 0 {
            continue;
        }
        let len = ext.block_count as u64 * bs;
        if fork_offset < cursor + len {
            let local = fork_offset - cursor;
            let dev = partition_offset + ext.start_block as u64 * bs + local;
            return Some((dev, len - local));
        }
        cursor += len;
    }
    None
}

/// Read exactly one B-tree node from a fork, by node index.
fn read_node_from_fork<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    block_size: u32,
    fork: &ForkData,
    node_idx: u32,
    node_size: usize,
) -> Option<Vec<u8>> {
    let fork_offset = (node_idx as u64).checked_mul(node_size as u64)?;
    let (dev_off, contig) = translate_fork_offset(fork, block_size, partition_offset, fork_offset)?;
    if (contig as usize) < node_size {
        // Node straddles an extent boundary — should never happen for HFS+
        // since node_size divides block_size. Bail rather than do a multi-
        // extent read for the cheap probe path.
        return None;
    }
    reader.seek(SeekFrom::Start(dev_off)).ok()?;
    let mut buf = vec![0u8; node_size];
    reader.read_exact(&mut buf).ok()?;
    Some(buf)
}

/// Probe an HFS+ volume header at `partition_offset` and return the volume
/// label (root folder thread name) without loading the full catalog.
///
/// Reads node 0 (the B-tree header) and the first leaf node of the catalog
/// file directly from disk via the fork's inline extents — typically two
/// small reads. Returns None if the partition doesn't hold an HFS+/HFSX
/// volume, or the root thread record isn't where it should be (the empty
/// name sorts first, so it lives in record 0 of the first leaf node).
pub fn probe_hfsplus_volume_label<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
) -> Option<String> {
    reader.seek(SeekFrom::Start(partition_offset + 1024)).ok()?;
    let mut vh_buf = [0u8; 512];
    reader.read_exact(&mut vh_buf).ok()?;
    let vh = HfsPlusVolumeHeader::parse(&vh_buf).ok()?;
    if vh.block_size == 0 {
        return None;
    }

    // Read node 0 to learn the catalog's actual node_size and first_leaf_node.
    // We don't yet know node_size, so probe with the volume's allocation block
    // size (HFS+ catalog node sizes are typically 4 KiB or 8 KiB and never
    // exceed the allocation block size on commonly-encountered volumes).
    let probe_size = (vh.block_size as usize).clamp(512, 32 * 1024);
    let node0 = read_node_from_fork(
        reader,
        partition_offset,
        vh.block_size,
        &vh.catalog_file,
        0,
        probe_size,
    )?;
    if node0.len() < 14 + 30 {
        return None;
    }
    let header = BTreeHeaderRecord::parse(&node0[14..14 + 30]);
    let node_size = header.node_size as usize;
    if node_size == 0 || node_size > 32 * 1024 || header.first_leaf_node == 0 {
        return None;
    }

    // Read the first leaf node and look at its first record. The root thread
    // (parent=2, name="") sorts before all other records in the catalog.
    let leaf = read_node_from_fork(
        reader,
        partition_offset,
        vh.block_size,
        &vh.catalog_file,
        header.first_leaf_node,
        node_size,
    )?;
    if leaf.len() < node_size || leaf[8] as i8 != -1 {
        return None;
    }
    let num_records = BigEndian::read_u16(&leaf[10..12]) as usize;
    for i in 0..num_records {
        let (rec_start, rec_end) = super::hfs_common::btree_record_range(&leaf, node_size, i);
        if rec_start >= rec_end || rec_end > node_size {
            continue;
        }
        let rec = &leaf[rec_start..rec_end];
        if rec.len() < 8 {
            continue;
        }
        let key_len = BigEndian::read_u16(&rec[0..2]) as usize;
        if key_len < 6 || 2 + key_len > rec.len() {
            continue;
        }
        let parent_id = BigEndian::read_u32(&rec[2..6]);
        if parent_id != 2 {
            continue;
        }
        let name_length = BigEndian::read_u16(&rec[6..8]) as usize;
        if name_length != 0 {
            continue;
        }
        let mut data_start = 2 + key_len;
        if !data_start.is_multiple_of(2) {
            data_start += 1;
        }
        if data_start + 10 > rec.len() {
            continue;
        }
        let record_type = BigEndian::read_i16(&rec[data_start..data_start + 2]);
        if record_type != 3 {
            continue;
        }
        let thread_name_len = BigEndian::read_u16(&rec[data_start + 8..data_start + 10]) as usize;
        let body_start = data_start + 10;
        let body_end = body_start + thread_name_len * 2;
        if body_end > rec.len() {
            continue;
        }
        let label = decode_utf16be(&rec[body_start..body_end]);
        if !label.is_empty() {
            return Some(label);
        }
    }
    None
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
    let chars: Vec<u16> = data.chunks_exact(2).map(BigEndian::read_u16).collect();
    String::from_utf16_lossy(&chars)
}

/// Find the volume label from the catalog B-tree (root folder thread record).
fn find_volume_label(catalog_data: &[u8], header: &BTreeHeaderRecord) -> String {
    use super::hfs_common::walk_leaf_records;

    let node_size = header.node_size as usize;
    if node_size == 0 {
        return String::new();
    }

    walk_leaf_records::<String, _>(
        catalog_data,
        header.first_leaf_node,
        node_size,
        |_node_idx, _rec_idx, _abs_off, rec| {
            if rec.len() < 8 {
                return None;
            }
            let key_len = BigEndian::read_u16(&rec[0..2]) as usize;
            if key_len < 6 || 2 + key_len > rec.len() {
                return None;
            }
            // Catalog key: parent_id(4) + name_length(2) + name(UTF-16BE)
            let parent_id = BigEndian::read_u32(&rec[2..6]);
            if parent_id != 2 {
                return None;
            }
            let name_length = BigEndian::read_u16(&rec[6..8]) as usize;
            if name_length != 0 {
                return None;
            }

            // Record data follows the key, 2-byte aligned. HFS+ keys are
            // already 2-byte sized so no padding is needed in the standard
            // case, but tolerate odd offsets defensively.
            let mut data_start = 2 + key_len;
            if !data_start.is_multiple_of(2) {
                data_start += 1;
            }
            if data_start + 10 > rec.len() {
                return None;
            }
            let record_type = BigEndian::read_i16(&rec[data_start..data_start + 2]);
            // Folder thread = 3
            if record_type != 3 {
                return None;
            }
            let thread_name_len =
                BigEndian::read_u16(&rec[data_start + 8..data_start + 10]) as usize;
            let body_start = data_start + 10;
            let body_end = body_start + thread_name_len * 2;
            if body_end > rec.len() {
                return None;
            }
            Some(decode_utf16be(&rec[body_start..body_end]))
        },
    )
    .unwrap_or_default()
}

/// Find the index of the last set bit in a bitmap (MSB-first).
fn find_last_set_bit(bitmap: &[u8], max_bits: u32) -> Option<u32> {
    // Scan from the end for efficiency
    let last_byte = (max_bits as usize).min(bitmap.len() * 8).div_ceil(8);
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
        if !rec_data_start.is_multiple_of(2) {
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
        if !frec_start.is_multiple_of(2) {
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
        if !rec_data_start.is_multiple_of(2) {
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
        if !frec_start.is_multiple_of(2) {
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
        log::debug!(
            "[HFS+ compact] seeking to VH at offset {}",
            partition_offset + 1024
        );
        reader.seek(SeekFrom::Start(partition_offset + 1024))?;
        let mut vh_buf = [0u8; 512];
        reader.read_exact(&mut vh_buf)?;
        let vh = HfsPlusVolumeHeader::parse(&vh_buf)?;
        log::debug!(
            "[HFS+ compact] VH ok: block_size={}, total_blocks={}, alloc_file extents[0]=(start={}, count={})",
            vh.block_size,
            vh.total_blocks,
            vh.allocation_file.extents[0].start_block,
            vh.allocation_file.extents[0].block_count,
        );

        // Load extents-overflow first so the allocation file can use it if
        // it's fragmented past 8 inline extents.
        let extents_overflow_data = if vh.extents_file.logical_size > 0 {
            Some(read_fork(
                &mut reader,
                partition_offset,
                vh.block_size,
                &vh.extents_file,
            )?)
        } else {
            None
        };

        // Read allocation file
        let alloc_data = read_fork_with_overflow(
            &mut reader,
            partition_offset,
            vh.block_size,
            &vh.allocation_file,
            HFSPLUS_ALLOCATION_FILE_ID,
            HFSPLUS_FORK_DATA,
            extents_overflow_data.as_deref(),
        )
        .map_err(|e| {
            log::debug!("[HFS+ compact] read_fork(alloc_file) failed: {e}");
            e
        })?;
        log::debug!(
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
        log::debug!(
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
        log::debug!(
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
        let vh = HfsPlusVolumeHeader {
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
