use byteorder::{BigEndian, ByteOrder};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};
use super::hfs_common::{
    self, bitmap_clear_bit_be, bitmap_collect_clear_runs_be, bitmap_set_bit_be, bitmap_test_bit_be,
    btree_free_node, btree_remove_record, BTreeHeader, BTreeKeyFormat,
};
use super::CompactResult;

const HFS_PLUS_SIGNATURE: u16 = 0x482B;
const HFSX_SIGNATURE: u16 = 0x4858;

/// HFS+ reserved CNIDs (Inside Macintosh: Files / TN1150).
const HFSPLUS_EXTENTS_FILE_ID: u32 = 3;
const HFSPLUS_CATALOG_FILE_ID: u32 = 4;
const HFSPLUS_ALLOCATION_FILE_ID: u32 = 6;
const HFSPLUS_ATTRIBUTES_FILE_ID: u32 = 8;

/// Which special-file B-tree a grow-on-full operation targets. Each is
/// backed by a `ForkData` in the volume header and an in-memory tree buffer
/// (`catalog_data` / `extents_overflow_data` / `attributes_data`).
/// See `grow_btree_fork` and `docs/todo_hfsplus_fork_growth.md` §4b.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum BTreeFork {
    Catalog,
    Extents,
    Attributes,
}

impl BTreeFork {
    /// Reserved CNID of the special file backing this B-tree — the key under
    /// which its own overflow extents are recorded in the extents-overflow
    /// B-tree (Phase B).
    fn file_id(self) -> u32 {
        match self {
            BTreeFork::Catalog => HFSPLUS_CATALOG_FILE_ID,
            BTreeFork::Extents => HFSPLUS_EXTENTS_FILE_ID,
            BTreeFork::Attributes => HFSPLUS_ATTRIBUTES_FILE_ID,
        }
    }

    fn label(self) -> &'static str {
        match self {
            BTreeFork::Catalog => "catalog",
            BTreeFork::Extents => "extents-overflow",
            BTreeFork::Attributes => "attributes",
        }
    }
}

/// Volume attribute bit (`vh.attributes`) set when the volume carries a
/// journal. We refuse to enter edit mode on these volumes until journal
/// replay is implemented — see Step 4 of `docs/hfsplus_enhancements.md`.
pub(crate) const HFSPLUS_VOLUME_JOURNALED_BIT: u32 = 0x2000;

/// `kHFSVolumeUnmountedBit` — set in `vh.attributes` to mark a cleanly-
/// unmounted volume. Mac OS refuses to mount volumes without this bit
/// (it triggers a "needs scavenging" path).
pub(crate) const HFSPLUS_VOLUME_UNMOUNTED_BIT: u32 = 0x100;

/// `kHFSBootVolumeInconsistentBit` — set by Mac OS while a volume is
/// mounted; cleared on clean unmount. Must be cleared on sync so the
/// volume isn't flagged for repair on next mount.
pub(crate) const HFSPLUS_BOOT_VOLUME_INCONSISTENT_BIT: u32 = 0x800;

/// `kHFSVolumeInconsistentBit` — newer companion to the boot-inconsistent
/// bit; Mac OS sets it when the volume is in a transient inconsistent
/// state (mid-mount, mid-write). Must be cleared on sync so Disk Utility
/// doesn't refuse to mount the volume.
pub(crate) const HFSPLUS_VOLUME_INCONSISTENT_BIT: u32 = 0x4000;

/// HFS+ B-tree `keyCompareType` byte values (BTHeaderRec offset 37 in
/// node 0). Plain HFS+ uses case-folding NFD compare; HFSX with the
/// case-sensitive attribute uses binary compare.
const KEY_COMPARE_CASE_FOLDING: u8 = 0xCF;
const KEY_COMPARE_BINARY: u8 = 0xBC;

/// Fork-type byte used in extents-overflow keys.
const HFSPLUS_FORK_DATA: u8 = 0x00;
#[allow(dead_code)]
const HFSPLUS_FORK_RESOURCE: u8 = 0xFF;

/// Attributes B-tree record types (per Apple TN1150 / `hfs_format.h`).
pub(crate) const HFSPLUS_ATTR_INLINE_DATA: u32 = 0x10;
#[allow(dead_code)] // matched by Step 13 (xattr writes); kept for completeness
const HFSPLUS_ATTR_FORK_DATA: u32 = 0x20;
#[allow(dead_code)]
const HFSPLUS_ATTR_EXTENTS: u32 = 0x30;

/// HFS+ extent descriptor: start_block (u32) + block_count (u32).
#[derive(Debug, Clone, Copy)]
pub(crate) struct ExtentDescriptor {
    pub(crate) start_block: u32,
    pub(crate) block_count: u32,
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
pub(crate) struct ForkData {
    pub(crate) logical_size: u64,
    // Preserved for VH round-trip; HFS+ on-disk fork data has a clump_size
    // slot at bytes [8..12]. We never tune it, but parsing/emitting the
    // named field keeps the serializer symmetric with the parser.
    #[allow(dead_code)]
    pub(crate) clump_size: u32,
    pub(crate) total_blocks: u32,
    pub(crate) extents: [ExtentDescriptor; 8],
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

    pub(crate) fn empty() -> Self {
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

/// One extended-attribute record decoded from the attributes B-tree.
///
/// The HFS+ attributes file stores xattrs keyed by `(fileID, startBlock, name)`;
/// inline records carry their value byte-for-byte, fork records carry a fork
/// header pointing at a separate xattr fork (read on demand), and extents
/// records carry continuation extents for forks larger than 8 inline extents.
#[derive(Debug, Clone)]
#[allow(dead_code)] // accessor fields are read in upcoming Step 13/14 callers
pub(crate) struct XattrRecord {
    pub name: String,
    /// Starting block of the value within an attribute fork. `0` for inline
    /// records and the first segment of fork-style records; nonzero for
    /// `Extents` continuation records.
    pub start_block: u32,
    pub kind: XattrKind,
}

#[derive(Debug, Clone)]
#[allow(dead_code)] // Fork/Extents variants are surfaced once xattr writes ship
pub(crate) enum XattrKind {
    /// Value stored directly in the B-tree record (the common case for
    /// `com.apple.FinderInfo` and most `com.apple.*` keys).
    Inline(Vec<u8>),
    /// Value stored in its own fork; the inline 8 extents in `ForkData`
    /// cover the value (or anchor a chain of `Extents` records).
    Fork(ForkData),
    /// Continuation extents for a fork-style xattr that needed more than
    /// 8 extents.
    Extents(Vec<ExtentDescriptor>),
}

/// HFS+ Volume Header (512 bytes at partition_offset + 1024).
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
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
    /// `keyCompareType` byte (BTHeaderRec offset 37): `0xCF` =
    /// case-folding NFD, `0xBC` = binary (HFSX case-sensitive). Older /
    /// hand-built B-trees may leave this zero — callers should treat any
    /// value other than `0xBC` as case-folding to stay compatible with
    /// non-HFSX volumes.
    key_compare_type: u8,
}

impl BTreeHeaderRecord {
    fn parse(data: &[u8]) -> Self {
        // The keyCompareType byte sits 7 bytes past the freeNodes field,
        // i.e. at offset 37 of the BTHeaderRec. The 106-byte slice handed
        // to us is large enough to cover it; older callers that only had
        // 30 bytes get treated as case-folding.
        let key_compare_type = if data.len() >= 38 { data[37] } else { 0 };
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
            key_compare_type,
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
    let utf16_len = super::hfs_unicode::decompose_str(name).len();
    if utf16_len > 255 {
        return Err(FilesystemError::InvalidData(format!(
            "filename is too long ({utf16_len} UTF-16 units); HFS+ allows up to 255 — \
             shorten the name (note: some emoji and rare characters count as 2 units)"
        )));
    }
    Ok(())
}
#[allow(dead_code)]
const CATALOG_FOLDER_THREAD: i16 = 3;
#[allow(dead_code)]
const CATALOG_FILE_THREAD: i16 = 4;

/// A parsed HFS+ catalog entry.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)] // catalog entries are short-lived per-record buffers; boxing adds heap churn
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
        /// Raw 4-byte Mac OSType type/creator, straight off disk (no lossy
        /// display decode) so high-bit codes survive round-trips.
        type_code: [u8; 4],
        creator_code: [u8; 4],
        /// Finder flags (userInfo.fdFlags) — bit 0x8000 is `kIsAlias`.
        finder_flags: u16,
        /// HFS+ file hardlink target inode number (`bsdInfo.special` u32).
        /// `Some` only when the FInfo type/creator are `hlnk`/`hfs+`. The
        /// inode itself lives at `iNode<N>` inside the volume's
        /// `HFS+ Private Data` directory.
        link_inode_num: Option<u32>,
        /// HFS+ directory hardlink target inode number. `Some` only when
        /// the FInfo type/creator are `fdrp`/`MACS`. The directory inode
        /// lives at `dir_<N>` inside the volume's
        /// `.HFS+ Private Directory Data\r` directory and replaces the
        /// stub when surfaced through `list_directory`.
        dir_link_inode_num: Option<u32>,
    },
}

/// Magic 4-byte type code on file hardlink stubs (`fdType`).
const HFSPLUS_HARDLINK_FILE_TYPE: &[u8; 4] = b"hlnk";
/// Magic 4-byte creator code on file hardlink stubs (`fdCreator`).
const HFSPLUS_HARDLINK_FILE_CREATOR: &[u8; 4] = b"hfs+";
/// Magic 4-byte type code on directory hardlink stubs (10.5+).
const HFSPLUS_HARDLINK_DIR_TYPE: &[u8; 4] = b"fdrp";
/// Magic 4-byte creator code on directory hardlink stubs.
const HFSPLUS_HARDLINK_DIR_CREATOR: &[u8; 4] = b"MACS";

/// Free-function form of `HfsPlusFilesystem::catalog_compare`, callable
/// from sibling modules (e.g. `hfsplus_fsck`) that only have the raw
/// catalog bytes and no `<R>`-typed filesystem instance.
pub(crate) fn catalog_compare_keys(a: &[u8], b: &[u8], case_sensitive: bool) -> Ordering {
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
    hfs_common::compare_hfsplus_keys(parent_a, &name_a, parent_b, &name_b, case_sensitive)
}

/// Name of the hidden directory at the volume root that stores HFS+ file
/// hardlink inodes (TN1150). Four NUL bytes followed by ASCII text — the
/// nulls keep the directory invisible to clients that ignore zero-byte
/// filename units.
fn hfsplus_private_dir_name() -> String {
    let mut s = String::with_capacity(4 + 17);
    for _ in 0..4 {
        s.push('\u{0}');
    }
    s.push_str("HFS+ Private Data");
    s
}

/// Name of the hidden directory at the volume root that stores directory
/// hardlink inodes (10.5+). The trailing carriage return keeps the
/// directory hidden from clients that disallow `\r` in filenames.
fn hfsplus_dir_private_dir_name() -> String {
    let mut s = String::with_capacity(28 + 1);
    s.push_str(".HFS+ Private Directory Data");
    s.push('\u{D}');
    s
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
    /// Cached attributes B-tree file data. `None` until Phase 5 (xattr
    /// write path) populates it; included in the snapshot now so
    /// rollback semantics are forward-compatible.
    attributes_data: Option<Vec<u8>>,
    /// Set of CNIDs that own at least one record in the attributes B-tree
    /// (extended attributes). Populated only by `prepare_for_edit` — `None`
    /// for read-only opens. Until the xattr write path lands (Phase 5 of
    /// `docs/hfsplus_enhancements.md`), `delete_entry` refuses any CNID in
    /// this set so we don't silently leave dangling attribute records.
    xattr_cnids: Option<HashSet<u32>>,
    /// File hardlink resolution map (iNodeNum -> real inode CNID). Populated
    /// lazily the first time a `list_directory` / `read_file` call needs to
    /// follow a hardlink stub. `None` until consulted; `Some(map)` after the
    /// first lookup, with `map` empty on volumes with no hardlinks. Cleared
    /// by snapshot rollback so writes that move the private dir don't leave
    /// a stale map behind.
    hardlink_inode_map: Option<HashMap<u32, u32>>,
    /// Directory hardlink resolution map (dir-inode iNodeNum -> directory
    /// CNID). Same lazy/snapshot semantics as `hardlink_inode_map`.
    dir_hardlink_inode_map: Option<HashMap<u32, u32>>,
    /// Boot blocks staged via `set_boot_blocks` and flushed by
    /// `do_sync_metadata`. Used by the clone path (Step 21) to carry the
    /// source's first 1024 bytes onto the freshly-built target verbatim.
    /// Not part of the snapshot — clone stages once at the end and only
    /// the final `sync_metadata` writes to disk.
    pending_boot_blocks: Option<Box<[u8; 1024]>>,
}

/// Snapshot of every byte/value an HFS+ mutation can touch — taken at the
/// start of each `EditableFilesystem` method so a mid-flight failure can
/// rewind to byte-identical pre-call state instead of leaving the catalog
/// half-applied.
///
/// Forward-compatible with Phase 5 (attributes B-tree write side) — the
/// `attributes_data` slot is captured today even though it's always `None`
/// until xattr writes ship.
#[derive(Debug, Clone)]
pub struct HfsPlusSnapshot {
    vh: HfsPlusVolumeHeader,
    catalog_data: Vec<u8>,
    catalog_header: BTreeHeaderRecord,
    bitmap: Option<Vec<u8>>,
    extents_overflow_data: Option<Vec<u8>>,
    attributes_data: Option<Vec<u8>>,
    xattr_cnids: Option<HashSet<u32>>,
    hardlink_inode_map: Option<HashMap<u32, u32>>,
    dir_hardlink_inode_map: Option<HashMap<u32, u32>>,
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
            attributes_data: None,
            xattr_cnids: None,
            hardlink_inode_map: None,
            dir_hardlink_inode_map: None,
            pending_boot_blocks: None,
        })
    }

    // ---- Accessors for `hfsplus_clone` (Step 10 of
    //      docs/hfsplus_enhancements.md). The clone module captures source
    //      metadata without taking a reference to the private VH/ForkData
    //      types — these helpers expose primitives + raw fork bytes.

    #[allow(dead_code)] // used in later HFS+ clone steps
    pub(crate) fn partition_offset(&self) -> u64 {
        self.partition_offset
    }

    pub fn block_size(&self) -> u32 {
        self.vh.block_size
    }

    pub(crate) fn total_blocks(&self) -> u32 {
        self.vh.total_blocks
    }

    pub(crate) fn signature(&self) -> u16 {
        self.vh.signature
    }

    pub(crate) fn vh_attributes(&self) -> u32 {
        self.vh.attributes
    }

    /// True when the volume header's journaled bit is set.
    pub fn is_journaled(&self) -> bool {
        self.vh.attributes & HFSPLUS_VOLUME_JOURNALED_BIT != 0
    }

    /// Locate, parse, and summarize the journal without applying any state.
    /// Returns `Ok(None)` when the volume is not journaled. The summaries walk
    /// `start -> end`; the walk stops early (and `partial` is set) on the first
    /// corrupt or short transaction, mirroring how macOS replay behaves.
    ///
    /// See `docs/hfsplus_enhancements.md` Phase 9, Step 24.
    /// Locate and parse the JIB + journal header. `Ok(None)` when the volume
    /// is not journaled. Shared by `read_journal` and `journal_detail`.
    fn locate_journal(
        &mut self,
    ) -> Result<
        Option<(
            super::hfsplus_journal::JournalInfoBlock,
            super::hfsplus_journal::JournalHeader,
        )>,
        FilesystemError,
    > {
        use super::hfsplus_journal::{JournalHeader, JournalInfoBlock};

        if !self.is_journaled() {
            return Ok(None);
        }
        let to_fs = |e: super::hfsplus_journal::JournalError| {
            FilesystemError::InvalidData(format!("journal: {e}"))
        };

        // JIB lives at allocation block `journal_info_block`.
        let jib_byte =
            self.partition_offset + self.vh.journal_info_block as u64 * self.vh.block_size as u64;
        self.reader.seek(SeekFrom::Start(jib_byte))?;
        let mut jib_buf = vec![0u8; 512];
        self.reader.read_exact(&mut jib_buf)?;
        let jib = JournalInfoBlock::parse(&jib_buf).map_err(to_fs)?;

        // The journal header's fixed fields live in the first 512 bytes.
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + jib.offset))?;
        let mut jh_buf = vec![0u8; 512];
        self.reader.read_exact(&mut jh_buf)?;
        let header = JournalHeader::parse(&jh_buf).map_err(to_fs)?;
        Ok(Some((jib, header)))
    }

    /// Byte ranges (relative to the partition) of the volume's metadata
    /// structures, for classifying which subsystem a journaled block targets.
    fn metadata_regions(&self) -> Vec<super::hfsplus_journal::MetadataRegion> {
        use super::hfsplus_journal::MetadataRegion;
        let bs = self.vh.block_size as u64;
        let mut regions = vec![MetadataRegion {
            name: "volume header",
            start: 1024,
            end: 1536,
        }];
        let fork = |name: &'static str, fork: &ForkData| -> Option<MetadataRegion> {
            let first = fork.extents.iter().find(|e| !e.is_empty())?;
            let start = first.start_block as u64 * bs;
            let bytes = fork.total_blocks as u64 * bs;
            Some(MetadataRegion {
                name,
                start,
                end: start + bytes,
            })
        };
        for (name, f) in [
            ("allocation bitmap", &self.vh.allocation_file),
            ("extents btree", &self.vh.extents_file),
            ("catalog btree", &self.vh.catalog_file),
            ("attributes btree", &self.vh.attributes_file),
            ("startup file", &self.vh.startup_file),
        ] {
            if let Some(r) = fork(name, f) {
                regions.push(r);
            }
        }
        regions
    }

    /// Decode the journal into per-transaction, per-block detail for the GUI
    /// history viewer (Step 29). Block previews are capped; classification is
    /// against [`metadata_regions`]. `Ok(None)` when not journaled.
    pub fn journal_detail(
        &mut self,
    ) -> Result<Option<super::hfsplus_journal::JournalDetail>, FilesystemError> {
        let Some((jib, header)) = self.locate_journal()? else {
            return Ok(None);
        };
        let regions = self.metadata_regions();
        let detail = super::hfsplus_journal::read_journal_detail(
            &mut self.reader,
            self.partition_offset,
            jib,
            header,
            regions,
        );
        Ok(Some(detail))
    }

    pub fn read_journal(
        &mut self,
    ) -> Result<Option<super::hfsplus_journal::JournalState>, FilesystemError> {
        use super::hfsplus_journal::JournalWalker;

        let Some((jib, header)) = self.locate_journal()? else {
            return Ok(None);
        };

        let mut transactions = Vec::new();
        let mut partial = false;
        let mut checksum_mismatch = None;
        let mut sequence_jumps = Vec::new();
        {
            let mut walker =
                JournalWalker::new(&mut self.reader, self.partition_offset, &jib, &header);
            let mut prev_seq: Option<u32> = None;
            while let Some(item) = walker.next() {
                match item {
                    Ok(view) => {
                        let seq = view.sequence_num;
                        if let Some(prev) = prev_seq {
                            // macOS expects each transaction's sequence to be
                            // prev or prev+1; anything else is a jump.
                            if seq != prev && seq != prev.wrapping_add(1) {
                                sequence_jumps.push((prev, seq));
                            }
                        }
                        prev_seq = Some(seq);
                        transactions.push(view.summary());
                    }
                    Err(super::hfsplus_journal::JournalError::CorruptTransaction {
                        sequence_num,
                    }) => {
                        checksum_mismatch = Some(sequence_num);
                        partial = true;
                        break;
                    }
                    Err(_) => {
                        partial = true;
                        break;
                    }
                }
            }
        }

        Ok(Some(super::hfsplus_journal::JournalState {
            info: jib,
            header,
            transactions,
            partial,
            checksum_mismatch,
            sequence_jumps,
        }))
    }

    pub(crate) fn vh_create_date(&self) -> u32 {
        self.vh.create_date
    }

    pub(crate) fn vh_modify_date(&self) -> u32 {
        self.vh.modify_date
    }

    pub(crate) fn vh_backup_date(&self) -> u32 {
        self.vh.backup_date
    }

    pub(crate) fn vh_checked_date(&self) -> u32 {
        self.vh.checked_date
    }

    pub(crate) fn vh_file_count(&self) -> u32 {
        self.vh.file_count
    }

    pub(crate) fn vh_folder_count(&self) -> u32 {
        self.vh.folder_count
    }

    pub(crate) fn vh_finder_info(&self) -> [u32; 8] {
        self.vh.finder_info
    }

    pub(crate) fn vh_free_blocks(&self) -> u32 {
        self.vh.free_blocks
    }

    #[allow(dead_code)] // used in later HFS+ clone steps
    pub(crate) fn vh_next_catalog_id(&self) -> u32 {
        self.vh.next_catalog_id
    }

    /// Read and return the allocation-file bitmap. Routes through the
    /// extents-overflow-aware fork reader, so volumes whose allocation file
    /// spills past 8 inline extents work correctly.
    pub(crate) fn read_allocation_bitmap_for_fsck(&mut self) -> Result<Vec<u8>, FilesystemError> {
        self.read_allocation_bitmap()
    }

    /// `total_blocks` field of the source's catalog file fork. Used by the
    /// streamed defrag planner to size the target's catalog B-tree (with
    /// a small safety margin) so it has enough free nodes to host every
    /// inserted record without running out of B-tree node slots.
    pub(crate) fn vh_catalog_total_blocks(&self) -> u32 {
        self.vh.catalog_file.total_blocks
    }

    /// `total_blocks` field of the source's extents-overflow file fork.
    /// Most defrag targets pack files contiguously and need little or no
    /// overflow capacity, but matching source size is a safe ceiling.
    pub(crate) fn vh_extents_total_blocks(&self) -> u32 {
        self.vh.extents_file.total_blocks
    }

    /// `total_blocks` field of the source's attributes file fork. Zero on
    /// volumes without xattrs.
    pub(crate) fn vh_attributes_total_blocks(&self) -> u32 {
        self.vh.attributes_file.total_blocks
    }

    pub(crate) fn label(&self) -> &str {
        &self.label
    }

    pub(crate) fn catalog_data(&self) -> &[u8] {
        &self.catalog_data
    }

    pub(crate) fn catalog_node_size(&self) -> usize {
        self.catalog_header.node_size as usize
    }

    pub(crate) fn catalog_first_leaf(&self) -> u32 {
        self.catalog_header.first_leaf_node
    }

    pub(crate) fn case_sensitive_catalog(&self) -> bool {
        self.case_sensitive()
    }

    /// Read the first 1024 bytes of the partition (HFS+ "boot blocks" —
    /// always zero on macOS-formatted volumes but copied verbatim by clone).
    pub(crate) fn read_boot_blocks(&mut self) -> Result<[u8; 1024], FilesystemError> {
        self.reader.seek(SeekFrom::Start(self.partition_offset))?;
        let mut buf = [0u8; 1024];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Open a streaming `Read` over a user fork without buffering its bytes.
    /// Used by the clone pipeline (Step 21+) so a 38 GiB volume's data
    /// forks don't have to materialise in RAM. The returned reader borrows
    /// `self`'s underlying I/O for its lifetime — drop it before issuing
    /// any other read against this filesystem.
    pub(crate) fn fork_stream_reader(
        &mut self,
        file_id: u32,
        fork_type: u8,
        fork_record: &[u8; 80],
    ) -> Result<ForkStreamReader<'_, R>, FilesystemError> {
        let fork = ForkData::parse(fork_record);
        let logical_size = fork.logical_size;

        // Resolve every extent (inline + overflow) into device-offset / length
        // pairs once at construction. The extents-overflow B-tree was already
        // loaded eagerly during `open`; consulting it again here is O(records).
        let block_size = self.vh.block_size as u64;
        let mut extents: Vec<(u64, u64)> = Vec::new();
        let mut covered: u64 = 0;
        for ext in fork.extents.iter() {
            if ext.block_count == 0 {
                continue;
            }
            let off = self.partition_offset + ext.start_block as u64 * block_size;
            let len = ext.block_count as u64 * block_size;
            extents.push((off, len));
            covered += len;
        }
        if covered < logical_size {
            let inline_blocks: u32 = fork.extents.iter().map(|e| e.block_count).sum();
            let overflow = match self.extents_overflow_data.as_deref() {
                Some(data) => {
                    collect_hfsplus_overflow_extents(data, file_id, fork_type, inline_blocks)
                }
                None => {
                    return Err(FilesystemError::InvalidData(format!(
                        "file {file_id} fork {fork_type:#x}: {} bytes requested but only \
                         {} bytes in inline extents and no extents-overflow B-tree available",
                        logical_size, covered
                    )));
                }
            };
            for ext in overflow {
                if ext.block_count == 0 {
                    continue;
                }
                let off = self.partition_offset + ext.start_block as u64 * block_size;
                let len = ext.block_count as u64 * block_size;
                extents.push((off, len));
                covered += len;
                if covered >= logical_size {
                    break;
                }
            }
            if covered < logical_size {
                return Err(FilesystemError::InvalidData(format!(
                    "file {file_id} fork {fork_type:#x}: extents (inline + overflow) \
                     cover {covered} of {logical_size} bytes"
                )));
            }
        }

        Ok(ForkStreamReader {
            reader: &mut self.reader,
            extents,
            logical_size,
            pos: 0,
            cur_extent: 0,
            cur_extent_consumed: 0,
        })
    }

    /// Capture the full mutable state for snapshot/rollback. Cheap on the
    /// scale of a single mutation: catalog + bitmap clone is O(volume
    /// metadata size), not O(volume size).
    pub fn snapshot(&self) -> HfsPlusSnapshot {
        HfsPlusSnapshot {
            vh: self.vh.clone(),
            catalog_data: self.catalog_data.clone(),
            catalog_header: self.catalog_header.clone(),
            bitmap: self.bitmap.clone(),
            extents_overflow_data: self.extents_overflow_data.clone(),
            attributes_data: self.attributes_data.clone(),
            xattr_cnids: self.xattr_cnids.clone(),
            hardlink_inode_map: self.hardlink_inode_map.clone(),
            dir_hardlink_inode_map: self.dir_hardlink_inode_map.clone(),
        }
    }

    /// Restore previously captured state. Used by the snapshot guard
    /// around every `EditableFilesystem` mutation when the body returns
    /// `Err`.
    pub fn restore_snapshot(&mut self, snap: HfsPlusSnapshot) {
        self.vh = snap.vh;
        self.catalog_data = snap.catalog_data;
        self.catalog_header = snap.catalog_header;
        self.bitmap = snap.bitmap;
        self.extents_overflow_data = snap.extents_overflow_data;
        self.attributes_data = snap.attributes_data;
        self.xattr_cnids = snap.xattr_cnids;
        self.hardlink_inode_map = snap.hardlink_inode_map;
        self.dir_hardlink_inode_map = snap.dir_hardlink_inode_map;
    }

    /// Sanity-check the volume for edit-mode compatibility and pre-load the
    /// attributes-B-tree CNID set. Called by `open_editable_filesystem` —
    /// not by read-only opens, so the cost is paid only when we're about
    /// to mutate.
    ///
    /// Journaled volumes are accepted only when the journal is *clean* (empty,
    /// `start == end`) — the cleanly-unmounted case. A clean journal has no
    /// pending state, so editing the catalog in place and leaving the journal
    /// empty keeps the volume valid for macOS (it replays nothing on mount).
    /// A *dirty* journal carries metadata the catalog hasn't absorbed yet;
    /// editing on top of that stale catalog would lose the journaled changes,
    /// so we still refuse it here. In-place transactional writes that journal
    /// our own mutations and recover dirty volumes are the remaining half of
    /// Step 27 (`docs/hfsplus_enhancements.md`) and are not yet wired in.
    ///
    /// xattr-bearing volumes succeed but later `delete_entry` calls against any
    /// of the cached CNIDs are refused at the source until Phase 5 lands.
    pub fn prepare_for_edit(&mut self) -> Result<(), FilesystemError> {
        if self.vh.attributes & HFSPLUS_VOLUME_JOURNALED_BIT != 0 {
            match self.locate_journal() {
                Ok(Some((_jib, header))) if !header.is_empty() => {
                    return Err(FilesystemError::Unsupported(
                        "dirty journaled HFS+ volume — mount and cleanly unmount it in macOS \
                         to flush the journal, then retry (or open read-only)"
                            .into(),
                    ));
                }
                Ok(_) => {
                    // Clean journal (or, defensively, none) — safe to edit in
                    // place; the journal stays empty and valid.
                }
                Err(e) => {
                    return Err(FilesystemError::Unsupported(format!(
                        "journaled HFS+ volume with an unreadable journal ({e}) — \
                         open read-only"
                    )));
                }
            }
        }

        // No attributes file → nothing to scan, leave the set as `None`
        // (delete_entry treats it as "no xattr-bearing CNIDs known").
        if self.vh.attributes_file.logical_size == 0 {
            self.xattr_cnids = Some(HashSet::new());
            return Ok(());
        }

        // Load the attributes B-tree fork (with overflow extents if needed)
        // and walk its leaves to harvest fileIDs from each record key.
        let (attr_data, attr_header) = self
            .ensure_attributes_loaded()?
            .expect("attributes_file.logical_size > 0 must produce a header (checked just above)");
        let node_size = attr_header.node_size as usize;
        let mut cnids: HashSet<u32> = HashSet::new();
        hfs_common::walk_leaf_records::<(), _>(
            attr_data,
            attr_header.first_leaf_node,
            node_size,
            |_node_idx, _rec_idx, _abs_off, rec_bytes| {
                if rec_bytes.len() < 2 {
                    return None;
                }
                let key_len = BigEndian::read_u16(&rec_bytes[0..2]) as usize;
                // key body must hold at least pad(2) + fileID(4)
                if key_len < 6 || 2 + 6 > rec_bytes.len() {
                    return None;
                }
                let file_id = BigEndian::read_u32(&rec_bytes[4..8]);
                cnids.insert(file_id);
                None
            },
        );
        log::debug!(
            "[HFS+ open_editable] attributes B-tree carries records for {} CNIDs",
            cnids.len()
        );
        self.xattr_cnids = Some(cnids);
        Ok(())
    }

    /// Lazy-load the attributes B-tree file into `self.attributes_data`. Returns
    /// `Ok(None)` if the volume has no attributes file (`logical_size == 0`),
    /// otherwise `Ok(Some((&[u8], header)))`.
    ///
    /// Subsequent calls reuse the cached buffer, so xattr lookups across many
    /// CNIDs cost one fork read total.
    fn ensure_attributes_loaded(
        &mut self,
    ) -> Result<Option<(&[u8], BTreeHeaderRecord)>, FilesystemError> {
        if self.vh.attributes_file.logical_size == 0 {
            return Ok(None);
        }
        if self.attributes_data.is_none() {
            let data = read_fork_with_overflow(
                &mut self.reader,
                self.partition_offset,
                self.vh.block_size,
                &self.vh.attributes_file,
                HFSPLUS_ATTRIBUTES_FILE_ID,
                HFSPLUS_FORK_DATA,
                self.extents_overflow_data.as_deref(),
            )?;
            if data.len() < 14 + 106 {
                return Err(FilesystemError::Parse(
                    "attributes file too small for B-tree header".into(),
                ));
            }
            self.attributes_data = Some(data);
        }
        let data = self.attributes_data.as_deref().expect("just populated");
        let header = BTreeHeaderRecord::parse(&data[14..14 + 106]);
        Ok(Some((data, header)))
    }

    /// Return every extended-attribute record attached to `cnid`, decoded into
    /// [`XattrRecord`]. Returns an empty vec if the volume has no attributes
    /// file or `cnid` owns no xattrs.
    ///
    /// Inline payloads are decoded eagerly (the common case for
    /// `com.apple.FinderInfo` and the rest of the `com.apple.*` namespace).
    /// Fork/extents records are returned with their on-disk metadata; the
    /// actual fork bytes are read on demand by Step 13's xattr write/read
    /// path, not here.
    ///
    /// HFS+ attribute key layout (after the 2-byte keyLength prefix):
    ///   pad (2) | fileID (4) | startBlock (4) | nameLen (2) | name (UTF-16BE)
    /// Records are 2-byte aligned; data follows the key at the next even
    /// offset and starts with a u32 record type (`HFSPLUS_ATTR_*`).
    #[allow(dead_code)] // first user lands in Step 13/14 (xattr writes + clone replay)
    pub(crate) fn list_xattrs(&mut self, cnid: u32) -> Result<Vec<XattrRecord>, FilesystemError> {
        let Some((data, header)) = self.ensure_attributes_loaded()? else {
            return Ok(Vec::new());
        };
        let node_size = header.node_size as usize;
        let first_leaf = header.first_leaf_node;
        let mut out: Vec<XattrRecord> = Vec::new();
        hfs_common::walk_leaf_records::<(), _>(
            data,
            first_leaf,
            node_size,
            |_node_idx, _rec_idx, _abs_off, rec_bytes| {
                if rec_bytes.len() < 2 {
                    return None;
                }
                let key_len = BigEndian::read_u16(&rec_bytes[0..2]) as usize;
                // key body must hold pad(2) + fileID(4) + startBlock(4) + nameLen(2)
                if key_len < 12 || 2 + key_len > rec_bytes.len() {
                    return None;
                }
                let file_id = BigEndian::read_u32(&rec_bytes[4..8]);
                if file_id != cnid {
                    return None;
                }
                let start_block = BigEndian::read_u32(&rec_bytes[8..12]);
                let name_len = BigEndian::read_u16(&rec_bytes[12..14]) as usize;
                let name_bytes_end = 14 + name_len * 2;
                if name_bytes_end > 2 + key_len {
                    return None;
                }
                let name = decode_utf16be(&rec_bytes[14..name_bytes_end]);

                // Record data starts at next even offset after the key (the
                // key body is always an even number of bytes — pad+id+sb+nl
                // is 12 plus 2*name_len — so data immediately follows).
                let data_off = 2 + key_len;
                if data_off + 4 > rec_bytes.len() {
                    return None;
                }
                let record_type = BigEndian::read_u32(&rec_bytes[data_off..data_off + 4]);
                let kind = match record_type {
                    HFSPLUS_ATTR_INLINE_DATA => {
                        // recordType(4) + reserved[2](8) + attrSize(4) + attrData[attrSize]
                        let header_end = data_off + 4 + 8 + 4;
                        if header_end > rec_bytes.len() {
                            return None;
                        }
                        let attr_size =
                            BigEndian::read_u32(&rec_bytes[header_end - 4..header_end]) as usize;
                        if header_end + attr_size > rec_bytes.len() {
                            return None;
                        }
                        XattrKind::Inline(rec_bytes[header_end..header_end + attr_size].to_vec())
                    }
                    HFSPLUS_ATTR_FORK_DATA => {
                        // recordType(4) + reserved(4) + HFSPlusForkData(80)
                        let fork_off = data_off + 4 + 4;
                        if fork_off + 80 > rec_bytes.len() {
                            return None;
                        }
                        XattrKind::Fork(ForkData::parse(&rec_bytes[fork_off..fork_off + 80]))
                    }
                    HFSPLUS_ATTR_EXTENTS => {
                        // recordType(4) + reserved(4) + 8 × ExtentDescriptor(8) = 64
                        let ext_off = data_off + 4 + 4;
                        if ext_off + 64 > rec_bytes.len() {
                            return None;
                        }
                        let mut extents = Vec::with_capacity(8);
                        for i in 0..8 {
                            let e = ExtentDescriptor::parse(
                                &rec_bytes[ext_off + i * 8..ext_off + (i + 1) * 8],
                            );
                            extents.push(e);
                        }
                        XattrKind::Extents(extents)
                    }
                    _ => {
                        log::warn!(
                            "[HFS+ list_xattrs] unknown attribute record type 0x{:x} \
                             on cnid={} name={:?}; skipping",
                            record_type,
                            cnid,
                            name
                        );
                        return None;
                    }
                };
                out.push(XattrRecord {
                    name,
                    start_block,
                    kind,
                });
                None
            },
        );
        Ok(out)
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
        let cs = self.case_sensitive();
        let cmp = |a: &[u8], b: &[u8]| Self::catalog_compare(a, b, cs);
        let (start_leaf, _chain) =
            hfs_common::btree_find_insert_leaf(&self.catalog_data, &header, &search_key, &cmp);
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
                        let type_code: [u8; 4] = rec[48..52].try_into().unwrap();
                        let creator_code: [u8; 4] = rec[52..56].try_into().unwrap();
                        let finder_flags = BigEndian::read_u16(&rec[56..58]);
                        // BSD info at offset 32 (16 bytes); the `special` u32
                        // at byte 12 of that block (offset 44 of the record)
                        // is `iNodeNum` on hardlink stubs and the link
                        // count on inodes — we only consume the former.
                        let link_inode_num = if &rec[48..52] == HFSPLUS_HARDLINK_FILE_TYPE
                            && &rec[52..56] == HFSPLUS_HARDLINK_FILE_CREATOR
                        {
                            Some(BigEndian::read_u32(&rec[44..48]))
                        } else {
                            None
                        };
                        let dir_link_inode_num = if &rec[48..52] == HFSPLUS_HARDLINK_DIR_TYPE
                            && &rec[52..56] == HFSPLUS_HARDLINK_DIR_CREATOR
                        {
                            Some(BigEndian::read_u32(&rec[44..48]))
                        } else {
                            None
                        };
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
                            link_inode_num,
                            dir_link_inode_num,
                        });
                    }
                    _ => {}
                }
            }

            node_idx = desc.next;
        }

        Ok(results)
    }

    /// Lazily build (and cache) the iNodeNum -> inode-CNID map by scanning
    /// the children of the volume's `HFS+ Private Data` directory. Returns
    /// `Ok(())` whether or not the directory exists; volumes without
    /// hardlinks end up with an empty map and the lookup short-circuits.
    fn ensure_hardlink_inode_map(&mut self) -> Result<(), FilesystemError> {
        if self.hardlink_inode_map.is_some() {
            return Ok(());
        }
        let mut map: HashMap<u32, u32> = HashMap::new();
        let private_name = hfsplus_private_dir_name();
        let private_cnid = self.list_children(2)?.into_iter().find_map(|c| match c {
            CatalogEntry::Folder { folder_id, name } if name == private_name => Some(folder_id),
            _ => None,
        });
        if let Some(cnid) = private_cnid {
            for child in self.list_children(cnid)? {
                if let CatalogEntry::File { file_id, name, .. } = child {
                    if let Some(rest) = name.strip_prefix("iNode") {
                        if let Ok(inode_num) = rest.parse::<u32>() {
                            map.insert(inode_num, file_id);
                        }
                    }
                }
            }
        }
        self.hardlink_inode_map = Some(map);
        Ok(())
    }

    /// Resolve a file hardlink stub's iNodeNum to its inode CNID. Returns
    /// `None` when the volume has no `HFS+ Private Data` directory or the
    /// inode is missing — callers fall back to treating the entry as an
    /// ordinary file.
    fn resolve_hardlink_inode(&mut self, inode_num: u32) -> Result<Option<u32>, FilesystemError> {
        self.ensure_hardlink_inode_map()?;
        Ok(self
            .hardlink_inode_map
            .as_ref()
            .and_then(|m| m.get(&inode_num).copied()))
    }

    /// CNID of the volume's `HFS+ Private Data` directory, if present.
    /// Lookup is O(root.children); cheap enough that we don't cache.
    fn find_private_dir_cnid(&self) -> Result<Option<u32>, FilesystemError> {
        let private_name = hfsplus_private_dir_name();
        Ok(self.list_children(2)?.into_iter().find_map(|c| match c {
            CatalogEntry::Folder { folder_id, name } if name == private_name => Some(folder_id),
            _ => None,
        }))
    }

    /// CNID of the volume's `.HFS+ Private Directory Data\r` directory, if
    /// present. Same cost profile as `find_private_dir_cnid`.
    fn find_dir_private_dir_cnid(&self) -> Result<Option<u32>, FilesystemError> {
        let dir_private_name = hfsplus_dir_private_dir_name();
        Ok(self.list_children(2)?.into_iter().find_map(|c| match c {
            CatalogEntry::Folder { folder_id, name } if name == dir_private_name => Some(folder_id),
            _ => None,
        }))
    }

    /// Lazily build (and cache) the dir-hardlink iNodeNum -> directory-inode
    /// CNID map. Mirrors `ensure_hardlink_inode_map` but scans children of
    /// the directory-hardlink private dir for `dir_<N>` folders.
    fn ensure_dir_hardlink_inode_map(&mut self) -> Result<(), FilesystemError> {
        if self.dir_hardlink_inode_map.is_some() {
            return Ok(());
        }
        let mut map: HashMap<u32, u32> = HashMap::new();
        if let Some(cnid) = self.find_dir_private_dir_cnid()? {
            for child in self.list_children(cnid)? {
                if let CatalogEntry::Folder { folder_id, name } = child {
                    if let Some(rest) = name.strip_prefix("dir_") {
                        if let Ok(inode_num) = rest.parse::<u32>() {
                            map.insert(inode_num, folder_id);
                        }
                    }
                }
            }
        }
        self.dir_hardlink_inode_map = Some(map);
        Ok(())
    }

    /// Resolve a dir-hardlink stub's iNodeNum to the dir-inode CNID.
    fn resolve_dir_hardlink_inode(
        &mut self,
        inode_num: u32,
    ) -> Result<Option<u32>, FilesystemError> {
        self.ensure_dir_hardlink_inode_map()?;
        Ok(self
            .dir_hardlink_inode_map
            .as_ref()
            .and_then(|m| m.get(&inode_num).copied()))
    }

    /// If the catalog record at `(parent_cnid, name)` is a file hardlink stub
    /// (`fdType='hlnk' fdCreator='hfs+'`), return its target iNodeNum from
    /// `bsdInfo.special`. `Ok(None)` for ordinary files / folders / missing
    /// records — callers treat that as "not a hardlink".
    fn read_file_record_link_inode_num(
        &self,
        parent_cnid: u32,
        name: &str,
    ) -> Result<Option<u32>, FilesystemError> {
        let Some((_, _, abs_off)) = self.find_catalog_record(parent_cnid, name) else {
            return Ok(None);
        };
        if abs_off + 2 > self.catalog_data.len() {
            return Ok(None);
        }
        let key_len = BigEndian::read_u16(&self.catalog_data[abs_off..abs_off + 2]) as usize;
        let mut body_start = abs_off + 2 + key_len;
        if !body_start.is_multiple_of(2) {
            body_start += 1;
        }
        if body_start + 56 > self.catalog_data.len() {
            return Ok(None);
        }
        let record_type = BigEndian::read_i16(&self.catalog_data[body_start..body_start + 2]);
        if record_type != CATALOG_FILE {
            return Ok(None);
        }
        if &self.catalog_data[body_start + 48..body_start + 52] != HFSPLUS_HARDLINK_FILE_TYPE
            || &self.catalog_data[body_start + 52..body_start + 56] != HFSPLUS_HARDLINK_FILE_CREATOR
        {
            return Ok(None);
        }
        Ok(Some(BigEndian::read_u32(
            &self.catalog_data[body_start + 44..body_start + 48],
        )))
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
    pub(crate) fn build_catalog_key(parent_cnid: u32, name: &str) -> Vec<u8> {
        let utf16: Vec<u16> = super::hfs_unicode::decompose_str(name);
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
    /// `true` on HFSX volumes whose catalog B-tree advertises binary (case-
    /// sensitive) key compare. Plain HFS+ and HFSX-with-case-folding both
    /// return `false`. Driven by the catalog header `keyCompareType` byte
    /// rather than the volume signature alone — Apple's spec explicitly
    /// allows an `H+` filesystem to declare itself HFSX-shaped.
    pub(crate) fn case_sensitive(&self) -> bool {
        self.catalog_header.key_compare_type == KEY_COMPARE_BINARY
    }

    fn catalog_compare(a: &[u8], b: &[u8], case_sensitive: bool) -> Ordering {
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
        hfs_common::compare_hfsplus_keys(parent_a, &name_a, parent_b, &name_b, case_sensitive)
    }

    /// Find a catalog record by (parent_cnid, name).
    /// Returns Some((node_idx, rec_idx, absolute_offset_in_catalog_data)) if found.
    /// Scans leaf nodes linearly (correct for all catalog sizes we encounter).
    fn find_catalog_record(&self, parent_cnid: u32, name: &str) -> Option<(u32, usize, usize)> {
        let search_key = Self::build_catalog_key(parent_cnid, name);
        let node_size = self.catalog_header.node_size as usize;
        let first_leaf = self.catalog_header.first_leaf_node;
        let cs = self.case_sensitive();

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
                if Self::catalog_compare(key_portion, &search_key, cs) == Ordering::Equal {
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
        let val_offset = val_offset.ok_or_else(|| {
            FilesystemError::NotFound(format!(
                "folder record for parent CNID {parent_cnid} not found in catalog"
            ))
        })?;
        let old_val = BigEndian::read_u32(&self.catalog_data[val_offset..val_offset + 4]);
        let new_val = (old_val as i64 + delta as i64).max(0) as u32;
        BigEndian::write_u32(&mut self.catalog_data[val_offset..val_offset + 4], new_val);
        let _ = node_idx; // suppress warning
        Ok(())
    }

    /// Insert a catalog record into the B-tree, handling splits and growth.
    ///
    /// Delegates to the shared `btree_insert_full`, which — for a non-sequential
    /// (per-`put`) insert into a full leaf — first tries a B*-style rotation into
    /// an adjacent sibling before splitting, lifting random-insert leaf occupancy
    /// from the ~69% a plain split leaves to ~88% (docs/hfsplus_btree_growth_plan.md
    /// §4d). `BTreeKeyFormat::HFSPLUS_CATALOG` drives the 2-byte / variable-length
    /// index keys. This is the same path classic HFS's `insert_catalog_record`
    /// uses; the previous hand-rolled split dance here packed less densely because
    /// it never rotated.
    fn insert_catalog_record(&mut self, key_record: &[u8]) -> Result<(), FilesystemError>
    where
        R: std::io::Write,
    {
        let cs = self.case_sensitive();
        // Insert; if the catalog runs out of free B-tree nodes, grow the fork
        // and retry once (docs/todo_hfsplus_fork_growth.md §4b). `btree_insert_full`
        // returns a *clean* DiskFull (no partial split) when it can't cover the
        // worst-case cascade, so the retry can't duplicate the record. A second
        // DiskFull means the volume itself is full — propagate it.
        let mut grown = false;
        loop {
            let cmp = |a: &[u8], b: &[u8]| Self::catalog_compare(a, b, cs);
            match hfs_common::btree_insert_full(
                &mut self.catalog_data,
                key_record,
                &BTreeKeyFormat::HFSPLUS_CATALOG,
                &cmp,
            ) {
                Ok(()) => break,
                Err(FilesystemError::DiskFull(_)) if !grown => {
                    self.grow_btree_fork(BTreeFork::Catalog)?;
                    grown = true;
                }
                Err(e) => return Err(e),
            }
        }
        self.catalog_header = BTreeHeaderRecord::parse(&self.catalog_data[14..14 + 106]);
        Ok(())
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

// --- Extents-overflow B-tree helpers (read+write side) ---

impl<R: Read + Seek> HfsPlusFilesystem<R> {
    /// Compare two extents-overflow records by key. Per TN1150 the canonical
    /// order is fileID, then forkType, then startBlock — even though the on-
    /// disk byte layout puts forkType first.
    ///
    /// Both arguments are full record bytes (or trimmed index records); the
    /// 2-byte key_len prefix lives at offset 0..2 and the key body at 2..12.
    fn extents_compare(a: &[u8], b: &[u8]) -> Ordering {
        if a.len() < 12 || b.len() < 12 {
            return a.len().cmp(&b.len());
        }
        let a_file = BigEndian::read_u32(&a[4..8]);
        let b_file = BigEndian::read_u32(&b[4..8]);
        a_file.cmp(&b_file).then(a[2].cmp(&b[2])).then_with(|| {
            let a_sb = BigEndian::read_u32(&a[8..12]);
            let b_sb = BigEndian::read_u32(&b[8..12]);
            a_sb.cmp(&b_sb)
        })
    }

    /// Build a full extents-overflow record as it lives on disk:
    /// `[key_len_u16=10, forkType, pad=0, fileID(BE), startBlock(BE),
    ///   8 × HFSPlusExtentDescriptor]` = 76 bytes total.
    ///
    /// `chunk` may hold fewer than 8 entries; remaining slots are written as
    /// empty descriptors (start=0, count=0) — same convention as `ForkData`.
    fn build_extents_overflow_record(
        file_id: u32,
        fork_type: u8,
        file_rel_start: u32,
        chunk: &[ExtentDescriptor],
    ) -> Vec<u8> {
        let mut rec = vec![0u8; 2 + 10 + 64];
        BigEndian::write_u16(&mut rec[0..2], 10); // key_len
        rec[2] = fork_type;
        rec[3] = 0; // pad
        BigEndian::write_u32(&mut rec[4..8], file_id);
        BigEndian::write_u32(&mut rec[8..12], file_rel_start);
        for (i, ext) in chunk.iter().take(8).enumerate() {
            ext.serialize(&mut rec[12 + i * 8..12 + i * 8 + 8]);
        }
        rec
    }
}

impl<R: Read + Write + Seek> HfsPlusFilesystem<R> {
    /// Insert a record into the extents-overflow B-tree, splitting the leaf and
    /// growing the root when the target leaf is full — mirroring
    /// `insert_catalog_record` / `insert_xattr_record`.
    ///
    /// The extents-overflow tree uses 2-byte ("big") keys with *fixed*-length
    /// index separators (`kBTBigKeysMask` set, `kBTVariableIndexKeysMask` clear),
    /// padded to the 10-byte `maxKeyLength`. `BTreeKeyFormat::HFSPLUS_EXTENTS`
    /// drives that; before P1's key-format descriptor the shared growth helpers
    /// forced the classic 1-byte/0x25 shape, which is why this path was pinned to
    /// a single leaf level (docs/hfsplus_btree_growth_plan.md §4a/P3).
    fn insert_extents_overflow_record(&mut self, key_record: &[u8]) -> Result<(), FilesystemError> {
        {
            let data = self.extents_overflow_data.as_ref().ok_or_else(|| {
                FilesystemError::Unsupported(
                    "volume has no extents-overflow B-tree — fragmented files cannot be created"
                        .into(),
                )
            })?;
            if BTreeHeader::read(data).node_size == 0 {
                return Err(FilesystemError::InvalidData(
                    "extents-overflow B-tree has zero node_size".into(),
                ));
            }
        }
        // Insert; grow the extents-overflow fork and retry once on a clean
        // node-exhaustion DiskFull (see `insert_catalog_record`).
        let mut grown = false;
        loop {
            let data = self.extents_overflow_data.as_mut().unwrap();
            match hfs_common::btree_insert_full(
                data,
                key_record,
                &BTreeKeyFormat::HFSPLUS_EXTENTS,
                &Self::extents_compare,
            ) {
                Ok(()) => return Ok(()),
                Err(FilesystemError::DiskFull(_)) if !grown => {
                    self.grow_btree_fork(BTreeFork::Extents)?;
                    grown = true;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Remove every overflow record belonging to `(file_id, fork_type)`.
    /// Used by `delete_entry_inner` to clean up overflow records before the
    /// catalog row is removed.
    fn remove_extents_overflow_records_for(&mut self, file_id: u32, fork_type: u8) {
        let Some(ref data) = self.extents_overflow_data else {
            return;
        };
        let header = BTreeHeader::read(data);
        let node_size = header.node_size as usize;
        if node_size == 0 {
            return;
        }

        // Collect (node_idx, rec_idx) pairs first — mutating during a walk
        // would invalidate offsets.
        let mut victims: Vec<(u32, usize)> = Vec::new();
        hfs_common::walk_leaf_records::<(), _>(
            data,
            header.first_leaf_node,
            node_size,
            |node_idx, rec_idx, _abs_off, rec| {
                if rec.len() < 12 {
                    return None;
                }
                let key_len = BigEndian::read_u16(&rec[0..2]) as usize;
                if key_len < 10 {
                    return None;
                }
                let rec_fork = rec[2];
                let rec_file = BigEndian::read_u32(&rec[4..8]);
                if rec_fork == fork_type && rec_file == file_id {
                    victims.push((node_idx, rec_idx));
                }
                None
            },
        );
        if victims.is_empty() {
            return;
        }

        // Remove highest rec_idx first per node so earlier indices stay
        // stable.
        let data = self.extents_overflow_data.as_mut().unwrap();
        victims.sort_by(|a, b| b.0.cmp(&a.0).then(b.1.cmp(&a.1)));
        let mut removed: u32 = 0;
        for (node_idx, rec_idx) in victims {
            let off = node_idx as usize * node_size;
            btree_remove_record(&mut data[off..off + node_size], node_size, rec_idx);
            removed += 1;
        }
        let mut h = BTreeHeader::read(data);
        h.leaf_records = h.leaf_records.saturating_sub(removed);
        h.write(data);
    }
}

// --- Attributes B-tree helpers (write side) ---

impl<R: Read + Seek> HfsPlusFilesystem<R> {
    /// Compare two attribute records by key. Per TN1150 the canonical order is
    /// fileID, then attrName as UTF-16BE codepoints, then startBlock — even
    /// though the on-disk byte layout puts startBlock before the name.
    ///
    /// Both arguments are full record bytes (or trimmed index records); the
    /// 2-byte key_len prefix lives at offset 0..2 and the key body at 2..2+keyLen.
    /// Key body layout: pad(2) + fileID(4) + startBlock(4) + nameLen(2) + name(UTF-16BE).
    pub(crate) fn attr_compare(a: &[u8], b: &[u8]) -> Ordering {
        if a.len() < 14 || b.len() < 14 {
            return a.len().cmp(&b.len());
        }
        let af = BigEndian::read_u32(&a[4..8]);
        let bf = BigEndian::read_u32(&b[4..8]);
        if af != bf {
            return af.cmp(&bf);
        }
        let an_len = BigEndian::read_u16(&a[12..14]) as usize;
        let bn_len = BigEndian::read_u16(&b[12..14]) as usize;
        let a_name_end = (14 + an_len * 2).min(a.len());
        let b_name_end = (14 + bn_len * 2).min(b.len());
        // UTF-16BE codepoint comparison via raw byte compare: high byte first
        // makes byte-lexicographic ordering equal to u16-codepoint ordering.
        let cmp = a[14..a_name_end].cmp(&b[14..b_name_end]);
        if cmp != Ordering::Equal {
            return cmp;
        }
        let asb = BigEndian::read_u32(&a[8..12]);
        let bsb = BigEndian::read_u32(&b[8..12]);
        asb.cmp(&bsb)
    }

    /// Build a complete inline attribute record:
    /// `[key_len_u16, pad=0, fileID, startBlock=0, nameLen, name(UTF-16BE),
    ///   recordType=HFSPLUS_ATTR_INLINE_DATA, reserved[2]=0, attrSize, attrData]`.
    ///
    /// Caller is expected to have already NFD-normalized `name` if needed; we
    /// re-normalize defensively so on-disk keys stay canonical.
    pub(crate) fn build_inline_attr_record(cnid: u32, name: &str, value: &[u8]) -> Vec<u8> {
        let utf16: Vec<u16> = super::hfs_unicode::decompose_str(name);
        let key_body_len = 12 + utf16.len() * 2;
        let data_section_len = 4 + 8 + 4 + value.len();
        let mut rec = vec![0u8; 2 + key_body_len + data_section_len];
        BigEndian::write_u16(&mut rec[0..2], key_body_len as u16);
        // pad already zero at [2..4]
        BigEndian::write_u32(&mut rec[4..8], cnid);
        BigEndian::write_u32(&mut rec[8..12], 0); // startBlock = 0 for inline
        BigEndian::write_u16(&mut rec[12..14], utf16.len() as u16);
        for (i, ch) in utf16.iter().enumerate() {
            BigEndian::write_u16(&mut rec[14 + i * 2..16 + i * 2], *ch);
        }
        let data_off = 2 + key_body_len;
        BigEndian::write_u32(&mut rec[data_off..data_off + 4], HFSPLUS_ATTR_INLINE_DATA);
        // reserved[2] (8 bytes) at [data_off+4..data_off+12] already zero
        BigEndian::write_u32(&mut rec[data_off + 12..data_off + 16], value.len() as u32);
        rec[data_off + 16..data_off + 16 + value.len()].copy_from_slice(value);
        rec
    }
}

impl<R: Read + Write + Seek> HfsPlusFilesystem<R> {
    /// Insert a record into the attributes B-tree. Splits the leaf and grows
    /// the root if the target leaf is full, mirroring `insert_catalog_record`.
    fn insert_xattr_record(&mut self, key_record: &[u8]) -> Result<(), FilesystemError> {
        if self.vh.attributes_file.logical_size == 0 {
            return Err(FilesystemError::Unsupported(
                "volume has no attributes B-tree — extended attribute writes \
                 require an existing attributes file"
                    .into(),
            ));
        }
        self.ensure_attributes_loaded()?;
        let data = self
            .attributes_data
            .as_mut()
            .expect("ensure_attributes_loaded populated the buffer");
        let header = BTreeHeader::read(data);
        let node_size = header.node_size as usize;
        if node_size == 0 {
            return Err(FilesystemError::InvalidData(
                "attributes B-tree has zero node_size".into(),
            ));
        }
        // Reject records that can't fit in any leaf node — node has 14 bytes
        // of descriptor + a 2-byte free-space slot in the offset table.
        if key_record.len() + 2 > node_size.saturating_sub(14 + 2) {
            return Err(FilesystemError::XattrTooLarge(format!(
                "xattr record of {} bytes exceeds attributes node capacity ({} bytes); \
                 fork-style attribute writes are not yet implemented",
                key_record.len(),
                node_size,
            )));
        }
        // The attributes B-tree, like the catalog, uses 2-byte big keys and
        // variable-length index separators, so it threads HFSPLUS_ATTRIBUTES
        // through the shared insert (which also rotates before splitting).
        // Insert; grow the attributes fork and retry once on a clean
        // node-exhaustion DiskFull (see `insert_catalog_record`).
        let mut grown = false;
        loop {
            let data = self
                .attributes_data
                .as_mut()
                .expect("ensure_attributes_loaded populated the buffer");
            match hfs_common::btree_insert_full(
                data,
                key_record,
                &BTreeKeyFormat::HFSPLUS_ATTRIBUTES,
                &Self::attr_compare,
            ) {
                Ok(()) => return Ok(()),
                Err(FilesystemError::DiskFull(_)) if !grown => {
                    self.grow_btree_fork(BTreeFork::Attributes)?;
                    grown = true;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Remove every attribute record matching `(cnid, name)`. Returns the
    /// number of records removed (0 on a no-attributes-file volume or when no
    /// records match). Used by both `remove_xattr` (caller-driven) and
    /// `delete_entry` (cleanup, with `name == None` to drop all xattrs on the
    /// CNID).
    fn remove_xattr_records(
        &mut self,
        cnid: u32,
        name: Option<&str>,
    ) -> Result<u32, FilesystemError> {
        if self.vh.attributes_file.logical_size == 0 {
            return Ok(0);
        }
        self.ensure_attributes_loaded()?;
        let data = self
            .attributes_data
            .as_mut()
            .expect("ensure_attributes_loaded populated the buffer");
        let header = BTreeHeader::read(data);
        let node_size = header.node_size as usize;
        if node_size == 0 {
            return Ok(0);
        }
        let target_utf16: Option<Vec<u16>> = name.map(super::hfs_unicode::decompose_str);

        let mut victims: Vec<(u32, usize)> = Vec::new();
        hfs_common::walk_leaf_records::<(), _>(
            data,
            header.first_leaf_node,
            node_size,
            |node_idx, rec_idx, _abs_off, rec| {
                if rec.len() < 14 {
                    return None;
                }
                let key_len = BigEndian::read_u16(&rec[0..2]) as usize;
                if key_len < 12 || 2 + key_len > rec.len() {
                    return None;
                }
                let file_id = BigEndian::read_u32(&rec[4..8]);
                if file_id != cnid {
                    return None;
                }
                if let Some(target) = target_utf16.as_ref() {
                    let nl = BigEndian::read_u16(&rec[12..14]) as usize;
                    if nl != target.len() {
                        return None;
                    }
                    if 14 + nl * 2 > 2 + key_len {
                        return None;
                    }
                    for (i, ch) in target.iter().enumerate() {
                        let off = 14 + i * 2;
                        if BigEndian::read_u16(&rec[off..off + 2]) != *ch {
                            return None;
                        }
                    }
                }
                victims.push((node_idx, rec_idx));
                None
            },
        );
        if victims.is_empty() {
            return Ok(0);
        }
        // Highest (node, rec) first so earlier indices stay stable.
        victims.sort_by(|a, b| b.0.cmp(&a.0).then(b.1.cmp(&a.1)));
        let mut removed: u32 = 0;
        for (node_idx, rec_idx) in &victims {
            let off = *node_idx as usize * node_size;
            btree_remove_record(&mut data[off..off + node_size], node_size, *rec_idx);
            removed += 1;
        }
        let mut h = BTreeHeader::read(data);
        h.leaf_records = h.leaf_records.saturating_sub(removed);
        h.write(data);
        Ok(removed)
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

    /// Write the extents-overflow B-tree data back to disk through the
    /// extents_file fork extents. No-op on volumes without an overflow file
    /// or when the cached buffer hasn't been touched.
    fn write_extents_overflow(&mut self) -> Result<(), FilesystemError> {
        if self.vh.extents_file.logical_size == 0 {
            return Ok(());
        }
        let Some(ref data) = self.extents_overflow_data else {
            return Ok(());
        };
        write_fork_data(
            &mut self.reader,
            self.partition_offset,
            self.vh.block_size,
            &self.vh.extents_file,
            data,
        )
    }

    /// Write the attributes B-tree data back to disk through the
    /// attributes_file fork extents. No-op on volumes without an attributes
    /// file or when the cached buffer hasn't been touched.
    fn write_attributes(&mut self) -> Result<(), FilesystemError> {
        if self.vh.attributes_file.logical_size == 0 {
            return Ok(());
        }
        if self.attributes_data.is_none() {
            return Ok(());
        }
        write_fork_data_with_overflow(
            &mut self.reader,
            self.partition_offset,
            self.vh.block_size,
            &self.vh.attributes_file,
            HFSPLUS_ATTRIBUTES_FILE_ID,
            0,
            self.extents_overflow_data.as_deref(),
            self.attributes_data.as_deref().unwrap(),
        )
    }

    /// Write the catalog B-tree data back to disk through the catalog_file
    /// fork extents — including any overflow extents past the 8 inline slots
    /// that §4b growth may have added.
    fn write_catalog(&mut self) -> Result<(), FilesystemError> {
        write_fork_data_with_overflow(
            &mut self.reader,
            self.partition_offset,
            self.vh.block_size,
            &self.vh.catalog_file,
            HFSPLUS_CATALOG_FILE_ID,
            0,
            self.extents_overflow_data.as_deref(),
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

    /// Allocate `blocks_needed` blocks from the allocation bitmap, possibly
    /// across multiple non-contiguous extents.
    ///
    /// Strategy: greedy largest-run-first — collect every clear run, sort by
    /// length descending, take a prefix of each run until the request is
    /// satisfied. This avoids `DiskFull` on volumes that have plenty of free
    /// space but no contiguous run large enough for the whole request.
    ///
    /// On success the bitmap is updated, `vh.free_blocks` is decremented,
    /// and `vh.next_allocation` is set to the byte just past the last
    /// extent so the next allocation starts looking forward instead of
    /// re-scanning the same area.
    fn allocate_extents(
        &mut self,
        blocks_needed: u32,
    ) -> Result<Vec<ExtentDescriptor>, FilesystemError> {
        if blocks_needed == 0 {
            return Ok(Vec::new());
        }
        self.ensure_bitmap()?;
        let total_blocks = self.vh.total_blocks;
        let bitmap = self.bitmap.as_mut().unwrap();

        // Quick total-free check before any scan: if free_blocks < requested
        // we cannot possibly satisfy the request, regardless of layout.
        if self.vh.free_blocks < blocks_needed {
            return Err(FilesystemError::DiskFull(format!(
                "{} blocks free, {} requested",
                self.vh.free_blocks, blocks_needed
            )));
        }

        let mut runs = bitmap_collect_clear_runs_be(bitmap, total_blocks);
        // Largest run first, ties broken by lower start (deterministic).
        runs.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

        let mut extents: Vec<ExtentDescriptor> = Vec::new();
        let mut remaining = blocks_needed;
        let mut last_end = 0u32;
        for (start, len) in runs {
            if remaining == 0 {
                break;
            }
            let take = len.min(remaining);
            extents.push(ExtentDescriptor {
                start_block: start,
                block_count: take,
            });
            for i in 0..take {
                bitmap_set_bit_be(bitmap, start + i);
            }
            remaining -= take;
            last_end = start + take;
        }

        if remaining > 0 {
            // Roll back any partial allocation before reporting failure so
            // the bitmap stays consistent for the snapshot guard.
            for ext in &extents {
                for i in 0..ext.block_count {
                    bitmap_clear_bit_be(bitmap, ext.start_block + i);
                }
            }
            return Err(FilesystemError::DiskFull(format!(
                "needed {blocks_needed} blocks, short by {remaining} after gathering all free runs"
            )));
        }

        self.vh.free_blocks -= blocks_needed;
        self.vh.next_allocation = last_end;
        Ok(extents)
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

    /// Free every block referenced by a fork's *overflow* extents (the
    /// extras living in the extents-overflow B-tree, beyond the 8 inline
    /// `ForkData.extents`). The record removal is a separate call —
    /// `remove_extents_overflow_records_for` — so callers can sequence
    /// "free blocks" and "remove records" independently.
    fn free_fork_overflow_extents(&mut self, file_id: u32, fork_type: u8, fork: &ForkData) {
        let Some(ref ext_data) = self.extents_overflow_data else {
            return;
        };
        let inline_blocks: u32 = fork.extents.iter().map(|e| e.block_count).sum();
        let overflow =
            collect_hfsplus_overflow_extents(ext_data, file_id, fork_type, inline_blocks);
        for ext in overflow {
            if ext.block_count == 0 {
                continue;
            }
            self.free_blocks(ext.start_block, ext.block_count);
        }
    }

    // --- B-tree fork grow-on-full (docs/todo_hfsplus_fork_growth.md §4b) ---

    /// Mutable handle to the in-memory tree buffer for `kind`. Errors if the
    /// volume has no such B-tree (e.g. growing the extents-overflow tree on a
    /// volume that never built one).
    fn tree_buf_mut(&mut self, kind: BTreeFork) -> Result<&mut Vec<u8>, FilesystemError> {
        match kind {
            BTreeFork::Catalog => Ok(&mut self.catalog_data),
            BTreeFork::Extents => self.extents_overflow_data.as_mut().ok_or_else(|| {
                FilesystemError::Unsupported("volume has no extents-overflow B-tree".into())
            }),
            BTreeFork::Attributes => self.attributes_data.as_mut().ok_or_else(|| {
                FilesystemError::Unsupported("volume has no attributes B-tree".into())
            }),
        }
    }

    /// The volume-header `ForkData` backing `kind`.
    fn fork_mut(&mut self, kind: BTreeFork) -> &mut ForkData {
        match kind {
            BTreeFork::Catalog => &mut self.vh.catalog_file,
            BTreeFork::Extents => &mut self.vh.extents_file,
            BTreeFork::Attributes => &mut self.vh.attributes_file,
        }
    }

    /// First volume block immediately *after* the fork's last inline extent —
    /// the candidate start for a contiguous tail growth. Returns 0 (never a
    /// valid data block) when the fork has no extents. Overflow extents are
    /// intentionally not consulted: if the true tail lies in an overflow
    /// extent, `allocate_contiguous_after` simply finds the candidate block
    /// busy and the caller falls back to the general allocator — correct, just
    /// not optimally contiguous.
    fn fork_last_inline_block(&self, kind: BTreeFork) -> u32 {
        let fork = match kind {
            BTreeFork::Catalog => &self.vh.catalog_file,
            BTreeFork::Extents => &self.vh.extents_file,
            BTreeFork::Attributes => &self.vh.attributes_file,
        };
        let mut last = 0u32;
        for e in &fork.extents {
            if e.block_count == 0 {
                break;
            }
            last = e.start_block + e.block_count;
        }
        last
    }

    /// Try to claim `blocks` free volume blocks *immediately following*
    /// `after_block`, so the fork's last extent simply gets longer (extent
    /// count unchanged — the fragmentation-free common case). Returns the new
    /// extent on success, or `None` if `after_block` is 0, the run would run
    /// past the volume, or any block in it is already allocated. Updates the
    /// bitmap, `free_blocks`, and `next_allocation` exactly like
    /// `allocate_extents`.
    fn allocate_contiguous_after(
        &mut self,
        after_block: u32,
        blocks: u32,
    ) -> Option<ExtentDescriptor> {
        if blocks == 0 || after_block == 0 {
            return None;
        }
        self.ensure_bitmap().ok()?;
        if after_block.checked_add(blocks)? > self.vh.total_blocks {
            return None;
        }
        if self.vh.free_blocks < blocks {
            return None;
        }
        let bitmap = self.bitmap.as_mut()?;
        for i in 0..blocks {
            if bitmap_test_bit_be(bitmap, after_block + i) {
                return None; // tail is not free — caller falls back
            }
        }
        for i in 0..blocks {
            bitmap_set_bit_be(bitmap, after_block + i);
        }
        self.vh.free_blocks -= blocks;
        self.vh.next_allocation = (after_block + blocks).max(self.vh.next_allocation);
        Some(ExtentDescriptor {
            start_block: after_block,
            block_count: blocks,
        })
    }

    /// Attach freshly-allocated `new_extents` to the fork backing `kind`,
    /// merging the first one into the fork's last inline extent when they are
    /// physically contiguous (so a contiguous tail grow keeps the extent count
    /// unchanged), dropping the rest into free inline slots, and spilling any
    /// remainder past the 8 inline slots into the extents-overflow B-tree
    /// (Phase B). Bumps the fork's `total_blocks` / `logical_size`.
    fn attach_extents_to_fork(
        &mut self,
        kind: BTreeFork,
        new_extents: &[ExtentDescriptor],
    ) -> Result<(), FilesystemError> {
        let block_size = self.vh.block_size as u64;
        let fork = self.fork_mut(kind);
        let mut added_blocks = 0u32;
        let mut overflow: Vec<ExtentDescriptor> = Vec::new();
        for ext in new_extents {
            if ext.block_count == 0 {
                continue;
            }
            added_blocks += ext.block_count;
            // Index of the last non-empty inline slot, if any.
            let last_idx = (0..8)
                .take_while(|&i| fork.extents[i].block_count != 0)
                .last();
            let mut placed = false;
            if let Some(i) = last_idx {
                let le = fork.extents[i];
                if le.start_block + le.block_count == ext.start_block {
                    fork.extents[i].block_count += ext.block_count;
                    placed = true;
                }
            }
            if !placed {
                if let Some(i) = (0..8).find(|&i| fork.extents[i].block_count == 0) {
                    fork.extents[i] = *ext;
                    placed = true;
                }
            }
            if !placed {
                overflow.push(*ext);
            }
        }
        fork.total_blocks += added_blocks;
        fork.logical_size += added_blocks as u64 * block_size;
        if !overflow.is_empty() {
            self.spill_fork_extents_to_overflow(kind, &overflow)?;
        }
        Ok(())
    }

    /// Phase B: record fork `extents` that overflowed the 8 inline slots into
    /// the extents-overflow B-tree, keyed by the special file's reserved CNID
    /// (fork type 0). These append after the fork's existing inline + overflow
    /// blocks, so their file-relative start is `inline_blocks +
    /// existing_overflow_blocks`. The write side (`write_fork_data_with_overflow`)
    /// and read side (`read_fork_with_overflow`) both consult these records.
    fn spill_fork_extents_to_overflow(
        &mut self,
        kind: BTreeFork,
        extents: &[ExtentDescriptor],
    ) -> Result<(), FilesystemError> {
        // The extents-overflow file cannot store its *own* overflow extents
        // (it would have to contain a record describing where it lives) — HFS+
        // limits it to 8 inline extents. In practice it stays tiny, so this is
        // a hard error if it ever happens, and the recursion guard that keeps
        // a catalog/attributes spill (which grows the extents fork) from
        // re-entering here for the extents fork itself.
        if kind == BTreeFork::Extents {
            return Err(FilesystemError::DiskFull(
                "extents-overflow B-tree fork exceeded its 8 inline extents".into(),
            ));
        }
        let file_id = kind.file_id();
        let fork = match kind {
            BTreeFork::Catalog => &self.vh.catalog_file,
            BTreeFork::Attributes => &self.vh.attributes_file,
            BTreeFork::Extents => unreachable!(),
        };
        let inline_blocks: u32 = fork.extents.iter().map(|e| e.block_count).sum();
        let existing_overflow: u32 = match self.extents_overflow_data.as_deref() {
            Some(ed) => collect_hfsplus_overflow_extents(ed, file_id, 0, inline_blocks)
                .iter()
                .map(|e| e.block_count)
                .sum(),
            None => 0,
        };
        let mut file_rel = inline_blocks + existing_overflow;
        // HFS+ extents-overflow records hold up to 8 extent descriptors each.
        for chunk in extents.chunks(8) {
            let rec = Self::build_extents_overflow_record(file_id, 0, file_rel, chunk);
            self.insert_extents_overflow_record(&rec)?;
            file_rel += chunk.iter().map(|e| e.block_count).sum::<u32>();
        }
        Ok(())
    }

    /// Grow the B-tree fork backing `kind` by at least one clump of nodes so a
    /// subsequent `btree_insert_full` has free nodes to allocate. Allocates
    /// volume blocks (preferring a contiguous tail extension), attaches them to
    /// the fork, extends the in-memory tree buffer, and publishes the new nodes
    /// to the tree header (`total_nodes` / `free_nodes`). The node-allocation
    /// bitmap bits for the new nodes already read as 0 (free) inside the header
    /// node's capacity; past that, a map node is appended to the bitmap chain
    /// (§4b Phase C).
    fn grow_btree_fork(&mut self, kind: BTreeFork) -> Result<(), FilesystemError> {
        let block_size = self.vh.block_size;

        let (node_size, total_nodes) = {
            let buf = self.tree_buf_mut(kind)?;
            let h = BTreeHeader::read(buf);
            (h.node_size as usize, h.total_nodes)
        };
        if node_size == 0 || node_size < 256 {
            return Err(FilesystemError::InvalidData(format!(
                "{} B-tree has invalid node_size {node_size}",
                kind.label()
            )));
        }
        let blocks_per_node = (node_size as u32 / block_size).max(1);

        // Grow by at least 8 nodes, rounded up to the fork's clump, to amortise
        // the work and keep per-`put` workloads from re-entering grow on every
        // insert (which would fragment the fork and defeat the contiguous-tail
        // strategy).
        let clump_nodes = (self.vh.data_clump_size as usize)
            .div_ceil(node_size)
            .max(1) as u32;
        let grow_nodes = clump_nodes.max(8);

        // The header node's bitmap addresses (node_size - 256) * 8 node bits;
        // each map node (BTNodeKind=2) chained off the header adds
        // map_node_bitmap_bytes*8 more. When a grow would push the node count
        // past the current addressable range, we must append one map node so
        // the new node indices have bitmap bits (§4b Phase C). A single map
        // node covers far more nodes than one grow adds, so at most one new map
        // node is ever needed per grow.
        let header_cap = ((node_size - 256) * 8) as u32;
        let per_map = (hfs_common::map_node_bitmap_bytes(node_size) * 8) as u32;
        let existing_map = {
            let buf = self.tree_buf_mut(kind)?;
            (hfs_common::btree_bitmap_segments(buf, node_size).len() as u32).saturating_sub(1)
        };
        let current_capacity = header_cap + existing_map * per_map;
        let need_map = total_nodes + grow_nodes > current_capacity;
        if need_map && per_map == 0 {
            return Err(FilesystemError::DiskFull(format!(
                "{} B-tree node bitmap full and node size too small for map nodes",
                kind.label()
            )));
        }
        let add_slots = grow_nodes + u32::from(need_map);
        let grow_blocks = add_slots * blocks_per_node;

        // Allocate volume blocks, preferring a contiguous tail extension so the
        // fork's last extent just gets longer.
        let last_block = self.fork_last_inline_block(kind);
        let new_extents = match self.allocate_contiguous_after(last_block, grow_blocks) {
            Some(e) => vec![e],
            None => self.allocate_extents(grow_blocks)?,
        };

        self.attach_extents_to_fork(kind, &new_extents)?;

        // Extend the in-memory tree buffer and publish the new nodes. The added
        // bytes are zero, so the new node-bitmap bits read as free and
        // `btree_alloc_node` will hand them out.
        {
            let buf = self.tree_buf_mut(kind)?;
            buf.resize(buf.len() + add_slots as usize * node_size, 0);

            if need_map {
                // The new map node takes the first new slot; the free data
                // nodes follow it. Its own index is still inside the *existing*
                // bitmap capacity (one grow adds far fewer nodes than the
                // header/last-map-node segment can address), so we can mark it
                // used after linking it into the chain.
                let map_idx = total_nodes;
                let tail = btree_map_chain_tail(buf, node_size); // 0 => link via header node 0
                hfs_common::init_map_node(buf, node_size, map_idx, tail, 0);
                let linker_off = tail as usize * node_size; // node 0 is the header
                BigEndian::write_u32(&mut buf[linker_off..linker_off + 4], map_idx);
                hfs_common::btree_bitmap_set(buf, node_size, map_idx);
            }

            let mut h = BTreeHeader::read(buf);
            h.total_nodes += add_slots;
            h.free_nodes += grow_nodes; // the map node is allocated, not free
            h.write(buf);
        }
        self.vh.write_count = self.vh.write_count.wrapping_add(1);
        if kind == BTreeFork::Catalog {
            self.catalog_header = BTreeHeaderRecord::parse(&self.catalog_data[14..14 + 106]);
        }
        Ok(())
    }

    /// Write file data to allocated blocks. Returns the ForkData describing
    /// the allocation.
    ///
    /// Allocates via `allocate_extents`. The first 8 extents land in the
    /// inline `ForkData.extents` slots; the remainder is grouped into
    /// 8-extent chunks and inserted into the extents-overflow B-tree as
    /// records keyed by `(forkType, fileID, file-relative startBlock)`.
    ///
    /// `file_id` and `fork_type` are required so overflow records carry
    /// the correct key. Pass `HFSPLUS_FORK_DATA` for the data fork and
    /// `HFSPLUS_FORK_RESOURCE` for the resource fork.
    fn write_data_to_blocks(
        &mut self,
        data: &mut dyn std::io::Read,
        data_len: u64,
        file_id: u32,
        fork_type: u8,
    ) -> Result<ForkData, FilesystemError> {
        if data_len == 0 {
            return Ok(ForkData::empty());
        }
        let block_size = self.vh.block_size as u64;
        let blocks_needed = data_len.div_ceil(block_size) as u32;
        let extents = self.allocate_extents(blocks_needed)?;

        // Stream the data block-by-block, walking the extent vec in order.
        let mut buf = vec![0u8; block_size as usize];
        let mut remaining = data_len;
        'outer: for ext in &extents {
            for i in 0..ext.block_count {
                if remaining == 0 {
                    break 'outer;
                }
                let to_read = remaining.min(block_size) as usize;
                data.read_exact(&mut buf[..to_read]).map_err(|e| {
                    FilesystemError::Io(std::io::Error::new(
                        e.kind(),
                        format!("reading file data: {e}"),
                    ))
                })?;
                if to_read < block_size as usize {
                    buf[to_read..].fill(0);
                }
                self.write_block(ext.start_block + i, &buf)?;
                remaining -= to_read as u64;
            }
        }

        // Build the inline ForkData from the first 8 extents.
        let mut fork = ForkData::empty();
        fork.logical_size = data_len;
        fork.total_blocks = blocks_needed;
        for (slot, ext) in fork.extents.iter_mut().zip(extents.iter().take(8)) {
            *slot = *ext;
        }

        // For each 8-extent chunk past the inline capacity, build and
        // insert one overflow record. The key's startBlock is the file-
        // relative block index where the chunk's first extent begins.
        if extents.len() > 8 {
            let inline_blocks: u32 = extents.iter().take(8).map(|e| e.block_count).sum();
            let mut file_rel_start = inline_blocks;
            for chunk in extents[8..].chunks(8) {
                let rec =
                    Self::build_extents_overflow_record(file_id, fork_type, file_rel_start, chunk);
                self.insert_extents_overflow_record(&rec)?;
                file_rel_start += chunk.iter().map(|e| e.block_count).sum::<u32>();
            }
        }

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

    /// Stamp the per-record date fields. `frec_start` is the absolute byte
    /// offset of the catalog record's data section (i.e. just past the
    /// `recordType` field at offset 0 of the record body). At the standard
    /// HFS+ file/folder record offsets:
    ///   contentModDate @ +16, attributeModDate @ +20, accessDate @ +24.
    ///
    /// Pass `update_content = true` when the operation changed file content
    /// (fork rewrite). Attribute-only changes (type/creator, finder flags,
    /// blessed-folder) leave contentModDate alone.
    fn stamp_record_dates(&mut self, frec_start: usize, update_content: bool) {
        let now = hfs_common::hfs_now();
        if update_content {
            BigEndian::write_u32(
                &mut self.catalog_data[frec_start + 16..frec_start + 20],
                now,
            );
        }
        BigEndian::write_u32(
            &mut self.catalog_data[frec_start + 20..frec_start + 24],
            now,
        );
        BigEndian::write_u32(
            &mut self.catalog_data[frec_start + 24..frec_start + 28],
            now,
        );
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
        let utf16: Vec<u16> = super::hfs_unicode::decompose_str(name);
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
        // Stamp clean-unmount state into the volume header. Without this,
        // a VH read from a mounted/dirty volume (bit 8 clear, bits 11/14
        // set) gets persisted as-is and Disk Utility refuses to mount the
        // result. Mirrors the unconditional `drAtrbUmounted` set in
        // classic HFS `write_mdb`. See ~/Downloads/HFSTests/ corruption
        // case (2026-05): rusty-backup edit on /dev/rdisk preserved
        // attributes 0x80004000 instead of writing 0x80000100.
        self.vh.attributes |= HFSPLUS_VOLUME_UNMOUNTED_BIT;
        self.vh.attributes &= !HFSPLUS_BOOT_VOLUME_INCONSISTENT_BIT;
        self.vh.attributes &= !HFSPLUS_VOLUME_INCONSISTENT_BIT;
        self.write_catalog()?;
        self.write_extents_overflow()?;
        self.write_attributes()?;
        self.write_allocation_bitmap()?;
        self.write_volume_header()?;
        if let Some(ref boot) = self.pending_boot_blocks {
            self.reader.seek(SeekFrom::Start(self.partition_offset))?;
            self.reader.write_all(boot.as_ref())?;
        }
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
            link_target_cnid: None,
            amiga_protection: None,
            amiga_comment: None,
            amiga_date: None,
            dos_attributes: None,
            finder_flags: None,
            prodos_file_type: None,
            mac_dates: None,
        })
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        // For directory hardlinks, `entry.location` points at the stub
        // catalog row; the actual children live under the `dir_<N>` inode
        // referenced by `link_target_cnid`. Following the link here keeps
        // recursive walks (browse view, fsck, capture) seeing the real
        // contents instead of the empty stub.
        let parent_cnid = entry
            .link_target_cnid
            .map(|c| c as u32)
            .unwrap_or(entry.location as u32);
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
                    link_inode_num,
                    dir_link_inode_num,
                } => {
                    let path = if entry.path == "/" {
                        format!("/{name}")
                    } else {
                        format!("{}/{name}", entry.path)
                    };
                    // Directory hardlink: surface as a Directory entry that
                    // points at the dir-inode under .HFS+ Private Directory
                    // Data. list_directory called on it follows the link
                    // (see the parent_cnid resolution at the top of this fn).
                    if let Some(num) = dir_link_inode_num {
                        let mut fe = FileEntry::new_directory(name, path, file_id as u64);
                        fe.type_code = Some(type_code);
                        fe.creator_code = Some(creator_code);
                        if let Some(target_cnid) = self.resolve_dir_hardlink_inode(num)? {
                            fe.link_target_cnid = Some(target_cnid as u64);
                        }
                        let _ = (data_fork, rsrc_fork, finder_flags);
                        entries.push(fe);
                        continue;
                    }
                    // For file hardlinks (`fdType='hlnk'`/`fdCreator='hfs+'`),
                    // resolve the inode CNID and surface the inode's data /
                    // resource fork sizes — the link's own forks are empty.
                    let mut display_size = data_size;
                    let mut display_rsrc = rsrc_size;
                    let resolved_target = match link_inode_num {
                        Some(num) => self.resolve_hardlink_inode(num)?,
                        None => None,
                    };
                    if let Some(target_cnid) = resolved_target {
                        if let Some((d, r)) = self.find_file_by_id(target_cnid) {
                            display_size = d.logical_size;
                            display_rsrc = r.logical_size;
                        }
                    }
                    let mut fe = FileEntry::new_file(name, path, display_size, file_id as u64);
                    fe.type_code = Some(type_code);
                    fe.creator_code = Some(creator_code);
                    fe.finder_flags = Some(finder_flags);
                    if display_rsrc > 0 {
                        fe.resource_fork_size = Some(display_rsrc);
                    }
                    if let Some(target) = resolved_target {
                        fe.link_target_cnid = Some(target as u64);
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
                    let _ = (data_fork, rsrc_fork);
                    entries.push(fe);
                }
            }
        }

        entries.sort_by_key(|a| a.name.to_lowercase());
        Ok(entries)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        // Hardlink stubs carry empty forks; follow the indirection to the
        // inode under `HFS+ Private Data` and read its data fork instead.
        let file_id = entry
            .link_target_cnid
            .map(|c| c as u32)
            .unwrap_or(entry.location as u32);
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
        // Hardlink stubs: read forks from the inode CNID, not the stub.
        let file_id = entry
            .link_target_cnid
            .map(|c| c as u32)
            .unwrap_or(entry.location as u32);
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

    fn uses_colon_paths(&self) -> bool {
        true
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

    fn defragmented_minimum_size(&mut self) -> Result<u64, FilesystemError> {
        // After a packed clone the volume holds:
        //   - boot blocks + primary VH (in allocation block 0)
        //   - all currently-allocated user data + B-tree files + bitmap
        //   - the alternate VH at the last 1024 bytes of the volume
        //
        // We can't access the `SourceCatalogSnapshot` here (it's built
        // later by the clone pipeline), so we can't run the precise
        // record-byte tally that [`hfsplus_defrag::plan_defrag_layout`]
        // uses. Instead, over-estimate the target catalog as
        // `2× source_catalog_file.logical_size`, matching the packing
        // overhead multiplier the planner applies. Then add an extra ×2
        // safety pad: the planner's exact estimate could in theory exceed
        // 2× source catalog if a volume has been heavily deleted (lots of
        // empty space inside leaves that the precise estimator wouldn't
        // count), but it can't exceed 4×. This puts a hard ceiling on the
        // partition envelope without the catalog-growth invariant the
        // planner enforces precisely on the clone path.
        //
        // We also add 1024 bytes to host the new alt-VH at the end of the
        // smaller image and round up to a whole block.
        let block_size = self.vh.block_size as u64;
        let used_blocks = self.vh.total_blocks.saturating_sub(self.vh.free_blocks) as u64;
        let used_bytes = used_blocks.saturating_mul(block_size);
        // Catalog-growth pad: 3× source catalog logical size. Combined
        // with the catalog blocks already counted in `used_blocks` (the
        // source's allocated catalog) that's 4× source total — the safe
        // over-estimate for the planner's exact-record-bytes × 2 result.
        let catalog_pad = self.vh.catalog_file.logical_size.saturating_mul(3);
        let total = used_bytes.saturating_add(catalog_pad).saturating_add(1024);
        Ok(total.div_ceil(block_size) * block_size)
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

    fn fragmentation_stats(
        &mut self,
    ) -> Option<Result<super::filesystem::FragmentationStats, FilesystemError>> {
        Some(Ok(hfsplus_fragmentation_stats(
            self.catalog_data(),
            self.catalog_first_leaf(),
            self.catalog_node_size(),
        )))
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

    fn fsck(&mut self) -> Option<Result<super::fsck::FsckResult, FilesystemError>> {
        Some(super::hfsplus_fsck::check(self))
    }

    fn read_journal(
        &mut self,
    ) -> Result<Option<super::hfsplus_journal::JournalState>, FilesystemError> {
        // Delegate to the inherent method (which takes priority for concrete
        // calls); this override is what `dyn Filesystem` callers reach.
        HfsPlusFilesystem::read_journal(self)
    }

    fn journal_detail(
        &mut self,
    ) -> Result<Option<super::hfsplus_journal::JournalDetail>, FilesystemError> {
        HfsPlusFilesystem::journal_detail(self)
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
/// label without loading the full catalog.
///
/// The volume name lives in the *key* of the root folder record at
/// `(parent=1, name=<vol_name>)`. Catalog records are sorted primarily by
/// parent CNID (ascending u32), and `parent=1` is smaller than every other
/// parent in the catalog — so the root folder record is guaranteed to be
/// record 0 of the first leaf node. We don't need to look at the root
/// thread record at all (it sorts after every `parent=1` and `parent=2`
/// non-thread record, and on a volume with many top-level entries it can
/// spill into a later leaf — which is what was causing this probe to come
/// back empty for real volumes).
///
/// Reads node 0 (the B-tree header) and the first leaf node of the catalog
/// directly via the catalog fork's inline extents — typically two small
/// reads. Returns None if the partition doesn't hold an HFS+/HFSX volume.
pub fn probe_hfsplus_volume_label<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
) -> Option<String> {
    // APM "Apple_HFS" partitions can be pure classic HFS, native HFS+, or
    // **HFS-wrapped HFS+** (the legacy hybrid where a classic HFS MDB at
    // partition_offset+1024 carries `drEmbedSigWord=0x482B` and points at
    // the embedded HFS+ region somewhere inside the partition). The wrapper
    // case is the failure mode we hit on real volumes — `resolve_apple_hfs`
    // returns the embedded HFS+ offset for all three cases.
    let (fs_type, hfsplus_offset) = super::resolve_apple_hfs(reader, partition_offset);
    if fs_type != "hfsplus" {
        return None;
    }
    reader.seek(SeekFrom::Start(hfsplus_offset + 1024)).ok()?;
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
        hfsplus_offset,
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

    // Read the first leaf node. Record 0 is the root folder record whose key
    // is (parent=1, nameLength, name in UTF-16BE).
    let leaf = read_node_from_fork(
        reader,
        hfsplus_offset,
        vh.block_size,
        &vh.catalog_file,
        header.first_leaf_node,
        node_size,
    )?;
    if leaf.len() < node_size || leaf[8] as i8 != -1 {
        return None;
    }
    let num_records = BigEndian::read_u16(&leaf[10..12]) as usize;
    if num_records == 0 {
        return None;
    }
    let (rec_start, rec_end) = super::hfs_common::btree_record_range(&leaf, node_size, 0);
    if rec_start >= rec_end || rec_end > node_size {
        return None;
    }
    let rec = &leaf[rec_start..rec_end];
    if rec.len() < 8 {
        return None;
    }
    let key_len = BigEndian::read_u16(&rec[0..2]) as usize;
    if key_len < 6 || 2 + key_len > rec.len() {
        return None;
    }
    let parent_id = BigEndian::read_u32(&rec[2..6]);
    if parent_id != 1 {
        return None;
    }
    let name_length = BigEndian::read_u16(&rec[6..8]) as usize;
    if name_length == 0 {
        return None;
    }
    let body_start = 8;
    let body_end = body_start + name_length * 2;
    if body_end > rec.len() || body_end > 2 + key_len {
        return None;
    }
    let label = decode_utf16be(&rec[body_start..body_end]);
    if label.is_empty() {
        None
    } else {
        Some(label)
    }
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

/// Write a special-file B-tree fork that may have grown past its 8 inline
/// extents (§4b Phase B). Writes the inline extents first, then continues
/// into the fork's overflow extents pulled from the extents-overflow B-tree
/// — the write-side mirror of [`read_fork_with_overflow`]. Without this, a
/// grown catalog/attributes fork would be silently truncated on disk to its
/// first 8 extents even though it looks complete in memory.
#[allow(clippy::too_many_arguments)]
fn write_fork_data_with_overflow<R: Write + Seek>(
    writer: &mut R,
    partition_offset: u64,
    block_size: u32,
    fork: &ForkData,
    file_id: u32,
    fork_type: u8,
    extents_overflow_data: Option<&[u8]>,
    data: &[u8],
) -> Result<(), FilesystemError> {
    let mut written = 0usize;
    let write_extent = |writer: &mut R, ext: &ExtentDescriptor, written: &mut usize| {
        if ext.block_count == 0 || *written >= data.len() {
            return Ok(false);
        }
        let offset = partition_offset + ext.start_block as u64 * block_size as u64;
        let extent_len = ext.block_count as u64 * block_size as u64;
        let to_write = extent_len.min((data.len() - *written) as u64) as usize;
        writer.seek(SeekFrom::Start(offset))?;
        writer.write_all(&data[*written..*written + to_write])?;
        *written += to_write;
        Ok::<bool, FilesystemError>(true)
    };

    for ext in &fork.extents {
        if ext.is_empty() {
            break;
        }
        if !write_extent(writer, ext, &mut written)? {
            return Ok(());
        }
    }
    if written >= data.len() {
        return Ok(());
    }
    // Inline extents didn't cover the whole fork — continue into overflow.
    let Some(ext_data) = extents_overflow_data else {
        return Err(FilesystemError::InvalidData(format!(
            "special file {file_id} fork {fork_type:#x}: {} bytes to write but only \
             inline extents and no extents-overflow B-tree",
            data.len()
        )));
    };
    let inline_blocks: u32 = fork.extents.iter().map(|e| e.block_count).sum();
    for ext in collect_hfsplus_overflow_extents(ext_data, file_id, fork_type, inline_blocks) {
        if !write_extent(writer, &ext, &mut written)? {
            break;
        }
    }
    if written < data.len() {
        return Err(FilesystemError::InvalidData(format!(
            "special file {file_id} fork {fork_type:#x}: extents cover {written} of {} bytes",
            data.len()
        )));
    }
    Ok(())
}

/// Index of the last node in a B-tree's map-node chain (`header.fLink` →
/// mapNode → … → 0), or 0 when the chain is empty (so the caller links the
/// first map node via the header node at index 0). Cycle-guarded.
fn btree_map_chain_tail(buf: &[u8], node_size: usize) -> u32 {
    let mut next = BigEndian::read_u32(&buf[0..4]); // header.fLink
    let mut last = 0u32;
    let mut guard = 0;
    while next != 0 && guard < 1_000_000 {
        last = next;
        let off = next as usize * node_size;
        if off + 4 > buf.len() {
            break;
        }
        next = BigEndian::read_u32(&buf[off..off + 4]);
        guard += 1;
    }
    last
}

/// Decode a UTF-16BE byte slice to a String.
fn decode_utf16be(data: &[u8]) -> String {
    let chars: Vec<u16> = data.chunks_exact(2).map(BigEndian::read_u16).collect();
    String::from_utf16_lossy(&chars)
}

/// Find the volume label from the catalog B-tree (root folder thread record).
/// Count data-fork extents per file by walking catalog leaves. Cheap —
/// all catalog data already lives in `catalog_data` (loaded at FS open),
/// so this is a pure in-memory scan. Each file record's data fork holds 8
/// inline extent descriptors at offset 88+16; we tally non-zero ones and
/// flag forks that overflow into the extents-overflow B-tree as having
/// >1 extent (the most common cause of real-world fragmentation).
fn hfsplus_fragmentation_stats(
    catalog_data: &[u8],
    first_leaf: u32,
    node_size: usize,
) -> super::filesystem::FragmentationStats {
    use super::hfs_common::walk_leaf_records;
    use byteorder::{BigEndian, ByteOrder};

    let mut stats = super::filesystem::FragmentationStats::default();
    if node_size == 0 || catalog_data.is_empty() {
        return stats;
    }
    walk_leaf_records::<(), _>(
        catalog_data,
        first_leaf,
        node_size,
        |_node_idx, _rec_idx, _abs_off, rec| {
            // HFS+ catalog leaf record: u16 keyLength at offset 0, then the
            // key payload (parentID + HFSUniStr255), then the record body
            // (padded to even offset). The body's first 2 bytes carry the
            // record type — 0x0002 = file, 0x0001 = folder, threads = 3/4.
            if rec.len() < 4 {
                return None;
            }
            let key_len = BigEndian::read_u16(&rec[0..2]) as usize;
            let mut body_start = 2 + key_len;
            if !body_start.is_multiple_of(2) {
                body_start += 1;
            }
            if body_start + 88 + 80 > rec.len() {
                return None;
            }
            let body = &rec[body_start..];
            let rec_type = BigEndian::read_u16(&body[0..2]);
            if rec_type != 0x0002 {
                return None;
            }
            let data_fork = &body[88..88 + 80];
            let total_blocks = BigEndian::read_u32(&data_fork[12..16]);
            if total_blocks == 0 {
                return None;
            }
            let mut inline_extents: u32 = 0;
            let mut inline_blocks: u32 = 0;
            for i in 0..8 {
                let off = 16 + i * 8;
                let count = BigEndian::read_u32(&data_fork[off + 4..off + 8]);
                if count > 0 {
                    inline_extents += 1;
                    inline_blocks = inline_blocks.saturating_add(count);
                }
            }
            stats.files_with_data += 1;
            // Multiple inline extents OR overflow into the extents B-tree
            // both count as fragmented for UI purposes.
            if inline_extents > 1 || total_blocks > inline_blocks {
                stats.fragmented_files += 1;
            }
            None
        },
    );
    stats
}

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
///
/// MSB-first means bit position 7 of byte N (mask 0x80) corresponds to
/// global index `N*8`, and bit position 0 (mask 0x01) to global index
/// `N*8 + 7`. The "last" set bit is the one with the highest global index.
fn find_last_set_bit(bitmap: &[u8], max_bits: u32) -> Option<u32> {
    let max = max_bits as usize;
    let scan_bytes = max.div_ceil(8).min(bitmap.len());
    for byte_idx in (0..scan_bytes).rev() {
        let byte = bitmap[byte_idx];
        if byte == 0 {
            continue;
        }
        // Within the byte, the highest global index belongs to the lowest
        // bit position (LSB). Iterate bit positions 0..8 ascending and
        // return the first set one in range.
        for bit_pos in 0..8u32 {
            let global = (byte_idx as u32) * 8 + (7 - bit_pos);
            if global >= max_bits {
                continue;
            }
            if (byte >> bit_pos) & 1 == 1 {
                return Some(global);
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
        let snap = self.snapshot();
        let result = self.create_file_inner(parent, name, data, data_len, options);
        if result.is_err() {
            self.restore_snapshot(snap);
        }
        result
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let snap = self.snapshot();
        let result = self.create_directory_inner(parent, name, options);
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
        let result = self.delete_entry_inner(parent, entry);
        if result.is_err() {
            self.restore_snapshot(snap);
        }
        result
    }

    fn rename(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
        new_name: &str,
    ) -> Result<(), FilesystemError> {
        if new_name == entry.name {
            return Ok(());
        }
        validate_hfsplus_create_name(new_name)?;
        let snap = self.snapshot();
        let result = self.rename_inner(parent, entry, new_name);
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
        let result = self.set_type_creator_inner(entry, type_code, creator_code);
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
        let result = self.write_resource_fork_inner(entry, data, len);
        if result.is_err() {
            self.restore_snapshot(snap);
        }
        result
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.do_sync_metadata()
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.vh.free_blocks as u64 * self.vh.block_size as u64)
    }

    fn set_blessed_folder(&mut self, entry: &FileEntry) -> Result<(), FilesystemError> {
        let snap = self.snapshot();
        let result = self.set_blessed_folder_inner(entry);
        if result.is_err() {
            self.restore_snapshot(snap);
        }
        result
    }

    fn write_boot_blocks(&mut self, blocks: &[u8; 1024]) -> Result<(), FilesystemError> {
        self.set_boot_blocks(blocks);
        Ok(())
    }
}

impl<R: Read + Write + Seek + Send> HfsPlusFilesystem<R> {
    /// Set (or replace) an inline extended attribute on `cnid`.
    ///
    /// Any existing record keyed by `(cnid, name)` is removed first so the
    /// final state holds exactly one record per `(cnid, name)` pair. Records
    /// whose payload would not fit in a single B-tree leaf node are rejected
    /// with `XattrTooLarge` — fork-style storage is not yet wired up.
    ///
    /// Wrapped in the snapshot guard so partial failures (e.g. a leaf split
    /// running out of free B-tree nodes) leave the in-memory state byte-equal
    /// to the pre-call state.
    pub fn set_xattr(
        &mut self,
        cnid: u32,
        name: &str,
        value: &[u8],
    ) -> Result<(), FilesystemError> {
        let snap = self.snapshot();
        let result = (|| -> Result<(), FilesystemError> {
            self.remove_xattr_records(cnid, Some(name))?;
            let rec = Self::build_inline_attr_record(cnid, name, value);
            self.insert_xattr_record(&rec)?;
            if let Some(set) = self.xattr_cnids.as_mut() {
                set.insert(cnid);
            }
            Ok(())
        })();
        if result.is_err() {
            self.restore_snapshot(snap);
        }
        result
    }

    /// Remove a single extended attribute by name. Returns `NotFound` if no
    /// matching record exists. Wrapped in the snapshot guard.
    pub fn remove_xattr(&mut self, cnid: u32, name: &str) -> Result<(), FilesystemError> {
        let snap = self.snapshot();
        let result = (|| -> Result<(), FilesystemError> {
            let removed = self.remove_xattr_records(cnid, Some(name))?;
            if removed == 0 {
                return Err(FilesystemError::NotFound(format!(
                    "extended attribute '{name}' on cnid {cnid}"
                )));
            }
            // Drop the cnid from the cached set if no records remain.
            if self.list_xattrs(cnid)?.is_empty() {
                if let Some(set) = self.xattr_cnids.as_mut() {
                    set.remove(&cnid);
                }
            }
            Ok(())
        })();
        if result.is_err() {
            self.restore_snapshot(snap);
        }
        result
    }
}

/// Captured per-record metadata, replayed by [`HfsPlusFilesystem::set_record_metadata`]
/// onto a freshly-created target catalog record. Field offsets are
/// identical for HFS+ file (248-byte) and folder (88-byte) records, so the
/// same struct serves both shapes.
///
/// Used by the clone pipeline (Step 21 of `docs/hfsplus_enhancements.md`)
/// to stamp the source's dates / Finder metadata / BSD info / text
/// encoding onto each cloned entry after `create_file` /
/// `create_directory` has assigned it a new CNID.
#[derive(Debug, Clone, Default)]
pub(crate) struct RecordMetadata {
    pub create_date: u32,
    pub content_mod_date: u32,
    pub attribute_mod_date: u32,
    pub access_date: u32,
    pub backup_date: u32,
    pub bsd_info: [u8; 16],
    pub finder_info: [u8; 16],
    pub extended_finder_info: [u8; 16],
    pub text_encoding: u32,
}

impl<R: Read + Write + Seek + Send> HfsPlusFilesystem<R> {
    /// Replay captured per-record metadata onto the catalog record for
    /// `cnid`. Patches dates, BSD info, FInfo / FXInfo (or DInfo / DXInfo
    /// for folders — same offsets), and textEncoding. The locked-bit and
    /// flags-word fields are intentionally untouched here so callers can
    /// stamp them via dedicated paths (none today; the source's `flags` is
    /// already mirrored by `create_file` / `create_directory` for the
    /// fields the clone cares about).
    ///
    /// Mostly used by the clone replay path; tests cover the round-trip
    /// in `clone_round_trip_*`.
    pub(crate) fn set_record_metadata(
        &mut self,
        cnid: u32,
        meta: &RecordMetadata,
    ) -> Result<(), FilesystemError> {
        // Resolve the catalog record body via the thread record.
        let (_, _, t_offset) = self.find_catalog_record_by_cnid(cnid).ok_or_else(|| {
            FilesystemError::NotFound(format!("thread record for CNID {cnid} not found"))
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

        let (_, _, f_offset) = self
            .find_catalog_record(thread_parent, &thread_name)
            .ok_or_else(|| {
                FilesystemError::NotFound(format!(
                    "catalog record for CNID {cnid} ('{thread_name}') not found"
                ))
            })?;
        let fkey_len = BigEndian::read_u16(&self.catalog_data[f_offset..f_offset + 2]) as usize;
        let mut frec = f_offset + 2 + fkey_len;
        if !frec.is_multiple_of(2) {
            frec += 1;
        }
        if frec + 84 > self.catalog_data.len() {
            return Err(FilesystemError::InvalidData(format!(
                "catalog record for CNID {cnid} truncated"
            )));
        }

        BigEndian::write_u32(
            &mut self.catalog_data[frec + 12..frec + 16],
            meta.create_date,
        );
        BigEndian::write_u32(
            &mut self.catalog_data[frec + 16..frec + 20],
            meta.content_mod_date,
        );
        BigEndian::write_u32(
            &mut self.catalog_data[frec + 20..frec + 24],
            meta.attribute_mod_date,
        );
        BigEndian::write_u32(
            &mut self.catalog_data[frec + 24..frec + 28],
            meta.access_date,
        );
        BigEndian::write_u32(
            &mut self.catalog_data[frec + 28..frec + 32],
            meta.backup_date,
        );
        self.catalog_data[frec + 32..frec + 48].copy_from_slice(&meta.bsd_info);
        self.catalog_data[frec + 48..frec + 64].copy_from_slice(&meta.finder_info);
        self.catalog_data[frec + 64..frec + 80].copy_from_slice(&meta.extended_finder_info);
        BigEndian::write_u32(
            &mut self.catalog_data[frec + 80..frec + 84],
            meta.text_encoding,
        );
        Ok(())
    }

    /// Set the volume's create / modify / backup / checked dates. The
    /// `modify_date` is overwritten by `do_sync_metadata` with `hfs_now()`
    /// at sync time, so callers that want the source's modifyDate to
    /// survive should call this *after* the final mutation (i.e. as the
    /// last step before sync) — but for the clone pipeline the modifyDate
    /// is intentionally bumped to "now" anyway, so the input value here is
    /// a fallback used only if sync doesn't run.
    pub(crate) fn set_volume_dates(
        &mut self,
        create_date: u32,
        modify_date: u32,
        backup_date: u32,
        checked_date: u32,
    ) {
        self.vh.create_date = create_date;
        self.vh.modify_date = modify_date;
        self.vh.backup_date = backup_date;
        self.vh.checked_date = checked_date;
    }

    /// Replace the volume header's 8 × u32 Finder Info slots verbatim. The
    /// caller is responsible for any CNID remap (slots 0/3/5 etc. may hold
    /// CNIDs that point at the now-renumbered blessed folder / OS folder).
    pub(crate) fn set_volume_finder_info(&mut self, finder_info: [u32; 8]) {
        self.vh.finder_info = finder_info;
    }

    /// Stage the source volume's first 1024 bytes (boot blocks) for write
    /// at the next `sync_metadata`. Macs only ever care about the boot
    /// blocks on the System volume, but the area is part of the volume's
    /// signature, so the clone copies it verbatim for round-trip parity.
    pub(crate) fn set_boot_blocks(&mut self, blocks: &[u8; 1024]) {
        self.pending_boot_blocks = Some(Box::new(*blocks));
    }
}

impl<R: Read + Write + Seek + Send> HfsPlusFilesystem<R> {
    /// Locate the `\0\0\0\0HFS+ Private Data` directory at root, creating
    /// it if missing. Returns its `FileEntry`. The directory's existence is
    /// the prerequisite for any hardlink-related write — both
    /// [`Self::create_hardlink_inode`] and [`Self::create_hardlink`] call
    /// this internally so the replay path doesn't need to.
    pub fn ensure_private_dir(&mut self) -> Result<FileEntry, FilesystemError> {
        let private_name = hfsplus_private_dir_name();
        let root = self.root()?;
        for entry in self.list_directory(&root)? {
            if entry.is_directory() && entry.name == private_name {
                return Ok(entry);
            }
        }
        self.create_directory(&root, &private_name, &CreateDirectoryOptions::default())
    }

    /// Locate the `.HFS+ Private Directory Data\r` directory at root,
    /// creating it if missing. Companion to [`Self::ensure_private_dir`]
    /// for directory hardlinks.
    pub fn ensure_dir_private_dir(&mut self) -> Result<FileEntry, FilesystemError> {
        let dir_private_name = hfsplus_dir_private_dir_name();
        let root = self.root()?;
        for entry in self.list_directory(&root)? {
            if entry.is_directory() && entry.name == dir_private_name {
                return Ok(entry);
            }
        }
        self.create_directory(&root, &dir_private_name, &CreateDirectoryOptions::default())
    }

    /// Create a hardlink inode (`iNode<inode_num>`) under the volume's
    /// private directory. The inode's data and resource forks hold the
    /// shared payload; its `bsdInfo.special` (linkCount) starts at zero
    /// and is bumped per-link by [`Self::create_hardlink`]. The caller is
    /// responsible for calling `sync_metadata` once all replay edits are
    /// staged.
    ///
    /// Returns an `AlreadyExists` error if `iNode<inode_num>` already lives
    /// in the private dir, so callers don't accidentally double-emit.
    pub fn create_hardlink_inode(
        &mut self,
        inode_num: u32,
        data: &mut dyn std::io::Read,
        data_len: u64,
        options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let snap = self.snapshot();
        let result = (|| -> Result<FileEntry, FilesystemError> {
            let private = self.ensure_private_dir()?;
            let inode_name = format!("iNode{inode_num}");
            self.create_file(&private, &inode_name, data, data_len, options)
        })();
        if result.is_err() {
            self.restore_snapshot(snap);
        }
        result
    }

    /// Create a file hardlink stub at `(parent, name)` referencing the
    /// inode `iNode<inode_num>` under the private dir. The stub carries
    /// empty forks and FInfo `('hlnk', 'hfs+')` with
    /// `bsdInfo.special = inode_num`. The matching inode's linkCount
    /// (`bsdInfo.special`) is bumped by 1.
    ///
    /// Returns the stub's `FileEntry` with `link_target_cnid` set to the
    /// inode's CNID — callers that want the inode bytes can read through
    /// it directly via [`Self::read_file`] / [`Self::write_file_to`].
    pub fn create_hardlink(
        &mut self,
        parent: &FileEntry,
        name: &str,
        inode_num: u32,
    ) -> Result<FileEntry, FilesystemError> {
        let snap = self.snapshot();
        let result = (|| -> Result<FileEntry, FilesystemError> {
            let private_cnid = self.find_private_dir_cnid()?.ok_or_else(|| {
                FilesystemError::NotFound(
                    "HFS+ Private Data directory missing — call create_hardlink_inode first".into(),
                )
            })?;
            // Locate the inode and confirm it exists; bump its linkCount.
            let inode_name = format!("iNode{inode_num}");
            let inode_cnid = self
                .list_children(private_cnid)?
                .into_iter()
                .find_map(|c| match c {
                    CatalogEntry::File {
                        file_id, name: n, ..
                    } if n == inode_name => Some(file_id),
                    _ => None,
                })
                .ok_or_else(|| {
                    FilesystemError::NotFound(format!(
                        "hardlink inode '{inode_name}' missing — create it before linking"
                    ))
                })?;

            // Create the stub as an empty file with type/creator overrides.
            let mut empty: &[u8] = &[];
            let stub = self.create_file(
                parent,
                name,
                &mut empty,
                0,
                &CreateFileOptions {
                    os_type: Some(*b"hlnk"),
                    os_creator: Some(*b"hfs+"),
                    ..Default::default()
                },
            )?;

            // Patch bsdInfo.special on the stub to hold the iNodeNum.
            let parent_cnid = parent.location as u32;
            let (_, _, abs) = self.find_catalog_record(parent_cnid, name).ok_or_else(|| {
                FilesystemError::NotFound(format!(
                    "freshly-created stub '{name}' missing from catalog"
                ))
            })?;
            let key_len = BigEndian::read_u16(&self.catalog_data[abs..abs + 2]) as usize;
            let mut body = abs + 2 + key_len;
            if !body.is_multiple_of(2) {
                body += 1;
            }
            BigEndian::write_u32(&mut self.catalog_data[body + 44..body + 48], inode_num);

            // Bump linkCount on the inode (saturates if it overflows).
            self.adjust_inode_link_count(private_cnid, inode_num, 1)?;

            // Drop the cached inode_num -> CNID map so the next hardlink
            // resolution sees the new inode without picking up stale state.
            self.hardlink_inode_map = None;

            // Surface the inode CNID on the returned entry so callers can
            // inspect it without re-walking the catalog.
            let mut fe = stub;
            fe.link_target_cnid = Some(inode_cnid as u64);
            Ok(fe)
        })();
        if result.is_err() {
            self.restore_snapshot(snap);
        }
        result
    }

    /// Create a directory-hardlink inode (`dir_<inode_num>`) under the
    /// volume's directory-private directory. The inode is a regular folder
    /// — callers populate its contents like any other directory. Its
    /// `bsdInfo.special` (linkCount) starts at zero and is bumped per stub
    /// by [`Self::create_dir_hardlink`].
    pub fn create_dir_hardlink_inode(
        &mut self,
        inode_num: u32,
        options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let snap = self.snapshot();
        let result = (|| -> Result<FileEntry, FilesystemError> {
            let private = self.ensure_dir_private_dir()?;
            let inode_name = format!("dir_{inode_num}");
            self.create_directory(&private, &inode_name, options)
        })();
        if result.is_err() {
            self.restore_snapshot(snap);
        }
        result
    }

    /// Create a directory-hardlink stub at `(parent, name)` referencing
    /// `dir_<inode_num>` under the directory-private directory. The stub
    /// is a FILE record (not a folder) with FInfo `('fdrp', 'MACS')` and
    /// `bsdInfo.special = inode_num`; `list_directory` surfaces it as a
    /// `Directory` entry that follows the link automatically. The matching
    /// inode's linkCount (`bsdInfo.special`) is bumped by 1.
    pub fn create_dir_hardlink(
        &mut self,
        parent: &FileEntry,
        name: &str,
        inode_num: u32,
    ) -> Result<FileEntry, FilesystemError> {
        let snap = self.snapshot();
        let result = (|| -> Result<FileEntry, FilesystemError> {
            let dir_private_cnid = self.find_dir_private_dir_cnid()?.ok_or_else(|| {
                FilesystemError::NotFound(
                    ".HFS+ Private Directory Data missing — call create_dir_hardlink_inode first"
                        .into(),
                )
            })?;
            let inode_name = format!("dir_{inode_num}");
            let inode_cnid = self
                .list_children(dir_private_cnid)?
                .into_iter()
                .find_map(|c| match c {
                    CatalogEntry::Folder { folder_id, name: n } if n == inode_name => {
                        Some(folder_id)
                    }
                    _ => None,
                })
                .ok_or_else(|| {
                    FilesystemError::NotFound(format!(
                        "directory inode '{inode_name}' missing — create it before linking"
                    ))
                })?;

            let mut empty: &[u8] = &[];
            let stub = self.create_file(
                parent,
                name,
                &mut empty,
                0,
                &CreateFileOptions {
                    os_type: Some(*b"fdrp"),
                    os_creator: Some(*b"MACS"),
                    ..Default::default()
                },
            )?;

            // Patch bsdInfo.special on the stub to hold the iNodeNum.
            let parent_cnid = parent.location as u32;
            let (_, _, abs) = self.find_catalog_record(parent_cnid, name).ok_or_else(|| {
                FilesystemError::NotFound(format!(
                    "freshly-created stub '{name}' missing from catalog"
                ))
            })?;
            let key_len = BigEndian::read_u16(&self.catalog_data[abs..abs + 2]) as usize;
            let mut body = abs + 2 + key_len;
            if !body.is_multiple_of(2) {
                body += 1;
            }
            BigEndian::write_u32(&mut self.catalog_data[body + 44..body + 48], inode_num);

            // Bump linkCount on the dir-inode's folder record (lives at
            // body+44..48 of the folder record too — DInfo doesn't overlap).
            self.adjust_dir_inode_link_count(dir_private_cnid, inode_num, 1)?;

            // Drop the cached map so the next lookup sees the new inode.
            self.dir_hardlink_inode_map = None;

            // Surface the inode CNID on the returned entry; flip its type
            // to Directory so callers treat it as one.
            let mut fe = stub;
            fe.entry_type = crate::fs::entry::EntryType::Directory;
            fe.size = 0;
            fe.link_target_cnid = Some(inode_cnid as u64);
            Ok(fe)
        })();
        if result.is_err() {
            self.restore_snapshot(snap);
        }
        result
    }

    /// Adjust the directory-inode `linkCount` (`bsdInfo.special` u32 — at
    /// folder-record body offset 32+12 = 44, same as file records) by
    /// `delta`. Mirrors `adjust_inode_link_count` for file inodes.
    fn adjust_dir_inode_link_count(
        &mut self,
        dir_private_cnid: u32,
        inode_num: u32,
        delta: i64,
    ) -> Result<u32, FilesystemError> {
        let inode_name = format!("dir_{inode_num}");
        let (_, _, abs_off) = self
            .find_catalog_record(dir_private_cnid, &inode_name)
            .ok_or_else(|| {
                FilesystemError::InvalidData(format!(
                    "directory inode '{inode_name}' missing from .HFS+ Private Directory Data"
                ))
            })?;
        let key_len = BigEndian::read_u16(&self.catalog_data[abs_off..abs_off + 2]) as usize;
        let mut body_start = abs_off + 2 + key_len;
        if !body_start.is_multiple_of(2) {
            body_start += 1;
        }
        let cur = BigEndian::read_u32(&self.catalog_data[body_start + 44..body_start + 48]) as i64;
        let new = (cur + delta).max(0) as u32;
        BigEndian::write_u32(
            &mut self.catalog_data[body_start + 44..body_start + 48],
            new,
        );
        Ok(new)
    }

    fn create_file_inner(
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
        // Raw `os_type`/`os_creator` win (byte-exact, high-bit safe); else
        // fall back to the lossy text `type_code`, then the extension dict.
        let type_code = options.os_type.unwrap_or_else(|| {
            options
                .type_code
                .as_deref()
                .map(hfs_common::encode_fourcc)
                .unwrap_or(dict_t)
        });
        let creator_code = options.os_creator.unwrap_or_else(|| {
            options
                .creator_code
                .as_deref()
                .map(hfs_common::encode_fourcc)
                .unwrap_or(dict_c)
        });

        // Allocate blocks and write data
        let data_fork = self.write_data_to_blocks(data, data_len, file_id, HFSPLUS_FORK_DATA)?;

        // Handle resource fork
        let rsrc_fork = if let Some(ref rsrc_src) = options.resource_fork {
            match rsrc_src {
                super::filesystem::ResourceForkSource::Data(rsrc_data) => {
                    let mut cursor = std::io::Cursor::new(rsrc_data);
                    self.write_data_to_blocks(
                        &mut cursor,
                        rsrc_data.len() as u64,
                        file_id,
                        HFSPLUS_FORK_RESOURCE,
                    )?
                }
                super::filesystem::ResourceForkSource::File(path) => {
                    let mut f = std::fs::File::open(path)?;
                    let len = f.metadata()?.len();
                    self.write_data_to_blocks(&mut f, len, file_id, HFSPLUS_FORK_RESOURCE)?
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
        if type_code != [0; 4] {
            fe.type_code = Some(type_code);
            fe.creator_code = Some(creator_code);
        }
        if rsrc_fork.logical_size > 0 {
            fe.resource_fork_size = Some(rsrc_fork.logical_size);
        }
        Ok(fe)
    }

    fn create_directory_inner(
        &mut self,
        parent: &FileEntry,
        name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let parent_cnid = parent.location as u32;

        validate_hfsplus_create_name(name)?;

        // Diagnostic: refuse to create a second `\0\0\0\0HFS+ Private Data`
        // or `.HFS+ Private Directory Data\r` directory at the volume root.
        // The hardlink subsystem is the only legitimate creator of these
        // magic-named directories (via `ensure_private_dir`), and it walks
        // `list_directory(root)` to detect an existing one before falling
        // back to create. If `find_catalog_record` later misses it (e.g.
        // due to a key-compare quirk), the resulting duplicate is the bug
        // observed in HFSTest6 (2026-05). Belt-and-suspenders: in addition
        // to the find_catalog_record check below, scan root by CNID-aware
        // lookup so neither a name-fold mismatch nor a misplaced record
        // slips through. See docs/hfs_write_tweaks.md.
        if parent_cnid == 2
            && (name == hfsplus_private_dir_name() || name == hfsplus_dir_private_dir_name())
        {
            let already_present = self
                .find_private_dir_cnid()?
                .filter(|_| name == hfsplus_private_dir_name())
                .is_some()
                || self
                    .find_dir_private_dir_cnid()?
                    .filter(|_| name == hfsplus_dir_private_dir_name())
                    .is_some();
            if already_present {
                let bt = std::backtrace::Backtrace::force_capture();
                log::error!(
                    "[HFS+] refused create_directory for magic name (already \
                     present). parent_cnid={}, leading NULs={}\nbacktrace:\n{bt}",
                    parent_cnid,
                    name.chars().take_while(|c| *c == '\0').count()
                );
                return Err(FilesystemError::AlreadyExists(name.into()));
            }
        }

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

    fn delete_entry_inner(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        let parent_cnid = parent.location as u32;
        let cnid = entry.location as u32;

        // Drop every attribute record attached to this CNID so we don't leave
        // dangling rows in the attributes B-tree pointing at a freed CNID.
        // No-op on volumes without an attributes file. Errors here trip the
        // snapshot rollback in `delete_entry`.
        let xattrs_removed = self.remove_xattr_records(cnid, None)?;
        if xattrs_removed > 0 {
            if let Some(set) = self.xattr_cnids.as_mut() {
                set.remove(&cnid);
            }
        }

        // Check directory is empty
        if entry.is_directory() {
            let children = self.list_children(cnid)?;
            if !children.is_empty() {
                return Err(FilesystemError::InvalidData(
                    "cannot delete non-empty directory".into(),
                ));
            }
        }

        // File hardlink stub? If so, decrement the inode's linkCount before
        // we touch the stub's catalog row. Last reference also frees the
        // inode's forks and removes its iNode<N> entry from the private
        // directory.
        let link_inode_num = if entry.is_directory() {
            None
        } else {
            self.read_file_record_link_inode_num(parent_cnid, &entry.name)?
        };
        if let Some(inode_num) = link_inode_num {
            self.delete_hardlink_inode_ref(inode_num)?;
        }

        // Find and remove the entry record
        let (node_idx, rec_idx, _offset) = self
            .find_catalog_record(parent_cnid, &entry.name)
            .ok_or_else(|| {
                FilesystemError::NotFound(format!("entry '{}' not found in catalog", entry.name))
            })?;

        // If it's a file, free its data and resource fork blocks (including
        // any overflow extents) and remove its overflow records. Hardlink
        // stubs have empty forks (the data lives on the inode we already
        // adjusted above), so this is a no-op for them.
        if !entry.is_directory() {
            if let Some((data_fork, rsrc_fork)) = self.find_file_by_id(cnid) {
                self.free_fork_overflow_extents(cnid, HFSPLUS_FORK_DATA, &data_fork);
                self.free_fork_overflow_extents(cnid, HFSPLUS_FORK_RESOURCE, &rsrc_fork);
                self.remove_extents_overflow_records_for(cnid, HFSPLUS_FORK_DATA);
                self.remove_extents_overflow_records_for(cnid, HFSPLUS_FORK_RESOURCE);
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

    fn rename_inner(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
        new_name: &str,
    ) -> Result<(), FilesystemError> {
        let parent_cnid = parent.location as u32;
        let cnid = entry.location as u32;

        // Locate the entry's catalog record by its current (parent, name) key.
        let (node_idx, rec_idx, _abs) = self
            .find_catalog_record(parent_cnid, &entry.name)
            .ok_or_else(|| {
                FilesystemError::NotFound(format!("entry '{}' not found in catalog", entry.name))
            })?;

        // Reject a collision only with a DIFFERENT record. On a case-insensitive
        // volume a case-only rename folds back to this same record (allowed).
        if let Some((n2, r2, _)) = self.find_catalog_record(parent_cnid, new_name) {
            if (n2, r2) != (node_idx, rec_idx) {
                return Err(FilesystemError::AlreadyExists(new_name.to_string()));
            }
        }

        // Extract the record's data body to re-emit it verbatim under the new
        // key (CNID, forks, Finder info, BSD info all preserved).
        let node_size = self.catalog_node_size();
        let node_off = node_idx as usize * node_size;
        let node = &self.catalog_data[node_off..node_off + node_size];
        let (rec_start, rec_end) = super::hfs_common::btree_record_range(node, node_size, rec_idx);
        let key_len = BigEndian::read_u16(&node[rec_start..rec_start + 2]) as usize;
        let mut data_rel = 2 + key_len;
        if !data_rel.is_multiple_of(2) {
            data_rel += 1;
        }
        let data_bytes = node[rec_start + data_rel..rec_end].to_vec();

        let mut new_record = Self::build_catalog_key(parent_cnid, new_name);
        if new_record.len() % 2 != 0 {
            new_record.push(0);
        }
        new_record.extend_from_slice(&data_bytes);

        self.remove_catalog_record(node_idx, rec_idx);
        self.insert_catalog_record(&new_record)?;

        // The thread record's name is a variable-length UTF-16 string, so its
        // footprint changes with the new name — rebuild it (remove + reinsert)
        // rather than patching in place. Folders always have a thread; files
        // generally do.
        if let Some((t_node, t_rec, _)) = self.find_catalog_record_by_cnid(cnid) {
            self.remove_catalog_record(t_node, t_rec);
            let thread_type = if entry.is_directory() {
                CATALOG_FOLDER_THREAD
            } else {
                CATALOG_FILE_THREAD
            };
            let (_k, thread_data) = Self::build_thread_record(thread_type, parent_cnid, new_name);
            let mut thread_record = Self::build_catalog_key(cnid, "");
            if thread_record.len() % 2 != 0 {
                thread_record.push(0);
            }
            thread_record.extend_from_slice(&thread_data);
            self.insert_catalog_record(&thread_record)?;
        }
        // A same-parent rename leaves parent valence and VH counts unchanged.
        Ok(())
    }

    /// Adjust the inode-record `linkCount` (`bsdInfo.special`, body offset
    /// 44..48) by `delta` and return the post-update count. Locates the
    /// record by `(private_dir_cnid, "iNode<N>")` so the in-memory edit
    /// hits the live catalog leaf. Touches `attributeModDate` so users can
    /// see when the inode was last referenced.
    fn adjust_inode_link_count(
        &mut self,
        private_cnid: u32,
        inode_num: u32,
        delta: i64,
    ) -> Result<u32, FilesystemError> {
        let inode_name = format!("iNode{inode_num}");
        let (_, _, abs_off) = self
            .find_catalog_record(private_cnid, &inode_name)
            .ok_or_else(|| {
                FilesystemError::InvalidData(format!(
                    "hardlink inode '{inode_name}' missing from HFS+ Private Data"
                ))
            })?;
        let key_len = BigEndian::read_u16(&self.catalog_data[abs_off..abs_off + 2]) as usize;
        let mut body_start = abs_off + 2 + key_len;
        if !body_start.is_multiple_of(2) {
            body_start += 1;
        }
        let cur = BigEndian::read_u32(&self.catalog_data[body_start + 44..body_start + 48]) as i64;
        let new = (cur + delta).max(0) as u32;
        BigEndian::write_u32(
            &mut self.catalog_data[body_start + 44..body_start + 48],
            new,
        );
        self.stamp_record_dates(body_start, false);
        Ok(new)
    }

    /// Decrement `linkCount` on the inode named `iNode<inode_num>` under the
    /// volume's `HFS+ Private Data` dir. When the count drops to zero, free
    /// the inode's forks, drop its xattrs, and remove the inode catalog row
    /// plus thread plus map entry. Bumps `vh.file_count` and the private
    /// dir's valence accordingly.
    fn delete_hardlink_inode_ref(&mut self, inode_num: u32) -> Result<(), FilesystemError> {
        let private_cnid = self.find_private_dir_cnid()?.ok_or_else(|| {
            FilesystemError::InvalidData(
                "hardlink stub on volume with no HFS+ Private Data directory".into(),
            )
        })?;
        let inode_cnid = self.resolve_hardlink_inode(inode_num)?.ok_or_else(|| {
            FilesystemError::InvalidData(format!(
                "hardlink target iNode{inode_num} missing from HFS+ Private Data"
            ))
        })?;

        let new_count = self.adjust_inode_link_count(private_cnid, inode_num, -1)?;
        if new_count > 0 {
            return Ok(());
        }

        // Last reference — free the inode's forks and drop its rows.
        if let Some((data_fork, rsrc_fork)) = self.find_file_by_id(inode_cnid) {
            self.free_fork_overflow_extents(inode_cnid, HFSPLUS_FORK_DATA, &data_fork);
            self.free_fork_overflow_extents(inode_cnid, HFSPLUS_FORK_RESOURCE, &rsrc_fork);
            self.remove_extents_overflow_records_for(inode_cnid, HFSPLUS_FORK_DATA);
            self.remove_extents_overflow_records_for(inode_cnid, HFSPLUS_FORK_RESOURCE);
            self.free_fork_blocks(&data_fork);
            self.free_fork_blocks(&rsrc_fork);
        }

        let xattrs_removed = self.remove_xattr_records(inode_cnid, None)?;
        if xattrs_removed > 0 {
            if let Some(set) = self.xattr_cnids.as_mut() {
                set.remove(&inode_cnid);
            }
        }

        let inode_name = format!("iNode{inode_num}");
        if let Some((n, r, _)) = self.find_catalog_record(private_cnid, &inode_name) {
            self.remove_catalog_record(n, r);
        }
        if let Some((tn, tr, _)) = self.find_catalog_record_by_cnid(inode_cnid) {
            self.remove_catalog_record(tn, tr);
        }
        self.update_parent_valence(private_cnid, -1)?;
        self.vh.file_count = self.vh.file_count.saturating_sub(1);

        if let Some(map) = self.hardlink_inode_map.as_mut() {
            map.remove(&inode_num);
        }
        Ok(())
    }

    fn set_type_creator_inner(
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

        // Type/creator is an attribute-only change.
        self.stamp_record_dates(frec_start, false);

        let _ = (f_node, f_rec, node_size); // suppress warnings
        Ok(())
    }

    fn write_resource_fork_inner(
        &mut self,
        entry: &FileEntry,
        data: &mut dyn std::io::Read,
        len: u64,
    ) -> Result<(), FilesystemError> {
        let cnid = entry.location as u32;

        // Free existing resource fork blocks (inline + any overflow extents).
        if let Some((_data_fork, rsrc_fork)) = self.find_file_by_id(cnid) {
            self.free_fork_overflow_extents(cnid, HFSPLUS_FORK_RESOURCE, &rsrc_fork);
            self.remove_extents_overflow_records_for(cnid, HFSPLUS_FORK_RESOURCE);
            self.free_fork_blocks(&rsrc_fork);
        }

        // Allocate and write new resource fork data
        let new_rsrc = self.write_data_to_blocks(data, len, cnid, HFSPLUS_FORK_RESOURCE)?;

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

        // Fork rewrite is a content change, so contentModDate also moves.
        self.stamp_record_dates(frec_start, true);

        let _ = t_node;
        Ok(())
    }

    fn set_blessed_folder_inner(&mut self, entry: &FileEntry) -> Result<(), FilesystemError> {
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
/// Streaming reader over a single user fork. Built by
/// [`HfsPlusFilesystem::fork_stream_reader`] with all inline + overflow
/// extents resolved up front; `read` walks the extent list, seeking the
/// underlying source reader as it crosses extent boundaries. Bounded
/// strictly by the fork's `logical_size` — the trailing partial block is
/// truncated at the byte level rather than the block level.
pub struct ForkStreamReader<'a, R: Read + Seek> {
    reader: &'a mut R,
    /// `(device_offset, byte_length)` for each extent, in fork order.
    extents: Vec<(u64, u64)>,
    /// Total bytes the reader is allowed to emit (`fork.logical_size`).
    logical_size: u64,
    /// Bytes already produced.
    pos: u64,
    cur_extent: usize,
    /// Bytes already consumed from the current extent.
    cur_extent_consumed: u64,
}

impl<'a, R: Read + Seek> Read for ForkStreamReader<'a, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.logical_size || buf.is_empty() {
            return Ok(0);
        }
        // Skip past extents whose bytes are fully consumed.
        while self.cur_extent < self.extents.len() {
            let (_, len) = self.extents[self.cur_extent];
            if self.cur_extent_consumed < len {
                break;
            }
            self.cur_extent += 1;
            self.cur_extent_consumed = 0;
        }
        if self.cur_extent >= self.extents.len() {
            return Ok(0);
        }
        let (off, len) = self.extents[self.cur_extent];
        let remaining_in_extent = len - self.cur_extent_consumed;
        let remaining_logical = self.logical_size - self.pos;
        let max_chunk = remaining_in_extent.min(remaining_logical);
        let to_read = (buf.len() as u64).min(max_chunk) as usize;
        self.reader
            .seek(SeekFrom::Start(off + self.cur_extent_consumed))?;
        let n = self.reader.read(&mut buf[..to_read])?;
        if n == 0 {
            return Ok(0);
        }
        self.cur_extent_consumed += n as u64;
        self.pos += n as u64;
        Ok(n)
    }
}

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

        // Find the longest run starting at the current block whose
        // allocation state matches the current block's, then service as
        // much of that run as fits in `buf`. Coalescing turns a 24 GiB
        // partition's ~6M per-4-KiB-block syscalls into far fewer reads
        // (typically one per file extent), critical on slow source media
        // where each seek + read costs milliseconds.
        let allocated = self.is_block_allocated(self.current_block);
        let mut run_end = self.current_block + 1;
        while run_end < self.total_blocks && self.is_block_allocated(run_end) == allocated {
            run_end += 1;
        }

        let bytes_in_run = (run_end - self.current_block) as u64 * block_size - self.block_pos;
        let to_read = (bytes_in_run as usize).min(buf.len());

        let n = if allocated {
            let offset =
                self.partition_offset + self.current_block as u64 * block_size + self.block_pos;
            self.reader.seek(SeekFrom::Start(offset))?;
            self.reader.read(&mut buf[..to_read])?
        } else {
            // Unallocated run — emit zeros so free space compresses to nothing.
            buf[..to_read].fill(0);
            to_read
        };

        // Advance cursor by however many bytes we actually produced.
        let mut remaining_in_n = n as u64;
        // First, finish the partial block we may have been mid-way through.
        if self.block_pos > 0 {
            let want = block_size - self.block_pos;
            let take = remaining_in_n.min(want);
            self.block_pos += take;
            remaining_in_n -= take;
            if self.block_pos >= block_size {
                self.block_pos = 0;
                self.current_block += 1;
            }
        }
        // Then jump whole blocks.
        let whole = (remaining_in_n / block_size) as u32;
        self.current_block += whole;
        remaining_in_n -= whole as u64 * block_size;
        // Finally the trailing partial block.
        if remaining_in_n > 0 {
            self.block_pos = remaining_in_n;
        }

        Ok(n)
    }
}

// --- Blank-volume builder ---

/// Build a freshly-formatted, mountable HFS+ (or HFSX) image as a raw byte
/// buffer.
///
/// Layout (sized for `block_size` ≤ `node_size`; node_size is fixed at
/// 4096 to match Apple's default):
///
/// - Block 0: VH region (1024 boot bytes + primary VH at byte 1024)
/// - Bitmap blocks: enough to hold `total_blocks / 8` bytes
/// - Extents-overflow B-tree: 4 nodes (header + empty leaf + 2 free)
/// - Catalog B-tree: 4 nodes (header + leaf with root folder + thread)
/// - Free user-data area
/// - Last 1024 bytes: alt VH (mirror of primary)
///
/// The catalog leaf carries two records: the root folder thread keyed by
/// `(parent=2, name="")` (data: `recordType=3`, `parentID=1`, nodeName =
/// `name`) and the root folder record keyed by `(parent=1, name=name)`
/// (data: `recordType=1`, `folderID=2`, valence=0, dates set to now).
///
/// `case_sensitive=true` produces an HFSX volume (signature `H+`/`HX`,
/// catalog header `keyCompareType = kHFSBinaryCompare`); `false` produces
/// plain HFS+ with case-folding compare. The journaled bit is never set —
/// edit mode refuses journaled volumes (Step 4) so a freshly built blank
/// must opt out.
///
/// Panics if `size_bytes` is too small to hold the reserved region or if
/// `block_size` isn't a power of 2 in `[512, 4096]`.
pub fn create_blank_hfsplus(
    size_bytes: u64,
    block_size: u32,
    name: &str,
    case_sensitive: bool,
) -> Vec<u8> {
    create_blank_hfsplus_sized(size_bytes, block_size, name, case_sensitive, 0, 0)
}

/// Volume-scaled default catalog B-tree size, in bytes, for a fresh HFS+
/// volume. Mirrors classic HFS's `default_btree_sizes`: ~0.5% of the volume so
/// a blank holds thousands of records before exhausting its node budget (HFS+
/// has no grow-on-full path — docs/hfsplus_btree_growth_plan.md §4c). The
/// `build_blank_hfsplus_front` caller clamps the result to whole nodes in
/// `[4, header-bitmap capacity]`.
pub fn default_hfsplus_catalog_bytes(volume_bytes: u64) -> u64 {
    volume_bytes / 200
}

/// Variant of [`create_blank_hfsplus`] that lets the caller request minimum
/// catalog and extents-overflow B-tree sizes (in bytes) — e.g. a clone target
/// that must hold every record from a fragmented source, or a test that needs a
/// deep catalog without a multi-GiB volume. Each minimum is rounded up to whole
/// nodes and raised to the 4-node floor; the catalog also never drops below the
/// volume-scaled [`default_hfsplus_catalog_bytes`].
pub fn create_blank_hfsplus_sized(
    size_bytes: u64,
    block_size: u32,
    name: &str,
    case_sensitive: bool,
    min_catalog_bytes: u32,
    min_extents_bytes: u32,
) -> Vec<u8> {
    let (front, vh_bytes, image_size) = build_blank_hfsplus_front(
        size_bytes,
        block_size,
        name,
        case_sensitive,
        min_catalog_bytes,
        min_extents_bytes,
    );
    let mut img = vec![0u8; image_size];
    img[..front.len()].copy_from_slice(&front);
    let alt = image_size - 1024;
    img[alt..alt + 512].copy_from_slice(&vh_bytes);
    img
}

/// Build only the front (boot/VH/bitmap/B-trees) region and the 512-byte
/// VH bytes. Returns `(front_bytes, vh_bytes, total_image_size)`. Used by
/// both [`create_blank_hfsplus`] (full Vec) and
/// [`write_blank_hfsplus_into`] (sparse-file streaming).
fn build_blank_hfsplus_front(
    size_bytes: u64,
    block_size: u32,
    name: &str,
    case_sensitive: bool,
    min_catalog_bytes: u32,
    min_extents_bytes: u32,
) -> (Vec<u8>, [u8; 512], usize) {
    assert!(
        block_size.is_power_of_two() && (512..=4096).contains(&block_size),
        "block_size must be a power of 2 in [512, 4096], got {block_size}"
    );
    let bs = block_size as u64;
    let total_blocks = (size_bytes / bs) as u32;
    assert!(total_blocks >= 64, "image too small ({size_bytes} bytes)");

    // node_size pinned at 4096 — Apple's default and what every existing
    // helper in this file assumes (compute_record_offsets, etc.).
    let node_size: usize = 4096;
    let nodes_per_block: u32 = (node_size as u32 / block_size).max(1);
    let blocks_per_node: u32 = (block_size / node_size as u32).max(1);
    let _ = nodes_per_block;

    // Size the catalog B-tree from the volume (docs/hfsplus_btree_growth_plan.md
    // §4c): HFS+ has no grow-on-full path, so a blank that reserved only the
    // legacy 4 nodes exhausted after ~24 files. Scale it to ~0.5% of the volume
    // (mirroring classic HFS's `default_btree_sizes`) so a fresh volume holds
    // thousands of records, honouring an explicit `min_catalog_bytes` floor from
    // sized/clone callers. The extents-overflow tree keeps the 4-node default
    // unless a caller asks for more (its own scaling is P3 work).
    //
    // `to_nodes` rounds a byte budget up to whole nodes and clamps to
    // [4, header-bitmap capacity]. The blank's node-allocation bitmap lives
    // entirely in the header node's record 2 (bytes 248..node_size-8 — see
    // `write_blank_btree_header_node`), which addresses `(node_size - 256) * 8`
    // nodes without dedicated map nodes; cap there. Volumes whose 0.5% catalog
    // would exceed this (~123 MiB at 4096, i.e. hundreds of GiB of volume) are
    // far larger than this tool targets, and the defrag/clone path — which does
    // build map nodes — covers them. (The §4b live-grow path appends map nodes
    // incrementally past this cap; see `grow_btree_fork`.)
    let max_btree_nodes: u32 = ((node_size - 256) * 8) as u32;
    let to_nodes = |bytes: u64| -> u32 {
        bytes
            .div_ceil(node_size as u64)
            .clamp(4, max_btree_nodes as u64) as u32
    };
    let catalog_node_count =
        to_nodes(default_hfsplus_catalog_bytes(size_bytes).max(min_catalog_bytes as u64));
    let extents_node_count = to_nodes(min_extents_bytes as u64);

    let catalog_btree_blocks: u32 = catalog_node_count * blocks_per_node;
    let extents_btree_blocks: u32 = extents_node_count * blocks_per_node;

    let bitmap_bytes = total_blocks.div_ceil(8) as u64;
    let bitmap_blocks = bitmap_bytes.div_ceil(bs) as u32;

    let bitmap_start: u32 = 1;
    let extents_start: u32 = bitmap_start + bitmap_blocks;
    let catalog_start: u32 = extents_start + extents_btree_blocks;
    let reserved_blocks: u32 = catalog_start + catalog_btree_blocks;
    assert!(
        reserved_blocks + 1 < total_blocks,
        "image too small for reserved region: need {} blocks, have {total_blocks}",
        reserved_blocks + 1
    );

    let image_size = total_blocks as usize * block_size as usize;
    // Build only the front (boot/VH/bitmap/B-trees) and the 512-byte alt VH
    // separately, so multi-GB blanks don't have to materialise a full image
    // in RAM. The middle of the volume is free space — sparse-file zeroes
    // are good enough.
    let front_size = reserved_blocks as usize * block_size as usize;
    let mut front = vec![0u8; front_size];

    // The alternate volume header occupies the last 1024 bytes of the volume.
    // Apple always marks the trailing allocation block(s) it touches as used
    // (one block for block_size >= 1024, two for 512-byte blocks) and excludes
    // them from the free-block count. Without this, fsck_hfs reports
    // "Invalid volume free block count" / bitmap under-allocation.
    let alt_vh_start_block: u32 = ((image_size as u64 - 1024) / bs) as u32;
    let trailing_blocks: u32 = total_blocks - alt_vh_start_block;

    // --- Allocation bitmap ---
    {
        let bitmap_off = bitmap_start as usize * block_size as usize;
        let bitmap = &mut front[bitmap_off..bitmap_off + (bitmap_bytes as usize).max(1)];
        let mut mark = |blk: u32| {
            // MSB-first big-endian: bit position (7 - blk%8) of byte (blk/8).
            let byte_idx = (blk / 8) as usize;
            let bit_pos = 7 - (blk % 8);
            bitmap[byte_idx] |= 1 << bit_pos;
        };
        for blk in 0..reserved_blocks {
            mark(blk);
        }
        for blk in alt_vh_start_block..total_blocks {
            mark(blk);
        }
    }

    // --- Extents-overflow B-tree (empty: header node only) ---
    // An empty tree is depth-0 / root-0 with no leaf node, exactly as Apple's
    // newfs_hfs lays it out (see `write_blank_btree_header_node`). The first
    // fragmented-file insert bootstraps a root leaf via `btree_insert_full`.
    {
        let off = extents_start as usize * block_size as usize;
        write_blank_btree_header_node(
            &mut front[off..off + node_size],
            node_size,
            /* leaf_records= */ 0,
            /* total_nodes= */ extents_node_count,
            /* free_nodes= */ extents_node_count - 1, // only the header node is used
            /* max_key_len= */ 10,
            /* key_compare_type= */ 0, // unused for extents
        );
    }

    // --- Catalog B-tree (header + leaf with root + thread) ---
    let name_utf16: Vec<u16> = super::hfs_unicode::decompose_str(name);
    {
        let off = catalog_start as usize * block_size as usize;
        let key_compare = if case_sensitive {
            KEY_COMPARE_BINARY
        } else {
            KEY_COMPARE_CASE_FOLDING
        };
        write_blank_btree_header_node(
            &mut front[off..off + node_size],
            node_size,
            /* leaf_records= */ 2,
            catalog_node_count,
            catalog_node_count - 2,
            /* max_key_len= */ 516, // HFS+ catalog max key
            key_compare,
        );
        let leaf_off = off + node_size;
        write_root_catalog_leaf(&mut front[leaf_off..leaf_off + node_size], &name_utf16);
    }

    // --- Volume header ---
    let signature = if case_sensitive {
        HFSX_SIGNATURE
    } else {
        HFS_PLUS_SIGNATURE
    };
    let now = hfs_common::hfs_now();
    let vh = HfsPlusVolumeHeader {
        signature,
        version: if case_sensitive { 5 } else { 4 },
        attributes: HFSPLUS_VOLUME_UNMOUNTED_BIT,
        last_mounted_version: 0,
        journal_info_block: 0,
        create_date: now,
        modify_date: now,
        backup_date: 0,
        checked_date: now,
        file_count: 0,
        // Per Apple TN1150, folder_count EXCLUDES the root directory.
        // Matches what macOS / Disk Utility / fsck_hfs expect on a fresh
        // volume; mismatching this causes false-positive
        // FolderCountMismatch reports when fsck'ing volumes created
        // elsewhere and edited here.
        folder_count: 0,
        block_size,
        total_blocks,
        free_blocks: total_blocks - reserved_blocks - trailing_blocks,
        next_allocation: reserved_blocks,
        rsrc_clump_size: block_size,
        data_clump_size: block_size,
        next_catalog_id: 16,
        write_count: 0,
        encodings_bitmap: 1, // bit 0 = MacRoman, conventional default
        finder_info: [0u32; 8],
        allocation_file: ForkData {
            logical_size: bitmap_blocks as u64 * bs,
            clump_size: 0,
            total_blocks: bitmap_blocks,
            extents: extent_array(bitmap_start, bitmap_blocks),
        },
        extents_file: ForkData {
            logical_size: extents_btree_blocks as u64 * bs,
            clump_size: 0,
            total_blocks: extents_btree_blocks,
            extents: extent_array(extents_start, extents_btree_blocks),
        },
        catalog_file: ForkData {
            logical_size: catalog_btree_blocks as u64 * bs,
            clump_size: 0,
            total_blocks: catalog_btree_blocks,
            extents: extent_array(catalog_start, catalog_btree_blocks),
        },
        attributes_file: ForkData::empty(),
        startup_file: ForkData::empty(),
    };
    let vh_bytes = vh.serialize();

    // Primary VH at offset 1024 (always inside the front region).
    front[1024..1536].copy_from_slice(&vh_bytes);

    (front, vh_bytes, image_size)
}

/// Streaming variant of [`create_blank_hfsplus`]. Writes the metadata
/// regions (boot, VH, bitmap, B-trees, alt-VH) into `target` and leaves
/// the free middle untouched. Caller is responsible for sizing the
/// underlying file to `size_bytes` (e.g. via `File::set_len`) so reads of
/// the free area return zeros.
///
/// Used by the defrag-clone backup path so a 38 GiB blank target doesn't
/// need 38 GiB of contiguous RAM. Only the front region (~2 MiB worst
/// case) plus the 512-byte alt VH is held in memory.
pub fn write_blank_hfsplus_into<W: Write + Seek>(
    target: &mut W,
    size_bytes: u64,
    block_size: u32,
    name: &str,
    case_sensitive: bool,
) -> std::io::Result<()> {
    let (front, vh_bytes, _image_size) =
        build_blank_hfsplus_front(size_bytes, block_size, name, case_sensitive, 0, 0);
    target.seek(SeekFrom::Start(0))?;
    target.write_all(&front)?;
    target.seek(SeekFrom::Start(size_bytes - 1024))?;
    target.write_all(&vh_bytes)?;
    target.write_all(&[0u8; 512])?;
    Ok(())
}

/// Helper for `create_blank_hfsplus`: build a single-extent inline array.
fn extent_array(start: u32, count: u32) -> [ExtentDescriptor; 8] {
    let mut e = [ExtentDescriptor {
        start_block: 0,
        block_count: 0,
    }; 8];
    e[0] = ExtentDescriptor {
        start_block: start,
        block_count: count,
    };
    e
}

/// Stamp a B-tree header node into `node` (must be exactly `node_size`
/// bytes). Lays out three records — header(106), user-data(128), node
/// bitmap (rest) — and marks node 0 (header) and node 1 (leaf) as
/// allocated in the node bitmap.
pub(crate) fn write_blank_btree_header_node(
    node: &mut [u8],
    node_size: usize,
    leaf_records: u32,
    total_nodes: u32,
    free_nodes: u32,
    max_key_len: u16,
    key_compare_type: u8,
) {
    node.fill(0);
    node[8] = 1; // kind = header
    BigEndian::write_u16(&mut node[10..12], 3); // 3 records

    // Record 0: BTHeaderRec at offset 14 (106 bytes).
    //
    // An *empty* tree (no leaf records, e.g. a fresh extents-overflow or
    // attributes B-tree) is represented exactly as Apple's `newfs_hfs` does:
    // treeDepth = 0, rootNode = 0, firstLeafNode = lastLeafNode = 0, and no
    // leaf node allocated — only the header node is in use. A populated blank
    // (the catalog, with its root folder + thread records) has a single
    // root *leaf* at node 1. `fsck_hfs` rejects a depth-1 tree whose root is
    // an empty leaf ("Invalid node structure"), which is why the empty case
    // must collapse to depth 0. `btree_insert_full` bootstraps the first leaf
    // on the initial insert into a depth-0 tree.
    let empty = leaf_records == 0;
    let (depth, root_first_last): (u16, u32) = if empty { (0, 0) } else { (1, 1) };
    let hr = 14usize;
    BigEndian::write_u16(&mut node[hr..hr + 2], depth);
    BigEndian::write_u32(&mut node[hr + 2..hr + 6], root_first_last); // root
    BigEndian::write_u32(&mut node[hr + 6..hr + 10], leaf_records);
    BigEndian::write_u32(&mut node[hr + 10..hr + 14], root_first_last); // first leaf
    BigEndian::write_u32(&mut node[hr + 14..hr + 18], root_first_last); // last leaf
    BigEndian::write_u16(&mut node[hr + 18..hr + 20], node_size as u16);
    BigEndian::write_u16(&mut node[hr + 20..hr + 22], max_key_len);
    BigEndian::write_u32(&mut node[hr + 22..hr + 26], total_nodes);
    BigEndian::write_u32(&mut node[hr + 26..hr + 30], free_nodes);
    // BTHeaderRec offset 36 = btreeType (0=control); offset 37 = keyCompareType
    node[hr + 36] = 0; // kBTHFSTreeType
    node[hr + 37] = key_compare_type;

    // Canonical TN1150 B-tree header node layout. The BTHeaderRec (record 0)
    // is *exactly* 106 bytes — Apple's `fsck_hfs` rejects any other length
    // ("Invalid BTH length"). So record 1 (the 128-byte reserved user-data
    // record) starts at 14 + 106 = 120, and record 2 (the node-allocation
    // bitmap) starts at 120 + 128 = 248, filling the node to the offset
    // table. The bitmap therefore addresses (node_size - 256) * 8 nodes,
    // which is exactly what `hfs_common::map_nodes_required` assumes — so the
    // header-vs-map-node boundary is consistent (see the discrepancy noted in
    // docs/todo_hfsplus_fork_growth.md §5, resolved by using 248 here).
    let user_off: u16 = 120;
    let bitmap_off: u16 = 248;
    let free_off: u16 = (node_size as u16).saturating_sub(8); // fills to offset table

    let ot_end = node_size;
    BigEndian::write_u16(&mut node[ot_end - 2..ot_end], 14); // record 0
    BigEndian::write_u16(&mut node[ot_end - 4..ot_end - 2], user_off);
    BigEndian::write_u16(&mut node[ot_end - 6..ot_end - 4], bitmap_off);
    BigEndian::write_u16(&mut node[ot_end - 8..ot_end - 6], free_off);

    // Mark allocated nodes in the bitmap: node 0 (header) always; node 1
    // (the root leaf) only for a populated tree. An empty tree's node 1 is
    // free and zero-filled, matching Apple.
    node[bitmap_off as usize] = if empty { 0b10000000 } else { 0b11000000 };
}

/// Write an empty leaf node (kind=-1, height=1, 0 records). Only used by test
/// fixtures now — the production blank/defrag builders represent an empty tree
/// as depth 0 with no leaf node (the first insert bootstraps the root leaf).
#[cfg(test)]
pub(crate) fn write_empty_leaf_node(node: &mut [u8]) {
    node.fill(0);
    node[8] = 0xFF; // kind = -1
    node[9] = 1; // height
    BigEndian::write_u16(&mut node[10..12], 0); // 0 records
    let n = node.len();
    BigEndian::write_u16(&mut node[n - 2..n], 14); // free-space offset
}

/// Write the catalog leaf node containing the root folder record (key =
/// `(parent=1, name=<volume>)`) and the root thread record (key =
/// `(parent=2, name="")`, data = recordType=3 + parentID=1 + name=volume).
fn write_root_catalog_leaf(node: &mut [u8], name_utf16: &[u16]) {
    node.fill(0);
    let n = node.len();
    node[8] = 0xFF; // kind = -1 leaf
    node[9] = 1; // height
    BigEndian::write_u16(&mut node[10..12], 2); // 2 records

    let now = hfs_common::hfs_now();

    // ---- Record 0: root folder record ----
    // Key: key_len(2) + parent_id(4) + name_len(2) + name(UTF-16BE)
    let r0_off = 14usize;
    let key_body = 4 + 2 + name_utf16.len() * 2;
    BigEndian::write_u16(&mut node[r0_off..r0_off + 2], key_body as u16);
    BigEndian::write_u32(&mut node[r0_off + 2..r0_off + 6], 1); // parent = 1
    BigEndian::write_u16(&mut node[r0_off + 6..r0_off + 8], name_utf16.len() as u16);
    for (i, &u) in name_utf16.iter().enumerate() {
        BigEndian::write_u16(&mut node[r0_off + 8 + i * 2..r0_off + 10 + i * 2], u);
    }
    let mut r0_data = r0_off + 2 + key_body;
    if !r0_data.is_multiple_of(2) {
        r0_data += 1;
    }
    BigEndian::write_i16(&mut node[r0_data..r0_data + 2], CATALOG_FOLDER);
    // valence = 0 at +4, folderID = 2 at +8, then dates.
    BigEndian::write_u32(&mut node[r0_data + 8..r0_data + 12], 2);
    BigEndian::write_u32(&mut node[r0_data + 12..r0_data + 16], now);
    BigEndian::write_u32(&mut node[r0_data + 16..r0_data + 20], now);
    BigEndian::write_u32(&mut node[r0_data + 20..r0_data + 24], now);
    BigEndian::write_u32(&mut node[r0_data + 24..r0_data + 28], now);
    let r0_end = r0_data + 88; // folder record is 88 bytes total

    // ---- Record 1: root thread record ----
    // Key: key_len(2) + parent_id(4=2) + name_len(2=0)
    let r1_off = r0_end;
    BigEndian::write_u16(&mut node[r1_off..r1_off + 2], 6); // key_len
    BigEndian::write_u32(&mut node[r1_off + 2..r1_off + 6], 2); // parent = 2 (root CNID)
    BigEndian::write_u16(&mut node[r1_off + 6..r1_off + 8], 0); // name_len = 0
    let mut r1_data = r1_off + 2 + 6;
    if !r1_data.is_multiple_of(2) {
        r1_data += 1;
    }
    // Thread data: type(2)=3, reserved(2)=0, parentID(4)=1, name_len(2),
    // name(UTF-16BE).
    BigEndian::write_i16(&mut node[r1_data..r1_data + 2], 3); // folder thread
    BigEndian::write_u32(&mut node[r1_data + 4..r1_data + 8], 1);
    BigEndian::write_u16(
        &mut node[r1_data + 8..r1_data + 10],
        name_utf16.len() as u16,
    );
    for (i, &u) in name_utf16.iter().enumerate() {
        BigEndian::write_u16(&mut node[r1_data + 10 + i * 2..r1_data + 12 + i * 2], u);
    }
    let r1_end = r1_data + 10 + name_utf16.len() * 2;

    // Offset table at the end of the node.
    BigEndian::write_u16(&mut node[n - 2..n], 14); // record 0 offset
    BigEndian::write_u16(&mut node[n - 4..n - 2], r1_off as u16);
    BigEndian::write_u16(&mut node[n - 6..n - 4], r1_end as u16); // free-space offset
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
#[allow(clippy::identity_op)] // `1usize * block_size` keeps offset rows aligned
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
    fn test_find_last_set_bit_dense_byte() {
        // Regression: when the highest non-zero byte has multiple set bits,
        // the result must be the highest global index in that byte, not the
        // lowest. 0b11111100 has bits 0..=5 set (MSB-first), so the answer
        // is 5, not 0.
        assert_eq!(find_last_set_bit(&[0b11111100], 8), Some(5));
        assert_eq!(find_last_set_bit(&[0b11111111], 8), Some(7));
        assert_eq!(find_last_set_bit(&[0b00000001], 8), Some(7));
        assert_eq!(find_last_set_bit(&[0b10000000], 8), Some(0));
        // Multi-byte: byte 0 dense, byte 1 zero — answer is in byte 0.
        assert_eq!(find_last_set_bit(&[0b11111100, 0b00000000], 16), Some(5));
        // Bound clipping: 0b11111111 with max_bits=4 → only bits 0..=3 are
        // valid; the highest-valid one is 3.
        assert_eq!(find_last_set_bit(&[0b11111111], 4), Some(3));
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
        let alloc_blocks = 1 + 1 + catalog_blocks; // VH + bitmap + catalog = 6 blocks
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
            folder_count: 0, // Apple TN1150: excludes root
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

    /// Build an editable HFS+ image that also carries a 4-node empty
    /// extents-overflow B-tree at blocks 6..9. Used by tests that exercise
    /// fragmented files needing more than 8 extents (Step 7).
    ///
    /// Layout (4 KiB blocks, 256 total = 1 MiB image):
    /// - Block 0: VH/boot region
    /// - Block 1: allocation bitmap
    /// - Blocks 2..5: catalog B-tree (4 nodes)
    /// - Blocks 6..9: extents-overflow B-tree (4 nodes — header + empty leaf
    ///   + 2 free)
    /// - Blocks 10..255: free user data area
    fn make_editable_hfsplus_image_with_extents_btree() -> Vec<u8> {
        // Start from the standard editable image, then carve in an
        // extents-overflow B-tree.
        let mut img = make_editable_hfsplus_image();
        let block_size = 4096u32;
        let node_size = 4096usize;
        let extents_start_block = 6u32;
        let extents_blocks = 4u32;

        // Mark blocks 6..9 allocated in the bitmap (byte 0 was 0b11111100 →
        // blocks 0..=5 allocated; flip bits 1..=3 of byte 1 to allocate
        // blocks 6..=9, leaving bits 4..=7 free for user data continuation).
        let bitmap_off = 1usize * block_size as usize;
        img[bitmap_off] = 0xFF; // blocks 0..=7 allocated
        img[bitmap_off + 1] = 0b11000000; // blocks 8..=9 allocated

        // --- Header node (block 6, node 0) ---
        let hdr_off = extents_start_block as usize * block_size as usize;
        img[hdr_off + 8] = 1; // kind = header
        BigEndian::write_u16(&mut img[hdr_off + 10..hdr_off + 12], 3); // 3 records

        // B-tree header record at offset 14
        let hr = hdr_off + 14;
        BigEndian::write_u16(&mut img[hr..hr + 2], 1); // depth = 1
        BigEndian::write_u32(&mut img[hr + 2..hr + 6], 1); // root_node = 1
        BigEndian::write_u32(&mut img[hr + 6..hr + 10], 0); // leaf_records = 0
        BigEndian::write_u32(&mut img[hr + 10..hr + 14], 1); // first_leaf = 1
        BigEndian::write_u32(&mut img[hr + 14..hr + 18], 1); // last_leaf = 1
        BigEndian::write_u16(&mut img[hr + 18..hr + 20], node_size as u16);
        BigEndian::write_u16(&mut img[hr + 20..hr + 22], 10); // max_key_len
        BigEndian::write_u32(&mut img[hr + 22..hr + 26], 4); // total_nodes
        BigEndian::write_u32(&mut img[hr + 26..hr + 30], 2); // free_nodes (2,3)

        // Offset table at end of node 0: header(14), user(142), bitmap(270),
        // free(526) — same shape as the catalog header node.
        let ot = hdr_off + node_size;
        BigEndian::write_u16(&mut img[ot - 2..ot], 14);
        BigEndian::write_u16(&mut img[ot - 4..ot - 2], 142);
        BigEndian::write_u16(&mut img[ot - 6..ot - 4], 270);
        BigEndian::write_u16(&mut img[ot - 8..ot - 6], 526);

        // Node bitmap (record 2): mark nodes 0 and 1 allocated.
        img[hdr_off + 270] = 0b11000000;

        // --- Empty leaf node (block 7, node 1) ---
        let leaf_off = hdr_off + node_size;
        img[leaf_off + 8] = 0xFF; // kind = -1 (leaf)
        img[leaf_off + 9] = 1; // height = 1
        BigEndian::write_u16(&mut img[leaf_off + 10..leaf_off + 12], 0); // 0 records

        // Offset table: just the free-space offset at byte 14 (start of
        // record area).
        let lot = leaf_off + node_size;
        BigEndian::write_u16(&mut img[lot - 2..lot], 14);

        // Patch the volume header: point extents_file at blocks 6..9 with
        // a 16 KiB logical size, and decrement free_blocks by 4.
        // VH lives at offset 1024; offset 4..8 is `attributes`, but
        // extents_file fork lives at VH bytes 192..272.
        // Easier path: re-parse the VH, patch the fields, re-serialize.
        let mut vh = HfsPlusVolumeHeader::parse(&img[1024..1536]).expect("parse seed VH");
        vh.extents_file = ForkData {
            logical_size: (extents_blocks as u64) * (block_size as u64),
            clump_size: 0,
            total_blocks: extents_blocks,
            extents: {
                let mut e = [ExtentDescriptor {
                    start_block: 0,
                    block_count: 0,
                }; 8];
                e[0] = ExtentDescriptor {
                    start_block: extents_start_block,
                    block_count: extents_blocks,
                };
                e
            },
        };
        vh.free_blocks -= extents_blocks;
        vh.next_allocation = 10;
        let vh_bytes = vh.serialize();
        img[1024..1536].copy_from_slice(&vh_bytes);
        let alt = img.len() - 1024;
        img[alt..alt + 512].copy_from_slice(&vh_bytes);
        img
    }

    /// Build an editable HFS+ image whose attributes B-tree carries one inline
    /// `com.apple.FinderInfo` record for `cnid` with `value` as the payload.
    /// Used by the Step 12 round-trip test.
    ///
    /// Layout (4 KiB blocks, 256 total = 1 MiB image):
    /// - Block 0: VH/boot region
    /// - Block 1: allocation bitmap
    /// - Blocks 2-5: catalog B-tree
    /// - Blocks 6-7: attributes B-tree (header + leaf)
    /// - Blocks 8..255: free
    fn make_editable_hfsplus_image_with_inline_xattr(cnid: u32, value: &[u8]) -> Vec<u8> {
        let mut img = make_editable_hfsplus_image();
        let block_size = 4096u32;
        let node_size = 4096usize;
        let attr_start_block = 6u32;
        let attr_blocks = 2u32;

        // Mark blocks 6 and 7 allocated. Seed image had byte 0 = 0b11111100
        // (blocks 0..=5); set bits 6..=7 of byte 0.
        let bitmap_off = 1usize * block_size as usize;
        img[bitmap_off] = 0xFF; // blocks 0..=7 allocated

        // --- Header node (block 6, node 0) ---
        let hdr_off = attr_start_block as usize * block_size as usize;
        img[hdr_off + 8] = 1; // kind = header
        BigEndian::write_u16(&mut img[hdr_off + 10..hdr_off + 12], 3);

        let hr = hdr_off + 14;
        BigEndian::write_u16(&mut img[hr..hr + 2], 1); // depth = 1
        BigEndian::write_u32(&mut img[hr + 2..hr + 6], 1); // root_node = 1
        BigEndian::write_u32(&mut img[hr + 6..hr + 10], 1); // leaf_records = 1
        BigEndian::write_u32(&mut img[hr + 10..hr + 14], 1); // first_leaf = 1
        BigEndian::write_u32(&mut img[hr + 14..hr + 18], 1); // last_leaf = 1
        BigEndian::write_u16(&mut img[hr + 18..hr + 20], node_size as u16);
        BigEndian::write_u16(&mut img[hr + 20..hr + 22], 512); // max_key_len
        BigEndian::write_u32(&mut img[hr + 22..hr + 26], attr_blocks); // total_nodes = 2

        // Offset table for header node (header(14) / user(142) / bitmap(270) /
        // free(526) — same shape as the catalog header node).
        let ot = hdr_off + node_size;
        BigEndian::write_u16(&mut img[ot - 2..ot], 14);
        BigEndian::write_u16(&mut img[ot - 4..ot - 2], 142);
        BigEndian::write_u16(&mut img[ot - 6..ot - 4], 270);
        BigEndian::write_u16(&mut img[ot - 8..ot - 6], 526);

        // Node bitmap (record 2): mark nodes 0 and 1 allocated.
        img[hdr_off + 270] = 0b11000000;

        // --- Leaf node (block 7, node 1) carrying one inline xattr record ---
        let leaf_off = hdr_off + node_size;
        img[leaf_off + 8] = 0xFF; // kind = -1 (leaf)
        img[leaf_off + 9] = 1; // height = 1
        BigEndian::write_u16(&mut img[leaf_off + 10..leaf_off + 12], 1); // 1 record

        // Build the record: key + record-data.
        // Key body: pad(2) + fileID(4) + startBlock(4) + nameLen(2) + name UTF-16BE
        let name = "com.apple.FinderInfo";
        let name_utf16: Vec<u16> = name.encode_utf16().collect();
        let name_byte_len = name_utf16.len() * 2;
        let key_body_len = 12 + name_byte_len;
        // recordType(4) + reserved[2](8) + attrSize(4) + attrData(value.len())
        let data_section_len = 4 + 8 + 4 + value.len();
        let rec_total = 2 + key_body_len + data_section_len;

        let r0 = leaf_off + 14;
        BigEndian::write_u16(&mut img[r0..r0 + 2], key_body_len as u16);
        // pad already zero
        BigEndian::write_u32(&mut img[r0 + 4..r0 + 8], cnid);
        BigEndian::write_u32(&mut img[r0 + 8..r0 + 12], 0); // startBlock = 0
        BigEndian::write_u16(&mut img[r0 + 12..r0 + 14], name_utf16.len() as u16);
        for (i, ch) in name_utf16.iter().enumerate() {
            BigEndian::write_u16(&mut img[r0 + 14 + i * 2..r0 + 14 + i * 2 + 2], *ch);
        }
        let data_off = r0 + 2 + key_body_len;
        BigEndian::write_u32(&mut img[data_off..data_off + 4], HFSPLUS_ATTR_INLINE_DATA);
        // reserved[2] (8 bytes) already zero
        BigEndian::write_u32(&mut img[data_off + 12..data_off + 16], value.len() as u32);
        img[data_off + 16..data_off + 16 + value.len()].copy_from_slice(value);

        // Offset table: record 0 at 14, free-space at 14 + rec_total.
        let lot = leaf_off + node_size;
        BigEndian::write_u16(&mut img[lot - 2..lot], 14);
        BigEndian::write_u16(&mut img[lot - 4..lot - 2], (14 + rec_total) as u16);

        // Patch the volume header: point attributes_file at blocks 6..7,
        // decrement free_blocks by 2.
        let mut vh = HfsPlusVolumeHeader::parse(&img[1024..1536]).expect("parse seed VH");
        vh.attributes_file = ForkData {
            logical_size: (attr_blocks as u64) * (block_size as u64),
            clump_size: 0,
            total_blocks: attr_blocks,
            extents: {
                let mut e = [ExtentDescriptor {
                    start_block: 0,
                    block_count: 0,
                }; 8];
                e[0] = ExtentDescriptor {
                    start_block: attr_start_block,
                    block_count: attr_blocks,
                };
                e
            },
        };
        vh.free_blocks -= attr_blocks;
        vh.next_allocation = 8;
        let vh_bytes = vh.serialize();
        img[1024..1536].copy_from_slice(&vh_bytes);
        let alt = img.len() - 1024;
        img[alt..alt + 512].copy_from_slice(&vh_bytes);
        img
    }

    /// Build a minimal HFS+ image whose first leaf node holds the root folder
    /// record at `(parent=1, name=<volume_name>)` followed by N synthetic
    /// "child" records keyed by `(parent=2, name=childN)` — pushing the root
    /// thread record `(parent=2, name="")` into a later leaf. This mirrors
    /// the failure mode `probe_hfsplus_volume_label` had on real volumes
    /// where many top-level entries existed under the root.
    fn make_hfsplus_image_with_label(volume_name: &str, num_children: usize) -> Vec<u8> {
        let block_size = 4096u32;
        let total_blocks = 256u32;
        let image_size = total_blocks as usize * block_size as usize;
        let mut img = vec![0u8; image_size];

        // Layout: block 0 (VH region), block 1 (bitmap), blocks 2..=5 (catalog, 4 nodes).
        let bitmap_block = 1u32;
        let catalog_start_block = 2u32;
        let catalog_blocks = 4u32;
        // Bitmap byte 0 = 0b11111100 (blocks 0..=5 allocated).
        img[bitmap_block as usize * block_size as usize] = 0b11111100;

        let node_size = 4096usize;
        let catalog_offset = catalog_start_block as usize * block_size as usize;

        // Node 0: header.
        let hdr = catalog_offset;
        img[hdr + 8] = 1; // kind = header
        BigEndian::write_u16(&mut img[hdr + 10..hdr + 12], 3);
        let hr = hdr + 14;
        BigEndian::write_u16(&mut img[hr..hr + 2], 1); // depth
        BigEndian::write_u32(&mut img[hr + 2..hr + 6], 1); // root_node = 1 (first leaf)
        BigEndian::write_u32(&mut img[hr + 6..hr + 10], (1 + num_children + 1) as u32);
        BigEndian::write_u32(&mut img[hr + 10..hr + 14], 1); // first_leaf
                                                             // Probe only ever reads the first leaf, so last_leaf can be anything ≥ 1.
        BigEndian::write_u32(&mut img[hr + 14..hr + 18], 2);
        BigEndian::write_u16(&mut img[hr + 18..hr + 20], node_size as u16);
        BigEndian::write_u16(&mut img[hr + 20..hr + 22], 516);
        BigEndian::write_u32(&mut img[hr + 22..hr + 26], 4);
        BigEndian::write_u32(&mut img[hr + 26..hr + 30], 0);
        // Header offset table.
        let ot = hdr + node_size;
        BigEndian::write_u16(&mut img[ot - 2..ot], 14); // header rec
        BigEndian::write_u16(&mut img[ot - 4..ot - 2], 142); // user data
        BigEndian::write_u16(&mut img[ot - 6..ot - 4], 270); // bitmap
        BigEndian::write_u16(&mut img[ot - 8..ot - 6], 526); // free space

        // Node 1: first leaf. Record 0 = root folder record at
        // (parent=1, name=<volume_name>). The probe reads the volume name
        // from this record's key alone.
        let leaf = catalog_offset + node_size;
        img[leaf + 8] = 0xFF; // kind = -1 (leaf)
        img[leaf + 9] = 1; // height
        let total_recs = 1 + num_children;
        BigEndian::write_u16(&mut img[leaf + 10..leaf + 12], total_recs as u16);

        // Record 0: root folder record key `(parent=1, name_len=N, name)` +
        // a 88-byte folder data payload.
        let name_units: Vec<u16> = volume_name.encode_utf16().collect();
        let name_bytes = name_units.len() * 2;
        let r0 = leaf + 14;
        let key_len = 6 + name_bytes; // parent(4) + name_len(2) + name
        BigEndian::write_u16(&mut img[r0..r0 + 2], key_len as u16);
        BigEndian::write_u32(&mut img[r0 + 2..r0 + 6], 1); // parent = 1
        BigEndian::write_u16(&mut img[r0 + 6..r0 + 8], name_units.len() as u16);
        for (i, u) in name_units.iter().enumerate() {
            BigEndian::write_u16(&mut img[r0 + 8 + i * 2..r0 + 10 + i * 2], *u);
        }
        let r0_data = r0 + 2 + key_len + ((2 + key_len) % 2); // 2-byte align
        BigEndian::write_i16(&mut img[r0_data..r0_data + 2], CATALOG_FOLDER);
        BigEndian::write_u32(&mut img[r0_data + 8..r0_data + 12], 2); // folderID

        // Records 1..=N: synthetic child records keyed by (parent=2, name="cN")
        // so the root thread (parent=2, name="") gets pushed past the first leaf.
        // Using the smallest plausible record (we don't actually open the FS
        // here, just probe), so payload size doesn't matter beyond bounds.
        let mut rec_offsets = vec![14u16];
        let mut cursor = r0_data + 88;
        for i in 0..num_children {
            let child_name = format!("c{i}");
            let cn_units: Vec<u16> = child_name.encode_utf16().collect();
            let key_len = 6 + cn_units.len() * 2;
            let off = cursor;
            BigEndian::write_u16(&mut img[off..off + 2], key_len as u16);
            BigEndian::write_u32(&mut img[off + 2..off + 6], 2);
            BigEndian::write_u16(&mut img[off + 6..off + 8], cn_units.len() as u16);
            for (j, u) in cn_units.iter().enumerate() {
                BigEndian::write_u16(&mut img[off + 8 + j * 2..off + 10 + j * 2], *u);
            }
            // 2-byte aligned data offset, minimal 4-byte folder-record stub
            // (the probe never inspects it).
            let data = off + 2 + key_len + ((2 + key_len) % 2);
            BigEndian::write_i16(&mut img[data..data + 2], CATALOG_FOLDER);
            rec_offsets.push((off - leaf) as u16);
            cursor = data + 88;
        }

        // Offset table at end of leaf, MSB-first record-offset slots.
        let lot = leaf + node_size;
        for (i, off) in rec_offsets.iter().enumerate() {
            BigEndian::write_u16(&mut img[lot - 2 - i * 2..lot - i * 2], *off);
        }
        BigEndian::write_u16(
            &mut img[lot - 2 - rec_offsets.len() * 2..lot - rec_offsets.len() * 2],
            (cursor - leaf) as u16,
        );

        // Volume header.
        let vh = HfsPlusVolumeHeader {
            signature: HFS_PLUS_SIGNATURE,
            version: 4,
            attributes: 0,
            last_mounted_version: 0,
            journal_info_block: 0,
            create_date: 0,
            modify_date: 0,
            backup_date: 0,
            checked_date: 0,
            file_count: 0,
            folder_count: num_children as u32,
            block_size,
            total_blocks,
            free_blocks: total_blocks - 6,
            next_allocation: 6,
            rsrc_clump_size: 0,
            data_clump_size: 0,
            next_catalog_id: (16 + num_children) as u32,
            write_count: 0,
            encodings_bitmap: 0,
            finder_info: [0; 8],
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
                logical_size: (catalog_blocks as u64) * (block_size as u64),
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
        let alt = image_size - 1024;
        img[alt..alt + 512].copy_from_slice(&vh_bytes);
        img
    }

    #[test]
    fn test_probe_hfsplus_volume_label_reads_from_root_key() {
        // Volume name lives in the root folder record's key, even when the
        // first leaf is dominated by `parent=2` child records that push the
        // root thread out of the first leaf. This is the regression case
        // that broke the probe on real volumes like Ariel-backup.
        let img = make_hfsplus_image_with_label("Ariel-backup", 20);
        let mut cursor = std::io::Cursor::new(img);
        let label = probe_hfsplus_volume_label(&mut cursor, 0);
        assert_eq!(label.as_deref(), Some("Ariel-backup"));
    }

    #[test]
    fn test_probe_hfsplus_volume_label_short_name() {
        let img = make_hfsplus_image_with_label("X", 0);
        let mut cursor = std::io::Cursor::new(img);
        assert_eq!(
            probe_hfsplus_volume_label(&mut cursor, 0).as_deref(),
            Some("X")
        );
    }

    #[test]
    fn test_prepare_for_edit_refuses_journaled_volume_without_valid_journal() {
        // Set the journaled bit but provide no valid journal (journalInfoBlock
        // points at zeroed boot blocks). Since we can't verify the journal is
        // clean, `prepare_for_edit` refuses; a plain read-only `open` still
        // succeeds. (Clean journaled volumes — verified empty — are accepted;
        // see hfsplus_fsck::tests::prepare_for_edit_allows_clean_journaled_volume.)
        let mut img = make_editable_hfsplus_image();
        let attr_off = 1024 + 4; // VH offset 4 = attributes (u32 BE)
        let mut attrs = BigEndian::read_u32(&img[attr_off..attr_off + 4]);
        attrs |= HFSPLUS_VOLUME_JOURNALED_BIT;
        BigEndian::write_u32(&mut img[attr_off..attr_off + 4], attrs);

        let cursor = std::io::Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).expect("read-only open should succeed");
        let err = fs
            .prepare_for_edit()
            .expect_err("journaled volume with no valid journal must refuse edit prep");
        match err {
            FilesystemError::Unsupported(msg) => assert!(msg.contains("journal")),
            other => panic!("expected Unsupported(journal...), got {other:?}"),
        }
    }

    #[test]
    fn test_can_defrag_clone_accepts_clean_volume() {
        let img = make_editable_hfsplus_image();
        let mut cursor = std::io::Cursor::new(img);
        super::super::can_defrag_clone_hfsplus(&mut cursor, 0)
            .expect("plain unjournaled HFS+ must be accepted");
    }

    #[test]
    fn test_can_defrag_clone_accepts_clean_journaled_volume() {
        // Journaled bit set + unmounted bit set — clean. The on-disk
        // journal is empty, so the catalog is authoritative.
        let mut img = make_editable_hfsplus_image();
        let attr_off = 1024 + 4;
        let mut attrs = BigEndian::read_u32(&img[attr_off..attr_off + 4]);
        attrs |= HFSPLUS_VOLUME_JOURNALED_BIT | HFSPLUS_VOLUME_UNMOUNTED_BIT;
        BigEndian::write_u32(&mut img[attr_off..attr_off + 4], attrs);
        let mut cursor = std::io::Cursor::new(img);
        super::super::can_defrag_clone_hfsplus(&mut cursor, 0)
            .expect("cleanly-unmounted journaled volume must be accepted");
    }

    #[test]
    fn test_can_defrag_clone_refuses_dirty_journaled_volume() {
        // Journaled bit set, unmounted bit *clear* — dirty.
        let mut img = make_editable_hfsplus_image();
        let attr_off = 1024 + 4;
        let mut attrs = BigEndian::read_u32(&img[attr_off..attr_off + 4]);
        attrs |= HFSPLUS_VOLUME_JOURNALED_BIT;
        attrs &= !HFSPLUS_VOLUME_UNMOUNTED_BIT;
        BigEndian::write_u32(&mut img[attr_off..attr_off + 4], attrs);
        let mut cursor = std::io::Cursor::new(img);
        let err = super::super::can_defrag_clone_hfsplus(&mut cursor, 0)
            .expect_err("dirty journaled volume must be refused");
        assert!(
            err.contains("dirty") || err.contains("journal"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_sync_metadata_stamps_clean_unmount_attributes() {
        // Regression: an HFS+ volume opened from a "mounted/dirty" on-disk
        // state (bit 8 clear, bit 14 set) must be persisted as cleanly
        // unmounted (bit 8 set, bits 11/14 clear) after sync_metadata.
        // Without this, Disk Utility / fsck_hfs flag the volume as
        // inconsistent on next mount. See ~/Downloads/HFSTests/ case.
        let mut img = make_editable_hfsplus_image();
        let attr_off = 1024 + 4;
        let dirty = 0x80004000u32 | HFSPLUS_BOOT_VOLUME_INCONSISTENT_BIT;
        BigEndian::write_u32(&mut img[attr_off..attr_off + 4], dirty);

        let cursor = std::io::Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).expect("open");
        fs.prepare_for_edit().expect("prep");
        fs.sync_metadata().expect("sync");

        let img = fs.reader.into_inner();
        let after = BigEndian::read_u32(&img[attr_off..attr_off + 4]);
        assert!(
            after & HFSPLUS_VOLUME_UNMOUNTED_BIT != 0,
            "unmounted bit must be set after sync, got 0x{after:08x}"
        );
        assert!(
            after & HFSPLUS_VOLUME_INCONSISTENT_BIT == 0,
            "inconsistent bit must be cleared, got 0x{after:08x}"
        );
        assert!(
            after & HFSPLUS_BOOT_VOLUME_INCONSISTENT_BIT == 0,
            "boot-inconsistent bit must be cleared, got 0x{after:08x}"
        );
    }

    #[test]
    fn test_can_defrag_clone_refuses_non_hfsplus() {
        // 1 KiB of zeros at offset 0 → no VH at offset 1024. Refused.
        let img = vec![0u8; 4096];
        let mut cursor = std::io::Cursor::new(img);
        let err = super::super::can_defrag_clone_hfsplus(&mut cursor, 0)
            .expect_err("non-HFS+ partition must be refused");
        assert!(err.contains("not an HFS"), "unexpected error: {err}");
    }

    #[test]
    fn test_prepare_for_edit_clean_volume_succeeds() {
        let img = make_editable_hfsplus_image();
        let cursor = std::io::Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).unwrap();
        fs.prepare_for_edit()
            .expect("clean volume should accept edit prep");
        // No xattrs on the synthetic image — the cached set is empty.
        assert!(fs.xattr_cnids.as_ref().unwrap().is_empty());
    }

    #[test]
    fn test_create_blank_hfsplus_32mib() {
        let img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "MyVol", false);
        assert_eq!(img.len(), 32 * 1024 * 1024);

        let cursor = std::io::Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).expect("blank should open");
        assert_eq!(fs.fs_type(), "HFS+");
        assert_eq!(fs.label, "MyVol");

        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert!(entries.is_empty(), "freshly built blank must list empty");

        // 32 MiB / 4096 = 8192 blocks. The catalog is now volume-scaled
        // (~0.5%, docs/hfsplus_btree_growth_plan.md §4c): 32 MiB / 200 =
        // 167,772 bytes -> 41 nodes @ 4096. Reserved = 1 (boot/VH) + 1 (bitmap)
        // + 4 (extents, default) + 41 (catalog) = 47 at the front, plus the
        // trailing allocation block holding the alternate volume header (Apple
        // reserves it; required for an fsck_hfs-clean free-block count).
        assert_eq!(fs.vh.total_blocks, 8192);
        assert_eq!(fs.vh.free_blocks, 8192 - 47 - 1);
        // Edit-mode prep should succeed (unmounted bit set, no journal).
        fs.prepare_for_edit().expect("blank must accept edit prep");
    }

    #[test]
    fn test_create_blank_hfsx_32mib_signature() {
        let img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "CSVol", true);
        let cursor = std::io::Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).expect("HFSX blank should open");
        assert_eq!(fs.fs_type(), "HFSX");
        assert!(fs.vh.is_hfsx());
        assert_eq!(fs.label, "CSVol");
        let root = fs.root().unwrap();
        assert!(fs.list_directory(&root).unwrap().is_empty());
    }

    #[test]
    fn test_create_blank_hfsplus_supports_create_file() {
        // Round-trip: build a blank, create a file inside it, sync, reopen.
        let img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "Work", false);
        let cursor = std::io::Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).unwrap();
        fs.prepare_for_edit().unwrap();
        let root = fs.root().unwrap();
        let payload = b"hello world".to_vec();
        let mut data = std::io::Cursor::new(payload.clone());
        fs.create_file(
            &root,
            "hello.txt",
            &mut data,
            payload.len() as u64,
            &CreateFileOptions::default(),
        )
        .expect("create_file on blank");
        fs.sync_metadata().unwrap();

        let img2 = fs.reader.into_inner();
        let mut cursor2 = std::io::Cursor::new(img2);
        let mut fs2 = HfsPlusFilesystem::open(&mut cursor2, 0).unwrap();
        let root2 = fs2.root().unwrap();
        let entries = fs2.list_directory(&root2).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "hello.txt");
        let got = fs2.read_file(&entries[0], 1024).unwrap();
        assert_eq!(got, payload);
    }

    #[test]
    fn test_hfsplus_rename_round_trips() {
        let img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "Work", false);
        let mut fs = HfsPlusFilesystem::open(std::io::Cursor::new(img), 0).unwrap();
        fs.prepare_for_edit().unwrap();
        let root = fs.root().unwrap();

        let payload = b"keep my bytes across a rename".to_vec();
        let mut data = std::io::Cursor::new(payload.clone());
        let fe = fs
            .create_file(
                &root,
                "before.txt",
                &mut data,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
        let cnid = fe.location;
        fs.create_directory(&root, "olddir", &CreateDirectoryOptions::default())
            .unwrap();

        // File rename to a longer name — exercises the variable-length thread
        // record rebuild (not an in-place patch).
        fs.rename(&root, &fe, "a much longer renamed file.txt")
            .unwrap();
        // Directory rename — its thread record is rebuilt too.
        let old_dir = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "olddir")
            .unwrap();
        fs.rename(&root, &old_dir, "newdir").unwrap();

        // A collision with a different entry is rejected.
        let mut t = std::io::Cursor::new(b"x".as_slice());
        fs.create_file(&root, "taken.txt", &mut t, 1, &CreateFileOptions::default())
            .unwrap();
        let renamed_now = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "a much longer renamed file.txt")
            .unwrap();
        assert!(matches!(
            fs.rename(&root, &renamed_now, "taken.txt"),
            Err(FilesystemError::AlreadyExists(_))
        ));

        // Persist and reopen: the renames survive a round-trip to disk.
        fs.sync_metadata().unwrap();
        let img2 = fs.reader.into_inner();
        let mut c2 = std::io::Cursor::new(img2);
        let mut fs2 = HfsPlusFilesystem::open(&mut c2, 0).unwrap();
        let root2 = fs2.root().unwrap();
        let entries = fs2.list_directory(&root2).unwrap();
        assert!(
            entries
                .iter()
                .any(|e| e.name == "a much longer renamed file.txt"),
            "{entries:?}"
        );
        assert!(
            !entries.iter().any(|e| e.name == "before.txt"),
            "{entries:?}"
        );
        let renamed = entries
            .iter()
            .find(|e| e.name == "a much longer renamed file.txt")
            .unwrap();
        // CNID and content preserved through the catalog re-key.
        assert_eq!(renamed.location, cnid);
        assert_eq!(fs2.read_file(renamed, 4096).unwrap(), payload);
        // The renamed directory is still browsable.
        let newdir = entries.iter().find(|e| e.name == "newdir").unwrap();
        assert!(fs2.list_directory(newdir).is_ok());
    }

    #[test]
    fn test_create_file_stamps_dates() {
        // Newly-created files must have createDate / contentModDate /
        // attributeModDate / accessDate equal to sync-time hfs_now() to
        // within a couple seconds (test wall-clock jitter).
        let img = make_editable_hfsplus_image();
        let cursor = std::io::Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).unwrap();
        fs.prepare_for_edit().unwrap();

        let now_before = hfs_common::hfs_now();
        let root = fs.root().unwrap();
        let mut data = std::io::Cursor::new(b"hi".as_slice());
        let entry = fs
            .create_file(
                &root,
                "dated.txt",
                &mut data,
                2,
                &CreateFileOptions::default(),
            )
            .unwrap();
        let now_after = hfs_common::hfs_now();

        // Locate the new file's catalog record and read all four dates.
        let cnid = entry.location as u32;
        let (_n, _r, f_off) = fs.find_catalog_record(2, "dated.txt").unwrap();
        let fkey_len = BigEndian::read_u16(&fs.catalog_data[f_off..f_off + 2]) as usize;
        let mut frec = f_off + 2 + fkey_len;
        if !frec.is_multiple_of(2) {
            frec += 1;
        }
        let create_date = BigEndian::read_u32(&fs.catalog_data[frec + 12..frec + 16]);
        let content_mod = BigEndian::read_u32(&fs.catalog_data[frec + 16..frec + 20]);
        let attr_mod = BigEndian::read_u32(&fs.catalog_data[frec + 20..frec + 24]);
        let access = BigEndian::read_u32(&fs.catalog_data[frec + 24..frec + 28]);

        for (label, value) in [
            ("createDate", create_date),
            ("contentModDate", content_mod),
            ("attributeModDate", attr_mod),
            ("accessDate", access),
        ] {
            assert!(
                value >= now_before && value <= now_after + 1,
                "{label} {value} not in [{now_before}, {now_after}+1]"
            );
        }

        // Counter parity: VH file_count must have incremented by one.
        assert_eq!(fs.vh.file_count, 1);
        let _ = cnid;
    }

    #[test]
    fn test_set_type_creator_bumps_attribute_mod_date() {
        // set_type_creator is an attribute-only change: attributeModDate
        // moves forward, but contentModDate (set at create-time) stays
        // pinned to the original value.
        let img = make_editable_hfsplus_image();
        let cursor = std::io::Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).unwrap();
        fs.prepare_for_edit().unwrap();

        let root = fs.root().unwrap();
        let mut data = std::io::Cursor::new(b"x".as_slice());
        let entry = fs
            .create_file(&root, "a.txt", &mut data, 1, &CreateFileOptions::default())
            .unwrap();

        let (_n, _r, f_off) = fs.find_catalog_record(2, "a.txt").unwrap();
        let fkey_len = BigEndian::read_u16(&fs.catalog_data[f_off..f_off + 2]) as usize;
        let mut frec = f_off + 2 + fkey_len;
        if !frec.is_multiple_of(2) {
            frec += 1;
        }
        let content_before = BigEndian::read_u32(&fs.catalog_data[frec + 16..frec + 20]);

        // Force the timestamp to advance — hfs_now() has 1-second resolution.
        // Stuff the create-time content date into the past so we can assert
        // it stays put while attributeModDate moves to "now".
        BigEndian::write_u32(
            &mut fs.catalog_data[frec + 16..frec + 20],
            content_before - 60,
        );
        BigEndian::write_u32(
            &mut fs.catalog_data[frec + 20..frec + 24],
            content_before - 60,
        );

        let now_before = hfs_common::hfs_now();
        fs.set_type_creator(&entry, "TEXT", "ttxt").unwrap();
        let now_after = hfs_common::hfs_now();

        let content_after = BigEndian::read_u32(&fs.catalog_data[frec + 16..frec + 20]);
        let attr_after = BigEndian::read_u32(&fs.catalog_data[frec + 20..frec + 24]);
        assert_eq!(
            content_after,
            content_before - 60,
            "contentModDate must not move on attribute-only change"
        );
        assert!(
            attr_after >= now_before && attr_after <= now_after + 1,
            "attributeModDate {attr_after} not in [{now_before}, {now_after}+1]"
        );
    }

    #[test]
    fn test_extents_overflow_round_trip_12_extents() {
        // Pre-fragment so 12 isolated 1-block runs are the only free space,
        // then create a 12-block file. The first 8 land in the inline
        // ForkData slots, the remaining 4 in one extents-overflow record.
        // Sync, reopen, verify byte-equal read via read_fork_with_overflow.
        let img = make_editable_hfsplus_image_with_extents_btree();
        let cursor = std::io::Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).unwrap();
        fs.prepare_for_edit().unwrap();

        // Mark every block 0..=255 allocated, then clear 12 isolated single
        // blocks at evenly-spaced odd indices in the user-data region. The
        // 12 free blocks are at positions 11, 13, 15, ..., 33.
        fs.ensure_bitmap().unwrap();
        let bitmap = fs.bitmap.as_mut().unwrap();
        for byte in bitmap.iter_mut().take(32) {
            *byte = 0xFF;
        }
        let free_positions: Vec<u32> = (0..12).map(|i| 11 + 2 * i as u32).collect();
        for &blk in &free_positions {
            bitmap_clear_bit_be(bitmap, blk);
        }
        fs.vh.free_blocks = 12;

        let block_size = fs.vh.block_size as usize;
        let payload: Vec<u8> = (0..(12 * block_size)).map(|i| (i % 251) as u8).collect();
        let mut reader = std::io::Cursor::new(payload.clone());
        let root = fs.root().unwrap();
        let entry = fs
            .create_file(
                &root,
                "frag12.bin",
                &mut reader,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("12-extent allocation should succeed via overflow B-tree");

        let cnid = entry.location as u32;
        let (data_fork, _rsrc) = fs.find_file_by_id(cnid).unwrap();
        // 8 inline extents, all 1-block.
        for slot in &data_fork.extents {
            assert_eq!(slot.block_count, 1);
        }
        assert_eq!(data_fork.total_blocks, 12);

        // Sync to disk.
        fs.sync_metadata().unwrap();

        // Reopen the underlying buffer and confirm read_fork_with_overflow
        // reproduces the data byte-for-byte.
        let img_after = fs.reader.into_inner();
        let mut cursor2 = std::io::Cursor::new(img_after);
        let mut fs2 = HfsPlusFilesystem::open(&mut cursor2, 0).unwrap();
        let root2 = fs2.root().unwrap();
        let entries = fs2.list_directory(&root2).unwrap();
        let reopened = entries.iter().find(|e| e.name == "frag12.bin").unwrap();
        let got = fs2.read_file(reopened, payload.len() + 1).unwrap();
        assert_eq!(got.len(), payload.len());
        assert_eq!(got, payload);
    }

    #[test]
    fn test_allocate_extents_fragmented_bitmap() {
        // Pre-fragment the synthetic volume so 4 free blocks exist as four
        // 1-block runs (blocks 7, 9, 11, 13). Asking for 4 must return four
        // single-block extents and the data must round-trip byte-equal.
        let img = make_editable_hfsplus_image();
        let cursor = std::io::Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).unwrap();
        fs.prepare_for_edit().unwrap();

        // Force the bitmap to load, then rewrite it. Block 0 = first byte
        // (bits 0..=7 = blocks 0..=7); we want blocks 0..=255 all allocated
        // EXCEPT 7, 9, 11, 13.
        fs.ensure_bitmap().unwrap();
        let bitmap = fs.bitmap.as_mut().unwrap();
        for byte in bitmap.iter_mut().take(32) {
            *byte = 0xFF;
        }
        // Clear blocks 7, 9, 11, 13.
        for blk in [7u32, 9, 11, 13] {
            bitmap_clear_bit_be(bitmap, blk);
        }
        fs.vh.free_blocks = 4;

        let block_size = fs.vh.block_size as usize;
        let payload: Vec<u8> = (0..(4 * block_size)).map(|i| (i % 251) as u8).collect();
        let mut reader = std::io::Cursor::new(payload.clone());
        let root = fs.root().unwrap();
        let entry = fs
            .create_file(
                &root,
                "frag.bin",
                &mut reader,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("fragmented allocation should succeed");

        let cnid = entry.location as u32;
        let (data_fork, _rsrc) = fs.find_file_by_id(cnid).expect("file record present");

        // 4 single-block extents in slots 0..3.
        for (i, expected_start) in [7u32, 9, 11, 13].iter().enumerate() {
            assert_eq!(data_fork.extents[i].block_count, 1, "slot {i}");
            assert_eq!(
                data_fork.extents[i].start_block, *expected_start,
                "slot {i}"
            );
        }
        for slot in &data_fork.extents[4..] {
            assert!(slot.is_empty(), "slot past 4 should be empty");
        }
        assert_eq!(data_fork.total_blocks, 4);

        // Read it back and confirm byte-equal.
        let mut got = Vec::new();
        fs.write_file_to(&entry, &mut got).unwrap();
        assert_eq!(got, payload);
        assert_eq!(fs.vh.free_blocks, 0);
    }

    #[test]
    fn test_snapshot_rollback_on_disk_full_create_file() {
        // The synthetic editable image has 250 free 4 KiB blocks. Asking
        // create_file for 251 blocks' worth of data should trip the
        // allocator's DiskFull and the snapshot guard must rewind every
        // mutation made before the failure (next_catalog_id bump, bitmap
        // dirty load, etc.).
        let img = make_editable_hfsplus_image();
        let cursor = std::io::Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).unwrap();
        fs.prepare_for_edit().unwrap();

        let next_cnid_before = fs.vh.next_catalog_id;
        let free_blocks_before = fs.vh.free_blocks;
        let file_count_before = fs.vh.file_count;
        let catalog_before = fs.catalog_data.clone();

        let root = fs.root().unwrap();
        let big_len: u64 = 251 * 4096 + 1; // exceeds the 250 free blocks
        let big_buf = vec![0u8; big_len as usize];
        let mut reader = std::io::Cursor::new(big_buf);
        let err = fs
            .create_file(
                &root,
                "too-big.bin",
                &mut reader,
                big_len,
                &CreateFileOptions::default(),
            )
            .expect_err("create_file should fail with DiskFull");
        assert!(matches!(err, FilesystemError::DiskFull(_)), "got {err:?}");

        assert_eq!(fs.vh.next_catalog_id, next_cnid_before);
        assert_eq!(fs.vh.free_blocks, free_blocks_before);
        assert_eq!(fs.vh.file_count, file_count_before);
        assert_eq!(fs.catalog_data, catalog_before);
    }

    #[test]
    fn test_set_xattr_round_trip_and_delete_cleanup() {
        // Step 13 round-trip: on a volume with an attributes B-tree, create a
        // file, attach 3 inline xattrs, sync, reopen, verify all 3 round-trip,
        // then delete the file and confirm every attribute record for its
        // CNID is gone.
        //
        // Seed image carries a sentinel record on cnid=99 so the attributes
        // file actually exists; the test never reads it back, just verifies
        // it survives unaffected when an unrelated CNID's xattrs are deleted.
        let sentinel_cnid = 99u32;
        let img = make_editable_hfsplus_image_with_inline_xattr(sentinel_cnid, &[0xAA; 8]);
        let cursor = std::io::Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).unwrap();
        fs.prepare_for_edit().unwrap();

        let root = fs.root().unwrap();
        let mut data = std::io::Cursor::new(b"x".as_slice());
        let entry = fs
            .create_file(
                &root,
                "with-xattr.txt",
                &mut data,
                1,
                &CreateFileOptions::default(),
            )
            .expect("create_file");
        let cnid = entry.location as u32;

        let pairs: [(&str, &[u8]); 3] = [
            ("com.apple.FinderInfo", &[1u8; 32]),
            ("com.apple.metadata:_kMDItemUserTags", b"\x00bplist00 tag"),
            ("user.short", b"hi"),
        ];
        for (name, value) in pairs.iter() {
            fs.set_xattr(cnid, name, value).expect("set_xattr");
        }
        fs.sync_metadata().unwrap();

        // Reopen the freshly synced bytes and verify all three records survived.
        let img2 = fs.reader.into_inner();
        let mut cursor2 = std::io::Cursor::new(img2);
        let mut fs2 = HfsPlusFilesystem::open(&mut cursor2, 0).unwrap();
        let mut got = fs2.list_xattrs(cnid).expect("list_xattrs");
        got.sort_by(|a, b| a.name.cmp(&b.name));
        let mut want: Vec<(String, Vec<u8>)> = pairs
            .iter()
            .map(|(n, v)| (n.to_string(), v.to_vec()))
            .collect();
        want.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(got.len(), want.len());
        for (rec, (wname, wval)) in got.iter().zip(want.iter()) {
            assert_eq!(rec.name, *wname);
            match &rec.kind {
                XattrKind::Inline(bytes) => assert_eq!(bytes, wval),
                other => panic!("expected Inline xattr, got {other:?}"),
            }
        }

        // Sentinel xattr on the unrelated CNID still present.
        let sentinel = fs2
            .list_xattrs(sentinel_cnid)
            .expect("sentinel list_xattrs");
        assert_eq!(sentinel.len(), 1, "sentinel record should survive");

        // Delete the file: every attribute row keyed at our cnid must vanish.
        fs2.prepare_for_edit().unwrap();
        let root2 = fs2.root().unwrap();
        let listing = fs2.list_directory(&root2).unwrap();
        let target = listing
            .iter()
            .find(|e| e.name == "with-xattr.txt")
            .cloned()
            .expect("entry on reopened volume");
        fs2.delete_entry(&root2, &target).expect("delete");
        let after = fs2.list_xattrs(cnid).expect("post-delete list_xattrs");
        assert!(
            after.is_empty(),
            "expected 0 records after delete, got {}",
            after.len()
        );
        let sentinel_after = fs2
            .list_xattrs(sentinel_cnid)
            .expect("sentinel post-delete");
        assert_eq!(sentinel_after.len(), 1, "sentinel must survive delete");
    }

    #[test]
    fn test_remove_xattr_not_found_and_cleanup() {
        // remove_xattr on a missing name returns NotFound, and removing the
        // last xattr drops the cnid from the cached set.
        let cnid = 17u32;
        let img = make_editable_hfsplus_image_with_inline_xattr(cnid, &[0u8; 16]);
        let cursor = std::io::Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).unwrap();
        fs.prepare_for_edit().unwrap();
        assert!(fs.xattr_cnids.as_ref().unwrap().contains(&cnid));

        let err = fs
            .remove_xattr(cnid, "com.apple.does-not-exist")
            .expect_err("missing xattr must error");
        assert!(matches!(err, FilesystemError::NotFound(_)));

        fs.remove_xattr(cnid, "com.apple.FinderInfo")
            .expect("remove existing xattr");
        assert!(
            !fs.xattr_cnids.as_ref().unwrap().contains(&cnid),
            "cnid should be dropped from cache once its last xattr is gone"
        );
    }

    #[test]
    fn test_list_xattrs_round_trips_inline_finder_info() {
        // Synthesize a 32-byte com.apple.FinderInfo inline xattr on CNID 17,
        // assert list_xattrs returns it byte-for-byte and reports an empty
        // list for an unrelated CNID.
        let cnid = 17u32;
        let finder_info: Vec<u8> = (0..32u8).collect();
        let img = make_editable_hfsplus_image_with_inline_xattr(cnid, &finder_info);
        let cursor = std::io::Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).unwrap();

        let recs = fs.list_xattrs(cnid).expect("list_xattrs cnid=17");
        assert_eq!(recs.len(), 1, "expected exactly one xattr record");
        let rec = &recs[0];
        assert_eq!(rec.name, "com.apple.FinderInfo");
        assert_eq!(rec.start_block, 0);
        match &rec.kind {
            XattrKind::Inline(bytes) => assert_eq!(bytes, &finder_info),
            other => panic!("expected Inline xattr, got {other:?}"),
        }

        // Unrelated CNID returns empty.
        let none = fs.list_xattrs(99).expect("list_xattrs cnid=99");
        assert!(none.is_empty(), "expected no xattrs for cnid=99");

        // Volume with no attributes file (the seed image) returns empty too.
        let bare = make_editable_hfsplus_image();
        let mut bare_fs = HfsPlusFilesystem::open(std::io::Cursor::new(bare), 0).unwrap();
        let empty = bare_fs.list_xattrs(cnid).expect("list_xattrs no attr file");
        assert!(empty.is_empty(), "no attributes file → no records");
    }

    #[test]
    fn test_hardlink_resolution_two_links_one_inode() {
        // Step 15: synthesize a volume with two hardlinks pointing at one
        // inode under `HFS+ Private Data`. Both link rows must surface as
        // regular files in `list_directory`, carry `link_target_cnid` set
        // to the inode CNID, and read back the inode's data fork bytes.
        const PAYLOAD: &[u8] = b"INODE_PAYLOAD_42\n";
        const I_NODE_NUM: u32 = 99;

        let mut img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "Links", false);

        // Build the volume contents: private dir, inode, two link stubs.
        // Patch the link stubs' catalog records to set FInfo type='hlnk'
        // creator='hfs+' and bsdInfo.special = I_NODE_NUM. We do this via
        // direct catalog-byte edits while holding the editable handle, then
        // sync once at the end.
        let inode_cnid: u32;
        {
            let cursor = std::io::Cursor::new(&mut img);
            let mut fs = HfsPlusFilesystem::open(cursor, 0).expect("open blank");
            fs.prepare_for_edit().expect("prepare_for_edit");
            let root = fs.root().expect("root");

            let private_dir = fs
                .create_directory(
                    &root,
                    &hfsplus_private_dir_name(),
                    &CreateDirectoryOptions::default(),
                )
                .expect("create private dir");

            let mut inode_data = std::io::Cursor::new(PAYLOAD);
            let inode_entry = fs
                .create_file(
                    &private_dir,
                    &format!("iNode{I_NODE_NUM}"),
                    &mut inode_data,
                    PAYLOAD.len() as u64,
                    &CreateFileOptions::default(),
                )
                .expect("create inode file");
            inode_cnid = inode_entry.location as u32;

            for link_name in ["linkA", "linkB"] {
                let mut empty = std::io::Cursor::new(&[][..]);
                fs.create_file(
                    &root,
                    link_name,
                    &mut empty,
                    0,
                    &CreateFileOptions {
                        os_type: Some(*b"hlnk"),
                        os_creator: Some(*b"hfs+"),
                        ..Default::default()
                    },
                )
                .expect("create link stub");

                // Patch bsdInfo.special on the freshly-created stub to point
                // at the inode number. body offset 32+12 = 44.
                let (_node, _rec, abs) = fs
                    .find_catalog_record(2, link_name)
                    .expect("link stub in catalog");
                let key_len = BigEndian::read_u16(&fs.catalog_data[abs..abs + 2]) as usize;
                let mut body_start = abs + 2 + key_len;
                if !body_start.is_multiple_of(2) {
                    body_start += 1;
                }
                BigEndian::write_u32(
                    &mut fs.catalog_data[body_start + 44..body_start + 48],
                    I_NODE_NUM,
                );
            }

            fs.sync_metadata().expect("sync");
        }

        // Re-open, walk root, verify both link stubs resolve to the inode
        // and read back the inode's bytes.
        let cursor = std::io::Cursor::new(&mut img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).expect("re-open");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list root");
        let link_a = entries
            .iter()
            .find(|e| e.name == "linkA")
            .cloned()
            .expect("linkA");
        let link_b = entries
            .iter()
            .find(|e| e.name == "linkB")
            .cloned()
            .expect("linkB");

        assert_eq!(
            link_a.link_target_cnid,
            Some(inode_cnid as u64),
            "linkA must resolve to inode CNID"
        );
        assert_eq!(
            link_b.link_target_cnid,
            Some(inode_cnid as u64),
            "linkB must resolve to inode CNID"
        );
        // Display size should reflect the inode's data fork, not the empty
        // stub fork.
        assert_eq!(link_a.size, PAYLOAD.len() as u64);
        assert_eq!(link_b.size, PAYLOAD.len() as u64);

        let bytes_a = fs.read_file(&link_a, usize::MAX).expect("read linkA");
        let bytes_b = fs.read_file(&link_b, usize::MAX).expect("read linkB");
        assert_eq!(bytes_a, PAYLOAD);
        assert_eq!(bytes_b, PAYLOAD);

        // Sanity: a file with no hardlink magic stays plain.
        // The private dir itself shows up at root with `link_target_cnid`
        // unset (it's a folder, not a file hardlink).
        let private = entries
            .iter()
            .find(|e| e.name == hfsplus_private_dir_name())
            .expect("private dir at root");
        assert!(private.link_target_cnid.is_none());
    }

    #[test]
    fn test_hardlink_delete_decrements_then_frees_inode() {
        // Step 16: deleting a hardlink decrements `linkCount` on the inode;
        // deleting the last reference frees the inode, drops it from the
        // private directory, and reclaims its data fork blocks.
        const PAYLOAD: &[u8] = b"INODE_DATA_FOR_DELETE\n";
        const I_NODE_NUM: u32 = 7;

        let mut img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "DelLinks", false);

        // Capture initial free-block count after the build, then synthesize
        // private dir + inode (linkCount=2) + two stubs.
        let inode_cnid: u32;
        let private_cnid: u32;
        let payload_blocks_used: u32;
        let free_after_build: u32;
        let free_with_inode: u32;
        {
            let cursor = std::io::Cursor::new(&mut img);
            let mut fs = HfsPlusFilesystem::open(cursor, 0).expect("open blank");
            fs.prepare_for_edit().expect("prepare_for_edit");
            free_after_build = fs.vh.free_blocks;
            let root = fs.root().expect("root");
            let private_dir = fs
                .create_directory(
                    &root,
                    &hfsplus_private_dir_name(),
                    &CreateDirectoryOptions::default(),
                )
                .expect("create private dir");
            private_cnid = private_dir.location as u32;

            let mut inode_data = std::io::Cursor::new(PAYLOAD);
            let inode_entry = fs
                .create_file(
                    &private_dir,
                    &format!("iNode{I_NODE_NUM}"),
                    &mut inode_data,
                    PAYLOAD.len() as u64,
                    &CreateFileOptions::default(),
                )
                .expect("create inode file");
            inode_cnid = inode_entry.location as u32;
            free_with_inode = fs.vh.free_blocks;
            payload_blocks_used = free_after_build - free_with_inode;
            assert!(
                payload_blocks_used >= 1,
                "inode payload must consume at least one block"
            );

            // Initialize bsdInfo.special on the inode record to linkCount=2.
            {
                let (_, _, abs) = fs
                    .find_catalog_record(private_cnid, &format!("iNode{I_NODE_NUM}"))
                    .expect("inode catalog record");
                let key_len = BigEndian::read_u16(&fs.catalog_data[abs..abs + 2]) as usize;
                let mut body = abs + 2 + key_len;
                if !body.is_multiple_of(2) {
                    body += 1;
                }
                BigEndian::write_u32(&mut fs.catalog_data[body + 44..body + 48], 2);
            }

            for link_name in ["linkA", "linkB"] {
                let mut empty = std::io::Cursor::new(&[][..]);
                fs.create_file(
                    &root,
                    link_name,
                    &mut empty,
                    0,
                    &CreateFileOptions {
                        os_type: Some(*b"hlnk"),
                        os_creator: Some(*b"hfs+"),
                        ..Default::default()
                    },
                )
                .expect("create stub");
                let (_, _, abs) = fs.find_catalog_record(2, link_name).expect("stub");
                let key_len = BigEndian::read_u16(&fs.catalog_data[abs..abs + 2]) as usize;
                let mut body = abs + 2 + key_len;
                if !body.is_multiple_of(2) {
                    body += 1;
                }
                BigEndian::write_u32(&mut fs.catalog_data[body + 44..body + 48], I_NODE_NUM);
            }
            fs.sync_metadata().expect("sync");
        }

        // Re-open editable, delete linkA. linkB and the inode must remain
        // readable; linkCount must drop to 1.
        let cursor = std::io::Cursor::new(&mut img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).expect("re-open");
        fs.prepare_for_edit().expect("prepare_for_edit");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list");
        let link_a = entries
            .iter()
            .find(|e| e.name == "linkA")
            .cloned()
            .expect("linkA");
        let link_b = entries
            .iter()
            .find(|e| e.name == "linkB")
            .cloned()
            .expect("linkB");
        let free_before_delete = fs.vh.free_blocks;

        fs.delete_entry(&root, &link_a).expect("delete linkA");
        // linkCount should now be 1; inode + linkB still alive.
        let cnt_after_first = {
            let (_, _, abs) = fs
                .find_catalog_record(private_cnid, &format!("iNode{I_NODE_NUM}"))
                .expect("inode still present");
            let key_len = BigEndian::read_u16(&fs.catalog_data[abs..abs + 2]) as usize;
            let mut body = abs + 2 + key_len;
            if !body.is_multiple_of(2) {
                body += 1;
            }
            BigEndian::read_u32(&fs.catalog_data[body + 44..body + 48])
        };
        assert_eq!(cnt_after_first, 1, "linkCount should decrement to 1");
        assert_eq!(
            fs.vh.free_blocks, free_before_delete,
            "no blocks freed while inode still has refs"
        );
        // linkB still resolves and reads the original payload.
        let bytes_b = fs.read_file(&link_b, usize::MAX).expect("read linkB");
        assert_eq!(bytes_b, PAYLOAD);

        fs.delete_entry(&root, &link_b).expect("delete linkB");
        // Inode must be gone from the private dir, blocks reclaimed.
        let private_children = fs.list_children(private_cnid).expect("list private dir");
        assert!(
            private_children.is_empty(),
            "iNode entry should be removed once linkCount hits 0; got {} children",
            private_children.len()
        );
        assert!(
            fs.find_file_by_id(inode_cnid).is_none(),
            "inode catalog row should be gone"
        );
        assert_eq!(
            fs.vh.free_blocks,
            free_before_delete + payload_blocks_used,
            "inode payload blocks should be reclaimed"
        );
    }

    #[test]
    fn test_hardlink_capture_and_replay_dedupes_inode() {
        // Step 17: source carries 1 inode + 3 file hardlinks. Capture
        // routes the inode into `files` with `is_inode = true` and the
        // three stubs into `hardlinks`. Replay onto a freshly built blank
        // via `create_hardlink_inode` + `create_hardlink` produces a
        // target with one inode + three stub rows (not four independent
        // files), all reading the same payload bytes.
        const PAYLOAD: &[u8] = b"shared inode contents (3 names)\n";
        const I_NODE_NUM: u32 = 17;

        // Build the source.
        let mut source_img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "Src", false);
        let source_inode_cnid: u32;
        {
            let cursor = std::io::Cursor::new(&mut source_img);
            let mut fs = HfsPlusFilesystem::open(cursor, 0).expect("open src blank");
            fs.prepare_for_edit().expect("prepare_for_edit");
            let mut data = std::io::Cursor::new(PAYLOAD);
            let inode = fs
                .create_hardlink_inode(
                    I_NODE_NUM,
                    &mut data,
                    PAYLOAD.len() as u64,
                    &CreateFileOptions::default(),
                )
                .expect("create source inode");
            source_inode_cnid = inode.location as u32;
            // Initialize linkCount=0 on the inode then let create_hardlink
            // bump it three times.
            let private_cnid = fs.find_private_dir_cnid().unwrap().expect("private dir");
            let _ = fs
                .adjust_inode_link_count(private_cnid, I_NODE_NUM, 0)
                .expect("init linkCount");
            let root = fs.root().expect("root");
            for name in ["a.txt", "b.txt", "c.txt"] {
                fs.create_hardlink(&root, name, I_NODE_NUM)
                    .expect("create stub");
            }
            fs.sync_metadata().expect("sync src");
        }

        // Capture from the source.
        let cursor = std::io::Cursor::new(&mut source_img);
        let mut src_fs = HfsPlusFilesystem::open(cursor, 0).expect("re-open src");
        let snap =
            crate::fs::hfsplus_clone::SourceCatalogSnapshot::capture(&mut src_fs).expect("capture");

        // Snapshot expectations: exactly one inode, three hardlink stubs.
        let inodes: Vec<_> = snap.files.iter().filter(|f| f.is_inode).collect();
        assert_eq!(inodes.len(), 1, "expected exactly one inode in snapshot");
        let inode = inodes[0];
        assert_eq!(inode.cnid, source_inode_cnid);
        assert_eq!(inode.inode_num, Some(I_NODE_NUM));
        assert_eq!(inode.data_fork_size, PAYLOAD.len() as u64);
        // Stream the inode's data fork off the source and verify byte-equal.
        let inode_payload = {
            use std::io::Read;
            let mut buf = Vec::new();
            src_fs
                .fork_stream_reader(inode.cnid, 0x00, &inode.data_fork_raw)
                .expect("fork reader")
                .read_to_end(&mut buf)
                .expect("read fork");
            buf
        };
        assert_eq!(inode_payload, PAYLOAD);

        let user_files: Vec<_> = snap.files.iter().filter(|f| !f.is_inode).collect();
        assert!(
            user_files.is_empty(),
            "user-tree files should be empty (the three stubs are in `hardlinks`); \
             got {:?}",
            user_files.iter().map(|f| &f.name).collect::<Vec<_>>()
        );

        assert_eq!(snap.hardlinks.len(), 3, "expected 3 hardlink stubs");
        for h in &snap.hardlinks {
            assert_eq!(h.inode_num, I_NODE_NUM);
            assert_eq!(h.inode_source_cnid, source_inode_cnid);
            assert_eq!(h.parent_cnid, 2, "stubs live at root");
        }

        // Replay onto a fresh blank target using the capture data.
        let mut target_img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "Tgt", false);
        let target_inode_cnid: u32;
        {
            let cursor = std::io::Cursor::new(&mut target_img);
            let mut tgt = HfsPlusFilesystem::open(cursor, 0).expect("open tgt blank");
            tgt.prepare_for_edit().expect("prepare_for_edit");

            let mut data = std::io::Cursor::new(&inode_payload);
            let target_inode = tgt
                .create_hardlink_inode(
                    inode.inode_num.unwrap(),
                    &mut data,
                    inode.data_fork_size,
                    &CreateFileOptions::default(),
                )
                .expect("replay inode");
            target_inode_cnid = target_inode.location as u32;

            let root = tgt.root().expect("tgt root");
            for h in &snap.hardlinks {
                tgt.create_hardlink(&root, &h.name, h.inode_num)
                    .expect("replay stub");
            }
            tgt.sync_metadata().expect("sync tgt");
        }

        // Verify the target.
        let cursor = std::io::Cursor::new(&mut target_img);
        let mut tgt_fs = HfsPlusFilesystem::open(cursor, 0).expect("re-open tgt");
        let root = tgt_fs.root().expect("tgt root");
        let entries = tgt_fs.list_directory(&root).expect("list tgt root");

        // Three stubs at root, each pointing at the same inode CNID, each
        // reading back the original payload.
        let stubs: Vec<&FileEntry> = entries
            .iter()
            .filter(|e| ["a.txt", "b.txt", "c.txt"].contains(&e.name.as_str()))
            .collect();
        assert_eq!(stubs.len(), 3);
        for stub in &stubs {
            assert_eq!(
                stub.link_target_cnid,
                Some(target_inode_cnid as u64),
                "stub {} must resolve to the single target inode",
                stub.name
            );
            assert_eq!(stub.size, PAYLOAD.len() as u64);
        }
        for stub in &stubs {
            let bytes = tgt_fs.read_file(stub, usize::MAX).expect("read tgt stub");
            assert_eq!(bytes, PAYLOAD);
        }

        // Inode's linkCount on the target must be 3 (one per stub).
        let private_cnid = tgt_fs
            .find_private_dir_cnid()
            .unwrap()
            .expect("private dir");
        let inode_name = format!("iNode{I_NODE_NUM}");
        let (_, _, abs) = tgt_fs
            .find_catalog_record(private_cnid, &inode_name)
            .expect("inode in private dir");
        let key_len = BigEndian::read_u16(&tgt_fs.catalog_data[abs..abs + 2]) as usize;
        let mut body = abs + 2 + key_len;
        if !body.is_multiple_of(2) {
            body += 1;
        }
        let link_count = BigEndian::read_u32(&tgt_fs.catalog_data[body + 44..body + 48]);
        assert_eq!(link_count, 3, "inode linkCount must equal stub count");
    }

    #[test]
    fn test_dir_hardlink_capture_and_replay_resolves_to_inode() {
        // Step 18: Time-Machine-shaped image — one directory inode under
        // `.HFS+ Private Directory Data\r` with two hardlink stubs at the
        // root. Capture must dedupe the inode (one entry in `dirs` with
        // `is_dir_inode = true`, two entries in `dir_hardlinks`); replay
        // must produce a target whose two link rows resolve to the same
        // directory contents byte-for-byte.
        const PAYLOAD: &[u8] = b"file inside dir-hardlinked folder\n";
        const D_INODE_NUM: u32 = 13;

        // ---- Build the source. ----
        let mut source_img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "DhSrc", false);
        let source_inode_cnid: u32;
        {
            let cursor = std::io::Cursor::new(&mut source_img);
            let mut fs = HfsPlusFilesystem::open(cursor, 0).expect("open src");
            fs.prepare_for_edit().expect("prepare_for_edit");
            let inode_dir = fs
                .create_dir_hardlink_inode(D_INODE_NUM, &CreateDirectoryOptions::default())
                .expect("create dir inode");
            source_inode_cnid = inode_dir.location as u32;
            // Put a file inside the inode dir so `list_directory` has
            // something to compare on the replayed side.
            let mut data = std::io::Cursor::new(PAYLOAD);
            fs.create_file(
                &inode_dir,
                "inside.txt",
                &mut data,
                PAYLOAD.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create file in dir inode");
            // Initialize linkCount=0 then bump per stub.
            let dir_priv = fs
                .find_dir_private_dir_cnid()
                .unwrap()
                .expect("dir private");
            let _ = fs
                .adjust_dir_inode_link_count(dir_priv, D_INODE_NUM, 0)
                .expect("init dir linkCount");
            let root = fs.root().expect("root");
            for name in ["alpha", "beta"] {
                fs.create_dir_hardlink(&root, name, D_INODE_NUM)
                    .expect("create dir hardlink stub");
            }
            fs.sync_metadata().expect("sync src");
        }

        // ---- Capture from the source. ----
        let cursor = std::io::Cursor::new(&mut source_img);
        let mut src_fs = HfsPlusFilesystem::open(cursor, 0).expect("re-open src");
        let snap =
            crate::fs::hfsplus_clone::SourceCatalogSnapshot::capture(&mut src_fs).expect("capture");

        // Snapshot expectations: exactly one directory inode, two stubs.
        let dir_inodes: Vec<_> = snap.dirs.iter().filter(|d| d.is_dir_inode).collect();
        assert_eq!(
            dir_inodes.len(),
            1,
            "expected exactly one dir inode in snapshot; got {}",
            dir_inodes.len()
        );
        let dir_inode = dir_inodes[0];
        assert_eq!(dir_inode.cnid, source_inode_cnid);
        assert_eq!(dir_inode.inode_num, Some(D_INODE_NUM));

        assert_eq!(snap.dir_hardlinks.len(), 2, "expected 2 dir-hardlink stubs");
        for h in &snap.dir_hardlinks {
            assert_eq!(h.inode_num, D_INODE_NUM);
            assert_eq!(h.inode_dir_source_cnid, source_inode_cnid);
            assert_eq!(h.parent_cnid, 2, "stubs live at root");
        }

        // ---- Replay onto a fresh blank target. ----
        let mut target_img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "DhTgt", false);
        let target_inode_cnid: u32;
        {
            let cursor = std::io::Cursor::new(&mut target_img);
            let mut tgt = HfsPlusFilesystem::open(cursor, 0).expect("open tgt");
            tgt.prepare_for_edit().expect("prepare_for_edit");

            // Create the inode directory.
            let inode = tgt
                .create_dir_hardlink_inode(
                    dir_inode.inode_num.unwrap(),
                    &CreateDirectoryOptions::default(),
                )
                .expect("replay dir inode");
            target_inode_cnid = inode.location as u32;

            // Replay the inode's children. (For a real clone Step 21 will
            // recursively walk; here it's just one file.)
            let mut data = std::io::Cursor::new(PAYLOAD);
            tgt.create_file(
                &inode,
                "inside.txt",
                &mut data,
                PAYLOAD.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("replay child file");

            // Replay the stubs.
            let root = tgt.root().expect("tgt root");
            for h in &snap.dir_hardlinks {
                tgt.create_dir_hardlink(&root, &h.name, h.inode_num)
                    .expect("replay dir stub");
            }
            tgt.sync_metadata().expect("sync tgt");
        }

        // ---- Verify the target. ----
        let cursor = std::io::Cursor::new(&mut target_img);
        let mut tgt_fs = HfsPlusFilesystem::open(cursor, 0).expect("re-open tgt");
        let root = tgt_fs.root().expect("tgt root");
        let entries = tgt_fs.list_directory(&root).expect("list tgt root");

        // Both stubs surface as Directory entries pointing at the single
        // inode, and listing either of them returns the same children.
        for stub_name in ["alpha", "beta"] {
            let stub = entries
                .iter()
                .find(|e| e.name == stub_name)
                .cloned()
                .unwrap_or_else(|| panic!("stub {stub_name} missing from target root"));
            assert!(
                stub.is_directory(),
                "stub {stub_name} must surface as a directory"
            );
            assert_eq!(
                stub.link_target_cnid,
                Some(target_inode_cnid as u64),
                "stub {stub_name} must resolve to the single target inode"
            );

            let children = tgt_fs.list_directory(&stub).expect("list stub");
            let inside = children
                .iter()
                .find(|c| c.name == "inside.txt")
                .unwrap_or_else(|| panic!("inside.txt missing under {stub_name}"));
            let bytes = tgt_fs
                .read_file(inside, usize::MAX)
                .expect("read inside.txt via stub");
            assert_eq!(bytes, PAYLOAD);
        }

        // Inode's linkCount on the target must equal stub count (2).
        let dir_priv = tgt_fs
            .find_dir_private_dir_cnid()
            .unwrap()
            .expect("tgt dir private");
        let inode_name = format!("dir_{D_INODE_NUM}");
        let (_, _, abs) = tgt_fs
            .find_catalog_record(dir_priv, &inode_name)
            .expect("dir inode in private dir");
        let key_len = BigEndian::read_u16(&tgt_fs.catalog_data[abs..abs + 2]) as usize;
        let mut body = abs + 2 + key_len;
        if !body.is_multiple_of(2) {
            body += 1;
        }
        let link_count = BigEndian::read_u32(&tgt_fs.catalog_data[body + 44..body + 48]);
        assert_eq!(link_count, 2, "dir inode linkCount must equal stub count");
    }

    #[test]
    fn test_hfsx_case_sensitive_allows_distinct_case_siblings() {
        // Step 19: HFSX volumes with `keyCompareType == 0xBC` (binary)
        // must treat `Foo` and `foo` as distinct names. Plain HFS+ —
        // including HFSX volumes built with `keyCompareType == 0xCF`
        // (case-folding NFD compare) — must reject the second create with
        // `AlreadyExists`.
        const PAYLOAD: &[u8] = b"hi\n";

        // ---- HFSX, case-sensitive: both creates succeed. ----
        {
            let mut img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "HFSX", true);
            let cursor = std::io::Cursor::new(&mut img);
            let mut fs = HfsPlusFilesystem::open(cursor, 0).expect("open hfsx");
            assert!(
                fs.case_sensitive(),
                "fresh HFSX blank must be case-sensitive"
            );
            fs.prepare_for_edit().expect("prepare");
            let root = fs.root().expect("root");

            let mut data = std::io::Cursor::new(PAYLOAD);
            fs.create_file(
                &root,
                "Foo",
                &mut data,
                PAYLOAD.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create Foo");

            let mut data = std::io::Cursor::new(PAYLOAD);
            fs.create_file(
                &root,
                "foo",
                &mut data,
                PAYLOAD.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create foo on HFSX must succeed");

            fs.sync_metadata().expect("sync");
            // Both must list separately.
            let entries = fs.list_directory(&root).expect("list");
            let names: Vec<_> = entries.iter().map(|e| e.name.as_str()).collect();
            assert!(names.contains(&"Foo"), "Foo missing: {names:?}");
            assert!(names.contains(&"foo"), "foo missing: {names:?}");
        }

        // ---- HFS+, case-folding: second create rejects. ----
        {
            let mut img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "HFSPLUS", false);
            let cursor = std::io::Cursor::new(&mut img);
            let mut fs = HfsPlusFilesystem::open(cursor, 0).expect("open hfs+");
            assert!(
                !fs.case_sensitive(),
                "plain HFS+ blank must be case-folding"
            );
            fs.prepare_for_edit().expect("prepare");
            let root = fs.root().expect("root");

            let mut data = std::io::Cursor::new(PAYLOAD);
            fs.create_file(
                &root,
                "Foo",
                &mut data,
                PAYLOAD.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create Foo");

            let mut data = std::io::Cursor::new(PAYLOAD);
            let err = fs
                .create_file(
                    &root,
                    "foo",
                    &mut data,
                    PAYLOAD.len() as u64,
                    &CreateFileOptions::default(),
                )
                .expect_err("create foo must fail on case-folding HFS+");
            match err {
                FilesystemError::AlreadyExists(_) => {}
                other => panic!("expected AlreadyExists, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_clone_capture_carries_xattrs_byte_equal() {
        // Step 14: SourceCatalogSnapshot::capture must populate
        // SourceFileSnapshot::xattrs / SourceDirSnapshot::xattrs with the
        // inline records found in the attributes B-tree, byte-for-byte.
        //
        // Seed image already places an inline `com.apple.FinderInfo` xattr
        // on a sentinel CNID. We attach a second key on the same CNID via
        // `set_xattr`, sync, then graft a thread+file record into the
        // catalog so the walk associates that CNID with a real file —
        // capture should surface both inline values verbatim.
        let cnid = 99u32;
        let seed_value: Vec<u8> = (0..32u8).collect();
        let extra_value: Vec<u8> = b"\x00bplist00 demo".to_vec();
        let img = make_editable_hfsplus_image_with_inline_xattr(cnid, &seed_value);

        // Open editable, attach a second xattr to the sentinel cnid, attach an
        // xattr to an existing directory (CNID 2 = root), and sync.
        let cursor = std::io::Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).unwrap();
        fs.prepare_for_edit().unwrap();
        fs.set_xattr(cnid, "com.apple.metadata:demo", &extra_value)
            .expect("set extra xattr on sentinel cnid");
        fs.set_xattr(2u32, "com.apple.FinderInfo", &seed_value)
            .expect("set xattr on root dir");
        // Create a real file so the snapshot walk has a file row whose CNID
        // equals our sentinel — we relocate the captured xattrs onto it
        // by overriding the new file's catalog cnid is non-trivial, so
        // instead we capture and just look up by cnid directly.
        let root = fs.root().expect("root");
        let mut data = std::io::Cursor::new(b"hi".as_slice());
        fs.create_file(
            &root,
            "carrier.txt",
            &mut data,
            2,
            &CreateFileOptions::default(),
        )
        .expect("create_file");
        fs.sync_metadata().unwrap();
        let img2 = fs.reader.into_inner();

        let mut cursor2 = std::io::Cursor::new(img2);
        let mut fs2 = HfsPlusFilesystem::open(&mut cursor2, 0).unwrap();
        let snap =
            crate::fs::hfsplus_clone::SourceCatalogSnapshot::capture(&mut fs2).expect("capture");

        // The freshly-created file should snapshot with no xattrs (we never
        // attached any to its CNID).
        let carrier = snap
            .files
            .iter()
            .find(|f| f.name == "carrier.txt")
            .expect("carrier file in snapshot");
        assert!(
            carrier.xattrs.is_empty(),
            "fresh file should have no xattrs, got {}",
            carrier.xattrs.len()
        );

        // The root dir snapshot must carry the xattr we attached.
        let root_dir = snap
            .dirs
            .iter()
            .find(|d| d.cnid == 2)
            .expect("root dir in snapshot");
        assert_eq!(root_dir.xattrs.len(), 1, "expected 1 xattr on root dir");
        assert_eq!(root_dir.xattrs[0].name, "com.apple.FinderInfo");
        match &root_dir.xattrs[0].kind {
            XattrKind::Inline(bytes) => assert_eq!(bytes, &seed_value),
            other => panic!("expected inline FinderInfo on root, got {other:?}"),
        }

        // Attributes B-tree still carries the sentinel cnid's two records;
        // verify directly via list_xattrs (capture only attaches xattrs to
        // catalog rows it walks, and the sentinel cnid is not catalog-rooted).
        let mut sentinel = fs2.list_xattrs(cnid).expect("list_xattrs sentinel");
        sentinel.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(sentinel.len(), 2, "sentinel should carry 2 xattrs");
        let mut want: Vec<(String, Vec<u8>)> = vec![
            ("com.apple.FinderInfo".into(), seed_value.clone()),
            ("com.apple.metadata:demo".into(), extra_value.clone()),
        ];
        want.sort_by(|a, b| a.0.cmp(&b.0));
        for (rec, (wn, wv)) in sentinel.iter().zip(want.iter()) {
            assert_eq!(rec.name, *wn);
            match &rec.kind {
                XattrKind::Inline(bytes) => assert_eq!(bytes, wv),
                other => panic!("expected inline, got {other:?}"),
            }
        }
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
    fn test_hfsplus_defragmented_minimum_size() {
        // The synthetic image has 256 blocks of 4096 bytes total, with 6
        // blocks allocated near the front (VH/bitmap/catalog) and 250 free.
        let img = make_editable_hfsplus_image();
        let cursor = std::io::Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).unwrap();

        let total = fs.total_size();
        let in_place = fs.last_data_byte().unwrap();
        let defrag = fs.defragmented_minimum_size().unwrap();

        assert_eq!(total, 256 * 4096);
        // Last allocated block is 5 (bits 0..=5 set in bitmap byte 0) →
        // byte (5+1) * 4096 = 24 KiB.
        assert_eq!(in_place, 6 * 4096);
        // Defrag = (used + catalog_total_blocks*3) blocks * block_size +
        // 1024 alt-VH, rounded up. Synthetic image's catalog_total_blocks
        // is 4 → delta is 12 → 6 + 12 = 18 user blocks + alt-VH = 19.
        assert_eq!(defrag, 19 * 4096);
        assert!(defrag <= total);
    }

    #[test]
    fn test_hfsplus_defragmented_min_smaller_on_fragmented() {
        // Take the synthetic image and forge an extra allocation near the END
        // of the bitmap. The in-place trim point must hug the tail; the
        // defragmented minimum must reflect *only* the allocated count, not
        // the position — that's the whole point of the metric.
        let mut img = make_editable_hfsplus_image();
        let block_size = 4096usize;
        // Mark block 200 allocated (MSB-first bitmap, byte index 25, bit 0
        // -> shift 7 to set the most-significant bit).
        let bitmap_byte = block_size + 25; // bitmap lives in block 1
        img[bitmap_byte] |= 0b1000_0000;
        // Reflect the change in the VH so the defragmented calc sees one
        // fewer free block.
        let free_off = 1024 + 48;
        let free = BigEndian::read_u32(&img[free_off..free_off + 4]);
        BigEndian::write_u32(&mut img[free_off..free_off + 4], free - 1);
        // Mirror the change to the alternate VH at the tail.
        let image_size = img.len();
        let alt_free_off = image_size - 1024 + 48;
        BigEndian::write_u32(&mut img[alt_free_off..alt_free_off + 4], free - 1);

        let cursor = std::io::Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).unwrap();

        let in_place = fs.last_data_byte().unwrap();
        let defrag = fs.defragmented_minimum_size().unwrap();

        // In-place trim hugs the new tail allocation: byte (200+1)*4096.
        assert_eq!(in_place, 201 * 4096);
        // Defrag: (7 used + 12 catalog-growth) blocks * 4096 + 1024 alt-VH,
        // rounded up = 20 blocks. Still well under in_place (201 blocks).
        assert_eq!(defrag, 20 * 4096);
        assert!(defrag < in_place);
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
        assert_eq!(fe.type_code, Some(*b"TEXT"));
    }

    #[test]
    fn test_hfsplus_catalog_variable_index_keys_grow_multilevel() {
        // P1 (docs/hfsplus_btree_growth_plan.md §4a): the HFS+ catalog B-tree
        // must grow past a single index level without corrupting. The shared
        // split/grow helpers used to force the classic 1-byte-length, fixed-0x25
        // index-key shape onto HFS+ records, so the first leaf split produced a
        // malformed separator (it read the high byte of the 2-byte key length as
        // the whole length) and descent misrouted — the "leaves must be strictly
        // ascending" / mis-routed-descent corruption. With
        // `BTreeKeyFormat::HFSPLUS_CATALOG` the separator is the child's
        // variable-length 2-byte key, verbatim.
        //
        // Driven at the catalog-buffer level through the exact shared
        // `btree_insert_full` path `insert_catalog_record` uses. node_size is
        // 512 (vs the production 4096) so a few thousand multi-parent records
        // reach depth >= 3 in a < 1 MiB buffer; the machinery is node-size
        // agnostic. The volume-level fsck gate lands in P2 once blank catalogs
        // are sized to hold the records (today a blank catalog is only 4 nodes).
        use super::hfs_common::{
            btree_find_record, btree_insert_full, BTreeHeader, BTreeKeyFormat,
        };
        use byteorder::{BigEndian, ByteOrder};
        type Fs = HfsPlusFilesystem<std::io::Cursor<Vec<u8>>>;

        let node_size = 512usize;
        // A single-node bitmap (512-byte node) covers ~1872 nodes, so stay under
        // that — no map nodes needed, matching a real blank-built tree.
        let total_nodes = 1800u32;
        let mut buf = vec![0u8; total_nodes as usize * node_size];
        write_blank_btree_header_node(
            &mut buf[0..node_size],
            node_size,
            /* leaf_records= */ 0,
            total_nodes,
            /* free_nodes= */ total_nodes - 2,
            /* max_key_len= */ 516,
            KEY_COMPARE_CASE_FOLDING,
        );
        write_empty_leaf_node(&mut buf[node_size..2 * node_size]);

        let kf = BTreeKeyFormat::HFSPLUS_CATALOG;
        let cmp = |a: &[u8], b: &[u8]| Fs::catalog_compare(a, b, false);

        // ~2500 files in shuffled key order (multiplicative hash is a bijection
        // mod 2^32 — unique names that land mid-leaf, not appended) sprayed
        // across 50 parent directories, so inserts exercise every leaf and
        // index split, not just tail appends.
        const DIRS: u32 = 50;
        const N: u32 = 2500;
        let mut inserted: Vec<(u32, String)> = Vec::with_capacity(N as usize);
        for i in 0..N {
            let hash = i.wrapping_mul(2_654_435_761);
            let parent = 16 + (hash % DIRS); // arbitrary distinct parent CNIDs
            let name = format!("f{hash:08x}");
            let mut rec = Fs::build_catalog_key(parent, &name);
            // Append filler so records have a realistic size (the compare only
            // reads the leading key portion; the body is irrelevant here).
            rec.extend_from_slice(&[0u8; 40]);
            btree_insert_full(&mut buf, &rec, &kf, &cmp)
                .unwrap_or_else(|e| panic!("insert #{i} (parent {parent}, {name}): {e}"));
            inserted.push((parent, name));
        }

        // The tree must have grown past a single index level.
        let header = BTreeHeader::read(&buf);
        assert!(
            header.depth >= 3,
            "catalog did not reach depth >= 3 (got {}); test needs a deeper tree to \
             exercise index-node splits",
            header.depth
        );
        assert_eq!(
            header.leaf_records, N,
            "leaf_records header count drifted from inserts"
        );

        // (a) Every leaf key, walked in sibling-chain order, is strictly
        // ascending — the direct symptom of a malformed separator was
        // out-of-order leaves.
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(N as usize);
        super::hfs_common::walk_leaf_records::<(), _>(
            &buf,
            header.first_leaf_node,
            node_size,
            |_n, _r, _o, rec| {
                let kl = BigEndian::read_u16(&rec[0..2]) as usize;
                keys.push(rec[..(2 + kl).min(rec.len())].to_vec());
                None
            },
        );
        assert_eq!(
            keys.len(),
            N as usize,
            "leaf walk recovered {} of {N} records — chain is broken or has cycles",
            keys.len()
        );
        for w in keys.windows(2) {
            assert_eq!(
                cmp(&w[0], &w[1]),
                std::cmp::Ordering::Less,
                "catalog leaves are not strictly ascending — separator/descent corruption"
            );
        }

        // (b) Every inserted record is findable by a root-to-leaf descent, i.e.
        // every index separator routes correctly at every level. (A nested `fn`
        // rather than a closure so it satisfies the `for<'a> Fn(&'a [u8]) ->
        // &'a [u8]` bound `btree_find_record` requires.)
        fn key_extract(rec: &[u8]) -> &[u8] {
            let kl = BigEndian::read_u16(&rec[0..2]) as usize;
            &rec[..(2 + kl).min(rec.len())]
        }
        for (parent, name) in &inserted {
            let search = Fs::build_catalog_key(*parent, name);
            let (found, _chain) = btree_find_record(&buf, &header, &search, &key_extract, &cmp);
            assert!(
                found.is_some(),
                "record (parent {parent}, {name}) not findable by descent — misrouted index"
            );
        }
    }

    #[test]
    fn test_hfsplus_blank_sized_catalog_20k_inserts_fsck_clean() {
        // P2 (docs/hfsplus_btree_growth_plan.md §4c): a fresh HFS+ volume sized
        // to hold the records accepts >= 20k incremental catalog inserts across
        // many parent dirs and stays fsck-clean — the volume-level gate P1
        // deferred. A blank catalog used to be a fixed 4 nodes (exhausting after
        // ~24 files); it is now volume-scaled, and `create_blank_hfsplus_sized`
        // lets this test pin a large catalog into a modest 64 MiB image instead
        // of needing a multi-GiB volume to scale into.
        use super::hfs_common::BTreeHeader;
        type Fs = HfsPlusFilesystem<std::io::Cursor<Vec<u8>>>;

        // 64 MiB volume; catalog pinned to 16 MiB (~4096 nodes @ 4096) — ample
        // headroom for 20k file records at the byte-split's ~69% leaf occupancy.
        let img =
            create_blank_hfsplus_sized(64 * 1024 * 1024, 4096, "PutShuffle", false, 16 << 20, 0);
        let mut fs = Fs::open(std::io::Cursor::new(img), 0).unwrap();
        fs.prepare_for_edit().unwrap();

        // Real directories so threads / valence / folder_count stay fsck-correct.
        const DIRS: u32 = 50;
        let root = fs.root().unwrap();
        let mut dir_ids = Vec::with_capacity(DIRS as usize);
        for d in 0..DIRS {
            let de = fs
                .create_directory(&root, &format!("dir{d:03}"), &Default::default())
                .unwrap();
            dir_ids.push(de.location as u32);
        }
        let base_cnid = fs.vh.next_catalog_id;

        // 20k files in shuffled key order (multiplicative hash is a bijection
        // mod 2^32 — unique names landing mid-leaf, not appended), sprayed across
        // the directories. Insert the catalog records directly: `create_file`
        // snapshots the whole catalog per call for rollback, which would make a
        // 20k-record build O(N * catalog_size). fsck doesn't require per-file
        // thread records (only flags orphaned threads), so — like the classic
        // HFS scaling test — we insert just the file records and fix up counts.
        let n: u32 = 20_000;
        let mut per_dir = vec![0i32; DIRS as usize];
        let empty_fork = ForkData::empty();
        for i in 0..n {
            let hash = i.wrapping_mul(2_654_435_761);
            let d = (hash % DIRS) as usize;
            let name = format!("f{hash:08x}");
            let mut kr = Fs::build_catalog_key(dir_ids[d], &name);
            if !kr.len().is_multiple_of(2) {
                kr.push(0);
            }
            kr.extend_from_slice(&Fs::build_file_record(
                base_cnid + i,
                &empty_fork,
                &empty_fork,
                &[0u8; 4],
                &[0u8; 4],
            ));
            fs.insert_catalog_record(&kr)
                .unwrap_or_else(|e| panic!("insert #{i} into dir{d}: {e}"));
            per_dir[d] += 1;
        }
        // Mirror the bookkeeping the create path maintains so fsck's cross-checks
        // (file_count, next_catalog_id, per-dir valence) pass.
        fs.vh.file_count += n;
        fs.vh.next_catalog_id = base_cnid + n;
        for (d, &cnt) in per_dir.iter().enumerate() {
            fs.update_parent_valence(dir_ids[d], cnt).unwrap();
        }

        // The catalog must have grown several index levels...
        let h = BTreeHeader::read(fs.catalog_data());
        assert!(
            h.depth >= 3,
            "catalog depth {} < 3 after 20k inserts",
            h.depth
        );
        // ...without a premature DiskFull (every insert above succeeded).

        // No IndexSiblingLinkBroken / order / count damage at scale.
        let result = fs.fsck().unwrap().unwrap();
        assert!(
            result.errors.is_empty(),
            "fsck found {} errors after 20k inserts: {:?}",
            result.errors.len(),
            result
                .errors
                .iter()
                .take(8)
                .map(|e| &e.message)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_hfsplus_catalog_grows_map_nodes_past_cap_phase_c() {
        // §4b Phase C (docs/todo_hfsplus_fork_growth.md): once a B-tree fork
        // grows past the header node's bitmap capacity ((node_size-256)*8 =
        // 30,720 nodes @ 4096), a map node (BTNodeKind=2) must be appended to
        // the node-allocation bitmap chain so the new node indices are
        // addressable. Pre-size the catalog just under the cap, then grow a few
        // times to cross it — exercising the map-node append without needing
        // the millions of records a from-scratch fill would take.
        use super::hfs_common::{btree_bitmap_segments, BTreeHeader};
        type Fs = HfsPlusFilesystem<std::io::Cursor<Vec<u8>>>;
        let node_size = 4096usize;
        let header_cap = ((node_size - 256) * 8) as u32; // 30,720

        // Catalog pinned to ~16 nodes below the cap; 180 MiB volume holds it.
        let min_catalog = (header_cap - 16) * node_size as u32;
        let img =
            create_blank_hfsplus_sized(180 * 1024 * 1024, 4096, "MapCap", false, min_catalog, 0);
        let mut fs = Fs::open(std::io::Cursor::new(img), 0).unwrap();
        fs.prepare_for_edit().unwrap();

        let start = BTreeHeader::read(fs.catalog_data()).total_nodes;
        assert!(
            start <= header_cap && start > header_cap - 64,
            "blank catalog {start} not near cap"
        );
        // No map node yet — the whole bitmap lives in the header node.
        assert_eq!(
            btree_bitmap_segments(fs.catalog_data(), node_size).len(),
            1,
            "fresh near-cap catalog should have no map nodes"
        );

        // Grow until we cross the header bitmap cap.
        let mut guard = 0;
        while BTreeHeader::read(fs.catalog_data()).total_nodes <= header_cap {
            fs.grow_btree_fork(BTreeFork::Catalog).unwrap();
            guard += 1;
            assert!(guard < 64, "too many grows to cross the cap");
        }
        let h = BTreeHeader::read(fs.catalog_data());
        assert!(
            h.total_nodes > header_cap,
            "did not cross the cap: {}",
            h.total_nodes
        );
        // A map-node segment must now exist in the bitmap chain.
        let segs = btree_bitmap_segments(fs.catalog_data(), node_size);
        assert!(
            segs.len() >= 2,
            "expected a map-node segment after crossing the cap, got {} segment(s)",
            segs.len()
        );

        // fsck-clean with the map node in place.
        fs.sync_metadata().unwrap();
        let result = fs.fsck().unwrap().unwrap();
        assert!(
            result.errors.is_empty(),
            "fsck after map-node grow: {:?}",
            result
                .errors
                .iter()
                .take(8)
                .map(|e| &e.message)
                .collect::<Vec<_>>()
        );

        // Reopen re-reads the grown fork and the map chain.
        let img = fs.reader.get_ref().clone();
        if let Ok(path) = std::env::var("RB_EMIT_MAPC_IMAGE") {
            std::fs::write(&path, &img).unwrap();
        }
        let fs2 = Fs::open(std::io::Cursor::new(img), 0).unwrap();
        assert_eq!(
            BTreeHeader::read(fs2.catalog_data()).total_nodes,
            h.total_nodes,
            "reopen lost grown nodes"
        );
        assert!(
            btree_bitmap_segments(fs2.catalog_data(), node_size).len() >= 2,
            "reopen lost the map node"
        );
    }

    #[test]
    fn test_hfsplus_catalog_grow_spills_to_overflow_phase_b() {
        // §4b Phase B (docs/todo_hfsplus_fork_growth.md): real `create_file`
        // calls interleave a data-block allocation with each catalog grow, so
        // contiguous tail growth fails and the catalog fork fragments. Once it
        // exhausts its 8 inline extents, further growth spills into the
        // extents-overflow B-tree (keyed by the catalog's reserved CNID 4) and
        // is persisted by the overflow-aware write path. The volume stays
        // readable + fsck-clean, and a reopen re-reads the > 8-extent fork.
        type Fs = HfsPlusFilesystem<std::io::Cursor<Vec<u8>>>;
        let img = create_blank_hfsplus_sized(16 * 1024 * 1024, 4096, "SpillVol", false, 0, 0);
        let mut fs = Fs::open(std::io::Cursor::new(img), 0).unwrap();
        fs.prepare_for_edit().unwrap();
        let root = fs.root().unwrap();
        let mut dirs = Vec::new();
        for d in 0..20 {
            dirs.push(
                fs.create_directory(&root, &format!("d{d}"), &Default::default())
                    .unwrap(),
            );
        }
        const N: u32 = 1200;
        for i in 0..N {
            let d = &dirs[(i % 20) as usize];
            let mut data = std::io::Cursor::new(vec![b'x']);
            fs.create_file(
                d,
                &format!("f{i:05}.txt"),
                &mut data,
                1,
                &Default::default(),
            )
            .unwrap_or_else(|e| panic!("create_file #{i} (grow+spill should cover it): {e}"));
        }

        // The catalog fork saturated its 8 inline extents and spilled the rest
        // into the extents-overflow B-tree.
        let inline: u32 = fs
            .vh
            .catalog_file
            .extents
            .iter()
            .map(|e| e.block_count)
            .sum();
        let inline_count = fs
            .vh
            .catalog_file
            .extents
            .iter()
            .filter(|e| e.block_count != 0)
            .count();
        assert_eq!(
            inline_count, 8,
            "catalog should have filled all 8 inline extents"
        );
        assert!(
            fs.vh.catalog_file.total_blocks > inline,
            "catalog total_blocks {} should exceed inline-extent blocks {inline} (overflow used)",
            fs.vh.catalog_file.total_blocks
        );

        fs.sync_metadata().unwrap();
        let result = fs.fsck().unwrap().unwrap();
        assert!(
            result.errors.is_empty(),
            "fsck after overflow spill: {:?}",
            result
                .errors
                .iter()
                .take(8)
                .map(|e| &e.message)
                .collect::<Vec<_>>()
        );

        // Reopen re-reads the > 8-extent catalog fork (inline + overflow) and
        // every file is still enumerable.
        let img = fs.reader.get_ref().clone();
        if let Ok(path) = std::env::var("RB_EMIT_SPILL_IMAGE") {
            std::fs::write(&path, &img).unwrap();
        }
        let mut fs2 = Fs::open(std::io::Cursor::new(img), 0).unwrap();
        let r2 = fs2.root().unwrap();
        let mut total = 0;
        for de in fs2.list_directory(&r2).unwrap() {
            if de.is_directory() {
                total += fs2.list_directory(&de).unwrap().len();
            }
        }
        assert_eq!(total as u32, N, "reopen lost files: {total} != {N}");
    }

    #[test]
    fn test_hfsplus_catalog_grows_fork_on_full_phase_a() {
        // §4b Phase A (docs/todo_hfsplus_fork_growth.md): a volume whose catalog
        // fork is *too small* for its eventual record count grows the fork in
        // place through live inserts — no spurious DiskFull while the volume has
        // free blocks — and stays fsck-clean. Contiguous tail growth keeps the
        // catalog fork at a single extent. A reopen re-reads the grown fork.
        use super::hfs_common::BTreeHeader;
        type Fs = HfsPlusFilesystem<std::io::Cursor<Vec<u8>>>;

        // 16 MiB volume, minimal (volume-scaled, ~21-node) catalog. Inserting
        // far more records than that forces many grows.
        let img = create_blank_hfsplus_sized(16 * 1024 * 1024, 4096, "GrowVol", false, 0, 0);
        let mut fs = Fs::open(std::io::Cursor::new(img), 0).unwrap();
        fs.prepare_for_edit().unwrap();

        let start_nodes = BTreeHeader::read(fs.catalog_data()).total_nodes;

        const DIRS: u32 = 20;
        let root = fs.root().unwrap();
        let mut dir_ids = Vec::with_capacity(DIRS as usize);
        for d in 0..DIRS {
            let de = fs
                .create_directory(&root, &format!("dir{d:03}"), &Default::default())
                .unwrap();
            dir_ids.push(de.location as u32);
        }
        let base_cnid = fs.vh.next_catalog_id;

        let n: u32 = 6_000;
        let mut per_dir = vec![0i32; DIRS as usize];
        let empty_fork = ForkData::empty();
        for i in 0..n {
            let hash = i.wrapping_mul(2_654_435_761);
            let d = (hash % DIRS) as usize;
            let name = format!("f{hash:08x}");
            let mut kr = Fs::build_catalog_key(dir_ids[d], &name);
            if !kr.len().is_multiple_of(2) {
                kr.push(0);
            }
            kr.extend_from_slice(&Fs::build_file_record(
                base_cnid + i,
                &empty_fork,
                &empty_fork,
                &[0u8; 4],
                &[0u8; 4],
            ));
            fs.insert_catalog_record(&kr)
                .unwrap_or_else(|e| panic!("insert #{i} into dir{d} (grow should cover it): {e}"));
            per_dir[d] += 1;
        }
        fs.vh.file_count += n;
        fs.vh.next_catalog_id = base_cnid + n;
        for (d, &cnt) in per_dir.iter().enumerate() {
            fs.update_parent_valence(dir_ids[d], cnt).unwrap();
        }

        // The fork grew (each grow adds >= 8 nodes; >= 2 grows expected here).
        let end_nodes = BTreeHeader::read(fs.catalog_data()).total_nodes;
        assert!(
            end_nodes >= start_nodes + 16,
            "catalog total_nodes only went {start_nodes} -> {end_nodes}; expected >= 2 grows"
        );
        // Contiguous tail growth keeps the catalog fork at a single extent.
        let cat_extents = fs
            .vh
            .catalog_file
            .extents
            .iter()
            .filter(|e| e.block_count != 0)
            .count();
        assert_eq!(
            cat_extents, 1,
            "contiguous tail growth should keep the catalog fork at 1 extent, got {cat_extents}"
        );

        // Persist the grown fork, bitmap, and VH, then fsck the result.
        fs.sync_metadata().unwrap();

        // fsck-clean after all the grows.
        let result = fs.fsck().unwrap().unwrap();
        assert!(
            result.errors.is_empty(),
            "fsck found {} errors after grow: {:?}",
            result.errors.len(),
            result
                .errors
                .iter()
                .take(8)
                .map(|e| &e.message)
                .collect::<Vec<_>>()
        );

        // Reopen re-reads the grown fork (proves the fork extents + header
        // persisted) and every record is still findable.
        let img = fs.reader.get_ref().clone();
        // Optionally emit the grown image so a Mac can fsck_hfs it (the real
        // §4b acceptance test): set RB_EMIT_GROW_IMAGE=/path.
        if let Ok(path) = std::env::var("RB_EMIT_GROW_IMAGE") {
            std::fs::write(&path, &img).unwrap();
        }
        let mut fs2 = Fs::open(std::io::Cursor::new(img), 0).unwrap();
        let reopened_nodes = BTreeHeader::read(fs2.catalog_data()).total_nodes;
        assert_eq!(reopened_nodes, end_nodes, "reopen lost grown nodes");
        let r2 = fs2.root().unwrap();
        let dirs = fs2.list_directory(&r2).unwrap();
        assert_eq!(dirs.len(), DIRS as usize, "reopen lost directories");
    }

    #[test]
    fn test_hfsplus_extents_overflow_grows_multilevel() {
        // P3 (docs/hfsplus_btree_growth_plan.md): the extents-overflow B-tree —
        // 2-byte ("big") keys with *fixed* 10-byte index separators — must split
        // past a single leaf level. It was pinned depth-1 because the shared
        // helpers forced the classic key shape; it now threads
        // `BTreeKeyFormat::HFSPLUS_EXTENTS`. Driven through the same
        // `btree_insert_full` machinery `insert_extents_overflow_record` uses.
        use super::hfs_common::{
            btree_find_record, btree_insert_full, BTreeHeader, BTreeKeyFormat,
        };
        type Fs = HfsPlusFilesystem<std::io::Cursor<Vec<u8>>>;

        let node_size = 512usize;
        let total_nodes = 1800u32;
        let mut buf = vec![0u8; total_nodes as usize * node_size];
        write_blank_btree_header_node(
            &mut buf[0..node_size],
            node_size,
            0,
            total_nodes,
            total_nodes - 2,
            /* max_key_len= */ 10,
            /* key_compare_type= */ 0,
        );
        write_empty_leaf_node(&mut buf[node_size..2 * node_size]);

        let kf = BTreeKeyFormat::HFSPLUS_EXTENTS;
        // 600 overflow records keyed by distinct shuffled fileIDs (the multiplier
        // is odd, so it's a bijection mod 2^32 — distinct i give distinct keys
        // that land mid-leaf, not appended).
        const N: u32 = 600;
        let mut ids: Vec<u32> = Vec::with_capacity(N as usize);
        for i in 0..N {
            let file_id = i.wrapping_mul(2_654_435_761);
            let rec = Fs::build_extents_overflow_record(file_id, 0, 8, &[]);
            btree_insert_full(&mut buf, &rec, &kf, &Fs::extents_compare)
                .unwrap_or_else(|e| panic!("extents insert #{i} (file {file_id}): {e}"));
            ids.push(file_id);
        }

        let header = BTreeHeader::read(&buf);
        assert!(
            header.depth >= 2,
            "extents-overflow tree stayed depth {} — split past a leaf level failed",
            header.depth
        );
        assert_eq!(header.leaf_records, N);

        let mut recs: Vec<Vec<u8>> = Vec::with_capacity(N as usize);
        super::hfs_common::walk_leaf_records::<(), _>(
            &buf,
            header.first_leaf_node,
            node_size,
            |_n, _r, _o, r| {
                recs.push(r.to_vec());
                None
            },
        );
        assert_eq!(recs.len(), N as usize, "leaf walk lost records");
        for w in recs.windows(2) {
            assert_eq!(
                Fs::extents_compare(&w[0], &w[1]),
                std::cmp::Ordering::Less,
                "extents-overflow leaves out of order — bad separator"
            );
        }

        // The extents key portion is always the fixed 12 bytes (2-byte len + 10).
        fn ext_key(rec: &[u8]) -> &[u8] {
            &rec[..12.min(rec.len())]
        }
        let ext_cmp = |a: &[u8], b: &[u8]| Fs::extents_compare(a, b);
        for &file_id in &ids {
            let search = Fs::build_extents_overflow_record(file_id, 0, 8, &[]);
            let (found, _) = btree_find_record(&buf, &header, &search, &ext_key, &ext_cmp);
            assert!(
                found.is_some(),
                "extents record for file {file_id} not findable"
            );
        }
    }

    #[test]
    fn test_hfsplus_attributes_grows_multilevel() {
        // P3: the attributes B-tree — 2-byte keys with *variable*-length index
        // separators — splits past one leaf level via
        // `BTreeKeyFormat::HFSPLUS_ATTRIBUTES`, the same path `insert_xattr_record`
        // uses. (P1 wired the descriptor in; this proves the attribute key layout,
        // distinct from the catalog's, routes correctly at every level.)
        use super::hfs_common::{
            btree_find_record, btree_insert_full, BTreeHeader, BTreeKeyFormat,
        };
        use byteorder::{BigEndian, ByteOrder};
        type Fs = HfsPlusFilesystem<std::io::Cursor<Vec<u8>>>;

        let node_size = 512usize;
        let total_nodes = 1800u32;
        let mut buf = vec![0u8; total_nodes as usize * node_size];
        write_blank_btree_header_node(
            &mut buf[0..node_size],
            node_size,
            0,
            total_nodes,
            total_nodes - 2,
            /* max_key_len= */ 264,
            /* key_compare_type= */ 0,
        );
        write_empty_leaf_node(&mut buf[node_size..2 * node_size]);

        let kf = BTreeKeyFormat::HFSPLUS_ATTRIBUTES;
        const N: u32 = 600;
        let value = [0xABu8; 8];
        let mut cnids: Vec<u32> = Vec::with_capacity(N as usize);
        for i in 0..N {
            let cnid = i.wrapping_mul(2_654_435_761);
            let rec = Fs::build_inline_attr_record(cnid, "xattr", &value);
            btree_insert_full(&mut buf, &rec, &kf, &Fs::attr_compare)
                .unwrap_or_else(|e| panic!("attr insert #{i} (cnid {cnid}): {e}"));
            cnids.push(cnid);
        }

        let header = BTreeHeader::read(&buf);
        assert!(
            header.depth >= 2,
            "attributes tree stayed depth {} — split past a leaf level failed",
            header.depth
        );
        assert_eq!(header.leaf_records, N);

        let mut recs: Vec<Vec<u8>> = Vec::with_capacity(N as usize);
        super::hfs_common::walk_leaf_records::<(), _>(
            &buf,
            header.first_leaf_node,
            node_size,
            |_n, _r, _o, r| {
                recs.push(r.to_vec());
                None
            },
        );
        assert_eq!(recs.len(), N as usize, "leaf walk lost records");
        for w in recs.windows(2) {
            assert_eq!(
                Fs::attr_compare(&w[0], &w[1]),
                std::cmp::Ordering::Less,
                "attributes leaves out of order — bad separator"
            );
        }

        fn attr_key(rec: &[u8]) -> &[u8] {
            let kl = BigEndian::read_u16(&rec[0..2]) as usize;
            &rec[..(2 + kl).min(rec.len())]
        }
        let attr_cmp = |a: &[u8], b: &[u8]| Fs::attr_compare(a, b);
        for &cnid in &cnids {
            let search = Fs::build_inline_attr_record(cnid, "xattr", &value);
            let (found, _) = btree_find_record(&buf, &header, &search, &attr_key, &attr_cmp);
            assert!(found.is_some(), "attr record for cnid {cnid} not findable");
        }
    }

    #[test]
    fn test_hfsplus_fragmented_file_splits_extents_overflow_btree_real_path() {
        // P3 real-path integration: a heavily fragmented file produces enough
        // extents-overflow records to split that B-tree past a single leaf level
        // through the real `insert_extents_overflow_record`, and the file reads
        // back byte-for-byte through the resulting multi-level tree. (Read-back,
        // not fsck, because the all-used-then-free-singletons bitmap trick used to
        // force fragmentation leaves the bitmap deliberately over-allocated; the
        // point here is the overflow B-tree's split + read path, which P3 enabled.)
        type Fs = HfsPlusFilesystem<std::io::Cursor<Vec<u8>>>;
        // Extents-overflow tree pinned to 256 KiB (64 nodes @ 4096) so it has room
        // to split — the 4-node default could not.
        let img = create_blank_hfsplus_sized(32 << 20, 4096, "Frag", false, 0, 256 << 10);
        let mut fs = Fs::open(std::io::Cursor::new(img), 0).unwrap();
        fs.prepare_for_edit().unwrap();

        // Force maximal fragmentation: mark every block used, then free isolated
        // single blocks (every other block) in the user region. A file of N blocks
        // must then use N one-block extents: 8 inline + (N-8) overflow extents =
        // ceil((N-8)/8) overflow records. 520 blocks -> 64 overflow records, well
        // past one 4096-byte extents leaf (~52 records), so the tree must split.
        fs.ensure_bitmap().unwrap();
        let block_size = fs.vh.block_size as usize;
        let n_blocks: u32 = 520;
        let first_free = fs.vh.next_allocation;
        {
            let bitmap = fs.bitmap.as_mut().unwrap();
            for b in bitmap.iter_mut() {
                *b = 0xFF;
            }
            let mut blk = first_free + 1;
            for _ in 0..n_blocks {
                bitmap_clear_bit_be(bitmap, blk);
                blk += 2; // a used block between each free one -> singletons
            }
        }
        fs.vh.free_blocks = n_blocks;

        let payload: Vec<u8> = (0..(n_blocks as usize * block_size))
            .map(|i| (i % 251) as u8)
            .collect();
        let mut reader = std::io::Cursor::new(payload.clone());
        let root = fs.root().unwrap();
        let entry = fs
            .create_file(
                &root,
                "frag.bin",
                &mut reader,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("fragmented file create should succeed via a splitting overflow B-tree");

        // The extents-overflow tree must have grown past a single leaf level.
        let depth =
            super::hfs_common::BTreeHeader::read(fs.extents_overflow_data.as_ref().unwrap()).depth;
        assert!(
            depth >= 2,
            "extents-overflow tree depth {depth} < 2 — 64 overflow records did not split it"
        );

        // The data fork is the expected single-block-extent shape...
        let cnid = entry.location as u32;
        let (data_fork, _rsrc) = fs.find_file_by_id(cnid).unwrap();
        assert_eq!(data_fork.total_blocks, n_blocks);

        // ...and reads back byte-for-byte through the multi-level overflow tree.
        let got = fs.read_file(&entry, payload.len() + 1).unwrap();
        assert_eq!(got.len(), payload.len());
        assert_eq!(
            got, payload,
            "fragmented read-back mismatch through split overflow tree"
        );
    }

    #[test]
    fn test_hfsplus_catalog_shuffled_inserts_pack_densely() {
        // P5 (docs/hfsplus_btree_growth_plan.md §4d): with the B*-style rotation
        // now on the HFS+ catalog insert path (insert_catalog_record ->
        // btree_insert_full), shuffled multi-dir inserts must pack leaves to
        // ~80%+ occupancy, matching the classic-HFS result — not the ~69% a plain
        // split leaves. Measured through the same btree_insert_full the live path
        // uses, with the HFSPLUS_CATALOG (variable-key) format.
        use super::hfs_common::{btree_insert_full, BTreeHeader, BTreeKeyFormat};
        use byteorder::{BigEndian, ByteOrder};
        type Fs = HfsPlusFilesystem<std::io::Cursor<Vec<u8>>>;

        let node_size = 512usize;
        let total_nodes = 1800u32;
        let mut buf = vec![0u8; total_nodes as usize * node_size];
        write_blank_btree_header_node(
            &mut buf[0..node_size],
            node_size,
            0,
            total_nodes,
            total_nodes - 2,
            516,
            KEY_COMPARE_CASE_FOLDING,
        );
        write_empty_leaf_node(&mut buf[node_size..2 * node_size]);

        let kf = BTreeKeyFormat::HFSPLUS_CATALOG;
        let cmp = |a: &[u8], b: &[u8]| Fs::catalog_compare(a, b, false);
        const DIRS: u32 = 40;
        const N: u32 = 3000;
        for i in 0..N {
            let hash = i.wrapping_mul(2_654_435_761);
            let parent = 16 + (hash % DIRS);
            let name = format!("f{hash:08x}");
            let mut rec = Fs::build_catalog_key(parent, &name);
            rec.extend_from_slice(&[0u8; 40]);
            btree_insert_full(&mut buf, &rec, &kf, &cmp)
                .unwrap_or_else(|e| panic!("insert #{i}: {e}"));
        }

        // Average leaf occupancy across the whole leaf chain.
        let header = BTreeHeader::read(&buf);
        assert!(
            header.depth >= 3,
            "need a multi-level tree to judge packing"
        );
        let mut used_total = 0usize;
        let mut leaves = 0usize;
        let mut node = header.first_leaf_node;
        let mut seen = std::collections::HashSet::new();
        while node != 0 && seen.insert(node) {
            let off = node as usize * node_size;
            let num = BigEndian::read_u16(&buf[off + 10..off + 12]) as usize;
            // Records occupy 14..free_off; the offset table holds num+1 u16s.
            let free_off_pos = off + node_size - 2 * (num + 1);
            let free_off = BigEndian::read_u16(&buf[free_off_pos..free_off_pos + 2]) as usize;
            used_total += free_off + 2 * (num + 1);
            leaves += 1;
            node = BigEndian::read_u32(&buf[off..off + 4]); // forward link
        }
        // ~0.84 with the rotation (was ~0.69 with plain split), matching classic.
        let occupancy = used_total as f64 / (leaves * node_size) as f64;
        assert!(
            occupancy >= 0.80,
            "shuffled multi-dir leaf occupancy {occupancy:.3} (<0.80) over {leaves} leaves — \
             the rotation is not packing densely"
        );
    }
}
