//! Source-side snapshot types for the classic-HFS reformat-and-copy pipeline.
//!
//! Step 2 of docs/hfs_expand_block_size.md. Nothing here mutates the source
//! volume; these structs exist to capture every byte of metadata that must
//! survive the copy so later steps can replay it onto a freshly-formatted
//! target volume.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Seek, SeekFrom, Write};

use byteorder::{BigEndian, ByteOrder};

use super::entry::FileEntry;
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem,
};
use super::hfs::{HfsExtDescriptor, HfsFilesystem};
use crate::fs::FilesystemError;
use crate::partition::apm::{Apm, ApmPartitionEntry};

/// Classic-HFS catalog record type byte.
const CATALOG_DIR: i8 = 1;
const CATALOG_FILE: i8 = 2;

/// Volume-level metadata captured from the source.
#[derive(Debug, Clone)]
pub struct SourceVolumeSnapshot {
    /// UTF-8 decoding of the volume name (Pascal string in Mac Roman on disk).
    pub volume_name: String,
    /// Raw Mac Roman bytes of the volume name (up to 27 bytes).
    pub volume_name_raw: Vec<u8>,
    /// drCrDate — volume create date (HFS epoch seconds).
    pub create_date: u32,
    /// drLsMod — volume modify date.
    pub modify_date: u32,
    /// drVolBkUp — volume backup date.
    pub backup_date: u32,
    /// drFndrInfo — 8 × u32 Finder info. `finder_info[0]` is the blessed
    /// System Folder CNID; other slots are also copied verbatim with any
    /// CNID remap applied by the caller.
    pub finder_info: [u32; 8],
    /// Sectors 0..1 of the partition, 1024 bytes verbatim.
    pub boot_blocks: [u8; 1024],
    /// Source allocation block size (drAlBlkSiz) — informational only.
    pub block_size: u32,
    /// Source total allocation blocks (drNmAlBlks) — informational only.
    pub total_blocks: u16,
}

/// Per-file metadata captured from a classic-HFS catalog file record.
#[derive(Debug, Clone)]
pub struct SourceFileSnapshot {
    pub name: String,
    pub name_raw: Vec<u8>,
    /// Source CNID (filFlNum). Targets get fresh CNIDs so this is for
    /// building the source→target CNID map only.
    pub cnid: u32,
    pub parent_cnid: u32,
    /// filFlags byte (offset 2) — bit 0x01 = locked.
    pub flags: u8,
    /// 16-byte FInfo (fdType, fdCreator, fdFlags, fdLocation, fdFldr).
    pub finfo: [u8; 16],
    /// 16-byte FXInfo (fdIconID, fdScript, fdXFlags, fdComment, fdPutAway).
    pub fxinfo: [u8; 16],
    pub create_date: u32,
    pub modify_date: u32,
    pub backup_date: u32,
    pub data_size: u32,
    pub data_extents: [HfsExtDescriptor; 3],
    pub rsrc_size: u32,
    pub rsrc_extents: [HfsExtDescriptor; 3],
}

impl SourceFileSnapshot {
    /// True if the file's locked bit is set (filFlags bit 0x01).
    pub fn is_locked(&self) -> bool {
        self.flags & 0x01 != 0
    }
}

/// Per-directory metadata captured from a classic-HFS catalog dir record.
#[derive(Debug, Clone)]
pub struct SourceDirSnapshot {
    pub name: String,
    pub name_raw: Vec<u8>,
    /// Source CNID (dirDirID).
    pub cnid: u32,
    pub parent_cnid: u32,
    /// 16-byte DInfo (frRect, frFlags, frLocation, frView).
    pub dinfo: [u8; 16],
    /// 16-byte DXInfo (frScroll, frOpenChain, frScript, frXFlags, frComment, frPutAway).
    pub dxinfo: [u8; 16],
    pub create_date: u32,
    pub modify_date: u32,
    pub backup_date: u32,
}

/// Full capture of a source HFS volume: volume metadata plus every directory
/// and file record in the catalog.
///
/// Catalog *thread* records are not captured — the target writer recreates
/// threads from scratch.
#[derive(Debug, Clone)]
pub struct SourceCatalogSnapshot {
    pub volume: SourceVolumeSnapshot,
    pub dirs: Vec<SourceDirSnapshot>,
    pub files: Vec<SourceFileSnapshot>,
}

impl SourceVolumeSnapshot {
    /// Capture volume-level metadata from an opened source filesystem. The
    /// source is only read, never mutated.
    pub fn capture<R: Read + Seek>(fs: &mut HfsFilesystem<R>) -> Result<Self, FilesystemError> {
        let boot_blocks = fs.read_boot_blocks()?;
        let mdb = fs.mdb();
        // drVolBkUp lives at offset 64 of the MDB sector — not parsed into
        // the struct, so read it from the preserved raw sector.
        let backup_date = BigEndian::read_u32(&mdb.raw_sector[64..68]);
        Ok(SourceVolumeSnapshot {
            volume_name: mdb.volume_name.clone(),
            volume_name_raw: mdb.volume_name_raw.clone(),
            create_date: mdb.create_date,
            modify_date: mdb.modify_date,
            backup_date,
            finder_info: mdb.finder_info,
            boot_blocks,
            block_size: mdb.block_size,
            total_blocks: mdb.total_blocks,
        })
    }
}

impl SourceCatalogSnapshot {
    /// Capture volume + all directory + all file records. Uses only the
    /// primary catalog B-tree; index nodes are ignored and only leaf records
    /// are visited. Unknown record types (threads, corrupt) are skipped.
    pub fn capture<R: Read + Seek>(fs: &mut HfsFilesystem<R>) -> Result<Self, FilesystemError> {
        let volume = SourceVolumeSnapshot::capture(fs)?;
        let (dirs, files) = walk_catalog(fs.catalog_data());
        Ok(SourceCatalogSnapshot {
            volume,
            dirs,
            files,
        })
    }
}

/// Walk leaf records of the classic-HFS catalog B-tree and split them into
/// dir and file snapshots. Thread records and malformed records are skipped.
fn walk_catalog(catalog: &[u8]) -> (Vec<SourceDirSnapshot>, Vec<SourceFileSnapshot>) {
    let mut dirs = Vec::new();
    let mut files = Vec::new();

    if catalog.len() < 512 {
        return (dirs, files);
    }
    let node_size = BigEndian::read_u16(&catalog[32..34]) as usize;
    if node_size == 0 || catalog.len() < node_size {
        return (dirs, files);
    }
    // BTHeaderRec.firstLeafNode is at offset 14 within the header record,
    // which starts 14 bytes into node 0 → absolute offset 24..28.
    let mut node_idx = BigEndian::read_u32(&catalog[24..28]);

    while node_idx != 0 {
        let offset = node_idx as usize * node_size;
        if offset + node_size > catalog.len() {
            break;
        }
        let node = &catalog[offset..offset + node_size];
        let next_node = BigEndian::read_u32(&node[0..4]);
        let kind = node[8] as i8;
        if kind != -1 {
            break;
        }
        let num_records = BigEndian::read_u16(&node[10..12]) as usize;

        for i in 0..num_records {
            let offset_pos = node_size - 2 * (i + 1);
            if offset_pos + 2 > node.len() {
                break;
            }
            let rec_offset = BigEndian::read_u16(&node[offset_pos..offset_pos + 2]) as usize;
            if rec_offset + 7 > node.len() {
                continue;
            }
            let key_len = node[rec_offset] as usize;
            if key_len < 6 || rec_offset + 1 + key_len > node.len() {
                continue;
            }
            let parent_id = BigEndian::read_u32(&node[rec_offset + 2..rec_offset + 6]);
            let name_len = node[rec_offset + 6] as usize;
            let name_end = rec_offset + 7 + name_len;
            if name_end > node.len() {
                continue;
            }
            let name_raw = node[rec_offset + 7..name_end].to_vec();

            let mut rec_data_offset = rec_offset + 1 + key_len;
            if rec_data_offset % 2 != 0 {
                rec_data_offset += 1;
            }
            if rec_data_offset + 2 > node.len() {
                continue;
            }
            let rec_type = node[rec_data_offset] as i8;

            match rec_type {
                CATALOG_DIR => {
                    if rec_data_offset + 70 > node.len() {
                        continue;
                    }
                    let rec = &node[rec_data_offset..rec_data_offset + 70];
                    let cnid = BigEndian::read_u32(&rec[6..10]);
                    let create_date = BigEndian::read_u32(&rec[10..14]);
                    let modify_date = BigEndian::read_u32(&rec[14..18]);
                    let backup_date = BigEndian::read_u32(&rec[18..22]);
                    let mut dinfo = [0u8; 16];
                    dinfo.copy_from_slice(&rec[22..38]);
                    let mut dxinfo = [0u8; 16];
                    dxinfo.copy_from_slice(&rec[38..54]);
                    dirs.push(SourceDirSnapshot {
                        name: super::hfs::mac_roman_to_utf8(&name_raw),
                        name_raw,
                        cnid,
                        parent_cnid: parent_id,
                        dinfo,
                        dxinfo,
                        create_date,
                        modify_date,
                        backup_date,
                    });
                }
                CATALOG_FILE => {
                    if rec_data_offset + 102 > node.len() {
                        continue;
                    }
                    let rec = &node[rec_data_offset..rec_data_offset + 102];
                    let flags = rec[2];
                    let mut finfo = [0u8; 16];
                    finfo.copy_from_slice(&rec[4..20]);
                    let cnid = BigEndian::read_u32(&rec[20..24]);
                    let data_size = BigEndian::read_u32(&rec[26..30]);
                    let rsrc_size = BigEndian::read_u32(&rec[36..40]);
                    let create_date = BigEndian::read_u32(&rec[44..48]);
                    let modify_date = BigEndian::read_u32(&rec[48..52]);
                    let backup_date = BigEndian::read_u32(&rec[52..56]);
                    let mut fxinfo = [0u8; 16];
                    fxinfo.copy_from_slice(&rec[56..72]);
                    let mut data_extents = [HfsExtDescriptor {
                        start_block: 0,
                        block_count: 0,
                    }; 3];
                    for j in 0..3 {
                        data_extents[j] = HfsExtDescriptor::parse(&rec[74 + j * 4..78 + j * 4]);
                    }
                    let mut rsrc_extents = [HfsExtDescriptor {
                        start_block: 0,
                        block_count: 0,
                    }; 3];
                    for j in 0..3 {
                        rsrc_extents[j] = HfsExtDescriptor::parse(&rec[86 + j * 4..90 + j * 4]);
                    }
                    files.push(SourceFileSnapshot {
                        name: super::hfs::mac_roman_to_utf8(&name_raw),
                        name_raw,
                        cnid,
                        parent_cnid: parent_id,
                        flags,
                        finfo,
                        fxinfo,
                        create_date,
                        modify_date,
                        backup_date,
                        data_size,
                        data_extents,
                        rsrc_size,
                        rsrc_extents,
                    });
                }
                _ => {}
            }
        }

        node_idx = next_node;
    }

    (dirs, files)
}

/// Summary of a `clone_hfs_volume` run. `errors` and `skipped` are populated
/// only for non-fatal per-entry issues; fatal failures bubble up through the
/// `Result` instead.
#[derive(Debug, Default)]
pub struct CloneReport {
    pub files_copied: u64,
    pub dirs_copied: u64,
    pub data_bytes_copied: u64,
    pub rsrc_bytes_copied: u64,
    /// (path, reason) for entries the cloner deliberately skipped.
    pub skipped: Vec<(String, String)>,
    /// (path, reason) for non-fatal errors that allowed the clone to continue.
    pub errors: Vec<(String, String)>,
}

/// Walk every directory and file in `source` and replicate them onto `target`,
/// preserving Finder metadata, dates, locked state, resource forks, and the
/// blessed-system-folder pointer.
///
/// Pre-conditions:
/// - `target` is freshly built (empty catalog except for the root directory)
///   and large enough to hold every file in `source`.
/// - `source` is opened read-only; this function never mutates it.
///
/// Post-conditions on success:
/// - Every dir/file from `source` exists at the same path on `target`.
/// - Volume name was set when `target` was built; volume dates, boot blocks,
///   and `drFndrInfo` (with CNID remap applied to slots that look like CNIDs)
///   are copied from `source`.
/// - `target.sync_metadata()` has been called.
pub fn clone_hfs_volume<RS, RT>(
    source: &mut HfsFilesystem<RS>,
    target: &mut HfsFilesystem<RT>,
) -> Result<CloneReport, FilesystemError>
where
    RS: Read + Seek + Send,
    RT: Read + Write + Seek + Send,
{
    let snapshot = SourceCatalogSnapshot::capture(source)?;
    let mut report = CloneReport::default();

    // Volume-level metadata. The volume name was supplied when `target` was
    // created; everything else is staged here and flushed by sync_metadata.
    target.set_volume_dates(
        snapshot.volume.create_date,
        snapshot.volume.modify_date,
        snapshot.volume.backup_date,
    );
    target.set_boot_blocks(&snapshot.volume.boot_blocks);

    // Index source dirs/files by parent CNID so a BFS from the root visits
    // parents before children regardless of catalog ordering.
    let mut dirs_by_parent: HashMap<u32, Vec<&SourceDirSnapshot>> = HashMap::new();
    for d in &snapshot.dirs {
        if d.cnid == 2 {
            continue; // root handled separately
        }
        dirs_by_parent.entry(d.parent_cnid).or_default().push(d);
    }
    let mut files_by_parent: HashMap<u32, Vec<&SourceFileSnapshot>> = HashMap::new();
    for f in &snapshot.files {
        files_by_parent.entry(f.parent_cnid).or_default().push(f);
    }

    // Map source CNID → target CNID. Both volumes use CNID 2 for the root.
    let mut cnid_map: HashMap<u32, u32> = HashMap::new();
    cnid_map.insert(2, 2);

    // Map source CNID → target FileEntry. Used as the parent argument when
    // creating children.
    let target_root = target.root()?;
    let mut entry_map: HashMap<u32, FileEntry> = HashMap::new();
    entry_map.insert(2, target_root.clone());

    // Apply the source root's DInfo / dates to the target's pre-existing root.
    if let Some(root_src) = snapshot.dirs.iter().find(|d| d.cnid == 2) {
        target.set_directory_finder_info(&target_root, root_src.dinfo, root_src.dxinfo)?;
        target.set_dates(
            &target_root,
            root_src.create_date,
            root_src.modify_date,
            root_src.backup_date,
        )?;
    }

    let mut queue: VecDeque<u32> = VecDeque::new();
    queue.push_back(2);

    while let Some(src_parent_cnid) = queue.pop_front() {
        let parent_entry = entry_map.get(&src_parent_cnid).cloned().ok_or_else(|| {
            FilesystemError::InvalidData(format!(
                "clone: missing parent entry for source CNID {src_parent_cnid}"
            ))
        })?;

        // Directories first so files can land in them, but ordering doesn't
        // actually matter — they share the same parent.
        if let Some(children) = dirs_by_parent.remove(&src_parent_cnid) {
            for d in children {
                let new_dir = target.create_directory(
                    &parent_entry,
                    &d.name,
                    &CreateDirectoryOptions::default(),
                )?;
                target.set_directory_finder_info(&new_dir, d.dinfo, d.dxinfo)?;
                target.set_dates(&new_dir, d.create_date, d.modify_date, d.backup_date)?;
                cnid_map.insert(d.cnid, new_dir.location as u32);
                entry_map.insert(d.cnid, new_dir);
                report.dirs_copied += 1;
                queue.push_back(d.cnid);
            }
        }

        if let Some(children) = files_by_parent.remove(&src_parent_cnid) {
            for f in children {
                clone_one_file(source, target, &parent_entry, f, &mut cnid_map, &mut report)?;
            }
        }
    }

    // Volume Finder info: copy all 8 ints, remapping any value that matches
    // a known source CNID. drFndrInfo[0] is the blessed System Folder, [3] is
    // typically the OS folder, [5] the macOS X System file. We don't know
    // which slots hold CNIDs vs flags, so the "matches a known source CNID"
    // heuristic catches the right ones without disturbing flag fields.
    //
    // Slots 6 and 7 form the volume's "Finder ID" (`fndrInfo[6..8]` packed as
    // a u64) — Finder uses it to recognise a previously-seen disk across
    // mounts. Carrying the source's ID forward makes Mac OS treat the clone
    // as "the source disk, modified outside our knowledge" and run Disk
    // First Aid on first mount. Zeroing them lets Mac OS generate a fresh
    // ID on first mount, so the clone presents as a new disk and skips the
    // dirty-volume check.
    let known_source_cnids: HashSet<u32> = cnid_map.keys().copied().collect();
    let mut new_finder_info = [0u8; 32];
    for i in 0..8 {
        if i == 6 || i == 7 {
            continue; // leave Finder volume ID zeroed
        }
        let src_val = snapshot.volume.finder_info[i];
        let mapped = if known_source_cnids.contains(&src_val) {
            *cnid_map.get(&src_val).unwrap()
        } else {
            src_val
        };
        BigEndian::write_u32(&mut new_finder_info[i * 4..(i + 1) * 4], mapped);
    }
    target.set_volume_finder_info(&new_finder_info);

    target.sync_metadata()?;

    Ok(report)
}

/// Read one file's data + resource forks from source and write them to target,
/// then stamp Finder metadata, dates, and the locked bit.
fn clone_one_file<RS, RT>(
    source: &mut HfsFilesystem<RS>,
    target: &mut HfsFilesystem<RT>,
    parent_entry: &FileEntry,
    f: &SourceFileSnapshot,
    cnid_map: &mut HashMap<u32, u32>,
    report: &mut CloneReport,
) -> Result<(), FilesystemError>
where
    RS: Read + Seek + Send,
    RT: Read + Write + Seek + Send,
{
    let mut source_entry = FileEntry::new_file(
        f.name.clone(),
        String::new(),
        f.data_size as u64,
        f.cnid as u64,
    );
    if f.rsrc_size > 0 {
        source_entry.resource_fork_size = Some(f.rsrc_size as u64);
    }

    // Read entire data fork. HFS files top out at u32 byte size and HFS
    // volumes are <= 2 GB, so this is bounded; streaming would be a memory
    // optimisation rather than a correctness fix.
    let data = if f.data_size > 0 {
        source.read_file(&source_entry, f.data_size as usize)?
    } else {
        Vec::new()
    };
    let mut data_cursor = std::io::Cursor::new(&data);

    let new_file = target.create_file(
        parent_entry,
        &f.name,
        &mut data_cursor,
        f.data_size as u64,
        &CreateFileOptions::default(),
    )?;
    report.data_bytes_copied += f.data_size as u64;

    if f.rsrc_size > 0 {
        let mut rsrc = Vec::with_capacity(f.rsrc_size as usize);
        source.write_resource_fork_to(&source_entry, &mut rsrc)?;
        let mut rsrc_cursor = std::io::Cursor::new(&rsrc);
        target.write_resource_fork(&new_file, &mut rsrc_cursor, f.rsrc_size as u64)?;
        report.rsrc_bytes_copied += f.rsrc_size as u64;
    }

    target.set_finder_info(&new_file, f.finfo, f.fxinfo)?;
    target.set_dates(&new_file, f.create_date, f.modify_date, f.backup_date)?;
    if f.is_locked() {
        target.set_file_locked(&new_file, true)?;
    }

    cnid_map.insert(f.cnid, new_file.location as u32);
    report.files_copied += 1;
    Ok(())
}

/// Summary returned by [`emit_apm_disk_with_hfs`].
#[derive(Debug, Clone)]
pub struct EmitReport {
    /// Total bytes written to the output (DDR + map + drivers + HFS partition).
    pub total_bytes: u64,
    /// Number of `Apple_Driver*` partitions copied verbatim from the source.
    pub drivers_copied: u32,
    /// Block on which the new HFS partition starts (in 512-byte units).
    pub hfs_start_block: u32,
    /// Length of the new HFS partition in 512-byte blocks.
    pub hfs_block_count: u32,
}

/// APM partition types that should be copied verbatim from the source disk
/// when wrapping a cloned HFS partition.
fn is_driver_type(t: &str) -> bool {
    matches!(
        t,
        "Apple_Driver"
            | "Apple_Driver43"
            | "Apple_Driver43_CD"
            | "Apple_Driver_ATA"
            | "Apple_Driver_ATAPI"
            | "Apple_Patches"
            | "Apple_FWDriver"
    )
}

/// Wrap a freshly-cloned HFS partition in a new APM disk image.
///
/// Step 6 of `docs/hfs_expand_block_size.md`. The output is a complete
/// raw disk: DDR + partition map + driver partitions copied byte-for-byte
/// from the source + the new `Apple_HFS` partition holding `hfs_image`.
///
/// `source_disk` is read-only; only driver partitions and the original HFS
/// partition's start block are inspected. `hfs_image` is the
/// already-populated HFS volume (typically produced by [`clone_hfs_volume`]
/// onto a buffer from `create_blank_hfs`). It is written verbatim at the
/// new partition's offset and must be a multiple of 512 bytes.
///
/// The new HFS partition starts at the same block as the source's first
/// `Apple_HFS` partition when one exists; otherwise it starts immediately
/// after the last driver partition (or at block 64 if there are none),
/// matching the layout Apple's tools produce.
pub fn emit_apm_disk_with_hfs<R, W>(
    source_disk: &mut R,
    source_data_size: u64,
    hfs_image: &[u8],
    output: &mut W,
) -> Result<EmitReport, FilesystemError>
where
    R: Read + Seek,
    W: Read + Write + Seek,
{
    if hfs_image.len() % 512 != 0 {
        return Err(FilesystemError::InvalidData(format!(
            "hfs_image length {} is not a multiple of 512",
            hfs_image.len()
        )));
    }
    let hfs_block_count_u64 = (hfs_image.len() / 512) as u64;
    if hfs_block_count_u64 == 0 {
        return Err(FilesystemError::InvalidData(
            "hfs_image is empty".to_string(),
        ));
    }

    let source_apm = Apm::parse(source_disk)
        .map_err(|e| FilesystemError::InvalidData(format!("failed to parse source APM: {e}")))?;
    let block_size = source_apm.ddr.block_size as u64;
    if block_size != 512 {
        return Err(FilesystemError::InvalidData(format!(
            "unsupported APM block size {block_size}; only 512 is handled"
        )));
    }

    // Collect driver partitions from the source, in start-block order.
    let mut drivers: Vec<ApmPartitionEntry> = source_apm
        .entries
        .iter()
        .filter(|e| is_driver_type(&e.partition_type))
        .cloned()
        .collect();
    drivers.sort_by_key(|d| d.start_block);

    // HFS start block: prefer the source's first Apple_HFS start so emulators
    // that hard-code partition layouts still recognise it; otherwise place it
    // right after the last driver, with a minimum of block 64.
    let drivers_end: u32 = drivers
        .iter()
        .map(|d| d.start_block.saturating_add(d.block_count))
        .max()
        .unwrap_or(0);
    let source_hfs_start: Option<u32> = source_apm
        .entries
        .iter()
        .find(|e| e.partition_type == "Apple_HFS")
        .map(|e| e.start_block);

    // The APM map itself is `1 + map_entries` blocks (DDR + entries) starting
    // at block 0; first usable block is `1 + map_entries`. Plan for the worst
    // case (1 self-entry + drivers + 1 hfs).
    let min_after_map = 1 + (1 + drivers.len() as u32 + 1);
    let hfs_start_block = match source_hfs_start {
        Some(s) if s >= drivers_end.max(min_after_map) => s,
        _ => drivers_end.max(min_after_map).max(64),
    };

    let hfs_block_count: u32 = hfs_block_count_u64.try_into().map_err(|_| {
        FilesystemError::InvalidData(format!(
            "hfs_image too large for u32 block count: {} bytes",
            hfs_image.len()
        ))
    })?;
    let target_block_count: u32 = hfs_start_block
        .checked_add(hfs_block_count)
        .ok_or_else(|| FilesystemError::InvalidData("APM total block count overflow".into()))?;

    // Build the new APM. One self-referencing partition_map entry, then each
    // driver verbatim, then the new HFS partition.
    let map_entries: u32 = 1 + drivers.len() as u32 + 1;
    let mut entries: Vec<ApmPartitionEntry> = Vec::with_capacity(map_entries as usize);
    // Preserve the source's Apple_partition_map self-entry verbatim: keep its
    // reserved block_count (typically 63 on classic Mac disks) and status
    // flags (0x37 on bootable Quadra disks). Some Mac ROMs reject the disk
    // if the self-entry's status is zero.
    let source_self = source_apm
        .entries
        .iter()
        .find(|e| e.partition_type == "Apple_partition_map");
    let map_block_count = source_self.map(|e| e.block_count).unwrap_or(map_entries);
    entries.push(ApmPartitionEntry {
        signature: 0x504D,
        map_entries,
        start_block: 1,
        block_count: map_block_count,
        name: source_self
            .map(|e| e.name.clone())
            .unwrap_or_else(|| "Apple".to_string()),
        partition_type: "Apple_partition_map".to_string(),
        data_start: 0,
        data_count: map_block_count,
        status: source_self.map(|e| e.status).unwrap_or(0x37),
        boot_start: source_self.map(|e| e.boot_start).unwrap_or(0),
        boot_size: source_self.map(|e| e.boot_size).unwrap_or(0),
        boot_load: source_self.map(|e| e.boot_load).unwrap_or(0),
        boot_entry: source_self.map(|e| e.boot_entry).unwrap_or(0),
        boot_checksum: source_self.map(|e| e.boot_checksum).unwrap_or(0),
        processor: source_self.map(|e| e.processor.clone()).unwrap_or_default(),
        pad: source_self.map(|e| e.pad.clone()).unwrap_or_default(),
    });
    for d in &drivers {
        let mut e = d.clone();
        e.map_entries = map_entries;
        entries.push(e);
    }
    // Preserve the source's boot-related fields (status + pmBoot* + processor
    // + name) on the new Apple_HFS entry. Quadra ROMs read these out of the
    // partition map at boot, so a bootable source disk's flags must carry
    // through to the expanded copy. Fall back to 0x33 / "MacOS" only when
    // the source has no Apple_HFS entry.
    let source_hfs = source_apm
        .entries
        .iter()
        .find(|e| e.partition_type == "Apple_HFS");
    entries.push(ApmPartitionEntry {
        signature: 0x504D,
        map_entries,
        start_block: hfs_start_block,
        block_count: hfs_block_count,
        name: source_hfs
            .map(|e| e.name.clone())
            .unwrap_or_else(|| "MacOS".to_string()),
        partition_type: "Apple_HFS".to_string(),
        data_start: 0,
        data_count: hfs_block_count,
        status: source_hfs.map(|e| e.status).unwrap_or(0x33),
        boot_start: source_hfs.map(|e| e.boot_start).unwrap_or(0),
        boot_size: source_hfs.map(|e| e.boot_size).unwrap_or(0),
        boot_load: source_hfs.map(|e| e.boot_load).unwrap_or(0),
        boot_entry: source_hfs.map(|e| e.boot_entry).unwrap_or(0),
        boot_checksum: source_hfs.map(|e| e.boot_checksum).unwrap_or(0),
        processor: source_hfs.map(|e| e.processor.clone()).unwrap_or_default(),
        pad: source_hfs.map(|e| e.pad.clone()).unwrap_or_default(),
    });

    let mut new_apm = source_apm.clone();
    new_apm.entries = entries;
    new_apm.map_entry_count = map_entries;
    new_apm.ddr.block_count = target_block_count;
    new_apm.ddr.block_size = 512;

    let apm_blocks = new_apm.build_apm_blocks(Some(target_block_count));

    // Layout writers linearly: DDR+map at offset 0, then each entry at its
    // start_block*512 with zero-fill between. Output supports Seek so we can
    // skip ahead, but we still write the trailing region to set the file
    // length correctly.
    output.seek(SeekFrom::Start(0))?;
    output.write_all(&apm_blocks)?;
    let mut cursor: u64 = apm_blocks.len() as u64;

    // Sort write-targets by start offset so we can zero-fill any gap simply.
    struct Region {
        start: u64,
        bytes: Vec<u8>,
    }
    let mut regions: Vec<Region> = Vec::new();
    let mut buf = vec![0u8; 64 * 1024];
    for d in &drivers {
        let src_off = d.start_block as u64 * 512;
        let len = d.block_count as u64 * 512;
        let copy_bound = len.min(source_data_size.saturating_sub(src_off));
        let mut data = vec![0u8; len as usize];
        if copy_bound > 0 {
            source_disk.seek(SeekFrom::Start(src_off))?;
            let mut filled: u64 = 0;
            while filled < copy_bound {
                let want = ((copy_bound - filled) as usize).min(buf.len());
                let n = source_disk.read(&mut buf[..want])?;
                if n == 0 {
                    break;
                }
                data[filled as usize..filled as usize + n].copy_from_slice(&buf[..n]);
                filled += n as u64;
            }
        }
        regions.push(Region {
            start: d.start_block as u64 * 512,
            bytes: data,
        });
    }
    regions.push(Region {
        start: hfs_start_block as u64 * 512,
        bytes: hfs_image.to_vec(),
    });
    regions.sort_by_key(|r| r.start);

    for region in regions {
        if region.start < cursor {
            return Err(FilesystemError::InvalidData(format!(
                "APM emit: region at {} overlaps already-written data (cursor at {})",
                region.start, cursor
            )));
        }
        if region.start > cursor {
            let gap = region.start - cursor;
            let zeros = vec![0u8; 64 * 1024];
            let mut remaining = gap;
            while remaining > 0 {
                let n = (remaining as usize).min(zeros.len());
                output.write_all(&zeros[..n])?;
                remaining -= n as u64;
            }
            cursor = region.start;
        }
        output.write_all(&region.bytes)?;
        cursor += region.bytes.len() as u64;
    }

    Ok(EmitReport {
        total_bytes: cursor,
        drivers_copied: drivers.len() as u32,
        hfs_start_block,
        hfs_block_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
    use crate::fs::hfs::HfsFilesystem;
    use byteorder::{BigEndian, ByteOrder};
    use std::io::Cursor;

    /// Build a minimal classic-HFS image with a volume name, known dates,
    /// and a blessed folder CNID. Fields not exercised by the cloner are
    /// left at defaults.
    fn make_test_image() -> Vec<u8> {
        {
            let block_size = 4096u32;
            let total_blocks: u16 = 128;
            let first_alloc_block: u16 = 5;
            let alloc_start = first_alloc_block as usize * 512;
            let image_size = alloc_start + total_blocks as usize * block_size as usize;
            let mut img = vec![0u8; image_size];

            // Boot blocks (sectors 0..1, 1024 bytes): write a recognisable
            // pattern so capture() can round-trip them.
            for i in 0..1024 {
                img[i] = (i as u8).wrapping_mul(7).wrapping_add(3);
            }

            // Bitmap sector 3, mark blocks 0-3 allocated (catalog)
            img[3 * 512] = 0b11110000;

            // Build a catalog B-tree with just the root dir + its thread
            let node_size = 4096usize;
            let catalog_offset = alloc_start;
            img[catalog_offset + 8] = 1; // header node
            BigEndian::write_u16(&mut img[catalog_offset + 10..catalog_offset + 12], 3);
            let hr = catalog_offset + 14;
            BigEndian::write_u16(&mut img[hr..hr + 2], 1);
            BigEndian::write_u32(&mut img[hr + 2..hr + 6], 1);
            BigEndian::write_u32(&mut img[hr + 6..hr + 10], 2);
            BigEndian::write_u32(&mut img[hr + 10..hr + 14], 1);
            BigEndian::write_u32(&mut img[hr + 14..hr + 18], 1);
            BigEndian::write_u16(&mut img[hr + 18..hr + 20], node_size as u16);
            BigEndian::write_u16(&mut img[hr + 20..hr + 22], 37);
            BigEndian::write_u32(&mut img[hr + 22..hr + 26], 4);
            BigEndian::write_u32(&mut img[hr + 26..hr + 30], 2);
            let ot = catalog_offset + node_size;
            BigEndian::write_u16(&mut img[ot - 2..ot], 14);
            BigEndian::write_u16(&mut img[ot - 4..ot - 2], 142);
            BigEndian::write_u16(&mut img[ot - 6..ot - 4], 270);
            BigEndian::write_u16(&mut img[ot - 8..ot - 6], 526);
            img[catalog_offset + 270] = 0b11000000;

            let leaf_off = catalog_offset + node_size;
            img[leaf_off + 8] = 0xFF; // leaf
            img[leaf_off + 9] = 1;
            BigEndian::write_u16(&mut img[leaf_off + 10..leaf_off + 12], 2);

            let r0_off = leaf_off + 14;
            img[r0_off] = 6;
            img[r0_off + 1] = 0;
            BigEndian::write_u32(&mut img[r0_off + 2..r0_off + 6], 1);
            img[r0_off + 6] = 0;
            img[r0_off + 7] = 0;
            let r0_data = r0_off + 8;
            img[r0_data] = 1; // CATALOG_DIR
            BigEndian::write_u32(&mut img[r0_data + 6..r0_data + 10], 2);
            // Give the root dir some recognisable dates + DInfo
            BigEndian::write_u32(&mut img[r0_data + 10..r0_data + 14], 0x11111111);
            BigEndian::write_u32(&mut img[r0_data + 14..r0_data + 18], 0x22222222);
            BigEndian::write_u32(&mut img[r0_data + 18..r0_data + 22], 0x33333333);
            img[r0_data + 22..r0_data + 38].copy_from_slice(b"DINFO_ROOT______");
            img[r0_data + 38..r0_data + 54].copy_from_slice(b"DXINFO_ROOT_____");

            let r1_off = r0_data + 70;
            img[r1_off] = 6;
            img[r1_off + 1] = 0;
            BigEndian::write_u32(&mut img[r1_off + 2..r1_off + 6], 2);
            img[r1_off + 6] = 0;
            img[r1_off + 7] = 0;
            let r1_data = r1_off + 8;
            img[r1_data] = 3; // CATALOG_DIR_THREAD
            BigEndian::write_u32(&mut img[r1_data + 10..r1_data + 14], 1);
            img[r1_data + 14] = 0;

            let lot = leaf_off + node_size;
            BigEndian::write_u16(&mut img[lot - 2..lot], 14);
            let r1_rel = (r1_off - leaf_off) as u16;
            BigEndian::write_u16(&mut img[lot - 4..lot - 2], r1_rel);
            let free_rel = (r1_data + 46 - leaf_off) as u16;
            BigEndian::write_u16(&mut img[lot - 6..lot - 4], free_rel);

            // MDB at byte 1024 — write AFTER the boot-block pattern above
            // so the pattern doesn't stomp on it.
            let mdb_off = 1024;
            for b in img[mdb_off..mdb_off + 512].iter_mut() {
                *b = 0;
            }
            let mut mdb = [0u8; 512];
            BigEndian::write_u16(&mut mdb[0..2], 0x4244); // 'BD' HFS signature
            BigEndian::write_u32(&mut mdb[2..6], 0xAABBCCDD); // create date
            BigEndian::write_u32(&mut mdb[6..10], 0x11223344); // modify date
            BigEndian::write_u16(&mut mdb[14..16], 3); // volume bitmap sector
            BigEndian::write_u16(&mut mdb[18..20], total_blocks);
            BigEndian::write_u32(&mut mdb[20..24], block_size);
            BigEndian::write_u16(&mut mdb[28..30], first_alloc_block);
            BigEndian::write_u32(&mut mdb[30..34], 16);
            BigEndian::write_u16(&mut mdb[34..36], total_blocks - 4);
            mdb[36] = 7;
            mdb[37..44].copy_from_slice(b"TestVol");
            // drVolBkUp at offset 64
            BigEndian::write_u32(&mut mdb[64..68], 0xDEADBEEF);
            // drFndrInfo[0] = 99 (blessed folder CNID)
            BigEndian::write_u32(&mut mdb[92..96], 99);
            // drFndrInfo[3] = 77 (simulate OS-folder fingerprint)
            BigEndian::write_u32(&mut mdb[92 + 12..92 + 16], 77);
            BigEndian::write_u32(&mut mdb[146..150], 4 * block_size);
            BigEndian::write_u16(&mut mdb[150..152], 0);
            BigEndian::write_u16(&mut mdb[152..154], 4);
            img[mdb_off..mdb_off + 512].copy_from_slice(&mdb);

            img
        }
    }

    #[test]
    fn volume_snapshot_roundtrip() {
        let img = make_test_image();
        let mut fs = HfsFilesystem::open(Cursor::new(img.clone()), 0).unwrap();
        let snap = SourceVolumeSnapshot::capture(&mut fs).unwrap();

        assert_eq!(snap.volume_name, "TestVol");
        assert_eq!(snap.volume_name_raw, b"TestVol");
        assert_eq!(snap.create_date, 0xAABBCCDD);
        assert_eq!(snap.modify_date, 0x11223344);
        assert_eq!(snap.backup_date, 0xDEADBEEF);
        assert_eq!(snap.finder_info[0], 99);
        assert_eq!(snap.finder_info[3], 77);
        assert_eq!(snap.block_size, 4096);
        assert_eq!(snap.total_blocks, 128);

        // Boot blocks round-trip: reproduce the pattern the generator wrote
        // into sectors 0..1.
        let mut expected = [0u8; 1024];
        for i in 0..1024 {
            expected[i] = (i as u8).wrapping_mul(7).wrapping_add(3);
        }
        assert_eq!(snap.boot_blocks, expected);
    }

    #[test]
    fn dir_snapshot_captures_dinfo_and_dates() {
        let img = make_test_image();
        let mut fs = HfsFilesystem::open(Cursor::new(img.clone()), 0).unwrap();
        let snap = SourceCatalogSnapshot::capture(&mut fs).unwrap();

        // Root directory should be captured (CNID 2, parent 1) with the
        // dates and DInfo we stamped.
        let root = snap
            .dirs
            .iter()
            .find(|d| d.cnid == 2)
            .expect("root dir captured");
        assert_eq!(root.parent_cnid, 1);
        assert_eq!(root.create_date, 0x11111111);
        assert_eq!(root.modify_date, 0x22222222);
        assert_eq!(root.backup_date, 0x33333333);
        assert_eq!(&root.dinfo, b"DINFO_ROOT______");
        assert_eq!(&root.dxinfo, b"DXINFO_ROOT_____");
        assert!(snap.files.is_empty());
    }

    #[test]
    fn file_snapshot_captures_finfo_dates_and_extents() {
        // Start from the test image, add a file via the editable API so
        // FInfo type/creator + dates get written by the real code path,
        // then capture and verify the snapshot round-trips those fields.
        let mut backing: Vec<u8> = make_test_image();
        {
            let mut fs = HfsFilesystem::open(Cursor::new(&mut backing), 0).unwrap();
            let root = fs.root().unwrap();
            let opts = CreateFileOptions {
                type_code: Some("TEXT".to_string()),
                creator_code: Some("ttxt".to_string()),
                ..Default::default()
            };
            let payload = b"hello world".to_vec();
            let mut src = payload.as_slice();
            fs.create_file(&root, "greet", &mut src, payload.len() as u64, &opts)
                .unwrap();
            fs.sync_metadata().unwrap();
        }

        let mut fs = HfsFilesystem::open(Cursor::new(&mut backing), 0).unwrap();
        let snap = SourceCatalogSnapshot::capture(&mut fs).unwrap();
        let f = snap
            .files
            .iter()
            .find(|f| f.name == "greet")
            .expect("greet captured");
        assert_eq!(f.parent_cnid, 2);
        assert_eq!(&f.finfo[0..4], b"TEXT");
        assert_eq!(&f.finfo[4..8], b"ttxt");
        assert_eq!(f.data_size, 11);
        assert!(f.data_extents[0].block_count >= 1);
        assert!(f.create_date != 0);
        assert!(f.modify_date != 0);
        assert!(!f.is_locked());
    }

    /// Build a 1 MiB blank HFS at 4K blocks, populate it via the editable
    /// API with a directory tree containing files (incl. a resource fork)
    /// and a blessed System Folder, then return the backing bytes.
    fn build_populated_source() -> Vec<u8> {
        use crate::fs::filesystem::{CreateDirectoryOptions, ResourceForkSource};
        use crate::fs::hfs::create_blank_hfs;

        let mut backing = create_blank_hfs(1024 * 1024, 4096, "Source").unwrap();
        {
            let mut fs = HfsFilesystem::open(Cursor::new(&mut backing), 0).unwrap();
            let root = fs.root().unwrap();

            let sysf = fs
                .create_directory(&root, "System Folder", &CreateDirectoryOptions::default())
                .unwrap();
            let apps = fs
                .create_directory(&root, "Apps", &CreateDirectoryOptions::default())
                .unwrap();
            let sub = fs
                .create_directory(&apps, "Sub", &CreateDirectoryOptions::default())
                .unwrap();

            // Stamp recognisable DInfo / dates on Apps so we can verify
            // directory metadata flowed through.
            let mut dinfo = [0u8; 16];
            dinfo[..4].copy_from_slice(&[0x12, 0x34, 0x56, 0x78]);
            let mut dxinfo = [0u8; 16];
            dxinfo[..4].copy_from_slice(&[0xAA, 0xBB, 0xCC, 0xDD]);
            fs.set_directory_finder_info(&apps, dinfo, dxinfo).unwrap();
            fs.set_dates(&apps, 0x10000001, 0x10000002, 0x10000003)
                .unwrap();

            // Plain text file in Apps.
            let payload = b"hello world".to_vec();
            let mut data = payload.as_slice();
            let opts = CreateFileOptions {
                type_code: Some("TEXT".to_string()),
                creator_code: Some("ttxt".to_string()),
                ..Default::default()
            };
            let note = fs
                .create_file(&apps, "note.txt", &mut data, payload.len() as u64, &opts)
                .unwrap();
            fs.set_dates(&note, 0x20000001, 0x20000002, 0x20000003)
                .unwrap();

            // Bigger file in Apps/Sub to exercise multi-block reads.
            let big = vec![0x42u8; 6000];
            let mut data = big.as_slice();
            let opts = CreateFileOptions {
                type_code: Some("BINA".to_string()),
                creator_code: Some("hexd".to_string()),
                ..Default::default()
            };
            fs.create_file(&sub, "data.bin", &mut data, big.len() as u64, &opts)
                .unwrap();

            // File with a resource fork in the root.
            let rsrc = b"RESOURCE_FORK_BYTES".to_vec();
            let payload = b"with rsrc".to_vec();
            let mut data = payload.as_slice();
            let opts = CreateFileOptions {
                type_code: Some("APPL".to_string()),
                creator_code: Some("MACS".to_string()),
                resource_fork: Some(ResourceForkSource::Data(rsrc.clone())),
                ..Default::default()
            };
            let _ = fs
                .create_file(&root, "App", &mut data, payload.len() as u64, &opts)
                .unwrap();

            // Locked file.
            let payload = b"locked content".to_vec();
            let mut data = payload.as_slice();
            let locked_file = fs
                .create_file(
                    &root,
                    "ReadOnly",
                    &mut data,
                    payload.len() as u64,
                    &CreateFileOptions::default(),
                )
                .unwrap();
            fs.set_file_locked(&locked_file, true).unwrap();

            fs.set_blessed_folder(&sysf).unwrap();

            // Stamp deterministic volume dates so we can assert on them.
            fs.set_volume_dates(0x30000001, 0x30000002, 0x30000003);

            fs.sync_metadata().unwrap();
        }
        backing
    }

    fn find_child<'a, R: Read + Seek + Send>(
        fs: &mut HfsFilesystem<R>,
        parent: &FileEntry,
        name: &str,
    ) -> FileEntry {
        let kids = fs.list_directory(parent).unwrap();
        kids.into_iter()
            .find(|e| e.name == name)
            .unwrap_or_else(|| panic!("entry '{name}' not found under {}", parent.path))
    }

    #[test]
    fn clone_round_trip_basic() {
        let mut source_bytes = build_populated_source();
        // Target uses a different (larger) block size to exercise the
        // reformat aspect of the clone.
        let mut target_bytes =
            crate::fs::hfs::create_blank_hfs(2 * 1024 * 1024, 8192, "Target").unwrap();

        {
            let mut source = HfsFilesystem::open(Cursor::new(&mut source_bytes), 0).unwrap();
            let mut target = HfsFilesystem::open(Cursor::new(&mut target_bytes), 0).unwrap();
            let report = clone_hfs_volume(&mut source, &mut target).unwrap();
            assert_eq!(report.files_copied, 4);
            assert_eq!(report.dirs_copied, 3);
            assert!(report.data_bytes_copied >= 6000 + 11);
            assert_eq!(
                report.rsrc_bytes_copied,
                b"RESOURCE_FORK_BYTES".len() as u64
            );
        }

        // Re-open target read-only and verify the catalog matches.
        let mut fs = HfsFilesystem::open(Cursor::new(&target_bytes), 0).unwrap();
        let root = fs.root().unwrap();
        let kids: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(kids.contains(&"System Folder".to_string()));
        assert!(kids.contains(&"Apps".to_string()));
        assert!(kids.contains(&"App".to_string()));
        assert!(kids.contains(&"ReadOnly".to_string()));

        // Apps dir metadata round-trip.
        let apps = find_child(&mut fs, &root, "Apps");
        let apps_kids: Vec<String> = fs
            .list_directory(&apps)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(apps_kids.contains(&"note.txt".to_string()));
        assert!(apps_kids.contains(&"Sub".to_string()));

        // Sub/data.bin contents.
        let sub = find_child(&mut fs, &apps, "Sub");
        let data_bin = find_child(&mut fs, &sub, "data.bin");
        assert_eq!(data_bin.size, 6000);
        let bytes = fs.read_file(&data_bin, 6000).unwrap();
        assert_eq!(bytes, vec![0x42u8; 6000]);
        assert_eq!(data_bin.type_code.as_deref(), Some("BINA"));
        assert_eq!(data_bin.creator_code.as_deref(), Some("hexd"));

        // note.txt contents and type/creator.
        let note = find_child(&mut fs, &apps, "note.txt");
        let bytes = fs.read_file(&note, 32).unwrap();
        assert_eq!(bytes, b"hello world");
        assert_eq!(note.type_code.as_deref(), Some("TEXT"));
        assert_eq!(note.creator_code.as_deref(), Some("ttxt"));

        // Resource-forked file.
        let app = find_child(&mut fs, &root, "App");
        assert_eq!(app.resource_fork_size, Some(19));
        let mut buf = Vec::new();
        fs.write_resource_fork_to(&app, &mut buf).unwrap();
        assert_eq!(buf, b"RESOURCE_FORK_BYTES");

        // Blessed folder pointer remapped to target's "System Folder" CNID.
        let blessed = fs.blessed_system_folder().expect("blessed folder set");
        let sys_folder = find_child(&mut fs, &root, "System Folder");
        assert_eq!(blessed.0, sys_folder.location);
        assert_eq!(blessed.1, "System Folder");

        // Volume name + dates carried.
        assert_eq!(fs.volume_label(), Some("Target"));

        // fsck reports no errors on the cloned volume.
        let fsck = fs.fsck().expect("HFS supports fsck");
        assert!(
            fsck.is_clean(),
            "fsck errors on cloned volume: {:?}",
            fsck.errors
                .iter()
                .map(|e| e.code.as_str())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn clone_preserves_locked_flag_and_dates() {
        let mut source_bytes = build_populated_source();
        let mut target_bytes =
            crate::fs::hfs::create_blank_hfs(2 * 1024 * 1024, 8192, "Target").unwrap();
        {
            let mut source = HfsFilesystem::open(Cursor::new(&mut source_bytes), 0).unwrap();
            let mut target = HfsFilesystem::open(Cursor::new(&mut target_bytes), 0).unwrap();
            clone_hfs_volume(&mut source, &mut target).unwrap();
        }

        // Capture target catalog and inspect raw flags / dates.
        let mut fs = HfsFilesystem::open(Cursor::new(&target_bytes), 0).unwrap();
        let snap = SourceCatalogSnapshot::capture(&mut fs).unwrap();
        let locked = snap
            .files
            .iter()
            .find(|f| f.name == "ReadOnly")
            .expect("ReadOnly file present");
        assert!(locked.is_locked(), "locked flag must survive clone");

        let note = snap
            .files
            .iter()
            .find(|f| f.name == "note.txt")
            .expect("note.txt present");
        assert_eq!(note.create_date, 0x20000001);
        assert_eq!(note.modify_date, 0x20000002);
        assert_eq!(note.backup_date, 0x20000003);

        let apps = snap
            .dirs
            .iter()
            .find(|d| d.name == "Apps")
            .expect("Apps dir present");
        assert_eq!(apps.create_date, 0x10000001);
        assert_eq!(apps.modify_date, 0x10000002);
        assert_eq!(apps.backup_date, 0x10000003);
        assert_eq!(&apps.dinfo[..4], &[0x12, 0x34, 0x56, 0x78]);
        assert_eq!(&apps.dxinfo[..4], &[0xAA, 0xBB, 0xCC, 0xDD]);

        // Volume dates copied.
        assert_eq!(snap.volume.create_date, 0x30000001);
        assert_eq!(snap.volume.modify_date, 0x30000002);
        assert_eq!(snap.volume.backup_date, 0x30000003);
    }

    /// Build a synthetic raw APM source disk with a partition_map entry, an
    /// `Apple_Driver43` partition with recognisable bytes, and an `Apple_HFS`
    /// partition. Used to verify the emit path preserves drivers and lays out
    /// the HFS partition at the right block.
    fn build_synthetic_apm_disk() -> Vec<u8> {
        use crate::partition::apm::build_minimal_apm;
        let total_blocks: u32 = 512; // 256 KiB total
        let driver_start: u32 = 32;
        let driver_blocks: u32 = 8;
        let hfs_start: u32 = 64;
        let hfs_blocks: u32 = total_blocks - hfs_start;
        let mut apm = build_minimal_apm(
            &[
                ("Apple_Driver43".to_string(), driver_start, driver_blocks),
                ("Apple_HFS".to_string(), hfs_start, hfs_blocks),
            ],
            512,
            total_blocks,
        );
        apm.entries[1].name = "Macintosh".to_string();
        apm.entries[2].name = "MacOS".to_string();
        let blocks = apm.build_apm_blocks(Some(total_blocks));

        let mut disk = vec![0u8; total_blocks as usize * 512];
        disk[..blocks.len()].copy_from_slice(&blocks);
        // Stamp the driver partition with a recognisable byte pattern so the
        // emit test can verify byte-for-byte preservation.
        for i in 0..(driver_blocks as usize * 512) {
            disk[driver_start as usize * 512 + i] = (i as u8).wrapping_mul(13).wrapping_add(7);
        }
        disk
    }

    #[test]
    fn emit_apm_disk_round_trip() {
        let source_disk = build_synthetic_apm_disk();
        let source_len = source_disk.len() as u64;

        // Cloned HFS partition. Use create_blank_hfs to produce a syntactically
        // valid (and fsck-clean) HFS volume of a chosen size. The emit step
        // doesn't care that the volume is empty, only that the bytes are valid.
        let hfs_image =
            crate::fs::hfs::create_blank_hfs(2 * 1024 * 1024, 4096, "Expanded").unwrap();

        let mut src_cursor = Cursor::new(source_disk.clone());
        let mut output: Vec<u8> = Vec::new();
        let mut out_cursor = Cursor::new(&mut output);
        let report =
            emit_apm_disk_with_hfs(&mut src_cursor, source_len, &hfs_image, &mut out_cursor)
                .expect("emit succeeds");

        assert_eq!(report.drivers_copied, 1);
        // Driver was at block 32 + 8 blocks; min_after_map = 1 + (1+1+1) = 4;
        // source HFS started at block 64, which is >= driver_end(40) and >= 64.
        assert_eq!(report.hfs_start_block, 64);
        assert_eq!(report.hfs_block_count, hfs_image.len() as u32 / 512);
        assert_eq!(
            report.total_bytes,
            (report.hfs_start_block as u64 + report.hfs_block_count as u64) * 512
        );

        // Re-parse the emitted disk's APM and verify the layout.
        let mut cursor = Cursor::new(&output);
        let parsed = Apm::parse(&mut cursor).expect("emitted APM parses");
        assert_eq!(parsed.ddr.block_size, 512);
        assert_eq!(parsed.ddr.block_count, report.total_bytes as u32 / 512);
        assert_eq!(parsed.entries.len(), 3);
        assert_eq!(parsed.entries[0].partition_type, "Apple_partition_map");
        assert_eq!(parsed.entries[1].partition_type, "Apple_Driver43");
        assert_eq!(parsed.entries[1].start_block, 32);
        assert_eq!(parsed.entries[1].block_count, 8);
        assert_eq!(parsed.entries[2].partition_type, "Apple_HFS");
        assert_eq!(parsed.entries[2].start_block, 64);

        // Driver bytes preserved verbatim.
        let drv_off = 32 * 512;
        let drv_len = 8 * 512;
        assert_eq!(
            &output[drv_off..drv_off + drv_len],
            &source_disk[drv_off..drv_off + drv_len]
        );

        // HFS bytes written at the right offset.
        let hfs_off = report.hfs_start_block as usize * 512;
        assert_eq!(&output[hfs_off..hfs_off + hfs_image.len()], &hfs_image[..]);

        // The wrapped HFS partition opens and is fsck-clean.
        let mut fs =
            HfsFilesystem::open(Cursor::new(&output), report.hfs_start_block as u64 * 512).unwrap();
        let fsck = fs.fsck().expect("HFS supports fsck");
        assert!(
            fsck.is_clean(),
            "fsck errors on emitted HFS partition: {:?}",
            fsck.errors
                .iter()
                .map(|e| e.code.as_str())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn emit_rejects_non_512_aligned_image() {
        let source_disk = build_synthetic_apm_disk();
        let source_len = source_disk.len() as u64;
        let bad_image = vec![0u8; 511];
        let mut src = Cursor::new(source_disk);
        let mut out: Vec<u8> = Vec::new();
        let mut out_cursor = Cursor::new(&mut out);
        let err = emit_apm_disk_with_hfs(&mut src, source_len, &bad_image, &mut out_cursor)
            .expect_err("non-512-aligned image must be rejected");
        assert!(matches!(err, FilesystemError::InvalidData(_)));
    }
}
