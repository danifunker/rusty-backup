//! Lightweight HFS+ / HFSX filesystem checker.
//!
//! Step 11 of `docs/hfsplus_enhancements.md`. Three phases — each cheap
//! enough that we can run this against every clone-related test from
//! Step 12 onward as a regression harness.
//!
//! Phase 1: Volume header sanity (signature, block size, file/folder
//!          counts vs. catalog walk).
//! Phase 2: Catalog walk — count files / dirs / threads, detect orphans
//!          (entries whose parent CNID has no folder record), detect
//!          dangling threads (threads pointing at a non-existent CNID).
//! Phase 3: Bitmap consistency — sum of allocated bits == total_blocks -
//!          free_blocks.
//!
//! Returns the shared `FsckResult` so the GUI Check button works without
//! any per-filesystem branches. Repair is not implemented in this step;
//! every issue is flagged as non-repairable.

use std::collections::{HashMap, HashSet};
use std::io::{Read, Seek};

use byteorder::{BigEndian, ByteOrder};

use super::filesystem::FilesystemError;
use super::fsck::{FsckIssue, FsckResult, FsckStats, OrphanedEntry};
use super::hfs_common::walk_leaf_records;
use super::hfsplus::HfsPlusFilesystem;

const HFS_PLUS_SIGNATURE: u16 = 0x482B; // "H+"
const HFSX_SIGNATURE: u16 = 0x4858; // "HX"
const ROOT_PARENT_CNID: u32 = 1;
const ROOT_FOLDER_CNID: u32 = 2;

const CATALOG_FOLDER: i16 = 1;
const CATALOG_FILE: i16 = 2;
const CATALOG_FOLDER_THREAD: i16 = 3;
const CATALOG_FILE_THREAD: i16 = 4;

/// Issue codes specific to the HFS+ checker. Kept private to this module —
/// the GUI sees only the stringified form on `FsckIssue.code`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HfsPlusFsckCode {
    BadSignature,
    BadBlockSize,
    BlockSizeNotPowerOfTwo,
    FileCountMismatch,
    FolderCountMismatch,
    OrphanedEntry,
    OrphanedThread,
    DuplicateCnid,
    DuplicateCatalogKey,
    KeyOutOfOrder,
    ParentValenceMismatch,
    BitmapAllocCountMismatch,
    BitmapTooShort,
}

impl HfsPlusFsckCode {
    fn as_str(self) -> &'static str {
        match self {
            HfsPlusFsckCode::BadSignature => "BadSignature",
            HfsPlusFsckCode::BadBlockSize => "BadBlockSize",
            HfsPlusFsckCode::BlockSizeNotPowerOfTwo => "BlockSizeNotPowerOfTwo",
            HfsPlusFsckCode::FileCountMismatch => "FileCountMismatch",
            HfsPlusFsckCode::FolderCountMismatch => "FolderCountMismatch",
            HfsPlusFsckCode::OrphanedEntry => "OrphanedEntry",
            HfsPlusFsckCode::OrphanedThread => "OrphanedThread",
            HfsPlusFsckCode::DuplicateCnid => "DuplicateCnid",
            HfsPlusFsckCode::DuplicateCatalogKey => "DuplicateCatalogKey",
            HfsPlusFsckCode::KeyOutOfOrder => "KeyOutOfOrder",
            HfsPlusFsckCode::ParentValenceMismatch => "ParentValenceMismatch",
            HfsPlusFsckCode::BitmapAllocCountMismatch => "BitmapAllocCountMismatch",
            HfsPlusFsckCode::BitmapTooShort => "BitmapTooShort",
        }
    }
}

fn issue(code: HfsPlusFsckCode, message: impl Into<String>) -> FsckIssue {
    FsckIssue {
        code: code.as_str().to_string(),
        message: message.into(),
        repairable: false,
        debug: false,
    }
}

/// Run the read-only HFS+ check. The volume is not mutated.
pub(super) fn check<R: Read + Seek>(
    fs: &mut HfsPlusFilesystem<R>,
) -> Result<FsckResult, FilesystemError> {
    let mut errors: Vec<FsckIssue> = Vec::new();
    let warnings: Vec<FsckIssue> = Vec::new();
    let mut orphaned: Vec<OrphanedEntry> = Vec::new();

    // ---- Phase 1: Volume header sanity --------------------------------
    let signature = fs.signature();
    if signature != HFS_PLUS_SIGNATURE && signature != HFSX_SIGNATURE {
        errors.push(issue(
            HfsPlusFsckCode::BadSignature,
            format!("VH signature 0x{signature:04X} is not HFS+ (0x482B) or HFSX (0x4858)"),
        ));
    }
    let block_size = fs.block_size();
    if block_size < 512 {
        errors.push(issue(
            HfsPlusFsckCode::BadBlockSize,
            format!("VH block size {block_size} is below 512"),
        ));
    } else if !block_size.is_power_of_two() {
        errors.push(issue(
            HfsPlusFsckCode::BlockSizeNotPowerOfTwo,
            format!("VH block size {block_size} is not a power of two"),
        ));
    }

    // ---- Phase 2: Catalog walk ----------------------------------------
    let mut walk = CatalogWalk::default();
    {
        let catalog = fs.catalog_data();
        let node_size = fs.catalog_node_size();
        let first_leaf = fs.catalog_first_leaf();
        let case_sensitive = fs.case_sensitive_catalog();
        let mut prev_key: Option<(u32, usize, Vec<u8>)> = None;
        walk_leaf_records(
            catalog,
            first_leaf,
            node_size,
            |node_idx, rec_idx, abs_off, rec| -> Option<()> {
                if rec.len() >= 8 {
                    let key_len = BigEndian::read_u16(&rec[0..2]) as usize;
                    if 2 + key_len <= rec.len() {
                        let cur_key = rec[..2 + key_len].to_vec();
                        if let Some((prev_node, prev_rec, ref prev)) = prev_key {
                            use std::cmp::Ordering as Ord2;
                            let cmp = super::hfsplus::catalog_compare_keys(
                                prev,
                                &cur_key,
                                case_sensitive,
                            );
                            if cmp != Ord2::Less {
                                errors.push(issue(
                                    HfsPlusFsckCode::KeyOutOfOrder,
                                    format!(
                                        "leaf record (node={node_idx}, idx={rec_idx}) at \
                                         offset 0x{abs_off:x} sorts {cmp:?} relative to the \
                                         preceding record (node={prev_node}, idx={prev_rec}); \
                                         B-tree leaves must be strictly ascending",
                                    ),
                                ));
                            }
                        }
                        prev_key = Some((node_idx, rec_idx, cur_key));
                    }
                }
                walk.visit(rec);
                None
            },
        );
    }

    // Folder/file counters: per Apple TN1150, VH.folder_count EXCLUDES
    // the root directory (CNID 2). The catalog walk counts every folder
    // record it visits, including root, so we subtract 1 for the
    // comparison. `create_blank_hfsplus` / `create_directory` /
    // `delete_entry` are aligned with this convention.
    let total_folder_count = walk.folder_cnids.len() as u32;
    let total_file_count = walk.file_cnids.len() as u32;
    let folder_count_excl_root = total_folder_count.saturating_sub(1);
    let vh_folder = fs.vh_folder_count();
    let vh_file = fs.vh_file_count();
    if vh_file != total_file_count {
        errors.push(issue(
            HfsPlusFsckCode::FileCountMismatch,
            format!("VH.file_count = {vh_file} but catalog walk found {total_file_count} files"),
        ));
    }
    if vh_folder != folder_count_excl_root {
        errors.push(issue(
            HfsPlusFsckCode::FolderCountMismatch,
            format!(
                "VH.folder_count = {vh_folder} but catalog walk found {folder_count_excl_root} \
                 folders (excluding root)"
            ),
        ));
    }

    // Orphans: entries whose parent CNID is not a known folder (and not
    // the special parent=1 placeholder used for the root folder record).
    for entry in &walk.entries {
        if entry.parent_cnid == ROOT_PARENT_CNID {
            continue;
        }
        if !walk.folder_cnids.contains(&entry.parent_cnid) {
            errors.push(issue(
                HfsPlusFsckCode::OrphanedEntry,
                format!(
                    "{} CNID {} (\"{}\") references missing parent CNID {}",
                    if entry.is_dir { "folder" } else { "file" },
                    entry.cnid,
                    entry.name,
                    entry.parent_cnid,
                ),
            ));
            orphaned.push(OrphanedEntry {
                id: entry.cnid as u64,
                name: entry.name.clone(),
                is_directory: entry.is_dir,
                missing_parent_id: entry.parent_cnid as u64,
            });
        }
    }

    // Threads: every thread targets some CNID via its key (parent_cnid in
    // the key is actually the CNID being described). A thread without a
    // matching folder/file record is an orphan thread; a folder without a
    // matching thread isn't strictly required but is unusual.
    let known_cnids: HashSet<u32> = walk
        .folder_cnids
        .iter()
        .copied()
        .chain(walk.file_cnids.iter().copied())
        .collect();
    for &thread_cnid in &walk.threads {
        if thread_cnid == ROOT_FOLDER_CNID {
            continue;
        }
        if !known_cnids.contains(&thread_cnid) {
            errors.push(issue(
                HfsPlusFsckCode::OrphanedThread,
                format!(
                    "thread record targets CNID {thread_cnid} which has no folder \
                     or file record"
                ),
            ));
        }
    }

    // Duplicate (parent, name) catalog keys. macOS fsck_hfs reports this
    // as "key for X is out of order" because two records sharing a key
    // violates the B-tree's strict ordering invariant. Listing each
    // collision lets users see exactly what got duplicated.
    for hit in &walk.duplicate_keys {
        let printable: String = hit
            .name
            .bytes()
            .map(|b| {
                if (0x20..0x7f).contains(&b) {
                    (b as char).to_string()
                } else {
                    format!("\\x{b:02x}")
                }
            })
            .collect();
        errors.push(issue(
            HfsPlusFsckCode::DuplicateCatalogKey,
            format!(
                "two catalog records share key (parent={}, name=\"{}\"): CNIDs {} and {}",
                hit.parent_cnid, printable, hit.cnid_a, hit.cnid_b
            ),
        ));
    }

    // Per-folder valence: each folder record stores a `valence` field
    // (number of immediate children). Compare against the actual child
    // count from the walk. A mismatch usually indicates a stale folder
    // record left over from a botched edit, or a missed valence update.
    for (cnid, &stored_valence) in &walk.folder_valence {
        let actual = walk.child_counts.get(cnid).copied().unwrap_or(0);
        if stored_valence != actual {
            errors.push(issue(
                HfsPlusFsckCode::ParentValenceMismatch,
                format!(
                    "folder CNID {cnid} stores valence={stored_valence} but catalog \
                     walk shows {actual} immediate children"
                ),
            ));
        }
    }

    if !walk.duplicate_cnids.is_empty() {
        let mut sample: Vec<_> = walk.duplicate_cnids.iter().take(8).collect();
        sample.sort();
        errors.push(issue(
            HfsPlusFsckCode::DuplicateCnid,
            format!(
                "{} CNID(s) appear in more than one catalog record (sample: {:?})",
                walk.duplicate_cnids.len(),
                sample
            ),
        ));
    }

    // ---- Phase 3: Bitmap consistency ----------------------------------
    let total_blocks = fs.total_blocks();
    let free_blocks = fs.vh_free_blocks();
    let expected_alloc = total_blocks.saturating_sub(free_blocks);
    let bitmap = fs.read_allocation_bitmap_for_fsck()?;
    let bitmap_bits = (bitmap.len() as u64).saturating_mul(8);
    if bitmap_bits < total_blocks as u64 {
        errors.push(issue(
            HfsPlusFsckCode::BitmapTooShort,
            format!(
                "allocation bitmap covers {bitmap_bits} bits but volume has \
                 {total_blocks} blocks"
            ),
        ));
    } else {
        let mut allocated: u64 = 0;
        // Count only bits within the volume; bits past total_blocks are
        // padding and should be zero on a healthy volume.
        for block in 0..total_blocks {
            let byte = (block / 8) as usize;
            let bit = 7 - (block % 8) as u8;
            if bitmap[byte] & (1 << bit) != 0 {
                allocated += 1;
            }
        }
        if allocated != expected_alloc as u64 {
            errors.push(issue(
                HfsPlusFsckCode::BitmapAllocCountMismatch,
                format!(
                    "bitmap reports {allocated} blocks allocated but \
                     total_blocks - free_blocks = {expected_alloc}"
                ),
            ));
        }
    }

    let stats = FsckStats {
        files_checked: total_file_count,
        directories_checked: total_folder_count,
        extra: vec![
            ("threads".to_string(), walk.threads.len().to_string()),
            ("total_blocks".to_string(), total_blocks.to_string()),
            ("free_blocks".to_string(), free_blocks.to_string()),
        ],
    };

    Ok(FsckResult {
        errors,
        warnings,
        stats,
        repairable: false,
        orphaned_entries: orphaned,
    })
}

#[derive(Default)]
struct CatalogWalk {
    /// Every folder/file record encountered (parsed metadata).
    entries: Vec<WalkedEntry>,
    folder_cnids: HashSet<u32>,
    file_cnids: HashSet<u32>,
    /// CNIDs claimed by thread records (the first 4 bytes after the
    /// 2-byte record_type + 2-byte reserved are the parent CNID, but the
    /// thread's *key* parent is the target CNID).
    threads: Vec<u32>,
    duplicate_cnids: HashSet<u32>,
    seen_cnids: HashMap<u32, ()>,
    /// `(parent_cnid, name_bytes)` keys we've already seen on a folder /
    /// file record. A second hit means two records share the same catalog
    /// key, which fsck_hfs flags as "key out of order" because the B-tree
    /// invariant is violated. Tracked separately from CNIDs so we catch
    /// the case where two records have *different* CNIDs but the same
    /// `(parent, name)` — e.g. a stale `\0\0\0\0HFS+ Private Data` left
    /// behind after a botched edit.
    seen_keys: HashMap<(u32, Vec<u8>), u32>,
    duplicate_keys: Vec<DuplicateKeyHit>,
    /// Per-folder child counts derived from the walk. Compared against
    /// each folder record's stored `valence` field at the end of phase 2.
    child_counts: HashMap<u32, u32>,
    /// Per-folder valence values (as stored in the on-disk folder record).
    folder_valence: HashMap<u32, u32>,
}

#[derive(Debug)]
struct DuplicateKeyHit {
    parent_cnid: u32,
    name: String,
    cnid_a: u32,
    cnid_b: u32,
}

struct WalkedEntry {
    cnid: u32,
    parent_cnid: u32,
    name: String,
    is_dir: bool,
}

impl CatalogWalk {
    fn record_key_seen(&mut self, parent_cnid: u32, name_bytes: &[u8], name_str: &str, cnid: u32) {
        let key = (parent_cnid, name_bytes.to_vec());
        match self.seen_keys.insert(key, cnid) {
            None => {}
            Some(first_cnid) => {
                self.duplicate_keys.push(DuplicateKeyHit {
                    parent_cnid,
                    name: name_str.to_string(),
                    cnid_a: first_cnid,
                    cnid_b: cnid,
                });
            }
        }
    }

    fn visit(&mut self, rec: &[u8]) {
        if rec.len() < 4 {
            return;
        }
        let key_len = BigEndian::read_u16(&rec[0..2]) as usize;
        if key_len < 6 || 2 + key_len > rec.len() {
            return;
        }
        let key = &rec[2..2 + key_len];
        let key_parent_cnid = BigEndian::read_u32(&key[0..4]);
        let name_length = BigEndian::read_u16(&key[4..6]) as usize;
        if 6 + name_length * 2 > key.len() {
            return;
        }
        let name_bytes = &key[6..6 + name_length * 2];
        let name_utf16: Vec<u16> = (0..name_length)
            .map(|i| BigEndian::read_u16(&name_bytes[i * 2..i * 2 + 2]))
            .collect();
        let name = String::from_utf16(&name_utf16).unwrap_or_default();

        let mut body_start = 2 + key_len;
        if !body_start.is_multiple_of(2) {
            body_start += 1;
        }
        if body_start + 2 > rec.len() {
            return;
        }
        let body = &rec[body_start..];
        let record_type = BigEndian::read_i16(&body[0..2]);
        match record_type {
            CATALOG_FOLDER => {
                if body.len() < 88 {
                    return;
                }
                let cnid = BigEndian::read_u32(&body[8..12]);
                let valence = BigEndian::read_u32(&body[4..8]);
                if self.seen_cnids.insert(cnid, ()).is_some() {
                    self.duplicate_cnids.insert(cnid);
                }
                self.record_key_seen(key_parent_cnid, name_bytes, &name, cnid);
                if key_parent_cnid != ROOT_PARENT_CNID {
                    *self.child_counts.entry(key_parent_cnid).or_insert(0) += 1;
                }
                self.folder_valence.insert(cnid, valence);
                self.folder_cnids.insert(cnid);
                self.entries.push(WalkedEntry {
                    cnid,
                    parent_cnid: key_parent_cnid,
                    name,
                    is_dir: true,
                });
            }
            CATALOG_FILE => {
                if body.len() < 248 {
                    return;
                }
                let cnid = BigEndian::read_u32(&body[8..12]);
                if self.seen_cnids.insert(cnid, ()).is_some() {
                    self.duplicate_cnids.insert(cnid);
                }
                self.record_key_seen(key_parent_cnid, name_bytes, &name, cnid);
                *self.child_counts.entry(key_parent_cnid).or_insert(0) += 1;
                self.file_cnids.insert(cnid);
                self.entries.push(WalkedEntry {
                    cnid,
                    parent_cnid: key_parent_cnid,
                    name,
                    is_dir: false,
                });
            }
            CATALOG_FOLDER_THREAD | CATALOG_FILE_THREAD => {
                // Thread record key is `(target_cnid, "")`; the body
                // restates parent + name of the targeted entry.
                self.threads.push(key_parent_cnid);
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use byteorder::{BigEndian, ByteOrder};
    use std::io::Cursor;

    use super::super::filesystem::Filesystem;
    use super::super::hfsplus::{create_blank_hfsplus, HfsPlusFilesystem};

    /// VH file_count lives at offset 1024 + 32 (big-endian u32). Mirror to
    /// the alternate VH at end-of-volume too so re-open uses the poked
    /// value. Same trick the Step 10 xattr test uses.
    fn poke_vh_file_count(img: &mut [u8], new_count: u32) {
        let primary = 1024 + 32;
        let mut buf = [0u8; 4];
        BigEndian::write_u32(&mut buf, new_count);
        img[primary..primary + 4].copy_from_slice(&buf);
        let alt = img.len() - 1024 + 32;
        img[alt..alt + 4].copy_from_slice(&buf);
    }

    #[test]
    fn fsck_blank_volume_is_clean() {
        let img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "Clean", false);
        let cursor = Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).expect("open");
        let result = fs.fsck().expect("fsck supported").expect("fsck ran");
        assert!(
            result.errors.is_empty(),
            "blank volume should be clean; errors: {:?}",
            result.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        assert!(result.is_clean());
    }

    #[test]
    fn fsck_blank_hfsx_volume_is_clean() {
        let img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "ClnX", true);
        let cursor = Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).expect("open");
        let result = fs.fsck().expect("fsck supported").expect("fsck ran");
        assert!(
            result.errors.is_empty(),
            "blank HFSX volume should be clean; errors: {:?}",
            result.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn fsck_flags_file_count_mismatch() {
        let mut img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "Bad", false);
        poke_vh_file_count(&mut img, 999);
        let cursor = Cursor::new(img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).expect("open");
        let result = fs.fsck().expect("fsck supported").expect("fsck ran");
        let codes: Vec<&str> = result.errors.iter().map(|e| e.code.as_str()).collect();
        assert!(
            codes.contains(&"FileCountMismatch"),
            "expected FileCountMismatch, got {codes:?}"
        );
        assert!(!result.is_clean());
    }
}
