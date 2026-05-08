//! Source-side snapshot types for the HFS+ reformat-and-copy pipeline.
//!
//! Step 10 of `docs/hfsplus_enhancements.md`. Parallel to `hfs_clone.rs`,
//! but for HFS+/HFSX. Nothing here mutates the source volume; these structs
//! capture every byte of metadata that must survive a clone-shrink so later
//! steps can replay it onto a freshly-formatted target.
//!
//! Step 14 wired extended-attribute capture into both `SourceFileSnapshot`
//! and `SourceDirSnapshot`. Step 17 added file-hardlink support: stubs
//! routed into a separate `SourceHardlinkSnapshot` list and inodes
//! (files under `\0\0\0\0HFS+ Private Data` named `iNode<N>`) flagged
//! with `is_inode = true`. Step 18 (this revision) does the same for
//! directory hardlinks: file rows with FInfo `fdrp`/`MACS` route into
//! `SourceDirHardlinkSnapshot`, and folders under
//! `.HFS+ Private Directory Data\r` named `dir_<N>` are flagged with
//! `is_dir_inode = true`. Replay (Step 21) emits each directory inode
//! once and points each link at it.

use std::io::{Read, Seek};

use byteorder::{BigEndian, ByteOrder};

use super::filesystem::FilesystemError;
use super::hfs_common::walk_leaf_records;
use super::hfsplus::{HfsPlusFilesystem, XattrRecord};

/// Fork-type byte used in extents-overflow keys (mirrors the constant in
/// `hfsplus`).
const HFSPLUS_FORK_DATA: u8 = 0x00;
const HFSPLUS_FORK_RESOURCE: u8 = 0xFF;

/// Catalog record types.
const CATALOG_FOLDER: i16 = 1;
const CATALOG_FILE: i16 = 2;

/// FInfo magic that flags an HFS+ hardlink. File hardlinks carry
/// `fdType='hlnk' fdCreator='hfs+'`; directory hardlinks (10.5+) carry
/// `fdType='fdrp' fdCreator='MACS'`. Both are unsupported until Phase 7.
const HARDLINK_FILE_TYPE: &[u8; 4] = b"hlnk";
const HARDLINK_FILE_CREATOR: &[u8; 4] = b"hfs+";
const HARDLINK_DIR_TYPE: &[u8; 4] = b"fdrp";
const HARDLINK_DIR_CREATOR: &[u8; 4] = b"MACS";

/// Volume-level metadata captured from the source.
#[derive(Debug, Clone)]
pub struct SourceVolumeSnapshot {
    /// Volume label (decoded UTF-8 of the root folder key name).
    pub label: String,
    /// 0x482B (HFS+) or 0x4858 (HFSX). Preserved so the target can be
    /// re-formatted with the same case-sensitivity behavior.
    pub signature: u16,
    /// VH attributes word — clone needs this for the journaled bit, the
    /// case-sensitivity bit (HFSX), and a few low-level flags.
    pub attributes: u32,
    pub create_date: u32,
    pub modify_date: u32,
    pub backup_date: u32,
    pub checked_date: u32,
    /// 8 × u32 Finder info. `finder_info[0]` is the blessed System Folder
    /// CNID; other slots are copied verbatim with any CNID remap applied
    /// by the caller in Step 21.
    pub finder_info: [u32; 8],
    /// Source allocation block size (informational — clone may pick a
    /// different block size on the target).
    pub block_size: u32,
    /// Source total allocation blocks (informational).
    pub total_blocks: u32,
    pub file_count: u32,
    pub folder_count: u32,
    /// First 1024 bytes of the partition. Always zero on macOS-formatted
    /// volumes; copied verbatim by clone for round-trip parity.
    pub boot_blocks: [u8; 1024],
}

/// Per-directory metadata captured from a `kHFSPlusFolderRecord`.
#[derive(Debug, Clone)]
pub struct SourceDirSnapshot {
    pub name: String,
    /// Original UTF-16BE name code units. Kept so the target writer can
    /// re-key the catalog with byte-identical names (UTF-8 round-tripping
    /// can lose detail on some Unicode forms).
    pub name_utf16: Vec<u16>,
    /// Source CNID (`folderID`). Targets get fresh CNIDs so this is for
    /// building the source→target CNID map only.
    pub cnid: u32,
    pub parent_cnid: u32,
    /// Number of immediate children (directly from the source record).
    pub valence: u32,
    /// `flags` field (offset 2 of the folder record).
    pub flags: u16,
    /// `userInfo` — 16 bytes of FolderInfo (`DInfo`).
    pub finder_info: [u8; 16],
    /// `finderInfo` — 16 bytes of ExtendedFolderInfo (`DXInfo`).
    pub extended_finder_info: [u8; 16],
    /// `permissions` — 16 bytes of `HFSPlusBSDInfo` (zeros on most volumes).
    pub bsd_info: [u8; 16],
    pub create_date: u32,
    pub content_mod_date: u32,
    pub attribute_mod_date: u32,
    pub access_date: u32,
    pub backup_date: u32,
    pub text_encoding: u32,
    /// Extended attributes attached to this CNID. `XattrRecord::name` holds
    /// the key (e.g. `com.apple.FinderInfo`); `XattrRecord::kind` is the
    /// inline / fork / extents payload. Empty vector when the volume has no
    /// attributes file or no records for this CNID. `pub(crate)` because
    /// `XattrRecord` is itself crate-private; promote both together if a
    /// non-test external consumer ever needs to read them.
    pub(crate) xattrs: Vec<XattrRecord>,
    /// `true` for folders that live under the source's
    /// `.HFS+ Private Directory Data\r` directory whose name parses as
    /// `dir_<N>` — i.e. directory-hardlink inodes. Replay must emit them
    /// through `create_dir_hardlink_inode` rather than as ordinary
    /// directories in the user-visible tree.
    pub is_dir_inode: bool,
    /// Source iNodeNum (decoded from the `dir_<N>` folder name) for
    /// directory-inode rows; `None` for ordinary folders.
    pub inode_num: Option<u32>,
}

/// Per-file metadata captured from a `kHFSPlusFileRecord`.
#[derive(Debug, Clone)]
pub struct SourceFileSnapshot {
    pub name: String,
    pub name_utf16: Vec<u16>,
    /// Source CNID (`fileID`).
    pub cnid: u32,
    pub parent_cnid: u32,
    pub flags: u16,
    /// `userInfo` — 16 bytes of FileInfo (`FInfo`). `fdType` at byte 0 and
    /// `fdCreator` at byte 4 are the type/creator codes; `fdFlags` at
    /// byte 8 is the Finder flags word (0x8000 = `kIsAlias`).
    pub finder_info: [u8; 16],
    /// `finderInfo` — 16 bytes of ExtendedFileInfo (`FXInfo`).
    pub extended_finder_info: [u8; 16],
    /// `permissions` — 16 bytes of `HFSPlusBSDInfo` (zeros on most volumes).
    pub bsd_info: [u8; 16],
    pub create_date: u32,
    pub content_mod_date: u32,
    pub attribute_mod_date: u32,
    pub access_date: u32,
    pub backup_date: u32,
    pub text_encoding: u32,
    /// Verbatim data fork bytes (`logical_size` long). Empty for forkless
    /// files like classic-Mac aliases without data.
    pub data_fork_bytes: Vec<u8>,
    /// Verbatim resource fork bytes. Empty when the file has no resource
    /// fork.
    pub rsrc_fork_bytes: Vec<u8>,
    /// Extended attributes attached to this CNID. See
    /// [`SourceDirSnapshot::xattrs`] for layout details.
    pub(crate) xattrs: Vec<XattrRecord>,
    /// `true` for files that live under the source's `HFS+ Private Data`
    /// directory whose name parses as `iNode<N>` — i.e. file-hardlink
    /// inodes. Replay must emit them through `create_hardlink_inode`
    /// rather than as ordinary files in the user-visible tree, and the
    /// hardlink stubs that reference them carry the source CNID stored
    /// here. Always `false` until [`SourceCatalogSnapshot::capture`]
    /// resolves the private dir.
    pub is_inode: bool,
    /// Source iNodeNum (decoded from the `iNode<N>` filename) for inode
    /// rows; `None` for ordinary files. Carried so the replay path can
    /// emit each hardlink stub with the same iNodeNum the inode itself
    /// will be created under on the target.
    pub inode_num: Option<u32>,
}

/// One file-hardlink stub captured from the source catalog. Replay creates
/// `inode_count` inodes once and N stub rows pointing at them.
#[derive(Debug, Clone)]
pub struct SourceHardlinkSnapshot {
    /// Stub name visible in the user directory.
    pub name: String,
    pub name_utf16: Vec<u16>,
    /// Source CNID of the stub catalog row (not the inode).
    pub cnid: u32,
    /// User-visible parent CNID.
    pub parent_cnid: u32,
    /// Source CNID of the inode this stub references. The inode appears
    /// in `SourceCatalogSnapshot::files` with `is_inode = true` and the
    /// matching `cnid`.
    pub inode_source_cnid: u32,
    /// Original iNodeNum (`bsdInfo.special` u32). Replay can re-use this
    /// number directly since target CNIDs differ from source CNIDs.
    pub inode_num: u32,
    pub flags: u16,
    pub finder_info: [u8; 16],
    pub extended_finder_info: [u8; 16],
    pub bsd_info: [u8; 16],
    pub create_date: u32,
    pub content_mod_date: u32,
    pub attribute_mod_date: u32,
    pub access_date: u32,
    pub backup_date: u32,
    pub text_encoding: u32,
    pub(crate) xattrs: Vec<XattrRecord>,
}

impl SourceFileSnapshot {
    /// Decoded type code (FInfo bytes 0..4) as a 4-character string.
    pub fn type_code(&self) -> String {
        decode_fourcc(&self.finder_info[0..4])
    }

    /// Decoded creator code (FInfo bytes 4..8) as a 4-character string.
    pub fn creator_code(&self) -> String {
        decode_fourcc(&self.finder_info[4..8])
    }
}

/// One directory-hardlink stub captured from the source catalog. The stub
/// is encoded as a FILE record on disk; replay re-emits it the same way
/// (FInfo `fdrp`/`MACS` plus `bsdInfo.special = inode_num`).
#[derive(Debug, Clone)]
pub struct SourceDirHardlinkSnapshot {
    pub name: String,
    pub name_utf16: Vec<u16>,
    pub cnid: u32,
    pub parent_cnid: u32,
    /// Source CNID of the directory inode this stub references. The
    /// inode appears in `SourceCatalogSnapshot::dirs` with
    /// `is_dir_inode = true` and the matching `cnid`.
    pub inode_dir_source_cnid: u32,
    pub inode_num: u32,
    pub flags: u16,
    pub finder_info: [u8; 16],
    pub extended_finder_info: [u8; 16],
    pub bsd_info: [u8; 16],
    pub create_date: u32,
    pub content_mod_date: u32,
    pub attribute_mod_date: u32,
    pub access_date: u32,
    pub backup_date: u32,
    pub text_encoding: u32,
    pub(crate) xattrs: Vec<XattrRecord>,
}

/// Full capture of a source HFS+/HFSX volume: volume metadata plus every
/// directory and file record. Catalog *thread* records are not captured —
/// the target writer recreates them from scratch.
#[derive(Debug, Clone)]
pub struct SourceCatalogSnapshot {
    pub volume: SourceVolumeSnapshot,
    pub dirs: Vec<SourceDirSnapshot>,
    /// Every captured file row, both user-visible files and the per-inode
    /// `iNode<N>` rows under `HFS+ Private Data`. Inodes carry
    /// `is_inode = true` and an `inode_num`; replay must emit them through
    /// the dedicated hardlink-inode helper rather than as user-tree files.
    pub files: Vec<SourceFileSnapshot>,
    /// Hardlink stub rows. Each references one `is_inode=true` entry in
    /// `files` by source CNID.
    pub hardlinks: Vec<SourceHardlinkSnapshot>,
    /// Directory-hardlink stub rows. Each references one
    /// `is_dir_inode=true` entry in `dirs` by source CNID.
    pub dir_hardlinks: Vec<SourceDirHardlinkSnapshot>,
}

impl SourceVolumeSnapshot {
    /// Capture volume-level metadata from an opened source filesystem. The
    /// source is read but never mutated.
    pub fn capture<R: Read + Seek>(fs: &mut HfsPlusFilesystem<R>) -> Result<Self, FilesystemError> {
        let boot_blocks = fs.read_boot_blocks()?;
        Ok(SourceVolumeSnapshot {
            label: fs.label().to_string(),
            signature: fs.signature(),
            attributes: fs.vh_attributes(),
            create_date: fs.vh_create_date(),
            modify_date: fs.vh_modify_date(),
            backup_date: fs.vh_backup_date(),
            checked_date: fs.vh_checked_date(),
            finder_info: fs.vh_finder_info(),
            block_size: fs.block_size(),
            total_blocks: fs.total_blocks(),
            file_count: fs.vh_file_count(),
            folder_count: fs.vh_folder_count(),
            boot_blocks,
        })
    }
}

impl SourceCatalogSnapshot {
    /// Capture volume + every directory + every file record from the source
    /// catalog. Walks the leaf-node chain of the catalog B-tree (skipping
    /// thread records) and reads each file's data + resource fork into the
    /// snapshot. Index nodes are ignored.
    ///
    /// File hardlinks (Step 17) are captured into a separate
    /// [`SourceHardlinkSnapshot`] list and the inode rows under
    /// `\0\0\0\0HFS+ Private Data` are flagged with `is_inode = true`.
    /// Directory hardlinks (Step 18) still trip the `Unsupported` guard.
    pub fn capture<R: Read + Seek>(fs: &mut HfsPlusFilesystem<R>) -> Result<Self, FilesystemError> {
        let volume = SourceVolumeSnapshot::capture(fs)?;

        let mut dirs: Vec<SourceDirSnapshot> = Vec::new();
        let mut files: Vec<SourceFileSnapshot> = Vec::new();
        let mut pending_forks: Vec<([u8; 80], [u8; 80])> = Vec::new();
        let mut pending_links: Vec<PendingHardlink> = Vec::new();
        let mut pending_dir_links: Vec<PendingDirHardlink> = Vec::new();
        let mut walk_err: Option<FilesystemError> = None;
        {
            let catalog = fs.catalog_data();
            let node_size = fs.catalog_node_size();
            let first_leaf = fs.catalog_first_leaf();
            walk_leaf_records(
                catalog,
                first_leaf,
                node_size,
                |_node_idx, _rec_idx, _abs_off, rec| -> Option<()> {
                    match parse_catalog_record(rec) {
                        Ok(ParsedRecord::Folder(d)) => {
                            dirs.push(d);
                            None
                        }
                        Ok(ParsedRecord::File {
                            file,
                            data_fork,
                            rsrc_fork,
                        }) => {
                            files.push(file);
                            pending_forks.push((data_fork, rsrc_fork));
                            None
                        }
                        Ok(ParsedRecord::Hardlink(p)) => {
                            pending_links.push(p);
                            None
                        }
                        Ok(ParsedRecord::DirHardlink(p)) => {
                            pending_dir_links.push(p);
                            None
                        }
                        Ok(ParsedRecord::Thread) | Ok(ParsedRecord::Skip) => None,
                        Err(e) => {
                            walk_err = Some(e);
                            Some(())
                        }
                    }
                },
            );
        }
        if let Some(e) = walk_err {
            return Err(e);
        }

        // Resolve the source's HFS+ Private Data dir CNID and flag every
        // captured file whose parent matches and whose name is `iNode<N>`
        // as an inode. Build the iNodeNum -> source CNID map for the
        // hardlink resolution pass below.
        let private_name = hfsplus_private_dir_name();
        let dir_private_name = hfsplus_dir_private_dir_name();
        let private_cnid = dirs.iter().find_map(|d| {
            if d.name == private_name {
                Some(d.cnid)
            } else {
                None
            }
        });
        let dir_private_cnid = dirs.iter().find_map(|d| {
            if d.name == dir_private_name {
                Some(d.cnid)
            } else {
                None
            }
        });
        let mut inode_num_to_source_cnid: std::collections::HashMap<u32, u32> =
            std::collections::HashMap::new();
        let mut dir_inode_num_to_source_cnid: std::collections::HashMap<u32, u32> =
            std::collections::HashMap::new();
        if let Some(pcnid) = dir_private_cnid {
            for d in dirs.iter_mut() {
                if d.parent_cnid != pcnid {
                    continue;
                }
                let Some(rest) = d.name.strip_prefix("dir_") else {
                    continue;
                };
                let Ok(num) = rest.parse::<u32>() else {
                    continue;
                };
                d.is_dir_inode = true;
                d.inode_num = Some(num);
                dir_inode_num_to_source_cnid.insert(num, d.cnid);
            }
        }
        if let Some(pcnid) = private_cnid {
            for f in files.iter_mut() {
                if f.parent_cnid != pcnid {
                    continue;
                }
                let Some(rest) = f.name.strip_prefix("iNode") else {
                    continue;
                };
                let Ok(num) = rest.parse::<u32>() else {
                    continue;
                };
                f.is_inode = true;
                f.inode_num = Some(num);
                inode_num_to_source_cnid.insert(num, f.cnid);
            }
        }

        // Resolve pending hardlink stubs against the inode map. A stub
        // with a missing iNode is treated as a corrupted source.
        let mut hardlinks: Vec<SourceHardlinkSnapshot> = Vec::with_capacity(pending_links.len());
        for p in pending_links {
            let inode_source_cnid = inode_num_to_source_cnid.get(&p.inode_num).copied()
                .ok_or_else(|| FilesystemError::InvalidData(format!(
                    "source HFS+ catalog references iNode{} but no matching entry exists in HFS+ Private Data",
                    p.inode_num,
                )))?;
            hardlinks.push(SourceHardlinkSnapshot {
                name: p.name,
                name_utf16: p.name_utf16,
                cnid: p.cnid,
                parent_cnid: p.parent_cnid,
                inode_source_cnid,
                inode_num: p.inode_num,
                flags: p.flags,
                finder_info: p.finder_info,
                extended_finder_info: p.extended_finder_info,
                bsd_info: p.bsd_info,
                create_date: p.create_date,
                content_mod_date: p.content_mod_date,
                attribute_mod_date: p.attribute_mod_date,
                access_date: p.access_date,
                backup_date: p.backup_date,
                text_encoding: p.text_encoding,
                xattrs: Vec::new(),
            });
        }

        let mut dir_hardlinks: Vec<SourceDirHardlinkSnapshot> =
            Vec::with_capacity(pending_dir_links.len());
        for p in pending_dir_links {
            let inode_dir_source_cnid = dir_inode_num_to_source_cnid
                .get(&p.inode_num)
                .copied()
                .ok_or_else(|| {
                    FilesystemError::InvalidData(format!(
                        "source HFS+ catalog references dir_{} but no matching entry exists \
                         in .HFS+ Private Directory Data",
                        p.inode_num,
                    ))
                })?;
            dir_hardlinks.push(SourceDirHardlinkSnapshot {
                name: p.name,
                name_utf16: p.name_utf16,
                cnid: p.cnid,
                parent_cnid: p.parent_cnid,
                inode_dir_source_cnid,
                inode_num: p.inode_num,
                flags: p.flags,
                finder_info: p.finder_info,
                extended_finder_info: p.extended_finder_info,
                bsd_info: p.bsd_info,
                create_date: p.create_date,
                content_mod_date: p.content_mod_date,
                attribute_mod_date: p.attribute_mod_date,
                access_date: p.access_date,
                backup_date: p.backup_date,
                text_encoding: p.text_encoding,
                xattrs: Vec::new(),
            });
        }

        // Read fork bytes and xattrs now that the catalog borrow is released.
        for (file, (data_fork, rsrc_fork)) in files.iter_mut().zip(pending_forks.into_iter()) {
            let data_logical = BigEndian::read_u64(&data_fork[0..8]);
            if data_logical > 0 {
                file.data_fork_bytes =
                    fs.read_user_fork_bytes(file.cnid, HFSPLUS_FORK_DATA, &data_fork)?;
            }
            let rsrc_logical = BigEndian::read_u64(&rsrc_fork[0..8]);
            if rsrc_logical > 0 {
                file.rsrc_fork_bytes =
                    fs.read_user_fork_bytes(file.cnid, HFSPLUS_FORK_RESOURCE, &rsrc_fork)?;
            }
            file.xattrs = fs.list_xattrs(file.cnid)?;
        }
        for dir in dirs.iter_mut() {
            dir.xattrs = fs.list_xattrs(dir.cnid)?;
        }
        for link in hardlinks.iter_mut() {
            link.xattrs = fs.list_xattrs(link.cnid)?;
        }
        for link in dir_hardlinks.iter_mut() {
            link.xattrs = fs.list_xattrs(link.cnid)?;
        }

        Ok(SourceCatalogSnapshot {
            volume,
            dirs,
            files,
            hardlinks,
            dir_hardlinks,
        })
    }
}

/// Outcome of parsing a single catalog leaf record. `Skip` covers index /
/// malformed records that we silently drop; `Thread` is the (parent, "")
/// or (parent, name) thread record we don't capture (target rebuilds them).
enum ParsedRecord {
    Folder(SourceDirSnapshot),
    File {
        file: SourceFileSnapshot,
        data_fork: [u8; 80],
        rsrc_fork: [u8; 80],
    },
    Hardlink(PendingHardlink),
    DirHardlink(PendingDirHardlink),
    Thread,
    Skip,
}

/// Catalog file row identified as a hardlink stub during the first pass.
/// Held until [`SourceCatalogSnapshot::capture`] resolves the private dir
/// and links each stub to its captured inode.
struct PendingHardlink {
    name: String,
    name_utf16: Vec<u16>,
    cnid: u32,
    parent_cnid: u32,
    inode_num: u32,
    flags: u16,
    finder_info: [u8; 16],
    extended_finder_info: [u8; 16],
    bsd_info: [u8; 16],
    create_date: u32,
    content_mod_date: u32,
    attribute_mod_date: u32,
    access_date: u32,
    backup_date: u32,
    text_encoding: u32,
}

/// Pending directory-hardlink stub. Same shape as `PendingHardlink` but
/// resolved against the dir-inode map instead of the file-inode map.
struct PendingDirHardlink {
    name: String,
    name_utf16: Vec<u16>,
    cnid: u32,
    parent_cnid: u32,
    inode_num: u32,
    flags: u16,
    finder_info: [u8; 16],
    extended_finder_info: [u8; 16],
    bsd_info: [u8; 16],
    create_date: u32,
    content_mod_date: u32,
    attribute_mod_date: u32,
    access_date: u32,
    backup_date: u32,
    text_encoding: u32,
}

/// Build the source's `.HFS+ Private Directory Data\r` directory name.
fn hfsplus_dir_private_dir_name() -> String {
    let mut s = String::with_capacity(28 + 1);
    s.push_str(".HFS+ Private Directory Data");
    s.push('\u{D}');
    s
}

/// Build the source's `\0\0\0\0HFS+ Private Data` directory name.
/// Mirrors the helper in `hfsplus.rs` (which is module-private there).
fn hfsplus_private_dir_name() -> String {
    let mut s = String::with_capacity(4 + 17);
    for _ in 0..4 {
        s.push('\u{0}');
    }
    s.push_str("HFS+ Private Data");
    s
}

fn parse_catalog_record(rec: &[u8]) -> Result<ParsedRecord, FilesystemError> {
    if rec.len() < 4 {
        return Ok(ParsedRecord::Skip);
    }
    let key_len = BigEndian::read_u16(&rec[0..2]) as usize;
    if key_len < 6 || 2 + key_len > rec.len() {
        return Ok(ParsedRecord::Skip);
    }
    let key = &rec[2..2 + key_len];
    let parent_cnid = BigEndian::read_u32(&key[0..4]);
    let name_length = BigEndian::read_u16(&key[4..6]) as usize;
    if 6 + name_length * 2 > key.len() {
        return Ok(ParsedRecord::Skip);
    }
    let name_bytes = &key[6..6 + name_length * 2];
    let name_utf16: Vec<u16> = (0..name_length)
        .map(|i| BigEndian::read_u16(&name_bytes[i * 2..i * 2 + 2]))
        .collect();
    let name = String::from_utf16(&name_utf16).unwrap_or_else(|_| {
        name_utf16
            .iter()
            .map(|&u| char::from_u32(u as u32).unwrap_or('?'))
            .collect()
    });

    // Record body follows the key, padded to even offset.
    let mut body_start = 2 + key_len;
    if !body_start.is_multiple_of(2) {
        body_start += 1;
    }
    if body_start + 2 > rec.len() {
        return Ok(ParsedRecord::Skip);
    }
    let body = &rec[body_start..];
    let record_type = BigEndian::read_i16(&body[0..2]);

    match record_type {
        CATALOG_FOLDER => {
            if body.len() < 88 {
                return Ok(ParsedRecord::Skip);
            }
            let flags = BigEndian::read_u16(&body[2..4]);
            let valence = BigEndian::read_u32(&body[4..8]);
            let cnid = BigEndian::read_u32(&body[8..12]);
            let create_date = BigEndian::read_u32(&body[12..16]);
            let content_mod_date = BigEndian::read_u32(&body[16..20]);
            let attribute_mod_date = BigEndian::read_u32(&body[20..24]);
            let access_date = BigEndian::read_u32(&body[24..28]);
            let backup_date = BigEndian::read_u32(&body[28..32]);
            let mut bsd_info = [0u8; 16];
            bsd_info.copy_from_slice(&body[32..48]);
            let mut finder_info = [0u8; 16];
            finder_info.copy_from_slice(&body[48..64]);
            let mut extended_finder_info = [0u8; 16];
            extended_finder_info.copy_from_slice(&body[64..80]);
            let text_encoding = BigEndian::read_u32(&body[80..84]);

            // Directory hardlinks are encoded as FILE records (handled in
            // the CATALOG_FILE branch below), not as FOLDER records — so
            // there is no fdrp/MACS check here. The folder-side flag
            // `is_dir_inode` is set in capture's second pass for folders
            // living under `.HFS+ Private Directory Data\r`.
            Ok(ParsedRecord::Folder(SourceDirSnapshot {
                name,
                name_utf16,
                cnid,
                parent_cnid,
                valence,
                flags,
                finder_info,
                extended_finder_info,
                bsd_info,
                create_date,
                content_mod_date,
                attribute_mod_date,
                access_date,
                backup_date,
                text_encoding,
                xattrs: Vec::new(),
                is_dir_inode: false,
                inode_num: None,
            }))
        }
        CATALOG_FILE => {
            if body.len() < 248 {
                return Ok(ParsedRecord::Skip);
            }
            let flags = BigEndian::read_u16(&body[2..4]);
            // body[4..8] = reserved1
            let cnid = BigEndian::read_u32(&body[8..12]);
            let create_date = BigEndian::read_u32(&body[12..16]);
            let content_mod_date = BigEndian::read_u32(&body[16..20]);
            let attribute_mod_date = BigEndian::read_u32(&body[20..24]);
            let access_date = BigEndian::read_u32(&body[24..28]);
            let backup_date = BigEndian::read_u32(&body[28..32]);
            let mut bsd_info = [0u8; 16];
            bsd_info.copy_from_slice(&body[32..48]);
            let mut finder_info = [0u8; 16];
            finder_info.copy_from_slice(&body[48..64]);
            let mut extended_finder_info = [0u8; 16];
            extended_finder_info.copy_from_slice(&body[64..80]);
            let text_encoding = BigEndian::read_u32(&body[80..84]);
            // body[84..88] = reserved2
            let mut data_fork = [0u8; 80];
            data_fork.copy_from_slice(&body[88..168]);
            let mut rsrc_fork = [0u8; 80];
            rsrc_fork.copy_from_slice(&body[168..248]);

            // File hardlink stub: FInfo `hlnk`/`hfs+`. Route into the
            // pending-hardlinks list so the second pass can resolve the
            // iNodeNum (`bsdInfo.special`, body offset 44..48) against the
            // captured iNode<N> rows under HFS+ Private Data.
            if &finder_info[0..4] == HARDLINK_FILE_TYPE
                && &finder_info[4..8] == HARDLINK_FILE_CREATOR
            {
                let inode_num = BigEndian::read_u32(&body[44..48]);
                return Ok(ParsedRecord::Hardlink(PendingHardlink {
                    name,
                    name_utf16,
                    cnid,
                    parent_cnid,
                    inode_num,
                    flags,
                    finder_info,
                    extended_finder_info,
                    bsd_info,
                    create_date,
                    content_mod_date,
                    attribute_mod_date,
                    access_date,
                    backup_date,
                    text_encoding,
                }));
            }

            // Directory hardlink stub: FILE record with FInfo `fdrp`/`MACS`.
            // The `dir_<N>` directory inode is captured via the FOLDER
            // branch and flagged with `is_dir_inode=true` in capture's
            // second pass.
            if &finder_info[0..4] == HARDLINK_DIR_TYPE && &finder_info[4..8] == HARDLINK_DIR_CREATOR
            {
                let inode_num = BigEndian::read_u32(&body[44..48]);
                return Ok(ParsedRecord::DirHardlink(PendingDirHardlink {
                    name,
                    name_utf16,
                    cnid,
                    parent_cnid,
                    inode_num,
                    flags,
                    finder_info,
                    extended_finder_info,
                    bsd_info,
                    create_date,
                    content_mod_date,
                    attribute_mod_date,
                    access_date,
                    backup_date,
                    text_encoding,
                }));
            }

            Ok(ParsedRecord::File {
                file: SourceFileSnapshot {
                    name,
                    name_utf16,
                    cnid,
                    parent_cnid,
                    flags,
                    finder_info,
                    extended_finder_info,
                    bsd_info,
                    create_date,
                    content_mod_date,
                    attribute_mod_date,
                    access_date,
                    backup_date,
                    text_encoding,
                    data_fork_bytes: Vec::new(),
                    rsrc_fork_bytes: Vec::new(),
                    xattrs: Vec::new(),
                    is_inode: false,
                    inode_num: None,
                },
                data_fork,
                rsrc_fork,
            })
        }
        // 3 = folder thread, 4 = file thread; recreated by target writer.
        3 | 4 => Ok(ParsedRecord::Thread),
        _ => Ok(ParsedRecord::Skip),
    }
}

/// Decode a 4-byte Mac OS type/creator code to a printable string. Mirrors
/// the helper in `hfsplus.rs` (kept private there).
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

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::super::filesystem::{
        CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem,
    };
    use super::super::hfsplus::{create_blank_hfsplus, HfsPlusFilesystem};
    use super::*;

    #[test]
    fn capture_blank_volume_only_has_root_dir() {
        let mut img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "Empty", false);
        let cursor = Cursor::new(&mut img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).expect("open blank");
        let snap = SourceCatalogSnapshot::capture(&mut fs).expect("capture");
        assert_eq!(snap.volume.label, "Empty");
        assert_eq!(snap.volume.block_size, 4096);
        // The catalog walk always visits the root folder record
        // (parent=1, cnid=2). User-level dirs filter on parent_cnid != 1.
        let root = snap
            .dirs
            .iter()
            .find(|d| d.parent_cnid == 1)
            .expect("root dir");
        assert_eq!(root.cnid, 2);
        assert_eq!(root.name, "Empty");
        let user_dirs: Vec<_> = snap.dirs.iter().filter(|d| d.parent_cnid != 1).collect();
        assert!(
            user_dirs.is_empty(),
            "blank should have no user dirs, got: {:?}",
            user_dirs.iter().map(|d| &d.name).collect::<Vec<_>>()
        );
    }

    #[test]
    fn capture_walks_dirs_and_files_and_streams_forks() {
        let mut img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "Work", false);
        {
            let cursor = Cursor::new(&mut img);
            let mut fs = HfsPlusFilesystem::open(cursor, 0).expect("open blank");
            fs.prepare_for_edit().expect("prepare_for_edit");
            let root = fs.root().expect("root entry");
            let _docs = fs
                .create_directory(&root, "Docs", &CreateDirectoryOptions::default())
                .expect("create dir");
            let payload: &[u8] = b"hello world\n";
            let mut data = std::io::Cursor::new(payload);
            fs.create_file(
                &root,
                "note.txt",
                &mut data,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create file");
            fs.sync_metadata().expect("sync");
        }

        let cursor = Cursor::new(&mut img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).expect("re-open");
        let snap = SourceCatalogSnapshot::capture(&mut fs).expect("capture");

        assert_eq!(snap.volume.label, "Work");
        // Root + "Docs". User-only dirs filter on parent_cnid != 1.
        let user_dirs: Vec<&SourceDirSnapshot> =
            snap.dirs.iter().filter(|d| d.parent_cnid != 1).collect();
        assert_eq!(user_dirs.len(), 1, "expected one user directory");
        let docs = user_dirs[0];
        assert_eq!(docs.name, "Docs");
        assert_eq!(docs.parent_cnid, 2);

        assert_eq!(snap.files.len(), 1, "expected one user file");
        let note = &snap.files[0];
        assert_eq!(note.name, "note.txt");
        assert_eq!(note.parent_cnid, 2);
        assert_eq!(note.data_fork_bytes, b"hello world\n");
        assert!(note.rsrc_fork_bytes.is_empty());
    }

    // The xattr round-trip capture test (Step 14) lives in hfsplus.rs's test
    // module since it relies on `make_editable_hfsplus_image_with_inline_xattr`
    // to seed an attributes B-tree (a freshly built blank from
    // `create_blank_hfsplus` has `attributes_file.logical_size == 0`, so
    // `set_xattr` on it would be rejected by `insert_xattr_record`).

    #[test]
    fn capture_no_xattrs_yields_empty_vec() {
        let mut img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "Plain", false);
        {
            let cursor = Cursor::new(&mut img);
            let mut fs = HfsPlusFilesystem::open(cursor, 0).expect("open");
            fs.prepare_for_edit().expect("prepare_for_edit");
            let root = fs.root().expect("root");
            let mut data = std::io::Cursor::new(&b"x"[..]);
            fs.create_file(&root, "x.txt", &mut data, 1, &CreateFileOptions::default())
                .expect("create");
            fs.sync_metadata().expect("sync");
        }
        let cursor = Cursor::new(&mut img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).expect("re-open");
        let snap = SourceCatalogSnapshot::capture(&mut fs).expect("capture");
        for f in &snap.files {
            assert!(f.xattrs.is_empty(), "file {} has xattrs", f.name);
        }
        for d in &snap.dirs {
            assert!(d.xattrs.is_empty(), "dir {} has xattrs", d.name);
        }
    }
}
