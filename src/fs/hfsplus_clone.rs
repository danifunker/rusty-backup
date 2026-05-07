//! Source-side snapshot types for the HFS+ reformat-and-copy pipeline.
//!
//! Step 10 of `docs/hfsplus_enhancements.md`. Parallel to `hfs_clone.rs`,
//! but for HFS+/HFSX. Nothing here mutates the source volume; these structs
//! capture every byte of metadata that must survive a clone-shrink so later
//! steps can replay it onto a freshly-formatted target.
//!
//! Until later phases land, `SourceCatalogSnapshot::capture` returns
//! `Err(Unsupported(...))` for volumes carrying:
//!   - extended attributes (`vh.attributes_file.logical_size > 0`) — relaxed
//!     in Step 13 of the plan;
//!   - file or directory hardlinks (`fdType='hlnk' creator='hfs+'` or
//!     `fdType='fdrp' creator='MACS'` in the userInfo block) — relaxed in
//!     Steps 16–18.

use std::io::{Read, Seek};

use byteorder::{BigEndian, ByteOrder};

use super::filesystem::FilesystemError;
use super::hfs_common::walk_leaf_records;
use super::hfsplus::HfsPlusFilesystem;

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

/// Full capture of a source HFS+/HFSX volume: volume metadata plus every
/// directory and file record. Catalog *thread* records are not captured —
/// the target writer recreates them from scratch.
#[derive(Debug, Clone)]
pub struct SourceCatalogSnapshot {
    pub volume: SourceVolumeSnapshot,
    pub dirs: Vec<SourceDirSnapshot>,
    pub files: Vec<SourceFileSnapshot>,
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
    /// Returns `Err(Unsupported)` early on volumes carrying extended
    /// attributes or hardlinks — Steps 13 / 16–18 of
    /// `docs/hfsplus_enhancements.md` will relax these guards once the
    /// supporting infrastructure lands.
    pub fn capture<R: Read + Seek>(fs: &mut HfsPlusFilesystem<R>) -> Result<Self, FilesystemError> {
        if fs.attributes_file_size() > 0 {
            return Err(FilesystemError::Unsupported(
                "HFS+ source carries extended attributes; xattr capture lands in Phase 5 \
                 (Step 13 of docs/hfsplus_enhancements.md)"
                    .into(),
            ));
        }

        let volume = SourceVolumeSnapshot::capture(fs)?;

        // Walk the catalog leaves and accumulate dir / file metadata. File
        // records carry the 80-byte data + resource fork descriptors which
        // we hold onto so we can stream the fork bytes after the immutable
        // catalog borrow is released.
        let mut dirs: Vec<SourceDirSnapshot> = Vec::new();
        let mut files: Vec<SourceFileSnapshot> = Vec::new();
        let mut pending_forks: Vec<([u8; 80], [u8; 80])> = Vec::new();
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

        // Read fork bytes now that the catalog borrow is released.
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
        }

        Ok(SourceCatalogSnapshot {
            volume,
            dirs,
            files,
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
    Thread,
    Skip,
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

            // Directory hardlink (10.5+): aliasing record with FInfo
            // fdType='fdrp' / fdCreator='MACS'. Step 18 will relax.
            if &finder_info[0..4] == HARDLINK_DIR_TYPE && &finder_info[4..8] == HARDLINK_DIR_CREATOR
            {
                return Err(FilesystemError::Unsupported(
                    "HFS+ source carries directory hardlinks; capture lands in Step 18 \
                     of docs/hfsplus_enhancements.md"
                        .into(),
                ));
            }

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

            // File hardlink: aliasing record with FInfo fdType='hlnk' /
            // fdCreator='hfs+'. Steps 16–17 will relax.
            if &finder_info[0..4] == HARDLINK_FILE_TYPE
                && &finder_info[4..8] == HARDLINK_FILE_CREATOR
            {
                return Err(FilesystemError::Unsupported(
                    "HFS+ source carries file hardlinks; capture lands in Steps 16–17 \
                     of docs/hfsplus_enhancements.md"
                        .into(),
                ));
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

    #[test]
    fn capture_refuses_volumes_with_xattrs() {
        // Hand-poke `attributes_file.logical_size` in the VH so capture
        // exercises the early-out without us needing a real xattr writer
        // (lands in Phase 5).
        let mut img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "Attrs", false);
        // VH lives at offset 1024; attributes_file ForkData starts at +352.
        // logical_size is the first 8 bytes (big-endian u64).
        let attr_off = 1024 + 352;
        let fake_size = 4096u64.to_be_bytes();
        img[attr_off..attr_off + 8].copy_from_slice(&fake_size);
        // Mirror to the alternate VH at end-of-volume so re-open doesn't
        // bail; HFS+ open reads the primary first.
        let alt_off = img.len() - 1024 + 352;
        img[alt_off..alt_off + 8].copy_from_slice(&fake_size);

        let cursor = Cursor::new(&mut img);
        let mut fs = HfsPlusFilesystem::open(cursor, 0).expect("open");
        let err = SourceCatalogSnapshot::capture(&mut fs)
            .err()
            .expect("should refuse xattr volume");
        match err {
            FilesystemError::Unsupported(msg) => {
                assert!(msg.contains("extended attributes"), "got: {msg}");
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }
}
