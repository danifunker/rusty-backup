//! Source-side snapshot types for the classic-HFS reformat-and-copy pipeline.
//!
//! Step 2 of docs/hfs_expand_block_size.md. Nothing here mutates the source
//! volume; these structs exist to capture every byte of metadata that must
//! survive the copy so later steps can replay it onto a freshly-formatted
//! target volume.

use std::io::{Read, Seek};

use byteorder::{BigEndian, ByteOrder};

use super::hfs::{HfsExtDescriptor, HfsFilesystem};
use crate::fs::FilesystemError;

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
}
