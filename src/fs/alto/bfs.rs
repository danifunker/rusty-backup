//! Read-only walk of the Alto Basic File System (BFS) over an in-memory
//! [`Disk`].
//!
//! A BFS file is a chain of pages. Page 0 is the **leader** (name, dates,
//! hints); pages 1.. are data. Each sector's **label** links the chain: word 0
//! is `next` and word 1 is `previous` (both real disk addresses, or `eofDA` =
//! `0xFFFF` / `0` at the ends), word 3 is `numChars` (valid bytes on the page),
//! word 4 is `pageNumber`, and words 5-7 are the file id (`version`, serial
//! number). The directory file **SysDir** has a fixed leader at virtual disk
//! address 1 (serial 100, directory bit set, version 1) and holds a flat
//! sequence of `DV` entries mapping names to file pointers.
//!
//! Structure references: `AltoFileSys.d` (`LD`, `DV`, `FP`, `SN`), `BFS.d`
//! (`DL` label), `BFSInit.bcpl` (the fixed SysDir file pointer),
//! `BFSBase.bcpl` (the disk-address mapping).

use std::io::Read;
use std::path::PathBuf;

use super::super::entry::FileEntry;
use super::super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};
use super::{be16, Disk};

/// `eofDA` (Disks.d): a `next`/`previous` link of this value terminates the
/// chain. `BFSVirtualDA` also treats a stored real-DA of `0` as end-of-chain.
const EOF_DA: u16 = 0xffff;

/// SysDir's leader page is fixed at virtual disk address 1 (`BFSInit.bcpl`).
pub const SYSDIR_LEADER_VDA: usize = 1;

const DV_TYPE_FILE: u16 = 1;

/// A maximum filename length guard (`maxLengthFn` in `AltoFileSys.d` is 39).
const MAX_NAME_LEN: usize = 40;

/// One file discovered in SysDir.
#[derive(Debug, Clone)]
pub struct BfsFile {
    /// Name as recorded in the directory entry (e.g. `"SysDir."`).
    pub name: String,
    /// File serial number (the 29-bit value, for display/identity).
    pub serial: u32,
    pub version: u16,
    /// Virtual disk address of the file's leader page.
    pub leader_vda: usize,
    /// File length in bytes (sum of `numChars` over the data pages).
    pub size: u64,
    /// Number of data pages (excludes the leader).
    pub n_pages: u32,
}

pub struct Bfs<'a> {
    disk: &'a Disk,
}

/// True when a label link terminates a page chain.
fn is_eof_link(link: u16) -> bool {
    link == EOF_DA || link == 0
}

impl<'a> Bfs<'a> {
    pub fn new(disk: &'a Disk) -> Self {
        Bfs { disk }
    }

    /// Decode the filename stored in a leader page's data (an Alto string:
    /// length byte then ASCII chars, starting at word 6 / byte 12 of `LD`).
    pub fn leader_name(&self, leader_vda: usize) -> Option<String> {
        let leader = self.disk.sector(leader_vda)?;
        decode_alto_string(&leader.data, 12)
    }

    /// Walk a file's data-page chain and return `(size_bytes, n_data_pages)`
    /// without copying the data.
    fn chain_stats(&self, leader_vda: usize) -> Result<(u64, u32), FilesystemError> {
        let leader = self.disk.sector(leader_vda).ok_or_else(|| {
            FilesystemError::InvalidData(format!("leader VDA {leader_vda} out of range"))
        })?;
        let mut link = be16(&leader.label, 0); // LD.next -> first data page
        let mut size = 0u64;
        let mut pages = 0u32;
        let total = self.disk.geometry.total_sectors();
        while !is_eof_link(link) {
            let vda = self.disk.geometry.vda_from_da(link);
            let s = self.disk.sector(vda).ok_or_else(|| {
                FilesystemError::InvalidData(format!("page VDA {vda} out of range"))
            })?;
            let num_chars = be16(&s.label, 6) as usize; // DL.numChars
            size += num_chars.min(s.data.len()) as u64;
            pages += 1;
            if pages as usize > total {
                return Err(FilesystemError::InvalidData(
                    "BFS page chain longer than the disk (loop?)".into(),
                ));
            }
            link = be16(&s.label, 0);
        }
        Ok((size, pages))
    }

    /// Read a file's bytes by following its leader's data-page chain, up to
    /// `max_bytes`.
    pub fn read_file_bytes(
        &self,
        leader_vda: usize,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let leader = self.disk.sector(leader_vda).ok_or_else(|| {
            FilesystemError::InvalidData(format!("leader VDA {leader_vda} out of range"))
        })?;
        let mut link = be16(&leader.label, 0);
        let mut out = Vec::new();
        let total = self.disk.geometry.total_sectors();
        let mut pages = 0usize;
        while !is_eof_link(link) && out.len() < max_bytes {
            let vda = self.disk.geometry.vda_from_da(link);
            let s = self.disk.sector(vda).ok_or_else(|| {
                FilesystemError::InvalidData(format!("page VDA {vda} out of range"))
            })?;
            let num_chars = (be16(&s.label, 6) as usize).min(s.data.len());
            let want = (max_bytes - out.len()).min(num_chars);
            out.extend_from_slice(&s.data[..want]);
            pages += 1;
            if pages > total {
                return Err(FilesystemError::InvalidData(
                    "BFS page chain longer than the disk (loop?)".into(),
                ));
            }
            link = be16(&s.label, 0);
        }
        Ok(out)
    }

    /// List the files named in SysDir.
    pub fn list_files(&self) -> Result<Vec<BfsFile>, FilesystemError> {
        // SysDir's content is the concatenation of its data pages.
        let dir_bytes = self.read_file_bytes(SYSDIR_LEADER_VDA, 8 * 1024 * 1024)?;
        let entries = parse_dir_entries(&dir_bytes)?;
        let mut files = Vec::new();
        for e in entries {
            if e.entry_type != DV_TYPE_FILE {
                continue;
            }
            let (size, n_pages) = match self.chain_stats(e.leader_vda) {
                Ok(v) => v,
                Err(err) => {
                    log::warn!("BFS: skipping {}: {err}", e.name);
                    (0, 0)
                }
            };
            files.push(BfsFile {
                name: e.name,
                serial: e.serial,
                version: e.version,
                leader_vda: e.leader_vda,
                size,
                n_pages,
            });
        }
        Ok(files)
    }

    /// Find a live file's directory entry by name (case-insensitive, a
    /// trailing `.` is ignored on both sides).
    fn find_entry(&self, name: &str) -> Option<DirEntry> {
        let target = name.trim_end_matches('.');
        let dir_bytes = self
            .read_file_bytes(SYSDIR_LEADER_VDA, 8 * 1024 * 1024)
            .ok()?;
        parse_dir_entries(&dir_bytes).ok()?.into_iter().find(|e| {
            e.entry_type == DV_TYPE_FILE
                && e.name.trim_end_matches('.').eq_ignore_ascii_case(target)
        })
    }

    /// Read a named file's bytes (up to `max_bytes`).
    pub fn read_file_by_name(
        &self,
        name: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let e = self
            .find_entry(name)
            .ok_or_else(|| FilesystemError::NotFound(name.to_string()))?;
        self.read_file_bytes(e.leader_vda, max_bytes)
    }

    /// Read and parse the `DiskDescriptor` file's header (`KDH`): geometry plus
    /// free-page accounting. Because this file is found and read through the
    /// normal page-chain walk, a successful parse whose geometry matches the
    /// container's is a strong end-to-end check that file extraction is correct.
    pub fn disk_descriptor(&self) -> Result<DiskDescriptor, FilesystemError> {
        let e = self
            .find_entry("DiskDescriptor")
            .ok_or_else(|| FilesystemError::NotFound("DiskDescriptor".into()))?;
        let data = self.read_file_bytes(e.leader_vda, 64 * 1024)?;
        if data.len() < 20 {
            return Err(FilesystemError::Parse(
                "DiskDescriptor file too short".into(),
            ));
        }
        Ok(DiskDescriptor {
            n_disks: be16(&data, 0),
            n_tracks: be16(&data, 2),
            n_heads: be16(&data, 4),
            n_sectors: be16(&data, 6),
            disk_bt_size: be16(&data, 14),
            default_versions_kept: be16(&data, 16),
            free_pages: be16(&data, 18) as u32,
        })
    }
}

/// The `DiskDescriptor` file's header (`KDH` in `AltoFileSys.d`). Field byte
/// offsets within the file's data: `nDisks@0 nTracks@2 nHeads@4 nSectors@6
/// lastSn@8(2w) diskBTsize@14 defaultVersionsKept@16 freePages@18`.
#[derive(Debug, Clone)]
pub struct DiskDescriptor {
    pub n_disks: u16,
    pub n_tracks: u16,
    pub n_heads: u16,
    pub n_sectors: u16,
    /// Valid words in the free-page bit table.
    pub disk_bt_size: u16,
    pub default_versions_kept: u16,
    /// Free pages remaining on the volume.
    pub free_pages: u32,
}

/// A [`Filesystem`]-trait view of a BFS pack. Owns the in-memory [`Disk`] so it
/// can be opened, browsed, and read through the same interface as every other
/// filesystem in the engine. BFS has a flat namespace (the single SysDir), so
/// the root directory lists every file and there are no subdirectories.
pub struct BfsFilesystem {
    disk: Disk,
    /// Free pages from the DiskDescriptor, cached at open time (`None` if the
    /// volume has no readable DiskDescriptor).
    free_pages: Option<u32>,
    /// When set, edits persist by writing the rebuilt volume as a PDI to this
    /// path. The read-only open leaves it `None`.
    save_path: Option<PathBuf>,
}

impl BfsFilesystem {
    pub fn open(disk: Disk) -> Self {
        let free_pages = Bfs::new(&disk).disk_descriptor().ok().map(|d| d.free_pages);
        BfsFilesystem {
            disk,
            free_pages,
            save_path: None,
        }
    }

    /// Open for editing: mutations rebuild the volume in memory, and
    /// [`EditableFilesystem::sync_metadata`] writes it back to `save_path` as a
    /// PARC Disk Image (so a CopyDisk source is converted to PDI on save —
    /// both still open by magic).
    pub fn open_editable(disk: Disk, save_path: PathBuf) -> Self {
        let mut fs = Self::open(disk);
        fs.save_path = Some(save_path);
        fs
    }

    pub fn disk(&self) -> &Disk {
        &self.disk
    }

    fn refresh_free(&mut self) {
        self.free_pages = Bfs::new(&self.disk)
            .disk_descriptor()
            .ok()
            .map(|d| d.free_pages);
    }

    /// Build a `FileEntry` for the user file whose name (sans trailing `.`)
    /// matches `bare_name`, by re-scanning the rebuilt volume.
    fn entry_for(&self, bare_name: &str) -> Option<FileEntry> {
        Bfs::new(&self.disk)
            .list_files()
            .ok()?
            .into_iter()
            .find(|f| f.name.trim_end_matches('.').eq_ignore_ascii_case(bare_name))
            .map(|f| {
                FileEntry::new_file(
                    f.name.clone(),
                    format!("/{}", f.name),
                    f.size,
                    f.leader_vda as u64,
                )
            })
    }
}

impl Filesystem for BfsFilesystem {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::root())
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if entry.path != "/" {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        let files = Bfs::new(&self.disk).list_files()?;
        Ok(files
            .into_iter()
            .map(|f| {
                FileEntry::new_file(
                    f.name.clone(),
                    format!("/{}", f.name),
                    f.size,
                    f.leader_vda as u64,
                )
            })
            .collect())
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        Bfs::new(&self.disk).read_file_bytes(entry.location as usize, max_bytes)
    }

    fn volume_label(&self) -> Option<&str> {
        None
    }

    fn fs_type(&self) -> &str {
        "Alto BFS"
    }

    fn total_size(&self) -> u64 {
        self.disk.geometry.total_sectors() as u64 * self.disk.geometry.data_bytes as u64
    }

    fn used_size(&self) -> u64 {
        let data = self.disk.geometry.data_bytes as u64;
        match self.free_pages {
            Some(free) => {
                (self.disk.geometry.total_sectors() as u64).saturating_sub(free as u64) * data
            }
            None => 0,
        }
    }
}

impl EditableFilesystem for BfsFilesystem {
    fn create_file(
        &mut self,
        _parent: &FileEntry,
        name: &str,
        data: &mut dyn Read,
        data_len: u64,
        _options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let mut buf = Vec::with_capacity(data_len as usize);
        data.read_to_end(&mut buf).map_err(FilesystemError::Io)?;
        self.disk = super::write::add_file(&self.disk, name, &buf)?;
        self.refresh_free();
        self.entry_for(name.trim_end_matches('.'))
            .ok_or_else(|| FilesystemError::InvalidData(format!("created file {name} not found")))
    }

    fn create_directory(
        &mut self,
        _parent: &FileEntry,
        _name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "Alto BFS has a flat namespace (no subdirectories)".into(),
        ))
    }

    fn delete_entry(
        &mut self,
        _parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        self.disk = super::write::delete_file(&self.disk, &entry.name)?;
        self.refresh_free();
        Ok(())
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        let path = self.save_path.as_ref().ok_or_else(|| {
            FilesystemError::Unsupported("Alto volume opened read-only (no save path)".into())
        })?;
        std::fs::write(path, super::pdi::write(&self.disk)).map_err(FilesystemError::Io)
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.free_pages.unwrap_or(0) as u64 * self.disk.geometry.data_bytes as u64)
    }
}

/// A raw directory entry decoded from SysDir.
#[derive(Debug, Clone)]
pub(crate) struct DirEntry {
    pub entry_type: u16,
    pub name: String,
    pub serial: u32,
    /// Raw serial-number words (`SN.word1` carries flag bits incl. the
    /// directory bit; `SN.word2` is the low serial value). Kept verbatim so a
    /// clone can preserve file identity.
    pub sn_w1: u16,
    pub sn_w2: u16,
    pub version: u16,
    pub leader_vda: usize,
}

/// Parse a flat sequence of `DV` entries. Each entry's first word packs
/// `type` (6 bits) and `length` (10 bits, the entry size in words); then a
/// 5-word file pointer and a length-prefixed name. Free entries (type 0) are
/// retained in the output (callers filter by type).
pub(crate) fn parse_dir_entries(dir: &[u8]) -> Result<Vec<DirEntry>, FilesystemError> {
    let total_words = dir.len() / 2;
    let mut out = Vec::new();
    let mut w = 0usize; // word cursor
    while w + 6 <= total_words {
        let head = be16(dir, w * 2);
        let entry_type = (head >> 10) & 0x3f;
        let length = (head & 0x3ff) as usize; // entry length in words
        if length == 0 {
            break; // end of directory / malformed
        }
        if w + length > total_words {
            break; // truncated final entry
        }

        // File pointer (words w+1 .. w+5).
        let sn_w1 = be16(dir, (w + 1) * 2);
        let sn_w2 = be16(dir, (w + 2) * 2);
        let version = be16(dir, (w + 3) * 2);
        let leader_vda = be16(dir, (w + 5) * 2) as usize;
        let serial = (((sn_w1 & 0x1fff) as u32) << 16) | sn_w2 as u32;

        // Name string begins at word w+6 (byte (w+6)*2).
        let name = decode_alto_string(dir, (w + 6) * 2).unwrap_or_default();

        out.push(DirEntry {
            entry_type,
            name,
            serial,
            sn_w1,
            sn_w2,
            version,
            leader_vda,
        });
        w += length;
    }
    Ok(out)
}

/// Decode an Alto string at byte offset `off`: a length byte followed by that
/// many ASCII characters. Returns `None` if out of range.
fn decode_alto_string(buf: &[u8], off: usize) -> Option<String> {
    let len = *buf.get(off)? as usize;
    let len = len.min(MAX_NAME_LEN);
    let start = off + 1;
    let end = start + len;
    if end > buf.len() {
        return None;
    }
    let s: String = buf[start..end]
        .iter()
        .map(|&b| {
            if (0x20..0x7f).contains(&b) {
                b as char
            } else {
                '?'
            }
        })
        .collect();
    Some(s.trim_end().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_string_basic() {
        // length 6, "SysDir"
        let mut buf = vec![0u8; 32];
        buf[0] = 6;
        buf[1..7].copy_from_slice(b"SysDir");
        assert_eq!(decode_alto_string(&buf, 0).unwrap(), "SysDir");
    }

    #[test]
    fn parse_one_file_entry() {
        // Build a single DV file entry: type=1, length=9 words.
        // words: [type/len][sn1][sn2][ver][blank][leaderVDA][name...]
        // name = "HI." (len 3) -> needs ceil((1+3)/2)=2 words; total header
        // words = 6, +2 name words = 8; round entry length to 9 for slack.
        let length: u16 = 9;
        let mut dir = vec![0u8; (length as usize) * 2];
        let put = |d: &mut [u8], wi: usize, v: u16| {
            d[wi * 2..wi * 2 + 2].copy_from_slice(&v.to_be_bytes())
        };
        put(&mut dir, 0, (DV_TYPE_FILE << 10) | length);
        put(&mut dir, 1, 0x8000 | 123); // sn word1 (directory bit clear here, value 123)
        put(&mut dir, 2, 456); // sn word2
        put(&mut dir, 3, 1); // version
        put(&mut dir, 5, 42); // leaderVDA
        dir[6 * 2] = 3; // name length
        dir[6 * 2 + 1..6 * 2 + 4].copy_from_slice(b"HI.");

        let entries = parse_dir_entries(&dir).unwrap();
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.entry_type, DV_TYPE_FILE);
        assert_eq!(e.name, "HI.");
        assert_eq!(e.version, 1);
        assert_eq!(e.leader_vda, 42);
        assert_eq!(e.serial, ((123u32 & 0x1fff) << 16) | 456);
    }

    #[test]
    fn parse_stops_on_zero_length() {
        let dir = vec![0u8; 64]; // all zero -> length 0 -> stop immediately
        assert!(parse_dir_entries(&dir).unwrap().is_empty());
    }

    /// Build a tiny synthetic BFS volume (SysDir + one file) and read it back
    /// through the `Filesystem` trait — exercises chain-following end to end
    /// without needing an external pack. Geometry 1x1x1x8 makes `VDA == sector`,
    /// so the real disk address of VDA `v` is simply `v << 12`.
    #[test]
    fn bfs_filesystem_lists_and_reads() {
        use super::super::{Disk, FsFamily, Geometry, Sector};

        let geometry = Geometry {
            family: FsFamily::Diablo,
            disk_model: 0,
            n_disks: 1,
            n_cylinders: 1,
            n_heads: 1,
            n_sectors: 8,
            label_bytes: 16,
            data_bytes: 512,
        };
        let da = |v: u16| v << 12; // real DA for VDA v under this geometry
        let mut sectors: Vec<Sector> = (0..8).map(|_| Sector::zeroed(16, 512)).collect();
        let put = |s: &mut Sector, off: usize, v: u16| {
            s.label[off..off + 2].copy_from_slice(&v.to_be_bytes())
        };

        // VDA 1: SysDir leader -> data page at VDA 2.
        put(&mut sectors[1], 0, da(2)); // next
        put(&mut sectors[1], 2, EOF_DA); // previous
        sectors[1].data[12] = 6;
        sectors[1].data[13..19].copy_from_slice(b"SysDir");

        // VDA 2: SysDir's one data page, holding a single DV entry for "HI.".
        put(&mut sectors[2], 0, EOF_DA); // next = end
        let length: u16 = 9;
        {
            let d = &mut sectors[2].data;
            let putw = |d: &mut [u8], wi: usize, v: u16| {
                d[wi * 2..wi * 2 + 2].copy_from_slice(&v.to_be_bytes())
            };
            putw(d, 0, (DV_TYPE_FILE << 10) | length);
            putw(d, 2, 100); // serial low word
            putw(d, 3, 1); // version
            putw(d, 5, 4); // leaderVirtualDa = 4
            d[6 * 2] = 3;
            d[6 * 2 + 1..6 * 2 + 4].copy_from_slice(b"HI.");
        }
        put(&mut sectors[2], 6, length * 2); // numChars = 18 bytes of dir content

        // VDA 4: "HI." leader -> data page at VDA 5.
        put(&mut sectors[4], 0, da(5));
        put(&mut sectors[4], 2, EOF_DA);
        sectors[4].data[12] = 3;
        sectors[4].data[13..16].copy_from_slice(b"HI.");

        // VDA 5: file data page, 5 bytes "hello".
        put(&mut sectors[5], 0, EOF_DA);
        put(&mut sectors[5], 6, 5); // numChars
        sectors[5].data[..5].copy_from_slice(b"hello");

        let disk = Disk { geometry, sectors };
        let mut fs = BfsFilesystem::open(disk);
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "HI.");
        assert_eq!(entries[0].size, 5);
        assert_eq!(fs.fs_type(), "Alto BFS");
        let data = fs.read_file(&entries[0], usize::MAX).unwrap();
        assert_eq!(data, b"hello");

        // In-GUI extraction streams through `write_file_to` (the trait default
        // calls `read_file`). Confirm the extract path yields the same bytes.
        let mut extracted: Vec<u8> = Vec::new();
        fs.write_file_to(&entries[0], &mut extracted).unwrap();
        assert_eq!(extracted, b"hello");
    }

    /// The exact path the GUI Edit Mode takes: open_editable a PDI on disk,
    /// create_file / delete_entry, sync_metadata, then re-read the file and
    /// confirm the change persisted.
    #[test]
    fn editable_create_delete_persists_to_pdi() {
        use super::super::write::create_blank;
        use super::super::{pdi, FsFamily, Geometry};
        use std::io::Write as _;

        let geom = Geometry {
            family: FsFamily::Diablo,
            disk_model: 31,
            n_disks: 1,
            n_cylinders: 203,
            n_heads: 2,
            n_sectors: 12,
            label_bytes: 16,
            data_bytes: 512,
        };
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(&pdi::write(&create_blank(geom).unwrap()))
            .unwrap();
        let path = tmp.path().to_path_buf();
        let payload = b"persisted through the editable trait".to_vec();

        // create + sync
        {
            let disk = pdi::read(&std::fs::read(&path).unwrap()).unwrap();
            let mut efs = BfsFilesystem::open_editable(disk, path.clone());
            let root = efs.root().unwrap();
            let mut cur = std::io::Cursor::new(payload.clone());
            efs.create_file(
                &root,
                "NOTE.TXT",
                &mut cur,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
            efs.sync_metadata().unwrap();
        }
        // reload from disk: the file is there with the right bytes
        let mut fs = BfsFilesystem::open(pdi::read(&std::fs::read(&path).unwrap()).unwrap());
        let root = fs.root().unwrap();
        let note = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "NOTE.TXT.")
            .expect("file persisted");
        assert_eq!(fs.read_file(&note, usize::MAX).unwrap(), payload);

        // delete + sync
        {
            let disk = pdi::read(&std::fs::read(&path).unwrap()).unwrap();
            let mut efs = BfsFilesystem::open_editable(disk, path.clone());
            let root = efs.root().unwrap();
            let note = efs
                .list_directory(&root)
                .unwrap()
                .into_iter()
                .find(|e| e.name == "NOTE.TXT.")
                .unwrap();
            efs.delete_entry(&root, &note).unwrap();
            efs.sync_metadata().unwrap();
        }
        let mut fs = BfsFilesystem::open(pdi::read(&std::fs::read(&path).unwrap()).unwrap());
        let root = fs.root().unwrap();
        assert!(!fs
            .list_directory(&root)
            .unwrap()
            .iter()
            .any(|e| e.name == "NOTE.TXT."));
    }
}
