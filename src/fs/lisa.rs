//! Apple Lisa File System — read-only browse + extract.
//!
//! Lisa 3.5" floppies (Sony 400K / 800K GCR, 512-byte sectors) are distributed
//! as DiskCopy 4.2 images that *retain* the 12-byte-per-sector **tag** region.
//! Those tags are the key to the volume: each sector's tag carries the **file
//! ID** it belongs to and the file-**relative block** index, so a file is
//! reconstructed by gathering every sector with a given file ID and ordering
//! them by relative block — no dependence on the (three different, version-
//! specific) catalog structures. This mirrors Ray Arachelian's `lisafsh-tool`,
//! the reference implementation.
//!
//! Friendly file names come from the flat catalog (file ID `0x0004`): 54-byte
//! entries, a Pascal-string name at offset 0 and the file ID as a big-endian
//! `u16` at offset 36. This works for the flat-table (`0x0e`) and flat-hash
//! (`0x0f`) filesystem versions; the hierarchical B-tree version (`0x11`) keeps
//! its catalog elsewhere, so those volumes fall back to `file-XXXX` names while
//! extraction still works for every file.
//!
//! ## Limitations (shared with the reference tool)
//! - **Read-only.** Writing would require maintaining tags plus one of three
//!   catalog formats; out of scope.
//! - **Sector-granular sizes.** The exact end-of-file (the "extents" tail of up
//!   to 511 bytes) isn't tracked, so extracted files are padded to a whole
//!   sector.
//! - **Tags required.** DiskCopy versions after 4.2 strip tags; such an image
//!   can't be browsed (detection fails cleanly).

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};
use crate::rbformats::dc42;

const SECTOR_SIZE: usize = 512;
const TAG_SIZE: usize = 12;
/// Normal files carry a 0xF0-byte metadata "label" on their first sector; it is
/// stripped from the reconstructed data fork (matching `lisafsh-tool`).
const LABEL_LEN: usize = 0xF0;
/// Catalog (directory) entry stride, in bytes.
const CATALOG_ENTRY: usize = 54;
/// Offset of the big-endian file-ID within a 54-byte catalog entry.
const CATALOG_FILEID_OFF: usize = 36;

// Reserved / special file IDs (see `lisafsh-tool`).
const FID_FREE: u16 = 0x0000;
const FID_MDDF: u16 = 0x0001;
const FID_BITMAP: u16 = 0x0002;
const FID_SRECORDS: u16 = 0x0003;
const FID_DIRECTORY: u16 = 0x0004;
const FID_ERASED: u16 = 0x7fff;
const FID_MAXTAG: u16 = 0x7fff;
const FID_BOOT: u16 = 0xaaaa;
const FID_OSLOADER: u16 = 0xbbbb;

fn be16(b: &[u8], o: usize) -> u16 {
    ((b[o] as u16) << 8) | (b[o + 1] as u16)
}

/// One reconstructed Lisa file (or system pseudo-file), keyed by file ID.
struct LisaEntry {
    fileid: u16,
    name: String,
    /// Sector indices in file-relative-block order.
    sectors: Vec<usize>,
    /// Strip the 0xF0 metadata label from the first sector (normal files only).
    strip_label: bool,
    /// Reconstructed length in bytes (sector-granular).
    size: u64,
}

/// A read-only view over an Apple Lisa File System floppy image.
pub struct LisaFilesystem {
    numblocks: usize,
    data: Vec<u8>,
    tags: Vec<u8>,
    volume_name: String,
    fs_version_name: &'static str,
    /// Indexed by `FileEntry::location`.
    entries: Vec<LisaEntry>,
    free_blocks: usize,
}

/// Human label for an MDDF `fsversion`.
fn fsversion_name(v: u16) -> &'static str {
    match v {
        0x0e => "Lisa FS (flat table)",
        0x0f => "Lisa FS (flat hash)",
        0x11 => "Lisa FS (B-tree)",
        _ => "Lisa FS",
    }
}

fn is_valid_fsversion(v: u16) -> bool {
    matches!(v, 0x0e | 0x0f | 0x11)
}

/// Friendly name for a reserved file ID; `None` for ordinary files (which get
/// their name from the catalog, falling back to `file-XXXX`).
fn reserved_name(fileid: u16) -> Option<&'static str> {
    match fileid {
        FID_FREE => Some("freeblocks-0000"),
        FID_MDDF => Some("MDDF-0001"),
        FID_BITMAP => Some("alloc-bitmap-0002"),
        FID_SRECORDS => Some("srecords-0003"),
        FID_DIRECTORY => Some("directory-0004"),
        FID_ERASED => Some("deleted-7fff"),
        FID_BOOT => Some("bootsect-aaaa"),
        FID_OSLOADER => Some("OSLoader-bbbb"),
        _ => None,
    }
}

/// A "normal" user file — the catalog names it and its first sector holds a
/// 0xF0-byte metadata label. Reserved IDs (`<= 0x0004`, the erased marker, and
/// everything `> 0x7fff`) are system scaffolding.
fn is_normal_file(fileid: u16) -> bool {
    fileid > FID_DIRECTORY && fileid < FID_ERASED
}

impl LisaFilesystem {
    /// Open a Lisa File System image. `reader` must cover the **whole**
    /// DiskCopy 4.2 file (header + data + tags) — not the unwrapped data region,
    /// which would have discarded the tags this filesystem depends on.
    pub fn open<R: Read + Seek>(mut reader: R) -> Result<Self, FilesystemError> {
        reader
            .seek(SeekFrom::Start(0))
            .map_err(FilesystemError::Io)?;
        let header = dc42::parse_dc42_header(&mut reader)
            .ok_or_else(|| FilesystemError::Parse("not a DiskCopy 4.2 image".into()))?;

        let data_size = header.data_size as usize;
        if data_size == 0 || !data_size.is_multiple_of(SECTOR_SIZE) {
            return Err(FilesystemError::Parse(format!(
                "unexpected data size {data_size}"
            )));
        }
        let numblocks = data_size / SECTOR_SIZE;
        if header.tag_size as usize != numblocks * TAG_SIZE {
            return Err(FilesystemError::Parse(
                "image has no Lisa tags (post-4.2 DiskCopy strips them) - cannot browse".into(),
            ));
        }

        let mut data = vec![0u8; data_size];
        reader.read_exact(&mut data).map_err(FilesystemError::Io)?;
        let mut tags = vec![0u8; numblocks * TAG_SIZE];
        reader.read_exact(&mut tags).map_err(FilesystemError::Io)?;

        Self::from_parts(data, tags)
    }

    /// Construct directly from a de-containerized `(data, tags)` pair — the
    /// media-agnostic seam any producer can target: DiskCopy 4.2 today (via
    /// [`open`](LisaFilesystem::open)), a DART decompressor or a Twiggy
    /// de-interleaver later. `data` must be `numblocks * 512` bytes and `tags`
    /// exactly `numblocks * 12`.
    pub fn from_parts(data: Vec<u8>, tags: Vec<u8>) -> Result<Self, FilesystemError> {
        if data.is_empty() || !data.len().is_multiple_of(SECTOR_SIZE) {
            return Err(FilesystemError::Parse(format!(
                "unexpected data length {}",
                data.len()
            )));
        }
        let numblocks = data.len() / SECTOR_SIZE;
        if tags.len() != numblocks * TAG_SIZE {
            return Err(FilesystemError::Parse(format!(
                "tag length {} != numblocks {} * {}",
                tags.len(),
                numblocks,
                TAG_SIZE
            )));
        }

        let mut fs = LisaFilesystem {
            numblocks,
            data,
            tags,
            volume_name: String::new(),
            fs_version_name: "Lisa FS",
            entries: Vec::new(),
            free_blocks: 0,
        };
        fs.build()?;
        Ok(fs)
    }

    fn tag_fileid(&self, sector: usize) -> u16 {
        be16(&self.tags, sector * TAG_SIZE + 4)
    }

    /// File-relative block index (masked to 11 bits, as `lisafsh-tool` does).
    fn tag_relblock(&self, sector: usize) -> u16 {
        be16(&self.tags, sector * TAG_SIZE + 6) & 0x7ff
    }

    fn sector_bytes(&self, sector: usize) -> &[u8] {
        &self.data[sector * SECTOR_SIZE..(sector + 1) * SECTOR_SIZE]
    }

    fn build(&mut self) -> Result<(), FilesystemError> {
        // Group sectors by file ID, ordered by file-relative block.
        let mut by_file: BTreeMap<u16, Vec<usize>> = BTreeMap::new();
        let mut free_blocks = 0usize;
        for n in 0..self.numblocks {
            let fid = self.tag_fileid(n);
            if fid == FID_FREE {
                free_blocks += 1;
            }
            by_file.entry(fid).or_default().push(n);
        }
        for secs in by_file.values_mut() {
            secs.sort_by_key(|&n| self.tag_relblock(n));
        }
        self.free_blocks = free_blocks;

        // MDDF: volume name + filesystem version.
        if let Some(secs) = by_file.get(&FID_MDDF) {
            if let Some(&n) = secs.first() {
                let (ver, vol) = {
                    let s = self.sector_bytes(n);
                    (fsversion_name(be16(s, 0)), read_mddf_volume_name(s))
                };
                self.fs_version_name = ver;
                self.volume_name = vol;
            }
        }

        // Catalog names (flat table / hash only; B-tree has no 0x0004 blocks).
        let real_files: BTreeSet<u16> = by_file
            .keys()
            .copied()
            .filter(|&f| is_normal_file(f))
            .collect();
        let names = self.read_catalog_names(by_file.get(&FID_DIRECTORY), &real_files);

        // Build the browsable entry list: user files plus recognised system
        // files, skipping free/erased blocks and unnamed high-ID scaffolding.
        for (&fileid, sectors) in &by_file {
            if !should_list(fileid) {
                continue;
            }
            let strip_label = is_normal_file(fileid);
            let name = names
                .get(&fileid)
                .cloned()
                .or_else(|| reserved_name(fileid).map(String::from))
                .unwrap_or_else(|| format!("file-{fileid:04x}"));
            let raw = sectors.len() * SECTOR_SIZE;
            let size = if strip_label {
                raw.saturating_sub(LABEL_LEN)
            } else {
                raw
            } as u64;
            self.entries.push(LisaEntry {
                fileid,
                name,
                sectors: sectors.clone(),
                strip_label,
                size,
            });
        }
        // Stable, predictable order: by file ID.
        self.entries.sort_by_key(|e| e.fileid);
        Ok(())
    }

    /// Parse the flat catalog into a file-ID → name map. Scans 54-byte entries
    /// (Pascal name at offset 0, file ID at offset 36), accepting only entries
    /// whose file ID is a real file present in the tags.
    fn read_catalog_names(
        &self,
        dir_sectors: Option<&Vec<usize>>,
        real_files: &BTreeSet<u16>,
    ) -> BTreeMap<u16, String> {
        let mut names = BTreeMap::new();
        let dir_sectors = match dir_sectors {
            Some(s) if !s.is_empty() => s,
            _ => return names,
        };
        let mut cat = Vec::with_capacity(dir_sectors.len() * SECTOR_SIZE);
        for &n in dir_sectors {
            cat.extend_from_slice(self.sector_bytes(n));
        }

        let mut o = 0usize;
        while o + CATALOG_ENTRY <= cat.len() {
            let len = cat[o] as usize;
            let name_ok = (1..=31).contains(&len)
                && cat[o + 1..o + 1 + len]
                    .iter()
                    .all(|&c| (32..127).contains(&c));
            if name_ok {
                let fileid = be16(&cat, o + CATALOG_FILEID_OFF);
                if real_files.contains(&fileid) {
                    let name: String = cat[o + 1..o + 1 + len].iter().map(|&c| c as char).collect();
                    names.entry(fileid).or_insert(name);
                    o += CATALOG_ENTRY;
                    continue;
                }
            }
            o += 2;
        }
        names
    }

    /// Reconstruct a file's bytes by concatenating its sectors in order,
    /// stripping the metadata label from the first sector for normal files.
    fn reconstruct(&self, entry: &LisaEntry, max_bytes: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(entry.size as usize);
        for (i, &sec) in entry.sectors.iter().enumerate() {
            if out.len() >= max_bytes {
                break;
            }
            let s = self.sector_bytes(sec);
            let start = if i == 0 && entry.strip_label {
                LABEL_LEN.min(s.len())
            } else {
                0
            };
            out.extend_from_slice(&s[start..]);
        }
        out.truncate(max_bytes);
        out
    }

    fn entry_at(&self, location: u64) -> Result<&LisaEntry, FilesystemError> {
        self.entries
            .get(location as usize)
            .ok_or_else(|| FilesystemError::NotFound(format!("entry {location} out of range")))
    }
}

/// Read the MDDF volume name (fixed field at 0x0d..0x20), trimming at the first
/// non-printable byte and dropping trailing spaces.
fn read_mddf_volume_name(mddf: &[u8]) -> String {
    let field = &mddf[0x0d..0x20];
    let end = field
        .iter()
        .position(|&c| !(32..127).contains(&c))
        .unwrap_or(field.len());
    String::from_utf8_lossy(&field[..end])
        .trim_end()
        .to_string()
}

/// Whether a file ID should appear in the browse listing: user files plus the
/// recognised system files, but not free/erased blocks or the high-ID
/// (`> 0x7fff`) internal scaffolding (except boot / OS-loader).
fn should_list(fileid: u16) -> bool {
    if fileid == FID_FREE || fileid == FID_ERASED {
        return false;
    }
    if fileid > FID_MAXTAG {
        return matches!(fileid, FID_BOOT | FID_OSLOADER);
    }
    true
}

/// Detect an Apple Lisa File System image: a DiskCopy 4.2 file whose tag region
/// is present (12 bytes/sector) and contains a valid MDDF (file ID `0x0001`
/// with a recognised `fsversion`). Leaves `reader` position undefined.
pub fn looks_like_lisa<R: Read + Seek>(reader: &mut R) -> bool {
    if reader.seek(SeekFrom::Start(0)).is_err() {
        return false;
    }
    let header = match dc42::parse_dc42_header(reader) {
        Some(h) => h,
        None => return false,
    };
    let data_size = header.data_size as usize;
    if data_size == 0 || !data_size.is_multiple_of(SECTOR_SIZE) {
        return false;
    }
    let numblocks = data_size / SECTOR_SIZE;
    if header.tag_size as usize != numblocks * TAG_SIZE {
        return false;
    }

    // Read the tag region and scan for an MDDF whose sector carries a valid
    // fsversion — the combination distinguishes Lisa from a tagged Mac GCR disk.
    if reader
        .seek(SeekFrom::Start(
            (dc42::DC42_HEADER_SIZE as usize + data_size) as u64,
        ))
        .is_err()
    {
        return false;
    }
    let mut tags = vec![0u8; numblocks * TAG_SIZE];
    if reader.read_exact(&mut tags).is_err() {
        return false;
    }
    for n in 0..numblocks {
        if be16(&tags, n * TAG_SIZE + 4) == FID_MDDF {
            // Read just this sector's first two bytes (the fsversion).
            let off = dc42::DC42_HEADER_SIZE as usize + n * SECTOR_SIZE;
            if reader.seek(SeekFrom::Start(off as u64)).is_err() {
                return false;
            }
            let mut v = [0u8; 2];
            if reader.read_exact(&mut v).is_ok() && is_valid_fsversion(be16(&v, 0)) {
                return true;
            }
        }
    }
    false
}

impl Filesystem for LisaFilesystem {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if entry.path != "/" {
            return Ok(Vec::new());
        }
        // Lisa allows duplicate names; disambiguate the path with the file ID
        // while keeping the display name intact.
        let mut out = Vec::with_capacity(self.entries.len());
        for (idx, e) in self.entries.iter().enumerate() {
            let path = format!("/{} [{:04x}]", e.name, e.fileid);
            let mut fe = FileEntry::new_file(e.name.clone(), path, e.size, idx as u64);
            if !is_normal_file(e.fileid) {
                fe.special_type = Some("system".into());
            }
            out.push(fe);
        }
        Ok(out)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let e = self.entry_at(entry.location)?;
        Ok(self.reconstruct(e, max_bytes))
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let e = self.entry_at(entry.location)?;
        let mut written = 0u64;
        for (i, &sec) in e.sectors.iter().enumerate() {
            let s = self.sector_bytes(sec);
            let start = if i == 0 && e.strip_label {
                LABEL_LEN.min(s.len())
            } else {
                0
            };
            writer.write_all(&s[start..]).map_err(FilesystemError::Io)?;
            written += (s.len() - start) as u64;
        }
        Ok(written)
    }

    fn fs_type(&self) -> &str {
        self.fs_version_name
    }

    fn volume_label(&self) -> Option<&str> {
        if self.volume_name.is_empty() {
            None
        } else {
            Some(&self.volume_name)
        }
    }

    fn total_size(&self) -> u64 {
        (self.numblocks * SECTOR_SIZE) as u64
    }

    fn used_size(&self) -> u64 {
        ((self.numblocks - self.free_blocks) * SECTOR_SIZE) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a synthetic DiskCopy 4.2 Lisa image from a per-sector
    /// `(fileid, relblock, data)` layout. `data` is copied into the sector at
    /// offset 0.
    fn build_image(sectors: &[(u16, u16, Vec<u8>)]) -> Vec<u8> {
        let numblocks = sectors.len();
        let data_size = numblocks * SECTOR_SIZE;
        let tag_size = numblocks * TAG_SIZE;

        let mut header = vec![0u8; 84];
        let name = b"-lisa test-";
        header[0] = name.len() as u8;
        header[1..1 + name.len()].copy_from_slice(name);
        header[64..68].copy_from_slice(&(data_size as u32).to_be_bytes());
        header[68..72].copy_from_slice(&(tag_size as u32).to_be_bytes());
        header[80] = 0; // 400K GCR
        header[81] = 0x02;
        header[82..84].copy_from_slice(&0x0100u16.to_be_bytes());

        let mut data = vec![0u8; data_size];
        let mut tags = vec![0u8; tag_size];
        for (i, (fileid, relblock, bytes)) in sectors.iter().enumerate() {
            let off = i * SECTOR_SIZE;
            let n = bytes.len().min(SECTOR_SIZE);
            data[off..off + n].copy_from_slice(&bytes[..n]);
            tags[i * TAG_SIZE + 4..i * TAG_SIZE + 6].copy_from_slice(&fileid.to_be_bytes());
            tags[i * TAG_SIZE + 6..i * TAG_SIZE + 8].copy_from_slice(&relblock.to_be_bytes());
        }

        let mut out = header;
        out.extend_from_slice(&data);
        out.extend_from_slice(&tags);
        out
    }

    /// An MDDF sector: fsversion at 0, volume name at 0x0d.
    fn mddf_sector(fsversion: u16, volume: &str) -> Vec<u8> {
        let mut s = vec![0u8; SECTOR_SIZE];
        s[0..2].copy_from_slice(&fsversion.to_be_bytes());
        let v = volume.as_bytes();
        let n = v.len().min(0x20 - 0x0d);
        s[0x0d..0x0d + n].copy_from_slice(&v[..n]);
        s
    }

    /// A catalog sector containing one entry (Pascal name at 0, file ID at 36).
    fn catalog_sector(name: &str, fileid: u16) -> Vec<u8> {
        let mut s = vec![0u8; SECTOR_SIZE];
        let nm = name.as_bytes();
        s[0] = nm.len() as u8;
        s[1..1 + nm.len()].copy_from_slice(nm);
        s[CATALOG_FILEID_OFF..CATALOG_FILEID_OFF + 2].copy_from_slice(&fileid.to_be_bytes());
        s
    }

    /// A normal file's first sector: 0xF0 label then payload.
    fn file_first_sector(label_marker: u8, payload: &[u8]) -> Vec<u8> {
        let mut s = vec![label_marker; SECTOR_SIZE];
        let n = payload.len().min(SECTOR_SIZE - LABEL_LEN);
        s[LABEL_LEN..LABEL_LEN + n].copy_from_slice(&payload[..n]);
        s
    }

    fn sample_image() -> Vec<u8> {
        build_image(&[
            (FID_BOOT, 0, b"BOOTBLOCK".to_vec()),                  // 0
            (FID_MDDF, 0, mddf_sector(0x0f, "TESTVOL")),           // 1
            (FID_DIRECTORY, 0, catalog_sector("HELLO", 0x0005)),   // 2
            (0x0005, 0, file_first_sector(0xEE, b"Hello, Lisa!")), // 3
            (0x0005, 1, b"SECOND-BLOCK".to_vec()),                 // 4
            (FID_FREE, 0, vec![]),                                 // 5
        ])
    }

    #[test]
    fn detects_lisa_image() {
        let img = sample_image();
        assert!(looks_like_lisa(&mut Cursor::new(&img)));
    }

    #[test]
    fn rejects_non_lisa_dc42() {
        // Same geometry but tags all zero (no MDDF) → not Lisa.
        let img = build_image(&[
            (FID_FREE, 0, vec![]),
            (FID_FREE, 0, vec![]),
            (FID_FREE, 0, vec![]),
        ]);
        assert!(!looks_like_lisa(&mut Cursor::new(&img)));
    }

    #[test]
    fn reads_volume_and_version() {
        let mut fs = LisaFilesystem::open(Cursor::new(sample_image())).expect("open");
        assert_eq!(fs.volume_label(), Some("TESTVOL"));
        assert_eq!(fs.fs_type(), "Lisa FS (flat hash)");
        assert_eq!(fs.total_size(), 6 * SECTOR_SIZE as u64);
        assert_eq!(fs.used_size(), 5 * SECTOR_SIZE as u64); // one free block
                                                            // root lists correctly
        let root = fs.root().unwrap();
        assert_eq!(root.path, "/");
    }

    #[test]
    fn lists_named_file_and_system_files() {
        let mut fs = LisaFilesystem::open(Cursor::new(sample_image())).expect("open");
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"HELLO"), "catalog name resolved: {names:?}");
        assert!(names.contains(&"bootsect-aaaa"));
        assert!(names.contains(&"MDDF-0001"));
        assert!(names.contains(&"directory-0004"));
        // free block is not listed
        assert!(!names.iter().any(|n| n.starts_with("freeblocks")));
    }

    #[test]
    fn reconstructs_file_stripping_label() {
        let mut fs = LisaFilesystem::open(Cursor::new(sample_image())).expect("open");
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let hello = entries.iter().find(|e| e.name == "HELLO").unwrap();
        // Two sectors, first is a normal-file label sector.
        assert_eq!(hello.size, (2 * SECTOR_SIZE - LABEL_LEN) as u64);
        let data = fs.read_file(hello, usize::MAX).unwrap();
        assert!(data.starts_with(b"Hello, Lisa!"), "label stripped");
        assert!(
            data.windows(12).any(|w| w == b"SECOND-BLOCK"),
            "second block present"
        );
        // write_file_to matches read_file
        let mut buf = Vec::new();
        let n = fs.write_file_to(hello, &mut buf).unwrap();
        assert_eq!(n as usize, buf.len());
        assert_eq!(buf, data);
    }

    #[test]
    fn system_file_keeps_full_first_sector() {
        let mut fs = LisaFilesystem::open(Cursor::new(sample_image())).expect("open");
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let boot = entries.iter().find(|e| e.name == "bootsect-aaaa").unwrap();
        let data = fs.read_file(boot, usize::MAX).unwrap();
        assert!(
            data.starts_with(b"BOOTBLOCK"),
            "no label strip on system file"
        );
        assert_eq!(data.len(), SECTOR_SIZE);
    }

    #[test]
    fn btree_volume_falls_back_to_fileid_names() {
        // fsversion 0x11 and no catalog block → file-XXXX names, extraction ok.
        let img = build_image(&[
            (FID_MDDF, 0, mddf_sector(0x11, "Lisa Guide")),
            (0x0007, 0, file_first_sector(0xEE, b"payload")),
        ]);
        let mut fs = LisaFilesystem::open(Cursor::new(img)).expect("open");
        assert_eq!(fs.fs_type(), "Lisa FS (B-tree)");
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let f = entries.iter().find(|e| e.name == "file-0007").unwrap();
        assert!(f.path.contains("[0007]"));
        let data = fs.read_file(f, usize::MAX).unwrap();
        assert!(data.starts_with(b"payload"));
    }
}
