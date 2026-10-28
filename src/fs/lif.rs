//! HP LIF (Logical Interchange Format): the boot area of HP 9000 disks, tapes and CDs.
//!
//! An HP-UX disk, a NetBSD/hp300 disk and an HP-UX or NetBSD/hppa install CD all
//! start with a LIF volume: the boot ROM reads its directory to find `ISL`, `HPUX`,
//! `SYS_UBOOT`, `CDBOOT` and friends. Everything is big-endian and counted in
//! 256-byte LIF sectors.
//!
//! Volume header (sector 0): `u16` magic `0x8000`, 6-byte label, `u32` directory
//! start sector, `u16` `0x1000` (the System 3000 id), `u32` directory length in
//! sectors, `u16` version.
//!
//! Directory entry (32 bytes): 10-byte space-padded name, `i16` file type
//! (`0` = purged, `-1` = end of directory), `u32` start sector, `u32` length in
//! sectors, 6-byte BCD date `YYMMDDhhmmss`, `u16` volume flag, `u32`
//! implementation word. Entries may alias the same extent: the HP-UX 11.11 DVD
//! lists `INSTALLFS`, `VINSTALLFS` and `WINSTALLFS` over one range.
//!
//! Read-only: the files are listed flat at the root and read by extent.

use std::io::{Read, Seek, SeekFrom};

use byteorder::{BigEndian, ByteOrder};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};

/// LIF counts everything in 256-byte sectors.
pub const LIF_SECTOR: u64 = 256;
/// Volume header magic at byte 0.
const LIF_MAGIC: u16 = 0x8000;
/// Directory sizes seen in the wild are a handful of sectors; anything past this is not LIF.
const MAX_DIR_SECTORS: u32 = 4096;

/// One file in a LIF directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifFile {
    pub name: String,
    /// LIF file type, e.g. `0xCE00` ISL, `0xCD80` HP-UX boot, `0xCFFF` AUTO.
    pub file_type: u16,
    /// Byte offset of the file from the start of the LIF volume.
    pub start_byte: u64,
    pub len_bytes: u64,
    /// `YYYY-MM-DD hh:mm:ss`, or `None` when the BCD date is not valid.
    pub modified: Option<String>,
}

/// A parsed LIF volume header and directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifVolume {
    pub label: String,
    pub files: Vec<LifFile>,
}

/// Whether the first sector of `head` is a LIF volume header.
pub fn looks_like_lif(head: &[u8]) -> bool {
    if head.len() < 22 || BigEndian::read_u16(&head[0..2]) != LIF_MAGIC {
        return false;
    }
    let dir_start = BigEndian::read_u32(&head[8..12]);
    let dir_len = BigEndian::read_u32(&head[16..20]);
    dir_start >= 1 && (1..=MAX_DIR_SECTORS).contains(&dir_len)
}

/// Parse the LIF volume at byte `base` of `reader`, or `None` when there is none.
pub fn parse_lif<R: Read + Seek>(reader: &mut R, base: u64) -> Option<LifVolume> {
    let mut head = [0u8; LIF_SECTOR as usize];
    reader.seek(SeekFrom::Start(base)).ok()?;
    reader.read_exact(&mut head).ok()?;
    if !looks_like_lif(&head) {
        return None;
    }
    let label = String::from_utf8_lossy(&head[2..8]).trim_end().to_string();
    let dir_start = BigEndian::read_u32(&head[8..12]) as u64;
    let dir_len = BigEndian::read_u32(&head[16..20]) as usize;
    let mut dir = vec![0u8; dir_len * LIF_SECTOR as usize];
    reader
        .seek(SeekFrom::Start(base + dir_start * LIF_SECTOR))
        .ok()?;
    reader.read_exact(&mut dir).ok()?;

    let mut files = Vec::new();
    for entry in dir.chunks_exact(32) {
        let file_type = BigEndian::read_i16(&entry[10..12]);
        if file_type == -1 {
            break;
        }
        if file_type == 0 {
            continue;
        }
        let name = String::from_utf8_lossy(&entry[0..10])
            .trim_end()
            .to_string();
        if name.is_empty() {
            continue;
        }
        files.push(LifFile {
            name,
            file_type: file_type as u16,
            start_byte: BigEndian::read_u32(&entry[12..16]) as u64 * LIF_SECTOR,
            len_bytes: BigEndian::read_u32(&entry[16..20]) as u64 * LIF_SECTOR,
            modified: bcd_date(&entry[20..26]),
        });
    }
    Some(LifVolume { label, files })
}

/// Decode LIF's 6-byte BCD `YYMMDDhhmmss`; years below 70 are 20xx.
fn bcd_date(b: &[u8]) -> Option<String> {
    let mut v = [0u32; 6];
    for (i, &byte) in b.iter().take(6).enumerate() {
        let (hi, lo) = ((byte >> 4) as u32, (byte & 0x0F) as u32);
        if hi > 9 || lo > 9 {
            return None;
        }
        v[i] = hi * 10 + lo;
    }
    let [yy, mo, dd, hh, mi, ss] = v;
    if !(1..=12).contains(&mo) || !(1..=31).contains(&dd) || hh > 23 || mi > 59 || ss > 59 {
        return None;
    }
    let year = if yy < 70 { 2000 + yy } else { 1900 + yy };
    Some(format!("{year:04}-{mo:02}-{dd:02} {hh:02}:{mi:02}:{ss:02}"))
}

/// A LIF volume browsed as a flat, read-only directory of its files.
pub struct LifFilesystem<R: Read + Seek + Send> {
    reader: R,
    base: u64,
    volume: LifVolume,
}

impl<R: Read + Seek + Send> LifFilesystem<R> {
    pub fn open(mut reader: R, base: u64) -> Result<Self, FilesystemError> {
        let volume = parse_lif(&mut reader, base)
            .ok_or_else(|| FilesystemError::Parse("not a LIF volume".into()))?;
        Ok(Self {
            reader,
            base,
            volume,
        })
    }

    /// The parsed directory.
    pub fn volume(&self) -> &LifVolume {
        &self.volume
    }

    fn entry_for(&self, index: usize, file: &LifFile) -> FileEntry {
        let mut e = FileEntry::new_file(
            file.name.clone(),
            format!("/{}", file.name),
            file.len_bytes,
            index as u64,
        );
        e.modified = file.modified.clone();
        e.special_type = Some(format!("LIF type {:04X}", file.file_type));
        e
    }
}

impl<R: Read + Seek + Send> Filesystem for LifFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::root())
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if entry.path != "/" {
            return Ok(Vec::new());
        }
        Ok(self
            .volume
            .files
            .iter()
            .enumerate()
            .map(|(i, f)| self.entry_for(i, f))
            .collect())
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let file = self
            .volume
            .files
            .get(entry.location as usize)
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;
        let len = (file.len_bytes as usize).min(max_bytes);
        let mut out = vec![0u8; len];
        self.reader
            .seek(SeekFrom::Start(self.base + file.start_byte))?;
        self.reader.read_exact(&mut out)?;
        Ok(out)
    }

    fn volume_label(&self) -> Option<&str> {
        Some(self.volume.label.as_str()).filter(|l| !l.is_empty())
    }

    fn fs_type(&self) -> &str {
        "LIF (HP boot area)"
    }

    fn total_size(&self) -> u64 {
        self.volume
            .files
            .iter()
            .map(|f| f.start_byte + f.len_bytes)
            .max()
            .unwrap_or(0)
    }

    fn used_size(&self) -> u64 {
        self.total_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// A LIF volume "BOOT43" with ISL (2 sectors of 0xAA) and a purged entry, NetBSD/hp300 style.
    fn sample_lif() -> Vec<u8> {
        let mut img = vec![0u8; 16 * 256];
        BigEndian::write_u16(&mut img[0..2], LIF_MAGIC);
        img[2..8].copy_from_slice(b"BOOT43");
        BigEndian::write_u32(&mut img[8..12], 2);
        BigEndian::write_u16(&mut img[12..14], 0x1000);
        BigEndian::write_u32(&mut img[16..20], 1);
        BigEndian::write_u16(&mut img[20..22], 1);
        let dir = 2 * 256;
        let mut put = |slot: usize, name: &[u8], ty: i16, start: u32, len: u32, date: [u8; 6]| {
            let e = &mut img[dir + slot * 32..dir + slot * 32 + 32];
            e[0..10].fill(b' ');
            e[0..name.len()].copy_from_slice(name);
            BigEndian::write_i16(&mut e[10..12], ty);
            BigEndian::write_u32(&mut e[12..16], start);
            BigEndian::write_u32(&mut e[16..20], len);
            e[20..26].copy_from_slice(&date);
        };
        put(0, b"OLD", 0, 0, 0, [0; 6]);
        put(
            1,
            b"ISL",
            0xCE00u16 as i16,
            4,
            2,
            [0x04, 0x11, 0x04, 0x14, 0x27, 0x53],
        );
        put(2, b"BAD", -1, 0, 0, [0; 6]);
        img[4 * 256..6 * 256].fill(0xAA);
        img
    }

    #[test]
    fn parses_directory_and_skips_purged_entries() {
        let vol = parse_lif(&mut Cursor::new(sample_lif()), 0).unwrap();
        assert_eq!(vol.label, "BOOT43");
        assert_eq!(
            vol.files.len(),
            1,
            "purged entry skipped, end marker stops the walk"
        );
        let isl = &vol.files[0];
        assert_eq!((isl.name.as_str(), isl.file_type), ("ISL", 0xCE00));
        assert_eq!((isl.start_byte, isl.len_bytes), (1024, 512));
        assert_eq!(isl.modified.as_deref(), Some("2004-11-04 14:27:53"));
    }

    #[test]
    fn filesystem_lists_and_reads_files() {
        let mut fs = LifFilesystem::open(Cursor::new(sample_lif()), 0).unwrap();
        let root = fs.root().unwrap();
        let files = fs.list_directory(&root).unwrap();
        assert_eq!(files.len(), 1);
        let data = fs.read_file(&files[0], usize::MAX).unwrap();
        assert_eq!(data, vec![0xAA; 512]);
        assert_eq!(fs.volume_label(), Some("BOOT43"));
    }

    #[test]
    fn rejects_non_lif_and_bad_dates() {
        assert!(!looks_like_lif(&[0u8; 256]));
        let mut iso_like = sample_lif();
        BigEndian::write_u32(&mut iso_like[16..20], 0);
        assert!(!looks_like_lif(&iso_like));
        assert_eq!(bcd_date(&[0x08, 0x17, 0xC1, 0x15, 0x13, 0x05]), None);
    }
}
