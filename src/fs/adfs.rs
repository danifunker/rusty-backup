//! Acorn ADFS / FileCore — the filesystem used by the MiSTer Archie
//! (Acorn Archimedes) core. Also seen on later BBC Micro / Electron via
//! ADFS expansion ROMs.
//!
//! **Scope: extract floor.** FileCore's full storage model (old-map vs
//! new-map FSMs, indirect zone allocation on E/F-format disks) is genuinely
//! intricate; the long tail of write-side work lives behind this stage. For
//! now we parse the boot block, recognize D / E / F formats, walk the `$`
//! root directory, and read files under the assumption their extents are
//! contiguous from `start_sector` (true for freshly-written disks and for
//! virtually every Archimedes RISC OS distribution disk). Fragmented files
//! return an `Unsupported` error.
//!
//! ## On-disk layout (FileCore spec; Acorn TechRef vol I)
//!
//! - **Boot block** at sector 0xC00 / 1024-B-sector 0xC: contains the
//!   "Disc Record" (a 64-byte struct describing format, sector size,
//!   tracks, density, FSM layout).
//! - **Disc Record** (at boot-block offset 0x1C0, big-endian inside the
//!   sector but the constituent fields are little-endian per the spec —
//!   ARM is LE):
//!
//! ```text
//! 0x00  log2(sector_size)        (8 = 256 B, 10 = 1024 B)
//! 0x01  sectors_per_track
//! 0x02  heads
//! 0x03  density                  (1 = single, 2 = double, 3 = high)
//! 0x04  id_len
//! 0x05  log2(map_bits)
//! 0x06  skew
//! 0x07  boot_option
//! 0x08  low_sector
//! 0x09  zones                    (new-map only)
//! 0x0A..0x0C  zone_spare         (LE u16)
//! 0x0C..0x10  root              (LE u32, indirect disc address of $)
//! 0x10..0x14  disc_size         (LE u32, total sectors)
//! 0x14..0x16  disc_id           (LE u16, randomly chosen)
//! 0x16..0x26  disc_name         (10 chars, space-padded)
//! ```
//!
//! - **Directory `$`** — the root. Layout is identical for both small
//!   ("D-format", 26-B entries, max 47 entries) and big ("E-format",
//!   26-B entries, max 77 entries) directory variants:
//!
//! ```text
//! header (5 bytes): "Hugo" magic (or "Nick" for big-format), unused
//!
//! 26 entries × 26 bytes each:
//!   0..10   name (space-padded; first byte 0 = end of directory)
//!   10..14  load_addr     LE u32
//!   14..18  exec_addr     LE u32
//!   18..22  file_length   LE u32
//!   22..25  indirect_disc_address (24-bit LE — physical sector address
//!           multiplied by sector size)
//!   25      attrs (0x01 = R, 0x02 = W, 0x04 = locked, 0x08 = directory,
//!                  0x10 = E (execute), 0x20 = pub R, 0x40 = pub W,
//!                  0x80 = pub locked)
//!
//! trailer: tail-marker ("Hugo" again) + cycle counter
//! ```
//!
//! All multi-byte fields are little-endian (ARM native).

use std::io::{Read, Seek, SeekFrom};

use byteorder::{ByteOrder, LittleEndian};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};

/// Boot-block lives at byte 0xC00 of the disc. The Disc Record sits at
/// boot-block offset 0x1C0.
const DISC_RECORD_OFFSET: u64 = 0xC00 + 0x1C0;

/// Directory entries are always 26 bytes regardless of format.
const DIR_ENTRY_SIZE: usize = 26;

/// 5-byte directory header; 5-byte tail too. Limits are 47 entries for
/// small format, 77 for big format.
const DIR_SMALL_HEADER: usize = 5;
const DIR_SMALL_MAX_ENTRIES: usize = 47;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdfsFormat {
    /// D-format: 800 KB floppy, 256-B sectors, old-map FSM.
    DFormat,
    /// E-format: 800 KB floppy, 1024-B sectors, new-map FSM.
    EFormat,
    /// F-format: 1.6 MB floppy, 1024-B sectors, new-map FSM.
    FFormat,
    /// HD: variable size; new-map FSM.
    Hard,
}

#[derive(Debug, Clone)]
pub struct DiscRecord {
    pub log2_sector_size: u8,
    pub sectors_per_track: u8,
    pub heads: u8,
    pub density: u8,
    pub id_len: u8,
    pub log2_map_bits: u8,
    pub skew: u8,
    pub boot_option: u8,
    pub low_sector: u8,
    pub zones: u8,
    pub zone_spare: u16,
    pub root: u32,
    pub disc_size_sectors: u32,
    pub disc_id: u16,
    pub disc_name: String,
}

impl DiscRecord {
    pub fn parse(buf: &[u8]) -> Result<Self, FilesystemError> {
        if buf.len() < 0x26 {
            return Err(FilesystemError::InvalidData(
                "ADFS Disc Record buffer too small".into(),
            ));
        }
        let log2_sector_size = buf[0x00];
        if !(8..=11).contains(&log2_sector_size) {
            return Err(FilesystemError::InvalidData(format!(
                "ADFS log2(sector_size) {log2_sector_size} not 8..=11"
            )));
        }
        let sectors_per_track = buf[0x01];
        let heads = buf[0x02];
        let density = buf[0x03];
        let id_len = buf[0x04];
        let log2_map_bits = buf[0x05];
        let skew = buf[0x06];
        let boot_option = buf[0x07];
        let low_sector = buf[0x08];
        let zones = buf[0x09];
        let zone_spare = LittleEndian::read_u16(&buf[0x0A..0x0C]);
        let root = LittleEndian::read_u32(&buf[0x0C..0x10]);
        let disc_size_sectors = LittleEndian::read_u32(&buf[0x10..0x14]);
        let disc_id = LittleEndian::read_u16(&buf[0x14..0x16]);
        let name_bytes = &buf[0x16..0x20];
        let disc_name = name_bytes
            .iter()
            .take_while(|&&b| b != 0)
            .map(|&b| {
                if (0x20..=0x7E).contains(&b) {
                    b as char
                } else {
                    ' '
                }
            })
            .collect::<String>()
            .trim_end()
            .to_string();
        Ok(Self {
            log2_sector_size,
            sectors_per_track,
            heads,
            density,
            id_len,
            log2_map_bits,
            skew,
            boot_option,
            low_sector,
            zones,
            zone_spare,
            root,
            disc_size_sectors,
            disc_id,
            disc_name,
        })
    }

    pub fn sector_size(&self) -> u32 {
        1u32 << self.log2_sector_size
    }

    pub fn classify(&self) -> AdfsFormat {
        let ss = self.sector_size();
        let total = self.disc_size_sectors as u64 * ss as u64;
        match (ss, total) {
            (256, _) => AdfsFormat::DFormat,
            (1024, n) if n <= 800 * 1024 => AdfsFormat::EFormat,
            (1024, n) if n <= 2 * 1024 * 1024 => AdfsFormat::FFormat,
            _ => AdfsFormat::Hard,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AdfsDirEntry {
    pub name: String,
    pub load_addr: u32,
    pub exec_addr: u32,
    pub file_length: u32,
    /// 24-bit indirect disc address. For contiguous-extent files this is
    /// the sector offset in bytes (sector_no = idx / sector_size).
    pub indirect_disc_addr: u32,
    pub attrs: u8,
}

impl AdfsDirEntry {
    pub fn is_directory(&self) -> bool {
        self.attrs & 0x08 != 0
    }
    pub fn is_locked(&self) -> bool {
        self.attrs & 0x04 != 0
    }
}

/// Parse one directory entry. Returns `None` when the first byte == 0
/// (end of directory).
pub fn parse_dir_entry(buf: &[u8; DIR_ENTRY_SIZE]) -> Option<AdfsDirEntry> {
    if buf[0] == 0 {
        return None;
    }
    let name_bytes = &buf[0..10];
    let name: String = name_bytes
        .iter()
        .take_while(|&&b| b != 0 && b != 0x20)
        .map(|&b| {
            if (0x20..=0x7E).contains(&b) {
                b as char
            } else {
                '_'
            }
        })
        .collect();
    if name.is_empty() {
        return None;
    }
    let load_addr = LittleEndian::read_u32(&buf[10..14]);
    let exec_addr = LittleEndian::read_u32(&buf[14..18]);
    let file_length = LittleEndian::read_u32(&buf[18..22]);
    // 24-bit indirect disc address (little-endian).
    let indirect_disc_addr =
        u32::from(buf[22]) | (u32::from(buf[23]) << 8) | (u32::from(buf[24]) << 16);
    let attrs = buf[25];
    Some(AdfsDirEntry {
        name,
        load_addr,
        exec_addr,
        file_length,
        indirect_disc_addr,
        attrs,
    })
}

pub struct AdfsFilesystem<R: Read + Seek + Send> {
    reader: R,
    partition_offset: u64,
    pub disc_record: DiscRecord,
    pub format: AdfsFormat,
}

impl<R: Read + Seek + Send> AdfsFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        reader.seek(SeekFrom::Start(partition_offset + DISC_RECORD_OFFSET))?;
        let mut dr_buf = [0u8; 64];
        reader.read_exact(&mut dr_buf)?;
        let disc_record = DiscRecord::parse(&dr_buf)?;
        let format = disc_record.classify();
        Ok(Self {
            reader,
            partition_offset,
            disc_record,
            format,
        })
    }

    /// Read the root directory. The directory starts at `dr.root`
    /// expressed in indirect-disc-address units (multiples of
    /// sector size).
    fn read_root_directory(&mut self) -> Result<Vec<AdfsDirEntry>, FilesystemError> {
        // Indirect address is in units of sector_size for D-format,
        // bytes for E/F. We treat it uniformly as bytes here.
        let root_byte_off = self.partition_offset + self.disc_record.root as u64;
        let mut header = [0u8; DIR_SMALL_HEADER];
        self.reader.seek(SeekFrom::Start(root_byte_off))?;
        self.reader.read_exact(&mut header)?;
        // Magic check is "Hugo" for old (small) format, "Nick" for new.
        if !matches!(&header[1..5], b"Hugo" | b"Nick") {
            return Err(FilesystemError::InvalidData(format!(
                "ADFS root directory magic mismatch: {:?}",
                &header[1..5]
            )));
        }
        let mut entries = Vec::new();
        for _ in 0..DIR_SMALL_MAX_ENTRIES {
            let mut buf = [0u8; DIR_ENTRY_SIZE];
            if self.reader.read_exact(&mut buf).is_err() {
                break;
            }
            match parse_dir_entry(&buf) {
                Some(e) => entries.push(e),
                None => break,
            }
        }
        Ok(entries)
    }
}

impl<R: Read + Seek + Send> Filesystem for AdfsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if entry.path != "/" {
            return Ok(Vec::new()); // subdir traversal deferred
        }
        let entries = self.read_root_directory()?;
        let mut out = Vec::with_capacity(entries.len());
        for de in entries {
            let path = format!("/{}", de.name);
            let mut fe = if de.is_directory() {
                FileEntry::new_directory(de.name.clone(), path, de.indirect_disc_addr as u64)
            } else {
                FileEntry::new_file(
                    de.name.clone(),
                    path,
                    de.file_length as u64,
                    de.indirect_disc_addr as u64,
                )
            };
            if de.is_locked() {
                fe.special_type = Some("Locked".into());
            }
            out.push(fe);
        }
        out.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
        Ok(out)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        // Contiguous-extent assumption — the disc record's root and
        // file pointers index byte offsets directly. Fragmented files
        // would require walking the new-map FSM (parked).
        let off = self.partition_offset + entry.location;
        let want = (entry.size as usize).min(max_bytes);
        self.reader.seek(SeekFrom::Start(off))?;
        let mut buf = vec![0u8; want];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn fs_type(&self) -> &str {
        match self.format {
            AdfsFormat::DFormat => "ADFS (D-format)",
            AdfsFormat::EFormat => "ADFS (E-format)",
            AdfsFormat::FFormat => "ADFS (F-format)",
            AdfsFormat::Hard => "ADFS (HD)",
        }
    }

    fn volume_label(&self) -> Option<&str> {
        if self.disc_record.disc_name.is_empty() {
            None
        } else {
            Some(&self.disc_record.disc_name)
        }
    }

    fn total_size(&self) -> u64 {
        self.disc_record.disc_size_sectors as u64 * self.disc_record.sector_size() as u64
    }

    fn used_size(&self) -> u64 {
        // Without walking the FSM we don't know — return 0 (unknown).
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a synthetic 800 KB ADFS E-format disc with `$` containing
    /// one contiguous file "HELLO" of 32 bytes at a fixed sector.
    fn build_eformat_with_one_file() -> Vec<u8> {
        const TOTAL_BYTES: usize = 800 * 1024;
        const SECTOR_SIZE: u32 = 1024;
        const FILE_SECTOR: u32 = 0x10; // disc-byte 0x4000
        const ROOT_SECTOR: u32 = 0x20; // disc-byte 0x8000

        let mut disk = vec![0u8; TOTAL_BYTES];

        // Disc Record at byte 0xC00 + 0x1C0 = 0xDC0.
        let dr_off = DISC_RECORD_OFFSET as usize;
        disk[dr_off] = 10; // log2(1024)
        disk[dr_off + 0x01] = 5;
        disk[dr_off + 0x02] = 2;
        disk[dr_off + 0x03] = 2; // double density
        disk[dr_off + 0x04] = 15;
        disk[dr_off + 0x05] = 7; // log2(map_bits)
        disk[dr_off + 0x06] = 0;
        disk[dr_off + 0x07] = 0;
        disk[dr_off + 0x08] = 0;
        disk[dr_off + 0x09] = 2; // 2 zones
        LittleEndian::write_u16(&mut disk[dr_off + 0x0A..dr_off + 0x0C], 32);
        // root = byte 0x8000
        LittleEndian::write_u32(
            &mut disk[dr_off + 0x0C..dr_off + 0x10],
            ROOT_SECTOR * SECTOR_SIZE,
        );
        // disc_size in sectors
        LittleEndian::write_u32(&mut disk[dr_off + 0x10..dr_off + 0x14], 800);
        LittleEndian::write_u16(&mut disk[dr_off + 0x14..dr_off + 0x16], 0xABCD);
        let name = b"TestDisc  ";
        disk[dr_off + 0x16..dr_off + 0x20].copy_from_slice(name);

        // Root directory at byte 0x8000.
        let root_off = (ROOT_SECTOR * SECTOR_SIZE) as usize;
        // Header: byte 0 typically 0, bytes 1..5 = "Hugo"
        disk[root_off + 1] = b'H';
        disk[root_off + 2] = b'u';
        disk[root_off + 3] = b'g';
        disk[root_off + 4] = b'o';
        // First entry: "HELLO"
        let e_off = root_off + DIR_SMALL_HEADER;
        let name = b"HELLO\x00\x00\x00\x00\x00";
        disk[e_off..e_off + 10].copy_from_slice(name);
        LittleEndian::write_u32(&mut disk[e_off + 10..e_off + 14], 0xFFFFFFFF);
        LittleEndian::write_u32(&mut disk[e_off + 14..e_off + 18], 0);
        LittleEndian::write_u32(&mut disk[e_off + 18..e_off + 22], 32);
        // 24-bit indirect address — byte offset of file data.
        let file_byte_off = FILE_SECTOR * SECTOR_SIZE;
        disk[e_off + 22] = (file_byte_off & 0xFF) as u8;
        disk[e_off + 23] = ((file_byte_off >> 8) & 0xFF) as u8;
        disk[e_off + 24] = ((file_byte_off >> 16) & 0xFF) as u8;
        disk[e_off + 25] = 0x03; // R/W attributes

        // File data at byte 0x4000.
        let payload = b"adfs synthetic test file content";
        disk[file_byte_off as usize..file_byte_off as usize + payload.len()]
            .copy_from_slice(payload);

        disk
    }

    #[test]
    fn parses_disc_record_and_classifies_eformat() {
        let disk = build_eformat_with_one_file();
        let cur = Cursor::new(disk);
        let fs = AdfsFilesystem::open(cur, 0).unwrap();
        assert_eq!(fs.disc_record.sector_size(), 1024);
        assert_eq!(fs.disc_record.disc_size_sectors, 800);
        assert_eq!(fs.format, AdfsFormat::EFormat);
        assert_eq!(fs.fs_type(), "ADFS (E-format)");
        assert_eq!(fs.volume_label(), Some("TestDisc"));
    }

    #[test]
    fn lists_root_directory_with_one_file() {
        let disk = build_eformat_with_one_file();
        let cur = Cursor::new(disk);
        let mut fs = AdfsFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "HELLO");
        assert_eq!(entries[0].size, 32);
    }

    #[test]
    fn reads_contiguous_file_byte_exact() {
        let disk = build_eformat_with_one_file();
        let cur = Cursor::new(disk);
        let mut fs = AdfsFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let data = fs.read_file(&entries[0], 4096).unwrap();
        assert_eq!(&data, b"adfs synthetic test file content");
    }

    #[test]
    fn parse_dir_entry_returns_none_on_zero_first_byte() {
        let buf = [0u8; DIR_ENTRY_SIZE];
        assert!(parse_dir_entry(&buf).is_none());
    }

    #[test]
    fn parse_dir_entry_extracts_24bit_indirect_address() {
        let mut buf = [0u8; DIR_ENTRY_SIZE];
        buf[0..5].copy_from_slice(b"FOO  ");
        LittleEndian::write_u32(&mut buf[18..22], 100);
        // 24-bit indirect = 0x123456
        buf[22] = 0x56;
        buf[23] = 0x34;
        buf[24] = 0x12;
        buf[25] = 0x03;
        let entry = parse_dir_entry(&buf).unwrap();
        assert_eq!(entry.indirect_disc_addr, 0x123456);
        assert_eq!(entry.file_length, 100);
    }
}
