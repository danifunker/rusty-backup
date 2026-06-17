//! Atari DOS 2.0S / 2.5 filesystem — the floppy filesystem for the 8-bit
//! Atari 400 / 800 / XL / XE (and the MiSTer Atari800 core).
//!
//! The host OS can't mount these disks, so file extract + add + delete is
//! the whole point (Axis 1 of the MiSTer plan — floppy-only, no resize).
//!
//! ## Geometry
//!
//! Atari disks are flat sector images (the 16-byte `.atr` header is
//! stripped upstream by the container layer; `.xfd` is already headerless).
//! Sectors are numbered from 1. Two single-density-derived shapes are
//! supported:
//!
//! | Body bytes | Sectors | Sector size | Density            |
//! |------------|---------|-------------|--------------------|
//! | 92160      | 720     | 128         | single (DOS 2.0S)  |
//! | 133120     | 1040    | 128         | enhanced (DOS 2.5) |
//!
//! (Double-density 256-byte images use a different data-byte-per-sector
//! count and a 128-byte boot region; read support for those is a follow-up.)
//!
//! ## On-disk layout (Atari DOS 2 / "Atari DOS 2.0S" reference)
//!
//! - **Boot sectors** 1-3 hold DOS itself.
//! - **VTOC** (Volume Table of Contents) at **sector 360**:
//!   ```text
//!   0x00       directory/DOS code (2 = DOS 2.0/2.5)
//!   0x01..0x02 total available sectors (LE) — 707 on an SD disk
//!   0x03..0x04 current free sectors (LE)
//!   0x0A..0x63 sector bitmap, 90 bytes = 720 bits. Bit SET = free.
//!              Sector N is bit (7 - N%8) of byte 0x0A + N/8 (MSB-first).
//!   ```
//! - **Directory** at **sectors 361-368** (8 sectors × 8 entries of 16
//!   bytes = 64 files max):
//!   ```text
//!   0x00       flag: 0x00 unused (end), 0x80 deleted, 0x42 in-use DOS2,
//!              bit 0x20 = locked
//!   0x01..0x02 sector count (LE)
//!   0x03..0x04 starting sector (LE)
//!   0x05..0x0C filename (8 bytes, ATASCII, space-padded)
//!   0x0D..0x0F extension (3 bytes, space-padded)
//!   ```
//! - **File data**: a linked sector chain. In each sector the last 3 bytes
//!   are metadata (`data_bytes = sector_size - 3`):
//!   ```text
//!   [data_bytes]   payload
//!   +0  high 6 bits = file number (dir index), low 2 bits = next-sector hi
//!   +1  next-sector low 8 bits  (next == 0 marks the last sector)
//!   +2  bytes used in this sector (1..=data_bytes)
//!   ```
//!   `next_sector = ((b0 & 3) << 8) | b1` addresses up to sector 1023.

use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::FileEntry;
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};

pub const VTOC_SECTOR: u16 = 360;
pub const DIR_FIRST_SECTOR: u16 = 361;
pub const DIR_SECTORS: u16 = 8;
pub const MAX_DIR_ENTRIES: usize = 64;
const ENTRY_LEN: usize = 16;
const BITMAP_OFFSET: usize = 0x0A;

/// Directory-entry flag bits.
const FLAG_IN_USE: u8 = 0x40;
const FLAG_DELETED: u8 = 0x80;
const FLAG_LOCKED: u8 = 0x20;
const FLAG_DOS2: u8 = 0x02;

/// Runaway guard for a single file's sector chain.
const MAX_FILE_SECTORS: usize = 2048;

/// Disk geometry, derived from the flat-body byte length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AtariGeometry {
    pub sector_size: usize,
    pub total_sectors: u16,
}

impl AtariGeometry {
    /// Recognize a flat Atari disk body by exact size. Returns `None` for
    /// anything that isn't a known geometry.
    pub fn from_body_len(len: u64) -> Option<AtariGeometry> {
        match len {
            92_160 => Some(AtariGeometry {
                sector_size: 128,
                total_sectors: 720,
            }),
            133_120 => Some(AtariGeometry {
                sector_size: 128,
                total_sectors: 1040,
            }),
            _ => None,
        }
    }

    /// Data bytes carried per sector (the last 3 bytes are chain metadata).
    fn data_bytes(self) -> usize {
        self.sector_size - 3
    }

    /// Byte offset of a 1-based sector within the flat body.
    fn sector_offset(self, sector: u16) -> u64 {
        (sector as u64 - 1) * self.sector_size as u64
    }
}

/// One parsed directory entry plus its slot index.
#[derive(Debug, Clone)]
pub struct AtariDirEntry {
    pub flag: u8,
    pub sector_count: u16,
    pub start_sector: u16,
    /// Raw 11-byte 8.3 name (8 name + 3 ext), for byte-exact compares.
    pub raw_name: [u8; 11],
    /// Decoded display name ("NAME.EXT").
    pub name: String,
    /// Slot index 0..64.
    pub slot: usize,
}

impl AtariDirEntry {
    fn is_live(&self) -> bool {
        self.flag & FLAG_IN_USE != 0 && self.flag & FLAG_DELETED == 0
    }

    fn locked(&self) -> bool {
        self.flag & FLAG_LOCKED != 0
    }
}

/// Decode an 11-byte 8.3 name into "NAME.EXT" (trailing spaces stripped).
fn decode_name(raw: &[u8; 11]) -> String {
    let name = String::from_utf8_lossy(&raw[0..8]).trim_end().to_string();
    let ext = String::from_utf8_lossy(&raw[8..11]).trim_end().to_string();
    if ext.is_empty() {
        name
    } else {
        format!("{name}.{ext}")
    }
}

/// Encode "NAME.EXT" into the 11-byte space-padded on-disk form. Atari
/// names are uppercase ATASCII; the first character must be a letter.
pub fn encode_name(name: &str) -> Result<[u8; 11], FilesystemError> {
    let (base, ext) = match name.split_once('.') {
        Some((b, e)) => (b, e),
        None => (name, ""),
    };
    if base.is_empty() || base.len() > 8 || ext.len() > 3 {
        return Err(FilesystemError::InvalidData(format!(
            "Atari filename '{name}' must be 1-8 chars + optional 1-3 char extension"
        )));
    }
    let mut out = [b' '; 11];
    let put = |slice: &str, dst: &mut [u8]| -> Result<(), FilesystemError> {
        for (i, c) in slice.chars().enumerate() {
            if !c.is_ascii() {
                return Err(FilesystemError::InvalidData(format!(
                    "Atari filename '{name}' contains non-ASCII character '{c}'"
                )));
            }
            let b = (c as u8).to_ascii_uppercase();
            if !b.is_ascii_uppercase() && !b.is_ascii_digit() {
                return Err(FilesystemError::InvalidData(format!(
                    "Atari filename '{name}' char '{c}' must be A-Z or 0-9"
                )));
            }
            dst[i] = b;
        }
        Ok(())
    };
    put(base, &mut out[0..8])?;
    put(ext, &mut out[8..11])?;
    if !out[0].is_ascii_uppercase() {
        return Err(FilesystemError::InvalidData(format!(
            "Atari filename '{name}' must start with a letter"
        )));
    }
    Ok(out)
}

/// Sniff whether the flat body at `partition_offset` is an Atari DOS 2
/// volume: exact geometry size AND a plausible VTOC at sector 360.
pub fn looks_like_atari_dos<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
) -> Option<AtariGeometry> {
    let len = reader
        .seek(SeekFrom::End(0))
        .ok()?
        .checked_sub(partition_offset)?;
    let geom = AtariGeometry::from_body_len(len)?;
    let off = partition_offset + geom.sector_offset(VTOC_SECTOR);
    reader.seek(SeekFrom::Start(off)).ok()?;
    let mut v = [0u8; 128];
    reader.read_exact(&mut v).ok()?;
    let dos = v[0];
    let total = u16::from_le_bytes([v[1], v[2]]);
    let free = u16::from_le_bytes([v[3], v[4]]);
    // DOS 2.0/2.5 use code 2; total tracks the available data sectors and
    // free can't exceed it. The SD total is 707; ED is larger.
    if dos == 2 && total >= 700 && total < geom.total_sectors && free <= total {
        Some(geom)
    } else {
        None
    }
}

/// An Atari DOS filesystem over a flat sector body.
pub struct AtariDosFilesystem<R: Read + Seek + Send> {
    reader: R,
    partition_offset: u64,
    geom: AtariGeometry,
    entries: Vec<AtariDirEntry>,
}

impl<R: Read + Seek + Send> AtariDosFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        let len = reader
            .seek(SeekFrom::End(0))?
            .checked_sub(partition_offset)
            .ok_or_else(|| FilesystemError::InvalidData("partition offset past EOF".into()))?;
        let geom = AtariGeometry::from_body_len(len).ok_or_else(|| {
            FilesystemError::InvalidData(format!("{len} bytes is not a known Atari DOS geometry"))
        })?;
        let mut fs = AtariDosFilesystem {
            reader,
            partition_offset,
            geom,
            entries: Vec::new(),
        };
        fs.refresh_entries()?;
        Ok(fs)
    }

    pub fn into_inner(self) -> R {
        self.reader
    }

    fn read_sector(&mut self, sector: u16) -> Result<Vec<u8>, FilesystemError> {
        if sector == 0 || sector > self.geom.total_sectors {
            return Err(FilesystemError::InvalidData(format!(
                "sector {sector} out of range 1..={}",
                self.geom.total_sectors
            )));
        }
        let off = self.partition_offset + self.geom.sector_offset(sector);
        self.reader.seek(SeekFrom::Start(off))?;
        let mut buf = vec![0u8; self.geom.sector_size];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Rebuild the in-memory directory (live + deleted entries kept; the
    /// `is_live` filter is applied by `list_directory`).
    fn refresh_entries(&mut self) -> Result<(), FilesystemError> {
        let mut entries = Vec::new();
        for s in 0..DIR_SECTORS {
            let buf = self.read_sector(DIR_FIRST_SECTOR + s)?;
            for i in 0..8 {
                let off = i * ENTRY_LEN;
                let e = &buf[off..off + ENTRY_LEN];
                let flag = e[0];
                let mut raw_name = [0u8; 11];
                raw_name.copy_from_slice(&e[5..16]);
                entries.push(AtariDirEntry {
                    flag,
                    sector_count: u16::from_le_bytes([e[1], e[2]]),
                    start_sector: u16::from_le_bytes([e[3], e[4]]),
                    raw_name,
                    name: decode_name(&raw_name),
                    slot: (s as usize) * 8 + i,
                });
            }
        }
        self.entries = entries;
        Ok(())
    }

    /// Walk a file's sector chain, honoring the per-sector byte count.
    fn read_chain(&mut self, entry: &AtariDirEntry) -> Result<Vec<u8>, FilesystemError> {
        let data_bytes = self.geom.data_bytes();
        let mut out = Vec::with_capacity(entry.sector_count as usize * data_bytes);
        let mut sec = entry.start_sector;
        let mut seen = HashSet::new();
        for _ in 0..MAX_FILE_SECTORS {
            if sec == 0 {
                break;
            }
            if !seen.insert(sec) {
                return Err(FilesystemError::InvalidData(format!(
                    "file '{}' has a sector-chain loop at {sec}",
                    entry.name
                )));
            }
            let buf = self.read_sector(sec)?;
            let b0 = buf[data_bytes];
            let b1 = buf[data_bytes + 1];
            let used = buf[data_bytes + 2] as usize;
            let next = (((b0 & 0x03) as u16) << 8) | b1 as u16;
            out.extend_from_slice(&buf[..used.min(data_bytes)]);
            sec = next;
        }
        Ok(out)
    }
}

impl<R: Read + Seek + Send> Filesystem for AtariDosFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if entry.path != "/" {
            return Ok(Vec::new());
        }
        let data_bytes = self.geom.data_bytes() as u64;
        let mut out = Vec::new();
        for (idx, e) in self.entries.iter().enumerate() {
            if !e.is_live() {
                continue;
            }
            // Approximate size from the sector count (exact size needs the
            // last sector's byte count; read_file returns the exact bytes).
            let approx = e.sector_count as u64 * data_bytes;
            let mut fe =
                FileEntry::new_file(e.name.clone(), format!("/{}", e.name), approx, idx as u64);
            if e.locked() {
                fe.special_type = Some("locked".into());
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
        let idx = entry.location as usize;
        let de = self
            .entries
            .get(idx)
            .ok_or_else(|| FilesystemError::NotFound(format!("entry idx {idx} out of range")))?
            .clone();
        if !de.is_live() {
            return Err(FilesystemError::NotFound(format!(
                "'{}' is deleted or unused",
                de.name
            )));
        }
        let mut data = self.read_chain(&de)?;
        data.truncate(max_bytes);
        Ok(data)
    }

    fn fs_type(&self) -> &str {
        "Atari DOS 2"
    }

    fn volume_label(&self) -> Option<&str> {
        // Atari DOS 2 has no volume label.
        None
    }

    fn total_size(&self) -> u64 {
        self.geom.total_sectors as u64 * self.geom.sector_size as u64
    }

    fn used_size(&self) -> u64 {
        self.entries
            .iter()
            .filter(|e| e.is_live())
            .map(|e| e.sector_count as u64 * self.geom.sector_size as u64)
            .sum()
    }
}

// ---------------------------------------------------------------------------
// VTOC / write side
// ---------------------------------------------------------------------------

impl<R: Read + Write + Seek + Send> AtariDosFilesystem<R> {
    fn write_sector(&mut self, sector: u16, data: &[u8]) -> Result<(), FilesystemError> {
        if sector == 0 || sector > self.geom.total_sectors {
            return Err(FilesystemError::InvalidData(format!(
                "write sector {sector} out of range"
            )));
        }
        if data.len() != self.geom.sector_size {
            return Err(FilesystemError::InvalidData(
                "sector write must be a full sector".into(),
            ));
        }
        let off = self.partition_offset + self.geom.sector_offset(sector);
        self.reader.seek(SeekFrom::Start(off))?;
        self.reader.write_all(data)?;
        Ok(())
    }

    fn read_vtoc(&mut self) -> Result<Vec<u8>, FilesystemError> {
        self.read_sector(VTOC_SECTOR)
    }

    /// Bit SET = free.
    fn bitmap_is_free(vtoc: &[u8], sector: u16) -> bool {
        let byte = BITMAP_OFFSET + (sector as usize) / 8;
        let bit = 7 - (sector as usize) % 8;
        byte < vtoc.len() && (vtoc[byte] >> bit) & 1 == 1
    }

    fn bitmap_set(vtoc: &mut [u8], sector: u16, free: bool) {
        let byte = BITMAP_OFFSET + (sector as usize) / 8;
        let bit = 7 - (sector as usize) % 8;
        if byte >= vtoc.len() {
            return;
        }
        if free {
            vtoc[byte] |= 1 << bit;
        } else {
            vtoc[byte] &= !(1 << bit);
        }
    }

    fn vtoc_free_count(vtoc: &[u8]) -> u16 {
        u16::from_le_bytes([vtoc[3], vtoc[4]])
    }

    fn vtoc_set_free_count(vtoc: &mut [u8], count: u16) {
        let b = count.to_le_bytes();
        vtoc[3] = b[0];
        vtoc[4] = b[1];
    }

    /// Sectors usable for file data: everything except the boot sectors
    /// (1-3), the VTOC (360), the directory (361-368), and the reserved
    /// last sector (720 on SD). Allocation walks them low-to-high.
    fn allocatable_sectors(&self) -> Vec<u16> {
        let last = self.geom.total_sectors; // reserved on SD/ED
        (4..self.geom.total_sectors)
            .filter(|&s| {
                s != VTOC_SECTOR
                    && !(DIR_FIRST_SECTOR..DIR_FIRST_SECTOR + DIR_SECTORS).contains(&s)
                    && s != last
            })
            .collect()
    }
}

impl<R: Read + Write + Seek + Send> EditableFilesystem for AtariDosFilesystem<R> {
    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn std::io::Read,
        data_len: u64,
        _options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        if parent.path != "/" {
            return Err(FilesystemError::Unsupported(
                "Atari DOS is flat; only the root accepts files".into(),
            ));
        }
        let raw_name = encode_name(name)?;
        if self
            .entries
            .iter()
            .any(|e| e.is_live() && e.raw_name == raw_name)
        {
            return Err(FilesystemError::AlreadyExists(format!(
                "Atari file '{name}' already exists"
            )));
        }

        let mut payload = Vec::with_capacity(data_len as usize);
        data.read_to_end(&mut payload)?;
        if payload.len() as u64 != data_len {
            return Err(FilesystemError::InvalidData(format!(
                "data_len {data_len} != actual {} bytes",
                payload.len()
            )));
        }

        let data_bytes = self.geom.data_bytes();
        let sectors_needed = payload.len().div_ceil(data_bytes).max(1);
        if sectors_needed > u16::MAX as usize {
            return Err(FilesystemError::InvalidData("file too large".into()));
        }

        // Find a free directory slot (reuse deleted/unused).
        let slot = self
            .entries
            .iter()
            .position(|e| e.flag == 0 || e.flag & FLAG_DELETED != 0)
            .ok_or_else(|| FilesystemError::DiskFull("directory is full (64 files)".into()))?;

        let mut vtoc = self.read_vtoc()?;
        if (Self::vtoc_free_count(&vtoc) as usize) < sectors_needed {
            return Err(FilesystemError::DiskFull(format!(
                "need {sectors_needed} sectors, {} free",
                Self::vtoc_free_count(&vtoc)
            )));
        }

        // Allocate the chain.
        let mut chain: Vec<u16> = Vec::with_capacity(sectors_needed);
        for &s in &self.allocatable_sectors() {
            if chain.len() == sectors_needed {
                break;
            }
            if Self::bitmap_is_free(&vtoc, s) {
                Self::bitmap_set(&mut vtoc, s, false);
                chain.push(s);
            }
        }
        if chain.len() != sectors_needed {
            return Err(FilesystemError::DiskFull(
                "not enough contiguous free sectors".into(),
            ));
        }

        // Write the data sectors with forward links + per-sector byte count.
        let file_no = slot as u8 & 0x3F;
        for (i, &sec) in chain.iter().enumerate() {
            let mut buf = vec![0u8; self.geom.sector_size];
            let off = i * data_bytes;
            let end = (off + data_bytes).min(payload.len());
            let chunk = if off < payload.len() {
                &payload[off..end]
            } else {
                &[][..]
            };
            buf[..chunk.len()].copy_from_slice(chunk);
            let next = if i + 1 < chain.len() { chain[i + 1] } else { 0 };
            buf[data_bytes] = (file_no << 2) | ((next >> 8) & 0x03) as u8;
            buf[data_bytes + 1] = (next & 0xFF) as u8;
            buf[data_bytes + 2] = chunk.len() as u8;
            self.write_sector(sec, &buf)?;
        }

        // Write the directory entry.
        let dir_sector = DIR_FIRST_SECTOR + (slot / 8) as u16;
        let mut dbuf = self.read_sector(dir_sector)?;
        let eoff = (slot % 8) * ENTRY_LEN;
        dbuf[eoff] = FLAG_IN_USE | FLAG_DOS2;
        let sc = (sectors_needed as u16).to_le_bytes();
        dbuf[eoff + 1] = sc[0];
        dbuf[eoff + 2] = sc[1];
        let st = chain[0].to_le_bytes();
        dbuf[eoff + 3] = st[0];
        dbuf[eoff + 4] = st[1];
        dbuf[eoff + 5..eoff + 16].copy_from_slice(&raw_name);
        self.write_sector(dir_sector, &dbuf)?;

        // Update VTOC free count + write it back.
        let new_free = Self::vtoc_free_count(&vtoc) - sectors_needed as u16;
        Self::vtoc_set_free_count(&mut vtoc, new_free);
        self.write_sector(VTOC_SECTOR, &vtoc)?;
        self.reader.flush()?;
        self.refresh_entries()?;

        let idx = self
            .entries
            .iter()
            .position(|e| e.is_live() && e.raw_name == raw_name)
            .ok_or_else(|| {
                FilesystemError::InvalidData("internal: created file not visible".into())
            })?;
        Ok(FileEntry::new_file(
            self.entries[idx].name.clone(),
            format!("/{}", self.entries[idx].name),
            payload.len() as u64,
            idx as u64,
        ))
    }

    fn create_directory(
        &mut self,
        _parent: &FileEntry,
        _name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "Atari DOS 2 is a flat filesystem — no subdirectories".into(),
        ))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        if parent.path != "/" {
            return Err(FilesystemError::Unsupported(
                "Atari DOS is flat; only root files are deletable".into(),
            ));
        }
        let idx = entry.location as usize;
        let de =
            self.entries.get(idx).cloned().ok_or_else(|| {
                FilesystemError::NotFound(format!("entry idx {idx} out of range"))
            })?;
        if !de.is_live() {
            return Err(FilesystemError::NotFound("already deleted".into()));
        }
        if de.locked() {
            return Err(FilesystemError::Unsupported(format!(
                "'{}' is locked",
                de.name
            )));
        }

        let mut vtoc = self.read_vtoc()?;
        // Free every sector in the chain.
        let data_bytes = self.geom.data_bytes();
        let mut sec = de.start_sector;
        let mut seen = HashSet::new();
        let mut freed = 0u16;
        for _ in 0..MAX_FILE_SECTORS {
            if sec == 0 {
                break;
            }
            if !seen.insert(sec) {
                return Err(FilesystemError::InvalidData(
                    "delete hit a sector-chain loop".into(),
                ));
            }
            let buf = self.read_sector(sec)?;
            let next = (((buf[data_bytes] & 0x03) as u16) << 8) | buf[data_bytes + 1] as u16;
            Self::bitmap_set(&mut vtoc, sec, true);
            freed += 1;
            sec = next;
        }

        // Mark the directory entry deleted (flag bit 0x80).
        let dir_sector = DIR_FIRST_SECTOR + (de.slot / 8) as u16;
        let mut dbuf = self.read_sector(dir_sector)?;
        let eoff = (de.slot % 8) * ENTRY_LEN;
        dbuf[eoff] = FLAG_DELETED;
        self.write_sector(dir_sector, &dbuf)?;

        let new_free = Self::vtoc_free_count(&vtoc) + freed;
        Self::vtoc_set_free_count(&mut vtoc, new_free);
        self.write_sector(VTOC_SECTOR, &vtoc)?;
        self.reader.flush()?;
        self.refresh_entries()?;
        Ok(())
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
        if parent.path != "/" {
            return Err(FilesystemError::Unsupported(
                "Atari DOS is flat; only root files are renamable".into(),
            ));
        }
        let raw_name = encode_name(new_name)?;

        let idx = entry.location as usize;
        let de =
            self.entries.get(idx).cloned().ok_or_else(|| {
                FilesystemError::NotFound(format!("entry idx {idx} out of range"))
            })?;
        if !de.is_live() {
            return Err(FilesystemError::NotFound("already deleted".into()));
        }
        if de.locked() {
            return Err(FilesystemError::Unsupported(format!(
                "'{}' is locked",
                de.name
            )));
        }

        // Reject a collision with a *different* live slot. Atari names are
        // folded to uppercase by the encoder, so byte-exact comparison of the
        // encoded form covers case folding; a case-only rename resolves back
        // to the same slot and is allowed.
        if self
            .entries
            .iter()
            .enumerate()
            .any(|(i, e)| i != idx && e.is_live() && e.raw_name == raw_name)
        {
            return Err(FilesystemError::AlreadyExists(new_name.to_string()));
        }

        // Overwrite only the 11-byte name field; flag, sector count and start
        // sector are untouched, so the file keeps its identity and contents.
        let dir_sector = DIR_FIRST_SECTOR + (de.slot / 8) as u16;
        let mut dbuf = self.read_sector(dir_sector)?;
        let eoff = (de.slot % 8) * ENTRY_LEN;
        dbuf[eoff + 5..eoff + 16].copy_from_slice(&raw_name);
        self.write_sector(dir_sector, &dbuf)?;

        self.reader.flush()?;
        self.refresh_entries()?;
        Ok(())
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.reader.flush()?;
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        let vtoc = self.read_vtoc()?;
        Ok(Self::vtoc_free_count(&vtoc) as u64 * self.geom.data_bytes() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a blank single-density DOS 2.0S image: boot/VTOC/directory
    /// reserved, all data sectors free, 707 free in the VTOC.
    pub fn create_blank_sd() -> Vec<u8> {
        let geom = AtariGeometry {
            sector_size: 128,
            total_sectors: 720,
        };
        let mut img = vec![0u8; geom.total_sectors as usize * geom.sector_size];
        let voff = geom.sector_offset(VTOC_SECTOR) as usize;
        // VTOC header.
        img[voff] = 2; // DOS 2
        let total = 707u16.to_le_bytes();
        img[voff + 1] = total[0];
        img[voff + 2] = total[1];
        img[voff + 3] = total[0];
        img[voff + 4] = total[1];
        // Bitmap: mark every sector free, then reserve the structural ones.
        for b in &mut img[voff + BITMAP_OFFSET..voff + BITMAP_OFFSET + 90] {
            *b = 0xFF;
        }
        let reserve = |img: &mut [u8], sector: u16| {
            let byte = voff + BITMAP_OFFSET + (sector as usize) / 8;
            let bit = 7 - (sector as usize) % 8;
            img[byte] &= !(1 << bit);
        };
        reserve(&mut img, 0); // sector 0 doesn't exist
        for s in 1..=3 {
            reserve(&mut img, s); // boot
        }
        reserve(&mut img, VTOC_SECTOR);
        for s in DIR_FIRST_SECTOR..DIR_FIRST_SECTOR + DIR_SECTORS {
            reserve(&mut img, s);
        }
        reserve(&mut img, 720); // reserved last sector
        img
    }

    fn open_mem(img: Vec<u8>) -> AtariDosFilesystem<Cursor<Vec<u8>>> {
        AtariDosFilesystem::open(Cursor::new(img), 0).expect("open")
    }

    #[test]
    fn name_encode_decode_round_trips() {
        let raw = encode_name("HELLO.TXT").unwrap();
        assert_eq!(&raw, b"HELLO   TXT");
        assert_eq!(decode_name(&raw), "HELLO.TXT");
        let raw2 = encode_name("game").unwrap();
        assert_eq!(decode_name(&raw2), "GAME");
        assert!(encode_name("9NOPE").is_err()); // must start with a letter
        assert!(encode_name("TOOLONGXX.Y").is_err());
    }

    #[test]
    fn blank_disk_has_no_files_and_707_free() {
        let mut fs = open_mem(create_blank_sd());
        assert_eq!(fs.fs_type(), "Atari DOS 2");
        let root = fs.root().unwrap();
        assert!(fs.list_directory(&root).unwrap().is_empty());
        // 707 free sectors * 125 data bytes.
        assert_eq!(fs.free_space().unwrap(), 707 * 125);
    }

    #[test]
    fn create_read_delete_round_trip() {
        let mut fs = open_mem(create_blank_sd());
        let root = fs.root().unwrap();

        let hello = b"HELLO ATARI\x9b".to_vec();
        fs.create_file(
            &root,
            "HELLO.TXT",
            &mut Cursor::new(hello.clone()),
            hello.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        // A multi-sector file (1000 bytes spans 8 sectors of 125).
        let big: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        fs.create_file(
            &root,
            "DATA.BIN",
            &mut Cursor::new(big.clone()),
            big.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();

        let listing = fs.list_directory(&root).unwrap();
        assert_eq!(listing.len(), 2);
        let h = listing.iter().find(|e| e.name == "HELLO.TXT").unwrap();
        assert_eq!(fs.read_file(h, usize::MAX).unwrap(), hello);
        let d = listing.iter().find(|e| e.name == "DATA.BIN").unwrap();
        assert_eq!(fs.read_file(d, usize::MAX).unwrap(), big);

        let free_before = fs.free_space().unwrap();
        fs.delete_entry(&root, &h.clone()).unwrap();
        let listing = fs.list_directory(&root).unwrap();
        assert_eq!(listing.len(), 1);
        assert_eq!(listing[0].name, "DATA.BIN");
        assert!(fs.free_space().unwrap() > free_before);

        // Re-open from bytes to prove persistence.
        let bytes = fs.into_inner().into_inner();
        let mut fs2 = AtariDosFilesystem::open(Cursor::new(bytes), 0).unwrap();
        let r2 = fs2.root().unwrap();
        let l2 = fs2.list_directory(&r2).unwrap();
        assert_eq!(l2.len(), 1);
        assert_eq!(fs2.read_file(&l2[0], usize::MAX).unwrap(), big);
    }

    #[test]
    fn rename_in_place_round_trip() {
        let mut fs = open_mem(create_blank_sd());
        let root = fs.root().unwrap();

        let hello = b"HELLO ATARI\x9b".to_vec();
        fs.create_file(
            &root,
            "HELLO.TXT",
            &mut Cursor::new(hello.clone()),
            hello.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();

        let listing = fs.list_directory(&root).unwrap();
        let h = listing
            .iter()
            .find(|e| e.name == "HELLO.TXT")
            .unwrap()
            .clone();
        let start = fs.entries[h.location as usize].start_sector;

        fs.rename(&root, &h, "GREET.TXT").unwrap();

        // Re-open from bytes to prove persistence.
        let bytes = fs.into_inner().into_inner();
        let mut fs2 = AtariDosFilesystem::open(Cursor::new(bytes), 0).unwrap();
        let r2 = fs2.root().unwrap();
        let l2 = fs2.list_directory(&r2).unwrap();
        let names: Vec<&str> = l2.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"GREET.TXT"));
        assert!(!names.contains(&"HELLO.TXT"));

        // Identity (start sector) + contents preserved.
        let renamed = l2.iter().find(|e| e.name == "GREET.TXT").unwrap();
        assert_eq!(fs2.entries[renamed.location as usize].start_sector, start);
        assert_eq!(fs2.read_file(renamed, usize::MAX).unwrap(), hello);
    }

    #[test]
    fn rename_collision_rejected() {
        let mut fs = open_mem(create_blank_sd());
        let root = fs.root().unwrap();
        fs.create_file(
            &root,
            "ONE",
            &mut Cursor::new(vec![1u8; 10]),
            10,
            &CreateFileOptions::default(),
        )
        .unwrap();
        fs.create_file(
            &root,
            "TWO",
            &mut Cursor::new(vec![2u8; 10]),
            10,
            &CreateFileOptions::default(),
        )
        .unwrap();
        let listing = fs.list_directory(&root).unwrap();
        let two = listing.iter().find(|e| e.name == "TWO").unwrap().clone();
        let err = fs.rename(&root, &two, "ONE").unwrap_err();
        assert!(matches!(err, FilesystemError::AlreadyExists(_)));
    }

    #[test]
    fn duplicate_name_rejected() {
        let mut fs = open_mem(create_blank_sd());
        let root = fs.root().unwrap();
        fs.create_file(
            &root,
            "ONE",
            &mut Cursor::new(vec![1u8; 10]),
            10,
            &CreateFileOptions::default(),
        )
        .unwrap();
        let err = fs
            .create_file(
                &root,
                "ONE",
                &mut Cursor::new(vec![2u8; 10]),
                10,
                &CreateFileOptions::default(),
            )
            .unwrap_err();
        assert!(matches!(err, FilesystemError::AlreadyExists(_)));
    }
}
