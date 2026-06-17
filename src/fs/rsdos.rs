//! RS-DOS / Color Computer Disk BASIC filesystem — the floppy filesystem
//! for the Tandy/Radio Shack TRS-80 Color Computer (CoCo 1/2/3) and the
//! MiSTer CoCo2/CoCo3 cores.
//!
//! The host OS can't mount these disks, so file extract + add + delete is
//! the whole point (Axis 1 of the MiSTer plan — floppy-only, no resize).
//!
//! ## Geometry
//!
//! Disk BASIC disks are flat sector images (a raw `.dsk`/`.jvc` dump whose
//! byte length is a multiple of 256 carries no container header). Tracks
//! are 18 sectors of 256 bytes; sectors are numbered from 1. Two
//! single-sided shapes are recognized:
//!
//! | Body bytes | Tracks | Granules |
//! |------------|--------|----------|
//! | 161280     | 35     | 68       |
//! | 184320     | 40     | 78       |
//!
//! ## On-disk layout (reference: toolshed `libdecb`)
//!
//! - **Track 17** is the directory track.
//!   - **Sector 2** holds the **granule allocation table (FAT)**: one byte
//!     per granule, indexed 0..granule_count.
//!   - **Sectors 3..11** hold the **directory**: 72 entries of 32 bytes
//!     (9 sectors x 8 entries).
//! - A **granule** is 9 sectors = 2304 bytes (half a track). Granule `g`
//!   lives at byte `g*2304`, plus `2*2304` once `g > 33` because the two
//!   granules of track 17 are reserved for the directory and skipped in the
//!   granule numbering.
//! - **FAT byte** semantics:
//!   - `0xFF` — granule is free.
//!   - `< 0xC0` — granule is in use; the value is the next granule in the
//!     file's chain.
//!   - `0xC0 | S` — last granule of the file; `S = value & 0x3F` is the
//!     number of sectors used in this granule (1..9).
//! - **Directory entry** (32 bytes):
//!   ```text
//!   0x00..0x07 filename (8 bytes, ASCII, space-padded). First byte:
//!              0x00 = deleted/free slot, 0xFF = end of directory.
//!   0x08..0x0A extension (3 bytes, space-padded)
//!   0x0B       file type: 0 BASIC, 1 BASIC data, 2 machine language,
//!              3 text-editor source
//!   0x0C       ASCII flag: 0x00 binary, 0xFF ASCII
//!   0x0D       first granule
//!   0x0E..0x0F bytes used in the last sector of the last granule (BE)
//!   0x10..0x1F unused
//!   ```
//! - **File size** = `full_granules*2304 + (S-1)*256 + last_sector_size`,
//!   where `S` is the last granule's sector count from its FAT byte.

use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::FileEntry;
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};

pub const SECTOR_SIZE: usize = 256;
pub const SECTORS_PER_TRACK: u64 = 18;
pub const GRANULE_SECTORS: u64 = 9;
pub const GRANULE_SIZE: usize = GRANULE_SECTORS as usize * SECTOR_SIZE; // 2304
pub const DIR_TRACK: u64 = 17;
pub const FAT_SECTOR: u64 = 2;
pub const DIR_FIRST_SECTOR: u64 = 3;
pub const DIR_SECTORS: u64 = 9;
pub const MAX_DIR_ENTRIES: usize = 72;
const ENTRY_LEN: usize = 32;

/// FAT byte: granule is free.
const GRAN_FREE: u8 = 0xFF;
/// FAT byte threshold: `>= 0xC0` marks the last granule of a chain.
const GRAN_LAST: u8 = 0xC0;

/// Runaway guard for a single file's granule chain.
const MAX_FILE_GRANULES: usize = 256;

/// Disk geometry, derived from the flat-body byte length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RsdosGeometry {
    pub tracks: u64,
    pub granules: u8,
}

impl RsdosGeometry {
    /// Recognize a flat RS-DOS disk body by exact size. Returns `None` for
    /// anything that isn't a known single-sided geometry.
    pub fn from_body_len(len: u64) -> Option<RsdosGeometry> {
        let bytes_per_track = SECTORS_PER_TRACK * SECTOR_SIZE as u64;
        if len == 0 || !len.is_multiple_of(bytes_per_track) {
            return None;
        }
        let tracks = len / bytes_per_track;
        match tracks {
            35 | 40 => Some(RsdosGeometry {
                tracks,
                granules: (tracks * 2 - 2) as u8,
            }),
            _ => None,
        }
    }

    /// Byte offset of a 1-based sector on `track` (0-based) within the body.
    fn sector_offset(self, track: u64, sector: u64) -> u64 {
        (track * SECTORS_PER_TRACK + (sector - 1)) * SECTOR_SIZE as u64
    }

    /// Byte offset of granule `g` within the body. Granules 34+ skip the two
    /// directory granules of track 17.
    fn granule_offset(self, g: u8) -> u64 {
        let mut off = g as u64 * GRANULE_SIZE as u64;
        if g > 33 {
            off += 2 * GRANULE_SIZE as u64;
        }
        off
    }

    fn fat_offset(self) -> u64 {
        self.sector_offset(DIR_TRACK, FAT_SECTOR)
    }

    fn dir_offset(self, slot: usize) -> u64 {
        self.sector_offset(DIR_TRACK, DIR_FIRST_SECTOR) + (slot * ENTRY_LEN) as u64
    }
}

/// One parsed directory slot.
#[derive(Debug, Clone)]
pub struct RsdosDirEntry {
    /// Slot index 0..72.
    pub slot: usize,
    /// First filename byte: 0x00 deleted/free, 0xFF end-of-directory, else live.
    pub flag0: u8,
    /// Raw 11-byte 8.3 name (8 name + 3 ext).
    pub raw_name: [u8; 11],
    /// Decoded display name ("NAME.EXT").
    pub name: String,
    pub file_type: u8,
    pub ascii_flag: u8,
    pub first_granule: u8,
    pub last_sector_size: u16,
}

impl RsdosDirEntry {
    fn is_live(&self) -> bool {
        self.flag0 != 0x00 && self.flag0 != 0xFF
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

/// Encode "NAME.EXT" into the 11-byte space-padded on-disk form. Disk BASIC
/// names are uppercase ASCII, up to 8 chars plus a 1-3 char extension.
pub fn encode_name(name: &str) -> Result<[u8; 11], FilesystemError> {
    let (base, ext) = match name.split_once('.') {
        Some((b, e)) => (b, e),
        None => (name, ""),
    };
    if base.is_empty() || base.len() > 8 || ext.len() > 3 {
        return Err(FilesystemError::InvalidData(format!(
            "RS-DOS filename '{name}' must be 1-8 chars + optional 1-3 char extension"
        )));
    }
    let mut out = [b' '; 11];
    let put = |slice: &str, dst: &mut [u8]| -> Result<(), FilesystemError> {
        for (i, c) in slice.chars().enumerate() {
            if !c.is_ascii() {
                return Err(FilesystemError::InvalidData(format!(
                    "RS-DOS filename '{name}' contains non-ASCII character '{c}'"
                )));
            }
            let b = (c as u8).to_ascii_uppercase();
            if !b.is_ascii_uppercase() && !b.is_ascii_digit() {
                return Err(FilesystemError::InvalidData(format!(
                    "RS-DOS filename '{name}' char '{c}' must be A-Z or 0-9"
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
            "RS-DOS filename '{name}' must start with a letter"
        )));
    }
    Ok(out)
}

/// True when FAT byte `v` is a legal granule entry for a disk with
/// `granules` usable granules: free, a chain pointer in range, or a
/// last-granule marker.
fn fat_byte_valid(v: u8, granules: u8) -> bool {
    v == GRAN_FREE || (v < granules) || (0xC0..=0xC9).contains(&v)
}

/// Sniff whether the flat body at `partition_offset` is an RS-DOS volume:
/// exact geometry size AND a structurally consistent granule table +
/// directory. The format has no magic number, so the structural checks do
/// all the discriminating (an OS-9 or random same-sized blob is rejected).
pub fn looks_like_rsdos<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
) -> Option<RsdosGeometry> {
    let len = reader
        .seek(SeekFrom::End(0))
        .ok()?
        .checked_sub(partition_offset)?;
    let geom = RsdosGeometry::from_body_len(len)?;

    // Read the FAT.
    reader
        .seek(SeekFrom::Start(partition_offset + geom.fat_offset()))
        .ok()?;
    let mut fat = [0u8; SECTOR_SIZE];
    reader.read_exact(&mut fat).ok()?;
    if !(0..geom.granules).all(|g| fat_byte_valid(fat[g as usize], geom.granules)) {
        return None;
    }

    // Read the directory track sectors 3..11.
    reader
        .seek(SeekFrom::Start(
            partition_offset + geom.sector_offset(DIR_TRACK, DIR_FIRST_SECTOR),
        ))
        .ok()?;
    let mut dir = vec![0u8; DIR_SECTORS as usize * SECTOR_SIZE];
    reader.read_exact(&mut dir).ok()?;

    let mut live = 0usize;
    let mut blank_fat = (0..geom.granules).all(|g| fat[g as usize] == GRAN_FREE);
    for slot in 0..MAX_DIR_ENTRIES {
        let e = &dir[slot * ENTRY_LEN..slot * ENTRY_LEN + ENTRY_LEN];
        if e[0] == 0xFF {
            break; // end of directory
        }
        if e[0] == 0x00 {
            continue; // deleted/free slot
        }
        // Live entry: validate the name and the granule chain.
        if !e[0..11].iter().all(|&b| (0x20..0x7F).contains(&b)) {
            return None;
        }
        let first = e[13];
        if first >= geom.granules || fat[first as usize] == GRAN_FREE {
            return None;
        }
        // Walk the chain; it must stay in range, not loop, and terminate.
        let mut g = first;
        let mut seen = HashSet::new();
        let mut ok = false;
        for _ in 0..MAX_FILE_GRANULES {
            if !seen.insert(g) {
                return None;
            }
            let v = fat[g as usize];
            if v >= GRAN_LAST {
                ok = true;
                break;
            }
            if v >= geom.granules {
                return None;
            }
            g = v;
        }
        if !ok {
            return None;
        }
        live += 1;
        blank_fat = false;
    }

    // Accept a populated disk (>=1 valid live entry) or a freshly formatted
    // blank one (DSKINI fills the disk with 0xFF: empty directory + free FAT).
    if live > 0 || (blank_fat && dir[0] == 0xFF) {
        Some(geom)
    } else {
        None
    }
}

/// An RS-DOS / Disk BASIC filesystem over a flat sector body.
pub struct RsdosFilesystem<R: Read + Seek + Send> {
    reader: R,
    partition_offset: u64,
    geom: RsdosGeometry,
    fat: [u8; SECTOR_SIZE],
    entries: Vec<RsdosDirEntry>,
}

impl<R: Read + Seek + Send> RsdosFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        let len = reader
            .seek(SeekFrom::End(0))?
            .checked_sub(partition_offset)
            .ok_or_else(|| FilesystemError::InvalidData("partition offset past EOF".into()))?;
        let geom = RsdosGeometry::from_body_len(len).ok_or_else(|| {
            FilesystemError::InvalidData(format!("{len} bytes is not a known RS-DOS geometry"))
        })?;
        let mut fs = RsdosFilesystem {
            reader,
            partition_offset,
            geom,
            fat: [0u8; SECTOR_SIZE],
            entries: Vec::new(),
        };
        fs.refresh()?;
        Ok(fs)
    }

    pub fn into_inner(self) -> R {
        self.reader
    }

    fn read_at(&mut self, off: u64, len: usize) -> Result<Vec<u8>, FilesystemError> {
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + off))?;
        let mut buf = vec![0u8; len];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Reload the FAT and directory from disk.
    fn refresh(&mut self) -> Result<(), FilesystemError> {
        let fat = self.read_at(self.geom.fat_offset(), SECTOR_SIZE)?;
        self.fat.copy_from_slice(&fat);

        let dir = self.read_at(
            self.geom.sector_offset(DIR_TRACK, DIR_FIRST_SECTOR),
            DIR_SECTORS as usize * SECTOR_SIZE,
        )?;
        let mut entries = Vec::new();
        for slot in 0..MAX_DIR_ENTRIES {
            let e = &dir[slot * ENTRY_LEN..slot * ENTRY_LEN + ENTRY_LEN];
            let mut raw_name = [0u8; 11];
            raw_name.copy_from_slice(&e[0..11]);
            entries.push(RsdosDirEntry {
                slot,
                flag0: e[0],
                raw_name,
                name: decode_name(&raw_name),
                file_type: e[11],
                ascii_flag: e[12],
                first_granule: e[13],
                last_sector_size: u16::from_be_bytes([e[14], e[15]]),
            });
        }
        self.entries = entries;
        Ok(())
    }

    /// Walk a file's granule chain. Returns `(chain, exact_size)`.
    fn chain_of(&self, entry: &RsdosDirEntry) -> Result<(Vec<u8>, u64), FilesystemError> {
        let mut chain = Vec::new();
        let mut size: u64 = 0;
        let mut g = entry.first_granule;
        let mut seen = HashSet::new();
        for _ in 0..MAX_FILE_GRANULES {
            if g >= self.geom.granules {
                return Err(FilesystemError::InvalidData(format!(
                    "file '{}' references granule {g} out of range",
                    entry.name
                )));
            }
            if !seen.insert(g) {
                return Err(FilesystemError::InvalidData(format!(
                    "file '{}' has a granule-chain loop at {g}",
                    entry.name
                )));
            }
            chain.push(g);
            let v = self.fat[g as usize];
            if v >= GRAN_LAST {
                let sectors = (v & 0x3F).saturating_sub(1) as u64;
                size += sectors * SECTOR_SIZE as u64 + entry.last_sector_size as u64;
                return Ok((chain, size));
            }
            size += GRANULE_SIZE as u64;
            g = v;
        }
        Err(FilesystemError::InvalidData(format!(
            "file '{}' chain exceeds {MAX_FILE_GRANULES} granules",
            entry.name
        )))
    }

    fn read_chain_bytes(&mut self, entry: &RsdosDirEntry) -> Result<Vec<u8>, FilesystemError> {
        let (chain, size) = self.chain_of(entry)?;
        let mut out = Vec::with_capacity(size as usize);
        let mut remaining = size as usize;
        for g in chain {
            let gb = self.read_at(self.geom.granule_offset(g), GRANULE_SIZE)?;
            let take = remaining.min(GRANULE_SIZE);
            out.extend_from_slice(&gb[..take]);
            remaining -= take;
        }
        Ok(out)
    }
}

impl<R: Read + Seek + Send> Filesystem for RsdosFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if entry.path != "/" {
            return Ok(Vec::new());
        }
        let live: Vec<RsdosDirEntry> = self
            .entries
            .iter()
            .filter(|e| e.is_live())
            .cloned()
            .collect();
        let mut out = Vec::new();
        for e in &live {
            let size = self.chain_of(e).map(|(_, s)| s).unwrap_or(0);
            let mut fe =
                FileEntry::new_file(e.name.clone(), format!("/{}", e.name), size, e.slot as u64);
            if e.ascii_flag == 0xFF {
                fe.special_type = Some("ascii".into());
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
        let slot = entry.location as usize;
        let de = self
            .entries
            .get(slot)
            .ok_or_else(|| FilesystemError::NotFound(format!("slot {slot} out of range")))?
            .clone();
        if !de.is_live() {
            return Err(FilesystemError::NotFound(format!(
                "'{}' is deleted or unused",
                de.name
            )));
        }
        let mut data = self.read_chain_bytes(&de)?;
        data.truncate(max_bytes);
        Ok(data)
    }

    fn fs_type(&self) -> &str {
        "RS-DOS"
    }

    fn volume_label(&self) -> Option<&str> {
        None
    }

    fn total_size(&self) -> u64 {
        self.geom.tracks * SECTORS_PER_TRACK * SECTOR_SIZE as u64
    }

    fn used_size(&self) -> u64 {
        (0..self.geom.granules)
            .filter(|&g| self.fat[g as usize] != GRAN_FREE)
            .count() as u64
            * GRANULE_SIZE as u64
    }
}

// ---------------------------------------------------------------------------
// Write side
// ---------------------------------------------------------------------------

impl<R: Read + Write + Seek + Send> RsdosFilesystem<R> {
    fn write_at(&mut self, off: u64, data: &[u8]) -> Result<(), FilesystemError> {
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + off))?;
        self.reader.write_all(data)?;
        Ok(())
    }

    fn free_granules(&self) -> Vec<u8> {
        (0..self.geom.granules)
            .filter(|&g| self.fat[g as usize] == GRAN_FREE)
            .collect()
    }
}

impl<R: Read + Write + Seek + Send> EditableFilesystem for RsdosFilesystem<R> {
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
                "RS-DOS is flat; only the root accepts files".into(),
            ));
        }
        let raw_name = encode_name(name)?;
        if self
            .entries
            .iter()
            .any(|e| e.is_live() && e.raw_name == raw_name)
        {
            return Err(FilesystemError::AlreadyExists(format!(
                "RS-DOS file '{name}' already exists"
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

        // Granules needed (always at least one, even for an empty file).
        let need = payload.len().div_ceil(GRANULE_SIZE).max(1);
        let last_bytes = payload.len() - (need - 1) * GRANULE_SIZE;
        // Sectors used in the last granule + bytes in its final sector.
        let (last_sectors, last_sector_size): (u8, u16) = if payload.is_empty() {
            (1, 0)
        } else {
            let s = last_bytes.div_ceil(SECTOR_SIZE).max(1) as u8;
            let lss = (last_bytes - (s as usize - 1) * SECTOR_SIZE) as u16; // 1..=256
            (s, lss)
        };

        // Find a free directory slot (reuse deleted, else the end terminator).
        let slot = self
            .entries
            .iter()
            .position(|e| e.flag0 == 0x00 || e.flag0 == 0xFF)
            .ok_or_else(|| FilesystemError::DiskFull("directory is full (72 files)".into()))?;

        // Allocate the granule chain.
        let free = self.free_granules();
        if free.len() < need {
            return Err(FilesystemError::DiskFull(format!(
                "need {need} granules, {} free",
                free.len()
            )));
        }
        let chain = &free[..need];
        for (i, &g) in chain.iter().enumerate() {
            self.fat[g as usize] = if i + 1 < need {
                chain[i + 1]
            } else {
                GRAN_LAST | last_sectors
            };
        }

        // Write the data granules (zero-padded to a full granule).
        for (i, &g) in chain.iter().enumerate() {
            let mut buf = vec![0u8; GRANULE_SIZE];
            let off = i * GRANULE_SIZE;
            let end = (off + GRANULE_SIZE).min(payload.len());
            if off < payload.len() {
                let chunk = &payload[off..end];
                buf[..chunk.len()].copy_from_slice(chunk);
            }
            self.write_at(self.geom.granule_offset(g), &buf)?;
        }

        // Write the directory entry.
        let mut entry = [0u8; ENTRY_LEN];
        entry[0..11].copy_from_slice(&raw_name);
        entry[11] = 2; // machine-language / generic binary
        entry[12] = 0x00; // binary
        entry[13] = chain[0];
        entry[14..16].copy_from_slice(&last_sector_size.to_be_bytes());
        self.write_at(self.geom.dir_offset(slot), &entry)?;

        // Persist the FAT.
        let fat = self.fat;
        self.write_at(self.geom.fat_offset(), &fat)?;
        self.reader.flush()?;
        self.refresh()?;

        Ok(FileEntry::new_file(
            decode_name(&raw_name),
            format!("/{}", decode_name(&raw_name)),
            payload.len() as u64,
            slot as u64,
        ))
    }

    fn create_directory(
        &mut self,
        _parent: &FileEntry,
        _name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "RS-DOS / Disk BASIC is a flat filesystem — no subdirectories".into(),
        ))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        if parent.path != "/" {
            return Err(FilesystemError::Unsupported(
                "RS-DOS is flat; only root files are deletable".into(),
            ));
        }
        let slot = entry.location as usize;
        let de = self
            .entries
            .get(slot)
            .cloned()
            .ok_or_else(|| FilesystemError::NotFound(format!("slot {slot} out of range")))?;
        if !de.is_live() {
            return Err(FilesystemError::NotFound("already deleted".into()));
        }

        // Free every granule in the chain.
        let mut g = de.first_granule;
        let mut seen = HashSet::new();
        for _ in 0..MAX_FILE_GRANULES {
            if g >= self.geom.granules || !seen.insert(g) {
                break;
            }
            let v = self.fat[g as usize];
            self.fat[g as usize] = GRAN_FREE;
            if v >= GRAN_LAST {
                break;
            }
            g = v;
        }

        // Mark the directory slot deleted (first filename byte = 0x00).
        let mut ebuf = self.read_at(self.geom.dir_offset(slot), ENTRY_LEN)?;
        ebuf[0] = 0x00;
        self.write_at(self.geom.dir_offset(slot), &ebuf)?;

        let fat = self.fat;
        self.write_at(self.geom.fat_offset(), &fat)?;
        self.reader.flush()?;
        self.refresh()?;
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
                "RS-DOS is flat; only root files are renamable".into(),
            ));
        }
        let slot = entry.location as usize;
        let de = self
            .entries
            .get(slot)
            .cloned()
            .ok_or_else(|| FilesystemError::NotFound(format!("slot {slot} out of range")))?;
        if !de.is_live() {
            return Err(FilesystemError::NotFound("already deleted".into()));
        }
        let raw_name = encode_name(new_name)?;

        // Reject a collision with a *different* live slot. Names are folded to
        // uppercase by the encoder, so byte-exact comparison covers case
        // folding; the encoded first byte is always a valid letter/digit
        // (never 0x00/0xFF), so the slot stays a live header.
        if self
            .entries
            .iter()
            .any(|e| e.slot != slot && e.is_live() && e.raw_name == raw_name)
        {
            return Err(FilesystemError::AlreadyExists(new_name.to_string()));
        }

        // Overwrite only the 11-byte name field; file type, ASCII flag, first
        // granule and last-sector size (bytes 0x0B..0x10) are preserved, so the
        // file keeps its identity and contents.
        let mut ebuf = self.read_at(self.geom.dir_offset(slot), ENTRY_LEN)?;
        ebuf[0..11].copy_from_slice(&raw_name);
        self.write_at(self.geom.dir_offset(slot), &ebuf)?;
        self.reader.flush()?;
        self.refresh()?;
        Ok(())
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.reader.flush()?;
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.free_granules().len() as u64 * GRANULE_SIZE as u64)
    }
}

/// Build a blank, DSKINI-style RS-DOS disk: the whole image is 0xFF (free
/// FAT, empty directory). Used by tests.
pub fn create_blank(tracks: u64) -> Vec<u8> {
    vec![0xFFu8; (tracks * SECTORS_PER_TRACK * SECTOR_SIZE as u64) as usize]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn open_mem(img: Vec<u8>) -> RsdosFilesystem<Cursor<Vec<u8>>> {
        RsdosFilesystem::open(Cursor::new(img), 0).expect("open")
    }

    #[test]
    fn name_encode_decode_round_trips() {
        let raw = encode_name("HELLO.BIN").unwrap();
        assert_eq!(&raw, b"HELLO   BIN");
        assert_eq!(decode_name(&raw), "HELLO.BIN");
        assert_eq!(decode_name(&encode_name("game").unwrap()), "GAME");
        assert!(encode_name("9NOPE").is_err());
        assert!(encode_name("TOOLONGXX.Y").is_err());
    }

    #[test]
    fn blank_disk_detects_and_is_empty() {
        let img = create_blank(35);
        let mut cur = Cursor::new(img.clone());
        assert!(looks_like_rsdos(&mut cur, 0).is_some());
        let mut fs = open_mem(img);
        assert_eq!(fs.fs_type(), "RS-DOS");
        let root = fs.root().unwrap();
        assert!(fs.list_directory(&root).unwrap().is_empty());
        assert_eq!(fs.free_space().unwrap(), 68 * GRANULE_SIZE as u64);
    }

    #[test]
    fn create_read_delete_round_trip() {
        let mut fs = open_mem(create_blank(35));
        let root = fs.root().unwrap();

        // Multi-granule binary (2560 bytes -> 2 granules).
        let big: Vec<u8> = (0..2560).map(|i| (i % 256) as u8).collect();
        fs.create_file(
            &root,
            "DATA.BIN",
            &mut Cursor::new(big.clone()),
            big.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        // Tiny file + an empty file.
        let hello = b"HELLO COCO\r".to_vec();
        fs.create_file(
            &root,
            "HELLO.TXT",
            &mut Cursor::new(hello.clone()),
            hello.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        fs.create_file(
            &root,
            "EMPTY",
            &mut Cursor::new(Vec::new()),
            0,
            &CreateFileOptions::default(),
        )
        .unwrap();

        let listing = fs.list_directory(&root).unwrap();
        assert_eq!(listing.len(), 3);
        let d = listing.iter().find(|e| e.name == "DATA.BIN").unwrap();
        assert_eq!(d.size, 2560);
        assert_eq!(fs.read_file(d, usize::MAX).unwrap(), big);
        let h = listing.iter().find(|e| e.name == "HELLO.TXT").unwrap();
        assert_eq!(fs.read_file(h, usize::MAX).unwrap(), hello);
        let e = listing.iter().find(|e| e.name == "EMPTY").unwrap();
        assert_eq!(e.size, 0);
        assert_eq!(fs.read_file(e, usize::MAX).unwrap(), Vec::<u8>::new());

        let free_before = fs.free_space().unwrap();
        fs.delete_entry(&root, &h.clone()).unwrap();
        let listing = fs.list_directory(&root).unwrap();
        assert_eq!(listing.len(), 2);
        assert!(fs.free_space().unwrap() > free_before);

        // Re-open from bytes to prove persistence.
        let bytes = fs.into_inner().into_inner();
        let mut fs2 = RsdosFilesystem::open(Cursor::new(bytes), 0).unwrap();
        let r2 = fs2.root().unwrap();
        let l2 = fs2.list_directory(&r2).unwrap();
        assert_eq!(l2.len(), 2);
        let d2 = l2.iter().find(|e| e.name == "DATA.BIN").unwrap();
        assert_eq!(fs2.read_file(d2, usize::MAX).unwrap(), big);
    }

    #[test]
    fn duplicate_name_rejected() {
        let mut fs = open_mem(create_blank(35));
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

    #[test]
    fn rename_in_place_round_trip() {
        let mut fs = open_mem(create_blank(35));
        let root = fs.root().unwrap();

        let big: Vec<u8> = (0..2560).map(|i| (i % 256) as u8).collect();
        fs.create_file(
            &root,
            "DATA.BIN",
            &mut Cursor::new(big.clone()),
            big.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        let d = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "DATA.BIN")
            .unwrap();
        let first_gran = fs.entries[d.location as usize].first_granule;

        fs.rename(&root, &d, "REPORT.TXT").unwrap();

        // Re-open from bytes to prove persistence.
        let bytes = fs.into_inner().into_inner();
        let mut fs2 = RsdosFilesystem::open(Cursor::new(bytes), 0).unwrap();
        let r2 = fs2.root().unwrap();
        let l2 = fs2.list_directory(&r2).unwrap();
        let names: Vec<&str> = l2.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"REPORT.TXT"));
        assert!(!names.contains(&"DATA.BIN"));

        // Identity (first granule) + contents preserved.
        let renamed = l2.iter().find(|e| e.name == "REPORT.TXT").unwrap();
        assert_eq!(
            fs2.entries[renamed.location as usize].first_granule,
            first_gran
        );
        assert_eq!(fs2.read_file(renamed, usize::MAX).unwrap(), big);
    }

    #[test]
    fn rename_collision_rejected() {
        let mut fs = open_mem(create_blank(35));
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
        let two = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "TWO")
            .unwrap();
        let err = fs.rename(&root, &two, "ONE").unwrap_err();
        assert!(matches!(err, FilesystemError::AlreadyExists(_)));
    }
}
