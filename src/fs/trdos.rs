//! TR-DOS (ZX Spectrum Beta Disk) filesystem — read driver.
//!
//! The native disk filesystem of the ZX Spectrum's Beta Disk interface, stored
//! in the ubiquitous `.trd` raw image. Like UCSD p-System it is deliberately
//! tiny: a single **flat** directory (no subdirectories) of at most 128 files,
//! each stored as a **contiguous** run of 256-byte sectors. There is no
//! allocation bitmap — files are packed from the first data sector in catalogue
//! order and a first-free "high-water mark" advances on save and never retreats
//! on erase (space is reclaimed only by TR-DOS's MOVE/compress command).
//!
//! Layout:
//!
//! ```text
//!   track 0, sectors 0..7   catalogue (8 sectors = 128 x 16-byte entries)
//!   track 0, sector 8        disk-info sector (offset 0x800)
//!   track 1, sector 0 ..     file data
//! ```
//!
//! Geometry comes from the disk-type byte (offset 0xE3 of the disk-info sector):
//! 0x16 = 80-track double-sided (640 KiB), 0x17 = 40-track DS, 0x18 = 80-track
//! single-sided, 0x19 = 40-track SS — all 16 sectors/track, 256 bytes/sector.
//!
//! **Addressing.** File entries carry a *logical* track number, and a `.trd`
//! stores its sectors in logical order, so `byte_offset = (track*16 + sector) *
//! 256` — a flat sector array with no head/cylinder interleaving to reason about
//! (the single easiest place to introduce a bug on a double-sided disk).
//!
//! All offsets below were validated byte-for-byte against a clean-room oracle
//! (`scripts/trdos-oracle.py`) built from the same spec.
//!
//! This is the read half of the TR-DOS quartet (Browse); edit / create / fsck
//! build on it (`docs/filesystem_completion_plan.md` Part 2).

use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::FileEntry;
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};
use super::fsck::{FsckIssue, FsckResult, FsckStats, RepairReport};

/// Bytes per sector.
pub(crate) const SECTOR: u64 = 256;
/// Sectors per (logical) track.
pub(crate) const SPT: u64 = 16;
/// The catalogue occupies track 0, sectors 0..7.
const CATALOG_SECTORS: u64 = 8;
/// The disk-info sector is track 0, sector 8 (byte offset 0x800).
pub(crate) const INFO_SECTOR: u64 = 8;
/// First data sector: track 1, sector 0 = linear sector 16.
pub(crate) const DATA_START: u64 = SPT;
/// Directory-entry stride.
const ENTRY: usize = 16;
/// Catalogue capacity.
pub(crate) const MAX_FILES: usize = 128;
/// Byte offset of the disk-info sector.
pub(crate) const INFO_OFF: u64 = INFO_SECTOR * SECTOR;
/// TR-DOS identifier byte (disk-info offset 0xE7).
pub(crate) const TRDOS_ID: u8 = 0x10;

/// A parsed disk geometry, derived from the disk-type byte.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrdosGeometry {
    pub disk_type: u8,
    pub logical_tracks: u16,
}

impl TrdosGeometry {
    /// Map a disk-type byte to its logical-track count, or `None` if invalid.
    pub fn from_type(disk_type: u8) -> Option<Self> {
        let logical_tracks = match disk_type {
            0x16 => 160, // 80 track, double sided
            0x17 => 80,  // 40 track, double sided
            0x18 => 80,  // 80 track, single sided
            0x19 => 40,  // 40 track, single sided
            _ => return None,
        };
        Some(TrdosGeometry {
            disk_type,
            logical_tracks,
        })
    }

    pub fn total_sectors(&self) -> u64 {
        self.logical_tracks as u64 * SPT
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_sectors() * SECTOR
    }

    /// Human-readable geometry (for logs / fsck stats).
    pub fn describe(&self) -> &'static str {
        match self.disk_type {
            0x16 => "80 track, double sided",
            0x17 => "40 track, double sided",
            0x18 => "80 track, single sided",
            0x19 => "40 track, single sided",
            _ => "unknown",
        }
    }
}

/// A parsed catalogue entry (16 bytes on disk).
#[derive(Clone, Debug)]
pub struct TrdosEntry {
    /// Slot index in the catalogue (0..127).
    pub slot: usize,
    /// The raw 8-byte name (space-padded), byte 0 holding the original name
    /// character (a live entry — deleted tombstones are filtered out).
    pub name: [u8; 8],
    /// Single-character file type (`B` BASIC, `C` CODE, `D` data array,
    /// `#` sequential, etc.).
    pub type_char: u8,
    /// Type-dependent parameter (CODE load address / BASIC autostart, LE).
    pub start_param: u16,
    /// Logical length in bytes.
    pub length_bytes: u16,
    /// Allocated length in sectors.
    pub length_sectors: u8,
    /// Starting sector within the starting track (0..15).
    pub start_sector: u8,
    /// Starting logical track.
    pub start_track: u8,
}

impl TrdosEntry {
    /// Linear sector index of the file's first sector.
    pub fn start_linear(&self) -> u64 {
        self.start_track as u64 * SPT + self.start_sector as u64
    }

    /// Byte size for display / extraction: the logical byte length when it fits
    /// the allocation, else the full sector allocation (robust on a foreign or
    /// slightly-inconsistent disk).
    pub fn byte_size(&self) -> u64 {
        let alloc = self.length_sectors as u64 * SECTOR;
        let lb = self.length_bytes as u64;
        if (1..=alloc).contains(&lb) {
            lb
        } else {
            alloc
        }
    }

    /// Display name: the trimmed 8-byte name plus a `.T` type suffix, so two
    /// entries sharing a name but differing in type (`boot.B` vs `boot.C`)
    /// stay distinct.
    pub fn display_name(&self) -> String {
        format!("{}.{}", trim_name(&self.name), printable(self.type_char))
    }
}

/// Trim trailing spaces from an 8-byte name and render printable ASCII,
/// replacing control bytes with `?` for display.
fn trim_name(name: &[u8]) -> String {
    let end = name.iter().rposition(|&b| b != b' ').map_or(0, |i| i + 1);
    name[..end].iter().map(|&b| printable(b)).collect()
}

fn printable(b: u8) -> char {
    if (0x20..0x7F).contains(&b) {
        b as char
    } else {
        '?'
    }
}

pub struct TrdosFilesystem<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    geometry: TrdosGeometry,
    volume_label: String,
    /// Free sectors from the disk-info sector.
    free_sectors: u16,
    /// Number of catalogue slots in use (live + tombstoned), from disk-info.
    num_files: u16,
    /// Number of deleted (tombstoned) slots, from disk-info (0xF4).
    deleted_count: u8,
    /// Live (non-deleted) entries, in catalogue order.
    entries: Vec<TrdosEntry>,
    /// First-free linear sector — the high-water mark where the next file is
    /// appended. Taken as the max of the disk-info pointer and every entry's
    /// packed end, so an understated free-count can neither hide a file from
    /// backup trimming nor let a create overwrite live data. Advances on
    /// `create_file`; never retreats on delete (TR-DOS reclaims only via MOVE).
    first_free_linear: u64,
}

/// Parse a 16-byte catalogue entry at `off` into a live entry, or `None` when
/// the slot is empty (byte 0 == 0x00, end of catalogue) or a deleted tombstone
/// (byte 0 == 0x01).
fn parse_entry(cat: &[u8], slot: usize) -> Option<TrdosEntry> {
    let off = slot * ENTRY;
    let raw = &cat[off..off + ENTRY];
    if raw[0] == 0x00 || raw[0] == 0x01 {
        return None;
    }
    let mut name = [0u8; 8];
    name.copy_from_slice(&raw[0..8]);
    Some(TrdosEntry {
        slot,
        name,
        type_char: raw[8],
        start_param: u16::from_le_bytes([raw[9], raw[10]]),
        length_bytes: u16::from_le_bytes([raw[11], raw[12]]),
        length_sectors: raw[13],
        start_sector: raw[14],
        start_track: raw[15],
    })
}

/// True when the byte at `off` is 0x00 (end of catalogue).
fn is_end_of_catalog(cat: &[u8], slot: usize) -> bool {
    cat[slot * ENTRY] == 0x00
}

/// Structural detector: read the disk-info sector and confirm the TR-DOS
/// signature (id byte 0x10, a valid disk-type byte) plus a size/geometry sanity
/// check. Returns the parsed geometry on a match. Gated tightly so a same-sized
/// blob from another format doesn't false-positive.
pub fn looks_like_trdos<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
) -> Option<TrdosGeometry> {
    let len = reader
        .seek(SeekFrom::End(0))
        .ok()?
        .checked_sub(partition_offset)?;
    // Must be sector-aligned and at least large enough to hold catalogue+info.
    if len < INFO_OFF + SECTOR || len % SECTOR != 0 {
        return None;
    }
    reader
        .seek(SeekFrom::Start(partition_offset + INFO_OFF))
        .ok()?;
    let mut info = [0u8; SECTOR as usize];
    reader.read_exact(&mut info).ok()?;

    // The TR-DOS id byte and a valid disk-type byte are the signature.
    if info[0xE7] != TRDOS_ID {
        return None;
    }
    let geom = TrdosGeometry::from_type(info[0xE3])?;
    // The image must fit within the declared geometry (a truncated `.trd` that
    // drops trailing free sectors is fine; an oversized blob is not us).
    if len > geom.total_bytes() {
        return None;
    }
    // Counters must be in range (file count is a single byte at 0xE4).
    if info[0xE4] as usize > MAX_FILES {
        return None;
    }
    let free = u16::from_le_bytes([info[0xE5], info[0xE6]]);
    if free as u64 > geom.total_sectors() {
        return None;
    }
    // First-free pointer must land inside the volume.
    let ff_linear = info[0xE2] as u64 * SPT + info[0xE1] as u64;
    if ff_linear > geom.total_sectors() {
        return None;
    }
    Some(geom)
}

impl<R: Read + Seek> TrdosFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        let geometry = looks_like_trdos(&mut reader, partition_offset)
            .ok_or_else(|| FilesystemError::InvalidData("not a TR-DOS volume".into()))?;

        // Read the catalogue (8 sectors) and the disk-info sector.
        let mut cat = vec![0u8; (CATALOG_SECTORS * SECTOR) as usize];
        reader.seek(SeekFrom::Start(partition_offset))?;
        reader.read_exact(&mut cat)?;
        let mut info = [0u8; SECTOR as usize];
        reader.seek(SeekFrom::Start(partition_offset + INFO_OFF))?;
        reader.read_exact(&mut info)?;

        let free_sectors = u16::from_le_bytes([info[0xE5], info[0xE6]]);
        let num_files = info[0xE4] as u16;
        let deleted_count = info[0xF4];
        let volume_label = trim_name(&info[0xF5..0xFD]);

        let mut entries = Vec::new();
        // Track the packed high-water mark across all used slots (live and
        // tombstoned) so a foreign / understated free-count can't hide a file.
        let mut packed_end = DATA_START;
        for slot in 0..MAX_FILES {
            if is_end_of_catalog(&cat, slot) {
                break;
            }
            let off = slot * ENTRY;
            let length_sectors = cat[off + 13] as u64;
            let start_linear = cat[off + 15] as u64 * SPT + cat[off + 14] as u64;
            packed_end = packed_end.max(start_linear + length_sectors);
            if let Some(e) = parse_entry(&cat, slot) {
                entries.push(e);
            }
        }
        let info_high_water = geometry.total_sectors().saturating_sub(free_sectors as u64);
        let first_free_linear = packed_end
            .max(info_high_water)
            .min(geometry.total_sectors());

        Ok(TrdosFilesystem {
            reader,
            partition_offset,
            geometry,
            volume_label,
            free_sectors,
            num_files,
            deleted_count,
            entries,
            first_free_linear,
        })
    }

    fn read_at(&mut self, off: u64, buf: &mut [u8]) -> Result<(), FilesystemError> {
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + off))?;
        self.reader.read_exact(buf)?;
        Ok(())
    }

    fn entry_to_file(&self, e: &TrdosEntry) -> FileEntry {
        let name = e.display_name();
        FileEntry::new_file(
            name.clone(),
            format!("/{name}"),
            e.byte_size(),
            e.start_linear(),
        )
    }

    /// Number of catalogue slots in use per the disk-info sector.
    pub fn declared_file_count(&self) -> u16 {
        self.num_files
    }
}

impl<R: Read + Seek + Send> Filesystem for TrdosFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if !entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        // Flat filesystem: only the root has entries.
        if entry.path != "/" {
            return Ok(Vec::new());
        }
        Ok(self
            .entries
            .clone()
            .iter()
            .map(|e| self.entry_to_file(e))
            .collect())
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        if entry.is_directory() {
            return Err(FilesystemError::InvalidData(format!(
                "TR-DOS read_file on directory: {}",
                entry.path
            )));
        }
        let want = (entry.size as usize).min(max_bytes);
        if want == 0 {
            return Ok(Vec::new());
        }
        let mut buf = vec![0u8; want];
        self.read_at(entry.location * SECTOR, &mut buf)?;
        Ok(buf)
    }

    fn volume_label(&self) -> Option<&str> {
        if self.volume_label.is_empty() {
            None
        } else {
            Some(&self.volume_label)
        }
    }

    fn fs_type(&self) -> &str {
        "TR-DOS"
    }

    fn total_size(&self) -> u64 {
        self.geometry.total_bytes()
    }

    fn used_size(&self) -> u64 {
        let total = self.geometry.total_sectors();
        total.saturating_sub(self.free_sectors as u64) * SECTOR
    }

    fn allocation_unit(&self) -> Option<u64> {
        Some(SECTOR)
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.first_free_linear * SECTOR)
    }

    fn validate_name(&self, name: &str) -> Result<(), FilesystemError> {
        parse_trdos_name(name).map(|_| ())
    }

    fn fsck(&mut self) -> Option<Result<FsckResult, FilesystemError>> {
        Some(fsck_trdos(self))
    }
}

/// Split a display name (`NAME.T`) into the 8-byte, space-padded on-disk name
/// and the single type character. A basename with no `.T` suffix (or a suffix
/// that isn't exactly one character) keeps its whole basename and defaults to a
/// `C` (CODE) type. Errors when the name part is empty, longer than 8 bytes, or
/// carries a non-printable byte.
fn parse_trdos_name(name: &str) -> Result<([u8; 8], u8), FilesystemError> {
    let (stem, type_char) = match name.rsplit_once('.') {
        Some((s, t)) if t.len() == 1 => (s, t.as_bytes()[0]),
        _ => (name, b'C'),
    };
    let bytes = stem.as_bytes();
    if bytes.is_empty() || bytes.len() > 8 {
        return Err(FilesystemError::InvalidData(format!(
            "invalid TR-DOS file name {name:?} (name part must be 1-8 characters)"
        )));
    }
    if bytes.iter().any(|&b| !(0x20..0x7F).contains(&b)) || !(0x20..0x7F).contains(&type_char) {
        return Err(FilesystemError::InvalidData(format!(
            "invalid TR-DOS file name {name:?} (printable ASCII only)"
        )));
    }
    let mut name8 = [b' '; 8];
    name8[..bytes.len()].copy_from_slice(bytes);
    Ok((name8, type_char))
}

/// Sanitize a disk label to 8 space-padded printable-ASCII bytes.
fn sanitize_trdos_label(name: &str) -> [u8; 8] {
    let mut lab = [b' '; 8];
    let bytes: Vec<u8> = name
        .bytes()
        .filter(|&b| (0x20..0x7F).contains(&b))
        .take(8)
        .collect();
    lab[..bytes.len()].copy_from_slice(&bytes);
    lab
}

/// Format a blank TR-DOS volume. The geometry is the smallest TR-DOS disk type
/// whose capacity covers `size_bytes`, capped at 80-track double-sided (640 KiB,
/// the format's maximum): ≤ 160 KiB → 40-track SS, ≤ 320 KiB → 80-track SS, else
/// 640 KiB. `label` becomes the 8-char disk label. Returns the raw image.
pub fn create_blank_trdos(size_bytes: u64, label: &str) -> Result<Vec<u8>, FilesystemError> {
    let disk_type = if size_bytes <= 160 * 1024 {
        0x19 // 40 track, single sided (160 KiB)
    } else if size_bytes <= 320 * 1024 {
        0x18 // 80 track, single sided (320 KiB)
    } else {
        0x16 // 80 track, double sided (640 KiB) — the maximum
    };
    let geom = TrdosGeometry::from_type(disk_type).expect("valid disk type");
    let mut img = vec![0u8; geom.total_bytes() as usize];
    let base = INFO_OFF as usize;
    img[base + 0xE1] = (DATA_START % SPT) as u8; // first free sector = 0
    img[base + 0xE2] = (DATA_START / SPT) as u8; // first free track = 1
    img[base + 0xE3] = disk_type;
    img[base + 0xE4] = 0; // file count
    let free = (geom.total_sectors() - DATA_START) as u16;
    img[base + 0xE5..base + 0xE7].copy_from_slice(&free.to_le_bytes());
    img[base + 0xE7] = TRDOS_ID;
    // 0xF4 deleted count is already 0.
    img[base + 0xF5..base + 0xFD].copy_from_slice(&sanitize_trdos_label(label));
    Ok(img)
}

impl<R: Read + Write + Seek> TrdosFilesystem<R> {
    fn write_at(&mut self, off: u64, data: &[u8]) -> Result<(), FilesystemError> {
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + off))?;
        self.reader.write_all(data)?;
        Ok(())
    }

    /// Serialize and write a 16-byte catalogue entry at its slot.
    fn write_entry(&mut self, e: &TrdosEntry) -> Result<(), FilesystemError> {
        let mut raw = [0u8; ENTRY];
        raw[0..8].copy_from_slice(&e.name);
        raw[8] = e.type_char;
        raw[9..11].copy_from_slice(&e.start_param.to_le_bytes());
        raw[11..13].copy_from_slice(&e.length_bytes.to_le_bytes());
        raw[13] = e.length_sectors;
        raw[14] = e.start_sector;
        raw[15] = e.start_track;
        self.write_at((e.slot * ENTRY) as u64, &raw)
    }

    /// Write the mutable disk-info counters (first-free pointer, file count,
    /// free-sector count, deleted count) back to the disk-info sector.
    fn write_disk_info(&mut self) -> Result<(), FilesystemError> {
        let ff = self.first_free_linear;
        self.write_at(INFO_OFF + 0xE1, &[(ff % SPT) as u8, (ff / SPT) as u8])?;
        self.write_at(INFO_OFF + 0xE4, &[self.num_files as u8])?;
        self.write_at(INFO_OFF + 0xE5, &self.free_sectors.to_le_bytes())?;
        self.write_at(INFO_OFF + 0xF4, &[self.deleted_count])?;
        Ok(())
    }

    /// Find a live entry by its display name, returning its index in `entries`.
    fn find_live(&self, display: &str) -> Option<usize> {
        self.entries
            .iter()
            .position(|e| e.display_name() == display)
    }
}

impl<R: Read + Write + Seek + Send> EditableFilesystem for TrdosFilesystem<R> {
    fn as_filesystem(&self) -> &dyn crate::fs::filesystem::Filesystem {
        self
    }
    fn as_filesystem_mut(&mut self) -> &mut dyn crate::fs::filesystem::Filesystem {
        self
    }
    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn Read,
        data_len: u64,
        _options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        if !parent.is_directory() || parent.path != "/" {
            return Err(FilesystemError::NotADirectory(parent.path.clone()));
        }
        let (name8, type_char) = parse_trdos_name(name)?;
        if self
            .entries
            .iter()
            .any(|e| e.name == name8 && e.type_char == type_char)
        {
            return Err(FilesystemError::AlreadyExists(name.into()));
        }
        if self.num_files as usize >= MAX_FILES {
            return Err(FilesystemError::DiskFull(
                "TR-DOS catalogue full (128 entries)".into(),
            ));
        }
        let need = data_len.div_ceil(SECTOR).max(1);
        // The length-in-sectors field is a single byte, so a TR-DOS file tops
        // out at 255 sectors (65280 bytes).
        if need > 255 {
            return Err(FilesystemError::InvalidData(
                "file too large for TR-DOS (max 255 sectors / 65280 bytes)".into(),
            ));
        }
        let start = self.first_free_linear;
        if start + need > self.geometry.total_sectors() {
            return Err(FilesystemError::DiskFull(format!(
                "not enough contiguous free space (need {need} sectors)"
            )));
        }
        // Write the payload, zero-padding the final sector.
        let mut buf = vec![0u8; (need * SECTOR) as usize];
        if data_len > 0 {
            data.read_exact(&mut buf[..data_len as usize])?;
        }
        self.write_at(start * SECTOR, &buf)?;

        let entry = TrdosEntry {
            slot: self.num_files as usize,
            name: name8,
            type_char,
            start_param: 0,
            length_bytes: data_len as u16, // fits: need <= 255 => data_len <= 65280
            length_sectors: need as u8,
            start_sector: (start % SPT) as u8,
            start_track: (start / SPT) as u8,
        };
        self.write_entry(&entry)?;

        // Advance the high-water mark and free count (append semantics).
        self.num_files += 1;
        self.first_free_linear = start + need;
        self.free_sectors =
            (self.geometry.total_sectors() - self.first_free_linear).min(u16::MAX as u64) as u16;
        self.write_disk_info()?;

        let fe = self.entry_to_file(&entry);
        self.entries.push(entry);
        Ok(fe)
    }

    fn create_directory(
        &mut self,
        _parent: &FileEntry,
        _name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "TR-DOS is a flat filesystem — no subdirectories".into(),
        ))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        if !parent.is_directory() {
            return Err(FilesystemError::NotADirectory(parent.path.clone()));
        }
        let idx = self
            .find_live(&entry.name)
            .ok_or_else(|| FilesystemError::NotFound(entry.name.clone()))?;
        let slot = self.entries[idx].slot;
        // Tombstone: TR-DOS marks a deleted file by setting the first name byte
        // to 0x01, bumps the deleted count, and does NOT reclaim the sectors or
        // the catalogue slot (space is recovered only by the MOVE command).
        self.write_at((slot * ENTRY) as u64, &[0x01])?;
        self.deleted_count = self.deleted_count.saturating_add(1);
        self.write_at(INFO_OFF + 0xF4, &[self.deleted_count])?;
        self.entries.remove(idx);
        Ok(())
    }

    fn rename(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
        new_name: &str,
    ) -> Result<(), FilesystemError> {
        if !parent.is_directory() {
            return Err(FilesystemError::NotADirectory(parent.path.clone()));
        }
        if new_name == entry.name {
            return Ok(());
        }
        let (name8, type_char) = parse_trdos_name(new_name)?;
        if self
            .entries
            .iter()
            .any(|e| e.name == name8 && e.type_char == type_char)
        {
            return Err(FilesystemError::AlreadyExists(new_name.into()));
        }
        let idx = self
            .find_live(&entry.name)
            .ok_or_else(|| FilesystemError::NotFound(entry.name.clone()))?;
        let slot = self.entries[idx].slot;
        // Rewrite the 8-byte name + type char in place (identity/contents keep).
        let mut hdr = [0u8; 9];
        hdr[0..8].copy_from_slice(&name8);
        hdr[8] = type_char;
        self.write_at((slot * ENTRY) as u64, &hdr)?;
        self.entries[idx].name = name8;
        self.entries[idx].type_char = type_char;
        Ok(())
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.reader.flush()?;
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.free_sectors as u64 * SECTOR)
    }

    fn repair(&mut self) -> Result<RepairReport, FilesystemError> {
        repair_trdos(self)
    }
}

// ---- fsck (check + repair) ----
//
// TR-DOS has no allocation bitmap — the packed catalogue *is* the allocation
// map — so the check is self-consistency: a valid disk-info signature, every
// catalogue entry packed contiguously from the first data sector and within the
// disk, and the disk-info counters (file / deleted counts, first-free pointer,
// free-sector count) agreeing with a walk of the catalogue. The counters are
// safely recomputable and repaired by rewriting the disk-info sector; structural
// damage (non-contiguous / past-end / zero-length entries, a bad signature) is
// surfaced read-only, and repair is then withheld.

fn issue(code: &str, message: String, repairable: bool) -> FsckIssue {
    FsckIssue {
        code: code.into(),
        message,
        repairable,
        debug: false,
    }
}

/// Label an entry for a diagnostic message.
fn entry_label(cat: &[u8], slot: usize) -> String {
    let off = slot * ENTRY;
    if cat[off] == 0x01 {
        return format!("deleted slot {slot}");
    }
    format!(
        "{}.{}",
        trim_name(&cat[off..off + 8]),
        printable(cat[off + 8])
    )
}

pub fn fsck_trdos<R: Read + Seek>(
    fs: &mut TrdosFilesystem<R>,
) -> Result<FsckResult, FilesystemError> {
    let geom = fs.geometry;
    let total = geom.total_sectors();
    let end = fs.reader.seek(SeekFrom::End(0))?;
    let disk_sectors = end.saturating_sub(fs.partition_offset) / SECTOR;

    let mut cat = vec![0u8; (CATALOG_SECTORS * SECTOR) as usize];
    fs.read_at(0, &mut cat)?;
    let mut info = [0u8; SECTOR as usize];
    fs.read_at(INFO_OFF, &mut info)?;

    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    // Disk-info signature / geometry.
    if info[0xE7] != TRDOS_ID {
        errors.push(issue(
            "TrdosId",
            format!("disk-info id byte 0x{:02X} is not 0x10", info[0xE7]),
            false,
        ));
    }
    if TrdosGeometry::from_type(info[0xE3]).is_none() {
        errors.push(issue(
            "DiskType",
            format!("invalid disk-type byte 0x{:02X}", info[0xE3]),
            false,
        ));
    }
    if total > disk_sectors {
        errors.push(issue(
            "Truncated",
            format!("disk type implies {total} sectors but the image has {disk_sectors}"),
            false,
        ));
    }

    // Walk every used catalogue slot (live + tombstoned), verifying the
    // contiguous packing that TR-DOS maintains.
    let mut used = 0usize;
    let mut tombstones = 0usize;
    let mut files_checked = 0u32;
    let mut cursor = DATA_START;
    for slot in 0..MAX_FILES {
        if is_end_of_catalog(&cat, slot) {
            break;
        }
        used += 1;
        let off = slot * ENTRY;
        let deleted = cat[off] == 0x01;
        if deleted {
            tombstones += 1;
        } else {
            files_checked += 1;
        }
        let length_sectors = cat[off + 13] as u64;
        let length_bytes = u16::from_le_bytes([cat[off + 11], cat[off + 12]]) as u64;
        let start_linear = cat[off + 15] as u64 * SPT + cat[off + 14] as u64;
        let label = entry_label(&cat, slot);
        if length_sectors == 0 {
            errors.push(issue(
                "ZeroLength",
                format!("entry {slot} ({label}) has a zero length in sectors"),
                false,
            ));
        }
        if start_linear != cursor {
            errors.push(issue(
                "NonContiguous",
                format!(
                    "entry {slot} ({label}) starts at sector {start_linear}, expected {cursor}"
                ),
                false,
            ));
        }
        if start_linear + length_sectors > total {
            errors.push(issue(
                "PastEnd",
                format!("entry {slot} ({label}) extends past the disk end"),
                false,
            ));
        }
        if length_bytes > length_sectors * SECTOR {
            warnings.push(issue(
                "LengthOverflow",
                format!(
                    "entry {slot} ({label}) byte length {length_bytes} exceeds its \
                     {length_sectors}-sector allocation"
                ),
                false,
            ));
        }
        cursor = start_linear + length_sectors;
    }

    // The disk-info counters are recomputable from the catalogue, but only
    // *safely* when the structure the walk relies on is intact. If a structural
    // error was found above, the derived high-water mark is unreliable, so we
    // mark the counter mismatches unrepairable and surface them read-only.
    let structure_clean = errors.is_empty();

    let num_files = info[0xE4] as usize;
    if num_files != used {
        errors.push(issue(
            "FileCount",
            format!("disk-info file count {num_files} disagrees with {used} used catalogue slots"),
            structure_clean,
        ));
    }
    if info[0xF4] as usize != tombstones {
        errors.push(issue(
            "DeletedCount",
            format!(
                "disk-info deleted count {} disagrees with {tombstones} tombstones",
                info[0xF4]
            ),
            structure_clean,
        ));
    }
    let ff_linear = info[0xE2] as u64 * SPT + info[0xE1] as u64;
    if ff_linear != cursor {
        errors.push(issue(
            "FirstFree",
            format!("first-free pointer {ff_linear} disagrees with the high-water mark {cursor}"),
            structure_clean,
        ));
    }
    let free = u16::from_le_bytes([info[0xE5], info[0xE6]]) as u64;
    let expect_free = total.saturating_sub(cursor);
    if free != expect_free {
        errors.push(issue(
            "FreeCount",
            format!("free-sector count {free} disagrees with the computed {expect_free}"),
            structure_clean,
        ));
    }

    let repairable = errors.iter().any(|e| e.repairable);
    Ok(FsckResult {
        stats: FsckStats {
            files_checked,
            directories_checked: 1,
            extra: vec![
                ("volume".into(), fs.volume_label.clone()),
                ("geometry".into(), geom.describe().to_string()),
                ("deleted".into(), tombstones.to_string()),
                ("free_sectors".into(), expect_free.to_string()),
            ],
        },
        repairable,
        errors,
        warnings,
        orphaned_entries: Vec::new(),
    })
}

pub fn repair_trdos<R: Read + Write + Seek>(
    fs: &mut TrdosFilesystem<R>,
) -> Result<RepairReport, FilesystemError> {
    let check = fsck_trdos(fs)?;
    let mut report = RepairReport {
        fixes_applied: Vec::new(),
        fixes_failed: Vec::new(),
        unrepairable_count: check.errors.iter().filter(|e| !e.repairable).count(),
    };
    // The counter mismatches are the only repairable errors, and they are marked
    // repairable only when the structure is intact — so a repairable error being
    // present already implies the packing walk is trustworthy.
    if check.errors.iter().any(|e| e.repairable) {
        let mut cat = vec![0u8; (CATALOG_SECTORS * SECTOR) as usize];
        fs.read_at(0, &mut cat)?;
        let mut used = 0usize;
        let mut tombstones = 0usize;
        let mut cursor = DATA_START;
        for slot in 0..MAX_FILES {
            if is_end_of_catalog(&cat, slot) {
                break;
            }
            used += 1;
            if cat[slot * ENTRY] == 0x01 {
                tombstones += 1;
            }
            cursor += cat[slot * ENTRY + 13] as u64; // contiguous: start == cursor
        }
        fs.num_files = used as u16;
        fs.deleted_count = tombstones as u8;
        fs.first_free_linear = cursor;
        fs.free_sectors = fs
            .geometry
            .total_sectors()
            .saturating_sub(cursor)
            .min(u16::MAX as u64) as u16;
        fs.write_disk_info()?;
        fs.reader.flush()?;
        report.fixes_applied.push(
            "recomputed the disk-info counters (file count, deleted count, first-free pointer, \
             free-sector count) from the catalogue"
                .into(),
        );
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::process::Command;

    /// Hand-build a minimal 640K TR-DOS image with a couple of files placed at
    /// known offsets — pins the reader to the on-disk spec independently of the
    /// oracle.
    fn forge_image() -> Vec<u8> {
        let geom = TrdosGeometry::from_type(0x16).unwrap();
        let mut img = vec![0u8; geom.total_bytes() as usize];

        // Two contiguous files starting at the first data sector (linear 16).
        // File A: "HELLO" type C, 300 bytes -> 2 sectors, at track 1 sector 0.
        // File B: "WORLD" type B, 256 bytes -> 1 sector, at track 1 sector 2.
        let mut put_entry =
            |slot: usize, name: &str, tc: u8, len: u16, secs: u8, st: u8, ss: u8| {
                let off = slot * ENTRY;
                let nm = format!("{name:<8}");
                img[off..off + 8].copy_from_slice(&nm.as_bytes()[..8]);
                img[off + 8] = tc;
                img[off + 9..off + 11].copy_from_slice(&0u16.to_le_bytes());
                img[off + 11..off + 13].copy_from_slice(&len.to_le_bytes());
                img[off + 13] = secs;
                img[off + 14] = ss;
                img[off + 15] = st;
            };
        put_entry(0, "HELLO", b'C', 300, 2, 1, 0);
        put_entry(1, "WORLD", b'B', 256, 1, 1, 2);

        // Payloads (track 1 sector 0 = linear 16 = SPT; track 1 sector 2 = 18).
        let a_off = (SPT * SECTOR) as usize;
        img[a_off..a_off + 300].fill(0xAA);
        let b_off = ((SPT + 2) * SECTOR) as usize;
        img[b_off..b_off + 256].fill(0xBB);

        // Disk-info sector.
        let base = INFO_OFF as usize;
        img[base + 0xE1] = 3; // first free sector (linear 19 = track 1 sector 3)
        img[base + 0xE2] = 1; // first free track
        img[base + 0xE3] = 0x16; // disk type
        img[base + 0xE4] = 2; // file count
        let ff_linear = SPT + 3; // track 1 sector 3 = linear 19
        let free = (geom.total_sectors() - ff_linear) as u16;
        img[base + 0xE5..base + 0xE7].copy_from_slice(&free.to_le_bytes());
        img[base + 0xE7] = TRDOS_ID;
        let label = b"TESTDISK";
        img[base + 0xF5..base + 0xFD].copy_from_slice(label);
        img
    }

    #[test]
    fn detects_and_reads_forged_image() {
        let img = forge_image();
        let mut fs = TrdosFilesystem::open(Cursor::new(img), 0).expect("open");
        assert_eq!(fs.fs_type(), "TR-DOS");
        assert_eq!(fs.volume_label(), Some("TESTDISK"));
        assert_eq!(fs.total_size(), 655_360);

        let root = fs.root().unwrap();
        let files = fs.list_directory(&root).unwrap();
        let names: Vec<&str> = files.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"HELLO.C"), "{names:?}");
        assert!(names.contains(&"WORLD.B"), "{names:?}");

        let hello = files.iter().find(|e| e.name == "HELLO.C").unwrap();
        assert_eq!(hello.size, 300);
        let data = fs.read_file(hello, usize::MAX).unwrap();
        assert_eq!(data.len(), 300);
        assert!(data.iter().all(|&b| b == 0xAA));

        let world = files.iter().find(|e| e.name == "WORLD.B").unwrap();
        assert_eq!(world.size, 256);
        let data = fs.read_file(world, usize::MAX).unwrap();
        assert!(data.iter().all(|&b| b == 0xBB));
    }

    #[test]
    fn rejects_non_trdos() {
        // All zeros: no id byte.
        let img = vec![0u8; 655_360];
        assert!(looks_like_trdos(&mut Cursor::new(img), 0).is_none());
        // Random-ish with a bad type byte but a good id byte.
        let mut img = vec![0u8; 655_360];
        img[INFO_OFF as usize + 0xE7] = TRDOS_ID;
        img[INFO_OFF as usize + 0xE3] = 0x99; // invalid type
        assert!(looks_like_trdos(&mut Cursor::new(img), 0).is_none());
    }

    #[test]
    fn geometry_sizes() {
        assert_eq!(
            TrdosGeometry::from_type(0x16).unwrap().total_bytes(),
            640 * 1024
        );
        assert_eq!(
            TrdosGeometry::from_type(0x17).unwrap().total_bytes(),
            320 * 1024
        );
        assert_eq!(
            TrdosGeometry::from_type(0x18).unwrap().total_bytes(),
            320 * 1024
        );
        assert_eq!(
            TrdosGeometry::from_type(0x19).unwrap().total_bytes(),
            160 * 1024
        );
        assert!(TrdosGeometry::from_type(0x00).is_none());
    }

    // ---- Oracle cross-validation ----

    fn oracle() -> Option<PathBuf> {
        let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/trdos-oracle.py");
        p.exists().then_some(p)
    }

    fn have_python() -> bool {
        Command::new("python3").arg("--version").output().is_ok()
    }

    /// Build a real TR-DOS volume with the clean-room oracle (mkfs + a couple of
    /// files) and return the raw image, or None if python3/oracle unavailable.
    fn oracle_volume() -> Option<Vec<u8>> {
        let script = oracle()?;
        if !have_python() {
            return None;
        }
        let dir = std::env::temp_dir();
        let vol = dir.join(format!("rb_trdos_{}.trd", std::process::id()));
        let volp = vol.to_str()?;
        let run = |args: &[&str]| {
            Command::new("python3")
                .arg(&script)
                .args(args)
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false)
        };
        if !run(&["mkfs", volp, "80DS", "ORACLE"]) {
            return None;
        }
        let hi = dir.join(format!("rb_trdos_hi_{}.bin", std::process::id()));
        std::fs::write(&hi, vec![0x41u8; 700]).ok()?; // 700 bytes -> 3 sectors
        let big = dir.join(format!("rb_trdos_big_{}.bin", std::process::id()));
        std::fs::write(&big, vec![0x42u8; 5000]).ok()?; // 5000 bytes -> 20 sectors
        run(&["addfile", volp, "HELLO", "C", hi.to_str()?]);
        run(&["addfile", volp, "BIGFILE", "C", big.to_str()?]);
        let bytes = std::fs::read(&vol).ok();
        let _ = std::fs::remove_file(&vol);
        let _ = std::fs::remove_file(&hi);
        let _ = std::fs::remove_file(&big);
        bytes
    }

    #[test]
    fn reads_oracle_volume() {
        let Some(img) = oracle_volume() else {
            eprintln!("skipping reads_oracle_volume: python3/oracle unavailable");
            return;
        };
        let mut fs = TrdosFilesystem::open(Cursor::new(img), 0).expect("open oracle volume");
        assert_eq!(fs.volume_label(), Some("ORACLE"));
        assert_eq!(fs.total_size(), 655_360);
        let root = fs.root().unwrap();
        let files = fs.list_directory(&root).unwrap();
        let names: Vec<&str> = files.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"HELLO.C"), "{names:?}");
        assert!(names.contains(&"BIGFILE.C"), "{names:?}");

        let big = files.iter().find(|e| e.name == "BIGFILE.C").unwrap();
        assert_eq!(big.size, 5000);
        let data = fs.read_file(big, usize::MAX).unwrap();
        assert_eq!(data.len(), 5000);
        assert!(data.iter().all(|&b| b == 0x42));
    }

    // ---- Edit ----

    /// A blank writable TR-DOS volume (empty catalogue) built in memory.
    fn blank_image(disk_type: u8, label: &str) -> Vec<u8> {
        let geom = TrdosGeometry::from_type(disk_type).unwrap();
        let mut img = vec![0u8; geom.total_bytes() as usize];
        let base = INFO_OFF as usize;
        img[base + 0xE1] = (DATA_START % SPT) as u8; // first free sector
        img[base + 0xE2] = (DATA_START / SPT) as u8; // first free track
        img[base + 0xE3] = disk_type;
        img[base + 0xE4] = 0; // file count
        let free = (geom.total_sectors() - DATA_START) as u16;
        img[base + 0xE5..base + 0xE7].copy_from_slice(&free.to_le_bytes());
        img[base + 0xE7] = TRDOS_ID;
        let mut lab = [b' '; 8];
        let lb = label.as_bytes();
        let n = lb.len().min(8);
        lab[..n].copy_from_slice(&lb[..n]);
        img[base + 0xF5..base + 0xFD].copy_from_slice(&lab);
        img
    }

    #[test]
    fn parse_name_forms() {
        assert_eq!(parse_trdos_name("GAME.C").unwrap(), (*b"GAME    ", b'C'));
        assert_eq!(parse_trdos_name("boot").unwrap(), (*b"boot    ", b'C')); // default type
        assert_eq!(parse_trdos_name("A.B.D").unwrap(), (*b"A.B     ", b'D')); // dots in name
        assert_eq!(
            parse_trdos_name("EIGHTLET.#").unwrap(),
            (*b"EIGHTLET", b'#')
        );
        assert!(parse_trdos_name("TOOLONGNAME.C").is_err()); // stem > 8
        assert!(parse_trdos_name("").is_err());
    }

    #[test]
    fn edit_create_rename_delete_roundtrip() {
        let mut fs = TrdosFilesystem::open(Cursor::new(blank_image(0x16, "EDITVOL")), 0)
            .expect("open blank");
        let root = fs.root().unwrap();
        assert!(fs.list_directory(&root).unwrap().is_empty());

        let a = vec![0x2Au8; 300]; // 2 sectors
        fs.create_file(
            &root,
            "ALPHA.C",
            &mut &a[..],
            300,
            &CreateFileOptions::default(),
        )
        .expect("create ALPHA");
        let b = vec![0x5Bu8; 5000]; // 20 sectors
        fs.create_file(
            &root,
            "BETA.B",
            &mut &b[..],
            5000,
            &CreateFileOptions::default(),
        )
        .expect("create BETA");

        let alpha = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "ALPHA.C")
            .unwrap();
        fs.rename(&root, &alpha, "GAMMA.C").expect("rename");

        fs.create_file(
            &root,
            "TMP.D",
            &mut &b"xy"[..],
            2,
            &CreateFileOptions::default(),
        )
        .expect("create TMP");
        let tmp = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "TMP.D")
            .unwrap();
        fs.delete_entry(&root, &tmp).expect("delete TMP");

        let files = fs.list_directory(&root).unwrap();
        let names: Vec<&str> = files.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"GAMMA.C"), "{names:?}");
        assert!(names.contains(&"BETA.B"), "{names:?}");
        assert!(!names.contains(&"ALPHA.C"));
        assert!(!names.contains(&"TMP.D"));

        // BETA's data survived the rename + delete-of-another-file untouched.
        let beta = files.iter().find(|e| e.name == "BETA.B").unwrap();
        assert_eq!(beta.size, 5000);
        let data = fs.read_file(beta, usize::MAX).unwrap();
        assert_eq!(data.len(), 5000);
        assert!(data.iter().all(|&x| x == 0x5B));

        // Subdirectories are not a thing in TR-DOS.
        assert!(matches!(
            fs.create_directory(&root, "sub", &CreateDirectoryOptions::default()),
            Err(FilesystemError::Unsupported(_))
        ));
    }

    #[test]
    fn edit_persists_across_reopen() {
        let mut cur = Cursor::new(blank_image(0x16, "REOPEN"));
        {
            let mut fs = TrdosFilesystem::open(&mut cur, 0).unwrap();
            let root = fs.root().unwrap();
            let data = vec![0x7Eu8; 1500];
            fs.create_file(
                &root,
                "KEEP.C",
                &mut &data[..],
                1500,
                &CreateFileOptions::default(),
            )
            .unwrap();
            fs.sync_metadata().unwrap();
        }
        cur.set_position(0);
        let mut fs = TrdosFilesystem::open(&mut cur, 0).unwrap();
        let root = fs.root().unwrap();
        let files = fs.list_directory(&root).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].name, "KEEP.C");
        assert_eq!(files[0].size, 1500);
        assert_eq!(fs.read_file(&files[0], usize::MAX).unwrap().len(), 1500);
    }

    #[test]
    fn oracle_validates_our_edits() {
        let Some(script) = oracle() else {
            eprintln!("skipping oracle_validates_our_edits: oracle unavailable");
            return;
        };
        if !have_python() {
            return;
        }
        let vol = std::env::temp_dir().join(format!("rb_trdos_edit_{}.trd", std::process::id()));
        let volp = vol.to_str().unwrap();
        let run = |args: &[&str]| {
            Command::new("python3")
                .arg(&script)
                .args(args)
                .output()
                .expect("python3")
        };
        assert!(run(&["mkfs", volp, "80DS", "OVOL"]).status.success());
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&vol)
                .unwrap();
            let mut fs = TrdosFilesystem::open(file, 0).unwrap();
            let root = fs.root().unwrap();
            fs.create_file(
                &root,
                "ALPHA.C",
                &mut &[0x41u8; 400][..],
                400,
                &CreateFileOptions::default(),
            )
            .unwrap();
            fs.create_file(
                &root,
                "BETA.C",
                &mut &[0x42u8; 5000][..],
                5000,
                &CreateFileOptions::default(),
            )
            .unwrap();
            fs.create_file(
                &root,
                "TMP.C",
                &mut &[0x43u8; 300][..],
                300,
                &CreateFileOptions::default(),
            )
            .unwrap();
            let tmp = fs
                .list_directory(&root)
                .unwrap()
                .into_iter()
                .find(|e| e.name == "TMP.C")
                .unwrap();
            fs.delete_entry(&root, &tmp).unwrap();
            let alpha = fs
                .list_directory(&root)
                .unwrap()
                .into_iter()
                .find(|e| e.name == "ALPHA.C")
                .unwrap();
            fs.rename(&root, &alpha, "GAMMA.C").unwrap();
            fs.sync_metadata().unwrap();
        }
        // The oracle's fsck must find our create + delete + rename consistent.
        let out = run(&["fsck", volp]);
        let clean = out.status.success();
        let log = String::from_utf8_lossy(&out.stdout).into_owned();
        let _ = std::fs::remove_file(&vol);
        assert!(clean, "oracle fsck flagged our edits:\n{log}");
    }

    // ---- Create-blank ----

    #[test]
    fn create_blank_geometry_by_size() {
        // Size selects the smallest covering geometry, capped at 640 KiB.
        let checks = [
            (140 * 1024, 0x19u8, 160 * 1024u64), // rounds up to 160 KiB
            (160 * 1024, 0x19, 160 * 1024),
            (300 * 1024, 0x18, 320 * 1024),
            (640 * 1024, 0x16, 640 * 1024),
            (2 * 1024 * 1024, 0x16, 640 * 1024), // capped at the maximum
        ];
        for (req, want_type, want_bytes) in checks {
            let img = create_blank_trdos(req, "SZ").unwrap();
            assert_eq!(img.len() as u64, want_bytes, "size {req}");
            let fs = TrdosFilesystem::open(Cursor::new(img), 0).unwrap();
            assert_eq!(fs.geometry.disk_type, want_type, "size {req}");
        }
    }

    #[test]
    fn create_blank_opens_and_edits() {
        let img = create_blank_trdos(640 * 1024, "SPECCY").unwrap();
        let mut fs = TrdosFilesystem::open(Cursor::new(img), 0).expect("open blank");
        assert_eq!(fs.volume_label(), Some("SPECCY"));
        assert_eq!(fs.total_size(), 655_360);
        let root = fs.root().unwrap();
        assert!(fs.list_directory(&root).unwrap().is_empty());
        // The blank is a valid substrate for edits.
        fs.create_file(
            &root,
            "X.C",
            &mut &[1u8; 500][..],
            500,
            &CreateFileOptions::default(),
        )
        .unwrap();
        assert_eq!(fs.list_directory(&root).unwrap().len(), 1);
    }

    #[test]
    fn create_blank_label_sanitized() {
        // Overlong labels are truncated to 8 bytes; the reader trims the
        // trailing space, so "my long label!" -> "my long " -> "my long".
        let img = create_blank_trdos(640 * 1024, "my long label!").unwrap();
        let fs = TrdosFilesystem::open(Cursor::new(img), 0).unwrap();
        assert_eq!(fs.volume_label(), Some("my long"));
    }

    #[test]
    fn oracle_create_blank_is_fsck_clean() {
        let Some(script) = oracle() else {
            eprintln!("skipping oracle_create_blank_is_fsck_clean: oracle unavailable");
            return;
        };
        if !have_python() {
            return;
        }
        let vol = std::env::temp_dir().join(format!("rb_trdos_blank_{}.trd", std::process::id()));
        std::fs::write(&vol, create_blank_trdos(640 * 1024, "BLANKV").unwrap()).unwrap();
        let volp = vol.to_str().unwrap();
        let fsck = |v: &str| {
            Command::new("python3")
                .arg(&script)
                .args(["fsck", v])
                .output()
                .expect("python3")
                .status
                .success()
        };
        assert!(fsck(volp), "oracle fsck flagged our blank");
        // Still clean after writing a file through our editor.
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&vol)
                .unwrap();
            let mut fs = TrdosFilesystem::open(file, 0).unwrap();
            let root = fs.root().unwrap();
            fs.create_file(
                &root,
                "R.C",
                &mut &[0x20u8; 900][..],
                900,
                &CreateFileOptions::default(),
            )
            .unwrap();
            fs.sync_metadata().unwrap();
        }
        let ok = fsck(volp);
        let _ = std::fs::remove_file(&vol);
        assert!(ok, "oracle fsck flagged our blank after an edit");
    }

    // ---- fsck ----

    /// Poke a raw catalogue entry into an image (for structural-damage tests).
    /// `start` is the (logical track, sector) of the entry's first sector.
    fn poke_entry(
        img: &mut [u8],
        slot: usize,
        name: &str,
        tc: u8,
        len_bytes: u16,
        secs: u8,
        start: (u8, u8),
    ) {
        let off = slot * ENTRY;
        let nm = format!("{name:<8}");
        img[off..off + 8].copy_from_slice(&nm.as_bytes()[..8]);
        img[off + 8] = tc;
        img[off + 11..off + 13].copy_from_slice(&len_bytes.to_le_bytes());
        img[off + 13] = secs;
        img[off + 14] = start.1; // sector
        img[off + 15] = start.0; // track
    }

    #[test]
    fn fsck_on_blank_and_populated_is_clean() {
        let mut fs = TrdosFilesystem::open(
            Cursor::new(create_blank_trdos(640 * 1024, "CLEAN").unwrap()),
            0,
        )
        .unwrap();
        let r = fsck_trdos(&mut fs).unwrap();
        assert!(r.is_clean() && r.warnings.is_empty(), "{r:?}");
        let root = fs.root().unwrap();
        fs.create_file(
            &root,
            "A.C",
            &mut &[1u8; 1000][..],
            1000,
            &CreateFileOptions::default(),
        )
        .unwrap();
        fs.create_file(
            &root,
            "B.B",
            &mut &[2u8; 500][..],
            500,
            &CreateFileOptions::default(),
        )
        .unwrap();
        let r2 = fsck_trdos(&mut fs).unwrap();
        assert!(
            r2.is_clean() && r2.warnings.is_empty(),
            "errors={:?} warnings={:?}",
            r2.errors,
            r2.warnings
        );
        assert_eq!(r2.stats.files_checked, 2);
    }

    #[test]
    fn fsck_clean_after_delete_tombstone() {
        // A tombstoned entry keeps its slot + sectors; the counters must still
        // reconcile (the delete path the oracle also validates).
        let mut fs = TrdosFilesystem::open(
            Cursor::new(create_blank_trdos(640 * 1024, "TOMB").unwrap()),
            0,
        )
        .unwrap();
        let root = fs.root().unwrap();
        fs.create_file(
            &root,
            "KEEP.C",
            &mut &[1u8; 300][..],
            300,
            &CreateFileOptions::default(),
        )
        .unwrap();
        fs.create_file(
            &root,
            "GONE.C",
            &mut &[2u8; 300][..],
            300,
            &CreateFileOptions::default(),
        )
        .unwrap();
        let gone = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "GONE.C")
            .unwrap();
        fs.delete_entry(&root, &gone).unwrap();
        let r = fsck_trdos(&mut fs).unwrap();
        assert!(
            r.is_clean() && r.warnings.is_empty(),
            "errors={:?} warnings={:?}",
            r.errors,
            r.warnings
        );
        assert_eq!(r.stats.files_checked, 1);
        let deleted = &r
            .stats
            .extra
            .iter()
            .find(|(k, _)| k == "deleted")
            .unwrap()
            .1;
        assert_eq!(deleted, "1");
    }

    #[test]
    fn fsck_detects_and_repairs_bad_free_count() {
        let mut cur = Cursor::new(create_blank_trdos(640 * 1024, "FIX").unwrap());
        {
            let mut fs = TrdosFilesystem::open(&mut cur, 0).unwrap();
            let root = fs.root().unwrap();
            fs.create_file(
                &root,
                "X.C",
                &mut &[9u8; 700][..],
                700,
                &CreateFileOptions::default(),
            )
            .unwrap();
            fs.sync_metadata().unwrap();
        }
        // Corrupt the free-sector count (disk-info 0xE5..0xE6).
        cur.get_mut()[INFO_OFF as usize + 0xE5] = 0;
        cur.get_mut()[INFO_OFF as usize + 0xE6] = 0;

        {
            let mut fs = TrdosFilesystem::open(&mut cur, 0).unwrap();
            let r = fsck_trdos(&mut fs).unwrap();
            assert!(
                r.errors.iter().any(|e| e.code == "FreeCount"),
                "{:?}",
                r.errors
            );
            assert!(r.repairable);
            let rep = repair_trdos(&mut fs).unwrap();
            assert!(!rep.fixes_applied.is_empty(), "{rep:?}");
            assert_eq!(rep.unrepairable_count, 0);
        }
        cur.set_position(0);
        let mut fs2 = TrdosFilesystem::open(&mut cur, 0).unwrap();
        let r2 = fsck_trdos(&mut fs2).unwrap();
        assert!(r2.is_clean() && r2.warnings.is_empty(), "{r2:?}");
    }

    #[test]
    fn fsck_flags_non_contiguous_unrepairable() {
        let mut img = create_blank_trdos(640 * 1024, "GAP").unwrap();
        // Second entry starts past where the first ends (a gap) — non-contiguous.
        poke_entry(&mut img, 0, "A", b'C', 512, 2, (1, 0)); // linear 16..18
        poke_entry(&mut img, 1, "B", b'C', 256, 1, (1, 5)); // linear 21 (expected 18)
        img[INFO_OFF as usize + 0xE4] = 2; // file count
        let mut fs = TrdosFilesystem::open(Cursor::new(img), 0).unwrap();
        let r = fsck_trdos(&mut fs).unwrap();
        assert!(
            r.errors.iter().any(|e| e.code == "NonContiguous"),
            "{:?}",
            r.errors
        );
        // Repair is withheld while the structure is damaged.
        let rep = repair_trdos(&mut fs).unwrap();
        assert!(rep.unrepairable_count >= 1, "{rep:?}");
        assert!(rep.fixes_applied.is_empty(), "{rep:?}");
    }

    #[test]
    fn oracle_repair_of_corrupt_counters_is_fsck_clean() {
        let Some(script) = oracle() else {
            eprintln!("skipping oracle_repair_of_corrupt_counters: oracle unavailable");
            return;
        };
        if !have_python() {
            return;
        }
        let vol = std::env::temp_dir().join(format!("rb_trdos_rep_{}.trd", std::process::id()));
        std::fs::write(&vol, create_blank_trdos(640 * 1024, "REPV").unwrap()).unwrap();
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&vol)
                .unwrap();
            let mut fs = TrdosFilesystem::open(file, 0).unwrap();
            let root = fs.root().unwrap();
            fs.create_file(
                &root,
                "K.C",
                &mut &[7u8; 700][..],
                700,
                &CreateFileOptions::default(),
            )
            .unwrap();
            fs.sync_metadata().unwrap();
        }
        // Corrupt the disk-info file count on disk, then repair through our code.
        {
            use std::io::{Read as _, Seek as _, Write as _};
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&vol)
                .unwrap();
            f.seek(SeekFrom::Start(INFO_OFF + 0xE4)).unwrap();
            f.write_all(&[9u8]).unwrap();
            let _ = f.read(&mut [0u8; 0]);
        }
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&vol)
                .unwrap();
            let mut fs = TrdosFilesystem::open(file, 0).unwrap();
            let rep = repair_trdos(&mut fs).unwrap();
            assert_eq!(rep.unrepairable_count, 0, "{rep:?}");
        }
        let clean = Command::new("python3")
            .arg(&script)
            .args(["fsck", vol.to_str().unwrap()])
            .output()
            .unwrap()
            .status
            .success();
        let _ = std::fs::remove_file(&vol);
        assert!(clean, "oracle fsck flagged the repaired volume");
    }
}
