//! DragonDOS filesystem — the floppy filesystem for the Dragon Data
//! Dragon 32/64 (and CoCo running DragonDOS) and the MiSTer Dragon core.
//!
//! It is the sister filesystem to RS-DOS / CoCo Disk BASIC ([`super::rsdos`]):
//! both are 18-sector, 256-byte-sector flat `.dsk` images, but the on-disk
//! structures are entirely different. The host OS can't mount these disks, so
//! file extract + add + delete is the whole point (Axis 1 of the MiSTer plan —
//! floppy-only, no resize).
//!
//! ## Geometry
//!
//! Tracks are 18 sectors of 256 bytes; sectors are numbered from 1. Two
//! standard shapes are recognized, distinguished by the on-disk geometry
//! bytes rather than by length alone:
//!
//! | Body bytes | Tracks | Sides | Sectors |
//! |------------|--------|-------|---------|
//! | 184320     | 40     | 1     | 720     |
//! | 368640     | 40     | 2     | 1440    |
//!
//! The image is addressed by **logical sector number (LSN)**, a single linear
//! index across the whole disk. For the standard interleaved `.dsk` layout the
//! byte offset of LSN `L` is simply `L * 256` (the head/track/sector mapping
//! the Dragon ROM uses collapses to a flat index for both single- and
//! double-sided images).
//!
//! ## On-disk layout (reference: MAME `imgtool` `dgndos.cpp`)
//!
//! - **Track 20** is the directory track; **track 16** holds a backup copy
//!   that DragonDOS keeps in sync. Each is 18 contiguous sectors.
//!   - **Sectors 1-2** (first 512 bytes of the track) hold the **bitmap**:
//!     one bit per LSN, **set = free, clear = in use** (the inverse of most
//!     filesystems). Bit for LSN `n` is `byte n/8, bit n%8`; for `n > 1439`
//!     the byte index gains `+76` to skip the geometry tail into sector 2.
//!   - **Sector 1** byte `0xFC` = tracks, `0xFD` = sectors-per-track
//!     (18 single-sided, 36 double-sided), `0xFE` = `~0xFC`, `0xFF` = `~0xFD`.
//!     The two one's-complement bytes act as the format's signature.
//!   - **Sectors 3-18** hold the **directory**: 25-byte entries, 10 per
//!     sector (6 bytes padding per sector), 160 entries total.
//! - **Directory entry** (25 bytes):
//!   ```text
//!   0x00       flag byte: 0x80 deleted, 0x20 continued (byte 0x18 = next
//!              entry index), 0x08 end-of-directory, 0x02 protect,
//!              0x01 this-is-a-continuation-block
//!   header block (flag & 0x01 == 0):
//!     0x01..0x0C filename (8 + 3, NUL-padded)
//!     0x0C..0x18 4 x sector-allocation block: lsn (u16 BE) + count (u8)
//!   continuation block (flag & 0x01 != 0):
//!     0x01..0x16 7 x sector-allocation block
//!     0x16..0x18 unused
//!   0x18       last-or-next: if continued, the next entry index; otherwise
//!              the number of bytes used in the file's last sector (0 = 256)
//!   ```
//! - **File size** = `(total_sectors - 1) * 256 + last_sector_bytes`, where
//!   `total_sectors` is the sum of every extent's `count` and
//!   `last_sector_bytes` comes from the final entry's byte `0x18`.

use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::FileEntry;
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};

pub const SECTOR_SIZE: usize = 256;
pub const SECTORS_PER_TRACK: u64 = 18;
pub const DIR_TRACK: u64 = 20;
pub const BACKUP_DIR_TRACK: u64 = 16;
/// Bytes of the directory track held in memory (18 sectors).
const DIR_TRACK_BYTES: usize = SECTORS_PER_TRACK as usize * SECTOR_SIZE; // 4608
const ENTRY_LEN: usize = 25;
const MAX_DIRENTS: usize = 160;
const HEADER_EXTENTS: usize = 4;
const CONT_EXTENTS: usize = 7;
/// A single sector-allocation extent caps its sector `count` at a `u8`; we
/// follow `imgtool` and stop at 254 so a run never wraps the field.
const MAX_EXTENT_COUNT: u16 = 254;

// Flag-byte bits.
const FLAG_DELETED: u8 = 0x80;
const FLAG_CONTINUED: u8 = 0x20;
const FLAG_END: u8 = 0x08;
const FLAG_PROTECT: u8 = 0x02;
const FLAG_CONTINUATION: u8 = 0x01;
/// Flag a freshly formatted (unused) directory slot carries: deleted + end +
/// continuation, matching `dgndos_diskimage_create`.
const FLAG_BLANK: u8 = FLAG_DELETED | FLAG_END | FLAG_CONTINUATION; // 0x89

/// Disk geometry derived from the on-disk directory track.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DragonDosGeometry {
    pub tracks: u64,
    /// 1 (single-sided) or 2 (double-sided).
    pub sides: u64,
    /// Total logical sectors on the disk.
    pub total_sectors: u64,
}

impl DragonDosGeometry {
    /// Byte offset of the directory track's first sector within the body.
    fn dir_offset(self) -> u64 {
        DIR_TRACK * SECTORS_PER_TRACK * self.sides * SECTOR_SIZE as u64
    }

    /// Byte offset of the backup directory track within the body.
    fn backup_dir_offset(self) -> u64 {
        BACKUP_DIR_TRACK * SECTORS_PER_TRACK * self.sides * SECTOR_SIZE as u64
    }

    /// Byte offset of logical sector `lsn` within the body.
    fn lsn_offset(self, lsn: u64) -> u64 {
        lsn * SECTOR_SIZE as u64
    }
}

/// Map an LSN to its `(byte, bit)` position inside the 512-byte bitmap that
/// occupies the directory track's first two sectors.
fn bitmap_addr(lsn: u64) -> (usize, u8) {
    let mut byte = (lsn / 8) as usize;
    let bit = (lsn % 8) as u8;
    if lsn > 1439 {
        byte += 76;
    }
    (byte, bit)
}

/// Decode an 11-byte (8 + 3) NUL/space-padded name into "NAME.EXT".
fn decode_name(raw: &[u8]) -> String {
    let trim = |s: &[u8]| -> String {
        let end = s
            .iter()
            .rposition(|&b| b != 0x00 && b != b' ')
            .map(|p| p + 1)
            .unwrap_or(0);
        String::from_utf8_lossy(&s[..end]).to_string()
    };
    let name = trim(&raw[0..8]);
    let ext = trim(&raw[8..11]);
    if ext.is_empty() {
        name
    } else {
        format!("{name}.{ext}")
    }
}

/// Encode "NAME.EXT" into the 11-byte NUL-padded on-disk form (matching
/// `imgtool`'s `memset(0)` + `memcpy`).
pub fn encode_name(name: &str) -> Result<[u8; 11], FilesystemError> {
    let (base, ext) = match name.split_once('.') {
        Some((b, e)) => (b, e),
        None => (name, ""),
    };
    if base.is_empty() || base.len() > 8 || ext.len() > 3 {
        return Err(FilesystemError::InvalidData(format!(
            "DragonDOS filename '{name}' must be 1-8 chars + optional 1-3 char extension"
        )));
    }
    let mut out = [0u8; 11];
    let put = |slice: &str, dst: &mut [u8]| -> Result<(), FilesystemError> {
        for (i, c) in slice.chars().enumerate() {
            if !c.is_ascii() || c.is_ascii_control() {
                return Err(FilesystemError::InvalidData(format!(
                    "DragonDOS filename '{name}' contains an illegal character '{c}'"
                )));
            }
            dst[i] = (c as u8).to_ascii_uppercase();
        }
        Ok(())
    };
    put(base, &mut out[0..8])?;
    put(ext, &mut out[8..11])?;
    Ok(out)
}

/// A parsed sector-allocation extent: `count` contiguous sectors from `lsn`.
#[derive(Debug, Clone, Copy)]
struct Extent {
    lsn: u16,
    count: u8,
}

/// Read the geometry bytes from an in-memory directory track and validate the
/// one's-complement signature. Returns `(tracks, sides)` on success.
fn parse_geometry(dir_track: &[u8]) -> Option<(u64, u64)> {
    let tod = dir_track[0xFC];
    let spt = dir_track[0xFD];
    if (!tod) != dir_track[0xFE] || (!spt) != dir_track[0xFF] {
        return None;
    }
    let sides = match spt {
        18 => 1,
        36 => 2,
        _ => return None,
    };
    // The directory track must physically exist on the disk.
    if (tod as u64) <= DIR_TRACK {
        return None;
    }
    Some((tod as u64, sides))
}

/// Sniff whether the flat body at `partition_offset` is a DragonDOS volume.
/// The one's-complement geometry bytes on the directory track act as a strong
/// signature; we additionally require the image length to match the declared
/// geometry exactly.
pub fn looks_like_dragondos<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
) -> Option<DragonDosGeometry> {
    let len = reader
        .seek(SeekFrom::End(0))
        .ok()?
        .checked_sub(partition_offset)?;
    let track_bytes = SECTORS_PER_TRACK * SECTOR_SIZE as u64;
    if len == 0 || !len.is_multiple_of(track_bytes) {
        return None;
    }

    // The directory track lives at a side-dependent offset, so try both.
    for sides in [1u64, 2] {
        let dir_off = DIR_TRACK * SECTORS_PER_TRACK * sides * SECTOR_SIZE as u64;
        if len < dir_off + 2 * SECTOR_SIZE as u64 {
            continue;
        }
        if reader
            .seek(SeekFrom::Start(partition_offset + dir_off))
            .is_err()
        {
            continue;
        }
        let mut sec = [0u8; SECTOR_SIZE];
        if reader.read_exact(&mut sec).is_err() {
            continue;
        }
        let Some((tracks, declared_sides)) = parse_geometry(&sec) else {
            continue;
        };
        if declared_sides != sides {
            continue;
        }
        let total_sectors = tracks * SECTORS_PER_TRACK * sides;
        if len != total_sectors * SECTOR_SIZE as u64 {
            continue;
        }
        return Some(DragonDosGeometry {
            tracks,
            sides,
            total_sectors,
        });
    }
    None
}

/// A DragonDOS filesystem over a flat sector body.
pub struct DragonDosFilesystem<R: Read + Seek + Send> {
    reader: R,
    partition_offset: u64,
    geom: DragonDosGeometry,
    /// The whole 18-sector directory track held in memory (bitmap + dirents).
    dir_track: Vec<u8>,
}

impl<R: Read + Seek + Send> DragonDosFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        let len = reader
            .seek(SeekFrom::End(0))?
            .checked_sub(partition_offset)
            .ok_or_else(|| FilesystemError::InvalidData("partition offset past EOF".into()))?;
        let geom = looks_like_dragondos(&mut reader, partition_offset).ok_or_else(|| {
            FilesystemError::InvalidData(format!("{len} bytes is not a DragonDOS disk"))
        })?;
        let mut fs = DragonDosFilesystem {
            reader,
            partition_offset,
            geom,
            dir_track: vec![0u8; DIR_TRACK_BYTES],
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

    /// Reload the directory track from disk.
    fn refresh(&mut self) -> Result<(), FilesystemError> {
        let dir = self.read_at(self.geom.dir_offset(), DIR_TRACK_BYTES)?;
        self.dir_track.copy_from_slice(&dir);
        Ok(())
    }

    /// Byte range of directory entry `index` inside `dir_track`.
    fn dirent_range(index: usize) -> (usize, usize) {
        let sector = 3 + index / 10; // 1-based sector within the dir track
        let offset = (index * ENTRY_LEN) % 250;
        let start = (sector - 1) * SECTOR_SIZE + offset;
        (start, start + ENTRY_LEN)
    }

    fn dirent(&self, index: usize) -> &[u8] {
        let (s, e) = Self::dirent_range(index);
        &self.dir_track[s..e]
    }

    fn dirent_flag(&self, index: usize) -> u8 {
        self.dirent(index)[0]
    }

    /// Is this a real, listable file (a non-deleted header block)?
    fn is_real_file(&self, index: usize) -> bool {
        let f = self.dirent_flag(index);
        f & FLAG_DELETED == 0 && f & FLAG_CONTINUATION == 0
    }

    /// Read the extents for a file starting at header dirent `index`, plus the
    /// last sector's byte count. Walks the continuation chain.
    fn collect_extents(&self, index: usize) -> Result<(Vec<Extent>, u16), FilesystemError> {
        let mut extents = Vec::new();
        let mut idx = index;
        let mut guard = 0;
        let mut seen = HashSet::new();
        loop {
            guard += 1;
            if guard > MAX_DIRENTS || idx >= MAX_DIRENTS || !seen.insert(idx) {
                return Err(FilesystemError::InvalidData(
                    "DragonDOS directory chain is corrupt".into(),
                ));
            }
            let ent = self.dirent(idx);
            let flag = ent[0];
            let is_cont = flag & FLAG_CONTINUATION != 0;
            let (base, n) = if is_cont {
                (1, CONT_EXTENTS)
            } else {
                (12, HEADER_EXTENTS)
            };
            for b in 0..n {
                let off = base + b * 3;
                let count = ent[off + 2];
                if count == 0 {
                    break;
                }
                let lsn = u16::from_be_bytes([ent[off], ent[off + 1]]);
                extents.push(Extent { lsn, count });
            }
            if flag & FLAG_CONTINUED != 0 {
                idx = ent[24] as usize;
                continue;
            }
            let last = ent[24];
            let last_bytes = if last == 0 { 256 } else { last as u16 };
            return Ok((extents, last_bytes));
        }
    }

    /// Exact byte size of the file whose header is at `index`.
    fn file_size(&self, index: usize) -> Result<u64, FilesystemError> {
        let (extents, last_bytes) = self.collect_extents(index)?;
        let total_sectors: u64 = extents.iter().map(|e| e.count as u64).sum();
        if total_sectors == 0 {
            return Ok(0);
        }
        Ok((total_sectors - 1) * SECTOR_SIZE as u64 + last_bytes as u64)
    }

    fn read_file_bytes(&mut self, index: usize) -> Result<Vec<u8>, FilesystemError> {
        let (extents, _) = self.collect_extents(index)?;
        let size = self.file_size(index)?;
        let mut out = Vec::with_capacity(size as usize);
        for ext in &extents {
            let off = self.geom.lsn_offset(ext.lsn as u64);
            let bytes = self.read_at(off, ext.count as usize * SECTOR_SIZE)?;
            out.extend_from_slice(&bytes);
        }
        out.truncate(size as usize);
        Ok(out)
    }

    fn bitmap_count(&self) -> u64 {
        self.geom.total_sectors
    }

    fn is_free(&self, lsn: u64) -> bool {
        let (byte, bit) = bitmap_addr(lsn);
        self.dir_track[byte] & (1 << bit) != 0
    }

    fn free_lsn_count(&self) -> u64 {
        (0..self.bitmap_count())
            .filter(|&l| self.is_free(l))
            .count() as u64
    }
}

impl<R: Read + Seek + Send> Filesystem for DragonDosFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if entry.path != "/" {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for index in 0..MAX_DIRENTS {
            let flag = self.dirent_flag(index);
            if flag & FLAG_END != 0 && !self.is_real_file(index) {
                break;
            }
            if !self.is_real_file(index) {
                continue;
            }
            let name = decode_name(&self.dirent(index)[1..12]);
            let size = self.file_size(index).unwrap_or(0);
            let mut fe = FileEntry::new_file(name.clone(), format!("/{name}"), size, index as u64);
            if flag & FLAG_PROTECT != 0 {
                fe.special_type = Some("protected".into());
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
        let index = entry.location as usize;
        if index >= MAX_DIRENTS || !self.is_real_file(index) {
            return Err(FilesystemError::NotFound(format!(
                "'{}' is not a live DragonDOS file",
                entry.name
            )));
        }
        let mut data = self.read_file_bytes(index)?;
        data.truncate(max_bytes);
        Ok(data)
    }

    fn fs_type(&self) -> &str {
        "DragonDOS"
    }

    fn volume_label(&self) -> Option<&str> {
        None
    }

    fn total_size(&self) -> u64 {
        self.geom.total_sectors * SECTOR_SIZE as u64
    }

    fn used_size(&self) -> u64 {
        (self.bitmap_count() - self.free_lsn_count()) * SECTOR_SIZE as u64
    }
}

// ---------------------------------------------------------------------------
// Write side
// ---------------------------------------------------------------------------

impl<R: Read + Write + Seek + Send> DragonDosFilesystem<R> {
    fn write_at(&mut self, off: u64, data: &[u8]) -> Result<(), FilesystemError> {
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + off))?;
        self.reader.write_all(data)?;
        Ok(())
    }

    fn set_used(&mut self, lsn: u64) {
        let (byte, bit) = bitmap_addr(lsn);
        self.dir_track[byte] &= !(1 << bit);
    }

    fn set_free(&mut self, lsn: u64) {
        let (byte, bit) = bitmap_addr(lsn);
        self.dir_track[byte] |= 1 << bit;
    }

    fn put_dirent(&mut self, index: usize, ent: &[u8; ENTRY_LEN]) {
        let (s, e) = Self::dirent_range(index);
        self.dir_track[s..e].copy_from_slice(ent);
    }

    /// Lowest `n` directory-slot indices that are deleted or end-of-directory.
    fn free_dirent_slots(&self, n: usize) -> Option<Vec<usize>> {
        let mut slots = Vec::with_capacity(n);
        for index in 0..MAX_DIRENTS {
            let flag = self.dirent_flag(index);
            if flag & FLAG_DELETED != 0 || flag & FLAG_END != 0 {
                slots.push(index);
                if slots.len() == n {
                    return Some(slots);
                }
            }
        }
        None
    }

    /// Lowest free LSNs (data sectors), `n` of them, in ascending order.
    fn alloc_lsns(&self, n: usize) -> Option<Vec<u64>> {
        let mut out = Vec::with_capacity(n);
        for lsn in 0..self.bitmap_count() {
            if self.is_free(lsn) {
                out.push(lsn);
                if out.len() == n {
                    return Some(out);
                }
            }
        }
        None
    }

    /// Persist the in-memory directory track to disk and mirror it to the
    /// backup directory track (track 16), as DragonDOS keeps the two in sync.
    fn flush_dir_track(&mut self) -> Result<(), FilesystemError> {
        let dir = self.dir_track.clone();
        let dir_off = self.geom.dir_offset();
        let backup_off = self.geom.backup_dir_offset();
        self.write_at(dir_off, &dir)?;
        self.write_at(backup_off, &dir)?;
        self.reader.flush()?;
        Ok(())
    }
}

/// Group a sorted list of LSNs into contiguous extents, each capped at
/// [`MAX_EXTENT_COUNT`] sectors.
fn group_extents(lsns: &[u64]) -> Vec<Extent> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < lsns.len() {
        let start = lsns[i];
        let mut count = 1u16;
        while i + (count as usize) < lsns.len()
            && lsns[i + count as usize] == start + count as u64
            && count < MAX_EXTENT_COUNT
        {
            count += 1;
        }
        out.push(Extent {
            lsn: start as u16,
            count: count as u8,
        });
        i += count as usize;
    }
    out
}

impl<R: Read + Write + Seek + Send> EditableFilesystem for DragonDosFilesystem<R> {
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
                "DragonDOS is flat; only the root accepts files".into(),
            ));
        }
        if data_len == 0 {
            return Err(FilesystemError::Unsupported(
                "DragonDOS cannot store a zero-length file".into(),
            ));
        }
        let raw_name = encode_name(name)?;
        // Duplicate check.
        for index in 0..MAX_DIRENTS {
            if self.is_real_file(index) && self.dirent(index)[1..12] == raw_name {
                return Err(FilesystemError::AlreadyExists(format!(
                    "DragonDOS file '{name}' already exists"
                )));
            }
        }

        let mut payload = Vec::with_capacity(data_len as usize);
        data.read_to_end(&mut payload)?;
        if payload.len() as u64 != data_len {
            return Err(FilesystemError::InvalidData(format!(
                "data_len {data_len} != actual {} bytes",
                payload.len()
            )));
        }

        let total_sectors = payload.len().div_ceil(SECTOR_SIZE);
        let last_sector_bytes = payload.len() - (total_sectors - 1) * SECTOR_SIZE; // 1..=256
        let last_or_next: u8 = if last_sector_bytes == SECTOR_SIZE {
            0
        } else {
            last_sector_bytes as u8
        };

        // Allocate the data sectors and group them into extents.
        let lsns = self.alloc_lsns(total_sectors).ok_or_else(|| {
            FilesystemError::DiskFull(format!(
                "need {total_sectors} sectors, {} free",
                self.free_lsn_count()
            ))
        })?;
        let extents = group_extents(&lsns);

        // How many directory entries does the extent list need?
        let dirents_needed = if extents.len() <= HEADER_EXTENTS {
            1
        } else {
            1 + (extents.len() - HEADER_EXTENTS).div_ceil(CONT_EXTENTS)
        };
        let slots = self
            .free_dirent_slots(dirents_needed)
            .ok_or_else(|| FilesystemError::DiskFull("DragonDOS directory is full".into()))?;

        // Mark sectors used and write the payload into them.
        for ext in &extents {
            for s in 0..ext.count as u64 {
                self.set_used(ext.lsn as u64 + s);
            }
        }
        let mut written = 0usize;
        for ext in &extents {
            let mut buf = vec![0u8; ext.count as usize * SECTOR_SIZE];
            let take = (payload.len() - written).min(buf.len());
            buf[..take].copy_from_slice(&payload[written..written + take]);
            let off = self.geom.lsn_offset(ext.lsn as u64);
            self.write_at(off, &buf)?;
            written += take;
        }

        // Build and write the directory entries.
        let mut ext_iter = extents.iter().peekable();
        for (di, &slot) in slots.iter().enumerate() {
            let is_header = di == 0;
            let is_last_dirent = di + 1 == slots.len();
            let (base, capacity) = if is_header {
                (12usize, HEADER_EXTENTS)
            } else {
                (1usize, CONT_EXTENTS)
            };
            let mut ent = [0u8; ENTRY_LEN];
            ent[0] = if is_header { 0 } else { FLAG_CONTINUATION };
            if is_header {
                ent[1..12].copy_from_slice(&raw_name);
            }
            for b in 0..capacity {
                let Some(ext) = ext_iter.peek() else { break };
                let off = base + b * 3;
                ent[off..off + 2].copy_from_slice(&ext.lsn.to_be_bytes());
                ent[off + 2] = ext.count;
                ext_iter.next();
            }
            if is_last_dirent {
                ent[24] = last_or_next;
            } else {
                ent[0] |= FLAG_CONTINUED;
                ent[24] = slots[di + 1] as u8;
            }
            self.put_dirent(slot, &ent);
        }

        self.flush_dir_track()?;
        self.refresh()?;

        let display = decode_name(&raw_name);
        Ok(FileEntry::new_file(
            display.clone(),
            format!("/{display}"),
            payload.len() as u64,
            slots[0] as u64,
        ))
    }

    fn create_directory(
        &mut self,
        _parent: &FileEntry,
        _name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "DragonDOS is a flat filesystem — no subdirectories".into(),
        ))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        if parent.path != "/" {
            return Err(FilesystemError::Unsupported(
                "DragonDOS is flat; only root files are deletable".into(),
            ));
        }
        let index = entry.location as usize;
        if index >= MAX_DIRENTS || !self.is_real_file(index) {
            return Err(FilesystemError::NotFound(format!(
                "'{}' is not a live DragonDOS file",
                entry.name
            )));
        }

        // Walk the dirent chain: free each extent's sectors and mark every
        // entry in the chain deleted.
        let mut idx = index;
        let mut guard = 0;
        let mut seen = HashSet::new();
        loop {
            guard += 1;
            if guard > MAX_DIRENTS || idx >= MAX_DIRENTS || !seen.insert(idx) {
                break;
            }
            let (extents, flag, next) = {
                let ent = self.dirent(idx);
                let flag = ent[0];
                let is_cont = flag & FLAG_CONTINUATION != 0;
                let (base, n) = if is_cont {
                    (1, CONT_EXTENTS)
                } else {
                    (12, HEADER_EXTENTS)
                };
                let mut exts = Vec::new();
                for b in 0..n {
                    let off = base + b * 3;
                    let count = ent[off + 2];
                    if count == 0 {
                        break;
                    }
                    let lsn = u16::from_be_bytes([ent[off], ent[off + 1]]);
                    exts.push(Extent { lsn, count });
                }
                (exts, flag, ent[24])
            };
            for ext in &extents {
                for s in 0..ext.count as u64 {
                    self.set_free(ext.lsn as u64 + s);
                }
            }
            // Mark the slot deleted, clearing protect, zeroing extents+link.
            let mut ent = [0u8; ENTRY_LEN];
            let (rs, re) = Self::dirent_range(idx);
            ent.copy_from_slice(&self.dir_track[rs..re]);
            ent[0] = (flag | FLAG_DELETED) & !FLAG_PROTECT;
            // Zero extent counts so a stale chain can't be re-walked.
            let (base, n) = if flag & FLAG_CONTINUATION != 0 {
                (1, CONT_EXTENTS)
            } else {
                (12, HEADER_EXTENTS)
            };
            for b in 0..n {
                let off = base + b * 3;
                ent[off] = 0;
                ent[off + 1] = 0;
                ent[off + 2] = 0;
            }
            ent[24] = 0;
            self.put_dirent(idx, &ent);

            if flag & FLAG_CONTINUED != 0 {
                idx = next as usize;
            } else {
                break;
            }
        }

        self.flush_dir_track()?;
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
                "DragonDOS is flat; only root files are renamable".into(),
            ));
        }
        let index = entry.location as usize;
        if index >= MAX_DIRENTS || !self.is_real_file(index) {
            return Err(FilesystemError::NotFound(format!(
                "'{}' is not a live DragonDOS file",
                entry.name
            )));
        }
        let raw_name = encode_name(new_name)?;

        // Reject a collision with a *different* live header dirent. Names are
        // folded to uppercase by the encoder, so byte-exact comparison of the
        // encoded form covers case folding.
        for i in 0..MAX_DIRENTS {
            if i != index && self.is_real_file(i) && self.dirent(i)[1..12] == raw_name {
                return Err(FilesystemError::AlreadyExists(new_name.to_string()));
            }
        }

        // Overwrite only the 11-byte name field of the header dirent; the flag
        // byte (incl. protect), extents and continuation link are untouched, so
        // the file keeps its identity and contents. Continuation dirents carry
        // no name, so only the header slot changes.
        let (s, _e) = Self::dirent_range(index);
        self.dir_track[s + 1..s + 12].copy_from_slice(&raw_name);
        self.flush_dir_track()?;
        self.refresh()?;
        Ok(())
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.reader.flush()?;
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.free_lsn_count() * SECTOR_SIZE as u64)
    }
}

/// Build a blank, freshly formatted DragonDOS disk. Mirrors
/// `dgndos_diskimage_create`: a free bitmap with the two directory tracks
/// allocated, geometry bytes set, and all 160 directory slots blank.
pub fn create_blank(tracks: u64, sides: u64) -> Vec<u8> {
    assert!(sides == 1 || sides == 2, "sides must be 1 or 2");
    assert!(tracks > DIR_TRACK, "disk must have a track 20");
    let total_sectors = tracks * SECTORS_PER_TRACK * sides;
    let mut img = vec![0u8; (total_sectors * SECTOR_SIZE as u64) as usize];

    // Assemble the directory track in a scratch buffer first.
    let mut dir = vec![0u8; DIR_TRACK_BYTES];
    // Bitmap: every sector free (bit set).
    for lsn in 0..total_sectors {
        let (byte, bit) = bitmap_addr(lsn);
        dir[byte] |= 1 << bit;
    }
    // Geometry bytes + complement checks.
    dir[0xFC] = tracks as u8;
    dir[0xFD] = (SECTORS_PER_TRACK * sides) as u8;
    dir[0xFE] = !dir[0xFC];
    dir[0xFF] = !dir[0xFD];
    // Allocate the two directory tracks (clear their bits).
    for t in [DIR_TRACK, BACKUP_DIR_TRACK] {
        let base = t * SECTORS_PER_TRACK * sides;
        for i in 0..SECTORS_PER_TRACK {
            let (byte, bit) = bitmap_addr(base + i);
            dir[byte] &= !(1 << bit);
        }
    }
    // Blank every directory slot.
    for index in 0..MAX_DIRENTS {
        let (s, _e) = DragonDosFilesystem::<std::io::Cursor<Vec<u8>>>::dirent_range(index);
        dir[s] = FLAG_BLANK;
    }

    // Place the directory track and its backup copy.
    let dir_off = (DIR_TRACK * SECTORS_PER_TRACK * sides * SECTOR_SIZE as u64) as usize;
    let backup_off = (BACKUP_DIR_TRACK * SECTORS_PER_TRACK * sides * SECTOR_SIZE as u64) as usize;
    img[dir_off..dir_off + DIR_TRACK_BYTES].copy_from_slice(&dir);
    img[backup_off..backup_off + DIR_TRACK_BYTES].copy_from_slice(&dir);
    img
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn open_mem(img: Vec<u8>) -> DragonDosFilesystem<Cursor<Vec<u8>>> {
        DragonDosFilesystem::open(Cursor::new(img), 0).expect("open")
    }

    #[test]
    fn name_encode_decode_round_trips() {
        let raw = encode_name("HELLO.BIN").unwrap();
        assert_eq!(&raw, b"HELLO\0\0\0BIN");
        assert_eq!(decode_name(&raw), "HELLO.BIN");
        assert_eq!(decode_name(&encode_name("game").unwrap()), "GAME");
        assert!(encode_name("TOOLONGXX.Y").is_err());
        assert!(encode_name("").is_err());
    }

    #[test]
    fn blank_disk_detects_and_is_empty() {
        let img = create_blank(40, 1);
        assert_eq!(img.len(), 184320);
        let mut cur = Cursor::new(img.clone());
        let geom = looks_like_dragondos(&mut cur, 0).expect("detect");
        assert_eq!(geom.tracks, 40);
        assert_eq!(geom.sides, 1);
        assert_eq!(geom.total_sectors, 720);

        let mut fs = open_mem(img);
        assert_eq!(fs.fs_type(), "DragonDOS");
        let root = fs.root().unwrap();
        assert!(fs.list_directory(&root).unwrap().is_empty());
        // 720 total - 36 reserved (two dir tracks) = 684 free sectors.
        assert_eq!(fs.free_space().unwrap(), 684 * SECTOR_SIZE as u64);
    }

    #[test]
    fn double_sided_blank_detects() {
        let img = create_blank(40, 2);
        assert_eq!(img.len(), 368640);
        let mut cur = Cursor::new(img.clone());
        let geom = looks_like_dragondos(&mut cur, 0).expect("detect");
        assert_eq!(geom.sides, 2);
        assert_eq!(geom.total_sectors, 1440);
    }

    #[test]
    fn create_read_delete_round_trip() {
        let mut fs = open_mem(create_blank(40, 1));
        let root = fs.root().unwrap();

        // Multi-sector binary (700 bytes -> 3 sectors).
        let big: Vec<u8> = (0..700).map(|i| (i % 256) as u8).collect();
        fs.create_file(
            &root,
            "DATA.BIN",
            &mut Cursor::new(big.clone()),
            big.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        // Exact-multiple-of-256 file.
        let exact = vec![0xAAu8; 512];
        fs.create_file(
            &root,
            "EXACT",
            &mut Cursor::new(exact.clone()),
            exact.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        let tiny = b"HELLO DRAGON\r".to_vec();
        fs.create_file(
            &root,
            "HI.TXT",
            &mut Cursor::new(tiny.clone()),
            tiny.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();

        let listing = fs.list_directory(&root).unwrap();
        assert_eq!(listing.len(), 3);
        let d = listing.iter().find(|e| e.name == "DATA.BIN").unwrap();
        assert_eq!(d.size, 700);
        assert_eq!(fs.read_file(d, usize::MAX).unwrap(), big);
        let x = listing.iter().find(|e| e.name == "EXACT").unwrap();
        assert_eq!(x.size, 512);
        assert_eq!(fs.read_file(x, usize::MAX).unwrap(), exact);
        let h = listing.iter().find(|e| e.name == "HI.TXT").unwrap();
        assert_eq!(fs.read_file(h, usize::MAX).unwrap(), tiny);

        let free_before = fs.free_space().unwrap();
        fs.delete_entry(&root, &h.clone()).unwrap();
        assert_eq!(fs.list_directory(&root).unwrap().len(), 2);
        assert!(fs.free_space().unwrap() > free_before);

        // Re-open from bytes to prove persistence.
        let bytes = fs.into_inner().into_inner();
        let mut fs2 = DragonDosFilesystem::open(Cursor::new(bytes), 0).unwrap();
        let r2 = fs2.root().unwrap();
        let l2 = fs2.list_directory(&r2).unwrap();
        assert_eq!(l2.len(), 2);
        let d2 = l2.iter().find(|e| e.name == "DATA.BIN").unwrap();
        assert_eq!(fs2.read_file(d2, usize::MAX).unwrap(), big);
        let x2 = l2.iter().find(|e| e.name == "EXACT").unwrap();
        assert_eq!(fs2.read_file(x2, usize::MAX).unwrap(), exact);
    }

    #[test]
    fn rename_in_place_round_trip() {
        let mut fs = open_mem(create_blank(40, 1));
        let root = fs.root().unwrap();

        let big: Vec<u8> = (0..700).map(|i| (i % 256) as u8).collect();
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
        // Capture the first data LSN (identity) before the rename.
        let (lsns_before, _) = fs.collect_extents(d.location as usize).unwrap();
        let first_lsn = lsns_before[0].lsn;

        fs.rename(&root, &d, "REPORT.TXT").unwrap();

        // Re-open from bytes to prove persistence.
        let bytes = fs.into_inner().into_inner();
        let mut fs2 = DragonDosFilesystem::open(Cursor::new(bytes), 0).unwrap();
        let r2 = fs2.root().unwrap();
        let l2 = fs2.list_directory(&r2).unwrap();
        let names: Vec<&str> = l2.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"REPORT.TXT"));
        assert!(!names.contains(&"DATA.BIN"));

        // Identity (first data LSN) + contents preserved.
        let renamed = l2.iter().find(|e| e.name == "REPORT.TXT").unwrap();
        let (lsns_after, _) = fs2.collect_extents(renamed.location as usize).unwrap();
        assert_eq!(lsns_after[0].lsn, first_lsn);
        assert_eq!(fs2.read_file(renamed, usize::MAX).unwrap(), big);
    }

    #[test]
    fn rename_collision_rejected() {
        let mut fs = open_mem(create_blank(40, 1));
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

    #[test]
    fn large_multi_extent_file() {
        // Force a file big enough to span more than the 4 header extents only
        // if fragmented; here it is contiguous so it stays a single extent,
        // but it exercises the >256-sector data path on a double-sided disk.
        let mut fs = open_mem(create_blank(40, 2));
        let root = fs.root().unwrap();
        let big: Vec<u8> = (0..300 * 256).map(|i| (i * 7 % 256) as u8).collect();
        fs.create_file(
            &root,
            "HUGE.DAT",
            &mut Cursor::new(big.clone()),
            big.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        let listing = fs.list_directory(&root).unwrap();
        let h = listing.iter().find(|e| e.name == "HUGE.DAT").unwrap();
        assert_eq!(h.size, big.len() as u64);
        assert_eq!(fs.read_file(h, usize::MAX).unwrap(), big);
    }

    #[test]
    fn duplicate_name_rejected() {
        let mut fs = open_mem(create_blank(40, 1));
        let root = fs.root().unwrap();
        fs.create_file(
            &root,
            "ONE",
            &mut Cursor::new(vec![1u8; 100]),
            100,
            &CreateFileOptions::default(),
        )
        .unwrap();
        let err = fs
            .create_file(
                &root,
                "ONE",
                &mut Cursor::new(vec![2u8; 100]),
                100,
                &CreateFileOptions::default(),
            )
            .unwrap_err();
        assert!(matches!(err, FilesystemError::AlreadyExists(_)));
    }
}
