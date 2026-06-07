//! Apple DOS 3.3 filesystem — the floppy filesystem for the Apple ][ /
//! ][+ / //e (and Apple-II MiSTer core) until ProDOS took over.
//!
//! Disk geometry: 35 tracks × 16 sectors × 256 B = 140 KB. Files live in
//! a flat catalog on track 17. Disk-image byte ordering (`.do` vs `.po`)
//! is handled upstream by [`crate::rbformats::containers::sector_order`];
//! this driver always reads **DOS-order** bytes from offset 0.
//!
//! ## On-disk layout (Beneath Apple DOS, Worth & Lechner, 1981, ch. 4)
//!
//! - **VTOC** at T17S0 (byte 0x11000) — volume info, free-sector bitmap,
//!   first-catalog pointer:
//!
//! ```text
//! 0x01..0x02   first catalog T/S (typically 17, 15)
//! 0x03         DOS version (3 for DOS 3.3)
//! 0x06         volume number (1..254)
//! 0x27         max T/S pairs per T/S list sector (= 122)
//! 0x30         last track allocated
//! 0x31         direction of next allocation (+1 / -1)
//! 0x34         tracks/disk (= 35)
//! 0x35         sectors/track (= 16)
//! 0x36..0x37   bytes/sector LE (= 256)
//! 0x38..0xFF   free-sector bitmap, 4 bytes per track (50 tracks max)
//! ```
//!
//! Bitmap convention: bit set = sector is FREE. For track T the 4-byte
//! row at byte `0x38 + T*4` packs 16 bits (sectors 15..0 in
//! big-endian-style packing, see [`is_sector_free`]).
//!
//! - **Catalog sectors** at T17S15 → T17S14 → ... → T17S1 — 15 sectors
//!   total, chained via byte 0x01 (track) + 0x02 (sector). Each carries
//!   up to 7 35-byte file entries starting at byte 0x0B:
//!
//! ```text
//! 0x00         first T/S list track (0 = empty slot, 0xFF = deleted)
//! 0x01         first T/S list sector
//! 0x02         file type + lock flag
//!                bit 7   = locked
//!                0x00    = T (text)
//!                0x01    = I (Integer BASIC)
//!                0x02    = A (Applesoft BASIC)
//!                0x04    = B (binary)
//!                0x08    = S
//!                0x10    = R (relocatable)
//! 0x03..0x20   filename, 30 bytes, "Apple ASCII" (high bit set),
//!              space-padded (0xA0).
//! 0x21..0x22   file length in sectors, LE
//! ```
//!
//! Deleted entries: byte 0x00 = 0xFF; the original first T/S list track
//! is preserved in byte 0x20 (last byte of filename) so DOS 3.3's
//! `UNDELETE` command could restore them. We skip deleted entries in
//! `list_directory`.
//!
//! - **T/S list sector** — chain of T/S pairs describing file data:
//!
//! ```text
//! 0x01..0x02   next T/S list T/S (0,0 = end of chain)
//! 0x05..0x06   sector offset within file (LE) — = how many T/S pairs
//!              are in earlier T/S list sectors. The first list has 0.
//! 0x0C..0xFF   122 T/S pairs (2 bytes each: track, sector). T=0 marks a
//!              sparse hole in the file (skip 256 zero bytes).
//! ```
//!
//! The Filesystem trait surface here is **extract floor**: read raw file
//! data via the T/S chain. No interpretation by type (binary header
//! stripping, BASIC detokenization) — that's the consumer's job. The
//! MiSTer-plan Add/Delete bar will land in a follow-up.

use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};
use crate::rbformats::containers::sector_order::{
    APPLE_II_DISK_BYTES, APPLE_II_SECTORS_PER_TRACK, APPLE_II_SECTOR_BYTES, APPLE_II_TRACKS,
    APPLE_II_TRACK_BYTES,
};

/// Track that holds the VTOC + the entire catalog chain. DOS 3.3 hard-
/// codes this; we surface it as a constant for clarity / tests.
pub const VTOC_TRACK: u8 = 17;
/// Sector that holds the VTOC.
pub const VTOC_SECTOR: u8 = 0;

/// DOS 3.3 file-type byte values (low 7 bits; bit 7 = locked).
pub const TYPE_T: u8 = 0x00; // text
pub const TYPE_I: u8 = 0x01; // Integer BASIC
pub const TYPE_A: u8 = 0x02; // Applesoft BASIC
pub const TYPE_B: u8 = 0x04; // binary
pub const TYPE_S: u8 = 0x08;
pub const TYPE_R: u8 = 0x10; // relocatable

/// Locked-flag bit in the file-type byte.
const LOCK_BIT: u8 = 0x80;

/// Maximum catalog sectors we walk during enumeration. DOS 3.3 always
/// places the catalog on track 17 in 15 sectors; the cap is a runaway
/// guard on damaged disks.
const MAX_CATALOG_SECTORS: usize = 32;

/// Maximum T/S list sectors we walk for a single file. With 122 pairs
/// per list and 256-byte sectors, one full list addresses ~30 KB; a
/// 140 KB floppy can chain at most a handful. Cap is a runaway guard.
const MAX_TS_LISTS: usize = 32;

/// Maximum file entries we surface — sanity bound; a 140 KB DOS disk
/// tops out around 105 (7 entries × 15 catalog sectors).
const MAX_FILES: usize = 256;

/// Parsed VTOC fields the rest of the module needs.
#[derive(Debug, Clone)]
pub struct AppleDosVtoc {
    pub first_catalog_track: u8,
    pub first_catalog_sector: u8,
    pub dos_version: u8,
    pub volume_number: u8,
    pub max_ts_pairs_per_list: u8,
    pub tracks_per_disk: u8,
    pub sectors_per_track: u8,
    pub bytes_per_sector: u16,
    /// Raw free-bitmap bytes copied straight out of the VTOC for later
    /// queries via `is_sector_free`.
    pub bitmap: [u8; 200],
}

impl AppleDosVtoc {
    fn parse(sector: &[u8; APPLE_II_SECTOR_BYTES]) -> Result<Self, FilesystemError> {
        // Minimal sanity: the layout-defining fields must match DOS 3.3
        // hard-codes. Same checks as `containers::sector_order::
        // looks_like_dos_order_vtoc`, but now we want the actual values.
        let first_catalog_track = sector[0x01];
        let first_catalog_sector = sector[0x02];
        let dos_version = sector[0x03];
        let volume_number = sector[0x06];
        let max_ts_pairs_per_list = sector[0x27];
        let tracks_per_disk = sector[0x34];
        let sectors_per_track = sector[0x35];
        let bytes_per_sector = u16::from_le_bytes([sector[0x36], sector[0x37]]);

        if tracks_per_disk != APPLE_II_TRACKS as u8
            || sectors_per_track != APPLE_II_SECTORS_PER_TRACK as u8
            || bytes_per_sector != APPLE_II_SECTOR_BYTES as u16
        {
            return Err(FilesystemError::InvalidData(format!(
                "VTOC geometry mismatch: {tracks_per_disk}T x {sectors_per_track}S x \
                 {bytes_per_sector}B (need 35x16x256)"
            )));
        }
        if max_ts_pairs_per_list != 122 {
            return Err(FilesystemError::InvalidData(format!(
                "VTOC max T/S pairs is {max_ts_pairs_per_list}, expected 122"
            )));
        }

        let mut bitmap = [0u8; 200];
        bitmap.copy_from_slice(&sector[0x38..0x38 + 200]);

        Ok(AppleDosVtoc {
            first_catalog_track,
            first_catalog_sector,
            dos_version,
            volume_number,
            max_ts_pairs_per_list,
            tracks_per_disk,
            sectors_per_track,
            bytes_per_sector,
            bitmap,
        })
    }

    /// True if a sector is free per the VTOC bitmap. Each track gets 4
    /// bytes; byte 0 holds bits for sectors 15..8 (MSB = sector 15),
    /// byte 1 holds bits for sectors 7..0 (MSB = sector 7). Bit set =
    /// FREE (opposite of most modern filesystems).
    pub fn is_sector_free(&self, track: u8, sector: u8) -> bool {
        if (track as usize) >= APPLE_II_TRACKS || (sector as usize) >= APPLE_II_SECTORS_PER_TRACK {
            return false;
        }
        let row = (track as usize) * 4;
        if sector >= 8 {
            // High byte covers sectors 15..8, with sector 15 = bit 7.
            let bit = sector - 8;
            (self.bitmap[row] >> bit) & 1 == 1
        } else {
            // Low byte covers sectors 7..0, with sector 7 = bit 7.
            (self.bitmap[row + 1] >> sector) & 1 == 1
        }
    }

    /// Count the free sectors across every track in the bitmap.
    pub fn free_sector_count(&self) -> u32 {
        let mut total = 0u32;
        for t in 0..APPLE_II_TRACKS {
            for s in 0..APPLE_II_SECTORS_PER_TRACK {
                if self.is_sector_free(t as u8, s as u8) {
                    total += 1;
                }
            }
        }
        total
    }
}

/// One catalog-entry slot, decoded from a 35-byte slice.
#[derive(Debug, Clone)]
pub struct AppleDosFileEntry {
    /// First T/S list sector for this file. (0xFF, _) means deleted;
    /// (0, 0) means the entry slot has never been used.
    pub first_ts_track: u8,
    pub first_ts_sector: u8,
    /// File type code (low 7 bits of byte 0x02), no lock bit.
    pub type_code: u8,
    /// True if the type byte's high bit was set.
    pub locked: bool,
    /// Decoded filename (Apple-ASCII high-bit stripped, trailing spaces
    /// stripped). May be empty for malformed entries.
    pub name: String,
    /// File length in sectors per the VTOC. Approximation — the real byte
    /// length comes from the file's content (binary header / Applesoft
    /// header / text terminator).
    pub length_sectors: u16,
}

impl AppleDosFileEntry {
    fn parse(buf: &[u8; 35]) -> Self {
        let first_ts_track = buf[0x00];
        let first_ts_sector = buf[0x01];
        let type_byte = buf[0x02];
        let locked = type_byte & LOCK_BIT != 0;
        let type_code = type_byte & 0x7F;
        let name = decode_apple_text(&buf[0x03..0x21]);
        let length_sectors = u16::from_le_bytes([buf[0x21], buf[0x22]]);
        AppleDosFileEntry {
            first_ts_track,
            first_ts_sector,
            type_code,
            locked,
            name: name.trim_end().to_string(),
            length_sectors,
        }
    }

    /// True if the slot has never been used by DOS 3.3 (both first-T/S
    /// pointer bytes zero). `list_directory` skips these.
    pub fn is_unused(&self) -> bool {
        self.first_ts_track == 0 && self.first_ts_sector == 0
    }

    /// True if the file has been deleted (first T/S list track set to
    /// 0xFF by `DELETE`). The original track byte is preserved in the
    /// last byte of the name field so DOS's `UNDELETE` can restore.
    pub fn is_deleted(&self) -> bool {
        self.first_ts_track == 0xFF
    }
}

/// Decode a slice of high-bit-set "Apple ASCII" bytes into UTF-8. Each
/// byte's high bit is masked off; 0x00 bytes terminate; trailing spaces
/// (0x20 after masking) are kept and the caller trims as needed.
fn decode_apple_text(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len());
    for &b in bytes {
        if b == 0 {
            break;
        }
        let c = (b & 0x7F) as char;
        out.push(c);
    }
    out
}

/// Map a low-7-bit type code to its DOS letter ("T", "I", "A", "B",
/// "S", "R"), defaulting to "?" for unknown values. Used to surface the
/// file kind through `FileEntry::special_type`.
pub fn type_letter(type_code: u8) -> &'static str {
    match type_code {
        TYPE_T => "T",
        TYPE_I => "I",
        TYPE_A => "A",
        TYPE_B => "B",
        TYPE_S => "S",
        TYPE_R => "R",
        _ => "?",
    }
}

/// Live Apple DOS 3.3 reader. Holds the VTOC + the decoded catalog so
/// list/read calls don't re-walk the catalog chain.
pub struct AppleDosFilesystem<R: Read + Seek + Send> {
    reader: R,
    partition_offset: u64,
    pub(crate) vtoc: AppleDosVtoc,
    entries: Vec<AppleDosFileEntry>,
}

impl<R: Read + Seek + Send> AppleDosFilesystem<R> {
    /// Open a DOS 3.3 volume at `partition_offset` (0 for a superfloppy).
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        let vtoc_buf = read_sector(&mut reader, partition_offset, VTOC_TRACK, VTOC_SECTOR)?;
        let vtoc = AppleDosVtoc::parse(&vtoc_buf)?;

        // Walk the catalog chain.
        let mut entries = Vec::new();
        let mut track = vtoc.first_catalog_track;
        let mut sector = vtoc.first_catalog_sector;
        let mut seen = std::collections::HashSet::new();
        for _hop in 0..MAX_CATALOG_SECTORS {
            if track == 0 {
                break;
            }
            if !seen.insert((track, sector)) {
                return Err(FilesystemError::InvalidData(format!(
                    "catalog chain cycle at T{track}S{sector}"
                )));
            }
            let cat = read_sector(&mut reader, partition_offset, track, sector)?;
            // 7 entries per catalog sector at offsets 0x0B, 0x0B+35, ...
            for i in 0..7 {
                let off = 0x0B + i * 35;
                let entry_buf: &[u8; 35] = cat[off..off + 35].try_into().unwrap();
                let e = AppleDosFileEntry::parse(entry_buf);
                if entries.len() >= MAX_FILES {
                    return Err(FilesystemError::InvalidData(format!(
                        "catalog exceeds MAX_FILES = {MAX_FILES}"
                    )));
                }
                entries.push(e);
            }
            track = cat[0x01];
            sector = cat[0x02];
        }

        Ok(AppleDosFilesystem {
            reader,
            partition_offset,
            vtoc,
            entries,
        })
    }

    /// Read every byte of a file by walking its T/S list chain. Sparse
    /// holes (T = 0 in a T/S pair) are filled with 256 zero bytes per
    /// hole. Caller is responsible for type-specific reinterpretation
    /// (e.g. stripping the 4-byte binary-file header).
    fn read_file_bytes(&mut self, e: &AppleDosFileEntry) -> Result<Vec<u8>, FilesystemError> {
        if e.is_unused() || e.is_deleted() {
            return Err(FilesystemError::NotFound(format!(
                "file '{}' is deleted or unallocated",
                e.name
            )));
        }
        let mut data = Vec::with_capacity(e.length_sectors as usize * APPLE_II_SECTOR_BYTES);
        let mut tslist_track = e.first_ts_track;
        let mut tslist_sector = e.first_ts_sector;
        let mut seen = std::collections::HashSet::new();
        for _ in 0..MAX_TS_LISTS {
            if tslist_track == 0 {
                break;
            }
            if !seen.insert((tslist_track, tslist_sector)) {
                return Err(FilesystemError::InvalidData(format!(
                    "T/S list cycle in '{}' at T{tslist_track}S{tslist_sector}",
                    e.name
                )));
            }
            let list_buf = read_sector(
                &mut self.reader,
                self.partition_offset,
                tslist_track,
                tslist_sector,
            )?;
            // 122 T/S pairs starting at byte 0x0C.
            for pair in 0..self.vtoc.max_ts_pairs_per_list as usize {
                let off = 0x0C + pair * 2;
                let dt = list_buf[off];
                let ds = list_buf[off + 1];
                if dt == 0 && ds == 0 {
                    // Trailing zero pairs mark end-of-file within this
                    // list. Continue scanning to keep things simple
                    // (DOS 3.3 typically packs from the front).
                    continue;
                }
                if dt == 0 {
                    // Sparse hole — 256 zeros.
                    data.extend(std::iter::repeat_n(0u8, APPLE_II_SECTOR_BYTES));
                    continue;
                }
                let sec = read_sector(&mut self.reader, self.partition_offset, dt, ds)?;
                data.extend_from_slice(&sec);
            }
            tslist_track = list_buf[0x01];
            tslist_sector = list_buf[0x02];
        }
        Ok(data)
    }

    /// Apple DOS 3.3 stores binary files with a 4-byte header prepended
    /// to the actual data: `load_address(LE u16) + length(LE u16)`. This
    /// helper strips it and returns just the payload. For other file
    /// types the bytes are returned as-is.
    pub fn strip_binary_header(bytes: &[u8]) -> &[u8] {
        if bytes.len() < 4 {
            return bytes;
        }
        let claimed_len = u16::from_le_bytes([bytes[2], bytes[3]]) as usize;
        if claimed_len + 4 > bytes.len() {
            return bytes;
        }
        &bytes[4..4 + claimed_len]
    }
}

/// Read a 256-byte Apple-II sector relative to `partition_offset`. The
/// caller is responsible for ensuring `(track, sector)` is in range —
/// out-of-range pairs return `InvalidData`.
fn read_sector<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    track: u8,
    sector: u8,
) -> Result<[u8; APPLE_II_SECTOR_BYTES], FilesystemError> {
    if (track as usize) >= APPLE_II_TRACKS || (sector as usize) >= APPLE_II_SECTORS_PER_TRACK {
        return Err(FilesystemError::InvalidData(format!(
            "T{track}S{sector} out of range for 35x16 Apple-II geometry"
        )));
    }
    let off = partition_offset
        + (track as u64) * APPLE_II_TRACK_BYTES as u64
        + (sector as u64) * APPLE_II_SECTOR_BYTES as u64;
    reader.seek(SeekFrom::Start(off))?;
    let mut buf = [0u8; APPLE_II_SECTOR_BYTES];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

impl<R: Read + Seek + Send> Filesystem for AppleDosFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if entry.path != "/" {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(self.entries.len());
        for (idx, e) in self.entries.iter().enumerate() {
            if e.is_unused() || e.is_deleted() {
                continue;
            }
            let approx_size = e.length_sectors as u64 * APPLE_II_SECTOR_BYTES as u64;
            // Use the entry index as the FileEntry location so read_file
            // can find this specific catalog slot O(1).
            let mut fe = FileEntry::new_file(
                e.name.clone(),
                format!("/{}", e.name),
                approx_size,
                idx as u64,
            );
            fe.special_type = Some(format!(
                "{}{}",
                type_letter(e.type_code),
                if e.locked { "*" } else { "" }
            ));
            out.push(fe);
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
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
        let mut data = self.read_file_bytes(&de)?;
        // For binary files, strip the 4-byte header so the bytes the
        // user gets back are the actual payload (matching what
        // CiderPress2's "Extract Raw" does on binary files).
        if de.type_code == TYPE_B {
            let payload = Self::strip_binary_header(&data).to_vec();
            data = payload;
        }
        data.truncate(max_bytes);
        Ok(data)
    }

    fn fs_type(&self) -> &str {
        "DOS 3.3"
    }

    fn volume_label(&self) -> Option<&str> {
        // DOS 3.3 has no volume label — only a numeric volume_number.
        // Surfacing as a string would imply something it isn't.
        None
    }

    fn total_size(&self) -> u64 {
        APPLE_II_DISK_BYTES as u64
    }

    fn used_size(&self) -> u64 {
        let free = self.vtoc.free_sector_count() as u64 * APPLE_II_SECTOR_BYTES as u64;
        self.total_size().saturating_sub(free)
    }
}

/// Encode an Apple-ASCII filename to its 30-byte on-disk form (high-bit
/// set on every char; trailing 0xA0 padding).
fn encode_apple_name(name: &str) -> Result<[u8; 30], FilesystemError> {
    if name.is_empty() {
        return Err(FilesystemError::InvalidData(
            "DOS 3.3 filename is empty".into(),
        ));
    }
    if name.len() > 30 {
        return Err(FilesystemError::InvalidData(format!(
            "DOS 3.3 filename '{name}' exceeds 30 chars"
        )));
    }
    let mut buf = [0xA0u8; 30];
    for (i, c) in name.chars().enumerate() {
        if !c.is_ascii() {
            return Err(FilesystemError::InvalidData(format!(
                "DOS 3.3 filename '{name}' contains non-ASCII character '{c}'"
            )));
        }
        let b = c as u8;
        if !(0x20..=0x7E).contains(&b) {
            return Err(FilesystemError::InvalidData(format!(
                "DOS 3.3 filename '{name}' contains non-printable byte 0x{b:02X}"
            )));
        }
        buf[i] = b | 0x80;
    }
    Ok(buf)
}

impl<R: Read + Write + Seek + Send> AppleDosFilesystem<R> {
    /// Mark a sector USED (bit cleared in the bitmap).
    fn bitmap_mark_used(&mut self, track: u8, sector: u8) {
        let row = 0x38 + (track as usize) * 4;
        if sector >= 8 {
            self.vtoc.bitmap[row - 0x38] &= !(1 << (sector - 8));
        } else {
            self.vtoc.bitmap[row + 1 - 0x38] &= !(1 << sector);
        }
    }

    /// Mark a sector FREE (bit set in the bitmap).
    fn bitmap_mark_free(&mut self, track: u8, sector: u8) {
        let row = 0x38 + (track as usize) * 4;
        if sector >= 8 {
            self.vtoc.bitmap[row - 0x38] |= 1 << (sector - 8);
        } else {
            self.vtoc.bitmap[row + 1 - 0x38] |= 1 << sector;
        }
    }

    /// Find a free sector and mark it used. Allocation walks tracks
    /// outward from track 17 (DOS 3.3's standard convention: prefer the
    /// directory track's neighbors, then spiral). Returns
    /// `Err(InvalidData)` when the volume is full.
    fn alloc_one_sector(&mut self) -> Result<(u8, u8), FilesystemError> {
        // Walk outward from track 17 in alternating direction so the
        // catalog stays clustered near the head's typical resting place.
        let order: Vec<u8> = std::iter::once(17u8)
            .chain((1..=16).flat_map(|d: u8| {
                let inc = 17u8.checked_add(d);
                let dec = 17u8.checked_sub(d);
                inc.into_iter().chain(dec)
            }))
            .filter(|&t| (t as usize) < APPLE_II_TRACKS)
            .collect();
        for t in order {
            // Skip the entire VTOC+catalog track for data allocation
            // to mirror DOS 3.3's behavior; the catalog itself is
            // pre-allocated at fixture-build time.
            if t == 17 {
                continue;
            }
            for s in 0..APPLE_II_SECTORS_PER_TRACK as u8 {
                if self.vtoc.is_sector_free(t, s) {
                    self.bitmap_mark_used(t, s);
                    return Ok((t, s));
                }
            }
        }
        Err(FilesystemError::InvalidData(
            "DOS 3.3 volume full — no free sectors outside track 17".into(),
        ))
    }

    /// Find a free catalog entry slot in the 15-sector catalog chain.
    /// Returns `(catalog_track, catalog_sector, entry_index_within_sector)`.
    fn find_free_catalog_slot(&mut self) -> Result<(u8, u8, usize), FilesystemError> {
        let mut track = self.vtoc.first_catalog_track;
        let mut sector = self.vtoc.first_catalog_sector;
        let mut seen = std::collections::HashSet::new();
        for _hop in 0..MAX_CATALOG_SECTORS {
            if track == 0 {
                break;
            }
            if !seen.insert((track, sector)) {
                break;
            }
            let cat = read_sector(&mut self.reader, self.partition_offset, track, sector)?;
            for i in 0..7 {
                let off = 0x0B + i * 35;
                let flags = cat[off];
                // Free if first-T/S track byte is 0 (never used) or 0xFF
                // (deleted, can be reclaimed).
                if flags == 0 || flags == 0xFF {
                    return Ok((track, sector, i));
                }
            }
            track = cat[0x01];
            sector = cat[0x02];
        }
        Err(FilesystemError::InvalidData(
            "DOS 3.3 catalog full — no free entry slot in the 15-sector catalog chain".into(),
        ))
    }

    /// Write a 256-byte sector at (track, sector).
    fn write_sector(
        &mut self,
        track: u8,
        sector: u8,
        data: &[u8; APPLE_II_SECTOR_BYTES],
    ) -> Result<(), FilesystemError> {
        if (track as usize) >= APPLE_II_TRACKS || (sector as usize) >= APPLE_II_SECTORS_PER_TRACK {
            return Err(FilesystemError::InvalidData(format!(
                "write_sector T{track}S{sector} out of range"
            )));
        }
        let off = self.partition_offset
            + (track as u64) * APPLE_II_TRACK_BYTES as u64
            + (sector as u64) * APPLE_II_SECTOR_BYTES as u64;
        self.reader.seek(SeekFrom::Start(off))?;
        self.reader.write_all(data)?;
        Ok(())
    }

    /// Write the VTOC bitmap back into T17S0 (overwrites only the
    /// 200-byte bitmap region; preserves the geometry fields and the
    /// first-catalog pointer).
    fn vtoc_write_back(&mut self) -> Result<(), FilesystemError> {
        let mut sec = read_sector(&mut self.reader, self.partition_offset, 17, 0)?;
        sec[0x38..0x38 + 200].copy_from_slice(&self.vtoc.bitmap);
        self.write_sector(17, 0, &sec)
    }
}

impl<R: Read + Write + Seek + Send> EditableFilesystem for AppleDosFilesystem<R> {
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
                "DOS 3.3 is flat; only the root accepts new files".into(),
            ));
        }
        let encoded_name = encode_apple_name(name)?;
        if self
            .entries
            .iter()
            .any(|e| !e.is_unused() && !e.is_deleted() && e.name == name)
        {
            return Err(FilesystemError::InvalidData(format!(
                "DOS 3.3 file '{name}' already exists"
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
        if data_len > u16::MAX as u64 * APPLE_II_SECTOR_BYTES as u64 {
            return Err(FilesystemError::InvalidData(
                "DOS 3.3 file exceeds the 16-bit sector-count field".into(),
            ));
        }

        // Default to type T (text) — caller may overwrite via direct
        // catalog-slot edit if they need binary; this floor is
        // conservative and avoids stamping a binary header the
        // EditableFilesystem trait can't communicate.
        let type_code = TYPE_T;

        let data_sectors_needed = payload.len().div_ceil(APPLE_II_SECTOR_BYTES).max(1) as u32;
        let pairs_per_list = self.vtoc.max_ts_pairs_per_list as u32;
        let ts_list_count = data_sectors_needed.div_ceil(pairs_per_list).max(1);
        let total_sectors = data_sectors_needed + ts_list_count;
        let free = self.vtoc.free_sector_count();
        if total_sectors > free {
            return Err(FilesystemError::InvalidData(format!(
                "out of space: need {total_sectors} sectors, have {free} free"
            )));
        }

        // Allocate T/S list sectors first so we know their addresses.
        let mut ts_list_addrs: Vec<(u8, u8)> = Vec::with_capacity(ts_list_count as usize);
        for _ in 0..ts_list_count {
            ts_list_addrs.push(self.alloc_one_sector()?);
        }
        // Allocate data sectors.
        let mut data_addrs: Vec<(u8, u8)> = Vec::with_capacity(data_sectors_needed as usize);
        for _ in 0..data_sectors_needed {
            data_addrs.push(self.alloc_one_sector()?);
        }

        // Write data sectors. The last one is zero-padded.
        for (i, &(t, s)) in data_addrs.iter().enumerate() {
            let chunk_off = i * APPLE_II_SECTOR_BYTES;
            let chunk_end = (chunk_off + APPLE_II_SECTOR_BYTES).min(payload.len());
            let mut sec = [0u8; APPLE_II_SECTOR_BYTES];
            if chunk_off < payload.len() {
                let chunk = &payload[chunk_off..chunk_end];
                sec[..chunk.len()].copy_from_slice(chunk);
            }
            self.write_sector(t, s, &sec)?;
        }

        // Write T/S list sectors.
        for (li, &(lt, ls)) in ts_list_addrs.iter().enumerate() {
            let mut sec = [0u8; APPLE_II_SECTOR_BYTES];
            // Next T/S list pointer at bytes 0x01..0x03.
            if li + 1 < ts_list_addrs.len() {
                let (nt, ns) = ts_list_addrs[li + 1];
                sec[0x01] = nt;
                sec[0x02] = ns;
            }
            // sector offset within file at 0x05..0x07 LE.
            let offset_in_file = (li as u32 * pairs_per_list) as u16;
            sec[0x05] = (offset_in_file & 0xFF) as u8;
            sec[0x06] = (offset_in_file >> 8) as u8;
            // Pairs at 0x0C..
            let start = li as u32 * pairs_per_list;
            let end = (start + pairs_per_list).min(data_sectors_needed);
            for pair_i in start..end {
                let (dt, ds) = data_addrs[pair_i as usize];
                let local_off = 0x0C + ((pair_i - start) as usize) * 2;
                sec[local_off] = dt;
                sec[local_off + 1] = ds;
            }
            self.write_sector(lt, ls, &sec)?;
        }

        // Write the catalog entry.
        let (ct, cs, slot) = self.find_free_catalog_slot()?;
        let mut cat = read_sector(&mut self.reader, self.partition_offset, ct, cs)?;
        let off = 0x0B + slot * 35;
        cat[off] = ts_list_addrs[0].0;
        cat[off + 1] = ts_list_addrs[0].1;
        cat[off + 2] = type_code; // type T, unlocked
        cat[off + 3..off + 33].copy_from_slice(&encoded_name);
        let length_sectors = data_sectors_needed as u16 + ts_list_count as u16;
        cat[off + 33] = (length_sectors & 0xFF) as u8;
        cat[off + 34] = (length_sectors >> 8) as u8;
        self.write_sector(ct, cs, &cat)?;
        // Persist VTOC bitmap.
        self.vtoc_write_back()?;
        self.reader.flush()?;

        // Refresh the in-memory catalog so subsequent list_directory
        // calls see the new entry.
        self.refresh_entries()?;

        // Locate the new entry by name in the refreshed list to give
        // the caller a usable `location` index.
        let idx = self
            .entries
            .iter()
            .position(|e| !e.is_unused() && !e.is_deleted() && e.name == name)
            .ok_or_else(|| {
                FilesystemError::InvalidData(
                    "internal: created file not visible after refresh".into(),
                )
            })?;
        let mut fe = FileEntry::new_file(
            name.to_string(),
            format!("/{name}"),
            payload.len() as u64,
            idx as u64,
        );
        fe.special_type = Some(type_letter(type_code).to_string());
        Ok(fe)
    }

    fn create_directory(
        &mut self,
        _parent: &FileEntry,
        _name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "DOS 3.3 is flat — no subdirectories".into(),
        ))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        if parent.path != "/" {
            return Err(FilesystemError::Unsupported(
                "DOS 3.3 is flat; only files in the root are deletable".into(),
            ));
        }
        if entry.entry_type != EntryType::File {
            return Err(FilesystemError::InvalidData(
                "DOS 3.3 only stores files".into(),
            ));
        }
        let idx = entry.location as usize;
        let de =
            self.entries.get(idx).cloned().ok_or_else(|| {
                FilesystemError::NotFound(format!("entry idx {idx} out of range"))
            })?;
        if de.is_unused() || de.is_deleted() {
            return Err(FilesystemError::NotFound(
                "entry already deleted or unallocated".into(),
            ));
        }

        // Walk the T/S list, freeing every data sector and the list
        // sectors themselves.
        let mut tslist_track = de.first_ts_track;
        let mut tslist_sector = de.first_ts_sector;
        let mut seen = std::collections::HashSet::new();
        for _ in 0..MAX_TS_LISTS {
            if tslist_track == 0 {
                break;
            }
            if !seen.insert((tslist_track, tslist_sector)) {
                return Err(FilesystemError::InvalidData(
                    "delete_entry hit a T/S list cycle".into(),
                ));
            }
            let list_buf = read_sector(
                &mut self.reader,
                self.partition_offset,
                tslist_track,
                tslist_sector,
            )?;
            for pair in 0..self.vtoc.max_ts_pairs_per_list as usize {
                let off = 0x0C + pair * 2;
                let dt = list_buf[off];
                let ds = list_buf[off + 1];
                if dt != 0 {
                    self.bitmap_mark_free(dt, ds);
                }
            }
            self.bitmap_mark_free(tslist_track, tslist_sector);
            tslist_track = list_buf[0x01];
            tslist_sector = list_buf[0x02];
        }

        // Locate the catalog slot for this entry by re-walking the
        // catalog chain and matching name. Easier than caching the
        // (track, sector, slot) trio for every entry.
        let mut found = None;
        let mut track = self.vtoc.first_catalog_track;
        let mut sector = self.vtoc.first_catalog_sector;
        let mut seen_cat = std::collections::HashSet::new();
        'outer: for _ in 0..MAX_CATALOG_SECTORS {
            if track == 0 {
                break;
            }
            if !seen_cat.insert((track, sector)) {
                break;
            }
            let cat = read_sector(&mut self.reader, self.partition_offset, track, sector)?;
            for i in 0..7 {
                let off = 0x0B + i * 35;
                if cat[off] == 0 || cat[off] == 0xFF {
                    continue;
                }
                let mut probe = [0u8; 35];
                probe.copy_from_slice(&cat[off..off + 35]);
                let parsed = AppleDosFileEntry::parse(&probe);
                if parsed.name == de.name {
                    found = Some((track, sector, i));
                    break 'outer;
                }
            }
            track = cat[0x01];
            sector = cat[0x02];
        }
        let (ct, cs, slot) = found.ok_or_else(|| {
            FilesystemError::NotFound(format!("catalog slot for '{}' not found", de.name))
        })?;
        let mut cat = read_sector(&mut self.reader, self.partition_offset, ct, cs)?;
        let off = 0x0B + slot * 35;
        // Mark as deleted per DOS 3.3 spec: byte 0x00 = 0xFF, original
        // first-T/S track preserved in byte 0x20 (last filename byte) so
        // UNDELETE could recover. We just stamp 0xFF over byte 0 — the
        // rest of the slot stays so the user could recover via DOS
        // tools.
        cat[off + 0x20] = cat[off];
        cat[off] = 0xFF;
        self.write_sector(ct, cs, &cat)?;
        self.vtoc_write_back()?;
        self.reader.flush()?;
        self.refresh_entries()?;
        Ok(())
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        // Every primitive flushes synchronously, so sync_metadata is a
        // no-op. The trait requires it, so we just bounce the reader.
        self.reader.flush()?;
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.vtoc.free_sector_count() as u64 * APPLE_II_SECTOR_BYTES as u64)
    }
}

impl<R: Read + Write + Seek + Send> AppleDosFilesystem<R> {
    /// Re-walk the catalog chain and rebuild the in-memory `entries`
    /// list. Called after every mutation so subsequent list_directory
    /// reflects the on-disk state.
    fn refresh_entries(&mut self) -> Result<(), FilesystemError> {
        let mut entries = Vec::new();
        let mut track = self.vtoc.first_catalog_track;
        let mut sector = self.vtoc.first_catalog_sector;
        let mut seen = std::collections::HashSet::new();
        for _ in 0..MAX_CATALOG_SECTORS {
            if track == 0 {
                break;
            }
            if !seen.insert((track, sector)) {
                break;
            }
            let cat = read_sector(&mut self.reader, self.partition_offset, track, sector)?;
            for i in 0..7 {
                let off = 0x0B + i * 35;
                let buf: &[u8; 35] = cat[off..off + 35].try_into().unwrap();
                entries.push(AppleDosFileEntry::parse(buf));
            }
            track = cat[0x01];
            sector = cat[0x02];
        }
        self.entries = entries;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Encode an ASCII string into 30 bytes of high-bit-set "Apple
    /// ASCII", space-padded (0xA0 = ' ').
    fn enc_name(name: &str) -> [u8; 30] {
        let mut buf = [0xA0u8; 30];
        for (i, c) in name.chars().take(30).enumerate() {
            buf[i] = (c as u8) | 0x80;
        }
        buf
    }

    /// Build a minimal DOS 3.3 disk image: VTOC + one catalog sector
    /// (T17S15) chaining to (0,0), one binary file "HELLO" with payload
    /// b"hi from dos33".
    fn build_test_disk() -> Vec<u8> {
        let mut disk = vec![0u8; APPLE_II_DISK_BYTES];

        // ----- VTOC at T17S0 -----
        let vtoc_off = 17 * APPLE_II_TRACK_BYTES;
        let v = &mut disk[vtoc_off..vtoc_off + APPLE_II_SECTOR_BYTES];
        v[0x01] = 17; // first catalog track
        v[0x02] = 15; // first catalog sector
        v[0x03] = 3; // DOS version
        v[0x06] = 254; // volume number
        v[0x27] = 122; // max T/S pairs per list
        v[0x34] = APPLE_II_TRACKS as u8;
        v[0x35] = APPLE_II_SECTORS_PER_TRACK as u8;
        v[0x36] = 0x00;
        v[0x37] = 0x01;
        // Mark every sector free initially (bitmap rows of 0xFFFF on the
        // first 2 bytes per track), then mark the four sectors we use
        // (T17S0 VTOC, T17S15 catalog, T1S0 T/S list, T2S0 data) as used.
        for t in 0..APPLE_II_TRACKS {
            let row = 0x38 + t * 4;
            v[row] = 0xFF; // sectors 15..8 free
            v[row + 1] = 0xFF; // sectors 7..0 free
        }
        let mut mark_used = |t: u8, s: u8| {
            let row = 0x38 + (t as usize) * 4;
            if s >= 8 {
                v[row] &= !(1 << (s - 8));
            } else {
                v[row + 1] &= !(1 << s);
            }
        };
        mark_used(17, 0); // VTOC
        mark_used(17, 15); // catalog
        mark_used(1, 0); // T/S list
        mark_used(2, 0); // data sector

        // ----- Catalog at T17S15 -----
        let cat_off = 17 * APPLE_II_TRACK_BYTES + 15 * APPLE_II_SECTOR_BYTES;
        let c = &mut disk[cat_off..cat_off + APPLE_II_SECTOR_BYTES];
        c[0x01] = 0; // next catalog track = 0 (end of chain — single-sector for test)
        c[0x02] = 0;
        // Entry 0 starts at offset 0x0B.
        let e0 = 0x0B;
        c[e0] = 1; // first T/S list track
        c[e0 + 1] = 0; // first T/S list sector
        c[e0 + 2] = TYPE_B; // binary, unlocked
        c[e0 + 3..e0 + 33].copy_from_slice(&enc_name("HELLO"));
        c[e0 + 33] = 2; // length in sectors (LE u16)
        c[e0 + 34] = 0;

        // ----- T/S list at T1S0 -----
        let tsl_off = APPLE_II_TRACK_BYTES; // T1S0
        let l = &mut disk[tsl_off..tsl_off + APPLE_II_SECTOR_BYTES];
        l[0x01] = 0; // next T/S list = (0, 0) end
        l[0x02] = 0;
        l[0x0C] = 2; // first data sector at T2S0
        l[0x0D] = 0;

        // ----- Data at T2S0 -----
        // Binary file: 4-byte header (load_addr LE + length LE) then payload.
        let data_off = 2 * APPLE_II_TRACK_BYTES;
        let payload = b"hi from dos33";
        let d = &mut disk[data_off..data_off + APPLE_II_SECTOR_BYTES];
        d[0] = 0x00; // load address LE: 0x0800
        d[1] = 0x08;
        d[2] = (payload.len() & 0xFF) as u8; // length LE
        d[3] = (payload.len() >> 8) as u8;
        d[4..4 + payload.len()].copy_from_slice(payload);

        disk
    }

    #[test]
    fn parses_vtoc_geometry() {
        let disk = build_test_disk();
        let cur = Cursor::new(disk);
        let fs = AppleDosFilesystem::open(cur, 0).unwrap();
        assert_eq!(fs.vtoc.tracks_per_disk, 35);
        assert_eq!(fs.vtoc.sectors_per_track, 16);
        assert_eq!(fs.vtoc.bytes_per_sector, 256);
        assert_eq!(fs.vtoc.dos_version, 3);
        assert_eq!(fs.vtoc.volume_number, 254);
        assert_eq!(fs.fs_type(), "DOS 3.3");
    }

    #[test]
    fn lists_single_file() {
        let disk = build_test_disk();
        let cur = Cursor::new(disk);
        let mut fs = AppleDosFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "HELLO");
        assert_eq!(entries[0].special_type.as_deref(), Some("B"));
    }

    #[test]
    fn reads_binary_file_strips_header() {
        let disk = build_test_disk();
        let cur = Cursor::new(disk);
        let mut fs = AppleDosFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let payload = fs.read_file(&entries[0], 1024).unwrap();
        assert_eq!(&payload, b"hi from dos33");
    }

    #[test]
    fn strip_binary_header_round_trip() {
        // Build a synthetic 4-byte-header + payload buffer and check the
        // helper strips correctly. Bounds-check: claimed_len past the
        // buffer end returns the original bytes unchanged.
        let mut buf = vec![0u8; 4];
        buf[2] = 5; // claimed length 5
        buf[3] = 0;
        buf.extend_from_slice(b"hello");
        let payload = AppleDosFilesystem::<Cursor<Vec<u8>>>::strip_binary_header(&buf);
        assert_eq!(payload, b"hello");

        let truncated = vec![0u8; 4]; // claimed length 0; payload is empty
        let payload = AppleDosFilesystem::<Cursor<Vec<u8>>>::strip_binary_header(&truncated);
        assert_eq!(payload, b"");

        let bogus = vec![0u8, 0u8, 0xFF, 0x7F, b'a']; // claimed length 0x7FFF > buf
        let payload = AppleDosFilesystem::<Cursor<Vec<u8>>>::strip_binary_header(&bogus);
        assert_eq!(payload, &bogus[..]);
    }

    #[test]
    fn deleted_entries_are_skipped() {
        let mut disk = build_test_disk();
        // Mark the one file deleted: byte 0x00 of catalog entry 0 -> 0xFF.
        let entry0 = 17 * APPLE_II_TRACK_BYTES + 15 * APPLE_II_SECTOR_BYTES + 0x0B;
        disk[entry0] = 0xFF;
        let cur = Cursor::new(disk);
        let mut fs = AppleDosFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn bitmap_polarity_set_means_free() {
        let disk = build_test_disk();
        let cur = Cursor::new(disk);
        let fs = AppleDosFilesystem::open(cur, 0).unwrap();
        // Used sectors marked at build time.
        assert!(!fs.vtoc.is_sector_free(17, 0));
        assert!(!fs.vtoc.is_sector_free(17, 15));
        assert!(!fs.vtoc.is_sector_free(1, 0));
        assert!(!fs.vtoc.is_sector_free(2, 0));
        // Plenty of free sectors elsewhere.
        assert!(fs.vtoc.is_sector_free(0, 0));
        assert!(fs.vtoc.is_sector_free(5, 7));
        assert!(fs.vtoc.is_sector_free(34, 15));
    }

    #[test]
    fn total_and_used_size_reflect_bitmap() {
        let disk = build_test_disk();
        let cur = Cursor::new(disk);
        let fs = AppleDosFilesystem::open(cur, 0).unwrap();
        assert_eq!(fs.total_size(), 35 * 16 * 256);
        // 4 sectors used (VTOC, catalog, T/S list, data) => 4 * 256 = 1024 B used.
        assert_eq!(fs.used_size(), 4 * 256);
    }

    #[test]
    fn rejects_bad_vtoc_geometry() {
        let mut disk = build_test_disk();
        // Stomp tracks/disk to 80 — should fail.
        disk[17 * APPLE_II_TRACK_BYTES + 0x34] = 80;
        let cur = Cursor::new(disk);
        match AppleDosFilesystem::open(cur, 0) {
            Ok(_) => panic!("expected open to fail on bad geometry"),
            Err(FilesystemError::InvalidData(msg)) => {
                assert!(msg.contains("VTOC geometry"), "got: {msg}");
            }
            Err(other) => panic!("expected InvalidData, got {other:?}"),
        }
    }

    #[test]
    fn type_letter_round_trip() {
        assert_eq!(type_letter(TYPE_T), "T");
        assert_eq!(type_letter(TYPE_I), "I");
        assert_eq!(type_letter(TYPE_A), "A");
        assert_eq!(type_letter(TYPE_B), "B");
        assert_eq!(type_letter(TYPE_S), "S");
        assert_eq!(type_letter(TYPE_R), "R");
        assert_eq!(type_letter(0x40), "?"); // unknown
    }

    #[test]
    fn apple_text_decoder_strips_high_bit() {
        // 0xC8 0xC5 0xCC 0xCC 0xCF = "HELLO" with high bits set.
        let s = decode_apple_text(&[0xC8, 0xC5, 0xCC, 0xCC, 0xCF, 0x00, 0xFF]);
        assert_eq!(s, "HELLO");
    }

    // ------------------------------------------------------------------
    // EditableFilesystem — create / delete / round-trip
    // ------------------------------------------------------------------

    /// Build a test disk wrapped in a Cursor so the edit tests have
    /// Read + Write + Seek bounds.
    fn edit_fixture() -> Cursor<Vec<u8>> {
        Cursor::new(build_test_disk())
    }

    #[test]
    fn encode_apple_name_round_trips_through_decode_apple_text() {
        let buf = encode_apple_name("NEW.TXT").unwrap();
        let s = decode_apple_text(&buf);
        assert_eq!(s.trim_end(), "NEW.TXT");
    }

    #[test]
    fn encode_apple_name_rejects_empty_and_oversize_and_nonascii() {
        assert!(encode_apple_name("").is_err());
        let too_long = "A".repeat(31);
        assert!(encode_apple_name(&too_long).is_err());
        assert!(encode_apple_name("café").is_err());
    }

    #[test]
    fn create_file_persists_through_sync_and_reopen() {
        let mut fs = AppleDosFilesystem::open(edit_fixture(), 0).unwrap();
        let root = fs.root().unwrap();
        let before_free = EditableFilesystem::free_space(&mut fs).unwrap();
        let payload = b"freshly created via EditableFilesystem".to_vec();
        let mut src = Cursor::new(payload.clone());
        let fe = fs
            .create_file(
                &root,
                "NEW.TXT",
                &mut src,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
        assert_eq!(fe.name, "NEW.TXT");
        assert_eq!(fe.special_type.as_deref(), Some("T"));
        fs.sync_metadata().unwrap();

        let inner = fs.reader.clone();
        let mut fs2 = AppleDosFilesystem::open(inner, 0).unwrap();
        let root2 = fs2.root().unwrap();
        let entries = fs2.list_directory(&root2).unwrap();
        let new = entries.iter().find(|e| e.name == "NEW.TXT").unwrap();
        let read_back = fs2.read_file(new, 1024).unwrap();
        // Type T means no binary-header strip — bytes come back verbatim
        // padded to the sector boundary; trim the trailing zeros.
        let trimmed_len = payload.len();
        assert_eq!(&read_back[..trimmed_len], payload.as_slice());

        // Free space dropped by 2 sectors (1 data + 1 T/S list).
        let after_free = EditableFilesystem::free_space(&mut fs2).unwrap();
        assert_eq!(after_free, before_free - 2 * APPLE_II_SECTOR_BYTES as u64);
    }

    #[test]
    fn delete_entry_releases_sectors() {
        let mut fs = AppleDosFilesystem::open(edit_fixture(), 0).unwrap();
        let root = fs.root().unwrap();
        let before_free = EditableFilesystem::free_space(&mut fs).unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let hello = entries.iter().find(|e| e.name == "HELLO").unwrap().clone();
        fs.delete_entry(&root, &hello).unwrap();
        fs.sync_metadata().unwrap();

        let inner = fs.reader.clone();
        let mut fs2 = AppleDosFilesystem::open(inner, 0).unwrap();
        let root2 = fs2.root().unwrap();
        let entries = fs2.list_directory(&root2).unwrap();
        assert!(
            entries.iter().all(|e| e.name != "HELLO"),
            "HELLO should be gone after delete"
        );
        // HELLO used T/S list at T1S0 + data at T2S0 = 2 sectors.
        let after_free = EditableFilesystem::free_space(&mut fs2).unwrap();
        assert_eq!(after_free, before_free + 2 * APPLE_II_SECTOR_BYTES as u64);
    }

    #[test]
    fn create_file_rejects_duplicate_names() {
        let mut fs = AppleDosFilesystem::open(edit_fixture(), 0).unwrap();
        let root = fs.root().unwrap();
        let mut src = Cursor::new(b"x".to_vec());
        let err = fs
            .create_file(&root, "HELLO", &mut src, 1, &CreateFileOptions::default())
            .unwrap_err();
        assert!(matches!(err, FilesystemError::InvalidData(_)));
    }

    #[test]
    fn create_file_rejects_oversize() {
        let mut fs = AppleDosFilesystem::open(edit_fixture(), 0).unwrap();
        let root = fs.root().unwrap();
        // 140 KB disk, only ~560 sectors free outside track 17.
        // Try 200 KB — must reject.
        let huge = vec![0u8; 200 * 1024];
        let mut src = Cursor::new(huge);
        let err = fs
            .create_file(
                &root,
                "TOOBIG",
                &mut src,
                (200 * 1024) as u64,
                &CreateFileOptions::default(),
            )
            .unwrap_err();
        assert!(matches!(err, FilesystemError::InvalidData(_)));
    }

    #[test]
    fn create_then_delete_returns_to_original_free_count() {
        let mut fs = AppleDosFilesystem::open(edit_fixture(), 0).unwrap();
        let root = fs.root().unwrap();
        let before = EditableFilesystem::free_space(&mut fs).unwrap();
        let mut src = Cursor::new(b"abc".to_vec());
        let fe = fs
            .create_file(&root, "TMP", &mut src, 3, &CreateFileOptions::default())
            .unwrap();
        let mid = EditableFilesystem::free_space(&mut fs).unwrap();
        assert_eq!(mid, before - 2 * APPLE_II_SECTOR_BYTES as u64);
        fs.delete_entry(&root, &fe).unwrap();
        let after = EditableFilesystem::free_space(&mut fs).unwrap();
        assert_eq!(after, before, "free count must return after create+delete");
    }

    #[test]
    fn create_directory_is_unsupported() {
        let mut fs = AppleDosFilesystem::open(edit_fixture(), 0).unwrap();
        let root = fs.root().unwrap();
        let err = fs
            .create_directory(&root, "SUB", &CreateDirectoryOptions::default())
            .unwrap_err();
        assert!(matches!(err, FilesystemError::Unsupported(_)));
    }
}
