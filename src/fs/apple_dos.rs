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

use std::io::{Read, Seek, SeekFrom};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};
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
}
