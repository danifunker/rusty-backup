//! Commodore DOS (CBM DOS) filesystem — the floppy filesystem used by the
//! 1541 / 1571 / 1581 disk drives of the C64 / C128 / C16 / Plus-4 /
//! VIC-20 / PET (and the matching MiSTer cores).
//!
//! The host OS cannot mount these disks, so file extract + add + delete is
//! the whole point (Axis 1 of the MiSTer plan — floppy-only, no resize).
//!
//! ## Image families
//!
//! All CBM images are flat 256-byte-sector dumps. The geometry (sectors
//! per track) varies by track and by drive:
//!
//! | Variant   | Drive | Tracks | Bytes   | Dir track | Notes              |
//! |-----------|-------|--------|---------|-----------|--------------------|
//! | `.d64`    | 1541  | 35     | 174848  | 18        | also 40-track 196608 |
//! | `.d71`    | 1571  | 70     | 349696  | 18        | second BAM on T53  |
//! | `.d81`    | 1581  | 80     | 819200  | 40        | header + 2 BAM secs |
//!
//! ## On-disk layout (Schepers D64 spec; Immers/Neufeld "Inside CBM DOS")
//!
//! - **Sectors per track** (1541): tracks 1–17 = 21, 18–24 = 19,
//!   25–30 = 18, 31–35 = 17. The 1571 mirrors the 35-track map on the
//!   second side (tracks 36–70). The 1581 is a flat 40 sectors/track.
//! - **BAM** (Block Availability Map): on the 1541/1571 it shares the
//!   directory header sector (T18S0); the 1581 keeps a header at T40S0
//!   and two dedicated BAM sectors (T40S1 / T40S2). Bit **set = free**
//!   (the Amiga-style "set = free" convention — easy bug source).
//! - **Directory**: a chain of 256-byte sectors, each holding 8 file
//!   entries of 32 bytes. Bytes 0–1 of a directory sector link to the
//!   next sector (T=0 ends the chain). Per 32-byte entry:
//!
//! ```text
//! +0x00..0x01  next-dir T/S (only meaningful in the sector's entry 0)
//! +0x02        file type: low nibble 0=DEL 1=SEQ 2=PRG 3=USR 4=REL,
//!              bit 6 (0x40) locked, bit 7 (0x80) closed (normal files set it)
//! +0x03        track  of first data block
//! +0x04        sector of first data block
//! +0x05..0x14  filename, 16 bytes, PETSCII, padded with 0xA0
//! +0x15..0x16  first side-sector T/S (REL only)
//! +0x17        REL record length
//! +0x1E..0x1F  file size in 256-byte blocks (LE)
//! ```
//!
//! - **File data**: a linked chain of sectors. Bytes 0–1 of each block
//!   link to the next (T=0 ends the chain); bytes 2–255 are 254 data
//!   bytes. In the final block the sector-link byte holds the index of
//!   the last valid byte, so the exact file length is
//!   `(blocks - 1) * 254 + (last_link_sector - 1)`.

use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::FileEntry;
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};

/// CBM 256-byte logical sector.
pub const SECTOR_BYTES: usize = 256;
/// Data bytes per block (256 − 2 link bytes).
pub const DATA_PER_BLOCK: usize = 254;
/// Filename length on disk (PETSCII, 0xA0-padded).
pub const NAME_LEN: usize = 16;
/// PETSCII shifted-space pad byte.
const PAD: u8 = 0xA0;

/// Runaway guards (a single floppy can never chain this far).
const MAX_DIR_SECTORS: usize = 64;
const MAX_FILE_BLOCKS: usize = 4096;

/// Image geometry variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CbmVariant {
    /// 1541 single-sided, 35 tracks (174848 bytes).
    D64,
    /// 1541 40-track extension (196608 bytes).
    D64_40,
    /// 1571 double-sided, 70 tracks (349696 bytes).
    D71,
    /// 1581 3.5", 80 tracks × 40 sectors (819200 bytes).
    D81,
}

impl CbmVariant {
    /// Identify a variant from the exact image length. Returns `None`
    /// for any size that isn't a known CBM geometry. (Variants with
    /// trailing per-sector error bytes — 175531 / 197376 / 351062 — are
    /// accepted by trimming the error map.)
    pub fn from_image_len(len: u64) -> Option<CbmVariant> {
        match len {
            174_848 | 175_531 => Some(CbmVariant::D64),
            196_608 | 197_376 => Some(CbmVariant::D64_40),
            349_696 | 351_062 => Some(CbmVariant::D71),
            819_200 => Some(CbmVariant::D81),
            _ => None,
        }
    }

    /// Total track count.
    pub fn tracks(self) -> u8 {
        match self {
            CbmVariant::D64 => 35,
            CbmVariant::D64_40 => 40,
            CbmVariant::D71 => 70,
            CbmVariant::D81 => 80,
        }
    }

    /// Sectors on a given 1-based track.
    pub fn sectors_in_track(self, track: u8) -> u8 {
        match self {
            CbmVariant::D81 => 40,
            CbmVariant::D64 | CbmVariant::D64_40 | CbmVariant::D71 => {
                // 1541 zone map. The 1571 mirrors it onto side 2 (tracks
                // 36–70); the 40-track 1541 just extends the outermost
                // zone (tracks 36–40 stay at 17 sectors).
                let t = if self == CbmVariant::D71 && track > 35 {
                    track - 35
                } else {
                    track
                };
                match t {
                    1..=17 => 21,
                    18..=24 => 19,
                    25..=30 => 18,
                    _ => 17,
                }
            }
        }
    }

    /// Directory/header track.
    fn dir_track(self) -> u8 {
        match self {
            CbmVariant::D81 => 40,
            _ => 18,
        }
    }

    /// Total addressable sectors (= image length / 256).
    fn total_sectors(self) -> u32 {
        (1..=self.tracks())
            .map(|t| self.sectors_in_track(t) as u32)
            .sum()
    }

    /// Byte offset of the header (BAM) sector from the image start —
    /// T18S0 for the 1541/1571, T40S0 for the 1581.
    fn header_offset(self) -> u64 {
        let dir_track = self.dir_track();
        (1..dir_track)
            .map(|t| self.sectors_in_track(t) as u64)
            .sum::<u64>()
            * SECTOR_BYTES as u64
    }

    /// Friendly drive name for `fs_type`.
    fn drive_name(self) -> &'static str {
        match self {
            CbmVariant::D64 | CbmVariant::D64_40 => "CBM DOS (1541)",
            CbmVariant::D71 => "CBM DOS (1571)",
            CbmVariant::D81 => "CBM DOS (1581)",
        }
    }
}

/// Sniff whether the stream at `partition_offset` is a CBM DOS image.
///
/// CBM images are flat sector dumps with no partition table and weak
/// magic, so we gate on the **exact geometry length** *and* the
/// header-sector signature (DOS version byte + a directory-track link +
/// the DOS-type marker) to avoid false positives on same-sized opaque
/// blobs. Returns the detected geometry on a hit.
pub fn looks_like_cbm<R: Read + Seek>(reader: &mut R, partition_offset: u64) -> Option<CbmVariant> {
    let size = reader
        .seek(SeekFrom::End(0))
        .ok()?
        .checked_sub(partition_offset)?;
    let variant = CbmVariant::from_image_len(size)?;
    let hdr_off = partition_offset + variant.header_offset();
    reader.seek(SeekFrom::Start(hdr_off)).ok()?;
    let mut buf = [0u8; SECTOR_BYTES];
    reader.read_exact(&mut buf).ok()?;

    let dir_track = variant.dir_track();
    // The header links to the first directory sector on the dir track.
    if buf[0] != dir_track || buf[1] == 0 || buf[1] >= variant.sectors_in_track(dir_track) {
        return None;
    }
    let signature_ok = match variant {
        CbmVariant::D81 => {
            // DOS version 'D'; DOS-type "3D" at 0x19..0x1B.
            buf[0x02] == b'D' && buf[0x19] == b'3' && buf[0x1A] == b'D'
        }
        _ => {
            // DOS version 'A'; DOS-type "2x" at 0xA5..0xA7 (1541 = "2A",
            // some 1571 variants "2C"/"2D").
            buf[0x02] == b'A' && buf[0xA5] == b'2' && buf[0xA6].is_ascii_uppercase()
        }
    };
    signature_ok.then_some(variant)
}

/// CBM file type (low nibble of the type byte).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CbmFileType {
    Del,
    Seq,
    Prg,
    Usr,
    Rel,
    /// Unknown nibble (corrupt entry).
    Other(u8),
}

impl CbmFileType {
    fn from_nibble(n: u8) -> CbmFileType {
        match n & 0x0F {
            0 => CbmFileType::Del,
            1 => CbmFileType::Seq,
            2 => CbmFileType::Prg,
            3 => CbmFileType::Usr,
            4 => CbmFileType::Rel,
            other => CbmFileType::Other(other),
        }
    }

    fn nibble(self) -> u8 {
        match self {
            CbmFileType::Del => 0,
            CbmFileType::Seq => 1,
            CbmFileType::Prg => 2,
            CbmFileType::Usr => 3,
            CbmFileType::Rel => 4,
            CbmFileType::Other(n) => n & 0x0F,
        }
    }

    /// Three-letter label used by CBM directory listings.
    pub fn label(self) -> &'static str {
        match self {
            CbmFileType::Del => "DEL",
            CbmFileType::Seq => "SEQ",
            CbmFileType::Prg => "PRG",
            CbmFileType::Usr => "USR",
            CbmFileType::Rel => "REL",
            CbmFileType::Other(_) => "???",
        }
    }

    /// Map a host-file extension to a CBM type. Defaults to PRG (the
    /// overwhelmingly common case for C64 software).
    pub fn from_extension(name: &str) -> CbmFileType {
        match name.rsplit('.').next().map(|e| e.to_ascii_lowercase()) {
            Some(ref e) if e == "seq" || e == "txt" => CbmFileType::Seq,
            Some(ref e) if e == "usr" => CbmFileType::Usr,
            _ => CbmFileType::Prg,
        }
    }
}

/// Encode a host filename into the 16-byte 0xA0-padded PETSCII form.
///
/// CBM disks are uppercase-default PETSCII; for the printable ASCII range
/// 0x20..=0x5F the encodings coincide, so we pass those through (folding
/// lowercase ASCII to uppercase) and reject anything outside it. The CBM
/// reserved characters `,`/`*`/`?`/`@`/`:`/`=`/`$` are *permitted* in raw
/// names (the DOS uses them in patterns, not in stored names) but we keep
/// them as-is for fidelity.
pub fn encode_petscii_name(name: &str) -> Result<[u8; NAME_LEN], FilesystemError> {
    if name.is_empty() {
        return Err(FilesystemError::InvalidData("CBM filename is empty".into()));
    }
    if name.chars().count() > NAME_LEN {
        return Err(FilesystemError::InvalidData(format!(
            "CBM filename '{name}' exceeds {NAME_LEN} characters"
        )));
    }
    let mut out = [PAD; NAME_LEN];
    for (n, c) in name.chars().enumerate() {
        if !c.is_ascii() {
            return Err(FilesystemError::InvalidData(format!(
                "CBM filename '{name}' contains non-ASCII character '{c}'"
            )));
        }
        let b = (c as u8).to_ascii_uppercase();
        if !(0x20..=0x5F).contains(&b) {
            return Err(FilesystemError::InvalidData(format!(
                "CBM filename '{name}' contains unrepresentable byte 0x{b:02X}"
            )));
        }
        out[n] = b;
    }
    Ok(out)
}

/// Decode an on-disk 16-byte PETSCII name into a display string: trailing
/// 0xA0 padding stripped, printable ASCII passed through, shifted
/// uppercase (0xC1..=0xDA) folded down, everything else shown as '?'.
pub fn decode_petscii_name(raw: &[u8]) -> String {
    // Strip trailing 0xA0 pad.
    let end = raw.iter().rposition(|&b| b != PAD).map_or(0, |p| p + 1);
    let mut out = String::with_capacity(end);
    for &b in &raw[..end] {
        let c = match b {
            0x20..=0x5F => b as char,
            0xC1..=0xDA => (b - 0x80) as char,
            _ => '?',
        };
        out.push(c);
    }
    out
}

/// One parsed directory entry plus enough location info to rewrite it.
#[derive(Debug, Clone)]
pub struct CbmDirEntry {
    /// File type nibble.
    pub file_type: CbmFileType,
    /// Closed flag (bit 7 of the type byte) — clear means a "splat" file
    /// (improperly closed). We surface it but still read such files.
    pub closed: bool,
    /// Locked flag (bit 6 of the type byte).
    pub locked: bool,
    /// First data block (track, sector). (0,0) for an empty slot.
    pub first_track: u8,
    pub first_sector: u8,
    /// Raw 16-byte PETSCII name (for byte-exact compares / rewrites).
    pub raw_name: [u8; NAME_LEN],
    /// Decoded display name.
    pub name: String,
    /// Size in 256-byte blocks per the directory.
    pub size_blocks: u16,
    /// Location of this slot: (dir_track, dir_sector, index 0..8).
    pub slot: (u8, u8, usize),
}

impl CbmDirEntry {
    /// True when the slot holds a live (non-scratched) file. A scratched
    /// entry has type byte 0x00 (DEL with the closed bit clear).
    fn is_live(&self) -> bool {
        self.file_type != CbmFileType::Del || self.closed
    }
}

/// A CBM DOS filesystem over a flat 256-byte-sector image.
pub struct CbmFilesystem<R: Read + Seek + Send> {
    reader: R,
    partition_offset: u64,
    variant: CbmVariant,
    /// Track offsets: `track_offset[t]` = byte offset of (t+1, sector 0).
    track_offset: Vec<u64>,
    /// Disk name (decoded).
    disk_name: String,
    /// Disk id (2 chars, decoded best-effort).
    disk_id: String,
    /// Live directory entries (rebuilt after each mutation).
    entries: Vec<CbmDirEntry>,
}

impl<R: Read + Seek + Send> CbmFilesystem<R> {
    /// Open a CBM volume. `partition_offset` is 0 for a flat floppy
    /// image (the only shape these formats take).
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        let len = reader
            .seek(SeekFrom::End(0))?
            .checked_sub(partition_offset)
            .ok_or_else(|| FilesystemError::InvalidData("partition offset past EOF".into()))?;
        let variant = CbmVariant::from_image_len(len).ok_or_else(|| {
            FilesystemError::InvalidData(format!("{len} bytes is not a known CBM image geometry"))
        })?;
        Self::open_variant(reader, partition_offset, variant)
    }

    /// Open with the geometry already decided (used when the image has
    /// trailing error bytes that perturb the raw length).
    pub fn open_variant(
        reader: R,
        partition_offset: u64,
        variant: CbmVariant,
    ) -> Result<Self, FilesystemError> {
        // Precompute cumulative track byte offsets.
        let mut track_offset = Vec::with_capacity(variant.tracks() as usize + 1);
        let mut acc = 0u64;
        for t in 1..=variant.tracks() {
            track_offset.push(acc);
            acc += variant.sectors_in_track(t) as u64 * SECTOR_BYTES as u64;
        }
        track_offset.push(acc); // sentinel: end-of-image

        let mut fs = CbmFilesystem {
            reader,
            partition_offset,
            variant,
            track_offset,
            disk_name: String::new(),
            disk_id: String::new(),
            entries: Vec::new(),
        };
        fs.load_header()?;
        fs.refresh_entries()?;
        Ok(fs)
    }

    /// Consume the filesystem, returning the underlying reader (e.g. to
    /// recover the image bytes from a `Cursor`).
    pub fn into_inner(self) -> R {
        self.reader
    }

    /// Byte offset of (track, sector) within the partition.
    fn ts_offset(&self, track: u8, sector: u8) -> Result<u64, FilesystemError> {
        if track == 0 || track > self.variant.tracks() {
            return Err(FilesystemError::InvalidData(format!(
                "track {track} out of range 1..={}",
                self.variant.tracks()
            )));
        }
        if sector >= self.variant.sectors_in_track(track) {
            return Err(FilesystemError::InvalidData(format!(
                "sector {sector} out of range on track {track}"
            )));
        }
        Ok(self.partition_offset
            + self.track_offset[(track - 1) as usize]
            + sector as u64 * SECTOR_BYTES as u64)
    }

    fn read_sector(
        &mut self,
        track: u8,
        sector: u8,
    ) -> Result<[u8; SECTOR_BYTES], FilesystemError> {
        let off = self.ts_offset(track, sector)?;
        self.reader.seek(SeekFrom::Start(off))?;
        let mut buf = [0u8; SECTOR_BYTES];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Read the header (disk name + id) from the variant's header sector.
    fn load_header(&mut self) -> Result<(), FilesystemError> {
        let (name_off, id_off) = match self.variant {
            CbmVariant::D81 => (0x04usize, 0x16usize),
            _ => (0x90usize, 0xA2usize),
        };
        let dir_track = self.variant.dir_track();
        let hdr = self.read_sector(dir_track, 0)?;
        self.disk_name = decode_petscii_name(&hdr[name_off..name_off + NAME_LEN]);
        self.disk_id = decode_petscii_name(&hdr[id_off..id_off + 2]);
        Ok(())
    }

    /// (track, sector) of the first directory entry sector.
    fn dir_start(&mut self) -> Result<(u8, u8), FilesystemError> {
        let dir_track = self.variant.dir_track();
        let hdr = self.read_sector(dir_track, 0)?;
        // Bytes 0-1 of the header link to the first directory sector.
        let (t, s) = (hdr[0], hdr[1]);
        if t == 0 {
            // Fall back to the canonical defaults if the link is blank.
            Ok((
                dir_track,
                if self.variant == CbmVariant::D81 {
                    3
                } else {
                    1
                },
            ))
        } else {
            Ok((t, s))
        }
    }

    /// Walk the directory chain and rebuild `entries` (live files only).
    fn refresh_entries(&mut self) -> Result<(), FilesystemError> {
        let mut entries = Vec::new();
        let (mut track, mut sector) = self.dir_start()?;
        let mut seen = HashSet::new();
        for _ in 0..MAX_DIR_SECTORS {
            if track == 0 {
                break;
            }
            if !seen.insert((track, sector)) {
                return Err(FilesystemError::InvalidData(format!(
                    "directory chain cycle at T{track}S{sector}"
                )));
            }
            let buf = self.read_sector(track, sector)?;
            for i in 0..8 {
                let off = i * 32;
                let type_byte = buf[off + 0x02];
                let file_type = CbmFileType::from_nibble(type_byte);
                let closed = type_byte & 0x80 != 0;
                let locked = type_byte & 0x40 != 0;
                let mut raw_name = [0u8; NAME_LEN];
                raw_name.copy_from_slice(&buf[off + 0x05..off + 0x05 + NAME_LEN]);
                let entry = CbmDirEntry {
                    file_type,
                    closed,
                    locked,
                    first_track: buf[off + 0x03],
                    first_sector: buf[off + 0x04],
                    raw_name,
                    name: decode_petscii_name(&raw_name),
                    size_blocks: u16::from_le_bytes([buf[off + 0x1E], buf[off + 0x1F]]),
                    slot: (track, sector, i),
                };
                if entry.is_live() {
                    entries.push(entry);
                }
            }
            track = buf[0];
            sector = buf[1];
        }
        self.entries = entries;
        Ok(())
    }

    /// Read a file's full contents by walking its sector chain. Honors
    /// the final block's last-byte pointer for the exact length.
    fn read_chain(
        &mut self,
        first_track: u8,
        first_sector: u8,
    ) -> Result<Vec<u8>, FilesystemError> {
        let mut data = Vec::new();
        let (mut track, mut sector) = (first_track, first_sector);
        let mut seen = HashSet::new();
        for _ in 0..MAX_FILE_BLOCKS {
            if track == 0 {
                break;
            }
            if !seen.insert((track, sector)) {
                return Err(FilesystemError::InvalidData(format!(
                    "file chain cycle at T{track}S{sector}"
                )));
            }
            let buf = self.read_sector(track, sector)?;
            let next_track = buf[0];
            let next_sector = buf[1];
            if next_track == 0 {
                // Final block: sector-link byte is the index of the last
                // valid byte. Data spans offsets 2..=next_sector.
                let last = next_sector as usize;
                let end = last.clamp(1, SECTOR_BYTES - 1) + 1;
                data.extend_from_slice(&buf[2..end.max(2)]);
                break;
            }
            data.extend_from_slice(&buf[2..SECTOR_BYTES]);
            track = next_track;
            sector = next_sector;
        }
        Ok(data)
    }
}

impl<R: Read + Seek + Send> Filesystem for CbmFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if entry.path != "/" {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(self.entries.len());
        for (idx, e) in self.entries.iter().enumerate() {
            let approx = e.size_blocks as u64 * SECTOR_BYTES as u64;
            let mut fe =
                FileEntry::new_file(e.name.clone(), format!("/{}", e.name), approx, idx as u64);
            fe.special_type = Some(format!(
                "{}{}",
                e.file_type.label(),
                if e.locked { "<" } else { "" }
            ));
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
        let e = self
            .entries
            .get(idx)
            .ok_or_else(|| FilesystemError::NotFound(format!("entry idx {idx} out of range")))?
            .clone();
        if e.first_track == 0 {
            return Ok(Vec::new());
        }
        let mut data = self.read_chain(e.first_track, e.first_sector)?;
        data.truncate(max_bytes);
        Ok(data)
    }

    fn fs_type(&self) -> &str {
        self.variant.drive_name()
    }

    fn volume_label(&self) -> Option<&str> {
        if self.disk_name.is_empty() {
            None
        } else {
            Some(&self.disk_name)
        }
    }

    fn total_size(&self) -> u64 {
        self.variant.total_sectors() as u64 * SECTOR_BYTES as u64
    }

    fn used_size(&self) -> u64 {
        // Best-effort: total minus free; recomputed lazily via a BAM read
        // would need &mut, so approximate from the directory block counts.
        self.entries
            .iter()
            .map(|e| e.size_blocks as u64 * SECTOR_BYTES as u64)
            .sum()
    }
}

// ---------------------------------------------------------------------------
// Block Availability Map (write side)
// ---------------------------------------------------------------------------

/// In-memory BAM: per-track free count + bitmap (bit set = free).
struct Bam {
    variant: CbmVariant,
    /// Indexed by track-1. `free[t]` = free sector count on track t+1.
    free: Vec<u8>,
    /// Indexed by track-1. Up to 5 bitmap bytes (D81 has 40 sectors).
    bits: Vec<[u8; 5]>,
}

impl Bam {
    fn bitmap_bytes(variant: CbmVariant, track: u8) -> usize {
        variant.sectors_in_track(track).div_ceil(8) as usize
    }

    fn is_free(&self, track: u8, sector: u8) -> bool {
        let row = &self.bits[(track - 1) as usize];
        let byte = (sector / 8) as usize;
        let bit = sector % 8;
        (row[byte] >> bit) & 1 == 1
    }

    fn set(&mut self, track: u8, sector: u8, free: bool) {
        let was = self.is_free(track, sector);
        if was == free {
            return;
        }
        let row = &mut self.bits[(track - 1) as usize];
        let byte = (sector / 8) as usize;
        let bit = sector % 8;
        if free {
            row[byte] |= 1 << bit;
            self.free[(track - 1) as usize] = self.free[(track - 1) as usize].saturating_add(1);
        } else {
            row[byte] &= !(1 << bit);
            self.free[(track - 1) as usize] = self.free[(track - 1) as usize].saturating_sub(1);
        }
    }

    /// Sum of free sectors that file data may actually use — i.e. the
    /// CBM "blocks free" figure. The directory track (and the 1571's
    /// second-side BAM track) are reserved for directory growth and never
    /// counted, matching what real CBM DOS reports.
    fn total_free(&self) -> u64 {
        let dir = self.variant.dir_track();
        self.free
            .iter()
            .enumerate()
            .filter(|(i, _)| {
                let t = *i as u8 + 1;
                t != dir && !(self.variant == CbmVariant::D71 && t == 53)
            })
            .map(|(_, &f)| f as u64)
            .sum()
    }
}

impl<R: Read + Write + Seek + Send> CbmFilesystem<R> {
    fn write_sector(
        &mut self,
        track: u8,
        sector: u8,
        data: &[u8; SECTOR_BYTES],
    ) -> Result<(), FilesystemError> {
        let off = self.ts_offset(track, sector)?;
        self.reader.seek(SeekFrom::Start(off))?;
        self.reader.write_all(data)?;
        Ok(())
    }

    /// Read the BAM into memory.
    fn read_bam(&mut self) -> Result<Bam, FilesystemError> {
        let variant = self.variant;
        let tracks = variant.tracks();
        let mut free = vec![0u8; tracks as usize];
        let mut bits = vec![[0u8; 5]; tracks as usize];

        match variant {
            CbmVariant::D64 | CbmVariant::D64_40 | CbmVariant::D71 => {
                let b18 = self.read_sector(18, 0)?;
                // Tracks 1..=35 (or 1..=40 for D64_40) at offset 4 + (t-1)*4.
                let first_block_tracks = if variant == CbmVariant::D71 {
                    35
                } else {
                    tracks
                };
                for t in 1..=first_block_tracks {
                    let o = 4 + (t as usize - 1) * 4;
                    free[t as usize - 1] = b18[o];
                    let nb = Bam::bitmap_bytes(variant, t);
                    bits[t as usize - 1][..nb].copy_from_slice(&b18[o + 1..o + 1 + nb]);
                }
                if variant == CbmVariant::D71 {
                    // Tracks 36..=70: free counts in T18S0[0xDD..], bitmaps
                    // in T53S0 (3 bytes per track).
                    let b53 = self.read_sector(53, 0)?;
                    for t in 36..=70u8 {
                        free[t as usize - 1] = b18[0xDD + (t as usize - 36)];
                        let o = (t as usize - 36) * 3;
                        bits[t as usize - 1][..3].copy_from_slice(&b53[o..o + 3]);
                    }
                }
            }
            CbmVariant::D81 => {
                // Two BAM sectors: T40S1 (tracks 1..=40), T40S2 (41..=80),
                // 6 bytes per track at offset 0x10.
                let b1 = self.read_sector(40, 1)?;
                let b2 = self.read_sector(40, 2)?;
                for t in 1..=80u8 {
                    let (blk, base_t) = if t <= 40 { (&b1, 1u8) } else { (&b2, 41u8) };
                    let o = 0x10 + (t - base_t) as usize * 6;
                    free[t as usize - 1] = blk[o];
                    bits[t as usize - 1][..5].copy_from_slice(&blk[o + 1..o + 6]);
                }
            }
        }
        Ok(Bam {
            variant,
            free,
            bits,
        })
    }

    /// Write the BAM back to its sector(s).
    fn write_bam(&mut self, bam: &Bam) -> Result<(), FilesystemError> {
        match self.variant {
            CbmVariant::D64 | CbmVariant::D64_40 | CbmVariant::D71 => {
                let mut b18 = self.read_sector(18, 0)?;
                let first_block_tracks = if self.variant == CbmVariant::D71 {
                    35
                } else {
                    self.variant.tracks()
                };
                for t in 1..=first_block_tracks {
                    let o = 4 + (t as usize - 1) * 4;
                    b18[o] = bam.free[t as usize - 1];
                    let nb = Bam::bitmap_bytes(self.variant, t);
                    b18[o + 1..o + 1 + nb].copy_from_slice(&bam.bits[t as usize - 1][..nb]);
                }
                if self.variant == CbmVariant::D71 {
                    let mut b53 = self.read_sector(53, 0)?;
                    for t in 36..=70u8 {
                        b18[0xDD + (t as usize - 36)] = bam.free[t as usize - 1];
                        let o = (t as usize - 36) * 3;
                        b53[o..o + 3].copy_from_slice(&bam.bits[t as usize - 1][..3]);
                    }
                    self.write_sector(53, 0, &b53)?;
                }
                self.write_sector(18, 0, &b18)?;
            }
            CbmVariant::D81 => {
                let mut b1 = self.read_sector(40, 1)?;
                let mut b2 = self.read_sector(40, 2)?;
                for t in 1..=80u8 {
                    let (blk, base_t) = if t <= 40 {
                        (&mut b1, 1u8)
                    } else {
                        (&mut b2, 41u8)
                    };
                    let o = 0x10 + (t - base_t) as usize * 6;
                    blk[o] = bam.free[t as usize - 1];
                    blk[o + 1..o + 6].copy_from_slice(&bam.bits[t as usize - 1][..5]);
                }
                self.write_sector(40, 1, &b1)?;
                self.write_sector(40, 2, &b2)?;
            }
        }
        Ok(())
    }

    /// Allocation order: spiral outward from the directory track, the way
    /// real CBM DOS keeps data near the directory. The directory track
    /// itself (and the 1571's second BAM track) are skipped — their
    /// sectors are already marked used in the BAM, but skipping keeps
    /// data off them entirely.
    fn alloc_track_order(&self) -> Vec<u8> {
        let dir = self.variant.dir_track();
        let tracks = self.variant.tracks();
        let mut order = Vec::with_capacity(tracks as usize);
        let mut d = 1i16;
        loop {
            let up = dir as i16 + d;
            let down = dir as i16 - d;
            let mut pushed = false;
            if down >= 1 {
                order.push(down as u8);
                pushed = true;
            }
            if up <= tracks as i16 {
                order.push(up as u8);
                pushed = true;
            }
            if !pushed {
                break;
            }
            d += 1;
        }
        order
            .into_iter()
            .filter(|&t| t != dir && !(self.variant == CbmVariant::D71 && t == 53))
            .collect()
    }

    /// Allocate one free sector, preferring `interleave` sectors past
    /// `near` on the same track for realistic clustering. Marks it used
    /// in the BAM. Returns `(track, sector)`.
    fn alloc_sector(
        &mut self,
        bam: &mut Bam,
        near: Option<(u8, u8)>,
        interleave: u8,
    ) -> Result<(u8, u8), FilesystemError> {
        // Prefer staying on the same track as `near`.
        let mut tracks: Vec<u8> = Vec::new();
        if let Some((nt, _)) = near {
            tracks.push(nt);
        }
        tracks.extend(self.alloc_track_order());

        for t in tracks {
            let spt = self.variant.sectors_in_track(t);
            // Pick a starting sector honoring interleave from `near`.
            let start = match near {
                Some((nt, ns)) if nt == t => (ns + interleave) % spt,
                _ => 0,
            };
            for off in 0..spt {
                let s = (start + off) % spt;
                if bam.is_free(t, s) {
                    bam.set(t, s, false);
                    return Ok((t, s));
                }
            }
        }
        Err(FilesystemError::DiskFull(
            "no free sectors left on the volume".into(),
        ))
    }

    /// Find a free directory slot, extending the directory chain by one
    /// sector when every existing slot is taken. Returns
    /// `(dir_track, dir_sector, index)`.
    fn find_or_make_dir_slot(&mut self, bam: &mut Bam) -> Result<(u8, u8, usize), FilesystemError> {
        let (mut track, mut sector) = self.dir_start()?;
        let mut seen = HashSet::new();
        for _ in 0..MAX_DIR_SECTORS {
            let buf = self.read_sector(track, sector)?;
            for i in 0..8 {
                let off = i * 32;
                let type_byte = buf[off + 0x02];
                // A slot is reusable if scratched (type byte 0) or never
                // used (whole entry blank).
                if type_byte == 0 {
                    return Ok((track, sector, i));
                }
            }
            let next_t = buf[0];
            let next_s = buf[1];
            if next_t == 0 {
                // Directory full — append a new sector on the dir track.
                let dir_track = self.variant.dir_track();
                let (nt, ns) = {
                    // Allocate from the directory track specifically.
                    let spt = self.variant.sectors_in_track(dir_track);
                    let mut chosen = None;
                    for s in 0..spt {
                        if bam.is_free(dir_track, s) {
                            bam.set(dir_track, s, false);
                            chosen = Some((dir_track, s));
                            break;
                        }
                    }
                    chosen.ok_or_else(|| {
                        FilesystemError::DiskFull(
                            "directory track is full — cannot extend the directory".into(),
                        )
                    })?
                };
                // Link the current last sector to the new one.
                let mut cur = buf;
                cur[0] = nt;
                cur[1] = ns;
                self.write_sector(track, sector, &cur)?;
                // Initialize the new directory sector: empty, end-of-chain.
                let mut fresh = [0u8; SECTOR_BYTES];
                fresh[0] = 0x00;
                fresh[1] = 0xFF;
                self.write_sector(nt, ns, &fresh)?;
                return Ok((nt, ns, 0));
            }
            if !seen.insert((next_t, next_s)) {
                return Err(FilesystemError::InvalidData(
                    "directory chain cycle while seeking a free slot".into(),
                ));
            }
            track = next_t;
            sector = next_s;
        }
        Err(FilesystemError::DiskFull(
            "directory chain exceeds the sane sector cap".into(),
        ))
    }
}

impl<R: Read + Write + Seek + Send> EditableFilesystem for CbmFilesystem<R> {
    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn std::io::Read,
        data_len: u64,
        options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        if parent.path != "/" {
            return Err(FilesystemError::Unsupported(
                "CBM DOS directories are flat; only the root accepts files".into(),
            ));
        }
        let raw_name = encode_petscii_name(name)?;
        if self.entries.iter().any(|e| e.raw_name == raw_name) {
            return Err(FilesystemError::AlreadyExists(format!(
                "CBM file '{name}' already exists"
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

        // Decide the file type: explicit option overrides extension.
        let ftype = match options.type_code.as_deref() {
            Some("PRG") => CbmFileType::Prg,
            Some("SEQ") => CbmFileType::Seq,
            Some("USR") => CbmFileType::Usr,
            _ => CbmFileType::from_extension(name),
        };

        let mut bam = self.read_bam()?;

        // A zero-length file still occupies one block (CBM convention).
        let blocks_needed = payload.len().div_ceil(DATA_PER_BLOCK).max(1);
        if (blocks_needed as u64) > bam.total_free() {
            return Err(FilesystemError::DiskFull(format!(
                "need {blocks_needed} blocks, only {} free",
                bam.total_free()
            )));
        }

        // Allocate the chain, interleave 10 (1541 file interleave).
        let interleave = if self.variant == CbmVariant::D81 {
            1
        } else {
            10
        };
        let mut chain: Vec<(u8, u8)> = Vec::with_capacity(blocks_needed);
        let mut near = None;
        for _ in 0..blocks_needed {
            let ts = self.alloc_sector(&mut bam, near, interleave)?;
            chain.push(ts);
            near = Some(ts);
        }

        // Write the data blocks with forward links.
        for (i, &(t, s)) in chain.iter().enumerate() {
            let mut block = [0u8; SECTOR_BYTES];
            let data_off = i * DATA_PER_BLOCK;
            let data_end = (data_off + DATA_PER_BLOCK).min(payload.len());
            let chunk = if data_off < payload.len() {
                &payload[data_off..data_end]
            } else {
                &[][..]
            };
            block[2..2 + chunk.len()].copy_from_slice(chunk);
            if i + 1 < chain.len() {
                let (nt, ns) = chain[i + 1];
                block[0] = nt;
                block[1] = ns;
            } else {
                // Final block: track-link 0, sector-link = last valid index.
                block[0] = 0;
                block[1] = (chunk.len() + 1).max(1) as u8;
            }
            self.write_sector(t, s, &block)?;
        }

        // Claim a directory slot (may extend the chain, consuming BAM).
        let (dt, ds, slot) = self.find_or_make_dir_slot(&mut bam)?;
        let mut dir = self.read_sector(dt, ds)?;
        let off = slot * 32;
        // Type byte: closed (0x80) | optional lock | nibble.
        dir[off + 0x02] = 0x80 | ftype.nibble();
        dir[off + 0x03] = chain[0].0;
        dir[off + 0x04] = chain[0].1;
        dir[off + 0x05..off + 0x05 + NAME_LEN].copy_from_slice(&raw_name);
        // Clear REL / GEOS bytes in the slot.
        for b in &mut dir[off + 0x15..off + 0x1E] {
            *b = 0;
        }
        let size_blocks = blocks_needed as u16;
        dir[off + 0x1E] = (size_blocks & 0xFF) as u8;
        dir[off + 0x1F] = (size_blocks >> 8) as u8;
        self.write_sector(dt, ds, &dir)?;

        self.write_bam(&bam)?;
        self.reader.flush()?;
        self.refresh_entries()?;

        let idx = self
            .entries
            .iter()
            .position(|e| e.raw_name == raw_name)
            .ok_or_else(|| {
                FilesystemError::InvalidData(
                    "internal: created file not visible after refresh".into(),
                )
            })?;
        let mut fe = FileEntry::new_file(
            self.entries[idx].name.clone(),
            format!("/{}", self.entries[idx].name),
            payload.len() as u64,
            idx as u64,
        );
        fe.special_type = Some(ftype.label().to_string());
        Ok(fe)
    }

    fn create_directory(
        &mut self,
        _parent: &FileEntry,
        _name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "CBM DOS is a flat filesystem — no subdirectories".into(),
        ))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        if parent.path != "/" {
            return Err(FilesystemError::Unsupported(
                "CBM DOS is flat; only root files are deletable".into(),
            ));
        }
        let idx = entry.location as usize;
        let de =
            self.entries.get(idx).cloned().ok_or_else(|| {
                FilesystemError::NotFound(format!("entry idx {idx} out of range"))
            })?;

        let mut bam = self.read_bam()?;

        // Free every block in the file's chain.
        if de.first_track != 0 {
            let (mut t, mut s) = (de.first_track, de.first_sector);
            let mut seen = HashSet::new();
            for _ in 0..MAX_FILE_BLOCKS {
                if t == 0 {
                    break;
                }
                if !seen.insert((t, s)) {
                    return Err(FilesystemError::InvalidData(
                        "delete hit a file-chain cycle".into(),
                    ));
                }
                let buf = self.read_sector(t, s)?;
                bam.set(t, s, true);
                let nt = buf[0];
                let ns = buf[1];
                t = nt;
                s = ns;
            }
        }

        // Scratch the directory slot: type byte = 0.
        let (dt, ds, slot) = de.slot;
        let mut dir = self.read_sector(dt, ds)?;
        dir[slot * 32 + 0x02] = 0x00;
        self.write_sector(dt, ds, &dir)?;

        self.write_bam(&bam)?;
        self.reader.flush()?;
        self.refresh_entries()?;
        Ok(())
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.reader.flush()?;
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        let bam = self.read_bam()?;
        Ok(bam.total_free() * SECTOR_BYTES as u64)
    }
}

/// Build a blank, freshly-formatted CBM image in memory. Mirrors what
/// `c1541 format` / the Python `d64` library produce: a BAM with every
/// data sector free, the directory header sector and first directory
/// sector reserved, and a single empty directory sector. `name` is the
/// disk name (≤ 16 chars), `id` the 2-character disk ID.
///
/// This is the single source of truth for the on-disk format layout used
/// by tests, the `rb-cli new` path, and external-oracle cross-checks.
pub fn create_blank(variant: CbmVariant, name: &str, id: &str) -> Result<Vec<u8>, FilesystemError> {
    let total = variant.total_sectors() as usize * SECTOR_BYTES;
    let mut img = vec![0u8; total];

    // Helper: byte offset of (track, sector).
    let ts_off = |track: u8, sector: u8| -> usize {
        let mut acc = 0usize;
        for t in 1..track {
            acc += variant.sectors_in_track(t) as usize * SECTOR_BYTES;
        }
        acc + sector as usize * SECTOR_BYTES
    };

    let dir_track = variant.dir_track();
    let first_dir_sector = if variant == CbmVariant::D81 { 3 } else { 1 };

    // Mark all sectors free in a working bitmap, then reserve the
    // structural ones.
    let mut bam = Bam {
        variant,
        free: (1..=variant.tracks())
            .map(|t| variant.sectors_in_track(t))
            .collect(),
        bits: {
            let mut v = vec![[0u8; 5]; variant.tracks() as usize];
            for t in 1..=variant.tracks() {
                let spt = variant.sectors_in_track(t);
                for s in 0..spt {
                    v[(t - 1) as usize][(s / 8) as usize] |= 1 << (s % 8);
                }
            }
            v
        },
    };
    // Reserve header + BAM + first directory sector.
    bam.set(dir_track, 0, false);
    bam.set(dir_track, first_dir_sector, false);
    if variant == CbmVariant::D71 {
        bam.set(53, 0, false);
    }
    if variant == CbmVariant::D81 {
        bam.set(40, 1, false);
        bam.set(40, 2, false);
    }

    // Write the header sector.
    let hdr = ts_off(dir_track, 0);
    img[hdr] = dir_track;
    img[hdr + 1] = first_dir_sector;
    let raw_name = encode_petscii_name(name)?;
    let mut raw_id = [PAD; 2];
    for (i, b) in id.bytes().take(2).enumerate() {
        raw_id[i] = b;
    }
    match variant {
        CbmVariant::D81 => {
            img[hdr + 0x02] = b'D';
            img[hdr + 0x04..hdr + 0x04 + NAME_LEN].copy_from_slice(&raw_name);
            img[hdr + 0x14] = PAD;
            img[hdr + 0x15] = PAD;
            img[hdr + 0x16] = raw_id[0];
            img[hdr + 0x17] = raw_id[1];
            img[hdr + 0x18] = PAD;
            img[hdr + 0x19] = b'3';
            img[hdr + 0x1A] = b'D';
        }
        _ => {
            img[hdr + 0x02] = b'A';
            img[hdr + 0x90..hdr + 0x90 + NAME_LEN].copy_from_slice(&raw_name);
            img[hdr + 0xA0] = PAD;
            img[hdr + 0xA1] = PAD;
            img[hdr + 0xA2] = raw_id[0];
            img[hdr + 0xA3] = raw_id[1];
            img[hdr + 0xA4] = PAD;
            img[hdr + 0xA5] = b'2';
            img[hdr + 0xA6] = b'A';
            img[hdr + 0xA7] = PAD;
            img[hdr + 0xA8] = PAD;
        }
    }

    // Empty first directory sector: end-of-chain.
    let d0 = ts_off(dir_track, first_dir_sector);
    img[d0] = 0x00;
    img[d0 + 1] = 0xFF;

    // Serialize the BAM into the image via a throwaway filesystem.
    // (Simplest: write the bitmap bytes directly to mirror write_bam.)
    match variant {
        CbmVariant::D64 | CbmVariant::D64_40 | CbmVariant::D71 => {
            let first_block_tracks = if variant == CbmVariant::D71 {
                35
            } else {
                variant.tracks()
            };
            for t in 1..=first_block_tracks {
                let o = hdr + 4 + (t as usize - 1) * 4;
                img[o] = bam.free[t as usize - 1];
                let nb = Bam::bitmap_bytes(variant, t);
                img[o + 1..o + 1 + nb].copy_from_slice(&bam.bits[t as usize - 1][..nb]);
            }
            if variant == CbmVariant::D71 {
                let b53 = ts_off(53, 0);
                for t in 36..=70u8 {
                    img[hdr + 0xDD + (t as usize - 36)] = bam.free[t as usize - 1];
                    let o = (t as usize - 36) * 3;
                    img[b53 + o..b53 + o + 3].copy_from_slice(&bam.bits[t as usize - 1][..3]);
                }
                img[hdr + 0x03] = 0x80; // double-sided flag
            }
        }
        CbmVariant::D81 => {
            let b1 = ts_off(40, 1);
            let b2 = ts_off(40, 2);
            // BAM sector headers link S1 -> S2.
            img[b1] = 40;
            img[b1 + 1] = 2;
            img[b1 + 2] = b'D';
            img[b1 + 4] = raw_id[0];
            img[b1 + 5] = raw_id[1];
            img[b2 + 2] = b'D';
            img[b2 + 4] = raw_id[0];
            img[b2 + 5] = raw_id[1];
            for t in 1..=80u8 {
                let (base, base_t) = if t <= 40 { (b1, 1u8) } else { (b2, 41u8) };
                let o = base + 0x10 + (t - base_t) as usize * 6;
                img[o] = bam.free[t as usize - 1];
                img[o + 1..o + 6].copy_from_slice(&bam.bits[t as usize - 1][..5]);
            }
        }
    }

    Ok(img)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn blank_image(variant: CbmVariant, name: &str, id: &str) -> Vec<u8> {
        create_blank(variant, name, id).expect("create_blank")
    }

    #[test]
    fn variant_geometry_matches_image_sizes() {
        assert_eq!(CbmVariant::D64.total_sectors(), 683);
        assert_eq!(CbmVariant::D64_40.total_sectors(), 768);
        assert_eq!(CbmVariant::D71.total_sectors(), 1366);
        assert_eq!(CbmVariant::D81.total_sectors(), 3200);
        assert_eq!(CbmVariant::from_image_len(174_848), Some(CbmVariant::D64));
        assert_eq!(CbmVariant::from_image_len(349_696), Some(CbmVariant::D71));
        assert_eq!(CbmVariant::from_image_len(819_200), Some(CbmVariant::D81));
        assert_eq!(CbmVariant::from_image_len(123), None);
    }

    #[test]
    fn petscii_name_round_trips() {
        let raw = encode_petscii_name("HELLO").unwrap();
        assert_eq!(&raw[..5], b"HELLO");
        assert_eq!(raw[5], PAD);
        assert_eq!(decode_petscii_name(&raw), "HELLO");
        // lowercase folds to uppercase
        let raw2 = encode_petscii_name("game").unwrap();
        assert_eq!(decode_petscii_name(&raw2), "GAME");
    }

    fn open_mem(img: Vec<u8>) -> CbmFilesystem<Cursor<Vec<u8>>> {
        CbmFilesystem::open(Cursor::new(img), 0).expect("open")
    }

    #[test]
    fn blank_d64_has_no_files_and_full_free() {
        let img = blank_image(CbmVariant::D64, "TESTDISK", "XY");
        let mut fs = open_mem(img);
        assert_eq!(fs.volume_label(), Some("TESTDISK"));
        let root = fs.root().unwrap();
        assert!(fs.list_directory(&root).unwrap().is_empty());
        // 664 blocks free on a fresh 1541 disk (683 − 18 dir track − 1 BAM).
        let free = fs.free_space().unwrap();
        assert_eq!(free, 664 * SECTOR_BYTES as u64);
    }

    #[test]
    fn create_read_delete_round_trip_d64() {
        let img = blank_image(CbmVariant::D64, "TESTDISK", "XY");
        let mut fs = open_mem(img);
        let root = fs.root().unwrap();

        let payload = b"\x01\x08HELLO WORLD\r".to_vec();
        let mut cur = Cursor::new(payload.clone());
        let opts = CreateFileOptions::default();
        fs.create_file(&root, "HELLO", &mut cur, payload.len() as u64, &opts)
            .unwrap();

        // A 768-byte SEQ-ish payload spanning 4 blocks.
        let big: Vec<u8> = (0..768).map(|i| (i % 256) as u8).collect();
        let mut cur2 = Cursor::new(big.clone());
        let opts2 = CreateFileOptions {
            type_code: Some("SEQ".into()),
            ..Default::default()
        };
        fs.create_file(&root, "DATA", &mut cur2, big.len() as u64, &opts2)
            .unwrap();

        let listing = fs.list_directory(&root).unwrap();
        assert_eq!(listing.len(), 2);
        let names: Vec<&str> = listing.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"HELLO"));
        assert!(names.contains(&"DATA"));

        // Read both back byte-exact.
        let hello = listing.iter().find(|e| e.name == "HELLO").unwrap();
        assert_eq!(fs.read_file(hello, usize::MAX).unwrap(), payload);
        let data = listing.iter().find(|e| e.name == "DATA").unwrap();
        assert_eq!(fs.read_file(data, usize::MAX).unwrap(), big);

        // The PRG type was inferred for HELLO; SEQ requested for DATA.
        assert_eq!(hello.special_type.as_deref(), Some("PRG"));
        assert_eq!(data.special_type.as_deref(), Some("SEQ"));

        // Delete HELLO; DATA survives, free space recovers.
        let free_before = fs.free_space().unwrap();
        fs.delete_entry(&root, hello).unwrap();
        let listing = fs.list_directory(&root).unwrap();
        assert_eq!(listing.len(), 1);
        assert_eq!(listing[0].name, "DATA");
        let data = &listing[0].clone();
        assert_eq!(fs.read_file(data, usize::MAX).unwrap(), big);
        assert!(fs.free_space().unwrap() > free_before);
    }

    #[test]
    fn duplicate_name_rejected() {
        let img = blank_image(CbmVariant::D64, "DISK", "01");
        let mut fs = open_mem(img);
        let root = fs.root().unwrap();
        let mut c = Cursor::new(vec![1u8; 10]);
        fs.create_file(&root, "ONE", &mut c, 10, &CreateFileOptions::default())
            .unwrap();
        let mut c2 = Cursor::new(vec![2u8; 10]);
        let err = fs
            .create_file(&root, "ONE", &mut c2, 10, &CreateFileOptions::default())
            .unwrap_err();
        assert!(matches!(err, FilesystemError::AlreadyExists(_)));
    }

    #[test]
    fn directory_extends_past_eight_files() {
        let img = blank_image(CbmVariant::D64, "DISK", "01");
        let mut fs = open_mem(img);
        let root = fs.root().unwrap();
        for i in 0..20 {
            let name = format!("FILE{i:02}");
            let mut c = Cursor::new(vec![i as u8; 5]);
            fs.create_file(&root, &name, &mut c, 5, &CreateFileOptions::default())
                .unwrap();
        }
        let listing = fs.list_directory(&root).unwrap();
        assert_eq!(listing.len(), 20);
        // Re-open from the underlying bytes to prove persistence.
        let bytes = fs.reader.into_inner();
        let mut fs2 = CbmFilesystem::open(Cursor::new(bytes), 0).unwrap();
        let root2 = fs2.root().unwrap();
        assert_eq!(fs2.list_directory(&root2).unwrap().len(), 20);
    }

    #[test]
    fn round_trip_d71_and_d81() {
        for variant in [CbmVariant::D71, CbmVariant::D81] {
            let img = blank_image(variant, "DISK", "01");
            let mut fs = open_mem(img);
            let root = fs.root().unwrap();
            let payload: Vec<u8> = (0..1000).map(|i| (i * 7 % 256) as u8).collect();
            let mut c = Cursor::new(payload.clone());
            fs.create_file(
                &root,
                "BIGFILE",
                &mut c,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
            let listing = fs.list_directory(&root).unwrap();
            assert_eq!(listing.len(), 1, "variant {variant:?}");
            assert_eq!(
                fs.read_file(&listing[0], usize::MAX).unwrap(),
                payload,
                "variant {variant:?}"
            );
        }
    }
}
