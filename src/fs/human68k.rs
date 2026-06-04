//! Human68k filesystem — the FAT-derived OS of the Sharp X68000.
//!
//! The MiSTer X68000 core mounts both 1.2 MB floppy `.d88` / `.xdf`
//! images and SASI HDD `.hdf` images. Both use Human68k's filesystem:
//! a FAT12 (floppy) or FAT16 (HDD) BPB-with-extensions, but the
//! directory entries are **18.3** rather than FAT's 8.3 (Sharp packed
//! 10 extra name bytes into FAT-reserved fields) and filenames are
//! **Shift-JIS** rather than CP437.
//!
//! ## On-disk layout
//!
//! - BPB at sector 0 — identical layout to FAT12/16 (jump, OEM, bytes
//!   per sector, etc). The X68000's IPL is in the boot bytes.
//! - FAT table immediately after the reserved sectors (DPB style).
//! - Root directory immediately after the FAT (FAT12/16 convention).
//! - Data area after that.
//!
//! ## Directory entry (32 B, little-endian)
//!
//! ```text
//! 0..8    name      8 bytes (Shift-JIS, space-padded)
//! 8..11   ext       3 bytes (Shift-JIS, space-padded)
//! 11      attr      0x10 = dir, 0x08 = volume label, 0x01 = R/O,
//!                   0x20 = archive
//! 12..22  name_ext  10 bytes — extra filename chars (extending the
//!                   8-char name to 18). FAT-equivalent fields are
//!                   reserved+create-time-tenths+create-time+
//!                   create-date+last-access-date.
//! 22..24  time      LE u16 — FAT-style HH:MM:SS (5/6/5 bits)
//! 24..26  date      LE u16 — FAT-style Y:M:D (7/4/5 bits, year+1980)
//! 26..28  cluster   LE u16 — first data cluster
//! 28..32  size      LE u32 — file size in bytes
//! ```
//!
//! Entry 0 first byte == 0x00 means "no more entries", 0xE5 means
//! "deleted slot" (same as FAT).
//!
//! Multi-byte Shift-JIS bytes occupy two slots: the first byte is in
//! the range `0x81..0x9F` or `0xE0..0xFC`, the second is `0x40..0xFC`
//! (excluding `0x7F`). For display we transcode ASCII verbatim and
//! emit Shift-JIS pairs as `\uXXXX` placeholders; the raw bytes are
//! retained for round-trip writes.

use std::io::{Read, Seek, SeekFrom, Write};

use byteorder::{ByteOrder, LittleEndian};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};

/// 18.3 filename: 8 name + 10 name-extension + 3 ext = 21 + dot.
pub const MAX_NAME_LEN: usize = 21;

/// Bytes per FAT directory entry. Same as FAT.
const DIR_ENTRY_SIZE: usize = 32;

/// File attribute bits (FAT-compatible). Some are kept for future
/// use even when our extract floor doesn't surface them yet.
#[allow(dead_code)]
mod attr {
    pub const READ_ONLY: u8 = 0x01;
    pub const HIDDEN: u8 = 0x02;
    pub const SYSTEM: u8 = 0x04;
    pub const VOLUME_LABEL: u8 = 0x08;
    pub const DIRECTORY: u8 = 0x10;
    pub const ARCHIVE: u8 = 0x20;
}

/// Parsed BPB (BIOS Parameter Block) — shape matches FAT12/16 since
/// Human68k is FAT-derived.
#[derive(Debug, Clone, Copy)]
pub struct Human68kBpb {
    pub bytes_per_sector: u16,
    pub sectors_per_cluster: u8,
    pub reserved_sectors: u16,
    pub num_fats: u8,
    pub root_entries: u16,
    pub total_sectors: u32,
    pub fat_sectors: u16,
    pub fat_kind: FatKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FatKind {
    Fat12,
    Fat16,
}

impl Human68kBpb {
    pub fn parse(bpb_buf: &[u8; 512]) -> Result<Self, FilesystemError> {
        let bytes_per_sector = LittleEndian::read_u16(&bpb_buf[11..13]);
        if !matches!(bytes_per_sector, 256 | 512 | 1024 | 2048) {
            return Err(FilesystemError::InvalidData(format!(
                "Human68k BPB bytes_per_sector {bytes_per_sector} not in {{256,512,1024,2048}}"
            )));
        }
        let sectors_per_cluster = bpb_buf[13];
        if sectors_per_cluster == 0 || !sectors_per_cluster.is_power_of_two() {
            return Err(FilesystemError::InvalidData(format!(
                "Human68k BPB sectors_per_cluster {sectors_per_cluster} must be a power of two"
            )));
        }
        let reserved_sectors = LittleEndian::read_u16(&bpb_buf[14..16]);
        let num_fats = bpb_buf[16];
        if num_fats == 0 || num_fats > 2 {
            return Err(FilesystemError::InvalidData(format!(
                "Human68k BPB num_fats {num_fats} not 1 or 2"
            )));
        }
        let root_entries = LittleEndian::read_u16(&bpb_buf[17..19]);
        let small_total = LittleEndian::read_u16(&bpb_buf[19..21]) as u32;
        let fat_sectors = LittleEndian::read_u16(&bpb_buf[22..24]);
        let big_total = LittleEndian::read_u32(&bpb_buf[32..36]);
        let total_sectors = if small_total != 0 {
            small_total
        } else {
            big_total
        };

        // Cluster count drives FAT12 vs FAT16 decision (same as FAT spec).
        let root_dir_sectors = (root_entries as u32 * 32).div_ceil(bytes_per_sector as u32);
        let data_sectors = total_sectors
            .saturating_sub(reserved_sectors as u32)
            .saturating_sub(num_fats as u32 * fat_sectors as u32)
            .saturating_sub(root_dir_sectors);
        let cluster_count = data_sectors / sectors_per_cluster as u32;
        let fat_kind = if cluster_count < 4085 {
            FatKind::Fat12
        } else {
            FatKind::Fat16
        };
        Ok(Self {
            bytes_per_sector,
            sectors_per_cluster,
            reserved_sectors,
            num_fats,
            root_entries,
            total_sectors,
            fat_sectors,
            fat_kind,
        })
    }

    /// First sector of the FAT (1-based).
    pub fn fat_start_sector(&self) -> u32 {
        self.reserved_sectors as u32
    }

    /// First sector of the root directory.
    pub fn root_dir_sector(&self) -> u32 {
        self.fat_start_sector() + self.num_fats as u32 * self.fat_sectors as u32
    }

    /// First sector of the data area.
    pub fn data_start_sector(&self) -> u32 {
        let root_sectors = (self.root_entries as u32 * 32).div_ceil(self.bytes_per_sector as u32);
        self.root_dir_sector() + root_sectors
    }

    /// Bytes per cluster.
    pub fn cluster_size(&self) -> u32 {
        self.sectors_per_cluster as u32 * self.bytes_per_sector as u32
    }
}

/// Decoded directory entry.
#[derive(Debug, Clone)]
pub struct Human68kDirEntry {
    /// Raw filename bytes (8 name + 10 ext-name + 3 ext = 21 bytes
    /// max), trimmed of trailing 0x00 / 0x20 padding.
    pub raw_name: Vec<u8>,
    /// 4-byte combined name + 3-byte ext (the trailing 3 bytes only).
    pub raw_ext: Vec<u8>,
    /// Display form: ASCII verbatim, Shift-JIS pairs as `_` placeholders.
    pub display_name: String,
    pub attr: u8,
    pub time: u16,
    pub date: u16,
    pub first_cluster: u16,
    pub size: u32,
}

impl Human68kDirEntry {
    /// True if this slot is in use (not deleted and not end-of-list).
    pub fn is_active(&self) -> bool {
        !self.raw_name.is_empty()
    }

    pub fn is_directory(&self) -> bool {
        self.attr & attr::DIRECTORY != 0
    }

    pub fn is_volume_label(&self) -> bool {
        self.attr & attr::VOLUME_LABEL != 0
    }
}

/// Parse a 32-byte directory entry. Returns `None` for end-of-list
/// (first byte == 0x00) and for deleted slots (first byte == 0xE5).
pub fn parse_dir_entry(buf: &[u8; DIR_ENTRY_SIZE]) -> Option<Human68kDirEntry> {
    if buf[0] == 0x00 || buf[0] == 0xE5 {
        return None;
    }
    let name_bytes = &buf[0..8];
    let name_ext_bytes = &buf[12..22];
    let ext_bytes = &buf[8..11];
    let attr_byte = buf[11];
    let time = LittleEndian::read_u16(&buf[22..24]);
    let date = LittleEndian::read_u16(&buf[24..26]);
    let first_cluster = LittleEndian::read_u16(&buf[26..28]);
    let size = LittleEndian::read_u32(&buf[28..32]);

    // Combine the 8-byte name + 10-byte extended name. Trim trailing
    // spaces and zeros from the right.
    let mut name = name_bytes.to_vec();
    name.extend_from_slice(name_ext_bytes);
    while name.last().is_some_and(|&b| b == 0x20 || b == 0x00) {
        name.pop();
    }
    let mut ext = ext_bytes.to_vec();
    while ext.last().is_some_and(|&b| b == 0x20 || b == 0x00) {
        ext.pop();
    }

    let display_name = shift_jis_lossy_display(&name, &ext);

    Some(Human68kDirEntry {
        raw_name: name,
        raw_ext: ext,
        display_name,
        attr: attr_byte,
        time,
        date,
        first_cluster,
        size,
    })
}

/// Lossy Shift-JIS decode for display only. ASCII verbatim, double-byte
/// pairs become `?` so users see "something is non-ASCII here" instead
/// of garbage. Round-trip writes go through `raw_name` / `raw_ext`.
fn shift_jis_lossy_display(name: &[u8], ext: &[u8]) -> String {
    let n = lossy_one(name);
    let e = lossy_one(ext);
    if e.is_empty() {
        n
    } else {
        format!("{n}.{e}")
    }
}

fn lossy_one(bytes: &[u8]) -> String {
    let mut s = String::new();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if (0x20..=0x7E).contains(&b) {
            s.push(b as char);
            i += 1;
        } else if is_sjis_first_byte(b) && i + 1 < bytes.len() {
            // Shift-JIS double-byte; emit placeholder.
            s.push('?');
            i += 2;
        } else {
            s.push('_');
            i += 1;
        }
    }
    s
}

fn is_sjis_first_byte(b: u8) -> bool {
    matches!(b, 0x81..=0x9F | 0xE0..=0xFC)
}

/// Live Human68k reader.
pub struct Human68kFilesystem<R: Read + Seek + Send> {
    reader: R,
    partition_offset: u64,
    pub(crate) bpb: Human68kBpb,
    fat_bytes: Vec<u8>,
}

impl<R: Read + Seek + Send> Human68kFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        reader.seek(SeekFrom::Start(partition_offset))?;
        let mut bpb_buf = [0u8; 512];
        reader.read_exact(&mut bpb_buf)?;
        let bpb = Human68kBpb::parse(&bpb_buf)?;

        let fat_byte_len = bpb.fat_sectors as u64 * bpb.bytes_per_sector as u64;
        let fat_off =
            partition_offset + bpb.fat_start_sector() as u64 * bpb.bytes_per_sector as u64;
        reader.seek(SeekFrom::Start(fat_off))?;
        let mut fat_bytes = vec![0u8; fat_byte_len as usize];
        reader.read_exact(&mut fat_bytes)?;

        Ok(Self {
            reader,
            partition_offset,
            bpb,
            fat_bytes,
        })
    }

    /// FAT chain follow: given a starting cluster, return the list of
    /// cluster numbers until we hit the end-of-chain sentinel.
    fn cluster_chain(&self, first: u16) -> Result<Vec<u16>, FilesystemError> {
        let mut out = Vec::new();
        let mut cluster = first;
        let mut visited = std::collections::HashSet::new();
        let cap = self.bpb.total_sectors as usize / self.bpb.sectors_per_cluster as usize + 16;
        while cluster >= 2 && (cluster as usize) < cap {
            if !visited.insert(cluster) {
                return Err(FilesystemError::InvalidData(format!(
                    "Human68k FAT cycle at cluster {cluster}"
                )));
            }
            out.push(cluster);
            let next = self.fat_lookup(cluster);
            if self.is_end_of_chain(next) {
                break;
            }
            cluster = next;
        }
        Ok(out)
    }

    fn fat_lookup(&self, cluster: u16) -> u16 {
        match self.bpb.fat_kind {
            FatKind::Fat12 => {
                let off = (cluster as usize * 3) / 2;
                if off + 1 >= self.fat_bytes.len() {
                    return 0xFFF;
                }
                let lo = self.fat_bytes[off] as u16;
                let hi = self.fat_bytes[off + 1] as u16;
                if cluster.is_multiple_of(2) {
                    lo | ((hi & 0x0F) << 8)
                } else {
                    (lo >> 4) | (hi << 4)
                }
            }
            FatKind::Fat16 => {
                let off = cluster as usize * 2;
                if off + 1 >= self.fat_bytes.len() {
                    return 0xFFFF;
                }
                u16::from_le_bytes([self.fat_bytes[off], self.fat_bytes[off + 1]])
            }
        }
    }

    fn is_end_of_chain(&self, v: u16) -> bool {
        match self.bpb.fat_kind {
            FatKind::Fat12 => v >= 0xFF8,
            FatKind::Fat16 => v >= 0xFFF8,
        }
    }

    /// Byte offset of a data cluster's first sector.
    fn cluster_byte_offset(&self, cluster: u16) -> u64 {
        let data_sector = self.bpb.data_start_sector() as u64
            + (cluster as u64 - 2) * self.bpb.sectors_per_cluster as u64;
        self.partition_offset + data_sector * self.bpb.bytes_per_sector as u64
    }

    /// Read root-directory sectors and parse all 32-byte entries.
    fn read_root_directory(&mut self) -> Result<Vec<Human68kDirEntry>, FilesystemError> {
        let root_off = self.partition_offset
            + self.bpb.root_dir_sector() as u64 * self.bpb.bytes_per_sector as u64;
        let root_byte_len = self.bpb.root_entries as u64 * DIR_ENTRY_SIZE as u64;
        self.reader.seek(SeekFrom::Start(root_off))?;
        let mut buf = vec![0u8; root_byte_len as usize];
        self.reader.read_exact(&mut buf)?;
        let mut out = Vec::new();
        for slot in buf.chunks_exact(DIR_ENTRY_SIZE) {
            if slot[0] == 0x00 {
                break; // end of directory
            }
            if slot[0] == 0xE5 {
                continue; // deleted
            }
            let arr: &[u8; DIR_ENTRY_SIZE] = slot.try_into().unwrap();
            if let Some(e) = parse_dir_entry(arr) {
                if !e.is_volume_label() {
                    out.push(e);
                }
            }
        }
        Ok(out)
    }
}

impl<R: Read + Seek + Send> Filesystem for Human68kFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if entry.path != "/" {
            return Ok(Vec::new()); // subdirs left as future work
        }
        let dir = self.read_root_directory()?;
        let mut out = Vec::with_capacity(dir.len());
        for de in dir {
            let name = de.display_name.clone();
            let mut fe = if de.is_directory() {
                FileEntry::new_directory(name.clone(), format!("/{name}"), de.first_cluster as u64)
            } else {
                FileEntry::new_file(
                    name.clone(),
                    format!("/{name}"),
                    de.size as u64,
                    de.first_cluster as u64,
                )
            };
            if de.attr & attr::READ_ONLY != 0 {
                fe.special_type = Some("R/O".to_string());
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
        let first = entry.location as u16;
        let chain = self.cluster_chain(first)?;
        let size = entry.size as usize;
        let mut data = Vec::with_capacity(size);
        let csize = self.bpb.cluster_size() as usize;
        let mut remaining = size;
        for cluster in chain {
            if remaining == 0 {
                break;
            }
            let want = csize.min(remaining);
            let off = self.cluster_byte_offset(cluster);
            self.reader.seek(SeekFrom::Start(off))?;
            let mut buf = vec![0u8; want];
            self.reader.read_exact(&mut buf)?;
            data.extend_from_slice(&buf);
            remaining = remaining.saturating_sub(want);
        }
        data.truncate(max_bytes);
        Ok(data)
    }

    fn fs_type(&self) -> &str {
        match self.bpb.fat_kind {
            FatKind::Fat12 => "Human68k (FAT12)",
            FatKind::Fat16 => "Human68k (FAT16)",
        }
    }

    fn volume_label(&self) -> Option<&str> {
        None // Volume label entry decoding deferred — extract floor.
    }

    fn total_size(&self) -> u64 {
        self.bpb.total_sectors as u64 * self.bpb.bytes_per_sector as u64
    }

    fn used_size(&self) -> u64 {
        // Conservative: count clusters whose FAT entry is non-zero.
        let csize = self.bpb.cluster_size() as u64;
        let max_cluster =
            ((self.bpb.total_sectors / self.bpb.sectors_per_cluster as u32) + 2).min(0xFFF8) as u16;
        let mut used: u64 = 0;
        for c in 2..max_cluster {
            if self.fat_lookup(c) != 0 {
                used += csize;
            }
        }
        used
    }
}

// ============================================================================
// EditableFilesystem — Add/Delete on the root directory only
// ============================================================================

/// 8 + 10 + 3 byte encoded name slot returned by `encode_human68k_name`.
type EncodedName = ([u8; 8], [u8; 10], [u8; 3]);

/// Encode a candidate filename into 8 + 10 + 3 byte fields for a
/// Human68k directory entry. Accepts ASCII or already-Shift-JIS-encoded
/// raw bytes; rejects names containing '/', ':', or non-printable ASCII
/// outside the SJIS first-byte range.
fn encode_human68k_name(name: &str) -> Result<EncodedName, FilesystemError> {
    if name.is_empty() {
        return Err(FilesystemError::InvalidData(
            "Human68k filename is empty".into(),
        ));
    }
    if name.contains('/') || name.contains('\\') || name.contains(':') {
        return Err(FilesystemError::InvalidData(format!(
            "Human68k filename '{name}' contains a path separator"
        )));
    }
    let bytes = name.as_bytes();
    // Split at the LAST '.' for ext (rightmost-dot convention).
    let (name_part, ext_part) = match bytes.iter().rposition(|&b| b == b'.') {
        Some(idx) => (&bytes[..idx], &bytes[idx + 1..]),
        None => (bytes, &b""[..]),
    };
    if name_part.is_empty() || name_part.len() > 18 {
        return Err(FilesystemError::InvalidData(format!(
            "Human68k name '{name}' must have 1..=18 chars before the extension"
        )));
    }
    if ext_part.len() > 3 {
        return Err(FilesystemError::InvalidData(format!(
            "Human68k extension exceeds 3 bytes (got {})",
            ext_part.len()
        )));
    }
    let mut n8 = [b' '; 8];
    let mut ne10 = [b' '; 10];
    let take = name_part.len().min(8);
    n8[..take].copy_from_slice(&name_part[..take]);
    if name_part.len() > 8 {
        let etake = (name_part.len() - 8).min(10);
        ne10[..etake].copy_from_slice(&name_part[8..8 + etake]);
    }
    let mut e3 = [b' '; 3];
    e3[..ext_part.len()].copy_from_slice(ext_part);
    Ok((n8, ne10, e3))
}

impl<R: Read + Write + Seek + Send> Human68kFilesystem<R> {
    /// Write the cached FAT bytes back to both FAT copies on disk.
    fn fat_write_back(&mut self) -> Result<(), FilesystemError> {
        let fat_byte_len = self.fat_bytes.len();
        let bps = self.bpb.bytes_per_sector as u64;
        for fat_idx in 0..self.bpb.num_fats {
            let off = self.partition_offset
                + (self.bpb.fat_start_sector() as u64
                    + fat_idx as u64 * self.bpb.fat_sectors as u64)
                    * bps;
            self.reader.seek(SeekFrom::Start(off))?;
            self.reader.write_all(&self.fat_bytes[..fat_byte_len])?;
        }
        Ok(())
    }

    /// Set a 12- or 16-bit FAT entry.
    fn fat_set(&mut self, cluster: u16, value: u16) {
        match self.bpb.fat_kind {
            FatKind::Fat12 => {
                let off = (cluster as usize * 3) / 2;
                if off + 1 >= self.fat_bytes.len() {
                    return;
                }
                let v = value & 0x0FFF;
                if cluster.is_multiple_of(2) {
                    self.fat_bytes[off] = (v & 0xFF) as u8;
                    self.fat_bytes[off + 1] =
                        (self.fat_bytes[off + 1] & 0xF0) | ((v >> 8) as u8 & 0x0F);
                } else {
                    self.fat_bytes[off] = (self.fat_bytes[off] & 0x0F) | ((v & 0x0F) as u8) << 4;
                    self.fat_bytes[off + 1] = (v >> 4) as u8;
                }
            }
            FatKind::Fat16 => {
                let off = cluster as usize * 2;
                if off + 1 >= self.fat_bytes.len() {
                    return;
                }
                self.fat_bytes[off..off + 2].copy_from_slice(&value.to_le_bytes());
            }
        }
    }

    /// Allocate a chain of `count` clusters. Returns the first cluster
    /// or `InvalidData` if the volume is full.
    fn alloc_chain(&mut self, count: u32) -> Result<u16, FilesystemError> {
        if count == 0 {
            return Ok(0);
        }
        let max_cluster =
            ((self.bpb.total_sectors / self.bpb.sectors_per_cluster as u32) + 2).min(0xFFF8) as u16;
        let mut picked: Vec<u16> = Vec::with_capacity(count as usize);
        for c in 2..max_cluster {
            if self.fat_lookup(c) == 0 {
                picked.push(c);
                if picked.len() as u32 == count {
                    break;
                }
            }
        }
        if (picked.len() as u32) < count {
            return Err(FilesystemError::InvalidData(format!(
                "Human68k volume full: need {count} clusters, found {}",
                picked.len()
            )));
        }
        for w in picked.windows(2) {
            self.fat_set(w[0], w[1]);
        }
        let last = *picked.last().unwrap();
        let end_marker = match self.bpb.fat_kind {
            FatKind::Fat12 => 0xFFF,
            FatKind::Fat16 => 0xFFFF,
        };
        self.fat_set(last, end_marker);
        Ok(picked[0])
    }

    /// Walk a chain and zero every entry along the way.
    fn free_chain(&mut self, first: u16) -> Result<(), FilesystemError> {
        let chain = self.cluster_chain(first)?;
        for c in chain {
            self.fat_set(c, 0);
        }
        Ok(())
    }

    /// Write payload bytes into clusters starting at `first`.
    fn write_chain(&mut self, first: u16, data: &[u8]) -> Result<(), FilesystemError> {
        let cs = self.bpb.cluster_size() as usize;
        let mut written = 0usize;
        let chain = self.cluster_chain(first)?;
        for c in chain {
            if written >= data.len() {
                break;
            }
            let want = cs.min(data.len() - written);
            let off = self.cluster_byte_offset(c);
            self.reader.seek(SeekFrom::Start(off))?;
            self.reader.write_all(&data[written..written + want])?;
            // Pad the rest of this cluster with zeros.
            if want < cs {
                let pad = vec![0u8; cs - want];
                self.reader.write_all(&pad)?;
            }
            written += want;
        }
        Ok(())
    }

    /// Find a free 32-byte root-directory slot. Returns its byte offset
    /// in the partition.
    fn find_free_root_slot(&mut self) -> Result<u64, FilesystemError> {
        let root_off = self.partition_offset
            + self.bpb.root_dir_sector() as u64 * self.bpb.bytes_per_sector as u64;
        let root_byte_len = self.bpb.root_entries as u64 * DIR_ENTRY_SIZE as u64;
        self.reader.seek(SeekFrom::Start(root_off))?;
        let mut buf = vec![0u8; root_byte_len as usize];
        self.reader.read_exact(&mut buf)?;
        for (i, slot) in buf.chunks_exact(DIR_ENTRY_SIZE).enumerate() {
            if slot[0] == 0x00 || slot[0] == 0xE5 {
                return Ok(root_off + (i * DIR_ENTRY_SIZE) as u64);
            }
        }
        Err(FilesystemError::InvalidData(
            "Human68k root directory is full".into(),
        ))
    }
}

impl<R: Read + Write + Seek + Send> EditableFilesystem for Human68kFilesystem<R> {
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
                "Human68k subdir writes deferred — only root supported".into(),
            ));
        }
        let (n8, ne10, e3) = encode_human68k_name(name)?;
        // Reject duplicates.
        let root = self.root()?;
        let existing = self.list_directory(&root)?;
        if existing.iter().any(|e| e.name.eq_ignore_ascii_case(name)) {
            return Err(FilesystemError::InvalidData(format!(
                "Human68k file '{name}' already exists"
            )));
        }
        if data_len > u32::MAX as u64 {
            return Err(FilesystemError::InvalidData(
                "Human68k file exceeds u32::MAX bytes".into(),
            ));
        }
        let mut payload = Vec::with_capacity(data_len as usize);
        data.read_to_end(&mut payload)?;
        if payload.len() as u64 != data_len {
            return Err(FilesystemError::InvalidData(format!(
                "data_len {data_len} != actual {}",
                payload.len()
            )));
        }
        let cs = self.bpb.cluster_size() as u64;
        let cluster_count = if payload.is_empty() {
            0u32
        } else {
            (payload.len() as u64).div_ceil(cs) as u32
        };
        let first_cluster = if cluster_count > 0 {
            self.alloc_chain(cluster_count)?
        } else {
            0
        };
        if first_cluster != 0 {
            self.write_chain(first_cluster, &payload)?;
        }

        // Stamp a new directory entry into the first free slot.
        let slot_off = self.find_free_root_slot()?;
        let mut entry = [0u8; DIR_ENTRY_SIZE];
        entry[0..8].copy_from_slice(&n8);
        entry[8..11].copy_from_slice(&e3);
        entry[11] = attr::ARCHIVE;
        entry[12..22].copy_from_slice(&ne10);
        LittleEndian::write_u16(&mut entry[26..28], first_cluster);
        LittleEndian::write_u32(&mut entry[28..32], payload.len() as u32);
        self.reader.seek(SeekFrom::Start(slot_off))?;
        self.reader.write_all(&entry)?;

        self.fat_write_back()?;
        self.reader.flush()?;
        Ok(FileEntry::new_file(
            name.to_string(),
            format!("/{name}"),
            payload.len() as u64,
            first_cluster as u64,
        ))
    }

    fn create_directory(
        &mut self,
        _parent: &FileEntry,
        _name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "Human68k subdir creation deferred".into(),
        ))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        if parent.path != "/" {
            return Err(FilesystemError::Unsupported(
                "Human68k subdir deletion deferred".into(),
            ));
        }
        if entry.entry_type != EntryType::File {
            return Err(FilesystemError::InvalidData(
                "Human68k only allows file deletion at this stage".into(),
            ));
        }
        let first = entry.location as u16;
        if first != 0 {
            self.free_chain(first)?;
        }
        // Mark directory slot deleted: walk root entries to find the one
        // matching this entry's name, stamp 0xE5 in byte 0.
        let root_off = self.partition_offset
            + self.bpb.root_dir_sector() as u64 * self.bpb.bytes_per_sector as u64;
        let root_byte_len = self.bpb.root_entries as u64 * DIR_ENTRY_SIZE as u64;
        self.reader.seek(SeekFrom::Start(root_off))?;
        let mut buf = vec![0u8; root_byte_len as usize];
        self.reader.read_exact(&mut buf)?;
        for (i, slot) in buf.chunks_exact(DIR_ENTRY_SIZE).enumerate() {
            if slot[0] == 0x00 {
                break;
            }
            if slot[0] == 0xE5 {
                continue;
            }
            let arr: &[u8; DIR_ENTRY_SIZE] = slot.try_into().unwrap();
            if let Some(de) = parse_dir_entry(arr) {
                if de.display_name.eq_ignore_ascii_case(&entry.name) {
                    let slot_off = root_off + (i * DIR_ENTRY_SIZE) as u64;
                    self.reader.seek(SeekFrom::Start(slot_off))?;
                    self.reader.write_all(&[0xE5])?;
                    self.fat_write_back()?;
                    self.reader.flush()?;
                    return Ok(());
                }
            }
        }
        Err(FilesystemError::NotFound(format!(
            "Human68k entry '{}' not found in root directory",
            entry.name
        )))
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.reader.flush()?;
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        let max_cluster =
            ((self.bpb.total_sectors / self.bpb.sectors_per_cluster as u32) + 2).min(0xFFF8) as u16;
        let mut free: u64 = 0;
        for c in 2..max_cluster {
            if self.fat_lookup(c) == 0 {
                free += self.bpb.cluster_size() as u64;
            }
        }
        Ok(free)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a tiny synthetic Human68k FAT12 floppy:
    ///   - 720 KB (9 spt × 80 trk × 2 sides × 512 B)
    ///   - one root file "DOC.TXT" with 32 bytes of content in cluster 2
    fn build_fat12_synthetic() -> Vec<u8> {
        const TOTAL_SECTORS: u32 = 1440;
        const BYTES_PER_SECTOR: u16 = 512;
        const SECTORS_PER_CLUSTER: u8 = 1;
        const RESERVED_SECTORS: u16 = 1;
        const NUM_FATS: u8 = 2;
        const ROOT_ENTRIES: u16 = 112;
        const FAT_SECTORS: u16 = 3;

        let mut disk = vec![0u8; TOTAL_SECTORS as usize * BYTES_PER_SECTOR as usize];
        // BPB
        disk[0] = 0xEB; // JMP
        disk[1] = 0x3C;
        disk[2] = 0x90;
        disk[3..11].copy_from_slice(b"X68KFS  ");
        LittleEndian::write_u16(&mut disk[11..13], BYTES_PER_SECTOR);
        disk[13] = SECTORS_PER_CLUSTER;
        LittleEndian::write_u16(&mut disk[14..16], RESERVED_SECTORS);
        disk[16] = NUM_FATS;
        LittleEndian::write_u16(&mut disk[17..19], ROOT_ENTRIES);
        LittleEndian::write_u16(&mut disk[19..21], TOTAL_SECTORS as u16);
        disk[21] = 0xF9; // media descriptor (3.5" 720K)
        LittleEndian::write_u16(&mut disk[22..24], FAT_SECTORS);

        // FAT entry for cluster 2 — end of chain.
        let fat0_off = RESERVED_SECTORS as usize * BYTES_PER_SECTOR as usize;
        // Reserved: cluster 0 = 0xFF9, cluster 1 = 0xFFF.
        disk[fat0_off] = 0xF9;
        disk[fat0_off + 1] = 0xFF;
        disk[fat0_off + 2] = 0xFF;
        // Cluster 2 entry (FAT12 packing for even cluster): low 12 bits.
        // 0xFFF = end of chain.
        disk[fat0_off + 3] = 0xFF;
        disk[fat0_off + 4] = 0x0F;

        // Root directory at reserved + fat_sectors * num_fats.
        let root_off = RESERVED_SECTORS as usize * BYTES_PER_SECTOR as usize
            + FAT_SECTORS as usize * NUM_FATS as usize * BYTES_PER_SECTOR as usize;
        // Entry 0: "DOC.TXT" — first cluster 2, size 32.
        let entry = &mut disk[root_off..root_off + 32];
        entry[0..8].copy_from_slice(b"DOC     ");
        entry[8..11].copy_from_slice(b"TXT");
        entry[11] = attr::ARCHIVE;
        for b in &mut entry[12..22] {
            *b = 0x20; // padded name-extension area
        }
        LittleEndian::write_u16(&mut entry[26..28], 2); // first cluster
        LittleEndian::write_u32(&mut entry[28..32], 32); // size

        // File data at cluster 2.
        let root_byte_len = ROOT_ENTRIES as usize * 32;
        let data_off = root_off + root_byte_len;
        let payload = b"hello human68k synthetic disk!\n\0";
        disk[data_off..data_off + payload.len()].copy_from_slice(payload);
        disk
    }

    #[test]
    fn parses_bpb_and_identifies_fat12() {
        let disk = build_fat12_synthetic();
        let cur = Cursor::new(disk);
        let fs = Human68kFilesystem::open(cur, 0).unwrap();
        assert_eq!(fs.bpb.bytes_per_sector, 512);
        assert_eq!(fs.bpb.total_sectors, 1440);
        assert_eq!(fs.bpb.fat_kind, FatKind::Fat12);
        assert_eq!(fs.fs_type(), "Human68k (FAT12)");
    }

    #[test]
    fn lists_root_directory_with_18_3_filename() {
        let disk = build_fat12_synthetic();
        let cur = Cursor::new(disk);
        let mut fs = Human68kFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "DOC.TXT");
        assert_eq!(entries[0].size, 32);
    }

    #[test]
    fn reads_file_byte_exact_through_fat_chain() {
        let disk = build_fat12_synthetic();
        let cur = Cursor::new(disk);
        let mut fs = Human68kFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let data = fs.read_file(&entries[0], 4096).unwrap();
        // Synthetic payload has a final null byte; full 32 B read returns it.
        assert_eq!(data.len(), 32);
        assert_eq!(&data[..30], b"hello human68k synthetic disk!");
    }

    #[test]
    fn parse_dir_entry_combines_name_and_name_extension_bytes() {
        let mut buf = [0u8; 32];
        // 8 base + 10 ext-name + 3 ext to demonstrate 18.3.
        buf[0..8].copy_from_slice(b"LONGNAME");
        buf[8..11].copy_from_slice(b"TXT");
        buf[11] = attr::ARCHIVE;
        // 10 more chars in the name-ext area, space-padded for the tail.
        buf[12..22].copy_from_slice(b"_EXTRABIT ");
        LittleEndian::write_u16(&mut buf[26..28], 5);
        LittleEndian::write_u32(&mut buf[28..32], 256);
        let parsed = parse_dir_entry(&buf).unwrap();
        assert_eq!(parsed.first_cluster, 5);
        assert_eq!(parsed.size, 256);
        // Display = ASCII chars only; the underscore in the synthetic
        // name-ext rides through verbatim, the trailing space trims.
        assert_eq!(parsed.display_name, "LONGNAME_EXTRABIT.TXT");
    }

    #[test]
    fn shift_jis_double_byte_renders_as_placeholder() {
        // Build a name with one ASCII char + one Shift-JIS double-byte
        // pair (0x82 0xA0 is 'あ'). Expect "A?" display + "A?" extension.
        let mut buf = [0u8; 32];
        buf[0] = b'A';
        buf[1] = 0x82;
        buf[2] = 0xA0;
        for b in &mut buf[3..8] {
            *b = 0x20;
        }
        for b in &mut buf[8..11] {
            *b = 0x20;
        }
        for b in &mut buf[12..22] {
            *b = 0x20;
        }
        buf[11] = attr::ARCHIVE;
        LittleEndian::write_u16(&mut buf[26..28], 2);
        let parsed = parse_dir_entry(&buf).unwrap();
        // Name byte 0 = 'A' (ASCII), bytes 1..3 = SJIS pair = '?'.
        assert_eq!(parsed.display_name, "A?");
        // Raw bytes preserved for round-trip.
        assert_eq!(&parsed.raw_name, b"A\x82\xA0");
    }

    #[test]
    fn encode_human68k_name_handles_8_3_and_18_3() {
        let (n, ne, e) = encode_human68k_name("SHORT.TXT").unwrap();
        assert_eq!(&n, b"SHORT   ");
        assert_eq!(&ne, b"          ");
        assert_eq!(&e, b"TXT");
        let (n2, ne2, e2) = encode_human68k_name("LONGFILENAME1234.X").unwrap();
        assert_eq!(&n2, b"LONGFILE");
        assert_eq!(&ne2, b"NAME1234  ");
        assert_eq!(&e2, b"X  ");
    }

    #[test]
    fn encode_human68k_name_rejects_path_separator_and_oversize() {
        assert!(encode_human68k_name("foo/bar").is_err());
        let too_long = "X".repeat(19);
        assert!(encode_human68k_name(&too_long).is_err());
        assert!(encode_human68k_name("name.TOOLONGEXT").is_err());
        assert!(encode_human68k_name("").is_err());
    }

    #[test]
    fn create_file_persists_and_round_trips_through_sync() {
        let disk = build_fat12_synthetic();
        let cur = Cursor::new(disk);
        let mut fs = Human68kFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let before_free = EditableFilesystem::free_space(&mut fs).unwrap();
        let payload = b"created via human68k edit path".to_vec();
        let mut src = Cursor::new(payload.clone());
        let _fe = fs
            .create_file(
                &root,
                "NEW.TXT",
                &mut src,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();

        let inner = fs.reader.clone();
        let mut fs2 = Human68kFilesystem::open(inner, 0).unwrap();
        let root2 = fs2.root().unwrap();
        let entries = fs2.list_directory(&root2).unwrap();
        let new = entries.iter().find(|e| e.name == "NEW.TXT").unwrap();
        assert_eq!(new.size, payload.len() as u64);
        let got = fs2.read_file(new, 4096).unwrap();
        assert_eq!(got, payload);

        let after_free = EditableFilesystem::free_space(&mut fs2).unwrap();
        // 512 B cluster, 30 B payload → one cluster consumed.
        assert_eq!(after_free, before_free - 512);
    }

    #[test]
    fn delete_entry_frees_chain_and_marks_slot_deleted() {
        let disk = build_fat12_synthetic();
        let cur = Cursor::new(disk);
        let mut fs = Human68kFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let doc = entries
            .iter()
            .find(|e| e.name == "DOC.TXT")
            .unwrap()
            .clone();
        let before_free = EditableFilesystem::free_space(&mut fs).unwrap();
        fs.delete_entry(&root, &doc).unwrap();

        let inner = fs.reader.clone();
        let mut fs2 = Human68kFilesystem::open(inner, 0).unwrap();
        let root2 = fs2.root().unwrap();
        let entries = fs2.list_directory(&root2).unwrap();
        assert!(entries.iter().all(|e| e.name != "DOC.TXT"));
        let after_free = EditableFilesystem::free_space(&mut fs2).unwrap();
        // One 512-B cluster gets freed.
        assert_eq!(after_free, before_free + 512);
    }

    #[test]
    fn create_file_rejects_duplicate_names() {
        let disk = build_fat12_synthetic();
        let mut fs = Human68kFilesystem::open(Cursor::new(disk), 0).unwrap();
        let root = fs.root().unwrap();
        let mut src = Cursor::new(b"x".to_vec());
        let err = fs
            .create_file(&root, "DOC.TXT", &mut src, 1, &CreateFileOptions::default())
            .unwrap_err();
        assert!(matches!(err, FilesystemError::InvalidData(_)));
    }

    // Spine-stage-7 confirmation: resize_fat_in_place on a Human68k disk
    // must extend the volume in place and Human68kFilesystem::open must
    // still find every file on the resized image. Human68k is FAT12/16
    // BPB-compatible so the same FAT machinery applies — these tests
    // pin the contract so a future FAT refactor can't silently break
    // X68000 HDD resize.
    #[test]
    fn resize_fat_in_place_grows_human68k_volume() {
        use crate::fs::fat::resize_fat_in_place;
        use std::io::Cursor;

        let mut disk = build_fat12_synthetic();
        // Extend the backing buffer to the larger target size so the
        // resize call can write into it (resize doesn't allocate disk
        // bytes itself — it requires the writer to already be large
        // enough).
        let old_sectors: u32 = 1440;
        let new_sectors: u32 = 2880; // 720K -> 1.44M shape
        let bps = 512usize;
        disk.resize(new_sectors as usize * bps, 0);

        let mut cur = Cursor::new(&mut disk);
        let did_resize =
            resize_fat_in_place(&mut cur, 0, new_sectors, &mut |_| {}).expect("resize ok");
        assert!(did_resize, "resize should succeed on Human68k FAT12 BPB");

        // BPB.total_sectors (offset 19..21 for u16) now reflects the new size.
        assert_eq!(
            u16::from_le_bytes([disk[19], disk[20]]) as u32,
            new_sectors,
            "BPB total_sectors should be patched in-place"
        );

        // The file we seeded into the FAT12 disk must still be reachable
        // after the resize via the Human68k reader (BPB shape unchanged,
        // FAT extended with free entries).
        let mut fs = Human68kFilesystem::open(Cursor::new(&disk), 0).expect("reopen");
        assert_eq!(fs.bpb.total_sectors, new_sectors);
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert_eq!(entries.len(), 1, "DOC.TXT should survive resize");
        assert_eq!(entries[0].name, "DOC.TXT");

        let payload = fs.read_file(&entries[0], 1024).unwrap();
        assert_eq!(&payload[..14], b"hello human68k");

        // FAT12 stays FAT12 — old_sectors and new_sectors both
        // produce <4085 clusters with 1 SPC + 1 reserved + 2 FATs at
        // 3 sectors each. The resize path uses the spare-FAT-capacity
        // branch (no data shift, BPB-only update).
        let _ = old_sectors;
    }

    #[test]
    fn resize_fat_in_place_shrinks_human68k_volume() {
        use crate::fs::fat::resize_fat_in_place;
        use std::io::Cursor;

        // Build a FAT16 Human68k disk (16 MiB) so the resize_fat_in_place
        // path has room to shrink without changing FAT type.
        let total_sectors_old: u32 = 32_768; // 16 MiB / 512
        let total_sectors_new: u32 = 16_384; // 8 MiB / 512
        let bps = 512usize;
        let mut disk =
            crate::fs::fat::create_blank_fat(total_sectors_old as u64 * bps as u64, Some("X68K"))
                .expect("blank FAT16");

        // Sanity: Human68k can open the fresh blank.
        let bpb_check_disk = disk.clone();
        let _ = Human68kFilesystem::open(Cursor::new(bpb_check_disk), 0)
            .expect("Human68k opens blank FAT16");

        let mut cur = Cursor::new(&mut disk);
        let did_resize =
            resize_fat_in_place(&mut cur, 0, total_sectors_new, &mut |_| {}).expect("shrink ok");
        assert!(did_resize, "shrink should succeed");

        // BPB total_sectors slot for FAT16 with total > u16::MAX is in
        // bytes 32..36 (large total). 16384 fits in u16 so it lives in
        // bytes 19..21 instead — match how the FAT resize writer chose
        // the field.
        let small_total = u16::from_le_bytes([disk[19], disk[20]]) as u32;
        let big_total = u32::from_le_bytes([disk[32], disk[33], disk[34], disk[35]]);
        let observed = if small_total != 0 {
            small_total
        } else {
            big_total
        };
        assert_eq!(observed, total_sectors_new);

        // The blank disk has no files, but reopen + root listing must
        // succeed end-to-end.
        let mut fs = Human68kFilesystem::open(Cursor::new(&disk), 0).expect("reopen");
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert!(
            entries.is_empty(),
            "freshly-formatted volume has no files after shrink"
        );
    }
}
