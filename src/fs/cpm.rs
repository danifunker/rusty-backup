//! CP/M 2.2 / 3.0 filesystem — the FAT-less floppy filesystem shipped
//! on every CP/M-era 8-bit machine the MiSTer project supports
//! (Altair8800, Amstrad CPC / PCW, Tatung Einstein, SVI-328, MultiComp,
//! ZX-Spectrum +3DOS).
//!
//! One engine handles all of them via a per-machine **Disk Parameter
//! Block (DPB)** registry; see [`super::cpm_diskdefs`] for the shipped
//! presets. The DPB tells the FS where the directory lives, what an
//! allocation block is, and how many blocks the disk has. Without one,
//! every CP/M-format disk is unparseable noise.
//!
//! ## On-disk layout
//!
//! ```text
//! reserved tracks (DPB.off) — boot blocks / CCP image / BDOS image
//! data tracks                — allocation blocks of (128 << bsh) bytes:
//!     block 0..N (per DPB.al0/al1 bitmap)  — directory: 32-B entries
//!     remaining blocks                      — file data
//! ```
//!
//! ## Directory entry (32 B)
//!
//! ```text
//! 0       user number (0..15) or 0xE5 = deleted/empty
//! 1..9    filename, 8 chars ASCII, space-padded; high bit reserved
//! 9..12   extension, 3 chars; high bits = read-only / system / archive
//! 12      Xl — extent number low byte
//! 13      Bc — byte count in last record (CP/M 3 only; CP/M 2.2 = 0)
//! 14      Xh — extent number high byte
//! 15      Rc — record count in this extent (records of 128 B)
//! 16..32  allocation map — 16 byte block ptrs OR 8 word block ptrs
//!         (word ptrs when DPB.dsm >= 256)
//! ```
//!
//! A file may span many extents (same name + same user, different
//! `Xl|Xh<<5`). To read a file we sort its extents by extent number
//! and concatenate their data blocks.
//!
//! All multi-byte fields are **little-endian**. Filenames are ASCII
//! upper-case by convention (CP/M's CCP uppercases input).

use std::io::{Read, Seek, SeekFrom, Write};

use super::cpm_diskdefs::Dpb;
use super::entry::{EntryType, FileEntry};
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};

/// One CP/M directory entry, decoded.
#[derive(Debug, Clone)]
pub struct CpmDirEntry {
    /// 0..15 = active user, 0xE5 = deleted, other = invalid (skipped).
    pub user: u8,
    /// 8-char filename (ASCII, spaces padded). Hi-bits stripped.
    pub name: String,
    /// 3-char extension (ASCII). Hi-bits stripped.
    pub ext: String,
    /// File attribute bits derived from the high bits of the
    /// extension chars: 0x80 = read-only, 0x40 = system, 0x20 = archive.
    pub attrs: u8,
    /// Combined extent number = Xl | (Xh << 5).
    pub extent: u16,
    /// Record count (128-B records) in this extent.
    pub record_count: u8,
    /// Byte count in last record (CP/M 3 only). 0 means "use Rc * 128".
    pub byte_count: u8,
    /// Allocation block pointers. 8 or 16 entries depending on
    /// DPB.uses_word_pointers().
    pub block_ptrs: Vec<u16>,
}

impl CpmDirEntry {
    /// True if this entry's user byte marks it as active (not deleted
    /// or uninitialized).
    pub fn is_active(&self) -> bool {
        self.user < 16
    }

    /// "<name>.<ext>" trimmed (CP/M-style), with attribute high-bits
    /// stripped.
    pub fn formatted_name(&self) -> String {
        let n = self.name.trim_end();
        let e = self.ext.trim_end();
        if e.is_empty() {
            n.to_string()
        } else {
            format!("{n}.{e}")
        }
    }
}

/// Parse a 32-byte directory entry. Caller picks the byte vs word
/// pointer layout via the DPB.
pub fn parse_dir_entry(buf: &[u8; 32], dpb: &Dpb) -> CpmDirEntry {
    let user = buf[0];
    let mut name_chars = [0u8; 8];
    let mut ext_chars = [0u8; 3];
    let mut attrs = 0u8;
    for (i, b) in buf[1..9].iter().enumerate() {
        // CP/M itself doesn't use high bits on the filename, but some
        // implementations stash markers. Strip for display.
        name_chars[i] = b & 0x7F;
    }
    for (i, b) in buf[9..12].iter().enumerate() {
        let bare = b & 0x7F;
        ext_chars[i] = bare;
        if b & 0x80 != 0 {
            attrs |= match i {
                0 => 0x80, // R/O
                1 => 0x40, // SYS
                2 => 0x20, // ARC
                _ => 0,
            };
        }
    }
    let xl = buf[12];
    let bc = buf[13];
    let xh = buf[14];
    let rc = buf[15];
    let extent = (xl as u16) | ((xh as u16) << 5);

    let block_ptrs: Vec<u16> = if dpb.uses_word_pointers() {
        (0..8)
            .map(|i| u16::from_le_bytes([buf[16 + i * 2], buf[17 + i * 2]]))
            .collect()
    } else {
        (0..16).map(|i| buf[16 + i] as u16).collect()
    };

    CpmDirEntry {
        user,
        name: String::from_utf8_lossy(&name_chars).to_string(),
        ext: String::from_utf8_lossy(&ext_chars).to_string(),
        attrs,
        extent,
        record_count: rc,
        byte_count: bc,
        block_ptrs,
    }
}

/// Serialize a directory entry to 32 bytes for write-back.
fn encode_dir_entry(de: &CpmDirEntry, dpb: &Dpb) -> [u8; 32] {
    let mut buf = [0u8; 32];
    buf[0] = de.user;
    // Pad-or-truncate filename and extension.
    let name_bytes = de.name.as_bytes();
    for i in 0..8 {
        buf[1 + i] = if i < name_bytes.len() {
            name_bytes[i]
        } else {
            b' '
        };
    }
    let ext_bytes = de.ext.as_bytes();
    for i in 0..3 {
        let bare = if i < ext_bytes.len() {
            ext_bytes[i]
        } else {
            b' '
        };
        let attr_bit = match i {
            0 if de.attrs & 0x80 != 0 => 0x80,
            1 if de.attrs & 0x40 != 0 => 0x80,
            2 if de.attrs & 0x20 != 0 => 0x80,
            _ => 0,
        };
        buf[9 + i] = bare | attr_bit;
    }
    buf[12] = (de.extent & 0x1F) as u8;
    buf[13] = de.byte_count;
    buf[14] = ((de.extent >> 5) & 0x3F) as u8;
    buf[15] = de.record_count;
    if dpb.uses_word_pointers() {
        for i in 0..8 {
            let v = if i < de.block_ptrs.len() {
                de.block_ptrs[i]
            } else {
                0
            };
            buf[16 + i * 2..18 + i * 2].copy_from_slice(&v.to_le_bytes());
        }
    } else {
        for i in 0..16 {
            buf[16 + i] = if i < de.block_ptrs.len() {
                de.block_ptrs[i] as u8
            } else {
                0
            };
        }
    }
    buf
}

/// Live CP/M reader. Holds the DPB, the in-memory directory, and the
/// reader. Edits go through the in-memory state and flush via
/// `sync_metadata`.
pub struct CpmFilesystem<R: Read + Seek + Send> {
    reader: R,
    partition_offset: u64,
    pub(crate) dpb: Dpb,
    /// All decoded directory entries (active + deleted) in original
    /// on-disk order. Edits push to / mutate this Vec.
    entries: Vec<CpmDirEntry>,
    /// Cached directory bytes from disk; needed because the directory
    /// occupies `(al0/al1 bitmap)` block(s) which we re-pack on write.
    dir_bytes_cache: Vec<u8>,
}

impl<R: Read + Seek + Send> CpmFilesystem<R> {
    /// Open the volume against the given DPB preset.
    pub fn open_with_dpb(
        mut reader: R,
        partition_offset: u64,
        dpb: Dpb,
    ) -> Result<Self, FilesystemError> {
        let dir_bytes = read_directory(&mut reader, partition_offset, &dpb)?;
        let mut entries = Vec::with_capacity(dir_bytes.len() / 32);
        for chunk in dir_bytes.chunks_exact(32) {
            let arr: &[u8; 32] = chunk.try_into().unwrap();
            entries.push(parse_dir_entry(arr, &dpb));
        }
        Ok(CpmFilesystem {
            reader,
            partition_offset,
            dpb,
            entries,
            dir_bytes_cache: dir_bytes,
        })
    }

    /// Byte offset of allocation block `b` within the volume (after
    /// the reserved-track skip).
    fn block_byte_offset(&self, b: u16) -> u64 {
        let reserved = self.dpb.off as u64 * self.dpb.spt as u64 * 128;
        // Each block is `block_size` bytes; the data area starts at the
        // first non-reserved logical record. block 0 starts at the first
        // record of the data area.
        reserved + b as u64 * self.dpb.block_size() as u64
    }

    /// Read all 32-byte records that make up the directory and group
    /// them by (user, name, ext). Returns a Vec of (name, total_size,
    /// sorted_extents). Skips deleted/inactive entries.
    fn collect_files(&self) -> Vec<CpmFileGroup> {
        use std::collections::HashMap;
        let mut map: HashMap<(u8, String, String), Vec<&CpmDirEntry>> = HashMap::new();
        for e in &self.entries {
            if !e.is_active() {
                continue;
            }
            let key = (e.user, e.name.clone(), e.ext.clone());
            map.entry(key).or_default().push(e);
        }
        let mut out = Vec::with_capacity(map.len());
        for ((user, name, ext), mut exts) in map {
            exts.sort_by_key(|e| e.extent);
            // Total file size from extents:
            //   for every non-last extent: Rc * 128
            //   for last extent: if Bc != 0 then (Rc - 1) * 128 + Bc
            //                    else Rc * 128
            let mut total: u64 = 0;
            let last_idx = exts.len() - 1;
            for (i, e) in exts.iter().enumerate() {
                if i == last_idx && e.byte_count != 0 {
                    total += (e.record_count.saturating_sub(1) as u64) * 128 + e.byte_count as u64;
                } else {
                    total += e.record_count as u64 * 128;
                }
            }
            out.push(CpmFileGroup {
                user,
                name,
                ext,
                attrs: exts[0].attrs,
                total_size: total,
                extents: exts.into_iter().cloned().collect(),
            });
        }
        out.sort_by_key(|g| g.formatted_name());
        out
    }

    /// Find a group by formatted name (case-insensitive).
    fn find_group(&self, name: &str) -> Option<CpmFileGroup> {
        self.collect_files()
            .into_iter()
            .find(|g| g.formatted_name().eq_ignore_ascii_case(name))
    }

    /// Read all bytes of a file given its grouped extents.
    fn read_group(&mut self, group: &CpmFileGroup) -> Result<Vec<u8>, FilesystemError> {
        let mut data = Vec::with_capacity(group.total_size as usize);
        let bs = self.dpb.block_size();
        let mut written: u64 = 0;
        let last_extent_num = group.extents.last().map(|e| e.extent);
        for ext in &group.extents {
            // Bytes covered = sum of non-zero pointers, but trimmed
            // to extent.record_count * 128. The very last extent uses
            // byte_count (CP/M-3) for the trailing trim when non-zero.
            let is_last = Some(ext.extent) == last_extent_num;
            let bytes_in_ext = if is_last && ext.byte_count != 0 {
                (ext.record_count.saturating_sub(1) as u64) * 128 + ext.byte_count as u64
            } else {
                ext.record_count as u64 * 128
            };
            let mut remaining = bytes_in_ext;
            for &ptr in &ext.block_ptrs {
                if ptr == 0 || remaining == 0 {
                    break;
                }
                if ptr as u32 >= self.dpb.total_blocks() {
                    return Err(FilesystemError::InvalidData(format!(
                        "CP/M block pointer {ptr} >= total blocks {}",
                        self.dpb.total_blocks()
                    )));
                }
                let off = self.partition_offset + self.block_byte_offset(ptr);
                self.reader.seek(SeekFrom::Start(off))?;
                let want = (bs as u64).min(remaining) as usize;
                let mut buf = vec![0u8; want];
                self.reader.read_exact(&mut buf)?;
                data.extend_from_slice(&buf);
                remaining -= want as u64;
                written += want as u64;
            }
            let _ = written;
        }
        Ok(data)
    }
}

/// One CP/M file: collected extents under one (user, name, ext).
#[derive(Debug, Clone)]
struct CpmFileGroup {
    user: u8,
    name: String,
    ext: String,
    attrs: u8,
    total_size: u64,
    extents: Vec<CpmDirEntry>,
}

impl CpmFileGroup {
    fn formatted_name(&self) -> String {
        let n = self.name.trim_end();
        let e = self.ext.trim_end();
        if e.is_empty() {
            n.to_string()
        } else {
            format!("{n}.{e}")
        }
    }
}

/// Read the directory bytes from disk based on the DPB's al0/al1 bitmap.
fn read_directory<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    dpb: &Dpb,
) -> Result<Vec<u8>, FilesystemError> {
    // `spt` is records-per-track (128-B records), so reserved-bytes is
    // `off × spt × 128`. The old formula's extra `(sector_size / 128)`
    // factor double-counted for sector_size > 128 (e.g. PCW 512-B sectors
    // landed the directory at 4× the correct byte offset).
    let reserved = dpb.off as u64 * dpb.spt as u64 * 128;
    let bs = dpb.block_size() as u64;
    let mut out = Vec::new();
    let bitmap = ((dpb.al0 as u16) << 8) | dpb.al1 as u16;
    for b in 0..16 {
        if bitmap & (0x8000 >> b) != 0 {
            let off = partition_offset + reserved + b as u64 * bs;
            reader.seek(SeekFrom::Start(off))?;
            let mut buf = vec![0u8; bs as usize];
            reader.read_exact(&mut buf)?;
            out.extend_from_slice(&buf);
        }
    }
    Ok(out)
}

impl<R: Read + Seek + Send> Filesystem for CpmFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if entry.path != "/" {
            return Ok(Vec::new()); // CP/M is flat.
        }
        let mut out = Vec::new();
        for g in self.collect_files() {
            let name = g.formatted_name();
            let mut fe = FileEntry::new_file(
                name.clone(),
                format!("/{name}"),
                g.total_size,
                u64::from_be_bytes([g.user, 0, 0, 0, 0, 0, 0, 0]),
            );
            if g.attrs & 0x80 != 0 {
                fe.special_type = Some("R/O".to_string());
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
        let group = self.find_group(&entry.name).ok_or_else(|| {
            FilesystemError::NotFound(format!("CP/M file {} not found", entry.name))
        })?;
        let mut data = self.read_group(&group)?;
        data.truncate(max_bytes);
        Ok(data)
    }

    fn fs_type(&self) -> &str {
        "CP/M"
    }

    fn volume_label(&self) -> Option<&str> {
        Some(self.dpb.name)
    }

    fn total_size(&self) -> u64 {
        self.dpb.data_bytes()
    }

    fn used_size(&self) -> u64 {
        let mut bytes: u64 = 0;
        for g in self.collect_files() {
            bytes += g.total_size;
        }
        bytes
    }
}

// ============================================================================
// EditableFilesystem (Add/Delete)
// ============================================================================

/// Validate a candidate "NAME.EXT" filename for `create_file`. Returns
/// (name_8, ext_3) padded with spaces and upper-cased. No `:` (drive-
/// separator), no path components.
fn validate_cpm_name(name: &str) -> Result<(String, String), FilesystemError> {
    if name.is_empty() {
        return Err(FilesystemError::InvalidData("empty filename".into()));
    }
    if name.contains('/') || name.contains('\\') || name.contains(':') {
        return Err(FilesystemError::InvalidData(format!(
            "filename '{name}' may not contain '/', '\\\\', or ':'"
        )));
    }
    let (n, e) = match name.rsplit_once('.') {
        Some((n, e)) => (n.to_string(), e.to_string()),
        None => (name.to_string(), String::new()),
    };
    if n.is_empty() || n.len() > 8 {
        return Err(FilesystemError::InvalidData(format!(
            "name part '{n}' must be 1..8 chars"
        )));
    }
    if e.len() > 3 {
        return Err(FilesystemError::InvalidData(format!(
            "ext part '{e}' must be ≤ 3 chars"
        )));
    }
    for c in n.chars().chain(e.chars()) {
        if !c.is_ascii() || !(0x20..=0x7E).contains(&(c as u8)) || c.is_ascii_whitespace() {
            return Err(FilesystemError::InvalidData(format!(
                "filename '{name}' contains invalid char '{c}'"
            )));
        }
    }
    let upper_n = n.to_ascii_uppercase();
    let upper_e = e.to_ascii_uppercase();
    let padded_n = format!("{:<8}", upper_n);
    let padded_e = format!("{:<3}", upper_e);
    Ok((padded_n, padded_e))
}

impl<R: Read + Write + Seek + Send> CpmFilesystem<R> {
    /// Free blocks: the set of allocation-block indices not pointed at
    /// by any active directory entry (and excluding the directory
    /// blocks themselves).
    fn free_blocks(&self) -> Vec<u16> {
        let total = self.dpb.total_blocks() as u16;
        let bitmap = ((self.dpb.al0 as u16) << 8) | self.dpb.al1 as u16;
        let mut used = vec![false; total as usize];
        for b in 0..16u16 {
            if bitmap & (0x8000 >> b) != 0 && (b as u32) < total as u32 {
                used[b as usize] = true;
            }
        }
        for e in &self.entries {
            if !e.is_active() {
                continue;
            }
            for &ptr in &e.block_ptrs {
                if ptr != 0 && (ptr as u32) < total as u32 {
                    used[ptr as usize] = true;
                }
            }
        }
        (0..total).filter(|&i| !used[i as usize]).collect()
    }

    /// Write data into the named extent's block pointers.
    fn write_blocks(&mut self, ptrs: &[u16], data: &[u8]) -> Result<(), FilesystemError> {
        let bs = self.dpb.block_size();
        let mut off_in_data = 0usize;
        for &ptr in ptrs {
            if ptr == 0 {
                continue;
            }
            let abs = self.partition_offset + self.block_byte_offset(ptr);
            self.reader.seek(SeekFrom::Start(abs))?;
            let want = (bs).min(data.len() - off_in_data);
            let mut chunk = vec![0u8; bs];
            chunk[..want].copy_from_slice(&data[off_in_data..off_in_data + want]);
            self.reader.write_all(&chunk)?;
            off_in_data += want;
            if off_in_data >= data.len() {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Re-pack the entries Vec back into directory bytes and write to disk.
    fn dir_write_back(&mut self) -> Result<(), FilesystemError> {
        // Recompute the directory byte buffer from the entries Vec.
        let mut new_bytes = vec![0xE5u8; self.dir_bytes_cache.len()];
        for (i, e) in self.entries.iter().enumerate() {
            let bytes = encode_dir_entry(e, &self.dpb);
            let off = i * 32;
            if off + 32 > new_bytes.len() {
                return Err(FilesystemError::InvalidData(
                    "directory full: more entries than DPB.drm".into(),
                ));
            }
            new_bytes[off..off + 32].copy_from_slice(&bytes);
        }
        self.dir_bytes_cache = new_bytes.clone();
        // Write back to the blocks indicated by al0/al1.
        let reserved = self.dpb.off as u64 * self.dpb.spt as u64 * 128;
        let bs = self.dpb.block_size() as u64;
        let bitmap = ((self.dpb.al0 as u16) << 8) | self.dpb.al1 as u16;
        let mut written: u64 = 0;
        for b in 0..16 {
            if bitmap & (0x8000 >> b) != 0 {
                let off = self.partition_offset + reserved + b as u64 * bs;
                let want = (bs as usize).min(new_bytes.len() - written as usize);
                if want == 0 {
                    break;
                }
                self.reader.seek(SeekFrom::Start(off))?;
                self.reader
                    .write_all(&new_bytes[written as usize..written as usize + want])?;
                // Pad rest of this directory block with 0xE5 to keep
                // CP/M's "deleted" marker convention.
                if want < bs as usize {
                    let pad = vec![0xE5u8; bs as usize - want];
                    self.reader.write_all(&pad)?;
                }
                written += want as u64;
            }
        }
        Ok(())
    }
}

impl<R: Read + Write + Seek + Send> EditableFilesystem for CpmFilesystem<R> {
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
                "CP/M is flat; only the root accepts new files".into(),
            ));
        }
        let (n8, e3) = validate_cpm_name(name)?;
        if self
            .collect_files()
            .iter()
            .any(|g| g.name.eq_ignore_ascii_case(&n8) && g.ext.eq_ignore_ascii_case(&e3))
        {
            return Err(FilesystemError::InvalidData(format!(
                "CP/M file '{name}' already exists"
            )));
        }
        let mut payload = Vec::with_capacity(data_len as usize);
        data.read_to_end(&mut payload)?;
        if payload.len() as u64 != data_len {
            return Err(FilesystemError::InvalidData(format!(
                "data_len {data_len} != actual {}",
                payload.len()
            )));
        }

        // Round up to record (128 B) boundary for record-count accounting.
        let total_records = payload.len().div_ceil(128) as u32;
        let bytes_in_last_record = (payload.len() % 128) as u8;

        // Allocate blocks.
        let bs = self.dpb.block_size();
        let blocks_needed = payload.len().div_ceil(bs).max(1);
        let free = self.free_blocks();
        if free.len() < blocks_needed {
            return Err(FilesystemError::InvalidData(format!(
                "out of space: need {blocks_needed} blocks, have {} free",
                free.len()
            )));
        }
        let picked: Vec<u16> = free.into_iter().take(blocks_needed).collect();

        // Write data into those blocks.
        if !payload.is_empty() {
            // Treat payload as a single contiguous write across picked blocks.
            self.write_blocks(&picked, &payload)?;
        }

        // Build directory extents. Each extent holds 16 or 8 block
        // pointers, but Rc is capped at 128 records per extent. For
        // most one-block-size disks the limits coincide; for larger
        // block sizes Rc is the binding limit.
        let records_per_block = self.dpb.records_per_block();
        let ptrs_per_ext = if self.dpb.uses_word_pointers() { 8 } else { 16 };
        let records_per_extent_via_ptrs = ptrs_per_ext as u32 * records_per_block;
        let records_per_extent = records_per_extent_via_ptrs.min(128);

        // Find a free entry slot to extend / push into.
        let max_entries = self.dpb.max_dir_entries() as usize;
        if self.entries.len() < max_entries {
            self.entries.resize(
                max_entries,
                CpmDirEntry {
                    user: 0xE5,
                    name: " ".repeat(8),
                    ext: " ".repeat(3),
                    attrs: 0,
                    extent: 0,
                    record_count: 0,
                    byte_count: 0,
                    block_ptrs: if self.dpb.uses_word_pointers() {
                        vec![0u16; 8]
                    } else {
                        vec![0u16; 16]
                    },
                },
            );
        }

        let mut placed = 0usize;
        let mut records_left = total_records;
        let mut block_cursor = 0usize;
        let mut ext_idx: u16 = 0;
        while records_left > 0 {
            let slot = self.entries.iter().position(|e| !e.is_active());
            let Some(idx) = slot else {
                return Err(FilesystemError::InvalidData(
                    "directory full: no free entry slot for new extent".into(),
                ));
            };
            // Records covered by this extent.
            let rc = records_left.min(records_per_extent) as u8;
            // Pointer slice for this extent.
            let blocks_for_ext = (rc as u32).div_ceil(records_per_block) as usize;
            let take = blocks_for_ext.min(picked.len() - block_cursor);
            let mut ptrs = vec![0u16; ptrs_per_ext];
            ptrs[..take].copy_from_slice(&picked[block_cursor..block_cursor + take]);
            block_cursor += take;
            let is_last = records_left <= records_per_extent;
            let bc = if is_last { bytes_in_last_record } else { 0 };
            self.entries[idx] = CpmDirEntry {
                user: 0,
                name: n8.clone(),
                ext: e3.clone(),
                attrs: 0,
                extent: ext_idx,
                record_count: rc,
                byte_count: bc,
                block_ptrs: ptrs,
            };
            placed += 1;
            records_left = records_left.saturating_sub(rc as u32);
            ext_idx += 1;
        }
        let _ = placed;

        let formatted = format!("{}.{}", n8.trim_end(), e3.trim_end())
            .trim_end_matches('.')
            .to_string();
        let fe = FileEntry::new_file(
            formatted.clone(),
            format!("/{formatted}"),
            payload.len() as u64,
            0,
        );
        Ok(fe)
    }

    fn create_directory(
        &mut self,
        _parent: &FileEntry,
        _name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "CP/M has no directory hierarchy — files are addressed by (user, name)".into(),
        ))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        if parent.path != "/" {
            return Err(FilesystemError::Unsupported(
                "CP/M is flat; only files in the root are deletable".into(),
            ));
        }
        if entry.entry_type != EntryType::File {
            return Err(FilesystemError::InvalidData(
                "CP/M only stores files".into(),
            ));
        }
        let name = &entry.name;
        let (n8, e3) = name
            .rsplit_once('.')
            .map(|(n, e)| {
                (
                    format!("{:<8}", n.to_ascii_uppercase()),
                    format!("{:<3}", e.to_ascii_uppercase()),
                )
            })
            .unwrap_or((
                format!("{:<8}", name.to_ascii_uppercase()),
                "   ".to_string(),
            ));
        let mut hit = false;
        for e in self.entries.iter_mut() {
            if !e.is_active() {
                continue;
            }
            if e.name.eq_ignore_ascii_case(&n8) && e.ext.eq_ignore_ascii_case(&e3) {
                e.user = 0xE5;
                hit = true;
            }
        }
        if !hit {
            return Err(FilesystemError::NotFound(format!(
                "CP/M file '{name}' not in directory"
            )));
        }
        Ok(())
    }

    fn rename(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
        new_name: &str,
    ) -> Result<(), FilesystemError> {
        if parent.path != "/" {
            return Err(FilesystemError::Unsupported(
                "CP/M is flat; only files in the root are renameable".into(),
            ));
        }
        if entry.entry_type != EntryType::File {
            return Err(FilesystemError::InvalidData(
                "CP/M only stores files".into(),
            ));
        }
        if new_name == entry.name {
            return Ok(());
        }
        let (new_n8, new_e3) = validate_cpm_name(new_name)?;

        // Resolve the entry's current padded (name, ext) the same way
        // delete_entry does, so we can match its directory extents.
        let old = &entry.name;
        let (old_n8, old_e3) = old
            .rsplit_once('.')
            .map(|(n, e)| {
                (
                    format!("{:<8}", n.to_ascii_uppercase()),
                    format!("{:<3}", e.to_ascii_uppercase()),
                )
            })
            .unwrap_or((
                format!("{:<8}", old.to_ascii_uppercase()),
                "   ".to_string(),
            ));

        // Reject a collision with a *different* file group. CP/M folds
        // case (names are stored uppercase), so a case-only rename can
        // never collide with another entry — it resolves to the same
        // (name, ext) we're already on, so it's a no-op skip.
        if (new_n8 != old_n8 || new_e3 != old_e3)
            && self.collect_files().iter().any(|g| {
                g.name.eq_ignore_ascii_case(&new_n8) && g.ext.eq_ignore_ascii_case(&new_e3)
            })
        {
            return Err(FilesystemError::AlreadyExists(new_name.to_string()));
        }

        // A file occupies multiple directory entries (one per extent),
        // all sharing (user, name, ext). Rewrite the name/ext on EVERY
        // matching active extent, preserving the per-entry attribute bits
        // (R/O, SYS, ARC) and all identity fields (extent #, block ptrs).
        let mut hit = false;
        for e in self.entries.iter_mut() {
            if !e.is_active() {
                continue;
            }
            if e.name.eq_ignore_ascii_case(&old_n8) && e.ext.eq_ignore_ascii_case(&old_e3) {
                e.name = new_n8.clone();
                e.ext = new_e3.clone();
                hit = true;
            }
        }
        if !hit {
            return Err(FilesystemError::NotFound(format!(
                "CP/M file '{old}' not in directory"
            )));
        }
        self.dir_write_back()?;
        Ok(())
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.dir_write_back()?;
        self.reader.flush()?;
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.free_blocks().len() as u64 * self.dpb.block_size() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::super::cpm_diskdefs::AMSTRAD_DATA;
    use super::*;
    use std::io::Cursor;

    /// Build a fresh Amstrad-data-format CP/M disk in memory with one
    /// pre-seeded file "HELLO.TXT" containing 64 bytes of test data.
    /// Uses block 2 for the file data (blocks 0/1 hold the directory
    /// per al0=0xC0).
    fn build_amstrad_disk_with_one_file() -> Vec<u8> {
        let dpb = AMSTRAD_DATA;
        // Disk size: data_bytes + reserved_tracks * spt * 128. `spt` is
        // already in 128-B records, so the reserved bytes are exactly
        // `off × spt × 128` — no further sector-size scaling needed.
        let reserved = dpb.off as usize * dpb.spt as usize * 128;
        let total = reserved + dpb.data_bytes() as usize;
        let mut disk = vec![0xE5u8; total];

        // Zero the data area + reserved area; we used 0xE5 for the
        // entire buffer so directory blocks start as "all empty".
        // Now stamp one active directory entry at offset 0 of block 0.
        let dir_off = reserved; // block 0 starts here
        let mut entry = [0u8; 32];
        entry[0] = 0; // user 0
        entry[1..9].copy_from_slice(b"HELLO   ");
        entry[9..12].copy_from_slice(b"TXT");
        // Xl, Bc, Xh, Rc
        entry[12] = 0; // ext 0
        entry[13] = 0;
        entry[14] = 0;
        // File data at block 2. Define payload first so we can stamp
        // the byte-count (Bc) accurately in the directory entry.
        let payload = b"Hello, CP/M! This is the seed content for our CP/M unit test.\n";
        entry[15] = 1; // 1 record (128 B)
        entry[13] = payload.len() as u8; // CP/M-3 byte count in last record
                                         // Block pointer: byte ptr (AMSTRAD_DATA has dsm = 179 < 256).
        entry[16] = 2; // block 2
        disk[dir_off..dir_off + 32].copy_from_slice(&entry);

        let bs = dpb.block_size();
        let block_off = reserved + 2 * bs;
        disk[block_off..block_off + payload.len()].copy_from_slice(payload);
        disk
    }

    #[test]
    fn opens_seed_disk_and_lists_one_file() {
        let disk = build_amstrad_disk_with_one_file();
        let cur = Cursor::new(disk);
        let mut fs = CpmFilesystem::open_with_dpb(cur, 0, AMSTRAD_DATA).unwrap();
        assert_eq!(fs.fs_type(), "CP/M");
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "HELLO.TXT");
        // 62-byte payload; CP/M-3 Bc = 62 in the directory entry.
        assert_eq!(entries[0].size, 62);
    }

    #[test]
    fn reads_seed_file_byte_exact() {
        let disk = build_amstrad_disk_with_one_file();
        let cur = Cursor::new(disk);
        let mut fs = CpmFilesystem::open_with_dpb(cur, 0, AMSTRAD_DATA).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let data = fs.read_file(&entries[0], 4096).unwrap();
        assert_eq!(
            &data,
            b"Hello, CP/M! This is the seed content for our CP/M unit test.\n"
        );
    }

    #[test]
    fn parse_dir_entry_extracts_extent_number_correctly() {
        let mut buf = [0u8; 32];
        buf[0] = 0;
        buf[1..9].copy_from_slice(b"FOO     ");
        buf[9..12].copy_from_slice(b"BAR");
        buf[12] = 3; // Xl
        buf[14] = 1; // Xh
        let parsed = parse_dir_entry(&buf, &AMSTRAD_DATA);
        // extent = Xl | Xh << 5 = 3 | 32 = 35.
        assert_eq!(parsed.extent, 35);
        assert_eq!(parsed.user, 0);
        assert!(parsed.is_active());
    }

    #[test]
    fn validate_cpm_name_accepts_8_3_and_rejects_garbage() {
        assert!(validate_cpm_name("FOO.BAR").is_ok());
        assert!(validate_cpm_name("HELLO.TXT").is_ok());
        // No ext.
        assert!(validate_cpm_name("READ").is_ok());
        // Path separator.
        assert!(validate_cpm_name("FOO/BAR").is_err());
        // Too long.
        assert!(validate_cpm_name("ABCDEFGHI.TXT").is_err());
        assert!(validate_cpm_name("FOO.BARS").is_err());
        // Empty.
        assert!(validate_cpm_name("").is_err());
    }

    #[test]
    fn create_then_read_round_trips_through_sync() {
        let disk = build_amstrad_disk_with_one_file();
        let mut fs = CpmFilesystem::open_with_dpb(Cursor::new(disk), 0, AMSTRAD_DATA).unwrap();
        let root = fs.root().unwrap();
        let payload = b"created via EditableFilesystem on CP/M".to_vec();
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
        fs.sync_metadata().unwrap();

        // Reopen the same backing store and prove the new file is there.
        let inner = fs.reader.clone();
        let mut fs2 = CpmFilesystem::open_with_dpb(inner, 0, AMSTRAD_DATA).unwrap();
        let root2 = fs2.root().unwrap();
        let entries = fs2.list_directory(&root2).unwrap();
        let new = entries.iter().find(|e| e.name == "NEW.TXT").unwrap();
        assert_eq!(new.size, payload.len() as u64);
        let got = fs2.read_file(new, 4096).unwrap();
        assert_eq!(got, payload);
    }

    #[test]
    fn delete_marks_user_e5_and_frees_blocks() {
        let disk = build_amstrad_disk_with_one_file();
        let mut fs = CpmFilesystem::open_with_dpb(Cursor::new(disk), 0, AMSTRAD_DATA).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let target = entries
            .iter()
            .find(|e| e.name == "HELLO.TXT")
            .unwrap()
            .clone();
        let before_free = EditableFilesystem::free_space(&mut fs).unwrap();
        fs.delete_entry(&root, &target).unwrap();
        fs.sync_metadata().unwrap();

        let inner = fs.reader.clone();
        let mut fs2 = CpmFilesystem::open_with_dpb(inner, 0, AMSTRAD_DATA).unwrap();
        let root2 = fs2.root().unwrap();
        let entries = fs2.list_directory(&root2).unwrap();
        assert!(entries.iter().all(|e| e.name != "HELLO.TXT"));
        let after_free = EditableFilesystem::free_space(&mut fs2).unwrap();
        assert!(after_free > before_free, "block 2 should be freed");
    }

    #[test]
    fn create_directory_is_unsupported() {
        let disk = build_amstrad_disk_with_one_file();
        let mut fs = CpmFilesystem::open_with_dpb(Cursor::new(disk), 0, AMSTRAD_DATA).unwrap();
        let root = fs.root().unwrap();
        let err = fs
            .create_directory(&root, "SUB", &CreateDirectoryOptions::default())
            .unwrap_err();
        assert!(matches!(err, FilesystemError::Unsupported(_)));
    }

    #[test]
    fn create_file_rejects_duplicates() {
        let disk = build_amstrad_disk_with_one_file();
        let mut fs = CpmFilesystem::open_with_dpb(Cursor::new(disk), 0, AMSTRAD_DATA).unwrap();
        let root = fs.root().unwrap();
        let mut src = Cursor::new(b"x".to_vec());
        let err = fs
            .create_file(
                &root,
                "HELLO.TXT",
                &mut src,
                1,
                &CreateFileOptions::default(),
            )
            .unwrap_err();
        assert!(matches!(err, FilesystemError::InvalidData(_)));
    }

    #[test]
    fn rename_changes_name_preserves_content() {
        let disk = build_amstrad_disk_with_one_file();
        let mut fs = CpmFilesystem::open_with_dpb(Cursor::new(disk), 0, AMSTRAD_DATA).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let target = entries
            .iter()
            .find(|e| e.name == "HELLO.TXT")
            .unwrap()
            .clone();
        let original = fs.read_file(&target, 4096).unwrap();
        fs.rename(&root, &target, "GREET.TXT").unwrap();
        fs.sync_metadata().unwrap();

        // Reopen the backing store; old name gone, new name present,
        // contents byte-identical (same blocks).
        let inner = fs.reader.clone();
        let mut fs2 = CpmFilesystem::open_with_dpb(inner, 0, AMSTRAD_DATA).unwrap();
        let root2 = fs2.root().unwrap();
        let entries = fs2.list_directory(&root2).unwrap();
        assert!(entries.iter().all(|e| e.name != "HELLO.TXT"));
        let renamed = entries.iter().find(|e| e.name == "GREET.TXT").unwrap();
        assert_eq!(renamed.size, target.size);
        let got = fs2.read_file(renamed, 4096).unwrap();
        assert_eq!(got, original);
    }
}
