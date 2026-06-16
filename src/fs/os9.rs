//! OS-9 / NitrOS-9 RBF filesystem — the hierarchical Unix-like floppy (and
//! hard-disk) filesystem used by Microware OS-9 Level 1/2 on the Tandy Color
//! Computer and Dragon, and by the MiSTer CoCo2/CoCo3 cores.
//!
//! Unlike RS-DOS / Disk BASIC (a flat granule filesystem — see
//! [`super::rsdos`]), OS-9 RBF is genuinely hierarchical: every file and
//! directory is described by a 256-byte **file descriptor** (FD) sector, and
//! directories are ordinary files whose contents are 32-byte entries pairing
//! a name with the LSN of the entry's FD. The host OS can't mount these
//! disks, so extract + add + delete is the whole point (Axis 1 of the MiSTer
//! plan — floppy-only, no resize).
//!
//! ## On-disk layout (reference: toolshed `os9` / `libos9`, OS-9 RBF tech
//! manual)
//!
//! Sectors are addressed by **LSN** (logical sector number, 0-based). The
//! sector size is fixed per device (256 bytes on CoCo/Dragon floppies); we
//! derive it from `DD.TOT * sector == image length`.
//!
//! - **LSN 0 — identification sector** (big-endian throughout):
//!   ```text
//!   0x00 DD.TOT  3   total sectors on the volume
//!   0x03 DD.TKS  1   track size in sectors
//!   0x04 DD.MAP  2   number of bytes in the allocation bitmap
//!   0x06 DD.BIT  2   sectors per cluster (allocation unit, per bitmap bit)
//!   0x08 DD.DIR  3   LSN of the root directory's FD
//!   0x0B DD.OWN  2   owner ID
//!   0x0D DD.ATT  1   disk attributes
//!   0x0E DD.DSK  2   disk ID
//!   0x10 DD.FMT  1   format byte (sides / density)
//!   0x11 DD.SPT  2   sectors per track
//!   0x1F DD.NAM  ..  volume name, last char has bit 7 set
//!   ```
//! - **LSN 1.. — allocation bitmap**: `DD.MAP` bytes, MSB-first. A set bit
//!   marks the corresponding cluster (`DD.BIT` sectors) as **allocated**;
//!   clear = free. (Opposite polarity to the Amiga filesystems.)
//! - **File descriptor (FD)** sector:
//!   ```text
//!   0x00 FD.ATT  1   attributes: D S PE PW PR E W R (bit7 D = directory)
//!   0x01 FD.OWN  2   owner ID
//!   0x03 FD.DAT  5   last-modified date (Y M D H M)
//!   0x08 FD.LNK  1   link count
//!   0x09 FD.SIZ  4   file size in bytes
//!   0x0D FD.DCR  3   creation date (Y M D)
//!   0x10 FD.SEG  ..  segment list: 5 bytes each (3-byte LSN + 2-byte
//!                    sector count), terminated by a zero-LSN entry. Up to
//!                    48 segments fit in the remaining 240 bytes.
//!   ```
//! - **Directory data**: 32-byte entries — 29-byte name (last char's bit 7
//!   set; a leading `0x00` marks a deleted slot) + 3-byte FD LSN. The first
//!   two live entries are conventionally `..` (parent) and `.` (self).

use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::FileEntry;
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};

/// CoCo/Dragon OS-9 floppies use 256-byte sectors. We still derive the
/// effective sector size from the identification sector and accept 512 for
/// completeness, but reject anything else.
const SECTOR_MIN: u64 = 256;

const DIR_ENTRY_LEN: usize = 32;
const NAME_LEN: usize = 29;
const SEG_LIST_OFFSET: usize = 16;
const SEG_ENTRY_LEN: usize = 5;
const MAX_SEGMENTS: usize = (256 - SEG_LIST_OFFSET) / SEG_ENTRY_LEN; // 48

/// Directory attribute bit in FD.ATT.
const ATT_DIR: u8 = 0x80;

/// Runaway guards.
const MAX_SEGMENTS_WALK: usize = MAX_SEGMENTS;
const MAX_DIR_ENTRIES: usize = 4096;

fn u24_be(b: &[u8]) -> u64 {
    ((b[0] as u64) << 16) | ((b[1] as u64) << 8) | (b[2] as u64)
}

fn put_u24_be(v: u64) -> [u8; 3] {
    [(v >> 16) as u8, (v >> 8) as u8, v as u8]
}

/// Decode an OS-9 high-bit-terminated name. Reads characters until one has
/// bit 7 set (that char, masked, is the last) or the field ends.
fn decode_os9_name(raw: &[u8]) -> String {
    let mut s = String::new();
    for &b in raw {
        if b == 0 {
            break;
        }
        s.push((b & 0x7F) as char);
        if b & 0x80 != 0 {
            break;
        }
    }
    s
}

/// Encode a name into the 29-byte directory name field: plain ASCII with bit
/// 7 set on the final character, zero-padded. OS-9 names are 1..29 chars from
/// the set `A-Z a-z 0-9 . _ $`, and must start with a letter or `.`.
fn encode_os9_name(name: &str) -> Result<[u8; NAME_LEN], FilesystemError> {
    if name.is_empty() || name.chars().count() > NAME_LEN {
        return Err(FilesystemError::InvalidData(format!(
            "OS-9 name '{name}' must be 1..{NAME_LEN} characters"
        )));
    }
    let valid = |c: char| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '$');
    if !name.chars().all(valid) {
        return Err(FilesystemError::InvalidData(format!(
            "OS-9 name '{name}' may only contain A-Z a-z 0-9 . _ $"
        )));
    }
    let first = name.chars().next().unwrap();
    if !(first.is_ascii_alphabetic() || first == '.') {
        return Err(FilesystemError::InvalidData(format!(
            "OS-9 name '{name}' must start with a letter or '.'"
        )));
    }
    let mut out = [0u8; NAME_LEN];
    let bytes = name.as_bytes();
    out[..bytes.len()].copy_from_slice(bytes);
    out[bytes.len() - 1] |= 0x80; // terminator high bit
    Ok(out)
}

/// Parsed LSN-0 identification sector.
#[derive(Debug, Clone)]
pub struct Os9Ident {
    pub total_sectors: u64,
    pub bitmap_bytes: u64,
    pub sectors_per_cluster: u64,
    pub root_dir_lsn: u64,
    pub sectors_per_track: u64,
    pub volume_name: String,
}

impl Os9Ident {
    fn parse(lsn0: &[u8]) -> Option<Os9Ident> {
        if lsn0.len() < 32 {
            return None;
        }
        let total_sectors = u24_be(&lsn0[0..3]);
        let bitmap_bytes = u16::from_be_bytes([lsn0[4], lsn0[5]]) as u64;
        let mut sectors_per_cluster = u16::from_be_bytes([lsn0[6], lsn0[7]]) as u64;
        if sectors_per_cluster == 0 {
            sectors_per_cluster = 1;
        }
        let root_dir_lsn = u24_be(&lsn0[8..11]);
        let sectors_per_track = u16::from_be_bytes([lsn0[17], lsn0[18]]) as u64;
        let volume_name = decode_os9_name(&lsn0[31..lsn0.len().min(31 + 32)]);
        Some(Os9Ident {
            total_sectors,
            bitmap_bytes,
            sectors_per_cluster,
            root_dir_lsn,
            sectors_per_track,
            volume_name,
        })
    }
}

/// A parsed file descriptor.
#[derive(Debug, Clone)]
struct FileDescriptor {
    attributes: u8,
    size: u64,
    /// Segment list: `(lsn, sector_count)` runs.
    segments: Vec<(u64, u64)>,
}

impl FileDescriptor {
    fn parse(fd: &[u8]) -> FileDescriptor {
        let attributes = fd[0];
        let size = u32::from_be_bytes([fd[9], fd[10], fd[11], fd[12]]) as u64;
        let mut segments = Vec::new();
        let mut off = SEG_LIST_OFFSET;
        for _ in 0..MAX_SEGMENTS_WALK {
            if off + SEG_ENTRY_LEN > fd.len() {
                break;
            }
            let lsn = u24_be(&fd[off..off + 3]);
            let count = u16::from_be_bytes([fd[off + 3], fd[off + 4]]) as u64;
            if lsn == 0 || count == 0 {
                break;
            }
            segments.push((lsn, count));
            off += SEG_ENTRY_LEN;
        }
        FileDescriptor {
            attributes,
            size,
            segments,
        }
    }

    fn is_dir(&self) -> bool {
        self.attributes & ATT_DIR != 0
    }
}

/// Sniff whether the flat image at `partition_offset` is an OS-9 RBF volume.
/// There is no magic number; we validate the identification sector against
/// the image length and confirm the root directory FD is a directory whose
/// size is a whole number of 32-byte entries. That combination reliably
/// rejects RS-DOS (same byte size) and random blobs.
pub fn looks_like_os9<R: Read + Seek>(reader: &mut R, partition_offset: u64) -> Option<Os9Ident> {
    let len = reader
        .seek(SeekFrom::End(0))
        .ok()?
        .checked_sub(partition_offset)?;
    if len == 0 {
        return None;
    }
    reader.seek(SeekFrom::Start(partition_offset)).ok()?;
    let mut lsn0 = [0u8; 256];
    reader.read_exact(&mut lsn0).ok()?;
    let id = Os9Ident::parse(&lsn0)?;

    if id.total_sectors == 0 {
        return None;
    }
    // Sector size is implied by total_sectors dividing the image evenly.
    if !len.is_multiple_of(id.total_sectors) {
        return None;
    }
    let sector = len / id.total_sectors;
    if sector < SECTOR_MIN || !matches!(sector, 256 | 512) {
        return None;
    }
    // Bitmap must hold at least one bit per cluster and start at LSN 1.
    let clusters = id.total_sectors.div_ceil(id.sectors_per_cluster);
    if id.bitmap_bytes == 0 || id.bitmap_bytes * 8 < clusters {
        return None;
    }
    // Root directory FD must be in range and actually be a directory.
    if id.root_dir_lsn == 0 || id.root_dir_lsn >= id.total_sectors {
        return None;
    }
    reader
        .seek(SeekFrom::Start(partition_offset + id.root_dir_lsn * sector))
        .ok()?;
    let mut fdbuf = vec![0u8; sector as usize];
    reader.read_exact(&mut fdbuf).ok()?;
    let fd = FileDescriptor::parse(&fdbuf);
    if !fd.is_dir() || fd.segments.is_empty() {
        return None;
    }
    // Directory size is a whole number of entries, and the first segment is
    // in range.
    if fd.size == 0 || !fd.size.is_multiple_of(DIR_ENTRY_LEN as u64) {
        return None;
    }
    let (seg_lsn, seg_cnt) = fd.segments[0];
    if seg_lsn == 0 || seg_lsn + seg_cnt > id.total_sectors {
        return None;
    }
    Some(id)
}

/// An OS-9 / NitrOS-9 RBF filesystem over a flat sector image.
pub struct Os9Filesystem<R: Read + Seek + Send> {
    reader: R,
    partition_offset: u64,
    sector: u64,
    ident: Os9Ident,
    /// Allocation bitmap (`DD.MAP` bytes), loaded from LSN 1..
    bitmap: Vec<u8>,
}

impl<R: Read + Seek + Send> Os9Filesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        let ident = looks_like_os9(&mut reader, partition_offset)
            .ok_or_else(|| FilesystemError::InvalidData("not an OS-9 RBF volume".into()))?;
        let len = reader.seek(SeekFrom::End(0))? - partition_offset;
        let sector = len / ident.total_sectors;
        let mut fs = Os9Filesystem {
            reader,
            partition_offset,
            sector,
            ident,
            bitmap: Vec::new(),
        };
        fs.reload_bitmap()?;
        Ok(fs)
    }

    pub fn into_inner(self) -> R {
        self.reader
    }

    pub fn volume_name(&self) -> &str {
        &self.ident.volume_name
    }

    fn read_sectors(&mut self, lsn: u64, count: u64) -> Result<Vec<u8>, FilesystemError> {
        let byte_len = (count * self.sector) as usize;
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + lsn * self.sector))?;
        let mut buf = vec![0u8; byte_len];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn reload_bitmap(&mut self) -> Result<(), FilesystemError> {
        // The bitmap starts at LSN 1 and spans ceil(bitmap_bytes/sector)
        // sectors.
        let span = self.ident.bitmap_bytes.div_ceil(self.sector);
        let raw = self.read_sectors(1, span)?;
        self.bitmap = raw[..self.ident.bitmap_bytes as usize].to_vec();
        Ok(())
    }

    fn read_fd(&mut self, lsn: u64) -> Result<FileDescriptor, FilesystemError> {
        if lsn == 0 || lsn >= self.ident.total_sectors {
            return Err(FilesystemError::InvalidData(format!(
                "FD LSN {lsn} out of range"
            )));
        }
        let buf = self.read_sectors(lsn, 1)?;
        Ok(FileDescriptor::parse(&buf))
    }

    /// Read a file/directory's full contents by concatenating its segments
    /// and truncating to the declared size.
    fn read_fd_data(&mut self, fd: &FileDescriptor) -> Result<Vec<u8>, FilesystemError> {
        let mut out = Vec::with_capacity(fd.size as usize);
        let mut remaining = fd.size as usize;
        for &(lsn, count) in &fd.segments {
            if remaining == 0 {
                break;
            }
            if lsn + count > self.ident.total_sectors {
                return Err(FilesystemError::InvalidData(format!(
                    "segment LSN {lsn}+{count} past end of volume"
                )));
            }
            let chunk = self.read_sectors(lsn, count)?;
            let take = remaining.min(chunk.len());
            out.extend_from_slice(&chunk[..take]);
            remaining -= take;
        }
        Ok(out)
    }

    /// Parse a directory FD's data into live `(name, fd_lsn)` pairs, skipping
    /// `.`, `..`, and deleted slots.
    fn read_dir_entries(
        &mut self,
        fd: &FileDescriptor,
    ) -> Result<Vec<(String, u64)>, FilesystemError> {
        let data = self.read_fd_data(fd)?;
        let mut out = Vec::new();
        let n = (data.len() / DIR_ENTRY_LEN).min(MAX_DIR_ENTRIES);
        for i in 0..n {
            let e = &data[i * DIR_ENTRY_LEN..(i + 1) * DIR_ENTRY_LEN];
            if e[0] == 0x00 {
                continue; // deleted / unused slot
            }
            let name = decode_os9_name(&e[0..NAME_LEN]);
            if name.is_empty() || name == "." || name == ".." {
                continue;
            }
            let lsn = u24_be(&e[NAME_LEN..NAME_LEN + 3]);
            if lsn == 0 || lsn >= self.ident.total_sectors {
                continue;
            }
            out.push((name, lsn));
        }
        Ok(out)
    }

    /// Resolve a directory `FileEntry` to its FD LSN. The root carries LSN 0
    /// (sentinel) → DD.DIR; everything else stores its FD LSN in `location`.
    fn dir_fd_lsn(&self, entry: &FileEntry) -> u64 {
        if entry.path == "/" {
            self.ident.root_dir_lsn
        } else {
            entry.location
        }
    }
}

impl<R: Read + Seek + Send> Filesystem for Os9Filesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory(
            "/".into(),
            "/".into(),
            self.ident.root_dir_lsn,
        ))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        let dir_lsn = self.dir_fd_lsn(entry);
        let dir_fd = self.read_fd(dir_lsn)?;
        if !dir_fd.is_dir() {
            return Err(FilesystemError::InvalidData(format!(
                "'{}' is not a directory",
                entry.path
            )));
        }
        let children = self.read_dir_entries(&dir_fd)?;
        let base = if entry.path == "/" {
            String::new()
        } else {
            entry.path.clone()
        };
        let mut out = Vec::new();
        for (name, lsn) in children {
            let path = format!("{base}/{name}");
            let child_fd = self.read_fd(lsn)?;
            if child_fd.is_dir() {
                out.push(FileEntry::new_directory(name, path, lsn));
            } else {
                out.push(FileEntry::new_file(name, path, child_fd.size, lsn));
            }
        }
        Ok(out)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let fd = self.read_fd(entry.location)?;
        if fd.is_dir() {
            return Err(FilesystemError::InvalidData(format!(
                "'{}' is a directory",
                entry.path
            )));
        }
        let mut data = self.read_fd_data(&fd)?;
        data.truncate(max_bytes);
        Ok(data)
    }

    fn fs_type(&self) -> &str {
        "OS-9"
    }

    fn volume_label(&self) -> Option<&str> {
        if self.ident.volume_name.is_empty() {
            None
        } else {
            Some(&self.ident.volume_name)
        }
    }

    fn total_size(&self) -> u64 {
        self.ident.total_sectors * self.sector
    }

    fn used_size(&self) -> u64 {
        let clusters = self
            .ident
            .total_sectors
            .div_ceil(self.ident.sectors_per_cluster);
        let mut used = 0u64;
        for c in 0..clusters {
            if self.bit_is_set(c) {
                used += 1;
            }
        }
        used * self.ident.sectors_per_cluster * self.sector
    }
}

impl<R: Read + Seek + Send> Os9Filesystem<R> {
    fn bit_is_set(&self, cluster: u64) -> bool {
        let byte = (cluster / 8) as usize;
        let mask = 0x80u8 >> (cluster % 8);
        self.bitmap
            .get(byte)
            .map(|&b| b & mask != 0)
            .unwrap_or(true)
    }
}

// ---------------------------------------------------------------------------
// Write side
// ---------------------------------------------------------------------------

impl<R: Read + Write + Seek + Send> Os9Filesystem<R> {
    fn write_sectors(&mut self, lsn: u64, data: &[u8]) -> Result<(), FilesystemError> {
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + lsn * self.sector))?;
        self.reader.write_all(data)?;
        Ok(())
    }

    fn cluster_count(&self) -> u64 {
        self.ident
            .total_sectors
            .div_ceil(self.ident.sectors_per_cluster)
    }

    fn set_bit(&mut self, cluster: u64, allocated: bool) {
        let byte = (cluster / 8) as usize;
        let mask = 0x80u8 >> (cluster % 8);
        if byte >= self.bitmap.len() {
            return;
        }
        if allocated {
            self.bitmap[byte] |= mask;
        } else {
            self.bitmap[byte] &= !mask;
        }
    }

    /// Persist the in-memory bitmap back to LSN 1..
    fn flush_bitmap(&mut self) -> Result<(), FilesystemError> {
        let span = self.ident.bitmap_bytes.div_ceil(self.sector);
        let mut raw = self.read_sectors(1, span)?;
        raw[..self.bitmap.len()].copy_from_slice(&self.bitmap);
        self.write_sectors(1, &raw)
    }

    /// Allocate `sectors` worth of clusters, returned as a coalesced segment
    /// list `(lsn, sector_count)`. Marks the bitmap but does not flush it.
    fn allocate_segments(&mut self, sectors: u64) -> Result<Vec<(u64, u64)>, FilesystemError> {
        if sectors == 0 {
            return Ok(Vec::new());
        }
        let spc = self.ident.sectors_per_cluster;
        let need_clusters = sectors.div_ceil(spc);
        let total_clusters = self.cluster_count();

        // Gather free clusters (skip cluster 0 — it covers LSN 0/bitmap/root).
        let mut chosen = Vec::new();
        for c in 0..total_clusters {
            if chosen.len() as u64 == need_clusters {
                break;
            }
            if !self.bit_is_set(c) {
                chosen.push(c);
            }
        }
        if (chosen.len() as u64) < need_clusters {
            return Err(FilesystemError::DiskFull(format!(
                "need {need_clusters} clusters, only {} free",
                chosen.len()
            )));
        }
        for &c in &chosen {
            self.set_bit(c, true);
        }
        // Coalesce consecutive clusters into segments (each ≤ 48 in the FD).
        let mut segments: Vec<(u64, u64)> = Vec::new();
        for &c in &chosen {
            let lsn = c * spc;
            if let Some(last) = segments.last_mut() {
                if last.0 + last.1 == lsn {
                    last.1 += spc;
                    continue;
                }
            }
            segments.push((lsn, spc));
        }
        if segments.len() > MAX_SEGMENTS {
            // Roll back the allocation before failing.
            for &c in &chosen {
                self.set_bit(c, false);
            }
            return Err(FilesystemError::DiskFull(
                "file too fragmented for an OS-9 FD (>48 segments)".into(),
            ));
        }
        Ok(segments)
    }

    fn free_segments(&mut self, segments: &[(u64, u64)]) {
        let spc = self.ident.sectors_per_cluster;
        for &(lsn, count) in segments {
            let mut s = lsn;
            while s < lsn + count {
                self.set_bit(s / spc, false);
                s += spc;
            }
        }
    }

    /// Build a 256-byte FD image with the given attributes, size and segments.
    fn build_fd(attributes: u8, size: u64, segments: &[(u64, u64)]) -> [u8; 256] {
        let mut fd = [0u8; 256];
        fd[0] = attributes;
        fd[8] = 1; // link count
        fd[9..13].copy_from_slice(&(size as u32).to_be_bytes());
        let mut off = SEG_LIST_OFFSET;
        for &(lsn, count) in segments {
            fd[off..off + 3].copy_from_slice(&put_u24_be(lsn));
            fd[off + 3..off + 5].copy_from_slice(&(count as u16).to_be_bytes());
            off += SEG_ENTRY_LEN;
        }
        fd
    }

    /// Append a directory entry `(name -> fd_lsn)` to `dir_lsn`, reusing a
    /// deleted slot if available, otherwise growing the directory file by one
    /// sector. Updates the directory FD's size. Does not flush the bitmap.
    fn dir_add_entry(
        &mut self,
        dir_lsn: u64,
        name_raw: &[u8; NAME_LEN],
        fd_lsn: u64,
    ) -> Result<(), FilesystemError> {
        let mut dir_fd = self.read_fd(dir_lsn)?;
        let mut data = self.read_fd_data(&dir_fd)?;

        let mut entry = [0u8; DIR_ENTRY_LEN];
        entry[0..NAME_LEN].copy_from_slice(name_raw);
        entry[NAME_LEN..NAME_LEN + 3].copy_from_slice(&put_u24_be(fd_lsn));

        // Reuse a deleted slot within the existing live size.
        let live_slots = data.len() / DIR_ENTRY_LEN;
        for i in 0..live_slots {
            if data[i * DIR_ENTRY_LEN] == 0x00 {
                let off = i * DIR_ENTRY_LEN;
                data[off..off + DIR_ENTRY_LEN].copy_from_slice(&entry);
                self.write_dir_slot(&dir_fd, i, &entry)?;
                return Ok(());
            }
        }

        // No free slot: does the last allocated sector have spare room past
        // the live size?
        let allocated_sectors: u64 = dir_fd.segments.iter().map(|&(_, c)| c).sum();
        let capacity = allocated_sectors * self.sector;
        let new_size = dir_fd.size + DIR_ENTRY_LEN as u64;
        if new_size > capacity {
            // Grow by one sector.
            let new_seg = self.allocate_segments(1)?;
            // Zero the freshly allocated sector so stale bytes don't look like
            // live entries.
            for &(lsn, count) in &new_seg {
                let zero = vec![0u8; (count * self.sector) as usize];
                self.write_sectors(lsn, &zero)?;
            }
            merge_segments(&mut dir_fd.segments, new_seg);
            if dir_fd.segments.len() > MAX_SEGMENTS {
                return Err(FilesystemError::DiskFull(
                    "directory too fragmented to grow".into(),
                ));
            }
        }

        // Write the new entry at offset == old size.
        let slot = (dir_fd.size / DIR_ENTRY_LEN as u64) as usize;
        dir_fd.size = new_size;
        self.write_dir_slot(&dir_fd, slot, &entry)?;
        // Persist the updated directory FD (size + possibly new segment).
        let fd_img = Self::build_fd(dir_fd.attributes, dir_fd.size, &dir_fd.segments);
        self.write_sectors(dir_lsn, &fd_img)?;
        Ok(())
    }

    /// Write a single 32-byte directory slot, resolving the slot index
    /// through the directory's segment list.
    fn write_dir_slot(
        &mut self,
        dir_fd: &FileDescriptor,
        slot: usize,
        entry: &[u8; DIR_ENTRY_LEN],
    ) -> Result<(), FilesystemError> {
        let byte_off = (slot * DIR_ENTRY_LEN) as u64;
        // Map a logical byte offset through the segment list to an absolute LSN.
        let mut walked = 0u64;
        for &(lsn, count) in &dir_fd.segments {
            let seg_bytes = count * self.sector;
            if byte_off < walked + seg_bytes {
                let within = byte_off - walked;
                let abs = self.partition_offset + lsn * self.sector + within;
                self.reader.seek(SeekFrom::Start(abs))?;
                self.reader.write_all(entry)?;
                return Ok(());
            }
            walked += seg_bytes;
        }
        Err(FilesystemError::InvalidData(
            "directory slot past allocated space".into(),
        ))
    }

    /// Mark a directory entry deleted (first name byte = 0) by name.
    fn dir_remove_entry(&mut self, dir_lsn: u64, name: &str) -> Result<u64, FilesystemError> {
        let dir_fd = self.read_fd(dir_lsn)?;
        let data = self.read_fd_data(&dir_fd)?;
        let slots = data.len() / DIR_ENTRY_LEN;
        for i in 0..slots {
            let e = &data[i * DIR_ENTRY_LEN..(i + 1) * DIR_ENTRY_LEN];
            if e[0] == 0x00 {
                continue;
            }
            if decode_os9_name(&e[0..NAME_LEN]) == name {
                let fd_lsn = u24_be(&e[NAME_LEN..NAME_LEN + 3]);
                let mut cleared = [0u8; DIR_ENTRY_LEN];
                cleared.copy_from_slice(e);
                cleared[0] = 0x00;
                self.write_dir_slot(&dir_fd, i, &cleared)?;
                return Ok(fd_lsn);
            }
        }
        Err(FilesystemError::NotFound(format!(
            "'{name}' not found in directory"
        )))
    }

    fn entry_exists(&mut self, dir_lsn: u64, name: &str) -> Result<bool, FilesystemError> {
        let dir_fd = self.read_fd(dir_lsn)?;
        Ok(self
            .read_dir_entries(&dir_fd)?
            .iter()
            .any(|(n, _)| n == name))
    }
}

/// Merge `new` segments into `segs`, coalescing a run that abuts the tail.
fn merge_segments(segs: &mut Vec<(u64, u64)>, new: Vec<(u64, u64)>) {
    for (lsn, count) in new {
        if let Some(last) = segs.last_mut() {
            if last.0 + last.1 == lsn {
                last.1 += count;
                continue;
            }
        }
        segs.push((lsn, count));
    }
}

impl<R: Read + Write + Seek + Send> EditableFilesystem for Os9Filesystem<R> {
    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn std::io::Read,
        data_len: u64,
        _options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let dir_lsn = self.dir_fd_lsn(parent);
        let name_raw = encode_os9_name(name)?;
        if self.entry_exists(dir_lsn, name)? {
            return Err(FilesystemError::AlreadyExists(format!(
                "OS-9 file '{name}' already exists"
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

        // Allocate one sector for the FD, then the data sectors.
        let data_sectors = (payload.len() as u64).div_ceil(self.sector);
        let fd_seg = self.allocate_segments(1)?;
        let fd_lsn = fd_seg[0].0;
        let data_segs = match self.allocate_segments(data_sectors) {
            Ok(s) => s,
            Err(e) => {
                self.free_segments(&fd_seg);
                return Err(e);
            }
        };

        // Write the data, zero-padded to the sector.
        for &(lsn, count) in &data_segs {
            let mut buf = vec![0u8; (count * self.sector) as usize];
            // figure out which slice of payload this segment covers
            let seg_first_sector = data_segs
                .iter()
                .take_while(|s| s.0 != lsn)
                .map(|s| s.1)
                .sum::<u64>();
            let start = (seg_first_sector * self.sector) as usize;
            let end = (start + buf.len()).min(payload.len());
            if start < payload.len() {
                buf[..end - start].copy_from_slice(&payload[start..end]);
            }
            self.write_sectors(lsn, &buf)?;
        }

        // Write the FD (regular file: owner read+write).
        let fd_img = Self::build_fd(0x03, payload.len() as u64, &data_segs);
        self.write_sectors(fd_lsn, &fd_img)?;

        // Link it into the parent directory.
        self.dir_add_entry(dir_lsn, &name_raw, fd_lsn)?;
        self.flush_bitmap()?;
        self.reader.flush()?;
        self.reload_bitmap()?;

        let path = if parent.path == "/" {
            format!("/{name}")
        } else {
            format!("{}/{name}", parent.path)
        };
        Ok(FileEntry::new_file(
            name.to_string(),
            path,
            payload.len() as u64,
            fd_lsn,
        ))
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let parent_lsn = self.dir_fd_lsn(parent);
        let name_raw = encode_os9_name(name)?;
        if self.entry_exists(parent_lsn, name)? {
            return Err(FilesystemError::AlreadyExists(format!(
                "OS-9 entry '{name}' already exists"
            )));
        }

        // FD sector + one data sector for the "." / ".." entries.
        let fd_seg = self.allocate_segments(1)?;
        let fd_lsn = fd_seg[0].0;
        let data_seg = match self.allocate_segments(1) {
            Ok(s) => s,
            Err(e) => {
                self.free_segments(&fd_seg);
                return Err(e);
            }
        };
        let data_lsn = data_seg[0].0;

        // Initialise the directory data: ".." -> parent, "." -> self.
        let mut dir_data = vec![0u8; self.sector as usize];
        let dotdot = encode_os9_name("..").unwrap();
        let dot = encode_os9_name(".").unwrap();
        dir_data[0..NAME_LEN].copy_from_slice(&dotdot);
        dir_data[NAME_LEN..NAME_LEN + 3].copy_from_slice(&put_u24_be(parent_lsn));
        dir_data[DIR_ENTRY_LEN..DIR_ENTRY_LEN + NAME_LEN].copy_from_slice(&dot);
        dir_data[DIR_ENTRY_LEN + NAME_LEN..DIR_ENTRY_LEN + NAME_LEN + 3]
            .copy_from_slice(&put_u24_be(fd_lsn));
        self.write_sectors(data_lsn, &dir_data)?;

        // Directory FD: directory attribute + full perms, size = 2 entries.
        let fd_img = Self::build_fd(ATT_DIR | 0x3F, (2 * DIR_ENTRY_LEN) as u64, &data_seg);
        self.write_sectors(fd_lsn, &fd_img)?;

        self.dir_add_entry(parent_lsn, &name_raw, fd_lsn)?;
        self.flush_bitmap()?;
        self.reader.flush()?;
        self.reload_bitmap()?;

        let path = if parent.path == "/" {
            format!("/{name}")
        } else {
            format!("{}/{name}", parent.path)
        };
        Ok(FileEntry::new_directory(name.to_string(), path, fd_lsn))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        let parent_lsn = self.dir_fd_lsn(parent);
        let fd = self.read_fd(entry.location)?;

        // Refuse to delete a non-empty directory (matches OS-9 `deldir`
        // semantics — caller must empty it first).
        if fd.is_dir() {
            let live = self.read_dir_entries(&fd)?;
            if !live.is_empty() {
                return Err(FilesystemError::Unsupported(format!(
                    "directory '{}' is not empty",
                    entry.path
                )));
            }
        }

        // Free the data segments and the FD sector itself.
        self.free_segments(&fd.segments);
        let fd_seg = [(entry.location, 1u64)];
        self.free_segments(&fd_seg);

        // Unlink from the parent directory.
        self.dir_remove_entry(parent_lsn, &entry.name)?;

        self.flush_bitmap()?;
        self.reader.flush()?;
        self.reload_bitmap()?;
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
        let new_raw = encode_os9_name(new_name)?;
        let dir_lsn = self.dir_fd_lsn(parent);

        // Reject a collision with a *different* existing entry. OS-9 names are
        // case-sensitive, so a plain equality check is the right fold here.
        if self.entry_exists(dir_lsn, new_name)? {
            return Err(FilesystemError::AlreadyExists(format!(
                "OS-9 entry '{new_name}' already exists"
            )));
        }

        // Find the directory slot whose name matches the old name, then rewrite
        // only the 29-byte name field, preserving the 24-bit FD LSN (bytes
        // 29..32) so the entry keeps its identity and contents.
        let dir_fd = self.read_fd(dir_lsn)?;
        let data = self.read_fd_data(&dir_fd)?;
        let slots = data.len() / DIR_ENTRY_LEN;
        for i in 0..slots {
            let e = &data[i * DIR_ENTRY_LEN..(i + 1) * DIR_ENTRY_LEN];
            if e[0] == 0x00 {
                continue;
            }
            if decode_os9_name(&e[0..NAME_LEN]) == entry.name {
                let mut slot = [0u8; DIR_ENTRY_LEN];
                slot.copy_from_slice(e);
                slot[0..NAME_LEN].copy_from_slice(&new_raw);
                // bytes NAME_LEN..DIR_ENTRY_LEN (the FD LSN) left untouched.
                self.write_dir_slot(&dir_fd, i, &slot)?;
                self.reader.flush()?;
                return Ok(());
            }
        }
        Err(FilesystemError::NotFound(format!(
            "'{}' not found in directory",
            entry.name
        )))
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.reader.flush()?;
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        let clusters = self.cluster_count();
        let mut free = 0u64;
        for c in 0..clusters {
            if !self.bit_is_set(c) {
                free += 1;
            }
        }
        Ok(free * self.ident.sectors_per_cluster * self.sector)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_round_trips() {
        let raw = encode_os9_name("OS9Boot").unwrap();
        assert_eq!(raw[6], b't' | 0x80);
        assert_eq!(decode_os9_name(&raw), "OS9Boot");
        assert_eq!(decode_os9_name(&encode_os9_name("..").unwrap()), "..");
        assert_eq!(decode_os9_name(&encode_os9_name(".").unwrap()), ".");
        assert!(encode_os9_name("9bad").is_err());
        assert!(encode_os9_name("has space").is_err());
    }

    #[test]
    fn u24_round_trips() {
        for v in [0u64, 1, 255, 256, 567, 0x123456, 0xFFFFFF] {
            assert_eq!(u24_be(&put_u24_be(v)), v);
        }
    }

    /// Decompress the shared OS-9 fixture (a real 35-track NitrOS-9 L2 disk).
    fn fixture_bytes() -> Vec<u8> {
        use std::io::Read as _;
        let compressed =
            std::fs::read("tests/fixtures/test_coco_os9l2.dsk.zst").expect("read fixture");
        let mut dec = zstd::stream::read::Decoder::new(std::io::Cursor::new(compressed))
            .expect("zstd decoder");
        let mut bytes = Vec::new();
        dec.read_to_end(&mut bytes).expect("decompress");
        bytes
    }

    #[test]
    fn rename_in_place_round_trip() {
        use std::io::Cursor;
        let mut fs = Os9Filesystem::open(Cursor::new(fixture_bytes()), 0).unwrap();
        let root = fs.root().unwrap();

        // Create a fresh file at the root so the test owns it.
        let payload: Vec<u8> = (0..900u32).map(|i| (i % 256) as u8).collect();
        let created = fs
            .create_file(
                &root,
                "BEFORE.B09",
                &mut Cursor::new(payload.clone()),
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
        let fd_lsn = created.location;

        fs.rename(&root, &created, "AFTER.B09").unwrap();
        fs.sync_metadata().unwrap();

        // Re-open and confirm the new name lists, the old is gone, and the FD
        // LSN (identity) plus contents are preserved.
        let bytes = fs.into_inner().into_inner();
        let mut fs2 = Os9Filesystem::open(Cursor::new(bytes), 0).unwrap();
        let root2 = fs2.root().unwrap();
        let listing = fs2.list_directory(&root2).unwrap();
        let names: Vec<&str> = listing.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"AFTER.B09"));
        assert!(!names.contains(&"BEFORE.B09"));

        let renamed = listing.iter().find(|e| e.name == "AFTER.B09").unwrap();
        assert_eq!(renamed.location, fd_lsn, "FD LSN (identity) changed");
        assert_eq!(fs2.read_file(renamed, usize::MAX).unwrap(), payload);
    }

    #[test]
    fn rename_collision_rejected() {
        use std::io::Cursor;
        let mut fs = Os9Filesystem::open(Cursor::new(fixture_bytes()), 0).unwrap();
        let root = fs.root().unwrap();
        let one = fs
            .create_file(
                &root,
                "ALPHA.X",
                &mut Cursor::new(vec![1u8; 20]),
                20,
                &CreateFileOptions::default(),
            )
            .unwrap();
        fs.create_file(
            &root,
            "BETA.X",
            &mut Cursor::new(vec![2u8; 20]),
            20,
            &CreateFileOptions::default(),
        )
        .unwrap();
        let err = fs.rename(&root, &one, "BETA.X").unwrap_err();
        assert!(matches!(err, FilesystemError::AlreadyExists(_)));
    }
}
