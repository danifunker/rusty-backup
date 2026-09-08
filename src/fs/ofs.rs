//! OFS — the "old" Be File System, used by BeOS before BFS arrived in DR9.
//!
//! This is the filesystem on the 1993-94 Hobbit BeBox prototypes and the early
//! PowerPC Developer Releases. It is a flat, sector-oriented design with no
//! inodes: a file's metadata lives entirely in its parent's directory entry,
//! and the only indirection is an optional extent list. Everything is
//! big-endian and everything is counted in 512-byte sectors.
//!
//! **Table of contents**, sector 0:
//! `version` @0 (`major << 16 | minor`, 1..3), `create_date` @4,
//! `bitmap_start` @8, `bitmap_sectors` @12, `first_dir_sector` @16,
//! `total_sectors` @20, `sector_size` @24, @28 unused, @32 a free-space hint,
//! `used_sectors` @36, then `volume_name[32]` @44.
//!
//! **Allocation bitmap**, `bitmap_sectors` sectors from `bitmap_start`:
//! one bit per sector, **set = allocated**, LSB first within each byte.
//!
//! **Directories** are chains of fixed blocks: 4096 bytes (8 sectors) on
//! version 1, 8128 in 16 sectors on 2 and 3. Each block is a `next_block`
//! sector number, 60 bytes of padding, then 63 entries. An entry is a name
//! (32 bytes on v1, 64 on v2/v3), 32 bytes of attributes, and — on v2/v3 —
//! 32 further bytes we preserve but do not interpret. A zero first name byte
//! marks a free slot. There are no `.` or `..` entries.
//!
//! **Attributes**: `first_alloc_list` @0, `last_alloc_list` @4, `file_type` @8
//! (`0xFFFFFFFF` means directory, otherwise a Mac-style `OSType`),
//! `create_date` @12, `modify_date` @16 (both Unix seconds),
//! `logical_size` @20, `physical_size` @24, `creator` @28.
//!
//! **File data** comes in two shapes, chosen by the top bit of
//! `first_alloc_list`:
//! - set — the file is one contiguous run starting at
//!   `first_alloc_list & 0x7FFFFFFF`, and `last_alloc_list` holds its length
//!   in sectors,
//! - clear — `first_alloc_list` is an *extent-list* sector holding 64
//!   `{start, count}` pairs. Pair 0 is the chain header (`start` names the
//!   next list sector, or 0); pairs 1.. are data extents, terminated by a
//!   `start` of `0xFFFFFFFF`.
//!
//! Re-derived from the Hobbit BeBox image plus Steve White's 2004
//! `ofs-extractor`, which is the only published description of the format.

use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};

/// Sector size. The TOC records it, but every known volume says 512 and the
/// directory-block geometry below assumes it.
pub const SECTOR: u64 = 512;

/// Entries per directory block, on every version.
pub const ENTRIES_PER_BLOCK: usize = 63;
/// Bytes of block header before the first entry.
const BLOCK_HEADER: usize = 64;
/// A directory entry's attribute record.
const ATTR_SIZE: usize = 32;
/// `file_type` for a directory.
const TYPE_DIRECTORY: u32 = 0xFFFF_FFFF;
/// Set in `first_alloc_list` when the file is one contiguous run.
const CONTIGUOUS_FLAG: u32 = 0x8000_0000;
/// Terminator in an extent list.
const EXTENT_END: u32 = 0xFFFF_FFFF;
/// `{start, count}` pairs in one extent-list sector.
const EXTENTS_PER_SECTOR: usize = 64;

const TOC_OFF_VERSION: usize = 0;
const TOC_OFF_CREATE_DATE: usize = 4;
const TOC_OFF_BITMAP_START: usize = 8;
const TOC_OFF_BITMAP_SECTORS: usize = 12;
const TOC_OFF_FIRST_DIR: usize = 16;
const TOC_OFF_TOTAL_SECTORS: usize = 20;
const TOC_OFF_SECTOR_SIZE: usize = 24;
const TOC_OFF_USED_SECTORS: usize = 36;
const TOC_OFF_VOLUME_NAME: usize = 44;
const VOLUME_NAME_LEN: usize = 32;

/// Guard against a corrupt `next_block` cycle in a directory chain.
const MAX_DIR_BLOCKS: usize = 4096;

fn be32(b: &[u8], off: usize) -> u32 {
    u32::from_be_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
}

pub(crate) fn put_be32(b: &mut [u8], off: usize, v: u32) {
    b[off..off + 4].copy_from_slice(&v.to_be_bytes());
}

/// The volume's table of contents (sector 0).
#[derive(Debug, Clone)]
pub struct OfsToc {
    pub major: u16,
    pub minor: u16,
    pub create_date: u32,
    pub bitmap_start: u32,
    pub bitmap_sectors: u32,
    pub first_dir_sector: u32,
    pub total_sectors: u32,
    pub sector_size: u32,
    pub used_sectors: u32,
    pub volume_name: String,
}

impl OfsToc {
    pub fn parse(buf: &[u8]) -> Result<Self, FilesystemError> {
        if buf.len() < SECTOR as usize {
            return Err(FilesystemError::Parse("ofs: TOC buffer too short".into()));
        }
        let version = be32(buf, TOC_OFF_VERSION);
        let major = (version >> 16) as u16;
        let minor = (version & 0xFFF) as u16;
        if !(1..=3).contains(&major) {
            return Err(FilesystemError::Parse(format!(
                "ofs: version major {major} is not 1, 2, or 3"
            )));
        }
        let sector_size = be32(buf, TOC_OFF_SECTOR_SIZE);
        if sector_size != SECTOR as u32 {
            return Err(FilesystemError::Parse(format!(
                "ofs: sector size {sector_size} is not 512"
            )));
        }
        let bitmap_start = be32(buf, TOC_OFF_BITMAP_START);
        let bitmap_sectors = be32(buf, TOC_OFF_BITMAP_SECTORS);
        let first_dir_sector = be32(buf, TOC_OFF_FIRST_DIR);
        let total_sectors = be32(buf, TOC_OFF_TOTAL_SECTORS);
        if bitmap_start == 0 || bitmap_sectors == 0 || first_dir_sector == 0 || total_sectors == 0 {
            return Err(FilesystemError::Parse(
                "ofs: TOC geometry has a zero where a sector number belongs".into(),
            ));
        }
        // The bitmap has to be big enough for the volume it describes, and the
        // root directory has to sit past it. Both fail loudly on a false hit.
        let need = (total_sectors as u64).div_ceil(8).div_ceil(SECTOR);
        if (bitmap_sectors as u64) < need
            || (first_dir_sector as u64) < bitmap_start as u64 + bitmap_sectors as u64
        {
            return Err(FilesystemError::Parse(format!(
                "ofs: bitmap of {bitmap_sectors} sectors cannot cover {total_sectors} sectors, \
                 or the root directory at {first_dir_sector} overlaps it"
            )));
        }

        let raw = &buf[TOC_OFF_VOLUME_NAME..TOC_OFF_VOLUME_NAME + VOLUME_NAME_LEN];
        let end = raw.iter().position(|&c| c == 0).unwrap_or(raw.len());

        Ok(OfsToc {
            major,
            minor,
            create_date: be32(buf, TOC_OFF_CREATE_DATE),
            bitmap_start,
            bitmap_sectors,
            first_dir_sector,
            total_sectors,
            sector_size,
            used_sectors: be32(buf, TOC_OFF_USED_SECTORS),
            volume_name: String::from_utf8_lossy(&raw[..end]).trim().to_string(),
        })
    }

    /// Name field width — 32 bytes on version 1, 64 from version 2.
    pub fn name_len(&self) -> usize {
        if self.major == 1 {
            32
        } else {
            64
        }
    }

    /// One directory entry's stride. Version 2 and 3 append 32 bytes we keep
    /// but do not decode.
    pub fn entry_size(&self) -> usize {
        if self.major == 1 {
            self.name_len() + ATTR_SIZE
        } else {
            self.name_len() + ATTR_SIZE + 32
        }
    }

    /// Bytes a directory block's records occupy: 4096 on v1, 8128 on v2/v3
    /// (which then rounds up to a whole 16 sectors on disk).
    pub fn block_bytes(&self) -> usize {
        BLOCK_HEADER + ENTRIES_PER_BLOCK * self.entry_size()
    }

    /// Sectors one directory block occupies.
    pub fn block_sectors(&self) -> u64 {
        (self.block_bytes() as u64).div_ceil(SECTOR)
    }
}

/// A directory entry's 32-byte attribute record.
#[derive(Debug, Clone, Copy, Default)]
pub struct OfsAttrs {
    pub first_alloc_list: u32,
    pub last_alloc_list: u32,
    pub file_type: u32,
    pub create_date: u32,
    pub modify_date: u32,
    pub logical_size: u32,
    pub physical_size: u32,
    pub creator: u32,
}

impl OfsAttrs {
    fn parse(b: &[u8], off: usize) -> Self {
        OfsAttrs {
            first_alloc_list: be32(b, off),
            last_alloc_list: be32(b, off + 4),
            file_type: be32(b, off + 8),
            create_date: be32(b, off + 12),
            modify_date: be32(b, off + 16),
            logical_size: be32(b, off + 20),
            physical_size: be32(b, off + 24),
            creator: be32(b, off + 28),
        }
    }
    pub(crate) fn write(&self, b: &mut [u8], off: usize) {
        put_be32(b, off, self.first_alloc_list);
        put_be32(b, off + 4, self.last_alloc_list);
        put_be32(b, off + 8, self.file_type);
        put_be32(b, off + 12, self.create_date);
        put_be32(b, off + 16, self.modify_date);
        put_be32(b, off + 20, self.logical_size);
        put_be32(b, off + 24, self.physical_size);
        put_be32(b, off + 28, self.creator);
    }
    pub fn is_directory(&self) -> bool {
        self.file_type == TYPE_DIRECTORY
    }
    pub fn is_contiguous(&self) -> bool {
        self.first_alloc_list & CONTIGUOUS_FLAG != 0
    }
    /// Start sector of a contiguous file's single run.
    pub fn contiguous_start(&self) -> u64 {
        (self.first_alloc_list & !CONTIGUOUS_FLAG) as u64
    }
}

/// A located directory entry: where it lives, and what it says.
#[derive(Debug, Clone)]
pub struct OfsEntry {
    /// Sector of the directory block holding this entry.
    pub block: u64,
    /// Slot within that block, 0..63.
    pub index: usize,
    pub name: String,
    pub attrs: OfsAttrs,
}

impl OfsEntry {
    /// Pack `(block, index)` into the `FileEntry::location` a caller hands
    /// back — OFS has no inode number to use instead.
    pub fn location(&self) -> u64 {
        (self.block << 16) | self.index as u64
    }
}

/// Split a packed location back into `(block, index)`.
pub(crate) fn unpack_location(loc: u64) -> (u64, usize) {
    (loc >> 16, (loc & 0xFFFF) as usize)
}

/// A mounted OFS volume.
pub struct OfsFilesystem<R> {
    pub(crate) reader: R,
    pub(crate) partition_offset: u64,
    pub(crate) toc: OfsToc,
    pub(crate) toc_dirty: bool,
}

impl<R: Read + Seek + Send> OfsFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        reader.seek(SeekFrom::Start(partition_offset))?;
        let mut buf = [0u8; SECTOR as usize];
        reader.read_exact(&mut buf)?;
        let toc = OfsToc::parse(&buf)?;
        Ok(OfsFilesystem {
            reader,
            partition_offset,
            toc,
            toc_dirty: false,
        })
    }

    /// Does a plausible OFS table of contents sit at sector 0?
    pub fn detect(reader: &mut (impl Read + Seek), partition_offset: u64) -> bool {
        if reader.seek(SeekFrom::Start(partition_offset)).is_err() {
            return false;
        }
        let mut buf = [0u8; SECTOR as usize];
        reader.read_exact(&mut buf).is_ok() && OfsToc::parse(&buf).is_ok()
    }

    pub fn toc(&self) -> &OfsToc {
        &self.toc
    }

    pub(crate) fn read_sectors(
        &mut self,
        sector: u64,
        count: u64,
    ) -> Result<Vec<u8>, FilesystemError> {
        if sector.saturating_add(count) > self.toc.total_sectors as u64 + 1 {
            return Err(FilesystemError::Parse(format!(
                "ofs: sector range {sector}..{} is past the end of the volume",
                sector + count
            )));
        }
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + sector * SECTOR))?;
        let mut buf = vec![0u8; (count * SECTOR) as usize];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Every `(start, count)` extent a file occupies, in file order.
    pub fn file_extents(&mut self, attrs: &OfsAttrs) -> Result<Vec<(u64, u64)>, FilesystemError> {
        // An empty file records `0xFFFFFFFF` in both alloc-list fields rather
        // than a zero, so the contiguous flag reads as set on a file that owns
        // nothing at all.
        if attrs.first_alloc_list == EXTENT_END || attrs.first_alloc_list == 0 {
            return Ok(Vec::new());
        }
        let total = self.toc.total_sectors as u64;
        if attrs.is_contiguous() {
            let start = attrs.contiguous_start();
            let need = (attrs.logical_size as u64).div_ceil(SECTOR);
            // `last_alloc_list` is the run's length in sectors here; fall back
            // to the logical size when it does not fit the volume.
            let claimed = attrs.last_alloc_list as u64;
            let sectors = if claimed > 0 && start + claimed <= total {
                claimed.max(need)
            } else {
                need
            };
            if sectors == 0 {
                return Ok(Vec::new());
            }
            return Ok(vec![(start, sectors)]);
        }
        let mut out = Vec::new();
        let mut list = attrs.first_alloc_list as u64;
        let mut guard = 0;
        while list != 0 && guard < 256 {
            guard += 1;
            let raw = self.read_sectors(list, 1)?;
            // Pair 0 is the chain header; its `start` names the next list
            // sector, and the data extents run from pair 1 to the terminator.
            let next = be32(&raw, 0) as u64;
            for i in 1..EXTENTS_PER_SECTOR {
                let start = be32(&raw, i * 8);
                let count = be32(&raw, i * 8 + 4);
                if start == EXTENT_END {
                    return Ok(out);
                }
                if count == 0 {
                    continue;
                }
                out.push((start as u64, count as u64));
            }
            list = next;
        }
        Ok(out)
    }

    /// Stream a file's bytes into `sink`, clamped to its logical size.
    pub fn stream_file(
        &mut self,
        attrs: &OfsAttrs,
        limit: u64,
        sink: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let want = (attrs.logical_size as u64).min(limit);
        if want == 0 {
            return Ok(0);
        }
        let extents = self.file_extents(attrs)?;
        let mut written = 0u64;
        for (start, count) in extents {
            if written >= want {
                break;
            }
            let take = (count * SECTOR).min(want - written);
            let sectors = take.div_ceil(SECTOR);
            let raw = self.read_sectors(start, sectors)?;
            sink.write_all(&raw[..take as usize])?;
            written += take;
        }
        Ok(written)
    }

    /// The sector a directory's block chain starts at.
    pub(crate) fn dir_start(&mut self, entry: &FileEntry) -> Result<u64, FilesystemError> {
        if entry.location == 0 {
            return Ok(self.toc.first_dir_sector as u64);
        }
        let located = self.entry_at(entry.location)?;
        if !located.attrs.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        Ok(located.attrs.first_alloc_list as u64)
    }

    /// Re-read the directory entry a packed location points at.
    pub(crate) fn entry_at(&mut self, location: u64) -> Result<OfsEntry, FilesystemError> {
        let (block, index) = unpack_location(location);
        if index >= ENTRIES_PER_BLOCK {
            return Err(FilesystemError::Parse(format!(
                "ofs: entry index {index} is past the {ENTRIES_PER_BLOCK} slots in a block"
            )));
        }
        let raw = self.read_sectors(block, self.toc.block_sectors())?;
        self.decode_entry(&raw, block, index)
            .ok_or_else(|| FilesystemError::NotFound(format!("ofs: empty slot {index} at {block}")))
    }

    /// Decode one slot of an already-read directory block. `None` for a free
    /// slot (a zero first name byte).
    pub(crate) fn entry_in_block(&self, raw: &[u8], block: u64, index: usize) -> Option<OfsEntry> {
        self.decode_entry(raw, block, index)
    }

    fn decode_entry(&self, raw: &[u8], block: u64, index: usize) -> Option<OfsEntry> {
        let name_len = self.toc.name_len();
        let off = BLOCK_HEADER + index * self.toc.entry_size();
        if off + name_len + ATTR_SIZE > raw.len() || raw[off] == 0 {
            return None;
        }
        let name_bytes = &raw[off..off + name_len];
        let end = name_bytes.iter().position(|&c| c == 0).unwrap_or(name_len);
        Some(OfsEntry {
            block,
            index,
            name: String::from_utf8_lossy(&name_bytes[..end]).into_owned(),
            attrs: OfsAttrs::parse(raw, off + name_len),
        })
    }

    /// Every live entry in a directory, following the block chain.
    pub fn read_directory(&mut self, start: u64) -> Result<Vec<OfsEntry>, FilesystemError> {
        let mut out = Vec::new();
        let mut block = start;
        let mut visited = 0usize;
        while block != 0 {
            visited += 1;
            if visited > MAX_DIR_BLOCKS {
                return Err(FilesystemError::Parse(
                    "ofs: directory block chain is longer than 4096 blocks (cycle?)".into(),
                ));
            }
            let raw = self.read_sectors(block, self.toc.block_sectors())?;
            for index in 0..ENTRIES_PER_BLOCK {
                if let Some(e) = self.decode_entry(&raw, block, index) {
                    out.push(e);
                }
            }
            block = be32(&raw, 0) as u64;
        }
        Ok(out)
    }

    fn build_entry(&self, e: &OfsEntry, parent: &FileEntry) -> FileEntry {
        let parent_path = if parent.path == "/" {
            String::new()
        } else {
            parent.path.clone()
        };
        let path = format!("{parent_path}/{}", e.name);
        let mut out = if e.attrs.is_directory() {
            FileEntry::new_directory(e.name.clone(), path, e.location())
        } else {
            FileEntry::new_file(
                e.name.clone(),
                path,
                e.attrs.logical_size as u64,
                e.location(),
            )
        };
        if e.attrs.modify_date != 0 {
            out.modified_unix = Some(e.attrs.modify_date as u64);
            out.modified = Some(super::unix_common::inode::format_unix_timestamp(
                e.attrs.modify_date as i64,
            ));
        }
        // Early BeOS typed files exactly the way the Mac Finder did, so the
        // type/creator pair belongs in the same fields HFS uses.
        if !e.attrs.is_directory() && e.attrs.file_type != 0 {
            out.type_code = Some(e.attrs.file_type.to_be_bytes());
        }
        if e.attrs.creator != 0 {
            out.creator_code = Some(e.attrs.creator.to_be_bytes());
        }
        out
    }
}

impl<R: Read + Seek + Send> Filesystem for OfsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::root())
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        let start = self.dir_start(entry)?;
        let entries = self.read_directory(start)?;
        Ok(entries.iter().map(|e| self.build_entry(e, entry)).collect())
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let located = self.entry_at(entry.location)?;
        let mut out = Vec::new();
        self.stream_file(&located.attrs, max_bytes as u64, &mut out)?;
        Ok(out)
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let located = self.entry_at(entry.location)?;
        self.stream_file(&located.attrs, u64::MAX, writer)
    }

    fn volume_label(&self) -> Option<&str> {
        if self.toc.volume_name.is_empty() {
            None
        } else {
            Some(&self.toc.volume_name)
        }
    }

    fn fs_type(&self) -> &str {
        "BeOS OFS"
    }

    fn fsck(&mut self) -> Option<Result<super::fsck::FsckResult, FilesystemError>> {
        Some(self.fsck_ofs())
    }

    fn total_size(&self) -> u64 {
        self.toc.total_sectors as u64 * SECTOR
    }

    fn used_size(&self) -> u64 {
        self.toc.used_sectors as u64 * SECTOR
    }

    fn allocation_unit(&self) -> Option<u64> {
        Some(SECTOR)
    }

    fn validate_name(&self, name: &str) -> Result<(), FilesystemError> {
        validate_ofs_name(name, self.toc.name_len())
    }
}

/// Names are NUL-terminated bytes in a fixed field, so one byte has to be left
/// for the terminator.
pub(crate) fn validate_ofs_name(name: &str, field: usize) -> Result<(), FilesystemError> {
    if name.is_empty() {
        return Err(FilesystemError::InvalidData("name cannot be empty".into()));
    }
    if name.len() >= field {
        return Err(FilesystemError::InvalidData(format!(
            "ofs: name is {} bytes; this volume's name field holds {} plus a terminator",
            name.len(),
            field - 1
        )));
    }
    if name.contains('/') || name.contains('\0') {
        return Err(FilesystemError::InvalidData(
            "ofs: '/' and NUL cannot appear in a name".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The Hobbit BeBox volume's actual TOC values.
    fn hobbit_toc() -> Vec<u8> {
        let mut b = vec![0u8; 512];
        put_be32(&mut b, TOC_OFF_VERSION, 0x0001_0000);
        put_be32(&mut b, TOC_OFF_CREATE_DATE, 0x2edb_1ed6);
        put_be32(&mut b, TOC_OFF_BITMAP_START, 1);
        put_be32(&mut b, TOC_OFF_BITMAP_SECTORS, 82);
        put_be32(&mut b, TOC_OFF_FIRST_DIR, 83);
        put_be32(&mut b, TOC_OFF_TOTAL_SECTORS, 332_487);
        put_be32(&mut b, TOC_OFF_SECTOR_SIZE, 512);
        put_be32(&mut b, TOC_OFF_USED_SECTORS, 53_796);
        b[TOC_OFF_VOLUME_NAME..TOC_OFF_VOLUME_NAME + 2].copy_from_slice(b"be");
        b
    }

    #[test]
    fn parses_the_hobbit_toc() {
        let toc = OfsToc::parse(&hobbit_toc()).unwrap();
        assert_eq!((toc.major, toc.minor), (1, 0));
        assert_eq!(toc.first_dir_sector, 83);
        assert_eq!(toc.volume_name, "be");
        // Version 1: 32-byte names, 64-byte entries, 4 KiB (8-sector) blocks.
        assert_eq!(toc.name_len(), 32);
        assert_eq!(toc.entry_size(), 64);
        assert_eq!(toc.block_bytes(), 4096);
        assert_eq!(toc.block_sectors(), 8);
    }

    #[test]
    fn version_two_widens_names_and_blocks() {
        let mut b = hobbit_toc();
        put_be32(&mut b, TOC_OFF_VERSION, 0x0002_0000);
        let toc = OfsToc::parse(&b).unwrap();
        assert_eq!(toc.name_len(), 64);
        assert_eq!(toc.entry_size(), 128);
        // 64 + 63*128 = 8128, which rounds up to 16 sectors on disk.
        assert_eq!(toc.block_bytes(), 8128);
        assert_eq!(toc.block_sectors(), 16);
    }

    #[test]
    fn a_bitmap_too_small_for_the_volume_is_rejected() {
        let mut b = hobbit_toc();
        put_be32(&mut b, TOC_OFF_BITMAP_SECTORS, 4);
        assert!(OfsToc::parse(&b).is_err());
    }

    #[test]
    fn an_unknown_version_is_rejected() {
        let mut b = hobbit_toc();
        put_be32(&mut b, TOC_OFF_VERSION, 0x0009_0000);
        assert!(OfsToc::parse(&b).is_err());
    }

    #[test]
    fn locations_round_trip_through_the_pack() {
        let e = OfsEntry {
            block: 12_345,
            index: 62,
            name: "x".into(),
            attrs: OfsAttrs::default(),
        };
        assert_eq!(unpack_location(e.location()), (12_345, 62));
    }

    #[test]
    fn contiguous_flag_splits_start_from_length() {
        let attrs = OfsAttrs {
            first_alloc_list: 0x8000_b081,
            last_alloc_list: 482,
            logical_size: 246_744,
            ..Default::default()
        };
        assert!(attrs.is_contiguous());
        assert_eq!(attrs.contiguous_start(), 45_185);
    }

    #[test]
    fn names_must_fit_the_field_with_a_terminator() {
        assert!(validate_ofs_name(&"a".repeat(31), 32).is_ok());
        assert!(validate_ofs_name(&"a".repeat(32), 32).is_err());
        assert!(validate_ofs_name("a/b", 32).is_err());
        assert!(validate_ofs_name("", 32).is_err());
    }
}
