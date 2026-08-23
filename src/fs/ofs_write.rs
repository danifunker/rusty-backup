//! BeOS OFS writing — sector allocator, directory-slot management, and the
//! [`EditableFilesystem`] half of [`crate::fs::ofs`].
//!
//! OFS has no inodes, so creating a file means three things: mark sectors in
//! the bitmap (**set = allocated**), write the data, and fill a free 64- or
//! 128-byte slot in the parent directory's block chain. Deleting reverses it.
//! There is no journal and no second copy of anything, so each operation
//! writes data first and the directory entry last — a crash in between leaks
//! sectors, which a bitmap rebuild can reclaim, rather than leaving a
//! directory entry pointing at nothing.
//!
//! A file is stored contiguously when one free run is long enough (the top bit
//! of `first_alloc_list` and a sector count in `last_alloc_list`); otherwise it
//! gets an extent-list sector. See `crate::fs::ofs` for both layouts.

use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::FileEntry;
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};
use super::hfs_common::resolve_create_type_creator;
use super::ofs::{
    put_be32, unpack_location, validate_ofs_name, OfsAttrs, OfsEntry, OfsFilesystem,
    ENTRIES_PER_BLOCK, SECTOR,
};

/// Bytes of block header before the first entry.
const BLOCK_HEADER: usize = 64;
/// `file_type` for a directory.
const TYPE_DIRECTORY: u32 = 0xFFFF_FFFF;
/// Set in `first_alloc_list` when the file is one contiguous run.
const CONTIGUOUS_FLAG: u32 = 0x8000_0000;
/// Terminator in an extent list.
const EXTENT_END: u32 = 0xFFFF_FFFF;
/// `{start, count}` pairs in one extent-list sector; pair 0 is the chain head.
const EXTENTS_PER_SECTOR: usize = 64;
/// A `count` field is 32-bit, but keep runs to something a `physical_size`
/// (also 32-bit, in bytes) can express.
const MAX_RUN_SECTORS: u64 = 0x7F_FFFF;
/// Refuse to grow a directory past this many blocks; the chain is linear.
const MAX_DIR_BLOCKS: usize = 4096;

impl<R: Read + Write + Seek + Send> OfsFilesystem<R> {
    fn write_sectors(&mut self, sector: u64, data: &[u8]) -> Result<(), FilesystemError> {
        let count = (data.len() as u64).div_ceil(SECTOR);
        if sector.saturating_add(count) > self.toc.total_sectors as u64 + 1 {
            return Err(FilesystemError::InvalidData(format!(
                "ofs: write of {count} sectors at {sector} runs past the volume"
            )));
        }
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + sector * SECTOR))?;
        self.reader.write_all(data)?;
        Ok(())
    }

    // ---- allocation bitmap ----

    fn read_bitmap(&mut self) -> Result<Vec<u8>, FilesystemError> {
        let (start, count) = (self.toc.bitmap_start as u64, self.toc.bitmap_sectors as u64);
        self.read_sectors(start, count)
    }

    fn write_bitmap(&mut self, bitmap: &[u8]) -> Result<(), FilesystemError> {
        let start = self.toc.bitmap_start as u64;
        self.write_sectors(start, bitmap)
    }

    /// Bitmap writer for `ofs_fsck`, which takes its own snapshot first.
    pub(crate) fn write_bitmap_at(
        &mut self,
        start: u64,
        bitmap: &[u8],
    ) -> Result<(), FilesystemError> {
        self.write_sectors(start, bitmap)
    }

    fn bit_is_set(bitmap: &[u8], sector: u64) -> bool {
        let byte = (sector / 8) as usize;
        byte < bitmap.len() && bitmap[byte] & (1 << (sector % 8)) != 0
    }

    fn set_bits(bitmap: &mut [u8], sector: u64, count: u64, on: bool) {
        for s in sector..sector + count {
            let byte = (s / 8) as usize;
            if byte >= bitmap.len() {
                continue;
            }
            let mask = 1u8 << (s % 8);
            if on {
                bitmap[byte] |= mask;
            } else {
                bitmap[byte] &= !mask;
            }
        }
    }

    /// First free run of at least `want` sectors, or the longest one there is
    /// when nothing that long exists (`(start, len)`).
    fn best_free_run(&self, bitmap: &[u8], want: u64) -> Option<(u64, u64)> {
        let first = self.toc.bitmap_start as u64 + self.toc.bitmap_sectors as u64;
        let total = self.toc.total_sectors as u64;
        let mut best: Option<(u64, u64)> = None;
        let mut run_start = first;
        let mut run_len = 0u64;
        for s in first..total {
            if Self::bit_is_set(bitmap, s) {
                if run_len > 0 && best.map(|(_, l)| run_len > l).unwrap_or(true) {
                    best = Some((run_start, run_len));
                }
                run_start = s + 1;
                run_len = 0;
                continue;
            }
            run_len += 1;
            if run_len >= want {
                return Some((run_start, run_len));
            }
        }
        if run_len > 0 && best.map(|(_, l)| run_len > l).unwrap_or(true) {
            best = Some((run_start, run_len));
        }
        best
    }

    /// Allocate `count` sectors as one run, or fail.
    fn alloc_contiguous(&mut self, count: u64) -> Result<u64, FilesystemError> {
        let mut bitmap = self.read_bitmap()?;
        let (start, len) = self
            .best_free_run(&bitmap, count)
            .filter(|(_, l)| *l >= count)
            .ok_or_else(|| {
                FilesystemError::DiskFull(format!("ofs: no free run of {count} sectors"))
            })?;
        let _ = len;
        Self::set_bits(&mut bitmap, start, count, true);
        self.write_bitmap(&bitmap)?;
        self.toc.used_sectors = self.toc.used_sectors.saturating_add(count as u32);
        self.toc_dirty = true;
        Ok(start)
    }

    /// Allocate `count` sectors as however many runs the free space allows.
    fn alloc_extents(&mut self, count: u64) -> Result<Vec<(u64, u64)>, FilesystemError> {
        let mut bitmap = self.read_bitmap()?;
        let mut runs: Vec<(u64, u64)> = Vec::new();
        let mut remaining = count;
        while remaining > 0 {
            let Some((start, len)) = self.best_free_run(&bitmap, remaining) else {
                for (s, c) in &runs {
                    Self::set_bits(&mut bitmap, *s, *c, false);
                }
                self.write_bitmap(&bitmap)?;
                return Err(FilesystemError::DiskFull(format!(
                    "ofs: only {} of {count} sectors could be allocated",
                    count - remaining
                )));
            };
            let take = len.min(remaining).min(MAX_RUN_SECTORS);
            Self::set_bits(&mut bitmap, start, take, true);
            runs.push((start, take));
            remaining -= take;
            if runs.len() > EXTENTS_PER_SECTOR - 2 {
                for (s, c) in &runs {
                    Self::set_bits(&mut bitmap, *s, *c, false);
                }
                self.write_bitmap(&bitmap)?;
                return Err(FilesystemError::DiskFull(
                    "ofs: the volume is too fragmented to hold this file in 62 extents".into(),
                ));
            }
        }
        self.write_bitmap(&bitmap)?;
        self.toc.used_sectors = self.toc.used_sectors.saturating_add(count as u32);
        self.toc_dirty = true;
        Ok(runs)
    }

    fn free_sectors(&mut self, runs: &[(u64, u64)]) -> Result<(), FilesystemError> {
        if runs.is_empty() {
            return Ok(());
        }
        let mut bitmap = self.read_bitmap()?;
        let mut freed = 0u64;
        for (start, count) in runs {
            Self::set_bits(&mut bitmap, *start, *count, false);
            freed += count;
        }
        self.write_bitmap(&bitmap)?;
        self.toc.used_sectors = self.toc.used_sectors.saturating_sub(freed as u32);
        self.toc_dirty = true;
        Ok(())
    }

    /// Flush the table of contents' counters.
    pub(crate) fn sync_toc(&mut self) -> Result<(), FilesystemError> {
        if !self.toc_dirty {
            return Ok(());
        }
        let mut buf = self.read_sectors(0, 1)?;
        put_be32(&mut buf, 36, self.toc.used_sectors);
        self.write_sectors(0, &buf)?;
        self.toc_dirty = false;
        Ok(())
    }

    // ---- directory slots ----

    /// Locate a named entry inside a directory chain.
    fn find_in_dir(&mut self, start: u64, name: &str) -> Result<Option<OfsEntry>, FilesystemError> {
        Ok(self
            .read_directory(start)?
            .into_iter()
            .find(|e| e.name == name))
    }

    /// First free slot in a directory chain, extending it with a fresh block
    /// when every existing slot is taken.
    fn claim_slot(&mut self, start: u64) -> Result<(u64, usize), FilesystemError> {
        let name_len = self.toc.name_len();
        let stride = self.toc.entry_size();
        let block_sectors = self.toc.block_sectors();
        let mut block = start;
        let mut last = start;
        let mut visited = 0usize;
        while block != 0 {
            visited += 1;
            if visited > MAX_DIR_BLOCKS {
                return Err(FilesystemError::Parse(
                    "ofs: directory block chain is longer than 4096 blocks (cycle?)".into(),
                ));
            }
            let raw = self.read_sectors(block, block_sectors)?;
            for index in 0..ENTRIES_PER_BLOCK {
                let off = BLOCK_HEADER + index * stride;
                if off + name_len <= raw.len() && raw[off] == 0 {
                    return Ok((block, index));
                }
            }
            last = block;
            block = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as u64;
        }

        let fresh = self.alloc_contiguous(block_sectors)?;
        let zeros = vec![0u8; (block_sectors * SECTOR) as usize];
        self.write_sectors(fresh, &zeros)?;
        let mut tail = self.read_sectors(last, block_sectors)?;
        put_be32(&mut tail, 0, fresh as u32);
        self.write_sectors(last, &tail)?;
        Ok((fresh, 0))
    }

    /// Write a name + attributes into one slot.
    fn write_slot(
        &mut self,
        block: u64,
        index: usize,
        name: &str,
        attrs: &OfsAttrs,
    ) -> Result<(), FilesystemError> {
        let name_len = self.toc.name_len();
        let stride = self.toc.entry_size();
        let block_sectors = self.toc.block_sectors();
        let mut raw = self.read_sectors(block, block_sectors)?;
        let off = BLOCK_HEADER + index * stride;
        for b in &mut raw[off..off + stride] {
            *b = 0;
        }
        let bytes = name.as_bytes();
        raw[off..off + bytes.len()].copy_from_slice(bytes);
        attrs.write(&mut raw, off + name_len);
        self.write_sectors(block, &raw)
    }

    /// Blank a slot so the next create can take it.
    fn clear_slot(&mut self, block: u64, index: usize) -> Result<(), FilesystemError> {
        let stride = self.toc.entry_size();
        let block_sectors = self.toc.block_sectors();
        let mut raw = self.read_sectors(block, block_sectors)?;
        let off = BLOCK_HEADER + index * stride;
        for b in &mut raw[off..off + stride] {
            *b = 0;
        }
        self.write_sectors(block, &raw)
    }

    /// Sectors an entry owns, so a delete can hand them back.
    fn owned_runs(&mut self, attrs: &OfsAttrs) -> Result<Vec<(u64, u64)>, FilesystemError> {
        if attrs.is_directory() {
            // Directory blocks are a chain; each block is one run.
            let mut runs = Vec::new();
            let mut block = attrs.first_alloc_list as u64;
            let block_sectors = self.toc.block_sectors();
            let mut visited = 0usize;
            while block != 0 && visited < MAX_DIR_BLOCKS {
                visited += 1;
                runs.push((block, block_sectors));
                let raw = self.read_sectors(block, block_sectors)?;
                block = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as u64;
            }
            return Ok(runs);
        }
        let mut runs = self.file_extents(attrs)?;
        if !attrs.is_contiguous() && attrs.first_alloc_list != 0 {
            // The extent-list sector itself is allocated too.
            runs.push((attrs.first_alloc_list as u64, 1));
        }
        Ok(runs)
    }

    /// Lay `data` down and return the attribute fields that describe it.
    fn store_data(&mut self, data: &[u8]) -> Result<(u32, u32, u32), FilesystemError> {
        if data.is_empty() {
            return Ok((CONTIGUOUS_FLAG, 0, 0));
        }
        let sectors = (data.len() as u64).div_ceil(SECTOR);
        if let Ok(start) = self.alloc_contiguous(sectors) {
            let mut padded = vec![0u8; (sectors * SECTOR) as usize];
            padded[..data.len()].copy_from_slice(data);
            self.write_sectors(start, &padded)?;
            return Ok((
                start as u32 | CONTIGUOUS_FLAG,
                sectors as u32,
                (sectors * SECTOR) as u32,
            ));
        }

        // Fragmented: one extent-list sector plus however many runs it takes.
        let runs = self.alloc_extents(sectors)?;
        // `inspect_err` is 1.76; the engine floor is 1.73.
        let list = match self.alloc_contiguous(1) {
            Ok(l) => l,
            Err(e) => {
                let _ = self.free_sectors(&runs);
                return Err(e);
            }
        };
        let mut cursor = 0usize;
        for (start, count) in &runs {
            let span = (*count * SECTOR) as usize;
            let take = span.min(data.len() - cursor);
            let mut padded = vec![0u8; span];
            padded[..take].copy_from_slice(&data[cursor..cursor + take]);
            self.write_sectors(*start, &padded)?;
            cursor += take;
        }
        let mut raw = vec![0u8; SECTOR as usize];
        for (i, (start, count)) in runs.iter().enumerate() {
            put_be32(&mut raw, (i + 1) * 8, *start as u32);
            put_be32(&mut raw, (i + 1) * 8 + 4, *count as u32);
        }
        put_be32(&mut raw, (runs.len() + 1) * 8, EXTENT_END);
        self.write_sectors(list, &raw)?;
        Ok((list as u32, list as u32, (sectors * SECTOR) as u32))
    }

    fn parent_dir_start(&mut self, parent: &FileEntry) -> Result<u64, FilesystemError> {
        if !parent.is_directory() {
            return Err(FilesystemError::NotADirectory(parent.path.clone()));
        }
        self.dir_start(parent)
    }

    fn child_entry(&self, parent: &FileEntry, name: &str, attrs: &OfsAttrs, loc: u64) -> FileEntry {
        let parent_path = if parent.path == "/" {
            String::new()
        } else {
            parent.path.clone()
        };
        let path = format!("{parent_path}/{name}");
        let mut out = if attrs.is_directory() {
            FileEntry::new_directory(name.to_string(), path, loc)
        } else {
            FileEntry::new_file(name.to_string(), path, attrs.logical_size as u64, loc)
        };
        if attrs.file_type != 0 && !attrs.is_directory() {
            out.type_code = Some(attrs.file_type.to_be_bytes());
        }
        if attrs.creator != 0 {
            out.creator_code = Some(attrs.creator.to_be_bytes());
        }
        out.modified_unix = Some(attrs.modify_date as u64);
        out
    }
}

impl<R: Read + Write + Seek + Send> EditableFilesystem for OfsFilesystem<R> {
    fn as_filesystem(&self) -> &dyn Filesystem {
        self
    }
    fn as_filesystem_mut(&mut self) -> &mut dyn Filesystem {
        self
    }

    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn Read,
        data_len: u64,
        options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        validate_ofs_name(name, self.toc.name_len())?;
        if data_len > u32::MAX as u64 {
            return Err(FilesystemError::Unsupported(
                "ofs: file sizes are a 32-bit field; 4 GiB is the ceiling".into(),
            ));
        }
        let start = self.parent_dir_start(parent)?;
        if self.find_in_dir(start, name)?.is_some() {
            return Err(FilesystemError::AlreadyExists(name.to_string()));
        }
        let mut bytes = Vec::with_capacity(data_len.min(64 << 20) as usize);
        data.take(data_len).read_to_end(&mut bytes)?;
        if bytes.len() as u64 != data_len {
            return Err(FilesystemError::InvalidData(format!(
                "ofs create_file: source gave {} bytes, expected {data_len}",
                bytes.len()
            )));
        }

        let (first, last, physical) = self.store_data(&bytes)?;
        // Early BeOS used Mac-style type/creator codes, so the same resolution
        // the HFS drivers use gives an untyped copy a sensible pair.
        let (type_code, creator) = resolve_create_type_creator(
            name,
            options.os_type,
            options.os_creator,
            options.type_code.as_deref(),
            options.creator_code.as_deref(),
        );
        let now = super::times::resolve_or_now(options.unix_times).mtime_or_now() as u32;
        let attrs = OfsAttrs {
            first_alloc_list: first,
            last_alloc_list: last,
            file_type: u32::from_be_bytes(type_code),
            create_date: now,
            modify_date: now,
            logical_size: bytes.len() as u32,
            physical_size: physical,
            creator: u32::from_be_bytes(creator),
        };

        let (block, index) = self.claim_slot(start)?;
        self.write_slot(block, index, name, &attrs)?;
        self.sync_toc()?;
        let loc = (block << 16) | index as u64;
        Ok(self.child_entry(parent, name, &attrs, loc))
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        validate_ofs_name(name, self.toc.name_len())?;
        let start = self.parent_dir_start(parent)?;
        if self.find_in_dir(start, name)?.is_some() {
            return Err(FilesystemError::AlreadyExists(name.to_string()));
        }

        let block_sectors = self.toc.block_sectors();
        let fresh = self.alloc_contiguous(block_sectors)?;
        let zeros = vec![0u8; (block_sectors * SECTOR) as usize];
        self.write_sectors(fresh, &zeros)?;

        let now = super::times::resolve_or_now(options.unix_times).mtime_or_now() as u32;
        let attrs = OfsAttrs {
            first_alloc_list: fresh as u32,
            last_alloc_list: 0,
            file_type: TYPE_DIRECTORY,
            create_date: now,
            modify_date: now,
            logical_size: 0,
            physical_size: 0,
            creator: 0,
        };
        let (block, index) = match self.claim_slot(start) {
            Ok(v) => v,
            Err(e) => {
                let _ = self.free_sectors(&[(fresh, block_sectors)]);
                return Err(e);
            }
        };
        self.write_slot(block, index, name, &attrs)?;
        self.sync_toc()?;
        let loc = (block << 16) | index as u64;
        Ok(self.child_entry(parent, name, &attrs, loc))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        let start = self.parent_dir_start(parent)?;
        let located = self
            .find_in_dir(start, &entry.name)?
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;
        if located.attrs.is_directory() {
            let children = self.read_directory(located.attrs.first_alloc_list as u64)?;
            if !children.is_empty() {
                return Err(FilesystemError::InvalidData(format!(
                    "ofs: directory '{}' is not empty",
                    entry.path
                )));
            }
        }
        let runs = self.owned_runs(&located.attrs)?;
        self.clear_slot(located.block, located.index)?;
        self.free_sectors(&runs)?;
        self.sync_toc()
    }

    fn rename(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
        new_name: &str,
    ) -> Result<(), FilesystemError> {
        validate_ofs_name(new_name, self.toc.name_len())?;
        if new_name == entry.name {
            return Ok(());
        }
        let start = self.parent_dir_start(parent)?;
        if self.find_in_dir(start, new_name)?.is_some() {
            return Err(FilesystemError::AlreadyExists(new_name.to_string()));
        }
        let located = self
            .find_in_dir(start, &entry.name)?
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;
        self.write_slot(located.block, located.index, new_name, &located.attrs)
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.sync_toc()
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        let free = self.toc.total_sectors.saturating_sub(self.toc.used_sectors) as u64;
        Ok(free * SECTOR)
    }

    fn repair(&mut self) -> Result<super::fsck::RepairReport, FilesystemError> {
        self.repair_ofs()
    }
}

/// Split a packed `FileEntry::location` for callers outside the module.
pub fn location_parts(loc: u64) -> (u64, usize) {
    unpack_location(loc)
}

/// Format a blank version-1 OFS volume in memory.
///
/// Layout is the one the Hobbit BeBox image has: the table of contents in
/// sector 0, the allocation bitmap from sector 1, and the root directory's
/// first block immediately after it. `size_bytes` rounds down to a whole
/// sector. Returns the image bytes.
pub fn create_blank_ofs(size_bytes: u64, name: &str) -> Result<Vec<u8>, FilesystemError> {
    let total = size_bytes / SECTOR;
    if name.len() > 31 {
        return Err(FilesystemError::InvalidData(
            "ofs: volume name is capped at 31 bytes".into(),
        ));
    }
    // Version 1 directory blocks are 8 sectors; the bitmap has to cover the
    // whole volume, and the root block has to fit after it.
    let bitmap_sectors = total.div_ceil(8).div_ceil(SECTOR).max(1);
    let first_dir = 1 + bitmap_sectors;
    let used = first_dir + 8;
    if total < used + 8 {
        return Err(FilesystemError::InvalidData(format!(
            "ofs: {size_bytes} bytes is too small for a table of contents, a bitmap, and a              root directory (needs at least {} bytes)",
            (used + 8) * SECTOR
        )));
    }
    if total > u32::MAX as u64 {
        return Err(FilesystemError::InvalidData(
            "ofs: sector numbers are a 32-bit field; 2 TiB is the ceiling".into(),
        ));
    }

    let mut img = vec![0u8; (total * SECTOR) as usize];
    put_be32(&mut img, 0, 0x0001_0000);
    put_be32(&mut img, 4, super::times::now() as u32);
    put_be32(&mut img, 8, 1);
    put_be32(&mut img, 12, bitmap_sectors as u32);
    put_be32(&mut img, 16, first_dir as u32);
    put_be32(&mut img, 20, total as u32);
    put_be32(&mut img, 24, SECTOR as u32);
    put_be32(&mut img, 36, used as u32);
    img[44..44 + name.len()].copy_from_slice(name.as_bytes());

    // Mark the TOC, the bitmap, and the root directory block as allocated.
    for s in 0..used {
        let byte = SECTOR as usize + (s / 8) as usize;
        img[byte] |= 1 << (s % 8);
    }
    Ok(img)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// A 4 MiB version-1 volume: TOC, bitmap, and an empty root directory.
    fn blank_volume() -> Cursor<Vec<u8>> {
        Cursor::new(create_blank_ofs(4 * 1024 * 1024, "be").expect("format"))
    }

    fn open() -> OfsFilesystem<Cursor<Vec<u8>>> {
        OfsFilesystem::open(blank_volume(), 0).expect("blank volume opens")
    }

    #[test]
    fn a_volume_too_small_for_its_own_metadata_is_refused() {
        assert!(create_blank_ofs(4096, "tiny").is_err());
    }

    #[test]
    fn a_formatted_volume_opens_with_an_empty_root() {
        let mut fs = open();
        assert_eq!(fs.volume_label(), Some("be"));
        let root = fs.root().unwrap();
        assert!(fs.list_directory(&root).unwrap().is_empty());
    }

    #[test]
    fn create_read_back_and_delete_a_file() {
        let mut fs = open();
        let root = fs.root().unwrap();
        let body = b"the quick brown fox".to_vec();
        let created = fs
            .create_file(
                &root,
                "fox.txt",
                &mut body.as_slice(),
                body.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
        assert_eq!(created.size, body.len() as u64);

        let listed = fs.list_directory(&root).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name, "fox.txt");
        assert_eq!(fs.read_file(&listed[0], usize::MAX).unwrap(), body);

        fs.delete_entry(&root, &listed[0]).unwrap();
        assert!(fs.list_directory(&root).unwrap().is_empty());
    }

    #[test]
    fn deleting_hands_every_sector_back() {
        let mut fs = open();
        let root = fs.root().unwrap();
        let before = fs.free_space().unwrap();
        let body = vec![7u8; 40_000];
        fs.create_file(
            &root,
            "blob",
            &mut body.as_slice(),
            body.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        assert!(fs.free_space().unwrap() < before);
        let listed = fs.list_directory(&root).unwrap();
        fs.delete_entry(&root, &listed[0]).unwrap();
        assert_eq!(fs.free_space().unwrap(), before);
    }

    #[test]
    fn a_directory_holds_its_own_children() {
        let mut fs = open();
        let root = fs.root().unwrap();
        let dir = fs
            .create_directory(&root, "sub", &CreateDirectoryOptions::default())
            .unwrap();
        let body = b"nested".to_vec();
        fs.create_file(
            &dir,
            "inner",
            &mut body.as_slice(),
            body.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();

        assert_eq!(fs.list_directory(&root).unwrap().len(), 1);
        let inner = fs.list_directory(&dir).unwrap();
        assert_eq!(inner.len(), 1);
        assert_eq!(inner[0].name, "inner");
        assert_eq!(fs.read_file(&inner[0], usize::MAX).unwrap(), body);
    }

    #[test]
    fn a_non_empty_directory_will_not_delete() {
        let mut fs = open();
        let root = fs.root().unwrap();
        let dir = fs
            .create_directory(&root, "sub", &CreateDirectoryOptions::default())
            .unwrap();
        fs.create_file(
            &dir,
            "inner",
            &mut b"x".as_slice(),
            1,
            &CreateFileOptions::default(),
        )
        .unwrap();
        let listed = fs.list_directory(&root).unwrap();
        assert!(fs.delete_entry(&root, &listed[0]).is_err());
    }

    /// 63 slots per block, so the 64th entry has to grow the chain.
    #[test]
    fn the_directory_chain_grows_past_sixty_three_entries() {
        let mut fs = open();
        let root = fs.root().unwrap();
        for i in 0..70 {
            let name = format!("f{i:03}");
            fs.create_file(
                &root,
                &name,
                &mut name.as_bytes(),
                name.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
        }
        let listed = fs.list_directory(&root).unwrap();
        assert_eq!(listed.len(), 70);
        for e in &listed {
            assert_eq!(fs.read_file(e, usize::MAX).unwrap(), e.name.as_bytes());
        }
    }

    #[test]
    fn rename_keeps_the_contents() {
        let mut fs = open();
        let root = fs.root().unwrap();
        fs.create_file(
            &root,
            "before",
            &mut b"same bytes".as_slice(),
            10,
            &CreateFileOptions::default(),
        )
        .unwrap();
        let listed = fs.list_directory(&root).unwrap();
        fs.rename(&root, &listed[0], "after").unwrap();
        let listed = fs.list_directory(&root).unwrap();
        assert_eq!(listed[0].name, "after");
        assert_eq!(fs.read_file(&listed[0], usize::MAX).unwrap(), b"same bytes");
    }

    #[test]
    fn duplicate_names_are_refused() {
        let mut fs = open();
        let root = fs.root().unwrap();
        fs.create_file(
            &root,
            "dup",
            &mut b"a".as_slice(),
            1,
            &CreateFileOptions::default(),
        )
        .unwrap();
        assert!(fs
            .create_file(
                &root,
                "dup",
                &mut b"b".as_slice(),
                1,
                &CreateFileOptions::default()
            )
            .is_err());
    }
}
