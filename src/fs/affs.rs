//! AmigaDOS Fast/Original File System (AFFS) reader.
//!
//! Supports all eight DosType variants `DOS\0`..`DOS\7`:
//!   - OFS / FFS data block layouts
//!   - International (Intl) case folding (`DOS\2`, `DOS\3`, `DOS\6`, `DOS\7`)
//!   - Directory Cache (`DOS\4`, `DOS\5`) — cache blocks ignored on read;
//!     directories are walked via the canonical hash table instead
//!   - Long File Names (`DOS\6`, `DOS\7`) — read as plain header blocks
//!     (the long-name extension lives in the existing `name` field for
//!     names ≤30 chars; longer names will be added when write support
//!     lands).
//!
//! Floppies use a fixed 512-byte block size; RDB partitions take their block
//! size from the DosEnv. This module currently requires `block_size == 512`
//! and rejects others — every real-world AFFS volume uses 512.

use std::io::{Read, Seek, SeekFrom, Write};

use super::affs_common::*;
use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};
use super::CompactResult;

/// Reusable error helper.
fn parse_err<S: Into<String>>(msg: S) -> FilesystemError {
    FilesystemError::Parse(msg.into())
}

/// Decoded root block.
#[derive(Debug, Clone)]
pub struct AffsRootBlock {
    pub block_num: u32,
    pub ht_size: u32,
    pub hash_table: Vec<u32>, // length == ht_size
    pub bm_flag: i32,         // -1 = valid
    pub bm_pages: [u32; BM_PAGES_ROOT],
    pub bm_ext: u32,
    pub disk_name: String,
    pub create_days: i32,
    pub create_mins: i32,
    pub create_ticks: i32,
    pub modify_days: i32,
    pub modify_mins: i32,
    pub modify_ticks: i32,
}

impl AffsRootBlock {
    fn parse(buf: &[u8; BSIZE], block_num: u32) -> Result<Self, FilesystemError> {
        if read_i32(buf, 0) != T_HEADER {
            return Err(parse_err("root block: type != T_HEADER"));
        }
        if read_i32(buf, BSIZE - 4) != ST_ROOT {
            return Err(parse_err("root block: secType != ST_ROOT"));
        }
        if !verify_normal_checksum(buf, 5) {
            return Err(parse_err(format!(
                "root block: checksum mismatch (block {block_num})"
            )));
        }
        let ht_size = read_u32(buf, 12);
        if ht_size as usize != HT_SIZE {
            return Err(parse_err(format!(
                "root block: hashTableSize {ht_size} != {HT_SIZE}"
            )));
        }
        let mut hash_table = Vec::with_capacity(HT_SIZE);
        for i in 0..HT_SIZE {
            hash_table.push(read_u32(buf, 0x18 + i * 4));
        }
        let bm_flag = read_i32(buf, 0x138);
        let mut bm_pages = [0u32; BM_PAGES_ROOT];
        for (i, slot) in bm_pages.iter_mut().enumerate() {
            *slot = read_u32(buf, 0x13C + i * 4);
        }
        let bm_ext = read_u32(buf, 0x1A0);

        let disk_name = read_bstr(buf, 0x1B0, MAX_NAME_LEN);
        let create_days = read_i32(buf, 0x1A4);
        let create_mins = read_i32(buf, 0x1A8);
        let create_ticks = read_i32(buf, 0x1AC);
        let modify_days = read_i32(buf, 0x1D8);
        let modify_mins = read_i32(buf, 0x1DC);
        let modify_ticks = read_i32(buf, 0x1E0);

        Ok(AffsRootBlock {
            block_num,
            ht_size,
            hash_table,
            bm_flag,
            bm_pages,
            bm_ext,
            disk_name,
            create_days,
            create_mins,
            create_ticks,
            modify_days,
            modify_mins,
            modify_ticks,
        })
    }
}

/// Decoded directory entry header (shared by file headers, user dirs, links).
#[derive(Debug, Clone)]
pub struct AffsEntry {
    pub block_num: u32,
    pub sec_type: i32,
    pub byte_size: u32,
    pub name: String,
    pub comment: String,
    pub access: u32,
    pub modify_days: i32,
    pub modify_mins: i32,
    pub modify_ticks: i32,
    pub next_same_hash: u32,
    pub parent: u32,
    pub extension: u32,
    /// `firstData` for files; unused for directories.
    pub first_data: u32,
    /// `highSeq` — number of data blocks referenced by this header.
    pub high_seq: u32,
    /// Inline data-block pointer array (stored in reverse order on disk,
    /// already reversed here so index 0 = first data block of the file).
    pub data_blocks: Vec<u32>,
    /// For user-dir blocks: hash table.
    pub hash_table: Vec<u32>,
}

impl AffsEntry {
    fn parse(buf: &[u8; BSIZE], block_num: u32) -> Result<Self, FilesystemError> {
        if read_i32(buf, 0) != T_HEADER {
            return Err(parse_err(format!(
                "entry block {block_num}: type != T_HEADER"
            )));
        }
        if !verify_normal_checksum(buf, 5) {
            return Err(parse_err(format!(
                "entry block {block_num}: checksum mismatch"
            )));
        }
        let sec_type = read_i32(buf, BSIZE - 4);
        let high_seq = read_u32(buf, 8);
        let first_data = read_u32(buf, 0x10);
        let access = read_u32(buf, 0x140);
        let byte_size = read_u32(buf, 0x144);
        let comment = read_bstr(buf, 0x148, MAX_COMMENT_LEN);
        let modify_days = read_i32(buf, 0x1A4);
        let modify_mins = read_i32(buf, 0x1A8);
        let modify_ticks = read_i32(buf, 0x1AC);
        let name = read_bstr(buf, 0x1B0, MAX_NAME_LEN);
        let next_same_hash = read_u32(buf, 0x1F0);
        let parent = read_u32(buf, 0x1F4);
        let extension = read_u32(buf, 0x1F8);

        let mut data_blocks = Vec::new();
        let mut hash_table = Vec::new();
        if sec_type == ST_FILE {
            // File header / file ext: dataBlocks[72] at offset 0x18..0x138,
            // stored on disk in REVERSE order — dataBlocks[71] is the file's
            // first data block. Reverse them here so callers can index 0..n.
            let count = (high_seq as usize).min(MAX_DATA_BLOCKS);
            data_blocks.reserve(count);
            for i in 0..count {
                let slot = MAX_DATA_BLOCKS - 1 - i;
                data_blocks.push(read_u32(buf, 0x18 + slot * 4));
            }
        } else if sec_type == ST_ROOT || sec_type == ST_DIR {
            hash_table.reserve(HT_SIZE);
            for i in 0..HT_SIZE {
                hash_table.push(read_u32(buf, 0x18 + i * 4));
            }
        }

        Ok(AffsEntry {
            block_num,
            sec_type,
            byte_size,
            name,
            comment,
            access,
            modify_days,
            modify_mins,
            modify_ticks,
            next_same_hash,
            parent,
            extension,
            first_data,
            high_seq,
            data_blocks,
            hash_table,
        })
    }
}

/// Top-level AFFS filesystem.
pub struct AffsFilesystem<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    partition_size: u64,
    block_size: u64,
    variant: u8,
    intl: bool,
    ffs: bool,
    /// Root block number (in 512-byte sectors from partition start).
    root_block: u32,
    /// Parsed root block — kept for volume label, dates, hash table.
    root: AffsRootBlock,
    /// Volume bitmap, concatenated across all bitmap pages. `bit i` corresponds
    /// to allocation block `(2 + i)` — the first two blocks (boot) are not
    /// represented in the bitmap. Bit **set** = block is **free**.
    bitmap: Vec<u8>,
    /// Total blocks tracked by the bitmap.
    total_blocks: u32,
}

impl<R: Read + Seek> AffsFilesystem<R> {
    /// Open the filesystem at the given partition offset.
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        // Boot block: 1024 bytes. Magic "DOS\x".
        let mut boot = [0u8; 1024];
        reader.seek(SeekFrom::Start(partition_offset))?;
        reader.read_exact(&mut boot)?;
        if &boot[0..3] != b"DOS" {
            return Err(parse_err("not an AmigaDOS volume: missing 'DOS' magic"));
        }
        let variant = boot[3];
        if variant > 7 {
            return Err(parse_err(format!("unknown DosType variant {variant}")));
        }
        let intl = is_intl_variant(variant);
        let ffs = is_ffs_variant(variant);

        // Determine partition size by seeking to end.
        let end = reader.seek(SeekFrom::End(0))?;
        let partition_size = end.saturating_sub(partition_offset);
        if partition_size < 4 * BSIZE_U64 {
            return Err(parse_err("partition too small to host AFFS"));
        }
        let block_size = BSIZE_U64;
        let total_blocks = (partition_size / block_size) as u32;

        // Root block: floor((reserved + total) / 2) — reserved=2, total =
        // total blocks. For an 880K floppy this is (2 + 1760)/2 = 881.
        // ADFlib uses ((reserved + numBlocks - 1) / 2) which gives 880 for
        // 1760 sectors; that's the canonical value.
        let root_block = (2 + total_blocks - 1) / 2;

        let mut root_buf = [0u8; BSIZE];
        reader.seek(SeekFrom::Start(
            partition_offset + root_block as u64 * block_size,
        ))?;
        reader.read_exact(&mut root_buf)?;
        let root = AffsRootBlock::parse(&root_buf, root_block)?;

        // Load the bitmap.
        let bitmap = read_bitmap(
            &mut reader,
            partition_offset,
            block_size,
            &root,
            total_blocks,
        )?;

        Ok(AffsFilesystem {
            reader,
            partition_offset,
            partition_size,
            block_size,
            variant,
            intl,
            ffs,
            root_block,
            root,
            bitmap,
            total_blocks,
        })
    }

    /// Bit index for a given block: blocks 0 and 1 (boot) are not in the
    /// bitmap; bit 0 covers block 2, bit 1 covers block 3, etc.
    fn block_is_allocated(&self, block: u32) -> bool {
        if block < 2 {
            return true; // boot blocks
        }
        let bit = (block - 2) as usize;
        let byte = bit / 8;
        let off = bit % 8;
        if byte >= self.bitmap.len() {
            return false;
        }
        // AFFS bitmap convention: bit SET = block FREE.
        (self.bitmap[byte] >> off) & 1 == 0
    }

    fn read_block(&mut self, block: u32) -> Result<[u8; BSIZE], FilesystemError> {
        let offset = self.partition_offset + block as u64 * self.block_size;
        if offset + BSIZE_U64 > self.partition_offset + self.partition_size {
            return Err(parse_err(format!(
                "block {block} out of range (offset {offset})"
            )));
        }
        let mut buf = [0u8; BSIZE];
        self.reader.seek(SeekFrom::Start(offset))?;
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn read_entry(&mut self, block: u32) -> Result<AffsEntry, FilesystemError> {
        let buf = self.read_block(block)?;
        AffsEntry::parse(&buf, block)
    }

    fn list_hash_table(
        &mut self,
        hash_table: &[u32],
        parent_path: &str,
    ) -> Result<Vec<FileEntry>, FilesystemError> {
        let mut out = Vec::new();
        for &head in hash_table {
            let mut next = head;
            while next != 0 {
                let entry = self.read_entry(next)?;
                next = entry.next_same_hash;
                out.push(self.entry_to_file_entry(&entry, parent_path));
            }
        }
        // AmigaDOS doesn't define a directory order; sort alphabetically for
        // a predictable browse experience.
        out.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
        Ok(out)
    }

    fn entry_to_file_entry(&self, entry: &AffsEntry, parent_path: &str) -> FileEntry {
        let path = if parent_path == "/" {
            format!("/{}", entry.name)
        } else {
            format!("{parent_path}/{}", entry.name)
        };
        let mut fe = match entry.sec_type {
            ST_DIR => FileEntry::new_directory(entry.name.clone(), path, entry.block_num as u64),
            ST_FILE => FileEntry::new_file(
                entry.name.clone(),
                path,
                entry.byte_size as u64,
                entry.block_num as u64,
            ),
            ST_LFILE | ST_LDIR | ST_LSOFT => FileEntry::new_symlink(
                entry.name.clone(),
                path,
                entry.byte_size as u64,
                entry.block_num as u64,
                String::new(),
            ),
            _ => FileEntry::new_file(
                entry.name.clone(),
                path,
                entry.byte_size as u64,
                entry.block_num as u64,
            ),
        };
        fe.modified = datestamp_string(entry.modify_days, entry.modify_mins, entry.modify_ticks);
        fe
    }

    /// Read all data bytes for a file entry. Walks the inline data-block
    /// table and any extension blocks. Honors `byte_size` so trailing
    /// padding in the last data block is dropped.
    fn read_file_data(
        &mut self,
        entry: &AffsEntry,
        max_bytes: Option<u64>,
    ) -> Result<Vec<u8>, FilesystemError> {
        let total = match max_bytes {
            Some(m) => (entry.byte_size as u64).min(m),
            None => entry.byte_size as u64,
        };
        let mut out: Vec<u8> = Vec::with_capacity(total as usize);
        let mut writer = std::io::Cursor::new(&mut out);
        self.stream_file_data(entry, total, &mut writer)?;
        Ok(out)
    }

    fn stream_file_data(
        &mut self,
        entry: &AffsEntry,
        max_bytes: u64,
        out: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let mut remaining = max_bytes;
        let mut written = 0u64;

        // Walk header → data blocks → next extension block → repeat.
        let mut current_header_blocks = entry.data_blocks.clone();
        let mut next_ext = entry.extension;

        loop {
            for &dblk in &current_header_blocks {
                if remaining == 0 {
                    return Ok(written);
                }
                if dblk == 0 {
                    continue;
                }
                let buf = self.read_block(dblk)?;
                let (payload_off, payload_len) = if self.ffs {
                    (0usize, BSIZE)
                } else {
                    // OFS data block: 24-byte header, 488 payload bytes; the
                    // header's dataSize tells us how many payload bytes are
                    // valid (last block typically holds less than 488).
                    let data_size = read_u32(&buf, 0x0C) as usize;
                    let data_size = data_size.min(488);
                    (0x18, data_size)
                };
                let chunk = (payload_len as u64).min(remaining) as usize;
                out.write_all(&buf[payload_off..payload_off + chunk])?;
                written += chunk as u64;
                remaining -= chunk as u64;
            }
            if next_ext == 0 || remaining == 0 {
                break;
            }
            let ext = self.read_entry(next_ext)?;
            if ext.sec_type != ST_FILE {
                return Err(parse_err(format!(
                    "file extension block {} has unexpected secType {}",
                    next_ext, ext.sec_type
                )));
            }
            current_header_blocks = ext.data_blocks;
            next_ext = ext.extension;
        }
        Ok(written)
    }

    pub fn variant(&self) -> u8 {
        self.variant
    }

    pub fn is_ffs(&self) -> bool {
        self.ffs
    }

    pub fn root_block_num(&self) -> u32 {
        self.root_block
    }

    pub fn total_blocks(&self) -> u32 {
        self.total_blocks
    }

    pub fn allocated_blocks(&self) -> u32 {
        let mut count = 2u32; // boot blocks always allocated
        for block in 2..self.total_blocks {
            if self.block_is_allocated(block) {
                count += 1;
            }
        }
        count
    }
}

fn read_bitmap<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    block_size: u64,
    root: &AffsRootBlock,
    total_blocks: u32,
) -> Result<Vec<u8>, FilesystemError> {
    // Each bitmap block holds 127 u32 map entries = 127*32 = 4064 bits.
    // We concatenate map[1..128] from each bitmap block into a flat byte
    // array. The bitmap covers blocks [2 .. total_blocks).
    let mut pages: Vec<u32> = root
        .bm_pages
        .iter()
        .copied()
        .take_while(|&p| p != 0)
        .collect();
    let mut bm_ext = root.bm_ext;
    let mut ext_seen = std::collections::HashSet::new();
    while bm_ext != 0 {
        if !ext_seen.insert(bm_ext) {
            return Err(parse_err("bitmap extension list contains a cycle"));
        }
        let offset = partition_offset + bm_ext as u64 * block_size;
        reader.seek(SeekFrom::Start(offset))?;
        let mut buf = [0u8; BSIZE];
        reader.read_exact(&mut buf)?;
        // Bitmap extension: bm_pages[0..127] followed by next_block at long 127.
        for i in 0..127 {
            let p = read_u32(&buf, i * 4);
            if p == 0 {
                continue;
            }
            pages.push(p);
        }
        bm_ext = read_u32(&buf, 127 * 4);
    }

    let bits_per_page = 127 * 32; // 4064
    let required_bits = total_blocks.saturating_sub(2) as usize;
    let mut bitmap = vec![0u8; required_bits.div_ceil(8)];

    for (page_idx, &page_block) in pages.iter().enumerate() {
        let offset = partition_offset + page_block as u64 * block_size;
        reader.seek(SeekFrom::Start(offset))?;
        let mut buf = [0u8; BSIZE];
        reader.read_exact(&mut buf)?;
        // We intentionally don't fail on a bad bitmap checksum here — the
        // "validation needed" condition is the entire reason a Disk Validator
        // exists. Phase 4 surfaces that. For now we just consume the bytes.
        for word_idx in 0..127 {
            let word = read_u32(&buf, (word_idx + 1) * 4);
            for bit in 0..32 {
                let global_bit = page_idx * bits_per_page + word_idx * 32 + bit;
                if global_bit >= required_bits {
                    break;
                }
                // AmigaDOS stores the bitmap in little-endian-bit order within
                // each big-endian u32: bit 0 of the *word* maps to the lowest
                // block in that group.
                let set = (word >> bit) & 1 != 0;
                if set {
                    bitmap[global_bit / 8] |= 1 << (global_bit % 8);
                }
            }
        }
    }
    Ok(bitmap)
}

impl<R: Read + Seek + Send> Filesystem for AffsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        let mut fe = FileEntry::root();
        fe.location = self.root_block as u64;
        Ok(fe)
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        let parent_path = entry.path.clone();
        // Root directory: use the cached hash table from the root block.
        if entry.location as u32 == self.root_block {
            let table = self.root.hash_table.clone();
            return self.list_hash_table(&table, &parent_path);
        }
        let dir = self.read_entry(entry.location as u32)?;
        if dir.sec_type != ST_DIR {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        self.list_hash_table(&dir.hash_table, &parent_path)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let header = self.read_entry(entry.location as u32)?;
        if header.sec_type != ST_FILE {
            return Err(parse_err(format!(
                "read_file: entry at block {} is not a file (secType {})",
                entry.location, header.sec_type
            )));
        }
        self.read_file_data(&header, Some(max_bytes as u64))
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let header = self.read_entry(entry.location as u32)?;
        if header.sec_type != ST_FILE {
            return Err(parse_err(format!(
                "write_file_to: entry at block {} is not a file (secType {})",
                entry.location, header.sec_type
            )));
        }
        let size = header.byte_size as u64;
        self.stream_file_data(&header, size, writer)
    }

    fn volume_label(&self) -> Option<&str> {
        if self.root.disk_name.is_empty() {
            None
        } else {
            Some(&self.root.disk_name)
        }
    }

    fn fs_type(&self) -> &str {
        match (
            self.ffs,
            self.intl,
            is_dircache_variant(self.variant),
            is_lnfs_variant(self.variant),
        ) {
            (false, false, false, _) => "OFS",
            (true, false, false, false) => "FFS",
            (false, true, false, _) => "OFS Intl",
            (true, true, false, false) => "FFS Intl",
            (false, _, true, _) => "OFS DirCache",
            (true, _, true, _) => "FFS DirCache",
            (_, _, _, true) => "FFS Long Names",
        }
    }

    fn total_size(&self) -> u64 {
        self.partition_size
    }

    fn used_size(&self) -> u64 {
        self.allocated_blocks() as u64 * self.block_size
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        // Highest allocated block + 1, multiplied by block size. Bitmap
        // covers [2 .. total_blocks); fall back to root_block if every
        // user-visible block is free (empty volume).
        let mut last = self.root_block;
        for block in (2..self.total_blocks).rev() {
            if self.block_is_allocated(block) {
                last = block;
                break;
            }
        }
        Ok((last as u64 + 1) * self.block_size)
    }
}

/// Layout-preserving compaction reader for AFFS volumes.
///
/// Streams the partition verbatim, except blocks marked **free** in the
/// AFFS bitmap are emitted as 512 zero bytes instead of being read from
/// the source. CHD / zstd compress those zero runs to nothing.
pub struct CompactAffsReader<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    partition_size: u64,
    block_size: u64,
    total_blocks: u32,
    /// Bit set = block is free (AmigaDOS convention).
    bitmap: Vec<u8>,
    /// Current output position relative to partition start.
    position: u64,
    /// Cached current block's source bytes, populated on demand when the
    /// block is allocated.
    block_buf: [u8; BSIZE],
    block_buf_loaded_for: Option<u32>,
}

impl<R: Read + Seek> CompactAffsReader<R> {
    pub fn new(
        mut reader: R,
        partition_offset: u64,
    ) -> Result<(Self, CompactResult), FilesystemError> {
        // Read the full filesystem header info via AffsFilesystem::open
        // so we share the same bitmap-load + validation code.
        let fs = AffsFilesystem::open(&mut reader, partition_offset)?;
        let partition_size = fs.partition_size;
        let block_size = fs.block_size;
        let total_blocks = fs.total_blocks;
        let bitmap = fs.bitmap.clone();
        let allocated = fs.allocated_blocks();

        let original_size = partition_size;
        let compacted_size = original_size; // layout-preserving
        let data_size = allocated as u64 * block_size;

        let result = CompactResult {
            original_size,
            compacted_size,
            data_size,
            clusters_used: allocated,
        };

        Ok((
            CompactAffsReader {
                reader,
                partition_offset,
                partition_size,
                block_size,
                total_blocks,
                bitmap,
                position: 0,
                block_buf: [0u8; BSIZE],
                block_buf_loaded_for: None,
            },
            result,
        ))
    }

    fn block_is_allocated(&self, block: u32) -> bool {
        if block < 2 {
            return true;
        }
        if block >= self.total_blocks {
            return true; // any tail beyond bitmap range — emit verbatim
        }
        let bit = (block - 2) as usize;
        let byte = bit / 8;
        let off = bit % 8;
        if byte >= self.bitmap.len() {
            return true;
        }
        (self.bitmap[byte] >> off) & 1 == 0
    }
}

impl<R: Read + Seek> Read for CompactAffsReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.position >= self.partition_size || buf.is_empty() {
            return Ok(0);
        }
        let block = (self.position / self.block_size) as u32;
        let block_pos = (self.position % self.block_size) as usize;
        let remaining_in_block = BSIZE - block_pos;
        let n = remaining_in_block.min(buf.len());

        if self.block_is_allocated(block) {
            if self.block_buf_loaded_for != Some(block) {
                let off = self.partition_offset + block as u64 * self.block_size;
                self.reader.seek(SeekFrom::Start(off))?;
                self.reader.read_exact(&mut self.block_buf)?;
                self.block_buf_loaded_for = Some(block);
            }
            buf[..n].copy_from_slice(&self.block_buf[block_pos..block_pos + n]);
        } else {
            buf[..n].fill(0);
        }
        self.position += n as u64;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::{BigEndian, ByteOrder};
    use std::io::Cursor;

    /// Build a minimal valid OFS-style 880K ADF image with one root block
    /// and no files. Returns the raw bytes.
    fn make_empty_floppy_image(variant: u8, volume_name: &str) -> Vec<u8> {
        const SECTORS: usize = 1760; // 880 KiB / 512
        let mut img = vec![0u8; SECTORS * BSIZE];

        // Boot block: "DOS\x" at offset 0, then a small footprint of zeros
        // (we don't need a working boot loader — the parser only checks the
        // magic, and the boot block isn't in the bitmap).
        img[0] = b'D';
        img[1] = b'O';
        img[2] = b'S';
        img[3] = variant;

        // Root block at block 880.
        let root_off = 880 * BSIZE;
        let root = &mut img[root_off..root_off + BSIZE];

        // type = T_HEADER (long 0)
        BigEndian::write_i32(&mut root[0..4], T_HEADER);
        // headerKey = 880 (long 1)
        BigEndian::write_u32(&mut root[4..8], 880);
        // hashTableSize = HT_SIZE (long 3)
        BigEndian::write_u32(&mut root[12..16], HT_SIZE as u32);
        // bm_flag = -1 (long 0x4E)
        BigEndian::write_i32(&mut root[0x138..0x13C], -1);
        // bm_pages[0] = 881
        BigEndian::write_u32(&mut root[0x13C..0x140], 881);
        // diskName BSTR at 0x1B0
        let name_bytes = volume_name.as_bytes();
        let n = name_bytes.len().min(MAX_NAME_LEN);
        root[0x1B0] = n as u8;
        root[0x1B1..0x1B1 + n].copy_from_slice(&name_bytes[..n]);
        // modify timestamp (last access)
        BigEndian::write_i32(&mut root[0x1D8..0x1DC], 1);
        // creation timestamp
        BigEndian::write_i32(&mut root[0x1E4..0x1E8], 1);
        // secType = ST_ROOT
        BigEndian::write_i32(&mut root[0x1FC..0x200], ST_ROOT);
        // Checksum (long 5 = bytes 0x14..0x18)
        let sum = normal_checksum(root, 5);
        BigEndian::write_u32(&mut root[0x14..0x18], sum);

        // Bitmap block at 881: every block free except blocks 880 and 881
        // (root + bitmap themselves are allocated).
        let bm_off = 881 * BSIZE;
        let bm = &mut img[bm_off..bm_off + BSIZE];
        // Initialise map[1..128] to all-free (0xFFFFFFFF in BE), then clear
        // bits for blocks 880 (root) and 881 (this bitmap block).
        for i in 1..128 {
            BigEndian::write_u32(&mut bm[i * 4..i * 4 + 4], 0xFFFFFFFF);
        }
        // bit index for block N = N - 2. bit i is in word (i/32), bit (i%32).
        for blk in [880u32, 881u32] {
            let bit = (blk - 2) as usize;
            let word_idx = bit / 32 + 1; // skip checksum at word 0
            let bit_in_word = bit % 32;
            let mut w = BigEndian::read_u32(&bm[word_idx * 4..word_idx * 4 + 4]);
            w &= !(1u32 << bit_in_word);
            BigEndian::write_u32(&mut bm[word_idx * 4..word_idx * 4 + 4], w);
        }
        // Bitmap checksum at offset 0.
        let sum = bitmap_checksum(bm);
        BigEndian::write_u32(&mut bm[0..4], sum);

        img
    }

    #[test]
    fn open_empty_ffs_floppy() {
        let img = make_empty_floppy_image(1, "TestVolume");
        let mut cur = Cursor::new(img);
        let fs = AffsFilesystem::open(&mut cur, 0).expect("open");
        assert_eq!(fs.variant(), 1);
        assert!(fs.is_ffs());
        assert_eq!(fs.root_block_num(), 880);
        assert_eq!(fs.total_blocks(), 1760);
        assert_eq!(fs.root.disk_name, "TestVolume");
    }

    #[test]
    fn list_empty_root_returns_no_entries() {
        let img = make_empty_floppy_image(0, "Empty");
        let mut cur = Cursor::new(img);
        let mut fs = AffsFilesystem::open(&mut cur, 0).expect("open");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list");
        assert!(entries.is_empty());
    }

    #[test]
    fn compact_reader_emits_zeros_for_free_blocks() {
        let img = make_empty_floppy_image(1, "C");
        let original_size = img.len() as u64;
        let mut cur = Cursor::new(img);
        let (mut reader, info) = CompactAffsReader::new(&mut cur, 0).expect("compact");
        assert_eq!(info.original_size, original_size);
        assert_eq!(info.compacted_size, original_size);

        // Read whole stream and verify: blocks 0/1 (boot) and 880/881 are
        // allocated; everything else is zero. (Boot blocks were not zeroed
        // by our builder either, since we wrote the magic.)
        let mut out = Vec::with_capacity(original_size as usize);
        std::io::copy(&mut reader, &mut out).expect("copy");
        assert_eq!(out.len() as u64, original_size);
        // Block 2 should be all zero (free).
        assert!(out[2 * BSIZE..3 * BSIZE].iter().all(|&b| b == 0));
        // Block 880 should have the root magic — non-zero.
        assert!(out[880 * BSIZE..881 * BSIZE].iter().any(|&b| b != 0));
        // Block 881 should have the bitmap — non-zero.
        assert!(out[881 * BSIZE..882 * BSIZE].iter().any(|&b| b != 0));
    }
}
