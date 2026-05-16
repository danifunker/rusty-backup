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

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use byteorder::{BigEndian, ByteOrder};

use super::affs_common::*;
use super::entry::FileEntry;
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};
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
    /// Bitmap block numbers in order — index `i` of `bitmap_pages` holds
    /// the 127 u32 map words covering bits `[i*4064 .. (i+1)*4064)`.
    bitmap_pages: Vec<u32>,
    /// Total blocks tracked by the bitmap.
    total_blocks: u32,

    /// In-memory cache of blocks that have been mutated but not yet flushed
    /// to disk. `sync_metadata` writes every entry and clears the map; if
    /// any mutation fails before sync, none of the changes ever hit disk.
    /// Reads consult this map before falling back to the underlying reader.
    dirty: HashMap<u32, [u8; BSIZE]>,
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
        let (bitmap, bitmap_pages) = read_bitmap(
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
            bitmap_pages,
            total_blocks,
            dirty: HashMap::new(),
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
        if let Some(buf) = self.dirty.get(&block) {
            return Ok(*buf);
        }
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

    fn write_block_cached(&mut self, block: u32, buf: [u8; BSIZE]) {
        self.dirty.insert(block, buf);
    }

    /// Mark `block` as allocated in the in-memory bitmap and update the
    /// dirty bitmap-page block accordingly. Recomputes the page checksum
    /// before caching.
    fn mark_bitmap(&mut self, block: u32, allocated: bool) -> Result<(), FilesystemError> {
        if block < 2 || block >= self.total_blocks {
            return Err(parse_err(format!(
                "mark_bitmap: block {block} outside the bitmap range"
            )));
        }
        let bit = (block - 2) as usize;
        let byte = bit / 8;
        let off = bit % 8;
        // Update flat bitmap.
        if allocated {
            self.bitmap[byte] &= !(1u8 << off);
        } else {
            self.bitmap[byte] |= 1u8 << off;
        }

        // Find which bitmap page covers this bit and rewrite it. Each page
        // holds 127 u32 map words = 4064 bits.
        let page_idx = bit / 4064;
        if page_idx >= self.bitmap_pages.len() {
            return Err(parse_err(format!(
                "mark_bitmap: no bitmap page covers block {block}"
            )));
        }
        let page_block = self.bitmap_pages[page_idx];
        let bit_in_page = bit - page_idx * 4064;
        let word_idx = bit_in_page / 32; // 0..=126
        let bit_in_word = bit_in_page % 32;

        let mut buf = self.read_block(page_block)?;
        // Word slot (word_idx + 1) — slot 0 is the page checksum.
        let slot = (word_idx + 1) * 4;
        let mut w = read_u32(&buf, slot);
        if allocated {
            w &= !(1u32 << bit_in_word);
        } else {
            w |= 1u32 << bit_in_word;
        }
        buf[slot..slot + 4].copy_from_slice(&w.to_be_bytes());
        // Recompute bitmap-block checksum (offset 0).
        buf[0..4].copy_from_slice(&[0u8; 4]);
        let sum = bitmap_checksum(&buf);
        buf[0..4].copy_from_slice(&sum.to_be_bytes());
        self.write_block_cached(page_block, buf);
        Ok(())
    }

    /// Find the lowest-numbered free block and mark it allocated, returning
    /// its block number. Reserves blocks 0/1 (boot) and never returns the
    /// root or any current bitmap page.
    fn alloc_block(&mut self) -> Result<u32, FilesystemError> {
        for block in 2..self.total_blocks {
            // Skip blocks that are already allocated.
            if self.block_is_allocated(block) {
                continue;
            }
            // Mark and return.
            self.mark_bitmap(block, true)?;
            // Zero the new block in the cache so callers can synth content
            // from a known state.
            self.write_block_cached(block, [0u8; BSIZE]);
            return Ok(block);
        }
        Err(FilesystemError::DiskFull(
            "AFFS: no free blocks available".into(),
        ))
    }

    fn free_block(&mut self, block: u32) -> Result<(), FilesystemError> {
        self.mark_bitmap(block, false)?;
        Ok(())
    }

    /// Returns the AmigaDOS DateStamp triplet for the current wall-clock time.
    fn now_datestamp() -> (i32, i32, i32) {
        let dur = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let unix = dur.as_secs() as i64;
        // 1978-01-01 epoch:
        let amiga_secs = unix - 2922 * 86_400;
        if amiga_secs <= 0 {
            return (0, 0, 0);
        }
        let days = (amiga_secs / 86_400) as i32;
        let tod = amiga_secs % 86_400;
        let mins = (tod / 60) as i32;
        let secs_in_min = (tod % 60) as i32;
        let ticks = secs_in_min * 50; // 50 Hz PAL
        (days, mins, ticks)
    }

    /// Synthesize a new directory header block in the cache.
    fn build_dir_block(
        &mut self,
        block: u32,
        parent: u32,
        name: &str,
    ) -> Result<(), FilesystemError> {
        let mut buf = [0u8; BSIZE];
        buf[0..4].copy_from_slice(&T_HEADER.to_be_bytes());
        buf[4..8].copy_from_slice(&block.to_be_bytes());
        // bytes 8..0x14 are the rest of the header prefix — left zero.
        // Hash table at 0x18..0x138 — already zero from buf initialization.
        let (days, mins, ticks) = Self::now_datestamp();
        buf[0x1A4..0x1A8].copy_from_slice(&days.to_be_bytes());
        buf[0x1A8..0x1AC].copy_from_slice(&mins.to_be_bytes());
        buf[0x1AC..0x1B0].copy_from_slice(&ticks.to_be_bytes());
        write_bstr(&mut buf, 0x1B0, MAX_NAME_LEN, name)?;
        buf[0x1F4..0x1F8].copy_from_slice(&parent.to_be_bytes());
        // extension = 0 (no DirCache linked) at 0x1F8 — already zero.
        buf[0x1FC..0x200].copy_from_slice(&ST_DIR.to_be_bytes());
        let sum = normal_checksum(&buf, 5);
        buf[0x14..0x18].copy_from_slice(&sum.to_be_bytes());
        self.write_block_cached(block, buf);
        Ok(())
    }

    /// Synthesize a new file header block in the cache.
    ///
    /// `access` is the AmigaDOS protection word stored at offset 0x140
    /// (0 = `----rwed`, matching the on-disk default). `comment` is the
    /// optional Amiga filenote stored at offset 0x148; pass `""` to
    /// leave it empty.
    #[allow(clippy::too_many_arguments)]
    fn build_file_header_block(
        &mut self,
        block: u32,
        parent: u32,
        name: &str,
        byte_size: u32,
        data_blocks_first_72: &[u32],
        first_data: u32,
        extension: u32,
        access: u32,
        comment: &str,
    ) -> Result<(), FilesystemError> {
        let mut buf = [0u8; BSIZE];
        buf[0..4].copy_from_slice(&T_HEADER.to_be_bytes());
        buf[4..8].copy_from_slice(&block.to_be_bytes());
        // high_seq = number of data block pointers actually stored in this
        // header (max 72).
        let high_seq = data_blocks_first_72.len() as u32;
        buf[8..12].copy_from_slice(&high_seq.to_be_bytes());
        // data_size — kept zero per AmigaDOS convention; byte_size at 0x144
        // is the authoritative size.
        buf[0x10..0x14].copy_from_slice(&first_data.to_be_bytes());
        // dataBlocks[72] in REVERSE order: dataBlocks[71] = first data block.
        for (i, &dblk) in data_blocks_first_72.iter().enumerate() {
            let slot = MAX_DATA_BLOCKS - 1 - i;
            buf[0x18 + slot * 4..0x18 + slot * 4 + 4].copy_from_slice(&dblk.to_be_bytes());
        }
        // protection (access) at 0x140
        buf[0x140..0x144].copy_from_slice(&access.to_be_bytes());
        // byte_size at 0x144.
        buf[0x144..0x148].copy_from_slice(&byte_size.to_be_bytes());
        // comment BSTR at 0x148 (length + up to MAX_COMMENT_LEN bytes).
        // Skip the encoder for empty comments — the buffer is already
        // zeroed (length=0) so the on-disk BSTR is the correct empty
        // form, and `write_bstr` rejects empty strings on the grounds
        // that it's primarily a filename encoder.
        if !comment.is_empty() {
            write_bstr(&mut buf, 0x148, MAX_COMMENT_LEN, comment)?;
        }
        let (days, mins, ticks) = Self::now_datestamp();
        buf[0x1A4..0x1A8].copy_from_slice(&days.to_be_bytes());
        buf[0x1A8..0x1AC].copy_from_slice(&mins.to_be_bytes());
        buf[0x1AC..0x1B0].copy_from_slice(&ticks.to_be_bytes());
        write_bstr(&mut buf, 0x1B0, MAX_NAME_LEN, name)?;
        buf[0x1F4..0x1F8].copy_from_slice(&parent.to_be_bytes());
        buf[0x1F8..0x1FC].copy_from_slice(&extension.to_be_bytes());
        buf[0x1FC..0x200].copy_from_slice(&ST_FILE.to_be_bytes());
        let sum = normal_checksum(&buf, 5);
        buf[0x14..0x18].copy_from_slice(&sum.to_be_bytes());
        self.write_block_cached(block, buf);
        Ok(())
    }

    /// Synthesize a file-extension block (T_LIST) listing the next batch
    /// of data block pointers.
    fn build_file_ext_block(
        &mut self,
        block: u32,
        parent: u32,
        data_blocks: &[u32],
        next_extension: u32,
    ) -> Result<(), FilesystemError> {
        let mut buf = [0u8; BSIZE];
        buf[0..4].copy_from_slice(&T_LIST.to_be_bytes());
        buf[4..8].copy_from_slice(&block.to_be_bytes());
        let high_seq = data_blocks.len() as u32;
        buf[8..12].copy_from_slice(&high_seq.to_be_bytes());
        for (i, &dblk) in data_blocks.iter().enumerate() {
            let slot = MAX_DATA_BLOCKS - 1 - i;
            buf[0x18 + slot * 4..0x18 + slot * 4 + 4].copy_from_slice(&dblk.to_be_bytes());
        }
        buf[0x1F4..0x1F8].copy_from_slice(&parent.to_be_bytes());
        buf[0x1F8..0x1FC].copy_from_slice(&next_extension.to_be_bytes());
        buf[0x1FC..0x200].copy_from_slice(&ST_FILE.to_be_bytes());
        let sum = normal_checksum(&buf, 5);
        buf[0x14..0x18].copy_from_slice(&sum.to_be_bytes());
        self.write_block_cached(block, buf);
        Ok(())
    }

    /// Write a payload byte slice as either an OFS data block (24-byte header
    /// + up to 488 payload bytes) or an FFS data block (raw 512 bytes).
    fn build_data_block(
        &mut self,
        block: u32,
        file_header: u32,
        seq_num: u32,
        payload: &[u8],
        next_data: u32,
    ) -> Result<(), FilesystemError> {
        let mut buf = [0u8; BSIZE];
        if self.ffs {
            let n = payload.len().min(BSIZE);
            buf[..n].copy_from_slice(&payload[..n]);
        } else {
            // OFS header
            buf[0..4].copy_from_slice(&T_DATA.to_be_bytes());
            buf[4..8].copy_from_slice(&file_header.to_be_bytes());
            buf[8..12].copy_from_slice(&seq_num.to_be_bytes());
            let n = payload.len().min(488);
            buf[12..16].copy_from_slice(&(n as u32).to_be_bytes());
            buf[16..20].copy_from_slice(&next_data.to_be_bytes());
            buf[24..24 + n].copy_from_slice(&payload[..n]);
            // Checksum at 0x14 (long index 5).
            let sum = normal_checksum(&buf, 5);
            buf[0x14..0x18].copy_from_slice(&sum.to_be_bytes());
        }
        self.write_block_cached(block, buf);
        Ok(())
    }

    /// Insert `entry_block` into the hash chain of `parent_dir_block` for
    /// the given name. `parent_dir_block` must be ST_ROOT or ST_DIR.
    fn hash_chain_insert(
        &mut self,
        parent_dir_block: u32,
        entry_block: u32,
        name: &str,
    ) -> Result<(), FilesystemError> {
        let hash = name_hash(name.as_bytes(), self.intl) as usize;
        let slot_byte = 0x18 + hash * 4;
        let mut parent = self.read_block(parent_dir_block)?;
        let prev_head = read_u32(&parent, slot_byte);

        // Update entry's nextSameHash → previous head; parent dir's hash
        // slot → entry block.
        let mut entry = self.read_block(entry_block)?;
        entry[0x1F0..0x1F4].copy_from_slice(&prev_head.to_be_bytes());
        // Recompute checksum (offset 0x14).
        entry[0x14..0x18].copy_from_slice(&[0u8; 4]);
        let sum = normal_checksum(&entry, 5);
        entry[0x14..0x18].copy_from_slice(&sum.to_be_bytes());
        self.write_block_cached(entry_block, entry);

        parent[slot_byte..slot_byte + 4].copy_from_slice(&entry_block.to_be_bytes());
        parent[0x14..0x18].copy_from_slice(&[0u8; 4]);
        let sum = normal_checksum(&parent, 5);
        parent[0x14..0x18].copy_from_slice(&sum.to_be_bytes());
        self.write_block_cached(parent_dir_block, parent);

        // Keep the cached `self.root` in sync if the parent is the root.
        if parent_dir_block == self.root_block {
            self.root.hash_table[hash] = entry_block;
        }
        Ok(())
    }

    /// Remove `entry_block` from `parent_dir_block`'s hash chain.
    fn hash_chain_remove(
        &mut self,
        parent_dir_block: u32,
        entry_block: u32,
        name: &str,
    ) -> Result<(), FilesystemError> {
        let hash = name_hash(name.as_bytes(), self.intl) as usize;
        let slot_byte = 0x18 + hash * 4;
        let entry = self.read_block(entry_block)?;
        let entry_next = read_u32(&entry, 0x1F0);

        let mut parent = self.read_block(parent_dir_block)?;
        let head = read_u32(&parent, slot_byte);

        if head == entry_block {
            parent[slot_byte..slot_byte + 4].copy_from_slice(&entry_next.to_be_bytes());
            parent[0x14..0x18].copy_from_slice(&[0u8; 4]);
            let sum = normal_checksum(&parent, 5);
            parent[0x14..0x18].copy_from_slice(&sum.to_be_bytes());
            self.write_block_cached(parent_dir_block, parent);
            if parent_dir_block == self.root_block {
                self.root.hash_table[hash] = entry_next;
            }
            return Ok(());
        }

        // Walk chain to find predecessor.
        let mut prev = head;
        loop {
            if prev == 0 {
                return Err(FilesystemError::NotFound(format!(
                    "hash_chain_remove: entry {entry_block} not in chain of {parent_dir_block}"
                )));
            }
            let prev_buf = self.read_block(prev)?;
            let next = read_u32(&prev_buf, 0x1F0);
            if next == entry_block {
                let mut updated = prev_buf;
                updated[0x1F0..0x1F4].copy_from_slice(&entry_next.to_be_bytes());
                updated[0x14..0x18].copy_from_slice(&[0u8; 4]);
                let sum = normal_checksum(&updated, 5);
                updated[0x14..0x18].copy_from_slice(&sum.to_be_bytes());
                self.write_block_cached(prev, updated);
                return Ok(());
            }
            prev = next;
        }
    }

    /// True if the directory at `dir_block` has at least one child.
    fn dir_is_empty(&mut self, dir_block: u32) -> Result<bool, FilesystemError> {
        let buf = self.read_block(dir_block)?;
        for slot in 0..HT_SIZE {
            let v = read_u32(&buf, 0x18 + slot * 4);
            if v != 0 {
                return Ok(false);
            }
        }
        Ok(true)
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
        fe.amiga_protection = Some(entry.access);
        if !entry.comment.is_empty() {
            fe.amiga_comment = Some(entry.comment.clone());
        }
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

    /// True if `block` is a bitmap page (root inline or extension).
    /// Used by the fsck rebuild to ensure bitmap blocks remain marked
    /// allocated even though they're never reached via directory walks.
    pub fn bitmap_page_owns_block(&self, block: u32) -> bool {
        self.bitmap_pages.iter().any(|&b| b == block)
    }

    /// Public accessor — same semantics as the private `block_is_allocated`.
    pub fn block_is_allocated_public(&self, block: u32) -> bool {
        self.block_is_allocated(block)
    }

    /// Read-only owned copy of the volume label, if set.
    pub fn volume_label_owned(&self) -> Option<String> {
        if self.root.disk_name.is_empty() {
            None
        } else {
            Some(self.root.disk_name.clone())
        }
    }

    /// Short label for the FS variant ("FFS", "OFS", "FFS Intl", …).
    pub fn fs_type_label(&self) -> &'static str {
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

    /// Root block modify-date `(days, mins, ticks)` triplet.
    pub fn root_modify_datestamp(&self) -> (i32, i32, i32) {
        (
            self.root.modify_days,
            self.root.modify_mins,
            self.root.modify_ticks,
        )
    }

    /// Parse the directory/root block at `block` and return its hash table.
    /// Used by fsck's BFS walk; surfaces parse errors so the validator can
    /// flag corruption rather than crashing the walk.
    pub fn peek_directory_hash_table(&mut self, block: u32) -> Result<Vec<u32>, FilesystemError> {
        if block == self.root_block {
            return Ok(self.root.hash_table.clone());
        }
        let entry = self.read_entry(block)?;
        if entry.hash_table.is_empty() {
            return Err(parse_err(format!(
                "block {block}: expected a directory hash table but secType is {}",
                entry.sec_type
            )));
        }
        Ok(entry.hash_table)
    }

    /// Parse the entry block at `block` for fsck inspection.
    pub fn peek_entry_block(&mut self, block: u32) -> Result<AffsEntry, FilesystemError> {
        self.read_entry(block)
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

/// Encode `name` as an AFFS BSTR at byte offset `offset` of `buf`, with
/// length capped at `max`. Returns an error if the name is empty or
/// contains a forbidden byte (NUL, '/', ':').
fn write_bstr(
    buf: &mut [u8; BSIZE],
    offset: usize,
    max: usize,
    name: &str,
) -> Result<(), FilesystemError> {
    if name.is_empty() {
        return Err(FilesystemError::InvalidData("name cannot be empty".into()));
    }
    let bytes = name.as_bytes();
    if bytes.len() > max {
        return Err(FilesystemError::InvalidData(format!(
            "AFFS name '{name}' exceeds {max} bytes"
        )));
    }
    for &b in bytes {
        if b == 0 || b == b'/' || b == b':' {
            return Err(FilesystemError::InvalidData(format!(
                "AFFS name '{name}' contains forbidden character"
            )));
        }
    }
    buf[offset] = bytes.len() as u8;
    buf[offset + 1..offset + 1 + bytes.len()].copy_from_slice(bytes);
    Ok(())
}

fn read_bitmap<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    block_size: u64,
    root: &AffsRootBlock,
    total_blocks: u32,
) -> Result<(Vec<u8>, Vec<u32>), FilesystemError> {
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
    Ok((bitmap, pages))
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

    fn validate_name(&self, name: &str) -> Result<(), FilesystemError> {
        if name.is_empty() {
            return Err(FilesystemError::InvalidData(
                "AFFS name cannot be empty".into(),
            ));
        }
        if name.as_bytes().len() > MAX_NAME_LEN {
            return Err(FilesystemError::InvalidData(format!(
                "AFFS name '{name}' exceeds {MAX_NAME_LEN} bytes (Long Names not yet supported for write)"
            )));
        }
        for b in name.as_bytes() {
            if *b == 0 || *b == b'/' || *b == b':' {
                return Err(FilesystemError::InvalidData(format!(
                    "AFFS name '{name}' contains forbidden character"
                )));
            }
        }
        Ok(())
    }

    fn fsck(&mut self) -> Option<Result<super::fsck::FsckResult, FilesystemError>> {
        Some(super::affs_fsck::check_affs(self))
    }
}

impl<R: Read + Write + Seek + Send> EditableFilesystem for AffsFilesystem<R> {
    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn Read,
        data_len: u64,
        options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        self.validate_name(name)?;
        let parent_block = if parent.location == 0 {
            self.root_block
        } else {
            parent.location as u32
        };
        // Reject duplicates.
        let existing = self.lookup_in_dir(parent_block, name)?;
        if existing.is_some() {
            return Err(FilesystemError::AlreadyExists(name.to_string()));
        }

        // Allocate the file header block first.
        let header_block = self.alloc_block()?;

        // Per-data-block payload size depends on variant.
        let payload_per_block = if self.ffs { BSIZE as u64 } else { 488 };
        let num_data_blocks = if data_len == 0 {
            0
        } else {
            data_len.div_ceil(payload_per_block) as u32
        };

        // Read all payload up-front so we know we have enough disk for the
        // whole file before allocating data blocks. (For very large files
        // we'd want a streaming approach; AFFS volumes are at most ~2 GiB
        // so loading the whole file is acceptable for v1.)
        let mut payload = Vec::with_capacity(data_len as usize);
        if data_len > 0 {
            // Reading exactly `data_len` bytes; if the source provides fewer,
            // surface a clear error rather than silently truncating.
            let mut taker = data.take(data_len);
            taker
                .read_to_end(&mut payload)
                .map_err(FilesystemError::Io)?;
            if (payload.len() as u64) != data_len {
                self.free_block(header_block)?;
                return Err(FilesystemError::InvalidData(format!(
                    "create_file: source produced {} bytes, expected {}",
                    payload.len(),
                    data_len
                )));
            }
        }

        // Allocate every data block.
        let mut data_blocks: Vec<u32> = Vec::with_capacity(num_data_blocks as usize);
        for _ in 0..num_data_blocks {
            match self.alloc_block() {
                Ok(b) => data_blocks.push(b),
                Err(e) => {
                    // Roll back partial allocation.
                    for &b in &data_blocks {
                        let _ = self.free_block(b);
                    }
                    let _ = self.free_block(header_block);
                    return Err(e);
                }
            }
        }

        // Write data blocks.
        let mut payload_offset = 0usize;
        for (i, &block) in data_blocks.iter().enumerate() {
            let chunk_end = (payload_offset + payload_per_block as usize).min(payload.len());
            let chunk = &payload[payload_offset..chunk_end];
            let seq = (i as u32) + 1;
            let next_data = data_blocks.get(i + 1).copied().unwrap_or(0);
            self.build_data_block(block, header_block, seq, chunk, next_data)?;
            payload_offset = chunk_end;
        }

        // For files with > MAX_DATA_BLOCKS data blocks, build the chain of
        // file-extension blocks. The first MAX_DATA_BLOCKS pointers live in
        // the header; each extension holds MAX_DATA_BLOCKS more.
        let inline_count = data_blocks.len().min(MAX_DATA_BLOCKS);
        let header_data_blocks = &data_blocks[..inline_count];
        let remainder = &data_blocks[inline_count..];

        // Allocate extension blocks up front so we know where to chain.
        let ext_count = remainder.chunks(MAX_DATA_BLOCKS).count();
        let mut ext_blocks: Vec<u32> = Vec::with_capacity(ext_count);
        for _ in 0..ext_count {
            match self.alloc_block() {
                Ok(b) => ext_blocks.push(b),
                Err(e) => {
                    for &b in &ext_blocks {
                        let _ = self.free_block(b);
                    }
                    for &b in &data_blocks {
                        let _ = self.free_block(b);
                    }
                    let _ = self.free_block(header_block);
                    return Err(e);
                }
            }
        }
        let header_extension = ext_blocks.first().copied().unwrap_or(0);

        for (idx, chunk) in remainder.chunks(MAX_DATA_BLOCKS).enumerate() {
            let this_block = ext_blocks[idx];
            let next_ext = ext_blocks.get(idx + 1).copied().unwrap_or(0);
            self.build_file_ext_block(this_block, header_block, chunk, next_ext)?;
        }

        let first_data = data_blocks.first().copied().unwrap_or(0);
        let access = options.amiga_protection.unwrap_or(0);
        let comment_str = options.amiga_comment.as_deref().unwrap_or("");
        self.build_file_header_block(
            header_block,
            parent_block,
            name,
            data_len as u32,
            header_data_blocks,
            first_data,
            header_extension,
            access,
            comment_str,
        )?;
        self.hash_chain_insert(parent_block, header_block, name)?;

        // Build the returned FileEntry.
        let path = if parent.path == "/" {
            format!("/{name}")
        } else {
            format!("{}/{}", parent.path, name)
        };
        Ok(FileEntry::new_file(
            name.to_string(),
            path,
            data_len,
            header_block as u64,
        ))
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        self.validate_name(name)?;
        let parent_block = if parent.location == 0 {
            self.root_block
        } else {
            parent.location as u32
        };
        let existing = self.lookup_in_dir(parent_block, name)?;
        if existing.is_some() {
            return Err(FilesystemError::AlreadyExists(name.to_string()));
        }
        let block = self.alloc_block()?;
        self.build_dir_block(block, parent_block, name)?;
        self.hash_chain_insert(parent_block, block, name)?;
        let path = if parent.path == "/" {
            format!("/{name}")
        } else {
            format!("{}/{}", parent.path, name)
        };
        Ok(FileEntry::new_directory(
            name.to_string(),
            path,
            block as u64,
        ))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        let parent_block = if parent.location == 0 {
            self.root_block
        } else {
            parent.location as u32
        };
        let block = entry.location as u32;
        let parsed = self.read_entry(block)?;

        match parsed.sec_type {
            ST_DIR => {
                if !self.dir_is_empty(block)? {
                    return Err(FilesystemError::InvalidData(format!(
                        "directory '{}' is not empty",
                        entry.name
                    )));
                }
                self.hash_chain_remove(parent_block, block, &entry.name)?;
                self.free_block(block)?;
            }
            ST_FILE => {
                // Walk data blocks (header + all extensions) and free.
                let mut cur_data = parsed.data_blocks.clone();
                let mut next_ext = parsed.extension;
                loop {
                    for &dblk in &cur_data {
                        if dblk != 0 {
                            self.free_block(dblk)?;
                        }
                    }
                    if next_ext == 0 {
                        break;
                    }
                    let ext = self.read_entry(next_ext)?;
                    let this_ext = next_ext;
                    cur_data = ext.data_blocks.clone();
                    next_ext = ext.extension;
                    self.free_block(this_ext)?;
                }
                self.hash_chain_remove(parent_block, block, &entry.name)?;
                self.free_block(block)?;
            }
            _ => {
                return Err(FilesystemError::Unsupported(format!(
                    "deleting AFFS entry with secType {} is not supported",
                    parsed.sec_type
                )));
            }
        }
        Ok(())
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        // Write every dirty block to disk in block-number order — gives the
        // OS a clean forward sweep for the file system cache and makes the
        // worst case (a half-flushed sync) the most innocuous: bitmap
        // pages and the root come last.
        let mut keys: Vec<u32> = self.dirty.keys().copied().collect();
        keys.sort_unstable();
        for block in keys {
            let buf = self.dirty.get(&block).copied().unwrap();
            let off = self.partition_offset + block as u64 * self.block_size;
            self.reader.seek(SeekFrom::Start(off))?;
            self.reader.write_all(&buf).map_err(FilesystemError::Io)?;
        }
        self.reader.flush().map_err(FilesystemError::Io)?;
        self.dirty.clear();
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        let mut free = 0u64;
        for block in 2..self.total_blocks {
            if !self.block_is_allocated(block) {
                free += self.block_size;
            }
        }
        Ok(free)
    }

    fn repair(&mut self) -> Result<super::fsck::RepairReport, FilesystemError> {
        super::affs_fsck::repair_affs(self)
    }
}

impl<R: Read + Write + Seek> AffsFilesystem<R> {
    /// Rebuild every bitmap page from a flat `observed` byte array where
    /// bit `i` set = block `(2 + i)` is allocated. Updates `self.bitmap`
    /// in-memory and re-stages each bitmap page in the dirty cache with
    /// a fresh checksum. Caller must `flush_metadata`/`sync_metadata` after.
    pub fn rewrite_bitmap_from(&mut self, observed: &[u8]) -> Result<(), FilesystemError> {
        let total = self.total_blocks;
        let bits = total.saturating_sub(2) as usize;
        // Update the flat in-memory bitmap to match `observed`, converting
        // from "set = allocated" (observed convention) to "set = free"
        // (on-disk AmigaDOS convention).
        for byte_idx in 0..self.bitmap.len() {
            self.bitmap[byte_idx] = 0;
        }
        for bit in 0..bits {
            let alloc = (observed[bit / 8] >> (bit % 8)) & 1 != 0;
            if !alloc {
                self.bitmap[bit / 8] |= 1u8 << (bit % 8);
            }
        }

        // Rewrite each bitmap page from scratch.
        for (page_idx, &page_block) in self.bitmap_pages.clone().iter().enumerate() {
            let mut buf = [0u8; BSIZE];
            // 127 map words follow the checksum at slot 0.
            for word_idx in 0..127 {
                let mut w: u32 = 0;
                for bit in 0..32 {
                    let global = page_idx * 4064 + word_idx * 32 + bit;
                    if global >= bits {
                        break;
                    }
                    // On-disk: set bit = free. We flip the observed
                    // allocation bit accordingly.
                    let alloc = (observed[global / 8] >> (global % 8)) & 1 != 0;
                    if !alloc {
                        w |= 1u32 << bit;
                    }
                }
                let slot = (word_idx + 1) * 4;
                buf[slot..slot + 4].copy_from_slice(&w.to_be_bytes());
            }
            let sum = bitmap_checksum(&buf);
            buf[0..4].copy_from_slice(&sum.to_be_bytes());
            self.write_block_cached(page_block, buf);
        }
        Ok(())
    }

    /// Convenience: flush every dirty block to disk and clear the cache.
    /// Same effect as `sync_metadata` but available without the
    /// `EditableFilesystem` trait import at the call site.
    pub fn flush_metadata(&mut self) -> Result<(), FilesystemError> {
        let mut keys: Vec<u32> = self.dirty.keys().copied().collect();
        keys.sort_unstable();
        for block in keys {
            let buf = self.dirty.get(&block).copied().unwrap();
            let off = self.partition_offset + block as u64 * self.block_size;
            self.reader.seek(SeekFrom::Start(off))?;
            self.reader.write_all(&buf).map_err(FilesystemError::Io)?;
        }
        self.reader.flush().map_err(FilesystemError::Io)?;
        self.dirty.clear();
        Ok(())
    }
}

impl<R: Read + Seek> AffsFilesystem<R> {
    /// Find a child of `dir_block` matching `name` (case-insensitive per
    /// the active variant's folding). Returns `None` when absent.
    fn lookup_in_dir(
        &mut self,
        dir_block: u32,
        name: &str,
    ) -> Result<Option<u32>, FilesystemError> {
        let hash = name_hash(name.as_bytes(), self.intl) as usize;
        let dir_buf = if dir_block == self.root_block {
            // Use the cached root hash table to avoid re-parsing the block.
            None
        } else {
            Some(self.read_block(dir_block)?)
        };
        let mut chain = match &dir_buf {
            Some(buf) => read_u32(buf, 0x18 + hash * 4),
            None => self.root.hash_table[hash],
        };
        while chain != 0 {
            let entry = self.read_entry(chain)?;
            if names_equal(&entry.name, name, self.intl) {
                return Ok(Some(chain));
            }
            chain = entry.next_same_hash;
        }
        Ok(None)
    }
}

/// Case-insensitive name comparison using the same case folding as the
/// hash function. Always operates byte-for-byte on the ISO-8859-1 encoding.
fn names_equal(a: &str, b: &str, intl: bool) -> bool {
    let ab = a.as_bytes();
    let bb = b.as_bytes();
    if ab.len() != bb.len() {
        return false;
    }
    for i in 0..ab.len() {
        let ua = fold(ab[i], intl);
        let ub = fold(bb[i], intl);
        if ua != ub {
            return false;
        }
    }
    true
}

fn fold(b: u8, intl: bool) -> u8 {
    if (b'a'..=b'z').contains(&b) {
        return b - 0x20;
    }
    if intl && (0xE0..=0xFE).contains(&b) && b != 0xF7 {
        return b - 0x20;
    }
    b
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

// === AFFS in-place resize ===================================================
//
// Unlike FAT / NTFS / HFS / PFS3 / SFS — all of which keep their primary
// metadata block at a fixed offset and only need a field update on
// resize — AFFS computes the root-block position as
// `(2 + total_blocks - 1) / 2`. Resizing therefore requires physically
// moving the root block (and its adjacent bitmap pages) to a new
// position derived from the new total. Plus the bitmap itself has to
// be rebuilt to reflect the new layout.
//
// Scope of this implementation:
// - Volumes whose bitmap fits in `root.bm_pages[..]` (up to 25 pages =
//   ~50 MB at 512-byte blocks). Volumes that use the `root.bm_ext`
//   chain are refused with a clear error so we don't silently corrupt
//   anything.
// - Both shrink and grow paths, both with the same set of safety
//   guards: every currently-allocated user-data block must still fit
//   in the new volume AND must not collide with any of the new
//   metadata positions.
//
// The function is a silent no-op on non-AFFS volumes, matching every
// other `resize_*_in_place` in `resize_filesystem_for`.

const AFFS_BITS_PER_BM_PAGE: u32 = 127 * 32; // 4064
const AFFS_BM_PAGES_ROOT: u32 = 25;

/// Resize an AFFS volume at `partition_offset` to `new_size_bytes`. See
/// the module-level resize comment for the scope and safety contract.
pub fn resize_affs_in_place(
    device: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    new_size_bytes: u64,
    log: &mut impl FnMut(&str),
) -> anyhow::Result<()> {
    // --- 1. Probe boot block for the DOS magic. ---
    device.seek(SeekFrom::Start(partition_offset))?;
    let mut boot = [0u8; 4];
    if device.read_exact(&mut boot).is_err() {
        return Ok(());
    }
    if &boot[0..3] != b"DOS" {
        return Ok(());
    }
    if boot[3] > 7 {
        log(&format!(
            "AFFS resize: unknown DosType variant {}, skipping",
            boot[3]
        ));
        return Ok(());
    }

    // --- 2. Locate the current root block. ---
    //
    // The on-disk layout doesn't store `total_blocks`; AFFS computes the
    // root location from the partition extent at mount time. That means
    // we can't trust the EOF-based formula here: by the time
    // `resize_filesystem_for` runs, the partition may already be at the
    // NEW size (e.g. on a fresh restore), so `(2 + EOF_total - 1) / 2`
    // points at the wrong sector. Instead, try the EOF formula first
    // (handles the common case where the medium hasn't been resized
    // yet), then fall back to a bounded scan for an ST_ROOT block whose
    // header-key matches its own position.
    let upper_eof = device.seek(SeekFrom::End(0))?;
    let upper_partition_size = upper_eof.saturating_sub(partition_offset);
    if upper_partition_size < (4 * BSIZE) as u64 {
        log("AFFS resize: partition too small, skipping");
        return Ok(());
    }
    let upper_total = (upper_partition_size / BSIZE_U64) as u32;
    let old_root = match find_affs_root_block(device, partition_offset, upper_total)? {
        Some(blk) => blk,
        None => {
            log("AFFS resize: no ST_ROOT block found, skipping");
            return Ok(());
        }
    };
    let old_total = old_root
        .checked_mul(2)
        .and_then(|n| n.checked_sub(1))
        .ok_or_else(|| anyhow::anyhow!("AFFS resize: old_root {} overflows", old_root))?;
    // root = (2 + total - 1) / 2 = (total + 1) / 2 — integer floor division.
    // So total ∈ {2*root - 1, 2*root}. Disambiguate by checking which fits
    // in the medium and roundtrips back to the same root.
    let candidate_a = 2 * old_root - 1;
    let candidate_b = 2 * old_root;
    let old_total = if (2 + candidate_b - 1) / 2 == old_root && candidate_b <= upper_total {
        candidate_b
    } else if (2 + candidate_a - 1) / 2 == old_root {
        candidate_a
    } else {
        old_total // candidate_a from earlier computation
    };

    let new_total_u64 = new_size_bytes / BSIZE_U64;
    if new_total_u64 < 4 {
        anyhow::bail!(
            "AFFS resize: target {} bytes ({} sectors) is below the minimum",
            new_size_bytes,
            new_total_u64,
        );
    }
    if new_total_u64 > u32::MAX as u64 {
        anyhow::bail!(
            "AFFS resize: target {} bytes resolves to {} sectors, out of u32 range",
            new_size_bytes,
            new_total_u64,
        );
    }
    let new_total = new_total_u64 as u32;
    let new_root = (2 + new_total - 1) / 2;

    if new_total == old_total {
        log("AFFS resize: no size change, skipping");
        return Ok(());
    }

    // --- 3. Read the old root block. ---
    let mut old_root_buf = [0u8; BSIZE];
    device.seek(SeekFrom::Start(
        partition_offset + old_root as u64 * BSIZE_U64,
    ))?;
    device.read_exact(&mut old_root_buf)?;
    let old_root_parsed = AffsRootBlock::parse(&old_root_buf, old_root)
        .map_err(|e| anyhow::anyhow!("AFFS resize: root parse failed: {e}"))?;

    // We don't support bm_ext chains here (see module comment).
    if old_root_parsed.bm_ext != 0 {
        anyhow::bail!(
            "AFFS resize: volume uses a bitmap-extension chain (bm_ext = {}), \
             which is not yet supported by the in-place resize.",
            old_root_parsed.bm_ext,
        );
    }

    let old_pages: Vec<u32> = old_root_parsed
        .bm_pages
        .iter()
        .copied()
        .take_while(|&p| p != 0)
        .collect();
    let new_pages_needed = affs_bm_pages_needed(new_total);
    if new_pages_needed > AFFS_BM_PAGES_ROOT as usize {
        anyhow::bail!(
            "AFFS resize: target {} sectors needs {} bitmap pages, but only {} \
             fit in the root block (bm_ext chain growth not yet implemented).",
            new_total,
            new_pages_needed,
            AFFS_BM_PAGES_ROOT,
        );
    }
    let new_pages: Vec<u32> = (0..new_pages_needed as u32)
        .map(|i| new_root + 1 + i)
        .collect();

    // --- 4. Reconstruct the old logical bitmap (set bit = free). ---
    let (bitmap, _) = read_bitmap(
        device,
        partition_offset,
        BSIZE_U64,
        &old_root_parsed,
        old_total,
    )
    .map_err(|e| anyhow::anyhow!("AFFS resize: reading bitmap failed: {e}"))?;

    // `bitmap` is `(old_total - 2 + 7) / 8` bytes, where bit i covers block
    // (i + 2). Build a helper closure for is-allocated.
    let bit_set = |bm: &[u8], blk: u32| -> bool {
        if blk < 2 {
            return true;
        }
        let bit = (blk - 2) as usize;
        let byte = bit / 8;
        let off = bit % 8;
        if byte >= bm.len() {
            return false;
        }
        (bm[byte] >> off) & 1 != 0
    };
    let is_allocated = |bm: &[u8], blk: u32| -> bool {
        if blk < 2 {
            return true;
        }
        // Old metadata (root + bm_pages) is also marked allocated. We
        // treat those specially during the move check.
        !bit_set(bm, blk)
    };

    // --- 5. Walk every old-bitmap entry, accumulate user-data blocks
    //        (every allocated block that isn't old_root or an old bm_page),
    //        check the safety contract. ---
    let old_metadata: std::collections::HashSet<u32> = std::iter::once(old_root)
        .chain(old_pages.iter().copied())
        .collect();
    let new_metadata: std::collections::HashSet<u32> = std::iter::once(new_root)
        .chain(new_pages.iter().copied())
        .collect();

    let mut user_blocks: Vec<u32> = Vec::new();
    for blk in 2..old_total {
        if !is_allocated(&bitmap, blk) {
            continue;
        }
        if old_metadata.contains(&blk) {
            continue;
        }
        user_blocks.push(blk);
    }

    for &blk in &user_blocks {
        if blk >= new_total {
            anyhow::bail!(
                "AFFS resize: allocated block {} lies past the target tail \
                 ({} sectors). Shrink to at least {} sectors to keep this \
                 data, or pick a larger target.",
                blk,
                new_total,
                blk + 1,
            );
        }
        if new_metadata.contains(&blk) {
            anyhow::bail!(
                "AFFS resize: allocated user block {} collides with the new \
                 root/bitmap position. Move or delete the file occupying \
                 that block, or pick a different target size.",
                blk,
            );
        }
    }

    // --- 6. Build the new flat bitmap. Bits cover [2..new_total). Start
    //        with everything free, then mark user blocks + new metadata as
    //        allocated. ---
    let new_bits = new_total.saturating_sub(2) as usize;
    let new_bitmap_bytes = new_bits.div_ceil(8);
    let mut new_bm = vec![0xFFu8; new_bitmap_bytes];
    // Trim padding bits past `new_bits` so they don't leak into the
    // written bitmap pages.
    if new_bits % 8 != 0 {
        let mask = (1u8 << (new_bits % 8)) - 1;
        if let Some(last) = new_bm.last_mut() {
            *last &= mask;
        }
    }
    let clear_bit = |bm: &mut [u8], blk: u32| {
        if blk < 2 {
            return;
        }
        let bit = (blk - 2) as usize;
        if bit / 8 >= bm.len() {
            return;
        }
        bm[bit / 8] &= !(1u8 << (bit % 8));
    };
    for &blk in &user_blocks {
        clear_bit(&mut new_bm, blk);
    }
    clear_bit(&mut new_bm, new_root);
    for &p in &new_pages {
        clear_bit(&mut new_bm, p);
    }

    // --- 7. Build the new root block buffer. Start from the old root,
    //        update bm_pages[] + headerKey, rewrite checksum. ---
    let mut new_root_buf = old_root_buf;
    // headerKey at long 1 (offset 4..8).
    BigEndian::write_u32(&mut new_root_buf[4..8], new_root);
    // bm_pages[0..25] at 0x13C onward, zero-fill unused slots.
    for i in 0..AFFS_BM_PAGES_ROOT as usize {
        let off = 0x13C + i * 4;
        let val = new_pages.get(i).copied().unwrap_or(0);
        BigEndian::write_u32(&mut new_root_buf[off..off + 4], val);
    }
    // bm_ext stays zero (we refused bm_ext volumes earlier).
    BigEndian::write_u32(&mut new_root_buf[0x1A0..0x1A4], 0);
    // bm_flag at 0x138 stays -1 (valid).
    BigEndian::write_i32(&mut new_root_buf[0x138..0x13C], -1);
    // Re-stamp the normal checksum (word 5).
    BigEndian::write_u32(&mut new_root_buf[0x14..0x18], 0);
    let sum = normal_checksum(&new_root_buf, 5);
    BigEndian::write_u32(&mut new_root_buf[0x14..0x18], sum);

    // --- 8. Build each new bitmap-page buffer. ---
    let mut new_bm_page_bufs: Vec<[u8; BSIZE]> = Vec::with_capacity(new_pages.len());
    for page_idx in 0..new_pages.len() {
        let mut buf = [0u8; BSIZE];
        let bits_offset_in_page = page_idx as u32 * AFFS_BITS_PER_BM_PAGE;
        for word_idx in 0..127u32 {
            let mut word: u32 = 0;
            for bit_in_word in 0..32u32 {
                let global_bit = bits_offset_in_page + word_idx * 32 + bit_in_word;
                let global_block = global_bit + 2;
                if global_block >= new_total {
                    break;
                }
                // Set bit if `clear_bit` did NOT clear it (i.e. block is free).
                let byte = (global_bit / 8) as usize;
                let off = (global_bit % 8) as u8;
                let is_free = new_bm.get(byte).map_or(false, |b| (b >> off) & 1 == 1);
                if is_free {
                    word |= 1u32 << bit_in_word;
                }
            }
            let o = (word_idx as usize + 1) * 4;
            BigEndian::write_u32(&mut buf[o..o + 4], word);
        }
        // Bitmap checksum at offset 0.
        let chk = bitmap_checksum(&buf);
        BigEndian::write_u32(&mut buf[0..4], chk);
        new_bm_page_bufs.push(buf);
    }

    // --- 9. Move data blocks that fall on old-metadata positions but not
    //        on new-metadata positions out of the way. (No: that would
    //        need actual data relocation. Instead, the safety contract
    //        already requires user data NOT to live at new_metadata
    //        positions. Old metadata blocks become free space.)
    //
    //        Plan: write new root + new bitmap pages at the new positions
    //        first, then zero the old metadata block bodies for cleanliness.
    //        Order matters for crash safety: write bitmap pages first, then
    //        the root last (since the root is what the reader keys off).

    for (page_idx, &page_blk) in new_pages.iter().enumerate() {
        device.seek(SeekFrom::Start(
            partition_offset + page_blk as u64 * BSIZE_U64,
        ))?;
        device.write_all(&new_bm_page_bufs[page_idx])?;
    }
    device.seek(SeekFrom::Start(
        partition_offset + new_root as u64 * BSIZE_U64,
    ))?;
    device.write_all(&new_root_buf)?;

    // --- 10. Zero the OLD metadata positions IF they fall inside the new
    //         volume AND aren't reused as new metadata. ---
    let mut zero_buf = [0u8; BSIZE];
    let to_zero: Vec<u32> = old_metadata
        .iter()
        .copied()
        .filter(|&blk| blk < new_total && !new_metadata.contains(&blk))
        .collect();
    for blk in to_zero {
        // Re-zero to remove the stale root / bitmap so a stray scan
        // doesn't trip over a misplaced ST_ROOT.
        zero_buf.fill(0);
        device.seek(SeekFrom::Start(partition_offset + blk as u64 * BSIZE_U64))?;
        device.write_all(&zero_buf)?;
    }

    device.flush()?;

    log(&format!(
        "AFFS resize: {} -> {} sectors ({} -> {} bytes), root {} -> {}, \
         {} bitmap page(s)",
        old_total,
        new_total,
        old_total as u64 * BSIZE_U64,
        new_total as u64 * BSIZE_U64,
        old_root,
        new_root,
        new_pages.len(),
    ));
    Ok(())
}

/// Scan for an ST_ROOT block whose header-key matches its own block
/// number. Tries `(2 + upper_total - 1) / 2` first (cheap path for
/// volumes that haven't been resized yet), then walks the plausible
/// range upward looking for the marker. Returns the block number, or
/// `None` if no ST_ROOT was found in the volume.
fn find_affs_root_block(
    device: &mut (impl Read + Seek),
    partition_offset: u64,
    upper_total: u32,
) -> anyhow::Result<Option<u32>> {
    let probe_at = |device: &mut (dyn AffsRootProbe), blk: u32| -> std::io::Result<bool> {
        let mut buf = [0u8; BSIZE];
        device.read_block(partition_offset + blk as u64 * BSIZE_U64, &mut buf)?;
        if read_i32(&buf, BSIZE - 4) != ST_ROOT {
            return Ok(false);
        }
        if read_i32(&buf, 0) != T_HEADER {
            return Ok(false);
        }
        if read_u32(&buf, 4) != blk {
            return Ok(false);
        }
        Ok(true)
    };

    // Adapter: Read+Seek -> AffsRootProbe trait so we can reuse one helper.
    struct Adapter<'a, D: Read + Seek + ?Sized>(&'a mut D);
    impl<'a, D: Read + Seek + ?Sized> AffsRootProbe for Adapter<'a, D> {
        fn read_block(&mut self, offset: u64, buf: &mut [u8]) -> std::io::Result<()> {
            self.0.seek(SeekFrom::Start(offset))?;
            self.0.read_exact(buf)
        }
    }
    let mut adapter = Adapter(device);

    // Cheap path: try the EOF-derived root first.
    let cheap = (2 + upper_total.saturating_sub(1)) / 2;
    if cheap >= 2 && cheap < upper_total && probe_at(&mut adapter, cheap)? {
        return Ok(Some(cheap));
    }
    // Bounded scan. The root sits at `(2 + total - 1) / 2` for some
    // `total <= upper_total`, so the search space is
    // `[2, upper_total / 2 + 1]`. Walk it linearly.
    let scan_end = upper_total / 2 + 1;
    for blk in 2..=scan_end {
        if probe_at(&mut adapter, blk)? {
            return Ok(Some(blk));
        }
    }
    Ok(None)
}

trait AffsRootProbe {
    fn read_block(&mut self, offset: u64, buf: &mut [u8]) -> std::io::Result<()>;
}

/// How many bitmap pages are needed to cover blocks `[2..total)`. Returns
/// 1 for the trivial case where `total <= 2` (no data area). Each page
/// covers 4064 bits.
fn affs_bm_pages_needed(total: u32) -> usize {
    let bits = total.saturating_sub(2) as u64;
    if bits == 0 {
        return 1;
    }
    ((bits + AFFS_BITS_PER_BM_PAGE as u64 - 1) / AFFS_BITS_PER_BM_PAGE as u64) as usize
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
    fn create_file_and_directory_roundtrip() {
        use super::super::filesystem::{CreateDirectoryOptions, CreateFileOptions};

        let img = make_empty_floppy_image(1, "RW");
        let mut cur = Cursor::new(img);
        {
            let mut fs = AffsFilesystem::open(&mut cur, 0).expect("open");
            let root = fs.root().expect("root");
            fs.create_directory(&root, "Docs", &CreateDirectoryOptions::default())
                .expect("mkdir");
            let payload = b"Hello, Amiga!".to_vec();
            let mut src = std::io::Cursor::new(payload.clone());
            fs.create_file(
                &root,
                "README",
                &mut src,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create_file");
            fs.sync_metadata().expect("sync");
        }
        // Re-open and read back.
        let mut fs = AffsFilesystem::open(&mut cur, 0).expect("reopen");
        let root = fs.root().expect("root");
        let children = fs.list_directory(&root).expect("list");
        let names: Vec<&str> = children.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"Docs"), "Docs missing: {names:?}");
        assert!(names.contains(&"README"), "README missing: {names:?}");
        let readme = children.iter().find(|c| c.name == "README").unwrap();
        assert_eq!(readme.size, 13);
        let data = fs.read_file(readme, usize::MAX).expect("read");
        assert_eq!(&data, b"Hello, Amiga!");
    }

    #[test]
    fn delete_entry_frees_blocks() {
        use super::super::filesystem::CreateFileOptions;

        let img = make_empty_floppy_image(1, "DEL");
        let mut cur = Cursor::new(img);
        let allocated_before;
        let allocated_after_create;
        let allocated_after_delete;
        {
            let mut fs = AffsFilesystem::open(&mut cur, 0).expect("open");
            allocated_before = fs.allocated_blocks();
            let root = fs.root().expect("root");
            let payload = vec![0xAAu8; 2000];
            let mut src = std::io::Cursor::new(payload.clone());
            fs.create_file(
                &root,
                "big",
                &mut src,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create_file");
            fs.sync_metadata().expect("sync");
            allocated_after_create = fs.allocated_blocks();
            // The file should have grown the allocation by:
            //   1 header + ceil(2000/512) = 1 + 4 = 5 blocks.
            assert_eq!(allocated_after_create - allocated_before, 5);

            let children = fs.list_directory(&root).expect("list");
            let big = children.iter().find(|c| c.name == "big").unwrap().clone();
            fs.delete_entry(&root, &big).expect("delete");
            fs.sync_metadata().expect("sync");
            allocated_after_delete = fs.allocated_blocks();
        }
        assert_eq!(
            allocated_after_delete, allocated_before,
            "delete should free all blocks the file occupied"
        );
        // Reopen and verify the entry is gone.
        let mut fs = AffsFilesystem::open(&mut cur, 0).expect("reopen");
        let root = fs.root().expect("root");
        let children = fs.list_directory(&root).expect("list");
        assert!(children.iter().all(|c| c.name != "big"));
    }

    #[test]
    fn fsck_clean_volume_reports_no_errors() {
        let img = make_empty_floppy_image(1, "Clean");
        let mut cur = Cursor::new(img);
        let mut fs = AffsFilesystem::open(&mut cur, 0).expect("open");
        let res = fs.fsck().expect("fsck available").expect("fsck succeeded");
        assert!(
            res.errors.is_empty(),
            "errors: {:?}",
            res.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        assert!(res.is_clean());
        assert!(res.stats.directories_checked >= 1);
    }

    #[test]
    fn fsck_detects_and_repairs_bitmap_mismatch() {
        use super::super::filesystem::CreateFileOptions;

        let img = make_empty_floppy_image(1, "BitmapCorrupt");
        let mut cur = Cursor::new(img);

        // Step 1: create a file to populate the bitmap, sync to disk.
        {
            let mut fs = AffsFilesystem::open(&mut cur, 0).expect("open");
            let root = fs.root().expect("root");
            let payload = vec![0xCDu8; 1500];
            let mut src = std::io::Cursor::new(payload);
            fs.create_file(&root, "data", &mut src, 1500, &CreateFileOptions::default())
                .expect("create_file");
            fs.sync_metadata().expect("sync");
        }

        // Step 2: corrupt the bitmap on disk. Bitmap page is block 881.
        // Find the word covering some allocated bit and flip it to "free"
        // (set the bit) — that simulates an unclean unmount.
        {
            // Flip word index 0 (covering blocks 2..=33) entirely to "all
            // free" — any block actually allocated in that range becomes a
            // bitmap mismatch.
            let bm_off = 881 * BSIZE;
            let inner = cur.get_mut();
            let word_slot = bm_off + 4; // word_idx 0 + 1 (skip checksum)
            inner[word_slot..word_slot + 4].copy_from_slice(&0xFFFFFFFFu32.to_be_bytes());
            // Recompute checksum.
            let bm = &mut inner[bm_off..bm_off + BSIZE];
            let mut sum_buf = [0u8; BSIZE];
            sum_buf.copy_from_slice(bm);
            sum_buf[0..4].copy_from_slice(&[0u8; 4]);
            let sum = bitmap_checksum(&sum_buf);
            bm[0..4].copy_from_slice(&sum.to_be_bytes());
        }

        // Step 3: fsck should now report a repairable bitmap mismatch.
        {
            let mut fs = AffsFilesystem::open(&mut cur, 0).expect("reopen");
            let res = fs.fsck().expect("fsck").expect("fsck ok");
            assert!(!res.is_clean(), "fsck should detect bitmap mismatch");
            assert!(
                res.errors.iter().any(|e| e.code == "AffsBitmapMismatch"),
                "expected AffsBitmapMismatch among {:?}",
                res.errors.iter().map(|e| &e.code).collect::<Vec<_>>(),
            );
            assert!(res.repairable);
        }

        // Step 4: repair, then re-check — should be clean.
        {
            use super::super::filesystem::EditableFilesystem;
            let mut fs = AffsFilesystem::open(&mut cur, 0).expect("reopen");
            let report = fs.repair().expect("repair");
            assert!(
                !report.fixes_applied.is_empty(),
                "expected at least one fix; applied={:?} failed={:?}",
                report.fixes_applied,
                report.fixes_failed,
            );
        }
        {
            let mut fs = AffsFilesystem::open(&mut cur, 0).expect("reopen after repair");
            let res = fs.fsck().expect("fsck").expect("ok");
            assert!(
                res.is_clean(),
                "fsck should be clean after repair; errors: {:?}",
                res.errors.iter().map(|e| &e.message).collect::<Vec<_>>(),
            );
        }
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

    /// Verify that `CreateFileOptions::amiga_protection` /
    /// `amiga_comment` round-trip through `create_file` → `sync_metadata`
    /// → reopen → `peek_entry_block`.
    #[test]
    fn create_file_persists_amiga_protection_and_comment() {
        use super::super::filesystem::CreateFileOptions;

        let img = make_empty_floppy_image(1, "META");
        let mut cur = Cursor::new(img);
        let header_block;
        {
            let mut fs = AffsFilesystem::open(&mut cur, 0).expect("open");
            let root = fs.root().expect("root");
            let payload = b"protected".to_vec();
            let mut src = std::io::Cursor::new(payload.clone());
            let opts = CreateFileOptions {
                // AmigaDOS protection: archive | hold | script | pure (high
                // nibble) plus the standard r/w/e/d in the low nibble. The
                // exact value doesn't matter — what matters is that it
                // round-trips byte-for-byte through the header block.
                amiga_protection: Some(0x0000_00A5),
                amiga_comment: Some("a test filenote".to_string()),
                ..Default::default()
            };
            let fe = fs
                .create_file(&root, "secret", &mut src, payload.len() as u64, &opts)
                .expect("create_file");
            header_block = fe.location as u32;
            fs.sync_metadata().expect("sync");
        }
        // Re-open and re-parse the header block.
        let mut fs = AffsFilesystem::open(&mut cur, 0).expect("reopen");
        let entry = fs.peek_entry_block(header_block).expect("peek");
        assert_eq!(entry.name, "secret");
        assert_eq!(entry.access, 0x0000_00A5);
        assert_eq!(entry.comment, "a test filenote");
    }

    /// Shrink a blank FFS floppy from 1760 → 800 sectors. The new root
    /// lands at (2 + 800 - 1) / 2 = 400, which is currently free. The
    /// old root + bitmap blocks (880, 881) are inside the new tail and
    /// become free space.
    #[test]
    fn resize_shrink_blank_floppy() {
        let img = make_empty_floppy_image(1, "Shrink");
        let mut cur = Cursor::new(img);
        let mut log: Vec<String> = Vec::new();
        let new_size = 800u64 * BSIZE_U64;
        resize_affs_in_place(&mut cur, 0, new_size, &mut |s| log.push(s.to_string()))
            .expect("shrink");
        assert!(
            log.iter().any(|l| l.contains("1760 -> 800")),
            "log: {log:?}"
        );

        // The image bytes are still 1760 sectors long, but truncate to
        // the new size and verify reopen sees the volume at its new
        // total. The actual partition-table layer would truncate too.
        let mut bytes = cur.into_inner();
        bytes.truncate(new_size as usize);
        let mut cur2 = Cursor::new(bytes);
        let fs = AffsFilesystem::open(&mut cur2, 0).expect("reopen");
        assert_eq!(fs.total_blocks(), 800);
        assert_eq!(fs.root_block_num(), 400);
        // The new bitmap page at 401 should claim blocks 400+401 as
        // allocated, everything else free.
        let alloc_count = fs.allocated_blocks();
        // Boot blocks (2) + root + 1 bm page = 4.
        assert_eq!(alloc_count, 4, "expected 4 alloc, got {alloc_count}");
    }

    /// Grow a blank FFS floppy from 1760 → 3520 sectors. New root lands
    /// at (2 + 3520 - 1) / 2 = 1760, which is exactly past the old
    /// tail — fully free space in the grown medium.
    #[test]
    fn resize_grow_blank_floppy() {
        let mut img = make_empty_floppy_image(1, "Grow");
        // Pad to the new size so we can write into it.
        let new_size = 3520usize * BSIZE;
        img.resize(new_size, 0);
        let mut cur = Cursor::new(img);
        let mut log: Vec<String> = Vec::new();
        resize_affs_in_place(&mut cur, 0, new_size as u64, &mut |s| {
            log.push(s.to_string())
        })
        .expect("grow");
        assert!(
            log.iter().any(|l| l.contains("1760 -> 3520")),
            "log: {log:?}"
        );
        // Reopen and verify.
        let mut cur = cur;
        let fs = AffsFilesystem::open(&mut cur, 0).expect("reopen after grow");
        assert_eq!(fs.total_blocks(), 3520);
        assert_eq!(fs.root_block_num(), 1760);
        let alloc = fs.allocated_blocks();
        // boot(2) + new root + 1 bm page = 4. (The OLD root/bm at
        // 880/881 were inside the volume but get marked free in the new
        // bitmap.)
        assert_eq!(alloc, 4, "expected 4 alloc, got {alloc}");
    }

    /// Refuse to shrink past a file's data block. The AFFS allocator
    /// places data blocks at the lowest free sectors. By writing a
    /// large file we push the data tail high enough to set up a
    /// meaningful shrink-refusal target.
    #[test]
    fn resize_shrink_refuses_when_data_lost() {
        use super::super::filesystem::CreateFileOptions;
        let img = make_empty_floppy_image(1, "Reject");
        let mut cur = Cursor::new(img);
        let header_block: u32;
        let last_data_block: u32;
        {
            let mut fs = AffsFilesystem::open(&mut cur, 0).expect("open");
            let root = fs.root().expect("root");
            // 100 KiB → 200 data blocks (FFS, 512-byte blocks). With
            // alloc-from-low, those occupy sectors 2..~202 plus the
            // file header.
            let payload = vec![0xABu8; 100 * 1024];
            let mut src = std::io::Cursor::new(payload.clone());
            let fe = fs
                .create_file(
                    &root,
                    "big",
                    &mut src,
                    payload.len() as u64,
                    &CreateFileOptions::default(),
                )
                .expect("create");
            fs.sync_metadata().expect("sync");
            header_block = fe.location as u32;
            let entry = fs.peek_entry_block(header_block).expect("peek");
            last_data_block = *entry.data_blocks.last().expect("data blocks");
        }
        // Place the new tail BELOW the highest allocated block (but
        // above the 4-sector minimum). The resize must refuse with a
        // "past the target tail" error.
        let highest = header_block.max(last_data_block);
        assert!(
            highest > 10,
            "test setup expected high block, got {highest}"
        );
        let target_sectors = (highest - 1) as u64;
        let mut log: Vec<String> = Vec::new();
        let result = resize_affs_in_place(&mut cur, 0, target_sectors * BSIZE_U64, &mut |s| {
            log.push(s.to_string())
        });
        assert!(result.is_err(), "expected refusal, log: {log:?}");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("past the target tail") || err.contains("collides"),
            "unexpected error: {err}",
        );
    }

    /// Non-AFFS volume is a silent no-op.
    #[test]
    fn resize_skips_non_affs_volume() {
        let mut img = vec![0u8; 1760 * BSIZE];
        img[0..4].copy_from_slice(b"NOTA");
        let mut cur = Cursor::new(img);
        let mut log: Vec<String> = Vec::new();
        resize_affs_in_place(&mut cur, 0, 800 * BSIZE_U64, &mut |s| {
            log.push(s.to_string())
        })
        .expect("noop");
        assert!(log.is_empty(), "expected silent no-op, got: {log:?}");
    }

    /// Equal sizes log "no size change" and exit clean.
    #[test]
    fn resize_no_op_when_unchanged() {
        let img = make_empty_floppy_image(1, "Same");
        let mut cur = Cursor::new(img);
        let mut log: Vec<String> = Vec::new();
        resize_affs_in_place(&mut cur, 0, 1760 * BSIZE_U64, &mut |s| {
            log.push(s.to_string())
        })
        .expect("noop");
        assert!(
            log.iter().any(|l| l.contains("no size change")),
            "log: {log:?}"
        );
    }

    /// bm_pages_needed correctly accounts for the bitmap page size.
    #[test]
    fn bm_pages_needed_math() {
        // Empty volume.
        assert_eq!(affs_bm_pages_needed(0), 1);
        assert_eq!(affs_bm_pages_needed(2), 1);
        // 4064 + 2 = 4066 = exactly one page.
        assert_eq!(affs_bm_pages_needed(4066), 1);
        // One bit over: 4067 needs two pages.
        assert_eq!(affs_bm_pages_needed(4067), 2);
        // 50 pages worth.
        assert_eq!(affs_bm_pages_needed(2 + 50 * 4064), 50, "50 pages",);
    }
}
