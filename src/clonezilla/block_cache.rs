//! On-demand block cache for browsing Clonezilla partclone images.
//!
//! Instead of creating a large seekable zstd cache file, this module caches
//! filesystem metadata blocks (boot sector, FAT table, directory entries) in
//! memory and decompresses file data blocks on demand from the partclone stream.

use std::collections::BTreeMap;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use super::partclone::{self, PartcloneBitmap};

/// State of the block cache.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CacheState {
    /// Background metadata scan is running.
    Scanning,
    /// Metadata scan complete; cache is ready for browsing.
    Ready,
    /// An error occurred during scanning.
    Failed,
}

/// Shared block cache state, protected by a Mutex for thread-safe access.
///
/// Holds partclone block data in memory, indexed by partition block number.
/// Filesystem metadata blocks are pre-cached during the initial scan; data
/// blocks are loaded on demand via the sequential decompressor.
pub struct PartcloneBlockCache {
    // Partclone image metadata
    pub(super) block_size: u32,
    pub(super) total_blocks: u64,
    pub(super) used_blocks: u64,
    pub(super) checksum_mode: u8,
    pub(super) blocks_per_checksum: u32,
    pub(super) bitmap: Option<PartcloneBitmap>,

    // Block data cache: partition block index → data
    pub(super) blocks: BTreeMap<u64, Vec<u8>>,

    // Decompressor state (forward-only through used-block stream)
    pub(super) partclone_files: Vec<PathBuf>,
    pub(super) decompressor: Option<Box<dyn Read + Send>>,
    /// The next partition block index to scan for in the bitmap when advancing.
    pub(super) decompressor_next_partition_block: u64,
    /// Number of used blocks already consumed from the decompressor.
    pub(super) decompressor_used_count: u64,

    // Scan progress
    pub state: CacheState,
    pub error: Option<String>,
    pub scanned_used_blocks: u64,
    pub total_used_blocks: u64,
}

impl PartcloneBlockCache {
    /// Create a new empty cache for the given partclone files.
    pub fn new(partclone_files: Vec<PathBuf>) -> Self {
        Self {
            block_size: 0,
            total_blocks: 0,
            used_blocks: 0,
            checksum_mode: 0,
            blocks_per_checksum: 0,
            bitmap: None,
            blocks: BTreeMap::new(),
            partclone_files,
            decompressor: None,
            decompressor_next_partition_block: 0,
            decompressor_used_count: 0,
            state: CacheState::Scanning,
            error: None,
            scanned_used_blocks: 0,
            total_used_blocks: 0,
        }
    }

    /// Check if a partition block is used (has data in the partclone stream).
    pub fn is_block_used(&self, block_index: u64) -> bool {
        match &self.bitmap {
            Some(bm) => bm.is_used(block_index),
            None => false,
        }
    }

    /// Number of blocks currently cached.
    pub fn cached_block_count(&self) -> usize {
        self.blocks.len()
    }

    /// Total virtual partition size in bytes.
    pub fn partition_size(&self) -> u64 {
        self.total_blocks * self.block_size as u64
    }

    /// Read a single block, returning its data.
    ///
    /// - Unused blocks (per bitmap): returns zeros.
    /// - Cached blocks: returns from cache.
    /// - Uncached used blocks: decompresses on demand from the partclone stream.
    pub fn read_block(&mut self, block_index: u64) -> io::Result<Vec<u8>> {
        let bs = self.block_size as usize;
        if bs == 0 || block_index >= self.total_blocks {
            return Ok(vec![0u8; bs.max(512)]);
        }

        // Unused block → zeros
        if !self.is_block_used(block_index) {
            return Ok(vec![0u8; bs]);
        }

        // Already cached
        if let Some(data) = self.blocks.get(&block_index) {
            return Ok(data.clone());
        }

        // Must decompress on demand
        self.decompress_to_block(block_index)
    }

    /// Advance the decompressor to read the target block.
    ///
    /// If the decompressor has already passed the target, reopens it from the
    /// beginning. Intermediate blocks are read and discarded.
    fn decompress_to_block(&mut self, target_block: u64) -> io::Result<Vec<u8>> {
        // Check if we need to restart the decompressor
        if self.decompressor_next_partition_block > target_block {
            self.reopen_decompressor()?;
        }

        // Ensure we have a decompressor
        if self.decompressor.is_none() {
            self.reopen_decompressor()?;
        }

        let bs = self.block_size as usize;
        let mut block_buf = vec![0u8; bs];

        // Walk the bitmap from decompressor_next_partition_block forward
        let reader = self
            .decompressor
            .as_mut()
            .ok_or_else(|| io::Error::other("no decompressor available"))?;

        let bitmap = self
            .bitmap
            .as_ref()
            .ok_or_else(|| io::Error::other("no bitmap available"))?;

        let mut current_block = self.decompressor_next_partition_block;
        let total = self.total_blocks;
        let checksum_mode = self.checksum_mode;
        let blocks_per_checksum = self.blocks_per_checksum;
        let mut used_count = self.decompressor_used_count;

        while current_block <= target_block && current_block < total {
            if !bitmap.is_used(current_block) {
                current_block += 1;
                continue;
            }

            // Read this used block from the decompressor
            reader.read_exact(&mut block_buf).map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "failed to read block {} (used block #{}): {e}",
                        current_block, used_count,
                    ),
                )
            })?;
            used_count += 1;

            // Skip CRC32 checksum after every blocks_per_checksum used blocks
            if checksum_mode > 0
                && blocks_per_checksum > 0
                && used_count.is_multiple_of(blocks_per_checksum as u64)
            {
                let mut crc = [0u8; 4];
                reader.read_exact(&mut crc)?;
            }

            if current_block == target_block {
                // Found our target block — cache it and return
                self.blocks.insert(current_block, block_buf.clone());
                self.decompressor_next_partition_block = current_block + 1;
                self.decompressor_used_count = used_count;
                return Ok(block_buf);
            }

            // Not the target — skip (don't cache data blocks to save memory)
            current_block += 1;
        }

        self.decompressor_next_partition_block = current_block;
        self.decompressor_used_count = used_count;

        // Target block was not found (shouldn't happen if bitmap says it's used)
        Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("block {target_block} not found in partclone stream"),
        ))
    }

    /// Reopen the decompressor from scratch, positioned at the start of block data.
    fn reopen_decompressor(&mut self) -> io::Result<()> {
        let (header, bitmap, decoder) = partclone::open_partclone_raw(&self.partclone_files)
            .map_err(|e| io::Error::other(format!("failed to reopen partclone: {e}")))?;

        self.bitmap = Some(bitmap);
        self.block_size = header.block_size;
        self.total_blocks = header.total_blocks;
        self.used_blocks = header.used_blocks;
        self.checksum_mode = header.checksum_mode;
        self.blocks_per_checksum = header.blocks_per_checksum;

        self.decompressor = Some(Box::new(decoder));
        self.decompressor_next_partition_block = 0;
        self.decompressor_used_count = 0;
        Ok(())
    }

    /// Store a block in the cache.
    pub(super) fn cache_block(&mut self, block_index: u64, data: Vec<u8>) {
        self.blocks.insert(block_index, data);
    }

    /// Save the cached metadata blocks to a file for fast re-loading.
    ///
    /// Format: magic + header fields + bitmap + block entries.
    pub fn save_to_file(&self, path: &Path) -> io::Result<()> {
        let bitmap = match &self.bitmap {
            Some(b) => b,
            None => return Err(io::Error::other("no bitmap to save")),
        };

        let file = std::fs::File::create(path)?;
        let mut w = io::BufWriter::new(file);

        // Magic + version
        w.write_all(b"RBMC")?;
        w.write_u32::<LittleEndian>(1)?; // format version

        // Header fields
        w.write_u32::<LittleEndian>(self.block_size)?;
        w.write_u64::<LittleEndian>(self.total_blocks)?;
        w.write_u64::<LittleEndian>(self.used_blocks)?;
        w.write_u8(self.checksum_mode)?;
        w.write_u32::<LittleEndian>(self.blocks_per_checksum)?;

        // Bitmap
        w.write_u8(if bitmap.is_v2 { 1 } else { 0 })?;
        w.write_u64::<LittleEndian>(bitmap.data.len() as u64)?;
        w.write_all(&bitmap.data)?;

        // Cached blocks
        w.write_u64::<LittleEndian>(self.blocks.len() as u64)?;
        for (&block_index, data) in &self.blocks {
            w.write_u64::<LittleEndian>(block_index)?;
            w.write_all(data)?;
        }

        w.flush()?;
        Ok(())
    }

    /// Load cached metadata blocks from a file, restoring the cache to Ready state.
    ///
    /// The partclone_files must be set separately (for on-demand decompression).
    pub fn load_from_file(path: &Path, partclone_files: Vec<PathBuf>) -> io::Result<Self> {
        let file = std::fs::File::open(path)?;
        let mut r = io::BufReader::new(file);

        // Magic
        let mut magic = [0u8; 4];
        r.read_exact(&mut magic)?;
        if &magic != b"RBMC" {
            return Err(io::Error::other("invalid cache magic"));
        }

        let version = r.read_u32::<LittleEndian>()?;
        if version != 1 {
            return Err(io::Error::other(format!(
                "unsupported cache version: {version}"
            )));
        }

        // Header fields
        let block_size = r.read_u32::<LittleEndian>()?;
        let total_blocks = r.read_u64::<LittleEndian>()?;
        let used_blocks = r.read_u64::<LittleEndian>()?;
        let checksum_mode = r.read_u8()?;
        let blocks_per_checksum = r.read_u32::<LittleEndian>()?;

        // Bitmap
        let is_v2 = r.read_u8()? != 0;
        let bitmap_len = r.read_u64::<LittleEndian>()? as usize;
        let mut bitmap_data = vec![0u8; bitmap_len];
        r.read_exact(&mut bitmap_data)?;

        // Cached blocks
        let num_blocks = r.read_u64::<LittleEndian>()?;
        let mut blocks = BTreeMap::new();
        for _ in 0..num_blocks {
            let block_index = r.read_u64::<LittleEndian>()?;
            let mut data = vec![0u8; block_size as usize];
            r.read_exact(&mut data)?;
            blocks.insert(block_index, data);
        }

        Ok(Self {
            block_size,
            total_blocks,
            used_blocks,
            checksum_mode,
            blocks_per_checksum,
            bitmap: Some(PartcloneBitmap {
                data: bitmap_data,
                total_blocks,
                is_v2,
            }),
            blocks,
            partclone_files,
            decompressor: None,
            decompressor_next_partition_block: 0,
            decompressor_used_count: 0,
            state: CacheState::Ready,
            error: None,
            scanned_used_blocks: used_blocks,
            total_used_blocks: used_blocks,
        })
    }
}

/// A `Read + Seek` adapter that presents the full expanded partition image
/// by reading blocks from a shared `PartcloneBlockCache`.
pub struct PartcloneBlockReader {
    cache: Arc<Mutex<PartcloneBlockCache>>,
    /// Current virtual read position within the expanded partition.
    position: u64,
    /// Total size of the expanded partition.
    total_size: u64,
}

impl PartcloneBlockReader {
    pub fn new(cache: Arc<Mutex<PartcloneBlockCache>>) -> Self {
        let total_size = cache.lock().unwrap().partition_size();
        Self {
            cache,
            position: 0,
            total_size,
        }
    }
}

impl Read for PartcloneBlockReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.position >= self.total_size {
            return Ok(0);
        }

        let mut cache = self
            .cache
            .lock()
            .map_err(|e| io::Error::other(format!("cache lock poisoned: {e}")))?;

        let block_size = cache.block_size as u64;
        if block_size == 0 {
            return Ok(0);
        }

        let mut total_read = 0;

        while total_read < buf.len() && self.position < self.total_size {
            let block_index = self.position / block_size;
            let offset_in_block = (self.position % block_size) as usize;

            let block_data = cache.read_block(block_index)?;

            let available = block_data.len() - offset_in_block;
            let to_copy = available.min(buf.len() - total_read);
            buf[total_read..total_read + to_copy]
                .copy_from_slice(&block_data[offset_in_block..offset_in_block + to_copy]);

            total_read += to_copy;
            self.position += to_copy as u64;
        }

        Ok(total_read)
    }
}

impl Seek for PartcloneBlockReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(offset) => offset as i64,
            SeekFrom::End(offset) => self.total_size as i64 + offset,
            SeekFrom::Current(offset) => self.position as i64 + offset,
        };
        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek to negative position",
            ));
        }
        self.position = new_pos as u64;
        Ok(self.position)
    }
}

// Safety: PartcloneBlockReader is Send because Arc<Mutex<>> is Send.
// The Mutex ensures only one thread accesses the decompressor at a time.
unsafe impl Send for PartcloneBlockReader {}
