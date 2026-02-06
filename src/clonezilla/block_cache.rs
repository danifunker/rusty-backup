//! On-demand block cache for browsing Clonezilla partclone images.
//!
//! Instead of creating a large seekable zstd cache file, this module caches
//! filesystem metadata blocks (boot sector, FAT table, directory entries) in
//! memory and decompresses file data blocks on demand from the partclone stream.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use log::{debug, warn};

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
    block_size: u32,
    total_blocks: u64,
    used_blocks: u64,
    checksum_mode: u8,
    blocks_per_checksum: u32,
    bitmap: Option<PartcloneBitmap>,

    // Block data cache: partition block index → data
    blocks: BTreeMap<u64, Vec<u8>>,

    // Decompressor state (forward-only through used-block stream)
    partclone_files: Vec<PathBuf>,
    decompressor: Option<Box<dyn Read + Send>>,
    /// The next partition block index to scan for in the bitmap when advancing.
    decompressor_next_partition_block: u64,
    /// Number of used blocks already consumed from the decompressor.
    decompressor_used_count: u64,

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
    fn cache_block(&mut self, block_index: u64, data: Vec<u8>) {
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

// ---------------------------------------------------------------------------
// Background metadata scan
// ---------------------------------------------------------------------------

/// Run the initial metadata scan in a background thread.
///
/// Decompresses the partclone stream sequentially, identifies filesystem
/// metadata blocks (boot sector, FAT table, directory entries), and caches
/// them. Leaves the decompressor positioned for future on-demand reads.
pub fn scan_metadata(
    cache: &Arc<Mutex<PartcloneBlockCache>>,
    partition_type: u8,
    cache_path: Option<&Path>,
) -> Result<()> {
    // Step 1: Open partclone and parse header + bitmap
    let files = cache.lock().unwrap().partclone_files.clone();
    let (header, bitmap, decoder) = partclone::open_partclone_raw(&files)?;

    let block_size = header.block_size;
    let total_blocks = header.total_blocks;
    let checksum_mode = header.checksum_mode;
    let blocks_per_checksum = header.blocks_per_checksum;

    // Step 2: Store header/bitmap info in cache
    {
        let mut c = cache.lock().unwrap();
        c.block_size = block_size;
        c.total_blocks = total_blocks;
        c.used_blocks = header.used_blocks;
        c.checksum_mode = checksum_mode;
        c.blocks_per_checksum = blocks_per_checksum;
        c.total_used_blocks = header.used_blocks;
        c.bitmap = Some(bitmap);
    }

    // Step 3: Sequential decompression — cache metadata blocks only
    let mut reader: Box<dyn Read + Send> = Box::new(decoder);
    let bs = block_size as usize;
    let mut block_buf = vec![0u8; bs];
    let mut used_count: u64 = 0;
    let mut current_block: u64 = 0;

    // We'll build a set of block indices that need to be cached.
    // Start with block 0 (boot sector), then expand after parsing it.
    let mut needed_blocks: BTreeSet<u64> = BTreeSet::new();
    needed_blocks.insert(0);

    // Blocks we've discovered as metadata but haven't cached yet.
    // Once this is empty and we've parsed all cached metadata, we're done.
    let mut metadata_identified = false;
    let mut directory_scan_done = false;

    // Track the highest needed block so we know when we can stop.
    let mut max_needed_block: u64 = 0;

    let bitmap_ref = {
        let c = cache.lock().unwrap();
        c.bitmap.as_ref().unwrap().data.clone()
    };
    let bitmap_is_v2 = cache.lock().unwrap().bitmap.as_ref().unwrap().is_v2;

    let is_used = |block: u64| -> bool {
        if bitmap_is_v2 {
            let byte_idx = (block / 8) as usize;
            let bit_idx = (block % 8) as u32;
            byte_idx < bitmap_ref.len() && (bitmap_ref[byte_idx] >> bit_idx) & 1 == 1
        } else {
            let idx = block as usize;
            idx < bitmap_ref.len() && bitmap_ref[idx] != 0
        }
    };

    while current_block < total_blocks {
        // Check if we've cached everything we need
        if metadata_identified && directory_scan_done && current_block > max_needed_block {
            break;
        }

        if !is_used(current_block) {
            current_block += 1;
            continue;
        }

        // Read this used block
        reader.read_exact(&mut block_buf).map_err(|e| {
            anyhow::anyhow!(
                "failed to read block {} (used block #{}): {e}",
                current_block,
                used_count,
            )
        })?;
        used_count += 1;

        // Skip CRC32 checksum
        if checksum_mode > 0
            && blocks_per_checksum > 0
            && used_count.is_multiple_of(blocks_per_checksum as u64)
        {
            let mut crc = [0u8; 4];
            reader.read_exact(&mut crc)?;
        }

        // Cache this block if it's needed
        if needed_blocks.contains(&current_block) {
            let mut c = cache.lock().unwrap();
            c.cache_block(current_block, block_buf.clone());
            c.scanned_used_blocks = used_count;
            drop(c);
            needed_blocks.remove(&current_block);
        }

        // After caching block 0, identify metadata layout
        if current_block == 0 && !metadata_identified {
            let new_blocks =
                identify_metadata_blocks(&block_buf, partition_type, block_size, total_blocks);
            for b in &new_blocks {
                if !cache.lock().unwrap().blocks.contains_key(b) {
                    needed_blocks.insert(*b);
                }
            }
            if let Some(&m) = new_blocks.iter().max() {
                max_needed_block = max_needed_block.max(m);
            }
            metadata_identified = true;
            debug!(
                "Identified {} metadata blocks (max block {})",
                new_blocks.len(),
                max_needed_block,
            );
        }

        // After metadata blocks are cached, discover directory clusters
        if metadata_identified && needed_blocks.is_empty() && !directory_scan_done {
            let c = cache.lock().unwrap();
            let dir_blocks = discover_directory_blocks(&c, partition_type, block_size);
            drop(c);

            for b in &dir_blocks {
                if !cache.lock().unwrap().blocks.contains_key(b) {
                    needed_blocks.insert(*b);
                    max_needed_block = max_needed_block.max(*b);
                }
            }
            directory_scan_done = true;
            debug!(
                "Discovered {} directory blocks (max block {})",
                dir_blocks.len(),
                max_needed_block,
            );
        }

        // Update progress
        if used_count.is_multiple_of(1000) {
            let mut c = cache.lock().unwrap();
            c.scanned_used_blocks = used_count;
        }

        current_block += 1;
    }

    // Store decompressor in cache for future on-demand reads
    {
        let mut c = cache.lock().unwrap();
        c.decompressor = Some(reader);
        c.decompressor_next_partition_block = current_block;
        c.decompressor_used_count = used_count;
        c.scanned_used_blocks = used_count;
        c.state = CacheState::Ready;
        debug!(
            "Metadata scan complete: cached {} blocks, scanned {} used blocks",
            c.blocks.len(),
            used_count,
        );

        // Save to disk for fast re-loading on next session
        if let Some(path) = cache_path {
            if let Err(e) = c.save_to_file(path) {
                warn!("Failed to save metadata cache to {}: {e}", path.display());
            } else {
                debug!("Saved metadata cache to {}", path.display());
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Filesystem-specific metadata identification
// ---------------------------------------------------------------------------

/// Given the boot sector, determine which partition blocks contain filesystem
/// metadata (FAT table, root directory, MFT, etc.).
fn identify_metadata_blocks(
    boot_sector: &[u8],
    partition_type: u8,
    block_size: u32,
    _total_blocks: u64,
) -> Vec<u64> {
    let bs = block_size as u64;
    if bs == 0 || boot_sector.len() < 512 {
        return vec![0];
    }

    match partition_type {
        // FAT12/16/32
        0x01 | 0x04 | 0x06 | 0x0E | 0x14 | 0x16 | 0x1E | 0x0B | 0x0C | 0x1B | 0x1C => {
            identify_fat_metadata_blocks(boot_sector, bs)
        }
        // NTFS / exFAT (type 0x07)
        0x07 => {
            // Distinguish by OEM ID at offset 3
            let oem = &boot_sector[3..11];
            if oem == b"NTFS    " {
                identify_ntfs_metadata_blocks(boot_sector, bs)
            } else if oem == b"EXFAT   " {
                identify_exfat_metadata_blocks(boot_sector, bs)
            } else {
                vec![0]
            }
        }
        _ => vec![0],
    }
}

/// Identify FAT12/16/32 metadata blocks: reserved area + FAT tables + root directory.
fn identify_fat_metadata_blocks(bpb: &[u8], block_size: u64) -> Vec<u64> {
    let mut blocks = BTreeSet::new();

    let bytes_per_sector = u16::from_le_bytes([bpb[11], bpb[12]]) as u64;
    if bytes_per_sector == 0 {
        return vec![0];
    }
    let reserved_sectors = u16::from_le_bytes([bpb[14], bpb[15]]) as u64;
    let num_fats = bpb[16] as u64;
    let root_entry_count = u16::from_le_bytes([bpb[17], bpb[18]]) as u64;
    let sectors_per_fat_16 = u16::from_le_bytes([bpb[22], bpb[23]]) as u64;
    let sectors_per_fat_32 = u32::from_le_bytes([bpb[36], bpb[37], bpb[38], bpb[39]]) as u64;
    let sectors_per_fat = if sectors_per_fat_16 != 0 {
        sectors_per_fat_16
    } else {
        sectors_per_fat_32
    };

    // Reserved area (includes boot sector)
    let reserved_end = reserved_sectors * bytes_per_sector;
    for byte_off in (0..reserved_end).step_by(block_size as usize) {
        blocks.insert(byte_off / block_size);
    }

    // FAT tables
    let fat_start = reserved_sectors * bytes_per_sector;
    let fat_end = fat_start + num_fats * sectors_per_fat * bytes_per_sector;
    for byte_off in (fat_start..fat_end).step_by(block_size as usize) {
        blocks.insert(byte_off / block_size);
    }

    // Root directory (FAT12/16 only — fixed area after FAT)
    if root_entry_count > 0 {
        let root_start = fat_end;
        let root_size = root_entry_count * 32;
        let root_end = root_start + root_size;
        for byte_off in (root_start..root_end).step_by(block_size as usize) {
            blocks.insert(byte_off / block_size);
        }
    }

    // For FAT32, we also need the root directory cluster — but that requires
    // reading the FAT table first. We'll handle that in discover_directory_blocks().

    blocks.into_iter().collect()
}

/// Identify NTFS metadata blocks: VBR + MFT region.
fn identify_ntfs_metadata_blocks(vbr: &[u8], block_size: u64) -> Vec<u64> {
    let mut blocks = BTreeSet::new();

    // Block 0 (VBR)
    blocks.insert(0);

    let bytes_per_sector = u16::from_le_bytes([vbr[0x0B], vbr[0x0C]]) as u64;
    let sectors_per_cluster = vbr[0x0D] as u64;
    if bytes_per_sector == 0 || sectors_per_cluster == 0 {
        return blocks.into_iter().collect();
    }
    let cluster_size = bytes_per_sector * sectors_per_cluster;

    let mft_cluster = u64::from_le_bytes([
        vbr[0x30], vbr[0x31], vbr[0x32], vbr[0x33], vbr[0x34], vbr[0x35], vbr[0x36], vbr[0x37],
    ]);

    // Compute MFT record size
    let clusters_per_mft_raw = vbr[0x40] as i8;
    let mft_record_size: u64 = if clusters_per_mft_raw < 0 {
        1u64 << (-clusters_per_mft_raw as u32)
    } else {
        clusters_per_mft_raw as u64 * cluster_size
    };

    // Cache the first 64 MFT records (covers $MFT, $MFTMirr, $LogFile, $Volume,
    // $AttrDef, root directory, $Bitmap, $Boot, $BadClus, $Secure, $UpCase, $Extend, etc.)
    let mft_byte_start = mft_cluster * cluster_size;
    let mft_records_to_cache: u64 = 64;
    let mft_byte_end = mft_byte_start + mft_records_to_cache * mft_record_size;

    for byte_off in (mft_byte_start..mft_byte_end).step_by(block_size as usize) {
        blocks.insert(byte_off / block_size);
    }

    blocks.into_iter().collect()
}

/// Identify exFAT metadata blocks: VBR + FAT + root directory area.
fn identify_exfat_metadata_blocks(vbr: &[u8], block_size: u64) -> Vec<u64> {
    let mut blocks = BTreeSet::new();

    // Block 0 (VBR)
    blocks.insert(0);

    if vbr.len() < 120 {
        return blocks.into_iter().collect();
    }

    let bytes_per_sector_shift = vbr[108] as u64;
    let sectors_per_cluster_shift = vbr[109] as u64;
    if !(9..=12).contains(&bytes_per_sector_shift) {
        return blocks.into_iter().collect();
    }
    let bytes_per_sector = 1u64 << bytes_per_sector_shift;
    let _cluster_size = bytes_per_sector << sectors_per_cluster_shift;

    let fat_offset_sectors = u32::from_le_bytes([vbr[80], vbr[81], vbr[82], vbr[83]]) as u64;
    let fat_length_sectors = u32::from_le_bytes([vbr[84], vbr[85], vbr[86], vbr[87]]) as u64;

    // VBR region (first 12 sectors for main + backup boot region)
    let vbr_end = 24 * bytes_per_sector; // 12 main + 12 backup
    for byte_off in (0..vbr_end).step_by(block_size as usize) {
        blocks.insert(byte_off / block_size);
    }

    // FAT table
    let fat_start = fat_offset_sectors * bytes_per_sector;
    let fat_end = fat_start + fat_length_sectors * bytes_per_sector;
    for byte_off in (fat_start..fat_end).step_by(block_size as usize) {
        blocks.insert(byte_off / block_size);
    }

    // Root directory cluster — need to read the FAT to follow the chain.
    // The root cluster is at a known offset. Cache the first cluster.
    let cluster_heap_offset = u32::from_le_bytes([vbr[88], vbr[89], vbr[90], vbr[91]]) as u64;
    let root_cluster = u32::from_le_bytes([vbr[96], vbr[97], vbr[98], vbr[99]]) as u64;
    let cluster_size = bytes_per_sector << sectors_per_cluster_shift;

    if root_cluster >= 2 {
        let root_byte =
            (cluster_heap_offset * bytes_per_sector) + (root_cluster - 2) * cluster_size;
        for byte_off in (root_byte..root_byte + cluster_size).step_by(block_size as usize) {
            blocks.insert(byte_off / block_size);
        }
    }

    blocks.into_iter().collect()
}

// ---------------------------------------------------------------------------
// Directory cluster discovery (requires cached FAT table)
// ---------------------------------------------------------------------------

/// After FAT table blocks are cached, discover all directory cluster blocks
/// by parsing directory entries and following cluster chains.
fn discover_directory_blocks(
    cache: &PartcloneBlockCache,
    partition_type: u8,
    block_size: u32,
) -> Vec<u64> {
    match partition_type {
        0x01 | 0x04 | 0x06 | 0x0E | 0x14 | 0x16 | 0x1E | 0x0B | 0x0C | 0x1B | 0x1C => {
            discover_fat_directory_blocks(cache, block_size)
        }
        0x07 => {
            // NTFS/exFAT — for NTFS the MFT covers all metadata.
            // For exFAT, directory discovery follows the same FAT-chain pattern.
            // For now, the initial MFT/FAT cache is sufficient for browsing.
            // Directory index blocks in NTFS are loaded on demand.
            vec![]
        }
        _ => vec![],
    }
}

/// Discover all FAT directory cluster blocks by BFS from the root directory.
fn discover_fat_directory_blocks(cache: &PartcloneBlockCache, block_size: u32) -> Vec<u64> {
    let bs = block_size as u64;
    if bs == 0 {
        return vec![];
    }

    // Read BPB from cached block 0
    let boot = match cache.blocks.get(&0) {
        Some(b) => b,
        None => return vec![],
    };
    if boot.len() < 512 {
        return vec![];
    }

    let bytes_per_sector = u16::from_le_bytes([boot[11], boot[12]]) as u64;
    let sectors_per_cluster = boot[13] as u64;
    if bytes_per_sector == 0 || sectors_per_cluster == 0 {
        return vec![];
    }
    let cluster_size = bytes_per_sector * sectors_per_cluster;

    let reserved_sectors = u16::from_le_bytes([boot[14], boot[15]]) as u64;
    let num_fats = boot[16] as u64;
    let root_entry_count = u16::from_le_bytes([boot[17], boot[18]]) as u64;
    let sectors_per_fat_16 = u16::from_le_bytes([boot[22], boot[23]]) as u64;
    let sectors_per_fat_32 = u32::from_le_bytes([boot[36], boot[37], boot[38], boot[39]]) as u64;
    let sectors_per_fat = if sectors_per_fat_16 != 0 {
        sectors_per_fat_16
    } else {
        sectors_per_fat_32
    };

    let root_dir_sectors = (root_entry_count * 32).div_ceil(bytes_per_sector);
    let data_start_sector = reserved_sectors + (num_fats * sectors_per_fat) + root_dir_sectors;
    let data_start_byte = data_start_sector * bytes_per_sector;

    let is_fat32 = sectors_per_fat_16 == 0 && root_entry_count == 0;

    // Helper: cluster number → byte offset in partition
    let cluster_to_byte = |cluster: u32| -> u64 {
        if cluster < 2 {
            return 0;
        }
        data_start_byte + (cluster as u64 - 2) * cluster_size
    };

    // Helper: read FAT entry from cached blocks
    let read_fat_entry = |cluster: u32| -> Option<u32> {
        let fat_start = reserved_sectors * bytes_per_sector;
        let (offset, entry_size) = if is_fat32 {
            (fat_start + cluster as u64 * 4, 4)
        } else if sectors_per_fat_16 > 0 {
            // FAT16
            (fat_start + cluster as u64 * 2, 2)
        } else {
            return None;
        };

        let block_idx = offset / bs;
        let block_off = (offset % bs) as usize;
        let block_data = cache.blocks.get(&block_idx)?;

        if block_off + entry_size > block_data.len() {
            // Entry spans two blocks — handle the boundary
            let mut entry_buf = [0u8; 4];
            let first_part = block_data.len() - block_off;
            entry_buf[..first_part].copy_from_slice(&block_data[block_off..]);
            let next_block = cache.blocks.get(&(block_idx + 1))?;
            let remaining = entry_size - first_part;
            entry_buf[first_part..first_part + remaining].copy_from_slice(&next_block[..remaining]);

            if entry_size == 4 {
                Some(u32::from_le_bytes(entry_buf) & 0x0FFF_FFFF)
            } else {
                Some(u16::from_le_bytes([entry_buf[0], entry_buf[1]]) as u32)
            }
        } else if entry_size == 4 {
            Some(
                u32::from_le_bytes([
                    block_data[block_off],
                    block_data[block_off + 1],
                    block_data[block_off + 2],
                    block_data[block_off + 3],
                ]) & 0x0FFF_FFFF,
            )
        } else {
            Some(u16::from_le_bytes([block_data[block_off], block_data[block_off + 1]]) as u32)
        }
    };

    // Helper: follow cluster chain and return all cluster numbers
    let follow_chain = |start: u32| -> Vec<u32> {
        let mut clusters = vec![];
        let mut current = start;
        let end_marker = if is_fat32 { 0x0FFF_FFF8 } else { 0xFFF8 };
        let max_iters = 100_000; // safety limit
        for _ in 0..max_iters {
            if current < 2 || current >= end_marker {
                break;
            }
            clusters.push(current);
            match read_fat_entry(current) {
                Some(next) => current = next,
                None => break,
            }
        }
        clusters
    };

    // Helper: parse 32-byte directory entries to find subdirectory start clusters
    let find_subdirectory_clusters = |dir_data: &[u8]| -> Vec<u32> {
        let mut subdirs = vec![];
        for entry in dir_data.chunks(32) {
            if entry.len() < 32 || entry[0] == 0x00 {
                break; // end of directory
            }
            if entry[0] == 0xE5 {
                continue; // deleted entry
            }
            if entry[11] == 0x0F {
                continue; // LFN entry
            }
            // Check directory attribute (bit 4)
            if entry[11] & 0x10 != 0 {
                // Skip . and .. entries
                if entry[0] == b'.' {
                    continue;
                }
                let cluster_hi = u16::from_le_bytes([entry[20], entry[21]]) as u32;
                let cluster_lo = u16::from_le_bytes([entry[26], entry[27]]) as u32;
                let cluster = (cluster_hi << 16) | cluster_lo;
                if cluster >= 2 {
                    subdirs.push(cluster);
                }
            }
        }
        subdirs
    };

    let mut dir_blocks: BTreeSet<u64> = BTreeSet::new();
    let mut dir_queue: VecDeque<u32> = VecDeque::new();
    let mut visited_clusters: BTreeSet<u32> = BTreeSet::new();

    // Seed: root directory
    if is_fat32 {
        let root_cluster = u32::from_le_bytes([boot[44], boot[45], boot[46], boot[47]]);
        if root_cluster >= 2 {
            dir_queue.push_back(root_cluster);
        }
    } else {
        // FAT12/16: root directory is in fixed area (already cached)
        // Parse it to find subdirectories
        let root_start = (reserved_sectors + num_fats * sectors_per_fat) * bytes_per_sector;
        let root_size = (root_entry_count * 32) as usize;
        let mut root_data = vec![0u8; root_size];
        let mut filled = 0;
        // Reconstruct root dir data from cached blocks
        let mut byte_off = root_start;
        while filled < root_size {
            let block_idx = byte_off / bs;
            let block_off = (byte_off % bs) as usize;
            if let Some(block_data) = cache.blocks.get(&block_idx) {
                let avail = (block_data.len() - block_off).min(root_size - filled);
                root_data[filled..filled + avail]
                    .copy_from_slice(&block_data[block_off..block_off + avail]);
                filled += avail;
                byte_off += avail as u64;
            } else {
                break;
            }
        }
        let subdirs = find_subdirectory_clusters(&root_data);
        for c in subdirs {
            dir_queue.push_back(c);
        }
    }

    // BFS: follow all directory cluster chains
    while let Some(dir_start_cluster) = dir_queue.pop_front() {
        if visited_clusters.contains(&dir_start_cluster) {
            continue;
        }
        visited_clusters.insert(dir_start_cluster);

        let chain = follow_chain(dir_start_cluster);
        let mut dir_data = Vec::new();

        for &cluster in &chain {
            let byte_start = cluster_to_byte(cluster);
            // Add all blocks covering this cluster
            for off in (0..cluster_size).step_by(bs as usize) {
                dir_blocks.insert((byte_start + off) / bs);
            }

            // Read cluster data from cache (if already cached)
            let mut cluster_data = vec![0u8; cluster_size as usize];
            let mut filled = 0;
            let mut byte_off = byte_start;
            while filled < cluster_size as usize {
                let block_idx = byte_off / bs;
                let block_off = (byte_off % bs) as usize;
                if let Some(block_data) = cache.blocks.get(&block_idx) {
                    let avail = (block_data.len() - block_off).min(cluster_size as usize - filled);
                    cluster_data[filled..filled + avail]
                        .copy_from_slice(&block_data[block_off..block_off + avail]);
                    filled += avail;
                    byte_off += avail as u64;
                } else {
                    // Block not yet cached — we'll need to cache it.
                    // The directory data won't be available now, but the block
                    // is already in dir_blocks set for caching.
                    break;
                }
            }

            if filled == cluster_size as usize {
                dir_data.extend_from_slice(&cluster_data);
            }
        }

        // Parse this directory's entries to find more subdirectories
        if !dir_data.is_empty() {
            let subdirs = find_subdirectory_clusters(&dir_data);
            for c in subdirs {
                if !visited_clusters.contains(&c) {
                    dir_queue.push_back(c);
                }
            }
        }
    }

    dir_blocks.into_iter().collect()
}
