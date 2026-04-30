//! Background metadata scan for Clonezilla partclone images.
//!
//! Walks the decompressed partclone stream once, identifies filesystem-specific
//! metadata blocks (boot sector, FAT table, MFT, ext GDT/inode tables, btrfs
//! tree roots, FAT/Linux directory clusters), and stages them in
//! [`PartcloneBlockCache`]. Once complete, the cache is `Ready` and browsing
//! reads metadata from RAM while data blocks decompress on demand.
//!
//! Split out of `block_cache.rs` per §7 of `docs/codecleanup.md`.

use std::collections::{BTreeSet, VecDeque};
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use log::{debug, warn};

use super::block_cache::{CacheState, PartcloneBlockCache};
use super::partclone;

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
        // Linux (ext2/3/4, btrfs)
        0x83 => identify_linux_metadata_blocks(boot_sector, bs, _total_blocks),
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
        // Linux (ext2/3/4, btrfs) — directory data is loaded on demand
        // via the filesystem's own tree/inode reading. The metadata blocks
        // cached in identify_linux_metadata_blocks cover superblock, GDTs,
        // bitmaps, inode tables, and tree nodes needed for browsing.
        0x83 => discover_linux_directory_blocks(cache, block_size),
        _ => vec![],
    }
}

/// Identify metadata blocks for Linux partitions (ext2/3/4 or btrfs).
///
/// Since we can't always tell which filesystem it is from block 0 alone,
/// we probe the cached data for ext superblock magic and btrfs magic,
/// and return blocks for whichever is detected.
fn identify_linux_metadata_blocks(
    boot_sector: &[u8],
    block_size: u64,
    total_blocks: u64,
) -> Vec<u64> {
    let mut blocks = BTreeSet::new();
    blocks.insert(0);

    // Check if block 0 contains the ext superblock (at byte 1024 within the partition).
    // If block_size >= 2048, the superblock is within block 0 at offset 1024.
    // If block_size == 1024, the superblock is in block 1.
    let ext_sb_block = 1024 / block_size;
    blocks.insert(ext_sb_block);

    // Also request the btrfs superblock block (at byte offset 0x10000 = 65536)
    let btrfs_sb_block = 0x10000 / block_size;
    if btrfs_sb_block < total_blocks {
        blocks.insert(btrfs_sb_block);
    }

    // Try to detect ext from block 0 data if block_size >= 2048
    // (superblock at byte 1024 would be within block 0)
    if block_size >= 2048 && boot_sector.len() >= 1024 + 0x3A {
        let magic = u16::from_le_bytes([boot_sector[1024 + 0x38], boot_sector[1024 + 0x39]]);
        if magic == 0xEF53 {
            let ext_blocks = identify_ext_metadata_blocks(boot_sector, block_size);
            for b in ext_blocks {
                blocks.insert(b);
            }
        }
    }

    blocks.into_iter().collect()
}

/// Identify ext2/3/4 metadata blocks from a data buffer that contains the superblock.
///
/// `data` must contain at least 1024 + 256 bytes (superblock at offset 1024).
/// Returns partition block indices for: superblock, group descriptor table,
/// block bitmaps, inode bitmaps, and inode tables for all block groups.
fn identify_ext_metadata_blocks(data: &[u8], block_size: u64) -> Vec<u64> {
    let mut blocks = BTreeSet::new();

    // Superblock block
    let sb_block = 1024 / block_size;
    blocks.insert(sb_block);

    let sb_off = 1024usize;
    if data.len() < sb_off + 0x60 {
        return blocks.into_iter().collect();
    }

    // Parse essential superblock fields
    let magic = u16::from_le_bytes([data[sb_off + 0x38], data[sb_off + 0x39]]);
    if magic != 0xEF53 {
        return blocks.into_iter().collect();
    }

    let blocks_count_lo = u32::from_le_bytes(data[sb_off..sb_off + 4].try_into().unwrap()) as u64;
    let _inodes_count = u32::from_le_bytes(data[sb_off..sb_off + 0x04].try_into().unwrap());
    let first_data_block =
        u32::from_le_bytes(data[sb_off + 0x14..sb_off + 0x18].try_into().unwrap()) as u64;
    let log_block_size = u32::from_le_bytes(data[sb_off + 0x18..sb_off + 0x1C].try_into().unwrap());
    let ext_block_size = 1024u64 << log_block_size;
    let blocks_per_group =
        u32::from_le_bytes(data[sb_off + 0x20..sb_off + 0x24].try_into().unwrap()) as u64;
    let inodes_per_group =
        u32::from_le_bytes(data[sb_off + 0x28..sb_off + 0x2C].try_into().unwrap());
    let inode_size = if data.len() >= sb_off + 0x5A {
        u16::from_le_bytes([data[sb_off + 0x58], data[sb_off + 0x59]]) as u64
    } else {
        128
    };

    // Check for 64-bit mode
    let incompat_flags = if data.len() >= sb_off + 0x64 {
        u32::from_le_bytes(data[sb_off + 0x60..sb_off + 0x64].try_into().unwrap())
    } else {
        0
    };
    let is_64bit = incompat_flags & 0x80 != 0;
    let desc_size = if is_64bit && data.len() >= sb_off + 0x100 {
        u16::from_le_bytes([data[sb_off + 0xFE], data[sb_off + 0xFF]]).max(32) as u64
    } else {
        32
    };

    // Total blocks (handle 64-bit)
    let blocks_count_hi = if is_64bit && data.len() >= sb_off + 0x154 {
        u32::from_le_bytes(data[sb_off + 0x150..sb_off + 0x154].try_into().unwrap()) as u64
    } else {
        0
    };
    let total_ext_blocks = blocks_count_lo | (blocks_count_hi << 32);

    if blocks_per_group == 0 || ext_block_size == 0 {
        return blocks.into_iter().collect();
    }

    let group_count = total_ext_blocks.div_ceil(blocks_per_group);

    // Group descriptor table starts at the block after the superblock block
    let gdt_start_ext_block = first_data_block + 1;
    let gdt_total_bytes = group_count * desc_size;
    let gdt_ext_blocks = gdt_total_bytes.div_ceil(ext_block_size);

    // Convert ext blocks to partition blocks
    let ext_to_partition = |ext_block: u64| -> u64 { (ext_block * ext_block_size) / block_size };

    // Add GDT blocks
    for i in 0..gdt_ext_blocks {
        let pb = ext_to_partition(gdt_start_ext_block + i);
        blocks.insert(pb);
        // If ext_block_size > block_size, we may need multiple partition blocks
        if ext_block_size > block_size {
            let end_pb = ext_to_partition(gdt_start_ext_block + i + 1);
            for b in pb..end_pb {
                blocks.insert(b);
            }
        }
    }

    // Add block bitmaps, inode bitmaps, and inode tables for first N groups.
    // Limit to avoid caching too much for huge filesystems.
    let max_groups = group_count.min(64);

    // GDT entries contain the bitmap/inode table locations.
    // If block_size >= ext_block_size, GDT may be in already-requested blocks.
    // Parse GDT entries from the data buffer if available, otherwise just
    // request blocks for the first few groups based on standard layout.
    let gdt_byte_start = gdt_start_ext_block * ext_block_size;
    let gdt_in_data = gdt_byte_start as usize;

    for g in 0..max_groups {
        let gd_offset = gdt_in_data + (g as usize) * desc_size as usize;
        if gd_offset + 12 > data.len() {
            // GDT not available in this buffer — use standard layout assumption
            // For standard ext layout: bitmap at group_start + overhead
            // We'll just add the first few blocks of each group as a fallback
            let group_start = first_data_block + g * blocks_per_group;
            // Block bitmap, inode bitmap, and first inode table block
            for offset in 0..3 {
                let pb = ext_to_partition(group_start + gdt_ext_blocks + 1 + offset);
                blocks.insert(pb);
            }
            // Inode table: ceil(inodes_per_group * inode_size / ext_block_size) blocks
            let inode_table_blocks =
                (inodes_per_group as u64 * inode_size).div_ceil(ext_block_size);
            let inode_table_start = group_start + gdt_ext_blocks + 3;
            for i in 0..inode_table_blocks.min(32) {
                let pb = ext_to_partition(inode_table_start + i);
                blocks.insert(pb);
                if ext_block_size > block_size {
                    for b in pb..ext_to_partition(inode_table_start + i + 1) {
                        blocks.insert(b);
                    }
                }
            }
            continue;
        }

        // Parse GDT entry
        let bb_lo = u32::from_le_bytes(data[gd_offset..gd_offset + 4].try_into().unwrap()) as u64;
        let ib_lo =
            u32::from_le_bytes(data[gd_offset + 4..gd_offset + 8].try_into().unwrap()) as u64;
        let it_lo =
            u32::from_le_bytes(data[gd_offset + 8..gd_offset + 12].try_into().unwrap()) as u64;

        // 64-bit high parts
        let (bb, ib, it) = if is_64bit && desc_size >= 64 && gd_offset + 0x28 <= data.len() {
            let bb_hi =
                u32::from_le_bytes(data[gd_offset + 0x20..gd_offset + 0x24].try_into().unwrap())
                    as u64;
            let ib_hi =
                u32::from_le_bytes(data[gd_offset + 0x24..gd_offset + 0x28].try_into().unwrap())
                    as u64;
            let it_hi =
                u32::from_le_bytes(data[gd_offset + 0x28..gd_offset + 0x2C].try_into().unwrap())
                    as u64;
            (
                bb_lo | (bb_hi << 32),
                ib_lo | (ib_hi << 32),
                it_lo | (it_hi << 32),
            )
        } else {
            (bb_lo, ib_lo, it_lo)
        };

        // Block bitmap
        let pb = ext_to_partition(bb);
        blocks.insert(pb);
        if ext_block_size > block_size {
            for b in pb..ext_to_partition(bb + 1) {
                blocks.insert(b);
            }
        }

        // Inode bitmap
        let pb = ext_to_partition(ib);
        blocks.insert(pb);
        if ext_block_size > block_size {
            for b in pb..ext_to_partition(ib + 1) {
                blocks.insert(b);
            }
        }

        // Inode table
        let inode_table_blocks = (inodes_per_group as u64 * inode_size).div_ceil(ext_block_size);
        for i in 0..inode_table_blocks.min(32) {
            let pb = ext_to_partition(it + i);
            blocks.insert(pb);
            if ext_block_size > block_size {
                for b in pb..ext_to_partition(it + i + 1) {
                    blocks.insert(b);
                }
            }
        }
    }

    blocks.into_iter().collect()
}

/// Identify btrfs metadata blocks: superblock + root tree + chunk tree + FS tree nodes.
///
/// `data` must contain the superblock at its expected offset within the buffer.
/// `sb_byte_offset` is the byte offset of the superblock within the partition.
fn identify_btrfs_metadata_blocks_from_sb(sb_data: &[u8], block_size: u64) -> Vec<u64> {
    let mut blocks = BTreeSet::new();

    // Superblock at partition byte offset 0x10000
    let sb_byte = 0x10000u64;
    let sb_partition_block = sb_byte / block_size;
    blocks.insert(sb_partition_block);
    // Superblock is 4096 bytes, may span multiple partition blocks
    let sb_end_block = (sb_byte + 4096 - 1) / block_size;
    for b in sb_partition_block..=sb_end_block {
        blocks.insert(b);
    }

    if sb_data.len() < 0x60 {
        return blocks.into_iter().collect();
    }

    // Validate magic
    if sb_data.len() >= 0x48 && &sb_data[0x40..0x48] != b"_BHRfS_M" {
        return blocks.into_iter().collect();
    }

    // Parse key superblock fields
    let root_tree_root = u64::from_le_bytes(sb_data[0x50..0x58].try_into().unwrap());
    let chunk_root = u64::from_le_bytes(sb_data[0x58..0x60].try_into().unwrap());
    let total_bytes = u64::from_le_bytes(sb_data[0x70..0x78].try_into().unwrap());
    let node_size = if sb_data.len() >= 0x98 {
        u32::from_le_bytes(sb_data[0x94..0x98].try_into().unwrap()) as u64
    } else {
        16384
    };

    // Parse sys_chunk_array to build initial chunk map for logical→physical translation
    let sys_chunk_size = if sb_data.len() >= 0xA4 {
        u32::from_le_bytes(sb_data[0xA0..0xA4].try_into().unwrap()) as usize
    } else {
        0
    };

    let mut chunk_map: Vec<(u64, u64, u64)> = Vec::new(); // (logical, physical, length)
    if sb_data.len() >= 0x32B + sys_chunk_size {
        let sys_data = &sb_data[0x32B..0x32B + sys_chunk_size];
        let mut off = 0;
        while off + 17 < sys_data.len() {
            let logical = u64::from_le_bytes(sys_data[off + 9..off + 17].try_into().unwrap());
            off += 17;
            if off + 48 > sys_data.len() {
                break;
            }
            let length = u64::from_le_bytes(sys_data[off..off + 8].try_into().unwrap());
            let num_stripes =
                u16::from_le_bytes(sys_data[off + 44..off + 46].try_into().unwrap()) as usize;
            off += 48;
            if num_stripes == 0 || off + 32 > sys_data.len() {
                break;
            }
            let physical = u64::from_le_bytes(sys_data[off + 8..off + 16].try_into().unwrap());
            off += num_stripes * 32;
            chunk_map.push((logical, physical, length));
        }
        chunk_map.sort_by_key(|&(l, _, _)| l);
    }

    // Helper: translate logical address to physical using the chunk map
    let logical_to_physical = |logical: u64| -> Option<u64> {
        // Binary search
        let idx = match chunk_map.binary_search_by(|&(l, _, _)| l.cmp(&logical)) {
            Ok(i) => i,
            Err(0) => return None,
            Err(i) => i - 1,
        };
        let (l, p, len) = chunk_map[idx];
        if logical >= l && logical < l + len {
            Some(p + (logical - l))
        } else {
            None
        }
    };

    // Add blocks for a tree node (node_size bytes at a physical offset)
    let add_node_blocks = |blocks: &mut BTreeSet<u64>, physical: u64| {
        if physical >= total_bytes {
            return;
        }
        let start_block = physical / block_size;
        let end_block = (physical + node_size - 1) / block_size;
        for b in start_block..=end_block {
            blocks.insert(b);
        }
    };

    // Root tree root node
    if let Some(phys) = logical_to_physical(root_tree_root) {
        add_node_blocks(&mut blocks, phys);
    }

    // Chunk tree root node
    if let Some(phys) = logical_to_physical(chunk_root) {
        add_node_blocks(&mut blocks, phys);
    }

    blocks.into_iter().collect()
}

/// Discover additional directory/metadata blocks for Linux filesystems
/// after the initial metadata blocks have been cached.
fn discover_linux_directory_blocks(cache: &PartcloneBlockCache, block_size: u32) -> Vec<u64> {
    let bs = block_size as u64;
    if bs == 0 {
        return vec![];
    }

    // Try ext: check for superblock magic at byte 1024
    let sb_block = 1024 / bs;
    if let Some(sb_data) = cache.blocks.get(&sb_block) {
        let sb_off_in_block = (1024 % bs) as usize;
        if sb_data.len() >= sb_off_in_block + 0x3A {
            let magic = u16::from_le_bytes([
                sb_data[sb_off_in_block + 0x38],
                sb_data[sb_off_in_block + 0x39],
            ]);
            if magic == 0xEF53 {
                // Reconstruct full superblock data from cache for identify_ext_metadata_blocks
                // We need data starting from byte 0 with the superblock at offset 1024
                let needed_bytes = 1024 + 1024; // superblock starts at 1024, is 1024 bytes
                let mut buf = vec![0u8; needed_bytes];
                // Fill from cached blocks
                let mut filled = 0u64;
                while filled < needed_bytes as u64 {
                    let block_idx = filled / bs;
                    let block_off = (filled % bs) as usize;
                    if let Some(block_data) = cache.blocks.get(&block_idx) {
                        let avail =
                            (block_data.len() - block_off).min(needed_bytes - filled as usize);
                        buf[filled as usize..filled as usize + avail]
                            .copy_from_slice(&block_data[block_off..block_off + avail]);
                        filled += avail as u64;
                    } else {
                        filled += (bs - filled % bs).min(needed_bytes as u64 - filled);
                    }
                }
                return identify_ext_metadata_blocks(&buf, bs);
            }
        }
    }

    // Try btrfs: check for superblock at byte 0x10000
    let btrfs_sb_block = 0x10000 / bs;
    if let Some(sb_block_data) = cache.blocks.get(&btrfs_sb_block) {
        let sb_off_in_block = (0x10000 % bs) as usize;
        if sb_block_data.len() >= sb_off_in_block + 0x48
            && &sb_block_data[sb_off_in_block + 0x40..sb_off_in_block + 0x48] == b"_BHRfS_M"
        {
            // Reconstruct 4096-byte superblock from cached blocks
            let mut sb_buf = vec![0u8; 4096];
            let mut filled = 0usize;
            let mut byte_pos = 0x10000u64;
            while filled < 4096 {
                let block_idx = byte_pos / bs;
                let block_off = (byte_pos % bs) as usize;
                if let Some(block_data) = cache.blocks.get(&block_idx) {
                    let avail = (block_data.len() - block_off).min(4096 - filled);
                    sb_buf[filled..filled + avail]
                        .copy_from_slice(&block_data[block_off..block_off + avail]);
                    filled += avail;
                    byte_pos += avail as u64;
                } else {
                    break;
                }
            }
            if filled >= 0x60 {
                return identify_btrfs_metadata_blocks_from_sb(&sb_buf, bs);
            }
        }
    }

    vec![]
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
