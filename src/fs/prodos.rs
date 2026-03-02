use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};
use super::hfs_common::{bitmap_clear_bit_be, bitmap_find_set_bit_be, bitmap_set_bit_be};
use super::CompactResult;

const BLOCK_SIZE: u64 = 512;

// ─────────────────────────────── header ──────────────────────────────────────

struct ProDosVolumeHeader {
    name: String,
    total_blocks: u16,
    bitmap_pointer: u16,
    free_blocks: u16,
}

// ─────────────────────────────── filesystem ──────────────────────────────────

/// ProDOS filesystem reader for volume-directory browsing and compaction.
pub struct ProDosFilesystem<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    header: ProDosVolumeHeader,
    /// Lazy-loaded bitmap for editing operations.
    bitmap: Option<Vec<u8>>,
}

impl<R: Read + Seek + Send> ProDosFilesystem<R> {
    /// Open a ProDOS filesystem at the given partition offset.
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        let header = read_volume_header(&mut reader, partition_offset)?;
        Ok(Self {
            reader,
            partition_offset,
            header,
            bitmap: None,
        })
    }
}

impl<R: Read + Seek + Send> Filesystem for ProDosFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry {
            name: self.header.name.clone(),
            path: "/".into(),
            entry_type: EntryType::Directory,
            size: 0,
            location: 2, // Volume Directory Key Block is always at block 2
            modified: None,
            type_code: None,
            creator_code: None,
            symlink_target: None,
            special_type: None,
            mode: None,
            uid: None,
            gid: None,
            resource_fork_size: None,
        })
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        list_prodos_directory(&mut self.reader, self.partition_offset, entry)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        if entry.size == 0 || max_bytes == 0 {
            return Ok(Vec::new());
        }
        let key_ptr = entry.location as u16;
        let eof = entry.size as usize;
        // storage_type is stored in entry.mode (set by list_directory).
        // Fall back to inference from eof if mode is absent.
        let storage_type = entry.mode.unwrap_or_else(|| infer_storage_type(eof)) as u8;

        let data = match storage_type {
            1 => read_seedling(&mut self.reader, self.partition_offset, key_ptr, eof)?,
            2 => read_sapling(&mut self.reader, self.partition_offset, key_ptr, eof)?,
            3 => read_tree(&mut self.reader, self.partition_offset, key_ptr, eof)?,
            _ => read_seedling(&mut self.reader, self.partition_offset, key_ptr, eof)?,
        };

        if data.len() > max_bytes {
            Ok(data[..max_bytes].to_vec())
        } else {
            Ok(data)
        }
    }

    fn volume_label(&self) -> Option<&str> {
        Some(&self.header.name)
    }

    fn fs_type(&self) -> &str {
        "ProDOS"
    }

    fn total_size(&self) -> u64 {
        self.header.total_blocks as u64 * BLOCK_SIZE
    }

    fn used_size(&self) -> u64 {
        (self.header.total_blocks as u64).saturating_sub(self.header.free_blocks as u64)
            * BLOCK_SIZE
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        let bitmap = read_bitmap(&mut self.reader, self.partition_offset, &self.header)?;
        let total = self.header.total_blocks as u32;
        let mut last_used = 0u32;
        for block in 0..total {
            if is_block_used(&bitmap, block) {
                last_used = block;
            }
        }
        Ok((last_used as u64 + 1) * BLOCK_SIZE)
    }
}

// ─────────────────────────────── editing helpers ─────────────────────────────

impl<R: Read + Write + Seek + Send> ProDosFilesystem<R> {
    /// Lazy-load the bitmap into memory if not already loaded.
    fn ensure_bitmap(&mut self) -> Result<(), FilesystemError> {
        if self.bitmap.is_none() {
            let bm = read_bitmap(&mut self.reader, self.partition_offset, &self.header)?;
            self.bitmap = Some(bm);
        }
        Ok(())
    }

    /// Read a single 512-byte block.
    fn read_block(&mut self, block: u16) -> Result<[u8; 512], FilesystemError> {
        let offset = self.partition_offset + block as u64 * BLOCK_SIZE;
        self.reader
            .seek(SeekFrom::Start(offset))
            .map_err(FilesystemError::Io)?;
        let mut buf = [0u8; 512];
        self.reader
            .read_exact(&mut buf)
            .map_err(FilesystemError::Io)?;
        Ok(buf)
    }

    /// Write a single 512-byte block.
    fn write_block(&mut self, block: u16, data: &[u8; 512]) -> Result<(), FilesystemError> {
        let offset = self.partition_offset + block as u64 * BLOCK_SIZE;
        self.reader
            .seek(SeekFrom::Start(offset))
            .map_err(FilesystemError::Io)?;
        self.reader.write_all(data).map_err(FilesystemError::Io)?;
        Ok(())
    }

    /// Allocate a free block: find SET bit (free), CLEAR it (mark used).
    fn allocate_block(&mut self) -> Result<u16, FilesystemError> {
        self.ensure_bitmap()?;
        let bm = self.bitmap.as_mut().unwrap();
        let total = self.header.total_blocks as u32;
        let bit = bitmap_find_set_bit_be(bm, total)
            .ok_or_else(|| FilesystemError::DiskFull("no free blocks available".into()))?;
        bitmap_clear_bit_be(bm, bit); // mark used
        self.header.free_blocks = self.header.free_blocks.saturating_sub(1);
        Ok(bit as u16)
    }

    /// Free a block: SET the bit (mark free).
    fn free_block(&mut self, block: u16) -> Result<(), FilesystemError> {
        self.ensure_bitmap()?;
        let bm = self.bitmap.as_mut().unwrap();
        bitmap_set_bit_be(bm, block as u32); // mark free
        self.header.free_blocks = self.header.free_blocks.saturating_add(1);
        Ok(())
    }

    /// Flush the in-memory bitmap to disk.
    fn write_bitmap_to_disk(&mut self) -> Result<(), FilesystemError> {
        if let Some(ref bm) = self.bitmap {
            let bitmap_blocks = ((self.header.total_blocks as u32 + 4095) / 4096) as usize;
            for i in 0..bitmap_blocks {
                let offset = self.partition_offset
                    + (self.header.bitmap_pointer as u64 + i as u64) * BLOCK_SIZE;
                self.reader
                    .seek(SeekFrom::Start(offset))
                    .map_err(FilesystemError::Io)?;
                let start = i * 512;
                let end = (start + 512).min(bm.len());
                let mut block = [0u8; 512];
                if start < bm.len() {
                    block[..end - start].copy_from_slice(&bm[start..end]);
                }
                self.reader.write_all(&block).map_err(FilesystemError::Io)?;
            }
        }
        Ok(())
    }

    /// Re-read the volume directory key block header, patch total_blocks, and write back.
    /// This keeps the on-disk free_blocks consistent with bitmap changes.
    fn write_volume_header_to_disk(&mut self) -> Result<(), FilesystemError> {
        // We don't have an on-disk free_blocks field in the standard ProDOS volume
        // directory header, but total_blocks is at bytes 33-34. The bitmap IS the
        // source of truth for free/used. We just need to make sure the key block is
        // consistent. Nothing extra to patch beyond the bitmap.
        Ok(())
    }

    /// Flush all metadata: bitmap + volume header + underlying writer.
    fn do_sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.write_bitmap_to_disk()?;
        self.write_volume_header_to_disk()?;
        self.reader.flush().map_err(FilesystemError::Io)?;
        Ok(())
    }

    // ── Directory helpers ────────────────────────────────────────────────

    /// Find a directory entry by name (case-insensitive) in a directory chain.
    /// Returns `(block_num, slot_index)` if found.
    fn find_dir_entry(
        &mut self,
        dir_key_block: u16,
        name: &str,
    ) -> Result<Option<(u16, usize)>, FilesystemError> {
        let upper = name.to_ascii_uppercase();
        let mut block_num = dir_key_block;

        while block_num != 0 {
            let block = self.read_block(block_num)?;
            let next_block = u16::from_le_bytes([block[2], block[3]]);

            for i in 0..13usize {
                let eo = 4 + i * 39;
                if eo + 39 > 512 {
                    break;
                }
                let type_nibble = block[eo] >> 4;
                let name_len = (block[eo] & 0xF) as usize;
                // Skip deleted, volume dir header, subdir header
                if type_nibble == 0 || type_nibble == 0xF || type_nibble == 0xD {
                    continue;
                }
                if name_len == 0 || name_len > 15 {
                    continue;
                }
                let entry_name: String = block[eo + 1..eo + 1 + name_len]
                    .iter()
                    .map(|&b| (b as char).to_ascii_uppercase())
                    .collect();
                if entry_name == upper {
                    return Ok(Some((block_num, i)));
                }
            }
            block_num = next_block;
        }
        Ok(None)
    }

    /// Find a free slot in a directory chain. If no free slot exists, allocate a
    /// new directory block and link it into the chain. Returns `(block_num, slot_index)`.
    fn find_free_dir_slot(&mut self, dir_key_block: u16) -> Result<(u16, usize), FilesystemError> {
        let mut block_num = dir_key_block;
        let mut last_block = dir_key_block;

        while block_num != 0 {
            let block = self.read_block(block_num)?;
            let next_block = u16::from_le_bytes([block[2], block[3]]);

            for i in 0..13usize {
                let eo = 4 + i * 39;
                if eo + 39 > 512 {
                    break;
                }
                let type_nibble = block[eo] >> 4;
                // Skip the header entry (slot 0 on the key block)
                if block_num == dir_key_block && i == 0 {
                    continue;
                }
                // Empty slot: type nibble == 0
                if type_nibble == 0 {
                    return Ok((block_num, i));
                }
            }
            last_block = block_num;
            block_num = next_block;
        }

        // No free slot — allocate a new block and link it to the chain.
        let new_block = self.allocate_block()?;
        let mut new_data = [0u8; 512];
        // prev_block = last_block, next_block = 0
        let prev = last_block.to_le_bytes();
        new_data[0] = prev[0];
        new_data[1] = prev[1];
        self.write_block(new_block, &new_data)?;

        // Update the last block's next pointer.
        let mut last_data = self.read_block(last_block)?;
        let nb = new_block.to_le_bytes();
        last_data[2] = nb[0];
        last_data[3] = nb[1];
        self.write_block(last_block, &last_data)?;

        // Slot 0 of the new block is available (it's not a header).
        Ok((new_block, 0))
    }

    /// Write a 39-byte entry at a given block and slot.
    fn write_dir_entry(
        &mut self,
        block_num: u16,
        slot: usize,
        entry: &[u8; 39],
    ) -> Result<(), FilesystemError> {
        let mut block = self.read_block(block_num)?;
        let eo = 4 + slot * 39;
        block[eo..eo + 39].copy_from_slice(entry);
        self.write_block(block_num, &block)
    }

    /// Clear a directory entry (zero out 39 bytes).
    fn clear_dir_entry(&mut self, block_num: u16, slot: usize) -> Result<(), FilesystemError> {
        let zero = [0u8; 39];
        self.write_dir_entry(block_num, slot, &zero)
    }

    /// Update the file_count field in the directory header (key block, slot 0, bytes 29-30).
    fn update_dir_file_count(
        &mut self,
        dir_key_block: u16,
        delta: i16,
    ) -> Result<(), FilesystemError> {
        let mut block = self.read_block(dir_key_block)?;
        // file_count is at entry offset 4 (start of slot 0) + 29 = byte 33 in the block... wait.
        // Actually: slot 0 starts at offset 4 in the block. The header entry has
        // file_count at bytes 25-26 within the 39-byte entry.
        // So absolute position = 4 + 25 = 29, 4 + 26 = 30.
        let fc = u16::from_le_bytes([block[4 + 25], block[4 + 26]]);
        let new_fc = (fc as i16 + delta).max(0) as u16;
        let bytes = new_fc.to_le_bytes();
        block[4 + 25] = bytes[0];
        block[4 + 26] = bytes[1];
        self.write_block(dir_key_block, &block)
    }

    // ── Data write helpers ───────────────────────────────────────────────

    /// Write a seedling file (≤512 bytes): allocate 1 block, write data.
    fn write_seedling(&mut self, data: &[u8]) -> Result<u16, FilesystemError> {
        let block_num = self.allocate_block()?;
        let mut buf = [0u8; 512];
        let len = data.len().min(512);
        buf[..len].copy_from_slice(&data[..len]);
        self.write_block(block_num, &buf)?;
        Ok(block_num)
    }

    /// Write a sapling file (≤128KB): index block + up to 256 data blocks.
    /// Returns `(index_block, blocks_used)`.
    fn write_sapling(&mut self, data: &[u8]) -> Result<(u16, u16), FilesystemError> {
        let data_block_count = (data.len() + 511) / 512;
        if data_block_count > 256 {
            return Err(FilesystemError::InvalidData(
                "sapling file too large (>128KB)".into(),
            ));
        }

        let index_block = self.allocate_block()?;
        let mut index_buf = [0u8; 512];
        let mut blocks_used = 1u16; // count the index block itself

        for i in 0..data_block_count {
            let start = i * 512;
            let end = (start + 512).min(data.len());
            let db = self.allocate_block()?;
            let mut buf = [0u8; 512];
            buf[..end - start].copy_from_slice(&data[start..end]);
            self.write_block(db, &buf)?;

            // Index block layout: low bytes at 0-255, high bytes at 256-511.
            index_buf[i] = db as u8;
            index_buf[256 + i] = (db >> 8) as u8;
            blocks_used += 1;
        }

        self.write_block(index_block, &index_buf)?;
        Ok((index_block, blocks_used))
    }

    /// Write a tree file (≤16MB): master index + up to 128 index blocks + data blocks.
    /// Returns `(master_block, blocks_used)`.
    fn write_tree(&mut self, data: &[u8]) -> Result<(u16, u16), FilesystemError> {
        let total_data_blocks = (data.len() + 511) / 512;
        let index_block_count = (total_data_blocks + 255) / 256;
        if index_block_count > 128 {
            return Err(FilesystemError::InvalidData(
                "tree file too large (>16MB)".into(),
            ));
        }

        let master_block = self.allocate_block()?;
        let mut master_buf = [0u8; 512];
        let mut blocks_used = 1u16; // master

        for idx_i in 0..index_block_count {
            let index_block = self.allocate_block()?;
            let mut index_buf = [0u8; 512];
            blocks_used += 1;

            let data_start = idx_i * 256;
            let data_end = total_data_blocks.min(data_start + 256);

            for j in 0..(data_end - data_start) {
                let byte_start = (data_start + j) * 512;
                let byte_end = (byte_start + 512).min(data.len());
                let db = self.allocate_block()?;
                let mut buf = [0u8; 512];
                if byte_start < data.len() {
                    buf[..byte_end - byte_start].copy_from_slice(&data[byte_start..byte_end]);
                }
                self.write_block(db, &buf)?;
                index_buf[j] = db as u8;
                index_buf[256 + j] = (db >> 8) as u8;
                blocks_used += 1;
            }

            self.write_block(index_block, &index_buf)?;
            master_buf[idx_i] = index_block as u8;
            master_buf[256 + idx_i] = (index_block >> 8) as u8;
        }

        self.write_block(master_block, &master_buf)?;
        Ok((master_block, blocks_used))
    }

    /// Free all blocks belonging to a file (seedling/sapling/tree).
    fn free_file_blocks(
        &mut self,
        key_pointer: u16,
        storage_type: u8,
    ) -> Result<(), FilesystemError> {
        if key_pointer == 0 {
            return Ok(());
        }
        match storage_type {
            1 => {
                // Seedling: just one data block.
                self.free_block(key_pointer)?;
            }
            2 => {
                // Sapling: index block + data blocks.
                let index = read_index_block(&mut self.reader, self.partition_offset, key_pointer)?;
                for &db in &index {
                    if db != 0 {
                        self.free_block(db)?;
                    }
                }
                self.free_block(key_pointer)?;
            }
            3 => {
                // Tree: master index + index blocks + data blocks.
                let master =
                    read_index_block(&mut self.reader, self.partition_offset, key_pointer)?;
                for &ib in &master[..128] {
                    if ib != 0 {
                        let index = read_index_block(&mut self.reader, self.partition_offset, ib)?;
                        for &db in &index {
                            if db != 0 {
                                self.free_block(db)?;
                            }
                        }
                        self.free_block(ib)?;
                    }
                }
                self.free_block(key_pointer)?;
            }
            _ => {
                // Unknown storage type — just free the key block.
                self.free_block(key_pointer)?;
            }
        }
        Ok(())
    }

    /// Free all blocks in a directory chain (but NOT the files within it).
    fn free_dir_blocks(&mut self, dir_key_block: u16) -> Result<(), FilesystemError> {
        let mut block_num = dir_key_block;
        while block_num != 0 {
            let block = self.read_block(block_num)?;
            let next = u16::from_le_bytes([block[2], block[3]]);
            self.free_block(block_num)?;
            block_num = next;
        }
        Ok(())
    }

    /// Check if a directory is empty (has no file or subdir entries).
    fn is_directory_empty(&mut self, dir_key_block: u16) -> Result<bool, FilesystemError> {
        let mut block_num = dir_key_block;
        while block_num != 0 {
            let block = self.read_block(block_num)?;
            let next = u16::from_le_bytes([block[2], block[3]]);
            for i in 0..13usize {
                let eo = 4 + i * 39;
                if eo + 39 > 512 {
                    break;
                }
                // Skip header entries
                if block_num == dir_key_block && i == 0 {
                    continue;
                }
                let type_nibble = block[eo] >> 4;
                if type_nibble != 0 && type_nibble != 0xD {
                    return Ok(false);
                }
            }
            block_num = next;
        }
        Ok(true)
    }
}

// ─────────────────────────────── entry builders ─────────────────────────────

/// Build a 39-byte ProDOS file directory entry.
fn build_file_entry_bytes(
    name: &str,
    file_type: u8,
    aux_type: u16,
    storage_type: u8,
    key_ptr: u16,
    blocks_used: u16,
    eof: u32,
) -> [u8; 39] {
    let mut e = [0u8; 39];
    let name_bytes = name.as_bytes();
    let name_len = name_bytes.len().min(15) as u8;
    e[0] = (storage_type << 4) | name_len;
    e[1..1 + name_len as usize].copy_from_slice(&name_bytes[..name_len as usize]);
    e[16] = file_type;
    let kp = key_ptr.to_le_bytes();
    e[17] = kp[0];
    e[18] = kp[1];
    let bu = blocks_used.to_le_bytes();
    e[19] = bu[0];
    e[20] = bu[1];
    e[21] = eof as u8;
    e[22] = (eof >> 8) as u8;
    e[23] = (eof >> 16) as u8;
    let (date, time) = make_prodos_datetime_now();
    let cd = date.to_le_bytes();
    e[24] = cd[0];
    e[25] = cd[1];
    let ct = time.to_le_bytes();
    e[26] = ct[0];
    e[27] = ct[1];
    // version = 0, min_version = 0 at bytes 28-29
    // access = 0xC3 (unlocked: read, write, destroy enabled) at byte 30
    e[30] = 0xC3;
    let at = aux_type.to_le_bytes();
    e[31] = at[0];
    e[32] = at[1];
    // Modified date/time = same as creation
    e[33] = cd[0];
    e[34] = cd[1];
    e[35] = ct[0];
    e[36] = ct[1];
    // header_pointer (bytes 37-38) will be set to 0 here; caller can patch if needed.
    e
}

/// Build a 39-byte subdirectory entry (type nibble 0xE) for the parent directory.
fn build_subdir_entry_bytes(name: &str, key_ptr: u16) -> [u8; 39] {
    let mut e = [0u8; 39];
    let name_bytes = name.as_bytes();
    let name_len = name_bytes.len().min(15) as u8;
    e[0] = (0xE << 4) | name_len;
    e[1..1 + name_len as usize].copy_from_slice(&name_bytes[..name_len as usize]);
    e[16] = 0x0F; // file_type = DIR
    let kp = key_ptr.to_le_bytes();
    e[17] = kp[0];
    e[18] = kp[1];
    // blocks_used = 1 (the key block itself)
    e[19] = 1;
    e[20] = 0;
    let (date, time) = make_prodos_datetime_now();
    let cd = date.to_le_bytes();
    e[24] = cd[0];
    e[25] = cd[1];
    let ct = time.to_le_bytes();
    e[26] = ct[0];
    e[27] = ct[1];
    e[30] = 0xC3; // access
    e[33] = cd[0];
    e[34] = cd[1];
    e[35] = ct[0];
    e[36] = ct[1];
    e
}

/// Build a 39-byte subdirectory header entry (type nibble 0xD) for slot 0 of a new dir block.
fn build_subdir_header_bytes(name: &str, parent_key_block: u16, parent_entry_num: u8) -> [u8; 39] {
    let mut e = [0u8; 39];
    let name_bytes = name.as_bytes();
    let name_len = name_bytes.len().min(15) as u8;
    e[0] = (0xD << 4) | name_len;
    e[1..1 + name_len as usize].copy_from_slice(&name_bytes[..name_len as usize]);
    // Bytes 16-19: reserved (0x76 is sometimes used as magic, but 0 is fine)
    e[20] = 0x75; // ProDOS 8 subdir marker
    let (date, time) = make_prodos_datetime_now();
    let cd = date.to_le_bytes();
    e[24] = cd[0];
    e[25] = cd[1];
    let ct = time.to_le_bytes();
    e[26] = ct[0];
    e[27] = ct[1];
    e[27] = 39; // entry_length
    e[28] = 13; // entries_per_block
                // file_count = 0 at bytes 29-30 (LE u16)
                // parent_pointer at bytes 31-32
    let pp = parent_key_block.to_le_bytes();
    e[31] = pp[0];
    e[32] = pp[1];
    // parent_entry_number at byte 33
    e[33] = parent_entry_num;
    // parent_entry_length at byte 34
    e[34] = 39;
    e
}

/// Generate current date/time in ProDOS format.
/// Returns `(date_word, time_word)`.
fn make_prodos_datetime_now() -> (u16, u16) {
    // Use a fixed reasonable date for reproducibility in tests and simplicity.
    // In a real scenario you'd call system time. For now: 2024-01-15 12:00.
    #[cfg(test)]
    {
        let year: u16 = 24; // 2024 (year < 70 → 2000+year)
        let month: u16 = 1;
        let day: u16 = 15;
        let date = (year << 9) | (month << 5) | day;
        let time = (12u16 << 8) | 0;
        (date, time)
    }
    #[cfg(not(test))]
    {
        use std::time::SystemTime;
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        // Simple conversion from Unix epoch seconds.
        let days = (now / 86400) as i64;
        let time_of_day = now % 86400;
        let hour = (time_of_day / 3600) as u16;
        let minute = ((time_of_day % 3600) / 60) as u16;

        // Convert days since 1970-01-01 to year/month/day.
        let (year, month, day) = days_to_ymd(days);
        let prodos_year = (year % 100) as u16;
        let date = (prodos_year << 9) | ((month as u16) << 5) | (day as u16);
        let time = (hour << 8) | minute;
        (date, time)
    }
}

/// Convert days since Unix epoch to (year, month, day).
#[cfg(not(test))]
fn days_to_ymd(days: i64) -> (i32, u32, u32) {
    // Civil calendar algorithm from Howard Hinnant.
    let z = days + 719468;
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    (year as i32, m, d)
}

/// Validate a ProDOS filename: 1-15 chars, A-Z/0-9/period, first char must be a letter.
/// Returns the uppercase name.
fn validate_prodos_name(name: &str) -> Result<String, FilesystemError> {
    let upper = name.to_ascii_uppercase();
    let bytes = upper.as_bytes();
    if bytes.is_empty() || bytes.len() > 15 {
        return Err(FilesystemError::InvalidData(format!(
            "ProDOS name must be 1-15 characters, got '{name}'"
        )));
    }
    if !bytes[0].is_ascii_alphabetic() {
        return Err(FilesystemError::InvalidData(format!(
            "ProDOS name must start with a letter, got '{name}'"
        )));
    }
    for &b in bytes {
        if !b.is_ascii_alphanumeric() && b != b'.' {
            return Err(FilesystemError::InvalidData(format!(
                "ProDOS name contains invalid character in '{name}'"
            )));
        }
    }
    Ok(upper)
}

/// Parse a file type from CreateFileOptions.type_code (e.g. "$06" → 0x06).
/// Defaults to 0x06 (BIN) if not set or unparseable.
fn parse_file_type(opts: &CreateFileOptions) -> u8 {
    if let Some(ref tc) = opts.type_code {
        let s = tc.trim().trim_start_matches('$');
        u8::from_str_radix(s, 16).unwrap_or(0x06)
    } else {
        0x06 // BIN
    }
}

// ─────────────────────────────── EditableFilesystem ──────────────────────────

impl<R: Read + Write + Seek + Send> EditableFilesystem for ProDosFilesystem<R> {
    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn std::io::Read,
        data_len: u64,
        options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let validated_name = validate_prodos_name(name)?;
        let dir_key_block = parent.location as u16;

        // Check for duplicate
        if self
            .find_dir_entry(dir_key_block, &validated_name)?
            .is_some()
        {
            return Err(FilesystemError::AlreadyExists(validated_name));
        }

        // Max ProDOS file size: 16MB
        if data_len > 16 * 1024 * 1024 {
            return Err(FilesystemError::InvalidData(
                "ProDOS file size cannot exceed 16MB".into(),
            ));
        }

        // Read all data
        let mut file_data = vec![0u8; data_len as usize];
        data.read_exact(&mut file_data)
            .map_err(FilesystemError::Io)?;

        let eof = file_data.len() as u32;
        let file_type = parse_file_type(options);

        // Choose storage type and write blocks
        let (key_ptr, blocks_used, storage_type) = if file_data.len() <= 512 {
            let kp = self.write_seedling(&file_data)?;
            (kp, 1u16, 1u8)
        } else if file_data.len() <= 256 * 512 {
            let (kp, bu) = self.write_sapling(&file_data)?;
            (kp, bu, 2u8)
        } else {
            let (kp, bu) = self.write_tree(&file_data)?;
            (kp, bu, 3u8)
        };

        // Build directory entry
        let entry_bytes = build_file_entry_bytes(
            &validated_name,
            file_type,
            0,
            storage_type,
            key_ptr,
            blocks_used,
            eof,
        );

        // Find slot and write
        let (block_num, slot) = self.find_free_dir_slot(dir_key_block)?;
        self.write_dir_entry(block_num, slot, &entry_bytes)?;
        self.update_dir_file_count(dir_key_block, 1)?;
        self.do_sync_metadata()?;

        let path = if parent.path == "/" {
            format!("/{validated_name}")
        } else {
            format!("{}/{validated_name}", parent.path)
        };

        let mut fe = FileEntry::new_file(validated_name, path, eof as u64, key_ptr as u64);
        fe.type_code = Some(format!("${file_type:02X} {}", prodos_type_name(file_type)));
        fe.mode = Some(storage_type as u32);
        Ok(fe)
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let validated_name = validate_prodos_name(name)?;
        let parent_key_block = parent.location as u16;

        // Check for duplicate
        if self
            .find_dir_entry(parent_key_block, &validated_name)?
            .is_some()
        {
            return Err(FilesystemError::AlreadyExists(validated_name));
        }

        // Allocate key block for new subdirectory
        let new_key_block = self.allocate_block()?;

        // Find a slot in the parent first so we know the entry number
        let (entry_block, entry_slot) = self.find_free_dir_slot(parent_key_block)?;

        // Build subdirectory key block with header at slot 0
        let header = build_subdir_header_bytes(&validated_name, parent_key_block, entry_slot as u8);
        let mut new_block_data = [0u8; 512];
        // prev_block = 0, next_block = 0 (bytes 0-3)
        // Header at slot 0 (offset 4)
        new_block_data[4..4 + 39].copy_from_slice(&header);
        self.write_block(new_key_block, &new_block_data)?;

        // Build parent directory entry (type 0xE)
        let subdir_entry = build_subdir_entry_bytes(&validated_name, new_key_block);
        self.write_dir_entry(entry_block, entry_slot, &subdir_entry)?;
        self.update_dir_file_count(parent_key_block, 1)?;
        self.do_sync_metadata()?;

        let path = if parent.path == "/" {
            format!("/{validated_name}")
        } else {
            format!("{}/{validated_name}", parent.path)
        };

        Ok(FileEntry::new_directory(
            validated_name,
            path,
            new_key_block as u64,
        ))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        let dir_key_block = parent.location as u16;

        if entry.is_directory() {
            // Check if directory is empty
            let entry_key = entry.location as u16;
            if !self.is_directory_empty(entry_key)? {
                return Err(FilesystemError::InvalidData(format!(
                    "cannot delete non-empty directory '{}'",
                    entry.name
                )));
            }
            // Free directory chain blocks
            self.free_dir_blocks(entry_key)?;
        } else {
            // Free file data blocks
            let key_ptr = entry.location as u16;
            let storage_type = entry
                .mode
                .unwrap_or_else(|| infer_storage_type(entry.size as usize))
                as u8;
            self.free_file_blocks(key_ptr, storage_type)?;
        }

        // Find and clear the directory entry
        if let Some((block_num, slot)) = self.find_dir_entry(dir_key_block, &entry.name)? {
            self.clear_dir_entry(block_num, slot)?;
            self.update_dir_file_count(dir_key_block, -1)?;
        }

        self.do_sync_metadata()
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.do_sync_metadata()
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        // If bitmap is loaded, use in-memory count; otherwise use header.
        Ok(self.header.free_blocks as u64 * BLOCK_SIZE)
    }
}

// ─────────────────────────────── compact reader ──────────────────────────────

/// Compact reader for ProDOS: emits all blocks 0..total_blocks, replacing
/// free blocks (bitmap bit=1) with zeros for better compression.
///
/// The original block layout is preserved (block N always at byte N×512),
/// so a restore from the compact image produces an immediately-usable ProDOS
/// volume with no further patching needed.
pub struct CompactProDosReader<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    total_blocks: u32,
    bitmap: Vec<u8>,
    current_block: u32,
    block_pos: u64,
}

impl<R: Read + Seek> CompactProDosReader<R> {
    pub fn new(
        mut reader: R,
        partition_offset: u64,
    ) -> Result<(Self, CompactResult), FilesystemError> {
        let header = read_volume_header(&mut reader, partition_offset)?;
        let bitmap = read_bitmap(&mut reader, partition_offset, &header)?;

        let total_blocks = header.total_blocks as u32;
        let used_blocks = total_blocks.saturating_sub(header.free_blocks as u32);
        let size = total_blocks as u64 * BLOCK_SIZE;

        let result = CompactResult {
            original_size: size,
            compacted_size: size,
            data_size: used_blocks as u64 * BLOCK_SIZE,
            clusters_used: used_blocks,
        };

        Ok((
            CompactProDosReader {
                reader,
                partition_offset,
                total_blocks,
                bitmap,
                current_block: 0,
                block_pos: 0,
            },
            result,
        ))
    }
}

impl<R: Read + Seek> Read for CompactProDosReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.current_block >= self.total_blocks {
            return Ok(0);
        }

        let remaining_in_block = BLOCK_SIZE - self.block_pos;
        let to_read = (remaining_in_block as usize).min(buf.len());

        let n = if is_block_used(&self.bitmap, self.current_block) {
            let offset =
                self.partition_offset + self.current_block as u64 * BLOCK_SIZE + self.block_pos;
            self.reader
                .seek(SeekFrom::Start(offset))
                .map_err(std::io::Error::other)?;
            self.reader.read(&mut buf[..to_read])?
        } else {
            // Free block — zero fill so free space compresses away.
            buf[..to_read].fill(0);
            to_read
        };

        self.block_pos += n as u64;
        if self.block_pos >= BLOCK_SIZE {
            self.block_pos = 0;
            self.current_block += 1;
        }

        Ok(n)
    }
}

// ─────────────────────────────── resize ──────────────────────────────────────

/// Resize a ProDOS filesystem in place.
///
/// Updates `total_blocks` in the volume directory key block and, when growing,
/// extends the volume bitmap so new blocks are marked free.
pub fn resize_prodos_in_place(
    device: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    new_total_bytes: u64,
    log: &mut impl FnMut(&str),
) -> anyhow::Result<()> {
    // Read the volume directory key block (always at partition_offset + 1024).
    device.seek(SeekFrom::Start(partition_offset + 1024))?;
    let mut sector = [0u8; 512];
    device.read_exact(&mut sector)?;

    if !validate_volume_key_block(&sector) {
        log("ProDOS: not a ProDOS volume, skipping resize");
        return Ok(());
    }

    let old_total = u16::from_le_bytes([sector[33], sector[34]]);
    let bitmap_pointer = u16::from_le_bytes([sector[31], sector[32]]);

    // Count used blocks to guard against shrinking into data.
    let bitmap_blocks_needed = ((old_total as u32 + 4095) / 4096) as usize;
    let mut bitmap: Vec<u8> = Vec::with_capacity(bitmap_blocks_needed * 512);
    {
        let mut bm_block = [0u8; 512];
        for i in 0..bitmap_blocks_needed {
            let offset = partition_offset + (bitmap_pointer as u64 + i as u64) * BLOCK_SIZE;
            device.seek(SeekFrom::Start(offset))?;
            // Ignore read errors; unreadable blocks are treated as fully-used.
            let _ = device.read_exact(&mut bm_block);
            bitmap.extend_from_slice(&bm_block);
        }
    }

    let mut used_blocks = 0u32;
    for block in 0..old_total as u32 {
        if is_block_used(&bitmap, block) {
            used_blocks += 1;
        }
    }

    let new_total = ((new_total_bytes / BLOCK_SIZE) as u32).min(65535) as u16;

    if new_total == old_total {
        log("ProDOS: no resize needed");
        return Ok(());
    }

    if (new_total as u32) < used_blocks {
        log(&format!(
            "ProDOS: cannot resize to {} blocks; {} blocks are in use — skipping",
            new_total, used_blocks
        ));
        return Ok(());
    }

    // Patch total_blocks in the key block and write it back.
    let tb = new_total.to_le_bytes();
    sector[33] = tb[0];
    sector[34] = tb[1];
    device.seek(SeekFrom::Start(partition_offset + 1024))?;
    device.write_all(&sector)?;

    // If growing, mark new blocks as free (bit=1) in the bitmap.
    if new_total > old_total {
        let new_bitmap_blocks = ((new_total as u32 + 4095) / 4096) as usize;
        bitmap.resize(new_bitmap_blocks * 512, 0);

        for block in old_total as u32..new_total as u32 {
            let byte_idx = (block / 8) as usize;
            let bit_idx = 7 - (block % 8);
            if byte_idx < bitmap.len() {
                bitmap[byte_idx] |= 1 << bit_idx;
            }
        }

        for i in 0..new_bitmap_blocks {
            let offset = partition_offset + (bitmap_pointer as u64 + i as u64) * BLOCK_SIZE;
            device.seek(SeekFrom::Start(offset))?;
            let start = i * 512;
            let end = (start + 512).min(bitmap.len());
            let mut bm_block = [0u8; 512];
            if start < bitmap.len() {
                bm_block[..end - start].copy_from_slice(&bitmap[start..end]);
            }
            device.write_all(&bm_block)?;
        }
    }

    log(&format!(
        "ProDOS: resized from {} to {} blocks",
        old_total, new_total
    ));
    Ok(())
}

/// Validate a ProDOS filesystem structure and return any warnings.
pub fn validate_prodos_integrity(
    device: &mut (impl Read + Seek),
    partition_offset: u64,
    log: &mut impl FnMut(&str),
) -> anyhow::Result<Vec<String>> {
    let mut warnings = Vec::new();

    device.seek(SeekFrom::Start(partition_offset + 1024))?;
    let mut sector = [0u8; 512];
    device.read_exact(&mut sector)?;

    if !validate_volume_key_block(&sector) {
        warnings.push("ProDOS: invalid volume directory key block signature".into());
        return Ok(warnings);
    }

    let entry_length = sector[27];
    let entries_per_block = sector[28];
    let total_blocks = u16::from_le_bytes([sector[33], sector[34]]);

    if entry_length != 39 {
        warnings.push(format!(
            "ProDOS: unexpected entry_length {entry_length} (expected 39)"
        ));
    }
    if entries_per_block != 13 {
        warnings.push(format!(
            "ProDOS: unexpected entries_per_block {entries_per_block} (expected 13)"
        ));
    }
    if total_blocks == 0 {
        warnings.push("ProDOS: total_blocks is zero".into());
    }

    if warnings.is_empty() {
        log("ProDOS: filesystem structure OK");
    }

    Ok(warnings)
}

// ─────────────────────────────── internal helpers ────────────────────────────

fn read_volume_header<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
) -> Result<ProDosVolumeHeader, FilesystemError> {
    reader
        .seek(SeekFrom::Start(partition_offset + 1024))
        .map_err(FilesystemError::Io)?;
    let mut buf = [0u8; 512];
    reader.read_exact(&mut buf).map_err(FilesystemError::Io)?;

    if !validate_volume_key_block(&buf) {
        return Err(FilesystemError::Parse(
            "not a ProDOS volume (invalid volume directory key block)".into(),
        ));
    }

    let name_len = (buf[4] & 0xF) as usize;
    let name: String = buf[5..5 + name_len.min(15)]
        .iter()
        .map(|&b| b as char)
        .collect();

    let bitmap_pointer = u16::from_le_bytes([buf[31], buf[32]]);
    let total_blocks = u16::from_le_bytes([buf[33], buf[34]]);

    // Build a temporary header so read_bitmap can iterate correctly.
    let tmp = ProDosVolumeHeader {
        name: name.clone(),
        total_blocks,
        bitmap_pointer,
        free_blocks: 0,
    };
    let bitmap = read_bitmap(reader, partition_offset, &tmp)?;

    let mut free_blocks = 0u32;
    for block in 0..total_blocks as u32 {
        if !is_block_used(&bitmap, block) {
            free_blocks += 1;
        }
    }

    Ok(ProDosVolumeHeader {
        name,
        total_blocks,
        bitmap_pointer,
        free_blocks: free_blocks.min(u16::MAX as u32) as u16,
    })
}

fn read_bitmap<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    header: &ProDosVolumeHeader,
) -> Result<Vec<u8>, FilesystemError> {
    let total_blocks = header.total_blocks as u32;
    let bitmap_blocks = ((total_blocks + 4095) / 4096) as usize;
    let mut bitmap = Vec::with_capacity(bitmap_blocks * 512);
    let mut bm_block = [0u8; 512];

    for i in 0..bitmap_blocks {
        let offset = partition_offset + (header.bitmap_pointer as u64 + i as u64) * BLOCK_SIZE;
        reader
            .seek(SeekFrom::Start(offset))
            .map_err(FilesystemError::Io)?;
        reader
            .read_exact(&mut bm_block)
            .map_err(FilesystemError::Io)?;
        bitmap.extend_from_slice(&bm_block);
    }

    Ok(bitmap)
}

/// Returns `true` if the block is used (ProDOS bitmap bit == 0 means used).
fn is_block_used(bitmap: &[u8], block: u32) -> bool {
    let byte_idx = (block / 8) as usize;
    let bit_idx = 7 - (block % 8); // MSB-first ordering
    if byte_idx >= bitmap.len() {
        return false;
    }
    (bitmap[byte_idx] >> bit_idx) & 1 == 0 // 0=used, 1=free
}

/// Infer storage type from EOF when the mode field is absent.
fn infer_storage_type(eof: usize) -> u32 {
    if eof <= 512 {
        1 // seedling
    } else if eof <= 256 * 512 {
        2 // sapling
    } else {
        3 // tree
    }
}

fn validate_volume_key_block(buf: &[u8]) -> bool {
    buf.len() >= 29
        && buf[0] == 0
        && buf[1] == 0 // prev_block == 0
        && (buf[4] >> 4) == 0xF // storage_type nibble = volume dir
        && (buf[4] & 0xF) >= 1 // name_length valid
        && buf[27] == 39 // entry_length
        && buf[28] == 13 // entries_per_block
}

fn list_prodos_directory<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    entry: &FileEntry,
) -> Result<Vec<FileEntry>, FilesystemError> {
    let mut result = Vec::new();
    let mut block_num = entry.location as u16;

    while block_num != 0 {
        let block_offset = partition_offset + block_num as u64 * BLOCK_SIZE;
        reader
            .seek(SeekFrom::Start(block_offset))
            .map_err(FilesystemError::Io)?;
        let mut block = [0u8; 512];
        reader.read_exact(&mut block).map_err(FilesystemError::Io)?;

        // next_block pointer at bytes 2–3 (LE u16)
        let next_block = u16::from_le_bytes([block[2], block[3]]);

        // 13 entries × 39 bytes starting at offset 4
        for i in 0..13usize {
            let eo = 4 + i * 39;
            if eo + 39 > 512 {
                break;
            }
            let e = &block[eo..eo + 39];
            let type_nibble = e[0] >> 4;
            let name_len = (e[0] & 0xF) as usize;

            // Skip deleted (0), volume directory header (0xF), subdir header (0xD).
            if type_nibble == 0 || type_nibble == 0xF || type_nibble == 0xD {
                continue;
            }
            if name_len == 0 || name_len > 15 {
                continue;
            }

            let name: String = e[1..1 + name_len].iter().map(|&b| b as char).collect();
            let path = if entry.path == "/" {
                format!("/{name}")
            } else {
                format!("{}/{name}", entry.path)
            };

            let key_pointer = u16::from_le_bytes([e[17], e[18]]);
            let eof = (e[21] as u32) | ((e[22] as u32) << 8) | ((e[23] as u32) << 16);
            let file_type = e[16];
            let creation_date = u16::from_le_bytes([e[24], e[25]]);
            let creation_time = u16::from_le_bytes([e[26], e[27]]);
            let modified_date = u16::from_le_bytes([e[33], e[34]]);
            let modified_time = u16::from_le_bytes([e[35], e[36]]);

            let modified = parse_prodos_datetime(modified_date, modified_time)
                .or_else(|| parse_prodos_datetime(creation_date, creation_time));

            if type_nibble == 0xE {
                // Subdirectory entry: key_pointer is the subdirectory key block.
                let mut fe = FileEntry::new_directory(name, path, key_pointer as u64);
                fe.modified = modified;
                result.push(fe);
            } else {
                // Regular file: seedling (1), sapling (2), or tree (3).
                let type_name = prodos_type_name(file_type);
                let mut fe = FileEntry::new_file(name, path, eof as u64, key_pointer as u64);
                fe.modified = modified;
                fe.type_code = Some(format!("${file_type:02X} {type_name}"));
                // Store storage type in `mode` so read_file can dispatch correctly.
                fe.mode = Some(type_nibble as u32);
                result.push(fe);
            }
        }

        block_num = next_block;
    }

    Ok(result)
}

/// Read the 256 block pointers from an index block.
///
/// Index block layout: low bytes at offsets 0–255, high bytes at offsets 256–511.
/// block_number[i] = buf[256 + i] as u16 * 256 + buf[i] as u16.
/// Block number 0 denotes a sparse (all-zeros) block.
fn read_index_block<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    block_num: u16,
) -> Result<[u16; 256], FilesystemError> {
    if block_num == 0 {
        return Ok([0u16; 256]);
    }
    let offset = partition_offset + block_num as u64 * BLOCK_SIZE;
    reader
        .seek(SeekFrom::Start(offset))
        .map_err(FilesystemError::Io)?;
    let mut buf = [0u8; 512];
    reader.read_exact(&mut buf).map_err(FilesystemError::Io)?;

    let mut ptrs = [0u16; 256];
    for i in 0..256 {
        ptrs[i] = buf[i] as u16 | ((buf[256 + i] as u16) << 8);
    }
    Ok(ptrs)
}

/// Read a seedling file (single data block, key_ptr → data).
fn read_seedling<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    key_ptr: u16,
    eof: usize,
) -> Result<Vec<u8>, FilesystemError> {
    if key_ptr == 0 {
        return Ok(vec![0u8; eof.min(512)]);
    }
    let offset = partition_offset + key_ptr as u64 * BLOCK_SIZE;
    reader
        .seek(SeekFrom::Start(offset))
        .map_err(FilesystemError::Io)?;
    let mut buf = [0u8; 512];
    reader.read_exact(&mut buf).map_err(FilesystemError::Io)?;
    Ok(buf[..eof.min(512)].to_vec())
}

/// Read a sapling file (index block → up to 256 data blocks).
fn read_sapling<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    key_ptr: u16,
    eof: usize,
) -> Result<Vec<u8>, FilesystemError> {
    let index = read_index_block(reader, partition_offset, key_ptr)?;
    let mut data = Vec::with_capacity(eof);

    for &data_block in index.iter() {
        if data.len() >= eof {
            break;
        }
        let remaining = eof - data.len();
        let to_take = remaining.min(512);

        if data_block == 0 {
            data.extend_from_slice(&vec![0u8; to_take]);
        } else {
            let offset = partition_offset + data_block as u64 * BLOCK_SIZE;
            reader
                .seek(SeekFrom::Start(offset))
                .map_err(FilesystemError::Io)?;
            let mut buf = [0u8; 512];
            reader.read_exact(&mut buf).map_err(FilesystemError::Io)?;
            data.extend_from_slice(&buf[..to_take]);
        }
    }

    Ok(data)
}

/// Read a tree file (master index block → up to 128 index blocks → data blocks).
fn read_tree<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    key_ptr: u16,
    eof: usize,
) -> Result<Vec<u8>, FilesystemError> {
    let master = read_index_block(reader, partition_offset, key_ptr)?;
    let mut data = Vec::with_capacity(eof);

    // ProDOS tree files use at most 128 index block slots in the master index.
    for &index_block_num in master[..128].iter() {
        if data.len() >= eof {
            break;
        }

        let index = if index_block_num == 0 {
            [0u16; 256] // Sparse index block
        } else {
            read_index_block(reader, partition_offset, index_block_num)?
        };

        for &data_block in index.iter() {
            if data.len() >= eof {
                break;
            }
            let remaining = eof - data.len();
            let to_take = remaining.min(512);

            if data_block == 0 {
                data.extend_from_slice(&vec![0u8; to_take]);
            } else {
                let offset = partition_offset + data_block as u64 * BLOCK_SIZE;
                reader
                    .seek(SeekFrom::Start(offset))
                    .map_err(FilesystemError::Io)?;
                let mut buf = [0u8; 512];
                reader.read_exact(&mut buf).map_err(FilesystemError::Io)?;
                data.extend_from_slice(&buf[..to_take]);
            }
        }
    }

    Ok(data)
}

// ─────────────────────────────── ProDOS type codes ───────────────────────────

fn prodos_type_name(file_type: u8) -> &'static str {
    match file_type {
        0x00 => "NON",
        0x01 => "BAD",
        0x04 => "TXT",
        0x06 => "BIN",
        0x0F => "DIR",
        0x19 => "ADB",
        0x1A => "AWP",
        0x1B => "ASP",
        0xB0 => "SRC",
        0xB3 => "S16",
        0xB4 => "RTL",
        0xB5 => "EXE",
        0xB6 => "PIF",
        0xB7 => "TIF",
        0xB8 => "NDA",
        0xB9 => "CDA",
        0xBA => "TOL",
        0xBB => "DVR",
        0xBC => "LDF",
        0xBD => "FST",
        0xBF => "DOC",
        0xC0 => "PNT",
        0xC1 => "PIC",
        0xC2 => "ANI",
        0xC3 => "PAL",
        0xC8 => "FON",
        0xC9 => "FND",
        0xCA => "ICN",
        0xD5 => "MUS",
        0xD6 => "INS",
        0xD7 => "MDI",
        0xD8 => "SND",
        0xDB => "DBM",
        0xE0 => "LBR",
        0xEF => "PAS",
        0xF0 => "CMD",
        0xF1 => "OVL",
        0xFC => "BAS",
        0xFD => "VAR",
        0xFE => "REL",
        0xFF => "SYS",
        _ => "???",
    }
}

/// Parse a ProDOS date+time pair into a human-readable string.
///
/// Date format (u16 LE): bits[15:9]=year (7-bit, +1900; <70 → 2000+),
///   bits[8:5]=month (4-bit, 1–12), bits[4:0]=day (5-bit, 1–31).
/// Time format (u16 LE): high byte = hour, low byte = minute.
fn parse_prodos_datetime(date: u16, time: u16) -> Option<String> {
    if date == 0 {
        return None;
    }

    let year_raw = (date >> 9) & 0x7F;
    let month = (date >> 5) & 0xF;
    let day = date & 0x1F;
    let hour = (time >> 8) as u8;
    let minute = (time & 0xFF) as u8;

    if month == 0 || month > 12 || day == 0 || day > 31 {
        return None;
    }

    let year = if year_raw < 70 {
        2000 + year_raw as u16
    } else {
        1900 + year_raw as u16
    };

    Some(format!(
        "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}"
    ))
}

// ─────────────────────────────── tests ───────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // Build a minimal 512-byte volume directory key block for testing.
    // Volume name "BLANK", total_blocks=280, bitmap at block 6.
    fn make_volume_key_block() -> [u8; 512] {
        let mut block = [0u8; 512];
        // prev_block = 0 (bytes 0-1), next_block = 0 (bytes 2-3)
        // storage_type=0xF, name_length=5
        block[4] = 0xF5;
        block[5] = b'B';
        block[6] = b'L';
        block[7] = b'A';
        block[8] = b'N';
        block[9] = b'K';
        block[27] = 39; // entry_length
        block[28] = 13; // entries_per_block
        block[31] = 6;
        block[32] = 0; // bitmap_pointer = 6
        let tb: u16 = 280;
        block[33] = tb as u8;
        block[34] = (tb >> 8) as u8; // total_blocks = 280
        block
    }

    // Build a 280-block disk image with the given key block at block 2
    // and a bitmap at block 6 that marks blocks 0–6 as used.
    fn make_disk_image(volume_key: [u8; 512]) -> Vec<u8> {
        let total = 280usize * 512;
        let mut disk = vec![0u8; total];
        disk[1024..1024 + 512].copy_from_slice(&volume_key);

        // Bitmap at block 6 (offset 3072):
        //   blocks 0-6 used (bit=0), block 7 free (bit=1), rest free (0xFF)
        // Byte 0 = 0b0000_0001: MSB=block0(used)…bit0=block7(free)
        disk[3072] = 0x01;
        for i in 1..512 {
            disk[3072 + i] = 0xFF;
        }
        disk
    }

    #[test]
    fn test_parse_volume_header() {
        let vk = make_volume_key_block();
        let disk = make_disk_image(vk);
        let mut cursor = Cursor::new(disk);
        let header = read_volume_header(&mut cursor, 0).unwrap();
        assert_eq!(header.name, "BLANK");
        assert_eq!(header.total_blocks, 280);
        assert_eq!(header.bitmap_pointer, 6);
        // 7 used blocks → free = 280 − 7 = 273
        assert_eq!(header.free_blocks, 273);
    }

    #[test]
    fn test_validate_volume_key_block() {
        let vk = make_volume_key_block();
        assert!(validate_volume_key_block(&vk));

        let mut bad = vk;
        bad[27] = 40; // wrong entry_length
        assert!(!validate_volume_key_block(&bad));

        let mut bad2 = vk;
        bad2[4] = 0xA5; // storage_type nibble != 0xF
        assert!(!validate_volume_key_block(&bad2));
    }

    #[test]
    fn test_is_block_used() {
        // byte 0 = 0x01 = 0b0000_0001
        // blocks 0–6: used (bit=0), block 7: free (bit=1)
        let bitmap = [0x01u8, 0xFF, 0xFF];
        assert!(is_block_used(&bitmap, 0));
        assert!(is_block_used(&bitmap, 1));
        assert!(is_block_used(&bitmap, 6));
        assert!(!is_block_used(&bitmap, 7));
        assert!(!is_block_used(&bitmap, 8));
    }

    #[test]
    fn test_parse_prodos_datetime() {
        // Year 91 (1991), month 10 (Oct), day 15.
        // Standard ProDOS format: bits[15:9]=year, bits[8:5]=month, bits[4:0]=day.
        let year: u16 = 91;
        let month: u16 = 10;
        let day: u16 = 15;
        let date = (year << 9) | (month << 5) | day;
        // Time: hour=14, minute=30
        let time: u16 = (14 << 8) | 30;
        let result = parse_prodos_datetime(date, time);
        assert_eq!(result, Some("1991-10-15 14:30".to_string()));

        // Y2K: year 3 → 2003
        let date2 = (3u16 << 9) | (month << 5) | day;
        let result2 = parse_prodos_datetime(date2, time);
        assert_eq!(result2, Some("2003-10-15 14:30".to_string()));

        // date == 0 → None
        assert_eq!(parse_prodos_datetime(0, 0), None);

        // month 0 → None
        let bad_date = (year << 9) | (0u16 << 5) | day;
        assert_eq!(parse_prodos_datetime(bad_date, time), None);
    }

    #[test]
    fn test_read_seedling_file() {
        let content = b"Hello, ProDOS!";
        let mut disk = vec![0u8; 512 * 10];
        // File data at block 5 (offset 2560)
        disk[5 * 512..5 * 512 + content.len()].copy_from_slice(content);
        let mut cursor = Cursor::new(disk);
        let data = read_seedling(&mut cursor, 0, 5, content.len()).unwrap();
        assert_eq!(&data, content);
    }

    #[test]
    fn test_read_sapling_file() {
        // Build a sapling: index block at block 3, data at blocks 4 and 5.
        // ProDOS sapling: block 4 holds bytes 0–511, block 5 holds bytes 512–1023.
        let mut disk = vec![0u8; 512 * 10];
        // Index block at block 3: entry[0] = block 4, entry[1] = block 5
        disk[3 * 512] = 4; // lo byte of pointer[0]
        disk[3 * 512 + 256] = 0; // hi byte of pointer[0]
        disk[3 * 512 + 1] = 5; // lo byte of pointer[1]
        disk[3 * 512 + 257] = 0; // hi byte of pointer[1]
                                 // Block 4: fill with 'A' (bytes 0–511 of file)
        for i in 0..512 {
            disk[4 * 512 + i] = b'A';
        }
        // Block 5: first 4 bytes = "EFGH" (bytes 512–515 of file)
        disk[5 * 512..5 * 512 + 4].copy_from_slice(b"EFGH");

        let mut cursor = Cursor::new(disk);
        // eof = 516: 512 bytes from block 4, 4 bytes from block 5
        let data = read_sapling(&mut cursor, 0, 3, 516).unwrap();
        assert_eq!(data.len(), 516);
        assert!(
            data[..512].iter().all(|&b| b == b'A'),
            "block 4 content mismatch"
        );
        assert_eq!(&data[512..516], b"EFGH");
    }

    #[test]
    fn test_compact_reader_size() {
        let vk = make_volume_key_block();
        let disk = make_disk_image(vk);
        let cursor = Cursor::new(disk);

        let (mut reader, info) = CompactProDosReader::new(cursor, 0).unwrap();
        assert_eq!(info.original_size, 280 * 512);
        assert_eq!(info.compacted_size, 280 * 512);
        assert_eq!(info.clusters_used, 7);
        assert_eq!(info.data_size, 7 * 512);

        // Read all output
        let mut output = Vec::new();
        std::io::Read::read_to_end(&mut reader, &mut output).unwrap();
        assert_eq!(output.len(), 280 * 512);
    }

    #[test]
    fn test_resize_grow() {
        let vk = make_volume_key_block();
        let mut disk = make_disk_image(vk);
        // Extend disk to hold more blocks.
        disk.resize(400 * 512, 0);

        let mut cursor = Cursor::new(disk);
        resize_prodos_in_place(&mut cursor, 0, 400 * 512, &mut |_| {}).unwrap();

        // Verify total_blocks was updated.
        cursor.seek(SeekFrom::Start(1024)).unwrap();
        let mut sector = [0u8; 512];
        cursor.read_exact(&mut sector).unwrap();
        let new_total = u16::from_le_bytes([sector[33], sector[34]]);
        assert_eq!(new_total, 400);
    }

    #[test]
    fn test_resize_no_op() {
        let vk = make_volume_key_block();
        let disk = make_disk_image(vk);
        let mut cursor = Cursor::new(disk);
        // Request same size — should be a no-op.
        resize_prodos_in_place(&mut cursor, 0, 280 * 512, &mut |_| {}).unwrap();
        cursor.seek(SeekFrom::Start(1024)).unwrap();
        let mut sector = [0u8; 512];
        cursor.read_exact(&mut sector).unwrap();
        let total = u16::from_le_bytes([sector[33], sector[34]]);
        assert_eq!(total, 280); // unchanged
    }

    #[test]
    fn test_validate_integrity() {
        let vk = make_volume_key_block();
        let disk = make_disk_image(vk);
        let mut cursor = Cursor::new(disk);
        let warnings = validate_prodos_integrity(&mut cursor, 0, &mut |_| {}).unwrap();
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_list_root_directory() {
        let vk = make_volume_key_block();
        let mut disk = make_disk_image(vk);

        // Write two file entries into the volume key block at offsets 43 and 82.
        // Entry 1: seedling (storage_type=1), name="HELLO", type=TXT ($04), eof=14
        {
            let eo = 1024 + 43; // block 2 + skip header entry (slot 0 = 39 bytes)
            disk[eo] = (1 << 4) | 5; // type=1 (seedling), name_len=5
            disk[eo + 1..eo + 6].copy_from_slice(b"HELLO");
            disk[eo + 16] = 0x04; // file_type = TXT
            disk[eo + 17] = 10; // key_pointer = block 10
            disk[eo + 18] = 0;
            disk[eo + 21] = 14; // eof lo byte
            disk[eo + 22] = 0;
            disk[eo + 23] = 0;
        }
        // Entry 2: subdirectory (storage_type=0xE), name="SUBDIR"
        {
            let eo = 1024 + 82; // slot 2
            disk[eo] = (0xE << 4) | 6; // type=0xE (subdir), name_len=6
            disk[eo + 1..eo + 7].copy_from_slice(b"SUBDIR");
            disk[eo + 17] = 20; // key_pointer = block 20
            disk[eo + 18] = 0;
        }

        let cursor = Cursor::new(disk);
        let mut fs = ProDosFilesystem::open(cursor, 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();

        assert_eq!(entries.len(), 2);

        let file_entry = entries.iter().find(|e| e.name == "HELLO").unwrap();
        assert!(file_entry.is_file());
        assert_eq!(file_entry.size, 14);
        assert_eq!(file_entry.location, 10);
        assert_eq!(file_entry.mode, Some(1)); // seedling

        let dir_entry = entries.iter().find(|e| e.name == "SUBDIR").unwrap();
        assert!(dir_entry.is_directory());
        assert_eq!(dir_entry.location, 20);
    }

    // ── Editing tests ────────────────────────────────────────────────────

    fn make_editable_fs() -> ProDosFilesystem<Cursor<Vec<u8>>> {
        let vk = make_volume_key_block();
        let disk = make_disk_image(vk);
        let cursor = Cursor::new(disk);
        ProDosFilesystem::open(cursor, 0).unwrap()
    }

    #[test]
    fn test_allocate_and_free_block() {
        let mut fs = make_editable_fs();
        let initial_free = fs.header.free_blocks;

        // First free block should be 7 (blocks 0-6 are used).
        let block = fs.allocate_block().unwrap();
        assert_eq!(block, 7);
        assert_eq!(fs.header.free_blocks, initial_free - 1);

        // Free it back.
        fs.free_block(block).unwrap();
        assert_eq!(fs.header.free_blocks, initial_free);
    }

    #[test]
    fn test_create_seedling_file() {
        let mut fs = make_editable_fs();
        let root = fs.root().unwrap();
        let data = b"Hello, ProDOS!";
        let opts = CreateFileOptions {
            type_code: Some("$04".into()),
            ..Default::default()
        };

        let entry = fs
            .create_file(&root, "HELLO", &mut &data[..], data.len() as u64, &opts)
            .unwrap();

        assert_eq!(entry.name, "HELLO");
        assert_eq!(entry.size, 14);
        assert!(entry.type_code.as_ref().unwrap().starts_with("$04"));

        // Verify we can read it back.
        let read_back = fs.read_file(&entry, usize::MAX).unwrap();
        assert_eq!(&read_back, data);

        // Verify it shows up in directory listing.
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "HELLO");
    }

    #[test]
    fn test_create_sapling_file() {
        let mut fs = make_editable_fs();
        let root = fs.root().unwrap();

        // Create a 1024-byte file (needs sapling: 1 index block + 2 data blocks).
        let data = vec![0xABu8; 1024];
        let opts = CreateFileOptions::default();

        let entry = fs
            .create_file(&root, "BIGFILE", &mut &data[..], data.len() as u64, &opts)
            .unwrap();

        assert_eq!(entry.size, 1024);
        assert_eq!(entry.mode, Some(2)); // sapling

        // Read back and verify.
        let read_back = fs.read_file(&entry, usize::MAX).unwrap();
        assert_eq!(read_back.len(), 1024);
        assert!(read_back.iter().all(|&b| b == 0xAB));
    }

    #[test]
    fn test_create_directory() {
        let mut fs = make_editable_fs();
        let root = fs.root().unwrap();
        let opts = CreateDirectoryOptions::default();

        let dir = fs.create_directory(&root, "MYDIR", &opts).unwrap();
        assert_eq!(dir.name, "MYDIR");
        assert!(dir.is_directory());

        // Should show up in root listing.
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "MYDIR");
        assert!(entries[0].is_directory());

        // New directory should be empty.
        let children = fs.list_directory(&entries[0]).unwrap();
        assert!(children.is_empty());
    }

    #[test]
    fn test_delete_file() {
        let mut fs = make_editable_fs();
        let root = fs.root().unwrap();
        let free_before = fs.free_space().unwrap();

        let data = b"delete me";
        let opts = CreateFileOptions::default();
        let entry = fs
            .create_file(&root, "DELME", &mut &data[..], data.len() as u64, &opts)
            .unwrap();

        let free_after_create = fs.free_space().unwrap();
        assert!(free_after_create < free_before);

        let root = fs.root().unwrap();
        fs.delete_entry(&root, &entry).unwrap();

        // Free space should be restored.
        let free_after_delete = fs.free_space().unwrap();
        assert_eq!(free_after_delete, free_before);

        // Directory should be empty.
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_delete_empty_directory() {
        let mut fs = make_editable_fs();
        let root = fs.root().unwrap();
        let opts = CreateDirectoryOptions::default();
        let dir = fs.create_directory(&root, "EMPTYDIR", &opts).unwrap();

        let root = fs.root().unwrap();
        fs.delete_entry(&root, &dir).unwrap();

        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_delete_nonempty_directory_fails() {
        let mut fs = make_editable_fs();
        let root = fs.root().unwrap();

        let dir = fs
            .create_directory(&root, "FULL", &CreateDirectoryOptions::default())
            .unwrap();

        // Add a file inside the directory.
        let data = b"inside";
        fs.create_file(
            &dir,
            "INNER",
            &mut &data[..],
            data.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();

        // Deleting non-empty dir should fail.
        let root = fs.root().unwrap();
        let result = fs.delete_entry(&root, &dir);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("non-empty"), "got: {err}");
    }

    #[test]
    fn test_delete_recursive() {
        let mut fs = make_editable_fs();
        let root = fs.root().unwrap();

        let dir = fs
            .create_directory(&root, "RDIR", &CreateDirectoryOptions::default())
            .unwrap();

        let data = b"recursive";
        fs.create_file(
            &dir,
            "FILE1",
            &mut &data[..],
            data.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        fs.create_file(
            &dir,
            "FILE2",
            &mut &data[..],
            data.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();

        let root = fs.root().unwrap();
        fs.delete_recursive(&root, &dir).unwrap();

        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_create_file_duplicate_name() {
        let mut fs = make_editable_fs();
        let root = fs.root().unwrap();
        let data = b"first";
        let opts = CreateFileOptions::default();

        fs.create_file(&root, "DUP", &mut &data[..], data.len() as u64, &opts)
            .unwrap();

        let root = fs.root().unwrap();
        let result = fs.create_file(&root, "DUP", &mut &data[..], data.len() as u64, &opts);
        assert!(result.is_err());
        match result.unwrap_err() {
            FilesystemError::AlreadyExists(_) => {}
            other => panic!("expected AlreadyExists, got: {other}"),
        }
    }

    #[test]
    fn test_free_space() {
        let mut fs = make_editable_fs();
        let initial = fs.free_space().unwrap();
        assert_eq!(initial, 273 * 512); // 280 - 7 used blocks

        let root = fs.root().unwrap();
        let data = b"test";
        fs.create_file(
            &root,
            "F",
            &mut &data[..],
            data.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();

        let after = fs.free_space().unwrap();
        assert!(after < initial);
        // Seedling uses 1 block.
        assert_eq!(after, initial - 512);
    }

    #[test]
    fn test_name_validation() {
        assert!(validate_prodos_name("HELLO").is_ok());
        assert!(validate_prodos_name("A.FILE").is_ok());
        assert!(validate_prodos_name("hello").is_ok()); // lowercased → OK
        assert_eq!(validate_prodos_name("hello").unwrap(), "HELLO");

        // Invalid: empty
        assert!(validate_prodos_name("").is_err());
        // Invalid: too long (16 chars)
        assert!(validate_prodos_name("ABCDEFGHIJKLMNOP").is_err());
        // Invalid: starts with digit
        assert!(validate_prodos_name("1FILE").is_err());
        // Invalid: contains space
        assert!(validate_prodos_name("MY FILE").is_err());
        // Invalid: special chars
        assert!(validate_prodos_name("FILE/NAME").is_err());
    }
}
