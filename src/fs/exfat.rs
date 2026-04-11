use std::io::{self, Read, Seek, SeekFrom, Write};

use anyhow::{bail, Result};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};

// exFAT directory entry types
const ENTRY_TYPE_ALLOCATION_BITMAP: u8 = 0x81;
const ENTRY_TYPE_VOLUME_LABEL: u8 = 0x83;
const ENTRY_TYPE_FILE: u8 = 0x85;
const ENTRY_TYPE_STREAM_EXT: u8 = 0xC0;
const ENTRY_TYPE_FILE_NAME: u8 = 0xC1;

// File attributes
const ATTR_DIRECTORY: u16 = 0x10;

/// exFAT VBR fields.
struct ExfatVbr {
    volume_length: u64,
    fat_offset: u32,
    fat_length: u32,
    cluster_heap_offset: u32,
    cluster_count: u32,
    root_cluster: u32,
    revision: (u8, u8),
    bytes_per_sector_shift: u8,
    sectors_per_cluster_shift: u8,
    number_of_fats: u8,
}

fn parse_exfat_vbr(vbr: &[u8; 512]) -> Result<ExfatVbr, FilesystemError> {
    // Check OEM: "EXFAT   " at offset 3
    if &vbr[3..11] != b"EXFAT   " {
        return Err(FilesystemError::Parse(
            "not an exFAT volume (OEM ID mismatch)".into(),
        ));
    }

    // Check that the must-be-zero region (0x0B..0x40) is indeed zero
    // (this is a key exFAT identifier)
    let must_be_zero = &vbr[0x0B..0x40];
    if must_be_zero.iter().any(|&b| b != 0) {
        return Err(FilesystemError::Parse(
            "exFAT must-be-zero region is not zero".into(),
        ));
    }

    let volume_length = u64::from_le_bytes([
        vbr[0x48], vbr[0x49], vbr[0x4A], vbr[0x4B], vbr[0x4C], vbr[0x4D], vbr[0x4E], vbr[0x4F],
    ]);

    let fat_offset = u32::from_le_bytes([vbr[0x50], vbr[0x51], vbr[0x52], vbr[0x53]]);
    let fat_length = u32::from_le_bytes([vbr[0x54], vbr[0x55], vbr[0x56], vbr[0x57]]);
    let cluster_heap_offset = u32::from_le_bytes([vbr[0x58], vbr[0x59], vbr[0x5A], vbr[0x5B]]);
    let cluster_count = u32::from_le_bytes([vbr[0x5C], vbr[0x5D], vbr[0x5E], vbr[0x5F]]);
    let root_cluster = u32::from_le_bytes([vbr[0x60], vbr[0x61], vbr[0x62], vbr[0x63]]);

    let revision_minor = vbr[0x68];
    let revision_major = vbr[0x69];

    let bytes_per_sector_shift = vbr[0x6C];
    let sectors_per_cluster_shift = vbr[0x6D];
    let number_of_fats = vbr[0x6E];

    if !(9..=12).contains(&bytes_per_sector_shift) {
        return Err(FilesystemError::Parse(format!(
            "invalid exFAT bytes per sector shift: {}",
            bytes_per_sector_shift
        )));
    }

    Ok(ExfatVbr {
        volume_length,
        fat_offset,
        fat_length,
        cluster_heap_offset,
        cluster_count,
        root_cluster,
        revision: (revision_major, revision_minor),
        bytes_per_sector_shift,
        sectors_per_cluster_shift,
        number_of_fats,
    })
}

/// exFAT filesystem reader.
pub struct ExfatFilesystem<R> {
    reader: R,
    partition_offset: u64,
    bytes_per_sector: u64,
    #[allow(dead_code)]
    sectors_per_cluster: u64,
    volume_length: u64,
    fat_offset_sectors: u32,
    #[allow(dead_code)]
    fat_length_sectors: u32,
    cluster_heap_offset_sectors: u32,
    cluster_count: u32,
    root_cluster: u32,
    cluster_size: u64,
    label: Option<String>,
    #[allow(dead_code)]
    revision: (u8, u8),
    #[allow(dead_code)]
    number_of_fats: u8,
    bitmap_start_cluster: u32,
    bitmap_size: u64,
    used_bytes: u64,
}

impl<R: Read + Seek> ExfatFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        reader.seek(SeekFrom::Start(partition_offset))?;
        let mut vbr_buf = [0u8; 512];
        reader
            .read_exact(&mut vbr_buf)
            .map_err(|e| FilesystemError::Parse(format!("cannot read exFAT VBR: {e}")))?;

        let vbr = parse_exfat_vbr(&vbr_buf)?;

        let bytes_per_sector = 1u64 << vbr.bytes_per_sector_shift;
        let sectors_per_cluster = 1u64 << vbr.sectors_per_cluster_shift;
        let cluster_size = bytes_per_sector * sectors_per_cluster;

        let mut fs = ExfatFilesystem {
            reader,
            partition_offset,
            bytes_per_sector,
            sectors_per_cluster,
            volume_length: vbr.volume_length,
            fat_offset_sectors: vbr.fat_offset,
            fat_length_sectors: vbr.fat_length,
            cluster_heap_offset_sectors: vbr.cluster_heap_offset,
            cluster_count: vbr.cluster_count,
            root_cluster: vbr.root_cluster,
            cluster_size,
            label: None,
            revision: vbr.revision,
            number_of_fats: vbr.number_of_fats,
            bitmap_start_cluster: 0,
            bitmap_size: 0,
            used_bytes: 0,
        };

        // Read root directory to find allocation bitmap and volume label
        fs.scan_root_directory_metadata()?;

        // Calculate used size from bitmap
        fs.used_bytes = fs.calculate_used_bytes().unwrap_or(0);

        Ok(fs)
    }

    /// Absolute byte offset for a cluster number (clusters are 2-based).
    fn cluster_offset(&self, cluster: u32) -> u64 {
        self.partition_offset
            + self.cluster_heap_offset_sectors as u64 * self.bytes_per_sector
            + (cluster as u64 - 2) * self.cluster_size
    }

    /// Absolute byte offset for the FAT.
    fn fat_offset(&self) -> u64 {
        self.partition_offset + self.fat_offset_sectors as u64 * self.bytes_per_sector
    }

    /// Read the next cluster from the FAT.
    fn next_cluster(&mut self, cluster: u32) -> Result<Option<u32>, FilesystemError> {
        if cluster < 2 || cluster >= self.cluster_count + 2 {
            return Ok(None);
        }

        let fat_entry_offset = self.fat_offset() + cluster as u64 * 4;
        self.reader.seek(SeekFrom::Start(fat_entry_offset))?;
        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf)?;
        let next = u32::from_le_bytes(buf);

        if next >= 0xFFFFFFF8 || next < 2 {
            Ok(None)
        } else {
            Ok(Some(next))
        }
    }

    /// Read cluster chain data.
    fn read_cluster_chain(
        &mut self,
        start_cluster: u32,
        max_bytes: Option<u64>,
    ) -> Result<Vec<u8>, FilesystemError> {
        let limit = max_bytes.unwrap_or(u64::MAX);
        let mut data = Vec::new();
        let mut cluster = start_cluster;
        let mut count = 0u32;

        while cluster >= 2 && count <= self.cluster_count {
            if data.len() as u64 >= limit {
                break;
            }

            let offset = self.cluster_offset(cluster);
            self.reader.seek(SeekFrom::Start(offset))?;

            let remaining = limit - data.len() as u64;
            let to_read = self.cluster_size.min(remaining) as usize;
            let mut buf = vec![0u8; to_read];

            match self.reader.read(&mut buf) {
                Ok(n) if n > 0 => {
                    data.extend_from_slice(&buf[..n]);
                    if n < to_read {
                        break;
                    }
                }
                _ => break,
            }

            count += 1;
            match self.next_cluster(cluster)? {
                Some(next) => cluster = next,
                None => break,
            }
        }

        Ok(data)
    }

    /// Read contiguous cluster data (NoFatChain flag set).
    fn read_contiguous_clusters(
        &mut self,
        start_cluster: u32,
        data_length: u64,
        max_bytes: Option<u64>,
    ) -> Result<Vec<u8>, FilesystemError> {
        let limit = max_bytes.map(|m| m.min(data_length)).unwrap_or(data_length);
        let offset = self.cluster_offset(start_cluster);
        self.reader.seek(SeekFrom::Start(offset))?;
        let mut data = vec![0u8; limit as usize];
        self.reader.read_exact(&mut data)?;
        Ok(data)
    }

    /// Scan root directory to find metadata entries (bitmap, label).
    fn scan_root_directory_metadata(&mut self) -> Result<(), FilesystemError> {
        let root_data = self.read_cluster_chain(self.root_cluster, None)?;

        let mut pos = 0;
        while pos + 32 <= root_data.len() {
            let entry_type = root_data[pos];

            if entry_type == 0x00 {
                break; // End of directory
            }

            match entry_type {
                ENTRY_TYPE_ALLOCATION_BITMAP => {
                    // Bitmap entry: start cluster at offset 20 (u32), data length at offset 24 (u64)
                    if pos + 32 <= root_data.len() {
                        self.bitmap_start_cluster = u32::from_le_bytes([
                            root_data[pos + 20],
                            root_data[pos + 21],
                            root_data[pos + 22],
                            root_data[pos + 23],
                        ]);
                        self.bitmap_size = u64::from_le_bytes([
                            root_data[pos + 24],
                            root_data[pos + 25],
                            root_data[pos + 26],
                            root_data[pos + 27],
                            root_data[pos + 28],
                            root_data[pos + 29],
                            root_data[pos + 30],
                            root_data[pos + 31],
                        ]);
                    }
                }
                ENTRY_TYPE_VOLUME_LABEL => {
                    // Volume label entry: character count at offset 1, name at offset 2 (UTF-16LE)
                    let char_count = root_data[pos + 1] as usize;
                    if char_count > 0 && pos + 2 + char_count * 2 <= root_data.len() {
                        let chars: Vec<u16> = (0..char_count)
                            .map(|i| {
                                u16::from_le_bytes([
                                    root_data[pos + 2 + i * 2],
                                    root_data[pos + 2 + i * 2 + 1],
                                ])
                            })
                            .collect();
                        let label = String::from_utf16_lossy(&chars).trim().to_string();
                        if !label.is_empty() {
                            self.label = Some(label);
                        }
                    }
                }
                _ => {}
            }

            pos += 32;
        }

        Ok(())
    }

    /// Calculate used bytes from the allocation bitmap.
    fn calculate_used_bytes(&mut self) -> Result<u64, FilesystemError> {
        if self.bitmap_start_cluster == 0 {
            return Ok(0);
        }

        let bitmap = self.read_contiguous_clusters(
            self.bitmap_start_cluster,
            self.bitmap_size,
            Some(self.bitmap_size),
        )?;

        let used_clusters = count_set_bits(&bitmap, self.cluster_count);
        Ok(used_clusters * self.cluster_size)
    }

    /// Find the last used cluster by scanning the bitmap backwards.
    fn find_last_used_cluster(&mut self) -> Result<u64, FilesystemError> {
        if self.bitmap_start_cluster == 0 {
            return Ok(0);
        }

        let bitmap = self.read_contiguous_clusters(
            self.bitmap_start_cluster,
            self.bitmap_size,
            Some(self.bitmap_size),
        )?;

        for byte_idx in (0..bitmap.len()).rev() {
            if bitmap[byte_idx] != 0 {
                for bit in (0..8).rev() {
                    let cluster = byte_idx as u64 * 8 + bit as u64;
                    if cluster < self.cluster_count as u64 && bitmap[byte_idx] & (1 << bit) != 0 {
                        return Ok(cluster);
                    }
                }
            }
        }

        Ok(0)
    }

    /// Parse a directory from its starting cluster, returning file entries.
    fn parse_directory(
        &mut self,
        start_cluster: u32,
        parent_path: &str,
    ) -> Result<Vec<FileEntry>, FilesystemError> {
        let dir_data = self.read_cluster_chain(start_cluster, None)?;
        let mut entries = Vec::new();

        let mut pos = 0;
        while pos + 32 <= dir_data.len() {
            let entry_type = dir_data[pos];

            if entry_type == 0x00 {
                break;
            }

            // Look for File directory entry set: 0x85 + 0xC0 + one or more 0xC1
            if entry_type == ENTRY_TYPE_FILE {
                let secondary_count = dir_data[pos + 1] as usize;
                let file_attrs = u16::from_le_bytes([dir_data[pos + 4], dir_data[pos + 5]]);

                // Next entry should be stream extension (0xC0)
                let stream_pos = pos + 32;
                if stream_pos + 32 > dir_data.len() || dir_data[stream_pos] != ENTRY_TYPE_STREAM_EXT
                {
                    pos += 32;
                    continue;
                }

                let _no_fat_chain = (dir_data[stream_pos + 1] & 0x02) != 0;
                let name_length = dir_data[stream_pos + 3] as usize;
                let first_cluster = u32::from_le_bytes([
                    dir_data[stream_pos + 20],
                    dir_data[stream_pos + 21],
                    dir_data[stream_pos + 22],
                    dir_data[stream_pos + 23],
                ]);
                let data_length = u64::from_le_bytes([
                    dir_data[stream_pos + 24],
                    dir_data[stream_pos + 25],
                    dir_data[stream_pos + 26],
                    dir_data[stream_pos + 27],
                    dir_data[stream_pos + 28],
                    dir_data[stream_pos + 29],
                    dir_data[stream_pos + 30],
                    dir_data[stream_pos + 31],
                ]);

                // Collect filename from 0xC1 entries
                let mut name_chars = Vec::new();
                for i in 0..secondary_count.saturating_sub(1) {
                    let fn_pos = pos + 64 + i * 32; // after file + stream entries
                    if fn_pos + 32 > dir_data.len() || dir_data[fn_pos] != ENTRY_TYPE_FILE_NAME {
                        break;
                    }
                    // 15 UTF-16LE characters per filename entry (at offset 2)
                    for j in 0..15 {
                        if name_chars.len() >= name_length {
                            break;
                        }
                        let char_off = fn_pos + 2 + j * 2;
                        if char_off + 1 < dir_data.len() {
                            name_chars.push(u16::from_le_bytes([
                                dir_data[char_off],
                                dir_data[char_off + 1],
                            ]));
                        }
                    }
                }

                name_chars.truncate(name_length);
                let name = String::from_utf16_lossy(&name_chars);

                // Skip . and ..
                if name == "." || name == ".." || name.is_empty() {
                    pos += 32 * (1 + secondary_count);
                    continue;
                }

                let is_dir = file_attrs & ATTR_DIRECTORY != 0;
                let path = if parent_path == "/" {
                    format!("/{name}")
                } else {
                    format!("{parent_path}/{name}")
                };

                let entry = if is_dir {
                    FileEntry {
                        name,
                        path,
                        entry_type: EntryType::Directory,
                        size: 0,
                        location: first_cluster as u64,
                        modified: None,
                        type_code: None,
                        creator_code: None,
                        symlink_target: None,
                        special_type: None,
                        mode: None,
                        uid: None,
                        gid: None,
                        resource_fork_size: None,
                        aux_type: None,
                    }
                } else {
                    FileEntry {
                        name,
                        path,
                        entry_type: EntryType::File,
                        size: data_length,
                        location: first_cluster as u64,
                        modified: None,
                        type_code: None,
                        creator_code: None,
                        symlink_target: None,
                        special_type: None,
                        mode: None,
                        uid: None,
                        gid: None,
                        resource_fork_size: None,
                        aux_type: None,
                    }
                };

                entries.push(entry);

                // Skip past all secondary entries
                pos += 32 * (1 + secondary_count);
                continue;
            }

            pos += 32;
        }

        Ok(entries)
    }
}

impl<R: Read + Seek + Send> Filesystem for ExfatFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry {
            name: "/".into(),
            path: "/".into(),
            entry_type: EntryType::Directory,
            size: 0,
            location: self.root_cluster as u64,
            modified: None,
            type_code: None,
            creator_code: None,
            symlink_target: None,
            special_type: None,
            mode: None,
            uid: None,
            gid: None,
            resource_fork_size: None,
            aux_type: None,
        })
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if !entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }

        let cluster = if entry.path == "/" {
            self.root_cluster
        } else {
            entry.location as u32
        };

        self.parse_directory(cluster, &entry.path)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        if entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }

        let cluster = entry.location as u32;
        if cluster < 2 {
            return Ok(Vec::new());
        }

        self.read_cluster_chain(cluster, Some(max_bytes as u64))
    }

    fn volume_label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    fn fs_type(&self) -> &str {
        "exFAT"
    }

    fn total_size(&self) -> u64 {
        self.volume_length * self.bytes_per_sector
    }

    fn used_size(&self) -> u64 {
        self.used_bytes
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        let last_cluster = self.find_last_used_cluster()?;
        if last_cluster == 0 {
            return Ok(self.total_size());
        }
        // +2 because clusters are 2-based, +1 to include the last cluster
        let last_byte = self.cluster_heap_offset_sectors as u64 * self.bytes_per_sector
            + (last_cluster + 1) * self.cluster_size;
        Ok(last_byte)
    }
}

// =============================================================================
// Editing support
// =============================================================================

/// Get current timestamp as exFAT format (DOS-style u32: date in upper 16 bits, time in lower 16).
fn current_exfat_timestamp() -> u32 {
    // Use same approach as FAT: encode current UTC time
    // For simplicity in an embedded context, use a fixed recent timestamp
    // Date: bits 31-25=year(0-127 from 1980), 24-21=month, 20-16=day
    // Time: bits 15-11=hour, 10-5=minute, 4-0=second/2
    // 2024-01-01 00:00:00
    let year = 2024 - 1980; // 44
    let month = 1u32;
    let day = 1u32;
    let date = (year << 9) | (month << 5) | day;
    let time = 0u32; // midnight
    (date << 16) | time
}

impl<R: Read + Write + Seek> ExfatFilesystem<R> {
    // -- Bitmap I/O --

    /// Read the allocation bitmap from disk.
    fn read_bitmap(&mut self) -> Result<Vec<u8>, FilesystemError> {
        if self.bitmap_start_cluster < 2 {
            return Err(FilesystemError::InvalidData(
                "allocation bitmap not found".into(),
            ));
        }
        let offset = self.cluster_offset(self.bitmap_start_cluster);
        self.reader.seek(SeekFrom::Start(offset))?;
        let mut data = vec![0u8; self.bitmap_size as usize];
        self.reader.read_exact(&mut data)?;
        Ok(data)
    }

    /// Write the allocation bitmap back to disk (sector-aligned).
    fn write_bitmap(&mut self, bitmap: &[u8]) -> Result<(), FilesystemError> {
        let offset = self.cluster_offset(self.bitmap_start_cluster);
        self.reader.seek(SeekFrom::Start(offset))?;
        // Pad to sector alignment for raw device compatibility
        let aligned_size = ((bitmap.len() + 511) / 512) * 512;
        let mut aligned = vec![0u8; aligned_size];
        aligned[..bitmap.len()].copy_from_slice(bitmap);
        self.reader.write_all(&aligned)?;
        Ok(())
    }

    // -- Cluster Management --

    /// Allocate `count` clusters, chain them in FAT, set bitmap bits.
    fn allocate_clusters(&mut self, count: u32) -> Result<u32, FilesystemError> {
        if count == 0 {
            return Ok(0);
        }
        let mut bitmap = self.read_bitmap()?;
        let mut allocated = Vec::new();

        // Scan bitmap for free clusters
        for cluster_idx in 0..self.cluster_count {
            if allocated.len() as u32 >= count {
                break;
            }
            let byte = cluster_idx / 8;
            let bit = cluster_idx % 8;
            if byte < bitmap.len() as u32 && bitmap[byte as usize] & (1 << bit) == 0 {
                // Mark as used
                bitmap[byte as usize] |= 1 << bit;
                allocated.push(cluster_idx + 2); // clusters are 2-based
            }
        }

        if (allocated.len() as u32) < count {
            return Err(FilesystemError::DiskFull(format!(
                "need {} clusters, only {} free",
                count,
                allocated.len()
            )));
        }

        // Write FAT chain
        for i in 0..allocated.len() {
            let next = if i + 1 < allocated.len() {
                allocated[i + 1]
            } else {
                0xFFFFFFFFu32 // end of chain
            };
            self.write_fat_entry(allocated[i], next)?;
        }

        // Write bitmap
        self.write_bitmap(&bitmap)?;

        Ok(allocated[0])
    }

    /// Free a cluster chain: clear FAT entries, clear bitmap bits.
    fn free_cluster_chain_rw(&mut self, start: u32) -> Result<(), FilesystemError> {
        if start < 2 {
            return Ok(());
        }
        let mut bitmap = self.read_bitmap()?;
        let mut cluster = start;
        let mut count = 0u32;

        loop {
            if cluster < 2 || count > self.cluster_count {
                break;
            }
            // Read next before zeroing
            let next = self.next_cluster(cluster)?;
            // Zero FAT entry
            self.write_fat_entry(cluster, 0)?;
            // Clear bitmap bit
            let idx = cluster - 2;
            let byte = idx / 8;
            let bit = idx % 8;
            if (byte as usize) < bitmap.len() {
                bitmap[byte as usize] &= !(1 << bit);
            }
            count += 1;
            match next {
                Some(n) => cluster = n,
                None => break,
            }
        }

        self.write_bitmap(&bitmap)?;
        Ok(())
    }

    /// Write a FAT entry (4 bytes per entry).
    fn write_fat_entry(&mut self, cluster: u32, value: u32) -> Result<(), FilesystemError> {
        let offset = self.fat_offset() + cluster as u64 * 4;
        self.reader.seek(SeekFrom::Start(offset))?;
        self.reader.write_all(&value.to_le_bytes())?;
        Ok(())
    }

    /// Write data to a cluster's location on disk.
    fn write_cluster_data(&mut self, cluster: u32, data: &[u8]) -> Result<(), FilesystemError> {
        let offset = self.cluster_offset(cluster);
        self.reader.seek(SeekFrom::Start(offset))?;
        self.reader.write_all(data)?;
        Ok(())
    }

    /// Count free clusters by scanning the bitmap.
    fn count_free_clusters(&mut self) -> Result<u32, FilesystemError> {
        let bitmap = self.read_bitmap()?;
        let used = count_set_bits(&bitmap, self.cluster_count);
        Ok(self.cluster_count - used as u32)
    }

    // -- Name & Checksum --

    /// Uppercase a UTF-16 character (ASCII range only for MVP).
    fn upcase_char(c: u16) -> u16 {
        if (0x0061..=0x007A).contains(&c) {
            c - 0x0020
        } else {
            c
        }
    }

    /// Compute the exFAT name hash for a filename.
    fn name_hash(name: &str) -> u16 {
        let mut hash: u16 = 0;
        for c in name.encode_utf16() {
            let uc = Self::upcase_char(c);
            let lo = (uc & 0xFF) as u8;
            let hi = (uc >> 8) as u8;
            hash = hash.rotate_right(1).wrapping_add(lo as u16);
            hash = hash.rotate_right(1).wrapping_add(hi as u16);
        }
        hash
    }

    /// Compute the entry set checksum over raw entry bytes.
    /// Skips bytes 2-3 of the first entry (the SetChecksum field itself).
    fn entry_set_checksum(entries: &[u8]) -> u16 {
        let mut cs: u16 = 0;
        for (i, &byte) in entries.iter().enumerate() {
            // Skip bytes 2 and 3 (SetChecksum field in the File entry)
            if i == 2 || i == 3 {
                continue;
            }
            cs = if cs & 1 != 0 {
                0x8000 | (cs >> 1)
            } else {
                cs >> 1
            };
            cs = cs.wrapping_add(byte as u16);
        }
        cs
    }

    /// Validate an exFAT filename.
    fn validate_exfat_name(name: &str) -> Result<(), FilesystemError> {
        if name.is_empty() || name.len() > 255 {
            return Err(FilesystemError::InvalidData(
                "exFAT name must be 1-255 characters".into(),
            ));
        }
        const FORBIDDEN: &[char] = &['"', '*', '/', ':', '<', '>', '?', '\\', '|'];
        for c in name.chars() {
            if FORBIDDEN.contains(&c) || (c as u32) < 0x20 {
                return Err(FilesystemError::InvalidData(format!(
                    "invalid character '{}' in exFAT filename",
                    c
                )));
            }
        }
        Ok(())
    }

    // -- Directory Entry Sets --

    /// Build a complete entry set (File + Stream + FileName entries).
    fn build_entry_set(name: &str, attrs: u16, first_cluster: u32, data_len: u64) -> Vec<u8> {
        let name_utf16: Vec<u16> = name.encode_utf16().collect();
        let name_entry_count = (name_utf16.len() + 14) / 15; // ceil(len/15)
        let secondary_count = 1 + name_entry_count; // stream + name entries
        let total_entries = 1 + secondary_count; // file + secondaries
        let mut entries = vec![0u8; total_entries * 32];

        let ts = current_exfat_timestamp();

        // File Entry (0x85)
        entries[0] = ENTRY_TYPE_FILE;
        entries[1] = secondary_count as u8;
        // [2-3] = SetChecksum (filled later)
        entries[4] = (attrs & 0xFF) as u8;
        entries[5] = (attrs >> 8) as u8;
        // CreateTimestamp [8-11]
        entries[8..12].copy_from_slice(&ts.to_le_bytes());
        // ModifyTimestamp [12-15]
        entries[12..16].copy_from_slice(&ts.to_le_bytes());
        // AccessTimestamp [16-19]
        entries[16..20].copy_from_slice(&ts.to_le_bytes());

        // Stream Extension (0xC0) at offset 32
        let s = 32;
        entries[s] = ENTRY_TYPE_STREAM_EXT;
        entries[s + 1] = 0x01; // AllocationPossible, NoFatChain=0 (use FAT chain)
        entries[s + 3] = name_utf16.len() as u8; // NameLength
        let nh = Self::name_hash(name);
        entries[s + 4] = (nh & 0xFF) as u8;
        entries[s + 5] = (nh >> 8) as u8;
        // ValidDataLength [8-15]
        entries[s + 8..s + 16].copy_from_slice(&data_len.to_le_bytes());
        // FirstCluster [20-23]
        entries[s + 20..s + 24].copy_from_slice(&first_cluster.to_le_bytes());
        // DataLength [24-31]
        entries[s + 24..s + 32].copy_from_slice(&data_len.to_le_bytes());

        // File Name entries (0xC1) starting at offset 64
        for i in 0..name_entry_count {
            let off = 64 + i * 32;
            entries[off] = ENTRY_TYPE_FILE_NAME;
            entries[off + 1] = 0x00;
            for j in 0..15 {
                let char_idx = i * 15 + j;
                if char_idx < name_utf16.len() {
                    let c = name_utf16[char_idx];
                    entries[off + 2 + j * 2] = (c & 0xFF) as u8;
                    entries[off + 2 + j * 2 + 1] = (c >> 8) as u8;
                }
            }
        }

        // Compute and set the entry set checksum
        let checksum = Self::entry_set_checksum(&entries);
        entries[2] = (checksum & 0xFF) as u8;
        entries[3] = (checksum >> 8) as u8;

        entries
    }

    /// Check if a name already exists in a directory's raw data.
    fn name_exists_in_dir(dir_data: &[u8], name: &str) -> bool {
        let mut pos = 0;
        while pos + 32 <= dir_data.len() {
            let entry_type = dir_data[pos];
            if entry_type == 0x00 {
                break;
            }
            if entry_type == ENTRY_TYPE_FILE {
                let secondary_count = dir_data[pos + 1] as usize;
                let stream_pos = pos + 32;
                if stream_pos + 32 <= dir_data.len()
                    && dir_data[stream_pos] == ENTRY_TYPE_STREAM_EXT
                {
                    let name_length = dir_data[stream_pos + 3] as usize;
                    // Collect name from 0xC1 entries
                    let mut name_chars = Vec::new();
                    for i in 0..secondary_count.saturating_sub(1) {
                        let fn_pos = pos + 64 + i * 32;
                        if fn_pos + 32 > dir_data.len() || dir_data[fn_pos] != ENTRY_TYPE_FILE_NAME
                        {
                            break;
                        }
                        for j in 0..15 {
                            if name_chars.len() >= name_length {
                                break;
                            }
                            let co = fn_pos + 2 + j * 2;
                            if co + 1 < dir_data.len() {
                                name_chars
                                    .push(u16::from_le_bytes([dir_data[co], dir_data[co + 1]]));
                            }
                        }
                    }
                    name_chars.truncate(name_length);
                    let existing = String::from_utf16_lossy(&name_chars);
                    if existing.eq_ignore_ascii_case(name) {
                        return true;
                    }
                }
                pos += 32 * (1 + secondary_count);
                continue;
            }
            pos += 32;
        }
        false
    }

    // -- Directory Operations --

    /// Add an entry set to a parent directory. Finds free slots or extends the chain.
    fn add_entry_to_directory(
        &mut self,
        parent: &FileEntry,
        entry_bytes: &[u8],
    ) -> Result<(), FilesystemError> {
        let parent_cluster = if parent.path == "/" {
            self.root_cluster
        } else {
            parent.location as u32
        };

        let dir_data = self.read_cluster_chain(parent_cluster, None)?;
        let entries_needed = entry_bytes.len() / 32;

        // Find a run of free slots (type 0x00 or deleted entries with InUse bit clear)
        let mut pos = 0;
        let mut run_start = None;
        let mut run_count = 0;

        while pos + 32 <= dir_data.len() {
            let t = dir_data[pos];
            if t == 0x00 || (t & 0x80 == 0 && t != 0x00) {
                // Free or deleted entry
                if run_start.is_none() {
                    run_start = Some(pos);
                    run_count = 1;
                } else {
                    run_count += 1;
                }
                // If this is end-of-directory (0x00), all remaining slots are free too
                if t == 0x00 {
                    let remaining_slots = (dir_data.len() - pos) / 32;
                    run_count = run_count + remaining_slots - 1; // -1 since we already counted this one
                    break;
                }
                if run_count >= entries_needed {
                    break;
                }
            } else {
                run_start = None;
                run_count = 0;
            }
            pos += 32;
        }

        if run_count >= entries_needed {
            // Write entry set at run_start position
            let start = run_start.unwrap();
            let cluster_offset_in_chain = start / self.cluster_size as usize;
            let offset_in_cluster = start % self.cluster_size as usize;

            // Walk to the right cluster
            let mut cluster = parent_cluster;
            for _ in 0..cluster_offset_in_chain {
                match self.next_cluster(cluster)? {
                    Some(next) => cluster = next,
                    None => {
                        return Err(FilesystemError::InvalidData(
                            "directory chain shorter than expected".into(),
                        ))
                    }
                }
            }

            // Write the entry bytes, potentially spanning clusters
            let mut written = 0;
            let mut cur_cluster = cluster;
            let mut cur_offset = offset_in_cluster;
            while written < entry_bytes.len() {
                let avail = self.cluster_size as usize - cur_offset;
                let to_write = avail.min(entry_bytes.len() - written);
                let disk_offset = self.cluster_offset(cur_cluster) + cur_offset as u64;
                self.reader.seek(SeekFrom::Start(disk_offset))?;
                self.reader
                    .write_all(&entry_bytes[written..written + to_write])?;
                written += to_write;
                cur_offset = 0;
                if written < entry_bytes.len() {
                    match self.next_cluster(cur_cluster)? {
                        Some(next) => cur_cluster = next,
                        None => {
                            return Err(FilesystemError::InvalidData(
                                "directory chain too short for entry".into(),
                            ))
                        }
                    }
                }
            }

            // If we overwrote end-of-directory markers, ensure there's a 0x00 terminator after
            let end_pos = start + entry_bytes.len();
            if end_pos < dir_data.len() && dir_data[end_pos] != 0x00 {
                // The next slot should already be 0x00 or another entry; only set if needed
            }
        } else {
            // Need to extend directory: allocate a new cluster
            let new_cluster = self.allocate_clusters(1)?;
            // Zero the new cluster
            let zeroed = vec![0u8; self.cluster_size as usize];
            self.write_cluster_data(new_cluster, &zeroed)?;

            // Append to chain: walk to last cluster
            let mut last = parent_cluster;
            loop {
                match self.next_cluster(last)? {
                    Some(next) => last = next,
                    None => break,
                }
            }
            // Link last -> new_cluster
            self.write_fat_entry(last, new_cluster)?;
            self.write_fat_entry(new_cluster, 0xFFFFFFFF)?;

            // Write entry set at the start of the new cluster
            let disk_offset = self.cluster_offset(new_cluster);
            self.reader.seek(SeekFrom::Start(disk_offset))?;
            self.reader.write_all(entry_bytes)?;
        }

        Ok(())
    }

    /// Remove an entry set from a directory by clearing InUse bits.
    fn remove_entry_from_directory(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        let parent_cluster = if parent.path == "/" {
            self.root_cluster
        } else {
            parent.location as u32
        };

        let dir_data = self.read_cluster_chain(parent_cluster, None)?;

        // Find the entry set by matching name
        let mut pos = 0;
        while pos + 32 <= dir_data.len() {
            let t = dir_data[pos];
            if t == 0x00 {
                break;
            }
            if t == ENTRY_TYPE_FILE {
                let secondary_count = dir_data[pos + 1] as usize;
                let stream_pos = pos + 32;
                if stream_pos + 32 <= dir_data.len()
                    && dir_data[stream_pos] == ENTRY_TYPE_STREAM_EXT
                {
                    let name_length = dir_data[stream_pos + 3] as usize;
                    let mut name_chars = Vec::new();
                    for i in 0..secondary_count.saturating_sub(1) {
                        let fn_pos = pos + 64 + i * 32;
                        if fn_pos + 32 > dir_data.len() || dir_data[fn_pos] != ENTRY_TYPE_FILE_NAME
                        {
                            break;
                        }
                        for j in 0..15 {
                            if name_chars.len() >= name_length {
                                break;
                            }
                            let co = fn_pos + 2 + j * 2;
                            if co + 1 < dir_data.len() {
                                name_chars
                                    .push(u16::from_le_bytes([dir_data[co], dir_data[co + 1]]));
                            }
                        }
                    }
                    name_chars.truncate(name_length);
                    let existing = String::from_utf16_lossy(&name_chars);

                    if existing.eq_ignore_ascii_case(&entry.name) {
                        // Found it — clear InUse bit on all entries in the set
                        let total = 1 + secondary_count;
                        for i in 0..total {
                            let entry_pos = pos + i * 32;
                            // Calculate which cluster and offset
                            let cluster_idx = entry_pos / self.cluster_size as usize;
                            let offset_in_cluster = entry_pos % self.cluster_size as usize;

                            let mut cluster = parent_cluster;
                            for _ in 0..cluster_idx {
                                match self.next_cluster(cluster)? {
                                    Some(next) => cluster = next,
                                    None => break,
                                }
                            }

                            let disk_offset =
                                self.cluster_offset(cluster) + offset_in_cluster as u64;
                            self.reader.seek(SeekFrom::Start(disk_offset))?;
                            let mut type_byte = [0u8; 1];
                            self.reader.read_exact(&mut type_byte)?;
                            // Clear bit 7 (InUse)
                            type_byte[0] &= 0x7F;
                            self.reader.seek(SeekFrom::Start(disk_offset))?;
                            self.reader.write_all(&type_byte)?;
                        }
                        return Ok(());
                    }
                }
                pos += 32 * (1 + secondary_count);
                continue;
            }
            pos += 32;
        }

        Err(FilesystemError::NotFound(entry.name.clone()))
    }

    // -- Boot Checksum --

    /// Recalculate and write the boot region checksum (main + backup).
    fn recalculate_boot_checksum(&mut self) -> Result<(), FilesystemError> {
        // Read main boot region (sectors 0-11)
        let boot_region_size = 12 * self.bytes_per_sector;
        self.reader.seek(SeekFrom::Start(self.partition_offset))?;
        let mut boot_region = vec![0u8; boot_region_size as usize];
        self.reader.read_exact(&mut boot_region)?;

        let checksum = compute_exfat_boot_checksum(&boot_region, self.bytes_per_sector);
        let checksum_data = build_checksum_sector(checksum, self.bytes_per_sector);

        // Write checksum to sector 11 of main boot region
        let cs_offset = self.partition_offset + 11 * self.bytes_per_sector;
        self.reader.seek(SeekFrom::Start(cs_offset))?;
        self.reader.write_all(&checksum_data)?;

        // Copy main boot region (sectors 0-11) to backup (sectors 12-23)
        // Re-read to include the checksum we just wrote
        self.reader.seek(SeekFrom::Start(self.partition_offset))?;
        let mut full_boot = vec![0u8; 12 * self.bytes_per_sector as usize];
        self.reader.read_exact(&mut full_boot)?;

        let backup_offset = self.partition_offset + 12 * self.bytes_per_sector;
        self.reader.seek(SeekFrom::Start(backup_offset))?;
        self.reader.write_all(&full_boot)?;

        Ok(())
    }
}

// =============================================================================
// EditableFilesystem implementation
// =============================================================================

impl<R: Read + Write + Seek + Send> EditableFilesystem for ExfatFilesystem<R> {
    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn std::io::Read,
        data_len: u64,
        _options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Self::validate_exfat_name(name)?;

        // Check for duplicate
        let parent_cluster = if parent.path == "/" {
            self.root_cluster
        } else {
            parent.location as u32
        };
        let dir_data = self.read_cluster_chain(parent_cluster, None)?;
        if Self::name_exists_in_dir(&dir_data, name) {
            return Err(FilesystemError::AlreadyExists(name.to_string()));
        }

        // Allocate clusters for file data
        let cluster_size = self.cluster_size as usize;
        let clusters_needed = if data_len == 0 {
            0
        } else {
            ((data_len as usize + cluster_size - 1) / cluster_size) as u32
        };

        let first_cluster = if clusters_needed > 0 {
            self.allocate_clusters(clusters_needed)?
        } else {
            0
        };

        // Write file data to clusters
        if clusters_needed > 0 {
            let mut cluster = first_cluster;
            let mut remaining = data_len as usize;
            let mut buf = vec![0u8; cluster_size];
            loop {
                if cluster < 2 || remaining == 0 {
                    break;
                }
                let to_read = remaining.min(cluster_size);
                buf.fill(0);
                let mut total_read = 0;
                while total_read < to_read {
                    match data.read(&mut buf[total_read..to_read]) {
                        Ok(0) => break,
                        Ok(n) => total_read += n,
                        Err(e) => return Err(FilesystemError::Io(e)),
                    }
                }
                self.write_cluster_data(cluster, &buf)?;
                remaining -= to_read;
                match self.next_cluster(cluster)? {
                    Some(next) => cluster = next,
                    None => break,
                }
            }
        }

        // Build entry set and add to directory
        let entry_bytes = Self::build_entry_set(name, 0x20, first_cluster, data_len); // 0x20 = Archive
        self.add_entry_to_directory(parent, &entry_bytes)?;

        let path = if parent.path == "/" {
            format!("/{name}")
        } else {
            format!("{}/{name}", parent.path)
        };

        Ok(FileEntry::new_file(
            name.to_string(),
            path,
            data_len,
            first_cluster as u64,
        ))
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Self::validate_exfat_name(name)?;

        let parent_cluster = if parent.path == "/" {
            self.root_cluster
        } else {
            parent.location as u32
        };
        let dir_data = self.read_cluster_chain(parent_cluster, None)?;
        if Self::name_exists_in_dir(&dir_data, name) {
            return Err(FilesystemError::AlreadyExists(name.to_string()));
        }

        // Allocate 1 cluster for new directory (exFAT dirs don't have . and ..)
        let new_cluster = self.allocate_clusters(1)?;
        let zeroed = vec![0u8; self.cluster_size as usize];
        self.write_cluster_data(new_cluster, &zeroed)?;

        // Build entry set with directory attribute
        let entry_bytes = Self::build_entry_set(name, ATTR_DIRECTORY as u16, new_cluster, 0);
        self.add_entry_to_directory(parent, &entry_bytes)?;

        let path = if parent.path == "/" {
            format!("/{name}")
        } else {
            format!("{}/{name}", parent.path)
        };

        Ok(FileEntry::new_directory(
            name.to_string(),
            path,
            new_cluster as u64,
        ))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        if entry.is_directory() {
            let children = self.list_directory(entry)?;
            if !children.is_empty() {
                return Err(FilesystemError::InvalidData(format!(
                    "directory '{}' is not empty",
                    entry.name
                )));
            }
        }

        self.remove_entry_from_directory(parent, entry)?;

        if entry.location >= 2 {
            self.free_cluster_chain_rw(entry.location as u32)?;
        }

        Ok(())
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.recalculate_boot_checksum()?;
        self.reader.flush()?;
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        let free = self.count_free_clusters()?;
        Ok(free as u64 * self.cluster_size)
    }
}

/// Count set bits in a bitmap, limited to `max_clusters` bits.
fn count_set_bits(data: &[u8], max_clusters: u32) -> u64 {
    let mut count = 0u64;
    let full_bytes = (max_clusters / 8) as usize;
    let remaining_bits = (max_clusters % 8) as u8;

    for &byte in &data[..full_bytes.min(data.len())] {
        count += byte.count_ones() as u64;
    }

    if remaining_bits > 0 && full_bytes < data.len() {
        let mask = (1u8 << remaining_bits) - 1;
        count += (data[full_bytes] & mask).count_ones() as u64;
    }

    count
}

// =============================================================================
// Compaction
// =============================================================================

pub struct CompactExfatInfo {
    pub original_size: u64,
    pub compacted_size: u64,
    pub clusters_used: u32,
}

/// A reader that streams only used clusters of an exFAT partition.
///
/// Layout: boot region (12 sectors) + FAT + used clusters in order
pub struct CompactExfatReader<R> {
    source: R,
    partition_offset: u64,
    cluster_size: u64,
    bytes_per_sector: u64,

    // Pre-read regions
    boot_region: Vec<u8>,
    fat_data: Vec<u8>,

    // Used clusters list
    used_cluster_list: Vec<u32>,

    // Streaming state
    position: u64,
    total_size: u64,
    cluster_buf: Vec<u8>,

    // Region boundaries
    boot_region_size: u64,
    fat_region_size: u64,
}

impl<R: Read + Seek> CompactExfatReader<R> {
    pub fn new(
        mut source: R,
        partition_offset: u64,
    ) -> Result<(Self, CompactExfatInfo), FilesystemError> {
        // Read VBR
        source.seek(SeekFrom::Start(partition_offset))?;
        let mut vbr_buf = [0u8; 512];
        source
            .read_exact(&mut vbr_buf)
            .map_err(|e| FilesystemError::Parse(format!("cannot read exFAT VBR: {e}")))?;

        let vbr = parse_exfat_vbr(&vbr_buf)?;

        let bytes_per_sector = 1u64 << vbr.bytes_per_sector_shift;
        let sectors_per_cluster = 1u64 << vbr.sectors_per_cluster_shift;
        let cluster_size = bytes_per_sector * sectors_per_cluster;
        let original_size = vbr.volume_length * bytes_per_sector;

        // Read boot region (first 12 sectors for main boot region)
        let boot_region_size = 12 * bytes_per_sector;
        source.seek(SeekFrom::Start(partition_offset))?;
        let mut boot_region = vec![0u8; boot_region_size as usize];
        source.read_exact(&mut boot_region)?;

        // Read FAT
        let fat_offset = partition_offset + vbr.fat_offset as u64 * bytes_per_sector;
        let fat_size = vbr.fat_length as u64 * bytes_per_sector;
        source.seek(SeekFrom::Start(fat_offset))?;
        let mut fat_data = vec![0u8; fat_size as usize];
        source.read_exact(&mut fat_data)?;

        // Find allocation bitmap in root directory
        let cluster_heap_offset = vbr.cluster_heap_offset as u64 * bytes_per_sector;
        let root_offset =
            partition_offset + cluster_heap_offset + (vbr.root_cluster as u64 - 2) * cluster_size;

        source.seek(SeekFrom::Start(root_offset))?;
        let mut root_buf = vec![0u8; cluster_size as usize];
        source.read_exact(&mut root_buf)?;

        let mut bitmap_cluster = 0u32;
        let mut bitmap_size = 0u64;

        let mut pos = 0;
        while pos + 32 <= root_buf.len() {
            if root_buf[pos] == 0x00 {
                break;
            }
            if root_buf[pos] == ENTRY_TYPE_ALLOCATION_BITMAP {
                bitmap_cluster = u32::from_le_bytes([
                    root_buf[pos + 20],
                    root_buf[pos + 21],
                    root_buf[pos + 22],
                    root_buf[pos + 23],
                ]);
                bitmap_size = u64::from_le_bytes([
                    root_buf[pos + 24],
                    root_buf[pos + 25],
                    root_buf[pos + 26],
                    root_buf[pos + 27],
                    root_buf[pos + 28],
                    root_buf[pos + 29],
                    root_buf[pos + 30],
                    root_buf[pos + 31],
                ]);
                break;
            }
            pos += 32;
        }

        // Read bitmap
        let mut used_cluster_list = Vec::new();
        if bitmap_cluster >= 2 {
            let bmp_offset =
                partition_offset + cluster_heap_offset + (bitmap_cluster as u64 - 2) * cluster_size;
            source.seek(SeekFrom::Start(bmp_offset))?;
            let mut bitmap_data = vec![0u8; bitmap_size as usize];
            source.read_exact(&mut bitmap_data)?;

            for (byte_idx, &byte) in bitmap_data.iter().enumerate() {
                for bit in 0..8u8 {
                    let cluster = byte_idx as u32 * 8 + bit as u32;
                    if cluster < vbr.cluster_count && byte & (1 << bit) != 0 {
                        used_cluster_list.push(cluster + 2); // Clusters are 2-based
                    }
                }
            }
        }

        let clusters_used = used_cluster_list.len() as u32;
        let compacted_size = boot_region_size + fat_size + clusters_used as u64 * cluster_size;

        Ok((
            CompactExfatReader {
                source,
                partition_offset,
                cluster_size,
                bytes_per_sector,
                boot_region,
                fat_data,
                used_cluster_list,
                position: 0,
                total_size: compacted_size,
                cluster_buf: Vec::new(),
                boot_region_size,
                fat_region_size: fat_size,
            },
            CompactExfatInfo {
                original_size,
                compacted_size,
                clusters_used,
            },
        ))
    }
}

impl<R: Read + Seek> Read for CompactExfatReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.position >= self.total_size {
            return Ok(0);
        }

        let mut bytes_written = 0;

        while bytes_written < buf.len() && self.position < self.total_size {
            if self.position < self.boot_region_size {
                // Boot region
                let boot_pos = self.position as usize;
                let avail = self.boot_region.len() - boot_pos;
                let to_copy = avail.min(buf.len() - bytes_written);
                buf[bytes_written..bytes_written + to_copy]
                    .copy_from_slice(&self.boot_region[boot_pos..boot_pos + to_copy]);
                bytes_written += to_copy;
                self.position += to_copy as u64;
            } else if self.position < self.boot_region_size + self.fat_region_size {
                // FAT region
                let fat_pos = (self.position - self.boot_region_size) as usize;
                let avail = self.fat_data.len() - fat_pos;
                let to_copy = avail.min(buf.len() - bytes_written);
                buf[bytes_written..bytes_written + to_copy]
                    .copy_from_slice(&self.fat_data[fat_pos..fat_pos + to_copy]);
                bytes_written += to_copy;
                self.position += to_copy as u64;
            } else {
                // Cluster data region
                let data_pos = self.position - self.boot_region_size - self.fat_region_size;
                let cluster_idx = (data_pos / self.cluster_size) as usize;
                let within_cluster = (data_pos % self.cluster_size) as usize;

                if cluster_idx >= self.used_cluster_list.len() {
                    break;
                }

                // Read cluster if needed
                if self.cluster_buf.is_empty() || within_cluster == 0 {
                    let cluster_num = self.used_cluster_list[cluster_idx];
                    let cluster_heap_offset = u32::from_le_bytes([
                        self.boot_region[0x58],
                        self.boot_region[0x59],
                        self.boot_region[0x5A],
                        self.boot_region[0x5B],
                    ]) as u64
                        * self.bytes_per_sector;

                    let src_offset = self.partition_offset
                        + cluster_heap_offset
                        + (cluster_num as u64 - 2) * self.cluster_size;

                    self.source
                        .seek(SeekFrom::Start(src_offset))
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                    self.cluster_buf.resize(self.cluster_size as usize, 0);
                    self.source
                        .read_exact(&mut self.cluster_buf)
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                }

                let avail = self.cluster_size as usize - within_cluster;
                let to_copy = avail.min(buf.len() - bytes_written);
                buf[bytes_written..bytes_written + to_copy]
                    .copy_from_slice(&self.cluster_buf[within_cluster..within_cluster + to_copy]);
                bytes_written += to_copy;
                self.position += to_copy as u64;
            }
        }

        Ok(bytes_written)
    }
}

// =============================================================================
// Resize
// =============================================================================

/// Resize an exFAT partition with full bitmap resize.
///
/// This updates the allocation bitmap, FAT, VBR fields, and boot region checksums.
pub fn resize_exfat_in_place(
    file: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    new_volume_length_sectors: u64,
    log_cb: &mut impl FnMut(&str),
) -> Result<bool> {
    // Read VBR
    file.seek(SeekFrom::Start(partition_offset))?;
    let mut vbr = [0u8; 512];
    file.read_exact(&mut vbr)?;

    if &vbr[3..11] != b"EXFAT   " {
        return Ok(false);
    }

    let bytes_per_sector_shift = vbr[0x6C];
    let sectors_per_cluster_shift = vbr[0x6D];
    let bytes_per_sector = 1u64 << bytes_per_sector_shift;
    let sectors_per_cluster = 1u64 << sectors_per_cluster_shift;
    let cluster_size = bytes_per_sector * sectors_per_cluster;

    let old_volume_length = u64::from_le_bytes([
        vbr[0x48], vbr[0x49], vbr[0x4A], vbr[0x4B], vbr[0x4C], vbr[0x4D], vbr[0x4E], vbr[0x4F],
    ]);

    if old_volume_length == new_volume_length_sectors {
        return Ok(false);
    }

    let cluster_heap_offset = u32::from_le_bytes([vbr[0x58], vbr[0x59], vbr[0x5A], vbr[0x5B]]);
    let old_cluster_count = u32::from_le_bytes([vbr[0x5C], vbr[0x5D], vbr[0x5E], vbr[0x5F]]);
    let root_cluster = u32::from_le_bytes([vbr[0x60], vbr[0x61], vbr[0x62], vbr[0x63]]);

    // Calculate new cluster count
    let data_sectors = new_volume_length_sectors.saturating_sub(cluster_heap_offset as u64);
    let new_cluster_count = (data_sectors / sectors_per_cluster) as u32;

    if new_cluster_count == 0 {
        bail!("exFAT resize: new size too small, no clusters available");
    }

    // Find allocation bitmap in root directory
    let root_offset = partition_offset
        + cluster_heap_offset as u64 * bytes_per_sector
        + (root_cluster as u64 - 2) * cluster_size;
    file.seek(SeekFrom::Start(root_offset))?;
    let mut root_buf = vec![0u8; cluster_size as usize];
    file.read_exact(&mut root_buf)?;

    let mut bitmap_cluster = 0u32;
    let mut bitmap_size = 0u64;
    let mut bitmap_entry_offset = 0usize;

    let mut pos = 0;
    while pos + 32 <= root_buf.len() {
        if root_buf[pos] == 0x00 {
            break;
        }
        if root_buf[pos] == ENTRY_TYPE_ALLOCATION_BITMAP {
            bitmap_cluster = u32::from_le_bytes([
                root_buf[pos + 20],
                root_buf[pos + 21],
                root_buf[pos + 22],
                root_buf[pos + 23],
            ]);
            bitmap_size = u64::from_le_bytes([
                root_buf[pos + 24],
                root_buf[pos + 25],
                root_buf[pos + 26],
                root_buf[pos + 27],
                root_buf[pos + 28],
                root_buf[pos + 29],
                root_buf[pos + 30],
                root_buf[pos + 31],
            ]);
            bitmap_entry_offset = pos;
            break;
        }
        pos += 32;
    }

    if bitmap_cluster < 2 {
        bail!("exFAT resize: allocation bitmap not found");
    }

    // Check last used cluster fits within new size
    let bmp_offset = partition_offset
        + cluster_heap_offset as u64 * bytes_per_sector
        + (bitmap_cluster as u64 - 2) * cluster_size;
    // Read bitmap with sector-aligned size for raw device compatibility
    let bitmap_aligned_size = ((bitmap_size as usize + 511) / 512) * 512;
    file.seek(SeekFrom::Start(bmp_offset))?;
    let mut bitmap_data = vec![0u8; bitmap_aligned_size];
    file.read_exact(&mut bitmap_data)?;
    bitmap_data.truncate(bitmap_size as usize);

    // Verify no data beyond new cluster count
    if new_cluster_count < old_cluster_count {
        for byte_idx in (0..bitmap_data.len()).rev() {
            for bit in (0..8).rev() {
                let cluster = byte_idx as u32 * 8 + bit as u32;
                if cluster >= new_cluster_count && cluster < old_cluster_count {
                    if bitmap_data[byte_idx] & (1 << bit) != 0 {
                        bail!(
                            "exFAT resize rejected: cluster {} is in use but beyond new boundary ({})",
                            cluster + 2,
                            new_cluster_count
                        );
                    }
                }
            }
        }
    }

    log_cb(&format!(
        "exFAT resize: {} -> {} clusters ({} -> {} sectors)",
        old_cluster_count, new_cluster_count, old_volume_length, new_volume_length_sectors,
    ));

    // Resize bitmap
    let new_bitmap_size = ((new_cluster_count as u64 + 7) / 8) as usize;
    if new_bitmap_size > bitmap_data.len() {
        // Growing: extend with zeros
        bitmap_data.resize(new_bitmap_size, 0);
    } else {
        // Shrinking: truncate and clear trailing bits in last byte
        bitmap_data.truncate(new_bitmap_size);
        let remaining_bits = new_cluster_count % 8;
        if remaining_bits > 0 && !bitmap_data.is_empty() {
            let mask = (1u8 << remaining_bits) - 1;
            let last = bitmap_data.len() - 1;
            bitmap_data[last] &= mask;
        }
    }

    // Write resized bitmap (sector-aligned for raw device compatibility)
    let write_aligned_size = ((new_bitmap_size + 511) / 512) * 512;
    bitmap_data.resize(write_aligned_size, 0);
    file.seek(SeekFrom::Start(bmp_offset))?;
    file.write_all(&bitmap_data)?;

    // Update bitmap directory entry with new size
    let new_bitmap_size_u64 = new_bitmap_size as u64;
    root_buf[bitmap_entry_offset + 24..bitmap_entry_offset + 32]
        .copy_from_slice(&new_bitmap_size_u64.to_le_bytes());
    file.seek(SeekFrom::Start(root_offset))?;
    file.write_all(&root_buf)?;

    // Verify FAT has no entries beyond new cluster count (for shrinking)
    // Uses sector-aligned I/O: read/write whole 512-byte sectors at a time
    if new_cluster_count < old_cluster_count {
        let fat_offset = partition_offset
            + u32::from_le_bytes([vbr[0x50], vbr[0x51], vbr[0x52], vbr[0x53]]) as u64
                * bytes_per_sector;
        let first_cluster = new_cluster_count + 2;
        let last_cluster = old_cluster_count + 2; // exclusive

        // Process one 512-byte sector at a time (128 FAT entries per sector)
        let first_byte = first_cluster as u64 * 4;
        let last_byte = last_cluster as u64 * 4;
        let first_sector_off = (first_byte / 512) * 512;
        let last_sector_off = ((last_byte + 511) / 512) * 512;

        let mut sector = [0u8; 512];
        let mut sector_off = first_sector_off;
        while sector_off < last_sector_off {
            file.seek(SeekFrom::Start(fat_offset + sector_off))?;
            file.read_exact(&mut sector)?;

            let mut modified = false;
            // Check each 4-byte entry within this sector
            for byte_pos in (0..512).step_by(4) {
                let abs_byte = sector_off + byte_pos as u64;
                let cluster = abs_byte / 4;
                if cluster >= first_cluster as u64 && cluster < last_cluster as u64 {
                    let entry = u32::from_le_bytes([
                        sector[byte_pos],
                        sector[byte_pos + 1],
                        sector[byte_pos + 2],
                        sector[byte_pos + 3],
                    ]);
                    if entry != 0 {
                        sector[byte_pos..byte_pos + 4].copy_from_slice(&0u32.to_le_bytes());
                        modified = true;
                    }
                }
            }

            if modified {
                file.seek(SeekFrom::Start(fat_offset + sector_off))?;
                file.write_all(&sector)?;
            }

            sector_off += 512;
        }
    }

    // Patch VBR fields
    vbr[0x48..0x50].copy_from_slice(&new_volume_length_sectors.to_le_bytes());
    vbr[0x5C..0x60].copy_from_slice(&new_cluster_count.to_le_bytes());

    // Write main VBR (sector 0)
    file.seek(SeekFrom::Start(partition_offset))?;
    file.write_all(&vbr)?;

    // Read the full main boot region (sectors 0-11) for checksum
    file.seek(SeekFrom::Start(partition_offset))?;
    let boot_region_sectors = 12u64;
    let boot_region_size = boot_region_sectors * bytes_per_sector;
    let mut boot_region = vec![0u8; boot_region_size as usize];
    file.read_exact(&mut boot_region)?;
    // The VBR we just wrote is already at sector 0, but re-read to get sectors 1-11
    // Actually we need to update sector 0 in boot_region with our patched VBR
    boot_region[..512].copy_from_slice(&vbr);

    // Calculate boot region checksum (over sectors 0-10, excluding bytes 106-107 and 112 of sector 0)
    let checksum = compute_exfat_boot_checksum(&boot_region, bytes_per_sector);

    // Write checksum to sector 11 of main boot region
    let checksum_sector_offset = partition_offset + 11 * bytes_per_sector;
    let checksum_data = build_checksum_sector(checksum, bytes_per_sector);
    file.seek(SeekFrom::Start(checksum_sector_offset))?;
    file.write_all(&checksum_data)?;

    // Write backup boot region (sectors 12-23): copy main boot region
    let backup_offset = partition_offset + 12 * bytes_per_sector;
    file.seek(SeekFrom::Start(backup_offset))?;
    // Write sectors 0-10 of backup
    file.write_all(&boot_region[..11 * bytes_per_sector as usize])?;
    // Write checksum sector for backup
    file.write_all(&checksum_data)?;

    log_cb(&format!(
        "exFAT: resized to {} clusters, updated bitmap, FAT, VBR, and checksums",
        new_cluster_count
    ));

    Ok(true)
}

/// Compute the exFAT boot region checksum over sectors 0-10.
fn compute_exfat_boot_checksum(boot_region: &[u8], bytes_per_sector: u64) -> u32 {
    let mut checksum: u32 = 0;
    let check_size = 11 * bytes_per_sector as usize;

    for (i, &byte) in boot_region[..check_size].iter().enumerate() {
        // Skip VolumeFlags (bytes 106-107) and PercentInUse (byte 112) in sector 0
        if i == 106 || i == 107 || i == 112 {
            continue;
        }
        checksum = if checksum & 1 != 0 {
            0x80000000 | (checksum >> 1)
        } else {
            checksum >> 1
        };
        checksum = checksum.wrapping_add(byte as u32);
    }

    checksum
}

/// Build a full sector filled with the checksum value repeated.
fn build_checksum_sector(checksum: u32, bytes_per_sector: u64) -> Vec<u8> {
    let mut sector = vec![0u8; bytes_per_sector as usize];
    let checksum_bytes = checksum.to_le_bytes();
    for i in (0..sector.len()).step_by(4) {
        sector[i..i + 4].copy_from_slice(&checksum_bytes);
    }
    sector
}

// =============================================================================
// Validation
// =============================================================================

/// Validate basic exFAT integrity.
pub fn validate_exfat_integrity(
    file: &mut (impl Read + Seek),
    partition_offset: u64,
    log_cb: &mut impl FnMut(&str),
) -> Result<bool> {
    file.seek(SeekFrom::Start(partition_offset))?;
    let mut vbr = [0u8; 512];
    file.read_exact(&mut vbr)?;

    if &vbr[3..11] != b"EXFAT   " {
        log_cb("exFAT validation: not an exFAT volume");
        return Ok(false);
    }

    let bytes_per_sector_shift = vbr[0x6C];
    let bytes_per_sector = 1u64 << bytes_per_sector_shift;

    // Verify boot region checksum
    file.seek(SeekFrom::Start(partition_offset))?;
    let boot_region_size = 12 * bytes_per_sector;
    let mut boot_region = vec![0u8; boot_region_size as usize];
    file.read_exact(&mut boot_region)?;

    let computed = compute_exfat_boot_checksum(&boot_region, bytes_per_sector);

    // Read stored checksum (first 4 bytes of sector 11)
    let cs_offset = 11 * bytes_per_sector as usize;
    if cs_offset + 4 <= boot_region.len() {
        let stored = u32::from_le_bytes([
            boot_region[cs_offset],
            boot_region[cs_offset + 1],
            boot_region[cs_offset + 2],
            boot_region[cs_offset + 3],
        ]);
        if computed != stored {
            log_cb(&format!(
                "exFAT validation: boot checksum mismatch (computed {:#010x} vs stored {:#010x})",
                computed, stored
            ));
            return Ok(false);
        }
    }

    log_cb("exFAT validation: VBR and boot checksum OK");
    Ok(true)
}

// =============================================================================
// Hidden Sectors Patching
// =============================================================================

/// Patch the partition offset field in the exFAT VBR.
///
/// exFAT stores the partition offset at VBR offset 0x40 as a u64.
pub fn patch_exfat_hidden_sectors(
    file: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    start_lba: u64,
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    // Read full boot region for checksum recalculation
    file.seek(SeekFrom::Start(partition_offset))?;
    let mut vbr = [0u8; 512];
    file.read_exact(&mut vbr)?;

    if &vbr[3..11] != b"EXFAT   " {
        return Ok(());
    }

    let bytes_per_sector_shift = vbr[0x6C];
    let bytes_per_sector = 1u64 << bytes_per_sector_shift;

    let old_partition_offset = u64::from_le_bytes([
        vbr[0x40], vbr[0x41], vbr[0x42], vbr[0x43], vbr[0x44], vbr[0x45], vbr[0x46], vbr[0x47],
    ]);

    if old_partition_offset == start_lba {
        return Ok(());
    }

    // Patch partition offset
    vbr[0x40..0x48].copy_from_slice(&start_lba.to_le_bytes());

    // Write patched VBR
    file.seek(SeekFrom::Start(partition_offset))?;
    file.write_all(&vbr)?;

    // Re-read full boot region with patched VBR
    file.seek(SeekFrom::Start(partition_offset))?;
    let boot_region_size = 12 * bytes_per_sector;
    let mut boot_region = vec![0u8; boot_region_size as usize];
    file.read_exact(&mut boot_region)?;
    // Ensure VBR in boot_region is patched (it should be since we wrote it)
    boot_region[..512].copy_from_slice(&vbr);

    // Recalculate and write checksum
    let checksum = compute_exfat_boot_checksum(&boot_region, bytes_per_sector);
    let checksum_data = build_checksum_sector(checksum, bytes_per_sector);

    // Main boot region checksum (sector 11)
    let cs_offset = partition_offset + 11 * bytes_per_sector;
    file.seek(SeekFrom::Start(cs_offset))?;
    file.write_all(&checksum_data)?;

    // Backup boot region (sectors 12-23): copy patched VBR and checksum
    let backup_vbr_offset = partition_offset + 12 * bytes_per_sector;
    file.seek(SeekFrom::Start(backup_vbr_offset))?;
    file.write_all(&vbr)?;

    let backup_cs_offset = partition_offset + 23 * bytes_per_sector;
    file.seek(SeekFrom::Start(backup_cs_offset))?;
    file.write_all(&checksum_data)?;

    log_cb(&format!(
        "exFAT: patched partition offset {} -> {}",
        old_partition_offset, start_lba
    ));

    Ok(())
}

/// Check if a boot sector contains exFAT magic.
pub fn is_exfat(boot_sector: &[u8]) -> bool {
    boot_sector.len() >= 11 && &boot_sector[3..11] == b"EXFAT   "
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_exfat_vbr() -> [u8; 512] {
        let mut vbr = [0u8; 512];
        // Jump boot code
        vbr[0] = 0xEB;
        vbr[1] = 0x76;
        vbr[2] = 0x90;
        // OEM ID
        vbr[3..11].copy_from_slice(b"EXFAT   ");
        // Must-be-zero region 0x0B..0x40 (already zero)
        // Partition offset = 2048 sectors
        vbr[0x40..0x48].copy_from_slice(&2048u64.to_le_bytes());
        // Volume length = 204800 sectors
        vbr[0x48..0x50].copy_from_slice(&204800u64.to_le_bytes());
        // FAT offset = 24 sectors
        vbr[0x50..0x54].copy_from_slice(&24u32.to_le_bytes());
        // FAT length = 128 sectors
        vbr[0x54..0x58].copy_from_slice(&128u32.to_le_bytes());
        // Cluster heap offset = 256 sectors
        vbr[0x58..0x5C].copy_from_slice(&256u32.to_le_bytes());
        // Cluster count = 25000
        vbr[0x5C..0x60].copy_from_slice(&25000u32.to_le_bytes());
        // Root cluster = 4
        vbr[0x60..0x64].copy_from_slice(&4u32.to_le_bytes());
        // Volume serial
        vbr[0x64..0x68].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());
        // Revision: 1.0
        vbr[0x68] = 0; // minor
        vbr[0x69] = 1; // major
                       // Bytes per sector shift = 9 (512 bytes)
        vbr[0x6C] = 9;
        // Sectors per cluster shift = 3 (8 sectors = 4096 bytes)
        vbr[0x6D] = 3;
        // Number of FATs = 1
        vbr[0x6E] = 1;
        // Boot signature
        vbr[510] = 0x55;
        vbr[511] = 0xAA;
        vbr
    }

    #[test]
    fn test_parse_exfat_vbr_valid() {
        let vbr = make_exfat_vbr();
        let parsed = parse_exfat_vbr(&vbr).unwrap();
        assert_eq!(parsed.volume_length, 204800);
        assert_eq!(parsed.fat_offset, 24);
        assert_eq!(parsed.fat_length, 128);
        assert_eq!(parsed.cluster_heap_offset, 256);
        assert_eq!(parsed.cluster_count, 25000);
        assert_eq!(parsed.root_cluster, 4);
        assert_eq!(parsed.bytes_per_sector_shift, 9);
        assert_eq!(parsed.sectors_per_cluster_shift, 3);
        assert_eq!(parsed.revision, (1, 0));
    }

    #[test]
    fn test_parse_exfat_vbr_invalid_magic() {
        let mut vbr = make_exfat_vbr();
        vbr[3..11].copy_from_slice(b"NOTEXFAT");
        assert!(parse_exfat_vbr(&vbr).is_err());
    }

    #[test]
    fn test_parse_exfat_vbr_nonzero_must_be_zero() {
        let mut vbr = make_exfat_vbr();
        vbr[0x0B] = 0xFF; // Must-be-zero violation
        assert!(parse_exfat_vbr(&vbr).is_err());
    }

    #[test]
    fn test_is_exfat() {
        let vbr = make_exfat_vbr();
        assert!(is_exfat(&vbr));
        assert!(!is_exfat(&[0u8; 512]));
        assert!(!is_exfat(&[0u8; 10]));
    }

    #[test]
    fn test_count_set_bits_basic() {
        assert_eq!(count_set_bits(&[0xFF], 8), 8);
        assert_eq!(count_set_bits(&[0xFF], 4), 4); // Only count first 4 bits
        assert_eq!(count_set_bits(&[0x00], 8), 0);
        assert_eq!(count_set_bits(&[0xAA], 8), 4); // 10101010
    }

    #[test]
    fn test_count_set_bits_partial_byte() {
        // 0xFF with max_clusters=5: should count 5 bits (lower 5 of 0xFF = all set)
        assert_eq!(count_set_bits(&[0xFF], 5), 5);
        // 0x1F = 00011111 with max_clusters=8: should count 5
        assert_eq!(count_set_bits(&[0x1F], 8), 5);
    }

    #[test]
    fn test_compute_exfat_boot_checksum() {
        // Basic smoke test: checksum of all-zeros (except must-skip bytes) should be deterministic
        let mut region = vec![0u8; 12 * 512];
        region[3..11].copy_from_slice(b"EXFAT   ");
        let cs = compute_exfat_boot_checksum(&region, 512);
        assert_ne!(cs, 0); // Should produce a non-zero checksum
    }

    #[test]
    fn test_build_checksum_sector() {
        let sector = build_checksum_sector(0xDEADBEEF, 512);
        assert_eq!(sector.len(), 512);
        for i in (0..512).step_by(4) {
            assert_eq!(
                u32::from_le_bytes([sector[i], sector[i + 1], sector[i + 2], sector[i + 3]]),
                0xDEADBEEF
            );
        }
    }

    // =========================================================================
    // Editing tests
    // =========================================================================

    use crate::fs::filesystem::{CreateDirectoryOptions, CreateFileOptions, EditableFilesystem};
    use std::io::Cursor;

    /// Create a minimal in-memory exFAT image suitable for editing tests.
    ///
    /// Layout (512 bytes/sector, 8 sectors/cluster = 4096 bytes/cluster):
    /// - Sectors 0-10: Main boot region (VBR + extended boot + OEM params)
    /// - Sector 11: Boot checksum
    /// - Sectors 12-23: Backup boot region
    /// - Sectors 24-151: FAT (128 sectors)
    /// - Sector 256+: Cluster heap
    ///   - Cluster 2: Allocation bitmap
    ///   - Cluster 3: (reserved for upcase table placeholder)
    ///   - Cluster 4: Root directory
    ///   - Clusters 5+: Free
    fn make_test_exfat_image() -> Vec<u8> {
        let bytes_per_sector: u64 = 512;
        let sectors_per_cluster: u64 = 8;
        let cluster_size = bytes_per_sector * sectors_per_cluster; // 4096
        let cluster_heap_offset: u64 = 256; // sectors
        let cluster_count: u32 = 100; // 100 clusters
        let volume_length: u64 = cluster_heap_offset + cluster_count as u64 * sectors_per_cluster;
        let total_bytes = volume_length * bytes_per_sector;
        let mut image = vec![0u8; total_bytes as usize];

        // -- VBR (sector 0) --
        let vbr = make_exfat_vbr_custom(volume_length, cluster_count);
        image[..512].copy_from_slice(&vbr);

        // -- FAT (starts at sector 24) --
        let fat_start = 24 * bytes_per_sector as usize;
        // Cluster 0,1: reserved (media type)
        image[fat_start..fat_start + 4].copy_from_slice(&0xFFFFFFF8u32.to_le_bytes());
        image[fat_start + 4..fat_start + 8].copy_from_slice(&0xFFFFFFFFu32.to_le_bytes());
        // Cluster 2 (bitmap): end of chain
        image[fat_start + 8..fat_start + 12].copy_from_slice(&0xFFFFFFFFu32.to_le_bytes());
        // Cluster 3 (upcase placeholder): end of chain
        image[fat_start + 12..fat_start + 16].copy_from_slice(&0xFFFFFFFFu32.to_le_bytes());
        // Cluster 4 (root dir): end of chain
        image[fat_start + 16..fat_start + 20].copy_from_slice(&0xFFFFFFFFu32.to_le_bytes());

        // -- Allocation bitmap (cluster 2) --
        let bitmap_offset = cluster_heap_offset as usize * 512 + 0 * cluster_size as usize;
        // Clusters 2,3,4 are used (bitmap indices 0,1,2)
        image[bitmap_offset] = 0x07; // bits 0,1,2 set

        // -- Root directory (cluster 4) --
        let root_offset = cluster_heap_offset as usize * 512 + 2 * cluster_size as usize;

        // Allocation Bitmap entry (0x81)
        image[root_offset] = ENTRY_TYPE_ALLOCATION_BITMAP;
        // BitmapFlags = 0 (first bitmap)
        // FirstCluster = 2
        image[root_offset + 20..root_offset + 24].copy_from_slice(&2u32.to_le_bytes());
        // DataLength = ceil(cluster_count/8)
        let bitmap_size = ((cluster_count + 7) / 8) as u64;
        image[root_offset + 24..root_offset + 32].copy_from_slice(&bitmap_size.to_le_bytes());

        // Volume Label entry (0x83) at offset 32
        let label_offset = root_offset + 32;
        image[label_offset] = ENTRY_TYPE_VOLUME_LABEL;
        image[label_offset + 1] = 4; // 4 chars
        let label = "TEST";
        for (i, c) in label.encode_utf16().enumerate() {
            image[label_offset + 2 + i * 2] = (c & 0xFF) as u8;
            image[label_offset + 2 + i * 2 + 1] = (c >> 8) as u8;
        }

        // Compute and write boot checksum for sectors 0-10
        let boot_region = &image[..12 * 512];
        let checksum = compute_exfat_boot_checksum(boot_region, bytes_per_sector);
        let cs_sector = build_checksum_sector(checksum, bytes_per_sector);
        let cs_offset = 11 * 512;
        image[cs_offset..cs_offset + 512].copy_from_slice(&cs_sector);

        // Copy main boot region to backup (sectors 12-23)
        let main_boot: Vec<u8> = image[..12 * 512].to_vec();
        image[12 * 512..24 * 512].copy_from_slice(&main_boot);

        image
    }

    fn make_exfat_vbr_custom(volume_length: u64, cluster_count: u32) -> [u8; 512] {
        let mut vbr = [0u8; 512];
        vbr[0] = 0xEB;
        vbr[1] = 0x76;
        vbr[2] = 0x90;
        vbr[3..11].copy_from_slice(b"EXFAT   ");
        // Partition offset
        vbr[0x40..0x48].copy_from_slice(&0u64.to_le_bytes());
        // Volume length
        vbr[0x48..0x50].copy_from_slice(&volume_length.to_le_bytes());
        // FAT offset = 24 sectors
        vbr[0x50..0x54].copy_from_slice(&24u32.to_le_bytes());
        // FAT length = 128 sectors
        vbr[0x54..0x58].copy_from_slice(&128u32.to_le_bytes());
        // Cluster heap offset = 256 sectors
        vbr[0x58..0x5C].copy_from_slice(&256u32.to_le_bytes());
        // Cluster count
        vbr[0x5C..0x60].copy_from_slice(&cluster_count.to_le_bytes());
        // Root cluster = 4
        vbr[0x60..0x64].copy_from_slice(&4u32.to_le_bytes());
        // Volume serial
        vbr[0x64..0x68].copy_from_slice(&0x12345678u32.to_le_bytes());
        // Revision 1.0
        vbr[0x68] = 0;
        vbr[0x69] = 1;
        // Bytes per sector shift = 9 (512)
        vbr[0x6C] = 9;
        // Sectors per cluster shift = 3 (8 sectors = 4096 bytes)
        vbr[0x6D] = 3;
        // Number of FATs = 1
        vbr[0x6E] = 1;
        // Boot signature
        vbr[510] = 0x55;
        vbr[511] = 0xAA;
        vbr
    }

    fn open_test_exfat(image: &mut Vec<u8>) -> ExfatFilesystem<Cursor<&mut Vec<u8>>> {
        let cursor = Cursor::new(image);
        ExfatFilesystem::open(cursor, 0).expect("failed to open test exFAT image")
    }

    #[test]
    fn test_exfat_name_hash() {
        // The hash should be deterministic
        let h1 = ExfatFilesystem::<Cursor<Vec<u8>>>::name_hash("test.txt");
        let h2 = ExfatFilesystem::<Cursor<Vec<u8>>>::name_hash("test.txt");
        assert_eq!(h1, h2);

        // Case-insensitive: "TEST.TXT" should hash the same (ASCII uppercase)
        let h3 = ExfatFilesystem::<Cursor<Vec<u8>>>::name_hash("TEST.TXT");
        assert_eq!(h1, h3);
    }

    #[test]
    fn test_exfat_entry_set_checksum() {
        // Build an entry set and verify the checksum is non-zero and consistent
        let entries =
            ExfatFilesystem::<Cursor<Vec<u8>>>::build_entry_set("hello.txt", 0x20, 5, 100);
        assert!(entries.len() >= 96); // At least 3 entries (file + stream + 1 name)
        let stored_cs = u16::from_le_bytes([entries[2], entries[3]]);
        assert_ne!(stored_cs, 0);

        // Verify: recompute with checksum field zeroed out
        let mut copy = entries.clone();
        copy[2] = 0;
        copy[3] = 0;
        let recomputed = ExfatFilesystem::<Cursor<Vec<u8>>>::entry_set_checksum(&copy);
        assert_eq!(recomputed, stored_cs);
    }

    #[test]
    fn test_exfat_create_file() {
        let mut image = make_test_exfat_image();
        let mut fs = open_test_exfat(&mut image);

        let root = fs.root().unwrap();
        let initial_free = fs.free_space().unwrap();

        let data = b"Hello exFAT world!";
        let mut cursor = std::io::Cursor::new(data.as_slice());
        let file = fs
            .create_file(
                &root,
                "hello.txt",
                &mut cursor,
                data.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();

        assert_eq!(file.name, "hello.txt");
        assert_eq!(file.size, data.len() as u64);

        // Should appear in directory listing
        let entries = fs.list_directory(&root).unwrap();
        assert!(entries.iter().any(|e| e.name == "hello.txt"));

        // Free space should decrease by at least one cluster
        let new_free = fs.free_space().unwrap();
        assert!(new_free < initial_free);
    }

    #[test]
    fn test_exfat_create_directory() {
        let mut image = make_test_exfat_image();
        let mut fs = open_test_exfat(&mut image);

        let root = fs.root().unwrap();
        let dir = fs
            .create_directory(&root, "subdir", &CreateDirectoryOptions::default())
            .unwrap();

        assert_eq!(dir.name, "subdir");
        assert!(dir.is_directory());

        let entries = fs.list_directory(&root).unwrap();
        assert!(entries
            .iter()
            .any(|e| e.name == "subdir" && e.is_directory()));
    }

    #[test]
    fn test_exfat_delete_file() {
        let mut image = make_test_exfat_image();
        let mut fs = open_test_exfat(&mut image);

        let root = fs.root().unwrap();
        let initial_free = fs.free_space().unwrap();

        // Create a file
        let data = b"delete me";
        let mut cursor = std::io::Cursor::new(data.as_slice());
        let file = fs
            .create_file(
                &root,
                "todelete.txt",
                &mut cursor,
                data.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();

        // Delete it
        let root = fs.root().unwrap();
        fs.delete_entry(&root, &file).unwrap();

        // Should not appear in listing
        let entries = fs.list_directory(&root).unwrap();
        assert!(!entries.iter().any(|e| e.name == "todelete.txt"));

        // Free space should be recovered
        let final_free = fs.free_space().unwrap();
        assert_eq!(final_free, initial_free);
    }

    #[test]
    fn test_exfat_duplicate_name() {
        let mut image = make_test_exfat_image();
        let mut fs = open_test_exfat(&mut image);

        let root = fs.root().unwrap();
        let data = b"first";
        let mut cursor = std::io::Cursor::new(data.as_slice());
        fs.create_file(
            &root,
            "dup.txt",
            &mut cursor,
            data.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();

        // Try to create again — should fail
        let mut cursor2 = std::io::Cursor::new(data.as_slice());
        let root = fs.root().unwrap();
        let result = fs.create_file(
            &root,
            "dup.txt",
            &mut cursor2,
            data.len() as u64,
            &CreateFileOptions::default(),
        );
        assert!(matches!(result, Err(FilesystemError::AlreadyExists(_))));
    }

    #[test]
    fn test_exfat_boot_checksum_updated() {
        let mut image = make_test_exfat_image();

        // Read initial checksum
        let initial_cs = u32::from_le_bytes([
            image[11 * 512],
            image[11 * 512 + 1],
            image[11 * 512 + 2],
            image[11 * 512 + 3],
        ]);

        {
            let mut fs = open_test_exfat(&mut image);
            let root = fs.root().unwrap();
            let data = b"trigger checksum update";
            let mut cursor = std::io::Cursor::new(data.as_slice());
            fs.create_file(
                &root,
                "checksumtest.txt",
                &mut cursor,
                data.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
        }

        // The boot checksum should still be valid (not necessarily changed since we
        // didn't modify the VBR itself, but sync_metadata recalculated it)
        let final_cs = u32::from_le_bytes([
            image[11 * 512],
            image[11 * 512 + 1],
            image[11 * 512 + 2],
            image[11 * 512 + 3],
        ]);

        // The checksum should be the same since VBR didn't change, but it was recalculated
        assert_eq!(initial_cs, final_cs);

        // Verify backup matches main
        let main_boot: Vec<u8> = image[..12 * 512].to_vec();
        let backup_boot = &image[12 * 512..24 * 512];
        assert_eq!(main_boot, backup_boot);
    }
}
