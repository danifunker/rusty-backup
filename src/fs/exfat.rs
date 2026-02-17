use std::io::{self, Read, Seek, SeekFrom, Write};

use anyhow::{bail, Result};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{Filesystem, FilesystemError};

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
    file.seek(SeekFrom::Start(bmp_offset))?;
    let mut bitmap_data = vec![0u8; bitmap_size as usize];
    file.read_exact(&mut bitmap_data)?;

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

    // Write resized bitmap
    file.seek(SeekFrom::Start(bmp_offset))?;
    file.write_all(&bitmap_data)?;

    // Update bitmap directory entry with new size
    let new_bitmap_size_u64 = new_bitmap_size as u64;
    root_buf[bitmap_entry_offset + 24..bitmap_entry_offset + 32]
        .copy_from_slice(&new_bitmap_size_u64.to_le_bytes());
    file.seek(SeekFrom::Start(root_offset))?;
    file.write_all(&root_buf)?;

    // Verify FAT has no entries beyond new cluster count (for shrinking)
    if new_cluster_count < old_cluster_count {
        let fat_offset = partition_offset
            + u32::from_le_bytes([vbr[0x50], vbr[0x51], vbr[0x52], vbr[0x53]]) as u64
                * bytes_per_sector;
        for cluster in new_cluster_count + 2..old_cluster_count + 2 {
            let entry_offset = fat_offset + cluster as u64 * 4;
            file.seek(SeekFrom::Start(entry_offset))?;
            let mut entry_buf = [0u8; 4];
            file.read_exact(&mut entry_buf)?;
            let entry = u32::from_le_bytes(entry_buf);
            if entry != 0 {
                // Clear the FAT entry
                file.seek(SeekFrom::Start(entry_offset))?;
                file.write_all(&0u32.to_le_bytes())?;
            }
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
}
