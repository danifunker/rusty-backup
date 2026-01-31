use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{self, Read, Seek, SeekFrom, Write};

use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};

const CHUNK_SIZE: usize = 256 * 1024; // 256 KB I/O buffer

/// FAT12/16/32 filesystem reader.
pub struct FatFilesystem<R> {
    reader: R,
    /// Bytes per sector (typically 512).
    bytes_per_sector: u64,
    /// Sectors per cluster.
    sectors_per_cluster: u64,
    /// Number of reserved sectors before the first FAT.
    reserved_sectors: u64,
    /// Number of FATs (typically 2).
    num_fats: u8,
    /// Total sectors on the volume.
    total_sectors: u64,
    /// Sectors per FAT.
    sectors_per_fat: u64,
    /// Root directory entry count (FAT12/16 only; 0 for FAT32).
    root_entry_count: u16,
    /// Root cluster (FAT32 only).
    root_cluster: u32,
    /// FAT type.
    fat_type: FatType,
    /// Byte offset of the partition within the reader.
    partition_offset: u64,
    /// Volume label from boot sector.
    label: Option<String>,
    /// Total clusters.
    total_clusters: u64,
    /// Cluster count used (lazily computed).
    #[allow(dead_code)]
    used_clusters: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum FatType {
    Fat12,
    Fat16,
    Fat32,
}

impl FatType {
    fn name(&self) -> &'static str {
        match self {
            FatType::Fat12 => "FAT12",
            FatType::Fat16 => "FAT16",
            FatType::Fat32 => "FAT32",
        }
    }
}

// FAT directory entry constants
const DIR_ENTRY_SIZE: usize = 32;
const ATTR_READ_ONLY: u8 = 0x01;
const ATTR_HIDDEN: u8 = 0x02;
const ATTR_SYSTEM: u8 = 0x04;
const ATTR_VOLUME_ID: u8 = 0x08;
const ATTR_DIRECTORY: u8 = 0x10;
#[allow(dead_code)]
const ATTR_ARCHIVE: u8 = 0x20;
const ATTR_LONG_NAME: u8 = ATTR_READ_ONLY | ATTR_HIDDEN | ATTR_SYSTEM | ATTR_VOLUME_ID;

impl<R: Read + Seek> FatFilesystem<R> {
    /// Open a FAT filesystem at the given offset within a reader.
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        reader.seek(SeekFrom::Start(partition_offset))?;

        let mut bpb = [0u8; 512];
        reader
            .read_exact(&mut bpb)
            .map_err(|e| FilesystemError::Parse(format!("cannot read boot sector: {e}")))?;

        // Validate jump instruction (EB xx 90 or E9 xx xx)
        if bpb[0] != 0xEB && bpb[0] != 0xE9 {
            return Err(FilesystemError::Parse(
                "invalid FAT boot sector: bad jump instruction".into(),
            ));
        }

        let bytes_per_sector = u16::from_le_bytes([bpb[11], bpb[12]]) as u64;
        if bytes_per_sector == 0 || bytes_per_sector > 4096 {
            return Err(FilesystemError::Parse(format!(
                "invalid bytes per sector: {bytes_per_sector}"
            )));
        }

        let sectors_per_cluster = bpb[13] as u64;
        if sectors_per_cluster == 0 {
            return Err(FilesystemError::Parse(
                "invalid sectors per cluster: 0".into(),
            ));
        }

        let reserved_sectors = u16::from_le_bytes([bpb[14], bpb[15]]) as u64;
        let num_fats = bpb[16];
        let root_entry_count = u16::from_le_bytes([bpb[17], bpb[18]]);

        let total_sectors_16 = u16::from_le_bytes([bpb[19], bpb[20]]) as u64;
        let total_sectors_32 = u32::from_le_bytes([bpb[32], bpb[33], bpb[34], bpb[35]]) as u64;
        let total_sectors = if total_sectors_16 != 0 {
            total_sectors_16
        } else {
            total_sectors_32
        };

        let sectors_per_fat_16 = u16::from_le_bytes([bpb[22], bpb[23]]) as u64;
        let sectors_per_fat_32 =
            u32::from_le_bytes([bpb[36], bpb[37], bpb[38], bpb[39]]) as u64;
        let sectors_per_fat = if sectors_per_fat_16 != 0 {
            sectors_per_fat_16
        } else {
            sectors_per_fat_32
        };

        // Determine data region start
        let root_dir_sectors =
            ((root_entry_count as u64 * 32) + (bytes_per_sector - 1)) / bytes_per_sector;
        let data_start_sector =
            reserved_sectors + (num_fats as u64 * sectors_per_fat) + root_dir_sectors;
        let data_sectors = total_sectors.saturating_sub(data_start_sector);
        let total_clusters = data_sectors / sectors_per_cluster;

        // Determine FAT type.
        //
        // The Microsoft spec uses cluster count thresholds, but compacted
        // images may have fewer clusters than the FAT32 minimum while still
        // using FAT32 on-disk structures.  When the 16-bit sectors-per-FAT
        // field is zero and the root entry count is zero the BPB is FAT32
        // format regardless of cluster count.
        let fat_type = if sectors_per_fat_16 == 0 && root_entry_count == 0 {
            FatType::Fat32
        } else if total_clusters < 4085 {
            FatType::Fat12
        } else if total_clusters < 65525 {
            FatType::Fat16
        } else {
            FatType::Fat32
        };

        // Root cluster for FAT32
        let root_cluster = if fat_type == FatType::Fat32 {
            u32::from_le_bytes([bpb[44], bpb[45], bpb[46], bpb[47]])
        } else {
            0
        };

        // Volume label
        let label_offset = if fat_type == FatType::Fat32 { 71 } else { 43 };
        let label_bytes = &bpb[label_offset..label_offset + 11];
        let label_str = std::str::from_utf8(label_bytes)
            .unwrap_or("")
            .trim()
            .to_string();
        let label = if label_str.is_empty() || label_str == "NO NAME" {
            None
        } else {
            Some(label_str)
        };

        Ok(Self {
            reader,
            bytes_per_sector,
            sectors_per_cluster,
            reserved_sectors,
            num_fats,
            total_sectors,
            sectors_per_fat,
            root_entry_count,
            root_cluster,
            fat_type,
            partition_offset,
            label,
            total_clusters,
            used_clusters: None,
        })
    }

    /// Absolute byte offset for a given sector number.
    fn sector_offset(&self, sector: u64) -> u64 {
        self.partition_offset + sector * self.bytes_per_sector
    }

    /// Absolute byte offset for the start of a cluster's data.
    fn cluster_offset(&self, cluster: u32) -> u64 {
        let root_dir_sectors = ((self.root_entry_count as u64 * 32)
            + (self.bytes_per_sector - 1))
            / self.bytes_per_sector;
        let data_start_sector = self.reserved_sectors
            + (self.num_fats as u64 * self.sectors_per_fat)
            + root_dir_sectors;
        let first_sector =
            data_start_sector + (cluster as u64 - 2) * self.sectors_per_cluster;
        self.sector_offset(first_sector)
    }

    /// Bytes per cluster.
    fn cluster_size(&self) -> u64 {
        self.bytes_per_sector * self.sectors_per_cluster
    }

    /// Start of the data region, in bytes from partition start.
    fn data_region_offset(&self) -> u64 {
        let root_dir_sectors = ((self.root_entry_count as u64 * 32)
            + (self.bytes_per_sector - 1))
            / self.bytes_per_sector;
        let data_start_sector = self.reserved_sectors
            + (self.num_fats as u64 * self.sectors_per_fat)
            + root_dir_sectors;
        data_start_sector * self.bytes_per_sector
    }

    /// Scan the FAT table backwards to find the highest cluster number in use.
    /// Returns `None` if no data clusters are allocated.
    fn highest_used_cluster(&mut self) -> Result<Option<u32>, FilesystemError> {
        let fat_offset = self.sector_offset(self.reserved_sectors);
        let cluster_count = self.total_clusters as u32;

        if cluster_count == 0 {
            return Ok(None);
        }

        // Total entries in the FAT (clusters 0..cluster_count+1)
        let total_entries = cluster_count + 2;

        match self.fat_type {
            FatType::Fat12 => {
                // FAT12 is always small (max ~6 KB) — read entire FAT
                let fat_size = ((total_entries as usize) * 3 + 1) / 2;
                self.reader.seek(SeekFrom::Start(fat_offset))?;
                let mut fat_data = vec![0u8; fat_size];
                let actually_read = self.reader.read(&mut fat_data)?;
                fat_data.truncate(actually_read);

                for cluster in (2..total_entries).rev() {
                    let byte_off = (cluster as usize * 3) / 2;
                    if byte_off + 1 >= fat_data.len() {
                        continue;
                    }
                    let val = u16::from_le_bytes([fat_data[byte_off], fat_data[byte_off + 1]]);
                    let entry = if cluster & 1 == 1 { val >> 4 } else { val & 0x0FFF };
                    if entry != 0 {
                        return Ok(Some(cluster));
                    }
                }
                Ok(None)
            }
            FatType::Fat16 => {
                // FAT16 is at most ~128 KB — read entire FAT
                let fat_size = total_entries as usize * 2;
                self.reader.seek(SeekFrom::Start(fat_offset))?;
                let mut fat_data = vec![0u8; fat_size];
                let actually_read = self.reader.read(&mut fat_data)?;
                fat_data.truncate(actually_read);

                for cluster in (2..total_entries).rev() {
                    let off = cluster as usize * 2;
                    if off + 1 >= fat_data.len() {
                        continue;
                    }
                    let val = u16::from_le_bytes([fat_data[off], fat_data[off + 1]]);
                    if val != 0 {
                        return Ok(Some(cluster));
                    }
                }
                Ok(None)
            }
            FatType::Fat32 => {
                // FAT32 can be large — scan backwards in 64 KB chunks
                let entries_per_chunk: u32 = 16384; // 64 KB per chunk
                let mut end = total_entries; // exclusive upper bound

                while end > 2 {
                    let start = end.saturating_sub(entries_per_chunk).max(2);
                    let chunk_entries = (end - start) as usize;
                    let byte_start = start as u64 * 4;

                    self.reader.seek(SeekFrom::Start(fat_offset + byte_start))?;
                    let mut buf = vec![0u8; chunk_entries * 4];
                    self.reader.read_exact(&mut buf)?;

                    for i in (0..chunk_entries).rev() {
                        let off = i * 4;
                        let val = u32::from_le_bytes([
                            buf[off],
                            buf[off + 1],
                            buf[off + 2],
                            buf[off + 3],
                        ]) & 0x0FFF_FFFF;
                        if val != 0 {
                            return Ok(Some(start + i as u32));
                        }
                    }

                    end = start;
                }
                Ok(None)
            }
        }
    }

    /// Read the next cluster number from the FAT.
    fn next_cluster(&mut self, cluster: u32) -> Result<Option<u32>, FilesystemError> {
        let fat_offset = self.sector_offset(self.reserved_sectors);
        match self.fat_type {
            FatType::Fat12 => {
                let entry_offset = cluster as u64 + (cluster as u64 / 2);
                self.reader
                    .seek(SeekFrom::Start(fat_offset + entry_offset))?;
                let val = self.reader.read_u16::<LittleEndian>()?;
                let next = if cluster & 1 == 1 {
                    val >> 4
                } else {
                    val & 0x0FFF
                };
                if next >= 0x0FF8 {
                    Ok(None) // end of chain
                } else if next == 0 || next >= 0x0FF0 {
                    Ok(None) // bad/reserved
                } else {
                    Ok(Some(next as u32))
                }
            }
            FatType::Fat16 => {
                let entry_offset = cluster as u64 * 2;
                self.reader
                    .seek(SeekFrom::Start(fat_offset + entry_offset))?;
                let next = self.reader.read_u16::<LittleEndian>()?;
                if next >= 0xFFF8 {
                    Ok(None)
                } else if next == 0 || next >= 0xFFF0 {
                    Ok(None)
                } else {
                    Ok(Some(next as u32))
                }
            }
            FatType::Fat32 => {
                let entry_offset = cluster as u64 * 4;
                self.reader
                    .seek(SeekFrom::Start(fat_offset + entry_offset))?;
                let next = self.reader.read_u32::<LittleEndian>()? & 0x0FFF_FFFF;
                if next >= 0x0FFF_FFF8 {
                    Ok(None)
                } else if next < 2 || next >= 0x0FFF_FFF0 {
                    Ok(None)
                } else {
                    Ok(Some(next))
                }
            }
        }
    }

    /// Follow the cluster chain and read all cluster data.
    fn read_cluster_chain(&mut self, start_cluster: u32) -> Result<Vec<u8>, FilesystemError> {
        let cluster_size = self.cluster_size() as usize;
        let mut data = Vec::new();
        let mut cluster = start_cluster;
        let mut count = 0u32;

        loop {
            if cluster < 2 || count > self.total_clusters as u32 {
                break;
            }

            let offset = self.cluster_offset(cluster);
            self.reader.seek(SeekFrom::Start(offset))?;
            let mut buf = vec![0u8; cluster_size];
            self.reader.read_exact(&mut buf)?;
            data.extend_from_slice(&buf);
            count += 1;

            match self.next_cluster(cluster)? {
                Some(next) => cluster = next,
                None => break,
            }
        }

        Ok(data)
    }

    /// Read the root directory data.
    fn read_root_directory(&mut self) -> Result<Vec<u8>, FilesystemError> {
        match self.fat_type {
            FatType::Fat12 | FatType::Fat16 => {
                // Fixed root directory region after the FATs
                let root_start = self.reserved_sectors
                    + (self.num_fats as u64 * self.sectors_per_fat);
                let root_size = self.root_entry_count as u64 * DIR_ENTRY_SIZE as u64;
                let offset = self.sector_offset(root_start);
                self.reader.seek(SeekFrom::Start(offset))?;
                let mut buf = vec![0u8; root_size as usize];
                self.reader.read_exact(&mut buf)?;
                Ok(buf)
            }
            FatType::Fat32 => {
                // Root directory is a cluster chain starting at root_cluster
                self.read_cluster_chain(self.root_cluster)
            }
        }
    }

    /// Parse directory entries from raw data bytes.
    fn parse_directory(
        &self,
        data: &[u8],
        parent_path: &str,
    ) -> Vec<FileEntry> {
        let mut entries = Vec::new();
        let mut lfn_parts: Vec<(u8, String)> = Vec::new();
        let num_entries = data.len() / DIR_ENTRY_SIZE;

        for i in 0..num_entries {
            let off = i * DIR_ENTRY_SIZE;
            let entry_bytes = &data[off..off + DIR_ENTRY_SIZE];

            // End of directory marker
            if entry_bytes[0] == 0x00 {
                break;
            }

            // Deleted entry
            if entry_bytes[0] == 0xE5 {
                lfn_parts.clear();
                continue;
            }

            let attr = entry_bytes[11];

            // Long filename entry
            if attr == ATTR_LONG_NAME {
                let seq = entry_bytes[0] & 0x3F;
                // LFN entry layout (13 UTF-16LE characters per entry):
                //   Bytes 1-10:  characters 1-5
                //   Bytes 14-25: characters 6-11
                //   Bytes 26-27: first cluster low (always 0, NOT character data)
                //   Bytes 28-31: characters 12-13
                let chars: Vec<u16> = vec![
                    u16::from_le_bytes([entry_bytes[1], entry_bytes[2]]),
                    u16::from_le_bytes([entry_bytes[3], entry_bytes[4]]),
                    u16::from_le_bytes([entry_bytes[5], entry_bytes[6]]),
                    u16::from_le_bytes([entry_bytes[7], entry_bytes[8]]),
                    u16::from_le_bytes([entry_bytes[9], entry_bytes[10]]),
                    u16::from_le_bytes([entry_bytes[14], entry_bytes[15]]),
                    u16::from_le_bytes([entry_bytes[16], entry_bytes[17]]),
                    u16::from_le_bytes([entry_bytes[18], entry_bytes[19]]),
                    u16::from_le_bytes([entry_bytes[20], entry_bytes[21]]),
                    u16::from_le_bytes([entry_bytes[22], entry_bytes[23]]),
                    u16::from_le_bytes([entry_bytes[24], entry_bytes[25]]),
                    u16::from_le_bytes([entry_bytes[28], entry_bytes[29]]),
                    u16::from_le_bytes([entry_bytes[30], entry_bytes[31]]),
                ];

                // Filter out padding (0x0000 and 0xFFFF)
                let part: String = chars
                    .into_iter()
                    .take_while(|&c| c != 0x0000 && c != 0xFFFF)
                    .flat_map(|c| std::char::from_u32(c as u32))
                    .collect();

                if entry_bytes[0] & 0x40 != 0 {
                    // First (last physical) LFN entry
                    lfn_parts.clear();
                }
                lfn_parts.push((seq, part));
                continue;
            }

            // Volume ID entry
            if attr & ATTR_VOLUME_ID != 0 {
                lfn_parts.clear();
                continue;
            }

            // Regular 8.3 entry
            let is_dir = attr & ATTR_DIRECTORY != 0;

            // Build short name
            let name_bytes = &entry_bytes[0..8];
            let ext_bytes = &entry_bytes[8..11];
            let short_name = build_short_name(name_bytes, ext_bytes);

            // Skip . and .. entries
            if short_name == "." || short_name == ".." {
                lfn_parts.clear();
                continue;
            }

            // Build long name from accumulated LFN parts
            let long_name = if !lfn_parts.is_empty() {
                lfn_parts.sort_by_key(|&(seq, _)| seq);
                let name: String = lfn_parts.iter().map(|(_, s)| s.as_str()).collect();
                lfn_parts.clear();
                name
            } else {
                lfn_parts.clear();
                String::new()
            };

            let display_name = if !long_name.is_empty() {
                long_name
            } else {
                short_name
            };

            // Cluster number
            let cluster_hi = u16::from_le_bytes([entry_bytes[20], entry_bytes[21]]) as u32;
            let cluster_lo = u16::from_le_bytes([entry_bytes[26], entry_bytes[27]]) as u32;
            let cluster = (cluster_hi << 16) | cluster_lo;

            let size = u32::from_le_bytes([
                entry_bytes[28],
                entry_bytes[29],
                entry_bytes[30],
                entry_bytes[31],
            ]) as u64;

            // Date/time
            let date_val = u16::from_le_bytes([entry_bytes[24], entry_bytes[25]]);
            let time_val = u16::from_le_bytes([entry_bytes[22], entry_bytes[23]]);
            let modified = format_fat_datetime(date_val, time_val);

            let path = if parent_path == "/" {
                format!("/{display_name}")
            } else {
                format!("{parent_path}/{display_name}")
            };

            let mut entry = if is_dir {
                FileEntry::new_directory(display_name, path, cluster as u64)
            } else {
                FileEntry::new_file(display_name, path, size, cluster as u64)
            };
            entry.modified = Some(modified);
            entries.push(entry);
        }

        // Sort: directories first, then alphabetically
        entries.sort_by(|a, b| {
            let dir_ord = b.is_directory().cmp(&a.is_directory());
            if dir_ord != std::cmp::Ordering::Equal {
                dir_ord
            } else {
                a.name.to_lowercase().cmp(&b.name.to_lowercase())
            }
        });

        entries
    }
}

impl<R: Read + Seek + Send> Filesystem for FatFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        let mut root = FileEntry::root();
        root.location = if self.fat_type == FatType::Fat32 {
            self.root_cluster as u64
        } else {
            0
        };
        Ok(root)
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if !entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }

        let data = if entry.path == "/" {
            self.read_root_directory()?
        } else {
            if entry.location < 2 {
                return Err(FilesystemError::InvalidData(format!(
                    "invalid cluster for directory: {}",
                    entry.location
                )));
            }
            self.read_cluster_chain(entry.location as u32)?
        };

        Ok(self.parse_directory(&data, &entry.path))
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        if entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }

        if entry.location < 2 {
            // Zero-length file or invalid cluster
            return Ok(Vec::new());
        }

        let data = self.read_cluster_chain(entry.location as u32)?;
        let actual_size = (entry.size as usize).min(data.len()).min(max_bytes);
        Ok(data[..actual_size].to_vec())
    }

    fn volume_label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    fn fs_type(&self) -> &str {
        self.fat_type.name()
    }

    fn total_size(&self) -> u64 {
        self.total_sectors * self.bytes_per_sector
    }

    fn used_size(&self) -> u64 {
        // Rough estimate: total - (free clusters * cluster size)
        // For accuracy we'd need to scan the FAT, but this is a reasonable approximation
        self.total_size() / 2 // placeholder
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        match self.highest_used_cluster()? {
            Some(cluster) => {
                // Include all bytes from partition start through the end of
                // the highest allocated cluster.
                let cluster_end_abs = self.cluster_offset(cluster) + self.cluster_size();
                let relative = cluster_end_abs - self.partition_offset;
                // Round up to sector boundary
                let aligned =
                    ((relative + self.bytes_per_sector - 1) / self.bytes_per_sector)
                        * self.bytes_per_sector;
                Ok(aligned)
            }
            None => {
                // No data clusters allocated — still need boot sector, FATs,
                // and root directory area.
                Ok(self.data_region_offset())
            }
        }
    }
}

/// Build a DOS 8.3 short filename from the name and extension bytes.
///
/// FAT short names use OEM codepage encoding (typically CP437). Bytes 0x80-0xFF
/// are decoded using the CP437 table rather than assuming UTF-8.
fn build_short_name(name: &[u8], ext: &[u8]) -> String {
    let name_str = decode_oem_string(name);
    let name_trimmed = name_str.trim_end();
    let ext_str = decode_oem_string(ext);
    let ext_trimmed = ext_str.trim_end();

    if ext_trimmed.is_empty() {
        name_trimmed.to_string()
    } else {
        format!("{name_trimmed}.{ext_trimmed}")
    }
}

/// Decode a byte slice from OEM codepage (CP437) to a UTF-8 String.
///
/// Bytes 0x00-0x7F are ASCII. Bytes 0x80-0xFF are mapped using the standard
/// CP437 table used by DOS and FAT short filenames.
fn decode_oem_string(bytes: &[u8]) -> String {
    bytes.iter().map(|&b| cp437_to_char(b)).collect()
}

/// Map a single CP437 byte to a Unicode character.
fn cp437_to_char(b: u8) -> char {
    if b < 0x80 {
        b as char
    } else {
        CP437_HIGH[b as usize - 0x80]
    }
}

/// CP437 to Unicode mapping for bytes 0x80-0xFF.
#[rustfmt::skip]
const CP437_HIGH: [char; 128] = [
    // 0x80-0x8F
    'Ç','ü','é','â','ä','à','å','ç', 'ê','ë','è','ï','î','ì','Ä','Å',
    // 0x90-0x9F
    'É','æ','Æ','ô','ö','ò','û','ù', 'ÿ','Ö','Ü','¢','£','¥','₧','ƒ',
    // 0xA0-0xAF
    'á','í','ó','ú','ñ','Ñ','ª','º', '¿','⌐','¬','½','¼','¡','«','»',
    // 0xB0-0xBF
    '░','▒','▓','│','┤','╡','╢','╖', '╕','╣','║','╗','╝','╜','╛','┐',
    // 0xC0-0xCF
    '└','┴','┬','├','─','┼','╞','╟', '╚','╔','╩','╦','╠','═','╬','╧',
    // 0xD0-0xDF
    '╨','╤','╥','╙','╘','╒','╓','╫', '╪','┘','┌','█','▄','▌','▐','▀',
    // 0xE0-0xEF
    'α','ß','Γ','π','Σ','σ','µ','τ', 'Φ','Θ','Ω','δ','∞','φ','ε','∩',
    // 0xF0-0xFF
    '≡','±','≥','≤','⌠','⌡','÷','≈', '°','∙','·','√','ⁿ','²','■','\u{00A0}',
];

/// Format a FAT date/time pair as "YYYY-MM-DD HH:MM:SS".
fn format_fat_datetime(date: u16, time: u16) -> String {
    if date == 0 {
        return String::new();
    }
    let day = date & 0x1F;
    let month = (date >> 5) & 0x0F;
    let year = ((date >> 9) & 0x7F) + 1980;
    let second = (time & 0x1F) * 2;
    let minute = (time >> 5) & 0x3F;
    let hour = (time >> 11) & 0x1F;
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}")
}

// ---------------------------------------------------------------------------
// CompactFatReader — streaming compacted FAT image
// ---------------------------------------------------------------------------

/// Result of filesystem compaction analysis.
pub struct CompactInfo {
    pub original_size: u64,
    pub compacted_size: u64,
    pub clusters_used: u32,
}

/// A streaming `Read` that produces a compacted FAT partition image.
///
/// Allocated clusters are packed contiguously from cluster 2 onwards, producing
/// a smaller image where all data is defragmented. Directory entries have their
/// cluster references remapped to match the new layout.
///
/// The virtual image layout is:
///   [ Reserved sectors (boot sector + extras) ]
///   [ FAT table #1 (rebuilt) ]
///   [ FAT table #2 (copy of #1) ]
///   [ Root directory (FAT12/16 only, cluster refs patched) ]
///   [ Data clusters 2, 3, 4, ... packed contiguously ]
pub struct CompactFatReader<R> {
    source: R,
    #[allow(dead_code)]
    partition_offset: u64,

    // Pre-built sections
    boot_sector: Vec<u8>,
    fat_tables: Vec<u8>,
    root_dir: Vec<u8>,

    // Cluster mapping
    new_to_old: Vec<u32>,
    old_to_new: HashMap<u32, u32>,
    directory_clusters: HashSet<u32>,

    // Source geometry (original, for seeking to source clusters)
    src_data_start_abs: u64,
    bytes_per_sector: u64,
    sectors_per_cluster: u64,

    // Virtual image geometry
    cluster_size: usize,
    fat_offset: u64,
    root_dir_offset: u64,
    data_offset: u64,
    total_size: u64,

    // Streaming state
    position: u64,
    cluster_buf: Vec<u8>,
    cluster_buf_idx: Option<usize>,
}

impl<R: Read + Seek> CompactFatReader<R> {
    /// Create a new compacted FAT reader.
    ///
    /// The constructor reads the BPB and entire FAT table, scans for allocated
    /// clusters, builds the remapping, and pre-computes boot sector, FAT tables,
    /// and root directory bytes.
    pub fn new(mut source: R, partition_offset: u64) -> Result<(Self, CompactInfo), FilesystemError> {
        // --- Parse BPB ---
        source.seek(SeekFrom::Start(partition_offset))?;
        let mut bpb = [0u8; 512];
        source.read_exact(&mut bpb)
            .map_err(|e| FilesystemError::Parse(format!("cannot read boot sector: {e}")))?;

        if bpb[0] != 0xEB && bpb[0] != 0xE9 {
            return Err(FilesystemError::Parse("invalid FAT boot sector".into()));
        }

        let bytes_per_sector = u16::from_le_bytes([bpb[11], bpb[12]]) as u64;
        if bytes_per_sector == 0 || bytes_per_sector > 4096 {
            return Err(FilesystemError::Parse(format!(
                "invalid bytes per sector: {bytes_per_sector}"
            )));
        }

        let sectors_per_cluster = bpb[13] as u64;
        if sectors_per_cluster == 0 {
            return Err(FilesystemError::Parse("invalid sectors per cluster: 0".into()));
        }

        let reserved_sectors = u16::from_le_bytes([bpb[14], bpb[15]]) as u64;
        let num_fats = bpb[16] as u64;
        let root_entry_count = u16::from_le_bytes([bpb[17], bpb[18]]);

        let total_sectors_16 = u16::from_le_bytes([bpb[19], bpb[20]]) as u64;
        let total_sectors_32 = u32::from_le_bytes([bpb[32], bpb[33], bpb[34], bpb[35]]) as u64;
        let original_total_sectors = if total_sectors_16 != 0 {
            total_sectors_16
        } else {
            total_sectors_32
        };

        let sectors_per_fat_16 = u16::from_le_bytes([bpb[22], bpb[23]]) as u64;
        let sectors_per_fat_32 = u32::from_le_bytes([bpb[36], bpb[37], bpb[38], bpb[39]]) as u64;
        let original_sectors_per_fat = if sectors_per_fat_16 != 0 {
            sectors_per_fat_16
        } else {
            sectors_per_fat_32
        };

        let root_dir_sectors =
            ((root_entry_count as u64 * 32) + (bytes_per_sector - 1)) / bytes_per_sector;
        let original_data_start_sector =
            reserved_sectors + (num_fats * original_sectors_per_fat) + root_dir_sectors;
        let data_sectors = original_total_sectors.saturating_sub(original_data_start_sector);
        let total_clusters = data_sectors / sectors_per_cluster;

        let fat_type = if sectors_per_fat_16 == 0 && root_entry_count == 0 {
            FatType::Fat32
        } else if total_clusters < 4085 {
            FatType::Fat12
        } else if total_clusters < 65525 {
            FatType::Fat16
        } else {
            FatType::Fat32
        };

        let original_root_cluster = if fat_type == FatType::Fat32 {
            u32::from_le_bytes([bpb[44], bpb[45], bpb[46], bpb[47]])
        } else {
            0
        };

        let cluster_size = (bytes_per_sector * sectors_per_cluster) as usize;
        let original_size = original_total_sectors * bytes_per_sector;

        // Absolute byte offset of the data region in the source
        let src_data_start_abs = partition_offset + original_data_start_sector * bytes_per_sector;

        // --- Read entire FAT table ---
        let fat_byte_offset = partition_offset + reserved_sectors * bytes_per_sector;
        source.seek(SeekFrom::Start(fat_byte_offset))?;
        let fat_byte_size = (original_sectors_per_fat * bytes_per_sector) as usize;
        let mut fat_data = vec![0u8; fat_byte_size];
        source.read_exact(&mut fat_data)?;

        // --- Identify allocated clusters ---
        let total_entries = (total_clusters as u32) + 2;
        let mut allocated: Vec<u32> = Vec::new();

        for cluster in 2..total_entries {
            let entry = read_fat_entry(&fat_data, cluster, fat_type);
            if entry != 0 {
                allocated.push(cluster);
            }
        }

        let clusters_used = allocated.len() as u32;

        // --- Build cluster mapping ---
        let mut old_to_new: HashMap<u32, u32> = HashMap::with_capacity(allocated.len());
        let mut new_to_old: Vec<u32> = Vec::with_capacity(allocated.len());

        for (i, &old_cluster) in allocated.iter().enumerate() {
            let new_cluster = (i as u32) + 2;
            old_to_new.insert(old_cluster, new_cluster);
            new_to_old.push(old_cluster);
        }

        // --- Walk directory tree to identify directory clusters ---
        let mut directory_clusters: HashSet<u32> = HashSet::new();

        if fat_type == FatType::Fat32 {
            walk_directory_tree_fat32(
                &mut source,
                partition_offset,
                &fat_data,
                fat_type,
                original_root_cluster,
                bytes_per_sector,
                sectors_per_cluster,
                original_data_start_sector,
                cluster_size,
                total_entries,
                &mut directory_clusters,
            )?;
        } else {
            // FAT12/16: root directory is fixed, walk subdirectories from it
            let root_start_sector = reserved_sectors + (num_fats * original_sectors_per_fat);
            let root_size = root_entry_count as u64 * DIR_ENTRY_SIZE as u64;
            let root_abs_offset = partition_offset + root_start_sector * bytes_per_sector;
            source.seek(SeekFrom::Start(root_abs_offset))?;
            let mut root_data = vec![0u8; root_size as usize];
            source.read_exact(&mut root_data)?;

            let mut queue: VecDeque<u32> = VecDeque::new();
            let mut visited_dirs: HashSet<u32> = HashSet::new();

            find_subdirectories_in_data(
                &root_data,
                &mut queue,
                &mut visited_dirs,
                &mut directory_clusters,
                &fat_data,
                fat_type,
                total_entries,
            );

            while let Some(dir_start) = queue.pop_front() {
                let dir_data = read_chain_from_source(
                    &mut source,
                    partition_offset,
                    &fat_data,
                    fat_type,
                    dir_start,
                    bytes_per_sector,
                    sectors_per_cluster,
                    original_data_start_sector,
                    cluster_size,
                    total_entries,
                )?;

                find_subdirectories_in_data(
                    &dir_data,
                    &mut queue,
                    &mut visited_dirs,
                    &mut directory_clusters,
                    &fat_data,
                    fat_type,
                    total_entries,
                );
            }
        }

        // --- Calculate new volume geometry ---
        // Use exactly the number of allocated clusters — no padding.
        // The FAT type is preserved in the BPB and FAT entry width regardless
        // of cluster count. Padding to the FAT type minimum would waste
        // significant space (e.g. 65525 * 32KB = 2GB for FAT32).
        let new_cluster_count = clusters_used as u64;

        let new_sectors_per_fat = if fat_type == FatType::Fat12 {
            let fat_bytes = ((new_cluster_count + 2) * 3 + 1) / 2;
            (fat_bytes + bytes_per_sector - 1) / bytes_per_sector
        } else {
            let entry_bytes: u64 = match fat_type {
                FatType::Fat16 => 2,
                FatType::Fat32 => 4,
                _ => unreachable!(),
            };
            let fat_bytes = (new_cluster_count + 2) * entry_bytes;
            (fat_bytes + bytes_per_sector - 1) / bytes_per_sector
        };

        let new_data_sectors = new_cluster_count * sectors_per_cluster;
        let new_total_sectors =
            reserved_sectors + (num_fats * new_sectors_per_fat) + root_dir_sectors + new_data_sectors;

        // --- Build boot sector bytes ---
        source.seek(SeekFrom::Start(partition_offset))?;
        let reserved_bytes = (reserved_sectors * bytes_per_sector) as usize;
        let mut boot_sector = vec![0u8; reserved_bytes];
        source.read_exact(&mut boot_sector)?;

        // Patch total sectors
        if new_total_sectors <= 0xFFFF && total_sectors_16 != 0 {
            let ts16 = (new_total_sectors as u16).to_le_bytes();
            boot_sector[19] = ts16[0];
            boot_sector[20] = ts16[1];
            boot_sector[32] = 0;
            boot_sector[33] = 0;
            boot_sector[34] = 0;
            boot_sector[35] = 0;
        } else {
            boot_sector[19] = 0;
            boot_sector[20] = 0;
            let ts32 = (new_total_sectors as u32).to_le_bytes();
            boot_sector[32] = ts32[0];
            boot_sector[33] = ts32[1];
            boot_sector[34] = ts32[2];
            boot_sector[35] = ts32[3];
        }

        // Patch sectors per FAT
        if fat_type == FatType::Fat32 {
            boot_sector[22] = 0;
            boot_sector[23] = 0;
            let spf32 = (new_sectors_per_fat as u32).to_le_bytes();
            boot_sector[36] = spf32[0];
            boot_sector[37] = spf32[1];
            boot_sector[38] = spf32[2];
            boot_sector[39] = spf32[3];

            // Patch root cluster
            let new_root = old_to_new
                .get(&original_root_cluster)
                .copied()
                .unwrap_or(original_root_cluster);
            let rc = new_root.to_le_bytes();
            boot_sector[44] = rc[0];
            boot_sector[45] = rc[1];
            boot_sector[46] = rc[2];
            boot_sector[47] = rc[3];
        } else {
            let spf16 = (new_sectors_per_fat as u16).to_le_bytes();
            boot_sector[22] = spf16[0];
            boot_sector[23] = spf16[1];
        }

        // --- Patch FAT32 FSInfo in boot sector ---
        // After compaction, the FSInfo sector must have the correct free cluster
        // count (0 — all clusters in the compacted image are allocated) and
        // next_free hint.
        if fat_type == FatType::Fat32 {
            let fsinfo_sector = u16::from_le_bytes([bpb[48], bpb[49]]);
            if fsinfo_sector > 0
                && (fsinfo_sector as u64) < reserved_sectors
                && ((fsinfo_sector as u64 + 1) * bytes_per_sector) <= boot_sector.len() as u64
            {
                let fs_off = fsinfo_sector as usize * bytes_per_sector as usize;
                let sig1 = u32::from_le_bytes(
                    boot_sector[fs_off..fs_off + 4].try_into().unwrap_or([0; 4]),
                );
                let sig2 = u32::from_le_bytes(
                    boot_sector[fs_off + 484..fs_off + 488].try_into().unwrap_or([0; 4]),
                );
                if sig1 == 0x41615252 && sig2 == 0x61417272 {
                    // Free count = 0 (all clusters in compacted image are used)
                    boot_sector[fs_off + 488..fs_off + 492]
                        .copy_from_slice(&0u32.to_le_bytes());
                    // Next free = first cluster past used data
                    let next_free = (clusters_used as u32) + 2;
                    boot_sector[fs_off + 492..fs_off + 496]
                        .copy_from_slice(&next_free.to_le_bytes());
                }
            }
        }

        // --- Build FAT table bytes ---
        let new_fat_byte_size = (new_sectors_per_fat * bytes_per_sector) as usize;
        let mut single_fat = vec![0u8; new_fat_byte_size];

        // Set FAT[0] and FAT[1] with proper values.
        // FAT[0] must contain the media byte (0xF8 for hard disk) with high bits set.
        // FAT[1] must contain the end-of-chain marker with clean shutdown flags.
        // (Cross-reference: partclone's fatclone.c check_fat_status())
        let media_byte = bpb[21]; // BPB media byte (typically 0xF8 for hard disks)
        match fat_type {
            FatType::Fat12 => {
                // FAT12: entries are 12 bits. Entry 0 = media | 0xF00, entry 1 = 0xFFF (EOC)
                // FAT12 has no dirty flags in FAT[1]
                let entry0 = 0x0F00u16 | media_byte as u16;
                let entry1 = 0x0FFFu16;
                // Pack two 12-bit entries into 3 bytes:
                // entry0 occupies bits [0..11], entry1 occupies bits [12..23]
                let packed: u32 = (entry0 as u32) | ((entry1 as u32) << 12);
                if single_fat.len() >= 3 {
                    single_fat[0] = packed as u8;
                    single_fat[1] = (packed >> 8) as u8;
                    single_fat[2] = (packed >> 16) as u8;
                }
            }
            FatType::Fat16 => {
                // FAT16: entry 0 = media | 0xFF00, entry 1 = 0xFFFF (EOC + clean flags)
                // Bits 15 = clean shutdown, bit 14 = no I/O errors (both set = 0xC000)
                let entry0 = 0xFF00u16 | media_byte as u16;
                let entry1 = 0xFFFFu16; // EOC marker with all flag bits set
                if single_fat.len() >= 4 {
                    single_fat[0..2].copy_from_slice(&entry0.to_le_bytes());
                    single_fat[2..4].copy_from_slice(&entry1.to_le_bytes());
                }
            }
            FatType::Fat32 => {
                // FAT32: entry 0 = media | 0x0FFFFF00, entry 1 = 0x0FFFFFFF (EOC + clean flags)
                // Bits 27 = clean shutdown, bit 26 = no I/O errors (both set = 0x0C000000)
                let entry0 = 0x0FFF_FF00u32 | media_byte as u32;
                let entry1 = 0x0FFF_FFFFu32; // EOC marker with all flag bits set
                if single_fat.len() >= 8 {
                    single_fat[0..4].copy_from_slice(&entry0.to_le_bytes());
                    single_fat[4..8].copy_from_slice(&entry1.to_le_bytes());
                }
            }
        }

        // Remap entries for allocated clusters
        for new_idx in 0..new_to_old.len() {
            let new_cluster = (new_idx as u32) + 2;
            let old_cluster = new_to_old[new_idx];
            let old_entry = read_fat_entry(&fat_data, old_cluster, fat_type);

            let new_entry = if is_end_of_chain(old_entry, fat_type) {
                end_of_chain_marker(fat_type)
            } else if is_bad_cluster(old_entry, fat_type) {
                bad_cluster_marker(fat_type)
            } else if old_entry >= 2 {
                old_to_new.get(&old_entry).copied().unwrap_or_else(|| {
                    end_of_chain_marker(fat_type)
                })
            } else {
                0
            };

            write_fat_entry(&mut single_fat, new_cluster, new_entry, fat_type);
        }

        // Build complete FAT tables (num_fats copies)
        let mut fat_tables = Vec::with_capacity(new_fat_byte_size * num_fats as usize);
        for _ in 0..num_fats {
            fat_tables.extend_from_slice(&single_fat);
        }

        // --- Build root directory (FAT12/16 only) ---
        let root_dir = if fat_type != FatType::Fat32 && root_entry_count > 0 {
            let root_start_sector = reserved_sectors + (num_fats * original_sectors_per_fat);
            let root_size = root_entry_count as u64 * DIR_ENTRY_SIZE as u64;
            let root_abs_offset = partition_offset + root_start_sector * bytes_per_sector;
            source.seek(SeekFrom::Start(root_abs_offset))?;
            let mut rd = vec![0u8; root_size as usize];
            source.read_exact(&mut rd)?;
            patch_directory_cluster_refs(&mut rd, &old_to_new);
            rd
        } else {
            Vec::new()
        };

        // --- Calculate virtual layout offsets ---
        let fat_offset_in_image = reserved_sectors * bytes_per_sector;
        let root_dir_offset_in_image = fat_offset_in_image + (num_fats * new_sectors_per_fat * bytes_per_sector);
        let data_offset_in_image = root_dir_offset_in_image + (root_dir_sectors * bytes_per_sector);
        let total_virtual_size = new_total_sectors * bytes_per_sector;

        let info = CompactInfo {
            original_size,
            compacted_size: total_virtual_size,
            clusters_used,
        };

        Ok((
            CompactFatReader {
                source,
                partition_offset,
                boot_sector,
                fat_tables,
                root_dir,
                new_to_old,
                old_to_new,
                directory_clusters,
                src_data_start_abs: src_data_start_abs,
                bytes_per_sector,
                sectors_per_cluster,
                cluster_size,
                fat_offset: fat_offset_in_image,
                root_dir_offset: root_dir_offset_in_image,
                data_offset: data_offset_in_image,
                total_size: total_virtual_size,
                position: 0,
                cluster_buf: vec![0u8; cluster_size],
                cluster_buf_idx: None,
            },
            info,
        ))
    }

    /// Load a data cluster from the source into the internal buffer,
    /// patching directory entries if the cluster contains directory data.
    fn load_cluster(&mut self, new_cluster_idx: usize) -> io::Result<()> {
        if self.cluster_buf_idx == Some(new_cluster_idx) {
            return Ok(());
        }

        let old_cluster = self.new_to_old[new_cluster_idx];

        // Compute source absolute offset for this cluster
        let src_offset = self.src_data_start_abs
            + (old_cluster as u64 - 2) * self.sectors_per_cluster * self.bytes_per_sector;

        self.source.seek(SeekFrom::Start(src_offset))?;
        self.source.read_exact(&mut self.cluster_buf)?;

        // If this cluster contains directory data, patch cluster references
        if self.directory_clusters.contains(&old_cluster) {
            patch_directory_cluster_refs(&mut self.cluster_buf, &self.old_to_new);
        }

        self.cluster_buf_idx = Some(new_cluster_idx);
        Ok(())
    }

    /// Total size of the compacted virtual image.
    pub fn total_size(&self) -> u64 {
        self.total_size
    }
}

impl<R: Read + Seek> Read for CompactFatReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.position >= self.total_size {
            return Ok(0);
        }

        let pos = self.position;
        let to_read = buf.len().min((self.total_size - pos) as usize);
        if to_read == 0 {
            return Ok(0);
        }

        let mut filled = 0;

        while filled < to_read {
            let current_pos = pos + filled as u64;
            if current_pos >= self.total_size {
                break;
            }

            let (src_slice, offset_in_src) = if current_pos < self.fat_offset {
                // Boot sector / reserved sectors region
                let off = current_pos as usize;
                (&self.boot_sector[..], off)
            } else if current_pos < self.root_dir_offset {
                // FAT tables region
                let off = (current_pos - self.fat_offset) as usize;
                (&self.fat_tables[..], off)
            } else if current_pos < self.data_offset {
                // Root directory region (FAT12/16) or empty (FAT32)
                let off = (current_pos - self.root_dir_offset) as usize;
                if self.root_dir.is_empty() {
                    // FAT32: data_offset == root_dir_offset, so this shouldn't
                    // happen, but produce zeros as safety
                    buf[filled] = 0;
                    filled += 1;
                    continue;
                }
                (&self.root_dir[..], off)
            } else {
                // Data cluster region — need to load from source
                let rel = (current_pos - self.data_offset) as usize;
                let cluster_idx = rel / self.cluster_size;
                let offset_in_cluster = rel % self.cluster_size;

                if cluster_idx >= self.new_to_old.len() {
                    // Beyond mapped clusters — emit zeros (safety fallback)
                    let remaining_in_pad = self.cluster_size - offset_in_cluster;
                    let copy_len = (to_read - filled).min(remaining_in_pad);
                    buf[filled..filled + copy_len].fill(0);
                    filled += copy_len;
                    continue;
                }

                self.load_cluster(cluster_idx)?;
                let remaining_in_cluster = self.cluster_size - offset_in_cluster;
                let copy_len = (to_read - filled).min(remaining_in_cluster);
                buf[filled..filled + copy_len]
                    .copy_from_slice(&self.cluster_buf[offset_in_cluster..offset_in_cluster + copy_len]);
                filled += copy_len;
                continue;
            };

            // Copy from pre-built section
            let remaining_in_section = src_slice.len() - offset_in_src;
            let copy_len = (to_read - filled).min(remaining_in_section);
            buf[filled..filled + copy_len]
                .copy_from_slice(&src_slice[offset_in_src..offset_in_src + copy_len]);
            filled += copy_len;
        }

        self.position += filled as u64;
        Ok(filled)
    }
}

// ---------------------------------------------------------------------------
// Helper functions for FAT entry manipulation and directory walking
// ---------------------------------------------------------------------------

/// Read a FAT entry value for the given cluster number.
fn read_fat_entry(fat_data: &[u8], cluster: u32, fat_type: FatType) -> u32 {
    match fat_type {
        FatType::Fat12 => {
            let byte_off = (cluster as usize * 3) / 2;
            if byte_off + 1 >= fat_data.len() {
                return 0;
            }
            let val = u16::from_le_bytes([fat_data[byte_off], fat_data[byte_off + 1]]);
            if cluster & 1 == 1 {
                (val >> 4) as u32
            } else {
                (val & 0x0FFF) as u32
            }
        }
        FatType::Fat16 => {
            let off = cluster as usize * 2;
            if off + 1 >= fat_data.len() {
                return 0;
            }
            u16::from_le_bytes([fat_data[off], fat_data[off + 1]]) as u32
        }
        FatType::Fat32 => {
            let off = cluster as usize * 4;
            if off + 3 >= fat_data.len() {
                return 0;
            }
            u32::from_le_bytes([fat_data[off], fat_data[off + 1], fat_data[off + 2], fat_data[off + 3]])
                & 0x0FFF_FFFF
        }
    }
}

/// Write a FAT entry value for the given cluster number.
fn write_fat_entry(fat_data: &mut [u8], cluster: u32, value: u32, fat_type: FatType) {
    match fat_type {
        FatType::Fat12 => {
            let byte_off = (cluster as usize * 3) / 2;
            if byte_off + 1 >= fat_data.len() {
                return;
            }
            let existing = u16::from_le_bytes([fat_data[byte_off], fat_data[byte_off + 1]]);
            let new_val = if cluster & 1 == 1 {
                (existing & 0x000F) | ((value as u16) << 4)
            } else {
                (existing & 0xF000) | (value as u16 & 0x0FFF)
            };
            let bytes = new_val.to_le_bytes();
            fat_data[byte_off] = bytes[0];
            fat_data[byte_off + 1] = bytes[1];
        }
        FatType::Fat16 => {
            let off = cluster as usize * 2;
            if off + 1 >= fat_data.len() {
                return;
            }
            let bytes = (value as u16).to_le_bytes();
            fat_data[off] = bytes[0];
            fat_data[off + 1] = bytes[1];
        }
        FatType::Fat32 => {
            let off = cluster as usize * 4;
            if off + 3 >= fat_data.len() {
                return;
            }
            // Preserve upper 4 bits of original entry
            let existing = u32::from_le_bytes([
                fat_data[off], fat_data[off + 1], fat_data[off + 2], fat_data[off + 3],
            ]);
            let new_val = (existing & 0xF000_0000) | (value & 0x0FFF_FFFF);
            let bytes = new_val.to_le_bytes();
            fat_data[off] = bytes[0];
            fat_data[off + 1] = bytes[1];
            fat_data[off + 2] = bytes[2];
            fat_data[off + 3] = bytes[3];
        }
    }
}

fn is_end_of_chain(entry: u32, fat_type: FatType) -> bool {
    match fat_type {
        FatType::Fat12 => entry >= 0x0FF8,
        FatType::Fat16 => entry >= 0xFFF8,
        FatType::Fat32 => entry >= 0x0FFF_FFF8,
    }
}

fn is_bad_cluster(entry: u32, fat_type: FatType) -> bool {
    match fat_type {
        FatType::Fat12 => entry == 0x0FF7,
        FatType::Fat16 => entry == 0xFFF7,
        FatType::Fat32 => entry == 0x0FFF_FFF7,
    }
}

fn end_of_chain_marker(fat_type: FatType) -> u32 {
    match fat_type {
        FatType::Fat12 => 0x0FFF,
        FatType::Fat16 => 0xFFFF,
        FatType::Fat32 => 0x0FFF_FFFF,
    }
}

fn bad_cluster_marker(fat_type: FatType) -> u32 {
    match fat_type {
        FatType::Fat12 => 0x0FF7,
        FatType::Fat16 => 0xFFF7,
        FatType::Fat32 => 0x0FFF_FFF7,
    }
}

/// Patch cluster references in directory entry data.
///
/// For each 32-byte entry that is a regular file or directory (not LFN,
/// not volume ID, not deleted, not end-of-dir), remap the cluster number
/// fields (bytes 20-21 high, 26-27 low) using the old_to_new mapping.
fn patch_directory_cluster_refs(data: &mut [u8], old_to_new: &HashMap<u32, u32>) {
    let num_entries = data.len() / DIR_ENTRY_SIZE;
    for i in 0..num_entries {
        let off = i * DIR_ENTRY_SIZE;
        if data[off] == 0x00 {
            break;
        }
        if data[off] == 0xE5 {
            continue;
        }
        let attr = data[off + 11];
        if attr == ATTR_LONG_NAME || (attr & ATTR_VOLUME_ID) != 0 {
            continue;
        }

        let cluster_hi = u16::from_le_bytes([data[off + 20], data[off + 21]]) as u32;
        let cluster_lo = u16::from_le_bytes([data[off + 26], data[off + 27]]) as u32;
        let old_cluster = (cluster_hi << 16) | cluster_lo;

        if old_cluster == 0 {
            continue; // zero-length file, leave as 0
        }

        if let Some(&new_cluster) = old_to_new.get(&old_cluster) {
            let new_hi = ((new_cluster >> 16) as u16).to_le_bytes();
            let new_lo = ((new_cluster & 0xFFFF) as u16).to_le_bytes();
            data[off + 20] = new_hi[0];
            data[off + 21] = new_hi[1];
            data[off + 26] = new_lo[0];
            data[off + 27] = new_lo[1];
        }
    }
}

/// Read a cluster chain from the source, following FAT links.
fn read_chain_from_source<R: Read + Seek>(
    source: &mut R,
    partition_offset: u64,
    fat_data: &[u8],
    fat_type: FatType,
    start_cluster: u32,
    bytes_per_sector: u64,
    sectors_per_cluster: u64,
    data_start_sector: u64,
    cluster_size: usize,
    max_entries: u32,
) -> Result<Vec<u8>, FilesystemError> {
    let mut data = Vec::new();
    let mut cluster = start_cluster;
    let mut count = 0u32;

    loop {
        if cluster < 2 || cluster >= max_entries || count > max_entries {
            break;
        }

        let first_sector = data_start_sector + (cluster as u64 - 2) * sectors_per_cluster;
        let abs_offset = partition_offset + first_sector * bytes_per_sector;
        source.seek(SeekFrom::Start(abs_offset))?;
        let mut buf = vec![0u8; cluster_size];
        source.read_exact(&mut buf)?;
        data.extend_from_slice(&buf);
        count += 1;

        let entry = read_fat_entry(fat_data, cluster, fat_type);
        if is_end_of_chain(entry, fat_type) || entry < 2 {
            break;
        }
        cluster = entry;
    }

    Ok(data)
}

/// Scan directory data for subdirectory entries, adding them to the BFS queue.
fn find_subdirectories_in_data(
    dir_data: &[u8],
    queue: &mut VecDeque<u32>,
    visited_dirs: &mut HashSet<u32>,
    directory_clusters: &mut HashSet<u32>,
    fat_data: &[u8],
    fat_type: FatType,
    max_entries: u32,
) {
    let num_entries = dir_data.len() / DIR_ENTRY_SIZE;
    for i in 0..num_entries {
        let off = i * DIR_ENTRY_SIZE;
        let entry_bytes = &dir_data[off..off + DIR_ENTRY_SIZE];

        if entry_bytes[0] == 0x00 {
            break;
        }
        if entry_bytes[0] == 0xE5 {
            continue;
        }

        let attr = entry_bytes[11];
        if attr == ATTR_LONG_NAME || (attr & ATTR_VOLUME_ID) != 0 {
            continue;
        }
        if entry_bytes[0] == b'.' {
            continue;
        }

        if (attr & ATTR_DIRECTORY) != 0 {
            let cluster_hi = u16::from_le_bytes([entry_bytes[20], entry_bytes[21]]) as u32;
            let cluster_lo = u16::from_le_bytes([entry_bytes[26], entry_bytes[27]]) as u32;
            let sub_cluster = (cluster_hi << 16) | cluster_lo;

            if sub_cluster >= 2
                && sub_cluster < max_entries
                && !visited_dirs.contains(&sub_cluster)
            {
                visited_dirs.insert(sub_cluster);
                queue.push_back(sub_cluster);

                // Mark all clusters in this subdirectory's chain as directory clusters
                let mut c = sub_cluster;
                loop {
                    if c < 2 || c >= max_entries {
                        break;
                    }
                    directory_clusters.insert(c);
                    let e = read_fat_entry(fat_data, c, fat_type);
                    if is_end_of_chain(e, fat_type) || e < 2 {
                        break;
                    }
                    c = e;
                }
            }
        }
    }
}

/// Walk directory tree for FAT32 (root directory is in data clusters).
fn walk_directory_tree_fat32<R: Read + Seek>(
    source: &mut R,
    partition_offset: u64,
    fat_data: &[u8],
    fat_type: FatType,
    root_cluster: u32,
    bytes_per_sector: u64,
    sectors_per_cluster: u64,
    data_start_sector: u64,
    cluster_size: usize,
    max_entries: u32,
    directory_clusters: &mut HashSet<u32>,
) -> Result<(), FilesystemError> {
    // Mark root chain as directory
    let mut cluster = root_cluster;
    loop {
        if cluster < 2 || cluster >= max_entries {
            break;
        }
        directory_clusters.insert(cluster);
        let entry = read_fat_entry(fat_data, cluster, fat_type);
        if is_end_of_chain(entry, fat_type) || entry < 2 {
            break;
        }
        cluster = entry;
    }

    let mut queue: VecDeque<u32> = VecDeque::new();
    queue.push_back(root_cluster);
    let mut visited_dirs: HashSet<u32> = HashSet::new();
    visited_dirs.insert(root_cluster);

    while let Some(dir_start) = queue.pop_front() {
        let dir_data = read_chain_from_source(
            source,
            partition_offset,
            fat_data,
            fat_type,
            dir_start,
            bytes_per_sector,
            sectors_per_cluster,
            data_start_sector,
            cluster_size,
            max_entries,
        )?;

        find_subdirectories_in_data(
            &dir_data,
            &mut queue,
            &mut visited_dirs,
            directory_clusters,
            fat_data,
            fat_type,
            max_entries,
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// FAT manipulation functions (resize, validate, BPB patching)
// ---------------------------------------------------------------------------

/// Resize a FAT12/16/32 filesystem in-place within an output file.
///
/// The partition data must already be written starting at `partition_offset`.
/// This function:
///
/// - For shrinking: updates BPB `total_sectors` only (the oversized FAT is harmless)
/// - For growing: extends FAT tables with free cluster entries, shifting the
///   data region forward if the FAT needs additional sectors, then updates BPB
/// - For FAT32: also updates the backup BPB at sector 6 and the FSInfo sector
///
/// Silently returns `Ok(false)` for non-FAT partitions.
/// Returns `Ok(true)` if the resize was performed.
pub fn resize_fat_in_place(
    file: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    new_total_sectors: u32,
    log_cb: &mut impl FnMut(&str),
) -> Result<bool> {
    // --- 1. Read and validate BPB ---
    file.seek(SeekFrom::Start(partition_offset))?;
    let mut bpb = [0u8; 512];
    file.read_exact(&mut bpb)?;

    if bpb[0] != 0xEB && bpb[0] != 0xE9 {
        return Ok(false); // Not a FAT BPB
    }

    let bytes_per_sector = u16::from_le_bytes([bpb[11], bpb[12]]);
    if !matches!(bytes_per_sector, 512 | 1024 | 2048 | 4096) {
        return Ok(false);
    }
    let bps = bytes_per_sector as u64;

    let sectors_per_cluster = bpb[13];
    if sectors_per_cluster == 0 || !sectors_per_cluster.is_power_of_two() {
        return Ok(false);
    }
    let spc = sectors_per_cluster as u32;

    let reserved_sectors = u16::from_le_bytes([bpb[14], bpb[15]]) as u32;
    let num_fats = bpb[16] as u32;
    if num_fats == 0 || num_fats > 2 {
        return Ok(false);
    }

    let root_entry_count = u16::from_le_bytes([bpb[17], bpb[18]]);
    let ts16 = u16::from_le_bytes([bpb[19], bpb[20]]);
    let spf16 = u16::from_le_bytes([bpb[22], bpb[23]]);
    let ts32 = u32::from_le_bytes([bpb[32], bpb[33], bpb[34], bpb[35]]);
    let spf32 = u32::from_le_bytes([bpb[36], bpb[37], bpb[38], bpb[39]]);

    let is_fat32 = spf16 == 0 && root_entry_count == 0;
    let old_spf = if is_fat32 { spf32 } else { spf16 as u32 };
    let old_total = if ts16 != 0 { ts16 as u32 } else { ts32 };

    if old_total == new_total_sectors {
        return Ok(false); // Nothing to do
    }

    let root_dir_sectors = if is_fat32 {
        0u32
    } else {
        ((root_entry_count as u32 * 32) + (bytes_per_sector as u32 - 1))
            / bytes_per_sector as u32
    };

    // --- 2. Calculate old layout ---
    let old_data_start = reserved_sectors + num_fats * old_spf + root_dir_sectors;
    let old_data_sectors = old_total.saturating_sub(old_data_start);
    let old_clusters = old_data_sectors / spc;

    let fat_bits: u32 = if is_fat32 {
        32
    } else if old_clusters < 4085 {
        12
    } else {
        16
    };

    // --- 3. Calculate new layout ---
    let new_spf = compute_fat_sectors(
        new_total_sectors, reserved_sectors, num_fats,
        root_dir_sectors, spc, fat_bits, bytes_per_sector,
    );
    let new_data_start = reserved_sectors + num_fats * new_spf + root_dir_sectors;
    let new_data_sectors = new_total_sectors.saturating_sub(new_data_start);
    let new_clusters = new_data_sectors / spc;

    // Verify FAT type doesn't change
    let new_fat_bits = if is_fat32 {
        32
    } else if new_clusters < 4085 {
        12
    } else {
        16
    };
    if new_fat_bits != fat_bits {
        log_cb(&format!(
            "FAT resize: type would change from FAT{} to FAT{}, updating BPB only",
            fat_bits, new_fat_bits,
        ));
        patch_bpb_total_sectors(&mut bpb, new_total_sectors, ts16);
        write_bpb(file, partition_offset, &bpb, is_fat32, bytes_per_sector)?;
        return Ok(true);
    }

    let growing = new_total_sectors > old_total;
    let fat_needs_growth = new_spf > old_spf;

    log_cb(&format!(
        "FAT{}: clusters {} -> {}, spf {} -> {}",
        fat_bits, old_clusters, new_clusters, old_spf, new_spf,
    ));

    // --- 4. Growing with FAT growth: shift data + extend FAT ---
    if growing && fat_needs_growth {
        let shift_sectors = (new_spf - old_spf) * num_fats;
        let shift_bytes = shift_sectors as u64 * bps;

        // Read old FAT data (one copy — both copies are identical)
        let old_fat_start = partition_offset + reserved_sectors as u64 * bps;
        file.seek(SeekFrom::Start(old_fat_start))?;
        let old_fat_bytes = old_spf as usize * bps as usize;
        let mut fat_data = vec![0u8; old_fat_bytes];
        file.read_exact(&mut fat_data)?;

        // Shift rootdir (FAT12/16) + data region forward to make room for larger FATs
        let move_start_sector = reserved_sectors + num_fats * old_spf;
        let move_start = partition_offset + move_start_sector as u64 * bps;
        let move_end = partition_offset + old_total as u64 * bps;

        if move_end > move_start {
            shift_region_forward(file, move_start, move_end, shift_bytes)?;
            log_cb(&format!(
                "Shifted data region forward by {} sectors",
                shift_sectors,
            ));
        }

        // Extend FAT data with free entries (zero = free for all FAT types)
        let new_fat_bytes = new_spf as usize * bps as usize;
        fat_data.resize(new_fat_bytes, 0);

        // Write extended FAT to each copy
        for i in 0..num_fats as u64 {
            let fat_pos = partition_offset
                + reserved_sectors as u64 * bps
                + i * new_fat_bytes as u64;
            file.seek(SeekFrom::Start(fat_pos))?;
            file.write_all(&fat_data)?;
        }

        log_cb(&format!(
            "Extended FAT: {} -> {} sectors per copy",
            old_spf, new_spf,
        ));
    } else if growing {
        log_cb("FAT has spare capacity, no table extension needed");
    }

    // --- 5. Update BPB ---
    patch_bpb_total_sectors(&mut bpb, new_total_sectors, ts16);
    if new_spf != old_spf {
        if is_fat32 {
            bpb[36..40].copy_from_slice(&new_spf.to_le_bytes());
        } else {
            bpb[22..24].copy_from_slice(&(new_spf as u16).to_le_bytes());
        }
    }
    write_bpb(file, partition_offset, &bpb, is_fat32, bytes_per_sector)?;

    // --- 6. Set FAT dirty/clean flags ---
    // FAT[1] contains volume status flags. After manipulation we must set the
    // clean shutdown + no I/O error bits, otherwise Windows 95/98 and scandisk
    // will detect corruption.
    // FAT12 has no dirty flags in FAT[1].
    if fat_bits == 16 || fat_bits == 32 {
        let fat_start = partition_offset + reserved_sectors as u64 * bps;
        for fat_copy in 0..num_fats as u64 {
            let fat_copy_start = fat_start + fat_copy * new_spf as u64 * bps;
            let entry1_offset = fat_copy_start + match fat_bits {
                16 => 2u64,  // FAT16: entry 1 at byte offset 2
                32 => 4u64,  // FAT32: entry 1 at byte offset 4
                _ => unreachable!(),
            };
            file.seek(SeekFrom::Start(entry1_offset))?;
            match fat_bits {
                16 => {
                    let mut entry = [0u8; 2];
                    file.read_exact(&mut entry)?;
                    let mut val = u16::from_le_bytes(entry);
                    val |= 0xC000; // bit 15 = clean shutdown, bit 14 = no I/O errors
                    file.seek(SeekFrom::Start(entry1_offset))?;
                    file.write_all(&val.to_le_bytes())?;
                }
                32 => {
                    let mut entry = [0u8; 4];
                    file.read_exact(&mut entry)?;
                    let mut val = u32::from_le_bytes(entry);
                    val |= 0x0C00_0000; // bit 27 = clean shutdown, bit 26 = no I/O errors
                    file.seek(SeekFrom::Start(entry1_offset))?;
                    file.write_all(&val.to_le_bytes())?;
                }
                _ => unreachable!(),
            }
        }
        log_cb(&format!("FAT{}: set clean shutdown flags in FAT[1]", fat_bits));
    }

    // --- 7. FAT32: update FSInfo ---
    if is_fat32 {
        let fsinfo_sector = u16::from_le_bytes([bpb[48], bpb[49]]);
        if fsinfo_sector > 0 && (fsinfo_sector as u32) < reserved_sectors {
            let fsinfo_offset = partition_offset + fsinfo_sector as u64 * bps;
            file.seek(SeekFrom::Start(fsinfo_offset))?;
            let mut fsinfo = [0u8; 512];
            file.read_exact(&mut fsinfo)?;

            let sig1 = u32::from_le_bytes(fsinfo[0..4].try_into().unwrap());
            let sig2 = u32::from_le_bytes(fsinfo[484..488].try_into().unwrap());
            if sig1 == 0x41615252 && sig2 == 0x61417272 {
                // Calculate actual free cluster count instead of setting to unknown.
                // Windows 95's FAT32 driver may not recompute from 0xFFFFFFFF and
                // could display 0 free space.
                let actual_free = compute_free_clusters(
                    file, partition_offset, reserved_sectors, new_spf,
                    new_clusters, bps, fat_bits,
                )?;
                fsinfo[488..492].copy_from_slice(&actual_free.to_le_bytes());

                // Next free cluster hint
                if new_clusters > old_clusters {
                    fsinfo[492..496].copy_from_slice(&(old_clusters + 2).to_le_bytes());
                } else {
                    fsinfo[492..496].copy_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
                }

                file.seek(SeekFrom::Start(fsinfo_offset))?;
                file.write_all(&fsinfo)?;
                log_cb(&format!(
                    "Updated FAT32 FSInfo: {} free clusters", actual_free
                ));
            }
        }
    }

    file.flush()?;
    log_cb(&format!(
        "FAT{} resize complete: {} clusters, {} total sectors",
        fat_bits, new_clusters, new_total_sectors,
    ));

    Ok(true)
}

/// Validate the integrity of a FAT filesystem after resize/manipulation.
///
/// Checks BPB consistency, FAT[0] media byte, FAT[1] clean flags,
/// FSInfo signatures (FAT32), and cluster chain bounds.
/// Returns a list of warning messages (empty = all good).
pub fn validate_fat_integrity(
    file: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    log_cb: &mut impl FnMut(&str),
) -> Result<Vec<String>> {
    let mut warnings = Vec::new();

    // Read BPB
    file.seek(SeekFrom::Start(partition_offset))?;
    let mut bpb = [0u8; 512];
    file.read_exact(&mut bpb)?;

    if bpb[0] != 0xEB && bpb[0] != 0xE9 {
        warnings.push("BPB: invalid jump instruction".to_string());
        return Ok(warnings);
    }

    let bytes_per_sector = u16::from_le_bytes([bpb[11], bpb[12]]);
    if !matches!(bytes_per_sector, 512 | 1024 | 2048 | 4096) {
        warnings.push(format!("BPB: invalid bytes_per_sector: {}", bytes_per_sector));
        return Ok(warnings);
    }
    let bps = bytes_per_sector as u64;

    let sectors_per_cluster = bpb[13];
    if sectors_per_cluster == 0 || !sectors_per_cluster.is_power_of_two() {
        warnings.push(format!("BPB: invalid sectors_per_cluster: {}", sectors_per_cluster));
        return Ok(warnings);
    }
    let spc = sectors_per_cluster as u32;

    let reserved_sectors = u16::from_le_bytes([bpb[14], bpb[15]]) as u32;
    let num_fats = bpb[16] as u32;
    let root_entry_count = u16::from_le_bytes([bpb[17], bpb[18]]);
    let ts16 = u16::from_le_bytes([bpb[19], bpb[20]]);
    let spf16 = u16::from_le_bytes([bpb[22], bpb[23]]);
    let ts32 = u32::from_le_bytes([bpb[32], bpb[33], bpb[34], bpb[35]]);
    let spf32 = u32::from_le_bytes([bpb[36], bpb[37], bpb[38], bpb[39]]);

    let is_fat32 = spf16 == 0 && root_entry_count == 0;
    let spf = if is_fat32 { spf32 } else { spf16 as u32 };
    let total = if ts16 != 0 { ts16 as u32 } else { ts32 };

    let root_dir_sectors = if is_fat32 {
        0u32
    } else {
        ((root_entry_count as u32 * 32) + (bytes_per_sector as u32 - 1)) / bytes_per_sector as u32
    };

    let data_start = reserved_sectors + num_fats * spf + root_dir_sectors;
    let data_sectors = total.saturating_sub(data_start);
    let clusters = data_sectors / spc;

    let fat_bits: u32 = if is_fat32 {
        32
    } else if clusters < 4085 {
        12
    } else {
        16
    };

    // Check BPB self-consistency
    if data_start > total {
        warnings.push(format!(
            "BPB: data_start ({}) > total_sectors ({})", data_start, total
        ));
    }

    // Check FAT[0] media byte
    let fat_start = partition_offset + reserved_sectors as u64 * bps;
    file.seek(SeekFrom::Start(fat_start))?;
    match fat_bits {
        12 => {
            let mut entry = [0u8; 2];
            file.read_exact(&mut entry)?;
            let val = u16::from_le_bytes(entry) & 0x0FFF;
            let media = bpb[21];
            if val != (0x0F00 | media as u16) {
                warnings.push(format!(
                    "FAT12: FAT[0] = 0x{:03X}, expected 0x{:03X}",
                    val, 0x0F00 | media as u16
                ));
            }
        }
        16 => {
            let mut entry = [0u8; 2];
            file.read_exact(&mut entry)?;
            let val = u16::from_le_bytes(entry);
            if val & 0x00FF != bpb[21] as u16 {
                warnings.push(format!(
                    "FAT16: FAT[0] low byte = 0x{:02X}, media byte = 0x{:02X}",
                    val & 0xFF, bpb[21]
                ));
            }
        }
        32 => {
            let mut entry = [0u8; 4];
            file.read_exact(&mut entry)?;
            let val = u32::from_le_bytes(entry) & 0x0FFF_FFFF;
            if val & 0xFF != bpb[21] as u32 {
                warnings.push(format!(
                    "FAT32: FAT[0] low byte = 0x{:02X}, media byte = 0x{:02X}",
                    val & 0xFF, bpb[21]
                ));
            }
        }
        _ => {}
    }

    // Check FAT[1] clean flags
    match fat_bits {
        16 => {
            file.seek(SeekFrom::Start(fat_start + 2))?;
            let mut entry = [0u8; 2];
            file.read_exact(&mut entry)?;
            let val = u16::from_le_bytes(entry);
            if val & 0x8000 == 0 {
                warnings.push("FAT16: FAT[1] clean shutdown bit not set".to_string());
            }
            if val & 0x4000 == 0 {
                warnings.push("FAT16: FAT[1] no-error bit not set".to_string());
            }
        }
        32 => {
            file.seek(SeekFrom::Start(fat_start + 4))?;
            let mut entry = [0u8; 4];
            file.read_exact(&mut entry)?;
            let val = u32::from_le_bytes(entry);
            if val & 0x0800_0000 == 0 {
                warnings.push("FAT32: FAT[1] clean shutdown bit not set".to_string());
            }
            if val & 0x0400_0000 == 0 {
                warnings.push("FAT32: FAT[1] no-error bit not set".to_string());
            }
        }
        _ => {} // FAT12 has no dirty flags
    }

    // Check FSInfo (FAT32 only)
    if is_fat32 {
        let fsinfo_sector = u16::from_le_bytes([bpb[48], bpb[49]]);
        if fsinfo_sector > 0 && (fsinfo_sector as u32) < reserved_sectors {
            let fsinfo_offset = partition_offset + fsinfo_sector as u64 * bps;
            file.seek(SeekFrom::Start(fsinfo_offset))?;
            let mut fsinfo = [0u8; 512];
            file.read_exact(&mut fsinfo)?;

            let sig1 = u32::from_le_bytes(fsinfo[0..4].try_into().unwrap());
            let sig2 = u32::from_le_bytes(fsinfo[484..488].try_into().unwrap());
            if sig1 != 0x41615252 {
                warnings.push(format!("FSInfo: bad signature1 0x{:08X}", sig1));
            }
            if sig2 != 0x61417272 {
                warnings.push(format!("FSInfo: bad signature2 0x{:08X}", sig2));
            }

            let free_count = u32::from_le_bytes(fsinfo[488..492].try_into().unwrap());
            if free_count != 0xFFFF_FFFF && free_count > clusters {
                warnings.push(format!(
                    "FSInfo: free_count ({}) > total clusters ({})",
                    free_count, clusters
                ));
            }
        }
    }

    // Check for cluster chains referencing beyond total
    let fat_byte_size = spf as u64 * bps;
    file.seek(SeekFrom::Start(fat_start))?;
    let mut fat_data = vec![0u8; fat_byte_size as usize];
    file.read_exact(&mut fat_data)?;

    let total_entries = clusters + 2;
    let mut out_of_bounds = 0u32;
    for cluster in 2..total_entries {
        let entry = match fat_bits {
            12 => {
                let byte_off = (cluster as usize * 3) / 2;
                if byte_off + 1 >= fat_data.len() { continue; }
                let val = u16::from_le_bytes([fat_data[byte_off], fat_data[byte_off + 1]]);
                if cluster & 1 == 1 { (val >> 4) as u32 } else { (val & 0x0FFF) as u32 }
            }
            16 => {
                let off = cluster as usize * 2;
                if off + 1 >= fat_data.len() { continue; }
                u16::from_le_bytes([fat_data[off], fat_data[off + 1]]) as u32
            }
            32 => {
                let off = cluster as usize * 4;
                if off + 3 >= fat_data.len() { continue; }
                u32::from_le_bytes([
                    fat_data[off], fat_data[off + 1],
                    fat_data[off + 2], fat_data[off + 3],
                ]) & 0x0FFF_FFFF
            }
            _ => continue,
        };

        // Check if entry points to a valid cluster (not free, not EOC, not bad)
        let is_free = entry == 0;
        let is_eoc = match fat_bits {
            12 => entry >= 0x0FF8,
            16 => entry >= 0xFFF8,
            32 => entry >= 0x0FFF_FFF8,
            _ => false,
        };
        let is_bad = match fat_bits {
            12 => entry == 0x0FF7,
            16 => entry == 0xFFF7,
            32 => entry == 0x0FFF_FFF7,
            _ => false,
        };

        if !is_free && !is_eoc && !is_bad {
            if entry < 2 || entry >= total_entries {
                out_of_bounds += 1;
            }
        }
    }

    if out_of_bounds > 0 {
        warnings.push(format!(
            "{} cluster(s) reference beyond total ({})",
            out_of_bounds, total_entries
        ));
    }

    for w in &warnings {
        log_cb(&format!("FAT validation warning: {}", w));
    }
    if warnings.is_empty() {
        log_cb("FAT validation: all checks passed");
    }

    Ok(warnings)
}

/// Update the BPB hidden sectors field (offset 0x1C) with the partition's
/// actual start LBA. This field must match the partition's position on disk.
pub fn patch_bpb_hidden_sectors(
    file: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    start_lba: u64,
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    file.seek(SeekFrom::Start(partition_offset))?;
    let mut bpb = [0u8; 512];
    file.read_exact(&mut bpb)?;

    if bpb[0] != 0xEB && bpb[0] != 0xE9 {
        return Ok(()); // Not a FAT BPB
    }

    let bytes_per_sector = u16::from_le_bytes([bpb[11], bpb[12]]);
    if !matches!(bytes_per_sector, 512 | 1024 | 2048 | 4096) {
        return Ok(()); // Not a valid FAT BPB
    }

    let old_hidden = u32::from_le_bytes([bpb[0x1C], bpb[0x1D], bpb[0x1E], bpb[0x1F]]);
    let new_hidden = start_lba as u32;

    if old_hidden != new_hidden {
        bpb[0x1C..0x20].copy_from_slice(&new_hidden.to_le_bytes());

        let spf16 = u16::from_le_bytes([bpb[22], bpb[23]]);
        let root_entry_count = u16::from_le_bytes([bpb[17], bpb[18]]);
        let is_fat32 = spf16 == 0 && root_entry_count == 0;

        write_bpb(file, partition_offset, &bpb, is_fat32, bytes_per_sector)?;
        log_cb(&format!(
            "Patched BPB hidden sectors: {} -> {}",
            old_hidden, new_hidden
        ));
    }

    Ok(())
}

/// Patch BPB total_sectors fields (16-bit or 32-bit) in a BPB buffer.
fn patch_bpb_total_sectors(bpb: &mut [u8; 512], new_total: u32, old_ts16: u16) {
    if old_ts16 != 0 && new_total <= u16::MAX as u32 {
        bpb[19..21].copy_from_slice(&(new_total as u16).to_le_bytes());
        bpb[32..36].copy_from_slice(&0u32.to_le_bytes());
    } else {
        bpb[19..21].copy_from_slice(&0u16.to_le_bytes());
        bpb[32..36].copy_from_slice(&new_total.to_le_bytes());
    }
}

/// Write a BPB to the primary boot sector and (for FAT32) the backup at sector 6.
fn write_bpb(
    file: &mut (impl Write + Seek),
    partition_offset: u64,
    bpb: &[u8; 512],
    is_fat32: bool,
    bytes_per_sector: u16,
) -> Result<()> {
    file.seek(SeekFrom::Start(partition_offset))?;
    file.write_all(bpb)?;
    if is_fat32 {
        let backup = partition_offset + 6 * bytes_per_sector as u64;
        file.seek(SeekFrom::Start(backup))?;
        file.write_all(bpb)?;
    }
    Ok(())
}

/// Compute the number of sectors needed for one FAT copy given the partition
/// parameters and FAT type.
fn compute_fat_sectors(
    total_sectors: u32,
    reserved: u32,
    num_fats: u32,
    root_dir_sectors: u32,
    sectors_per_cluster: u32,
    fat_bits: u32,
    bytes_per_sector: u16,
) -> u32 {
    let avail = total_sectors.saturating_sub(reserved + root_dir_sectors) as u64;
    let bps = bytes_per_sector as u64;
    let spc = sectors_per_cluster as u64;
    let n = num_fats as u64;

    match fat_bits {
        12 => {
            // FAT12: 1.5 bytes per entry — use iterative approach
            let mut spf = 1u32;
            loop {
                let data_sectors = avail.saturating_sub(n * spf as u64);
                let clusters = data_sectors / spc;
                let fat_bytes = ((clusters + 2) * 3 + 1) / 2;
                let needed = ((fat_bytes + bps - 1) / bps) as u32;
                if needed <= spf {
                    return spf;
                }
                spf = needed;
            }
        }
        16 => {
            // FAT16: 2 bytes per entry
            // Closed-form: ceil(2 * (avail + 2*spc) / (bps*spc + 2*n))
            let num = 2 * (avail + 2 * spc);
            let den = bps * spc + 2 * n;
            ((num + den - 1) / den) as u32
        }
        32 => {
            // FAT32: 4 bytes per entry
            let num = 4 * (avail + 2 * spc);
            let den = bps * spc + 4 * n;
            ((num + den - 1) / den) as u32
        }
        _ => 1,
    }
}

/// Shift a region of a file forward by `shift` bytes.
/// Reads backward from the end to avoid overwriting unread data.
fn shift_region_forward(
    file: &mut (impl Read + Write + Seek),
    src_start: u64,
    src_end: u64,
    shift: u64,
) -> Result<()> {
    let data_len = src_end.saturating_sub(src_start);
    if data_len == 0 || shift == 0 {
        return Ok(());
    }

    let mut buf = vec![0u8; CHUNK_SIZE];
    let mut remaining = data_len;

    // Copy backward: read from the end, write to offset + shift
    while remaining > 0 {
        let chunk = remaining.min(CHUNK_SIZE as u64);
        let read_pos = src_start + remaining - chunk;

        file.seek(SeekFrom::Start(read_pos))?;
        file.read_exact(&mut buf[..chunk as usize])?;

        file.seek(SeekFrom::Start(read_pos + shift))?;
        file.write_all(&buf[..chunk as usize])?;

        remaining -= chunk;
    }

    // Zero-fill the gap left by the shift
    let zeros = vec![0u8; CHUNK_SIZE];
    let mut gap = shift;
    file.seek(SeekFrom::Start(src_start))?;
    while gap > 0 {
        let n = (gap as usize).min(CHUNK_SIZE);
        file.write_all(&zeros[..n])?;
        gap -= n as u64;
    }

    Ok(())
}

/// Count the number of free (zero-value) cluster entries in the FAT.
fn compute_free_clusters(
    file: &mut (impl Read + Seek),
    partition_offset: u64,
    reserved_sectors: u32,
    sectors_per_fat: u32,
    total_clusters: u32,
    bps: u64,
    fat_bits: u32,
) -> Result<u32> {
    let fat_start = partition_offset + reserved_sectors as u64 * bps;
    let fat_size = sectors_per_fat as u64 * bps;
    file.seek(SeekFrom::Start(fat_start))?;
    let mut fat_data = vec![0u8; fat_size as usize];
    file.read_exact(&mut fat_data)?;

    let total_entries = total_clusters + 2;
    let mut free_count: u32 = 0;

    for cluster in 2..total_entries {
        let entry = match fat_bits {
            12 => {
                let byte_off = (cluster as usize * 3) / 2;
                if byte_off + 1 >= fat_data.len() {
                    0
                } else {
                    let val = u16::from_le_bytes([fat_data[byte_off], fat_data[byte_off + 1]]);
                    if cluster & 1 == 1 { (val >> 4) as u32 } else { (val & 0x0FFF) as u32 }
                }
            }
            16 => {
                let off = cluster as usize * 2;
                if off + 1 >= fat_data.len() {
                    0
                } else {
                    u16::from_le_bytes([fat_data[off], fat_data[off + 1]]) as u32
                }
            }
            32 => {
                let off = cluster as usize * 4;
                if off + 3 >= fat_data.len() {
                    0
                } else {
                    u32::from_le_bytes([
                        fat_data[off], fat_data[off + 1],
                        fat_data[off + 2], fat_data[off + 3],
                    ]) & 0x0FFF_FFFF
                }
            }
            _ => 0,
        };
        if entry == 0 {
            free_count += 1;
        }
    }

    Ok(free_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_short_name() {
        assert_eq!(build_short_name(b"KERNEL  ", b"SYS"), "KERNEL.SYS");
        assert_eq!(build_short_name(b"README  ", b"TXT"), "README.TXT");
        assert_eq!(build_short_name(b"BOOTMGR ", b"   "), "BOOTMGR");
        assert_eq!(build_short_name(b"IO      ", b"SYS"), "IO.SYS");
    }

    #[test]
    fn test_build_short_name_cp437() {
        // CP437 byte 0xAB = ½, 0xAC = ¼
        assert_eq!(build_short_name(b"3\xABFLOP~1", b"LNK"), "3½FLOP~1.LNK");
        // CP437 byte 0x81 = ü
        assert_eq!(build_short_name(b"GR\x81\x81E   ", b"TXT"), "GRüüE.TXT");
        // CP437 byte 0x82 = é
        assert_eq!(build_short_name(b"CAF\x82    ", b"   "), "CAFé");
    }

    #[test]
    fn test_format_fat_datetime() {
        // 2026-01-29 14:30:00
        // Date: day=29, month=1, year=2026-1980=46
        let date = 29 | (1 << 5) | (46 << 9);
        // Time: second=0, minute=30, hour=14
        let time = 0 | (30 << 5) | (14 << 11);
        assert_eq!(format_fat_datetime(date, time), "2026-01-29 14:30:00");
    }

    #[test]
    fn test_format_fat_datetime_zero() {
        assert_eq!(format_fat_datetime(0, 0), "");
    }
}
