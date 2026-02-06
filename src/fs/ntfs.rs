use std::io::{self, Read, Seek, SeekFrom, Write};

use anyhow::{bail, Result};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{Filesystem, FilesystemError};

// Well-known MFT record numbers
const MFT_RECORD_VOLUME: u64 = 3;
const MFT_RECORD_ROOT: u64 = 5;
const MFT_RECORD_BITMAP: u64 = 6;

// Attribute type codes
const ATTR_VOLUME_NAME: u32 = 0x60;
const ATTR_VOLUME_INFORMATION: u32 = 0x70;
const ATTR_DATA: u32 = 0x80;
const ATTR_INDEX_ROOT: u32 = 0x90;
const ATTR_INDEX_ALLOCATION: u32 = 0xA0;
const ATTR_BITMAP: u32 = 0xB0;
const ATTR_END: u32 = 0xFFFF_FFFF;

// File attribute flags (from $FILE_NAME)
const FILE_ATTR_DIRECTORY: u32 = 0x1000_0000;

/// NTFS Volume Boot Record fields.
struct NtfsVbr {
    bytes_per_sector: u64,
    sectors_per_cluster: u64,
    total_sectors: u64,
    mft_cluster: u64,
    mft_mirror_cluster: u64,
    mft_record_size: u32,
}

fn parse_vbr(vbr: &[u8; 512]) -> Result<NtfsVbr, FilesystemError> {
    // Check OEM ID: "NTFS    " at offset 3
    if &vbr[3..11] != b"NTFS    " {
        return Err(FilesystemError::Parse("not an NTFS volume (OEM ID mismatch)".into()));
    }

    let bytes_per_sector = u16::from_le_bytes([vbr[0x0B], vbr[0x0C]]) as u64;
    if bytes_per_sector == 0 || !bytes_per_sector.is_power_of_two() || bytes_per_sector > 4096 {
        return Err(FilesystemError::Parse(format!(
            "invalid NTFS bytes per sector: {bytes_per_sector}"
        )));
    }

    let sectors_per_cluster = vbr[0x0D] as u64;
    if sectors_per_cluster == 0 {
        return Err(FilesystemError::Parse("invalid NTFS sectors per cluster: 0".into()));
    }

    let total_sectors = u64::from_le_bytes([
        vbr[0x28], vbr[0x29], vbr[0x2A], vbr[0x2B],
        vbr[0x2C], vbr[0x2D], vbr[0x2E], vbr[0x2F],
    ]);

    let mft_cluster = u64::from_le_bytes([
        vbr[0x30], vbr[0x31], vbr[0x32], vbr[0x33],
        vbr[0x34], vbr[0x35], vbr[0x36], vbr[0x37],
    ]);

    let mft_mirror_cluster = u64::from_le_bytes([
        vbr[0x38], vbr[0x39], vbr[0x3A], vbr[0x3B],
        vbr[0x3C], vbr[0x3D], vbr[0x3E], vbr[0x3F],
    ]);

    // Clusters per MFT record: if negative, record size = 2^|value| bytes
    let clusters_per_mft_raw = vbr[0x40] as i8;
    let mft_record_size = if clusters_per_mft_raw < 0 {
        1u32 << ((-clusters_per_mft_raw) as u32)
    } else {
        clusters_per_mft_raw as u32 * sectors_per_cluster as u32 * bytes_per_sector as u32
    };

    Ok(NtfsVbr {
        bytes_per_sector,
        sectors_per_cluster,
        total_sectors,
        mft_cluster,
        mft_mirror_cluster,
        mft_record_size,
    })
}

/// A parsed attribute from an MFT record.
#[derive(Debug, Clone)]
struct MftAttribute {
    attr_type: u32,
    resident: bool,
    /// For resident attributes, the raw value data.
    value: Vec<u8>,
    /// For non-resident attributes, the data runs.
    data_runs: Vec<DataRun>,
    /// For non-resident: real size of attribute data.
    real_size: u64,
    /// For non-resident: allocated size.
    #[allow(dead_code)]
    allocated_size: u64,
    /// For non-resident: starting VCN.
    #[allow(dead_code)]
    starting_vcn: u64,
}

/// A single data run (cluster offset, length in clusters).
#[derive(Debug, Clone)]
struct DataRun {
    /// Absolute cluster offset (cumulative from previous runs).
    cluster_offset: i64,
    /// Number of clusters in this run.
    length: u64,
}

/// Decode data runs from an MFT attribute's non-resident data.
fn decode_data_runs(data: &[u8]) -> Vec<DataRun> {
    let mut runs = Vec::new();
    let mut pos = 0;
    let mut prev_offset: i64 = 0;

    while pos < data.len() {
        let header = data[pos];
        if header == 0 {
            break;
        }
        pos += 1;

        let length_size = (header & 0x0F) as usize;
        let offset_size = ((header >> 4) & 0x0F) as usize;

        if length_size == 0 || pos + length_size + offset_size > data.len() {
            break;
        }

        // Read length (unsigned)
        let mut length: u64 = 0;
        for i in 0..length_size {
            length |= (data[pos + i] as u64) << (i * 8);
        }
        pos += length_size;

        // Read offset (signed, relative to previous)
        if offset_size == 0 {
            // Sparse run
            runs.push(DataRun {
                cluster_offset: 0,
                length,
            });
        } else {
            let mut offset: i64 = 0;
            for i in 0..offset_size {
                offset |= (data[pos + i] as i64) << (i * 8);
            }
            // Sign-extend
            if offset_size < 8 && (data[pos + offset_size - 1] & 0x80) != 0 {
                for i in offset_size..8 {
                    offset |= 0xFF_i64 << (i * 8);
                }
            }
            pos += offset_size;

            let abs_offset = prev_offset + offset;
            prev_offset = abs_offset;

            runs.push(DataRun {
                cluster_offset: abs_offset,
                length,
            });
        }
    }

    runs
}

/// Parse attributes from an MFT record (already fixup-applied).
fn parse_mft_attributes(record: &[u8], record_size: u32) -> Vec<MftAttribute> {
    let mut attrs = Vec::new();

    if record.len() < 24 {
        return attrs;
    }

    // First attribute offset
    let attr_offset = u16::from_le_bytes([record[0x14], record[0x15]]) as usize;
    let mut pos = attr_offset;

    while pos + 16 <= record.len() && pos < record_size as usize {
        let attr_type = u32::from_le_bytes([
            record[pos], record[pos + 1], record[pos + 2], record[pos + 3],
        ]);

        if attr_type == ATTR_END || attr_type == 0 {
            break;
        }

        let attr_len = u32::from_le_bytes([
            record[pos + 4], record[pos + 5], record[pos + 6], record[pos + 7],
        ]) as usize;

        if attr_len < 16 || pos + attr_len > record.len() {
            break;
        }

        let non_resident = record[pos + 8];

        if non_resident == 0 {
            // Resident attribute
            let value_length = u32::from_le_bytes([
                record[pos + 0x10], record[pos + 0x11], record[pos + 0x12], record[pos + 0x13],
            ]) as usize;
            let value_offset = u16::from_le_bytes([record[pos + 0x14], record[pos + 0x15]]) as usize;

            let value = if value_offset + value_length <= attr_len {
                record[pos + value_offset..pos + value_offset + value_length].to_vec()
            } else {
                Vec::new()
            };

            attrs.push(MftAttribute {
                attr_type,
                resident: true,
                value,
                data_runs: Vec::new(),
                real_size: value_length as u64,
                allocated_size: value_length as u64,
                starting_vcn: 0,
            });
        } else {
            // Non-resident attribute
            let starting_vcn = if pos + 0x18 <= record.len() {
                u64::from_le_bytes([
                    record[pos + 0x10], record[pos + 0x11], record[pos + 0x12], record[pos + 0x13],
                    record[pos + 0x14], record[pos + 0x15], record[pos + 0x16], record[pos + 0x17],
                ])
            } else {
                0
            };

            let real_size = if pos + 0x38 <= record.len() {
                u64::from_le_bytes([
                    record[pos + 0x30], record[pos + 0x31], record[pos + 0x32], record[pos + 0x33],
                    record[pos + 0x34], record[pos + 0x35], record[pos + 0x36], record[pos + 0x37],
                ])
            } else {
                0
            };

            let allocated_size = if pos + 0x30 <= record.len() {
                u64::from_le_bytes([
                    record[pos + 0x28], record[pos + 0x29], record[pos + 0x2A], record[pos + 0x2B],
                    record[pos + 0x2C], record[pos + 0x2D], record[pos + 0x2E], record[pos + 0x2F],
                ])
            } else {
                0
            };

            let run_offset = if pos + 0x22 <= record.len() {
                u16::from_le_bytes([record[pos + 0x20], record[pos + 0x21]]) as usize
            } else {
                0
            };

            let data_runs = if run_offset > 0 && pos + run_offset < pos + attr_len {
                decode_data_runs(&record[pos + run_offset..pos + attr_len])
            } else {
                Vec::new()
            };

            attrs.push(MftAttribute {
                attr_type,
                resident: true, // will be set to false below
                value: Vec::new(),
                data_runs,
                real_size,
                allocated_size,
                starting_vcn,
            });
            // Fix the resident flag
            if let Some(last) = attrs.last_mut() {
                last.resident = false;
            }
        }

        pos += attr_len;
    }

    attrs
}

/// Apply fixup array to an MFT record buffer.
fn apply_fixup(record: &mut [u8], bytes_per_sector: u64) -> Result<(), FilesystemError> {
    if record.len() < 48 {
        return Err(FilesystemError::Parse("MFT record too small for fixup".into()));
    }

    let fixup_offset = u16::from_le_bytes([record[0x04], record[0x05]]) as usize;
    let fixup_count = u16::from_le_bytes([record[0x06], record[0x07]]) as usize;

    if fixup_count < 2 || fixup_offset + fixup_count * 2 > record.len() {
        return Ok(()); // No fixup needed or invalid
    }

    let signature = u16::from_le_bytes([record[fixup_offset], record[fixup_offset + 1]]);

    for i in 1..fixup_count {
        let sector_end = i * bytes_per_sector as usize;
        if sector_end < 2 || sector_end > record.len() {
            break;
        }
        let pos = sector_end - 2;
        let stored = u16::from_le_bytes([record[pos], record[pos + 1]]);
        if stored != signature {
            return Err(FilesystemError::Parse(format!(
                "MFT fixup mismatch at sector {i}: expected {signature:#06x}, got {stored:#06x}"
            )));
        }
        let replace_offset = fixup_offset + i * 2;
        if replace_offset + 1 < record.len() {
            record[pos] = record[replace_offset];
            record[pos + 1] = record[replace_offset + 1];
        }
    }

    Ok(())
}

/// NTFS filesystem reader.
pub struct NtfsFilesystem<R> {
    reader: R,
    partition_offset: u64,
    bytes_per_sector: u64,
    #[allow(dead_code)]
    sectors_per_cluster: u64,
    total_sectors: u64,
    mft_cluster: u64,
    #[allow(dead_code)]
    mft_mirror_cluster: u64,
    mft_record_size: u32,
    cluster_size: u64,
    label: Option<String>,
    ntfs_version: (u8, u8),
    fs_type_string: String,
    used_bytes: u64,
}

impl<R: Read + Seek> NtfsFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        reader.seek(SeekFrom::Start(partition_offset))?;
        let mut vbr_buf = [0u8; 512];
        reader.read_exact(&mut vbr_buf)
            .map_err(|e| FilesystemError::Parse(format!("cannot read NTFS VBR: {e}")))?;

        let vbr = parse_vbr(&vbr_buf)?;
        let cluster_size = vbr.bytes_per_sector * vbr.sectors_per_cluster;

        let mut fs = NtfsFilesystem {
            reader,
            partition_offset,
            bytes_per_sector: vbr.bytes_per_sector,
            sectors_per_cluster: vbr.sectors_per_cluster,
            total_sectors: vbr.total_sectors,
            mft_cluster: vbr.mft_cluster,
            mft_mirror_cluster: vbr.mft_mirror_cluster,
            mft_record_size: vbr.mft_record_size,
            cluster_size,
            label: None,
            ntfs_version: (0, 0),
            fs_type_string: String::new(),
            used_bytes: 0,
        };

        // Read NTFS version from $Volume (MFT record #3)
        fs.ntfs_version = fs.read_ntfs_version().unwrap_or((0, 0));

        fs.fs_type_string = if fs.ntfs_version != (0, 0) {
            format!("NTFS {}.{}", fs.ntfs_version.0, fs.ntfs_version.1)
        } else {
            "NTFS".to_string()
        };

        // Read volume label from $Volume
        fs.label = fs.read_volume_label();

        // Read used size from $Bitmap
        fs.used_bytes = fs.calculate_used_bytes().unwrap_or(0);

        Ok(fs)
    }

    /// Absolute byte offset for a cluster number.
    fn cluster_offset(&self, cluster: u64) -> u64 {
        self.partition_offset + cluster * self.cluster_size
    }

    /// Read an MFT record by record number.
    fn read_mft_record(&mut self, record_number: u64) -> Result<Vec<u8>, FilesystemError> {
        let mft_offset = self.cluster_offset(self.mft_cluster);
        let record_offset = mft_offset + record_number * self.mft_record_size as u64;

        self.reader.seek(SeekFrom::Start(record_offset))?;
        let mut record = vec![0u8; self.mft_record_size as usize];
        self.reader.read_exact(&mut record)?;

        // Verify FILE magic
        if &record[0..4] != b"FILE" {
            return Err(FilesystemError::Parse(format!(
                "MFT record {record_number} has invalid magic: {:?}",
                &record[0..4]
            )));
        }

        apply_fixup(&mut record, self.bytes_per_sector)?;

        Ok(record)
    }

    /// Read the NTFS version from the $Volume MFT entry (record #3).
    fn read_ntfs_version(&mut self) -> Result<(u8, u8), FilesystemError> {
        let record = self.read_mft_record(MFT_RECORD_VOLUME)?;
        let attrs = parse_mft_attributes(&record, self.mft_record_size);

        for attr in &attrs {
            if attr.attr_type == ATTR_VOLUME_INFORMATION && attr.resident && attr.value.len() >= 10 {
                let major = attr.value[8];
                let minor = attr.value[9];
                return Ok((major, minor));
            }
        }

        Ok((0, 0))
    }

    /// Read the volume label from $Volume's $VOLUME_NAME attribute (0x60).
    fn read_volume_label(&mut self) -> Option<String> {
        let record = self.read_mft_record(MFT_RECORD_VOLUME).ok()?;
        let attrs = parse_mft_attributes(&record, self.mft_record_size);

        for attr in &attrs {
            if attr.attr_type == ATTR_VOLUME_NAME && attr.resident && !attr.value.is_empty() {
                // Value is UTF-16LE
                let len = attr.value.len() / 2;
                let chars: Vec<u16> = (0..len)
                    .map(|i| u16::from_le_bytes([attr.value[i * 2], attr.value[i * 2 + 1]]))
                    .collect();
                let label = String::from_utf16_lossy(&chars).trim().to_string();
                if label.is_empty() {
                    return None;
                }
                return Some(label);
            }
        }

        None
    }

    /// Read attribute data (handles both resident and non-resident).
    fn read_attribute_data(&mut self, attr: &MftAttribute, max_bytes: Option<u64>) -> Result<Vec<u8>, FilesystemError> {
        if attr.resident {
            let limit = max_bytes.map(|m| m as usize).unwrap_or(attr.value.len());
            Ok(attr.value[..limit.min(attr.value.len())].to_vec())
        } else {
            self.read_data_runs(&attr.data_runs, attr.real_size, max_bytes)
        }
    }

    /// Read data from data runs (non-resident attribute data).
    fn read_data_runs(
        &mut self,
        runs: &[DataRun],
        real_size: u64,
        max_bytes: Option<u64>,
    ) -> Result<Vec<u8>, FilesystemError> {
        let limit = max_bytes.unwrap_or(real_size).min(real_size);
        let mut data = Vec::with_capacity(limit as usize);

        for run in runs {
            if data.len() as u64 >= limit {
                break;
            }

            let run_bytes = run.length * self.cluster_size;
            let remaining = limit - data.len() as u64;
            let to_read = run_bytes.min(remaining);

            if run.cluster_offset == 0 {
                // Sparse run - fill with zeros
                data.resize(data.len() + to_read as usize, 0);
            } else {
                let offset = self.cluster_offset(run.cluster_offset as u64);
                self.reader.seek(SeekFrom::Start(offset))?;

                let mut buf = vec![0u8; to_read as usize];
                self.reader.read_exact(&mut buf)?;
                data.extend_from_slice(&buf);
            }
        }

        data.truncate(limit as usize);
        Ok(data)
    }

    /// Calculate used bytes by reading the $Bitmap (MFT record #6).
    fn calculate_used_bytes(&mut self) -> Result<u64, FilesystemError> {
        let record = self.read_mft_record(MFT_RECORD_BITMAP)?;
        let attrs = parse_mft_attributes(&record, self.mft_record_size);

        for attr in &attrs {
            if attr.attr_type == ATTR_DATA {
                let bitmap = self.read_attribute_data(attr, None)?;
                let used_clusters = count_set_bits(&bitmap);
                return Ok(used_clusters * self.cluster_size);
            }
        }

        Ok(0)
    }

    /// Find the highest used cluster by scanning $Bitmap backwards.
    fn find_last_used_cluster(&mut self) -> Result<u64, FilesystemError> {
        let record = self.read_mft_record(MFT_RECORD_BITMAP)?;
        let attrs = parse_mft_attributes(&record, self.mft_record_size);

        for attr in &attrs {
            if attr.attr_type == ATTR_DATA {
                let bitmap = self.read_attribute_data(attr, None)?;
                // Scan backwards for last set bit
                for byte_idx in (0..bitmap.len()).rev() {
                    if bitmap[byte_idx] != 0 {
                        // Find highest set bit in this byte
                        let byte = bitmap[byte_idx];
                        for bit in (0..8).rev() {
                            if byte & (1 << bit) != 0 {
                                return Ok(byte_idx as u64 * 8 + bit as u64);
                            }
                        }
                    }
                }
                return Ok(0);
            }
        }

        Err(FilesystemError::Parse("$Bitmap $DATA attribute not found".into()))
    }

    /// Parse index entries from $INDEX_ROOT and $INDEX_ALLOCATION to list directory contents.
    fn list_directory_entries(
        &mut self,
        record_number: u64,
        parent_path: &str,
    ) -> Result<Vec<FileEntry>, FilesystemError> {
        let record = self.read_mft_record(record_number)?;
        let attrs = parse_mft_attributes(&record, self.mft_record_size);

        let mut entries = Vec::new();

        // Parse $INDEX_ROOT (always resident)
        for attr in &attrs {
            if attr.attr_type == ATTR_INDEX_ROOT && attr.resident {
                self.parse_index_root_entries(&attr.value, parent_path, &mut entries)?;
            }
        }

        // Parse $INDEX_ALLOCATION (non-resident) if present
        let mut bitmap_data = Vec::new();
        for attr in &attrs {
            if attr.attr_type == ATTR_BITMAP && attr.resident {
                bitmap_data = attr.value.clone();
            } else if attr.attr_type == ATTR_BITMAP && !attr.resident {
                bitmap_data = self.read_attribute_data(attr, None)?;
            }
        }

        for attr in &attrs {
            if attr.attr_type == ATTR_INDEX_ALLOCATION && !attr.resident {
                self.parse_index_allocation_entries(attr, &bitmap_data, parent_path, &mut entries)?;
            }
        }

        Ok(entries)
    }

    /// Parse index entries from $INDEX_ROOT attribute value.
    fn parse_index_root_entries(
        &mut self,
        data: &[u8],
        parent_path: &str,
        entries: &mut Vec<FileEntry>,
    ) -> Result<(), FilesystemError> {
        if data.len() < 32 {
            return Ok(());
        }

        // Index root header: attribute type (4), collation rule (4), index allocation size (4),
        // clusters per index record (1), padding (3), then index node header
        let node_offset = 16; // Start of index node header within INDEX_ROOT value
        if node_offset + 16 > data.len() {
            return Ok(());
        }

        let entries_offset = u32::from_le_bytes([
            data[node_offset], data[node_offset + 1],
            data[node_offset + 2], data[node_offset + 3],
        ]) as usize;

        let entries_size = u32::from_le_bytes([
            data[node_offset + 4], data[node_offset + 5],
            data[node_offset + 6], data[node_offset + 7],
        ]) as usize;

        let start = node_offset + entries_offset;
        self.parse_index_entry_list(&data[start..data.len().min(node_offset + entries_size)], parent_path, entries)
    }

    /// Parse index entries from $INDEX_ALLOCATION (non-resident B+ tree nodes).
    fn parse_index_allocation_entries(
        &mut self,
        attr: &MftAttribute,
        _bitmap: &[u8],
        parent_path: &str,
        entries: &mut Vec<FileEntry>,
    ) -> Result<(), FilesystemError> {
        let data = self.read_attribute_data(attr, None)?;

        // Index allocation is a sequence of INDX records
        let record_size = 4096; // Standard NTFS index record size
        let mut pos = 0;

        while pos + record_size <= data.len() {
            let record = &data[pos..pos + record_size];

            // Check for INDX magic
            if &record[0..4] != b"INDX" {
                pos += record_size;
                continue;
            }

            // Apply fixup to a copy
            let mut record_copy = record.to_vec();
            let _ = apply_fixup(&mut record_copy, self.bytes_per_sector);

            // Index node header is at offset 0x18 in INDX record
            let node_offset = 0x18;
            if node_offset + 16 > record_copy.len() {
                pos += record_size;
                continue;
            }

            let entries_offset = u32::from_le_bytes([
                record_copy[node_offset], record_copy[node_offset + 1],
                record_copy[node_offset + 2], record_copy[node_offset + 3],
            ]) as usize;

            let entries_size = u32::from_le_bytes([
                record_copy[node_offset + 4], record_copy[node_offset + 5],
                record_copy[node_offset + 6], record_copy[node_offset + 7],
            ]) as usize;

            let start = node_offset + entries_offset;
            let end = (node_offset + entries_size).min(record_copy.len());
            if start < end {
                let _ = self.parse_index_entry_list(&record_copy[start..end], parent_path, entries);
            }

            pos += record_size;
        }

        Ok(())
    }

    /// Parse a list of index entries from raw bytes.
    fn parse_index_entry_list(
        &self,
        data: &[u8],
        parent_path: &str,
        entries: &mut Vec<FileEntry>,
    ) -> Result<(), FilesystemError> {
        let mut pos = 0;

        while pos + 16 <= data.len() {
            let entry_length = u16::from_le_bytes([data[pos + 8], data[pos + 9]]) as usize;
            let content_length = u16::from_le_bytes([data[pos + 10], data[pos + 11]]) as usize;
            let flags = u32::from_le_bytes([data[pos + 12], data[pos + 13], data[pos + 14], data[pos + 15]]);

            if entry_length < 16 || pos + entry_length > data.len() {
                break;
            }

            // Check for last entry flag (0x02)
            if flags & 0x02 != 0 {
                break;
            }

            // Parse $FILE_NAME content if present
            if content_length >= 66 {
                let content = &data[pos + 16..pos + 16 + content_length];
                if let Some(entry) = self.parse_file_name_entry(content, parent_path) {
                    // Skip . and .. entries and system metafiles
                    let mft_ref = u64::from_le_bytes([
                        data[pos], data[pos + 1], data[pos + 2], data[pos + 3],
                        data[pos + 4], data[pos + 5], 0, 0,
                    ]) & 0x0000_FFFF_FFFF_FFFF;

                    if mft_ref >= 24 || (mft_ref >= 11 && mft_ref < 24) {
                        entries.push(entry);
                    }
                }
            }

            pos += entry_length;
        }

        Ok(())
    }

    /// Parse a $FILE_NAME attribute into a FileEntry.
    fn parse_file_name_entry(&self, data: &[u8], parent_path: &str) -> Option<FileEntry> {
        if data.len() < 66 {
            return None;
        }

        let file_flags = u32::from_le_bytes([data[56], data[57], data[58], data[59]]);
        let real_size = u64::from_le_bytes([
            data[48], data[49], data[50], data[51],
            data[52], data[53], data[54], data[55],
        ]);
        let name_length = data[64] as usize;
        let name_type = data[65]; // 0=POSIX, 1=Win32, 2=DOS, 3=Win32+DOS

        // Skip DOS-only names (type 2) — prefer Win32 or Win32+DOS
        if name_type == 2 {
            return None;
        }

        if 66 + name_length * 2 > data.len() {
            return None;
        }

        // Decode UTF-16LE filename
        let name_chars: Vec<u16> = (0..name_length)
            .map(|i| u16::from_le_bytes([data[66 + i * 2], data[66 + i * 2 + 1]]))
            .collect();
        let name = String::from_utf16_lossy(&name_chars);

        // Skip . and .. and hidden system metafiles
        if name == "." || name == ".." {
            return None;
        }

        let is_dir = file_flags & FILE_ATTR_DIRECTORY != 0;
        let path = if parent_path == "/" {
            format!("/{name}")
        } else {
            format!("{parent_path}/{name}")
        };

        // Use parent MFT reference as location (for directory listing)
        let parent_ref = u64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], 0, 0,
        ]) & 0x0000_FFFF_FFFF_FFFF;

        if is_dir {
            Some(FileEntry::new_directory(name, path, parent_ref))
        } else {
            Some(FileEntry::new_file(name, path, real_size, parent_ref))
        }
    }
}

impl<R: Read + Seek + Send> Filesystem for NtfsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry {
            name: "/".into(),
            path: "/".into(),
            entry_type: EntryType::Directory,
            size: 0,
            location: MFT_RECORD_ROOT,
            modified: None,
        })
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if !entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }

        // For root, use MFT record #5. For subdirectories, we need to look up by MFT reference.
        let record_number = if entry.path == "/" {
            MFT_RECORD_ROOT
        } else {
            entry.location
        };

        self.list_directory_entries(record_number, &entry.path)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        if entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }

        // We need to find the file's MFT record to get its $DATA attribute.
        // The entry.location stores the parent MFT reference from the index entry.
        // For proper file reading, we'd need the file's own MFT reference.
        // Since index entries store the file's MFT reference at offset 0 of the index entry,
        // we stored it in location during directory listing.
        //
        // For now, we'll try to read from the stored location.
        // In the directory listing, we use the MFT reference from the index entry.
        let record_number = entry.location;

        let record = self.read_mft_record(record_number)?;
        let attrs = parse_mft_attributes(&record, self.mft_record_size);

        for attr in &attrs {
            if attr.attr_type == ATTR_DATA {
                return self.read_attribute_data(attr, Some(max_bytes as u64));
            }
        }

        Err(FilesystemError::NotFound(format!(
            "$DATA attribute not found for {}",
            entry.path
        )))
    }

    fn volume_label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    fn fs_type(&self) -> &str {
        &self.fs_type_string
    }

    fn total_size(&self) -> u64 {
        self.total_sectors * self.bytes_per_sector
    }

    fn used_size(&self) -> u64 {
        self.used_bytes
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        let last_cluster = self.find_last_used_cluster()?;
        if last_cluster == 0 {
            return Ok(self.total_size());
        }
        // Include the full cluster plus one sector for the backup boot sector
        let data_end = (last_cluster + 1) * self.cluster_size;
        // NTFS has a backup boot sector at the last sector
        let backup_boot = self.total_sectors * self.bytes_per_sector;
        Ok(data_end.max(backup_boot))
    }
}

/// Count set bits in a byte slice.
fn count_set_bits(data: &[u8]) -> u64 {
    data.iter().map(|&b| b.count_ones() as u64).sum()
}

// =============================================================================
// Compaction
// =============================================================================

/// Information about an NTFS compaction result.
pub struct CompactNtfsInfo {
    pub original_size: u64,
    pub compacted_size: u64,
    pub clusters_used: u32,
}

/// A reader that streams only the used clusters of an NTFS partition.
///
/// Layout: boot sector(s) | used clusters in order (skipping free ones)
pub struct CompactNtfsReader<R> {
    source: R,
    partition_offset: u64,
    cluster_size: u64,

    // Boot sector region (first cluster worth of data)
    boot_sectors: Vec<u8>,

    // Bitmap of which clusters are in use
    used_cluster_list: Vec<u64>,

    // Streaming state
    position: u64,
    total_size: u64,
    cluster_buf: Vec<u8>,
}

impl<R: Read + Seek> CompactNtfsReader<R> {
    pub fn new(
        mut source: R,
        partition_offset: u64,
    ) -> Result<(Self, CompactNtfsInfo), FilesystemError> {
        // Read VBR
        source.seek(SeekFrom::Start(partition_offset))?;
        let mut vbr_buf = [0u8; 512];
        source.read_exact(&mut vbr_buf)
            .map_err(|e| FilesystemError::Parse(format!("cannot read NTFS VBR: {e}")))?;

        let vbr = parse_vbr(&vbr_buf)?;
        let cluster_size = vbr.bytes_per_sector * vbr.sectors_per_cluster;
        let total_clusters = vbr.total_sectors / vbr.sectors_per_cluster;
        let original_size = vbr.total_sectors * vbr.bytes_per_sector;

        // Read boot sectors (one cluster worth)
        source.seek(SeekFrom::Start(partition_offset))?;
        let mut boot_sectors = vec![0u8; cluster_size as usize];
        source.read_exact(&mut boot_sectors)?;

        // Read $Bitmap to determine used clusters
        let mft_offset = partition_offset + vbr.mft_cluster * cluster_size;
        let bitmap_record_offset = mft_offset + MFT_RECORD_BITMAP * vbr.mft_record_size as u64;

        source.seek(SeekFrom::Start(bitmap_record_offset))?;
        let mut record = vec![0u8; vbr.mft_record_size as usize];
        source.read_exact(&mut record)?;

        if &record[0..4] != b"FILE" {
            return Err(FilesystemError::Parse("$Bitmap MFT record invalid".into()));
        }
        apply_fixup(&mut record, vbr.bytes_per_sector)?;

        let attrs = parse_mft_attributes(&record, vbr.mft_record_size);
        let mut bitmap_data = Vec::new();
        for attr in &attrs {
            if attr.attr_type == ATTR_DATA {
                if attr.resident {
                    bitmap_data = attr.value.clone();
                } else {
                    // Read bitmap from data runs
                    for run in &attr.data_runs {
                        if run.cluster_offset <= 0 {
                            bitmap_data.resize(bitmap_data.len() + (run.length * cluster_size) as usize, 0);
                            continue;
                        }
                        let run_offset = partition_offset + run.cluster_offset as u64 * cluster_size;
                        source.seek(SeekFrom::Start(run_offset))?;
                        let run_size = (run.length * cluster_size) as usize;
                        let old_len = bitmap_data.len();
                        bitmap_data.resize(old_len + run_size, 0);
                        source.read_exact(&mut bitmap_data[old_len..])?;
                    }
                    // Trim to real size
                    bitmap_data.truncate(attr.real_size as usize);
                }
                break;
            }
        }

        // Build list of used clusters
        let mut used_cluster_list = Vec::new();
        for (byte_idx, &byte) in bitmap_data.iter().enumerate() {
            for bit in 0..8 {
                if byte & (1 << bit) != 0 {
                    let cluster = byte_idx as u64 * 8 + bit as u64;
                    if cluster < total_clusters {
                        used_cluster_list.push(cluster);
                    }
                }
            }
        }

        let clusters_used = used_cluster_list.len() as u32;
        // Compacted size: boot region + used clusters
        let compacted_size = cluster_size + clusters_used as u64 * cluster_size;

        Ok((
            CompactNtfsReader {
                source,
                partition_offset,
                cluster_size,
                boot_sectors,
                used_cluster_list,
                position: 0,
                total_size: compacted_size,
                cluster_buf: Vec::new(),
            },
            CompactNtfsInfo {
                original_size,
                compacted_size,
                clusters_used,
            },
        ))
    }
}

impl<R: Read + Seek> Read for CompactNtfsReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.position >= self.total_size {
            return Ok(0);
        }

        let mut bytes_written = 0;

        while bytes_written < buf.len() && self.position < self.total_size {
            if self.position < self.cluster_size {
                // Reading from boot sector region
                let boot_pos = self.position as usize;
                let avail = self.boot_sectors.len() - boot_pos;
                let to_copy = avail.min(buf.len() - bytes_written);
                buf[bytes_written..bytes_written + to_copy]
                    .copy_from_slice(&self.boot_sectors[boot_pos..boot_pos + to_copy]);
                bytes_written += to_copy;
                self.position += to_copy as u64;
            } else {
                // Reading from used cluster data
                let data_pos = self.position - self.cluster_size;
                let cluster_idx = (data_pos / self.cluster_size) as usize;
                let within_cluster = (data_pos % self.cluster_size) as usize;

                if cluster_idx >= self.used_cluster_list.len() {
                    break;
                }

                // Read the cluster if we don't have it buffered
                if self.cluster_buf.is_empty() || within_cluster == 0 {
                    let src_cluster = self.used_cluster_list[cluster_idx];
                    let src_offset = self.partition_offset + src_cluster * self.cluster_size;
                    self.source.seek(SeekFrom::Start(src_offset))
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                    self.cluster_buf.resize(self.cluster_size as usize, 0);
                    self.source.read_exact(&mut self.cluster_buf)
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

/// Resize an NTFS partition in place by patching the VBR total sectors field.
///
/// This is a conservative approach: only the boot sector fields are patched.
/// The function rejects the resize if data extends beyond the new boundary.
pub fn resize_ntfs_in_place(
    file: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    new_total_sectors: u64,
    log_cb: &mut impl FnMut(&str),
) -> Result<bool> {
    // Read VBR
    file.seek(SeekFrom::Start(partition_offset))?;
    let mut vbr = [0u8; 512];
    file.read_exact(&mut vbr)?;

    // Validate NTFS magic
    if &vbr[3..11] != b"NTFS    " {
        return Ok(false);
    }

    let bytes_per_sector = u16::from_le_bytes([vbr[0x0B], vbr[0x0C]]) as u64;
    if bytes_per_sector == 0 || bytes_per_sector > 4096 {
        return Ok(false);
    }

    let sectors_per_cluster = vbr[0x0D] as u64;
    if sectors_per_cluster == 0 {
        return Ok(false);
    }

    let old_total = u64::from_le_bytes([
        vbr[0x28], vbr[0x29], vbr[0x2A], vbr[0x2B],
        vbr[0x2C], vbr[0x2D], vbr[0x2E], vbr[0x2F],
    ]);

    if old_total == new_total_sectors {
        return Ok(false);
    }

    let cluster_size = bytes_per_sector * sectors_per_cluster;

    // Check that data doesn't extend beyond new size by reading $Bitmap
    // We need to find the last used cluster
    let mft_cluster = u64::from_le_bytes([
        vbr[0x30], vbr[0x31], vbr[0x32], vbr[0x33],
        vbr[0x34], vbr[0x35], vbr[0x36], vbr[0x37],
    ]);

    let clusters_per_mft_raw = vbr[0x40] as i8;
    let mft_record_size = if clusters_per_mft_raw < 0 {
        1u32 << ((-clusters_per_mft_raw) as u32)
    } else {
        clusters_per_mft_raw as u32 * sectors_per_cluster as u32 * bytes_per_sector as u32
    };

    // Try to read $Bitmap to check last used cluster
    let mft_offset = partition_offset + mft_cluster * cluster_size;
    let bitmap_offset = mft_offset + MFT_RECORD_BITMAP * mft_record_size as u64;

    if let Ok(last_cluster) = read_last_used_cluster_from_bitmap(
        file,
        bitmap_offset,
        partition_offset,
        mft_record_size,
        bytes_per_sector,
        cluster_size,
    ) {
        let last_data_byte = (last_cluster + 1) * cluster_size;
        let new_size = new_total_sectors * bytes_per_sector;
        if last_data_byte > new_size {
            bail!(
                "NTFS resize rejected: data extends to byte {} but new size is {} bytes",
                last_data_byte,
                new_size
            );
        }
    }

    log_cb(&format!(
        "NTFS resize: {} -> {} total sectors",
        old_total, new_total_sectors
    ));

    // Patch total sectors in VBR
    vbr[0x28..0x30].copy_from_slice(&new_total_sectors.to_le_bytes());

    // Write patched VBR
    file.seek(SeekFrom::Start(partition_offset))?;
    file.write_all(&vbr)?;

    // Write backup boot sector at last sector of new partition
    let backup_offset = partition_offset + (new_total_sectors - 1) * bytes_per_sector;
    file.seek(SeekFrom::Start(backup_offset))?;
    file.write_all(&vbr)?;

    log_cb(&format!(
        "NTFS: patched VBR and backup boot sector (total sectors: {})",
        new_total_sectors
    ));

    Ok(true)
}

/// Helper to read the last used cluster from $Bitmap.
fn read_last_used_cluster_from_bitmap(
    file: &mut (impl Read + Seek),
    bitmap_record_offset: u64,
    partition_offset: u64,
    mft_record_size: u32,
    bytes_per_sector: u64,
    cluster_size: u64,
) -> Result<u64> {
    file.seek(SeekFrom::Start(bitmap_record_offset))?;
    let mut record = vec![0u8; mft_record_size as usize];
    file.read_exact(&mut record)?;

    if &record[0..4] != b"FILE" {
        bail!("$Bitmap MFT record invalid");
    }
    apply_fixup(&mut record, bytes_per_sector)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let attrs = parse_mft_attributes(&record, mft_record_size);
    for attr in &attrs {
        if attr.attr_type == ATTR_DATA {
            let bitmap = if attr.resident {
                attr.value.clone()
            } else {
                let mut data = Vec::new();
                for run in &attr.data_runs {
                    if run.cluster_offset <= 0 {
                        data.resize(data.len() + (run.length * cluster_size) as usize, 0);
                        continue;
                    }
                    let run_offset = partition_offset + run.cluster_offset as u64 * cluster_size;
                    file.seek(SeekFrom::Start(run_offset))?;
                    let run_size = (run.length * cluster_size) as usize;
                    let old_len = data.len();
                    data.resize(old_len + run_size, 0);
                    file.read_exact(&mut data[old_len..])?;
                }
                data.truncate(attr.real_size as usize);
                data
            };

            for byte_idx in (0..bitmap.len()).rev() {
                if bitmap[byte_idx] != 0 {
                    let byte = bitmap[byte_idx];
                    for bit in (0..8).rev() {
                        if byte & (1 << bit) != 0 {
                            return Ok(byte_idx as u64 * 8 + bit as u64);
                        }
                    }
                }
            }
            return Ok(0);
        }
    }

    bail!("$Bitmap $DATA attribute not found");
}

// =============================================================================
// Validation
// =============================================================================

/// Validate basic NTFS integrity.
pub fn validate_ntfs_integrity(
    file: &mut (impl Read + Seek),
    partition_offset: u64,
    log_cb: &mut impl FnMut(&str),
) -> Result<bool> {
    // Read VBR
    file.seek(SeekFrom::Start(partition_offset))?;
    let mut vbr = [0u8; 512];
    file.read_exact(&mut vbr)?;

    if &vbr[3..11] != b"NTFS    " {
        log_cb("NTFS validation: not an NTFS volume");
        return Ok(false);
    }

    let bytes_per_sector = u16::from_le_bytes([vbr[0x0B], vbr[0x0C]]) as u64;
    let sectors_per_cluster = vbr[0x0D] as u64;
    let cluster_size = bytes_per_sector * sectors_per_cluster;

    let mft_cluster = u64::from_le_bytes([
        vbr[0x30], vbr[0x31], vbr[0x32], vbr[0x33],
        vbr[0x34], vbr[0x35], vbr[0x36], vbr[0x37],
    ]);

    let clusters_per_mft_raw = vbr[0x40] as i8;
    let mft_record_size = if clusters_per_mft_raw < 0 {
        1u32 << ((-clusters_per_mft_raw) as u32)
    } else {
        clusters_per_mft_raw as u32 * sectors_per_cluster as u32 * bytes_per_sector as u32
    };

    // Verify MFT record #0 ($MFT) is readable
    let mft_offset = partition_offset + mft_cluster * cluster_size;
    file.seek(SeekFrom::Start(mft_offset))?;
    let mut record = vec![0u8; mft_record_size as usize];
    file.read_exact(&mut record)?;

    if &record[0..4] != b"FILE" {
        log_cb("NTFS validation: $MFT record has invalid magic");
        return Ok(false);
    }

    if let Err(e) = apply_fixup(&mut record, bytes_per_sector) {
        log_cb(&format!("NTFS validation: $MFT fixup failed: {e}"));
        return Ok(false);
    }

    log_cb("NTFS validation: VBR and $MFT record OK");
    Ok(true)
}

// =============================================================================
// Hidden Sectors Patching
// =============================================================================

/// Patch the hidden sectors field in the NTFS VBR.
///
/// NTFS stores hidden sectors at offset 0x1C as a u32 (same location as FAT).
pub fn patch_ntfs_hidden_sectors(
    file: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    start_lba: u64,
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    file.seek(SeekFrom::Start(partition_offset))?;
    let mut vbr = [0u8; 512];
    file.read_exact(&mut vbr)?;

    if &vbr[3..11] != b"NTFS    " {
        return Ok(());
    }

    let old_hidden = u32::from_le_bytes([vbr[0x1C], vbr[0x1D], vbr[0x1E], vbr[0x1F]]);
    let new_hidden = start_lba as u32;

    if old_hidden != new_hidden {
        vbr[0x1C..0x20].copy_from_slice(&new_hidden.to_le_bytes());

        // Write primary VBR
        file.seek(SeekFrom::Start(partition_offset))?;
        file.write_all(&vbr)?;

        // Write backup boot sector at last sector
        let total_sectors = u64::from_le_bytes([
            vbr[0x28], vbr[0x29], vbr[0x2A], vbr[0x2B],
            vbr[0x2C], vbr[0x2D], vbr[0x2E], vbr[0x2F],
        ]);
        let bytes_per_sector = u16::from_le_bytes([vbr[0x0B], vbr[0x0C]]) as u64;
        if total_sectors > 0 && bytes_per_sector > 0 {
            let backup_offset = partition_offset + (total_sectors - 1) * bytes_per_sector;
            file.seek(SeekFrom::Start(backup_offset))?;
            file.write_all(&vbr)?;
        }

        log_cb(&format!(
            "NTFS: patched hidden sectors {} -> {}",
            old_hidden, new_hidden
        ));
    }

    Ok(())
}

/// Check if a boot sector contains NTFS magic.
pub fn is_ntfs(boot_sector: &[u8]) -> bool {
    boot_sector.len() >= 11 && &boot_sector[3..11] == b"NTFS    "
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ntfs_vbr() -> [u8; 512] {
        let mut vbr = [0u8; 512];
        // Jump instruction
        vbr[0] = 0xEB;
        vbr[1] = 0x52;
        vbr[2] = 0x90;
        // OEM ID
        vbr[3..11].copy_from_slice(b"NTFS    ");
        // Bytes per sector = 512
        vbr[0x0B..0x0D].copy_from_slice(&512u16.to_le_bytes());
        // Sectors per cluster = 8
        vbr[0x0D] = 8;
        // Reserved sectors = 0
        vbr[0x0E..0x10].copy_from_slice(&0u16.to_le_bytes());
        // Total sectors = 102400
        vbr[0x28..0x30].copy_from_slice(&102400u64.to_le_bytes());
        // MFT cluster = 100
        vbr[0x30..0x38].copy_from_slice(&100u64.to_le_bytes());
        // MFT mirror cluster = 50
        vbr[0x38..0x40].copy_from_slice(&50u64.to_le_bytes());
        // Clusters per MFT record = -10 (2^10 = 1024 bytes)
        vbr[0x40] = (-10i8) as u8;
        // Serial number
        vbr[0x48..0x50].copy_from_slice(&0x1234567890ABCDEFu64.to_le_bytes());
        // Boot signature
        vbr[510] = 0x55;
        vbr[511] = 0xAA;
        vbr
    }

    #[test]
    fn test_parse_vbr_valid() {
        let vbr = make_ntfs_vbr();
        let parsed = parse_vbr(&vbr).unwrap();
        assert_eq!(parsed.bytes_per_sector, 512);
        assert_eq!(parsed.sectors_per_cluster, 8);
        assert_eq!(parsed.total_sectors, 102400);
        assert_eq!(parsed.mft_cluster, 100);
        assert_eq!(parsed.mft_mirror_cluster, 50);
        assert_eq!(parsed.mft_record_size, 1024);
    }

    #[test]
    fn test_parse_vbr_invalid_magic() {
        let mut vbr = make_ntfs_vbr();
        vbr[3..11].copy_from_slice(b"NOTNTFS!");
        assert!(parse_vbr(&vbr).is_err());
    }

    #[test]
    fn test_parse_vbr_zero_sector_size() {
        let mut vbr = make_ntfs_vbr();
        vbr[0x0B..0x0D].copy_from_slice(&0u16.to_le_bytes());
        assert!(parse_vbr(&vbr).is_err());
    }

    #[test]
    fn test_decode_data_runs_simple() {
        // Single run: 4 clusters starting at cluster 10
        // Header: 0x11 (1 byte length, 1 byte offset)
        // Length: 4, Offset: 10
        let data = [0x11, 0x04, 0x0A, 0x00];
        let runs = decode_data_runs(&data);
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].length, 4);
        assert_eq!(runs[0].cluster_offset, 10);
    }

    #[test]
    fn test_decode_data_runs_multiple() {
        // Two runs:
        // Run 1: 4 clusters at absolute offset 10
        // Run 2: 8 clusters at absolute offset 10 + 20 = 30
        let data = [
            0x11, 0x04, 0x0A, // run 1: len=4, offset=+10
            0x11, 0x08, 0x14, // run 2: len=8, offset=+20 (abs=30)
            0x00,              // end
        ];
        let runs = decode_data_runs(&data);
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0].length, 4);
        assert_eq!(runs[0].cluster_offset, 10);
        assert_eq!(runs[1].length, 8);
        assert_eq!(runs[1].cluster_offset, 30);
    }

    #[test]
    fn test_decode_data_runs_negative_offset() {
        // Two runs where second has negative relative offset
        let data = [
            0x11, 0x04, 0x20,       // run 1: len=4, offset=+32
            0x11, 0x04, 0xF0,       // run 2: len=4, offset=-16 (abs=32-16=16)
            0x00,
        ];
        let runs = decode_data_runs(&data);
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0].cluster_offset, 32);
        assert_eq!(runs[1].cluster_offset, 16);
    }

    #[test]
    fn test_is_ntfs() {
        let vbr = make_ntfs_vbr();
        assert!(is_ntfs(&vbr));
        assert!(!is_ntfs(&[0u8; 512]));
        assert!(!is_ntfs(&[0u8; 10]));
    }

    #[test]
    fn test_count_set_bits() {
        assert_eq!(count_set_bits(&[0xFF]), 8);
        assert_eq!(count_set_bits(&[0x00]), 0);
        assert_eq!(count_set_bits(&[0xAA]), 4); // 10101010
        assert_eq!(count_set_bits(&[0xFF, 0xFF]), 16);
        assert_eq!(count_set_bits(&[0x01, 0x80]), 2);
    }

    #[test]
    fn test_mft_record_size_negative() {
        // Clusters per MFT record = -10 means 2^10 = 1024 bytes
        let mut vbr = make_ntfs_vbr();
        vbr[0x40] = (-10i8) as u8;
        let parsed = parse_vbr(&vbr).unwrap();
        assert_eq!(parsed.mft_record_size, 1024);
    }

    #[test]
    fn test_mft_record_size_positive() {
        // Clusters per MFT record = 2, sectors_per_cluster = 8, bytes_per_sector = 512
        // -> 2 * 8 * 512 = 8192 bytes
        let mut vbr = make_ntfs_vbr();
        vbr[0x40] = 2;
        let parsed = parse_vbr(&vbr).unwrap();
        assert_eq!(parsed.mft_record_size, 8192);
    }
}
