use std::collections::HashMap;
use std::io::{self, Read, Seek, SeekFrom, Write};

use anyhow::{bail, Result};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};
use super::CompactResult;

// Well-known MFT record numbers
const MFT_RECORD_VOLUME: u64 = 3;
const MFT_RECORD_ROOT: u64 = 5;
const MFT_RECORD_BITMAP: u64 = 6;

// Attribute type codes
const ATTR_ATTRIBUTE_LIST: u32 = 0x20;
const ATTR_VOLUME_NAME: u32 = 0x60;
const ATTR_VOLUME_INFORMATION: u32 = 0x70;
pub(crate) const ATTR_DATA: u32 = 0x80;
const ATTR_INDEX_ROOT: u32 = 0x90;
const ATTR_INDEX_ALLOCATION: u32 = 0xA0;
const ATTR_BITMAP: u32 = 0xB0;
const ATTR_END: u32 = 0xFFFF_FFFF;

// Additional attribute type codes (for editing)
const ATTR_STANDARD_INFORMATION: u32 = 0x10;
const ATTR_FILE_NAME: u32 = 0x30;
const ATTR_SECURITY_DESCRIPTOR: u32 = 0x50;

// MFT record flags
const MFT_RECORD_IN_USE: u16 = 0x0001;
const MFT_RECORD_IS_DIRECTORY: u16 = 0x0002;

// Index entry flags
const INDEX_ENTRY_END: u32 = 0x02;

// File attribute flags (from $FILE_NAME)
const FILE_ATTR_DIRECTORY: u32 = 0x1000_0000;

/// NTFS Volume Boot Record fields.
#[derive(Clone)]
pub(crate) struct NtfsVbr {
    pub(crate) bytes_per_sector: u64,
    pub(crate) sectors_per_cluster: u64,
    pub(crate) total_sectors: u64,
    pub(crate) mft_cluster: u64,
    pub(crate) mft_mirror_cluster: u64,
    pub(crate) mft_record_size: u32,
}

pub(crate) fn parse_vbr(vbr: &[u8; 512]) -> Result<NtfsVbr, FilesystemError> {
    // Check OEM ID: "NTFS    " at offset 3
    if &vbr[3..11] != b"NTFS    " {
        return Err(FilesystemError::Parse(
            "not an NTFS volume (OEM ID mismatch)".into(),
        ));
    }

    let bytes_per_sector = u16::from_le_bytes([vbr[0x0B], vbr[0x0C]]) as u64;
    if bytes_per_sector == 0 || !bytes_per_sector.is_power_of_two() || bytes_per_sector > 4096 {
        return Err(FilesystemError::Parse(format!(
            "invalid NTFS bytes per sector: {bytes_per_sector}"
        )));
    }

    let sectors_per_cluster = vbr[0x0D] as u64;
    if sectors_per_cluster == 0 {
        return Err(FilesystemError::Parse(
            "invalid NTFS sectors per cluster: 0".into(),
        ));
    }

    let total_sectors = u64::from_le_bytes([
        vbr[0x28], vbr[0x29], vbr[0x2A], vbr[0x2B], vbr[0x2C], vbr[0x2D], vbr[0x2E], vbr[0x2F],
    ]);

    let mft_cluster = u64::from_le_bytes([
        vbr[0x30], vbr[0x31], vbr[0x32], vbr[0x33], vbr[0x34], vbr[0x35], vbr[0x36], vbr[0x37],
    ]);

    let mft_mirror_cluster = u64::from_le_bytes([
        vbr[0x38], vbr[0x39], vbr[0x3A], vbr[0x3B], vbr[0x3C], vbr[0x3D], vbr[0x3E], vbr[0x3F],
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
pub(crate) struct MftAttribute {
    pub(crate) attr_type: u32,
    pub(crate) resident: bool,
    /// For resident attributes, the raw value data.
    pub(crate) value: Vec<u8>,
    /// For non-resident attributes, the data runs.
    pub(crate) data_runs: Vec<DataRun>,
    /// For non-resident: real size of attribute data.
    pub(crate) real_size: u64,
    /// For non-resident: allocated size.
    #[allow(dead_code)]
    pub(crate) allocated_size: u64,
    /// For non-resident: starting VCN.
    #[allow(dead_code)]
    pub(crate) starting_vcn: u64,
}

/// A single data run (cluster offset, length in clusters).
#[derive(Debug, Clone)]
pub(crate) struct DataRun {
    /// Absolute cluster offset (cumulative from previous runs).
    pub(crate) cluster_offset: i64,
    /// Number of clusters in this run.
    pub(crate) length: u64,
}

/// Decode data runs from an MFT attribute's non-resident data.
pub(crate) fn decode_data_runs(data: &[u8]) -> Vec<DataRun> {
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
pub(crate) fn parse_mft_attributes(record: &[u8], record_size: u32) -> Vec<MftAttribute> {
    let mut attrs = Vec::new();

    if record.len() < 24 {
        return attrs;
    }

    // First attribute offset
    let attr_offset = u16::from_le_bytes([record[0x14], record[0x15]]) as usize;
    let mut pos = attr_offset;

    while pos + 16 <= record.len() && pos < record_size as usize {
        let attr_type = u32::from_le_bytes([
            record[pos],
            record[pos + 1],
            record[pos + 2],
            record[pos + 3],
        ]);

        if attr_type == ATTR_END || attr_type == 0 {
            break;
        }

        let attr_len = u32::from_le_bytes([
            record[pos + 4],
            record[pos + 5],
            record[pos + 6],
            record[pos + 7],
        ]) as usize;

        if attr_len < 16 || pos + attr_len > record.len() {
            break;
        }

        let non_resident = record[pos + 8];

        if non_resident == 0 {
            // Resident attribute
            let value_length = u32::from_le_bytes([
                record[pos + 0x10],
                record[pos + 0x11],
                record[pos + 0x12],
                record[pos + 0x13],
            ]) as usize;
            let value_offset =
                u16::from_le_bytes([record[pos + 0x14], record[pos + 0x15]]) as usize;

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
                    record[pos + 0x10],
                    record[pos + 0x11],
                    record[pos + 0x12],
                    record[pos + 0x13],
                    record[pos + 0x14],
                    record[pos + 0x15],
                    record[pos + 0x16],
                    record[pos + 0x17],
                ])
            } else {
                0
            };

            let real_size = if pos + 0x38 <= record.len() {
                u64::from_le_bytes([
                    record[pos + 0x30],
                    record[pos + 0x31],
                    record[pos + 0x32],
                    record[pos + 0x33],
                    record[pos + 0x34],
                    record[pos + 0x35],
                    record[pos + 0x36],
                    record[pos + 0x37],
                ])
            } else {
                0
            };

            let allocated_size = if pos + 0x30 <= record.len() {
                u64::from_le_bytes([
                    record[pos + 0x28],
                    record[pos + 0x29],
                    record[pos + 0x2A],
                    record[pos + 0x2B],
                    record[pos + 0x2C],
                    record[pos + 0x2D],
                    record[pos + 0x2E],
                    record[pos + 0x2F],
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
pub(crate) fn apply_fixup(record: &mut [u8], bytes_per_sector: u64) -> Result<(), FilesystemError> {
    if record.len() < 48 {
        return Err(FilesystemError::Parse(
            "MFT record too small for fixup".into(),
        ));
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
    mft_cache: HashMap<u64, Vec<u8>>,
    /// Data runs of the $MFT's own $DATA attribute. Empty until loaded in
    /// `open()`; when populated, record reads/writes resolve through these
    /// runs so a fragmented MFT is read correctly instead of assuming the
    /// whole table is one contiguous run starting at `mft_cluster`.
    mft_data_runs: Vec<DataRun>,
}

impl<R: Read + Seek> NtfsFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        reader.seek(SeekFrom::Start(partition_offset))?;
        let mut vbr_buf = [0u8; 512];
        reader
            .read_exact(&mut vbr_buf)
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
            mft_cache: HashMap::new(),
            mft_data_runs: Vec::new(),
        };

        // Load the $MFT's own data runs (record 0) so reads of high record
        // numbers follow the table across fragments. Record 0 always lives in
        // the first fragment at `mft_cluster`, so this initial read uses the
        // contiguous fallback (mft_data_runs is still empty here).
        fs.mft_data_runs = fs.read_mft_self_data_runs().unwrap_or_default();

        // Read NTFS version from $Volume (MFT record #3)
        fs.ntfs_version = fs.read_ntfs_version().unwrap_or((0, 0));

        fs.fs_type_string = if fs.ntfs_version != (0, 0) {
            format!("NTFS {}.{}", fs.ntfs_version.0, fs.ntfs_version.1)
        } else {
            "NTFS".to_string()
        };

        // Read volume label from $Volume
        fs.label = fs.read_volume_label();

        Ok(fs)
    }

    /// Absolute byte offset for a cluster number.
    fn cluster_offset(&self, cluster: u64) -> u64 {
        self.partition_offset + cluster * self.cluster_size
    }

    const MFT_CACHE_MAX: usize = 4096;

    /// Read an MFT record by record number, returning a cached copy when available.
    fn read_mft_record(&mut self, record_number: u64) -> Result<Vec<u8>, FilesystemError> {
        if let Some(cached) = self.mft_cache.get(&record_number) {
            return Ok(cached.clone());
        }

        let mut record = vec![0u8; self.mft_record_size as usize];
        let logical = record_number * self.mft_record_size as u64;
        if self.mft_data_runs.is_empty() {
            // Fallback: assume the MFT is one contiguous run. Used while
            // bootstrapping (reading record 0 before runs are loaded) and for
            // volumes whose $MFT $DATA runs we couldn't parse.
            let record_offset = self.cluster_offset(self.mft_cluster) + logical;
            self.reader.seek(SeekFrom::Start(record_offset))?;
            self.reader.read_exact(&mut record)?;
        } else {
            self.read_mft_bytes(logical, &mut record)?;
        }

        // Verify FILE magic
        if &record[0..4] != b"FILE" {
            return Err(FilesystemError::Parse(format!(
                "MFT record {record_number} has invalid magic: {:?}",
                &record[0..4]
            )));
        }

        apply_fixup(&mut record, self.bytes_per_sector)?;

        if self.mft_cache.len() >= Self::MFT_CACHE_MAX {
            self.mft_cache.clear();
        }
        self.mft_cache.insert(record_number, record.clone());

        Ok(record)
    }

    /// Parse the $MFT's own non-resident $DATA runs from record 0 so that
    /// record reads/writes can follow a fragmented MFT across the disk.
    fn read_mft_self_data_runs(&mut self) -> Result<Vec<DataRun>, FilesystemError> {
        let record = self.read_mft_record(0)?;
        let attrs = parse_mft_attributes(&record, self.mft_record_size);
        for attr in &attrs {
            if attr.attr_type == ATTR_DATA && !attr.resident && !attr.data_runs.is_empty() {
                return Ok(attr.data_runs.clone());
            }
        }
        Ok(Vec::new())
    }

    /// Read `buf.len()` bytes starting at logical byte offset `start` within
    /// the $MFT data stream, resolving each cluster through the MFT's data
    /// runs. Handles records that straddle a run boundary by reading one
    /// run-contiguous chunk at a time.
    fn read_mft_bytes(&mut self, start: u64, buf: &mut [u8]) -> Result<(), FilesystemError> {
        let runs = self.mft_data_runs.clone();
        let cluster_size = self.cluster_size;
        let mut filled = 0usize;
        while filled < buf.len() {
            let logical = start + filled as u64;
            let vcn = logical / cluster_size;
            let intra = (logical % cluster_size) as usize;
            let disk_off = self.resolve_vcn_to_offset(&runs, vcn).ok_or_else(|| {
                FilesystemError::Parse(format!(
                    "MFT logical offset {logical} (vcn {vcn}) not mapped by $MFT data runs"
                ))
            })?;
            let chunk = (cluster_size as usize - intra).min(buf.len() - filled);
            self.reader.seek(SeekFrom::Start(disk_off + intra as u64))?;
            self.reader.read_exact(&mut buf[filled..filled + chunk])?;
            filled += chunk;
        }
        Ok(())
    }

    /// Read the NTFS version from the $Volume MFT entry (record #3).
    fn read_ntfs_version(&mut self) -> Result<(u8, u8), FilesystemError> {
        let record = self.read_mft_record(MFT_RECORD_VOLUME)?;
        let attrs = parse_mft_attributes(&record, self.mft_record_size);

        for attr in &attrs {
            if attr.attr_type == ATTR_VOLUME_INFORMATION && attr.resident && attr.value.len() >= 10
            {
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

    /// Collect the complete unnamed `$DATA` for a file whose base record uses an
    /// `$ATTRIBUTE_LIST` to spill attributes into extension records (the case
    /// for heavily-fragmented files like large registry hives, whose runlist no
    /// longer fits in one MFT record).
    ///
    /// Returns `Ok(None)` when the base record has no `$ATTRIBUTE_LIST` — the
    /// caller then uses the base record's own `$DATA` as before. Otherwise walks
    /// the attribute list, gathers every unnamed `$DATA` fragment (matched by its
    /// `(holding-record, starting-VCN)`), reads each holding record, and
    /// concatenates the fragments' data runs in VCN order. The runs each carry
    /// absolute LCNs, so concatenation reconstructs the whole file. `real_size`
    /// comes from the `starting_vcn == 0` fragment (the only one that records it).
    fn collect_attrlist_data(
        &mut self,
        record_number: u64,
    ) -> Result<Option<(Vec<DataRun>, u64)>, FilesystemError> {
        let base = self.read_mft_record(record_number)?;
        let base_attrs = parse_mft_attributes(&base, self.mft_record_size);

        let mut attrlist: Option<Vec<u8>> = None;
        for a in &base_attrs {
            if a.attr_type == ATTR_ATTRIBUTE_LIST {
                attrlist = Some(if a.resident {
                    a.value.clone()
                } else {
                    self.read_attribute_data(a, None)?
                });
                break;
            }
        }
        let Some(al) = attrlist else {
            return Ok(None);
        };

        // Walk attribute-list entries; collect unnamed $DATA fragments as
        // (holding mft record, starting VCN). Entry layout: type(4) len(2)
        // name_len(1) name_off(1) start_vcn(8) base_ref(8) attr_id(2) [name].
        let mut frags: Vec<(u64, u64)> = Vec::new();
        let mut p = 0usize;
        while p + 0x1A <= al.len() {
            let atype = u32::from_le_bytes([al[p], al[p + 1], al[p + 2], al[p + 3]]);
            let elen = u16::from_le_bytes([al[p + 4], al[p + 5]]) as usize;
            if elen < 0x1A || p + elen > al.len() {
                break;
            }
            let name_len = al[p + 6];
            if atype == ATTR_DATA && name_len == 0 {
                let svcn = u64::from_le_bytes(al[p + 8..p + 16].try_into().unwrap());
                let mref = u64::from_le_bytes(al[p + 0x10..p + 0x18].try_into().unwrap())
                    & 0xFFFF_FFFF_FFFF;
                frags.push((mref, svcn));
            }
            p += elen;
        }
        if frags.is_empty() {
            return Ok(None);
        }
        frags.sort_by_key(|&(_, svcn)| svcn);
        frags.dedup();

        let mut runs: Vec<DataRun> = Vec::new();
        let mut real_size = 0u64;
        for (mref, svcn) in &frags {
            let rec = if *mref == record_number {
                base.clone()
            } else {
                self.read_mft_record(*mref)?
            };
            let attrs = parse_mft_attributes(&rec, self.mft_record_size);
            for a in &attrs {
                if a.attr_type == ATTR_DATA && !a.resident && a.starting_vcn == *svcn {
                    if *svcn == 0 {
                        real_size = a.real_size;
                    }
                    runs.extend(a.data_runs.iter().cloned());
                    break;
                }
            }
        }
        if runs.is_empty() {
            return Ok(None);
        }
        Ok(Some((runs, real_size)))
    }

    /// Read attribute data (handles both resident and non-resident).
    fn read_attribute_data(
        &mut self,
        attr: &MftAttribute,
        max_bytes: Option<u64>,
    ) -> Result<Vec<u8>, FilesystemError> {
        if attr.resident {
            let limit = max_bytes.map(|m| m as usize).unwrap_or(attr.value.len());
            Ok(attr.value[..limit.min(attr.value.len())].to_vec())
        } else {
            self.read_data_runs(&attr.data_runs, attr.real_size, max_bytes)
        }
    }

    /// Stream attribute data to a writer up to `max_bytes`. Avoids the full
    /// allocation in `read_attribute_data`; used by `write_file_to`.
    fn write_attribute_data_to(
        &mut self,
        attr: &MftAttribute,
        writer: &mut dyn std::io::Write,
        max_bytes: u64,
    ) -> Result<u64, FilesystemError> {
        if attr.resident {
            let n = (attr.value.len() as u64).min(max_bytes) as usize;
            writer.write_all(&attr.value[..n])?;
            Ok(n as u64)
        } else {
            self.write_data_runs_to(&attr.data_runs, attr.real_size, writer, max_bytes)
        }
    }

    /// Stream non-resident data runs to a writer.
    fn write_data_runs_to(
        &mut self,
        runs: &[DataRun],
        real_size: u64,
        writer: &mut dyn std::io::Write,
        max_bytes: u64,
    ) -> Result<u64, FilesystemError> {
        let limit = max_bytes.min(real_size);
        let mut written: u64 = 0;
        let zeros = vec![0u8; 64 * 1024];

        for run in runs {
            if written >= limit {
                break;
            }
            let run_bytes = run.length * self.cluster_size;
            let remaining = limit - written;
            let to_write = run_bytes.min(remaining);

            if run.cluster_offset == 0 {
                // Sparse run — emit zeros in chunks.
                let mut left = to_write;
                while left > 0 {
                    let n = (zeros.len() as u64).min(left) as usize;
                    writer.write_all(&zeros[..n])?;
                    left -= n as u64;
                }
            } else {
                let offset = self.cluster_offset(run.cluster_offset as u64);
                self.reader.seek(SeekFrom::Start(offset))?;
                // Stream this run in 64 KiB chunks to avoid a per-run allocation
                // for very large runs.
                let mut buf = vec![0u8; 64 * 1024];
                let mut left = to_write;
                while left > 0 {
                    let n = (buf.len() as u64).min(left) as usize;
                    self.reader.read_exact(&mut buf[..n])?;
                    writer.write_all(&buf[..n])?;
                    left -= n as u64;
                }
            }
            written += to_write;
        }
        Ok(written)
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

    /// Populate `used_bytes` from $Bitmap if not already computed.
    pub fn ensure_used_bytes(&mut self) {
        if self.used_bytes == 0 {
            self.used_bytes = self.calculate_used_bytes().unwrap_or(0);
        }
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

        Err(FilesystemError::Parse(
            "$Bitmap $DATA attribute not found".into(),
        ))
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
            data[node_offset],
            data[node_offset + 1],
            data[node_offset + 2],
            data[node_offset + 3],
        ]) as usize;

        let entries_size = u32::from_le_bytes([
            data[node_offset + 4],
            data[node_offset + 5],
            data[node_offset + 6],
            data[node_offset + 7],
        ]) as usize;

        let start = node_offset + entries_offset;
        self.parse_index_entry_list(
            &data[start..data.len().min(node_offset + entries_size)],
            parent_path,
            entries,
        )
    }

    /// Resolve a VCN (virtual cluster number) within an attribute's data runs
    /// to an absolute byte offset on disk. Returns `None` for sparse runs.
    fn resolve_vcn_to_offset(&self, runs: &[DataRun], vcn: u64) -> Option<u64> {
        let mut run_vcn: u64 = 0;
        for run in runs {
            let run_end = run_vcn + run.length;
            if vcn >= run_vcn && vcn < run_end {
                if run.cluster_offset == 0 {
                    return None;
                }
                let offset_in_run = vcn - run_vcn;
                return Some(self.cluster_offset((run.cluster_offset as u64) + offset_in_run));
            }
            run_vcn = run_end;
        }
        None
    }

    /// Parse index entries from $INDEX_ALLOCATION by reading one INDX record at
    /// a time via VCN lookup, skipping records the bitmap marks as unused.
    fn parse_index_allocation_entries(
        &mut self,
        attr: &MftAttribute,
        bitmap: &[u8],
        parent_path: &str,
        entries: &mut Vec<FileEntry>,
    ) -> Result<(), FilesystemError> {
        let record_size: u64 = 4096;
        let clusters_per_record = record_size.div_ceil(self.cluster_size);
        let total_records = if attr.real_size > 0 {
            attr.real_size / record_size
        } else {
            attr.allocated_size / record_size
        };

        let runs = attr.data_runs.clone();

        for i in 0..total_records {
            if !bitmap.is_empty() {
                let byte_idx = i as usize / 8;
                let bit_idx = i as usize % 8;
                if byte_idx < bitmap.len() && bitmap[byte_idx] & (1 << bit_idx) == 0 {
                    continue;
                }
            }

            let vcn = i * clusters_per_record;
            let disk_offset = match self.resolve_vcn_to_offset(&runs, vcn) {
                Some(off) => off,
                None => continue,
            };

            self.reader.seek(SeekFrom::Start(disk_offset))?;
            let mut record_buf = vec![0u8; record_size as usize];
            if self.reader.read_exact(&mut record_buf).is_err() {
                continue;
            }

            if &record_buf[0..4] != b"INDX" {
                continue;
            }

            if apply_fixup(&mut record_buf, self.bytes_per_sector).is_err() {
                continue;
            }

            let node_offset = 0x18;
            if node_offset + 16 > record_buf.len() {
                continue;
            }

            let entries_offset = u32::from_le_bytes([
                record_buf[node_offset],
                record_buf[node_offset + 1],
                record_buf[node_offset + 2],
                record_buf[node_offset + 3],
            ]) as usize;

            let entries_size = u32::from_le_bytes([
                record_buf[node_offset + 4],
                record_buf[node_offset + 5],
                record_buf[node_offset + 6],
                record_buf[node_offset + 7],
            ]) as usize;

            let start = node_offset + entries_offset;
            let end = (node_offset + entries_size).min(record_buf.len());
            if start < end {
                let _ = self.parse_index_entry_list(&record_buf[start..end], parent_path, entries);
            }
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
            let flags = u32::from_le_bytes([
                data[pos + 12],
                data[pos + 13],
                data[pos + 14],
                data[pos + 15],
            ]);

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
                // The file's own MFT reference is at the start of the index entry
                let mft_ref = u64::from_le_bytes([
                    data[pos],
                    data[pos + 1],
                    data[pos + 2],
                    data[pos + 3],
                    data[pos + 4],
                    data[pos + 5],
                    0,
                    0,
                ]) & 0x0000_FFFF_FFFF_FFFF;

                if let Some(entry) = self.parse_file_name_entry(content, parent_path, mft_ref) {
                    // NTFS reserves MFT records 0-23 for system metafiles
                    // ($MFT, $Bitmap, $Boot, $Extend, $ObjId, ...). These are
                    // filesystem metadata, not user files (Windows hides them),
                    // and the reserved $Extend children have no unnamed $DATA
                    // attribute, so reading them as files fails. User files
                    // always start at record 24, so only push those.
                    if mft_ref >= 24 {
                        entries.push(entry);
                    }
                }
            }

            pos += entry_length;
        }

        Ok(())
    }

    /// Parse a $FILE_NAME attribute into a FileEntry.
    /// `file_mft_ref` is the file's own MFT record number (from the index entry).
    fn parse_file_name_entry(
        &self,
        data: &[u8],
        parent_path: &str,
        file_mft_ref: u64,
    ) -> Option<FileEntry> {
        if data.len() < 66 {
            return None;
        }

        let file_flags = u32::from_le_bytes([data[56], data[57], data[58], data[59]]);
        let real_size = u64::from_le_bytes([
            data[48], data[49], data[50], data[51], data[52], data[53], data[54], data[55],
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

        if is_dir {
            Some(FileEntry::new_directory(name, path, file_mft_ref))
        } else {
            Some(FileEntry::new_file(name, path, real_size, file_mft_ref))
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
            type_code: None,
            creator_code: None,
            symlink_target: None,
            special_type: None,
            mode: None,
            uid: None,
            gid: None,
            resource_fork_size: None,
            aux_type: None,
            link_target_cnid: None,
            amiga_protection: None,
            amiga_comment: None,
            amiga_date: None,
            dos_attributes: None,
            mac_dates: None,
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

        // A non-resident $DATA in the base record is complete only when there's
        // no $ATTRIBUTE_LIST spilling the rest into extension records. Prefer the
        // attribute-list collection when present so fragmented files (large
        // registry hives, etc.) read in full.
        if let Some((runs, real_size)) = self.collect_attrlist_data(record_number)? {
            return self.read_data_runs(&runs, real_size, Some(max_bytes as u64));
        }

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

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn std::io::Write,
    ) -> Result<u64, FilesystemError> {
        if entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        let record = self.read_mft_record(entry.location)?;
        let attrs = parse_mft_attributes(&record, self.mft_record_size);
        // Follow $ATTRIBUTE_LIST first (see read_file) so fragmented files whose
        // runlist spilled into extension records stream in full.
        if let Some((runs, real_size)) = self.collect_attrlist_data(entry.location)? {
            let cap = if entry.size > 0 {
                entry.size
            } else {
                real_size
            };
            return self.write_data_runs_to(&runs, real_size, writer, cap);
        }
        for attr in &attrs {
            if attr.attr_type == ATTR_DATA {
                return self.write_attribute_data_to(attr, writer, entry.size);
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

    fn validate_name(&self, name: &str) -> Result<(), FilesystemError> {
        validate_ntfs_name(name)
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
// Data Run Encoding
// =============================================================================

/// Encode data runs as NTFS variable-length mapping pairs.
///
/// Each entry is (absolute_cluster, length_in_clusters). Encodes as delta offsets.
fn encode_data_runs(runs: &[(u64, u64)]) -> Vec<u8> {
    let mut result = Vec::new();
    let mut prev_offset: i64 = 0;

    for &(abs_cluster, length) in runs {
        let delta = abs_cluster as i64 - prev_offset;
        prev_offset = abs_cluster as i64;

        // Calculate minimum bytes needed for length (unsigned)
        let length_size = min_unsigned_bytes(length);
        // Calculate minimum bytes needed for offset (signed)
        let offset_size = min_signed_bytes(delta);

        let header = (offset_size as u8) << 4 | (length_size as u8);
        result.push(header);

        // Write length (little-endian, unsigned)
        for i in 0..length_size {
            result.push((length >> (i * 8)) as u8);
        }

        // Write offset (little-endian, signed)
        for i in 0..offset_size {
            result.push((delta >> (i * 8)) as u8);
        }
    }

    result.push(0x00); // terminator
    result
}

/// Minimum bytes to represent an unsigned value.
fn min_unsigned_bytes(val: u64) -> usize {
    if val == 0 {
        return 1;
    }
    let bits = 64 - val.leading_zeros() as usize;
    bits.div_ceil(8)
}

/// Minimum bytes to represent a signed value.
fn min_signed_bytes(val: i64) -> usize {
    if val == 0 {
        return 1;
    }
    if val > 0 {
        // Need enough bytes + sign bit must be 0
        let bits = 64 - val.leading_zeros() as usize;
        (bits + 8) / 8 // +1 for sign bit, round up
    } else {
        // Need enough bytes + sign bit must be 1
        let bits = 64 - ((!val) as u64).leading_zeros() as usize;
        (bits + 8) / 8
    }
}

// =============================================================================
// Editing Helpers
// =============================================================================

/// Fixed NTFS timestamp for new files (2024-01-01 00:00:00 UTC).
/// 100-nanosecond intervals since 1601-01-01.
const FIXED_NTFS_TIMESTAMP: u64 = 133_480_416_000_000_000;

/// Validate an NTFS filename.
fn validate_ntfs_name(name: &str) -> Result<(), FilesystemError> {
    if name.is_empty() {
        return Err(FilesystemError::InvalidData(
            "filename is empty — pick a non-blank name".into(),
        ));
    }
    let char_count = name.chars().count();
    if char_count > 255 {
        return Err(FilesystemError::InvalidData(format!(
            "filename is too long ({char_count} chars); NTFS allows up to 255 — shorten the name"
        )));
    }
    const FORBIDDEN: &[char] = &['"', '*', '/', ':', '<', '>', '?', '\\', '|'];
    for c in name.chars() {
        if FORBIDDEN.contains(&c) {
            return Err(FilesystemError::InvalidData(format!(
                "filename contains '{c}', which NTFS does not allow \
                 (forbidden: \" * / : < > ? \\ |) — rename the file"
            )));
        }
        if (c as u32) < 0x20 {
            return Err(FilesystemError::InvalidData(format!(
                "filename contains a control character (U+{:04X}); \
                 NTFS disallows control codes — rename the file",
                c as u32
            )));
        }
    }
    Ok(())
}

/// Prepare fixup array for writing an MFT record (inverse of apply_fixup).
fn prepare_fixup(record: &mut [u8], bytes_per_sector: u64) {
    let fixup_offset = u16::from_le_bytes([record[0x04], record[0x05]]) as usize;
    let fixup_count = u16::from_le_bytes([record[0x06], record[0x07]]) as usize;

    if fixup_count < 2 || fixup_offset + fixup_count * 2 > record.len() {
        return;
    }

    // Increment the update sequence number
    let usn = u16::from_le_bytes([record[fixup_offset], record[fixup_offset + 1]]);
    let new_usn = usn.wrapping_add(1).max(1); // avoid 0
    record[fixup_offset] = new_usn as u8;
    record[fixup_offset + 1] = (new_usn >> 8) as u8;

    // For each sector, save the real last-2-bytes into the fixup array slot,
    // then write the USN at the sector end
    for i in 1..fixup_count {
        let sector_end = i * bytes_per_sector as usize;
        if sector_end < 2 || sector_end > record.len() {
            break;
        }
        let pos = sector_end - 2;
        let slot_offset = fixup_offset + i * 2;
        if slot_offset + 1 >= record.len() {
            break;
        }
        // Save original bytes to fixup array
        record[slot_offset] = record[pos];
        record[slot_offset + 1] = record[pos + 1];
        // Write USN at sector end
        record[pos] = new_usn as u8;
        record[pos + 1] = (new_usn >> 8) as u8;
    }
}

/// Build a $STANDARD_INFORMATION attribute value (48 bytes).
fn build_standard_information() -> Vec<u8> {
    let mut data = vec![0u8; 48];
    let ts = FIXED_NTFS_TIMESTAMP.to_le_bytes();
    data[0..8].copy_from_slice(&ts); // creation time
    data[8..16].copy_from_slice(&ts); // modification time
    data[16..24].copy_from_slice(&ts); // MFT modification time
    data[24..32].copy_from_slice(&ts); // access time
                                       // flags at offset 32 = 0 (normal)
    data
}

/// Build a $FILE_NAME attribute value.
fn build_file_name_attr(parent_ref: u64, name: &str, is_dir: bool, size: u64) -> Vec<u8> {
    let utf16: Vec<u16> = name.encode_utf16().collect();
    let name_bytes = utf16.len() * 2;
    let data_len = 66 + name_bytes;
    let mut data = vec![0u8; data_len];

    // Parent MFT reference (6 bytes ref + 2 bytes sequence number = 0)
    data[0..8].copy_from_slice(&parent_ref.to_le_bytes());

    let ts = FIXED_NTFS_TIMESTAMP.to_le_bytes();
    data[8..16].copy_from_slice(&ts); // creation
    data[16..24].copy_from_slice(&ts); // modification
    data[24..32].copy_from_slice(&ts); // MFT modification
    data[32..40].copy_from_slice(&ts); // access

    // allocated size
    data[40..48].copy_from_slice(&size.to_le_bytes());
    // real size
    data[48..56].copy_from_slice(&size.to_le_bytes());

    // flags
    let flags: u32 = if is_dir { FILE_ATTR_DIRECTORY } else { 0 };
    data[56..60].copy_from_slice(&flags.to_le_bytes());

    // reparse = 0 (bytes 60..64 already zero)

    // name length
    data[64] = utf16.len() as u8;
    // namespace = 0x03 (Win32+DOS)
    data[65] = 0x03;

    // UTF-16LE name
    for (i, &ch) in utf16.iter().enumerate() {
        data[66 + i * 2] = ch as u8;
        data[66 + i * 2 + 1] = (ch >> 8) as u8;
    }

    data
}

/// Build a minimal security descriptor granting Everyone:FullControl.
fn build_default_security_descriptor() -> Vec<u8> {
    // Self-relative SD with DACL, owner=Everyone SID, group=Everyone SID
    // Everyone SID = S-1-1-0 = 01 01 00 00 00 00 00 01 00 00 00 00
    let everyone_sid: [u8; 12] = [
        0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
    ];
    // ACL with single ACE: Everyone:FullControl
    // ACL header: revision(1)=2, padding(1)=0, size(2), ace_count(2), padding(2)
    let ace_size: u16 = 4 + 4 + 12; // ACE header + mask + SID
    let acl_size: u16 = 8 + ace_size;
    let mut acl = vec![0u8; acl_size as usize];
    acl[0] = 0x02; // revision
    acl[2..4].copy_from_slice(&acl_size.to_le_bytes());
    acl[4..6].copy_from_slice(&1u16.to_le_bytes()); // ace count
                                                    // ACE: type=0 (ACCESS_ALLOWED), flags=0, size, mask=0x1F01FF (full control)
    let ace_start = 8;
    acl[ace_start] = 0x00; // type
    acl[ace_start + 1] = 0x00; // flags
    acl[ace_start + 2..ace_start + 4].copy_from_slice(&ace_size.to_le_bytes());
    acl[ace_start + 4..ace_start + 8].copy_from_slice(&0x001F01FFu32.to_le_bytes());
    acl[ace_start + 8..ace_start + 8 + 12].copy_from_slice(&everyone_sid);

    // Security descriptor header (self-relative):
    // revision=1, padding=0, control=0x8004 (SE_SELF_RELATIVE | SE_DACL_PRESENT)
    // owner offset, group offset, SACL offset=0, DACL offset
    let header_size = 20u32;
    let owner_offset = header_size;
    let group_offset = owner_offset + 12;
    let dacl_offset = group_offset + 12;
    let total = dacl_offset as usize + acl.len();

    let mut sd = vec![0u8; total];
    sd[0] = 0x01; // revision
    sd[2..4].copy_from_slice(&0x8004u16.to_le_bytes()); // control
    sd[4..8].copy_from_slice(&owner_offset.to_le_bytes());
    sd[8..12].copy_from_slice(&group_offset.to_le_bytes());
    // SACL offset = 0 (none)
    sd[16..20].copy_from_slice(&dacl_offset.to_le_bytes());
    sd[owner_offset as usize..owner_offset as usize + 12].copy_from_slice(&everyone_sid);
    sd[group_offset as usize..group_offset as usize + 12].copy_from_slice(&everyone_sid);
    sd[dacl_offset as usize..].copy_from_slice(&acl);

    sd
}

/// Build an empty $INDEX_ROOT attribute value for a new directory.
fn build_empty_index_root() -> Vec<u8> {
    // Index root header (16 bytes):
    // attr_type=0x30 ($FILE_NAME), collation=0x01 (filename),
    // index_alloc_size=4096, clusters_per_index=1
    // Then index node header + end sentinel
    let end_entry_size = 16u32; // minimal end entry
    let entries_total = 0x10 + end_entry_size; // node header (16) + entries

    let mut data = vec![0u8; 16 + entries_total as usize];
    // Index root header
    data[0..4].copy_from_slice(&ATTR_FILE_NAME.to_le_bytes()); // indexed attr type
    data[4..8].copy_from_slice(&1u32.to_le_bytes()); // collation rule
    data[8..12].copy_from_slice(&4096u32.to_le_bytes()); // index alloc size
    data[12] = 1; // clusters per index record

    // Index node header (at offset 16)
    let node = 16;
    data[node..node + 4].copy_from_slice(&0x10u32.to_le_bytes()); // entries offset
    data[node + 4..node + 8].copy_from_slice(&(0x10 + end_entry_size).to_le_bytes()); // total size of entries
    data[node + 8..node + 12].copy_from_slice(&(0x10 + end_entry_size).to_le_bytes()); // allocated size
                                                                                       // flags = 0 (small index, no children)

    // End sentinel entry (at node + 0x10)
    let entry = node + 0x10;
    // MFT ref = 0 (bytes 0-7 already zero)
    data[entry + 8..entry + 10].copy_from_slice(&16u16.to_le_bytes()); // entry length
                                                                       // content length = 0
    data[entry + 12..entry + 16].copy_from_slice(&INDEX_ENTRY_END.to_le_bytes()); // flags

    data
}

/// Build a resident attribute header + data, padded to 8-byte alignment.
fn build_resident_attr(attr_type: u32, data: &[u8]) -> Vec<u8> {
    let value_offset = 0x18u16;
    let total = (value_offset as usize + data.len() + 7) & !7; // 8-byte aligned
    let mut attr = vec![0u8; total];
    attr[0..4].copy_from_slice(&attr_type.to_le_bytes());
    attr[4..8].copy_from_slice(&(total as u32).to_le_bytes());
    // non-resident flag = 0 (resident)
    // name_len = 0, name_offset = 0, flags = 0
    attr[0x10..0x14].copy_from_slice(&(data.len() as u32).to_le_bytes()); // value length
    attr[0x14..0x16].copy_from_slice(&value_offset.to_le_bytes());
    attr[value_offset as usize..value_offset as usize + data.len()].copy_from_slice(data);
    attr
}

/// Replace a resident attribute's whole blob in place, shifting following
/// attributes to accommodate a different length and updating the record's
/// bytes-in-use. Unlike [`HfsFilesystem`]-style rebuilds this preserves the
/// record header (sequence number, hard-link count, flags), so it is safe to
/// use on records from a real volume. Returns `DiskFull` if the grown record
/// would overflow `record`.
fn replace_resident_attr(
    record: &mut [u8],
    target_type: u32,
    new_attr: &[u8],
) -> Result<(), FilesystemError> {
    let attr_offset = u16::from_le_bytes([record[0x14], record[0x15]]) as usize;
    let used =
        u32::from_le_bytes([record[0x18], record[0x19], record[0x1A], record[0x1B]]) as usize;
    let mut pos = attr_offset;
    while pos + 16 <= record.len() {
        let attr_type = u32::from_le_bytes([
            record[pos],
            record[pos + 1],
            record[pos + 2],
            record[pos + 3],
        ]);
        if attr_type == ATTR_END || attr_type == 0 {
            break;
        }
        let attr_len = u32::from_le_bytes([
            record[pos + 4],
            record[pos + 5],
            record[pos + 6],
            record[pos + 7],
        ]) as usize;
        if attr_len < 16 || pos + attr_len > record.len() {
            break;
        }
        // Resident attribute of the requested type (residency flag at +8 == 0).
        if attr_type == target_type && record[pos + 8] == 0 {
            let tail_start = pos + attr_len;
            if tail_start > used || used > record.len() {
                return Err(FilesystemError::InvalidData(
                    "corrupt MFT record during rename".into(),
                ));
            }
            let tail: Vec<u8> = record[tail_start..used].to_vec();
            let new_used = pos + new_attr.len() + tail.len();
            if new_used > record.len() {
                return Err(FilesystemError::DiskFull(
                    "MFT record full after rename".into(),
                ));
            }
            record[pos..pos + new_attr.len()].copy_from_slice(new_attr);
            record[pos + new_attr.len()..new_used].copy_from_slice(&tail);
            if new_used < used {
                for b in &mut record[new_used..used] {
                    *b = 0;
                }
            }
            record[0x18..0x1C].copy_from_slice(&(new_used as u32).to_le_bytes());
            return Ok(());
        }
        pos += attr_len;
    }
    Err(FilesystemError::NotFound(format!(
        "resident attribute 0x{target_type:X} not found in record"
    )))
}

/// Build a non-resident attribute header + data runs, padded to 8-byte alignment.
fn build_nonresident_attr(attr_type: u32, runs: &[(u64, u64)], real_size: u64) -> Vec<u8> {
    let encoded = encode_data_runs(runs);
    let run_offset = 0x40u16;
    let total = (run_offset as usize + encoded.len() + 7) & !7;
    let mut attr = vec![0u8; total];

    attr[0..4].copy_from_slice(&attr_type.to_le_bytes());
    attr[4..8].copy_from_slice(&(total as u32).to_le_bytes());
    attr[8] = 1; // non-resident

    // start VCN = 0 (offset 0x10)
    // end VCN (offset 0x18)
    let total_clusters: u64 = runs.iter().map(|(_, l)| l).sum();
    if total_clusters > 0 {
        attr[0x18..0x20].copy_from_slice(&(total_clusters - 1).to_le_bytes());
    }

    attr[0x20..0x22].copy_from_slice(&run_offset.to_le_bytes());

    // compression unit = 0 (offset 0x22)
    // allocated size (offset 0x28) — cluster-aligned
    let cluster_size_placeholder = real_size; // will be corrected by caller if needed
    attr[0x28..0x30].copy_from_slice(&cluster_size_placeholder.to_le_bytes());
    attr[0x30..0x38].copy_from_slice(&real_size.to_le_bytes()); // real size
    attr[0x38..0x40].copy_from_slice(&real_size.to_le_bytes()); // initialized size

    attr[run_offset as usize..run_offset as usize + encoded.len()].copy_from_slice(&encoded);
    attr
}

/// Assemble a complete MFT record from attribute blobs.
fn assemble_mft_record(attrs: &[Vec<u8>], flags: u16, record_size: u32) -> Vec<u8> {
    let mut record = vec![0u8; record_size as usize];

    // FILE magic
    record[0..4].copy_from_slice(b"FILE");
    // Fixup offset = 0x30
    record[0x04..0x06].copy_from_slice(&0x0030u16.to_le_bytes());
    // Fixup count = 3 (for 1024-byte record with 512-byte sectors: 1 USN + 2 entries)
    let fixup_count = (record_size / 512 + 1) as u16;
    record[0x06..0x08].copy_from_slice(&fixup_count.to_le_bytes());
    // Log file sequence = 0 (offset 0x08..0x10)
    // Sequence number = 1 (offset 0x10..0x12)
    record[0x10..0x12].copy_from_slice(&1u16.to_le_bytes());
    // Hard link count = 1 (offset 0x12..0x14)
    record[0x12..0x14].copy_from_slice(&1u16.to_le_bytes());
    // First attribute offset = 0x38 (after fixup array)
    let first_attr = 0x30 + fixup_count as usize * 2;
    let first_attr_aligned = (first_attr + 7) & !7;
    record[0x14..0x16].copy_from_slice(&(first_attr_aligned as u16).to_le_bytes());
    // Flags
    record[0x16..0x18].copy_from_slice(&flags.to_le_bytes());
    // Allocated size
    record[0x1C..0x20].copy_from_slice(&record_size.to_le_bytes());
    // Next attribute ID (offset 0x28) — count of attrs
    record[0x28..0x2A].copy_from_slice(&(attrs.len() as u16).to_le_bytes());

    // Write attributes
    let mut pos = first_attr_aligned;
    for attr in attrs {
        if pos + attr.len() + 4 > record_size as usize {
            break; // shouldn't happen if record_size is adequate
        }
        record[pos..pos + attr.len()].copy_from_slice(attr);
        pos += attr.len();
    }

    // End marker
    if pos + 4 <= record_size as usize {
        record[pos..pos + 4].copy_from_slice(&ATTR_END.to_le_bytes());
        pos += 4;
    }

    // Used size
    record[0x18..0x1C].copy_from_slice(&(pos as u32).to_le_bytes());

    // Initialize fixup array with default USN
    let fixup_off = 0x30usize;
    record[fixup_off] = 0x01; // USN = 1
    record[fixup_off + 1] = 0x00;
    // Fixup entries (slots) — will be filled by prepare_fixup

    record
}

/// Build an index entry for insertion into a directory index.
fn build_index_entry(child_mft_ref: u64, child_seq: u16, file_name_attr: &[u8]) -> Vec<u8> {
    // MFT reference: low 6 bytes = record number, high 2 bytes = sequence number
    let mft_ref_bytes = (child_mft_ref & 0x0000_FFFF_FFFF_FFFF) | ((child_seq as u64) << 48);

    let content_len = file_name_attr.len() as u16;
    let entry_len = ((16 + content_len as usize + 7) & !7) as u16; // 8-byte aligned

    let mut entry = vec![0u8; entry_len as usize];
    entry[0..8].copy_from_slice(&mft_ref_bytes.to_le_bytes());
    entry[8..10].copy_from_slice(&entry_len.to_le_bytes());
    entry[10..12].copy_from_slice(&content_len.to_le_bytes());
    // flags = 0 (no sub-node)
    entry[16..16 + file_name_attr.len()].copy_from_slice(file_name_attr);

    entry
}

// =============================================================================
// Editing Methods on NtfsFilesystem
// =============================================================================

impl<R: Read + Write + Seek> NtfsFilesystem<R> {
    /// Write an MFT record back to disk with fixup applied.
    fn write_mft_record(
        &mut self,
        record_number: u64,
        record: &mut [u8],
    ) -> Result<(), FilesystemError> {
        prepare_fixup(record, self.bytes_per_sector);
        let logical = record_number * self.mft_record_size as u64;
        if self.mft_data_runs.is_empty() {
            let record_offset = self.cluster_offset(self.mft_cluster) + logical;
            self.reader.seek(SeekFrom::Start(record_offset))?;
            self.reader.write_all(record)?;
        } else {
            self.write_mft_bytes(logical, record)?;
        }
        self.mft_cache.remove(&record_number);
        Ok(())
    }

    /// Write `buf` to the $MFT data stream starting at logical byte offset
    /// `start`, following the MFT's data runs across fragments.
    fn write_mft_bytes(&mut self, start: u64, buf: &[u8]) -> Result<(), FilesystemError> {
        let runs = self.mft_data_runs.clone();
        let cluster_size = self.cluster_size;
        let mut written = 0usize;
        while written < buf.len() {
            let logical = start + written as u64;
            let vcn = logical / cluster_size;
            let intra = (logical % cluster_size) as usize;
            let disk_off = self.resolve_vcn_to_offset(&runs, vcn).ok_or_else(|| {
                FilesystemError::Parse(format!(
                    "MFT logical offset {logical} (vcn {vcn}) not mapped by $MFT data runs"
                ))
            })?;
            let chunk = (cluster_size as usize - intra).min(buf.len() - written);
            self.reader.seek(SeekFrom::Start(disk_off + intra as u64))?;
            self.reader.write_all(&buf[written..written + chunk])?;
            written += chunk;
        }
        Ok(())
    }

    /// Read the MFT bitmap ($MFT record 0's $BITMAP attribute).
    fn read_mft_bitmap(&mut self) -> Result<Vec<u8>, FilesystemError> {
        let record = self.read_mft_record(0)?;
        let attrs = parse_mft_attributes(&record, self.mft_record_size);
        for attr in &attrs {
            if attr.attr_type == ATTR_BITMAP {
                return self.read_attribute_data(attr, None);
            }
        }
        Err(FilesystemError::Parse(
            "$MFT $BITMAP attribute not found".into(),
        ))
    }

    /// Write MFT bitmap data back through $MFT's $BITMAP attribute runs.
    fn write_mft_bitmap(&mut self, bitmap: &[u8]) -> Result<(), FilesystemError> {
        let record = self.read_mft_record(0)?;
        let attrs = parse_mft_attributes(&record, self.mft_record_size);
        for attr in &attrs {
            if attr.attr_type == ATTR_BITMAP {
                if attr.resident {
                    // Resident bitmap — need to write back into MFT record 0
                    // Find the attribute position and update in-place
                    let mut rec = record.clone();
                    self.update_resident_attr_value(&mut rec, ATTR_BITMAP, bitmap)?;
                    self.write_mft_record(0, &mut rec)?;
                    return Ok(());
                } else {
                    return self.write_data_to_runs(&attr.data_runs, bitmap);
                }
            }
        }
        Err(FilesystemError::Parse(
            "$MFT $BITMAP attribute not found".into(),
        ))
    }

    /// Update a resident attribute's value in a raw MFT record buffer.
    fn update_resident_attr_value(
        &self,
        record: &mut [u8],
        target_type: u32,
        new_value: &[u8],
    ) -> Result<(), FilesystemError> {
        let attr_offset = u16::from_le_bytes([record[0x14], record[0x15]]) as usize;
        let mut pos = attr_offset;

        while pos + 16 <= record.len() {
            let attr_type = u32::from_le_bytes([
                record[pos],
                record[pos + 1],
                record[pos + 2],
                record[pos + 3],
            ]);
            if attr_type == ATTR_END || attr_type == 0 {
                break;
            }
            let attr_len = u32::from_le_bytes([
                record[pos + 4],
                record[pos + 5],
                record[pos + 6],
                record[pos + 7],
            ]) as usize;
            if attr_len < 16 || pos + attr_len > record.len() {
                break;
            }

            if attr_type == target_type && record[pos + 8] == 0 {
                // Resident — check it fits
                let value_offset =
                    u16::from_le_bytes([record[pos + 0x14], record[pos + 0x15]]) as usize;
                let old_value_len = u32::from_le_bytes([
                    record[pos + 0x10],
                    record[pos + 0x11],
                    record[pos + 0x12],
                    record[pos + 0x13],
                ]) as usize;
                if new_value.len() <= old_value_len {
                    // Write new value (same size or smaller)
                    record[pos + value_offset..pos + value_offset + new_value.len()]
                        .copy_from_slice(new_value);
                    // Update length
                    record[pos + 0x10..pos + 0x14]
                        .copy_from_slice(&(new_value.len() as u32).to_le_bytes());
                    return Ok(());
                }
                return Err(FilesystemError::DiskFull(
                    "new attribute value too large for resident slot".into(),
                ));
            }
            pos += attr_len;
        }
        Err(FilesystemError::NotFound(format!(
            "resident attribute 0x{target_type:X} not found in record"
        )))
    }

    /// Write data to disk through data runs.
    fn write_data_to_runs(&mut self, runs: &[DataRun], data: &[u8]) -> Result<(), FilesystemError> {
        let mut written = 0usize;
        for run in runs {
            if written >= data.len() {
                break;
            }
            if run.cluster_offset <= 0 {
                // Sparse — skip
                written += (run.length * self.cluster_size) as usize;
                continue;
            }
            let offset = self.cluster_offset(run.cluster_offset as u64);
            let run_bytes = (run.length * self.cluster_size) as usize;
            let to_write = run_bytes.min(data.len() - written);
            self.reader.seek(SeekFrom::Start(offset))?;
            self.reader.write_all(&data[written..written + to_write])?;
            written += to_write;
        }
        Ok(())
    }

    /// Allocate an MFT record. Returns the record number.
    fn allocate_mft_record(&mut self) -> Result<u64, FilesystemError> {
        let mut bitmap = self.read_mft_bitmap()?;

        // Find first free bit starting from record 24 (skip system metafiles)
        for byte_idx in 3..bitmap.len() {
            // byte 3 = records 24-31
            if bitmap[byte_idx] != 0xFF {
                for bit in 0..8u8 {
                    if bitmap[byte_idx] & (1 << bit) == 0 {
                        let record_num = byte_idx as u64 * 8 + bit as u64;
                        // Set bit
                        bitmap[byte_idx] |= 1 << bit;
                        self.write_mft_bitmap(&bitmap)?;

                        // Initialize blank MFT record
                        let mut blank = vec![0u8; self.mft_record_size as usize];
                        blank[0..4].copy_from_slice(b"FILE");
                        blank[0x04..0x06].copy_from_slice(&0x0030u16.to_le_bytes());
                        let fixup_count =
                            (self.mft_record_size / self.bytes_per_sector as u32 + 1) as u16;
                        blank[0x06..0x08].copy_from_slice(&fixup_count.to_le_bytes());
                        blank[0x10..0x12].copy_from_slice(&1u16.to_le_bytes()); // seq = 1
                        let first_attr = (0x30 + fixup_count as usize * 2 + 7) & !7;
                        blank[0x14..0x16].copy_from_slice(&(first_attr as u16).to_le_bytes());
                        blank[0x18..0x1C].copy_from_slice(&((first_attr + 4) as u32).to_le_bytes()); // used size
                        blank[0x1C..0x20].copy_from_slice(&self.mft_record_size.to_le_bytes());
                        // End marker
                        blank[first_attr..first_attr + 4].copy_from_slice(&ATTR_END.to_le_bytes());

                        self.write_mft_record(record_num, &mut blank)?;
                        return Ok(record_num);
                    }
                }
            }
        }

        Err(FilesystemError::DiskFull(
            "no free MFT records available".into(),
        ))
    }

    /// Free an MFT record.
    fn free_mft_record(&mut self, record_number: u64) -> Result<(), FilesystemError> {
        let mut bitmap = self.read_mft_bitmap()?;
        let byte_idx = (record_number / 8) as usize;
        let bit = (record_number % 8) as u8;
        if byte_idx < bitmap.len() {
            bitmap[byte_idx] &= !(1 << bit);
            self.write_mft_bitmap(&bitmap)?;
        }

        // Mark record as not-in-use
        let mut record = self
            .read_mft_record(record_number)
            .unwrap_or_else(|_| vec![0u8; self.mft_record_size as usize]);
        if &record[0..4] == b"FILE" {
            record[0x16] = 0;
            record[0x17] = 0;
            self.write_mft_record(record_number, &mut record)?;
        }

        Ok(())
    }

    /// Read the volume bitmap ($Bitmap, MFT record #6).
    fn read_volume_bitmap(&mut self) -> Result<(Vec<u8>, Vec<DataRun>), FilesystemError> {
        let record = self.read_mft_record(MFT_RECORD_BITMAP)?;
        let attrs = parse_mft_attributes(&record, self.mft_record_size);
        for attr in &attrs {
            if attr.attr_type == ATTR_DATA {
                let data = self.read_attribute_data(attr, None)?;
                return Ok((data, attr.data_runs.clone()));
            }
        }
        Err(FilesystemError::Parse(
            "$Bitmap $DATA attribute not found".into(),
        ))
    }

    /// Write volume bitmap back through its data runs.
    fn write_volume_bitmap(
        &mut self,
        bitmap: &[u8],
        runs: &[DataRun],
    ) -> Result<(), FilesystemError> {
        self.write_data_to_runs(runs, bitmap)
    }

    /// Allocate contiguous-ish volume clusters. Returns list of (start_cluster, length).
    fn allocate_volume_clusters(&mut self, count: u32) -> Result<Vec<(u64, u64)>, FilesystemError> {
        let (mut bitmap, runs) = self.read_volume_bitmap()?;
        let total_bits = bitmap.len() * 8;

        let mut allocated = Vec::new();
        let mut remaining = count as u64;
        let mut run_start: Option<u64> = None;
        let mut run_len: u64 = 0;

        for cluster in 0..total_bits as u64 {
            if remaining == 0 {
                break;
            }
            let byte_idx = (cluster / 8) as usize;
            let bit = (cluster % 8) as u8;
            if bitmap[byte_idx] & (1 << bit) == 0 {
                // Free cluster
                bitmap[byte_idx] |= 1 << bit;
                remaining -= 1;

                match run_start {
                    Some(start) if cluster == start + run_len => {
                        run_len += 1;
                    }
                    _ => {
                        if let Some(start) = run_start {
                            allocated.push((start, run_len));
                        }
                        run_start = Some(cluster);
                        run_len = 1;
                    }
                }
            }
        }

        if let Some(start) = run_start {
            allocated.push((start, run_len));
        }

        if remaining > 0 {
            return Err(FilesystemError::DiskFull(
                "not enough free clusters on volume".into(),
            ));
        }

        self.write_volume_bitmap(&bitmap, &runs)?;
        Ok(allocated)
    }

    /// Free volume clusters.
    fn free_volume_clusters(&mut self, runs: &[(u64, u64)]) -> Result<(), FilesystemError> {
        let (mut bitmap, bitmap_runs) = self.read_volume_bitmap()?;
        for &(start, length) in runs {
            for cluster in start..start + length {
                let byte_idx = (cluster / 8) as usize;
                let bit = (cluster % 8) as u8;
                if byte_idx < bitmap.len() {
                    bitmap[byte_idx] &= !(1 << bit);
                }
            }
        }
        self.write_volume_bitmap(&bitmap, &bitmap_runs)
    }

    /// Count free volume clusters.
    fn count_free_volume_clusters(&mut self) -> Result<u64, FilesystemError> {
        let (bitmap, _) = self.read_volume_bitmap()?;
        let total_bits = bitmap.len() as u64 * 8;
        let set_bits = count_set_bits(&bitmap);
        Ok(total_bits - set_bits)
    }

    /// Read parent directory's security descriptor, or build a default one.
    /// If the parent's SD is too large to fit as a resident attribute, uses a minimal default.
    fn read_parent_security_descriptor(
        &mut self,
        parent_record_num: u64,
    ) -> Result<Vec<u8>, FilesystemError> {
        // Max SD size that will fit as a resident attr in a 1024-byte record
        // alongside other attributes (leave ~600 bytes for other attrs + header)
        let max_sd_size = (self.mft_record_size as usize).saturating_sub(600);

        if let Ok(record) = self.read_mft_record(parent_record_num) {
            let attrs = parse_mft_attributes(&record, self.mft_record_size);
            for attr in &attrs {
                if attr.attr_type == ATTR_SECURITY_DESCRIPTOR {
                    let sd = self.read_attribute_data(attr, None)?;
                    if sd.len() <= max_sd_size {
                        return Ok(sd);
                    }
                    // Parent SD too large, fall through to default
                    break;
                }
            }
        }
        Ok(build_default_security_descriptor())
    }

    /// Check if a name exists in a directory's index.
    fn name_exists_in_index(
        &mut self,
        parent_record_num: u64,
        name: &str,
    ) -> Result<bool, FilesystemError> {
        let entries = self.list_directory_entries(parent_record_num, "/")?;
        let name_lower = name.to_lowercase();
        Ok(entries.iter().any(|e| e.name.to_lowercase() == name_lower))
    }

    /// Insert an index entry into a directory's $INDEX_ROOT.
    /// Falls back to $INDEX_ALLOCATION if $INDEX_ROOT is full.
    fn insert_index_entry(
        &mut self,
        parent_record_num: u64,
        entry_bytes: &[u8],
    ) -> Result<(), FilesystemError> {
        let mut record = self.read_mft_record(parent_record_num)?;
        let record_size = self.mft_record_size;

        // Try inserting into $INDEX_ROOT first
        if self.try_insert_into_index_root(&mut record, entry_bytes, record_size)? {
            self.write_mft_record(parent_record_num, &mut record)?;
            return Ok(());
        }

        // Try inserting into existing $INDEX_ALLOCATION INDX nodes
        let attrs = parse_mft_attributes(&record, record_size);
        for attr in &attrs {
            if attr.attr_type == ATTR_INDEX_ALLOCATION && !attr.resident {
                let mut alloc_data = self.read_attribute_data(attr, None)?;
                if self.try_insert_into_index_allocation(&mut alloc_data, entry_bytes)? {
                    self.write_data_to_runs(&attr.data_runs, &alloc_data)?;
                    return Ok(());
                }
            }
        }

        Err(FilesystemError::DiskFull(
            "directory index full, no room in existing nodes".into(),
        ))
    }

    /// Try to insert an index entry into $INDEX_ROOT. Returns true if successful.
    fn try_insert_into_index_root(
        &self,
        record: &mut [u8],
        entry_bytes: &[u8],
        record_size: u32,
    ) -> Result<bool, FilesystemError> {
        // Find $INDEX_ROOT attribute in the record
        let attr_offset = u16::from_le_bytes([record[0x14], record[0x15]]) as usize;
        let mut pos = attr_offset;

        while pos + 16 <= record.len() {
            let attr_type = u32::from_le_bytes([
                record[pos],
                record[pos + 1],
                record[pos + 2],
                record[pos + 3],
            ]);
            if attr_type == ATTR_END || attr_type == 0 {
                break;
            }
            let attr_len = u32::from_le_bytes([
                record[pos + 4],
                record[pos + 5],
                record[pos + 6],
                record[pos + 7],
            ]) as usize;
            if attr_len < 16 || pos + attr_len > record.len() {
                break;
            }

            if attr_type == ATTR_INDEX_ROOT && record[pos + 8] == 0 {
                // Found resident $INDEX_ROOT
                let value_offset =
                    u16::from_le_bytes([record[pos + 0x14], record[pos + 0x15]]) as usize;
                let value_length = u32::from_le_bytes([
                    record[pos + 0x10],
                    record[pos + 0x11],
                    record[pos + 0x12],
                    record[pos + 0x13],
                ]) as usize;

                let ir_start = pos + value_offset; // start of INDEX_ROOT value
                if ir_start + 32 > record.len() || value_length < 32 {
                    return Ok(false);
                }

                // Index node header is at ir_start + 16
                let node_start = ir_start + 16;
                let entries_offset = u32::from_le_bytes([
                    record[node_start],
                    record[node_start + 1],
                    record[node_start + 2],
                    record[node_start + 3],
                ]) as usize;
                let entries_size = u32::from_le_bytes([
                    record[node_start + 4],
                    record[node_start + 5],
                    record[node_start + 6],
                    record[node_start + 7],
                ]) as usize;

                let entries_start = node_start + entries_offset;
                let entries_end = node_start + entries_size;

                // Find insertion point (sorted by name, before end sentinel)
                let insert_pos = self
                    .find_index_insert_position(&record[entries_start..entries_end], entry_bytes);
                let abs_insert = entries_start + insert_pos;

                // Check if there's room in the MFT record
                let used_size =
                    u32::from_le_bytes([record[0x18], record[0x19], record[0x1A], record[0x1B]])
                        as usize;
                if used_size + entry_bytes.len() > record_size as usize {
                    return Ok(false);
                }

                // Make room: shift everything after insert point
                let shift_end = used_size; // end of used record data
                record.copy_within(abs_insert..shift_end, abs_insert + entry_bytes.len());

                // Insert the entry
                record[abs_insert..abs_insert + entry_bytes.len()].copy_from_slice(entry_bytes);

                // Update entries_size in node header
                let new_entries_size = entries_size + entry_bytes.len();
                record[node_start + 4..node_start + 8]
                    .copy_from_slice(&(new_entries_size as u32).to_le_bytes());
                // Update allocated_entries_size too
                record[node_start + 8..node_start + 12]
                    .copy_from_slice(&(new_entries_size as u32).to_le_bytes());

                // Update INDEX_ROOT value length
                let new_value_length = value_length + entry_bytes.len();
                record[pos + 0x10..pos + 0x14]
                    .copy_from_slice(&(new_value_length as u32).to_le_bytes());

                // Update INDEX_ROOT attribute length
                let new_attr_len = attr_len + entry_bytes.len();
                record[pos + 4..pos + 8].copy_from_slice(&(new_attr_len as u32).to_le_bytes());

                // Update record used_size
                let new_used = used_size + entry_bytes.len();
                record[0x18..0x1C].copy_from_slice(&(new_used as u32).to_le_bytes());

                return Ok(true);
            }

            pos += attr_len;
        }

        Ok(false)
    }

    /// Try to insert an index entry into an existing INDX node in $INDEX_ALLOCATION.
    fn try_insert_into_index_allocation(
        &self,
        alloc_data: &mut [u8],
        entry_bytes: &[u8],
    ) -> Result<bool, FilesystemError> {
        let indx_size = 4096usize;
        let mut pos = 0;

        while pos + indx_size <= alloc_data.len() {
            if &alloc_data[pos..pos + 4] != b"INDX" {
                pos += indx_size;
                continue;
            }

            // Apply fixup to work with the record
            let mut indx = alloc_data[pos..pos + indx_size].to_vec();
            let _ = apply_fixup(&mut indx, self.bytes_per_sector);

            let node_offset = 0x18;
            if node_offset + 16 > indx.len() {
                pos += indx_size;
                continue;
            }

            let entries_offset = u32::from_le_bytes([
                indx[node_offset],
                indx[node_offset + 1],
                indx[node_offset + 2],
                indx[node_offset + 3],
            ]) as usize;
            let entries_size = u32::from_le_bytes([
                indx[node_offset + 4],
                indx[node_offset + 5],
                indx[node_offset + 6],
                indx[node_offset + 7],
            ]) as usize;
            let alloc_entries_size = u32::from_le_bytes([
                indx[node_offset + 8],
                indx[node_offset + 9],
                indx[node_offset + 10],
                indx[node_offset + 11],
            ]) as usize;

            let available = alloc_entries_size - entries_size;
            if available >= entry_bytes.len() {
                let entries_start = node_offset + entries_offset;
                let entries_end = node_offset + entries_size;

                let insert_pos =
                    self.find_index_insert_position(&indx[entries_start..entries_end], entry_bytes);
                let abs_insert = entries_start + insert_pos;

                // Shift and insert
                indx.copy_within(abs_insert..entries_end, abs_insert + entry_bytes.len());
                indx[abs_insert..abs_insert + entry_bytes.len()].copy_from_slice(entry_bytes);

                // Update entries_size
                let new_entries_size = entries_size + entry_bytes.len();
                indx[node_offset + 4..node_offset + 8]
                    .copy_from_slice(&(new_entries_size as u32).to_le_bytes());

                // Apply fixup for writing back
                prepare_fixup(&mut indx, self.bytes_per_sector);
                alloc_data[pos..pos + indx_size].copy_from_slice(&indx);
                return Ok(true);
            }

            pos += indx_size;
        }

        Ok(false)
    }

    /// Find the sorted insertion position in an index entry list.
    /// Returns byte offset within the entries data where the new entry should go
    /// (before the end sentinel).
    fn find_index_insert_position(&self, entries_data: &[u8], new_entry: &[u8]) -> usize {
        let new_name = extract_name_from_index_entry(new_entry);
        let mut pos = 0;

        while pos + 16 <= entries_data.len() {
            let entry_len =
                u16::from_le_bytes([entries_data[pos + 8], entries_data[pos + 9]]) as usize;
            let flags = u32::from_le_bytes([
                entries_data[pos + 12],
                entries_data[pos + 13],
                entries_data[pos + 14],
                entries_data[pos + 15],
            ]);

            if entry_len < 16 || pos + entry_len > entries_data.len() {
                break;
            }

            // If end sentinel, insert before it
            if flags & INDEX_ENTRY_END != 0 {
                return pos;
            }

            // Compare names for sorted insertion
            let existing_name = extract_name_from_index_entry(&entries_data[pos..pos + entry_len]);
            if new_name.to_uppercase() < existing_name.to_uppercase() {
                return pos;
            }

            pos += entry_len;
        }

        pos
    }

    /// Remove an index entry by name from a directory.
    fn remove_index_entry(
        &mut self,
        parent_record_num: u64,
        name: &str,
    ) -> Result<(), FilesystemError> {
        let mut record = self.read_mft_record(parent_record_num)?;
        let record_size = self.mft_record_size;

        // Try removing from $INDEX_ROOT first
        if self.try_remove_from_index_root(&mut record, name, record_size)? {
            self.write_mft_record(parent_record_num, &mut record)?;
            return Ok(());
        }

        // Try removing from $INDEX_ALLOCATION
        let attrs = parse_mft_attributes(&record, record_size);
        for attr in &attrs {
            if attr.attr_type == ATTR_INDEX_ALLOCATION && !attr.resident {
                let mut alloc_data = self.read_attribute_data(attr, None)?;
                if self.try_remove_from_index_allocation(&mut alloc_data, name)? {
                    self.write_data_to_runs(&attr.data_runs, &alloc_data)?;
                    return Ok(());
                }
            }
        }

        Err(FilesystemError::NotFound(format!(
            "index entry '{}' not found in directory",
            name
        )))
    }

    /// Try to remove an entry from $INDEX_ROOT. Returns true if found and removed.
    fn try_remove_from_index_root(
        &self,
        record: &mut [u8],
        name: &str,
        record_size: u32,
    ) -> Result<bool, FilesystemError> {
        let _ = record_size;
        let attr_offset = u16::from_le_bytes([record[0x14], record[0x15]]) as usize;
        let mut pos = attr_offset;

        while pos + 16 <= record.len() {
            let attr_type = u32::from_le_bytes([
                record[pos],
                record[pos + 1],
                record[pos + 2],
                record[pos + 3],
            ]);
            if attr_type == ATTR_END || attr_type == 0 {
                break;
            }
            let attr_len = u32::from_le_bytes([
                record[pos + 4],
                record[pos + 5],
                record[pos + 6],
                record[pos + 7],
            ]) as usize;
            if attr_len < 16 || pos + attr_len > record.len() {
                break;
            }

            if attr_type == ATTR_INDEX_ROOT && record[pos + 8] == 0 {
                let value_offset =
                    u16::from_le_bytes([record[pos + 0x14], record[pos + 0x15]]) as usize;
                let value_length = u32::from_le_bytes([
                    record[pos + 0x10],
                    record[pos + 0x11],
                    record[pos + 0x12],
                    record[pos + 0x13],
                ]) as usize;

                let ir_start = pos + value_offset;
                let node_start = ir_start + 16;
                if node_start + 16 > record.len() {
                    return Ok(false);
                }

                let entries_offset = u32::from_le_bytes([
                    record[node_start],
                    record[node_start + 1],
                    record[node_start + 2],
                    record[node_start + 3],
                ]) as usize;
                let entries_size = u32::from_le_bytes([
                    record[node_start + 4],
                    record[node_start + 5],
                    record[node_start + 6],
                    record[node_start + 7],
                ]) as usize;

                let entries_start = node_start + entries_offset;
                let entries_end = node_start + entries_size;

                if let Some((entry_off, entry_len)) =
                    find_entry_by_name(&record[entries_start..entries_end], name)
                {
                    let abs_off = entries_start + entry_off;
                    let used_size = u32::from_le_bytes([
                        record[0x18],
                        record[0x19],
                        record[0x1A],
                        record[0x1B],
                    ]) as usize;

                    // Shift data after the entry
                    record.copy_within(abs_off + entry_len..used_size, abs_off);
                    // Zero out freed space
                    let new_used = used_size - entry_len;
                    record[new_used..used_size].fill(0);

                    // Update entries_size
                    let new_entries_size = entries_size - entry_len;
                    record[node_start + 4..node_start + 8]
                        .copy_from_slice(&(new_entries_size as u32).to_le_bytes());
                    record[node_start + 8..node_start + 12]
                        .copy_from_slice(&(new_entries_size as u32).to_le_bytes());

                    // Update value length
                    let new_value_length = value_length - entry_len;
                    record[pos + 0x10..pos + 0x14]
                        .copy_from_slice(&(new_value_length as u32).to_le_bytes());

                    // Update attr length
                    let new_attr_len = attr_len - entry_len;
                    record[pos + 4..pos + 8].copy_from_slice(&(new_attr_len as u32).to_le_bytes());

                    // Update record used_size
                    record[0x18..0x1C].copy_from_slice(&(new_used as u32).to_le_bytes());

                    return Ok(true);
                }
            }

            pos += attr_len;
        }

        Ok(false)
    }

    /// Try to remove an entry from $INDEX_ALLOCATION INDX nodes.
    fn try_remove_from_index_allocation(
        &self,
        alloc_data: &mut [u8],
        name: &str,
    ) -> Result<bool, FilesystemError> {
        let indx_size = 4096usize;
        let mut pos = 0;

        while pos + indx_size <= alloc_data.len() {
            if &alloc_data[pos..pos + 4] != b"INDX" {
                pos += indx_size;
                continue;
            }

            let mut indx = alloc_data[pos..pos + indx_size].to_vec();
            let _ = apply_fixup(&mut indx, self.bytes_per_sector);

            let node_offset = 0x18;
            if node_offset + 16 > indx.len() {
                pos += indx_size;
                continue;
            }

            let entries_offset = u32::from_le_bytes([
                indx[node_offset],
                indx[node_offset + 1],
                indx[node_offset + 2],
                indx[node_offset + 3],
            ]) as usize;
            let entries_size = u32::from_le_bytes([
                indx[node_offset + 4],
                indx[node_offset + 5],
                indx[node_offset + 6],
                indx[node_offset + 7],
            ]) as usize;

            let entries_start = node_offset + entries_offset;
            let entries_end = node_offset + entries_size;

            if let Some((entry_off, entry_len)) =
                find_entry_by_name(&indx[entries_start..entries_end], name)
            {
                let abs_off = entries_start + entry_off;
                indx.copy_within(abs_off + entry_len..entries_end, abs_off);
                let freed_start = entries_end - entry_len;
                indx[freed_start..entries_end].fill(0);

                let new_entries_size = entries_size - entry_len;
                indx[node_offset + 4..node_offset + 8]
                    .copy_from_slice(&(new_entries_size as u32).to_le_bytes());

                prepare_fixup(&mut indx, self.bytes_per_sector);
                alloc_data[pos..pos + indx_size].copy_from_slice(&indx);
                return Ok(true);
            }

            pos += indx_size;
        }

        Ok(false)
    }
}

/// Extract the UTF-16LE name from an index entry's $FILE_NAME content.
fn extract_name_from_index_entry(entry: &[u8]) -> String {
    if entry.len() < 16 + 66 {
        return String::new();
    }
    let content = &entry[16..];
    if content.len() < 66 {
        return String::new();
    }
    let name_len = content[64] as usize;
    if 66 + name_len * 2 > content.len() {
        return String::new();
    }
    let chars: Vec<u16> = (0..name_len)
        .map(|i| u16::from_le_bytes([content[66 + i * 2], content[66 + i * 2 + 1]]))
        .collect();
    String::from_utf16_lossy(&chars)
}

/// Find an index entry by name within entries data. Returns (offset, length).
fn find_entry_by_name(entries_data: &[u8], target_name: &str) -> Option<(usize, usize)> {
    let target_lower = target_name.to_lowercase();
    let mut pos = 0;

    while pos + 16 <= entries_data.len() {
        let entry_len = u16::from_le_bytes([entries_data[pos + 8], entries_data[pos + 9]]) as usize;
        let flags = u32::from_le_bytes([
            entries_data[pos + 12],
            entries_data[pos + 13],
            entries_data[pos + 14],
            entries_data[pos + 15],
        ]);

        if entry_len < 16 || pos + entry_len > entries_data.len() {
            break;
        }
        if flags & INDEX_ENTRY_END != 0 {
            break;
        }

        let name = extract_name_from_index_entry(&entries_data[pos..pos + entry_len]);
        if name.to_lowercase() == target_lower {
            return Some((pos, entry_len));
        }

        pos += entry_len;
    }

    None
}

// =============================================================================
// EditableFilesystem Implementation
// =============================================================================

impl<R: Read + Write + Seek + Send> EditableFilesystem for NtfsFilesystem<R> {
    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn std::io::Read,
        data_len: u64,
        _options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        validate_ntfs_name(name)?;

        let parent_record_num = if parent.path == "/" {
            MFT_RECORD_ROOT
        } else {
            parent.location
        };

        if self.name_exists_in_index(parent_record_num, name)? {
            return Err(FilesystemError::AlreadyExists(name.to_string()));
        }

        let record_num = self.allocate_mft_record()?;

        // Read file data
        let mut file_data = vec![0u8; data_len as usize];
        if data_len > 0 {
            data.read_exact(&mut file_data)
                .map_err(FilesystemError::Io)?;
        }

        // Determine resident vs non-resident threshold
        // Approximate: record_size - header(0x38) - StdInfo(~72) - FileName(~104) - SD(~80) - DATA_header(~24) - end(4)
        let overhead = 0x38 + 72 + 104 + 80 + 24 + 4;
        let resident_threshold = (self.mft_record_size as usize).saturating_sub(overhead);

        let data_attr = if data_len as usize <= resident_threshold {
            // Resident $DATA
            build_resident_attr(ATTR_DATA, &file_data)
        } else {
            // Non-resident: allocate clusters
            let clusters_needed = data_len.div_ceil(self.cluster_size) as u32;
            let runs = self.allocate_volume_clusters(clusters_needed)?;

            // Write data to allocated clusters
            let mut written = 0u64;
            for &(start_cluster, length) in &runs {
                let offset = self.cluster_offset(start_cluster);
                self.reader.seek(SeekFrom::Start(offset))?;
                let run_bytes = length * self.cluster_size;
                let to_write = run_bytes.min(data_len - written);
                self.reader
                    .write_all(&file_data[written as usize..(written + to_write) as usize])?;
                // Zero-fill remainder of last cluster
                if to_write < run_bytes {
                    let zeros = vec![0u8; (run_bytes - to_write) as usize];
                    self.reader.write_all(&zeros)?;
                }
                written += to_write;
            }

            let alloc_size = clusters_needed as u64 * self.cluster_size;
            let mut attr = build_nonresident_attr(ATTR_DATA, &runs, data_len);
            // Fix up allocated size
            attr[0x28..0x30].copy_from_slice(&alloc_size.to_le_bytes());
            attr
        };

        // Build attributes
        let std_info =
            build_resident_attr(ATTR_STANDARD_INFORMATION, &build_standard_information());
        let file_name_value = build_file_name_attr(parent_record_num, name, false, data_len);
        let file_name_attr = build_resident_attr(ATTR_FILE_NAME, &file_name_value);
        let sd_value = self.read_parent_security_descriptor(parent_record_num)?;
        let sd_attr = build_resident_attr(ATTR_SECURITY_DESCRIPTOR, &sd_value);

        let attrs = vec![std_info, file_name_attr, sd_attr, data_attr];
        let mut record = assemble_mft_record(&attrs, MFT_RECORD_IN_USE, self.mft_record_size);
        self.write_mft_record(record_num, &mut record)?;

        // Build index entry and insert
        let index_entry = build_index_entry(record_num, 1, &file_name_value);
        self.insert_index_entry(parent_record_num, &index_entry)?;

        let path = if parent.path == "/" {
            format!("/{name}")
        } else {
            format!("{}/{name}", parent.path)
        };

        Ok(FileEntry::new_file(
            name.to_string(),
            path,
            data_len,
            record_num,
        ))
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        validate_ntfs_name(name)?;

        let parent_record_num = if parent.path == "/" {
            MFT_RECORD_ROOT
        } else {
            parent.location
        };

        if self.name_exists_in_index(parent_record_num, name)? {
            return Err(FilesystemError::AlreadyExists(name.to_string()));
        }

        let record_num = self.allocate_mft_record()?;

        let std_info =
            build_resident_attr(ATTR_STANDARD_INFORMATION, &build_standard_information());
        let file_name_value = build_file_name_attr(parent_record_num, name, true, 0);
        let file_name_attr = build_resident_attr(ATTR_FILE_NAME, &file_name_value);
        let sd_value = self.read_parent_security_descriptor(parent_record_num)?;
        let sd_attr = build_resident_attr(ATTR_SECURITY_DESCRIPTOR, &sd_value);
        let index_root = build_resident_attr(ATTR_INDEX_ROOT, &build_empty_index_root());

        let attrs = vec![std_info, file_name_attr, sd_attr, index_root];
        let mut record = assemble_mft_record(
            &attrs,
            MFT_RECORD_IN_USE | MFT_RECORD_IS_DIRECTORY,
            self.mft_record_size,
        );
        self.write_mft_record(record_num, &mut record)?;

        // Build index entry and insert
        let index_entry = build_index_entry(record_num, 1, &file_name_value);
        self.insert_index_entry(parent_record_num, &index_entry)?;

        let path = if parent.path == "/" {
            format!("/{name}")
        } else {
            format!("{}/{name}", parent.path)
        };

        Ok(FileEntry::new_directory(name.to_string(), path, record_num))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        if entry.is_directory() {
            let children = self.list_directory(entry)?;
            if !children.is_empty() {
                return Err(FilesystemError::InvalidData(
                    "cannot delete non-empty directory".into(),
                ));
            }
        }

        let parent_record_num = if parent.path == "/" {
            MFT_RECORD_ROOT
        } else {
            parent.location
        };

        // Remove from parent's index
        self.remove_index_entry(parent_record_num, &entry.name)?;

        // Free data clusters if non-resident
        let record_number = entry.location;
        if let Ok(record) = self.read_mft_record(record_number) {
            let attrs = parse_mft_attributes(&record, self.mft_record_size);
            for attr in &attrs {
                if attr.attr_type == ATTR_DATA && !attr.resident {
                    let runs: Vec<(u64, u64)> = attr
                        .data_runs
                        .iter()
                        .filter(|r| r.cluster_offset > 0)
                        .map(|r| (r.cluster_offset as u64, r.length))
                        .collect();
                    if !runs.is_empty() {
                        self.free_volume_clusters(&runs)?;
                    }
                }
            }
        }

        // Free MFT record
        self.free_mft_record(record_number)?;

        Ok(())
    }

    fn rename(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
        new_name: &str,
    ) -> Result<(), FilesystemError> {
        if new_name == entry.name {
            return Ok(());
        }
        validate_ntfs_name(new_name)?;
        let parent_record_num = if parent.path == "/" {
            MFT_RECORD_ROOT
        } else {
            parent.location
        };

        // The index reports the entry's own (case-folded) name, so reject a
        // collision only when the new name differs from the old one.
        if !new_name.eq_ignore_ascii_case(&entry.name)
            && self.name_exists_in_index(parent_record_num, new_name)?
        {
            return Err(FilesystemError::AlreadyExists(new_name.to_string()));
        }

        let record_number = entry.location;
        let is_dir = entry.is_directory();

        // The new name lives in two places: the child record's $FILE_NAME
        // attribute and the parent directory's $I30 index entry. Both carry a
        // $FILE_NAME structure; build it once.
        let new_fn_value = build_file_name_attr(parent_record_num, new_name, is_dir, entry.size);

        // 1) Rewrite the child's $FILE_NAME in place (grow/shrink), preserving
        //    the record's sequence number, link count, and every other attribute.
        let mut record = self.read_mft_record(record_number)?;
        let child_seq = u16::from_le_bytes([record[0x10], record[0x11]]);
        let new_attr = build_resident_attr(ATTR_FILE_NAME, &new_fn_value);
        replace_resident_attr(&mut record, ATTR_FILE_NAME, &new_attr)?;
        self.write_mft_record(record_number, &mut record)?;

        // 2) Re-key the parent index entry (remove old name, insert new). The
        //    index entry's MFT reference must carry the child's real sequence
        //    number, not a hardcoded 1.
        self.remove_index_entry(parent_record_num, &entry.name)?;
        let index_entry = build_index_entry(record_number, child_seq, &new_fn_value);
        self.insert_index_entry(parent_record_num, &index_entry)?;

        Ok(())
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.reader.flush()?;
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        let free_clusters = self.count_free_volume_clusters()?;
        Ok(free_clusters * self.cluster_size)
    }
}

// =============================================================================
// Compaction
// =============================================================================

/// A reader that streams only the used clusters of an NTFS partition.
///
/// Layout: boot sector(s) | used clusters in order (skipping free ones)
pub struct CompactNtfsReader<R> {
    source: R,
    partition_offset: u64,
    cluster_size: u64,

    // Boot sector region (first cluster worth of data)
    boot_sectors: Vec<u8>,

    // Bitmap of which clusters are in use (sorted ascending; all entries
    // strictly less than `src_total_clusters`).
    used_cluster_list: Vec<u64>,

    /// Source's total cluster count (volume_size / cluster_size). Needed
    /// to derive the free-cluster set in `into_layout_preserving`.
    src_total_clusters: u64,
    /// Source partition's original byte size.
    src_original_size: u64,

    // Streaming state
    position: u64,
    total_size: u64,
    cluster_buf: Vec<u8>,
}

impl<R: Read + Seek> CompactNtfsReader<R> {
    pub fn new(
        mut source: R,
        partition_offset: u64,
    ) -> Result<(Self, CompactResult), FilesystemError> {
        // Read VBR
        source.seek(SeekFrom::Start(partition_offset))?;
        let mut vbr_buf = [0u8; 512];
        source
            .read_exact(&mut vbr_buf)
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
                            bitmap_data.resize(
                                bitmap_data.len() + (run.length * cluster_size) as usize,
                                0,
                            );
                            continue;
                        }
                        let run_offset =
                            partition_offset + run.cluster_offset as u64 * cluster_size;
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
                src_total_clusters: total_clusters,
                src_original_size: original_size,
                position: 0,
                total_size: compacted_size,
                cluster_buf: Vec::new(),
            },
            CompactResult {
                original_size,
                compacted_size,
                data_size: compacted_size,
                clusters_used,
            },
        ))
    }

    /// Convert this packed-output compact reader into a layout-preserving
    /// reader over the same source. Allocated clusters stream verbatim
    /// from the source; free clusters (per the parsed `$Bitmap`) emit
    /// zeros without reading. See `LayoutPreservingReader` for the
    /// design rationale.
    pub fn into_layout_preserving(
        self,
    ) -> (
        super::layout_preserving::LayoutPreservingReader<R>,
        CompactResult,
    ) {
        let cluster_size = self.cluster_size;
        // NTFS clusters span the entire volume from offset 0 — no
        // separate "data start". Free clusters lie in the gaps between
        // entries of the (sorted) used_cluster_list.
        let mut zero_ranges: Vec<(u64, u64)> = Vec::new();
        let mut next_used = 0usize;
        let mut run_start: Option<u64> = None;
        for cluster in 0..self.src_total_clusters {
            let off = cluster * cluster_size;
            let is_used = next_used < self.used_cluster_list.len()
                && self.used_cluster_list[next_used] == cluster;
            if is_used {
                next_used += 1;
                if let Some(start) = run_start.take() {
                    zero_ranges.push((start, off - start));
                }
            } else if run_start.is_none() {
                run_start = Some(off);
            }
        }
        if let Some(start) = run_start {
            let end = self.src_total_clusters * cluster_size;
            zero_ranges.push((start, end - start));
        }

        let clusters_used = self.used_cluster_list.len() as u32;
        let info = CompactResult {
            original_size: self.src_original_size,
            compacted_size: self.src_original_size,
            data_size: clusters_used as u64 * cluster_size,
            clusters_used,
        };
        let reader = super::layout_preserving::LayoutPreservingReader::new(
            self.source,
            self.partition_offset,
            self.src_original_size,
            zero_ranges,
        )
        .expect("NTFS zero ranges must be sorted, non-overlapping, in-bounds");
        (reader, info)
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
                    self.source
                        .seek(SeekFrom::Start(src_offset))
                        .map_err(io::Error::other)?;
                    self.cluster_buf.resize(self.cluster_size as usize, 0);
                    self.source
                        .read_exact(&mut self.cluster_buf)
                        .map_err(io::Error::other)?;
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
        vbr[0x28], vbr[0x29], vbr[0x2A], vbr[0x2B], vbr[0x2C], vbr[0x2D], vbr[0x2E], vbr[0x2F],
    ]);

    if old_total == new_total_sectors {
        return Ok(false);
    }

    let cluster_size = bytes_per_sector * sectors_per_cluster;

    // Check that data doesn't extend beyond new size by reading $Bitmap
    // We need to find the last used cluster
    let mft_cluster = u64::from_le_bytes([
        vbr[0x30], vbr[0x31], vbr[0x32], vbr[0x33], vbr[0x34], vbr[0x35], vbr[0x36], vbr[0x37],
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
    apply_fixup(&mut record, bytes_per_sector).map_err(|e| anyhow::anyhow!("{e}"))?;

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
        vbr[0x30], vbr[0x31], vbr[0x32], vbr[0x33], vbr[0x34], vbr[0x35], vbr[0x36], vbr[0x37],
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
    let mut vbr = crate::fs::patch::read_boot_sector(file, partition_offset)?;

    if &vbr[3..11] != b"NTFS    " {
        return Ok(());
    }

    if let Some(old_hidden) =
        crate::fs::patch::patch_u32_le_in_buf(&mut vbr, 0x1C, start_lba as u32)
    {
        // Write primary VBR
        crate::fs::patch::write_sector_at(file, partition_offset, &vbr)?;

        // Write backup boot sector at last sector
        let total_sectors = u64::from_le_bytes([
            vbr[0x28], vbr[0x29], vbr[0x2A], vbr[0x2B], vbr[0x2C], vbr[0x2D], vbr[0x2E], vbr[0x2F],
        ]);
        let bytes_per_sector = u16::from_le_bytes([vbr[0x0B], vbr[0x0C]]) as u64;
        if total_sectors > 0 && bytes_per_sector > 0 {
            let backup_offset = partition_offset + (total_sectors - 1) * bytes_per_sector;
            crate::fs::patch::write_sector_at(file, backup_offset, &vbr)?;
        }

        log_cb(&format!(
            "NTFS: patched hidden sectors {} -> {}",
            old_hidden, start_lba as u32
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
    use super::super::filesystem::{CreateDirectoryOptions, CreateFileOptions, EditableFilesystem};
    use super::*;
    use std::io::Cursor;

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

    /// Load and decompress a zstd-compressed test fixture.
    fn load_fixture(name: &str) -> Vec<u8> {
        let path = format!("tests/fixtures/{name}");
        let compressed =
            std::fs::read(&path).unwrap_or_else(|e| panic!("Failed to read fixture {path}: {e}"));
        let mut decoder = crate::rbformats::zstd_compat::decoder(Cursor::new(compressed))
            .unwrap_or_else(|e| panic!("Failed to create zstd decoder for {path}: {e}"));
        let mut output = Vec::new();
        std::io::Read::read_to_end(&mut decoder, &mut output)
            .unwrap_or_else(|e| panic!("Failed to decompress {path}: {e}"));
        output
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
            0x00, // end
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
            0x11, 0x04, 0x20, // run 1: len=4, offset=+32
            0x11, 0x04, 0xF0, // run 2: len=4, offset=-16 (abs=32-16=16)
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

    // =========================================================================
    // Data run encoding tests
    // =========================================================================

    #[test]
    fn test_ntfs_encode_data_runs() {
        // Round-trip: encode then decode, compare
        let original_runs = vec![(10u64, 4u64), (30, 8), (100, 2)];
        let encoded = encode_data_runs(&original_runs);

        // Decode and compare
        let decoded = decode_data_runs(&encoded);
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].cluster_offset, 10);
        assert_eq!(decoded[0].length, 4);
        assert_eq!(decoded[1].cluster_offset, 30);
        assert_eq!(decoded[1].length, 8);
        assert_eq!(decoded[2].cluster_offset, 100);
        assert_eq!(decoded[2].length, 2);
    }

    #[test]
    fn test_ntfs_encode_data_runs_single() {
        let runs = vec![(5u64, 1u64)];
        let encoded = encode_data_runs(&runs);
        let decoded = decode_data_runs(&encoded);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].cluster_offset, 5);
        assert_eq!(decoded[0].length, 1);
    }

    #[test]
    fn test_ntfs_encode_data_runs_large_offsets() {
        let runs = vec![(1000u64, 256u64), (5000, 512)];
        let encoded = encode_data_runs(&runs);
        let decoded = decode_data_runs(&encoded);
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].cluster_offset, 1000);
        assert_eq!(decoded[0].length, 256);
        assert_eq!(decoded[1].cluster_offset, 5000);
        assert_eq!(decoded[1].length, 512);
    }

    // =========================================================================
    // Fixup round-trip test
    // =========================================================================

    #[test]
    fn test_ntfs_fixup_round_trip() {
        // Create a 1024-byte record with known data
        let mut record = vec![0xABu8; 1024];
        record[0..4].copy_from_slice(b"FILE");
        record[0x04..0x06].copy_from_slice(&0x0030u16.to_le_bytes()); // fixup offset
        record[0x06..0x08].copy_from_slice(&3u16.to_le_bytes()); // fixup count (1 USN + 2 entries)

        // Initialize fixup: USN=1, entries contain what's at sector ends
        record[0x30] = 0x01; // USN
        record[0x31] = 0x00;
        // Sector 1 end (offset 510-511)
        record[510] = 0xCC;
        record[511] = 0xDD;
        // Sector 2 end (offset 1022-1023)
        record[1022] = 0xEE;
        record[1023] = 0xFF;

        let original = record.clone();

        // prepare_fixup should save original bytes and stamp USN
        prepare_fixup(&mut record, 512);

        // The sector ends should now contain the USN
        let usn = u16::from_le_bytes([record[0x30], record[0x31]]);
        assert_eq!(record[510], usn as u8);
        assert_eq!(record[511], (usn >> 8) as u8);
        assert_eq!(record[1022], usn as u8);
        assert_eq!(record[1023], (usn >> 8) as u8);

        // The fixup array slots should contain the original bytes
        assert_eq!(record[0x32], original[510]);
        assert_eq!(record[0x33], original[511]);
        assert_eq!(record[0x34], original[1022]);
        assert_eq!(record[0x35], original[1023]);

        // apply_fixup should restore original bytes
        apply_fixup(&mut record, 512).unwrap();
        assert_eq!(record[510], original[510]);
        assert_eq!(record[511], original[511]);
        assert_eq!(record[1022], original[1022]);
        assert_eq!(record[1023], original[1023]);
    }

    // =========================================================================
    // Integration tests using real NTFS fixture
    // =========================================================================

    #[test]
    fn test_ntfs_create_small_file() {
        let mut image = load_fixture("test_ntfs.img.zst");
        let mut fs = NtfsFilesystem::open(Cursor::new(&mut image), 0).unwrap();

        let root = fs.root().unwrap();
        let initial_free = fs.free_space().unwrap();

        let data = b"Hello NTFS editing!";
        let mut cursor = Cursor::new(data.as_slice());
        let file = fs
            .create_file(
                &root,
                "test_edit.txt",
                &mut cursor,
                data.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();

        assert_eq!(file.name, "test_edit.txt");
        assert_eq!(file.size, data.len() as u64);

        // Verify in directory listing
        let entries = fs.list_directory(&root).unwrap();
        assert!(entries.iter().any(|e| e.name == "test_edit.txt"));

        // Read back data
        let read_back = fs.read_file(&file, data.len()).unwrap();
        assert_eq!(read_back, data);

        // Free space should have decreased (or stayed same for resident)
        let new_free = fs.free_space().unwrap();
        assert!(new_free <= initial_free);
    }

    #[test]
    fn test_ntfs_create_large_file() {
        let mut image = load_fixture("test_ntfs.img.zst");
        let mut fs = NtfsFilesystem::open(Cursor::new(&mut image), 0).unwrap();

        let root = fs.root().unwrap();
        let initial_free = fs.free_space().unwrap();

        // Create a file larger than resident threshold (~700 bytes)
        let data = vec![0x42u8; 2048];
        let mut cursor = Cursor::new(data.as_slice());
        let file = fs
            .create_file(
                &root,
                "large.bin",
                &mut cursor,
                data.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();

        assert_eq!(file.name, "large.bin");
        assert_eq!(file.size, 2048);

        // Verify listing
        let entries = fs.list_directory(&root).unwrap();
        assert!(entries.iter().any(|e| e.name == "large.bin"));

        // Read back
        let read_back = fs.read_file(&file, data.len()).unwrap();
        assert_eq!(read_back, data);

        // Free space should have decreased
        let new_free = fs.free_space().unwrap();
        assert!(new_free < initial_free);
    }

    #[test]
    fn test_ntfs_create_directory() {
        let mut image = load_fixture("test_ntfs.img.zst");
        let mut fs = NtfsFilesystem::open(Cursor::new(&mut image), 0).unwrap();

        let root = fs.root().unwrap();
        let dir = fs
            .create_directory(&root, "newdir", &CreateDirectoryOptions::default())
            .unwrap();

        assert_eq!(dir.name, "newdir");
        assert!(dir.is_directory());

        // Verify in parent listing
        let entries = fs.list_directory(&root).unwrap();
        assert!(entries
            .iter()
            .any(|e| e.name == "newdir" && e.is_directory()));

        // New directory should be empty
        let children = fs.list_directory(&dir).unwrap();
        assert!(children.is_empty());
    }

    #[test]
    fn test_ntfs_rename_file() {
        let mut image = load_fixture("test_ntfs.img.zst");
        let mut fs = NtfsFilesystem::open(Cursor::new(&mut image), 0).unwrap();
        let root = fs.root().unwrap();

        let data = b"rename me, keep my bytes";
        let mut cursor = Cursor::new(data.as_slice());
        let file = fs
            .create_file(
                &root,
                "old.txt",
                &mut cursor,
                data.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
        let rec = file.location;

        // Rename to a LONGER name — exercises the resident $FILE_NAME grow path
        // (and re-keys the parent $I30 index).
        fs.rename(&root, &file, "a considerably longer name.txt")
            .unwrap();
        let renamed = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "a considerably longer name.txt")
            .expect("renamed entry listed");
        assert!(!fs
            .list_directory(&root)
            .unwrap()
            .iter()
            .any(|e| e.name == "old.txt"));
        // Same MFT record (identity) and content preserved.
        assert_eq!(renamed.location, rec);
        assert_eq!(fs.read_file(&renamed, data.len()).unwrap(), data);

        // Rename to a SHORTER name — the shrink path.
        fs.rename(&root, &renamed, "x.txt").unwrap();
        let short = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "x.txt")
            .expect("short name listed");
        assert_eq!(fs.read_file(&short, data.len()).unwrap(), data);

        // A collision with a different entry is rejected.
        let mut c2 = Cursor::new(b"y".as_slice());
        fs.create_file(
            &root,
            "taken.txt",
            &mut c2,
            1,
            &CreateFileOptions::default(),
        )
        .unwrap();
        assert!(matches!(
            fs.rename(&root, &short, "taken.txt"),
            Err(FilesystemError::AlreadyExists(_))
        ));

        fs.sync_metadata().unwrap();
    }

    #[test]
    fn test_ntfs_delete_file() {
        let mut image = load_fixture("test_ntfs.img.zst");
        let mut fs = NtfsFilesystem::open(Cursor::new(&mut image), 0).unwrap();

        let root = fs.root().unwrap();
        let initial_free = fs.free_space().unwrap();

        // Create then delete
        let data = b"temporary";
        let mut cursor = Cursor::new(data.as_slice());
        let file = fs
            .create_file(
                &root,
                "temp.txt",
                &mut cursor,
                data.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();

        fs.delete_entry(&root, &file).unwrap();

        // Should no longer appear
        let entries = fs.list_directory(&root).unwrap();
        assert!(!entries.iter().any(|e| e.name == "temp.txt"));

        // Free space should be restored (approximately) after delete.
        let final_free = fs.free_space().unwrap();
        assert!(
            final_free >= initial_free,
            "free space did not recover after delete"
        );
    }

    #[test]
    fn test_ntfs_duplicate_name() {
        let mut image = load_fixture("test_ntfs.img.zst");
        let mut fs = NtfsFilesystem::open(Cursor::new(&mut image), 0).unwrap();

        let root = fs.root().unwrap();

        // The fixture already has hello.txt
        let data = b"duplicate";
        let mut cursor = Cursor::new(data.as_slice());
        let result = fs.create_file(
            &root,
            "hello.txt",
            &mut cursor,
            data.len() as u64,
            &CreateFileOptions::default(),
        );

        assert!(matches!(result, Err(FilesystemError::AlreadyExists(_))));
    }
}
