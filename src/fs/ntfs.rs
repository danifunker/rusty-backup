use std::collections::HashMap;
use std::io::{self, Read, Seek, SeekFrom, Write};

#[cfg(feature = "rust173-polyfill")]
use crate::rust173_compat::IntIsMultipleOf as _;
use anyhow::{bail, Result};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};
use super::CompactResult;

/// NTFS update-sequence (fixup) block size: one USA entry protects each
/// 512-byte block of a multi-sector structure, regardless of the volume's
/// actual sector size. (A 4096-byte record on a 4096-byte-sector volume still
/// carries 4096/512 + 1 = 9 USA entries.)
const NTFS_BLOCK_SIZE: usize = 512;

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
/// First `$Secure` id every NTFS formatter registers; used when the parent has no usable one.
const DEFAULT_SECURITY_ID: u32 = 0x100;
const ATTR_FILE_NAME: u32 = 0x30;
const ATTR_SECURITY_DESCRIPTOR: u32 = 0x50;

// MFT record flags
const MFT_RECORD_IN_USE: u16 = 0x0001;
const MFT_RECORD_IS_DIRECTORY: u16 = 0x0002;

// Index entry flags
const INDEX_ENTRY_END: u32 = 0x02;

// File attribute flags (from $FILE_NAME)
const FILE_ATTR_DIRECTORY: u32 = 0x1000_0000;
/// Windows writes this on every ordinary file; a zero attribute word is a shape it never produces.
const FILE_ATTR_ARCHIVE: u32 = 0x20;

/// NTFS Volume Boot Record fields.
#[derive(Clone)]
pub(crate) struct NtfsVbr {
    pub(crate) bytes_per_sector: u64,
    pub(crate) sectors_per_cluster: u64,
    pub(crate) total_sectors: u64,
    pub(crate) mft_cluster: u64,
    pub(crate) mft_mirror_cluster: u64,
    pub(crate) mft_record_size: u32,
    pub(crate) index_record_size: u32,
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
    let cluster_bytes = sectors_per_cluster as u32 * bytes_per_sector as u32;
    let mft_record_size = mft_record_bytes(vbr[0x40] as i8, cluster_bytes).ok_or_else(|| {
        FilesystemError::Parse(format!(
            "NTFS: clusters-per-MFT-record byte 0x{:02X} is not a valid record size",
            vbr[0x40]
        ))
    })?;

    // Clusters per index record at 0x44: same signed encoding as 0x40.
    let clusters_per_index_raw = vbr[0x44] as i8;
    let index_record_size = if clusters_per_index_raw < 0 {
        1u32 << ((-clusters_per_index_raw) as u32)
    } else {
        clusters_per_index_raw as u32 * sectors_per_cluster as u32 * bytes_per_sector as u32
    };

    Ok(NtfsVbr {
        bytes_per_sector,
        sectors_per_cluster,
        total_sectors,
        mft_cluster,
        mft_mirror_cluster,
        mft_record_size,
        index_record_size: if index_record_size == 0 || !index_record_size.is_power_of_two() {
            4096
        } else {
            index_record_size
        },
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
    /// Hole (no offset field). LCN 0 alone cannot mean sparse: $Boot lives there.
    pub(crate) sparse: bool,
}

/// MFT record size from the boot sector's clusters-per-record byte: a negative
/// value is a power-of-two shift, a positive one a cluster count. Anything
/// outside 256 bytes to 1 MiB is damaged media, not a record size.
pub(crate) fn mft_record_bytes(raw: i8, cluster_bytes: u32) -> Option<u32> {
    let size = if raw < 0 {
        let shift = (-(raw as i32)) as u32;
        if !(8..=20).contains(&shift) {
            return None;
        }
        1u32 << shift
    } else {
        (raw as u32).checked_mul(cluster_bytes)?
    };
    ((256..=1 << 20).contains(&size) && size.is_power_of_two()).then_some(size)
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
        // A nibble above 8 cannot be a real run (the shift below would overflow).
        if length_size == 0 || length_size > 8 || offset_size > 8 {
            break;
        }
        if pos + length_size + offset_size > data.len() {
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
                sparse: true,
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
                sparse: false,
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
pub(crate) fn apply_fixup(record: &mut [u8]) -> Result<(), FilesystemError> {
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
        // NTFS fixups are spaced one per 512-byte block, independent of the
        // volume's actual sector size (a 4096-byte record uses 9 USA entries).
        let sector_end = i * NTFS_BLOCK_SIZE;
        if sector_end < 2 || sector_end > record.len() {
            break;
        }
        let pos = sector_end - 2;
        let stored = u16::from_le_bytes([record[pos], record[pos + 1]]);
        if stored != signature {
            return Err(FilesystemError::Parse(format!(
                "MFT fixup mismatch at block {i}: expected {signature:#06x}, got {stored:#06x}"
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
    index_record_size: u32,
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
            index_record_size: vbr.index_record_size,
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

    /// Consume the filesystem and return the underlying reader/writer. Used by
    /// the defragmenting clone to drain the freshly repacked tempfile.
    pub(crate) fn into_reader(self) -> R {
        self.reader
    }

    /// Number of MFT records the source `$MFT` can address (its `$DATA`
    /// allocation / record size). A safe upper bound on the records the clone
    /// target needs — the repacked volume holds the same files. Used to size
    /// the blank target's MFT via [`crate::fs::ntfs_format::create_blank_ntfs`].
    pub fn mft_record_capacity(&self) -> u64 {
        let clusters: u64 = self.mft_data_runs.iter().map(|r| r.length).sum();
        if self.mft_record_size == 0 {
            return 64;
        }
        (clusters * self.cluster_size / self.mft_record_size as u64).max(64)
    }

    /// Bytes per sector of the source volume. Used by the defragmenting clone
    /// so the repacked target inherits the source geometry rather than forcing
    /// a fixed sector size.
    pub fn bytes_per_sector(&self) -> u64 {
        self.bytes_per_sector
    }

    /// Cluster (allocation unit) size of the source volume, in bytes. Inherited
    /// by the defragmenting clone target so it stays geometry-compatible.
    pub fn cluster_size(&self) -> u64 {
        self.cluster_size
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

        apply_fixup(&mut record)?;

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

            if run.sparse {
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

            if run.sparse {
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
                // Bits at or past the cluster count are the end-of-volume mark, not data.
                let volume_clusters =
                    self.total_sectors * self.bytes_per_sector / self.cluster_size;
                let mut c = (bitmap.len() as u64 * 8).min(volume_clusters);
                while c > 0 {
                    c -= 1;
                    if bitmap[(c / 8) as usize] & (1 << (c % 8)) != 0 {
                        return Ok(c);
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

        // Parse $INDEX_ROOT (always resident); its header declares the volume's
        // index block size, which the $INDEX_ALLOCATION walk below must honour.
        let mut block_size = self.index_record_size;
        for attr in &attrs {
            if attr.attr_type == ATTR_INDEX_ROOT && attr.resident {
                if let Some(raw) = attr.value.get(8..12) {
                    let bs = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
                    if bs != 0 && bs.is_power_of_two() && bs <= 2 * 1024 * 1024 {
                        block_size = bs;
                    }
                }
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
                self.parse_index_allocation_entries(
                    attr,
                    &bitmap_data,
                    block_size,
                    parent_path,
                    &mut entries,
                )?;
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
                if run.sparse || run.cluster_offset < 0 {
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
        block_size: u32,
        parent_path: &str,
        entries: &mut Vec<FileEntry>,
    ) -> Result<(), FilesystemError> {
        let record_size = block_size.max(512) as u64;
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

            // Blocks are dense in the stream: block i starts at byte i * size.
            // Gather cluster-by-cluster so a block spanning a run boundary
            // still reads correctly.
            let stream_off = i * record_size;
            let mut record_buf = vec![0u8; record_size as usize];
            let mut got = 0u64;
            let mut ok = true;
            while got < record_size {
                let off = stream_off + got;
                let intra = off % self.cluster_size;
                let chunk = (self.cluster_size - intra).min(record_size - got);
                match self.resolve_vcn_to_offset(&runs, off / self.cluster_size) {
                    Some(disk) => {
                        self.reader.seek(SeekFrom::Start(disk + intra))?;
                        if self
                            .reader
                            .read_exact(&mut record_buf[got as usize..(got + chunk) as usize])
                            .is_err()
                        {
                            ok = false;
                            break;
                        }
                    }
                    None => {
                        ok = false;
                        break;
                    }
                }
                got += chunk;
            }
            if !ok {
                continue;
            }

            if &record_buf[0..4] != b"INDX" {
                continue;
            }

            if apply_fixup(&mut record_buf).is_err() {
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

            // Parse $FILE_NAME content if present; a length past the entry is damage.
            if content_length >= 66 && 16 + content_length <= entry_length {
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
        // $FILE_NAME's LastModifiedTime lives at offset 16..24 — 8-byte FILETIME
        // (100-ns intervals since 1601-01-01 UTC).
        let modify_ft = u64::from_le_bytes([
            data[16], data[17], data[18], data[19], data[20], data[21], data[22], data[23],
        ]);
        let modified_unix = super::times::filetime_to_unix(modify_ft);
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

        let mut fe = if is_dir {
            FileEntry::new_directory(name, path, file_mft_ref)
        } else {
            FileEntry::new_file(name, path, real_size, file_mft_ref)
        };
        fe.modified_unix = modified_unix;
        Some(fe)
    }

    // ---- fsck helpers (see ntfs_fsck.rs) ----

    /// Geometry snapshot the fsck module needs.
    pub(crate) fn fsck_geometry(&self) -> NtfsGeom {
        NtfsGeom {
            partition_offset: self.partition_offset,
            bytes_per_sector: self.bytes_per_sector,
            cluster_size: self.cluster_size,
            total_sectors: self.total_sectors,
            total_clusters: (self.total_sectors * self.bytes_per_sector) / self.cluster_size,
            mft_cluster: self.mft_cluster,
            mft_record_size: self.mft_record_size,
        }
    }

    /// Read MFT record `n` if the record's IN_USE flag is set, apply fixups.
    /// Returns `None` for records the fixup layer / magic check rejects — those
    /// are treated as "not in use" for reconciliation.
    pub(crate) fn fsck_read_in_use_record(
        &mut self,
        n: u64,
    ) -> Result<Option<Vec<u8>>, FilesystemError> {
        let record = match self.read_mft_record(n) {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };
        if record.len() < 0x18 {
            return Ok(None);
        }
        let flags = u16::from_le_bytes([record[0x16], record[0x17]]);
        if flags & MFT_RECORD_IN_USE == 0 {
            return Ok(None);
        }
        Ok(Some(record))
    }

    /// Decode every non-resident attribute in an MFT record into a list of
    /// (LCN, length_in_clusters). Sparse runs (LCN 0) are skipped — they own
    /// no on-disk clusters.
    pub(crate) fn fsck_record_clusters(&self, record: &[u8]) -> Vec<(u64, u64)> {
        let attrs = parse_mft_attributes(record, self.mft_record_size);
        let mut out = Vec::new();
        for a in &attrs {
            if a.resident {
                continue;
            }
            for run in &a.data_runs {
                if !run.sparse && run.cluster_offset >= 0 && run.length > 0 {
                    out.push((run.cluster_offset as u64, run.length));
                }
            }
        }
        out
    }

    /// True when the record has a non-null `$ATTRIBUTE_LIST` (0x20) attribute.
    /// v1 fsck surfaces these as "not fully traced" rather than following the
    /// list into extension records.
    pub(crate) fn fsck_record_has_attribute_list(&self, record: &[u8]) -> bool {
        parse_mft_attributes(record, self.mft_record_size)
            .iter()
            .any(|a| a.attr_type == ATTR_ATTRIBUTE_LIST)
    }

    /// Read `$Bitmap`'s $DATA (MFT record 6) — the volume allocation bitmap.
    pub(crate) fn fsck_read_volume_bitmap(&mut self) -> Result<Vec<u8>, FilesystemError> {
        let record = self.read_mft_record(MFT_RECORD_BITMAP)?;
        let attrs = parse_mft_attributes(&record, self.mft_record_size);
        for a in &attrs {
            if a.attr_type == ATTR_DATA {
                return self.read_attribute_data(a, None);
            }
        }
        Err(FilesystemError::Parse(
            "$Bitmap $DATA attribute not found".into(),
        ))
    }

    /// Data runs of `$Bitmap`'s $DATA (for in-place rewrite).
    pub(crate) fn fsck_bitmap_data_runs(&mut self) -> Result<Vec<DataRun>, FilesystemError> {
        let record = self.read_mft_record(MFT_RECORD_BITMAP)?;
        let attrs = parse_mft_attributes(&record, self.mft_record_size);
        for a in &attrs {
            if a.attr_type == ATTR_DATA && !a.resident {
                return Ok(a.data_runs.clone());
            }
        }
        Err(FilesystemError::Parse(
            "$Bitmap $DATA must be non-resident".into(),
        ))
    }

    /// Raw first 4 MFT records (the four `$MFTMirr` mirrors).
    pub(crate) fn fsck_read_first_mft_records(&mut self) -> Result<Vec<u8>, FilesystemError> {
        let n = self.mft_record_size as u64 * 4;
        let mut buf = vec![0u8; n as usize];
        if self.mft_data_runs.is_empty() {
            let off = self.cluster_offset(self.mft_cluster);
            self.reader.seek(SeekFrom::Start(off))?;
            self.reader.read_exact(&mut buf)?;
        } else {
            self.read_mft_bytes(0, &mut buf)?;
        }
        Ok(buf)
    }

    /// Absolute byte offset (in the underlying reader) of `$MFTMirr`'s first
    /// record. `$MFTMirr` mirrors the first 4 MFT records at this LCN.
    pub(crate) fn fsck_mftmirr_offset(&self) -> u64 {
        self.cluster_offset(self.mft_mirror_cluster)
    }

    /// Read `n` raw bytes at absolute offset `off` in the underlying reader.
    pub(crate) fn fsck_read_raw(&mut self, off: u64, n: usize) -> Result<Vec<u8>, FilesystemError> {
        let mut buf = vec![0u8; n];
        self.reader.seek(SeekFrom::Start(off))?;
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Read the `$Volume` record (MFT #3) and return the `$VOLUME_INFORMATION`
    /// attribute's flags word (0 when absent). Bit 0 is `VolumeDirty`.
    pub(crate) fn fsck_volume_flags(&mut self) -> Result<u16, FilesystemError> {
        let record = self.read_mft_record(MFT_RECORD_VOLUME)?;
        let attrs = parse_mft_attributes(&record, self.mft_record_size);
        for a in &attrs {
            if a.attr_type == ATTR_VOLUME_INFORMATION && a.resident && a.value.len() >= 12 {
                return Ok(u16::from_le_bytes([a.value[10], a.value[11]]));
            }
        }
        Ok(0)
    }
}

/// Geometry snapshot handed to the fsck module. Kept `Copy` so `analyze()` can
/// stash it in its result without borrowing the filesystem.
#[derive(Debug, Clone, Copy)]
pub(crate) struct NtfsGeom {
    pub(crate) partition_offset: u64,
    pub(crate) bytes_per_sector: u64,
    pub(crate) cluster_size: u64,
    pub(crate) total_sectors: u64,
    pub(crate) total_clusters: u64,
    pub(crate) mft_cluster: u64,
    pub(crate) mft_record_size: u32,
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
            modified_unix: None,
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
            finder_flags: None,
            prodos_file_type: None,
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
        // The last used cluster plus the backup boot sector a resize rewrites behind it.
        let data_end = (last_cluster + 1) * self.cluster_size + self.bytes_per_sector;
        Ok(data_end.min(self.total_size()))
    }

    /// The packed (defragmenting-clone) target size: a fresh NTFS holding only
    /// the used data + the system files + an MFT sized for this volume's file
    /// count. Unlike `last_data_byte` (the in-place trim, pinned high by a lone
    /// allocated cluster on a fragmented volume), this is what the clone emits.
    /// See [`crate::fs::ntfs_format::ntfs_min_packed_size`] and `ntfs_clone`.
    fn defragmented_minimum_size(&mut self) -> Result<u64, FilesystemError> {
        let entries = self.mft_record_capacity().saturating_sub(24);
        // The defragmenting clone inherits the source volume's cluster size, so
        // size the target with the same cluster.
        Ok(crate::fs::ntfs_format::ntfs_min_packed_size(
            self.used_bytes,
            entries,
            self.cluster_size,
        ))
    }

    fn fsck(&mut self) -> Option<Result<super::fsck::FsckResult, FilesystemError>> {
        Some(super::ntfs_fsck::fsck_ntfs(self))
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

        // Both fields are signed: a length whose top bit lands in the high byte
        // reads back negative and Windows calls the whole file corrupt, so a
        // 128-cluster run must be `12 80 00`, never `11 80`.
        let length_size = min_signed_bytes(length as i64);
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

/// Fallback NTFS timestamp (2024-01-01 00:00:00 UTC), used only when the host
/// clock reads before the Unix epoch. 100-nanosecond intervals since 1601-01-01.
const FIXED_NTFS_TIMESTAMP: u64 = 133_480_416_000_000_000;

/// Seconds between the NTFS epoch (1601-01-01) and the Unix epoch (1970-01-01).
const NTFS_EPOCH_OFFSET_SECS: u64 = 11_644_473_600;

/// Wall-clock now, as NTFS 100 ns intervals since 1601-01-01.
fn now_ntfs_timestamp() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => {
            (d.as_secs() + NTFS_EPOCH_OFFSET_SECS) * 10_000_000 + (d.subsec_nanos() / 100) as u64
        }
        Err(_) => FIXED_NTFS_TIMESTAMP,
    }
}

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
fn prepare_fixup(record: &mut [u8]) {
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
        // One USA entry per 512-byte NTFS block (see apply_fixup).
        let sector_end = i * NTFS_BLOCK_SIZE;
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

/// Build a $STANDARD_INFORMATION value: 72 bytes with a `security_id` on NTFS 3.x, else 48.
///
/// 3.0 moved ACLs into `$Secure`, keyed by that id; the 48-byte 1.2 record has no field for it and
/// carries a per-file `$SECURITY_DESCRIPTOR` instead. Writing the 1.2 form on a 3.x volume leaves
/// Windows unable to resolve the ACL at all.
fn build_standard_information(file_attrs: u32, security_id: Option<u32>, when: u64) -> Vec<u8> {
    let Some(security_id) = security_id else {
        let mut data = vec![0u8; 48];
        let ts = when.to_le_bytes();
        for i in 0..4 {
            data[i * 8..i * 8 + 8].copy_from_slice(&ts);
        }
        data[0x20..0x24].copy_from_slice(&file_attrs.to_le_bytes());
        return data;
    };
    let mut data = vec![0u8; 72];
    let ts = when.to_le_bytes();
    data[0..8].copy_from_slice(&ts); // creation time
    data[8..16].copy_from_slice(&ts); // modification time
    data[16..24].copy_from_slice(&ts); // MFT modification time
    data[24..32].copy_from_slice(&ts); // access time
    data[0x20..0x24].copy_from_slice(&file_attrs.to_le_bytes());
    data[0x34..0x38].copy_from_slice(&security_id.to_le_bytes());
    data
}

/// True when `name` is usable verbatim as a DOS 8.3 name (namespace 3);
/// Windows stores long names as WIN32-only instead of lying about it.
fn is_valid_dos_name(name: &str) -> bool {
    let mut parts = name.split('.');
    let base = parts.next().unwrap_or("");
    let ext = parts.next().unwrap_or("");
    if parts.next().is_some() || base.is_empty() || base.len() > 8 || ext.len() > 3 {
        return false;
    }
    let ok = |c: char| c.is_ascii_alphanumeric() || "$%'-_@~`!(){}^#&".contains(c);
    base.chars().all(ok) && ext.chars().all(ok)
}

/// Build a $FILE_NAME attribute value.
fn build_file_name_attr(
    parent_ref: u64,
    name: &str,
    is_dir: bool,
    size: u64,
    when: u64,
) -> Vec<u8> {
    let utf16: Vec<u16> = name.encode_utf16().collect();
    let name_bytes = utf16.len() * 2;
    let data_len = 66 + name_bytes;
    let mut data = vec![0u8; data_len];

    // Parent MFT reference (6 bytes ref + 2 bytes sequence number = 0)
    data[0..8].copy_from_slice(&parent_ref.to_le_bytes());

    let ts = when.to_le_bytes();
    data[8..16].copy_from_slice(&ts); // creation
    data[16..24].copy_from_slice(&ts); // modification
    data[24..32].copy_from_slice(&ts); // MFT modification
    data[32..40].copy_from_slice(&ts); // access

    // Allocated size as for resident data (quadword-rounded); a caller with
    // non-resident data overrides it with the cluster-rounded size.
    data[40..48].copy_from_slice(&((size + 7) & !7).to_le_bytes());
    // real size
    data[48..56].copy_from_slice(&size.to_le_bytes());

    // Windows puts the directory bit in $FILE_NAME only, never in $STANDARD_INFORMATION.
    let flags: u32 = if is_dir {
        FILE_ATTR_DIRECTORY
    } else {
        FILE_ATTR_ARCHIVE
    };
    data[56..60].copy_from_slice(&flags.to_le_bytes());

    // reparse = 0 (bytes 60..64 already zero)

    // name length
    data[64] = utf16.len() as u8;
    // Win32+DOS when the name is a valid 8.3 name; otherwise POSIX, the namespace
    // Windows itself uses with 8.3 creation off. A lone Win32 name fails chkdsk.
    data[65] = if is_valid_dos_name(name) { 0x03 } else { 0x00 };

    // UTF-16LE name
    for (i, &ch) in utf16.iter().enumerate() {
        data[66 + i * 2] = ch as u8;
        data[66 + i * 2 + 1] = (ch >> 8) as u8;
    }

    data
}

/// Repack a self-relative security descriptor compactly: mkntfs stores the
/// root's with a padded 4 KiB DACL, far too big to inherit resident.
/// None when the blob is malformed or carries a SACL (never ours).
fn repack_security_descriptor(sd: &[u8]) -> Option<Vec<u8>> {
    if sd.len() < 20 {
        return None;
    }
    let off = |i: usize| u32::from_le_bytes([sd[i], sd[i + 1], sd[i + 2], sd[i + 3]]) as usize;
    let (o_own, o_grp, o_sacl, o_dacl) = (off(4), off(8), off(12), off(16));
    if o_sacl != 0 || o_own == 0 || o_grp == 0 || o_dacl == 0 {
        return None;
    }
    let sid = |o: usize| -> Option<&[u8]> {
        let count = *sd.get(o + 1)? as usize;
        sd.get(o..o + 8 + count * 4)
    };
    let owner = sid(o_own)?;
    let group = sid(o_grp)?;

    // Walk the DACL's ACEs by their own size fields; the ACL header's size
    // includes slack we drop.
    let ace_count = u16::from_le_bytes([*sd.get(o_dacl + 4)?, *sd.get(o_dacl + 5)?]) as usize;
    let mut aces: Vec<u8> = Vec::new();
    let mut pos = o_dacl + 8;
    for _ in 0..ace_count {
        let size = u16::from_le_bytes([*sd.get(pos + 2)?, *sd.get(pos + 3)?]) as usize;
        if size < 8 {
            return None;
        }
        aces.extend_from_slice(sd.get(pos..pos + size)?);
        pos += size;
    }
    let acl_len = 8 + aces.len();

    let mut out = vec![0u8; 20];
    out[0] = sd[0]; // revision
    out[2..4].copy_from_slice(&sd[2..4]); // control
    out[16..20].copy_from_slice(&20u32.to_le_bytes()); // DACL right after header
                                                       // ACL header: revision(1) sbz1(1) size(2) ace_count(2) sbz2(2).
    out.extend_from_slice(&sd[o_dacl..o_dacl + 2]);
    out.extend_from_slice(&(acl_len as u16).to_le_bytes());
    out.extend_from_slice(&(ace_count as u16).to_le_bytes());
    out.extend_from_slice(&[0, 0]);
    out.extend_from_slice(&aces);
    let o = out.len() as u32;
    out[4..8].copy_from_slice(&o.to_le_bytes()); // owner offset
    out.extend_from_slice(owner);
    let g = out.len() as u32;
    out[8..12].copy_from_slice(&g.to_le_bytes()); // group offset
    out.extend_from_slice(group);
    Some(out)
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

/// Sub-node pointer flag on an index entry (trailing 8-byte VCN present).
const INDEX_ENTRY_NODE: u32 = 0x01;
/// Index node header flag: this node has children (a "large" index).
const INDEX_NODE_HAS_CHILDREN: u8 = 0x01;

/// The `clusters_per_index_block` byte for an `$INDEX_ROOT` header; a sector
/// count when the block is smaller than a cluster (same rule as the formatter).
fn idx_clusters_per_block_byte(index_block_size: u32, cluster_size: u64, sector_size: u64) -> u8 {
    if index_block_size as u64 >= cluster_size {
        (index_block_size as u64 / cluster_size) as u8
    } else {
        (index_block_size as u64 / sector_size.max(512)) as u8
    }
}

/// VCN units spanned by one index block: clusters normally, 512-byte blocks
/// when the cluster is larger than the index block (ntfs-3g's vcn_size rule).
fn idx_vcn_units_per_block(index_block_size: u32, cluster_size: u64) -> u64 {
    if cluster_size <= index_block_size as u64 {
        index_block_size as u64 / cluster_size
    } else {
        index_block_size as u64 / 512
    }
}

/// Byte offset of a sub-node VCN within the $INDEX_ALLOCATION stream.
fn idx_vcn_to_stream_offset(vcn: u64, index_block_size: u32, cluster_size: u64) -> u64 {
    if cluster_size <= index_block_size as u64 {
        vcn * cluster_size
    } else {
        vcn * 512
    }
}

/// $INDEX_ROOT value: 16-byte root header + node header + a lone end sentinel.
/// `large` selects the has-children form whose end entry carries sub-node VCN 0.
fn build_empty_index_root(
    index_block_size: u32,
    cluster_size: u64,
    sector_size: u64,
    large: bool,
) -> Vec<u8> {
    let end_entry_size: u32 = if large { 24 } else { 16 };
    let entries_total = 0x10 + end_entry_size;

    let mut data = vec![0u8; 16 + entries_total as usize];
    data[0..4].copy_from_slice(&ATTR_FILE_NAME.to_le_bytes()); // indexed attr type
    data[4..8].copy_from_slice(&1u32.to_le_bytes()); // collation rule
    data[8..12].copy_from_slice(&index_block_size.to_le_bytes());
    data[12] = idx_clusters_per_block_byte(index_block_size, cluster_size, sector_size);

    // Index node header (at offset 16)
    let node = 16;
    data[node..node + 4].copy_from_slice(&0x10u32.to_le_bytes()); // entries offset
    data[node + 4..node + 8].copy_from_slice(&entries_total.to_le_bytes()); // index_used
    data[node + 8..node + 12].copy_from_slice(&entries_total.to_le_bytes()); // index_allocated
    if large {
        data[node + 12] = INDEX_NODE_HAS_CHILDREN;
    }

    // End sentinel entry (at node + 0x10); sub-node VCN 0 trails it when large.
    let entry = node + 0x10;
    data[entry + 8..entry + 10].copy_from_slice(&(end_entry_size as u16).to_le_bytes());
    let flags = INDEX_ENTRY_END | if large { INDEX_ENTRY_NODE } else { 0 };
    data[entry + 12..entry + 16].copy_from_slice(&flags.to_le_bytes());

    data
}

/// Build a resident attribute header + data, padded to 8-byte alignment.
fn build_resident_attr(attr_type: u32, data: &[u8]) -> Vec<u8> {
    build_named_resident_attr(attr_type, "", data)
}

/// A directory's index attributes are *named* `$I30`; Windows looks the index up
/// by that name and treats a nameless `$INDEX_ROOT` as no index at all.
fn build_named_resident_attr(attr_type: u32, name: &str, data: &[u8]) -> Vec<u8> {
    let name_utf16: Vec<u16> = name.encode_utf16().collect();
    let name_offset = 0x18usize;
    let name_bytes = name_utf16.len() * 2;
    let value_offset = ((name_offset + name_bytes) + 7) & !7;
    let total = (value_offset + data.len() + 7) & !7; // 8-byte aligned
    let mut attr = vec![0u8; total];
    attr[0..4].copy_from_slice(&attr_type.to_le_bytes());
    attr[4..8].copy_from_slice(&(total as u32).to_le_bytes());
    // non-resident flag = 0 (resident); flags = 0
    attr[9] = name_utf16.len() as u8; // name length, in characters
    if name_bytes > 0 {
        attr[0x0A..0x0C].copy_from_slice(&(name_offset as u16).to_le_bytes());
        for (i, ch) in name_utf16.iter().enumerate() {
            attr[name_offset + i * 2..name_offset + i * 2 + 2].copy_from_slice(&ch.to_le_bytes());
        }
    }
    attr[0x10..0x14].copy_from_slice(&(data.len() as u32).to_le_bytes()); // value length
    attr[0x14..0x16].copy_from_slice(&(value_offset as u16).to_le_bytes());
    // Windows flags every $FILE_NAME as indexed (it lives in the parent's $I30 too).
    if attr_type == ATTR_FILE_NAME {
        attr[0x16] = 1;
    }
    attr[value_offset..value_offset + data.len()].copy_from_slice(data);
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
fn assemble_mft_record(
    attrs: &[Vec<u8>],
    flags: u16,
    record_size: u32,
    record_num: u64,
    seq: u16,
) -> Vec<u8> {
    let mut record = vec![0u8; record_size as usize];

    // FILE magic
    record[0..4].copy_from_slice(b"FILE");
    // Fixup offset = 0x30
    record[0x04..0x06].copy_from_slice(&0x0030u16.to_le_bytes());
    // One USN entry per 512-byte NTFS block (1024-byte record -> 3 entries).
    let fixup_count = (record_size / NTFS_BLOCK_SIZE as u32 + 1) as u16;
    record[0x06..0x08].copy_from_slice(&fixup_count.to_le_bytes());
    // Log file sequence = 0 (offset 0x08..0x10)
    record[0x10..0x12].copy_from_slice(&seq.max(1).to_le_bytes());
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
    // NTFS 3.1 records carry their own index here; chkdsk rejects the record without it.
    record[0x2C..0x30].copy_from_slice(&(record_num as u32).to_le_bytes());

    // Write attributes
    let mut pos = first_attr_aligned;
    for (id, attr) in attrs.iter().enumerate() {
        if pos + attr.len() + 4 > record_size as usize {
            break; // shouldn't happen if record_size is adequate
        }
        record[pos..pos + attr.len()].copy_from_slice(attr);
        // Instance ids must be unique within the record and below next_attribute_id above.
        record[pos + 0x0E..pos + 0x10].copy_from_slice(&(id as u16).to_le_bytes());
        pos += attr.len();
    }

    // End marker; Windows counts it as eight bytes in the used size, and chkdsk
    // corrects a record whose first free byte sits four bytes early.
    if pos + 8 <= record_size as usize {
        record[pos..pos + 4].copy_from_slice(&ATTR_END.to_le_bytes());
        pos += 8;
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
        prepare_fixup(record);
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
            if run.sparse || run.cluster_offset < 0 {
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
    /// Allocate an MFT record; returns (record number, sequence number).
    /// A reused record keeps its bumped sequence so stale references stay stale.
    fn allocate_mft_record(&mut self) -> Result<(u64, u16), FilesystemError> {
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

                        let seq = self
                            .read_mft_record(record_num)
                            .ok()
                            .filter(|r| &r[0..4] == b"FILE")
                            .map(|r| u16::from_le_bytes([r[0x10], r[0x11]]))
                            .filter(|&s| s != 0)
                            .unwrap_or(1);

                        // Initialize blank MFT record
                        let mut blank = vec![0u8; self.mft_record_size as usize];
                        blank[0..4].copy_from_slice(b"FILE");
                        blank[0x04..0x06].copy_from_slice(&0x0030u16.to_le_bytes());
                        let fixup_count =
                            (self.mft_record_size / NTFS_BLOCK_SIZE as u32 + 1) as u16;
                        blank[0x06..0x08].copy_from_slice(&fixup_count.to_le_bytes());
                        blank[0x10..0x12].copy_from_slice(&seq.to_le_bytes());
                        let first_attr = (0x30 + fixup_count as usize * 2 + 7) & !7;
                        blank[0x14..0x16].copy_from_slice(&(first_attr as u16).to_le_bytes());
                        blank[0x18..0x1C].copy_from_slice(&((first_attr + 4) as u32).to_le_bytes()); // used size
                        blank[0x1C..0x20].copy_from_slice(&self.mft_record_size.to_le_bytes());
                        // End marker
                        blank[first_attr..first_attr + 4].copy_from_slice(&ATTR_END.to_le_bytes());

                        self.write_mft_record(record_num, &mut blank)?;
                        return Ok((record_num, seq));
                    }
                }
            }
        }

        Err(FilesystemError::DiskFull(
            "no free MFT records available".into(),
        ))
    }

    /// The `$FILE_NAME`s in `parent` that spell `name`, DOS alias included.
    fn names_for(names: &[FileNameAttr], parent: u64, name: &str) -> Vec<usize> {
        let lower = name.to_lowercase();
        let has_long = names
            .iter()
            .any(|n| n.parent == parent && n.namespace != 2 && n.name.to_lowercase() == lower);
        names
            .iter()
            .enumerate()
            .filter(|(_, n)| {
                n.parent == parent
                    && (n.name.to_lowercase() == lower || (has_long && n.namespace == 2))
            })
            .map(|(i, _)| i)
            .collect()
    }

    /// Drop only this name when the record has other hard links; true if it did.
    fn unlink_one_name(
        &mut self,
        record_number: u64,
        parent_record_num: u64,
        name: &str,
    ) -> Result<bool, FilesystemError> {
        let Ok(mut record) = self.read_mft_record(record_number) else {
            return Ok(false);
        };
        let names = file_name_attrs(&record);
        let ours = Self::names_for(&names, parent_record_num, name);
        if ours.is_empty() || ours.len() == names.len() {
            return Ok(false);
        }
        for &i in &ours {
            self.remove_alias_or_name(parent_record_num, &names[i])?;
        }
        for &i in ours.iter().rev() {
            remove_attr_at(&mut record, names[i].pos)?;
        }
        let remaining = (names.len() - ours.len()) as u16;
        record[0x12..0x14].copy_from_slice(&remaining.to_le_bytes());
        self.write_mft_record(record_number, &mut record)?;
        Ok(true)
    }

    /// Remove one name's index entry; a DOS alias with no entry is not an error.
    fn remove_alias_or_name(
        &mut self,
        parent_record_num: u64,
        name: &FileNameAttr,
    ) -> Result<(), FilesystemError> {
        match self.remove_index_entry(parent_record_num, &name.name) {
            Err(FilesystemError::NotFound(_)) if name.namespace == 2 => Ok(()),
            other => other,
        }
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

        // Mark record as not-in-use; bump the sequence so old references go stale.
        let mut record = self
            .read_mft_record(record_number)
            .unwrap_or_else(|_| vec![0u8; self.mft_record_size as usize]);
        if &record[0..4] == b"FILE" {
            record[0x16] = 0;
            record[0x17] = 0;
            let seq = u16::from_le_bytes([record[0x10], record[0x11]])
                .wrapping_add(1)
                .max(1);
            record[0x10..0x12].copy_from_slice(&seq.to_le_bytes());
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

    /// Copy `data_len` bytes into freshly allocated `runs` 1 MiB at a time; the
    /// source is never held whole in memory (CONTRIBUTING streaming rule).
    fn stream_into_runs(
        &mut self,
        runs: &[(u64, u64)],
        data: &mut dyn std::io::Read,
        data_len: u64,
    ) -> Result<(), FilesystemError> {
        let mut buf = vec![0u8; 1024 * 1024];
        let mut written = 0u64;
        for &(start_cluster, length) in runs {
            let offset = self.cluster_offset(start_cluster);
            self.reader.seek(SeekFrom::Start(offset))?;
            let run_bytes = length * self.cluster_size;
            let to_write = run_bytes.min(data_len - written);
            let mut left = to_write;
            while left > 0 {
                let n = (buf.len() as u64).min(left) as usize;
                data.read_exact(&mut buf[..n])
                    .map_err(FilesystemError::Io)?;
                self.reader.write_all(&buf[..n])?;
                left -= n as u64;
            }
            // Zero the slack of the last cluster so stale bytes never sit past EOF.
            let mut pad = run_bytes - to_write;
            while pad > 0 {
                let n = (buf.len() as u64).min(pad) as usize;
                buf[..n].fill(0);
                self.reader.write_all(&buf[..n])?;
                pad -= n as u64;
            }
            written += to_write;
        }
        Ok(())
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

    /// Bring `$Bitmap` in step with a volume that grew or shrank from
    /// `old_total_sectors` to `new_total_sectors` (both excluding the backup boot sector).
    fn resize_volume_bitmap(
        &mut self,
        old_total_sectors: u64,
        new_total_sectors: u64,
    ) -> Result<(), FilesystemError> {
        let bps = self.bytes_per_sector;
        let cs = self.cluster_size;
        let old_vc = old_total_sectors * bps / cs;
        let new_vc = new_total_sectors * bps / cs;
        // Windows sizes the bitmap in whole quadwords and chkdsk holds it to that.
        let needed = ((new_vc as usize).div_ceil(64) * 8).max(8);

        let (old_bitmap, old_runs) = self.read_volume_bitmap()?;
        let mut bitmap = old_bitmap;
        bitmap.resize(needed, 0);
        // Clusters the volume gained are free; everything past its end is off limits.
        for c in old_vc.min(new_vc)..new_vc {
            bitmap[(c / 8) as usize] &= !(1 << (c % 8));
        }
        for c in new_vc..(needed as u64 * 8) {
            bitmap[(c / 8) as usize] |= 1 << (c % 8);
        }

        let mut record = self.read_mft_record(MFT_RECORD_BITMAP)?;
        let (pos, _) = find_attr_pos(&record, ATTR_DATA, false)
            .ok_or_else(|| FilesystemError::Parse("$Bitmap $DATA attribute not found".into()))?;
        let instance = [record[pos + 0x0E], record[pos + 0x0F]];
        let allocated_clusters: u64 = old_runs
            .iter()
            .filter(|r| !r.sparse)
            .map(|r| r.length)
            .sum();

        if needed as u64 <= allocated_clusters * cs {
            // Slack past the real size reads as "in use" should anything ignore the size.
            let mut padded = bitmap;
            padded.resize((allocated_clusters * cs) as usize, 0xFF);
            self.write_volume_bitmap(&padded, &old_runs)?;
            record[pos + 0x30..pos + 0x38].copy_from_slice(&(needed as u64).to_le_bytes());
            record[pos + 0x38..pos + 0x40].copy_from_slice(&(needed as u64).to_le_bytes());
            return self.write_mft_record(MFT_RECORD_BITMAP, &mut record);
        }

        // The bitmap outgrew its clusters: give it a fresh run, then release the old one.
        let want = (needed as u64).div_ceil(cs);
        let new_runs = find_free_cluster_runs(&bitmap, new_vc, want)?;
        for &(start, len) in &new_runs {
            for c in start..start + len {
                bitmap[(c / 8) as usize] |= 1 << (c % 8);
            }
        }
        for run in old_runs
            .iter()
            .filter(|r| !r.sparse && r.cluster_offset >= 0)
        {
            let start = run.cluster_offset as u64;
            for c in start..(start + run.length).min(new_vc) {
                bitmap[(c / 8) as usize] &= !(1 << (c % 8));
            }
        }
        let runs: Vec<DataRun> = new_runs
            .iter()
            .map(|&(start, len)| DataRun {
                cluster_offset: start as i64,
                length: len,
                sparse: false,
            })
            .collect();
        let mut padded = bitmap;
        padded.resize((want * cs) as usize, 0xFF);
        self.write_volume_bitmap(&padded, &runs)?;
        let mut attr =
            build_named_nonresident_attr(ATTR_DATA, "", &new_runs, want * cs, needed as u64);
        attr[0x0E..0x10].copy_from_slice(&instance);
        replace_attr_at(&mut record, pos, &attr)?;
        self.write_mft_record(MFT_RECORD_BITMAP, &mut record)
    }

    /// Read parent directory's security descriptor, or build a default one.
    /// If the parent's SD is too large to fit as a resident attribute, uses a minimal default.
    /// Inherit the parent's `$Secure` entry: its descriptor is already registered, so no new one
    /// is written. Falls back to the id every formatter registers when the parent predates NTFS 3.
    fn read_parent_security_id(&mut self, parent_record_num: u64) -> u32 {
        if let Ok(record) = self.read_mft_record(parent_record_num) {
            for attr in &parse_mft_attributes(&record, self.mft_record_size) {
                if attr.attr_type == ATTR_STANDARD_INFORMATION {
                    if let Ok(v) = self.read_attribute_data(attr, None) {
                        if v.len() >= 0x38 {
                            let id = u32::from_le_bytes([v[0x34], v[0x35], v[0x36], v[0x37]]);
                            if id != 0 {
                                return id;
                            }
                        }
                    }
                    break;
                }
            }
        }
        DEFAULT_SECURITY_ID
    }

    /// The parent's own $SECURITY_DESCRIPTOR value, trimmed to its effective
    /// length. None when the parent carries no SD attribute (3.x volumes
    /// normally express ACLs through $Secure ids instead), or it cannot fit.
    fn parent_sd_attr_value(&mut self, parent_record_num: u64) -> Option<Vec<u8>> {
        // Max SD size that will fit as a resident attr in a 1024-byte record
        // alongside other attributes (leave ~600 bytes for other attrs + header)
        let max_sd_size = (self.mft_record_size as usize).saturating_sub(600);
        let record = self.read_mft_record(parent_record_num).ok()?;
        let attrs = parse_mft_attributes(&record, self.mft_record_size);
        for attr in &attrs {
            if attr.attr_type == ATTR_SECURITY_DESCRIPTOR {
                let raw = self.read_attribute_data(attr, None).ok()?;
                let sd = repack_security_descriptor(&raw)?;
                if sd.len() <= max_sd_size {
                    return Some(sd);
                }
                return None;
            }
        }
        None
    }

    fn read_parent_security_descriptor(
        &mut self,
        parent_record_num: u64,
    ) -> Result<Vec<u8>, FilesystemError> {
        Ok(self
            .parent_sd_attr_value(parent_record_num)
            .unwrap_or_else(build_default_security_descriptor))
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
    /// An NTFS file reference: 48-bit record number plus the record's own
    /// sequence number on top. A parent reference carrying a zero sequence looks
    /// stale to Windows, which prunes the entry when it self-heals on mount.
    fn file_reference(&mut self, record_num: u64) -> u64 {
        let seq = self
            .read_mft_record(record_num)
            .ok()
            .map(|r| u16::from_le_bytes([r[0x10], r[0x11]]))
            .unwrap_or(1);
        (record_num & 0x0000_FFFF_FFFF_FFFF) | ((seq as u64) << 48)
    }

    fn insert_index_entry(
        &mut self,
        parent_record_num: u64,
        entry_bytes: &[u8],
    ) -> Result<(), FilesystemError> {
        let mut record = self.read_mft_record(parent_record_num)?;
        let record_size = self.mft_record_size;

        let root = parse_root_node(&record, self.index_record_size).ok_or_else(|| {
            FilesystemError::Parse("directory record has no resident $INDEX_ROOT".into())
        })?;

        if !root.large {
            if self.try_splice_into_root(&mut record, entry_bytes, record_size)? {
                self.write_mft_record(parent_record_num, &mut record)?;
                return Ok(());
            }
            // Small root is full: move its entries out into a fresh INDX block.
            self.promote_index_root(&mut record)?;
        }
        self.insert_into_large_index(parent_record_num, &mut record, entry_bytes)
    }

    /// Splice an entry into the resident $INDEX_ROOT node at its sorted
    /// position (leaf entries and pushed-up separators alike). False = no room.
    fn try_splice_into_root(
        &self,
        record: &mut [u8],
        entry_bytes: &[u8],
        record_size: u32,
    ) -> Result<bool, FilesystemError> {
        let Some(root) = parse_root_node(record, self.index_record_size) else {
            return Ok(false);
        };
        let used_size = record_used_size(record);
        if used_size + entry_bytes.len() > record_size as usize {
            return Ok(false);
        }

        let insert_pos = self
            .find_index_insert_position(&record[root.entries_start..root.entries_end], entry_bytes);
        let abs_insert = root.entries_start + insert_pos;

        record.copy_within(abs_insert..used_size, abs_insert + entry_bytes.len());
        record[abs_insert..abs_insert + entry_bytes.len()].copy_from_slice(entry_bytes);

        let grow = entry_bytes.len();
        let node_start = root.node_start;
        let new_node_used = (root.entries_end - node_start + grow) as u32;
        record[node_start + 4..node_start + 8].copy_from_slice(&new_node_used.to_le_bytes());
        // Resident node: allocated tracks used.
        record[node_start + 8..node_start + 12].copy_from_slice(&new_node_used.to_le_bytes());
        record[root.attr_pos + 0x10..root.attr_pos + 0x14]
            .copy_from_slice(&((root.value_len + grow) as u32).to_le_bytes());
        record[root.attr_pos + 4..root.attr_pos + 8]
            .copy_from_slice(&((root.attr_len + grow) as u32).to_le_bytes());
        set_record_used_size(record, used_size + grow);
        Ok(true)
    }

    /// Remove `len` bytes at `abs_off` from the resident $INDEX_ROOT node,
    /// shrinking the attribute and shifting the record tail.
    fn splice_out_of_root(&self, record: &mut [u8], abs_off: usize, len: usize) {
        let Some(root) = parse_root_node(record, self.index_record_size) else {
            return;
        };
        let used_size = record_used_size(record);
        record.copy_within(abs_off + len..used_size, abs_off);
        record[used_size - len..used_size].fill(0);

        let node_start = root.node_start;
        let new_node_used = (root.entries_end - node_start - len) as u32;
        record[node_start + 4..node_start + 8].copy_from_slice(&new_node_used.to_le_bytes());
        record[node_start + 8..node_start + 12].copy_from_slice(&new_node_used.to_le_bytes());
        record[root.attr_pos + 0x10..root.attr_pos + 0x14]
            .copy_from_slice(&((root.value_len - len) as u32).to_le_bytes());
        record[root.attr_pos + 4..root.attr_pos + 8]
            .copy_from_slice(&((root.attr_len - len) as u32).to_le_bytes());
        set_record_used_size(record, used_size - len);
    }

    /// Convert a fresh cluster allocation to the DataRun form used by
    /// write_data_to_runs.
    fn runs_to_data_runs(runs: &[(u64, u64)]) -> Vec<DataRun> {
        runs.iter()
            .map(|&(start, len)| DataRun {
                cluster_offset: start as i64,
                length: len,
                sparse: false,
            })
            .collect()
    }

    /// Turn a full small $INDEX_ROOT into a large one: the resident entries
    /// move into a newly allocated INDX block 0, and the record gains
    /// $INDEX_ALLOCATION + $BITMAP attributes (both named $I30).
    fn promote_index_root(&mut self, record: &mut [u8]) -> Result<(), FilesystemError> {
        let root = parse_root_node(record, self.index_record_size).ok_or_else(|| {
            FilesystemError::Parse("directory record has no resident $INDEX_ROOT".into())
        })?;
        if root.large {
            return Ok(());
        }
        let block_size = root.block_size;

        // Everything before the end sentinel moves to the INDX block.
        let region = &record[root.entries_start..root.entries_end];
        let end_off = entries_end_sentinel_offset(region);
        let moved: Vec<u8> = region[..end_off].to_vec();

        let clusters = (block_size as u64).div_ceil(self.cluster_size).max(1);
        let runs = self.allocate_volume_clusters(clusters as u32)?;
        let mut block = build_indx_block(block_size, 0, &moved, &leaf_end_sentinel())?;
        prepare_fixup(&mut block);
        self.write_data_to_runs(&Self::runs_to_data_runs(&runs), &block)?;

        // Swap the root value for the large-empty form, keeping the instance id.
        let instance =
            u16::from_le_bytes([record[root.attr_pos + 0x0E], record[root.attr_pos + 0x0F]]);
        let mut new_root = build_named_resident_attr(
            ATTR_INDEX_ROOT,
            "$I30",
            &build_empty_index_root(block_size, self.cluster_size, self.bytes_per_sector, true),
        );
        new_root[0x0E..0x10].copy_from_slice(&instance.to_le_bytes());
        replace_attr_at(record, root.attr_pos, &new_root)?;

        let root = parse_root_node(record, self.index_record_size).ok_or_else(|| {
            FilesystemError::Parse("promoted $INDEX_ROOT vanished from record".into())
        })?;
        let insert_at = root.attr_pos + root.attr_len;
        let next_instance = u16::from_le_bytes([record[0x28], record[0x29]]);

        let alloc_bytes = clusters * self.cluster_size;
        let mut alloc_attr = build_named_nonresident_attr(
            ATTR_INDEX_ALLOCATION,
            "$I30",
            &runs,
            alloc_bytes,
            block_size as u64,
        );
        alloc_attr[0x0E..0x10].copy_from_slice(&next_instance.to_le_bytes());

        let mut bitmap_value = [0u8; 8];
        bitmap_value[0] = 1;
        let mut bitmap_attr = build_named_resident_attr(ATTR_BITMAP, "$I30", &bitmap_value);
        bitmap_attr[0x0E..0x10].copy_from_slice(&(next_instance + 1).to_le_bytes());

        insert_attr_at(record, insert_at, &alloc_attr)?;
        insert_attr_at(record, insert_at + alloc_attr.len(), &bitmap_attr)?;
        record[0x28..0x2A].copy_from_slice(&(next_instance + 2).to_le_bytes());
        Ok(())
    }

    /// Read the whole $I30 $INDEX_ALLOCATION stream of a directory record.
    fn read_i30_allocation(&mut self, record: &[u8]) -> Result<Vec<u8>, FilesystemError> {
        let attrs = parse_mft_attributes(record, self.mft_record_size);
        for attr in &attrs {
            if attr.attr_type == ATTR_INDEX_ALLOCATION && !attr.resident {
                return self.read_attribute_data(attr, None);
            }
        }
        Err(FilesystemError::Parse(
            "large index has no $INDEX_ALLOCATION attribute".into(),
        ))
    }

    /// Write the whole $I30 $INDEX_ALLOCATION stream back through its runs.
    fn write_i30_allocation(
        &mut self,
        record: &[u8],
        stream: &[u8],
    ) -> Result<(), FilesystemError> {
        let attrs = parse_mft_attributes(record, self.mft_record_size);
        for attr in &attrs {
            if attr.attr_type == ATTR_INDEX_ALLOCATION && !attr.resident {
                return self.write_data_to_runs(&attr.data_runs, stream);
            }
        }
        Err(FilesystemError::Parse(
            "large index has no $INDEX_ALLOCATION attribute".into(),
        ))
    }

    fn block_index_to_vcn(&self, index: u64, block_size: u32) -> u64 {
        index * idx_vcn_units_per_block(block_size, self.cluster_size)
    }

    fn vcn_to_block_index(&self, vcn: u64, block_size: u32) -> Result<u64, FilesystemError> {
        let off = idx_vcn_to_stream_offset(vcn, block_size, self.cluster_size);
        if !off.is_multiple_of(block_size as u64) {
            return Err(FilesystemError::Parse(format!(
                "index sub-node VCN {vcn} is not block-aligned"
            )));
        }
        Ok(off / block_size as u64)
    }

    /// Grow $INDEX_ALLOCATION by one index block (clusters, sizes, bitmap bit,
    /// zero-filled stream tail). Returns the new block's index.
    fn append_index_block(
        &mut self,
        record: &mut [u8],
        stream: &mut Vec<u8>,
        block_size: u32,
    ) -> Result<u64, FilesystemError> {
        let new_index = (stream.len() / block_size as usize) as u64;
        let clusters = (block_size as u64).div_ceil(self.cluster_size).max(1);
        let new_runs = self.allocate_volume_clusters(clusters as u32)?;

        // Rebuild the $INDEX_ALLOCATION attribute with the extended run list.
        let (attr_pos, _) =
            find_attr_pos(record, ATTR_INDEX_ALLOCATION, false).ok_or_else(|| {
                FilesystemError::Parse("large index has no $INDEX_ALLOCATION attribute".into())
            })?;
        let instance = u16::from_le_bytes([record[attr_pos + 0x0E], record[attr_pos + 0x0F]]);
        let attrs = parse_mft_attributes(record, self.mft_record_size);
        let old = attrs
            .iter()
            .find(|a| a.attr_type == ATTR_INDEX_ALLOCATION && !a.resident)
            .ok_or_else(|| {
                FilesystemError::Parse("large index has no $INDEX_ALLOCATION attribute".into())
            })?;
        let mut all_runs: Vec<(u64, u64)> = old
            .data_runs
            .iter()
            .filter(|r| !r.sparse && r.cluster_offset >= 0)
            .map(|r| (r.cluster_offset as u64, r.length))
            .collect();
        all_runs.extend_from_slice(&new_runs);
        let new_alloc = old.allocated_size + clusters * self.cluster_size;
        let new_real = old.real_size + block_size as u64;
        let mut new_attr = build_named_nonresident_attr(
            ATTR_INDEX_ALLOCATION,
            "$I30",
            &all_runs,
            new_alloc,
            new_real,
        );
        new_attr[0x0E..0x10].copy_from_slice(&instance.to_le_bytes());
        replace_attr_at(record, attr_pos, &new_attr)?;

        self.set_i30_bitmap_bit(record, new_index)?;
        stream.resize(stream.len() + block_size as usize, 0);
        Ok(new_index)
    }

    /// Set (or clear) bit `index` in the directory's $I30 $BITMAP.
    fn set_i30_bitmap_bit_value(
        &mut self,
        record: &mut [u8],
        index: u64,
        value: bool,
    ) -> Result<(), FilesystemError> {
        let byte = (index / 8) as usize;
        let bit = (index % 8) as u8;
        let Some((attr_pos, _)) = find_attr_pos(record, ATTR_BITMAP, true) else {
            // Non-resident $I30 bitmap (huge Windows-made directory).
            let attrs = parse_mft_attributes(record, self.mft_record_size);
            for attr in &attrs {
                if attr.attr_type == ATTR_BITMAP && !attr.resident {
                    let mut data = self.read_attribute_data(attr, None)?;
                    if byte >= data.len() {
                        return Err(FilesystemError::DiskFull(
                            "directory index bitmap is full".into(),
                        ));
                    }
                    if value {
                        data[byte] |= 1 << bit;
                    } else {
                        data[byte] &= !(1 << bit);
                    }
                    return self.write_data_to_runs(&attr.data_runs, &data);
                }
            }
            return Err(FilesystemError::Parse(
                "large index has no $I30 $BITMAP attribute".into(),
            ));
        };

        let instance = u16::from_le_bytes([record[attr_pos + 0x0E], record[attr_pos + 0x0F]]);
        let value_off =
            u16::from_le_bytes([record[attr_pos + 0x14], record[attr_pos + 0x15]]) as usize;
        let value_len = u32::from_le_bytes([
            record[attr_pos + 0x10],
            record[attr_pos + 0x11],
            record[attr_pos + 0x12],
            record[attr_pos + 0x13],
        ]) as usize;
        if byte < value_len {
            let at = attr_pos + value_off + byte;
            if value {
                record[at] |= 1 << bit;
            } else {
                record[at] &= !(1 << bit);
            }
            return Ok(());
        }
        // Grow the resident bitmap value in 8-byte steps.
        let mut data = record[attr_pos + value_off..attr_pos + value_off + value_len].to_vec();
        data.resize((byte + 8) & !7, 0);
        if value {
            data[byte] |= 1 << bit;
        }
        let mut new_attr = build_named_resident_attr(ATTR_BITMAP, "$I30", &data);
        new_attr[0x0E..0x10].copy_from_slice(&instance.to_le_bytes());
        replace_attr_at(record, attr_pos, &new_attr)
    }

    fn set_i30_bitmap_bit(&mut self, record: &mut [u8], index: u64) -> Result<(), FilesystemError> {
        self.set_i30_bitmap_bit_value(record, index, true)
    }

    /// Descend a large index to the leaf that should hold `entry_bytes`,
    /// recording (node, routed-entry offset within the entries region) per hop.
    /// Node None = the root; Some(i) = INDX block i.
    #[allow(clippy::type_complexity)]
    fn descend_to_leaf(
        &self,
        record: &[u8],
        stream: &[u8],
        block_size: u32,
        name_upper: &str,
    ) -> Result<(Vec<(Option<u64>, usize)>, u64), FilesystemError> {
        let mut path: Vec<(Option<u64>, usize)> = Vec::new();
        let mut node: Option<u64> = None;
        loop {
            let (entries, internal): (Vec<u8>, bool) = match node {
                None => {
                    let root =
                        parse_root_node(record, self.index_record_size).ok_or_else(|| {
                            FilesystemError::Parse(
                                "directory record has no resident $INDEX_ROOT".into(),
                            )
                        })?;
                    (
                        record[root.entries_start..root.entries_end].to_vec(),
                        root.large,
                    )
                }
                Some(i) => {
                    let block = get_indx_block(stream, i, block_size)?;
                    let (es, ee, _) = indx_entry_bounds(&block).ok_or_else(|| {
                        FilesystemError::Parse(format!("INDX block {i} has a bad node header"))
                    })?;
                    (block[es..ee].to_vec(), indx_is_internal(&block))
                }
            };
            if !internal {
                let leaf = node.ok_or_else(|| {
                    FilesystemError::Parse("large index root is not marked as internal".into())
                })?;
                return Ok((path, leaf));
            }
            let (off, vcn) = route_in_entries(&entries, name_upper)?;
            let child = self.vcn_to_block_index(vcn, block_size)?;
            path.push((node, off));
            node = Some(child);
        }
    }

    /// Insert into a large index, splitting full nodes upward as needed.
    fn insert_into_large_index(
        &mut self,
        parent_record_num: u64,
        record: &mut [u8],
        entry_bytes: &[u8],
    ) -> Result<(), FilesystemError> {
        let record_size = self.mft_record_size;
        let root = parse_root_node(record, self.index_record_size).ok_or_else(|| {
            FilesystemError::Parse("directory record has no resident $INDEX_ROOT".into())
        })?;
        let block_size = root.block_size;
        let mut stream = self.read_i30_allocation(record)?;

        let name_upper = extract_name_from_index_entry(entry_bytes).to_uppercase();
        let (mut path, leaf) = self.descend_to_leaf(record, &stream, block_size, &name_upper)?;

        let mut pending: Vec<u8> = entry_bytes.to_vec();
        let mut target: Option<u64> = Some(leaf);
        loop {
            match target {
                Some(i) => {
                    let mut block = get_indx_block(&stream, i, block_size)?;
                    let pos = {
                        let (es, ee, _) = indx_entry_bounds(&block).ok_or_else(|| {
                            FilesystemError::Parse(format!("INDX block {i} has a bad node header"))
                        })?;
                        es + self.find_index_insert_position(&block[es..ee], &pending)
                    };
                    if splice_into_indx(&mut block, pos, &pending) {
                        put_indx_block(&mut stream, i, block_size, &mut block)?;
                        break;
                    }

                    // Node is full: split it around the median.
                    let (left_entries, median, right_entries, end_sentinel) =
                        split_indx_entries(&block, pos, &pending)?;
                    let new_i = self.append_index_block(record, &mut stream, block_size)?;

                    // Left keeps this block's VCN; a median child pointer, if
                    // any, becomes the left node's end-sentinel sub-node.
                    let left_end = match entry_sub_vcn(&median) {
                        Some(v) => internal_end_sentinel(v),
                        None => leaf_end_sentinel(),
                    };
                    let mut left = build_indx_block(
                        block_size,
                        self.block_index_to_vcn(i, block_size),
                        &left_entries,
                        &left_end,
                    )?;
                    put_indx_block(&mut stream, i, block_size, &mut left)?;
                    let mut right = build_indx_block(
                        block_size,
                        self.block_index_to_vcn(new_i, block_size),
                        &right_entries,
                        &end_sentinel,
                    )?;
                    put_indx_block(&mut stream, new_i, block_size, &mut right)?;

                    // The entry that routed here now covers only the right half.
                    let (parent, routed_off) = path.pop().ok_or_else(|| {
                        FilesystemError::Parse("index split reached a node with no parent".into())
                    })?;
                    let right_vcn = self.block_index_to_vcn(new_i, block_size);
                    match parent {
                        None => {
                            let r = parse_root_node(record, self.index_record_size).ok_or_else(
                                || {
                                    FilesystemError::Parse(
                                        "directory record has no resident $INDEX_ROOT".into(),
                                    )
                                },
                            )?;
                            set_entry_sub_vcn_at(record, r.entries_start + routed_off, right_vcn)?;
                        }
                        Some(p) => {
                            let mut pblock = get_indx_block(&stream, p, block_size)?;
                            let (es, _, _) = indx_entry_bounds(&pblock).ok_or_else(|| {
                                FilesystemError::Parse(format!(
                                    "INDX block {p} has a bad node header"
                                ))
                            })?;
                            set_entry_sub_vcn_at(&mut pblock, es + routed_off, right_vcn)?;
                            put_indx_block(&mut stream, p, block_size, &mut pblock)?;
                        }
                    }

                    // Median moves up, pointing at the left half.
                    pending = entry_with_sub_vcn(&median, self.block_index_to_vcn(i, block_size));
                    target = parent;
                }
                None => {
                    if self.try_splice_into_root(record, &pending, record_size)? {
                        break;
                    }
                    return Err(FilesystemError::DiskFull(
                        "directory index root is full; cannot grow this directory further".into(),
                    ));
                }
            }
        }

        self.write_i30_allocation(record, &stream)?;
        self.write_mft_record(parent_record_num, record)?;
        Ok(())
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

        let Some(root) = parse_root_node(&record, self.index_record_size) else {
            return Err(FilesystemError::Parse(
                "directory record has no resident $INDEX_ROOT".into(),
            ));
        };
        let block_size = root.block_size;

        // The root node first.
        let region = record[root.entries_start..root.entries_end].to_vec();
        if let Some((off, len)) = find_entry_by_name(&region, name) {
            let entry = region[off..off + len].to_vec();
            if let Some(left_vcn) = entry_sub_vcn(&entry) {
                return self.replace_separator_with_predecessor(
                    parent_record_num,
                    &mut record,
                    None,
                    &entry,
                    left_vcn,
                );
            }
            self.splice_out_of_root(&mut record, root.entries_start + off, len);
            return self.write_mft_record(parent_record_num, &mut record);
        }

        // Then every INDX block.
        if find_attr_pos(&record, ATTR_INDEX_ALLOCATION, false).is_some() {
            let mut stream = self.read_i30_allocation(&record)?;
            let nblocks = stream.len() / block_size as usize;
            for i in 0..nblocks as u64 {
                let Ok(block) = get_indx_block(&stream, i, block_size) else {
                    continue; // unused / never-initialized block
                };
                let Some((es, ee, _)) = indx_entry_bounds(&block) else {
                    continue;
                };
                if let Some((off, len)) = find_entry_by_name(&block[es..ee], name) {
                    let entry = block[es + off..es + off + len].to_vec();
                    if let Some(left_vcn) = entry_sub_vcn(&entry) {
                        return self.replace_separator_with_predecessor(
                            parent_record_num,
                            &mut record,
                            Some(i),
                            &entry,
                            left_vcn,
                        );
                    }
                    let mut b = block;
                    splice_out_of_indx(&mut b, es + off, len);
                    put_indx_block(&mut stream, i, block_size, &mut b)?;
                    self.write_i30_allocation(&record, &stream)?;
                    return Ok(());
                }
            }
        }

        Err(FilesystemError::NotFound(format!(
            "index entry '{}' not found in directory",
            name
        )))
    }

    /// Remove a separator entry from an internal node by pulling up its
    /// in-order predecessor (the last entry of the rightmost leaf of the left
    /// subtree), which keeps the B-tree ordering invariants intact.
    fn replace_separator_with_predecessor(
        &mut self,
        parent_record_num: u64,
        record: &mut [u8],
        node: Option<u64>,
        old_entry: &[u8],
        left_vcn: u64,
    ) -> Result<(), FilesystemError> {
        let root = parse_root_node(record, self.index_record_size).ok_or_else(|| {
            FilesystemError::Parse("directory record has no resident $INDEX_ROOT".into())
        })?;
        let block_size = root.block_size;
        let record_size = self.mft_record_size;
        let mut stream = self.read_i30_allocation(record)?;

        // Rightmost leaf of the left subtree.
        let direct_child = self.vcn_to_block_index(left_vcn, block_size)?;
        let mut leaf_i = direct_child;
        loop {
            let block = get_indx_block(&stream, leaf_i, block_size)?;
            if !indx_is_internal(&block) {
                break;
            }
            let (es, ee, _) = indx_entry_bounds(&block).ok_or_else(|| {
                FilesystemError::Parse(format!("INDX block {leaf_i} has a bad node header"))
            })?;
            let end_off = entries_end_sentinel_offset(&block[es..ee]);
            let end_entry = &block[es + end_off..ee];
            let vcn = entry_sub_vcn(end_entry).ok_or_else(|| {
                FilesystemError::Parse("internal index node's end entry has no sub-node".into())
            })?;
            leaf_i = self.vcn_to_block_index(vcn, block_size)?;
        }

        let mut leaf = get_indx_block(&stream, leaf_i, block_size)?;
        let (es, ee, _) = indx_entry_bounds(&leaf).ok_or_else(|| {
            FilesystemError::Parse(format!("INDX block {leaf_i} has a bad node header"))
        })?;
        let region = leaf[es..ee].to_vec();
        let pred = last_real_entry(&region);

        let name = extract_name_from_index_entry(old_entry);
        match pred {
            None => {
                // Empty left subtree: only handled when it is a single leaf —
                // then dropping the separator orphans nothing but that block.
                if leaf_i != direct_child {
                    return Err(FilesystemError::InvalidData(format!(
                        "cannot remove '{name}': its left index subtree is deeper than one level and empty"
                    )));
                }
                match node {
                    None => {
                        let r =
                            parse_root_node(record, self.index_record_size).ok_or_else(|| {
                                FilesystemError::Parse(
                                    "directory record has no resident $INDEX_ROOT".into(),
                                )
                            })?;
                        let region = record[r.entries_start..r.entries_end].to_vec();
                        let (off, len) = find_entry_by_name(&region, &name).ok_or_else(|| {
                            FilesystemError::NotFound(format!("index entry '{name}' vanished"))
                        })?;
                        self.splice_out_of_root(record, r.entries_start + off, len);
                    }
                    Some(p) => {
                        let mut pblock = get_indx_block(&stream, p, block_size)?;
                        let (pes, pee, _) = indx_entry_bounds(&pblock).ok_or_else(|| {
                            FilesystemError::Parse(format!("INDX block {p} has a bad node header"))
                        })?;
                        let (off, len) =
                            find_entry_by_name(&pblock[pes..pee], &name).ok_or_else(|| {
                                FilesystemError::NotFound(format!("index entry '{name}' vanished"))
                            })?;
                        splice_out_of_indx(&mut pblock, pes + off, len);
                        put_indx_block(&mut stream, p, block_size, &mut pblock)?;
                    }
                }
                self.set_i30_bitmap_bit_value(record, leaf_i, false)?;
            }
            Some((pred_off, pred_len)) => {
                let pred_entry = region[pred_off..pred_off + pred_len].to_vec();
                let new_sep = entry_with_sub_vcn(&pred_entry, left_vcn);

                // Pre-check room so a failed swap cannot lose the old entry.
                match node {
                    None => {
                        let grow = new_sep.len().saturating_sub(old_entry.len());
                        if record_used_size(record) + grow > record_size as usize {
                            return Err(FilesystemError::DiskFull(
                                "directory index root has no room to rewrite a separator".into(),
                            ));
                        }
                        let r =
                            parse_root_node(record, self.index_record_size).ok_or_else(|| {
                                FilesystemError::Parse(
                                    "directory record has no resident $INDEX_ROOT".into(),
                                )
                            })?;
                        let region = record[r.entries_start..r.entries_end].to_vec();
                        let (off, len) = find_entry_by_name(&region, &name).ok_or_else(|| {
                            FilesystemError::NotFound(format!("index entry '{name}' vanished"))
                        })?;
                        self.splice_out_of_root(record, r.entries_start + off, len);
                        if !self.try_splice_into_root(record, &new_sep, record_size)? {
                            return Err(FilesystemError::DiskFull(
                                "directory index root has no room to rewrite a separator".into(),
                            ));
                        }
                    }
                    Some(p) => {
                        let mut pblock = get_indx_block(&stream, p, block_size)?;
                        let (pes, pee, alloc_end) =
                            indx_entry_bounds(&pblock).ok_or_else(|| {
                                FilesystemError::Parse(format!(
                                    "INDX block {p} has a bad node header"
                                ))
                            })?;
                        let (off, len) =
                            find_entry_by_name(&pblock[pes..pee], &name).ok_or_else(|| {
                                FilesystemError::NotFound(format!("index entry '{name}' vanished"))
                            })?;
                        if pee - len + new_sep.len() > alloc_end {
                            return Err(FilesystemError::DiskFull(
                                "index node has no room to rewrite a separator".into(),
                            ));
                        }
                        splice_out_of_indx(&mut pblock, pes + off, len);
                        let (pes2, pee2, _) = indx_entry_bounds(&pblock).ok_or_else(|| {
                            FilesystemError::Parse(format!("INDX block {p} has a bad node header"))
                        })?;
                        let pos =
                            pes2 + self.find_index_insert_position(&pblock[pes2..pee2], &new_sep);
                        if !splice_into_indx(&mut pblock, pos, &new_sep) {
                            return Err(FilesystemError::DiskFull(
                                "index node has no room to rewrite a separator".into(),
                            ));
                        }
                        put_indx_block(&mut stream, p, block_size, &mut pblock)?;
                    }
                }

                // Finally drop the predecessor from its leaf.
                splice_out_of_indx(&mut leaf, es + pred_off, pred_len);
                put_indx_block(&mut stream, leaf_i, block_size, &mut leaf)?;
            }
        }

        self.write_i30_allocation(record, &stream)?;
        self.write_mft_record(parent_record_num, record)?;
        Ok(())
    }

    // ---- fsck repair helpers (see ntfs_fsck.rs) ----

    /// Write `bytes` at absolute offset `off` (boot region / mirror patches).
    pub(crate) fn fsck_write_raw(&mut self, off: u64, bytes: &[u8]) -> Result<(), FilesystemError> {
        self.reader.seek(SeekFrom::Start(off))?;
        self.reader.write_all(bytes)?;
        Ok(())
    }

    /// Overwrite `$Bitmap`'s $DATA in place through its data runs. Caller
    /// supplies the full computed bitmap; a shorter slice pads the tail with
    /// the on-disk bytes so we only touch the reconciled range.
    pub(crate) fn fsck_write_volume_bitmap(&mut self, bytes: &[u8]) -> Result<(), FilesystemError> {
        let runs = self.fsck_bitmap_data_runs()?;
        self.write_data_to_runs(&runs, bytes)
    }

    /// Flush the underlying writer once at the end of a repair pass.
    pub(crate) fn fsck_flush_writer(&mut self) -> Result<(), FilesystemError> {
        self.reader.flush()?;
        Ok(())
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

// ---- B-tree index plumbing (shapes mirror ntfs_format.rs) ----

/// Parsed location of a directory's resident $INDEX_ROOT within its record.
struct RootNode {
    attr_pos: usize,
    attr_len: usize,
    value_len: usize,
    node_start: usize,
    entries_start: usize,
    entries_end: usize,
    block_size: u32,
    large: bool,
}

fn record_used_size(record: &[u8]) -> usize {
    u32::from_le_bytes([record[0x18], record[0x19], record[0x1A], record[0x1B]]) as usize
}

fn set_record_used_size(record: &mut [u8], used: usize) {
    record[0x18..0x1C].copy_from_slice(&(used as u32).to_le_bytes());
}

/// One `$FILE_NAME` of an MFT record: where it sits and whom it names.
struct FileNameAttr {
    pos: usize,
    parent: u64,
    namespace: u8,
    name: String,
}

/// Every resident `$FILE_NAME` in `record`, in attribute order.
fn file_name_attrs(record: &[u8]) -> Vec<FileNameAttr> {
    let mut out = Vec::new();
    let mut pos = u16::from_le_bytes([record[0x14], record[0x15]]) as usize;
    while pos + 24 <= record.len() {
        let atype = u32::from_le_bytes([
            record[pos],
            record[pos + 1],
            record[pos + 2],
            record[pos + 3],
        ]);
        if atype == ATTR_END || atype == 0 {
            break;
        }
        let alen = u32::from_le_bytes([
            record[pos + 4],
            record[pos + 5],
            record[pos + 6],
            record[pos + 7],
        ]) as usize;
        if alen < 24 || pos + alen > record.len() {
            break;
        }
        if atype == ATTR_FILE_NAME && record[pos + 8] == 0 {
            let vlen = u32::from_le_bytes([
                record[pos + 0x10],
                record[pos + 0x11],
                record[pos + 0x12],
                record[pos + 0x13],
            ]) as usize;
            let voff = u16::from_le_bytes([record[pos + 0x14], record[pos + 0x15]]) as usize;
            if voff + vlen <= alen && vlen >= 0x42 {
                let v = &record[pos + voff..pos + voff + vlen];
                let name_len = v[0x40] as usize;
                if 0x42 + name_len * 2 <= vlen {
                    let units: Vec<u16> = v[0x42..0x42 + name_len * 2]
                        .chunks_exact(2)
                        .map(|c| u16::from_le_bytes([c[0], c[1]]))
                        .collect();
                    out.push(FileNameAttr {
                        pos,
                        parent: u64::from_le_bytes([v[0], v[1], v[2], v[3], v[4], v[5], 0, 0]),
                        namespace: v[0x41],
                        name: String::from_utf16_lossy(&units),
                    });
                }
            }
        }
        pos += alen;
    }
    out
}

/// First-fit `want` free clusters below `limit` as (start, length) runs, one
/// contiguous run when the volume has one, fragments otherwise.
fn find_free_cluster_runs(
    bitmap: &[u8],
    limit: u64,
    want: u64,
) -> Result<Vec<(u64, u64)>, FilesystemError> {
    let is_free = |c: u64| bitmap[(c / 8) as usize] & (1 << (c % 8)) == 0;
    let limit = limit.min(bitmap.len() as u64 * 8);
    let mut runs: Vec<(u64, u64)> = Vec::new();
    let mut c = 0;
    while c < limit {
        if !is_free(c) {
            c += 1;
            continue;
        }
        let start = c;
        while c < limit && is_free(c) {
            c += 1;
        }
        if c - start >= want {
            return Ok(vec![(start, want)]);
        }
        runs.push((start, c - start));
    }
    let mut left = want;
    let mut picked = Vec::new();
    for (start, len) in runs {
        if left == 0 {
            break;
        }
        let take = len.min(left);
        picked.push((start, take));
        left -= take;
    }
    if left > 0 {
        return Err(FilesystemError::DiskFull(
            "not enough free clusters to grow $Bitmap".into(),
        ));
    }
    Ok(picked)
}

/// Remove the attribute at `pos`, closing the gap and shrinking the used size.
fn remove_attr_at(record: &mut [u8], pos: usize) -> Result<(), FilesystemError> {
    let used =
        u32::from_le_bytes([record[0x18], record[0x19], record[0x1A], record[0x1B]]) as usize;
    let len = u32::from_le_bytes([
        record[pos + 4],
        record[pos + 5],
        record[pos + 6],
        record[pos + 7],
    ]) as usize;
    if len < 16 || pos + len > used || used > record.len() {
        return Err(FilesystemError::InvalidData(
            "corrupt MFT record while removing an attribute".into(),
        ));
    }
    record.copy_within(pos + len..used, pos);
    record[used - len..used].fill(0);
    record[0x18..0x1C].copy_from_slice(&((used - len) as u32).to_le_bytes());
    Ok(())
}

/// First attribute of `attr_type` with the requested residency: (pos, len).
fn find_attr_pos(record: &[u8], attr_type: u32, resident: bool) -> Option<(usize, usize)> {
    let mut pos = u16::from_le_bytes([record[0x14], record[0x15]]) as usize;
    while pos + 16 <= record.len() {
        let atype = u32::from_le_bytes([
            record[pos],
            record[pos + 1],
            record[pos + 2],
            record[pos + 3],
        ]);
        if atype == ATTR_END || atype == 0 {
            break;
        }
        let alen = u32::from_le_bytes([
            record[pos + 4],
            record[pos + 5],
            record[pos + 6],
            record[pos + 7],
        ]) as usize;
        if alen < 16 || pos + alen > record.len() {
            break;
        }
        if atype == attr_type && (record[pos + 8] == 0) == resident {
            return Some((pos, alen));
        }
        pos += alen;
    }
    None
}

/// Locate and sanity-check the resident $INDEX_ROOT node of a directory record.
fn parse_root_node(record: &[u8], default_block_size: u32) -> Option<RootNode> {
    let (attr_pos, attr_len) = find_attr_pos(record, ATTR_INDEX_ROOT, true)?;
    let value_off = u16::from_le_bytes([record[attr_pos + 0x14], record[attr_pos + 0x15]]) as usize;
    let value_len = u32::from_le_bytes([
        record[attr_pos + 0x10],
        record[attr_pos + 0x11],
        record[attr_pos + 0x12],
        record[attr_pos + 0x13],
    ]) as usize;
    let ir_start = attr_pos + value_off;
    let node_start = ir_start + 16;
    if value_len < 32 || node_start + 16 > record.len() || ir_start + value_len > record.len() {
        return None;
    }
    let raw_bs = u32::from_le_bytes([
        record[ir_start + 8],
        record[ir_start + 9],
        record[ir_start + 10],
        record[ir_start + 11],
    ]);
    let block_size = if raw_bs == 0 || !raw_bs.is_power_of_two() || raw_bs > 2 * 1024 * 1024 {
        default_block_size
    } else {
        raw_bs
    };
    let entries_offset = u32::from_le_bytes([
        record[node_start],
        record[node_start + 1],
        record[node_start + 2],
        record[node_start + 3],
    ]) as usize;
    let node_used = u32::from_le_bytes([
        record[node_start + 4],
        record[node_start + 5],
        record[node_start + 6],
        record[node_start + 7],
    ]) as usize;
    let entries_start = node_start + entries_offset;
    let entries_end = node_start + node_used;
    if entries_start > entries_end || entries_end > record.len() {
        return None;
    }
    Some(RootNode {
        attr_pos,
        attr_len,
        value_len,
        node_start,
        entries_start,
        entries_end,
        block_size,
        large: record[node_start + 12] & INDEX_NODE_HAS_CHILDREN != 0,
    })
}

/// Replace the attribute at `pos` with `new_attr`, shifting the record tail.
fn replace_attr_at(record: &mut [u8], pos: usize, new_attr: &[u8]) -> Result<(), FilesystemError> {
    let old_len = u32::from_le_bytes([
        record[pos + 4],
        record[pos + 5],
        record[pos + 6],
        record[pos + 7],
    ]) as usize;
    let used = record_used_size(record);
    let new_used = used - old_len + new_attr.len();
    if new_used > record.len() {
        return Err(FilesystemError::DiskFull(
            "MFT record has no room to grow an index attribute".into(),
        ));
    }
    record.copy_within(pos + old_len..used, pos + new_attr.len());
    if new_used < used {
        record[new_used..used].fill(0);
    }
    record[pos..pos + new_attr.len()].copy_from_slice(new_attr);
    set_record_used_size(record, new_used);
    Ok(())
}

/// Insert a whole attribute blob at `pos`, shifting the record tail.
fn insert_attr_at(record: &mut [u8], pos: usize, attr: &[u8]) -> Result<(), FilesystemError> {
    let used = record_used_size(record);
    let new_used = used + attr.len();
    if new_used > record.len() {
        return Err(FilesystemError::DiskFull(
            "MFT record has no room for a new index attribute".into(),
        ));
    }
    record.copy_within(pos..used, pos + attr.len());
    record[pos..pos + attr.len()].copy_from_slice(attr);
    set_record_used_size(record, new_used);
    Ok(())
}

/// Build a named non-resident attribute over `runs` ((start, len) clusters).
fn build_named_nonresident_attr(
    attr_type: u32,
    name: &str,
    runs: &[(u64, u64)],
    alloc_size: u64,
    real_size: u64,
) -> Vec<u8> {
    let encoded = encode_data_runs(runs);
    let name_utf16: Vec<u16> = name.encode_utf16().collect();
    let name_off = 0x40usize;
    let mp_off = (name_off + name_utf16.len() * 2 + 7) & !7;
    let total = (mp_off + encoded.len() + 7) & !7;
    let mut attr = vec![0u8; total];
    attr[0..4].copy_from_slice(&attr_type.to_le_bytes());
    attr[4..8].copy_from_slice(&(total as u32).to_le_bytes());
    attr[8] = 1; // non-resident
    attr[9] = name_utf16.len() as u8;
    attr[0x0A..0x0C].copy_from_slice(&(name_off as u16).to_le_bytes());
    let total_clusters: u64 = runs.iter().map(|(_, l)| l).sum();
    if total_clusters > 0 {
        attr[0x18..0x20].copy_from_slice(&(total_clusters - 1).to_le_bytes());
    }
    attr[0x20..0x22].copy_from_slice(&(mp_off as u16).to_le_bytes());
    attr[0x28..0x30].copy_from_slice(&alloc_size.to_le_bytes());
    attr[0x30..0x38].copy_from_slice(&real_size.to_le_bytes());
    attr[0x38..0x40].copy_from_slice(&real_size.to_le_bytes()); // initialized
    for (i, ch) in name_utf16.iter().enumerate() {
        attr[name_off + i * 2..name_off + i * 2 + 2].copy_from_slice(&ch.to_le_bytes());
    }
    attr[mp_off..mp_off + encoded.len()].copy_from_slice(&encoded);
    attr
}

/// Walk an entries region; yields (offset, length, flags) including the end.
fn walk_index_entries(region: &[u8]) -> Vec<(usize, usize, u32)> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos + 16 <= region.len() {
        let len = u16::from_le_bytes([region[pos + 8], region[pos + 9]]) as usize;
        let flags = u32::from_le_bytes([
            region[pos + 12],
            region[pos + 13],
            region[pos + 14],
            region[pos + 15],
        ]);
        if len < 16 || pos + len > region.len() {
            break;
        }
        out.push((pos, len, flags));
        if flags & INDEX_ENTRY_END != 0 {
            break;
        }
        pos += len;
    }
    out
}

/// Offset of the end sentinel within an entries region.
fn entries_end_sentinel_offset(region: &[u8]) -> usize {
    for (off, _, flags) in walk_index_entries(region) {
        if flags & INDEX_ENTRY_END != 0 {
            return off;
        }
    }
    region.len()
}

/// Last real (non-end) entry of an entries region: (offset, length).
fn last_real_entry(region: &[u8]) -> Option<(usize, usize)> {
    let mut last = None;
    for (off, len, flags) in walk_index_entries(region) {
        if flags & INDEX_ENTRY_END != 0 {
            break;
        }
        last = Some((off, len));
    }
    last
}

/// Sub-node VCN carried by an entry, when its NODE flag is set.
fn entry_sub_vcn(entry: &[u8]) -> Option<u64> {
    if entry.len() < 24 {
        return None;
    }
    let flags = u32::from_le_bytes([entry[12], entry[13], entry[14], entry[15]]);
    if flags & INDEX_ENTRY_NODE == 0 {
        return None;
    }
    let n = entry.len();
    Some(u64::from_le_bytes([
        entry[n - 8],
        entry[n - 7],
        entry[n - 6],
        entry[n - 5],
        entry[n - 4],
        entry[n - 3],
        entry[n - 2],
        entry[n - 1],
    ]))
}

/// Copy of `entry` carrying sub-node `vcn` (replacing an existing one if set).
fn entry_with_sub_vcn(entry: &[u8], vcn: u64) -> Vec<u8> {
    let mut out = entry.to_vec();
    let flags = u32::from_le_bytes([out[12], out[13], out[14], out[15]]);
    if flags & INDEX_ENTRY_NODE == 0 {
        out.extend_from_slice(&vcn.to_le_bytes());
        let new_len = out.len() as u16;
        out[8..10].copy_from_slice(&new_len.to_le_bytes());
        let new_flags = flags | INDEX_ENTRY_NODE;
        out[12..16].copy_from_slice(&new_flags.to_le_bytes());
    } else {
        let n = out.len();
        out[n - 8..].copy_from_slice(&vcn.to_le_bytes());
    }
    out
}

/// Rewrite the sub-node VCN of the entry at `abs_off` in place.
fn set_entry_sub_vcn_at(buf: &mut [u8], abs_off: usize, vcn: u64) -> Result<(), FilesystemError> {
    let len = u16::from_le_bytes([buf[abs_off + 8], buf[abs_off + 9]]) as usize;
    let flags = u32::from_le_bytes([
        buf[abs_off + 12],
        buf[abs_off + 13],
        buf[abs_off + 14],
        buf[abs_off + 15],
    ]);
    if flags & INDEX_ENTRY_NODE == 0 || len < 24 || abs_off + len > buf.len() {
        return Err(FilesystemError::Parse(
            "index entry expected to carry a sub-node VCN does not".into(),
        ));
    }
    buf[abs_off + len - 8..abs_off + len].copy_from_slice(&vcn.to_le_bytes());
    Ok(())
}

/// Pick the child to follow for `name_upper`: (entry offset, sub-node VCN).
fn route_in_entries(region: &[u8], name_upper: &str) -> Result<(usize, u64), FilesystemError> {
    for (off, len, flags) in walk_index_entries(region) {
        let is_end = flags & INDEX_ENTRY_END != 0;
        if !is_end {
            let entry_name = extract_name_from_index_entry(&region[off..off + len]).to_uppercase();
            if name_upper >= entry_name.as_str() {
                continue;
            }
        }
        let vcn = entry_sub_vcn(&region[off..off + len]).ok_or_else(|| {
            FilesystemError::Parse("internal index node entry has no sub-node VCN".into())
        })?;
        return Ok((off, vcn));
    }
    Err(FilesystemError::Parse(
        "internal index node has no end entry".into(),
    ))
}

/// A 16-byte leaf end sentinel.
fn leaf_end_sentinel() -> Vec<u8> {
    let mut e = vec![0u8; 16];
    e[8..10].copy_from_slice(&16u16.to_le_bytes());
    e[12..16].copy_from_slice(&INDEX_ENTRY_END.to_le_bytes());
    e
}

/// A 24-byte end sentinel pointing at sub-node `vcn`.
fn internal_end_sentinel(vcn: u64) -> Vec<u8> {
    let mut e = vec![0u8; 24];
    e[8..10].copy_from_slice(&24u16.to_le_bytes());
    e[12..16].copy_from_slice(&(INDEX_ENTRY_END | INDEX_ENTRY_NODE).to_le_bytes());
    e[16..24].copy_from_slice(&vcn.to_le_bytes());
    e
}

/// Assemble an INDX block from concatenated entries plus an end sentinel.
fn build_indx_block(
    block_size: u32,
    vcn_field: u64,
    entries: &[u8],
    end_sentinel: &[u8],
) -> Result<Vec<u8>, FilesystemError> {
    let bs = block_size as usize;
    let usa_count = (bs / NTFS_BLOCK_SIZE + 1) as u16;
    let entries_start = (0x28 + usa_count as usize * 2 + 7) & !7;
    let content_end = entries_start + entries.len() + end_sentinel.len();
    if content_end > bs {
        return Err(FilesystemError::DiskFull(
            "index entries overflow an INDX block".into(),
        ));
    }
    let mut b = vec![0u8; bs];
    b[0..4].copy_from_slice(b"INDX");
    b[4..6].copy_from_slice(&0x28u16.to_le_bytes());
    b[6..8].copy_from_slice(&usa_count.to_le_bytes());
    b[0x10..0x18].copy_from_slice(&vcn_field.to_le_bytes());
    let node = 0x18;
    b[node..node + 4].copy_from_slice(&((entries_start - node) as u32).to_le_bytes());
    b[node + 4..node + 8].copy_from_slice(&((content_end - node) as u32).to_le_bytes());
    b[node + 8..node + 12].copy_from_slice(&((bs - node) as u32).to_le_bytes());
    if end_sentinel
        .get(12)
        .is_some_and(|f| f & INDEX_NODE_HAS_CHILDREN != 0)
    {
        b[node + 12] = INDEX_NODE_HAS_CHILDREN;
    }
    b[entries_start..entries_start + entries.len()].copy_from_slice(entries);
    b[entries_start + entries.len()..content_end].copy_from_slice(end_sentinel);
    Ok(b)
}

/// Copy INDX block `i` out of the allocation stream with fixups applied.
fn get_indx_block(stream: &[u8], i: u64, block_size: u32) -> Result<Vec<u8>, FilesystemError> {
    let bs = block_size as usize;
    let start = i as usize * bs;
    if start + bs > stream.len() {
        return Err(FilesystemError::Parse(format!(
            "INDX block {i} lies beyond the $INDEX_ALLOCATION stream"
        )));
    }
    let mut block = stream[start..start + bs].to_vec();
    if &block[0..4] != b"INDX" {
        return Err(FilesystemError::Parse(format!(
            "block {i} in $INDEX_ALLOCATION is not an INDX record"
        )));
    }
    apply_fixup(&mut block)?;
    Ok(block)
}

/// Re-protect a block with fixups and store it back into the stream.
fn put_indx_block(
    stream: &mut [u8],
    i: u64,
    block_size: u32,
    block: &mut [u8],
) -> Result<(), FilesystemError> {
    let bs = block_size as usize;
    let start = i as usize * bs;
    if start + bs > stream.len() || block.len() != bs {
        return Err(FilesystemError::Parse(format!(
            "INDX block {i} does not fit the $INDEX_ALLOCATION stream"
        )));
    }
    prepare_fixup(block);
    stream[start..start + bs].copy_from_slice(block);
    Ok(())
}

/// (entries start, entries end, allocation end), absolute within the block.
fn indx_entry_bounds(block: &[u8]) -> Option<(usize, usize, usize)> {
    let node = 0x18;
    if node + 16 > block.len() {
        return None;
    }
    let entries_offset = u32::from_le_bytes([
        block[node],
        block[node + 1],
        block[node + 2],
        block[node + 3],
    ]) as usize;
    let used = u32::from_le_bytes([
        block[node + 4],
        block[node + 5],
        block[node + 6],
        block[node + 7],
    ]) as usize;
    let alloc = u32::from_le_bytes([
        block[node + 8],
        block[node + 9],
        block[node + 10],
        block[node + 11],
    ]) as usize;
    let es = node + entries_offset;
    let ee = node + used;
    let ae = node + alloc;
    if es > ee || ee > ae || ae > block.len() {
        return None;
    }
    Some((es, ee, ae))
}

fn indx_is_internal(block: &[u8]) -> bool {
    block
        .get(0x18 + 12)
        .is_some_and(|f| f & INDEX_NODE_HAS_CHILDREN != 0)
}

/// Insert `entry` at absolute `pos` in a block. False = no room.
fn splice_into_indx(block: &mut [u8], pos: usize, entry: &[u8]) -> bool {
    let Some((_, ee, ae)) = indx_entry_bounds(block) else {
        return false;
    };
    if ee + entry.len() > ae {
        return false;
    }
    block.copy_within(pos..ee, pos + entry.len());
    block[pos..pos + entry.len()].copy_from_slice(entry);
    let node = 0x18;
    let new_used = (ee - node + entry.len()) as u32;
    block[node + 4..node + 8].copy_from_slice(&new_used.to_le_bytes());
    true
}

/// Remove `len` bytes at absolute `pos` from a block's entries.
fn splice_out_of_indx(block: &mut [u8], pos: usize, len: usize) {
    let Some((_, ee, _)) = indx_entry_bounds(block) else {
        return;
    };
    block.copy_within(pos + len..ee, pos);
    block[ee - len..ee].fill(0);
    let node = 0x18;
    let new_used = (ee - node - len) as u32;
    block[node + 4..node + 8].copy_from_slice(&new_used.to_le_bytes());
}

/// Split a full block's entries around the median, with `pending` occupying
/// `insert_pos`. Returns (left entries, median entry, right entries, end
/// sentinel preserved verbatim).
#[allow(clippy::type_complexity)]
fn split_indx_entries(
    block: &[u8],
    insert_pos: usize,
    pending: &[u8],
) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>), FilesystemError> {
    let (es, ee, _) = indx_entry_bounds(block).ok_or_else(|| {
        FilesystemError::Parse("INDX block has a bad node header during split".into())
    })?;
    let region = &block[es..ee];
    let end_off = entries_end_sentinel_offset(region);
    let end_sentinel = region[end_off..].to_vec();

    let mut combined: Vec<&[u8]> = Vec::new();
    for (off, len, flags) in walk_index_entries(region) {
        if es + off == insert_pos {
            combined.push(pending);
        }
        if flags & INDEX_ENTRY_END != 0 {
            break;
        }
        combined.push(&region[off..off + len]);
    }
    if combined.len() < 3 {
        return Err(FilesystemError::Parse(
            "index node too small to split".into(),
        ));
    }
    let m = combined.len() / 2;
    let left: Vec<u8> = combined[..m].concat();
    let median = combined[m].to_vec();
    let right: Vec<u8> = combined[m + 1..].concat();
    Ok((left, median, right, end_sentinel))
}

// =============================================================================
// EditableFilesystem Implementation
// =============================================================================

impl<R: Read + Write + Seek + Send> EditableFilesystem for NtfsFilesystem<R> {
    fn as_filesystem(&self) -> &dyn crate::fs::filesystem::Filesystem {
        self
    }
    fn as_filesystem_mut(&mut self) -> &mut dyn crate::fs::filesystem::Filesystem {
        self
    }
    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn std::io::Read,
        data_len: u64,
        options: &CreateFileOptions,
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

        // Determine resident vs non-resident threshold
        // Approximate: record_size - header(0x38) - StdInfo(~72) - FileName(~104) - SD(~80) - DATA_header(~24) - end(4)
        let overhead = 0x38 + 72 + 104 + 80 + 24 + 4;
        let resident_threshold = (self.mft_record_size as usize).saturating_sub(overhead);

        let data_attr = if data_len as usize <= resident_threshold {
            // Resident $DATA lives inside the 1 KiB record, so buffering it is bounded.
            let mut file_data = vec![0u8; data_len as usize];
            data.read_exact(&mut file_data)
                .map_err(FilesystemError::Io)?;
            build_resident_attr(ATTR_DATA, &file_data)
        } else {
            // Non-resident: allocate clusters, then stream the source into them.
            let clusters_needed = data_len.div_ceil(self.cluster_size) as u32;
            let runs = self.allocate_volume_clusters(clusters_needed)?;
            if let Err(e) = self.stream_into_runs(&runs, data, data_len) {
                let _ = self.free_volume_clusters(&runs);
                return Err(e);
            }

            let alloc_size = clusters_needed as u64 * self.cluster_size;
            let mut attr = build_nonresident_attr(ATTR_DATA, &runs, data_len);
            // Fix up allocated size
            attr[0x28..0x30].copy_from_slice(&alloc_size.to_le_bytes());
            attr
        };

        // The record is claimed only once the data is safely on disk, so a
        // short or failing source leaves no orphan in the $MFT bitmap.
        let (record_num, record_seq) = self.allocate_mft_record()?;

        // Build attributes
        // A parent with its own $SECURITY_DESCRIPTOR (our formatter's root) is
        // inherited verbatim; otherwise 3.x volumes inherit the $Secure id.
        let parent_sd = self.parent_sd_attr_value(parent_record_num);
        let sec_id = (self.ntfs_version.0 >= 3).then(|| {
            if parent_sd.is_some() {
                0
            } else {
                self.read_parent_security_id(parent_record_num)
            }
        });
        // One stamp for both structures: Windows writes them equal at creation.
        // `options.unix_times` (when set) is a source mtime from an import /
        // cross-fs copy; fall back to wall-clock `now` for a genuinely new file.
        let stamp = options
            .unix_times
            .map(|t| super::times::unix_to_filetime(t.mtime_or_now()))
            .unwrap_or_else(now_ntfs_timestamp);
        let std_info = build_resident_attr(
            ATTR_STANDARD_INFORMATION,
            &build_standard_information(FILE_ATTR_ARCHIVE, sec_id, stamp),
        );
        let parent_ref = self.file_reference(parent_record_num);
        let mut file_name_value = build_file_name_attr(parent_ref, name, false, data_len, stamp);
        if data_len as usize > resident_threshold {
            let alloc = data_len.div_ceil(self.cluster_size) * self.cluster_size;
            file_name_value[40..48].copy_from_slice(&alloc.to_le_bytes());
        }
        let file_name_attr = build_resident_attr(ATTR_FILE_NAME, &file_name_value);

        // 3.x resolves the ACL through $Secure by the inherited id; a per-file
        // descriptor beside it is the 1.2 form, and Windows honours that instead.
        let mut attrs = vec![std_info, file_name_attr];
        if let Some(sd) = &parent_sd {
            attrs.push(build_resident_attr(ATTR_SECURITY_DESCRIPTOR, sd));
        } else if sec_id.is_none() {
            let sd_value = self.read_parent_security_descriptor(parent_record_num)?;
            attrs.push(build_resident_attr(ATTR_SECURITY_DESCRIPTOR, &sd_value));
        }
        attrs.push(data_attr);
        let mut record = assemble_mft_record(
            &attrs,
            MFT_RECORD_IN_USE,
            self.mft_record_size,
            record_num,
            record_seq,
        );
        self.write_mft_record(record_num, &mut record)?;

        // Build index entry and insert
        let index_entry = build_index_entry(record_num, record_seq, &file_name_value);
        self.insert_index_entry(parent_record_num, &index_entry)?;

        let path = if parent.path == "/" {
            format!("/{name}")
        } else {
            format!("{}/{name}", parent.path)
        };

        let mut fe = FileEntry::new_file(name.to_string(), path, data_len, record_num);
        fe.modified_unix = super::times::filetime_to_unix(stamp);
        Ok(fe)
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        options: &CreateDirectoryOptions,
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

        let (record_num, record_seq) = self.allocate_mft_record()?;

        // A parent with its own $SECURITY_DESCRIPTOR (our formatter's root) is
        // inherited verbatim; otherwise 3.x volumes inherit the $Secure id.
        let parent_sd = self.parent_sd_attr_value(parent_record_num);
        let sec_id = (self.ntfs_version.0 >= 3).then(|| {
            if parent_sd.is_some() {
                0
            } else {
                self.read_parent_security_id(parent_record_num)
            }
        });
        // Directories carry no archive bit; their directory flag lives in $FILE_NAME.
        let stamp = options
            .unix_times
            .map(|t| super::times::unix_to_filetime(t.mtime_or_now()))
            .unwrap_or_else(now_ntfs_timestamp);
        let std_info = build_resident_attr(
            ATTR_STANDARD_INFORMATION,
            &build_standard_information(0, sec_id, stamp),
        );
        let parent_ref = self.file_reference(parent_record_num);
        let file_name_value = build_file_name_attr(parent_ref, name, true, 0, stamp);
        let file_name_attr = build_resident_attr(ATTR_FILE_NAME, &file_name_value);
        let index_root = build_named_resident_attr(
            ATTR_INDEX_ROOT,
            "$I30",
            &build_empty_index_root(
                self.index_record_size,
                self.cluster_size,
                self.bytes_per_sector,
                false,
            ),
        );

        let mut attrs = vec![std_info, file_name_attr];
        if let Some(sd) = &parent_sd {
            attrs.push(build_resident_attr(ATTR_SECURITY_DESCRIPTOR, sd));
        } else if sec_id.is_none() {
            let sd_value = self.read_parent_security_descriptor(parent_record_num)?;
            attrs.push(build_resident_attr(ATTR_SECURITY_DESCRIPTOR, &sd_value));
        }
        attrs.push(index_root);
        let mut record = assemble_mft_record(
            &attrs,
            MFT_RECORD_IN_USE | MFT_RECORD_IS_DIRECTORY,
            self.mft_record_size,
            record_num,
            record_seq,
        );
        self.write_mft_record(record_num, &mut record)?;

        // Build index entry and insert
        let index_entry = build_index_entry(record_num, record_seq, &file_name_value);
        self.insert_index_entry(parent_record_num, &index_entry)?;

        let path = if parent.path == "/" {
            format!("/{name}")
        } else {
            format!("{}/{name}", parent.path)
        };

        let mut fe = FileEntry::new_directory(name.to_string(), path, record_num);
        fe.modified_unix = super::times::filetime_to_unix(stamp);
        Ok(fe)
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

        let record_number = entry.location;
        if self.unlink_one_name(record_number, parent_record_num, &entry.name)? {
            return Ok(());
        }

        // Remove from parent's index, the DOS alias's entry included.
        self.remove_index_entry(parent_record_num, &entry.name)?;
        if let Ok(record) = self.read_mft_record(record_number) {
            let names = file_name_attrs(&record);
            for i in Self::names_for(&names, parent_record_num, &entry.name) {
                if names[i].namespace == 2 && !names[i].name.eq_ignore_ascii_case(&entry.name) {
                    self.remove_alias_or_name(parent_record_num, &names[i])?;
                }
            }
        }

        // Free data and index-allocation clusters if non-resident
        if let Ok(record) = self.read_mft_record(record_number) {
            let attrs = parse_mft_attributes(&record, self.mft_record_size);
            for attr in &attrs {
                if (attr.attr_type == ATTR_DATA || attr.attr_type == ATTR_INDEX_ALLOCATION)
                    && !attr.resident
                {
                    let runs: Vec<(u64, u64)> = attr
                        .data_runs
                        .iter()
                        .filter(|r| !r.sparse && r.cluster_offset >= 0)
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

        // chkdsk holds the index entry to the record's live times and sizes, so
        // the copy is built from $STANDARD_INFORMATION and $DATA, not stamped now.
        let mut record = self.read_mft_record(record_number)?;
        let attrs = parse_mft_attributes(&record, self.mft_record_size);
        let si_times: Option<[u8; 32]> = attrs
            .iter()
            .find(|a| a.attr_type == ATTR_STANDARD_INFORMATION)
            .and_then(|a| a.value.get(0..32).and_then(|s| s.try_into().ok()));
        let (alloc_size, real_size) = attrs
            .iter()
            .find(|a| a.attr_type == ATTR_DATA)
            .map(|a| {
                if a.resident {
                    let len = a.value.len() as u64;
                    ((len + 7) & !7, len)
                } else {
                    (a.allocated_size, a.real_size)
                }
            })
            .unwrap_or((0, 0));

        // The new name lives in two places: the child record's $FILE_NAME
        // attribute and the parent directory's $I30 index entry. Both carry a
        // $FILE_NAME structure; build it once.
        let parent_ref = self.file_reference(parent_record_num);
        let mut new_fn_value = build_file_name_attr(
            parent_ref,
            new_name,
            is_dir,
            real_size,
            now_ntfs_timestamp(),
        );
        new_fn_value[40..48].copy_from_slice(&alloc_size.to_le_bytes());
        if let Some(times) = si_times {
            new_fn_value[8..40].copy_from_slice(&times);
        }

        // 1) Drop every $FILE_NAME this parent knows the entry by, the Win32
        //    name and any DOS alias, and put the one new name in their place.
        let child_seq = u16::from_le_bytes([record[0x10], record[0x11]]);
        let names = file_name_attrs(&record);
        let ours = Self::names_for(&names, parent_record_num, &entry.name);
        if ours.is_empty() {
            self.remove_index_entry(parent_record_num, &entry.name)?;
        }
        for &i in &ours {
            self.remove_alias_or_name(parent_record_num, &names[i])?;
        }
        let insert_at = ours.first().map(|&i| names[i].pos);
        for &i in ours.iter().rev() {
            remove_attr_at(&mut record, names[i].pos)?;
        }
        let mut new_attr = build_resident_attr(ATTR_FILE_NAME, &new_fn_value);
        // A fresh instance id: chkdsk calls an attribute sharing one corrupt.
        let instance = u16::from_le_bytes([record[0x28], record[0x29]]);
        new_attr[0x0E..0x10].copy_from_slice(&instance.to_le_bytes());
        record[0x28..0x2A].copy_from_slice(&(instance + 1).to_le_bytes());
        match insert_at {
            Some(pos) => insert_attr_at(&mut record, pos, &new_attr)?,
            None => replace_resident_attr(&mut record, ATTR_FILE_NAME, &new_attr)?,
        }
        let links = (names.len() - ours.len() + 1) as u16;
        record[0x12..0x14].copy_from_slice(&links.to_le_bytes());
        self.write_mft_record(record_number, &mut record)?;

        // 2) Key the parent index by the new name. The entry's MFT reference
        //    must carry the child's real sequence number, not a hardcoded 1.
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

    fn repair(&mut self) -> Result<super::fsck::RepairReport, FilesystemError> {
        super::ntfs_fsck::repair_ntfs(self)
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
        apply_fixup(&mut record)?;

        let attrs = parse_mft_attributes(&record, vbr.mft_record_size);
        let mut bitmap_data = Vec::new();
        for attr in &attrs {
            if attr.attr_type == ATTR_DATA {
                if attr.resident {
                    bitmap_data = attr.value.clone();
                } else {
                    // Read bitmap from data runs
                    for run in &attr.data_runs {
                        if run.sparse || run.cluster_offset < 0 {
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
                        .map_err(crate::compat::io_other)?;
                    self.cluster_buf.resize(self.cluster_size as usize, 0);
                    self.source
                        .read_exact(&mut self.cluster_buf)
                        .map_err(crate::compat::io_other)?;
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

    // The caller passes the partition's sector count; NTFS keeps its backup
    // boot sector in the last one, so the volume itself is a sector shorter.
    let new_total_sectors = new_total_sectors.saturating_sub(1);
    if old_total == new_total_sectors {
        return Ok(false);
    }

    let cluster_size = bytes_per_sector * sectors_per_cluster;

    // Check that data doesn't extend beyond new size by reading $Bitmap
    // We need to find the last used cluster
    let mft_cluster = u64::from_le_bytes([
        vbr[0x30], vbr[0x31], vbr[0x32], vbr[0x33], vbr[0x34], vbr[0x35], vbr[0x36], vbr[0x37],
    ]);

    let cluster_bytes = sectors_per_cluster as u32 * bytes_per_sector as u32;
    let Some(mft_record_size) = mft_record_bytes(vbr[0x40] as i8, cluster_bytes) else {
        anyhow::bail!(
            "NTFS: clusters-per-MFT-record byte 0x{:02X} is not a valid record size",
            vbr[0x40]
        );
    };

    // Try to read $Bitmap to check last used cluster
    let mft_offset = partition_offset + mft_cluster * cluster_size;
    let bitmap_offset = mft_offset + MFT_RECORD_BITMAP * mft_record_size as u64;

    // Bits at or past the old cluster count are the end-of-volume mark, not data.
    let old_volume_clusters = old_total * bytes_per_sector / cluster_size;
    if let Ok(last_cluster) = read_last_used_cluster_from_bitmap(
        file,
        bitmap_offset,
        partition_offset,
        mft_record_size,
        cluster_size,
        old_volume_clusters,
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

    // The backup boot sector is the sector after the volume: the partition's last.
    let backup_offset = partition_offset + new_total_sectors * bytes_per_sector;
    file.seek(SeekFrom::Start(backup_offset))?;
    file.write_all(&vbr)?;

    log_cb(&format!(
        "NTFS: patched VBR and backup boot sector (total sectors: {})",
        new_total_sectors
    ));

    // Without this the gained clusters stay unaddressable and the formatter's
    // end-of-volume mark turns into a leaked cluster chkdsk objects to.
    {
        let mut fs = NtfsFilesystem::open(&mut *file, partition_offset)
            .map_err(|e| anyhow::anyhow!("NTFS: cannot reopen the resized volume: {e}"))?;
        fs.resize_volume_bitmap(old_total, new_total_sectors)
            .map_err(|e| anyhow::anyhow!("NTFS: cannot resize $Bitmap: {e}"))?;
    }
    log_cb("NTFS: $Bitmap brought in step with the new cluster count");

    Ok(true)
}

/// Helper to read the last used cluster from $Bitmap.
fn read_last_used_cluster_from_bitmap(
    file: &mut (impl Read + Seek),
    bitmap_record_offset: u64,
    partition_offset: u64,
    mft_record_size: u32,
    cluster_size: u64,
    volume_clusters: u64,
) -> Result<u64> {
    file.seek(SeekFrom::Start(bitmap_record_offset))?;
    let mut record = vec![0u8; mft_record_size as usize];
    file.read_exact(&mut record)?;

    if &record[0..4] != b"FILE" {
        bail!("$Bitmap MFT record invalid");
    }
    apply_fixup(&mut record).map_err(|e| anyhow::anyhow!("{e}"))?;

    let attrs = parse_mft_attributes(&record, mft_record_size);
    for attr in &attrs {
        if attr.attr_type == ATTR_DATA {
            let bitmap = if attr.resident {
                attr.value.clone()
            } else {
                let mut data = Vec::new();
                for run in &attr.data_runs {
                    if run.sparse || run.cluster_offset < 0 {
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

            let mut c = (bitmap.len() as u64 * 8).min(volume_clusters);
            while c > 0 {
                c -= 1;
                if bitmap[(c / 8) as usize] & (1 << (c % 8)) != 0 {
                    return Ok(c);
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

    let cluster_bytes = sectors_per_cluster as u32 * bytes_per_sector as u32;
    let Some(mft_record_size) = mft_record_bytes(vbr[0x40] as i8, cluster_bytes) else {
        anyhow::bail!(
            "NTFS: clusters-per-MFT-record byte 0x{:02X} is not a valid record size",
            vbr[0x40]
        );
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

    if let Err(e) = apply_fixup(&mut record) {
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
            // TotalSectors excludes the backup boot sector, which follows it.
            let backup_offset = partition_offset + total_sectors * bytes_per_sector;
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
        // 0x44 is zero in this fixture; the parser falls back to 4096.
        assert_eq!(parsed.index_record_size, 4096);
    }

    /// D17: a zeroed or absurd clusters-per-record byte reached shifts and
    /// zero-sized buffers and panicked instead of failing to open.
    #[test]
    fn damaged_record_size_and_run_headers_fail_cleanly() {
        for raw in [0x00u8, 0x80, 0x01, 0x7F, 0xFF] {
            let mut vbr = make_ntfs_vbr();
            vbr[0x40] = raw;
            let res = parse_vbr(&vbr);
            if raw == 0x01 {
                assert_eq!(res.unwrap().mft_record_size, 4096, "one 4 KiB cluster");
            } else {
                assert!(res.is_err(), "byte 0x{raw:02X} must be rejected");
            }
        }
        assert_eq!(mft_record_bytes(-10, 4096), Some(1024));
        assert_eq!(mft_record_bytes(3, 4096), None, "not a power of two");
        // A run header whose nibbles claim 15-byte fields.
        assert!(decode_data_runs(&[0xFF, 1, 2, 3]).is_empty());
        assert!(decode_data_runs(&[0x9F, 1, 2, 3, 4, 5, 6, 7, 8, 9]).is_empty());
    }

    #[test]
    fn test_parse_vbr_index_record_size_encodings() {
        let mut vbr = make_ntfs_vbr();
        vbr[0x44] = 1; // positive: clusters per index record (cluster = 4096)
        assert_eq!(parse_vbr(&vbr).unwrap().index_record_size, 4096);
        vbr[0x44] = (-12i8) as u8; // negative power: 2^12
        assert_eq!(parse_vbr(&vbr).unwrap().index_record_size, 4096);
        vbr[0x44] = (-11i8) as u8; // 2^11 (unusual but well-formed)
        assert_eq!(parse_vbr(&vbr).unwrap().index_record_size, 2048);
    }

    // ---- B-tree index editing tests ----

    fn format_test_volume(cluster: u32, mft_hint: u64) -> Cursor<Vec<u8>> {
        use crate::fs::ntfs_format::{create_ntfs, NtfsFormatParams, NtfsGeometry};
        let mut cur = Cursor::new(Vec::new());
        create_ntfs(
            &mut cur,
            &NtfsFormatParams {
                total_size: 64 * 1024 * 1024,
                geometry: NtfsGeometry::with_cluster_size(cluster, 512).unwrap(),
                mft_records_hint: mft_hint,
                label: Some("RBIDX".to_string()),
            },
        )
        .unwrap();
        cur.seek(SeekFrom::Start(0)).unwrap();
        cur
    }

    /// chkdsk corrected the first free byte of every record we assembled: the
    /// end marker counts as eight bytes in the used size, not four.
    #[test]
    fn assembled_records_count_the_end_marker_as_eight_bytes() {
        let cur = format_test_volume(4096, 256);
        let mut fs = NtfsFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let mut data = std::io::Cursor::new(b"resident".to_vec());
        let file = fs
            .create_file(
                &root,
                "created.txt",
                &mut data,
                8,
                &CreateFileOptions::default(),
            )
            .unwrap();
        let dir = fs
            .create_directory(&root, "created-dir", &CreateDirectoryOptions::default())
            .unwrap();
        for rec_no in [file.location, dir.location] {
            let record = fs.read_mft_record(rec_no).unwrap();
            let used = record_used_size(&record);
            let mut off = u16::from_le_bytes([record[0x14], record[0x15]]) as usize;
            while u32::from_le_bytes(record[off..off + 4].try_into().unwrap()) != ATTR_END {
                off += u32::from_le_bytes(record[off + 4..off + 8].try_into().unwrap()) as usize;
            }
            assert_eq!(used, off + 8, "record {rec_no}: used size vs end marker");
        }
    }

    /// The trim point was pinned to the volume size by its own backup boot
    /// sector, so every in-place shrink was refused as cutting live data.
    #[test]
    fn trim_point_leaves_room_to_shrink() {
        let mut img = format_test_volume(4096, 256).into_inner();
        let (total, floor, cluster) = {
            let mut fs = NtfsFilesystem::open(Cursor::new(&mut img), 0).unwrap();
            (
                fs.total_size(),
                fs.last_data_byte().unwrap(),
                fs.cluster_size,
            )
        };
        assert!(
            floor < total / 2,
            "fresh volume trim point {floor} of {total}"
        );
        assert_eq!(
            floor % cluster,
            512,
            "data end plus one sector for the backup boot"
        );

        let new_len = floor.div_ceil(1024 * 1024) * 1024 * 1024;
        let mut cur = Cursor::new(&mut img);
        assert!(resize_ntfs_in_place(&mut cur, 0, new_len / 512, &mut |_| {}).unwrap());
        img.truncate(new_len as usize);
        let mut fs = NtfsFilesystem::open(Cursor::new(&mut img), 0).unwrap();
        let report = crate::fs::ntfs_fsck::fsck_ntfs(&mut fs).unwrap();
        assert!(
            report.errors.is_empty(),
            "after shrink: {:?}",
            report.errors
        );
    }

    /// Resize used to patch only the boot sectors, so a grown volume kept the
    /// formatter's end mark as a leaked cluster and a bitmap too short for it.
    #[test]
    fn resize_keeps_the_volume_bitmap_in_step() {
        let cases: [(u32, i64); 4] = [
            (512, 6 * 1024 * 1024),
            (4096, 4096),
            (4096, 6 * 1024 * 1024),
            (4096, -4 * 1024 * 1024),
        ];
        for (cluster, delta) in cases {
            let mut img = format_test_volume(cluster, 256).into_inner();
            let new_len = (img.len() as i64 + delta) as u64;
            img.resize(new_len as usize, 0);
            let mut cur = Cursor::new(img);
            let changed = resize_ntfs_in_place(&mut cur, 0, new_len / 512, &mut |_| {}).unwrap();
            assert!(changed, "cluster {cluster} delta {delta}");
            let mut img = cur.into_inner();

            let total_sectors = new_len / 512 - 1;
            assert_eq!(
                img[(total_sectors * 512) as usize..(total_sectors * 512 + 512) as usize],
                img[..512],
                "backup boot sector sits in the last sector"
            );
            let mut fs = NtfsFilesystem::open(Cursor::new(&mut img), 0).unwrap();
            let report = crate::fs::ntfs_fsck::fsck_ntfs(&mut fs).unwrap();
            assert!(
                report.errors.is_empty(),
                "cluster {cluster} delta {delta}: {:?}",
                report.errors
            );
            let geom = fs.fsck_geometry();
            let bm = fs.fsck_read_volume_bitmap().unwrap();
            // Windows sizes $Bitmap in whole quadwords; chkdsk rejects anything else.
            assert_eq!(
                bm.len() as u64,
                geom.total_clusters.div_ceil(64) * 8,
                "bitmap length"
            );
            let bit = |c: u64| bm[(c / 8) as usize] & (1 << (c % 8)) != 0;
            if geom.total_clusters < bm.len() as u64 * 8 {
                assert!(
                    bit(geom.total_clusters),
                    "the bits past the volume end stay marked"
                );
            }
            assert!(
                !bit(geom.total_clusters - 1),
                "the last volume cluster is free"
            );
            // The volume still allocates and lists after the rewrite.
            let root = fs.root().unwrap();
            let mut data = std::io::Cursor::new(vec![7u8; 100_000]);
            fs.create_file(
                &root,
                "after.bin",
                &mut data,
                100_000,
                &CreateFileOptions::default(),
            )
            .unwrap();
            let report = crate::fs::ntfs_fsck::fsck_ntfs(&mut fs).unwrap();
            assert!(
                report.errors.is_empty(),
                "after create: {:?}",
                report.errors
            );
        }
    }

    /// A second `$FILE_NAME` on a record is a hard link; deleting one name
    /// used to free the record and its clusters out from under the other.
    #[test]
    fn deleting_one_hard_link_keeps_the_other() {
        let cur = format_test_volume(4096, 256);
        let mut fs = NtfsFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let content: Vec<u8> = (0..20_000u32).map(|i| (i % 253) as u8).collect();
        put_file(&mut fs, &root, "a.txt", &content);
        let find = |fs: &mut NtfsFilesystem<Cursor<Vec<u8>>>, name: &str| {
            fs.list_directory(&root)
                .unwrap()
                .into_iter()
                .find(|e| e.name == name)
        };
        let a = find(&mut fs, "a.txt").unwrap();
        let rec = a.location;
        let mut record = fs.read_mft_record(rec).unwrap();
        let seq = u16::from_le_bytes([record[0x10], record[0x11]]);
        let parent_ref = fs.file_reference(MFT_RECORD_ROOT);
        let link = build_file_name_attr(
            parent_ref,
            "b.txt",
            false,
            content.len() as u64,
            now_ntfs_timestamp(),
        );
        let (pos, len) = find_attr_pos(&record, ATTR_FILE_NAME, true).unwrap();
        insert_attr_at(
            &mut record,
            pos + len,
            &build_resident_attr(ATTR_FILE_NAME, &link),
        )
        .unwrap();
        record[0x12..0x14].copy_from_slice(&2u16.to_le_bytes());
        fs.write_mft_record(rec, &mut record).unwrap();
        fs.insert_index_entry(MFT_RECORD_ROOT, &build_index_entry(rec, seq, &link))
            .unwrap();

        let b = find(&mut fs, "b.txt").expect("the link is listed");
        assert_eq!(b.location, rec);
        fs.delete_entry(&root, &b).unwrap();

        assert!(find(&mut fs, "b.txt").is_none());
        let a = find(&mut fs, "a.txt").expect("the other link survives");
        assert_eq!(fs.read_file(&a, usize::MAX).unwrap(), content);
        let record = fs.read_mft_record(rec).unwrap();
        assert_eq!(u16::from_le_bytes([record[0x12], record[0x13]]), 1);
        let report = super::super::ntfs_fsck::fsck_ntfs(&mut fs).unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);

        // The last name frees the record for real.
        fs.delete_entry(&root, &a).unwrap();
        assert!(find(&mut fs, "a.txt").is_none());
        let record = fs.read_mft_record(rec).unwrap();
        assert_eq!(
            u16::from_le_bytes([record[0x16], record[0x17]]) & MFT_RECORD_IN_USE,
            0
        );
        let report = super::super::ntfs_fsck::fsck_ntfs(&mut fs).unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
    }

    /// Deterministic non-sorted creation order covering 0..n (gcd(7, n) == 1).
    fn shuffled_names(n: usize) -> Vec<String> {
        (0..n)
            .map(|i| format!("file-{:03}.dat", (i * 7 + 3) % n))
            .collect()
    }

    /// Recursively assert a node's entries are sorted, bounded, and that child
    /// pointers match the node kind; collects visited blocks and entry refs.
    #[allow(clippy::too_many_arguments)]
    fn verify_index_node(
        stream: &[u8],
        block_size: u32,
        cluster_size: u64,
        region: &[u8],
        internal: bool,
        lower: Option<&str>,
        upper: Option<&str>,
        blocks: &mut Vec<u64>,
        refs: &mut Vec<(String, u64)>,
    ) {
        let mut prev: Option<String> = lower.map(|s| s.to_string());
        let mut saw_end = false;
        for (off, len, flags) in walk_index_entries(region) {
            let entry = &region[off..off + len];
            let is_end = flags & INDEX_ENTRY_END != 0;
            let vcn = entry_sub_vcn(entry);
            assert_eq!(
                vcn.is_some(),
                internal,
                "sub-node VCN presence must match node kind"
            );
            let name_opt = if is_end {
                saw_end = true;
                None
            } else {
                let name = extract_name_from_index_entry(entry).to_uppercase();
                if let Some(p) = &prev {
                    assert!(p.as_str() < name.as_str(), "sorted order: {p} !< {name}");
                }
                if let Some(u) = upper {
                    assert!(name.as_str() < u, "{name} exceeds upper bound {u}");
                }
                refs.push((
                    extract_name_from_index_entry(entry),
                    u64::from_le_bytes(entry[0..8].try_into().unwrap()),
                ));
                Some(name)
            };
            if let Some(v) = vcn {
                // The child holds keys strictly between the previous entry and
                // this one (or `upper` for the end sentinel's child).
                let child_upper = name_opt.as_deref().or(upper);
                let off_bytes = idx_vcn_to_stream_offset(v, block_size, cluster_size);
                assert_eq!(off_bytes % block_size as u64, 0, "child VCN block-aligned");
                let bi = off_bytes / block_size as u64;
                blocks.push(bi);
                let block = get_indx_block(stream, bi, block_size).expect("child INDX block");
                let (es, ee, ae) = indx_entry_bounds(&block).expect("child node bounds");
                assert_eq!(ae, block_size as usize, "index_allocated spans the block");
                verify_index_node(
                    stream,
                    block_size,
                    cluster_size,
                    &block[es..ee],
                    indx_is_internal(&block),
                    prev.as_deref(),
                    child_upper,
                    blocks,
                    refs,
                );
            }
            if let Some(name) = name_opt {
                prev = Some(name);
            }
            if is_end {
                break;
            }
        }
        assert!(saw_end, "every index node ends with an end sentinel");
    }

    /// Full structural check of a directory's $I30 tree; returns every entry's
    /// (name, raw file reference) found in the index.
    fn verify_directory_index<R: Read + Write + Seek + Send>(
        fs: &mut NtfsFilesystem<R>,
        dir_record: u64,
    ) -> Vec<(String, u64)> {
        let record = fs.read_mft_record(dir_record).unwrap();
        let root = parse_root_node(&record, fs.index_record_size).expect("resident $INDEX_ROOT");
        assert_eq!(root.block_size, fs.index_record_size, "declared block size");
        let ir_start = root.node_start - 16;
        assert_eq!(
            record[ir_start + 12],
            idx_clusters_per_block_byte(fs.index_record_size, fs.cluster_size, fs.bytes_per_sector),
            "clusters-per-index-block byte must match the volume geometry"
        );

        let region = record[root.entries_start..root.entries_end].to_vec();
        let mut blocks = Vec::new();
        let mut refs = Vec::new();
        if root.large {
            let stream = fs.read_i30_allocation(&record).unwrap();
            verify_index_node(
                &stream,
                root.block_size,
                fs.cluster_size,
                &region,
                true,
                None,
                None,
                &mut blocks,
                &mut refs,
            );
            // Every reachable block must be marked in the $I30 bitmap.
            let (bpos, _) = find_attr_pos(&record, ATTR_BITMAP, true).expect("$I30 $BITMAP");
            let voff = u16::from_le_bytes([record[bpos + 0x14], record[bpos + 0x15]]) as usize;
            let vlen = u32::from_le_bytes([
                record[bpos + 0x10],
                record[bpos + 0x11],
                record[bpos + 0x12],
                record[bpos + 0x13],
            ]) as usize;
            let bm = &record[bpos + voff..bpos + voff + vlen];
            for b in &blocks {
                assert!(
                    bm[(b / 8) as usize] & (1 << (b % 8)) != 0,
                    "INDX block {b} must be set in the $I30 bitmap"
                );
            }
        } else {
            assert!(
                find_attr_pos(&record, ATTR_INDEX_ALLOCATION, false).is_none(),
                "small index must not carry $INDEX_ALLOCATION"
            );
            verify_index_node(
                &[],
                root.block_size,
                fs.cluster_size,
                &region,
                false,
                None,
                None,
                &mut blocks,
                &mut refs,
            );
        }

        // Every index entry's file reference must resolve to an in-use record
        // whose sequence matches (Windows prunes entries when it does not).
        for (name, raw_ref) in &refs {
            let rec_num = raw_ref & 0x0000_FFFF_FFFF_FFFF;
            let seq = (raw_ref >> 48) as u16;
            let child = fs.read_mft_record(rec_num).unwrap();
            let child_seq = u16::from_le_bytes([child[0x10], child[0x11]]);
            let flags = u16::from_le_bytes([child[0x16], child[0x17]]);
            assert_eq!(child_seq, seq, "entry '{name}' carries a stale sequence");
            assert!(
                flags & MFT_RECORD_IN_USE != 0,
                "entry '{name}' points at a free record"
            );
        }
        refs
    }

    fn put_file<R: Read + Write + Seek + Send>(
        fs: &mut NtfsFilesystem<R>,
        dir: &FileEntry,
        name: &str,
        content: &[u8],
    ) {
        let mut src = Cursor::new(content.to_vec());
        fs.create_file(
            dir,
            name,
            &mut src,
            content.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap_or_else(|e| panic!("create {name}: {e:?}"));
    }

    #[test]
    fn directory_grows_past_resident_root_across_geometry() {
        for cluster in [512u32, 1024, 4096] {
            let cur = format_test_volume(cluster, 256);
            let mut fs = NtfsFilesystem::open(cur, 0).unwrap();
            let root = fs.root().unwrap();
            let dir = fs
                .create_directory(&root, "stage", &CreateDirectoryOptions::default())
                .unwrap();

            let names = shuffled_names(40);
            for name in &names {
                put_file(&mut fs, &dir, name, name.as_bytes());
            }
            // One non-resident file exercises cluster allocation alongside.
            let big = vec![0xA5u8; 200_000];
            put_file(&mut fs, &dir, "big-payload.bin", &big);

            let refs = verify_directory_index(&mut fs, dir.location);
            assert_eq!(refs.len(), 41, "cluster={cluster}: all entries indexed");
            EditableFilesystem::sync_metadata(&mut fs).unwrap();

            // Reopen from scratch and read everything back.
            let cur = fs.into_reader();
            let mut fs = NtfsFilesystem::open(cur, 0).unwrap();
            let root = fs.root().unwrap();
            let dir = fs
                .list_directory(&root)
                .unwrap()
                .into_iter()
                .find(|e| e.name == "stage")
                .expect("stage dir listed");
            let listed = fs.list_directory(&dir).unwrap();
            assert_eq!(listed.len(), 41, "cluster={cluster}");
            for name in &names {
                let f = listed
                    .iter()
                    .find(|e| e.name == *name)
                    .unwrap_or_else(|| panic!("cluster={cluster}: {name} missing after reopen"));
                assert_eq!(fs.read_file(f, f.size as usize).unwrap(), name.as_bytes());
            }
            let f = listed.iter().find(|e| e.name == "big-payload.bin").unwrap();
            assert_eq!(fs.read_file(f, f.size as usize).unwrap(), big);
        }
    }

    #[test]
    fn root_directory_takes_a_hundred_files_alongside_metafiles() {
        let cur = format_test_volume(512, 512);
        let mut fs = NtfsFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let names = shuffled_names(100);
        for name in &names {
            put_file(&mut fs, &root, name, name.as_bytes());
        }
        let refs = verify_directory_index(&mut fs, MFT_RECORD_ROOT);
        // 100 files + 12 system entries ('.' + 11 metafiles).
        assert_eq!(refs.len(), 112);

        let cur = fs.into_reader();
        let mut fs = NtfsFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let listed = fs.list_directory(&root).unwrap();
        assert_eq!(listed.len(), 100, "metafiles filtered, user files kept");
        for name in &names {
            let f = listed.iter().find(|e| e.name == *name).unwrap();
            assert_eq!(fs.read_file(f, f.size as usize).unwrap(), name.as_bytes());
        }
    }

    #[test]
    fn deleting_separator_entries_keeps_the_tree_sound() {
        let cur = format_test_volume(512, 256);
        let mut fs = NtfsFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let dir = fs
            .create_directory(&root, "stage", &CreateDirectoryOptions::default())
            .unwrap();
        for name in &shuffled_names(60) {
            put_file(&mut fs, &dir, name, name.as_bytes());
        }

        // The large root's own entries are the separators.
        let record = fs.read_mft_record(dir.location).unwrap();
        let rn = parse_root_node(&record, fs.index_record_size).unwrap();
        assert!(rn.large, "60 entries must have promoted the root");
        let separators: Vec<String> = walk_index_entries(&record[rn.entries_start..rn.entries_end])
            .iter()
            .filter(|(_, _, f)| f & INDEX_ENTRY_END == 0)
            .map(|(o, l, _)| {
                extract_name_from_index_entry(
                    &record[rn.entries_start + o..rn.entries_start + o + l],
                )
            })
            .collect();
        assert!(!separators.is_empty(), "expected pushed-up separators");

        for sep in &separators {
            let listing = fs.list_directory(&dir).unwrap();
            let victim = listing
                .iter()
                .find(|e| e.name == *sep)
                .expect("separator listed");
            fs.delete_entry(&dir, victim).unwrap();
            verify_directory_index(&mut fs, dir.location);
        }

        // Delete everything else; the directory must empty out cleanly.
        for entry in fs.list_directory(&dir).unwrap() {
            fs.delete_entry(&dir, &entry).unwrap();
            verify_directory_index(&mut fs, dir.location);
        }
        assert!(fs.list_directory(&dir).unwrap().is_empty());
        let root = fs.root().unwrap();
        let dir_entry = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "stage")
            .unwrap();
        fs.delete_entry(&root, &dir_entry).unwrap();
        let root = fs.root().unwrap();
        assert!(fs.list_directory(&root).unwrap().is_empty());
    }

    /// D12: a file Windows gave both a long and a DOS name kept the stale
    /// alias, attribute and index entry, after a rename.
    #[test]
    fn rename_replaces_every_name_the_entry_had() {
        let cur = format_test_volume(512, 256);
        let mut fs = NtfsFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        put_file(&mut fs, &root, "LongFileName.txt", b"payload");
        let entry = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "LongFileName.txt")
            .unwrap();

        // Graft a DOS alias the way Windows stores one: a second $FILE_NAME
        // (namespace 2) with its own index entry, and a link count of 2.
        let rec_no = entry.location;
        let mut record = fs.read_mft_record(rec_no).unwrap();
        let names = file_name_attrs(&record);
        assert_eq!(names.len(), 1);
        let parent_ref = fs.file_reference(MFT_RECORD_ROOT);
        let mut alias = build_file_name_attr(
            parent_ref,
            "LONGFI~1.TXT",
            false,
            entry.size,
            now_ntfs_timestamp(),
        );
        alias[0x41] = 2;
        let p = names[0].pos;
        let first_len =
            u32::from_le_bytes([record[p + 4], record[p + 5], record[p + 6], record[p + 7]])
                as usize;
        insert_attr_at(
            &mut record,
            p + first_len,
            &build_resident_attr(ATTR_FILE_NAME, &alias),
        )
        .unwrap();
        record[0x12..0x14].copy_from_slice(&2u16.to_le_bytes());
        let seq = u16::from_le_bytes([record[0x10], record[0x11]]);
        fs.write_mft_record(rec_no, &mut record).unwrap();
        fs.insert_index_entry(MFT_RECORD_ROOT, &build_index_entry(rec_no, seq, &alias))
            .unwrap();
        assert_eq!(
            file_name_attrs(&fs.read_mft_record(rec_no).unwrap()).len(),
            2
        );

        let root = fs.root().unwrap();
        fs.rename(&root, &entry, "Renamed.txt").unwrap();
        verify_directory_index(&mut fs, MFT_RECORD_ROOT);

        let record = fs.read_mft_record(rec_no).unwrap();
        let names = file_name_attrs(&record);
        let seen: Vec<(String, u8)> = names
            .iter()
            .map(|n| (n.name.clone(), n.namespace))
            .collect();
        assert_eq!(names.len(), 1, "one name after rename: {seen:?}");
        assert_eq!(names[0].name, "Renamed.txt");
        // chkdsk called the renamed record corrupt when the new name reused
        // $STANDARD_INFORMATION's instance id 0.
        let mut instances = Vec::new();
        let mut off = u16::from_le_bytes([record[0x14], record[0x15]]) as usize;
        loop {
            let atype = u32::from_le_bytes(record[off..off + 4].try_into().unwrap());
            if atype == ATTR_END {
                break;
            }
            instances.push(u16::from_le_bytes([record[off + 0x0E], record[off + 0x0F]]));
            off += u32::from_le_bytes(record[off + 4..off + 8].try_into().unwrap()) as usize;
        }
        let mut unique = instances.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(
            unique.len(),
            instances.len(),
            "instance ids clash: {instances:?}"
        );
        let next = u16::from_le_bytes([record[0x28], record[0x29]]);
        assert!(
            instances.iter().all(|i| *i < next),
            "next id {next} vs {instances:?}"
        );
        assert_eq!(
            u16::from_le_bytes([record[0x12], record[0x13]]),
            1,
            "link count"
        );
        let listing: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(listing.iter().any(|n| n == "Renamed.txt"), "{listing:?}");
        assert!(
            !listing
                .iter()
                .any(|n| n == "LongFileName.txt" || n == "LONGFI~1.TXT"),
            "{listing:?}"
        );
        assert!(!fs
            .name_exists_in_index(MFT_RECORD_ROOT, "LONGFI~1.TXT")
            .unwrap());
        assert!(!fs
            .name_exists_in_index(MFT_RECORD_ROOT, "LongFileName.txt")
            .unwrap());
    }

    #[test]
    fn rename_moves_entries_between_index_nodes() {
        let cur = format_test_volume(512, 256);
        let mut fs = NtfsFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let dir = fs
            .create_directory(&root, "stage", &CreateDirectoryOptions::default())
            .unwrap();
        for name in &shuffled_names(50) {
            put_file(&mut fs, &dir, name, name.as_bytes());
        }
        let listing = fs.list_directory(&dir).unwrap();
        let first = listing.iter().find(|e| e.name == "file-000.dat").unwrap();
        fs.rename(&dir, first, "zzz-moved-to-the-far-end.dat")
            .unwrap();
        verify_directory_index(&mut fs, dir.location);

        let listing = fs.list_directory(&dir).unwrap();
        assert!(listing
            .iter()
            .any(|e| e.name == "zzz-moved-to-the-far-end.dat"));
        assert!(!listing.iter().any(|e| e.name == "file-000.dat"));
        let moved = listing
            .iter()
            .find(|e| e.name == "zzz-moved-to-the-far-end.dat")
            .unwrap();
        assert_eq!(
            fs.read_file(moved, moved.size as usize).unwrap(),
            b"file-000.dat"
        );
    }

    #[test]
    fn reused_mft_record_bumps_sequence_and_index_entry_matches() {
        let cur = format_test_volume(512, 128);
        let mut fs = NtfsFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        put_file(&mut fs, &root, "first.txt", b"one");
        let listing = fs.list_directory(&root).unwrap();
        let first = listing.iter().find(|e| e.name == "first.txt").unwrap();
        let rec_num = first.location;
        let seq1 = {
            let r = fs.read_mft_record(rec_num).unwrap();
            u16::from_le_bytes([r[0x10], r[0x11]])
        };
        fs.delete_entry(&root, first).unwrap();

        put_file(&mut fs, &root, "second.txt", b"two");
        let listing = fs.list_directory(&root).unwrap();
        let second = listing.iter().find(|e| e.name == "second.txt").unwrap();
        assert_eq!(second.location, rec_num, "record slot reused");
        let seq2 = {
            let r = fs.read_mft_record(rec_num).unwrap();
            u16::from_le_bytes([r[0x10], r[0x11]])
        };
        assert_eq!(seq2, seq1 + 1, "sequence bumped on reuse");
        let refs = verify_directory_index(&mut fs, MFT_RECORD_ROOT);
        let (_, raw) = refs.iter().find(|(n, _)| n == "second.txt").unwrap();
        assert_eq!(
            (raw >> 48) as u16,
            seq2,
            "index entry carries the new sequence"
        );
    }

    /// Walk a self-relative SD's DACL, returning (mask, sid-bytes) per ACE.
    /// Panics on any structural inconsistency — this is the shape Windows
    /// parses, and a malformed one silently reads as "no access".
    fn parse_dacl(sd: &[u8]) -> Vec<(u32, Vec<u8>)> {
        let off = |i: usize| u32::from_le_bytes(sd[i..i + 4].try_into().unwrap()) as usize;
        let (o_own, o_grp, o_dacl) = (off(4), off(8), off(16));
        assert_eq!(off(12), 0, "no SACL expected");
        assert!(o_own > 0 && o_grp > 0 && o_dacl > 0);
        let acl_size = u16::from_le_bytes([sd[o_dacl + 2], sd[o_dacl + 3]]) as usize;
        let ace_count = u16::from_le_bytes([sd[o_dacl + 4], sd[o_dacl + 5]]) as usize;
        assert!(
            o_dacl + acl_size <= sd.len(),
            "ACL size {acl_size} overruns the {}-byte SD",
            sd.len()
        );
        let mut out = Vec::new();
        let mut pos = o_dacl + 8;
        for i in 0..ace_count {
            let size = u16::from_le_bytes([sd[pos + 2], sd[pos + 3]]) as usize;
            assert!(size >= 8, "ACE {i} has size {size}");
            assert!(pos + size <= o_dacl + acl_size, "ACE {i} overruns the ACL");
            let mask = u32::from_le_bytes(sd[pos + 4..pos + 8].try_into().unwrap());
            out.push((mask, sd[pos + 8..pos + size].to_vec()));
            pos += size;
        }
        out
    }

    /// Bytes the DACL declares vs. the bytes its ACEs actually occupy. A
    /// repacked SD must have no slack; mkntfs's source SD legitimately does.
    fn dacl_extent(sd: &[u8]) -> (usize, usize) {
        let o_dacl = u32::from_le_bytes(sd[16..20].try_into().unwrap()) as usize;
        let acl_size = u16::from_le_bytes([sd[o_dacl + 2], sd[o_dacl + 3]]) as usize;
        let ace_count = u16::from_le_bytes([sd[o_dacl + 4], sd[o_dacl + 5]]) as usize;
        let mut pos = o_dacl + 8;
        for _ in 0..ace_count {
            pos += u16::from_le_bytes([sd[pos + 2], sd[pos + 3]]) as usize;
        }
        (acl_size, pos - o_dacl)
    }

    #[test]
    fn repacked_security_descriptor_keeps_every_ace() {
        // The formatter's root SD: a 4 KiB blob whose DACL is mostly padding.
        let root = crate::fs::ntfs_tables::root_secdesc_for_test();
        let orig = parse_dacl(root);
        assert_eq!(orig.len(), 8, "root SD carries 8 ACEs");

        let packed = repack_security_descriptor(root).expect("repack the root SD");
        assert!(packed.len() < 400, "repacked to {} bytes", packed.len());
        assert_eq!(parse_dacl(&packed), orig, "same ACEs, same order");
        let (declared, used) = dacl_extent(&packed);
        assert_eq!(declared, used, "repacked ACL size must match its ACEs");

        // Idempotent: a child inheriting from a child must repack again.
        let twice = repack_security_descriptor(&packed).expect("repack a repacked SD");
        assert_eq!(twice, packed, "repacking is a fixed point");

        // Users (S-1-5-32-545) keep read+execute, so a file we write is runnable.
        let users = [1u8, 2, 0, 0, 0, 0, 0, 5, 32, 0, 0, 0, 33, 2, 0, 0];
        let (mask, _) = orig
            .iter()
            .find(|(_, sid)| sid[..] == users[..])
            .expect("Users ACE present");
        assert_eq!(*mask & 0x20, 0x20, "FILE_EXECUTE granted to Users");
    }

    #[test]
    fn created_files_inherit_a_working_dacl_down_the_tree() {
        let cur = format_test_volume(512, 128);
        let mut fs = NtfsFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let expected = parse_dacl(&fs.parent_sd_attr_value(root.location).unwrap());

        // Three levels deep: each level's parent SD is the previous repack.
        let a = fs
            .create_directory(&root, "a", &CreateDirectoryOptions::default())
            .unwrap();
        let b = fs
            .create_directory(&a, "b", &CreateDirectoryOptions::default())
            .unwrap();
        put_file(&mut fs, &b, "deep.txt", b"payload");
        let deep = fs
            .list_directory(&b)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "deep.txt")
            .unwrap();

        for (rec, what) in [
            (a.location, "dir a"),
            (b.location, "dir b"),
            (deep.location, "deep.txt"),
        ] {
            let record = fs.read_mft_record(rec).unwrap();
            let attrs = parse_mft_attributes(&record, fs.mft_record_size);
            let sd_attr = attrs
                .iter()
                .find(|x| x.attr_type == ATTR_SECURITY_DESCRIPTOR)
                .unwrap_or_else(|| panic!("{what} carries no $SECURITY_DESCRIPTOR"));
            let sd = fs.read_attribute_data(sd_attr, None).unwrap();
            assert_eq!(parse_dacl(&sd), expected, "{what} lost the inherited DACL");
        }
    }

    #[test]
    fn file_name_namespace_matches_dos_validity() {
        assert!(is_valid_dos_name("file.txt"));
        assert!(is_valid_dos_name("TOOL.EXE"));
        assert!(is_valid_dos_name("noext"));
        assert!(!is_valid_dos_name("big-payload.bin")); // 11-char base
        assert!(!is_valid_dos_name("two.dots.txt"));
        assert!(!is_valid_dos_name("has space.txt"));
        assert!(!is_valid_dos_name("file.text")); // 4-char extension
        let short = build_file_name_attr(5, "ok.txt", false, 0, 0);
        assert_eq!(short[65], 0x03);
        // A lone Win32 name is invalid without a DOS alias; Windows uses POSIX here.
        let long = build_file_name_attr(5, "Long File Name.textfile", false, 0, 0);
        assert_eq!(long[65], 0x00);
    }

    #[test]
    fn created_directory_declares_volume_index_geometry() {
        // Regression for the hardcoded 4096/1 geometry: on a 512-byte-cluster
        // volume the clusters-per-index-block byte must be 8.
        let cur = format_test_volume(512, 128);
        let mut fs = NtfsFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let dir = fs
            .create_directory(&root, "geomdir", &CreateDirectoryOptions::default())
            .unwrap();
        let record = fs.read_mft_record(dir.location).unwrap();
        let rn = parse_root_node(&record, fs.index_record_size).unwrap();
        let ir_start = rn.node_start - 16;
        assert_eq!(rn.block_size, 4096);
        assert_eq!(
            record[ir_start + 12],
            8,
            "8 clusters of 512 bytes per block"
        );
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
        prepare_fixup(&mut record);

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
        apply_fixup(&mut record).unwrap();
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
    fn data_run_lengths_never_set_the_sign_bit() {
        // Both mapping-pair fields are signed. A run length whose top bit lands in
        // the high byte reads back negative and Windows calls the file corrupt:
        // measured on Win7, a 200-cluster run encoded `21 97 ..` was unreadable.
        for clusters in [
            1u64, 100, 127, 128, 200, 251, 255, 256, 300, 32767, 32768, 70000,
        ] {
            let enc = encode_data_runs(&[(5956, clusters)]);
            let len_size = (enc[0] & 0x0F) as usize;
            let bytes = &enc[1..1 + len_size];
            assert_eq!(
                u64::from_le_bytes({
                    let mut b = [0u8; 8];
                    b[..len_size].copy_from_slice(bytes);
                    b
                }),
                clusters,
                "{clusters} clusters must round-trip"
            );
            assert!(
                bytes[len_size - 1] < 0x80,
                "{clusters} clusters encoded {bytes:02x?}: high bit set, Windows reads this negative"
            );
        }
    }

    #[test]
    fn created_file_carries_a_usable_security_id() {
        // A 48-byte NTFS 1.2 $STANDARD_INFORMATION has no security_id, and on a 3.x volume
        // Windows resolves a file's ACL through $Secure by that id.
        fn sec_of<T: Read + Seek>(fs: &mut NtfsFilesystem<T>, rec: u64) -> (usize, u32) {
            let record = fs.read_mft_record(rec).unwrap();
            let attrs = parse_mft_attributes(&record, fs.mft_record_size);
            let a = attrs
                .iter()
                .find(|a| a.attr_type == ATTR_STANDARD_INFORMATION)
                .expect("$STANDARD_INFORMATION");
            let v = fs.read_attribute_data(a, None).unwrap();
            let id = if v.len() >= 0x38 {
                u32::from_le_bytes([v[0x34], v[0x35], v[0x36], v[0x37]])
            } else {
                0
            };
            (v.len(), id)
        }

        // A volume our own formatter wrote: the root carries a real id, so it must be inherited.
        let mut blank = Cursor::new(Vec::new());
        crate::fs::ntfs_format::create_blank_ntfs(&mut blank, 64 * 1024 * 1024, 64, Some("T"))
            .unwrap();
        let mut img = blank.into_inner();
        let mut fs = NtfsFilesystem::open(Cursor::new(&mut img), 0).unwrap();
        let root = fs.root().unwrap();
        let (plen, pid) = sec_of(&mut fs, root.location);
        assert_eq!(plen, 72, "formatter writes the NTFS 3.x form");
        assert_ne!(pid, 0);

        let data = b"acl inheritance";
        let file = fs
            .create_file(
                &root,
                "acl.txt",
                &mut Cursor::new(data.as_slice()),
                data.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
        let (len, id) = sec_of(&mut fs, file.location);
        assert_eq!(len, 72, "not the 48-byte NTFS 1.2 form");
        // The formatter's root carries a real $SECURITY_DESCRIPTOR (the standard
        // permissive data-volume ACL); children inherit that SD verbatim with a
        // zero $Secure id, so Users keep read+execute on everything we write.
        assert_eq!(id, 0, "SD-attr inheritance leaves the $Secure id unset");
        let raw = fs.read_mft_record(file.location).unwrap();
        let attrs = parse_mft_attributes(&raw, fs.mft_record_size);
        let sd_attr = attrs
            .iter()
            .find(|a| a.attr_type == ATTR_SECURITY_DESCRIPTOR)
            .expect("child inherits the root's $SECURITY_DESCRIPTOR");
        let child_sd = fs.read_attribute_data(sd_attr, None).unwrap();
        let root_sd = fs.parent_sd_attr_value(root.location).unwrap();
        assert_eq!(child_sd, root_sd, "child SD is the repacked parent SD");
        assert!(child_sd.len() < 400, "repacked SD is compact");

        // A parent WITHOUT an SD attribute (Windows-made dirs) still inherits
        // the parent's $Secure id: strip the SD attr off a fresh dir to model
        // one, then create a child inside it.
        let dir = fs
            .create_directory(&root, "plain", &CreateDirectoryOptions::default())
            .unwrap();
        {
            let mut rec = fs.read_mft_record(dir.location).unwrap();
            let (pos, alen) = find_attr_pos(&rec, ATTR_SECURITY_DESCRIPTOR, true).unwrap();
            let used = record_used_size(&rec);
            rec.copy_within(pos + alen..used, pos);
            rec[used - alen..used].fill(0);
            set_record_used_size(&mut rec, used - alen);
            // Give the dir a real id so the child has something to inherit.
            let si = find_attr_pos(&rec, ATTR_STANDARD_INFORMATION, true)
                .unwrap()
                .0;
            let voff = u16::from_le_bytes([rec[si + 0x14], rec[si + 0x15]]) as usize;
            rec[si + voff + 0x34..si + voff + 0x38].copy_from_slice(&0x101u32.to_le_bytes());
            fs.write_mft_record(dir.location, &mut rec).unwrap();
        }
        let child = fs
            .create_file(
                &dir,
                "byid.txt",
                &mut Cursor::new(b"x".as_slice()),
                1,
                &CreateFileOptions::default(),
            )
            .unwrap();
        let (clen, cid) = sec_of(&mut fs, child.location);
        assert_eq!(clen, 72);
        assert_eq!(cid, 0x101, "id path inherits the parent's $Secure id");
        let raw = fs.read_mft_record(child.location).unwrap();
        assert!(
            !parse_mft_attributes(&raw, fs.mft_record_size)
                .iter()
                .any(|a| a.attr_type == ATTR_SECURITY_DESCRIPTOR),
            "id-path children carry no inline $SECURITY_DESCRIPTOR"
        );

        // Windows never writes a zero attribute word for a real file.
        for (ty, off, what) in [
            (
                ATTR_STANDARD_INFORMATION,
                0x20usize,
                "$STANDARD_INFORMATION",
            ),
            (ATTR_FILE_NAME, 0x38usize, "$FILE_NAME"),
        ] {
            let a = attrs.iter().find(|a| a.attr_type == ty).expect(what);
            let v = fs.read_attribute_data(a, None).unwrap();
            let got = u32::from_le_bytes([v[off], v[off + 1], v[off + 2], v[off + 3]]);
            assert_eq!(
                got & FILE_ATTR_ARCHIVE,
                FILE_ATTR_ARCHIVE,
                "{what} must set the archive bit (got {got:#010x})"
            );
        }

        // chkdsk rejects a 3.1 record whose self-index is absent or whose attribute
        // instance ids collide, and deletes the file outright.
        let rec = fs.read_mft_record(file.location).unwrap();
        assert_eq!(
            u32::from_le_bytes([rec[0x2C], rec[0x2D], rec[0x2E], rec[0x2F]]) as u64,
            file.location,
            "record must carry its own MFT index at 0x2C"
        );
        let mut seen = Vec::new();
        let mut off = u16::from_le_bytes([rec[0x14], rec[0x15]]) as usize;
        while off + 8 <= rec.len() {
            let atype = u32::from_le_bytes([rec[off], rec[off + 1], rec[off + 2], rec[off + 3]]);
            if atype == ATTR_END {
                break;
            }
            let alen = u32::from_le_bytes([rec[off + 4], rec[off + 5], rec[off + 6], rec[off + 7]])
                as usize;
            if alen == 0 {
                break;
            }
            seen.push(u16::from_le_bytes([rec[off + 0x0E], rec[off + 0x0F]]));
            off += alen;
        }
        assert!(!seen.is_empty(), "record must have attributes");
        let mut sorted = seen.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            seen.len(),
            "attribute instance ids must be unique: {seen:?}"
        );
        let next = u16::from_le_bytes([rec[0x28], rec[0x29]]);
        assert!(
            seen.iter().all(|i| *i < next),
            "next_attribute_id {next} must exceed every instance {seen:?}"
        );

        let dir = fs
            .create_directory(&root, "acldir", &CreateDirectoryOptions::default())
            .unwrap();
        let (dlen, did) = sec_of(&mut fs, dir.location);
        assert_eq!(dlen, 72);
        assert_eq!(did, 0, "directories inherit the root SD the same way");

        // A volume whose root predates NTFS 3 still gets a usable ACL.
        let mut fixture = load_fixture("test_ntfs.img.zst");
        let mut fs2 = NtfsFilesystem::open(Cursor::new(&mut fixture), 0).unwrap();
        let root2 = fs2.root().unwrap();
        let root2_has_sd = fs2.parent_sd_attr_value(root2.location).is_some();
        let f2 = fs2
            .create_file(
                &root2,
                "acl2.txt",
                &mut Cursor::new(data.as_slice()),
                data.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
        let (len2, id2) = sec_of(&mut fs2, f2.location);
        if fs2.ntfs_version.0 >= 3 {
            assert_eq!(len2, 72, "3.x volume must get the 3.x record");
            if root2_has_sd {
                assert_eq!(id2, 0, "SD-attr inheritance leaves the id unset");
            } else {
                assert_ne!(id2, 0, "id inheritance needs a resolvable id");
            }
        } else {
            assert_eq!(len2, 48, "a 1.2 volume must keep the 1.2 record");
        }
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

    /// Hands out at most `step` bytes per `read`, so `read_exact` must loop.
    struct Trickle {
        pos: u64,
        len: u64,
        step: usize,
    }

    impl std::io::Read for Trickle {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let left = (self.len - self.pos) as usize;
            let n = buf.len().min(self.step).min(left);
            for (i, b) in buf[..n].iter_mut().enumerate() {
                *b = ((self.pos + i as u64) % 251) as u8;
            }
            self.pos += n as u64;
            Ok(n)
        }
    }

    #[test]
    fn nonresident_create_streams_across_chunks_and_pads_the_last_cluster() {
        let mut blank = Cursor::new(Vec::new());
        crate::fs::ntfs_format::create_blank_ntfs(&mut blank, 32 * 1024 * 1024, 64, Some("S"))
            .unwrap();
        let mut img = blank.into_inner();
        let mut fs = NtfsFilesystem::open(Cursor::new(&mut img), 0).unwrap();
        let root = fs.root().unwrap();

        // Crosses two 1 MiB chunk boundaries and ends mid-cluster.
        let len = 2 * 1024 * 1024 + 777;
        let mut src = Trickle {
            pos: 0,
            len,
            step: 4093,
        };
        let file = fs
            .create_file(
                &root,
                "big.bin",
                &mut src,
                len,
                &CreateFileOptions::default(),
            )
            .unwrap();
        assert_eq!(file.size, len);

        let back = fs.read_file(&file, len as usize).unwrap();
        assert_eq!(back.len(), len as usize);
        assert!(back
            .iter()
            .enumerate()
            .all(|(i, &b)| b == (i as u64 % 251) as u8));
    }

    #[test]
    fn nonresident_create_from_short_source_leaks_nothing() {
        let mut blank = Cursor::new(Vec::new());
        crate::fs::ntfs_format::create_blank_ntfs(&mut blank, 32 * 1024 * 1024, 64, Some("S"))
            .unwrap();
        let mut img = blank.into_inner();
        let mut fs = NtfsFilesystem::open(Cursor::new(&mut img), 0).unwrap();
        let root = fs.root().unwrap();
        let free_before = fs.free_space().unwrap();
        let mft_before = fs.read_mft_bitmap().unwrap();

        // Declares 1 MiB but only delivers half of it.
        let mut src = Trickle {
            pos: 0,
            len: 512 * 1024,
            step: 4096,
        };
        let err = fs
            .create_file(
                &root,
                "short.bin",
                &mut src,
                1024 * 1024,
                &CreateFileOptions::default(),
            )
            .unwrap_err();
        assert!(matches!(err, FilesystemError::Io(_)), "{err:?}");

        assert_eq!(
            fs.free_space().unwrap(),
            free_before,
            "clusters were not returned"
        );
        assert_eq!(
            fs.read_mft_bitmap().unwrap(),
            mft_before,
            "an MFT record was orphaned"
        );
        let names: Vec<_> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(!names.iter().any(|n| n == "short.bin"), "{names:?}");
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
