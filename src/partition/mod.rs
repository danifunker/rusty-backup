pub mod apm;
pub mod editor;
pub mod gpt;
pub mod mbr;
pub mod resize;

use serde::Serialize;
use std::io::{Read, Seek, SeekFrom};

use crate::error::RustyBackupError;
use apm::Apm;
use gpt::Gpt;
use mbr::Mbr;

/// Detected partition table type with parsed data.
#[derive(Debug, Clone)]
pub enum PartitionTable {
    Mbr(Mbr),
    Gpt {
        protective_mbr: Mbr,
        gpt: Gpt,
    },
    Apm(Apm),
    /// Superfloppy / floppy: no partition table, filesystem at sector 0.
    None {
        /// Total disk size in bytes (needed to synthesize partition info).
        size_bytes: u64,
        /// Detected filesystem hint: "FAT", "HFS", "HFS+", or "Unknown".
        fs_hint: String,
    },
}

/// Detected partition alignment pattern.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub enum AlignmentType {
    /// LBA 63 start, cylinder boundaries (255 heads x 63 sectors = 16065 sectors/cylinder)
    DosTraditional,
    /// LBA 2048 start, 1MB (2048 sector) boundaries
    Modern1MB,
    /// Some other consistent alignment pattern
    Custom(u64),
    /// No detectable alignment pattern
    None,
}

impl std::fmt::Display for AlignmentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlignmentType::DosTraditional => write!(f, "DOS Traditional (255x63)"),
            AlignmentType::Modern1MB => write!(f, "Modern 1MB boundaries"),
            AlignmentType::Custom(sectors) => write!(f, "Custom ({sectors} sectors)"),
            AlignmentType::None => write!(f, "None detected"),
        }
    }
}

/// Partition alignment information.
#[derive(Debug, Clone, Serialize)]
pub struct PartitionAlignment {
    pub first_lba: u64,
    pub alignment_sectors: u64,
    pub alignment_type: AlignmentType,
    pub heads: u16,
    pub sectors_per_track: u16,
}

/// Unified partition info for display purposes.
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub index: usize,
    pub type_name: String,
    /// Raw partition type byte (MBR type ID; 0 for GPT).
    pub partition_type_byte: u8,
    pub start_lba: u64,
    pub size_bytes: u64,
    pub bootable: bool,
    /// True for logical partitions inside an extended container.
    pub is_logical: bool,
    /// True for the extended container entry itself (not backed up individually).
    pub is_extended_container: bool,
    /// APM partition type string (e.g. "Apple_HFS"). None for MBR/GPT.
    pub partition_type_string: Option<String>,
    /// Allocation block size in bytes for HFS / HFS+ partitions. `None` for
    /// non-HFS partitions or when the volume header could not be probed.
    pub hfs_block_size: Option<u32>,
}

/// Standard floppy disk image sizes (bytes).
///
/// Images matching one of these sizes that lack both a recognized filesystem
/// and a valid MBR/GPT signature are treated as superfloppies with an unknown
/// filesystem rather than producing a confusing partition-table error.
fn is_floppy_size(size: u64) -> bool {
    matches!(
        size,
        143_360     // 5.25" 140K (35 tracks × 16 sectors × 256 bytes)
        | 409_600   // 3.5" 400K single-sided GCR
        | 819_200   // 3.5" 800K double-sided GCR
        | 737_280   // 3.5" 720K MFM (PC double-density)
        | 1_474_560 // 3.5" 1.44MB MFM (PC/Mac high-density)
    )
}

/// Detect whether a disk image is a superfloppy (no partition table, filesystem at sector 0).
///
/// Checks the first sector for a valid FAT BPB, and offset 1024 for HFS/HFS+.
/// Returns `Some(fs_hint)` where `fs_hint` is `"FAT"`, `"HFS"`, `"HFS+"`,
/// or `None` if not a superfloppy.
fn detect_superfloppy(first_sector: &[u8; 512], reader: &mut (impl Read + Seek)) -> Option<String> {
    // Check for FAT VBR: JMP instruction + valid BPB fields.
    // We validate multiple BPB fields to reliably distinguish a FAT boot sector
    // from an MBR. The partition table area (offsets 446-509) of a FAT VBR
    // contains bootstrap code which often has non-zero bytes that would look
    // like partition entries, so we do NOT check that area.
    if first_sector[0] == 0xEB || first_sector[0] == 0xE9 {
        let bytes_per_sector = u16::from_le_bytes([first_sector[11], first_sector[12]]);
        let sectors_per_cluster = first_sector[13];
        let reserved_sectors = u16::from_le_bytes([first_sector[14], first_sector[15]]);
        let num_fats = first_sector[16];
        let media_descriptor = first_sector[21];

        let valid_bps = matches!(bytes_per_sector, 512 | 1024 | 2048 | 4096);
        let valid_spc = sectors_per_cluster.is_power_of_two() && sectors_per_cluster > 0;
        let valid_reserved = reserved_sectors >= 1;
        let valid_fats = num_fats == 1 || num_fats == 2;
        let valid_media = media_descriptor == 0xF0 || media_descriptor >= 0xF8;

        if valid_bps && valid_spc && valid_reserved && valid_fats && valid_media {
            // The combined probability of a non-FAT sector having all five
            // valid BPB fields by coincidence is ~10^-9, so this is a very
            // strong detection signal. We intentionally do NOT check the
            // partition table area (bytes 446-509) because on FAT VBRs that
            // area contains bootstrap code which often has bytes that look
            // like valid MBR partition entries.
            return Some("FAT".to_string());
        }
    }

    // Check for HFS / HFS+ / ProDOS at offset 1024.
    // Read a full 512-byte sector for raw device compatibility (macOS /dev/rdiskN
    // requires sector-aligned reads).
    if reader.seek(SeekFrom::Start(1024)).is_ok() {
        let mut buf = [0u8; 512];
        if reader.read_exact(&mut buf).is_ok() {
            let sig = u16::from_be_bytes([buf[0], buf[1]]);
            match sig {
                0x4244 => return Some("HFS".to_string()),
                0x482B | 0x4858 => return Some("HFS+".to_string()),
                _ => {}
            }
            // ProDOS volume directory key block: prev_block==0, storage_type nibble==0xF,
            // entry_length==39, entries_per_block==13.
            // The directory header entry starts at offset 4 (after the 4-byte
            // prev/next block pointers), so entry_length (offset 31) and
            // entries_per_block (offset 32) within the entry land at block
            // bytes 35 and 36.
            if buf[0] == 0
                && buf[1] == 0
                && (buf[4] >> 4) == 0xF
                && (buf[4] & 0xF) >= 1
                && buf[35] == 39
                && buf[36] == 13
            {
                return Some("ProDOS".to_string());
            }
        }
    }

    None
}

impl PartitionTable {
    /// Detect and parse the partition table from a readable+seekable source.
    pub fn detect(reader: &mut (impl Read + Seek)) -> Result<Self, RustyBackupError> {
        // Read first 512 bytes (MBR / protective MBR / DDR)
        reader
            .seek(SeekFrom::Start(0))
            .map_err(RustyBackupError::Io)?;
        let mut mbr_data = [0u8; 512];
        reader
            .read_exact(&mut mbr_data)
            .map_err(|e| RustyBackupError::InvalidMbr(format!("cannot read first sector: {e}")))?;

        // Check for APM (Driver Descriptor Record signature 0x4552 at offset 0)
        let ddr_sig = u16::from_be_bytes([mbr_data[0], mbr_data[1]]);
        if ddr_sig == 0x4552 {
            reader
                .seek(SeekFrom::Start(0))
                .map_err(RustyBackupError::Io)?;
            if let Ok(apm) = Apm::parse(reader) {
                return Ok(PartitionTable::Apm(apm));
            }
            // Fall through to MBR/GPT parsing on failure
            reader
                .seek(SeekFrom::Start(0))
                .map_err(RustyBackupError::Io)?;
        }

        // Check for superfloppy (no partition table) before MBR parsing
        if let Some(fs_hint) = detect_superfloppy(&mbr_data, reader) {
            // Get disk size via seek to end
            let size_bytes = reader
                .seek(SeekFrom::End(0))
                .map_err(RustyBackupError::Io)?;
            return Ok(PartitionTable::None {
                size_bytes,
                fs_hint,
            });
        }

        reader
            .seek(SeekFrom::Start(0))
            .map_err(RustyBackupError::Io)?;

        // Try MBR parsing.  If it fails and the image is a standard floppy
        // size, treat it as a superfloppy with an unrecognized filesystem
        // (e.g. Apple DOS 3.3) rather than surfacing a confusing MBR error.
        let mut mbr = match Mbr::parse(&mbr_data) {
            Ok(m) => m,
            Err(e) => {
                let size_bytes = reader
                    .seek(SeekFrom::End(0))
                    .map_err(RustyBackupError::Io)?;
                if is_floppy_size(size_bytes) {
                    return Ok(PartitionTable::None {
                        size_bytes,
                        fs_hint: "Unknown".to_string(),
                    });
                }
                return Err(e);
            }
        };

        if mbr.is_protective_gpt() {
            // Try parsing GPT
            match Gpt::parse(reader) {
                Ok(gpt) => Ok(PartitionTable::Gpt {
                    protective_mbr: mbr,
                    gpt,
                }),
                Err(e) => {
                    // A protective 0xEE MBR means this IS a GPT disk.
                    // If GPT parsing fails, surface the error instead of
                    // falling back to MBR (which would expose a fake 0xEE
                    // partition spanning the entire disk).
                    Err(e)
                }
            }
        } else {
            // Parse EBR chain for any extended partition entries
            for entry in &mbr.entries {
                if entry.is_extended() && !entry.is_empty() {
                    match mbr::parse_ebr_chain(reader, entry.start_lba) {
                        Ok(logicals) => {
                            mbr.logical_partitions = logicals;
                        }
                        Err(_) => {
                            // EBR parsing failure is non-fatal; just skip logical partitions
                        }
                    }
                    break; // Only one extended partition is valid per MBR
                }
            }
            Ok(PartitionTable::Mbr(mbr))
        }
    }

    /// Get a unified list of partition info for display.
    pub fn partitions(&self) -> Vec<PartitionInfo> {
        match self {
            PartitionTable::Mbr(mbr) => {
                let mut result: Vec<PartitionInfo> = mbr
                    .entries
                    .iter()
                    .enumerate()
                    .filter(|(_, e)| !e.is_empty())
                    .map(|(i, e)| PartitionInfo {
                        index: i,
                        type_name: e.partition_type_name().to_string(),
                        partition_type_byte: e.partition_type,
                        start_lba: e.start_lba as u64,
                        size_bytes: e.size_bytes(),
                        bootable: e.bootable,
                        is_logical: false,
                        is_extended_container: e.is_extended(),
                        partition_type_string: None,
                        hfs_block_size: None,
                    })
                    .collect();

                // Append logical partitions from EBR chain (index 4+)
                for (j, e) in mbr.logical_partitions.iter().enumerate() {
                    result.push(PartitionInfo {
                        index: 4 + j,
                        type_name: e.partition_type_name().to_string(),
                        partition_type_byte: e.partition_type,
                        start_lba: e.start_lba as u64,
                        size_bytes: e.size_bytes(),
                        bootable: e.bootable,
                        is_logical: true,
                        is_extended_container: false,
                        partition_type_string: None,
                        hfs_block_size: None,
                    });
                }

                result
            }
            PartitionTable::Gpt { gpt, .. } => gpt
                .entries
                .iter()
                .enumerate()
                .map(|(i, e)| PartitionInfo {
                    index: i,
                    type_name: format!("{} ({})", e.type_name(), e.name),
                    partition_type_byte: 0,
                    start_lba: e.first_lba,
                    size_bytes: e.size_bytes(),
                    bootable: false,
                    is_logical: false,
                    is_extended_container: false,
                    partition_type_string: Some(e.type_guid.to_string_formatted()),
                    hfs_block_size: None,
                })
                .collect(),
            PartitionTable::Apm(apm) => {
                let block_size = apm.ddr.block_size;
                apm.entries
                    .iter()
                    .enumerate()
                    .filter(|(_, e)| e.is_data_partition())
                    .map(|(i, e)| PartitionInfo {
                        index: i,
                        type_name: format!("{} ({})", e.partition_type, e.name),
                        partition_type_byte: 0,
                        start_lba: e.start_block as u64 * block_size as u64 / 512,
                        size_bytes: e.size_bytes(block_size),
                        bootable: e.is_bootable(),
                        is_logical: false,
                        is_extended_container: false,
                        partition_type_string: Some(e.partition_type.clone()),
                        hfs_block_size: None,
                    })
                    .collect()
            }
            PartitionTable::None {
                size_bytes,
                fs_hint,
            } => {
                vec![PartitionInfo {
                    index: 0,
                    type_name: fs_hint.clone(),
                    partition_type_byte: 0,
                    start_lba: 0,
                    size_bytes: *size_bytes,
                    bootable: false,
                    is_logical: false,
                    is_extended_container: false,
                    partition_type_string: None,
                    hfs_block_size: None,
                }]
            }
        }
    }

    /// Get a human-readable name for the partition table type.
    pub fn type_name(&self) -> &'static str {
        match self {
            PartitionTable::Mbr(_) => "MBR",
            PartitionTable::Gpt { .. } => "GPT",
            PartitionTable::Apm(_) => "APM",
            PartitionTable::None { .. } => "None",
        }
    }

    /// Get the MBR disk signature (available for both MBR and GPT via protective MBR).
    /// APM and superfloppy have no disk signature, returns 0.
    pub fn disk_signature(&self) -> u32 {
        match self {
            PartitionTable::Mbr(mbr) => mbr.disk_signature,
            PartitionTable::Gpt { protective_mbr, .. } => protective_mbr.disk_signature,
            PartitionTable::Apm(_) | PartitionTable::None { .. } => 0,
        }
    }
}

/// Detect partition alignment pattern from a partition table.
pub fn detect_alignment(table: &PartitionTable) -> PartitionAlignment {
    let partitions = table.partitions();

    if partitions.is_empty() {
        return PartitionAlignment {
            first_lba: 0,
            alignment_sectors: 0,
            alignment_type: AlignmentType::None,
            heads: 0,
            sectors_per_track: 0,
        };
    }

    let first_lba = partitions.iter().map(|p| p.start_lba).min().unwrap_or(0);

    // Extract CHS geometry from MBR if available (APM / superfloppy have no CHS)
    let (heads, sectors_per_track) = match table {
        PartitionTable::Mbr(mbr) => extract_chs_geometry(mbr),
        PartitionTable::Gpt { protective_mbr, .. } => extract_chs_geometry(protective_mbr),
        PartitionTable::Apm(_) | PartitionTable::None { .. } => (0, 0),
    };

    // Check for DOS traditional alignment: first partition at LBA 63
    if first_lba == 63 {
        // Verify cylinder boundary alignment if we have multiple partitions
        let sectors_per_cylinder = heads as u64 * sectors_per_track as u64;
        if sectors_per_cylinder > 0 && check_cylinder_alignment(&partitions, sectors_per_cylinder) {
            return PartitionAlignment {
                first_lba,
                alignment_sectors: sectors_per_cylinder,
                alignment_type: AlignmentType::DosTraditional,
                heads,
                sectors_per_track,
            };
        }
        // Even without multiple partitions, LBA 63 is a strong DOS indicator
        return PartitionAlignment {
            first_lba,
            alignment_sectors: if sectors_per_cylinder > 0 {
                sectors_per_cylinder
            } else {
                16065
            },
            alignment_type: AlignmentType::DosTraditional,
            heads,
            sectors_per_track,
        };
    }

    // Check for modern 1MB alignment: first partition at LBA 2048
    if first_lba == 2048 || first_lba % 2048 == 0 {
        if partitions.iter().all(|p| p.start_lba % 2048 == 0) {
            return PartitionAlignment {
                first_lba,
                alignment_sectors: 2048,
                alignment_type: AlignmentType::Modern1MB,
                heads,
                sectors_per_track,
            };
        }
    }

    // Check for any custom alignment pattern
    if partitions.len() >= 2 {
        let alignment = gcd_of_starts(&partitions);
        if alignment > 1 {
            return PartitionAlignment {
                first_lba,
                alignment_sectors: alignment,
                alignment_type: AlignmentType::Custom(alignment),
                heads,
                sectors_per_track,
            };
        }
    }

    PartitionAlignment {
        first_lba,
        alignment_sectors: 1,
        alignment_type: AlignmentType::None,
        heads,
        sectors_per_track,
    }
}

/// Extract CHS geometry from MBR partition entries.
/// Uses the maximum head and sector values found across all entries.
fn extract_chs_geometry(mbr: &Mbr) -> (u16, u16) {
    let mut max_head: u16 = 0;
    let mut max_sector: u16 = 0;

    for entry in &mbr.entries {
        if entry.is_empty() {
            continue;
        }
        max_head = max_head
            .max(entry.chs_start.head as u16)
            .max(entry.chs_end.head as u16);
        max_sector = max_sector
            .max(entry.chs_start.sector as u16)
            .max(entry.chs_end.sector as u16);
    }

    // Heads are 0-indexed in CHS, so total heads = max_head + 1
    let heads = if max_head > 0 { max_head + 1 } else { 0 };
    let sectors_per_track = max_sector; // sectors are 1-indexed, max value IS the count

    (heads, sectors_per_track)
}

/// Check if all partitions start on cylinder boundaries.
fn check_cylinder_alignment(partitions: &[PartitionInfo], sectors_per_cylinder: u64) -> bool {
    if sectors_per_cylinder == 0 {
        return false;
    }
    // First partition may start at LBA 63 (one track offset), subsequent should be on boundaries
    partitions
        .iter()
        .skip(1)
        .all(|p| p.start_lba % sectors_per_cylinder == 0)
}

/// Find the GCD of all partition start LBAs.
fn gcd_of_starts(partitions: &[PartitionInfo]) -> u64 {
    partitions
        .iter()
        .map(|p| p.start_lba)
        .reduce(gcd)
        .unwrap_or(1)
}

fn gcd(a: u64, b: u64) -> u64 {
    if b == 0 {
        a
    } else {
        gcd(b, a % b)
    }
}

/// Format a byte count as a human-readable size string using binary (base-1024) units.
pub fn format_size(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    const TIB: u64 = 1024 * GIB;

    if bytes >= TIB {
        format!("{:.1} TiB", bytes as f64 / TIB as f64)
    } else if bytes >= GIB {
        format!("{:.1} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}

/// Partition size override for VHD export and restore.
pub struct PartitionSizeOverride {
    pub index: usize,
    pub start_lba: u64,
    pub original_size: u64,
    pub export_size: u64,
    /// New start LBA for this partition (for restore with alignment changes).
    /// When `None`, the original `start_lba` is kept.
    pub new_start_lba: Option<u64>,
    /// Heads for CHS recalculation (0 = don't touch CHS fields).
    pub heads: u16,
    /// Sectors per track for CHS recalculation (0 = don't touch CHS fields).
    pub sectors_per_track: u16,
}

impl PartitionSizeOverride {
    /// Create a simple size-only override (backward compatible with VHD export).
    pub fn size_only(index: usize, start_lba: u64, original_size: u64, export_size: u64) -> Self {
        Self {
            index,
            start_lba,
            original_size,
            export_size,
            new_start_lba: None,
            heads: 0,
            sectors_per_track: 0,
        }
    }

    /// The effective start LBA (new if set, otherwise original).
    pub fn effective_start_lba(&self) -> u64 {
        self.new_start_lba.unwrap_or(self.start_lba)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build MBR bytes with specific partition start LBAs and CHS values.
    fn make_mbr_with_chs(
        entries: &[(u8, u32, u32, u8, u8, u16, u8, u8, u16)],
        // (type, start_lba, sectors, start_head, start_sec, start_cyl, end_head, end_sec, end_cyl)
    ) -> [u8; 512] {
        let mut data = [0u8; 512];
        data[440..444].copy_from_slice(&0x12345678u32.to_le_bytes());

        for (i, &(ptype, start_lba, sectors, sh, ss, sc, eh, es, ec)) in entries.iter().enumerate()
        {
            let offset = 446 + i * 16;
            data[offset] = 0x80; // bootable
            data[offset + 1] = sh;
            data[offset + 2] = (ss & 0x3F) | ((sc >> 2) as u8 & 0xC0);
            data[offset + 3] = sc as u8;
            data[offset + 4] = ptype;
            data[offset + 5] = eh;
            data[offset + 6] = (es & 0x3F) | ((ec >> 2) as u8 & 0xC0);
            data[offset + 7] = ec as u8;
            data[offset + 8..offset + 12].copy_from_slice(&start_lba.to_le_bytes());
            data[offset + 12..offset + 16].copy_from_slice(&sectors.to_le_bytes());
        }

        data[510] = 0x55;
        data[511] = 0xAA;
        data
    }

    #[test]
    fn test_dos_traditional_alignment() {
        // DOS layout: first partition at LBA 63, CHS geometry 255 heads x 63 sectors
        let mbr_data = make_mbr_with_chs(&[
            (0x06, 63, 1024000, 1, 1, 0, 254, 63, 63), // FAT16 at LBA 63
            (0x0B, 16065, 2048000, 0, 1, 1, 254, 63, 127), // FAT32 at next cylinder
        ]);
        let mbr = Mbr::parse(&mbr_data).unwrap();
        let table = PartitionTable::Mbr(mbr);
        let alignment = detect_alignment(&table);

        assert_eq!(alignment.first_lba, 63);
        assert_eq!(alignment.alignment_type, AlignmentType::DosTraditional);
        assert_eq!(alignment.heads, 255);
        assert_eq!(alignment.sectors_per_track, 63);
        assert_eq!(alignment.alignment_sectors, 16065); // 255 * 63
    }

    #[test]
    fn test_modern_1mb_alignment() {
        let mbr_data = make_mbr_with_chs(&[
            (0x0C, 2048, 1048576, 0, 1, 0, 254, 63, 100),
            (0x83, 1050624, 2097152, 0, 1, 101, 254, 63, 200),
        ]);
        let mbr = Mbr::parse(&mbr_data).unwrap();
        let table = PartitionTable::Mbr(mbr);
        let alignment = detect_alignment(&table);

        assert_eq!(alignment.first_lba, 2048);
        assert_eq!(alignment.alignment_type, AlignmentType::Modern1MB);
        assert_eq!(alignment.alignment_sectors, 2048);
    }

    #[test]
    fn test_single_partition_dos() {
        let mbr_data = make_mbr_with_chs(&[(0x06, 63, 1024000, 1, 1, 0, 254, 63, 63)]);
        let mbr = Mbr::parse(&mbr_data).unwrap();
        let table = PartitionTable::Mbr(mbr);
        let alignment = detect_alignment(&table);

        assert_eq!(alignment.alignment_type, AlignmentType::DosTraditional);
    }

    #[test]
    fn test_no_partitions() {
        let mut data = [0u8; 512];
        data[510] = 0x55;
        data[511] = 0xAA;
        let mbr = Mbr::parse(&data).unwrap();
        let table = PartitionTable::Mbr(mbr);
        let alignment = detect_alignment(&table);

        assert_eq!(alignment.alignment_type, AlignmentType::None);
    }

    #[test]
    fn test_detect_mbr_from_reader() {
        let mbr_data = make_mbr_with_chs(&[(0x0C, 2048, 1048576, 0, 1, 0, 254, 63, 100)]);
        let mut cursor = Cursor::new(mbr_data.to_vec());
        let table = PartitionTable::detect(&mut cursor).unwrap();

        assert_eq!(table.type_name(), "MBR");
        assert_eq!(table.partitions().len(), 1);
        assert_eq!(table.partitions()[0].type_name, "FAT32 (LBA)");
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(512), "512 B");
        assert_eq!(format_size(1024), "1.0 KiB");
        assert_eq!(format_size(1048576), "1.0 MiB");
        assert_eq!(format_size(1073741824), "1.0 GiB");
        assert_eq!(format_size(1099511627776), "1.0 TiB");
        assert_eq!(format_size(536870912), "512.0 MiB");
    }

    #[test]
    fn test_detect_superfloppy_fat12() {
        // Build a realistic FAT12 VBR with non-zero boot code in partition table area
        let mut data = vec![0u8; 1474560]; // 1.44 MB floppy
        data[0] = 0xEB; // JMP short
        data[1] = 0x3C;
        data[2] = 0x90; // NOP
                        // OEM ID
        data[3..11].copy_from_slice(b"MSDOS5.0");
        // bytes_per_sector = 512
        data[11] = 0x00;
        data[12] = 0x02;
        // sectors_per_cluster = 1
        data[13] = 0x01;
        // reserved_sectors = 1
        data[14] = 0x01;
        data[15] = 0x00;
        // num_fats = 2
        data[16] = 0x02;
        // media descriptor = 0xF0 (floppy)
        data[21] = 0xF0;
        // Boot signature
        data[510] = 0x55;
        data[511] = 0xAA;
        // Simulate non-zero bootstrap code in the partition table area (bytes 446-509)
        // This is common in real floppy VBRs and should NOT prevent superfloppy detection
        for i in 446..510 {
            data[i] = 0xCD + (i as u8 % 17); // arbitrary non-zero boot code
        }

        let mut cursor = Cursor::new(data);
        let table = PartitionTable::detect(&mut cursor).unwrap();
        assert_eq!(table.type_name(), "None");
        let parts = table.partitions();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].start_lba, 0);
        assert_eq!(parts[0].size_bytes, 1474560);
        assert_eq!(parts[0].type_name, "FAT");
    }

    #[test]
    fn test_detect_superfloppy_hfs() {
        // Build a minimal HFS floppy: signature 0x4244 at offset 1024
        let mut data = vec![0u8; 819200]; // 800K floppy
                                          // No JMP instruction at byte 0, no MBR signature
        data[1024] = 0x42;
        data[1025] = 0x44;

        let mut cursor = Cursor::new(data);
        let table = PartitionTable::detect(&mut cursor).unwrap();
        assert_eq!(table.type_name(), "None");
        let parts = table.partitions();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].type_name, "HFS");
    }

    #[test]
    fn test_hfs_superfloppy_backup_flow() {
        use std::io::BufReader;

        // Simulate the EXACT backup flow for an HFS floppy using a real
        // temp file (not Cursor) to catch File+BufReader interaction bugs.
        let mut data = vec![0xAAu8; 819200]; // 800K floppy, filled with 0xAA
                                             // HFS MDB signature at offset 1024
        data[1024] = 0x42;
        data[1025] = 0x44;

        // Write to a real temp file
        let tmp_dir = std::env::temp_dir();
        let tmp_path = tmp_dir.join("rusty_backup_test_hfs_floppy.img");
        std::fs::write(&tmp_path, &data).unwrap();

        let file = std::fs::File::open(&tmp_path).unwrap();
        let mut source = BufReader::new(file);

        // Step 1: Read first 512 bytes for MBR export (same as backup)
        let mut mbr_bytes = [0u8; 512];
        source.read_exact(&mut mbr_bytes).unwrap();
        source.seek(SeekFrom::Start(0)).unwrap();

        // Step 2: Detect partition table
        let table = PartitionTable::detect(&mut source).unwrap();
        assert_eq!(table.type_name(), "None");
        let partitions = table.partitions();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].start_lba, 0);
        let image_size = partitions[0].size_bytes;
        assert_eq!(image_size, 819200);

        // Step 3: Simulate get_file_size bypassing BufReader
        // (In real code: get_file_size(source.get_ref(), ...) seeks the
        // underlying File directly via &File impl Seek)
        use std::io::Write;
        {
            let inner: &std::fs::File = source.get_ref();
            let mut file_ref = inner;
            let _sz = std::io::Seek::seek(&mut file_ref, SeekFrom::End(0)).unwrap();
            std::io::Seek::seek(&mut file_ref, SeekFrom::Start(0)).unwrap();
        }

        // Step 4: Seek back and read data (trim-based path)
        let part_offset = partitions[0].start_lba * 512; // 0
        source.seek(SeekFrom::Start(part_offset)).unwrap();
        let part_reader = (&mut source).take(image_size);

        let mut output = Vec::new();
        let mut buf = [0u8; 65536]; // 64K chunks
        let mut limited = part_reader;
        loop {
            let n = limited.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            output.extend_from_slice(&buf[..n]);
        }

        // Cleanup
        let _ = std::fs::remove_file(&tmp_path);

        // Verify we read the full floppy
        assert_eq!(
            output.len(),
            819200,
            "Expected 819200 bytes but got {}",
            output.len()
        );
        assert_eq!(&output, &data);
    }

    #[test]
    fn test_real_mbr_not_superfloppy() {
        // A real MBR with valid partition entries should NOT be detected as superfloppy
        let mbr_data = make_mbr_with_chs(&[(0x0C, 2048, 1048576, 0, 1, 0, 254, 63, 100)]);
        let mut cursor = Cursor::new(mbr_data.to_vec());
        let table = PartitionTable::detect(&mut cursor).unwrap();
        assert_eq!(table.type_name(), "MBR");
    }

    /// Build a minimal GPT disk image with a protective MBR + GPT header + entries.
    fn make_gpt_disk_image(entries: &[(gpt::Guid, u64, u64, &str)]) -> Vec<u8> {
        let num_entries = 128u32;
        let entry_size = 128u32;
        let total_sectors: u64 = 34 + 1024;
        let mut image = vec![0u8; total_sectors as usize * 512];

        // Protective MBR at LBA 0
        image[446] = 0x00;
        image[450] = 0xEE; // GPT protective type
        image[454..458].copy_from_slice(&1u32.to_le_bytes());
        image[458..462].copy_from_slice(&0xFFFFFFFFu32.to_le_bytes());
        image[510] = 0x55;
        image[511] = 0xAA;

        // GPT header at LBA 1
        let hdr = 512usize;
        image[hdr..hdr + 8].copy_from_slice(&0x5452415020494645u64.to_le_bytes());
        image[hdr + 8..hdr + 12].copy_from_slice(&0x00010000u32.to_le_bytes());
        image[hdr + 12..hdr + 16].copy_from_slice(&92u32.to_le_bytes());
        image[hdr + 24..hdr + 32].copy_from_slice(&1u64.to_le_bytes());
        image[hdr + 32..hdr + 40].copy_from_slice(&(total_sectors - 1).to_le_bytes());
        image[hdr + 40..hdr + 48].copy_from_slice(&34u64.to_le_bytes());
        image[hdr + 48..hdr + 56].copy_from_slice(&(total_sectors - 34).to_le_bytes());
        image[hdr + 56..hdr + 72].copy_from_slice(&[0xAAu8; 16]);
        image[hdr + 72..hdr + 80].copy_from_slice(&2u64.to_le_bytes());
        image[hdr + 80..hdr + 84].copy_from_slice(&num_entries.to_le_bytes());
        image[hdr + 84..hdr + 88].copy_from_slice(&entry_size.to_le_bytes());

        // Partition entries at LBA 2
        for (i, (type_guid, first_lba, last_lba, name)) in entries.iter().enumerate() {
            let off = 1024 + i * entry_size as usize;
            image[off..off + 16].copy_from_slice(type_guid.as_bytes());
            // unique GUID
            image[off + 16..off + 32].copy_from_slice(&[
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                (i as u8) + 1,
            ]);
            image[off + 32..off + 40].copy_from_slice(&first_lba.to_le_bytes());
            image[off + 40..off + 48].copy_from_slice(&last_lba.to_le_bytes());
            let name_off = off + 56;
            for (j, ch) in name.encode_utf16().enumerate() {
                let o = name_off + j * 2;
                if o + 2 <= off + entry_size as usize {
                    image[o..o + 2].copy_from_slice(&ch.to_le_bytes());
                }
            }
        }

        image
    }

    #[test]
    fn test_detect_valid_gpt_disk() {
        let ms_basic = gpt::Guid::from_string("EBD0A0A2-B9E5-4433-87C0-68B6B72699C7").unwrap();
        let image = make_gpt_disk_image(&[(ms_basic, 2048, 1050623, "DATA")]);
        let mut cursor = Cursor::new(image);
        let table = PartitionTable::detect(&mut cursor).unwrap();

        assert_eq!(table.type_name(), "GPT");
        let parts = table.partitions();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].start_lba, 2048);
        assert_eq!(parts[0].size_bytes, (1050623 - 2048 + 1) * 512);
        // GPT entries should have partition_type_string set
        assert!(parts[0].partition_type_string.is_some());
        assert_eq!(
            parts[0].partition_type_string.as_deref().unwrap(),
            "EBD0A0A2-B9E5-4433-87C0-68B6B72699C7"
        );
    }

    #[test]
    fn test_protective_mbr_with_corrupt_gpt_returns_error() {
        // Build a disk with protective 0xEE MBR but no valid GPT header
        let mut image = vec![0u8; 34 * 512];
        // Protective MBR
        image[450] = 0xEE;
        image[454..458].copy_from_slice(&1u32.to_le_bytes());
        image[458..462].copy_from_slice(&0xFFFFFFFFu32.to_le_bytes());
        image[510] = 0x55;
        image[511] = 0xAA;
        // LBA 1 is all zeros — invalid GPT signature

        let mut cursor = Cursor::new(image);
        let result = PartitionTable::detect(&mut cursor);
        assert!(
            result.is_err(),
            "Expected error when GPT header is corrupt, not a fallback to MBR"
        );
    }
}
