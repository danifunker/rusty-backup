//! Partition alignment detection.
//!
//! Vintage DOS/Windows used cylinder-based geometry (LBA 63 + 255×63 sector
//! cylinders); modern systems align partitions to 1 MiB (LBA 2048). We record
//! the detected pattern so `restore` can preserve original geometry for
//! vintage OS compatibility.

use serde::Serialize;

use super::{mbr::Mbr, PartitionInfo, PartitionTable};

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
        PartitionTable::Apm(_)
        | PartitionTable::Rdb(_)
        | PartitionTable::Sgi(_)
        | PartitionTable::None { .. } => (0, 0),
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
    if (first_lba == 2048 || first_lba % 2048 == 0)
        && partitions.iter().all(|p| p.start_lba % 2048 == 0)
    {
        return PartitionAlignment {
            first_lba,
            alignment_sectors: 2048,
            alignment_type: AlignmentType::Modern1MB,
            heads,
            sectors_per_track,
        };
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

#[cfg(test)]
mod tests {
    use super::*;

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
        let mbr_data = make_mbr_with_chs(&[
            (0x06, 63, 1024000, 1, 1, 0, 254, 63, 63),
            (0x0B, 16065, 2048000, 0, 1, 1, 254, 63, 127),
        ]);
        let mbr = Mbr::parse(&mbr_data).unwrap();
        let table = PartitionTable::Mbr(mbr);
        let alignment = detect_alignment(&table);

        assert_eq!(alignment.first_lba, 63);
        assert_eq!(alignment.alignment_type, AlignmentType::DosTraditional);
        assert_eq!(alignment.heads, 255);
        assert_eq!(alignment.sectors_per_track, 63);
        assert_eq!(alignment.alignment_sectors, 16065);
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
}
