use byteorder::{LittleEndian, ReadBytesExt};
use serde::Serialize;
use std::collections::HashSet;
use std::io::{Cursor, Read, Seek, SeekFrom};

use crate::error::RustyBackupError;

const MBR_SIGNATURE: u16 = 0xAA55;
const PARTITION_TABLE_OFFSET: usize = 446;
const PARTITION_ENTRY_SIZE: usize = 16;

/// CHS (Cylinder-Head-Sector) address extracted from MBR partition entry.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct ChsAddress {
    pub head: u8,
    pub sector: u8,   // bits 0-5 only (6 bits)
    pub cylinder: u16, // 10 bits: 2 high bits from sector byte + 8 bits from cylinder byte
}

impl ChsAddress {
    fn parse(bytes: &[u8; 3]) -> Self {
        let head = bytes[0];
        let sector = bytes[1] & 0x3F;
        let cylinder = ((bytes[1] as u16 & 0xC0) << 2) | bytes[2] as u16;
        Self {
            head,
            sector,
            cylinder,
        }
    }
}

/// A single MBR partition table entry.
#[derive(Debug, Clone, Serialize)]
pub struct MbrPartitionEntry {
    pub bootable: bool,
    pub partition_type: u8,
    pub start_lba: u32,
    pub total_sectors: u32,
    pub chs_start: ChsAddress,
    pub chs_end: ChsAddress,
}

impl MbrPartitionEntry {
    fn parse(data: &[u8; PARTITION_ENTRY_SIZE]) -> Self {
        let bootable = data[0] == 0x80;
        let chs_start = ChsAddress::parse(&[data[1], data[2], data[3]]);
        let partition_type = data[4];
        let chs_end = ChsAddress::parse(&[data[5], data[6], data[7]]);

        let mut cursor = Cursor::new(&data[8..16]);
        let start_lba = cursor.read_u32::<LittleEndian>().unwrap();
        let total_sectors = cursor.read_u32::<LittleEndian>().unwrap();

        Self {
            bootable,
            partition_type,
            start_lba,
            total_sectors,
            chs_start,
            chs_end,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.partition_type == 0x00 && self.start_lba == 0 && self.total_sectors == 0
    }

    /// Check if this entry is an extended partition container (CHS, LBA, or Linux).
    pub fn is_extended(&self) -> bool {
        matches!(self.partition_type, 0x05 | 0x0F | 0x85)
    }

    pub fn size_bytes(&self) -> u64 {
        self.total_sectors as u64 * 512
    }

    pub fn partition_type_name(&self) -> &'static str {
        match self.partition_type {
            0x00 => "Empty",
            0x01 => "FAT12",
            0x04 => "FAT16 (<32MB)",
            0x05 => "Extended",
            0x06 => "FAT16 (>32MB)",
            0x07 => "NTFS/HPFS/exFAT",
            0x0B => "FAT32 (CHS)",
            0x0C => "FAT32 (LBA)",
            0x0E => "FAT16 (LBA)",
            0x0F => "Extended (LBA)",
            0x11 => "Hidden FAT12",
            0x14 => "Hidden FAT16 (<32MB)",
            0x16 => "Hidden FAT16 (>32MB)",
            0x1B => "Hidden FAT32 (CHS)",
            0x1C => "Hidden FAT32 (LBA)",
            0x1E => "Hidden FAT16 (LBA)",
            0x27 => "Windows RE",
            0x42 => "Dynamic Disk",
            0x82 => "Linux swap",
            0x83 => "Linux",
            0x85 => "Linux Extended",
            0x8E => "Linux LVM",
            0xA5 => "FreeBSD",
            0xA6 => "OpenBSD",
            0xAF => "HFS/HFS+",
            0xEE => "GPT Protective",
            0xEF => "EFI System",
            0xFD => "Linux RAID",
            _ => "Unknown",
        }
    }
}

/// Parsed MBR (Master Boot Record).
#[derive(Debug, Clone, Serialize)]
pub struct Mbr {
    pub disk_signature: u32,
    pub entries: [MbrPartitionEntry; 4],
    /// Logical partitions found by following the EBR chain of any extended
    /// partition entry. Populated after initial parse by `parse_ebr_chain()`.
    /// LBA values are absolute (already adjusted from EBR-relative offsets).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub logical_partitions: Vec<MbrPartitionEntry>,
}

impl Mbr {
    /// Parse an MBR from exactly 512 bytes.
    pub fn parse(data: &[u8; 512]) -> Result<Self, RustyBackupError> {
        // Validate boot signature at bytes 510-511
        let mut sig_cursor = Cursor::new(&data[510..512]);
        let signature = sig_cursor.read_u16::<LittleEndian>().unwrap();
        if signature != MBR_SIGNATURE {
            return Err(RustyBackupError::InvalidMbr(format!(
                "invalid boot signature: expected 0xAA55, got {:#06X}",
                signature
            )));
        }

        // Read disk signature at offset 440
        let mut ds_cursor = Cursor::new(&data[440..444]);
        let disk_signature = ds_cursor.read_u32::<LittleEndian>().unwrap();

        // Parse 4 partition entries starting at offset 446
        let mut entries: [MbrPartitionEntry; 4] = std::array::from_fn(|i| {
            let offset = PARTITION_TABLE_OFFSET + i * PARTITION_ENTRY_SIZE;
            let entry_data: [u8; PARTITION_ENTRY_SIZE] =
                data[offset..offset + PARTITION_ENTRY_SIZE].try_into().unwrap();
            MbrPartitionEntry::parse(&entry_data)
        });

        // Sort non-empty entries to the front (preserving order)
        let _ = &mut entries; // entries already in table order

        Ok(Self {
            disk_signature,
            entries,
            logical_partitions: Vec::new(),
        })
    }

    /// Check if this MBR is a GPT protective MBR.
    pub fn is_protective_gpt(&self) -> bool {
        let non_empty: Vec<_> = self.entries.iter().filter(|e| !e.is_empty()).collect();
        non_empty.len() == 1 && non_empty[0].partition_type == 0xEE
    }

    /// Return only non-empty partition entries.
    pub fn active_entries(&self) -> Vec<&MbrPartitionEntry> {
        self.entries.iter().filter(|e| !e.is_empty()).collect()
    }
}

/// Maximum number of logical partitions to prevent infinite loops on corrupted data.
const MAX_LOGICAL_PARTITIONS: usize = 64;

/// Parse the Extended Boot Record (EBR) chain starting at `extended_start_lba`.
///
/// Each EBR is a 512-byte MBR-like structure at the start of an extended partition
/// region. It uses only the first two of the four partition table entries:
/// - Entry 0: describes the logical partition (start LBA relative to this EBR)
/// - Entry 1: link to the next EBR (start LBA relative to the extended container)
///
/// The chain ends when the link entry is empty or we revisit an LBA.
/// Returns logical partition entries with absolute LBA addresses.
pub fn parse_ebr_chain(
    reader: &mut (impl Read + Seek),
    extended_start_lba: u32,
) -> Result<Vec<MbrPartitionEntry>, RustyBackupError> {
    let mut logical_partitions = Vec::new();
    let mut visited = HashSet::new();
    let mut current_ebr_lba = extended_start_lba;

    loop {
        if logical_partitions.len() >= MAX_LOGICAL_PARTITIONS {
            break;
        }

        // Prevent infinite loops
        if !visited.insert(current_ebr_lba) {
            break;
        }

        // Seek to the EBR
        let offset = current_ebr_lba as u64 * 512;
        reader
            .seek(SeekFrom::Start(offset))
            .map_err(|e| RustyBackupError::Io(e))?;

        let mut ebr_data = [0u8; 512];
        if reader.read_exact(&mut ebr_data).is_err() {
            // Can't read this EBR, end of chain
            break;
        }

        // Check boot signature (some disks omit it on the last EBR)
        let mut sig_cursor = Cursor::new(&ebr_data[510..512]);
        let signature = sig_cursor.read_u16::<LittleEndian>().unwrap();
        if signature != MBR_SIGNATURE {
            break;
        }

        // Parse entry 0: the logical partition descriptor
        let entry0_data: [u8; PARTITION_ENTRY_SIZE] = ebr_data
            [PARTITION_TABLE_OFFSET..PARTITION_TABLE_OFFSET + PARTITION_ENTRY_SIZE]
            .try_into()
            .unwrap();
        let entry0 = MbrPartitionEntry::parse(&entry0_data);

        // Parse entry 1: the next EBR link
        let entry1_offset = PARTITION_TABLE_OFFSET + PARTITION_ENTRY_SIZE;
        let entry1_data: [u8; PARTITION_ENTRY_SIZE] = ebr_data
            [entry1_offset..entry1_offset + PARTITION_ENTRY_SIZE]
            .try_into()
            .unwrap();
        let entry1 = MbrPartitionEntry::parse(&entry1_data);

        // Entry 0's start_lba is relative to current EBR position
        if !entry0.is_empty() {
            let mut logical = entry0;
            logical.start_lba = current_ebr_lba + logical.start_lba;
            logical_partitions.push(logical);
        }

        // Entry 1's start_lba is relative to the extended container start
        if entry1.is_empty() {
            break;
        }
        current_ebr_lba = extended_start_lba + entry1.start_lba;
    }

    Ok(logical_partitions)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_mbr_bytes(entries: &[(u8, u8, u32, u32)], signature: u16) -> [u8; 512] {
        let mut data = [0u8; 512];

        // Disk signature
        data[440..444].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());

        // Partition entries
        for (i, (status, ptype, start_lba, sectors)) in entries.iter().enumerate() {
            let offset = PARTITION_TABLE_OFFSET + i * PARTITION_ENTRY_SIZE;
            data[offset] = *status; // bootable flag
            // CHS start (dummy: head=1, sector=1, cylinder=0)
            data[offset + 1] = 1;
            data[offset + 2] = 1;
            data[offset + 3] = 0;
            data[offset + 4] = *ptype; // partition type
            // CHS end (dummy)
            data[offset + 5] = 254;
            data[offset + 6] = 63;
            data[offset + 7] = 100;
            data[offset + 8..offset + 12].copy_from_slice(&start_lba.to_le_bytes());
            data[offset + 12..offset + 16].copy_from_slice(&sectors.to_le_bytes());
        }

        // Boot signature
        data[510..512].copy_from_slice(&signature.to_le_bytes());
        data
    }

    #[test]
    fn test_valid_mbr_single_fat32() {
        let data = make_mbr_bytes(&[(0x80, 0x0C, 2048, 1048576)], 0xAA55);
        let mbr = Mbr::parse(&data).unwrap();

        assert_eq!(mbr.disk_signature, 0xDEADBEEF);
        assert!(mbr.entries[0].bootable);
        assert_eq!(mbr.entries[0].partition_type, 0x0C);
        assert_eq!(mbr.entries[0].partition_type_name(), "FAT32 (LBA)");
        assert_eq!(mbr.entries[0].start_lba, 2048);
        assert_eq!(mbr.entries[0].total_sectors, 1048576);
        assert_eq!(mbr.entries[0].size_bytes(), 1048576 * 512);
        assert!(!mbr.entries[0].is_empty());
        assert!(mbr.entries[1].is_empty());
        assert!(mbr.entries[2].is_empty());
        assert!(mbr.entries[3].is_empty());
        assert!(!mbr.is_protective_gpt());
    }

    #[test]
    fn test_protective_gpt_mbr() {
        let data = make_mbr_bytes(&[(0x00, 0xEE, 1, 0xFFFFFFFF)], 0xAA55);
        let mbr = Mbr::parse(&data).unwrap();

        assert!(mbr.is_protective_gpt());
        assert_eq!(mbr.active_entries().len(), 1);
        assert_eq!(mbr.active_entries()[0].partition_type_name(), "GPT Protective");
    }

    #[test]
    fn test_invalid_signature() {
        let data = make_mbr_bytes(&[], 0x0000);
        let result = Mbr::parse(&data);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("invalid boot signature"));
    }

    #[test]
    fn test_empty_mbr() {
        let data = make_mbr_bytes(&[], 0xAA55);
        let mbr = Mbr::parse(&data).unwrap();

        assert!(mbr.active_entries().is_empty());
        assert!(!mbr.is_protective_gpt());
    }

    #[test]
    fn test_multiple_partitions() {
        let data = make_mbr_bytes(
            &[
                (0x80, 0x06, 63, 1024000),       // FAT16, DOS alignment
                (0x00, 0x0B, 1024063, 2048000),   // FAT32
            ],
            0xAA55,
        );
        let mbr = Mbr::parse(&data).unwrap();

        assert_eq!(mbr.active_entries().len(), 2);
        assert_eq!(mbr.entries[0].partition_type_name(), "FAT16 (>32MB)");
        assert_eq!(mbr.entries[1].partition_type_name(), "FAT32 (CHS)");
        assert_eq!(mbr.entries[0].start_lba, 63);
    }

    #[test]
    fn test_is_extended() {
        let data = make_mbr_bytes(
            &[
                (0x80, 0x06, 63, 1024000),   // FAT16 - not extended
                (0x00, 0x05, 1024063, 4096000), // Extended (CHS)
            ],
            0xAA55,
        );
        let mbr = Mbr::parse(&data).unwrap();
        assert!(!mbr.entries[0].is_extended());
        assert!(mbr.entries[1].is_extended());
    }

    /// Build a disk image with an MBR containing an extended partition and
    /// an EBR chain with the given logical partitions.
    /// Returns a Vec<u8> large enough to hold all EBR sectors.
    fn make_disk_with_ebr(
        primary_entries: &[(u8, u8, u32, u32)],
        extended_start_lba: u32,
        // Each logical: (type, relative_start_from_ebr, sector_count)
        logical_entries: &[(u8, u32, u32)],
    ) -> Vec<u8> {
        // Calculate needed size: enough to hold all EBR sectors
        let max_lba = if logical_entries.is_empty() {
            extended_start_lba + 1
        } else {
            // Each EBR is at extended_start_lba + some offset; we need enough space
            let mut max = extended_start_lba + 1;
            let mut ebr_offset = 0u32;
            for (i, _) in logical_entries.iter().enumerate() {
                let ebr_lba = extended_start_lba + ebr_offset;
                max = max.max(ebr_lba + 1);
                // Next EBR offset: just put them 2048 sectors apart
                if i < logical_entries.len() - 1 {
                    ebr_offset += 2048;
                }
            }
            max + 2048 // extra space
        };

        let size = max_lba as usize * 512;
        let mut disk = vec![0u8; size];

        // Write MBR
        let mbr = make_mbr_bytes(primary_entries, 0xAA55);
        disk[..512].copy_from_slice(&mbr);

        // Write EBR chain
        let mut ebr_offset = 0u32;
        for (i, &(ptype, rel_start, sectors)) in logical_entries.iter().enumerate() {
            let ebr_lba = extended_start_lba + ebr_offset;
            let ebr_byte_offset = ebr_lba as usize * 512;

            // Boot signature
            disk[ebr_byte_offset + 510] = 0x55;
            disk[ebr_byte_offset + 511] = 0xAA;

            // Entry 0: logical partition (relative to this EBR)
            let e0_off = ebr_byte_offset + PARTITION_TABLE_OFFSET;
            disk[e0_off + 4] = ptype;
            disk[e0_off + 8..e0_off + 12].copy_from_slice(&rel_start.to_le_bytes());
            disk[e0_off + 12..e0_off + 16].copy_from_slice(&sectors.to_le_bytes());

            // Entry 1: link to next EBR (relative to extended container start)
            if i + 1 < logical_entries.len() {
                let next_ebr_offset = ebr_offset + 2048;
                let e1_off = ebr_byte_offset + PARTITION_TABLE_OFFSET + PARTITION_ENTRY_SIZE;
                disk[e1_off + 4] = 0x05; // extended type
                disk[e1_off + 8..e1_off + 12].copy_from_slice(&next_ebr_offset.to_le_bytes());
                disk[e1_off + 12..e1_off + 16].copy_from_slice(&2048u32.to_le_bytes());
            }

            ebr_offset += 2048;
        }

        disk
    }

    #[test]
    fn test_ebr_single_logical() {
        let disk = make_disk_with_ebr(
            &[
                (0x80, 0x06, 63, 1024000),       // FAT16
                (0x00, 0x05, 1024063, 4096000),   // Extended
            ],
            1024063,
            &[(0x0B, 1, 2048000)], // One FAT32 logical
        );
        let mut cursor = std::io::Cursor::new(disk);
        let result = parse_ebr_chain(&mut cursor, 1024063).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].partition_type, 0x0B);
        // Absolute LBA = EBR LBA (1024063) + relative start (1)
        assert_eq!(result[0].start_lba, 1024064);
        assert_eq!(result[0].total_sectors, 2048000);
    }

    #[test]
    fn test_ebr_three_logicals() {
        let disk = make_disk_with_ebr(
            &[
                (0x80, 0x06, 63, 1024000),
                (0x00, 0x05, 1024063, 8192000),
            ],
            1024063,
            &[
                (0x06, 1, 1000000),   // FAT16
                (0x0B, 1, 2000000),   // FAT32
                (0x83, 1, 3000000),   // Linux
            ],
        );
        let mut cursor = std::io::Cursor::new(disk);
        let result = parse_ebr_chain(&mut cursor, 1024063).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].partition_type_name(), "FAT16 (>32MB)");
        assert_eq!(result[0].start_lba, 1024063 + 1); // first EBR + 1
        assert_eq!(result[1].partition_type_name(), "FAT32 (CHS)");
        assert_eq!(result[1].start_lba, 1024063 + 2048 + 1); // second EBR + 1
        assert_eq!(result[2].partition_type_name(), "Linux");
        assert_eq!(result[2].start_lba, 1024063 + 4096 + 1); // third EBR + 1
    }

    #[test]
    fn test_ebr_empty_extended() {
        // Extended partition with no logical partitions (empty EBR)
        let disk = make_disk_with_ebr(
            &[(0x00, 0x05, 1024063, 4096000)],
            1024063,
            &[], // No logicals
        );
        let mut cursor = std::io::Cursor::new(disk);
        let result = parse_ebr_chain(&mut cursor, 1024063).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_chs_parsing() {
        let mut data = [0u8; 512];
        data[510] = 0x55;
        data[511] = 0xAA;

        // Entry 0: CHS start = head 1, sector 1, cylinder 0
        let offset = PARTITION_TABLE_OFFSET;
        data[offset] = 0x80;
        data[offset + 1] = 1;   // head
        data[offset + 2] = 1;   // sector (bits 0-5)
        data[offset + 3] = 0;   // cylinder low byte
        data[offset + 4] = 0x0C;
        // CHS end = head 254, sector 63, cylinder 1023
        data[offset + 5] = 254;
        data[offset + 6] = 0xFF; // sector=63 (0x3F) + cylinder high bits (0xC0)
        data[offset + 7] = 0xFF; // cylinder low byte = 255
        data[offset + 8..offset + 12].copy_from_slice(&2048u32.to_le_bytes());
        data[offset + 12..offset + 16].copy_from_slice(&1048576u32.to_le_bytes());

        let mbr = Mbr::parse(&data).unwrap();
        assert_eq!(mbr.entries[0].chs_start.head, 1);
        assert_eq!(mbr.entries[0].chs_start.sector, 1);
        assert_eq!(mbr.entries[0].chs_start.cylinder, 0);
        assert_eq!(mbr.entries[0].chs_end.head, 254);
        assert_eq!(mbr.entries[0].chs_end.sector, 63);
        assert_eq!(mbr.entries[0].chs_end.cylinder, 1023);
    }
}
