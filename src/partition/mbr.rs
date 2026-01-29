use byteorder::{LittleEndian, ReadBytesExt};
use serde::Serialize;
use std::io::Cursor;

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
