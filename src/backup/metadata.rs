use serde::{Deserialize, Serialize};

/// Top-level backup metadata written to `metadata.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupMetadata {
    pub version: u32,
    pub created: String,
    pub source_device: String,
    pub source_size_bytes: u64,
    pub partition_table_type: String,
    pub checksum_type: String,
    pub compression_type: String,
    pub split_size_mib: Option<u32>,
    /// True if the backup was made in sector-by-sector mode (all sectors
    /// including blank space). False means zero blocks were skipped.
    #[serde(default)]
    pub sector_by_sector: bool,
    pub alignment: AlignmentMetadata,
    pub partitions: Vec<PartitionMetadata>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bad_sectors: Vec<BadSectorEntry>,
}

/// Per-partition metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMetadata {
    pub index: usize,
    pub type_name: String,
    /// Raw MBR partition type byte (e.g. 0x0C for FAT32 LBA). 0 for GPT.
    #[serde(default)]
    pub partition_type_byte: u8,
    pub start_lba: u64,
    pub original_size_bytes: u64,
    /// Actual bytes captured from the partition. Equals `original_size_bytes`
    /// for sector-by-sector backups; may be smaller when smart trimming is used.
    #[serde(default)]
    pub imaged_size_bytes: u64,
    pub compressed_files: Vec<String>,
    pub checksum: String,
    pub resized: bool,
    /// True if the partition was compacted (FAT defragmentation) during backup.
    #[serde(default)]
    pub compacted: bool,
}

/// Partition alignment information for the backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlignmentMetadata {
    pub detected_type: String,
    pub first_partition_lba: u64,
    pub alignment_sectors: u64,
    pub heads: u16,
    pub sectors_per_track: u16,
}

/// A bad sector encountered during backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BadSectorEntry {
    pub partition: usize,
    pub sector: u64,
    pub lba: u64,
    pub timestamp: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_round_trip() {
        let metadata = BackupMetadata {
            version: 1,
            created: "2026-01-29T14:30:52Z".to_string(),
            source_device: "/dev/disk2".to_string(),
            source_size_bytes: 4_000_000_000,
            partition_table_type: "MBR".to_string(),
            checksum_type: "sha256".to_string(),
            compression_type: "zstd".to_string(),
            split_size_mib: Some(4000),
            sector_by_sector: false,
            alignment: AlignmentMetadata {
                detected_type: "DOS Traditional (255x63)".to_string(),
                first_partition_lba: 63,
                alignment_sectors: 16065,
                heads: 255,
                sectors_per_track: 63,
            },
            partitions: vec![
                PartitionMetadata {
                    index: 0,
                    type_name: "FAT32 (LBA)".to_string(),
                    partition_type_byte: 0x0C,
                    start_lba: 63,
                    original_size_bytes: 2_000_000_000,
                    imaged_size_bytes: 500_000_000,
                    compressed_files: vec!["partition-0.zst".to_string()],
                    checksum: "abcdef1234567890".to_string(),
                    resized: false,
                    compacted: false,
                },
            ],
            bad_sectors: vec![],
        };

        let json = serde_json::to_string_pretty(&metadata).unwrap();
        let parsed: BackupMetadata = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.version, 1);
        assert_eq!(parsed.source_device, "/dev/disk2");
        assert_eq!(parsed.partitions.len(), 1);
        assert_eq!(parsed.partitions[0].index, 0);
        assert_eq!(parsed.alignment.first_partition_lba, 63);
        assert!(parsed.bad_sectors.is_empty());
    }

    #[test]
    fn test_metadata_with_bad_sectors() {
        let metadata = BackupMetadata {
            version: 1,
            created: "2026-01-29T14:30:52Z".to_string(),
            source_device: "test.img".to_string(),
            source_size_bytes: 512_000,
            partition_table_type: "MBR".to_string(),
            checksum_type: "crc32".to_string(),
            compression_type: "none".to_string(),
            split_size_mib: None,
            sector_by_sector: true,
            alignment: AlignmentMetadata {
                detected_type: "None detected".to_string(),
                first_partition_lba: 0,
                alignment_sectors: 0,
                heads: 0,
                sectors_per_track: 0,
            },
            partitions: vec![],
            bad_sectors: vec![BadSectorEntry {
                partition: 0,
                sector: 100,
                lba: 163,
                timestamp: "2026-01-29T14:31:00Z".to_string(),
            }],
        };

        let json = serde_json::to_string_pretty(&metadata).unwrap();
        let parsed: BackupMetadata = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.bad_sectors.len(), 1);
        assert_eq!(parsed.bad_sectors[0].lba, 163);
    }
}
