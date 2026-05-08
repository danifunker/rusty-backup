use serde::{Deserialize, Serialize};

/// Backup folder layout. Selects how partition data is stored on disk.
///
/// `PerPartition` is the layout used by Zstd / Raw / per-partition VHD
/// output: one compressed file per selected partition (`partition-0.zst`,
/// `partition-1.zst`, …) plus a `metadata.json` index. Defaults via
/// `#[serde(default)]` so existing folders load unchanged.
///
/// `SingleFileChd` is the new CHD-only layout introduced for chdman/MAME
/// compatibility: a single `disk.chd` containing a real disk image (partition
/// table at sector 0, gaps zero-filled). Other compression types stay on
/// `PerPartition`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum BackupLayout {
    #[default]
    PerPartition,
    SingleFileChd,
}

/// Partition-size policy applied at backup time. Recorded for traceability —
/// the actual sizes live on each `PartitionMetadata` entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SizePolicy {
    /// Each partition keeps its source size.
    Original,
    /// Each partition shrunk to its filesystem's minimum, plus 20% headroom.
    /// Falls back to `Original` for partition types that can't be shrunk.
    MinPlus20,
    /// User-supplied size per partition.
    Custom,
}

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
    /// Backup folder layout. Defaults to `PerPartition` so existing
    /// `metadata.json` files load unchanged.
    #[serde(default)]
    pub layout: BackupLayout,
    /// For `SingleFileChd`: the relative filename of the CHD container
    /// (typically `"disk.chd"`). `None` for `PerPartition`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,
    /// For `SingleFileChd`: total logical bytes of the synthesised disk image
    /// stored in the CHD. `None` for `PerPartition`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_logical_size: Option<u64>,
    /// For `SingleFileChd`: hex SHA1 reported by the CHD itself (the value
    /// `chdman info` prints). `None` for `PerPartition`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_sha1: Option<String>,
    /// Sizing policy chosen at backup time. `None` means the field wasn't set
    /// by the writer (older metadata or non-CHD output).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_policy: Option<SizePolicy>,
    pub alignment: AlignmentMetadata,
    pub partitions: Vec<PartitionMetadata>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bad_sectors: Vec<BadSectorEntry>,
    /// Extended partition container info (MBR only). Stored so restore can
    /// reconstruct the container entry and EBR chain when logical partitions
    /// are resized.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extended_container: Option<ExtendedContainerMetadata>,
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
    /// True for logical partitions inside an extended container (MBR only).
    #[serde(default)]
    pub is_logical: bool,
    /// APM partition type string (e.g. "Apple_HFS"). None for MBR/GPT.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_type_string: Option<String>,
    /// Minimum bytes needed to hold the filesystem data (from `last_data_byte`).
    /// Populated during backup for all supported filesystem types.
    /// Used to offer shrink-to-minimum restore when `imaged_size_bytes` equals
    /// `original_size_bytes` (layout-preserving or sector-by-sector backups).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimum_size_bytes: Option<u64>,
    /// Smallest size the partition could shrink to **after a defragmenting
    /// clone** — only meaningfully different from `minimum_size_bytes` for
    /// HFS+ on fragmented volumes. Restore offers this number when the user
    /// selects "Restore from defrag-clone" (see Phase 8). `None` for
    /// filesystems without a clone-shrink path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub defragmented_min_size_bytes: Option<u64>,
    /// HFS+/HFSX volume header signature (`0x482B` for plain HFS+,
    /// `0x4858` for HFSX). Captured at backup time so restore can warn
    /// when the user picks a destination that doesn't match the source's
    /// case-sensitivity flavor. `None` for non-HFS+/HFSX partitions and
    /// for pre-Step-20 backups (`#[serde(default)]`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hfsplus_signature: Option<u16>,
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

/// Metadata for the extended partition container (MBR type 0x05/0x0F/0x85).
/// Stored so that restore can regenerate the container MBR entry and EBR chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtendedContainerMetadata {
    /// MBR primary entry index (0-3) where the extended container lives.
    pub mbr_index: usize,
    /// Original partition type byte (0x05, 0x0F, or 0x85).
    pub partition_type_byte: u8,
    /// Original start LBA of the extended container.
    pub start_lba: u64,
    /// Original size of the extended container in bytes.
    pub size_bytes: u64,
}

/// A bad sector encountered during backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BadSectorEntry {
    pub partition: usize,
    pub sector: u64,
    pub lba: u64,
    pub timestamp: String,
}

/// Update the checksum for a specific partition in a metadata.json file.
pub fn update_partition_checksum(
    metadata_path: &std::path::Path,
    partition_index: usize,
    new_checksum: &str,
    new_compressed_files: Option<&[String]>,
) -> anyhow::Result<()> {
    let data = std::fs::read_to_string(metadata_path)
        .map_err(|e| anyhow::anyhow!("failed to read metadata: {e}"))?;
    let mut metadata: BackupMetadata = serde_json::from_str(&data)
        .map_err(|e| anyhow::anyhow!("failed to parse metadata: {e}"))?;

    let part = metadata
        .partitions
        .iter_mut()
        .find(|p| p.index == partition_index)
        .ok_or_else(|| anyhow::anyhow!("partition {} not found in metadata", partition_index))?;

    part.checksum = new_checksum.to_string();
    if let Some(files) = new_compressed_files {
        part.compressed_files = files.to_vec();
    }

    let json = serde_json::to_string_pretty(&metadata)?;
    std::fs::write(metadata_path, json)?;
    Ok(())
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
            layout: BackupLayout::PerPartition,
            container: None,
            container_logical_size: None,
            container_sha1: None,
            size_policy: None,
            alignment: AlignmentMetadata {
                detected_type: "DOS Traditional (255x63)".to_string(),
                first_partition_lba: 63,
                alignment_sectors: 16065,
                heads: 255,
                sectors_per_track: 63,
            },
            partitions: vec![PartitionMetadata {
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
                is_logical: false,
                partition_type_string: None,
                minimum_size_bytes: Some(480_000_000),
                defragmented_min_size_bytes: None,
                hfsplus_signature: None,
            }],
            bad_sectors: vec![],
            extended_container: None,
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
    fn test_hfsplus_signature_round_trip() {
        // Step 20: HFSX volumes (signature 0x4858) and plain HFS+ (0x482B)
        // round-trip through serde, and pre-Step-20 metadata without the
        // field loads as None thanks to `#[serde(default)]`.
        let mut meta = sample_metadata();
        meta.partitions[0].hfsplus_signature = Some(0x4858);

        let json = serde_json::to_string(&meta).unwrap();
        assert!(json.contains("hfsplus_signature"));
        let parsed: BackupMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.partitions[0].hfsplus_signature, Some(0x4858));

        // Drop the field from JSON entirely.
        let stripped = json.replace(",\n  \"hfsplus_signature\": 18520", "");
        let stripped = stripped.replace(",\"hfsplus_signature\":18520", "");
        assert!(!stripped.contains("hfsplus_signature"));
        let reparsed: BackupMetadata = serde_json::from_str(&stripped).unwrap();
        assert_eq!(reparsed.partitions[0].hfsplus_signature, None);
    }

    #[test]
    fn test_defragmented_min_size_round_trip() {
        // Round-trip serialize+deserialize with a populated and a None
        // defragmented_min_size_bytes. Confirms `#[serde(default)]` lets old
        // metadata (without the field) load without breaking.
        let mut meta = sample_metadata();
        meta.partitions[0].defragmented_min_size_bytes = Some(1_234_567_890);

        let json = serde_json::to_string(&meta).unwrap();
        assert!(json.contains("defragmented_min_size_bytes"));
        let parsed: BackupMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(
            parsed.partitions[0].defragmented_min_size_bytes,
            Some(1_234_567_890)
        );

        // Now drop the field from the JSON entirely (simulating a backup
        // produced before this change) and verify it loads as None.
        let stripped = json.replace(",\n  \"defragmented_min_size_bytes\": 1234567890", "");
        let stripped = stripped.replace(",\"defragmented_min_size_bytes\":1234567890", "");
        let reparsed: BackupMetadata = serde_json::from_str(&stripped).unwrap();
        assert_eq!(reparsed.partitions[0].defragmented_min_size_bytes, None);
    }

    fn sample_metadata() -> BackupMetadata {
        BackupMetadata {
            version: 1,
            created: "2026-05-06T00:00:00Z".to_string(),
            source_device: "/dev/disk2".to_string(),
            source_size_bytes: 1_000_000_000,
            partition_table_type: "MBR".to_string(),
            checksum_type: "sha256".to_string(),
            compression_type: "zstd".to_string(),
            split_size_mib: None,
            sector_by_sector: false,
            container_logical_size: None,
            container_sha1: None,
            size_policy: None,
            alignment: AlignmentMetadata {
                detected_type: "DosTraditional".to_string(),
                first_partition_lba: 63,
                alignment_sectors: 63,
                heads: 255,
                sectors_per_track: 63,
            },
            partitions: vec![PartitionMetadata {
                index: 0,
                type_name: "HFS+".to_string(),
                partition_type_byte: 0xAF,
                start_lba: 63,
                original_size_bytes: 1_000_000_000,
                imaged_size_bytes: 1_000_000_000,
                compressed_files: vec![],
                checksum: "abc".to_string(),
                resized: false,
                compacted: true,
                is_logical: false,
                partition_type_string: None,
                minimum_size_bytes: Some(900_000_000),
                defragmented_min_size_bytes: None,
                hfsplus_signature: None,
            }],
            bad_sectors: vec![],
            extended_container: None,
            layout: BackupLayout::PerPartition,
            container: None,
        }
    }

    #[test]
    fn test_legacy_metadata_loads_with_default_layout() {
        // metadata.json from before the BackupLayout field existed must still
        // parse, with `layout` defaulting to PerPartition.
        let json = r#"{
            "version": 1,
            "created": "2024-12-01T00:00:00Z",
            "source_device": "/dev/disk2",
            "source_size_bytes": 1000000000,
            "partition_table_type": "MBR",
            "checksum_type": "sha256",
            "compression_type": "zstd",
            "split_size_mib": null,
            "alignment": {
                "detected_type": "None detected",
                "first_partition_lba": 0,
                "alignment_sectors": 0,
                "heads": 0,
                "sectors_per_track": 0
            },
            "partitions": []
        }"#;
        let parsed: BackupMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.layout, BackupLayout::PerPartition);
        assert!(parsed.container.is_none());
        assert!(parsed.container_logical_size.is_none());
        assert!(parsed.container_sha1.is_none());
        assert!(parsed.size_policy.is_none());
        assert!(!parsed.sector_by_sector);
    }

    #[test]
    fn test_single_file_chd_metadata_round_trip() {
        let metadata = BackupMetadata {
            version: 1,
            created: "2026-05-03T12:00:00Z".to_string(),
            source_device: "/dev/disk2".to_string(),
            source_size_bytes: 4_000_000_000,
            partition_table_type: "MBR".to_string(),
            checksum_type: "sha256".to_string(),
            compression_type: "chd".to_string(),
            split_size_mib: None,
            sector_by_sector: false,
            layout: BackupLayout::SingleFileChd,
            container: Some("disk.chd".to_string()),
            container_logical_size: Some(2_147_483_648),
            container_sha1: Some("0123456789abcdef0123456789abcdef01234567".to_string()),
            size_policy: Some(SizePolicy::MinPlus20),
            alignment: AlignmentMetadata {
                detected_type: "DOS Traditional (255x63)".to_string(),
                first_partition_lba: 63,
                alignment_sectors: 16065,
                heads: 255,
                sectors_per_track: 63,
            },
            partitions: vec![PartitionMetadata {
                index: 0,
                type_name: "FAT32 (LBA)".to_string(),
                partition_type_byte: 0x0C,
                start_lba: 63,
                original_size_bytes: 2_000_000_000,
                imaged_size_bytes: 600_000_000,
                compressed_files: vec![],
                checksum: "abcdef".to_string(),
                resized: true,
                compacted: true,
                is_logical: false,
                partition_type_string: None,
                minimum_size_bytes: Some(500_000_000),
                defragmented_min_size_bytes: None,
                hfsplus_signature: None,
            }],
            bad_sectors: vec![],
            extended_container: None,
        };
        let json = serde_json::to_string_pretty(&metadata).unwrap();
        // kebab-case for the layout enum. heck's kebab-case keeps trailing
        // digits attached to the previous word ("MinPlus20" -> "min-plus20").
        assert!(json.contains("\"single-file-chd\""), "json was: {json}");
        assert!(
            json.contains("\"min-plus20\"") || json.contains("\"min-plus-20\""),
            "size_policy serialization changed; json was: {json}"
        );
        let parsed: BackupMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.layout, BackupLayout::SingleFileChd);
        assert_eq!(parsed.container.as_deref(), Some("disk.chd"));
        assert_eq!(parsed.container_logical_size, Some(2_147_483_648));
        assert_eq!(parsed.size_policy, Some(SizePolicy::MinPlus20));
        assert_eq!(parsed.partitions.len(), 1);
        assert!(parsed.partitions[0].compressed_files.is_empty());
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
            layout: BackupLayout::PerPartition,
            container: None,
            container_logical_size: None,
            container_sha1: None,
            size_policy: None,
            alignment: AlignmentMetadata {
                detected_type: "None detected".to_string(),
                first_partition_lba: 0,
                alignment_sectors: 0,
                heads: 0,
                sectors_per_track: 0,
            },
            partitions: vec![],
            extended_container: None,
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
