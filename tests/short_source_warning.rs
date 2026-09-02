//! BR14: a source that ends before its partition does must be reported, not
//! silently recorded as if the missing tail were zeros.

use std::sync::{Arc, Mutex};

use rusty_backup::backup::{
    run_backup, BackupConfig, BackupProgress, ChecksumType, CompressionType, LogLevel,
};

const MIB: usize = 1024 * 1024;

/// One FAT partition at LBA 2048 declared 4 MiB long, on a file cut 1 MiB short.
fn truncated_mbr_image(path: &std::path::Path) {
    let fat = rusty_backup::fs::fat::create_blank_fat(4 * MIB as u64, Some("SHORT")).unwrap();
    let mut disk = vec![0u8; 2048 * 512];
    disk[446 + 4] = 0x06;
    disk[446 + 8..446 + 12].copy_from_slice(&2048u32.to_le_bytes());
    disk[446 + 12..446 + 16].copy_from_slice(&((4 * MIB / 512) as u32).to_le_bytes());
    disk[510] = 0x55;
    disk[511] = 0xAA;
    disk.extend_from_slice(&fat[..3 * MIB]);
    std::fs::write(path, disk).unwrap();
}

#[test]
fn a_source_shorter_than_its_partition_is_reported_with_byte_counts() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("short.img");
    truncated_mbr_image(&src);
    let dest = tmp.path().join("backups");
    std::fs::create_dir_all(&dest).unwrap();

    let progress = Arc::new(Mutex::new(BackupProgress::default()));
    let result = run_backup(
        BackupConfig {
            source_path: src,
            destination_dir: dest,
            backup_name: "short".to_string(),
            compression: CompressionType::None,
            checksum: ChecksumType::Crc32,
            split_size_mib: None,
            sector_by_sector: true,
            partition_filter: None,
            chd_options: None,
            size_policy: None,
            partition_target_sizes: None,
            shrink_to_minimum: false,
            precomputed_minimum_sizes: None,
            defrag_partition_indices: None,
            defrag_fat: false,
            keep_swap: false,
        },
        Arc::clone(&progress),
    );
    let lines: Vec<(bool, String)> = progress
        .lock()
        .unwrap()
        .log_messages
        .iter()
        .map(|m| (matches!(m.level, LogLevel::Warning), m.message.clone()))
        .collect();
    let joined: String = lines
        .iter()
        .map(|(_, m)| m.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    result.unwrap_or_else(|e| panic!("backup failed: {e:#}\n{joined}"));
    let warning = lines
        .iter()
        .find(|(w, m)| *w && m.contains("delivered"))
        .unwrap_or_else(|| panic!("no short-source warning in:\n{joined}"));
    assert!(
        warning.1.contains(&format!("{}", 3 * MIB)) && warning.1.contains(&format!("{}", 4 * MIB)),
        "{}",
        warning.1
    );
}
