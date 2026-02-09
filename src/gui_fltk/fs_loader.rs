use anyhow::{Context, Result};
/// Helper functions for loading filesystems from backup partitions
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

use rusty_backup::fs::filesystem::Filesystem;
use rusty_backup::rbformats;

/// Load a filesystem from a backed-up partition.
///
/// Finds the partition file (partition-N.chd, partition-N.zst, etc.),
/// decompresses it to a temporary file if needed, and opens the filesystem.
pub fn load_filesystem_from_backup(
    backup_folder: &Path,
    partition_index: usize,
    partition_type_byte: u8,
    compression_type: &str,
) -> Result<Box<dyn Filesystem>> {
    // Find the partition file
    let extension = match compression_type {
        "chd" => "chd",
        "zstd" => "zst",
        "vhd" => "vhd",
        "none" => "raw",
        _ => "raw",
    };

    let partition_file = backup_folder.join(format!("partition-{}.{}", partition_index, extension));

    if !partition_file.exists() {
        anyhow::bail!("Partition file not found: {}", partition_file.display());
    }

    // For now, decompress to a temporary file
    // TODO: Support in-memory decompression or streaming for better performance
    let temp_dir = std::env::temp_dir();
    let temp_file = temp_dir.join(format!("rusty-backup-partition-{}.tmp", partition_index));

    // Decompress the partition
    {
        let mut output =
            BufWriter::new(File::create(&temp_file).context("Failed to create temporary file")?);

        let mut progress = |_bytes: u64| {};
        let cancel_check = || false;
        let mut log = |_msg: &str| {};

        rbformats::decompress_to_writer(
            &partition_file,
            compression_type,
            &mut output,
            None, // No size limit
            &mut progress,
            &cancel_check,
            &mut log,
        )
        .context("Failed to decompress partition")?;
    }

    // Open the decompressed file
    let file = File::open(&temp_file).context("Failed to open decompressed partition")?;

    let reader = BufReader::new(file);

    // Open the filesystem
    let filesystem = rusty_backup::fs::open_filesystem(
        reader,
        0, // Partition offset (already at partition start)
        partition_type_byte,
        None, // No partition type string
    )
    .context("Failed to open filesystem")?;

    Ok(filesystem)
}

/// Delete temporary partition files created during browsing
pub fn cleanup_temp_files() {
    let temp_dir = std::env::temp_dir();

    // Find and delete any rusty-backup-partition-*.tmp files
    if let Ok(entries) = std::fs::read_dir(&temp_dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with("rusty-backup-partition-") && name.ends_with(".tmp") {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
    }
}
