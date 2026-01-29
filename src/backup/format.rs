use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::Local;

use crate::partition::mbr::Mbr;
use crate::partition::gpt::Gpt;

/// Create the backup output folder. Returns the path to the created directory.
pub fn create_backup_folder(dest: &Path, name: &str) -> Result<PathBuf> {
    let folder = dest.join(name);
    fs::create_dir_all(&folder)
        .with_context(|| format!("failed to create backup folder: {}", folder.display()))?;
    Ok(folder)
}

/// Generate a backup folder name from the source path, e.g. `disk2-2026-01-29-143052`.
pub fn generate_backup_name(source: &Path) -> String {
    let stem = source
        .file_stem()
        .or_else(|| source.file_name())
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "backup".to_string());
    let timestamp = Local::now().format("%Y-%m-%d-%H%M%S");
    format!("{stem}-{timestamp}")
}

/// Export an MBR to `mbr.bin` (raw 512 bytes) and `mbr.json` (structured).
pub fn export_mbr(mbr: &Mbr, raw_bytes: &[u8; 512], folder: &Path) -> Result<()> {
    // Write raw MBR binary
    let bin_path = folder.join("mbr.bin");
    fs::write(&bin_path, raw_bytes)
        .with_context(|| format!("failed to write {}", bin_path.display()))?;

    // Write structured JSON
    let json_path = folder.join("mbr.json");
    let json = serde_json::to_string_pretty(mbr)
        .context("failed to serialize MBR to JSON")?;
    fs::write(&json_path, json)
        .with_context(|| format!("failed to write {}", json_path.display()))?;

    Ok(())
}

/// Export a GPT to `gpt.json` and its protective MBR to `mbr.bin`.
pub fn export_gpt(
    gpt: &Gpt,
    protective_mbr_bytes: &[u8; 512],
    folder: &Path,
) -> Result<()> {
    // Write protective MBR binary
    let bin_path = folder.join("mbr.bin");
    fs::write(&bin_path, protective_mbr_bytes)
        .with_context(|| format!("failed to write {}", bin_path.display()))?;

    // Write GPT JSON
    let json_path = folder.join("gpt.json");
    let json = serde_json::to_string_pretty(gpt)
        .context("failed to serialize GPT to JSON")?;
    fs::write(&json_path, json)
        .with_context(|| format!("failed to write {}", json_path.display()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_generate_backup_name() {
        let name = generate_backup_name(Path::new("/dev/disk2"));
        assert!(name.starts_with("disk2-"));
        // Should contain a date pattern like 2026-01-29
        assert!(name.len() > 10);
    }

    #[test]
    fn test_generate_backup_name_from_image() {
        let name = generate_backup_name(Path::new("/home/user/my-disk.img"));
        assert!(name.starts_with("my-disk-"));
    }

    #[test]
    fn test_create_backup_folder() {
        let tmp = TempDir::new().unwrap();
        let folder = create_backup_folder(tmp.path(), "test-backup").unwrap();
        assert!(folder.exists());
        assert!(folder.is_dir());
        assert_eq!(folder.file_name().unwrap(), "test-backup");
    }

    #[test]
    fn test_export_mbr() {
        let tmp = TempDir::new().unwrap();
        let folder = tmp.path().join("backup");
        fs::create_dir(&folder).unwrap();

        // Build a minimal valid MBR
        let mut data = [0u8; 512];
        data[440..444].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());
        // One FAT32 partition
        let offset = 446;
        data[offset] = 0x80; // bootable
        data[offset + 4] = 0x0C; // FAT32 LBA
        data[offset + 8..offset + 12].copy_from_slice(&2048u32.to_le_bytes());
        data[offset + 12..offset + 16].copy_from_slice(&1048576u32.to_le_bytes());
        data[510] = 0x55;
        data[511] = 0xAA;

        let mbr = Mbr::parse(&data).unwrap();
        export_mbr(&mbr, &data, &folder).unwrap();

        assert!(folder.join("mbr.bin").exists());
        assert!(folder.join("mbr.json").exists());

        let bin = fs::read(folder.join("mbr.bin")).unwrap();
        assert_eq!(bin.len(), 512);
        assert_eq!(&bin[510..512], &[0x55, 0xAA]);

        let json: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(folder.join("mbr.json")).unwrap()).unwrap();
        assert_eq!(json["disk_signature"], 0xDEADBEEFu32);
    }
}
