use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::Local;

use crate::partition::apm::Apm;
use crate::partition::gpt::Gpt;
use crate::partition::mbr::Mbr;

/// Create the backup output folder. Returns the path to the created directory.
pub fn create_backup_folder(dest: &Path, name: &str) -> Result<PathBuf> {
    let folder = dest.join(name);
    fs::create_dir_all(&folder)
        .with_context(|| format!("failed to create backup folder: {}", folder.display()))?;
    Ok(folder)
}

/// Generate a backup folder name.
///
/// Format: `SIZE-PartName-YYYY-MM-DD-HHMM` (local time, no seconds).
/// `PartName` is the volume label from the first partition. Falls back to the
/// source file stem when no volume label is available.
pub fn generate_backup_name(
    source: &Path,
    size_bytes: Option<u64>,
    volume_label: Option<&str>,
) -> String {
    let timestamp = Local::now().format("%Y-%m-%d-%H%M");

    let size_part = size_bytes.map(format_size_short);

    let label_part = volume_label
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .map(sanitize_filename);

    match (size_part, label_part) {
        (Some(sz), Some(label)) => format!("{sz}-{label}-{timestamp}"),
        (Some(sz), None) => {
            let stem = get_source_stem(source);
            format!("{sz}-{stem}-{timestamp}")
        }
        (None, Some(label)) => format!("{label}-{timestamp}"),
        (None, None) => {
            let stem = get_source_stem(source);
            format!("{stem}-{timestamp}")
        }
    }
}

fn get_source_stem(source: &Path) -> String {
    let stem = source
        .file_stem()
        .or_else(|| source.file_name())
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "backup".to_string());
    sanitize_filename(&stem)
}

fn sanitize_filename(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| match c {
            // Replace invalid/special characters with hyphen
            ' ' | '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '-',
            c if c.is_ascii_alphanumeric() || c == '-' || c == '_' => c,
            _ => '-',
        })
        .collect();

    // Collapse consecutive hyphens and trim
    sanitized
        .split('-')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

/// Format a byte count as a short rounded size string for folder names.
/// Uses whole numbers: `8GB`, `512MB`, `16GB`.
fn format_size_short(bytes: u64) -> String {
    const GB: u64 = 1_000_000_000;
    const MB: u64 = 1_000_000;
    if bytes >= GB {
        let gb = ((bytes as f64) / (GB as f64)).round() as u64;
        format!("{gb}GB")
    } else {
        let mb = ((bytes as f64) / (MB as f64)).round() as u64;
        format!("{mb}MB")
    }
}

/// Export an MBR to `mbr.bin` (raw 512 bytes) and `mbr.json` (structured).
pub fn export_mbr(mbr: &Mbr, raw_bytes: &[u8; 512], folder: &Path) -> Result<()> {
    // Write raw MBR binary
    let bin_path = folder.join("mbr.bin");
    fs::write(&bin_path, raw_bytes)
        .with_context(|| format!("failed to write {}", bin_path.display()))?;

    // Write structured JSON
    let json_path = folder.join("mbr.json");
    let json = serde_json::to_string_pretty(mbr).context("failed to serialize MBR to JSON")?;
    fs::write(&json_path, json)
        .with_context(|| format!("failed to write {}", json_path.display()))?;

    Ok(())
}

/// Export a modified MBR to `mbr-min.bin` with partition `total_sectors`
/// reduced to the effective (imaged) sizes.
///
/// `min_sectors` is a list of `(mbr_entry_index, new_total_sectors)` where
/// each entry index refers to a primary MBR entry (0–3).  The function copies
/// the original 512-byte MBR verbatim (preserving boot code, CHS, disk
/// signature, etc.) and overwrites only the 4-byte `total_sectors` field
/// (bytes 12–15 of each 16-byte entry at offset 446) for the specified entries.
pub fn export_mbr_min(
    original_bytes: &[u8; 512],
    min_sectors: &[(usize, u32)],
    folder: &Path,
) -> Result<()> {
    let mut patched = *original_bytes;
    for &(entry_index, new_total) in min_sectors {
        let offset = 446 + entry_index * 16 + 12;
        patched[offset..offset + 4].copy_from_slice(&new_total.to_le_bytes());
    }
    let bin_path = folder.join("mbr-min.bin");
    fs::write(&bin_path, &patched)
        .with_context(|| format!("failed to write {}", bin_path.display()))?;
    Ok(())
}

/// Export a GPT to `gpt.json` and its protective MBR to `mbr.bin`.
pub fn export_gpt(gpt: &Gpt, protective_mbr_bytes: &[u8; 512], folder: &Path) -> Result<()> {
    // Write protective MBR binary
    let bin_path = folder.join("mbr.bin");
    fs::write(&bin_path, protective_mbr_bytes)
        .with_context(|| format!("failed to write {}", bin_path.display()))?;

    // Write GPT JSON
    let json_path = folder.join("gpt.json");
    let json = serde_json::to_string_pretty(gpt).context("failed to serialize GPT to JSON")?;
    fs::write(&json_path, json)
        .with_context(|| format!("failed to write {}", json_path.display()))?;

    Ok(())
}

/// Export raw GPT sectors (LBAs 0-33) to `gpt.bin`.
/// This preserves the exact on-disk GPT structures for byte-accurate restore.
pub fn export_gpt_bin(
    reader: &mut (impl std::io::Read + std::io::Seek),
    folder: &Path,
) -> Result<()> {
    use std::io::SeekFrom;

    reader
        .seek(SeekFrom::Start(0))
        .context("failed to seek to start for GPT export")?;

    // Read LBAs 0-33 (protective MBR + GPT header + partition entries)
    let mut buf = vec![0u8; 34 * 512];
    reader
        .read_exact(&mut buf)
        .context("failed to read GPT sectors")?;

    let bin_path = folder.join("gpt.bin");
    fs::write(&bin_path, &buf)
        .with_context(|| format!("failed to write {}", bin_path.display()))?;

    Ok(())
}

/// Export an APM to `apm.json` (structured) and `apm.bin` (raw DDR + map entries).
pub fn export_apm(apm: &Apm, folder: &Path) -> Result<()> {
    // Write structured JSON
    let json_path = folder.join("apm.json");
    let json = serde_json::to_string_pretty(apm).context("failed to serialize APM to JSON")?;
    fs::write(&json_path, json)
        .with_context(|| format!("failed to write {}", json_path.display()))?;

    // Write raw APM blocks (DDR + partition map entries)
    let bin_path = folder.join("apm.bin");
    let raw = apm.build_apm_blocks(None);
    fs::write(&bin_path, raw).with_context(|| format!("failed to write {}", bin_path.display()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_generate_backup_name_fallback() {
        let name = generate_backup_name(Path::new("/dev/disk2"), None, None);
        assert!(name.starts_with("disk2-"));
        // Format: disk2-YYYY-MM-DD-HHMM (21 chars: disk2-2026-01-01-0000)
        assert!(name.len() >= 21);
    }

    #[test]
    fn test_generate_backup_name_from_image() {
        let name = generate_backup_name(Path::new("/home/user/my-disk.img"), None, None);
        assert!(name.starts_with("my-disk-"));
    }

    #[test]
    fn test_generate_backup_name_with_size_and_label() {
        let name = generate_backup_name(
            Path::new("/dev/disk2"),
            Some(8_000_000_000),
            Some("LLAMADOS"),
        );
        assert!(name.starts_with("8GB-LLAMADOS-"));
    }

    #[test]
    fn test_generate_backup_name_size_only() {
        // When size is provided but no label, fallback to disk name
        let name = generate_backup_name(Path::new("/dev/disk2"), Some(512_000_000), None);
        assert!(name.starts_with("512MB-disk2-"));
    }

    #[test]
    fn test_generate_backup_name_label_only() {
        let name = generate_backup_name(Path::new("/dev/disk2"), None, Some("MYDISK"));
        assert!(name.starts_with("MYDISK-"));
    }

    #[test]
    fn test_sanitize_filename() {
        let name = generate_backup_name(
            Path::new("/dev/disk2"),
            Some(8_000_000_000),
            Some("MY DISK: Label*"),
        );
        // Spaces and special chars should be replaced with hyphens
        assert!(name.starts_with("8GB-MY-DISK-Label-"));
    }

    #[test]
    fn test_sanitize_filename_no_trailing_hyphens() {
        let name = generate_backup_name(
            Path::new("/dev/disk2"),
            Some(8_000_000_000),
            Some("  Label  "),
        );
        // Leading/trailing hyphens from spaces should be trimmed
        assert!(name.starts_with("8GB-Label-"));
        assert!(!name.starts_with("8GB--"));
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

    #[test]
    fn test_export_mbr_min() {
        let tmp = TempDir::new().unwrap();
        let folder = tmp.path().join("backup");
        fs::create_dir(&folder).unwrap();

        // Build a minimal valid MBR with two partitions
        let mut data = [0u8; 512];
        data[440..444].copy_from_slice(&0xCAFEBABEu32.to_le_bytes());

        // Partition 0: 1048576 sectors
        let e0 = 446;
        data[e0 + 4] = 0x0C; // FAT32 LBA
        data[e0 + 8..e0 + 12].copy_from_slice(&63u32.to_le_bytes());
        data[e0 + 12..e0 + 16].copy_from_slice(&1048576u32.to_le_bytes());

        // Partition 1: 2097152 sectors
        let e1 = 446 + 16;
        data[e1 + 4] = 0x06; // FAT16
        data[e1 + 8..e1 + 12].copy_from_slice(&1048639u32.to_le_bytes());
        data[e1 + 12..e1 + 16].copy_from_slice(&2097152u32.to_le_bytes());

        data[510] = 0x55;
        data[511] = 0xAA;

        // Patch partition 0 to 500000, partition 1 to 100000
        let min_sectors = vec![(0, 500_000u32), (1, 100_000u32)];
        export_mbr_min(&data, &min_sectors, &folder).unwrap();

        let patched = fs::read(folder.join("mbr-min.bin")).unwrap();
        assert_eq!(patched.len(), 512);
        // Boot code / signature preserved
        assert_eq!(&patched[440..444], &0xCAFEBABEu32.to_le_bytes());
        assert_eq!(&patched[510..512], &[0x55, 0xAA]);
        // Type bytes preserved
        assert_eq!(patched[e0 + 4], 0x0C);
        assert_eq!(patched[e1 + 4], 0x06);
        // Start LBAs preserved
        assert_eq!(&patched[e0 + 8..e0 + 12], &63u32.to_le_bytes());
        assert_eq!(&patched[e1 + 8..e1 + 12], &1048639u32.to_le_bytes());
        // Total sectors patched
        assert_eq!(&patched[e0 + 12..e0 + 16], &500_000u32.to_le_bytes());
        assert_eq!(&patched[e1 + 12..e1 + 16], &100_000u32.to_le_bytes());
    }
}
