use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::Path;

use anyhow::{Context, Result};

use super::ChecksumType;

const READ_BUF_SIZE: usize = 1024 * 1024; // 1 MB

/// Compute a checksum over the file at `path`.
/// Returns the hex-encoded checksum string.
pub fn compute_checksum(path: &Path, checksum_type: ChecksumType) -> Result<String> {
    let file = File::open(path)
        .with_context(|| format!("failed to open {} for checksum", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut buf = vec![0u8; READ_BUF_SIZE];

    match checksum_type {
        ChecksumType::Sha256 => {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            loop {
                let n = reader.read(&mut buf).context("checksum read error")?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            Ok(format!("{:x}", hasher.finalize()))
        }
        ChecksumType::Crc32 => {
            let mut hasher = crc32fast::Hasher::new();
            loop {
                let n = reader.read(&mut buf).context("checksum read error")?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            Ok(format!("{:08x}", hasher.finalize()))
        }
    }
}

/// Write a checksum sidecar file next to the data file.
///
/// For example, given `partition-0.zst` and SHA256, writes `partition-0.zst.sha256`
/// in the format: `{hash}  {filename}` (compatible with sha256sum/crc32).
pub fn write_checksum_file(
    checksum: &str,
    file_path: &Path,
    checksum_type: ChecksumType,
) -> Result<()> {
    let extension = match checksum_type {
        ChecksumType::Sha256 => "sha256",
        ChecksumType::Crc32 => "crc32",
    };
    let sidecar_path = file_path.with_extension(
        format!(
            "{}.{extension}",
            file_path
                .extension()
                .unwrap_or_default()
                .to_string_lossy()
        ),
    );

    let file_name = file_path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy();

    let mut f = File::create(&sidecar_path)
        .with_context(|| format!("failed to create {}", sidecar_path.display()))?;
    writeln!(f, "{checksum}  {file_name}")
        .with_context(|| format!("failed to write {}", sidecar_path.display()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_sha256_known_value() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.bin");
        // SHA-256 of empty file is well-known
        fs::write(&path, b"").unwrap();
        let checksum = compute_checksum(&path, ChecksumType::Sha256).unwrap();
        assert_eq!(
            checksum,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_sha256_hello() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("hello.bin");
        fs::write(&path, b"hello").unwrap();
        let checksum = compute_checksum(&path, ChecksumType::Sha256).unwrap();
        assert_eq!(
            checksum,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn test_crc32_known_value() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.bin");
        fs::write(&path, b"").unwrap();
        let checksum = compute_checksum(&path, ChecksumType::Crc32).unwrap();
        assert_eq!(checksum, "00000000");
    }

    #[test]
    fn test_crc32_hello() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("hello.bin");
        fs::write(&path, b"hello").unwrap();
        let checksum = compute_checksum(&path, ChecksumType::Crc32).unwrap();
        // CRC32 of "hello"
        assert_eq!(checksum, "3610a686");
    }

    #[test]
    fn test_write_checksum_file_sha256() {
        let tmp = TempDir::new().unwrap();
        let data_path = tmp.path().join("partition-0.zst");
        fs::write(&data_path, b"test data").unwrap();

        write_checksum_file("abcdef1234567890", &data_path, ChecksumType::Sha256).unwrap();

        let sidecar = tmp.path().join("partition-0.zst.sha256");
        assert!(sidecar.exists());
        let content = fs::read_to_string(&sidecar).unwrap();
        assert_eq!(content, "abcdef1234567890  partition-0.zst\n");
    }

    #[test]
    fn test_write_checksum_file_crc32() {
        let tmp = TempDir::new().unwrap();
        let data_path = tmp.path().join("partition-0.raw");
        fs::write(&data_path, b"test data").unwrap();

        write_checksum_file("deadbeef", &data_path, ChecksumType::Crc32).unwrap();

        let sidecar = tmp.path().join("partition-0.raw.crc32");
        assert!(sidecar.exists());
        let content = fs::read_to_string(&sidecar).unwrap();
        assert_eq!(content, "deadbeef  partition-0.raw\n");
    }
}
