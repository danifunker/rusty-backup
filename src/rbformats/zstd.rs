use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use anyhow::{bail, Context, Result};

use super::{file_name, output_path, SplitWriter, CHUNK_SIZE};

/// Compress with zstd, streaming through the encoder with optional splitting.
pub(crate) fn compress_zstd(
    reader: &mut impl Read,
    output_base: &Path,
    split_size: Option<u64>,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
) -> Result<Vec<String>> {
    let mut files = Vec::new();
    let mut total_read: u64 = 0;
    let mut part_index: u32 = 0;
    let split_bytes = split_size.unwrap_or(u64::MAX);

    let first_path = output_path(output_base, "zst", split_size.is_some(), part_index);
    let mut encoder = zstd::Encoder::new(
        SplitWriter::new(&first_path, split_bytes, &mut files, &mut part_index, output_base)?,
        3, // compression level
    )
    .context("failed to create zstd encoder")?;
    files.push(file_name(&first_path));

    let mut buf = vec![0u8; CHUNK_SIZE];
    loop {
        if cancel_check() {
            bail!("backup cancelled");
        }

        let n = reader.read(&mut buf).context("failed to read source")?;
        if n == 0 {
            break;
        }

        encoder
            .write_all(&buf[..n])
            .context("failed to write compressed data")?;
        total_read += n as u64;
        progress_cb(total_read);
    }

    encoder.finish().context("failed to finalize zstd stream")?;
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::CompressionType;
    use std::io::Cursor;
    use tempfile::TempDir;

    #[test]
    fn test_compress_zstd() {
        let tmp = TempDir::new().unwrap();
        // Highly compressible data
        let data = vec![0u8; 65536];
        let mut reader = Cursor::new(&data);
        let base = tmp.path().join("partition-0");

        let files = super::super::compress_partition(
            &mut reader,
            &base,
            CompressionType::Zstd,
            None,
            false,
            |_| {},
            || false,
            |_| {},
        )
        .unwrap();

        assert_eq!(files[0], "partition-0.zst");
        let compressed = std::fs::read(tmp.path().join("partition-0.zst")).unwrap();
        // Zstd compressed output should be smaller than input
        assert!(compressed.len() < 65536);

        // Decompress and verify
        let decompressed = zstd::decode_all(&compressed[..]).unwrap();
        assert_eq!(decompressed.len(), 65536);
        assert!(decompressed.iter().all(|&b| b == 0));
    }
}
