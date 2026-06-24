//! Gzip (DEFLATE) codec for the native PerPartition backup format.
//!
//! Added so the desktop can produce and consume the `partition-N.gz`
//! members that **crusty-backup** (`cb-dos`) writes on a real DOS box —
//! zstd is impractical under DJGPP, raw loses compression, so gzip is the
//! shared codec (see `docs/cb_dos.md` §3). Mirrors [`super::zstd`]: a
//! single contiguous member streamed through the encoder, with an optional
//! hasher tee for the `.gz.crc32` / `.gz.sha256` sidecar.
//!
//! Decompression lives in [`super::compress::decompress_to_writer`] (the
//! `"gzip"` arm) and uses `MultiGzDecoder`, so a file built from several
//! concatenated gzip members (the network `.cbk` chunk shape) also decodes
//! cleanly — a single-member `.gz` is just the n=1 case.

use std::io::{Read, Write};
use std::path::Path;

use anyhow::{bail, Context, Result};
use flate2::write::GzEncoder;
use flate2::Compression;

use super::compress::OutputHasherHandle;
use super::{file_name, output_path, SplitWriter, CHUNK_SIZE};
use crate::backup::verify::ChecksumWriter;

/// Compress with gzip, streaming through the encoder. Like
/// [`super::zstd::compress_zstd`], the gzip frame is contiguous so we
/// emit one file (no mid-stream split); `output_hasher`, when `Some`,
/// tees every compressed byte to the shared hasher for the checksum
/// sidecar.
pub(crate) fn compress_gzip(
    reader: &mut impl Read,
    output_base: &Path,
    split_size: Option<u64>,
    output_hasher: Option<OutputHasherHandle>,
    progress_cb: &mut dyn FnMut(u64),
    cancel_check: &dyn Fn() -> bool,
) -> Result<Vec<String>> {
    let mut files = Vec::new();
    let mut total_read: u64 = 0;
    let mut part_index: u32 = 0;
    let split_bytes = split_size.unwrap_or(u64::MAX);

    let first_path = output_path(output_base, "gz", split_size.is_some(), part_index);
    let split_writer = SplitWriter::new(
        &first_path,
        split_bytes,
        &mut files,
        &mut part_index,
        output_base,
    )?;
    // Optionally tee compressed bytes through a hasher before they hit
    // disk; `Box<dyn Write>` erases the raw-vs-checksummed writer shape so
    // the gzip encoder needn't be generic over the inner type.
    let sink: Box<dyn Write> = match output_hasher {
        Some(h) => Box::new(ChecksumWriter::new(split_writer, h)),
        None => Box::new(split_writer),
    };
    let mut encoder = GzEncoder::new(sink, Compression::default());
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

    encoder.finish().context("failed to finalize gzip stream")?;
    Ok(files)
}

#[cfg(test)]
mod tests {
    use crate::backup::CompressionType;
    use std::io::Cursor;
    use tempfile::TempDir;

    #[test]
    fn test_compress_gzip() {
        let tmp = TempDir::new().unwrap();
        // Highly compressible data
        let data = vec![0u8; 65536];
        let mut reader = Cursor::new(&data);
        let base = tmp.path().join("partition-0");

        let files = super::super::compress_partition(
            &mut reader,
            &base,
            CompressionType::Gzip,
            data.len() as u64,
            None,
            false,
            None,
            |_| {},
            || false,
            |_| {},
        )
        .unwrap();

        assert_eq!(files[0], "partition-0.gz");
        let compressed = std::fs::read(tmp.path().join("partition-0.gz")).unwrap();
        // Gzip compressed output should be smaller than input
        assert!(compressed.len() < 65536);

        // Decompress through MultiGzDecoder and verify
        let mut decoder = flate2::read::MultiGzDecoder::new(&compressed[..]);
        let mut decompressed = Vec::new();
        std::io::Read::read_to_end(&mut decoder, &mut decompressed).unwrap();
        assert_eq!(decompressed.len(), 65536);
        assert!(decompressed.iter().all(|&b| b == 0));
    }
}
