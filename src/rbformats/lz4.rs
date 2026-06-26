//! LZ4 (frame format) codec for the native PerPartition backup format.
//!
//! The optional faster-on-slow-CPUs codec shared with **crusty-backup**
//! (`cb-dos` `/CODEC:LZ4`): on a vintage 486 the DEFLATE/gzip path is
//! CPU-bound, so LZ4 trades ratio for a much faster backup. gzip stays the
//! default; this is opt-in via `--format lz4`. Mirrors [`super::gzip`] — a
//! single contiguous frame streamed through the encoder, with an optional
//! hasher tee for the `.lz4.crc32` / `.lz4.sha256` sidecar.
//!
//! The frame is the **standard LZ4 frame format** (magic `0x184D2204`), so a
//! `partition-N.lz4` written here decodes with the DOS tool's liblz4 and vice
//! versa. Decompression lives in [`super::compress::decompress_to_writer`]
//! (the `"lz4"` arm) via [`lz4_flex::frame::FrameDecoder`], which transparently
//! reads a file of several concatenated frames (the future network `.cbk` chunk
//! shape) as well as a single cb-dos-streamed frame.

use std::io::{Read, Write};
use std::path::Path;

use anyhow::{bail, Context, Result};
use lz4_flex::frame::FrameEncoder;

use super::compress::OutputHasherHandle;
use super::{file_name, output_path, SplitWriter, CHUNK_SIZE};
use crate::backup::verify::ChecksumWriter;

/// Compress with LZ4 (frame format), streaming through the encoder. Like
/// [`super::gzip::compress_gzip`], the frame is contiguous so we emit one file
/// (no mid-stream split); `output_hasher`, when `Some`, tees every compressed
/// byte to the shared hasher for the checksum sidecar.
pub(crate) fn compress_lz4(
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

    let first_path = output_path(output_base, "lz4", split_size.is_some(), part_index);
    let split_writer = SplitWriter::new(
        &first_path,
        split_bytes,
        &mut files,
        &mut part_index,
        output_base,
    )?;
    // Optionally tee compressed bytes through a hasher before they hit disk;
    // `Box<dyn Write>` erases the raw-vs-checksummed writer shape so the LZ4
    // encoder needn't be generic over the inner type.
    let sink: Box<dyn Write> = match output_hasher {
        Some(h) => Box::new(ChecksumWriter::new(split_writer, h)),
        None => Box::new(split_writer),
    };
    let mut encoder = FrameEncoder::new(sink);
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

    encoder.finish().context("failed to finalize lz4 frame")?;
    Ok(files)
}

#[cfg(test)]
mod tests {
    use crate::backup::CompressionType;
    use std::io::Cursor;
    use tempfile::TempDir;

    #[test]
    fn test_compress_lz4() {
        let tmp = TempDir::new().unwrap();
        // Highly compressible data
        let data = vec![0u8; 65536];
        let mut reader = Cursor::new(&data);
        let base = tmp.path().join("partition-0");

        let files = super::super::compress_partition(
            &mut reader,
            &base,
            CompressionType::Lz4,
            data.len() as u64,
            None,
            false,
            None,
            |_| {},
            || false,
            |_| {},
        )
        .unwrap();

        assert_eq!(files[0], "partition-0.lz4");
        let compressed = std::fs::read(tmp.path().join("partition-0.lz4")).unwrap();
        // LZ4 compressed output should be smaller than input
        assert!(compressed.len() < 65536);

        // Decompress through the frame decoder and verify
        let mut decoder = lz4_flex::frame::FrameDecoder::new(&compressed[..]);
        let mut decompressed = Vec::new();
        std::io::Read::read_to_end(&mut decoder, &mut decompressed).unwrap();
        assert_eq!(decompressed.len(), 65536);
        assert!(decompressed.iter().all(|&b| b == 0));
    }
}
