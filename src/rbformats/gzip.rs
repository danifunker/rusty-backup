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
use super::gz_index::{gz_index_path, write_gz_index, GzSpan, GZ_SPAN_BYTES};
use super::{file_name, output_path, SplitWriter, CHUNK_SIZE};
use crate::backup::verify::ChecksumWriter;

/// A `Write` wrapper that counts the bytes written through it, so we can record
/// each gzip member's start offset within the `.gz` file.
struct CountingWriter<W> {
    inner: W,
    count: u64,
}
impl<W: Write> CountingWriter<W> {
    fn new(inner: W) -> Self {
        Self { inner, count: 0 }
    }
}
impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(b)?;
        self.count += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

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
    files.push(file_name(&first_path));

    // Emit the partition as a sequence of independent gzip members, a fresh
    // member every `GZ_SPAN_BYTES` of *uncompressed* input, so the `.cbk` packer
    // and the lazy reader can seek to the span covering an offset (see
    // `super::gz_index`). A partition smaller than one span stays single-member
    // — byte-identical to the historical output, and no `.idx` is written. We
    // disable this for the split-output mode (`--split-size`), which already
    // chunks into separate files. The byte count tees through `CountingWriter`
    // so each member start's `.gz`-file offset is recorded.
    let multimember = split_size.is_none();
    let mut spans: Vec<GzSpan> = vec![GzSpan {
        uncompressed_offset: 0,
        compressed_offset: 0,
    }];
    let mut member_start_uncompressed: u64 = 0;
    let mut encoder = GzEncoder::new(CountingWriter::new(sink), Compression::default());

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

        if multimember && total_read - member_start_uncompressed >= GZ_SPAN_BYTES {
            // Close this member and open the next; its `.gz` offset is the byte
            // count once the trailer of the closed member has been written.
            let w = encoder.finish().context("failed to finalize gzip member")?;
            spans.push(GzSpan {
                uncompressed_offset: total_read,
                compressed_offset: w.count,
            });
            member_start_uncompressed = total_read;
            encoder = GzEncoder::new(w, Compression::default());
        }
    }

    let mut w = encoder.finish().context("failed to finalize gzip stream")?;
    w.flush().context("failed to flush gzip output")?;

    // Write the seek layout for a multi-member partition, or remove any stale one
    // (e.g. when re-compressing — during an edit — a partition that is now small
    // enough to be single-member, so a leftover `.idx` can't mis-describe it).
    let idx_path = gz_index_path(&first_path);
    if multimember && spans.len() > 1 {
        write_gz_index(&idx_path, &spans).context("failed to write gzip seek index")?;
    } else {
        let _ = std::fs::remove_file(&idx_path);
    }
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
