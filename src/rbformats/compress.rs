//! Format dispatch + shared compress/decompress helpers.
//!
//! Sits between `backup`/`restore` callers and the per-format modules
//! (`chd`, `vhd`, `zstd`, `raw`, `woz`, `woz_write`). The dispatch fns
//! (`compress_partition`, `decompress_to_writer`, etc.) match on a
//! compression type and route to the right format module.

use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

use crate::backup::CompressionType;

use super::{chd, raw, vhd, woz, woz_write, zstd};

pub(crate) const CHUNK_SIZE: usize = 256 * 1024; // 256 KB I/O buffer

/// Compress partition data from `reader` to `output_base` using the given compression method.
///
/// If `split_size` is `Some(bytes)`, output files are split at that boundary.
/// Returns the list of output file names (relative, e.g. `partition-0.zst`).
///
/// `progress_cb` is called with the number of bytes read so far from the source.
/// `cancel_check` returns true if the operation should abort.
pub fn compress_partition(
    reader: &mut impl Read,
    output_base: &Path,
    compression: CompressionType,
    logical_size: u64,
    split_size: Option<u64>,
    skip_zeros: bool,
    mut progress_cb: impl FnMut(u64),
    cancel_check: impl Fn() -> bool,
    mut log_cb: impl FnMut(&str),
) -> Result<Vec<String>> {
    match compression {
        CompressionType::None => raw::stream_with_split(
            reader,
            output_base,
            "raw",
            split_size,
            skip_zeros,
            &mut progress_cb,
            &cancel_check,
        ),
        CompressionType::Vhd => {
            // VHD = raw data + 512-byte footer; no splitting support
            vhd::write_vhd(
                reader,
                output_base,
                skip_zeros,
                &mut progress_cb,
                &cancel_check,
            )
        }
        CompressionType::Zstd => {
            // zstd compresses zero blocks efficiently; no need to skip
            zstd::compress_zstd(
                reader,
                output_base,
                split_size,
                &mut progress_cb,
                &cancel_check,
            )
        }
        CompressionType::Chd => chd::compress_chd(
            reader,
            output_base,
            logical_size,
            split_size,
            None,
            &mut progress_cb,
            &cancel_check,
            &mut log_cb,
        ),
        CompressionType::Dvd => chd::compress_chd_dvd(
            reader,
            output_base,
            logical_size,
            split_size,
            None,
            &mut progress_cb,
            &cancel_check,
            &mut log_cb,
        ),
    }
}

/// Decompress a partition data file and write it to the given writer.
/// If `max_bytes` is `Some(n)`, writing stops after `n` bytes.
/// Returns the number of raw bytes written.
pub fn decompress_to_writer(
    data_path: &Path,
    compression_type: &str,
    writer: &mut impl Write,
    max_bytes: Option<u64>,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
    log_cb: &mut impl FnMut(&str),
) -> Result<u64> {
    let limit = max_bytes.unwrap_or(u64::MAX);
    let mut total_written: u64 = 0;
    let mut buf = vec![0u8; CHUNK_SIZE];

    match compression_type {
        "none" | "raw" | "vhd" => {
            // Raw data — if it's a VHD file, strip the footer
            let file = File::open(data_path)
                .with_context(|| format!("failed to open {}", data_path.display()))?;
            let file_size = file.metadata()?.len();

            // Check if this is a VHD file (has footer)
            let data_size = if file_size >= 512 && compression_type == "vhd" {
                // Read last 512 bytes to check for VHD footer
                let mut f = File::open(data_path)?;
                f.seek(SeekFrom::End(-512))?;
                let mut footer_buf = [0u8; 8];
                f.read_exact(&mut footer_buf)?;
                if &footer_buf == vhd::VHD_COOKIE {
                    file_size - 512
                } else {
                    file_size
                }
            } else {
                file_size
            };

            let effective_size = data_size.min(limit);
            let mut reader = BufReader::new(File::open(data_path)?).take(effective_size);
            loop {
                if cancel_check() {
                    bail!("export cancelled");
                }
                let n = reader.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                writer.write_all(&buf[..n])?;
                total_written += n as u64;
                progress_cb(total_written);
            }
        }
        "woz" => {
            // WOZ files contain GCR-encoded bitstreams.  We decode the whole
            // image into memory (floppies are small) and then stream the
            // resulting flat sector buffer to the writer, respecting `limit`.
            let raw = fs::read(data_path)
                .with_context(|| format!("failed to read WOZ file: {}", data_path.display()))?;
            let mut reader = woz::WozReader::from_bytes(raw)
                .with_context(|| format!("failed to decode WOZ: {}", data_path.display()))?;
            let total = reader.len().min(limit);
            let mut remaining = total;
            while remaining > 0 {
                if cancel_check() {
                    bail!("export cancelled");
                }
                let to_read = (remaining as usize).min(CHUNK_SIZE);
                let n = reader.read(&mut buf[..to_read])?;
                if n == 0 {
                    break;
                }
                writer.write_all(&buf[..n])?;
                total_written += n as u64;
                remaining -= n as u64;
                progress_cb(total_written);
            }
        }
        "zstd" => {
            let file = File::open(data_path)
                .with_context(|| format!("failed to open {}", data_path.display()))?;
            let mut decoder = ::zstd::Decoder::new(BufReader::new(file))
                .context("failed to create zstd decoder")?;
            loop {
                if cancel_check() {
                    bail!("export cancelled");
                }
                let remaining = limit - total_written;
                if remaining == 0 {
                    break;
                }
                let to_read = (remaining as usize).min(CHUNK_SIZE);
                let n = decoder.read(&mut buf[..to_read])?;
                if n == 0 {
                    break;
                }
                writer
                    .write_all(&buf[..n])
                    .with_context(|| format!("write_all failed at offset {}", total_written))?;
                total_written += n as u64;
                progress_cb(total_written);
            }
        }
        "chd" | "chd-dvd" => {
            log_cb(&format!("Extracting CHD: {}", data_path.display()));
            let chd_reader = chd::ChdReader::open(data_path)
                .with_context(|| format!("failed to open CHD: {}", data_path.display()))?;
            let logical_size = chd_reader.logical_size();
            let effective_size = logical_size.min(limit);
            let mut reader = chd_reader.take(effective_size);
            loop {
                if cancel_check() {
                    bail!("export cancelled");
                }
                let n = reader.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                writer.write_all(&buf[..n])?;
                total_written += n as u64;
                progress_cb(total_written);
            }
        }
        other => {
            bail!("unsupported compression type for VHD export: {}", other);
        }
    }

    Ok(total_written)
}

/// Decompress a partition archive to a raw file.
///
/// If `compacted` is true, the decompressed data is smaller than `original_size`;
/// the output file is zero-padded to `original_size` so the filesystem is
/// at its original geometry.
pub fn decompress_partition_to_file(
    data_path: &Path,
    compression_type: &str,
    output_path: &Path,
    original_size: u64,
    compacted: bool,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
) -> Result<()> {
    let mut output = BufWriter::new(
        File::create(output_path)
            .with_context(|| format!("failed to create temp file: {}", output_path.display()))?,
    );
    let mut log_cb = |_: &str| {};

    let written = decompress_to_writer(
        data_path,
        compression_type,
        &mut output,
        Some(original_size),
        progress_cb,
        cancel_check,
        &mut log_cb,
    )?;

    // Zero-pad to original_size if compacted
    if compacted && written < original_size {
        let pad = original_size - written;
        write_zeros(&mut output, pad)?;
    }

    output.flush()?;
    Ok(())
}

/// Compress a raw file to an archive using the given compression type.
pub fn compress_file_to_archive(
    input_path: &Path,
    output_path_base: &Path,
    compression_type: &str,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
    log_cb: &mut impl FnMut(&str),
) -> Result<Vec<String>> {
    let mut reader = BufReader::new(
        File::open(input_path)
            .with_context(|| format!("failed to open {}", input_path.display()))?,
    );

    match compression_type {
        "zstd" => zstd::compress_zstd(
            &mut reader,
            output_path_base,
            None,
            progress_cb,
            cancel_check,
        ),
        "chd" => {
            let logical_size = reader.get_ref().metadata()?.len();
            chd::compress_chd(
                &mut reader,
                output_path_base,
                logical_size,
                None,
                None,
                progress_cb,
                cancel_check,
                log_cb,
            )
        }
        "chd-dvd" => {
            let logical_size = reader.get_ref().metadata()?.len();
            chd::compress_chd_dvd(
                &mut reader,
                output_path_base,
                logical_size,
                None,
                None,
                progress_cb,
                cancel_check,
                log_cb,
            )
        }
        "none" | "raw" => raw::stream_with_split(
            &mut reader,
            output_path_base,
            "raw",
            None,
            false,
            progress_cb,
            cancel_check,
        ),
        "woz" => {
            // Read the whole raw sector buffer (floppies are small), encode
            // as WOZ2, write it back to the original archive path.  The base
            // path already points to the `.woz` file (no splitting).
            let mut sectors = Vec::new();
            reader
                .read_to_end(&mut sectors)
                .context("failed to read raw sectors for WOZ re-encode")?;
            if cancel_check() {
                bail!("export cancelled");
            }
            progress_cb(sectors.len() as u64);
            let woz_bytes =
                woz_write::sectors_to_woz(&sectors).context("failed to encode WOZ image")?;
            let out_path = output_path_base.with_extension("woz");
            fs::write(&out_path, &woz_bytes)
                .with_context(|| format!("failed to write {}", out_path.display()))?;
            Ok(vec![file_name(&out_path)])
        }
        other => bail!("unsupported compression type for recompression: {}", other),
    }
}

/// Write `count` zero bytes to a writer, in chunks.
pub(crate) fn write_zeros(writer: &mut impl Write, count: u64) -> Result<()> {
    let zeros = vec![0u8; CHUNK_SIZE];
    let mut remaining = count;
    while remaining > 0 {
        let n = (remaining as usize).min(CHUNK_SIZE);
        writer
            .write_all(&zeros[..n])
            .context("failed to write zeros")?;
        remaining -= n as u64;
    }
    Ok(())
}

/// Write `count` zero bytes to a writer, calling `progress_cb` after each chunk.
/// `base_offset` is the total bytes already written before this call; the callback
/// receives `base_offset + bytes_written_so_far` so the caller's progress counter
/// stays in sync with the zero-fill.
pub(crate) fn write_zeros_with_progress(
    writer: &mut impl Write,
    count: u64,
    base_offset: u64,
    progress_cb: &mut impl FnMut(u64),
) -> Result<()> {
    let zeros = vec![0u8; CHUNK_SIZE];
    let mut remaining = count;
    let mut done: u64 = 0;
    while remaining > 0 {
        let n = (remaining as usize).min(CHUNK_SIZE);
        writer
            .write_all(&zeros[..n])
            .context("failed to write zeros")?;
        remaining -= n as u64;
        done += n as u64;
        progress_cb(base_offset + done);
    }
    Ok(())
}

pub(crate) fn is_all_zeros(data: &[u8]) -> bool {
    data.iter().all(|&b| b == 0)
}

/// Build the output file path with optional split numbering.
/// `partition-0.zst` (no split) or `partition-0.001.zst` (split).
pub(crate) fn output_path(
    base: &Path,
    extension: &str,
    splitting: bool,
    part_index: u32,
) -> PathBuf {
    let stem = base.file_stem().unwrap_or_default().to_string_lossy();
    let parent = base.parent().unwrap_or(Path::new("."));
    if splitting && part_index > 0 {
        parent.join(format!("{stem}.{:03}.{extension}", part_index))
    } else {
        parent.join(format!("{stem}.{extension}"))
    }
}

/// Extract just the file name as a String.
pub(crate) fn file_name(path: &Path) -> String {
    path.file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned()
}

/// A writer wrapper used only during zstd compression that does NOT do splitting.
/// Zstd doesn't support splitting mid-stream well (the compressed frame must be
/// contiguous), so we write the entire compressed output to one file.
/// Splitting of zstd output happens post-hoc if needed.
pub(crate) struct SplitWriter {
    inner: BufWriter<File>,
}

impl SplitWriter {
    pub(crate) fn new(
        path: &Path,
        _split_bytes: u64,
        _files: &mut Vec<String>,
        _part_index: &mut u32,
        _output_base: &Path,
    ) -> Result<Self> {
        let file =
            File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
        Ok(Self {
            inner: BufWriter::new(file),
        })
    }
}

impl Write for SplitWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::path::PathBuf;
    use tempfile::TempDir;

    #[test]
    fn test_output_path_no_split() {
        let base = Path::new("/tmp/backup/partition-0");
        let path = output_path(base, "zst", false, 0);
        assert_eq!(path, PathBuf::from("/tmp/backup/partition-0.zst"));
    }

    #[test]
    fn test_output_path_split() {
        let base = Path::new("/tmp/backup/partition-0");
        assert_eq!(
            output_path(base, "zst", true, 0),
            PathBuf::from("/tmp/backup/partition-0.zst")
        );
        assert_eq!(
            output_path(base, "zst", true, 1),
            PathBuf::from("/tmp/backup/partition-0.001.zst")
        );
        assert_eq!(
            output_path(base, "zst", true, 12),
            PathBuf::from("/tmp/backup/partition-0.012.zst")
        );
    }

    #[test]
    fn test_compress_none_no_split() {
        let tmp = TempDir::new().unwrap();
        let data = vec![0xABu8; 4096];
        let mut reader = Cursor::new(&data);
        let base = tmp.path().join("partition-0");

        let files = compress_partition(
            &mut reader,
            &base,
            CompressionType::None,
            data.len() as u64,
            None,
            false,
            |_| {},
            || false,
            |_| {},
        )
        .unwrap();

        assert_eq!(files, vec!["partition-0.raw"]);
        let written = fs::read(tmp.path().join("partition-0.raw")).unwrap();
        assert_eq!(written.len(), 4096);
        assert!(written.iter().all(|&b| b == 0xAB));
    }

    #[test]
    fn test_compress_none_with_split() {
        let tmp = TempDir::new().unwrap();
        let data = vec![0xCDu8; 3000];
        let mut reader = Cursor::new(&data);
        let base = tmp.path().join("partition-0");

        let files = compress_partition(
            &mut reader,
            &base,
            CompressionType::None,
            data.len() as u64,
            Some(1024),
            false,
            |_| {},
            || false,
            |_| {},
        )
        .unwrap();

        assert_eq!(files.len(), 3);
        assert_eq!(files[0], "partition-0.raw");
        assert_eq!(files[1], "partition-0.001.raw");
        assert_eq!(files[2], "partition-0.002.raw");

        let f0 = fs::read(tmp.path().join("partition-0.raw")).unwrap();
        assert_eq!(f0.len(), 1024);
        let f1 = fs::read(tmp.path().join("partition-0.001.raw")).unwrap();
        assert_eq!(f1.len(), 1024);
        let f2 = fs::read(tmp.path().join("partition-0.002.raw")).unwrap();
        assert_eq!(f2.len(), 952);
    }

    #[test]
    fn test_skip_zeros_raw() {
        let tmp = TempDir::new().unwrap();
        let mut data = vec![0u8; CHUNK_SIZE];
        data.extend(vec![0xAAu8; CHUNK_SIZE]);
        let mut reader = Cursor::new(&data);
        let base = tmp.path().join("partition-0");

        let files = compress_partition(
            &mut reader,
            &base,
            CompressionType::None,
            data.len() as u64,
            None,
            true,
            |_| {},
            || false,
            |_| {},
        )
        .unwrap();

        assert_eq!(files, vec!["partition-0.raw"]);
        let written = fs::read(tmp.path().join("partition-0.raw")).unwrap();
        assert_eq!(written.len(), CHUNK_SIZE * 2);
        assert!(written[..CHUNK_SIZE].iter().all(|&b| b == 0));
        assert!(written[CHUNK_SIZE..].iter().all(|&b| b == 0xAA));
    }

    #[test]
    fn test_skip_zeros_all_zeros() {
        let tmp = TempDir::new().unwrap();
        let data = vec![0u8; CHUNK_SIZE * 4];
        let mut reader = Cursor::new(&data);
        let base = tmp.path().join("partition-0");

        let files = compress_partition(
            &mut reader,
            &base,
            CompressionType::None,
            data.len() as u64,
            None,
            true,
            |_| {},
            || false,
            |_| {},
        )
        .unwrap();

        assert_eq!(files, vec!["partition-0.raw"]);
        let written = fs::read(tmp.path().join("partition-0.raw")).unwrap();
        assert_eq!(written.len(), CHUNK_SIZE * 4);
        assert!(written.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_woz_decompress_and_recompress_round_trip() {
        let tmp = TempDir::new().unwrap();
        let woz_path = tmp.path().join("disk.woz");

        let mut input = vec![0u8; woz_write::RAW_SIZE_525];
        for (i, b) in input.iter_mut().enumerate() {
            *b = ((i * 13) ^ (i >> 5)) as u8;
        }
        woz_write::write_woz(&woz_path, &input).unwrap();

        let raw_path = tmp.path().join("sectors.bin");
        decompress_partition_to_file(
            &woz_path,
            "woz",
            &raw_path,
            woz_write::RAW_SIZE_525 as u64,
            false,
            &mut |_| {},
            &|| false,
        )
        .unwrap();
        let round_in = fs::read(&raw_path).unwrap();
        assert_eq!(round_in, input, "WOZ → raw round-trip failed");

        let out_base = tmp.path().join("disk");
        let files = compress_file_to_archive(
            &raw_path,
            &out_base,
            "woz",
            &mut |_| {},
            &|| false,
            &mut |_| {},
        )
        .unwrap();
        assert_eq!(files, vec!["disk.woz"]);

        let reader = woz::WozReader::open(&out_base.with_extension("woz")).unwrap();
        let mut decoded = Vec::new();
        let mut reader = reader;
        Read::read_to_end(&mut reader, &mut decoded).unwrap();
        assert_eq!(decoded, input, "WOZ re-encode round-trip failed");
    }

    #[test]
    fn test_cancel_aborts() {
        let tmp = TempDir::new().unwrap();
        let data = vec![0u8; 65536];
        let mut reader = Cursor::new(&data);
        let base = tmp.path().join("partition-0");

        let result = compress_partition(
            &mut reader,
            &base,
            CompressionType::None,
            data.len() as u64,
            None,
            false,
            |_| {},
            || true,
            |_| {},
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cancelled"));
    }

    #[test]
    fn test_chd_decompress_round_trip() {
        // Compress raw bytes to a CHD, then stream them back through
        // decompress_to_writer (the path Stage 4 swapped from chdman extractraw
        // to libchdman-rs). Result must equal the input byte-for-byte.
        let tmp = TempDir::new().unwrap();
        let mut input = vec![0u8; 1024 * 1024];
        for (i, b) in input.iter_mut().enumerate() {
            *b = ((i * 31) ^ (i >> 7)) as u8;
        }
        let mut reader = Cursor::new(&input);
        let base = tmp.path().join("partition-0");

        compress_partition(
            &mut reader,
            &base,
            CompressionType::Chd,
            input.len() as u64,
            None,
            false,
            |_| {},
            || false,
            |_| {},
        )
        .unwrap();

        let chd_path = base.with_extension("chd");
        let mut output: Vec<u8> = Vec::new();
        let written = decompress_to_writer(
            &chd_path,
            "chd",
            &mut output,
            Some(input.len() as u64),
            &mut |_| {},
            &|| false,
            &mut |_| {},
        )
        .unwrap();

        assert_eq!(written as usize, input.len());
        assert_eq!(output, input, "CHD decompress round-trip mismatch");
    }
}
