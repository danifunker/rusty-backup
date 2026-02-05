pub mod chd;
pub mod raw;
pub mod vhd;
pub mod zstd;

use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};

use crate::backup::metadata::BackupMetadata;
use crate::backup::CompressionType;
use crate::fs::fat::patch_bpb_hidden_sectors;
use crate::partition::mbr::patch_mbr_entries;
use crate::partition::PartitionSizeOverride;

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
        CompressionType::Chd => {
            // CHD needs complete raw temp file; chdman handles zero compression
            chd::compress_chd(
                reader,
                output_base,
                split_size,
                &mut progress_cb,
                &cancel_check,
                &mut log_cb,
            )
        }
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
                writer.write_all(&buf[..n])?;
                total_written += n as u64;
                progress_cb(total_written);
            }
        }
        "chd" => {
            // Use chdman to extract raw data to a temp file, then stream it
            let parent = data_path.parent().unwrap_or(Path::new("."));
            let temp_path = parent.join(format!(
                ".vhd-export-{}.tmp",
                data_path.file_stem().unwrap_or_default().to_string_lossy()
            ));

            log_cb(&format!("Extracting CHD: {}", data_path.display()));
            let chdman_cmd = crate::update::UpdateConfig::load()
                .chdman_path
                .unwrap_or_else(|| "chdman".to_string());
            let output = Command::new(&chdman_cmd)
                .arg("extractraw")
                .arg("-i")
                .arg(data_path)
                .arg("-o")
                .arg(&temp_path)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .output()
                .context("failed to run chdman extractraw")?;

            if !output.status.success() {
                let _ = fs::remove_file(&temp_path);
                bail!(
                    "chdman extractraw failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }

            let temp_size = fs::metadata(&temp_path)
                .map(|m| m.len())
                .unwrap_or(u64::MAX);
            let effective_size = temp_size.min(limit);
            let mut reader =
                BufReader::new(File::open(&temp_path).with_context(|| {
                    format!("failed to open temp file: {}", temp_path.display())
                })?)
                .take(effective_size);
            loop {
                if cancel_check() {
                    let _ = fs::remove_file(&temp_path);
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
            let _ = fs::remove_file(&temp_path);
        }
        other => {
            bail!("unsupported compression type for VHD export: {}", other);
        }
    }

    Ok(total_written)
}

/// Reconstruct a disk image from a backup folder, writing to any seekable writer.
///
/// Shared by: VHD export (file writer), restore (device or file writer).
///
/// Writes the MBR (patched with partition overrides), then each partition's
/// compressed data at its correct offset, with FAT resize and BPB fixups.
/// Gaps and unused space handling depends on `is_device` and `write_zeros`.
///
/// Returns the total number of bytes written.
pub fn reconstruct_disk_from_backup(
    backup_folder: &Path,
    metadata: &BackupMetadata,
    mbr_bytes: Option<&[u8; 512]>,
    partition_sizes: &[PartitionSizeOverride],
    _target_size: u64,
    writer: &mut (impl Read + Write + Seek),
    is_device: bool,
    fill_unused_with_zeros: bool,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
    log_cb: &mut impl FnMut(&str),
) -> Result<u64> {
    let mut total_written: u64 = 0;

    // Helper to look up export size for a partition index
    let get_export_size = |index: usize, default: u64| -> u64 {
        partition_sizes
            .iter()
            .find(|ps| ps.index == index)
            .map(|ps| ps.export_size)
            .unwrap_or(default)
    };

    // Helper to look up effective start LBA for a partition
    let get_effective_start_lba = |index: usize, default: u64| -> u64 {
        partition_sizes
            .iter()
            .find(|ps| ps.index == index)
            .map(|ps| ps.effective_start_lba())
            .unwrap_or(default)
    };

    // Write MBR (first 512 bytes), patching partition sizes if needed
    let mut mbr_buf = if let Some(mbr) = mbr_bytes {
        *mbr
    } else {
        let mbr_path = backup_folder.join("mbr.bin");
        if mbr_path.exists() {
            let data = fs::read(&mbr_path).context("failed to read mbr.bin")?;
            let mut buf = [0u8; 512];
            let copy_len = data.len().min(512);
            buf[..copy_len].copy_from_slice(&data[..copy_len]);
            buf
        } else {
            bail!("no MBR data available for disk reconstruction");
        }
    };
    if !partition_sizes.is_empty() {
        patch_mbr_entries(&mut mbr_buf, partition_sizes);
        log_cb("Patched MBR partition table with export sizes");
    }
    writer.write_all(&mbr_buf).context("failed to write MBR")?;
    total_written += 512;

    // Write each partition at its correct offset, filling gaps with zeros
    for pm in &metadata.partitions {
        if cancel_check() {
            bail!("operation cancelled");
        }

        let effective_lba = get_effective_start_lba(pm.index, pm.start_lba);
        let part_offset = effective_lba * 512;
        let export_size = get_export_size(pm.index, pm.original_size_bytes);

        // Fill or seek over gap between current position and partition start
        if total_written < part_offset {
            let gap = part_offset - total_written;
            if is_device && fill_unused_with_zeros {
                // Write zeros to gap (needed for some devices/filesystems)
                write_zeros(writer, gap)?;
                total_written += gap;
            } else if is_device {
                // Device without zero-fill: seek forward
                // This may fail on some raw devices, but worth trying
                match writer.seek(SeekFrom::Current(gap as i64)) {
                    Ok(_) => total_written += gap,
                    Err(e) if e.kind() == io::ErrorKind::InvalidInput => {
                        // Seek not supported, write zeros as fallback
                        log_cb("Warning: device doesn't support seek, writing zeros to gap");
                        write_zeros(writer, gap)?;
                        total_written += gap;
                    }
                    Err(e) => return Err(e.into()),
                }
            } else {
                // File: use sparse seeks
                writer.seek(SeekFrom::Current(gap as i64))?;
                total_written += gap;
            }
        }

        // Write partition data
        if pm.compressed_files.is_empty() {
            log_cb(&format!("partition-{}: no data files, skipping", pm.index));
            continue;
        }

        let data_file = &pm.compressed_files[0];
        let data_path = backup_folder.join(data_file);

        if !data_path.exists() {
            log_cb(&format!(
                "partition-{}: data file not found: {}, filling with zeros",
                pm.index,
                data_path.display()
            ));
            write_zeros(writer, export_size)?;
            total_written += export_size;
            continue;
        }

        let bytes_written = decompress_to_writer(
            &data_path,
            &metadata.compression_type,
            writer,
            Some(export_size),
            progress_cb,
            cancel_check,
            log_cb,
        )?;
        total_written += bytes_written;

        // Handle unused space at end of partition
        if bytes_written < export_size {
            let pad = export_size - bytes_written;
            if fill_unused_with_zeros {
                // User requested zero-fill (may be needed for some filesystems)
                write_zeros(writer, pad)?;
                total_written += pad;
            } else if is_device {
                // Device without zero-fill: try seek, fallback to zeros if needed
                match writer.seek(SeekFrom::Current(pad as i64)) {
                    Ok(_) => total_written += pad,
                    Err(e) if e.kind() == io::ErrorKind::InvalidInput => {
                        log_cb(
                            "Warning: device doesn't support seek in data region, writing zeros",
                        );
                        write_zeros(writer, pad)?;
                        total_written += pad;
                    }
                    Err(e) => return Err(e.into()),
                }
            } else {
                // File: sparse seek - filesystem resize will handle the space
                writer.seek(SeekFrom::Current(pad as i64))?;
                total_written += pad;
            }
        }

        // Update BPB hidden sectors to match partition start LBA
        {
            writer.flush()?;
            patch_bpb_hidden_sectors(writer, part_offset, effective_lba, log_cb)?;
        }

        log_cb(&format!(
            "partition-{}: wrote {} bytes (export size: {})",
            pm.index, bytes_written, export_size,
        ));
    }

    writer.flush()?;
    Ok(total_written)
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

/// Check if a byte slice is entirely zeros.
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
        // 256KB of zeros followed by 256KB of data
        let mut data = vec![0u8; CHUNK_SIZE];
        data.extend(vec![0xAAu8; CHUNK_SIZE]);
        let mut reader = Cursor::new(&data);
        let base = tmp.path().join("partition-0");

        let files = compress_partition(
            &mut reader,
            &base,
            CompressionType::None,
            None,
            true, // skip zeros
            |_| {},
            || false,
            |_| {},
        )
        .unwrap();

        assert_eq!(files, vec!["partition-0.raw"]);
        let written = fs::read(tmp.path().join("partition-0.raw")).unwrap();
        // File should still be the full size (sparse on disk, full logically)
        assert_eq!(written.len(), CHUNK_SIZE * 2);
        // First chunk should be zeros, second should be 0xAA
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
            None,
            true, // skip zeros
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
    fn test_cancel_aborts() {
        let tmp = TempDir::new().unwrap();
        let data = vec![0u8; 65536];
        let mut reader = Cursor::new(&data);
        let base = tmp.path().join("partition-0");

        let result = compress_partition(
            &mut reader,
            &base,
            CompressionType::None,
            None,
            false,
            |_| {},
            || true, // always cancel
            |_| {},
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cancelled"));
    }
}
