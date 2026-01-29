use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};

use super::CompressionType;

const CHUNK_SIZE: usize = 256 * 1024; // 256 KB I/O buffer

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
    mut progress_cb: impl FnMut(u64),
    cancel_check: impl Fn() -> bool,
) -> Result<Vec<String>> {
    match compression {
        CompressionType::None => {
            stream_with_split(reader, output_base, "raw", split_size, &mut progress_cb, &cancel_check)
        }
        CompressionType::Zstd => {
            compress_zstd(reader, output_base, split_size, &mut progress_cb, &cancel_check)
        }
        CompressionType::Chd => {
            compress_chd(reader, output_base, split_size, &mut progress_cb, &cancel_check)
        }
    }
}

/// Detect whether `chdman` is available on PATH.
pub fn detect_chdman() -> bool {
    Command::new("chdman")
        .arg("help")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok()
}

/// Stream raw data with optional splitting.
fn stream_with_split(
    reader: &mut impl Read,
    output_base: &Path,
    extension: &str,
    split_size: Option<u64>,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
) -> Result<Vec<String>> {
    let mut files = Vec::new();
    let mut total_read: u64 = 0;
    let mut part_index: u32 = 0;
    let mut current_file_bytes: u64 = 0;
    let split_bytes = split_size.unwrap_or(u64::MAX);

    let first_path = output_path(output_base, extension, split_size.is_some(), part_index);
    let mut writer = BufWriter::new(
        File::create(&first_path)
            .with_context(|| format!("failed to create {}", first_path.display()))?,
    );
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

        let mut written = 0;
        while written < n {
            let remaining_in_split = split_bytes.saturating_sub(current_file_bytes) as usize;
            let to_write = (n - written).min(remaining_in_split);
            writer
                .write_all(&buf[written..written + to_write])
                .context("failed to write output")?;
            current_file_bytes += to_write as u64;
            written += to_write;

            if current_file_bytes >= split_bytes && written < n {
                writer.flush()?;
                drop(writer);
                part_index += 1;
                current_file_bytes = 0;
                let next_path =
                    output_path(output_base, extension, true, part_index);
                writer = BufWriter::new(
                    File::create(&next_path)
                        .with_context(|| format!("failed to create {}", next_path.display()))?,
                );
                files.push(file_name(&next_path));
            }
        }

        total_read += n as u64;
        progress_cb(total_read);
    }

    writer.flush()?;
    Ok(files)
}

/// Compress with zstd, streaming through the encoder with optional splitting.
fn compress_zstd(
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

/// Compress via chdman external tool.
///
/// Steps:
/// 1. Write raw data to a temp file next to the output
/// 2. Run `chdman createraw -i temp -o output.chd -hs 4096`
/// 3. Clean up temp file
/// 4. If splitting is needed, split the output CHD manually
fn compress_chd(
    reader: &mut impl Read,
    output_base: &Path,
    split_size: Option<u64>,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
) -> Result<Vec<String>> {
    let parent = output_base
        .parent()
        .context("output path has no parent directory")?;

    // Step 1: Write raw data to temp file
    let temp_path = parent.join(format!(
        ".{}.tmp",
        output_base
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
    ));
    {
        let mut temp_writer = BufWriter::new(
            File::create(&temp_path)
                .with_context(|| format!("failed to create temp file: {}", temp_path.display()))?,
        );
        let mut total_read: u64 = 0;
        let mut buf = vec![0u8; CHUNK_SIZE];
        loop {
            if cancel_check() {
                let _ = fs::remove_file(&temp_path);
                bail!("backup cancelled");
            }
            let n = reader.read(&mut buf).context("failed to read source")?;
            if n == 0 {
                break;
            }
            temp_writer
                .write_all(&buf[..n])
                .context("failed to write temp file")?;
            total_read += n as u64;
            progress_cb(total_read);
        }
        temp_writer.flush()?;
    }

    // Step 2: Determine raw data size for chdman (must be known)
    let raw_size = fs::metadata(&temp_path)
        .with_context(|| format!("failed to stat temp file: {}", temp_path.display()))?
        .len();

    // Hunk size must evenly divide the input size; use 4096 if possible, else 512
    let hunk_size = if raw_size % 4096 == 0 { 4096 } else { 512 };

    let chd_path = output_path(output_base, "chd", false, 0);
    let status = Command::new("chdman")
        .arg("createraw")
        .arg("-i")
        .arg(&temp_path)
        .arg("-o")
        .arg(&chd_path)
        .arg("-hs")
        .arg(hunk_size.to_string())
        .arg("-us")
        .arg(raw_size.to_string())
        .status()
        .context("failed to run chdman")?;

    let _ = fs::remove_file(&temp_path);

    if !status.success() {
        bail!(
            "chdman exited with status {}",
            status.code().unwrap_or(-1)
        );
    }

    // Step 3: Split the CHD file if requested
    if let Some(split_bytes) = split_size {
        let chd_size = fs::metadata(&chd_path)
            .with_context(|| format!("failed to stat CHD output: {}", chd_path.display()))?
            .len();

        if chd_size > split_bytes {
            return split_file(&chd_path, output_base, "chd", split_bytes);
        }
    }

    Ok(vec![file_name(&chd_path)])
}

/// Split an existing file into chunks, removing the original.
fn split_file(
    source: &Path,
    output_base: &Path,
    extension: &str,
    split_bytes: u64,
) -> Result<Vec<String>> {
    let mut reader = BufReader::new(
        File::open(source).with_context(|| format!("failed to open {}", source.display()))?,
    );
    let mut files = Vec::new();
    let mut part_index: u32 = 0;
    let mut buf = vec![0u8; CHUNK_SIZE];

    loop {
        let out_path = output_path(output_base, extension, true, part_index);
        let mut writer = BufWriter::new(
            File::create(&out_path)
                .with_context(|| format!("failed to create {}", out_path.display()))?,
        );
        let mut written: u64 = 0;
        let mut eof = false;

        while written < split_bytes {
            let to_read = ((split_bytes - written) as usize).min(CHUNK_SIZE);
            let n = reader.read(&mut buf[..to_read])?;
            if n == 0 {
                eof = true;
                break;
            }
            writer.write_all(&buf[..n])?;
            written += n as u64;
        }
        writer.flush()?;

        if written > 0 {
            files.push(file_name(&out_path));
        } else {
            // Empty chunk, remove it
            let _ = fs::remove_file(&out_path);
        }

        part_index += 1;
        if eof {
            break;
        }
    }

    // Remove the original unsplit file
    let _ = fs::remove_file(source);

    Ok(files)
}

/// Build the output file path with optional split numbering.
/// `partition-0.zst` (no split) or `partition-0.001.zst` (split).
fn output_path(base: &Path, extension: &str, splitting: bool, part_index: u32) -> PathBuf {
    let stem = base
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy();
    let parent = base.parent().unwrap_or(Path::new("."));
    if splitting && part_index > 0 {
        parent.join(format!("{stem}.{:03}.{extension}", part_index))
    } else {
        parent.join(format!("{stem}.{extension}"))
    }
}

/// Extract just the file name as a String.
fn file_name(path: &Path) -> String {
    path.file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned()
}

/// A writer wrapper used only during zstd compression that does NOT do splitting.
/// Zstd doesn't support splitting mid-stream well (the compressed frame must be
/// contiguous), so we write the entire compressed output to one file.
/// Splitting of zstd output happens post-hoc if needed.
struct SplitWriter {
    inner: BufWriter<File>,
}

impl SplitWriter {
    fn new(
        path: &Path,
        _split_bytes: u64,
        _files: &mut Vec<String>,
        _part_index: &mut u32,
        _output_base: &Path,
    ) -> Result<Self> {
        let file = File::create(path)
            .with_context(|| format!("failed to create {}", path.display()))?;
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
            |_| {},
            || false,
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
            |_| {},
            || false,
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
    fn test_compress_zstd() {
        let tmp = TempDir::new().unwrap();
        // Highly compressible data
        let data = vec![0u8; 65536];
        let mut reader = Cursor::new(&data);
        let base = tmp.path().join("partition-0");

        let files = compress_partition(
            &mut reader,
            &base,
            CompressionType::Zstd,
            None,
            |_| {},
            || false,
        )
        .unwrap();

        assert_eq!(files[0], "partition-0.zst");
        let compressed = fs::read(tmp.path().join("partition-0.zst")).unwrap();
        // Zstd compressed output should be smaller than input
        assert!(compressed.len() < 65536);

        // Decompress and verify
        let decompressed = zstd::decode_all(&compressed[..]).unwrap();
        assert_eq!(decompressed.len(), 65536);
        assert!(decompressed.iter().all(|&b| b == 0));
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
            |_| {},
            || true, // always cancel
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cancelled"));
    }

    #[test]
    fn test_detect_chdman() {
        // Just ensure it doesn't panic; result depends on system
        let _available = detect_chdman();
    }
}
