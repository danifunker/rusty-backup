use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::process::Command;

use anyhow::{bail, Context, Result};

use super::{file_name, output_path, CHUNK_SIZE};
use crate::update::UpdateConfig;

/// Read+Seek adapter over a CHD file backed by libchdman-rs. Enables
/// filesystem browsing without extracting to a temp file.
pub struct ChdReader {
    chd: libchdman_rs::Chd,
    logical_size: u64,
    position: u64,
}

// SAFETY: `libchdman_rs::Chd` holds a raw pointer to a heap-allocated CHD file
// handle. Operations are routed through &self, but the handle is only accessed
// from one thread at a time (we hand the reader off to a worker thread). The
// underlying C++ object is not shared across threads, so Send is sound.
unsafe impl Send for ChdReader {}

impl ChdReader {
    /// Open a CHD file for reading.
    pub fn open(path: &Path) -> Result<Self> {
        let path_str = path
            .to_str()
            .with_context(|| format!("CHD path is not valid UTF-8: {}", path.display()))?;
        let chd = libchdman_rs::Chd::open(path_str, false, None)
            .map_err(|e| anyhow::anyhow!("failed to open CHD {}: {:?}", path.display(), e))?;
        let logical_size = chd.logical_bytes();
        Ok(Self {
            chd,
            logical_size,
            position: 0,
        })
    }
}

impl Read for ChdReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.position >= self.logical_size {
            return Ok(0);
        }
        let remaining = self.logical_size - self.position;
        let to_read = (buf.len() as u64).min(remaining) as usize;
        if to_read == 0 {
            return Ok(0);
        }
        self.chd
            .read_bytes(self.position, &mut buf[..to_read])
            .map_err(|e| io::Error::other(format!("CHD read error: {:?}", e)))?;
        self.position += to_read as u64;
        Ok(to_read)
    }
}

impl Seek for ChdReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(n) => n as i64,
            SeekFrom::Current(n) => self.position as i64 + n,
            SeekFrom::End(n) => self.logical_size as i64 + n,
        };
        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek before start",
            ));
        }
        self.position = (new_pos as u64).min(self.logical_size);
        Ok(self.position)
    }
}

/// Get the chdman command name or path to use (from config or default to PATH)
fn get_chdman_command() -> String {
    UpdateConfig::load()
        .chdman_path
        .unwrap_or_else(|| "chdman".to_string())
}

/// Detect whether `chdman` is available on PATH or at configured path.
pub fn detect_chdman() -> bool {
    let cmd = get_chdman_command();
    Command::new(&cmd)
        .arg("help")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok()
}

/// Compress via chdman external tool.
///
/// Steps:
/// 1. Write raw data to a temp file next to the output
/// 2. Run `chdman createraw -i temp -o output.chd -hs 4096`
/// 3. Clean up temp file
/// 4. If splitting is needed, split the output CHD manually
pub(crate) fn compress_chd(
    reader: &mut impl Read,
    output_base: &Path,
    split_size: Option<u64>,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
    log_cb: &mut impl FnMut(&str),
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

    // chdman createraw parameters:
    // -us (unit size) = sector size, always 512 bytes
    // -hs (hunk size) = must be a multiple of unit size, and total data
    //     must be a multiple of hunk size. Default to 4096 (8 sectors).
    let unit_size: u64 = 512;
    let hunk_size: u64 = 4096;

    // Pad the raw data to the nearest hunk_size boundary if needed
    let remainder = raw_size % hunk_size;
    if remainder != 0 {
        let pad_bytes = hunk_size - remainder;
        let pad_file = fs::OpenOptions::new()
            .append(true)
            .open(&temp_path)
            .context("failed to open temp file for padding")?;
        let mut pad_writer = BufWriter::new(pad_file);
        let zeros = vec![0u8; pad_bytes as usize];
        pad_writer
            .write_all(&zeros)
            .context("failed to pad temp file")?;
        pad_writer.flush()?;
    }

    let chd_path = output_path(output_base, "chd", false, 0);
    log_cb(&format!(
        "Running chdman createraw -> {}",
        chd_path.display()
    ));
    let chdman_cmd = get_chdman_command();
    let output = Command::new(&chdman_cmd)
        .arg("createraw")
        .arg("-i")
        .arg(&temp_path)
        .arg("-o")
        .arg(&chd_path)
        .arg("-hs")
        .arg(hunk_size.to_string())
        .arg("-us")
        .arg(unit_size.to_string())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .context("failed to run chdman")?;

    // Forward chdman output to log
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            log_cb(trimmed);
        }
    }
    for line in String::from_utf8_lossy(&output.stderr).lines() {
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            log_cb(trimmed);
        }
    }

    let _ = fs::remove_file(&temp_path);

    if !output.status.success() {
        bail!(
            "chdman exited with status {}",
            output.status.code().unwrap_or(-1)
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
pub(crate) fn split_file(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_chdman() {
        // Just ensure it doesn't panic; result depends on system
        let _available = detect_chdman();
    }
}
