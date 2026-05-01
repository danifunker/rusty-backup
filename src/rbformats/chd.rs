use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{Context, Result};
use libchdman_rs::{
    hd::{create_from_reader, HdCreateOptions},
    ChdError, CompressionProgress,
};

use super::chd_options::{ChdOptions, ChdProfile};
use super::{file_name, output_path, CHUNK_SIZE};

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

/// Compress raw partition data into a hard-disk CHD via libchdman-rs.
///
/// `logical_size` is the partition's natural byte length. It is rounded
/// up to the nearest unit (512 byte) boundary for the CHD; any tail
/// shortfall in `reader` is zero-padded by libchdman-rs internally.
/// `opts` selects hunk size + codecs; `None` falls back to chdman's HD
/// defaults (`lzma, zlib, huff, flac`, 4096-byte hunks).
pub(crate) fn compress_chd(
    reader: &mut impl Read,
    output_base: &Path,
    logical_size: u64,
    split_size: Option<u64>,
    opts: Option<ChdOptions>,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
    log_cb: &mut impl FnMut(&str),
) -> Result<Vec<String>> {
    let chd_opts = opts.unwrap_or_else(|| ChdOptions::defaults_for(ChdProfile::Hd));

    // chdman uses 512-byte units for HD CHDs. Round logical_size up so
    // libchdman-rs's `logical_size % unit_size == 0` precondition holds;
    // any padding bytes are written by libchdman-rs as zeros.
    const UNIT_SIZE: u64 = 512;
    let padded_size = logical_size.div_ceil(UNIT_SIZE) * UNIT_SIZE;

    let chd_path = output_path(output_base, "chd", false, 0);
    log_cb(&format!(
        "Writing CHD {} (logical {} bytes, hunk {}, codecs {:?})",
        chd_path.display(),
        padded_size,
        chd_opts.hunk_size,
        chd_opts.codecs,
    ));

    let hd_opts = HdCreateOptions {
        logical_size: padded_size,
        hunk_size: chd_opts.hunk_size,
        unit_size: UNIT_SIZE as u32,
        codecs: chd_opts.codecs,
        geometry: None,
        ident: None,
    };

    let mut progress = |p: CompressionProgress| {
        // Clamp to the user-facing logical size so the progress bar never
        // reports more bytes than the caller knows about (CHD pads up to
        // the next hunk; the caller's "total" is `logical_size`).
        progress_cb(p.bytes_done.min(logical_size));
    };

    create_from_reader(reader, &chd_path, hd_opts, &mut progress, cancel_check).map_err(
        |e| match e {
            ChdError::Cancelled => anyhow::anyhow!("backup cancelled"),
            other => anyhow::anyhow!("CHD create failed: {:?}", other),
        },
    )?;

    // Final progress tick at logical_size — libchdman-rs reports in
    // terms of padded size, but the caller's total is `logical_size`.
    progress_cb(logical_size);

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
    use std::io::Cursor;
    use tempfile::TempDir;

    #[test]
    fn compress_chd_round_trip() {
        let tmp = TempDir::new().unwrap();
        let mut data = vec![0u8; 1024 * 1024];
        for (i, b) in data.iter_mut().enumerate() {
            *b = ((i * 31) ^ (i >> 7)) as u8;
        }
        let mut reader = Cursor::new(&data);
        let base = tmp.path().join("partition-0");

        let files = compress_chd(
            &mut reader,
            &base,
            data.len() as u64,
            None,
            None,
            &mut |_| {},
            &|| false,
            &mut |_| {},
        )
        .unwrap();
        assert_eq!(files, vec!["partition-0.chd"]);

        let chd_path = base.with_extension("chd");
        let mut chd_reader = ChdReader::open(&chd_path).unwrap();
        let mut decoded = vec![0u8; data.len()];
        chd_reader.read_exact(&mut decoded).unwrap();
        assert_eq!(decoded, data, "CHD round-trip mismatch");
    }
}
