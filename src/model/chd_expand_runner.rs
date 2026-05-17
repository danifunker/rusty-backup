//! Background worker that grows a CHD image by `add_bytes` of zero-padding.
//!
//! CHD's hunk layout is fixed at creation, so there's no in-place "grow the
//! file" operation. We stream the existing logical bytes through a
//! [`ChdReader`], chain a zero-padding tail of the requested length, and
//! feed the result to the CHD encoder with the new logical size. On
//! success we atomically rename the temp file over the original; on any
//! error or cancellation the temp file is cleaned up.
//!
//! Phase 6c of `docs/disk_expansion.md`.
//!
//! See [`super::status::ChdExpandStatus`] for the Status struct shape, and
//! `docs/progress_pattern.md` for the runner pattern (callbacks at the
//! leaf, Status at the runner).

use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use super::status::ChdExpandStatus;
use crate::rbformats::chd::ChdReader;

/// Spawn a worker that grows the CHD at `path` by `add_bytes` of trailing
/// zero-padding. Returns a shared status the caller polls each frame.
pub fn spawn(path: PathBuf, add_bytes: u64) -> Arc<Mutex<ChdExpandStatus>> {
    let status = Arc::new(Mutex::new(ChdExpandStatus {
        finished: false,
        error: None,
        current_bytes: 0,
        total_bytes: 0,
        cancel_requested: false,
        log_messages: Vec::new(),
    }));

    let status_clone = Arc::clone(&status);
    thread::spawn(move || {
        if let Err(e) = run_expand(&path, add_bytes, &status_clone) {
            if let Ok(mut s) = status_clone.lock() {
                s.error = Some(format!("{e}"));
            }
        }
        if let Ok(mut s) = status_clone.lock() {
            s.finished = true;
        }
    });

    status
}

fn run_expand(
    path: &Path,
    add_bytes: u64,
    status: &Arc<Mutex<ChdExpandStatus>>,
) -> anyhow::Result<()> {
    if add_bytes == 0 {
        anyhow::bail!("add_bytes must be > 0");
    }

    let reader = ChdReader::open(path)?;
    let old_logical = reader.logical_size();
    let new_logical = old_logical + add_bytes;

    log(
        status,
        format!(
            "Re-encoding CHD: {} -> {} ({} new free space)",
            crate::partition::format_size(old_logical),
            crate::partition::format_size(new_logical),
            crate::partition::format_size(add_bytes),
        ),
    );

    if let Ok(mut s) = status.lock() {
        s.total_bytes = new_logical;
    }

    // Write to a temp file next to the source so the atomic rename stays on
    // the same filesystem.
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let stem = path
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "chd_expand".to_string());
    let tmp_path = parent.join(format!(".{stem}.expand.tmp"));
    let tmp_chd_path = tmp_path.with_extension("chd");
    // compress_chd appends ".chd" itself if missing; pass the stem to keep
    // the contract simple.
    let output_base = parent.join(format!(".{stem}.expand.tmp"));

    let cancel_flag = AtomicBool::new(false);
    let status_progress = Arc::clone(status);
    let mut progress_cb = move |bytes: u64| {
        if let Ok(mut s) = status_progress.lock() {
            s.current_bytes = bytes;
            if s.cancel_requested {
                // Translation handled by the closure below.
            }
        }
    };
    let status_cancel = Arc::clone(status);
    let cancel_check = || {
        if let Ok(s) = status_cancel.lock() {
            if s.cancel_requested {
                cancel_flag.store(true, Ordering::Relaxed);
            }
        }
        cancel_flag.load(Ordering::Relaxed)
    };
    let status_log = Arc::clone(status);
    let mut log_cb = move |msg: &str| {
        if let Ok(mut s) = status_log.lock() {
            s.log_messages.push(msg.to_string());
        }
    };

    let mut chained = ChainedZeroPadReader::new(reader, add_bytes);

    let result = crate::rbformats::chd::compress_chd_expand(
        &mut chained,
        &output_base,
        new_logical,
        None,
        None,
        &mut progress_cb,
        &cancel_check,
        &mut log_cb,
    );

    match result {
        Ok(_) => {
            // Atomic rename: tmp_chd_path -> original path.
            log(
                status,
                format!("Renaming {} -> {}", tmp_chd_path.display(), path.display(),),
            );
            std::fs::rename(&tmp_chd_path, path).map_err(|e| {
                anyhow::anyhow!("failed to replace source CHD with re-encoded copy: {e}")
            })?;
            log(status, "CHD expansion complete.".to_string());
            Ok(())
        }
        Err(e) => {
            // Best-effort cleanup of the temp file.
            let _ = std::fs::remove_file(&tmp_chd_path);
            Err(e)
        }
    }
}

fn log(status: &Arc<Mutex<ChdExpandStatus>>, msg: String) {
    if let Ok(mut s) = status.lock() {
        s.log_messages.push(msg);
    }
}

/// Read adapter that emits the wrapped reader's bytes, then `pad_bytes` of
/// zeros, then EOF. Used to feed the CHD encoder a longer logical stream
/// than the source CHD's logical size.
struct ChainedZeroPadReader<R: Read> {
    inner: R,
    inner_exhausted: bool,
    pad_remaining: u64,
}

impl<R: Read> ChainedZeroPadReader<R> {
    fn new(inner: R, pad_bytes: u64) -> Self {
        Self {
            inner,
            inner_exhausted: false,
            pad_remaining: pad_bytes,
        }
    }
}

impl<R: Read> Read for ChainedZeroPadReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if !self.inner_exhausted {
            let n = self.inner.read(buf)?;
            if n == 0 {
                self.inner_exhausted = true;
            } else {
                return Ok(n);
            }
        }
        if self.pad_remaining == 0 {
            return Ok(0);
        }
        let n = (buf.len() as u64).min(self.pad_remaining) as usize;
        buf[..n].fill(0);
        self.pad_remaining -= n as u64;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn chained_zero_pad_reader_emits_source_then_zeros() {
        let source = b"hello".to_vec();
        let mut r = ChainedZeroPadReader::new(Cursor::new(source), 4);
        let mut buf = Vec::new();
        r.read_to_end(&mut buf).unwrap();
        assert_eq!(&buf[..5], b"hello");
        assert_eq!(&buf[5..], &[0, 0, 0, 0]);
    }

    #[test]
    fn chained_zero_pad_reader_eof_after_padding() {
        let mut r = ChainedZeroPadReader::new(Cursor::new(b"abc".to_vec()), 2);
        let mut buf = [0u8; 16];
        let mut total = 0;
        loop {
            let n = r.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            total += n;
        }
        assert_eq!(total, 5);
    }
}
