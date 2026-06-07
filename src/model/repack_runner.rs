//! Background worker for the Human68k (X68000) "Defragment" action: clone
//! the partition into a fresh, contiguously-packed volume and write it back
//! over the partition region of the image file.
//!
//! Shared by the GUI inspect tab (the per-partition "Defragment" button)
//! and modeled on [`super::chd_expand_runner`]: spawn returns a shared
//! [`RepackStatus`] the caller polls each frame. The heavy lifting lives in
//! [`crate::fs::human68k_clone::stream_defragmented_human68k`] — the same
//! engine the `rb-cli repack` verb calls — so the GUI and CLI behave
//! identically.
//!
//! The clone reads the source fully into a tempfile before draining it back
//! to the writer, so the read-only source handle and the read-write
//! write-back handle (both onto the same file) never race.

use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;

use super::status::RepackStatus;
use crate::fs::filesystem::Filesystem;
use crate::fs::human68k::Human68kFilesystem;
use crate::fs::human68k_clone::stream_defragmented_human68k;
use crate::partition::format_size;

/// Spawn a worker that defragments the Human68k partition at
/// `partition_offset` inside the image at `path`, in place. Returns a
/// shared status the caller polls each frame.
pub fn spawn(path: PathBuf, partition_offset: u64) -> Arc<Mutex<RepackStatus>> {
    let status = Arc::new(Mutex::new(RepackStatus::default()));
    let status_clone = Arc::clone(&status);
    thread::spawn(move || {
        if let Err(e) = run_repack(&path, partition_offset, &status_clone) {
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

fn log(status: &Arc<Mutex<RepackStatus>>, msg: impl Into<String>) {
    if let Ok(mut s) = status.lock() {
        s.log_messages.push(msg.into());
    }
}

fn run_repack(
    path: &Path,
    partition_offset: u64,
    status: &Arc<Mutex<RepackStatus>>,
) -> anyhow::Result<()> {
    // Read-only source handle. Opening the filesystem also validates that
    // the partition really is a Human68k volume.
    let ro = std::fs::File::open(path)
        .map_err(|e| anyhow::anyhow!("opening {} for reading: {e}", path.display()))?;
    let mut source = Human68kFilesystem::open(ro, partition_offset)
        .map_err(|e| anyhow::anyhow!("partition is not a Human68k volume: {e}"))?;

    // Pack into the filesystem's own declared size (BPB total_sectors x
    // sector size), which is always a whole number of FS sectors — unlike
    // the partition-table length on 256-byte-SASI disks.
    let fs_size = source.total_size();
    if let Ok(mut s) = status.lock() {
        s.total_bytes = fs_size;
    }
    log(
        status,
        format!("Defragmenting Human68k volume ({})", format_size(fs_size)),
    );

    let mut rw = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|e| anyhow::anyhow!("opening {} for writing: {e}", path.display()))?;
    rw.seek(SeekFrom::Start(partition_offset))?;

    let mut log_cb = |s: &str| log(status, s.to_string());
    let mut progress_cb = |done: u64| {
        if let Ok(mut s) = status.lock() {
            s.current_bytes = done;
        }
    };
    let report =
        stream_defragmented_human68k(&mut source, fs_size, &mut rw, &mut log_cb, &mut progress_cb)
            .map_err(|e| anyhow::anyhow!("repack failed: {e}"))?;
    rw.flush()?;

    for w in &report.warnings {
        log(status, format!("Warning: {w}"));
    }
    let summary = format!(
        "Defragment complete: {} dir(s) / {} file(s) / {} packed",
        report.dirs_copied,
        report.files_copied,
        format_size(report.bytes_copied),
    );
    log(status, summary.clone());
    if let Ok(mut s) = status.lock() {
        s.summary = Some(summary);
    }
    Ok(())
}
