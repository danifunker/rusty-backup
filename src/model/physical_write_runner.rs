//! Background-thread runner for the Physical Disk Export action.
//!
//! Streams a source image directly to a `/dev/sdX` (or platform equivalent)
//! device handle. Optionally wraps a superfloppy source with a synthetic
//! MBR/GPT partition table on the way through, using
//! [`crate::restore::superfloppy_wrap`].
//!
//! Follows the canonical pattern: leaf I/O (`superfloppy_wrap::wrap_and_write`,
//! direct-copy loop) takes `progress_cb` / `cancel_check` / `log_cb` callbacks;
//! the runner owns `Arc<Mutex<PhysicalWriteStatus>>` and translates callback
//! fires into Status writes. The GUI polls the Status struct each frame.
//!
//! See `docs/progress_pattern.md` for the canonical shape.
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};

use crate::os::{open_target_for_writing, SectorAlignedWriter};
use crate::restore::superfloppy_wrap::{self, WrapParams};

const STREAM_CHUNK: usize = 1024 * 1024;

/// Shared status for a background Physical Disk Export operation.
pub struct PhysicalWriteStatus {
    pub finished: bool,
    pub error: Option<String>,
    pub log_messages: Vec<String>,
    pub current_bytes: u64,
    pub total_bytes: u64,
    pub cancel_requested: bool,
}

/// Source for a physical-disk write. v1 only handles raw files; backup-folder
/// sources route through the existing per-partition restore path and aren't
/// wired in here yet.
#[derive(Debug, Clone)]
pub enum PhysicalWriteSource {
    /// Path to a raw image file. Length is read from the file's metadata.
    RawFile(PathBuf),
}

/// Inputs for a Physical Disk Export job.
#[derive(Debug, Clone)]
pub struct PhysicalWriteRequest {
    pub source: PhysicalWriteSource,
    pub target_device_path: PathBuf,
    pub target_size_bytes: u64,
    /// `Some(params)` to synthesize a partition table around the source.
    /// `None` to write the source byte-for-byte to LBA 0 of the target.
    pub wrap: Option<WrapParams>,
}

fn new_status(total_bytes: u64) -> Arc<Mutex<PhysicalWriteStatus>> {
    Arc::new(Mutex::new(PhysicalWriteStatus {
        finished: false,
        error: None,
        log_messages: Vec::new(),
        current_bytes: 0,
        total_bytes,
        cancel_requested: false,
    }))
}

/// Spawn a Physical Disk Export. Returns the Status handle for GUI polling.
pub fn start_physical_write(req: PhysicalWriteRequest) -> Arc<Mutex<PhysicalWriteStatus>> {
    let total = match &req.wrap {
        Some(p) => p.partition_size_bytes,
        None => source_size_for(&req.source).unwrap_or(0),
    };
    let status = new_status(total);
    let status_thread = Arc::clone(&status);

    std::thread::spawn(move || {
        let _wake = crate::os::wakelock::acquire("Rusty Backup: physical disk export");
        let result = run_worker(&req, Arc::clone(&status_thread));
        if let Ok(mut s) = status_thread.lock() {
            s.finished = true;
            if let Err(e) = result {
                s.error = Some(format!("{e:#}"));
            }
        }
    });

    status
}

fn source_size_for(source: &PhysicalWriteSource) -> Option<u64> {
    match source {
        PhysicalWriteSource::RawFile(p) => std::fs::metadata(p).ok().map(|m| m.len()),
    }
}

/// Worker function. Public for unit testing — call this inline with a
/// throwaway Status handle to skip the thread spawn.
pub fn run_worker(
    req: &PhysicalWriteRequest,
    status: Arc<Mutex<PhysicalWriteStatus>>,
) -> Result<()> {
    let status_progress = Arc::clone(&status);
    let status_cancel = Arc::clone(&status);
    let status_log = Arc::clone(&status);

    let mut progress_cb = move |bytes: u64| {
        if let Ok(mut s) = status_progress.lock() {
            s.current_bytes = bytes;
        }
    };
    let cancel_check = move || {
        status_cancel
            .lock()
            .map(|s| s.cancel_requested)
            .unwrap_or(false)
    };
    let mut log_cb = move |msg: &str| {
        if let Ok(mut s) = status_log.lock() {
            s.log_messages.push(msg.to_string());
        }
    };

    let handle = open_target_for_writing(&req.target_device_path).with_context(|| {
        format!(
            "opening target device {} for writing",
            req.target_device_path.display()
        )
    })?;
    // `handle` may carry platform-specific lock/claim fields that must remain
    // alive until the write finishes; bind it so it drops at end of scope.
    let mut target = SectorAlignedWriter::new(handle.file);

    match &req.source {
        PhysicalWriteSource::RawFile(path) => {
            let file =
                File::open(path).with_context(|| format!("opening source {}", path.display()))?;
            let source_size = file
                .metadata()
                .with_context(|| format!("stat source {}", path.display()))?
                .len();
            log_cb(&format!(
                "Source: {} ({} bytes)",
                path.display(),
                source_size,
            ));

            let mut src = file;
            if let Some(params) = &req.wrap {
                superfloppy_wrap::wrap_and_write(
                    &mut src,
                    source_size,
                    &mut target,
                    params,
                    &mut progress_cb,
                    &cancel_check,
                    &mut log_cb,
                )?;
            } else {
                direct_copy(
                    &mut src,
                    &mut target,
                    source_size,
                    &mut progress_cb,
                    &cancel_check,
                    &mut log_cb,
                )?;
            }
        }
    }

    target.flush().context("flushing target after write")?;
    log_cb("Physical disk export complete");
    Ok(())
}

fn direct_copy<R, W>(
    source: &mut R,
    target: &mut W,
    source_size: u64,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
    log_cb: &mut impl FnMut(&str),
) -> Result<()>
where
    R: Read,
    W: Write + Seek,
{
    target
        .seek(SeekFrom::Start(0))
        .context("seek target LBA 0")?;
    log_cb(&format!(
        "Direct write: streaming {} bytes to target",
        source_size
    ));
    let mut buf = vec![0u8; STREAM_CHUNK];
    let mut written: u64 = 0;
    while written < source_size {
        if cancel_check() {
            anyhow::bail!("physical write cancelled");
        }
        let want = ((source_size - written).min(STREAM_CHUNK as u64)) as usize;
        let mut filled = 0;
        while filled < want {
            let n = source
                .read(&mut buf[filled..want])
                .context("reading source")?;
            if n == 0 {
                anyhow::bail!(
                    "source ended early: expected {} bytes, got {}",
                    source_size,
                    written + filled as u64
                );
            }
            filled += n;
        }
        target.write_all(&buf[..filled]).context("writing target")?;
        written += filled as u64;
        progress_cb(written);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::restore::superfloppy_wrap::{WrapAlignment, WrapParams, WrapTable};
    use std::io::Write;
    use tempfile::NamedTempFile;

    const ONE_MIB: u64 = 1024 * 1024;

    fn ntfs_vbr_sector() -> [u8; 512] {
        let mut s = [0u8; 512];
        s[0] = 0xEB;
        s[1] = 0x52;
        s[2] = 0x90;
        s[3..11].copy_from_slice(b"NTFS    ");
        s[11..13].copy_from_slice(&512u16.to_le_bytes());
        s[510] = 0x55;
        s[511] = 0xAA;
        s
    }

    fn write_source(size: u64) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        let mut buf = vec![0u8; size as usize];
        buf[..512].copy_from_slice(&ntfs_vbr_sector());
        f.write_all(&buf).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn test_run_worker_wrap_to_file_target() {
        // Use a regular file as the "device" target — open_target_for_writing
        // creates/truncates a file path that doesn't start with /dev/ or \\.\.
        let src_file = write_source(4 * ONE_MIB);
        let target_file = NamedTempFile::new().unwrap();
        let target_path = target_file.path().to_path_buf();
        drop(target_file); // we'll recreate via open_target_for_writing

        let params = WrapParams {
            table: WrapTable::Mbr {
                type_byte: 0x07,
                bootable: false,
                alignment: WrapAlignment::Modern1MB,
            },
            partition_size_bytes: 4 * ONE_MIB,
            target_size_bytes: 8 * ONE_MIB,
            source_fs_hint: "NTFS".into(),
        };
        let req = PhysicalWriteRequest {
            source: PhysicalWriteSource::RawFile(src_file.path().to_path_buf()),
            target_device_path: target_path.clone(),
            target_size_bytes: 8 * ONE_MIB,
            wrap: Some(params),
        };
        let status = new_status(0);
        run_worker(&req, Arc::clone(&status)).expect("worker");
        let bytes = std::fs::read(&target_path).unwrap();
        // MBR signature.
        assert_eq!(bytes[510], 0x55);
        assert_eq!(bytes[511], 0xAA);
        // VBR at LBA 2048.
        let off = 2048 * 512;
        assert_eq!(&bytes[off + 3..off + 11], b"NTFS    ");
    }

    #[test]
    #[allow(clippy::identity_op)] // `1 * ONE_MIB` reads as "1 MiB"
    fn test_run_worker_direct_copy() {
        let src_file = write_source(1 * ONE_MIB);
        let target_file = NamedTempFile::new().unwrap();
        let target_path = target_file.path().to_path_buf();
        drop(target_file);

        let req = PhysicalWriteRequest {
            source: PhysicalWriteSource::RawFile(src_file.path().to_path_buf()),
            target_device_path: target_path.clone(),
            target_size_bytes: 1 * ONE_MIB,
            wrap: None,
        };
        let status = new_status(0);
        run_worker(&req, Arc::clone(&status)).expect("worker");
        let bytes = std::fs::read(&target_path).unwrap();
        // First sector matches the NTFS VBR.
        assert_eq!(bytes[0], 0xEB);
        assert_eq!(&bytes[3..11], b"NTFS    ");
    }

    #[test]
    fn test_run_worker_cancel() {
        let src_file = write_source(4 * ONE_MIB);
        let target_file = NamedTempFile::new().unwrap();
        let target_path = target_file.path().to_path_buf();
        drop(target_file);

        let req = PhysicalWriteRequest {
            source: PhysicalWriteSource::RawFile(src_file.path().to_path_buf()),
            target_device_path: target_path,
            target_size_bytes: 8 * ONE_MIB,
            wrap: None,
        };
        let status = new_status(0);
        // Pre-set cancel before starting.
        status.lock().unwrap().cancel_requested = true;
        let result = run_worker(&req, Arc::clone(&status));
        assert!(result.is_err());
    }
}
