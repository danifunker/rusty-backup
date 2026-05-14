//! Worker thread for computing partition minimum sizes for filesystems that
//! require an expensive volume walk (HFS / HFS+ / ext / btrfs / ProDOS).
//!
//! GUI tabs hold an `Arc<Mutex<MinSizeStatus>>` per pending partition and poll
//! it each frame; the worker writes phase strings while it scans and the final
//! result on completion.

use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::fs::{self, MinimumResult};
use crate::os::SectorAlignedReader;
use crate::rbformats::chd::ChdReader;

/// Shared state between the GUI poll loop and the worker thread.
pub struct MinSizeStatus {
    pub partition_index: usize,
    /// Latest phase reported by `partition_minimum_size` (e.g. "Opening filesystem...").
    pub phase: String,
    /// Append-only log of every phase emitted by the worker. The GUI poll loop
    /// drains this each frame so transient diagnostic lines (e.g. wrapper-info
    /// dumps) aren't lost when multiple progress() calls land between polls.
    pub phase_log: Vec<String>,
    pub finished: bool,
    /// In-place trim point (no data moved). `Some(bytes)` on successful computation;
    /// `None` if the FS could not be opened.
    pub result: Option<u64>,
    /// Defragmented minimum (size the volume would shrink to after a packed clone).
    /// Equals `result` for filesystems without a clone path; only meaningfully
    /// smaller for HFS+ on fragmented volumes.
    pub defragmented_min: Option<u64>,
    pub error: Option<String>,
}

impl MinSizeStatus {
    fn new(partition_index: usize) -> Self {
        Self {
            partition_index,
            phase: "Starting...".to_string(),
            phase_log: Vec::new(),
            finished: false,
            result: None,
            defragmented_min: None,
            error: None,
        }
    }
}

/// What the worker should read from. Most callers hand in a pre-opened
/// (possibly elevated) `File`; CHD sources are addressed by path because we
/// open a fresh `ChdReader` per worker.
pub enum MinSizeSource {
    /// Pre-opened raw file/device handle. `try_clone` is called per worker.
    /// Set `use_sector_aligned` for raw block devices (`/dev/rdiskN`).
    File {
        file: Arc<File>,
        use_sector_aligned: bool,
    },
    /// Path to a CHD container — the worker opens its own `ChdReader`.
    Chd(PathBuf),
}

/// Inputs to `spawn`.
pub struct MinSizeRequest {
    pub source: MinSizeSource,
    pub partition_offset: u64,
    pub partition_type: u8,
    pub partition_type_string: Option<String>,
    pub partition_size: u64,
    pub partition_index: usize,
}

/// Spawn a worker thread; returns the shared status the GUI polls.
pub fn spawn(req: MinSizeRequest) -> Arc<Mutex<MinSizeStatus>> {
    let status = Arc::new(Mutex::new(MinSizeStatus::new(req.partition_index)));
    let status_thread = Arc::clone(&status);
    thread::spawn(move || {
        let progress_status = Arc::clone(&status_thread);
        let progress = move |phase: &str| {
            if let Ok(mut s) = progress_status.lock() {
                s.phase = phase.to_string();
                s.phase_log.push(phase.to_string());
            }
        };

        let result = match req.source {
            MinSizeSource::File {
                file,
                use_sector_aligned,
            } => {
                let clone = match file.try_clone() {
                    Ok(f) => f,
                    Err(e) => {
                        if let Ok(mut s) = status_thread.lock() {
                            s.error = Some(format!("clone failed: {e}"));
                            s.finished = true;
                        }
                        return;
                    }
                };
                if use_sector_aligned {
                    fs::partition_minimum_size(
                        SectorAlignedReader::new(clone),
                        req.partition_offset,
                        req.partition_type,
                        req.partition_type_string.as_deref(),
                        req.partition_size,
                        true,
                        &progress,
                    )
                } else {
                    fs::partition_minimum_size(
                        BufReader::new(clone),
                        req.partition_offset,
                        req.partition_type,
                        req.partition_type_string.as_deref(),
                        req.partition_size,
                        true,
                        &progress,
                    )
                }
            }
            MinSizeSource::Chd(path) => match ChdReader::open(&path) {
                Ok(reader) => fs::partition_minimum_size(
                    reader,
                    req.partition_offset,
                    req.partition_type,
                    req.partition_type_string.as_deref(),
                    req.partition_size,
                    true,
                    &progress,
                ),
                Err(e) => {
                    if let Ok(mut s) = status_thread.lock() {
                        s.error = Some(format!("open CHD failed: {e}"));
                        s.finished = true;
                    }
                    return;
                }
            },
        };

        if let Ok(mut s) = status_thread.lock() {
            match result {
                MinimumResult::Computed {
                    in_place,
                    defragmented,
                } => {
                    s.result = in_place;
                    s.defragmented_min = defragmented;
                }
                MinimumResult::Deferred { fs_name } => {
                    s.error = Some(format!(
                        "internal: partition_minimum_size deferred unexpectedly ({fs_name})"
                    ));
                }
            }
            s.finished = true;
        }
    });
    status
}
