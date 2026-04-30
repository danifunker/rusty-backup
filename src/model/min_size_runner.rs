//! Worker thread for computing partition minimum sizes for filesystems that
//! require an expensive volume walk (HFS / HFS+ / ext / btrfs / ProDOS).
//!
//! GUI tabs hold an `Arc<Mutex<MinSizeStatus>>` per pending partition and poll
//! it each frame; the worker writes phase strings while it scans and the final
//! result on completion.

use std::fs::File;
use std::io::BufReader;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::fs::{self, MinimumResult};
use crate::os::SectorAlignedReader;

/// Shared state between the GUI poll loop and the worker thread.
pub struct MinSizeStatus {
    pub partition_index: usize,
    /// Latest phase reported by `partition_minimum_size` (e.g. "Opening filesystem...").
    pub phase: String,
    pub finished: bool,
    /// `Some(bytes)` on successful computation; `None` if the FS could not be opened.
    pub result: Option<u64>,
    pub error: Option<String>,
}

impl MinSizeStatus {
    fn new(partition_index: usize) -> Self {
        Self {
            partition_index,
            phase: "Starting...".to_string(),
            finished: false,
            result: None,
            error: None,
        }
    }
}

/// Inputs to `spawn`. The file handle is shared via `Arc<File>` so multiple
/// concurrent calculations (one per partition) can clone independently.
pub struct MinSizeRequest {
    pub file: Arc<File>,
    pub partition_offset: u64,
    pub partition_type: u8,
    pub partition_type_string: Option<String>,
    pub partition_size: u64,
    pub partition_index: usize,
    /// Use `SectorAlignedReader` for raw block devices (`/dev/rdiskN`).
    pub use_sector_aligned: bool,
}

/// Spawn a worker thread; returns the shared status the GUI polls.
pub fn spawn(req: MinSizeRequest) -> Arc<Mutex<MinSizeStatus>> {
    let status = Arc::new(Mutex::new(MinSizeStatus::new(req.partition_index)));
    let status_thread = Arc::clone(&status);
    thread::spawn(move || {
        let clone = match req.file.try_clone() {
            Ok(f) => f,
            Err(e) => {
                if let Ok(mut s) = status_thread.lock() {
                    s.error = Some(format!("clone failed: {e}"));
                    s.finished = true;
                }
                return;
            }
        };

        let progress_status = Arc::clone(&status_thread);
        let progress = move |phase: &str| {
            if let Ok(mut s) = progress_status.lock() {
                s.phase = phase.to_string();
            }
        };

        let result = if req.use_sector_aligned {
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
        };

        if let Ok(mut s) = status_thread.lock() {
            match result {
                MinimumResult::Computed(m) => s.result = m,
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
