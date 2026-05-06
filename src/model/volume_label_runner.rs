//! Worker thread for probing the HFS+ volume label of a partition's data
//! source — used by the inspect tab to display volume names on backup-folder
//! loads, where opening the full filesystem on the GUI thread would freeze
//! the UI.
//!
//! The probe itself reads only ~16 KB (volume header + 2 catalog B-tree nodes)
//! through `fs::hfsplus::probe_hfsplus_volume_label`, but for zstd-compressed
//! backups the catalog can sit megabytes into the partition stream, so the
//! work goes on a background thread and the GUI polls
//! `Arc<Mutex<VolumeLabelStatus>>` like every other inspect-tab worker.

use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::fs::hfsplus::probe_hfsplus_volume_label;
use crate::fs::zstd_stream::{ZstdStreamCache, ZstdStreamReader};
use crate::rbformats::chd::ChdReader;

/// Shared state between the GUI poll loop and the worker thread.
pub struct VolumeLabelStatus {
    pub partition_index: usize,
    pub finished: bool,
    /// `Some(label)` if the probe successfully read a non-empty volume name.
    pub label: Option<String>,
    pub error: Option<String>,
}

impl VolumeLabelStatus {
    fn new(partition_index: usize) -> Self {
        Self {
            partition_index,
            finished: false,
            label: None,
            error: None,
        }
    }
}

/// What the worker should read from. Each variant maps to one of the data
/// sources `apply_backup_outcome` can hand off:
///
/// - `Raw` for `partition-N.raw` and any other plain file with the partition
///   bytes laid out from offset 0.
/// - `Zstd` for `partition-N.zst`; the worker builds a one-shot
///   `ZstdStreamCache` for the probe and drops it on completion.
/// - `Chd` for backup folders that store partitions as per-partition CHD
///   containers (and for plain Mac CHDs opened directly).
pub enum VolumeLabelSource {
    /// Plain file with partition bytes from offset 0 (or some `partition_offset`
    /// for whole-disk images).
    Raw {
        path: PathBuf,
        partition_offset: u64,
    },
    /// Zstd-compressed partition data; the partition bytes start at the
    /// decompressed-stream offset 0.
    Zstd { path: PathBuf },
    /// CHD container; `partition_offset` is the byte offset of the partition
    /// inside the unwrapped disk image. For per-partition CHDs (one CHD per
    /// partition) pass `0`.
    Chd {
        path: PathBuf,
        partition_offset: u64,
    },
}

/// Inputs to `spawn`.
pub struct VolumeLabelRequest {
    pub source: VolumeLabelSource,
    pub partition_index: usize,
}

/// Spawn a worker thread; returns the shared status the GUI polls.
pub fn spawn(req: VolumeLabelRequest) -> Arc<Mutex<VolumeLabelStatus>> {
    let status = Arc::new(Mutex::new(VolumeLabelStatus::new(req.partition_index)));
    let status_thread = Arc::clone(&status);
    thread::spawn(move || {
        let result = probe(req.source);
        if let Ok(mut s) = status_thread.lock() {
            match result {
                Ok(label) => s.label = label,
                Err(e) => s.error = Some(e),
            }
            s.finished = true;
        }
    });
    status
}

fn probe(source: VolumeLabelSource) -> Result<Option<String>, String> {
    match source {
        VolumeLabelSource::Raw {
            path,
            partition_offset,
        } => {
            let f = File::open(&path).map_err(|e| format!("open {}: {e}", path.display()))?;
            let mut r = BufReader::new(f);
            Ok(probe_hfsplus_volume_label(&mut r, partition_offset))
        }
        VolumeLabelSource::Zstd { path } => {
            let cache = ZstdStreamCache::new(&path)
                .map_err(|e| format!("zstd open {}: {e}", path.display()))?;
            let mut r = ZstdStreamReader::new(Arc::new(Mutex::new(cache)));
            Ok(probe_hfsplus_volume_label(&mut r, 0))
        }
        VolumeLabelSource::Chd {
            path,
            partition_offset,
        } => {
            let mut r =
                ChdReader::open(&path).map_err(|e| format!("chd open {}: {e}", path.display()))?;
            Ok(probe_hfsplus_volume_label(&mut r, partition_offset))
        }
    }
}
