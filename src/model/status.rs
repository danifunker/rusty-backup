//! Background-thread status structs polled by the GUI.
//!
//! Each one is an Arc-Mutex'd shared state between a worker thread (writes
//! progress / log lines / errors) and the GUI thread (reads them on every
//! frame and renders). Moving them to the model layer means the same status
//! types can be reused if a different front-end ever drives the same workers.
//!
//! Extracted from `gui/inspect_tab.rs` and `gui/backup_tab.rs` per §5 of
//! `docs/codecleanup.md`.

use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::clonezilla::block_cache::PartcloneBlockCache;
use crate::fs::hfs_clone::{CloneReport, EmitReport};
use crate::os::TempFileGuard;
use crate::partition::{PartitionAlignment, PartitionInfo, PartitionTable};

/// Status of a background disk inspect operation.
pub struct InspectStatus {
    pub finished: bool,
    pub error: Option<String>,
    pub log_messages: Vec<String>,
    /// Human-readable description of the current step shown in the UI.
    pub current_step: String,
    // Results, populated on success:
    pub partition_table: Option<PartitionTable>,
    pub alignment: Option<PartitionAlignment>,
    pub partitions: Vec<PartitionInfo>,
    pub partition_min_sizes: HashMap<usize, u64>,
    /// Defragmented minimum (size after a clone-shrink). Populated only for
    /// HFS+ partitions where it can differ meaningfully from the in-place
    /// trim point in `partition_min_sizes`.
    pub partition_defrag_min_sizes: HashMap<usize, u64>,
    /// Per-partition volume labels (FAT volume label, HFS+ volume name, etc.)
    /// for display in the inspect grid. Empty when the filesystem has no
    /// label or could not be probed.
    pub partition_volume_labels: HashMap<usize, String>,
    /// Partitions whose minimum size requires an expensive volume walk; the
    /// GUI surfaces a "Calculate minimum size" button per index. Value is the
    /// human-readable filesystem name (for log/UI text).
    pub deferred_min_sizes: HashMap<usize, &'static str>,
    /// Detected image format label (e.g. "WOZ 3.5\"", "Fixed VHD").
    pub format_label: Option<String>,
    /// The open device file handle, passed back to the main thread so
    /// that BrowseView can reuse it without re-opening (and re-prompting).
    pub device_file: Option<File>,
    /// The `TempFileGuard` that holds the DiskClaim / temp file alive.
    /// Kept here so the main thread can decide when to release it.
    pub device_guard: Option<TempFileGuard>,
}

/// Status of a background seekable cache creation (native zstd backups).
pub struct CacheStatus {
    pub finished: bool,
    pub error: Option<String>,
    pub partition_index: usize,
    pub cache_path: Option<PathBuf>,
    pub current_bytes: u64,
    pub total_bytes: u64,
}

/// Status of a background block cache metadata scan (Clonezilla images).
pub struct BlockCacheScan {
    pub finished: bool,
    pub error: Option<String>,
    pub partition_index: usize,
    pub partition_type: u8,
    pub cache: Arc<Mutex<PartcloneBlockCache>>,
}

/// Status of a VHD whole-disk export running on a background thread.
pub struct VhdExportStatus {
    pub finished: bool,
    pub error: Option<String>,
    pub current_bytes: u64,
    pub total_bytes: u64,
    pub cancel_requested: bool,
    pub log_messages: Vec<String>,
}

/// Status of a background file/folder extraction from `BrowseView`.
pub struct ExtractionProgress {
    pub current_bytes: u64,
    pub total_bytes: u64,
    pub current_file: String,
    pub files_extracted: u32,
    /// Count of entries skipped because they could not be read/written
    /// (e.g. NTFS system metafiles with no $DATA attribute). A skipped
    /// file does not abort the whole extraction.
    pub files_skipped: u32,
    pub total_files: u32,
    pub finished: bool,
    pub error: Option<String>,
    pub cancel_requested: bool,
}

/// Status of a CHD "Expand Image" re-encode (Phase 6c of
/// `docs/disk_expansion.md`). Decodes the existing logical CHD,
/// concatenates a zero-padding tail, and writes a fresh CHD with the new
/// logical size. The CHD layout is fixed at creation, so growing in place
/// isn't possible — we re-encode and atomically rename on success.
pub struct ChdExpandStatus {
    pub finished: bool,
    pub error: Option<String>,
    pub current_bytes: u64,
    pub total_bytes: u64,
    pub cancel_requested: bool,
    pub log_messages: Vec<String>,
}

/// Status of an in-place partition resize running on a background thread.
pub struct ResizeStatus {
    pub finished: bool,
    pub error: Option<String>,
    pub log_messages: Vec<String>,
    pub current_bytes: u64,
    pub total_bytes: u64,
    pub cancel_requested: bool,
}

/// Status of an "Expand HFS Volume…" run.
pub struct ExpandStatus {
    pub finished: bool,
    pub error: Option<String>,
    pub log_messages: Vec<String>,
    pub current_step: String,
    pub clone_report: Option<CloneReport>,
    pub emit_report: Option<EmitReport>,
}

/// Log level for `BulkConvertStatus.log_messages`.
#[derive(Clone, Copy)]
pub enum BulkConvertLogLevel {
    Info,
    Warn,
    Error,
}

/// Status of a Bulk Convert dialog job.
pub struct BulkConvertStatus {
    pub finished: bool,
    pub cancel_requested: bool,
    /// 1-based index of the file currently being processed.
    pub current_index: usize,
    pub total_files: usize,
    pub current_file: String,
    /// Bytes written for the currently-processing file (for the progress bar).
    pub current_bytes: u64,
    pub current_total_bytes: u64,
    pub succeeded: usize,
    pub failed: usize,
    /// Drained by the main thread into the GUI log panel each frame.
    pub log_messages: Vec<(BulkConvertLogLevel, String)>,
}
