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
