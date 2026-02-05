//! IPC protocol for macOS daemon communication.
//!
//! Defines the request/response messages exchanged between the app and the
//! privileged helper daemon via XPC.

use serde::{Deserialize, Serialize};

/// Current daemon version (matches app version).
pub const DAEMON_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Minimum required daemon version for compatibility.
pub const MIN_DAEMON_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Request message from app to daemon.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DaemonRequest {
    /// Get daemon version.
    GetVersion,
    /// Open a disk device for reading.
    OpenDiskRead { path: String },
    /// Open a disk device for writing (unmounts volumes first).
    OpenDiskWrite { path: String },
    /// Read sectors from an open disk.
    ReadSectors { handle: u64, lba: u64, count: u32 },
    /// Write sectors to an open disk.
    WriteSectors {
        handle: u64,
        lba: u64,
        data: Vec<u8>,
    },
    /// Close an open disk handle.
    CloseDisk { handle: u64 },
}

/// Response message from daemon to app.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DaemonResponse {
    /// Daemon version information.
    Version { version: String },
    /// Disk opened successfully.
    DiskOpened { handle: u64, size_bytes: u64 },
    /// Sector data read successfully.
    SectorsRead { data: Vec<u8> },
    /// Operation completed successfully.
    Success,
    /// Operation failed with error message.
    Error { message: String },
}

/// Progress information for crash recovery.
///
/// Written to disk every second during operations, so the app can detect
/// where the daemon was if it crashes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressInfo {
    /// Disk handle being operated on.
    pub handle: u64,
    /// Current logical block address being processed.
    pub current_lba: u64,
    /// Operation type: "read" or "write".
    pub operation: String,
    /// Unix timestamp when this progress was recorded.
    pub timestamp: u64,
}

/// Path where daemon writes progress information.
pub const PROGRESS_FILE: &str = "/tmp/rusty-backup-daemon-progress.json";
