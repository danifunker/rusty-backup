//! Shared filesystem check (fsck) and repair types.
//!
//! These types are filesystem-agnostic. Each filesystem's fsck module
//! (e.g. `hfs_fsck`) produces results using these shared types so the
//! GUI and trait layer don't need filesystem-specific knowledge.

/// Result of a filesystem integrity check.
pub struct FsckResult {
    /// Errors that indicate data corruption or inconsistency.
    pub errors: Vec<FsckIssue>,
    /// Non-critical issues that don't prevent normal operation.
    pub warnings: Vec<FsckIssue>,
    /// Aggregate statistics from the check.
    pub stats: FsckStats,
    /// True when at least one error can be automatically repaired.
    pub repairable: bool,
    /// Files and directories whose parent directory is missing from the catalog.
    /// These represent unrepairable structural corruption.
    pub orphaned_entries: Vec<OrphanedEntry>,
}

impl FsckResult {
    /// Returns true if no errors were found (warnings are tolerated).
    pub fn is_clean(&self) -> bool {
        self.errors.is_empty()
    }
}

/// A single issue found during the check.
pub struct FsckIssue {
    /// Short identifier for the issue type (e.g. "BadSignature", "MissingParent").
    pub code: String,
    /// Human-readable description of the issue.
    pub message: String,
    /// Whether this issue can be automatically repaired.
    pub repairable: bool,
}

/// Aggregate statistics from a filesystem check.
pub struct FsckStats {
    pub files_checked: u32,
    pub directories_checked: u32,
    /// Filesystem-specific extra statistics (label, value).
    pub extra: Vec<(String, String)>,
}

/// A file or directory whose parent directory is missing.
pub struct OrphanedEntry {
    /// Filesystem ID of the orphaned entry (e.g. CNID for HFS, inode for ext).
    pub id: u64,
    /// Name of the entry (decoded to UTF-8).
    pub name: String,
    /// Whether this is a directory (true) or file (false).
    pub is_directory: bool,
    /// The missing parent's filesystem ID.
    pub missing_parent_id: u64,
}

/// Report from a repair operation.
pub struct RepairReport {
    /// Descriptions of successfully applied fixes.
    pub fixes_applied: Vec<String>,
    /// Descriptions of fixes that were attempted but failed.
    pub fixes_failed: Vec<String>,
    /// Number of errors that cannot be repaired (e.g. missing parent directories).
    pub unrepairable_count: usize,
}
