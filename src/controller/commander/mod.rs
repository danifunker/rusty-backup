//! Commander: the two-pane file manager, as controllers every shell renders.
//!
//! [`pane::CommanderPaneController`] holds one pane's state (listing, staged-edit queue,
//! wrapper tree, selection, cursor) and takes [`pane::CommanderPaneAction`]s; [`rows`]
//! turns it into the rows a shell draws. [`screen::CommanderController`] owns both panes
//! and the work between them (copy, export, checksums, File Info, the session log, keys).
//! The egui Commander (`src/gui/commander`) is the first shell on these; the TUI follows.

pub mod ids;
pub mod pane;
pub mod rows;
pub mod screen;

pub use pane::{
    purpose, validate_entry_name, ApplyPreflight, ApplyReport, CommanderPaneAction,
    CommanderPaneController, FailedEdit, NoRecent, OnConflict, PaneServices, RecentStore,
    SortColumn, SourceKind, StageOutcome, VolumeInfo, MAX_CONSECUTIVE_FAILURES,
};
pub use rows::{DisplayRow, RowKind};
pub use screen::{
    ChecksumRow, Checksums, CommanderAction, CommanderController, DetailField, ExportFormat,
    FileDetail, HashAlgo, IdNameMap, MetadataEdit, RenameNotice, ResourceForkMode, Side, SizePlan,
    Xattr,
};

// Plain data and helpers the views need, so they never name the model (CONTRIBUTING.md MVC).
pub use crate::model::edit_queue::EditQueue;
pub use crate::model::job::JobRegistry;
pub use disk_core::display::format_size;
pub use disk_core::entry::FileEntry;

/// A blank 2 MiB FAT volume at `dir/vol.img`, for view tests.
#[cfg(test)]
pub(crate) fn blank_fat_image(dir: &std::path::Path) -> std::path::PathBuf {
    let img = dir.join("vol.img");
    let bytes = fs_pc::fat::create_blank_fat(2 * 1024 * 1024, Some("T")).unwrap();
    std::fs::write(&img, bytes).unwrap();
    img
}
