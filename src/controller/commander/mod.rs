//! Commander: the two-pane file manager, as controllers every shell renders.
//!
//! Each pane is a [`crate::controller::browser::BrowserPaneController`] with the
//! [`PaneProfile::commander`] profile (re-exported here as `pane` and `rows`, the paths the
//! views already use). [`screen::CommanderController`] owns both panes and the work between
//! them (copy, export, checksums, File Info, the session log, keys). The egui Commander
//! (`src/gui/commander`) and the TUI Commander are shells on these.

pub mod ids;
pub mod screen;

pub use crate::controller::browser::{pane, rows};
pub use crate::controller::browser::{
    purpose, validate_entry_name, ApplyPreflight, ApplyReport, BrowserPaneAction,
    BrowserPaneController, CheckSummary, DetailPlacement, DisplayRow, FailedEdit, JournalDetail,
    ListingStyle, NoRecent, OnConflict, PaneProfile, PaneServices, RecentStore, RepairSummary,
    RowKind, SortColumn, SourceKind, StageOutcome, ToolReport, TreeText, VolumeInfo,
    MAX_CONSECUTIVE_FAILURES,
};
pub use screen::{
    export_choices, ChecksumRow, Checksums, CommanderAction, CommanderController, DetailField,
    ExportChoice, ExportFormat, ExtractOptions, FileDetail, HashAlgo, IdNameMap, MetadataEdit,
    RenameNotice, ResourceForkMode, Side, SizePlan, ViewItemKind, ViewMenuItem, Xattr,
    FOLLOW_DELAY,
};

// Plain data and helpers the views need, so they never name the model (CONTRIBUTING.md MVC).
pub use crate::model::browse_session::BrowseSession;
pub use crate::model::edit_queue::EditQueue;
pub use crate::model::job::{JobRegistry, Snapshot};
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
