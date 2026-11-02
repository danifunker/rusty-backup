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
    FileDetail, HashAlgo, MetadataEdit, RenameNotice, ResourceForkMode, Side,
};
