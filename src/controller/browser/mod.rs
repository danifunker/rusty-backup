//! The file browser pane: one controller, configured by a [`PaneProfile`], behind every file view.
//!
//! [`pane::BrowserPaneController`] holds one pane's state (source, listing, staged-edit queue,
//! wrapper tree, selection, cursor) and takes [`pane::BrowserPaneAction`]s; [`rows`] turns it
//! into the rows a shell draws. Each use configures it with a [`PaneProfile`]: each Commander
//! pane (`PaneProfile::commander`), the TUI Explorer and the Inspect browse view. A capability
//! the profile turns off refuses its action with a message instead of acting. See
//! docs/phase2_commander_plan.md § "Slice 10: one browser pane".

pub mod pane;
pub mod profile;
pub mod rows;

pub use pane::{
    format_mac_date, journal_block_class, journal_txn_classes, parse_mac_date, purpose,
    validate_entry_name, ApplyPreflight, ApplyReport, ArchiveRead, ArchiveRow, ArchiveSave,
    ArchiveUse, ArchiveViewer, BrowserPaneAction, BrowserPaneController, CheckSummary,
    CursorDetail, FailedEdit, ForkFormat, ImportChoice, JournalDetail, LineEnding, MacArchiveKind,
    MetadataFields, MetadataForm, NoRecent, OnConflict, PaneServices, RecentStore, RepairSummary,
    SortColumn, SourceKind, StageOutcome, TarChoice, TarPreview, TextDoc, TextEncoding, TextShape,
    ToolReport, TreeNode, TreeText, VolumeInfo, MAX_CONSECUTIVE_FAILURES, MAX_TEXT_EDIT,
};
pub use profile::{DetailPlacement, ListingStyle, PaneProfile, Sources};
pub use rows::{DisplayRow, RowKind};

#[cfg(test)]
mod archive_tests;
#[cfg(test)]
pub(crate) mod explorer_tests;
#[cfg(test)]
mod inspect_tests;
