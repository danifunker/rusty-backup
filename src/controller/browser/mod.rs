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
    purpose, validate_entry_name, ApplyPreflight, ApplyReport, BrowserPaneAction,
    BrowserPaneController, FailedEdit, NoRecent, OnConflict, PaneServices, RecentStore, SortColumn,
    SourceKind, StageOutcome, VolumeInfo, MAX_CONSECUTIVE_FAILURES,
};
pub use profile::{PaneProfile, Sources};
pub use rows::{DisplayRow, RowKind};
