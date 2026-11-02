//! Commander: the two-pane file manager, as view-models every shell renders.
//!
//! [`pane::CommanderPaneController`] holds one pane's state (listing, staged-edit queue,
//! wrapper tree); [`rows`] turns it into the rows a shell draws. The egui
//! Commander (`src/gui/commander`) is the first shell on these; the TUI follows.
//! Phase 2 of `docs/native_frontends_plan.md` moves the rest of the pane and the
//! two-pane orchestration here slice by slice.

pub mod ids;
pub mod pane;
pub mod rows;

pub use pane::CommanderPaneController;
pub use rows::{DisplayRow, RowKind};
