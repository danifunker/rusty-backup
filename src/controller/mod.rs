//! Controllers: one per task, screen or dialog, shared by every view (CONTRIBUTING.md § Architecture policy (MVC)).
//!
//! A view (egui, the TUI, the CLI, and native shells later) owns a controller,
//! sends it user intents as `Action`s, draws from its read accessors, and
//! carries out the [`Effect`]s it returns (file pickers, prompts, confirmations,
//! the clipboard), replying with another `Action` where the effect has an answer.
//! Slow work goes through [`crate::model::job`], so a view can only show a
//! wait state through a job snapshot.
//!
//! Rules:
//! - Nothing toolkit-specific here: no egui, ratatui, rfd, AppKit, GTK or Win32.
//!   `scripts/check-boundaries.sh` enforces it.
//! - `Action` and [`Effect`] enums are exhaustive and views `match` them with no
//!   `_` arm, so a new variant fails to compile in every view that ignores it.
//! - Every actionable element has a stable widget id (the `ids` modules), which
//!   each view sets as its accessibility identifier for UI tests.
//! - Shortcuts are data in [`keys`], never hard-coded in a view.
//! - Rust 1.73 and mrustc compile this, like the engine.
//! - Tier A tests replay `tests/ui_scenarios/*.toml` against controllers (`tests/ui_scenarios.rs`).

pub mod app;
pub mod commander;
pub mod keys;
pub mod log;

/// The shape every controller shares.
pub trait Controller {
    /// User intents, such as `OpenSource(path)`, `StartBackup` or `Cancel`.
    type Action;

    fn apply(&mut self, action: Self::Action) -> Vec<Effect>;

    /// Called by the shell when woken or about to draw: drains finished jobs and queued input.
    fn tick(&mut self) -> Vec<Effect>;
}

/// Something only the shell can do; it reports any answer back as an `Action`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    PickFile(PickSpec),
    PickFolder(PickSpec),
    Confirm(ConfirmSpec),
    AskPassword(PasswordSpec),
    /// A transient message for the user (status line, toast or alert).
    Message(String),
    OpenUrl(String),
    CopyToClipboard(String),
}

/// A file or folder picker; `purpose` says which `Action` the answer belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PickSpec {
    pub purpose: &'static str,
    pub title: String,
    /// `(name, extensions)` pairs; empty means any file.
    pub filters: Vec<(String, Vec<String>)>,
    pub save: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfirmSpec {
    pub purpose: &'static str,
    pub title: String,
    pub message: String,
    pub confirm_label: String,
    pub destructive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PasswordSpec {
    pub purpose: &'static str,
    pub prompt: String,
}
