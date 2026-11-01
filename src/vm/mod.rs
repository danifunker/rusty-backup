//! View-models: one per screen or dialog, rendered by every shell (`docs/native_frontends_plan.md` §4.2).
//!
//! A shell (egui today; the TUI and native shells later) owns a view-model,
//! sends it user intents as `Action`s, draws from its read accessors, and
//! carries out the [`Effect`]s it returns (file pickers, confirmations, the
//! clipboard), replying with another `Action` where the effect has an answer.
//! Slow work goes through [`crate::model::job`], so a shell can only show a
//! wait state through a job snapshot.
//!
//! Rules:
//! - Nothing toolkit-specific here: no egui, ratatui, rfd, AppKit, GTK or Win32.
//!   `scripts/check-boundaries.sh` enforces it.
//! - `Action` and [`Effect`] enums are exhaustive and shells `match` them with no
//!   `_` arm, so a new variant fails to compile in every shell that ignores it.
//! - Every actionable element has a stable widget id (the `ids` modules), which
//!   each shell sets as its accessibility identifier for UI tests.
//! - Rust 1.73 and mrustc compile this, like the engine.

pub mod app;
pub mod log;

/// The shape every view-model shares.
pub trait ViewModel {
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
