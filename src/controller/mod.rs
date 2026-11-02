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

pub mod alto_resize;
pub mod app;
pub mod backup;
pub mod bulk_convert;
pub mod browser;
pub mod commander;
pub mod disk_write;
pub mod expand_hfs;
pub mod floppy_convert;
pub mod inspect;
pub mod keys;
pub mod log;
#[cfg(feature = "optical")]
pub mod optical;
pub mod partition_edit;
#[cfg(feature = "remote")]
pub mod remote_pick;
pub mod resize;
pub mod restore;
#[cfg(any(feature = "gui", feature = "tui-update", feature = "tui"))]
pub mod settings;

/// A job's outcome once it ended: its result, or why it failed or was cancelled.
pub(crate) fn job_outcome<R: Send + 'static>(
    job: &crate::model::job::Job<R>,
) -> Option<Result<R, String>> {
    use crate::model::job::Status;
    match job.snapshot().status {
        Status::Running => None,
        Status::Done => Some(job.take_result().ok_or_else(|| "no result".to_string())),
        Status::Failed(e) => Some(Err(e)),
        Status::Cancelled => Some(Err("cancelled".to_string())),
    }
}

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
    /// A one-line text prompt; answer with the controller's `Answered`-style action.
    AskText(TextSpec),
    /// A transient message for the user (status line, toast or alert).
    Message(String),
    OpenUrl(String),
    CopyToClipboard(String),
    /// Leave the screen that asked (Commander's Close); the shell drops its controller.
    CloseScreen,
    /// Quit the application; the controller's unsaved-edits guard has passed.
    Quit,
}

/// A file or folder picker; `purpose` says which `Action` the answer belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PickSpec {
    pub purpose: &'static str,
    pub title: String,
    /// `(name, extensions)` pairs; empty means any file.
    pub filters: Vec<(String, Vec<String>)>,
    pub save: bool,
    /// The name a save dialog suggests; empty for none.
    pub file_name: String,
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

/// A one-line text prompt whose rule the controller defines and the view runs as the user types.
///
/// The validator is a plain fn pointer so the spec stays comparable; the context it needs
/// (names already taken, characters it refuses) travels in the spec as data. An `Err` with an
/// empty message disables OK without showing anything (an untouched or empty field).
#[derive(Debug, Clone)]
pub struct TextSpec {
    pub purpose: &'static str,
    pub title: String,
    pub prompt: String,
    pub initial: String,
    pub ok_label: String,
    /// The answer that counts as no change (a rename's current name).
    pub unchanged: String,
    /// Names the answer may not take, for the validator.
    pub taken: Vec<String>,
    /// Compare with `taken` ignoring case.
    pub fold_case: bool,
    /// Characters the answer may not contain.
    pub forbidden: String,
    /// Why the controller refused the last answer; shown until the user edits.
    pub error: Option<String>,
    pub validator: fn(&TextSpec, &str) -> Result<(), String>,
}

// Spelled out because comparing fn pointers directly trips rustc's unpredictable-comparison lint.
impl PartialEq for TextSpec {
    fn eq(&self, o: &Self) -> bool {
        self.purpose == o.purpose
            && self.title == o.title
            && self.prompt == o.prompt
            && self.initial == o.initial
            && self.ok_label == o.ok_label
            && self.unchanged == o.unchanged
            && self.taken == o.taken
            && self.fold_case == o.fold_case
            && self.forbidden == o.forbidden
            && self.error == o.error
            && self.validator as usize == o.validator as usize
    }
}

impl Eq for TextSpec {}

impl TextSpec {
    /// Run the controller's rule on `text`.
    pub fn validate(&self, text: &str) -> Result<(), String> {
        (self.validator)(self, text)
    }
}
