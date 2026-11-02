//! Changing what a pane shows: staged edits, Apply, immediate host writes and wrapper expansion.
//!
//! An image pane stages edits on its [`EditQueue`](crate::model::edit_queue::EditQueue).
//! Apply runs two jobs: a preflight that checks the queue against a fresh read-only open,
//! then (straight away when clean, or after the user answers the review) the write. What
//! landed leaves the queue, the rest stays staged, and a run that did not land everything
//! leaves a report. The partition is reopened afterwards because its cached catalog
//! predates the write. A remote image takes the same flow through the daemon, copies
//! and new folders only (`remote.rs`). A host pane writes immediately instead: delete (after a
//! confirmation), rename and new folder run as jobs through [`crate::model::host_ops`].
//! Rename and new folder ask for the name with [`Effect::AskText`]; its validator
//! ([`validate_entry_name`]) runs in the view as the user types and again here on the
//! answer, followed by the filesystem's own name rules.

use std::path::PathBuf;

use disk_core::entry::FileEntry;

use super::{CommanderPaneAction, CommanderPaneController};
use crate::controller::commander::pane::purpose;
use crate::controller::{ConfirmSpec, Effect, TextSpec};
use crate::fs::replace::OnConflict;
use crate::model::browse_session::BrowseSession;
use crate::model::commander_descend::classify_entry;
use crate::model::commander_ops::{
    self, ApplyOutcome, ApplyPreflight, FailedEdit, StageSource, WrapperExtract, WrapperOpenPlan,
};
use crate::model::edit_queue::StagedEdit;
use crate::model::host_ops::{self, DeleteOutcome};
use crate::model::job::{Job, JobCtx, Resource, Snapshot, Status};
use crate::model::stage_names::{legalize_staged_names, LegalizeReport, NameChange};
use crate::model::wrapper_tree::PreparedMount;

/// A finished apply that did not land everything; the rest stays staged.
#[derive(Debug, Clone)]
pub struct ApplyReport {
    pub total: usize,
    pub applied: usize,
    pub failed: Vec<FailedEdit>,
    /// Edits never tried because the run stopped early.
    pub not_tried: usize,
    pub fatal: Option<String>,
}

/// Where an apply's preflight reads and its write lands.
enum ApplyTarget {
    Session(BrowseSession),
    #[cfg(feature = "remote")]
    Remote(super::remote::RemoteImage),
}

impl ApplyTarget {
    fn preflight(&self, edits: &[StagedEdit]) -> anyhow::Result<ApplyPreflight> {
        match self {
            ApplyTarget::Session(s) => commander_ops::preflight_apply(s, edits),
            #[cfg(feature = "remote")]
            ApplyTarget::Remote(r) => {
                commander_ops::preflight_remote_apply(&r.addr, &r.path, r.partition.clone(), edits)
            }
        }
    }

    fn write(&self, edits: &[StagedEdit], ctx: &JobCtx) -> anyhow::Result<ApplyOutcome> {
        match self {
            ApplyTarget::Session(s) => commander_ops::apply_in_job(s, edits, ctx),
            #[cfg(feature = "remote")]
            ApplyTarget::Remote(r) => commander_ops::remote_apply_in_job(
                &r.addr,
                &r.path,
                r.partition.clone(),
                edits,
                ctx,
            ),
        }
    }
}

/// What one [`CommanderPaneController::stage_edits`] batch did.
#[derive(Debug, Clone)]
pub struct StageOutcome {
    pub staged: usize,
    pub queue_start: usize,
    pub fs_type: String,
    pub renamed: Vec<NameChange>,
    pub dropped: Vec<(String, String)>,
}

enum ApplyPhase {
    Idle,
    Checking(Job<ApplyPreflight>),
    Review(ApplyPreflight),
    Writing {
        job: Job<ApplyOutcome>,
        total: usize,
    },
}

enum PromptTarget {
    Rename(Box<FileEntry>),
    NewFolder,
}

struct Prompt {
    spec: TextSpec,
    target: PromptTarget,
}

enum HostDone {
    Deleted(DeleteOutcome),
    Renamed { from: String, to: String },
    Created(String),
}

struct HostJob {
    /// The operation's name for a failure message, e.g. "rename".
    what: &'static str,
    job: Job<HostDone>,
}

struct WrapperOpening {
    node_id: String,
    name: String,
    job: Job<PreparedMount>,
}

/// The editing half of a pane's state.
pub(super) struct EditState {
    show_edits: bool,
    prompt: Option<Prompt>,
    host_delete: Option<Vec<PathBuf>>,
    apply: ApplyPhase,
    report: Option<ApplyReport>,
    host_job: Option<HostJob>,
    wrapper: Option<WrapperOpening>,
    log: Vec<String>,
}

impl EditState {
    pub(super) fn new() -> Self {
        EditState {
            show_edits: false,
            prompt: None,
            host_delete: None,
            apply: ApplyPhase::Idle,
            report: None,
            host_job: None,
            wrapper: None,
            log: Vec::new(),
        }
    }

    /// Forget everything tied to the old source (the session log stays).
    pub(super) fn reset(&mut self) {
        let log = std::mem::take(&mut self.log);
        *self = EditState::new();
        self.log = log;
    }

    /// A partition is about to (re)open: what was in flight no longer applies; a report stays.
    pub(super) fn drop_in_flight(&mut self) {
        self.prompt = None;
        self.host_delete = None;
        self.apply = ApplyPhase::Idle;
        self.cancel_wrapper();
    }

    pub(super) fn cancel_wrapper(&mut self) {
        if let Some(w) = self.wrapper.take() {
            w.job.cancel();
        }
    }
}

/// The rule for a new entry name: not empty or unchanged, no refused character, not taken.
pub fn validate_entry_name(spec: &TextSpec, text: &str) -> Result<(), String> {
    let name = text.trim();
    if name.is_empty() || name == spec.unchanged {
        return Err(String::new());
    }
    if name == "." || name == ".." {
        return Err("That name is reserved.".into());
    }
    if let Some(c) = name.chars().find(|c| spec.forbidden.contains(*c)) {
        return Err(format!("A name cannot contain '{}'.", c.escape_default()));
    }
    let same = |t: &String| {
        if spec.fold_case {
            t.to_lowercase() == name.to_lowercase()
        } else {
            t == name
        }
    };
    if spec.taken.iter().any(same) {
        return Err(format!("'{name}' already exists here."));
    }
    Ok(())
}

/// Why a remote pane refuses an edit the daemon's write path does not carry.
const REMOTE_COPIES_ONLY: &str =
    "remote panes take copied files and new folders only; this edit is not supported over the wire yet.";

fn msg(text: impl Into<String>) -> Vec<Effect> {
    vec![Effect::Message(text.into())]
}

impl CommanderPaneController {
    // --- read accessors ---------------------------------------------------

    /// True while the "Pending edits" list is open (it closes itself once the queue empties).
    pub fn edits_shown(&self) -> bool {
        self.ed.show_edits && !self.queue.is_empty()
    }

    /// The staged edits in plain words, in apply order.
    pub fn edit_lines(&self) -> Vec<String> {
        self.queue.describe()
    }

    /// The preflight findings waiting for `ApplyReviewed` or `DismissReview`.
    pub fn apply_review(&self) -> Option<&ApplyPreflight> {
        match &self.ed.apply {
            ApplyPhase::Review(p) => Some(p),
            _ => None,
        }
    }

    /// The last apply's report, until `DismissReport`.
    pub fn apply_report(&self) -> Option<&ApplyReport> {
        self.ed.report.as_ref()
    }

    /// The Apply preflight in progress.
    pub fn checking(&self) -> Option<Snapshot> {
        match &self.ed.apply {
            ApplyPhase::Checking(job) => Some(job.snapshot()),
            _ => None,
        }
    }

    /// The apply write in progress.
    pub fn applying(&self) -> Option<Snapshot> {
        match &self.ed.apply {
            ApplyPhase::Writing { job, .. } => Some(job.snapshot()),
            _ => None,
        }
    }

    /// A host delete, rename or new folder in progress.
    pub fn host_writing(&self) -> Option<Snapshot> {
        self.ed.host_job.as_ref().map(|h| h.job.snapshot())
    }

    /// The wrapper being expanded off the UI thread: its name and job.
    pub fn wrapper_opening(&self) -> Option<(&str, Snapshot)> {
        let w = self.ed.wrapper.as_ref()?;
        Some((w.name.as_str(), w.job.snapshot()))
    }

    /// True while an open, preflight, apply or host write runs, so edits wait.
    pub fn is_busy(&self) -> bool {
        self.is_opening()
            || !matches!(self.ed.apply, ApplyPhase::Idle | ApplyPhase::Review(_))
            || self.ed.host_job.is_some()
    }

    /// True for a pane whose edits are staged (an image volume, not a host folder or read-only source).
    pub fn stages_edits(&self) -> bool {
        !self.listing.is_host() && !self.is_backup() && !self.is_archive() && !self.is_remote_host()
    }

    pub fn can_apply(&self) -> bool {
        self.stages_edits() && !self.queue.is_empty() && !self.is_busy()
    }

    pub fn can_discard(&self) -> bool {
        self.can_apply()
    }

    pub fn can_new_folder(&self) -> bool {
        self.can_edit_names() && !self.is_remote_host()
    }

    /// True when a copy can land here (a host folder writes now, an image stages).
    pub fn can_receive(&self) -> bool {
        self.listing.is_loaded() && !self.is_busy() && !self.is_backup() && !self.is_archive()
    }

    /// True for read-only media (archive, optical disc, backup), for the "Read-only" badge.
    pub fn is_read_only(&self) -> bool {
        self.listing.is_loaded()
            && !self.listing.is_host()
            && (self.is_archive() || self.is_optical() || self.is_backup())
    }

    /// Why a copy into this pane is refused, for the copy button's hover.
    pub fn readonly_reason(&self) -> Option<&'static str> {
        if self.is_archive() {
            Some("the archive is read-only")
        } else if self.is_backup() {
            Some("the backup is read-only")
        } else {
            None
        }
    }

    /// The mount key and entries of the wrapper-tree selection (one mount: the first row's).
    pub fn tree_selection(&self) -> Option<(String, Vec<FileEntry>)> {
        let first = self.tree_selected.first()?;
        let mount = self.tree_index.get(first)?.mount.clone();
        let entries = self
            .tree_selected
            .iter()
            .filter_map(|id| self.tree_index.get(id))
            .filter(|r| r.mount == mount)
            .map(|r| r.entry.clone())
            .collect();
        Some((mount, entries))
    }

    /// How a worker reopens the selection's source: its wrapper mount, else the pane's base source.
    pub fn copy_stage_source(&self) -> Option<StageSource> {
        if let Some((mount_key, _)) = self.tree_selection() {
            return self
                .wrapper_tree
                .mount_reopen(&mount_key)
                .map(StageSource::Reopen);
        }
        if self.tree_active() {
            return None;
        }
        self.base_stage_source()
    }

    /// How a worker reopens the pane's base source (a remote image, a session, an archive or disc).
    pub fn base_stage_source(&self) -> Option<StageSource> {
        self.remote_stage_source()
            .or_else(|| self.local_stage_source())
    }

    /// Lines for the session log since the last call (applies, host writes, failures).
    pub fn take_log(&mut self) -> Vec<String> {
        std::mem::take(&mut self.ed.log)
    }

    // --- entry points for the two-pane copy (slice 6) ------------------------

    /// Push a copy batch onto the queue, renaming names this volume cannot hold.
    pub fn stage_edits(&mut self, mut edits: Vec<StagedEdit>) -> StageOutcome {
        let queue_start = self.queue.len();
        let cwd_path = self.listing.cwd_path().to_string();
        let cwd_names: Vec<String> = self
            .listing
            .entries()
            .iter()
            .map(|e| e.name.clone())
            .collect();
        let queue = &self.queue;
        let mut existing = |path: &str| {
            let mut names: Vec<String> = queue
                .pending_adds_for(path)
                .into_iter()
                .map(|e| e.name)
                .collect();
            if path == cwd_path {
                names.extend(cwd_names.iter().cloned());
            }
            names
        };
        // A remote image has no local fs here; the daemon reports bad names at apply.
        let (report, fs_type) = match self.listing.fs_mut() {
            Some(fs) => {
                let fold = fs.case_insensitive_lookup();
                let fs_type = fs.fs_type().to_string();
                let validate = |n: &str| fs.validate_name(n);
                (
                    legalize_staged_names(&mut edits, &validate, fold, &mut existing),
                    fs_type,
                )
            }
            None => (LegalizeReport::default(), String::new()),
        };
        let staged = edits.len();
        for e in edits {
            self.queue.push(e);
        }
        StageOutcome {
            staged,
            queue_start,
            fs_type,
            renamed: report.renamed,
            dropped: report.dropped,
        }
    }

    /// Drop everything staged since `queue_start`; false while an apply holds the queue.
    pub fn unstage_since(&mut self, queue_start: usize) -> bool {
        if self.is_busy() {
            return false;
        }
        self.queue.truncate(queue_start);
        true
    }

    fn report_apply_failure(&mut self, fatal: String) {
        self.ed.report = Some(ApplyReport {
            total: self.queue.len(),
            applied: 0,
            failed: Vec::new(),
            not_tried: 0,
            fatal: Some(fatal),
        });
    }

    // --- actions ----------------------------------------------------------

    /// Handles an editing action, or hands it back for the source actions.
    pub(super) fn apply_edit(
        &mut self,
        action: CommanderPaneAction,
    ) -> Result<Vec<Effect>, CommanderPaneAction> {
        use CommanderPaneAction as A;
        let fx = match action {
            A::Delete => self.delete(),
            A::CancelRename(name) => {
                let path = self.entry_named(&name).map(|e| e.path);
                match path {
                    Some(p) if self.queue.remove_pending_rename(&p) => {
                        msg("cancelled staged rename.")
                    }
                    _ => Vec::new(),
                }
            }
            A::RequestRename(name) => self.request_rename(&name),
            A::RenameAtCursor => self.rename_at_cursor(),
            A::RequestNewFolder => self.request_new_folder(),
            A::Answered { purpose, text } => self.answered(purpose, text),
            A::Discard => self.request_discard(),
            A::Unstage(i) => {
                if !self.is_busy() && i < self.queue.len() {
                    self.queue.remove_indices(&[i]);
                }
                Vec::new()
            }
            A::ShowEdits(on) => {
                self.ed.show_edits = on;
                Vec::new()
            }
            A::Apply => self.start_check(),
            A::ApplyReviewed(on) => self.reviewed(on),
            A::DismissReview => {
                if !matches!(self.ed.apply, ApplyPhase::Review(_)) {
                    return Ok(Vec::new());
                }
                self.ed.apply = ApplyPhase::Idle;
                msg("apply cancelled; nothing was written.")
            }
            A::DismissReport => {
                self.ed.report = None;
                Vec::new()
            }
            A::Confirmed { purpose, yes } if purpose == purpose::HOST_DELETE => {
                match self.ed.host_delete.take() {
                    Some(paths) if yes => self.spawn_host_delete(paths),
                    _ => Vec::new(),
                }
            }
            A::Confirmed { purpose, yes } if purpose == purpose::DISCARD_STAGED => {
                if !yes || self.is_busy() {
                    return Ok(Vec::new());
                }
                self.queue.clear();
                msg("discarded staged edits.")
            }
            other => return Err(other),
        };
        Ok(fx)
    }

    fn entry_named(&self, name: &str) -> Option<FileEntry> {
        let entries = self.listing.entries();
        entries.iter().find(|e| e.name == name).cloned()
    }

    fn delete(&mut self) -> Vec<Effect> {
        if self.tree_active() {
            return msg("wrapper contents are read-only (copy out, don't edit).");
        }
        if self.is_backup() {
            return msg("backup panes are read-only.");
        }
        if self.is_archive() {
            return msg("archive panes are read-only.");
        }
        if self.is_remote() {
            return msg(REMOTE_COPIES_ONLY);
        }
        if self.is_busy() {
            return msg("wait for the current operation to finish.");
        }
        let names: Vec<String> = self.listing.selection().to_vec();
        if names.is_empty() {
            return msg("nothing selected to delete.");
        }
        if !self.listing.is_host() {
            self.toggle_delete(&names);
            return msg(format!("toggled delete on {} item(s).", names.len()));
        }
        let paths: Vec<PathBuf> = self
            .listing
            .entries()
            .iter()
            .filter(|e| names.contains(&e.name))
            .map(|e| PathBuf::from(&e.path))
            .collect();
        let n = paths.len();
        self.ed.host_delete = Some(paths);
        vec![Effect::Confirm(ConfirmSpec {
            purpose: purpose::HOST_DELETE,
            title: "Delete from host folder?".into(),
            message: format!(
                "Permanently delete {n} item(s) from the host folder?\n\
                 This is immediate and cannot be undone."
            ),
            confirm_label: "Delete".into(),
            destructive: true,
        })]
    }

    /// Stage a delete, undelete a pending one, or unstage a pending copy / new folder.
    fn toggle_delete(&mut self, names: &[String]) {
        let Some(cwd) = self.listing.cwd().cloned() else {
            return;
        };
        let pending_adds = self.queue.pending_adds_for(&cwd.path);
        for name in names {
            if let Some(add) = pending_adds.iter().find(|e| &e.name == name) {
                if add.is_directory() {
                    self.queue.remove_pending_subtree(&add.path);
                } else {
                    self.queue.remove_pending_add(&add.path);
                }
                continue;
            }
            let Some(entry) = self.entry_named(name) else {
                continue;
            };
            if self.queue.is_pending_delete(&entry.path) {
                self.queue.remove_pending_delete(&entry.path);
                continue;
            }
            // A delete supersedes a staged rename, else apply renames it and then misses the old name.
            self.queue.remove_pending_rename(&entry.path);
            let parent = cwd.clone();
            self.queue.push(if entry.is_directory() {
                StagedEdit::DeleteRecursive { parent, entry }
            } else {
                StagedEdit::DeleteEntry { parent, entry }
            });
        }
    }

    // --- rename and new folder ---------------------------------------------

    fn can_edit_names(&self) -> bool {
        self.listing.is_loaded() && !self.is_busy() && !self.is_backup() && !self.is_archive()
    }

    fn rename_at_cursor(&mut self) -> Vec<Effect> {
        let Some(i) = self.cursor_index() else {
            return Vec::new();
        };
        let row = self.rows()[i].clone();
        if row.is_tree || row.is_parent() || row.kind != super::RowKind::Normal {
            return Vec::new();
        }
        self.context_select(&row.name);
        self.request_rename(&row.name)
    }

    /// A text spec for a name in the current folder; `except` is left out of the taken names.
    fn name_spec(&mut self, purpose: &'static str, except: Option<&str>) -> TextSpec {
        let cwd = self.listing.cwd_path().to_string();
        let taken: Vec<String> = self
            .listing
            .entries()
            .iter()
            .map(|e| e.name.clone())
            .chain(
                self.queue
                    .pending_adds_for(&cwd)
                    .into_iter()
                    .map(|e| e.name),
            )
            .filter(|n| Some(n.as_str()) != except)
            .collect();
        let (fold_case, forbidden) = if self.listing.is_host() {
            let sep = if cfg!(windows) { "/\\\0" } else { "/\0" };
            (cfg!(any(windows, target_os = "macos")), sep.to_string())
        } else {
            let fold = self
                .listing
                .fs_mut()
                .is_some_and(|fs| fs.case_insensitive_lookup());
            (fold, String::new())
        };
        TextSpec {
            purpose,
            title: String::new(),
            prompt: String::new(),
            initial: String::new(),
            ok_label: String::new(),
            unchanged: String::new(),
            taken,
            fold_case,
            forbidden,
            error: None,
            validator: validate_entry_name,
        }
    }

    fn request_rename(&mut self, name: &str) -> Vec<Effect> {
        if self.is_remote() {
            return msg(REMOTE_COPIES_ONLY);
        }
        if !self.can_edit_names() {
            return Vec::new();
        }
        let Some(entry) = self.entry_named(name) else {
            return Vec::new();
        };
        let mut spec = self.name_spec(purpose::RENAME, Some(name));
        spec.title = "Rename".into();
        spec.prompt = format!("Rename \"{name}\" to:");
        spec.initial = name.to_string();
        spec.unchanged = name.to_string();
        spec.ok_label = "Rename".into();
        self.ask(spec, PromptTarget::Rename(Box::new(entry)))
    }

    fn request_new_folder(&mut self) -> Vec<Effect> {
        if !self.can_new_folder() {
            return Vec::new();
        }
        let mut spec = self.name_spec(purpose::NEW_FOLDER, None);
        spec.title = "New Folder".into();
        spec.prompt = "Folder name:".into();
        spec.ok_label = "Create".into();
        self.ask(spec, PromptTarget::NewFolder)
    }

    fn ask(&mut self, spec: TextSpec, target: PromptTarget) -> Vec<Effect> {
        let fx = vec![Effect::AskText(spec.clone())];
        self.ed.prompt = Some(Prompt { spec, target });
        fx
    }

    fn answered(&mut self, purpose: &'static str, text: Option<String>) -> Vec<Effect> {
        let Some(prompt) = self.ed.prompt.take() else {
            return Vec::new();
        };
        if prompt.spec.purpose != purpose {
            self.ed.prompt = Some(prompt);
            return Vec::new();
        }
        let Some(text) = text else {
            return Vec::new();
        };
        let name = text.trim().to_string();
        let mut why = prompt.spec.validate(&name).err();
        if why.is_none() {
            let fs_rule = self.listing.fs_mut().map(|fs| fs.validate_name(&name));
            why = fs_rule.and_then(|r| r.err()).map(|e| e.to_string());
        }
        if let Some(why) = why {
            let mut spec = prompt.spec;
            spec.initial = name;
            spec.error = Some(if why.is_empty() {
                "Enter a new name.".into()
            } else {
                why
            });
            return self.ask(spec, prompt.target);
        }
        match prompt.target {
            PromptTarget::Rename(entry) => self.rename(*entry, name),
            PromptTarget::NewFolder => self.new_folder(name),
        }
    }

    fn rename(&mut self, entry: FileEntry, new_name: String) -> Vec<Effect> {
        if self.listing.is_host() {
            let from = entry.name.clone();
            let old = PathBuf::from(&entry.path);
            let to = new_name.clone();
            return self.spawn_host("rename", format!("Renaming {from}"), move |_| {
                host_ops::rename_entry(&old, &to)?;
                Ok(HostDone::Renamed { from, to })
            });
        }
        let Some(cwd) = self.listing.cwd().cloned() else {
            return msg("no directory to rename in.");
        };
        self.queue.remove_pending_rename(&entry.path);
        self.queue.push(StagedEdit::Rename {
            parent: cwd,
            entry,
            new_name: new_name.clone(),
        });
        msg(format!("staged rename to '{new_name}'. Apply to write."))
    }

    fn new_folder(&mut self, name: String) -> Vec<Effect> {
        if self.listing.is_host() {
            let parent = PathBuf::from(self.listing.cwd_path());
            return self.spawn_host("create folder", format!("Creating {name}"), move |_| {
                host_ops::make_dir(&parent, &name)?;
                Ok(HostDone::Created(name))
            });
        }
        let Some(cwd) = self.listing.cwd().cloned() else {
            return msg("no directory open.");
        };
        self.queue.push(StagedEdit::CreateDirectory {
            parent: cwd,
            name: name.clone(),
            qnx2_attrs: None,
        });
        msg(format!("staged new folder '{name}'. Apply to write."))
    }

    // --- host writes --------------------------------------------------------

    fn spawn_host_delete(&mut self, paths: Vec<PathBuf>) -> Vec<Effect> {
        let label = format!("Deleting {} item(s)", paths.len());
        self.spawn_host("delete", label, move |ctx| {
            let total = paths.len() as u64;
            let out = host_ops::delete_paths(&paths, |i, p| {
                ctx.set_phase(format!("Deleting {}", p.display()));
                ctx.set_progress(crate::model::job::Progress::Determinate {
                    done: i as u64,
                    total,
                    unit: crate::model::job::Unit::Items,
                });
            });
            Ok(HostDone::Deleted(out))
        })
    }

    fn spawn_host(
        &mut self,
        what: &'static str,
        label: String,
        body: impl FnOnce(&crate::model::job::JobCtx) -> anyhow::Result<HostDone> + Send + 'static,
    ) -> Vec<Effect> {
        if self.ed.host_job.is_some() {
            return msg("wait for the current host write to finish.");
        }
        let folder = Resource::Folder(PathBuf::from(self.listing.cwd_path()));
        let (jobs, notify) = self.job_services();
        match jobs.spawn(label, vec![folder], notify, move |ctx| {
            ctx.set_can_cancel(false);
            body(ctx)
        }) {
            Ok(job) => {
                self.ed.host_job = Some(HostJob { what, job });
                Vec::new()
            }
            Err(busy) => msg(format!("{what} refused: {busy}")),
        }
    }

    fn poll_host(&mut self) -> Vec<Effect> {
        let finished = self
            .ed
            .host_job
            .as_ref()
            .is_some_and(|h| !h.job.is_running());
        if !finished {
            return Vec::new();
        }
        let Some(h) = self.ed.host_job.take() else {
            return Vec::new();
        };
        let text = match (h.job.snapshot().status, h.job.take_result()) {
            (Status::Done, Some(done)) => {
                let text = self.host_done(done);
                if let Err(e) = self.listing.reload() {
                    self.error = Some(format!("cannot refresh: {e}"));
                }
                text
            }
            (Status::Failed(e), _) => format!("{} failed: {e}", h.what),
            _ => format!("{} cancelled.", h.what),
        };
        msg(text)
    }

    fn host_done(&mut self, done: HostDone) -> String {
        let text = match done {
            HostDone::Deleted(out) => {
                for (path, why) in &out.failed {
                    self.ed.log.push(format!("could not delete {path}: {why}"));
                }
                if out.failed.is_empty() {
                    format!("deleted {} item(s) from the host folder.", out.removed)
                } else {
                    format!(
                        "deleted {} item(s); {} could not be removed.",
                        out.removed,
                        out.failed.len()
                    )
                }
            }
            HostDone::Renamed { from, to } => format!("renamed '{from}' to '{to}'."),
            HostDone::Created(name) => format!("created folder '{name}'."),
        };
        self.ed.log.push(text.clone());
        text
    }

    // --- discard and apply ---------------------------------------------------

    fn request_discard(&mut self) -> Vec<Effect> {
        if self.queue.is_empty() || self.is_busy() {
            return Vec::new();
        }
        let n = self.queue.len();
        vec![Effect::Confirm(ConfirmSpec {
            purpose: purpose::DISCARD_STAGED,
            title: "Discard staged edits?".into(),
            message: format!("Drop the {n} staged edit(s) on this pane? Nothing has been written."),
            confirm_label: "Discard".into(),
            destructive: true,
        })]
    }

    /// The source the apply jobs hold while they run.
    fn apply_resources(&self) -> Vec<Resource> {
        #[cfg(feature = "remote")]
        if let Some(image) = self.remote_image() {
            return vec![image.resource()];
        }
        self.source_path()
            .map(|p| vec![Resource::Image(p.to_path_buf())])
            .unwrap_or_default()
    }

    /// Where Apply writes: the open session, or the remote image on a fresh connection.
    fn apply_target(&self) -> Option<ApplyTarget> {
        #[cfg(feature = "remote")]
        if let Some(image) = self.remote_image() {
            return Some(ApplyTarget::Remote(image));
        }
        self.session().cloned().map(ApplyTarget::Session)
    }

    fn start_check(&mut self) -> Vec<Effect> {
        if !self.can_apply() {
            return Vec::new();
        }
        let Some(target) = self.apply_target() else {
            return msg("no source to apply to.");
        };
        let copies_only = |e: &StagedEdit| {
            matches!(
                e,
                StagedEdit::AddFile { .. } | StagedEdit::CreateDirectory { .. }
            )
        };
        if self.is_remote() && !self.queue.iter().all(copies_only) {
            return msg(REMOTE_COPIES_ONLY);
        }
        let edits: Vec<StagedEdit> = self.queue.iter().cloned().collect();
        let n = edits.len();
        let resources = self.apply_resources();
        let (jobs, notify) = self.job_services();
        let spawned = jobs.spawn("Checking staged edits", resources, notify, move |ctx| {
            ctx.set_phase("Checking staged edits...");
            target.preflight(&edits)
        });
        match spawned {
            Ok(job) => {
                self.ed.report = None;
                self.ed.apply = ApplyPhase::Checking(job);
                msg(format!("checking {n} staged edit(s)..."))
            }
            Err(busy) => msg(format!("cannot apply: {busy}")),
        }
    }

    fn reviewed(&mut self, on: OnConflict) -> Vec<Effect> {
        let ApplyPhase::Review(p) = std::mem::replace(&mut self.ed.apply, ApplyPhase::Idle) else {
            return Vec::new();
        };
        if !p.scan.conflicts.is_empty() {
            // The daemon cannot skip an existing file, so a remote skip leaves those copies out.
            if self.is_remote() && on == OnConflict::Skip {
                for (path, _) in &p.scan.conflicts {
                    self.queue.remove_pending_add(path);
                }
                if self.queue.is_empty() {
                    return msg("every staged copy already exists; nothing to apply.");
                }
            } else {
                self.queue.set_all_conflicts(on);
            }
        }
        self.start_write()
    }

    fn start_write(&mut self) -> Vec<Effect> {
        let Some(target) = self.apply_target() else {
            return msg("no source to apply to.");
        };
        let edits: Vec<StagedEdit> = self.queue.iter().cloned().collect();
        let total = edits.len();
        let resources = self.apply_resources();
        let (jobs, notify) = self.job_services();
        let spawned = jobs.spawn("Applying staged edits", resources, notify, move |ctx| {
            target.write(&edits, ctx)
        });
        match spawned {
            Ok(job) => {
                self.ed.apply = ApplyPhase::Writing { job, total };
                msg(format!("applying {total} edit(s)..."))
            }
            Err(busy) => msg(format!("cannot apply: {busy}")),
        }
    }

    fn poll_apply(&mut self) -> Vec<Effect> {
        let finished = match &self.ed.apply {
            ApplyPhase::Checking(job) => !job.is_running(),
            ApplyPhase::Writing { job, .. } => !job.is_running(),
            ApplyPhase::Idle | ApplyPhase::Review(_) => false,
        };
        if !finished {
            return Vec::new();
        }
        match std::mem::replace(&mut self.ed.apply, ApplyPhase::Idle) {
            ApplyPhase::Checking(job) => self.checked(job),
            ApplyPhase::Writing { job, total } => self.written(job, total),
            ApplyPhase::Idle | ApplyPhase::Review(_) => Vec::new(),
        }
    }

    fn checked(&mut self, job: Job<ApplyPreflight>) -> Vec<Effect> {
        match (job.snapshot().status, job.take_result()) {
            (Status::Done, Some(p)) if p.is_clean() => self.start_write(),
            (Status::Done, Some(p)) => {
                self.ed.apply = ApplyPhase::Review(p);
                msg("review the staged edits before applying.")
            }
            (Status::Failed(e), _) => {
                self.report_apply_failure(format!("Could not check the staged edits: {e}"));
                msg("could not check the staged edits.")
            }
            _ => msg("apply cancelled; nothing was written."),
        }
    }

    fn written(&mut self, job: Job<ApplyOutcome>, total: usize) -> Vec<Effect> {
        let (outcome, fatal) = match (job.snapshot().status, job.take_result()) {
            (Status::Done, Some(outcome)) => (outcome, None),
            (Status::Failed(e), _) => (ApplyOutcome::default(), Some(e)),
            _ => (ApplyOutcome::default(), Some("cancelled".to_string())),
        };
        self.queue.remove_indices(&outcome.applied_indices);
        let applied = outcome.applied();
        for f in &outcome.failed {
            self.ed
                .log
                .push(format!("failed: {}: {}", f.label, f.error));
        }
        let clean = fatal.is_none() && outcome.failed.is_empty();
        let text = if clean {
            format!("applied {applied} edit(s).")
        } else {
            format!(
                "applied {applied} of {total} edit(s); {} still staged.",
                self.queue.len()
            )
        };
        if !clean {
            self.ed.report = Some(ApplyReport {
                total,
                applied,
                not_tried: outcome.stopped_at.map_or(0, |at| total.saturating_sub(at)),
                failed: outcome.failed,
                fatal: fatal.map(|e| format!("Apply stopped: {e}")),
            });
        }
        self.ed.log.push(text.clone());
        let mut fx = msg(text);
        fx.extend(self.reopen_after_apply());
        fx
    }

    // --- wrappers -------------------------------------------------------------

    /// Expand a base-row wrapper as a job; only one opens at a time.
    pub(super) fn open_base_wrapper(&mut self, node_id: &str) {
        if self.ed.wrapper.is_some() {
            self.out.push(Effect::Message(
                "a wrapper is already opening; wait for it.".into(),
            ));
            return;
        }
        let Some(entry) = self
            .listing
            .entries()
            .iter()
            .find(|e| e.path == node_id)
            .cloned()
        else {
            return;
        };
        let Some(kind) = classify_entry(&entry) else {
            return;
        };
        let name = entry.name.clone();
        // A host file opens by path (bin/cue needs its sibling); an image wrapper is extracted on the job.
        let (plan, resources) = if self.listing.is_host() {
            let path = PathBuf::from(&entry.path);
            let res = vec![Resource::Image(path.clone())];
            (WrapperOpenPlan::HostPath(path), res)
        } else if let Some(base) = self.base_stage_source() {
            let ex = Box::new(WrapperExtract { base, entry });
            (WrapperOpenPlan::Extract(ex), self.apply_resources())
        } else {
            let Some(fs) = self.listing.fs_mut() else {
                return self.wrapper_failed(&name, "source is not open".into());
            };
            let mut buf = Vec::new();
            if let Err(e) = fs.write_file_to(&entry, &mut buf) {
                return self.wrapper_failed(&name, e.to_string());
            }
            (WrapperOpenPlan::Bytes(buf), Vec::new())
        };
        let (jobs, notify) = self.job_services();
        let file_name = name.clone();
        let spawned = jobs.spawn(format!("Opening {name}"), resources, notify, move |ctx| {
            commander_ops::prepare_wrapper(plan, kind, &file_name, ctx)
        });
        match spawned {
            Ok(job) => {
                self.out.push(Effect::Message(format!("opening {name}...")));
                self.ed.wrapper = Some(WrapperOpening {
                    node_id: node_id.to_string(),
                    name,
                    job,
                });
            }
            Err(busy) => self.wrapper_failed(&name, busy.to_string()),
        }
    }

    /// The full reason goes to the session log; the status line stays one short line.
    pub(super) fn wrapper_failed(&mut self, name: &str, detail: String) {
        self.ed
            .log
            .push(format!("couldn't open '{name}': {detail}"));
        self.out
            .push(Effect::Message(format!("couldn't open '{name}' (see Log)")));
    }

    fn poll_wrapper(&mut self) {
        let finished = self
            .ed
            .wrapper
            .as_ref()
            .is_some_and(|w| !w.job.is_running());
        if !finished {
            return;
        }
        let Some(w) = self.ed.wrapper.take() else {
            return;
        };
        match (w.job.snapshot().status, w.job.take_result()) {
            (Status::Done, Some(prepared)) => {
                let text = match self.wrapper_tree.install_prepared(&w.node_id, prepared) {
                    Some(note) => format!("{}: {note}", w.name),
                    None => format!("opened {}.", w.name),
                };
                self.out.push(Effect::Message(text));
            }
            (Status::Failed(e), _) => self.wrapper_failed(&w.name, e),
            _ => {}
        }
    }

    /// Installs finished edit jobs; called from `tick`.
    pub(super) fn poll_edits(&mut self) -> Vec<Effect> {
        let mut fx = self.poll_apply();
        fx.extend(self.poll_host());
        self.poll_wrapper();
        fx
    }
}

#[cfg(test)]
mod tests {
    use super::super::source::tests::{mbr_fat16, messages, names, settle};
    use super::*;
    use crate::controller::Controller;

    type A = CommanderPaneAction;

    fn image_pane(dir: &std::path::Path) -> CommanderPaneController {
        let mut vm = CommanderPaneController::new();
        vm.apply(A::OpenImage(mbr_fat16(dir)));
        settle(&mut vm);
        assert!(vm.listing.is_loaded(), "{:?}", vm.source_error());
        vm
    }

    fn files(vm: &CommanderPaneController) -> Vec<String> {
        let entries = vm.listing.entries().iter();
        entries
            .filter(|e| e.is_file())
            .map(|e| e.name.clone())
            .collect()
    }

    fn select(vm: &mut CommanderPaneController, name: &str) {
        vm.apply(A::Click {
            name: name.into(),
            ctrl: false,
            shift: false,
        });
    }

    fn one_text(fx: &[Effect]) -> TextSpec {
        match fx {
            [Effect::AskText(spec)] => spec.clone(),
            _ => panic!("expected one AskText, got {fx:?}"),
        }
    }

    fn one_confirm(fx: &[Effect]) -> ConfirmSpec {
        match fx {
            [Effect::Confirm(spec)] => spec.clone(),
            _ => panic!("expected one Confirm, got {fx:?}"),
        }
    }

    #[test]
    fn a_staged_delete_applies_through_preflight_and_write_jobs() {
        let dir = tempfile::tempdir().unwrap();
        let mut vm = image_pane(dir.path());
        let victim = files(&vm)[0].clone();
        select(&mut vm, &victim);
        let fx = vm.apply(A::Delete);
        assert_eq!(messages(&fx), ["toggled delete on 1 item(s)."]);
        assert_eq!(vm.queue.len(), 1);
        assert!(vm.can_apply());
        vm.apply(A::ShowEdits(true));
        assert!(vm.edits_shown());
        assert_eq!(vm.edit_lines().len(), 1);
        let fx = vm.apply(A::Apply);
        assert!(messages(&fx)[0].starts_with("checking 1"), "{fx:?}");
        assert!(vm.is_busy());
        let fx = settle(&mut vm);
        let said = messages(&fx);
        assert!(said.iter().any(|m| m == "applied 1 edit(s)."), "{said:?}");
        assert!(vm.queue.is_empty());
        assert!(!vm.edits_shown(), "the list closes once the queue empties");
        assert!(vm.apply_report().is_none() && vm.apply_review().is_none());
        assert!(!names(&vm).contains(&victim), "{:?}", names(&vm));
        assert!(vm.take_log().iter().any(|l| l == "applied 1 edit(s)."));
    }

    #[test]
    fn a_conflict_waits_for_the_review_then_applies_the_choice() {
        let dir = tempfile::tempdir().unwrap();
        let mut vm = image_pane(dir.path());
        let existing = files(&vm)[0].clone();
        let host = tempfile::tempdir().unwrap();
        std::fs::write(host.path().join(&existing), b"replacement").unwrap();
        let root = crate::model::dir_listing::read_host_root(host.path().to_path_buf()).unwrap();
        let cwd = vm.listing.cwd().unwrap().clone();
        let edits = commander_ops::stage_host_to_image(&root.entries, &cwd);
        let out = vm.stage_edits(edits);
        assert_eq!((out.staged, out.queue_start), (1, 0));
        vm.apply(A::Apply);
        let fx = settle(&mut vm);
        assert!(
            messages(&fx).contains(&"review the staged edits before applying.".to_string()),
            "{fx:?}"
        );
        let review = vm.apply_review().expect("a review");
        assert_eq!(review.scan.conflicts.len(), 1);
        assert!(!vm.is_busy(), "a review waits on the user");
        assert!(!vm.unstage_since(0) || vm.queue.is_empty());
        // Dismissing writes nothing; asking again and overwriting lands the copy.
        vm.stage_edits(commander_ops::stage_host_to_image(&root.entries, &cwd));
        vm.apply(A::DismissReview);
        assert!(vm.apply_review().is_none());
        vm.apply(A::Apply);
        settle(&mut vm);
        assert!(vm.apply_review().is_some());
        vm.apply(A::ApplyReviewed(OnConflict::Replace));
        settle(&mut vm);
        assert!(vm.queue.is_empty(), "{:?}", vm.apply_report());
        let entry = vm
            .listing
            .entries()
            .iter()
            .find(|e| e.name == existing)
            .unwrap()
            .clone();
        assert_eq!(entry.size, b"replacement".len() as u64);
    }

    #[test]
    fn discard_asks_first() {
        let dir = tempfile::tempdir().unwrap();
        let mut vm = image_pane(dir.path());
        let victim = files(&vm)[0].clone();
        select(&mut vm, &victim);
        vm.apply(A::Delete);
        let spec = one_confirm(&vm.apply(A::Discard));
        assert_eq!(spec.purpose, purpose::DISCARD_STAGED);
        assert!(spec.destructive);
        vm.apply(A::Confirmed {
            purpose: purpose::DISCARD_STAGED,
            yes: false,
        });
        assert_eq!(vm.queue.len(), 1);
        vm.apply(A::Discard);
        let fx = vm.apply(A::Confirmed {
            purpose: purpose::DISCARD_STAGED,
            yes: true,
        });
        assert_eq!(messages(&fx), ["discarded staged edits."]);
        assert!(vm.queue.is_empty());
        assert!(vm.apply(A::Discard).is_empty(), "nothing to discard");
    }

    #[test]
    fn rename_asks_with_a_validator_and_refuses_a_bad_answer() {
        let dir = tempfile::tempdir().unwrap();
        let mut vm = image_pane(dir.path());
        let all = names(&vm);
        assert!(all.len() >= 2, "{all:?}");
        let (old, other) = (all[0].clone(), all[1].clone());
        let spec = one_text(&vm.apply(A::RequestRename(old.clone())));
        assert_eq!(spec.purpose, purpose::RENAME);
        assert_eq!(spec.initial, old);
        assert_eq!(spec.validate(""), Err(String::new()));
        assert_eq!(spec.validate(&old), Err(String::new()), "unchanged");
        assert!(spec
            .validate(&other)
            .unwrap_err()
            .contains("already exists"));
        assert!(
            spec.validate(&other.to_lowercase()).is_err(),
            "FAT folds case"
        );
        assert!(spec.validate("RENAMED.TXT").is_ok());
        // The controller checks the answer again: a taken name comes back as the prompt with its error.
        let again = one_text(&vm.apply(A::Answered {
            purpose: purpose::RENAME,
            text: Some(other.clone()),
        }));
        assert!(again.error.unwrap().contains("already exists"));
        assert_eq!(again.initial, other);
        // The validator passes this one; FAT's own name rules refuse it.
        let fs_rule = one_text(&vm.apply(A::Answered {
            purpose: purpose::RENAME,
            text: Some("A*B.TXT".into()),
        }));
        assert!(fs_rule.error.is_some());
        assert!(vm.queue.is_empty());
        let fx = vm.apply(A::Answered {
            purpose: purpose::RENAME,
            text: Some(" RENAMED.TXT ".into()),
        });
        assert_eq!(
            messages(&fx),
            ["staged rename to 'RENAMED.TXT'. Apply to write."]
        );
        assert!(matches!(
            vm.queue.iter().next(),
            Some(StagedEdit::Rename { new_name, .. }) if new_name == "RENAMED.TXT"
        ));
        // An answer with no prompt up does nothing.
        assert!(vm
            .apply(A::Answered {
                purpose: purpose::RENAME,
                text: Some("X".into()),
            })
            .is_empty());
    }

    #[test]
    fn new_folder_stages_and_then_counts_as_taken() {
        let dir = tempfile::tempdir().unwrap();
        let mut vm = image_pane(dir.path());
        let spec = one_text(&vm.apply(A::RequestNewFolder));
        assert_eq!(spec.purpose, purpose::NEW_FOLDER);
        assert_eq!(spec.validate("   "), Err(String::new()));
        vm.apply(A::Answered {
            purpose: purpose::NEW_FOLDER,
            text: None,
        });
        assert!(vm.queue.is_empty(), "cancel stages nothing");
        vm.apply(A::RequestNewFolder);
        let fx = vm.apply(A::Answered {
            purpose: purpose::NEW_FOLDER,
            text: Some("NEWDIR".into()),
        });
        assert_eq!(vm.queue.len(), 1, "{fx:?} {:?}", names(&vm));
        let spec = one_text(&vm.apply(A::RequestNewFolder));
        assert!(spec.validate("NEWDIR").is_err());
        vm.apply(A::Unstage(0));
        assert!(vm.queue.is_empty());
    }

    #[test]
    fn host_writes_run_as_jobs_and_delete_only_after_yes() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.txt"), b"x").unwrap();
        let mut vm = CommanderPaneController::new();
        vm.apply(A::OpenHostFolder(dir.path().to_path_buf()));
        settle(&mut vm);

        let spec = one_text(&vm.apply(A::RequestNewFolder));
        assert!(spec.validate("a/b").unwrap_err().contains("cannot contain"));
        assert!(spec.validate("a.txt").is_err());
        vm.apply(A::Answered {
            purpose: purpose::NEW_FOLDER,
            text: Some("made".into()),
        });
        assert!(vm.host_writing().is_some());
        let fx = settle(&mut vm);
        assert_eq!(messages(&fx), ["created folder 'made'."]);
        assert!(dir.path().join("made").is_dir());
        assert!(names(&vm).contains(&"made".to_string()));

        vm.apply(A::RequestRename("a.txt".into()));
        vm.apply(A::Answered {
            purpose: purpose::RENAME,
            text: Some("b.txt".into()),
        });
        let fx = settle(&mut vm);
        assert_eq!(messages(&fx), ["renamed 'a.txt' to 'b.txt'."]);
        assert!(dir.path().join("b.txt").exists() && !dir.path().join("a.txt").exists());

        select(&mut vm, "b.txt");
        let spec = one_confirm(&vm.apply(A::Delete));
        assert_eq!(spec.purpose, purpose::HOST_DELETE);
        vm.apply(A::Confirmed {
            purpose: purpose::HOST_DELETE,
            yes: false,
        });
        settle(&mut vm);
        assert!(dir.path().join("b.txt").exists(), "no means no");
        vm.apply(A::Delete);
        vm.apply(A::Confirmed {
            purpose: purpose::HOST_DELETE,
            yes: true,
        });
        let fx = settle(&mut vm);
        assert_eq!(messages(&fx), ["deleted 1 item(s) from the host folder."]);
        assert!(!dir.path().join("b.txt").exists());
        assert!(queue_untouched(&vm));
        assert_eq!(vm.take_log().len(), 3);
    }

    fn queue_untouched(vm: &CommanderPaneController) -> bool {
        vm.queue.is_empty()
    }
}
