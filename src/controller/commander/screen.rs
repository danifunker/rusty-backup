//! The two-pane Commander screen: both panes, which one is active, and everything that spans them.
//!
//! [`CommanderController`] owns the left and right [`CommanderPaneController`]s and forwards
//! `Pane(side, ..)` actions to them. A pane's pickers, confirmations and prompts leave as effects;
//! the answer comes back as `Picked` / `Confirmed` / `Answered` and goes to the pane that asked
//! (matched by `purpose`). A pane's messages and errors become the status line, its log lines the
//! session log, and its File Info requests open the detail data here.
//!
//! On top of the panes it runs the cross-pane copy (`copy.rs`): host -> image is staged at once;
//! image -> image is extracted by a job, then staged with names legalised for the destination and
//! a rename notice when any changed; image -> host and host -> host write immediately in a job,
//! after a conflict review when files already exist. Export (to a picked folder or archive file)
//! and checksums are jobs too. Keys resolve through the [`KeyMap`] here, so a view only
//! translates its native key events. Close asks first while edits are staged, then emits
//! [`Effect::CloseScreen`].

use std::path::PathBuf;
use std::sync::Arc;

use disk_core::entry::FileEntry;

use super::pane::{CommanderPaneAction, CommanderPaneController, PaneServices, StageOutcome};
use super::{NoRecent, OnConflict};
use crate::controller::keys::{Command, Key, KeyMap, Mods};
use crate::controller::{ConfirmSpec, Controller, Effect};
use crate::model::checksum::FileChecksum;
use crate::model::commander_ops::{HostCopyDone, HostCopyJob};
use crate::model::edit_queue::StagedEdit;
use crate::model::job::{Job, JobRegistry, NoNotify, Notify, Resource, Snapshot};

pub use crate::fs::export_selection::ExportFormat;
pub use crate::model::edit_queue::MetadataEdit;
pub use fs_mac::resource_fork::ResourceForkMode;

mod copy;
#[cfg(test)]
mod tests;

/// Which pane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    Left,
    Right,
}

impl Side {
    pub fn idx(self) -> usize {
        match self {
            Side::Left => 0,
            Side::Right => 1,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Side::Left => "L",
            Side::Right => "R",
        }
    }

    pub fn other(self) -> Side {
        match self {
            Side::Left => Side::Right,
            Side::Right => Side::Left,
        }
    }
}

/// The `purpose` of the pickers and confirmations the screen itself asks for.
pub mod purpose {
    pub const EXPORT: &str = "commander.export";
    pub const CLOSE_UNSAVED: &str = "commander.close_unsaved";
}

/// Bytes of a file File Info reads for its preview.
pub const MAX_PREVIEW_SIZE: usize = 1024 * 1024;
/// Session-log entries kept (oldest drop first).
pub const MAX_LOG: usize = 200;

/// A checksum the window shows, in [`HashAlgo::ALL`] order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashAlgo {
    Crc32,
    Md5,
    Sha1,
    Sha256,
}

impl HashAlgo {
    pub const ALL: [HashAlgo; 4] = [
        HashAlgo::Crc32,
        HashAlgo::Md5,
        HashAlgo::Sha1,
        HashAlgo::Sha256,
    ];

    pub fn label(self) -> &'static str {
        match self {
            HashAlgo::Crc32 => "CRC32",
            HashAlgo::Md5 => "MD5",
            HashAlgo::Sha1 => "SHA1",
            HashAlgo::Sha256 => "SHA256",
        }
    }
}

/// What a user does on the Commander screen.
#[derive(Debug, Clone)]
pub enum CommanderAction {
    Pane(Side, CommanderPaneAction),
    /// The user worked in this pane; it becomes the one keys and the middle Delete act on.
    Focus(Side),
    SwitchPane,
    /// Copy this pane's selection into the other pane's folder.
    Copy(Side),
    /// The middle column's Delete, on the active pane.
    Delete,
    /// Export this pane's selection; `None` uses the screen's export format.
    Export {
        from: Side,
        format: Option<ExportFormat>,
    },
    Checksums(Side),
    /// Every finished checksum, as text, to the clipboard.
    CopyChecksums,
    /// One value of the `file`th result to the clipboard.
    CopyChecksum {
        file: usize,
        algo: HashAlgo,
    },
    CloseChecksums,
    OpenDetail(Side, String),
    CloseDetail,
    /// A File Info editor's change, staged on the detail's pane.
    StageMetadata(Box<MetadataEdit>),
    /// Run the held host copy, settling existing files this way.
    HostConflict(OnConflict),
    HostConflictCancel,
    RenameNoticeOk,
    /// Take the copy behind the rename notice back off the queue.
    RenameNoticeUndo,
    /// Stop the running copy or export.
    CancelCopy,
    SetKeepDates(bool),
    SetForkMode(ResourceForkMode),
    SetExportFormat(ExportFormat),
    ShowLog(bool),
    CopyLog,
    ClearLog,
    ShowHelp(bool),
    /// Close the screen, asking first while either pane has staged edits.
    RequestClose,
    /// A key press, resolved through the key map.
    Key(Key, Mods),
    /// Rows the pane's listing shows, the step for Page Up / Page Down.
    SetPageRows(Side, i32),
    /// A dropped file or folder opens in the active pane.
    OpenDropped(PathBuf),
    /// The persisted recent lists were cleared elsewhere.
    RecentCleared,
    /// The answer to a `PickFile` / `PickFolder`; `None` is cancel.
    Picked {
        purpose: &'static str,
        path: Option<PathBuf>,
    },
    Confirmed {
        purpose: &'static str,
        yes: bool,
    },
    Answered {
        purpose: &'static str,
        text: Option<String>,
    },
}

/// Names a staged copy changed (or left out) for the destination filesystem.
#[derive(Debug, Clone)]
pub struct RenameNotice {
    pub dest: Side,
    pub outcome: StageOutcome,
}

/// The File Info window's data; `serial` changes whenever it shows another entry.
#[derive(Debug, Clone)]
pub struct FileDetail {
    pub side: Side,
    pub entry: FileEntry,
    /// The pane takes metadata edits (a writable image volume, not a wrapper's contents).
    pub editable: bool,
    pub fs_type: String,
    /// Up to [`MAX_PREVIEW_SIZE`] bytes of a file; `None` for a folder or a failed read.
    pub preview: Option<Vec<u8>>,
    pub xattrs: Vec<disk_core::xattr::Xattr>,
    /// What the last staged edit did.
    pub result: Option<String>,
    pub serial: u64,
}

/// One file's checksums, or why it has none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChecksumRow {
    pub name: String,
    pub size: u64,
    pub error: Option<String>,
    /// Hex values in [`HashAlgo::ALL`] order.
    pub values: Option<[String; 4]>,
}

/// The checksum window: its job while hashing, then the rows.
pub struct Checksums {
    pub title: String,
    job: Option<Job<Vec<FileChecksum>>>,
    pub rows: Vec<ChecksumRow>,
    /// Why the whole run failed (the source would not open).
    pub error: Option<String>,
}

impl Checksums {
    /// The hashing job, while it runs.
    pub fn running(&self) -> Option<Snapshot> {
        self.job.as_ref().map(|j| j.snapshot())
    }
}

enum CopyRun {
    /// An immediate write; `dest` is the pane to re-list, `None` for an export.
    Host {
        job: Job<HostCopyDone>,
        dest: Option<Side>,
        where_to: String,
    },
    /// An image -> image extraction whose edits stage onto `dest`.
    Stage {
        job: Job<Vec<StagedEdit>>,
        dest: Side,
    },
}

/// A host copy held back because files it would write already exist.
struct PendingConflict {
    job: HostCopyJob,
    dest: Option<Side>,
    names: Vec<String>,
    resources: Vec<Resource>,
    where_to: String,
}

/// An export waiting for its destination picker.
struct PendingExport {
    from: Side,
    format: ExportFormat,
    entries: Vec<FileEntry>,
}

/// Both panes and the operations between them; see the module docs.
pub struct CommanderController {
    left: CommanderPaneController,
    right: CommanderPaneController,
    active: Side,
    status: String,
    /// Extracted files an image -> image copy staged; kept until the screen closes.
    temp: Option<tempfile::TempDir>,
    jobs: JobRegistry,
    notify: Arc<dyn Notify>,
    keymap: KeyMap,
    page_rows: [i32; 2],
    keep_dates: bool,
    export_fork_mode: ResourceForkMode,
    export_format: ExportFormat,
    copy: Option<CopyRun>,
    conflict: Option<PendingConflict>,
    rename_notice: Option<RenameNotice>,
    export: Option<PendingExport>,
    checksums: Option<Checksums>,
    detail: Option<FileDetail>,
    detail_serial: u64,
    log: Vec<String>,
    show_log: bool,
    show_help: bool,
    /// Which pane asked each outstanding picker, confirmation or prompt.
    askers: Vec<(&'static str, Side)>,
}

impl Controller for CommanderController {
    type Action = CommanderAction;

    fn apply(&mut self, action: CommanderAction) -> Vec<Effect> {
        use CommanderAction as A;
        match action {
            A::Pane(side, a) => self.forward(side, a),
            A::Focus(side) => {
                self.active = side;
                Vec::new()
            }
            A::SwitchPane => {
                self.active = self.active.other();
                Vec::new()
            }
            A::Copy(from) => {
                self.status = self.copy(from);
                Vec::new()
            }
            A::Delete => {
                if !self.can_delete() {
                    return Vec::new();
                }
                self.forward(self.active, CommanderPaneAction::Delete)
            }
            A::Export { from, format } => self.request_export(from, format),
            A::Checksums(from) => {
                self.status = self.start_checksums(from);
                Vec::new()
            }
            A::CopyChecksums => self.checksums_text().map_or_else(Vec::new, clip),
            A::CopyChecksum { file, algo } => {
                let row = self.checksums.as_ref().and_then(|c| c.rows.get(file));
                let value = row.and_then(|r| r.values.as_ref());
                value.map_or_else(Vec::new, |v| clip(v[algo as usize].clone()))
            }
            A::CloseChecksums => {
                self.checksums = None;
                Vec::new()
            }
            A::OpenDetail(side, name) => self.open_detail(side, name),
            A::CloseDetail => {
                self.detail = None;
                Vec::new()
            }
            A::StageMetadata(edit) => {
                self.stage_metadata(*edit);
                Vec::new()
            }
            A::HostConflict(on) => {
                self.resolve_conflict(Some(on));
                Vec::new()
            }
            A::HostConflictCancel => {
                self.resolve_conflict(None);
                Vec::new()
            }
            A::RenameNoticeOk => {
                self.rename_notice = None;
                Vec::new()
            }
            A::RenameNoticeUndo => {
                self.undo_rename_notice();
                Vec::new()
            }
            A::CancelCopy => {
                match &self.copy {
                    Some(CopyRun::Host { job, .. }) => job.cancel(),
                    Some(CopyRun::Stage { job, .. }) => job.cancel(),
                    None => {}
                }
                Vec::new()
            }
            A::SetKeepDates(on) => {
                self.keep_dates = on;
                Vec::new()
            }
            A::SetForkMode(mode) => {
                self.export_fork_mode = mode;
                Vec::new()
            }
            A::SetExportFormat(format) => {
                self.export_format = format;
                Vec::new()
            }
            A::ShowLog(on) => {
                self.show_log = on;
                Vec::new()
            }
            A::CopyLog if self.log.is_empty() => Vec::new(),
            A::CopyLog => clip(self.log.join("\n")),
            A::ClearLog => {
                self.log.clear();
                Vec::new()
            }
            A::ShowHelp(on) => {
                self.show_help = on;
                Vec::new()
            }
            A::RequestClose => self.request_close(),
            A::Key(key, mods) => self.key(key, mods),
            A::SetPageRows(side, rows) => {
                self.page_rows[side.idx()] = rows.max(1);
                Vec::new()
            }
            A::OpenDropped(path) => self.forward(self.active, CommanderPaneAction::OpenPath(path)),
            A::RecentCleared => {
                let mut fx = self.forward(Side::Left, CommanderPaneAction::RecentCleared);
                fx.extend(self.forward(Side::Right, CommanderPaneAction::RecentCleared));
                fx
            }
            A::Picked { purpose, path } if purpose == purpose::EXPORT => {
                self.status = self.export_picked(path);
                Vec::new()
            }
            A::Picked { purpose, path } => {
                let side = self.asker_take(purpose);
                self.forward(side, CommanderPaneAction::Picked { purpose, path })
            }
            A::Confirmed { purpose, yes } if purpose == purpose::CLOSE_UNSAVED => {
                if !yes {
                    return Vec::new();
                }
                self.left.queue.clear();
                self.right.queue.clear();
                vec![Effect::CloseScreen]
            }
            A::Confirmed { purpose, yes } => {
                let side = self.asker_take(purpose);
                self.forward(side, CommanderPaneAction::Confirmed { purpose, yes })
            }
            A::Answered { purpose, text } => {
                let side = self.asker_take(purpose);
                self.forward(side, CommanderPaneAction::Answered { purpose, text })
            }
        }
    }

    fn tick(&mut self) -> Vec<Effect> {
        let mut fx = Vec::new();
        for side in [Side::Left, Side::Right] {
            let pane_fx = self.pane_mut(side).tick();
            fx.extend(self.absorb(side, pane_fx));
        }
        self.poll_copy();
        self.poll_checksums();
        fx
    }
}

fn clip(text: String) -> Vec<Effect> {
    vec![Effect::CopyToClipboard(text)]
}

fn log_timestamp() -> String {
    chrono::Local::now().format("%H:%M:%S").to_string()
}

impl CommanderController {
    /// Both panes register their jobs in `left.jobs` and wake the shell through `left.notify`.
    pub fn new(left: PaneServices, right: PaneServices, keymap: KeyMap) -> Self {
        let jobs = left.jobs.clone();
        let notify = Arc::clone(&left.notify);
        Self {
            left: CommanderPaneController::with_services(left),
            right: CommanderPaneController::with_services(right),
            active: Side::Left,
            status: "Commander Mode -- open a disk image in each pane; select files and \
                     use the middle Copy buttons or right-click to stage a copy."
                .into(),
            temp: None,
            jobs,
            notify,
            keymap,
            page_rows: [1, 1],
            keep_dates: true,
            export_fork_mode: ResourceForkMode::MacBinary,
            export_format: ExportFormat::LooseFiles,
            copy: None,
            conflict: None,
            rename_notice: None,
            export: None,
            checksums: None,
            detail: None,
            detail_serial: 0,
            log: Vec::new(),
            show_log: false,
            show_help: false,
            askers: Vec::new(),
        }
    }

    /// One shared job registry, no wake, no recent files and the default keys: for tests and the CLI.
    pub fn headless() -> Self {
        let jobs = JobRegistry::new();
        let services = || PaneServices {
            jobs: jobs.clone(),
            notify: Arc::new(NoNotify),
            recent: Box::new(NoRecent),
        };
        Self::new(services(), services(), KeyMap::for_this_platform())
    }

    // --- read accessors ---------------------------------------------------

    pub fn pane(&self, side: Side) -> &CommanderPaneController {
        match side {
            Side::Left => &self.left,
            Side::Right => &self.right,
        }
    }

    pub fn active(&self) -> Side {
        self.active
    }

    pub fn status(&self) -> &str {
        &self.status
    }

    pub fn keep_dates(&self) -> bool {
        self.keep_dates
    }

    pub fn export_fork_mode(&self) -> ResourceForkMode {
        self.export_fork_mode
    }

    pub fn export_format(&self) -> ExportFormat {
        self.export_format
    }

    /// The running copy or export: the progress window's title and its job.
    pub fn copy_job(&self) -> Option<(&'static str, Snapshot)> {
        match self.copy.as_ref()? {
            CopyRun::Host { job, .. } => Some(("Copying", job.snapshot())),
            CopyRun::Stage { job, .. } => Some(("Copying to other pane", job.snapshot())),
        }
    }

    /// The names a held host copy would overwrite, while the review waits.
    pub fn host_conflict(&self) -> Option<&[String]> {
        self.conflict.as_ref().map(|c| c.names.as_slice())
    }

    pub fn rename_notice(&self) -> Option<&RenameNotice> {
        self.rename_notice.as_ref()
    }

    pub fn checksums(&self) -> Option<&Checksums> {
        self.checksums.as_ref()
    }

    pub fn detail(&self) -> Option<&FileDetail> {
        self.detail.as_ref()
    }

    /// Operations completed this session, timestamped, oldest first.
    pub fn log(&self) -> &[String] {
        &self.log
    }

    pub fn log_shown(&self) -> bool {
        self.show_log
    }

    pub fn help_shown(&self) -> bool {
        self.show_help
    }

    /// Every command with its first chord on this platform, for the help window.
    pub fn key_help(&self) -> Vec<(&'static str, String)> {
        let rows = Command::ALL.iter();
        rows.map(|c| (c.label(), self.keymap.hint(*c))).collect()
    }

    /// The help window's footer, naming the keys that close it.
    pub fn key_help_footer(&self) -> String {
        format!(
            "{} or {} closes this window. Rebind keys in keybindings.toml.",
            self.keymap.hint(Command::Help),
            self.keymap.hint(Command::Close)
        )
    }

    /// True when this key press means a command here, so a view hands it over instead of using it.
    pub fn binds(&self, key: Key, mods: Mods) -> bool {
        self.keymap.lookup(key, mods).is_some()
    }

    /// The pane that asked the outstanding prompt with this `purpose`, if a pane did.
    pub fn asker(&self, purpose: &str) -> Option<Side> {
        let found = self.askers.iter().rev().find(|(p, _)| *p == purpose);
        found.map(|(_, s)| *s)
    }

    /// True while one of the screen's own reviews (or a pane's apply review or report) is up.
    pub fn modal_open(&self) -> bool {
        let pane_modal =
            |p: &CommanderPaneController| p.apply_review().is_some() || p.apply_report().is_some();
        self.conflict.is_some()
            || self.rename_notice.is_some()
            || pane_modal(&self.left)
            || pane_modal(&self.right)
    }

    /// A copy needs a selection on the source, a ready destination and no copy in flight.
    pub fn can_copy(&self, from: Side) -> bool {
        self.copy.is_none()
            && self.pane(from).has_selection()
            && self.pane(from.other()).can_receive()
    }

    /// Delete acts on the active pane; read-only and remote panes take no deletes.
    pub fn can_delete(&self) -> bool {
        let pane = self.pane(self.active);
        self.copy.is_none()
            && pane.has_selection()
            && !(pane.is_backup() || pane.is_archive())
            && !pane.is_remote()
    }

    // --- panes ------------------------------------------------------------

    fn pane_mut(&mut self, side: Side) -> &mut CommanderPaneController {
        match side {
            Side::Left => &mut self.left,
            Side::Right => &mut self.right,
        }
    }

    fn forward(&mut self, side: Side, action: CommanderPaneAction) -> Vec<Effect> {
        let fx = self.pane_mut(side).apply(action);
        self.absorb(side, fx)
    }

    /// A pane's effects: messages become the status, prompts remember who asked, the rest pass.
    fn absorb(&mut self, side: Side, fx: Vec<Effect>) -> Vec<Effect> {
        let tag = side.label();
        let mut out = Vec::new();
        for effect in fx {
            let asked = match &effect {
                Effect::Message(m) => {
                    self.status = format!("[{tag}] {m}");
                    continue;
                }
                Effect::PickFile(s) | Effect::PickFolder(s) => Some(s.purpose),
                Effect::Confirm(s) => Some(s.purpose),
                Effect::AskPassword(s) => Some(s.purpose),
                Effect::AskText(s) => Some(s.purpose),
                Effect::OpenUrl(_) | Effect::CopyToClipboard(_) | Effect::CloseScreen => None,
            };
            if let Some(purpose) = asked {
                self.askers.retain(|(p, _)| *p != purpose);
                self.askers.push((purpose, side));
            }
            out.push(effect);
        }
        if let Some(e) = self.pane_mut(side).take_error() {
            self.status = format!("[{tag}] {e}");
        }
        for line in self.pane_mut(side).take_log() {
            self.record_log(format!("[{tag}] {line}"));
        }
        if let Some(name) = self.pane_mut(side).take_info_request() {
            out.extend(self.open_detail(side, name));
        }
        out
    }

    /// The pane that asked `purpose` (the active one if none did), forgetting the question.
    fn asker_take(&mut self, purpose: &str) -> Side {
        let side = self.asker(purpose).unwrap_or(self.active);
        self.askers.retain(|(p, _)| *p != purpose);
        side
    }

    /// Re-read a pane's folder after something wrote into it.
    fn reload(&mut self, side: Side) {
        let pane = self.pane_mut(side);
        pane.apply(CommanderPaneAction::Reload);
        pane.take_error();
    }

    fn record_log(&mut self, msg: String) {
        self.log.push(format!("[{}] {msg}", log_timestamp()));
        if self.log.len() > MAX_LOG {
            let excess = self.log.len() - MAX_LOG;
            self.log.drain(0..excess);
        }
    }

    /// Set the status line and record it in the session log.
    fn report(&mut self, msg: String) {
        self.status = msg.clone();
        self.record_log(msg);
    }

    // --- keys and closing -------------------------------------------------

    fn key(&mut self, key: Key, mods: Mods) -> Vec<Effect> {
        if self.modal_open() {
            return Vec::new();
        }
        match self.keymap.lookup(key, mods) {
            Some(cmd) => self.command(cmd),
            None => Vec::new(),
        }
    }

    /// What a command means here: pane commands go to the active pane, the rest to the buttons.
    fn command(&mut self, cmd: Command) -> Vec<Effect> {
        use CommanderPaneAction as P;
        let side = self.active;
        let page = self.page_rows[side.idx()];
        let pane_action = match cmd {
            Command::CursorUp => Some(P::CursorUp),
            Command::CursorDown => Some(P::CursorDown),
            Command::PageUp => Some(P::CursorPage(-page)),
            Command::PageDown => Some(P::CursorPage(page)),
            Command::Home => Some(P::CursorHome),
            Command::End => Some(P::CursorEnd),
            Command::Open => Some(P::ActivateCursor),
            Command::Parent => Some(P::Up),
            Command::Mark => Some(P::ToggleMark),
            Command::Refresh => Some(P::Reload),
            Command::Info => Some(P::InfoAtCursor),
            Command::Rename => Some(P::RenameAtCursor),
            Command::NewFolder => Some(P::RequestNewFolder),
            Command::Apply if self.pane(side).can_apply() => Some(P::Apply),
            Command::Delete if self.can_delete() => Some(P::Delete),
            Command::Apply
            | Command::Delete
            | Command::Help
            | Command::SwitchPane
            | Command::Copy
            | Command::Edit
            | Command::Close => None,
        };
        if let Some(action) = pane_action {
            return self.forward(side, action);
        }
        match cmd {
            Command::Help => self.show_help = !self.show_help,
            Command::SwitchPane => self.active = side.other(),
            Command::Copy => {
                if self.can_copy(side) {
                    self.status = self.copy(side);
                }
            }
            Command::Close => return self.close_shortcut(),
            // No edit operation exists yet; Apply and Delete land here only when refused.
            Command::Edit | Command::Apply | Command::Delete => {}
            Command::CursorUp
            | Command::CursorDown
            | Command::PageUp
            | Command::PageDown
            | Command::Home
            | Command::End
            | Command::Open
            | Command::Parent
            | Command::Mark
            | Command::Refresh
            | Command::Info
            | Command::Rename
            | Command::NewFolder => {}
        }
        Vec::new()
    }

    /// Esc closes the topmost Commander window, then Commander itself.
    fn close_shortcut(&mut self) -> Vec<Effect> {
        if self.show_help {
            self.show_help = false;
        } else if self.detail.is_some() {
            self.detail = None;
        } else if self.checksums.is_some() {
            self.checksums = None;
        } else if self.show_log {
            self.show_log = false;
        } else {
            return self.request_close();
        }
        Vec::new()
    }

    fn request_close(&mut self) -> Vec<Effect> {
        let n = self.left.queue.len() + self.right.queue.len();
        if n == 0 {
            return vec![Effect::CloseScreen];
        }
        vec![Effect::Confirm(ConfirmSpec {
            purpose: purpose::CLOSE_UNSAVED,
            title: "Unsaved staged edits".into(),
            message: format!(
                "{n} staged edit(s) across the panes have not been applied. \
                 Apply them per-pane first, or discard."
            ),
            confirm_label: "Discard & Close".into(),
            destructive: true,
        })]
    }

    // --- File Info --------------------------------------------------------

    fn open_detail(&mut self, side: Side, name: String) -> Vec<Effect> {
        let pane = self.pane_mut(side);
        // Host folders, read-only sources and a wrapper's contents show metadata without editors.
        let editable =
            !pane.is_host_pane() && !pane.is_backup() && !pane.is_archive() && !pane.tree_active();
        let fs_type = pane.detail_fs_type();
        let Some((entry, preview)) = pane.detail_payload(&name, MAX_PREVIEW_SIZE) else {
            self.status = format!("[{}] could not open File Info for '{name}'.", side.label());
            return Vec::new();
        };
        let xattrs = pane.detail_xattrs(&entry);
        self.status = format!("File Info: {}", entry.name);
        self.detail_serial += 1;
        self.detail = Some(FileDetail {
            side,
            entry,
            editable,
            fs_type,
            preview,
            xattrs,
            result: None,
            serial: self.detail_serial,
        });
        Vec::new()
    }

    fn stage_metadata(&mut self, edit: MetadataEdit) {
        let Some((side, editable)) = self.detail.as_ref().map(|d| (d.side, d.editable)) else {
            return;
        };
        let pane = self.pane_mut(side);
        let result = if !editable {
            "This entry is read-only here.".to_string()
        } else if pane.is_busy() {
            "Wait for the running apply to finish.".to_string()
        } else {
            pane.queue.stage_metadata(edit)
        };
        if let Some(d) = self.detail.as_mut() {
            d.result = Some(result);
        }
    }
}
