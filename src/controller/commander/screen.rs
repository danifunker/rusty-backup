//! The two-pane Commander screen: both panes, which one is active, and everything that spans them.
//!
//! [`CommanderController`] owns the left and right [`BrowserPaneController`]s and forwards
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

use super::{NoRecent, OnConflict, RecentStore};
use crate::controller::browser::pane::{
    BrowserPaneAction, BrowserPaneController, PaneServices, PrefsOnly, StageOutcome,
};
use crate::controller::browser::PaneProfile;
use crate::controller::keys::{Command, Key, KeyMap, Mods};
use crate::controller::{ConfirmSpec, Controller, Effect};
use crate::model::checksum::FileChecksum;
use crate::model::commander_ops::{HostCopyDone, HostCopyJob};
use crate::model::edit_queue::StagedEdit;
use crate::model::entry_detail::{self, DetailExtras};
use crate::model::job::{Job, JobCtx, JobRegistry, NoNotify, Notify, Resource, Snapshot};
pub use disk_core::id_names::IdNameMap;
pub use disk_core::xattr::Xattr;
pub use fs_squashfs::squashfs_edit::SizePlan;

pub use crate::fs::export_selection::ExportFormat;
use crate::model::dir_listing::SortColumn;
pub use crate::model::edit_queue::MetadataEdit;
pub use crate::model::extract::ExtractOptions;
pub use detail::DetailField;
pub use follow::{ViewItemKind, ViewMenuItem, FOLLOW_DELAY};
pub use fs_mac::resource_fork::ResourceForkMode;
pub use budget::{BudgetAction, BudgetForm, BudgetMode};
pub use modes::{ModeAction, SpaceReadout, UnsavedChoice, MIN_NESTED_IMAGE_BYTES};

mod budget;
mod copy;
mod detail;
mod extract;
mod follow;
mod modes;
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
    pub const EXTRACT: &str = "commander.extract";
    pub const CLOSE_UNSAVED: &str = "commander.close_unsaved";
    pub const QUIT_UNSAVED: &str = "commander.quit_unsaved";
    pub const DETAIL_MODE: &str = "commander.detail_mode";
    pub const DETAIL_OWNER: &str = "commander.detail_owner";
    pub const DETAIL_XATTR: &str = "commander.detail_xattr";
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
    Pane(Side, BrowserPaneAction),
    /// The Inspect browse view's source-level modes (Edit Mode, CHD, archives, budget).
    Mode(ModeAction),
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
    /// Export the entry at `path` (`/`: the whole volume) instead of the selection.
    ExportEntry {
        from: Side,
        path: String,
        format: Option<ExportFormat>,
    },
    Checksums(Side),
    /// Extract the entry at `path` (a folder with its contents) into `dest`, or a folder the user picks.
    Extract {
        from: Side,
        path: String,
        options: ExtractOptions,
        dest: Option<PathBuf>,
    },
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
    /// Ask for a new value of this File Info field, then stage it (the terminal's editors).
    EditDetail(DetailField),
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
    /// The partition chooser for the active pane, for views without an always-visible picker.
    ShowPartitions(bool),
    /// Show the active pane's File Info in the other pane, following its cursor; again to stop.
    ToggleQuickView,
    /// The View menu, for views that open it from a key rather than a menu bar.
    ShowViewMenu(bool),
    /// Close the screen, asking first while either pane has staged edits.
    RequestClose,
    /// Quit the application, asking first while either pane has staged edits.
    RequestQuit,
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

/// One entry of a view's "Export as" menu: a format, and the fork container for loose files.
#[derive(Debug, Clone, PartialEq)]
pub struct ExportChoice {
    pub label: String,
    pub format: ExportFormat,
    pub fork: Option<ResourceForkMode>,
}

/// The export format a volume of `fs_type` starts with: Zip for PC, MAR for Mac, tar.gz for Unix.
pub fn default_export_format(fs_type: &str) -> ExportFormat {
    let t = fs_type.to_ascii_lowercase();
    let has = |words: &[&str]| words.iter().any(|w| t.contains(w));
    if t == "mfs" || t.starts_with("hfs") {
        return ExportFormat::MacArchive;
    }
    if t.starts_with("fat") || t.starts_with("vfat") || has(&["exfat", "ntfs", "hpfs", "msdos"]) {
        return ExportFormat::Zip;
    }
    let unix = [
        "ext", "ufs", "xfs", "efs", "jfs", "minix", "reiser", "btrfs", "squashfs", "cramfs",
        "romfs", "sysv", "xenix", "zfs", "f2fs",
    ];
    // Xerox Alto's BFS shares the name with BeOS's but is no Unix filesystem.
    if has(&unix) || (t.contains("bfs") && !t.contains("alto")) {
        return ExportFormat::TarGz;
    }
    ExportFormat::LooseFiles
}

/// The "Export as" menu for a view without an always-visible format chooser.
pub fn export_choices() -> Vec<ExportChoice> {
    use ResourceForkMode as M;
    let forks = [
        M::AppleDouble,
        M::DataForkOnly,
        M::MacBinary,
        M::BinHex,
        M::SeparateRsrc,
    ];
    let loose = forks.iter().map(|m| ExportChoice {
        label: format!("Files, {}", m.label()),
        format: ExportFormat::LooseFiles,
        fork: Some(*m),
    });
    let rest = ExportFormat::ALL
        .iter()
        .filter(|f| **f != ExportFormat::LooseFiles);
    loose
        .chain(rest.map(|f| ExportChoice {
            label: f.label().to_string(),
            format: *f,
            fork: None,
        }))
        .collect()
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
    /// The preview, xattrs, names and budget are still being read by a job.
    pub loading: bool,
    /// Up to [`MAX_PREVIEW_SIZE`] bytes of a file; `None` for a folder or a failed read.
    pub preview: Option<Vec<u8>>,
    /// The same of its resource fork, when it has one.
    pub rsrc_preview: Option<Vec<u8>>,
    pub xattrs: Vec<Xattr>,
    /// uid/gid -> name from the image's account files, for the Owner row.
    pub id_names: IdNameMap,
    /// A SquashFS volume's size budget, when File Info opened in its root folder.
    pub size_plan: Option<SizePlan>,
    /// What the last staged edit did.
    pub result: Option<String>,
    pub serial: u64,
}

impl FileDetail {
    /// The Owner row: `uid:gid` with the image's account names where it has them.
    pub fn owner(&self) -> Option<String> {
        let (u, g) = (self.entry.uid?, self.entry.gid?);
        Some(self.id_names.format_owner(u, g))
    }

    /// The SquashFS size budget as text lines (empty when there is none).
    pub fn size_budget_lines(&self) -> Vec<String> {
        use disk_core::display::format_size;
        let Some(p) = &self.size_plan else {
            return Vec::new();
        };
        let room = match (p.capacity, p.headroom) {
            (Some(cap), Some(head)) => format!(
                "Room available {} ({} unused)",
                format_size(cap),
                format_size(head)
            ),
            _ => "Room available unbounded - this file is the filesystem".to_string(),
        };
        vec![
            format!("Image occupies {}", format_size(p.image_len)),
            format!("Content        {} uncompressed", format_size(p.content_len)),
            format!("Compressed to  {}", p.describe_ratio()),
            room,
            // Saving an edit rebuilds the whole image, so only a ceiling can be promised.
            "Set a ceiling with: rb-cli squashfs put|rm --size|--grow".to_string(),
        ]
    }
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
    left: BrowserPaneController,
    right: BrowserPaneController,
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
    /// Each pane's explicit export format pick and the volume it was made on.
    export_pick: [Option<(ExportFormat, String)>; 2],
    copy: Option<CopyRun>,
    conflict: Option<PendingConflict>,
    rename_notice: Option<RenameNotice>,
    export: Option<PendingExport>,
    /// An extraction waiting for its destination picker.
    extract: Option<extract::PendingExtract>,
    checksums: Option<Checksums>,
    detail: Option<FileDetail>,
    detail_serial: u64,
    /// Reads the open File Info's extras.
    detail_job: Option<Job<DetailExtras>>,
    log: Vec<String>,
    show_log: bool,
    show_help: bool,
    /// The pane whose partition chooser is up.
    partitions: Option<Side>,
    /// The pane where a key command selected the cursor row itself; a later key drops that selection.
    implicit: Option<Side>,
    /// Which pane asked each outstanding picker, confirmation or prompt.
    askers: Vec<(&'static str, Side)>,
    /// One pane (the TUI Explorer): only the left pane is used and there is nothing to copy to.
    single: bool,
    quick_view: bool,
    view_menu: bool,
    /// Each pane's File Info following its cursor (quick view, side panel).
    follow: [follow::Follow; 2],
    /// The Inspect browse view's source and edit modes (`open_inspect`).
    modes: modes::Modes,
}

impl Controller for CommanderController {
    type Action = CommanderAction;

    fn apply(&mut self, action: CommanderAction) -> Vec<Effect> {
        if !matches!(
            action,
            CommanderAction::Key(..) | CommanderAction::SetPageRows(..)
        ) {
            self.implicit = None;
        }
        let fx = self.apply_action(action);
        // A copy stages onto the other pane, so both panes' rows can change.
        self.left.rebuild_rows();
        self.right.rebuild_rows();
        self.sync_follow();
        fx
    }

    fn tick(&mut self) -> Vec<Effect> {
        let mut fx = Vec::new();
        for side in [Side::Left, Side::Right] {
            let pane_fx = self.pane_mut(side).tick();
            fx.extend(self.absorb(side, pane_fx));
        }
        self.poll_copy();
        self.poll_checksums();
        self.poll_detail();
        self.poll_modes();
        self.left.rebuild_rows();
        self.right.rebuild_rows();
        self.sync_follow();
        fx
    }
}

impl CommanderController {
    fn apply_action(&mut self, action: CommanderAction) -> Vec<Effect> {
        use CommanderAction as A;
        match action {
            A::Pane(side, a) => self.forward(side, a),
            A::Mode(m) => self.apply_mode(m),
            A::Focus(side) => {
                if !self.single {
                    self.active = side;
                }
                Vec::new()
            }
            A::SwitchPane if self.single => {
                self.forward(Side::Left, BrowserPaneAction::SwitchFocus)
            }
            A::SwitchPane => self.switch_pane(),
            A::Copy(from) => {
                self.status = self.copy(from);
                Vec::new()
            }
            A::Delete => {
                if !self.can_delete() {
                    return Vec::new();
                }
                self.forward(self.active, BrowserPaneAction::Delete)
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
                self.close_checksums();
                Vec::new()
            }
            A::OpenDetail(side, name) => self.open_detail(side, name),
            A::CloseDetail => {
                self.close_detail();
                Vec::new()
            }
            A::StageMetadata(edit) => {
                self.stage_metadata(*edit);
                Vec::new()
            }
            A::EditDetail(field) => self.ask_detail(field, None, None),
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
                let side = self.active;
                let key = self.volume_key(side);
                self.export_pick[side.idx()] = Some((format, key));
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
            A::ShowPartitions(on) => {
                self.partitions = None;
                if on {
                    self.toggle_partitions(self.active);
                }
                Vec::new()
            }
            A::ToggleQuickView => {
                self.toggle_quick_view();
                Vec::new()
            }
            A::ShowViewMenu(on) => {
                self.view_menu = on;
                Vec::new()
            }
            A::RequestClose => self.request_close(false),
            A::RequestQuit => self.request_close(true),
            A::Key(key, mods) => self.key(key, mods),
            A::SetPageRows(side, rows) => {
                self.page_rows[side.idx()] = rows.max(1);
                Vec::new()
            }
            A::OpenDropped(path) => self.forward(self.active, BrowserPaneAction::OpenPath(path)),
            A::RecentCleared => {
                let mut fx = self.forward(Side::Left, BrowserPaneAction::RecentCleared);
                fx.extend(self.forward(Side::Right, BrowserPaneAction::RecentCleared));
                fx
            }
            A::ExportEntry { from, path, format } => self.request_export_entry(from, &path, format),
            A::Extract {
                from,
                path,
                options,
                dest,
            } => self.request_extract(from, &path, options, dest),
            A::Picked { purpose, path } if purpose == purpose::EXTRACT => {
                self.status = self.extract_picked(path);
                Vec::new()
            }
            A::Picked { purpose, path } if purpose == purpose::EXPORT => {
                self.status = self.export_picked(path);
                Vec::new()
            }
            A::Picked { purpose, path } => {
                let side = self.asker_take(purpose);
                self.forward(side, BrowserPaneAction::Picked { purpose, path })
            }
            A::Confirmed { purpose, yes }
                if purpose == purpose::CLOSE_UNSAVED || purpose == purpose::QUIT_UNSAVED =>
            {
                if !yes {
                    return Vec::new();
                }
                self.left.queue.clear();
                self.right.queue.clear();
                vec![close_effect(purpose == purpose::QUIT_UNSAVED)]
            }
            A::Confirmed { purpose, yes } => {
                let side = self.asker_take(purpose);
                self.forward(side, BrowserPaneAction::Confirmed { purpose, yes })
            }
            A::Answered { purpose, text } if DetailField::for_purpose(purpose).is_some() => {
                self.detail_answered(purpose, text)
            }
            A::Answered { purpose, text } => {
                let side = self.asker_take(purpose);
                self.forward(side, BrowserPaneAction::Answered { purpose, text })
            }
        }
    }
}

fn close_effect(quit: bool) -> Effect {
    if quit {
        Effect::Quit
    } else {
        Effect::CloseScreen
    }
}

fn clip(text: String) -> Vec<Effect> {
    vec![Effect::CopyToClipboard(text)]
}

fn log_timestamp() -> String {
    chrono::Local::now().format("%H:%M:%S").to_string()
}

/// The sort the Sort command steps to: this column descending, then the next column ascending.
fn next_sort(pane: &BrowserPaneController) -> SortColumn {
    let col = pane.listing.sort_column();
    if !pane.listing.is_descending() {
        return col;
    }
    match col {
        SortColumn::Name => SortColumn::Size,
        SortColumn::Size => SortColumn::Modified,
        SortColumn::Modified => SortColumn::Type,
        SortColumn::Type => SortColumn::Name,
    }
}

fn sort_status(side: Side, listing: &crate::model::dir_listing::DirListing) -> String {
    let dir = if listing.is_descending() {
        "descending"
    } else {
        "ascending"
    };
    let col = listing.sort_column();
    format!("[{}] Sorted by {col:?}, {dir}.", side.label())
}

impl CommanderController {
    /// Both panes register their jobs in `left.jobs` and wake the shell through `left.notify`.
    pub fn new(left: PaneServices, right: PaneServices, keymap: KeyMap) -> Self {
        let jobs = left.jobs.clone();
        let notify = Arc::clone(&left.notify);
        let mut ctl = Self {
            left: BrowserPaneController::with_services(left),
            right: BrowserPaneController::with_services(right),
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
            export_pick: [None, None],
            copy: None,
            conflict: None,
            rename_notice: None,
            export: None,
            extract: None,
            checksums: None,
            detail: None,
            detail_serial: 0,
            detail_job: None,
            log: Vec::new(),
            show_log: false,
            show_help: false,
            partitions: None,
            implicit: None,
            askers: Vec::new(),
            single: false,
            quick_view: false,
            view_menu: false,
            follow: Default::default(),
            modes: Default::default(),
        };
        ctl.left.remember_style_as("commander-left");
        ctl.right.remember_style_as("commander-right");
        ctl
    }

    /// One pane configured by `profile` (the TUI Explorer), its jobs in `services.jobs`.
    pub fn single(services: PaneServices, keymap: KeyMap, profile: PaneProfile) -> Self {
        let spare = |s: &PaneServices| PaneServices {
            jobs: s.jobs.clone(),
            notify: Arc::clone(&s.notify),
            recent: Box::new(NoRecent),
        };
        let (left, right) = (spare(&services), spare(&services));
        let mut ctl = Self::new(left, right, keymap);
        // The host fixed the source, so only the view settings are kept, not recent files.
        ctl.left = BrowserPaneController::with_profile(
            PaneServices {
                jobs: ctl.jobs.clone(),
                notify: Arc::clone(&ctl.notify),
                recent: Box::new(PrefsOnly(services.recent)),
            },
            profile,
        );
        ctl.left.remember_style_as(profile.name);
        ctl.right = BrowserPaneController::with_profile(
            PaneServices::headless(),
            PaneProfile::commander().read_only(),
        );
        ctl.single = true;
        ctl.status = String::new();
        ctl
    }

    /// True for a one-pane screen; only [`Side::Left`] is drawn.
    pub fn is_single(&self) -> bool {
        self.single
    }

    /// The host opens the single pane's source: an image and this partition of it.
    pub fn open_fixed(&mut self, path: PathBuf, partition: Option<usize>) -> Vec<Effect> {
        let fx = self.left.open_fixed(path, partition);
        self.absorb(Side::Left, fx)
    }

    /// The host hands the single pane a ready session; `label` names it in the status line.
    pub fn open_fixed_session(
        &mut self,
        session: crate::model::browse_session::BrowseSession,
        label: &str,
    ) -> Vec<Effect> {
        let fx = self.left.open_fixed_session(session, label);
        self.absorb(Side::Left, fx)
    }

    /// The single pane, for a host screen that still stages through it directly (Inspect).
    pub fn host_pane(&mut self) -> &mut BrowserPaneController {
        &mut self.left
    }

    /// The single pane's file read for the text editor, handed over once.
    pub fn take_text_doc(&mut self) -> Option<crate::controller::browser::TextDoc> {
        self.left.take_text_doc()
    }

    /// The command a key press means here, for a view that shows its own chooser first (Export).
    pub fn command_for(&self, key: Key, mods: Mods) -> Option<Command> {
        self.keymap.lookup(key, mods)
    }

    /// One shared job registry, no wake, no recent files and the default keys: for tests and the CLI.
    pub fn headless() -> Self {
        Self::polled(
            Box::new(NoRecent),
            Box::new(NoRecent),
            KeyMap::for_this_platform(),
        )
    }

    /// One shared job registry and no wake, for a view that polls `tick` while [`Self::is_working`].
    pub fn polled(
        left_recent: Box<dyn RecentStore>,
        right_recent: Box<dyn RecentStore>,
        keymap: KeyMap,
    ) -> Self {
        let jobs = JobRegistry::new();
        let services = |recent| PaneServices {
            jobs: jobs.clone(),
            notify: Arc::new(NoNotify),
            recent,
        };
        Self::new(services(left_recent), services(right_recent), keymap)
    }

    // --- read accessors ---------------------------------------------------

    pub fn pane(&self, side: Side) -> &BrowserPaneController {
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

    /// Problems found loading the key map, for the status line and the session log.
    pub fn report_keymap_problems(&mut self, problems: Vec<String>) {
        for p in &problems {
            self.record_log(format!("keybindings: {p}"));
        }
        match problems.len() {
            0 => {}
            1 => self.status = format!("keybindings: {}", problems[0]),
            n => self.status = format!("keybindings: {n} problems (see the log)"),
        }
    }

    pub fn keep_dates(&self) -> bool {
        self.keep_dates
    }

    pub fn export_fork_mode(&self) -> ResourceForkMode {
        self.export_fork_mode
    }

    /// The active pane's export format: the user's pick for this volume, else its filesystem's default.
    pub fn export_format(&self) -> ExportFormat {
        self.export_format_for(self.active)
    }

    pub fn export_format_for(&self, side: Side) -> ExportFormat {
        let key = self.volume_key(side);
        match &self.export_pick[side.idx()] {
            Some((format, k)) if *k == key => *format,
            _ => default_export_format(&self.pane(side).volume().fs_type),
        }
    }

    /// Where `export_choices()` starts for the active pane: its export format (and fork mode).
    pub fn export_choice_index(&self) -> usize {
        let (format, fork) = (self.export_format(), self.export_fork_mode);
        let choices = export_choices();
        let exact = choices
            .iter()
            .position(|c| c.format == format && c.fork.unwrap_or(fork) == fork);
        exact
            .or_else(|| choices.iter().position(|c| c.format == format))
            .unwrap_or(0)
    }

    /// What a pick is remembered against: a refresh keeps it, another volume does not.
    fn volume_key(&self, side: Side) -> String {
        let p = self.pane(side);
        format!(
            "{:?}|{:?}|{}",
            p.source_path(),
            p.selected_partition(),
            p.volume().fs_type
        )
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

    /// The pane whose partition chooser is up, if one is.
    pub fn partitions_shown(&self) -> Option<Side> {
        self.partitions
    }

    /// Either pane holds staged edits that a close or quit would discard.
    pub fn has_staged_edits(&self) -> bool {
        !self.left.queue.is_empty() || !self.right.queue.is_empty()
    }

    /// True while any job runs (an open, apply, copy, export, wrapper or checksum), so a view polls.
    pub fn is_working(&self) -> bool {
        let pane_busy = |p: &BrowserPaneController| {
            let probing = p.volume_probing() || p.archive_working().is_some();
            p.is_busy() || p.wrapper_opening().is_some() || p.is_listing_folders() || probing
        };
        let hashing = self.checksums.as_ref().is_some_and(|c| c.job.is_some());
        let busy = self.copy.is_some() || hashing || self.detail_job.is_some();
        let busy = busy || self.modes_working();
        busy || self.follow_pending() || pane_busy(&self.left) || pane_busy(&self.right)
    }

    /// The first chord for `cmd` as this platform writes it, or "" when unbound.
    pub fn key_hint(&self, cmd: Command) -> String {
        self.keymap.hint(cmd)
    }

    /// Every command with its first chord on this platform, for the help window.
    pub fn key_help(&self) -> Vec<(&'static str, String)> {
        let rows = Command::ALL
            .iter()
            .map(|c| (c.label(), self.keymap.hint(*c)));
        rows.filter(|(_, k)| !k.is_empty()).collect()
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
        let cmd = self.keymap.lookup(key, mods);
        cmd.is_some_and(|c| !self.tree_command_idle(c))
    }

    /// The pane that asked the outstanding prompt with this `purpose`, if a pane did.
    pub fn asker(&self, purpose: &str) -> Option<Side> {
        let found = self.askers.iter().rev().find(|(p, _)| *p == purpose);
        found.map(|(_, s)| *s)
    }

    /// True while one of the screen's own reviews (or a pane's apply review or report) is up.
    pub fn modal_open(&self) -> bool {
        let pane_modal =
            |p: &BrowserPaneController| p.apply_review().is_some() || p.apply_report().is_some();
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

    fn pane_mut(&mut self, side: Side) -> &mut BrowserPaneController {
        match side {
            Side::Left => &mut self.left,
            Side::Right => &mut self.right,
        }
    }

    fn forward(&mut self, side: Side, action: BrowserPaneAction) -> Vec<Effect> {
        if matches!(action, BrowserPaneAction::SelectPartition(_)) {
            self.partitions = None;
        }
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
                Effect::OpenUrl(_)
                | Effect::CopyToClipboard(_)
                | Effect::CloseScreen
                | Effect::Quit => None,
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
        pane.apply(BrowserPaneAction::Reload);
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
        use BrowserPaneAction as P;
        let side = self.active;
        if let Some(was) = self.implicit.take() {
            if self.pane(was).selection_len() == 1 {
                self.forward(was, P::ClearSelection);
            }
        }
        if matches!(
            cmd,
            Command::Copy | Command::Delete | Command::Checksums | Command::Export
        ) && self
            .pane_mut(side)
            .select_cursor_if_none(cmd == Command::Delete)
        {
            self.implicit = Some(side);
        }
        let page = self.page_rows[side.idx()];
        let pane = self.pane(side);
        let remote_root = pane.is_remote_image() && pane.listing.at_root();
        let pane_action = match cmd {
            Command::CursorUp => Some(P::CursorUp),
            Command::CursorDown => Some(P::CursorDown),
            Command::PageUp => Some(P::CursorPage(-page)),
            Command::PageDown => Some(P::CursorPage(page)),
            Command::Home => Some(P::CursorHome),
            Command::End => Some(P::CursorEnd),
            Command::Open => Some(P::ActivateCursor),
            // At a remote image's root, going up leaves it for the daemon's host listing.
            Command::Parent if remote_root => Some(P::CloseRemoteImage),
            Command::Parent => Some(P::Up),
            Command::Mark => Some(P::ToggleMark),
            Command::Refresh => Some(P::Reload),
            Command::Info => Some(P::InfoAtCursor),
            Command::Rename => Some(P::RenameAtCursor),
            Command::NewFolder => Some(P::RequestNewFolder),
            Command::Apply if pane.can_apply() => Some(P::Apply),
            Command::Delete if self.can_delete() => Some(P::Delete),
            Command::OpenImage => Some(P::PickImage),
            Command::OpenFolder => Some(P::PickHostFolder),
            Command::OpenBackup => Some(P::PickBackupFolder),
            Command::Connect => Some(P::Connect),
            Command::Sort => Some(P::SortBy(next_sort(pane))),
            Command::Edits if !pane.queue.is_empty() => Some(P::ShowEdits(!pane.edits_shown())),
            Command::SwitchPane if self.single => Some(P::SwitchFocus),
            Command::Edit if pane.profile().tools.text_edit => Some(P::EditText),
            Command::Fsck => Some(P::Fsck),
            Command::Repair => Some(P::Repair),
            Command::Bless => Some(P::Bless),
            Command::Import => Some(P::Import),
            Command::Metadata => Some(P::EditMetadata),
            Command::Expand => Some(P::ExpandCursor),
            Command::Collapse => Some(P::CollapseCursor),
            Command::Apply
            | Command::Delete
            | Command::Edits
            | Command::Help
            | Command::SwitchPane
            | Command::Copy
            | Command::Edit
            | Command::Close
            | Command::Partition
            | Command::Checksums
            | Command::Export
            | Command::Log
            | Command::QuickView
            | Command::ViewMenu
            | Command::Transforms => None,
        };
        if let Some(action) = pane_action {
            let sort = matches!(action, P::SortBy(_));
            let fx = self.forward(side, action);
            if sort {
                self.status = sort_status(side, &self.pane(side).listing);
            }
            return fx;
        }
        match cmd {
            Command::Help => self.show_help = !self.show_help,
            Command::SwitchPane => return self.switch_pane(),
            Command::QuickView => self.toggle_quick_view(),
            Command::ViewMenu => self.view_menu = !self.view_menu,
            Command::Copy => {
                if self.can_copy(side) {
                    self.status = self.copy(side);
                }
            }
            Command::Close => return self.close_shortcut(),
            Command::Partition => self.toggle_partitions(side),
            Command::Checksums => self.status = self.start_checksums(side),
            Command::Export => return self.request_export(side, None),
            Command::Log => self.show_log = !self.show_log,
            Command::Edits => self.status = format!("[{}] No staged edits.", side.label()),
            // Apply and Delete land here only when refused; the view shows the transforms menu.
            Command::Apply | Command::Delete | Command::Transforms | Command::Edit => {}
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
            | Command::NewFolder
            | Command::OpenImage
            | Command::OpenFolder
            | Command::OpenBackup
            | Command::Connect
            | Command::Sort
            | Command::Fsck
            | Command::Repair
            | Command::Bless
            | Command::Import
            | Command::Metadata
            | Command::Expand
            | Command::Collapse => {}
        }
        Vec::new()
    }

    /// Tab: a pane with a folder tree takes it in two steps (tree, then list), then the other pane.
    fn switch_pane(&mut self) -> Vec<Effect> {
        let side = self.active;
        let pane = self.pane(side);
        if pane.has_folder_tree() && pane.tree_focused() {
            return self.forward(side, BrowserPaneAction::SwitchFocus);
        }
        self.active = side.other();
        let other = self.pane(self.active);
        if other.has_folder_tree() && !other.tree_focused() {
            return self.forward(self.active, BrowserPaneAction::SwitchFocus);
        }
        Vec::new()
    }

    /// Show `side`'s partition chooser, or hide it; a source with one volume has nothing to choose.
    fn toggle_partitions(&mut self, side: Side) {
        if self.partitions.is_some() {
            self.partitions = None;
        } else if self.pane(side).partition_labels().len() > 1 {
            self.partitions = Some(side);
        } else {
            self.status = format!("[{}] No other partition to choose.", side.label());
        }
    }

    /// Esc closes the topmost Commander window, then Commander itself.
    fn close_shortcut(&mut self) -> Vec<Effect> {
        let active = self.active;
        if self.show_help {
            self.show_help = false;
        } else if self.view_menu {
            self.view_menu = false;
        } else if self.partitions.is_some() {
            self.partitions = None;
        } else if self.pane(active).edits_shown() {
            return self.forward(active, BrowserPaneAction::ShowEdits(false));
        } else if self.detail.is_some() {
            self.close_detail();
        } else if self.checksums.is_some() {
            self.close_checksums();
        } else if self.show_log {
            self.show_log = false;
        } else if self.quick_view {
            self.toggle_quick_view();
        } else {
            return self.request_close(false);
        }
        Vec::new()
    }

    fn request_close(&mut self, quit: bool) -> Vec<Effect> {
        let n = self.left.queue.len() + self.right.queue.len();
        if n == 0 && self.single && !quit {
            return vec![Effect::Confirm(ConfirmSpec {
                purpose: purpose::CLOSE_UNSAVED,
                title: "Close".into(),
                message: format!("Close {}?", self.left.volume().name()),
                confirm_label: "Close".into(),
                destructive: false,
            })];
        }
        if n == 0 {
            return vec![close_effect(quit)];
        }
        let (purpose, label) = if quit {
            (purpose::QUIT_UNSAVED, "Discard & Quit")
        } else {
            (purpose::CLOSE_UNSAVED, "Discard & Close")
        };
        vec![Effect::Confirm(ConfirmSpec {
            purpose,
            title: "Unsaved staged edits".into(),
            message: format!(
                "{n} staged edit(s) across the panes have not been applied. \
                 Apply them per-pane first, or discard."
            ),
            confirm_label: label.into(),
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
        let Some(entry) = pane.detail_entry(&name) else {
            self.status = format!("[{}] could not open File Info for '{name}'.", side.label());
            return Vec::new();
        };
        let source = pane.detail_source();
        let at_root = !pane.tree_active() && !entry.path.trim_start_matches('/').contains('/');
        self.close_detail();
        let job = source.and_then(|source| {
            let entry = entry.clone();
            let body = move |_: &JobCtx| {
                entry_detail::read_detail(source, &entry, MAX_PREVIEW_SIZE, at_root)
            };
            let reads = self.reads(side);
            let notify = Arc::clone(&self.notify);
            self.jobs
                .spawn("Reading File Info", reads, notify, body)
                .ok()
        });
        self.status = format!("File Info: {}", entry.name);
        self.detail_serial += 1;
        self.detail = Some(FileDetail {
            side,
            entry,
            editable,
            fs_type,
            loading: job.is_some(),
            preview: None,
            rsrc_preview: None,
            xattrs: Vec::new(),
            id_names: IdNameMap::default(),
            size_plan: None,
            result: None,
            serial: self.detail_serial,
        });
        self.detail_job = job;
        Vec::new()
    }

    fn close_detail(&mut self) {
        if let Some(job) = self.detail_job.take() {
            job.cancel();
        }
        self.detail = None;
    }

    fn poll_detail(&mut self) {
        let Some(job) = self.detail_job.as_ref().filter(|j| !j.is_running()) else {
            return;
        };
        let extras = job.take_result();
        self.detail_job = None;
        let Some(d) = self.detail.as_mut() else {
            return;
        };
        d.loading = false;
        if let Some(x) = extras {
            d.preview = x.preview;
            d.rsrc_preview = x.rsrc_preview;
            d.xattrs = x.xattrs;
            d.id_names = x.id_names;
            d.size_plan = x.size_plan;
        }
    }

    fn close_checksums(&mut self) {
        if let Some(job) = self.checksums.take().and_then(|c| c.job) {
            job.cancel();
        }
    }

    fn stage_metadata(&mut self, edit: MetadataEdit) {
        let Some((side, editable)) = self.metadata_target() else {
            return;
        };
        let pane = self.pane_mut(side);
        let result = if !editable {
            "This entry is read-only here.".to_string()
        } else if pane.is_busy() {
            "Wait for the running apply to finish.".to_string()
        } else {
            pane.stage_metadata(edit)
        };
        self.set_metadata_result(result);
    }
}
