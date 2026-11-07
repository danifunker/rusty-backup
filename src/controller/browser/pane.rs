//! One browser pane's state: source, listing, staged-edit queue, expanded wrappers, selection and cursor.

use std::collections::HashMap;
use std::path::PathBuf;

use crate::model::commander_descend::classify_entry;
use crate::model::dir_listing::DirListing;
use crate::model::edit_queue::EditQueue;
use crate::model::wrapper_tree::{tree_range, TreeRow, WrapperTree};

use super::profile::{ListingStyle, PaneProfile};
use super::rows::{build_display_rows, DisplayRow, RowKind};
use crate::controller::{Controller, Effect};

mod archives;
mod edits;
mod expand;
mod labels;
mod remote;
pub(in crate::controller) mod source;
mod tools;
mod tree;
mod volume;

pub use crate::fs::replace::OnConflict;
pub use crate::model::archive_import::{ArchiveRow, TarPreview};
pub use crate::model::commander_ops::{ApplyPreflight, FailedEdit, MAX_CONSECUTIVE_FAILURES};
pub use crate::model::dir_listing::SortColumn;
pub use crate::model::text_edit::{LineEnding, TextEncoding, TextShape};
pub use archives::{ArchiveSave, ArchiveViewer, ForkFormat, ImportChoice, TarChoice};
pub use edits::{validate_entry_name, ApplyReport, CursorDetail, StageOutcome};
pub use labels::{RowLabel, RowTone};
pub use remote::validate_remote_addr;
pub use source::{
    purpose, DiscSource, NoRecent, PaneServices, PrefsOnly, RecentStore, SourceKind, VolumeInfo,
};
pub use tools::{
    format_mac_date, parse_mac_date, MetadataFields, MetadataForm, TextDoc, ToolReport,
    MAX_TEXT_EDIT,
};
pub use tree::TreeNode;
pub use volume::{
    journal_block_class, journal_status, journal_summary, journal_txn_blocks, journal_txn_classes,
    journal_txn_rows, ArchiveRead, ArchiveUse, CheckSummary, JournalDetail, JournalTone,
    MacArchiveKind, RepairSummary, TreeText,
};

/// What a user does to one pane: navigation, selection, sort, sources and edits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrowserPaneAction {
    /// Enter the child directory with this name.
    Enter(String),
    Up,
    NavigateTo(String),
    Reload,
    /// Click a base-listing row by name; takes the selection over from the wrapper tree.
    Click {
        name: String,
        ctrl: bool,
        shift: bool,
    },
    /// Click a row below an expanded wrapper or folder; takes the selection over from the listing.
    TreeClick {
        node_id: String,
        ctrl: bool,
        shift: bool,
    },
    /// Right-click a base row: select just it unless it is already selected.
    ContextSelect(String),
    /// Right-click a row below an expanded wrapper or folder: select just it unless already selected.
    TreeContextSelect(String),
    /// Toggle the cursor row in or out of the selection, then move down.
    ToggleMark,
    /// Toggle the row with this node id in or out of the selection; the cursor stays (a checkbox).
    Mark(String),
    ClearSelection,
    SortBy(SortColumn),
    CursorUp,
    CursorDown,
    /// Move the cursor by this many rows (negative is up), clamped.
    CursorPage(i32),
    CursorHome,
    CursorEnd,
    /// Put the cursor on the row with this node id, leaving the selection as it is.
    CursorTo(String),
    /// What double-click does, on the cursor row.
    ActivateCursor,
    /// What double-click does, on the row with this node id (`""` is `..`).
    Activate(String),
    /// Select the cursor row and ask for its File Info.
    InfoAtCursor,
    /// Expand or collapse the wrapper or inner folder with this node id.
    ToggleWrapper(String),
    /// Expand or collapse any expandable row: a folder of the combined tree, a wrapper, its folders.
    ToggleExpand(String),
    /// Right: expand the cursor row (a list beside a folder tree opens the folder instead).
    ExpandCursor,
    /// Left: collapse the cursor row or step to its parent row (a plain list goes up).
    CollapseCursor,
    /// Show the volume as one tree, a flat folder list, or a folder tree beside the list.
    SetListingStyle(ListingStyle),
    /// Click this row of the folder tree: show that folder and give the tree the keyboard.
    FolderTreeClick(usize),
    /// Expand or collapse this row of the folder tree.
    FolderTreeToggle(usize),
    /// Open a disk image, Mac archive or optical disc image.
    OpenImage(PathBuf),
    OpenHostFolder(PathBuf),
    /// Open a native rusty-backup or Clonezilla backup folder.
    OpenBackupFolder(PathBuf),
    /// A dropped or recent path: a folder opens as a host folder, a file as an image.
    OpenPath(PathBuf),
    /// Open a disc image or the disc in a drive, browsed by its filesystems.
    OpenDisc(DiscSource),
    PickImage,
    PickHostFolder,
    PickBackupFolder,
    /// The answer to a `PickFile` / `PickFolder` this pane asked for; `None` is cancel.
    Picked {
        purpose: &'static str,
        path: Option<PathBuf>,
    },
    /// Browse this partition (or this filesystem of an optical disc).
    SelectPartition(usize),
    CloseSource,
    /// Stop the open in progress.
    CancelOpen,
    /// Retry a locked source's open with this password or passphrase.
    SubmitPassword(String),
    /// The answer to a `Confirm` this pane asked for.
    Confirmed {
        purpose: &'static str,
        yes: bool,
    },
    /// The persisted recent lists were cleared elsewhere.
    RecentCleared,
    /// Toggle a staged delete on the selection; a host pane asks, then deletes now.
    Delete,
    /// Drop the staged rename of the entry with this name (a combined-tree row: its node id).
    CancelRename(String),
    /// Ask for a new name for the entry with this name.
    RequestRename(String),
    /// Select the cursor row and ask for its new name.
    RenameAtCursor,
    RequestNewFolder,
    /// The answer to an `AskText` this pane asked for; `None` is cancel.
    Answered {
        purpose: &'static str,
        text: Option<String>,
    },
    /// Drop every staged edit, after a confirmation.
    Discard,
    /// Drop the staged edit at this queue index.
    Unstage(usize),
    ShowEdits(bool),
    /// Check the staged edits, then write them; a review comes first if anything needs a decision.
    Apply,
    /// Write after the review, settling name conflicts this way.
    ApplyReviewed(OnConflict),
    DismissReview,
    DismissReport,
    /// Ask for an `rb-cli serve` daemon's address, then browse its host filesystem.
    Connect,
    /// Open this file of the remote host listing (by name) as a disk image, on the same connection.
    OpenRemoteImage(String),
    /// Leave the remote image for the host folder it was opened from.
    CloseRemoteImage,
    /// Drop the remote connection (asks first when edits are staged).
    Disconnect,
    /// Hand the keyboard between the folder tree and the listing (profile `folder_tree`).
    SwitchFocus,
    /// Check the open filesystem in a job; the result waits in `tool_report`.
    Fsck,
    /// Repair the open filesystem in place, after a confirmation.
    Repair,
    DismissToolReport,
    /// List the whole volume as a text tree in a job (with filesystem ids when `ids`).
    ListTree {
        ids: bool,
    },
    /// Read the HFS+ journal in a job; it waits in `take_journal`.
    ShowJournal,
    /// Pick a bootable donor disk and stage its HFS boot blocks.
    BootBlocks,
    /// Find out in a job whether the file at this path is a Mac archive (`archive_sniff`).
    SniffArchive(String),
    /// Read and decode the Mac archive at `path` in a job; it waits in `take_archive`.
    ReadArchive {
        path: String,
        purpose: ArchiveUse,
    },
    /// Bless the cursor folder (or the current one) as the System Folder, after a confirmation.
    Bless,
    /// Stage the cursor entry's ProDOS access byte: locked ($21, read + backup) or unlocked ($C3).
    SetProdosLock(bool),
    /// Pick a host file and write the last tree listing to it in a job.
    SaveTree,
    /// Read the cursor file for the text editor in a job; it waits in `take_text_doc`.
    EditText,
    /// Write the editor's text back over the file `name` in `folder` (the `TextDoc`'s).
    SaveText {
        name: String,
        folder: String,
        text: String,
        shape: TextShape,
    },
    /// Pick a host file and stage it into the current folder.
    Import,
    /// Stage these host files and folders into `folder` (a path), or the target folder when `None`.
    AddHostPaths {
        paths: Vec<PathBuf>,
        folder: Option<String>,
    },
    /// Close the list of host items that could not be staged.
    DismissStagingErrors,
    /// Stage host picks and drops into the target folder; a tar (or, on a Mac volume, a Mac
    /// archive) waits for `ChooseTar` / `ChooseImport` first.
    AddPicked(Vec<PathBuf>),
    /// Answer the Mac archive at the front of `pending_import`.
    ChooseImport(ImportChoice),
    /// Answer `pending_tar`.
    ChooseTar(TarChoice),
    /// Pick a tar and import its contents into the target folder (asking first when it is lossy).
    ImportTar,
    /// Go on with (or drop) the tar import whose preflight is in `tar_preview`.
    ConfirmTarImport(bool),
    /// Toggle entry `i` of the archive viewer.
    ArchiveMark(usize),
    /// The fork container the archive viewer (`viewer`) or the save dialog extracts into.
    ArchiveFormat {
        viewer: bool,
        format: ForkFormat,
    },
    /// Pick a folder and extract the viewer's entries: all, or the marked ones.
    ArchiveExtract {
        all: bool,
    },
    CloseArchive,
    /// Pick where the save dialog's archive goes: as stored (`raw`), or decoded into a folder.
    SaveArchive {
        raw: bool,
    },
    CancelSaveArchive,
    /// Open the metadata form for the cursor row.
    EditMetadata,
    SubmitMetadata(Box<MetadataFields>),
    CancelMetadata,
}

/// A pane's model state. Fields stay public while the shells move onto actions slice by slice.
pub struct BrowserPaneController {
    pub listing: DirListing,
    /// Edits staged against this pane's volume until Apply.
    pub queue: EditQueue,
    /// Archives and disk images expanded in place under their row.
    pub wrapper_tree: WrapperTree,
    /// Selected wrapper-tree node ids; exclusive with the listing's own selection.
    pub tree_selected: Vec<String>,
    /// Shift-click anchor for a range over tree rows.
    pub tree_anchor: Option<String>,
    /// Node id -> tree row for the rows last built, so a click resolves to its entry.
    pub tree_index: HashMap<String, TreeRow>,
    /// The rows last built, which cursor moves and activation work against.
    rows: Vec<DisplayRow>,
    remote_host: bool,
    /// Cursor row by node id, so it survives row rebuilds; `cursor_at` is the fallback.
    cursor: Option<String>,
    cursor_at: usize,
    /// Bumped whenever expanded wrappers are dropped, so a view can discard a stale open.
    wrapper_epoch: u64,
    error: Option<String>,
    /// File Info asked for by name (double-click, F3); the two-pane controller opens it.
    info_request: Option<String>,
    /// Effects raised inside navigation helpers, returned at the end of `apply` / `tick`.
    out: Vec<Effect>,
    /// What is open and how: path, partitions, session, open job, pending switch.
    src: source::SourceState,
    /// Staged-edit review, apply, host writes, prompts and the wrapper job.
    ed: edits::EditState,
    /// The `rb-cli serve` connection and its open / close job.
    remote: remote::RemoteState,
    /// What this use of the pane may do; checked before every action.
    profile: PaneProfile,
    /// Check / repair / editor jobs and their results.
    tl: tools::ToolState,
    /// The folder tree beside the listing, in the `TreeAndList` style.
    folders: tree::FolderTree,
    style: ListingStyle,
    /// The view name the chosen style is remembered under, when the host set one.
    style_key: Option<String>,
    /// The combined tree's expanded folders.
    dirs: expand::Folders,
    /// The tree listing, journal and boot-block tools.
    vol: volume::VolumeState,
    /// The archive viewer, save dialog and archive imports.
    arc: archives::ArchiveState,
}

impl Default for BrowserPaneController {
    fn default() -> Self {
        Self::new()
    }
}

impl Controller for BrowserPaneController {
    type Action = BrowserPaneAction;

    fn apply(&mut self, action: BrowserPaneAction) -> Vec<Effect> {
        self.error = None;
        if let Some(why) = self.refused(&action) {
            return vec![Effect::Message(why)];
        }
        self.rebuild_rows();
        let staged_before = self.queue.len();
        let mut fx = Vec::new();
        if self.tree_key(&action).is_some() {
            return self.finish_apply(fx, staged_before);
        }
        if matches!(
            action,
            BrowserPaneAction::Click { .. }
                | BrowserPaneAction::TreeClick { .. }
                | BrowserPaneAction::ContextSelect(_)
                | BrowserPaneAction::TreeContextSelect(_)
                | BrowserPaneAction::Activate(_)
        ) {
            self.focus_list();
        }
        match action {
            BrowserPaneAction::SwitchFocus => self.switch_focus(),
            BrowserPaneAction::Enter(name) => self.enter(&name),
            BrowserPaneAction::Up => self.up(),
            BrowserPaneAction::NavigateTo(path) => {
                let before = self.listing.cwd_path().to_string();
                if let Err(e) = self.listing.navigate_to(&path) {
                    self.error = Some(format!("cannot open '{path}': {e}"));
                }
                self.navigated(&before, None);
            }
            BrowserPaneAction::Reload => {
                self.folders.clear();
                self.folders_reloaded();
                if let Err(e) = self.listing.reload() {
                    self.error = Some(format!("cannot refresh: {e}"));
                }
            }
            BrowserPaneAction::Click { name, ctrl, shift } => {
                self.tree_selected.clear();
                if !(ctrl && self.mixes_levels()) {
                    self.clear_folder_selection();
                }
                if shift {
                    self.listing.shift_click(&name);
                } else if ctrl {
                    self.listing.ctrl_click(&name);
                } else {
                    self.listing.click(&name);
                }
                self.cursor_to_base(&name);
            }
            BrowserPaneAction::TreeClick {
                node_id,
                ctrl,
                shift,
            } => self.tree_click(node_id, ctrl, shift),
            BrowserPaneAction::ContextSelect(name) => self.context_select(&name),
            BrowserPaneAction::TreeContextSelect(node_id) => self.tree_context_select(node_id),
            BrowserPaneAction::ToggleMark => self.toggle_mark(),
            BrowserPaneAction::Mark(node_id) => self.mark(&node_id),
            BrowserPaneAction::ClearSelection => {
                self.listing.clear_selection();
                self.tree_selected.clear();
                self.clear_folder_selection();
            }
            BrowserPaneAction::SortBy(col) => {
                self.listing.resort(col);
                self.resort_folders();
            }
            BrowserPaneAction::CursorUp => self.move_cursor(-1),
            BrowserPaneAction::CursorDown => self.move_cursor(1),
            BrowserPaneAction::CursorPage(n) => self.move_cursor(i64::from(n)),
            BrowserPaneAction::CursorHome => self.set_cursor_index(0),
            BrowserPaneAction::CursorEnd => {
                let last = self.rows.len().saturating_sub(1);
                self.set_cursor_index(last);
            }
            BrowserPaneAction::CursorTo(node_id) => {
                if let Some(i) = self.rows.iter().position(|r| r.node_id == node_id) {
                    self.set_cursor_index(i);
                }
            }
            BrowserPaneAction::ActivateCursor => {
                if let Some(row) = self.cursor_row() {
                    self.activate(row);
                }
            }
            BrowserPaneAction::Activate(node_id) => {
                if let Some(row) = self.rows.iter().find(|r| r.node_id == node_id).cloned() {
                    self.activate(row);
                }
            }
            BrowserPaneAction::InfoAtCursor => self.info_at_cursor(),
            BrowserPaneAction::ToggleWrapper(node_id) => self.toggle_wrapper(&node_id),
            BrowserPaneAction::ToggleExpand(node_id) => self.toggle_expand(&node_id),
            BrowserPaneAction::ExpandCursor => self.expand_cursor(),
            BrowserPaneAction::CollapseCursor => self.collapse_cursor(),
            BrowserPaneAction::SetListingStyle(style) => fx = self.set_listing_style(style),
            BrowserPaneAction::FolderTreeClick(i) => self.folder_tree_click(i),
            BrowserPaneAction::FolderTreeToggle(i) => self.folder_tree_toggle(i),
            other => {
                let handled = self
                    .apply_remote(other)
                    .or_else(|a| self.apply_edit(a))
                    .or_else(|a| self.apply_tool(a))
                    .or_else(|a| self.apply_archive(a));
                fx = match handled {
                    Ok(fx) => fx,
                    Err(other) => self.apply_source(other),
                }
            }
        }
        self.finish_apply(fx, staged_before)
    }

    fn tick(&mut self) -> Vec<Effect> {
        let mut fx = self.poll_open();
        fx.extend(self.poll_remote());
        fx.extend(self.poll_edits());
        fx.extend(self.poll_tools());
        fx.extend(self.poll_volume());
        fx.extend(self.poll_archives());
        self.poll_folders();
        self.sync_tree();
        self.rebuild_rows();
        self.pump_folders();
        fx.append(&mut self.out);
        fx
    }
}

impl BrowserPaneController {
    /// A pane with no recent files, its own job registry and no wake: for tests and the CLI.
    pub fn new() -> Self {
        Self::with_services(PaneServices::headless())
    }

    /// A Commander pane whose jobs register in `services.jobs` and wake the shell through `services.notify`.
    pub fn with_services(services: PaneServices) -> Self {
        Self::with_profile(services, PaneProfile::commander())
    }

    /// A pane configured for one use (Commander, the TUI Explorer, the Inspect browse view).
    pub fn with_profile(services: PaneServices, profile: PaneProfile) -> Self {
        Self {
            listing: DirListing::new(),
            queue: EditQueue::new(),
            wrapper_tree: WrapperTree::new(),
            tree_selected: Vec::new(),
            tree_anchor: None,
            tree_index: HashMap::new(),
            rows: Vec::new(),
            remote_host: false,
            cursor: None,
            cursor_at: 0,
            wrapper_epoch: 0,
            error: None,
            info_request: None,
            out: Vec::new(),
            src: source::SourceState::new(services),
            ed: edits::EditState::new(),
            remote: remote::RemoteState::default(),
            profile,
            tl: tools::ToolState::default(),
            folders: tree::FolderTree::new(true),
            style: profile.listing,
            style_key: None,
            dirs: expand::Folders::default(),
            vol: volume::VolumeState::default(),
            arc: archives::ArchiveState::default(),
        }
    }

    pub fn profile(&self) -> &PaneProfile {
        &self.profile
    }

    /// Why the profile refuses `action`, or `None` when it may run.
    fn refused(&self, action: &BrowserPaneAction) -> Option<String> {
        use BrowserPaneAction as A;
        let p = &self.profile;
        let (allowed, what) = match action {
            A::OpenImage(_) | A::PickImage | A::OpenDisc(_) => {
                (p.sources.image, "opening an image")
            }
            A::OpenHostFolder(_) | A::PickHostFolder => (p.sources.host, "opening a folder"),
            A::OpenBackupFolder(_) | A::PickBackupFolder => (p.sources.backup, "opening a backup"),
            A::OpenPath(_) => (p.sources.image || p.sources.host, "opening another source"),
            A::Connect | A::OpenRemoteImage(_) => (p.sources.remote, "remote browsing"),
            // A disc's filesystems are its own choices, even when the host fixed the disc.
            A::SelectPartition(_) => (!p.fixed_source || self.is_optical(), "changing the source"),
            A::CloseSource => (!p.fixed_source, "changing the source"),
            A::Delete => (p.can_delete(), "delete"),
            A::RequestRename(_) | A::RenameAtCursor => (p.can_rename(), "rename"),
            A::RequestNewFolder => (p.can_new_folder(), "new folder"),
            A::ToggleWrapper(_) => (p.wrappers, "expanding archives and images"),
            A::SwitchFocus | A::FolderTreeClick(_) | A::FolderTreeToggle(_) => {
                (self.has_folder_tree(), "the folder tree")
            }
            A::Fsck => (p.tools.fsck, "checking the filesystem"),
            A::Repair => (p.tools.repair && !p.read_only, "repair"),
            A::Bless | A::BootBlocks => (p.tools.bless && !p.read_only, "bless"),
            A::SetProdosLock(_) => (p.tools.metadata && !p.read_only, "locking"),
            A::ShowJournal => (p.tools.fsck, "the journal"),
            A::EditText | A::SaveText { .. } => (p.tools.text_edit && !p.read_only, "editing text"),
            A::Import
            | A::AddHostPaths { .. }
            | A::AddPicked(_)
            | A::ChooseImport(_)
            | A::ChooseTar(_)
            | A::ImportTar
            | A::ConfirmTarImport(_) => (p.tools.import && !p.read_only, "import"),
            A::EditMetadata | A::SubmitMetadata(_) => {
                (p.tools.metadata && !p.read_only, "editing metadata")
            }
            _ => (true, ""),
        };
        (!allowed).then(|| p.refusal(what))
    }

    fn finish_apply(&mut self, mut fx: Vec<Effect>, staged_before: usize) -> Vec<Effect> {
        self.password_error_locks();
        if self.queue.len() > staged_before {
            self.auto_apply();
        }
        self.sync_tree();
        self.rebuild_rows();
        self.pump_folders();
        fx.append(&mut self.out);
        fx
    }

    /// Without staging, what was just staged applies at once (through the same preflight and review).
    fn auto_apply(&mut self) {
        if !self.profile.staging {
            let fx = self.start_check();
            self.out.extend(fx);
        }
    }

    /// The rows to draw, rebuilding [`Self::tree_index`].
    pub fn display_rows(&mut self) -> Vec<DisplayRow> {
        self.rebuild_rows();
        self.rows.clone()
    }

    /// The rows as last built.
    pub fn rows(&self) -> &[DisplayRow] {
        &self.rows
    }

    /// Index of the cursor row in [`Self::rows`], or `None` when there are no rows.
    pub fn cursor_index(&self) -> Option<usize> {
        if self.rows.is_empty() {
            return None;
        }
        let by_id = self
            .cursor
            .as_ref()
            .and_then(|id| self.rows.iter().position(|r| &r.node_id == id));
        Some(by_id.unwrap_or_else(|| self.cursor_at.min(self.rows.len() - 1)))
    }

    /// The last listing error from an action, such as a folder that would not open.
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    pub fn take_error(&mut self) -> Option<String> {
        self.error.take()
    }

    pub(in crate::controller) fn take_info_request(&mut self) -> Option<String> {
        self.info_request.take()
    }

    /// Changes whenever expanded wrappers are dropped, so an in-flight open can tell it is stale.
    pub fn wrapper_epoch(&self) -> u64 {
        self.wrapper_epoch
    }

    /// True when the selection is inside an expanded wrapper rather than the listing.
    pub fn tree_active(&self) -> bool {
        !self.tree_selected.is_empty()
    }

    /// Drops expanded wrappers, the tree selection and the cursor after the view swaps the source.
    pub fn source_changed(&mut self) {
        self.collapse_wrappers();
        self.folders.clear();
        self.folders_source_changed();
        self.cursor = None;
        self.cursor_at = 0;
        self.error = None;
        self.rows.clear();
    }

    /// The cwd as `(label, target)` crumbs; `target` is `None` above the root the pane can reach.
    pub fn path_crumbs(&self) -> Vec<(String, Option<String>)> {
        let path = self.listing.cwd_path();
        let root = self
            .listing
            .root_entry()
            .map(|e| e.path)
            .unwrap_or_default();
        let host = self.listing.is_host();
        let reachable = |t: &str| t.starts_with(root.as_str()).then(|| t.to_string());
        let mut out = Vec::new();
        let mut start = 0;
        for (i, c) in path.char_indices() {
            if c == '/' || (host && c == std::path::MAIN_SEPARATOR) {
                let end = i + c.len_utf8();
                let target = if start == 0 { &path[..end] } else { &path[..i] };
                let target = if target.len() < root.len() {
                    None
                } else {
                    reachable(target)
                };
                out.push((path[start..end].to_string(), target));
                start = end;
            }
        }
        if start < path.len() {
            out.push((path[start..].to_string(), Some(path.to_string())));
        }
        out
    }

    pub(in crate::controller) fn rebuild_rows(&mut self) {
        // A remote host listing tints images and never expands wrappers.
        self.remote_host = self.is_remote_host();
        self.rows = build_display_rows(
            &self.listing,
            &self.queue,
            &mut self.wrapper_tree,
            &mut self.tree_index,
            self.remote_host,
        );
        if !self.profile.wrappers {
            for r in self.rows.iter_mut().filter(|r| r.is_wrapper) {
                r.is_wrapper = false;
                r.expandable = false;
            }
        }
        self.splice_folder_rows();
    }

    fn collapse_wrappers(&mut self) {
        self.wrapper_tree.clear();
        self.tree_selected.clear();
        self.tree_anchor = None;
        self.tree_index.clear();
        self.wrapper_epoch += 1;
        self.ed.cancel_wrapper();
    }

    /// Wrappers are scoped to one directory, so leaving it collapses them.
    fn navigated(&mut self, before: &str, cursor: Option<String>) {
        if self.listing.cwd_path() != before {
            self.collapse_wrappers();
            self.clear_folder_selection();
            self.cursor = cursor;
            self.cursor_at = 0;
        }
    }

    fn enter(&mut self, name: &str) {
        let before = self.listing.cwd_path().to_string();
        match self.listing.enter(name) {
            Ok(()) => self.navigated(&before, None),
            Err(e) => self.error = Some(format!("cannot open '{name}': {e}")),
        }
    }

    /// Goes up and puts the cursor on the folder just left.
    fn up(&mut self) {
        let before = self.listing.cwd_path().to_string();
        self.listing.up();
        self.navigated(&before, Some(before.clone()));
    }

    fn tree_click(&mut self, node_id: String, ctrl: bool, shift: bool) {
        if self.dirs.index.contains_key(&node_id) {
            return self.folder_click(node_id, ctrl, shift);
        }
        self.listing.clear_selection();
        self.clear_folder_selection();
        self.cursor = Some(node_id.clone());
        let anchor = self
            .tree_anchor
            .clone()
            .filter(|a| self.tree_index.contains_key(a));
        if let (true, Some(anchor)) = (shift, anchor) {
            let visible: Vec<TreeRow> = self
                .rows
                .iter()
                .filter(|r| r.is_tree)
                .filter_map(|r| self.tree_index.get(&r.node_id).cloned())
                .collect();
            self.tree_selected = tree_range(&visible, &anchor, &node_id);
        } else if ctrl {
            self.toggle_tree_selected(&node_id);
            self.tree_anchor = Some(node_id);
        } else {
            self.tree_selected = vec![node_id.clone()];
            self.tree_anchor = Some(node_id);
        }
    }

    fn toggle_tree_selected(&mut self, node_id: &str) {
        if let Some(pos) = self.tree_selected.iter().position(|n| n == node_id) {
            self.tree_selected.remove(pos);
        } else {
            self.tree_selected.push(node_id.to_string());
        }
    }

    fn context_select(&mut self, name: &str) {
        if !self.listing.is_selected(name) {
            self.tree_selected.clear();
            self.clear_folder_selection();
            self.listing.click(name);
        }
        self.cursor_to_base(name);
    }

    fn tree_context_select(&mut self, node_id: String) {
        if self.dirs.index.contains_key(&node_id) {
            return self.folder_context_select(node_id);
        }
        if !self.tree_selected.contains(&node_id) {
            self.clear_folder_selection();
            self.listing.clear_selection();
            self.tree_selected = vec![node_id.clone()];
        }
        self.cursor = Some(node_id);
    }

    fn toggle_mark(&mut self) {
        let Some(row) = self.cursor_row() else {
            return;
        };
        self.toggle_row_mark(row);
        self.move_cursor(1);
    }

    fn mark(&mut self, node_id: &str) {
        if let Some(row) = self.rows.iter().find(|r| r.node_id == node_id).cloned() {
            self.toggle_row_mark(row);
        }
    }

    /// Toggle one row's mark; the combined tree keeps top-level and deep marks together.
    fn toggle_row_mark(&mut self, row: DisplayRow) {
        if row.in_folder {
            self.toggle_folder_selected(&row.node_id);
        } else if row.is_tree {
            self.listing.clear_selection();
            self.clear_folder_selection();
            self.toggle_tree_selected(&row.node_id);
            self.tree_anchor = Some(row.node_id);
        } else if !row.is_parent() {
            self.tree_selected.clear();
            if !self.mixes_levels() {
                self.clear_folder_selection();
            }
            self.listing.ctrl_click(&row.name);
        }
    }

    fn info_at_cursor(&mut self) {
        let Some(row) = self.cursor_row() else {
            return;
        };
        if row.is_parent() || row.kind == RowKind::PendingAdd {
            return;
        }
        if row.is_tree || row.in_folder {
            self.tree_context_select(row.node_id);
        } else {
            self.context_select(&row.name);
        }
        self.info_request = Some(row.name);
    }

    /// Double-click: `..` goes up, a folder opens (expands in the combined tree), a wrapper
    /// toggles, a file shows its info.
    fn activate(&mut self, row: DisplayRow) {
        if row.is_parent() {
            self.up();
        } else if row.expandable && (row.in_folder || (row.is_dir && self.combined())) {
            self.toggle_expand(&row.node_id);
        } else if row.in_folder {
            self.folder_context_select(row.node_id);
            self.info_request = Some(row.name);
        } else if row.is_tree {
            if row.expandable {
                self.toggle_wrapper(&row.node_id);
            } else {
                self.tree_click(row.node_id, false, false);
            }
        } else if row.is_dir {
            self.enter(&row.name);
        } else if row.is_wrapper {
            self.toggle_wrapper(&row.node_id);
        } else if self.remote_host {
            let fx = self.open_remote_image(&row.name);
            self.out.extend(fx);
        } else {
            self.context_select(&row.name);
            self.info_request = Some(row.name);
        }
    }

    fn toggle_wrapper(&mut self, node_id: &str) {
        if self.wrapper_tree.is_expanded(node_id) {
            self.wrapper_tree.collapse(node_id);
            self.tree_selected.retain(|id| !id.starts_with(node_id));
            return;
        }
        let Some(row) = self.tree_index.get(node_id).cloned() else {
            // A base row: opening it can take seconds, so it runs as a job.
            if !self.remote_host {
                self.open_base_wrapper(node_id);
            }
            return;
        };
        let name = row.entry.name.clone();
        if row.is_wrapper {
            let Some(kind) = classify_entry(&row.entry) else {
                return;
            };
            let opened = self
                .wrapper_tree
                .read_wrapper_source(&row)
                .and_then(|src| self.wrapper_tree.expand_wrapper(node_id, &name, kind, src));
            if let Err(e) = opened {
                self.wrapper_failed(&name, format!("{e:#}"));
                return;
            }
        } else {
            self.wrapper_tree.expand_folder(node_id);
        }
        if let Some(note) = self.wrapper_tree.note(node_id) {
            self.out.push(Effect::Message(format!("{name}: {note}")));
        }
    }

    /// Select the cursor row for a key command when nothing is selected; true when it did.
    pub(in crate::controller) fn select_cursor_if_none(&mut self, pending_ok: bool) -> bool {
        let Some(row) = self.cursor_row().filter(|_| !self.has_selection()) else {
            return false;
        };
        if row.is_parent() || (!pending_ok && row.kind == RowKind::PendingAdd) {
            return false;
        }
        if row.is_tree || row.in_folder {
            self.tree_context_select(row.node_id);
        } else {
            self.context_select(&row.name);
        }
        true
    }

    /// The number of selected rows, listing, wrapper tree or combined tree.
    pub(in crate::controller) fn selection_len(&self) -> usize {
        self.tree_selected.len() + self.listing.selection().len() + self.dirs.selected.len()
    }

    fn cursor_row(&self) -> Option<DisplayRow> {
        self.cursor_index().map(|i| self.rows[i].clone())
    }

    fn cursor_to_base(&mut self, name: &str) {
        if let Some(i) = self
            .rows
            .iter()
            .position(|r| !r.is_tree && !r.is_parent() && r.name == name)
        {
            self.set_cursor_index(i);
        }
    }

    fn set_cursor_index(&mut self, i: usize) {
        if let Some(row) = self.rows.get(i) {
            self.cursor = Some(row.node_id.clone());
            self.cursor_at = i;
        }
    }

    fn move_cursor(&mut self, delta: i64) {
        let Some(cur) = self.cursor_index() else {
            return;
        };
        let last = self.rows.len() as i64 - 1;
        let to = (cur as i64 + delta).max(0).min(last);
        self.set_cursor_index(to as usize);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::commander::RowKind;
    use crate::model::edit_queue::StagedEdit;

    fn host_pane(files: &[&str], dirs: &[&str]) -> (tempfile::TempDir, BrowserPaneController) {
        let dir = tempfile::tempdir().unwrap();
        for f in files {
            std::fs::write(dir.path().join(f), b"x").unwrap();
        }
        for d in dirs {
            std::fs::create_dir(dir.path().join(d)).unwrap();
        }
        let mut vm = BrowserPaneController::new();
        vm.listing.load_host_root(dir.path().to_path_buf()).unwrap();
        (dir, vm)
    }

    fn row<'a>(rows: &'a [DisplayRow], name: &str) -> &'a DisplayRow {
        rows.iter().find(|r| r.name == name).unwrap()
    }

    #[test]
    fn rows_list_the_directory() {
        let (_d, mut vm) = host_pane(&["a.txt", "b.txt"], &["sub"]);
        let rows = vm.display_rows();
        let names: Vec<&str> = rows.iter().map(|r| r.name.as_str()).collect();
        for n in ["a.txt", "b.txt", "sub"] {
            assert!(names.contains(&n), "{names:?}");
        }
        assert!(row(&rows, "sub").is_dir);
        assert_eq!(row(&rows, "a.txt").kind, RowKind::Normal);
    }

    #[test]
    fn staged_edits_overlay_the_rows() {
        let (_d, mut vm) = host_pane(&["gone.txt", "keep.txt"], &[]);
        let root = vm.listing.cwd().unwrap().clone();
        let gone = vm
            .listing
            .entries()
            .iter()
            .find(|e| e.name == "gone.txt")
            .unwrap()
            .clone();
        vm.queue.push(StagedEdit::DeleteEntry {
            parent: root.clone(),
            entry: gone,
        });
        vm.queue.push(StagedEdit::CreateDirectory {
            parent: root,
            name: "new-dir".into(),
            qnx2_attrs: None,
        });
        let rows = vm.display_rows();
        assert_eq!(row(&rows, "gone.txt").kind, RowKind::PendingDelete);
        assert_eq!(row(&rows, "keep.txt").kind, RowKind::Normal);
        assert_eq!(row(&rows, "new-dir").kind, RowKind::PendingAdd);
    }

    #[test]
    fn a_remote_host_listing_tints_images_and_never_wraps() {
        let (_d, mut vm) = host_pane(&["disk.img", "notes.txt"], &[]);
        let rows = build_display_rows(
            &vm.listing,
            &vm.queue,
            &mut vm.wrapper_tree,
            &mut vm.tree_index,
            true,
        );
        let img = row(&rows, "disk.img");
        assert!(img.is_image && !img.is_wrapper && !img.expandable);
        assert!(!row(&rows, "notes.txt").is_image);
    }

    fn names_selected(vm: &BrowserPaneController) -> Vec<String> {
        vm.listing.selection().to_vec()
    }

    fn click(name: &str, ctrl: bool, shift: bool) -> BrowserPaneAction {
        BrowserPaneAction::Click {
            name: name.into(),
            ctrl,
            shift,
        }
    }

    fn cursor_name(vm: &BrowserPaneController) -> String {
        vm.rows()[vm.cursor_index().unwrap()].name.clone()
    }

    #[test]
    fn enter_up_and_navigate_to() {
        let (d, mut vm) = host_pane(&[], &["sub"]);
        std::fs::create_dir(d.path().join("sub").join("deeper")).unwrap();
        let root = vm.listing.cwd_path().to_string();
        vm.apply(BrowserPaneAction::Enter("sub".into()));
        assert!(vm.listing.cwd_path().ends_with("sub"));
        assert!(vm.error().is_none());
        vm.apply(BrowserPaneAction::Up);
        assert_eq!(vm.listing.cwd_path(), root);
        // The cursor lands on the folder just left.
        assert_eq!(cursor_name(&vm), "sub");
        let deeper = d.path().join("sub").join("deeper");
        vm.apply(BrowserPaneAction::NavigateTo(
            deeper.to_string_lossy().to_string(),
        ));
        assert!(
            vm.listing.cwd_path().ends_with("deeper"),
            "{}",
            vm.listing.cwd_path()
        );
    }

    #[test]
    fn a_failed_enter_keeps_the_cwd_and_reports() {
        let (_d, mut vm) = host_pane(&["file.txt"], &[]);
        let root = vm.listing.cwd_path().to_string();
        vm.apply(BrowserPaneAction::Enter("nope".into()));
        assert_eq!(vm.listing.cwd_path(), root);
        assert!(vm.error().unwrap().contains("nope"));
        assert!(vm.take_error().is_some());
        assert!(vm.error().is_none());
    }

    #[test]
    fn click_ctrl_and_shift_selection() {
        let (_d, mut vm) = host_pane(&["a", "b", "c", "d"], &[]);
        vm.apply(click("b", false, false));
        assert_eq!(names_selected(&vm), ["b"]);
        assert_eq!(cursor_name(&vm), "b");
        vm.apply(click("d", false, true));
        assert_eq!(names_selected(&vm), ["b", "c", "d"]);
        vm.apply(click("c", true, false));
        assert_eq!(names_selected(&vm), ["b", "d"]);
        vm.apply(BrowserPaneAction::ContextSelect("d".into()));
        assert_eq!(names_selected(&vm), ["b", "d"], "already selected stays");
        vm.apply(BrowserPaneAction::ContextSelect("a".into()));
        assert_eq!(names_selected(&vm), ["a"]);
        vm.apply(BrowserPaneAction::ClearSelection);
        assert!(names_selected(&vm).is_empty());
    }

    #[test]
    fn toggle_mark_marks_and_moves_down() {
        let (_d, mut vm) = host_pane(&["a", "b"], &[]);
        // A host pane starts with `..` at the top, which cannot be marked.
        vm.apply(BrowserPaneAction::ToggleMark);
        assert!(names_selected(&vm).is_empty());
        assert_eq!(cursor_name(&vm), "a");
        vm.apply(BrowserPaneAction::ToggleMark);
        vm.apply(BrowserPaneAction::ToggleMark);
        assert_eq!(names_selected(&vm), ["a", "b"]);
        vm.apply(BrowserPaneAction::CursorUp);
        vm.apply(BrowserPaneAction::ToggleMark);
        assert_eq!(names_selected(&vm), ["b"]);
    }

    #[test]
    fn sort_by_flips_and_keeps_the_cursor_on_its_row() {
        let (d, mut vm) = host_pane(&[], &[]);
        std::fs::write(d.path().join("big"), vec![0u8; 100]).unwrap();
        std::fs::write(d.path().join("small"), b"x").unwrap();
        vm.apply(BrowserPaneAction::Reload);
        vm.apply(click("small", false, false));
        vm.apply(BrowserPaneAction::SortBy(SortColumn::Size));
        let order: Vec<_> = vm
            .listing
            .entries()
            .iter()
            .map(|e| e.name.clone())
            .collect();
        assert_eq!(order, ["small", "big"]);
        vm.apply(BrowserPaneAction::SortBy(SortColumn::Size));
        assert!(vm.listing.is_descending());
        assert_eq!(vm.listing.entries()[0].name, "big");
        assert_eq!(cursor_name(&vm), "small");
    }

    #[test]
    fn cursor_moves_and_clamps() {
        let (_d, mut vm) = host_pane(&["a", "b", "c"], &[]);
        vm.display_rows();
        assert_eq!(vm.cursor_index(), Some(0));
        vm.apply(BrowserPaneAction::CursorUp);
        assert_eq!(vm.cursor_index(), Some(0));
        vm.apply(BrowserPaneAction::CursorDown);
        assert_eq!(cursor_name(&vm), "a");
        vm.apply(BrowserPaneAction::CursorPage(10));
        assert_eq!(cursor_name(&vm), "c");
        vm.apply(BrowserPaneAction::CursorDown);
        assert_eq!(cursor_name(&vm), "c");
        vm.apply(BrowserPaneAction::CursorPage(-2));
        assert_eq!(cursor_name(&vm), "a");
        vm.apply(BrowserPaneAction::CursorEnd);
        assert_eq!(cursor_name(&vm), "c");
        vm.apply(BrowserPaneAction::CursorHome);
        assert_eq!(cursor_name(&vm), "..");
    }

    #[test]
    fn activate_cursor_enters_a_dir_goes_up_and_asks_for_file_info() {
        let (_d, mut vm) = host_pane(&["f.txt"], &["sub"]);
        let root = vm.listing.cwd_path().to_string();
        vm.apply(BrowserPaneAction::CursorDown);
        assert_eq!(cursor_name(&vm), "sub");
        vm.apply(BrowserPaneAction::ActivateCursor);
        assert!(vm.listing.cwd_path().ends_with("sub"));
        assert_eq!(cursor_name(&vm), "..");
        vm.apply(BrowserPaneAction::ActivateCursor);
        assert_eq!(vm.listing.cwd_path(), root);
        vm.apply(BrowserPaneAction::CursorEnd);
        vm.apply(BrowserPaneAction::ActivateCursor);
        assert_eq!(vm.take_info_request().as_deref(), Some("f.txt"));
        assert_eq!(names_selected(&vm), ["f.txt"]);
    }

    fn sit_bytes() -> Vec<u8> {
        use arc_mac::stuffit::{build_archive_tree, StuffItInput, StuffItInputNode, WriteMethod};
        let file = |name: &str| {
            StuffItInputNode::File(StuffItInput {
                name: name.into(),
                type_code: *b"TEXT",
                creator_code: *b"ttxt",
                finder_flags: 0,
                create_date: 0,
                mod_date: 0,
                data_fork: b"x".to_vec(),
                resource_fork: Vec::new(),
            })
        };
        let tree = vec![
            file("one.txt"),
            file("two.txt"),
            StuffItInputNode::Folder {
                name: "Inner".into(),
                finder_flags: 0,
                create_date: 0,
                mod_date: 0,
                children: vec![file("deep.txt")],
            },
        ];
        build_archive_tree(&tree, WriteMethod::Store).unwrap()
    }

    /// A host pane with `a.sit` expanded in place, plus a `sub` folder.
    fn wrapped_pane() -> (tempfile::TempDir, BrowserPaneController, String) {
        let (d, mut vm) = host_pane(&[], &["sub"]);
        std::fs::write(d.path().join("a.sit"), sit_bytes()).unwrap();
        vm.apply(BrowserPaneAction::Reload);
        let id = vm
            .listing
            .entries()
            .iter()
            .find(|e| e.name == "a.sit")
            .unwrap()
            .path
            .clone();
        vm.apply(BrowserPaneAction::ToggleWrapper(id.clone()));
        assert!(
            vm.wrapper_opening().is_some(),
            "a base wrapper opens as a job"
        );
        let start = std::time::Instant::now();
        let mut fx = Vec::new();
        while vm.wrapper_opening().is_some() {
            assert!(start.elapsed().as_secs() < 60, "wrapper never opened");
            std::thread::sleep(std::time::Duration::from_millis(5));
            fx.extend(vm.tick());
        }
        assert!(vm.wrapper_tree.is_expanded(&id), "{fx:?}");
        assert!(
            fx.contains(&Effect::Message("opened a.sit.".into())),
            "{fx:?}"
        );
        vm.display_rows();
        (d, vm, id)
    }

    fn tree_id(vm: &BrowserPaneController, name: &str) -> String {
        vm.rows()
            .iter()
            .find(|r| r.is_tree && r.name == name)
            .unwrap()
            .node_id
            .clone()
    }

    #[test]
    fn tree_and_listing_selection_are_exclusive() {
        let (_d, mut vm, _id) = wrapped_pane();
        let one = tree_id(&vm, "one.txt");
        let two = tree_id(&vm, "two.txt");
        vm.apply(click("sub", false, false));
        vm.apply(BrowserPaneAction::TreeClick {
            node_id: one.clone(),
            ctrl: false,
            shift: false,
        });
        assert!(names_selected(&vm).is_empty());
        assert_eq!(vm.tree_selected, std::slice::from_ref(&one));
        vm.apply(BrowserPaneAction::TreeClick {
            node_id: two.clone(),
            ctrl: false,
            shift: true,
        });
        assert_eq!(vm.tree_selected.len(), 2);
        vm.apply(BrowserPaneAction::TreeClick {
            node_id: one.clone(),
            ctrl: true,
            shift: false,
        });
        assert_eq!(vm.tree_selected, [two]);
        vm.apply(click("sub", false, false));
        assert!(vm.tree_selected.is_empty());
        assert_eq!(names_selected(&vm), ["sub"]);
    }

    #[test]
    fn inner_folders_toggle_and_leaving_the_folder_collapses_wrappers() {
        let (_d, mut vm, id) = wrapped_pane();
        let inner = tree_id(&vm, "Inner");
        vm.apply(BrowserPaneAction::Activate(inner.clone()));
        assert!(vm.rows().iter().any(|r| r.name == "deep.txt"));
        vm.apply(BrowserPaneAction::ToggleWrapper(inner));
        assert!(!vm.rows().iter().any(|r| r.name == "deep.txt"));
        vm.apply(BrowserPaneAction::TreeClick {
            node_id: tree_id(&vm, "one.txt"),
            ctrl: false,
            shift: false,
        });
        let epoch = vm.wrapper_epoch();
        vm.apply(BrowserPaneAction::Enter("sub".into()));
        vm.apply(BrowserPaneAction::Up);
        assert!(!vm.wrapper_tree.is_expanded(&id));
        assert!(vm.tree_selected.is_empty());
        assert!(vm.wrapper_epoch() > epoch);
        assert!(!vm.rows().iter().any(|r| r.is_tree));
    }

    #[test]
    fn path_crumbs_navigate_within_the_root_only() {
        let (d, mut vm) = host_pane(&[], &["sub"]);
        vm.apply(BrowserPaneAction::Enter("sub".into()));
        let root = d.path().to_string_lossy().to_string();
        let crumbs = vm.path_crumbs();
        let joined: String = crumbs.iter().map(|(l, _)| l.as_str()).collect();
        assert_eq!(joined, vm.listing.cwd_path());
        let targets: Vec<&str> = crumbs.iter().filter_map(|(_, t)| t.as_deref()).collect();
        assert_eq!(targets.len(), 2, "{crumbs:?}");
        assert_eq!(targets[0], root);
        vm.apply(BrowserPaneAction::NavigateTo(targets[0].to_string()));
        assert_eq!(vm.listing.cwd_path(), root);
    }

    fn refused(fx: &[Effect]) -> bool {
        matches!(fx, [Effect::Message(m)] if m.ends_with("is not available in this view."))
    }

    #[test]
    fn a_capability_the_profile_turns_off_refuses_its_action() {
        let dir = tempfile::tempdir().unwrap();
        let mut vm =
            BrowserPaneController::with_profile(PaneServices::headless(), PaneProfile::explorer());
        type A = BrowserPaneAction;
        for a in [
            A::OpenHostFolder(dir.path().to_path_buf()),
            A::OpenImage(dir.path().join("x.img")),
            A::OpenPath(dir.path().to_path_buf()),
            A::PickImage,
            A::PickBackupFolder,
            A::CloseSource,
            A::SelectPartition(0),
            A::Connect,
        ] {
            let fx = vm.apply(a.clone());
            assert!(refused(&fx), "{a:?}: {fx:?}");
        }
        assert!(vm.source_path().is_none() && !vm.is_opening());

        let (_d, mut ro) = host_pane(&["a"], &[]);
        ro.profile = PaneProfile::commander().read_only();
        ro.apply(click("a", false, false));
        for a in [
            A::Delete,
            A::RequestNewFolder,
            A::RequestRename("a".into()),
            A::RenameAtCursor,
        ] {
            assert!(refused(&ro.apply(a.clone())), "{a:?}");
        }
        assert!(!ro.can_receive() && !ro.can_new_folder() && ro.is_read_only());
        let out = ro.stage_edits(vec![StagedEdit::CreateDirectory {
            parent: ro.listing.cwd().unwrap().clone(),
            name: "n".into(),
            qnx2_attrs: None,
        }]);
        assert_eq!(out.staged, 0);
        assert!(ro.queue.is_empty());
    }

    #[test]
    fn without_wrappers_an_archive_row_is_a_plain_file() {
        let (d, mut vm) = host_pane(&[], &[]);
        std::fs::write(d.path().join("a.sit"), sit_bytes()).unwrap();
        vm.profile.wrappers = false;
        vm.apply(BrowserPaneAction::Reload);
        let r = row(vm.rows(), "a.sit").clone();
        assert!(!r.is_wrapper && !r.expandable);
        assert!(refused(
            &vm.apply(BrowserPaneAction::ToggleWrapper(r.node_id.clone()))
        ));
        vm.apply(BrowserPaneAction::Activate(r.node_id));
        assert!(vm.wrapper_opening().is_none());
        assert_eq!(vm.take_info_request().as_deref(), Some("a.sit"));
    }

    #[test]
    fn a_fixed_source_pane_opens_its_partition_and_writes_without_staging() {
        use super::source::tests::{mbr_fat16, names, settle};
        let dir = tempfile::tempdir().unwrap();
        let mut vm =
            BrowserPaneController::with_profile(PaneServices::headless(), PaneProfile::explorer());
        vm.open_fixed(mbr_fat16(dir.path()), Some(0));
        settle(&mut vm);
        assert!(vm.listing.is_loaded(), "{:?}", vm.source_error());
        assert_eq!(vm.selected_partition(), Some(0));
        vm.apply(BrowserPaneAction::RequestNewFolder);
        let fx = vm.apply(BrowserPaneAction::Answered {
            purpose: purpose::NEW_FOLDER,
            text: Some("NEWDIR".into()),
        });
        assert!(vm.is_busy(), "the new folder applies at once: {fx:?}");
        settle(&mut vm);
        assert!(vm.queue.is_empty());
        assert!(
            names(&vm).contains(&"NEWDIR".to_string()),
            "{:?}",
            names(&vm)
        );
    }
}
