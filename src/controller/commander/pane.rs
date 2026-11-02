//! One Commander pane's state: source, listing, staged-edit queue, expanded wrappers, selection and cursor.

use std::collections::HashMap;
use std::path::PathBuf;

use crate::model::commander_descend::classify_entry;
use crate::model::dir_listing::DirListing;
use crate::model::edit_queue::EditQueue;
use crate::model::wrapper_tree::{tree_range, TreeRow, WrapperTree};

use super::rows::{build_display_rows, DisplayRow, RowKind};
use crate::controller::{Controller, Effect};

mod edits;
mod remote;
pub(in crate::controller::commander) mod source;

pub use crate::fs::replace::OnConflict;
pub use crate::model::commander_ops::{ApplyPreflight, FailedEdit, MAX_CONSECUTIVE_FAILURES};
pub use crate::model::dir_listing::SortColumn;
pub use edits::{validate_entry_name, ApplyReport, StageOutcome};
pub use remote::validate_remote_addr;
pub use source::{purpose, NoRecent, PaneServices, RecentStore, SourceKind, VolumeInfo};

/// What a user does to one pane: navigation, selection, sort, sources and edits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommanderPaneAction {
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
    /// Click a row inside an expanded wrapper; takes the selection over from the listing.
    TreeClick {
        node_id: String,
        ctrl: bool,
        shift: bool,
    },
    /// Right-click a base row: select just it unless it is already selected.
    ContextSelect(String),
    /// Right-click a wrapper-tree row: select just it unless it is already selected.
    TreeContextSelect(String),
    /// Toggle the cursor row in or out of the selection, then move down.
    ToggleMark,
    ClearSelection,
    SortBy(SortColumn),
    CursorUp,
    CursorDown,
    /// Move the cursor by this many rows (negative is up), clamped.
    CursorPage(i32),
    CursorHome,
    CursorEnd,
    /// What double-click does, on the cursor row.
    ActivateCursor,
    /// What double-click does, on the row with this node id (`""` is `..`).
    Activate(String),
    /// Select the cursor row and ask for its File Info.
    InfoAtCursor,
    /// Expand or collapse the wrapper or inner folder with this node id.
    ToggleWrapper(String),
    /// Open a disk image, Mac archive or optical disc image.
    OpenImage(PathBuf),
    OpenHostFolder(PathBuf),
    /// Open a native rusty-backup or Clonezilla backup folder.
    OpenBackupFolder(PathBuf),
    /// A dropped or recent path: a folder opens as a host folder, a file as an image.
    OpenPath(PathBuf),
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
    /// Drop the staged rename of the entry with this name.
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
}

/// A pane's model state. Fields stay public while the shells move onto actions slice by slice.
pub struct CommanderPaneController {
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
}

impl Default for CommanderPaneController {
    fn default() -> Self {
        Self::new()
    }
}

impl Controller for CommanderPaneController {
    type Action = CommanderPaneAction;

    fn apply(&mut self, action: CommanderPaneAction) -> Vec<Effect> {
        self.error = None;
        self.rebuild_rows();
        let mut fx = Vec::new();
        match action {
            CommanderPaneAction::Enter(name) => self.enter(&name),
            CommanderPaneAction::Up => self.up(),
            CommanderPaneAction::NavigateTo(path) => {
                let before = self.listing.cwd_path().to_string();
                if let Err(e) = self.listing.navigate_to(&path) {
                    self.error = Some(format!("cannot open '{path}': {e}"));
                }
                self.navigated(&before, None);
            }
            CommanderPaneAction::Reload => {
                if let Err(e) = self.listing.reload() {
                    self.error = Some(format!("cannot refresh: {e}"));
                }
            }
            CommanderPaneAction::Click { name, ctrl, shift } => {
                self.tree_selected.clear();
                if shift {
                    self.listing.shift_click(&name);
                } else if ctrl {
                    self.listing.ctrl_click(&name);
                } else {
                    self.listing.click(&name);
                }
                self.cursor_to_base(&name);
            }
            CommanderPaneAction::TreeClick {
                node_id,
                ctrl,
                shift,
            } => self.tree_click(node_id, ctrl, shift),
            CommanderPaneAction::ContextSelect(name) => self.context_select(&name),
            CommanderPaneAction::TreeContextSelect(node_id) => self.tree_context_select(node_id),
            CommanderPaneAction::ToggleMark => self.toggle_mark(),
            CommanderPaneAction::ClearSelection => {
                self.listing.clear_selection();
                self.tree_selected.clear();
            }
            CommanderPaneAction::SortBy(col) => self.listing.resort(col),
            CommanderPaneAction::CursorUp => self.move_cursor(-1),
            CommanderPaneAction::CursorDown => self.move_cursor(1),
            CommanderPaneAction::CursorPage(n) => self.move_cursor(i64::from(n)),
            CommanderPaneAction::CursorHome => self.set_cursor_index(0),
            CommanderPaneAction::CursorEnd => {
                let last = self.rows.len().saturating_sub(1);
                self.set_cursor_index(last);
            }
            CommanderPaneAction::ActivateCursor => {
                if let Some(row) = self.cursor_row() {
                    self.activate(row);
                }
            }
            CommanderPaneAction::Activate(node_id) => {
                if let Some(row) = self.rows.iter().find(|r| r.node_id == node_id).cloned() {
                    self.activate(row);
                }
            }
            CommanderPaneAction::InfoAtCursor => self.info_at_cursor(),
            CommanderPaneAction::ToggleWrapper(node_id) => self.toggle_wrapper(&node_id),
            other => {
                fx = match self.apply_remote(other).or_else(|a| self.apply_edit(a)) {
                    Ok(fx) => fx,
                    Err(other) => self.apply_source(other),
                }
            }
        }
        self.password_error_locks();
        self.rebuild_rows();
        fx.append(&mut self.out);
        fx
    }

    fn tick(&mut self) -> Vec<Effect> {
        let mut fx = self.poll_open();
        fx.extend(self.poll_remote());
        fx.extend(self.poll_edits());
        self.rebuild_rows();
        fx.append(&mut self.out);
        fx
    }
}

impl CommanderPaneController {
    /// A pane with no recent files, its own job registry and no wake: for tests and the CLI.
    pub fn new() -> Self {
        Self::with_services(PaneServices::headless())
    }

    /// A pane whose jobs register in `services.jobs` and wake the shell through `services.notify`.
    pub fn with_services(services: PaneServices) -> Self {
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

    pub(in crate::controller::commander) fn take_info_request(&mut self) -> Option<String> {
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

    fn rebuild_rows(&mut self) {
        // A remote host listing tints images and never expands wrappers.
        self.remote_host = self.is_remote_host();
        self.rows = build_display_rows(
            &self.listing,
            &self.queue,
            &mut self.wrapper_tree,
            &mut self.tree_index,
            self.remote_host,
        );
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
        self.listing.clear_selection();
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
            self.listing.click(name);
        }
        self.cursor_to_base(name);
    }

    fn tree_context_select(&mut self, node_id: String) {
        if !self.tree_selected.contains(&node_id) {
            self.listing.clear_selection();
            self.tree_selected = vec![node_id.clone()];
        }
        self.cursor = Some(node_id);
    }

    fn toggle_mark(&mut self) {
        let Some(row) = self.cursor_row() else {
            return;
        };
        if row.is_tree {
            self.listing.clear_selection();
            self.toggle_tree_selected(&row.node_id);
            self.tree_anchor = Some(row.node_id);
        } else if !row.is_parent() {
            self.tree_selected.clear();
            self.listing.ctrl_click(&row.name);
        }
        self.move_cursor(1);
    }

    fn info_at_cursor(&mut self) {
        let Some(row) = self.cursor_row() else {
            return;
        };
        if row.is_parent() || row.kind == RowKind::PendingAdd {
            return;
        }
        if row.is_tree {
            self.tree_context_select(row.node_id);
        } else {
            self.context_select(&row.name);
        }
        self.info_request = Some(row.name);
    }

    /// Double-click: `..` goes up, a folder opens, a wrapper toggles, a file shows its info.
    fn activate(&mut self, row: DisplayRow) {
        if row.is_parent() {
            self.up();
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

    fn host_pane(files: &[&str], dirs: &[&str]) -> (tempfile::TempDir, CommanderPaneController) {
        let dir = tempfile::tempdir().unwrap();
        for f in files {
            std::fs::write(dir.path().join(f), b"x").unwrap();
        }
        for d in dirs {
            std::fs::create_dir(dir.path().join(d)).unwrap();
        }
        let mut vm = CommanderPaneController::new();
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

    fn names_selected(vm: &CommanderPaneController) -> Vec<String> {
        vm.listing.selection().to_vec()
    }

    fn click(name: &str, ctrl: bool, shift: bool) -> CommanderPaneAction {
        CommanderPaneAction::Click {
            name: name.into(),
            ctrl,
            shift,
        }
    }

    fn cursor_name(vm: &CommanderPaneController) -> String {
        vm.rows()[vm.cursor_index().unwrap()].name.clone()
    }

    #[test]
    fn enter_up_and_navigate_to() {
        let (d, mut vm) = host_pane(&[], &["sub"]);
        std::fs::create_dir(d.path().join("sub").join("deeper")).unwrap();
        let root = vm.listing.cwd_path().to_string();
        vm.apply(CommanderPaneAction::Enter("sub".into()));
        assert!(vm.listing.cwd_path().ends_with("sub"));
        assert!(vm.error().is_none());
        vm.apply(CommanderPaneAction::Up);
        assert_eq!(vm.listing.cwd_path(), root);
        // The cursor lands on the folder just left.
        assert_eq!(cursor_name(&vm), "sub");
        let deeper = d.path().join("sub").join("deeper");
        vm.apply(CommanderPaneAction::NavigateTo(
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
        vm.apply(CommanderPaneAction::Enter("nope".into()));
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
        vm.apply(CommanderPaneAction::ContextSelect("d".into()));
        assert_eq!(names_selected(&vm), ["b", "d"], "already selected stays");
        vm.apply(CommanderPaneAction::ContextSelect("a".into()));
        assert_eq!(names_selected(&vm), ["a"]);
        vm.apply(CommanderPaneAction::ClearSelection);
        assert!(names_selected(&vm).is_empty());
    }

    #[test]
    fn toggle_mark_marks_and_moves_down() {
        let (_d, mut vm) = host_pane(&["a", "b"], &[]);
        // A host pane starts with `..` at the top, which cannot be marked.
        vm.apply(CommanderPaneAction::ToggleMark);
        assert!(names_selected(&vm).is_empty());
        assert_eq!(cursor_name(&vm), "a");
        vm.apply(CommanderPaneAction::ToggleMark);
        vm.apply(CommanderPaneAction::ToggleMark);
        assert_eq!(names_selected(&vm), ["a", "b"]);
        vm.apply(CommanderPaneAction::CursorUp);
        vm.apply(CommanderPaneAction::ToggleMark);
        assert_eq!(names_selected(&vm), ["b"]);
    }

    #[test]
    fn sort_by_flips_and_keeps_the_cursor_on_its_row() {
        let (d, mut vm) = host_pane(&[], &[]);
        std::fs::write(d.path().join("big"), vec![0u8; 100]).unwrap();
        std::fs::write(d.path().join("small"), b"x").unwrap();
        vm.apply(CommanderPaneAction::Reload);
        vm.apply(click("small", false, false));
        vm.apply(CommanderPaneAction::SortBy(SortColumn::Size));
        let order: Vec<_> = vm
            .listing
            .entries()
            .iter()
            .map(|e| e.name.clone())
            .collect();
        assert_eq!(order, ["small", "big"]);
        vm.apply(CommanderPaneAction::SortBy(SortColumn::Size));
        assert!(vm.listing.is_descending());
        assert_eq!(vm.listing.entries()[0].name, "big");
        assert_eq!(cursor_name(&vm), "small");
    }

    #[test]
    fn cursor_moves_and_clamps() {
        let (_d, mut vm) = host_pane(&["a", "b", "c"], &[]);
        vm.display_rows();
        assert_eq!(vm.cursor_index(), Some(0));
        vm.apply(CommanderPaneAction::CursorUp);
        assert_eq!(vm.cursor_index(), Some(0));
        vm.apply(CommanderPaneAction::CursorDown);
        assert_eq!(cursor_name(&vm), "a");
        vm.apply(CommanderPaneAction::CursorPage(10));
        assert_eq!(cursor_name(&vm), "c");
        vm.apply(CommanderPaneAction::CursorDown);
        assert_eq!(cursor_name(&vm), "c");
        vm.apply(CommanderPaneAction::CursorPage(-2));
        assert_eq!(cursor_name(&vm), "a");
        vm.apply(CommanderPaneAction::CursorEnd);
        assert_eq!(cursor_name(&vm), "c");
        vm.apply(CommanderPaneAction::CursorHome);
        assert_eq!(cursor_name(&vm), "..");
    }

    #[test]
    fn activate_cursor_enters_a_dir_goes_up_and_asks_for_file_info() {
        let (_d, mut vm) = host_pane(&["f.txt"], &["sub"]);
        let root = vm.listing.cwd_path().to_string();
        vm.apply(CommanderPaneAction::CursorDown);
        assert_eq!(cursor_name(&vm), "sub");
        vm.apply(CommanderPaneAction::ActivateCursor);
        assert!(vm.listing.cwd_path().ends_with("sub"));
        assert_eq!(cursor_name(&vm), "..");
        vm.apply(CommanderPaneAction::ActivateCursor);
        assert_eq!(vm.listing.cwd_path(), root);
        vm.apply(CommanderPaneAction::CursorEnd);
        vm.apply(CommanderPaneAction::ActivateCursor);
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
    fn wrapped_pane() -> (tempfile::TempDir, CommanderPaneController, String) {
        let (d, mut vm) = host_pane(&[], &["sub"]);
        std::fs::write(d.path().join("a.sit"), sit_bytes()).unwrap();
        vm.apply(CommanderPaneAction::Reload);
        let id = vm
            .listing
            .entries()
            .iter()
            .find(|e| e.name == "a.sit")
            .unwrap()
            .path
            .clone();
        vm.apply(CommanderPaneAction::ToggleWrapper(id.clone()));
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

    fn tree_id(vm: &CommanderPaneController, name: &str) -> String {
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
        vm.apply(CommanderPaneAction::TreeClick {
            node_id: one.clone(),
            ctrl: false,
            shift: false,
        });
        assert!(names_selected(&vm).is_empty());
        assert_eq!(vm.tree_selected, std::slice::from_ref(&one));
        vm.apply(CommanderPaneAction::TreeClick {
            node_id: two.clone(),
            ctrl: false,
            shift: true,
        });
        assert_eq!(vm.tree_selected.len(), 2);
        vm.apply(CommanderPaneAction::TreeClick {
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
        vm.apply(CommanderPaneAction::Activate(inner.clone()));
        assert!(vm.rows().iter().any(|r| r.name == "deep.txt"));
        vm.apply(CommanderPaneAction::ToggleWrapper(inner));
        assert!(!vm.rows().iter().any(|r| r.name == "deep.txt"));
        vm.apply(CommanderPaneAction::TreeClick {
            node_id: tree_id(&vm, "one.txt"),
            ctrl: false,
            shift: false,
        });
        let epoch = vm.wrapper_epoch();
        vm.apply(CommanderPaneAction::Enter("sub".into()));
        vm.apply(CommanderPaneAction::Up);
        assert!(!vm.wrapper_tree.is_expanded(&id));
        assert!(vm.tree_selected.is_empty());
        assert!(vm.wrapper_epoch() > epoch);
        assert!(!vm.rows().iter().any(|r| r.is_tree));
    }

    #[test]
    fn path_crumbs_navigate_within_the_root_only() {
        let (d, mut vm) = host_pane(&[], &["sub"]);
        vm.apply(CommanderPaneAction::Enter("sub".into()));
        let root = d.path().to_string_lossy().to_string();
        let crumbs = vm.path_crumbs();
        let joined: String = crumbs.iter().map(|(l, _)| l.as_str()).collect();
        assert_eq!(joined, vm.listing.cwd_path());
        let targets: Vec<&str> = crumbs.iter().filter_map(|(_, t)| t.as_deref()).collect();
        assert_eq!(targets.len(), 2, "{crumbs:?}");
        assert_eq!(targets[0], root);
        vm.apply(CommanderPaneAction::NavigateTo(targets[0].to_string()));
        assert_eq!(vm.listing.cwd_path(), root);
    }
}
