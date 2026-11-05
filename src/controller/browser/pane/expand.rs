//! Listing styles, and the combined tree's folders (style `CombinedTree`).
//!
//! In the combined tree a folder row expands in place: its children are listed by a job on a
//! reader opened from the pane's source (a host folder lists through `std::fs`), kept between
//! listings and dropped when the source changes or is written. Expanded folders survive a
//! reopen after Apply and are listed again. Rows below an expanded folder are selected by path;
//! ctrl-clicks and marks mix them with the listing's own selection (one selection across levels),
//! the wrapper tree's stays exclusive, and a Shift range stays inside one folder. Staging (delete, rename, new folder, a copy into the
//! pane) acts on a deep row's own folder. See docs/phase2_commander_plan.md § "Slice 10 notes".

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;

use disk_core::entry::FileEntry;
use disk_core::filesystem::Filesystem;

use super::super::profile::ListingStyle;
use super::super::rows::{splice_folders, FolderRow, FolderSplice, RowKind};
use super::BrowserPaneController;
use crate::controller::Effect;
use crate::model::dir_listing::{list_folder, sort_entries, FolderSource};
use crate::model::job::{Job, Status};

/// A folder listing's result, with the reader handed back for the next one.
struct Listed {
    reader: Option<Box<dyn Filesystem>>,
    entries: Result<Vec<FileEntry>, String>,
}

struct Running {
    dir: FileEntry,
    generation: u64,
    job: Job<Listed>,
}

/// The combined tree's state: expanded folders, their listed children and the deep selection.
#[derive(Default)]
pub(super) struct Folders {
    expanded: HashSet<String>,
    children: HashMap<String, Vec<FileEntry>>,
    queue: VecDeque<FileEntry>,
    job: Option<Running>,
    reader: Option<Box<dyn Filesystem>>,
    /// Bumped when the source changes or is written, so a listing in flight is dropped.
    generation: u64,
    /// The source (path, partition) the expanded set belongs to.
    owner: Option<(PathBuf, Option<usize>)>,
    /// Selected rows below expanded folders, by path.
    pub(super) selected: Vec<String>,
    anchor: Option<String>,
    /// Path -> row for the deep rows last built.
    pub(super) index: HashMap<String, FolderRow>,
}

impl Folders {
    /// Forget what was listed (the source was written or reloaded); expanded folders stay.
    fn invalidate(&mut self) {
        self.generation += 1;
        self.reader = None;
        self.children.clear();
        self.queue.clear();
    }
}

impl BrowserPaneController {
    /// How this pane lists its volume now.
    pub fn listing_style(&self) -> ListingStyle {
        self.style
    }

    /// True when the folder tree is beside (or above) the listing.
    pub fn has_folder_tree(&self) -> bool {
        self.style == ListingStyle::TreeAndList
    }

    /// True while a folder of the combined tree is being listed or waits to be.
    pub fn is_listing_folders(&self) -> bool {
        self.dirs.job.is_some() || !self.dirs.queue.is_empty()
    }

    /// True when a row is selected below an expanded folder.
    pub fn folder_active(&self) -> bool {
        !self.dirs.selected.is_empty()
    }

    /// Whether a row is drawn selected, whichever selection it belongs to.
    pub fn row_selected(&self, row: &super::DisplayRow) -> bool {
        if row.is_parent() {
            false
        } else if row.is_tree {
            self.tree_selected.contains(&row.node_id)
        } else if row.in_folder {
            self.dirs.selected.contains(&row.node_id)
        } else {
            self.listing.is_selected(&row.name)
        }
    }

    /// The entry a row draws: a listed, deep, wrapper or staged one; `None` for `..`.
    pub fn row_entry(&self, row: &super::DisplayRow) -> Option<FileEntry> {
        if row.is_tree {
            return self.tree_index.get(&row.node_id).map(|r| r.entry.clone());
        }
        if row.kind == RowKind::PendingAdd {
            let parent = match row.node_id.rsplit_once('/') {
                Some(("", _)) | None => "/",
                Some((p, _)) => p,
            };
            let mut adds = self.queue.pending_adds_for(parent).into_iter();
            return adds.find(|e| e.path == row.node_id);
        }
        if row.is_parent() {
            return None;
        }
        self.entry_by_path(&row.node_id)
    }

    /// The folder or file at `path`: the root, a listed entry or one below an expanded folder.
    pub fn entry_at(&self, path: &str) -> Option<FileEntry> {
        let root = self.listing.root_entry().filter(|r| r.path == path);
        root.or_else(|| self.entry_by_path(path))
    }

    /// What an expanded folder was listed to hold; `None` before it is listed.
    pub fn folder_children(&self, path: &str) -> Option<&[FileEntry]> {
        if path == self.listing.cwd_path() {
            return Some(self.listing.entries());
        }
        self.dirs.children.get(path).map(|v| v.as_slice())
    }

    /// Turn writes off or on while the pane is open (the Inspect tab's Edit Mode).
    pub fn set_read_only(&mut self, read_only: bool) {
        self.profile.read_only = read_only;
    }

    /// The selected deep rows, in selection order.
    pub(super) fn folder_selection(&self) -> Vec<FolderRow> {
        let rows = self.dirs.selected.iter();
        rows.filter_map(|p| self.dirs.index.get(p).cloned())
            .collect()
    }

    /// The listing's stored style for this view, applied at once; also where a change is saved.
    pub(in crate::controller) fn remember_style_as(&mut self, key: &str) {
        self.style_key = Some(key.to_string());
        let stored = self.store().listing_style(key);
        if let Some(style) = stored.as_deref().and_then(ListingStyle::from_key) {
            if self.profile.allows(style) {
                self.change_style(style);
            }
        }
    }

    pub(super) fn set_listing_style(&mut self, style: ListingStyle) -> Vec<Effect> {
        if !self.profile.allows(style) {
            let what = format!("the {} style", style.label().to_lowercase());
            return vec![Effect::Message(self.profile.refusal(&what))];
        }
        self.change_style(style);
        if let Some(key) = &self.style_key {
            let key = key.clone();
            self.store_mut().remember_listing_style(&key, style.key());
        }
        Vec::new()
    }

    /// Switch styles keeping the folder and the listing's selection; a deep cursor moves to its top row.
    fn change_style(&mut self, style: ListingStyle) {
        if style == self.style {
            return;
        }
        if self.style == ListingStyle::CombinedTree {
            if let Some(top) = self.cursor_top_level() {
                self.cursor = Some(top);
            }
            self.dirs.selected.clear();
            self.dirs.anchor = None;
        }
        self.folders.clear();
        self.style = style;
    }

    /// The base row holding the cursor row (itself when it is a base row).
    fn cursor_top_level(&self) -> Option<String> {
        let i = self.cursor_index()?;
        let row = &self.rows[i];
        if row.depth == 0 {
            return Some(row.node_id.clone());
        }
        let base = self.rows[..i].iter().rev().find(|r| r.depth == 0)?;
        Some(base.node_id.clone())
    }

    /// Where a folder below the listing's is read from; `None` when it cannot be (a daemon's host).
    fn folder_source(&self) -> Option<FolderSource> {
        if self.is_remote_host() {
            return None;
        }
        if self.listing.is_host() {
            return Some(FolderSource::Host);
        }
        self.base_stage_source().map(FolderSource::Image)
    }

    /// True when marks on top-level rows and rows below expanded folders form one selection.
    pub(super) fn mixes_levels(&self) -> bool {
        self.style == ListingStyle::CombinedTree
    }

    /// True when this pane draws the combined tree with expandable folders.
    pub(super) fn combined(&self) -> bool {
        self.style == ListingStyle::CombinedTree && self.listing.is_loaded()
    }

    /// Splice expanded folders into the rows just built (combined tree only).
    pub(super) fn splice_folder_rows(&mut self) {
        if !self.combined() || self.folder_source().is_none() {
            self.dirs.index.clear();
            return;
        }
        let rows = std::mem::take(&mut self.rows);
        let splice = FolderSplice {
            expanded: &self.dirs.expanded,
            children: &self.dirs.children,
            queue: &self.queue,
            remote_host: self.remote_host,
        };
        self.rows = splice_folders(
            rows,
            self.listing.entries(),
            &splice,
            &mut self.wrapper_tree,
            &mut self.tree_index,
            &mut self.dirs.index,
        );
    }

    /// The entry of a folder row by path: a listed one, or one below an expanded folder.
    pub(super) fn entry_by_path(&self, path: &str) -> Option<FileEntry> {
        let listed = self.listing.entries().iter().find(|e| e.path == path);
        listed
            .cloned()
            .or_else(|| self.dirs.index.get(path).map(|r| r.entry.clone()))
    }

    /// Expand or collapse a folder, wrapper or wrapper folder by node id.
    pub(super) fn toggle_expand(&mut self, node_id: &str) {
        let Some(row) = self.rows.iter().find(|r| r.node_id == node_id).cloned() else {
            return;
        };
        if !row.expandable {
            return;
        }
        if !row.is_dir || row.is_tree {
            return self.toggle_wrapper(node_id);
        }
        if !self.dirs.expanded.remove(node_id) {
            self.dirs.expanded.insert(node_id.to_string());
            return;
        }
        // Collapsing hides the rows below, so their selection and wrappers go too.
        let below = |p: &str| p.len() > node_id.len() && p.starts_with(node_id);
        self.dirs.selected.retain(|p| !below(p));
        if self.cursor.as_deref().is_some_and(below) {
            self.cursor = Some(node_id.to_string());
        }
    }

    /// Right: open the cursor's folder or wrapper, or step onto its first child once open.
    pub(super) fn expand_cursor(&mut self) {
        if self.style == ListingStyle::FlatList || !self.combined() {
            return self.flat_expand();
        }
        let Some(i) = self.cursor_index() else { return };
        let row = self.rows[i].clone();
        if row.expandable && !row.expanded {
            self.toggle_expand(&row.node_id);
        } else if row.expanded && self.rows.get(i + 1).is_some_and(|r| r.depth > row.depth) {
            self.set_cursor_index(i + 1);
        }
    }

    /// Left: close the cursor's folder or wrapper, else step to the row it is in.
    pub(super) fn collapse_cursor(&mut self) {
        if self.style == ListingStyle::FlatList || !self.combined() {
            return self.up();
        }
        let Some(i) = self.cursor_index() else { return };
        let row = self.rows[i].clone();
        if row.expanded {
            self.toggle_expand(&row.node_id);
        } else if row.depth > 0 {
            if let Some(p) = (0..i).rev().find(|&j| self.rows[j].depth < row.depth) {
                self.set_cursor_index(p);
            }
        }
    }

    /// The list beside a folder tree: Right opens a folder or a wrapper, as the Explorer's list did.
    fn flat_expand(&mut self) {
        let Some(row) = self.cursor_row() else { return };
        if row.is_parent() || row.is_dir || row.expandable {
            self.activate(row);
        }
    }

    /// Click a row below an expanded folder.
    pub(super) fn folder_click(&mut self, node_id: String, ctrl: bool, shift: bool) {
        if !(ctrl && self.mixes_levels()) {
            self.listing.clear_selection();
        }
        self.tree_selected.clear();
        self.cursor = Some(node_id.clone());
        let anchor = self.dirs.anchor.clone();
        let parent = |id: &str| self.dirs.index.get(id).map(|r| r.parent.path.clone());
        let same_folder = anchor
            .as_deref()
            .is_some_and(|a| parent(a).is_some() && parent(a) == parent(&node_id));
        if let (true, true, Some(anchor)) = (shift, same_folder, anchor) {
            let folder = parent(&node_id);
            let ids: Vec<&String> = self
                .rows
                .iter()
                .filter(|r| r.in_folder && parent(&r.node_id) == folder)
                .map(|r| &r.node_id)
                .collect();
            let pos = |id: &str| ids.iter().position(|n| n.as_str() == id);
            if let (Some(a), Some(b)) = (pos(&anchor), pos(&node_id)) {
                let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
                self.dirs.selected = ids[lo..=hi].iter().map(|s| s.to_string()).collect();
            }
        } else if ctrl {
            self.toggle_folder_selected(&node_id);
            self.dirs.anchor = Some(node_id);
        } else {
            self.dirs.selected = vec![node_id.clone()];
            self.dirs.anchor = Some(node_id);
        }
    }

    pub(super) fn toggle_folder_selected(&mut self, node_id: &str) {
        if !self.mixes_levels() {
            self.listing.clear_selection();
        }
        self.tree_selected.clear();
        if let Some(pos) = self.dirs.selected.iter().position(|n| n == node_id) {
            self.dirs.selected.remove(pos);
        } else {
            self.dirs.selected.push(node_id.to_string());
        }
    }

    /// Right-click a deep row: select just it unless it is already selected.
    pub(super) fn folder_context_select(&mut self, node_id: String) {
        if !self.dirs.selected.contains(&node_id) {
            self.listing.clear_selection();
            self.tree_selected.clear();
            self.dirs.selected = vec![node_id.clone()];
            self.dirs.anchor = Some(node_id.clone());
        }
        self.cursor = Some(node_id);
    }

    pub(super) fn clear_folder_selection(&mut self) {
        self.dirs.selected.clear();
        self.dirs.anchor = None;
    }

    /// The folder new things land in: the cursor's open folder, else the folder the cursor row is in.
    pub(super) fn target_folder(&self) -> Option<FileEntry> {
        let cwd = self.listing.cwd().cloned();
        if !self.combined() {
            return cwd;
        }
        let Some(row) = self.cursor_index().map(|i| &self.rows[i]) else {
            return cwd;
        };
        if row.is_dir && row.expanded && !row.is_tree && row.kind != RowKind::PendingAdd {
            return self.entry_by_path(&row.node_id).or(cwd);
        }
        match self.dirs.index.get(&row.node_id) {
            Some(r) => Some(r.parent.clone()),
            None => cwd,
        }
    }

    /// The names already in `folder` (listed or staged), for name checks.
    pub(super) fn names_in(&self, folder: &str) -> Vec<String> {
        let listed: Vec<String> = if folder == self.listing.cwd_path() {
            self.listing
                .entries()
                .iter()
                .map(|e| e.name.clone())
                .collect()
        } else {
            let kids = self.dirs.children.get(folder).map(|k| k.as_slice());
            kids.unwrap_or(&[]).iter().map(|e| e.name.clone()).collect()
        };
        let staged = self.queue.pending_adds_for(folder).into_iter();
        listed.into_iter().chain(staged.map(|e| e.name)).collect()
    }

    /// The source was written, reloaded or replaced: list expanded folders again.
    pub(super) fn folders_source_changed(&mut self) {
        let owner = self
            .source_path()
            .map(|p| (p.to_path_buf(), self.selected_partition()));
        if owner != self.dirs.owner {
            self.dirs.expanded.clear();
            self.dirs.owner = owner;
        }
        self.dirs.invalidate();
        self.clear_folder_selection();
        self.volume_changed();
    }

    /// The volume's reader for a reading tool, with the generation it belongs to.
    pub(super) fn take_reader(&mut self) -> (Option<Box<dyn Filesystem>>, u64) {
        (self.dirs.reader.take(), self.dirs.generation)
    }

    /// A reading tool is done with the reader; kept unless the volume changed meanwhile.
    pub(super) fn give_reader(&mut self, reader: Option<Box<dyn Filesystem>>, generation: u64) {
        if generation == self.dirs.generation && self.dirs.reader.is_none() {
            self.dirs.reader = reader;
        }
    }

    /// The listing was re-read in place: list expanded folders again.
    pub(super) fn folders_reloaded(&mut self) {
        self.dirs.invalidate();
    }

    /// Queue every visible expanded folder that has no listing yet, then start the next listing.
    pub(super) fn pump_folders(&mut self) {
        if !self.combined() {
            return;
        }
        let wanted: Vec<String> = self
            .rows
            .iter()
            .filter(|r| r.loading)
            .map(|r| r.node_id.clone())
            .collect();
        for path in wanted {
            let queued = self.dirs.queue.iter().any(|d| d.path == path);
            let running = self.dirs.job.as_ref().is_some_and(|r| r.dir.path == path);
            if queued || running {
                continue;
            }
            if let Some(dir) = self.entry_by_path(&path) {
                self.dirs.queue.push_back(dir);
            }
        }
        self.start_folder_listing();
    }

    fn start_folder_listing(&mut self) {
        if self.dirs.job.is_some() {
            return;
        }
        let Some(source) = self.folder_source() else {
            self.dirs.queue.clear();
            return;
        };
        let Some(dir) = self.dirs.queue.pop_front() else {
            return;
        };
        let resources = if self.listing.is_host() {
            Vec::new()
        } else {
            self.source_resources()
        };
        let mut reader = self.dirs.reader.take();
        let target = dir.clone();
        let (jobs, notify) = self.job_services();
        let label = format!("Listing {}", dir.name);
        let spawned = jobs.spawn(label, resources, notify, move |_| {
            let entries = list_folder(source, &mut reader, &target);
            Ok(Listed { reader, entries })
        });
        match spawned {
            Ok(job) => {
                self.dirs.job = Some(Running {
                    dir,
                    generation: self.dirs.generation,
                    job,
                })
            }
            // Another job holds the source; try again on the next tick.
            Err(_) => self.dirs.queue.push_front(dir),
        }
    }

    /// Install a finished listing; called from `tick`.
    pub(super) fn poll_folders(&mut self) {
        let done = self.dirs.job.as_ref().is_some_and(|r| !r.job.is_running());
        if done {
            if let Some(run) = self.dirs.job.take() {
                self.listed(run);
            }
        }
        self.start_folder_listing();
    }

    fn listed(&mut self, run: Running) {
        let failed = match run.job.snapshot().status {
            Status::Failed(e) => Some(e),
            _ => None,
        };
        let Some(done) = run.job.take_result() else {
            if let Some(e) = failed {
                self.error = Some(format!("cannot list '{}': {e}", run.dir.name));
            }
            return;
        };
        if run.generation != self.dirs.generation {
            return;
        }
        if self.dirs.reader.is_none() {
            self.dirs.reader = done.reader;
        }
        let entries = match done.entries {
            Ok(mut entries) => {
                let (col, desc) = (self.listing.sort_column(), self.listing.is_descending());
                sort_entries(&mut entries, col, desc);
                entries
            }
            Err(e) => {
                self.error = Some(format!("cannot list '{}': {e}", run.dir.name));
                Vec::new()
            }
        };
        self.dirs.children.insert(run.dir.path, entries);
    }

    /// Re-sort listed children after the listing's sort changed.
    pub(super) fn resort_folders(&mut self) {
        let (col, desc) = (self.listing.sort_column(), self.listing.is_descending());
        for kids in self.dirs.children.values_mut() {
            sort_entries(kids, col, desc);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::source::tests::settle;
    use super::super::{BrowserPaneAction, BrowserPaneController, DisplayRow};
    use crate::controller::browser::{ListingStyle, PaneProfile, PaneServices};
    use crate::controller::Controller;
    use crate::model::edit_queue::StagedEdit;

    type A = BrowserPaneAction;

    /// `a/{a1.txt, inner/deep.txt}`, `b/`, `top.txt` in a host folder.
    fn host_tree() -> (tempfile::TempDir, BrowserPaneController) {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path().join("a/inner")).unwrap();
        std::fs::create_dir(d.path().join("b")).unwrap();
        std::fs::write(d.path().join("a/a1.txt"), b"one").unwrap();
        std::fs::write(d.path().join("a/inner/deep.txt"), b"deep").unwrap();
        std::fs::write(d.path().join("top.txt"), b"top").unwrap();
        let mut vm = BrowserPaneController::new();
        vm.listing.load_host_root(d.path().to_path_buf()).unwrap();
        vm.display_rows();
        (d, vm)
    }

    fn row<'a>(vm: &'a BrowserPaneController, name: &str) -> &'a DisplayRow {
        let found = vm.rows().iter().find(|r| r.name == name);
        found.unwrap_or_else(|| panic!("no row {name}: {:?}", names(vm)))
    }

    fn names(vm: &BrowserPaneController) -> Vec<String> {
        vm.rows().iter().map(|r| r.name.clone()).collect()
    }

    fn id(vm: &BrowserPaneController, name: &str) -> String {
        row(vm, name).node_id.clone()
    }

    fn expand(vm: &mut BrowserPaneController, name: &str) {
        let node = id(vm, name);
        vm.apply(A::ToggleExpand(node));
        settle(vm);
        assert!(row(vm, name).expanded && !row(vm, name).loading);
    }

    fn cursor_name(vm: &BrowserPaneController) -> String {
        vm.rows()[vm.cursor_index().unwrap()].name.clone()
    }

    fn click(node_id: String, ctrl: bool, shift: bool) -> A {
        A::TreeClick {
            node_id,
            ctrl,
            shift,
        }
    }

    #[test]
    fn each_style_lists_its_own_rows() {
        let (_d, mut vm) = host_tree();
        assert_eq!(vm.listing_style(), ListingStyle::FlatList);
        assert!(!row(&vm, "a").expandable && vm.folder_tree().is_empty());
        assert_eq!(names(&vm), ["..", "a", "b", "top.txt"]);

        vm.apply(A::SetListingStyle(ListingStyle::CombinedTree));
        let a = row(&vm, "a");
        assert!(a.expandable && !a.expanded && a.depth == 0);
        assert!(!row(&vm, "top.txt").expandable);

        vm.apply(A::SetListingStyle(ListingStyle::TreeAndList));
        assert!(vm.has_folder_tree() && !row(&vm, "a").expandable);
        let tree: Vec<&str> = vm
            .folder_tree()
            .iter()
            .map(|n| n.entry.name.as_str())
            .collect();
        assert!(tree.contains(&"a") && tree.contains(&"b"), "{tree:?}");
    }

    #[test]
    fn a_folder_expands_in_place_with_its_children_listed_by_a_job() {
        let (_d, mut vm) = host_tree();
        vm.apply(A::SetListingStyle(ListingStyle::CombinedTree));
        vm.apply(A::ToggleExpand(id(&vm, "a")));
        let a = row(&vm, "a");
        assert!(a.expanded && a.loading, "listed by a job, not at once");
        assert!(vm.is_listing_folders());
        settle(&mut vm);
        assert_eq!(names(&vm), ["..", "a", "inner", "a1.txt", "b", "top.txt"]);
        let a1 = row(&vm, "a1.txt");
        assert!(a1.in_folder && a1.depth == 1 && !a1.is_tree);
        expand(&mut vm, "inner");
        assert_eq!(row(&vm, "deep.txt").depth, 2);
        // Collapsing the outer folder hides everything below it.
        vm.apply(A::ToggleExpand(id(&vm, "a")));
        assert_eq!(names(&vm), ["..", "a", "b", "top.txt"]);
        // Expanding again shows the inner folder still open, from the listings already read.
        vm.apply(A::ToggleExpand(id(&vm, "a")));
        assert!(names(&vm).contains(&"deep.txt".to_string()));
    }

    #[test]
    fn the_cursor_and_selection_move_across_expanded_folders() {
        let (_d, mut vm) = host_tree();
        vm.apply(A::SetListingStyle(ListingStyle::CombinedTree));
        vm.apply(A::CursorDown);
        assert_eq!(cursor_name(&vm), "a");
        // Right expands, Right again steps in, Left steps out, Left again collapses.
        vm.apply(A::ExpandCursor);
        settle(&mut vm);
        vm.apply(A::ExpandCursor);
        assert_eq!(cursor_name(&vm), "inner");
        vm.apply(A::CursorDown);
        assert_eq!(cursor_name(&vm), "a1.txt");
        vm.apply(A::CollapseCursor);
        assert_eq!(cursor_name(&vm), "a");

        vm.apply(click(id(&vm, "a1.txt"), false, false));
        assert_eq!(vm.selected_entries()[0].name, "a1.txt");
        vm.apply(click(id(&vm, "inner"), true, false));
        assert_eq!(vm.selected_entries().len(), 2);
        assert!(vm.row_selected(row(&vm, "inner")) && !vm.row_selected(row(&vm, "a")));
        // A Shift range stays inside one folder.
        vm.apply(click(id(&vm, "inner"), false, false));
        vm.apply(click(id(&vm, "a1.txt"), false, true));
        assert_eq!(vm.selected_entries().len(), 2);
        // A base row takes the selection back, and marks work on deep rows too.
        vm.apply(A::Click {
            name: "top.txt".into(),
            ctrl: false,
            shift: false,
        });
        assert!(!vm.folder_active() && vm.selected_entries()[0].name == "top.txt");
        vm.apply(A::ClearSelection);
        vm.apply(A::CursorHome);
        vm.apply(A::CursorPage(3));
        assert_eq!(cursor_name(&vm), "a1.txt");
        vm.apply(A::ToggleMark);
        assert_eq!(vm.selected_entries()[0].name, "a1.txt");
        // File Info for a deep row names it.
        vm.apply(A::Activate(id(&vm, "a1.txt")));
        assert_eq!(vm.take_info_request().as_deref(), Some("a1.txt"));
        assert_eq!(vm.detail_entry("a1.txt").unwrap().name, "a1.txt");
    }

    #[test]
    fn a_delete_deep_in_the_tree_stages_against_its_own_folder_and_applies() {
        let dir = tempfile::tempdir().unwrap();
        let img = deep_fat(dir.path());
        let mut vm = BrowserPaneController::new();
        vm.apply(A::OpenImage(img));
        settle(&mut vm);
        assert!(vm.listing.is_loaded(), "{:?}", vm.source_error());
        vm.apply(A::SetListingStyle(ListingStyle::CombinedTree));
        expand(&mut vm, "SUB");
        vm.apply(click(id(&vm, "INNER.TXT"), false, false));
        vm.apply(A::Delete);
        match vm.queue.iter().next() {
            Some(StagedEdit::DeleteEntry { parent, entry }) => {
                assert_eq!(parent.name, "SUB");
                assert_eq!(entry.name, "INNER.TXT");
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(
            row(&vm, "INNER.TXT").kind,
            crate::controller::commander::RowKind::PendingDelete
        );
        vm.apply(A::Apply);
        settle(&mut vm);
        assert!(
            vm.queue.is_empty(),
            "{:?}",
            vm.apply_report().map(|r| &r.failed)
        );
        // The reopen lists the still-expanded folder again, without the file.
        settle(&mut vm);
        assert!(row(&vm, "SUB").expanded);
        assert!(
            !names(&vm).contains(&"INNER.TXT".to_string()),
            "{:?}",
            names(&vm)
        );
        assert!(names(&vm).contains(&"KEEP.TXT".to_string()));
    }

    /// A FAT volume holding `/SUB/INNER.TXT` and `/SUB/KEEP.TXT`.
    fn deep_fat(dir: &std::path::Path) -> std::path::PathBuf {
        let img = dir.join("deep.img");
        let bytes = fs_pc::fat::create_blank_fat(2 * 1024 * 1024, Some("T")).unwrap();
        std::fs::write(&img, bytes).unwrap();
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&img)
            .unwrap();
        let mut fs = crate::fs::open_editable_filesystem(f, 0, 0, None).unwrap();
        let root = fs.root().unwrap();
        let opts = disk_core::filesystem::CreateDirectoryOptions::default();
        let sub = fs.create_directory(&root, "SUB", &opts).unwrap();
        for name in ["INNER.TXT", "KEEP.TXT"] {
            let mut data: &[u8] = b"x";
            fs.create_file(&sub, name, &mut data, 1, &Default::default())
                .unwrap();
        }
        fs.sync_metadata().unwrap();
        img
    }

    #[test]
    fn switching_styles_keeps_the_folder_and_the_selection() {
        let (_d, mut vm) = host_tree();
        vm.apply(A::Enter("a".into()));
        vm.apply(A::Click {
            name: "a1.txt".into(),
            ctrl: false,
            shift: false,
        });
        let cwd = vm.listing.cwd_path().to_string();
        vm.apply(A::SetListingStyle(ListingStyle::CombinedTree));
        assert_eq!(vm.listing.cwd_path(), cwd);
        assert_eq!(vm.listing.selection(), ["a1.txt"]);
        assert_eq!(cursor_name(&vm), "a1.txt");
        expand(&mut vm, "inner");
        vm.apply(click(id(&vm, "deep.txt"), false, false));
        // Leaving the tree puts the cursor on the top row that held it and drops the deep selection.
        vm.apply(A::SetListingStyle(ListingStyle::FlatList));
        assert_eq!(vm.listing.cwd_path(), cwd);
        assert_eq!(cursor_name(&vm), "inner");
        assert!(!vm.has_selection());
        vm.apply(A::SetListingStyle(ListingStyle::TreeAndList));
        assert_eq!(vm.listing.cwd_path(), cwd);
        assert_eq!(vm.folder_tree()[vm.tree_cursor()].entry.name, "a");
    }

    #[test]
    fn a_profile_can_forbid_a_style() {
        let mut profile = PaneProfile::commander();
        profile.styles = &[ListingStyle::FlatList];
        let mut vm = BrowserPaneController::with_profile(PaneServices::headless(), profile);
        let fx = vm.apply(A::SetListingStyle(ListingStyle::CombinedTree));
        assert_eq!(vm.listing_style(), ListingStyle::FlatList);
        assert!(
            matches!(&fx[..], [crate::controller::Effect::Message(m)] if m.contains("not available"))
        );
    }
}
