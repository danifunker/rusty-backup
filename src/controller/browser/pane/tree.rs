//! The folder tree beside a pane's listing (profile `folder_tree`): the TUI Explorer's left pane.
//!
//! The tree's cursor is always the listing's current folder: moving it navigates the listing,
//! and any other navigation (Enter on a folder, Parent, a reopen after Apply) moves it, expanding
//! the folders above. A reload or source change drops the tree, which is rebuilt from the root.
//! While the tree has the keyboard, cursor moves, Open and Parent act on it instead of the list.

use disk_core::entry::FileEntry;

use super::{BrowserPaneAction, BrowserPaneController};

/// One folder of the tree, in display order (depth-first).
#[derive(Debug, Clone)]
pub struct TreeNode {
    pub entry: FileEntry,
    pub depth: usize,
    pub expanded: bool,
}

#[derive(Default)]
pub(super) struct FolderTree {
    nodes: Vec<TreeNode>,
    sel: usize,
    focused: bool,
}

impl FolderTree {
    pub(super) fn new(focused: bool) -> Self {
        FolderTree {
            nodes: Vec::new(),
            sel: 0,
            focused,
        }
    }

    pub(super) fn clear(&mut self) {
        self.nodes.clear();
        self.sel = 0;
    }
}

fn folders_first(mut dirs: Vec<FileEntry>) -> Vec<FileEntry> {
    dirs.retain(|e| e.is_directory());
    dirs.sort_by_key(|e| e.name.to_lowercase());
    dirs
}

impl BrowserPaneController {
    /// The tree's folders in display order; empty unless the profile has a folder tree.
    pub fn folder_tree(&self) -> &[TreeNode] {
        &self.folders.nodes
    }

    /// The tree row of the current folder.
    pub fn tree_cursor(&self) -> usize {
        self.folders.sel
    }

    /// True while the tree, not the listing, has the keyboard.
    pub fn tree_focused(&self) -> bool {
        self.profile.folder_tree && self.folders.focused
    }

    /// Expand `i`'s child folders under it, listing them now (quick once the volume is open).
    fn tree_expand(&mut self, i: usize) {
        let Some(node) = self.folders.nodes.get(i).filter(|n| !n.expanded) else {
            return;
        };
        let (dir, depth) = (node.entry.clone(), node.depth);
        let kids = folders_first(self.listing.list_dir(&dir).unwrap_or_default());
        self.folders.nodes[i].expanded = true;
        let nodes = kids.into_iter().map(|entry| TreeNode {
            entry,
            depth: depth + 1,
            expanded: false,
        });
        self.folders.nodes.splice(i + 1..i + 1, nodes);
    }

    fn tree_collapse(&mut self, i: usize) {
        let depth = self.folders.nodes[i].depth;
        let end = (i + 1..self.folders.nodes.len())
            .find(|&j| self.folders.nodes[j].depth <= depth)
            .unwrap_or(self.folders.nodes.len());
        self.folders.nodes.drain(i + 1..end);
        self.folders.nodes[i].expanded = false;
    }

    /// Rebuild the tree if it is gone, then expand down to the current folder and select it.
    pub(super) fn sync_tree(&mut self) {
        if !self.profile.folder_tree {
            return;
        }
        let Some(root) = self
            .listing
            .root_entry()
            .filter(|_| self.listing.is_loaded())
        else {
            self.folders.clear();
            return;
        };
        if self.folders.nodes.first().map(|n| &n.entry.path) != Some(&root.path) {
            self.folders.clear();
            self.folders.nodes.push(TreeNode {
                entry: root.clone(),
                depth: 0,
                expanded: false,
            });
        }
        let cwd = self.listing.cwd_path().to_string();
        let rel = cwd.strip_prefix(root.path.as_str()).unwrap_or(&cwd);
        let mut at = 0;
        self.tree_expand(0);
        for comp in rel.split('/').filter(|c| !c.is_empty()) {
            self.tree_expand(at);
            let depth = self.folders.nodes[at].depth + 1;
            let child = (at + 1..self.folders.nodes.len())
                .take_while(|&j| self.folders.nodes[j].depth >= depth)
                .find(|&j| {
                    self.folders.nodes[j].depth == depth && self.folders.nodes[j].entry.name == comp
                });
            match child {
                Some(j) => at = j,
                None => break,
            }
        }
        self.folders.sel = at;
    }

    /// Move the tree cursor to row `i` and show that folder in the listing.
    fn tree_select(&mut self, i: usize) {
        let Some(node) = self.folders.nodes.get(i) else {
            return;
        };
        let path = node.entry.path.clone();
        let before = self.listing.cwd_path().to_string();
        if let Err(e) = self.listing.navigate_to(&path) {
            self.error = Some(format!("cannot open '{path}': {e}"));
            return;
        }
        self.folders.sel = i;
        self.navigated(&before, None);
    }

    /// The tree's own handling of `action` while it has the keyboard; `None` leaves it to the list.
    pub(super) fn tree_key(&mut self, action: &BrowserPaneAction) -> Option<()> {
        use BrowserPaneAction as A;
        if !self.tree_focused() || self.folders.nodes.is_empty() {
            return None;
        }
        let last = self.folders.nodes.len() - 1;
        let sel = self.folders.sel.min(last);
        let to = |d: i64| (sel as i64 + d).max(0).min(last as i64) as usize;
        match action {
            A::CursorUp => self.tree_select(to(-1)),
            A::CursorDown => self.tree_select(to(1)),
            A::CursorPage(n) => self.tree_select(to(i64::from(*n))),
            A::CursorHome => self.tree_select(0),
            A::CursorEnd => self.tree_select(last),
            A::ActivateCursor => self.tree_expand(sel),
            A::Up if self.folders.nodes[sel].expanded && sel > 0 => self.tree_collapse(sel),
            A::Up => {
                let depth = self.folders.nodes[sel].depth;
                if let Some(p) = (0..sel)
                    .rev()
                    .find(|&j| self.folders.nodes[j].depth < depth)
                {
                    self.tree_select(p);
                }
            }
            // Marks belong to the listing's rows.
            A::ToggleMark => {}
            _ => return None,
        }
        Some(())
    }

    /// Hand the keyboard to the tree or back to the listing.
    pub(super) fn switch_focus(&mut self) {
        if self.profile.folder_tree {
            self.folders.focused = !self.folders.focused;
        }
    }
}
