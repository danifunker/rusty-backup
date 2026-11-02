//! One Commander pane's state: listing, staged-edit queue and expanded wrappers.

use std::collections::HashMap;

use crate::model::dir_listing::DirListing;
use crate::model::edit_queue::EditQueue;
use crate::model::wrapper_tree::{TreeRow, WrapperTree};

use super::rows::{build_display_rows, DisplayRow};

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
    /// The directory the wrappers were expanded in; leaving it collapses them.
    pub last_cwd: String,
}

impl Default for CommanderPaneController {
    fn default() -> Self {
        Self::new()
    }
}

impl CommanderPaneController {
    pub fn new() -> Self {
        Self {
            listing: DirListing::new(),
            queue: EditQueue::new(),
            wrapper_tree: WrapperTree::new(),
            tree_selected: Vec::new(),
            tree_anchor: None,
            tree_index: HashMap::new(),
            last_cwd: String::new(),
        }
    }

    /// The rows to draw, rebuilding [`Self::tree_index`]; `remote_host` disables wrapper descent.
    pub fn display_rows(&mut self, remote_host: bool) -> Vec<DisplayRow> {
        build_display_rows(
            &self.listing,
            &self.queue,
            &mut self.wrapper_tree,
            &mut self.tree_index,
            remote_host,
        )
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
        let rows = vm.display_rows(false);
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
        let rows = vm.display_rows(false);
        assert_eq!(row(&rows, "gone.txt").kind, RowKind::PendingDelete);
        assert_eq!(row(&rows, "keep.txt").kind, RowKind::Normal);
        assert_eq!(row(&rows, "new-dir").kind, RowKind::PendingAdd);
    }

    #[test]
    fn a_remote_host_pane_tints_images_and_never_wraps() {
        let (_d, mut vm) = host_pane(&["disk.img", "notes.txt"], &[]);
        let rows = vm.display_rows(true);
        let img = row(&rows, "disk.img");
        assert!(img.is_image && !img.is_wrapper && !img.expandable);
        assert!(!row(&rows, "notes.txt").is_image);
    }
}
