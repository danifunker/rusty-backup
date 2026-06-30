//! Inline wrapper expansion for Commander Mode — the `+`/`-` tree.
//!
//! A pane row that is a Mac archive or a disk image can be expanded *in place*
//! (rather than navigated into fullscreen): clicking `+` mounts the wrapper's
//! contents and renders them as indented child rows beneath it. Folders and
//! further wrappers inside expand the same way, to any depth. Selecting files
//! in the expanded tree and copying pulls them out through the normal copy path
//! (both forks + Finder info preserved) — the pane exposes the mounted
//! filesystem as the copy source.
//!
//! This module is GUI-free and owns the mounts (opened wrapper filesystems +
//! their temp files), the expansion state, and the recursive walk that turns
//! them into a flat row list. The pane drives it: it supplies the wrapper bytes
//! (read from the host or a parent layer) and renders the rows.

use std::collections::{HashMap, HashSet};

use anyhow::Result;

use crate::fs::entry::FileEntry;
use crate::fs::filesystem::Filesystem;
use crate::model::commander_descend::{self, DescendKind};

/// Separator used to build a node id from its ancestor chain. Control char so
/// it can't collide with a real filename component.
const SEP: char = '\u{1}';

/// One opened wrapper: its filesystem plus, for image mounts, the temp file
/// backing it (kept alive for the mount's lifetime). Archives are read from an
/// owned byte buffer and need no temp file.
struct Mount {
    fs: Box<dyn Filesystem>,
    _temp: Option<tempfile::TempDir>,
}

/// One visible row produced by the tree walk (a descendant of an expanded
/// wrapper — base rows come from the pane's `DirListing`, not from here).
#[derive(Clone)]
pub struct TreeRow {
    pub entry: FileEntry,
    /// Stable identity across the virtual tree (expand + selection key).
    pub node_id: String,
    /// Mount key whose filesystem owns `entry` (read it from there).
    pub mount: String,
    /// Indent level (1 = direct child of a base wrapper).
    pub depth: usize,
    /// A folder or a nested wrapper — has a `+`/`-` toggle.
    pub expandable: bool,
    pub expanded: bool,
    /// True for a nested wrapper row (vs. a plain folder).
    pub is_wrapper: bool,
}

/// Per-pane inline expansion state.
#[derive(Default)]
pub struct WrapperTree {
    /// node_id -> opened wrapper. Only wrapper nodes get a mount; folders read
    /// from their enclosing wrapper's mount.
    mounts: HashMap<String, Mount>,
    /// Expanded node ids (wrappers and folders alike).
    expanded: HashSet<String>,
    /// Informational notes per node (e.g. multi-volume image fallback).
    notes: HashMap<String, String>,
}

impl WrapperTree {
    pub fn new() -> Self {
        Self::default()
    }

    /// Drop all mounts and expansion (on source / partition switch).
    pub fn clear(&mut self) {
        self.mounts.clear();
        self.expanded.clear();
        self.notes.clear();
    }

    /// True when no wrapper is expanded (the pane renders a plain listing).
    pub fn is_idle(&self) -> bool {
        self.expanded.is_empty()
    }

    pub fn is_expanded(&self, node_id: &str) -> bool {
        self.expanded.contains(node_id)
    }

    /// Build a child node id under `parent_id`.
    pub fn child_id(parent_id: &str, name: &str) -> String {
        format!("{parent_id}{SEP}{name}")
    }

    /// Collapse a node (and everything under it stays mounted but hidden).
    pub fn collapse(&mut self, node_id: &str) {
        self.expanded.remove(node_id);
    }

    /// Expand a wrapper node, opening its mount from `bytes` on first use.
    /// `kind` selects archive-vs-image handling (the caller classifies the
    /// entry, which is more reliable than the file name for odd extensions).
    /// Returns an optional note (e.g. a multi-volume image showing only its
    /// first volume).
    pub fn expand_wrapper(
        &mut self,
        node_id: &str,
        file_name: &str,
        kind: DescendKind,
        bytes: Vec<u8>,
    ) -> Result<()> {
        if !self.mounts.contains_key(node_id) {
            let (mount, note) = open_mount(bytes, file_name, kind)?;
            self.mounts.insert(node_id.to_string(), mount);
            if let Some(n) = note {
                self.notes.insert(node_id.to_string(), n);
            }
        }
        self.expanded.insert(node_id.to_string());
        Ok(())
    }

    /// Expand a folder node (its enclosing wrapper is already mounted).
    pub fn expand_folder(&mut self, node_id: &str) {
        self.expanded.insert(node_id.to_string());
    }

    /// Note attached to a node, if any.
    pub fn note(&self, node_id: &str) -> Option<&str> {
        self.notes.get(node_id).map(String::as_str)
    }

    /// The filesystem of a mount, for reads / copy.
    pub fn mount_fs(&mut self, mount_key: &str) -> Option<&mut (dyn Filesystem + 'static)> {
        self.mounts.get_mut(mount_key).map(|m| m.fs.as_mut())
    }

    /// Read the openable bytes of a nested wrapper row so the pane can hand them
    /// back to [`expand_wrapper`]. Reads the data fork from the owning mount; if
    /// the entry is an NDIF disk image (its resource fork carries a `bcem` block
    /// map), reconstruct the raw image from both forks so the result opens like
    /// any flat disk.
    pub fn read_wrapper_bytes(&mut self, row: &TreeRow) -> Result<Vec<u8>> {
        let fs = self
            .mount_fs(&row.mount)
            .ok_or_else(|| anyhow::anyhow!("wrapper mount not open"))?;
        let mut data = Vec::new();
        fs.write_file_to(&row.entry, &mut data)
            .map_err(|e| anyhow::anyhow!("read '{}': {e}", row.entry.name))?;
        let mut rsrc = Vec::new();
        let _ = fs.write_resource_fork_to(&row.entry, &mut rsrc);
        if let Some(bcem) = crate::rbformats::ndif::extract_bcem(&rsrc) {
            return crate::rbformats::ndif::reconstruct(&data, &bcem)
                .map_err(|e| anyhow::anyhow!("NDIF '{}': {e:#}", row.entry.name));
        }
        Ok(data)
    }

    /// Walk the expanded subtree rooted at base wrapper `wrapper_id` (already
    /// mounted), producing indented rows. `base_depth` is the indent of the
    /// wrapper's direct children (typically 1).
    pub fn subtree_rows(&mut self, wrapper_id: &str, base_depth: usize) -> Vec<TreeRow> {
        let mut rows = Vec::new();
        let children = self.root_children(wrapper_id);
        self.walk(&mut rows, wrapper_id, wrapper_id, children, base_depth);
        rows
    }

    /// Children of a mount's root.
    fn root_children(&mut self, mount_key: &str) -> Vec<FileEntry> {
        let Some(mount) = self.mounts.get_mut(mount_key) else {
            return Vec::new();
        };
        let Ok(root) = mount.fs.root() else {
            return Vec::new();
        };
        sorted(mount.fs.list_directory(&root).unwrap_or_default())
    }

    /// Children of a folder entry within `mount_key`.
    fn folder_children(&mut self, mount_key: &str, dir: &FileEntry) -> Vec<FileEntry> {
        let Some(mount) = self.mounts.get_mut(mount_key) else {
            return Vec::new();
        };
        sorted(mount.fs.list_directory(dir).unwrap_or_default())
    }

    fn walk(
        &mut self,
        rows: &mut Vec<TreeRow>,
        owner_mount: &str,
        id_prefix: &str,
        entries: Vec<FileEntry>,
        depth: usize,
    ) {
        for e in entries {
            let node_id = Self::child_id(id_prefix, &e.name);
            let is_wrapper = !e.is_directory() && commander_descend::classify_entry(&e).is_some();
            let expandable = e.is_directory() || is_wrapper;
            let expanded = expandable && self.expanded.contains(&node_id);
            rows.push(TreeRow {
                entry: e.clone(),
                node_id: node_id.clone(),
                mount: owner_mount.to_string(),
                depth,
                expandable,
                expanded,
                is_wrapper,
            });
            if !expanded {
                continue;
            }
            if is_wrapper {
                // Nested wrapper: recurse into its own mount (opened on expand).
                if self.mounts.contains_key(&node_id) {
                    let kids = self.root_children(&node_id);
                    self.walk(rows, &node_id, &node_id, kids, depth + 1);
                }
            } else {
                // Folder: children come from the same enclosing mount.
                let kids = self.folder_children(owner_mount, &e);
                self.walk(rows, owner_mount, &node_id, kids, depth + 1);
            }
        }
    }
}

/// Directories first, then case-insensitive by name — matching the flat grid.
fn sorted(mut entries: Vec<FileEntry>) -> Vec<FileEntry> {
    entries.sort_by_key(|e| e.name.to_lowercase());
    entries.sort_by_key(|e| !e.is_directory());
    entries
}

/// Open a wrapper's bytes as a mount. Archives open directly; images open their
/// first browsable volume (with a note when more than one exists — a richer
/// per-volume tree is a follow-up).
fn open_mount(
    bytes: Vec<u8>,
    file_name: &str,
    kind: DescendKind,
) -> Result<(Mount, Option<String>)> {
    match kind {
        DescendKind::Archive => {
            // Archives are read from the owned buffer — no temp file needed.
            let fs = commander_descend::open_archive_bytes(
                bytes,
                file_name,
                Some(file_name.to_string()),
            )?;
            Ok((Mount { fs, _temp: None }, None))
        }
        DescendKind::DiskImage => {
            // Images open by path (the partition probe + readers seek a file).
            let (temp, path) = commander_descend::materialize(&bytes, file_name)?;
            let parts = commander_descend::browsable_partitions(&path)?;
            let (_, first) = parts
                .first()
                .ok_or_else(|| anyhow::anyhow!("'{file_name}' has no browsable volumes"))?;
            let fs = commander_descend::open_image_partition(&path, first)?;
            let note = (parts.len() > 1).then(|| {
                format!(
                    "showing volume '{}' (1 of {})",
                    first.type_name,
                    parts.len()
                )
            });
            Ok((
                Mount {
                    fs,
                    _temp: Some(temp),
                },
                note,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::macarchive::stuffit::{
        build_archive_tree, StuffItInput, StuffItInputNode, WriteMethod,
    };

    fn file(name: &str, data: &[u8]) -> StuffItInputNode {
        StuffItInputNode::File(StuffItInput {
            name: name.into(),
            type_code: *b"TEXT",
            creator_code: *b"ttxt",
            finder_flags: 0,
            create_date: 0,
            mod_date: 0,
            data_fork: data.to_vec(),
            resource_fork: Vec::new(),
        })
    }

    /// Archive bytes: root file + a folder holding a file.
    fn archive_bytes() -> Vec<u8> {
        let tree = vec![
            file("top.txt", b"top"),
            StuffItInputNode::Folder {
                name: "Sub".into(),
                finder_flags: 0,
                create_date: 0,
                mod_date: 0,
                children: vec![file("inner.txt", b"deep")],
            },
        ];
        build_archive_tree(&tree, WriteMethod::Store).unwrap()
    }

    #[test]
    fn expand_collapse_and_walk() {
        let mut tree = WrapperTree::new();
        assert!(tree.is_idle());

        // Expanding the base wrapper shows its top-level rows (folder first).
        tree.expand_wrapper("/a.sit", "a.sit", DescendKind::Archive, archive_bytes())
            .unwrap();
        let rows = tree.subtree_rows("/a.sit", 1);
        let names: Vec<_> = rows.iter().map(|r| r.entry.name.as_str()).collect();
        assert_eq!(names, vec!["Sub", "top.txt"]);
        assert!(rows[0].expandable && rows[0].entry.is_directory());
        assert_eq!(rows[0].depth, 1);

        // Expand the inner folder: its child appears, deeper-indented.
        let sub_id = WrapperTree::child_id("/a.sit", "Sub");
        tree.expand_folder(&sub_id);
        let rows = tree.subtree_rows("/a.sit", 1);
        let pos = rows
            .iter()
            .position(|r| r.entry.name == "inner.txt")
            .unwrap();
        assert_eq!(rows[pos].depth, 2);
        assert_eq!(rows[pos].mount, "/a.sit");

        // Collapse the base wrapper: nothing is walked (idle once empty).
        tree.collapse("/a.sit");
        tree.collapse(&sub_id);
        assert!(tree.is_idle());
    }

    #[test]
    fn reads_a_file_from_the_mount() {
        let mut tree = WrapperTree::new();
        tree.expand_wrapper("/a.sit", "a.sit", DescendKind::Archive, archive_bytes())
            .unwrap();
        let rows = tree.subtree_rows("/a.sit", 1);
        let top = rows.iter().find(|r| r.entry.name == "top.txt").unwrap();
        let fs = tree.mount_fs(&top.mount).unwrap();
        let mut buf = Vec::new();
        fs.write_file_to(&top.entry, &mut buf).unwrap();
        assert_eq!(buf, b"top");
    }
}
