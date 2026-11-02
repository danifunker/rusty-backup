//! The rows a Commander pane shows: the listing with the staged-edit overlay and expanded wrappers.

use std::collections::{HashMap, HashSet};

use disk_core::display::format_size;
use disk_core::entry::FileEntry;

use crate::model::commander_descend::{classify_entry, DescendKind};
use crate::model::dir_listing::{type_tag, DirListing, Row};
use crate::model::edit_queue::EditQueue;
use crate::model::wrapper_tree::{TreeRow, WrapperTree};
use crate::partition::PartitionInfo;

/// How a row participates in the staged-edit overlay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowKind {
    Parent,
    Normal,
    PendingDelete,
    PendingAdd,
    PendingRename,
}

/// One row as a shell draws it, owned so the shell can act on the pane while iterating.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisplayRow {
    pub name: String,
    pub is_dir: bool,
    pub size: u64,
    /// Resource-fork size in bytes (0 = none). Mac filesystems / archives only.
    pub rsrc_size: u64,
    pub modified: String,
    pub type_tag: String,
    pub kind: RowKind,
    /// For a `PendingRename` row, the staged new name (shown as `old -> new`).
    pub rename_to: Option<String>,
    /// A metadata edit (type/creator, dates, permissions, ...) is staged for this entry.
    pub meta_changed: bool,
    /// A remote-host file whose name looks like a disk image we can open.
    pub is_image: bool,
    /// Stable tree node id (base-row path, or a wrapper subtree id): the expand and selection key.
    pub node_id: String,
    /// Indent level: 0 for base rows, >=1 for rows inside an expanded wrapper.
    pub depth: usize,
    /// Has a `+`/`-` disclosure toggle (a wrapper, or a folder inside a wrapper).
    pub expandable: bool,
    pub expanded: bool,
    /// Produced from inside an expanded wrapper, rather than a base row.
    pub is_tree: bool,
    /// A wrapper row (archive / disk image / optical), tagged `<ARC>` / `<IMG>` / `<CD>`.
    pub is_wrapper: bool,
    /// Below an expanded folder of the pane's own volume (the combined tree).
    pub in_folder: bool,
    /// An expanded folder whose children are still being listed.
    pub loading: bool,
}

impl DisplayRow {
    pub fn is_parent(&self) -> bool {
        self.kind == RowKind::Parent
    }
}

/// Builds a pane's rows: entries flagged by the queue, pending adds appended, and expanded
/// wrappers spliced in beneath their row. `tree_index` is rebuilt to map wrapper node ids
/// back to their tree rows. Wrappers only descend on local panes (`remote_host` false).
pub fn build_display_rows(
    listing: &DirListing,
    queue: &EditQueue,
    wrapper_tree: &mut WrapperTree,
    tree_index: &mut HashMap<String, TreeRow>,
    remote_host: bool,
) -> Vec<DisplayRow> {
    let cwd_path = listing.cwd_path().to_string();
    let mut base: Vec<DisplayRow> = listing
        .current_rows()
        .into_iter()
        .map(|r| match r {
            Row::Parent => DisplayRow {
                name: "..".to_string(),
                is_dir: true,
                size: 0,
                rsrc_size: 0,
                modified: String::new(),
                type_tag: String::new(),
                kind: RowKind::Parent,
                rename_to: None,
                meta_changed: false,
                is_image: false,
                node_id: String::new(),
                depth: 0,
                expandable: false,
                expanded: false,
                is_tree: false,
                is_wrapper: false,
                in_folder: false,
                loading: false,
            },
            Row::Entry(e) => entry_row(e, queue, wrapper_tree, remote_host, 0),
        })
        .collect();

    for e in queue.pending_adds_for(&cwd_path) {
        base.push(pending_add_row(&e, 0));
    }

    tree_index.clear();
    let mut rows = Vec::with_capacity(base.len());
    for row in base {
        let expand_here = row.expanded && row.expandable && !row.is_tree;
        let node_id = row.node_id.clone();
        rows.push(row);
        if expand_here {
            for tr in wrapper_tree.subtree_rows(&node_id, 1) {
                tree_index.insert(tr.node_id.clone(), tr.clone());
                rows.push(tree_row_to_display(&tr));
            }
        }
    }
    rows
}

/// A listed entry's row, flagged by the queue; wrappers only on local panes.
fn entry_row(
    e: &FileEntry,
    queue: &EditQueue,
    wrapper_tree: &WrapperTree,
    remote_host: bool,
    depth: usize,
) -> DisplayRow {
    let rename_to = queue.pending_rename_for(&e.path).map(str::to_string);
    let kind = if queue.is_pending_delete(&e.path) {
        RowKind::PendingDelete
    } else if rename_to.is_some() {
        RowKind::PendingRename
    } else {
        RowKind::Normal
    };
    let is_wrapper = !remote_host && !e.is_directory() && classify_entry(e).is_some();
    DisplayRow {
        is_image: remote_host && !e.is_directory() && looks_like_disk_image(&e.name),
        name: e.name.clone(),
        is_dir: e.is_directory(),
        size: e.size,
        rsrc_size: e.resource_fork_size.unwrap_or(0),
        modified: e.modified.clone().unwrap_or_default(),
        type_tag: wrapper_or_type_tag(e, is_wrapper),
        kind,
        rename_to,
        meta_changed: queue.has_pending_metadata(&e.path),
        node_id: e.path.clone(),
        depth,
        expandable: is_wrapper,
        expanded: is_wrapper && wrapper_tree.is_expanded(&e.path),
        is_tree: false,
        is_wrapper,
        in_folder: depth > 0,
        loading: false,
    }
}

fn pending_add_row(e: &FileEntry, depth: usize) -> DisplayRow {
    DisplayRow {
        name: e.name.clone(),
        is_dir: e.is_directory(),
        size: e.size,
        rsrc_size: e.resource_fork_size.unwrap_or(0),
        modified: String::new(),
        type_tag: type_tag(e),
        kind: RowKind::PendingAdd,
        rename_to: None,
        meta_changed: false,
        is_image: false,
        node_id: e.path.clone(),
        depth,
        expandable: false,
        expanded: false,
        is_tree: false,
        is_wrapper: false,
        in_folder: depth > 0,
        loading: false,
    }
}

/// A row below an expanded folder: its entry and the folder it is in.
#[derive(Debug, Clone)]
pub struct FolderRow {
    pub entry: FileEntry,
    pub parent: FileEntry,
}

/// The combined tree's folders: which are expanded and what each was listed to hold.
pub struct FolderSplice<'a> {
    pub expanded: &'a HashSet<String>,
    pub children: &'a HashMap<String, Vec<FileEntry>>,
    pub queue: &'a EditQueue,
    pub remote_host: bool,
}

/// Turn the base rows' folders into an expandable tree: children of each expanded, listed folder
/// go under its row (recursively), wrappers expanded there splice in too. Fills `index`.
pub fn splice_folders(
    rows: Vec<DisplayRow>,
    base: &[FileEntry],
    f: &FolderSplice<'_>,
    wrapper_tree: &mut WrapperTree,
    tree_index: &mut HashMap<String, TreeRow>,
    index: &mut HashMap<String, FolderRow>,
) -> Vec<DisplayRow> {
    index.clear();
    let mut out = Vec::with_capacity(rows.len());
    let base_dirs: HashMap<&str, &FileEntry> = base
        .iter()
        .filter(|e| e.is_directory())
        .map(|e| (e.path.as_str(), e))
        .collect();
    for mut row in rows {
        let base_row = row.depth == 0 && row.kind != RowKind::Parent && !row.is_tree;
        let dir = base_row
            .then(|| base_dirs.get(row.node_id.as_str()).map(|e| (*e).clone()))
            .flatten();
        let Some(dir) = dir else {
            let staged_dir = (row.kind == RowKind::PendingAdd && row.is_dir)
                .then(|| staged_entry(f.queue, &row.node_id))
                .flatten();
            out.push(row);
            if let Some(d) = staged_dir {
                staged_children(&mut out, &d, 1, f, index);
            }
            continue;
        };
        mark_folder(&mut row, f);
        let open = row.expanded && !row.loading;
        out.push(row);
        if open {
            folder_children(&mut out, &dir, 1, f, wrapper_tree, tree_index, index);
        }
    }
    out
}

fn mark_folder(row: &mut DisplayRow, f: &FolderSplice<'_>) {
    row.expandable = true;
    row.expanded = f.expanded.contains(&row.node_id);
    row.loading = row.expanded && !f.children.contains_key(&row.node_id);
}

fn folder_children(
    out: &mut Vec<DisplayRow>,
    dir: &FileEntry,
    depth: usize,
    f: &FolderSplice<'_>,
    wrapper_tree: &mut WrapperTree,
    tree_index: &mut HashMap<String, TreeRow>,
    index: &mut HashMap<String, FolderRow>,
) {
    let Some(kids) = f.children.get(&dir.path) else {
        return;
    };
    for e in kids {
        let mut row = entry_row(e, f.queue, wrapper_tree, f.remote_host, depth);
        index.insert(
            e.path.clone(),
            FolderRow {
                entry: e.clone(),
                parent: dir.clone(),
            },
        );
        if e.is_directory() {
            mark_folder(&mut row, f);
        }
        let (open_dir, open_wrapper) = (
            e.is_directory() && row.expanded && !row.loading,
            row.is_wrapper && row.expanded,
        );
        out.push(row);
        if open_dir {
            folder_children(out, e, depth + 1, f, wrapper_tree, tree_index, index);
        } else if open_wrapper {
            for tr in wrapper_tree.subtree_rows(&e.path, depth + 1) {
                tree_index.insert(tr.node_id.clone(), tr.clone());
                out.push(tree_row_to_display(&tr));
            }
        }
    }
    staged_children(out, dir, depth, f, index);
}

/// The staged-add entry at `path`, looked up in its parent folder's adds.
fn staged_entry(queue: &EditQueue, path: &str) -> Option<FileEntry> {
    let parent = match path.rsplit_once('/') {
        Some(("", _)) | None => "/",
        Some((p, _)) => p,
    };
    let mut adds = queue.pending_adds_for(parent).into_iter();
    adds.find(|e| e.path == path)
}

/// The staged adds in `dir`, a staged folder's own adds always shown open beneath it.
fn staged_children(
    out: &mut Vec<DisplayRow>,
    dir: &FileEntry,
    depth: usize,
    f: &FolderSplice<'_>,
    index: &mut HashMap<String, FolderRow>,
) {
    for e in f.queue.pending_adds_for(&dir.path) {
        index.insert(
            e.path.clone(),
            FolderRow {
                entry: e.clone(),
                parent: dir.clone(),
            },
        );
        out.push(pending_add_row(&e, depth));
        if e.is_directory() {
            staged_children(out, &e, depth + 1, f, index);
        }
    }
}

/// Whether a filename's extension is one of the disk-image containers the engine can open.
pub fn looks_like_disk_image(name: &str) -> bool {
    name.rsplit_once('.')
        .map(|(_, ext)| {
            crate::model::file_types::DISK_IMAGE_EXTS.contains(&ext.to_ascii_lowercase().as_str())
        })
        .unwrap_or(false)
}

/// Type-column text for a base entry: `<ARC>` / `<IMG>` / `<CD>` for a wrapper, else the filesystem's tag.
pub fn wrapper_or_type_tag(e: &FileEntry, is_wrapper: bool) -> String {
    if is_wrapper {
        match classify_entry(e) {
            Some(DescendKind::Archive) => "<ARC>".into(),
            Some(DescendKind::DiskImage) => "<IMG>".into(),
            Some(DescendKind::Optical) => "<CD>".into(),
            None => type_tag(e),
        }
    } else {
        type_tag(e)
    }
}

/// A wrapper-tree row as a display row.
pub fn tree_row_to_display(tr: &TreeRow) -> DisplayRow {
    DisplayRow {
        name: tr.entry.name.clone(),
        is_dir: tr.entry.is_directory(),
        size: tr.entry.size,
        rsrc_size: tr.entry.resource_fork_size.unwrap_or(0),
        modified: tr.entry.modified.clone().unwrap_or_default(),
        type_tag: wrapper_or_type_tag(&tr.entry, tr.is_wrapper),
        kind: RowKind::Normal,
        rename_to: None,
        meta_changed: false,
        is_image: false,
        node_id: tr.node_id.clone(),
        depth: tr.depth,
        expandable: tr.expandable,
        expanded: tr.expanded,
        is_tree: true,
        is_wrapper: tr.is_wrapper,
        in_folder: false,
        loading: false,
    }
}

/// Short label for a partition: `1: FAT16 (510.0 MiB)`. `pos` matches `inspect`'s `#` and `IMG@N`.
pub fn partition_label(pos: usize, p: &PartitionInfo) -> String {
    format!(
        "{}: {} ({})",
        pos + 1,
        p.type_name,
        format_size(p.size_bytes)
    )
}

/// The parent directory of a daemon-side path (`/a/b/c.img` -> `/a/b`).
pub fn remote_parent_dir(path: &str) -> String {
    match path.rfind('/') {
        Some(0) | None => "/".to_string(),
        Some(i) => path[..i].to_string(),
    }
}

/// True for a Mac-archive path (`.sit` / `.hqx` / `.sea` / `.cpt` / `.mar`), which a pane opens as an archive.
pub fn is_mac_archive_path(path: &std::path::Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|ext| {
            crate::model::file_types::MAC_ARCHIVE_EXTS
                .iter()
                .any(|a| a.eq_ignore_ascii_case(ext))
        })
        .unwrap_or(false)
}
