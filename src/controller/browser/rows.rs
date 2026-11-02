//! The rows a Commander pane shows: the listing with the staged-edit overlay and expanded wrappers.

use std::collections::HashMap;

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
    let allow_wrap = !remote_host;
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
            },
            Row::Entry(e) => {
                let rename_to = queue.pending_rename_for(&e.path).map(str::to_string);
                let kind = if queue.is_pending_delete(&e.path) {
                    RowKind::PendingDelete
                } else if rename_to.is_some() {
                    RowKind::PendingRename
                } else {
                    RowKind::Normal
                };
                let is_wrapper = allow_wrap && !e.is_directory() && classify_entry(e).is_some();
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
                    depth: 0,
                    expandable: is_wrapper,
                    expanded: is_wrapper && wrapper_tree.is_expanded(&e.path),
                    is_tree: false,
                    is_wrapper,
                }
            }
        })
        .collect();

    for e in queue.pending_adds_for(&cwd_path) {
        base.push(DisplayRow {
            name: e.name.clone(),
            is_dir: e.is_directory(),
            size: e.size,
            rsrc_size: e.resource_fork_size.unwrap_or(0),
            modified: String::new(),
            type_tag: type_tag(&e),
            kind: RowKind::PendingAdd,
            rename_to: None,
            meta_changed: false,
            is_image: false,
            node_id: e.path.clone(),
            depth: 0,
            expandable: false,
            expanded: false,
            is_tree: false,
            is_wrapper: false,
        });
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
