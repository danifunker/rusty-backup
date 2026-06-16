//! Flat single-directory listing model for Commander Mode.
//!
//! A [`DirListing`] is the per-pane model that Commander's two panes render
//! over: it owns the open [`Filesystem`], a stack of directory frames (the
//! current directory plus its ancestors, each with a cached, sorted child
//! list), the active sort column, and the multi-selection. It is *pure* model
//! state — no egui, no threading — so the selection / sort / navigation logic
//! is unit-testable without a GUI (see the tests at the bottom of this file).
//!
//! The slow part of opening a source — reading the catalog and the root
//! directory — is done off-thread by [`BrowseSession::spawn_open`]; the worker
//! hands the opened `Filesystem` plus the root listing to [`load_root`]. After
//! that, `enter` / `up` re-list directories synchronously against the held
//! filesystem (these reads are fast once the catalog is resident).
//!
//! [`load_root`]: DirListing::load_root
//! [`BrowseSession::spawn_open`]: crate::model::browse_session::BrowseSession::spawn_open

use crate::fs::entry::{EntryType, FileEntry};
use crate::fs::filesystem::{Filesystem, FilesystemError};

/// Sortable listing columns. Mirrors the Commander mock's column set.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum SortColumn {
    #[default]
    Name,
    Size,
    Modified,
    Type,
}

/// One directory in the navigation stack: the directory entry plus its
/// children, kept sorted (folders-first) by the listing's active sort.
struct Frame {
    dir: FileEntry,
    entries: Vec<FileEntry>,
}

/// A single rendered row. `Parent` is the synthetic `..` row; `Entry` borrows
/// one of the current directory's (sorted) children.
pub enum Row<'a> {
    /// The `..` parent-navigation row.
    Parent,
    /// A real filesystem entry.
    Entry(&'a FileEntry),
}

/// Per-pane directory listing: open filesystem, navigation stack, sort, and
/// multi-selection.
///
/// Selection identity is the entry **name**, which is unique within a single
/// directory; this mirrors the mock and keeps the selection valid across a
/// re-sort (which reorders rows but not names).
#[derive(Default)]
pub struct DirListing {
    /// The opened, read-only filesystem. `None` before a source is loaded
    /// (and in the pure unit tests, which drive the stack directly).
    fs: Option<Box<dyn Filesystem>>,
    /// Ancestor stack; `stack[0]` is the volume root, `stack.last()` is the
    /// current directory. Empty until [`load_root`](Self::load_root).
    stack: Vec<Frame>,
    /// Host-folder pane: `..` is always offered (a host pane may ascend above
    /// the folder it was opened at). Image panes hide `..` at the volume root.
    host_mode: bool,
    sort: SortColumn,
    descending: bool,
    /// Selected entry names within the current directory.
    selected: Vec<String>,
    /// Anchor row for Shift-range selection.
    anchor: Option<String>,
}

impl DirListing {
    /// An empty listing with no source loaded.
    pub fn new() -> Self {
        Self::default()
    }

    /// Install a freshly-opened filesystem and its root directory listing,
    /// resetting navigation and selection. `host_mode` true means a host
    /// folder pane (parent navigation is always available).
    pub fn load_root(
        &mut self,
        fs: Box<dyn Filesystem>,
        root: FileEntry,
        entries: Vec<FileEntry>,
        host_mode: bool,
    ) {
        self.fs = Some(fs);
        self.host_mode = host_mode;
        self.stack.clear();
        self.selected.clear();
        self.anchor = None;
        self.push_dir(root, entries);
    }

    /// True once a source has been loaded.
    pub fn is_loaded(&self) -> bool {
        !self.stack.is_empty()
    }

    /// The current directory entry, or `None` before a source loads.
    pub fn cwd(&self) -> Option<&FileEntry> {
        self.stack.last().map(|f| &f.dir)
    }

    /// Path of the current directory (`"/"` at the root), or `""` if unloaded.
    pub fn cwd_path(&self) -> &str {
        self.cwd().map(|e| e.path.as_str()).unwrap_or("")
    }

    /// True when the current directory is the volume root.
    pub fn at_root(&self) -> bool {
        self.stack.len() <= 1
    }

    /// Whether a `..` row should be shown: always for host panes, otherwise
    /// only when below the volume root.
    pub fn show_parent(&self) -> bool {
        self.host_mode || self.stack.len() > 1
    }

    pub fn sort_column(&self) -> SortColumn {
        self.sort
    }

    pub fn is_descending(&self) -> bool {
        self.descending
    }

    /// The current directory's children, already sorted (folders-first).
    pub fn entries(&self) -> &[FileEntry] {
        self.stack
            .last()
            .map(|f| f.entries.as_slice())
            .unwrap_or(&[])
    }

    /// Rows to render: a leading `..` when [`show_parent`](Self::show_parent)
    /// is set, then each sorted child.
    pub fn current_rows(&self) -> Vec<Row<'_>> {
        let mut rows = Vec::new();
        if self.show_parent() {
            rows.push(Row::Parent);
        }
        for e in self.entries() {
            rows.push(Row::Entry(e));
        }
        rows
    }

    // --- navigation --------------------------------------------------------

    /// Enter the child directory named `name` in the current directory,
    /// reading its listing from the held filesystem. Errors if the name is not
    /// a directory here, or if no filesystem is loaded.
    pub fn enter(&mut self, name: &str) -> Result<(), FilesystemError> {
        let child = self
            .entries()
            .iter()
            .find(|e| e.is_directory() && e.name == name)
            .cloned()
            .ok_or_else(|| FilesystemError::NotFound(format!("directory '{name}'")))?;
        let fs = self
            .fs
            .as_mut()
            .ok_or_else(|| FilesystemError::Parse("no filesystem loaded".into()))?;
        let entries = fs.list_directory(&child)?;
        self.push_dir(child, entries);
        self.clear_selection();
        Ok(())
    }

    /// Navigate to the parent directory. No-op at the volume root (a host
    /// pane's ascent above its opened root is handled by re-rooting the
    /// listing, not here).
    pub fn up(&mut self) {
        if self.stack.len() > 1 {
            self.stack.pop();
            self.clear_selection();
        }
    }

    /// Re-read the current directory from the filesystem, preserving the cwd
    /// but dropping the selection. Used after a source is mutated and re-opened.
    pub fn reload(&mut self) -> Result<(), FilesystemError> {
        let Some(dir) = self.cwd().cloned() else {
            return Ok(());
        };
        let fs = self
            .fs
            .as_mut()
            .ok_or_else(|| FilesystemError::Parse("no filesystem loaded".into()))?;
        let entries = fs.list_directory(&dir)?;
        if let Some(frame) = self.stack.last_mut() {
            frame.entries = entries;
        }
        self.sort_current();
        self.clear_selection();
        Ok(())
    }

    /// Push a new directory frame, sorting its children by the active sort.
    fn push_dir(&mut self, dir: FileEntry, entries: Vec<FileEntry>) {
        self.stack.push(Frame { dir, entries });
        self.sort_current();
    }

    // --- sorting -----------------------------------------------------------

    /// Sort by `col`. Clicking the active column flips its direction; clicking
    /// a different column selects it ascending.
    pub fn resort(&mut self, col: SortColumn) {
        if self.sort == col {
            self.descending = !self.descending;
        } else {
            self.sort = col;
            self.descending = false;
        }
        self.sort_current();
    }

    /// Sort the current frame's children in place: by the active column /
    /// direction, then a stable folders-first pass. Directories therefore stay
    /// grouped ahead of files regardless of column or direction.
    fn sort_current(&mut self) {
        let (col, desc) = (self.sort, self.descending);
        let Some(frame) = self.stack.last_mut() else {
            return;
        };
        frame.entries.sort_by(|a, b| cmp_by(a, b, col));
        if desc {
            frame.entries.reverse();
        }
        // Stable: keeps the just-applied within-group order, groups dirs first.
        frame.entries.sort_by_key(|e| !e.is_directory());
    }

    // --- selection ---------------------------------------------------------

    /// Names of the current rows in display order, excluding `..`.
    fn ordered_names(&self) -> Vec<String> {
        self.entries().iter().map(|e| e.name.clone()).collect()
    }

    /// Plain click: select exactly `name`, set it as the range anchor.
    pub fn click(&mut self, name: &str) {
        self.selected = vec![name.to_string()];
        self.anchor = Some(name.to_string());
    }

    /// Ctrl/Cmd-click: toggle `name` in/out of the selection; move the anchor.
    pub fn ctrl_click(&mut self, name: &str) {
        if let Some(pos) = self.selected.iter().position(|n| n == name) {
            self.selected.remove(pos);
        } else {
            self.selected.push(name.to_string());
        }
        self.anchor = Some(name.to_string());
    }

    /// Shift-click: select the contiguous range from the anchor row to `name`
    /// over the current display order. With no anchor, behaves like a click.
    pub fn shift_click(&mut self, name: &str) {
        let names = self.ordered_names();
        let anchor = self.anchor.clone().unwrap_or_else(|| name.to_string());
        let ia = names.iter().position(|n| n == &anchor);
        let ib = names.iter().position(|n| n == name);
        match (ia, ib) {
            (Some(ia), Some(ib)) => {
                let (lo, hi) = if ia <= ib { (ia, ib) } else { (ib, ia) };
                self.selected = names[lo..=hi].to_vec();
            }
            _ => self.selected = vec![name.to_string()],
        }
    }

    /// Clear the selection and the range anchor.
    pub fn clear_selection(&mut self) {
        self.selected.clear();
        self.anchor = None;
    }

    /// True if `name` is currently selected.
    pub fn is_selected(&self, name: &str) -> bool {
        self.selected.iter().any(|n| n == name)
    }

    /// The selected entry names, in selection order.
    pub fn selection(&self) -> &[String] {
        &self.selected
    }

    /// The selected entries, resolved against the current directory (selection
    /// names that no longer match an entry are skipped).
    pub fn selected_entries(&self) -> Vec<&FileEntry> {
        self.entries()
            .iter()
            .filter(|e| self.is_selected(&e.name))
            .collect()
    }

    /// Borrow the open filesystem mutably (e.g. to read a file for preview or
    /// copy). `None` before a source loads.
    pub fn fs_mut(&mut self) -> Option<&mut (dyn Filesystem + 'static)> {
        self.fs.as_deref_mut()
    }
}

/// Compare two entries by a single column (ascending). Folders-first grouping
/// is applied separately by [`DirListing::sort_current`].
fn cmp_by(a: &FileEntry, b: &FileEntry, col: SortColumn) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match col {
        SortColumn::Name => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
        SortColumn::Size => a.size.cmp(&b.size),
        SortColumn::Modified => match (&a.modified, &b.modified) {
            (Some(x), Some(y)) => x.cmp(y),
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        },
        SortColumn::Type => type_tag(a).cmp(&type_tag(b)),
    }
}

/// Short, filesystem-appropriate tag for the Type column. ASCII only (the
/// project's no-Unicode-glyph rule): `<DIR>`, `->` for symlinks, an HFS
/// type code, FAT/exFAT `AHSR` attribute letters, a Unix octal mode, or the
/// uppercased filename extension. Shared by the view (display) and the model
/// (sort key) so both agree.
pub fn type_tag(e: &FileEntry) -> String {
    match e.entry_type {
        EntryType::Directory => "<DIR>".to_string(),
        EntryType::Symlink => "->".to_string(),
        EntryType::Special => e
            .special_type
            .clone()
            .unwrap_or_else(|| "(special)".to_string()),
        EntryType::File => {
            if let Some(tc) = &e.type_code {
                if !tc.is_empty() {
                    return tc.clone();
                }
            }
            if let Some(attrs) = e.dos_attributes {
                return fat_attr_tag(attrs);
            }
            if let Some(mode) = e.mode {
                return format!("{:04o}", mode & 0o7777);
            }
            e.name
                .rsplit_once('.')
                .map(|(_, ext)| ext.to_uppercase())
                .unwrap_or_default()
        }
    }
}

/// `AHSR` letters for DOS attribute bits (archive/hidden/system/read-only),
/// dashes where unset.
fn fat_attr_tag(attrs: u16) -> String {
    let a = if attrs & 0x20 != 0 { 'A' } else { '-' };
    let h = if attrs & 0x02 != 0 { 'H' } else { '-' };
    let s = if attrs & 0x04 != 0 { 'S' } else { '-' };
    let r = if attrs & 0x01 != 0 { 'R' } else { '-' };
    format!("{a}{h}{s}{r}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a `DirListing` with a hand-made directory (no filesystem), for
    /// the pure sort / selection / parent-row tests.
    fn listing(entries: Vec<FileEntry>, host_mode: bool) -> DirListing {
        let mut l = DirListing::new();
        l.host_mode = host_mode;
        l.push_dir(FileEntry::root(), entries);
        l
    }

    fn file(name: &str, size: u64) -> FileEntry {
        FileEntry::new_file(name.to_string(), format!("/{name}"), size, 0)
    }

    fn dir(name: &str) -> FileEntry {
        FileEntry::new_directory(name.to_string(), format!("/{name}"), 0)
    }

    fn row_names(l: &DirListing) -> Vec<String> {
        l.current_rows()
            .iter()
            .map(|r| match r {
                Row::Parent => "..".to_string(),
                Row::Entry(e) => e.name.clone(),
            })
            .collect()
    }

    #[test]
    fn sort_groups_folders_first_by_name() {
        let l = listing(
            vec![
                file("zebra.txt", 10),
                dir("beta"),
                file("alpha.txt", 5),
                dir("alpha"),
            ],
            false,
        );
        // Folders (alpha, beta) before files (alpha.txt, zebra.txt); each group
        // alphabetical, case-insensitive. No `..` at the root of an image pane.
        assert_eq!(
            row_names(&l),
            vec!["alpha", "beta", "alpha.txt", "zebra.txt"]
        );
    }

    #[test]
    fn resort_by_size_keeps_folders_first() {
        let mut l = listing(
            vec![file("big.bin", 9000), dir("docs"), file("small.bin", 3)],
            false,
        );
        l.resort(SortColumn::Size);
        // Directories still lead; files ascending by size.
        assert_eq!(row_names(&l), vec!["docs", "small.bin", "big.bin"]);
        // Re-clicking the active column reverses direction (files descending),
        // but folders stay grouped first.
        l.resort(SortColumn::Size);
        assert!(l.is_descending());
        assert_eq!(row_names(&l), vec!["docs", "big.bin", "small.bin"]);
    }

    #[test]
    fn resort_switching_column_resets_to_ascending() {
        let mut l = listing(vec![file("a", 1)], false);
        l.resort(SortColumn::Size);
        l.resort(SortColumn::Size); // now descending on Size
        assert!(l.is_descending());
        l.resort(SortColumn::Name); // switch column -> ascending
        assert_eq!(l.sort_column(), SortColumn::Name);
        assert!(!l.is_descending());
    }

    #[test]
    fn parent_row_hidden_at_image_root_shown_for_host() {
        let img = listing(vec![file("a", 1)], false);
        assert!(!img.show_parent());
        assert_eq!(row_names(&img), vec!["a"]);

        let host = listing(vec![file("a", 1)], true);
        assert!(host.show_parent());
        assert_eq!(row_names(&host), vec!["..", "a"]);
    }

    #[test]
    fn plain_click_selects_one() {
        let mut l = listing(vec![file("a", 1), file("b", 2), file("c", 3)], false);
        l.click("b");
        assert_eq!(l.selection(), &["b".to_string()]);
        l.click("c");
        assert_eq!(l.selection(), &["c".to_string()]);
    }

    #[test]
    fn ctrl_click_toggles() {
        let mut l = listing(vec![file("a", 1), file("b", 2), file("c", 3)], false);
        l.ctrl_click("a");
        l.ctrl_click("c");
        assert!(l.is_selected("a"));
        assert!(l.is_selected("c"));
        assert!(!l.is_selected("b"));
        // Toggling a selected row removes it.
        l.ctrl_click("a");
        assert!(!l.is_selected("a"));
        assert!(l.is_selected("c"));
    }

    #[test]
    fn shift_click_selects_range_from_anchor() {
        // Rows sort to: a, b, c, d (all files, alphabetical).
        let mut l = listing(
            vec![file("a", 1), file("c", 3), file("b", 2), file("d", 4)],
            false,
        );
        l.click("b"); // anchor = b
        l.shift_click("d");
        assert_eq!(
            l.selection(),
            &["b".to_string(), "c".to_string(), "d".to_string()]
        );
        // Shift in the other direction from the same anchor.
        l.click("c");
        l.shift_click("a");
        assert_eq!(
            l.selection(),
            &["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }

    #[test]
    fn shift_click_without_anchor_is_single() {
        let mut l = listing(vec![file("a", 1), file("b", 2)], false);
        l.shift_click("b");
        assert_eq!(l.selection(), &["b".to_string()]);
    }

    #[test]
    fn navigation_through_a_real_filesystem() {
        // Build a blank FAT12 floppy on disk, add a subdirectory with a file in
        // it, then drive DirListing's enter/up over the re-opened filesystem.
        use crate::fs::filesystem::{CreateDirectoryOptions, CreateFileOptions};
        use std::fs::OpenOptions;
        use std::io::Cursor;

        let flat = crate::fs::fat::create_blank_fat(737280, Some("NAV")).unwrap();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), &flat).unwrap();

        {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(tmp.path())
                .unwrap();
            let mut efs = crate::fs::open_editable_filesystem(f, 0, 0x01, None).unwrap();
            let root = efs.root().unwrap();
            efs.create_directory(&root, "SUB", &CreateDirectoryOptions::default())
                .unwrap();
            let sub = efs
                .list_directory(&root)
                .unwrap()
                .into_iter()
                .find(|e| e.name == "SUB")
                .unwrap();
            let mut data = Cursor::new(b"hi".to_vec());
            efs.create_file(
                &sub,
                "INNER.TXT",
                &mut data,
                2,
                &CreateFileOptions::default(),
            )
            .unwrap();
            efs.sync_metadata().unwrap();
        }

        let f = std::fs::File::open(tmp.path()).unwrap();
        let mut fs = crate::fs::open_filesystem(f, 0, 0x01, None).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();

        let mut l = DirListing::new();
        l.load_root(fs, root, entries, false);
        assert!(l.at_root());
        assert!(!l.show_parent());
        assert!(row_names(&l).iter().any(|n| n == "SUB"));

        // Enter SUB: parent row appears, INNER.TXT is listed.
        l.enter("SUB").unwrap();
        assert!(!l.at_root());
        assert!(l.show_parent());
        assert_eq!(l.cwd_path(), "/SUB");
        assert!(row_names(&l).iter().any(|n| n == "INNER.TXT"));

        // Entering a non-directory / missing name errors.
        assert!(l.enter("INNER.TXT").is_err());

        // Up returns to root; another Up is a no-op.
        l.up();
        assert!(l.at_root());
        l.up();
        assert!(l.at_root());
    }

    #[test]
    fn type_tag_covers_each_family() {
        assert_eq!(type_tag(&dir("Folder")), "<DIR>");

        let mut hfs = file("Read Me", 10);
        hfs.type_code = Some("TEXT".to_string());
        assert_eq!(type_tag(&hfs), "TEXT");

        let mut fat = file("IO.SYS", 10);
        fat.dos_attributes = Some(0x27); // archive+system+hidden+read-only
        assert_eq!(type_tag(&fat), "AHSR");

        let mut unix = file("script", 10);
        unix.mode = Some(0o100755);
        assert_eq!(type_tag(&unix), "0755");

        assert_eq!(type_tag(&file("photo.JPG", 10)), "JPG");
        assert_eq!(type_tag(&file("noext", 10)), "");
    }
}
