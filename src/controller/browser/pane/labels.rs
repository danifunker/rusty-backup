//! What a row means to a view: its label, tone and hover, and what clicks on it do.

use disk_core::display::format_size;
use disk_core::entry::{EntryType, FileEntry};
use fs_apple2::prodos_types::{is_known_type, type_description};

use super::super::metadata::resolved_prodos_type;
use super::super::profile::{DetailPlacement, ListingStyle};
use super::super::rows::{DisplayRow, RowKind};
use super::{BrowserPaneAction, BrowserPaneController};
use crate::fs::entry::FileEntryTypeDisplay as _;

/// What a row's staged state makes it; the view picks the colour.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowTone {
    Normal,
    /// A staged delete: dimmed and struck through.
    Deleted,
    /// A staged rename or metadata edit.
    Changed,
    /// A staged add.
    Added,
    /// A staged add whose type no dictionary or override resolved.
    Unresolved,
}

/// A row's text, tone and hover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowLabel {
    pub text: String,
    pub tone: RowTone,
    pub hover: Option<String>,
}

impl RowLabel {
    fn plain(text: String) -> Self {
        RowLabel {
            text,
            tone: RowTone::Normal,
            hover: None,
        }
    }
}

impl BrowserPaneController {
    /// What a source picker's closed state says this pane has open.
    pub fn source_label(&self) -> String {
        if let Some(label) = self.remote_label() {
            return label;
        }
        if self.listing.is_host() {
            return "Local folder".to_string();
        }
        let name = self.source_path().map(|p| {
            let base = p.file_name().unwrap_or_default();
            base.to_string_lossy().into_owned()
        });
        match (name, self.is_backup(), self.is_archive()) {
            (Some(n), true, _) => format!("Backup: {n}"),
            (None, true, _) => "Backup folder".to_string(),
            (Some(n), false, true) => format!("Archive: {n}"),
            (Some(n), false, false) => format!("Image: {n}"),
            (None, false, _) => "Open image, backup, or folder...".to_string(),
        }
    }

    /// The tag before the path: where the path is rooted.
    pub fn path_prefix(&self) -> String {
        if self.listing.is_host() {
            return "host ".to_string();
        }
        match self.selected_partition() {
            Some(i) => format!("@{} ", i + 1),
            None => "- ".to_string(),
        }
    }

    /// The volume readout's name: "local folder", or the label and filesystem.
    pub fn volume_title(&self) -> String {
        if self.listing.is_host() {
            return "local folder".to_string();
        }
        let v = self.volume();
        match v.label.is_empty() {
            true => v.fs_type.clone(),
            false => format!("{} ({})", v.label, v.fs_type),
        }
    }

    /// The partition chooser's text when there is nothing to choose.
    pub fn no_partitions_label(&self) -> &'static str {
        match self.is_optical() {
            true => "(no filesystems)",
            false => "(no partitions)",
        }
    }

    /// The browse view's combined tree marks rows with checkboxes, so a plain click only moves the cursor.
    pub fn marks_with_boxes(&self) -> bool {
        self.listing_style() == ListingStyle::CombinedTree
            && self.profile().detail == DetailPlacement::SidePanel
    }

    /// What a click on `row` means here.
    pub fn row_click(
        &self,
        row: &DisplayRow,
        ctrl: bool,
        shift: bool,
    ) -> Option<BrowserPaneAction> {
        let node_id = row.node_id.clone();
        if self.marks_with_boxes() && !ctrl && !shift {
            Some(BrowserPaneAction::CursorTo(node_id))
        } else if row.depth > 0 {
            Some(BrowserPaneAction::TreeClick {
                node_id,
                ctrl,
                shift,
            })
        } else if row.is_parent() {
            None
        } else {
            let name = row.name.clone();
            Some(BrowserPaneAction::Click { name, ctrl, shift })
        }
    }

    /// What a right-click on `row` selects before its menu opens.
    pub fn row_context(&self, row: &DisplayRow) -> Option<BrowserPaneAction> {
        if row.depth > 0 {
            Some(BrowserPaneAction::TreeContextSelect(row.node_id.clone()))
        } else if row.is_parent() {
            None
        } else {
            Some(BrowserPaneAction::ContextSelect(row.name.clone()))
        }
    }

    /// True when the view should ask `SniffArchive` for `entry` now (a file on a Mac volume, not yet known).
    pub fn wants_sniff(&self, entry: &FileEntry) -> bool {
        let mac = matches!(
            self.volume().fs_type.as_str(),
            "HFS" | "HFS+" | "HFSX" | "MFS" | "APFS"
        );
        mac && entry.is_file() && !self.is_sniffing() && self.archive_sniff(&entry.path).is_none()
    }

    fn fs_is_hfs(&self) -> bool {
        matches!(self.volume().fs_type.as_str(), "HFS" | "HFS+" | "HFSX")
    }

    fn fs_is_prodos(&self) -> bool {
        self.volume().fs_type == "ProDOS"
    }

    /// The row's label; `with_size` adds the size for a view with no size column.
    pub fn row_label(&self, row: &DisplayRow, with_size: bool) -> RowLabel {
        let entry = self.row_entry(row);
        if row.kind == RowKind::PendingAdd {
            return match &entry {
                Some(e) => self.pending_add_label(e, with_size),
                None => RowLabel::plain(format!("+ {}", row.name)),
            };
        }
        let Some(entry) = entry else {
            return RowLabel::plain(row.name.clone());
        };
        let (mut text, hover) = self.entry_text(&entry, with_size);
        if row.is_wrapper {
            text = format!("{text}  {}", row.type_tag);
        }
        if let Some(to) = &row.rename_to {
            text = format!("{text} -> {to}");
        }
        let tone = match row.kind {
            RowKind::PendingDelete => RowTone::Deleted,
            _ if row.meta_changed || row.rename_to.is_some() => RowTone::Changed,
            _ => RowTone::Normal,
        };
        RowLabel { text, tone, hover }
    }

    fn entry_text(&self, entry: &FileEntry, with_size: bool) -> (String, Option<String>) {
        let sized = |text: String, size: &str| match with_size {
            true => format!("{text}  ({size})"),
            false => text,
        };
        match entry.entry_type {
            EntryType::Directory => {
                let blessed = self.volume().blessed.as_ref();
                match blessed.is_some_and(|(cnid, _)| *cnid == entry.location) {
                    true => (format!("{} [System]", entry.name), None),
                    false => (entry.name.clone(), None),
                }
            }
            EntryType::File => {
                let badge = entry.type_code_display().filter(|_| self.fs_is_prodos());
                let hover = badge.as_ref().map(|tc| {
                    let desc = type_description(entry.prodos_file_type.unwrap_or(0));
                    let aux = entry.aux_type.unwrap_or(0);
                    match desc.is_empty() {
                        true => format!("{tc} ${aux:04X}"),
                        false => format!("{desc} ${aux:04X}"),
                    }
                });
                let name = match badge {
                    Some(b) => format!("{}  [{b}]", entry.name),
                    None => entry.name.clone(),
                };
                (sized(name, &entry.size_string()), hover)
            }
            EntryType::Symlink => {
                let target = entry.symlink_target.as_deref().unwrap_or("?");
                (format!("{} -> {target}", entry.name), None)
            }
            EntryType::Special => {
                let stype = entry.special_type.as_deref().unwrap_or("special");
                (sized(entry.name.clone(), stype), None)
            }
        }
    }

    /// A staged add: green, blue when its type is unresolved, with its resource-fork badge.
    fn pending_add_label(&self, entry: &FileEntry, with_size: bool) -> RowLabel {
        let queue = &self.queue;
        let unresolved = if self.fs_is_hfs() && entry.is_file() {
            queue.resolved_hfs_type_creator(entry) == ([0; 4], [0; 4])
        } else if self.fs_is_prodos() && entry.is_file() {
            !is_known_type(resolved_prodos_type(queue, entry).0)
        } else {
            false
        };
        let badge = queue.pending_resource_fork_for(&entry.path).map(|imp| {
            let code = |c: [u8; 4]| String::from_utf8_lossy(&c).to_string();
            let codes = match (imp.type_code.map(code), imp.creator_code.map(code)) {
                (Some(t), Some(c)) => format!("{t}/{c} "),
                (Some(t), None) => format!("{t} "),
                (None, Some(c)) => format!("?/{c} "),
                (None, None) => String::new(),
            };
            format!("[{}+rsrc {}]", codes, format_size(imp.data.len() as u64))
        });
        let size = entry.size_string();
        let text = match (entry.is_directory(), badge, with_size) {
            (true, ..) => format!("+ {}", entry.name),
            (false, Some(b), true) => format!("+ {}  {b}  ({size})", entry.name),
            (false, Some(b), false) => format!("+ {}  {b}", entry.name),
            (false, None, true) => format!("+ {}  ({size})", entry.name),
            (false, None, false) => format!("+ {}", entry.name),
        };
        let tone = match unresolved {
            true => RowTone::Unresolved,
            false => RowTone::Added,
        };
        RowLabel {
            text,
            tone,
            hover: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use super::*;
    use crate::model::edit_queue::StagedEdit;

    fn pane(
        style: ListingStyle,
        profile: PaneProfile,
    ) -> (tempfile::TempDir, BrowserPaneController) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("gone.txt"), b"xyz").unwrap();
        std::fs::write(dir.path().join("kept.txt"), b"x").unwrap();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        let mut vm = BrowserPaneController::with_profile(PaneServices::headless(), profile);
        vm.listing.load_host_root(dir.path().to_path_buf()).unwrap();
        vm.apply(BrowserPaneAction::SetListingStyle(style));
        (dir, vm)
    }

    fn row(vm: &mut BrowserPaneController, name: &str) -> DisplayRow {
        let rows = vm.display_rows();
        rows.into_iter().find(|r| r.name == name).unwrap()
    }

    #[test]
    fn a_label_says_what_is_staged_on_the_row() {
        let (_d, mut vm) = pane(ListingStyle::FlatList, PaneProfile::commander());
        let gone = vm.listing.entries().iter().find(|e| e.name == "gone.txt");
        let (entry, root) = (gone.unwrap().clone(), vm.listing.cwd().unwrap().clone());
        vm.queue.push(StagedEdit::DeleteEntry {
            parent: root.clone(),
            entry,
        });
        vm.queue.push(StagedEdit::CreateDirectory {
            parent: root,
            name: "new".into(),
            qnx2_attrs: None,
        });
        let kept = row(&mut vm, "kept.txt");
        let label = vm.row_label(&kept, true);
        assert_eq!(
            (label.text.as_str(), label.tone),
            ("kept.txt  (1 B)", RowTone::Normal)
        );
        assert_eq!(vm.row_label(&kept, false).text, "kept.txt");
        let gone = row(&mut vm, "gone.txt");
        assert_eq!(vm.row_label(&gone, false).tone, RowTone::Deleted);
        let added = row(&mut vm, "new");
        let label = vm.row_label(&added, false);
        assert_eq!((label.text.as_str(), label.tone), ("+ new", RowTone::Added));
    }

    #[test]
    fn a_plain_click_only_moves_the_cursor_where_marks_are_checkboxes() {
        let (_d, mut vm) = pane(ListingStyle::CombinedTree, PaneProfile::inspect());
        let kept = row(&mut vm, "kept.txt");
        assert!(vm.marks_with_boxes());
        assert_eq!(
            vm.row_click(&kept, false, false),
            Some(BrowserPaneAction::CursorTo(kept.node_id.clone()))
        );
        let click = BrowserPaneAction::Click {
            name: "kept.txt".into(),
            ctrl: true,
            shift: false,
        };
        assert_eq!(vm.row_click(&kept, true, false), Some(click));

        let (_d, mut flat) = pane(ListingStyle::CombinedTree, PaneProfile::commander());
        let kept = row(&mut flat, "kept.txt");
        assert!(!flat.marks_with_boxes(), "Commander selects with a click");
        let click = flat.row_click(&kept, false, false);
        assert!(
            matches!(click, Some(BrowserPaneAction::Click { .. })),
            "{click:?}"
        );
        let up = row(&mut flat, "..");
        assert_eq!(flat.row_click(&up, false, false), None);
        assert_eq!(flat.row_context(&up), None);
    }
}
