//! What the browse view offers on the open volume and on a row, for every shell that draws one.

use super::{CommanderAction, CommanderController, ExportFormat, Side};
use crate::controller::browser::rows::{DisplayRow, RowKind};
use crate::controller::browser::BrowserPaneAction;

/// The controls the open volume's filesystem takes, beyond browsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BrowseCaps {
    pub hfs: bool,
    pub prodos: bool,
    /// Files carry forks and type / creator codes, so Mac archive workflows apply.
    pub mac: bool,
    /// QNX 2: deleted entries can be listed.
    pub qnx2: bool,
    /// The raw carve view: its scan can cover the whole image.
    pub carve: bool,
    /// Edit Mode is on offer (Pilot/Cedar volumes stay read-only).
    pub edit_toggle: bool,
    /// HFS and Alto BFS packs check from the view; Inspect's Check covers the rest.
    pub check: bool,
    pub journal: bool,
    /// Alto BFS has a flat namespace.
    pub new_folder: bool,
}

/// One entry of a row's right-click menu; a view draws them in order.
#[derive(Debug, Clone)]
pub enum RowMenuEntry {
    Item {
        label: String,
        action: CommanderAction,
        enabled: bool,
    },
    Submenu {
        label: String,
        items: Vec<(String, CommanderAction)>,
    },
}

fn item(label: &str, action: CommanderAction) -> RowMenuEntry {
    RowMenuEntry::Item {
        label: label.to_string(),
        action,
        enabled: true,
    }
}

impl CommanderController {
    /// The single pane's filesystem, as the browse view's controls follow it.
    pub fn browse_caps(&self) -> BrowseCaps {
        let fs = self.pane(Side::Left).volume().fs_type.as_str();
        BrowseCaps {
            hfs: matches!(fs, "HFS" | "HFS+" | "HFSX"),
            prodos: fs == "ProDOS",
            mac: matches!(fs, "HFS" | "HFS+" | "HFSX" | "MFS" | "APFS"),
            qnx2: fs == "QNX 2",
            carve: fs == "Raw carve (no filesystem)",
            edit_toggle: self.edit_supported() && fs != "Pilot/Cedar",
            check: matches!(fs, "HFS" | "Alto BFS"),
            journal: matches!(fs, "HFS+" | "HFSX"),
            new_folder: fs != "Alto BFS",
        }
    }

    /// What a data row's right-click offers; a click also runs `row_context` first.
    pub fn row_menu(&self, side: Side, row: &DisplayRow) -> Vec<RowMenuEntry> {
        use BrowserPaneAction as P;
        use CommanderAction as A;
        let pane = self.pane(side);
        let mut out = Vec::new();
        // A not-yet-applied staged add has no data to copy, export, hash or describe.
        let real = !matches!(row.kind, RowKind::PendingAdd);
        if pane.is_remote_host() && !row.is_dir && !row.is_tree {
            let open = P::OpenRemoteImage(row.name.clone());
            out.push(item("Open Image", A::Pane(side, open)));
        }
        if real {
            out.push(RowMenuEntry::Item {
                label: "Copy to other pane".into(),
                action: A::Copy(side),
                enabled: self.can_copy(side),
            });
            let export = |format| A::Export { from: side, format };
            out.push(item("Export to hard drive...", export(None)));
            out.push(RowMenuEntry::Submenu {
                label: "Export as".into(),
                items: ExportFormat::ALL
                    .iter()
                    .map(|f| (f.label().to_string(), export(Some(*f))))
                    .collect(),
            });
            out.push(item("Calculate checksums...", A::Checksums(side)));
            // File Info works for wrapper-tree rows too, read through the mounted wrapper.
            let info = A::OpenDetail(side, row.name.clone());
            out.push(item("File Info / Details...", info));
        }
        // A wrapper's contents are read-only (copy out, don't edit), and so is a backup.
        if row.is_tree || pane.is_backup() {
            return out;
        }
        // A combined-tree row is named by its node id; the row's own select puts the cursor on it.
        let target = match row.in_folder {
            true => row.node_id.clone(),
            false => row.name.clone(),
        };
        match row.kind {
            RowKind::PendingRename => out.push(item(
                "Cancel rename",
                A::Pane(side, P::CancelRename(target)),
            )),
            RowKind::Normal => {
                let rename = match row.in_folder {
                    true => P::RenameAtCursor,
                    false => P::RequestRename(target),
                };
                out.push(item("Rename...", A::Pane(side, rename)));
            }
            RowKind::Parent | RowKind::PendingDelete | RowKind::PendingAdd => {}
        }
        let label = match row.kind {
            _ if pane.listing.is_host() => "Delete (immediate)",
            RowKind::PendingDelete => "Undelete",
            RowKind::PendingAdd => "Remove from staging",
            _ => "Delete",
        };
        out.push(item(label, A::Pane(side, P::Delete)));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(menu: &[RowMenuEntry]) -> Vec<&str> {
        menu.iter()
            .map(|e| match e {
                RowMenuEntry::Item { label, .. } | RowMenuEntry::Submenu { label, .. } => {
                    label.as_str()
                }
            })
            .collect()
    }

    #[test]
    fn a_row_menu_follows_what_is_staged_on_the_row() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.txt"), b"x").unwrap();
        let mut ctl = CommanderController::headless();
        ctl.left.listing.load_host_root(dir.path().into()).unwrap();
        let rows = ctl.left.display_rows();
        let row = rows.iter().find(|r| r.name == "a.txt").unwrap().clone();
        let menu = ctl.row_menu(Side::Left, &row);
        let names = labels(&menu);
        assert!(names.contains(&"Rename..."), "{names:?}");
        assert!(names.contains(&"Delete (immediate)"), "{names:?}");
        assert!(names.contains(&"Export as"), "{names:?}");
    }

    #[test]
    fn caps_follow_the_filesystem_name_and_a_closed_pane_offers_nothing_special() {
        let ctl = CommanderController::headless();
        let caps = ctl.browse_caps();
        assert!(!caps.hfs && !caps.mac && !caps.edit_toggle && caps.new_folder);
    }
}
