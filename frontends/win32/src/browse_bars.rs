//! The browse view's bars around the listing: the header's tools, the edit bar while Edit Mode
//! is on, and the mark bar while rows are marked. GTK's `browser/bars.rs` is the reference; what
//! they show comes from the controller (`header_tools`, `edit_bar`, `mark_bar`).

use rusty_backup::controller::browser::ids;
use rusty_backup::controller::commander::{
    ArchiveUse, BrowserPaneAction as P, CommanderAction as A, EditBar, ExportFormat, HeaderTools,
    MarkBar, ModeAction, PanelActions, ProdosExportMode, Side,
};
use windows::core::w;
use windows::Win32::Foundation::{HWND, LPARAM, WPARAM};
use windows::Win32::UI::WindowsAndMessaging::{
    SendMessageW, BM_GETCHECK, BM_SETCHECK, BN_CLICKED, CBS_DROPDOWNLIST, CB_ADDSTRING,
    CB_GETCURSEL, CB_SETCURSEL, WINDOW_EX_STYLE, WINDOW_STYLE, WS_TABSTOP, WS_VSCROLL,
};

use crate::layout::{self, Node, Size};
use crate::theme;
use crate::ui;

/// What a click on a bar asks of the view.
pub enum BarCmd {
    Send(Vec<A>),
    /// Pick host files and stage them.
    AddFiles,
    /// Pop the View menu up under its button.
    ViewMenu,
}

fn pane(action: P) -> A {
    A::Pane(Side::Left, action)
}

fn checked(h: HWND) -> bool {
    // SAFETY: a plain button query.
    unsafe { SendMessageW(h, BM_GETCHECK, None, None).0 == 1 }
}

fn set_check(h: HWND, on: bool) {
    if checked(h) != on {
        // SAFETY: a plain button message; it sends no BN_CLICKED.
        unsafe {
            SendMessageW(h, BM_SETCHECK, Some(WPARAM(on as usize)), None);
        }
    }
}

/// A button `w` DIPs wide, or nothing while it is hidden.
fn shown_button(h: HWND, w: i32) -> Option<Node<HWND>> {
    ui::shown(h).then(|| Node::button(h, w))
}

/// The header's second row: Edit Mode, the volume tools, View, and the carve and QNX toggles.
pub struct Tools {
    pub edit: HWND,
    check: HWND,
    journal: HWND,
    chd: HWND,
    tree: HWND,
    pub view: HWND,
    full_scan: HWND,
    deleted: HWND,
    /// "Boot blocks: present / absent" on the volume line, HFS in Edit Mode.
    pub boot: HWND,
}

impl Tools {
    pub fn build(page: HWND) -> Tools {
        Tools {
            edit: ui::button(page, "Edit Mode", ids::EDIT_MODE),
            check: ui::button(page, "Check", ids::CHECK),
            journal: ui::button(page, "Journal", ids::JOURNAL),
            chd: ui::button(page, "CHD Info", ids::CHD_INFO),
            tree: ui::button(page, "Tree", ids::TREE),
            view: ui::button(page, "View...", ids::VIEW_MENU),
            full_scan: ui::checkbox(page, "Full scan", ids::FULL_SCAN),
            deleted: ui::checkbox(page, "Show deleted", ids::SHOW_DELETED),
            boot: ui::label(page, ""),
        }
    }

    pub fn show(&self, t: &HeaderTools) {
        ui::set_visible(self.edit, t.caps.edit_toggle);
        ui::set_enabled(self.edit, t.edit_enabled);
        ui::set_text(self.edit, t.edit_label);
        ui::set_visible(self.check, t.check);
        ui::set_visible(self.journal, t.caps.journal);
        ui::set_visible(self.chd, t.chd);
        ui::set_visible(self.tree, t.loaded);
        ui::set_visible(self.full_scan, t.caps.carve);
        set_check(self.full_scan, t.full_scan);
        ui::set_visible(self.deleted, t.caps.qnx2);
        set_check(self.deleted, t.show_deleted);
        let (text, tone) = match t.boot {
            Some(true) => ("Boot blocks: present", None),
            Some(false) => (
                "Boot blocks: absent",
                Some(rusty_backup::controller::commander::Tone::Warning),
            ),
            None => ("", None),
        };
        ui::set_text(self.boot, text);
        theme::set_tone(self.boot, tone);
        ui::set_visible(self.boot, t.boot.is_some());
    }

    /// The tools before the row's stretch (Edit Mode and the volume tools) and after it (View).
    pub fn nodes(&self) -> (Vec<Node<HWND>>, Vec<Node<HWND>>) {
        let before = [
            shown_button(self.check, 70),
            shown_button(self.journal, 80),
            shown_button(self.chd, 80),
            shown_button(self.tree, 60),
        ];
        let mut after: Vec<Node<HWND>> = Vec::new();
        for h in [self.full_scan, self.deleted] {
            if ui::shown(h) {
                after.push(Node::leaf(h, Size::Fixed(110), Size::Fixed(layout::ROW)));
            }
        }
        after.push(Node::button(self.view, 70));
        (before.into_iter().flatten().collect(), after)
    }

    pub fn all(&self) -> [HWND; 9] {
        [
            self.edit,
            self.check,
            self.journal,
            self.chd,
            self.tree,
            self.view,
            self.full_scan,
            self.deleted,
            self.boot,
        ]
    }

    pub fn command(&self, from: HWND, code: u32) -> Option<BarCmd> {
        if code != BN_CLICKED {
            return None;
        }
        let a = if from == self.edit {
            A::Mode(ModeAction::ToggleEditMode)
        } else if from == self.check {
            pane(P::Fsck)
        } else if from == self.journal {
            pane(P::ShowJournal)
        } else if from == self.chd {
            A::Mode(ModeAction::ChdInfo)
        } else if from == self.tree {
            pane(P::ListTree { ids: false })
        } else if from == self.view {
            return Some(BarCmd::ViewMenu);
        } else if from == self.full_scan {
            A::Mode(ModeAction::CarveFullScan(checked(from)))
        } else if from == self.deleted {
            A::Mode(ModeAction::ShowDeleted(checked(from)))
        } else {
            return None;
        };
        Some(BarCmd::Send(vec![a]))
    }
}

/// The edit bar: shown while Edit Mode is on.
pub struct EditRow {
    note: HWND,
    target: HWND,
    add: HWND,
    import: HWND,
    new_folder: HWND,
    rename: HWND,
    delete: HWND,
    bless: HWND,
    boot: HWND,
    lock: HWND,
    unlock: HWND,
    apply: HWND,
    discard: HWND,
    edits: HWND,
    free: HWND,
    after: HWND,
    edits_shown: std::cell::Cell<bool>,
}

impl EditRow {
    pub fn build(page: HWND) -> EditRow {
        EditRow {
            note: ui::label(page, ""),
            target: ui::cell_label(page, ""),
            add: ui::button(page, "Add File...", ids::ADD_FILE),
            import: ui::button(page, "Import Archive...", ids::IMPORT_ARCHIVE),
            new_folder: ui::button(page, "New Folder...", ids::NEW_FOLDER),
            rename: ui::button(page, "Rename...", ids::RENAME),
            delete: ui::button(page, "Delete", ids::DELETE),
            bless: ui::button(page, "Bless Folder", ids::BLESS),
            boot: ui::button(page, "Boot Blocks...", ids::BOOT_BLOCKS),
            lock: ui::button(page, "Lock", ids::PRODOS_LOCK),
            unlock: ui::button(page, "Unlock", ids::PRODOS_UNLOCK),
            apply: ui::button(page, "Apply Edits", ids::APPLY),
            discard: ui::button(page, "Discard", ids::DISCARD),
            edits: ui::button(page, "Edits", ids::EDITS),
            free: ui::cell_label(page, ""),
            after: ui::cell_label(page, ""),
            edits_shown: std::cell::Cell::new(false),
        }
    }

    pub fn all(&self) -> [HWND; 16] {
        [
            self.note,
            self.target,
            self.add,
            self.import,
            self.new_folder,
            self.rename,
            self.delete,
            self.bless,
            self.boot,
            self.lock,
            self.unlock,
            self.apply,
            self.discard,
            self.edits,
            self.free,
            self.after,
        ]
    }

    pub fn show(&self, e: &EditBar) {
        for h in self.all() {
            ui::set_visible(h, e.on);
        }
        if !e.on {
            return;
        }
        ui::set_text(self.note, e.copy_note.unwrap_or(""));
        ui::set_visible(self.note, e.copy_note.is_some());
        theme::set_tone(
            self.note,
            Some(rusty_backup::controller::commander::Tone::Warning),
        );
        ui::set_text(self.target, &e.target);
        ui::set_visible(self.new_folder, e.caps.new_folder);
        ui::set_enabled(self.rename, e.deletable);
        ui::set_enabled(self.delete, e.deletable);
        ui::set_visible(self.bless, e.caps.hfs);
        ui::set_enabled(self.bless, e.folder);
        ui::set_visible(self.boot, e.caps.hfs);
        ui::set_visible(self.lock, e.caps.prodos);
        ui::set_visible(self.unlock, e.caps.prodos);
        ui::set_enabled(self.lock, e.deletable);
        ui::set_enabled(self.unlock, e.deletable);
        let any = e.staged > 0;
        for h in [self.apply, self.discard, self.edits] {
            ui::set_visible(h, any);
        }
        ui::set_text(self.apply, &e.apply_label);
        ui::set_text(self.edits, &e.edits_label);
        ui::set_enabled(self.apply, e.apply_enabled);
        ui::set_enabled(self.discard, e.discard_enabled);
        self.edits_shown.set(e.edits_shown);
        ui::set_text(self.free, &e.free);
        let (after, tone) = e.after.clone().unzip();
        ui::set_text(self.after, after.as_deref().unwrap_or(""));
        theme::set_tone(self.after, tone);
    }

    /// The bar's rows: the copy note, the tools, then Apply and the space readout.
    pub fn nodes(&self) -> Vec<Node<HWND>> {
        if !ui::shown(self.target) {
            return Vec::new();
        }
        let mut rows = Vec::new();
        if ui::shown(self.note) {
            rows.push(Node::leaf(
                self.note,
                Size::Stretch(1),
                Size::Fixed(layout::ROW - 8),
            ));
        }
        let tools = [
            (self.add, 85),
            (self.import, 115),
            (self.new_folder, 100),
            (self.rename, 80),
            (self.delete, 65),
            (self.bless, 95),
            (self.boot, 105),
            (self.lock, 55),
            (self.unlock, 65),
        ];
        let mut first = vec![Node::leaf(
            self.target,
            Size::Fixed(150),
            Size::Fixed(layout::ROW),
        )];
        first.extend(tools.iter().filter_map(|(h, w)| shown_button(*h, *w)));
        first.push(Node::Space(Size::Stretch(1)));
        rows.push(Node::row(first));
        let mut second: Vec<Node<HWND>> = [(self.apply, 130), (self.discard, 75), (self.edits, 90)]
            .iter()
            .filter_map(|(h, w)| shown_button(*h, *w))
            .collect();
        second.push(Node::leaf(
            self.free,
            Size::Fixed(140),
            Size::Fixed(layout::ROW),
        ));
        second.push(Node::leaf(
            self.after,
            Size::Fixed(140),
            Size::Fixed(layout::ROW),
        ));
        second.push(Node::Space(Size::Stretch(1)));
        rows.push(Node::row(second));
        rows
    }

    pub fn command(&self, from: HWND, code: u32) -> Option<BarCmd> {
        if code != BN_CLICKED {
            return None;
        }
        let a = if from == self.add {
            return Some(BarCmd::AddFiles);
        } else if from == self.import {
            pane(P::ImportTar)
        } else if from == self.new_folder {
            pane(P::RequestNewFolder)
        } else if from == self.rename {
            pane(P::RenameAtCursor)
        } else if from == self.delete {
            pane(P::Delete)
        } else if from == self.bless {
            pane(P::Bless)
        } else if from == self.boot {
            pane(P::BootBlocks)
        } else if from == self.lock {
            pane(P::SetProdosLock(true))
        } else if from == self.unlock {
            pane(P::SetProdosLock(false))
        } else if from == self.apply {
            A::Mode(ModeAction::Apply)
        } else if from == self.discard {
            pane(P::Discard)
        } else if from == self.edits {
            pane(P::ShowEdits(!self.edits_shown.get()))
        } else {
            return None;
        };
        Some(BarCmd::Send(vec![a]))
    }
}

/// The mark bar beside the status line: the count and Clear; the header's export bar exports.
pub struct MarkRow {
    count: HWND,
    clear: HWND,
}

impl MarkRow {
    pub fn build(page: HWND) -> MarkRow {
        MarkRow {
            count: ui::cell_label(page, ""),
            clear: ui::button(page, "Clear", ids::CLEAR_MARKS),
        }
    }

    pub fn all(&self) -> [HWND; 2] {
        [self.count, self.clear]
    }

    pub fn show(&self, m: &MarkBar) {
        for h in self.all() {
            ui::set_visible(h, m.count > 0);
        }
        ui::set_text(self.count, &m.label);
    }

    pub fn nodes(&self) -> Vec<Node<HWND>> {
        if !ui::shown(self.count) {
            return Vec::new();
        }
        vec![
            Node::leaf(self.count, Size::Fixed(90), Size::Fixed(layout::ROW)),
            Node::button(self.clear, 65),
        ]
    }

    pub fn command(&self, from: HWND, code: u32) -> Option<BarCmd> {
        if code == BN_CLICKED && from == self.clear {
            return Some(BarCmd::Send(vec![pane(P::ClearSelection)]));
        }
        None
    }
}

/// What a side-panel tool asks of the view for the entry under the cursor.
pub enum PanelCmd {
    /// Actions on the cursor's path.
    OnPath(fn(String) -> Vec<A>),
    /// Offer the Mac archive formats under `button`; the one picked exports the cursor's entry.
    MacMenu(HWND),
}

/// The Mac archive formats the side panel exports to, with their menu labels.
pub const MAC_FORMATS: [(ExportFormat, &str); 4] = [
    (ExportFormat::BinHex, "BinHex (.hqx)"),
    (ExportFormat::StuffIt, "StuffIt (.sit)"),
    (ExportFormat::StuffItHqx, "StuffIt over BinHex (.sit.hqx)"),
    (ExportFormat::MacArchive, "MAR (.mar)"),
];

/// The side panel's tools beyond Extract and Export: the ProDOS and QNX extract options, the
/// Mac archive buttons, Edit as text and Edit Metadata.
pub struct PanelTools {
    prodos_caption: HWND,
    prodos: HWND,
    rs_to_lf: HWND,
    save_archive: HWND,
    browse_archive: HWND,
    edit_text: HWND,
    metadata: HWND,
    mac: HWND,
}

impl PanelTools {
    pub fn build(page: HWND) -> PanelTools {
        let style = WINDOW_STYLE((CBS_DROPDOWNLIST as u32) | WS_TABSTOP.0 | WS_VSCROLL.0);
        let prodos = ui::child(page, w!("COMBOBOX"), "", style, WINDOW_EX_STYLE(0), "");
        for m in ProdosExportMode::ALL {
            let t = ui::wide(m.label());
            // SAFETY: the combo copies the string.
            unsafe {
                SendMessageW(
                    prodos,
                    CB_ADDSTRING,
                    None,
                    Some(LPARAM(t.as_ptr() as isize)),
                );
            }
        }
        // SAFETY: a plain combo message.
        unsafe {
            SendMessageW(prodos, CB_SETCURSEL, Some(WPARAM(0)), None);
        }
        PanelTools {
            prodos_caption: ui::cell_label(page, "Export name:"),
            prodos,
            rs_to_lf: ui::checkbox(page, "Text: RS to LF", ""),
            save_archive: ui::button(page, "Save as Mac archive...", ids::SAVE_ARCHIVE),
            browse_archive: ui::button(page, "Browse archive...", ids::BROWSE_ARCHIVE),
            edit_text: ui::button(page, "Edit as text...", ids::EDIT_TEXT),
            metadata: ui::button(page, "Edit Metadata...", ids::EDIT_METADATA),
            mac: ui::button(page, "Export as Mac archive...", "browse.export_mac"),
        }
    }

    pub fn all(&self) -> [HWND; 8] {
        [
            self.prodos_caption,
            self.prodos,
            self.rs_to_lf,
            self.save_archive,
            self.browse_archive,
            self.edit_text,
            self.metadata,
            self.mac,
        ]
    }

    pub fn show(&self, a: &PanelActions) {
        let on = a.extractable;
        ui::set_visible(self.prodos_caption, on && a.prodos);
        ui::set_visible(self.prodos, on && a.prodos);
        ui::set_visible(self.rs_to_lf, on && a.qnx);
        ui::set_visible(self.save_archive, on && a.archive);
        ui::set_visible(self.browse_archive, on && a.archive);
        ui::set_visible(self.edit_text, on && a.text);
        ui::set_enabled(self.edit_text, a.edit_mode);
        ui::set_visible(self.metadata, on && a.metadata);
        ui::set_visible(self.mac, on && a.mac);
    }

    /// The ProDOS name suffix and QNX line endings the extract options take.
    pub fn options(&self) -> (bool, bool) {
        // SAFETY: a plain combo query.
        let i = unsafe { SendMessageW(self.prodos, CB_GETCURSEL, None, None).0 };
        let mode = usize::try_from(i)
            .ok()
            .and_then(|i| ProdosExportMode::ALL.get(i).copied());
        (
            mode != Some(ProdosExportMode::Plain),
            checked(self.rs_to_lf),
        )
    }

    /// The panel's rows below the fork mode, each `width` DIPs wide.
    pub fn nodes(&self, width: i32) -> Vec<Node<HWND>> {
        let half = (width - layout::GAP) / 2;
        let mut rows = Vec::new();
        if ui::shown(self.prodos) {
            rows.push(Node::row(vec![
                Node::leaf(
                    self.prodos_caption,
                    Size::Fixed(110),
                    Size::Fixed(layout::ROW),
                ),
                Node::leaf(
                    self.prodos,
                    Size::Fixed(width - 110 - layout::GAP),
                    Size::Fixed(layout::ROW),
                ),
            ]));
        }
        if ui::shown(self.rs_to_lf) {
            rows.push(Node::leaf(
                self.rs_to_lf,
                Size::Fixed(width),
                Size::Fixed(layout::ROW),
            ));
        }
        for pair in [
            [self.save_archive, self.browse_archive],
            [self.edit_text, self.metadata],
        ] {
            let row: Vec<Node<HWND>> = pair.iter().filter_map(|h| shown_button(*h, half)).collect();
            if !row.is_empty() {
                rows.push(Node::row(row));
            }
        }
        if let Some(b) = shown_button(self.mac, half) {
            rows.push(b);
        }
        rows
    }

    pub fn command(&self, from: HWND, code: u32) -> Option<PanelCmd> {
        if code != BN_CLICKED {
            return None;
        }
        Some(if from == self.save_archive {
            PanelCmd::OnPath(|path| {
                let purpose = ArchiveUse::Extract;
                vec![pane(P::ReadArchive { path, purpose })]
            })
        } else if from == self.browse_archive {
            PanelCmd::OnPath(|path| {
                let purpose = ArchiveUse::Browse;
                vec![pane(P::ReadArchive { path, purpose })]
            })
        } else if from == self.edit_text {
            PanelCmd::OnPath(|_| vec![pane(P::EditText)])
        } else if from == self.metadata {
            PanelCmd::OnPath(|_| vec![pane(P::EditMetadata)])
        } else if from == self.mac {
            PanelCmd::MacMenu(from)
        } else {
            return None;
        })
    }
}
