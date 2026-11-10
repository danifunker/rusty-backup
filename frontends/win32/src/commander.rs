//! Commander Mode: two browser panes that take over the window, on one `CommanderController`.
//!
//! GTK's `commander/` is the reference. Each side is a [`SideView`]: its source, volume and path
//! rows (`side_bars`) over the same [`PaneView`] Inspect's browse view uses; between them is the
//! copy column (`copy_column`). Every window comes from the controller (`side_windows` for each
//! side, `screen_windows`) through [`ModelWindow`]. A row dragged onto the other pane copies, as
//! GTK's own pointer tracking does: a captured drag, not OLE, so it needs nothing else running.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use rusty_backup::controller::app::{CommanderRecent, Notify, Snapshot, UserConfig};
use rusty_backup::controller::commander::{
    ids, BrowserPaneAction as P, CommanderAction as A, CommanderController, CopyColumn,
    ExportFormat, JobRegistry, PaneServices, PaneWindow, ResourceForkMode, Side, SideBars,
    FOLLOW_DELAY,
};
use rusty_backup::controller::keys::{Command, Key, KeyMap, Mods};
use rusty_backup::controller::{Controller, Effect};
use windows::core::w;
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, POINT, RECT, WPARAM};
use windows::Win32::Graphics::Gdi::PtInRect;
use windows::Win32::UI::Controls::{BCN_DROPDOWN, BS_SPLITBUTTON, NMHDR};
use windows::Win32::UI::Input::KeyboardAndMouse::{ReleaseCapture, SetCapture, VK_ESCAPE};
use windows::Win32::UI::WindowsAndMessaging::{
    DispatchMessageW, GetAncestor, GetCursorPos, GetMessageW, GetWindowRect, KillTimer,
    LoadCursorW, PostMessageW, SendMessageW, SetCursor, SetTimer, TranslateMessage, BM_GETCHECK,
    BM_SETCHECK, BN_CLICKED, CBN_SELCHANGE, CBS_DROPDOWNLIST, CB_ADDSTRING, CB_GETCURSEL,
    CB_RESETCONTENT, CB_SETCURSEL, ES_AUTOHSCROLL, ES_PASSWORD, GA_ROOT, IDC_ARROW, IDC_NO, MSG,
    WINDOW_EX_STYLE, WINDOW_STYLE, WM_CAPTURECHANGED, WM_COMMAND, WM_KEYDOWN, WM_LBUTTONUP,
    WM_MOUSEMOVE, WS_EX_CLIENTEDGE, WS_TABSTOP, WS_VSCROLL,
};

use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::layout::{self, Node, Size};
use crate::menu::{track, MenuItem};
use crate::pane_view::{key_of, mods, PaneEvent, PaneShown, PaneView, DEFERRED};
use crate::panel::{self, Handler};
use crate::sync::SyncGuard;
use crate::ui;
use crate::window::{Event, ModelWindow};

const SIDES: [Side; 2] = [Side::Left, Side::Right];

/// The copy column's width, in DIPs.
const COLUMN_W: i32 = 150;

/// A `WM_COMMAND` code the screen posts to itself: pop up the View menu the key map asked for.
const VIEW_MENU: u32 = 0x7F01;

fn idx(side: Side) -> usize {
    match side {
        Side::Left => 0,
        Side::Right => 1,
    }
}

fn new_controller(jobs: &JobRegistry, notify: &Arc<dyn Notify>) -> CommanderController {
    let services = || PaneServices {
        jobs: jobs.clone(),
        notify: Arc::clone(notify),
        recent: Box::new(CommanderRecent(UserConfig)),
    };
    let (keymap, problems) = KeyMap::load(KeyMap::user_file().as_deref());
    let mut ctl = CommanderController::new(services(), services(), keymap);
    ctl.report_keymap_problems(problems);
    ctl
}

fn combo(parent: HWND, id: &str, items: &[&str]) -> HWND {
    let style = WINDOW_STYLE((CBS_DROPDOWNLIST as u32) | WS_TABSTOP.0 | WS_VSCROLL.0);
    let h = ui::child(parent, w!("COMBOBOX"), "", style, WINDOW_EX_STYLE(0), id);
    set_items(h, items);
    h
}

fn set_items(h: HWND, items: &[&str]) {
    // SAFETY: plain combo messages; the control copies each string.
    unsafe {
        SendMessageW(h, CB_RESETCONTENT, None, None);
        for item in items {
            let t = ui::wide(&ui::plain(item));
            SendMessageW(h, CB_ADDSTRING, None, Some(LPARAM(t.as_ptr() as isize)));
        }
    }
}

fn select(h: HWND, i: usize) {
    // SAFETY: plain combo messages; CB_SETCURSEL sends no CBN_SELCHANGE.
    unsafe {
        if SendMessageW(h, CB_GETCURSEL, None, None).0 != i as isize {
            SendMessageW(h, CB_SETCURSEL, Some(WPARAM(i)), None);
        }
    }
}

fn picked(h: HWND) -> Option<usize> {
    // SAFETY: a plain combo query.
    usize::try_from(unsafe { SendMessageW(h, CB_GETCURSEL, None, None).0 }).ok()
}

fn checked(h: HWND) -> bool {
    // SAFETY: a plain button query.
    unsafe { SendMessageW(h, BM_GETCHECK, None, None).0 == 1 }
}

fn rect_of(h: HWND) -> RECT {
    let mut r = RECT::default();
    // SAFETY: a plain query of our own window.
    let _ = unsafe { GetWindowRect(h, &mut r) };
    r
}

/// One side: its bars, its job line, the unlock row and the listing (or the other side's quick view).
struct SideView {
    side: Side,
    source: HWND,
    open_file: HWND,
    open_folder: HWND,
    open_backup: HWND,
    connect: HWND,
    close: HWND,
    close_image: HWND,
    partition: HWND,
    no_parts: HWND,
    title: HWND,
    free: HWND,
    read_only: HWND,
    new_folder: HWND,
    apply: HWND,
    discard: HWND,
    edits: HWND,
    prefix: HWND,
    crumbs: RefCell<Vec<(HWND, Option<String>)>>,
    job: HWND,
    progress: HWND,
    cancel_open: HWND,
    unlock_note: HWND,
    password: HWND,
    unlock: HWND,
    quick_hint: HWND,
    quick: HWND,
    pane: PaneView,
    bars: RefCell<Option<SideBars>>,
    edits_open: Cell<bool>,
}

/// What one side shows this refresh.
struct SideShown {
    bars: SideBars,
    job: Option<(Snapshot, bool)>,
    pane: PaneShown,
}

impl SideView {
    fn build(page: HWND, side: Side) -> SideView {
        let names = match side {
            Side::Left => ids::LEFT,
            Side::Right => ids::RIGHT,
        };
        let split = WINDOW_STYLE(BS_SPLITBUTTON as u32 | WS_TABSTOP.0);
        let source = ui::child(
            page,
            w!("BUTTON"),
            "",
            split,
            WINDOW_EX_STYLE(0),
            names.source,
        );
        let secret = WINDOW_STYLE(ES_AUTOHSCROLL as u32 | ES_PASSWORD as u32 | WS_TABSTOP.0);
        let quick_style = WINDOW_STYLE(
            windows::Win32::UI::WindowsAndMessaging::ES_MULTILINE as u32
                | windows::Win32::UI::WindowsAndMessaging::ES_READONLY as u32
                | WS_VSCROLL.0,
        );
        let quick = ui::child(page, w!("EDIT"), "", quick_style, WS_EX_CLIENTEDGE, "");
        // SAFETY: a plain query of our own page.
        let dpi = unsafe { windows::Win32::UI::HiDpi::GetDpiForWindow(page) }.max(96);
        ui::set_font(quick, ui::mono_font_at(dpi));
        let view = SideView {
            side,
            source,
            open_file: ui::button(page, "File...", names.open_file),
            open_folder: ui::button(page, "Folder...", names.open_folder),
            open_backup: ui::button(page, "Backup...", names.open_backup),
            connect: ui::button(page, "Remote...", names.connect),
            close: ui::button(page, "Close", names.close),
            close_image: ui::button(page, "Close image", names.close_image),
            partition: combo(page, names.partition, &[]),
            no_parts: ui::cell_label(page, ""),
            title: ui::cell_label(page, ""),
            free: ui::cell_label(page, ""),
            read_only: ui::cell_label(page, "Read-only"),
            new_folder: ui::button(page, "New Folder...", names.new_folder),
            apply: ui::button(page, "Apply", names.apply),
            discard: ui::button(page, "Discard", names.discard),
            edits: ui::button(page, "Edits", names.edits),
            prefix: ui::cell_label(page, ""),
            crumbs: RefCell::default(),
            job: ui::cell_label(page, ""),
            progress: ui::child(
                page,
                w!("msctls_progress32"),
                "",
                WINDOW_STYLE(0),
                WINDOW_EX_STYLE(0),
                "",
            ),
            cancel_open: ui::button(page, "Cancel", names.cancel_open),
            unlock_note: ui::label(page, ""),
            password: ui::child(page, w!("EDIT"), "", secret, WS_EX_CLIENTEDGE, ""),
            unlock: ui::button(page, "Unlock", names.unlock),
            quick_hint: ui::label(page, ""),
            quick,
            pane: PaneView::build(page, side, names.list, "", true),
            bars: RefCell::default(),
            edits_open: Cell::new(false),
        };
        ui::set_visible(view.quick, false);
        ui::set_visible(view.quick_hint, false);
        view
    }

    fn read(&self, ctl: &CommanderController) -> SideShown {
        let pane = ctl.pane(self.side);
        let mut jobs: Vec<(Snapshot, bool)> = Vec::new();
        jobs.extend(pane.opening().map(|s| (s, true)));
        jobs.extend(pane.wrapper_opening().map(|(_, s)| (s, false)));
        jobs.extend(pane.checking().map(|s| (s, false)));
        jobs.extend(pane.host_writing().map(|s| (s, false)));
        jobs.extend(pane.applying().map(|s| (s, false)));
        jobs.extend(pane.tool_working().map(|s| (s, false)));
        jobs.extend(pane.archive_working().map(|s| (s, false)));
        SideShown {
            bars: ctl.side_bars(self.side),
            job: jobs.into_iter().next(),
            pane: self.pane.read(ctl),
        }
    }

    fn show(&self, page: HWND, s: SideShown) -> bool {
        let (line, pct) = s
            .job
            .as_ref()
            .map(|(j, _)| crate::inspect::job_line(j))
            .unwrap_or_default();
        ui::set_text(self.job, &line);
        ui::set_visible(self.job, s.job.is_some());
        crate::inspect::set_progress(self.progress, s.job.is_some(), pct);
        ui::set_visible(self.cancel_open, s.job.as_ref().is_some_and(|(_, c)| *c));
        self.pane.show(s.pane);
        if self.bars.borrow().as_ref() == Some(&s.bars) {
            return false;
        }
        let b = s.bars;
        ui::set_text(self.source, &b.source);
        ui::set_visible(self.close_image, b.close_image.is_some());
        ui::set_enabled(self.close_image, b.close_image.unwrap_or(false));
        ui::set_visible(self.close, b.can_close);
        for h in [self.apply, self.discard] {
            ui::set_visible(h, b.stages);
            ui::set_enabled(h, b.apply_enabled);
        }
        ui::set_text(self.apply, &b.apply_label);
        ui::set_visible(self.edits, b.stages && b.staged > 0);
        ui::set_text(self.edits, &b.edits_label);
        self.edits_open.set(b.edits_open);
        let parts: Vec<&str> = b.parts.iter().map(String::as_str).collect();
        set_items(self.partition, &parts);
        if let Some(i) = b.part {
            select(self.partition, i);
        }
        ui::set_visible(self.partition, b.loaded && b.parts.len() > 1);
        ui::set_text(self.no_parts, b.no_parts);
        ui::set_visible(
            self.no_parts,
            b.loaded && b.parts.is_empty() && !b.no_parts.is_empty(),
        );
        ui::set_visible(self.new_folder, b.new_folder);
        ui::set_text(self.title, &b.title);
        ui::set_visible(self.title, b.loaded);
        ui::set_text(self.free, b.free.as_deref().unwrap_or(""));
        ui::set_visible(self.free, b.loaded && b.free.is_some());
        ui::set_visible(self.read_only, b.read_only);
        ui::set_text(self.prefix, &b.prefix);
        self.show_crumbs(page, &b.crumbs);
        let locked = b.locked;
        ui::set_visible(self.unlock_note, locked.is_some());
        ui::set_visible(self.password, locked.is_some());
        ui::set_visible(self.unlock, locked.is_some());
        if let Some(apfs) = locked {
            let note = match apfs {
                true => "This APFS volume is encrypted with FileVault. Enter the volume password or personal recovery key.",
                false => "This source is password-protected.",
            };
            ui::set_text(self.unlock_note, note);
        }
        *self.bars.borrow_mut() = Some(b);
        true
    }

    /// The path's crumbs as buttons; the last, where the pane is, as a plain label.
    fn show_crumbs(&self, page: HWND, crumbs: &[(String, Option<String>)]) {
        let held: Vec<(String, Option<String>)> = {
            let bars = self.bars.borrow();
            bars.as_ref().map(|b| b.crumbs.clone()).unwrap_or_default()
        };
        if held == crumbs && !self.crumbs.borrow().is_empty() {
            return;
        }
        for (h, _) in self.crumbs.borrow_mut().drain(..) {
            crate::theme::set_link(h, false);
            // SAFETY: our own controls, built again for the new path.
            let _ = unsafe { windows::Win32::UI::WindowsAndMessaging::DestroyWindow(h) };
        }
        let last = crumbs.len().saturating_sub(1);
        let mut built = Vec::new();
        if crumbs.is_empty() {
            built.push((ui::cell_label(page, "/"), None));
        }
        for (i, (label, target)) in crumbs.iter().enumerate() {
            match target.clone().filter(|_| i < last) {
                Some(target) => {
                    let h = ui::link_label(page, label);
                    crate::theme::set_link(h, true);
                    built.push((h, Some(target)));
                }
                None => built.push((ui::cell_label(page, label), None)),
            }
        }
        *self.crumbs.borrow_mut() = built;
    }

    /// The side's rows: bars, path, job, then the listing, the unlock row or the quick view.
    fn nodes(&self) -> Vec<Node<HWND>> {
        let shown = |h: HWND, w: i32| ui::shown(h).then(|| Node::button(h, w));
        let mut first = vec![Node::leaf(
            self.source,
            Size::Stretch(1),
            Size::Fixed(layout::ROW),
        )];
        first.extend(
            [
                (self.open_file, 60),
                (self.open_folder, 70),
                (self.open_backup, 70),
                (self.connect, 70),
                (self.close_image, 85),
                (self.close, 55),
            ]
            .iter()
            .filter_map(|(h, w)| shown(*h, *w)),
        );
        let mut second = Vec::new();
        if ui::shown(self.partition) {
            second.push(Node::leaf(
                self.partition,
                Size::Fixed(160),
                Size::Fixed(layout::ROW),
            ));
        }
        // The title takes what the buttons leave, so a narrow pane cuts the title, not Discard.
        for (h, w) in [
            (self.no_parts, Size::Fixed(110)),
            (self.title, Size::Stretch(1)),
            (self.free, Size::Fixed(100)),
            (self.read_only, Size::Fixed(70)),
        ] {
            if ui::shown(h) {
                second.push(Node::leaf(h, w, Size::Fixed(layout::ROW)));
            }
        }
        if !ui::shown(self.title) {
            second.push(Node::Space(Size::Stretch(1)));
        }
        second.extend(
            [
                (self.new_folder, 95),
                (self.apply, 80),
                (self.discard, 65),
                (self.edits, 75),
            ]
            .iter()
            .filter_map(|(h, w)| shown(*h, *w)),
        );
        // The path reads as one line: each piece as wide as its text, the prefix set off a little.
        let fit = |h: HWND| {
            Node::leaf(
                h,
                Size::Fixed(ui::text_dips(h) + 1),
                Size::Fixed(layout::ROW),
            )
        };
        let mut path = Vec::new();
        if !ui::text(self.prefix).is_empty() {
            path.push(fit(self.prefix));
            path.push(Node::Space(Size::Fixed(layout::GAP)));
        }
        path.extend(self.crumbs.borrow().iter().map(|(h, _)| fit(*h)));
        path.push(Node::Space(Size::Stretch(1)));
        let mut rows = vec![
            Node::row(first),
            Node::row(second),
            Node::Row {
                children: path,
                gap: 0,
            },
        ];
        if ui::shown(self.job) {
            let mut r = vec![
                Node::leaf(self.job, Size::Stretch(1), Size::Fixed(layout::ROW)),
                Node::leaf(
                    self.progress,
                    Size::Fixed(120),
                    Size::Fixed(layout::ROW - 6),
                ),
            ];
            if ui::shown(self.cancel_open) {
                r.push(Node::button(self.cancel_open, 70));
            }
            rows.push(Node::row(r));
        }
        if ui::shown(self.unlock) {
            rows.push(Node::leaf(
                self.unlock_note,
                Size::Stretch(1),
                Size::Fixed(2 * (layout::ROW - 8)),
            ));
            rows.push(Node::row(vec![
                Node::leaf(self.password, Size::Stretch(1), Size::Fixed(layout::ROW)),
                Node::button(self.unlock, 70),
            ]));
            rows.push(Node::Space(Size::Stretch(1)));
        } else if ui::shown(self.quick) {
            rows.push(Node::leaf(
                self.quick_hint,
                Size::Stretch(1),
                Size::Fixed(layout::ROW - 8),
            ));
            rows.push(Node::leaf(self.quick, Size::Stretch(1), Size::Stretch(1)));
        } else {
            let mut body = Vec::new();
            if ui::shown(self.pane.tree) {
                body.push(Node::leaf(
                    self.pane.tree,
                    Size::Fixed(160),
                    Size::Stretch(1),
                ));
            }
            body.push(Node::leaf(
                self.pane.list,
                Size::Stretch(1),
                Size::Stretch(1),
            ));
            rows.push(Node::row(body));
        }
        rows
    }

    /// A click on one of the side's own controls, as the actions it sends.
    fn command(&self, from: HWND, code: u32, owner: HWND) -> Option<Vec<A>> {
        let pane = |a| A::Pane(self.side, a);
        if from == self.partition {
            return (code == CBN_SELCHANGE)
                .then(|| picked(from).map(|i| vec![pane(P::SelectPartition(i))]))
                .flatten();
        }
        if code != BN_CLICKED {
            return None;
        }
        let a = if from == self.source {
            return self.source_menu(owner).map(|a| vec![a]);
        } else if from == self.open_file {
            pane(P::PickImage)
        } else if from == self.open_folder {
            pane(P::PickHostFolder)
        } else if from == self.open_backup {
            pane(P::PickBackupFolder)
        } else if from == self.connect {
            pane(P::Connect)
        } else if from == self.close {
            pane(P::CloseSource)
        } else if from == self.close_image {
            pane(P::CloseRemoteImage)
        } else if from == self.new_folder {
            pane(P::RequestNewFolder)
        } else if from == self.apply {
            pane(P::Apply)
        } else if from == self.discard {
            pane(P::Discard)
        } else if from == self.edits {
            pane(P::ShowEdits(!self.edits_open.get()))
        } else if from == self.cancel_open {
            pane(P::CancelOpen)
        } else if from == self.unlock {
            let secret = ui::text(self.password);
            ui::set_text(self.password, "");
            pane(P::SubmitPassword(secret))
        } else if let Some(target) = self
            .crumbs
            .borrow()
            .iter()
            .find(|(h, _)| *h == from)
            .and_then(|(_, t)| t.clone())
        {
            pane(P::NavigateTo(target))
        } else {
            return None;
        };
        Some(vec![A::Focus(self.side), a])
    }

    /// The source button's menu: the recent sources, then the ways to open one.
    fn source_menu(&self, owner: HWND) -> Option<A> {
        let pane = |a| A::Pane(self.side, a);
        let item = |label: &str, action| MenuItem::Item {
            label: label.to_string(),
            enabled: true,
            checked: false,
            action,
        };
        let recent = self
            .bars
            .borrow()
            .as_ref()
            .map(|b| b.recent.clone())
            .unwrap_or_default();
        let mut items: Vec<MenuItem> = recent
            .into_iter()
            .take(8)
            .map(|path| item(&path.clone(), pane(P::OpenPath(PathBuf::from(path)))))
            .collect();
        items.push(item("Open Image File...", pane(P::PickImage)));
        items.push(item("Open Host Folder...", pane(P::PickHostFolder)));
        items.push(item("Open Backup Folder...", pane(P::PickBackupFolder)));
        items.push(item("Connect to Remote...", pane(P::Connect)));
        let r = rect_of(self.source);
        track(owner, items, r.left, r.bottom)
    }

    fn all(&self) -> Vec<HWND> {
        let mut v = vec![
            self.close,
            self.close_image,
            self.partition,
            self.no_parts,
            self.title,
            self.free,
            self.read_only,
            self.new_folder,
            self.apply,
            self.discard,
            self.edits,
            self.job,
            self.cancel_open,
            self.unlock,
            self.quick,
            self.pane.tree,
        ];
        v.extend(self.crumbs.borrow().iter().map(|(h, _)| *h));
        v
    }
}

/// The column between the panes.
struct Column {
    ltr: HWND,
    rtl: HWND,
    delete: HWND,
    compare: HWND,
    keep_dates: HWND,
    format_caption: HWND,
    format: HWND,
    fork_caption: HWND,
    fork: HWND,
}

impl Column {
    fn build(page: HWND) -> Column {
        let formats: Vec<&str> = ExportFormat::ALL.iter().map(|f| f.label()).collect();
        let forks: Vec<&str> = ResourceForkMode::ALL.iter().map(|m| m.label()).collect();
        let compare = ui::button(page, "Compare", ids::COMPARE);
        ui::set_enabled(compare, false);
        Column {
            ltr: ui::button(page, "Copy ->", ids::COPY_LEFT_TO_RIGHT),
            rtl: ui::button(page, "<- Copy", ids::COPY_RIGHT_TO_LEFT),
            delete: ui::button(page, "Delete", ids::DELETE),
            compare,
            keep_dates: ui::checkbox(page, "Keep original dates", ids::KEEP_DATES),
            format_caption: ui::label(page, "Export as:"),
            format: combo(page, ids::EXPORT_FORMAT, &formats),
            fork_caption: ui::label(page, "Forks as:"),
            fork: combo(page, ids::FORK_MODE, &forks),
        }
    }

    fn show(&self, c: &CopyColumn) {
        ui::set_enabled(self.ltr, c.copy_left);
        ui::set_enabled(self.rtl, c.copy_right);
        ui::set_enabled(self.delete, c.delete);
        if checked(self.keep_dates) != c.keep_dates {
            // SAFETY: a plain button message; it sends no BN_CLICKED.
            unsafe {
                SendMessageW(
                    self.keep_dates,
                    BM_SETCHECK,
                    Some(WPARAM(c.keep_dates as usize)),
                    None,
                );
            }
        }
        select(self.format, c.format);
        select(self.fork, c.fork);
        ui::set_enabled(self.fork, c.loose);
    }

    fn nodes(&self) -> Node<HWND> {
        let w = COLUMN_W;
        let line = |h: HWND| Node::leaf(h, Size::Fixed(w), Size::Fixed(layout::ROW - 8));
        Node::column(vec![
            Node::Space(Size::Fixed(3 * layout::ROW)),
            Node::button(self.ltr, w),
            Node::button(self.rtl, w),
            Node::button(self.delete, w),
            Node::button(self.compare, w),
            Node::leaf(self.keep_dates, Size::Fixed(w), Size::Fixed(layout::ROW)),
            line(self.format_caption),
            Node::leaf(self.format, Size::Fixed(w), Size::Fixed(layout::ROW)),
            line(self.fork_caption),
            Node::leaf(self.fork, Size::Fixed(w), Size::Fixed(layout::ROW)),
            Node::Space(Size::Stretch(1)),
        ])
    }

    fn command(&self, from: HWND, code: u32) -> Option<A> {
        if from == self.format && code == CBN_SELCHANGE {
            return picked(from)
                .and_then(|i| ExportFormat::ALL.get(i))
                .map(|f| A::SetExportFormat(*f));
        }
        if from == self.fork && code == CBN_SELCHANGE {
            return picked(from)
                .and_then(|i| ResourceForkMode::ALL.get(i))
                .map(|m| A::SetForkMode(*m));
        }
        if code != BN_CLICKED {
            return None;
        }
        if from == self.ltr {
            Some(A::Copy(Side::Left))
        } else if from == self.rtl {
            Some(A::Copy(Side::Right))
        } else if from == self.delete {
            Some(A::Delete)
        } else if from == self.keep_dates {
            Some(A::SetKeepDates(checked(from)))
        } else {
            None
        }
    }
}

/// Everything a refresh shows, read while the controller is borrowed.
struct Shown {
    status: String,
    log_lines: usize,
    copying: Option<Snapshot>,
    sides: Vec<SideShown>,
    column: CopyColumn,
    windows: Vec<PaneWindow>,
    active: Side,
    /// The side the quick view covers, its text and its hint.
    quick: Option<(Side, String, String)>,
    menu_up: bool,
    follow: bool,
}

pub struct CommanderScreen {
    ctrl: RefCell<CommanderController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<CommanderScreen>,
    page: HWND,
    title: HWND,
    view: HWND,
    log: HWND,
    close: HWND,
    copy_job: HWND,
    copy_progress: HWND,
    cancel_copy: HWND,
    sides: [SideView; 2],
    column: Column,
    status: HWND,
    windows: RefCell<Vec<Rc<ModelWindow>>>,
    debug: [Cell<bool>; 2],
    active: Cell<Side>,
    /// `CloseScreen` arrived; the app drops the screen once the action in hand is done.
    closed: Cell<bool>,
    on_close: Rc<dyn Fn()>,
    follow_timer: Cell<usize>,
    shape: RefCell<Vec<bool>>,
}

thread_local! {
    /// The screen the follow timer wakes; a timer callback has no context of its own.
    static FOLLOW: RefCell<Weak<CommanderScreen>> = const { RefCell::new(Weak::new()) };
}

unsafe extern "system" fn follow_fired(_: HWND, _: u32, id: usize, _: u32) {
    // SAFETY: the timer is ours and one-shot.
    let _ = unsafe { KillTimer(None, id) };
    if let Some(screen) = FOLLOW.with(|f| f.borrow().upgrade()) {
        screen.follow_timer.set(0);
        screen.tick();
    }
}

impl CommanderScreen {
    /// The screen on `page`; `on_close` runs when the controller leaves it, and drops it.
    pub fn build(
        page: HWND,
        jobs: &JobRegistry,
        notify: &Arc<dyn Notify>,
        host: EffectHost,
        on_close: Rc<dyn Fn()>,
    ) -> Rc<CommanderScreen> {
        let title = ui::cell_label(page, "Commander Mode");
        let view = ui::button(page, "View...", ids::VIEW_MENU);
        let log = ui::button(page, "Log (0)", ids::LOG);
        let close = ui::button(page, "Close", ids::CLOSE);
        let copy_job = ui::cell_label(page, "");
        let copy_progress = ui::child(
            page,
            w!("msctls_progress32"),
            "",
            WINDOW_STYLE(0),
            WINDOW_EX_STYLE(0),
            "",
        );
        let cancel_copy = ui::button(page, "Cancel", ids::CANCEL_COPY);
        let sides = [
            SideView::build(page, Side::Left),
            SideView::build(page, Side::Right),
        ];
        let column = Column::build(page);
        let status = ui::cell_label(page, "");
        ui::set_automation_id(status, ids::STATUS);
        let screen = Rc::new_cyclic(|me: &Weak<CommanderScreen>| CommanderScreen {
            ctrl: RefCell::new(new_controller(jobs, notify)),
            host,
            sync: SyncGuard::default(),
            me: me.clone(),
            page,
            title,
            view,
            log,
            close,
            copy_job,
            copy_progress,
            cancel_copy,
            sides,
            column,
            status,
            windows: RefCell::default(),
            debug: [Cell::new(false), Cell::new(false)],
            active: Cell::new(Side::Left),
            closed: Cell::new(false),
            on_close,
            follow_timer: Cell::new(0),
            shape: RefCell::default(),
        });
        FOLLOW.with(|f| *f.borrow_mut() = screen.me.clone());
        let weak: Weak<dyn Handler> = screen.me.clone();
        panel::register(page, weak);
        screen.refresh();
        screen.sides[0].pane.focus();
        screen
    }

    /// Staged edits would be lost if the window closed now.
    pub fn has_staged_edits(&self) -> bool {
        self.ctrl.borrow().has_staged_edits()
    }

    /// Files dropped from Explorer at screen point `at`: the first opens in the pane under it.
    /// A drop between the panes asks which one it is for.
    pub fn drop_paths(&self, paths: Vec<PathBuf>, at: POINT) {
        let Some(path) = paths.first().cloned() else {
            return;
        };
        let side = match self.side_at(at) {
            Some(side) => side,
            None => {
                let name = path
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|| path.display().to_string());
                let more = match paths.len() {
                    1 => String::new(),
                    n => format!(
                        " Only the first of the {n} dropped items opens; a pane shows one source."
                    ),
                };
                let message = format!("Which pane should open \"{name}\"?{more}");
                let buttons = [
                    "Open in left pane".to_string(),
                    "Open in right pane".to_string(),
                ];
                match crate::effects::ask(self.owner(), "Open dropped item", &message, &buttons) {
                    Some(0) => Side::Left,
                    Some(1) => Side::Right,
                    _ => return,
                }
            }
        };
        self.apply(A::Focus(side));
        self.apply(A::OpenDropped(path));
    }

    /// The side whose bars or listing hold screen point `at`.
    fn side_at(&self, at: POINT) -> Option<Side> {
        SIDES.into_iter().find(|side| {
            let view = &self.sides[idx(*side)];
            let top = rect_of(view.source);
            let bottom = rect_of(view.pane.list);
            let area = RECT {
                left: top.left,
                top: top.top,
                right: bottom.right,
                bottom: bottom.bottom,
            };
            // SAFETY: a plain point-in-rectangle test.
            unsafe { PtInRect(&area, at).as_bool() }
        })
    }

    /// The window is closing with edits staged: ask the controller, which may quit after.
    pub fn hold_quit(&self) {
        self.apply(A::RequestQuit);
    }

    pub fn set_icons(&self) {
        for s in &self.sides {
            s.pane.set_icons();
        }
    }

    fn apply(&self, action: A) {
        let fx = self.ctrl.borrow_mut().apply(action);
        self.after(fx);
    }

    pub fn tick(&self) {
        let fx = self.ctrl.borrow_mut().tick();
        self.after(fx);
    }

    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            let Some(screen) = me.upgrade() else {
                return;
            };
            let action = match reply {
                Reply::Picked { purpose, path } => A::Picked { purpose, path },
                Reply::Confirmed { purpose, yes } => A::Confirmed { purpose, yes },
                Reply::Answered { purpose, text } => A::Answered { purpose, text },
                Reply::Chose { purpose, choice } => A::Chose { purpose, choice },
                Reply::CloseScreen => {
                    screen.closed.set(true);
                    return;
                }
            };
            screen.apply(action);
        })
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        if self.closed.get() {
            self.windows.borrow_mut().clear();
            (self.on_close)();
            return;
        }
        self.refresh();
    }

    /// Run a key through the key map; true when it meant something here.
    fn key(&self, key: Key, mods: Mods) -> bool {
        let (command, side) = {
            let ctl = self.ctrl.borrow();
            if ctl.modal_open() || !ctl.binds(key, mods) {
                return false;
            }
            (ctl.command_for(key, mods), ctl.active())
        };
        if matches!(command, Some(Command::PageUp | Command::PageDown)) {
            let rows = self.sides[idx(side)].pane.page_rows();
            self.apply(A::SetPageRows(side, rows));
        }
        self.apply(A::Key(key, mods));
        true
    }

    fn read(&self) -> Shown {
        let ctl = self.ctrl.borrow();
        let active = ctl.active();
        let quick = ctl.quick_view().then(|| {
            let side = active.other();
            let text = ctl
                .quick_view_detail()
                .map(|d| d.info_text(ctl.browse_caps()))
                .unwrap_or_default();
            let hint = ctl.key_hint(Command::QuickView);
            (side, text, format!("Quick view ({hint} to close)"))
        });
        let mut windows = Vec::new();
        for side in SIDES {
            windows.extend(ctl.side_windows(side, self.debug[idx(side)].get()));
        }
        windows.extend(ctl.screen_windows());
        Shown {
            status: ctl.status().to_string(),
            log_lines: ctl.log().len(),
            copying: ctl.copy_job().map(|(_, s)| s),
            sides: self.sides.iter().map(|s| s.read(&ctl)).collect(),
            column: ctl.copy_column(),
            windows,
            active,
            quick,
            menu_up: ctl.view_menu_shown(),
            follow: ctl.follow_pending(),
        }
    }

    /// Read the controller first and release it, then set the controls inside the guard.
    fn refresh(&self) {
        let shown = self.read();
        let follow = shown.follow;
        let menu_up = shown.menu_up;
        let mut relayout = false;
        self.sync.run(|| relayout = self.show(shown));
        if relayout || *self.shape.borrow() != self.shape_now() {
            self.layout();
        }
        if menu_up {
            // SAFETY: posting to our own page; the menu opens once this refresh is done.
            unsafe {
                let _ = PostMessageW(
                    Some(self.page),
                    WM_COMMAND,
                    WPARAM((VIEW_MENU as usize) << 16),
                    LPARAM(self.view.0 as isize),
                );
            }
        }
        if follow && self.follow_timer.get() == 0 {
            let ms = FOLLOW_DELAY.as_millis() as u32;
            // SAFETY: a one-shot thread timer; its callback finds the screen through FOLLOW.
            let id = unsafe { SetTimer(None, 0, ms, Some(follow_fired)) };
            self.follow_timer.set(id);
        }
    }

    fn show(&self, s: Shown) -> bool {
        ui::set_text(self.status, &s.status);
        ui::set_text(self.log, &format!("Log ({})", s.log_lines));
        let (line, pct) = s
            .copying
            .as_ref()
            .map(crate::inspect::job_line)
            .unwrap_or_default();
        ui::set_text(self.copy_job, &line);
        for h in [self.copy_job, self.cancel_copy] {
            ui::set_visible(h, s.copying.is_some());
        }
        crate::inspect::set_progress(self.copy_progress, s.copying.is_some(), pct);
        let mut relayout = false;
        for (view, side) in self.sides.iter().zip(s.sides) {
            relayout |= view.show(self.page, side);
        }
        self.column.show(&s.column);
        self.show_quick(s.quick);
        self.show_windows(s.windows);
        if self.active.replace(s.active) != s.active {
            self.sides[idx(s.active)].pane.focus();
        }
        relayout
    }

    fn show_quick(&self, quick: Option<(Side, String, String)>) {
        let covered = quick.as_ref().map(|q| q.0);
        for view in &self.sides {
            let on = covered == Some(view.side);
            ui::set_visible(view.quick, on);
            ui::set_visible(view.quick_hint, on);
            ui::set_visible(view.pane.list, !on);
            if on {
                ui::set_visible(view.pane.tree, false);
            }
        }
        if let Some((side, text, hint)) = quick {
            let view = &self.sides[idx(side)];
            ui::set_text(view.quick, &text.replace('\n', "\r\n"));
            ui::set_text(view.quick_hint, &hint);
        }
    }

    /// Open, update or close the controller's windows: one per side and kind.
    fn show_windows(&self, wanted: Vec<PaneWindow>) {
        let mut open = self.windows.borrow_mut();
        open.retain(|w| wanted.iter().any(|p| (p.side, p.kind) == (w.side, w.kind)));
        for p in wanted {
            let at = open
                .iter()
                .position(|w| (w.side, w.kind) == (p.side, p.kind));
            if let Some(w) = at.map(|i| &open[i]).filter(|w| w.key == p.key) {
                w.update(&p);
                continue;
            }
            let rect = at.map(|i| open.remove(i).rect());
            let me = self.me.clone();
            let side = p.side;
            let on_event: Rc<dyn Fn(Event)> = Rc::new(move |e| {
                if let Some(screen) = me.upgrade() {
                    screen.window_event(side, e);
                }
            });
            let w = ModelWindow::open(self.owner(), &p, rect, on_event);
            let me = self.me.clone();
            let keys: panel::KeyHook = Rc::new(move |vk| {
                let screen = me.upgrade();
                let key = key_of(vk).filter(|k| matches!(k, Key::F(_)));
                matches!((screen, key), (Some(s), Some(k)) if s.key(k, mods()))
            });
            panel::set_keys(w.hwnd(), keys);
            open.push(w);
        }
    }

    fn window_event(&self, side: Option<Side>, e: Event) {
        match e {
            Event::Send(actions) => {
                for a in actions {
                    self.apply(a);
                }
            }
            Event::Copy(text) => self.host.copy(&text),
            Event::Toggle(on) => {
                if let Some(side) = side {
                    self.debug[idx(side)].set(on);
                }
                self.refresh();
            }
            Event::Closed => self.refresh(),
        }
    }

    /// The main window, which owns the screen's windows and menus.
    fn owner(&self) -> HWND {
        // SAFETY: a plain query of our own page.
        unsafe { GetAncestor(self.page, GA_ROOT) }
    }

    fn view_menu(&self) {
        let items: Vec<MenuItem> = self
            .ctrl
            .borrow()
            .view_menu()
            .into_iter()
            .map(MenuItem::view)
            .collect();
        if self.ctrl.borrow().view_menu_shown() {
            self.apply(A::ShowViewMenu(false));
        }
        let r = rect_of(self.view);
        if let Some(a) = track(self.owner(), items, r.left, r.bottom) {
            self.apply(a);
        }
    }

    /// What the user did in `side`'s listing, as actions on the controller.
    fn pane_event(&self, side: Side, event: PaneEvent) {
        let pane = |a| A::Pane(side, a);
        match event {
            PaneEvent::Click { row, ctrl, shift } => {
                let click = self.ctrl.borrow().pane(side).row_click(&row, ctrl, shift);
                self.apply(A::Focus(side));
                if let Some(action) = click {
                    self.apply(pane(action));
                }
            }
            PaneEvent::Mark(id) => self.apply(pane(P::Mark(id))),
            PaneEvent::Toggle(id) => self.apply(pane(P::ToggleExpand(id))),
            PaneEvent::Activate => {
                let id = {
                    let ctl = self.ctrl.borrow();
                    let p = ctl.pane(side);
                    p.cursor_index().map(|i| p.rows()[i].node_id.clone())
                };
                self.apply(A::Focus(side));
                if let Some(id) = id {
                    self.apply(pane(P::Activate(id)));
                }
            }
            PaneEvent::Key(key, mods) => {
                if self.ctrl.borrow().active() != side {
                    self.apply(A::Focus(side));
                }
                self.key(key, mods);
            }
            PaneEvent::Sort(col) => self.apply(pane(P::SortBy(col))),
            PaneEvent::Menu(row) => {
                self.apply(A::Focus(side));
                let context = {
                    let ctl = self.ctrl.borrow();
                    let p = ctl.pane(side);
                    p.applying()
                        .is_none()
                        .then(|| p.row_context(&row))
                        .flatten()
                };
                if let Some(a) = context {
                    self.apply(pane(a));
                }
                let items: Vec<MenuItem> = self
                    .ctrl
                    .borrow()
                    .row_menu(side, &row)
                    .into_iter()
                    .map(MenuItem::row)
                    .collect();
                if items.is_empty() {
                    return;
                }
                let mut at = POINT::default();
                // SAFETY: a plain query of the pointer.
                let _ = unsafe { GetCursorPos(&mut at) };
                if let Some(a) = track(self.owner(), items, at.x, at.y) {
                    self.apply(a);
                }
            }
            PaneEvent::Drag => self.drag(side),
            PaneEvent::Focused => {
                if self.ctrl.borrow().active() != side {
                    self.apply(A::Focus(side));
                }
            }
        }
    }

    /// Follow a row dragged off `side`'s list: let go over the other pane, it copies there.
    fn drag(&self, side: Side) {
        let from = self.sides[idx(side)].pane.list;
        let target = self.sides[idx(side.other())].pane.list;
        let can = self.ctrl.borrow().can_copy(side);
        let mut over = false;
        // SAFETY: a capture loop on our own list; every message is dispatched as usual.
        unsafe {
            let _ = SetCapture(from);
            let arrow = LoadCursorW(None, IDC_ARROW).ok();
            let no = LoadCursorW(None, IDC_NO).ok();
            let mut msg = MSG::default();
            while GetMessageW(&mut msg, None, 0, 0).as_bool() {
                match msg.message {
                    WM_MOUSEMOVE => {
                        let mut at = POINT::default();
                        let _ = GetCursorPos(&mut at);
                        over = can && PtInRect(&rect_of(target), at).as_bool();
                        SetCursor(if over { arrow } else { no });
                    }
                    WM_LBUTTONUP => break,
                    WM_KEYDOWN if msg.wParam.0 as u16 == VK_ESCAPE.0 => {
                        over = false;
                        break;
                    }
                    WM_CAPTURECHANGED => {
                        over = false;
                        break;
                    }
                    _ => {
                        let _ = TranslateMessage(&msg);
                        DispatchMessageW(&msg);
                    }
                }
            }
            let _ = ReleaseCapture();
        }
        if over {
            self.apply(A::Copy(side));
        }
    }

    /// Which optional controls are up, so a change lays the page out again.
    fn shape_now(&self) -> Vec<bool> {
        let mut all = vec![self.copy_job];
        for s in &self.sides {
            all.extend(s.all());
        }
        all.iter().map(|h| ui::shown(*h)).collect()
    }

    pub fn layout(&self) {
        *self.shape.borrow_mut() = self.shape_now();
        let (area, dpi) = panel::client(self.page);
        let mut col = vec![Node::row(vec![
            Node::leaf(self.title, Size::Fixed(150), Size::Fixed(layout::ROW)),
            Node::button(self.view, 70),
            Node::Space(Size::Stretch(1)),
            Node::button(self.log, 80),
            Node::button(self.close, 70),
        ])];
        if ui::shown(self.copy_job) {
            col.push(Node::row(vec![
                Node::leaf(self.copy_job, Size::Stretch(1), Size::Fixed(layout::ROW)),
                Node::leaf(
                    self.copy_progress,
                    Size::Fixed(160),
                    Size::Fixed(layout::ROW - 6),
                ),
                Node::button(self.cancel_copy, 70),
            ]));
        }
        col.push(Node::row(vec![
            Node::column(self.sides[0].nodes()),
            self.column.nodes(),
            Node::column(self.sides[1].nodes()),
        ]));
        col.push(Node::leaf(
            self.status,
            Size::Stretch(1),
            Size::Fixed(layout::ROW),
        ));
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        for (h, r) in layout::layout(&tree, area, dpi) {
            if ui::shown(h) {
                ui::move_to(h, r);
            }
        }
    }

    fn side_of(&self, h: HWND) -> Option<Side> {
        SIDES
            .into_iter()
            .find(|s| self.sides[idx(*s)].pane.list == h || self.sides[idx(*s)].pane.tree == h)
    }
}

impl Handler for CommanderScreen {
    fn command(&self, from: HWND, code: u32) {
        if code == DEFERRED {
            if let Some(side) = self.side_of(from) {
                if let Some(action) = self.sides[idx(side)].pane.take_deferred() {
                    self.apply(A::Focus(side));
                    self.apply(A::Pane(side, action));
                }
            }
            return;
        }
        if code == VIEW_MENU {
            self.view_menu();
            return;
        }
        if self.sync.active() {
            return;
        }
        if let Some(a) = self.column.command(from, code) {
            self.apply(a);
            return;
        }
        for view in &self.sides {
            if let Some(actions) = view.command(from, code, self.owner()) {
                for a in actions {
                    self.apply(a);
                }
                return;
            }
        }
        if code != BN_CLICKED {
            return;
        }
        if from == self.view {
            self.view_menu();
        } else if from == self.log {
            let shown = self.ctrl.borrow().log_shown();
            self.apply(A::ShowLog(!shown));
        } else if from == self.close {
            self.apply(A::RequestClose);
        } else if from == self.cancel_copy {
            self.apply(A::CancelCopy);
        }
    }

    fn notify(&self, hdr: &NMHDR, lp: LPARAM) -> Option<LRESULT> {
        for view in &self.sides {
            if hdr.hwndFrom == view.source && hdr.code == BCN_DROPDOWN {
                if let Some(a) = view.source_menu(self.owner()) {
                    self.apply(A::Focus(view.side));
                    self.apply(a);
                }
                return Some(LRESULT(0));
            }
            if let Some((answer, event)) = view.pane.notify(hdr, lp) {
                if let Some(event) = event {
                    self.pane_event(view.side, event);
                }
                return answer;
            }
        }
        None
    }

    fn resized(&self) {
        self.layout();
    }

    fn close(&self) {}
}
