//! Inspect's remote image picker over `controller::remote_pick`; GTK's `inspect/remote.rs` is the
//! reference. The daemon's listing takes Inspect's place on a page of its own until it hands an
//! image over, so its keys reach it like the rest of the main window's.

use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use rusty_backup::controller::app::Notify;
use rusty_backup::controller::commander::Tone;
use rusty_backup::controller::remote_pick::{
    ids, PickRow, RemoteImage, RemotePickAction, RemotePickController,
};
use rusty_backup::controller::{Controller, Effect};
use windows::core::{w, PWSTR};
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, WPARAM};
use windows::Win32::UI::Controls::{
    LVCF_TEXT, LVCF_WIDTH, LVCOLUMNW, LVIF_TEXT, LVITEMW, LVM_DELETEALLITEMS, LVM_GETNEXTITEM,
    LVM_INSERTCOLUMNW, LVM_INSERTITEMW, LVM_SETEXTENDEDLISTVIEWSTYLE, LVM_SETITEMTEXTW,
    LVNI_SELECTED, LVN_ITEMACTIVATE, LVS_EX_DOUBLEBUFFER, LVS_EX_FULLROWSELECT, LVS_NOSORTHEADER,
    LVS_REPORT, LVS_SHOWSELALWAYS, LVS_SINGLESEL, NMHDR,
};
use windows::Win32::UI::HiDpi::GetDpiForWindow;
use windows::Win32::UI::WindowsAndMessaging::{
    SendMessageW, BN_CLICKED, WINDOW_EX_STYLE, WINDOW_STYLE, WS_EX_CLIENTEDGE, WS_TABSTOP,
};

use crate::build_disk::set_line;
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::layout::{self, Node, Size};
use crate::panel::{self, Handler};
use crate::sync::SyncGuard;
use crate::ui;

/// The list asks the dialog loop for Enter, which `IsDialogMessage` would keep for a default button.
unsafe extern "system" fn list_proc(
    hwnd: HWND,
    msg: u32,
    wp: WPARAM,
    lp: LPARAM,
    _id: usize,
    _data: usize,
) -> LRESULT {
    use windows::Win32::UI::Input::KeyboardAndMouse::VK_RETURN;
    use windows::Win32::UI::WindowsAndMessaging::{
        DLGC_WANTMESSAGE, MSG, WM_GETDLGCODE, WM_KEYDOWN,
    };
    if msg == WM_GETDLGCODE && lp.0 != 0 {
        // SAFETY: WM_GETDLGCODE's lParam, when set, is the MSG being dispatched.
        let m = unsafe { &*(lp.0 as *const MSG) };
        if m.message == WM_KEYDOWN && m.wParam.0 as u16 == VK_RETURN.0 {
            return LRESULT(DLGC_WANTMESSAGE as isize);
        }
    }
    // SAFETY: everything else goes on to the list.
    unsafe { windows::Win32::UI::Shell::DefSubclassProc(hwnd, msg, wp, lp) }
}

const COLUMNS: [(&str, i32); 3] = [("Name", 420), ("Size", 110), ("Type", 90)];

fn cells(r: &PickRow) -> [String; 3] {
    match r.parent {
        true => ["..".to_string(), String::new(), "dir".to_string()],
        false => [r.name.clone(), r.size.clone(), r.type_tag.clone()],
    }
}

pub struct RemotePage {
    ctrl: RefCell<RemotePickController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<RemotePage>,
    /// Runs after every change, so Inspect drains the log and opens a picked image.
    on_change: RefCell<Option<Rc<dyn Fn()>>>,
    page: HWND,
    title: HWND,
    job: HWND,
    job_progress: HWND,
    error: HWND,
    hint: HWND,
    list: HWND,
    rows: RefCell<Vec<PickRow>>,
    disconnect: HWND,
}

impl RemotePage {
    pub fn build(page: HWND, notify: Arc<dyn Notify>, host: EffectHost) -> Rc<RemotePage> {
        let style = WINDOW_STYLE(
            LVS_REPORT | LVS_NOSORTHEADER | LVS_SHOWSELALWAYS | LVS_SINGLESEL | WS_TABSTOP.0,
        );
        let list = ui::child(
            page,
            w!("SysListView32"),
            "",
            style,
            WS_EX_CLIENTEDGE,
            ids::LIST,
        );
        // SAFETY: plain list-view setup; each title outlives its insert.
        unsafe {
            let ex = LVS_EX_FULLROWSELECT | LVS_EX_DOUBLEBUFFER;
            SendMessageW(
                list,
                LVM_SETEXTENDEDLISTVIEWSTYLE,
                Some(WPARAM(ex as usize)),
                Some(LPARAM(ex as isize)),
            );
            let dpi = GetDpiForWindow(page).max(96);
            for (i, (title, width)) in COLUMNS.iter().enumerate() {
                let mut t = ui::wide(title);
                let col = LVCOLUMNW {
                    mask: LVCF_TEXT | LVCF_WIDTH,
                    cx: layout::scale(*width, dpi),
                    pszText: PWSTR(t.as_mut_ptr()),
                    ..Default::default()
                };
                SendMessageW(
                    list,
                    LVM_INSERTCOLUMNW,
                    Some(WPARAM(i)),
                    Some(LPARAM(&col as *const _ as isize)),
                );
            }
        }
        crate::theme::control(list);
        // SAFETY: subclassing our own list; the proc lives as long as the program.
        unsafe {
            let _ = windows::Win32::UI::Shell::SetWindowSubclass(list, Some(list_proc), 1, 0);
        }
        let progress = ui::child(
            page,
            w!("msctls_progress32"),
            "",
            WINDOW_STYLE(0),
            WINDOW_EX_STYLE(0),
            "",
        );
        let me = Rc::new_cyclic(|me: &Weak<RemotePage>| RemotePage {
            ctrl: RefCell::new(RemotePickController::new(notify)),
            host,
            sync: SyncGuard::default(),
            me: me.clone(),
            on_change: RefCell::default(),
            page,
            title: ui::label(page, "Remote images"),
            job: ui::cell_label(page, ""),
            job_progress: progress,
            error: ui::label(page, ""),
            hint: ui::label(page, ""),
            list,
            rows: RefCell::default(),
            disconnect: ui::button(page, "", ids::DISCONNECT),
        });
        let weak: Weak<dyn Handler> = me.me.clone();
        panel::register(page, weak);
        me
    }

    /// Inspect's hook: it drains the log and opens a picked image after every change.
    pub fn on_change(&self, f: Rc<dyn Fn()>) {
        *self.on_change.borrow_mut() = Some(f);
    }

    pub fn is_active(&self) -> bool {
        self.ctrl.borrow().is_active()
    }

    pub fn apply(&self, action: RemotePickAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    pub fn tick(&self) {
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    /// Status lines since the last call, and the image picked for Inspect to open.
    pub fn take(&self) -> (Vec<String>, Option<RemoteImage>) {
        let mut ctrl = self.ctrl.borrow_mut();
        (ctrl.take_log(), ctrl.take_request())
    }

    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            let Some(p) = me.upgrade() else {
                return;
            };
            let action = match reply {
                Reply::Answered { purpose, text } => RemotePickAction::Answered { purpose, text },
                // The picker asks only for the daemon's address.
                Reply::Picked { .. }
                | Reply::Confirmed { .. }
                | Reply::Chose { .. }
                | Reply::CloseScreen => return,
            };
            p.apply(action);
        })
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        self.refresh();
        let f = self.on_change.borrow().clone();
        if let Some(f) = f {
            f();
        }
    }

    fn refresh(&self) {
        let ctrl = self.ctrl.borrow();
        let loaded = ctrl.working().is_none() && ctrl.error().is_none() && ctrl.is_loaded();
        let job = ctrl.working();
        let rows = if loaded { ctrl.rows() } else { Vec::new() };
        let hint = loaded.then(|| ctrl.hint());
        self.sync.run(|| {
            let (line, pct) = job
                .as_ref()
                .map(crate::inspect::job_line)
                .unwrap_or_default();
            ui::set_text(self.job, &line);
            ui::set_visible(self.job, job.is_some());
            crate::inspect::set_progress(self.job_progress, job.is_some(), pct);
            set_line(self.error, ctrl.error(), Some(Tone::Error));
            set_line(self.hint, hint, None);
            ui::set_visible(self.list, loaded);
            ui::set_visible(self.disconnect, ctrl.can_disconnect());
            ui::set_enabled(self.disconnect, !ctrl.transition_running());
            ui::set_text(self.disconnect, &ctrl.disconnect_label());
            if *self.rows.borrow() != rows {
                self.fill(&rows);
                *self.rows.borrow_mut() = rows;
            }
        });
        drop(ctrl);
        self.layout();
    }

    fn fill(&self, rows: &[PickRow]) {
        // SAFETY: plain list-view messages; each text outlives its message.
        unsafe {
            SendMessageW(self.list, LVM_DELETEALLITEMS, None, None);
            for (n, row) in rows.iter().enumerate() {
                for (col, text) in cells(row).iter().enumerate() {
                    // Names come off the daemon: `plain` keeps control characters out.
                    let mut t = ui::wide(&ui::plain(text));
                    let item = LVITEMW {
                        mask: LVIF_TEXT,
                        iItem: n as i32,
                        iSubItem: col as i32,
                        pszText: PWSTR(t.as_mut_ptr()),
                        ..Default::default()
                    };
                    let (msg, wp) = match col {
                        0 => (LVM_INSERTITEMW, 0),
                        _ => (LVM_SETITEMTEXTW, n),
                    };
                    SendMessageW(
                        self.list,
                        msg,
                        Some(WPARAM(wp)),
                        Some(LPARAM(&item as *const _ as isize)),
                    );
                }
            }
        }
    }

    /// Double-click or Enter on a row: up, into a folder, or open the image.
    fn activated(&self) {
        // SAFETY: a plain list-view query.
        let i = unsafe {
            SendMessageW(
                self.list,
                LVM_GETNEXTITEM,
                Some(WPARAM(usize::MAX)),
                Some(LPARAM(LVNI_SELECTED as isize)),
            )
            .0
        };
        let row = usize::try_from(i)
            .ok()
            .and_then(|i| self.rows.borrow().get(i).cloned());
        let action = match row {
            Some(r) if r.parent => RemotePickAction::Up,
            Some(r) if r.is_dir => RemotePickAction::Enter(r.name),
            Some(r) if r.openable => RemotePickAction::OpenImage(r.path),
            _ => return,
        };
        self.apply(action);
    }

    fn layout(&self) {
        let (area, dpi) = panel::client(self.page);
        let row_h = Size::Fixed(layout::ROW);
        let line = |h: HWND| Node::leaf(h, Size::Stretch(1), Size::Fixed(layout::ROW - 8));
        let mut col = vec![Node::row(vec![
            Node::leaf(self.title, Size::Stretch(1), row_h),
            Node::button(self.disconnect, 260),
        ])];
        if ui::shown(self.job) {
            col.push(Node::row(vec![
                Node::leaf(self.job, Size::Stretch(1), row_h),
                Node::leaf(
                    self.job_progress,
                    Size::Fixed(180),
                    Size::Fixed(layout::ROW - 6),
                ),
            ]));
        }
        for h in [self.error, self.hint] {
            if ui::shown(h) {
                col.push(line(h));
            }
        }
        col.push(Node::leaf(self.list, Size::Stretch(1), Size::Stretch(1)));
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        for (h, r) in layout::layout(&tree, area, dpi) {
            if ui::shown(h) {
                ui::move_to(h, r);
            }
        }
    }
}

impl Handler for RemotePage {
    fn command(&self, from: HWND, code: u32) {
        if !self.sync.active() && code == BN_CLICKED && from == self.disconnect {
            self.apply(RemotePickAction::Disconnect);
        }
    }

    fn notify(&self, hdr: &NMHDR, _lp: LPARAM) -> Option<LRESULT> {
        if hdr.hwndFrom == self.list && hdr.code == LVN_ITEMACTIVATE && !self.sync.active() {
            self.activated();
            return Some(LRESULT(0));
        }
        None
    }

    fn resized(&self) {
        self.layout();
    }

    fn close(&self) {}
}
