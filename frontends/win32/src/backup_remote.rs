//! Backup's Remote Drive window over the Backup controller; GTK's `backup/windows.rs` is the
//! reference. Open while `BackupController::remote_dialog_open()` says so: the daemon's address,
//! Connect, and the physical drives it lists, one of which becomes the backup's source.

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use rusty_backup::controller::backup::{hints, ids, BackupAction, RemoteDrive};
use rusty_backup::controller::commander::Tone;
use windows::core::{w, PWSTR};
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, WPARAM};
use windows::Win32::UI::Controls::{
    LVCF_WIDTH, LVCOLUMNW, LVIF_TEXT, LVITEMW, LVM_DELETEALLITEMS, LVM_GETNEXTITEM,
    LVM_INSERTCOLUMNW, LVM_INSERTITEMW, LVM_SETEXTENDEDLISTVIEWSTYLE, LVNI_SELECTED,
    LVN_ITEMACTIVATE, LVS_EX_FULLROWSELECT, LVS_NOCOLUMNHEADER, LVS_REPORT, LVS_SHOWSELALWAYS,
    LVS_SINGLESEL, NMHDR,
};
use windows::Win32::UI::WindowsAndMessaging::{
    DestroyWindow, SendMessageW, ShowWindow, BN_CLICKED, EN_CHANGE, SW_SHOW, WINDOW_STYLE,
    WS_EX_CLIENTEDGE, WS_TABSTOP,
};

use crate::build_disk::{set_field, set_line};
use crate::layout::{self, Node, Size};
use crate::panel::{self, Handler};
use crate::sync::SyncGuard;
use crate::ui;

/// What the window shows, read while the controller is borrowed.
pub struct RemoteShown {
    pub address: String,
    pub busy: bool,
    pub error: Option<String>,
    /// The daemon's drives once it has listed them.
    pub drives: Option<Vec<RemoteDrive>>,
}

pub struct RemoteWindow {
    window: HWND,
    send: Rc<dyn Fn(BackupAction)>,
    sync: SyncGuard,
    me: Weak<RemoteWindow>,
    intro: HWND,
    caption: HWND,
    address: HWND,
    connect: HWND,
    close: HWND,
    busy: HWND,
    error: HWND,
    note: HWND,
    list: HWND,
    drives: RefCell<Vec<RemoteDrive>>,
}

impl RemoteWindow {
    pub fn open(owner: HWND, send: Rc<dyn Fn(BackupAction)>) -> Rc<Self> {
        let window = panel::dialog(owner, "Remote Drive", 520, 380);
        let style = WINDOW_STYLE(
            LVS_REPORT | LVS_NOCOLUMNHEADER | LVS_SHOWSELALWAYS | LVS_SINGLESEL | WS_TABSTOP.0,
        );
        let list = ui::child(
            window,
            w!("SysListView32"),
            "",
            style,
            WS_EX_CLIENTEDGE,
            ids::REMOTE_DRIVE,
        );
        // SAFETY: plain list-view setup.
        unsafe {
            let ex = LVS_EX_FULLROWSELECT;
            SendMessageW(
                list,
                LVM_SETEXTENDEDLISTVIEWSTYLE,
                Some(WPARAM(ex as usize)),
                Some(LPARAM(ex as isize)),
            );
            let col = LVCOLUMNW {
                mask: LVCF_WIDTH,
                cx: 2000,
                ..Default::default()
            };
            SendMessageW(
                list,
                LVM_INSERTCOLUMNW,
                Some(WPARAM(0)),
                Some(LPARAM(&col as *const _ as isize)),
            );
        }
        crate::theme::control(list);
        let me = Rc::new_cyclic(|me: &Weak<RemoteWindow>| RemoteWindow {
            window,
            send,
            sync: SyncGuard::default(),
            me: me.clone(),
            intro: ui::label(window, hints::REMOTE_DRIVE_INTRO),
            caption: ui::cell_label(window, "Host:"),
            address: ui::edit(window, ids::REMOTE_ADDRESS),
            connect: ui::button(window, "Connect", ids::REMOTE_CONNECT),
            close: ui::button(window, "Close", ids::REMOTE_CLOSE),
            busy: ui::cell_label(window, "Connecting..."),
            error: ui::label(window, ""),
            note: ui::label(window, ""),
            list,
            drives: RefCell::default(),
        });
        let weak: Weak<dyn Handler> = me.me.clone();
        panel::register(window, weak);
        // SAFETY: showing the window we created.
        unsafe {
            let _ = ShowWindow(window, SW_SHOW);
        }
        me
    }

    pub fn show(&self, s: RemoteShown) {
        self.sync.run(|| {
            set_field(self.address, &s.address);
            ui::set_enabled(self.connect, !s.busy);
            ui::set_visible(self.busy, s.busy);
            set_line(self.error, s.error.as_deref(), Some(Tone::Error));
            let note = s.drives.as_ref().map(|d| match d.is_empty() {
                true => hints::NO_REMOTE_DRIVES,
                false => "Pick a drive to back up (double-click or Enter):",
            });
            set_line(self.note, note, None);
            let drives = s.drives.unwrap_or_default();
            ui::set_visible(self.list, !drives.is_empty());
            if *self.drives.borrow() != drives {
                self.fill(&drives);
                *self.drives.borrow_mut() = drives;
            }
        });
        self.layout();
    }

    fn fill(&self, drives: &[RemoteDrive]) {
        // SAFETY: plain list-view messages; each text outlives its message.
        unsafe {
            SendMessageW(self.list, LVM_DELETEALLITEMS, None, None);
            for (n, d) in drives.iter().enumerate() {
                // Labels come off the daemon: `plain` keeps control characters out.
                let mut t = ui::wide(&ui::plain(&d.label));
                let item = LVITEMW {
                    mask: LVIF_TEXT,
                    iItem: n as i32,
                    pszText: PWSTR(t.as_mut_ptr()),
                    ..Default::default()
                };
                SendMessageW(
                    self.list,
                    LVM_INSERTITEMW,
                    Some(WPARAM(0)),
                    Some(LPARAM(&item as *const _ as isize)),
                );
            }
        }
    }

    fn picked(&self) -> Option<BackupAction> {
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
        let d = usize::try_from(i)
            .ok()
            .and_then(|i| self.drives.borrow().get(i).cloned())?;
        Some(BackupAction::RemotePick {
            path: d.path,
            size: d.size_bytes,
        })
    }

    fn layout(&self) {
        let (area, dpi) = panel::client(self.window);
        let row_h = Size::Fixed(layout::ROW);
        let line = |h: HWND| Node::leaf(h, Size::Stretch(1), Size::Fixed((layout::ROW - 8) * 2));
        let mut col = vec![
            line(self.intro),
            Node::row(vec![
                Node::leaf(self.caption, Size::Fixed(50), row_h),
                Node::field(self.address),
                Node::button(self.connect, 90),
            ]),
            Node::leaf(self.busy, Size::Stretch(1), row_h),
        ];
        for h in [self.error, self.note] {
            if ui::shown(h) {
                col.push(line(h));
            }
        }
        col.push(Node::leaf(self.list, Size::Stretch(1), Size::Stretch(1)));
        col.push(Node::row(vec![
            Node::Space(Size::Stretch(1)),
            Node::button(self.close, 90),
        ]));
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        for (h, r) in layout::layout(&tree, area, dpi) {
            ui::move_to(h, r);
        }
    }
}

impl Drop for RemoteWindow {
    fn drop(&mut self) {
        panel::unregister(self.window);
        // SAFETY: the controller closed the window's state; the window goes with it.
        let _ = unsafe { DestroyWindow(self.window) };
    }
}

impl Handler for RemoteWindow {
    fn command(&self, from: HWND, code: u32) {
        if self.sync.active() {
            return;
        }
        let action = match code {
            EN_CHANGE if from == self.address => BackupAction::RemoteSetAddress(ui::text(from)),
            BN_CLICKED if from == self.connect => BackupAction::RemoteConnect,
            BN_CLICKED if from == self.close => BackupAction::RemoteClose,
            _ => return,
        };
        (self.send)(action);
    }

    fn notify(&self, hdr: &NMHDR, _lp: LPARAM) -> Option<LRESULT> {
        if hdr.hwndFrom == self.list && hdr.code == LVN_ITEMACTIVATE && !self.sync.active() {
            if let Some(a) = self.picked() {
                (self.send)(a);
            }
            return Some(LRESULT(0));
        }
        None
    }

    fn resized(&self) {
        self.layout();
    }

    fn close(&self) {
        (self.send)(BackupAction::RemoteClose);
    }
}
