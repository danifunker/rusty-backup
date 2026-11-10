//! Restore's remote target rows over the Restore controller; GTK's `restore/target.rs` is the
//! reference. A full-disk restore can go to a drive or an image file on an `rb-cli serve`
//! daemon: the switch, the daemon's address with Connect, the drives it lists, an image name.

use std::cell::RefCell;

use rusty_backup::controller::commander::Tone;
use rusty_backup::controller::restore::{
    hints, ids, RemoteDrive, RestoreAction, RestoreController,
};
use windows::core::{w, PWSTR};
use windows::Win32::Foundation::{HWND, LPARAM, WPARAM};
use windows::Win32::UI::Controls::{
    LVCF_WIDTH, LVCOLUMNW, LVIF_TEXT, LVITEMW, LVM_DELETEALLITEMS, LVM_GETNEXTITEM,
    LVM_INSERTCOLUMNW, LVM_INSERTITEMW, LVM_SETEXTENDEDLISTVIEWSTYLE, LVNI_SELECTED,
    LVN_ITEMACTIVATE, LVS_EX_FULLROWSELECT, LVS_NOCOLUMNHEADER, LVS_REPORT, LVS_SHOWSELALWAYS,
    LVS_SINGLESEL, NMHDR,
};
use windows::Win32::UI::WindowsAndMessaging::{
    SendMessageW, BN_CLICKED, EN_CHANGE, WINDOW_STYLE, WS_EX_CLIENTEDGE, WS_TABSTOP,
};

use crate::build_disk::{checked, set_check, set_field, set_line};
use crate::layout::{self, Node, Size};
use crate::ui;

pub struct RemoteTarget {
    switch: HWND,
    caption: HWND,
    address: HWND,
    connect: HWND,
    busy: HWND,
    error: HWND,
    note: HWND,
    list: HWND,
    drives: RefCell<Vec<RemoteDrive>>,
    image_caption: HWND,
    image: HWND,
    use_image: HWND,
    selected: HWND,
}

impl RemoteTarget {
    pub fn build(page: HWND) -> RemoteTarget {
        let style = WINDOW_STYLE(
            LVS_REPORT | LVS_NOCOLUMNHEADER | LVS_SHOWSELALWAYS | LVS_SINGLESEL | WS_TABSTOP.0,
        );
        let list = ui::child(
            page,
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
        RemoteTarget {
            switch: ui::checkbox(page, hints::REMOTE, ids::REMOTE_TARGET),
            caption: ui::cell_label(page, "Host:"),
            address: ui::edit(page, ids::REMOTE_ADDRESS),
            connect: ui::button(page, "Connect", ids::REMOTE_CONNECT),
            busy: ui::cell_label(page, "Connecting..."),
            error: ui::label(page, ""),
            note: ui::label(page, ""),
            list,
            drives: RefCell::default(),
            image_caption: ui::cell_label(page, "or image file:"),
            image: ui::edit(page, ids::REMOTE_IMAGE_NAME),
            use_image: ui::button(page, "Use file", ids::REMOTE_USE_IMAGE),
            selected: ui::path_label(page, ""),
        }
    }

    /// The remote target is on: the local target rows step aside.
    pub fn active(&self) -> bool {
        ui::shown(self.address)
    }

    /// Run inside the screen's `SyncGuard`; `full` is the full-disk mode, the only one offering it.
    pub fn show(&self, c: &RestoreController, full: bool) {
        let on = full && c.remote_active();
        ui::set_visible(self.switch, full);
        set_check(self.switch, on);
        for h in [self.caption, self.address, self.connect] {
            ui::set_visible(h, on);
        }
        set_field(self.address, c.remote_address());
        ui::set_enabled(self.connect, !c.remote_busy());
        let label = if c.remote_connected() {
            "Reconnect"
        } else {
            "Connect"
        };
        ui::set_text(self.connect, label);
        ui::set_visible(self.busy, on && c.remote_busy());
        set_line(
            self.error,
            c.remote_error().filter(|_| on),
            Some(Tone::Error),
        );
        let linked = on && c.remote_connected();
        let drives = if linked {
            c.remote_drives()
        } else {
            Vec::new()
        };
        let note = match (linked, drives.is_empty()) {
            (true, true) => Some(hints::NO_REMOTE_DRIVES),
            (true, false) => Some("Restore target on the remote machine (double-click or Enter):"),
            (false, _) => None,
        };
        set_line(self.note, note, None);
        ui::set_visible(self.list, linked && !drives.is_empty());
        if *self.drives.borrow() != drives {
            self.fill(&drives);
            *self.drives.borrow_mut() = drives;
        }
        for h in [self.image_caption, self.image, self.use_image] {
            ui::set_visible(h, linked);
        }
        set_field(self.image, c.remote_image_name());
        ui::set_enabled(self.use_image, !c.remote_image_name().trim().is_empty());
        let chosen = c
            .remote_target_label()
            .filter(|_| linked)
            .map(|t| format!("Selected target: {t}"));
        set_line(self.selected, chosen.as_deref(), None);
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

    /// What decides the layout.
    pub fn shape(&self) -> Vec<usize> {
        [
            self.switch,
            self.address,
            self.busy,
            self.error,
            self.note,
            self.list,
            self.image,
            self.selected,
        ]
        .iter()
        .map(|h| usize::from(ui::shown(*h)))
        .collect()
    }

    pub fn nodes(&self, caption: i32) -> Vec<Node<HWND>> {
        let row_h = Size::Fixed(layout::ROW);
        let line = |h: HWND| Node::leaf(h, Size::Stretch(1), Size::Fixed(layout::ROW - 8));
        let mut col = Vec::new();
        if ui::shown(self.switch) {
            col.push(Node::row(vec![
                Node::Space(Size::Fixed(caption)),
                Node::leaf(self.switch, Size::Stretch(1), row_h),
            ]));
        }
        if !ui::shown(self.address) {
            return col;
        }
        let mut host = vec![
            Node::leaf(self.caption, Size::Fixed(caption), row_h),
            Node::field(self.address),
            Node::button(self.connect, 95),
        ];
        if ui::shown(self.busy) {
            host.push(Node::leaf(self.busy, Size::Fixed(100), row_h));
        }
        col.push(Node::row(host));
        for h in [self.error, self.note] {
            if ui::shown(h) {
                col.push(line(h));
            }
        }
        if ui::shown(self.list) {
            col.push(Node::leaf(
                self.list,
                Size::Stretch(1),
                Size::Fixed(layout::ROW * 3),
            ));
        }
        if ui::shown(self.image) {
            col.push(Node::row(vec![
                Node::leaf(self.image_caption, Size::Fixed(caption), row_h),
                Node::field(self.image),
                Node::button(self.use_image, 85),
            ]));
        }
        if ui::shown(self.selected) {
            col.push(line(self.selected));
        }
        col
    }

    /// The controls a running restore locks.
    pub fn controls(&self) -> [HWND; 6] {
        [
            self.switch,
            self.address,
            self.connect,
            self.list,
            self.image,
            self.use_image,
        ]
    }

    pub fn command(&self, from: HWND, code: u32) -> Option<RestoreAction> {
        Some(match code {
            BN_CLICKED if from == self.switch => RestoreAction::SetRemote(checked(from)),
            BN_CLICKED if from == self.connect => RestoreAction::RemoteConnect,
            BN_CLICKED if from == self.use_image => RestoreAction::RemoteUseImage,
            EN_CHANGE if from == self.address => RestoreAction::RemoteSetAddress(ui::text(from)),
            EN_CHANGE if from == self.image => RestoreAction::RemoteSetImageName(ui::text(from)),
            _ => return None,
        })
    }

    pub fn notify(&self, hdr: &NMHDR) -> Option<RestoreAction> {
        if hdr.hwndFrom != self.list || hdr.code != LVN_ITEMACTIVATE {
            return None;
        }
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
        Some(RestoreAction::RemotePickDevice {
            path: d.path,
            size: d.size_bytes,
        })
    }
}
