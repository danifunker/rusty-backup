//! Resize Partitions over `controller::resize`; `src/gui/resize_popup.rs` is the reference.
//!
//! The size grid is one row of controls per partition, rebuilt only when the rows' shape (their
//! partitions and size-mode choices) changes. The preview is a small list view, refilled when
//! the controller's preview changes. Apply asks the controller, whose own question confirms it.

use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};
use std::sync::Arc;

use rusty_backup::controller::app::{DiskDevice, Notify};
use rusty_backup::controller::inspect::{format_size, ResizePlan};
use rusty_backup::controller::resize::{
    ids, PreviewRow, ResizeAction, ResizeController, ResizeRow, SizeMode,
};
use rusty_backup::controller::{Controller, Effect};
use windows::core::{w, PWSTR};
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, WPARAM};
use windows::Win32::UI::Controls::{
    LVCF_TEXT, LVCF_WIDTH, LVCOLUMNW, LVIF_TEXT, LVITEMW, LVM_DELETEALLITEMS, LVM_INSERTCOLUMNW,
    LVM_INSERTITEMW, LVM_SETITEMTEXTW, LVS_NOSORTHEADER, LVS_REPORT, NMHDR,
};
use windows::Win32::UI::HiDpi::GetDpiForWindow;
use windows::Win32::UI::Input::KeyboardAndMouse::GetFocus;
use windows::Win32::UI::WindowsAndMessaging::{
    DestroyWindow, SendMessageW, ShowWindow, BM_GETCHECK, BM_SETCHECK, BN_CLICKED, CBN_SELCHANGE,
    CBS_DROPDOWNLIST, CB_ADDSTRING, CB_GETCURSEL, CB_SETCURSEL, EN_CHANGE, SW_SHOW,
    WINDOW_EX_STYLE, WINDOW_STYLE, WS_EX_CLIENTEDGE, WS_TABSTOP, WS_VSCROLL,
};

use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::layout::{self, Node, Size};
use crate::panel::{self, Handler};
use crate::partition_bar;
use crate::sync::SyncGuard;
use crate::ui;

/// One partition's controls in the size grid.
struct Cells {
    index: usize,
    labels: [HWND; 4],
    mode: HWND,
    choices: Vec<SizeMode>,
    size: HWND,
}

pub struct ResizeWindow {
    ctrl: RefCell<ResizeController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<ResizeWindow>,
    window: HWND,
    warn: HWND,
    risk: HWND,
    disk_line: HWND,
    current_caption: HWND,
    current: HWND,
    after_caption: HWND,
    after: HWND,
    sizes_head: HWND,
    heads: [HWND; 6],
    cells: RefCell<Vec<Cells>>,
    shape: RefCell<Vec<(usize, bool, Vec<SizeMode>)>>,
    error: HWND,
    preview_head: HWND,
    preview: HWND,
    shown_preview: RefCell<Option<Vec<PreviewRow>>>,
    job: HWND,
    target: HWND,
    preview_button: HWND,
    apply: HWND,
    cancel: HWND,
    close: HWND,
    changed: Cell<bool>,
}

impl ResizeWindow {
    pub fn open(
        owner: HWND,
        notify: Arc<dyn Notify>,
        host: EffectHost,
        plan: ResizePlan,
    ) -> Rc<Self> {
        let window = panel::dialog(owner, "Resize Partitions", 720, 600);
        let warn = ui::label(
            window,
            "WARNING: This will directly modify the physical device. Data loss is permanent and irreversible!",
        );
        let risk = ui::checkbox(
            window,
            "I understand the risks and want to proceed",
            ids::DEVICE_RISK,
        );
        let disk_line = ui::label(window, "");
        let current_caption = ui::label(window, "");
        let current = partition_bar::build(window, Box::new(|_| {}));
        let after_caption = ui::label(window, "");
        let after = partition_bar::build(window, Box::new(|_| {}));
        let sizes_head = ui::label(window, "Partition Sizes:");
        let heads = [
            "#",
            "Type",
            "Current Size",
            "Min Size",
            "Size Mode",
            "New Size (MiB)",
        ]
        .map(|t| ui::label(window, t));
        let error = ui::label(window, "");
        let preview_head = ui::label(window, "Preview:");
        let style = WINDOW_STYLE(LVS_REPORT | LVS_NOSORTHEADER | WS_TABSTOP.0);
        let preview = ui::child(
            window,
            w!("SysListView32"),
            "",
            style,
            WS_EX_CLIENTEDGE,
            ids::PREVIEW,
        );
        // SAFETY: plain list-view setup; each title outlives its insert.
        unsafe {
            let dpi = GetDpiForWindow(window).max(96);
            for (i, (title, width)) in [
                ("#", 40),
                ("Old Start", 110),
                ("Old Size", 110),
                ("New Start", 110),
                ("New Size", 110),
                ("Action", 180),
            ]
            .iter()
            .enumerate()
            {
                let mut t = ui::wide(title);
                let col = LVCOLUMNW {
                    mask: LVCF_TEXT | LVCF_WIDTH,
                    cx: layout::scale(*width, dpi),
                    pszText: PWSTR(t.as_mut_ptr()),
                    ..Default::default()
                };
                SendMessageW(
                    preview,
                    LVM_INSERTCOLUMNW,
                    Some(WPARAM(i)),
                    Some(LPARAM(&col as *const _ as isize)),
                );
            }
        }
        let job = ui::label(window, "");
        let target = ui::path_label(window, "");
        let preview_button = ui::button(window, "Preview", ids::PREVIEW);
        let apply = ui::button(window, "Apply", ids::APPLY);
        let cancel = ui::button(window, "Cancel", ids::CANCEL);
        let close = ui::button(window, "Close", ids::CLOSE);
        partition_bar::set_options(current, 1.0, false);
        partition_bar::set_options(after, 1.0, true);
        let me = Rc::new_cyclic(|me: &Weak<ResizeWindow>| ResizeWindow {
            ctrl: RefCell::new(ResizeController::new(notify)),
            host,
            sync: SyncGuard::default(),
            me: me.clone(),
            window,
            warn,
            risk,
            disk_line,
            current_caption,
            current,
            after_caption,
            after,
            sizes_head,
            heads,
            cells: RefCell::default(),
            shape: RefCell::default(),
            error,
            preview_head,
            preview,
            shown_preview: RefCell::default(),
            job,
            target,
            preview_button,
            apply,
            cancel,
            close,
            changed: Cell::new(false),
        });
        let weak: Weak<dyn Handler> = me.me.clone();
        panel::register(window, weak);
        me.apply(ResizeAction::Open(Box::new(plan)));
        // SAFETY: showing the window we created.
        unsafe {
            let _ = ShowWindow(window, SW_SHOW);
        }
        me
    }

    pub fn apply(&self, action: ResizeAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    pub fn tick(&self) {
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    /// A refreshed device list: the device being resized must still be in it.
    pub fn set_devices(&self, devices: &[DiskDevice]) {
        self.ctrl.borrow_mut().reconcile_devices(devices);
    }

    pub fn is_open(&self) -> bool {
        self.ctrl.borrow().is_open()
    }

    pub fn is_running(&self) -> bool {
        self.ctrl.borrow().is_running()
    }

    /// A write ended since the last call, so the host reads the table again.
    pub fn take_changed(&self) -> bool {
        self.changed.replace(false)
    }

    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            let Some(d) = me.upgrade() else { return };
            let action = match reply {
                Reply::Confirmed { purpose, yes } => ResizeAction::Confirmed { purpose, yes },
                // Resize asks for no file, text or choice, and has no screen to leave.
                Reply::Picked { .. }
                | Reply::Answered { .. }
                | Reply::Chose { .. }
                | Reply::CloseScreen => return,
            };
            d.apply(action);
        })
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        let (log, changed) = {
            let mut c = self.ctrl.borrow_mut();
            (c.take_log(), c.take_disk_changed())
        };
        crate::app::append_log(log);
        if changed {
            self.changed.set(true);
        }
        if !self.ctrl.borrow().is_open() {
            panel::unregister(self.window);
            // SAFETY: the controller closed; the window goes with it.
            let _ = unsafe { DestroyWindow(self.window) };
            return;
        }
        self.sync.run(|| self.refresh());
    }

    fn refresh(&self) {
        let (
            device,
            risk,
            mut disk_line,
            bars,
            rows,
            error,
            preview,
            job,
            target,
            running,
            can_apply,
        ) = {
            let c = self.ctrl.borrow();
            (
                c.is_device(),
                c.device_risk_accepted(),
                format!("Disk size: {}", format_size(c.disk_size())),
                c.layout(),
                c.rows().to_vec(),
                c.plan_error().map(|e| format!("Error: {e}")),
                c.preview().map(<[PreviewRow]>::to_vec),
                c.progress(),
                c.write_target().map(|t| format!("Writes to: {t}")),
                c.is_running(),
                c.can_apply(),
            )
        };
        if self.ctrl.borrow().can_grow_container() {
            disk_line.push_str(" - to make more room, close this and use \"Expand Image...\"");
        }
        ui::set_visible(self.warn, device);
        ui::set_visible(self.risk, device);
        set_check(self.risk, risk);
        ui::set_enabled(self.risk, !running);
        ui::set_text(self.disk_line, &disk_line);
        let (current, after, scale) = &bars;
        let scale = (*scale).max(1) as f32;
        ui::set_text(self.current_caption, &current.caption);
        ui::set_text(self.after_caption, &after.caption);
        partition_bar::set_options(self.current, current.total_bytes() as f32 / scale, false);
        partition_bar::set_options(self.after, after.total_bytes() as f32 / scale, true);
        partition_bar::show(self.current, current.layout_segments(), None);
        partition_bar::show(self.after, after.layout_segments(), None);
        self.show_rows(&rows, running);
        ui::set_text(self.error, error.as_deref().unwrap_or(""));
        self.show_preview(preview);
        let job = job.map(|j| {
            if j.phase.is_empty() {
                j.label
            } else {
                format!("{}: {}", j.label, j.phase)
            }
        });
        ui::set_text(self.job, job.as_deref().unwrap_or(""));
        ui::set_visible(self.cancel, running);
        ui::set_text(self.target, target.as_deref().unwrap_or(""));
        ui::set_enabled(self.preview_button, !running);
        ui::set_enabled(self.apply, can_apply);
        ui::set_enabled(self.close, !running);
    }

    fn show_rows(&self, rows: &[ResizeRow], running: bool) {
        let shape: Vec<(usize, bool, Vec<SizeMode>)> = rows
            .iter()
            .map(|r| (r.index, r.extended, r.mode_choices()))
            .collect();
        if *self.shape.borrow() != shape {
            self.build_rows(rows);
            *self.shape.borrow_mut() = shape;
            self.layout();
        }
        // SAFETY: a plain focus query: the field being typed in keeps the user's text.
        let focus = unsafe { GetFocus() };
        for (row, c) in rows.iter().zip(self.cells.borrow().iter()) {
            let at = c.choices.iter().position(|m| *m == row.mode).unwrap_or(0);
            // SAFETY: plain combo box messages.
            unsafe {
                if SendMessageW(c.mode, CB_GETCURSEL, None, None).0 as usize != at {
                    SendMessageW(c.mode, CB_SETCURSEL, Some(WPARAM(at)), None);
                }
            }
            ui::set_enabled(c.mode, !running && !row.extended);
            if c.size != focus {
                ui::set_text(c.size, &row.text);
            }
            ui::set_enabled(
                c.size,
                !running && !row.extended && row.mode == SizeMode::Custom,
            );
        }
    }

    fn build_rows(&self, rows: &[ResizeRow]) {
        for c in self.cells.borrow_mut().drain(..) {
            for h in c.labels.iter().chain([&c.mode, &c.size]) {
                // SAFETY: our own controls, rebuilt for the new rows.
                let _ = unsafe { DestroyWindow(*h) };
            }
        }
        let w = self.window;
        let mut cells = Vec::new();
        for row in rows {
            let labels = [
                row.index.to_string(),
                row.type_name.clone(),
                format_size(row.original_size),
                format_size(row.minimum_size),
            ]
            .map(|t| ui::cell_label(w, &t));
            let style = WINDOW_STYLE((CBS_DROPDOWNLIST as u32) | WS_TABSTOP.0 | WS_VSCROLL.0);
            let mode = ui::child(
                w,
                w!("COMBOBOX"),
                "",
                style,
                WINDOW_EX_STYLE(0),
                ids::SIZE_MODE,
            );
            let choices = row.mode_choices();
            for m in &choices {
                let t = ui::wide(m.label());
                // SAFETY: the combo copies the string.
                unsafe {
                    SendMessageW(mode, CB_ADDSTRING, None, Some(LPARAM(t.as_ptr() as isize)));
                }
            }
            let size = ui::edit(w, ids::NEW_SIZE);
            cells.push(Cells {
                index: row.index,
                labels,
                mode,
                choices,
                size,
            });
        }
        *self.cells.borrow_mut() = cells;
    }

    fn show_preview(&self, preview: Option<Vec<PreviewRow>>) {
        ui::set_visible(self.preview_head, preview.is_some());
        ui::set_visible(self.preview, preview.is_some());
        if *self.shown_preview.borrow() == preview {
            return;
        }
        // SAFETY: plain list-view messages; each text outlives its message.
        unsafe {
            SendMessageW(self.preview, LVM_DELETEALLITEMS, None, None);
            for (n, r) in preview.iter().flatten().enumerate() {
                let index = r.index.to_string();
                let texts = [
                    &index,
                    &r.old_start,
                    &r.old_size,
                    &r.new_start,
                    &r.new_size,
                    &r.action,
                ];
                for (col, text) in texts.iter().enumerate() {
                    let mut t = ui::wide(text);
                    let item = LVITEMW {
                        mask: LVIF_TEXT,
                        iItem: n as i32,
                        iSubItem: col as i32,
                        pszText: PWSTR(t.as_mut_ptr()),
                        ..Default::default()
                    };
                    let msg = if col == 0 {
                        LVM_INSERTITEMW
                    } else {
                        LVM_SETITEMTEXTW
                    };
                    let wp = if col == 0 { 0 } else { n };
                    SendMessageW(
                        self.preview,
                        msg,
                        Some(WPARAM(wp)),
                        Some(LPARAM(&item as *const _ as isize)),
                    );
                }
            }
        }
        *self.shown_preview.borrow_mut() = preview;
        self.layout();
    }

    fn layout(&self) {
        let (area, dpi) = panel::client(self.window);
        let line =
            |h: HWND, n: i32| Node::leaf(h, Size::Stretch(1), Size::Fixed((layout::ROW - 8) * n));
        let grid = |hs: [HWND; 6]| {
            let widths = [40, 160, 110, 110, 130];
            let mut row: Vec<Node<HWND>> = hs[..5]
                .iter()
                .zip(widths)
                .map(|(h, w)| Node::leaf(*h, Size::Fixed(w), Size::Fixed(layout::ROW)))
                .collect();
            row.push(Node::leaf(
                hs[5],
                Size::Stretch(1),
                Size::Fixed(layout::ROW),
            ));
            Node::row(row)
        };
        let mut rows = Vec::new();
        if ui::shown(self.warn) {
            rows.push(line(self.warn, 2));
            rows.push(Node::leaf(
                self.risk,
                Size::Stretch(1),
                Size::Fixed(layout::ROW),
            ));
        }
        rows.push(line(self.disk_line, 1));
        rows.push(line(self.current_caption, 1));
        rows.push(Node::leaf(
            self.current,
            Size::Stretch(1),
            Size::Fixed(partition_bar::BAR_ONLY),
        ));
        rows.push(line(self.after_caption, 1));
        rows.push(Node::leaf(
            self.after,
            Size::Stretch(1),
            Size::Fixed(partition_bar::HEIGHT),
        ));
        rows.push(line(self.sizes_head, 1));
        rows.push(grid(self.heads));
        for c in self.cells.borrow().iter() {
            rows.push(grid([
                c.labels[0],
                c.labels[1],
                c.labels[2],
                c.labels[3],
                c.mode,
                c.size,
            ]));
        }
        rows.push(line(self.error, 1));
        if ui::shown(self.preview) {
            rows.push(line(self.preview_head, 1));
            rows.push(Node::leaf(self.preview, Size::Stretch(1), Size::Stretch(1)));
        } else {
            rows.push(Node::Space(Size::Stretch(1)));
        }
        rows.push(line(self.job, 1));
        rows.push(line(self.target, 1));
        rows.push(Node::row(vec![
            Node::button(self.preview_button, 90),
            Node::Space(Size::Stretch(1)),
            Node::button(self.cancel, 90),
            Node::button(self.apply, 90),
            Node::button(self.close, 90),
        ]));
        let tree = Node::inset(Node::column(rows), layout::MARGIN);
        for (h, mut r) in layout::layout(&tree, area, dpi) {
            if self.cells.borrow().iter().any(|c| c.mode == h) {
                // A drop-down list's window height includes its open list.
                r.h *= 8;
            }
            ui::move_to(h, r);
        }
    }
}

fn set_check(h: HWND, on: bool) {
    // SAFETY: plain button messages.
    unsafe {
        let now = SendMessageW(h, BM_GETCHECK, None, None).0 == 1;
        if now != on {
            SendMessageW(h, BM_SETCHECK, Some(WPARAM(on as usize)), None);
        }
    }
}

impl Handler for ResizeWindow {
    fn command(&self, from: HWND, code: u32) {
        if self.sync.active() {
            return;
        }
        let action = if code == BN_CLICKED && from == self.risk {
            // SAFETY: a plain button query.
            let on = unsafe { SendMessageW(from, BM_GETCHECK, None, None).0 } == 1;
            ResizeAction::AcceptDeviceRisk(on)
        } else if code == BN_CLICKED && from == self.preview_button {
            ResizeAction::Preview
        } else if code == BN_CLICKED && from == self.apply {
            // Apply asks the controller again at the click; the question is its own.
            if !self.ctrl.borrow().can_apply() {
                return;
            }
            ResizeAction::Apply
        } else if code == BN_CLICKED && from == self.cancel {
            ResizeAction::Cancel
        } else if code == BN_CLICKED && from == self.close {
            ResizeAction::Close
        } else {
            let cells = self.cells.borrow();
            let Some(c) = cells.iter().find(|c| c.mode == from || c.size == from) else {
                return;
            };
            if code == CBN_SELCHANGE && from == c.mode {
                // SAFETY: a plain combo box query.
                let i = unsafe { SendMessageW(c.mode, CB_GETCURSEL, None, None).0 };
                let Some(mode) = usize::try_from(i)
                    .ok()
                    .and_then(|i| c.choices.get(i).copied())
                else {
                    return;
                };
                ResizeAction::SetMode {
                    index: c.index,
                    mode,
                }
            } else if code == EN_CHANGE && from == c.size {
                ResizeAction::SetSizeText {
                    index: c.index,
                    text: ui::text(c.size),
                }
            } else {
                return;
            }
        };
        self.apply(action);
    }

    fn notify(&self, _hdr: &NMHDR, _lp: LPARAM) -> Option<LRESULT> {
        None
    }

    fn resized(&self) {
        self.layout();
    }

    fn close(&self) {
        if !self.ctrl.borrow().is_running() {
            self.apply(ResizeAction::Close);
        }
    }
}
