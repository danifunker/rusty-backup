//! Resize Partitions over `controller::resize`; `src/gui/resize_popup.rs` is the reference.

use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};
use std::sync::Arc;

use adw::prelude::*;
use rusty_backup::controller::app::{DiskDevice, Notify, Snapshot};
use rusty_backup::controller::partition_edit::LayoutBar;
use rusty_backup::controller::resize::{
    format_size, ids, PreviewRow, ResizeAction, ResizeController, ResizePlan, ResizeRow, SizeMode,
};
use rusty_backup::controller::{Controller, Effect};

use super::{button_row, window, CaptionedBar};
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::{button, cell, clear_grid, grid, head, set_line};
use crate::progress::JobList;
use crate::sync::SyncGuard;
use crate::widgets::{plain, set_text};

/// Everything a refresh shows, read while the controller is borrowed.
struct Shown {
    open: bool,
    device: bool,
    risk: bool,
    disk_line: String,
    bars: (LayoutBar, LayoutBar, u64),
    rows: Vec<ResizeRow>,
    error: Option<String>,
    preview: Option<Vec<PreviewRow>>,
    job: Option<Snapshot>,
    target: Option<String>,
    running: bool,
    can_apply: bool,
}

/// One size row's live widgets.
struct RowCells {
    mode: gtk::DropDown,
    choices: Vec<SizeMode>,
    size: gtk::Entry,
}

pub struct ResizeDialog {
    ctrl: RefCell<ResizeController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<ResizeDialog>,
    dialog: adw::Dialog,
    risk_box: gtk::Box,
    risk: gtk::CheckButton,
    disk_line: gtk::Label,
    current: CaptionedBar,
    after: CaptionedBar,
    rows: gtk::Grid,
    cells: RefCell<Vec<RowCells>>,
    /// The rows the cells were built for, minus their typed text.
    shape: RefCell<Vec<(usize, bool, Vec<SizeMode>)>>,
    error: gtk::Label,
    preview_head: gtk::Label,
    preview: gtk::Grid,
    shown_preview: RefCell<Option<Vec<PreviewRow>>>,
    jobs: JobList,
    target: gtk::Label,
    preview_button: gtk::Button,
    apply: gtk::Button,
    close: gtk::Button,
    changed: Cell<bool>,
}

fn label(text: &str) -> gtk::Label {
    gtk::Label::builder()
        .label(plain(text).as_ref())
        .xalign(0.0)
        .build()
}

impl ResizeDialog {
    pub fn open(notify: Arc<dyn Notify>, host: EffectHost, plan: ResizePlan) -> Rc<ResizeDialog> {
        let screen = Rc::new_cyclic(|me: &Weak<ResizeDialog>| {
            let send = {
                let me = me.clone();
                move |action: ResizeAction| {
                    if let Some(d) = me.upgrade() {
                        if !d.sync.active() {
                            d.apply(action);
                        }
                    }
                }
            };
            let send: Rc<dyn Fn(ResizeAction)> = Rc::new(send);

            let warn = label(
                "WARNING: This will directly modify the physical device. Data loss is permanent and irreversible!",
            );
            warn.set_wrap(true);
            warn.add_css_class("error");
            let risk = gtk::CheckButton::with_label("I understand the risks and want to proceed");
            risk.set_widget_name(ids::DEVICE_RISK);
            let s = send.clone();
            risk.connect_toggled(move |b| s(ResizeAction::AcceptDeviceRisk(b.is_active())));
            let risk_box = gtk::Box::new(gtk::Orientation::Vertical, 4);
            risk_box.append(&warn);
            risk_box.append(&risk);

            let disk_line = label("");
            disk_line.set_wrap(true);
            disk_line.add_css_class("dim-label");
            let current = CaptionedBar::new(false);
            let after = CaptionedBar::new(true);
            let sizes = label("Partition Sizes:");
            sizes.add_css_class("heading");
            let rows = grid();
            rows.set_widget_name(ids::SIZE_MODE);
            let error = label("");
            error.set_wrap(true);
            error.add_css_class("error");
            let preview_head = label("Preview:");
            preview_head.add_css_class("heading");
            let preview = grid();
            preview.set_widget_name(ids::PREVIEW);
            let s = send.clone();
            let jobs = JobList::new(ids::CANCEL, Rc::new(move || s(ResizeAction::Cancel)));
            let target = label("");
            target.set_wrap(true);
            target.add_css_class("heading");

            let preview_button = button("Preview", ids::PREVIEW);
            let s = send.clone();
            preview_button.connect_clicked(move |_| s(ResizeAction::Preview));
            let apply = button("Apply", ids::APPLY);
            apply.add_css_class("destructive-action");
            let d = me.clone();
            apply.connect_clicked(move |_| {
                if let Some(d) = d.upgrade() {
                    d.start();
                }
            });
            let close = button("Close", ids::CLOSE);
            let s = send.clone();
            close.connect_clicked(move |_| s(ResizeAction::Close));

            let body = crate::browser::dialogs::page();
            body.append(&risk_box);
            body.append(&disk_line);
            body.append(current.root());
            body.append(after.root());
            body.append(&sizes);
            body.append(&rows);
            body.append(&error);
            body.append(&preview_head);
            body.append(&preview);
            body.append(jobs.root());
            body.append(&target);
            body.append(&button_row(&[&preview_button, &apply, &close]));
            let s = send.clone();
            let dialog = window(
                "Resize Partitions",
                720,
                640,
                &body,
                Rc::new(move || s(ResizeAction::Close)),
            );

            ResizeDialog {
                ctrl: RefCell::new(ResizeController::new(notify)),
                host,
                sync: SyncGuard::default(),
                me: me.clone(),
                dialog,
                risk_box,
                risk,
                disk_line,
                current,
                after,
                rows,
                cells: RefCell::default(),
                shape: RefCell::default(),
                error,
                preview_head,
                preview,
                shown_preview: RefCell::default(),
                jobs,
                target,
                preview_button,
                apply,
                close,
                changed: Cell::new(false),
            }
        });
        screen.apply(ResizeAction::Open(Box::new(plan)));
        screen.host.present(&screen.dialog);
        screen
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

    /// Apply asks the controller again at the click; the question is its own.
    fn start(&self) {
        if self.ctrl.borrow().can_apply() && !self.host.dialog_over(&self.dialog) {
            self.apply(ResizeAction::Apply);
        }
    }

    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            let Some(d) = me.upgrade() else {
                return;
            };
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
            let mut ctrl = self.ctrl.borrow_mut();
            (ctrl.take_log(), ctrl.take_disk_changed())
        };
        if changed {
            self.changed.set(true);
        }
        crate::app::append_log(log);
        let shown = self.read();
        if !shown.open {
            self.dialog.force_close();
            return;
        }
        self.sync.run(|| self.show(shown));
    }

    fn read(&self) -> Shown {
        let ctrl = self.ctrl.borrow();
        let mut disk_line = format!("Disk size: {}", format_size(ctrl.disk_size()));
        if ctrl.can_grow_container() {
            disk_line.push_str(" - to make more room, close this and use \"Expand Image...\"");
        }
        Shown {
            open: ctrl.is_open(),
            device: ctrl.is_device(),
            risk: ctrl.device_risk_accepted(),
            disk_line,
            bars: ctrl.layout(),
            rows: ctrl.rows().to_vec(),
            error: ctrl.plan_error().map(|e| format!("Error: {e}")),
            preview: ctrl.preview().map(<[PreviewRow]>::to_vec),
            job: ctrl.progress(),
            target: ctrl.write_target().map(|t| format!("Writes to: {t}")),
            running: ctrl.is_running(),
            can_apply: ctrl.can_apply(),
        }
    }

    fn show(&self, s: Shown) {
        self.risk_box.set_visible(s.device);
        if self.risk.is_active() != s.risk {
            self.risk.set_active(s.risk);
        }
        self.risk.set_sensitive(!s.running);
        set_text(&self.disk_line, &s.disk_line);
        let (current, after, scale) = &s.bars;
        let total = |b: &LayoutBar| b.segments.iter().map(|x| x.size_bytes).sum::<u64>() as f64;
        let scale = (*scale).max(1) as f64;
        self.current.show(current, (total(current) / scale) as f32);
        self.after.show(after, (total(after) / scale) as f32);
        self.show_rows(&s.rows, s.running);
        set_line(&self.error, s.error.as_deref());
        self.show_preview(s.preview);
        let jobs: Vec<(Snapshot, bool)> = s.job.into_iter().map(|j| (j, true)).collect();
        self.jobs.show(&jobs);
        set_line(&self.target, s.target.as_deref());
        self.preview_button.set_sensitive(!s.running);
        self.apply.set_sensitive(s.can_apply);
        self.close.set_sensitive(!s.running);
    }

    fn show_rows(&self, rows: &[ResizeRow], running: bool) {
        let shape: Vec<(usize, bool, Vec<SizeMode>)> = rows
            .iter()
            .map(|r| (r.index, r.extended, r.mode_choices()))
            .collect();
        if *self.shape.borrow() != shape {
            self.build_rows(rows);
            *self.shape.borrow_mut() = shape;
        }
        let cells = self.cells.borrow();
        for (row, c) in rows.iter().zip(cells.iter()) {
            let at = c.choices.iter().position(|m| *m == row.mode).unwrap_or(0) as u32;
            if c.mode.selected() != at {
                c.mode.set_selected(at);
            }
            c.mode.set_sensitive(!running && !row.extended);
            let text = plain(&row.text);
            if c.size.text() != text.as_ref() && !c.size.has_focus() {
                c.size.set_text(&text);
            }
            c.size
                .set_sensitive(!running && !row.extended && row.mode == SizeMode::Custom);
        }
    }

    fn build_rows(&self, rows: &[ResizeRow]) {
        clear_grid(&self.rows);
        for (i, title) in [
            "#",
            "Type",
            "Current Size",
            "Min Size",
            "Size Mode",
            "New Size (MiB)",
        ]
        .iter()
        .enumerate()
        {
            head(&self.rows, i as i32, title, None);
        }
        let mut cells = Vec::new();
        for (n, row) in rows.iter().enumerate() {
            let top = n as i32 + 1;
            let number = cell(0.0);
            number.set_label(&row.index.to_string());
            let kind = cell(0.0);
            let name = if row.extended {
                format!("{} (extended)", row.type_name)
            } else {
                row.type_name.clone()
            };
            kind.set_label(&plain(&name));
            let size = cell(1.0);
            size.set_label(&format_size(row.original_size));
            let min = cell(1.0);
            min.set_label(&if row.minimum_size > 0 && !row.extended {
                format_size(row.minimum_size)
            } else {
                "-".to_string()
            });
            for (col, l) in [&number, &kind, &size, &min].into_iter().enumerate() {
                if row.extended {
                    l.add_css_class("dim-label");
                }
                self.rows.attach(l, col as i32, top, 1, 1);
            }
            let choices = row.mode_choices();
            let labels: Vec<&str> = choices.iter().map(|m| m.label()).collect();
            let mode = gtk::DropDown::from_strings(&labels);
            mode.set_widget_name(&format!("{}.{}", ids::SIZE_MODE, row.index));
            let entry = gtk::Entry::builder().width_chars(10).build();
            entry.set_widget_name(&format!("{}.{}", ids::NEW_SIZE, row.index));
            let index = row.index;
            let held = choices.clone();
            let me = self.me.clone();
            mode.connect_selected_notify(move |d| {
                let picked = held.get(d.selected() as usize).copied();
                if let (Some(mode), Some(s)) = (picked, me.upgrade()) {
                    if !s.sync.active() {
                        s.apply(ResizeAction::SetMode { index, mode });
                    }
                }
            });
            let me = self.me.clone();
            entry.connect_changed(move |e| {
                if let Some(s) = me.upgrade() {
                    if !s.sync.active() {
                        let text = e.text().to_string();
                        s.apply(ResizeAction::SetSizeText { index, text });
                    }
                }
            });
            self.rows.attach(&mode, 4, top, 1, 1);
            self.rows.attach(&entry, 5, top, 1, 1);
            cells.push(RowCells {
                mode,
                choices,
                size: entry,
            });
        }
        *self.cells.borrow_mut() = cells;
    }

    fn show_preview(&self, preview: Option<Vec<PreviewRow>>) {
        self.preview_head.set_visible(preview.is_some());
        self.preview.set_visible(preview.is_some());
        if *self.shown_preview.borrow() == preview {
            return;
        }
        clear_grid(&self.preview);
        if let Some(rows) = &preview {
            for (i, title) in [
                "#",
                "Old Start",
                "Old Size",
                "New Start",
                "New Size",
                "Action",
            ]
            .iter()
            .enumerate()
            {
                head(&self.preview, i as i32, title, None);
            }
            for (n, r) in rows.iter().enumerate() {
                let index = r.index.to_string();
                let texts = [
                    index.as_str(),
                    r.old_start.as_str(),
                    r.old_size.as_str(),
                    r.new_start.as_str(),
                    r.new_size.as_str(),
                    r.action.as_str(),
                ];
                for (col, text) in texts.into_iter().enumerate() {
                    let l = cell(0.0);
                    l.set_label(text);
                    self.preview.attach(&l, col as i32, n as i32 + 1, 1, 1);
                }
            }
        }
        *self.shown_preview.borrow_mut() = preview;
    }
}
