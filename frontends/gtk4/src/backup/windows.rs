//! Backup's two windows, each up while the controller says so: VHD options and Remote Drive.

use std::cell::RefCell;
use std::rc::Rc;

use adw::prelude::*;
use rusty_backup::controller::backup::{
    hints, ids, BackupAction, BackupController, RemoteDrive, VhdRow,
};

use super::ActionFn;
use crate::browser::dialogs::{page, window, Slot};
use crate::effects::EffectHost;
use crate::form::{button, cell, combo, grid, group, head, line, set_combo, set_entry, set_line};
use crate::size_row::{Deferred, SizeCell, SizeShown};
use crate::widgets::{plain, set_text};

const LAYOUTS: [&str; 2] = [
    "Whole Disk (single .vhd file)",
    "Per Partition (one .vhd per partition)",
];

/// What the VHD window shows.
pub struct VhdShown {
    whole_disk: bool,
    rows: Vec<(VhdRow, Option<String>)>,
}

impl VhdShown {
    pub fn read(ctrl: &BackupController) -> Option<VhdShown> {
        ctrl.vhd_open().then(|| VhdShown {
            whole_disk: ctrl.vhd_whole_disk(),
            rows: ctrl
                .vhd_rows()
                .iter()
                .map(|r| (r.clone(), ctrl.min_phase(r.index)))
                .collect(),
        })
    }
}

/// What the Remote Drive window shows.
pub struct RemoteShown {
    address: String,
    busy: bool,
    error: Option<String>,
    drives: Option<Vec<RemoteDrive>>,
}

impl RemoteShown {
    pub fn read(ctrl: &BackupController) -> Option<RemoteShown> {
        ctrl.remote_dialog_open().then(|| RemoteShown {
            address: ctrl.remote_address().to_string(),
            busy: ctrl.remote_busy(),
            error: ctrl.remote_error().map(str::to_string),
            drives: ctrl.remote_listed().then(|| ctrl.remote_drives()),
        })
    }
}

struct VhdWindow {
    layout: adw::ComboRow,
    grid: gtk::Grid,
    cells: Vec<(usize, SizeCell)>,
    start: gtk::Button,
}

impl VhdWindow {
    fn build(rows: &[(VhdRow, Option<String>)], send: &ActionFn) -> (VhdWindow, adw::Dialog) {
        let body = page();
        body.append(
            &gtk::Label::builder()
                .label("Configure VHD output.")
                .xalign(0.0)
                .build(),
        );
        let form = group("");
        let layout = combo("Layout", ids::VHD_LAYOUT, &LAYOUTS);
        let s = send.clone();
        layout.connect_selected_notify(move |r| s(BackupAction::VhdWholeDisk(r.selected() == 0)));
        form.add(&layout);
        body.append(&form);

        let table = grid();
        head(&table, 0, "#", None);
        head(&table, 1, "Type", None);
        head(&table, 2, "Size Mode", None);
        head(&table, 3, "Size (MiB)", None);
        let mut cells = Vec::new();
        for (i, (row, _)) in rows.iter().enumerate() {
            let line = i as i32 + 1;
            let index = row.index;
            let number = cell(1.0);
            number.set_label(&index.to_string());
            let kind = cell(0.0);
            kind.set_label(&plain(&row.type_name));
            table.attach(&number, 0, line, 1, 1);
            table.attach(&kind, 1, line, 1, 1);
            let s = send.clone();
            let change = Rc::new(move |mode, custom_mib| {
                s(BackupAction::VhdSize {
                    index,
                    mode,
                    custom_mib,
                });
            });
            let s = send.clone();
            let calc = Rc::new(move || s(BackupAction::CalcMin(index)));
            let id = format!("{}.{index}", ids::VHD_SIZE);
            let size = SizeCell::new((&id, &format!("{id}.mib")), change, calc);
            size.attach(&table, 2, line);
            cells.push((index, size));
        }
        if !rows.is_empty() {
            body.append(&table);
        }

        let start = button("Start VHD Backup", ids::VHD_START);
        start.add_css_class("suggested-action");
        let s = send.clone();
        start.connect_clicked(move |_| s(BackupAction::VhdStart));
        let cancel = button("Cancel", ids::VHD_CANCEL);
        let s = send.clone();
        cancel.connect_clicked(move |_| s(BackupAction::VhdClose));
        let buttons = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        buttons.set_halign(gtk::Align::End);
        buttons.append(&cancel);
        buttons.append(&start);
        body.append(&buttons);
        let dialog = window("VHD Backup Options", 620, 420, &body);
        let shown = VhdWindow {
            layout,
            grid: table,
            cells,
            start,
        };
        (shown, dialog)
    }

    fn show(&self, s: &VhdShown, can_start: bool) {
        set_combo(&self.layout, u32::from(!s.whole_disk));
        self.grid.set_visible(!s.rows.is_empty());
        self.start.set_sensitive(can_start);
        for (index, size) in &self.cells {
            let Some((row, phase)) = s.rows.iter().find(|(r, _)| r.index == *index) else {
                continue;
            };
            let deferred = match (phase, row.deferred_fs) {
                (Some(phase), _) => Some(Deferred::Pending(phase.clone())),
                (None, Some(fs)) => Some(Deferred::Available(format!(
                    "Compute minimum size (walks the {fs} volume)"
                ))),
                (None, None) => None,
            };
            size.show(SizeShown {
                choices: row.mode_choices(),
                mode: row.mode,
                custom_mib: row.custom_mib,
                bounds: row.custom_bounds_mib(),
                effective: row.effective_size(),
                deferred,
            });
        }
    }
}

struct RemoteWindow {
    address: adw::EntryRow,
    connect: gtk::Button,
    spinner: gtk::Spinner,
    error: gtk::Label,
    list_note: gtk::Label,
    list: gtk::ListBox,
    drives: Rc<RefCell<Vec<RemoteDrive>>>,
}

impl RemoteWindow {
    fn build(send: &ActionFn) -> (RemoteWindow, adw::Dialog) {
        let body = page();
        let form = group("");
        form.set_description(Some(hints::REMOTE_DRIVE_INTRO));
        let address = adw::EntryRow::builder().title("Host").build();
        address.set_widget_name(ids::REMOTE_ADDRESS);
        address.set_tooltip_text(Some(hints::REMOTE_ADDRESS));
        let s = send.clone();
        address.connect_changed(move |e| s(BackupAction::RemoteSetAddress(e.text().to_string())));
        form.add(&address);
        body.append(&form);

        let connect = button("Connect", ids::REMOTE_CONNECT);
        let s = send.clone();
        connect.connect_clicked(move |_| s(BackupAction::RemoteConnect));
        let close = button("Close", ids::REMOTE_CLOSE);
        let s = send.clone();
        close.connect_clicked(move |_| s(BackupAction::RemoteClose));
        let spinner = gtk::Spinner::new();
        let buttons = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        buttons.append(&connect);
        buttons.append(&spinner);
        buttons.append(&close);
        body.append(&buttons);

        let error = line();
        error.add_css_class("error");
        body.append(&error);
        let list_note = line();
        body.append(&list_note);
        let list = gtk::ListBox::builder()
            .selection_mode(gtk::SelectionMode::None)
            .activate_on_single_click(true)
            .build();
        list.add_css_class("boxed-list");
        let drives: Rc<RefCell<Vec<RemoteDrive>>> = Rc::default();
        let held = drives.clone();
        let s = send.clone();
        list.connect_row_activated(move |_, row| {
            let drive = held.borrow().get(row.index() as usize).cloned();
            if let Some(d) = drive {
                s(BackupAction::RemotePick {
                    path: d.path,
                    size: d.size_bytes,
                });
            }
        });
        let scroll = gtk::ScrolledWindow::builder()
            .child(&list)
            .hscrollbar_policy(gtk::PolicyType::Never)
            .propagate_natural_height(true)
            .max_content_height(220)
            .build();
        body.append(&scroll);
        let dialog = window("Remote Drive", 460, 320, &body);
        let shown = RemoteWindow {
            address,
            connect,
            spinner,
            error,
            list_note,
            list,
            drives,
        };
        (shown, dialog)
    }

    fn show(&self, s: &RemoteShown) {
        set_entry(&self.address, &s.address);
        self.connect.set_sensitive(!s.busy);
        self.spinner.set_visible(s.busy);
        self.spinner.set_spinning(s.busy);
        set_line(&self.error, s.error.as_deref());
        let note = s.drives.as_ref().map(|d| match d.is_empty() {
            true => hints::NO_REMOTE_DRIVES,
            false => "Pick a drive to back up:",
        });
        set_line(&self.list_note, note);
        let drives = s.drives.clone().unwrap_or_default();
        if *self.drives.borrow() != drives {
            *self.drives.borrow_mut() = drives.clone();
            self.list.remove_all();
            for (i, d) in drives.iter().enumerate() {
                let label = gtk::Label::builder()
                    .xalign(0.0)
                    .wrap(true)
                    .margin_top(6)
                    .margin_bottom(6)
                    .margin_start(6)
                    .margin_end(6)
                    .build();
                set_text(&label, &d.label);
                let row = gtk::ListBoxRow::builder().child(&label).build();
                row.set_widget_name(&format!("{}.{i}", ids::REMOTE_DRIVE));
                self.list.append(&row);
            }
        }
    }
}

pub struct Windows {
    host: EffectHost,
    send: ActionFn,
    vhd_slot: Slot,
    vhd: RefCell<Option<VhdWindow>>,
    remote_slot: Slot,
    remote: RefCell<Option<RemoteWindow>>,
}

impl Windows {
    pub fn new(host: EffectHost, send: ActionFn) -> Windows {
        Windows {
            host,
            send,
            vhd_slot: Slot::default(),
            vhd: RefCell::default(),
            remote_slot: Slot::default(),
            remote: RefCell::default(),
        }
    }

    /// Run inside the screen's `SyncGuard`; `can_start` locks the VHD window's Start.
    pub fn show(&self, vhd: Option<VhdShown>, remote: Option<RemoteShown>, can_start: bool) {
        let s = self.send.clone();
        let close_vhd: Rc<dyn Fn(())> = Rc::new(move |()| s(BackupAction::VhdClose));
        let key = vhd.as_ref().map(|v| {
            let rows: Vec<String> = v.rows.iter().map(|(r, _)| r.index.to_string()).collect();
            format!("vhd:{}", rows.join(","))
        });
        self.vhd_slot
            .sync(&self.host, &close_vhd, key, Some(()), || {
                let rows = vhd.as_ref().map_or(&[][..], |v| v.rows.as_slice());
                let (shown, dialog) = VhdWindow::build(rows, &self.send);
                *self.vhd.borrow_mut() = Some(shown);
                dialog
            });
        if let (Some(v), Some(w)) = (&vhd, self.vhd.borrow().as_ref()) {
            w.show(v, can_start);
        }

        let s = self.send.clone();
        let close_remote: Rc<dyn Fn(())> = Rc::new(move |()| s(BackupAction::RemoteClose));
        let key = remote.as_ref().map(|_| "remote".to_string());
        self.remote_slot
            .sync(&self.host, &close_remote, key, Some(()), || {
                let (shown, dialog) = RemoteWindow::build(&self.send);
                *self.remote.borrow_mut() = Some(shown);
                dialog
            });
        if let (Some(r), Some(w)) = (&remote, self.remote.borrow().as_ref()) {
            w.show(r);
        }
    }
}
