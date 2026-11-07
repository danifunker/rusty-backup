//! The Physical Disk Export window over `controller::disk_write`, which Restore's Write Image File
//! opens; `src/gui/physical_disk_export.rs` is the reference. The name above Write is the
//! controller's `write_target_name()`, the name its question uses.

use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use adw::prelude::*;
use rusty_backup::controller::app::{DiskDevice, Notify, Snapshot};
use rusty_backup::controller::disk_write::{
    format_size, ids, DiskWriteAction, DiskWriteController, DiskWriteServices, JobRegistry,
    SizeMode, TableKind, TargetPartition, WrapAlign, WrapForm, WriteOpen, WriteSource,
    TYPE_BYTE_PRESETS,
};
use rusty_backup::controller::{Controller, Effect};

use super::{button_row, fact_line, radios, set_radio, window};
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::{button, set_line};
use crate::progress::JobList;
use crate::size_row::{SizeCell, SizeShown};
use crate::sync::SyncGuard;
use crate::widgets::{plain, set_text};

const ALIGNS: [WrapAlign; 3] = [
    WrapAlign::DosTraditional,
    WrapAlign::Modern1MB,
    WrapAlign::Custom,
];

struct Shown {
    open: bool,
    opening: Option<Snapshot>,
    source: Option<WriteSource>,
    has_run: bool,
    progress: Option<Snapshot>,
    writing: bool,
    result: Option<Result<u64, String>>,
    target_row: Option<usize>,
    target_image: Option<String>,
    device_size: Option<u64>,
    scan: Option<Snapshot>,
    parts: Vec<TargetPartition>,
    part: Option<usize>,
    warning: Option<String>,
    can_synth: bool,
    form: WrapForm,
    choices: [SizeMode; 3],
    bounds: (u32, u32),
    error: Option<String>,
    target: Option<String>,
    can_write: bool,
}

pub struct DiskWriteDialog {
    ctrl: RefCell<DiskWriteController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<DiskWriteDialog>,
    devices: RefCell<Vec<DiskDevice>>,
    dialog: RefCell<Option<adw::Dialog>>,
    opening: JobList,
    source: gtk::Label,
    form: gtk::Box,
    device: gtk::DropDown,
    device_names: RefCell<Vec<String>>,
    image: gtk::Label,
    scan: JobList,
    region: gtk::Box,
    region_list: gtk::Box,
    region_radios: RefCell<(Vec<Option<usize>>, Vec<gtk::CheckButton>)>,
    region_note: gtk::Label,
    warning: gtk::Label,
    synth: gtk::CheckButton,
    synth_form: gtk::Box,
    table: Vec<gtk::CheckButton>,
    align: Vec<gtk::CheckButton>,
    custom_align: gtk::SpinButton,
    type_pick: gtk::DropDown,
    type_hex: gtk::Entry,
    size: SizeCell,
    error: gtk::Label,
    run: gtk::Box,
    progress: JobList,
    result: gtk::Label,
    target: gtk::Label,
    write: gtk::Button,
    close: gtk::Button,
}

fn row(title: &str, widget: &impl IsA<gtk::Widget>) -> gtk::Box {
    let b = gtk::Box::new(gtk::Orientation::Horizontal, 8);
    b.append(&gtk::Label::new(Some(title)));
    b.append(widget);
    b
}

impl DiskWriteDialog {
    pub fn new(notify: Arc<dyn Notify>, host: EffectHost) -> Rc<Self> {
        Rc::new_cyclic(|me: &Weak<DiskWriteDialog>| {
            let send: Rc<dyn Fn(DiskWriteAction)> = {
                let me = me.clone();
                Rc::new(move |action| {
                    if let Some(d) = me.upgrade() {
                        if !d.sync.active() {
                            d.apply(action);
                        }
                    }
                })
            };
            let opening = JobList::new(ids::CLOSE, Rc::new(|| {}));
            let source_head = gtk::Label::builder().label("Source").xalign(0.0).build();
            source_head.add_css_class("heading");
            let source = fact_line();

            let device = gtk::DropDown::from_strings(&["Select a device..."]);
            device.set_widget_name(ids::TARGET_DEVICE);
            let me2 = me.clone();
            device.connect_selected_notify(move |dd| {
                let Some(d) = me2.upgrade() else {
                    return;
                };
                if d.sync.active() || dd.selected() == 0 {
                    return;
                }
                let row = dd.selected() as usize - 1;
                let device = d.devices.borrow().get(row).cloned();
                if let Some(device) = device {
                    d.apply(DiskWriteAction::SelectDevice { row, device });
                }
            });
            let pick_image = button("Image file...", ids::TARGET_IMAGE);
            let s = send.clone();
            pick_image.connect_clicked(move |_| s(DiskWriteAction::PickTargetImage));
            let target_row = row("Target disk:", &device);
            target_row.append(&pick_image);
            let image = fact_line();

            let scan = JobList::new(ids::CLOSE, Rc::new(|| {}));
            let region_head = gtk::Label::builder().label("Write to").xalign(0.0).build();
            region_head.add_css_class("heading");
            let region_list = gtk::Box::new(gtk::Orientation::Vertical, 4);
            region_list.set_widget_name(ids::TARGET_REGION);
            let region_note = fact_line();
            region_note.add_css_class("dim-label");
            region_note.set_label(
                "No partition table detected on the target, so only a whole-disk write is available.",
            );
            let warning = fact_line();
            warning.add_css_class("error");
            let region = gtk::Box::new(gtk::Orientation::Vertical, 4);
            region.append(&region_head);
            region.append(&region_list);
            region.append(&region_note);
            region.append(&warning);

            let synth = gtk::CheckButton::with_label("Synthesize partition table");
            synth.set_widget_name(ids::SYNTHESIZE);
            let s = send.clone();
            synth.connect_toggled(move |b| s(DiskWriteAction::SetSynthesize(b.is_active())));
            let (table_row, table) = radios(&["MBR", "GPT"]);
            table_row.set_widget_name(ids::TABLE_KIND);
            for (i, r) in table.iter().enumerate() {
                let s = send.clone();
                r.connect_toggled(move |r| {
                    if r.is_active() {
                        let kind = if i == 0 {
                            TableKind::Mbr
                        } else {
                            TableKind::Gpt
                        };
                        s(DiskWriteAction::SetTableKind(kind));
                    }
                });
            }
            let (align_row, align) =
                radios(&["DOS legacy (LBA 63)", "Modern (LBA 2048)", "Custom"]);
            align_row.set_widget_name(ids::ALIGNMENT);
            for (i, r) in align.iter().enumerate() {
                let s = send.clone();
                r.connect_toggled(move |r| {
                    if r.is_active() {
                        s(DiskWriteAction::SetAlignment(ALIGNS[i]));
                    }
                });
            }
            let custom_align = gtk::SpinButton::with_range(1.0, 1.0e12, 1.0);
            let s = send.clone();
            custom_align.connect_value_changed(move |sp| {
                s(DiskWriteAction::SetCustomAlignment(sp.value() as u64))
            });
            align_row.append(&custom_align);
            align_row.append(&gtk::Label::new(Some("sectors")));
            let labels: Vec<&str> = TYPE_BYTE_PRESETS.iter().map(|(_, n)| *n).collect();
            let type_pick = gtk::DropDown::from_strings(&labels);
            type_pick.set_widget_name(ids::TYPE_BYTE);
            let s = send.clone();
            type_pick.connect_selected_notify(move |dd| {
                if let Some((b, _)) = TYPE_BYTE_PRESETS.get(dd.selected() as usize) {
                    s(DiskWriteAction::SetTypeByte(*b));
                }
            });
            let type_hex = gtk::Entry::builder().width_chars(4).max_length(2).build();
            type_hex.set_tooltip_text(Some("Any other type byte, in hex"));
            let s = send.clone();
            type_hex.connect_changed(move |e| {
                if let Ok(b) = u8::from_str_radix(e.text().trim(), 16) {
                    s(DiskWriteAction::SetTypeByte(b));
                }
            });
            let type_row = row("Partition type:", &type_pick);
            type_row.append(&gtk::Label::new(Some("or 0x")));
            type_row.append(&type_hex);
            let s = send.clone();
            let size = SizeCell::new(
                (ids::PARTITION_SIZE, ids::PARTITION_SIZE),
                Rc::new(move |mode, custom_mib| s(DiskWriteAction::SetSize { mode, custom_mib })),
                Rc::new(|| {}),
            );
            let size_grid = gtk::Grid::builder().column_spacing(8).build();
            size_grid.attach(&gtk::Label::new(Some("Partition size:")), 0, 0, 1, 1);
            size.attach(&size_grid, 1, 0);
            let synth_form = gtk::Box::new(gtk::Orientation::Vertical, 6);
            synth_form.set_margin_start(24);
            synth_form.append(&row("Table:", &table_row));
            synth_form.append(&row("Alignment:", &align_row));
            synth_form.append(&type_row);
            synth_form.append(&size_grid);

            let error = fact_line();
            error.add_css_class("error");
            let form = gtk::Box::new(gtk::Orientation::Vertical, 8);
            form.append(&target_row);
            form.append(&image);
            form.append(scan.root());
            form.append(&region);
            form.append(&gtk::Separator::new(gtk::Orientation::Horizontal));
            form.append(&synth);
            form.append(&synth_form);
            form.append(&error);

            let s = send.clone();
            let progress = JobList::new(
                ids::CANCEL_WRITE,
                Rc::new(move || s(DiskWriteAction::Cancel)),
            );
            let result = fact_line();
            let run = gtk::Box::new(gtk::Orientation::Vertical, 6);
            run.append(progress.root());
            run.append(&result);

            let target = fact_line();
            target.add_css_class("heading");
            let write = button("Write to disk...", ids::WRITE);
            write.add_css_class("destructive-action");
            let d = me.clone();
            write.connect_clicked(move |_| {
                if let Some(d) = d.upgrade() {
                    d.start();
                }
            });
            let close = button("Cancel", ids::CLOSE);
            let s = send.clone();
            close.connect_clicked(move |_| s(DiskWriteAction::Close));

            let body = crate::browser::dialogs::page();
            body.append(opening.root());
            body.append(&source_head);
            body.append(&source);
            body.append(&gtk::Separator::new(gtk::Orientation::Horizontal));
            body.append(&form);
            body.append(&run);
            body.append(&target);
            body.append(&button_row(&[&write, &close]));
            let s = send.clone();
            let dialog = window(
                "Physical Disk Export",
                620,
                640,
                &body,
                Rc::new(move || s(DiskWriteAction::Close)),
            );
            dialog.set_widget_name("disk_write");
            DiskWriteDialog {
                ctrl: RefCell::new(DiskWriteController::new(DiskWriteServices {
                    jobs: JobRegistry::new(),
                    notify,
                })),
                host,
                sync: SyncGuard::default(),
                me: me.clone(),
                devices: RefCell::default(),
                dialog: RefCell::new(Some(dialog)),
                opening,
                source,
                form,
                device,
                device_names: RefCell::default(),
                image,
                scan,
                region,
                region_list,
                region_radios: RefCell::default(),
                region_note,
                warning,
                synth,
                synth_form,
                table,
                align,
                custom_align,
                type_pick,
                type_hex,
                size,
                error,
                run,
                progress,
                result,
                target,
                write,
                close,
            }
        })
    }

    /// Open the window on an image another screen hands over.
    pub fn open(&self, open: WriteOpen) {
        let dialog = self.dialog.borrow().clone();
        if let Some(dialog) = dialog.filter(|d| d.parent().is_none()) {
            self.host.present(&dialog);
        }
        self.apply(DiskWriteAction::Open(open));
    }

    pub fn apply(&self, action: DiskWriteAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    pub fn tick(&self) {
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    /// The app's device list: the target follows its own disk, and never another one.
    pub fn set_devices(&self, devices: &[DiskDevice]) {
        self.ctrl.borrow_mut().reconcile_devices(devices);
        *self.devices.borrow_mut() = devices.to_vec();
    }

    pub fn is_writing(&self) -> bool {
        self.ctrl.borrow().is_writing()
    }

    /// Write asks the controller again at the click; the question is its own.
    fn start(&self) {
        let covered = self
            .dialog
            .borrow()
            .as_ref()
            .is_some_and(|d| self.host.dialog_over(d));
        if self.ctrl.borrow().can_write() && !covered {
            self.apply(DiskWriteAction::Write);
        }
    }

    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            let Some(d) = me.upgrade() else {
                return;
            };
            let action = match reply {
                Reply::Picked { purpose, path } => DiskWriteAction::Picked { purpose, path },
                Reply::Confirmed { purpose, yes } => DiskWriteAction::Confirmed { purpose, yes },
                Reply::Answered { purpose, text } => DiskWriteAction::Answered { purpose, text },
                // The writer offers no choice and has no screen to leave.
                Reply::Chose { .. } | Reply::CloseScreen => return,
            };
            d.apply(action);
        })
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        let log = self.ctrl.borrow_mut().take_log();
        crate::app::append_log(log);
        let shown = self.read();
        let dialog = self.dialog.borrow().clone();
        let Some(dialog) = dialog else {
            return;
        };
        if !shown.open {
            if dialog.parent().is_some() {
                dialog.force_close();
            }
            return;
        }
        if dialog.parent().is_some() {
            self.sync.run(|| self.show(shown));
        }
    }

    fn read(&self) -> Shown {
        let ctrl = self.ctrl.borrow();
        Shown {
            open: ctrl.is_open(),
            opening: ctrl.opening(),
            source: ctrl.source().cloned(),
            has_run: ctrl.has_run(),
            progress: ctrl.progress(),
            writing: ctrl.is_writing(),
            result: ctrl.result().cloned(),
            target_row: ctrl.target_row(),
            target_image: ctrl.target_image().map(|p| p.display().to_string()),
            device_size: ctrl.target_device().map(|d| d.size_bytes),
            scan: ctrl.target_scan(),
            parts: ctrl.target_partitions(),
            part: ctrl.target_partition(),
            warning: ctrl.partition_warning(),
            can_synth: ctrl.can_synthesize(),
            form: *ctrl.form(),
            choices: ctrl.size_choices(),
            bounds: ctrl.custom_bounds_mib(),
            error: ctrl.error().map(str::to_string),
            target: ctrl.write_target_name().map(|t| format!("Writes to: {t}")),
            can_write: ctrl.can_write(),
        }
    }

    fn show(&self, s: Shown) {
        let jobs: Vec<(Snapshot, bool)> = s.opening.iter().map(|j| (j.clone(), false)).collect();
        self.opening.show(&jobs);
        let source = s.source.as_ref().map(|src| {
            let table = if src.has_partition_table {
                "has partition table"
            } else {
                "superfloppy (no partition table)"
            };
            format!(
                "Path: {}\nSize: {} bytes  -  Filesystem: {}  -  {table}",
                src.path.display(),
                src.size_bytes,
                src.fs_hint
            )
        });
        set_text(&self.source, source.as_deref().unwrap_or(""));
        let ready = s.opening.is_none();
        self.form.set_visible(ready && !s.has_run);
        self.run.set_visible(s.has_run);
        self.show_devices(s.target_row);
        let image = s
            .target_image
            .as_ref()
            .map(|p| format!("Target image: {p}"));
        set_line(&self.image, image.as_deref());
        let scans: Vec<(Snapshot, bool)> = s.scan.iter().map(|j| (j.clone(), false)).collect();
        self.scan.show(&scans);
        self.region
            .set_visible(s.device_size.is_some() && s.scan.is_none());
        if let Some(size) = s.device_size {
            self.show_region(size, &s.parts, s.part);
        }
        self.region_note.set_visible(s.parts.is_empty());
        set_line(&self.warning, s.warning.as_deref());
        self.show_synth(&s);
        set_line(&self.error, s.error.as_deref());

        let jobs: Vec<(Snapshot, bool)> =
            s.progress.iter().map(|j| (j.clone(), s.writing)).collect();
        self.progress.show(&jobs);
        self.result.remove_css_class("error");
        let result = match &s.result {
            Some(Ok(_)) => Some("Write complete.".to_string()),
            Some(Err(e)) => {
                self.result.add_css_class("error");
                Some(format!("Error: {e}"))
            }
            None => None,
        };
        set_line(&self.result, result.as_deref());
        set_line(&self.target, s.target.as_deref());
        self.write.set_visible(!s.has_run);
        self.write.set_sensitive(s.can_write);
        self.close.set_sensitive(!s.writing);
        let label = if s.has_run { "Close" } else { "Cancel" };
        if self.close.label().as_deref() != Some(label) {
            self.close.set_label(label);
        }
    }

    /// The device list, rebuilt only when its names change; row 0 is "Select a device...".
    fn show_devices(&self, selected: Option<usize>) {
        let names: Vec<String> = self
            .devices
            .borrow()
            .iter()
            .map(|d| d.display_name())
            .collect();
        if *self.device_names.borrow() != names {
            let mut labels = vec!["Select a device...".to_string()];
            labels.extend(names.iter().map(|n| plain(n).into_owned()));
            let refs: Vec<&str> = labels.iter().map(String::as_str).collect();
            self.device.set_model(Some(&gtk::StringList::new(&refs)));
            *self.device_names.borrow_mut() = names;
        }
        let at = selected.map_or(0, |r| r as u32 + 1);
        if self.device.selected() != at {
            self.device.set_selected(at);
        }
    }

    fn show_region(&self, size: u64, parts: &[TargetPartition], part: Option<usize>) {
        let mut keys: Vec<Option<usize>> = vec![None];
        keys.extend(parts.iter().map(|p| Some(p.index)));
        let mut held = self.region_radios.borrow_mut();
        if held.0 != keys {
            while let Some(child) = self.region_list.first_child() {
                self.region_list.remove(&child);
            }
            let mut labels = vec![format!(
                "Entire disk ({}) - replaces the partition table",
                format_size(size)
            )];
            labels.extend(parts.iter().map(|p| p.label.clone()));
            let mut list: Vec<gtk::CheckButton> = Vec::new();
            for (label, key) in labels.iter().zip(keys.iter().copied()) {
                let radio = gtk::CheckButton::with_label(&plain(label));
                if let Some(first) = list.first() {
                    radio.set_group(Some(first));
                }
                let me = self.me.clone();
                radio.connect_toggled(move |r| {
                    let Some(d) = me.upgrade() else {
                        return;
                    };
                    if r.is_active() && !d.sync.active() {
                        d.apply(DiskWriteAction::SetTargetPartition(key));
                    }
                });
                self.region_list.append(&radio);
                list.push(radio);
            }
            *held = (keys.clone(), list);
        }
        let at = keys.iter().position(|k| *k == part).unwrap_or(0);
        set_radio(&held.1, at);
    }

    fn show_synth(&self, s: &Shown) {
        let label = if s.can_synth {
            "Synthesize partition table"
        } else {
            "Synthesize partition table (source already has one - disabled)"
        };
        if self.synth.label().as_deref() != Some(label) {
            self.synth.set_label(Some(label));
        }
        self.synth.set_sensitive(s.can_synth);
        if self.synth.is_active() != s.form.synthesize {
            self.synth.set_active(s.form.synthesize);
        }
        let f = &s.form;
        self.synth_form.set_sensitive(s.can_synth && f.synthesize);
        set_radio(&self.table, usize::from(f.table_kind == TableKind::Gpt));
        let at = ALIGNS.iter().position(|a| *a == f.alignment).unwrap_or(0);
        set_radio(&self.align, at);
        self.custom_align
            .set_visible(f.alignment == WrapAlign::Custom);
        let n = f.custom_alignment_sectors as f64;
        if self.custom_align.value() != n {
            self.custom_align.set_value(n);
        }
        if let Some(i) = TYPE_BYTE_PRESETS
            .iter()
            .position(|(b, _)| *b == f.type_byte)
        {
            if self.type_pick.selected() != i as u32 {
                self.type_pick.set_selected(i as u32);
            }
        }
        let typed = u8::from_str_radix(self.type_hex.text().trim(), 16).ok();
        if typed != Some(f.type_byte) && !self.type_hex.has_focus() {
            self.type_hex.set_text(&format!("{:02X}", f.type_byte));
        }
        let size = s.source.as_ref().map_or(0, |src| src.size_bytes);
        self.size.show(SizeShown {
            choices: s.choices.to_vec(),
            mode: f.size_mode,
            custom_mib: f.custom_size_mib,
            bounds: s.bounds,
            effective: size,
            deferred: None,
        });
    }
}
