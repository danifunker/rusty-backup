//! The Disk step: a table and its donor, a volume's filesystem, or a preset; then the size.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use adw::prelude::*;
use gtk::glib;
use rusty_backup::controller::build_disk::{
    format_size, hints, ids, BuildDiskAction, BuildDiskController, DiskKind, FsKind, PresetKind,
    PresetView, TableKind,
};
use rusty_backup::controller::partition_edit::{BuildAction, BuildView};

use super::ActionFn;
use crate::form::{
    button, combo, group, line, path_row, set_combo, set_entry, set_labels, set_line, set_spin,
    set_subtitle, spin,
};
use crate::widgets::{plain, set_text};

const CONTROLLERS: [&str; 2] = ["SASI (256-byte sectors)", "SCSI (1024-byte sectors)"];

/// What the step shows.
pub struct DiskShown {
    kind: DiskKind,
    preset: PresetView,
    volume_fs: FsKind,
    range: Option<String>,
    view: BuildView,
    donor: Option<String>,
    donor_reading: bool,
    donor_error: Option<String>,
    is_device: bool,
    size_text: String,
    size: Result<u64, String>,
}

impl DiskShown {
    pub fn read(ctrl: &BuildDiskController) -> DiskShown {
        DiskShown {
            kind: ctrl.kind(),
            preset: ctrl.preset().clone(),
            volume_fs: ctrl.volume_fs(),
            range: ctrl.volume_size_range(),
            view: ctrl.table_view(),
            donor: ctrl.donor().map(|(p, t)| format!("{t}: {}", p.display())),
            donor_reading: ctrl.donor_reading(),
            donor_error: ctrl.donor_error().map(str::to_string),
            is_device: ctrl.output_is_device(),
            size_text: ctrl.size_text().to_string(),
            size: ctrl.disk_size(),
        }
    }
}

/// The fields of the two presets, sent whole as the controller takes them.
struct PresetRows {
    root: adw::PreferencesGroup,
    held: Rc<RefCell<Option<PresetView>>>,
    kind: adw::ComboRow,
    controller: adw::ComboRow,
    partitions: adw::SpinRow,
    system: adw::ActionRow,
    clear: gtk::Button,
    label: adw::EntryRow,
}

impl PresetRows {
    fn build(send: &ActionFn) -> PresetRows {
        let root = group("Preset");
        let held: Rc<RefCell<Option<PresetView>>> = Rc::default();
        let edit = |change: Rc<dyn Fn(&mut PresetView)>| {
            let held = held.clone();
            let send = send.clone();
            move || {
                let mut preset = held.borrow().clone();
                if let Some(p) = preset.as_mut() {
                    change(p);
                }
                if let Some(p) = preset {
                    send(BuildDiskAction::SetPreset(p));
                }
            }
        };

        let labels: Vec<&str> = PresetKind::ALL.iter().map(|k| k.label()).collect();
        let kind = combo("Disk", ids::PRESET, &labels);
        let picked = kind.clone();
        let apply = edit(Rc::new(move |p| {
            if let Some(k) = PresetKind::ALL.get(picked.selected() as usize) {
                p.kind = *k;
            }
        }));
        kind.connect_selected_notify(move |_| apply());
        root.add(&kind);

        let controller = combo("Controller", ids::X68K_SCSI, &CONTROLLERS);
        let picked = controller.clone();
        let apply = edit(Rc::new(move |p| p.x68k_scsi = picked.selected() == 1));
        controller.connect_selected_notify(move |_| apply());
        root.add(&controller);

        let partitions = spin("Human68k partitions", ids::X68K_PARTS, 1.0, 8.0);
        let picked = partitions.clone();
        let apply = edit(Rc::new(move |p| {
            p.x68k_partitions = picked.value() as usize;
        }));
        partitions.connect_value_notify(move |_| apply());
        root.add(&partitions);

        let choose = button("Choose...", ids::SYSTEM_DISK);
        choose.set_tooltip_text(Some(hints::SYSTEM_DISK));
        let s = send.clone();
        choose.connect_clicked(move |_| s(BuildDiskAction::PickSystemDisk));
        let system = path_row("System disk", &choose);
        let clear = button("Clear", ids::SYSTEM_DISK_CLEAR);
        clear.set_valign(gtk::Align::Center);
        let apply = edit(Rc::new(|p| p.x68k_system_disk = None));
        clear.connect_clicked(move |_| apply());
        system.add_suffix(&clear);
        root.add(&system);

        let label = adw::EntryRow::builder().title("EFS label").build();
        label.set_widget_name(ids::SGI_LABEL);
        let picked = label.clone();
        let apply = edit(Rc::new(move |p| p.sgi_label = picked.text().to_string()));
        label.connect_changed(move |_| apply());
        root.add(&label);
        PresetRows {
            root,
            held,
            kind,
            controller,
            partitions,
            system,
            clear,
            label,
        }
    }

    fn show(&self, p: PresetView) {
        let at = PresetKind::ALL.iter().position(|k| *k == p.kind);
        set_combo(&self.kind, at.unwrap_or(0) as u32);
        let x68k = p.kind == PresetKind::X68k;
        self.controller.set_visible(x68k);
        self.partitions.set_visible(x68k);
        self.system.set_visible(x68k);
        self.label.set_visible(!x68k);
        set_combo(&self.controller, u32::from(p.x68k_scsi));
        set_spin(&self.partitions, p.x68k_partitions as f64);
        let disk = p.x68k_system_disk.as_ref().map(|d| d.display().to_string());
        set_subtitle(
            &self.system,
            disk.as_deref().unwrap_or(hints::NO_SYSTEM_DISK),
        );
        self.clear.set_visible(disk.is_some());
        set_entry(&self.label, &p.sgi_label);
        *self.held.borrow_mut() = Some(p);
    }
}

pub struct DiskStep {
    root: gtk::Box,
    table_group: adw::PreferencesGroup,
    donor: adw::ActionRow,
    donor_spinner: gtk::Spinner,
    donor_blank: gtk::Button,
    donor_error: gtk::Label,
    table: adw::ComboRow,
    kinds: Rc<Cell<&'static [TableKind]>>,
    preset: PresetRows,
    volume_group: adw::PreferencesGroup,
    volume: adw::ComboRow,
    fixed: gtk::Label,
    size_group: adw::PreferencesGroup,
    size: adw::EntryRow,
    size_note: gtk::Label,
    device_size: adw::ActionRow,
    size_error: gtk::Label,
    layout_group: adw::PreferencesGroup,
    align: adw::EntryRow,
    heads: adw::SpinRow,
    sectors: adw::SpinRow,
}

impl DiskStep {
    pub fn build(send: &ActionFn) -> DiskStep {
        let table_group = group("Partition Table");
        let donor = adw::ActionRow::builder()
            .title("Start from")
            .subtitle_lines(2)
            .build();
        donor.set_tooltip_text(Some(hints::DONOR));
        let donor_spinner = gtk::Spinner::new();
        let pick = button("An existing disk's layout...", ids::DONOR);
        pick.set_valign(gtk::Align::Center);
        let s = send.clone();
        pick.connect_clicked(move |_| s(BuildDiskAction::PickDonor));
        let donor_blank = button("Blank table", ids::DONOR_BLANK);
        donor_blank.set_valign(gtk::Align::Center);
        let s = send.clone();
        donor_blank.connect_clicked(move |_| s(BuildDiskAction::ClearDonor));
        donor.add_suffix(&donor_spinner);
        donor.add_suffix(&pick);
        donor.add_suffix(&donor_blank);
        table_group.add(&donor);
        let donor_error = line();
        donor_error.add_css_class("error");

        let table = combo("Table type", ids::TABLE, &[]);
        let kinds: Rc<Cell<&'static [TableKind]>> = Rc::new(Cell::new(&[]));
        let held = kinds.clone();
        let s = send.clone();
        table.connect_selected_notify(move |r| {
            if let Some(k) = held.get().get(r.selected() as usize) {
                s(BuildDiskAction::Table(BuildAction::SetKind(*k)));
            }
        });
        table_group.add(&table);
        table_group.add(&donor_error);

        let preset = PresetRows::build(send);

        let volume_group = group("Volume");
        let labels: Vec<&str> = FsKind::ALL.iter().map(|k| k.display_name()).collect();
        let volume = combo("Filesystem", ids::VOLUME_FS, &labels);
        let s = send.clone();
        volume.connect_selected_notify(move |r| {
            if let Some(k) = FsKind::ALL.get(r.selected() as usize) {
                s(BuildDiskAction::SetVolumeFs(*k));
            }
        });
        volume_group.add(&volume);
        let fixed = line();
        fixed.add_css_class("dim-label");
        volume_group.add(&fixed);

        let size_group = group("");
        let size = adw::EntryRow::builder().title("Size").build();
        size.set_widget_name(ids::SIZE);
        size.set_tooltip_text(Some(hints::SIZE));
        let s = send.clone();
        size.connect_changed(move |e| s(BuildDiskAction::SetSize(e.text().to_string())));
        let size_note = gtk::Label::new(None);
        size_note.add_css_class("dim-label");
        size.add_suffix(&size_note);
        let device_size = adw::ActionRow::builder().title("Size").build();
        let size_error = line();
        size_error.add_css_class("error");
        size_group.add(&size);
        size_group.add(&device_size);
        size_group.add(&size_error);

        let layout_group = group("Layout");
        let align = adw::EntryRow::builder().title("Alignment").build();
        align.set_widget_name(ids::ALIGN);
        align.set_tooltip_text(Some(hints::ALIGN));
        let s = send.clone();
        align.connect_changed(move |e| {
            s(BuildDiskAction::Table(BuildAction::SetAlign(
                e.text().to_string(),
            )));
        });
        let heads = spin("Heads", ids::HEADS, 1.0, 255.0);
        let s = send.clone();
        heads.connect_value_notify(move |r| {
            s(BuildDiskAction::Table(BuildAction::SetHeads(
                r.value() as u16
            )));
        });
        let sectors = spin("Sectors/track", ids::SECTORS, 1.0, 1024.0);
        let s = send.clone();
        sectors.connect_value_notify(move |r| {
            s(BuildDiskAction::Table(BuildAction::SetSectorsPerTrack(
                r.value() as u16,
            )));
        });
        layout_group.add(&align);
        layout_group.add(&heads);
        layout_group.add(&sectors);

        let root = gtk::Box::new(gtk::Orientation::Vertical, 12);
        root.append(&table_group);
        root.append(&preset.root);
        root.append(&volume_group);
        root.append(&size_group);
        root.append(&layout_group);
        DiskStep {
            root,
            table_group,
            donor,
            donor_spinner,
            donor_blank,
            donor_error,
            table,
            kinds,
            preset,
            volume_group,
            volume,
            fixed,
            size_group,
            size,
            size_note,
            device_size,
            size_error,
            layout_group,
            align,
            heads,
            sectors,
        }
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    fn show_table(&self, s: &DiskShown) {
        let v = &s.view;
        set_subtitle(&self.donor, s.donor.as_deref().unwrap_or("A blank table"));
        self.donor_blank.set_visible(s.donor.is_some());
        self.donor_spinner.set_visible(s.donor_reading);
        self.donor_spinner.set_spinning(s.donor_reading);
        set_line(&self.donor_error, s.donor_error.as_deref());

        // The kinds first: replacing the model fires the handler, which reads them.
        self.kinds.set(v.kinds);
        let labels: Vec<String> = v.kinds.iter().map(|k| k.label().to_string()).collect();
        set_labels(&self.table, &labels);
        let at = v.kinds.iter().position(|k| *k == v.kind);
        set_combo(&self.table, at.unwrap_or(0) as u32);
        self.table.set_sensitive(!v.fixed);

        let title = format!("Alignment (blank: {})", v.align_hint);
        let title = glib::markup_escape_text(&plain(&title));
        if self.align.title() != title.as_str() {
            self.align.set_title(&title);
        }
        set_entry(&self.align, &v.align_text);
        self.layout_group.set_sensitive(!v.fixed);
        self.heads.set_visible(v.geometry.is_some());
        self.sectors.set_visible(v.geometry.is_some());
        if let Some((heads, sectors)) = v.geometry {
            set_spin(&self.heads, f64::from(heads));
            set_spin(&self.sectors, f64::from(sectors));
        }
    }

    fn show_size(&self, s: &DiskShown) {
        let fixed = s.kind == DiskKind::Volume && s.range.is_none();
        self.size_group.set_visible(!fixed || s.is_device);
        self.size.set_visible(!s.is_device);
        self.device_size.set_visible(s.is_device);
        set_entry(&self.size, &s.size_text);
        let text = match &s.size {
            Ok(n) => format!("{} (the device's size)", format_size(*n)),
            Err(e) => e.clone(),
        };
        set_subtitle(&self.device_size, &text);
        let range = s.range.as_deref().filter(|_| s.kind == DiskKind::Volume);
        let note = match (&s.size, range) {
            (Ok(n), Some(r)) => format!("{}, {r}", format_size(*n)),
            (Ok(n), None) => format_size(*n),
            (Err(_), Some(r)) => r.to_string(),
            (Err(_), None) => String::new(),
        };
        set_text(&self.size_note, &note);
        let error = s.size.as_ref().err().filter(|_| !s.is_device);
        set_line(&self.size_error, error.map(String::as_str));
    }

    pub fn show(&self, s: DiskShown) {
        self.table_group
            .set_visible(s.kind == DiskKind::Partitioned);
        self.layout_group
            .set_visible(s.kind == DiskKind::Partitioned);
        self.preset.root.set_visible(s.kind == DiskKind::Preset);
        self.volume_group.set_visible(s.kind == DiskKind::Volume);

        self.show_table(&s);
        let at = FsKind::ALL.iter().position(|k| *k == s.volume_fs);
        set_combo(&self.volume, at.unwrap_or(0) as u32);
        let fixed = s.range.is_none();
        set_line(&self.fixed, fixed.then_some(hints::FIXED_SIZE));
        self.show_size(&s);
        self.preset.show(s.preset);
    }
}
