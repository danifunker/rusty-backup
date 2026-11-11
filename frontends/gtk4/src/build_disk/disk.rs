//! The Disk step: a table and its donor, a volume's filesystem, a preset or a CD-ROM; then the size.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use adw::prelude::*;
use gtk::glib;
use rusty_backup::controller::build_disk::{
    format_size, hints, ids, BuildDiskAction, BuildDiskController, CdLayout, CdOption, CdOptionRow,
    CdSidecars, CdView, DiskKind, FsChoice, PresetKind, PresetView, TableKind, CD_AUTO_SIZE,
};
use rusty_backup::controller::partition_edit::{BuildAction, BuildView};

use super::ActionFn;
use crate::form::{
    button, combo, group, line, path_row, set_combo, set_entry, set_labels, set_line, set_spin,
    set_subtitle, set_switch, spin, switch,
};
use crate::widgets::{plain, set_text};

const CONTROLLERS: [&str; 2] = ["SASI (256-byte sectors)", "SCSI (1024-byte sectors)"];

/// What the step shows.
pub struct DiskShown {
    kind: DiskKind,
    preset: PresetView,
    cd: CdView,
    cd_auto: bool,
    cd_options: Vec<CdOptionRow>,
    cd_sidecars: CdSidecars,
    cd_sidecars_unavailable: Option<&'static str>,
    volume_fs: FsChoice,
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
            cd: ctrl.cd().clone(),
            cd_auto: ctrl.cd_auto_size(),
            cd_options: ctrl.cd_options(),
            cd_sidecars: ctrl.cd_sidecars(),
            cd_sidecars_unavailable: ctrl.cd_sidecars_unavailable(),
            volume_fs: ctrl.volume_fs_choice(),
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

/// A CD-ROM's layout, name, size and folder, sent whole as the controller takes them.
struct CdRows {
    root: adw::PreferencesGroup,
    held: Rc<RefCell<Option<CdView>>>,
    layout: adw::ComboRow,
    name: adw::EntryRow,
    auto: adw::SwitchRow,
    folder: adw::ActionRow,
    clear: gtk::Button,
    /// One switch per `CdOption::ALL`, in that order.
    options: Vec<adw::SwitchRow>,
    sidecars: adw::ComboRow,
    /// The sidecar choice's help, under the list: a long subtitle would squeeze the value out.
    sidecars_help: gtk::Label,
    /// What `sidecars` lists now, which follows the layout.
    choices: Rc<Cell<&'static [CdSidecars]>>,
}

impl CdRows {
    fn build(send: &ActionFn) -> CdRows {
        let root = group("CD-ROM");
        let held: Rc<RefCell<Option<CdView>>> = Rc::default();
        let edit = |change: Rc<dyn Fn(&mut CdView)>| {
            let held = held.clone();
            let send = send.clone();
            move || {
                let mut cd = held.borrow().clone();
                if let Some(c) = cd.as_mut() {
                    change(c);
                }
                if let Some(c) = cd {
                    send(BuildDiskAction::SetCd(c));
                }
            }
        };

        let labels: Vec<&str> = CdLayout::ALL.iter().map(|l| l.label()).collect();
        let layout = combo("Layout", ids::CD_LAYOUT, &labels);
        let picked = layout.clone();
        let apply = edit(Rc::new(move |c| {
            if let Some(l) = CdLayout::ALL.get(picked.selected() as usize) {
                c.layout = *l;
            }
        }));
        layout.connect_selected_notify(move |_| apply());
        root.add(&layout);

        let name = adw::EntryRow::builder().title("Volume name").build();
        name.set_widget_name(ids::CD_NAME);
        let picked = name.clone();
        let apply = edit(Rc::new(move |c| c.name = picked.text().to_string()));
        name.connect_changed(move |_| apply());
        root.add(&name);

        let auto = switch("Size to the folder", ids::CD_AUTO_SIZE);
        auto.set_tooltip_text(Some(hints::CD_AUTO_SIZE));
        let s = send.clone();
        auto.connect_active_notify(move |r| {
            let text = if r.is_active() { CD_AUTO_SIZE } else { "600M" };
            s(BuildDiskAction::SetSize(text.to_string()));
        });
        root.add(&auto);

        let choose = button("Choose Folder...", ids::CD_FOLDER);
        choose.set_tooltip_text(Some(hints::CD_FOLDER));
        let s = send.clone();
        choose.connect_clicked(move |_| s(BuildDiskAction::PickCdFolder));
        let folder = path_row("Fill from", &choose);
        let clear = button("Clear", ids::CD_FOLDER_CLEAR);
        clear.set_valign(gtk::Align::Center);
        let apply = edit(Rc::new(|c| c.from_dir = None));
        clear.connect_clicked(move |_| apply());
        folder.add_suffix(&clear);
        root.add(&folder);

        let options: Vec<adw::SwitchRow> = CdOption::ALL
            .iter()
            .map(|&option| {
                let row = switch(option.label(), option.id());
                let s = send.clone();
                row.connect_active_notify(move |r| {
                    s(BuildDiskAction::SetCdOption(option, r.is_active()));
                });
                root.add(&row);
                row
            })
            .collect();

        let sidecars = combo("macOS ._ files", ids::CD_SIDECARS, &[]);
        let choices: Rc<Cell<&'static [CdSidecars]>> = Rc::new(Cell::new(&[]));
        let (s, list) = (send.clone(), choices.clone());
        sidecars.connect_selected_notify(move |r| {
            if let Some(c) = list.get().get(r.selected() as usize) {
                s(BuildDiskAction::SetCdSidecars(*c));
            }
        });
        root.add(&sidecars);
        let sidecars_help = line();
        sidecars_help.add_css_class("dim-label");
        sidecars_help.add_css_class("caption");
        sidecars_help.set_margin_top(6);
        root.add(&sidecars_help);
        CdRows {
            root,
            held,
            layout,
            name,
            auto,
            folder,
            clear,
            options,
            sidecars,
            sidecars_help,
            choices,
        }
    }

    fn show(&self, s: &DiskShown) {
        let (c, auto, options) = (s.cd.clone(), s.cd_auto, &s.cd_options);
        let at = CdLayout::ALL.iter().position(|l| *l == c.layout);
        set_combo(&self.layout, at.unwrap_or(0) as u32);
        self.layout.set_subtitle(c.layout.hint());
        set_entry(&self.name, &c.name);
        set_switch(&self.auto, auto);
        let dir = c.from_dir.as_ref().map(|d| d.display().to_string());
        set_subtitle(&self.folder, dir.as_deref().unwrap_or("(a blank disc)"));
        self.clear.set_visible(dir.is_some());
        // Every option stays listed; one that does not apply is greyed with the reason.
        for (switch, row) in self.options.iter().zip(options) {
            set_switch(switch, row.on);
            switch.set_sensitive(row.unavailable.is_none());
            set_help(switch, row.option.hint(), row.unavailable);
        }
        let choices = CdSidecars::choices(c.layout);
        let labels: Vec<String> = choices.iter().map(|c| c.label().to_string()).collect();
        set_labels(&self.sidecars, &labels);
        self.choices.set(choices);
        let at = choices.iter().position(|x| *x == s.cd_sidecars);
        set_combo(&self.sidecars, at.unwrap_or(0) as u32);
        self.sidecars
            .set_sensitive(s.cd_sidecars_unavailable.is_none());
        let help = format!("{} {}", hints::CD_SIDECARS, s.cd_sidecars.hint());
        let help = match s.cd_sidecars_unavailable {
            Some(why) => format!("macOS ._ files: not available: {why}\n{help}"),
            None => help,
        };
        set_line(&self.sidecars_help, Some(&help));
        let tip = s.cd_sidecars_unavailable;
        if self.sidecars.tooltip_text().as_deref() != tip {
            self.sidecars.set_tooltip_text(tip);
        }
        *self.held.borrow_mut() = Some(c);
    }
}

/// An option's help as its subtitle, led by why it cannot be changed now.
fn set_help(row: &impl IsA<adw::ActionRow>, help: &str, unavailable: Option<&str>) {
    let row = row.as_ref();
    if row.subtitle_lines() != 0 {
        row.set_subtitle_lines(0);
    }
    match unavailable {
        Some(why) => set_subtitle(row, &format!("Not available: {why}\n{help}")),
        None => set_subtitle(row, help),
    }
    if row.tooltip_text().as_deref() != unavailable {
        row.set_tooltip_text(unavailable);
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
    cd: CdRows,
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
        let cd = CdRows::build(send);

        let volume_group = group("Volume");
        let names: Vec<String> = FsChoice::all().iter().map(|k| k.label()).collect();
        let labels: Vec<&str> = names.iter().map(String::as_str).collect();
        let volume = combo("Filesystem", ids::VOLUME_FS, &labels);
        let s = send.clone();
        volume.connect_selected_notify(move |r| {
            if let Some(k) = FsChoice::all().get(r.selected() as usize) {
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
        root.append(&cd.root);
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
            cd,
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
        let cd_auto = s.kind == DiskKind::Cd && s.cd_auto;
        self.size_group
            .set_visible((!fixed || s.is_device) && !cd_auto);
        self.size.set_visible(!s.is_device);
        self.device_size.set_visible(s.is_device);
        set_entry(&self.size, &s.size_text);
        let text = match &s.size {
            Ok(n) => format!("{} (the device's size)", format_size(*n)),
            Err(e) => e.clone(),
        };
        set_subtitle(&self.device_size, &text);
        let range = s.range.as_deref().filter(|_| s.kind == DiskKind::Volume);
        let range = range.or((s.kind == DiskKind::Cd).then_some(hints::CD_SIZE));
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
        self.cd.root.set_visible(s.kind == DiskKind::Cd);
        self.volume_group.set_visible(s.kind == DiskKind::Volume);

        self.show_table(&s);
        let at = s.volume_fs.index_in(&FsChoice::all());
        set_combo(&self.volume, at.unwrap_or(0) as u32);
        let fixed = s.range.is_none();
        set_line(&self.fixed, fixed.then_some(hints::FIXED_SIZE));
        self.show_size(&s);
        self.cd.show(&s);
        self.preset.show(s.preset);
    }
}
