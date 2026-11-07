//! The target rows of each mode: a device, an image file, a remote drive or a scanned partition.

use std::cell::{Cell, RefCell};
use std::path::Path;
use std::rc::Rc;

use adw::prelude::*;
use rusty_backup::controller::app::{no_devices_hint, DiskDevice};
use rusty_backup::controller::restore::{
    device_label, format_size, hints, ids, AlignmentChoice, Caveat, NewDiskChoice, NewTableType,
    RemoteDrive, RestoreAction, RestoreController, RestoreMode, TargetPartition, TargetSlot,
    ALIGN_SECTORS_MAX,
};

use super::{slot_of, tone, ActionFn};
use crate::form::{
    button, combo, group, line, path_row, set_combo, set_entry, set_labels, set_line, set_spin,
    set_subtitle, set_switch, spin, switch, Notes,
};
use crate::widgets::{plain, set_text};

const KINDS: [&str; 2] = ["Write to device", "Save as image file"];
const TABLES: [(NewTableType, &str); 3] = [
    (NewTableType::Mbr, "MBR"),
    (NewTableType::Gpt, "GPT"),
    (NewTableType::Apm, "APM"),
];
const ALIGNMENTS: [(AlignmentChoice, &str); 3] = [
    (AlignmentChoice::Modern1MB, "1 MB (LBA 2048)"),
    (AlignmentChoice::Original, "DOS (LBA 63)"),
    (AlignmentChoice::Custom, "Custom"),
];

/// What the target rows show; `None` for a slot hides them.
pub struct TargetShown {
    slot: Option<TargetSlot>,
    remote_shown: bool,
    remote: bool,
    is_device: bool,
    devices: Vec<DiskDevice>,
    device_row: Option<usize>,
    device_name: Option<String>,
    image: Option<String>,
    warning: Option<&'static str>,
    readout: Option<Caveat>,
    scanning: bool,
    partitions: Vec<TargetPartition>,
    partition: Option<usize>,
    scan_error: Option<String>,
    remote_address: String,
    remote_busy: bool,
    remote_connected: bool,
    remote_error: Option<String>,
    remote_drives: Vec<RemoteDrive>,
    remote_device: Option<String>,
    remote_image: String,
    remote_target: Option<String>,
}

impl TargetShown {
    pub fn read(ctrl: &RestoreController, devices: &[DiskDevice]) -> TargetShown {
        let mode = ctrl.mode();
        let slot = slot_of(mode);
        let of = TargetSlot::FullDisk;
        let shown = slot.unwrap_or(of);
        let full = mode == RestoreMode::FullDisk;
        TargetShown {
            slot,
            remote_shown: full && RestoreController::remote_available(),
            remote: full && ctrl.remote_active(),
            is_device: ctrl.target_is_device(shown),
            devices: devices.to_vec(),
            device_row: ctrl.device_row(shown),
            device_name: ctrl.device(shown).map(|d| d.display_name()),
            image: ctrl
                .target_image(shown)
                .map(|p: &Path| p.display().to_string()),
            warning: ctrl.system_disk_warning(shown),
            readout: if full { ctrl.target_readout() } else { None },
            scanning: ctrl.target_scan().is_some(),
            partitions: ctrl.target_partitions(),
            partition: ctrl.target_partition(),
            scan_error: ctrl.target_scan_error().map(str::to_string),
            remote_address: ctrl.remote_address().to_string(),
            remote_busy: ctrl.remote_busy(),
            remote_connected: ctrl.remote_connected(),
            remote_error: ctrl.remote_error().map(str::to_string),
            remote_drives: ctrl.remote_drives(),
            remote_device: ctrl.remote_selected_device().map(str::to_string),
            remote_image: ctrl.remote_image_name().to_string(),
            remote_target: ctrl.remote_target_label().map(str::to_string),
        }
    }
}

/// The remote rows of a full-disk restore.
struct RemoteRows {
    address: adw::EntryRow,
    connect: gtk::Button,
    spinner: gtk::Spinner,
    error: gtk::Label,
    note: gtk::Label,
    list: gtk::ListBox,
    image: adw::EntryRow,
    use_image: gtk::Button,
    selected: gtk::Label,
    drives: Rc<RefCell<Vec<RemoteDrive>>>,
}

impl RemoteRows {
    fn build(root: &adw::PreferencesGroup, send: &ActionFn) -> RemoteRows {
        let address = adw::EntryRow::builder().title("Host").build();
        address.set_widget_name(ids::REMOTE_ADDRESS);
        address.set_tooltip_text(Some(hints::REMOTE_ADDRESS));
        let s = send.clone();
        address.connect_changed(move |e| s(RestoreAction::RemoteSetAddress(e.text().to_string())));
        let connect = button("Connect", ids::REMOTE_CONNECT);
        connect.set_valign(gtk::Align::Center);
        let s = send.clone();
        connect.connect_clicked(move |_| s(RestoreAction::RemoteConnect));
        let spinner = gtk::Spinner::new();
        address.add_suffix(&spinner);
        address.add_suffix(&connect);
        root.add(&address);

        let image = adw::EntryRow::builder().title("or image file").build();
        image.set_widget_name(ids::REMOTE_IMAGE_NAME);
        image.set_tooltip_text(Some(hints::REMOTE_IMAGE_NAME));
        let s = send.clone();
        image.connect_changed(move |e| s(RestoreAction::RemoteSetImageName(e.text().to_string())));
        let use_image = button("Use file", ids::REMOTE_USE_IMAGE);
        use_image.set_valign(gtk::Align::Center);
        let s = send.clone();
        use_image.connect_clicked(move |_| s(RestoreAction::RemoteUseImage));
        image.add_suffix(&use_image);
        root.add(&image);

        let error = line();
        error.add_css_class("error");
        let note = line();
        note.add_css_class("dim-label");
        let list = gtk::ListBox::builder()
            .selection_mode(gtk::SelectionMode::None)
            .activate_on_single_click(true)
            .margin_top(6)
            .build();
        list.add_css_class("boxed-list");
        let drives: Rc<RefCell<Vec<RemoteDrive>>> = Rc::default();
        let held = drives.clone();
        let s = send.clone();
        list.connect_row_activated(move |_, row| {
            let drive = held.borrow().get(row.index() as usize).cloned();
            if let Some(d) = drive {
                s(RestoreAction::RemotePickDevice {
                    path: d.path,
                    size: d.size_bytes,
                });
            }
        });
        let selected = line();
        selected.add_css_class("dim-label");
        root.add(&error);
        root.add(&note);
        root.add(&list);
        root.add(&selected);
        RemoteRows {
            address,
            connect,
            spinner,
            error,
            note,
            list,
            image,
            use_image,
            selected,
            drives,
        }
    }

    fn show(&self, s: &TargetShown) {
        let on = s.remote;
        self.address.set_visible(on);
        set_entry(&self.address, &s.remote_address);
        self.connect.set_sensitive(!s.remote_busy);
        let label = if s.remote_connected {
            "Reconnect"
        } else {
            "Connect"
        };
        if self.connect.label().as_deref() != Some(label) {
            self.connect.set_label(label);
        }
        self.spinner.set_visible(s.remote_busy);
        self.spinner.set_spinning(s.remote_busy);
        set_line(&self.error, s.remote_error.as_deref().filter(|_| on));

        let linked = on && s.remote_connected;
        let none = linked && s.remote_drives.is_empty();
        let drives_note = if none {
            Some(hints::NO_REMOTE_DRIVES)
        } else if linked {
            Some("Restore target on the remote machine:")
        } else {
            None
        };
        set_line(&self.note, drives_note);
        self.list.set_visible(linked && !s.remote_drives.is_empty());
        if *self.drives.borrow() != s.remote_drives {
            *self.drives.borrow_mut() = s.remote_drives.clone();
            self.list.remove_all();
            for (i, d) in s.remote_drives.iter().enumerate() {
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
        let mut child = self.list.first_child();
        let mut i = 0;
        while let Some(widget) = child {
            let picked = s.remote_drives.get(i).map(|d| d.path.as_str());
            let on = picked.is_some() && picked == s.remote_device.as_deref();
            if on {
                widget.add_css_class("accent");
            } else {
                widget.remove_css_class("accent");
            }
            child = widget.next_sibling();
            i += 1;
        }

        self.image.set_visible(linked);
        set_entry(&self.image, &s.remote_image);
        self.use_image
            .set_sensitive(!s.remote_image.trim().is_empty());
        let target = s.remote_target.as_ref().filter(|_| linked);
        let chosen = target.map(|t| format!("Selected target: {t}"));
        set_line(&self.selected, chosen.as_deref());
    }
}

/// A scanned partition's radio.
struct PartRow {
    index: usize,
    radio: gtk::CheckButton,
}

pub struct TargetRows {
    root: adw::PreferencesGroup,
    /// The target the rows act on: the mode's, set with each refresh.
    slot: Rc<Cell<TargetSlot>>,
    remote_switch: adw::SwitchRow,
    kind: adw::ComboRow,
    device: adw::ComboRow,
    close: gtk::Button,
    scan: gtk::Button,
    image: adw::ActionRow,
    remote: RemoteRows,
    warning: gtk::Label,
    readout: Notes,
    devices: Rc<RefCell<Vec<DiskDevice>>>,
    parts: gtk::ListBox,
    part_rows: RefCell<Vec<PartRow>>,
    shown_parts: RefCell<Vec<TargetPartition>>,
    scan_error: gtk::Label,
    send: ActionFn,
}

impl TargetRows {
    pub fn build(send: &ActionFn) -> TargetRows {
        let root = group("Target");
        let slot = Rc::new(Cell::new(TargetSlot::FullDisk));

        let remote_switch = switch(hints::REMOTE, ids::REMOTE_TARGET);
        remote_switch.set_tooltip_text(Some(hints::REMOTE_HOVER));
        let s = send.clone();
        remote_switch.connect_active_notify(move |r| s(RestoreAction::SetRemote(r.is_active())));
        root.add(&remote_switch);

        let kind = combo("Target", ids::TARGET_KIND, &KINDS);
        let s = send.clone();
        let at = slot.clone();
        kind.connect_selected_notify(move |r| {
            s(RestoreAction::SetTargetIsDevice(
                at.get(),
                r.selected() == 0,
            ));
        });
        root.add(&kind);

        let device = combo("Device", ids::TARGET_DEVICE, &[]);
        device.set_use_subtitle(true);
        let devices: Rc<RefCell<Vec<DiskDevice>>> = Rc::default();
        let held = devices.clone();
        let s = send.clone();
        let at = slot.clone();
        device.connect_selected_notify(move |r| {
            let slot = at.get();
            let Some(row) = (r.selected() as usize).checked_sub(1) else {
                s(RestoreAction::ClearDevice(slot));
                return;
            };
            let device = held.borrow().get(row).cloned();
            if let Some(device) = device {
                s(RestoreAction::SelectDevice { slot, row, device });
            }
        });
        let close = button("Close Drive", ids::CLOSE_TARGET);
        close.set_tooltip_text(Some(hints::CLOSE_TARGET));
        close.set_valign(gtk::Align::Center);
        let s = send.clone();
        let at = slot.clone();
        close.connect_clicked(move |_| s(RestoreAction::ClearDevice(at.get())));
        let scan = button("Scan Target", ids::SCAN_TARGET);
        scan.set_valign(gtk::Align::Center);
        let s = send.clone();
        scan.connect_clicked(move |_| s(RestoreAction::ScanTarget));
        device.add_suffix(&close);
        device.add_suffix(&scan);
        root.add(&device);

        let save = button("Save As...", ids::PICK_TARGET_IMAGE);
        let s = send.clone();
        let at = slot.clone();
        save.connect_clicked(move |_| s(RestoreAction::PickTargetImage(at.get())));
        let image = path_row("Image File", &save);
        image.set_widget_name(ids::TARGET_IMAGE);
        root.add(&image);

        let remote = RemoteRows::build(&root, send);

        let warning = line();
        warning.add_css_class("error");
        let readout = Notes::new();
        let parts = gtk::ListBox::builder()
            .selection_mode(gtk::SelectionMode::None)
            .margin_top(6)
            .build();
        parts.add_css_class("boxed-list");
        let scan_error = line();
        scan_error.add_css_class("warning");
        root.add(&warning);
        root.add(readout.root());
        root.add(&parts);
        root.add(&scan_error);
        TargetRows {
            root,
            slot,
            remote_switch,
            kind,
            device,
            close,
            scan,
            image,
            remote,
            warning,
            readout,
            devices,
            parts,
            part_rows: RefCell::default(),
            shown_parts: RefCell::default(),
            scan_error,
            send: send.clone(),
        }
    }

    pub fn root(&self) -> &adw::PreferencesGroup {
        &self.root
    }

    fn show_parts(&self, s: &TargetShown) {
        let single = s.slot == Some(TargetSlot::SinglePartition);
        self.parts.set_visible(single && !s.partitions.is_empty());
        if *self.shown_parts.borrow() != s.partitions {
            *self.shown_parts.borrow_mut() = s.partitions.clone();
            self.parts.remove_all();
            let mut rows = self.part_rows.borrow_mut();
            rows.clear();
            let mut first: Option<gtk::CheckButton> = None;
            for part in &s.partitions {
                let radio = gtk::CheckButton::new();
                radio.set_widget_name(&format!("{}.{}", ids::TARGET_PARTITION, part.index));
                radio.set_group(first.as_ref());
                first.get_or_insert_with(|| radio.clone());
                let index = part.index;
                let send = self.send.clone();
                radio.connect_toggled(move |b| {
                    if b.is_active() {
                        send(RestoreAction::SetTargetPartition(index));
                    }
                });
                let title = format!("{}: {}", part.position, part.type_name);
                let row = adw::ActionRow::builder()
                    .title(gtk::glib::markup_escape_text(&plain(&title)).as_str())
                    .subtitle(format_size(part.size_bytes).as_str())
                    .activatable_widget(&radio)
                    .build();
                row.add_prefix(&radio);
                match part.fits {
                    Some(false) => {
                        let note = gtk::Label::new(Some("too small"));
                        note.add_css_class("error");
                        row.add_suffix(&note);
                    }
                    Some(true) => row.add_suffix(&gtk::Label::new(Some("OK"))),
                    None => {}
                }
                self.parts.append(&row);
                rows.push(PartRow { index, radio });
            }
        }
        for row in self.part_rows.borrow().iter() {
            let on = s.partition == Some(row.index);
            if row.radio.is_active() != on {
                row.radio.set_active(on);
            }
        }
    }

    pub fn show(&self, s: TargetShown) {
        self.root.set_visible(s.slot.is_some());
        let Some(slot) = s.slot else {
            return;
        };
        self.slot.set(slot);
        let single = slot == TargetSlot::SinglePartition;

        self.remote_switch.set_visible(s.remote_shown);
        set_switch(&self.remote_switch, s.remote);
        self.kind.set_visible(!single && !s.remote);
        set_combo(&self.kind, u32::from(!s.is_device));

        // The device list first: replacing the model fires the handler, which reads it.
        let on_device = (single || s.is_device) && !s.remote;
        self.device.set_visible(on_device);
        *self.devices.borrow_mut() = s.devices.clone();
        let mut labels = vec![if s.devices.is_empty() {
            no_devices_hint().to_string()
        } else {
            "Select a target device...".to_string()
        }];
        labels.extend(s.devices.iter().map(device_label));
        set_labels(&self.device, &labels);
        set_combo(&self.device, s.device_row.map_or(0, |r| r as u32 + 1));
        self.close
            .set_visible(slot == TargetSlot::FullDisk && s.device_row.is_some() && on_device);
        self.scan
            .set_visible(single && s.device_name.is_some() && !s.scanning);

        self.image.set_visible(!single && !s.is_device && !s.remote);
        set_subtitle(
            &self.image,
            s.image.as_deref().unwrap_or("No file selected"),
        );
        self.remote.show(&s);

        set_line(&self.warning, s.warning.filter(|_| on_device));
        let readout = s.readout.iter().map(|n| (tone(n.level), n.text.clone()));
        self.readout.show(readout.collect());
        self.show_parts(&s);
        set_line(&self.scan_error, s.scan_error.as_deref().filter(|_| single));
    }
}

/// What the new-disk rows show.
pub struct NewDiskShown {
    choice: NewDiskChoice,
    start_lba: u64,
}

impl NewDiskShown {
    pub fn read(ctrl: &RestoreController) -> NewDiskShown {
        NewDiskShown {
            choice: ctrl.new_disk(),
            start_lba: ctrl.new_disk_start_lba(),
        }
    }
}

pub struct NewDiskRows {
    root: adw::PreferencesGroup,
    held: Rc<Cell<Option<NewDiskChoice>>>,
    table: adw::ComboRow,
    alignment: adw::ComboRow,
    custom: adw::SpinRow,
    bootable: adw::SwitchRow,
    start: gtk::Label,
}

impl NewDiskRows {
    pub fn build(send: &ActionFn) -> NewDiskRows {
        let root = group("New Partition Table");
        let held: Rc<Cell<Option<NewDiskChoice>>> = Rc::default();
        let edit = |change: Rc<dyn Fn(&mut NewDiskChoice)>| {
            let held = held.clone();
            let send = send.clone();
            move || {
                if let Some(mut choice) = held.get() {
                    change(&mut choice);
                    send(RestoreAction::SetNewDisk(choice));
                }
            }
        };

        let labels: Vec<&str> = TABLES.iter().map(|t| t.1).collect();
        let table = combo("Table Type", ids::NEW_DISK_TABLE, &labels);
        let picked = table.clone();
        let apply = edit(Rc::new(move |c| {
            if let Some(t) = TABLES.get(picked.selected() as usize) {
                c.table = t.0;
            }
        }));
        table.connect_selected_notify(move |_| apply());

        let labels: Vec<&str> = ALIGNMENTS.iter().map(|a| a.1).collect();
        let alignment = combo("Alignment", ids::NEW_DISK_ALIGNMENT, &labels);
        let picked = alignment.clone();
        let apply = edit(Rc::new(move |c| {
            if let Some(a) = ALIGNMENTS.get(picked.selected() as usize) {
                c.alignment = a.0;
            }
        }));
        alignment.connect_selected_notify(move |_| apply());

        let custom = spin(
            "Custom alignment (sectors)",
            ids::NEW_DISK_CUSTOM,
            1.0,
            f64::from(ALIGN_SECTORS_MAX),
        );
        let picked = custom.clone();
        let apply = edit(Rc::new(move |c| c.custom_alignment = picked.value() as u32));
        custom.connect_value_notify(move |_| apply());

        let bootable = switch("Mark partition as bootable", ids::NEW_DISK_BOOTABLE);
        let picked = bootable.clone();
        let apply = edit(Rc::new(move |c| c.bootable = picked.is_active()));
        bootable.connect_active_notify(move |_| apply());

        let start = line();
        start.add_css_class("dim-label");
        root.add(&table);
        root.add(&alignment);
        root.add(&custom);
        root.add(&bootable);
        root.add(&start);
        NewDiskRows {
            root,
            held,
            table,
            alignment,
            custom,
            bootable,
            start,
        }
    }

    pub fn root(&self) -> &adw::PreferencesGroup {
        &self.root
    }

    pub fn show(&self, s: NewDiskShown) {
        let c = s.choice;
        self.held.set(Some(c));
        let table = TABLES.iter().position(|t| t.0 == c.table).unwrap_or(0);
        set_combo(&self.table, table as u32);
        let at = ALIGNMENTS.iter().position(|a| a.0 == c.alignment);
        set_combo(&self.alignment, at.unwrap_or(0) as u32);
        self.custom
            .set_visible(c.alignment == AlignmentChoice::Custom);
        set_spin(&self.custom, f64::from(c.custom_alignment));
        self.bootable.set_visible(c.table == NewTableType::Mbr);
        set_switch(&self.bootable, c.bootable);
        let start = format!("Partition will start at LBA {}", s.start_lba);
        set_line(&self.start, Some(&start));
    }
}
