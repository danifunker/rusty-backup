//! The Output step: an image file with its container, or a physical disk.

use std::cell::RefCell;
use std::rc::Rc;

use adw::prelude::*;
use rusty_backup::controller::app::{no_devices_hint, DiskDevice};
use rusty_backup::controller::build_disk::{
    containers, hints, ids, BuildDiskAction, BuildDiskController, ExportFormat,
};
use rusty_backup::controller::restore::device_label;

use super::ActionFn;
use crate::form::{
    button, combo, group, line, path_row, set_combo, set_labels, set_line, set_subtitle,
};

const KINDS: [&str; 2] = ["Image file", "Physical disk"];

/// What the step shows.
pub struct OutputShown {
    is_device: bool,
    /// A CD-ROM is a raw file: no device, no container.
    cd: bool,
    image: Option<String>,
    container: ExportFormat,
    devices: Vec<DiskDevice>,
    device_row: Option<usize>,
}

impl OutputShown {
    pub fn read(ctrl: &BuildDiskController, devices: &[DiskDevice]) -> OutputShown {
        OutputShown {
            is_device: ctrl.output_is_device(),
            cd: ctrl.kind() == rusty_backup::controller::build_disk::DiskKind::Cd,
            image: ctrl.image().map(|p| p.display().to_string()),
            container: ctrl.container(),
            devices: devices.to_vec(),
            device_row: ctrl.device_row(),
        }
    }
}

pub struct OutputStep {
    root: adw::PreferencesGroup,
    kind: adw::ComboRow,
    image: adw::ActionRow,
    container: adw::ComboRow,
    device: adw::ComboRow,
    devices: Rc<RefCell<Vec<DiskDevice>>>,
    formats: Vec<ExportFormat>,
    warning: gtk::Label,
}

impl OutputStep {
    pub fn build(send: &ActionFn) -> OutputStep {
        let root = group("Output");
        let kind = combo("Write to", ids::OUTPUT_KIND, &KINDS);
        kind.set_tooltip_text(Some(hints::DEVICE));
        let s = send.clone();
        kind.connect_selected_notify(move |r| {
            s(BuildDiskAction::SetOutputIsDevice(r.selected() == 1));
        });
        root.add(&kind);

        let save = button("Save As...", ids::PICK_IMAGE);
        let s = send.clone();
        save.connect_clicked(move |_| s(BuildDiskAction::PickOutputImage));
        let image = path_row("Save as", &save);
        image.set_widget_name(ids::OUTPUT_IMAGE);
        root.add(&image);

        let formats = containers();
        let labels: Vec<&str> = formats.iter().map(|c| c.description()).collect();
        let container = combo("Format", ids::CONTAINER, &labels);
        container.set_tooltip_text(Some(hints::CONTAINER));
        let s = send.clone();
        let held = formats.clone();
        container.connect_selected_notify(move |r| {
            if let Some(c) = held.get(r.selected() as usize) {
                s(BuildDiskAction::SetContainer(*c));
            }
        });
        root.add(&container);

        let device = combo("Device", ids::OUTPUT_DEVICE, &[]);
        device.set_use_subtitle(true);
        let devices: Rc<RefCell<Vec<DiskDevice>>> = Rc::default();
        let list = devices.clone();
        let s = send.clone();
        device.connect_selected_notify(move |r| {
            let Some(row) = (r.selected() as usize).checked_sub(1) else {
                s(BuildDiskAction::ClearDevice);
                return;
            };
            let device = list.borrow().get(row).cloned();
            if let Some(device) = device {
                s(BuildDiskAction::SelectDevice { row, device });
            }
        });
        root.add(&device);

        let warning = line();
        warning.add_css_class("warning");
        root.add(&warning);
        OutputStep {
            root,
            kind,
            image,
            container,
            device,
            devices,
            formats,
            warning,
        }
    }

    pub fn root(&self) -> &adw::PreferencesGroup {
        &self.root
    }

    pub fn show(&self, s: OutputShown) {
        set_combo(&self.kind, u32::from(s.is_device));
        // A CD-ROM image is burned from the file, so it never offers a device or a container.
        self.kind.set_visible(!s.cd || s.is_device);
        self.image.set_visible(!s.is_device);
        self.container.set_visible(!s.is_device && !s.cd);
        self.device.set_visible(s.is_device);
        set_subtitle(&self.image, s.image.as_deref().unwrap_or("(not chosen)"));
        let at = self.formats.iter().position(|c| *c == s.container);
        set_combo(&self.container, at.unwrap_or(0) as u32);

        // The device list first: replacing the model fires the handler, which reads it.
        *self.devices.borrow_mut() = s.devices.clone();
        let mut labels = vec![if s.devices.is_empty() {
            no_devices_hint().to_string()
        } else {
            "Select a device...".to_string()
        }];
        labels.extend(s.devices.iter().map(device_label));
        set_labels(&self.device, &labels);
        set_combo(&self.device, s.device_row.map_or(0, |r| r as u32 + 1));
        set_line(&self.warning, s.is_device.then_some(hints::DEVICE_WARNING));
    }
}
