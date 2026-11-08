//! The options form: destination, name, output format, CHD options, sizing, split and checksum.

use std::path::PathBuf;
use std::rc::Rc;

use adw::prelude::*;
use rusty_backup::controller::app::chd::ChdOptionsControl;
use rusty_backup::controller::backup::{
    hints, ids, BackupAction, BackupController, ChdProfile, CompressionType, CHECKSUMS, FORMATS,
    SPLIT_MIB_MAX, SPLIT_MIB_MIN,
};

use super::ActionFn;
use crate::chd::{ChdControl, ChdIds};
use crate::form::{
    button, combo, group, path_row, set_combo, set_entry, set_spin, set_subtitle, set_switch, spin,
    switch,
};

/// What the form shows.
pub struct OptionsShown {
    destination: Option<PathBuf>,
    name: String,
    format: usize,
    chd: bool,
    checksum: usize,
    sector_by_sector: bool,
    size_editable: bool,
    resize: bool,
    keep_swap: bool,
    split_shown: bool,
    split_editable: bool,
    split: bool,
    split_mib: u32,
}

impl OptionsShown {
    pub fn read(ctrl: &BackupController) -> OptionsShown {
        let o = ctrl.options();
        OptionsShown {
            destination: ctrl.destination().map(PathBuf::from),
            name: ctrl.name().to_string(),
            format: FORMATS
                .iter()
                .position(|f| f.0 == o.compression)
                .unwrap_or(0),
            chd: o.compression == CompressionType::Chd,
            checksum: CHECKSUMS
                .iter()
                .position(|c| c.0 == o.checksum)
                .unwrap_or(0),
            sector_by_sector: o.sector_by_sector,
            size_editable: o.size_options_editable(),
            resize: o.resize,
            keep_swap: o.keep_swap,
            split_shown: o.split_shown(),
            split_editable: o.split_editable(),
            split: o.split,
            split_mib: o.split_mib,
        }
    }
}

pub struct Options {
    root: gtk::Box,
    destination: adw::ActionRow,
    name: adw::EntryRow,
    format: adw::ComboRow,
    chd: Rc<ChdControl>,
    sector_by_sector: adw::SwitchRow,
    resize: adw::SwitchRow,
    keep_swap: adw::SwitchRow,
    split: adw::SwitchRow,
    split_mib: adw::SpinRow,
    checksum: adw::ComboRow,
}

impl Options {
    /// `changed` redraws the screen after a change only this form holds (the CHD control).
    pub fn build(send: &ActionFn, changed: Rc<dyn Fn()>) -> Rc<Options> {
        let target = group("Destination");
        let browse = button("Browse...", ids::PICK_DESTINATION);
        let s = send.clone();
        browse.connect_clicked(move |_| s(BackupAction::PickDestination));
        let destination = path_row("Folder", &browse);
        destination.set_widget_name(ids::DESTINATION);
        let name = adw::EntryRow::builder().title("Backup Name").build();
        name.set_widget_name(ids::NAME);
        let s = send.clone();
        name.connect_changed(move |e| s(BackupAction::SetName(e.text().to_string())));
        target.add(&destination);
        target.add(&name);

        let output = group("Options");
        let labels: Vec<&str> = FORMATS.iter().map(|f| f.1).collect();
        let format = combo("Output Format", ids::FORMAT, &labels);
        let s = send.clone();
        format.connect_selected_notify(move |row| {
            if let Some(f) = FORMATS.get(row.selected() as usize) {
                s(BackupAction::SetFormat(f.0));
            }
        });
        output.add(&format);

        let mut custom = ChdOptionsControl::new(ChdProfile::Hd);
        custom.custom = BackupController::saved_chd_custom(custom.custom.clone());
        let chd = ChdControl::build(
            "CHD",
            hints::CHD_OUTPUT,
            ChdIds {
                mode: ids::CHD_MODE,
                hunk: ids::CHD_HUNK,
                codec: ids::CHD_CODEC,
            },
            custom,
            ChdProfile::Hd,
            changed,
        );

        let sizing = group("");
        let sector_by_sector = switch(hints::SECTOR_BY_SECTOR, ids::SECTOR_BY_SECTOR);
        let s = send.clone();
        sector_by_sector
            .connect_active_notify(move |r| s(BackupAction::SetSectorBySector(r.is_active())));
        let resize = switch(hints::RESIZE, ids::RESIZE);
        let s = send.clone();
        resize.connect_active_notify(move |r| s(BackupAction::SetResize(r.is_active())));
        let keep_swap = switch(hints::KEEP_SWAP, ids::KEEP_SWAP);
        keep_swap.set_tooltip_text(Some(hints::KEEP_SWAP_HOVER));
        let s = send.clone();
        keep_swap.connect_active_notify(move |r| s(BackupAction::SetKeepSwap(r.is_active())));
        let split = switch("Split archives", ids::SPLIT);
        let s = send.clone();
        split.connect_active_notify(move |r| s(BackupAction::SetSplit(r.is_active())));
        let split_mib = spin(
            "Split size (MiB)",
            ids::SPLIT_MIB,
            f64::from(SPLIT_MIB_MIN),
            f64::from(SPLIT_MIB_MAX),
        );
        let s = send.clone();
        split_mib.connect_value_notify(move |r| s(BackupAction::SetSplitMib(r.value() as u32)));
        let checksum_labels: Vec<&str> = CHECKSUMS.iter().map(|c| c.1).collect();
        let checksum = combo("Checksum", ids::CHECKSUM, &checksum_labels);
        let s = send.clone();
        checksum.connect_selected_notify(move |row| {
            if let Some(c) = CHECKSUMS.get(row.selected() as usize) {
                s(BackupAction::SetChecksum(c.0));
            }
        });
        sizing.add(&sector_by_sector);
        sizing.add(&resize);
        sizing.add(&keep_swap);
        sizing.add(&split);
        sizing.add(&split_mib);
        sizing.add(&checksum);

        let root = gtk::Box::new(gtk::Orientation::Vertical, 12);
        root.append(&target);
        root.append(&output);
        root.append(chd.root());
        root.append(&sizing);
        Rc::new(Options {
            root,
            destination,
            name,
            format,
            chd,
            sector_by_sector,
            resize,
            keep_swap,
            split,
            split_mib,
            checksum,
        })
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    pub fn chd(&self) -> &ChdControl {
        &self.chd
    }

    pub fn show(&self, s: OptionsShown) {
        let folder = s.destination.as_ref().map(|p| p.display().to_string());
        set_subtitle(
            &self.destination,
            folder.as_deref().unwrap_or("No folder selected"),
        );
        set_entry(&self.name, &s.name);
        set_combo(&self.format, s.format as u32);
        self.chd.root().set_visible(s.chd);
        set_switch(&self.sector_by_sector, s.sector_by_sector);
        let locked = if s.size_editable {
            ""
        } else {
            hints::NOT_WITH_SECTOR_COPY
        };
        set_switch(&self.resize, s.resize);
        self.resize.set_sensitive(s.size_editable);
        set_subtitle(&self.resize, locked);
        set_switch(&self.keep_swap, s.keep_swap);
        self.keep_swap.set_sensitive(s.size_editable);
        set_subtitle(&self.keep_swap, locked);

        self.split.set_visible(s.split_shown);
        self.split.set_sensitive(s.split_editable);
        set_switch(&self.split, s.split);
        let vhd = if s.split_editable {
            ""
        } else {
            hints::NOT_FOR_VHD
        };
        set_subtitle(&self.split, vhd);
        self.split_mib
            .set_visible(s.split_shown && s.split && s.split_editable);
        set_spin(&self.split_mib, f64::from(s.split_mib));
        set_combo(&self.checksum, s.checksum as u32);
    }
}
