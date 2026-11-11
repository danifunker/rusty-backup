//! The Options step: the label and options of every formatted row, or of the volume.

use std::cell::RefCell;
use std::rc::Rc;

use adw::prelude::*;
use rusty_backup::controller::build_disk::{
    cpm_presets, geometries, hints, ids, parse_size_field, BuildDiskAction, FormatTarget, FsKind,
    FsOption, OptionEdit, AFFS_VARIANTS, NTFS_VERSIONS, SECTOR_SIZES,
};

use super::ActionFn;
use crate::form::{combo, group, line, set_combo, set_entry, set_line, set_switch, switch};

type EditFn = Rc<dyn Fn(OptionEdit)>;

/// A blank (auto) or sized field; "4K" stays as typed while it means the controller's value.
struct SizeField {
    row: adw::EntryRow,
}

impl SizeField {
    fn new(title: &str, id: &str, edit: impl Fn(Option<u64>) + 'static) -> SizeField {
        let row = adw::EntryRow::builder().title(title).build();
        row.set_widget_name(id);
        row.connect_changed(move |e| match parse_size_field(&e.text()) {
            Ok(v) => {
                e.remove_css_class("error");
                edit(v);
            }
            Err(_) => e.add_css_class("error"),
        });
        SizeField { row }
    }

    fn show(&self, value: Option<u64>) {
        match parse_size_field(&self.row.text()) {
            Ok(v) if v == value => {}
            Err(_) if self.row.focus_child().is_some() => {}
            _ => set_entry(&self.row, &value.map(|v| v.to_string()).unwrap_or_default()),
        }
    }
}

/// One option's widget.
enum Field {
    Size(SizeField, fn(&FormatTarget) -> Option<u64>),
    Switch(adw::SwitchRow, fn(&FormatTarget) -> bool),
    Combo(adw::ComboRow, fn(&FormatTarget) -> u32),
}

fn opts_index<T: PartialEq>(all: &[T], value: Option<T>) -> u32 {
    let at = value.and_then(|v| all.iter().position(|x| *x == v));
    at.unwrap_or(0) as u32
}

/// `opt`'s row, added to `root`; `None` for the options only the CLI sets.
fn field(
    opt: FsOption,
    fs: FsKind,
    id: &str,
    root: &adw::PreferencesGroup,
    edit: &EditFn,
) -> Field {
    let size = |title: &str, make: fn(Option<u64>) -> OptionEdit, read| {
        let e = edit.clone();
        let f = SizeField::new(title, id, move |v| e(make(v)));
        f.row
            .set_tooltip_text(Some("Blank is automatic. Accepts 4K / 1M."));
        root.add(&f.row);
        Field::Size(f, read)
    };
    let toggle = |title: &str, make: fn(bool) -> OptionEdit, read| {
        let row = switch(title, id);
        let e = edit.clone();
        row.connect_active_notify(move |r| e(make(r.is_active())));
        root.add(&row);
        Field::Switch(row, read)
    };
    let pick = |title: &str, labels: &[&str], read, make: Rc<dyn Fn(u32) -> Option<OptionEdit>>| {
        let row = combo(title, id, labels);
        let e = edit.clone();
        row.connect_selected_notify(move |r| {
            if let Some(edit) = make(r.selected()) {
                e(edit);
            }
        });
        root.add(&row);
        Field::Combo(row, read)
    };
    match opt {
        FsOption::BlockSize => size("Block size", OptionEdit::BlockSize, |t| {
            t.opts.block_size.map(u64::from)
        }),
        FsOption::Inodes => size("Inodes", OptionEdit::Inodes, |t| t.opts.inodes),
        FsOption::BytesPerInode => size("Bytes per inode", OptionEdit::BytesPerInode, |t| {
            t.opts.bytes_per_inode
        }),
        FsOption::ClusterSize => size("Cluster size", OptionEdit::ClusterSize, |t| {
            t.opts.cluster_bytes.map(u64::from)
        }),
        FsOption::BigEndian => toggle("Big-endian", OptionEdit::BigEndian, |t| t.opts.big_endian),
        FsOption::Fat32 => {
            let f = toggle("FAT32 whatever the size", OptionEdit::Fat32, |t| {
                t.opts.fat32
            });
            if let Field::Switch(row, _) = &f {
                row.set_tooltip_text(Some(hints::FAT32));
            }
            f
        }
        FsOption::CaseSensitive => {
            toggle("Case-sensitive (HFSX)", OptionEdit::CaseSensitive, |t| {
                t.opts.case_sensitive
            })
        }
        FsOption::AffsVariant => {
            let labels = AFFS_VARIANTS.map(|v| v.1);
            let read = |t: &FormatTarget| {
                let all = AFFS_VARIANTS.map(|v| v.0);
                opts_index(&all, Some(t.opts.affs_variant))
            };
            let make = |i: u32| {
                let v = AFFS_VARIANTS.get(i as usize);
                v.map(|v| OptionEdit::AffsVariant(v.0))
            };
            pick("Variant", &labels, read, Rc::new(make))
        }
        FsOption::CpmPreset => {
            let all = cpm_presets();
            let read = |t: &FormatTarget| {
                opts_index(cpm_presets().as_slice(), t.opts.cpm_preset.as_deref())
            };
            let make = |i: u32| {
                let all = cpm_presets();
                let p = all.get(i as usize);
                p.map(|p| OptionEdit::CpmPreset(p.to_string()))
            };
            pick("Disk format", &all, read, Rc::new(make))
        }
        FsOption::Geometry => {
            let all = geometries(fs);
            let labels: Vec<&str> = all.iter().map(|g| g.1).collect();
            let read = |t: &FormatTarget| {
                let ids: Vec<&str> = geometries(t.fs).iter().map(|g| g.0).collect();
                opts_index(ids.as_slice(), t.opts.geometry.as_deref())
            };
            let make = move |i: u32| {
                let g = all.get(i as usize);
                g.map(|g| OptionEdit::Geometry(g.0.to_string()))
            };
            pick("Geometry", &labels, read, Rc::new(make))
        }
        FsOption::SectorSize => {
            let labels = SECTOR_SIZES.map(|s| s.to_string());
            let labels: Vec<&str> = labels.iter().map(String::as_str).collect();
            let read = |t: &FormatTarget| {
                opts_index(&SECTOR_SIZES, Some(t.opts.sector_size.unwrap_or(512)))
            };
            let make = |i: u32| {
                let s = SECTOR_SIZES.get(i as usize);
                s.map(|s| OptionEdit::SectorSize(*s))
            };
            pick("Sector size", &labels, read, Rc::new(make))
        }
        FsOption::NtfsVersion => {
            let labels: Vec<&str> = NTFS_VERSIONS.iter().map(|v| v.1).collect();
            let all: Vec<_> = NTFS_VERSIONS.iter().map(|v| v.0).collect();
            let read = |t: &FormatTarget| {
                let all: Vec<_> = NTFS_VERSIONS.iter().map(|v| v.0).collect();
                opts_index(
                    all.as_slice(),
                    Some(t.opts.ntfs_version.unwrap_or_default()),
                )
            };
            let make = move |i: u32| all.get(i as usize).map(|v| OptionEdit::NtfsVersion(*v));
            pick("NTFS version", &labels, read, Rc::new(make))
        }
        // `FormatTarget::fields` leaves these to the CLI.
        FsOption::BtreeSizes | FsOption::MinCatalog => Field::Switch(switch("", id), |_| false),
    }
}

/// One formatted target's group.
struct TargetForm {
    root: adw::PreferencesGroup,
    held: Rc<RefCell<FormatTarget>>,
    label: Option<adw::EntryRow>,
    fields: Vec<Field>,
    error: gtk::Label,
}

impl TargetForm {
    fn build(t: &FormatTarget, send: &ActionFn) -> TargetForm {
        let root = group(&t.title);
        let held = Rc::new(RefCell::new(t.clone()));
        let target = held.clone();
        let send = send.clone();
        let edit: EditFn = Rc::new(move |edit| {
            // Cloned first: the send refreshes this form, which replaces the held target.
            let t = target.borrow().clone();
            let opts = t.edited(edit);
            send(match t.row {
                Some(row) => BuildDiskAction::SetRowOptions { row, opts },
                None => BuildDiskAction::SetVolumeOptions(opts),
            });
        });
        let key = t.row.map_or("volume".to_string(), |r| r.to_string());
        let label = (t.caps.label_max > 0).then(|| {
            let row = adw::EntryRow::builder().title("Label").build();
            row.set_widget_name(&format!("{}.{key}", ids::LABEL));
            row.set_tooltip_text(Some(&format!("Up to {} bytes", t.caps.label_max)));
            let e = edit.clone();
            row.connect_changed(move |r| e(OptionEdit::Label(r.text().to_string())));
            root.add(&row);
            row
        });
        let fields = t.fields().into_iter().enumerate().map(|(i, opt)| {
            let id = format!("{}.{key}.{i}", ids::OPTION);
            field(opt, t.fs, &id, &root, &edit)
        });
        let fields = fields.collect();
        let error = line();
        error.add_css_class("error");
        root.add(&error);
        TargetForm {
            root,
            held,
            label,
            fields,
            error,
        }
    }

    fn show(&self, t: FormatTarget) {
        if let Some(row) = &self.label {
            set_entry(row, &t.opts.label);
        }
        for field in &self.fields {
            match field {
                Field::Size(f, read) => f.show(read(&t)),
                Field::Switch(row, read) => set_switch(row, read(&t)),
                Field::Combo(row, read) => set_combo(row, read(&t)),
            }
        }
        set_line(&self.error, t.error.as_deref());
        *self.held.borrow_mut() = t;
    }
}

pub struct OptionsStep {
    root: gtk::Box,
    forms: RefCell<Vec<TargetForm>>,
    keys: RefCell<Vec<(Option<usize>, FsKind)>>,
    send: ActionFn,
}

impl OptionsStep {
    pub fn build(send: &ActionFn) -> OptionsStep {
        OptionsStep {
            root: gtk::Box::new(gtk::Orientation::Vertical, 12),
            forms: RefCell::default(),
            keys: RefCell::default(),
            send: send.clone(),
        }
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    pub fn show(&self, targets: Vec<FormatTarget>) {
        let keys: Vec<_> = targets.iter().map(|t| (t.row, t.fs)).collect();
        let mut forms = self.forms.borrow_mut();
        if *self.keys.borrow() != keys {
            while let Some(child) = self.root.first_child() {
                self.root.remove(&child);
            }
            *forms = targets
                .iter()
                .map(|t| TargetForm::build(t, &self.send))
                .collect();
            for form in forms.iter() {
                self.root.append(&form.root);
            }
            *self.keys.borrow_mut() = keys;
        }
        for (form, target) in forms.iter().zip(targets) {
            form.show(target);
        }
    }
}
