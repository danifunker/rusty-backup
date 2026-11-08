//! The source rows: a backup folder, or a partition of it, or an image; and Write Image's source.

use std::path::{Path, PathBuf};
use std::rc::Rc;

use adw::prelude::*;
use rusty_backup::controller::restore::{
    hints, ids, RestoreAction, RestoreController, RestoreMode, SourcePartition,
};

use super::ActionFn;
use crate::form::{
    button, combo, group, line, path_row, set_combo, set_labels, set_line, set_subtitle,
};
use crate::source::{Pick, PickerSpec, SourcePicker};

fn shown_path(path: Option<&Path>) -> Option<String> {
    path.map(|p| p.display().to_string())
}

/// What the source rows show.
pub struct SourceShown {
    mode: RestoreMode,
    recent: Vec<String>,
    folder: Option<String>,
    image: Option<String>,
    summary: Option<(String, String)>,
    native: bool,
    partitions: Vec<SourcePartition>,
    partition: Option<usize>,
    load_error: Option<String>,
}

impl SourceShown {
    pub fn read(ctrl: &RestoreController, recent: &[String]) -> SourceShown {
        SourceShown {
            mode: ctrl.mode(),
            recent: recent.to_vec(),
            folder: shown_path(ctrl.backup_folder()),
            image: shown_path(ctrl.source_image()),
            summary: ctrl.backup_summary(),
            native: ctrl.has_native_backup(),
            partitions: ctrl.source_partitions(),
            partition: ctrl.source_partition(),
            load_error: ctrl.load_error().map(str::to_string),
        }
    }
}

pub struct SourceRows {
    root: adw::PreferencesGroup,
    picker: Rc<SourcePicker>,
    image: adw::ActionRow,
    partition: adw::ComboRow,
    whole_image: gtk::Label,
    info: gtk::Label,
    origin: gtk::Label,
    error: gtk::Label,
    /// The partition list's table slots, in the combo's order after its placeholder.
    indices: Rc<std::cell::RefCell<Vec<usize>>>,
}

impl SourceRows {
    pub fn build(send: &ActionFn) -> SourceRows {
        let root = group("Source");
        let picker = SourcePicker::build(PickerSpec {
            source: ids::BACKUP_FOLDER,
            recent: ids::RECENT,
            device: "",
            open_file: None,
            open_folder: None,
            open_backup: Some(ids::PICK_BACKUP_FOLDER),
            connect: None,
            connect_live: false,
            no_devices: None,
            width_chars: 36,
            caption: None,
        });
        let s = send.clone();
        picker.connect_pick(move |pick| match pick {
            Pick::Recent(path) => s(RestoreAction::OpenDropped(path)),
            Pick::OpenBackup => s(RestoreAction::PickBackupFolder),
            // This picker offers nothing else.
            Pick::Device(_) | Pick::OpenFile | Pick::OpenFolder | Pick::Connect => {}
        });
        let folder = adw::ActionRow::builder().title("Backup Folder").build();
        picker.root().set_valign(gtk::Align::Center);
        folder.add_suffix(picker.root());
        root.add(&folder);

        let browse = button("Browse...", ids::PICK_SOURCE_IMAGE);
        let s = send.clone();
        browse.connect_clicked(move |_| s(RestoreAction::PickSourceImage));
        let image = path_row("Or Image File", &browse);
        image.set_widget_name(ids::SOURCE_IMAGE);
        root.add(&image);

        let partition = combo("Source Partition", ids::SOURCE_PARTITION, &[]);
        partition.set_use_subtitle(true);
        let indices: Rc<std::cell::RefCell<Vec<usize>>> = Rc::default();
        let held = indices.clone();
        let s = send.clone();
        partition.connect_selected_notify(move |row| {
            let pick = (row.selected() as usize).checked_sub(1);
            let index = pick.and_then(|i| held.borrow().get(i).copied());
            s(RestoreAction::SetSourcePartition(index));
        });
        root.add(&partition);

        let whole_image = line();
        whole_image.add_css_class("dim-label");
        let info = line();
        let origin = line();
        let error = line();
        error.add_css_class("warning");
        root.add(&whole_image);
        root.add(&info);
        root.add(&origin);
        root.add(&error);
        SourceRows {
            root,
            picker,
            image,
            partition,
            whole_image,
            info,
            origin,
            error,
            indices,
        }
    }

    pub fn root(&self) -> &adw::PreferencesGroup {
        &self.root
    }

    pub fn show(&self, s: SourceShown) {
        let full = s.mode == RestoreMode::FullDisk;
        let folder = s.folder.as_deref().unwrap_or("No folder selected");
        self.picker.show(folder, &s.recent, &[]);

        self.image.set_visible(!full);
        set_subtitle(
            &self.image,
            s.image.as_deref().unwrap_or("No file selected"),
        );
        self.partition.set_visible(!full && s.native);
        *self.indices.borrow_mut() = s.partitions.iter().map(|p| p.index).collect();
        let mut labels = vec!["Select a partition...".to_string()];
        labels.extend(s.partitions.iter().map(|p| p.label.clone()));
        set_labels(&self.partition, &labels);
        let at = s
            .partition
            .and_then(|i| s.partitions.iter().position(|p| p.index == i));
        set_combo(&self.partition, at.map_or(0, |i| i as u32 + 1));

        let whole = s.mode == RestoreMode::SinglePartition && !s.native && s.image.is_some();
        set_line(&self.whole_image, whole.then_some(hints::ENTIRE_IMAGE));
        let summary = s.summary.filter(|_| full);
        let info = summary.as_ref().map(|(i, _)| format!("Backup Info: {i}"));
        let origin = summary.as_ref().map(|(_, o)| format!("Source: {o}"));
        set_line(&self.info, info.as_deref());
        set_line(&self.origin, origin.as_deref());
        set_line(&self.error, s.load_error.as_deref());
    }
}

/// What Write Image File's rows show.
pub struct WriteShown {
    path: Option<PathBuf>,
    line: Option<String>,
}

impl WriteShown {
    pub fn read(ctrl: &RestoreController) -> WriteShown {
        WriteShown {
            path: ctrl.write_image().map(|(p, _)| p.to_path_buf()),
            line: ctrl.write_image_line(),
        }
    }
}

pub struct WriteImageRows {
    root: adw::PreferencesGroup,
    image: adw::ActionRow,
    size: gtk::Label,
    note: gtk::Label,
    write: gtk::Button,
}

impl WriteImageRows {
    pub fn build(send: &ActionFn) -> WriteImageRows {
        let root = group("Source Image");
        let choose = button("Choose...", ids::WRITE_IMAGE);
        let s = send.clone();
        choose.connect_clicked(move |_| s(RestoreAction::PickWriteImage));
        let image = path_row("Image file", &choose);
        root.add(&image);
        let size = line();
        let note = line();
        let write = super::write_target_button(send);
        write.set_halign(gtk::Align::Start);
        write.set_margin_top(6);
        root.add(&size);
        root.add(&note);
        root.add(&write);
        WriteImageRows {
            root,
            image,
            size,
            note,
            write,
        }
    }

    pub fn root(&self) -> &adw::PreferencesGroup {
        &self.root
    }

    pub fn show(&self, s: WriteShown) {
        let path = s.path.as_ref().map(|p| p.display().to_string());
        set_subtitle(&self.image, path.as_deref().unwrap_or("(none selected)"));
        set_line(&self.size, s.line.as_deref());
        self.write.set_visible(s.line.is_some());
        let note = match (&s.path, &s.line) {
            (_, Some(_)) => None,
            (Some(_), None) => Some(hints::WRITE_IMAGE_UNREADABLE),
            (None, None) => Some(hints::WRITE_IMAGE_EMPTY),
        };
        set_line(&self.note, note);
    }
}
