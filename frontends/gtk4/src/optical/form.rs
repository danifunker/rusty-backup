//! The tab's rows: the source, the disc line with its actions, and the output form.

use std::path::PathBuf;
use std::rc::Rc;

use adw::prelude::*;
use rusty_backup::controller::app::chd::ChdOptionsControl;
use rusty_backup::controller::app::physical_devices_available;
use rusty_backup::controller::optical::audio::ids as audio_ids;
use rusty_backup::controller::optical::{
    hints, ids, AudioAction, ChdProfile, DiscSummary, OpticalAction, OpticalController,
    OutputFormat, SourceMode, Task,
};

use super::ActionFn;
use crate::chd::{ChdControl, ChdIds};
use crate::form::{
    button, group, path_row, set_combo, set_labels, set_subtitle, set_switch, switch,
};
use crate::source::{Pick, PickerSpec, SourcePicker};
use crate::widgets::{fact, flow, flow_show, set_markup, set_text};

const NO_DRIVE: &str = "Select a drive...";

/// What the source rows show.
pub struct SourceShown {
    mode: SourceMode,
    drive_ok: bool,
    drives: Vec<String>,
    selected: u32,
    scanning: bool,
    can_eject: bool,
    image: String,
    recent: Vec<String>,
}

impl SourceShown {
    pub fn read(ctrl: &OpticalController, recent: Vec<String>) -> SourceShown {
        let mut drives = vec![NO_DRIVE.to_string()];
        drives.extend(ctrl.drive_labels());
        SourceShown {
            mode: ctrl.source_mode(),
            // A local drive needs elevation; a remote drive is the daemon's to open.
            drive_ok: physical_devices_available() || ctrl.has_remote_daemons(),
            drives,
            selected: ctrl.selected_drive().map_or(0, |i| i as u32 + 1),
            scanning: ctrl.scanning_drives(),
            can_eject: ctrl.can_eject(),
            image: ctrl
                .image()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "No file selected".to_string()),
            recent,
        }
    }
}

pub struct SourceForm {
    root: adw::PreferencesGroup,
    drive_mode: gtk::CheckButton,
    image_mode: gtk::CheckButton,
    drive: adw::ComboRow,
    refresh: gtk::Button,
    scanning: gtk::Spinner,
    eject: gtk::Button,
    image_row: adw::ActionRow,
    image: Rc<SourcePicker>,
}

impl SourceForm {
    pub fn build(send: &ActionFn) -> Rc<SourceForm> {
        let drive_mode = gtk::CheckButton::with_label("Physical drive");
        drive_mode.set_widget_name(ids::SOURCE_DRIVE);
        let image_mode = gtk::CheckButton::with_label("Image file");
        image_mode.set_widget_name(ids::SOURCE_IMAGE);
        image_mode.set_group(Some(&drive_mode));
        for (radio, mode) in [
            (&drive_mode, SourceMode::PhysicalDrive),
            (&image_mode, SourceMode::ImageFile),
        ] {
            let s = send.clone();
            radio.connect_toggled(move |b| {
                if b.is_active() {
                    s(OpticalAction::SetSourceMode(mode));
                }
            });
        }
        let add_remote = button("Add remote daemon...", ids::ADD_REMOTE);
        add_remote.set_visible(OpticalController::remote_available());
        let s = send.clone();
        add_remote.connect_clicked(move |_| s(OpticalAction::OpenAddRemote));
        let modes = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        modes.set_valign(gtk::Align::Center);
        modes.append(&drive_mode);
        modes.append(&image_mode);
        modes.append(&add_remote);
        let mode_row = adw::ActionRow::builder().title("Source").build();
        mode_row.add_suffix(&modes);

        let drive = adw::ComboRow::builder()
            .title("Drive")
            .model(&gtk::StringList::new(&[NO_DRIVE]))
            .build();
        drive.set_widget_name(ids::DRIVE);
        let s = send.clone();
        drive.connect_selected_notify(move |row| {
            let row = row.selected().checked_sub(1).map(|i| i as usize);
            s(OpticalAction::SelectDrive(row));
        });
        let refresh = button("Refresh", ids::REFRESH);
        refresh.set_valign(gtk::Align::Center);
        let s = send.clone();
        refresh.connect_clicked(move |_| s(OpticalAction::RefreshDrives));
        let scanning = gtk::Spinner::new();
        let eject = button("Eject", ids::EJECT);
        eject.set_valign(gtk::Align::Center);
        let s = send.clone();
        eject.connect_clicked(move |_| s(OpticalAction::Eject));
        drive.add_suffix(&scanning);
        drive.add_suffix(&refresh);
        drive.add_suffix(&eject);

        let image = SourcePicker::build(PickerSpec {
            source: ids::IMAGE,
            recent: ids::IMAGE_RECENT,
            device: ids::IMAGE,
            open_file: Some(ids::BROWSE_IMAGE),
            open_folder: None,
            open_backup: None,
            connect: None,
            connect_live: false,
            no_devices: None,
            width_chars: 36,
            caption: None,
        });
        image.root().set_valign(gtk::Align::Center);
        let s = send.clone();
        image.connect_pick(move |pick| match pick {
            Pick::Recent(path) => s(OpticalAction::OpenImage(path)),
            Pick::OpenFile => s(OpticalAction::PickImage),
            // This picker offers nothing else.
            Pick::Device(_) | Pick::OpenFolder | Pick::OpenBackup | Pick::Connect => {}
        });
        let image_row = adw::ActionRow::builder().title("Disc image").build();
        image_row.add_suffix(image.root());

        let root = group("Optical Disc");
        root.add(&mode_row);
        root.add(&drive);
        root.add(&image_row);
        Rc::new(SourceForm {
            root,
            drive_mode,
            image_mode,
            drive,
            refresh,
            scanning,
            eject,
            image_row,
            image,
        })
    }

    pub fn root(&self) -> &adw::PreferencesGroup {
        &self.root
    }

    pub fn show(&self, s: SourceShown) {
        let by_drive = s.mode == SourceMode::PhysicalDrive;
        if self.drive_mode.is_active() != by_drive {
            self.drive_mode.set_active(by_drive);
        }
        if self.image_mode.is_active() == by_drive {
            self.image_mode.set_active(!by_drive);
        }
        self.drive_mode.set_sensitive(s.drive_ok);
        let hint = (!s.drive_ok).then_some(hints::PHYSICAL_DRIVE);
        self.drive_mode.set_tooltip_text(hint);
        self.drive.set_visible(by_drive);
        self.image_row.set_visible(!by_drive);

        set_labels(&self.drive, &s.drives);
        set_combo(&self.drive, s.selected);
        self.refresh.set_visible(!s.scanning);
        self.scanning.set_visible(s.scanning);
        self.scanning.set_spinning(s.scanning);
        self.eject.set_sensitive(s.can_eject);
        let hint = (!s.can_eject).then_some(hints::EJECT);
        self.eject.set_tooltip_text(hint);
        self.image.show(&s.image, &s.recent, &[]);
    }
}

/// What the disc line and its buttons show.
pub struct DiscShown {
    disc: Option<DiscSummary>,
    error: Option<String>,
    browsing: bool,
    can_chd_info: bool,
    audio: Option<(usize, Option<String>)>,
}

impl DiscShown {
    pub fn read(ctrl: &OpticalController) -> DiscShown {
        let player = ctrl.audio();
        DiscShown {
            disc: ctrl.disc(),
            error: ctrl.disc_error().map(str::to_string),
            browsing: ctrl.is_browsing(),
            can_chd_info: ctrl.can_chd_info(),
            audio: player.has_audio().then(|| {
                let count = player.audio_tracks().count();
                (count, player.readout().map(|r| r.line))
            }),
        }
    }

    /// A track plays, so the readouts move with time.
    pub fn playing(&self) -> bool {
        self.audio.as_ref().is_some_and(|(_, line)| line.is_some())
    }
}

pub struct DiscBar {
    root: gtk::Box,
    info: gtk::Label,
    browse: gtk::Button,
    close: gtk::Button,
    chd_info: gtk::Button,
    audio: gtk::Button,
    playing: gtk::Label,
}

impl DiscBar {
    pub fn build(send: &ActionFn) -> DiscBar {
        let info = gtk::Label::builder()
            .xalign(0.0)
            .wrap(true)
            .wrap_mode(gtk::pango::WrapMode::WordChar)
            .selectable(true)
            .build();
        let browse = button("Browse Contents", ids::BROWSE_CONTENTS);
        let s = send.clone();
        browse.connect_clicked(move |_| s(OpticalAction::BrowseContents));
        let close = button("Close Disc", ids::CLOSE_DISC);
        let s = send.clone();
        close.connect_clicked(move |_| s(OpticalAction::CloseDisc));
        let chd_info = button("CHD Info", ids::CHD_INFO);
        let s = send.clone();
        chd_info.connect_clicked(move |_| s(OpticalAction::ChdInfo));
        let audio = button("Audio Tracks", audio_ids::OPEN);
        audio.set_tooltip_text(Some(hints::AUDIO_TRACKS));
        let s = send.clone();
        audio.connect_clicked(move |_| s(OpticalAction::Audio(AudioAction::Open)));
        let playing = gtk::Label::new(None);
        let actions = flow();
        for widget in [
            browse.upcast_ref::<gtk::Widget>(),
            close.upcast_ref(),
            chd_info.upcast_ref(),
            audio.upcast_ref(),
            playing.upcast_ref(),
        ] {
            actions.insert(widget, -1);
        }
        let root = gtk::Box::new(gtk::Orientation::Vertical, 6);
        root.append(&info);
        root.append(&actions);
        DiscBar {
            root,
            info,
            browse,
            close,
            chd_info,
            audio,
            playing,
        }
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    pub fn show(&self, s: DiscShown) {
        self.info.remove_css_class("error");
        self.info.remove_css_class("dim-label");
        if let Some(d) = &s.disc {
            let mut facts = vec![fact("Format:", &d.format), fact("FS:", &d.filesystem)];
            let more = [
                ("Volume:", &d.volume),
                ("Size:", &d.size),
                ("Game:", &d.game),
            ];
            for (name, value) in more {
                facts.extend(value.as_ref().map(|v| fact(name, v)));
            }
            set_markup(&self.info, &facts.join("   "));
        } else if let Some(error) = &s.error {
            set_text(&self.info, error);
            self.info.add_css_class("error");
        } else {
            set_text(&self.info, "No source selected");
            self.info.add_css_class("dim-label");
        }
        let has_disc = s.disc.is_some();
        flow_show(&self.browse, has_disc && !s.browsing);
        flow_show(&self.close, has_disc);
        flow_show(&self.chd_info, has_disc && s.can_chd_info);
        flow_show(&self.audio, s.audio.is_some());
        let (count, line) = s.audio.unwrap_or((0, None));
        self.audio.set_label(&format!("Audio Tracks ({count})"));
        flow_show(&self.playing, line.is_some());
        set_text(&self.playing, line.as_deref().unwrap_or(""));
    }
}

/// What the output form shows.
pub struct OutputShown {
    task: &'static str,
    rip: bool,
    format: OutputFormat,
    choices: Vec<(OutputFormat, bool, Option<String>)>,
    output: Option<PathBuf>,
    eject_after: bool,
}

impl OutputShown {
    pub fn read(ctrl: &OpticalController) -> OutputShown {
        OutputShown {
            task: ctrl.task_label(),
            rip: ctrl.task() == Task::Rip,
            format: ctrl.output_format(),
            choices: ctrl
                .format_choices()
                .into_iter()
                .map(|(f, on)| (f, on, ctrl.format_refusal(f)))
                .collect(),
            output: ctrl.output().map(PathBuf::from),
            eject_after: ctrl.eject_after(),
        }
    }
}

pub struct OutputForm {
    root: gtk::Box,
    task: adw::ActionRow,
    formats: Vec<(OutputFormat, gtk::CheckButton)>,
    chd: Rc<ChdControl>,
    output: adw::ActionRow,
    eject_after: adw::SwitchRow,
}

impl OutputForm {
    /// `changed` redraws the screen after a change only this form holds (the CHD control).
    pub fn build(send: &ActionFn, changed: Rc<dyn Fn()>) -> Rc<OutputForm> {
        let form = group("Output");
        let task = adw::ActionRow::builder().title("Action").build();
        form.add(&task);

        let radios = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        radios.set_valign(gtk::Align::Center);
        radios.set_widget_name(ids::OUTPUT_FORMAT);
        let mut formats: Vec<(OutputFormat, gtk::CheckButton)> = Vec::new();
        for format in OutputFormat::ALL {
            let radio = gtk::CheckButton::with_label(format.label());
            radio.set_widget_name(&format!("{}.{}", ids::OUTPUT_FORMAT, format.extension()));
            if let Some((_, first)) = formats.first() {
                radio.set_group(Some(first));
            }
            let s = send.clone();
            radio.connect_toggled(move |b| {
                if b.is_active() {
                    s(OpticalAction::SetOutputFormat(format));
                }
            });
            radios.append(&radio);
            formats.push((format, radio));
        }
        let format_row = adw::ActionRow::builder().title("Output format").build();
        format_row.add_suffix(&radios);
        form.add(&format_row);

        let pick = button("Browse...", ids::PICK_OUTPUT);
        let s = send.clone();
        pick.connect_clicked(move |_| s(OpticalAction::PickOutput));
        let output = path_row("Output file", &pick);
        output.set_widget_name(ids::OUTPUT);
        form.add(&output);

        let eject_after = switch("Eject after ripping", ids::EJECT_AFTER);
        eject_after.set_tooltip_text(Some(hints::EJECT_AFTER));
        let s = send.clone();
        eject_after.connect_active_notify(move |r| s(OpticalAction::SetEjectAfter(r.is_active())));
        form.add(&eject_after);

        let mut custom = ChdOptionsControl::new(ChdProfile::Cd);
        custom.custom = OpticalController::saved_chd_custom(custom.custom.clone());
        let chd = ChdControl::build(
            "CHD",
            "",
            ChdIds {
                mode: ids::CHD_MODE,
                hunk: ids::CHD_HUNK,
                codec: ids::CHD_CODEC,
            },
            custom,
            ChdProfile::Cd,
            changed,
        );

        let root = gtk::Box::new(gtk::Orientation::Vertical, 12);
        root.append(&form);
        root.append(chd.root());
        Rc::new(OutputForm {
            root,
            task,
            formats,
            chd,
            output,
            eject_after,
        })
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    pub fn chd(&self) -> &ChdControl {
        &self.chd
    }

    pub fn show(&self, s: OutputShown) {
        set_subtitle(&self.task, s.task);
        for (format, radio) in &self.formats {
            let choice = s.choices.iter().find(|c| c.0 == *format);
            radio.set_sensitive(choice.is_some_and(|c| c.1));
            radio.set_tooltip_text(choice.and_then(|c| c.2.as_deref()));
            if *format == s.format && !radio.is_active() {
                radio.set_active(true);
            }
        }
        self.chd.root().set_visible(s.format == OutputFormat::Chd);
        let path = s.output.as_ref().map(|p| p.display().to_string());
        set_subtitle(
            &self.output,
            path.as_deref().unwrap_or("No output path selected"),
        );
        self.eject_after.set_visible(s.rip);
        set_switch(&self.eject_after, s.eject_after);
    }
}
