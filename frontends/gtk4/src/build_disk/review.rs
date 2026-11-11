//! The first and last pages: Kind, Review and the build itself.

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

use adw::prelude::*;
use gtk::glib;
use rusty_backup::controller::app::Snapshot;
use rusty_backup::controller::build_disk::{ids, labels, BuildDiskAction, BuildDiskController, DiskKind};

use super::ActionFn;
use crate::form::{button, group, line, set_entry, set_line, Notes, Tone};
use crate::progress::JobList;
use crate::widgets::plain;

pub struct KindStep {
    root: adw::PreferencesGroup,
    radios: Vec<(DiskKind, gtk::CheckButton)>,
}

impl KindStep {
    pub fn build(send: &ActionFn) -> KindStep {
        let root = group("What are you building?");
        let mut radios: Vec<(DiskKind, gtk::CheckButton)> = Vec::new();
        for kind in DiskKind::ALL {
            let radio = gtk::CheckButton::new();
            radio.set_widget_name(&format!("{}.{kind:?}", ids::KIND));
            radio.set_group(radios.first().map(|r| &r.1));
            let s = send.clone();
            radio.connect_toggled(move |b| {
                if b.is_active() {
                    s(BuildDiskAction::SetKind(kind));
                }
            });
            let row = adw::ActionRow::builder()
                .title(glib::markup_escape_text(kind.label()).as_str())
                .subtitle(glib::markup_escape_text(kind.hint()).as_str())
                .activatable_widget(&radio)
                .build();
            row.add_prefix(&radio);
            root.add(&row);
            radios.push((kind, radio));
        }
        KindStep { root, radios }
    }

    pub fn root(&self) -> &adw::PreferencesGroup {
        &self.root
    }

    pub fn show(&self, kind: DiskKind) {
        for (k, radio) in &self.radios {
            if *k == kind && !radio.is_active() {
                radio.set_active(true);
            }
        }
    }
}

/// What the Review page shows.
pub struct ReviewShown {
    lines: Vec<(Tone, String)>,
    warnings: Vec<(Tone, String)>,
    confirm: Option<String>,
    confirm_text: String,
}

impl ReviewShown {
    pub fn read(ctrl: &BuildDiskController) -> ReviewShown {
        let lines = ctrl.review_lines().into_iter();
        let warnings = ctrl.warnings().into_iter().map(|w| {
            let tone = match w.starts_with("Warning:") {
                true => Tone::Warning,
                false => Tone::Info,
            };
            (tone, w)
        });
        ReviewShown {
            lines: lines.map(|l| (Tone::Plain, l)).collect(),
            warnings: warnings.collect(),
            confirm: ctrl.confirm_expected(),
            confirm_text: ctrl.confirm_text().to_string(),
        }
    }
}

pub struct ReviewStep {
    root: gtk::Box,
    lines: Notes,
    warnings: Notes,
    danger: gtk::Label,
    confirm_group: adw::PreferencesGroup,
    confirm: adw::EntryRow,
}

impl ReviewStep {
    pub fn build(send: &ActionFn) -> ReviewStep {
        let lines = Notes::new();
        let warnings = Notes::new();
        let danger = line();
        danger.add_css_class("error");
        let confirm_group = group("");
        let confirm = adw::EntryRow::new();
        confirm.set_widget_name(ids::CONFIRM);
        let s = send.clone();
        confirm.connect_changed(move |e| {
            s(BuildDiskAction::SetConfirmText(e.text().to_string()));
        });
        confirm_group.add(&danger);
        confirm_group.add(&confirm);
        let root = gtk::Box::new(gtk::Orientation::Vertical, 12);
        root.append(lines.root());
        root.append(warnings.root());
        root.append(&confirm_group);
        ReviewStep {
            root,
            lines,
            warnings,
            danger,
            confirm_group,
            confirm,
        }
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    pub fn show(&self, s: ReviewShown) {
        self.lines.show(s.lines);
        self.warnings.show(s.warnings);
        self.confirm_group.set_visible(s.confirm.is_some());
        let Some(want) = s.confirm else {
            return;
        };
        let erased = format!("Everything on {want} will be erased.");
        set_line(&self.danger, Some(&erased));
        let title = format!("Type `{want}` to confirm");
        let title = glib::markup_escape_text(&plain(&title));
        if self.confirm.title() != title.as_str() {
            self.confirm.set_title(&title);
        }
        set_entry(&self.confirm, &s.confirm_text);
    }
}

/// What the Build page shows.
pub struct BuildShown {
    progress: Option<Snapshot>,
    outcome: Option<Result<(), String>>,
    built: Option<PathBuf>,
}

impl BuildShown {
    pub fn read(ctrl: &BuildDiskController) -> BuildShown {
        BuildShown {
            progress: ctrl.progress().filter(|p| p.is_running()),
            outcome: ctrl.outcome().cloned(),
            built: ctrl.built_image().map(PathBuf::from),
        }
    }
}

pub struct BuildStep {
    root: gtk::Box,
    jobs: JobList,
    outcome: Notes,
    buttons: gtk::Box,
    open: gtk::Button,
    built: Rc<RefCell<Option<PathBuf>>>,
}

impl BuildStep {
    pub fn build(send: &ActionFn) -> BuildStep {
        let s = send.clone();
        let jobs = JobList::new(ids::CANCEL, Rc::new(move || s(BuildDiskAction::Cancel)));
        let outcome = Notes::new();
        let open = button(labels::OPEN_INSPECT, ids::OPEN_INSPECT);
        let built: Rc<RefCell<Option<PathBuf>>> = Default::default();
        let held = built.clone();
        open.connect_clicked(move |_| {
            let path = held.borrow().clone();
            if let Some(path) = path {
                crate::app::open_in_inspect(path);
            }
        });
        let another = button("Build Another", ids::BUILD_ANOTHER);
        let s = send.clone();
        another.connect_clicked(move |_| s(BuildDiskAction::BuildAnother));
        let buttons = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        buttons.append(&open);
        buttons.append(&another);
        let root = gtk::Box::new(gtk::Orientation::Vertical, 12);
        root.append(jobs.root());
        root.append(outcome.root());
        root.append(&buttons);
        BuildStep {
            root,
            jobs,
            outcome,
            buttons,
            open,
            built,
        }
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    pub fn show(&self, s: BuildShown) {
        let running = s.progress.is_some();
        let jobs: Vec<(Snapshot, bool)> = s.progress.into_iter().map(|p| (p, true)).collect();
        self.jobs.show(&jobs);
        self.outcome.show(match &s.outcome {
            Some(Ok(())) => vec![(Tone::Info, "Disk build complete.".to_string())],
            Some(Err(e)) => vec![(Tone::Danger, format!("Error: {e}"))],
            None => Vec::new(),
        });
        self.buttons.set_visible(!running);
        self.open.set_visible(s.built.is_some());
        *self.built.borrow_mut() = s.built;
    }
}
