//! The Archives tab: a retained view over `controller::archives::ArchivesController`.
//!
//! `src/gui/archives_tab.rs` is the reference. Read-only: open an archive, tick entries, extract
//! all or the ticked ones in a fork-preserving format, or mount a single-entry disk image in Inspect.

mod list;

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use adw::prelude::*;
use rusty_backup::controller::app::{Notify, RecentHandle, RecentMode, Snapshot};
use rusty_backup::controller::archives::{
    hints, ids, mount_hint, mount_label, ArchivesAction, ArchivesController, ExtractOutcome,
    ForkFormat, HostRequest, MountableImageKind,
};
use rusty_backup::controller::inspect::InspectSource;
use rusty_backup::controller::{Controller, Effect};

use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::{button, Notes, Tone};
use crate::progress::JobList;
use crate::source::{Pick, PickerSpec, SourcePicker};
use crate::sync::SyncGuard;
use crate::widgets::{plain, set_text};

use list::EntryList;

/// Sends an action to the controller; a change a sync made is dropped here, for every widget.
type ActionFn = Rc<dyn Fn(ArchivesAction)>;

pub struct ArchivesScreen {
    ctrl: RefCell<ArchivesController>,
    host: EffectHost,
    recent: RecentHandle,
    recent_files: RefCell<Vec<String>>,
    sync: SyncGuard,
    me: Weak<ArchivesScreen>,
    root: gtk::Box,
    source: Rc<SourcePicker>,
    close: gtk::Button,
    jobs: JobList,
    notes: Notes,
    count: gtk::Label,
    list: Rc<EntryList>,
    controls: gtk::Box,
    format: gtk::DropDown,
    extract_all: gtk::Button,
    extract_selected: gtk::Button,
    mount: gtk::Button,
    bar: gtk::ProgressBar,
}

/// Everything a refresh shows, read while the controller is borrowed and shown after it is not.
struct Shown {
    source: String,
    has_path: bool,
    jobs: Vec<(Snapshot, bool)>,
    load_error: Option<(String, String)>,
    loaded: bool,
    summary: String,
    selected: usize,
    format: ForkFormat,
    mountable: Option<MountableImageKind>,
    extracting: bool,
    progress: Option<(usize, usize, String)>,
    outcome: Option<String>,
    failed: bool,
}

fn outcome_text(outcome: &ExtractOutcome) -> (String, bool) {
    match outcome {
        Ok((stats, dest)) => {
            let skipped = match stats.skipped {
                0 => String::new(),
                n => format!(", {n} skipped"),
            };
            let line = format!(
                "Extracted {} files to {}{skipped}",
                stats.files,
                dest.display()
            );
            (line, false)
        }
        Err(e) => (format!("Extraction failed: {}", e.message), true),
    }
}

impl ArchivesScreen {
    pub fn build(notify: Arc<dyn Notify>, recent: RecentHandle, host: EffectHost) -> Rc<Self> {
        Rc::new_cyclic(|me: &Weak<ArchivesScreen>| {
            let target = me.clone();
            let send: ActionFn = Rc::new(move |action| {
                let Some(screen) = target.upgrade() else {
                    return;
                };
                if !screen.sync.active() {
                    screen.apply(action);
                }
            });

            let intro = gtk::Label::builder()
                .label(hints::INTRO)
                .xalign(0.0)
                .wrap(true)
                .build();
            intro.add_css_class("dim-label");

            let source = SourcePicker::build(PickerSpec {
                source: ids::SOURCE,
                recent: ids::RECENT,
                device: ids::SOURCE,
                open_file: Some(ids::BROWSE),
                open_folder: None,
                open_backup: None,
                connect: None,
                connect_live: false,
                no_devices: None,
                width_chars: 44,
                caption: Some("Archive:"),
            });
            let s = send.clone();
            source.connect_pick(move |pick| match pick {
                Pick::Recent(path) => s(ArchivesAction::Open(path)),
                Pick::OpenFile => s(ArchivesAction::Pick),
                // This picker offers nothing else.
                Pick::Device(_) | Pick::OpenFolder | Pick::OpenBackup | Pick::Connect => {}
            });
            let close = button("Close", ids::CLOSE);
            close.set_tooltip_text(Some(hints::CLOSE));
            let s = send.clone();
            close.connect_clicked(move |_| s(ArchivesAction::Close));
            let source_bar = gtk::Box::new(gtk::Orientation::Horizontal, 6);
            source_bar.append(source.root());
            source_bar.append(&close);

            // Reading an archive cannot be cancelled, so the row's Cancel never shows.
            let jobs = JobList::new(ids::CLOSE, Rc::new(|| {}));
            let notes = Notes::new();
            let count = gtk::Label::builder().xalign(0.0).build();
            let target = me.clone();
            let ticked = Rc::new(move |row: usize| {
                let screen = target.upgrade();
                screen.is_some_and(|s| s.ctrl.borrow().is_selected(row))
            });
            let target = me.clone();
            let guarded = Rc::new(move |f: &mut dyn FnMut()| {
                if let Some(screen) = target.upgrade() {
                    screen.sync.run(f);
                }
            });
            let list = EntryList::build(&send, ticked, guarded);

            let labels: Vec<&str> = ForkFormat::ALL.iter().map(|f| f.label()).collect();
            let format = gtk::DropDown::from_strings(&labels);
            format.set_widget_name(ids::FORMAT);
            let s = send.clone();
            format.connect_selected_notify(move |d| {
                if let Some(f) = ForkFormat::ALL.get(d.selected() as usize) {
                    s(ArchivesAction::SetFormat(*f));
                }
            });
            let extract_all = button("Extract All...", ids::EXTRACT_ALL);
            let s = send.clone();
            extract_all.connect_clicked(move |_| s(ArchivesAction::ExtractAll));
            let extract_selected = button("Extract Selected... (0)", ids::EXTRACT_SELECTED);
            extract_selected.set_tooltip_text(Some(hints::EXTRACT_SELECTED));
            let s = send.clone();
            extract_selected.connect_clicked(move |_| s(ArchivesAction::ExtractSelected));
            let mount = button("Mount in new Inspect tab", ids::MOUNT);
            let s = send.clone();
            mount.connect_clicked(move |_| s(ArchivesAction::Mount));
            let controls = gtk::Box::new(gtk::Orientation::Horizontal, 6);
            controls.append(&gtk::Label::new(Some("Extract as:")));
            controls.append(&format);
            controls.append(&extract_all);
            controls.append(&extract_selected);
            controls.append(&mount);

            let bar = gtk::ProgressBar::builder().show_text(true).build();
            bar.set_ellipsize(gtk::pango::EllipsizeMode::Middle);

            let root = gtk::Box::new(gtk::Orientation::Vertical, 8);
            root.set_margin_top(12);
            root.set_margin_bottom(12);
            root.set_margin_start(12);
            root.set_margin_end(12);
            root.append(&intro);
            root.append(&source_bar);
            root.append(jobs.root());
            root.append(notes.root());
            root.append(&count);
            root.append(list.root());
            root.append(&controls);
            root.append(&bar);
            let target = me.clone();
            root.connect_map(move |_| {
                if let Some(screen) = target.upgrade() {
                    screen.refresh();
                }
            });

            let recent_files = recent.list(RecentMode::Archives);
            ArchivesScreen {
                ctrl: RefCell::new(ArchivesController::new(notify)),
                host,
                recent,
                recent_files: RefCell::new(recent_files),
                sync: SyncGuard::default(),
                me: me.clone(),
                root,
                source,
                close,
                jobs,
                notes,
                count,
                list,
                controls,
                format,
                extract_all,
                extract_selected,
                mount,
                bar,
            }
        })
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    /// Load `path` as the archive (Inspect hands a picked Mac archive over here).
    pub fn open_path(&self, path: PathBuf) {
        self.apply(ArchivesAction::Open(path));
    }

    pub fn apply(&self, action: ArchivesAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    pub fn tick(&self) {
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    /// Answers to this controller's own questions.
    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            if let Some(screen) = me.upgrade() {
                screen.answered(reply);
            }
        })
    }

    fn answered(&self, reply: Reply) {
        let action = match reply {
            Reply::Picked { purpose, path } => ArchivesAction::Picked { purpose, path },
            // Archives only ever opens pickers.
            Reply::Confirmed { .. }
            | Reply::Answered { .. }
            | Reply::Chose { .. }
            | Reply::CloseScreen => return,
        };
        self.apply(action);
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        let (requests, log) = {
            let mut ctrl = self.ctrl.borrow_mut();
            (ctrl.take_requests(), ctrl.take_log())
        };
        for request in requests {
            match request {
                HostRequest::RememberRecent(path) => self.remember(path),
                HostRequest::OpenInInspect { path, guard } => {
                    crate::app::load_in_inspect(InspectSource::Image {
                        path,
                        temp: Some(guard),
                    });
                }
            }
        }
        crate::app::append_log(log);
        // Off screen nothing needs it, and a job wakes this many times a second.
        if self.root.is_mapped() {
            self.refresh();
        }
    }

    /// Settings cleared the recent files: the picker forgets its copy too.
    pub fn forget_recents(&self) {
        self.recent_files.borrow_mut().clear();
        self.refresh();
    }

    fn remember(&self, path: PathBuf) {
        let list = self
            .recent
            .remember(RecentMode::Archives, &path.display().to_string());
        *self.recent_files.borrow_mut() = list;
    }

    fn read(&self) -> Shown {
        let ctrl = self.ctrl.borrow();
        let mut jobs = Vec::new();
        jobs.extend(ctrl.load_snapshot().map(|s| (s, false)));
        let outcome = ctrl.last_extract().map(outcome_text);
        Shown {
            source: ctrl
                .path()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "(none selected)".to_string()),
            has_path: ctrl.path().is_some(),
            jobs,
            load_error: ctrl
                .load_error()
                .map(|e| (e.message.clone(), e.detail.clone())),
            loaded: ctrl.is_loaded(),
            summary: ctrl.summary(),
            selected: ctrl.selected_file_count(),
            format: ctrl.format(),
            mountable: ctrl.mountable(),
            extracting: ctrl.is_extracting(),
            progress: ctrl.extract_progress(),
            failed: outcome.as_ref().is_some_and(|o| o.1),
            outcome: outcome.map(|o| o.0),
        }
    }

    /// Read the controller first and release it: setting a widget can fire its change signal.
    fn refresh(&self) {
        let shown = self.read();
        let rows = self.list.changed(&self.ctrl.borrow());
        self.sync.run(|| {
            if let Some(rows) = rows {
                self.list.show(rows);
            }
            self.show(shown);
        });
    }

    fn show(&self, s: Shown) {
        let recent = self.recent_files.borrow();
        self.source.show(&s.source, &recent, &[]);
        self.source.root().set_sensitive(!s.extracting);
        self.close.set_visible(s.has_path);
        self.close.set_sensitive(!s.extracting);
        self.jobs.show(&s.jobs);

        let mut notes = Vec::new();
        if let Some((message, _)) = &s.load_error {
            notes.push((Tone::Danger, format!("Error: {message}")));
        }
        if let Some(line) = s.outcome {
            let tone = if s.failed { Tone::Danger } else { Tone::Info };
            notes.push((tone, line));
        }
        self.notes.show(notes);
        self.notes.root().set_tooltip_text(
            s.load_error
                .as_ref()
                .map(|e| plain(&e.1).into_owned())
                .as_deref(),
        );

        self.count.set_visible(s.loaded);
        set_text(&self.count, &s.summary);
        self.list.root().set_visible(s.loaded);
        self.controls.set_visible(s.loaded);
        let at = ForkFormat::ALL.iter().position(|f| *f == s.format);
        let at = at.unwrap_or(0) as u32;
        if self.format.selected() != at {
            self.format.set_selected(at);
        }
        self.extract_all.set_sensitive(!s.extracting);
        self.extract_selected
            .set_sensitive(!s.extracting && s.selected > 0);
        let label = format!("Extract Selected... ({})", s.selected);
        if self.extract_selected.label().as_deref() != Some(label.as_str()) {
            self.extract_selected.set_label(&label);
        }
        self.mount.set_visible(s.mountable.is_some());
        self.mount.set_sensitive(!s.extracting);
        if let Some(kind) = s.mountable {
            self.mount.set_label(&mount_label(kind));
            self.mount.set_tooltip_text(Some(&mount_hint(kind)));
        }

        self.bar.set_visible(s.progress.is_some());
        if let Some((done, total, current)) = s.progress {
            let fraction = if total > 0 {
                done as f64 / total as f64
            } else {
                0.0
            };
            self.bar.set_fraction(fraction);
            let text = format!("{done} / {total}  {current}");
            self.bar.set_text(Some(&plain(&text)));
        }
    }
}
