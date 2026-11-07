//! The Restore tab: a retained view over `controller::restore::RestoreController`.
//!
//! See docs/gtk4_shell_plan.md for the pattern; `src/gui/restore_tab.rs` is the reference. Start
//! only ever sends `Start`: the write begins on the controller's own `Confirm`, and the target
//! named above the button is `write_target()`, the name that question uses. Write Image File's
//! window is the Physical Disk Export, a later slice, so it stays greyed out here.

mod sizes;
mod source;
mod target;

use std::cell::{Cell, RefCell};
use std::path::{Path, PathBuf};
use std::rc::{Rc, Weak};
use std::sync::Arc;

use adw::prelude::*;
use rusty_backup::controller::app::{
    busy_elsewhere_notice, DiskDevice, Notify, RecentHandle, RecentMode, Snapshot,
};
use rusty_backup::controller::log::LogLevel;
use rusty_backup::controller::restore::{
    hints, ids, purpose, CaveatLevel, HostRequest, JobRegistry, RestoreAction, RestoreController,
    RestoreMode, RestoreServices, TargetSlot, MODES,
};
use rusty_backup::controller::{Controller, Effect};

use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::{button, combo, group, line, set_combo, set_line, set_switch, switch};
use crate::form::{Notes, Tone};
use crate::progress::{JobList, Run, RunBar};
use crate::sync::SyncGuard;
use crate::widgets::{coming, set_text};

use sizes::{Sizes, SizesShown};
use source::{SourceRows, SourceShown, WriteImageRows, WriteShown};
use target::{NewDiskRows, NewDiskShown, TargetRows, TargetShown};

/// Sends an action to the controller; a change a sync made is dropped here, for every widget.
type ActionFn = Rc<dyn Fn(RestoreAction)>;

/// A tone for a controller note.
fn tone(level: CaveatLevel) -> Tone {
    match level {
        CaveatLevel::Info => Tone::Info,
        CaveatLevel::Warning => Tone::Warning,
        CaveatLevel::Blocked => Tone::Danger,
    }
}

/// The target a mode writes; Write Image File has its own window.
fn slot_of(mode: RestoreMode) -> Option<TargetSlot> {
    match mode {
        RestoreMode::FullDisk => Some(TargetSlot::FullDisk),
        RestoreMode::SinglePartition => Some(TargetSlot::SinglePartition),
        RestoreMode::NewDisk => Some(TargetSlot::NewDisk),
        RestoreMode::WriteImage => None,
    }
}

pub struct RestoreScreen {
    ctrl: RefCell<RestoreController>,
    host: EffectHost,
    recent: RecentHandle,
    recent_files: RefCell<Vec<String>>,
    devices: RefCell<Vec<DiskDevice>>,
    /// Another tab's disk job, which locks this one.
    busy: Cell<Option<&'static str>>,
    /// The device list is being read again, so a target row may be about to change.
    refreshing: Cell<bool>,
    sync: SyncGuard,
    me: Weak<RestoreScreen>,
    root: gtk::Box,
    busy_note: gtk::Label,
    jobs: JobList,
    /// Everything that locks while a job runs.
    form: gtk::Box,
    mode: adw::ComboRow,
    source: SourceRows,
    write_image: WriteImageRows,
    target: TargetRows,
    new_disk: NewDiskRows,
    sizes: Sizes,
    caveats_group: adw::PreferencesGroup,
    mac_driver: adw::SwitchRow,
    caveats: Notes,
    target_line: gtk::Label,
    outcome: Notes,
    why: gtk::Label,
    bar: RunBar,
    start: gtk::Button,
    cancel: gtk::Button,
}

/// Everything a refresh shows, read while the controller is borrowed and shown after it is not.
struct Shown {
    mode: RestoreMode,
    jobs: Vec<(Snapshot, bool)>,
    source: SourceShown,
    write_image: WriteShown,
    target: TargetShown,
    new_disk: NewDiskShown,
    sizes: SizesShown,
    mac_driver: Option<bool>,
    caveats: Vec<(Tone, String)>,
    write_target: Option<String>,
    running: bool,
    cancelling: bool,
    can_start: bool,
    why: Option<String>,
    outcome: Option<Result<(), String>>,
    run: Option<Run>,
}

impl RestoreScreen {
    pub fn build(notify: Arc<dyn Notify>, recent: RecentHandle, host: EffectHost) -> Rc<Self> {
        Rc::new_cyclic(|me: &Weak<RestoreScreen>| {
            let target = me.clone();
            let send: ActionFn = Rc::new(move |action| {
                let Some(screen) = target.upgrade() else {
                    return;
                };
                if !screen.sync.active() {
                    screen.apply(action);
                }
            });

            let busy_note = line();
            busy_note.add_css_class("warning");
            let jobs = JobList::new(ids::JOB_CANCEL, Rc::new(|| {}));

            let labels: Vec<&str> = MODES.iter().map(|m| m.1).collect();
            let mode = combo("Mode", ids::MODE, &labels);
            let s = send.clone();
            mode.connect_selected_notify(move |row| {
                if let Some(m) = MODES.get(row.selected() as usize) {
                    s(RestoreAction::SetMode(m.0));
                }
            });
            let modes = group("");
            modes.add(&mode);

            let source = SourceRows::build(&send);
            let write_image = WriteImageRows::build(&send);
            let target_rows = TargetRows::build(&send);
            let new_disk = NewDiskRows::build(&send);
            let sizes = Sizes::build(&send);

            let caveats_group = group("");
            let mac_driver = switch(hints::MAC_DRIVER, ids::MAC_DRIVER);
            let s = send.clone();
            mac_driver.connect_active_notify(move |r| {
                s(RestoreAction::SetInstallMacDriver(r.is_active()))
            });
            let caveats = Notes::new();
            caveats_group.add(&mac_driver);
            caveats_group.add(caveats.root());

            let form = gtk::Box::new(gtk::Orientation::Vertical, 12);
            form.append(&modes);
            form.append(source.root());
            form.append(write_image.root());
            form.append(target_rows.root());
            form.append(new_disk.root());
            form.append(sizes.root());
            form.append(&caveats_group);

            let content = gtk::Box::new(gtk::Orientation::Vertical, 8);
            content.append(&busy_note);
            content.append(jobs.root());
            content.append(&form);
            let scroll = crate::form::page(&content);

            let target_line = line();
            target_line.add_css_class("heading");
            let outcome = Notes::new();
            let why = line();
            why.add_css_class("dim-label");
            let bar = RunBar::new();
            let start = button("Restore", ids::START);
            start.add_css_class("destructive-action");
            let screen = me.clone();
            start.connect_clicked(move |_| {
                if let Some(screen) = screen.upgrade() {
                    screen.start();
                }
            });
            let cancel = button("Cancel", ids::CANCEL);
            cancel.add_css_class("destructive-action");
            let s = send.clone();
            cancel.connect_clicked(move |_| s(RestoreAction::Cancel));
            let buttons = gtk::Box::new(gtk::Orientation::Horizontal, 6);
            buttons.append(&start);
            buttons.append(&cancel);
            buttons.append(&why);
            let footer = gtk::Box::new(gtk::Orientation::Vertical, 6);
            footer.set_margin_top(6);
            footer.set_margin_bottom(6);
            footer.set_margin_start(12);
            footer.set_margin_end(12);
            footer.append(outcome.root());
            footer.append(&target_line);
            footer.append(bar.root());
            footer.append(&buttons);

            let root = gtk::Box::new(gtk::Orientation::Vertical, 0);
            root.append(&scroll);
            root.append(&gtk::Separator::new(gtk::Orientation::Horizontal));
            root.append(&footer);

            let recent_files = recent.list(RecentMode::Restore);
            RestoreScreen {
                ctrl: RefCell::new(RestoreController::new(RestoreServices {
                    jobs: JobRegistry::new(),
                    notify,
                })),
                host,
                recent,
                recent_files: RefCell::new(recent_files),
                devices: RefCell::new(Vec::new()),
                busy: Cell::new(None),
                refreshing: Cell::new(false),
                sync: SyncGuard::default(),
                me: me.clone(),
                root,
                busy_note,
                jobs,
                form,
                mode,
                source,
                write_image,
                target: target_rows,
                new_disk,
                sizes,
                caveats_group,
                mac_driver,
                caveats,
                target_line,
                outcome,
                why,
                bar,
                start,
                cancel,
            }
        })
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    pub fn is_running(&self) -> bool {
        self.ctrl.borrow().is_running()
    }

    /// The app's device list; a target's row is reconciled before anything shows it.
    pub fn set_devices(&self, devices: &[DiskDevice], refreshing: bool) {
        self.ctrl.borrow_mut().reconcile_devices(devices);
        *self.devices.borrow_mut() = devices.to_vec();
        self.refreshing.set(refreshing);
    }

    /// Another tab's job, if any, which locks this tab's start controls.
    pub fn set_busy(&self, busy: Option<&'static str>) {
        self.busy.set(busy);
    }

    /// The backup folder open here, for the share with Inspect.
    pub fn loaded_backup(&self) -> Option<PathBuf> {
        self.ctrl.borrow().backup_folder().map(Path::to_path_buf)
    }

    /// Nothing is open, so a shared backup may be adopted.
    pub fn is_empty(&self) -> bool {
        let ctrl = self.ctrl.borrow();
        !ctrl.has_backup() && !ctrl.has_other_source()
    }

    pub fn load_backup(&self, folder: &Path) {
        self.apply(RestoreAction::OpenBackup(folder.to_path_buf()));
    }

    pub fn clear_backup(&self) {
        self.apply(RestoreAction::ClearBackup);
    }

    pub fn apply(&self, action: RestoreAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    pub fn tick(&self) {
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    /// The target is a local device, which a list being read could still move.
    fn writes_device(&self) -> bool {
        let ctrl = self.ctrl.borrow();
        let local = !(ctrl.mode() == RestoreMode::FullDisk && ctrl.remote_active());
        local && slot_of(ctrl.mode()).is_some_and(|slot| ctrl.target_is_device(slot))
    }

    /// Why the tab cannot start for a reason outside the controller.
    fn locked_by(&self) -> Option<String> {
        if let Some(tab) = self.busy.get() {
            return Some(busy_elsewhere_notice(tab));
        }
        let refreshing = self.refreshing.get() && self.writes_device();
        refreshing.then(|| "The device list is being read.".to_string())
    }

    /// Start's whole rule: asked again at the click, not only when the button was last drawn.
    fn may_start(&self) -> bool {
        self.ctrl.borrow().can_start() && self.locked_by().is_none()
    }

    fn start(&self) {
        if self.may_start() && !self.host.dialog_open() {
            self.apply(RestoreAction::Start);
        }
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
            Reply::Picked { purpose, path } => RestoreAction::Picked { purpose, path },
            Reply::Confirmed {
                purpose: asked,
                yes,
            } => {
                // A job another tab was waiting on can begin behind the question.
                let locked = self.locked_by().filter(|_| asked == purpose::RESTORE);
                if let Some(why) = locked.as_ref().filter(|_| yes) {
                    crate::app::append_log(vec![(
                        LogLevel::Warning,
                        format!("Restore not started. {why}"),
                    )]);
                }
                RestoreAction::Confirmed {
                    purpose: asked,
                    yes: yes && locked.is_none(),
                }
            }
            Reply::Answered { purpose, text } => RestoreAction::Answered { purpose, text },
            // The restore asks for no choice and has no screen to leave.
            Reply::Chose { .. } | Reply::CloseScreen => return,
        };
        self.apply(action);
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        let (requests, mut log) = {
            let mut ctrl = self.ctrl.borrow_mut();
            (ctrl.take_requests(), ctrl.take_log())
        };
        for request in requests {
            match request {
                HostRequest::RememberRecent(path) => self.remember(&path),
                // The button that sends this stays greyed out until the window exists.
                HostRequest::WriteImage(_) => log.push((
                    LogLevel::Warning,
                    "The Physical Disk Export window is coming soon.".to_string(),
                )),
            }
        }
        crate::app::append_log(log);
        // Off screen nothing needs it, and a job wakes this many times a second.
        if self.root.is_mapped() {
            self.refresh();
        }
    }

    fn remember(&self, path: &Path) {
        let list = self
            .recent
            .remember(RecentMode::Restore, &path.display().to_string());
        *self.recent_files.borrow_mut() = list;
    }

    fn read(&self) -> Shown {
        let devices = self.devices.borrow();
        let ctrl = self.ctrl.borrow();
        let mode = ctrl.mode();
        let mut jobs = Vec::new();
        for snap in [
            ctrl.decoding(),
            ctrl.loading(),
            ctrl.probing(),
            ctrl.target_scan(),
        ] {
            jobs.extend(snap.map(|s| (s, false)));
        }
        let mut caveats = Vec::new();
        if slot_of(mode).is_some() {
            for c in ctrl.size_caveats() {
                caveats.push((tone(c.caveat.level), c.line()));
            }
        }
        let locked = self.locked_by();
        let can_start = ctrl.can_start() && locked.is_none();
        Shown {
            mode,
            jobs,
            source: SourceShown::read(&ctrl, &self.recent_files.borrow()),
            write_image: WriteShown::read(&ctrl),
            target: TargetShown::read(&ctrl, devices.as_slice()),
            new_disk: NewDiskShown::read(&ctrl),
            sizes: SizesShown::read(&ctrl),
            mac_driver: ctrl.mac_driver_missing().then(|| ctrl.install_mac_driver()),
            caveats,
            write_target: ctrl.write_target(),
            running: ctrl.is_running(),
            cancelling: ctrl.cancelling(),
            can_start,
            why: ctrl.validate().err().or(locked),
            outcome: ctrl.last_result().cloned(),
            run: ctrl.progress().map(|b| Run {
                operation: b.operation,
                current: b.current_bytes,
                total: b.total_bytes,
                full: 0,
            }),
        }
    }

    /// Read the controller first and release it: setting a widget can fire its change signal.
    fn refresh(&self) {
        let shown = self.read();
        self.sync.run(|| self.show(shown));
    }

    fn show(&self, s: Shown) {
        let busy = self.busy.get();
        let notice = busy.map(busy_elsewhere_notice);
        set_line(&self.busy_note, notice.as_deref());
        self.form.set_sensitive(!s.running && busy.is_none());
        self.jobs.show(&s.jobs);

        let index = MODES.iter().position(|m| m.0 == s.mode).unwrap_or(0);
        set_combo(&self.mode, index as u32);
        let hover = MODES.get(index).map_or("", |m| m.2);
        self.mode
            .set_tooltip_text((!hover.is_empty()).then_some(hover));

        let writing_image = s.mode == RestoreMode::WriteImage;
        self.source.root().set_visible(!writing_image);
        self.source.show(s.source);
        self.write_image.root().set_visible(writing_image);
        self.write_image.show(s.write_image);
        self.target.show(s.target);
        self.new_disk
            .root()
            .set_visible(s.mode == RestoreMode::NewDisk);
        self.new_disk.show(s.new_disk);
        self.sizes.show(s.sizes, s.mode == RestoreMode::FullDisk);

        self.mac_driver.set_visible(s.mac_driver.is_some());
        set_switch(&self.mac_driver, s.mac_driver.unwrap_or(false));
        self.caveats_group
            .set_visible(s.mac_driver.is_some() || !s.caveats.is_empty());
        self.caveats.show(s.caveats);

        let shown = s.write_target.map(|t| format!("Writes to: {t}"));
        set_line(&self.target_line, shown.as_deref());
        self.start.set_visible(!s.running && !writing_image);
        self.start.set_sensitive(s.can_start);
        self.cancel.set_visible(s.running);
        self.cancel.set_sensitive(!s.cancelling);
        let label = if s.cancelling {
            "Cancelling..."
        } else {
            "Cancel"
        };
        if self.cancel.label().as_deref() != Some(label) {
            self.cancel.set_label(label);
        }
        self.bar.show(s.run.as_ref());
        set_text(&self.why, s.why.as_deref().unwrap_or(""));
        self.why
            .set_visible(!s.running && !s.can_start && !writing_image);
        self.outcome.show(match &s.outcome {
            Some(Ok(())) => vec![(Tone::Info, "Restore finished.".to_string())],
            Some(Err(e)) => vec![(Tone::Danger, e.clone())],
            None => Vec::new(),
        });
    }
}

/// "Choose target and write..." opens a window a later slice builds.
fn write_target_button() -> gtk::Button {
    let write = button("Choose target and write...", ids::WRITE_TARGET);
    coming(&write, "the Physical Disk Export window");
    write
}
