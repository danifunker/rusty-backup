//! The Backup tab: a retained view over `controller::backup::BackupController`.
//!
//! See docs/gtk4_shell_plan.md for the pattern; `src/gui/backup_tab.rs` is the reference. The form
//! scrolls, and the footer with Start, Cancel and the progress bar stays on screen. The CHD options
//! control is widget state; its choice travels with Start.

mod form;
mod partitions;
mod windows;

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use adw::prelude::*;
use rusty_backup::controller::app::{
    busy_elsewhere_notice, no_devices_hint, DiskDevice, Notify, RecentHandle, RecentMode, Snapshot,
};
use rusty_backup::controller::backup::{
    hints, ids, BackupAction, BackupController, BackupServices, ChdChoice, ChdOptions,
    CompressionType, HostRequest, JobRegistry,
};
use rusty_backup::controller::{Controller, Effect};

use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::{button, line, set_line, Notes, Tone};
use crate::progress::{JobList, Run, RunBar};
use crate::source::{Pick, PickerSpec, SourcePicker};
use crate::sync::SyncGuard;
use crate::widgets::set_text;

use form::{Options, OptionsShown};
use partitions::{GridShown, PartitionGrid};
use windows::{RemoteShown, VhdShown, Windows};

/// Sends an action to the controller; a change a sync made is dropped here, for every widget.
type ActionFn = Rc<dyn Fn(BackupAction)>;

pub struct BackupScreen {
    ctrl: RefCell<BackupController>,
    host: EffectHost,
    recent: RecentHandle,
    recent_files: RefCell<Vec<String>>,
    devices: RefCell<Vec<DiskDevice>>,
    /// Another tab's disk job, which locks this one.
    busy: Cell<Option<&'static str>>,
    /// The device list is being read again, so a source row may be about to change.
    refreshing: Cell<bool>,
    sync: SyncGuard,
    me: Weak<BackupScreen>,
    root: gtk::Box,
    busy_note: gtk::Label,
    /// Everything that locks while a job runs.
    form: gtk::Box,
    source: Rc<SourcePicker>,
    close: gtk::Button,
    clear_remote: gtk::Button,
    scan: gtk::Button,
    jobs: JobList,
    grid: PartitionGrid,
    options: Rc<Options>,
    windows: Windows,
    outcome: Notes,
    why: gtk::Label,
    bar: RunBar,
    start: gtk::Button,
    waiting: gtk::Spinner,
    cancel_pending: gtk::Button,
    cancel: gtk::Button,
}

/// Everything a refresh shows, read while the controller is borrowed and shown after it is not.
struct Shown {
    source: String,
    has_local: bool,
    remote: bool,
    can_scan: Option<(&'static str, &'static str)>,
    jobs: Vec<(Snapshot, bool)>,
    grid: GridShown,
    options: OptionsShown,
    running: bool,
    cancelling: bool,
    pending: bool,
    start_label: &'static str,
    can_start: bool,
    why: Option<String>,
    outcome: Option<Result<(), String>>,
    run: Option<Run>,
    vhd: Option<VhdShown>,
    remote_window: Option<RemoteShown>,
}

impl BackupScreen {
    pub fn build(notify: Arc<dyn Notify>, recent: RecentHandle, host: EffectHost) -> Rc<Self> {
        Rc::new_cyclic(|me: &Weak<BackupScreen>| {
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

            let source = SourcePicker::build(PickerSpec {
                source: ids::SOURCE,
                recent: ids::SOURCE_RECENT,
                device: ids::SOURCE_DEVICE,
                open_file: Some(ids::OPEN_IMAGE),
                open_folder: None,
                open_backup: None,
                connect: Some(ids::REMOTE_OPEN),
                connect_live: BackupController::remote_available(),
                no_devices: Some(no_devices_hint()),
                width_chars: 44,
                caption: Some("Source:"),
            });
            let target = me.clone();
            source.connect_pick(move |pick| {
                if let Some(screen) = target.upgrade() {
                    screen.picked_source(pick);
                }
            });
            let close = button("Close Drive", ids::CLOSE_SOURCE);
            close.set_tooltip_text(Some(hints::CLOSE_SOURCE));
            let s = send.clone();
            close.connect_clicked(move |_| s(BackupAction::CloseSource));
            let clear_remote = button("Disconnect", ids::CLEAR_REMOTE);
            clear_remote.set_tooltip_text(Some(hints::CLEAR_REMOTE));
            let s = send.clone();
            clear_remote.connect_clicked(move |_| s(BackupAction::RemoteClear));
            let scan = button("Load partition info", ids::LOAD_PARTITIONS);
            let s = send.clone();
            scan.connect_clicked(move |_| s(BackupAction::ScanPartitions));
            let source_bar = gtk::Box::new(gtk::Orientation::Horizontal, 6);
            source_bar.append(source.root());
            source_bar.append(&close);
            source_bar.append(&clear_remote);
            source_bar.append(&scan);

            let s = send.clone();
            let jobs = JobList::new(
                ids::CANCEL_SCAN,
                Rc::new(move || s(BackupAction::CancelScan)),
            );
            let grid = PartitionGrid::build(&send);
            let target = me.clone();
            let options = Options::build(
                &send,
                Rc::new(move || {
                    if let Some(screen) = target.upgrade() {
                        screen.refresh();
                    }
                }),
            );

            let form = gtk::Box::new(gtk::Orientation::Vertical, 12);
            form.append(&source_bar);
            form.append(grid.root());
            form.append(options.root());

            let content = gtk::Box::new(gtk::Orientation::Vertical, 8);
            content.append(&busy_note);
            content.append(jobs.root());
            content.append(&form);
            let scroll = crate::form::page(&content);

            let outcome = Notes::new();
            let why = line();
            why.add_css_class("dim-label");
            let bar = RunBar::new();
            let start = button("Start Backup", ids::START);
            start.add_css_class("suggested-action");
            let target = me.clone();
            start.connect_clicked(move |_| {
                if let Some(screen) = target.upgrade() {
                    screen.start();
                }
            });
            let waiting = gtk::Spinner::new();
            waiting.set_tooltip_text(Some(hints::WAITING_FOR_MINIMUMS));
            let cancel_pending = button("Cancel", ids::CANCEL_PENDING);
            let s = send.clone();
            cancel_pending.connect_clicked(move |_| s(BackupAction::CancelPendingStart));
            let cancel = button("Cancel", ids::CANCEL);
            cancel.add_css_class("destructive-action");
            let s = send.clone();
            cancel.connect_clicked(move |_| s(BackupAction::Cancel));
            let buttons = gtk::Box::new(gtk::Orientation::Horizontal, 6);
            buttons.append(&start);
            buttons.append(&waiting);
            buttons.append(&cancel_pending);
            buttons.append(&cancel);
            buttons.append(&why);
            let footer = gtk::Box::new(gtk::Orientation::Vertical, 6);
            footer.set_margin_top(6);
            footer.set_margin_bottom(6);
            footer.set_margin_start(12);
            footer.set_margin_end(12);
            footer.append(outcome.root());
            footer.append(bar.root());
            footer.append(&buttons);

            let root = gtk::Box::new(gtk::Orientation::Vertical, 0);
            root.append(&scroll);
            root.append(&gtk::Separator::new(gtk::Orientation::Horizontal));
            root.append(&footer);

            let recent_files = recent.list(RecentMode::Backup);
            let windows = Windows::new(host.clone(), send);
            BackupScreen {
                ctrl: RefCell::new(BackupController::new(BackupServices {
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
                form,
                source,
                close,
                clear_remote,
                scan,
                jobs,
                grid,
                options,
                windows,
                outcome,
                why,
                bar,
                start,
                waiting,
                cancel_pending,
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

    /// The app's device list; a change under the open device closes or re-reads it.
    pub fn set_devices(&self, devices: &[DiskDevice], refreshing: bool) {
        self.ctrl.borrow_mut().reconcile_devices(devices);
        *self.devices.borrow_mut() = devices.to_vec();
        self.refreshing.set(refreshing);
    }

    /// Another tab's job, if any, which locks this tab's start controls.
    pub fn set_busy(&self, busy: Option<&'static str>) {
        self.busy.set(busy);
    }

    pub fn apply(&self, action: BackupAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    pub fn tick(&self) {
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    /// What Start carries, from the options control.
    fn chd_choice(&self) -> Option<ChdChoice> {
        let chd = self.ctrl.borrow().options().compression == CompressionType::Chd;
        chd.then(|| {
            let (options, custom): (ChdOptions, bool) = self.options.chd().choice();
            ChdChoice { options, custom }
        })
    }

    fn device_source(&self) -> bool {
        self.ctrl.borrow().device_slot().is_some()
    }

    /// Start's whole rule: asked again at the click, not only when the button was last drawn.
    fn may_start(&self) -> bool {
        let ctrl = self.ctrl.borrow();
        let chd_ok =
            ctrl.options().compression != CompressionType::Chd || self.options.chd().is_valid();
        ctrl.can_start() && chd_ok && self.locked_by().is_none()
    }

    /// Why the tab cannot start for a reason outside the controller.
    fn locked_by(&self) -> Option<String> {
        if let Some(tab) = self.busy.get() {
            return Some(busy_elsewhere_notice(tab));
        }
        let refreshing = self.refreshing.get() && self.device_source();
        refreshing.then(|| "The device list is being read.".to_string())
    }

    fn start(&self) {
        if self.may_start() {
            let chd = self.chd_choice();
            self.apply(BackupAction::Start(chd));
        }
    }

    fn picked_source(&self, pick: Pick) {
        let action = match pick {
            Pick::Recent(path) => BackupAction::OpenImage { path, rename: true },
            Pick::Device(slot) => {
                let Some(device) = self.devices.borrow().get(slot).cloned() else {
                    return;
                };
                BackupAction::SelectDevice { slot, device }
            }
            Pick::OpenFile => BackupAction::PickImage,
            Pick::Connect => BackupAction::RemoteOpen,
            // Backup's picker offers neither.
            Pick::OpenFolder | Pick::OpenBackup => return,
        };
        self.apply(action);
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
            Reply::Picked { purpose, path } => BackupAction::Picked { purpose, path },
            // Backup only ever opens pickers.
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
            }
        }
        crate::app::append_log(log);
        // Off screen nothing needs it, and a job wakes this many times a second.
        if self.root.is_mapped() {
            self.refresh();
        }
    }

    fn remember(&self, path: PathBuf) {
        let list = self
            .recent
            .remember(RecentMode::Backup, &path.display().to_string());
        *self.recent_files.borrow_mut() = list;
    }

    fn read(&self) -> Shown {
        let devices = self.devices.borrow();
        let ctrl = self.ctrl.borrow();
        let mut jobs = Vec::new();
        jobs.extend(ctrl.decoding().map(|s| (s, false)));
        jobs.extend(ctrl.scanning().map(|s| (s, true)));
        let locked = self.locked_by();
        Shown {
            source: ctrl.source_label(devices.as_slice()),
            has_local: ctrl.has_local_source(),
            remote: ctrl.remote_active(),
            can_scan: ctrl.can_scan_device().then(|| ctrl.scan_button()),
            jobs,
            grid: GridShown::read(&ctrl),
            options: OptionsShown::read(&ctrl),
            running: ctrl.is_running(),
            cancelling: ctrl.cancelling(),
            pending: ctrl.start_pending(),
            start_label: ctrl.start_label(),
            can_start: self.may_start(),
            why: ctrl.validate().err().or(locked),
            outcome: ctrl.last_result().cloned(),
            run: ctrl.progress().map(|b| Run {
                operation: b.operation,
                current: b.current_bytes,
                total: b.total_bytes,
                full: b.full_size_bytes,
            }),
            vhd: VhdShown::read(&ctrl),
            remote_window: RemoteShown::read(&ctrl),
        }
    }

    /// Read the controller first and release it: setting a widget can fire its change signal.
    fn refresh(&self) {
        let shown = self.read();
        self.sync.run(|| self.show(shown));
    }

    fn show(&self, s: Shown) {
        let devices: Vec<String> = self
            .devices
            .borrow()
            .iter()
            .map(|d| d.display_name())
            .collect();
        self.source
            .show(&s.source, &self.recent_files.borrow(), &devices);

        let busy = self.busy.get();
        let notice = busy.map(busy_elsewhere_notice);
        set_line(&self.busy_note, notice.as_deref());
        self.form.set_sensitive(!s.running && busy.is_none());
        self.close.set_visible(s.has_local);
        self.clear_remote.set_visible(s.remote);
        self.scan.set_visible(s.can_scan.is_some());
        if let Some((label, hint)) = s.can_scan {
            self.scan.set_label(label);
            self.scan.set_tooltip_text(Some(hint));
        }
        self.jobs.show(&s.jobs);
        self.grid.show(s.grid);
        self.options.show(s.options);

        self.start.set_visible(!s.running);
        self.start.set_sensitive(s.can_start);
        if self.start.label().as_deref() != Some(s.start_label) {
            self.start.set_label(s.start_label);
        }
        self.waiting.set_visible(s.pending);
        self.waiting.set_spinning(s.pending);
        self.cancel_pending.set_visible(s.pending);
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
        self.why.set_visible(!s.running && !s.can_start);
        self.outcome.show(match &s.outcome {
            Some(Ok(())) => vec![(Tone::Info, "Backup finished.".to_string())],
            Some(Err(e)) => vec![(Tone::Danger, e.clone())],
            None => Vec::new(),
        });
        self.windows.show(s.vhd, s.remote_window, busy.is_none());
    }
}
