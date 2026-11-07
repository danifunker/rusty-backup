//! The Optical tab: a retained view over `controller::optical::OpticalController`.
//!
//! See docs/gtk4_shell_plan.md for the pattern; `src/gui/optical_tab.rs` is the reference. The form
//! scrolls and the footer with Start and Cancel stays on screen. The disc browser takes the form's
//! place while it is open, so a run under it stays in view. A drive is unit-tested only.

mod form;
mod windows;

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::time::Duration;

use adw::prelude::*;
use gtk::glib;
use rusty_backup::controller::app::{
    busy_elsewhere_notice, Notify, RecentHandle, RecentMode, Snapshot,
};
use rusty_backup::controller::keys::{Key, Mods};
use rusty_backup::controller::optical::{
    ids, ChdChoice, HostRequest, OpticalAction, OpticalController, OutputFormat,
};
use rusty_backup::controller::{Controller, Effect};

use crate::browser::BrowseView;
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::{button, line, set_line, Notes, Tone};
use crate::progress::{JobList, Run, RunBar};
use crate::sync::SyncGuard;
use crate::widgets::{keep_focus, set_text};

use form::{DiscBar, DiscShown, OutputForm, OutputShown, SourceForm, SourceShown};
use windows::{AudioShown, RemoteShown, Windows};

/// Sends an action to the controller; a change a sync made is dropped here, for every widget.
type ActionFn = Rc<dyn Fn(OpticalAction)>;

/// How often the player's readouts move: time passes with no job to wake the shell.
const PLAYER_TICK: Duration = Duration::from_millis(250);

pub struct OpticalScreen {
    ctrl: RefCell<OpticalController>,
    host: EffectHost,
    recent: RecentHandle,
    recent_files: RefCell<Vec<String>>,
    /// Another tab's disk job, which locks this one.
    busy: Cell<Option<&'static str>>,
    player_timer: Cell<bool>,
    sync: SyncGuard,
    me: Weak<OpticalScreen>,
    root: gtk::Box,
    pages: gtk::Stack,
    browse: Rc<BrowseView>,
    busy_note: gtk::Label,
    source: Rc<SourceForm>,
    jobs: JobList,
    disc: DiscBar,
    output: Rc<OutputForm>,
    windows: Windows,
    outcome: Notes,
    target: gtk::Label,
    why: gtk::Label,
    bar: RunBar,
    start: gtk::Button,
    cancel: gtk::Button,
}

/// Everything a refresh shows, read while the controller is borrowed and shown after it is not.
struct Shown {
    source: SourceShown,
    jobs: Vec<(Snapshot, bool)>,
    disc: DiscShown,
    output: OutputShown,
    running: bool,
    cancelling: bool,
    start_label: &'static str,
    can_start: bool,
    why: Option<String>,
    target: Option<String>,
    outcome: Option<Result<(), String>>,
    run: Option<Run>,
    chd_info: Option<String>,
    remote: Option<RemoteShown>,
    audio: Option<AudioShown>,
}

impl OpticalScreen {
    pub fn build(notify: Arc<dyn Notify>, recent: RecentHandle, host: EffectHost) -> Rc<Self> {
        Rc::new_cyclic(|me: &Weak<OpticalScreen>| {
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
            let source = SourceForm::build(&send);
            let jobs = JobList::new(ids::CANCEL, Rc::new(|| {}));
            let disc = DiscBar::build(&send);
            let target = me.clone();
            let output = OutputForm::build(
                &send,
                Rc::new(move || {
                    if let Some(screen) = target.upgrade() {
                        screen.refresh();
                    }
                }),
            );

            let content = gtk::Box::new(gtk::Orientation::Vertical, 8);
            content.append(&busy_note);
            content.append(source.root());
            content.append(jobs.root());
            content.append(disc.root());
            content.append(output.root());
            let scroll = crate::form::page(&content);

            let browse = BrowseView::disc(notify.clone(), host.clone());
            let target = me.clone();
            browse.on_change(Rc::new(move || {
                if let Some(screen) = target.upgrade() {
                    screen.browser_changed();
                }
            }));
            // Clipped: a wide header wraps rather than widens the window.
            let clip = gtk::ScrolledWindow::builder()
                .child(browse.root())
                .hscrollbar_policy(gtk::PolicyType::External)
                .vscrollbar_policy(gtk::PolicyType::Never)
                .vexpand(true)
                .build();
            let pages = gtk::Stack::new();
            pages.add_named(&scroll, Some("form"));
            pages.add_named(&clip, Some("browse"));

            let outcome = Notes::new();
            let write_to = line();
            write_to.set_widget_name(ids::WRITE_TARGET);
            let why = line();
            why.add_css_class("dim-label");
            let bar = RunBar::new();
            let start = button("Start Convert", ids::START);
            start.add_css_class("suggested-action");
            let target = me.clone();
            start.connect_clicked(move |_| {
                if let Some(screen) = target.upgrade() {
                    screen.start();
                }
            });
            let cancel = button("Cancel", ids::CANCEL);
            cancel.add_css_class("destructive-action");
            let s = send.clone();
            cancel.connect_clicked(move |_| s(OpticalAction::Cancel));
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
            footer.append(&write_to);
            footer.append(bar.root());
            footer.append(&buttons);

            let root = gtk::Box::new(gtk::Orientation::Vertical, 0);
            root.append(&pages);
            root.append(&gtk::Separator::new(gtk::Orientation::Horizontal));
            root.append(&footer);
            keep_focus(&footer);
            let target = me.clone();
            root.connect_map(move |_| {
                if let Some(screen) = target.upgrade() {
                    screen.refresh();
                }
            });

            let recent_files = recent.list(RecentMode::Optical);
            let windows = Windows::new(host.clone(), send);
            OpticalScreen {
                ctrl: RefCell::new(OpticalController::new(notify)),
                host,
                recent,
                recent_files: RefCell::new(recent_files),
                busy: Cell::new(None),
                player_timer: Cell::new(false),
                sync: SyncGuard::default(),
                me: me.clone(),
                root,
                pages,
                browse,
                busy_note,
                source,
                jobs,
                disc,
                output,
                windows,
                outcome,
                target: write_to,
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

    /// Another tab's job, if any, which locks this tab's start controls.
    pub fn set_busy(&self, busy: Option<&'static str>) {
        self.busy.set(busy);
    }

    /// Scan the drives at start-up; the count reaches the log when the scan ends.
    pub fn find_drives(&self) {
        self.apply(OpticalAction::FindDrives);
    }

    /// Open `path` as the image-file source (Inspect's hand-off).
    pub fn is_browsing(&self) -> bool {
        self.ctrl.borrow().is_browsing()
    }

    pub fn open_image(&self, path: PathBuf) {
        self.apply(OpticalAction::OpenImage(path));
    }

    /// A window-wide key: the disc browser's, while it is open.
    pub fn key(&self, key: Key, mods: Mods) -> bool {
        self.browse.key(key, mods)
    }

    pub fn apply(&self, action: OpticalAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    pub fn tick(&self) {
        self.browse.tick();
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    /// The disc browser opened or closed; a close the user made tells the controller.
    fn browser_changed(&self) {
        let page = match self.browse.is_active() {
            true => "browse",
            false => "form",
        };
        if self.pages.visible_child_name().as_deref() != Some(page) {
            self.pages.set_visible_child_name(page);
        }
        let closed = self.ctrl.borrow().is_browsing() && !self.browse.is_active();
        if closed {
            self.apply(OpticalAction::BrowserClosed);
        }
    }

    /// What Start carries, from the options control.
    fn chd_choice(&self) -> Option<ChdChoice> {
        let chd = self.ctrl.borrow().output_format() == OutputFormat::Chd;
        chd.then(|| {
            let (options, custom) = self.output.chd().choice();
            ChdChoice { options, custom }
        })
    }

    /// Start's whole rule: asked again at the click, not only when the button was last drawn.
    fn may_start(&self) -> bool {
        let ctrl = self.ctrl.borrow();
        let chd_ok = ctrl.output_format() != OutputFormat::Chd || self.output.chd().is_valid();
        ctrl.can_start() && chd_ok && self.busy.get().is_none()
    }

    fn start(&self) {
        if self.may_start() {
            let chd = self.chd_choice();
            self.apply(OpticalAction::Start(chd));
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
            Reply::Picked { purpose, path } => OpticalAction::Picked { purpose, path },
            Reply::Confirmed { purpose, yes } => OpticalAction::Confirmed { purpose, yes },
            // Optical asks for neither text nor a choice and has no screen to leave.
            Reply::Answered { .. } | Reply::Chose { .. } | Reply::CloseScreen => return,
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
                HostRequest::OpenBrowser(src) => self.browse.open_disc(src),
                HostRequest::CloseBrowser => self.browse.close(),
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
            .remember(RecentMode::Optical, &path.display().to_string());
        *self.recent_files.borrow_mut() = list;
    }

    fn read(&self) -> Shown {
        let ctrl = self.ctrl.borrow();
        let mut jobs = Vec::new();
        jobs.extend(ctrl.detecting().map(|s| (s, false)));
        let running = ctrl.is_running();
        let chd_error = (ctrl.output_format() == OutputFormat::Chd
            && !self.output.chd().is_valid())
        .then(|| "Check the CHD options.".to_string());
        let locked = self.busy.get().map(busy_elsewhere_notice);
        Shown {
            source: SourceShown::read(&ctrl, self.recent_files.borrow().clone()),
            jobs,
            disc: DiscShown::read(&ctrl),
            output: OutputShown::read(&ctrl),
            running,
            cancelling: ctrl.cancelling(),
            start_label: ctrl.start_label(),
            can_start: self.may_start(),
            why: ctrl.start_refusal().or(chd_error).or(locked),
            target: ctrl.write_target(),
            outcome: ctrl.last_result().cloned(),
            run: ctrl.progress().map(|b| Run {
                operation: b.operation,
                current: b.current_bytes,
                total: b.total_bytes,
                full: 0,
            }),
            chd_info: ctrl.chd_info().map(str::to_string),
            remote: RemoteShown::read(&ctrl),
            audio: AudioShown::read(&ctrl),
        }
    }

    /// Read the controller first and release it: setting a widget can fire its change signal.
    fn refresh(&self) {
        let shown = self.read();
        let playing = shown.disc.playing();
        self.sync.run(|| self.show(shown));
        if playing {
            self.wake_for_player();
        }
    }

    /// A track's position moves with no job to wake the shell, so a one-shot does while it plays.
    fn wake_for_player(&self) {
        if self.player_timer.replace(true) {
            return;
        }
        let me = self.me.clone();
        glib::timeout_add_local_once(PLAYER_TICK, move || {
            if let Some(screen) = me.upgrade() {
                screen.player_timer.set(false);
                screen.tick();
            }
        });
    }

    fn show(&self, s: Shown) {
        let busy = self.busy.get();
        let notice = busy.map(busy_elsewhere_notice);
        set_line(&self.busy_note, notice.as_deref());
        let locked = s.running || busy.is_some();
        self.source.root().set_sensitive(!locked);
        self.output.root().set_sensitive(!locked);
        self.source.show(s.source);
        self.jobs.show(&s.jobs);
        self.disc.show(s.disc);
        self.output.show(s.output);

        self.start.set_visible(!s.running);
        self.start.set_sensitive(s.can_start);
        if self.start.label().as_deref() != Some(s.start_label) {
            self.start.set_label(s.start_label);
        }
        self.cancel.set_visible(s.running);
        self.cancel.set_sensitive(!s.cancelling);
        let label = match s.cancelling {
            true => "Cancelling...",
            false => "Cancel",
        };
        if self.cancel.label().as_deref() != Some(label) {
            self.cancel.set_label(label);
        }
        self.bar.show(s.run.as_ref());
        let writes = s.target.map(|t| format!("Writes: {t}"));
        set_line(&self.target, writes.as_deref());
        set_text(&self.why, s.why.as_deref().unwrap_or(""));
        self.why.set_visible(!s.running && !s.can_start);
        self.outcome.show(match &s.outcome {
            Some(Ok(())) => vec![(Tone::Info, "Finished.".to_string())],
            Some(Err(e)) => vec![(Tone::Danger, e.clone())],
            None => Vec::new(),
        });
        self.windows.show(s.chd_info, s.remote, s.audio);
    }
}
