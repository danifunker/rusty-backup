//! The Build Disk tab: a retained view over `controller::build_disk::BuildDiskController`.
//!
//! See docs/gtk4_shell_plan.md for the pattern; `src/gui/build_disk_tab.rs` is the reference. The
//! steps, what blocks Next and the build are the controller's: a step strip, one page per step and
//! Back / Next under them. A device build starts only on the controller's typed-path check.

mod disk;
mod options;
mod output;
mod review;
mod rows;

use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};
use std::sync::Arc;

use adw::prelude::*;
use rusty_backup::controller::app::{busy_elsewhere_notice, DiskDevice, Notify};
use rusty_backup::controller::build_disk::{
    ids, BuildDiskAction, BuildDiskController, DiskKind, FormatTarget, Step,
};
use rusty_backup::controller::{Controller, Effect};

use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::{button, line, set_line};
use crate::sync::SyncGuard;
use crate::widgets::set_text;

use disk::{DiskShown, DiskStep};
use options::OptionsStep;
use output::{OutputShown, OutputStep};
use review::{BuildShown, BuildStep, KindStep, ReviewShown, ReviewStep};
use rows::{RowsShown, RowsStep};

/// Sends an action to the controller; a change a sync made is dropped here, for every widget.
type ActionFn = Rc<dyn Fn(BuildDiskAction)>;

/// What the page on screen shows; only that page is read.
enum Panel {
    Kind(DiskKind),
    Output(OutputShown),
    Disk(Box<DiskShown>),
    Partitions(Box<RowsShown>),
    Options(Vec<FormatTarget>),
    Review(ReviewShown),
    Build(BuildShown),
}

/// Numbered steps: those passed are links back, the one on screen is bold.
struct Strip {
    root: gtk::Box,
    shown: RefCell<Option<(Vec<Step>, usize, bool)>>,
    send: ActionFn,
}

impl Strip {
    fn new(send: &ActionFn) -> Strip {
        let root = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        root.set_margin_top(6);
        root.set_margin_start(12);
        root.set_margin_end(12);
        root.set_widget_name(ids::STEP);
        Strip {
            root,
            shown: RefCell::default(),
            send: send.clone(),
        }
    }

    fn show(&self, steps: &[Step], here: Step, running: bool) {
        let at = steps.iter().position(|s| *s == here).unwrap_or(0);
        let now = (steps.to_vec(), at, running);
        if self.shown.borrow().as_ref() == Some(&now) {
            return;
        }
        *self.shown.borrow_mut() = Some(now);
        while let Some(child) = self.root.first_child() {
            self.root.remove(&child);
        }
        for (i, step) in steps.iter().enumerate() {
            if i > 0 {
                let arrow = gtk::Label::new(Some("->"));
                arrow.add_css_class("dim-label");
                self.root.append(&arrow);
            }
            let text = format!("{}. {}", i + 1, step.label());
            if i < at && !running && *step != Step::Build {
                let link = gtk::Button::with_label(&text);
                link.add_css_class("flat");
                link.set_widget_name(&format!("{}.{i}", ids::STEP));
                link.set_tooltip_text(Some("Go back to this step"));
                let (send, step) = (self.send.clone(), *step);
                link.connect_clicked(move |_| send(BuildDiskAction::GoTo(step)));
                self.root.append(&link);
            } else {
                let label = gtk::Label::new(Some(&text));
                label.add_css_class(if i == at { "heading" } else { "dim-label" });
                self.root.append(&label);
            }
        }
    }
}

pub struct BuildDiskScreen {
    ctrl: RefCell<BuildDiskController>,
    host: EffectHost,
    devices: RefCell<Vec<DiskDevice>>,
    /// Another tab's disk job, which locks this one.
    busy: Cell<Option<&'static str>>,
    /// The device list is being read again, so the target's row may be about to change.
    refreshing: Cell<bool>,
    sync: SyncGuard,
    me: Weak<BuildDiskScreen>,
    root: gtk::Box,
    busy_note: gtk::Label,
    strip: Strip,
    page: gtk::ScrolledWindow,
    pages: gtk::Stack,
    on_screen: Cell<Option<Step>>,
    kind: KindStep,
    output: OutputStep,
    disk: DiskStep,
    rows: RowsStep,
    options: OptionsStep,
    review: ReviewStep,
    build: BuildStep,
    nav: gtk::Box,
    back: gtk::Button,
    next: gtk::Button,
    why: gtk::Label,
}

/// Everything a refresh shows, read while the controller is borrowed and shown after it is not.
struct Shown {
    steps: Vec<Step>,
    step: Step,
    running: bool,
    can_next: bool,
    why: Option<String>,
    panel: Panel,
}

impl BuildDiskScreen {
    pub fn build(notify: Arc<dyn Notify>, host: EffectHost) -> Rc<Self> {
        Rc::new_cyclic(|me: &Weak<BuildDiskScreen>| {
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
            let strip = Strip::new(&send);

            let kind = KindStep::build(&send);
            let output = OutputStep::build(&send);
            let disk = DiskStep::build(&send);
            let rows = RowsStep::build(&send);
            let options = OptionsStep::build(&send);
            let review = ReviewStep::build(&send);
            let build = BuildStep::build(&send);

            let pages = gtk::Stack::builder()
                .hhomogeneous(false)
                .vhomogeneous(false)
                .build();
            pages.add_named(kind.root(), Some(Step::Kind.label()));
            pages.add_named(output.root(), Some(Step::Output.label()));
            pages.add_named(disk.root(), Some(Step::Disk.label()));
            pages.add_named(rows.root(), Some(Step::Partitions.label()));
            pages.add_named(options.root(), Some(Step::Options.label()));
            pages.add_named(review.root(), Some(Step::Review.label()));
            pages.add_named(build.root(), Some(Step::Build.label()));

            let content = gtk::Box::new(gtk::Orientation::Vertical, 8);
            content.append(&busy_note);
            content.append(&pages);
            let page = crate::form::page(&content);

            let why = line();
            why.add_css_class("warning");
            let back = button("Back", ids::BACK);
            let s = send.clone();
            back.connect_clicked(move |_| s(BuildDiskAction::Back));
            let next = button("Next", ids::NEXT);
            next.add_css_class("suggested-action");
            let screen = me.clone();
            next.connect_clicked(move |_| {
                if let Some(screen) = screen.upgrade() {
                    screen.next();
                }
            });
            let nav = gtk::Box::new(gtk::Orientation::Horizontal, 6);
            nav.set_margin_top(6);
            nav.set_margin_bottom(6);
            nav.set_margin_start(12);
            nav.set_margin_end(12);
            nav.append(&back);
            nav.append(&next);
            nav.append(&why);

            let root = gtk::Box::new(gtk::Orientation::Vertical, 0);
            root.append(&strip.root);
            root.append(&gtk::Separator::new(gtk::Orientation::Horizontal));
            root.append(&page);
            root.append(&gtk::Separator::new(gtk::Orientation::Horizontal));
            root.append(&nav);

            BuildDiskScreen {
                ctrl: RefCell::new(BuildDiskController::new(notify)),
                host,
                devices: RefCell::new(Vec::new()),
                busy: Cell::new(None),
                refreshing: Cell::new(false),
                sync: SyncGuard::default(),
                me: me.clone(),
                root,
                busy_note,
                strip,
                page,
                pages,
                on_screen: Cell::new(None),
                kind,
                output,
                disk,
                rows,
                options,
                review,
                build,
                nav,
                back,
                next,
                why,
            }
        })
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    pub fn is_running(&self) -> bool {
        self.ctrl.borrow().is_running()
    }

    /// The app's device list; the target's row is reconciled before anything shows it.
    pub fn set_devices(&self, devices: &[DiskDevice], refreshing: bool) {
        self.ctrl.borrow_mut().reconcile_devices(devices);
        *self.devices.borrow_mut() = devices.to_vec();
        self.refreshing.set(refreshing);
    }

    /// Another tab's job, if any, which locks this tab's controls.
    pub fn set_busy(&self, busy: Option<&'static str>) {
        self.busy.set(busy);
    }

    pub fn apply(&self, action: BuildDiskAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    pub fn tick(&self) {
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    /// Why the tab cannot go on for a reason outside the controller.
    fn locked_by(&self) -> Option<String> {
        if let Some(tab) = self.busy.get() {
            return Some(busy_elsewhere_notice(tab));
        }
        let refreshing = self.refreshing.get() && self.ctrl.borrow().output_is_device();
        refreshing.then(|| "The device list is being read.".to_string())
    }

    /// Next's whole rule: asked again at the click, not only when the button was last drawn.
    fn next(&self) {
        let open = self.ctrl.borrow().next_blocked().is_none();
        if open && self.locked_by().is_none() && !self.host.dialog_open() {
            self.apply(BuildDiskAction::Next);
        }
    }

    /// Answers to this controller's own questions.
    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            let Some(screen) = me.upgrade() else {
                return;
            };
            match reply {
                Reply::Picked { purpose, path } => {
                    screen.apply(BuildDiskAction::Picked { purpose, path })
                }
                // The wizard asks for files, and which NTFS version a new NTFS target gets.
                Reply::Chose { purpose, choice } => {
                    screen.apply(BuildDiskAction::Chose { purpose, choice })
                }
                Reply::Confirmed { .. } | Reply::Answered { .. } | Reply::CloseScreen => {}
            }
        })
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        let log = self.ctrl.borrow_mut().take_log();
        crate::app::append_log(log);
        // Off screen nothing needs it, and a job wakes this many times a second.
        if self.root.is_mapped() {
            self.refresh();
        }
    }

    fn read(&self) -> Shown {
        let locked = self.locked_by();
        let devices = self.devices.borrow();
        let ctrl = self.ctrl.borrow();
        let step = ctrl.step();
        let blocked = ctrl.next_blocked();
        let panel = match step {
            Step::Kind => Panel::Kind(ctrl.kind()),
            Step::Output => Panel::Output(OutputShown::read(&ctrl, devices.as_slice())),
            Step::Disk => Panel::Disk(Box::new(DiskShown::read(&ctrl))),
            Step::Partitions => Panel::Partitions(Box::new(RowsShown::read(&ctrl))),
            Step::Options => Panel::Options(ctrl.format_targets()),
            Step::Review => Panel::Review(ReviewShown::read(&ctrl)),
            Step::Build => Panel::Build(BuildShown::read(&ctrl)),
        };
        Shown {
            steps: ctrl.steps(),
            step,
            running: ctrl.is_running(),
            can_next: blocked.is_none() && locked.is_none(),
            why: blocked.or(locked),
            panel,
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
        let enabled = busy.is_none() || s.step == Step::Build;
        self.pages.set_sensitive(enabled);
        self.strip.root.set_sensitive(enabled);
        self.strip.show(&s.steps, s.step, s.running);

        match s.panel {
            Panel::Kind(kind) => self.kind.show(kind),
            Panel::Output(o) => self.output.show(o),
            Panel::Disk(d) => self.disk.show(*d),
            Panel::Partitions(r) => self.rows.show(*r),
            Panel::Options(t) => self.options.show(t),
            Panel::Review(r) => self.review.show(r),
            Panel::Build(b) => self.build.show(b),
        }
        if self.on_screen.replace(Some(s.step)) != Some(s.step) {
            self.pages.set_visible_child_name(s.step.label());
            self.page.vadjustment().set_value(0.0);
        }

        self.nav.set_visible(s.step != Step::Build);
        self.back
            .set_sensitive(enabled && s.step != Step::Kind && !s.running);
        self.next.set_sensitive(s.can_next);
        let label = match s.step {
            Step::Review => "Create Disk",
            _ => "Next",
        };
        if self.next.label().as_deref() != Some(label) {
            self.next.set_label(label);
        }
        self.next.set_widget_name(match s.step {
            Step::Review => ids::CREATE,
            _ => ids::NEXT,
        });
        self.why.set_visible(s.why.is_some());
        set_text(&self.why, s.why.as_deref().unwrap_or(""));
    }
}
