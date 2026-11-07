//! Running jobs on screen: the step, a bar (a spinner without a total), and the detail line.

use std::cell::RefCell;
use std::rc::Rc;

use adw::prelude::*;
use rusty_backup::controller::app::{progress_text, RateTracker, Snapshot};

use crate::widgets::set_text;

struct JobRow {
    root: gtk::Box,
    phase: gtk::Label,
    spinner: gtk::Spinner,
    cancel: gtk::Button,
    cancelling: gtk::Label,
    bar: gtk::ProgressBar,
    detail: gtk::Label,
}

impl JobRow {
    fn new(cancel_id: &str, on_cancel: Rc<dyn Fn()>) -> JobRow {
        // Fixed width: a phase naming each file must not walk Cancel away from the pointer.
        let phase = gtk::Label::builder()
            .xalign(0.0)
            .hexpand(true)
            .ellipsize(gtk::pango::EllipsizeMode::Middle)
            .build();
        let spinner = gtk::Spinner::new();
        let cancel = gtk::Button::with_label("Cancel");
        cancel.add_css_class("flat");
        cancel.set_widget_name(cancel_id);
        cancel.connect_clicked(move |_| on_cancel());
        let cancelling = gtk::Label::new(Some("(cancelling)"));
        cancelling.add_css_class("dim-label");
        let head = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        head.append(&spinner);
        head.append(&phase);
        head.append(&cancelling);
        head.append(&cancel);
        let bar = gtk::ProgressBar::builder().show_text(true).build();
        let detail = gtk::Label::builder().xalign(0.0).build();
        detail.add_css_class("dim-label");
        detail.add_css_class("caption");
        let root = gtk::Box::new(gtk::Orientation::Vertical, 2);
        root.append(&head);
        root.append(&bar);
        root.append(&detail);
        JobRow {
            root,
            phase,
            spinner,
            cancel,
            cancelling,
            bar,
            detail,
        }
    }

    fn show(&self, snap: &Snapshot, cancellable: bool) {
        let phase = if snap.phase.is_empty() {
            snap.label.as_str()
        } else {
            snap.phase.as_str()
        };
        set_text(&self.phase, phase);
        set_text(&self.detail, &snap.detail());
        self.cancelling.set_visible(snap.cancel_requested);
        self.cancel
            .set_visible(cancellable && snap.can_cancel && !snap.cancel_requested);
        let fraction = snap.progress.fraction();
        self.bar.set_visible(fraction.is_some());
        self.spinner.set_visible(fraction.is_none());
        self.spinner.set_spinning(fraction.is_none());
        if let Some(f) = fraction {
            self.bar.set_fraction(f64::from(f));
        }
    }
}

/// A stack of job rows, kept and reused as jobs come and go.
pub struct JobList {
    root: gtk::Box,
    rows: RefCell<Vec<JobRow>>,
    cancel_id: &'static str,
    on_cancel: Rc<dyn Fn()>,
}

impl JobList {
    pub fn new(cancel_id: &'static str, on_cancel: Rc<dyn Fn()>) -> JobList {
        JobList {
            root: gtk::Box::new(gtk::Orientation::Vertical, 6),
            rows: RefCell::new(Vec::new()),
            cancel_id,
            on_cancel,
        }
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    /// Show `jobs`, each with whether its Cancel button has an action behind it.
    pub fn show(&self, jobs: &[(Snapshot, bool)]) {
        let mut rows = self.rows.borrow_mut();
        while rows.len() < jobs.len() {
            let row = JobRow::new(self.cancel_id, self.on_cancel.clone());
            self.root.append(&row.root);
            rows.push(row);
        }
        for (i, row) in rows.iter().enumerate() {
            row.root.set_visible(i < jobs.len());
            if let Some((snap, cancellable)) = jobs.get(i) {
                row.show(snap, *cancellable);
            }
        }
        self.root.set_visible(!jobs.is_empty());
    }
}

/// One running backup or restore, as the footer's bar draws it.
pub struct Run {
    pub operation: String,
    pub current: u64,
    pub total: u64,
    /// The untrimmed size, larger than `total` while compaction saves space.
    pub full: u64,
}

/// A backup or restore's bar, with speed and time left; a spinner until the total is known.
pub struct RunBar {
    root: gtk::Box,
    operation: gtk::Label,
    spinner: gtk::Spinner,
    bar: gtk::ProgressBar,
    tracker: RefCell<RateTracker>,
}

impl RunBar {
    pub fn new() -> RunBar {
        let operation = gtk::Label::builder()
            .xalign(0.0)
            .hexpand(true)
            .ellipsize(gtk::pango::EllipsizeMode::Middle)
            .build();
        let spinner = gtk::Spinner::new();
        let head = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        head.append(&spinner);
        head.append(&operation);
        let bar = gtk::ProgressBar::builder().show_text(true).build();
        let root = gtk::Box::new(gtk::Orientation::Vertical, 2);
        root.append(&head);
        root.append(&bar);
        root.set_visible(false);
        RunBar {
            root,
            operation,
            spinner,
            bar,
            tracker: RefCell::default(),
        }
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    pub fn show(&self, run: Option<&Run>) {
        self.root.set_visible(run.is_some());
        let Some(run) = run else {
            self.tracker.borrow_mut().reset();
            self.spinner.set_spinning(false);
            return;
        };
        set_text(&self.operation, &run.operation);
        let measured = run.total > 0;
        self.spinner.set_visible(!measured);
        self.spinner.set_spinning(!measured);
        self.bar.set_visible(measured);
        if measured {
            let mut tracker = self.tracker.borrow_mut();
            tracker.record(run.current, &run.operation);
            let rate = tracker.suffix(run.current, run.total);
            let text = progress_text(run.current, run.total, run.full, &rate);
            self.bar.set_text(Some(&text));
            self.bar
                .set_fraction((run.current as f64 / run.total as f64).clamp(0.0, 1.0));
        }
    }
}
