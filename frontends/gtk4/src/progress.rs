//! Running jobs on screen: the step, a bar (a spinner without a total), and the detail line.

use std::cell::RefCell;
use std::rc::Rc;

use adw::prelude::*;
use rusty_backup::controller::app::Snapshot;

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
        let phase = gtk::Label::builder().xalign(0.0).build();
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
