//! Resize Image over `controller::resize_image`: one stack page per wizard page; AppKit's
//! `resize_image.rs` is the reference. Partition rows are built once, when it opens.

use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use adw::prelude::*;
use rusty_backup::controller::app::Notify;
use rusty_backup::controller::resize_image::{
    format_size, ids, labels, Page, PartMode, ResizeImageAction as A, ResizeImageController,
    ResizeImageSource, StepState, Verdict,
};
use rusty_backup::controller::{Controller, Effect};

use super::{button_row, fact_line, radios, set_radio, window};
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::{button, set_line};
use crate::inspect::bar::PartitionBar;
use crate::sync::SyncGuard;
use crate::widgets::set_text;

/// One partition's controls on the Partitions page.
struct RowView {
    title: gtk::Label,
    mode: gtk::DropDown,
    modes: RefCell<Vec<PartMode>>,
    size: gtk::Entry,
    note: gtk::Label,
}

pub struct ResizeImageDialog {
    ctrl: RefCell<ResizeImageController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<ResizeImageDialog>,
    dialog: adw::Dialog,
    stack: gtk::Stack,
    heading: gtk::Label,
    current: gtk::Label,
    disk: gtk::Entry,
    change: gtk::Label,
    check: gtk::CheckButton,
    checks: gtk::Label,
    repair: gtk::Button,
    anyway: gtk::CheckButton,
    before: PartitionBar,
    after: PartitionBar,
    rows: Vec<RowView>,
    free: gtk::Label,
    review: gtk::Label,
    output_choice: Vec<gtk::CheckButton>,
    output: gtk::Label,
    change_output: gtk::Button,
    steps: gtk::Label,
    phase: gtk::Label,
    after_checks: gtk::Label,
    outcome: gtk::Label,
    error: gtk::Label,
    open_result: gtk::Button,
    cancel: gtk::Button,
    back: gtk::Button,
    next: gtk::Button,
}

fn page_box() -> gtk::Box {
    gtk::Box::new(gtk::Orientation::Vertical, 8)
}

fn repairable(v: &Option<Verdict>) -> bool {
    matches!(
        v,
        Some(Verdict::Errors {
            repairable: true,
            ..
        })
    )
}

impl ResizeImageDialog {
    pub fn open(notify: Arc<dyn Notify>, host: EffectHost, source: ResizeImageSource) -> Rc<Self> {
        let mut ctrl = ResizeImageController::new(notify);
        ctrl.apply(A::Open(Box::new(source)));
        let n = ctrl.rows().len();
        let d = Rc::new_cyclic(|me: &Weak<ResizeImageDialog>| {
            let send: Rc<dyn Fn(A)> = {
                let me = me.clone();
                Rc::new(move |a| {
                    if let Some(d) = me.upgrade() {
                        if !d.sync.active() {
                            d.apply(a);
                        }
                    }
                })
            };
            let heading = gtk::Label::builder().xalign(0.0).build();
            heading.add_css_class("heading");

            // Size
            let current = fact_line();
            let disk = gtk::Entry::new();
            disk.set_widget_name(ids::DISK_SIZE);
            disk.set_width_chars(10);
            let s = send.clone();
            disk.connect_changed(move |e| s(A::SetDiskSize(e.text().to_string())));
            let size_row = gtk::Box::new(gtk::Orientation::Horizontal, 8);
            size_row.append(&gtk::Label::new(Some(&format!("{}:", labels::DISK_SIZE))));
            size_row.append(&disk);
            let hint = gtk::Label::new(Some(labels::SIZE_HINT));
            hint.add_css_class("dim-label");
            size_row.append(&hint);
            let change = fact_line();
            let size_page = page_box();
            size_page.append(&current);
            size_page.append(&size_row);
            size_page.append(&change);

            // Check
            let check = gtk::CheckButton::with_label(labels::CHECK);
            check.set_widget_name(ids::CHECK);
            let s = send.clone();
            check.connect_toggled(move |c| s(A::SetCheck(c.is_active())));
            let checks = fact_line();
            let repair = button("Repair...", ids::REPAIR);
            repair.set_halign(gtk::Align::Start);
            let anyway = gtk::CheckButton::with_label(labels::CONTINUE_ANYWAY);
            anyway.set_widget_name(ids::CONTINUE_ANYWAY);
            let s = send.clone();
            anyway.connect_toggled(move |c| s(A::ContinueAnyway(c.is_active())));
            let check_page = page_box();
            check_page.append(&check);
            check_page.append(&checks);
            check_page.append(&repair);
            check_page.append(&anyway);

            // Partitions
            let before = PartitionBar::build(|_| {});
            before.hide_legend();
            let after = PartitionBar::build(|_| {});
            let grid = gtk::Grid::builder()
                .row_spacing(4)
                .column_spacing(8)
                .build();
            let mut rows = Vec::new();
            for i in 0..n {
                let title = gtk::Label::builder().xalign(0.0).hexpand(true).build();
                let mode = gtk::DropDown::from_strings(&[]);
                mode.set_widget_name(&format!("{}.{i}", ids::ROW_MODE));
                let size = gtk::Entry::new();
                size.set_widget_name(&format!("{}.{i}", ids::ROW_SIZE));
                size.set_width_chars(8);
                let note = fact_line();
                note.add_css_class("dim-label");
                grid.attach(&title, 0, (i * 2) as i32, 1, 1);
                grid.attach(&mode, 1, (i * 2) as i32, 1, 1);
                grid.attach(&size, 2, (i * 2) as i32, 1, 1);
                grid.attach(&note, 0, (i * 2 + 1) as i32, 3, 1);
                rows.push(RowView {
                    title,
                    mode,
                    modes: RefCell::default(),
                    size,
                    note,
                });
            }
            let free = fact_line();
            let parts_page = page_box();
            parts_page.append(&gtk::Label::builder().label("Now:").xalign(0.0).build());
            parts_page.append(before.root());
            parts_page.append(&gtk::Label::builder().label("After:").xalign(0.0).build());
            parts_page.append(after.root());
            parts_page.append(&grid);
            parts_page.append(&free);

            // Review
            let review = fact_line();
            let (choice_row, output_choice) = radios(&["Write a resized copy", labels::IN_PLACE]);
            choice_row.set_orientation(gtk::Orientation::Vertical);
            output_choice[0].set_widget_name(ids::OUTPUT);
            output_choice[1].set_widget_name(ids::IN_PLACE);
            for (k, radio) in output_choice.iter().enumerate() {
                let s = send.clone();
                radio.connect_toggled(move |r| {
                    if r.is_active() {
                        s(A::SetInPlace(k == 1));
                    }
                });
            }
            let output = fact_line();
            let change_output = button("Change...", "resize_image.change_output");
            let s = send.clone();
            change_output.connect_clicked(move |_| s(A::PickOutput));
            let out_row = gtk::Box::new(gtk::Orientation::Horizontal, 8);
            out_row.append(&output);
            out_row.append(&change_output);
            let review_page = page_box();
            review_page.append(&review);
            review_page.append(&choice_row);
            review_page.append(&out_row);

            // Work and Done
            let steps = fact_line();
            steps.add_css_class("monospace");
            let phase = fact_line();
            let after_checks = fact_line();
            let outcome = fact_line();
            let work_page = page_box();
            for w in [&steps, &phase, &after_checks, &outcome] {
                work_page.append(w);
            }

            let stack = gtk::Stack::new();
            stack.add_named(&size_page, Some("size"));
            stack.add_named(&check_page, Some("check"));
            stack.add_named(&parts_page, Some("partitions"));
            stack.add_named(&review_page, Some("review"));
            stack.add_named(&work_page, Some("work"));

            let error = fact_line();
            error.add_css_class("error");
            let open_result = button(labels::OPEN_RESULT, ids::OPEN_RESULT);
            let s = send.clone();
            open_result.connect_clicked(move |_| s(A::OpenResult));
            let cancel = button("Cancel", ids::CANCEL);
            let back = button("Back", ids::BACK);
            let s = send.clone();
            back.connect_clicked(move |_| s(A::Back));
            let next = button("Next", ids::NEXT);
            next.add_css_class("suggested-action");

            let body = crate::browser::dialogs::page();
            body.append(&heading);
            body.append(&stack);
            body.append(&error);
            body.append(&button_row(&[&open_result, &cancel, &back, &next]));
            let s = send.clone();
            let dialog = window(labels::TITLE, 680, 560, &body, Rc::new(move || s(A::Close)));
            ResizeImageDialog {
                ctrl: RefCell::new(ctrl),
                host,
                sync: SyncGuard::default(),
                me: me.clone(),
                dialog,
                stack,
                heading,
                current,
                disk,
                change,
                check,
                checks,
                repair,
                anyway,
                before,
                after,
                rows,
                free,
                review,
                output_choice,
                output,
                change_output,
                steps,
                phase,
                after_checks,
                outcome,
                error,
                open_result,
                cancel,
                back,
                next,
            }
        });
        d.connect_rows();
        let me = Rc::downgrade(&d);
        d.next.connect_clicked(move |_| {
            if let Some(d) = me.upgrade() {
                let a = match d.ctrl.borrow().page() {
                    Page::Review => A::Start,
                    _ => A::Next,
                };
                d.apply(a);
            }
        });
        let me = Rc::downgrade(&d);
        d.cancel.connect_clicked(move |_| {
            if let Some(d) = me.upgrade() {
                let a = match d.ctrl.borrow().page() {
                    Page::Work => A::Cancel,
                    _ => A::Close,
                };
                d.apply(a);
            }
        });
        let me = Rc::downgrade(&d);
        d.repair.connect_clicked(move |_| {
            if let Some(d) = me.upgrade() {
                let index = d
                    .ctrl
                    .borrow()
                    .checks()
                    .iter()
                    .find(|r| repairable(&r.verdict))
                    .map(|r| r.index);
                if let Some(i) = index {
                    d.apply(A::Repair(i));
                }
            }
        });
        d.after(Vec::new());
        d.host.present(&d.dialog);
        d
    }

    /// Each row's mode list and size field report to the controller by the row's position.
    fn connect_rows(self: &Rc<Self>) {
        for (i, row) in self.rows.iter().enumerate() {
            let me = Rc::downgrade(self);
            row.mode.connect_selected_notify(move |dd| {
                let Some(d) = me.upgrade() else { return };
                if d.sync.active() {
                    return;
                }
                let mode = d.rows[i]
                    .modes
                    .borrow()
                    .get(dd.selected() as usize)
                    .copied();
                let index = d.ctrl.borrow().rows().get(i).map(|r| r.index);
                if let (Some(mode), Some(index)) = (mode, index) {
                    d.apply(A::SetMode { index, mode });
                }
            });
            let me = Rc::downgrade(self);
            row.size.connect_changed(move |e| {
                let Some(d) = me.upgrade() else { return };
                if d.sync.active() {
                    return;
                }
                let index = d.ctrl.borrow().rows().get(i).map(|r| r.index);
                if let Some(index) = index {
                    d.apply(A::SetSizeText {
                        index,
                        text: e.text().to_string(),
                    });
                }
            });
        }
    }

    pub fn apply(&self, action: A) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    pub fn tick(&self) {
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    pub fn is_open(&self) -> bool {
        self.ctrl.borrow().is_open()
    }

    pub fn is_running(&self) -> bool {
        self.ctrl.borrow().is_running()
    }

    pub fn take_disk_changed(&self) -> bool {
        self.ctrl.borrow_mut().take_disk_changed()
    }

    pub fn take_open_request(&self) -> Option<std::path::PathBuf> {
        self.ctrl.borrow_mut().take_open_request()
    }

    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            let Some(d) = me.upgrade() else { return };
            let action = match reply {
                Reply::Picked { purpose, path } => A::Picked { purpose, path },
                Reply::Confirmed { purpose, yes } => A::Confirmed { purpose, yes },
                Reply::Answered { .. } | Reply::Chose { .. } | Reply::CloseScreen => return,
            };
            d.apply(action);
        })
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        let log = self.ctrl.borrow_mut().take_log();
        crate::app::append_log(log);
        if !self.ctrl.borrow().is_open() {
            self.dialog.force_close();
            return;
        }
        self.sync.run(|| self.refresh());
    }

    /// Read the controller, then set the widgets; a field keeps the user's typing.
    fn refresh(&self) {
        let c = self.ctrl.borrow();
        let page = c.page();
        set_text(&self.heading, &page.heading());
        let name = match page {
            Page::Size => "size",
            Page::Check => "check",
            Page::Partitions => "partitions",
            Page::Review => "review",
            Page::Work | Page::Done => "work",
        };
        if self.stack.visible_child_name().as_deref() != Some(name) {
            self.stack.set_visible_child_name(name);
        }

        let file = c
            .source_path()
            .and_then(|p| p.file_name())
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        set_text(
            &self.current,
            &format!("{file}: {} now.", format_size(c.current_size())),
        );
        if !self.disk.has_focus() && self.disk.text() != c.disk_text() {
            self.disk.set_text(c.disk_text());
        }
        set_text(&self.change, &c.disk_change());

        if self.check.is_active() != c.check_enabled() {
            self.check.set_active(c.check_enabled());
        }
        let lines: Vec<String> = c
            .checks()
            .iter()
            .map(|r| {
                let v = r.verdict.as_ref().map(|v| v.text());
                format!("{}: {}", r.title, v.unwrap_or_else(|| "Checking...".into()))
            })
            .collect();
        let text = match (c.check_enabled(), lines.is_empty()) {
            (false, _) => "The volumes will not be checked.".to_string(),
            (true, true) => "Checking...".to_string(),
            (true, false) => lines.join("\n"),
        };
        set_text(&self.checks, &text);
        let fixable = c.checks().iter().find(|r| repairable(&r.verdict));
        self.repair.set_visible(fixable.is_some());
        if let Some(r) = fixable {
            let short = r.title.split(" of ").next().unwrap_or(&r.title);
            self.repair.set_label(&format!("Repair {short}..."));
        }
        self.anyway.set_visible(c.check_trouble());
        if self.anyway.is_active() != c.continue_anyway() {
            self.anyway.set_active(c.continue_anyway());
        }

        let (before, after) = c.bars();
        self.before.show(before, None);
        self.after.show(after, None);
        let fill = c.final_sizes().unwrap_or_default();
        for (view, row) in self.rows.iter().zip(c.rows()) {
            set_text(&view.title, &row.title);
            let modes = row.modes();
            if *view.modes.borrow() != modes {
                let names: Vec<&str> = modes.iter().map(|m| m.label()).collect();
                view.mode.set_model(Some(&gtk::StringList::new(&names)));
                *view.modes.borrow_mut() = modes.clone();
            }
            let at = modes.iter().position(|m| *m == row.mode).unwrap_or(0) as u32;
            if view.mode.selected() != at {
                view.mode.set_selected(at);
            }
            view.size.set_sensitive(row.fixed.is_none());
            if !view.size.has_focus() {
                let shown = match row.mode {
                    PartMode::Fill => fill
                        .iter()
                        .find(|(x, _)| *x == row.index)
                        .map(|s| format!("{:.2}", s.1 as f64 / 1048576.0))
                        .unwrap_or_else(|| row.text.clone()),
                    _ => row.text.clone(),
                };
                if view.size.text() != shown.as_str() {
                    view.size.set_text(&shown);
                }
            }
            let warning = c.row_warning(row.index);
            set_text(&view.note, warning.as_deref().unwrap_or(&row.note()));
            match warning {
                Some(_) => view.note.add_css_class("warning"),
                None => view.note.remove_css_class("warning"),
            }
        }
        let free = c
            .free_at_end()
            .map(|n| format!("Free space left at the end of the disk: {}", format_size(n)));
        set_text(&self.free, free.as_deref().unwrap_or(""));

        set_text(&self.review, &c.review().join("\n"));
        set_radio(&self.output_choice, usize::from(c.in_place()));
        let out = match c.in_place() {
            true => String::new(),
            false => c
                .output()
                .map(|p| p.display().to_string())
                .unwrap_or_default(),
        };
        set_text(&self.output, &out);
        self.change_output.set_sensitive(!c.in_place());

        let steps: Vec<String> = c
            .steps()
            .iter()
            .map(|(step, state)| {
                let (mark, note) = match state {
                    StepState::Pending => ("     ", String::new()),
                    StepState::Running => ("...  ", String::new()),
                    StepState::Done(n) => ("OK   ", format!(" - {n}")),
                    StepState::Failed(n) => ("FAIL ", format!(" - {n}")),
                };
                format!("{mark}{}{note}", step.label())
            })
            .collect();
        set_text(&self.steps, &steps.join("\n"));
        let phase = c.progress().map(|s| match s.progress.fraction() {
            Some(x) => format!("{} ({:.0}%)", s.phase, x * 100.0),
            None => s.phase,
        });
        set_text(&self.phase, phase.as_deref().unwrap_or(""));
        let checks: Vec<String> = c
            .after_checks()
            .iter()
            .map(|r| {
                let v = r.verdict.as_ref().map(|v| v.text()).unwrap_or_default();
                format!("{}: {v}", r.title)
            })
            .collect();
        set_text(&self.after_checks, &checks.join("\n"));
        let outcome = match c.outcome() {
            Some(Ok(())) => format!(
                "Done. The resized image is {}.",
                c.output()
                    .map(|p| p.display().to_string())
                    .unwrap_or_default()
            ),
            Some(Err(e)) => format!("Failed: {e}"),
            None => String::new(),
        };
        set_text(&self.outcome, &outcome);

        let error = match page {
            Page::Partitions => c
                .live_error()
                .or_else(|| c.plan_error().map(str::to_string)),
            Page::Review => c.plan_error().map(str::to_string),
            Page::Size => c.disk_size().err(),
            _ => None,
        };
        set_line(&self.error, error.as_deref());
        self.back.set_visible(matches!(
            page,
            Page::Check | Page::Partitions | Page::Review
        ));
        self.back.set_sensitive(c.can_back());
        let (label, can) = match page {
            Page::Review => (labels::START, c.can_start()),
            Page::Partitions if c.planning() => ("Checking...", false),
            _ => ("Next", c.can_next()),
        };
        self.next.set_label(label);
        self.next.set_sensitive(can);
        self.next
            .set_visible(!matches!(page, Page::Work | Page::Done));
        self.cancel.set_label(match page {
            Page::Done => "Close",
            Page::Work => "Stop",
            _ => "Cancel",
        });
        self.open_result
            .set_visible(page == Page::Done && c.outcome().is_some_and(|o| o.is_ok()));
    }
}
