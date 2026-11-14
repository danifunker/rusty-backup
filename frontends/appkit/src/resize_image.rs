//! Resize Image: the wizard window over `controller::resize_image`, one page at a time.
//!
//! Every page's controls live in one form; a refresh shows the page the controller is on and
//! hides the rest. Partition rows are built once, when the window opens on its source.

use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use objc2::rc::Retained;
use objc2::MainThreadMarker;
use objc2_app_kit::NSWindow;
use rusty_backup::controller::app::Notify;
use rusty_backup::controller::resize_image::{
    format_size, ids, labels, Page, PartMode, ResizeImageAction as A, ResizeImageController,
    ResizeImageSource, StepState,
};
use rusty_backup::controller::{Controller, Effect};

use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::Form;
use crate::layout::{self, Node, Size};
use crate::partition_bar::{self, Bar};
use crate::sync::SyncGuard;
use crate::ui::{self, CloseDelegate};
use rusty_backup::controller::commander::Tone;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum K {
    Heading,
    // Size
    Current,
    DiskCaption,
    Disk,
    Hint,
    Change,
    // Check
    Check,
    CheckRows,
    Repair,
    Anyway,
    // Partitions
    BeforeCaption,
    Before,
    AfterCaption,
    After,
    RowTitle(usize),
    RowMode(usize),
    RowSize(usize),
    RowNote(usize),
    Free,
    // Review
    Review,
    Copy,
    InPlace,
    Output,
    ChangeOutput,
    // Work / Done
    Steps,
    Phase,
    AfterChecks,
    Outcome,
    // Every page
    Error,
    Back,
    Next,
    Cancel,
    OpenResult,
}

pub struct ResizeImageWindow {
    ctrl: RefCell<ResizeImageController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<ResizeImageWindow>,
    window: Retained<NSWindow>,
    _delegate: Retained<CloseDelegate>,
    form: Form<K>,
    before: Retained<Bar>,
    after: Retained<Bar>,
    rows: usize,
}

impl ResizeImageWindow {
    pub fn open(
        mtm: MainThreadMarker,
        notify: Arc<dyn Notify>,
        host: EffectHost,
        source: ResizeImageSource,
    ) -> Rc<ResizeImageWindow> {
        let mut ctrl = ResizeImageController::new(notify);
        ctrl.apply(A::Open(Box::new(source)));
        let rows = ctrl.rows().len();
        let me = Rc::new_cyclic(|me: &Weak<ResizeImageWindow>| {
            let close = me.clone();
            let (window, delegate) = ui::window(
                mtm,
                labels::TITLE,
                680.0,
                420.0 + 30.0 * rows as f64,
                Box::new(move || {
                    if let Some(d) = close.upgrade() {
                        d.send(A::Close);
                    }
                }),
            );
            let body = window.contentView().expect("the window's content view");
            let fire = me.clone();
            let form = Form::new(
                mtm,
                body,
                Rc::new(move |k| {
                    if let Some(d) = fire.upgrade() {
                        d.fired(k);
                    }
                }),
            );
            let f = &form;
            f.label(K::Heading, "");
            f.wrap(K::Current, "");
            f.label(K::DiskCaption, &format!("{}:", labels::DISK_SIZE));
            f.field(K::Disk, ids::DISK_SIZE, labels::DISK_SIZE, false);
            f.label(K::Hint, labels::SIZE_HINT);
            f.label(K::Change, "");
            f.check(K::Check, labels::CHECK, ids::CHECK);
            f.wrap(K::CheckRows, "");
            f.button(K::Repair, "Repair...", ids::REPAIR);
            f.check(K::Anyway, labels::CONTINUE_ANYWAY, ids::CONTINUE_ANYWAY);
            f.label(K::BeforeCaption, "Now:");
            let before = Bar::new(mtm, ids::BAR);
            before.set_options(1.0, false);
            f.add(K::Before, &before);
            f.label(K::AfterCaption, "After:");
            let after = Bar::new(mtm, "resize_image.bar_after");
            after.set_options(1.0, true);
            f.add(K::After, &after);
            for i in 0..rows {
                f.label(K::RowTitle(i), "");
                f.popup(K::RowMode(i), &[], &format!("{}.{i}", ids::ROW_MODE));
                f.field(
                    K::RowSize(i),
                    &format!("{}.{i}", ids::ROW_SIZE),
                    "Size (MiB)",
                    false,
                );
                f.wrap(K::RowNote(i), "");
            }
            f.label(K::Free, "");
            f.wrap(K::Review, "");
            f.radio(K::Copy, 0, "Write a resized copy:", ids::OUTPUT);
            f.radio(K::InPlace, 0, labels::IN_PLACE, ids::IN_PLACE);
            f.wrap(K::Output, "");
            f.button(K::ChangeOutput, "Change...", "resize_image.change_output");
            f.wrap(K::Steps, "");
            f.label(K::Phase, "");
            f.wrap(K::AfterChecks, "");
            f.wrap(K::Outcome, "");
            f.wrap(K::Error, "");
            f.button(K::Back, "Back", ids::BACK);
            f.button(K::Next, "Next", ids::NEXT);
            f.button(K::Cancel, "Cancel", ids::CANCEL);
            f.button(K::OpenResult, labels::OPEN_RESULT, ids::OPEN_RESULT);
            ui::present(&window);
            ResizeImageWindow {
                ctrl: RefCell::new(ctrl),
                host,
                sync: SyncGuard::default(),
                me: me.clone(),
                window,
                _delegate: delegate,
                form,
                before,
                after,
                rows,
            }
        });
        me.after(Vec::new());
        me
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

    pub fn take_disk_changed(&self) -> bool {
        self.ctrl.borrow_mut().take_disk_changed()
    }

    pub fn take_open_request(&self) -> Option<std::path::PathBuf> {
        self.ctrl.borrow_mut().take_open_request()
    }

    /// Send `action` once the current handler returns: the window may close with it.
    fn send(&self, action: A) {
        let me = self.me.clone();
        crate::app::later(Box::new(move || {
            if let Some(d) = me.upgrade() {
                d.apply(action);
            }
        }));
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
            self.window.orderOut(None);
            return;
        }
        self.sync.run(|| self.refresh());
        self.layout();
    }

    /// Read the controller, then set the controls; only changed values are set.
    fn refresh(&self) {
        let f = &self.form;
        let c = self.ctrl.borrow();
        let page = c.page();
        f.set_text(K::Heading, &page.heading());
        let on = |p: Page| page == p;
        for k in [K::Current, K::DiskCaption, K::Disk, K::Hint, K::Change] {
            f.show(k, on(Page::Size));
        }
        for k in [K::Check, K::CheckRows, K::Repair, K::Anyway] {
            f.show(k, on(Page::Check));
        }
        let parts = on(Page::Partitions);
        for k in [
            K::BeforeCaption,
            K::Before,
            K::AfterCaption,
            K::After,
            K::Free,
        ] {
            f.show(k, parts);
        }
        for i in 0..self.rows {
            for k in [K::RowTitle(i), K::RowMode(i), K::RowSize(i), K::RowNote(i)] {
                f.show(k, parts);
            }
        }
        for k in [K::Review, K::Copy, K::InPlace, K::Output, K::ChangeOutput] {
            f.show(k, on(Page::Review));
        }
        f.show(K::Steps, on(Page::Work) || on(Page::Done));
        f.show(K::Phase, on(Page::Work));
        f.show(K::AfterChecks, on(Page::Done));
        f.show(K::Outcome, on(Page::Done));

        // Size
        let name = c
            .source_path()
            .and_then(|p| p.file_name())
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        f.set_text(
            K::Current,
            &format!("{name}: {} now.", format_size(c.current_size())),
        );
        if !f.typing(K::Disk) {
            f.set_text(K::Disk, c.disk_text());
        }
        f.set_text(K::Change, &c.disk_change());

        // Check
        f.set_check(K::Check, c.check_enabled());
        let rows: Vec<String> = c
            .checks()
            .iter()
            .map(|r| {
                let v = r.verdict.as_ref().map(|v| v.text());
                format!("{}: {}", r.title, v.unwrap_or_else(|| "Checking...".into()))
            })
            .collect();
        let text = match (c.check_enabled(), rows.is_empty()) {
            (false, _) => "The volumes will not be checked.".to_string(),
            (true, true) => "Checking...".to_string(),
            (true, false) => rows.join("\n"),
        };
        f.set_text(K::CheckRows, &text);
        let repairable = c.checks().iter().find(|r| {
            matches!(
                r.verdict,
                Some(rusty_backup::controller::resize_image::Verdict::Errors {
                    repairable: true,
                    ..
                })
            )
        });
        f.show(K::Repair, on(Page::Check) && repairable.is_some());
        if let Some(r) = repairable {
            f.set_title(K::Repair, &format!("Repair {}...", short(&r.title)));
        }
        f.show(K::Anyway, on(Page::Check) && c.check_trouble());
        f.set_check(K::Anyway, c.continue_anyway());

        // Partitions
        let (before, after) = c.bars();
        self.before.show(before, None);
        self.after.show(after, None);
        for (i, row) in c.rows().iter().enumerate().take(self.rows) {
            f.set_text(K::RowTitle(i), &row.title);
            let modes = row.modes();
            let names: Vec<String> = modes.iter().map(|m| m.label().to_string()).collect();
            f.set_items(K::RowMode(i), &names);
            f.select(
                K::RowMode(i),
                modes.iter().position(|m| *m == row.mode).unwrap_or(0),
            );
            f.enable(K::RowSize(i), row.fixed.is_none());
            if !f.typing(K::RowSize(i)) {
                let shown = match row.mode {
                    PartMode::Fill => c
                        .final_sizes()
                        .ok()
                        .and_then(|s| s.iter().find(|(x, _)| *x == row.index).map(|s| s.1))
                        .map(|b| format!("{:.2}", b as f64 / 1048576.0))
                        .unwrap_or_else(|| row.text.clone()),
                    _ => row.text.clone(),
                };
                f.set_text(K::RowSize(i), &shown);
            }
            let warn = c.row_warning(row.index);
            f.set_text(K::RowNote(i), warn.as_deref().unwrap_or(&row.note()));
            f.set_tone(K::RowNote(i), warn.as_ref().map(|_| Tone::Warning));
        }
        let free = match c.free_at_end() {
            Some(n) => format!("Free space left at the end of the disk: {}", format_size(n)),
            None => String::new(),
        };
        f.set_text(K::Free, &free);

        // Review
        f.set_text(K::Review, &c.review().join("\n"));
        f.set_check(K::Copy, !c.in_place());
        f.set_check(K::InPlace, c.in_place());
        let out = match c.in_place() {
            true => String::new(),
            false => c
                .output()
                .map(|p| p.display().to_string())
                .unwrap_or_default(),
        };
        f.set_text(K::Output, &out);
        f.enable(K::ChangeOutput, !c.in_place());

        // Work / Done
        let steps: Vec<String> = c
            .steps()
            .iter()
            .map(|(step, state)| {
                let mark = match state {
                    StepState::Pending => "     ".to_string(),
                    StepState::Running => "...  ".to_string(),
                    StepState::Done(_) => "OK   ".to_string(),
                    StepState::Failed(_) => "FAIL ".to_string(),
                };
                let note = match state {
                    StepState::Done(n) | StepState::Failed(n) => format!(" - {n}"),
                    _ => String::new(),
                };
                format!("{mark}{}{note}", step.label())
            })
            .collect();
        f.set_text(K::Steps, &steps.join("\n"));
        let phase = c.progress().map(|s| match s.progress.fraction() {
            Some(x) => format!("{} ({:.0}%)", s.phase, x * 100.0),
            None => s.phase,
        });
        f.set_text(K::Phase, phase.as_deref().unwrap_or(""));
        let checks: Vec<String> = c
            .after_checks()
            .iter()
            .map(|r| {
                let v = r.verdict.as_ref().map(|v| v.text()).unwrap_or_default();
                format!("{}: {v}", r.title)
            })
            .collect();
        f.set_text(K::AfterChecks, &checks.join("\n"));
        let (outcome, tone) = match c.outcome() {
            Some(Ok(())) => (
                format!(
                    "Done. The resized image is {}.",
                    c.output()
                        .map(|p| p.display().to_string())
                        .unwrap_or_default()
                ),
                None,
            ),
            Some(Err(e)) => (format!("Failed: {e}"), Some(Tone::Error)),
            None => (String::new(), None),
        };
        f.set_text(K::Outcome, &outcome);
        f.set_tone(K::Outcome, tone);

        // Every page
        let error = match page {
            Page::Partitions => c
                .live_error()
                .or_else(|| c.plan_error().map(str::to_string)),
            Page::Review => c.plan_error().map(str::to_string),
            Page::Size => c.disk_size().err(),
            _ => None,
        };
        f.line(K::Error, error.as_deref(), Some(Tone::Error));
        f.show(
            K::Back,
            c.can_back() || matches!(page, Page::Check | Page::Partitions),
        );
        f.enable(K::Back, c.can_back());
        let (next, can) = match page {
            Page::Review => (labels::START, c.can_start()),
            Page::Partitions if c.planning() => ("Checking...", false),
            _ => ("Next", c.can_next()),
        };
        f.set_title(K::Next, next);
        f.enable(K::Next, can);
        f.show(K::Next, !matches!(page, Page::Work | Page::Done));
        f.set_title(
            K::Cancel,
            match page {
                Page::Done => "Close",
                Page::Work => "Stop",
                _ => "Cancel",
            },
        );
        f.show(
            K::OpenResult,
            on(Page::Done) && c.outcome().is_some_and(|o| o.is_ok()),
        );
    }

    fn layout(&self) {
        let Some(body) = self.window.contentView() else {
            return;
        };
        let row_h = Size::Fixed(layout::ROW);
        let text = |k: K, lines: i32| {
            Node::leaf(
                k,
                Size::Stretch(1),
                Size::Fixed((layout::ROW - 6) * lines + 4),
            )
        };
        let c = self.ctrl.borrow();
        // A line naming a file path may wrap once.
        let check_lines = (c.checks().len().max(1) as i32).min(8);
        let review_lines = (c.review().len().max(1) as i32 + 1).min(14);
        let step_lines = (c.steps().len().max(1) as i32 + 1).min(10);
        let after_lines = c.after_checks().len() as i32;
        let notes: Vec<i32> = c
            .rows()
            .iter()
            .map(|r| {
                if c.row_warning(r.index).is_some() {
                    2
                } else {
                    1
                }
            })
            .collect();
        drop(c);
        let bar = |k: K, h: i32| Node::leaf(k, Size::Stretch(1), Size::Fixed(h));
        let mut col = vec![
            text(K::Heading, 1),
            text(K::Current, 1),
            Node::row(vec![
                Node::leaf(
                    K::DiskCaption,
                    Size::Fixed(130),
                    Size::Fixed(layout::ROW - 4),
                ),
                Node::leaf(K::Disk, Size::Fixed(110), row_h),
                Node::leaf(K::Hint, Size::Stretch(1), Size::Fixed(layout::ROW - 4)),
            ]),
            text(K::Change, 1),
            Node::leaf(K::Check, Size::Stretch(1), row_h),
            text(K::CheckRows, check_lines),
            Node::row(vec![
                Node::button(K::Repair, 220),
                Node::Space(Size::Stretch(1)),
            ]),
            Node::leaf(K::Anyway, Size::Stretch(1), row_h),
            text(K::BeforeCaption, 1),
            bar(K::Before, partition_bar::BAR_ONLY),
            text(K::AfterCaption, 1),
            bar(K::After, partition_bar::HEIGHT),
        ];
        for i in 0..self.rows {
            col.push(Node::row(vec![
                Node::leaf(
                    K::RowTitle(i),
                    Size::Stretch(1),
                    Size::Fixed(layout::ROW - 4),
                ),
                Node::leaf(
                    K::RowMode(i),
                    Size::Fixed(140),
                    Size::Fixed(layout::ROW + 2),
                ),
                Node::leaf(K::RowSize(i), Size::Fixed(90), row_h),
            ]));
            col.push(text(K::RowNote(i), notes.get(i).copied().unwrap_or(1)));
        }
        col.extend([
            text(K::Free, 1),
            text(K::Review, review_lines),
            Node::leaf(K::Copy, Size::Stretch(1), row_h),
            Node::row(vec![text(K::Output, 2), Node::button(K::ChangeOutput, 100)]),
            Node::leaf(K::InPlace, Size::Stretch(1), row_h),
            text(K::Steps, step_lines),
            text(K::Phase, 1),
            text(K::AfterChecks, after_lines.max(1)),
            text(K::Outcome, 2),
            text(K::Error, 2),
            Node::Space(Size::Stretch(1)),
            Node::row(vec![
                Node::button(K::OpenResult, 140),
                Node::Space(Size::Stretch(1)),
                Node::button(K::Cancel, 90),
                Node::button(K::Back, 90),
                Node::button(K::Next, 110),
            ]),
        ]);
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        self.form.place_shown(tree, ui::area(&body));
    }

    fn fired(&self, k: K) {
        if self.sync.active() {
            return;
        }
        let f = &self.form;
        let action = match k {
            K::Disk => A::SetDiskSize(f.text(k)),
            K::Check => A::SetCheck(f.checked(k)),
            K::Anyway => A::ContinueAnyway(f.checked(k)),
            K::Repair => {
                let c = self.ctrl.borrow();
                let Some(r) = c.checks().iter().find(|r| {
                    matches!(
                        r.verdict,
                        Some(rusty_backup::controller::resize_image::Verdict::Errors {
                            repairable: true,
                            ..
                        })
                    )
                }) else {
                    return;
                };
                A::Repair(r.index)
            }
            K::RowMode(i) => {
                let c = self.ctrl.borrow();
                let Some(row) = c.rows().get(i) else { return };
                let Some(mode) = f.picked(k).and_then(|p| row.modes().get(p).copied()) else {
                    return;
                };
                A::SetMode {
                    index: row.index,
                    mode,
                }
            }
            K::RowSize(i) => {
                let Some(index) = self.ctrl.borrow().rows().get(i).map(|r| r.index) else {
                    return;
                };
                A::SetSizeText {
                    index,
                    text: f.text(k),
                }
            }
            K::Copy => A::SetInPlace(false),
            K::InPlace => A::SetInPlace(true),
            K::ChangeOutput => A::PickOutput,
            K::Back => A::Back,
            K::Next => match self.ctrl.borrow().page() {
                Page::Review => A::Start,
                _ => A::Next,
            },
            K::Cancel => match self.ctrl.borrow().page() {
                Page::Work => A::Cancel,
                _ => A::Close,
            },
            K::OpenResult => A::OpenResult,
            _ => return,
        };
        self.send(action);
    }
}

/// "Partition 2 of 3: Data (FAT32)" -> "Partition 2".
fn short(title: &str) -> &str {
    title.split(" of ").next().unwrap_or(title)
}

impl Drop for ResizeImageWindow {
    fn drop(&mut self) {
        self.window.setDelegate(None);
        self.window.orderOut(None);
    }
}
