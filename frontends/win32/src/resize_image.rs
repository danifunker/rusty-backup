//! Resize Image over `controller::resize_image`: every page's controls in one window, the page
//! the controller is on shown; AppKit's `resize_image.rs` is the reference.

use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use rusty_backup::controller::app::Notify;
use rusty_backup::controller::resize_image::{
    format_size, ids, labels, Page, PartMode, ResizeImageAction as A, ResizeImageController,
    ResizeImageSource, StepState, Verdict,
};
use rusty_backup::controller::{Controller, Effect};
use windows::core::w;
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, WPARAM};
use windows::Win32::UI::Controls::{BST_CHECKED, NMHDR};
use windows::Win32::UI::WindowsAndMessaging::{
    DestroyWindow, SendMessageW, ShowWindow, BM_GETCHECK, BM_SETCHECK, BN_CLICKED,
    BS_AUTORADIOBUTTON, CBN_SELCHANGE, CBS_DROPDOWNLIST, CB_ADDSTRING, CB_GETCURSEL,
    CB_RESETCONTENT, CB_SETCURSEL, EN_CHANGE, SW_SHOW, WINDOW_EX_STYLE, WINDOW_STYLE,
    WS_EX_CLIENTEDGE, WS_GROUP, WS_TABSTOP, WS_VSCROLL,
};

use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::layout::{self, Node, Size};
use crate::panel::{self, Handler};
use crate::partition_bar;
use crate::sync::SyncGuard;
use crate::ui;

/// One partition's controls on the Partitions page.
struct RowView {
    title: HWND,
    mode: HWND,
    modes: RefCell<Vec<PartMode>>,
    size: HWND,
    note: HWND,
}

pub struct ResizeImageWindow {
    ctrl: RefCell<ResizeImageController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<ResizeImageWindow>,
    window: HWND,
    heading: HWND,
    current: HWND,
    disk_caption: HWND,
    disk: HWND,
    hint: HWND,
    change: HWND,
    check: HWND,
    checks: HWND,
    repair: HWND,
    anyway: HWND,
    now_caption: HWND,
    before: HWND,
    after_caption: HWND,
    after: HWND,
    rows: Vec<RowView>,
    free: HWND,
    review: HWND,
    copy: HWND,
    output: HWND,
    change_output: HWND,
    in_place: HWND,
    steps: HWND,
    phase: HWND,
    after_checks: HWND,
    outcome: HWND,
    error: HWND,
    open_result: HWND,
    cancel: HWND,
    back: HWND,
    next: HWND,
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

fn set_check(h: HWND, on: bool) {
    // SAFETY: plain button messages.
    unsafe {
        let now = SendMessageW(h, BM_GETCHECK, None, None).0 == BST_CHECKED.0 as isize;
        if now != on {
            SendMessageW(h, BM_SETCHECK, Some(WPARAM(on as usize)), None);
        }
    }
}

fn checked(h: HWND) -> bool {
    // SAFETY: a plain button query.
    unsafe { SendMessageW(h, BM_GETCHECK, None, None).0 == BST_CHECKED.0 as isize }
}

/// Fill a drop-down list with `items`.
fn set_items(h: HWND, items: &[&str]) {
    // SAFETY: plain combo box messages; each string outlives its call.
    unsafe {
        SendMessageW(h, CB_RESETCONTENT, None, None);
        for item in items {
            let t = ui::wide(item);
            SendMessageW(h, CB_ADDSTRING, None, Some(LPARAM(t.as_ptr() as isize)));
        }
    }
}

fn select(h: HWND, at: usize) {
    // SAFETY: plain combo box messages.
    unsafe {
        if SendMessageW(h, CB_GETCURSEL, None, None).0 as usize != at {
            SendMessageW(h, CB_SETCURSEL, Some(WPARAM(at)), None);
        }
    }
}

fn picked(h: HWND) -> Option<usize> {
    // SAFETY: a plain combo box query.
    usize::try_from(unsafe { SendMessageW(h, CB_GETCURSEL, None, None).0 }).ok()
}

impl ResizeImageWindow {
    pub fn open(
        owner: HWND,
        notify: Arc<dyn Notify>,
        host: EffectHost,
        source: ResizeImageSource,
    ) -> Rc<Self> {
        let mut ctrl = ResizeImageController::new(notify);
        ctrl.apply(A::Open(Box::new(source)));
        let n = ctrl.rows().len();
        let window = panel::dialog(owner, labels::TITLE, 680, 440 + 34 * n as i32);
        let edit = |id: &str| {
            ui::child(
                window,
                w!("EDIT"),
                "",
                WINDOW_STYLE(WS_TABSTOP.0),
                WS_EX_CLIENTEDGE,
                id,
            )
        };
        let combo = |id: &str| {
            let style = (CBS_DROPDOWNLIST as u32) | WS_TABSTOP.0 | WS_VSCROLL.0;
            ui::child(
                window,
                w!("COMBOBOX"),
                "",
                WINDOW_STYLE(style),
                WINDOW_EX_STYLE(0),
                id,
            )
        };
        let radio = |text: &str, first: bool, id: &str| {
            let style =
                (BS_AUTORADIOBUTTON as u32) | WS_TABSTOP.0 | if first { WS_GROUP.0 } else { 0 };
            ui::child(
                window,
                w!("BUTTON"),
                text,
                WINDOW_STYLE(style),
                WINDOW_EX_STYLE(0),
                id,
            )
        };
        let bar = |legend: bool| {
            let b = partition_bar::build(window, Box::new(|_| {}));
            partition_bar::set_options(b, 1.0, legend);
            b
        };
        let rows = (0..n)
            .map(|i| RowView {
                title: ui::label(window, ""),
                mode: combo(&format!("{}.{i}", ids::ROW_MODE)),
                modes: RefCell::default(),
                size: edit(&format!("{}.{i}", ids::ROW_SIZE)),
                note: ui::label(window, ""),
            })
            .collect();
        let me = Rc::new_cyclic(|me: &Weak<ResizeImageWindow>| ResizeImageWindow {
            ctrl: RefCell::new(ctrl),
            host,
            sync: SyncGuard::default(),
            me: me.clone(),
            window,
            heading: ui::label(window, ""),
            current: ui::label(window, ""),
            disk_caption: ui::cell_label(window, &format!("{}:", labels::DISK_SIZE)),
            disk: edit(ids::DISK_SIZE),
            hint: ui::cell_label(window, labels::SIZE_HINT),
            change: ui::label(window, ""),
            check: ui::checkbox(window, labels::CHECK, ids::CHECK),
            checks: ui::label(window, ""),
            repair: ui::button(window, "Repair...", ids::REPAIR),
            anyway: ui::checkbox(window, labels::CONTINUE_ANYWAY, ids::CONTINUE_ANYWAY),
            now_caption: ui::label(window, "Now:"),
            before: bar(false),
            after_caption: ui::label(window, "After:"),
            after: bar(true),
            rows,
            free: ui::label(window, ""),
            review: ui::label(window, ""),
            copy: radio("Write a resized copy:", true, ids::OUTPUT),
            output: ui::path_label(window, ""),
            change_output: ui::button(window, "Change...", "resize_image.change_output"),
            in_place: radio(labels::IN_PLACE, false, ids::IN_PLACE),
            steps: ui::label(window, ""),
            phase: ui::label(window, ""),
            after_checks: ui::label(window, ""),
            outcome: ui::label(window, ""),
            error: ui::label(window, ""),
            open_result: ui::button(window, labels::OPEN_RESULT, ids::OPEN_RESULT),
            cancel: ui::button(window, "Cancel", ids::CANCEL),
            back: ui::button(window, "Back", ids::BACK),
            next: ui::button(window, "Next", ids::NEXT),
        });
        let weak: Weak<dyn Handler> = me.me.clone();
        panel::register(window, weak);
        me.after(Vec::new());
        // SAFETY: showing the window we created.
        unsafe {
            let _ = ShowWindow(window, SW_SHOW);
        }
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
            panel::unregister(self.window);
            // SAFETY: the controller closed; the window goes with it.
            let _ = unsafe { DestroyWindow(self.window) };
            return;
        }
        self.sync.run(|| self.refresh());
        self.layout();
    }

    /// The controls each page shows.
    fn page_controls(&self, page: Page) -> Vec<HWND> {
        let mut v = match page {
            Page::Size => vec![
                self.current,
                self.disk_caption,
                self.disk,
                self.hint,
                self.change,
            ],
            Page::Check => vec![self.check, self.checks, self.repair, self.anyway],
            Page::Partitions => {
                let mut v = vec![
                    self.now_caption,
                    self.before,
                    self.after_caption,
                    self.after,
                    self.free,
                ];
                for r in &self.rows {
                    v.extend([r.title, r.mode, r.size, r.note]);
                }
                v
            }
            Page::Review => vec![
                self.review,
                self.copy,
                self.output,
                self.change_output,
                self.in_place,
            ],
            Page::Work => vec![self.steps, self.phase],
            Page::Done => vec![self.steps, self.after_checks, self.outcome],
        };
        v.extend([self.heading, self.error, self.cancel]);
        v
    }

    fn refresh(&self) {
        let c = self.ctrl.borrow();
        let page = c.page();
        let shown = self.page_controls(page);
        let mut all: Vec<HWND> = Page::ALL
            .iter()
            .flat_map(|p| self.page_controls(*p))
            .collect();
        all.extend([self.back, self.next, self.open_result]);
        for h in all {
            ui::set_visible(h, shown.contains(&h));
        }
        ui::set_text(self.heading, &page.heading());

        let file = c
            .source_path()
            .and_then(|p| p.file_name())
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        ui::set_text(
            self.current,
            &format!("{file}: {} now.", format_size(c.current_size())),
        );
        ui::set_text(self.disk, c.disk_text());
        ui::set_text(self.change, &c.disk_change());

        set_check(self.check, c.check_enabled());
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
            (true, false) => lines.join("\r\n"),
        };
        ui::set_text(self.checks, &text);
        let fixable = c.checks().iter().find(|r| repairable(&r.verdict));
        ui::set_visible(self.repair, page == Page::Check && fixable.is_some());
        if let Some(r) = fixable {
            let short = r.title.split(" of ").next().unwrap_or(&r.title);
            ui::set_text(self.repair, &format!("Repair {short}..."));
        }
        ui::set_visible(self.anyway, page == Page::Check && c.check_trouble());
        set_check(self.anyway, c.continue_anyway());

        let (before, after) = c.bars();
        partition_bar::show(self.before, before, None);
        partition_bar::show(self.after, after, None);
        let fill = c.final_sizes().unwrap_or_default();
        for (view, row) in self.rows.iter().zip(c.rows()) {
            ui::set_text(view.title, &row.title);
            let modes = row.modes();
            if *view.modes.borrow() != modes {
                let names: Vec<&str> = modes.iter().map(|m| m.label()).collect();
                set_items(view.mode, &names);
                *view.modes.borrow_mut() = modes.clone();
            }
            select(
                view.mode,
                modes.iter().position(|m| *m == row.mode).unwrap_or(0),
            );
            ui::set_enabled(view.size, row.fixed.is_none());
            let shown = match row.mode {
                PartMode::Fill => fill
                    .iter()
                    .find(|(x, _)| *x == row.index)
                    .map(|s| format!("{:.2}", s.1 as f64 / 1048576.0))
                    .unwrap_or_else(|| row.text.clone()),
                _ => row.text.clone(),
            };
            ui::set_text(view.size, &shown);
            let warning = c.row_warning(row.index);
            ui::set_text(view.note, warning.as_deref().unwrap_or(&row.note()));
        }
        let free = c
            .free_at_end()
            .map(|n| format!("Free space left at the end of the disk: {}", format_size(n)));
        ui::set_text(self.free, free.as_deref().unwrap_or(""));

        ui::set_text(self.review, &c.review().join("\r\n"));
        set_check(self.copy, !c.in_place());
        set_check(self.in_place, c.in_place());
        let out = match c.in_place() {
            true => String::new(),
            false => c
                .output()
                .map(|p| p.display().to_string())
                .unwrap_or_default(),
        };
        ui::set_text(self.output, &out);
        ui::set_enabled(self.change_output, !c.in_place());

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
        ui::set_text(self.steps, &steps.join("\r\n"));
        let phase = c.progress().map(|s| match s.progress.fraction() {
            Some(x) => format!("{} ({:.0}%)", s.phase, x * 100.0),
            None => s.phase,
        });
        ui::set_text(self.phase, phase.as_deref().unwrap_or(""));
        let checks: Vec<String> = c
            .after_checks()
            .iter()
            .map(|r| {
                let v = r.verdict.as_ref().map(|v| v.text()).unwrap_or_default();
                format!("{}: {v}", r.title)
            })
            .collect();
        ui::set_text(self.after_checks, &checks.join("\r\n"));
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
        ui::set_text(self.outcome, &outcome);

        let error = match page {
            Page::Partitions => c
                .live_error()
                .or_else(|| c.plan_error().map(str::to_string)),
            Page::Review => c.plan_error().map(str::to_string),
            Page::Size => c.disk_size().err(),
            _ => None,
        };
        ui::set_text(self.error, error.as_deref().unwrap_or(""));
        ui::set_visible(
            self.back,
            matches!(page, Page::Check | Page::Partitions | Page::Review),
        );
        ui::set_enabled(self.back, c.can_back());
        let (label, can) = match page {
            Page::Review => (labels::START, c.can_start()),
            Page::Partitions if c.planning() => ("Checking...", false),
            _ => ("Next", c.can_next()),
        };
        ui::set_text(self.next, label);
        ui::set_enabled(self.next, can);
        ui::set_visible(self.next, !matches!(page, Page::Work | Page::Done));
        ui::set_text(
            self.cancel,
            match page {
                Page::Done => "Close",
                Page::Work => "Stop",
                _ => "Cancel",
            },
        );
        ui::set_visible(
            self.open_result,
            page == Page::Done && c.outcome().is_some_and(|o| o.is_ok()),
        );
    }

    fn layout(&self) {
        let (area, dpi) = panel::client(self.window);
        let lines = |h: HWND, n: i32| {
            Node::leaf(
                h,
                Size::Stretch(1),
                Size::Fixed((layout::ROW - 8) * n.max(1)),
            )
        };
        let count = |t: &str| t.lines().count() as i32;
        let row_h = Size::Fixed(layout::ROW);
        let mut col = Vec::new();
        let mut push = |node: Node<HWND>, h: HWND| {
            if ui::shown(h) {
                col.push(node);
            }
        };
        push(lines(self.heading, 1), self.heading);
        push(lines(self.current, 1), self.current);
        push(
            Node::row(vec![
                Node::leaf(
                    self.disk_caption,
                    Size::Fixed(130),
                    Size::Fixed(layout::ROW - 8),
                ),
                Node::leaf(self.disk, Size::Fixed(110), row_h),
                Node::leaf(self.hint, Size::Stretch(1), Size::Fixed(layout::ROW - 8)),
            ]),
            self.disk,
        );
        push(lines(self.change, 1), self.change);
        push(Node::leaf(self.check, Size::Stretch(1), row_h), self.check);
        push(
            lines(self.checks, count(&ui::text(self.checks))),
            self.checks,
        );
        push(
            Node::row(vec![
                Node::button(self.repair, 200),
                Node::Space(Size::Stretch(1)),
            ]),
            self.repair,
        );
        push(
            Node::leaf(self.anyway, Size::Stretch(1), row_h),
            self.anyway,
        );
        push(lines(self.now_caption, 1), self.now_caption);
        push(
            Node::leaf(
                self.before,
                Size::Stretch(1),
                Size::Fixed(partition_bar::BAR_ONLY),
            ),
            self.before,
        );
        push(lines(self.after_caption, 1), self.after_caption);
        push(
            Node::leaf(
                self.after,
                Size::Stretch(1),
                Size::Fixed(partition_bar::HEIGHT),
            ),
            self.after,
        );
        for r in &self.rows {
            push(
                Node::row(vec![
                    Node::leaf(r.title, Size::Stretch(1), Size::Fixed(layout::ROW - 8)),
                    Node::leaf(r.mode, Size::Fixed(140), row_h),
                    Node::leaf(r.size, Size::Fixed(90), row_h),
                ]),
                r.title,
            );
            push(lines(r.note, 2), r.note);
        }
        push(lines(self.free, 1), self.free);
        push(
            lines(self.review, count(&ui::text(self.review)) + 1),
            self.review,
        );
        push(Node::leaf(self.copy, Size::Stretch(1), row_h), self.copy);
        push(
            Node::row(vec![
                Node::leaf(self.output, Size::Stretch(1), Size::Fixed(layout::ROW - 8)),
                Node::button(self.change_output, 100),
            ]),
            self.output,
        );
        push(
            Node::leaf(self.in_place, Size::Stretch(1), row_h),
            self.in_place,
        );
        push(
            lines(self.steps, count(&ui::text(self.steps)) + 1),
            self.steps,
        );
        push(lines(self.phase, 1), self.phase);
        push(
            lines(self.after_checks, count(&ui::text(self.after_checks))),
            self.after_checks,
        );
        push(lines(self.outcome, 2), self.outcome);
        push(lines(self.error, 2), self.error);
        col.push(Node::Space(Size::Stretch(1)));
        col.push(Node::row(vec![
            Node::button(self.open_result, 140),
            Node::Space(Size::Stretch(1)),
            Node::button(self.cancel, 90),
            Node::button(self.back, 90),
            Node::button(self.next, 110),
        ]));
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        let combos: Vec<HWND> = self.rows.iter().map(|r| r.mode).collect();
        for (h, mut r) in layout::layout(&tree, area, dpi) {
            if combos.contains(&h) {
                // A drop-down list's window height includes its open list.
                r.h *= 8;
            }
            ui::move_to(h, r);
        }
    }
}

impl Handler for ResizeImageWindow {
    fn command(&self, from: HWND, code: u32) {
        if self.sync.active() {
            return;
        }
        let page = self.ctrl.borrow().page();
        let row_at =
            |pick: &dyn Fn(&RowView) -> HWND| self.rows.iter().position(|r| pick(r) == from);
        let index_of = |i: usize| self.ctrl.borrow().rows().get(i).map(|r| r.index);
        let action = if code == EN_CHANGE && from == self.disk {
            A::SetDiskSize(ui::text(self.disk))
        } else if code == BN_CLICKED && from == self.check {
            A::SetCheck(checked(self.check))
        } else if code == BN_CLICKED && from == self.anyway {
            A::ContinueAnyway(checked(self.anyway))
        } else if code == BN_CLICKED && from == self.repair {
            let c = self.ctrl.borrow();
            let Some(r) = c.checks().iter().find(|r| repairable(&r.verdict)) else {
                return;
            };
            A::Repair(r.index)
        } else if code == CBN_SELCHANGE && row_at(&|r| r.mode).is_some() {
            let i = row_at(&|r| r.mode).unwrap_or(0);
            let mode = picked(from).and_then(|p| self.rows[i].modes.borrow().get(p).copied());
            let (Some(mode), Some(index)) = (mode, index_of(i)) else {
                return;
            };
            A::SetMode { index, mode }
        } else if code == EN_CHANGE && row_at(&|r| r.size).is_some() {
            let i = row_at(&|r| r.size).unwrap_or(0);
            let Some(index) = index_of(i) else { return };
            A::SetSizeText {
                index,
                text: ui::text(from),
            }
        } else if code == BN_CLICKED && from == self.copy {
            A::SetInPlace(false)
        } else if code == BN_CLICKED && from == self.in_place {
            A::SetInPlace(true)
        } else if code == BN_CLICKED && from == self.change_output {
            A::PickOutput
        } else if code == BN_CLICKED && from == self.back {
            A::Back
        } else if code == BN_CLICKED && from == self.next {
            match page {
                Page::Review => A::Start,
                _ => A::Next,
            }
        } else if code == BN_CLICKED && from == self.cancel {
            match page {
                Page::Work => A::Cancel,
                _ => A::Close,
            }
        } else if code == BN_CLICKED && from == self.open_result {
            A::OpenResult
        } else {
            return;
        };
        self.apply(action);
    }

    fn notify(&self, _hdr: &NMHDR, _lp: LPARAM) -> Option<LRESULT> {
        None
    }

    fn resized(&self) {
        self.layout();
    }

    fn close(&self) {
        let running = self.ctrl.borrow().is_running();
        self.apply(if running { A::Cancel } else { A::Close });
    }
}

impl Drop for ResizeImageWindow {
    fn drop(&mut self) {
        panel::unregister(self.window);
        // SAFETY: the controller closed its state; the window goes with it.
        let _ = unsafe { DestroyWindow(self.window) };
    }
}
