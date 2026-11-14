//! Resize Partitions over `controller::resize`; the Win32 shell's `resize.rs` is the reference.
//!
//! The size grid is one row of controls per partition, built again only when the rows' shape
//! (their partitions and size-mode choices) changes. The preview is a table, refilled when the
//! controller's preview changes. Apply asks the controller, whose own question confirms it.

use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};
use std::sync::Arc;

use objc2::rc::Retained;
use objc2::MainThreadMarker;
use objc2_app_kit::NSWindow;
use rusty_backup::controller::app::{DiskDevice, Notify};
use rusty_backup::controller::inspect::{format_size, ResizePlan};
use rusty_backup::controller::resize::{
    ids, PreviewRow, ResizeAction, ResizeController, ResizeRow, SizeMode,
};
use rusty_backup::controller::{Controller, Effect};

use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::Form;
use crate::layout::{self, Node, Size};
use crate::partition_bar::{self, Bar};
use crate::sync::SyncGuard;
use crate::table::Table;
use crate::ui::{self, CloseDelegate};

const HEADS: [&str; 6] = [
    "#",
    "Type",
    "Current Size",
    "Min Size",
    "Size Mode",
    "New Size (MiB)",
];
const WIDTHS: [i32; 5] = [40, 160, 110, 110, 140];

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum K {
    Warn,
    Risk,
    DiskLine,
    CurrentCaption,
    Current,
    AfterCaption,
    After,
    SizesHead,
    Head(usize),
    Cell(usize, usize),
    Mode(usize),
    Size(usize),
    Error,
    PreviewHead,
    Preview,
    Job,
    Target,
    PreviewButton,
    Apply,
    Cancel,
    Close,
}

pub struct ResizeWindow {
    ctrl: RefCell<ResizeController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<ResizeWindow>,
    window: Retained<NSWindow>,
    _delegate: Retained<CloseDelegate>,
    form: Form<K>,
    current: Retained<Bar>,
    after: Retained<Bar>,
    preview: Table,
    /// Each row's partition and its size-mode choices, as built.
    shape: RefCell<Vec<(usize, bool, Vec<SizeMode>)>>,
    shown_preview: RefCell<Option<Vec<PreviewRow>>>,
    changed: Cell<bool>,
}

impl ResizeWindow {
    pub fn open(
        mtm: MainThreadMarker,
        notify: Arc<dyn Notify>,
        host: EffectHost,
        plan: ResizePlan,
    ) -> Rc<ResizeWindow> {
        let me = Rc::new_cyclic(|me: &Weak<ResizeWindow>| {
            let close = me.clone();
            let (window, delegate) = ui::window(
                mtm,
                "Resize Partitions",
                720.0,
                600.0,
                Box::new(move || {
                    if let Some(d) = close.upgrade() {
                        if !d.ctrl.borrow().is_running() {
                            d.send(ResizeAction::Close);
                        }
                    }
                }),
            );
            let resize = me.clone();
            delegate.on_resize(Box::new(move || {
                if let Some(d) = resize.upgrade() {
                    d.layout();
                }
            }));
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
            f.wrap(
                K::Warn,
                "WARNING: This will directly modify the physical device. Data loss is permanent and irreversible!",
            );
            f.set_tone(
                K::Warn,
                Some(rusty_backup::controller::commander::Tone::Error),
            );
            f.check(
                K::Risk,
                "I understand the risks and want to proceed",
                ids::DEVICE_RISK,
            );
            f.label(K::DiskLine, "");
            f.label(K::CurrentCaption, "");
            let current = Bar::new(mtm, "resize.current_bar");
            current.set_options(1.0, false);
            f.add(K::Current, &current);
            f.label(K::AfterCaption, "");
            let after = Bar::new(mtm, "resize.after_bar");
            after.set_options(1.0, true);
            f.add(K::After, &after);
            f.label(K::SizesHead, "Partition Sizes:");
            for (i, t) in HEADS.iter().enumerate() {
                f.label(K::Head(i), t);
            }
            f.label(K::Error, "");
            f.label(K::PreviewHead, "Preview:");
            let preview = Table::new(
                mtm,
                "resize.preview_list",
                &[
                    ("#", 40.0, true),
                    ("Old Start", 110.0, true),
                    ("Old Size", 110.0, true),
                    ("New Start", 110.0, true),
                    ("New Size", 110.0, true),
                    ("Action", 180.0, false),
                ],
            );
            f.add(K::Preview, &preview.scroll);
            f.label(K::Job, "");
            f.label(K::Target, "");
            f.button(K::PreviewButton, "Preview", ids::PREVIEW);
            f.button(K::Apply, "Apply", ids::APPLY);
            f.button(K::Cancel, "Cancel", ids::CANCEL);
            f.button(K::Close, "Close", ids::CLOSE);
            ui::present(&window);
            ResizeWindow {
                ctrl: RefCell::new(ResizeController::new(notify)),
                host,
                sync: SyncGuard::default(),
                me: me.clone(),
                window,
                _delegate: delegate,
                form,
                current,
                after,
                preview,
                shape: RefCell::default(),
                shown_preview: RefCell::default(),
                changed: Cell::new(false),
            }
        });
        me.apply(ResizeAction::Open(Box::new(plan)));
        me
    }

    pub fn apply(&self, action: ResizeAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    pub fn tick(&self) {
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    /// A refreshed device list: the device being resized must still be in it.
    pub fn set_devices(&self, devices: &[DiskDevice]) {
        self.ctrl.borrow_mut().reconcile_devices(devices);
    }

    pub fn is_open(&self) -> bool {
        self.ctrl.borrow().is_open()
    }

    pub fn is_running(&self) -> bool {
        self.ctrl.borrow().is_running()
    }

    /// A write ended since the last call, so the host reads the table again.
    pub fn take_changed(&self) -> bool {
        self.changed.replace(false)
    }

    /// Send `action` once the current handler returns: the window may close with it.
    fn send(&self, action: ResizeAction) {
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
                Reply::Confirmed { purpose, yes } => ResizeAction::Confirmed { purpose, yes },
                // Resize asks for no file, text or choice, and has no screen to leave.
                Reply::Picked { .. }
                | Reply::Answered { .. }
                | Reply::Chose { .. }
                | Reply::CloseScreen => return,
            };
            d.apply(action);
        })
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        let (log, changed) = {
            let mut c = self.ctrl.borrow_mut();
            (c.take_log(), c.take_disk_changed())
        };
        crate::app::append_log(log);
        if changed {
            self.changed.set(true);
        }
        if !self.ctrl.borrow().is_open() {
            self.window.orderOut(None);
            return;
        }
        self.sync.run(|| self.refresh());
        self.layout();
    }

    fn refresh(&self) {
        let f = &self.form;
        let (device, risk, mut disk_line, bars, rows, error, preview, job, target, running, can) = {
            let c = self.ctrl.borrow();
            (
                c.is_device(),
                c.device_risk_accepted(),
                format!("Disk size: {}", format_size(c.disk_size())),
                c.layout(),
                c.rows().to_vec(),
                c.plan_error().map(|e| format!("Error: {e}")),
                c.preview().map(<[PreviewRow]>::to_vec),
                c.progress(),
                c.write_target().map(|t| format!("Writes to: {t}")),
                c.is_running(),
                c.can_apply(),
            )
        };
        if self.ctrl.borrow().can_grow_container() {
            disk_line.push_str(" - to make more room, close this and use \"Expand Image...\"");
        }
        f.show(K::Warn, device);
        f.show(K::Risk, device);
        f.set_check(K::Risk, risk);
        f.enable(K::Risk, !running);
        f.set_text(K::DiskLine, &disk_line);
        let (current, after, scale) = &bars;
        let scale = (*scale).max(1) as f32;
        f.set_text(K::CurrentCaption, &current.caption);
        f.set_text(K::AfterCaption, &after.caption);
        self.current
            .set_options(current.total_bytes() as f32 / scale, false);
        self.after
            .set_options(after.total_bytes() as f32 / scale, true);
        self.current.show(current.layout_segments(), None);
        self.after.show(after.layout_segments(), None);
        self.show_rows(&rows, running);
        f.set_text(K::Error, error.as_deref().unwrap_or(""));
        f.show(K::PreviewHead, preview.is_some());
        f.show(K::Preview, preview.is_some());
        if *self.shown_preview.borrow() != preview {
            let cells = preview
                .iter()
                .flatten()
                .map(|r| {
                    vec![
                        r.index.to_string(),
                        r.old_start.clone(),
                        r.old_size.clone(),
                        r.new_start.clone(),
                        r.new_size.clone(),
                        r.action.clone(),
                    ]
                })
                .collect();
            self.preview.set_rows(cells);
            *self.shown_preview.borrow_mut() = preview;
        }
        let job = job.map(|j| {
            if j.phase.is_empty() {
                j.label
            } else {
                format!("{}: {}", j.label, j.phase)
            }
        });
        f.set_text(K::Job, job.as_deref().unwrap_or(""));
        f.show(K::Cancel, running);
        f.set_text(K::Target, target.as_deref().unwrap_or(""));
        f.enable(K::PreviewButton, !running);
        f.enable(K::Apply, can);
        f.enable(K::Close, !running);
    }

    fn show_rows(&self, rows: &[ResizeRow], running: bool) {
        let f = &self.form;
        let shape: Vec<(usize, bool, Vec<SizeMode>)> = rows
            .iter()
            .map(|r| (r.index, r.extended, r.mode_choices()))
            .collect();
        if *self.shape.borrow() != shape {
            f.remove(|k| matches!(k, K::Cell(..) | K::Mode(_) | K::Size(_)));
            for row in rows {
                let i = row.index;
                let texts = [
                    row.index.to_string(),
                    row.type_name.clone(),
                    format_size(row.original_size),
                    format_size(row.minimum_size),
                ];
                for (c, t) in texts.iter().enumerate() {
                    f.label(K::Cell(i, c), t);
                }
                let names: Vec<String> = row
                    .mode_choices()
                    .iter()
                    .map(|m| m.label().to_string())
                    .collect();
                f.popup(K::Mode(i), &names, &format!("{}.{i}", ids::SIZE_MODE));
                f.field(
                    K::Size(i),
                    &format!("{}.{i}", ids::NEW_SIZE),
                    "New Size (MiB)",
                    false,
                );
            }
            *self.shape.borrow_mut() = shape;
        }
        for row in rows {
            let i = row.index;
            let choices = row.mode_choices();
            let at = choices.iter().position(|m| *m == row.mode).unwrap_or(0);
            f.select(K::Mode(i), at);
            f.enable(K::Mode(i), !running && !row.extended);
            f.set_text(K::Size(i), &row.text);
            f.enable(
                K::Size(i),
                !running && !row.extended && row.mode == SizeMode::Custom,
            );
        }
    }

    fn layout(&self) {
        let Some(body) = self.window.contentView() else {
            return;
        };
        let f = &self.form;
        let line =
            |k: K, n: i32| Node::leaf(k, Size::Stretch(1), Size::Fixed((layout::ROW - 6) * n + 4));
        let grid = |ks: [K; 6], pop: bool| {
            let mut row: Vec<Node<K>> = ks[..5]
                .iter()
                .zip(WIDTHS)
                .enumerate()
                .map(|(n, (k, w))| {
                    let h = if pop && n == 4 {
                        layout::ROW + 2
                    } else {
                        layout::ROW
                    };
                    Node::leaf(*k, Size::Fixed(w), Size::Fixed(h))
                })
                .collect();
            row.push(Node::leaf(
                ks[5],
                Size::Stretch(1),
                Size::Fixed(layout::ROW),
            ));
            Node::row(row)
        };
        let mut rows = vec![
            line(K::Warn, 2),
            Node::leaf(K::Risk, Size::Stretch(1), Size::Fixed(layout::ROW)),
            line(K::DiskLine, 1),
            line(K::CurrentCaption, 1),
            Node::leaf(
                K::Current,
                Size::Stretch(1),
                Size::Fixed(partition_bar::BAR_ONLY),
            ),
            line(K::AfterCaption, 1),
            Node::leaf(
                K::After,
                Size::Stretch(1),
                Size::Fixed(partition_bar::HEIGHT),
            ),
            line(K::SizesHead, 1),
            grid(
                [
                    K::Head(0),
                    K::Head(1),
                    K::Head(2),
                    K::Head(3),
                    K::Head(4),
                    K::Head(5),
                ],
                false,
            ),
        ];
        for (i, _, _) in self.shape.borrow().iter() {
            let i = *i;
            rows.push(grid(
                [
                    K::Cell(i, 0),
                    K::Cell(i, 1),
                    K::Cell(i, 2),
                    K::Cell(i, 3),
                    K::Mode(i),
                    K::Size(i),
                ],
                true,
            ));
        }
        rows.push(line(K::Error, 1));
        if f.shown(K::Preview) {
            rows.push(line(K::PreviewHead, 1));
            rows.push(Node::leaf(K::Preview, Size::Stretch(1), Size::Stretch(1)));
        } else {
            rows.push(Node::Space(Size::Stretch(1)));
        }
        rows.push(line(K::Job, 1));
        rows.push(line(K::Target, 1));
        rows.push(Node::row(vec![
            Node::button(K::PreviewButton, 95),
            Node::Space(Size::Stretch(1)),
            Node::button(K::Cancel, 90),
            Node::button(K::Apply, 90),
            Node::button(K::Close, 90),
        ]));
        let tree = Node::inset(Node::column(rows), layout::MARGIN);
        f.place_shown(tree, ui::area(&body));
    }

    fn fired(&self, k: K) {
        if self.sync.active() {
            return;
        }
        let f = &self.form;
        let action = match k {
            K::Risk => ResizeAction::AcceptDeviceRisk(f.checked(k)),
            K::PreviewButton => ResizeAction::Preview,
            // Apply asks the controller again at the click; the question is its own.
            K::Apply if self.ctrl.borrow().can_apply() => ResizeAction::Apply,
            K::Cancel => ResizeAction::Cancel,
            K::Close => ResizeAction::Close,
            K::Mode(index) => {
                let shape = self.shape.borrow();
                let Some(mode) = shape
                    .iter()
                    .find(|(i, _, _)| *i == index)
                    .and_then(|(_, _, m)| m.get(f.picked(k)?).copied())
                else {
                    return;
                };
                ResizeAction::SetMode { index, mode }
            }
            K::Size(index) => ResizeAction::SetSizeText {
                index,
                text: f.text(k),
            },
            _ => return,
        };
        self.send(action);
    }
}

impl Drop for ResizeWindow {
    fn drop(&mut self) {
        self.window.setDelegate(None);
        self.window.orderOut(None);
    }
}
