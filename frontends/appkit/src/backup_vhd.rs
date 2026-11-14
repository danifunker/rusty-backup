//! Backup's VHD options window over the Backup controller; the Win32 shell's `backup_vhd.rs` is
//! the reference. Open while `BackupController::vhd_open()` says so: the layout (one disk image,
//! or one .vhd per partition) and a size row per partition, with Calc min where a minimum still
//! needs a walk.

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use objc2::rc::Retained;
use objc2::MainThreadMarker;
use objc2_app_kit::NSWindow;
use rusty_backup::controller::backup::{format_size, ids, BackupAction, SizeMode, VhdRow};

use crate::form::Form;
use crate::layout::{self, Node, Size};
use crate::sync::SyncGuard;
use crate::ui::{self, CloseDelegate};

const LAYOUTS: [&str; 2] = [
    "Whole Disk (single .vhd file)",
    "Per Partition (one .vhd per partition)",
];
const MIB: u64 = 1024 * 1024;

/// What the window shows, read while the controller is borrowed.
pub struct VhdShown {
    pub whole_disk: bool,
    /// Each row with its Calc min walk's phase, while one runs.
    pub rows: Vec<(VhdRow, Option<String>)>,
    pub can_start: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum K {
    Intro,
    LayoutCaption,
    Layout,
    Head(usize),
    Number(usize),
    Kind(usize),
    Mode(usize),
    Mib(usize),
    Calc(usize),
    Phase(usize),
    Start,
    Cancel,
}

pub struct VhdWindow {
    window: Retained<NSWindow>,
    _delegate: Retained<CloseDelegate>,
    form: Form<K>,
    send: Rc<dyn Fn(BackupAction)>,
    sync: SyncGuard,
    rows: Vec<usize>,
    choices: RefCell<Vec<Vec<SizeMode>>>,
}

impl VhdWindow {
    /// The window, with a row per partition the controller offered; `send` delivers its edits.
    pub fn open(
        mtm: MainThreadMarker,
        send: Rc<dyn Fn(BackupAction)>,
        rows: &[VhdRow],
    ) -> Rc<Self> {
        Rc::new_cyclic(|me: &Weak<VhdWindow>| {
            let close = send.clone();
            let (window, delegate) = ui::window(
                mtm,
                "VHD Backup Options",
                640.0,
                420.0,
                Box::new(move || close(BackupAction::VhdClose)),
            );
            let resize = me.clone();
            delegate.on_resize(Box::new(move || {
                if let Some(w) = resize.upgrade() {
                    w.layout();
                }
            }));
            let body = window.contentView().expect("the window's content view");
            let fire = me.clone();
            let form = Form::new(
                mtm,
                body,
                Rc::new(move |k| {
                    if let Some(w) = fire.upgrade() {
                        w.fired(k);
                    }
                }),
            );
            form.label(K::Intro, "Configure VHD output.");
            form.label(K::LayoutCaption, "Layout:");
            let layouts: Vec<String> = LAYOUTS.iter().map(|s| s.to_string()).collect();
            form.popup(K::Layout, &layouts, ids::VHD_LAYOUT);
            for (i, t) in ["#", "Type", "Size Mode", "Size (MiB)"].iter().enumerate() {
                form.label(K::Head(i), t);
            }
            for r in rows {
                let i = r.index;
                let id = format!("{}.{i}", ids::VHD_SIZE);
                form.label(K::Number(i), &i.to_string());
                form.label(K::Kind(i), &r.type_name);
                form.popup(K::Mode(i), &[], &id);
                form.field(K::Mib(i), &format!("{id}.mib"), "Size (MiB)", false);
                form.button(K::Calc(i), "Calc min", &format!("{id}.calc"));
                form.label(K::Phase(i), "");
            }
            form.button(K::Start, "Start VHD Backup", ids::VHD_START);
            form.button(K::Cancel, "Cancel", ids::VHD_CANCEL);
            ui::present(&window);
            VhdWindow {
                window,
                _delegate: delegate,
                form,
                send,
                sync: SyncGuard::default(),
                rows: rows.iter().map(|r| r.index).collect(),
                choices: RefCell::new(vec![Vec::new(); rows.len()]),
            }
        })
    }

    /// The partitions this window was built for; other rows need a new window.
    pub fn indices(&self) -> Vec<usize> {
        self.rows.clone()
    }

    pub fn show(&self, s: VhdShown) {
        let f = &self.form;
        self.sync.run(|| {
            f.select(K::Layout, usize::from(!s.whole_disk));
            f.enable(K::Start, s.can_start);
            for (n, i) in self.rows.iter().enumerate() {
                let Some((row, phase)) = s.rows.iter().find(|(r, _)| r.index == *i) else {
                    continue;
                };
                let choices = row.mode_choices();
                let names: Vec<String> = choices.iter().map(|m| m.label().to_string()).collect();
                f.set_items(K::Mode(*i), &names);
                f.select(
                    K::Mode(*i),
                    choices.iter().position(|m| *m == row.mode).unwrap_or(0),
                );
                self.choices.borrow_mut()[n] = choices;
                let custom = row.mode == SizeMode::Custom;
                let mib = if custom {
                    let (lo, hi) = row.custom_bounds_mib();
                    row.custom_mib.clamp(lo, hi)
                } else {
                    (row.effective_size() / MIB) as u32
                };
                f.set_text(K::Mib(*i), &mib.to_string());
                f.enable(K::Mib(*i), custom);
                let walk = row.deferred_fs.filter(|_| phase.is_none());
                f.show(K::Calc(*i), walk.is_some());
                let note = match (phase, walk) {
                    (Some(p), _) => p.clone(),
                    (None, Some(fs)) => format!("walks the {fs} volume"),
                    (None, None) => format!("min {}", format_size(row.minimum_size)),
                };
                f.set_text(K::Phase(*i), &note);
            }
        });
        self.layout();
    }

    fn layout(&self) {
        let Some(body) = self.window.contentView() else {
            return;
        };
        let row_h = Size::Fixed(layout::ROW);
        let fixed = |k: K, w: i32| Node::leaf(k, Size::Fixed(w), row_h);
        let widths = [30, 200, 160, 90];
        let mut col = vec![
            Node::leaf(K::Intro, Size::Stretch(1), Size::Fixed(layout::ROW - 4)),
            Node::row(vec![
                fixed(K::LayoutCaption, 70),
                fixed(K::Layout, 320),
                Node::Space(Size::Stretch(1)),
            ]),
        ];
        if !self.rows.is_empty() {
            col.push(Node::row(
                (0..4)
                    .zip(widths)
                    .map(|(h, w)| fixed(K::Head(h), w))
                    .collect(),
            ));
        }
        for i in &self.rows {
            let i = *i;
            col.push(Node::row(vec![
                fixed(K::Number(i), widths[0]),
                fixed(K::Kind(i), widths[1]),
                fixed(K::Mode(i), widths[2]),
                fixed(K::Mib(i), widths[3]),
                Node::button(K::Calc(i), 90),
                Node::leaf(K::Phase(i), Size::Stretch(1), row_h),
            ]));
        }
        col.push(Node::Space(Size::Stretch(1)));
        col.push(Node::row(vec![
            Node::Space(Size::Stretch(1)),
            Node::button(K::Cancel, 90),
            Node::button(K::Start, 150),
        ]));
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        self.form.place_shown(tree, ui::area(&body));
    }

    fn size_action(&self, i: usize, mode: Option<SizeMode>) -> Option<BackupAction> {
        let n = self.rows.iter().position(|r| *r == i)?;
        let mode = match mode {
            Some(m) => m,
            None => *self
                .choices
                .borrow()
                .get(n)?
                .get(self.form.picked(K::Mode(i))?)?,
        };
        let custom_mib = self.form.text(K::Mib(i)).trim().parse::<u32>().unwrap_or(0);
        Some(BackupAction::VhdSize {
            index: i,
            mode,
            custom_mib,
        })
    }

    fn fired(&self, k: K) {
        if self.sync.active() {
            return;
        }
        let action = match k {
            K::Start => Some(BackupAction::VhdStart),
            K::Cancel => Some(BackupAction::VhdClose),
            K::Layout => self
                .form
                .picked(K::Layout)
                .map(|i| BackupAction::VhdWholeDisk(i == 0)),
            K::Calc(i) => Some(BackupAction::CalcMin(i)),
            K::Mode(i) => self.size_action(i, None),
            K::Mib(i) => self.size_action(i, Some(SizeMode::Custom)),
            _ => None,
        };
        if let Some(a) = action {
            // The window may be rebuilt by what this sends: act once the handler returns.
            let send = self.send.clone();
            crate::app::later(Box::new(move || send(a)));
        }
    }
}

impl Drop for VhdWindow {
    fn drop(&mut self) {
        self.window.setDelegate(None);
        self.window.orderOut(None);
    }
}
