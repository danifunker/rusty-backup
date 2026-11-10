//! Backup's VHD options window over the Backup controller; GTK's `backup/windows.rs` is the reference.
//!
//! Open while `BackupController::vhd_open()` says so: the layout (one disk image, or one .vhd per
//! partition) and a size row per partition, with Calc min where a minimum still needs a walk.

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use rusty_backup::controller::backup::{format_size, ids, BackupAction, SizeMode, VhdRow};
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT};
use windows::Win32::UI::Controls::NMHDR;
use windows::Win32::UI::Input::KeyboardAndMouse::GetFocus;
use windows::Win32::UI::WindowsAndMessaging::{
    DestroyWindow, ShowWindow, BN_CLICKED, CBN_SELCHANGE, EN_CHANGE, ES_NUMBER, SW_SHOW,
    WINDOW_STYLE, WS_EX_CLIENTEDGE, WS_TABSTOP,
};

use crate::combo::Combo;
use crate::layout::{self, Node, Size};
use crate::panel::{self, Handler};
use crate::sync::SyncGuard;
use crate::ui;

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

/// One partition's size row.
struct Cells {
    index: usize,
    number: HWND,
    kind: HWND,
    mode: Combo,
    choices: RefCell<Vec<SizeMode>>,
    mib: HWND,
    calc: HWND,
    phase: HWND,
}

pub struct VhdWindow {
    window: HWND,
    send: Rc<dyn Fn(BackupAction)>,
    sync: SyncGuard,
    me: Weak<VhdWindow>,
    intro: HWND,
    layout_caption: HWND,
    layout: Combo,
    heads: [HWND; 4],
    cells: Vec<Cells>,
    start: HWND,
    cancel: HWND,
    shape: RefCell<Vec<bool>>,
}

impl VhdWindow {
    /// The window, with a row per partition the controller offered; `send` delivers its edits.
    pub fn open(owner: HWND, send: Rc<dyn Fn(BackupAction)>, rows: &[VhdRow]) -> Rc<Self> {
        let window = panel::dialog(owner, "VHD Backup Options", 640, 420);
        let cells = rows
            .iter()
            .map(|r| {
                let id = format!("{}.{}", ids::VHD_SIZE, r.index);
                let style = WINDOW_STYLE(ES_NUMBER as u32 | WS_TABSTOP.0);
                Cells {
                    index: r.index,
                    number: ui::cell_label(window, &r.index.to_string()),
                    kind: ui::cell_label(window, &ui::plain(&r.type_name)),
                    mode: Combo::build(window, &id, &[]),
                    choices: RefCell::default(),
                    mib: ui::child(
                        window,
                        windows::core::w!("EDIT"),
                        "",
                        style,
                        WS_EX_CLIENTEDGE,
                        &format!("{id}.mib"),
                    ),
                    calc: ui::button(window, "Calc min", &format!("{id}.calc")),
                    phase: ui::cell_label(window, ""),
                }
            })
            .collect();
        let me = Rc::new_cyclic(|me: &Weak<VhdWindow>| VhdWindow {
            window,
            send,
            sync: SyncGuard::default(),
            me: me.clone(),
            intro: ui::label(window, "Configure VHD output."),
            layout_caption: ui::cell_label(window, "Layout:"),
            layout: Combo::build(window, ids::VHD_LAYOUT, &LAYOUTS),
            heads: ["#", "Type", "Size Mode", "Size (MiB)"].map(|t| ui::cell_label(window, t)),
            cells,
            start: ui::button(window, "Start VHD Backup", ids::VHD_START),
            cancel: ui::button(window, "Cancel", ids::VHD_CANCEL),
            shape: RefCell::default(),
        });
        let weak: Weak<dyn Handler> = me.me.clone();
        panel::register(window, weak);
        // SAFETY: showing the window we created.
        unsafe {
            let _ = ShowWindow(window, SW_SHOW);
        }
        me
    }

    /// The partitions this window was built for; other rows need a new window.
    pub fn indices(&self) -> Vec<usize> {
        self.cells.iter().map(|c| c.index).collect()
    }

    pub fn show(&self, s: VhdShown) {
        self.sync.run(|| {
            self.layout.select(usize::from(!s.whole_disk));
            ui::set_enabled(self.start, s.can_start);
            // SAFETY: a plain focus query: the field being typed in keeps the user's text.
            let focus = unsafe { GetFocus() };
            for c in &self.cells {
                let Some((row, phase)) = s.rows.iter().find(|(r, _)| r.index == c.index) else {
                    continue;
                };
                let choices = row.mode_choices();
                let names: Vec<String> = choices.iter().map(|m| m.label().to_string()).collect();
                c.mode.set_items(&names);
                let at = choices.iter().position(|m| *m == row.mode).unwrap_or(0);
                c.mode.select(at);
                *c.choices.borrow_mut() = choices;
                let custom = row.mode == SizeMode::Custom;
                let mib = if custom {
                    let (lo, hi) = row.custom_bounds_mib();
                    row.custom_mib.clamp(lo, hi)
                } else {
                    (row.effective_size() / MIB) as u32
                };
                if c.mib != focus {
                    ui::set_text(c.mib, &mib.to_string());
                }
                ui::set_enabled(c.mib, custom);
                let walk = row.deferred_fs.filter(|_| phase.is_none());
                ui::set_visible(c.calc, walk.is_some());
                let note = match (phase, walk) {
                    (Some(p), _) => p.clone(),
                    (None, Some(fs)) => format!("walks the {fs} volume"),
                    (None, None) => format!("min {}", format_size(row.minimum_size)),
                };
                ui::set_text(c.phase, &note);
            }
        });
        if *self.shape.borrow() != self.shape_now() {
            self.layout();
        }
    }

    fn shape_now(&self) -> Vec<bool> {
        self.cells.iter().map(|c| ui::shown(c.calc)).collect()
    }

    fn layout(&self) {
        *self.shape.borrow_mut() = self.shape_now();
        let (area, dpi) = panel::client(self.window);
        let row_h = Size::Fixed(layout::ROW);
        let fixed = |h: HWND, w: i32| Node::leaf(h, Size::Fixed(w), row_h);
        let widths = [30, 200, 150, 90];
        let mut col = vec![
            Node::leaf(self.intro, Size::Stretch(1), Size::Fixed(layout::ROW - 8)),
            Node::row(vec![
                fixed(self.layout_caption, 70),
                fixed(self.layout.h, 300),
                Node::Space(Size::Stretch(1)),
            ]),
        ];
        if !self.cells.is_empty() {
            col.push(Node::row(
                self.heads
                    .iter()
                    .zip(widths)
                    .map(|(h, w)| fixed(*h, w))
                    .collect(),
            ));
        }
        for c in &self.cells {
            let mut row: Vec<Node<HWND>> = [c.number, c.kind, c.mode.h, c.mib]
                .iter()
                .zip(widths)
                .map(|(h, w)| fixed(*h, w))
                .collect();
            if ui::shown(c.calc) {
                row.push(Node::button(c.calc, 80));
            }
            row.push(Node::leaf(c.phase, Size::Stretch(1), row_h));
            col.push(Node::row(row));
        }
        col.push(Node::Space(Size::Stretch(1)));
        col.push(Node::row(vec![
            Node::Space(Size::Stretch(1)),
            Node::button(self.cancel, 90),
            Node::button(self.start, 140),
        ]));
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        for (h, r) in layout::layout(&tree, area, dpi) {
            if ui::shown(h) {
                ui::move_to(h, r);
            }
        }
    }

    fn size_action(&self, c: &Cells, mode: Option<SizeMode>) -> Option<BackupAction> {
        let mode = match mode {
            Some(m) => m,
            None => *c.choices.borrow().get(c.mode.picked()?)?,
        };
        let custom_mib = ui::text(c.mib).trim().parse::<u32>().unwrap_or(0);
        Some(BackupAction::VhdSize {
            index: c.index,
            mode,
            custom_mib,
        })
    }
}

impl Drop for VhdWindow {
    fn drop(&mut self) {
        panel::unregister(self.window);
        // SAFETY: the controller closed the VHD options; the window goes with it.
        let _ = unsafe { DestroyWindow(self.window) };
    }
}

impl Handler for VhdWindow {
    fn command(&self, from: HWND, code: u32) {
        if self.sync.active() {
            return;
        }
        let action = if code == BN_CLICKED && from == self.start {
            Some(BackupAction::VhdStart)
        } else if code == BN_CLICKED && from == self.cancel {
            Some(BackupAction::VhdClose)
        } else if code == CBN_SELCHANGE && from == self.layout.h {
            self.layout
                .picked()
                .map(|i| BackupAction::VhdWholeDisk(i == 0))
        } else if let Some(c) = self.cells.iter().find(|c| c.calc == from) {
            (code == BN_CLICKED).then_some(BackupAction::CalcMin(c.index))
        } else if let Some(c) = self.cells.iter().find(|c| c.mode.h == from) {
            if code == CBN_SELCHANGE {
                self.size_action(c, None)
            } else {
                None
            }
        } else if let Some(c) = self.cells.iter().find(|c| c.mib == from) {
            if code == EN_CHANGE {
                self.size_action(c, Some(SizeMode::Custom))
            } else {
                None
            }
        } else {
            None
        };
        if let Some(a) = action {
            (self.send)(a);
        }
    }

    fn notify(&self, _hdr: &NMHDR, _lp: LPARAM) -> Option<LRESULT> {
        None
    }

    fn resized(&self) {
        self.layout();
    }

    fn close(&self) {
        (self.send)(BackupAction::VhdClose);
    }
}
