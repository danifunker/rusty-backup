//! Expand HFS Volume over `controller::expand_hfs`; `src/gui/expand_hfs_dialog.rs` is the reference.

use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use rusty_backup::controller::app::Notify;
use rusty_backup::controller::expand_hfs::{
    ids, labels, BlockChoice, ExpandHfsAction, ExpandHfsController, ExpandSource,
};
use rusty_backup::controller::{Controller, Effect};
use windows::core::w;
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, WPARAM};
use windows::Win32::UI::Controls::{BST_CHECKED, NMHDR};
use windows::Win32::UI::WindowsAndMessaging::{
    DestroyWindow, SendMessageW, ShowWindow, BM_GETCHECK, BM_SETCHECK, BN_CLICKED,
    BS_AUTORADIOBUTTON, CBN_SELCHANGE, CBS_DROPDOWNLIST, CB_ADDSTRING, CB_GETCURSEL,
    CB_RESETCONTENT, CB_SETCURSEL, EN_CHANGE, ES_NUMBER, SW_SHOW, WINDOW_EX_STYLE, WINDOW_STYLE,
    WS_EX_CLIENTEDGE, WS_GROUP, WS_TABSTOP, WS_VSCROLL,
};

use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::layout::{self, Node, Size};
use crate::panel::{self, Handler};
use crate::sync::SyncGuard;
use crate::ui;

const MIB: u64 = 1024 * 1024;
const HFV_NOTE: &str = "Bare classic-HFS volume with no partition table: BasiliskII / \
    SheepShaver mount it as .hfv. Capped at 4063 MB (classic HFS limit).";

pub struct ExpandHfsWindow {
    ctrl: RefCell<ExpandHfsController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<ExpandHfsWindow>,
    window: HWND,
    facts: HWND,
    apm: HWND,
    flat: HWND,
    note: HWND,
    block: HWND,
    blocks: RefCell<Vec<BlockChoice>>,
    size: HWND,
    output: HWND,
    save: HWND,
    phase: HWND,
    outcome: HWND,
    lines: HWND,
    expand: HWND,
    close: HWND,
    labels: Vec<HWND>,
}

impl ExpandHfsWindow {
    pub fn open(
        owner: HWND,
        notify: Arc<dyn Notify>,
        host: EffectHost,
        source: ExpandSource,
    ) -> Rc<Self> {
        let window = panel::dialog(owner, "Expand HFS Volume", 560, 520);
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
        let source_head = ui::label(window, "Source");
        let facts = ui::label(window, "");
        let target_head = ui::label(window, "Target");
        let format_label = ui::label(window, "Output format:");
        let apm = radio("APM disk (.hda)", true, ids::OUTPUT_FORMAT);
        let flat = radio("Bare HFS image (.hfv / .img)", false, "");
        let note = ui::label(window, HFV_NOTE);
        let block_label = ui::label(window, "Allocation block size:");
        let style = (CBS_DROPDOWNLIST as u32) | WS_TABSTOP.0 | WS_VSCROLL.0;
        let block = ui::child(
            window,
            w!("COMBOBOX"),
            "",
            WINDOW_STYLE(style),
            WINDOW_EX_STYLE(0),
            ids::BLOCK_SIZE,
        );
        let size_label = ui::label(window, &format!("{}:", labels::TARGET_SIZE));
        let size = ui::child(
            window,
            w!("EDIT"),
            "",
            WINDOW_STYLE((ES_NUMBER as u32) | WS_TABSTOP.0),
            WS_EX_CLIENTEDGE,
            ids::TARGET_SIZE,
        );
        let out_label = ui::label(window, "Output file:");
        let output = ui::label(window, "(not chosen)");
        let save = ui::button(window, labels::SAVE_AS, ids::SAVE_AS);
        let phase = ui::label(window, "");
        let outcome = ui::label(window, "");
        let lines = ui::label(window, "");
        let expand = ui::button(window, labels::EXPAND, ids::EXPAND);
        let close = ui::button(window, "Cancel", ids::CLOSE);
        let labels = vec![
            source_head,
            target_head,
            format_label,
            block_label,
            size_label,
            out_label,
        ];
        let me = Rc::new_cyclic(|me: &Weak<ExpandHfsWindow>| ExpandHfsWindow {
            ctrl: RefCell::new(ExpandHfsController::new(notify)),
            host,
            sync: SyncGuard::default(),
            me: me.clone(),
            window,
            facts,
            apm,
            flat,
            note,
            block,
            blocks: RefCell::default(),
            size,
            output,
            save,
            phase,
            outcome,
            lines,
            expand,
            close,
            labels,
        });
        let weak: Weak<dyn Handler> = me.me.clone();
        panel::register(window, weak);
        me.layout();
        me.apply(ExpandHfsAction::Open(source));
        // SAFETY: showing the window we created.
        unsafe {
            let _ = ShowWindow(window, SW_SHOW);
        }
        me
    }

    pub fn apply(&self, action: ExpandHfsAction) {
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

    #[allow(dead_code)] // the tabs' one-disk-job lock, once a second tab runs jobs
    pub fn is_running(&self) -> bool {
        self.ctrl.borrow().is_running()
    }

    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            let Some(d) = me.upgrade() else { return };
            let action = match reply {
                Reply::Picked { purpose, path } => ExpandHfsAction::Picked { purpose, path },
                // Expand HFS asks only for its output file.
                Reply::Confirmed { .. }
                | Reply::Answered { .. }
                | Reply::Chose { .. }
                | Reply::CloseScreen => return,
            };
            d.apply(action);
        })
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        let log = self.ctrl.borrow_mut().take_log();
        crate::app::append_log(log);
        if self.ctrl.borrow().source().is_none() {
            panel::unregister(self.window);
            // SAFETY: the controller closed; the window goes with it.
            let _ = unsafe { DestroyWindow(self.window) };
            return;
        }
        self.sync.run(|| self.refresh());
    }

    /// Read the controller, then set the controls; only changed values are set.
    fn refresh(&self) {
        let c = self.ctrl.borrow();
        if let Some(src) = c.source() {
            let block = match src.source_block_size % 1024 {
                0 => format!("{} KiB", src.source_block_size / 1024),
                _ => format!("{} bytes", src.source_block_size),
            };
            ui::set_text(
                self.facts,
                &format!(
                    "Volume: {}\r\nSize: {} MiB ({} bytes)\r\nAllocation block size: {block}\r\nUsed: {} MiB across {} file(s) in {} folder(s)",
                    src.volume_name,
                    src.partition_size / MIB,
                    src.partition_size,
                    src.used_bytes / MIB,
                    src.file_count,
                    src.dir_count
                ),
            );
        }
        let editable = c.editable();
        for h in [self.apm, self.flat, self.block, self.size, self.save] {
            ui::set_enabled(h, editable);
        }
        let flat = c.flat_hfv();
        set_check(self.apm, !flat);
        set_check(self.flat, flat);
        ui::set_visible(self.note, flat);
        let blocks = c.block_choices();
        if *self.blocks.borrow() != blocks {
            // SAFETY: plain combo box messages; the strings are copied by the control.
            unsafe {
                SendMessageW(self.block, CB_RESETCONTENT, None, None);
                for b in &blocks {
                    let t = ui::wide(&b.label);
                    SendMessageW(
                        self.block,
                        CB_ADDSTRING,
                        None,
                        Some(LPARAM(t.as_ptr() as isize)),
                    );
                }
            }
            *self.blocks.borrow_mut() = blocks.clone();
        }
        let at = blocks
            .iter()
            .position(|b| b.bytes == c.block_size())
            .unwrap_or(0);
        // SAFETY: plain combo box messages.
        unsafe {
            if SendMessageW(self.block, CB_GETCURSEL, None, None).0 as usize != at {
                SendMessageW(self.block, CB_SETCURSEL, Some(WPARAM(at)), None);
            }
        }
        // The field keeps the user's words while they mean the controller's value.
        let typed: Option<u32> = ui::text(self.size).trim().parse().ok();
        if typed != Some(c.size_mib()) {
            ui::set_text(self.size, &c.size_mib().to_string());
        }
        let (lo, hi) = c.size_bounds();
        ui::set_text(
            self.labels[4],
            &format!("{} ({lo}-{hi}):", labels::TARGET_SIZE),
        );
        ui::set_text(
            self.output,
            &c.output()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "(not chosen)".into()),
        );
        let phase = c.progress().map(|s| {
            if s.phase.is_empty() {
                "Starting...".to_string()
            } else {
                s.phase
            }
        });
        ui::set_text(self.phase, phase.as_deref().unwrap_or(""));
        let outcome = match c.outcome() {
            Some(Ok(())) => labels::DONE.to_string(),
            Some(Err(e)) => format!("Failed: {e}"),
            None => String::new(),
        };
        ui::set_text(self.outcome, &outcome);
        ui::set_text(self.lines, &c.lines().join("\r\n"));
        ui::set_enabled(self.expand, c.can_start());
        ui::set_enabled(self.close, !c.is_running());
        ui::set_text(
            self.close,
            if c.outcome().is_some() {
                "Close"
            } else {
                "Cancel"
            },
        );
    }

    fn layout(&self) {
        let (area, dpi) = panel::client(self.window);
        let text = |h: HWND, lines: i32| {
            Node::leaf(h, Size::Stretch(1), Size::Fixed((layout::ROW - 8) * lines))
        };
        let caption = |h: HWND| Node::leaf(h, Size::Fixed(150), Size::Fixed(layout::ROW - 8));
        let l = &self.labels;
        let tree = Node::inset(
            Node::column(vec![
                text(l[0], 1),
                text(self.facts, 4),
                text(l[1], 1),
                Node::row(vec![
                    caption(l[2]),
                    Node::button(self.apm, 150),
                    Node::leaf(self.flat, Size::Stretch(1), Size::Fixed(layout::ROW)),
                ]),
                text(self.note, 2),
                Node::row(vec![
                    caption(l[3]),
                    Node::leaf(self.block, Size::Fixed(200), Size::Fixed(layout::ROW)),
                ]),
                Node::row(vec![
                    caption(l[4]),
                    Node::leaf(self.size, Size::Fixed(100), Size::Fixed(layout::ROW)),
                ]),
                Node::row(vec![
                    caption(l[5]),
                    Node::leaf(self.output, Size::Stretch(1), Size::Fixed(layout::ROW - 8)),
                    Node::button(self.save, 90),
                ]),
                text(self.phase, 1),
                text(self.outcome, 1),
                Node::leaf(self.lines, Size::Stretch(1), Size::Stretch(1)),
                Node::row(vec![
                    Node::Space(Size::Stretch(1)),
                    Node::button(self.expand, 90),
                    Node::button(self.close, 90),
                ]),
            ]),
            layout::MARGIN,
        );
        for (h, mut r) in layout::layout(&tree, area, dpi) {
            if h == self.block {
                // A drop-down list's window height includes its open list.
                r.h *= 8;
            }
            ui::move_to(h, r);
        }
    }
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

impl Handler for ExpandHfsWindow {
    fn command(&self, from: HWND, code: u32) {
        if self.sync.active() {
            return;
        }
        let action = if code == BN_CLICKED && from == self.apm {
            ExpandHfsAction::SetFlatHfv(false)
        } else if code == BN_CLICKED && from == self.flat {
            ExpandHfsAction::SetFlatHfv(true)
        } else if code == CBN_SELCHANGE && from == self.block {
            // SAFETY: a plain combo box query.
            let i = unsafe { SendMessageW(self.block, CB_GETCURSEL, None, None).0 };
            let Some(b) = usize::try_from(i)
                .ok()
                .and_then(|i| self.blocks.borrow().get(i).map(|b| b.bytes))
            else {
                return;
            };
            ExpandHfsAction::SetBlockSize(b)
        } else if code == EN_CHANGE && from == self.size {
            let Ok(mib) = ui::text(self.size).trim().parse::<u32>() else {
                return;
            };
            ExpandHfsAction::SetSizeMib(mib)
        } else if code == BN_CLICKED && from == self.save {
            ExpandHfsAction::PickOutput
        } else if code == BN_CLICKED && from == self.expand {
            ExpandHfsAction::Start
        } else if code == BN_CLICKED && from == self.close {
            ExpandHfsAction::Close
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
        self.apply(ExpandHfsAction::Close);
    }
}
