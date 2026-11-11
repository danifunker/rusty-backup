//! Resize Alto Disk over `controller::alto_resize`; `src/gui/alto_resize_dialog.rs` is the reference.

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use rusty_backup::controller::alto_resize::{
    ids, AltoResizeAction, AltoResizeController, AltoTarget,
};
use rusty_backup::controller::app::Notify;
use rusty_backup::controller::{Controller, Effect};
use windows::core::w;
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, WPARAM};
use windows::Win32::UI::Controls::{BST_CHECKED, NMHDR};
use windows::Win32::UI::WindowsAndMessaging::{
    DestroyWindow, SendMessageW, ShowWindow, BM_GETCHECK, BM_SETCHECK, BN_CLICKED,
    BS_AUTORADIOBUTTON, SW_SHOW, WINDOW_EX_STYLE, WINDOW_STYLE, WS_GROUP, WS_TABSTOP,
};

use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::layout::{self, Node, Size};
use crate::panel::{self, Handler};
use crate::sync::SyncGuard;
use crate::ui;

pub struct AltoWindow {
    ctrl: RefCell<AltoResizeController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<AltoWindow>,
    window: HWND,
    source: HWND,
    summary: HWND,
    caption: HWND,
    targets: Vec<HWND>,
    warning: HWND,
    result: HWND,
    save: HWND,
    close: HWND,
}

impl AltoWindow {
    pub fn open(owner: HWND, notify: Arc<dyn Notify>, host: EffectHost, path: PathBuf) -> Rc<Self> {
        let window = panel::dialog(owner, "Resize Alto Disk", 520, 340);
        let source = ui::path_label(window, "");
        let summary = ui::label(window, "");
        let caption = ui::label(window, "Target geometry:");
        let targets = AltoTarget::ALL
            .iter()
            .enumerate()
            .map(|(i, t)| {
                let group = if i == 0 { WS_GROUP.0 } else { 0 };
                let style = WINDOW_STYLE((BS_AUTORADIOBUTTON as u32) | WS_TABSTOP.0 | group);
                let id = if i == 0 { ids::TARGET } else { "" };
                ui::child(
                    window,
                    w!("BUTTON"),
                    t.label(),
                    style,
                    WINDOW_EX_STYLE(0),
                    id,
                )
            })
            .collect();
        let warning = ui::label(window, "");
        let result = ui::label(window, "");
        let save = ui::button(window, "Resize and Save As...", ids::SAVE_AS);
        let close = ui::button(window, "Close", ids::CLOSE);
        let me = Rc::new_cyclic(|me: &Weak<AltoWindow>| AltoWindow {
            ctrl: RefCell::new(AltoResizeController::new(notify)),
            host,
            sync: SyncGuard::default(),
            me: me.clone(),
            window,
            source,
            summary,
            caption,
            targets,
            warning,
            result,
            save,
            close,
        });
        let weak: Weak<dyn Handler> = me.me.clone();
        panel::register(window, weak);
        me.layout();
        me.apply(AltoResizeAction::Open(path));
        // SAFETY: showing the window we created.
        unsafe {
            let _ = ShowWindow(window, SW_SHOW);
        }
        me
    }

    pub fn apply(&self, action: AltoResizeAction) {
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

    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            let Some(d) = me.upgrade() else { return };
            let action = match reply {
                Reply::Picked { purpose, path } => AltoResizeAction::Picked { purpose, path },
                // The Alto resize asks only for its output file.
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
        if !self.ctrl.borrow().is_open() {
            panel::unregister(self.window);
            // SAFETY: the controller closed; the window goes with it.
            let _ = unsafe { DestroyWindow(self.window) };
            return;
        }
        self.sync.run(|| self.refresh());
    }

    fn refresh(&self) {
        let c = self.ctrl.borrow();
        let source = c
            .source()
            .map(|p| format!("Source: {}", p.display()))
            .unwrap_or_default();
        ui::set_text(self.source, &source);
        let summary = match c.opening() {
            Some(job) => format!("{}...", job.phase),
            None => c.summary_line().unwrap_or("").to_string(),
        };
        ui::set_text(self.summary, &summary);
        let writing = c.is_writing();
        for (t, h) in AltoTarget::ALL.iter().zip(&self.targets) {
            set_check(*h, *t == c.target());
            ui::set_enabled(*h, !writing && c.opening().is_none());
        }
        ui::set_text(self.warning, c.fit_warning().as_deref().unwrap_or(""));
        let result = match c.result() {
            Some(Ok(msg)) => msg.clone(),
            Some(Err(e)) => format!("Error: {e}"),
            None if writing => "Resizing...".to_string(),
            None => String::new(),
        };
        ui::set_text(self.result, &result);
        ui::set_enabled(self.save, c.can_save());
        ui::set_enabled(self.close, !writing);
    }

    fn layout(&self) {
        let (area, dpi) = panel::client(self.window);
        let line =
            |h: HWND, n: i32| Node::leaf(h, Size::Stretch(1), Size::Fixed((layout::ROW - 8) * n));
        let mut rows = vec![
            line(self.source, 1),
            line(self.summary, 2),
            line(self.caption, 1),
        ];
        rows.extend(
            self.targets
                .iter()
                .map(|h| Node::leaf(*h, Size::Stretch(1), Size::Fixed(layout::ROW))),
        );
        rows.push(line(self.warning, 2));
        rows.push(line(self.result, 2));
        rows.push(Node::row(vec![
            Node::Space(Size::Stretch(1)),
            Node::button(self.save, 160),
            Node::button(self.close, 90),
        ]));
        let tree = Node::inset(Node::column(rows), layout::MARGIN);
        for (h, r) in layout::layout(&tree, area, dpi) {
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

impl Handler for AltoWindow {
    fn command(&self, from: HWND, code: u32) {
        if self.sync.active() || code != BN_CLICKED {
            return;
        }
        let action = if from == self.save {
            AltoResizeAction::SaveAs
        } else if from == self.close {
            AltoResizeAction::Close
        } else if let Some(i) = self.targets.iter().position(|h| *h == from) {
            AltoResizeAction::SetTarget(AltoTarget::ALL[i])
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
        self.apply(AltoResizeAction::Close);
    }
}
