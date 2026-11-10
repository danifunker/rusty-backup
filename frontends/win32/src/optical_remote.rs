//! Optical's Add remote daemon window over the Optical controller; GTK's `optical/windows.rs` is
//! the reference. Open while `OpticalController::add_remote_open()` says so; a daemon that
//! connects adds its optical drives to the drive list.

use std::rc::{Rc, Weak};

use rusty_backup::controller::commander::Tone;
use rusty_backup::controller::optical::{ids, OpticalAction};
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT};
use windows::Win32::UI::Controls::NMHDR;
use windows::Win32::UI::WindowsAndMessaging::{DestroyWindow, ShowWindow, BN_CLICKED, SW_SHOW};

use crate::build_disk::set_line;
use crate::layout::{self, Node, Size};
use crate::panel::{self, Handler};
use crate::sync::SyncGuard;
use crate::ui;

/// What the window shows.
pub struct AddRemoteShown {
    pub connecting: bool,
    pub error: Option<String>,
}

pub struct AddRemoteWindow {
    window: HWND,
    send: Rc<dyn Fn(OpticalAction)>,
    sync: SyncGuard,
    me: Weak<AddRemoteWindow>,
    caption: HWND,
    address: HWND,
    connect: HWND,
    cancel: HWND,
    connecting: HWND,
    error: HWND,
    recent_caption: HWND,
    /// One button per daemon connected before, and its address.
    recent: Vec<(HWND, String)>,
}

impl AddRemoteWindow {
    pub fn open(owner: HWND, recent: &[String], send: Rc<dyn Fn(OpticalAction)>) -> Rc<Self> {
        let window = panel::dialog(owner, "Add remote daemon", 420, 320);
        let recent = recent
            .iter()
            .map(|a| (ui::button(window, &ui::plain(a), ""), a.clone()))
            .collect();
        let me = Rc::new_cyclic(|me: &Weak<AddRemoteWindow>| AddRemoteWindow {
            window,
            send,
            sync: SyncGuard::default(),
            me: me.clone(),
            caption: ui::label(window, "Daemon address (host:port):"),
            address: ui::edit(window, ids::REMOTE_ADDRESS),
            connect: ui::button(window, "Connect", ids::REMOTE_CONNECT),
            cancel: ui::button(window, "Cancel", ids::REMOTE_CANCEL),
            connecting: ui::cell_label(window, "Connecting..."),
            error: ui::label(window, ""),
            recent_caption: ui::label(window, "Recent:"),
            recent,
        });
        ui::set_visible(me.recent_caption, !me.recent.is_empty());
        let weak: Weak<dyn Handler> = me.me.clone();
        panel::register(window, weak);
        // SAFETY: showing the window we created.
        unsafe {
            let _ = ShowWindow(window, SW_SHOW);
        }
        me
    }

    pub fn show(&self, s: AddRemoteShown) {
        self.sync.run(|| {
            ui::set_visible(self.connecting, s.connecting);
            ui::set_enabled(self.connect, !s.connecting);
            set_line(self.error, s.error.as_deref(), Some(Tone::Error));
        });
        self.layout();
    }

    fn layout(&self) {
        let (area, dpi) = panel::client(self.window);
        let row_h = Size::Fixed(layout::ROW);
        let line = |h: HWND| Node::leaf(h, Size::Stretch(1), Size::Fixed(layout::ROW - 8));
        let mut col = vec![
            line(self.caption),
            Node::leaf(self.address, Size::Stretch(1), row_h),
            Node::row(vec![
                Node::button(self.connect, 90),
                Node::button(self.cancel, 90),
                Node::Space(Size::Stretch(1)),
            ]),
        ];
        for h in [self.connecting, self.error] {
            if ui::shown(h) {
                col.push(line(h));
            }
        }
        if !self.recent.is_empty() {
            col.push(line(self.recent_caption));
            for (b, _) in &self.recent {
                col.push(Node::row(vec![
                    Node::button(*b, 240),
                    Node::Space(Size::Stretch(1)),
                ]));
            }
        }
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        for (h, r) in layout::layout(&tree, area, dpi) {
            if ui::shown(h) {
                ui::move_to(h, r);
            }
        }
    }
}

impl Drop for AddRemoteWindow {
    fn drop(&mut self) {
        panel::unregister(self.window);
        // SAFETY: the controller closed the window's state; the window goes with it.
        let _ = unsafe { DestroyWindow(self.window) };
    }
}

impl Handler for AddRemoteWindow {
    fn command(&self, from: HWND, code: u32) {
        if self.sync.active() || code != BN_CLICKED {
            return;
        }
        let action = if from == self.connect {
            OpticalAction::ConnectRemote(ui::text(self.address))
        } else if from == self.cancel {
            OpticalAction::CloseAddRemote
        } else if let Some((_, addr)) = self.recent.iter().find(|(b, _)| *b == from) {
            ui::set_text(self.address, addr);
            OpticalAction::ConnectRemote(addr.clone())
        } else {
            return;
        };
        (self.send)(action);
    }

    fn notify(&self, _hdr: &NMHDR, _lp: LPARAM) -> Option<LRESULT> {
        None
    }

    fn resized(&self) {
        self.layout();
    }

    fn close(&self) {
        (self.send)(OpticalAction::CloseAddRemote);
    }
}
