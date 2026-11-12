//! Backup's Remote Drive window over the Backup controller; the Win32 shell's `backup_remote.rs`
//! is the reference. Open while `BackupController::remote_dialog_open()` says so: the daemon's
//! address, Connect, and the physical drives it lists, one of which becomes the backup's source.

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use objc2::rc::Retained;
use objc2::MainThreadMarker;
use objc2_app_kit::NSWindow;
use rusty_backup::controller::backup::{hints, ids, BackupAction, RemoteDrive};
use rusty_backup::controller::commander::Tone;

use crate::form::Form;
use crate::layout::{self, Node, Size};
use crate::sync::SyncGuard;
use crate::table::Table;
use crate::ui::{self, CloseDelegate};

/// What the window shows, read while the controller is borrowed.
pub struct RemoteShown {
    pub address: String,
    pub busy: bool,
    pub error: Option<String>,
    /// The daemon's drives once it has listed them.
    pub drives: Option<Vec<RemoteDrive>>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum K {
    Caption,
    Address,
    Connect,
    Busy,
    Error,
    Note,
    List,
    Close,
}

pub struct RemoteWindow {
    window: Retained<NSWindow>,
    _delegate: Retained<CloseDelegate>,
    form: Form<K>,
    list: Table,
    send: Rc<dyn Fn(BackupAction)>,
    sync: SyncGuard,
    drives: RefCell<Vec<RemoteDrive>>,
}

impl RemoteWindow {
    pub fn open(mtm: MainThreadMarker, send: Rc<dyn Fn(BackupAction)>) -> Rc<RemoteWindow> {
        Rc::new_cyclic(|me: &Weak<RemoteWindow>| {
            let close = send.clone();
            let (window, delegate) = ui::window(
                mtm,
                "Remote Drive",
                520.0,
                380.0,
                Box::new(move || {
                    let close = close.clone();
                    crate::app::later(Box::new(move || close(BackupAction::RemoteClose)));
                }),
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
            form.label(K::Caption, "Host:");
            form.field(K::Address, ids::REMOTE_ADDRESS, "Host", false);
            form.button(K::Connect, "Connect", ids::REMOTE_CONNECT);
            form.label(K::Busy, "Connecting...");
            form.wrap(K::Error, "");
            form.wrap(K::Note, "");
            let list = Table::new(mtm, ids::REMOTE_DRIVE, &[("Drive", 460.0, false)]);
            list.view.setHeaderView(None);
            let activate = me.clone();
            list.on_activate(
                mtm,
                Rc::new(move || {
                    if let Some(w) = activate.upgrade() {
                        w.picked();
                    }
                }),
            );
            form.add(K::List, &list.scroll);
            form.button(K::Close, "Close", ids::REMOTE_CLOSE);
            ui::present(&window);
            RemoteWindow {
                window,
                _delegate: delegate,
                form,
                list,
                send,
                sync: SyncGuard::default(),
                drives: RefCell::default(),
            }
        })
    }

    pub fn show(&self, s: RemoteShown) {
        let f = &self.form;
        self.sync.run(|| {
            f.set_text(K::Address, &s.address);
            f.enable(K::Connect, !s.busy);
            f.show(K::Busy, s.busy);
            f.line(K::Error, s.error.as_deref(), Some(Tone::Error));
            let note = s.drives.as_ref().map(|d| match d.is_empty() {
                true => hints::NO_REMOTE_DRIVES,
                false => "Pick a drive to back up (double-click or Enter):",
            });
            f.line(K::Note, note, None);
            let drives = s.drives.unwrap_or_default();
            f.show(K::List, !drives.is_empty());
            if *self.drives.borrow() != drives {
                self.list
                    .set_rows(drives.iter().map(|d| vec![d.label.clone()]).collect());
                *self.drives.borrow_mut() = drives;
            }
        });
        self.layout();
    }

    fn picked(&self) {
        let Some(d) = self
            .list
            .selected()
            .and_then(|i| self.drives.borrow().get(i).cloned())
        else {
            return;
        };
        self.send_later(BackupAction::RemotePick {
            path: d.path,
            size: d.size_bytes,
        });
    }

    fn layout(&self) {
        let Some(body) = self.window.contentView() else {
            return;
        };
        let row_h = Size::Fixed(layout::ROW);
        let line = |k: K| Node::leaf(k, Size::Stretch(1), Size::Fixed((layout::ROW - 6) * 2 + 4));
        let tree = Node::inset(
            Node::column(vec![
                Node::row(vec![
                    Node::leaf(K::Caption, Size::Fixed(50), row_h),
                    Node::field(K::Address),
                    Node::button(K::Connect, 95),
                ]),
                Node::leaf(K::Busy, Size::Stretch(1), row_h),
                line(K::Error),
                line(K::Note),
                Node::leaf(K::List, Size::Stretch(1), Size::Stretch(1)),
                Node::row(vec![
                    Node::Space(Size::Stretch(1)),
                    Node::button(K::Close, 90),
                ]),
            ]),
            layout::MARGIN,
        );
        self.form.place_shown(tree, ui::area(&body));
    }

    /// Send once the current handler returns: the window may close with it.
    fn send_later(&self, action: BackupAction) {
        let send = self.send.clone();
        crate::app::later(Box::new(move || send(action)));
    }

    fn fired(&self, k: K) {
        if self.sync.active() {
            return;
        }
        let action = match k {
            K::Address => BackupAction::RemoteSetAddress(self.form.text(k)),
            K::Connect => BackupAction::RemoteConnect,
            K::Close => BackupAction::RemoteClose,
            _ => return,
        };
        self.send_later(action);
    }
}

impl Drop for RemoteWindow {
    fn drop(&mut self) {
        self.window.setDelegate(None);
        self.window.orderOut(None);
    }
}
