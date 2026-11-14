//! Optical's Add remote daemon window over the Optical controller; the Win32 shell's
//! `optical_remote.rs` is the reference. Open while `OpticalController::add_remote_open()` says
//! so; a daemon that connects adds its optical drives to the drive list.

use std::rc::{Rc, Weak};

use rusty_backup::controller::commander::Tone;
use rusty_backup::controller::optical::{ids, OpticalAction};

use super::form::Form;
use super::host::{Host, Surface};
use super::layout::{self, Node, Size};
use super::sync::SyncGuard;

/// What the window shows.
pub struct AddRemoteShown {
    pub connecting: bool,
    pub error: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum K {
    Caption,
    Address,
    Connect,
    Cancel,
    Connecting,
    Error,
    RecentCaption,
    Recent(usize),
}

pub struct AddRemoteWindow {
    ui: Rc<dyn Host>,
    window: Surface,
    form: Form<K>,
    send: Rc<dyn Fn(OpticalAction)>,
    sync: SyncGuard,
    /// The daemons connected before, one button each.
    recent: Vec<String>,
}

impl AddRemoteWindow {
    pub fn open(
        ui: Rc<dyn Host>,
        recent: &[String],
        send: Rc<dyn Fn(OpticalAction)>,
    ) -> Rc<AddRemoteWindow> {
        Rc::new_cyclic(|me: &Weak<AddRemoteWindow>| {
            let (close, later) = (send.clone(), ui.clone());
            let window = ui.window(
                "Add remote daemon",
                420,
                320,
                Box::new(move || {
                    let close = close.clone();
                    later.later(Box::new(move || close(OpticalAction::CloseAddRemote)));
                }),
            );
            let fire = me.clone();
            let form = Form::with_host(
                ui.clone(),
                window,
                Rc::new(move |k| {
                    if let Some(w) = fire.upgrade() {
                        w.fired(k);
                    }
                }),
            );
            form.label(K::Caption, "Daemon address (host:port):");
            form.field(K::Address, ids::REMOTE_ADDRESS, "Daemon address", false);
            form.button(K::Connect, "Connect", ids::REMOTE_CONNECT);
            form.button(K::Cancel, "Cancel", ids::REMOTE_CANCEL);
            form.label(K::Connecting, "Connecting...");
            form.wrap(K::Error, "");
            form.label(K::RecentCaption, "Recent:");
            form.show(K::RecentCaption, !recent.is_empty());
            for (i, a) in recent.iter().enumerate() {
                form.button(K::Recent(i), a, "");
            }
            ui.present(window);
            AddRemoteWindow {
                ui: ui.clone(),
                window,
                form,
                send,
                sync: SyncGuard::default(),
                recent: recent.to_vec(),
            }
        })
    }

    pub fn show(&self, s: AddRemoteShown) {
        let f = &self.form;
        self.sync.run(|| {
            f.show(K::Connecting, s.connecting);
            f.enable(K::Connect, !s.connecting);
            f.line(K::Error, s.error.as_deref(), Some(Tone::Error));
        });
        self.layout();
    }

    fn layout(&self) {
        let row_h = Size::Fixed(layout::ROW);
        let line = |k: K| Node::leaf(k, Size::Stretch(1), Size::Fixed(layout::ROW - 4));
        let mut col = vec![
            line(K::Caption),
            Node::leaf(K::Address, Size::Stretch(1), row_h),
            Node::row(vec![
                Node::button(K::Connect, 95),
                Node::button(K::Cancel, 90),
                Node::Space(Size::Stretch(1)),
            ]),
            line(K::Connecting),
            line(K::Error),
            line(K::RecentCaption),
        ];
        for i in 0..self.recent.len() {
            col.push(Node::row(vec![
                Node::button(K::Recent(i), 250),
                Node::Space(Size::Stretch(1)),
            ]));
        }
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        self.form.place_shown(tree, self.ui.area(self.window));
    }

    fn fired(&self, k: K) {
        if self.sync.active() {
            return;
        }
        let f = &self.form;
        let action = match k {
            K::Connect => OpticalAction::ConnectRemote(f.text(K::Address)),
            K::Cancel => OpticalAction::CloseAddRemote,
            K::Recent(i) => match self.recent.get(i) {
                Some(addr) => {
                    f.set_text(K::Address, addr);
                    OpticalAction::ConnectRemote(addr.clone())
                }
                None => return,
            },
            _ => return,
        };
        let send = self.send.clone();
        self.ui.later(Box::new(move || send(action)));
    }
}

impl Drop for AddRemoteWindow {
    fn drop(&mut self) {
        self.ui.release(self.window);
    }
}
