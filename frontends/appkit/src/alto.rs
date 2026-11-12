//! Resize Alto Disk over `controller::alto_resize`; the Win32 shell's `alto.rs` is the reference.

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use objc2::rc::Retained;
use objc2::MainThreadMarker;
use objc2_app_kit::NSWindow;
use rusty_backup::controller::alto_resize::{
    ids, AltoResizeAction, AltoResizeController, AltoTarget,
};
use rusty_backup::controller::app::Notify;
use rusty_backup::controller::{Controller, Effect};

use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::Form;
use crate::layout::{self, Node, Size};
use crate::sync::SyncGuard;
use crate::ui::{self, CloseDelegate};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum K {
    Source,
    Summary,
    Caption,
    Target(usize),
    Warning,
    Result,
    Save,
    Close,
}

pub struct AltoWindow {
    ctrl: RefCell<AltoResizeController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<AltoWindow>,
    window: Retained<NSWindow>,
    _delegate: Retained<CloseDelegate>,
    form: Form<K>,
}

impl AltoWindow {
    pub fn open(
        mtm: MainThreadMarker,
        notify: Arc<dyn Notify>,
        host: EffectHost,
        path: PathBuf,
    ) -> Rc<AltoWindow> {
        let me = Rc::new_cyclic(|me: &Weak<AltoWindow>| {
            let close = me.clone();
            let (window, delegate) = ui::window(
                mtm,
                "Resize Alto Disk",
                520.0,
                340.0,
                Box::new(move || {
                    if let Some(d) = close.upgrade() {
                        d.send(AltoResizeAction::Close);
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
            form.label(K::Source, "");
            form.wrap(K::Summary, "");
            form.label(K::Caption, "Target geometry:");
            for (i, t) in AltoTarget::ALL.iter().enumerate() {
                let id = if i == 0 { ids::TARGET } else { "" };
                form.radio(K::Target(i), t.label(), id);
            }
            form.wrap(K::Warning, "");
            form.wrap(K::Result, "");
            form.button(K::Save, "Resize and Save As...", ids::SAVE_AS);
            form.button(K::Close, "Close", ids::CLOSE);
            window.makeKeyAndOrderFront(None);
            AltoWindow {
                ctrl: RefCell::new(AltoResizeController::new(notify)),
                host,
                sync: SyncGuard::default(),
                me: me.clone(),
                window,
                _delegate: delegate,
                form,
            }
        });
        me.layout();
        me.apply(AltoResizeAction::Open(path));
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

    /// Send `action` once the current handler returns: the window may close with it.
    fn send(&self, action: AltoResizeAction) {
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
            self.window.orderOut(None);
            return;
        }
        self.sync.run(|| self.refresh());
    }

    fn refresh(&self) {
        let f = &self.form;
        let c = self.ctrl.borrow();
        let source = c
            .source()
            .map(|p| format!("Source: {}", p.display()))
            .unwrap_or_default();
        f.set_text(K::Source, &source);
        let summary = match c.opening() {
            Some(job) => format!("{}...", job.phase),
            None => c.summary_line().unwrap_or("").to_string(),
        };
        f.set_text(K::Summary, &summary);
        let writing = c.is_writing();
        for (i, t) in AltoTarget::ALL.iter().enumerate() {
            f.set_check(K::Target(i), *t == c.target());
            f.enable(K::Target(i), !writing && c.opening().is_none());
        }
        f.set_text(K::Warning, c.fit_warning().as_deref().unwrap_or(""));
        let result = match c.result() {
            Some(Ok(msg)) => msg.clone(),
            Some(Err(e)) => format!("Error: {e}"),
            None if writing => "Resizing...".to_string(),
            None => String::new(),
        };
        f.set_text(K::Result, &result);
        f.enable(K::Save, c.can_save());
        f.enable(K::Close, !writing);
    }

    fn layout(&self) {
        let Some(body) = self.window.contentView() else {
            return;
        };
        let line =
            |k: K, n: i32| Node::leaf(k, Size::Stretch(1), Size::Fixed((layout::ROW - 6) * n + 4));
        let mut rows = vec![line(K::Source, 1), line(K::Summary, 2), line(K::Caption, 1)];
        rows.extend(
            (0..AltoTarget::ALL.len())
                .map(|i| Node::leaf(K::Target(i), Size::Stretch(1), Size::Fixed(layout::ROW))),
        );
        rows.push(line(K::Warning, 2));
        rows.push(line(K::Result, 2));
        rows.push(Node::row(vec![
            Node::Space(Size::Stretch(1)),
            Node::button(K::Save, 170),
            Node::button(K::Close, 90),
        ]));
        let tree = Node::inset(Node::column(rows), layout::MARGIN);
        self.form.place(&tree, ui::area(&body));
    }

    fn fired(&self, k: K) {
        if self.sync.active() {
            return;
        }
        let action = match k {
            K::Save => AltoResizeAction::SaveAs,
            K::Close => AltoResizeAction::Close,
            K::Target(i) => AltoResizeAction::SetTarget(AltoTarget::ALL[i]),
            _ => return,
        };
        self.send(action);
    }
}

impl Drop for AltoWindow {
    fn drop(&mut self) {
        self.window.setDelegate(None);
        self.window.orderOut(None);
    }
}
