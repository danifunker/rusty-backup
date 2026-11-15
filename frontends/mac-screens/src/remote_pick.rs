//! Inspect's remote image picker over `controller::remote_pick`; the Win32 shell's
//! `remote_pick.rs` is the reference. The daemon's listing stands in Inspect's place on a page of
//! its own until it hands an image over.

use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use rusty_backup::controller::app::Notify;
use rusty_backup::controller::commander::Tone;
use rusty_backup::controller::remote_pick::{
    ids, PickRow, RemoteImage, RemotePickAction, RemotePickController,
};
use rusty_backup::controller::{Controller, Effect};

use super::effects::{EffectHost, Reply, ReplyFn};
use super::form::Form;
use super::host::{Host, Surface};
use super::layout::{self, Node, Size};
use super::sync::SyncGuard;

const COLUMNS: [(&str, f64, bool); 3] = [
    ("Name", 420.0, false),
    ("Size", 110.0, true),
    ("Type", 90.0, false),
];

fn cells(r: &PickRow) -> Vec<String> {
    match r.parent {
        true => vec!["..".to_string(), String::new(), "dir".to_string()],
        false => vec![r.name.clone(), r.size.clone(), r.type_tag.clone()],
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum K {
    Title,
    Job,
    JobProgress,
    Error,
    Hint,
    List,
    /// Return or a double-click on a row.
    Open,
    Disconnect,
}

pub struct RemotePage {
    ctrl: RefCell<RemotePickController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<RemotePage>,
    /// Runs after every change, so Inspect drains the log and opens a picked image.
    on_change: RefCell<Option<Rc<dyn Fn()>>>,
    ui: Rc<dyn Host>,
    /// The page, which Inspect's page gives way to while the picker is active.
    page: Surface,
    form: Form<K>,
    rows: RefCell<Vec<PickRow>>,
}

impl RemotePage {
    /// The picker on `page`, a surface its parent places.
    pub fn build(
        ui: Rc<dyn Host>,
        page: Surface,
        notify: Arc<dyn Notify>,
        host: EffectHost,
    ) -> Rc<RemotePage> {
        Rc::new_cyclic(|me: &Weak<RemotePage>| {
            let fire = me.clone();
            let form = Form::with_host(
                ui.clone(),
                page,
                Rc::new(move |k| {
                    if let Some(p) = fire.upgrade() {
                        p.fired(k);
                    }
                }),
            );
            form.label(K::Title, "Remote images");
            form.label(K::Job, "");
            form.progress(K::JobProgress);
            form.label(K::Error, "");
            form.label(K::Hint, "");
            form.table(K::List, ids::LIST, &COLUMNS, Some(K::Open), None);
            form.button(K::Disconnect, "", ids::DISCONNECT);
            RemotePage {
                ctrl: RefCell::new(RemotePickController::new(notify)),
                host,
                sync: SyncGuard::default(),
                me: me.clone(),
                on_change: RefCell::default(),
                ui: ui.clone(),
                page,
                form,
                rows: RefCell::default(),
            }
        })
    }

    /// Inspect's hook: it drains the log and opens a picked image after every change.
    pub fn on_change(&self, f: Rc<dyn Fn()>) {
        *self.on_change.borrow_mut() = Some(f);
    }

    pub fn is_active(&self) -> bool {
        self.ctrl.borrow().is_active()
    }

    pub fn apply(&self, action: RemotePickAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    pub fn tick(&self) {
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    /// Status lines since the last call, and the image picked for Inspect to open.
    pub fn take(&self) -> (Vec<String>, Option<RemoteImage>) {
        let mut ctrl = self.ctrl.borrow_mut();
        (ctrl.take_log(), ctrl.take_request())
    }

    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            let Some(p) = me.upgrade() else {
                return;
            };
            let action = match reply {
                Reply::Answered { purpose, text } => RemotePickAction::Answered { purpose, text },
                // The picker asks only for the daemon's address.
                Reply::Picked { .. }
                | Reply::Confirmed { .. }
                | Reply::Chose { .. }
                | Reply::CloseScreen => return,
            };
            p.apply(action);
        })
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        self.refresh();
        let f = self.on_change.borrow().clone();
        if let Some(f) = f {
            f();
        }
    }

    fn refresh(&self) {
        let f = &self.form;
        {
            let ctrl = self.ctrl.borrow();
            let loaded = ctrl.working().is_none() && ctrl.error().is_none() && ctrl.is_loaded();
            let job = ctrl.working();
            let rows = if loaded { ctrl.rows() } else { Vec::new() };
            let hint = loaded.then(|| ctrl.hint());
            self.sync.run(|| {
                let (line, pct) = job.as_ref().map(super::jobs::job_line).unwrap_or_default();
                f.set_text(K::Job, &line);
                f.show(K::Job, job.is_some());
                f.set_progress(K::JobProgress, job.is_some(), pct);
                f.line(K::Error, ctrl.error(), Some(Tone::Error));
                f.line(K::Hint, hint, None);
                f.show(K::List, loaded);
                f.show(K::Disconnect, ctrl.can_disconnect());
                f.enable(K::Disconnect, !ctrl.transition_running());
                f.set_title(K::Disconnect, &ctrl.disconnect_label());
                if *self.rows.borrow() != rows {
                    let cells: Vec<Vec<String>> = rows.iter().map(cells).collect();
                    f.set_rows(K::List, &cells);
                    *self.rows.borrow_mut() = rows;
                }
            });
        }
        self.layout();
    }

    /// Return or a double-click on a row: up, into a folder, or open the image.
    fn activated(&self) {
        let row = self
            .form
            .selected_row(K::List)
            .and_then(|i| self.rows.borrow().get(i).cloned());
        let action = match row {
            Some(r) if r.parent => RemotePickAction::Up,
            Some(r) if r.is_dir => RemotePickAction::Enter(r.name),
            Some(r) if r.openable => RemotePickAction::OpenImage(r.path),
            _ => return,
        };
        self.apply(action);
    }

    pub fn layout(&self) {
        let row_h = Size::Fixed(layout::ROW);
        let line = |k: K| Node::leaf(k, Size::Stretch(1), Size::Fixed(layout::ROW - 4));
        let tree = Node::inset(
            Node::column(vec![
                Node::row(vec![
                    Node::leaf(K::Title, Size::Stretch(1), row_h),
                    Node::button(K::Disconnect, 270),
                ]),
                Node::row(vec![
                    Node::leaf(K::Job, Size::Stretch(1), row_h),
                    Node::leaf(
                        K::JobProgress,
                        Size::Fixed(180),
                        Size::Fixed(layout::ROW - 4),
                    ),
                ]),
                line(K::Error),
                line(K::Hint),
                Node::leaf(K::List, Size::Stretch(1), Size::Stretch(1)),
            ]),
            layout::MARGIN,
        );
        self.form.place_shown(tree, self.ui.area(self.page));
    }

    fn fired(&self, k: K) {
        if self.sync.active() {
            return;
        }
        let me = self.me.clone();
        if k == K::Open {
            // Act once the list's own handler returns: the page may give way to Inspect.
            self.ui.later(Box::new(move || {
                if let Some(p) = me.upgrade() {
                    p.activated();
                }
            }));
            return;
        }
        if k != K::Disconnect {
            return;
        }
        self.ui.later(Box::new(move || {
            if let Some(p) = me.upgrade() {
                p.apply(RemotePickAction::Disconnect);
            }
        }));
    }
}
