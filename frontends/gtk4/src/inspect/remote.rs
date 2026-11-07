//! Inspect's remote picker over `controller::remote_pick`; `src/gui/remote_browser.rs` is the
//! reference. The listing takes Inspect's main area until it hands an image over.

use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use adw::prelude::*;
use gtk::{gio, glib};
use rusty_backup::controller::app::{Notify, Snapshot};
use rusty_backup::controller::remote_pick::{
    hints, ids, PickRow, RemoteImage, RemotePickAction, RemotePickController,
};
use rusty_backup::controller::{Controller, Effect};

use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::button;
use crate::progress::JobList;
use crate::sync::SyncGuard;
use crate::widgets::set_text;

/// What the remote image row of the source bar shows, from the Inspect controller.
pub struct RemoteBar {
    /// The image open over a daemon, for Pick Another Image.
    pub image: Option<RemoteImage>,
    pub backing_up: bool,
    pub can_backup: bool,
}

struct Shown {
    active: bool,
    can_disconnect: bool,
    transition: bool,
    disconnect: String,
    working: Option<Snapshot>,
    error: Option<String>,
    hint: Option<&'static str>,
    rows: Vec<PickRow>,
}

pub struct RemotePanel {
    ctrl: RefCell<RemotePickController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<RemotePanel>,
    /// Runs after every change, so Inspect drains the log and opens a picked image.
    on_change: Rc<dyn Fn()>,
    root: gtk::Box,
    jobs: JobList,
    error: gtk::Label,
    hint: gtk::Label,
    list: gtk::ScrolledWindow,
    store: gio::ListStore,
    rows: RefCell<Vec<PickRow>>,
    bar: gtk::Box,
    disconnect: gtk::Button,
    pick: gtk::Button,
    backup: gtk::Button,
    backing: gtk::Spinner,
    backing_label: gtk::Label,
    close: gtk::Button,
    image: RefCell<Option<RemoteImage>>,
}

fn list_item(obj: &glib::Object) -> &gtk::ListItem {
    obj.downcast_ref().expect("a column factory's item")
}

fn row_of(item: &gtk::ListItem) -> Option<glib::BoxedAnyObject> {
    item.item().and_downcast::<glib::BoxedAnyObject>()
}

fn column(title: &str, expand: bool, cell: fn(&PickRow) -> String) -> gtk::ColumnViewColumn {
    let factory = gtk::SignalListItemFactory::new();
    factory.connect_setup(|_, obj| {
        let label = gtk::Label::builder()
            .xalign(0.0)
            .ellipsize(gtk::pango::EllipsizeMode::Middle)
            .build();
        list_item(obj).set_child(Some(&label));
    });
    factory.connect_bind(move |_, obj| {
        let item = list_item(obj);
        let (Some(row), Some(label)) = (row_of(item), item.child().and_downcast::<gtk::Label>())
        else {
            return;
        };
        let row = row.borrow::<PickRow>();
        // Names come off the daemon: `set_text` keeps a NUL out of GTK.
        set_text(&label, &cell(&row));
        label.remove_css_class("accent");
        label.remove_css_class("heading");
        if expand && row.openable {
            label.add_css_class("accent");
        } else if expand && row.is_dir {
            label.add_css_class("heading");
        }
        let tip = (expand && row.openable).then_some(hints::OPENABLE);
        label.set_tooltip_text(tip);
    });
    let c = gtk::ColumnViewColumn::new(Some(title), Some(factory));
    c.set_resizable(true);
    c.set_expand(expand);
    c
}

impl RemotePanel {
    /// `backup` and `close` are Inspect's own actions for the image open over a daemon.
    pub fn build(
        notify: Arc<dyn Notify>,
        host: EffectHost,
        on_change: Rc<dyn Fn()>,
        backup: Rc<dyn Fn()>,
        close: Rc<dyn Fn()>,
    ) -> Rc<RemotePanel> {
        Rc::new_cyclic(|me: &Weak<RemotePanel>| {
            let jobs = JobList::new(ids::LIST, Rc::new(|| {}));
            let error = gtk::Label::builder().xalign(0.0).wrap(true).build();
            error.add_css_class("error");
            let hint = gtk::Label::builder().xalign(0.0).wrap(true).build();
            hint.add_css_class("dim-label");
            let store = gio::ListStore::new::<glib::BoxedAnyObject>();
            let selection = gtk::SingleSelection::new(Some(store.clone()));
            let view = gtk::ColumnView::new(Some(selection));
            view.set_widget_name(ids::LIST);
            view.append_column(&column("Name", true, |r| match r.parent {
                true => "..".to_string(),
                false => r.name.clone(),
            }));
            view.append_column(&column("Size", false, |r| r.size.clone()));
            view.append_column(&column("Type", false, |r| match r.parent {
                true => "dir".to_string(),
                false => r.type_tag.clone(),
            }));
            let target = me.clone();
            view.connect_activate(move |_, position| {
                if let Some(p) = target.upgrade() {
                    p.activated(position);
                }
            });
            let list = gtk::ScrolledWindow::builder()
                .child(&view)
                .vexpand(true)
                .min_content_height(320)
                .build();
            let root = gtk::Box::new(gtk::Orientation::Vertical, 6);
            root.append(jobs.root());
            root.append(&error);
            root.append(&hint);
            root.append(&list);

            let disconnect = button("", ids::DISCONNECT);
            disconnect.set_tooltip_text(Some(hints::DISCONNECT));
            let pick = button("Pick Another Image", ids::PICK_ANOTHER);
            pick.set_tooltip_text(Some(hints::PICK_ANOTHER));
            let backup_button = button("Back Up Image...", ids::BACKUP);
            backup_button.set_tooltip_text(Some(hints::BACKUP));
            let backing = gtk::Spinner::new();
            let backing_label = gtk::Label::new(Some(hints::BACKING_UP));
            let close_button = button("Close Remote", ids::CLOSE);
            close_button.set_tooltip_text(Some(hints::CLOSE));
            let bar = gtk::Box::new(gtk::Orientation::Horizontal, 6);
            for w in [
                disconnect.upcast_ref::<gtk::Widget>(),
                pick.upcast_ref(),
                backup_button.upcast_ref(),
                backing.upcast_ref(),
                backing_label.upcast_ref(),
                close_button.upcast_ref(),
            ] {
                bar.append(w);
            }

            let target = me.clone();
            disconnect.connect_clicked(move |_| {
                if let Some(p) = target.upgrade() {
                    p.apply(RemotePickAction::Disconnect);
                }
            });
            let target = me.clone();
            pick.connect_clicked(move |_| {
                if let Some(p) = target.upgrade() {
                    let image = p.image.borrow().clone();
                    if let Some(image) = image {
                        p.apply(RemotePickAction::BrowseOn(image));
                    }
                }
            });
            backup_button.connect_clicked(move |_| backup());
            close_button.connect_clicked(move |_| close());

            RemotePanel {
                ctrl: RefCell::new(RemotePickController::new(notify)),
                host,
                sync: SyncGuard::default(),
                me: me.clone(),
                on_change,
                root,
                jobs,
                error,
                hint,
                list,
                store,
                rows: RefCell::default(),
                bar,
                disconnect,
                pick,
                backup: backup_button,
                backing,
                backing_label,
                close: close_button,
                image: RefCell::default(),
            }
        })
    }

    /// The listing, for Inspect's main area.
    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    /// The buttons beside the source picker.
    pub fn bar(&self) -> &gtk::Box {
        &self.bar
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
        (self.on_change)();
    }

    fn activated(&self, position: u32) {
        let row = self.rows.borrow().get(position as usize).cloned();
        let action = match row {
            Some(r) if r.parent => RemotePickAction::Up,
            Some(r) if r.is_dir => RemotePickAction::Enter(r.name),
            Some(r) if r.openable => RemotePickAction::OpenImage(r.path),
            _ => return,
        };
        self.apply(action);
    }

    fn read(&self) -> Shown {
        let ctrl = self.ctrl.borrow();
        let loaded = ctrl.working().is_none() && ctrl.error().is_none() && ctrl.is_loaded();
        Shown {
            active: ctrl.is_active(),
            can_disconnect: ctrl.can_disconnect(),
            transition: ctrl.transition_running(),
            disconnect: ctrl.disconnect_label(),
            working: ctrl.working(),
            error: ctrl.error().map(str::to_string),
            hint: loaded.then(|| ctrl.hint()),
            rows: if loaded { ctrl.rows() } else { Vec::new() },
        }
    }

    /// Show the listing and the source-bar buttons; run inside Inspect's `SyncGuard`.
    pub fn show(&self, bar: RemoteBar) {
        let s = self.read();
        self.sync.run(|| {
            self.root.set_visible(s.active);
            let jobs: Vec<(Snapshot, bool)> =
                s.working.iter().map(|j| (j.clone(), false)).collect();
            self.jobs.show(&jobs);
            crate::form::set_line(&self.error, s.error.as_deref());
            crate::form::set_line(&self.hint, s.hint);
            self.list.set_visible(s.hint.is_some());
            if *self.rows.borrow() != s.rows {
                let boxed: Vec<glib::BoxedAnyObject> = s
                    .rows
                    .iter()
                    .cloned()
                    .map(glib::BoxedAnyObject::new)
                    .collect();
                *self.rows.borrow_mut() = s.rows;
                self.store.splice(0, self.store.n_items(), &boxed);
            }

            self.disconnect.set_visible(s.can_disconnect);
            self.disconnect.set_sensitive(!s.transition);
            if self.disconnect.label().as_deref() != Some(s.disconnect.as_str()) {
                self.disconnect.set_label(&s.disconnect);
            }
            let open = bar.image.is_some();
            self.pick.set_visible(open);
            self.backup.set_visible(open);
            self.backup.set_sensitive(bar.can_backup && !bar.backing_up);
            self.backing.set_visible(open && bar.backing_up);
            self.backing.set_spinning(open && bar.backing_up);
            self.backing_label.set_visible(open && bar.backing_up);
            self.close.set_visible(open);
            self.close.set_sensitive(!bar.backing_up);
            self.bar.set_visible(s.can_disconnect || open);
            *self.image.borrow_mut() = bar.image;
        });
    }
}
