//! The entry list: a `ColumnView` over the archive's rows, a tick box on every file.

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

use adw::prelude::*;
use gtk::{gio, glib};
use rusty_backup::controller::archives::{
    human_size, ids, ArchiveEntryRow, ArchivesAction, ArchivesController,
};

use super::ActionFn;
use crate::widgets::set_text;

/// One row: its index in the archive and what it lists.
#[derive(Clone)]
pub struct Entry {
    index: usize,
    row: ArchiveEntryRow,
}

type Cell = fn(&ArchiveEntryRow) -> String;

/// Runs a widget change with the screen's change handlers muted.
pub type Guarded = Rc<dyn Fn(&mut dyn FnMut())>;

fn list_item(obj: &glib::Object) -> &gtk::ListItem {
    obj.downcast_ref().expect("a column factory's item")
}

fn entry_of(item: &gtk::ListItem) -> Option<glib::BoxedAnyObject> {
    item.item().and_downcast::<glib::BoxedAnyObject>()
}

fn text_column(title: &str, xalign: f32, expand: bool, cell: Cell) -> gtk::ColumnViewColumn {
    let factory = gtk::SignalListItemFactory::new();
    factory.connect_setup(move |_, obj| {
        let label = gtk::Label::builder()
            .xalign(xalign)
            .ellipsize(gtk::pango::EllipsizeMode::Middle)
            .build();
        list_item(obj).set_child(Some(&label));
    });
    factory.connect_bind(move |_, obj| {
        let item = list_item(obj);
        let (Some(entry), Some(label)) =
            (entry_of(item), item.child().and_downcast::<gtk::Label>())
        else {
            return;
        };
        // Names come off the archive: `set_text` keeps a NUL out of GTK.
        set_text(&label, &cell(&entry.borrow::<Entry>().row));
    });
    let column = gtk::ColumnViewColumn::new(Some(title), Some(factory));
    column.set_resizable(true);
    column.set_expand(expand);
    column
}

/// A row's box: folders are always extracted, so they have none.
fn tick_column(
    send: &ActionFn,
    ticked: Rc<dyn Fn(usize) -> bool>,
    guarded: Guarded,
) -> gtk::ColumnViewColumn {
    let factory = gtk::SignalListItemFactory::new();
    let send = send.clone();
    factory.connect_setup(move |_, obj| {
        let item = list_item(obj);
        let tick = gtk::CheckButton::new();
        tick.set_halign(gtk::Align::Center);
        let weak = item.downgrade();
        let send = send.clone();
        tick.connect_toggled(move |b| {
            let entry = weak.upgrade().and_then(|i| entry_of(&i));
            if let Some(entry) = entry {
                let index = entry.borrow::<Entry>().index;
                send(ArchivesAction::SetSelected(index, b.is_active()));
            }
        });
        item.set_child(Some(&tick));
    });
    factory.connect_bind(move |_, obj| {
        let item = list_item(obj);
        let (Some(entry), Some(tick)) = (
            entry_of(item),
            item.child().and_downcast::<gtk::CheckButton>(),
        ) else {
            return;
        };
        let entry = entry.borrow::<Entry>();
        tick.set_visible(!entry.row.is_dir);
        // A recycled cell takes this row's state with the toggle muted.
        let on = ticked(entry.index);
        guarded(&mut || tick.set_active(on));
    });
    gtk::ColumnViewColumn::new(Some("Extract"), Some(factory))
}

pub struct EntryList {
    root: gtk::ScrolledWindow,
    store: gio::ListStore,
    /// The archive on screen, so its rows are copied once.
    shown: RefCell<Option<(PathBuf, usize)>>,
}

impl EntryList {
    pub fn build(
        send: &ActionFn,
        ticked: Rc<dyn Fn(usize) -> bool>,
        guarded: Guarded,
    ) -> Rc<EntryList> {
        let store = gio::ListStore::new::<glib::BoxedAnyObject>();
        let view = gtk::ColumnView::new(Some(gtk::NoSelection::new(Some(store.clone()))));
        view.set_widget_name(ids::ENTRIES);
        view.set_show_row_separators(true);
        view.append_column(&tick_column(send, ticked, guarded));
        view.append_column(&text_column("Name", 0.0, true, |r| match r.is_dir {
            true => format!("[ {} ]", r.path),
            false => r.path.clone(),
        }));
        view.append_column(&text_column("Type/Creator", 0.0, false, |r| {
            match r.is_dir {
                true => String::new(),
                false => format!("{} / {}", r.type_code, r.creator),
            }
        }));
        view.append_column(&text_column("Size", 1.0, false, |r| match r.is_dir {
            true => String::new(),
            false => human_size(r.size),
        }));
        view.append_column(&text_column("Method", 0.0, false, |r| match r.is_dir {
            true => String::new(),
            false => r.method.to_string(),
        }));
        let root = gtk::ScrolledWindow::builder()
            .child(&view)
            .vexpand(true)
            .build();
        Rc::new(EntryList {
            root,
            store,
            shown: RefCell::default(),
        })
    }

    pub fn root(&self) -> &gtk::ScrolledWindow {
        &self.root
    }

    /// The rows to show, when the archive on screen is not the controller's.
    pub fn changed(&self, ctrl: &ArchivesController) -> Option<Vec<Entry>> {
        let loaded = ctrl.is_loaded();
        let now = ctrl
            .path()
            .filter(|_| loaded)
            .map(|p| (p.to_path_buf(), ctrl.rows().len()));
        if *self.shown.borrow() == now {
            return None;
        }
        *self.shown.borrow_mut() = now;
        let rows = ctrl.rows().iter().enumerate();
        let rows = rows.map(|(index, row)| Entry {
            index,
            row: row.clone(),
        });
        Some(rows.collect())
    }

    /// Replace the rows; call this with the controller released, as the binds read it.
    pub fn show(&self, rows: Vec<Entry>) {
        let boxed: Vec<glib::BoxedAnyObject> =
            rows.into_iter().map(glib::BoxedAnyObject::new).collect();
        self.store.splice(0, self.store.n_items(), &boxed);
    }
}
