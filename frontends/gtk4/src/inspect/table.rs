//! The partition list: a `ColumnView` over boxed [`PartitionRow`]s, updated row by row.

use std::cell::RefCell;
use std::rc::Rc;

use adw::prelude::*;
use gtk::{gio, glib};
use rusty_backup::controller::inspect::{
    format_size, hints, ids, InspectAction, MinCell, PartitionRow,
};

use super::ActionFn;
use crate::widgets::{coming, set_text};

type Cell = fn(&PartitionRow) -> String;

const COMING_BROWSE: &str = "the Browse view";
const COMING_CHECK: &str = "the Check results window";
const COMING_EXPAND_HFS: &str = "the Expand HFS dialog";
const COMING_RESIZE_ALTO: &str = "the Alto resize dialog";

/// The row bound to a list item.
fn row_of(item: &gtk::ListItem) -> Option<glib::BoxedAnyObject> {
    item.item().and_downcast::<glib::BoxedAnyObject>()
}

fn list_item(obj: &glib::Object) -> &gtk::ListItem {
    obj.downcast_ref().expect("a column factory's item")
}

fn dim(widget: &impl IsA<gtk::Widget>, on: bool) {
    if on {
        widget.add_css_class("dim-label");
    } else {
        widget.remove_css_class("dim-label");
    }
}

fn text_column(
    title: &str,
    xalign: f32,
    hint: Option<&'static str>,
    cell: Cell,
) -> gtk::ColumnViewColumn {
    let factory = gtk::SignalListItemFactory::new();
    factory.connect_setup(move |_, obj| {
        let label = gtk::Label::builder().xalign(xalign).build();
        label.set_tooltip_text(hint);
        list_item(obj).set_child(Some(&label));
    });
    factory.connect_bind(move |_, obj| {
        let item = list_item(obj);
        let (Some(row), Some(label)) = (row_of(item), item.child().and_downcast::<gtk::Label>())
        else {
            return;
        };
        let row = row.borrow::<PartitionRow>();
        set_text(&label, &cell(&row));
        dim(&label, row.container);
    });
    let column = gtk::ColumnViewColumn::new(Some(title), Some(factory));
    column.set_resizable(true);
    column
}

/// Run `action` on the partition of the row `item` shows now; rows are recycled.
fn row_button(
    item: &gtk::ListItem,
    label: &str,
    id: &str,
    send: &ActionFn,
    action: fn(usize) -> InspectAction,
) -> gtk::Button {
    let button = gtk::Button::with_label(label);
    button.add_css_class("flat");
    button.set_widget_name(id);
    let item = item.downgrade();
    let send = send.clone();
    button.connect_clicked(move |_| {
        let row = item.upgrade().and_then(|i| row_of(&i));
        if let Some(row) = row {
            let index = row.borrow::<PartitionRow>().index;
            send(action(index));
        }
    });
    button
}

fn placeholder(label: &str, id: &str, what: &str) -> gtk::Button {
    let button = gtk::Button::with_label(label);
    button.add_css_class("flat");
    button.set_widget_name(id);
    coming(&button, what);
    button
}

fn min_column(send: &ActionFn) -> gtk::ColumnViewColumn {
    let factory = gtk::SignalListItemFactory::new();
    let send = send.clone();
    factory.connect_setup(move |_, obj| {
        let item = list_item(obj);
        let label = gtk::Label::builder().xalign(1.0).hexpand(true).build();
        label.set_tooltip_text(Some(hints::MIN_SIZE));
        let spinner = gtk::Spinner::new();
        let calc = row_button(
            item,
            "Calc min",
            ids::ROW_CALC_MIN,
            &send,
            InspectAction::CalcMin,
        );
        calc.set_tooltip_text(Some(hints::CALC_MIN));
        let cell = gtk::Box::new(gtk::Orientation::Horizontal, 0);
        cell.append(&label);
        cell.append(&spinner);
        cell.append(&calc);
        item.set_child(Some(&cell));
    });
    factory.connect_bind(move |_, obj| {
        let item = list_item(obj);
        let (Some(row), Some(cell)) = (row_of(item), item.child()) else {
            return;
        };
        let row = row.borrow::<PartitionRow>();
        let label = cell.first_child().and_downcast::<gtk::Label>();
        let spinner = label
            .as_ref()
            .and_then(|l| l.next_sibling())
            .and_downcast::<gtk::Spinner>();
        let calc = spinner.as_ref().and_then(|s| s.next_sibling());
        let (Some(label), Some(spinner), Some(calc)) = (label, spinner, calc) else {
            return;
        };
        let text = match &row.min {
            MinCell::Size(size) => format_size(*size),
            MinCell::Pending(_) | MinCell::CanCalc | MinCell::Empty => String::new(),
        };
        set_text(&label, &text);
        dim(&label, row.container);
        let pending = match &row.min {
            MinCell::Pending(phase) => Some(phase.as_str()),
            _ => None,
        };
        spinner.set_visible(pending.is_some());
        spinner.set_spinning(pending.is_some());
        spinner.set_tooltip_text(pending);
        calc.set_visible(matches!(row.min, MinCell::CanCalc));
    });
    let column = gtk::ColumnViewColumn::new(Some("Min Size"), Some(factory));
    column.set_resizable(true);
    column
}

fn actions_column(send: &ActionFn) -> gtk::ColumnViewColumn {
    let factory = gtk::SignalListItemFactory::new();
    let send = send.clone();
    factory.connect_setup(move |_, obj| {
        let item = list_item(obj);
        let cell = gtk::Box::new(gtk::Orientation::Horizontal, 2);
        cell.append(&placeholder("Browse", ids::ROW_BROWSE, COMING_BROWSE));
        cell.append(&placeholder("Check", ids::ROW_CHECK, COMING_CHECK));
        cell.append(&placeholder(
            "Expand/Export...",
            ids::ROW_EXPAND_HFS,
            COMING_EXPAND_HFS,
        ));
        let defragment = row_button(
            item,
            "Defragment...",
            ids::ROW_DEFRAGMENT,
            &send,
            InspectAction::Defragment,
        );
        defragment.set_tooltip_text(Some(hints::DEFRAGMENT));
        cell.append(&defragment);
        cell.append(&placeholder(
            "Resize...",
            ids::ROW_RESIZE_ALTO,
            COMING_RESIZE_ALTO,
        ));
        item.set_child(Some(&cell));
    });
    factory.connect_bind(move |_, obj| {
        let item = list_item(obj);
        let (Some(row), Some(cell)) = (row_of(item), item.child()) else {
            return;
        };
        let row = row.borrow::<PartitionRow>();
        let acts = row.actions;
        let shown = [
            acts.browse,
            acts.browse && acts.check,
            acts.browse && acts.expand_hfs,
            acts.browse && acts.defragment,
            acts.browse && acts.resize_alto,
        ];
        let mut child = cell.first_child();
        for visible in shown {
            let Some(button) = child else { break };
            button.set_visible(visible);
            child = button.next_sibling();
        }
        // Only Defragment is live in this slice; the rows lock while it runs.
        let defragment = cell.first_child().and_then(|c| {
            let c = c.next_sibling()?.next_sibling()?;
            c.next_sibling()
        });
        if let Some(defragment) = defragment {
            defragment.set_sensitive(!row.locked);
        }
    });
    gtk::ColumnViewColumn::new(None, Some(factory))
}

pub struct PartitionTable {
    root: gtk::ScrolledWindow,
    store: gio::ListStore,
    selection: gtk::SingleSelection,
    slot: gtk::ColumnViewColumn,
    min: gtk::ColumnViewColumn,
    defrag: gtk::ColumnViewColumn,
    rows: RefCell<Vec<PartitionRow>>,
}

impl PartitionTable {
    pub fn build(send: &ActionFn) -> Rc<PartitionTable> {
        let store = gio::ListStore::new::<glib::BoxedAnyObject>();
        let selection = gtk::SingleSelection::builder()
            .model(&store)
            .autoselect(false)
            .can_unselect(true)
            .build();
        let view = gtk::ColumnView::builder()
            .model(&selection)
            .show_column_separators(true)
            .show_row_separators(true)
            .build();
        view.set_widget_name(ids::PARTITION_TABLE);

        let number = text_column("#", 0.0, Some(hints::NUMBER), |r| r.number.clone());
        let slot = text_column("Slot", 0.0, Some(hints::SLOT), |r| r.slot.clone());
        let kind = text_column("Type", 0.0, None, |r| r.type_name.clone());
        kind.set_expand(true);
        let volume = text_column("Volume", 0.0, None, |r| r.volume.clone());
        volume.set_expand(true);
        let start = text_column("Start LBA", 1.0, None, |r| r.start_lba.to_string());
        let size = text_column("Size", 1.0, None, |r| format_size(r.size_bytes));
        let min = min_column(send);
        let defrag = text_column("Defrag Min", 1.0, Some(hints::DEFRAG_MIN), |r| {
            r.defrag_min.map(format_size).unwrap_or_default()
        });
        let block = text_column("Block Size", 1.0, None, |r| r.block_size.clone());
        let boot = text_column("Boot", 0.0, None, |r| {
            if r.bootable { "Yes" } else { "" }.to_string()
        });
        for column in [
            &number, &slot, &kind, &volume, &start, &size, &min, &defrag, &block, &boot,
        ] {
            view.append_column(column);
        }
        view.append_column(&actions_column(send));

        let root = gtk::ScrolledWindow::builder()
            .child(&view)
            .vexpand(true)
            .min_content_height(170)
            .build();
        root.add_css_class("card");

        let table = Rc::new(PartitionTable {
            root,
            store,
            selection,
            slot,
            min,
            defrag,
            rows: RefCell::new(Vec::new()),
        });
        let me = table.clone();
        let send = send.clone();
        table.selection.connect_selected_notify(move |s| {
            let picked = me.rows.borrow().get(s.selected() as usize).map(|r| r.index);
            send(InspectAction::SelectPartition(picked));
        });
        table
    }

    pub fn root(&self) -> &gtk::ScrolledWindow {
        &self.root
    }

    /// Replace only the rows that changed, so selection and scroll survive a refresh.
    pub fn show(&self, rows: Vec<PartitionRow>, selected: Option<usize>, min: bool, slots: bool) {
        self.slot.set_visible(slots);
        self.min.set_visible(min);
        self.defrag.set_visible(min);
        let position = selected
            .and_then(|i| rows.iter().position(|r| r.index == i))
            .map_or(gtk::INVALID_LIST_POSITION, |p| p as u32);
        // Swap the list before touching the store: a splice can fire the selection handler.
        let old = self.rows.replace(rows.clone());
        if old != rows {
            let boxed = |r: &PartitionRow| glib::BoxedAnyObject::new(r.clone());
            if old.len() == rows.len() {
                for (i, (before, now)) in old.iter().zip(&rows).enumerate() {
                    if before != now {
                        self.store.splice(i as u32, 1, &[boxed(now)]);
                    }
                }
            } else {
                let all: Vec<_> = rows.iter().map(boxed).collect();
                self.store.splice(0, old.len() as u32, &all);
            }
        }
        if self.selection.selected() != position {
            self.selection.set_selected(position);
        }
    }
}
