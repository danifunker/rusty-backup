//! One browser pane: the listing in any of the three styles, on a side of a `CommanderController`.
//!
//! The pane knows nothing of who hosts it: it reads through [`PaneView::read`], draws in
//! [`PaneView::show`] and sends through [`PaneHost`], so Inspect's browse view and each Commander
//! pane are the same widget. Rows live in a [`FlatTree`]; cells read the bound row when drawn and
//! when clicked, and labels are asked for only for the rows on screen.

use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::rc::{Rc, Weak};

use adw::prelude::*;
use gtk::{glib, graphene};
use rusty_backup::controller::browser::ids;
use rusty_backup::controller::commander::{
    format_size, BrowserPaneAction as P, CommanderAction as A, CommanderController, DisplayRow,
    ListingStyle, RowKind, RowLabel, RowMenuEntry, RowTone, Side, SortColumn,
};
use rusty_backup::controller::keys::{Key, Mods};

use super::tree::{FlatTree, Node};
use crate::keys::{key_of, mods_of};
use crate::widgets::{plain, set_text};

/// What a pane needs from whoever hosts it.
pub trait PaneHost {
    /// Run an action; one a sync caused is dropped here.
    fn send(&self, action: A);
    fn label(&self, side: Side, row: &DisplayRow) -> RowLabel;
    fn menu(&self, side: Side, row: &DisplayRow) -> Vec<RowMenuEntry>;
    fn click(&self, side: Side, row: &DisplayRow, ctrl: bool, shift: bool) -> Option<P>;
    fn context(&self, side: Side, row: &DisplayRow) -> Option<P>;
    /// Run a key through the key map; true when it meant something.
    fn key(&self, side: Side, key: Key, mods: Mods) -> bool;
    /// A row is being dragged, the pointer at `at` on the window `from`; `end` says it was let go.
    fn dragged(&self, _side: Side, _from: &gtk::Widget, _at: (f64, f64), _end: bool) {}
}

/// The pane's state, read while the controller is borrowed.
pub struct PaneShown {
    /// `None` when they match what is on screen.
    rows: Option<Vec<DisplayRow>>,
    style: ListingStyle,
    loaded: bool,
    message: String,
    boxes: bool,
    active: bool,
    cursor: Option<(u32, String)>,
    selected: Vec<u32>,
    marks: HashSet<String>,
    folders: Option<Vec<Node<String>>>,
    folder_cursor: u32,
    folder_focus: bool,
    sort: (SortColumn, bool),
}

/// A cell on screen, so its row can be drawn again when the controller changes it.
struct Drawn {
    item: glib::WeakRef<gtk::ListItem>,
    draw: Rc<dyn Fn(&gtk::ListItem)>,
}

pub struct PaneView {
    side: Side,
    host: Rc<dyn PaneHost>,
    me: Weak<PaneView>,
    root: gtk::Box,
    stack: gtk::Stack,
    message: gtk::Label,
    view: gtk::ColumnView,
    scroll: gtk::ScrolledWindow,
    tree: FlatTree<DisplayRow>,
    selection: gtk::MultiSelection,
    mark: gtk::ColumnViewColumn,
    rsrc: gtk::ColumnViewColumn,
    sortable: [(SortColumn, gtk::ColumnViewColumn); 4],
    folders: FlatTree<String>,
    folder_view: gtk::ListView,
    folder_scroll: gtk::ScrolledWindow,
    folder_selection: gtk::SingleSelection,
    folder_nodes: RefCell<Vec<Node<String>>>,
    rows: RefCell<Vec<DisplayRow>>,
    cursor: RefCell<Option<(u32, String)>>,
    selected: RefCell<Vec<u32>>,
    marks: RefCell<HashSet<String>>,
    boxes: Cell<bool>,
    active: Cell<bool>,
    /// A plain press on one of several selected rows: it selects on release, so a drag keeps them all.
    held_click: RefCell<Option<P>>,
    /// Where the left press went down, in window coordinates, and whether it was on a row.
    drag_start: Cell<Option<(f64, f64)>>,
    drag_armed: Cell<bool>,
    dragging: Cell<bool>,
    drawn: RefCell<Vec<Drawn>>,
}

fn list_item(obj: &glib::Object) -> &gtk::ListItem {
    obj.downcast_ref().expect("a list factory's item")
}

fn node_in(item: &gtk::ListItem) -> Option<glib::BoxedAnyObject> {
    let row = item.item().and_downcast::<gtk::TreeListRow>()?;
    row.item().and_downcast()
}

fn row_in(item: &gtk::ListItem) -> Option<DisplayRow> {
    let node = node_in(item)?;
    let node = node.borrow::<Node<DisplayRow>>();
    Some(node.data.clone())
}

fn toggle_class(widget: &impl IsA<gtk::Widget>, class: &str, on: bool) {
    if widget.has_css_class(class) != on {
        if on {
            widget.add_css_class(class);
        } else {
            widget.remove_css_class(class);
        }
    }
}

fn sort_of(pane_sort: (SortColumn, bool)) -> gtk::SortType {
    match pane_sort.1 {
        true => gtk::SortType::Descending,
        false => gtk::SortType::Ascending,
    }
}

fn row_icon(row: &DisplayRow) -> &'static str {
    if row.is_parent() {
        "go-up-symbolic"
    } else if row.is_dir {
        "folder-symbolic"
    } else if row.is_wrapper {
        "package-x-generic-symbolic"
    } else {
        "text-x-generic-symbolic"
    }
}

fn node_of(row: &DisplayRow) -> Node<DisplayRow> {
    Node {
        id: row.node_id.clone(),
        depth: row.depth,
        expandable: row.expandable,
        expanded: row.expanded,
        data: row.clone(),
    }
}

impl PaneView {
    pub fn build(side: Side, host: Rc<dyn PaneHost>) -> Rc<PaneView> {
        Rc::new_cyclic(|me: &Weak<PaneView>| {
            let tree = FlatTree::new();
            let selection = gtk::MultiSelection::new(Some(tree.model().clone()));
            let view = gtk::ColumnView::builder()
                .model(&selection)
                .show_column_separators(false)
                .show_row_separators(false)
                .reorderable(false)
                .build();
            view.add_css_class("rb-list");
            view.set_widget_name(ids::LIST);

            let mark = Self::mark_column(me);
            let name = Self::name_column(me);
            name.set_expand(true);
            let data = Self::text_column(me, "Data", 1.0, |r| match r.is_dir {
                true => String::new(),
                false => format_size(r.size),
            });
            let rsrc = Self::text_column(me, "Rsrc", 1.0, |r| match r.rsrc_size {
                0 => String::new(),
                n => format_size(n),
            });
            let modified = Self::text_column(me, "Modified", 0.0, |r| r.modified.clone());
            let kind = Self::text_column(me, "Type", 0.0, |r| r.type_tag.clone());
            for column in [&mark, &name, &data, &rsrc, &modified, &kind] {
                view.append_column(column);
            }
            let sortable = [
                (SortColumn::Name, name),
                (SortColumn::Size, data),
                (SortColumn::Modified, modified),
                (SortColumn::Type, kind),
            ];
            for (_, column) in &sortable {
                let any_order = gtk::CustomSorter::new(|_, _| Ordering::Equal.into());
                column.set_sorter(Some(&any_order));
            }
            Self::watch_sort(me, &view);
            Self::watch_empty_clicks(me, &view);

            let scroll = gtk::ScrolledWindow::builder()
                .child(&view)
                .vexpand(true)
                .hexpand(true)
                .build();

            let folders = FlatTree::new();
            let folder_selection = gtk::SingleSelection::builder()
                .model(folders.model())
                .autoselect(false)
                .can_unselect(true)
                .build();
            let folder_view = gtk::ListView::new(
                Some(folder_selection.clone()),
                Some(Self::folder_factory(me)),
            );
            folder_view.add_css_class("rb-list");
            folder_view.set_widget_name(ids::FOLDER_TREE);
            let folder_scroll = gtk::ScrolledWindow::builder()
                .child(&folder_view)
                .min_content_height(100)
                .visible(false)
                .build();

            let paned = gtk::Paned::builder()
                .orientation(gtk::Orientation::Vertical)
                .start_child(&folder_scroll)
                .end_child(&scroll)
                .resize_start_child(false)
                .shrink_start_child(false)
                .shrink_end_child(false)
                .position(170)
                .build();

            let message = gtk::Label::builder()
                .wrap(true)
                .justify(gtk::Justification::Center)
                .build();
            let stack = gtk::Stack::new();
            stack.add_named(&paned, Some("list"));
            stack.add_named(&message, Some("message"));
            let root = gtk::Box::new(gtk::Orientation::Vertical, 0);
            root.append(&stack);
            stack.set_vexpand(true);

            let keys = gtk::EventControllerKey::new();
            keys.set_propagation_phase(gtk::PropagationPhase::Capture);
            let target = me.clone();
            keys.connect_key_pressed(move |_, key, _, state| {
                let pressed = key_of(key).map(|k| (k, mods_of(state)));
                let used = target
                    .upgrade()
                    .zip(pressed)
                    .is_some_and(|(pane, (k, m))| pane.host.key(pane.side, k, m));
                match used {
                    true => glib::Propagation::Stop,
                    false => glib::Propagation::Proceed,
                }
            });
            root.add_controller(keys);

            PaneView {
                side,
                host,
                me: me.clone(),
                root,
                stack,
                message,
                view,
                scroll,
                tree,
                selection,
                mark,
                rsrc,
                sortable,
                folders,
                folder_view,
                folder_scroll,
                folder_selection,
                folder_nodes: RefCell::default(),
                rows: RefCell::default(),
                cursor: RefCell::default(),
                selected: RefCell::default(),
                marks: RefCell::default(),
                boxes: Cell::new(false),
                active: Cell::new(true),
                held_click: RefCell::default(),
                drag_start: Cell::new(None),
                drag_armed: Cell::new(false),
                dragging: Cell::new(false),
                drawn: RefCell::default(),
            }
        })
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    /// The rows the listing shows at once, the step for Page Up and Page Down.
    pub fn page_rows(&self) -> i32 {
        (self.scroll.height() / 30).max(1)
    }

    /// Give the listing the keyboard.
    pub fn focus(&self) {
        self.view.grab_focus();
    }

    /// Name the listing with a host's own id, for a host with more than one pane.
    pub fn set_list_id(&self, id: &str) {
        self.view.set_widget_name(id);
    }

    // --- reading ----------------------------------------------------------

    pub fn read(&self, ctl: &CommanderController) -> PaneShown {
        let pane = ctl.pane(self.side);
        let all = pane.rows();
        let rows = (*self.rows.borrow() != all).then(|| all.to_vec());
        let style = pane.listing_style();
        let boxes = pane.marks_with_boxes();
        // The tree has no `..`, so list positions shift by the row it leaves out.
        let skip = match boxes {
            true => all.iter().take_while(|r| r.is_parent()).count(),
            false => 0,
        };
        let cursor = pane.cursor_index().filter(|i| *i >= skip);
        let cursor = cursor.map(|i| ((i - skip) as u32, all[i].node_id.clone()));
        let mut marks = HashSet::new();
        let mut selected = Vec::new();
        if pane.has_selection() {
            for (i, row) in all.iter().enumerate().skip(skip) {
                if pane.row_selected(row) {
                    marks.insert(row.node_id.clone());
                    selected.push((i - skip) as u32);
                }
            }
        }
        let folders: Vec<Node<String>> = pane
            .folder_tree()
            .iter()
            .map(|n| Node {
                id: n.entry.path.clone(),
                depth: n.depth,
                expandable: true,
                expanded: n.expanded,
                data: match n.depth {
                    0 => "/".to_string(),
                    _ => format!("{}/", n.entry.name),
                },
            })
            .collect();
        let folders = (*self.folder_nodes.borrow() != folders).then_some(folders);
        let message = match (pane.opening(), pane.source_error()) {
            (Some(_), _) => "Reading the volume...".to_string(),
            (None, Some(err)) => err.to_string(),
            (None, None) if pane.is_opening() => "Opening...".to_string(),
            (None, None) => "Open a disk image or container to browse it here.".to_string(),
        };
        PaneShown {
            rows,
            style,
            loaded: pane.listing.is_loaded(),
            message,
            boxes,
            active: ctl.is_single() || ctl.active() == self.side,
            cursor,
            selected,
            marks,
            folders,
            folder_cursor: pane.tree_cursor() as u32,
            folder_focus: pane.tree_focused(),
            sort: (pane.listing.sort_column(), pane.listing.is_descending()),
        }
    }

    // --- drawing ----------------------------------------------------------

    pub fn show(&self, s: PaneShown) {
        let name = if s.loaded { "list" } else { "message" };
        if self.stack.visible_child_name().as_deref() != Some(name) {
            self.stack.set_visible_child_name(name);
        }
        set_text(&self.message, &s.message);
        self.mark.set_visible(s.boxes);
        self.boxes.set(s.boxes);
        self.active.set(s.active);
        let folder_tree = s.style == ListingStyle::TreeAndList;
        self.folder_scroll.set_visible(folder_tree);
        if let Some(folders) = s.folders {
            *self.folder_nodes.borrow_mut() = folders.clone();
            self.folders.sync(folders);
        }
        if folder_tree {
            let there = s.folder_cursor < self.folders.model().n_items();
            if there && self.folder_selection.selected() != s.folder_cursor {
                self.folder_selection.set_selected(s.folder_cursor);
                self.folder_view
                    .scroll_to(s.folder_cursor, gtk::ListScrollFlags::NONE, None);
            }
            toggle_class(
                &self.folder_view,
                "rb-tree-focus",
                s.folder_focus && s.active,
            );
        }
        if let Some(rows) = s.rows {
            let nodes: Vec<_> = rows
                .iter()
                .filter(|r| !(s.boxes && r.is_parent()))
                .map(node_of)
                .collect();
            self.rsrc.set_visible(rows.iter().any(|r| r.rsrc_size > 0));
            *self.rows.borrow_mut() = rows;
            self.tree.sync(nodes);
            *self.selected.borrow_mut() = vec![u32::MAX];
        }
        *self.marks.borrow_mut() = s.marks;
        let moved = *self.cursor.borrow() != s.cursor;
        *self.cursor.borrow_mut() = s.cursor.clone();
        let shown = match (s.boxes, &s.cursor) {
            (true, Some((position, _))) => vec![*position],
            (true, None) => Vec::new(),
            (false, _) => s.selected,
        };
        if *self.selected.borrow() != shown {
            let set = gtk::Bitset::new_empty();
            for position in &shown {
                set.add(*position);
            }
            let all = gtk::Bitset::new_range(0, self.tree.model().n_items());
            self.selection.set_selection(&set, &all);
            *self.selected.borrow_mut() = shown;
        }
        if let (true, Some((position, _))) = (moved, &s.cursor) {
            self.view
                .scroll_to(*position, None, gtk::ListScrollFlags::NONE, None);
        }
        self.show_sort(s.sort);
        self.redraw();
    }

    fn show_sort(&self, sort: (SortColumn, bool)) {
        let Some(sorter) = self.view.sorter().and_downcast::<gtk::ColumnViewSorter>() else {
            return;
        };
        let column = self.sortable.iter().find(|(c, _)| *c == sort.0);
        let Some((_, column)) = column else {
            return;
        };
        let order = sort_of(sort);
        if sorter.primary_sort_column().as_ref() != Some(column)
            || sorter.primary_sort_order() != order
        {
            self.view.sort_by_column(Some(column), order);
        }
    }

    fn redraw(&self) {
        let cells: Vec<_> = {
            let mut drawn = self.drawn.borrow_mut();
            drawn.retain(|c| c.item.upgrade().is_some());
            drawn
                .iter()
                .filter_map(|c| Some((c.item.upgrade()?, c.draw.clone())))
                .collect()
        };
        for (item, draw) in cells {
            draw(&item);
        }
    }

    // --- columns ----------------------------------------------------------

    /// Keep `draw` for `item`, so `redraw` reaches it, and run it whenever the cell gets a row.
    fn register(&self, item: &gtk::ListItem, draw: Rc<dyn Fn(&gtk::ListItem)>) {
        let weak = glib::WeakRef::new();
        weak.set(Some(item));
        self.drawn.borrow_mut().push(Drawn { item: weak, draw });
    }

    /// The cursor's outline on every cell of its row.
    fn mark_cursor(&self, cell: &impl IsA<gtk::Widget>, row: &DisplayRow) {
        let cursor = self.cursor.borrow();
        let here = cursor.as_ref().is_some_and(|(_, id)| *id == row.node_id);
        toggle_class(
            cell,
            "rb-cursor",
            here && !self.boxes.get() && self.active.get(),
        );
    }

    fn text_column(
        me: &Weak<PaneView>,
        title: &str,
        xalign: f32,
        text: fn(&DisplayRow) -> String,
    ) -> gtk::ColumnViewColumn {
        let factory = gtk::SignalListItemFactory::new();
        let setup = me.clone();
        factory.connect_setup(move |_, obj| {
            let item = list_item(obj);
            let label = gtk::Label::builder()
                .xalign(xalign)
                .hexpand(true)
                .ellipsize(gtk::pango::EllipsizeMode::End)
                .build();
            label.add_css_class("rb-row");
            let cell = gtk::Box::new(gtk::Orientation::Horizontal, 0);
            cell.append(&label);
            item.set_child(Some(&cell));
            let Some(pane) = setup.upgrade() else {
                return;
            };
            pane.clicks(&cell, item);
            let target = setup.clone();
            pane.register(
                item,
                Rc::new(move |item| {
                    let (Some(pane), Some(row)) = (target.upgrade(), row_in(item)) else {
                        return;
                    };
                    set_text(&label, &text(&row));
                    pane.mark_cursor(&cell, &row);
                }),
            );
        });
        Self::draw_on_bind(me, &factory);
        let column = gtk::ColumnViewColumn::new(Some(title), Some(factory));
        column.set_resizable(true);
        column
    }

    fn draw_on_bind(me: &Weak<PaneView>, factory: &gtk::SignalListItemFactory) {
        let target = me.clone();
        factory.connect_bind(move |_, obj| {
            let item = list_item(obj);
            if let Some(pane) = target.upgrade() {
                let draw = pane
                    .drawn
                    .borrow()
                    .iter()
                    .find(|c| c.item.upgrade().as_ref() == Some(item))
                    .map(|c| c.draw.clone());
                if let Some(draw) = draw {
                    draw(item);
                }
            }
        });
    }

    fn name_column(me: &Weak<PaneView>) -> gtk::ColumnViewColumn {
        let factory = gtk::SignalListItemFactory::new();
        let setup = me.clone();
        factory.connect_setup(move |_, obj| {
            let item = list_item(obj);
            let icon = gtk::Image::new();
            let label = gtk::Label::builder()
                .xalign(0.0)
                .hexpand(true)
                .single_line_mode(true)
                .ellipsize(gtk::pango::EllipsizeMode::End)
                .build();
            let inner = gtk::Box::new(gtk::Orientation::Horizontal, 6);
            inner.append(&icon);
            inner.append(&label);
            let expander = gtk::TreeExpander::builder()
                .child(&inner)
                .indent_for_icon(true)
                .hexpand(true)
                .build();
            expander.add_css_class("rb-row");
            let cell = gtk::Box::new(gtk::Orientation::Horizontal, 0);
            cell.append(&expander);
            item.set_child(Some(&cell));
            let Some(pane) = setup.upgrade() else {
                return;
            };
            pane.clicks(&cell, item);
            Self::watch_expander(&setup, &expander);
            let target = setup.clone();
            pane.register(
                item,
                Rc::new(move |item| {
                    let list_row = item.item().and_downcast::<gtk::TreeListRow>();
                    if expander.list_row() != list_row {
                        expander.set_list_row(list_row.as_ref());
                    }
                    let (Some(pane), Some(row)) = (target.upgrade(), row_in(item)) else {
                        return;
                    };
                    let shown = pane.host.label(pane.side, &row);
                    let text = match row.loading {
                        true => format!("{}  (loading...)", shown.text),
                        false => shown.text,
                    };
                    set_text(&label, &text);
                    label.set_tooltip_text(shown.hover.as_deref().map(plain).as_deref());
                    icon.set_icon_name(Some(row_icon(&row)));
                    for (class, tone) in [
                        ("rb-added", RowTone::Added),
                        ("rb-changed", RowTone::Changed),
                        ("rb-unresolved", RowTone::Unresolved),
                        ("rb-deleted", RowTone::Deleted),
                    ] {
                        toggle_class(&label, class, shown.tone == tone);
                    }
                    toggle_class(&label, "rb-image", row.is_image);
                    let attrs = gtk::pango::AttrList::new();
                    if shown.tone == RowTone::Deleted {
                        attrs.insert(gtk::pango::AttrInt::new_strikethrough(true));
                    }
                    label.set_attributes(Some(&attrs));
                    pane.mark_cursor(&cell, &row);
                }),
            );
        });
        Self::draw_on_bind(me, &factory);
        let column = gtk::ColumnViewColumn::new(Some("Name"), Some(factory));
        column.set_resizable(true);
        column
    }

    fn mark_column(me: &Weak<PaneView>) -> gtk::ColumnViewColumn {
        let factory = gtk::SignalListItemFactory::new();
        let setup = me.clone();
        factory.connect_setup(move |_, obj| {
            let item = list_item(obj);
            let check = gtk::CheckButton::new();
            check.set_tooltip_text(Some("Mark for multi-select export"));
            item.set_child(Some(&check));
            let weak = item.downgrade();
            let target = setup.clone();
            check.connect_toggled(move |_| {
                let row = weak.upgrade().and_then(|i| row_in(&i));
                if let (Some(pane), Some(row)) = (target.upgrade(), row) {
                    pane.host
                        .send(A::Pane(pane.side, P::Mark(row.node_id.clone())));
                }
            });
            let Some(pane) = setup.upgrade() else {
                return;
            };
            let target = setup.clone();
            pane.register(
                item,
                Rc::new(move |item| {
                    let (Some(pane), Some(row)) = (target.upgrade(), row_in(item)) else {
                        return;
                    };
                    let markable =
                        !matches!(row.kind, RowKind::PendingAdd | RowKind::PendingDelete);
                    check.set_visible(markable);
                    let on = pane.marks.borrow().contains(&row.node_id);
                    if check.is_active() != on {
                        check.set_active(on);
                    }
                }),
            );
        });
        Self::draw_on_bind(me, &factory);
        gtk::ColumnViewColumn::new(None, Some(factory))
    }

    /// The user opened or closed a row through its arrow; the controller decides what that means.
    fn watch_expander(me: &Weak<PaneView>, expander: &gtk::TreeExpander) {
        let held: Rc<RefCell<Option<(gtk::TreeListRow, glib::SignalHandlerId)>>> = Rc::default();
        let target = me.clone();
        expander.connect_list_row_notify(move |e| {
            if let Some((row, id)) = held.borrow_mut().take() {
                row.disconnect(id);
            }
            let Some(row) = e.list_row() else {
                return;
            };
            let target = target.clone();
            let id = row.connect_expanded_notify(move |row| {
                if let Some(pane) = target.upgrade() {
                    pane.user_toggled(row);
                }
            });
            *held.borrow_mut() = Some((row, id));
        });
    }

    fn user_toggled(&self, row: &gtk::TreeListRow) {
        let node = row.item().and_downcast::<glib::BoxedAnyObject>();
        let Some(id) = node.map(|n| n.borrow::<Node<DisplayRow>>().id.clone()) else {
            return;
        };
        // Sent later: the model is still inside its own toggle.
        let (side, target) = (self.side, self.me.clone());
        let row = row.clone();
        glib::idle_add_local_once(move || {
            if let Some(pane) = target.upgrade() {
                if pane
                    .rows
                    .borrow()
                    .iter()
                    .any(|r| r.node_id == id && r.expanded != row.is_expanded())
                {
                    pane.tree.note_toggled(&row, id.clone());
                    pane.host.send(A::Focus(side));
                    pane.host.send(A::Pane(side, P::ToggleExpand(id)));
                }
            }
        });
    }

    fn clicks(&self, cell: &gtk::Box, item: &gtk::ListItem) {
        let gesture = gtk::GestureClick::new();
        gesture.set_button(0);
        let weak = item.downgrade();
        let target = self.me.clone();
        let cell_weak = cell.downgrade();
        gesture.connect_pressed(move |g, n, x, y| {
            let row = weak.upgrade().and_then(|i| row_in(&i));
            let (Some(pane), Some(row), Some(cell)) = (target.upgrade(), row, cell_weak.upgrade())
            else {
                return;
            };
            // The arrow is the expander's own; a press there must reach it unclaimed.
            let mut hit = cell.pick(x, y, gtk::PickFlags::DEFAULT);
            while let Some(w) = hit.filter(|w| *w != *cell.upcast_ref::<gtk::Widget>()) {
                if w.css_name() == "expander" {
                    return;
                }
                hit = w.parent();
            }
            g.set_state(gtk::EventSequenceState::Claimed);
            pane.pressed(g, n, &row, &cell, (x, y));
        });
        cell.add_controller(gesture);
    }

    /// Follow presses from `screen`, an ancestor: raw events, since a claimed click denies a gesture
    /// and the pointer's grab does not survive the row being selected.
    pub fn watch_drags(&self, screen: &impl IsA<gtk::Widget>) {
        let legacy = gtk::EventControllerLegacy::new();
        legacy.set_propagation_phase(gtk::PropagationPhase::Capture);
        let target = self.me.clone();
        legacy.connect_event(move |_, event| {
            if let Some(pane) = target.upgrade() {
                pane.drag_event(event);
            }
            glib::Propagation::Proceed
        });
        screen.add_controller(legacy);
    }

    fn drag_event(&self, event: &gtk::gdk::Event) {
        use gtk::gdk::EventType as E;
        let (Some((x, y)), Some(native), Some(root)) =
            (event.position(), self.view.native(), self.view.root())
        else {
            return;
        };
        let (dx, dy) = native.surface_transform();
        let at = (x - dx, y - dy);
        let root: &gtk::Widget = root.upcast_ref();
        match event.event_type() {
            E::ButtonPress => self.drag_start.set(Some(at)),
            E::MotionNotify if self.drag_armed.get() => {
                let Some(start) = self.drag_start.get() else {
                    return;
                };
                let (sx, sy, ex, ey) = (start.0 as i32, start.1 as i32, at.0 as i32, at.1 as i32);
                if self.dragging.get() || self.view.drag_check_threshold(sx, sy, ex, ey) {
                    self.dragging.set(true);
                    self.held_click.borrow_mut().take();
                    self.host.dragged(self.side, root, at, false);
                }
            }
            E::ButtonRelease => {
                self.drag_armed.set(false);
                if self.dragging.replace(false) {
                    self.host.dragged(self.side, root, at, true);
                }
                let held = self.held_click.borrow_mut().take();
                if let Some(action) = held {
                    self.host.send(A::Pane(self.side, action));
                }
            }
            _ => {}
        }
    }

    fn pressed(
        &self,
        gesture: &gtk::GestureClick,
        n_press: i32,
        row: &DisplayRow,
        cell: &gtk::Box,
        at: (f64, f64),
    ) {
        use gtk::gdk::ModifierType as M;
        let side = self.side;
        let state = gesture.current_event_state();
        let (ctrl, shift) = (
            state.contains(M::CONTROL_MASK),
            state.contains(M::SHIFT_MASK),
        );
        self.view.grab_focus();
        self.host.send(A::Focus(side));
        match gesture.current_button() {
            1 if n_press == 2 => {
                self.host
                    .send(A::Pane(side, P::Activate(row.node_id.clone())));
            }
            1 if n_press == 1 => {
                self.drag_armed.set(!row.is_parent());
                let action = self.host.click(side, row, ctrl, shift);
                let many = self.selected.borrow().len() > 1;
                let one_of_many = many && self.marks.borrow().contains(&row.node_id);
                match action {
                    Some(action) if one_of_many && !ctrl && !shift => {
                        *self.held_click.borrow_mut() = Some(action);
                    }
                    Some(action) => self.host.send(A::Pane(side, action)),
                    None => {}
                }
            }
            3 if n_press == 1 => {
                if let Some(action) = self.host.context(side, row) {
                    self.host.send(A::Pane(side, action));
                    self.popup(row, cell, at);
                }
            }
            _ => {}
        }
    }

    /// The row's menu at the pointer; it is built from the controller's list each time.
    fn popup(&self, row: &DisplayRow, cell: &gtk::Box, at: (f64, f64)) {
        let entries = self.host.menu(self.side, row);
        let popover = gtk::Popover::new();
        popover.set_has_arrow(false);
        popover.set_parent(&self.root);
        let items = gtk::Box::new(gtk::Orientation::Vertical, 0);
        for entry in entries {
            match entry {
                RowMenuEntry::Item {
                    label,
                    action,
                    enabled,
                } => {
                    let button = self.menu_button(&label, action, &popover);
                    button.set_sensitive(enabled);
                    items.append(&button);
                }
                RowMenuEntry::Submenu { label, items: sub } => {
                    let inner = gtk::Box::new(gtk::Orientation::Vertical, 0);
                    for (label, action) in sub {
                        inner.append(&self.menu_button(&label, action, &popover));
                    }
                    let expander = gtk::Expander::builder().label(&label).child(&inner).build();
                    items.append(&expander);
                }
            }
        }
        popover.set_child(Some(&items));
        let point = graphene::Point::new(at.0 as f32, at.1 as f32);
        if let Some(p) = cell.compute_point(&self.root, &point) {
            let rect = gtk::gdk::Rectangle::new(p.x() as i32, p.y() as i32, 1, 1);
            popover.set_pointing_to(Some(&rect));
        }
        popover.connect_closed(|p| {
            let p = p.clone();
            glib::idle_add_local_once(move || p.unparent());
        });
        popover.popup();
    }

    fn menu_button(&self, label: &str, action: A, popover: &gtk::Popover) -> gtk::Button {
        let button = gtk::Button::with_label(&plain(label));
        button.add_css_class("flat");
        if let Some(child) = button.child().and_downcast::<gtk::Label>() {
            child.set_xalign(0.0);
        }
        let target = self.me.clone();
        let popover = popover.downgrade();
        button.connect_clicked(move |_| {
            if let Some(popover) = popover.upgrade() {
                popover.popdown();
            }
            if let Some(pane) = target.upgrade() {
                pane.host.send(action.clone());
            }
        });
        button
    }

    fn watch_sort(me: &Weak<PaneView>, view: &gtk::ColumnView) {
        let Some(sorter) = view.sorter().and_downcast::<gtk::ColumnViewSorter>() else {
            return;
        };
        let target = me.clone();
        sorter.connect_changed(move |s, _| {
            let (Some(pane), Some(column)) = (target.upgrade(), s.primary_sort_column()) else {
                return;
            };
            let col = pane.sortable.iter().find(|(_, c)| *c == column);
            if let Some((col, _)) = col {
                pane.host.send(A::Pane(pane.side, P::SortBy(*col)));
            }
        });
    }

    /// A press that reached the view itself missed every row: it clears the selection.
    fn watch_empty_clicks(me: &Weak<PaneView>, view: &gtk::ColumnView) {
        let gesture = gtk::GestureClick::new();
        let target = me.clone();
        gesture.connect_pressed(move |g, _, x, y| {
            let Some(pane) = target.upgrade() else {
                return;
            };
            let mut hit = g
                .widget()
                .and_then(|w| w.pick(x, y, gtk::PickFlags::DEFAULT));
            while let Some(w) = hit {
                if w.css_name() == "header" {
                    return;
                }
                hit = w.parent();
            }
            pane.view.grab_focus();
            pane.host.send(A::Focus(pane.side));
            pane.host.send(A::Pane(pane.side, P::ClearSelection));
        });
        view.add_controller(gesture);
    }

    fn folder_factory(me: &Weak<PaneView>) -> gtk::SignalListItemFactory {
        let factory = gtk::SignalListItemFactory::new();
        let setup = me.clone();
        factory.connect_setup(move |_, obj| {
            let item = list_item(obj);
            let label = gtk::Label::builder()
                .xalign(0.0)
                .single_line_mode(true)
                .build();
            let icon = gtk::Image::from_icon_name("folder-symbolic");
            let inner = gtk::Box::new(gtk::Orientation::Horizontal, 6);
            inner.append(&icon);
            inner.append(&label);
            let expander = gtk::TreeExpander::builder()
                .child(&inner)
                .hexpand(true)
                .build();
            expander.add_css_class("rb-row");
            let cell = gtk::Box::new(gtk::Orientation::Horizontal, 0);
            cell.append(&expander);
            item.set_child(Some(&cell));
            Self::watch_folder_expander(&setup, &expander);
            let gesture = gtk::GestureClick::new();
            let weak = item.downgrade();
            let target = setup.clone();
            gesture.connect_pressed(move |g, n, x, y| {
                let row = weak
                    .upgrade()
                    .and_then(|i| i.item().and_downcast::<gtk::TreeListRow>());
                let (Some(pane), Some(row)) = (target.upgrade(), row) else {
                    return;
                };
                let mut hit = g
                    .widget()
                    .and_then(|c| c.pick(x, y, gtk::PickFlags::DEFAULT));
                while let Some(w) = hit {
                    if w.css_name() == "expander" {
                        return;
                    }
                    hit = w.parent().filter(|p| Some(p) != g.widget().as_ref());
                }
                g.set_state(gtk::EventSequenceState::Claimed);
                let at = row.position() as usize;
                pane.folder_view.grab_focus();
                pane.host.send(A::Focus(pane.side));
                let action = match n {
                    2 => P::FolderTreeToggle(at),
                    _ => P::FolderTreeClick(at),
                };
                pane.host.send(A::Pane(pane.side, action));
            });
            cell.add_controller(gesture);
        });
        factory.connect_bind(|_, obj| {
            let item = list_item(obj);
            let row = item.item().and_downcast::<gtk::TreeListRow>();
            let Some(cell) = item.child() else {
                return;
            };
            let Some(expander) = cell.first_child().and_downcast::<gtk::TreeExpander>() else {
                return;
            };
            expander.set_list_row(row.as_ref());
            let name = row
                .as_ref()
                .and_then(|r| r.item())
                .and_downcast::<glib::BoxedAnyObject>();
            let label = expander
                .child()
                .and_then(|c| c.last_child())
                .and_downcast::<gtk::Label>();
            if let (Some(name), Some(label)) = (name, label) {
                set_text(&label, &name.borrow::<Node<String>>().data);
            }
        });
        factory
    }

    fn watch_folder_expander(me: &Weak<PaneView>, expander: &gtk::TreeExpander) {
        let held: Rc<RefCell<Option<(gtk::TreeListRow, glib::SignalHandlerId)>>> = Rc::default();
        let target = me.clone();
        expander.connect_list_row_notify(move |e| {
            if let Some((row, id)) = held.borrow_mut().take() {
                row.disconnect(id);
            }
            let Some(row) = e.list_row() else {
                return;
            };
            let target = target.clone();
            let id = row.connect_expanded_notify(move |row| {
                let Some(pane) = target.upgrade() else {
                    return;
                };
                let node = row.item().and_downcast::<glib::BoxedAnyObject>();
                let id = node.map(|n| n.borrow::<Node<String>>().id.clone());
                let (side, weak, row) = (pane.side, target.clone(), row.clone());
                glib::idle_add_local_once(move || {
                    let (Some(pane), Some(id)) = (weak.upgrade(), id) else {
                        return;
                    };
                    let nodes = pane.folder_nodes.borrow();
                    let at = nodes.iter().position(|n| n.id == id);
                    let changed = at.filter(|i| nodes[*i].expanded != row.is_expanded());
                    drop(nodes);
                    if let Some(at) = changed {
                        pane.folders.note_toggled(&row, id);
                        pane.host.send(A::Pane(side, P::FolderTreeToggle(at)));
                    }
                });
            });
            *held.borrow_mut() = Some((row, id));
        });
    }
}
