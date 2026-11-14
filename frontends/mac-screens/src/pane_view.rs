//! One browser pane's listing over a `CommanderController` side: the folder tree and the list,
//! each a host pane list with the row icons, tick boxes for marks and the indent of a row inside
//! a folder. The Win32 shell's `pane_view.rs` is the reference; Inspect's browse view has one and
//! Commander two.
//!
//! The view sends nothing itself: it reports what the user did as a [`PaneEvent`], and the screen
//! that owns it turns that into actions on its controller. Keys go through the controller's map:
//! the list hands over the ones it does not use itself (arrows move its own selection).

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use rusty_backup::controller::commander::{
    format_size, BrowserPaneAction as P, CommanderController, DisplayRow, ListingStyle, Side,
    SortColumn,
};
use rusty_backup::controller::keys::{Key, Mods};

use super::form::Form;
use super::host::{Ctl, Host, RowIcon, RowLine, RowsEvent, RowsSpec, Surface};

/// The listing's columns: title, width in points, right-aligned, the sort they ask for.
const COLUMNS: [(&str, f64, bool, Option<SortColumn>); 5] = [
    ("Name", 280.0, false, Some(SortColumn::Name)),
    ("Data", 90.0, true, Some(SortColumn::Size)),
    ("Rsrc", 80.0, true, None),
    ("Modified", 150.0, false, Some(SortColumn::Modified)),
    ("Type", 90.0, false, Some(SortColumn::Type)),
];

/// The column that shows resource-fork sizes, hidden until a listing has one.
const RSRC: usize = 2;

pub enum PaneEvent {
    /// The selection moved to `row` (a click or the arrow keys).
    Click {
        row: DisplayRow,
        ctrl: bool,
        shift: bool,
    },
    /// The row's tick box.
    Mark(String),
    /// The row's +/- icon.
    Toggle(String),
    /// A double-click: open what is under the cursor.
    Activate,
    Key(Key, Mods),
    Sort(SortColumn),
    /// A right-click on list position `row`, at `(x, y)` in the list.
    Menu(usize, f64, f64),
    /// A folder-tree row was picked or its icon clicked.
    Folder(P),
    /// The list took the keyboard focus: its pane is the one keys act on.
    Focused,
}

/// One listing row as the list draws it.
#[derive(Clone, PartialEq)]
struct Line {
    cells: [String; 5],
    marked: bool,
    node_id: String,
    expandable: bool,
    depth: usize,
    is_dir: bool,
    expanded: bool,
}

/// What a refresh shows, read while the controller is borrowed.
pub struct PaneShown {
    rows: Vec<DisplayRow>,
    lines: Vec<Line>,
    rsrc: bool,
    cursor: Option<usize>,
    folders: Vec<(usize, String, bool)>,
    folder_tree: bool,
    folder_cursor: usize,
    boxes: bool,
}

/// What the lists show now; the event mapping reads it.
#[derive(Default)]
struct State {
    rows: Vec<DisplayRow>,
    lines: Vec<Line>,
    /// Listing positions shift by the `..` rows a tick-box listing leaves out.
    skip: usize,
    folders: Vec<(usize, String, bool)>,
    boxes: bool,
    rsrc: bool,
    events: Option<Rc<dyn Fn(PaneEvent)>>,
}

fn line_of(row: &DisplayRow, name: String) -> Line {
    Line {
        marked: false,
        cells: [
            name,
            match row.is_dir {
                true => String::new(),
                false => format_size(row.size),
            },
            match row.rsrc_size {
                0 => String::new(),
                n => format_size(n),
            },
            row.modified.clone(),
            row.type_tag.clone(),
        ],
        node_id: row.node_id.clone(),
        expandable: row.expandable,
        depth: row.depth,
        is_dir: row.is_dir,
        expanded: row.expanded,
    }
}

pub struct PaneView {
    side: Side,
    ui: Rc<dyn Host>,
    surface: Surface,
    list: Ctl,
    tree: Ctl,
    state: Rc<RefCell<State>>,
    shown_cursor: Cell<Option<usize>>,
}

impl PaneView {
    /// The pane's list as `list_k` and its folder tree as `tree_k` on `form`.
    pub fn build<K: Copy + PartialEq + 'static>(
        form: &Form<K>,
        list_k: K,
        tree_k: K,
        side: Side,
        list_id: &str,
        tree_id: &str,
    ) -> PaneView {
        let ui = form.host().clone();
        let surface = form.surface();
        let state: Rc<RefCell<State>> = Rc::new(RefCell::new(State::default()));
        let (list, tree) = (form.custom(list_k), form.custom(tree_k));
        let st = state.clone();
        let list_spec = RowsSpec {
            columns: COLUMNS
                .iter()
                .map(|(t, w, r, _)| (t.to_string(), *w, *r))
                .collect(),
            header: true,
            multiple: true,
            stripes: true,
        };
        ui.rows_view(
            surface,
            list,
            list_id,
            &list_spec,
            Rc::new(move |e| {
                let event = list_event(&st.borrow(), e);
                send(&st, event);
            }),
        );
        let st = state.clone();
        let tree_spec = RowsSpec {
            columns: vec![(String::new(), 600.0, false)],
            header: false,
            multiple: false,
            stripes: false,
        };
        ui.rows_view(
            surface,
            tree,
            tree_id,
            &tree_spec,
            Rc::new(move |e| {
                let event = match e {
                    RowsEvent::Select { row, .. } => {
                        Some(PaneEvent::Folder(P::FolderTreeClick(row)))
                    }
                    RowsEvent::Icon(row) => Some(PaneEvent::Folder(P::FolderTreeToggle(row))),
                    RowsEvent::Key(k, m) => Some(PaneEvent::Key(k, m)),
                    RowsEvent::Focused => Some(PaneEvent::Focused),
                    _ => None,
                };
                send(&st, event);
            }),
        );
        // Rsrc stays hidden until a listing has resource forks.
        ui.hide_column(surface, list, RSRC, true);
        PaneView {
            side,
            ui,
            surface,
            list,
            tree,
            state,
            shown_cursor: Cell::new(None),
        }
    }

    /// Where the pane's user actions go.
    pub fn on_event(&self, f: Rc<dyn Fn(PaneEvent)>) {
        self.state.borrow_mut().events = Some(f);
    }

    pub fn read(&self, ctl: &CommanderController) -> PaneShown {
        let pane = ctl.pane(self.side);
        let all = pane.rows();
        let skip = match pane.marks_with_boxes() {
            true => all.iter().take_while(|r| r.is_parent()).count(),
            false => 0,
        };
        let lines = all
            .iter()
            .skip(skip)
            .map(|r| {
                let mut l = line_of(r, pane.row_label(r, false).text);
                l.marked = pane.has_selection() && pane.row_selected(r);
                l
            })
            .collect();
        let folders = pane
            .folder_tree()
            .iter()
            .map(|n| {
                let name = match n.depth {
                    0 => "/".to_string(),
                    _ => n.entry.name.clone(),
                };
                (n.depth, name, n.expanded)
            })
            .collect();
        PaneShown {
            rsrc: all.iter().any(|r| r.rsrc_size > 0),
            rows: all.to_vec(),
            lines,
            cursor: pane.cursor_index().filter(|i| *i >= skip).map(|i| i - skip),
            folders,
            folder_tree: pane.listing_style() == ListingStyle::TreeAndList,
            folder_cursor: pane.tree_cursor(),
            boxes: pane.marks_with_boxes(),
        }
    }

    /// Show `s`; the lists reload only what changed, so selection and scroll survive.
    pub fn show(&self, s: PaneShown) {
        let (ui, sf) = (&self.ui, self.surface);
        let skip = s.rows.len() - s.lines.len();
        let (lines_changed, folders_changed, rsrc_changed) = {
            let st = self.state.borrow();
            (
                st.lines != s.lines || st.boxes != s.boxes,
                st.folders != s.folders,
                st.rsrc != s.rsrc,
            )
        };
        ui.show(sf, self.tree, s.folder_tree);
        if rsrc_changed {
            ui.hide_column(sf, self.list, RSRC, !s.rsrc);
        }
        if lines_changed {
            let rows: Vec<RowLine> = s
                .lines
                .iter()
                .map(|l| RowLine {
                    cells: l.cells.to_vec(),
                    depth: l.depth,
                    icon: match l.is_dir {
                        true => RowIcon::Folder {
                            expandable: l.expandable,
                            expanded: l.expanded,
                        },
                        false => RowIcon::File {
                            expandable: l.expandable,
                            expanded: l.expanded,
                        },
                    },
                    tick: s.boxes.then_some(l.marked),
                })
                .collect();
            ui.set_row_lines(sf, self.list, &rows);
        }
        if folders_changed {
            let rows: Vec<RowLine> = s
                .folders
                .iter()
                .map(|(depth, name, open)| RowLine {
                    cells: vec![name.clone()],
                    depth: *depth,
                    icon: RowIcon::TreeFolder { open: *open },
                    tick: None,
                })
                .collect();
            ui.set_row_lines(sf, self.tree, &rows);
        }
        if s.folder_tree {
            ui.select_rows(sf, self.tree, &[s.folder_cursor], None);
        }
        // A tick-box pane shows its cursor as the selection; a click-marking one its marks.
        let want: Vec<usize> = if s.boxes {
            s.cursor.into_iter().collect()
        } else {
            s.lines
                .iter()
                .enumerate()
                .filter(|(_, l)| l.marked)
                .map(|(i, _)| i)
                .collect()
        };
        let scroll = s
            .cursor
            .filter(|i| self.shown_cursor.replace(Some(*i)) != Some(*i));
        ui.select_rows(sf, self.list, &want, scroll);
        let mut st = self.state.borrow_mut();
        st.rows = s.rows;
        st.skip = skip;
        st.boxes = s.boxes;
        st.rsrc = s.rsrc;
        st.lines = s.lines;
        st.folders = s.folders;
    }

    /// The listing row at list position `i` (a right-click names its row by position).
    pub fn row_at(&self, i: usize) -> Option<DisplayRow> {
        let s = self.state.borrow();
        s.rows.get(i + s.skip).cloned()
    }

    pub fn focus(&self) {
        self.ui.focus(self.surface, self.list);
    }

    /// The list's control on its form, for menus shown at a point in it.
    pub fn list_ctl(&self) -> Ctl {
        self.list
    }

    /// The rows one page of the list shows, for Page Up and Page Down.
    pub fn page_rows(&self) -> i32 {
        self.ui.page_rows(self.surface, self.list)
    }
}

fn list_event(s: &State, e: RowsEvent) -> Option<PaneEvent> {
    match e {
        RowsEvent::Select { row, cmd, shift } => {
            s.rows
                .get(row + s.skip)
                .cloned()
                .map(|row| PaneEvent::Click {
                    row,
                    ctrl: cmd,
                    shift,
                })
        }
        RowsEvent::Tick(row) => s.lines.get(row).map(|l| PaneEvent::Mark(l.node_id.clone())),
        RowsEvent::Icon(row) => s
            .lines
            .get(row)
            .filter(|l| l.expandable)
            .map(|l| PaneEvent::Toggle(l.node_id.clone())),
        RowsEvent::Activate => Some(PaneEvent::Activate),
        RowsEvent::Key(k, m) => Some(PaneEvent::Key(k, m)),
        RowsEvent::Header(col) => COLUMNS.get(col).and_then(|c| c.3).map(PaneEvent::Sort),
        RowsEvent::Menu { row, x, y } => Some(PaneEvent::Menu(row, x, y)),
        RowsEvent::Focused => Some(PaneEvent::Focused),
    }
}

/// Hand `event` to the pane's owner, outside the state's borrow (it reads the pane back).
fn send(state: &Rc<RefCell<State>>, event: Option<PaneEvent>) {
    let f = state.borrow().events.clone();
    if let (Some(f), Some(e)) = (f, event) {
        f(e);
    }
}
