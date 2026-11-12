//! One browser pane's listing over a `CommanderController` side: the folder tree and the list,
//! each a view-based `NSTableView` with the row icons, tick boxes for marks and the indent of a
//! row inside a folder. The Win32 shell's `pane_view.rs` is the reference; Inspect's browse view
//! has one and Commander two.
//!
//! The view sends nothing itself: it reports what the user did as a [`PaneEvent`], and the screen
//! that owns it turns that into actions on its controller. Keys go through the controller's map:
//! the table hands the ones it does not use itself (arrows move its own selection) to the screen.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use objc2::rc::Retained;
use objc2::runtime::{AnyObject, NSObject, NSObjectProtocol, ProtocolObject};
use objc2::{define_class, msg_send, sel, DefinedClass, MainThreadMarker, MainThreadOnly};
use objc2_app_kit::{
    NSBorderType, NSButton, NSControlStateValueOff, NSControlStateValueOn,
    NSControlTextEditingDelegate, NSEvent, NSEventModifierFlags, NSScrollView, NSTableColumn,
    NSTableView, NSTableViewDataSource, NSTableViewDelegate, NSTextAlignment, NSView,
};
use objc2_foundation::{NSIndexSet, NSMutableIndexSet, NSNotification, NSPoint, NSRect, NSSize};
use rusty_backup::controller::commander::{
    format_size, BrowserPaneAction as P, CommanderController, DisplayRow, ListingStyle, Side,
    SortColumn,
};
use rusty_backup::controller::keys::{Key, Mods};

use crate::row_icons;
use crate::ui::{self, ns, Callback};

/// The listing's columns: title, width in points, right-aligned, the sort they ask for.
const COLUMNS: [(&str, f64, bool, Option<SortColumn>); 5] = [
    ("Name", 280.0, false, Some(SortColumn::Name)),
    ("Data", 90.0, true, Some(SortColumn::Size)),
    ("Rsrc", 80.0, true, None),
    ("Modified", 150.0, false, Some(SortColumn::Modified)),
    ("Type", 90.0, false, Some(SortColumn::Type)),
];

/// The indent of one level inside a folder, in points.
const INDENT: f64 = 16.0;

/// What the user did in the pane, for its screen to act on.
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
    /// A right-click on list position `row`, at a point in the list.
    Menu(usize, NSPoint),
    /// A folder-tree row was picked or its icon clicked.
    Folder(P),
    /// The list took the keyboard focus: its pane is the one keys act on.
    Focused,
}

/// One listing row as the table draws it.
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

/// The tables' shared state; the data source reads it, the screen writes it.
#[derive(Default)]
pub struct State {
    rows: Vec<DisplayRow>,
    lines: Vec<Line>,
    /// Listing positions shift by the `..` rows a tick-box listing leaves out.
    skip: usize,
    folders: Vec<(usize, String, bool)>,
    boxes: bool,
    /// The screen is setting a selection itself, so the change is not reported back.
    quiet: bool,
    events: Option<Rc<dyn Fn(PaneEvent)>>,
    target: Option<Retained<Callback>>,
}

define_class!(
    /// The data source and delegate of both tables (the list's and the folder tree's).
    #[unsafe(super(NSObject))]
    #[thread_kind = MainThreadOnly]
    #[name = "RBPaneSource"]
    #[ivars = RefCell<State>]
    pub struct Source;

    unsafe impl NSObjectProtocol for Source {}
    unsafe impl NSControlTextEditingDelegate for Source {}

    unsafe impl NSTableViewDataSource for Source {
        #[unsafe(method(numberOfRowsInTableView:))]
        fn count(&self, table: &NSTableView) -> isize {
            let s = self.ivars().borrow();
            match is_tree(table) {
                true => s.folders.len() as isize,
                false => s.lines.len() as isize,
            }
        }
    }

    unsafe impl NSTableViewDelegate for Source {
        #[unsafe(method_id(tableView:viewForTableColumn:row:))]
        fn cell(
            &self,
            table: &NSTableView,
            column: Option<&NSTableColumn>,
            row: isize,
        ) -> Option<Retained<NSView>> {
            let col = column
                .map(|c| c.identifier().to_string())
                .and_then(|i| i.parse::<usize>().ok())
                .unwrap_or(0);
            Some(self.make_cell(is_tree(table), col, row as usize))
        }

        #[unsafe(method(tableViewSelectionDidChange:))]
        fn selection_changed(&self, note: &NSNotification) {
            let Some(table) = note.object().and_then(|o| o.downcast::<NSTableView>().ok()) else {
                return;
            };
            // The row a click or arrow key moved to; a marking pane reports it with its modifiers.
            let row = match table.clickedRow() {
                r if r >= 0 => r,
                _ => table.selectedRow(),
            };
            let (ctrl, shift) = modifiers();
            let event = {
                let s = self.ivars().borrow();
                if s.quiet || row < 0 {
                    return;
                }
                let i = row as usize;
                if is_tree(&table) {
                    Some(PaneEvent::Folder(P::FolderTreeClick(i)))
                } else {
                    s.rows.get(i + s.skip).cloned().map(|row| PaneEvent::Click { row, ctrl, shift })
                }
            };
            if let Some(e) = event {
                self.send(e);
            }
        }

        #[unsafe(method(tableView:didClickTableColumn:))]
        fn column_clicked(&self, table: &NSTableView, column: &NSTableColumn) {
            if is_tree(table) {
                return;
            }
            let sort = column
                .identifier()
                .to_string()
                .parse::<usize>()
                .ok()
                .and_then(|i| COLUMNS.get(i))
                .and_then(|c| c.3);
            if let Some(sort) = sort {
                self.send(PaneEvent::Sort(sort));
            }
        }
    }
);

define_class!(
    /// A table that hands the keys it does not use, and a right-click, to its pane.
    #[unsafe(super(NSTableView))]
    #[thread_kind = MainThreadOnly]
    #[name = "RBPaneTable"]
    #[ivars = RefCell<Option<Rc<dyn Fn(PaneEvent)>>>]
    pub struct Table;

    unsafe impl NSObjectProtocol for Table {}

    impl Table {
        #[unsafe(method(keyDown:))]
        fn key_down(&self, event: &NSEvent) {
            let f = self.ivars().borrow().clone();
            match (key_of(event), f) {
                (Some(key), Some(f)) => f(PaneEvent::Key(key, mods_of(event))),
                // SAFETY: the table's own handling of a key the pane leaves to it.
                _ => unsafe {
                    let _: () = msg_send![super(self), keyDown: event];
                },
            }
        }

        #[unsafe(method(becomeFirstResponder))]
        fn become_first_responder(&self) -> bool {
            // SAFETY: the table's own answer, then the pane hears it has the keys.
            let took: bool = unsafe { msg_send![super(self), becomeFirstResponder] };
            if took {
                if let Some(f) = self.ivars().borrow().clone() {
                    f(PaneEvent::Focused);
                }
            }
            took
        }

        #[unsafe(method(rightMouseDown:))]
        fn right_mouse_down(&self, event: &NSEvent) {
            let at = self.convertPoint_fromView(event.locationInWindow(), None);
            let row = self.rowAtPoint(at);
            let f = self.ivars().borrow().clone();
            if let (Some(f), true) = (f, row >= 0) {
                f(PaneEvent::Menu(row as usize, at));
            }
        }
    }
);

/// Cmd (the Mac's add-to-selection key, as Ctrl elsewhere) and Shift held on the current event.
fn modifiers() -> (bool, bool) {
    let Some(mtm) = MainThreadMarker::new() else {
        return (false, false);
    };
    let app = objc2_app_kit::NSApplication::sharedApplication(mtm);
    let Some(e) = app.currentEvent() else {
        return (false, false);
    };
    let f = e.modifierFlags();
    (
        f.contains(NSEventModifierFlags::Command),
        f.contains(NSEventModifierFlags::Shift),
    )
}

fn is_tree(table: &NSTableView) -> bool {
    table.tag() == 1
}

/// The controller's name for a key the table hands over; `None` for one it keeps.
pub fn key_of(event: &NSEvent) -> Option<Key> {
    Some(match event.keyCode() {
        36 | 76 => Key::Enter,
        53 => Key::Escape,
        51 => Key::Backspace,
        117 => Key::Delete,
        114 => Key::Insert,
        48 => Key::Tab,
        49 => Key::Space,
        123 => Key::Left,
        124 => Key::Right,
        122 => Key::F(1),
        120 => Key::F(2),
        99 => Key::F(3),
        118 => Key::F(4),
        96 => Key::F(5),
        97 => Key::F(6),
        98 => Key::F(7),
        100 => Key::F(8),
        101 => Key::F(9),
        109 => Key::F(10),
        103 => Key::F(11),
        111 => Key::F(12),
        // Arrows, Page Up / Down, Home and End move the table's own selection.
        125 | 126 | 116 | 121 | 115 | 119 => return None,
        _ => {
            let chars = event.charactersIgnoringModifiers()?.to_string();
            let c = chars.chars().next().filter(|c| c.is_ascii_alphanumeric())?;
            Key::Char(c.to_ascii_lowercase())
        }
    })
}

pub fn mods_of(event: &NSEvent) -> Mods {
    let f = event.modifierFlags();
    Mods {
        ctrl: f.contains(NSEventModifierFlags::Control),
        alt: f.contains(NSEventModifierFlags::Option),
        shift: f.contains(NSEventModifierFlags::Shift),
        cmd: f.contains(NSEventModifierFlags::Command),
    }
}

impl Source {
    fn send(&self, e: PaneEvent) {
        let f = self.ivars().borrow().events.clone();
        if let Some(f) = f {
            f(e);
        }
    }

    fn make_cell(&self, tree: bool, col: usize, row: usize) -> Retained<NSView> {
        let mtm = self.mtm();
        let s = self.ivars().borrow();
        let target = s.target.clone().expect("the pane's target");
        if tree {
            let (depth, name, open) = s.folders.get(row).cloned().unwrap_or_default();
            let name = if depth == 0 { "/".to_string() } else { name };
            return name_cell(
                mtm,
                &target,
                depth,
                row_icons::folder(open),
                &name,
                None,
                row,
                true,
            );
        }
        let Some(line) = s.lines.get(row) else {
            return ui::flipped(mtm);
        };
        if col == 0 {
            let image = row_icons::for_row(line.is_dir, line.expandable, line.expanded);
            let tick = s.boxes.then_some(line.marked);
            return name_cell(
                mtm,
                &target,
                line.depth,
                image,
                &line.cells[0],
                tick,
                row,
                false,
            );
        }
        let label = ui::label(mtm, &line.cells[col]);
        if COLUMNS[col].2 {
            label.setAlignment(NSTextAlignment::Right);
        }
        Retained::into_super(Retained::into_super(label))
    }
}

/// Tags of the controls inside a name cell: the row, and which control.
const TICK: isize = 1 << 24;
const ICON: isize = 2 << 24;
const FOLDER_ICON: isize = 3 << 24;

/// A name cell: indented by `depth`, a tick box when marking with them, the icon (a button that
/// opens or shuts an expandable row), then the name.
#[allow(clippy::too_many_arguments)]
fn name_cell(
    mtm: MainThreadMarker,
    target: &Callback,
    depth: usize,
    image: Option<Retained<objc2_app_kit::NSImage>>,
    name: &str,
    tick: Option<bool>,
    row: usize,
    tree: bool,
) -> Retained<NSView> {
    let cell = ui::flipped(mtm);
    let mut x = 2.0 + INDENT * depth as f64;
    if let Some(on) = tick {
        // SAFETY: the target is the pane's, kept with the tables.
        let b = unsafe { NSButton::checkboxWithTitle_target_action(&ns(""), None, None, mtm) };
        b.setState(if on {
            NSControlStateValueOn
        } else {
            NSControlStateValueOff
        });
        b.setTag(TICK | row as isize);
        ui::hook(&b, target);
        b.setFrame(NSRect::new(NSPoint::new(x, 0.0), NSSize::new(18.0, 18.0)));
        cell.addSubview(&b);
        x += 20.0;
    }
    if let Some(image) = image {
        // SAFETY: as above.
        let b = unsafe { NSButton::buttonWithImage_target_action(&image, None, None, mtm) };
        b.setBordered(false);
        b.setTag(if tree { FOLDER_ICON } else { ICON } | row as isize);
        ui::hook(&b, target);
        b.setFrame(NSRect::new(NSPoint::new(x, 0.0), NSSize::new(18.0, 18.0)));
        cell.addSubview(&b);
        x += 20.0;
    }
    let label = ui::label(mtm, name);
    label.setFrame(NSRect::new(NSPoint::new(x, 1.0), NSSize::new(2000.0, 17.0)));
    cell.addSubview(&label);
    cell
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
    pub list_scroll: Retained<NSScrollView>,
    pub tree_scroll: Retained<NSScrollView>,
    list: Retained<Table>,
    tree: Retained<Table>,
    source: Retained<Source>,
    shown_cursor: Cell<Option<usize>>,
}

fn table(mtm: MainThreadMarker, id: &str, tree: bool) -> (Retained<NSScrollView>, Retained<Table>) {
    let this = mtm.alloc::<Table>().set_ivars(RefCell::new(None));
    // SAFETY: NSTableView's designated initialiser.
    let t: Retained<Table> = unsafe { msg_send![super(this), initWithFrame: NSRect::ZERO] };
    t.setTag(if tree { 1 } else { 0 });
    if tree {
        let col = NSTableColumn::initWithIdentifier(mtm.alloc(), &ns("0"));
        col.setWidth(600.0);
        t.addTableColumn(&col);
        t.setHeaderView(None);
    } else {
        for (i, (title, width, _, _)) in COLUMNS.iter().enumerate() {
            let col = NSTableColumn::initWithIdentifier(mtm.alloc(), &ns(&i.to_string()));
            col.setTitle(&ns(title));
            col.setWidth(*width);
            t.addTableColumn(&col);
        }
        t.setUsesAlternatingRowBackgroundColors(true);
    }
    t.setAllowsMultipleSelection(!tree);
    t.setAllowsEmptySelection(true);
    ui::set_id(&t, id);
    let scroll = NSScrollView::initWithFrame(mtm.alloc(), NSRect::ZERO);
    scroll.setHasVerticalScroller(true);
    scroll.setHasHorizontalScroller(true);
    scroll.setAutohidesScrollers(true);
    scroll.setBorderType(NSBorderType::BezelBorder);
    scroll.setDocumentView(Some(&t));
    (scroll, t)
}

impl PaneView {
    pub fn build(mtm: MainThreadMarker, side: Side, list_id: &str, tree_id: &str) -> PaneView {
        let (list_scroll, list) = table(mtm, list_id, false);
        let (tree_scroll, tree) = table(mtm, tree_id, true);
        let source: Retained<Source> = {
            let this = mtm.alloc::<Source>().set_ivars(RefCell::default());
            // SAFETY: NSObject's plain init.
            unsafe { msg_send![super(this), init] }
        };
        // SAFETY: the tables keep these weakly; `PaneView` keeps the source alive with them.
        unsafe {
            for t in [&list, &tree] {
                t.setDataSource(Some(ProtocolObject::from_ref(&*source)));
                t.setDelegate(Some(ProtocolObject::from_ref(&*source)));
            }
        }
        // Rsrc stays hidden until a listing has resource forks.
        if let Some(c) = list.tableColumns().objectAtIndex(2).into() {
            let c: Retained<NSTableColumn> = c;
            c.setHidden(true);
        }
        PaneView {
            side,
            list_scroll,
            tree_scroll,
            list,
            tree,
            source,
            shown_cursor: Cell::new(None),
        }
    }

    /// Where the pane's user actions go; the tick boxes, icons and double-click report through it.
    pub fn on_event(&self, mtm: MainThreadMarker, f: Rc<dyn Fn(PaneEvent)>) {
        *self.list.ivars().borrow_mut() = Some(f.clone());
        *self.tree.ivars().borrow_mut() = Some(f.clone());
        let source = self.source.clone();
        let events = f.clone();
        let target = ui::callback(
            mtm,
            Box::new(move |sender: &AnyObject| {
                let Some(b) = sender.downcast_ref::<NSButton>() else {
                    return;
                };
                let tag = b.tag();
                let row = (tag & 0xff_ffff) as usize;
                let event = {
                    let s = source.ivars().borrow();
                    match tag & !0xff_ffff {
                        TICK => s.lines.get(row).map(|l| PaneEvent::Mark(l.node_id.clone())),
                        ICON => s
                            .lines
                            .get(row)
                            .filter(|l| l.expandable)
                            .map(|l| PaneEvent::Toggle(l.node_id.clone())),
                        FOLDER_ICON => Some(PaneEvent::Folder(P::FolderTreeToggle(row))),
                        _ => None,
                    }
                };
                if let Some(e) = event {
                    events(e);
                }
            }),
        );
        let double = ui::callback(
            mtm,
            Box::new(move |_| {
                f(PaneEvent::Activate);
            }),
        );
        // SAFETY: the targets are kept in the source's state, with the tables.
        unsafe {
            self.list.setTarget(Some(&double));
            self.list.setDoubleAction(Some(sel!(fire:)));
        }
        let mut s = self.source.ivars().borrow_mut();
        s.events = Some(Rc::new({
            let list = self.list.clone();
            move |e| {
                if let Some(f) = list.ivars().borrow().clone() {
                    f(e);
                }
            }
        }));
        s.target = Some(target);
        // The double-click target lives as long as the source.
        std::mem::forget(double);
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

    /// Show `s`; the tables reload only what changed, so selection and scroll survive.
    pub fn show(&self, s: PaneShown) {
        let skip = s.rows.len() - s.lines.len();
        let (lines_changed, folders_changed) = {
            let st = self.source.ivars().borrow();
            (
                st.lines != s.lines || st.boxes != s.boxes,
                st.folders != s.folders,
            )
        };
        ui::set_visible(&self.tree_scroll, s.folder_tree);
        if let Some(c) = self.list.tableColumns().objectAtIndex(2).into() {
            let c: Retained<NSTableColumn> = c;
            c.setHidden(!s.rsrc);
        }
        {
            let mut st = self.source.ivars().borrow_mut();
            st.rows = s.rows;
            st.skip = skip;
            st.boxes = s.boxes;
            st.lines = s.lines;
            st.folders = s.folders;
        }
        self.quietly(|| {
            if lines_changed {
                self.list.reloadData();
            }
            if folders_changed {
                self.tree.reloadData();
            }
            if s.folder_tree && self.tree.selectedRow() != s.folder_cursor as isize {
                let set = NSIndexSet::indexSetWithIndex(s.folder_cursor);
                self.tree.selectRowIndexes_byExtendingSelection(&set, false);
            }
            // A tick-box pane shows its cursor as the selection; a click-marking one its marks.
            let want: Vec<usize> = if s.boxes {
                s.cursor.into_iter().collect()
            } else {
                let st = self.source.ivars().borrow();
                st.lines
                    .iter()
                    .enumerate()
                    .filter(|(_, l)| l.marked)
                    .map(|(i, _)| i)
                    .collect()
            };
            let set = NSMutableIndexSet::new();
            for i in &want {
                set.addIndex(*i);
            }
            if !self.list.selectedRowIndexes().isEqualToIndexSet(&set) {
                self.list.selectRowIndexes_byExtendingSelection(&set, false);
            }
            if let Some(i) = s.cursor {
                if self.shown_cursor.replace(Some(i)) != Some(i) {
                    self.list.scrollRowToVisible(i as isize);
                }
            }
        });
    }

    /// The listing row at list position `i` (a right-click names its row by position).
    pub fn row_at(&self, i: usize) -> Option<DisplayRow> {
        let s = self.source.ivars().borrow();
        s.rows.get(i + s.skip).cloned()
    }

    pub fn focus(&self) {
        if let Some(w) = self.list.window() {
            w.makeFirstResponder(Some(&self.list));
        }
    }

    pub fn list_view(&self) -> &NSView {
        &self.list
    }

    /// The rows one page of the list shows, for Page Up and Page Down.
    pub fn page_rows(&self) -> i32 {
        let visible = self.list_scroll.contentSize().height;
        let row = self.list.rowHeight() + self.list.intercellSpacing().height;
        ((visible / row.max(1.0)) as i32).max(1)
    }

    fn quietly(&self, f: impl FnOnce()) {
        self.source.ivars().borrow_mut().quiet = true;
        f();
        self.source.ivars().borrow_mut().quiet = false;
    }
}
