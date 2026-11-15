//! The pane list behind `Host::rows_view`: a view-based `NSTableView` drawing `RowLine`s with the
//! row icons, tick boxes and indent, reporting what the user does as a `RowsEvent`. The shared
//! `pane_view` turns those into its pane's events; the arrows, paging and Home / End stay here.

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
use objc2_foundation::{NSMutableIndexSet, NSNotification, NSPoint, NSRect, NSSize};
use rusty_backup::controller::keys::{Key, Mods};

use crate::row_icons;
use crate::screens::host::{RowIcon, RowLine, RowsEvent, RowsSpec};
use crate::ui::{self, ns, Callback};

/// The indent of one level inside a folder, in points.
const INDENT: f64 = 16.0;

/// Tags of the controls inside a name cell: the row, and which control.
const TICK: isize = 1 << 24;
const ICON: isize = 2 << 24;

type Events = Option<Rc<dyn Fn(RowsEvent)>>;

pub struct State {
    lines: Vec<RowLine>,
    right: Vec<bool>,
    /// The screen is setting a selection itself, so the change is not reported back.
    quiet: bool,
    events: Events,
    target: Option<Retained<Callback>>,
}

define_class!(
    #[unsafe(super(NSObject))]
    #[thread_kind = MainThreadOnly]
    #[name = "RBRowsSource"]
    #[ivars = RefCell<State>]
    pub struct Source;

    unsafe impl NSObjectProtocol for Source {}
    unsafe impl NSControlTextEditingDelegate for Source {}

    unsafe impl NSTableViewDataSource for Source {
        #[unsafe(method(numberOfRowsInTableView:))]
        fn count(&self, _table: &NSTableView) -> isize {
            self.ivars().borrow().lines.len() as isize
        }
    }

    unsafe impl NSTableViewDelegate for Source {
        #[unsafe(method_id(tableView:viewForTableColumn:row:))]
        fn cell(
            &self,
            _table: &NSTableView,
            column: Option<&NSTableColumn>,
            row: isize,
        ) -> Option<Retained<NSView>> {
            let col = column
                .map(|c| c.identifier().to_string())
                .and_then(|i| i.parse::<usize>().ok())
                .unwrap_or(0);
            Some(self.make_cell(col, row as usize))
        }

        #[unsafe(method(tableViewSelectionDidChange:))]
        fn selection_changed(&self, note: &NSNotification) {
            let Some(table) = note.object().and_then(|o| o.downcast::<NSTableView>().ok()) else {
                return;
            };
            // The row a click or arrow key moved to, with the modifiers held.
            let row = match table.clickedRow() {
                r if r >= 0 => r,
                _ => table.selectedRow(),
            };
            if self.ivars().borrow().quiet || row < 0 {
                return;
            }
            let (cmd, shift) = modifiers();
            self.send(RowsEvent::Select {
                row: row as usize,
                cmd,
                shift,
            });
        }

        #[unsafe(method(tableView:didClickTableColumn:))]
        fn column_clicked(&self, _table: &NSTableView, column: &NSTableColumn) {
            if let Ok(i) = column.identifier().to_string().parse::<usize>() {
                self.send(RowsEvent::Header(i));
            }
        }
    }
);

define_class!(
    /// A table that hands the keys it does not use, and a right-click, to its owner.
    #[unsafe(super(NSTableView))]
    #[thread_kind = MainThreadOnly]
    #[name = "RBRowsTable"]
    #[ivars = RefCell<Events>]
    pub struct Table;

    unsafe impl NSObjectProtocol for Table {}

    impl Table {
        #[unsafe(method(keyDown:))]
        fn key_down(&self, event: &NSEvent) {
            let f = self.ivars().borrow().clone();
            match (key_of(event), f) {
                (Some(key), Some(f)) => f(RowsEvent::Key(key, mods_of(event))),
                // SAFETY: the table's own handling of a key the owner leaves to it.
                _ => unsafe {
                    let _: () = msg_send![super(self), keyDown: event];
                },
            }
        }

        #[unsafe(method(becomeFirstResponder))]
        fn become_first_responder(&self) -> bool {
            // SAFETY: the table's own answer, then the owner hears it has the keys.
            let took: bool = unsafe { msg_send![super(self), becomeFirstResponder] };
            if took {
                if let Some(f) = self.ivars().borrow().clone() {
                    f(RowsEvent::Focused);
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
                f(RowsEvent::Menu {
                    row: row as usize,
                    x: at.x,
                    y: at.y,
                });
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
    fn send(&self, e: RowsEvent) {
        let f = self.ivars().borrow().events.clone();
        if let Some(f) = f {
            f(e);
        }
    }

    fn make_cell(&self, col: usize, row: usize) -> Retained<NSView> {
        let mtm = self.mtm();
        let s = self.ivars().borrow();
        let Some(line) = s.lines.get(row) else {
            return ui::flipped(mtm);
        };
        let text = line.cells.get(col).cloned().unwrap_or_default();
        if col == 0 {
            let target = s.target.clone().expect("the list's target");
            return name_cell(mtm, &target, line, &text, row);
        }
        let label = ui::label(mtm, &text);
        if s.right.get(col).copied().unwrap_or(false) {
            label.setAlignment(NSTextAlignment::Right);
        }
        Retained::into_super(Retained::into_super(label))
    }
}

fn icon_image(icon: RowIcon) -> Option<Retained<objc2_app_kit::NSImage>> {
    match icon {
        RowIcon::None => None,
        RowIcon::File {
            expandable,
            expanded,
        } => row_icons::for_row(false, expandable, expanded),
        RowIcon::Folder {
            expandable,
            expanded,
        } => row_icons::for_row(true, expandable, expanded),
        RowIcon::TreeFolder { open } => row_icons::folder(open),
    }
}

/// A name cell: indented by its depth, a tick box when marking with them, the icon (a button that
/// opens or shuts an expandable row), then the name.
fn name_cell(
    mtm: MainThreadMarker,
    target: &Callback,
    line: &RowLine,
    name: &str,
    row: usize,
) -> Retained<NSView> {
    let cell = ui::flipped(mtm);
    let mut x = 2.0 + INDENT * line.depth as f64;
    if let Some(on) = line.tick {
        // SAFETY: the target is the list's, kept with the table.
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
    if let Some(image) = icon_image(line.icon) {
        // SAFETY: as above.
        let b = unsafe { NSButton::buttonWithImage_target_action(&image, None, None, mtm) };
        b.setBordered(false);
        b.setTag(ICON | row as isize);
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

/// A pane list in its scroll view.
pub struct Rows {
    pub scroll: Retained<NSScrollView>,
    table: Retained<Table>,
    source: Retained<Source>,
    _double: Retained<Callback>,
    shown: Cell<bool>,
}

impl Rows {
    pub fn new(
        mtm: MainThreadMarker,
        id: &str,
        spec: &RowsSpec,
        on: Rc<dyn Fn(RowsEvent)>,
    ) -> Rows {
        let this = mtm
            .alloc::<Table>()
            .set_ivars(RefCell::new(Some(on.clone())));
        // SAFETY: NSTableView's designated initialiser.
        let t: Retained<Table> = unsafe { msg_send![super(this), initWithFrame: NSRect::ZERO] };
        for (i, (title, width, _)) in spec.columns.iter().enumerate() {
            let col = NSTableColumn::initWithIdentifier(mtm.alloc(), &ns(&i.to_string()));
            col.setTitle(&ns(title));
            col.setWidth(*width);
            t.addTableColumn(&col);
        }
        if !spec.header {
            t.setHeaderView(None);
        }
        t.setUsesAlternatingRowBackgroundColors(spec.stripes);
        t.setAllowsMultipleSelection(spec.multiple);
        t.setAllowsEmptySelection(true);
        if !id.is_empty() {
            ui::set_id(&t, id);
        }
        let events = on.clone();
        let target = ui::callback(
            mtm,
            Box::new(move |sender: &AnyObject| {
                let Some(b) = sender.downcast_ref::<NSButton>() else {
                    return;
                };
                let tag = b.tag();
                let row = (tag & 0xff_ffff) as usize;
                match tag & !0xff_ffff {
                    TICK => events(RowsEvent::Tick(row)),
                    ICON => events(RowsEvent::Icon(row)),
                    _ => {}
                }
            }),
        );
        let double = ui::callback(mtm, Box::new(move |_| on(RowsEvent::Activate)));
        let source: Retained<Source> = {
            let state = State {
                lines: Vec::new(),
                right: spec.columns.iter().map(|c| c.2).collect(),
                quiet: false,
                events: t.ivars().borrow().clone(),
                target: Some(target),
            };
            let this = mtm.alloc::<Source>().set_ivars(RefCell::new(state));
            // SAFETY: NSObject's plain init.
            unsafe { msg_send![super(this), init] }
        };
        // SAFETY: the table keeps these weakly; `Rows` keeps them alive with it.
        unsafe {
            t.setDataSource(Some(ProtocolObject::from_ref(&*source)));
            t.setDelegate(Some(ProtocolObject::from_ref(&*source)));
            t.setTarget(Some(&double));
            t.setDoubleAction(Some(sel!(fire:)));
        }
        let scroll = NSScrollView::initWithFrame(mtm.alloc(), NSRect::ZERO);
        scroll.setHasVerticalScroller(true);
        scroll.setHasHorizontalScroller(true);
        scroll.setAutohidesScrollers(true);
        scroll.setBorderType(NSBorderType::BezelBorder);
        scroll.setDocumentView(Some(&t));
        Rows {
            scroll,
            table: t,
            source,
            _double: double,
            shown: Cell::new(false),
        }
    }

    pub fn set_lines(&self, lines: &[RowLine]) {
        if self.shown.get() && self.source.ivars().borrow().lines == lines {
            return;
        }
        self.shown.set(true);
        self.source.ivars().borrow_mut().lines = lines.to_vec();
        self.quietly(|| self.table.reloadData());
    }

    pub fn hide_column(&self, col: usize, hidden: bool) {
        let cols = self.table.tableColumns();
        if col < cols.count() {
            cols.objectAtIndex(col).setHidden(hidden);
        }
    }

    pub fn select(&self, rows: &[usize], visible: Option<usize>) {
        let set = NSMutableIndexSet::new();
        for i in rows {
            set.addIndex(*i);
        }
        self.quietly(|| {
            if !self.table.selectedRowIndexes().isEqualToIndexSet(&set) {
                self.table
                    .selectRowIndexes_byExtendingSelection(&set, false);
            }
            if let Some(i) = visible {
                self.table.scrollRowToVisible(i as isize);
            }
        });
    }

    pub fn focus(&self) {
        if let Some(w) = self.table.window() {
            w.makeFirstResponder(Some(&self.table));
        }
    }

    pub fn page_rows(&self) -> i32 {
        let visible = self.scroll.contentSize().height;
        let row = self.table.rowHeight() + self.table.intercellSpacing().height;
        ((visible / row.max(1.0)) as i32).max(1)
    }

    /// The table, for a menu shown at a point in it.
    pub fn table_view(&self) -> &NSView {
        &self.table
    }

    fn quietly(&self, f: impl FnOnce()) {
        self.source.ivars().borrow_mut().quiet = true;
        f();
        self.source.ivars().borrow_mut().quiet = false;
    }
}
