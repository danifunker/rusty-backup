//! A list as a view-based `NSTableView` over rows of text the screen hands it, with the
//! selection reported back; the rows are the controller's, kept here only for drawing.

use std::cell::RefCell;
use std::rc::Rc;

use objc2::rc::Retained;
use objc2::runtime::{NSObject, NSObjectProtocol, ProtocolObject};
use objc2::{define_class, msg_send, DefinedClass, MainThreadMarker, MainThreadOnly};
use objc2_app_kit::{
    NSAccessibility, NSBorderType, NSButton, NSControlStateValueOff, NSControlStateValueOn,
    NSControlTextEditingDelegate, NSScrollView, NSTableColumn, NSTableView, NSTableViewDataSource,
    NSTableViewDelegate, NSTextAlignment, NSView,
};
use objc2_foundation::{NSIndexSet, NSNotification, NSRect};

use crate::ui::{self, ns};

/// The tick column's identifier; the text columns are numbered.
const CHECK: &str = "check";

pub struct State {
    rows: Vec<Vec<String>>,
    right: Vec<bool>,
    /// Each row's tick, when the table has a tick column, and the target its boxes act on.
    checks: Vec<bool>,
    check_target: Option<Retained<ui::Callback>>,
    on_select: Option<Rc<dyn Fn(Option<usize>)>>,
    /// The screen is setting the selection itself, so the change is not reported back.
    quiet: bool,
}

define_class!(
    #[unsafe(super(NSObject))]
    #[thread_kind = MainThreadOnly]
    #[name = "RBTableSource"]
    #[ivars = RefCell<State>]
    pub struct Source;

    unsafe impl NSObjectProtocol for Source {}
    unsafe impl NSControlTextEditingDelegate for Source {}

    unsafe impl NSTableViewDataSource for Source {
        #[unsafe(method(numberOfRowsInTableView:))]
        fn count(&self, _table: &NSTableView) -> isize {
            self.ivars().borrow().rows.len() as isize
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
            cell_view(self.mtm(), &self.ivars().borrow(), column, row)
        }

        #[unsafe(method(tableViewSelectionDidChange:))]
        fn selection_changed(&self, note: &NSNotification) {
            let Some(table) = note.object().and_then(|o| o.downcast::<NSTableView>().ok()) else {
                return;
            };
            let row = table.selectedRow();
            let picked = (row >= 0).then_some(row as usize);
            // The borrow ends before the screen runs: it may set the selection again.
            let f = {
                let state = self.ivars().borrow();
                if state.quiet {
                    return;
                }
                state.on_select.clone()
            };
            if let Some(f) = f {
                f(picked);
            }
        }
    }
);

/// One cell: a tick box in the tick column, else the row's text for that column.
fn cell_view(
    mtm: MainThreadMarker,
    state: &State,
    column: Option<&NSTableColumn>,
    row: isize,
) -> Option<Retained<NSView>> {
    let ident = column.map(|c| c.identifier().to_string());
    if ident.as_deref() == Some(CHECK) {
        let b = ui::checkbox(mtm, "", "");
        let on = state.checks.get(row as usize).copied().unwrap_or(false);
        b.setState(if on {
            NSControlStateValueOn
        } else {
            NSControlStateValueOff
        });
        b.setTag(row);
        b.setAccessibilityLabel(Some(&ns(&format!("Select row {}", row + 1))));
        if let Some(t) = &state.check_target {
            ui::hook(&b, t);
        }
        return Some(Retained::into_super(Retained::into_super(b)));
    }
    let col = ident.and_then(|i| i.parse::<usize>().ok()).unwrap_or(0);
    let text = state
        .rows
        .get(row as usize)
        .and_then(|r| r.get(col))
        .cloned()
        .unwrap_or_default();
    let label = ui::label(mtm, &text);
    if state.right.get(col).copied().unwrap_or(false) {
        label.setAlignment(NSTextAlignment::Right);
    }
    Some(Retained::into_super(Retained::into_super(label)))
}

/// A table in its scroll view; the screen keeps both.
pub struct Table {
    pub scroll: Retained<NSScrollView>,
    pub view: Retained<NSTableView>,
    source: Retained<Source>,
}

impl Table {
    /// `columns` are `(title, width in points, right-aligned)`; `id` is the controller id.
    pub fn new(mtm: MainThreadMarker, id: &str, columns: &[(&str, f64, bool)]) -> Table {
        let view = NSTableView::initWithFrame(mtm.alloc(), NSRect::ZERO);
        for (i, (title, width, _)) in columns.iter().enumerate() {
            let col = NSTableColumn::initWithIdentifier(mtm.alloc(), &ns(&i.to_string()));
            col.setTitle(&ns(title));
            col.setWidth(*width);
            view.addTableColumn(&col);
        }
        view.setUsesAlternatingRowBackgroundColors(true);
        view.setAllowsMultipleSelection(false);
        view.setAllowsEmptySelection(true);
        ui::set_id(&view, id);
        let state = State {
            rows: Vec::new(),
            right: columns.iter().map(|c| c.2).collect(),
            checks: Vec::new(),
            check_target: None,
            on_select: None,
            quiet: false,
        };
        let source: Retained<Source> = {
            let this = mtm.alloc::<Source>().set_ivars(RefCell::new(state));
            // SAFETY: NSObject's plain init.
            unsafe { msg_send![super(this), init] }
        };
        // SAFETY: the table keeps these weakly; `Table` keeps the source alive with it.
        unsafe {
            view.setDataSource(Some(ProtocolObject::from_ref(&*source)));
            view.setDelegate(Some(ProtocolObject::from_ref(&*source)));
        }
        let scroll = NSScrollView::initWithFrame(mtm.alloc(), NSRect::ZERO);
        scroll.setHasVerticalScroller(true);
        scroll.setHasHorizontalScroller(true);
        scroll.setAutohidesScrollers(true);
        scroll.setBorderType(NSBorderType::BezelBorder);
        scroll.setDocumentView(Some(&view));
        Table {
            scroll,
            view,
            source,
        }
    }

    pub fn on_select(&self, f: Rc<dyn Fn(Option<usize>)>) {
        self.source.ivars().borrow_mut().on_select = Some(f);
    }

    /// A first column of tick boxes; a click reports its row and its new state.
    pub fn add_checks(&self, mtm: MainThreadMarker, on: Rc<dyn Fn(usize, bool)>) {
        let col = NSTableColumn::initWithIdentifier(mtm.alloc(), &ns(CHECK));
        col.setTitle(&ns(""));
        col.setWidth(24.0);
        self.view.addTableColumn(&col);
        let last = self.view.numberOfColumns() - 1;
        self.view.moveColumn_toColumn(last, 0);
        let target = ui::callback(
            mtm,
            Box::new(move |sender| {
                if let Some(b) = sender.downcast_ref::<NSButton>() {
                    on(b.tag() as usize, b.state() == NSControlStateValueOn);
                }
            }),
        );
        self.source.ivars().borrow_mut().check_target = Some(target);
    }

    /// Show each row's tick; only a change reloads.
    pub fn set_checks(&self, checks: Vec<bool>) {
        if self.source.ivars().borrow().checks == checks {
            return;
        }
        let selected = self.selected();
        self.source.ivars().borrow_mut().checks = checks;
        self.quietly(|| self.view.reloadData());
        if let Some(i) = selected.filter(|i| *i < self.len()) {
            self.select(i);
        }
    }

    /// Show `rows`; only a change reloads, so the selection and scroll survive a refresh.
    pub fn set_rows(&self, rows: Vec<Vec<String>>) {
        if self.source.ivars().borrow().rows == rows {
            return;
        }
        let selected = self.selected();
        self.source.ivars().borrow_mut().rows = rows;
        self.quietly(|| {
            self.view.reloadData();
        });
        if let Some(i) = selected.filter(|i| *i < self.len()) {
            self.select(i);
        }
    }

    pub fn len(&self) -> usize {
        self.source.ivars().borrow().rows.len()
    }

    pub fn selected(&self) -> Option<usize> {
        let row = self.view.selectedRow();
        (row >= 0).then_some(row as usize)
    }

    /// Select row `i` as the screen's own change: it is not reported back.
    pub fn select(&self, i: usize) {
        if self.selected() == Some(i) {
            return;
        }
        let set = NSIndexSet::indexSetWithIndex(i);
        self.quietly(|| {
            self.view.selectRowIndexes_byExtendingSelection(&set, false);
            self.view.scrollRowToVisible(i as isize);
        });
    }

    fn quietly(&self, f: impl FnOnce()) {
        self.source.ivars().borrow_mut().quiet = true;
        f();
        self.source.ivars().borrow_mut().quiet = false;
    }
}
