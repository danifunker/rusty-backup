//! A list as a view-based `NSTableView` over rows of text the screen hands it, with the
//! selection reported back; the rows are the controller's, kept here only for drawing.

use std::cell::RefCell;
use std::rc::Rc;

use objc2::rc::Retained;
use objc2::runtime::{NSObject, NSObjectProtocol, ProtocolObject};
use objc2::{define_class, msg_send, DefinedClass, MainThreadMarker, MainThreadOnly};
use objc2_app_kit::{
    NSBorderType, NSControlTextEditingDelegate, NSScrollView, NSTableColumn, NSTableView,
    NSTableViewDataSource, NSTableViewDelegate, NSTextAlignment, NSView,
};
use objc2_foundation::{NSIndexSet, NSNotification, NSRect};

use crate::ui::{self, ns};

pub struct State {
    rows: Vec<Vec<String>>,
    right: Vec<bool>,
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
            let state = self.ivars().borrow();
            let col = column
                .map(|c| c.identifier().to_string())
                .and_then(|i| i.parse::<usize>().ok())
                .unwrap_or(0);
            let text = state
                .rows
                .get(row as usize)
                .and_then(|r| r.get(col))
                .cloned()
                .unwrap_or_default();
            let label = ui::label(self.mtm(), &text);
            if state.right.get(col).copied().unwrap_or(false) {
                label.setAlignment(NSTextAlignment::Right);
            }
            Some(Retained::into_super(Retained::into_super(label)))
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
