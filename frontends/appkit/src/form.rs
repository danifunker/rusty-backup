//! The AppKit [`Host`] for the shared screens' [`Form`] (frontends/mac-screens): each control an
//! objc2 view on a surface's body, its tag its control number, its action the surface's target.
//!
//! What stays here is AppKit's own: a field's cell that reports a value set through accessibility
//! (typing reports through the delegate), and a guard so reading a field, which commits an edit in
//! progress, reports nothing.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::Rc;

use objc2::rc::Retained;
use objc2::runtime::NSObjectProtocol;
use objc2::runtime::{AnyObject, ProtocolObject};
use objc2::{define_class, msg_send, DefinedClass, MainThreadMarker, MainThreadOnly, Message};
use objc2_app_kit::{
    NSAccessibility, NSApplication, NSButton, NSColor, NSControl, NSControlStateValueOff,
    NSControlStateValueOn, NSLineBreakMode, NSMenuItem, NSPopUpButton, NSProgressIndicator,
    NSScrollView, NSSecureTextField, NSTextField, NSTextFieldCell, NSTextView, NSView, NSWindow,
    NSWorkspace,
};
use objc2_foundation::{NSPoint, NSRange, NSRect, NSSize, NSURL};
use rusty_backup::controller::commander::Tone;
use rusty_backup::controller::log::LogLevel;
use rusty_backup::controller::{ChoiceSpec, ConfirmSpec, PasswordSpec, PickSpec, TextSpec};

use crate::canvas::Canvas;
use crate::layout;
use crate::rows::Rows;
use crate::screens::host::{
    Column, Ctl, Ev, Host, MenuEntry, Painter, PullItem, RowLine, RowsEvent, RowsSpec, Surface,
};
use crate::table::Table;
use crate::ui::{self, ns, Callback, CloseDelegate, TextChange};

/// A control's colour for a controller's tone.
pub fn tone_color(t: Option<Tone>) -> Retained<NSColor> {
    match t {
        Some(Tone::Warning) => NSColor::systemOrangeColor(),
        Some(Tone::Error) => NSColor::systemRedColor(),
        Some(Tone::Success) => NSColor::systemGreenColor(),
        Some(Tone::Plain) | None => NSColor::labelColor(),
    }
}

define_class!(
    /// A field's cell that reports a value set from outside (accessibility), as typing reports.
    #[unsafe(super(NSTextFieldCell))]
    #[thread_kind = MainThreadOnly]
    #[name = "RBFieldCell"]
    #[ivars = RefCell<Option<Rc<dyn Fn()>>>]
    pub struct FieldCell;

    unsafe impl NSObjectProtocol for FieldCell {}

    impl FieldCell {
        #[unsafe(method(setObjectValue:))]
        fn set_object_value(&self, value: Option<&AnyObject>) {
            // SAFETY: the cell's own setter; the form hears of it after (its sync guard drops its own sets).
            unsafe {
                let _: () = msg_send![super(self), setObjectValue: value];
            }
            let f = self.ivars().borrow().clone();
            if let Some(f) = f.filter(|_| !READING.with(|r| r.get())) {
                f();
            }
        }
    }
);

thread_local! {
    /// A field's text is being read: the commit that reading makes reports nothing.
    static READING: Cell<bool> = const { Cell::new(false) };
    static HOST: RefCell<Option<Rc<AppKitHost>>> = const { RefCell::new(None) };
    /// Work `after` queued, run in the order its delays end.
    static TIMED: RefCell<Vec<Timed>> = const { RefCell::new(Vec::new()) };
}

/// Work for when a delay ends.
type Timed = (std::time::Instant, Box<dyn FnOnce()>);

/// A delay ended: run the item due first.
fn run_timed() {
    let next = TIMED.with(|t| {
        let mut t = t.borrow_mut();
        let at = (0..t.len()).min_by_key(|i| t[*i].0)?;
        Some(t.remove(at).1)
    });
    if let Some(f) = next {
        f();
    }
}

/// A list's last tick-box change, as `toggled` answers it.
type LastTick = Rc<Cell<Option<(usize, bool)>>>;

/// Where a surface's controls report; set by `bind`, shared with its target and delegates.
type Slot = Rc<RefCell<Option<Rc<dyn Fn(Ctl, Ev)>>>>;

fn fire(slot: &Slot, c: Ctl, ev: Ev) {
    // The handler is taken out of the borrow first: it calls back into the host.
    let on = slot.borrow().clone();
    if let Some(on) = on {
        on(c, ev);
    }
}

struct Rec {
    body: Retained<NSView>,
    views: Vec<(Ctl, Retained<NSView>)>,
    /// Each radio group's layer over the body.
    layers: Vec<(usize, Retained<NSView>)>,
    target: Retained<Callback>,
    delegates: Vec<(Ctl, Retained<TextChange>)>,
    slot: Slot,
    /// The window whose content this surface is, when it is one.
    window: Option<(Retained<NSWindow>, Retained<CloseDelegate>)>,
    tables: Vec<(Ctl, Table, LastTick)>,
    canvases: Vec<(Ctl, Retained<Canvas>)>,
    rows: Vec<(Ctl, Rc<Rows>)>,
    /// Each text view and the text it shows.
    texts: Vec<(Ctl, Retained<NSTextView>, String)>,
}

pub struct AppKitHost {
    mtm: MainThreadMarker,
    /// Each surface by its number; `None` once released (numbers are not reused).
    surfaces: RefCell<Vec<Option<Rec>>>,
    /// The status line under the screens, once the app window has one.
    status: RefCell<Option<Retained<NSTextField>>>,
}

impl AppKitHost {
    /// The app's one host.
    pub fn get(mtm: MainThreadMarker) -> Rc<AppKitHost> {
        HOST.with(|h| {
            h.borrow_mut()
                .get_or_insert_with(|| {
                    Rc::new(AppKitHost {
                        mtm,
                        surfaces: RefCell::new(Vec::new()),
                        status: RefCell::new(None),
                    })
                })
                .clone()
        })
    }

    /// The app's one host, once `get` has made it.
    pub fn current() -> Rc<AppKitHost> {
        HOST.with(|h| h.borrow().clone())
            .expect("the host is made at launch")
    }

    /// Where `Effect::Message` text goes.
    pub fn set_status_view(&self, field: Retained<NSTextField>) {
        *self.status.borrow_mut() = Some(field);
    }

    /// The window of surface `s`, when it is one.
    pub fn ns_window(&self, s: Surface) -> Option<Retained<NSWindow>> {
        self.with(s, |r| r.window.as_ref().map(|w| w.0.clone()))
    }

    /// A new surface on `body`.
    pub fn adopt(&self, body: Retained<NSView>) -> Surface {
        let slot: Slot = Rc::new(RefCell::new(None));
        let fired = slot.clone();
        let target = ui::callback(
            self.mtm,
            Box::new(move |sender: &AnyObject| {
                let tag = sender
                    .downcast_ref::<NSControl>()
                    .map(|c| c.tag())
                    .or_else(|| sender.downcast_ref::<NSMenuItem>().map(|m| m.tag()));
                if let Some(t) = tag.filter(|t| *t >= 0) {
                    fire(&fired, t as Ctl, Ev::Act);
                }
            }),
        );
        let mut all = self.surfaces.borrow_mut();
        all.push(Some(Rec {
            body,
            views: Vec::new(),
            layers: Vec::new(),
            target,
            delegates: Vec::new(),
            slot,
            window: None,
            tables: Vec::new(),
            canvases: Vec::new(),
            rows: Vec::new(),
            texts: Vec::new(),
        }));
        (all.len() - 1) as Surface
    }

    fn with<R>(&self, s: Surface, f: impl FnOnce(&mut Rec) -> R) -> R {
        let mut all = self.surfaces.borrow_mut();
        let rec = all
            .get_mut(s as usize)
            .and_then(|r| r.as_mut())
            .expect("a live surface");
        f(rec)
    }

    pub fn body(&self, s: Surface) -> Retained<NSView> {
        self.with(s, |r| r.body.clone())
    }

    /// The view of control `c`.
    pub fn view(&self, s: Surface, c: Ctl) -> Retained<NSView> {
        self.with(s, |r| {
            r.views
                .iter()
                .find(|(x, _)| *x == c)
                .map(|(_, v)| v.clone())
                .expect("every control is built")
        })
    }

    /// Put `view` on surface `s` as control `c`; a control gets its tag and, unless a label, the action.
    pub fn adopt_view(&self, s: Surface, c: Ctl, view: &NSView) {
        let (body, target) = self.with(s, |r| (r.body.clone(), r.target.clone()));
        if let Some(ctl) = view.downcast_ref::<NSControl>() {
            ctl.setTag(c as isize);
            let label = ctl
                .downcast_ref::<NSTextField>()
                .is_some_and(|t| !t.isEditable());
            if !label {
                ui::hook(ctl, &target);
            }
        }
        body.addSubview(view);
        self.with(s, |r| {
            r.views.retain(|(x, _)| *x != c);
            r.views.push((c, view.retain()));
        });
    }

    fn button_of(&self, s: Surface, c: Ctl) -> Retained<NSButton> {
        self.view(s, c).downcast::<NSButton>().expect("a button")
    }

    fn popup_of(&self, s: Surface, c: Ctl) -> Retained<NSPopUpButton> {
        self.view(s, c)
            .downcast::<NSPopUpButton>()
            .expect("a pop-up")
    }

    fn text_of(&self, s: Surface, c: Ctl) -> Retained<NSTextField> {
        self.view(s, c)
            .downcast::<NSTextField>()
            .expect("a text field")
    }

    /// Run `f` on list `c`, outside the surfaces' borrow (a reload reports back into the host).
    fn rows_of(&self, s: Surface, c: Ctl) -> Rc<Rows> {
        self.with(s, |r| r.rows.iter().find(|x| x.0 == c).map(|x| x.1.clone()))
            .expect("a pane list")
    }

    fn on_table<R>(&self, s: Surface, c: Ctl, f: impl FnOnce(&Table) -> R) -> R {
        let found = self.with(s, |r| {
            let at = r.tables.iter().position(|t| t.0 == c);
            at.map(|i| r.tables.remove(i))
        });
        let entry = found.expect("a list");
        let out = f(&entry.1);
        self.with(s, |r| r.tables.push(entry));
        out
    }
}

impl Host for AppKitHost {
    fn bind(&self, s: Surface, on: Rc<dyn Fn(Ctl, Ev)>) {
        let slot = self.with(s, |r| r.slot.clone());
        *slot.borrow_mut() = Some(on);
    }

    fn release(&self, s: Surface) {
        let gone = self
            .surfaces
            .borrow_mut()
            .get_mut(s as usize)
            .and_then(|r| r.take());
        // Dropped outside the borrow: a view's teardown can reach the host.
        if let Some(rec) = gone {
            *rec.slot.borrow_mut() = None;
            if let Some((w, _)) = &rec.window {
                w.setDelegate(None);
                w.orderOut(None);
            }
            drop(rec);
        }
    }

    fn window(&self, title: &str, w: i32, h: i32, on_close: Box<dyn Fn()>) -> Surface {
        let (window, delegate) = ui::window(self.mtm, title, f64::from(w), f64::from(h), on_close);
        let body = window.contentView().expect("the window's content view");
        let s = self.adopt(body);
        self.with(s, |r| r.window = Some((window, delegate)));
        s
    }

    fn on_resize(&self, s: Surface, f: Box<dyn Fn()>) {
        if let Some(d) = self.with(s, |r| r.window.as_ref().map(|w| w.1.clone())) {
            d.on_resize(f);
        }
    }

    fn present(&self, s: Surface) {
        if let Some(w) = self.ns_window(s) {
            ui::present(&w);
        }
    }

    fn hide(&self, s: Surface) {
        if let Some(w) = self.ns_window(s) {
            w.orderOut(None);
        }
    }

    fn area(&self, s: Surface) -> layout::Rect {
        ui::area(&self.body(s))
    }

    fn run_modal(&self, s: Surface) {
        if let Some(w) = self.ns_window(s) {
            NSApplication::sharedApplication(self.mtm).runModalForWindow(&w);
        }
    }

    fn stop_modal(&self) {
        NSApplication::sharedApplication(self.mtm).stopModal();
    }

    fn button(&self, s: Surface, c: Ctl, title: &str, id: &str) {
        self.adopt_view(s, c, &ui::button(self.mtm, title, id));
    }

    fn check(&self, s: Surface, c: Ctl, title: &str, id: &str) {
        self.adopt_view(s, c, &ui::checkbox(self.mtm, title, id));
    }

    fn radio(&self, s: Surface, c: Ctl, group: usize, title: &str, id: &str) {
        // SAFETY: no target yet; it is pointed at the surface's own below.
        let b = unsafe {
            NSButton::radioButtonWithTitle_target_action(&ns(title), None, None, self.mtm)
        };
        if !id.is_empty() {
            ui::set_id(&b, id);
        }
        b.setTag(c as isize);
        let (body, target, layer) = self.with(s, |r| {
            (
                r.body.clone(),
                r.target.clone(),
                r.layers
                    .iter()
                    .find(|(g, _)| *g == group)
                    .map(|(_, l)| l.clone()),
            )
        });
        ui::hook(&b, &target);
        let layer = match layer {
            Some(l) => l,
            None => {
                let l = ui::radio_layer(self.mtm, &body);
                self.with(s, |r| r.layers.push((group, l.clone())));
                l
            }
        };
        layer.addSubview(&b);
        self.with(s, |r| {
            r.views.push((c, b.retain().into_super().into_super()))
        });
    }

    fn label(&self, s: Surface, c: Ctl, text: &str) {
        self.adopt_view(s, c, &ui::label(self.mtm, text));
    }

    fn wrap(&self, s: Surface, c: Ctl, text: &str) {
        let l = NSTextField::wrappingLabelWithString(&ns(text), self.mtm);
        l.setLineBreakMode(NSLineBreakMode::ByWordWrapping);
        self.adopt_view(s, c, &l);
    }

    fn popup(&self, s: Surface, c: Ctl, items: &[String], id: &str) {
        let p = NSPopUpButton::initWithFrame_pullsDown(self.mtm.alloc(), NSRect::ZERO, false);
        for i in items {
            p.addItemWithTitle(&ns(i));
        }
        if !id.is_empty() {
            ui::set_id(&p, id);
        }
        self.adopt_view(s, c, &p);
    }

    fn pulldown(&self, s: Surface, c: Ctl, id: &str) {
        let p = NSPopUpButton::initWithFrame_pullsDown(self.mtm.alloc(), NSRect::ZERO, true);
        ui::set_id(&p, id);
        self.adopt_view(s, c, &p);
    }

    fn set_pulldown(&self, s: Surface, c: Ctl, title: &str, items: &[PullItem]) {
        let p = self.popup_of(s, c);
        let target = self.with(s, |r| r.target.clone());
        p.removeAllItems();
        p.addItemWithTitle(&ns(title));
        for (label, key, on) in items {
            p.addItemWithTitle(&ns(label));
            let item = match p.lastItem() {
                Some(item) => item,
                None => continue,
            };
            match key {
                Some(key) => {
                    item.setTag(*key as isize);
                    // SAFETY: the target is the surface's, kept as long as its controls.
                    unsafe {
                        item.setTarget(Some(&target));
                        item.setAction(Some(objc2::sel!(fire:)));
                    }
                }
                None => item.setEnabled(false),
            }
            if !on {
                item.setEnabled(false);
            }
        }
    }

    fn field(&self, s: Surface, c: Ctl, id: &str, caption: &str, secret: bool) {
        let frame = NSRect::new(NSPoint::new(0.0, 0.0), NSSize::new(200.0, 22.0));
        let slot = self.with(s, |r| r.slot.clone());
        let f: Retained<NSTextField> = if secret {
            Retained::into_super(NSSecureTextField::initWithFrame(self.mtm.alloc(), frame))
        } else {
            let f = NSTextField::initWithFrame(self.mtm.alloc(), frame);
            let set = slot.clone();
            let report: Rc<dyn Fn()> = Rc::new(move || fire(&set, c, Ev::ValueSet));
            let this = self.mtm.alloc::<FieldCell>().set_ivars(RefCell::new(None));
            // SAFETY: NSTextFieldCell's text initialiser.
            let cell: Retained<FieldCell> =
                unsafe { msg_send![super(this), initTextCell: &*ns("")] };
            cell.setEditable(true);
            cell.setBezeled(true);
            cell.setScrollable(true);
            f.setCell(Some(&cell));
            *cell.ivars().borrow_mut() = Some(report);
            f
        };
        if !id.is_empty() {
            ui::set_id(&f, id);
        }
        f.setAccessibilityLabel(Some(&ns(caption)));
        let d = ui::on_change(self.mtm, Box::new(move || fire(&slot, c, Ev::Edited)));
        // SAFETY: the delegate is kept by the surface as long as the field.
        unsafe { f.setDelegate(Some(ProtocolObject::from_ref(&*d))) };
        f.setTag(c as isize);
        let body = self.body(s);
        body.addSubview(&f);
        self.with(s, |r| {
            r.delegates.push((c, d));
            r.views.retain(|(x, _)| *x != c);
            r.views
                .push((c, Retained::into_super(Retained::into_super(f.clone()))));
        });
    }

    fn progress(&self, s: Surface, c: Ctl) {
        self.adopt_view(s, c, &ui::progress(self.mtm));
    }

    fn set_progress(&self, s: Surface, c: Ctl, running: bool, pct: Option<u32>) {
        if let Some(bar) = self.view(s, c).downcast_ref::<NSProgressIndicator>() {
            ui::set_progress(bar, running, pct);
        }
    }

    fn table(&self, s: Surface, c: Ctl, id: &str, columns: &[Column], checks: bool) {
        let cols: Vec<(&str, f64, bool)> = columns
            .iter()
            .map(|(t, w, r)| (t.as_str(), *w, *r))
            .collect();
        let t = Table::new(self.mtm, id, &cols);
        if columns.iter().all(|c| c.0.is_empty()) {
            t.view.setHeaderView(None);
        }
        let slot = self.with(s, |r| r.slot.clone());
        let picked = slot.clone();
        t.on_select(Rc::new(move |_| fire(&picked, c, Ev::Selected)));
        let activated = slot.clone();
        t.on_activate(
            self.mtm,
            Rc::new(move || fire(&activated, c, Ev::Activated)),
        );
        let last: LastTick = Rc::new(Cell::new(None));
        if checks {
            let (seen, toggled) = (last.clone(), slot);
            t.add_checks(
                self.mtm,
                Rc::new(move |row, on| {
                    seen.set(Some((row, on)));
                    fire(&toggled, c, Ev::Toggled);
                }),
            );
        }
        let body = self.body(s);
        body.addSubview(&t.scroll);
        let view: Retained<NSView> = Retained::into_super(t.scroll.clone());
        self.with(s, |r| {
            r.views.retain(|(x, _)| *x != c);
            r.views.push((c, view));
            r.tables.retain(|x| x.0 != c);
            r.tables.push((c, t, last));
        });
    }

    fn set_rows(&self, s: Surface, c: Ctl, rows: &[Vec<String>]) {
        self.on_table(s, c, |t| t.set_rows(rows.to_vec()));
    }

    fn set_checks(&self, s: Surface, c: Ctl, checks: &[bool]) {
        self.on_table(s, c, |t| t.set_checks(checks.to_vec()));
    }

    fn selected_row(&self, s: Surface, c: Ctl) -> Option<usize> {
        self.on_table(s, c, |t| t.selected())
    }

    fn select_row(&self, s: Surface, c: Ctl, i: usize) {
        self.on_table(s, c, |t| t.select(i));
    }

    fn toggled(&self, s: Surface, c: Ctl) -> Option<(usize, bool)> {
        self.with(s, |r| {
            r.tables.iter().find(|t| t.0 == c).and_then(|t| t.2.get())
        })
    }

    fn canvas(&self, s: Surface, c: Ctl, id: &str, painter: Painter) {
        let slot = self.with(s, |r| r.slot.clone());
        let v = Canvas::new(
            self.mtm,
            id,
            painter,
            Box::new(move || fire(&slot, c, Ev::Act)),
        );
        self.adopt_view(s, c, &v);
        self.with(s, |r| {
            r.canvases.retain(|x| x.0 != c);
            r.canvases.push((c, v));
        });
    }

    fn redraw(&self, s: Surface, c: Ctl) {
        self.view(s, c).setNeedsDisplay(true);
    }

    fn clicked(&self, s: Surface, c: Ctl) -> Option<(f64, f64, f64, f64)> {
        let v = self.with(s, |r| {
            r.canvases.iter().find(|x| x.0 == c).map(|x| x.1.clone())
        });
        v.and_then(|v| v.clicked())
    }

    fn text_view(&self, s: Surface, c: Ctl, id: &str) {
        let frame = NSRect::new(NSPoint::new(0.0, 0.0), NSSize::new(400.0, 120.0));
        let scroll = NSScrollView::initWithFrame(self.mtm.alloc(), frame);
        scroll.setHasVerticalScroller(true);
        scroll.setBorderType(objc2_app_kit::NSBorderType::BezelBorder);
        let text = NSTextView::initWithFrame(self.mtm.alloc(), frame);
        text.setEditable(false);
        text.setSelectable(true);
        text.setVerticallyResizable(true);
        text.setHorizontallyResizable(false);
        text.setAutoresizingMask(objc2_app_kit::NSAutoresizingMaskOptions::ViewWidthSizable);
        text.setMaxSize(NSSize::new(f64::MAX, f64::MAX));
        // SAFETY: the text view's own container, read on the main thread.
        if let Some(container) = unsafe { text.textContainer() } {
            container.setWidthTracksTextView(true);
        }
        text.setFont(Some(&crate::app::log_font()));
        if !id.is_empty() {
            ui::set_id(&text, id);
        }
        scroll.setDocumentView(Some(&text));
        self.adopt_view(s, c, &scroll);
        self.with(s, |r| {
            r.texts.retain(|x| x.0 != c);
            r.texts.push((c, text, String::new()));
        });
    }

    fn set_text_view(&self, s: Surface, c: Ctl, text: &str) {
        let view = self.with(s, |r| {
            let entry = r.texts.iter_mut().find(|x| x.0 == c)?;
            if entry.2 == text && entry.1.string().to_string() == text {
                return None;
            }
            entry.2 = text.to_string();
            Some(entry.1.clone())
        });
        if let Some(v) = view {
            v.setString(&ns(text));
            v.setTextColor(Some(&NSColor::textColor()));
        }
    }

    fn append_text_view(&self, s: Surface, c: Ctl, text: &str, follow: bool) {
        let view = self.with(s, |r| {
            let entry = r.texts.iter_mut().find(|x| x.0 == c)?;
            entry.2.push_str(text);
            Some(entry.1.clone())
        });
        if let Some(v) = view {
            // SAFETY: inserting at the end of the view's own text storage.
            unsafe {
                if let Some(store) = v.textStorage() {
                    let attributed = objc2_foundation::NSAttributedString::from_nsstring(&ns(text));
                    store.appendAttributedString(&attributed);
                }
            }
            v.setFont(Some(&crate::app::log_font()));
            v.setTextColor(Some(&NSColor::textColor()));
            if follow {
                v.scrollRangeToVisible(NSRange::new(v.string().length(), 0));
            }
        }
    }

    fn tooltip(&self, s: Surface, c: Ctl, text: &str) {
        self.view(s, c).setToolTip(Some(&ns(text)));
    }

    fn control_size(&self, s: Surface, c: Ctl) -> (f64, f64) {
        let f = self.view(s, c).frame();
        (f.size.width, f.size.height)
    }

    fn rows_view(&self, s: Surface, c: Ctl, id: &str, spec: &RowsSpec, on: Rc<dyn Fn(RowsEvent)>) {
        let rows = Rc::new(Rows::new(self.mtm, id, spec, on));
        self.adopt_view(s, c, &rows.scroll);
        self.with(s, |r| {
            r.rows.retain(|x| x.0 != c);
            r.rows.push((c, rows));
        });
    }

    fn set_row_lines(&self, s: Surface, c: Ctl, lines: &[RowLine]) {
        self.rows_of(s, c).set_lines(lines);
    }

    fn hide_column(&self, s: Surface, c: Ctl, col: usize, hidden: bool) {
        self.rows_of(s, c).hide_column(col, hidden);
    }

    fn select_rows(&self, s: Surface, c: Ctl, rows: &[usize], visible: Option<usize>) {
        self.rows_of(s, c).select(rows, visible);
    }

    fn focus(&self, s: Surface, c: Ctl) {
        let list = self.with(s, |r| r.rows.iter().find(|x| x.0 == c).map(|x| x.1.clone()));
        match list {
            Some(l) => l.focus(),
            None => {
                let v = self.view(s, c);
                if let Some(w) = v.window() {
                    w.makeFirstResponder(Some(&v));
                }
            }
        }
    }

    fn page_rows(&self, s: Surface, c: Ctl) -> i32 {
        self.rows_of(s, c).page_rows()
    }

    fn menu_at(&self, s: Surface, c: Ctl, items: &[MenuEntry], x: f64, y: f64) -> Option<usize> {
        // A pane list's point is in its table, inside the scroll view the surface holds.
        let list = self.with(s, |r| r.rows.iter().find(|x| x.0 == c).map(|x| x.1.clone()));
        let view: Retained<NSView> = match &list {
            Some(l) => l.table_view().retain(),
            None => self.view(s, c),
        };
        crate::popup::show(self.mtm, &view, items, NSPoint::new(x, y))
    }

    fn wrap_text_view(&self, s: Surface, c: Ctl, wrap: bool) {
        let view = self.with(s, |r| {
            r.texts.iter().find(|x| x.0 == c).map(|x| x.1.clone())
        });
        let Some(v) = view else {
            return;
        };
        // SAFETY: the text view's own container, read on the main thread.
        let Some(container) = (unsafe { v.textContainer() }) else {
            return;
        };
        let wide = NSSize::new(1.0e7, 1.0e7);
        v.setHorizontallyResizable(!wrap);
        v.setMaxSize(wide);
        container.setWidthTracksTextView(wrap);
        let width = match wrap {
            true => v.frame().size.width,
            false => wide.width,
        };
        container.setContainerSize(NSSize::new(width, wide.height));
    }

    fn window_frame(&self, s: Surface) -> Option<(f64, f64, f64, f64)> {
        let f = self.ns_window(s)?.frame();
        Some((f.origin.x, f.origin.y, f.size.width, f.size.height))
    }

    fn set_window_frame(&self, s: Surface, (x, y, w, h): (f64, f64, f64, f64)) {
        if let Some(win) = self.ns_window(s) {
            win.setFrame_display(NSRect::new(NSPoint::new(x, y), NSSize::new(w, h)), true);
        }
    }

    fn page_viewport(&self, s: Surface) -> Option<(i32, i32)> {
        let scroll = self.body(s).enclosingScrollView()?;
        let visible = scroll.contentSize();
        Some((visible.width as i32, visible.height as i32))
    }

    fn set_surface_size(&self, s: Surface, w: i32, h: i32) {
        let body = self.body(s);
        let size = NSSize::new(f64::from(w), f64::from(h));
        if body.frame().size != size {
            body.setFrameSize(size);
        }
    }

    fn on_screen(&self, s: Surface) -> bool {
        !self.body(s).isHiddenOrHasHiddenAncestor()
    }

    fn text_area(&self, s: Surface, c: Ctl, id: &str, caption: &str) {
        let frame = NSRect::new(NSPoint::new(0.0, 0.0), NSSize::new(400.0, 160.0));
        let scroll = NSScrollView::initWithFrame(self.mtm.alloc(), frame);
        scroll.setHasVerticalScroller(true);
        scroll.setBorderType(objc2_app_kit::NSBorderType::BezelBorder);
        let view = NSTextView::initWithFrame(self.mtm.alloc(), frame);
        view.setEditable(true);
        view.setRichText(false);
        view.setVerticallyResizable(true);
        view.setAutoresizingMask(objc2_app_kit::NSAutoresizingMaskOptions::ViewWidthSizable);
        view.setFont(Some(&crate::app::log_font()));
        view.setTextColor(Some(&NSColor::textColor()));
        if !id.is_empty() {
            ui::set_id(&view, id);
        }
        view.setAccessibilityLabel(Some(&ns(caption)));
        let slot = self.with(s, |r| r.slot.clone());
        let d = ui::on_change(self.mtm, Box::new(move || fire(&slot, c, Ev::Edited)));
        view.setDelegate(Some(ProtocolObject::from_ref(&*d)));
        scroll.setDocumentView(Some(&view));
        self.adopt_view(s, c, &scroll);
        self.with(s, |r| {
            r.delegates.push((c, d));
            r.texts.retain(|x| x.0 != c);
            r.texts.push((c, view, String::new()));
        });
    }

    fn text_view_text(&self, s: Surface, c: Ctl) -> String {
        let view = self.with(s, |r| {
            r.texts.iter().find(|x| x.0 == c).map(|x| x.1.clone())
        });
        view.map(|v| v.string().to_string()).unwrap_or_default()
    }

    fn resize_window(&self, s: Surface, w: i32, h: i32) {
        if let Some(win) = self.ns_window(s) {
            win.setContentSize(NSSize::new(f64::from(w), f64::from(h)));
            win.center();
        }
    }

    fn key_equivalent(&self, s: Surface, c: Ctl, key: &str) {
        self.button_of(s, c).setKeyEquivalent(&ns(key));
    }

    fn remove(&self, s: Surface, c: Ctl) {
        let gone = self.with(s, |r| {
            r.delegates.retain(|(x, _)| *x != c);
            r.tables.retain(|t| t.0 != c);
            r.canvases.retain(|t| t.0 != c);
            r.rows.retain(|t| t.0 != c);
            r.texts.retain(|t| t.0 != c);
            let at = r.views.iter().position(|(x, _)| *x == c);
            at.map(|i| r.views.remove(i).1)
        });
        if let Some(v) = gone {
            v.removeFromSuperview();
        }
    }

    fn shown(&self, s: Surface, c: Ctl) -> bool {
        ui::shown(&self.view(s, c))
    }

    fn show(&self, s: Surface, c: Ctl, on: bool) {
        ui::set_visible(&self.view(s, c), on);
    }

    fn enable(&self, s: Surface, c: Ctl, on: bool) {
        if let Some(ctl) = self.view(s, c).downcast_ref::<NSControl>() {
            if ctl.isEnabled() != on {
                ctl.setEnabled(on);
            }
        }
    }

    fn checked(&self, s: Surface, c: Ctl) -> bool {
        self.button_of(s, c).state() == NSControlStateValueOn
    }

    fn set_check(&self, s: Surface, c: Ctl, on: bool) {
        let b = self.button_of(s, c);
        let want = if on {
            NSControlStateValueOn
        } else {
            NSControlStateValueOff
        };
        if b.state() != want {
            b.setState(want);
        }
    }

    fn set_title(&self, s: Surface, c: Ctl, title: &str) {
        let b = self.button_of(s, c);
        if b.title().to_string() != title {
            b.setTitle(&ns(title));
        }
    }

    fn set_text(&self, s: Surface, c: Ctl, text: &str) {
        ui::set_text(&self.text_of(s, c), text);
    }

    fn set_tone(&self, s: Surface, c: Ctl, tone: Option<Tone>) {
        self.text_of(s, c).setTextColor(Some(&tone_color(tone)));
    }

    fn text(&self, s: Surface, c: Ctl) -> String {
        let f = self.text_of(s, c);
        let was = READING.with(|r| r.replace(true));
        let now = f.stringValue().to_string();
        READING.with(|r| r.set(was));
        now
    }

    fn typing(&self, s: Surface, c: Ctl) -> bool {
        self.text_of(s, c).currentEditor().is_some()
    }

    fn picked(&self, s: Surface, c: Ctl) -> Option<usize> {
        usize::try_from(self.popup_of(s, c).indexOfSelectedItem()).ok()
    }

    fn select(&self, s: Surface, c: Ctl, i: usize) {
        self.popup_of(s, c).selectItemAtIndex(i as isize);
    }

    fn items(&self, s: Surface, c: Ctl) -> Vec<String> {
        let p = self.popup_of(s, c);
        let titles = p.itemTitles();
        titles.iter().map(|t| t.to_string()).collect()
    }

    fn set_items(&self, s: Surface, c: Ctl, items: &[String]) {
        let p = self.popup_of(s, c);
        p.removeAllItems();
        for i in items {
            p.addItemWithTitle(&ns(i));
        }
    }

    fn move_to(&self, s: Surface, c: Ctl, r: layout::Rect) {
        ui::move_to(&self.view(s, c), r);
    }

    fn later(&self, f: Box<dyn FnOnce()>) {
        crate::app::later(f);
    }

    fn after(&self, ms: u64, f: Box<dyn FnOnce()>) {
        let wait = std::time::Duration::from_millis(ms);
        let due = std::time::Instant::now() + wait;
        TIMED.with(|t| t.borrow_mut().push((due, f)));
        let when = dispatch2::DispatchTime::try_from(wait).unwrap_or(dispatch2::DispatchTime::NOW);
        let _ = dispatch2::DispatchQueue::main().after(when, run_timed);
    }

    fn log(&self, lines: Vec<(LogLevel, String)>) {
        crate::app::append_log(lines);
    }

    fn status(&self, text: &str) {
        let field = self.status.borrow().clone();
        if let Some(f) = field {
            ui::set_text(&f, text);
        }
    }

    fn menu_sources(&self, recent: &[String], devices: &[String]) {
        crate::menu::set_sources(recent, devices);
    }

    fn open_url(&self, url: &str) {
        if let Some(u) = NSURL::URLWithString(&ns(url)) {
            NSWorkspace::sharedWorkspace().openURL(&u);
        }
    }

    fn copy_text(&self, text: &str) {
        crate::effects::copy_text(text);
    }

    fn quit(&self) {
        NSApplication::sharedApplication(self.mtm).terminate(None);
    }

    fn pick(&self, spec: &PickSpec, folder: bool) -> Option<PathBuf> {
        crate::effects::pick(self.mtm, spec, folder)
    }

    fn pick_files(&self, title: &str) -> Vec<PathBuf> {
        crate::effects::pick_files(self.mtm, title)
    }

    fn confirm(&self, spec: &ConfirmSpec) -> bool {
        crate::effects::confirm(self.mtm, spec)
    }

    fn ask(&self, title: &str, message: &str, buttons: &[String]) -> Option<usize> {
        crate::effects::ask(self.mtm, title, message, buttons)
    }

    fn choose(&self, spec: &ChoiceSpec) -> usize {
        crate::effects::choose(self.mtm, spec)
    }

    fn ask_text(&self, spec: &TextSpec) -> Option<String> {
        crate::effects::ask_text(self.mtm, spec)
    }

    fn ask_password(&self, spec: &PasswordSpec) -> Option<String> {
        crate::effects::ask_password(self.mtm, spec)
    }
}
