//! The AppKit [`Host`] for the shared screens' [`Form`] (frontends/mac-screens): each control an
//! objc2 view on a surface's body, its tag its control number, its action the surface's target.
//!
//! What stays here is AppKit's own: a field's cell that reports a value set through accessibility
//! (typing reports through the delegate), and a guard so reading a field, which commits an edit in
//! progress, reports nothing. `Form::new`, `add`, `v` and `place_page` are this shell's extras.

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
    NSSecureTextField, NSTextField, NSTextFieldCell, NSView, NSWindow, NSWorkspace,
};
use objc2_foundation::{NSPoint, NSRect, NSSize, NSURL};
use rusty_backup::controller::commander::Tone;
use rusty_backup::controller::log::LogLevel;
use rusty_backup::controller::{ChoiceSpec, ConfirmSpec, PasswordSpec, PickSpec, TextSpec};

use crate::layout::{self, Node};
use crate::screens::host::{Column, Ctl, Ev, Host, PullItem, Surface};
use crate::table::Table;
use crate::ui::{self, ns, Callback, CloseDelegate, TextChange};

/// A screen's controls; on this shell the host is [`AppKitHost`].
pub type Form<K> = crate::screens::form::Form<K>;

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

    fn key_equivalent(&self, s: Surface, c: Ctl, key: &str) {
        self.button_of(s, c).setKeyEquivalent(&ns(key));
    }

    fn remove(&self, s: Surface, c: Ctl) {
        let gone = self.with(s, |r| {
            r.delegates.retain(|(x, _)| *x != c);
            r.tables.retain(|t| t.0 != c);
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

    fn log(&self, lines: Vec<(LogLevel, String)>) {
        crate::app::append_log(lines);
    }

    fn status(&self, text: &str) {
        let field = self.status.borrow().clone();
        if let Some(f) = field {
            ui::set_text(&f, text);
        }
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

    fn confirm(&self, spec: &ConfirmSpec) -> bool {
        crate::effects::confirm(self.mtm, spec)
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

impl<K: Copy + PartialEq + 'static> crate::screens::form::Form<K> {
    /// A form on `body`; `on` hears each control's key when it acts or its text changes.
    pub fn new(mtm: MainThreadMarker, body: Retained<NSView>, on: Rc<dyn Fn(K)>) -> Form<K> {
        let host = AppKitHost::get(mtm);
        let s = host.adopt(body);
        Self::with_host(host, s, on)
    }

    /// The AppKit host behind every form on this shell.
    fn ak(&self) -> Rc<AppKitHost> {
        AppKitHost::current()
    }

    /// Add a view the screen built (a list, a bar) as `k`.
    pub fn add(&self, k: K, view: &NSView) {
        let c = self.custom(k);
        self.ak().adopt_view(self.surface(), c, view);
    }

    pub fn v(&self, k: K) -> Retained<NSView> {
        self.ak().view(self.surface(), self.ctl(k))
    }

    /// Prune `tree` of hidden views and lay it out in the page the form is on, which scrolls.
    pub fn place_page(&self, tree: Node<K>) {
        let keep = |k: &K| self.shown(*k);
        if let Some(tree) = tree.prune(&keep) {
            let body = self.ak().body(self.surface());
            crate::app::place_page(&body, &tree, &|k, r| ui::move_to(&self.v(k), r));
        }
    }
}
