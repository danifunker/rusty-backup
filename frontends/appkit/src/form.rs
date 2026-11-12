//! A screen's controls keyed by its own enum: built once, each tagged with its place, its actions
//! and text changes handed to the screen as that key. The screens of slices 4 and on are written
//! on it, so each is its controller's reading and its layout, not AppKit plumbing.

use std::cell::RefCell;
use std::rc::Rc;

use objc2::rc::Retained;
use objc2::runtime::NSObjectProtocol;
use objc2::runtime::{AnyObject, ProtocolObject};
use objc2::{define_class, msg_send, DefinedClass, MainThreadMarker, MainThreadOnly, Message};
use objc2_app_kit::{
    NSAccessibility, NSButton, NSColor, NSControl, NSControlStateValueOff, NSControlStateValueOn,
    NSLineBreakMode, NSPopUpButton, NSProgressIndicator, NSSecureTextField, NSTextField,
    NSTextFieldCell, NSView,
};
use objc2_foundation::{NSPoint, NSRect, NSSize};
use rusty_backup::controller::commander::Tone;

use crate::layout::{self, Node};
use crate::ui::{self, ns, Callback, TextChange};

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
            // SAFETY: the cell's own setter; the screen hears of it after (its sync guard drops its own sets).
            unsafe {
                let _: () = msg_send![super(self), setObjectValue: value];
            }
            let f = self.ivars().borrow().clone();
            if let Some(f) = f {
                f();
            }
        }
    }
);

type Fields<K> = Rc<RefCell<Vec<(K, Retained<NSTextField>, String)>>>;
type Pops<K> = Rc<RefCell<Vec<(K, Retained<NSPopUpButton>, isize)>>>;

/// The next field whose text changed since it was last reported, now marked as reported.
fn next_edit<K: Copy>(fields: &Fields<K>) -> Option<K> {
    for (k, f, last) in fields.borrow_mut().iter_mut() {
        let now = f.stringValue().to_string();
        if now != *last {
            *last = now;
            return Some(*k);
        }
    }
    None
}

/// The next pop-up whose pick changed without its action (an accessibility pick), now marked.
fn next_pick<K: Copy>(pops: &Pops<K>) -> Option<K> {
    for (k, p, last) in pops.borrow_mut().iter_mut() {
        let now = p.indexOfSelectedItem();
        if now != *last {
            *last = now;
            return Some(*k);
        }
    }
    None
}

pub struct Form<K: Copy + PartialEq + 'static> {
    mtm: MainThreadMarker,
    body: Retained<NSView>,
    views: RefCell<Vec<(K, Retained<NSView>)>>,
    target: Retained<Callback>,
    delegates: RefCell<Vec<Retained<TextChange>>>,
    keys: Rc<RefCell<Vec<K>>>,
    /// Each field and the text last reported to the screen or set from the controller.
    fields: Fields<K>,
    pops: Pops<K>,
    on: Rc<dyn Fn(K)>,
}

impl<K: Copy + PartialEq + 'static> Form<K> {
    /// A form on `body`; `on` hears each control's key when it acts or its text changes.
    pub fn new(mtm: MainThreadMarker, body: Retained<NSView>, on: Rc<dyn Fn(K)>) -> Form<K> {
        let keys: Rc<RefCell<Vec<K>>> = Rc::default();
        let fields: Fields<K> = Rc::default();
        let pops: Pops<K> = Rc::default();
        let (held, call, edited, picks) = (keys.clone(), on.clone(), fields.clone(), pops.clone());
        let target = ui::callback(
            mtm,
            Box::new(move |sender: &AnyObject| {
                let tag = sender
                    .downcast_ref::<NSControl>()
                    .map(|c| c.tag())
                    .or_else(|| {
                        sender
                            .downcast_ref::<objc2_app_kit::NSMenuItem>()
                            .map(|m| m.tag())
                    });
                let key = tag.and_then(|t| held.borrow().get(t as usize).copied());
                if let Some(k) = key {
                    // The sender's own pick or tick, which the refreshes below could reset.
                    let pick = sender
                        .downcast_ref::<NSPopUpButton>()
                        .map(|p| p.indexOfSelectedItem());
                    let tick = sender.downcast_ref::<NSButton>().map(|b| b.state());
                    // The sender reports itself below; the others' unreported changes go first.
                    if let Some(i) = pick {
                        for (pk, _, last) in picks.borrow_mut().iter_mut() {
                            if *pk == k {
                                *last = i;
                            }
                        }
                    }
                    // A value or pick set through accessibility is reported before the action,
                    // one at a time: each report refreshes the form, which must not undo the next.
                    while let Some(f) = next_edit(&edited).or_else(|| next_pick(&picks)) {
                        call(f);
                    }
                    if let (Some(p), Some(i)) = (sender.downcast_ref::<NSPopUpButton>(), pick) {
                        p.selectItemAtIndex(i);
                    } else if let (Some(b), Some(st)) = (sender.downcast_ref::<NSButton>(), tick) {
                        b.setState(st);
                    }
                    call(k);
                }
            }),
        );
        Form {
            mtm,
            body,
            views: RefCell::default(),
            target,
            delegates: RefCell::default(),
            keys,
            fields,
            pops,
            on,
        }
    }

    fn tag_of(&self, k: K) -> isize {
        let mut keys = self.keys.borrow_mut();
        match keys.iter().position(|x| *x == k) {
            Some(i) => i as isize,
            None => {
                keys.push(k);
                keys.len() as isize - 1
            }
        }
    }

    /// Add `view` as `k`; a control gets its tag and, unless it is a plain label, the action.
    pub fn add(&self, k: K, view: &NSView) {
        if let Some(c) = view.downcast_ref::<NSControl>() {
            c.setTag(self.tag_of(k));
            let label = c
                .downcast_ref::<NSTextField>()
                .is_some_and(|t| !t.isEditable());
            if !label {
                ui::hook(c, &self.target);
            }
        }
        self.body.addSubview(view);
        self.views.borrow_mut().push((k, view.retain()));
    }

    pub fn button(&self, k: K, title: &str, id: &str) -> Retained<NSButton> {
        let b = ui::button(self.mtm, title, id);
        self.add(k, &b);
        b
    }

    pub fn check(&self, k: K, title: &str, id: &str) -> Retained<NSButton> {
        let b = ui::checkbox(self.mtm, title, id);
        self.add(k, &b);
        b
    }

    pub fn label(&self, k: K, text: &str) -> Retained<NSTextField> {
        let l = ui::label(self.mtm, text);
        self.add(k, &l);
        l
    }

    /// A label that wraps onto as many lines as its frame gives it.
    pub fn wrap(&self, k: K, text: &str) -> Retained<NSTextField> {
        let l = NSTextField::wrappingLabelWithString(&ns(&ui::plain(text)), self.mtm);
        l.setLineBreakMode(NSLineBreakMode::ByWordWrapping);
        self.add(k, &l);
        l
    }

    pub fn popup(&self, k: K, items: &[String], id: &str) -> Retained<NSPopUpButton> {
        let p = NSPopUpButton::initWithFrame_pullsDown(self.mtm.alloc(), NSRect::ZERO, false);
        for i in items {
            p.addItemWithTitle(&ns(&ui::plain(i)));
        }
        if !id.is_empty() {
            ui::set_id(&p, id);
        }
        self.add(k, &p);
        self.pops
            .borrow_mut()
            .push((k, p.clone(), p.indexOfSelectedItem()));
        p
    }

    /// A pull-down whose first item is its title and whose items each act as `item_key(i)`.
    pub fn pulldown(&self, k: K, id: &str) -> Retained<NSPopUpButton> {
        let p = NSPopUpButton::initWithFrame_pullsDown(self.mtm.alloc(), NSRect::ZERO, true);
        ui::set_id(&p, id);
        self.add(k, &p);
        p
    }

    /// Fill pull-down `k`: its title, then `(label, key, on)` items; a `None` key is a caption.
    pub fn set_pulldown(&self, k: K, title: &str, items: &[(String, Option<K>, bool)]) {
        let p = self.popup_of(k);
        p.removeAllItems();
        p.addItemWithTitle(&ns(&ui::plain(title)));
        for (label, key, on) in items {
            p.addItemWithTitle(&ns(&ui::plain(label)));
            let Some(item) = p.lastItem() else { continue };
            match key {
                Some(key) => {
                    item.setTag(self.tag_of(*key));
                    // SAFETY: the target is the form's, kept as long as its controls.
                    unsafe {
                        item.setTarget(Some(&self.target));
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

    /// An editable field; every change reaches the screen as `k`.
    pub fn field(&self, k: K, id: &str, caption: &str, secret: bool) -> Retained<NSTextField> {
        let frame = NSRect::new(NSPoint::new(0.0, 0.0), NSSize::new(200.0, 22.0));
        let f: Retained<NSTextField> = if secret {
            Retained::into_super(NSSecureTextField::initWithFrame(self.mtm.alloc(), frame))
        } else {
            let f = NSTextField::initWithFrame(self.mtm.alloc(), frame);
            let edited = self.fields.clone();
            let on = self.on.clone();
            let report: Rc<dyn Fn()> = Rc::new(move || {
                // Only a change the screen has not heard yet: typing reports through the delegate.
                if next_edit(&edited).is_some() {
                    on(k);
                }
            });
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
        let on = self.on.clone();
        let edited = self.fields.clone();
        let d = ui::on_change(
            self.mtm,
            Box::new(move || {
                while next_edit(&edited).is_some() {}
                on(k);
            }),
        );
        // SAFETY: the delegate is kept by the form as long as the field.
        unsafe { f.setDelegate(Some(ProtocolObject::from_ref(&*d))) };
        self.delegates.borrow_mut().push(d);
        f.setTag(self.tag_of(k));
        self.fields.borrow_mut().push((k, f.clone(), String::new()));
        self.body.addSubview(&f);
        self.views
            .borrow_mut()
            .push((k, Retained::into_super(Retained::into_super(f.clone()))));
        f
    }

    pub fn progress(&self, k: K) -> Retained<NSProgressIndicator> {
        let p = ui::progress(self.mtm);
        self.add(k, &p);
        p
    }

    /// Drop every view whose key `gone` names (rows built again for a new source).
    pub fn remove(&self, gone: impl Fn(&K) -> bool) {
        self.fields.borrow_mut().retain(|(k, _, _)| !gone(k));
        self.pops.borrow_mut().retain(|(k, _, _)| !gone(k));
        self.views.borrow_mut().retain(|(k, v)| {
            let drop = gone(k);
            if drop {
                v.removeFromSuperview();
            }
            !drop
        });
    }

    pub fn has(&self, k: K) -> bool {
        self.views.borrow().iter().any(|(x, _)| *x == k)
    }

    pub fn v(&self, k: K) -> Retained<NSView> {
        self.views
            .borrow()
            .iter()
            .find(|(x, _)| *x == k)
            .map(|(_, v)| v.clone())
            .expect("every control is built")
    }

    fn popup_of(&self, k: K) -> Retained<NSPopUpButton> {
        self.v(k).downcast::<NSPopUpButton>().expect("a pop-up")
    }

    fn button_of(&self, k: K) -> Retained<NSButton> {
        self.v(k).downcast::<NSButton>().expect("a button")
    }

    fn text_of(&self, k: K) -> Retained<NSTextField> {
        self.v(k).downcast::<NSTextField>().expect("a text field")
    }

    pub fn shown(&self, k: K) -> bool {
        self.has(k) && ui::shown(&self.v(k))
    }

    pub fn show(&self, k: K, on: bool) {
        ui::set_visible(&self.v(k), on);
    }

    pub fn enable(&self, k: K, on: bool) {
        if let Some(c) = self.v(k).downcast_ref::<NSControl>() {
            if c.isEnabled() != on {
                c.setEnabled(on);
            }
        }
    }

    pub fn checked(&self, k: K) -> bool {
        self.button_of(k).state() == NSControlStateValueOn
    }

    pub fn set_check(&self, k: K, on: bool) {
        let b = self.button_of(k);
        let want = if on {
            NSControlStateValueOn
        } else {
            NSControlStateValueOff
        };
        if b.state() != want {
            b.setState(want);
        }
    }

    pub fn set_title(&self, k: K, t: &str) {
        let b = self.button_of(k);
        if b.title().to_string() != t {
            b.setTitle(&ns(t));
        }
    }

    /// A label's text, or a field's: a field keeps text the screen has not heard yet.
    pub fn set_text(&self, k: K, t: &str) {
        let f = self.text_of(k);
        if !f.isEditable() {
            ui::set_text(&f, t);
            return;
        }
        if self.typing(k) {
            return;
        }
        {
            let mut fields = self.fields.borrow_mut();
            let Some(entry) = fields.iter_mut().find(|(x, _, _)| *x == k) else {
                return;
            };
            let now = f.stringValue().to_string();
            if now != entry.2 && now != t {
                return;
            }
            entry.2 = ui::plain(t);
        }
        // The borrow ends first: the cell reports the set, and its report reads the fields.
        ui::set_text(&f, t);
    }

    pub fn set_tone(&self, k: K, tone: Option<Tone>) {
        self.text_of(k).setTextColor(Some(&tone_color(tone)));
    }

    pub fn text(&self, k: K) -> String {
        self.text_of(k).stringValue().to_string()
    }

    /// The field has the keyboard: what is typed there wins over the controller's copy.
    pub fn typing(&self, k: K) -> bool {
        self.text_of(k).currentEditor().is_some()
    }

    pub fn picked(&self, k: K) -> Option<usize> {
        usize::try_from(self.popup_of(k).indexOfSelectedItem()).ok()
    }

    pub fn select(&self, k: K, i: usize) {
        let p = self.popup_of(k);
        if p.indexOfSelectedItem() != i as isize {
            p.selectItemAtIndex(i as isize);
        }
        self.mark_pick(k);
    }

    /// The pop-up's pick now is the screen's own, not one to report.
    fn mark_pick(&self, k: K) {
        for (pk, p, last) in self.pops.borrow_mut().iter_mut() {
            if *pk == k {
                *last = p.indexOfSelectedItem();
            }
        }
    }

    /// Replace a pop-up's items, only when they changed.
    pub fn set_items(&self, k: K, items: &[String]) {
        let p = self.popup_of(k);
        let now: Vec<String> = p.itemTitles().iter().map(|t| t.to_string()).collect();
        let want: Vec<String> = items.iter().map(|i| ui::plain(i)).collect();
        if now != want {
            p.removeAllItems();
            for i in &want {
                p.addItemWithTitle(&ns(i));
            }
        }
        self.mark_pick(k);
    }

    /// Lay `tree` out in `area` and move each view to its place.
    pub fn place(&self, tree: &Node<K>, area: layout::Rect) {
        for (k, r) in layout::layout(tree, area, 96) {
            ui::move_to(&self.v(k), r);
        }
    }

    /// Prune `tree` of hidden views, then place it in `area`.
    pub fn place_shown(&self, tree: Node<K>, area: layout::Rect) {
        let keep = |k: &K| self.shown(*k);
        if let Some(tree) = tree.prune(&keep) {
            self.place(&tree, area);
        }
    }

    /// Prune `tree` of hidden views and lay it out in the page the form is on, which scrolls.
    pub fn place_page(&self, tree: Node<K>) {
        let keep = |k: &K| self.shown(*k);
        if let Some(tree) = tree.prune(&keep) {
            crate::app::place_page(&self.body, &tree, &|k, r| ui::move_to(&self.v(k), r));
        }
    }
}
