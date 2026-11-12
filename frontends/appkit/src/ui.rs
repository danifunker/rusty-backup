//! Small AppKit helpers: views carrying their controller id, frames, text and the action target.
//!
//! Every actionable view gets its controller id as its accessibility identifier, which AX shows as
//! `AXIdentifier`; the flow driver finds controls by it. Every control's action goes to one
//! [`Target`], which hands the sender's id to `app::command`.

use std::cell::{OnceCell, RefCell};

use objc2::rc::Retained;
use objc2::runtime::{AnyObject, NSObject, NSObjectProtocol, ProtocolObject};
use objc2::{define_class, msg_send, sel, DefinedClass, MainThreadMarker, MainThreadOnly, Message};
use objc2_app_kit::{
    NSAccessibility, NSBackingStoreType, NSButton, NSControl, NSControlTextEditingDelegate,
    NSMenuItem, NSProgressIndicator, NSProgressIndicatorStyle, NSTextDelegate, NSTextField,
    NSTextFieldDelegate, NSTextViewDelegate, NSToolbarItem, NSUserInterfaceItemIdentification,
    NSView, NSWindow, NSWindowDelegate, NSWindowStyleMask,
};
use objc2_foundation::{NSNotification, NSPoint, NSRect, NSSize, NSString};

use crate::layout::Rect;

define_class!(
    /// A container whose y axis grows down, as the layout tree's does.
    #[unsafe(super(NSView))]
    #[thread_kind = MainThreadOnly]
    #[name = "RBFlippedView"]
    pub struct Flipped;

    unsafe impl NSObjectProtocol for Flipped {}

    impl Flipped {
        #[unsafe(method(isFlipped))]
        fn is_flipped(&self) -> bool {
            true
        }
    }
);

define_class!(
    /// The target of every control's action.
    #[unsafe(super(NSObject))]
    #[thread_kind = MainThreadOnly]
    #[name = "RBTarget"]
    pub struct Target;

    unsafe impl NSObjectProtocol for Target {}

    impl Target {
        #[unsafe(method(act:))]
        fn act(&self, sender: Option<&AnyObject>) {
            if let Some(id) = sender.and_then(sender_id) {
                crate::app::command(&id);
            }
        }
    }
);

define_class!(
    /// A window's delegate that hands a close to its owner, which decides when the window goes.
    #[unsafe(super(NSObject))]
    #[thread_kind = MainThreadOnly]
    #[name = "RBCloseDelegate"]
    #[ivars = (Box<dyn Fn()>, RefCell<Option<Box<dyn Fn()>>>)]
    pub struct CloseDelegate;

    unsafe impl NSObjectProtocol for CloseDelegate {}

    unsafe impl NSWindowDelegate for CloseDelegate {
        #[unsafe(method(windowShouldClose:))]
        fn should_close(&self, _sender: &NSWindow) -> bool {
            (self.ivars().0)();
            false
        }

        #[unsafe(method(windowDidResize:))]
        fn did_resize(&self, _note: &NSNotification) {
            if let Some(f) = &*self.ivars().1.borrow() {
                f();
            }
        }
    }
);

impl CloseDelegate {
    /// Run `f` whenever the window is resized, to lay it out again.
    pub fn on_resize(&self, f: Box<dyn Fn()>) {
        *self.ivars().1.borrow_mut() = Some(f);
    }
}

define_class!(
    /// An action target that runs a closure with the sender, for a window's own controls.
    #[unsafe(super(NSObject))]
    #[thread_kind = MainThreadOnly]
    #[name = "RBCallback"]
    #[ivars = Box<dyn Fn(&AnyObject)>]
    pub struct Callback;

    unsafe impl NSObjectProtocol for Callback {}

    impl Callback {
        #[unsafe(method(fire:))]
        fn fire(&self, sender: Option<&AnyObject>) {
            if let Some(s) = sender {
                (self.ivars())(s);
            }
        }
    }
);

define_class!(
    /// A text field's or text view's delegate that runs a closure on every change.
    #[unsafe(super(NSObject))]
    #[thread_kind = MainThreadOnly]
    #[name = "RBTextChange"]
    #[ivars = Box<dyn Fn()>]
    pub struct TextChange;

    unsafe impl NSObjectProtocol for TextChange {}
    unsafe impl NSControlTextEditingDelegate for TextChange {}
    unsafe impl NSTextFieldDelegate for TextChange {}
    unsafe impl NSTextDelegate for TextChange {}
    unsafe impl NSTextViewDelegate for TextChange {}

    impl TextChange {
        #[unsafe(method(controlTextDidChange:))]
        fn field_changed(&self, _note: &NSNotification) {
            (self.ivars())();
        }

        #[unsafe(method(textDidChange:))]
        fn view_changed(&self, _note: &NSNotification) {
            (self.ivars())();
        }
    }
);

/// A target running `f`; the caller keeps it as long as the controls that use it.
pub fn callback(mtm: MainThreadMarker, f: Box<dyn Fn(&AnyObject)>) -> Retained<Callback> {
    let this = mtm.alloc::<Callback>().set_ivars(f);
    // SAFETY: NSObject's plain init.
    unsafe { msg_send![super(this), init] }
}

/// Point `control`'s action at `target`.
pub fn hook(control: &NSControl, target: &Callback) {
    // SAFETY: the caller keeps the target alive with the control.
    unsafe {
        control.setTarget(Some(target));
        control.setAction(Some(sel!(fire:)));
    }
}

/// A change delegate running `f`; the caller keeps it as long as the field.
pub fn on_change(mtm: MainThreadMarker, f: Box<dyn Fn()>) -> Retained<TextChange> {
    let this = mtm.alloc::<TextChange>().set_ivars(f);
    // SAFETY: NSObject's plain init.
    unsafe { msg_send![super(this), init] }
}

thread_local! {
    static TARGET: OnceCell<Retained<Target>> = const { OnceCell::new() };
    /// Every control by its controller id, so the menu bar can mirror its state.
    static CONTROLS: RefCell<Vec<(String, Retained<NSControl>)>> = const { RefCell::new(Vec::new()) };
}

/// Remember `control` under `id`; a menu item with the same id follows its state.
pub fn register(control: &NSControl, id: &str) {
    if id.is_empty() {
        return;
    }
    let keep: Retained<NSControl> = control.retain();
    CONTROLS.with(|c| c.borrow_mut().push((id.to_string(), keep)));
}

/// Whether the control with `id` is on screen and enabled: what its menu item should say.
pub fn usable(id: &str) -> bool {
    CONTROLS.with(|c| {
        c.borrow().iter().any(|(i, v)| {
            i == id && v.isEnabled() && !v.isHiddenOrHasHiddenAncestor() && v.window().is_some()
        })
    })
}

/// A titled window the caller owns; closing it asks `on_close`, which decides.
pub fn window(
    mtm: MainThreadMarker,
    title: &str,
    w: f64,
    h: f64,
    on_close: Box<dyn Fn()>,
) -> (Retained<NSWindow>, Retained<CloseDelegate>) {
    let style =
        NSWindowStyleMask::Titled | NSWindowStyleMask::Closable | NSWindowStyleMask::Resizable;
    let frame = NSRect::new(NSPoint::new(0.0, 0.0), NSSize::new(w, h));
    // SAFETY: the designated initialiser; the caller keeps the window.
    let window = unsafe {
        NSWindow::initWithContentRect_styleMask_backing_defer(
            mtm.alloc(),
            frame,
            style,
            NSBackingStoreType::Buffered,
            false,
        )
    };
    // SAFETY: the caller owns the window, so AppKit must not release it on close.
    unsafe { window.setReleasedWhenClosed(false) };
    window.setTitle(&ns(title));
    window.setContentView(Some(&flipped(mtm)));
    let delegate: Retained<CloseDelegate> = {
        let this = mtm
            .alloc::<CloseDelegate>()
            .set_ivars((on_close, RefCell::new(None)));
        // SAFETY: NSObject's plain init.
        unsafe { msg_send![super(this), init] }
    };
    window.setDelegate(Some(ProtocolObject::from_ref(&*delegate)));
    window.center();
    (window, delegate)
}

/// A bar for a running job: its percentage, or an indeterminate sweep when it has none.
/// Show a new window with nothing focused: a focused field would keep the controller's text out.
pub fn present(window: &NSWindow) {
    window.makeKeyAndOrderFront(None);
    window.makeFirstResponder(None);
}

pub fn progress(mtm: MainThreadMarker) -> Retained<NSProgressIndicator> {
    let p = NSProgressIndicator::initWithFrame(mtm.alloc(), NSRect::ZERO);
    p.setStyle(NSProgressIndicatorStyle::Bar);
    p.setMinValue(0.0);
    p.setMaxValue(100.0);
    p
}

/// Show `bar` for a running job at `pct`, or sweeping when there is no percentage.
pub fn set_progress(bar: &NSProgressIndicator, running: bool, pct: Option<u32>) {
    set_visible(bar, running);
    let sweep = running && pct.is_none();
    if bar.isIndeterminate() != sweep {
        bar.setIndeterminate(sweep);
    }
    if sweep {
        // SAFETY: a plain animation start on the main thread.
        unsafe { bar.startAnimation(None) };
    } else {
        bar.setDoubleValue(f64::from(pct.unwrap_or(0)));
    }
}

/// The one action target; it lives as long as the app.
pub fn target(mtm: MainThreadMarker) -> Retained<Target> {
    TARGET.with(|t| {
        t.get_or_init(|| {
            let this = mtm.alloc::<Target>().set_ivars(());
            // SAFETY: NSObject's plain init.
            unsafe { msg_send![super(this), init] }
        })
        .clone()
    })
}

/// The controller id a sender carries: a toolbar item's identifier, a menu item's, a view's AX id.
fn sender_id(sender: &AnyObject) -> Option<String> {
    if let Some(item) = sender.downcast_ref::<NSToolbarItem>() {
        return Some(item.itemIdentifier().to_string());
    }
    if let Some(item) = sender.downcast_ref::<NSMenuItem>() {
        return item.identifier().map(|i| i.to_string());
    }
    let view = sender.downcast_ref::<NSView>()?;
    view.accessibilityIdentifier().map(|s| s.to_string())
}

pub fn ns(s: &str) -> Retained<NSString> {
    NSString::from_str(s)
}

/// Text from a volume can hold NUL, which an `NSString` would keep and a C API would cut: drop it.
pub fn plain(s: &str) -> String {
    s.replace('\0', "")
}

/// A new flipped container.
pub fn flipped(mtm: MainThreadMarker) -> Retained<NSView> {
    let this = mtm.alloc::<Flipped>().set_ivars(());
    // SAFETY: NSView's designated initialiser.
    let v: Retained<Flipped> = unsafe { msg_send![super(this), initWithFrame: NSRect::ZERO] };
    Retained::into_super(v)
}

/// Give `view` its controller id as its accessibility identifier.
pub fn set_id(view: &NSView, id: &str) {
    view.setAccessibilityIdentifier(Some(&ns(id)));
}

pub fn button(mtm: MainThreadMarker, title: &str, id: &str) -> Retained<NSButton> {
    let t = target(mtm);
    // SAFETY: the target outlives every control (it is never dropped).
    let b = unsafe {
        NSButton::buttonWithTitle_target_action(&ns(title), Some(&t), Some(sel!(act:)), mtm)
    };
    set_id(&b, id);
    register(&b, id);
    b
}

pub fn checkbox(mtm: MainThreadMarker, title: &str, id: &str) -> Retained<NSButton> {
    let t = target(mtm);
    // SAFETY: as for `button`.
    let b = unsafe {
        NSButton::checkboxWithTitle_target_action(&ns(title), Some(&t), Some(sel!(act:)), mtm)
    };
    set_id(&b, id);
    register(&b, id);
    b
}

pub fn label(mtm: MainThreadMarker, text: &str) -> Retained<NSTextField> {
    NSTextField::labelWithString(&ns(&plain(text)), mtm)
}

/// Set a field's text only when it changed: a blind set moves the caret mid-typing.
pub fn set_text(field: &NSTextField, text: &str) {
    let text = plain(text);
    if field.stringValue().to_string() != text {
        field.setStringValue(&ns(&text));
    }
}

pub fn shown(view: &NSView) -> bool {
    !view.isHidden()
}

pub fn set_visible(view: &NSView, on: bool) {
    if view.isHidden() == on {
        view.setHidden(!on);
    }
}

pub fn set_enabled(button: &NSButton, on: bool) {
    if button.isEnabled() != on {
        button.setEnabled(on);
    }
}

pub fn move_to(view: &NSView, r: Rect) {
    let frame = NSRect::new(
        NSPoint::new(f64::from(r.x), f64::from(r.y)),
        NSSize::new(f64::from(r.w.max(0)), f64::from(r.h.max(0))),
    );
    if view.frame() != frame {
        view.setFrame(frame);
    }
}

/// `view`'s size as a layout rectangle at the origin.
pub fn area(view: &NSView) -> Rect {
    let b = view.bounds();
    Rect {
        x: 0,
        y: 0,
        w: b.size.width as i32,
        h: b.size.height as i32,
    }
}
