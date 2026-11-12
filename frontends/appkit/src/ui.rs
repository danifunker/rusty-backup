//! Small AppKit helpers: views carrying their controller id, frames, text and the action target.
//!
//! Every actionable view gets its controller id as its accessibility identifier, which AX shows as
//! `AXIdentifier`; the flow driver finds controls by it. Every control's action goes to one
//! [`Target`], which hands the sender's id to `app::command`.

use std::cell::OnceCell;

use objc2::rc::Retained;
use objc2::runtime::{AnyObject, NSObject, NSObjectProtocol};
use objc2::{define_class, msg_send, sel, MainThreadMarker, MainThreadOnly};
use objc2_app_kit::{
    NSAccessibility, NSButton, NSMenuItem, NSTextField, NSToolbarItem,
    NSUserInterfaceItemIdentification, NSView,
};
use objc2_foundation::{NSPoint, NSRect, NSSize, NSString};

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

thread_local! {
    static TARGET: OnceCell<Retained<Target>> = const { OnceCell::new() };
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
    b
}

pub fn checkbox(mtm: MainThreadMarker, title: &str, id: &str) -> Retained<NSButton> {
    let t = target(mtm);
    // SAFETY: as for `button`.
    let b = unsafe {
        NSButton::checkboxWithTitle_target_action(&ns(title), Some(&t), Some(sel!(act:)), mtm)
    };
    set_id(&b, id);
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
