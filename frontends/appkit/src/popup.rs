//! The pop-up menu behind `Host::menu_at`: labels shown at a point in a view, answering the
//! entry picked. Which action each entry stands for is the shared `popup`'s.

use std::cell::Cell;

use objc2::rc::Retained;
use objc2::runtime::AnyObject;
use objc2::{sel, MainThreadMarker};
use objc2_app_kit::{NSControlStateValueOn, NSMenu, NSMenuItem, NSView};
use objc2_foundation::NSPoint;

use crate::ui::{self, ns};

thread_local! {
    static PICKED: Cell<Option<isize>> = const { Cell::new(None) };
}

/// Show host menu `entries` at `at` in `view` and wait; the pick, numbered as `MenuEntry` says.
pub fn show(
    mtm: MainThreadMarker,
    view: &NSView,
    entries: &[crate::screens::host::MenuEntry],
    at: NSPoint,
) -> Option<usize> {
    use crate::screens::host::MenuEntry;
    let target = ui::callback(
        mtm,
        Box::new(|sender: &AnyObject| {
            if let Some(item) = sender.downcast_ref::<NSMenuItem>() {
                PICKED.with(|p| p.set(Some(item.tag())));
            }
        }),
    );
    let entry = |label: &str, on: bool, tag: isize| -> Retained<NSMenuItem> {
        // SAFETY: a plain menu item; its action goes to `target`, kept until the menu closes.
        let it = unsafe {
            NSMenuItem::initWithTitle_action_keyEquivalent(
                mtm.alloc(),
                &ns(label),
                Some(sel!(fire:)),
                &ns(""),
            )
        };
        // SAFETY: as above.
        unsafe { it.setTarget(Some(&target)) };
        it.setTag(tag);
        it.setEnabled(on);
        it
    };
    let menu = NSMenu::new(mtm);
    menu.setAutoenablesItems(false);
    let mut n: isize = 0;
    for e in entries {
        match e {
            MenuEntry::Item {
                label,
                enabled,
                checked,
            } => {
                let it = entry(label, *enabled, n);
                n += 1;
                if *checked {
                    it.setState(NSControlStateValueOn);
                }
                menu.addItem(&it);
            }
            MenuEntry::Sub { label, items } => {
                let sub = NSMenu::new(mtm);
                sub.setAutoenablesItems(false);
                for l in items {
                    sub.addItem(&entry(l, true, n));
                    n += 1;
                }
                let holder = NSMenuItem::new(mtm);
                holder.setTitle(&ns(label));
                holder.setSubmenu(Some(&sub));
                menu.addItem(&holder);
            }
        }
    }
    PICKED.with(|p| p.set(None));
    menu.popUpMenuPositioningItem_atLocation_inView(None, at, Some(view));
    PICKED.with(Cell::take).map(|p| p as usize)
}
