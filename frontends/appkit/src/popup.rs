//! A pop-up menu of controller actions (the browse view's View menu, a row's menu): shown at a
//! point in a view, it answers the action picked, if any.

use std::cell::Cell;

use objc2::rc::Retained;
use objc2::runtime::AnyObject;
use objc2::{sel, MainThreadMarker};
use objc2_app_kit::{NSControlStateValueOn, NSMenu, NSMenuItem, NSView};
use objc2_foundation::NSPoint;
use rusty_backup::controller::commander::{
    CommanderAction as A, RowMenuEntry, ViewItemKind, ViewMenuItem,
};

use crate::ui::{self, ns};

/// One entry of a pop-up menu.
pub enum MenuItem {
    Item {
        label: String,
        enabled: bool,
        checked: bool,
        action: A,
    },
    Sub {
        label: String,
        items: Vec<(String, A)>,
    },
}

impl MenuItem {
    /// A View menu entry; its chord shows after the label.
    pub fn view(v: ViewMenuItem) -> MenuItem {
        MenuItem::Item {
            label: match v.hint.is_empty() {
                true => v.label,
                false => format!("{}    {}", v.label, v.hint),
            },
            enabled: v.enabled,
            checked: v.checked && v.kind != ViewItemKind::Command,
            action: v.action,
        }
    }

    pub fn row(e: RowMenuEntry) -> MenuItem {
        match e {
            RowMenuEntry::Item {
                label,
                action,
                enabled,
            } => MenuItem::Item {
                label,
                enabled,
                checked: false,
                action,
            },
            RowMenuEntry::Submenu { label, items } => MenuItem::Sub { label, items },
        }
    }
}

thread_local! {
    static PICKED: Cell<Option<isize>> = const { Cell::new(None) };
}

/// Show `items` at `at` in `view` and wait for the user; the action picked, if any.
pub fn track(mtm: MainThreadMarker, view: &NSView, items: Vec<MenuItem>, at: NSPoint) -> Option<A> {
    let mut actions: Vec<A> = Vec::new();
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
                &ns(&ui::plain(label)),
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
    for item in items {
        match item {
            MenuItem::Item {
                label,
                enabled,
                checked,
                action,
            } => {
                actions.push(action);
                let it = entry(&label, enabled, actions.len() as isize - 1);
                if checked {
                    it.setState(NSControlStateValueOn);
                }
                menu.addItem(&it);
            }
            MenuItem::Sub { label, items } => {
                let sub = NSMenu::new(mtm);
                sub.setAutoenablesItems(false);
                for (l, a) in items {
                    actions.push(a);
                    sub.addItem(&entry(&l, true, actions.len() as isize - 1));
                }
                let holder = NSMenuItem::new(mtm);
                holder.setTitle(&ns(&ui::plain(&label)));
                holder.setSubmenu(Some(&sub));
                menu.addItem(&holder);
            }
        }
    }
    PICKED.with(|p| p.set(None));
    menu.popUpMenuPositioningItem_atLocation_inView(None, at, Some(view));
    let picked = PICKED.with(Cell::take)?;
    actions.into_iter().nth(picked as usize)
}
