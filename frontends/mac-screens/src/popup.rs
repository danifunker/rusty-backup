//! A pop-up menu of controller actions (the browse view's View menu, a row's menu): shown at a
//! point in a control, it answers the action picked, if any. The host shows only labels.

use rusty_backup::controller::commander::{
    CommanderAction as A, RowMenuEntry, ViewItemKind, ViewMenuItem,
};

use super::form::plain;
use super::host::{Ctl, Host, MenuEntry, Surface};

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

/// Show `items` at `(x, y)` in control `c` and wait for the user; the action picked, if any.
pub fn track(ui: &dyn Host, s: Surface, c: Ctl, items: Vec<MenuItem>, x: f64, y: f64) -> Option<A> {
    let mut actions: Vec<A> = Vec::new();
    let mut entries: Vec<MenuEntry> = Vec::new();
    for item in items {
        match item {
            MenuItem::Item {
                label,
                enabled,
                checked,
                action,
            } => {
                actions.push(action);
                entries.push(MenuEntry::Item {
                    label: plain(&label),
                    enabled,
                    checked,
                });
            }
            MenuItem::Sub { label, items } => {
                let mut labels = Vec::new();
                for (l, a) in items {
                    actions.push(a);
                    labels.push(plain(&l));
                }
                entries.push(MenuEntry::Sub {
                    label: plain(&label),
                    items: labels,
                });
            }
        }
    }
    let picked = ui.menu_at(s, c, &entries, x, y)?;
    actions.into_iter().nth(picked)
}
