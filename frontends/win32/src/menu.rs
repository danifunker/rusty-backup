//! Popup menus whose entries carry the actions they send: the View menu and the row menus.

use rusty_backup::controller::commander::ViewItemKind;
use rusty_backup::controller::commander::{CommanderAction as A, RowMenuEntry, ViewMenuItem};
use windows::core::PCWSTR;
use windows::Win32::Foundation::HWND;
use windows::Win32::UI::WindowsAndMessaging::{
    AppendMenuW, CreatePopupMenu, DestroyMenu, TrackPopupMenu, MF_CHECKED, MF_GRAYED, MF_POPUP,
    MF_STRING, TPM_RETURNCMD,
};

use crate::ui;

/// One entry of a popup menu.
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
    /// A View menu entry, its chord after a tab as Windows menus show one.
    pub fn view(v: ViewMenuItem) -> MenuItem {
        MenuItem::Item {
            label: match v.hint.is_empty() {
                true => v.label,
                false => format!("{}\t{}", v.label, v.hint),
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

/// Track a popup menu of `items` at screen point `(x, y)`; the action picked, if any.
pub fn track(owner: HWND, items: Vec<MenuItem>, x: i32, y: i32) -> Option<A> {
    let mut actions: Vec<A> = Vec::new();
    // SAFETY: menus we create, track and destroy here; each label outlives its append.
    unsafe {
        let menu = CreatePopupMenu().ok()?;
        for item in items {
            match item {
                MenuItem::Item {
                    label,
                    enabled,
                    checked,
                    action,
                } => {
                    actions.push(action);
                    let mut flags = MF_STRING;
                    if !enabled {
                        flags |= MF_GRAYED;
                    }
                    if checked {
                        flags |= MF_CHECKED;
                    }
                    let t = ui::wide(&ui::plain(&label));
                    let _ = AppendMenuW(menu, flags, actions.len(), PCWSTR(t.as_ptr()));
                }
                MenuItem::Sub { label, items } => {
                    let Ok(sub) = CreatePopupMenu() else { continue };
                    for (l, a) in items {
                        actions.push(a);
                        let t = ui::wide(&ui::plain(&l));
                        let _ = AppendMenuW(sub, MF_STRING, actions.len(), PCWSTR(t.as_ptr()));
                    }
                    let t = ui::wide(&ui::plain(&label));
                    let _ = AppendMenuW(menu, MF_POPUP, sub.0 as usize, PCWSTR(t.as_ptr()));
                }
            }
        }
        let picked = TrackPopupMenu(menu, TPM_RETURNCMD, x, y, None, owner, None);
        let _ = DestroyMenu(menu);
        let i = usize::try_from(picked.0).ok()?;
        (i > 0).then(|| actions.swap_remove(i - 1))
    }
}
