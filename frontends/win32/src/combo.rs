//! A drop-down list control that is filled again only when its items change.

use std::cell::RefCell;

use windows::core::w;
use windows::Win32::Foundation::{HWND, LPARAM, WPARAM};
use windows::Win32::UI::WindowsAndMessaging::{
    SendMessageW, CBS_DROPDOWNLIST, CB_ADDSTRING, CB_GETCURSEL, CB_RESETCONTENT, CB_SETCURSEL,
    WINDOW_EX_STYLE, WINDOW_STYLE, WS_TABSTOP, WS_VSCROLL,
};

use crate::ui;

/// A drop-down list whose items the screen replaces only when they change.
pub struct Combo {
    pub h: HWND,
    items: RefCell<Vec<String>>,
}

impl Combo {
    pub fn build(parent: HWND, id: &str, items: &[&str]) -> Combo {
        let style = WINDOW_STYLE((CBS_DROPDOWNLIST as u32) | WS_TABSTOP.0 | WS_VSCROLL.0);
        let h = ui::child(parent, w!("COMBOBOX"), "", style, WINDOW_EX_STYLE(0), id);
        let c = Combo {
            h,
            items: RefCell::default(),
        };
        c.set_items(&items.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        c
    }

    pub fn set_items(&self, items: &[String]) {
        if *self.items.borrow() == items {
            return;
        }
        // SAFETY: plain combo messages; the combo copies each string.
        unsafe {
            SendMessageW(self.h, CB_RESETCONTENT, None, None);
            for item in items {
                let t = ui::wide(item);
                SendMessageW(
                    self.h,
                    CB_ADDSTRING,
                    None,
                    Some(LPARAM(t.as_ptr() as isize)),
                );
            }
        }
        *self.items.borrow_mut() = items.to_vec();
    }

    pub fn select(&self, i: usize) {
        // SAFETY: plain combo messages; CB_SETCURSEL sends no CBN_SELCHANGE.
        unsafe {
            if SendMessageW(self.h, CB_GETCURSEL, None, None).0 != i as isize {
                SendMessageW(self.h, CB_SETCURSEL, Some(WPARAM(i)), None);
            }
        }
    }

    pub fn picked(&self) -> Option<usize> {
        // SAFETY: a plain combo query.
        usize::try_from(unsafe { SendMessageW(self.h, CB_GETCURSEL, None, None).0 }).ok()
    }
}
