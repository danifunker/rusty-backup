//! A window that holds a screen's controls: a tab's page, or a modeless dialog with a controller.
//!
//! Win32 sends a control's notifications to its parent, so each panel's handler gets the
//! `WM_COMMAND` and `WM_NOTIFY` of its own controls, its size changes, and a close request it may
//! refuse (a running write keeps a dialog up).

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use windows::core::{w, PCWSTR};
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, RECT, WPARAM};
use windows::Win32::Graphics::Gdi::{FillRect, COLOR_BTNFACE, HBRUSH, HDC};
use windows::Win32::System::LibraryLoader::GetModuleHandleW;
use windows::Win32::UI::Controls::NMHDR;
use windows::Win32::UI::HiDpi::GetDpiForWindow;
use windows::Win32::UI::WindowsAndMessaging::{
    CreateWindowExW, DefWindowProcW, GetClientRect, GetWindowLongPtrW, GetWindowRect,
    RegisterClassW, SetWindowLongPtrW, SetWindowPos, CW_USEDEFAULT, GWLP_USERDATA, HMENU,
    SWP_NOZORDER, WM_CLOSE, WM_COMMAND, WM_ERASEBKGND, WM_NOTIFY, WM_SIZE, WNDCLASSW, WS_CAPTION,
    WS_CHILD, WS_CLIPCHILDREN, WS_EX_CONTROLPARENT, WS_POPUP, WS_SYSMENU, WS_THICKFRAME,
};

use crate::layout::{self, Rect};
use crate::theme;

/// What a panel's owner does with its window's messages.
pub trait Handler {
    fn command(&self, from: HWND, code: u32);
    /// A `WM_NOTIFY`; `Some` answers it, `None` leaves it to the default.
    fn notify(&self, hdr: &NMHDR, lp: LPARAM) -> Option<LRESULT>;
    fn resized(&self);
    /// The user asked to close the window; the handler closes it when its controller agrees.
    fn close(&self);
}

thread_local! {
    static PANELS: RefCell<Vec<(isize, Weak<dyn Handler>)>> = const { RefCell::new(Vec::new()) };
}

pub fn register(hwnd: HWND, handler: Weak<dyn Handler>) {
    PANELS.with(|p| p.borrow_mut().push((hwnd.0 as isize, handler)));
}

pub fn unregister(hwnd: HWND) {
    PANELS.with(|p| p.borrow_mut().retain(|(h, _)| *h != hwnd.0 as isize));
}

fn handler(hwnd: HWND) -> Option<Rc<dyn Handler>> {
    PANELS.with(|p| {
        p.borrow()
            .iter()
            .find(|(h, _)| *h == hwnd.0 as isize)
            .and_then(|(_, w)| w.upgrade())
    })
}

const PAGE: isize = 1;

fn class() -> PCWSTR {
    let name = w!("RustyBackupPanel");
    thread_local! {
        static DONE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
    if !DONE.with(|d| d.replace(true)) {
        // SAFETY: registering a window class once per thread.
        unsafe {
            let wc = WNDCLASSW {
                lpfnWndProc: Some(proc),
                hInstance: GetModuleHandleW(None).unwrap_or_default().into(),
                lpszClassName: name,
                hbrBackground: HBRUSH((COLOR_BTNFACE.0 + 1) as _),
                ..Default::default()
            };
            RegisterClassW(&wc);
        }
    }
    name
}

/// A tab's page: a child of `parent`, painted like the tab control's body.
pub fn page(parent: HWND) -> HWND {
    // SAFETY: creating a child window of ours.
    let hwnd = unsafe {
        CreateWindowExW(
            WS_EX_CONTROLPARENT,
            class(),
            PCWSTR::null(),
            WS_CHILD | WS_CLIPCHILDREN,
            0,
            0,
            0,
            0,
            Some(parent),
            Some(HMENU(std::ptr::null_mut())),
            GetModuleHandleW(None).ok().map(|i| i.into()),
            None,
        )
    }
    .expect("a page window");
    // SAFETY: tagging our own window.
    unsafe { SetWindowLongPtrW(hwnd, GWLP_USERDATA, PAGE) };
    hwnd
}

/// A modeless dialog owned by `owner`, `w` by `h` DIPs, centred on it; shown by the caller.
pub fn dialog(owner: HWND, title: &str, w: i32, h: i32) -> HWND {
    let t = crate::ui::wide(title);
    // SAFETY: creating an owned top-level window of ours and placing it over the owner.
    unsafe {
        let hwnd = CreateWindowExW(
            WS_EX_CONTROLPARENT,
            class(),
            PCWSTR(t.as_ptr()),
            WS_POPUP | WS_CAPTION | WS_SYSMENU | WS_THICKFRAME | WS_CLIPCHILDREN,
            CW_USEDEFAULT,
            CW_USEDEFAULT,
            0,
            0,
            Some(owner),
            Some(HMENU(std::ptr::null_mut())),
            GetModuleHandleW(None).ok().map(|i| i.into()),
            None,
        )
        .expect("a dialog window");
        let dpi = GetDpiForWindow(owner).max(96);
        let (cw, ch) = (layout::scale(w, dpi), layout::scale(h, dpi));
        let mut r = RECT::default();
        let _ = GetWindowRect(owner, &mut r);
        let x = r.left + ((r.right - r.left) - cw) / 2;
        let y = r.top + ((r.bottom - r.top) - ch) / 3;
        let _ = SetWindowPos(hwnd, None, x, y, cw, ch, SWP_NOZORDER);
        crate::theme::title_bar(hwnd);
        hwnd
    }
}

/// The panel's client area, for its layout.
pub fn client(hwnd: HWND) -> (Rect, u32) {
    let mut rc = RECT::default();
    // SAFETY: plain queries of our own window.
    let dpi = unsafe {
        let _ = GetClientRect(hwnd, &mut rc);
        GetDpiForWindow(hwnd).max(96)
    };
    (
        Rect {
            x: 0,
            y: 0,
            w: rc.right,
            h: rc.bottom,
        },
        dpi,
    )
}

unsafe extern "system" fn proc(hwnd: HWND, msg: u32, wp: WPARAM, lp: LPARAM) -> LRESULT {
    let h = handler(hwnd);
    match (msg, h) {
        (WM_COMMAND, Some(h)) => {
            h.command(HWND(lp.0 as _), ((wp.0 >> 16) & 0xffff) as u32);
            LRESULT(0)
        }
        (WM_NOTIFY, Some(h)) => {
            // SAFETY: WM_NOTIFY's lParam is an NMHDR.
            let hdr = unsafe { &*(lp.0 as *const NMHDR) };
            match h.notify(hdr, lp) {
                Some(r) => r,
                // SAFETY: the default handling.
                None => unsafe { DefWindowProcW(hwnd, msg, wp, lp) },
            }
        }
        (WM_SIZE, Some(h)) => {
            h.resized();
            LRESULT(0)
        }
        (WM_CLOSE, Some(h)) => {
            h.close();
            LRESULT(0)
        }
        // SAFETY: GWLP_USERDATA is ours; a page paints as a screen's body, a dialog as a window.
        (WM_ERASEBKGND, _) => unsafe {
            let page = GetWindowLongPtrW(hwnd, GWLP_USERDATA) == PAGE;
            let mut rc = RECT::default();
            let _ = GetClientRect(hwnd, &mut rc);
            let brush = if page {
                theme::panel_brush()
            } else {
                theme::window_brush()
            };
            FillRect(HDC(wp.0 as _), &rc, brush);
            LRESULT(1)
        },
        // SAFETY: GWLP_USERDATA is ours.
        (msg, _) if theme::ctl_color(msg, wp, lp, false).is_some() => {
            let page = unsafe { GetWindowLongPtrW(hwnd, GWLP_USERDATA) } == PAGE;
            theme::ctl_color(msg, wp, lp, page).unwrap_or_default()
        }
        // SAFETY: everything else gets the default handling.
        _ => unsafe { DefWindowProcW(hwnd, msg, wp, lp) },
    }
}
