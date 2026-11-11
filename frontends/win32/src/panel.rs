//! A window that holds a screen's controls: a tab's page, or a modeless dialog with a controller.
//!
//! Win32 sends a control's notifications to its parent, so each panel's handler gets the
//! `WM_COMMAND` and `WM_NOTIFY` of its own controls, its size changes, and a close request it may
//! refuse (a running write keeps a dialog up).
//!
//! UI Automation's Select changes an open drop-down list's selection without `CBN_SELCHANGE`, so a
//! list that closes on a new selection with no notification since it opened gets one here.

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
    SWP_NOZORDER, WM_CLOSE, WM_COMMAND, WM_ERASEBKGND, WM_MOUSEWHEEL, WM_NOTIFY, WM_SIZE,
    WM_VSCROLL, WNDCLASSW, WS_CAPTION,
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

/// A window's function keys: true when the key map took the virtual key.
pub type KeyHook = Rc<dyn Fn(u16) -> bool>;

thread_local! {
    static PANELS: RefCell<Vec<(isize, Weak<dyn Handler>)>> = const { RefCell::new(Vec::new()) };
    static KEYS: RefCell<Vec<(isize, KeyHook)>> = const { RefCell::new(Vec::new()) };
    /// Each open drop-down list and its selection when it opened.
    static DROPPED: RefCell<Vec<(isize, isize)>> = const { RefCell::new(Vec::new()) };
    /// Each list whose change was delivered at close-up, so a late real one is not sent twice.
    static SENT: RefCell<Vec<(isize, isize)>> = const { RefCell::new(Vec::new()) };
}

/// A combo box notification, with the `CBN_SELCHANGE` a silent selection change still owes.
fn combo_codes(from: HWND, code: u32) -> Vec<u32> {
    use windows::Win32::UI::WindowsAndMessaging::{
        SendMessageW, CBN_CLOSEUP, CBN_DROPDOWN, CBN_SELCHANGE, CB_GETCURSEL,
    };
    // SAFETY: a plain combo query on the control that sent the notification.
    let current = || unsafe { SendMessageW(from, CB_GETCURSEL, None, None).0 };
    let key = from.0 as isize;
    if code == CBN_SELCHANGE {
        let late = SENT.with(|s| {
            let mut s = s.borrow_mut();
            let at = s.iter().position(|(h, _)| *h == key);
            at.map(|i| s.remove(i).1) == Some(current())
        });
        if late {
            return Vec::new();
        }
    }
    DROPPED.with(|d| {
        let mut d = d.borrow_mut();
        let held = d.iter().position(|(h, _)| *h == key).map(|i| d.remove(i).1);
        match code {
            CBN_DROPDOWN => {
                SENT.with(|s| s.borrow_mut().retain(|(h, _)| *h != key));
                d.push((key, current()));
            }
            CBN_CLOSEUP if held.is_some_and(|was| was != current()) => {
                SENT.with(|s| s.borrow_mut().push((key, current())));
                return vec![CBN_SELCHANGE, code];
            }
            CBN_CLOSEUP | CBN_SELCHANGE => {}
            // Anything else leaves an open list's record as it was.
            _ => d.extend(held.map(|was| (key, was))),
        }
        vec![code]
    })
}

/// Send `hwnd`'s function keys to `hook` first, as GTK forwards a window's keys to its screen.
pub fn set_keys(hwnd: HWND, hook: KeyHook) {
    KEYS.with(|k| {
        let mut k = k.borrow_mut();
        k.retain(|(h, _)| *h != hwnd.0 as isize);
        k.push((hwnd.0 as isize, hook));
    });
}

/// Offer virtual key `vk`, pressed in top-level window `root`, to its hook; true when taken.
pub fn key(root: HWND, vk: u16) -> bool {
    let hook = KEYS.with(|k| {
        k.borrow()
            .iter()
            .find(|(h, _)| *h == root.0 as isize)
            .map(|(_, f)| f.clone())
    });
    hook.is_some_and(|f| f(vk))
}

pub fn register(hwnd: HWND, handler: Weak<dyn Handler>) {
    PANELS.with(|p| p.borrow_mut().push((hwnd.0 as isize, handler)));
}

pub fn unregister(hwnd: HWND) {
    PANELS.with(|p| p.borrow_mut().retain(|(h, _)| *h != hwnd.0 as isize));
    KEYS.with(|k| k.borrow_mut().retain(|(h, _)| *h != hwnd.0 as isize));
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
    unsafe { SetWindowLongPtrW(hwnd, GWLP_USERDATA, PAGE as _) };
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

/// `hwnd` is a tab's page (painted as a screen's body), not a dialog.
pub fn is_page(hwnd: HWND) -> bool {
    // SAFETY: a plain read of our own tag.
    unsafe { GetWindowLongPtrW(hwnd, GWLP_USERDATA) as isize == PAGE }
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

/// Where `tree` goes in `area`: the area itself while every showing control fits, else a taller
/// area moved up by the panel's scroll position, with the vertical scroll bar shown to reach the rest.
pub fn scroll_area(hwnd: HWND, tree: &layout::Node<HWND>, area: Rect, dpi: u32) -> Rect {
    use windows::Win32::UI::Controls::{SetScrollInfo, ShowScrollBar};
    use windows::Win32::UI::WindowsAndMessaging::{
        GetScrollPos, SCROLLINFO, SB_VERT, SIF_PAGE, SIF_RANGE,
    };
    // Laid out at its full height, the lowest showing control is how tall the page must be; the
    // hidden controls of other steps sit below it and take nothing on screen.
    let full = Rect {
        h: layout::scale(tree.min_height(), dpi),
        ..area
    };
    let need = layout::layout(tree, full, dpi)
        .iter()
        .filter(|(h, _)| crate::ui::shown(*h))
        .map(|(_, r)| r.y + r.h)
        .max()
        .unwrap_or(0)
        + layout::scale(layout::MARGIN, dpi);
    // SAFETY: scroll bar calls on our own window.
    unsafe {
        if need <= area.h {
            let _ = ShowScrollBar(hwnd, SB_VERT, false);
            return area;
        }
        let si = SCROLLINFO {
            cbSize: std::mem::size_of::<SCROLLINFO>() as u32,
            fMask: SIF_RANGE | SIF_PAGE,
            nMin: 0,
            nMax: need - 1,
            nPage: area.h.max(0) as u32,
            ..Default::default()
        };
        SetScrollInfo(hwnd, SB_VERT, &si, true);
        let _ = ShowScrollBar(hwnd, SB_VERT, true);
        let pos = GetScrollPos(hwnd, SB_VERT);
        Rect {
            x: area.x,
            y: area.y - pos,
            w: area.w,
            h: need,
        }
    }
}

/// Move the panel's scroll position for a `WM_VSCROLL` (`code`) or a wheel turn (`wheel`); true
/// when it moved, and the handler lays the panel out again at the new position.
fn scroll(hwnd: HWND, code: Option<u32>, wheel: i32) -> bool {
    use windows::Win32::UI::Controls::SetScrollInfo;
    use windows::Win32::UI::WindowsAndMessaging::{
        GetScrollInfo, SCROLLINFO, SB_BOTTOM, SB_LINEDOWN, SB_LINEUP, SB_PAGEDOWN,
        SB_PAGEUP, SB_THUMBPOSITION, SB_THUMBTRACK, SB_TOP, SB_VERT, SIF_ALL, SIF_POS,
    };
    let mut si = SCROLLINFO {
        cbSize: std::mem::size_of::<SCROLLINFO>() as u32,
        fMask: SIF_ALL,
        ..Default::default()
    };
    // SAFETY: scroll bar calls on our own window.
    unsafe {
        if GetScrollInfo(hwnd, SB_VERT, &mut si).is_err() || si.nMax <= 0 {
            return false;
        }
        let line = layout::scale(layout::ROW + layout::GAP, GetDpiForWindow(hwnd).max(96));
        let page = si.nPage as i32;
        let to = match code {
            Some(c) if c == SB_LINEUP.0 as u32 => si.nPos - line,
            Some(c) if c == SB_LINEDOWN.0 as u32 => si.nPos + line,
            Some(c) if c == SB_PAGEUP.0 as u32 => si.nPos - page,
            Some(c) if c == SB_PAGEDOWN.0 as u32 => si.nPos + page,
            Some(c) if c == SB_THUMBTRACK.0 as u32 || c == SB_THUMBPOSITION.0 as u32 => {
                si.nTrackPos
            }
            Some(c) if c == SB_TOP.0 as u32 => 0,
            Some(c) if c == SB_BOTTOM.0 as u32 => si.nMax,
            Some(_) => return false,
            None => si.nPos - wheel * 3 * line / 120,
        };
        let to = to.clamp(0, (si.nMax + 1 - page).max(0));
        if to == si.nPos {
            return false;
        }
        si.fMask = SIF_POS;
        si.nPos = to;
        SetScrollInfo(hwnd, SB_VERT, &si, true);
        true
    }
}

unsafe extern "system" fn proc(hwnd: HWND, msg: u32, wp: WPARAM, lp: LPARAM) -> LRESULT {
    let h = handler(hwnd);
    match (msg, h) {
        // Esc in a window arrives as IDCANCEL from no control: a close request, as the X is.
        (WM_COMMAND, Some(h)) if lp.0 == 0 && wp.0 & 0xffff == 2 => {
            // SAFETY: GWLP_USERDATA is ours.
            if unsafe { GetWindowLongPtrW(hwnd, GWLP_USERDATA) } as isize != PAGE {
                h.close();
            }
            LRESULT(0)
        }
        (WM_COMMAND, Some(h)) => {
            let from = HWND(lp.0 as _);
            for code in combo_codes(from, ((wp.0 >> 16) & 0xffff) as u32) {
                h.command(from, code);
            }
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
        (WM_VSCROLL, Some(h)) => {
            if scroll(hwnd, Some((wp.0 & 0xffff) as u32), 0) {
                h.resized();
            }
            LRESULT(0)
        }
        (WM_MOUSEWHEEL, Some(h)) => {
            let delta = ((wp.0 >> 16) & 0xffff) as u16 as i16 as i32;
            if scroll(hwnd, None, delta) {
                h.resized();
                LRESULT(0)
            } else {
                // SAFETY: an unscrollable panel passes the wheel on to its parent.
                unsafe { DefWindowProcW(hwnd, msg, wp, lp) }
            }
        }
        (WM_CLOSE, Some(h)) => {
            h.close();
            LRESULT(0)
        }
        // SAFETY: GWLP_USERDATA is ours; a page paints as a screen's body, a dialog as a window.
        (WM_ERASEBKGND, _) => unsafe {
            let page = GetWindowLongPtrW(hwnd, GWLP_USERDATA) as isize == PAGE;
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
            let page = unsafe { GetWindowLongPtrW(hwnd, GWLP_USERDATA) } as isize == PAGE;
            theme::ctl_color(msg, wp, lp, page).unwrap_or_default()
        }
        // SAFETY: everything else gets the default handling.
        _ => unsafe { DefWindowProcW(hwnd, msg, wp, lp) },
    }
}
