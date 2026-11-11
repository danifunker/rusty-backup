//! Dark-mode painting for the controls whose `DarkMode_*` style falls short.
//!
//! A tick box or radio keeps the dark style's box but draws its label black, and a drop-down
//! list keeps a light face. In dark mode both are painted here from [`theme::colors`]; in light
//! mode the subclasses pass everything through.

use windows::core::w;
use windows::Win32::Foundation::{COLORREF, HWND, LPARAM, LRESULT, POINT, RECT, WPARAM};
use windows::Win32::Graphics::Gdi::{
    BeginPaint, CreatePen, CreateSolidBrush, DeleteObject, DrawFocusRect, DrawTextW, EndPaint,
    FillRect, FrameRect, InvalidateRect, Polyline, SelectObject, SetBkMode, SetTextColor,
    DT_CALCRECT, DT_END_ELLIPSIS, DT_LEFT, DT_NOPREFIX, DT_SINGLELINE, DT_VCENTER, HBRUSH, HDC,
    HFONT, HGDIOBJ, PAINTSTRUCT, PS_SOLID, TRANSPARENT,
};
use windows::Win32::UI::Controls::{
    CloseThemeData, DrawThemeBackground, GetThemePartSize, OpenThemeData, TS_DRAW,
};
use windows::Win32::UI::HiDpi::GetDpiForWindow;
use windows::Win32::UI::Input::KeyboardAndMouse::{GetFocus, IsWindowEnabled};
use windows::Win32::UI::Shell::DefSubclassProc;
use windows::Win32::UI::WindowsAndMessaging::{
    GetClientRect, GetParent, GetWindowLongPtrW, GetWindowTextW, SendMessageW, BM_GETCHECK,
    BM_GETSTATE, BM_SETCHECK, BM_SETSTATE, CB_GETCURSEL, CB_GETLBTEXT, CB_GETLBTEXTLEN,
    CB_SETCURSEL, GWL_STYLE, WM_CTLCOLORSTATIC, WM_ENABLE, WM_ERASEBKGND, WM_GETFONT, WM_KILLFOCUS,
    WM_PAINT, WM_PRINTCLIENT, WM_SETFOCUS, WM_SETTEXT, WM_UPDATEUISTATE,
};

use crate::theme;

const BST_CHECKED: isize = 1;
const BST_INDETERMINATE: isize = 2;
const BST_PUSHED: isize = 4;
const BST_HOT: isize = 0x200;

/// The border of a drop-down list, and of one with the focus.
const BORDER: COLORREF = COLORREF(0x5a5a5a);
const BORDER_FOCUS: COLORREF = COLORREF(0x9a9a9a);

/// Messages after which the control's face may differ, so it is drawn again.
fn redraw_after(msg: u32) -> bool {
    matches!(
        msg,
        WM_SETFOCUS
            | WM_KILLFOCUS
            | WM_ENABLE
            | WM_SETTEXT
            | WM_UPDATEUISTATE
            | BM_SETCHECK
            | BM_SETSTATE
            | CB_SETCURSEL
    )
}

/// The subclass for tick boxes and radios.
pub unsafe extern "system" fn check_proc(
    hwnd: HWND,
    msg: u32,
    wp: WPARAM,
    lp: LPARAM,
    _id: usize,
    _data: usize,
) -> LRESULT {
    subclass(hwnd, msg, wp, lp, paint_check)
}

/// The subclass for drop-down lists.
pub unsafe extern "system" fn combo_proc(
    hwnd: HWND,
    msg: u32,
    wp: WPARAM,
    lp: LPARAM,
    _id: usize,
    _data: usize,
) -> LRESULT {
    subclass(hwnd, msg, wp, lp, paint_combo)
}

fn subclass(hwnd: HWND, msg: u32, wp: WPARAM, lp: LPARAM, paint: fn(HWND, HDC)) -> LRESULT {
    if theme::dark() {
        match msg {
            WM_ERASEBKGND => return LRESULT(1),
            WM_PAINT => {
                let mut ps = PAINTSTRUCT::default();
                // SAFETY: the paint cycle of our own control.
                unsafe {
                    let hdc = BeginPaint(hwnd, &mut ps);
                    paint(hwnd, hdc);
                    let _ = EndPaint(hwnd, &ps);
                }
                return LRESULT(0);
            }
            WM_PRINTCLIENT => {
                paint(hwnd, HDC(wp.0 as _));
                return LRESULT(0);
            }
            _ => {}
        }
    }
    // SAFETY: everything else goes on to the control.
    let r = unsafe { DefSubclassProc(hwnd, msg, wp, lp) };
    if theme::dark() && redraw_after(msg) {
        // SAFETY: invalidating our own control.
        let _ = unsafe { InvalidateRect(Some(hwnd), None, false) };
    }
    r
}

fn text_of(hwnd: HWND) -> Vec<u16> {
    let mut buf = [0u16; 256];
    // SAFETY: a plain query into a local buffer.
    let n = unsafe { GetWindowTextW(hwnd, &mut buf) };
    buf[..n.max(0) as usize].to_vec()
}

fn scale(hwnd: HWND, px: i32) -> i32 {
    // SAFETY: a plain query.
    let dpi = unsafe { GetDpiForWindow(hwnd) }.max(96) as i32;
    px * dpi / 96
}

/// Select the control's font into `hdc`; the old one comes back to restore.
fn use_font(hwnd: HWND, hdc: HDC) -> HGDIOBJ {
    // SAFETY: the font the control was given, selected into its paint DC.
    unsafe {
        let font = HFONT(SendMessageW(hwnd, WM_GETFONT, None, None).0 as _);
        SelectObject(hdc, font.into())
    }
}

fn paint_check(hwnd: HWND, hdc: HDC) {
    let mut rc = RECT::default();
    // SAFETY: drawing our own control into its paint DC; the theme handle is closed here.
    unsafe {
        let _ = GetClientRect(hwnd, &mut rc);
        // The parent's answer to WM_CTLCOLORSTATIC is the background the label sits on.
        let parent = GetParent(hwnd).unwrap_or_default();
        let br = SendMessageW(
            parent,
            WM_CTLCOLORSTATIC,
            Some(WPARAM(hdc.0 as usize)),
            Some(LPARAM(hwnd.0 as isize)),
        );
        let br = match br.0 {
            0 => theme::window_brush(),
            b => HBRUSH(b as _),
        };
        FillRect(hdc, &rc, br);
        let style = GetWindowLongPtrW(hwnd, GWL_STYLE) as u32 & 0xF;
        let radio = matches!(style, 4 | 9);
        let check = SendMessageW(hwnd, BM_GETCHECK, None, None).0;
        let state = SendMessageW(hwnd, BM_GETSTATE, None, None).0;
        let enabled = IsWindowEnabled(hwnd).as_bool();
        let base = match check {
            BST_CHECKED => 5,
            BST_INDETERMINATE if !radio => 9,
            _ => 1,
        };
        let offset = if !enabled {
            3
        } else if state & BST_PUSHED != 0 {
            2
        } else if state & BST_HOT != 0 {
            1
        } else {
            0
        };
        let part = if radio { 2 } else { 3 };
        let theme_h = OpenThemeData(Some(hwnd), w!("Button"));
        let size = GetThemePartSize(theme_h, Some(hdc), part, base + offset, None, TS_DRAW)
            .unwrap_or_default();
        let top = rc.top + (rc.bottom - rc.top - size.cy) / 2;
        let tick = RECT {
            left: rc.left,
            top,
            right: rc.left + size.cx,
            bottom: top + size.cy,
        };
        let _ = DrawThemeBackground(theme_h, hdc, part, base + offset, &tick, None);
        let _ = CloseThemeData(theme_h);
        let mut text = text_of(hwnd);
        let focused = GetFocus() == hwnd;
        if text.is_empty() {
            if focused {
                let around = RECT {
                    left: tick.left,
                    top: tick.top - 1,
                    right: tick.right + 1,
                    bottom: tick.bottom + 1,
                };
                let _ = DrawFocusRect(hdc, &around);
            }
            return;
        }
        let old = use_font(hwnd, hdc);
        SetBkMode(hdc, TRANSPARENT);
        let c = theme::colors();
        SetTextColor(hdc, if enabled { c.text } else { c.disabled });
        let mut label = RECT {
            left: tick.right + scale(hwnd, 4),
            ..rc
        };
        let flags = DT_SINGLELINE | DT_VCENTER | DT_LEFT | DT_NOPREFIX | DT_END_ELLIPSIS;
        DrawTextW(hdc, &mut text, &mut label, flags);
        if focused {
            let mut around = label;
            DrawTextW(hdc, &mut text, &mut around, flags | DT_CALCRECT);
            let h = around.bottom - around.top;
            around.top = rc.top + (rc.bottom - rc.top - h) / 2;
            around.bottom = around.top + h;
            around.left -= 1;
            around.right = (around.right + 1).min(rc.right);
            let _ = DrawFocusRect(hdc, &around);
        }
        SelectObject(hdc, old);
    }
}

fn paint_combo(hwnd: HWND, hdc: HDC) {
    let mut rc = RECT::default();
    let c = theme::colors();
    // SAFETY: drawing our own control into its paint DC; every GDI object made here is freed here.
    unsafe {
        let _ = GetClientRect(hwnd, &mut rc);
        FillRect(hdc, &rc, theme::field_brush());
        let focused = GetFocus() == hwnd;
        let enabled = IsWindowEnabled(hwnd).as_bool();
        let edge = CreateSolidBrush(if focused { BORDER_FOCUS } else { BORDER });
        FrameRect(hdc, &rc, edge);
        let _ = DeleteObject(edge.into());
        let ink = if enabled { c.text } else { c.disabled };
        // The arrow: a chevron centred in a square at the right.
        let side = rc.bottom - rc.top;
        let cx = rc.right - side / 2;
        let cy = rc.top + side / 2;
        let half = scale(hwnd, 4);
        let pen = CreatePen(PS_SOLID, scale(hwnd, 1), ink);
        let old_pen = SelectObject(hdc, pen.into());
        let _ = Polyline(
            hdc,
            &[
                POINT {
                    x: cx - half,
                    y: cy - half / 2,
                },
                POINT {
                    x: cx,
                    y: cy + half / 2,
                },
                POINT {
                    x: cx + half + 1,
                    y: cy - half / 2 - 1,
                },
            ],
        );
        SelectObject(hdc, old_pen);
        let _ = DeleteObject(pen.into());
        let i = SendMessageW(hwnd, CB_GETCURSEL, None, None).0;
        if i < 0 {
            return;
        }
        let n = SendMessageW(hwnd, CB_GETLBTEXTLEN, Some(WPARAM(i as usize)), None).0;
        if n <= 0 {
            return;
        }
        let mut text = vec![0u16; n as usize + 1];
        SendMessageW(
            hwnd,
            CB_GETLBTEXT,
            Some(WPARAM(i as usize)),
            Some(LPARAM(text.as_mut_ptr() as isize)),
        );
        text.truncate(n as usize);
        let old = use_font(hwnd, hdc);
        SetBkMode(hdc, TRANSPARENT);
        SetTextColor(hdc, ink);
        let mut label = RECT {
            left: rc.left + scale(hwnd, 6),
            right: rc.right - side,
            ..rc
        };
        DrawTextW(
            hdc,
            &mut text,
            &mut label,
            DT_SINGLELINE | DT_VCENTER | DT_LEFT | DT_NOPREFIX | DT_END_ELLIPSIS,
        );
        SelectObject(hdc, old);
    }
}
