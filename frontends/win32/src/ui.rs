//! Small helpers every screen uses: wide strings, controls with their AutomationId, the UI font.

use std::cell::RefCell;

use windows::core::{w, PCWSTR};
use windows::Win32::Foundation::{HWND, LPARAM, WPARAM};
use windows::Win32::Graphics::Gdi::{CreateFontIndirectW, DeleteObject, HFONT, HGDIOBJ};
use windows::Win32::System::Com::{CoCreateInstance, CLSCTX_INPROC_SERVER};
use windows::Win32::System::LibraryLoader::GetModuleHandleW;
use windows::Win32::UI::Accessibility::{
    AutomationId_Property_GUID, CLSID_AccPropServices, IAccPropServices,
};
use windows::Win32::UI::HiDpi::{GetDpiForWindow, SystemParametersInfoForDpi};
use windows::Win32::UI::Input::KeyboardAndMouse::{EnableWindow, IsWindowEnabled};
use windows::Win32::UI::WindowsAndMessaging::{
    CreateWindowExW, GetWindowLongPtrW, GetWindowTextLengthW, GetWindowTextW, SendMessageW,
    SetWindowTextW, ShowWindow, BS_AUTOCHECKBOX, BS_PUSHBUTTON, CHILDID_SELF, ES_AUTOHSCROLL,
    GWL_STYLE, HMENU, NONCLIENTMETRICSW, OBJID_CLIENT, SPI_GETNONCLIENTMETRICS, SW_HIDE, SW_SHOW,
    WINDOW_EX_STYLE, WINDOW_STYLE, WM_SETFONT, WS_CHILD, WS_EX_CLIENTEDGE, WS_TABSTOP, WS_VISIBLE,
};

/// `s` as a NUL-terminated UTF-16 string.
pub fn wide(s: &str) -> Vec<u16> {
    s.encode_utf16().chain(Some(0)).collect()
}

/// Text from a volume can hold NUL, which would end a wide string early: drop it.
pub fn plain(s: &str) -> String {
    s.replace('\0', "")
}

/// Set the UIA AutomationId of `hwnd` to the controller id, so drivers find it by id (Gate 0).
pub fn set_automation_id(hwnd: HWND, id: &str) {
    thread_local! {
        static SVC: RefCell<Option<IAccPropServices>> = const { RefCell::new(None) };
    }
    if id.is_empty() {
        return;
    }
    SVC.with(|svc| {
        let mut svc = svc.borrow_mut();
        if svc.is_none() {
            // SAFETY: COM is initialised on this (the UI) thread in main.
            *svc = unsafe { CoCreateInstance(&CLSID_AccPropServices, None, CLSCTX_INPROC_SERVER) }
                .ok();
        }
        if let Some(s) = svc.as_ref() {
            let text = wide(id);
            // SAFETY: `text` outlives the call; the window is ours.
            let _ = unsafe {
                s.SetHwndPropStr(
                    hwnd,
                    OBJID_CLIENT.0 as u32,
                    CHILDID_SELF,
                    AutomationId_Property_GUID,
                    PCWSTR(text.as_ptr()),
                )
            };
        }
    });
}

/// Create a child control of `class`; `id` is its controller id (empty for decoration).
pub fn child(
    parent: HWND,
    class: PCWSTR,
    text: &str,
    style: WINDOW_STYLE,
    ex: WINDOW_EX_STYLE,
    id: &str,
) -> HWND {
    let text = wide(&plain(text));
    // SAFETY: the class names are system classes or ours; `text` outlives the call.
    let hwnd = unsafe {
        let inst = GetModuleHandleW(None).ok();
        CreateWindowExW(
            ex,
            class,
            PCWSTR(text.as_ptr()),
            WS_CHILD | WS_VISIBLE | style,
            0,
            0,
            0,
            0,
            Some(parent),
            Some(HMENU(std::ptr::null_mut())),
            inst.map(|i| i.into()),
            None,
        )
    }
    .expect("a child control");
    crate::theme::control(hwnd);
    set_automation_id(hwnd, id);
    set_font(hwnd, font_for(parent));
    hwnd
}

pub fn button(parent: HWND, text: &str, id: &str) -> HWND {
    child(
        parent,
        w!("BUTTON"),
        text,
        WINDOW_STYLE((BS_PUSHBUTTON as u32) | WS_TABSTOP.0),
        WINDOW_EX_STYLE(0),
        id,
    )
}

pub fn checkbox(parent: HWND, text: &str, id: &str) -> HWND {
    child(
        parent,
        w!("BUTTON"),
        text,
        WINDOW_STYLE((BS_AUTOCHECKBOX as u32) | WS_TABSTOP.0),
        WINDOW_EX_STYLE(0),
        id,
    )
}

pub fn label(parent: HWND, text: &str) -> HWND {
    child(
        parent,
        w!("STATIC"),
        text,
        WINDOW_STYLE(0),
        WINDOW_EX_STYLE(0),
        "",
    )
}

/// A one-line label for a file path: a path too long to fit loses its middle, not its end.
pub fn path_label(parent: HWND, text: &str) -> HWND {
    // SS_PATHELLIPSIS: a path has no spaces to wrap at, so a wrapping label would hide it.
    child(
        parent,
        w!("STATIC"),
        text,
        WINDOW_STYLE(0x8000),
        WINDOW_EX_STYLE(0),
        "",
    )
}

pub fn edit(parent: HWND, id: &str) -> HWND {
    child(
        parent,
        w!("EDIT"),
        "",
        WINDOW_STYLE(ES_AUTOHSCROLL as u32 | WS_TABSTOP.0),
        WS_EX_CLIENTEDGE,
        id,
    )
}

/// The window's text, or empty.
pub fn text(hwnd: HWND) -> String {
    // SAFETY: plain reads of a window we own.
    unsafe {
        let n = GetWindowTextLengthW(hwnd);
        let mut buf = vec![0u16; n as usize + 1];
        let got = GetWindowTextW(hwnd, &mut buf);
        String::from_utf16_lossy(&buf[..got.max(0) as usize])
    }
}

/// Set the text only when it changed: a set resets the caret and fires `EN_CHANGE`.
pub fn set_text(hwnd: HWND, value: &str) {
    let value = plain(value);
    if text(hwnd) != value {
        let w = wide(&value);
        // SAFETY: `w` outlives the call.
        let _ = unsafe { SetWindowTextW(hwnd, PCWSTR(w.as_ptr())) };
    }
}

/// The control's own visible flag: `IsWindowVisible` is false for every child of a hidden parent.
pub fn shown(hwnd: HWND) -> bool {
    // SAFETY: a plain read of the window's style.
    let style = unsafe { GetWindowLongPtrW(hwnd, GWL_STYLE) } as u32;
    style & WS_VISIBLE.0 != 0
}

pub fn set_visible(hwnd: HWND, on: bool) {
    // SAFETY: plain window calls.
    unsafe {
        if shown(hwnd) != on {
            let _ = ShowWindow(hwnd, if on { SW_SHOW } else { SW_HIDE });
        }
    }
}

/// Place a control at `r` in its parent's client area.
pub fn move_to(hwnd: HWND, r: crate::layout::Rect) {
    use windows::Win32::UI::WindowsAndMessaging::{SetWindowPos, SWP_NOACTIVATE, SWP_NOZORDER};
    // SAFETY: moving our own control.
    let _ = unsafe {
        SetWindowPos(
            hwnd,
            None,
            r.x,
            r.y,
            r.w,
            r.h,
            SWP_NOZORDER | SWP_NOACTIVATE,
        )
    };
}

pub fn set_enabled(hwnd: HWND, on: bool) {
    // SAFETY: plain window calls.
    unsafe {
        if IsWindowEnabled(hwnd).as_bool() != on {
            let _ = EnableWindow(hwnd, on);
        }
    }
}

thread_local! {
    /// The message font at each DPI it was asked for; fonts live as long as the process.
    static FONTS: RefCell<Vec<(u32, isize)>> = const { RefCell::new(Vec::new()) };
}

/// The system's message font at `hwnd`'s DPI (Segoe UI 9 pt at 100%).
pub fn font_for(hwnd: HWND) -> HFONT {
    // SAFETY: a plain query of a window we own.
    let dpi = unsafe { GetDpiForWindow(hwnd) }.max(96);
    font_at(dpi)
}

pub fn font_at(dpi: u32) -> HFONT {
    FONTS.with(|f| {
        if let Some((_, h)) = f.borrow().iter().find(|(d, _)| *d == dpi) {
            return HFONT(*h as _);
        }
        let mut m = NONCLIENTMETRICSW {
            cbSize: std::mem::size_of::<NONCLIENTMETRICSW>() as u32,
            ..Default::default()
        };
        // SAFETY: `m` is sized for the call.
        let font = unsafe {
            let _ = SystemParametersInfoForDpi(
                SPI_GETNONCLIENTMETRICS.0,
                m.cbSize,
                Some(&mut m as *mut _ as *mut _),
                0,
                dpi,
            );
            CreateFontIndirectW(&m.lfMessageFont)
        };
        f.borrow_mut().push((dpi, font.0 as isize));
        font
    })
}

pub fn set_font(hwnd: HWND, font: HFONT) {
    // SAFETY: WM_SETFONT takes the font handle; the font outlives the control.
    unsafe {
        SendMessageW(
            hwnd,
            WM_SETFONT,
            Some(WPARAM(font.0 as usize)),
            Some(LPARAM(1)),
        );
    }
}

/// Free the fonts at exit.
pub fn free_fonts() {
    FONTS.with(|f| {
        for (_, h) in f.borrow_mut().drain(..) {
            // SAFETY: each font was created here and no control uses it once the window is gone.
            let _ = unsafe { DeleteObject(HGDIOBJ(h as _)) };
        }
    });
}
