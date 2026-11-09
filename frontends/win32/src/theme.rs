//! Light or dark, following the system's app theme (`AppsUseLightTheme`), or `RUSTY_BACKUP_THEME`.
//!
//! The title bar's dark mode is documented (`DWMWA_USE_IMMERSIVE_DARK_MODE`); the common controls
//! have no documented switch, so dark scroll bars, buttons, lists and menus come from the
//! `uxtheme` calls every dark Win32 app uses (by ordinal, Windows 10 1809 and later) and the
//! `DarkMode_*` visual-style classes. Backgrounds and text are ours, through `WM_CTLCOLOR*`.
//! Task Dialogs have no dark form and stay light. Tick boxes, radios and drop-down lists are
//! painted by [`crate::darkdraw`] in dark mode, where their dark styles fall short.

use std::cell::{Cell, RefCell};

use rusty_backup::controller::commander::Tone;
use windows::core::{w, BOOL, PCSTR, PCWSTR};
use windows::Win32::Foundation::{COLORREF, HWND, LPARAM, LRESULT, WPARAM};
use windows::Win32::Graphics::Dwm::{DwmSetWindowAttribute, DWMWA_USE_IMMERSIVE_DARK_MODE};
use windows::Win32::Graphics::Gdi::{
    CreateSolidBrush, DeleteObject, GetSysColor, RedrawWindow, SetBkColor, SetTextColor,
    COLOR_BTNFACE, COLOR_GRAYTEXT, COLOR_WINDOW, COLOR_WINDOWTEXT, HBRUSH, HDC, HGDIOBJ,
    RDW_ALLCHILDREN, RDW_ERASE, RDW_FRAME, RDW_INVALIDATE,
};
use windows::Win32::System::LibraryLoader::{GetProcAddress, LoadLibraryW};
use windows::Win32::System::Registry::{RegGetValueW, HKEY_CURRENT_USER, RRF_RT_REG_DWORD};
use windows::Win32::UI::Controls::{
    GetComboBoxInfo, SetWindowTheme, COMBOBOXINFO, LVM_GETHEADER, LVM_SETBKCOLOR,
    LVM_SETTEXTBKCOLOR, LVM_SETTEXTCOLOR,
};
use windows::Win32::UI::Shell::{DefSubclassProc, SetWindowSubclass};
use windows::Win32::UI::WindowsAndMessaging::{
    EnumChildWindows, GetClassNameW, GetWindowLongPtrW, SendMessageW, GWL_STYLE, WM_CTLCOLORBTN,
    WM_CTLCOLORDLG, WM_CTLCOLOREDIT, WM_CTLCOLORLISTBOX, WM_CTLCOLORSTATIC,
};

thread_local! {
    static DARK: Cell<bool> = const { Cell::new(false) };
    /// Labels drawn in a tone's colour rather than the text colour.
    static TONES: RefCell<Vec<(isize, Tone)>> = const { RefCell::new(Vec::new()) };
    /// The brushes of the current mode: window, panel, field.
    static BRUSHES: RefCell<Option<[isize; 3]>> = const { RefCell::new(None) };
}

/// The colours of one mode.
#[derive(Clone, Copy)]
pub struct Colors {
    /// The window and dialog background.
    pub window: COLORREF,
    /// A screen's page, the tab body.
    pub panel: COLORREF,
    /// Text boxes and lists.
    pub field: COLORREF,
    pub text: COLORREF,
    /// A pressed toolbar button, and one under the pointer.
    pub pressed: COLORREF,
    pub hover: COLORREF,
    pub disabled: COLORREF,
}

const fn rgb(r: u8, g: u8, b: u8) -> COLORREF {
    COLORREF(r as u32 | (g as u32) << 8 | (b as u32) << 16)
}

pub fn colors() -> Colors {
    if dark() {
        Colors {
            window: rgb(0x20, 0x20, 0x20),
            panel: rgb(0x2b, 0x2b, 0x2b),
            field: rgb(0x1c, 0x1c, 0x1c),
            text: rgb(0xf0, 0xf0, 0xf0),
            pressed: rgb(0x45, 0x45, 0x45),
            hover: rgb(0x38, 0x38, 0x38),
            disabled: rgb(0x80, 0x80, 0x80),
        }
    } else {
        // SAFETY: plain system colour reads.
        unsafe {
            Colors {
                window: COLORREF(GetSysColor(COLOR_BTNFACE)),
                panel: COLORREF(GetSysColor(COLOR_WINDOW)),
                field: COLORREF(GetSysColor(COLOR_WINDOW)),
                text: COLORREF(GetSysColor(COLOR_WINDOWTEXT)),
                pressed: COLORREF(GetSysColor(COLOR_BTNFACE)),
                hover: COLORREF(GetSysColor(COLOR_BTNFACE)),
                disabled: COLORREF(GetSysColor(COLOR_GRAYTEXT)),
            }
        }
    }
}

pub fn dark() -> bool {
    DARK.with(|d| d.get())
}

/// What the system (or the override) asks for now.
pub fn wanted() -> bool {
    match std::env::var("RUSTY_BACKUP_THEME").ok().as_deref() {
        Some("dark") => return true,
        Some("light") => return false,
        _ => {}
    }
    let mut value = 1u32;
    let mut size = std::mem::size_of::<u32>() as u32;
    // SAFETY: a DWORD read into a local of that size.
    let found = unsafe {
        RegGetValueW(
            HKEY_CURRENT_USER,
            w!("Software\\Microsoft\\Windows\\CurrentVersion\\Themes\\Personalize"),
            w!("AppsUseLightTheme"),
            RRF_RT_REG_DWORD,
            None,
            Some(&mut value as *mut u32 as *mut _),
            Some(&mut size),
        )
    };
    found.is_ok() && value == 0
}

/// The undocumented `uxtheme` entry points, by ordinal.
fn uxtheme(ordinal: u16) -> Option<unsafe extern "system" fn() -> isize> {
    // SAFETY: uxtheme is a system library; a missing ordinal answers None.
    unsafe {
        let lib = LoadLibraryW(w!("uxtheme.dll")).ok()?;
        GetProcAddress(lib, PCSTR(ordinal as usize as *const u8))
    }
}

/// Take the mode the system asks for; true when it changed.
pub fn adopt() -> bool {
    let dark = wanted();
    let changed = DARK.with(|d| d.replace(dark)) != dark;
    if changed {
        BRUSHES.with(|b| {
            for h in b.borrow_mut().take().into_iter().flatten() {
                // SAFETY: our own brushes, no longer handed out.
                let _ = unsafe { DeleteObject(HGDIOBJ(h as _)) };
            }
        });
    }
    // SAFETY: the ordinals' known signatures; 135 SetPreferredAppMode, 104 Refresh..., 136 FlushMenuThemes.
    unsafe {
        if let Some(f) = uxtheme(135) {
            let set: unsafe extern "system" fn(i32) -> i32 = std::mem::transmute(f);
            set(if dark { 2 } else { 3 });
        }
        if let Some(f) = uxtheme(104) {
            f();
        }
        if let Some(f) = uxtheme(136) {
            f();
        }
    }
    changed
}

fn brush(i: usize) -> HBRUSH {
    BRUSHES.with(|b| {
        let mut b = b.borrow_mut();
        let set = b.get_or_insert_with(|| {
            let c = colors();
            // SAFETY: solid brushes we own until the mode changes.
            unsafe { [c.window, c.panel, c.field].map(|col| CreateSolidBrush(col).0 as isize) }
        });
        HBRUSH(set[i] as _)
    })
}

pub fn window_brush() -> HBRUSH {
    brush(0)
}

pub fn panel_brush() -> HBRUSH {
    brush(1)
}

pub fn field_brush() -> HBRUSH {
    brush(2)
}

/// Give a top-level window the mode's title bar.
pub fn title_bar(hwnd: HWND) {
    let on = BOOL::from(dark());
    // SAFETY: a documented attribute; older systems ignore it.
    let _ = unsafe {
        DwmSetWindowAttribute(
            hwnd,
            DWMWA_USE_IMMERSIVE_DARK_MODE,
            &on as *const BOOL as *const _,
            std::mem::size_of::<BOOL>() as u32,
        )
    };
}

fn class_of(hwnd: HWND) -> String {
    let mut buf = [0u16; 64];
    // SAFETY: a plain query into a local buffer.
    let n = unsafe { GetClassNameW(hwnd, &mut buf) };
    String::from_utf16_lossy(&buf[..n.max(0) as usize])
}

/// Give one control the mode's visual style.
pub fn control(hwnd: HWND) {
    let dark = dark();
    let class = class_of(hwnd);
    // SAFETY: plain theming calls on our own controls; ordinal 133 is AllowDarkModeForWindow.
    unsafe {
        if let Some(f) = uxtheme(133) {
            let allow: unsafe extern "system" fn(HWND, BOOL) -> BOOL = std::mem::transmute(f);
            let _ = allow(hwnd, BOOL::from(dark));
        }
        let style = GetWindowLongPtrW(hwnd, GWL_STYLE) as u32 & 0xF;
        let theme: Option<PCWSTR> = match class.as_str() {
            // 2, 3, 4, 5, 6 and 9 are the tick-box and radio kinds.
            "Button" if matches!(style, 2 | 3 | 4 | 5 | 6 | 9) => {
                let _ = SetWindowSubclass(hwnd, Some(crate::darkdraw::check_proc), 2, 0);
                Some(if dark {
                    w!("DarkMode_Explorer")
                } else {
                    w!("Explorer")
                })
            }
            // Unstyled in dark mode, so its custom draw (toolbar::custom_draw) decides the colours.
            "ToolbarWindow32" => {
                if dark {
                    let _ = SetWindowTheme(hwnd, w!(""), w!(""));
                } else {
                    let _ = SetWindowTheme(hwnd, PCWSTR::null(), PCWSTR::null());
                }
                None
            }
            "Button" | "ScrollBar" | "SysListView32" | "SysTreeView32" => Some(if dark {
                w!("DarkMode_Explorer")
            } else {
                w!("Explorer")
            }),
            // 4 is ES_MULTILINE: DarkMode_CFD leaves a text box's scroll bars light, Explorer's do not.
            "Edit" if style & 4 != 0 => Some(if dark {
                w!("DarkMode_Explorer")
            } else {
                w!("Explorer")
            }),
            "Edit" | "ComboBox" => Some(if dark {
                w!("DarkMode_CFD")
            } else {
                w!("Explorer")
            }),
            _ => None,
        };
        // 3 is CBS_DROPDOWNLIST, whose face DarkMode_CFD leaves light.
        if class == "ComboBox" && style & 3 == 3 {
            let _ = SetWindowSubclass(hwnd, Some(crate::darkdraw::combo_proc), 2, 0);
            let mut info = COMBOBOXINFO {
                cbSize: std::mem::size_of::<COMBOBOXINFO>() as u32,
                ..Default::default()
            };
            if GetComboBoxInfo(hwnd, &mut info).is_ok() {
                let t = if dark {
                    w!("DarkMode_Explorer")
                } else {
                    w!("Explorer")
                };
                let _ = SetWindowTheme(info.hwndList, t, PCWSTR::null());
            }
        }
        if let Some(t) = theme {
            let _ = SetWindowTheme(hwnd, t, PCWSTR::null());
        }
        if class == "SysListView32" {
            let c = colors();
            SendMessageW(hwnd, LVM_SETBKCOLOR, None, Some(LPARAM(c.field.0 as isize)));
            SendMessageW(
                hwnd,
                LVM_SETTEXTBKCOLOR,
                None,
                Some(LPARAM(c.field.0 as isize)),
            );
            SendMessageW(
                hwnd,
                LVM_SETTEXTCOLOR,
                None,
                Some(LPARAM(c.text.0 as isize)),
            );
            let header = HWND(SendMessageW(hwnd, LVM_GETHEADER, None, None).0 as _);
            if !header.is_invalid() {
                let t = if dark {
                    w!("DarkMode_ItemsView")
                } else {
                    w!("ItemsView")
                };
                let _ = SetWindowTheme(header, t, PCWSTR::null());
                // The header's custom draw goes to the list, so the list is subclassed to answer it.
                let _ = SetWindowSubclass(hwnd, Some(list_proc), 1, 0);
            }
        }
    }
}

/// Draws a list's column titles in the mode's text colour; the header's style would not.
unsafe extern "system" fn list_proc(
    hwnd: HWND,
    msg: u32,
    wp: WPARAM,
    lp: LPARAM,
    _id: usize,
    _data: usize,
) -> LRESULT {
    use windows::Win32::UI::Controls::{
        CDDS_ITEMPREPAINT, CDDS_PREPAINT, CDRF_DODEFAULT, CDRF_NOTIFYITEMDRAW, NMCUSTOMDRAW,
        NM_CUSTOMDRAW,
    };
    use windows::Win32::UI::WindowsAndMessaging::WM_NOTIFY;
    if msg == WM_NOTIFY && dark() {
        // SAFETY: WM_NOTIFY's lParam is an NMHDR; NM_CUSTOMDRAW's is an NMCUSTOMDRAW.
        unsafe {
            let hdr = &*(lp.0 as *const windows::Win32::UI::Controls::NMHDR);
            let header = HWND(SendMessageW(hwnd, LVM_GETHEADER, None, None).0 as _);
            if hdr.code == NM_CUSTOMDRAW && hdr.hwndFrom == header {
                let cd = &*(lp.0 as *const NMCUSTOMDRAW);
                match cd.dwDrawStage {
                    CDDS_PREPAINT => return LRESULT(CDRF_NOTIFYITEMDRAW as isize),
                    CDDS_ITEMPREPAINT => {
                        SetTextColor(cd.hdc, colors().text);
                        return LRESULT(CDRF_DODEFAULT as isize);
                    }
                    _ => {}
                }
            }
        }
    }
    // SAFETY: everything else goes on to the list.
    unsafe { DefSubclassProc(hwnd, msg, wp, lp) }
}

/// Re-style `top` and every control in it, and repaint: the mode changed.
pub fn restyle(top: HWND) {
    unsafe extern "system" fn each(h: HWND, _: LPARAM) -> BOOL {
        control(h);
        BOOL::from(true)
    }
    title_bar(top);
    // SAFETY: enumerating and repainting our own windows.
    unsafe {
        let _ = EnumChildWindows(Some(top), Some(each), LPARAM(0));
        let _ = RedrawWindow(
            Some(top),
            None,
            None,
            RDW_INVALIDATE | RDW_ERASE | RDW_FRAME | RDW_ALLCHILDREN,
        );
    }
}

/// Draw label `hwnd` in `tone`'s colour, or the text colour again with `None`.
pub fn set_tone(hwnd: HWND, tone: Option<Tone>) {
    TONES.with(|t| {
        let mut t = t.borrow_mut();
        t.retain(|(h, _)| *h != hwnd.0 as isize);
        if let Some(tone) = tone {
            t.push((hwnd.0 as isize, tone));
        }
    });
}

fn tone_color(hwnd: HWND) -> Option<COLORREF> {
    let tone = TONES.with(|t| {
        t.borrow()
            .iter()
            .find(|(h, _)| *h == hwnd.0 as isize)
            .map(|(_, tone)| *tone)
    })?;
    let dark = dark();
    Some(match tone {
        Tone::Error if dark => rgb(0xff, 0x6b, 0x6b),
        Tone::Error => rgb(0xc4, 0x2b, 0x1c),
        Tone::Warning if dark => rgb(0xf0, 0xb4, 0x40),
        Tone::Warning => rgb(0x9d, 0x5d, 0x00),
        Tone::Success if dark => rgb(0x6c, 0xcb, 0x5f),
        Tone::Success => rgb(0x10, 0x7c, 0x10),
        Tone::Plain => return None,
    })
}

/// Answer a `WM_CTLCOLOR*` with the mode's colours; `page` is a screen's page, not a window.
pub fn ctl_color(msg: u32, wp: WPARAM, lp: LPARAM, page: bool) -> Option<LRESULT> {
    if !matches!(
        msg,
        WM_CTLCOLORSTATIC | WM_CTLCOLOREDIT | WM_CTLCOLORBTN | WM_CTLCOLORDLG | WM_CTLCOLORLISTBOX
    ) {
        return None;
    }
    let c = colors();
    let hdc = HDC(wp.0 as _);
    // A read-only text box asks for the static colour; it keeps a field's background.
    let field =
        msg == WM_CTLCOLOREDIT || msg == WM_CTLCOLORLISTBOX || class_of(HWND(lp.0 as _)) == "Edit";
    let (bg, br) = if field {
        (c.field, field_brush())
    } else if page {
        (c.panel, panel_brush())
    } else {
        (c.window, window_brush())
    };
    let ink = tone_color(HWND(lp.0 as _)).unwrap_or(c.text);
    // SAFETY: the DC the control handed us for this paint.
    unsafe {
        SetTextColor(hdc, ink);
        SetBkColor(hdc, bg);
    }
    Some(LRESULT(br.0 as isize))
}
