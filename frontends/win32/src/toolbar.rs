//! The screen switcher (D9): a toolbar of labelled icons, one pressed at a time.
//!
//! The icons are `assets/icons/toolbar/png/*.png`, embedded and turned into icons at the size
//! nearest the window's DPI; `scripts/render-toolbar-icons.ps1` renders them from the SVGs.

use windows::core::w;
use windows::Win32::Foundation::{HWND, LPARAM, WPARAM};
use windows::Win32::UI::Controls::{
    ImageList_Create, ImageList_Destroy, ImageList_ReplaceIcon, BTNS_BUTTON, BTNS_CHECK,
    BTNS_GROUP, CCS_NODIVIDER, CCS_NOPARENTALIGN, CCS_NORESIZE, HIMAGELIST, ILC_COLOR32, TBBUTTON,
    TBSTATE_ENABLED, TBSTYLE_FLAT, TB_ADDBUTTONSW, TB_ADDSTRINGW, TB_BUTTONSTRUCTSIZE,
    TB_CHECKBUTTON, TB_GETBUTTONSIZE, TB_ISBUTTONCHECKED, TB_SETBUTTONWIDTH, TB_SETIMAGELIST,
    TB_SETMAXTEXTROWS,
};
use windows::Win32::UI::HiDpi::GetDpiForWindow;
use windows::Win32::UI::WindowsAndMessaging::{
    CreateIconFromResourceEx, DestroyIcon, SendMessageW, LR_DEFAULTCOLOR, WINDOW_EX_STYLE,
    WINDOW_STYLE,
};

use crate::layout;
use crate::ui;

macro_rules! sizes {
    ($set:literal, $n:literal) => {
        [
            (
                24,
                include_bytes!(concat!(
                    "../../../assets/icons/toolbar/",
                    $set,
                    "/",
                    $n,
                    "-24.png"
                ))
                .as_slice(),
            ),
            (
                32,
                include_bytes!(concat!(
                    "../../../assets/icons/toolbar/",
                    $set,
                    "/",
                    $n,
                    "-32.png"
                ))
                .as_slice(),
            ),
            (
                48,
                include_bytes!(concat!(
                    "../../../assets/icons/toolbar/",
                    $set,
                    "/",
                    $n,
                    "-48.png"
                ))
                .as_slice(),
            ),
        ]
    };
    // White icons on a dark theme, black on a light one.
    ($n:literal) => {
        if crate::theme::dark() {
            sizes!("png-dark", $n)
        } else {
            sizes!("png", $n)
        }
    };
}

/// The PNGs of icon `name` (an `AppTab::icon()` stem, or "commander"), smallest first.
fn pngs(name: &str) -> [(i32, &'static [u8]); 3] {
    match name {
        "inspect" => sizes!("inspect"),
        "backup" => sizes!("backup"),
        "restore" => sizes!("restore"),
        "build" => sizes!("build"),
        "convert" => sizes!("convert"),
        "disc" => sizes!("disc"),
        "archive" => sizes!("archive"),
        _ => sizes!("commander"),
    }
}

/// The toolbar's icon edge in pixels: 24 DIPs at the window's DPI.
fn icon_px(dpi: u32) -> i32 {
    layout::scale(24, dpi)
}

/// A toolbar on `parent` with one button per `(id, label, icon)`; `check` makes them a group.
pub fn build(parent: HWND, id: &str, buttons: &[(i32, &str, &str)], check: bool) -> HWND {
    let style =
        TBSTYLE_FLAT | CCS_NODIVIDER as u32 | CCS_NORESIZE as u32 | CCS_NOPARENTALIGN as u32;
    let tb = ui::child(
        parent,
        w!("ToolbarWindow32"),
        "",
        WINDOW_STYLE(style),
        WINDOW_EX_STYLE(0),
        id,
    );
    // SAFETY: plain toolbar setup; the label pool is copied by TB_ADDSTRINGW.
    unsafe {
        SendMessageW(
            tb,
            TB_BUTTONSTRUCTSIZE,
            Some(WPARAM(std::mem::size_of::<TBBUTTON>())),
            None,
        );
        SendMessageW(tb, TB_SETMAXTEXTROWS, Some(WPARAM(1)), None);
        let dpi = GetDpiForWindow(parent).max(96);
        set_icons(tb, buttons.iter().map(|b| b.2), dpi);
        let style = (if check {
            BTNS_CHECK | BTNS_GROUP
        } else {
            BTNS_BUTTON
        }) as u8;
        let mut list: Vec<TBBUTTON> = Vec::new();
        for (i, (cmd, label, _)) in buttons.iter().enumerate() {
            let text: Vec<u16> = label.encode_utf16().chain([0, 0]).collect();
            let at = SendMessageW(
                tb,
                TB_ADDSTRINGW,
                Some(WPARAM(0)),
                Some(LPARAM(text.as_ptr() as isize)),
            )
            .0;
            list.push(TBBUTTON {
                iBitmap: i as i32,
                idCommand: *cmd,
                fsState: TBSTATE_ENABLED as u8,
                fsStyle: style,
                iString: at,
                ..Default::default()
            });
        }
        SendMessageW(
            tb,
            TB_ADDBUTTONSW,
            Some(WPARAM(list.len())),
            Some(LPARAM(list.as_ptr() as isize)),
        );
        set_width(tb, dpi);
    }
    tb
}

/// Give `tb` its icons at `dpi`; the old image list is freed.
pub fn set_icons<'a>(tb: HWND, names: impl Iterator<Item = &'a str>, dpi: u32) {
    let px = icon_px(dpi);
    // SAFETY: the image list belongs to the toolbar until replaced; each icon is copied in.
    unsafe {
        let il: HIMAGELIST = ImageList_Create(px, px, ILC_COLOR32, 8, 0);
        for name in names {
            let set = pngs(name);
            let (_, bytes) = set
                .iter()
                .find(|(s, _)| *s >= px)
                .copied()
                .unwrap_or(set[2]);
            if let Ok(icon) =
                CreateIconFromResourceEx(bytes, true, 0x0003_0000, px, px, LR_DEFAULTCOLOR)
            {
                ImageList_ReplaceIcon(il, -1, icon);
                let _ = DestroyIcon(icon);
            }
        }
        let old = SendMessageW(
            tb,
            TB_SETIMAGELIST,
            Some(WPARAM(0)),
            Some(LPARAM(il.0 as isize)),
        )
        .0;
        set_width(tb, dpi);
        if old != 0 {
            let _ = ImageList_Destroy(Some(HIMAGELIST(old as _)));
        }
    }
}

/// Equal-width buttons read as one row of places, as in the mockup.
fn set_width(tb: HWND, dpi: u32) {
    let (lo, hi) = (
        layout::scale(76, dpi) as isize,
        layout::scale(120, dpi) as isize,
    );
    // SAFETY: a plain toolbar message.
    unsafe {
        SendMessageW(tb, TB_SETBUTTONWIDTH, None, Some(LPARAM(lo | (hi << 16))));
    }
}

/// The toolbar's height in DIPs, from its button size.
pub fn height(tb: HWND, dpi: u32) -> i32 {
    // SAFETY: a plain toolbar query.
    let size = unsafe { SendMessageW(tb, TB_GETBUTTONSIZE, None, None).0 } as u32;
    let px = (size >> 16) as i32;
    px * 96 / dpi.max(96) as i32 + 2
}

/// Dark mode draws the toolbar itself (`NM_CUSTOMDRAW`): a visual style keeps its own text colour.
pub fn custom_draw(lp: LPARAM) -> windows::Win32::Foundation::LRESULT {
    use windows::Win32::Graphics::Gdi::{CreateSolidBrush, DeleteObject, FillRect, HGDIOBJ};
    use windows::Win32::UI::Controls::{
        CDDS_ITEMPREPAINT, CDDS_PREPAINT, CDIS_CHECKED, CDIS_DISABLED, CDIS_HOT, CDRF_DODEFAULT,
        CDRF_NOTIFYITEMDRAW, NMTBCUSTOMDRAW, TBCDRF_NOBACKGROUND, TBCDRF_NOEDGES,
        TBCDRF_USECDCOLORS,
    };
    // SAFETY: NM_CUSTOMDRAW from a toolbar comes with an NMTBCUSTOMDRAW.
    let cd = unsafe { &mut *(lp.0 as *mut NMTBCUSTOMDRAW) };
    let r = match cd.nmcd.dwDrawStage {
        CDDS_PREPAINT if crate::theme::dark() => CDRF_NOTIFYITEMDRAW,
        CDDS_ITEMPREPAINT if crate::theme::dark() => {
            let state = cd.nmcd.uItemState.0;
            let fill = if state & CDIS_CHECKED.0 != 0 {
                Some(crate::theme::colors().pressed)
            } else if state & CDIS_HOT.0 != 0 {
                Some(crate::theme::colors().hover)
            } else {
                None
            };
            if let Some(c) = fill {
                // SAFETY: the DC and rectangle the toolbar handed us for this button.
                unsafe {
                    let brush = CreateSolidBrush(c);
                    FillRect(cd.nmcd.hdc, &cd.nmcd.rc, brush);
                    let _ = DeleteObject(HGDIOBJ(brush.0));
                }
            }
            cd.clrText = if state & CDIS_DISABLED.0 != 0 {
                crate::theme::colors().disabled
            } else {
                crate::theme::colors().text
            };
            CDRF_DODEFAULT | TBCDRF_USECDCOLORS | TBCDRF_NOBACKGROUND | TBCDRF_NOEDGES
        }
        _ => CDRF_DODEFAULT,
    };
    windows::Win32::Foundation::LRESULT(r as isize)
}

/// Press button `cmd` (and so release the rest of its group), if it is not pressed already.
pub fn check(tb: HWND, cmd: i32) {
    // SAFETY: plain toolbar messages; a programmatic check sends no WM_COMMAND.
    unsafe {
        if SendMessageW(tb, TB_ISBUTTONCHECKED, Some(WPARAM(cmd as usize)), None).0 == 0 {
            SendMessageW(
                tb,
                TB_CHECKBUTTON,
                Some(WPARAM(cmd as usize)),
                Some(LPARAM(1)),
            );
        }
    }
}
