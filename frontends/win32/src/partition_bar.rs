//! The disk-layout bar: drawn with GDI from the controller's segments, its legend on a line below.
//! A click on a segment selects its partition; the widths and hit-testing are the controller's.

use std::cell::RefCell;
use std::rc::Rc;

use rusty_backup::controller::inspect::{
    ids, segment_at, segment_widths, LayoutSegment, SegmentKind, BAR_HEIGHT, MIN_SEGMENT_PX,
};
use windows::core::{w, PCWSTR};
use windows::Win32::Foundation::{COLORREF, HWND, LPARAM, LRESULT, RECT, WPARAM};
use windows::Win32::Graphics::Gdi::{
    BeginPaint, CreatePen, CreateSolidBrush, DeleteObject, DrawTextW, EndPaint, FillRect,
    FrameRect, InvalidateRect, LineTo, MoveToEx, SelectObject, SetBkMode, SetTextColor, DT_CENTER,
    DT_END_ELLIPSIS, DT_LEFT, DT_SINGLELINE, DT_VCENTER, HGDIOBJ, PAINTSTRUCT, PS_SOLID,
    TRANSPARENT,
};
use windows::Win32::System::LibraryLoader::GetModuleHandleW;
use windows::Win32::UI::HiDpi::GetDpiForWindow;
use windows::Win32::UI::WindowsAndMessaging::{
    CreateWindowExW, DefWindowProcW, GetClientRect, RegisterClassW, HMENU, WINDOW_EX_STYLE,
    WM_DESTROY, WM_ERASEBKGND, WM_LBUTTONDOWN, WM_PAINT, WNDCLASSW, WS_CHILD, WS_VISIBLE,
};

use crate::layout;
use crate::theme;
use crate::ui;

/// The legend line under the bar, in DIPs.
const LEGEND: i32 = 20;

/// The bar and its legend's height in DIPs.
pub const HEIGHT: i32 = BAR_HEIGHT as i32 + 6 + LEGEND;

const DARK_FILLS: [(u8, u8, u8); 8] = [
    (62, 118, 166),
    (74, 129, 52),
    (151, 103, 56),
    (170, 87, 155),
    (52, 125, 125),
    (127, 116, 46),
    (129, 100, 184),
    (172, 94, 94),
];
const LIGHT_FILLS: [(u8, u8, u8); 8] = [
    (76, 140, 196),
    (106, 182, 76),
    (196, 136, 76),
    (194, 99, 176),
    (76, 176, 176),
    (196, 180, 76),
    (148, 116, 209),
    (196, 108, 108),
];

fn rgb((r, g, b): (u8, u8, u8)) -> COLORREF {
    COLORREF(r as u32 | (g as u32) << 8 | (b as u32) << 16)
}

/// A segment's fill, the same palette as the egui and GTK shells.
fn fill(kind: SegmentKind) -> COLORREF {
    let dark = theme::dark();
    rgb(match kind {
        SegmentKind::Partition { color_index } => {
            let t = if dark { &DARK_FILLS } else { &LIGHT_FILLS };
            t[color_index % t.len()]
        }
        SegmentKind::Free if dark => (60, 60, 60),
        SegmentKind::Free => (225, 225, 228),
        SegmentKind::Dimmed if dark => (45, 45, 45),
        SegmentKind::Dimmed => (238, 238, 240),
    })
}

fn on_partition() -> COLORREF {
    rgb(if theme::dark() {
        (255, 255, 255)
    } else {
        (25, 25, 25)
    })
}

#[derive(Default)]
struct State {
    segments: Vec<LayoutSegment>,
    selected: Option<usize>,
    on_select: Option<Box<dyn Fn(Option<usize>)>>,
}

thread_local! {
    static BARS: RefCell<Vec<(isize, Rc<RefCell<State>>)>> = const { RefCell::new(Vec::new()) };
}

fn state(hwnd: HWND) -> Option<Rc<RefCell<State>>> {
    BARS.with(|b| {
        b.borrow()
            .iter()
            .find(|(h, _)| *h == hwnd.0 as isize)
            .map(|(_, s)| s.clone())
    })
}

/// A bar on `parent`; `on_select` gets the clicked segment's partition, `None` for free space.
pub fn build(parent: HWND, on_select: Box<dyn Fn(Option<usize>)>) -> HWND {
    let class = w!("RustyBackupPartitionBar");
    // SAFETY: registering our class (again is harmless) and creating a child of ours.
    let hwnd = unsafe {
        let inst = GetModuleHandleW(None).unwrap_or_default();
        let wc = WNDCLASSW {
            lpfnWndProc: Some(proc),
            hInstance: inst.into(),
            lpszClassName: class,
            ..Default::default()
        };
        RegisterClassW(&wc);
        CreateWindowExW(
            WINDOW_EX_STYLE(0),
            class,
            PCWSTR::null(),
            WS_CHILD | WS_VISIBLE,
            0,
            0,
            0,
            0,
            Some(parent),
            Some(HMENU(std::ptr::null_mut())),
            Some(inst.into()),
            None,
        )
    }
    .expect("the partition bar");
    ui::set_automation_id(hwnd, ids::PARTITION_BAR);
    let s = State {
        on_select: Some(on_select),
        ..State::default()
    };
    BARS.with(|b| {
        b.borrow_mut()
            .push((hwnd.0 as isize, Rc::new(RefCell::new(s))))
    });
    hwnd
}

/// Show `segments` with `selected` outlined; repaints only on a change.
pub fn show(hwnd: HWND, segments: Vec<LayoutSegment>, selected: Option<usize>) {
    let Some(s) = state(hwnd) else { return };
    let mut s = s.borrow_mut();
    if s.segments == segments && s.selected == selected {
        return;
    }
    s.segments = segments;
    s.selected = selected;
    // SAFETY: invalidating our own window.
    unsafe {
        let _ = InvalidateRect(Some(hwnd), None, true);
    }
}

fn paint(hwnd: HWND, s: &State) {
    let mut ps = PAINTSTRUCT::default();
    // SAFETY: the paint cycle of our own window; every GDI object made here is freed here.
    unsafe {
        let hdc = BeginPaint(hwnd, &mut ps);
        let mut rc = RECT::default();
        let _ = GetClientRect(hwnd, &mut rc);
        FillRect(hdc, &rc, theme::panel_brush());
        let dpi = GetDpiForWindow(hwnd).max(96);
        let bar_h = layout::scale(BAR_HEIGHT as i32, dpi);
        let old_font = SelectObject(hdc, HGDIOBJ(ui::font_at(dpi).0));
        SetBkMode(hdc, TRANSPARENT);
        let widths = segment_widths(
            &s.segments,
            rc.right as f32,
            MIN_SEGMENT_PX * dpi as f32 / 96.0,
        );
        let line = CreatePen(PS_SOLID, 1, theme::colors().disabled);
        let old_pen = SelectObject(hdc, HGDIOBJ(line.0));
        let mut x = 0f32;
        let mut picked = None;
        for (seg, w) in s.segments.iter().zip(&widths) {
            let r = RECT {
                left: x as i32,
                top: 0,
                right: (x + w) as i32,
                bottom: bar_h,
            };
            let brush = CreateSolidBrush(fill(seg.kind));
            FillRect(hdc, &r, brush);
            let _ = DeleteObject(HGDIOBJ(brush.0));
            if let Some(text) = seg.inline_label(*w * 96.0 / dpi as f32) {
                SetTextColor(hdc, on_partition());
                let mut t: Vec<u16> = ui::plain(&text).encode_utf16().collect();
                let mut tr = RECT {
                    left: r.left + 4,
                    right: r.right - 4,
                    ..r
                };
                DrawTextW(
                    hdc,
                    &mut t,
                    &mut tr,
                    DT_CENTER | DT_VCENTER | DT_SINGLELINE | DT_END_ELLIPSIS,
                );
            }
            if seg.partition.is_some() && seg.partition == s.selected {
                picked = Some(r);
            }
            x += w;
            let _ = MoveToEx(hdc, x as i32, 0, None);
            let _ = LineTo(hdc, x as i32, bar_h);
        }
        if let Some(r) = picked {
            let ink = CreateSolidBrush(theme::colors().text);
            for i in 0..layout::scale(3, dpi) {
                let f = RECT {
                    left: r.left + i,
                    top: i,
                    right: r.right - i,
                    bottom: r.bottom - i,
                };
                FrameRect(hdc, &f, ink);
            }
            let _ = DeleteObject(HGDIOBJ(ink.0));
        }
        // The legend: a swatch and a name per segment, on one line under the bar.
        let top = bar_h + layout::scale(6, dpi);
        let sw = layout::scale(12, dpi);
        let mut lx = 0;
        SetTextColor(hdc, theme::colors().text);
        for seg in &s.segments {
            let brush = CreateSolidBrush(fill(seg.kind));
            let swatch = RECT {
                left: lx,
                top: top + layout::scale(4, dpi),
                right: lx + sw,
                bottom: top + layout::scale(4, dpi) + sw,
            };
            FillRect(hdc, &swatch, brush);
            let _ = DeleteObject(HGDIOBJ(brush.0));
            let mut t: Vec<u16> = ui::plain(&seg.legend_text()).encode_utf16().collect();
            let mut tr = RECT {
                left: lx + sw + layout::scale(6, dpi),
                top,
                right: rc.right,
                bottom: top + layout::scale(LEGEND, dpi),
            };
            let mut measure = tr;
            DrawTextW(
                hdc,
                &mut t,
                &mut measure,
                DT_LEFT | DT_SINGLELINE | DT_VCENTER | windows::Win32::Graphics::Gdi::DT_CALCRECT,
            );
            DrawTextW(
                hdc,
                &mut t,
                &mut tr,
                DT_LEFT | DT_SINGLELINE | DT_VCENTER | DT_END_ELLIPSIS,
            );
            lx = measure.right + layout::scale(16, dpi);
            if lx >= rc.right {
                break;
            }
        }
        SelectObject(hdc, old_pen);
        let _ = DeleteObject(HGDIOBJ(line.0));
        SelectObject(hdc, old_font);
        let _ = EndPaint(hwnd, &ps);
    }
}

unsafe extern "system" fn proc(hwnd: HWND, msg: u32, wp: WPARAM, lp: LPARAM) -> LRESULT {
    match msg {
        WM_PAINT => {
            if let Some(s) = state(hwnd) {
                paint(hwnd, &s.borrow());
            }
            LRESULT(0)
        }
        WM_ERASEBKGND => LRESULT(1),
        WM_LBUTTONDOWN => {
            let x = (lp.0 & 0xffff) as i16 as f32;
            let Some(s) = state(hwnd) else {
                return LRESULT(0);
            };
            let picked = {
                let s = s.borrow();
                let mut rc = RECT::default();
                // SAFETY: plain queries of our own window.
                let dpi = unsafe {
                    let _ = GetClientRect(hwnd, &mut rc);
                    GetDpiForWindow(hwnd).max(96)
                };
                let widths = segment_widths(
                    &s.segments,
                    rc.right as f32,
                    MIN_SEGMENT_PX * dpi as f32 / 96.0,
                );
                segment_at(&widths, x)
                    .and_then(|i| s.segments.get(i))
                    .map(|seg| seg.partition)
            };
            if let Some(partition) = picked {
                // Taken out while it runs: the answer refreshes the screen, which shows the bar again.
                let call = s.borrow_mut().on_select.take();
                if let Some(f) = &call {
                    f(partition);
                }
                s.borrow_mut().on_select = call;
            }
            LRESULT(0)
        }
        WM_DESTROY => {
            BARS.with(|b| b.borrow_mut().retain(|(h, _)| *h != hwnd.0 as isize));
            LRESULT(0)
        }
        // SAFETY: the default handling.
        _ => unsafe { DefWindowProcW(hwnd, msg, wp, lp) },
    }
}
