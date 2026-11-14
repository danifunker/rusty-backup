//! The vintage [`Host`]: every call crosses the C ABI to `host/host.m`, which builds plain Cocoa
//! views from 10.4 up. Reports come back through [`rb_event`], dispatched to the surface's handler.
//!
//! Strings go out as NUL-terminated UTF-8 the callee copies; strings coming back are `malloc`ed by
//! the host and freed here.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::path::PathBuf;
use std::rc::Rc;

use rusty_backup::controller::app::picker_start_folder;
use rusty_backup::controller::commander::Tone;
use rusty_backup::controller::log::LogLevel;
use rusty_backup::controller::{ChoiceSpec, ConfirmSpec, PasswordSpec, PickSpec, TextSpec};

use rusty_backup::controller::keys::{Key, Mods};

use crate::screens::host::{
    Align, Column, Ctl, Ev, Host, Ink, MenuEntry, Painter, Pen, PullItem, RowIcon, RowLine,
    RowsEvent, RowsSpec, Surface, ToolItem,
};
use crate::screens::layout::Rect;
use crate::screens::prompt;

extern "C" {
    fn free(p: *mut c_void);

    fn rb_host_window(title: *const c_char, w: i32, h: i32) -> u32;
    fn rb_host_window_show(s: u32);
    fn rb_host_window_hide(s: u32);
    fn rb_host_area(s: u32, out: *mut i32);
    fn rb_host_release(s: u32);
    fn rb_host_run_modal(s: u32);
    fn rb_host_stop_modal();
    fn rb_host_post();
    fn rb_host_post_after(ms: u64);

    fn rb_host_set_progress(s: u32, c: u32, running: c_int, pct: i32);
    fn rb_host_table(
        s: u32,
        c: u32,
        id: *const c_char,
        titles: *const *const c_char,
        widths: *const f64,
        right: *const u8,
        n: u32,
        checks: c_int,
    );
    fn rb_host_set_rows(s: u32, c: u32, cells: *const *const c_char, rows: u32, cols: u32);
    fn rb_host_set_checks(s: u32, c: u32, checks: *const u8, n: u32);
    fn rb_host_selected_row(s: u32, c: u32) -> i32;
    fn rb_host_select_row(s: u32, c: u32, i: u32);
    fn rb_host_toggled(s: u32, c: u32, row: *mut i32, on: *mut c_int) -> c_int;
    fn rb_host_key_equivalent(s: u32, c: u32, key: *const c_char);
    fn rb_host_tooltip(s: u32, c: u32, text: *const c_char);
    fn rb_host_page_viewport(s: u32, out: *mut i32) -> c_int;
    fn rb_host_set_surface_size(s: u32, w: i32, h: i32);
    fn rb_host_on_screen(s: u32) -> c_int;
    fn rb_host_canvas(s: u32, c: u32, id: *const c_char);
    fn rb_host_control_size(s: u32, c: u32, out: *mut f64);
    fn rb_host_rows(
        s: u32,
        c: u32,
        id: *const c_char,
        titles: *const *const c_char,
        widths: *const f64,
        right: *const u8,
        n: u32,
        header: c_int,
        multiple: c_int,
        stripes: c_int,
    );
    fn rb_host_set_row_lines(
        s: u32,
        c: u32,
        cells: *const *const c_char,
        rows: u32,
        cols: u32,
        depth: *const u32,
        icon: *const u8,
        tick: *const i8,
    );
    fn rb_host_hide_column(s: u32, c: u32, col: u32, hidden: c_int);
    fn rb_host_select_rows(s: u32, c: u32, rows: *const u32, n: u32, visible: i32);
    fn rb_host_focus(s: u32, c: u32);
    fn rb_host_page_rows(s: u32, c: u32) -> i32;
    fn rb_host_menu_at(
        s: u32,
        c: u32,
        labels: *const *const c_char,
        kinds: *const u8,
        n: u32,
        x: f64,
        y: f64,
    ) -> i32;
    fn rb_host_wrap_text_view(s: u32, c: u32, wrap: c_int);
    fn rb_host_window_frame(s: u32, out: *mut f64) -> c_int;
    fn rb_host_set_window_frame(s: u32, x: f64, y: f64, w: f64, h: f64);
    fn rb_host_pick_files(title: *const c_char) -> *mut c_char;
    fn rb_host_text_area(s: u32, c: u32, id: *const c_char, caption: *const c_char);
    fn rb_host_set_min_size(s: u32, w: i32, h: i32);
    fn rb_host_page(s: u32, c: u32) -> u32;
    fn rb_host_pane(s: u32, c: u32) -> u32;
    fn rb_host_toolbar(
        s: u32,
        ids: *const *const c_char,
        labels: *const *const c_char,
        icons: *const *const c_char,
        n: u32,
        extra: *const c_char,
    );
    fn rb_host_select_tool(s: u32, id: *const c_char);
    fn rb_host_show_toolbar(s: u32, on: c_int);
    fn rb_host_accept_drops(s: u32);
    fn rb_host_hit(s: u32, c: u32, x: f64, y: f64) -> c_int;
    fn rb_host_text_view_text(s: u32, c: u32) -> *mut c_char;
    fn rb_host_resize_window(s: u32, w: i32, h: i32);
    fn rb_host_redraw(s: u32, c: u32);
    fn rb_host_clicked(s: u32, c: u32, out: *mut f64) -> c_int;
    fn rb_host_text_view(s: u32, c: u32, id: *const c_char);
    fn rb_host_set_text_view(s: u32, c: u32, text: *const c_char);
    fn rb_host_append_text_view(s: u32, c: u32, text: *const c_char, follow: c_int);

    fn rb_pen_fill(x: f64, y: f64, w: f64, h: f64, ink: c_int, rgb: u32);
    fn rb_pen_line(x0: f64, y0: f64, x1: f64, y1: f64, ink: c_int, rgb: u32);
    fn rb_pen_frame(x: f64, y: f64, w: f64, h: f64, width: f64, ink: c_int, rgb: u32);
    fn rb_pen_text(
        x: f64,
        y: f64,
        w: f64,
        h: f64,
        text: *const c_char,
        ink: c_int,
        rgb: u32,
        center: c_int,
    );
    fn rb_pen_text_width(text: *const c_char) -> f64;

    fn rb_host_open_url(url: *const c_char);
    fn rb_host_copy_text(text: *const c_char);
    fn rb_host_quit();
    fn rb_host_pick(
        title: *const c_char,
        save: c_int,
        folder: c_int,
        file_name: *const c_char,
        exts: *const *const c_char,
        n: u32,
        start: *const c_char,
    ) -> *mut c_char;
    fn rb_host_alert(
        title: *const c_char,
        message: *const c_char,
        buttons: *const *const c_char,
        n: u32,
        critical: c_int,
        escape: i32,
    ) -> i32;

    fn rb_host_button(s: u32, c: u32, title: *const c_char, id: *const c_char);
    fn rb_host_check(s: u32, c: u32, title: *const c_char, id: *const c_char);
    fn rb_host_radio(s: u32, c: u32, group: u32, title: *const c_char, id: *const c_char);
    fn rb_host_label(s: u32, c: u32, text: *const c_char, wrap: c_int);
    fn rb_host_popup(
        s: u32,
        c: u32,
        items: *const *const c_char,
        n: u32,
        id: *const c_char,
        pulls_down: c_int,
    );
    fn rb_host_set_pulldown(
        s: u32,
        c: u32,
        title: *const c_char,
        labels: *const *const c_char,
        ctls: *const i32,
        on: *const u8,
        n: u32,
    );
    fn rb_host_field(s: u32, c: u32, id: *const c_char, caption: *const c_char, secret: c_int);
    fn rb_host_progress(s: u32, c: u32);

    fn rb_host_remove(s: u32, c: u32);
    fn rb_host_shown(s: u32, c: u32) -> c_int;
    fn rb_host_show(s: u32, c: u32, on: c_int);
    fn rb_host_enable(s: u32, c: u32, on: c_int);
    fn rb_host_checked(s: u32, c: u32) -> c_int;
    fn rb_host_set_check(s: u32, c: u32, on: c_int);
    fn rb_host_set_title(s: u32, c: u32, title: *const c_char);
    fn rb_host_set_text(s: u32, c: u32, text: *const c_char);
    fn rb_host_set_tone(s: u32, c: u32, tone: c_int);
    fn rb_host_text(s: u32, c: u32) -> *mut c_char;
    fn rb_host_typing(s: u32, c: u32) -> c_int;
    fn rb_host_picked(s: u32, c: u32) -> i32;
    fn rb_host_select(s: u32, c: u32, i: u32);
    fn rb_host_item_count(s: u32, c: u32) -> u32;
    fn rb_host_item(s: u32, c: u32, i: u32) -> *mut c_char;
    fn rb_host_set_items(s: u32, c: u32, items: *const *const c_char, n: u32);
    fn rb_host_move(s: u32, c: u32, x: i32, y: i32, w: i32, h: i32);
}

/// A C string for the call; a NUL inside (from a volume) is dropped, as `plain` does.
fn cs(s: &str) -> CString {
    CString::new(s.replace('\0', "")).unwrap_or_default()
}

/// Take a string the host `malloc`ed.
fn take(p: *mut c_char) -> String {
    if p.is_null() {
        return String::new();
    }
    // SAFETY: the host returns a NUL-terminated malloc'd buffer, freed once here.
    let s = unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned();
    unsafe { free(p as *mut c_void) };
    s
}

/// A list of C strings and the pointer array the host reads them through.
fn list(items: &[String]) -> (Vec<CString>, Vec<*const c_char>) {
    let owned: Vec<CString> = items.iter().map(|i| cs(i)).collect();
    let ptrs = owned.iter().map(|c| c.as_ptr()).collect();
    (owned, ptrs)
}

fn flag(on: bool) -> c_int {
    if on {
        1
    } else {
        0
    }
}

/// A window's close request and resize handlers.
type WindowHooks = (Surface, Rc<dyn Fn()>, Option<Rc<dyn Fn()>>);

thread_local! {
    static HANDLERS: RefCell<Vec<(Surface, Rc<dyn Fn(Ctl, Ev)>)>> = RefCell::new(Vec::new());
    static WINDOWS: RefCell<Vec<WindowHooks>> = RefCell::new(Vec::new());
    static LATER: RefCell<VecDeque<Box<dyn FnOnce()>>> = RefCell::new(VecDeque::new());
    /// Work for a later turn, in the order its delays end; each post runs the earliest due.
    static TIMED: RefCell<Vec<(std::time::Instant, Box<dyn FnOnce()>)>> = RefCell::new(Vec::new());
    static PAINTERS: RefCell<Vec<((Surface, Ctl), Painter)>> = RefCell::new(Vec::new());
    static ROWS: RefCell<Vec<((Surface, Ctl), Rc<dyn Fn(RowsEvent)>)>> = RefCell::new(Vec::new());
    static DROPS: RefCell<Vec<(Surface, Rc<dyn Fn(Vec<PathBuf>, f64, f64)>)>> = RefCell::new(Vec::new());
    static HOST: Rc<VintageHost> = Rc::new(VintageHost {
        status: RefCell::new(None),
        log: RefCell::new(Vec::new()),
    });
}

/// Window `s` asked to close (0) or was resized (1).
#[no_mangle]
pub extern "C" fn rb_window_event(s: u32, kind: u32) {
    let hook = WINDOWS.with(|w| {
        w.borrow().iter().find(|x| x.0 == s).and_then(|x| {
            if kind == 0 {
                Some(x.1.clone())
            } else {
                x.2.clone()
            }
        })
    });
    if let Some(f) = hook {
        f();
    }
}

/// The controller's name for a key a list hands over (`code` is the Mac virtual key code); `None`
/// for one it keeps. AppKit's shell maps the same codes.
fn key_of(code: u32, ch: u32) -> Option<Key> {
    Some(match code {
        36 | 76 => Key::Enter,
        53 => Key::Escape,
        51 => Key::Backspace,
        117 => Key::Delete,
        114 => Key::Insert,
        48 => Key::Tab,
        49 => Key::Space,
        123 => Key::Left,
        124 => Key::Right,
        122 => Key::F(1),
        120 => Key::F(2),
        99 => Key::F(3),
        118 => Key::F(4),
        96 => Key::F(5),
        97 => Key::F(6),
        98 => Key::F(7),
        100 => Key::F(8),
        101 => Key::F(9),
        109 => Key::F(10),
        103 => Key::F(11),
        111 => Key::F(12),
        125 | 126 | 116 | 121 | 115 | 119 => return None,
        _ => {
            let c = char::from_u32(ch).filter(|c| c.is_ascii_alphanumeric())?;
            Key::Char(c.to_ascii_lowercase())
        }
    })
}

/// What a pane list reported: 0 select, 1 tick, 2 icon, 3 activate, 4 key, 5 header, 6 menu,
/// 7 focused. `a` is the row, column or key code; `b` the modifiers (1 cmd, 2 shift, 4 ctrl,
/// 8 alt) or the key's character; `x` and `y` a menu's point.
#[no_mangle]
pub extern "C" fn rb_rows_event(
    s: u32,
    c: u32,
    kind: u32,
    a: u32,
    b: u32,
    mods: u32,
    x: f64,
    y: f64,
) {
    let f = ROWS.with(|r| {
        r.borrow()
            .iter()
            .find(|e| e.0 == (s, c))
            .map(|e| e.1.clone())
    });
    let f = match f {
        Some(f) => f,
        None => return,
    };
    let m = Mods {
        cmd: mods & 1 != 0,
        shift: mods & 2 != 0,
        ctrl: mods & 4 != 0,
        alt: mods & 8 != 0,
    };
    let e = match kind {
        0 => RowsEvent::Select {
            row: a as usize,
            cmd: m.cmd,
            shift: m.shift,
        },
        1 => RowsEvent::Tick(a as usize),
        2 => RowsEvent::Icon(a as usize),
        3 => RowsEvent::Activate,
        4 => match key_of(a, b) {
            Some(k) => RowsEvent::Key(k, m),
            None => return,
        },
        5 => RowsEvent::Header(a as usize),
        6 => RowsEvent::Menu {
            row: a as usize,
            x,
            y,
        },
        _ => RowsEvent::Focused,
    };
    f(e);
}

/// Files dropped on window `s` at `(x, y)` in window coordinates, one path per line.
#[no_mangle]
pub extern "C" fn rb_dropped(s: u32, paths: *const c_char, x: f64, y: f64) {
    if paths.is_null() {
        return;
    }
    // SAFETY: the host passes a NUL-terminated string it keeps for the call.
    let all = unsafe { CStr::from_ptr(paths) }
        .to_string_lossy()
        .into_owned();
    let list: Vec<PathBuf> = all
        .lines()
        .filter(|l| !l.is_empty())
        .map(PathBuf::from)
        .collect();
    let f = DROPS.with(|d| d.borrow().iter().find(|e| e.0 == s).map(|e| e.1.clone()));
    if let Some(f) = f {
        f(list, x, y);
    }
}

/// A canvas's `drawRect:`: run its painter with a pen over the current graphics context.
#[no_mangle]
pub extern "C" fn rb_canvas_draw(s: u32, c: u32, w: f64, h: f64, dark: c_int) {
    let painter = PAINTERS.with(|p| {
        p.borrow()
            .iter()
            .find(|x| x.0 == (s, c))
            .map(|x| x.1.clone())
    });
    if let Some(p) = painter {
        p(&VintagePen, w, h, dark != 0);
    }
}

/// Draws through `rb_pen_*`, which work only inside a canvas's `drawRect:`.
struct VintagePen;

/// An ink as the host takes it: a kind (0 a colour, 1 label, 2 separator, 3 on a fill) and 0xRRGGBB.
fn ink(i: Ink) -> (c_int, u32) {
    match i {
        Ink::Rgb(r, g, b) => (0, (u32::from(r) << 16) | (u32::from(g) << 8) | u32::from(b)),
        Ink::Label => (1, 0),
        Ink::Separator => (2, 0),
        Ink::OnFill => (3, 0),
    }
}

impl Pen for VintagePen {
    fn fill(&self, x: f64, y: f64, w: f64, h: f64, i: Ink) {
        let (k, rgb) = ink(i);
        // SAFETY: called from a painter, inside the canvas's drawRect:.
        unsafe { rb_pen_fill(x, y, w, h, k, rgb) }
    }

    fn line(&self, x0: f64, y0: f64, x1: f64, y1: f64, i: Ink) {
        let (k, rgb) = ink(i);
        // SAFETY: as for `fill`.
        unsafe { rb_pen_line(x0, y0, x1, y1, k, rgb) }
    }

    fn frame(&self, x: f64, y: f64, w: f64, h: f64, width: f64, i: Ink) {
        let (k, rgb) = ink(i);
        // SAFETY: as for `fill`.
        unsafe { rb_pen_frame(x, y, w, h, width, k, rgb) }
    }

    fn text(&self, (x, y, w, h): (f64, f64, f64, f64), text: &str, i: Ink, align: Align) {
        let (k, rgb) = ink(i);
        let t = cs(text);
        // SAFETY: as for `fill`; the host copies the text.
        unsafe { rb_pen_text(x, y, w, h, t.as_ptr(), k, rgb, flag(align == Align::Center)) }
    }

    fn text_width(&self, text: &str) -> f64 {
        let t = cs(text);
        // SAFETY: measuring only; the host copies the text.
        unsafe { rb_pen_text_width(t.as_ptr()) }
    }
}

/// A delay `after` asked for ended: run the item that is due first.
#[no_mangle]
pub extern "C" fn rb_timed() {
    let next = TIMED.with(|t| {
        let mut t = t.borrow_mut();
        let at = (0..t.len()).min_by_key(|i| t[*i].0)?;
        Some(t.remove(at).1)
    });
    if let Some(f) = next {
        f();
    }
}

/// The main queue's turn for work `later` queued: run one item, as each post queued one.
#[no_mangle]
pub extern "C" fn rb_later() {
    let next = LATER.with(|l| l.borrow_mut().pop_front());
    if let Some(f) = next {
        f();
    }
}

/// A control on surface `s` reported `kind`: act, edited, value set, selected, activated, toggled.
#[no_mangle]
pub extern "C" fn rb_event(s: u32, c: u32, kind: u32) {
    let ev = match kind {
        0 => Ev::Act,
        1 => Ev::Edited,
        2 => Ev::ValueSet,
        3 => Ev::Selected,
        4 => Ev::Activated,
        _ => Ev::Toggled,
    };
    // Taken out of the borrow first: the handler calls back into the host.
    let on = HANDLERS.with(|h| h.borrow().iter().find(|x| x.0 == s).map(|x| x.1.clone()));
    if let Some(on) = on {
        on(c, ev);
    }
}

pub struct VintageHost {
    /// Where a transient message goes: the main window's status line, once it has one.
    status: RefCell<Option<(Surface, Ctl)>>,
    /// Log lines kept until the main window's log panel takes them.
    log: RefCell<Vec<(LogLevel, String)>>,
}

impl VintageHost {
    pub fn get() -> Rc<VintageHost> {
        HOST.with(|h| h.clone())
    }

    fn dyn_host(&self) -> Rc<dyn Host> {
        Self::get()
    }

    fn alert(
        &self,
        title: &str,
        message: &str,
        buttons: &[String],
        critical: bool,
        escape: Option<usize>,
    ) -> Option<usize> {
        let (t, m) = (cs(title), cs(message));
        let (_owned, ptrs) = list(buttons);
        let esc = escape.map(|e| e as i32).unwrap_or(-1);
        // SAFETY: strings and the button array are alive for the modal call; the host copies.
        let i = unsafe {
            rb_host_alert(
                t.as_ptr(),
                m.as_ptr(),
                ptrs.as_ptr(),
                ptrs.len() as u32,
                flag(critical),
                esc,
            )
        };
        usize::try_from(i).ok()
    }
}

impl Host for VintageHost {
    fn bind(&self, s: Surface, on: Rc<dyn Fn(Ctl, Ev)>) {
        HANDLERS.with(|h| {
            let mut h = h.borrow_mut();
            h.retain(|x| x.0 != s);
            h.push((s, on));
        });
    }

    fn release(&self, s: Surface) {
        let gone = HANDLERS.with(|h| {
            let mut h = h.borrow_mut();
            let at = h.iter().position(|x| x.0 == s);
            at.map(|i| h.remove(i))
        });
        drop(gone);
        let hooks = WINDOWS.with(|w| {
            let mut w = w.borrow_mut();
            let at = w.iter().position(|x| x.0 == s);
            at.map(|i| w.remove(i))
        });
        drop(hooks);
        let painters = PAINTERS.with(|p| {
            let mut p = p.borrow_mut();
            let gone: Vec<Painter> = p
                .iter()
                .filter(|x| x.0 .0 == s)
                .map(|x| x.1.clone())
                .collect();
            p.retain(|x| x.0 .0 != s);
            gone
        });
        drop(painters);
        let rows = ROWS.with(|r| {
            let mut r = r.borrow_mut();
            let gone: Vec<Rc<dyn Fn(RowsEvent)>> = r
                .iter()
                .filter(|e| e.0 .0 == s)
                .map(|e| e.1.clone())
                .collect();
            r.retain(|e| e.0 .0 != s);
            gone
        });
        drop(rows);
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_release(s) }
    }

    fn window(&self, title: &str, w: i32, h: i32, on_close: Box<dyn Fn()>) -> Surface {
        let t = cs(title);
        // SAFETY: the host copies the title.
        let s = unsafe { rb_host_window(t.as_ptr(), w, h) };
        WINDOWS.with(|x| x.borrow_mut().push((s, Rc::from(on_close), None)));
        s
    }

    fn on_resize(&self, s: Surface, f: Box<dyn Fn()>) {
        let f: Rc<dyn Fn()> = Rc::from(f);
        WINDOWS.with(|w| {
            if let Some(x) = w.borrow_mut().iter_mut().find(|x| x.0 == s) {
                x.2 = Some(f);
            }
        });
    }

    fn present(&self, s: Surface) {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_window_show(s) }
    }

    fn hide(&self, s: Surface) {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_window_hide(s) }
    }

    fn area(&self, s: Surface) -> Rect {
        let mut out = [0i32; 4];
        // SAFETY: the host writes four integers.
        unsafe { rb_host_area(s, out.as_mut_ptr()) };
        Rect {
            x: out[0],
            y: out[1],
            w: out[2],
            h: out[3],
        }
    }

    fn run_modal(&self, s: Surface) {
        // SAFETY: a modal loop on the main thread; reports still reach Rust from inside it.
        unsafe { rb_host_run_modal(s) }
    }

    fn stop_modal(&self) {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_stop_modal() }
    }

    fn button(&self, s: Surface, c: Ctl, title: &str, id: &str) {
        let (t, i) = (cs(title), cs(id));
        // SAFETY: the host copies both strings.
        unsafe { rb_host_button(s, c, t.as_ptr(), i.as_ptr()) }
    }

    fn check(&self, s: Surface, c: Ctl, title: &str, id: &str) {
        let (t, i) = (cs(title), cs(id));
        // SAFETY: as for `button`.
        unsafe { rb_host_check(s, c, t.as_ptr(), i.as_ptr()) }
    }

    fn radio(&self, s: Surface, c: Ctl, group: usize, title: &str, id: &str) {
        let (t, i) = (cs(title), cs(id));
        // SAFETY: as for `button`.
        unsafe { rb_host_radio(s, c, group as u32, t.as_ptr(), i.as_ptr()) }
    }

    fn label(&self, s: Surface, c: Ctl, text: &str) {
        let t = cs(text);
        // SAFETY: the host copies the text.
        unsafe { rb_host_label(s, c, t.as_ptr(), 0) }
    }

    fn wrap(&self, s: Surface, c: Ctl, text: &str) {
        let t = cs(text);
        // SAFETY: the host copies the text.
        unsafe { rb_host_label(s, c, t.as_ptr(), 1) }
    }

    fn popup(&self, s: Surface, c: Ctl, items: &[String], id: &str) {
        let (_owned, ptrs) = list(items);
        let i = cs(id);
        // SAFETY: `ptrs` points into `_owned`, alive for the call; the host copies.
        unsafe { rb_host_popup(s, c, ptrs.as_ptr(), ptrs.len() as u32, i.as_ptr(), 0) }
    }

    fn pulldown(&self, s: Surface, c: Ctl, id: &str) {
        let i = cs(id);
        // SAFETY: no items; the host copies the id.
        unsafe { rb_host_popup(s, c, std::ptr::null(), 0, i.as_ptr(), 1) }
    }

    fn set_pulldown(&self, s: Surface, c: Ctl, title: &str, items: &[PullItem]) {
        let labels: Vec<String> = items.iter().map(|i| i.0.clone()).collect();
        let (_owned, ptrs) = list(&labels);
        let ctls: Vec<i32> = items
            .iter()
            .map(|i| i.1.map(|k| k as i32).unwrap_or(-1))
            .collect();
        let on: Vec<u8> = items.iter().map(|i| u8::from(i.2)).collect();
        let t = cs(title);
        // SAFETY: three arrays of `n` entries, alive for the call; the host copies.
        unsafe {
            rb_host_set_pulldown(
                s,
                c,
                t.as_ptr(),
                ptrs.as_ptr(),
                ctls.as_ptr(),
                on.as_ptr(),
                items.len() as u32,
            )
        }
    }

    fn field(&self, s: Surface, c: Ctl, id: &str, caption: &str, secret: bool) {
        let (i, cap) = (cs(id), cs(caption));
        // SAFETY: the host copies both strings.
        unsafe { rb_host_field(s, c, i.as_ptr(), cap.as_ptr(), flag(secret)) }
    }

    fn progress(&self, s: Surface, c: Ctl) {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_progress(s, c) }
    }

    fn set_progress(&self, s: Surface, c: Ctl, running: bool, pct: Option<u32>) {
        let p = pct.map(|p| p as i32).unwrap_or(-1);
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_set_progress(s, c, flag(running), p) }
    }

    fn table(&self, s: Surface, c: Ctl, id: &str, columns: &[Column], checks: bool) {
        let titles: Vec<String> = columns.iter().map(|x| x.0.clone()).collect();
        let (_owned, ptrs) = list(&titles);
        let widths: Vec<f64> = columns.iter().map(|x| x.1).collect();
        let right: Vec<u8> = columns.iter().map(|x| u8::from(x.2)).collect();
        let i = cs(id);
        // SAFETY: three arrays of `n` entries, alive for the call; the host copies.
        unsafe {
            rb_host_table(
                s,
                c,
                i.as_ptr(),
                ptrs.as_ptr(),
                widths.as_ptr(),
                right.as_ptr(),
                columns.len() as u32,
                flag(checks),
            )
        }
    }

    fn set_rows(&self, s: Surface, c: Ctl, rows: &[Vec<String>]) {
        let cols = rows.iter().map(|r| r.len()).max().unwrap_or(0);
        let mut flat: Vec<String> = Vec::with_capacity(rows.len() * cols);
        for r in rows {
            for i in 0..cols {
                flat.push(r.get(i).cloned().unwrap_or_default());
            }
        }
        let (_owned, ptrs) = list(&flat);
        // SAFETY: `rows * cols` cells, alive for the call; the host copies.
        unsafe { rb_host_set_rows(s, c, ptrs.as_ptr(), rows.len() as u32, cols as u32) }
    }

    fn set_checks(&self, s: Surface, c: Ctl, checks: &[bool]) {
        let v: Vec<u8> = checks.iter().map(|x| u8::from(*x)).collect();
        // SAFETY: `n` bytes, alive for the call.
        unsafe { rb_host_set_checks(s, c, v.as_ptr(), v.len() as u32) }
    }

    fn selected_row(&self, s: Surface, c: Ctl) -> Option<usize> {
        // SAFETY: a plain call on the main thread.
        usize::try_from(unsafe { rb_host_selected_row(s, c) }).ok()
    }

    fn select_row(&self, s: Surface, c: Ctl, i: usize) {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_select_row(s, c, i as u32) }
    }

    fn toggled(&self, s: Surface, c: Ctl) -> Option<(usize, bool)> {
        let (mut row, mut on) = (-1i32, 0 as c_int);
        // SAFETY: the host writes both when it has a toggle to report.
        let any = unsafe { rb_host_toggled(s, c, &mut row, &mut on) };
        if any == 0 {
            return None;
        }
        usize::try_from(row).ok().map(|r| (r, on != 0))
    }

    fn tooltip(&self, s: Surface, c: Ctl, text: &str) {
        let t = cs(text);
        // SAFETY: the host copies the text.
        unsafe { rb_host_tooltip(s, c, t.as_ptr()) }
    }

    fn page_viewport(&self, s: Surface) -> Option<(i32, i32)> {
        let mut out = [0i32; 2];
        // SAFETY: the host writes two integers when the surface is in a scroll view.
        let any = unsafe { rb_host_page_viewport(s, out.as_mut_ptr()) };
        if any == 0 {
            None
        } else {
            Some((out[0], out[1]))
        }
    }

    fn set_surface_size(&self, s: Surface, w: i32, h: i32) {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_set_surface_size(s, w, h) }
    }

    fn on_screen(&self, s: Surface) -> bool {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_on_screen(s) != 0 }
    }

    fn control_size(&self, s: Surface, c: Ctl) -> (f64, f64) {
        let mut out = [0f64; 2];
        // SAFETY: the host writes two numbers.
        unsafe { rb_host_control_size(s, c, out.as_mut_ptr()) };
        (out[0], out[1])
    }

    fn rows_view(&self, s: Surface, c: Ctl, id: &str, spec: &RowsSpec, on: Rc<dyn Fn(RowsEvent)>) {
        ROWS.with(|r| {
            let mut r = r.borrow_mut();
            r.retain(|e| e.0 != (s, c));
            r.push(((s, c), on));
        });
        let titles: Vec<String> = spec.columns.iter().map(|x| x.0.clone()).collect();
        let (_owned, ptrs) = list(&titles);
        let widths: Vec<f64> = spec.columns.iter().map(|x| x.1).collect();
        let right: Vec<u8> = spec.columns.iter().map(|x| u8::from(x.2)).collect();
        let i = cs(id);
        // SAFETY: three arrays of `n` entries, alive for the call; the host copies.
        unsafe {
            rb_host_rows(
                s,
                c,
                i.as_ptr(),
                ptrs.as_ptr(),
                widths.as_ptr(),
                right.as_ptr(),
                titles.len() as u32,
                flag(spec.header),
                flag(spec.multiple),
                flag(spec.stripes),
            )
        }
    }

    fn set_row_lines(&self, s: Surface, c: Ctl, lines: &[RowLine]) {
        let cols = lines.iter().map(|l| l.cells.len()).max().unwrap_or(0);
        let mut flat: Vec<String> = Vec::with_capacity(lines.len() * cols);
        for l in lines {
            for i in 0..cols {
                flat.push(l.cells.get(i).cloned().unwrap_or_default());
            }
        }
        let (_owned, ptrs) = list(&flat);
        let depth: Vec<u32> = lines.iter().map(|l| l.depth as u32).collect();
        // Icons: 0 none; 1-4 a file, 5-8 a folder (+1 expandable, +2 expanded); 9 / 10 a tree folder.
        let icon: Vec<u8> = lines
            .iter()
            .map(|l| match l.icon {
                RowIcon::None => 0,
                RowIcon::File {
                    expandable,
                    expanded,
                } => 1 + u8::from(expandable) + 2 * u8::from(expanded),
                RowIcon::Folder {
                    expandable,
                    expanded,
                } => 5 + u8::from(expandable) + 2 * u8::from(expanded),
                RowIcon::TreeFolder { open } => 9 + u8::from(open),
            })
            .collect();
        let tick: Vec<i8> = lines
            .iter()
            .map(|l| match l.tick {
                None => -1,
                Some(on) => i8::from(on),
            })
            .collect();
        // SAFETY: `rows * cols` cells and three arrays of `rows`, alive for the call.
        unsafe {
            rb_host_set_row_lines(
                s,
                c,
                ptrs.as_ptr(),
                lines.len() as u32,
                cols as u32,
                depth.as_ptr(),
                icon.as_ptr(),
                tick.as_ptr(),
            )
        }
    }

    fn hide_column(&self, s: Surface, c: Ctl, col: usize, hidden: bool) {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_hide_column(s, c, col as u32, flag(hidden)) }
    }

    fn select_rows(&self, s: Surface, c: Ctl, rows: &[usize], visible: Option<usize>) {
        let v: Vec<u32> = rows.iter().map(|r| *r as u32).collect();
        let vis = visible.map(|i| i as i32).unwrap_or(-1);
        // SAFETY: `n` rows, alive for the call.
        unsafe { rb_host_select_rows(s, c, v.as_ptr(), v.len() as u32, vis) }
    }

    fn focus(&self, s: Surface, c: Ctl) {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_focus(s, c) }
    }

    fn page_rows(&self, s: Surface, c: Ctl) -> i32 {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_page_rows(s, c) }
    }

    fn menu_at(&self, s: Surface, c: Ctl, items: &[MenuEntry], x: f64, y: f64) -> Option<usize> {
        // Flattened: kind 0 an item (1 when off, 2 when ticked), 3 a submenu, 4 its item, 5 its end.
        let mut labels = Vec::new();
        let mut kinds = Vec::new();
        for e in items {
            match e {
                MenuEntry::Item {
                    label,
                    enabled,
                    checked,
                } => {
                    labels.push(label.clone());
                    kinds.push(if !enabled {
                        1
                    } else if *checked {
                        2
                    } else {
                        0
                    });
                }
                MenuEntry::Sub { label, items } => {
                    labels.push(label.clone());
                    kinds.push(3);
                    for i in items {
                        labels.push(i.clone());
                        kinds.push(4);
                    }
                    labels.push(String::new());
                    kinds.push(5);
                }
            }
        }
        let (_owned, ptrs) = list(&labels);
        // SAFETY: two arrays of `n`, alive for the modal call.
        let i = unsafe {
            rb_host_menu_at(
                s,
                c,
                ptrs.as_ptr(),
                kinds.as_ptr(),
                kinds.len() as u32,
                x,
                y,
            )
        };
        usize::try_from(i).ok()
    }

    fn wrap_text_view(&self, s: Surface, c: Ctl, wrap: bool) {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_wrap_text_view(s, c, flag(wrap)) }
    }

    fn window_frame(&self, s: Surface) -> Option<(f64, f64, f64, f64)> {
        let mut out = [0f64; 4];
        // SAFETY: the host writes four numbers for a window's surface.
        let any = unsafe { rb_host_window_frame(s, out.as_mut_ptr()) };
        if any == 0 {
            None
        } else {
            Some((out[0], out[1], out[2], out[3]))
        }
    }

    fn set_window_frame(&self, s: Surface, (x, y, w, h): (f64, f64, f64, f64)) {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_set_window_frame(s, x, y, w, h) }
    }

    fn text_area(&self, s: Surface, c: Ctl, id: &str, caption: &str) {
        let (i, cap) = (cs(id), cs(caption));
        // SAFETY: the host copies both strings.
        unsafe { rb_host_text_area(s, c, i.as_ptr(), cap.as_ptr()) }
    }

    fn text_view_text(&self, s: Surface, c: Ctl) -> String {
        // SAFETY: the host hands over a malloc'd string.
        take(unsafe { rb_host_text_view_text(s, c) })
    }

    fn resize_window(&self, s: Surface, w: i32, h: i32) {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_resize_window(s, w, h) }
    }

    fn set_min_size(&self, s: Surface, w: i32, h: i32) {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_set_min_size(s, w, h) }
    }

    fn page(&self, s: Surface, c: Ctl) -> Surface {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_page(s, c) }
    }

    fn pane(&self, s: Surface, c: Ctl) -> Surface {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_pane(s, c) }
    }

    fn toolbar(&self, s: Surface, items: &[ToolItem], extra: &str) {
        let ids: Vec<String> = items.iter().map(|i| i.id.clone()).collect();
        let labels: Vec<String> = items.iter().map(|i| i.label.clone()).collect();
        let icons: Vec<String> = items.iter().map(|i| i.icon.clone()).collect();
        let (_a, ip) = list(&ids);
        let (_b, lp) = list(&labels);
        let (_c, cp) = list(&icons);
        let e = cs(extra);
        // SAFETY: three arrays of `n` strings, alive for the call; the host copies them.
        unsafe {
            rb_host_toolbar(
                s,
                ip.as_ptr(),
                lp.as_ptr(),
                cp.as_ptr(),
                ids.len() as u32,
                e.as_ptr(),
            )
        }
    }

    fn select_tool(&self, s: Surface, id: &str) {
        let i = cs(id);
        // SAFETY: the host copies the id.
        unsafe { rb_host_select_tool(s, i.as_ptr()) }
    }

    fn show_toolbar(&self, s: Surface, on: bool) {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_show_toolbar(s, flag(on)) }
    }

    fn sync_menu(&self, on: &dyn Fn(&str) -> Option<bool>) {
        crate::menu::sync(on);
    }

    fn on_drop(&self, s: Surface, f: Rc<dyn Fn(Vec<PathBuf>, f64, f64)>) {
        DROPS.with(|d| {
            let mut d = d.borrow_mut();
            d.retain(|e| e.0 != s);
            d.push((s, f));
        });
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_accept_drops(s) }
    }

    fn hit(&self, on: Surface, c: Ctl, x: f64, y: f64) -> bool {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_hit(on, c, x, y) != 0 }
    }

    fn status_to(&self, s: Surface, c: Ctl) {
        *self.status.borrow_mut() = Some((s, c));
    }

    fn canvas(&self, s: Surface, c: Ctl, id: &str, painter: Painter) {
        PAINTERS.with(|p| {
            let mut p = p.borrow_mut();
            p.retain(|x| x.0 != (s, c));
            p.push(((s, c), painter));
        });
        let i = cs(id);
        // SAFETY: the host copies the id.
        unsafe { rb_host_canvas(s, c, i.as_ptr()) }
    }

    fn redraw(&self, s: Surface, c: Ctl) {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_redraw(s, c) }
    }

    fn clicked(&self, s: Surface, c: Ctl) -> Option<(f64, f64, f64, f64)> {
        let mut out = [0f64; 4];
        // SAFETY: the host writes four numbers when the canvas has been clicked.
        let any = unsafe { rb_host_clicked(s, c, out.as_mut_ptr()) };
        if any == 0 {
            None
        } else {
            Some((out[0], out[1], out[2], out[3]))
        }
    }

    fn text_view(&self, s: Surface, c: Ctl, id: &str) {
        let i = cs(id);
        // SAFETY: the host copies the id.
        unsafe { rb_host_text_view(s, c, i.as_ptr()) }
    }

    fn set_text_view(&self, s: Surface, c: Ctl, text: &str) {
        let t = cs(text);
        // SAFETY: the host copies the text.
        unsafe { rb_host_set_text_view(s, c, t.as_ptr()) }
    }

    fn append_text_view(&self, s: Surface, c: Ctl, text: &str, follow: bool) {
        let t = cs(text);
        // SAFETY: the host copies the text.
        unsafe { rb_host_append_text_view(s, c, t.as_ptr(), flag(follow)) }
    }

    fn key_equivalent(&self, s: Surface, c: Ctl, key: &str) {
        let k = cs(key);
        // SAFETY: the host copies the key.
        unsafe { rb_host_key_equivalent(s, c, k.as_ptr()) }
    }

    fn remove(&self, s: Surface, c: Ctl) {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_remove(s, c) }
    }

    fn shown(&self, s: Surface, c: Ctl) -> bool {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_shown(s, c) != 0 }
    }

    fn show(&self, s: Surface, c: Ctl, on: bool) {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_show(s, c, flag(on)) }
    }

    fn enable(&self, s: Surface, c: Ctl, on: bool) {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_enable(s, c, flag(on)) }
    }

    fn checked(&self, s: Surface, c: Ctl) -> bool {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_checked(s, c) != 0 }
    }

    fn set_check(&self, s: Surface, c: Ctl, on: bool) {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_set_check(s, c, flag(on)) }
    }

    fn set_title(&self, s: Surface, c: Ctl, title: &str) {
        let t = cs(title);
        // SAFETY: the host copies the title.
        unsafe { rb_host_set_title(s, c, t.as_ptr()) }
    }

    fn set_text(&self, s: Surface, c: Ctl, text: &str) {
        let t = cs(text);
        // SAFETY: the host copies the text.
        unsafe { rb_host_set_text(s, c, t.as_ptr()) }
    }

    fn set_tone(&self, s: Surface, c: Ctl, tone: Option<Tone>) {
        let t = match tone {
            None | Some(Tone::Plain) => 0,
            Some(Tone::Warning) => 1,
            Some(Tone::Error) => 2,
            Some(Tone::Success) => 3,
        };
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_set_tone(s, c, t) }
    }

    fn text(&self, s: Surface, c: Ctl) -> String {
        // SAFETY: the host hands over a malloc'd string.
        take(unsafe { rb_host_text(s, c) })
    }

    fn typing(&self, s: Surface, c: Ctl) -> bool {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_typing(s, c) != 0 }
    }

    fn picked(&self, s: Surface, c: Ctl) -> Option<usize> {
        // SAFETY: a plain call on the main thread.
        let i = unsafe { rb_host_picked(s, c) };
        usize::try_from(i).ok()
    }

    fn select(&self, s: Surface, c: Ctl, i: usize) {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_select(s, c, i as u32) }
    }

    fn items(&self, s: Surface, c: Ctl) -> Vec<String> {
        // SAFETY: a plain call on the main thread; each item is a malloc'd string.
        let n = unsafe { rb_host_item_count(s, c) };
        (0..n)
            .map(|i| take(unsafe { rb_host_item(s, c, i) }))
            .collect()
    }

    fn set_items(&self, s: Surface, c: Ctl, items: &[String]) {
        let (_owned, ptrs) = list(items);
        // SAFETY: `ptrs` points into `_owned`, alive for the call; the host copies.
        unsafe { rb_host_set_items(s, c, ptrs.as_ptr(), ptrs.len() as u32) }
    }

    fn move_to(&self, s: Surface, c: Ctl, r: Rect) {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_move(s, c, r.x, r.y, r.w, r.h) }
    }

    fn later(&self, f: Box<dyn FnOnce()>) {
        LATER.with(|l| l.borrow_mut().push_back(f));
        // SAFETY: queues one `rb_later` on the main thread.
        unsafe { rb_host_post() }
    }

    fn after(&self, ms: u64, f: Box<dyn FnOnce()>) {
        let due = std::time::Instant::now() + std::time::Duration::from_millis(ms);
        TIMED.with(|t| t.borrow_mut().push((due, f)));
        // SAFETY: queues one `rb_timed` on the main thread `ms` from now.
        unsafe { rb_host_post_after(ms) }
    }

    fn log(&self, lines: Vec<(LogLevel, String)>) {
        let mut all = std::mem::take(&mut *self.log.borrow_mut());
        all.extend(lines);
        // Lines from before the main window opened wait for it.
        if let Some(back) = crate::append_log(all) {
            *self.log.borrow_mut() = back;
        }
    }

    fn status(&self, text: &str) {
        let at = *self.status.borrow();
        if let Some((s, c)) = at {
            self.set_text(s, c, text);
        }
    }

    fn menu_sources(&self, _recent: &[String], _devices: &[String]) {}

    fn open_url(&self, url: &str) {
        let u = cs(url);
        // SAFETY: the host copies the URL.
        unsafe { rb_host_open_url(u.as_ptr()) }
    }

    fn copy_text(&self, text: &str) {
        let t = cs(text);
        // SAFETY: the host copies the text.
        unsafe { rb_host_copy_text(t.as_ptr()) }
    }

    fn quit(&self) {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_quit() }
    }

    fn pick(&self, spec: &PickSpec, folder: bool) -> Option<PathBuf> {
        let exts: Vec<String> = spec
            .filters
            .iter()
            .flat_map(|(_, e)| e.iter())
            .map(|e| {
                e.trim_start_matches("*.")
                    .trim_start_matches('.')
                    .to_string()
            })
            .filter(|e| e != "*")
            .collect();
        let (_owned, ptrs) = list(&exts);
        let start = picker_start_folder()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default();
        let (t, f, st) = (cs(&spec.title), cs(&spec.file_name), cs(&start));
        // SAFETY: every string and the extension array live for the modal call.
        let p = unsafe {
            rb_host_pick(
                t.as_ptr(),
                flag(spec.save && !folder),
                flag(folder),
                f.as_ptr(),
                ptrs.as_ptr(),
                ptrs.len() as u32,
                st.as_ptr(),
            )
        };
        let path = take(p);
        if path.is_empty() {
            None
        } else {
            Some(PathBuf::from(path))
        }
    }

    fn pick_files(&self, title: &str) -> Vec<PathBuf> {
        let t = cs(title);
        // SAFETY: a modal panel; the host returns the paths joined by newlines, malloc'd.
        let all = take(unsafe { rb_host_pick_files(t.as_ptr()) });
        all.lines()
            .filter(|l| !l.is_empty())
            .map(PathBuf::from)
            .collect()
    }

    fn confirm(&self, spec: &ConfirmSpec) -> bool {
        let buttons = vec![spec.confirm_label.clone(), "Cancel".to_string()];
        self.alert(
            &spec.title,
            &spec.message,
            &buttons,
            spec.destructive,
            Some(1),
        ) == Some(0)
    }

    fn note(&self, title: &str, text: &str) {
        let _ = self.alert(title, text, &["OK".to_string()], false, None);
    }

    fn ask(&self, title: &str, message: &str, buttons: &[String]) -> Option<usize> {
        let mut all = buttons.to_vec();
        all.push("Cancel".to_string());
        let cancel = all.len() - 1;
        self.alert(title, message, &all, false, Some(cancel))
            .filter(|i| *i < cancel)
    }

    fn choose(&self, spec: &ChoiceSpec) -> usize {
        let buttons: Vec<String> = spec.choices.iter().map(|c| c.label.clone()).collect();
        let esc = if spec.dismiss > 0 {
            Some(spec.dismiss)
        } else {
            None
        };
        match self.alert(&spec.title, &spec.message, &buttons, false, esc) {
            Some(i) if i < spec.choices.len() => i,
            _ => spec.dismiss,
        }
    }

    fn ask_text(&self, spec: &TextSpec) -> Option<String> {
        let rule = spec.clone();
        let check: prompt::Check = Rc::new(move |t: &str| (rule.validator)(&rule, t));
        let ask = prompt::Ask {
            title: &spec.title,
            message: &spec.prompt,
            initial: &spec.initial,
            ok: &spec.ok_label,
            secure: false,
            check,
            first_error: spec.error.clone(),
        };
        prompt::ask(&self.dyn_host(), ask)
    }

    fn ask_password(&self, spec: &PasswordSpec) -> Option<String> {
        let ask = prompt::Ask {
            title: "Password",
            message: &spec.prompt,
            initial: "",
            ok: "OK",
            secure: true,
            check: Rc::new(|_| Ok(())),
            first_error: None,
        };
        prompt::ask(&self.dyn_host(), ask)
    }
}
