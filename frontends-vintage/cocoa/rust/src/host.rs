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

use crate::screens::host::{Column, Ctl, Ev, Host, PullItem, Surface};
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
    static HOST: Rc<VintageHost> = Rc::new(VintageHost {
        status: RefCell::new(String::new()),
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
    /// The last transient message, for the status line the main window will show.
    status: RefCell<String>,
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

    fn log(&self, lines: Vec<(LogLevel, String)>) {
        for (_, line) in &lines {
            eprintln!("{}", line);
        }
        self.log.borrow_mut().extend(lines);
    }

    fn status(&self, text: &str) {
        *self.status.borrow_mut() = text.to_string();
    }

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
