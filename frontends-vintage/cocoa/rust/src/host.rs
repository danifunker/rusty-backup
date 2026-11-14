//! The vintage [`Host`]: every call crosses the C ABI to `host/host.m`, which builds plain Cocoa
//! views from 10.4 up. Reports come back through [`rb_event`], dispatched to the surface's handler.
//!
//! Strings go out as NUL-terminated UTF-8 the callee copies; strings coming back are `malloc`ed by
//! the host and freed here.

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::rc::Rc;

use rusty_backup::controller::commander::Tone;

use crate::screens::host::{Ctl, Ev, Host, PullItem, Surface};
use crate::screens::layout::Rect;

extern "C" {
    fn free(p: *mut c_void);

    fn rb_host_window(title: *const c_char, w: i32, h: i32) -> u32;
    fn rb_host_window_show(s: u32);
    fn rb_host_window_close(s: u32);
    fn rb_host_area(s: u32, out: *mut i32);
    fn rb_host_release(s: u32);

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

thread_local! {
    static HANDLERS: RefCell<Vec<(Surface, Rc<dyn Fn(Ctl, Ev)>)>> = RefCell::new(Vec::new());
    static HOST: Rc<VintageHost> = Rc::new(VintageHost { _private: () });
}

/// A control on surface `s` reported `kind`: 0 act, 1 edited, 2 value set.
#[no_mangle]
pub extern "C" fn rb_event(s: u32, c: u32, kind: u32) {
    let ev = match kind {
        0 => Ev::Act,
        1 => Ev::Edited,
        _ => Ev::ValueSet,
    };
    // Taken out of the borrow first: the handler calls back into the host.
    let on = HANDLERS.with(|h| h.borrow().iter().find(|x| x.0 == s).map(|x| x.1.clone()));
    if let Some(on) = on {
        on(c, ev);
    }
}

pub struct VintageHost {
    _private: (),
}

impl VintageHost {
    pub fn get() -> Rc<VintageHost> {
        HOST.with(|h| h.clone())
    }

    /// A titled window `w` by `h` points; its content is the returned surface.
    pub fn window(&self, title: &str, w: i32, h: i32) -> Surface {
        let t = cs(title);
        // SAFETY: the host copies the title.
        unsafe { rb_host_window(t.as_ptr(), w, h) }
    }

    pub fn present(&self, s: Surface) {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_window_show(s) }
    }

    pub fn close(&self, s: Surface) {
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_window_close(s) }
    }

    /// The surface's size as a layout rectangle at the origin.
    pub fn area(&self, s: Surface) -> Rect {
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
        // SAFETY: a plain call on the main thread.
        unsafe { rb_host_release(s) }
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
}
