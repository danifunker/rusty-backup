//! The C ABI the Objective-C host calls (docs/cocoa_vintage_shell_plan.md § 2).
//!
//! Strings Rust hands out are NUL-terminated UTF-8 owned by Rust; `'static` ones need no free.

use std::os::raw::c_char;

use rusty_backup::controller::app::{about, app_version};

/// Leaked once per process: a `'static` C string built from a `'static` Rust one.
fn c_static(s: &'static str) -> *const c_char {
    let mut v = Vec::with_capacity(s.len() + 1);
    v.extend_from_slice(s.as_bytes());
    v.push(0);
    Box::leak(v.into_boxed_slice()).as_ptr() as *const c_char
}

/// This build's version, e.g. `0.1.0-dev`. Valid for the life of the process.
#[no_mangle]
pub extern "C" fn rb_version() -> *const c_char {
    static ONCE: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *ONCE.get_or_init(|| c_static(app_version()) as usize) as *const c_char
}

/// The one-line summary the About box shows. Valid for the life of the process.
#[no_mangle]
pub extern "C" fn rb_summary() -> *const c_char {
    static ONCE: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *ONCE.get_or_init(|| c_static(about::SUMMARY) as usize) as *const c_char
}
