//! The vintage Cocoa shell's Rust half (docs/cocoa_vintage_shell_plan.md § 2): the shared Mac
//! screens over [`host::VintageHost`], and the C ABI the Objective-C host calls into.
//!
//! The host calls [`rb_start`] once the app has launched, then [`host::rb_event`] for every report.

// The same source the AppKit shell mounts; see its mod.rs for why it is not a crate.
#[path = "../../../../frontends/mac-screens/src/mod.rs"]
mod screens;

mod gate;
mod host;
mod wake;

use std::os::raw::c_char;

use rusty_backup::controller::app::app_version;

/// This build's version, e.g. `0.1.0-dev`. Valid for the life of the process.
#[no_mangle]
pub extern "C" fn rb_version() -> *const c_char {
    static ONCE: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *ONCE.get_or_init(|| {
        let mut v = app_version().as_bytes().to_vec();
        v.push(0);
        Box::leak(v.into_boxed_slice()).as_ptr() as usize
    }) as *const c_char
}

/// The app has launched: open the first window.
#[no_mangle]
pub extern "C" fn rb_start() {
    gate::open();
}

/// A wake's turn on the main thread: run the controllers.
#[no_mangle]
pub extern "C" fn rb_tick() {
    gate::tick();
}
