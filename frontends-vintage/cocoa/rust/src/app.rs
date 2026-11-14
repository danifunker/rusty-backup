// The shell's Rust half, included by both crate roots: lib.rs (the 10.7 staticlib) and
// ppc_main.rs (the PowerPC binary, whose main hands over to the Objective-C host).

// The same source the AppKit shell mounts; see its mod.rs for why it is not a crate.
#[path = "../../../../frontends/mac-screens/src/mod.rs"]
mod screens;

mod host;
mod menu;
mod wake;

use std::cell::RefCell;
use std::ffi::CStr;
use std::os::raw::c_char;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use rusty_backup::controller::app::app_version;
use rusty_backup::controller::log::{capture, LogLevel};

use crate::host::VintageHost;
use crate::screens::main_window::MainWindow;
use crate::wake::VintageWake;

thread_local! {
    static APP: RefCell<Option<Rc<MainWindow>>> = RefCell::new(None);
}

fn app() -> Option<Rc<MainWindow>> {
    APP.with(|a| a.borrow().clone())
}

/// Add lines to the main window's log; handed back when it is not open yet.
pub(crate) fn append_log(lines: Vec<(LogLevel, String)>) -> Option<Vec<(LogLevel, String)>> {
    match app() {
        Some(app) => {
            app.append_log(lines);
            None
        }
        None => Some(lines),
    }
}

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

/// The app has launched: build the menu bar and open the main window.
#[no_mangle]
pub extern "C" fn rb_start() {
    let wake = Arc::new(VintageWake::default());
    capture::set_wake(wake.clone());
    menu::build();
    let ticked = wake.clone();
    let app = MainWindow::open(
        VintageHost::get(),
        "Rusty Backup",
        wake,
        Box::new(move || ticked.ticked()),
    );
    APP.with(|a| *a.borrow_mut() = Some(app.clone()));
    let named: Vec<PathBuf> = std::env::args_os()
        .skip(1)
        .filter(|a| !a.to_string_lossy().starts_with("-psn_"))
        .map(PathBuf::from)
        .collect();
    app.open_files(named);
}

/// A wake's turn on the main thread: run the controllers.
#[no_mangle]
pub extern "C" fn rb_tick() {
    if let Some(app) = app() {
        app.tick();
    }
}

/// A toolbar or menu item, by its controller id.
#[no_mangle]
pub extern "C" fn rb_command(id: *const c_char) {
    if id.is_null() {
        return;
    }
    // SAFETY: the host passes a NUL-terminated string it keeps for the call.
    let id = unsafe { CStr::from_ptr(id) }.to_string_lossy().into_owned();
    if let Some(app) = app() {
        app.command(&id);
    }
}

/// Files the Finder asks this app to open, one path per line.
#[no_mangle]
pub extern "C" fn rb_open_files(paths: *const c_char) {
    if paths.is_null() {
        return;
    }
    // SAFETY: as for `rb_command`.
    let all = unsafe { CStr::from_ptr(paths) }
        .to_string_lossy()
        .into_owned();
    let list: Vec<PathBuf> = all
        .lines()
        .filter(|l| !l.is_empty())
        .map(PathBuf::from)
        .collect();
    if let Some(app) = app() {
        app.open_files(list);
    }
}
