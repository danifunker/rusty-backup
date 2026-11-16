//! The vintage shell's menu bar: the AppKit shell's commands, each an item that sends its
//! controller id to the main window as its button does.

use std::cell::RefCell;
use std::ffi::CString;
use std::os::raw::{c_char, c_int};

use rusty_backup::controller::app::ids;
use rusty_backup::controller::inspect::ids as inspect_ids;
#[cfg(feature = "optical")]
use rusty_backup::controller::optical::ids as optical_ids;

use crate::screens::main_window::{screen_id, tabs, ISSUES, WEBSITE};

extern "C" {
    fn rb_host_menu_add(
        menu: *const c_char,
        label: *const c_char,
        id: *const c_char,
        key: *const c_char,
    );
    fn rb_host_menu_enable(id: *const c_char, on: c_int);
}

thread_local! {
    static IDS: RefCell<Vec<String>> = RefCell::new(Vec::new());
}

fn add(menu: &str, label: &str, id: &str, key: &str) {
    let (m, l, i, k) = (
        CString::new(menu).unwrap_or_default(),
        CString::new(label).unwrap_or_default(),
        CString::new(id).unwrap_or_default(),
        CString::new(key).unwrap_or_default(),
    );
    // SAFETY: the host copies the strings.
    unsafe { rb_host_menu_add(m.as_ptr(), l.as_ptr(), i.as_ptr(), k.as_ptr()) };
    if !id.is_empty() {
        IDS.with(|v| v.borrow_mut().push(id.to_string()));
    }
}

/// Build the menu bar; the app menu's Quit is the host's own.
pub fn build() {
    add("Rusty Backup", "About Rusty Backup", ids::ABOUT, "");
    add(
        "Rusty Backup",
        "Check for Updates...",
        ids::UPDATE_CHECK,
        "",
    );
    add("Rusty Backup", "Settings...", ids::SETTINGS, ",");
    add(
        "File",
        "Open File for Inspect...",
        inspect_ids::OPEN_FILE,
        "o",
    );
    #[cfg(feature = "optical")]
    add("File", "Open CD-ROM Image...", optical_ids::BROWSE_IMAGE, "");
    add(
        "File",
        "Open Backup Folder...",
        inspect_ids::OPEN_BACKUP,
        "",
    );
    add("File", "Refresh Devices", ids::REFRESH_DEVICES, "");
    add("File", "Close Backup", ids::CLOSE_BACKUP, "");
    for (n, t) in tabs().into_iter().enumerate() {
        let key = if n < 9 {
            (n + 1).to_string()
        } else {
            String::new()
        };
        add("View", t.label(), &screen_id(t), &key);
    }
    add("View", "Commander", ids::COMMANDER, "");
    add("View", "Copy Log", ids::LOG_COPY, "");
    add("View", "Clear Log", ids::LOG_CLEAR, "");
    add("Help", "Rusty Backup Website", WEBSITE, "");
    add("Help", "Report an Issue", ISSUES, "");
}

/// Enable each item `on` has an answer for.
pub fn sync(on: &dyn Fn(&str) -> Option<bool>) {
    let all = IDS.with(|v| v.borrow().clone());
    for id in all {
        // Open CD-ROM Image shows Optical from any screen, so it never follows the hidden button.
        #[cfg(feature = "optical")]
        if id == optical_ids::BROWSE_IMAGE {
            continue;
        }
        if let Some(enabled) = on(&id) {
            let i = CString::new(id).unwrap_or_default();
            // SAFETY: the host copies the id.
            unsafe { rb_host_menu_enable(i.as_ptr(), c_int::from(enabled)) };
        }
    }
}
