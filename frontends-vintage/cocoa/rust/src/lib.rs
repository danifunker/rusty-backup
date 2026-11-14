//! The vintage Cocoa shell's Rust half (docs/cocoa_vintage_shell_plan.md § 2): the shared Mac
//! screens over [`host::VintageHost`], and the C ABI the Objective-C host calls into.
//!
//! The host calls [`rb_start`] once the app has launched, [`rb_tick`] for a wake, [`rb_command`]
//! for a toolbar or menu item, [`rb_open_files`] for files the Finder hands over, and
//! `host::rb_event` and its siblings for every control's report.

include!("app.rs");
