//! The AppKit shell: a pure view over `rusty_backup::controller` (docs/appkit_shell_plan.md).
//!
//! Layers and rules: CONTRIBUTING.md § Architecture policy (MVC). Nothing here names the engine
//! or the model; `scripts/check-boundaries.sh` enforces that.

#[cfg(target_os = "macos")]
mod app;
#[cfg(target_os = "macos")]
mod canvas;
#[cfg(target_os = "macos")]
mod effects;
#[cfg(target_os = "macos")]
mod form;
#[cfg(target_os = "macos")]
// Shared with the vintage Cocoa shell (docs/cocoa_vintage_shell_plan.md); `crate::layout` stays its path.
#[path = "../../mac-screens/src/mod.rs"]
mod screens;
use screens::{layout, sync};
#[cfg(target_os = "macos")]
mod menu;
#[cfg(target_os = "macos")]
mod popup;
#[cfg(target_os = "macos")]
mod row_icons;
mod rows;
#[cfg(target_os = "macos")]
mod table;
#[cfg(target_os = "macos")]
mod toolbar;
#[cfg(target_os = "macos")]
mod ui;
#[cfg(target_os = "macos")]
mod wake;

#[cfg(target_os = "macos")]
fn main() {
    rusty_backup::controller::log::capture::init();
    app::run();
}

#[cfg(not(target_os = "macos"))]
fn main() {
    eprintln!("rusty-backup-appkit runs on macOS only.");
    let _ = layout::ROW;
}
