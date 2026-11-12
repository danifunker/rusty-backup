//! The AppKit shell: a pure view over `rusty_backup::controller` (docs/appkit_shell_plan.md).
//!
//! Layers and rules: CONTRIBUTING.md § Architecture policy (MVC). Nothing here names the engine
//! or the model; `scripts/check-boundaries.sh` enforces that.

#[cfg(target_os = "macos")]
mod app;
#[cfg(target_os = "macos")]
mod effects;
mod layout;
#[cfg(target_os = "macos")]
mod menu;
#[cfg(target_os = "macos")]
mod sync;
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
