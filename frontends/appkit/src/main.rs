//! The AppKit shell: a pure view over `rusty_backup::controller` (docs/appkit_shell_plan.md).
//!
//! Layers and rules: CONTRIBUTING.md § Architecture policy (MVC). Nothing here names the engine
//! or the model; `scripts/check-boundaries.sh` enforces that.

#[cfg(target_os = "macos")]
mod alto;
mod app;
mod archives;
#[cfg(target_os = "macos")]
mod backup;
#[cfg(target_os = "macos")]
mod backup_remote;
mod backup_vhd;
#[cfg(target_os = "macos")]
mod browse;
mod build_disk;
mod build_rows;
#[cfg(target_os = "macos")]
mod chd;
#[cfg(target_os = "macos")]
mod commander;
mod convert;
mod disk_write;
#[cfg(target_os = "macos")]
mod editor;
mod effects;
mod expand_hfs;
mod export;
#[cfg(target_os = "macos")]
mod form;
#[cfg(target_os = "macos")]
mod inspect;
mod layout;
#[cfg(target_os = "macos")]
mod menu;
#[cfg(target_os = "macos")]
mod model_window;
mod optical;
mod optical_remote;
#[cfg(target_os = "macos")]
mod pane_view;
#[cfg(target_os = "macos")]
mod partition_bar;
#[cfg(target_os = "macos")]
mod popup;
#[cfg(target_os = "macos")]
mod remote_pick;
mod report;
mod resize;
mod resize_image;
mod restore;
#[cfg(target_os = "macos")]
mod row_icons;
#[cfg(target_os = "macos")]
mod settings;
mod sync;
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
