// New fltk-based GUI implementation
mod app;
mod backup_tab;
mod filesystem_browser;
mod fs_loader;
mod inspect_tab;
mod log_panel;
mod partition_selector;
mod partition_viewer;
mod progress;
mod restore_tab;
mod vhd_config;

pub use app::RustyBackupApp;
