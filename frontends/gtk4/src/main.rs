//! The GTK4 + libadwaita shell: pure view over `rusty_backup::controller`.
//!
//! Layers and rules: CONTRIBUTING.md § Architecture policy (MVC). Nothing here names the
//! engine or the model; `scripts/check-boundaries.sh` enforces that.

mod app;
mod archives;
mod backup;
mod browser;
mod build_disk;
mod chd;
mod commander;
mod convert;
mod effects;
mod form;
mod inspect;
mod keys;
mod optical;
mod progress;
mod restore;
mod settings;
mod size_row;
mod source;
mod sync;
mod theme;
mod volume;
mod wake;
mod widgets;

use adw::prelude::*;
use gtk::{gio, glib};
use rusty_backup::controller::log::capture;

pub const APP_ID: &str = "io.github.danifunker.RustyBackup";

fn main() -> glib::ExitCode {
    capture::init();
    let application = adw::Application::builder()
        .application_id(APP_ID)
        .flags(gio::ApplicationFlags::HANDLES_OPEN | gio::ApplicationFlags::NON_UNIQUE)
        .build();
    // The installed icon carries the app id's name, as the .desktop file says; GTK is up by now.
    application.connect_startup(|_| gtk::Window::set_default_icon_name(APP_ID));
    application.connect_activate(|a| app::present(a, None));
    application.connect_open(|a, files, _hint| {
        app::present(a, files.first().and_then(|f| f.path()));
    });
    application.run()
}
