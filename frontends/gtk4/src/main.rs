//! The GTK4 + libadwaita shell: pure view over `rusty_backup::controller`.
//!
//! Layers and rules: CONTRIBUTING.md § Architecture policy (MVC). Nothing here names the
//! engine or the model; `scripts/check-boundaries.sh` enforces that.

mod app;
mod effects;
mod inspect;
mod progress;
mod sync;
mod theme;
mod wake;
mod widgets;

use adw::prelude::*;
use gtk::{gio, glib};
use rusty_backup::controller::log::capture;

const APP_ID: &str = "io.github.danifunker.RustyBackup";

fn main() -> glib::ExitCode {
    capture::init();
    let application = adw::Application::builder()
        .application_id(APP_ID)
        .flags(gio::ApplicationFlags::HANDLES_OPEN | gio::ApplicationFlags::NON_UNIQUE)
        .build();
    application.connect_activate(|a| app::present(a, None));
    application.connect_open(|a, files, _hint| {
        app::present(a, files.first().and_then(|f| f.path()));
    });
    application.run()
}
