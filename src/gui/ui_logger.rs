//! egui's side of the app log: the [`Notify`] that wakes egui, and start-up wiring.
//!
//! The `log` sink and the log store live in `rusty_backup::vm::log`; this module
//! only connects them to egui. See that module for why the wake is thread-gated.

use std::sync::{Arc, OnceLock};

use rusty_backup::model::job::Notify;
use rusty_backup::vm::log::capture;

/// The egui context, registered once the window exists.
static REPAINT_CTX: OnceLock<egui::Context> = OnceLock::new();

/// Wakes egui with `request_repaint`; a no-op until the window exists.
pub struct EguiWake;

impl Notify for EguiWake {
    fn wake(&self) {
        if let Some(ctx) = REPAINT_CTX.get() {
            ctx.request_repaint();
        }
    }
}

/// Register the egui context so jobs and worker-thread log calls wake the UI; call from the UI thread.
pub fn set_repaint_ctx(ctx: egui::Context) {
    let _ = REPAINT_CTX.set(ctx);
    capture::set_wake(Arc::new(EguiWake));
}

/// Install the capturing `log` sink; safe to call once at start-up.
pub fn init() {
    capture::init();
}
