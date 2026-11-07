//! GTK's [`Notify`]: a job or a worker's log line wakes the main loop, coalesced to one pending tick.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use gtk::glib;
use rusty_backup::controller::app::Notify;

#[derive(Default)]
pub struct GtkWake {
    pending: Arc<AtomicBool>,
}

impl Notify for GtkWake {
    fn wake(&self) {
        if self.pending.swap(true, Ordering::AcqRel) {
            return;
        }
        let pending = self.pending.clone();
        // Always queued: a job's cancel wakes from the main thread, inside the controller's borrow.
        glib::idle_add_once(move || {
            pending.store(false, Ordering::Release);
            crate::app::tick();
        });
    }
}
