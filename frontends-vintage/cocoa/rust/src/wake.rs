//! The shell's [`Notify`]: a job or a worker's log line queues one tick on the main thread.

use std::sync::atomic::{AtomicBool, Ordering};

use rusty_backup::controller::app::Notify;

extern "C" {
    fn rb_host_wake();
}

#[derive(Default)]
pub struct VintageWake {
    pending: AtomicBool,
}

impl VintageWake {
    /// The tick ran: the next wake queues again.
    pub fn ticked(&self) {
        self.pending.store(false, Ordering::Release);
    }
}

impl Notify for VintageWake {
    fn wake(&self) {
        if self.pending.swap(true, Ordering::AcqRel) {
            return;
        }
        // Always queued, never run here: a job's cancel wakes from the main thread, inside a borrow.
        // SAFETY: the host only posts a selector to the main thread, from any thread.
        unsafe { rb_host_wake() }
    }
}
