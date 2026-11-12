//! The shell's [`Notify`]: a job or a worker's log line queues one tick on the main queue.

use std::sync::atomic::{AtomicBool, Ordering};

use rusty_backup::controller::app::Notify;

#[derive(Default)]
pub struct MacWake {
    pending: AtomicBool,
}

impl MacWake {
    /// The tick ran: the next wake queues again.
    pub fn ticked(&self) {
        self.pending.store(false, Ordering::Release);
    }
}

impl Notify for MacWake {
    fn wake(&self) {
        if self.pending.swap(true, Ordering::AcqRel) {
            return;
        }
        // Always queued, never run here: a job's cancel wakes from the main thread, inside a borrow.
        dispatch2::DispatchQueue::main().exec_async(crate::app::tick);
    }
}
