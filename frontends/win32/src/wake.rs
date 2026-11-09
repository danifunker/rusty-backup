//! The shell's [`Notify`]: a job or a worker's log line posts `WM_APP_JOB`, coalesced to one pending tick.

use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};

use rusty_backup::controller::app::Notify;
use windows::Win32::Foundation::{HWND, LPARAM, WPARAM};
use windows::Win32::UI::WindowsAndMessaging::{PostMessageW, WM_APP};

/// The message a wake posts to the main window.
pub const WM_APP_JOB: u32 = WM_APP + 1;

#[derive(Default)]
pub struct Win32Wake {
    pending: AtomicBool,
    /// The main window, as an integer: an `HWND` is not `Send`.
    window: AtomicIsize,
}

impl Win32Wake {
    pub fn set_window(&self, hwnd: HWND) {
        self.window.store(hwnd.0 as isize, Ordering::Release);
    }

    /// The tick ran: the next wake posts again.
    pub fn ticked(&self) {
        self.pending.store(false, Ordering::Release);
    }
}

impl Notify for Win32Wake {
    fn wake(&self) {
        let hwnd = self.window.load(Ordering::Acquire);
        if hwnd == 0 || self.pending.swap(true, Ordering::AcqRel) {
            return;
        }
        // Always posted, never run here: a job's cancel wakes from the UI thread, inside a borrow.
        // SAFETY: posting to a window handle is safe even if the window is gone.
        let sent = unsafe { PostMessageW(Some(HWND(hwnd as _)), WM_APP_JOB, WPARAM(0), LPARAM(0)) };
        if sent.is_err() {
            self.pending.store(false, Ordering::Release);
        }
    }
}
