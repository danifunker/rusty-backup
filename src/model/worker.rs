//! Panic-safe worker threads for the `Arc<Mutex<Status>>` pattern the GUI polls.
//!
//! A worker that panics never sets `finished`, and a panic while the lock is
//! held poisons it so a poller written as `let Ok(s) = lock() else { return }`
//! returns forever; either way the tab stays "running" until the app is
//! closed. `run_guarded` records the panic or error into the status, and
//! `lock_status` recovers a poisoned lock (the status structs are plain data,
//! so nothing in them can be left half-updated in a way that matters).
//! The guard itself is [`super::job::catch`], shared with [`super::job::Job`].

use std::sync::{Arc, Mutex, MutexGuard};

/// A status the GUI polls: it must be able to record a terminal failure.
pub trait WorkerStatus {
    fn fail(&mut self, message: String);
}

/// Lock a status even when a panicking worker poisoned it.
pub fn lock_status<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Run `body`, recording an error or a panic so the poller always sees the end.
pub fn run_guarded<S: WorkerStatus>(
    status: &Arc<Mutex<S>>,
    what: &str,
    body: impl FnOnce() -> anyhow::Result<()>,
) {
    if let Err(failure) = super::job::catch(what, body) {
        let message = match failure {
            super::job::Failure::Cancelled => super::job::Cancelled.to_string(),
            super::job::Failure::Error(message) => message,
        };
        lock_status(status).fail(message);
    }
}

/// Spawn a thread that runs `body` under [`run_guarded`].
pub fn spawn_guarded<S: WorkerStatus + Send + 'static>(
    status: Arc<Mutex<S>>,
    what: &'static str,
    body: impl FnOnce() -> anyhow::Result<()> + Send + 'static,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || run_guarded(&status, what, body))
}

/// Spawn a thread no status watches (audio playback, the update check); a panic or error is logged.
pub fn spawn_detached(
    what: &'static str,
    body: impl FnOnce() -> anyhow::Result<()> + Send + 'static,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        if let Err(super::job::Failure::Error(message)) = super::job::catch(what, body) {
            log::error!("{message}");
        }
    })
}

/// Relay a cancel from one flag to another until `done`: for a step that
/// runs with its own private progress object the GUI's Cancel never touches.
pub fn forward_cancel(
    is_cancelled: impl Fn() -> bool + Send + 'static,
    done: Arc<std::sync::atomic::AtomicBool>,
    set_cancel: impl Fn() + Send + 'static,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        while !done.load(std::sync::atomic::Ordering::Relaxed) {
            if is_cancelled() {
                set_cancel();
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    })
}

/// `impl WorkerStatus` for a status whose end is `finished = true` plus an `error` message.
#[macro_export]
macro_rules! finished_with_error {
    ($($t:ty),+ $(,)?) => {$(
        impl $crate::model::worker::WorkerStatus for $t {
            fn fail(&mut self, message: String) {
                self.error = Some(message);
                self.finished = true;
            }
        }
    )+};
}
pub use crate::finished_with_error;

impl WorkerStatus for crate::backup::BackupProgress {
    fn fail(&mut self, message: String) {
        self.error = Some(message);
        self.finished = true;
    }
}

impl WorkerStatus for crate::restore::RestoreProgress {
    fn fail(&mut self, message: String) {
        self.error = Some(message);
        self.finished = true;
    }
}

impl WorkerStatus for super::status::VhdExportStatus {
    fn fail(&mut self, message: String) {
        self.error = Some(message);
        self.finished = true;
    }
}

impl WorkerStatus for super::min_size_runner::MinSizeStatus {
    fn fail(&mut self, message: String) {
        self.error = Some(message);
        self.finished = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Probe {
        finished: bool,
        error: Option<String>,
    }

    impl WorkerStatus for Probe {
        fn fail(&mut self, message: String) {
            self.error = Some(message);
            self.finished = true;
        }
    }

    fn probe() -> Arc<Mutex<Probe>> {
        Arc::new(Mutex::new(Probe {
            finished: false,
            error: None,
        }))
    }

    struct Runner {
        finished: bool,
        error: Option<String>,
    }
    finished_with_error!(Runner);

    #[test]
    fn a_runner_status_from_the_macro_ends_failed_when_its_thread_panics() {
        let status = Arc::new(Mutex::new(Runner {
            finished: false,
            error: None,
        }));
        spawn_guarded(Arc::clone(&status), "export", || panic!("short read"))
            .join()
            .unwrap();
        let s = lock_status(&status);
        assert!(s.finished);
        assert_eq!(
            s.error.as_deref(),
            Some("export worker panicked: short read")
        );
    }

    #[test]
    fn a_panicking_worker_still_finishes_with_its_message() {
        let status = probe();
        run_guarded(&status, "probe", || panic!("boom"));
        let s = lock_status(&status);
        assert!(s.finished);
        assert_eq!(s.error.as_deref(), Some("probe worker panicked: boom"));
    }

    #[test]
    fn a_poisoned_status_is_still_readable_and_marked_failed() {
        let status = probe();
        let inner = Arc::clone(&status);
        run_guarded(&status, "probe", move || {
            let _held = inner.lock().unwrap();
            panic!("while locked");
        });
        assert!(status.is_poisoned());
        let s = lock_status(&status);
        assert!(s.finished);
        assert!(s.error.as_deref().unwrap().ends_with("while locked"));
    }

    #[test]
    fn an_error_is_recorded_and_success_leaves_the_status_alone() {
        let status = probe();
        run_guarded(&status, "probe", || Err(anyhow::anyhow!("bad")));
        assert_eq!(lock_status(&status).error.as_deref(), Some("bad"));
        let status = probe();
        run_guarded(&status, "probe", || Ok(()));
        assert!(!lock_status(&status).finished);
    }
}
