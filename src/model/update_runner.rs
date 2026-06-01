//! Background worker for the in-app self-update flow.
//!
//! The download + in-place replace logic lives in [`crate::update`]; this is
//! the GUI-facing runner that drives it on a worker thread and exposes a
//! pollable status, mirroring the other `model::*_runner` workers.
//!
//! Only meaningful on Windows (macOS/Linux update via DMG / AppImage), but the
//! runner itself is cross-platform — the platform gating lives at the call
//! sites and inside [`crate::update::download_and_apply_update`].

use std::sync::{Arc, Mutex};

use crate::update::{self, UpdateInfo};

/// Where the self-update worker is in its lifecycle.
#[derive(Debug, Clone, Default)]
pub enum UpdateRunState {
    /// No update operation in flight.
    #[default]
    Idle,
    /// Downloading the release asset. `total` is `None` until/unless the server
    /// reports a Content-Length.
    Downloading { downloaded: u64, total: Option<u64> },
    /// Download + in-place replace succeeded; the app should prompt the user to
    /// restart (then call [`crate::update::restart_app`]).
    Ready,
    /// The update failed; carries a human-readable reason.
    Failed(String),
}

#[derive(Default)]
pub struct UpdateRunStatus {
    pub state: UpdateRunState,
}

impl UpdateRunStatus {
    /// Fraction in `[0.0, 1.0]` while downloading, if the total is known.
    pub fn progress_fraction(&self) -> Option<f32> {
        match &self.state {
            UpdateRunState::Downloading {
                downloaded,
                total: Some(total),
            } if *total > 0 => Some((*downloaded as f32 / *total as f32).clamp(0.0, 1.0)),
            _ => None,
        }
    }
}

/// Spawn the download + apply worker. Poll `status` each frame; on
/// [`UpdateRunState::Ready`] prompt for restart.
pub fn spawn(info: UpdateInfo, status: Arc<Mutex<UpdateRunStatus>>) {
    std::thread::spawn(move || {
        if let Ok(mut s) = status.lock() {
            s.state = UpdateRunState::Downloading {
                downloaded: 0,
                total: None,
            };
        }

        let progress_status = Arc::clone(&status);
        let progress = move |downloaded: u64, total: Option<u64>| {
            if let Ok(mut s) = progress_status.lock() {
                s.state = UpdateRunState::Downloading { downloaded, total };
            }
        };

        let result = update::download_and_apply_update(&info, &progress);

        if let Ok(mut s) = status.lock() {
            s.state = match result {
                Ok(()) => UpdateRunState::Ready,
                Err(e) => UpdateRunState::Failed(e.to_string()),
            };
        }
    });
}
