//! "Add remote daemon": the connect dialog, the connect job, the connected daemons whose drives
//! join the drive list, and the recent-address list kept in config.json.

use std::sync::{Arc, Mutex};

use super::{DriveScan, OpticalController};
use crate::controller::job_outcome;
use crate::model::job::{Job, Notify, Snapshot};
use crate::remote::connection::RemoteConnection;

type Daemon = Arc<Mutex<RemoteConnection>>;

#[derive(Default)]
pub(super) struct RemoteState {
    daemons: Vec<Daemon>,
    dialog_open: bool,
    error: Option<String>,
    connect: Option<Job<Result<Daemon, String>>>,
    recent: Vec<String>,
}

impl RemoteState {
    pub fn load() -> Self {
        RemoteState {
            recent: load_recent(),
            ..RemoteState::default()
        }
    }

    pub fn has_daemons(&self) -> bool {
        !self.daemons.is_empty()
    }

    pub fn daemons(&self) -> Vec<Daemon> {
        self.daemons.clone()
    }

    pub fn connecting(&self) -> bool {
        self.connect.is_some()
    }

    pub fn open_dialog(&mut self) {
        self.dialog_open = true;
        self.error = None;
    }

    pub fn close_dialog(&mut self) {
        self.dialog_open = false;
        self.connect = None;
        self.error = None;
    }

    pub fn connect(&mut self, addr: &str, notify: &Arc<dyn Notify>) {
        if self.connect.is_some() {
            return;
        }
        let addr = addr.trim().to_string();
        if addr.is_empty() {
            self.error = Some("Enter a host:port".to_string());
            return;
        }
        self.error = None;
        self.connect = Some(Job::spawn(
            "Connecting...",
            Arc::clone(notify),
            move |ctx| {
                ctx.set_can_cancel(false);
                Ok(RemoteConnection::connect_shared(addr.as_str()).map_err(|e| format!("{e:#}")))
            },
        ));
    }

    /// Put a connected address at the top of the recent list (in memory and in config.json).
    fn remember(&mut self, addr: &str) {
        let addr = addr.trim();
        if addr.is_empty() {
            return;
        }
        self.recent.retain(|a| a != addr);
        self.recent.insert(0, addr.to_string());
        self.recent.truncate(8);
        persist_recent(addr);
    }
}

#[cfg(any(feature = "gui", feature = "tui", feature = "tui-update"))]
fn load_recent() -> Vec<String> {
    crate::update::UpdateConfig::load().recent_daemon_addrs
}

#[cfg(not(any(feature = "gui", feature = "tui", feature = "tui-update")))]
fn load_recent() -> Vec<String> {
    Vec::new()
}

#[cfg(any(feature = "gui", feature = "tui", feature = "tui-update"))]
fn persist_recent(addr: &str) {
    let mut cfg = crate::update::UpdateConfig::load();
    cfg.remember_daemon(addr);
    let _ = cfg.save();
}

#[cfg(not(any(feature = "gui", feature = "tui", feature = "tui-update")))]
fn persist_recent(_: &str) {}

impl OpticalController {
    /// The connect dialog is open.
    pub fn add_remote_open(&self) -> bool {
        self.remote.dialog_open
    }

    /// Why the last connect failed.
    pub fn add_remote_error(&self) -> Option<&str> {
        self.remote.error.as_deref()
    }

    /// The connect runs.
    pub fn remote_connecting(&self) -> Option<Snapshot> {
        self.remote.connect.as_ref().map(|j| j.snapshot())
    }

    /// Daemon addresses connected before, newest first.
    pub fn recent_daemons(&self) -> &[String] {
        self.remote.recent.as_slice()
    }

    pub(super) fn poll_remote(&mut self) {
        let Some(done) = self.remote.connect.as_ref().and_then(job_outcome) else {
            return;
        };
        self.remote.connect = None;
        match done.and_then(|r| r) {
            Ok(conn) => {
                let label = conn
                    .lock()
                    .map(|c| c.addr().to_string())
                    .unwrap_or_default();
                self.remote.remember(label.as_str());
                self.remote.daemons.push(conn);
                self.scan_drives(DriveScan::Quiet);
                self.remote.dialog_open = false;
                self.remote.error = None;
                self.info(format!("Connected to remote daemon {label}"));
            }
            Err(e) => self.remote.error = Some(e),
        }
    }
}
