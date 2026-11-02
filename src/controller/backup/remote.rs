//! A drive on an `rb-cli serve` daemon as the backup source: connect, list, pick, load.

#[cfg(feature = "remote")]
use std::sync::{Arc, Mutex};

use super::{BackupController, RemoteDrive};
#[cfg(feature = "remote")]
use crate::model::backup_remote::RemoteSourceInfo;
#[cfg(feature = "remote")]
use crate::model::job::Job;
#[cfg(feature = "remote")]
use crate::remote::protocol::WireDevice;
#[cfg(feature = "remote")]
use crate::remote::RemoteConnection;

#[cfg(feature = "remote")]
type Conn = Arc<Mutex<RemoteConnection>>;

#[cfg(feature = "remote")]
enum Outcome {
    Connected(Conn, Vec<WireDevice>),
    Loaded(RemoteSourceInfo),
}

#[derive(Default)]
pub(super) struct RemoteState {
    /// The connect window is open.
    pub dialog: bool,
    /// The window's host:port field.
    pub address: String,
    pub error: Option<String>,
    #[cfg(feature = "remote")]
    conn: Option<Conn>,
    /// The address connected to, for the source label.
    #[cfg(feature = "remote")]
    addr: Option<String>,
    #[cfg(feature = "remote")]
    devices: Vec<WireDevice>,
    /// The picked drive: its daemon-side path and the `rb://` label.
    #[cfg(feature = "remote")]
    selected: Option<(String, String)>,
    #[cfg(feature = "remote")]
    job: Option<Job<Outcome>>,
}

impl RemoteState {
    pub fn busy(&self) -> bool {
        #[cfg(feature = "remote")]
        {
            self.job.is_some()
        }
        #[cfg(not(feature = "remote"))]
        {
            false
        }
    }

    /// The connection and the picked drive's path, for a minimum-size walk.
    #[cfg(feature = "remote")]
    pub fn source(&self) -> Option<(Conn, String)> {
        let conn = self.conn.clone()?;
        let (path, _) = self.selected.as_ref()?;
        Some((conn, path.clone()))
    }

    /// The connection, the drive's path and its label, for the backup.
    #[cfg(feature = "remote")]
    pub fn selection(&self) -> Option<(Conn, String, String)> {
        let conn = self.conn.clone()?;
        let (path, display) = self.selected.clone()?;
        Some((conn, path, display))
    }
}

impl BackupController {
    /// A drive on a daemon is the source; the local source machinery stands aside.
    pub fn remote_active(&self) -> bool {
        #[cfg(feature = "remote")]
        {
            self.remote.selected.is_some()
        }
        #[cfg(not(feature = "remote"))]
        {
            false
        }
    }

    pub(super) fn remote_connected(&self) -> bool {
        #[cfg(feature = "remote")]
        {
            self.remote.conn.is_some()
        }
        #[cfg(not(feature = "remote"))]
        {
            false
        }
    }

    /// The source label's "rb://host:port/path", while a remote drive is the source.
    pub fn remote_label(&self) -> Option<&str> {
        #[cfg(feature = "remote")]
        {
            self.remote.selected.as_ref().map(|(_, d)| d.as_str())
        }
        #[cfg(not(feature = "remote"))]
        {
            None
        }
    }

    pub fn remote_dialog_open(&self) -> bool {
        self.remote.dialog
    }

    pub fn remote_address(&self) -> &str {
        self.remote.address.as_str()
    }

    pub fn remote_busy(&self) -> bool {
        self.remote.busy()
    }

    pub fn remote_error(&self) -> Option<&str> {
        self.remote.error.as_deref()
    }

    /// Connected: the window lists the daemon's drives.
    pub fn remote_listed(&self) -> bool {
        self.remote_connected()
    }

    pub fn remote_drives(&self) -> Vec<RemoteDrive> {
        #[cfg(feature = "remote")]
        {
            self.remote
                .devices
                .iter()
                .map(|d| {
                    let mut label =
                        format!("{} ({})", d.path, disk_core::display::format_size(d.size_bytes));
                    if !d.media.is_empty() {
                        label.push_str(format!(" - {}", d.media).as_str());
                    }
                    if d.is_removable {
                        label.push_str(" [removable]");
                    }
                    RemoteDrive {
                        path: d.path.clone(),
                        size_bytes: d.size_bytes,
                        label,
                    }
                })
                .collect()
        }
        #[cfg(not(feature = "remote"))]
        {
            Vec::new()
        }
    }

    pub(super) fn remote_open(&mut self) {
        self.remote.dialog = true;
        #[cfg(feature = "remote")]
        {
            self.remote.address = self.remote.addr.clone().unwrap_or_default();
        }
    }

    #[cfg(feature = "remote")]
    pub(super) fn remote_connect(&mut self) {
        if self.remote.busy() {
            return;
        }
        let a = self.remote.address.trim().to_string();
        if a.is_empty() {
            return;
        }
        let addr = if a.contains(':') { a } else { format!("{a}:7341") };
        self.remote.error = None;
        self.remote.addr = Some(addr.clone());
        self.remote.job = Some(self.spawn("Connecting", move |_| {
            let (conn, devices) =
                crate::model::backup_remote::connect_and_list_devices(addr.as_str())?;
            Ok(Outcome::Connected(conn, devices))
        }));
    }

    #[cfg(not(feature = "remote"))]
    pub(super) fn remote_connect(&mut self) {}

    #[cfg(feature = "remote")]
    pub(super) fn remote_pick(&mut self, path: String, size: u64) {
        self.remote.dialog = false;
        let Some(conn) = self.remote.conn.clone() else {
            return;
        };
        // The remote drive replaces the local source.
        self.source = None;
        self.src.drop_decode();
        self.run.pending_start = false;
        self.clear_found();
        let addr = self.remote.addr.clone().unwrap_or_default();
        let display = format!("rb://{addr}{path}");
        self.name = crate::backup::format::generate_backup_name(
            std::path::Path::new(path.as_str()),
            Some(size),
            None,
        );
        self.remote.selected = Some((path.clone(), display));
        self.remote.error = None;
        self.remote.job = Some(self.spawn("Loading the remote drive", move |_| {
            // Backup reads a physical drive on the daemon.
            let info =
                crate::model::backup_remote::load_remote_source(conn, path.as_str(), true)?;
            Ok(Outcome::Loaded(info))
        }));
    }

    #[cfg(not(feature = "remote"))]
    pub(super) fn remote_pick(&mut self, _path: String, _size: u64) {
        self.remote.dialog = false;
    }

    /// Disconnect and forget the remote drive and what was found on it.
    pub(super) fn remote_clear(&mut self) {
        let was_active = self.remote_active();
        self.remote = super::remote::RemoteState::default();
        if was_active {
            self.clear_found();
        }
    }

    /// A local pick replaces an active remote source.
    pub(super) fn drop_remote_source(&mut self) {
        if self.remote_active() {
            self.remote_clear();
        }
    }

    #[cfg(feature = "remote")]
    pub(super) fn poll_remote(&mut self) {
        let Some(done) = self
            .remote
            .job
            .as_ref()
            .and_then(super::source::finished)
        else {
            return;
        };
        self.remote.job = None;
        match done {
            Err(err) => {
                self.remote.error = Some(err.clone());
                self.error(format!("Remote: {err}"));
            }
            Ok(Outcome::Connected(conn, devices)) => {
                self.info(format!(
                    "Connected to remote daemon ({} device{})",
                    devices.len(),
                    if devices.len() == 1 { "" } else { "s" }
                ));
                self.remote.conn = Some(conn);
                self.remote.devices = devices;
            }
            Ok(Outcome::Loaded(info)) => self.remote_loaded(info),
        }
    }

    #[cfg(not(feature = "remote"))]
    pub(super) fn poll_remote(&mut self) {}

    #[cfg(feature = "remote")]
    fn remote_loaded(&mut self, info: RemoteSourceInfo) {
        if !self.remote_active() {
            return;
        }
        self.info(format!(
            "Remote source: {} ({} partition{})",
            info.table_desc,
            info.partitions.len(),
            if info.partitions.len() == 1 { "" } else { "s" }
        ));
        self.found.partitions = info.partitions;
        self.found.table_desc = Some(info.table_desc);
        self.found.load_error = None;
        self.select_all();
        // Cheap minimums are a few ranged reads; an expensive walk waits behind Calc min.
        let (defer, run) = crate::model::backup_plan::remote_min_plan(
            self.found.partitions.as_slice(),
            info.is_superfloppy,
        );
        for (idx, fs_name) in defer {
            self.found.deferred.insert(idx, fs_name);
        }
        for idx in run {
            self.calc_min(idx);
        }
    }
}
