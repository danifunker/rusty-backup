//! A drive or image file on an `rb-cli serve` daemon as the full-disk restore target.

#[cfg(feature = "remote")]
use std::sync::{Arc, Mutex};

use super::{RemoteDrive, RestoreController};
#[cfg(feature = "remote")]
use crate::model::job::Job;
#[cfg(feature = "remote")]
use crate::remote::protocol::WireDevice;
#[cfg(feature = "remote")]
use crate::remote::RemoteConnection;

#[cfg(feature = "remote")]
pub(super) type Conn = Arc<Mutex<RemoteConnection>>;

#[derive(Default)]
pub(super) struct RemoteState {
    /// "Restore to a remote machine" is ticked.
    pub active: bool,
    /// The host:port field.
    pub address: String,
    /// The image-file name field.
    pub image_name: String,
    pub error: Option<String>,
    #[cfg(feature = "remote")]
    conn: Option<Conn>,
    /// The address connected to, for the target label.
    #[cfg(feature = "remote")]
    addr: Option<String>,
    #[cfg(feature = "remote")]
    devices: Vec<WireDevice>,
    /// The chosen target: its daemon-side path, the `rb://` label, and whether it is a device.
    #[cfg(feature = "remote")]
    selected: Option<(String, String, bool)>,
    #[cfg(feature = "remote")]
    job: Option<Job<(Conn, Vec<WireDevice>)>>,
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

    /// The connection and the chosen target, for the restore.
    #[cfg(feature = "remote")]
    pub fn selection(&self) -> Option<(Conn, String, String, bool)> {
        let conn = self.conn.clone()?;
        let (path, display, is_device) = self.selected.clone()?;
        Some((conn, path, display, is_device))
    }
}

impl RestoreController {
    /// This build can restore to an `rb-cli serve` daemon.
    pub const fn remote_available() -> bool {
        cfg!(feature = "remote")
    }

    /// "Restore to a remote machine"; a build without `remote` says so and stays local.
    pub(super) fn set_remote(&mut self, on: bool) {
        if on && !Self::remote_available() {
            self.warn(crate::controller::backup::REMOTE_NOT_BUILT);
            return;
        }
        self.remote.active = on;
    }

    /// The full-disk target is on a daemon.
    pub fn remote_active(&self) -> bool {
        cfg!(feature = "remote") && self.remote.active
    }

    pub fn remote_address(&self) -> &str {
        self.remote.address.as_str()
    }

    pub fn remote_image_name(&self) -> &str {
        self.remote.image_name.as_str()
    }

    pub fn remote_busy(&self) -> bool {
        self.remote.busy()
    }

    pub fn remote_error(&self) -> Option<&str> {
        self.remote.error.as_deref()
    }

    pub fn remote_connected(&self) -> bool {
        #[cfg(feature = "remote")]
        {
            self.remote.conn.is_some()
        }
        #[cfg(not(feature = "remote"))]
        {
            false
        }
    }

    /// The chosen remote target's "rb://..." label.
    pub fn remote_target_label(&self) -> Option<&str> {
        #[cfg(feature = "remote")]
        {
            self.remote.selected.as_ref().map(|(_, d, _)| d.as_str())
        }
        #[cfg(not(feature = "remote"))]
        {
            None
        }
    }

    /// The chosen remote device's path (an image target has none).
    pub fn remote_selected_device(&self) -> Option<&str> {
        #[cfg(feature = "remote")]
        {
            self.remote
                .selected
                .as_ref()
                .filter(|(_, _, dev)| *dev)
                .map(|(p, _, _)| p.as_str())
        }
        #[cfg(not(feature = "remote"))]
        {
            None
        }
    }

    pub fn remote_drives(&self) -> Vec<RemoteDrive> {
        #[cfg(feature = "remote")]
        {
            self.remote
                .devices
                .iter()
                .map(|d| {
                    let mut label = format!(
                        "{} ({})",
                        d.path,
                        disk_core::display::format_size(d.size_bytes)
                    );
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

    /// Connect to the daemon and list its drives; a fresh connect drops the old target.
    #[cfg(feature = "remote")]
    pub(super) fn remote_connect(&mut self) {
        if self.remote.busy() {
            return;
        }
        let a = self.remote.address.trim().to_string();
        if a.is_empty() {
            return;
        }
        let addr = if a.contains(':') {
            a
        } else {
            format!("{a}:7341")
        };
        self.remote.selected = None;
        self.remote.devices.clear();
        self.remote.conn = None;
        self.remote.error = None;
        self.remote.addr = Some(addr.clone());
        self.remote.job = Some(self.spawn("Connecting", move |_| {
            crate::model::backup_remote::connect_and_list_devices(addr.as_str())
        }));
    }

    #[cfg(not(feature = "remote"))]
    pub(super) fn remote_connect(&mut self) {}

    #[cfg(feature = "remote")]
    pub(super) fn remote_pick_device(&mut self, path: String, size: u64) {
        let addr = self.remote.addr.clone().unwrap_or_default();
        let display = format!(
            "rb://{addr}{path} ({})",
            disk_core::display::format_size(size)
        );
        self.remote.selected = Some((path, display, true));
    }

    #[cfg(not(feature = "remote"))]
    pub(super) fn remote_pick_device(&mut self, _path: String, _size: u64) {}

    /// The image-file name field names the target under the daemon's serve root.
    #[cfg(feature = "remote")]
    pub(super) fn remote_use_image(&mut self) {
        let name = self.remote.image_name.trim().to_string();
        if name.is_empty() {
            return;
        }
        let path = if name.starts_with('/') {
            name
        } else {
            format!("/{name}")
        };
        let addr = self.remote.addr.clone().unwrap_or_default();
        let display = format!("rb://{addr}{path} (image)");
        self.remote.selected = Some((path, display, false));
    }

    #[cfg(not(feature = "remote"))]
    pub(super) fn remote_use_image(&mut self) {}

    #[cfg(feature = "remote")]
    pub(super) fn poll_remote(&mut self) {
        let Some(done) = self.remote.job.as_ref().and_then(super::source::finished) else {
            return;
        };
        self.remote.job = None;
        match done {
            Err(err) => {
                self.remote.error = Some(err.clone());
                self.error(format!("Remote: {err}"));
            }
            Ok((conn, devices)) => {
                self.info(format!(
                    "Connected to remote daemon ({} device{})",
                    devices.len(),
                    if devices.len() == 1 { "" } else { "s" }
                ));
                self.remote.conn = Some(conn);
                self.remote.devices = devices;
            }
        }
    }

    #[cfg(not(feature = "remote"))]
    pub(super) fn poll_remote(&mut self) {}
}
