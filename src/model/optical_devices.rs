//! The unified rip-device picker model.
//!
//! A rip can read from a locally-attached drive or a drive on a remote
//! `rb-cli serve` daemon. This module merges both into one [`RipDevice`] list so
//! the GUI shows a single pulldown and the CLI a single listing — each entry
//! tagged with where it lives and able to mint the right
//! [`crate::optical::rip::OpticalTarget`]. See `docs/remote_ripping.md`.

use crate::optical::rip::OpticalTarget;

#[cfg(feature = "remote")]
use crate::remote::connection::RemoteConnection;
#[cfg(feature = "remote")]
use std::sync::{Arc, Mutex};

/// Where a [`RipDevice`] physically lives.
pub enum DeviceLocation {
    /// A drive attached to this machine.
    Local,
    /// A drive on a remote daemon, reached over an open connection. `label` is
    /// the daemon's `host:port`.
    #[cfg(feature = "remote")]
    Remote {
        conn: Arc<Mutex<RemoteConnection>>,
        label: String,
    },
}

/// One selectable optical drive in the unified picker.
pub struct RipDevice {
    /// Human-readable drive name (e.g. `TSSTcorp CD/DVDW SH-224`).
    pub display_name: String,
    /// Device path on its own machine (e.g. `/dev/sr0`).
    pub device_path: String,
    pub location: DeviceLocation,
}

impl RipDevice {
    /// Label for a GUI pulldown: local shows `name (path)`; remote prefixes the
    /// daemon as `[host:port] name (path)`.
    pub fn picker_label(&self) -> String {
        match &self.location {
            DeviceLocation::Local => format!("{} ({})", self.display_name, self.device_path),
            #[cfg(feature = "remote")]
            DeviceLocation::Remote { label, .. } => {
                format!("[{}] {} ({})", label, self.display_name, self.device_path)
            }
        }
    }

    /// The `--device` argument that would re-open this drive from the CLI: a
    /// local path, or `rb://host:port/dev/sr0` for a remote drive.
    pub fn cli_device_arg(&self) -> String {
        match &self.location {
            DeviceLocation::Local => self.device_path.clone(),
            #[cfg(feature = "remote")]
            DeviceLocation::Remote { label, .. } => format!("rb://{}{}", label, self.device_path),
        }
    }

    /// The [`OpticalTarget`] a rip reads from (borrowing — clones the cheap
    /// device path / `Arc` connection handle, leaving the device in its list).
    pub fn to_target(&self) -> OpticalTarget {
        match &self.location {
            DeviceLocation::Local => OpticalTarget::Local(self.device_path.clone()),
            #[cfg(feature = "remote")]
            DeviceLocation::Remote { conn, .. } => OpticalTarget::Remote {
                conn: conn.clone(),
                device_path: self.device_path.clone(),
            },
        }
    }

    /// Consume into the [`OpticalTarget`] a rip reads from.
    pub fn into_target(self) -> OpticalTarget {
        match self.location {
            DeviceLocation::Local => OpticalTarget::Local(self.device_path),
            #[cfg(feature = "remote")]
            DeviceLocation::Remote { conn, .. } => OpticalTarget::Remote {
                conn,
                device_path: self.device_path,
            },
        }
    }

    /// Is this drive on a remote daemon? (Local-only features like disc-info
    /// auto-detection and Browse Contents are skipped for remote drives.)
    pub fn is_remote(&self) -> bool {
        #[cfg(feature = "remote")]
        {
            matches!(self.location, DeviceLocation::Remote { .. })
        }
        #[cfg(not(feature = "remote"))]
        {
            false
        }
    }
}

/// All locally-attached optical drives (the same enumeration the GUI's drive
/// combo and `rb-cli optical drives` use).
pub fn list_local_rip_devices() -> Vec<RipDevice> {
    opticaldiscs::drives::list_drives()
        .into_iter()
        .map(|d| RipDevice {
            display_name: d.display_name,
            device_path: d.device_path.to_string_lossy().into_owned(),
            location: DeviceLocation::Local,
        })
        .collect()
}

/// Append a connected daemon's optical drives to `out`, returning the count
/// added. Errors are swallowed — an offline daemon, or one built without the
/// `optical` feature (its `list_optical_drives` replies with an error), simply
/// contributes no drives. This is also how the picker stays capability-gated
/// without inspecting the handshake bits.
#[cfg(feature = "remote")]
pub fn append_remote_rip_devices(
    out: &mut Vec<RipDevice>,
    conn: &Arc<Mutex<RemoteConnection>>,
) -> usize {
    let (label, drives) = {
        let mut guard = match conn.lock() {
            Ok(g) => g,
            Err(_) => return 0,
        };
        let label = guard.addr().to_string();
        match guard.list_optical_drives() {
            Ok(d) => (label, d),
            Err(_) => return 0,
        }
    };
    let added = drives.len();
    for d in drives {
        out.push(RipDevice {
            display_name: d.display_name,
            device_path: d.device_path,
            location: DeviceLocation::Remote {
                conn: conn.clone(),
                label: label.clone(),
            },
        });
    }
    added
}

/// Local + every connected daemon's optical drives, merged into one list.
#[cfg(feature = "remote")]
pub fn list_rip_devices(remotes: &[Arc<Mutex<RemoteConnection>>]) -> Vec<RipDevice> {
    let mut out = list_local_rip_devices();
    for conn in remotes {
        append_remote_rip_devices(&mut out, conn);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_label_and_cli_arg() {
        let d = RipDevice {
            display_name: "TSSTcorp CD/DVDW".to_string(),
            device_path: "/dev/sr0".to_string(),
            location: DeviceLocation::Local,
        };
        assert_eq!(d.picker_label(), "TSSTcorp CD/DVDW (/dev/sr0)");
        assert_eq!(d.cli_device_arg(), "/dev/sr0");
        match d.into_target() {
            OpticalTarget::Local(p) => assert_eq!(p, "/dev/sr0"),
            #[cfg(feature = "remote")]
            _ => panic!("expected a local target"),
        }
    }
}
