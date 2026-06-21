//! Testable core for backing up a **remote source** (a physical drive or an
//! image file on an `rb-cli serve` daemon) from the Backup tab.
//!
//! The GUI shell in `src/gui/backup_tab.rs` is a thin state machine over these
//! two blocking transitions; keeping the network + parsing logic here lets it be
//! exercised headlessly over a loopback `serve_on` (see the integration test),
//! since GUI code can't run in the agent environment.

use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};

use crate::partition::{PartitionInfo, PartitionTable};
use crate::remote::protocol::WireDevice;
use crate::remote::{RemoteBlockReader, RemoteConnection};

/// Connect to a daemon at `addr` (host:port) and list its physical disks.
/// Returns the shared connection (kept open so the chosen device opens on the
/// same session) plus the advertised devices.
pub fn connect_and_list_devices(
    addr: &str,
) -> Result<(Arc<Mutex<RemoteConnection>>, Vec<WireDevice>)> {
    let conn = RemoteConnection::connect_shared(addr)?;
    let devices = conn
        .lock()
        .map_err(|_| anyhow!("remote connection lock poisoned"))?
        .list_devices()?;
    Ok((conn, devices))
}

/// The parsed shape of a remote source, ready to drive the Backup tab's
/// partition-selection UI.
pub struct RemoteSourceInfo {
    /// Total source length in bytes.
    pub size: u64,
    /// Human-readable partition-table label (e.g. `MBR`, `GPT`, or a
    /// superfloppy note).
    pub table_desc: String,
    /// Whether the source has no partition table (a bare filesystem / device).
    pub is_superfloppy: bool,
    /// Partitions to surface for backup selection.
    pub partitions: Vec<PartitionInfo>,
}

/// Open a remote source (`is_device` = physical drive vs image file) over the
/// block tier and parse its partition table — exactly what the desktop engine
/// does locally, but every read is a ranged request. Blocking (a few sectors).
pub fn load_remote_source(
    conn: Arc<Mutex<RemoteConnection>>,
    path: &str,
    is_device: bool,
) -> Result<RemoteSourceInfo> {
    let mut reader = if is_device {
        RemoteBlockReader::open_device(conn, path)?
    } else {
        RemoteBlockReader::open(conn, path)?
    };
    let size = reader.len();
    let table = PartitionTable::detect(&mut reader)
        .map_err(|e| anyhow!("reading the remote partition table: {e}"))?;
    let is_superfloppy = matches!(table, PartitionTable::None { .. });
    let table_desc = if is_superfloppy {
        "No partition table (superfloppy)".to_string()
    } else {
        table.type_name().to_string()
    };
    let partitions = table.partitions();
    Ok(RemoteSourceInfo {
        size,
        table_desc,
        is_superfloppy,
        partitions,
    })
}
