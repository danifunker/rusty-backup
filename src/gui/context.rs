//! Shared per-tab context bundle.
//!
//! Tabs (Inspect, Backup, Restore) each take the same pair of references on
//! every call: the live device list and the app-level log panel. Bundling
//! them as [`TabContext`] cuts each method's argument list by two and gives
//! the App a single value to construct when dispatching to a tab.
//!
//! `progress: &mut ProgressState` is *not* in the bundle: only a couple of
//! methods need it, and keeping it separate avoids `Option<&mut _>` ergonomic
//! pain at the (many) call sites that don't.
//!
//! Extracted from per-tab argument lists per §5 of `docs/codecleanup.md`.

use rusty_backup::device::DiskDevice;

use super::progress::LogPanel;

/// Bundle of references the App threads through every tab call.
pub struct TabContext<'a> {
    pub devices: &'a [DiskDevice],
    pub log: &'a mut LogPanel,
}

impl<'a> TabContext<'a> {
    pub fn new(devices: &'a [DiskDevice], log: &'a mut LogPanel) -> Self {
        Self { devices, log }
    }
}
