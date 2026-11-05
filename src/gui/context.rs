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

use rusty_backup::controller::log::LogStore;

/// Bundle of references the App threads through every tab call.
pub struct TabContext<'a> {
    pub devices: &'a [DiskDevice],
    pub log: &'a mut LogStore,
    /// Name of another tab with a disk job in flight; start controls lock on it.
    pub busy_elsewhere: Option<&'static str>,
}

impl<'a> TabContext<'a> {
    pub fn new(devices: &'a [DiskDevice], log: &'a mut LogStore) -> Self {
        Self {
            devices,
            log,
            busy_elsewhere: None,
        }
    }

    pub fn busy_elsewhere(mut self, tab: Option<&'static str>) -> Self {
        self.busy_elsewhere = tab;
        self
    }
}

/// The one-line notice a tab shows while another tab's job locks its controls.
pub fn busy_elsewhere_notice(tab: &str) -> String {
    format!("A disk job is running ({tab}); start controls unlock when it finishes.")
}
