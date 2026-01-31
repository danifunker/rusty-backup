use std::fs::File;
use std::path::Path;

use anyhow::{Context, Result};

use crate::device::DiskDevice;

/// Open a target device for writing on Windows.
///
/// Opens the physical drive path (e.g., `\\.\PhysicalDrive1`) with
/// read+write access.
pub fn open_target_for_writing(path: &Path) -> Result<File> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("cannot open {} for writing", path.display()))
}

/// Enumerate devices on Windows.
///
/// TODO: Implement using WMI or SetupAPI to list physical drives,
/// mapping \\.\PhysicalDriveN paths with sizes and removable flags.
pub fn enumerate_devices() -> Vec<DiskDevice> {
    Vec::new()
}
