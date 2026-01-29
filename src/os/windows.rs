use crate::device::DiskDevice;

/// Enumerate devices on Windows.
///
/// TODO: Implement using WMI or SetupAPI to list physical drives,
/// mapping \\.\PhysicalDriveN paths with sizes and removable flags.
pub fn enumerate_devices() -> Vec<DiskDevice> {
    Vec::new()
}
