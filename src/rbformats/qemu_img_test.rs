//! Shared `#[cfg(test)]` helper: gate `qemu-img check` round-trip tests on
//! the binary's availability.
//!
//! Sessions 2.2 (QCOW2 writer) and 4.2 (VMDK sparse writer) both want to
//! validate their output against the reference tool when it's installed, and
//! skip silently otherwise (so CI without QEMU still passes). This module is
//! that one shared helper — kept here to avoid duplicating the PATH-probe
//! logic in two writer test modules.

#![cfg(test)]

use std::path::Path;
use std::process::Command;

/// True if `qemu-img --version` runs successfully (i.e. the binary is on PATH).
fn qemu_img_available() -> bool {
    Command::new("qemu-img")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Result of [`qemu_img_check`]:
/// - `Ok(true)` — `qemu-img check` ran and succeeded.
/// - `Ok(false)` — `qemu-img` isn't installed; the caller should print a
///   skip message and move on.
/// - `Err(msg)` — `qemu-img` ran but reported a problem with the image. The
///   string carries both stderr and stdout so test failures pinpoint why.
pub(crate) fn qemu_img_check(path: &Path) -> Result<bool, String> {
    if !qemu_img_available() {
        return Ok(false);
    }
    let output = Command::new("qemu-img")
        .arg("check")
        .arg(path)
        .output()
        .map_err(|e| format!("failed to invoke qemu-img: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "qemu-img check exit {:?}\nstdout: {}\nstderr: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout).trim(),
            String::from_utf8_lossy(&output.stderr).trim(),
        ));
    }
    Ok(true)
}
