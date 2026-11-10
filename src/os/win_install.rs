//! Windows installed-build helpers.
//!
//! Distinguishes an Inno per-user install from a portable ZIP copy, and keeps
//! the installer's Add/Remove Programs (ARP) entry accurate after an in-app
//! self-update bypasses the installer. See `docs/windows_self_update.md`.

use std::io;

use winreg::enums::{HKEY_CURRENT_USER, KEY_READ, KEY_WRITE};
use winreg::RegKey;

/// Inno's per-user uninstall entry for the running GUI (`_is1` is Inno's suffix on the `AppId`).
fn uninstall_subkey() -> String {
    let id = crate::os::windows_gui::WindowsGui::current().app_id();
    format!("Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\{id}_is1")
}

/// True when this build was placed by the Inno installer (its ARP key exists),
/// as opposed to a portable ZIP copy. Portable copies should not touch the
/// installer-owned registry entries.
pub fn is_installed() -> bool {
    let hkcu = RegKey::predef(HKEY_CURRENT_USER);
    hkcu.open_subkey_with_flags(uninstall_subkey(), KEY_READ)
        .is_ok()
}

/// Set the ARP `DisplayVersion` to `version` if it differs. No-op for portable
/// copies (the key won't exist). HKCU, so no elevation needed.
///
/// `self_replace` swaps the exe in place without rerunning the installer, so
/// the version Inno wrote goes stale; call this on launch to keep it truthful.
/// The install dir / `UninstallString` are untouched, so the uninstaller keeps
/// working across self-updates.
pub fn refresh_arp_display_version(version: &str) -> io::Result<()> {
    let hkcu = RegKey::predef(HKEY_CURRENT_USER);
    let key = match hkcu.open_subkey_with_flags(uninstall_subkey(), KEY_READ | KEY_WRITE) {
        Ok(k) => k,
        Err(_) => return Ok(()), // portable / not installed
    };
    let current: Option<String> = key.get_value("DisplayVersion").ok();
    if current.as_deref() != Some(version) {
        key.set_value("DisplayVersion", &version.to_string())?;
    }
    Ok(())
}
