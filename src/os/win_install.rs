//! Windows installed-build helpers.
//!
//! Distinguishes an Inno per-user install from a portable ZIP copy, and keeps
//! the installer's Add/Remove Programs (ARP) entry accurate after an in-app
//! self-update bypasses the installer. See `docs/windows_self_update.md`.

use std::io;

use winreg::enums::{HKEY_CURRENT_USER, KEY_READ, KEY_WRITE};
use winreg::RegKey;

/// Inno Setup writes the per-user uninstall entry under this key. The `_is1`
/// suffix is Inno's convention; the GUID must match the `AppId` in
/// `installer/rusty-backup.iss`.
const UNINSTALL_SUBKEY: &str = "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\\
{8F3A1C2E-5B7D-4E9A-9C1F-2D6B8A4E7F10}_is1";

/// True when this build was placed by the Inno installer (its ARP key exists),
/// as opposed to a portable ZIP copy. Portable copies should not touch the
/// installer-owned registry entries.
pub fn is_installed() -> bool {
    let hkcu = RegKey::predef(HKEY_CURRENT_USER);
    hkcu.open_subkey_with_flags(UNINSTALL_SUBKEY, KEY_READ)
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
    let key = match hkcu.open_subkey_with_flags(UNINSTALL_SUBKEY, KEY_READ | KEY_WRITE) {
        Ok(k) => k,
        Err(_) => return Ok(()), // portable / not installed
    };
    let current: Option<String> = key.get_value("DisplayVersion").ok();
    if current.as_deref() != Some(version) {
        key.set_value("DisplayVersion", &version.to_string())?;
    }
    Ok(())
}
