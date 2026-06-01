//! Windows file-association registration (per-user, `HKCU\Software\Classes`).
//!
//! Rusty Backup is registered as a *handler* for its disk-image extensions via
//! `OpenWithProgids` — it appears in Explorer's "Open with" list and is
//! eligible to be made the default, but it does **not** silently seize the
//! default association (Windows 8+ requires the user to confirm that). The
//! extension list comes from [`crate::model::file_types`], the single source of
//! truth, so a self-update that adds a new container format re-registers it on
//! next launch without a reinstaller.
//!
//! All writes are under `HKEY_CURRENT_USER`, so no elevation is required.

use std::io;

use winreg::enums::{HKEY_CURRENT_USER, KEY_WRITE};
use winreg::RegKey;

use crate::model::file_types::{association_exts, DISK_IMAGE_PROGID, DISK_IMAGE_PROGID_DESC};

/// Open (creating if needed) `HKCU\Software\Classes`.
fn open_classes() -> io::Result<RegKey> {
    let hkcu = RegKey::predef(HKEY_CURRENT_USER);
    let (classes, _) = hkcu.create_subkey("Software\\Classes")?;
    Ok(classes)
}

/// Register Rusty Backup as a handler for every extension in
/// [`crate::model::file_types`]. Idempotent.
pub fn register_file_associations() -> io::Result<()> {
    let classes = open_classes()?;
    let exe = std::env::current_exe()?;
    let exe_str = exe.display().to_string();

    // ProgId: friendly description, icon, and the open command. Values are
    // passed as `&String` so winreg infers `ToRegValue for String` unambiguously.
    let (progid, _) = classes.create_subkey(DISK_IMAGE_PROGID)?;
    progid.set_value("", &DISK_IMAGE_PROGID_DESC.to_string())?;
    let (icon, _) = progid.create_subkey("DefaultIcon")?;
    icon.set_value("", &format!("{exe_str},0"))?;
    let (cmd, _) = progid.create_subkey("shell\\open\\command")?;
    cmd.set_value("", &format!("\"{exe_str}\" \"%1\""))?;

    // Non-destructive per-extension registration: list our ProgId under each
    // extension's OpenWithProgids (an empty-string value-data marker). This
    // never overwrites whatever the user has set as their default handler.
    for ext in association_exts() {
        let (key, _) = classes.create_subkey(format!(".{ext}\\OpenWithProgids"))?;
        key.set_value(DISK_IMAGE_PROGID, &String::new())?;
    }

    Ok(())
}

/// Remove the Rusty Backup ProgId and its per-extension OpenWithProgids markers.
/// Leaves any other handlers and the user's chosen defaults untouched.
pub fn unregister_file_associations() -> io::Result<()> {
    let classes = open_classes()?;
    let _ = classes.delete_subkey_all(DISK_IMAGE_PROGID);
    for ext in association_exts() {
        if let Ok(key) =
            classes.open_subkey_with_flags(format!(".{ext}\\OpenWithProgids"), KEY_WRITE)
        {
            let _ = key.delete_value(DISK_IMAGE_PROGID);
        }
    }
    Ok(())
}
