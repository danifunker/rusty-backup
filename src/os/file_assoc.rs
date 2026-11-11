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

use crate::model::file_types::association_exts;
use crate::os::windows_gui::WindowsGui;

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
    // Each GUI has its own ProgId, so installing or removing one leaves the other's "Open with".
    let gui = WindowsGui::current();
    let (id, desc) = gui.progid();
    let (progid, _) = classes.create_subkey(id)?;
    progid.set_value("", &desc.to_string())?;
    let (icon, _) = progid.create_subkey("DefaultIcon")?;
    icon.set_value("", &format!("{exe_str},0"))?;
    let (open, _) = progid.create_subkey("shell\\open")?;
    open.set_value("FriendlyAppName", &gui.app_name().to_string())?;
    let (cmd, _) = open.create_subkey("command")?;
    cmd.set_value("", &format!("\"{exe_str}\" \"%1\""))?;
    // "How do you want to open this file?" names the app from here, not from the ProgId.
    let (app, _) = classes.create_subkey(format!("Applications\\{}", gui.exe_name()))?;
    app.set_value("FriendlyAppName", &gui.app_name().to_string())?;

    // Non-destructive per-extension registration: list our ProgId under each
    // extension's OpenWithProgids (an empty-string value-data marker). This
    // never overwrites whatever the user has set as their default handler.
    for ext in association_exts() {
        let (key, _) = classes.create_subkey(format!(".{ext}\\OpenWithProgids"))?;
        key.set_value(id, &String::new())?;
    }

    Ok(())
}

/// Remove the Rusty Backup ProgId and its per-extension OpenWithProgids markers.
/// Leaves any other handlers and the user's chosen defaults untouched.
pub fn unregister_file_associations() -> io::Result<()> {
    let classes = open_classes()?;
    let gui = WindowsGui::current();
    let (id, _) = gui.progid();
    let _ = classes.delete_subkey_all(id);
    let _ = classes.delete_subkey_all(format!("Applications\\{}", gui.exe_name()));
    for ext in association_exts() {
        if let Ok(key) =
            classes.open_subkey_with_flags(format!(".{ext}\\OpenWithProgids"), KEY_WRITE)
        {
            let _ = key.delete_value(id);
        }
    }
    Ok(())
}

/// Launch hooks for an installed GUI, run before any window: the installer's and uninstaller's
/// `--register-file-associations` / `--unregister-file-associations` (which exit), a refresh of
/// associations registered by an older build, and the Add/Remove Programs version after an update.
pub fn launch_hooks() {
    use crate::update::UpdateConfig;

    let version = env!("APP_VERSION");
    let args: Vec<String> = std::env::args().collect();
    let register = args.iter().any(|a| a == "--register-file-associations");
    let unregister = args.iter().any(|a| a == "--unregister-file-associations");

    let mut cfg = UpdateConfig::load();
    if !register && !unregister {
        if cfg.file_associations_enabled && cfg.assoc_registered_version.as_deref() != Some(version)
        {
            let _ = register_file_associations();
            cfg.assoc_registered_version = Some(version.to_string());
            let _ = cfg.save();
        }
        // self_replace bypasses the installer, so its DisplayVersion goes stale (no-op when portable).
        let _ = crate::os::win_install::refresh_arp_display_version(version);
        return;
    }
    if register {
        let _ = register_file_associations();
        cfg.file_associations_enabled = true;
        cfg.assoc_registered_version = Some(version.to_string());
    } else {
        let _ = unregister_file_associations();
        cfg.file_associations_enabled = false;
        cfg.assoc_registered_version = None;
    }
    let _ = cfg.save();
    std::process::exit(0);
}
