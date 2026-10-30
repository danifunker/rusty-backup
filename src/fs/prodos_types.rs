//! Moved to the `fs-apple2` crate; re-exported so `crate::fs::prodos_types` paths keep working.

pub use fs_apple2::prodos_types::*;

/// Point the ProDOS type-table override at the invoking user's config dir; call once at startup.
pub fn init_user_config_dir() {
    // Same resolution as src/update.rs, so pkexec-elevated launches still find the real user's dir.
    #[cfg(target_os = "linux")]
    let base = crate::os::linux::real_user_home().map(|h| h.join(".config"));
    #[cfg(not(target_os = "linux"))]
    let base = dirs::config_dir();
    if let Some(dir) = base {
        set_user_config_dir(dir.join("rusty-backup"));
    }
}
