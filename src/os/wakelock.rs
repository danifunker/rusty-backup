//! Cross-platform wake lock to prevent the OS from sleeping during long
//! operations (backup, restore, format conversion, optical rip/burn, etc.).
//!
//! Acquire at the top of a worker thread; the returned guard releases on
//! `Drop`. Idle-sleep prevention only — the display is still allowed to dim
//! so multi-hour operations don't burn the user's monitor.
//!
//! - macOS: `IOPMAssertionCreateWithName` with `PreventUserIdleSystemSleep`.
//! - Windows: `SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)`
//!   on the calling thread; cleared on drop from the same thread.
//! - Linux: spawns `systemd-inhibit ... sleep infinity` as a child process
//!   and kills it on drop. Silently no-ops if `systemd-inhibit` is missing.
//!
//! Failures are non-fatal: if the platform call doesn't succeed, `acquire()`
//! still returns a guard (which does nothing on drop) so callers don't have
//! to handle errors.

#[cfg(target_os = "macos")]
mod imp {
    use objc2_core_foundation::CFString;
    use objc2_io_kit::{
        kIOPMAssertionLevelOn, IOPMAssertionCreateWithName, IOPMAssertionID, IOPMAssertionRelease,
    };

    pub struct WakeLock {
        id: Option<IOPMAssertionID>,
    }

    pub fn acquire(reason: &str) -> WakeLock {
        let kind = CFString::from_static_str("PreventUserIdleSystemSleep");
        let name = CFString::from_str(reason);
        let mut id: IOPMAssertionID = 0;
        let rc = unsafe {
            IOPMAssertionCreateWithName(Some(&kind), kIOPMAssertionLevelOn, Some(&name), &mut id)
        };
        if rc == 0 {
            log::debug!("wakelock acquired (macOS): {}", reason);
            WakeLock { id: Some(id) }
        } else {
            log::warn!("wakelock acquire failed (macOS), IOReturn={:#x}", rc);
            WakeLock { id: None }
        }
    }

    impl Drop for WakeLock {
        fn drop(&mut self) {
            if let Some(id) = self.id.take() {
                unsafe {
                    IOPMAssertionRelease(id);
                }
                log::debug!("wakelock released (macOS)");
            }
        }
    }
}

#[cfg(target_os = "windows")]
mod imp {
    use windows::Win32::System::Power::{
        SetThreadExecutionState, ES_CONTINUOUS, ES_SYSTEM_REQUIRED, EXECUTION_STATE,
    };

    pub struct WakeLock {
        active: bool,
    }

    pub fn acquire(reason: &str) -> WakeLock {
        let prev = unsafe { SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED) };
        if prev == EXECUTION_STATE(0) {
            log::warn!("wakelock acquire failed (Windows): SetThreadExecutionState returned 0");
            WakeLock { active: false }
        } else {
            log::debug!("wakelock acquired (Windows): {}", reason);
            WakeLock { active: true }
        }
    }

    impl Drop for WakeLock {
        fn drop(&mut self) {
            if self.active {
                unsafe {
                    SetThreadExecutionState(ES_CONTINUOUS);
                }
                log::debug!("wakelock released (Windows)");
            }
        }
    }
}

#[cfg(target_os = "linux")]
mod imp {
    use std::process::{Child, Command, Stdio};

    pub struct WakeLock {
        child: Option<Child>,
    }

    pub fn acquire(reason: &str) -> WakeLock {
        let result = Command::new("systemd-inhibit")
            .arg("--what=idle")
            .arg("--who=rusty-backup")
            .arg(format!("--why={}", reason))
            .arg("--mode=block")
            .arg("sleep")
            .arg("infinity")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();
        match result {
            Ok(child) => {
                log::debug!("wakelock acquired (Linux): {}", reason);
                WakeLock { child: Some(child) }
            }
            Err(e) => {
                log::warn!(
                    "wakelock acquire failed (Linux): systemd-inhibit unavailable: {}",
                    e
                );
                WakeLock { child: None }
            }
        }
    }

    impl Drop for WakeLock {
        fn drop(&mut self) {
            if let Some(mut child) = self.child.take() {
                let _ = child.kill();
                let _ = child.wait();
                log::debug!("wakelock released (Linux)");
            }
        }
    }
}

#[cfg(not(any(target_os = "macos", target_os = "windows", target_os = "linux")))]
mod imp {
    pub struct WakeLock;
    pub fn acquire(_reason: &str) -> WakeLock {
        WakeLock
    }
}

pub use imp::{acquire, WakeLock};
