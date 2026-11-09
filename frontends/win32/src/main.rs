//! The Win32 shell: a pure view over `rusty_backup::controller` (docs/win32_shell_plan.md).
//!
//! Layers and rules: CONTRIBUTING.md § Architecture policy (MVC). Nothing here names the engine
//! or the model; `scripts/check-boundaries.sh` enforces that.

#![cfg_attr(all(windows, not(debug_assertions)), windows_subsystem = "windows")]

#[cfg(windows)]
mod app;
#[cfg(windows)]
mod effects;
mod layout;
#[cfg(windows)]
mod sync;
#[cfg(windows)]
mod ui;
#[cfg(windows)]
mod wake;

#[cfg(windows)]
fn main() {
    use rusty_backup::controller::log::capture;
    use windows::Win32::System::Com::{CoInitializeEx, CoUninitialize, COINIT_APARTMENTTHREADED};
    use windows::Win32::UI::Controls::{
        InitCommonControlsEx, ICC_STANDARD_CLASSES, ICC_TAB_CLASSES, INITCOMMONCONTROLSEX,
    };
    use windows::Win32::UI::WindowsAndMessaging::{
        DispatchMessageW, GetMessageW, IsDialogMessageW, TranslateMessage, MSG,
    };

    capture::init();
    // SAFETY: COM and the common controls are set up once on the UI thread before any window.
    unsafe {
        let _ = CoInitializeEx(None, COINIT_APARTMENTTHREADED);
        let icc = INITCOMMONCONTROLSEX {
            dwSize: std::mem::size_of::<INITCOMMONCONTROLSEX>() as u32,
            dwICC: ICC_STANDARD_CLASSES | ICC_TAB_CLASSES,
        };
        let _ = InitCommonControlsEx(&icc);
    }
    app::start();
    let mut msg = MSG::default();
    // SAFETY: the standard message loop; IsDialogMessage gives Tab its meaning between controls.
    unsafe {
        while GetMessageW(&mut msg, None, 0, 0).as_bool() {
            if let Some(main) = app::window() {
                if IsDialogMessageW(main, &msg).as_bool() {
                    continue;
                }
            }
            let _ = TranslateMessage(&msg);
            DispatchMessageW(&msg);
        }
        ui::free_fonts();
        CoUninitialize();
    }
}

#[cfg(not(windows))]
fn main() {
    eprintln!("rusty-backup-win32 runs on Windows only.");
    let _ = layout::ROW;
}
