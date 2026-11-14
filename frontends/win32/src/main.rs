//! The Win32 shell: a pure view over `rusty_backup::controller` (docs/win32_shell_plan.md).
//!
//! Layers and rules: CONTRIBUTING.md § Architecture policy (MVC). Nothing here names the engine
//! or the model; `scripts/check-boundaries.sh` enforces that.

#![cfg_attr(all(windows, not(debug_assertions)), windows_subsystem = "windows")]

#[cfg(windows)]
mod alto;
#[cfg(windows)]
mod app;
#[cfg(windows)]
mod archives;
#[cfg(windows)]
mod backup;
#[cfg(windows)]
mod backup_remote;
#[cfg(windows)]
mod backup_vhd;
#[cfg(windows)]
mod browse;
#[cfg(windows)]
mod browse_bars;
#[cfg(windows)]
mod build_disk;
#[cfg(windows)]
mod build_rows;
#[cfg(windows)]
mod chd;
#[cfg(windows)]
mod combo;
#[cfg(windows)]
mod commander;
#[cfg(windows)]
mod convert;
#[cfg(windows)]
mod darkdraw;
#[cfg(windows)]
mod disk_write;
#[cfg(windows)]
mod editor;
#[cfg(windows)]
mod effects;
#[cfg(windows)]
mod expand_hfs;
#[cfg(windows)]
mod expand_image;
#[cfg(windows)]
mod export;
#[cfg(windows)]
mod inspect;
mod layout;
#[cfg(windows)]
mod menu;
#[cfg(windows)]
mod optical;
#[cfg(windows)]
mod optical_remote;
#[cfg(windows)]
mod pane_view;
#[cfg(windows)]
mod panel;
#[cfg(windows)]
mod partition_bar;
#[cfg(windows)]
mod remote_pick;
#[cfg(windows)]
mod report;
#[cfg(windows)]
mod resize;
mod resize_image;
#[cfg(windows)]
mod restore;
#[cfg(windows)]
mod restore_remote;
#[cfg(windows)]
mod row_icons;
#[cfg(windows)]
mod settings;
#[cfg(windows)]
mod sync;
#[cfg(windows)]
mod theme;
#[cfg(windows)]
mod toolbar;
#[cfg(windows)]
mod ui;
#[cfg(windows)]
mod wake;
#[cfg(windows)]
mod window;

#[cfg(windows)]
fn main() {
    use rusty_backup::controller::log::capture;
    use windows::Win32::System::Com::{CoInitializeEx, CoUninitialize, COINIT_APARTMENTTHREADED};
    use windows::Win32::UI::Controls::{
        InitCommonControlsEx, ICC_BAR_CLASSES, ICC_LISTVIEW_CLASSES, ICC_STANDARD_CLASSES,
        INITCOMMONCONTROLSEX,
    };
    use windows::Win32::UI::WindowsAndMessaging::{
        DispatchMessageW, GetAncestor, GetMessageW, IsDialogMessageW, TranslateMessage, GA_ROOT,
        MSG, WM_KEYDOWN,
    };

    // The installer's association flags exit here, before any window.
    rusty_backup::controller::app::launch_hooks();
    capture::init();
    // SAFETY: COM and the common controls are set up once on the UI thread before any window.
    unsafe {
        let _ = CoInitializeEx(None, COINIT_APARTMENTTHREADED);
        let icc = INITCOMMONCONTROLSEX {
            dwSize: std::mem::size_of::<INITCOMMONCONTROLSEX>() as u32,
            dwICC: ICC_STANDARD_CLASSES | ICC_BAR_CLASSES | ICC_LISTVIEW_CLASSES,
        };
        let _ = InitCommonControlsEx(&icc);
    }
    theme::adopt();
    app::start();
    let mut msg = MSG::default();
    // SAFETY: the standard message loop; IsDialogMessage gives Tab its meaning between controls.
    unsafe {
        while GetMessageW(&mut msg, None, 0, 0).as_bool() {
            // Tab moves between controls in the main window and in each modeless dialog.
            let root = GetAncestor(msg.hwnd, GA_ROOT);
            let vk = msg.wParam.0 as u16;
            // F1 to F24 in a window go to its screen's key map, as its other keys do not.
            if msg.message == WM_KEYDOWN && (0x70..=0x87).contains(&vk) && panel::key(root, vk) {
                continue;
            }
            if app::window().is_some()
                && !root.is_invalid()
                && IsDialogMessageW(root, &msg).as_bool()
            {
                continue;
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
