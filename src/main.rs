// Hide console window on Windows release builds
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod gui_fltk;

fn main() {
    // Linux: Request elevation at startup if not already running as root
    #[cfg(target_os = "linux")]
    {
        if !nix::unistd::geteuid().is_root() {
            eprintln!("Rusty Backup requires administrator privileges for disk access.");
            eprintln!("Requesting elevation...");

            // relaunch_with_elevation() replaces the process on success (never returns).
            // If it fails (user cancelled, pkexec unavailable), fall through and run
            // unprivileged — the "Request Elevation" button in the GUI is still available.
            if let Err(e) = rusty_backup::os::linux::relaunch_with_elevation() {
                eprintln!("Failed to elevate: {e}");
                eprintln!("Continuing without elevated privileges...");
            }
        } else {
            // Already root (elevated relaunch landed here) — set permissive umask
            // so backup files are created with 666/777 permissions accessible to the real user.
            rusty_backup::os::linux::set_permissive_umask_if_elevated();
            eprintln!("Running with administrator privileges ✓");
        }
    }

    // macOS: Request elevation at startup if not already running as root
    #[cfg(target_os = "macos")]
    {
        if unsafe { libc::geteuid() } != 0 {
            eprintln!("Rusty Backup requires administrator privileges for disk access.");
            eprintln!("Requesting elevation...");

            if let Err(e) = rusty_backup::os::macos::request_app_elevation() {
                eprintln!("Failed to elevate: {}", e);
                eprintln!("\nPlease run the application with sudo:");
                eprintln!("  sudo '/Applications/Rusty Backup.app/Contents/MacOS/rusty-backup'");
                std::process::exit(1);
            }

            // If we reach here, elevation was requested but the elevated process
            // completed (shouldn't happen in normal flow)
            std::process::exit(0);
        }

        eprintln!("Running with administrator privileges ✓");
    }

    // Initialize and run FLTK app
    let app = gui_fltk::RustyBackupApp::new();
    app.run();
}
