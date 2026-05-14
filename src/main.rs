// Hide console window on Windows release builds
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod gui;

fn main() -> eframe::Result {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();
    // Linux: Request elevation at startup if not already running as root
    #[cfg(target_os = "linux")]
    {
        if !nix::unistd::geteuid().is_root() {
            log::info!("Rusty Backup requires administrator privileges for disk access.");
            log::info!("Requesting elevation...");

            // relaunch_with_elevation() replaces the process on success (never returns).
            // If it fails (user cancelled, pkexec unavailable), fall through and run
            // unprivileged — the "Request Elevation" button in the GUI is still available.
            if let Err(e) = rusty_backup::os::linux::relaunch_with_elevation() {
                log::warn!("Failed to elevate: {e}");
                log::warn!("Continuing without elevated privileges...");
            }
        } else {
            // Already root (elevated relaunch landed here) — set permissive umask
            // so backup files are created with 666/777 permissions accessible to the real user.
            rusty_backup::os::linux::set_permissive_umask_if_elevated();
            log::info!("Running with administrator privileges");
        }
    }

    // macOS: no startup elevation needed — authopen handles per-operation
    // privilege escalation via the native macOS auth dialog when the user
    // initiates a backup or restore.

    // Load icon from bytes with transparency preserved
    let icon_bytes = include_bytes!("../assets/icons/icon-256.png");
    let icon_image = image::load_from_memory_with_format(icon_bytes, image::ImageFormat::Png)
        .expect("Failed to load icon");

    // Ensure we have RGBA with alpha channel
    let icon_rgba = icon_image.to_rgba8();
    let (icon_width, icon_height) = icon_rgba.dimensions();

    let icon_data = egui::IconData {
        rgba: icon_rgba.into_raw(),
        width: icon_width,
        height: icon_height,
    };

    // Allow forcing a specific renderer via env var; otherwise auto.
    //   RUSTY_BACKUP_RENDERER=wgpu     -> wgpu only (Vulkan/Metal/DX12, no fallback)
    //   RUSTY_BACKUP_RENDERER=glow     -> OpenGL only (no wgpu attempt)
    //   RUSTY_BACKUP_RENDERER=software -> OpenGL via mesa llvmpipe (no GPU)
    //   unset                          -> try wgpu, fall back to glow, then software
    let forced = std::env::var("RUSTY_BACKUP_RENDERER").ok();
    let force_software = || {
        // mesa software rasterizer — ignores GPU drivers entirely.
        unsafe {
            std::env::set_var("LIBGL_ALWAYS_SOFTWARE", "1");
            std::env::set_var("GALLIUM_DRIVER", "llvmpipe");
        }
    };

    let make_options = |renderer: eframe::Renderer| eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([900.0, 700.0])
            .with_min_inner_size([600.0, 400.0])
            .with_icon(icon_data.clone())
            // app_id must match the .desktop file name so Wayland
            // compositors can look up the installed icon instead of
            // falling back to a generic placeholder (e.g. a "W" glyph).
            // X11 WM_CLASS is set from argv[0] (also "rusty-backup").
            .with_app_id("rusty-backup")
            .with_drag_and_drop(true),
        renderer,
        ..Default::default()
    };

    let run = |renderer: eframe::Renderer| {
        eframe::run_native(
            "Rusty Backup",
            make_options(renderer),
            Box::new(|_cc| Ok(Box::new(gui::RustyBackupApp::default()))),
        )
    };

    match forced.as_deref() {
        Some("glow") => run(eframe::Renderer::Glow),
        Some("wgpu") => run(eframe::Renderer::Wgpu),
        Some("software") => {
            force_software();
            run(eframe::Renderer::Glow)
        }
        _ => match run(eframe::Renderer::Wgpu) {
            Ok(()) => Ok(()),
            Err(e) => {
                // wgpu failed (commonly inside an AppImage when the bundled
                // Vulkan ICDs don't match the host GPU — "Parent device is
                // lost"). Retry with the OpenGL backend, which works through
                // EGL/GL on the same mesa bundle.
                log::warn!("wgpu renderer failed ({e}); falling back to OpenGL (glow)");
                match run(eframe::Renderer::Glow) {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        // Even glow failed — last resort is mesa's llvmpipe
                        // software rasterizer, which has no GPU dependency.
                        log::warn!(
                            "glow renderer failed ({e}); falling back to software rendering"
                        );
                        force_software();
                        run(eframe::Renderer::Glow)
                    }
                }
            }
        },
    }
}
