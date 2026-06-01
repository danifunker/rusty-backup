// Hide console window on Windows release builds
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod gui;

/// Windows: handle the installer/uninstaller association hooks
/// (`--register-file-associations` / `--unregister-file-associations`) and the
/// launch-time re-registration that picks up newly supported extensions after
/// a self-update. The hook flags register/unregister (HKCU, unprivileged) and
/// exit without opening the GUI; a normal launch only refreshes if the enabled
/// associations were registered for an older build.
#[cfg(windows)]
fn handle_windows_assoc_cli() {
    use rusty_backup::update::UpdateConfig;

    let args: Vec<String> = std::env::args().collect();
    let register = args.iter().any(|a| a == "--register-file-associations");
    let unregister = args.iter().any(|a| a == "--unregister-file-associations");

    if !register && !unregister {
        // Normal launch: refresh associations if enabled but stale.
        let mut cfg = UpdateConfig::load();
        if cfg.file_associations_enabled
            && cfg.assoc_registered_version.as_deref() != Some(env!("APP_VERSION"))
        {
            let _ = rusty_backup::os::file_assoc::register_file_associations();
            cfg.assoc_registered_version = Some(env!("APP_VERSION").to_string());
            let _ = cfg.save();
        }
        return;
    }

    let mut cfg = UpdateConfig::load();
    if register {
        let _ = rusty_backup::os::file_assoc::register_file_associations();
        cfg.file_associations_enabled = true;
        cfg.assoc_registered_version = Some(env!("APP_VERSION").to_string());
    } else {
        let _ = rusty_backup::os::file_assoc::unregister_file_associations();
        cfg.file_associations_enabled = false;
        cfg.assoc_registered_version = None;
    }
    let _ = cfg.save();
    std::process::exit(0);
}

fn main() -> eframe::Result {
    // Windows installer/uninstaller association hooks (and launch-time refresh).
    // Must run before GUI init; the hook flags exit without opening a window.
    #[cfg(windows)]
    handle_windows_assoc_cli();

    // Install a logger that mirrors records to stderr AND buffers them for the
    // GUI log panel (so worker-thread log::info! lines, e.g. from the GHO
    // reader, surface in the UI). Replaces the plain env_logger stderr sink.
    gui::ui_logger::init();
    // Load icon from bytes with transparency preserved
    let icon_bytes = include_bytes!("../assets/icons/icon-256.png");
    let icon_image = image::load_from_memory_with_format(icon_bytes, image::ImageFormat::Png)
        .expect("Failed to load icon");

    // Install a panic hook that prints the panic location AND a backtrace
    // to stderr for every thread that panics — including sub-threads spawned
    // inside the backup worker. Without this, panics in producer/consumer
    // threads die silently and the GUI hangs at "0 B / X GiB" because the
    // top-level worker never sees a failure to propagate. `RUST_BACKTRACE`
    // is set to 1 if the user hasn't already chosen a value, so symbolized
    // frames land in the stderr capture (`tee /tmp/rusty-backup-run.log`).
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        // SAFETY: set before any threads spawn that could read this env var.
        unsafe { std::env::set_var("RUST_BACKTRACE", "1") };
    }
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let thread = std::thread::current();
        let name = thread.name().unwrap_or("<unnamed>");
        eprintln!("=== PANIC in thread '{name}' ===");
        eprintln!("{info}");
        let bt = std::backtrace::Backtrace::force_capture();
        eprintln!("backtrace:\n{bt}");
        eprintln!("=== END PANIC ===");
        // Defer to the default hook too so existing behavior is preserved.
        default_hook(info);
    }));
    // Linux: Request elevation at startup if not already running as root
    #[cfg(target_os = "linux")]
    {
        if !nix::unistd::geteuid().is_root() {
            // Install AppImage desktop integration *before* elevating, while
            // we're still the real user — otherwise the .desktop and icon
            // files would land in ~/.local/share/ owned by root, which
            // breaks future user-mode runs. Wayland has no per-window icon
            // protocol; the compositor resolves icons by app_id → installed
            // .desktop. No-op outside AppImage.
            rusty_backup::os::linux::install_appimage_desktop_integration(icon_bytes);

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
            Box::new(|cc| {
                gui::ui_logger::set_repaint_ctx(cc.egui_ctx.clone());
                Ok(Box::new(gui::RustyBackupApp::default()))
            }),
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
