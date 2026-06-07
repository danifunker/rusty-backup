fn main() {
    // Set version at compile time
    // Reads from RELEASE_VERSION env var (set by CI) or falls back to Cargo.toml version
    //
    // Re-run the build script whenever either input changes. This is critical:
    // emitting any `rerun-if-*` directive (see CARGO_FEATURE_CHD below) disables
    // Cargo's default "re-run on any source change" heuristic, so without these
    // lines a cached build-script run pins APP_VERSION forever. In CI the build
    // job runs `cargo test` (no RELEASE_VERSION -> APP_VERSION baked as the
    // 0.1.0 CARGO_PKG_VERSION fallback) before `cargo build` (RELEASE_VERSION
    // set); without rerun-if-env-changed the build step reuses the stale 0.1.0
    // and every released artifact reports 0.1.0, tripping the auto-updater on
    // every launch.
    println!("cargo:rerun-if-env-changed=RELEASE_VERSION");
    println!("cargo:rerun-if-env-changed=CARGO_PKG_VERSION");
    let version = std::env::var("RELEASE_VERSION")
        .unwrap_or_else(|_| std::env::var("CARGO_PKG_VERSION").unwrap());

    // Add -dev suffix for debug builds
    let profile = std::env::var("PROFILE").unwrap_or_default();
    let full_version = if profile == "debug" && std::env::var("RELEASE_VERSION").is_err() {
        format!("{}-dev", version)
    } else {
        version
    };

    println!("cargo:rustc-env=APP_VERSION={}", full_version);

    // Windows-specific icon and resource embedding
    #[cfg(windows)]
    {
        let mut res = winres::WindowsResource::new();
        res.set_icon("assets/icons/icon.ico");

        // Set application info
        res.set("ProductName", "Rusty Backup");
        res.set(
            "FileDescription",
            "Backup and restore vintage computer hard disk images",
        );
        res.set("CompanyName", "dani");

        // Use the APP_VERSION we just set
        res.set("FileVersion", &full_version);
        res.set("ProductVersion", &full_version);

        if let Err(e) = res.compile() {
            eprintln!("Warning: Failed to compile Windows resources: {}", e);
            eprintln!("The .exe will still work but won't have an embedded icon.");
        }
    }

    // libchdman-rs's prebuilt static archive uses C++ `std::thread`, whose
    // symbols (e.g. `std::thread::_M_start_thread`, `std::thread::_State::~_
    // State`) live in `libpthread.so` on glibc < 2.34 (Ubuntu 20.04 ships
    // glibc 2.31). The crate emits `cargo:rustc-link-lib=stdc++` for linux-
    // gnu targets but not `pthread`, so the link step fails on the armv7
    // MiSTer cross-compile with "undefined reference to std::thread::*".
    // Add it here whenever the `chd` feature is on and the target is glibc
    // Linux — applies to the x86_64 / aarch64 desktop builds too, but they
    // already pick up libpthread transitively so it's a no-op there.
    //
    // build.rs sees features via CARGO_FEATURE_<NAME> env vars rather than
    // the `cfg!(feature = ...)` macro, so we check that env var instead.
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_CHD");
    if std::env::var("CARGO_FEATURE_CHD").is_ok() {
        let target = std::env::var("TARGET").unwrap_or_default();
        if target.contains("unknown-linux-gnu") {
            println!("cargo:rustc-link-lib=pthread");
        }
    }

    // NOTE: The GUI no longer embeds a `requireAdministrator` manifest. It used
    // to (release-only, GUI binary only), which forced a UAC prompt at *launch*
    // for every run and — more importantly — made the per-user installer's
    // post-install `[Run]` step fail with "CreateProcess failed; code 740, the
    // requested operation requires elevation" (an un-elevated installer cannot
    // CreateProcess an always-elevated exe).
    //
    // Both `rusty-backup` and `rb-cli` now ship as `asInvoker`: no UAC at
    // launch. Administrator rights are requested *on demand* — the GUI's
    // top-bar "Show Physical Devices" button calls `os::windows::request_
    // elevation()` (whole-process UAC relaunch) only when the user actually
    // wants raw physical-disk access. File-only flows never elevate. See
    // docs/windows_self_update.md (elevation phase).
}
