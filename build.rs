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

    lion_compat();

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

// The vintage 10.7 build links rb-cli-vintage/lion_compat.cpp: what libc++ and libSystem on Lion lack.
fn lion_compat() {
    let dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_default();
    let src = std::path::Path::new(&dir).join("lion_compat.cpp");
    println!("cargo:rerun-if-changed={}", src.display());
    println!("cargo:rerun-if-env-changed=MACOSX_DEPLOYMENT_TARGET");
    let min = std::env::var("MACOSX_DEPLOYMENT_TARGET").unwrap_or_default();
    let target = std::env::var("TARGET").unwrap_or_default();
    if !src.exists() || target != "x86_64-apple-darwin" || !min.starts_with("10.") {
        return;
    }
    let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let obj = out.join("lion_compat.o");
    let run = |cmd: &mut std::process::Command| {
        let ok = cmd.status().map(|s| s.success()).unwrap_or(false);
        assert!(ok, "lion_compat: {:?} failed", cmd);
    };
    run(std::process::Command::new("clang++")
        .args([
            "-arch",
            "x86_64",
            "-O2",
            "-std=c++17",
            "-D_LIBCPP_DISABLE_AVAILABILITY",
            "-c",
        ])
        .arg(format!("-mmacosx-version-min={}", min))
        .arg(&src)
        .arg("-o")
        .arg(&obj));
    run(std::process::Command::new("ar")
        .arg("crs")
        .arg(out.join("liblion_compat.a"))
        .arg(&obj));
    println!("cargo:rustc-link-search=native={}", out.display());
    println!("cargo:rustc-link-lib=static=lion_compat");
}
