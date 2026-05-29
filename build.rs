fn main() {
    // Set version at compile time
    // Reads from RELEASE_VERSION env var (set by CI) or falls back to Cargo.toml version
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

    #[cfg(all(target_os = "windows", not(debug_assertions)))]
    {
        // Embed the requireAdministrator manifest in release builds — but ONLY
        // into the GUI binary (`rusty-backup`), NOT the CLI (`rb-cli`).
        //
        // The manifest declares `requireAdministrator`, which makes Windows fire
        // a UAC prompt at *launch*, before any code runs, for every invocation.
        // That is fine for the GUI (which always needs disk access), but the CLI
        // mostly operates on plain image files and should not prompt for admin
        // just to `inspect foo.img`. `compile_for` links the resource into the
        // named binaries only; `rb-cli` is left with the default `asInvoker`
        // execution level (no prompt at launch — it inherits admin when started
        // from an already-elevated terminal, which is what physical-disk ops need).
        //
        // The embed-resource crate needs a .rc file, not a raw manifest.
        let manifest_path = std::path::PathBuf::from("rusty-backup.manifest");
        if manifest_path.exists() {
            embed_resource::compile_for("rusty-backup.rc", &["rusty-backup"], embed_resource::NONE);
        }
    }
}
