//! Embeds the manifest: Common Controls 6, per-monitor DPI v2, asInvoker.
//!
//! The icon and version resource come from the engine's own build script, which links its
//! resources into every binary that depends on it; a second resource here would collide.

fn main() {
    println!("cargo:rerun-if-changed=rusty-backup-win32.manifest");
    let target = std::env::var("TARGET").unwrap_or_default();
    if target.contains("windows-msvc") {
        let manifest = std::path::Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap())
            .join("rusty-backup-win32.manifest");
        println!("cargo:rustc-link-arg-bins=/MANIFEST:EMBED");
        println!(
            "cargo:rustc-link-arg-bins=/MANIFESTINPUT:{}",
            manifest.display()
        );
    }
}
