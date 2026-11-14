//! The AppKit driver's client: builds `drivers/ax_driver.swift` once with `swiftc`, then runs it,
//! one JSON op per line, as `uia.rs` runs the Win32 one. The vintage Cocoa shell's driver is the
//! same in Objective-C (`drivers/ax_driver.m`), built with clang so it also runs on 10.7.

use crate::flow::UiError;
use crate::gtk::Driver;
use crate::report::Verdict;
use std::path::{Path, PathBuf};
use std::process::Command;

/// The driver binary, rebuilt when its source is newer; kept beside the runner's own build.
fn driver_binary() -> Result<PathBuf, UiError> {
    let source = crate::regression_dir()
        .join("drivers")
        .join("ax_driver.swift");
    let out = crate::regression_dir()
        .join("runner")
        .join("target")
        .join("ax_driver");
    let modified = |p: &Path| std::fs::metadata(p).and_then(|m| m.modified()).ok();
    if modified(&out).is_some() && modified(&out) >= modified(&source) {
        return Ok(out);
    }
    if let Some(dir) = out.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    let status = Command::new("swiftc")
        .arg("-O")
        .arg(&source)
        .arg("-o")
        .arg(&out)
        .status()
        .map_err(|e| UiError::Skip(Verdict::SkipTool, format!("swiftc: {}", e)))?;
    if !status.success() {
        return Err(UiError::Skip(
            Verdict::Error,
            format!("swiftc could not build {}", source.display()),
        ));
    }
    Ok(out)
}

/// Start the AX driver on the AppKit shell at `appkit`; macOS only, with swiftc and the AX permission.
pub fn start(appkit: &Path, work: &Path) -> Result<Driver, UiError> {
    if !cfg!(target_os = "macos") {
        return Err(UiError::Skip(
            Verdict::SkipPlatform,
            "the AppKit shell runs on macOS only".to_string(),
        ));
    }
    if !appkit.exists() {
        return Err(UiError::Skip(
            Verdict::SkipTool,
            format!(
                "no AppKit shell at {} (build frontends/appkit, or pass --appkit)",
                appkit.display()
            ),
        ));
    }
    if !crate::exec::tool_available("swiftc") {
        return Err(UiError::Skip(
            Verdict::SkipTool,
            "the AppKit driver needs 'swiftc' (the Xcode command-line tools)".to_string(),
        ));
    }
    let mut cmd = Command::new(driver_binary()?);
    cmd.arg(appkit).arg(work);
    Driver::launch(cmd, work, "AppKit")
}

/// The Objective-C driver for the vintage Cocoa shell, built with clang for 10.7 and up.
fn objc_driver_binary() -> Result<PathBuf, UiError> {
    let source = crate::regression_dir().join("drivers").join("ax_driver.m");
    let out = crate::regression_dir()
        .join("runner")
        .join("target")
        .join("ax_driver_objc");
    let modified = |p: &Path| std::fs::metadata(p).and_then(|m| m.modified()).ok();
    if modified(&out).is_some() && modified(&out) >= modified(&source) {
        return Ok(out);
    }
    if let Some(dir) = out.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    let status = Command::new("clang")
        .args(["-mmacosx-version-min=10.7", "-fno-objc-arc", "-O2"])
        .arg(&source)
        .args([
            "-framework",
            "Foundation",
            "-framework",
            "ApplicationServices",
            "-o",
        ])
        .arg(&out)
        .status()
        .map_err(|e| UiError::Skip(Verdict::SkipTool, format!("clang: {}", e)))?;
    if !status.success() {
        return Err(UiError::Skip(
            Verdict::Error,
            format!("clang could not build {}", source.display()),
        ));
    }
    Ok(out)
}

/// Start the Objective-C AX driver on the vintage Cocoa shell's executable at `cocoa`.
pub fn start_cocoa(cocoa: &Path, work: &Path) -> Result<Driver, UiError> {
    if !cfg!(target_os = "macos") {
        return Err(UiError::Skip(
            Verdict::SkipPlatform,
            "the Cocoa shell runs on macOS only".to_string(),
        ));
    }
    if !cocoa.exists() {
        return Err(UiError::Skip(
            Verdict::SkipTool,
            format!(
                "no Cocoa shell at {} (run frontends-vintage/cocoa/build.sh, or pass --cocoa)",
                cocoa.display()
            ),
        ));
    }
    if !crate::exec::tool_available("clang") {
        return Err(UiError::Skip(
            Verdict::SkipTool,
            "the Cocoa driver needs 'clang' (the Xcode command-line tools)".to_string(),
        ));
    }
    // RB_COCOA_DRIVER swaps in a relay to another Mac (drivers/remote_cocoa.py) for a 10.7 run.
    let mut cmd = match std::env::var_os("RB_COCOA_DRIVER") {
        Some(relay) => Command::new(relay),
        None => Command::new(objc_driver_binary()?),
    };
    cmd.arg(cocoa).arg(work);
    Driver::launch(cmd, work, "Cocoa")
}
