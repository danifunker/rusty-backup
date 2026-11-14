//! The AppKit driver's client: builds `drivers/ax_driver.swift` once with `swiftc`, then runs it,
//! one JSON op per line, as `uia.rs` runs the Win32 one.

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
