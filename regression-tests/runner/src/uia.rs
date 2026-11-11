//! The Win32 driver's client: runs `drivers/uia_driver.py`, one JSON op per line, as `gtk.rs` does.

use crate::flow::UiError;
use crate::gtk::Driver;
use crate::report::Verdict;
use std::path::Path;
use std::process::Command;

/// Start the UIA driver on the Win32 shell at `win32`; Windows only, with Python and comtypes.
pub fn start(win32: &Path, work: &Path) -> Result<Driver, UiError> {
    if !cfg!(windows) {
        return Err(UiError::Skip(
            Verdict::SkipPlatform,
            "the Win32 shell runs on Windows only".to_string(),
        ));
    }
    if !win32.exists() {
        return Err(UiError::Skip(
            Verdict::SkipTool,
            format!(
                "no Win32 shell at {} (build frontends/win32, or pass --win32)",
                win32.display()
            ),
        ));
    }
    if !crate::exec::tool_available("python") {
        return Err(UiError::Skip(
            Verdict::SkipTool,
            "the Win32 driver needs 'python' (with comtypes) on PATH".to_string(),
        ));
    }
    let script = crate::regression_dir()
        .join("drivers")
        .join("uia_driver.py");
    let mut cmd = Command::new("python");
    cmd.arg("-I").arg(&script).arg(win32).arg(work);
    Driver::launch(cmd, work, "Win32")
}
