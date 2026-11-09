//! The GTK driver's client: runs `drivers/gtk_driver.py` under Xvfb and D-Bus, one JSON op per line.

use crate::flow::UiError;
use crate::report::Verdict;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{channel, Receiver};
use std::time::Duration;

/// Xvfb's screen: tall enough that no GTK page needs scrolling (scripts/gtk4-gui-suite.sh).
const SCREEN: &str = "-screen 0 1600x2300x24";

pub struct Driver {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Receiver<String>,
    work: PathBuf,
}

impl Driver {
    pub fn start(gtk: &Path, work: &Path) -> Result<Driver, UiError> {
        if !gtk.exists() {
            return Err(UiError::Skip(
                Verdict::SkipTool,
                format!("no GTK shell at {} (build frontends/gtk4, or pass --gtk)", gtk.display()),
            ));
        }
        for tool in ["xvfb-run", "dbus-run-session", "xdotool", "python3"] {
            if !crate::exec::tool_available(tool) {
                return Err(UiError::Skip(
                    Verdict::SkipTool,
                    format!("the GTK driver needs '{}' on PATH", tool),
                ));
            }
        }
        let script = crate::regression_dir().join("drivers").join("gtk_driver.py");
        let mut cmd = Command::new("dbus-run-session");
        cmd.args(["--", "xvfb-run", "-a", "-s", SCREEN, "python3", "-I"])
            .arg(&script)
            .arg(gtk)
            .arg(work)
            .env_remove("DISPLAY")
            .env_remove("WAYLAND_DISPLAY");
        Driver::launch(cmd, work, "GTK")
    }

    /// Start a driver script built by `cmd`; it answers `{"ready": true}` once the app is up.
    pub fn launch(mut cmd: Command, work: &Path, what: &str) -> Result<Driver, UiError> {
        let _ = std::fs::remove_dir_all(work);
        std::fs::create_dir_all(work)
            .map_err(|e| UiError::Skip(Verdict::Error, format!("{}: {}", work.display(), e)))?;
        let mut child = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(
                std::fs::File::create(work.join("driver.log"))
                    .map_err(|e| UiError::Skip(Verdict::Error, e.to_string()))?,
            ))
            .spawn()
            .map_err(|e| UiError::Skip(Verdict::Error, format!("{} driver: {}", what, e)))?;
        let stdin = child.stdin.take().expect("piped");
        let stdout = child.stdout.take().expect("piped");
        let (tx, rx) = channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let sent = match line {
                    Ok(l) => tx.send(l).is_ok(),
                    Err(_) => false,
                };
                if !sent {
                    break;
                }
            }
        });
        let mut d = Driver {
            child,
            stdin: Some(stdin),
            lines: rx,
            work: work.to_path_buf(),
        };
        let ready = d.reply(Duration::from_secs(90)).map_err(|why| {
            UiError::Skip(Verdict::Error, format!("{} driver did not start: {}", what, why))
        })?;
        if ready.get("ready").and_then(|v| v.as_bool()) != Some(true) {
            let why = ready.get("why").and_then(|v| v.as_str()).unwrap_or("no reason given");
            return Err(UiError::Skip(Verdict::Error, format!("{} driver: {}", what, why)));
        }
        Ok(d)
    }

    fn reply(&mut self, limit: Duration) -> Result<serde_json::Value, String> {
        loop {
            let line = self
                .lines
                .recv_timeout(limit)
                .map_err(|_| format!("no answer within {:?}\n{}", limit, self.log()))?;
            // Python or GTK may print a stray line; only JSON objects are answers.
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) {
                if v.is_object() {
                    return Ok(v);
                }
            }
        }
    }

    /// Run one op; on failure, (expected, what the window showed).
    pub fn send(&mut self, op: &serde_json::Value, limit: Duration) -> Result<(), (String, String)> {
        let line = format!("{}\n", op);
        let stdin = match self.stdin.as_mut() {
            Some(s) => s,
            None => return Err(("the GTK driver running".to_string(), "closed".to_string())),
        };
        stdin
            .write_all(line.as_bytes())
            .and_then(|_| stdin.flush())
            .map_err(|e| ("the GTK driver to take the op".to_string(), e.to_string()))?;
        let r = self
            .reply(limit + Duration::from_secs(30))
            .map_err(|e| ("an answer from the GTK driver".to_string(), e))?;
        if r.get("ok").and_then(|v| v.as_bool()) == Some(true) {
            return Ok(());
        }
        let s = |k: &str| r.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string();
        Err((s("expected"), s("observed")))
    }

    /// The driver's and the app's logs, for the failure bundle.
    pub fn log(&self) -> String {
        let read = |n: &str| std::fs::read_to_string(self.work.join(n)).unwrap_or_default();
        let mut out = format!("--- driver.log ---\n{}", read("driver.log"));
        for app in ["gtk-app.log", "win32-app.log"] {
            if self.work.join(app).exists() {
                out.push_str(&format!("\n--- {} ---\n{}", app, read(app)));
            }
        }
        out
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        // Closing stdin ends the driver's loop, which stops the app; the kill is the backstop.
        drop(self.stdin.take());
        for _ in 0..50 {
            if matches!(self.child.try_wait(), Ok(Some(_))) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
