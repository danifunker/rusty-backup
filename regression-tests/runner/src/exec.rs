//! Subprocess driver.
//!
//! Runs `rb-cli` (or an external oracle tool), captures stdout/stderr in full,
//! and enforces a hard wall-clock budget. A hung case must never hang the run
//! — the whole suite is built around always reaching the end.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct Output {
    /// `None` when the process was killed for exceeding its budget.
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    /// Captured for every step; no report surfaces per-step timing yet.
    #[allow(dead_code)]
    pub duration: Duration,
    pub timed_out: bool,
}

#[derive(Debug)]
pub enum ExecError {
    /// The binary could not be launched at all (missing, not executable).
    Spawn(String),
}

impl std::fmt::Display for ExecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecError::Spawn(m) => write!(f, "could not launch: {}", m),
        }
    }
}

/// Run `program` with `args` in `cwd`, killing it after `timeout`.
pub fn run(
    program: &Path,
    args: &[String],
    cwd: &Path,
    timeout: Duration,
) -> Result<Output, ExecError> {
    let started = Instant::now();

    let mut child = Command::new(program)
        .args(args)
        .current_dir(cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| ExecError::Spawn(format!("{}: {}", program.display(), e)))?;

    // Drain both pipes on their own threads. Reading them inline would
    // deadlock the moment a chatty case fills a pipe buffer while we are
    // blocked reading the other one — and `inspect --format json` on a large
    // image is very capable of filling a pipe buffer.
    let mut stdout_pipe = child.stdout.take();
    let mut stderr_pipe = child.stderr.take();

    let (out_tx, out_rx) = mpsc::channel();
    let out_thread = thread::spawn(move || {
        let mut buf = Vec::new();
        if let Some(p) = stdout_pipe.as_mut() {
            let _ = p.read_to_end(&mut buf);
        }
        let _ = out_tx.send(buf);
    });

    let (err_tx, err_rx) = mpsc::channel();
    let err_thread = thread::spawn(move || {
        let mut buf = Vec::new();
        if let Some(p) = stderr_pipe.as_mut() {
            let _ = p.read_to_end(&mut buf);
        }
        let _ = err_tx.send(buf);
    });

    let mut timed_out = false;
    let exit_code = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status.code(),
            Ok(None) => {
                if started.elapsed() >= timeout {
                    let _ = child.kill();
                    let _ = child.wait();
                    timed_out = true;
                    break None;
                }
                thread::sleep(Duration::from_millis(10));
            }
            Err(e) => return Err(ExecError::Spawn(format!("wait failed: {}", e))),
        }
    };

    let stdout = out_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap_or_default();
    let stderr = err_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap_or_default();
    let _ = out_thread.join();
    let _ = err_thread.join();

    Ok(Output {
        exit_code,
        stdout: String::from_utf8_lossy(&stdout).into_owned(),
        stderr: String::from_utf8_lossy(&stderr).into_owned(),
        duration: started.elapsed(),
        timed_out,
    })
}

/// Is `tool` on PATH? Drives the `requires` field, which degrades a case to
/// `skip-tool` rather than failing it — an absent oracle is not a bug in
/// rb-cli.
pub fn tool_available(tool: &str) -> bool {
    let path = match std::env::var_os("PATH") {
        Some(p) => p,
        None => return false,
    };
    // On Windows a bare name needs an extension; PATHEXT is the authority but
    // the common four cover every oracle we care about.
    let exts: &[&str] = if cfg!(windows) {
        &["", ".exe", ".cmd", ".bat"]
    } else {
        &[""]
    };
    for dir in std::env::split_paths(&path) {
        for ext in exts {
            let candidate = dir.join(format!("{}{}", tool, ext));
            if candidate.is_file() {
                return true;
            }
        }
    }
    false
}

/// Make a path absolute without `canonicalize`, whose `\?\` prefix on Windows
/// breaks tools that parse their own arguments.
///
/// Every subprocess here runs with a scratch or artifact directory as its cwd,
/// and the two platforms disagree about what a relative *program* path means:
/// Windows resolves it against the parent's cwd, Unix against the child's,
/// after the chdir. So `--rb-cli ../../target/release/rb-cli` works on Windows
/// and cannot launch anything on Linux or macOS. Absolutising at the boundary
/// makes the two agree.
pub fn absolutise(p: &Path) -> PathBuf {
    let joined = if p.is_absolute() {
        p.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(p))
            .unwrap_or_else(|_| p.to_path_buf())
    };
    lexically_normalise(&joined)
}

/// Collapse `.` and `..` without touching the filesystem, so a resolved path
/// reads as one place rather than as directions to it. Purely lexical: it must
/// work for paths that do not exist yet, which is most of what `produce` and
/// the scratch tree deal in.
fn lexically_normalise(p: &Path) -> PathBuf {
    use std::path::Component;
    let mut out = PathBuf::new();
    for c in p.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                // Only pop a real directory name; popping past a root or a
                // Windows prefix would silently rewrite the path.
                let poppable = out
                    .components()
                    .next_back()
                    .map(|c| matches!(c, Component::Normal(_)))
                    .unwrap_or(false);
                if poppable {
                    out.pop();
                } else {
                    out.push(c.as_os_str());
                }
            }
            _ => out.push(c.as_os_str()),
        }
    }
    out
}

/// Short platform token used by `platforms` filters and report directory
/// names.
pub fn platform_token() -> &'static str {
    if cfg!(target_os = "windows") {
        "windows"
    } else if cfg!(target_os = "macos") {
        "macos"
    } else if cfg!(target_os = "linux") {
        "linux"
    } else {
        "other"
    }
}
