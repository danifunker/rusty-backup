//! Daemon lifecycle + boot integration for `rb-cli serve` (the MiSTer
//! "rb-daemon").
//!
//! This is the **execution** half of the MiSTer daemon: install it so it starts
//! every boot, start/stop/restart it on demand, and report whether it is running
//! and reachable. The interactive **UI** half (the Scripts-menu console) lives in
//! `src/cli/verbs/setup.rs` and drives the functions here.
//!
//! Modeled on mrext's `pkg/service` + `pkg/mister/startup.go` so it behaves like
//! every other MiSTer Scripts tool:
//!
//! - **Autostart** is one named section appended to
//!   `/media/fat/linux/user-startup.sh` (mrext's `Startup.AddService`): a `# name`
//!   header line followed by the launch command, blank-line delimited so it sits
//!   beside other tools' sections without clobbering them. "Enabled on boot" =
//!   the section is present; uninstall deletes just that section.
//! - **Liveness** is a PID file at `/tmp/rb-daemon.pid` plus a `kill(pid, 0)`
//!   probe (mrext's `Running()`), so a crashed daemon reads as inactive and a
//!   stale PID file is harmless.
//! - **Start** spawns `rb-cli serve` detached (its own session via `setsid`,
//!   stdio redirected to `/tmp/rb-daemon.log`) and records the child PID.
//!
//! The string/INI/PID helpers are pure and unit-tested; the FS/process glue is
//! parameterized by [`ServicePaths`] so it can be pointed at a temp dir.
//!
//! Cross-platform note: the autostart/boot wiring here is MiSTer/Linux-shaped
//! (the only packaged target today). The pure helpers are OS-agnostic; other
//! platforms can grow their own [`ServicePaths`] + launch integration later.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

/// The autostart section name written into `user-startup.sh` and shown in logs.
pub const SERVICE_NAME: &str = "rb-daemon";

/// Default daemon port (mrext owns 8182; plan Appendix B.1).
pub const DEFAULT_PORT: u16 = 7341;

// ---- MiSTer canonical paths (mrext pkg/config/mister.go) --------------------

/// `/media/fat` — the SD-card root inside the MiSTer's Linux userland.
pub const SD_ROOT: &str = "/media/fat";
/// `/media/fat/Scripts` — what the MiSTer "Scripts" menu scans (only `*.sh`
/// files show, so a bare `rb-cli` here is invisible to the menu).
pub const SCRIPTS_DIR: &str = "/media/fat/Scripts";
/// `/media/fat/linux/user-startup.sh` — mrext's `config.StartupFile`.
pub const STARTUP_FILE: &str = "/media/fat/linux/user-startup.sh";
/// Config sidecar next to the binary.
pub const INI_FILE: &str = "/media/fat/Scripts/rb-daemon.ini";
/// PID file (mrext's `/tmp/%s.pid`).
pub const PID_FILE: &str = "/tmp/rb-daemon.pid";
/// Log file the detached daemon's stdout/stderr is redirected to.
pub const LOG_FILE: &str = "/tmp/rb-daemon.log";

// ---- Config (rb-daemon.ini) -------------------------------------------------

/// The daemon's on-disk configuration, parsed from `rb-daemon.ini`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonConfig {
    /// Bind address, e.g. `0.0.0.0:7341`.
    pub bind: String,
    /// Root directory served (sandbox); `/media/fat` on a MiSTer.
    pub root: PathBuf,
    /// Whether write/apply is permitted (plan §13.1 — on by default).
    pub writable: bool,
    /// Staging dir for uploads; `None` = system temp.
    pub staging_dir: Option<PathBuf>,
    /// Optional shared token (future hardening; carried in `Hello`).
    pub token: Option<String>,
}

impl DaemonConfig {
    /// MiSTer defaults: serve the whole SD card on all interfaces, writable.
    pub fn mister_default() -> Self {
        DaemonConfig {
            bind: format!("0.0.0.0:{DEFAULT_PORT}"),
            root: PathBuf::from(SD_ROOT),
            writable: true,
            staging_dir: None,
            token: None,
        }
    }

    /// Parse `key = value` INI text over the MiSTer defaults. Unknown keys,
    /// blank lines, `#`/`;` comments and `[section]` headers are ignored, so a
    /// hand-edited or partial file still yields a usable config.
    pub fn from_ini_str(text: &str) -> Self {
        let mut cfg = DaemonConfig::mister_default();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty()
                || line.starts_with('#')
                || line.starts_with(';')
                || line.starts_with('[')
            {
                continue;
            }
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            let key = key.trim().to_ascii_lowercase();
            let value = value.trim();
            match key.as_str() {
                "bind" if !value.is_empty() => cfg.bind = value.to_string(),
                "root" if !value.is_empty() => cfg.root = PathBuf::from(value),
                "writable" => cfg.writable = parse_bool(value),
                "staging_dir" => {
                    cfg.staging_dir = (!value.is_empty()).then(|| PathBuf::from(value));
                }
                "token" => {
                    cfg.token = (!value.is_empty()).then(|| value.to_string());
                }
                _ => {}
            }
        }
        cfg
    }

    /// Render a commented INI file (what `install` writes when none exists).
    pub fn to_ini_str(&self) -> String {
        let mut out = String::new();
        out.push_str("# Rusty Backup daemon (rb-daemon) configuration.\n");
        out.push_str(
            "# Edit and restart the daemon to apply. See docs/remote_transfer_plan.md.\n\n",
        );
        out.push_str(&format!("bind = {}\n", self.bind));
        out.push_str(&format!("root = {}\n", self.root.display()));
        out.push_str(&format!(
            "writable = {}\n",
            if self.writable { "yes" } else { "no" }
        ));
        match &self.staging_dir {
            Some(dir) => out.push_str(&format!("staging_dir = {}\n", dir.display())),
            None => out.push_str("# staging_dir = /media/fat/.rb-daemon-staging\n"),
        }
        match &self.token {
            Some(tok) => out.push_str(&format!("token = {tok}\n")),
            None => out.push_str("# token =\n"),
        }
        out
    }

    /// Load the config from `path`, falling back to MiSTer defaults if it's
    /// missing (a fresh install) or unreadable.
    pub fn load(path: &Path) -> Self {
        match std::fs::read_to_string(path) {
            Ok(text) => DaemonConfig::from_ini_str(&text),
            Err(_) => DaemonConfig::mister_default(),
        }
    }
}

fn parse_bool(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "yes" | "true" | "on" | "y"
    )
}

// ---- user-startup.sh section editing (mrext-compatible) ---------------------

/// True if a `# name` section is present in `user-startup.sh` contents.
pub fn has_section(contents: &str, name: &str) -> bool {
    let header = format!("# {name}");
    contents.lines().any(|l| l.trim() == header)
}

/// Append a `# name` section (header + command) unless one already exists.
/// Idempotent; preserves every other section verbatim, so it sits beside
/// downloader / other Scripts tools without disturbing them.
pub fn add_section(contents: &str, name: &str, cmd: &str) -> String {
    if has_section(contents, name) {
        return contents.to_string();
    }
    let mut out = String::new();
    if contents.trim().is_empty() {
        // mrext starts user-startup.sh with a shebang.
        out.push_str("#!/bin/sh\n");
    } else {
        out.push_str(contents.trim_end());
        out.push('\n');
    }
    out.push('\n');
    out.push_str(&format!("# {name}\n{cmd}\n"));
    out
}

/// Remove the `# name` section (header through its trailing blank line),
/// leaving the rest of the file intact. A no-op if the section is absent.
pub fn remove_section(contents: &str, name: &str) -> String {
    let header = format!("# {name}");
    let lines: Vec<&str> = contents.lines().collect();
    let mut out: Vec<&str> = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        if lines[i].trim() == header {
            // Skip the header and its command block up to a blank line / EOF.
            i += 1;
            while i < lines.len() && !lines[i].trim().is_empty() {
                i += 1;
            }
            // Consume one trailing blank separator if present.
            if i < lines.len() && lines[i].trim().is_empty() {
                i += 1;
            }
            // Collapse the now-doubled blank line that preceded the section.
            while out.last().is_some_and(|l| l.trim().is_empty()) {
                out.pop();
            }
            continue;
        }
        out.push(lines[i]);
        i += 1;
    }
    let mut joined = out.join("\n");
    if !joined.is_empty() && !joined.ends_with('\n') {
        joined.push('\n');
    }
    joined
}

// ---- PID file + liveness (mrext pkg/service) --------------------------------

/// Read a PID from `path`, if the file exists and parses.
pub fn read_pid(path: &Path) -> Option<i32> {
    let text = std::fs::read_to_string(path).ok()?;
    text.trim().parse::<i32>().ok()
}

/// True if a process with `pid` exists (mrext's `signal(0)` probe).
#[cfg(unix)]
pub fn pid_alive(pid: i32) -> bool {
    // kill(pid, 0) -> 0 if the process exists and we may signal it; ESRCH if
    // it's gone. EPERM (exists, not ours) still means alive.
    let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if rc == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(not(unix))]
pub fn pid_alive(_pid: i32) -> bool {
    false
}

// ---- IPv4 discovery (so the console can print "connect to: …") --------------

/// A discovered local IPv4 address and the interface it's on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalAddr {
    pub iface: String,
    pub ip: String,
}

/// Enumerate non-loopback IPv4 addresses via `getifaddrs`. Empty if the host
/// has no network up (the console then says so rather than printing nothing).
#[cfg(unix)]
pub fn local_ipv4_addrs() -> Vec<LocalAddr> {
    use std::ffi::CStr;
    use std::net::Ipv4Addr;

    let mut out = Vec::new();
    unsafe {
        let mut ifap: *mut libc::ifaddrs = std::ptr::null_mut();
        if libc::getifaddrs(&mut ifap) != 0 {
            return out;
        }
        let mut cur = ifap;
        while !cur.is_null() {
            let ifa = &*cur;
            if !ifa.ifa_addr.is_null() && (*ifa.ifa_addr).sa_family as i32 == libc::AF_INET {
                let sin = ifa.ifa_addr as *const libc::sockaddr_in;
                // s_addr is in network byte order; from_be makes the high byte
                // the first octet for Ipv4Addr::from(u32).
                let ip = Ipv4Addr::from(u32::from_be((*sin).sin_addr.s_addr));
                if !ip.is_loopback() && !ip.is_unspecified() {
                    let iface = if ifa.ifa_name.is_null() {
                        String::new()
                    } else {
                        CStr::from_ptr(ifa.ifa_name).to_string_lossy().into_owned()
                    };
                    out.push(LocalAddr {
                        iface,
                        ip: ip.to_string(),
                    });
                }
            }
            cur = ifa.ifa_next;
        }
        libc::freeifaddrs(ifap);
    }
    out
}

#[cfg(not(unix))]
pub fn local_ipv4_addrs() -> Vec<LocalAddr> {
    Vec::new()
}

/// The port component of a `host:port` bind string, defaulting to [`DEFAULT_PORT`].
pub fn bind_port(bind: &str) -> u16 {
    bind.rsplit_once(':')
        .and_then(|(_, p)| p.parse().ok())
        .unwrap_or(DEFAULT_PORT)
}

// ---- Filesystem-backed paths + lifecycle ------------------------------------

/// Where the lifecycle operations read/write. Real runs use [`ServicePaths::detect`]
/// (current exe + MiSTer constants); tests point it at a temp dir.
#[derive(Debug, Clone)]
pub struct ServicePaths {
    /// The `rb-cli` binary to relaunch as the daemon.
    pub exe: PathBuf,
    pub pid_file: PathBuf,
    pub log_file: PathBuf,
    pub ini_file: PathBuf,
    pub startup_file: PathBuf,
}

impl ServicePaths {
    /// Production paths: this running binary + the MiSTer constants.
    pub fn detect() -> Result<Self> {
        let exe = std::env::current_exe().context("locating the running rb-cli binary")?;
        Ok(ServicePaths {
            exe,
            pid_file: PathBuf::from(PID_FILE),
            log_file: PathBuf::from(LOG_FILE),
            ini_file: PathBuf::from(INI_FILE),
            startup_file: PathBuf::from(STARTUP_FILE),
        })
    }

    /// The exact line appended to `user-startup.sh`. mrext form: guard the path
    /// then launch with the service arg, so a missing binary degrades to a
    /// no-op boot rather than an error.
    fn startup_cmd(&self) -> String {
        let exe = self.exe.display();
        format!("[[ -e {exe} ]] && {exe} serve service start")
    }
}

/// Snapshot of the daemon's state for the console / `status` verb.
#[derive(Debug, Clone)]
pub struct ServiceStatus {
    /// Running right now (PID file present and the process is alive).
    pub active: bool,
    /// The live PID, if active.
    pub pid: Option<i32>,
    /// Set to start on boot (autostart section present in `user-startup.sh`).
    pub enabled: bool,
    /// Effective bind address from the INI.
    pub bind: String,
    /// Discovered LAN addresses to connect to.
    pub addrs: Vec<LocalAddr>,
}

/// Read current status without changing anything ("default to maintain existing
/// configuration", plan §14).
pub fn status(paths: &ServicePaths) -> ServiceStatus {
    let pid = read_pid(&paths.pid_file).filter(|&p| pid_alive(p));
    let enabled = std::fs::read_to_string(&paths.startup_file)
        .map(|c| has_section(&c, SERVICE_NAME))
        .unwrap_or(false);
    let cfg = DaemonConfig::load(&paths.ini_file);
    ServiceStatus {
        active: pid.is_some(),
        pid,
        enabled,
        bind: cfg.bind,
        addrs: local_ipv4_addrs(),
    }
}

/// One menu action the setup console offers. Which ones are shown depends on
/// the live state (you can't Start an already-running daemon).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupAction {
    StartNow,
    StopNow,
    InstallAutostart,
    UninstallAutostart,
    Quit,
}

impl SetupAction {
    /// ASCII label shown in the console (no Unicode — egui/console font rule).
    pub fn label(self) -> &'static str {
        match self {
            SetupAction::StartNow => "Start Now",
            SetupAction::StopNow => "Stop Now",
            SetupAction::InstallAutostart => "Install Autostart (start on every boot)",
            SetupAction::UninstallAutostart => "Uninstall Autostart",
            SetupAction::Quit => "Quit",
        }
    }
}

/// The contextual action list for the current state: a start/stop toggle, an
/// install/uninstall-autostart toggle, then Quit.
pub fn setup_actions(status: &ServiceStatus) -> Vec<SetupAction> {
    let mut actions = Vec::with_capacity(3);
    actions.push(if status.active {
        SetupAction::StopNow
    } else {
        SetupAction::StartNow
    });
    actions.push(if status.enabled {
        SetupAction::UninstallAutostart
    } else {
        SetupAction::InstallAutostart
    });
    actions.push(SetupAction::Quit);
    actions
}

/// Run a console action against the live daemon, returning a one-line result for
/// the status bar. `Quit` is handled by the caller and never reaches here.
pub fn apply_setup_action(paths: &ServicePaths, action: SetupAction) -> Result<String> {
    match action {
        SetupAction::StartNow => {
            start(paths)?;
            Ok("Daemon started.".to_string())
        }
        SetupAction::StopNow => {
            stop(paths)?;
            Ok("Daemon stopped.".to_string())
        }
        SetupAction::InstallAutostart => {
            install(paths)?;
            Ok("Installed for autostart on boot, and started now.".to_string())
        }
        SetupAction::UninstallAutostart => {
            uninstall(paths)?;
            Ok("Autostart removed. A running daemon was left alone.".to_string())
        }
        SetupAction::Quit => Ok(String::new()),
    }
}

/// True if the daemon process is currently alive.
pub fn is_running(paths: &ServicePaths) -> bool {
    read_pid(&paths.pid_file).is_some_and(pid_alive)
}

/// Spawn `rb-cli serve` detached and record its PID. Errors if already running.
pub fn start(paths: &ServicePaths) -> Result<()> {
    if is_running(paths) {
        bail!("{SERVICE_NAME} is already running");
    }
    // Clear a stale PID file from a previous crash so the new PID lands clean.
    let _ = std::fs::remove_file(&paths.pid_file);

    let cfg = DaemonConfig::load(&paths.ini_file);
    let child = spawn_daemon(paths, &cfg).context("starting the daemon process")?;

    std::fs::write(&paths.pid_file, child.to_string())
        .with_context(|| format!("writing PID file {}", paths.pid_file.display()))?;
    Ok(())
}

/// Stop the running daemon (SIGTERM) and clear the PID file. Errors if it isn't
/// running.
pub fn stop(paths: &ServicePaths) -> Result<()> {
    let Some(pid) = read_pid(&paths.pid_file).filter(|&p| pid_alive(p)) else {
        let _ = std::fs::remove_file(&paths.pid_file);
        bail!("{SERVICE_NAME} is not running");
    };
    signal_term(pid)?;
    let _ = std::fs::remove_file(&paths.pid_file);
    Ok(())
}

/// Stop (if running) then start, waiting for the old process to exit first.
pub fn restart(paths: &ServicePaths) -> Result<()> {
    if is_running(paths) {
        stop(paths)?;
        // Give the old process up to ~3s to release the port.
        for _ in 0..30 {
            if !is_running(paths) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
    start(paths)
}

/// Register autostart: write a default INI if none exists, append the
/// `user-startup.sh` section, and start the daemon now if it isn't already.
pub fn install(paths: &ServicePaths) -> Result<()> {
    if !paths.ini_file.exists() {
        if let Some(parent) = paths.ini_file.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        std::fs::write(&paths.ini_file, DaemonConfig::mister_default().to_ini_str())
            .with_context(|| format!("writing {}", paths.ini_file.display()))?;
    }

    let existing = std::fs::read_to_string(&paths.startup_file).unwrap_or_default();
    if !has_section(&existing, SERVICE_NAME) {
        let updated = add_section(&existing, SERVICE_NAME, &paths.startup_cmd());
        write_startup(&paths.startup_file, &updated)?;
    }

    if !is_running(paths) {
        start(paths)?;
    }
    Ok(())
}

/// Unregister autostart: delete the `user-startup.sh` section. Leaves a running
/// daemon alone (the console offers a separate Stop) — uninstalling boot
/// shouldn't kill the live process.
pub fn uninstall(paths: &ServicePaths) -> Result<()> {
    let Ok(existing) = std::fs::read_to_string(&paths.startup_file) else {
        return Ok(());
    };
    if has_section(&existing, SERVICE_NAME) {
        let updated = remove_section(&existing, SERVICE_NAME);
        write_startup(&paths.startup_file, &updated)?;
    }
    Ok(())
}

fn write_startup(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    std::fs::write(path, contents).with_context(|| format!("writing {}", path.display()))
}

#[cfg(unix)]
fn signal_term(pid: i32) -> Result<()> {
    let rc = unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::ESRCH) {
            return Err(err).with_context(|| format!("sending SIGTERM to {pid}"));
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn signal_term(_pid: i32) -> Result<()> {
    bail!("stopping the daemon is only supported on Unix");
}

/// Launch `rb-cli serve --bind … --root …` in its own session, stdio redirected
/// to the log file, and return the child PID. The child outlives this process.
#[cfg(unix)]
fn spawn_daemon(paths: &ServicePaths, cfg: &DaemonConfig) -> Result<u32> {
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};

    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.log_file)
        .with_context(|| format!("opening log {}", paths.log_file.display()))?;
    let log_err = log.try_clone().context("cloning log handle")?;

    let mut cmd = Command::new(&paths.exe);
    cmd.arg("serve")
        .arg("--bind")
        .arg(&cfg.bind)
        .arg("--root")
        .arg(&cfg.root);
    if let Some(staging) = &cfg.staging_dir {
        cmd.arg("--staging-dir").arg(staging);
    }
    cmd.stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err));
    // setsid() detaches the child from this process's controlling terminal and
    // session, so it keeps running after `service start` returns.
    unsafe {
        cmd.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = cmd.spawn().context("spawning rb-cli serve")?;
    Ok(child.id())
}

#[cfg(not(unix))]
fn spawn_daemon(_paths: &ServicePaths, _cfg: &DaemonConfig) -> Result<u32> {
    bail!("running the daemon as a background service is only supported on Unix");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ini_round_trips_through_defaults() {
        let cfg = DaemonConfig::mister_default();
        let parsed = DaemonConfig::from_ini_str(&cfg.to_ini_str());
        assert_eq!(cfg, parsed);
    }

    #[test]
    fn ini_parses_overrides_and_ignores_noise() {
        let text = "\
# a comment
[rb-daemon]
BIND = 192.168.1.5:9000
root=/media/usb0
writable = no
token = hunter2
bogus = ignored
";
        let cfg = DaemonConfig::from_ini_str(text);
        assert_eq!(cfg.bind, "192.168.1.5:9000");
        assert_eq!(cfg.root, PathBuf::from("/media/usb0"));
        assert!(!cfg.writable);
        assert_eq!(cfg.token.as_deref(), Some("hunter2"));
        assert_eq!(cfg.staging_dir, None);
    }

    #[test]
    fn add_section_is_idempotent_and_preserves_others() {
        let existing = "#!/bin/sh\n\n# other_tool\n/media/fat/Scripts/other.sh\n";
        let once = add_section(existing, SERVICE_NAME, "echo hi");
        assert!(has_section(&once, SERVICE_NAME));
        assert!(has_section(&once, "other_tool"));
        // Second add must not duplicate.
        let twice = add_section(&once, SERVICE_NAME, "echo hi");
        assert_eq!(once, twice);
        assert_eq!(twice.matches("# rb-daemon").count(), 1);
    }

    #[test]
    fn add_section_seeds_shebang_when_empty() {
        let out = add_section("", SERVICE_NAME, "run me");
        assert!(out.starts_with("#!/bin/sh\n"));
        assert!(out.contains("# rb-daemon\nrun me\n"));
    }

    #[test]
    fn remove_section_deletes_only_its_block() {
        let existing = "\
#!/bin/sh

# other_tool
/media/fat/Scripts/other.sh

# rb-daemon
[[ -e /x ]] && /x serve service start

# third
/media/fat/Scripts/third.sh
";
        let out = remove_section(existing, SERVICE_NAME);
        assert!(!has_section(&out, SERVICE_NAME));
        assert!(has_section(&out, "other_tool"));
        assert!(has_section(&out, "third"));
        assert!(!out.contains("serve service start"));
    }

    #[test]
    fn remove_section_absent_is_noop() {
        let existing = "#!/bin/sh\n\n# other\n/x\n";
        assert_eq!(remove_section(existing, SERVICE_NAME), existing.to_string());
    }

    #[test]
    fn add_then_remove_round_trips_other_sections() {
        let base = "#!/bin/sh\n\n# keep\n/keep.sh\n";
        let added = add_section(base, SERVICE_NAME, "/x serve service start");
        let removed = remove_section(&added, SERVICE_NAME);
        assert!(has_section(&removed, "keep"));
        assert!(!has_section(&removed, SERVICE_NAME));
    }

    #[test]
    fn pid_round_trips_and_self_is_alive() {
        let dir = tempfile::tempdir().unwrap();
        let pid_path = dir.path().join("rb-daemon.pid");
        let me = std::process::id() as i32;
        std::fs::write(&pid_path, me.to_string()).unwrap();
        assert_eq!(read_pid(&pid_path), Some(me));
        assert!(pid_alive(me));
    }

    #[test]
    fn dead_pid_reads_as_not_alive() {
        // PID 1 exists; a very large PID almost certainly doesn't.
        assert!(!pid_alive(0x3FFF_FFFF));
    }

    fn status_fixture(active: bool, enabled: bool) -> ServiceStatus {
        ServiceStatus {
            active,
            pid: active.then_some(1234),
            enabled,
            bind: "0.0.0.0:7341".to_string(),
            addrs: Vec::new(),
        }
    }

    #[test]
    fn setup_actions_reflect_state() {
        assert_eq!(
            setup_actions(&status_fixture(false, false)),
            vec![
                SetupAction::StartNow,
                SetupAction::InstallAutostart,
                SetupAction::Quit
            ]
        );
        assert_eq!(
            setup_actions(&status_fixture(true, true)),
            vec![
                SetupAction::StopNow,
                SetupAction::UninstallAutostart,
                SetupAction::Quit
            ]
        );
        assert_eq!(
            setup_actions(&status_fixture(true, false)),
            vec![
                SetupAction::StopNow,
                SetupAction::InstallAutostart,
                SetupAction::Quit
            ]
        );
    }

    #[test]
    fn bind_port_parses_or_defaults() {
        assert_eq!(bind_port("0.0.0.0:7341"), 7341);
        assert_eq!(bind_port("192.168.1.5:9000"), 9000);
        assert_eq!(bind_port("garbage"), DEFAULT_PORT);
    }
}
