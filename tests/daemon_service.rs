//! End-to-end proof of the rb-daemon **execution** path: `service::start` really
//! spawns a detached `rb-cli serve`, `is_running` sees it via the PID file, and
//! `service::stop` SIGTERMs it. Plus an install/uninstall round-trip over a temp
//! `user-startup.sh`. Unix-only (the boot wiring is MiSTer/Linux-shaped) and
//! gated behind the `remote` feature like the rest of the daemon.

#![cfg(all(feature = "remote", unix))]

use std::net::TcpListener;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use rusty_backup::remote::service::{
    self as svc, has_section, DaemonConfig, ServicePaths, SERVICE_NAME,
};

/// Grab a momentarily-free localhost port for the test daemon to bind.
fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn wait_until(mut cond: impl FnMut() -> bool, max: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < max {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    cond()
}

fn temp_paths(dir: &std::path::Path, port: u16) -> ServicePaths {
    let ini = dir.join("rb-daemon.ini");
    let cfg = DaemonConfig {
        bind: format!("127.0.0.1:{port}"),
        root: dir.to_path_buf(),
        writable: true,
        staging_dir: None,
        token: None,
    };
    std::fs::write(&ini, cfg.to_ini_str()).unwrap();
    ServicePaths {
        exe: PathBuf::from(env!("CARGO_BIN_EXE_rb-cli")),
        pid_file: dir.join("rb-daemon.pid"),
        log_file: dir.join("rb-daemon.log"),
        ini_file: ini,
        startup_file: dir.join("user-startup.sh"),
    }
}

#[test]
fn start_then_stop_round_trips_a_real_daemon() {
    let dir = tempfile::tempdir().unwrap();
    let paths = temp_paths(dir.path(), free_port());

    assert!(!svc::is_running(&paths), "should start INACTIVE");

    svc::start(&paths).expect("start the daemon");
    assert!(
        wait_until(|| svc::is_running(&paths), Duration::from_secs(5)),
        "daemon should come up and be visible via the PID file"
    );
    assert!(paths.pid_file.exists(), "PID file written");

    // Starting again while running must be refused.
    assert!(svc::start(&paths).is_err(), "double-start is rejected");

    svc::stop(&paths).expect("stop the daemon");
    assert!(
        wait_until(|| !svc::is_running(&paths), Duration::from_secs(5)),
        "daemon should exit on SIGTERM"
    );
    assert!(!paths.pid_file.exists(), "PID file cleared on stop");
}

#[test]
fn install_uninstall_edits_only_our_startup_section() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let paths = temp_paths(dir.path(), port);

    // Seed user-startup.sh with another tool's section to prove we don't touch it.
    let seed = "#!/bin/sh\n\n# other_tool\n/media/fat/Scripts/other.sh\n";
    std::fs::write(&paths.startup_file, seed).unwrap();

    svc::install(&paths).expect("install autostart");
    let after_install = std::fs::read_to_string(&paths.startup_file).unwrap();
    assert!(
        has_section(&after_install, SERVICE_NAME),
        "our section added"
    );
    assert!(
        has_section(&after_install, "other_tool"),
        "other left intact"
    );
    assert!(
        after_install.contains("serve service start"),
        "boot line launches the service"
    );
    // install() also starts the daemon.
    assert!(wait_until(
        || svc::is_running(&paths),
        Duration::from_secs(5)
    ));

    // Uninstall removes only our section and leaves the running daemon alone.
    svc::uninstall(&paths).expect("uninstall autostart");
    let after_uninstall = std::fs::read_to_string(&paths.startup_file).unwrap();
    assert!(
        !has_section(&after_uninstall, SERVICE_NAME),
        "our section gone"
    );
    assert!(
        has_section(&after_uninstall, "other_tool"),
        "other still there"
    );
    assert!(
        svc::is_running(&paths),
        "uninstalling autostart must not kill a live daemon"
    );

    svc::stop(&paths).expect("clean up the test daemon");
    assert!(wait_until(
        || !svc::is_running(&paths),
        Duration::from_secs(5)
    ));
}
