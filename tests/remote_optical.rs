//! Loopback plumbing test for the optical tier (Family O).
//!
//! Drives the client↔daemon optical path over a port-0 `rb-cli serve` listener
//! WITHOUT a physical drive: the daemon (built with `optical`) must handle the
//! optical verbs, opening a bogus device must error cleanly, and the
//! process-global busy guard must be released so a second open isn't wrongly
//! reported "busy". The full rip-a-real-disc validation needs hardware (P1.8 in
//! docs/remote_ripping.md).
#![cfg(all(feature = "remote", feature = "optical"))]

use std::net::TcpListener;

use rusty_backup::remote::protocol::WireRetryConfig;
use rusty_backup::remote::{serve_on, RemoteConnection};

/// Bind a port-0 listener (so the connect lands in the listen backlog with no
/// sleep/race) and spawn the daemon on it. The returned `TempDir` guard must be
/// kept alive for the daemon's lifetime.
fn spawn_daemon() -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        let _ = serve_on(listener, root, None);
    });
    (addr, dir)
}

fn test_retry() -> WireRetryConfig {
    WireRetryConfig {
        max_attempts: 1,
        initial_backoff_ms: 0,
        max_backoff_ms: 0,
        reduce_chunk_on_retry: false,
        min_sectors_per_read: 1,
    }
}

/// The daemon (built with `optical`) handles `ListOpticalDrives` — it round-trips
/// over the wire instead of disclaiming the feature.
#[test]
fn optical_list_drives_round_trips_over_loopback() {
    let (addr, _dir) = spawn_daemon();
    let mut conn = RemoteConnection::connect(&addr).unwrap();
    // On a driveless CI box the list may be empty (Ok) or enumeration may error
    // (Err) per platform — both prove the wire path works. What must NOT happen
    // is the daemon claiming it lacks the optical feature.
    if let Err(e) = conn.list_optical_drives() {
        let msg = format!("{e:#}");
        assert!(
            !msg.contains("built without the optical feature"),
            "an optical-built daemon should not disclaim the feature: {msg}"
        );
    }
}

/// Opening a device that cannot exist errors cleanly, and the process-global
/// busy guard is released so a *second* open fails at open — not with "busy".
#[test]
fn optical_open_bogus_device_errors_and_releases_busy_guard() {
    let (addr, _dir) = spawn_daemon();
    let mut conn = RemoteConnection::connect(&addr).unwrap();

    let first = conn.open_optical("/dev/rb-test-no-such-optical-xyz", test_retry());
    assert!(first.is_err(), "opening a bogus device should error");

    let err = conn
        .open_optical("/dev/rb-test-no-such-optical-xyz", test_retry())
        .expect_err("second open of a bogus device should still error");
    let msg = format!("{err:#}").to_lowercase();
    assert!(
        !msg.contains("busy"),
        "busy guard was not released after a failed open: {msg}"
    );
}
