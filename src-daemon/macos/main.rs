//! macOS privileged helper daemon for Rusty Backup.
//!
//! This daemon runs as root via launchd and handles all privileged disk
//! operations. It communicates with the main app via Unix domain sockets.
//!
//! Socket: /var/run/rustybackup.sock (managed by launchd)
//! Protocol: JSON messages over socket (one request/response per connection)
//! Lifecycle: On-demand socket activation - launchd starts daemon when socket
//!            is accessed, daemon exits after idle timeout

#![cfg(target_os = "macos")]
#![crate_name = "rusty_backup_helper"]

mod handler;
mod state;

use rusty_backup::privileged::protocol::{DaemonRequest, DaemonResponse};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const SOCKET_PATH: &str = "/var/run/rustybackup.sock";
const IDLE_TIMEOUT: Duration = Duration::from_secs(30); // Exit after 30s idle

fn main() {
    eprintln!("Rusty Backup Helper v{}", env!("CARGO_PKG_VERSION"));
    eprintln!("Socket activation mode - will exit after {}s idle", IDLE_TIMEOUT.as_secs());
    eprintln!("Checking for launchd socket on FD 3...");
    
    // Get socket from launchd (file descriptor 3 for "Listener")
    let listener = match try_get_launchd_socket() {
        Ok(l) => {
            eprintln!("SUCCESS: Using socket from launchd: {}", SOCKET_PATH);
            l
        }
        Err(e) => {
            eprintln!("No valid launchd socket ({}), creating our own (development mode)", e);
            // Fallback for development: create socket ourselves
            let _ = std::fs::remove_file(SOCKET_PATH);
            match UnixListener::bind(SOCKET_PATH) {
                Ok(l) => {
                    // Set socket permissions (allow all users to connect)
                    if let Err(e) = std::fs::set_permissions(SOCKET_PATH, std::os::unix::fs::PermissionsExt::from_mode(0o666)) {
                        eprintln!("Failed to set socket permissions: {}", e);
                        std::process::exit(1);
                    }
                    l
                }
                Err(e) => {
                    eprintln!("Failed to bind socket {}: {}", SOCKET_PATH, e);
                    std::process::exit(1);
                }
            }
        }
    };
    
    eprintln!("Daemon listening on {}", SOCKET_PATH);
    
    // Set non-blocking mode for idle timeout
    listener.set_nonblocking(true).expect("Failed to set non-blocking");
    
    // Create shared state
    let state = Arc::new(Mutex::new(state::DaemonState::new()));
    let mut last_activity = Instant::now();
    
    // Accept connections with idle timeout
    loop {
        match listener.accept() {
            Ok((stream, _addr)) => {
                last_activity = Instant::now();
                let state = Arc::clone(&state);
                std::thread::spawn(move || {
                    handle_client(stream, state);
                });
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No connection available, check idle timeout
                if last_activity.elapsed() > IDLE_TIMEOUT {
                    eprintln!("Idle timeout reached, exiting daemon");
                    std::process::exit(0);
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => {
                eprintln!("Connection error: {}", e);
            }
        }
    }
}

/// Get the socket from launchd via file descriptor 3.
/// Launchd passes sockets as file descriptors, first socket is at FD 3.
/// Returns Ok if we successfully got a socket from launchd, Err otherwise.
fn try_get_launchd_socket() -> Result<UnixListener, Box<dyn std::error::Error>> {
    use std::os::unix::io::{FromRawFd, IntoRawFd};
    
    // Try to use file descriptor 3 (first launchd socket)
    unsafe {
        // First, check if FD 3 is valid before taking ownership
        use std::os::unix::io::RawFd;
        let fd: RawFd = 3;
        
        // Try to get socket name without taking ownership
        let mut addr: libc::sockaddr_un = std::mem::zeroed();
        let mut addr_len: libc::socklen_t = std::mem::size_of::<libc::sockaddr_un>() as libc::socklen_t;
        
        if libc::getsockname(fd, &mut addr as *mut _ as *mut libc::sockaddr, &mut addr_len) == 0 {
            // FD 3 is a valid socket, take ownership
            let listener = UnixListener::from_raw_fd(fd);
            eprintln!("Successfully acquired socket from launchd (FD 3)");
            Ok(listener)
        } else {
            // FD 3 is not a valid socket - don't take ownership
            Err("FD 3 is not a valid socket (launchd socket activation not active)".into())
        }
    }
}

fn handle_client(stream: UnixStream, state: Arc<Mutex<state::DaemonState>>) {
    let mut reader = BufReader::new(stream.try_clone().expect("Failed to clone stream"));
    let mut writer = stream;
    
    // Read JSON request (one line)
    let mut request_json = String::new();
    if let Err(e) = reader.read_line(&mut request_json) {
        eprintln!("Failed to read request: {}", e);
        return;
    }
    
    // Parse request
    let request: DaemonRequest = match serde_json::from_str(&request_json) {
        Ok(req) => req,
        Err(e) => {
            eprintln!("Failed to parse request: {}", e);
            let error = DaemonResponse::Error {
                message: format!("Invalid JSON: {}", e),
            };
            let _ = writeln!(writer, "{}", serde_json::to_string(&error).unwrap());
            return;
        }
    };
    
    // Handle request
    let mut state = state.lock().unwrap();
    let response = state.handle_request(request);
    
    // Send response
    let response_json = match serde_json::to_string(&response) {
        Ok(json) => json,
        Err(e) => {
            eprintln!("Failed to serialize response: {}", e);
            return;
        }
    };
    
    if let Err(e) = writeln!(writer, "{}", response_json) {
        eprintln!("Failed to write response: {}", e);
    }
}

