//! macOS privileged helper daemon for Rusty Backup.
//!
//! This daemon runs as root via launchd and handles all privileged disk
//! operations. It communicates with the main app via Unix domain sockets.
//!
//! Socket: /var/run/rustybackup.sock
//! Protocol: JSON messages over socket (one request/response per connection)
//! Security: Socket file permissions + peer credential validation

#![cfg(target_os = "macos")]
#![crate_name = "rusty_backup_helper"]

mod handler;
mod state;

use rusty_backup::privileged::protocol::{DaemonRequest, DaemonResponse};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::{Arc, Mutex};

const SOCKET_PATH: &str = "/var/run/rustybackup.sock";

fn main() {
    eprintln!("Rusty Backup Helper v{}", env!("CARGO_PKG_VERSION"));
    eprintln!("Starting daemon on socket: {}", SOCKET_PATH);
    
    // Remove old socket if it exists
    let _ = std::fs::remove_file(SOCKET_PATH);
    
    // Create Unix domain socket listener
    let listener = match UnixListener::bind(SOCKET_PATH) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("Failed to bind socket {}: {}", SOCKET_PATH, e);
            std::process::exit(1);
        }
    };
    
    // Set socket permissions (allow all users to connect)
    if let Err(e) = std::fs::set_permissions(SOCKET_PATH, std::os::unix::fs::PermissionsExt::from_mode(0o666)) {
        eprintln!("Failed to set socket permissions: {}", e);
        std::process::exit(1);
    }
    
    eprintln!("Daemon listening on {}", SOCKET_PATH);
    
    // Create shared state
    let state = Arc::new(Mutex::new(state::DaemonState::new()));
    
    // Accept connections
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let state = Arc::clone(&state);
                std::thread::spawn(move || {
                    handle_client(stream, state);
                });
            }
            Err(e) => {
                eprintln!("Connection error: {}", e);
            }
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

