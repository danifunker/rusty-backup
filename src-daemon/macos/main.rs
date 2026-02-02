//! macOS privileged helper daemon for Rusty Backup.
//!
//! This daemon runs as root via launchd and handles all privileged disk
//! operations. It communicates with the main app via XPC.
//!
//! **STATUS**: Proof-of-concept structure only. The raw XPC C API integration
//! via xpc-sys requires significant additional work due to complex FFI type
//! handling. Current implementation provides:
//! - Daemon binary structure
//! - State management with command handlers  
//! - Heartbeat and crash recovery logic
//!
//! **TODO for XPC integration**:
//! - Option A: Use xpc-sys higher-level helpers (XPCObject, pipe_routine)
//! - Option B: Complete raw xpc_connection FFI bindings with correct types
//! - Option C: Alternative IPC (Unix sockets + JSON, simpler but loses XPC benefits)

#![cfg(target_os = "macos")]
#![crate_name = "rusty_backup_helper"]

#[allow(dead_code)]
mod handler;
#[allow(dead_code)]
mod state;

fn main() {
    // Temporary stub - will be replaced with full XPC implementation
    println!("Rusty Backup Helper v{}", env!("CARGO_PKG_VERSION"));
    println!("This is a privileged helper daemon (work in progress)");
    
    eprintln!("Note: XPC integration not yet complete.");
    eprintln!("The daemon structure is implemented but XPC communication needs work.");
    eprintln!("See main.rs comments for TODO items.");
    
    // Exit for now - when XPC is ready, this will be replaced with:
    // - XPC listener setup
    // - Connection handler registration
    // - Event loop (dispatch_main or thread::park)
}

