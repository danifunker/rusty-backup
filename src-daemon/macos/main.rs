//! macOS privileged helper daemon for Rusty Backup.
//!
//! This daemon runs as root via launchd and handles all privileged disk
//! operations. It communicates with the main app via XPC.

#![cfg(target_os = "macos")]
#![crate_name = "rusty_backup_helper"]

mod handler;
mod state;

use std::ffi::CStr;
use std::os::raw::{c_char, c_void};
use std::ptr;

use xpc_sys::objects::xpc_object_t;
use xpc_sys::rs_syscalls::*;

fn main() {
    // Set up XPC listener
    unsafe {
        let service_name = b"com.rustybackup.helper\0";
        
        // Create mach service listener
        let listener = xpc_connection_create_mach_service(
            service_name.as_ptr() as *const c_char,
            ptr::null_mut(), // dispatch_get_main_queue()
            XPC_CONNECTION_MACH_SERVICE_LISTENER as u64,
        );
        
        if listener.is_null() {
            eprintln!("Failed to create XPC listener");
            std::process::exit(1);
        }
        
        // Set up event handler for new connections
        xpc_connection_set_event_handler(listener, connection_handler as *const c_void);
        
        // Start listening
        xpc_connection_resume(listener);
        
        // Run forever (dispatch_main equivalent)
        // Since we don't have dispatch_main binding, we'll park the thread
        std::thread::park();
    }
}

/// Handler for new XPC connections.
unsafe extern "C" fn connection_handler(connection: xpc_object_t) {
    // Verify this is a connection object
    let conn_type = xpc_get_type(connection);
    if conn_type != XPC_TYPE_CONNECTION {
        return;
    }
    
    // Create state for this connection
    let state = Box::new(state::DaemonState::new());
    let state_ptr = Box::into_raw(state);
    
    // Set up message handler
    xpc_connection_set_event_handler(connection, message_handler as *const c_void);
    xpc_connection_set_context(connection, state_ptr as *mut c_void);
    
    // Set finalizer to clean up state when connection closes
    xpc_connection_set_finalizer_f(connection, Some(connection_finalizer));
    
    // Start receiving messages
    xpc_connection_resume(connection);
}

/// Handler for messages on an established connection.
unsafe extern "C" fn message_handler(message: xpc_object_t) {
    let msg_type = xpc_get_type(message);
    
    if msg_type == XPC_TYPE_DICTIONARY {
        // Get connection from message (XPC provides this context)
        let connection = xpc_dictionary_get_remote_connection(message);
        if connection.is_null() {
            return;
        }
        
        // Get state from connection context
        let state_ptr = xpc_connection_get_context(connection) as *mut state::DaemonState;
        if state_ptr.is_null() {
            return;
        }
        let state = &mut *state_ptr;
        
        // Handle the request
        let response = state.handle_xpc_message(message);
        
        // Send response back
        xpc_connection_send_message(connection, response);
    }
}

/// Cleanup when connection closes.
unsafe extern "C" fn connection_finalizer(context: *mut c_void) {
    if !context.is_null() {
        // Drop the state
        let _ = Box::from_raw(context as *mut state::DaemonState);
    }
}

// XPC constants (from xpc/connection.h)
const XPC_CONNECTION_MACH_SERVICE_LISTENER: usize = 0x1;
const XPC_TYPE_CONNECTION: *const c_void = 1 as *const c_void;
const XPC_TYPE_DICTIONARY: *const c_void = 2 as *const c_void;

// XPC function declarations (minimal set we need)
extern "C" {
    fn xpc_dictionary_get_remote_connection(xdict: xpc_object_t) -> xpc_object_t;
}
