//! Daemon state management.
//!
//! Tracks open disk handles and manages progress information for crash recovery.

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use rusty_backup::privileged::protocol::{DaemonRequest, DaemonResponse, ProgressInfo, PROGRESS_FILE};

/// Daemon state for all connections.
pub struct DaemonState {
    /// Open disk file handles
    open_handles: HashMap<u64, File>,
    /// Next handle ID to assign
    next_handle: u64,
    /// Progress information for crash recovery (shared with heartbeat thread)
    progress: Arc<Mutex<HashMap<u64, ProgressInfo>>>,
}

impl DaemonState {
    pub fn new() -> Self {
        let progress = Arc::new(Mutex::new(HashMap::new()));
        
        // Start heartbeat thread for crash recovery
        start_heartbeat_thread(Arc::clone(&progress));
        
        Self {
            open_handles: HashMap::new(),
            next_handle: 1,
            progress,
        }
    }
    
    /// Handle a daemon request from the client.
    pub fn handle_request(&mut self, request: DaemonRequest) -> DaemonResponse {
        match request {
            DaemonRequest::GetVersion => self.get_version(),
            DaemonRequest::OpenDiskRead { path } => self.open_disk_read(&path),
            DaemonRequest::OpenDiskWrite { path } => self.open_disk_write(&path),
            DaemonRequest::ReadSectors { handle, lba, count } => {
                self.read_sectors(handle, lba, count)
            }
            DaemonRequest::WriteSectors { handle, lba, data } => {
                self.write_sectors(handle, lba, &data)
            }
            DaemonRequest::CloseDisk { handle } => self.close_disk(handle),
        }
    }
    
    fn get_version(&self) -> DaemonResponse {
        DaemonResponse::Version {
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
    
    fn open_disk_read(&mut self, path: &str) -> DaemonResponse {
        match File::open(path) {
            Ok(file) => {
                let size_bytes = file.metadata().map(|m| m.len()).unwrap_or(0);
                let handle = self.next_handle;
                self.next_handle += 1;
                self.open_handles.insert(handle, file);
                
                DaemonResponse::DiskOpened { handle, size_bytes }
            }
            Err(e) => DaemonResponse::Error {
                message: format!("Failed to open {}: {}", path, e),
            },
        }
    }
    
    fn open_disk_write(&mut self, path: &str) -> DaemonResponse {
        // TODO: Unmount volumes first using existing macos unmount logic
        
        match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
        {
            Ok(file) => {
                let size_bytes = file.metadata().map(|m| m.len()).unwrap_or(0);
                let handle = self.next_handle;
                self.next_handle += 1;
                self.open_handles.insert(handle, file);
                
                DaemonResponse::DiskOpened { handle, size_bytes }
            }
            Err(e) => DaemonResponse::Error {
                message: format!("Failed to open {} for writing: {}", path, e),
            },
        }
    }
    
    fn read_sectors(&mut self, handle: u64, lba: u64, count: u32) -> DaemonResponse {
        // Update progress for crash recovery
        {
            let mut prog = self.progress.lock().unwrap();
            prog.insert(
                handle,
                ProgressInfo {
                    handle,
                    current_lba: lba,
                    operation: "read".to_string(),
                    timestamp: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                },
            );
        }
        
        let file = match self.open_handles.get_mut(&handle) {
            Some(f) => f,
            None => {
                return DaemonResponse::Error {
                    message: format!("Invalid handle: {}", handle),
                }
            }
        };
        
        let offset = lba * 512;
        let size = count as usize * 512;
        
        if let Err(e) = file.seek(SeekFrom::Start(offset)) {
            return DaemonResponse::Error {
                message: format!("Seek failed: {}", e),
            };
        }
        
        let mut buffer = vec![0u8; size];
        if let Err(e) = file.read_exact(&mut buffer) {
            return DaemonResponse::Error {
                message: format!("Read failed: {}", e),
            };
        }
        
        // Clear progress
        self.progress.lock().unwrap().remove(&handle);
        
        DaemonResponse::SectorsRead { data: buffer }
    }
    
    fn write_sectors(&mut self, handle: u64, lba: u64, data: &[u8]) -> DaemonResponse {
        // Update progress for crash recovery
        {
            let mut prog = self.progress.lock().unwrap();
            prog.insert(
                handle,
                ProgressInfo {
                    handle,
                    current_lba: lba,
                    operation: "write".to_string(),
                    timestamp: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                },
            );
        }
        
        let file = match self.open_handles.get_mut(&handle) {
            Some(f) => f,
            None => {
                return DaemonResponse::Error {
                    message: format!("Invalid handle: {}", handle),
                }
            }
        };
        
        let offset = lba * 512;
        
        if let Err(e) = file.seek(SeekFrom::Start(offset)) {
            return DaemonResponse::Error {
                message: format!("Seek failed: {}", e),
            };
        }
        
        if let Err(e) = file.write_all(data) {
            return DaemonResponse::Error {
                message: format!("Write failed: {}", e),
            };
        }
        
        // Clear progress
        self.progress.lock().unwrap().remove(&handle);
        
        DaemonResponse::Success
    }
    
    fn close_disk(&mut self, handle: u64) -> DaemonResponse {
        self.progress.lock().unwrap().remove(&handle);
        
        if self.open_handles.remove(&handle).is_some() {
            DaemonResponse::Success
        } else {
            DaemonResponse::Error {
                message: format!("Invalid handle: {}", handle),
            }
        }
    }
}

/// Start a background thread that writes progress to disk every second.
fn start_heartbeat_thread(progress: Arc<Mutex<HashMap<u64, ProgressInfo>>>) {
    std::thread::spawn(move || loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
        
        if let Ok(prog) = progress.lock() {
            if !prog.is_empty() {
                let _ = write_progress_to_file(&prog);
            }
        }
    });
}

/// Write progress information to disk for crash recovery.
fn write_progress_to_file(progress: &HashMap<u64, ProgressInfo>) -> Result<(), std::io::Error> {
    let json = serde_json::to_string(progress)?;
    std::fs::write(PROGRESS_FILE, json)?;
    Ok(())
}

// Note: XPC helper functions removed - will be re-added when XPC integration is complete
