//! A **shared** connection to a daemon — one [`RemoteSession`], many open-image
//! handles.
//!
//! The daemon's handle table is **per-connection**: one TCP session can hold
//! several open images at once (`OpenImage` hands back a fresh `handle` each
//! time). `RemoteConnection` is the client-side broker for that — it owns the
//! single session and tracks the handles opened on it, so a user can open and
//! switch between images **without reconnecting**. The reusable file-browser UI
//! and the per-tab "Open Remote..." flows all share one `RemoteConnection`.
//!
//! ## Threading
//! `RemoteSession` is blocking, one-request-at-a-time, and not `Clone`. Wrap a
//! `RemoteConnection` in `Arc<Mutex<…>>` (see [`RemoteConnection::connect_shared`])
//! to share it across the egui UI thread and the worker threads that run opens /
//! reads / copies. Every operation serializes through the lock, which is fine —
//! the wire is sequential anyway. The cost to watch is a long `read_file`
//! holding the lock and blocking concurrent browsing; revisit with a
//! connection-actor thread only if that contention bites.

use std::io::Write;
use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::remote::client::{OpenedImage, RemoteSession};
use crate::remote::protocol::WireEntry;

/// One live daemon connection, brokering many open-image handles on a single
/// [`RemoteSession`].
pub struct RemoteConnection {
    addr: String,
    session: RemoteSession,
    /// Handles opened on this connection, tracked so they can be closed
    /// individually (or audited in tests). The daemon also reaps them all when
    /// the connection drops.
    open_handles: Vec<u64>,
}

impl RemoteConnection {
    /// Connect to `addr` (`host:port`) and complete the Family-F handshake.
    pub fn connect(addr: &str) -> Result<Self> {
        Ok(Self {
            addr: addr.to_string(),
            session: RemoteSession::connect(addr)?,
            open_handles: Vec::new(),
        })
    }

    /// Connect and hand back the `Arc<Mutex<…>>` the GUI / worker threads pass
    /// around. The shared shape the whole reusable-browser model is built on.
    pub fn connect_shared(addr: &str) -> Result<Arc<Mutex<RemoteConnection>>> {
        Ok(Arc::new(Mutex::new(Self::connect(addr)?)))
    }

    /// The daemon address this connection talks to (`host:port`).
    pub fn addr(&self) -> &str {
        &self.addr
    }

    // --- image handles (browse inside an opened image) ---------------------

    /// Open a disk image on the daemon and remember its handle. Many images can
    /// be open at once on the same connection.
    pub fn open_image(&mut self, path: &str, partition: Option<u32>) -> Result<OpenedImage> {
        let opened = self.session.open_image(path, partition)?;
        self.open_handles.push(opened.handle);
        Ok(opened)
    }

    /// Close one open-image handle, freeing it on the daemon. No-op-safe to call
    /// for a handle this connection no longer tracks.
    pub fn close_image(&mut self, handle: u64) -> Result<()> {
        let res = self.session.close(handle);
        // Drop our bookkeeping regardless — even if the wire close errored, the
        // handle is gone (or the socket is dead and the daemon will reap it).
        self.open_handles.retain(|&h| h != handle);
        res
    }

    /// List a directory inside an opened image.
    pub fn list_dir(&mut self, handle: u64, path: &str) -> Result<Vec<WireEntry>> {
        self.session.list_dir(handle, path)
    }

    /// Stream one file's bytes (from inside an opened image) into `sink`.
    pub fn read_file(&mut self, handle: u64, path: &str, sink: &mut dyn Write) -> Result<u64> {
        self.session.read_file(handle, path, sink)
    }

    // --- host filesystem (the file browser) --------------------------------

    /// Classify a host path on the daemon: `(exists, is_dir)`.
    pub fn host_stat(&mut self, path: &str) -> Result<(bool, bool)> {
        self.session.host_stat(path)
    }

    /// List a directory on the daemon's host filesystem.
    pub fn list_host_dir(&mut self, path: &str) -> Result<Vec<WireEntry>> {
        self.session.list_host_dir(path)
    }

    /// Stream a host file's bytes into `sink`.
    pub fn read_host_file(&mut self, path: &str, sink: &mut dyn Write) -> Result<u64> {
        self.session.read_host_file(path, sink)
    }

    /// Open a host image as a raw block device kept open on the daemon; returns
    /// `(handle, size)`. The block reader's first call.
    pub fn open_block(&mut self, path: &str) -> Result<(u64, u64)> {
        self.session.open_block(path)
    }

    /// Open a host image as a read-WRITE block device kept open on the daemon;
    /// returns `(handle, size)`. The writable block reader's first call.
    pub fn open_block_rw(&mut self, path: &str) -> Result<(u64, u64)> {
        self.session.open_block_rw(path)
    }

    /// Write `bytes` at `offset` into a read-write block handle (the block
    /// writer's fetch).
    pub fn write_block(&mut self, handle: u64, offset: u64, bytes: &[u8]) -> Result<()> {
        self.session.write_block(handle, offset, bytes)
    }

    /// Flush a read-write block handle to stable storage on the daemon.
    pub fn flush_block(&mut self, handle: u64) -> Result<()> {
        self.session.flush_block(handle)
    }

    /// List the daemon machine's physical disk devices.
    pub fn list_devices(&mut self) -> Result<Vec<crate::remote::protocol::WireDevice>> {
        self.session.list_devices()
    }

    /// Open one of the daemon's enumerated physical devices as a raw block
    /// handle (read-only); returns `(handle, size)`.
    pub fn open_device(&mut self, path: &str) -> Result<(u64, u64)> {
        self.session.open_device(path)
    }

    /// Read a byte range from an open block handle (the block reader's fetch).
    pub fn read_block(&mut self, handle: u64, offset: u64, len: u32) -> Result<Vec<u8>> {
        self.session.read_block(handle, offset, len)
    }

    /// Close an open block handle on the daemon.
    pub fn close_block(&mut self, handle: u64) -> Result<()> {
        self.session.close_block(handle)
    }

    /// How many open-image handles this connection currently holds (diagnostics
    /// / tests).
    pub fn open_handle_count(&self) -> usize {
        self.open_handles.len()
    }
}
