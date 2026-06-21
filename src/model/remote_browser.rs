//! Reusable, **testable** core for the remote file browser: one daemon
//! connection, host-FS browsing, and opening / switching between disk images on
//! it **without reconnecting**.
//!
//! This is the engine half of the reusable RemoteBrowser
//! (`docs/remote_access_handoff.md` Step 2). It owns the shared
//! `Arc<Mutex<RemoteConnection>>` and the host-vs-image browse mode; every
//! transition returns a [`BrowseTarget`] — a `Box<dyn Filesystem>` view + its
//! root listing + display metadata — that the caller feeds straight to
//! `DirListing::load_root`. The egui component and a future TUI are thin
//! renderers that run these (blocking) transitions on a worker thread and apply
//! the result.
//!
//! Everything here is synchronous and headlessly testable over a loopback
//! `serve_on` (see `tests/remote_filesystem.rs`). The no-reconnect property
//! falls out of the model: `open_image` / `close_image` reuse `self.conn`, so
//! the daemon's per-connection handle table just gains / loses a handle.

use std::sync::{Arc, Mutex};

use crate::fs::entry::FileEntry;
use crate::fs::filesystem::Filesystem;
use crate::remote::{RemoteConnection, RemoteFilesystem, RemoteHostFilesystem};

/// What the browser is currently showing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrowseMode {
    /// The daemon's host filesystem — a file browser. Files can be opened as
    /// disk images.
    Host,
    /// Inside a disk image opened on the daemon.
    Image {
        path: String,
        partition: Option<u32>,
    },
}

/// The product of a browse transition: a `Filesystem` view (boxed for
/// `DirListing::load_root`), its root listing, and the display metadata the
/// source bar shows. `mode` records what was opened.
pub struct BrowseTarget {
    pub fs: Box<dyn Filesystem>,
    pub root: FileEntry,
    pub entries: Vec<FileEntry>,
    pub mode: BrowseMode,
    pub fs_type: String,
    pub volume_label: String,
    pub total_size: u64,
    pub used_size: u64,
}

impl BrowseTarget {
    fn host(fs: Box<dyn Filesystem>, root: FileEntry, entries: Vec<FileEntry>) -> Self {
        Self {
            fs,
            root,
            entries,
            mode: BrowseMode::Host,
            fs_type: "remote-host".to_string(),
            volume_label: String::new(),
            total_size: 0,
            used_size: 0,
        }
    }

    /// True when this target is the host file browser (vs inside an image).
    pub fn is_host(&self) -> bool {
        matches!(self.mode, BrowseMode::Host)
    }
}

/// One remote file-browser session: a shared connection plus the current browse
/// mode. Opened images come and go as handles on the **same** connection, so the
/// user opens and switches between them without ever reconnecting.
pub struct RemoteBrowser {
    conn: Arc<Mutex<RemoteConnection>>,
    addr: String,
    mode: BrowseMode,
    /// The host directory an image was opened from, so [`close_image`] returns
    /// there instead of the host root.
    ///
    /// [`close_image`]: RemoteBrowser::close_image
    image_return_dir: Option<String>,
}

impl RemoteBrowser {
    /// Connect to `addr` and browse the host FS at `root_path` (usually `/`).
    /// Returns the browser plus the first target (the host listing). Blocking —
    /// a connect can stall on an unreachable host, so the GUI runs this on a
    /// worker thread.
    pub fn connect(addr: &str, root_path: &str) -> anyhow::Result<(Self, BrowseTarget)> {
        let conn = RemoteConnection::connect_shared(addr)?;
        let (fs, root, entries) =
            RemoteHostFilesystem::on_connection(Arc::clone(&conn), root_path)?;
        let browser = RemoteBrowser {
            conn,
            addr: addr.to_string(),
            mode: BrowseMode::Host,
            image_return_dir: None,
        };
        Ok((browser, BrowseTarget::host(Box::new(fs), root, entries)))
    }

    /// Browse the daemon's host FS at `root_path` on the existing connection.
    pub fn browse_host(&mut self, root_path: &str) -> anyhow::Result<BrowseTarget> {
        let (fs, root, entries) =
            RemoteHostFilesystem::on_connection(Arc::clone(&self.conn), root_path)?;
        self.mode = BrowseMode::Host;
        Ok(BrowseTarget::host(Box::new(fs), root, entries))
    }

    /// Open an image found while browsing the host FS, as a new handle on the
    /// **same** connection (no reconnect). `opened_from` is the host directory it
    /// was found in, remembered so [`close_image`] returns there.
    ///
    /// [`close_image`]: RemoteBrowser::close_image
    pub fn open_image(
        &mut self,
        path: &str,
        partition: Option<u32>,
        opened_from: &str,
    ) -> anyhow::Result<BrowseTarget> {
        let (fs, root, entries) =
            RemoteFilesystem::on_connection(Arc::clone(&self.conn), path, partition)?;
        let mode = BrowseMode::Image {
            path: path.to_string(),
            partition,
        };
        // Read the display metadata off the view before boxing it.
        let target = BrowseTarget {
            fs_type: fs.fs_type().to_string(),
            volume_label: fs.volume_label().unwrap_or_default().to_string(),
            total_size: fs.total_size(),
            used_size: fs.used_size(),
            fs: Box::new(fs),
            root,
            entries,
            mode: mode.clone(),
        };
        self.mode = mode;
        self.image_return_dir = Some(opened_from.to_string());
        Ok(target)
    }

    /// Step back out of an open image to the host file browser, at the directory
    /// the image was opened from (or the host root if unknown).
    pub fn close_image(&mut self) -> anyhow::Result<BrowseTarget> {
        let ret = self
            .image_return_dir
            .clone()
            .unwrap_or_else(|| "/".to_string());
        self.browse_host(&ret)
    }

    /// The daemon address (`host:port`).
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// The current browse mode (host file browser, or inside an image).
    pub fn mode(&self) -> &BrowseMode {
        &self.mode
    }

    /// A clone of the shared connection — for diagnostics (open-handle count) or
    /// the write path later.
    pub fn connection(&self) -> Arc<Mutex<RemoteConnection>> {
        Arc::clone(&self.conn)
    }
}
