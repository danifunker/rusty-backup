//! [`Filesystem`] **views** over a shared [`RemoteConnection`] (Family F read).
//!
//! Lets a remote disk image plug into anything that consumes a
//! `Box<dyn Filesystem>` — notably the Commander GUI's `ListingSource::Image`
//! and the shared copy engine (`fs::copy` / `commander_ops::stage_copy`), which
//! extracts file data via `write_file_to`. The required `Filesystem` methods map
//! onto the wire: `list_directory` → `ListDir`, `read_file`/`write_file_to` →
//! `ReadFile`, and the volume metadata comes from the `OpenImage` response.
//!
//! Each view is a thin handle onto a [`RemoteConnection`] shared via
//! `Arc<Mutex<…>>`: a [`RemoteFilesystem`] is bound to one opened-image handle,
//! a [`RemoteHostFilesystem`] to the daemon's host FS. **Many views can share
//! one connection** — that's how the user opens and switches between images
//! without reconnecting. Each `Filesystem` method locks the connection, runs its
//! one request/reply, and returns owned data, so the `&mut self` + `Send` shape
//! the trait wants is satisfied (the lock makes the connection `Sync`).

use std::io::Write;
use std::sync::{Arc, Mutex};

use crate::fs::entry::FileEntry;
use crate::fs::filesystem::{Filesystem, FilesystemError};
use crate::remote::connection::RemoteConnection;
use crate::remote::protocol::{WireEntry, WireKind};

/// Lock the shared connection, mapping a poisoned mutex to an I/O error (a
/// worker thread panicked mid-operation; the connection is no longer trusted).
fn lock_conn(
    conn: &Arc<Mutex<RemoteConnection>>,
) -> Result<std::sync::MutexGuard<'_, RemoteConnection>, FilesystemError> {
    conn.lock()
        .map_err(|_| FilesystemError::Io(std::io::Error::other("remote connection lock poisoned")))
}

/// A live remote image — one opened-image handle on a shared [`RemoteConnection`].
pub struct RemoteFilesystem {
    conn: Arc<Mutex<RemoteConnection>>,
    handle: u64,
    root: FileEntry,
    fs_type: String,
    volume_label: Option<String>,
    total_size: u64,
    used_size: u64,
}

impl RemoteFilesystem {
    /// Open `image_path` (partition `partition`) as a handle on an existing
    /// shared connection — the no-reconnect path. Returns the filesystem plus
    /// the root entry and its children, ready for `DirListing::load_root`.
    pub fn on_connection(
        conn: Arc<Mutex<RemoteConnection>>,
        image_path: &str,
        partition: Option<u32>,
    ) -> anyhow::Result<(Self, FileEntry, Vec<FileEntry>)> {
        let (opened, root_children) = {
            let mut c = conn
                .lock()
                .map_err(|_| anyhow::anyhow!("remote connection lock poisoned"))?;
            let opened = c.open_image(image_path, partition)?;
            let children = c
                .list_dir(opened.handle, "/")?
                .into_iter()
                .map(wire_to_entry)
                .collect();
            (opened, children)
        };
        let root = FileEntry::root();
        let fs = RemoteFilesystem {
            conn,
            handle: opened.handle,
            root: root.clone(),
            fs_type: opened.fs_type,
            volume_label: opened.volume_label,
            total_size: opened.total_size,
            used_size: opened.used_size,
        };
        Ok((fs, root, root_children))
    }

    /// Connect to `addr` on a fresh single-use connection, then open the image.
    /// Convenience for callers that don't (yet) share a connection.
    pub fn open(
        addr: &str,
        image_path: &str,
        partition: Option<u32>,
    ) -> anyhow::Result<(Self, FileEntry, Vec<FileEntry>)> {
        let conn = RemoteConnection::connect_shared(addr)?;
        Self::on_connection(conn, image_path, partition)
    }

    /// The opened-image handle this view holds on its connection.
    pub fn handle(&self) -> u64 {
        self.handle
    }

    /// A clone of the shared connection — lets a caller open *another* image on
    /// the same session (switch images without reconnecting).
    pub fn connection(&self) -> Arc<Mutex<RemoteConnection>> {
        Arc::clone(&self.conn)
    }
}

impl Filesystem for RemoteFilesystem {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(self.root.clone())
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        let path = if entry.path.is_empty() {
            "/"
        } else {
            entry.path.as_str()
        };
        let wire = lock_conn(&self.conn)?
            .list_dir(self.handle, path)
            .map_err(wire_err)?;
        Ok(wire.into_iter().map(wire_to_entry).collect())
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        // The wire `ReadFile` streams the whole file (no ranged read yet), so we
        // collect it all and then cap — fine for previews of modest files.
        let mut buf = Vec::new();
        lock_conn(&self.conn)?
            .read_file(self.handle, &entry.path, &mut buf)
            .map_err(wire_err)?;
        if buf.len() > max_bytes {
            buf.truncate(max_bytes);
        }
        Ok(buf)
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        // Stream straight from the wire into the sink — the efficient copy path
        // the engine's `fs::copy` / Commander staging relies on.
        lock_conn(&self.conn)?
            .read_file(self.handle, &entry.path, writer)
            .map_err(wire_err)
    }

    fn volume_label(&self) -> Option<&str> {
        self.volume_label.as_deref()
    }

    fn fs_type(&self) -> &str {
        &self.fs_type
    }

    fn total_size(&self) -> u64 {
        self.total_size
    }

    fn used_size(&self) -> u64 {
        self.used_size
    }
}

impl Drop for RemoteFilesystem {
    fn drop(&mut self) {
        // Best-effort handle close so a long-lived shared connection doesn't
        // accumulate open images as the user switches between them. `try_lock`
        // never blocks: if another op holds the lock (e.g. a big read in
        // flight), skip and let the daemon reap the handle when the connection
        // itself drops.
        if let Ok(mut c) = self.conn.try_lock() {
            let _ = c.close_image(self.handle);
        }
    }
}

fn wire_err(e: anyhow::Error) -> FilesystemError {
    FilesystemError::Io(std::io::Error::other(e.to_string()))
}

/// A [`Filesystem`] over the daemon's **host** filesystem — the remote *file
/// browser* view, sharing one [`RemoteConnection`]. `list_directory` →
/// `ListHostDir`, `read_file`/`write_file_to` → `ReadHostFile`. The user browses
/// the remote machine and then double-clicks (or right-click → Open Image) a
/// disk image, which opens as a [`RemoteFilesystem`] **on the same connection**.
pub struct RemoteHostFilesystem {
    conn: Arc<Mutex<RemoteConnection>>,
    root: FileEntry,
}

impl RemoteHostFilesystem {
    /// List `root_path` (e.g. `/`) on the daemon's host FS using an existing
    /// shared connection. Returns the filesystem plus the root entry + children.
    pub fn on_connection(
        conn: Arc<Mutex<RemoteConnection>>,
        root_path: &str,
    ) -> anyhow::Result<(Self, FileEntry, Vec<FileEntry>)> {
        let children = {
            let mut c = conn
                .lock()
                .map_err(|_| anyhow::anyhow!("remote connection lock poisoned"))?;
            let (exists, is_dir) = c.host_stat(root_path)?;
            if !exists {
                anyhow::bail!("no such path on the remote: {root_path}");
            }
            if !is_dir {
                anyhow::bail!("{root_path} is not a directory on the remote");
            }
            c.list_host_dir(root_path)?
                .into_iter()
                .map(wire_to_entry)
                .collect()
        };
        let root = FileEntry::new_directory("/".to_string(), root_path.to_string(), 0);
        Ok((
            RemoteHostFilesystem {
                conn,
                root: root.clone(),
            },
            root,
            children,
        ))
    }

    /// Connect to `addr` on a fresh single-use connection, then browse its host
    /// FS at `root_path`. Convenience for callers not (yet) sharing a connection.
    pub fn open(addr: &str, root_path: &str) -> anyhow::Result<(Self, FileEntry, Vec<FileEntry>)> {
        let conn = RemoteConnection::connect_shared(addr)?;
        Self::on_connection(conn, root_path)
    }

    /// A clone of the shared connection — lets the file browser open an image it
    /// finds as a [`RemoteFilesystem`] on this same session.
    pub fn connection(&self) -> Arc<Mutex<RemoteConnection>> {
        Arc::clone(&self.conn)
    }
}

impl Filesystem for RemoteHostFilesystem {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(self.root.clone())
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        let path = if entry.path.is_empty() {
            "/"
        } else {
            entry.path.as_str()
        };
        let wire = lock_conn(&self.conn)?
            .list_host_dir(path)
            .map_err(wire_err)?;
        Ok(wire.into_iter().map(wire_to_entry).collect())
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let mut buf = Vec::new();
        lock_conn(&self.conn)?
            .read_host_file(&entry.path, &mut buf)
            .map_err(wire_err)?;
        if buf.len() > max_bytes {
            buf.truncate(max_bytes);
        }
        Ok(buf)
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        lock_conn(&self.conn)?
            .read_host_file(&entry.path, writer)
            .map_err(wire_err)
    }

    fn volume_label(&self) -> Option<&str> {
        None
    }

    fn fs_type(&self) -> &str {
        "remote-host"
    }

    fn total_size(&self) -> u64 {
        0
    }

    fn used_size(&self) -> u64 {
        0
    }
}

/// Inverse of `WireEntry::from_entry` — rebuild a `FileEntry` for display /
/// copy. The `location` field is unused remotely (the daemon addresses entries
/// by path), so it is left 0.
fn wire_to_entry(w: WireEntry) -> FileEntry {
    let WireEntry {
        name,
        path,
        kind,
        size,
        type_code,
        creator_code,
        symlink_target,
    } = w;
    let mut e = match kind {
        WireKind::Dir => FileEntry::new_directory(name, path, 0),
        WireKind::File => FileEntry::new_file(name, path, size, 0),
        WireKind::Symlink => {
            FileEntry::new_symlink(name, path, size, 0, symlink_target.unwrap_or_default())
        }
        WireKind::Special => FileEntry::new_special(name, path, 0, "special".to_string()),
    };
    e.type_code = type_code;
    e.creator_code = creator_code;
    e
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_to_entry_maps_kinds_and_metadata() {
        let dir = wire_to_entry(WireEntry {
            name: "SUB".into(),
            path: "/SUB".into(),
            kind: WireKind::Dir,
            size: 0,
            type_code: None,
            creator_code: None,
            symlink_target: None,
        });
        assert!(dir.is_directory());
        assert_eq!(dir.path, "/SUB");

        let file = wire_to_entry(WireEntry {
            name: "DOC.TXT".into(),
            path: "/DOC.TXT".into(),
            kind: WireKind::File,
            size: 1234,
            type_code: Some("TEXT".into()),
            creator_code: Some("ttxt".into()),
            symlink_target: None,
        });
        assert!(!file.is_directory());
        assert_eq!(file.size, 1234);
        assert_eq!(file.type_code.as_deref(), Some("TEXT"));
        assert_eq!(file.creator_code.as_deref(), Some("ttxt"));

        let link = wire_to_entry(WireEntry {
            name: "LN".into(),
            path: "/LN".into(),
            kind: WireKind::Symlink,
            size: 0,
            type_code: None,
            creator_code: None,
            symlink_target: Some("/target".into()),
        });
        assert_eq!(link.symlink_target.as_deref(), Some("/target"));
    }
}
