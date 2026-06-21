//! A [`Filesystem`] implementation backed by a remote daemon (Family F read).
//!
//! Lets a remote disk image plug into anything that consumes a
//! `Box<dyn Filesystem>` — notably the Commander GUI's `ListingSource::Image`
//! and the shared copy engine (`fs::copy` / `commander_ops::stage_copy`), which
//! extracts file data via `write_file_to`. The seven required `Filesystem`
//! methods map onto the wire: `list_directory` → `ListDir`,
//! `read_file`/`write_file_to` → `ReadFile`, and the volume metadata comes from
//! the `OpenImage` response. The trait's bound is only `Send` (not `Sync`),
//! which a `RemoteSession` (a blocking `&mut`-per-call TCP client) satisfies.

use std::io::Write;

use crate::fs::entry::FileEntry;
use crate::fs::filesystem::{Filesystem, FilesystemError};
use crate::remote::client::RemoteSession;
use crate::remote::protocol::{WireEntry, WireKind};

/// A live remote image, addressed by path over an open [`RemoteSession`].
pub struct RemoteFilesystem {
    session: RemoteSession,
    handle: u64,
    root: FileEntry,
    fs_type: String,
    volume_label: Option<String>,
    total_size: u64,
    used_size: u64,
}

impl RemoteFilesystem {
    /// Connect to `addr`, open `image_path` (partition `partition`), and read
    /// the root listing. Returns the filesystem plus the root entry and its
    /// children, ready to hand to `DirListing::load_root`.
    pub fn open(
        addr: &str,
        image_path: &str,
        partition: Option<u32>,
    ) -> anyhow::Result<(Self, FileEntry, Vec<FileEntry>)> {
        let mut session = RemoteSession::connect(addr)?;
        let opened = session.open_image(image_path, partition)?;
        let root_children = session
            .list_dir(opened.handle, "/")?
            .into_iter()
            .map(wire_to_entry)
            .collect();
        let root = FileEntry::root();
        let fs = RemoteFilesystem {
            session,
            handle: opened.handle,
            root: root.clone(),
            fs_type: opened.fs_type,
            volume_label: opened.volume_label,
            total_size: opened.total_size,
            used_size: opened.used_size,
        };
        Ok((fs, root, root_children))
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
        let wire = self.session.list_dir(self.handle, path).map_err(wire_err)?;
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
        self.session
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
        self.session
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

fn wire_err(e: anyhow::Error) -> FilesystemError {
    FilesystemError::Io(std::io::Error::other(e.to_string()))
}

/// A [`Filesystem`] over the daemon's **host** filesystem — the Commander remote
/// pane's *file-browser* view. `list_directory` → `ListHostDir`,
/// `read_file`/`write_file_to` → `ReadHostFile`. The user browses the remote
/// machine and then double-clicks (or right-click → Open Image) a disk image,
/// which the GUI opens as a [`RemoteFilesystem`] to browse inside.
pub struct RemoteHostFilesystem {
    session: RemoteSession,
    root: FileEntry,
}

impl RemoteHostFilesystem {
    /// Connect to `addr` and list `root_path` (e.g. `/`) on the daemon's host
    /// filesystem. Returns the filesystem plus the root entry and its children.
    pub fn open(addr: &str, root_path: &str) -> anyhow::Result<(Self, FileEntry, Vec<FileEntry>)> {
        let mut session = RemoteSession::connect(addr)?;
        let (exists, is_dir) = session.host_stat(root_path)?;
        if !exists {
            anyhow::bail!("no such path on the remote: {root_path}");
        }
        if !is_dir {
            anyhow::bail!("{root_path} is not a directory on the remote");
        }
        let children = session
            .list_host_dir(root_path)?
            .into_iter()
            .map(wire_to_entry)
            .collect();
        let root = FileEntry::new_directory("/".to_string(), root_path.to_string(), 0);
        Ok((
            RemoteHostFilesystem {
                session,
                root: root.clone(),
            },
            root,
            children,
        ))
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
        let wire = self.session.list_host_dir(path).map_err(wire_err)?;
        Ok(wire.into_iter().map(wire_to_entry).collect())
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let mut buf = Vec::new();
        self.session
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
        self.session
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
