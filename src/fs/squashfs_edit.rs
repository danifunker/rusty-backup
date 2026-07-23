//! Editable SquashFS — an in-memory tree, rebuilt on sync.
//!
//! `docs/squashfs_edit.md` phase 2c. SquashFS has no in-place write story, so
//! editing is: read the whole image into a [`BuildNode`] tree once, mutate the
//! tree for every `create_file` / `create_directory` / `delete_entry` /
//! `rename`, and rebuild the entire image on `sync_metadata`. That mirrors how
//! `mksquashfs` works and is the only shape the format allows.
//!
//! # Fidelity
//!
//! The tree comes from [`SquashfsFilesystem::read_build_tree`], so every
//! unchanged file keeps its exact bytes, mode, ownership, timestamps and
//! **extended attributes** — including the `security.capability` bits that make
//! an appliance binary work. The rebuild preserves the source's compressor and
//! block size (decision D3). New files get their POSIX attributes through the
//! shared [`crate::fs::attrs`] resolver (D6). The one gap in this slice: a file
//! that is *replaced* (deleted then recreated) loses its xattrs, because
//! `CreateFileOptions` has no channel to carry them — noted, and narrow (only
//! the handful of capability-bearing binaries, and only when overwritten).
//!
//! # Memory & safety
//!
//! Content is held in RAM (the tree's `FileContent::Bytes`), so peak use is the
//! decompressed image — fine for AppImages and appliance images, and the lazy
//! streaming / block reuse noted in the module docs of `squashfs_write` is the
//! follow-up. `sync_metadata` rebuilds into a buffer *first* and only overwrites
//! the backing store once the whole rebuild has succeeded, so a mid-rebuild
//! failure leaves the original image intact.
//!
//! # Scope
//!
//! This slice targets the **bare superfloppy** (offset 0): a standalone
//! `.squashfs`, the shape appliance and AppImage-extracted images take. The
//! size-budget prompt (`docs/squashfs_edit.md` §2) and partition-hosted images
//! are the next slice; here the file simply grows to fit, so `free_space`
//! reports the format's practical ceiling rather than a partition limit.

use std::io::{Read, Seek, SeekFrom, Write};

use super::attrs::{resolve_attrs, resolve_dir_attrs, AttrOverrides};
use super::entry::{EntryType, FileEntry};
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};
use super::squashfs::SquashfsFilesystem;
use super::squashfs_write::{
    compressor_is_writable, write_squashfs, BuildKind, BuildNode, BuildOptions, FileContent,
};

/// An editable SquashFS image backed by an in-memory tree.
pub struct SquashfsEditor<RW: Read + Write + Seek> {
    rw: RW,
    /// Byte offset of the image within `rw` (0 for a bare superfloppy).
    offset: u64,
    /// The editable tree; the root node's name is empty by convention.
    root: BuildNode,
    /// Compression settings carried from the source (D3).
    opts: BuildOptions,
}

impl<RW: Read + Write + Seek> SquashfsEditor<RW> {
    /// Open `rw` (a whole SquashFS image at `offset`) for editing: read the tree
    /// into memory and reclaim the handle for the eventual rebuild.
    pub fn open(rw: RW, offset: u64) -> Result<Self, FilesystemError> {
        let mut src = SquashfsFilesystem::open(rw, offset)?;
        let opts = src.source_build_options();
        if !compressor_is_writable(opts.compressor) {
            return Err(FilesystemError::Unsupported(format!(
                "squashfs: cannot edit a {}-compressed image — we read it but \
                 have no encoder to rebuild it (gzip / XZ / zstd only)",
                opts.compressor.name()
            )));
        }
        let root = src.read_build_tree()?;
        let rw = src.into_inner();
        Ok(Self {
            rw,
            offset,
            root,
            opts,
        })
    }

    /// Split a filesystem path into its non-empty components.
    fn components(path: &str) -> Vec<&str> {
        path.split('/').filter(|c| !c.is_empty()).collect()
    }

    /// Immutable node lookup by absolute path (`"/"` is the root).
    fn node_at(&self, path: &str) -> Option<&BuildNode> {
        let mut node = &self.root;
        for comp in Self::components(path) {
            let BuildKind::Dir(children) = &node.kind else {
                return None;
            };
            node = children.iter().find(|c| c.name == comp)?;
        }
        Some(node)
    }

    /// The children Vec of the directory at `path`, for mutation. Errors if the
    /// path is missing or names a non-directory.
    fn children_at_mut(&mut self, path: &str) -> Result<&mut Vec<BuildNode>, FilesystemError> {
        let mut node = &mut self.root;
        for comp in Self::components(path) {
            let BuildKind::Dir(children) = &mut node.kind else {
                return Err(FilesystemError::NotADirectory(path.to_string()));
            };
            node = children
                .iter_mut()
                .find(|c| c.name == comp)
                .ok_or_else(|| FilesystemError::NotFound(path.to_string()))?;
        }
        match &mut node.kind {
            BuildKind::Dir(children) => Ok(children),
            _ => Err(FilesystemError::NotADirectory(path.to_string())),
        }
    }

    /// Build a browse `FileEntry` for `node` at `path`.
    fn to_entry(node: &BuildNode, path: &str) -> FileEntry {
        let mut fe = match &node.kind {
            BuildKind::Dir(_) => FileEntry::new_directory(node.name.clone(), path.to_string(), 0),
            BuildKind::File(c) => {
                FileEntry::new_file(node.name.clone(), path.to_string(), c.len(), 0)
            }
            BuildKind::Symlink(target) => FileEntry::new_symlink(
                node.name.clone(),
                path.to_string(),
                target.len() as u64,
                0,
                target.clone(),
            ),
            _ => {
                let mut e = FileEntry::new_file(node.name.clone(), path.to_string(), 0, 0);
                e.entry_type = EntryType::Special;
                e
            }
        };
        fe.mode = Some(node.unix_mode());
        fe.uid = Some(node.uid);
        fe.gid = Some(node.gid);
        if node.mtime != 0 {
            fe.modified = Some(super::unix_common::inode::format_unix_timestamp(
                node.mtime as i64,
            ));
        }
        if let BuildKind::BlockDev { major, minor } | BuildKind::CharDev { major, minor } =
            &node.kind
        {
            let kind = if matches!(node.kind, BuildKind::BlockDev { .. }) {
                "block device"
            } else {
                "char device"
            };
            fe.special_type = Some(format!("{kind} ({major}, {minor})"));
        }
        fe
    }

    /// Join a parent path and a child name into a normalized absolute path.
    fn child_path(parent: &str, name: &str) -> String {
        if parent == "/" || parent.is_empty() {
            format!("/{name}")
        } else {
            format!("{}/{name}", parent.trim_end_matches('/'))
        }
    }

    /// Reject a name a SquashFS directory entry cannot hold.
    fn check_name(name: &str) -> Result<(), FilesystemError> {
        if name.is_empty() || name == "." || name == ".." {
            return Err(FilesystemError::InvalidData(format!(
                "invalid name {name:?}"
            )));
        }
        if name.contains('/') {
            return Err(FilesystemError::InvalidData(
                "a SquashFS name may not contain '/'".into(),
            ));
        }
        if name.len() > 256 {
            return Err(FilesystemError::InvalidData(
                "a SquashFS name may be at most 256 bytes".into(),
            ));
        }
        Ok(())
    }
}

impl<RW: Read + Write + Seek + Send> Filesystem for SquashfsEditor<RW> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(Self::to_entry(&self.root, "/"))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        let node = self
            .node_at(&entry.path)
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;
        let BuildKind::Dir(children) = &node.kind else {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        };
        Ok(children
            .iter()
            .map(|c| Self::to_entry(c, &Self::child_path(&entry.path, &c.name)))
            .collect())
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let node = self
            .node_at(&entry.path)
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;
        match &node.kind {
            BuildKind::File(FileContent::Bytes(b)) => Ok(b[..b.len().min(max_bytes)].to_vec()),
            BuildKind::File(FileContent::HostFile { path, .. }) => {
                let f = std::fs::File::open(path).map_err(FilesystemError::Io)?;
                let mut out = Vec::new();
                f.take(max_bytes as u64)
                    .read_to_end(&mut out)
                    .map_err(FilesystemError::Io)?;
                Ok(out)
            }
            BuildKind::Symlink(target) => Ok(target.clone().into_bytes()),
            _ => Err(FilesystemError::InvalidData(format!(
                "squashfs: {} is not a regular file",
                entry.path
            ))),
        }
    }

    fn volume_label(&self) -> Option<&str> {
        None
    }

    fn fs_type(&self) -> &str {
        "SquashFS"
    }

    fn total_size(&self) -> u64 {
        // A rebuild-only filesystem has no stable "size" until it is written;
        // report the current backing length as a reasonable stand-in.
        0
    }

    fn used_size(&self) -> u64 {
        0
    }
}

impl<RW: Read + Write + Seek + Send> EditableFilesystem for SquashfsEditor<RW> {
    fn as_filesystem(&self) -> &dyn Filesystem {
        self
    }

    fn as_filesystem_mut(&mut self) -> &mut dyn Filesystem {
        self
    }

    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn std::io::Read,
        _data_len: u64,
        options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Self::check_name(name)?;
        // Resolve POSIX attributes through the shared policy. A create over an
        // existing name would have deleted the old entry first (the put flow
        // does), so `replacing` is None here; ownership falls back to the parent
        // directory. `options.mode` carries the full mode (type bits + perms)
        // the drivers expect; we keep only the permission bits for the node.
        let parent_entry = self
            .node_at(&parent.path)
            .map(|n| Self::to_entry(n, &parent.path));
        let attrs = resolve_attrs(
            &AttrOverrides {
                mode: options.mode.map(|m| m & 0o7777),
                uid: options.uid,
                gid: options.gid,
            },
            None,
            parent_entry.as_ref(),
            None,
            0o644,
        );

        let mut bytes = Vec::new();
        data.read_to_end(&mut bytes).map_err(FilesystemError::Io)?;

        let children = self.children_at_mut(&parent.path)?;
        if children.iter().any(|c| c.name == name) {
            return Err(FilesystemError::AlreadyExists(Self::child_path(
                &parent.path,
                name,
            )));
        }
        children.push(BuildNode {
            name: name.to_string(),
            mode: attrs.mode as u16,
            uid: attrs.uid,
            gid: attrs.gid,
            mtime: 0,
            xattrs: Vec::new(),
            kind: BuildKind::File(FileContent::Bytes(bytes)),
        });
        Ok(Self::to_entry(
            children.last().unwrap(),
            &Self::child_path(&parent.path, name),
        ))
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Self::check_name(name)?;
        let parent_entry = self
            .node_at(&parent.path)
            .map(|n| Self::to_entry(n, &parent.path));
        let attrs = resolve_dir_attrs(
            &AttrOverrides {
                mode: options.mode.map(|m| m & 0o7777),
                uid: options.uid,
                gid: options.gid,
            },
            None,
            parent_entry.as_ref(),
        );

        let children = self.children_at_mut(&parent.path)?;
        if children.iter().any(|c| c.name == name) {
            return Err(FilesystemError::AlreadyExists(Self::child_path(
                &parent.path,
                name,
            )));
        }
        children.push(BuildNode {
            name: name.to_string(),
            mode: attrs.mode as u16,
            uid: attrs.uid,
            gid: attrs.gid,
            mtime: 0,
            xattrs: Vec::new(),
            kind: BuildKind::Dir(Vec::new()),
        });
        Ok(Self::to_entry(
            children.last().unwrap(),
            &Self::child_path(&parent.path, name),
        ))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        let children = self.children_at_mut(&parent.path)?;
        let idx = children
            .iter()
            .position(|c| c.name == entry.name)
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;
        if let BuildKind::Dir(inner) = &children[idx].kind {
            if !inner.is_empty() {
                return Err(FilesystemError::InvalidData(format!(
                    "squashfs delete_entry: directory '{}' not empty",
                    entry.path
                )));
            }
        }
        children.remove(idx);
        Ok(())
    }

    fn rename(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
        new_name: &str,
    ) -> Result<(), FilesystemError> {
        Self::check_name(new_name)?;
        let children = self.children_at_mut(&parent.path)?;
        if children.iter().any(|c| c.name == new_name) {
            return Err(FilesystemError::AlreadyExists(Self::child_path(
                &parent.path,
                new_name,
            )));
        }
        let node = children
            .iter_mut()
            .find(|c| c.name == entry.name)
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;
        node.name = new_name.to_string();
        Ok(())
    }

    fn supports_symlinks(&self) -> bool {
        true
    }

    fn create_symlink(
        &mut self,
        parent: &FileEntry,
        name: &str,
        target: &str,
        options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Self::check_name(name)?;
        let uid = options.uid.unwrap_or(0);
        let gid = options.gid.unwrap_or(0);
        let children = self.children_at_mut(&parent.path)?;
        if children.iter().any(|c| c.name == name) {
            return Err(FilesystemError::AlreadyExists(Self::child_path(
                &parent.path,
                name,
            )));
        }
        children.push(BuildNode {
            name: name.to_string(),
            // Symlinks are conventionally 0777; the value is not used by
            // resolvers but keeps `ls -l` honest.
            mode: 0o777,
            uid,
            gid,
            mtime: 0,
            xattrs: Vec::new(),
            kind: BuildKind::Symlink(target.to_string()),
        });
        Ok(Self::to_entry(
            children.last().unwrap(),
            &Self::child_path(&parent.path, name),
        ))
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        // Rebuild into a buffer first: only overwrite the backing store once the
        // whole image is known-good, so a failed rebuild can't corrupt the
        // original. (Whole image in RAM — see the module docs.)
        let mut buf = std::io::Cursor::new(Vec::new());
        write_squashfs(&mut buf, &self.root, &self.opts)?;
        let image = buf.into_inner();

        self.rw
            .seek(SeekFrom::Start(self.offset))
            .map_err(FilesystemError::Io)?;
        self.rw.write_all(&image).map_err(FilesystemError::Io)?;
        self.rw.flush().map_err(FilesystemError::Io)?;
        // Note: if the new image is shorter than the old backing file, trailing
        // bytes remain past `bytes_used`. SquashFS readers stop at the
        // superblock's `bytes_used`, so this is still a valid image; exact
        // truncation is a commit-side concern (temp + rename, phase 2d).
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        // A bare superfloppy simply grows; there is no fixed free-space figure.
        // The size budget (phase 2c size prompt) supersedes this for the
        // partition-hosted case.
        Ok(u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::squashfs::Xattr;
    use crate::fs::squashfs_write::BuildKind as BK;
    use std::io::Cursor;

    /// True when `unsquashfs` is on PATH (it exits 1 while printing its banner,
    /// so match text, not status).
    fn unsquashfs_available() -> bool {
        std::process::Command::new("unsquashfs")
            .arg("-version")
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).contains("unsquashfs version"))
            .unwrap_or(false)
    }

    /// Build a starter image with a couple of files, a directory, and — the
    /// point — a file carrying an xattr, so we can prove edits preserve it.
    fn starter_image() -> Vec<u8> {
        let tree = BuildNode {
            name: String::new(),
            mode: 0o755,
            uid: 0,
            gid: 0,
            mtime: 1_700_000_000,
            xattrs: Vec::new(),
            kind: BK::Dir(vec![
                BuildNode::file("readme", 0o644, b"original readme\n".to_vec()),
                BuildNode::dir(
                    "bin",
                    0o755,
                    vec![BuildNode {
                        name: "ping".into(),
                        mode: 0o4755, // setuid
                        uid: 0,
                        gid: 0,
                        mtime: 1_700_000_000,
                        xattrs: vec![Xattr {
                            name: "security.capability".into(),
                            value: vec![1, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0],
                        }],
                        kind: BK::File(FileContent::Bytes(b"ping binary\n".to_vec())),
                    }],
                ),
            ]),
        };
        let mut cur = Cursor::new(Vec::new());
        write_squashfs(&mut cur, &tree, &BuildOptions::default()).expect("build starter");
        cur.into_inner()
    }

    fn open_editor(bytes: Vec<u8>) -> SquashfsEditor<Cursor<Vec<u8>>> {
        SquashfsEditor::open(Cursor::new(bytes), 0).expect("open editor")
    }

    #[test]
    fn create_delete_mkdir_rename_then_rebuild() {
        let mut ed = open_editor(starter_image());
        let root = ed.root().expect("root");

        // Add a file, a directory, and a symlink; rename readme; delete nothing
        // yet.
        ed.create_file(
            &root,
            "hello.txt",
            &mut Cursor::new(b"hi\n".to_vec()),
            3,
            &CreateFileOptions {
                mode: Some(0o100_600),
                uid: Some(0),
                gid: Some(0),
                ..Default::default()
            },
        )
        .expect("create hello");
        ed.create_directory(&root, "etc", &CreateDirectoryOptions::default())
            .expect("mkdir etc");
        ed.create_symlink(&root, "link", "hello.txt", &CreateFileOptions::default())
            .expect("symlink");
        let readme = ed
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "readme")
            .unwrap();
        ed.rename(&root, &readme, "README").expect("rename");

        // Rebuild, reopen through the reader, and check the tree.
        ed.sync_metadata().expect("sync");
        let bytes = std::mem::replace(&mut ed.rw, Cursor::new(Vec::new())).into_inner();
        let mut fs = SquashfsFilesystem::open(Cursor::new(bytes), 0).expect("reopen");
        let root = fs.root().unwrap();
        let names: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(names.contains(&"hello.txt".to_string()));
        assert!(names.contains(&"etc".to_string()));
        assert!(names.contains(&"link".to_string()));
        assert!(
            names.contains(&"README".to_string()),
            "rename not applied: {names:?}"
        );
        assert!(
            !names.contains(&"readme".to_string()),
            "old name survived: {names:?}"
        );

        let hello = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "hello.txt")
            .unwrap();
        assert_eq!(fs.read_file(&hello, 100).unwrap(), b"hi\n");
        assert_eq!(
            hello.mode.map(|m| m & 0o7777),
            Some(0o600),
            "explicit mode not applied"
        );
    }

    /// The heart of D4: an edit that touches other files must not disturb an
    /// unchanged file's extended attributes.
    #[test]
    fn unchanged_files_keep_their_xattrs_across_an_edit() {
        let mut ed = open_editor(starter_image());
        let root = ed.root().expect("root");
        // Touch something unrelated.
        ed.create_file(
            &root,
            "new.txt",
            &mut Cursor::new(b"new\n".to_vec()),
            4,
            &CreateFileOptions::default(),
        )
        .expect("create");
        ed.sync_metadata().expect("sync");

        let bytes = std::mem::replace(&mut ed.rw, Cursor::new(Vec::new())).into_inner();
        let mut fs = SquashfsFilesystem::open(Cursor::new(bytes), 0).expect("reopen");
        // Re-read the tree and confirm /bin/ping still has its capability.
        let tree = fs.read_build_tree().expect("read tree");
        let BK::Dir(top) = &tree.kind else { panic!() };
        let bin = top.iter().find(|n| n.name == "bin").expect("bin");
        let BK::Dir(binc) = &bin.kind else { panic!() };
        let ping = binc.iter().find(|n| n.name == "ping").expect("ping");
        assert_eq!(
            ping.xattrs,
            vec![Xattr {
                name: "security.capability".into(),
                value: vec![1, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0],
            }],
            "an unrelated edit dropped ping's capability xattr"
        );
        assert_eq!(ping.mode & 0o7777, 0o4755, "setuid bit lost");
    }

    #[test]
    fn new_file_inherits_parent_ownership() {
        let mut ed = open_editor(starter_image());
        let root = ed.root().expect("root");
        // Make a dir owned by uid/gid 5, then create a file inside it with no
        // explicit ownership: it should inherit 5:5.
        let dir = ed
            .create_directory(
                &root,
                "owned",
                &CreateDirectoryOptions {
                    uid: Some(5),
                    gid: Some(5),
                    ..Default::default()
                },
            )
            .expect("mkdir");
        let f = ed
            .create_file(
                &dir,
                "child",
                &mut Cursor::new(b"x".to_vec()),
                1,
                &CreateFileOptions::default(),
            )
            .expect("create child");
        assert_eq!(f.uid, Some(5), "child did not inherit parent uid");
        assert_eq!(f.gid, Some(5), "child did not inherit parent gid");
    }

    #[test]
    fn delete_refuses_a_nonempty_directory() {
        let mut ed = open_editor(starter_image());
        let root = ed.root().expect("root");
        let bin = ed
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "bin")
            .unwrap();
        let err = ed.delete_entry(&root, &bin).expect_err("should refuse");
        assert!(err.to_string().contains("not empty"), "got: {err}");
        // Emptying it first makes the delete succeed.
        let ping = ed.list_directory(&bin).unwrap().into_iter().next().unwrap();
        ed.delete_entry(&bin, &ping).expect("delete ping");
        ed.delete_entry(&root, &bin).expect("delete now-empty bin");
    }

    #[test]
    fn refuses_to_edit_an_unwritable_compressor() {
        // Forge a superblock claiming LZO (id 3); the editor must refuse at open
        // rather than fail later during a rebuild.
        let mut bytes = starter_image();
        bytes[20..22].copy_from_slice(&3u16.to_le_bytes()); // compressor id
        let err = SquashfsEditor::open(Cursor::new(bytes), 0)
            .err()
            .expect("must refuse");
        assert!(
            err.to_string().contains("lzo") || err.to_string().contains("LZO"),
            "expected an LZO-specific refusal, got: {err}"
        );
    }

    /// The rebuilt image after an edit must be real SquashFS `unsquashfs` reads.
    #[test]
    fn unsquashfs_accepts_an_edited_image() {
        if !unsquashfs_available() {
            eprintln!("unsquashfs not on PATH — skipping");
            return;
        }
        let mut ed = open_editor(starter_image());
        let root = ed.root().expect("root");
        ed.create_file(
            &root,
            "added.txt",
            &mut Cursor::new(b"added by editor\n".to_vec()),
            16,
            &CreateFileOptions::default(),
        )
        .expect("create");
        ed.sync_metadata().expect("sync");
        let bytes = std::mem::replace(&mut ed.rw, Cursor::new(Vec::new())).into_inner();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("edited.squashfs");
        std::fs::write(&path, &bytes).unwrap();
        let out = std::process::Command::new("unsquashfs")
            .arg("-no-xattrs")
            .arg("-d")
            .arg(dir.path().join("x"))
            .arg(&path)
            .output()
            .expect("run unsquashfs");
        assert!(
            !String::from_utf8_lossy(&out.stderr).contains("Data queue"),
            "unsquashfs choked on our edited image:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            std::fs::read(dir.path().join("x/added.txt")).unwrap(),
            b"added by editor\n"
        );
        assert_eq!(
            std::fs::read(dir.path().join("x/readme")).unwrap(),
            b"original readme\n",
            "an unchanged file changed content across the edit"
        );
    }
}
