//! Adapter presenting an optical disc image (ISO 9660 / bin-cue / MDF-MDS / NRG
//! / CCD …) as a [`Filesystem`], so Commander Mode can descend into it with the
//! inline `+` tree like any other wrapper.
//!
//! The heavy lifting — container detection, track layout, and the on-disc
//! filesystem (ISO 9660, plus HFS/HFS+/EFS hybrids) — lives in the
//! `opticaldiscs` crate, which exposes its *own* `browse::Filesystem` trait and
//! `FileEntry`. This module is a thin translation layer between that and our
//! [`Filesystem`] / [`FileEntry`], caching each opticaldiscs entry by path so
//! its filesystem-specific `location` hint round-trips on reads.
//!
//! Multi-file formats (bin + cue, mdf + mds) reference a sibling data file by
//! relative path, so the caller must open the *real* on-disk path (not a
//! materialized copy) for those — see the disk-image/optical handling in
//! [`crate::model::wrapper_tree`].

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;

use opticaldiscs::browse::entry::{EntryType as OptType, FileEntry as OptEntry};
use opticaldiscs::browse::filesystem::Filesystem as OptFilesystem;
use opticaldiscs::browse::open_disc_filesystem;
use opticaldiscs::detect::DiscImageInfo;

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};

/// A read-only [`Filesystem`] over an optical disc image's data filesystem.
pub struct OpticalFilesystem {
    inner: Box<dyn OptFilesystem>,
    /// Our path string -> the opticaldiscs entry (kept so `list_directory` /
    /// reads pass back the original entry with its `location` hint).
    by_path: HashMap<String, OptEntry>,
    label: Option<String>,
    fs_type: String,
}

impl OpticalFilesystem {
    /// Open the optical image at `path`. For bin/cue and mdf/mds the sibling
    /// data file must sit next to `path`.
    pub fn open(path: &Path, label: Option<String>) -> Result<Self, FilesystemError> {
        let info = DiscImageInfo::open(path)
            .map_err(|e| FilesystemError::Parse(format!("open disc image: {e:#}")))?;
        let fs_type = format!("{:?}", info.filesystem);
        let mut inner = open_disc_filesystem(&info)
            .map_err(|e| FilesystemError::Parse(format!("open disc filesystem: {e}")))?;
        let root = inner
            .root()
            .map_err(|e| FilesystemError::Parse(format!("disc root: {e}")))?;
        let mut by_path = HashMap::new();
        by_path.insert(root.path.clone(), root);
        Ok(Self {
            inner,
            by_path,
            label,
            fs_type,
        })
    }

    /// Wrap an already-opened opticaldiscs filesystem — e.g. a specific side of
    /// a hybrid Mac/PC disc selected via `open_hybrid_filesystem`, which
    /// [`open`](Self::open) (auto/primary only) can't reach. `fs_type` is the
    /// display token (e.g. "Hfs", "Iso9660"); `label` the volume name override.
    pub fn from_inner(
        mut inner: Box<dyn OptFilesystem>,
        fs_type: String,
        label: Option<String>,
    ) -> Result<Self, FilesystemError> {
        let root = inner
            .root()
            .map_err(|e| FilesystemError::Parse(format!("disc root: {e}")))?;
        let mut by_path = HashMap::new();
        by_path.insert(root.path.clone(), root);
        Ok(Self {
            inner,
            by_path,
            label,
            fs_type,
        })
    }

    /// Look up the cached opticaldiscs entry for one of our entries' paths.
    ///
    /// The cache fills lazily as directories are listed, so a path is present
    /// only after its parent has been listed *on this instance*. That holds for
    /// interactive browsing, but not when an entry produced by a *different*
    /// instance is handed back to a fresh one — which is exactly what the
    /// Commander async copy/checksum workers do: they reopen the disc on a
    /// worker thread and call `write_file_to` / `list_directory` with entries
    /// captured from the live pane's filesystem. On a cache miss we resolve the
    /// path by walking from the root, listing each directory along the way so
    /// `by_path` back-fills and the lookup succeeds.
    fn opt_entry(&mut self, path: &str) -> Result<OptEntry, FilesystemError> {
        if let Some(e) = self.by_path.get(path) {
            return Ok(e.clone());
        }
        self.resolve_from_root(path)
    }

    /// Walk from the disc root toward `target`, listing each directory on the
    /// way so its children land in `by_path`, and return the entry once its
    /// path matches. Errors with [`FilesystemError::NotFound`] if the path
    /// doesn't exist. Path strings are compared whole (never split on `/`) so a
    /// filename containing a slash — legal on HFS — still resolves.
    fn resolve_from_root(&mut self, target: &str) -> Result<OptEntry, FilesystemError> {
        let root = self
            .inner
            .root()
            .map_err(|e| FilesystemError::Parse(format!("disc root: {e}")))?;
        self.by_path.insert(root.path.clone(), root.clone());
        if root.path == target {
            return Ok(root);
        }
        let mut current = root;
        loop {
            let kids = self
                .inner
                .list_directory(&current)
                .map_err(|e| FilesystemError::Parse(format!("list '{}': {e}", current.path)))?;
            // The child that is `target` itself, or a directory whose path is a
            // `/`-delimited prefix of `target` (the next hop toward it).
            let mut next: Option<OptEntry> = None;
            for k in kids {
                let is_target = k.path == target;
                let is_ancestor = matches!(k.entry_type, OptType::Directory)
                    && target.len() > k.path.len()
                    && target.as_bytes().get(k.path.len()) == Some(&b'/')
                    && target.starts_with(&k.path);
                self.by_path.insert(k.path.clone(), k.clone());
                if is_target {
                    return Ok(k);
                }
                if is_ancestor {
                    next = Some(k);
                }
            }
            match next {
                Some(dir) => current = dir,
                None => return Err(FilesystemError::NotFound(target.to_string())),
            }
        }
    }
}

/// Translate an opticaldiscs entry into ours (the path strings share the same
/// `/`-rooted convention). Carries the HFS-on-CD metadata opticaldiscs exposes:
/// resource-fork size and Finder type/creator/flags. (opticaldiscs' browse API
/// has no date field, so the Modified column stays blank for optical discs.)
fn translate(e: &OptEntry) -> FileEntry {
    let mut fe = match e.entry_type {
        OptType::Directory => FileEntry::new_directory(e.name.clone(), e.path.clone(), e.location),
        OptType::File => {
            if let Some(target) = &e.symlink_target {
                FileEntry::new_symlink(
                    e.name.clone(),
                    e.path.clone(),
                    e.size,
                    e.location,
                    target.clone(),
                )
            } else {
                FileEntry::new_file(e.name.clone(), e.path.clone(), e.size, e.location)
            }
        }
    };
    fe.resource_fork_size = e.resource_fork_size;
    fe.type_code = e.type_code;
    fe.creator_code = e.creator_code;
    fe.finder_flags = e.finder_flags;
    fe
}

impl Filesystem for OpticalFilesystem {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        let root = self
            .inner
            .root()
            .map_err(|e| FilesystemError::Parse(format!("disc root: {e}")))?;
        self.by_path.insert(root.path.clone(), root.clone());
        Ok(translate(&root))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        let oe = self.opt_entry(&entry.path)?;
        let kids = self
            .inner
            .list_directory(&oe)
            .map_err(|e| FilesystemError::Parse(format!("list '{}': {e}", entry.path)))?;
        let mut out = Vec::with_capacity(kids.len());
        for k in kids {
            self.by_path.insert(k.path.clone(), k.clone());
            out.push(translate(&k));
        }
        Ok(out)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let oe = self.opt_entry(&entry.path)?;
        let mut data = self
            .inner
            .read_file(&oe)
            .map_err(|e| FilesystemError::Parse(format!("read '{}': {e}", entry.path)))?;
        data.truncate(max_bytes);
        Ok(data)
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let oe = self.opt_entry(&entry.path)?;
        let data = self
            .inner
            .read_file(&oe)
            .map_err(|e| FilesystemError::Parse(format!("read '{}': {e}", entry.path)))?;
        writer.write_all(&data)?;
        Ok(data.len() as u64)
    }

    fn write_resource_fork_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let oe = self.opt_entry(&entry.path)?;
        match self
            .inner
            .read_resource_fork(&oe)
            .map_err(|e| FilesystemError::Parse(format!("read rsrc '{}': {e}", entry.path)))?
        {
            Some(data) => {
                writer.write_all(&data)?;
                Ok(data.len() as u64)
            }
            None => Ok(0),
        }
    }

    fn resource_fork_size(&mut self, entry: &FileEntry) -> u64 {
        self.by_path
            .get(&entry.path)
            .and_then(|e| e.resource_fork_size)
            .unwrap_or(0)
    }

    fn volume_label(&self) -> Option<&str> {
        self.inner.volume_name().or(self.label.as_deref())
    }

    fn fs_type(&self) -> &str {
        // e.g. "Iso9660", "Hfs", "HfsPlus".
        &self.fs_type
    }

    fn total_size(&self) -> u64 {
        0
    }

    fn used_size(&self) -> u64 {
        0
    }

    /// Delegate to the opticaldiscs backend's allocation/logical block size
    /// (HFS/HFS+ allocation block, ISO 9660 logical block). Lets `du` round
    /// each fork up to a real block on optical HFS discs instead of the
    /// 512-byte floor.
    fn allocation_unit(&self) -> Option<u64> {
        self.inner.allocation_unit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opticaldiscs::browse::filesystem::FilesystemError as OptError;

    /// A fixed in-memory disc tree resolved purely by `location` (a node index),
    /// exactly as the real opticaldiscs backends resolve HFS/ISO entries. The
    /// point is that the *inner* fs needs no priming — only our `by_path` cache
    /// did, which is the bug under test.
    struct MockDisc {
        nodes: Vec<MockNode>,
    }
    struct MockNode {
        entry: OptEntry,
        children: Vec<usize>,
        data: Vec<u8>,
        rsrc: Option<Vec<u8>>,
    }

    fn mock_disc() -> MockDisc {
        // /                      (0)
        //   sub/                 (1)
        //     deep.txt           (2)  data="hello"
        //     rsrconly           (3)  data="", rsrc="RSRC"
        let root = OptEntry::new_directory("".into(), "/".into(), 0);
        let sub = OptEntry::new_directory("sub".into(), "/sub".into(), 1);
        let deep = OptEntry::new_file("deep.txt".into(), "/sub/deep.txt".into(), 5, 2);
        let mut rsrconly = OptEntry::new_file("rsrconly".into(), "/sub/rsrconly".into(), 0, 3);
        rsrconly.resource_fork_size = Some(4);
        MockDisc {
            nodes: vec![
                MockNode {
                    entry: root,
                    children: vec![1],
                    data: vec![],
                    rsrc: None,
                },
                MockNode {
                    entry: sub,
                    children: vec![2, 3],
                    data: vec![],
                    rsrc: None,
                },
                MockNode {
                    entry: deep,
                    children: vec![],
                    data: b"hello".to_vec(),
                    rsrc: None,
                },
                MockNode {
                    entry: rsrconly,
                    children: vec![],
                    data: vec![],
                    rsrc: Some(b"RSRC".to_vec()),
                },
            ],
        }
    }

    impl OptFilesystem for MockDisc {
        fn root(&mut self) -> Result<OptEntry, OptError> {
            Ok(self.nodes[0].entry.clone())
        }
        fn list_directory(&mut self, entry: &OptEntry) -> Result<Vec<OptEntry>, OptError> {
            let n = &self.nodes[entry.location as usize];
            Ok(n.children
                .iter()
                .map(|&i| self.nodes[i].entry.clone())
                .collect())
        }
        fn read_file(&mut self, entry: &OptEntry) -> Result<Vec<u8>, OptError> {
            Ok(self.nodes[entry.location as usize].data.clone())
        }
        fn read_file_range(
            &mut self,
            entry: &OptEntry,
            offset: u64,
            length: usize,
        ) -> Result<Vec<u8>, OptError> {
            let d = &self.nodes[entry.location as usize].data;
            let start = (offset as usize).min(d.len());
            let end = (start + length).min(d.len());
            Ok(d[start..end].to_vec())
        }
        fn read_resource_fork(&mut self, entry: &OptEntry) -> Result<Option<Vec<u8>>, OptError> {
            Ok(self.nodes[entry.location as usize].rsrc.clone())
        }
        fn read_resource_fork_range(
            &mut self,
            entry: &OptEntry,
            offset: u64,
            length: usize,
        ) -> Result<Option<Vec<u8>>, OptError> {
            Ok(self.nodes[entry.location as usize].rsrc.as_ref().map(|r| {
                let start = (offset as usize).min(r.len());
                let end = (start + length).min(r.len());
                r[start..end].to_vec()
            }))
        }
        fn volume_name(&self) -> Option<&str> {
            None
        }
    }

    /// Build an `OpticalFilesystem` the way `open()` leaves it: inner mounted,
    /// only the root cached in `by_path` — no directory listed yet.
    fn fresh(disc: MockDisc) -> OpticalFilesystem {
        let root = disc.nodes[0].entry.clone();
        let mut by_path = HashMap::new();
        by_path.insert(root.path.clone(), root);
        OpticalFilesystem {
            inner: Box::new(disc),
            by_path,
            label: None,
            fs_type: "MockHfs".into(),
        }
    }

    /// Regression: the Commander async copy reopens the disc fresh (only root
    /// cached) and hands it a deep entry captured from the live pane. Reads must
    /// still resolve — before the fix `opt_entry` returned NotFound on the miss.
    #[test]
    fn fresh_fs_resolves_deep_entry_without_prior_listing() {
        // A deep file entry as the live pane would have produced it.
        let deep = FileEntry::new_file("deep.txt".into(), "/sub/deep.txt".into(), 5, 2);

        let mut fs = fresh(mock_disc());
        let mut out = Vec::new();
        let n = fs
            .write_file_to(&deep, &mut out)
            .expect("write_file_to must resolve a deep entry on a fresh instance");
        assert_eq!(n, 5);
        assert_eq!(out, b"hello");

        // read_file too.
        let mut fs = fresh(mock_disc());
        assert_eq!(fs.read_file(&deep, usize::MAX).unwrap(), b"hello");
    }

    #[test]
    fn fresh_fs_resolves_deep_resource_fork() {
        let rf = FileEntry::new_file("rsrconly".into(), "/sub/rsrconly".into(), 0, 3);
        let mut fs = fresh(mock_disc());
        let mut out = Vec::new();
        let n = fs.write_resource_fork_to(&rf, &mut out).unwrap();
        assert_eq!(n, 4);
        assert_eq!(out, b"RSRC");
    }

    #[test]
    fn fresh_fs_lists_deep_directory_without_prior_listing() {
        let sub = FileEntry::new_directory("sub".into(), "/sub".into(), 1);
        let mut fs = fresh(mock_disc());
        let kids = fs
            .list_directory(&sub)
            .expect("list_directory must resolve a deep dir on a fresh instance");
        let names: Vec<_> = kids.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["deep.txt", "rsrconly"]);
    }

    #[test]
    fn genuinely_missing_path_still_errors() {
        let ghost = FileEntry::new_file("ghost".into(), "/sub/ghost".into(), 1, 99);
        let mut fs = fresh(mock_disc());
        let mut out = Vec::new();
        let err = fs.write_file_to(&ghost, &mut out).unwrap_err();
        assert!(
            matches!(err, FilesystemError::NotFound(ref p) if p == "/sub/ghost"),
            "a path that does not exist should still be NotFound, got {err:?}"
        );
    }
}
