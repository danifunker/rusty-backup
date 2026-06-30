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

    /// Look up the cached opticaldiscs entry for one of our entries' paths.
    fn opt_entry(&self, path: &str) -> Result<OptEntry, FilesystemError> {
        self.by_path
            .get(path)
            .cloned()
            .ok_or_else(|| FilesystemError::NotFound(path.to_string()))
    }
}

/// Translate an opticaldiscs entry into ours (the path strings share the same
/// `/`-rooted convention).
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
    // Optical filesystems carry no fork; mark size on files only.
    if !fe.is_directory() {
        fe.size = e.size;
    }
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
}
