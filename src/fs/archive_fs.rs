//! Read-only [`Filesystem`] view over a classic-Mac archive (StuffIt, StuffIt
//! 5, Compact Pro, MacBinary, and BinHex-wrapped forms of all of them).
//!
//! Commander Mode descends into an archive as if it were a directory tree and
//! copies files out of it — both forks and Finder type/creator preserved —
//! without extracting to disk first. Because the copy engine is generic over
//! the [`Filesystem`] trait, presenting the archive as a `Filesystem` is the
//! whole integration: `list_directory` / `write_file_to` /
//! `write_resource_fork_to` are all the copy path needs.
//!
//! The archive is fully decoded in memory by [`crate::macarchive::extract`];
//! fork bytes are produced lazily per read via
//! [`crate::macarchive::stuffit::decompress_fork`]. Volumes are tiny (a few MB
//! at most), so holding the decoded buffer is fine.

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};
use crate::macarchive::extract;
use crate::macarchive::stuffit::{decompress_fork, StuffItArchive};

/// Sentinel `location` for a synthesized intermediate directory — one that has
/// children but no explicit archive entry of its own. Such a node is never
/// read (directories carry no fork), so it needs no backing index.
const SYNTH_DIR: u64 = u64::MAX;

/// A read-only filesystem backed by an in-memory Mac archive.
pub struct ArchiveFilesystem {
    /// Decoded archive bytes; `ForkInfo` offsets are relative to this buffer.
    bytes: Vec<u8>,
    archive: StuffItArchive,
    label: Option<String>,
    /// Precomputed listing: parent-directory path string -> child entries.
    /// Built once at open so `list_directory` is a map lookup and intermediate
    /// directories missing an explicit marker are synthesized.
    children: HashMap<String, Vec<FileEntry>>,
    total: u64,
}

impl ArchiveFilesystem {
    /// Open an archive file from disk.
    pub fn open_path(path: &Path) -> Result<Self, FilesystemError> {
        let (bytes, archive) = extract::open(path)
            .map_err(|e| FilesystemError::Parse(format!("open archive: {e:#}")))?;
        let label = path.file_name().and_then(|n| n.to_str()).map(String::from);
        Ok(Self::from_parts(bytes, archive, label))
    }

    /// Open an archive already held in memory (e.g. a file's bytes pulled out
    /// of a parent archive or disk image for nested descent). `label` is the
    /// display name of the wrapper file, used as the volume label.
    pub fn open_bytes(raw: Vec<u8>, label: Option<String>) -> Result<Self, FilesystemError> {
        let (bytes, archive) = extract::open_bytes(raw)
            .map_err(|e| FilesystemError::Parse(format!("open archive: {e:#}")))?;
        Ok(Self::from_parts(bytes, archive, label))
    }

    fn from_parts(bytes: Vec<u8>, archive: StuffItArchive, label: Option<String>) -> Self {
        let mut children: HashMap<String, Vec<FileEntry>> = HashMap::new();
        children.insert("/".to_string(), Vec::new());
        let mut total: u64 = 0;
        // Track which directory path strings already have a child list so we
        // synthesize each intermediate dir exactly once.
        for (idx, e) in archive.entries.iter().enumerate() {
            // Ensure every ancestor directory exists as a navigable node.
            for depth in 1..e.path.len() {
                let dir_parts = &e.path[..depth];
                let dir_path = path_string(dir_parts);
                if !children.contains_key(&dir_path) {
                    children.insert(dir_path.clone(), Vec::new());
                    // Register the synthesized dir under its own parent, unless
                    // an explicit entry for it shows up later (deduped below).
                    let parent = parent_string(dir_parts);
                    let name = dir_parts[depth - 1].clone();
                    children
                        .entry(parent)
                        .or_default()
                        .push(FileEntry::new_directory(name, dir_path, SYNTH_DIR));
                }
            }

            let fe = to_file_entry(&archive, idx);
            if e.is_dir {
                children.entry(fe.path.clone()).or_default();
            } else {
                total += fe.size + fe.resource_fork_size.unwrap_or(0);
            }
            let parent = parent_string(&e.path);
            let bucket = children.entry(parent).or_default();
            // Prefer an explicit entry over a previously synthesized stub.
            if e.is_dir {
                bucket.retain(|c| !(c.path == fe.path && c.location == SYNTH_DIR));
            }
            bucket.push(fe);
        }
        Self {
            bytes,
            archive,
            label,
            children,
            total,
        }
    }

    /// Resolve a `FileEntry` back to its archive index. `location` is the index
    /// we stamped at listing time; fall back to a path scan if it's stale.
    fn index_of(&self, entry: &FileEntry) -> Option<usize> {
        if (entry.location as usize) < self.archive.entries.len()
            && entry.location != SYNTH_DIR
            && path_string(&self.archive.entries[entry.location as usize].path) == entry.path
        {
            return Some(entry.location as usize);
        }
        self.archive
            .entries
            .iter()
            .position(|e| path_string(&e.path) == entry.path)
    }
}

/// `"/"`-rooted path string for a sequence of name components.
fn path_string(parts: &[String]) -> String {
    if parts.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", parts.join("/"))
    }
}

/// Path string of the parent directory of `parts`.
fn parent_string(parts: &[String]) -> String {
    if parts.len() <= 1 {
        "/".to_string()
    } else {
        path_string(&parts[..parts.len() - 1])
    }
}

/// Map one archive entry to a [`FileEntry`], stamping `location` with the
/// archive index for O(1) reads and carrying Finder type/creator/flags/dates.
fn to_file_entry(archive: &StuffItArchive, idx: usize) -> FileEntry {
    let e = &archive.entries[idx];
    let path = path_string(&e.path);
    if e.is_dir {
        let mut fe = FileEntry::new_directory(e.name.clone(), path, idx as u64);
        fe.mac_dates = Some((e.create_date, e.mod_date, 0));
        fe.modified = crate::fs::hfs_common::format_mac_date(e.mod_date);
        fe
    } else {
        let size = e
            .data
            .as_ref()
            .map(|f| f.uncompressed_len as u64)
            .unwrap_or(0);
        let mut fe = FileEntry::new_file(e.name.clone(), path, size, idx as u64);
        fe.type_code = Some(e.type_code);
        fe.creator_code = Some(e.creator_code);
        fe.finder_flags = Some(e.finder_flags);
        fe.resource_fork_size = e.rsrc.as_ref().map(|f| f.uncompressed_len as u64);
        fe.mac_dates = Some((e.create_date, e.mod_date, 0));
        fe.modified = crate::fs::hfs_common::format_mac_date(e.mod_date);
        fe
    }
}

impl Filesystem for ArchiveFilesystem {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::root())
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        Ok(self.children.get(&entry.path).cloned().unwrap_or_default())
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let idx = self
            .index_of(entry)
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;
        let mut data = match &self.archive.entries[idx].data {
            Some(fork) => decompress_fork(&self.bytes, fork)
                .map_err(|e| FilesystemError::Parse(format!("decompress data fork: {e:#}")))?,
            None => Vec::new(),
        };
        data.truncate(max_bytes);
        Ok(data)
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let idx = self
            .index_of(entry)
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;
        let Some(fork) = &self.archive.entries[idx].data else {
            return Ok(0);
        };
        let data = decompress_fork(&self.bytes, fork)
            .map_err(|e| FilesystemError::Parse(format!("decompress data fork: {e:#}")))?;
        writer.write_all(&data)?;
        Ok(data.len() as u64)
    }

    fn write_resource_fork_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let idx = self
            .index_of(entry)
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;
        let Some(fork) = &self.archive.entries[idx].rsrc else {
            return Ok(0);
        };
        let data = decompress_fork(&self.bytes, fork)
            .map_err(|e| FilesystemError::Parse(format!("decompress resource fork: {e:#}")))?;
        writer.write_all(&data)?;
        Ok(data.len() as u64)
    }

    fn resource_fork_size(&mut self, entry: &FileEntry) -> u64 {
        self.index_of(entry)
            .and_then(|idx| self.archive.entries[idx].rsrc.as_ref())
            .map(|f| f.uncompressed_len as u64)
            .unwrap_or(0)
    }

    fn volume_label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    fn fs_type(&self) -> &str {
        "Mac Archive"
    }

    fn total_size(&self) -> u64 {
        self.total
    }

    fn used_size(&self) -> u64 {
        self.total
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::macarchive::stuffit::{
        build_archive_tree, StuffItInput, StuffItInputNode, WriteMethod,
    };

    fn file_node(
        name: &str,
        data: &[u8],
        rsrc: &[u8],
        ty: [u8; 4],
        cr: [u8; 4],
    ) -> StuffItInputNode {
        StuffItInputNode::File(StuffItInput {
            name: name.into(),
            type_code: ty,
            creator_code: cr,
            finder_flags: 0,
            create_date: 0,
            mod_date: 0,
            data_fork: data.to_vec(),
            resource_fork: rsrc.to_vec(),
        })
    }

    /// Build a small archive (root file + a folder with a forked file) in
    /// memory, open it as a filesystem, and verify listing + fork reads.
    fn sample_fs() -> ArchiveFilesystem {
        let tree = vec![
            file_node("readme.txt", b"hello", b"", *b"TEXT", *b"ttxt"),
            StuffItInputNode::Folder {
                name: "App".into(),
                finder_flags: 0,
                create_date: 0,
                mod_date: 0,
                children: vec![file_node("Tool", b"DATA", b"RSRC", *b"APPL", *b"Tool")],
            },
        ];
        let bytes = build_archive_tree(&tree, WriteMethod::Store).expect("build archive");
        ArchiveFilesystem::open_bytes(bytes, Some("sample.sit".into())).expect("open as fs")
    }

    #[test]
    fn lists_root_and_subdir() {
        let mut fs = sample_fs();
        let root = fs.root().unwrap();
        let mut names: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .iter()
            .map(|e| e.name.clone())
            .collect();
        names.sort();
        assert_eq!(names, vec!["App".to_string(), "readme.txt".to_string()]);

        let app = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "App")
            .expect("App dir");
        assert!(app.is_directory());
        let kids = fs.list_directory(&app).unwrap();
        assert_eq!(kids.len(), 1);
        assert_eq!(kids[0].name, "Tool");
    }

    #[test]
    fn reads_both_forks_and_finder_info() {
        let mut fs = sample_fs();
        let root = fs.root().unwrap();
        let app = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "App")
            .unwrap();
        let tool = fs.list_directory(&app).unwrap().remove(0);

        assert_eq!(tool.type_code, Some(*b"APPL"));
        assert_eq!(tool.creator_code, Some(*b"Tool"));
        assert_eq!(tool.resource_fork_size, Some(4));

        let mut data = Vec::new();
        fs.write_file_to(&tool, &mut data).unwrap();
        assert_eq!(data, b"DATA");

        let mut rsrc = Vec::new();
        fs.write_resource_fork_to(&tool, &mut rsrc).unwrap();
        assert_eq!(rsrc, b"RSRC");
    }
}
