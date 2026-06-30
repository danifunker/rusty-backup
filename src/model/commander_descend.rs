//! Descending into a wrapper file from a Commander pane — the model-layer
//! logic behind "view the contents of a `.hqx` / `.sit` / nested disk image
//! without extracting it first."
//!
//! A pane row is *descendable* when it's a Mac archive or a disk-image
//! container. Activating it opens the wrapper's contents as a
//! [`Filesystem`](crate::fs::filesystem::Filesystem) and pushes a layer onto
//! the pane's [`DirListing`](crate::model::dir_listing::DirListing); copying out
//! then works through the existing generic copy path.
//!
//! This module is GUI-free: classification (cheap, by name), byte
//! materialization (so path-based openers work on bytes pulled from a parent
//! layer), and opening an archive or a chosen image partition. The pane
//! orchestrates and owns the temp-file lifetimes.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::fs::archive_fs::ArchiveFilesystem;
use crate::fs::entry::FileEntry;
use crate::fs::filesystem::Filesystem;
use crate::model::commander_source;
use crate::model::file_types::{DISK_IMAGE_EXTS, MAC_ARCHIVE_EXTS};
use crate::partition::PartitionInfo;

/// What kind of wrapper a descendable row is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescendKind {
    /// A classic-Mac archive (StuffIt / Compact Pro / MacBinary / BinHex).
    Archive,
    /// A disk-image container (DiskCopy 4.2, 2MG, raw, CHD, …).
    DiskImage,
}

/// Classify a filename as descendable, by extension only (no I/O). The actual
/// open confirms by content; this just decides whether to offer the affordance.
/// Mac-archive extensions win over disk-image ones (a `.sit.hqx` is an archive).
pub fn classify(name: &str) -> Option<DescendKind> {
    let ext = name.rsplit_once('.').map(|(_, e)| e)?;
    if MAC_ARCHIVE_EXTS.iter().any(|a| a.eq_ignore_ascii_case(ext))
        || name.to_ascii_lowercase().ends_with(".bin")
    {
        return Some(DescendKind::Archive);
    }
    if DISK_IMAGE_EXTS.iter().any(|a| a.eq_ignore_ascii_case(ext)) {
        return Some(DescendKind::DiskImage);
    }
    None
}

/// Classify a `FileEntry` as descendable. Tries the filename first, then falls
/// back to the Mac Finder type code — so a disk image with an unusual name
/// (e.g. `MacBottom MFS.image`, type `dImg`) is still recognized. Entries from a
/// Mac archive / HFS volume carry a type code; host files generally don't, so
/// they rely on the extension.
pub fn classify_entry(entry: &FileEntry) -> Option<DescendKind> {
    if let Some(kind) = classify(&entry.name) {
        return Some(kind);
    }
    if entry.type_code.is_some_and(|tc| is_disk_image_ostype(&tc)) {
        return Some(DescendKind::DiskImage);
    }
    None
}

/// True for a Mac OSType that denotes a disk image: `dImg`/`dimg` (Disk Copy),
/// and the NDIF read-only/read-write types.
fn is_disk_image_ostype(tc: &[u8; 4]) -> bool {
    matches!(
        tc,
        b"dImg" | b"dimg" | b"rohd" | b"rdxw" | b"rwhd" | b"rkhd" | b"DDim"
    )
}

/// Write `bytes` to a fresh temp file named `file_name`, returning the guard
/// (which must outlive any `Filesystem` opened on the path) and the path. Used
/// when the descendable file lives *inside* a parent layer (archive / image),
/// so its bytes must hit disk before a path-based opener can read it. Files on
/// the host filesystem skip this and open in place.
pub fn materialize(bytes: &[u8], file_name: &str) -> Result<(tempfile::TempDir, PathBuf)> {
    let dir = tempfile::Builder::new()
        .prefix("rb-descend-")
        .tempdir()
        .context("create descend temp dir")?;
    // Keep the original name so extension-based detection still works.
    let safe = sanitize_temp_name(file_name);
    let path = dir.path().join(safe);
    std::fs::write(&path, bytes).with_context(|| format!("write {}", path.display()))?;
    Ok((dir, path))
}

/// Reduce a name to something safe as a single temp-file component while
/// preserving the extension (which drives content routing).
fn sanitize_temp_name(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '\0' => '_',
            c => c,
        })
        .collect();
    if cleaned.is_empty() {
        "descend.bin".to_string()
    } else {
        cleaned
    }
}

/// Open an archive file (by path) as a read-only filesystem. `label` is the
/// wrapper's display name for the breadcrumb / volume label.
pub fn open_archive(path: &Path, label: Option<String>) -> Result<Box<dyn Filesystem>> {
    let fs = ArchiveFilesystem::open_path(path)
        .map_err(|e| anyhow::anyhow!("open archive {}: {e}", path.display()))?;
    let _ = label; // label currently taken from the path's file name inside open_path
    Ok(Box::new(fs))
}

/// The browsable partitions of a nested disk image at `path`, paired with their
/// index in the raw partition table (so the picker can label and re-resolve
/// them). Non-filesystem entries (driver / map / EFI partitions, extended
/// containers) are filtered out.
pub fn browsable_partitions(path: &Path) -> Result<Vec<(usize, PartitionInfo)>> {
    let parts = commander_source::probe_partitions(path)?;
    Ok(parts
        .into_iter()
        .enumerate()
        .filter(|(_, p)| {
            !p.is_extended_container
                && crate::fs::partition_is_browsable(
                    p.partition_type_byte,
                    p.partition_type_string.as_deref(),
                    &p.type_name,
                )
        })
        .collect())
}

/// Open one partition of a nested disk image as a read-only filesystem
/// (synchronous — nested images pulled out of an archive are small).
pub fn open_image_partition(path: &Path, part: &PartitionInfo) -> Result<Box<dyn Filesystem>> {
    commander_source::session_for(path, part)
        .open()
        .map_err(|e| anyhow::anyhow!("open partition {}: {e}", part.type_name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_routes_by_extension() {
        assert_eq!(classify("Game.sit"), Some(DescendKind::Archive));
        assert_eq!(classify("Game.sit.hqx"), Some(DescendKind::Archive));
        assert_eq!(classify("Tool.bin"), Some(DescendKind::Archive));
        assert_eq!(classify("Disk.dc42"), Some(DescendKind::DiskImage));
        assert_eq!(classify("Disk.2mg"), Some(DescendKind::DiskImage));
        assert_eq!(classify("notes.txt"), None);
        assert_eq!(classify("noext"), None);
    }

    #[test]
    fn classify_entry_uses_type_code_for_odd_names() {
        // A Disk Copy image with a non-image extension is recognized by its
        // Finder type code ('dImg'), not its name.
        let mut e = FileEntry::new_file("MacBottom MFS.image".into(), "/x".into(), 0, 0);
        assert_eq!(classify(&e.name), None);
        e.type_code = Some(*b"dImg");
        assert_eq!(classify_entry(&e), Some(DescendKind::DiskImage));
        // NDIF type code too.
        e.type_code = Some(*b"rohd");
        assert_eq!(classify_entry(&e), Some(DescendKind::DiskImage));
        // A plain text file stays non-descendable.
        e.type_code = Some(*b"TEXT");
        assert_eq!(classify_entry(&e), None);
    }

    #[test]
    fn materialize_then_open_archive_round_trips() {
        use crate::macarchive::stuffit::{
            build_archive_tree, StuffItInput, StuffItInputNode, WriteMethod,
        };
        let tree = vec![StuffItInputNode::File(StuffItInput {
            name: "hi.txt".into(),
            type_code: *b"TEXT",
            creator_code: *b"ttxt",
            finder_flags: 0,
            create_date: 0,
            mod_date: 0,
            data_fork: b"payload".to_vec(),
            resource_fork: Vec::new(),
        })];
        let bytes = build_archive_tree(&tree, WriteMethod::Store).unwrap();

        let (_guard, path) = materialize(&bytes, "inner.sit").unwrap();
        assert_eq!(path.extension().unwrap(), "sit");
        let mut fs = open_archive(&path, Some("inner.sit".into())).unwrap();
        let root = fs.root().unwrap();
        let kids = fs.list_directory(&root).unwrap();
        assert_eq!(kids.len(), 1);
        assert_eq!(kids[0].name, "hi.txt");
    }
}
