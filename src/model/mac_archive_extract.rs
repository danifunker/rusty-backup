//! The Archives screen's model: open a classic Mac archive (StuffIt, SEA, BinHex), list its
//! entries as plain rows, spot a single-entry archive whose payload is a mountable disk image,
//! and extract all or some entries in a fork-preserving format.
//!
//! The decode and extraction are `arc_mac::extract`'s; the bodies here moved out of the egui
//! Archives tab and the TUI screen unchanged, so an extraction writes the same files.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arc_mac::detect::{detect_mountable_image, MountableImageKind};
use arc_mac::extract::{self, ExtractStats, ForkFormat};
use arc_mac::stuffit::{self, StuffItArchive};

/// One archive entry as the views list it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveEntryRow {
    pub is_dir: bool,
    pub path: String,
    pub type_code: String,
    pub creator: String,
    /// The data fork's length when it has one, else the resource fork's (0 for neither).
    pub size: u64,
    /// The compression method of the fork `size` counts ("None" for neither).
    pub method: &'static str,
    /// Each fork's length, when not empty.
    pub data_len: Option<u64>,
    pub rsrc_len: Option<u64>,
}

/// A single-entry archive's decoded data fork that is itself a disk image.
pub struct MountablePayload {
    pub kind: MountableImageKind,
    pub decoded: Vec<u8>,
    /// The entry's name, for the temporary file.
    pub name: String,
}

/// An opened archive: its bytes, the parsed directory and the rows drawn from it.
pub struct OpenedArchive {
    pub bytes: Arc<Vec<u8>>,
    pub archive: Arc<StuffItArchive>,
    pub rows: Vec<ArchiveEntryRow>,
    pub file_count: usize,
    pub mountable: Option<MountablePayload>,
}

/// Read and decode the archive at `path`.
pub fn open_archive(path: &Path) -> anyhow::Result<OpenedArchive> {
    let (bytes, archive) = extract::open(path)?;
    let rows = archive.entries.iter().map(entry_row).collect();
    let file_count = archive.entries.iter().filter(|e| !e.is_dir).count();
    let mountable = sniff_mountable_payload(bytes.as_slice(), &archive);
    Ok(OpenedArchive {
        bytes: Arc::new(bytes),
        archive: Arc::new(archive),
        rows,
        file_count,
        mountable,
    })
}

fn entry_row(e: &stuffit::StuffItEntry) -> ArchiveEntryRow {
    let (size, method) = e
        .data
        .as_ref()
        .filter(|f| f.uncompressed_len > 0)
        .or(e.rsrc.as_ref())
        .map(|f| (f.uncompressed_len as u64, f.method_name()))
        .unwrap_or((0, "None"));
    let non_empty = |f: Option<&stuffit::ForkInfo>| {
        f.filter(|f| f.uncompressed_len > 0)
            .map(|f| f.uncompressed_len as u64)
    };
    ArchiveEntryRow {
        is_dir: e.is_dir,
        path: e.display_path(),
        type_code: fs_mac::hfs::format_ostype(&e.type_code),
        creator: fs_mac::hfs::format_ostype(&e.creator_code),
        size,
        method,
        data_len: non_empty(e.data.as_ref()),
        rsrc_len: non_empty(e.rsrc.as_ref()),
    }
}

/// Exactly one non-folder entry whose data fork decodes to a DC42 / HFS / HFS+ image.
fn sniff_mountable_payload(bytes: &[u8], archive: &StuffItArchive) -> Option<MountablePayload> {
    let files: Vec<&_> = archive.entries.iter().filter(|e| !e.is_dir).collect();
    if files.len() != 1 {
        return None;
    }
    let entry = files[0];
    let fork = entry.data.as_ref()?;
    if fork.uncompressed_len == 0 {
        return None;
    }
    let decoded = stuffit::decompress_fork(bytes, fork).ok()?;
    let kind = detect_mountable_image(decoded.as_slice())?;
    Some(MountablePayload {
        kind,
        decoded,
        name: entry.name.clone(),
    })
}

/// Write a mountable payload to a new temporary folder; the guard keeps the file alive.
pub fn write_payload_to_tempdir(
    name: &str,
    bytes: &[u8],
) -> std::io::Result<(PathBuf, tempfile::TempDir)> {
    let dir = tempfile::Builder::new()
        .prefix("rusty-archive-mount-")
        .tempdir()?;
    let safe_name = if name.is_empty() {
        "payload.bin".to_string()
    } else {
        name.replace(['/', '\\'], "_")
    };
    let path = dir.path().join(safe_name);
    std::fs::write(&path, bytes)?;
    Ok((path, dir))
}

/// The number of files an extraction of `keep` (or all, for `None`) writes.
pub fn files_to_extract(archive: &StuffItArchive, keep: Option<&[bool]>) -> usize {
    archive
        .entries
        .iter()
        .enumerate()
        .filter(|(i, e)| {
            let kept = match keep {
                Some(k) => k.get(*i).copied().unwrap_or(true),
                None => true,
            };
            !e.is_dir && kept
        })
        .count()
}

/// Extract every entry, or only those `keep` ticks (folders always), into `dest`.
pub fn extract(
    bytes: &[u8],
    archive: &StuffItArchive,
    dest: &Path,
    format: ForkFormat,
    keep: Option<Vec<bool>>,
    progress: impl FnMut(usize, usize, &str),
    log: impl FnMut(String),
) -> anyhow::Result<ExtractStats> {
    match keep {
        Some(selected) => {
            let keep = move |i: usize| selected.get(i).copied().unwrap_or(true);
            extract::extract_filtered(bytes, archive, dest, format, keep, progress, log)
        }
        None => extract::extract_all(bytes, archive, dest, format, progress, log),
    }
}
