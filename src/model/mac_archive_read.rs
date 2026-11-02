//! Read a Mac archive (StuffIt, BinHex, SEA and their wrappings) off a volume: sniff, then decode.

use arc_mac::detect::{detect_mac_archive, MacArchiveKind};
use arc_mac::extract as mac_extract;
use arc_mac::stuffit::StuffItArchive;
use disk_core::entry::FileEntry;
use disk_core::filesystem::Filesystem;

/// How much of a file the sniff reads: every magic sits at or near the start.
pub const SNIFF_LIMIT: usize = 16 * 1024 * 1024;

/// What a decoded archive is for, which decides what is kept and how a failure reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveUse {
    /// The read-only archive viewer.
    Browse,
    /// Save the archive as it is stored, or decode and save it.
    Extract,
}

/// A Mac archive read off the volume and decoded.
pub struct ArchiveRead {
    pub kind: MacArchiveKind,
    /// The file as stored (kept for `Extract` only).
    pub raw: Vec<u8>,
    pub bytes: Vec<u8>,
    pub archive: StuffItArchive,
}

/// Whether the start of `entry` is a Mac archive; a failed read counts as "not one".
pub fn sniff(fs: &mut dyn Filesystem, entry: &FileEntry) -> Option<MacArchiveKind> {
    let bytes = fs.read_file(entry, SNIFF_LIMIT).ok()?;
    detect_mac_archive(&bytes)
}

/// Read `entry` whole, check it is a Mac archive, and decode it; the error is a status line.
pub fn read(
    fs: &mut dyn Filesystem,
    entry: &FileEntry,
    purpose: ArchiveUse,
) -> Result<ArchiveRead, String> {
    let raw = fs
        .read_file(entry, usize::MAX)
        .map_err(|e| format!("Read failed: {e}"))?;
    let kind = detect_mac_archive(&raw).ok_or_else(|| match purpose {
        ArchiveUse::Browse => format!("{} is not a recognized Mac archive.", entry.name),
        ArchiveUse::Extract => format!(
            "{} is not a recognized Mac archive (use Extract File to save raw bytes).",
            entry.name
        ),
    })?;
    let keep_raw = match purpose {
        ArchiveUse::Browse => Vec::new(),
        ArchiveUse::Extract => raw.clone(),
    };
    let (bytes, archive) =
        mac_extract::open_bytes(raw).map_err(|e| format!("{} decode failed: {e}", entry.name))?;
    Ok(ArchiveRead {
        kind,
        raw: keep_raw,
        bytes,
        archive,
    })
}
