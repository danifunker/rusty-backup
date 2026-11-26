//! Read a Mac archive (StuffIt, BinHex, SEA and their wrappings) off a volume: sniff, then decode.

use arc_mac::detect::{detect_mac_archive, MacArchiveKind};
use arc_mac::extract as mac_extract;
use arc_mac::stuffit::StuffItArchive;
use disk_core::entry::FileEntry;
use disk_core::filesystem::Filesystem;

/// How much of a file the sniff reads: every magic sits at or near the start.
pub const SNIFF_LIMIT: usize = 16 * 1024 * 1024;

/// The largest Mac archive decoded: the decoder holds the archive and its output in memory.
pub const DECODE_MAX: u64 = 512 * 1024 * 1024;

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

/// Largest file a content sniff hands to the Archives tab, which reads the whole file.
pub const OPEN_AS_ARCHIVE_LIMIT: u64 = 1 << 30;

/// Whether the host file at `path` *is* a Mac archive (MacBinary, StuffIt, BinHex, ...),
/// judged by content whatever its extension. Only kinds anchored at its start count, so a
/// disk image merely holding an archive somewhere is not taken for one (issue #127).
pub fn is_archive_file(path: &std::path::Path) -> bool {
    use std::io::Read;
    let Ok(file) = std::fs::File::open(path) else {
        return false;
    };
    match file.metadata() {
        Ok(m) if m.is_file() && m.len() <= OPEN_AS_ARCHIVE_LIMIT => {}
        _ => return false,
    }
    let mut head = Vec::new();
    if file
        .take(SNIFF_LIMIT as u64)
        .read_to_end(&mut head)
        .is_err()
    {
        return false;
    }
    match detect_mac_archive(&head) {
        // The BinHex banner and a SEA's signature are found by scanning, so they must lead.
        Some(
            MacArchiveKind::BinHexSingleFile
            | MacArchiveKind::BinHexOverSit
            | MacArchiveKind::BinHexOverSea
            | MacArchiveKind::BinHexOverCompactPro,
        ) => {
            let banner = b"(This file must be converted with BinHex";
            head[..head.len().min(4096)]
                .windows(banner.len())
                .any(|w| w == banner)
        }
        Some(MacArchiveKind::Sea) | None => false,
        Some(_) => true,
    }
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
    if entry.size > DECODE_MAX {
        return Err(format!(
            "{} is larger than the {} MiB a Mac archive is decoded in.",
            entry.name,
            DECODE_MAX >> 20
        ));
    }
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
