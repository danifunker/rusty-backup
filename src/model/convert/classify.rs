//! Tell a hard disk from a floppy from a disc: by its contents first, its extension second.

use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use super::ImageKind;

/// The largest image taken for a floppy: a 2.88 MB disk.
pub const FLOPPY_MAX: u64 = 2_949_120;

/// How much of the file classification reads.
const HEAD: usize = 0x9400;

/// The container a disc image is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscSource {
    Iso,
    /// A cue sheet and its .bin files.
    Cue,
    CdChd,
    DvdChd,
}

impl DiscSource {
    pub const fn label(self) -> &'static str {
        match self {
            DiscSource::Iso => "ISO",
            DiscSource::Cue => "BIN/CUE",
            DiscSource::CdChd => "CD CHD",
            DiscSource::DvdChd => "DVD CHD",
        }
    }
}

/// What a file was found to be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Classified {
    pub path: PathBuf,
    pub kind: ImageKind,
    /// The kind its extension suggests, when the extension says.
    pub by_extension: Option<ImageKind>,
    /// The disk's size once any container is peeled; a disc's file size.
    pub size: u64,
    pub disc: Option<DiscSource>,
    /// What was read, for Review: "ISO 9660", "WOZ", "raw 1.44 MB".
    pub detail: String,
}

impl Classified {
    /// The contents and the extension disagree.
    pub fn mismatch(&self) -> bool {
        self.by_extension.is_some_and(|k| k != self.kind)
    }

    /// Review's note when they disagree.
    pub fn mismatch_note(&self) -> Option<String> {
        let ext_kind = self.by_extension.filter(|_| self.mismatch())?;
        Some(format!(
            "Looks like a {}; its extension says {}.",
            self.kind.label().to_ascii_lowercase(),
            ext_kind.label().to_ascii_lowercase()
        ))
    }
}

/// The kind a file's extension suggests; `None` for ones used for more than one kind.
pub fn kind_of_extension(path: &Path) -> Option<ImageKind> {
    let ext = path.extension()?.to_str()?.to_ascii_lowercase();
    Some(match ext.as_str() {
        "iso" | "cue" | "toast" | "cdr" | "nrg" | "gdi" | "ccd" | "mds" | "cso" => ImageKind::Disc,
        "adf" | "adz" | "st" | "msa" | "d64" | "d71" | "d81" | "g64" | "woz" | "moof" | "dc42"
        | "xdf" | "hdm" | "dim" | "d88" | "d77" | "fdi" | "atr" | "dsd" | "edsk" | "ima"
        | "fdd" | "do" | "po" | "trd" | "ssd" | "dsk" => ImageKind::Floppy,
        "vhd" | "vhdx" | "vmdk" | "qcow2" | "qcow" | "hdf" | "hdz" | "hfv" | "hda" | "hdd"
        | "hds" | "gho" | "vdi" | "dmg" => ImageKind::HardDisk,
        _ => return None,
    })
}

/// Read `path` and say what it is.
pub fn classify(path: &Path) -> std::io::Result<Classified> {
    let file_size = std::fs::metadata(path)?.len();
    let gz = crate::model::source_reader::is_gzip_wrapped_path(path);
    let head = if gz {
        crate::model::source_reader::gunzip_prefix(path, HEAD)?
    } else {
        crate::compat::read_head(path, HEAD)?
    };
    let by_extension = kind_of_extension(path);
    let mut found = Classified {
        path: path.to_path_buf(),
        kind: ImageKind::HardDisk,
        by_extension,
        size: file_size,
        disc: None,
        detail: String::new(),
    };
    if let Some((disc, detail)) = disc_of(path, head.as_slice()) {
        found.kind = ImageKind::Disc;
        found.disc = Some(disc);
        found.detail = detail.to_string();
        return Ok(found);
    }
    let (size, container) = if gz {
        (gzip_size(path).unwrap_or(file_size), "gzip")
    } else {
        decoded_size(path, file_size)
    };
    found.size = size;
    let floppy = (64 * 1024..=FLOPPY_MAX).contains(&size);
    found.kind = if floppy {
        ImageKind::Floppy
    } else {
        ImageKind::HardDisk
    };
    found.detail = if container.is_empty() {
        format!("raw, {}", size_text(size))
    } else {
        format!("{container}, {}", size_text(size))
    };
    Ok(found)
}

/// A disc's container and what showed it, from the file's first bytes.
fn disc_of(path: &Path, head: &[u8]) -> Option<(DiscSource, &'static str)> {
    if head.starts_with(b"MComprHD") {
        return match chd_kind(path) {
            ChdKind::Cd => Some((DiscSource::CdChd, "CD CHD")),
            ChdKind::Dvd => Some((DiscSource::DvdChd, "DVD CHD")),
            ChdKind::Other => None,
        };
    }
    if is_cue_sheet(path, head) {
        return Some((DiscSource::Cue, "cue sheet"));
    }
    let at = |off: usize, magic: &[u8]| head.get(off..off + magic.len()) == Some(magic);
    if at(0x8001, b"CD001") || at(0x8001, b"BEA01") {
        return Some((DiscSource::Iso, "ISO 9660"));
    }
    if at(0x8009, b"CDROM") {
        return Some((DiscSource::Iso, "High Sierra"));
    }
    None
}

fn is_cue_sheet(path: &Path, head: &[u8]) -> bool {
    let ext_cue = path
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("cue"));
    let text = head.len() < 64 * 1024
        && head
            .iter()
            .all(|&b| b == b'\t' || b == b'\r' || b == b'\n' || (0x20..0x7f).contains(&b));
    let upper = String::from_utf8_lossy(head).to_ascii_uppercase();
    text && (ext_cue || (upper.contains("FILE ") && upper.contains("TRACK ")))
}

#[cfg_attr(not(feature = "chd"), allow(dead_code))]
enum ChdKind {
    Cd,
    Dvd,
    Other,
}

fn chd_kind(path: &Path) -> ChdKind {
    #[cfg(feature = "chd")]
    {
        match crate::rbformats::chd::chd_media(path) {
            Ok(crate::rbformats::chd::ChdMedia::Cd) => ChdKind::Cd,
            Ok(crate::rbformats::chd::ChdMedia::Dvd) => ChdKind::Dvd,
            _ => ChdKind::Other,
        }
    }
    #[cfg(not(feature = "chd"))]
    {
        let _ = path;
        ChdKind::Other
    }
}

/// The disk's size once its container is peeled, and the container's name ("" for a raw image).
fn decoded_size(path: &Path, file_size: u64) -> (u64, &'static str) {
    let container = container_name(path);
    if container.is_empty() {
        return (file_size, "");
    }
    let size = super::open_disk(path).map(|(_, n)| n).unwrap_or(file_size);
    (size, container)
}

fn container_name(path: &Path) -> &'static str {
    use crate::model::source_reader as sr;
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if sr::is_woz_path(path) {
        "WOZ"
    } else if sr::is_moof_path(path) {
        "MOOF"
    } else if sr::is_chd_path(path) {
        "CHD"
    } else if sr::is_flat_floppy_container_path(path) {
        "floppy container"
    } else {
        match ext.as_str() {
            "vhd" | "vhdx" => "VHD",
            "vmdk" => "VMDK",
            "qcow2" | "qcow" => "QCOW2",
            "2mg" => "2MG",
            "dc42" | "image" => "DiskCopy 4.2",
            "dsk" if is_dc42(path) => "DiskCopy 4.2",
            "gho" | "ghs" => "Ghost",
            "imz" => "IMZ",
            "zip" => "zip",
            "dmg" => "DMG",
            _ => "",
        }
    }
}

/// DiskCopy 4.2: a Pascal name of at most 63 bytes and the 0x0100 magic at 0x52.
fn is_dc42(path: &Path) -> bool {
    let Ok(head) = crate::compat::read_head(path, 0x54) else {
        return false;
    };
    head.len() >= 0x54 && head[0] <= 63 && head[0x52] == 0x01 && head[0x53] == 0x00
}

/// A gzip stream's size from its trailer (modulo 4 GiB, which no floppy reaches).
fn gzip_size(path: &Path) -> Option<u64> {
    let mut f = std::fs::File::open(path).ok()?;
    f.seek(SeekFrom::End(-4)).ok()?;
    let mut b = [0u8; 4];
    f.read_exact(&mut b).ok()?;
    Some(u32::from_le_bytes(b) as u64)
}

/// "720 KB", "1.44 MB", "2.0 GB".
pub fn size_text(bytes: u64) -> String {
    const KB: u64 = 1024;
    if let Some(s) = super::refit::SIZES.iter().find(|s| s.bytes == bytes) {
        return s.label.to_string();
    }
    if bytes >= KB * KB * KB {
        format!("{:.1} GB", bytes as f64 / (KB * KB * KB) as f64)
    } else if bytes >= KB * KB {
        format!("{:.1} MB", bytes as f64 / (KB * KB) as f64)
    } else {
        format!("{} KB", bytes / KB)
    }
}
