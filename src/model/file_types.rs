//! Single source of truth for the disk-image file extensions Rusty Backup
//! recognizes.
//!
//! Both the GUI file-dialog filters and the OS file-association registration
//! consume these lists, so adding support for a new container format means
//! editing exactly one place. Previously each `add_filter` call site carried
//! its own hand-maintained list and they had drifted (the restore tab was
//! missing `hda`/`hdv`/`dmg`/`chd` that inspect/backup listed).
//!
//! On Windows the association registrar (see `os::windows`) reads
//! [`association_exts`] at runtime, so a self-update that extends this list
//! re-registers the new extensions on next launch without a reinstaller.
//! On macOS/Linux the equivalent metadata (`Info.plist` `CFBundleDocumentTypes`,
//! `.desktop` `MimeType=`) is generated from these same lists at build time.

/// Disk-image extensions for rfd file pickers — the canonical set plus
/// uppercase variants of the case-sensitive container formats (rfd matches
/// extensions case-sensitively on some platforms, so `.GHO` / `.HFV` need
/// explicit entries to be selectable).
pub const DISK_IMAGE_EXTS: &[&str] = &[
    "vhd", "img", "raw", "bin", "iso", "dd", "hda", "hdv", "2mg", "dmg", "po", "do", "dsk", "dc42",
    "woz", "chd", "adf", "hdf", "adz", "hdz", "imz", "vmdk", "qcow2", "qcow", "gho", "ghs", "GHO",
    "GHS", "hfv", "HFV", "d88", "xdf", "hdm", "dim", "hds", "ima", "d64", "d71", "d81",
];

/// Optical disc-image extensions (CD/DVD images), a distinct picker group.
pub const OPTICAL_EXTS: &[&str] = &["iso", "bin", "cue", "chd", "toast", "img"];

/// Macintosh archive / encoding extensions (StuffIt + Compact Pro + BinHex), a
/// picker group for the Archives tab. Includes uppercase variants for
/// case-sensitive pickers; new entries are lowercase-only (`cpt`).
pub const MAC_ARCHIVE_EXTS: &[&str] = &["sit", "hqx", "sea", "cpt", "SIT", "HQX", "SEA"];

/// ProgId registered under `HKCU\Software\Classes` for disk-image associations.
pub const DISK_IMAGE_PROGID: &str = "RustyBackup.DiskImage";

/// Friendly description shown in Explorer and the "Open with" list.
pub const DISK_IMAGE_PROGID_DESC: &str = "Rusty Backup Disk Image";

/// Lowercased, de-duplicated extensions for OS file-association registration.
/// Registry keys are case-insensitive, so the uppercase picker variants
/// collapse away here.
pub fn association_exts() -> Vec<String> {
    let mut out: Vec<String> = Vec::with_capacity(DISK_IMAGE_EXTS.len());
    for ext in DISK_IMAGE_EXTS {
        let lower = ext.to_ascii_lowercase();
        if !out.contains(&lower) {
            out.push(lower);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn association_exts_are_lowercase_and_unique() {
        let exts = association_exts();
        // Uppercase duplicates collapse: GHO/gho -> gho, etc.
        assert!(exts.contains(&"gho".to_string()));
        assert!(exts.contains(&"hfv".to_string()));
        assert!(!exts
            .iter()
            .any(|e| e.chars().any(|c| c.is_ascii_uppercase())));
        // No dupes.
        let mut sorted = exts.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), exts.len());
    }

    #[test]
    fn common_formats_present() {
        for must in ["img", "raw", "vhd", "chd", "adf", "hdf", "dmg", "hda"] {
            assert!(
                association_exts().contains(&must.to_string()),
                "missing {must}"
            );
        }
    }

    #[test]
    fn x68000_hdd_extensions_present() {
        // Sharp X68000 SASI/SCSI HDD images — `.hda` (BlueSCSI),
        // `.hdf`, and `.hds`. The Human68k engine opens all three; pin
        // them so the picker keeps surfacing X68000 hard-disk images.
        for must in ["hda", "hdf", "hds"] {
            assert!(
                association_exts().contains(&must.to_string()),
                "missing X68000 HDD extension {must}"
            );
        }
    }

    #[test]
    fn mac_archive_family_present() {
        // StuffIt (.sit/.sea), BinHex (.hqx), and Compact Pro (.cpt) all flow
        // through `macarchive` detection + the Archives tab. Pin them so a
        // picker-list cleanup can't silently drop a supported archive format.
        for must in ["sit", "hqx", "sea", "cpt"] {
            assert!(
                MAC_ARCHIVE_EXTS.contains(&must),
                "missing Mac archive extension {must}"
            );
        }
    }

    #[test]
    fn floppy_container_family_present() {
        // X68000 / PC-98 / FM-7 floppy containers — engine support
        // (`rbformats::containers::{d88,xdf,hdm,dim}`) and the GUI
        // `Convert Floppy Container...` dialog all assume the pickers
        // surface these. Regression-pin them here so a future cleanup
        // pass that prunes the disk-image list has to do it deliberately.
        for must in ["d88", "xdf", "hdm", "dim"] {
            assert!(
                association_exts().contains(&must.to_string()),
                "missing floppy-container extension {must}"
            );
        }
    }

    #[test]
    fn cbm_disk_family_present() {
        // Commodore CBM DOS floppy images (`src/fs/cbm.rs`) for the
        // C64/C128/C16/VIC-20/PET MiSTer cores. Pin the picker extensions
        // so a future cleanup of the disk-image list can't silently drop
        // them and break double-click open / the file picker filter.
        for must in ["d64", "d71", "d81"] {
            assert!(
                association_exts().contains(&must.to_string()),
                "missing CBM disk extension {must}"
            );
        }
    }
}
