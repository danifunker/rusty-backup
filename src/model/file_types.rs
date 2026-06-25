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
    "GHS", "hfv", "HFV", "d88", "xdf", "hdm", "dim", "hds", "ima", "d64", "d71", "d81", "g64",
    "g71", "d80", "d82", "atr", "xfd", "jvc", "vdk", "ssd", "pdi", "bfs", "copydisk", "altodisk",
    "zdisk", "zdelta", "dsk80", "dsk300", "dsk44", "zip", "gz", "cbk",
];

/// Extensions that appear in the GUI file-picker dropdown (so a user can
/// browse to one) but are intentionally NOT registered as OS file
/// associations. `.zip` is the canonical case: Rusty Backup can open a
/// `.zip` holding a RAW disk image, but it must not become the system
/// handler for *every* `.zip` the user double-clicks. Filtered out of
/// [`association_exts`].
pub const NON_ASSOCIATED_EXTS: &[&str] = &["zip", "gz"];

/// Optical disc-image extensions (CD/DVD images), a distinct picker group.
pub const OPTICAL_EXTS: &[&str] = &["iso", "bin", "cue", "chd", "toast", "img"];

/// Macintosh archive / encoding extensions (StuffIt + Compact Pro + BinHex), a
/// picker group for the Archives tab. Includes uppercase variants for
/// case-sensitive pickers; new entries are lowercase-only (`cpt`).
pub const MAC_ARCHIVE_EXTS: &[&str] = &["sit", "hqx", "sea", "cpt", "mar", "SIT", "HQX", "SEA"];

/// Extra extensions shown in the Archives-tab Browse picker for MacBinary
/// files, *in addition to* [`MAC_ARCHIVE_EXTS`]. Deliberately kept out of
/// `MAC_ARCHIVE_EXTS` (which drives content-routing / the source-picker
/// redirect): `.bin` is overwhelmingly a raw disk/optical image, so it stays a
/// disk image by default and is only treated as a Mac archive when
/// `macarchive::detect::detect_mac_archive` confirms MacBinary on the actual
/// bytes. This list only widens the *picker filter* so a user can select a
/// `.bin` to inspect in the Archives tab; the content sniff is still the gate.
pub const MACBINARY_PICKER_EXTS: &[&str] = &["bin", "macbin"];

/// Extra extensions shown in the Archives-tab Browse picker for MacZip
/// archives. `.zip` is content-overloaded (a MacZip archive of Mac files vs. a
/// plain disk-image-in-a-zip — distinguished by `detect_mac_archive` finding a
/// `Mac3` extra field, not by extension), so like [`MACBINARY_PICKER_EXTS`] it
/// stays OUT of [`MAC_ARCHIVE_EXTS`] and [`DISK_IMAGE_EXTS`] keeps `.zip` as a
/// disk image by default.
pub const MACZIP_PICKER_EXTS: &[&str] = &["zip"];

/// Every extension the **Archives tab** Browse picker should surface, so a user
/// can select any archive format the engine knows how to open. This is the
/// single source of truth for that picker and is kept in parity with
/// `macarchive::detect::detect_mac_archive`'s coverage: the canonical Mac
/// archive set ([`MAC_ARCHIVE_EXTS`]) plus the content-overloaded extensions
/// ([`MACBINARY_PICKER_EXTS`], [`MACZIP_PICKER_EXTS`]) that can't live in the
/// routing list. Detection stays content-driven; this only controls which
/// files the dialog shows.
pub fn archives_picker_exts() -> Vec<&'static str> {
    let mut out: Vec<&'static str> = Vec::new();
    for ext in MAC_ARCHIVE_EXTS
        .iter()
        .chain(MACBINARY_PICKER_EXTS)
        .chain(MACZIP_PICKER_EXTS)
    {
        if !out.contains(ext) {
            out.push(ext);
        }
    }
    out
}

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
        if NON_ASSOCIATED_EXTS.contains(&lower.as_str()) {
            continue;
        }
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
    fn zip_is_picker_only_not_associated() {
        // `.zip` is openable (a RAW disk image inside a zip), so it must
        // appear in the picker dropdown — but Rusty Backup must NOT register
        // as the OS handler for every .zip the user double-clicks. So it
        // lives in DISK_IMAGE_EXTS yet is filtered out of association_exts().
        assert!(
            DISK_IMAGE_EXTS.contains(&"zip"),
            "zip must be in the picker list"
        );
        assert!(
            !association_exts().contains(&"zip".to_string()),
            "zip must NOT be registered as an OS file association"
        );
    }

    #[test]
    fn gz_is_picker_only_not_associated() {
        // `.gz` is openable (a gzip-wrapped disk image, e.g. a `.pdi.gz` Alto /
        // Pilot pack or a gzipped raw image), so it must appear in the picker
        // dropdown — but, exactly like `.zip`, Rusty Backup must NOT become the
        // OS handler for every `.gz` (which is overwhelmingly `.tar.gz` etc.).
        assert!(
            DISK_IMAGE_EXTS.contains(&"gz"),
            "gz must be in the picker list so .pdi.gz is selectable"
        );
        assert!(
            !association_exts().contains(&"gz".to_string()),
            "gz must NOT be registered as an OS file association"
        );
    }

    #[test]
    fn cbk_is_picker_and_associated() {
        // `.cbk` is the cb-dos backup container; the app opens it as a native
        // disk image (inspect/browse/restore), so it belongs in the picker AND,
        // unlike `.zip`/`.gz`, should own its file association (a `.cbk` is
        // always ours — there's no foreign `.cbk` to clobber).
        assert!(
            DISK_IMAGE_EXTS.contains(&"cbk"),
            "cbk must be in the picker list so a .cbk is selectable"
        );
        assert!(
            association_exts().contains(&"cbk".to_string()),
            "cbk should be registered as an OS file association"
        );
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
    fn alto_pack_extensions_present() {
        // Xerox Alto disk packs: the PARC Disk Image (`.pdi`) and the period
        // CopyDisk stream containers (`.bfs` / `.copydisk` / `.altodisk`), plus
        // the Dwarf Draco 6085 Pilot image (`.zdisk` / `.zdelta`). The
        // BrowseSession Alto branch opens all of them; pin them so the picker
        // keeps surfacing Alto / Pilot packs.
        for must in [
            "pdi", "bfs", "copydisk", "altodisk", "zdisk", "zdelta", "dsk300", "dsk80", "dsk44",
        ] {
            assert!(
                association_exts().contains(&must.to_string()),
                "missing Alto/Pilot pack extension {must}"
            );
        }
    }

    #[test]
    fn macbinary_picker_is_separate_from_routing_list() {
        // `.bin` must be selectable in the Archives picker but must NOT join
        // MAC_ARCHIVE_EXTS — otherwise every raw `.bin` disk image would be
        // routed to the Mac-archive path by extension. Content detection
        // (detect_mac_archive) is the real gate.
        assert!(MACBINARY_PICKER_EXTS.contains(&"bin"));
        assert!(
            !MAC_ARCHIVE_EXTS.contains(&"bin"),
            "bin must stay out of the content-routing list"
        );
        // And it stays a disk image by default.
        assert!(DISK_IMAGE_EXTS.contains(&"bin"));
    }

    #[test]
    fn maczip_picker_is_separate_from_routing_list() {
        // `.zip` is selectable in the Archives picker but must NOT join
        // MAC_ARCHIVE_EXTS (extension routing) — a plain disk-image-in-a-zip
        // would otherwise be force-routed to the Mac-archive path. It stays a
        // disk image by default; detect_mac_archive (Mac3 field) is the gate.
        assert!(MACZIP_PICKER_EXTS.contains(&"zip"));
        assert!(!MAC_ARCHIVE_EXTS.contains(&"zip"));
        assert!(DISK_IMAGE_EXTS.contains(&"zip"));
    }

    #[test]
    fn archives_picker_covers_every_supported_format() {
        // The Archives-tab Browse picker must surface every archive format the
        // engine can open (detect_mac_archive's coverage): StuffIt (.sit), SEA
        // (.sea), BinHex (.hqx), Compact Pro (.cpt), MAR (.mar), MacBinary
        // (.bin/.macbin), and MacZip (.zip). If a new format is added to the
        // classifier, add its picker extension here too.
        let picker = archives_picker_exts();
        for must in ["sit", "hqx", "sea", "cpt", "mar", "bin", "macbin", "zip"] {
            assert!(
                picker.contains(&must),
                "Archives picker is missing supported format .{must}"
            );
        }
        // Superset of the routing list, and de-duplicated.
        for ext in MAC_ARCHIVE_EXTS {
            assert!(picker.contains(ext), "picker dropped routing ext {ext}");
        }
        let mut sorted = picker.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), picker.len(), "picker has duplicates");
    }

    #[test]
    fn mac_archive_family_present() {
        // StuffIt (.sit/.sea), BinHex (.hqx), Compact Pro (.cpt), and MAR
        // (.mar) all flow through `macarchive` detection + the Archives tab.
        // Pin them so a picker-list cleanup can't silently drop a format.
        for must in ["sit", "hqx", "sea", "cpt", "mar"] {
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
        for must in ["d64", "d71", "d81", "g64", "g71", "d80", "d82"] {
            assert!(
                association_exts().contains(&must.to_string()),
                "missing CBM disk extension {must}"
            );
        }
    }

    #[test]
    fn atari_disk_family_present() {
        // Atari 8-bit ATR / XFD disk images (`src/fs/atari_dos.rs`) for the
        // Atari800 MiSTer core.
        for must in ["atr", "xfd"] {
            assert!(
                association_exts().contains(&must.to_string()),
                "missing Atari disk extension {must}"
            );
        }
    }

    #[test]
    fn acorn_dfs_family_present() {
        // Acorn DFS single-sided floppy images (`src/fs/dfs.rs`) for the
        // BBCMicro / AcornElectron MiSTer cores. `.ssd` is the flat
        // single-sided dump.
        assert!(
            association_exts().contains(&"ssd".to_string()),
            "missing Acorn DFS extension ssd"
        );
    }

    #[test]
    fn coco_disk_family_present() {
        // CoCo Disk BASIC (RS-DOS) disk images (`src/fs/rsdos.rs`) for the
        // CoCo2 / CoCo3 MiSTer cores. `.dsk` is the common raw dump; `.jvc`
        // and `.vdk` are CoCo-specific container extensions.
        for must in ["dsk", "jvc", "vdk"] {
            assert!(
                association_exts().contains(&must.to_string()),
                "missing CoCo disk extension {must}"
            );
        }
    }
}
