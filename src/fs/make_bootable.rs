//! Auto-detect what a Mac disk needs to become bootable, and apply only the
//! pieces it is missing.
//!
//! A classic-Mac volume boots when three things line up, and which of them a
//! given image needs depends on what *kind* of disk it is:
//!
//! 1. **A SCSI driver + Driver Descriptor Record (DDR)** — only a *full* (APM)
//!    disk needs this, so a Mac ROM can read the drive at all. A flat HFV has
//!    no partition table and emulators mount it directly, so it needs none.
//! 2. **Boot blocks** — the `'LK'` loader at the HFS volume's first sector
//!    (byte 0 of a flat HFV, the `Apple_HFS` partition start on a full disk).
//!    Never synthesized; copied from a donor that already boots that System.
//! 3. **A blessed System Folder** — the MDB `drFndrInfo[0]` pointing the ROM at
//!    the folder holding the System / Finder.
//!
//! [`assess_bootability`] classifies the disk and reports which of the three
//! are already in place; [`make_bootable`] applies only the missing ones
//! (idempotent — re-running a bootable disk is a no-op). The wrapper decision
//! is conservative: a flat HFV is **kept flat** (it boots in BasiliskII /
//! SheepShaver / Mini vMac as-is); turning a flat image into a full APM disk is
//! a deliberate, separate operation (the Expand/Export "APM disk" path), not
//! something this applies behind your back.

use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::fs::entry::FileEntry;
use crate::fs::filesystem::{Filesystem, FilesystemError};
use crate::fs::hfs_boot;
use crate::fs::mac_scsi_bless::{bless_apm_disk, BlessOptions, MacScsiDriver};
use crate::partition::PartitionTable;

/// Conventional name of the bootable folder on a classic-Mac volume.
const SYSTEM_FOLDER_NAME: &str = "System Folder";

/// What kind of bootable disk an image is — drives which components apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootDiskKind {
    /// Flat classic-HFS superfloppy (BasiliskII `.hfv` / infinite-mac `.dsk`):
    /// no partition table, the HFS volume is the whole file. Needs boot blocks
    /// + bless only — no driver/DDR (emulators mount it directly).
    FlatHfs,
    /// APM disk with an `Apple_HFS` partition (a "full disk" — an infinite-mac
    /// device image, or a Mac SCSI disk). Also needs a driver + DDR so the ROM
    /// can read it.
    ApmHfs,
}

impl BootDiskKind {
    /// The `(type_byte, type_string)` to open the HFS volume with.
    fn fs_dispatch(self) -> (u8, Option<&'static str>) {
        match self {
            // Superfloppy: auto-detect at the offset (the same way `put` /
            // `bless` open a flat `.dsk`).
            BootDiskKind::FlatHfs => (0, None),
            BootDiskKind::ApmHfs => (0, Some("Apple_HFS")),
        }
    }
}

/// The current bootability of a disk: what it is, and which of the three
/// components are already present.
#[derive(Debug, Clone)]
pub struct Assessment {
    pub kind: BootDiskKind,
    /// Byte offset of the HFS volume (0 for a flat HFV).
    pub hfs_offset: u64,
    /// Volume name, if it could be read.
    pub volume_name: Option<String>,
    /// APM disks: whether a SCSI driver + DDR is registered. `None` for a flat
    /// HFV (not applicable).
    pub has_driver: Option<bool>,
    /// Whether the HFS volume's first sector holds the `'LK'` boot loader.
    pub has_boot_blocks: bool,
    /// The currently-blessed System Folder name, if any.
    pub blessed_folder: Option<String>,
    /// A root folder that looks like the System Folder and could be blessed,
    /// when nothing is blessed yet.
    pub system_folder_candidate: Option<String>,
}

impl Assessment {
    /// Whether the disk is already fully bootable (nothing left to apply).
    pub fn is_bootable(&self) -> bool {
        self.has_driver.unwrap_or(true) && self.has_boot_blocks && self.blessed_folder.is_some()
    }
}

/// Where [`make_bootable`] sources the SCSI driver when an APM disk needs one.
#[derive(Debug, Clone, Default)]
pub enum DriverSource {
    /// The bundled known-good Apple SCSI driver (default).
    #[default]
    Builtin,
    /// Extract the driver verbatim from a donor Apple-formatted disk.
    Donor(PathBuf),
}

/// Inputs for [`make_bootable`].
#[derive(Debug, Clone, Default)]
pub struct MakeBootableOptions {
    /// A bootable donor disk to copy boot blocks from, used only if the target
    /// lacks them. Without one, missing boot blocks are reported as still
    /// missing (we never synthesize boot code).
    pub donor: Option<PathBuf>,
    /// Where to get the SCSI driver if an APM disk needs one.
    pub driver: DriverSource,
    /// Absolute Mac path of the folder to bless. `None` auto-blesses a root
    /// folder named "System Folder" when one exists.
    pub bless_path: Option<String>,
    /// Report the plan without writing anything.
    pub dry_run: bool,
}

/// What [`make_bootable`] did (or would do).
#[derive(Debug, Clone, Default)]
pub struct MakeBootableReport {
    /// Components changed (or, for a dry run, that would be changed).
    pub applied: Vec<String>,
    /// Components already in place and left untouched.
    pub skipped: Vec<String>,
    /// Components still missing because a required input wasn't provided.
    pub still_missing: Vec<String>,
    /// Whether the disk is bootable after the operation (for a dry run, this
    /// reflects the state before applying).
    pub now_bootable: bool,
}

/// Classify `path` and report which bootability components are present.
pub fn assess_bootability(path: &Path) -> Result<Assessment, FilesystemError> {
    let mut file = std::fs::File::open(path).map_err(FilesystemError::Io)?;
    let table = PartitionTable::detect(&mut file)
        .map_err(|e| FilesystemError::Parse(format!("detecting partition table: {e}")))?;

    let (kind, hfs_offset, has_driver) = match &table {
        PartitionTable::None { fs_hint, .. } if fs_hint.eq_ignore_ascii_case("HFS") => {
            (BootDiskKind::FlatHfs, 0u64, None)
        }
        PartitionTable::Apm(apm) => {
            let hfs = apm
                .entries
                .iter()
                .find(|e| e.partition_type.eq_ignore_ascii_case("Apple_HFS"))
                .ok_or_else(|| {
                    FilesystemError::Unsupported(
                        "APM disk has no Apple_HFS partition to make bootable".into(),
                    )
                })?;
            let block_size = apm.ddr.block_size.max(512) as u64;
            let offset = hfs.start_block as u64 * block_size;
            (
                BootDiskKind::ApmHfs,
                offset,
                Some(apm.ddr.driver_count >= 1),
            )
        }
        _ => {
            return Err(FilesystemError::Unsupported(
                "make-bootable supports flat classic-HFS images and APM disks with an \
                 Apple_HFS partition"
                    .into(),
            ))
        }
    };

    let has_boot_blocks = hfs_boot::has_boot_blocks(&mut file, hfs_offset)?;
    drop(file);

    // Open the HFS volume to read the current bless and look for a System
    // Folder to offer as a bless candidate.
    let (type_byte, type_string) = kind.fs_dispatch();
    let vol = std::fs::File::open(path).map_err(FilesystemError::Io)?;
    let mut fs = crate::fs::open_filesystem(vol, hfs_offset, type_byte, type_string)?;
    let volume_name = fs.volume_label().map(|s| s.to_string());
    let blessed_folder = fs.blessed_system_folder().map(|(_, name)| name);
    let system_folder_candidate = if blessed_folder.is_none() {
        find_system_folder(&mut *fs)
    } else {
        None
    };

    Ok(Assessment {
        kind,
        hfs_offset,
        volume_name,
        has_driver,
        has_boot_blocks,
        blessed_folder,
        system_folder_candidate,
    })
}

/// Apply whatever `path` is missing to become bootable, per `opts`. Idempotent.
pub fn make_bootable(
    path: &Path,
    opts: &MakeBootableOptions,
) -> Result<MakeBootableReport, FilesystemError> {
    let assessment = assess_bootability(path)?;
    let mut report = MakeBootableReport::default();

    if assessment.is_bootable() && opts.bless_path.is_none() {
        report.skipped.push("already bootable".into());
        report.now_bootable = true;
        return Ok(report);
    }

    // 1. Driver + DDR (APM full disks only).
    if assessment.kind == BootDiskKind::ApmHfs {
        if assessment.has_driver == Some(false) {
            if opts.dry_run {
                report
                    .applied
                    .push("install SCSI driver + DDR (APM)".into());
            } else {
                let driver = match &opts.driver {
                    DriverSource::Builtin => MacScsiDriver::builtin(),
                    DriverSource::Donor(p) => {
                        let mut f = std::fs::File::open(p).map_err(FilesystemError::Io)?;
                        MacScsiDriver::from_donor(&mut f)?
                    }
                };
                let mut f = open_rw(path)?;
                let len = f.metadata().map_err(FilesystemError::Io)?.len();
                let r = bless_apm_disk(&mut f, len, &driver, &BlessOptions::default())?;
                f.flush().map_err(FilesystemError::Io)?;
                report.applied.push(if r.was_already_present {
                    "refreshed SCSI driver + DDR".into()
                } else {
                    "installed SCSI driver + DDR".into()
                });
            }
        } else {
            report
                .skipped
                .push("SCSI driver + DDR already present".into());
        }
    }

    // 2. Boot blocks at the HFS volume's first sector.
    if assessment.has_boot_blocks {
        report.skipped.push("boot blocks already present".into());
    } else if let Some(donor) = &opts.donor {
        let donor_blocks = hfs_boot::read_donor_boot_blocks_from_image(donor)?;
        if opts.dry_run {
            report
                .applied
                .push(format!("copy boot blocks from {}", donor.display()));
        } else {
            let mut f = open_rw(path)?;
            f.seek(SeekFrom::Start(assessment.hfs_offset))?;
            f.write_all(donor_blocks.blocks.as_slice())?;
            f.flush().map_err(FilesystemError::Io)?;
            report
                .applied
                .push(format!("copied boot blocks from {}", donor.display()));
        }
    } else {
        report
            .still_missing
            .push("boot blocks (pass a bootable donor disk to copy them from)".into());
    }

    // 3. Blessed System Folder.
    if let Some(blessed) = &assessment.blessed_folder {
        if opts.bless_path.is_none() {
            report
                .skipped
                .push(format!("System Folder already blessed ({blessed})"));
        }
    }
    if assessment.blessed_folder.is_none() || opts.bless_path.is_some() {
        let target = opts.bless_path.clone().or_else(|| {
            assessment
                .system_folder_candidate
                .clone()
                .map(|n| format!("/{n}"))
        });
        match target {
            Some(tp) => {
                if opts.dry_run {
                    report.applied.push(format!("bless {tp}"));
                } else {
                    bless_folder_at(path, assessment.hfs_offset, assessment.kind, &tp)?;
                    report.applied.push(format!("blessed {tp}"));
                }
            }
            None => report.still_missing.push(
                "blessed System Folder (no 'System Folder' found at root; specify a folder)".into(),
            ),
        }
    }

    // Confirm the result by re-assessing (skip for a dry run, which wrote
    // nothing).
    report.now_bootable = if opts.dry_run {
        report.still_missing.is_empty()
    } else {
        assess_bootability(path)
            .map(|a| a.is_bootable())
            .unwrap_or(false)
    };

    Ok(report)
}

/// Open a raw image read+write.
fn open_rw(path: &Path) -> Result<std::fs::File, FilesystemError> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(FilesystemError::Io)
}

/// Find a root directory that looks like the System Folder.
fn find_system_folder(fs: &mut dyn Filesystem) -> Option<String> {
    let root = fs.root().ok()?;
    let kids = fs.list_directory(&root).ok()?;
    kids.into_iter()
        .find(|e| e.is_directory() && e.name.eq_ignore_ascii_case(SYSTEM_FOLDER_NAME))
        .map(|e| e.name)
}

/// Open the HFS volume at `offset` read+write, resolve the directory at `path`,
/// bless it, and sync.
fn bless_folder_at(
    path: &Path,
    offset: u64,
    kind: BootDiskKind,
    mac_path: &str,
) -> Result<(), FilesystemError> {
    let (type_byte, type_string) = kind.fs_dispatch();
    let f = open_rw(path)?;
    let mut fs = crate::fs::open_editable_filesystem(f, offset, type_byte, type_string)?;
    let entry = resolve_dir(&mut *fs, mac_path)?;
    if !entry.is_directory() {
        return Err(FilesystemError::InvalidData(format!(
            "{mac_path} is not a directory"
        )));
    }
    fs.set_blessed_folder(&entry)?;
    fs.sync_metadata()?;
    Ok(())
}

/// Walk from the root to the directory at `mac_path` (`/`-separated).
fn resolve_dir(fs: &mut dyn Filesystem, mac_path: &str) -> Result<FileEntry, FilesystemError> {
    let mut current = fs.root()?;
    for component in mac_path.trim_start_matches('/').split('/') {
        if component.is_empty() {
            continue;
        }
        let kids = fs.list_directory(&current)?;
        current = kids
            .into_iter()
            .find(|e| e.name == component)
            .ok_or_else(|| {
                FilesystemError::NotFound(format!("'{component}' not found resolving '{mac_path}'"))
            })?;
    }
    Ok(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::filesystem::{CreateDirectoryOptions, EditableFilesystem};
    use crate::fs::hfs::{create_blank_hfs, HfsFilesystem};
    use std::io::Cursor;

    const MIB: u64 = 1024 * 1024;

    /// Build a flat HFS image on disk with a "System Folder" at the root and
    /// no boot blocks / no bless. Returns the temp path.
    fn flat_hfs_with_system_folder(name: &str) -> PathBuf {
        let mut img = create_blank_hfs(8 * MIB, 4096, "Boot").unwrap();
        {
            let mut fs = HfsFilesystem::open(Cursor::new(&mut img), 0).unwrap();
            let root = fs.root().unwrap();
            fs.create_directory(&root, "System Folder", &CreateDirectoryOptions::default())
                .unwrap();
            fs.sync_metadata().unwrap();
        }
        let path = std::env::temp_dir().join(name);
        std::fs::write(&path, &img).unwrap();
        path
    }

    /// Build a 1024-byte boot region donor on disk (`'LK'`-stamped flat HFS).
    fn boot_donor(name: &str) -> PathBuf {
        let mut img = create_blank_hfs(8 * MIB, 4096, "Donor").unwrap();
        img[0] = 0x4C;
        img[1] = 0x4B;
        let path = std::env::temp_dir().join(name);
        std::fs::write(&path, &img).unwrap();
        path
    }

    #[test]
    fn assess_flat_hfs_reports_missing_pieces() {
        let path = flat_hfs_with_system_folder("rb_mb_assess.hfv");
        let a = assess_bootability(&path).unwrap();
        assert_eq!(a.kind, BootDiskKind::FlatHfs);
        assert_eq!(a.has_driver, None); // not applicable to a flat HFV
        assert!(!a.has_boot_blocks);
        assert!(a.blessed_folder.is_none());
        assert_eq!(a.system_folder_candidate.as_deref(), Some("System Folder"));
        assert!(!a.is_bootable());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn make_bootable_flat_applies_boot_blocks_and_bless() {
        let path = flat_hfs_with_system_folder("rb_mb_apply.hfv");
        let donor = boot_donor("rb_mb_donor.hfv");

        let opts = MakeBootableOptions {
            donor: Some(donor.clone()),
            ..Default::default()
        };
        let report = make_bootable(&path, &opts).unwrap();
        assert!(report.now_bootable, "report: {report:?}");
        assert!(report.still_missing.is_empty(), "report: {report:?}");

        // Verify on disk: LK at sector 0 and a blessed folder.
        let a = assess_bootability(&path).unwrap();
        assert!(a.has_boot_blocks);
        assert_eq!(a.blessed_folder.as_deref(), Some("System Folder"));
        assert!(a.is_bootable());

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&donor);
    }

    #[test]
    fn make_bootable_flat_without_donor_reports_missing_boot_blocks() {
        let path = flat_hfs_with_system_folder("rb_mb_nodonor.hfv");
        let report = make_bootable(&path, &MakeBootableOptions::default()).unwrap();
        // Bless happened (System Folder found), but boot blocks are still
        // missing because no donor was supplied.
        assert!(report.applied.iter().any(|s| s.contains("bless")));
        assert!(report
            .still_missing
            .iter()
            .any(|s| s.contains("boot blocks")));
        assert!(!report.now_bootable);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn make_bootable_is_idempotent() {
        let path = flat_hfs_with_system_folder("rb_mb_idem.hfv");
        let donor = boot_donor("rb_mb_idem_donor.hfv");
        let opts = MakeBootableOptions {
            donor: Some(donor.clone()),
            ..Default::default()
        };
        make_bootable(&path, &opts).unwrap();
        // Second run: nothing to do.
        let report = make_bootable(&path, &opts).unwrap();
        assert!(report.now_bootable);
        assert!(report.applied.is_empty(), "second run applied: {report:?}");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&donor);
    }
}
