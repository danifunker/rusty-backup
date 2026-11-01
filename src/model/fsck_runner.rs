//! Filesystem-check and repair orchestration, decoupled from any GUI tab.
//!
//! Both functions take a path + partition descriptor, open the filesystem
//! through the standard factory, run the operation, and return the raw
//! result. Logging and UI state updates stay with the caller.
//!
//! Extracted from `gui/inspect_tab.rs` per §5 of `docs/codecleanup.md`.

use std::fs::OpenOptions;
use std::path::Path;

use anyhow::{anyhow, Context, Result};

use crate::fs::{open_editable_filesystem, open_filesystem, FsckResult, RepairReport};
use crate::model::source_reader::{is_chd_path, open_read};

/// Run `fsck` against the partition at `offset` inside `path`.
///
/// Returns `Ok(None)` when the underlying filesystem implementation does
/// not provide a checker (so the caller can surface "not supported" rather
/// than treating it as a hard error).
pub fn run_fsck(
    path: &Path,
    offset: u64,
    ptype: u8,
    type_string: Option<&str>,
) -> Result<Option<FsckResult>> {
    let reader = open_read(path)?;
    run_fsck_reader(reader, offset, ptype, type_string)
}

/// Run `fsck` against the partition at `offset` in a **pre-opened reader** (e.g.
/// a remote block reader). Read-only — same checker as [`run_fsck`], just
/// reader-based so a remote image can be checked over the wire.
pub fn run_fsck_reader<R: std::io::Read + std::io::Seek + Send + 'static>(
    reader: R,
    offset: u64,
    ptype: u8,
    type_string: Option<&str>,
) -> Result<Option<FsckResult>> {
    let mut fs = open_filesystem(reader, offset, ptype, type_string)
        .with_context(|| "failed to open filesystem")?;
    match fs.fsck() {
        Some(result) => result.map(Some).with_context(|| "filesystem check failed"),
        None => Ok(None),
    }
}

/// Run repair against the partition at `offset` inside `path`.
pub fn run_repair(
    path: &Path,
    offset: u64,
    ptype: u8,
    type_string: Option<&str>,
) -> Result<RepairReport> {
    if is_chd_path(path) {
        return Err(anyhow!(
            "repair is not supported for CHD-compressed sources (decompress to a raw image first)"
        ));
    }
    // Floppy / gzip / WOZ containers: repair a decoded temp flat, then
    // re-encode. Repairing the container's own bytes wrote into its framing.
    if crate::model::source_reader::is_editable_container_path(path) {
        let session = crate::model::container_edit::ContainerEditSession::open(path)
            .map_err(|e| anyhow!("opening container for repair: {e:#}"))?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(session.flat_path())
            .with_context(|| format!("failed to open {} for repair", path.display()))?;
        let report = run_repair_reader(file, offset, ptype, type_string)?;
        session
            .commit()
            .map_err(|e| anyhow!("re-encoding container after repair: {e:#}"))?;
        return Ok(report);
    }
    // Same classifier as the edit paths, so a volume that edits also repairs.
    match crate::model::source_reader::open_container_rw(path)? {
        crate::model::source_reader::ContainerRw::Plain => {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .with_context(|| format!("failed to open {} for repair", path.display()))?;
            run_repair_reader(file, offset, ptype, type_string)
        }
        crate::model::source_reader::ContainerRw::Handle(handle) => {
            run_repair_reader(handle, offset, ptype, type_string)
        }
        crate::model::source_reader::ContainerRw::ReadOnly(why) => {
            Err(anyhow!("{}: {why}", path.display()))
        }
    }
}

/// Run repair against the partition at `offset` in a **pre-opened read-write
/// reader** (e.g. a writable remote block reader). The reader-based sibling of
/// [`run_repair`], so a remote image can be repaired in place over the wire —
/// the daemon patches the bytes the repair writes back.
pub fn run_repair_reader<R: std::io::Read + std::io::Write + std::io::Seek + Send + 'static>(
    reader: R,
    offset: u64,
    ptype: u8,
    type_string: Option<&str>,
) -> Result<RepairReport> {
    let mut efs = open_editable_filesystem(reader, offset, ptype, type_string)
        .with_context(|| "failed to open editable filesystem")?;
    efs.repair().with_context(|| "repair failed")
}

/// Where a check or repair reads: a local path, or an image on a `rb-cli serve` daemon.
pub enum FsckSource {
    Path(std::path::PathBuf),
    #[cfg(feature = "remote")]
    Remote(
        std::sync::Arc<std::sync::Mutex<crate::remote::RemoteConnection>>,
        String,
    ),
}

/// A partition to check: where, at what offset, and of which type.
pub struct FsckTarget {
    pub source: FsckSource,
    pub offset: u64,
    pub ptype: u8,
    pub type_string: Option<String>,
}

/// What a check, or a repair followed by a re-check, produced.
pub struct FsckRun {
    /// Set when this run repaired before checking.
    pub repair: Option<RepairReport>,
    /// `Ok(None)`: this filesystem has no checker; `Err`: the check itself failed.
    pub check: std::result::Result<Option<FsckResult>, String>,
}

/// Check `target`, read-only.
pub fn check_target(target: &FsckTarget) -> Result<Option<FsckResult>> {
    let ts = target.type_string.as_deref();
    match &target.source {
        FsckSource::Path(path) => run_fsck(path, target.offset, target.ptype, ts),
        #[cfg(feature = "remote")]
        FsckSource::Remote(conn, path) => {
            let reader = crate::remote::RemoteBlockReader::open(std::sync::Arc::clone(conn), path)?;
            run_fsck_reader(reader, target.offset, target.ptype, ts)
        }
    }
}

/// Repair `target` in place, then check it again; a failed repair is an error, a failed re-check is not.
pub fn repair_and_recheck(target: &FsckTarget) -> Result<FsckRun> {
    let ts = target.type_string.as_deref();
    let report = match &target.source {
        FsckSource::Path(path) => run_repair(path, target.offset, target.ptype, ts)?,
        #[cfg(feature = "remote")]
        FsckSource::Remote(conn, path) => {
            let rw = crate::remote::RemoteBlockReader::open_rw(std::sync::Arc::clone(conn), path)?;
            run_repair_reader(rw, target.offset, target.ptype, ts)?
        }
    };
    Ok(FsckRun {
        repair: Some(report),
        check: check_target(target).map_err(|e| format!("{e:#}")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Read};

    fn fat16_image() -> tempfile::NamedTempFile {
        let compressed = std::fs::read("tests/fixtures/test_fat16.img.zst").unwrap();
        let mut raw = Vec::new();
        crate::rbformats::zstd_compat::decoder(Cursor::new(compressed))
            .and_then(|mut d| d.read_to_end(&mut raw))
            .unwrap();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), raw).unwrap();
        tmp
    }

    fn target(path: &Path) -> FsckTarget {
        FsckTarget {
            source: FsckSource::Path(path.to_path_buf()),
            offset: 0,
            ptype: 0x06,
            type_string: None,
        }
    }

    #[test]
    fn a_clean_fat16_checks_clean_and_rechecks_clean_after_repair() {
        let img = fat16_image();
        let checked = check_target(&target(img.path())).unwrap().unwrap();
        assert!(checked.is_clean(), "{:?}", checked.errors);
        let run = repair_and_recheck(&target(img.path())).unwrap();
        assert!(run.repair.is_some());
        assert!(run.check.unwrap().unwrap().is_clean());
    }

    #[test]
    fn a_missing_image_is_an_error_not_a_panic() {
        assert!(check_target(&target(Path::new("/nonexistent/disk.img"))).is_err());
        assert!(repair_and_recheck(&target(Path::new("/nonexistent/disk.img"))).is_err());
    }
}
