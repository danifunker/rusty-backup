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
