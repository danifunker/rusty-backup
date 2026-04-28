//! Filesystem-check and repair orchestration, decoupled from any GUI tab.
//!
//! Both functions take a path + partition descriptor, open the filesystem
//! through the standard factory, run the operation, and return the raw
//! result. Logging and UI state updates stay with the caller.
//!
//! Extracted from `gui/inspect_tab.rs` per §5 of `docs/codecleanup.md`.

use std::fs::{File, OpenOptions};
use std::io::BufReader;
use std::path::Path;

use anyhow::{Context, Result};

use crate::fs::{open_editable_filesystem, open_filesystem, FsckResult, RepairReport};

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
    let file =
        File::open(path).with_context(|| format!("failed to open {} for fsck", path.display()))?;
    let reader = BufReader::new(file);
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
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("failed to open {} for repair", path.display()))?;
    let mut efs = open_editable_filesystem(file, offset, ptype, type_string)
        .with_context(|| "failed to open editable filesystem")?;
    efs.repair().with_context(|| "repair failed")
}
