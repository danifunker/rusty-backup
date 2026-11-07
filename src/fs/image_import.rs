//! Copy the files out of a disk image found in a host tree INTO an open filesystem: what
//! `--expand-images` means for a `.dsk`, `.img`, DiskCopy, `.dmg` or `.hfv` on its way onto
//! a volume.
//!
//! The image opens through the same code Commander uses to descend into one
//! ([`crate::model::commander_descend`]), so every container and filesystem the browser can
//! read can be expanded, forks and Finder info included. A single-volume image lands in one
//! folder named after the image; one with several browsable partitions gets a subfolder per
//! partition, named by its volume label. An image with nothing browsable is left for the
//! caller to copy in as a file.

use std::path::Path;

use anyhow::Result;
use disk_core::filesystem::{EditableFilesystem, Filesystem};
use fs_tar::import_sink::{ImportOptions, ImportStats, Importer};

use super::mac_archive_import::{import_filesystem_into, measure_filesystem};
use crate::model::commander_descend::{
    browsable_partitions, classify, open_image_partition, DescendKind,
};
use crate::partition::PartitionInfo;

/// True when the file name is a disk-image container the browser descends into (no I/O).
pub fn looks_like_disk_image(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .and_then(classify)
        .is_some_and(|k| k == DescendKind::DiskImage)
}

/// The browsable volumes in the image, each with the subfolder it lands in (`None` when the
/// image has only one, which lands in the image's own folder).
pub(crate) fn volumes(path: &Path) -> Vec<(Option<String>, PartitionInfo)> {
    let parts = match browsable_partitions(path) {
        Ok(p) => p,
        Err(e) => {
            log::warn!("{}: not expanded, no volume found: {e:#}", path.display());
            return Vec::new();
        }
    };
    let many = parts.len() > 1;
    parts
        .into_iter()
        .map(|(i, p)| {
            let name = many.then(|| subfolder_name(path, i, &p));
            (name, p)
        })
        .collect()
}

/// A partition's subfolder: its volume label, else `Partition N`.
fn subfolder_name(path: &Path, index: usize, part: &PartitionInfo) -> String {
    let label = open_image_partition(path, part)
        .ok()
        .and_then(|fs| fs.volume_label().map(|l| l.trim().to_string()))
        .filter(|l| !l.is_empty() && !l.contains('/'));
    label.unwrap_or_else(|| format!("Partition {}", index + 1))
}

/// `(files, dirs, bytes)` the image expands to; `None` when it has no browsable volume.
pub fn measure_image_expanded(path: &Path) -> Result<Option<(u64, u64, u64)>> {
    let vols = volumes(path);
    if vols.is_empty() {
        return Ok(None);
    }
    let mut totals = (0u64, 0u64, 0u64);
    for (sub, part) in &vols {
        let mut fs = open_image_partition(path, part)?;
        let (f, d, b) = measure_filesystem(&mut *fs)?;
        totals.0 += f;
        totals.1 += d + u64::from(sub.is_some());
        totals.2 += b;
    }
    Ok(Some(totals))
}

/// Copy every volume of the image under `dir` (already created by the caller), one subfolder
/// per volume when there are several.
pub(crate) fn import_image_into(
    efs: &mut dyn EditableFilesystem,
    sink: &mut Importer,
    dir_comps: &[String],
    path: &Path,
    vols: &[(Option<String>, PartitionInfo)],
    opts: &ImportOptions,
    progress: &dyn Fn(&ImportStats),
) -> Result<ImportStats> {
    let mut total = ImportStats::default();
    let what = path.display().to_string();
    for (sub, part) in vols {
        let mut comps = dir_comps.to_vec();
        if let Some(name) = sub {
            comps.push(name.clone());
        }
        let Some(dest) = sink.ensure_dir_at(efs, comps.as_slice(), None)? else {
            log::warn!("{what}: skipped a volume, its folder name is not valid here");
            continue;
        };
        let mut src: Box<dyn Filesystem> = open_image_partition(path, part)?;
        let base = total.clone();
        let report = |s: &ImportStats| {
            let mut merged = base.clone();
            merged.merge(s);
            progress(&merged);
        };
        let stats = import_filesystem_into(&mut *src, what.as_str(), efs, &dest, opts, &report)?;
        total.merge(&stats);
    }
    Ok(total)
}
