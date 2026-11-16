//! Resize Image as an `rb-cli serve` daemon runs it, for a client resizing a remote image. The
//! daemon reads the partition table from its own copy of the image, checks it against what the
//! client saw (`WireResizeJob::expect`), and runs the same engine the local wizard does.

use std::path::Path;

use anyhow::{bail, Result};

use super::{open_source, ResizeImageRequest};
use crate::partition::{detect_alignment, PartitionInfo, PartitionTable};
use crate::remote::protocol::WireResizeJob;

/// The image's table, its partitions and the alignment the resize keeps, read on the daemon.
pub fn layout_of(path: &Path) -> Result<(PartitionTable, Vec<PartitionInfo>, u64)> {
    let mut reader = open_source(path)?;
    let table = PartitionTable::detect(&mut reader)?;
    let partitions = table.partitions();
    let alignment = detect_alignment(&table).alignment_sectors;
    Ok((table, partitions, alignment))
}

/// The partitions with these indices, in the order asked.
pub fn partitions(path: &Path, indices: &[usize]) -> Result<Vec<PartitionInfo>> {
    let (_, all, _) = layout_of(path)?;
    indices
        .iter()
        .map(|i| match all.iter().find(|p| p.index == *i) {
            Some(p) => Ok(p.clone()),
            None => bail!("the image has no partition {}", i + 1),
        })
        .collect()
}

/// What the client saw of each partition, sent so the daemon can refuse a changed image.
pub fn expect_of(partitions: &[PartitionInfo]) -> Vec<(usize, u64, u64)> {
    let parts = partitions.iter();
    parts
        .map(|p| (p.index, p.start_lba, p.size_bytes))
        .collect()
}

/// The request the local wizard would build, from the daemon's own reading of the image.
pub fn request(path: &Path, job: &WireResizeJob) -> Result<ResizeImageRequest> {
    let (table, partitions, alignment_sectors) = layout_of(path)?;
    if expect_of(&partitions) != job.expect {
        bail!("the image's partitions changed since it was inspected; re-inspect it and try again");
    }
    Ok(ResizeImageRequest {
        source: path.to_path_buf(),
        copy_to: None,
        table,
        partitions,
        alignment_sectors,
        disk_size: job.disk_size,
        sizes: job.sizes.clone(),
        check_after: job.check_after,
        rebuild: job.rebuild.clone(),
    })
}
