//! Testable core for resizing a filesystem on a **remote image** in place over
//! the block tier.
//!
//! The local `rb-cli resize` verb opens a read-write file, resolves the
//! `IMG@N` partition, and calls [`crate::fs::resize_filesystem_for`] — which
//! patches the FS metadata *within* its existing partition (it never changes the
//! partition table or the image's byte length). That FS-only, never-grows-the-
//! file shape is exactly what the block tier supports: a read-write
//! [`RemoteBlockReader`] patches byte ranges over the wire, the daemon syncs on
//! flush/close. So a remote image resizes with the same engine and the same
//! `@N` semantics as a local one — every read/write is just a ranged request.
//!
//! Partition-table resize (moving partitions, growing/truncating the image) is
//! a different operation (`partition::apply_resize`) and is NOT supported over
//! the wire — it would have to grow the image file, which the block tier can't.
//!
//! Kept here (not in the GUI) so it can be exercised headlessly over a loopback
//! `serve_on`; the CLI `resize` verb is a thin shell over this.

use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};

use crate::cli::resolve::resolve_partition_in_reader;
use crate::fs::resize_filesystem_for;
use crate::remote::{RemoteBlockReader, RemoteConnection};

/// What a remote resize touched, for the caller's log line.
pub struct RemoteResizeOutcome {
    /// The human-readable partition label that was resized.
    pub label: String,
    /// Byte offset of the resized filesystem inside the image.
    pub partition_offset: u64,
    /// The partition's capacity in bytes (the FS can't exceed this).
    pub partition_capacity: u64,
}

/// Resize the filesystem at the `partition`-selected partition of a remote
/// `path` to `new_size_bytes`, in place over the block tier.
///
/// `partition` is the 1-based `IMG@N` selector (or `None` for a superfloppy /
/// single-FS image), resolved against the partition table read over the wire so
/// the offset / capacity match what the local CLI would pick. The resize is the
/// per-FS in-place path ([`resize_filesystem_for`]); it patches FS metadata only
/// and never grows the image, so it stays within the block handle's bounds.
pub fn resize_remote_partition(
    conn: Arc<Mutex<RemoteConnection>>,
    path: &str,
    partition: Option<u32>,
    new_size_bytes: u64,
    log_cb: &mut impl FnMut(&str),
) -> Result<RemoteResizeOutcome> {
    // One read-write reader serves both the table parse (reads) and the resize
    // (reads + writes). Dropping it sends CloseBlock, which syncs on the daemon.
    let mut reader = RemoteBlockReader::open_rw(conn, path)?;

    let ctx = resolve_partition_in_reader(&mut reader, partition)
        .map_err(|e| anyhow!("resolving the remote partition: {e:#}"))?;

    if new_size_bytes > ctx.size {
        log_cb(&format!(
            "warning: requested size {new_size_bytes} bytes exceeds partition capacity \
             {} bytes; the filesystem may refuse",
            ctx.size
        ));
    }

    resize_filesystem_for(&mut reader, ctx.offset, new_size_bytes, log_cb)
        .map_err(|e| anyhow!("resize failed: {e:#}"))?;

    Ok(RemoteResizeOutcome {
        label: ctx.label,
        partition_offset: ctx.offset,
        partition_capacity: ctx.size,
    })
}
