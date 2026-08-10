//! `rb-cli resize IMG@N --size BYTES` — resize the filesystem at the
//! selected partition to fit a new partition size. Thin CLI over the
//! per-FS in-place resize functions, dispatched via
//! [`crate::fs::resize_filesystem_for`].
//!
//! **Scope.** This verb resizes the *filesystem* inside the partition
//! — it does NOT modify the partition table. The partition itself must
//! already be the size you're asking the FS to inhabit. To change
//! partition sizes, use `rb-cli restore` (which lays out a fresh
//! disk from a backup with new sizes).
//!
//! Each per-FS resize is a no-op when the on-disk magic doesn't match,
//! so we never need to know up front which filesystem we're growing or
//! shrinking. The supported set follows `fs::resize_filesystem_for`:
//! FAT, Human68k, NTFS, exFAT, HFS, HFS+, ext{2,3,4}, btrfs, SFS, PFS3,
//! AFFS, EFS, QDOS, ProDOS — the same list the GUI's "Resize Partitions..."
//! now uses, so the two surfaces cannot drift apart again.

use anyhow::{Context, Result};
use clap::Args;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;
use crate::cli::parse::parse_size;
use crate::cli::resolve::{resolve_partition_ro, resolve_partition_rw};
use crate::partition::format_size;

#[derive(Debug, Args)]
pub struct ResizeArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// New filesystem size in bytes. Accepts suffixes (`K`, `M`, `G`).
    #[arg(long)]
    pub size: String,

    /// Required to shrink. Growing needs no flag; shrinking truncates the
    /// image, which is not reversible, so it has to be asked for. A shrink
    /// that would cut into live data is refused with or without this.
    #[arg(long)]
    pub confirm_shrink: bool,
}

pub fn run(args: ResizeArgs) -> Result<()> {
    let new_size = parse_size(&args.size).context("parsing --size")?;

    // Remote ref (`rb://host[:port]/img`): resize the filesystem in place over
    // the block tier. The `IMG@N` selector + `--size` mean the same as locally;
    // only the I/O path differs (ranged read/write requests to the daemon).
    #[cfg(feature = "remote")]
    if let Some(remote) = crate::remote::RemoteRef::parse(&args.image.path.to_string_lossy()) {
        return run_remote(remote, args.image.partition.clone(), new_size);
    }

    // Probe read-only first. A shrink has to be checked against where the
    // filesystem's data actually ends, and that answer comes from the driver.
    // Resolving the partition is a table parse; the filesystem is only opened
    // when we are actually shrinking. `repack` opens the same two handles in
    // the same order, so the pattern is known to work on Windows.
    let (ro_file, probe) = resolve_partition_ro(&args.image.path, args.image.partition.clone())?;
    let shrinking = new_size < probe.size;
    if shrinking {
        refuse_unsafe_shrink(ro_file, &probe, new_size, args.confirm_shrink)?;
    } else {
        drop(ro_file);
    }

    let (mut file, ctx, commit) =
        resolve_partition_rw(&args.image.path, args.image.partition.clone())?;
    log_stderr(&ctx.label);
    log_stderr(format!(
        "resize: target size {} ({} bytes), partition offset {} ({} bytes available)",
        format_size(new_size),
        new_size,
        ctx.offset,
        ctx.size,
    ));
    if new_size > ctx.size {
        grow_container_or_refuse(&mut file, &ctx, new_size)?;
    }

    let mut log_cb = |s: &str| log_stderr(format!("  {s}"));
    crate::fs::resize_filesystem_for(&mut file, ctx.offset, new_size, &mut log_cb)
        .context("resize failed")?;
    drop(file);
    // No-op for raw images. For a fixed-geometry floppy container a resize that
    // changed the flat length can't be re-encoded; commit() surfaces that as a
    // clear error rather than writing a malformed container.
    commit.commit()?;
    if shrinking {
        truncate_after_shrink(&ctx, new_size)?;
    }
    log_stderr("resize complete");
    Ok(())
}

/// Refuse a shrink that would cut into live data, or one that was not asked
/// for.
///
/// The filesystem is the only thing that knows where its data ends, so ask it:
/// `last_data_byte` is "bytes from the partition start needed to hold
/// everything that is allocated". Shrinking below that leaves metadata
/// describing blocks past the new end — for FAT that is a cluster chain
/// running off the end of a rewritten FAT, and the file comes back truncated
/// with `get` still exiting 0 (R-037).
///
/// A driver that does not override `last_data_byte` inherits `total_size`, so
/// every shrink is refused for it. That is the intended default: without an
/// answer, no shrink can be shown to be safe.
fn refuse_unsafe_shrink<R: std::io::Read + std::io::Seek + Send + 'static>(
    ro_file: R,
    probe: &crate::cli::resolve::PartitionContext,
    new_size: u64,
    confirmed: bool,
) -> Result<()> {
    let mut fs = crate::fs::open_filesystem(
        ro_file,
        probe.offset,
        probe.type_byte,
        probe.type_string.as_deref(),
    )
    .map_err(|e| anyhow::anyhow!("opening filesystem to check what a shrink would cut: {e}"))?;
    let floor = fs
        .last_data_byte()
        .map_err(|e| anyhow::anyhow!("asking {} where its data ends: {e}", probe.type_name))?;
    drop(fs);

    if new_size < floor {
        return Err(crate::cli::exit::usage(format!(
            "refusing to shrink {} to {}: its data extends to {} ({} bytes). Shrinking below \
             that would cut live data, and the volume would keep reporting the files it can no \
             longer read. The smallest safe size is {}.",
            probe.type_name,
            format_size(new_size),
            format_size(floor),
            floor,
            format_size(floor),
        )));
    }
    if !confirmed {
        return Err(crate::cli::exit::usage(format!(
            "refusing to shrink {} from {} to {} without --confirm-shrink. The shrink is safe \
             — data ends at {} — but truncating the image cannot be undone.",
            probe.type_name,
            format_size(probe.size),
            format_size(new_size),
            format_size(floor),
        )));
    }
    log_stderr(format!(
        "shrink: data ends at {}, target {} — safe",
        format_size(floor),
        format_size(new_size),
    ));
    Ok(())
}

/// Give back the space a shrink freed, when the volume is the whole file.
///
/// Only then: a partition inside a larger disk has data after it, and the disk
/// length is set by the partition table rather than by this verb. There the
/// filesystem shrinks and the image keeps its length, which is what
/// `partmap resize` exists to follow up on.
fn truncate_after_shrink(ctx: &crate::cli::resolve::PartitionContext, new_size: u64) -> Result<()> {
    let Some(path) = ctx.whole_file_path.as_deref() else {
        log_stderr(
            "the volume is a partition inside a larger disk, so the image keeps its length; \
             move the boundary with `rb-cli partmap resize`",
        );
        return Ok(());
    };
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .and_then(|f| f.set_len(new_size))
        .with_context(|| format!("truncating {} to {new_size} bytes", path.display()))?;
    log_stderr(format!("truncated the image to {}", format_size(new_size)));
    Ok(())
}

/// Make room for a grow, or refuse it.
///
/// Growing a filesystem past the end of whatever holds it writes metadata
/// describing blocks that do not exist. This used to print "the FS may refuse"
/// and carry on regardless: the FAT resize happily rewrote the BPB for twice
/// the clusters, `resize complete` printed, and the process exited 0 (R-021).
///
/// The two cases are not the same, so they are no longer treated the same:
///
/// - **The volume is the whole file** (a bare superfloppy in a plain image).
///   Nothing else lives there and there is no table to keep in step, so
///   appending zeros *is* what the caller asked for. Do it, then resize into it.
/// - **Anything else** — a partition inside a larger disk, a decoded container.
///   Its length is set by something we are not editing here, so overrunning it
///   is corruption. Refuse and name the verb that can move the boundary.
fn grow_container_or_refuse(
    file: &mut crate::rbformats::BoxRwSeek,
    ctx: &crate::cli::resolve::PartitionContext,
    new_size: u64,
) -> Result<()> {
    use std::io::{Seek, SeekFrom, Write};

    if ctx.whole_file_path.is_none() {
        return Err(crate::cli::exit::usage(format!(
            "requested size {} exceeds the {} available at partition offset {}. \
             Resizing the filesystem alone would describe blocks the partition does not \
             have. Move the boundary first with `rb-cli partmap resize`, or grow the \
             whole disk with `rb-cli grow IMG --add SIZE`, then resize again.",
            format_size(new_size),
            format_size(ctx.size),
            ctx.offset,
        )));
    }

    let add = new_size - ctx.size;
    log_stderr(format!(
        "growing the image by {} to {} before resizing (the volume is the whole file)",
        format_size(add),
        format_size(new_size),
    ));
    file.seek(SeekFrom::End(0))
        .context("seeking to end of image to grow it")?;
    // 1-MiB chunks so a very large grow doesn't allocate a buffer to match.
    let chunk = vec![0u8; 1024 * 1024];
    let mut remaining = add;
    while remaining > 0 {
        let n = remaining.min(chunk.len() as u64) as usize;
        file.write_all(&chunk[..n]).context("growing image")?;
        remaining -= n as u64;
    }
    file.flush().context("flushing grown image")?;
    Ok(())
}

/// Resize a remote image's filesystem in place over the block tier. Connects to
/// the daemon, then defers to the testable [`resize_remote_partition`] core.
#[cfg(feature = "remote")]
fn run_remote(
    remote: crate::remote::RemoteRef,
    partition: Option<crate::cli::img_at::PartSelector>,
    new_size: u64,
) -> Result<()> {
    use crate::model::resize_remote::resize_remote_partition;
    use crate::remote::RemoteConnection;

    log_stderr(format!(
        "resize: remote {} (partition {}), target size {} ({new_size} bytes)",
        remote.path,
        partition
            .as_ref()
            .map(|n| n.to_string())
            .unwrap_or_else(|| "auto".into()),
        format_size(new_size),
    ));
    let conn = RemoteConnection::connect_shared(&remote.addr())
        .with_context(|| format!("connecting to {}", remote.addr()))?;
    let mut log_cb = |s: &str| log_stderr(format!("  {s}"));
    let outcome = resize_remote_partition(conn, &remote.path, partition, new_size, &mut log_cb)?;
    log_stderr(outcome.label);
    log_stderr("resize complete");
    Ok(())
}
