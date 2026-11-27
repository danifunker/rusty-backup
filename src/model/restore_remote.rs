//! Testable core for restoring a backup folder to a **remote target** — a
//! physical device or an image file on an `rb-cli serve` daemon — over the
//! block tier.
//!
//! Restore's engine (`restore::run_restore`) writes to a concrete local `File`
//! (it wraps a `SectorAlignedWriter<File>`, calls `set_len`, re-opens the target
//! path mid-restore, and does per-partition FS finalization through `&mut
//! File`), so it isn't a clean streaming-target seam the way backup's read side
//! was. Rather than rewrite that 1500-line, multi-layout function, this mirrors
//! the remote-backup-CHD pattern in reverse:
//!
//!   1. run the FULL local restore into a local **staging image** (every layout
//!      — regular per-partition / single-file-CHD / Clonezilla — works
//!      unchanged), then
//!   2. raw-push the finished image to the remote target via `WriteBlock`.
//!
//! The desktop stays the smart side; the daemon just lands bytes. Cost is the
//! staging space + a second pass over the image — the same trade the remote-CHD
//! backup already makes. A future streaming path could skip the staging file by
//! making `run_restore` target-generic, but that's a larger refactor.

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, bail, Context, Result};

use crate::backup::LogLevel;
use crate::remote::RemoteConnection;
use crate::restore::{run_restore, LogMessage, RestoreConfig, RestoreProgress};

/// Bytes per `WriteBlock` frame when pushing the staging image. Kept under the
/// daemon's `MAX_RANGE_WRITE` cap.
const PUSH_CHUNK: usize = 1024 * 1024;

/// What a restore writes into its sparse stage: the backup's imaged partitions and 16 MiB of slack.
fn staged_bytes(backup_folder: &Path) -> Option<u64> {
    let f = File::open(backup_folder.join("metadata.json")).ok()?;
    let meta: crate::backup::metadata::BackupMetadata = serde_json::from_reader(f).ok()?;
    let data: u64 = meta.partitions.iter().map(|p| p.imaged_size_bytes).sum();
    Some(data.saturating_add(16 << 20))
}

/// Restore the backup folder named in `config` to a remote target.
///
/// `config.target_path` / `config.target_is_device` are overridden internally (a
/// local staging file is used for the materialize step); the real destination is
/// `remote_path` on `conn` — a physical device when `is_device`, else an image
/// file created under the daemon's serve root. `config.target_size` is the disk
/// image size to produce and push. `scratch_dir` is where the staging image
/// lives (system temp when `None`); it should have room for the whole image.
pub fn restore_to_remote(
    mut config: RestoreConfig,
    conn: Arc<Mutex<RemoteConnection>>,
    remote_path: &str,
    is_device: bool,
    progress: Arc<Mutex<RestoreProgress>>,
    scratch_dir: Option<&Path>,
    overwrite_existing: bool,
) -> Result<()> {
    let target_size = config.target_size;

    // --- 1. Materialize the restore into a local staging image. ---
    // The system temp may be a small tmpfs; a stage that can't fit fails now, not mid-write (MSR-017).
    // The stage is sparse, so only the imaged partitions (plus slack) land in it.
    let stage_dir = scratch_dir.map_or_else(std::env::temp_dir, Path::to_path_buf);
    let written = staged_bytes(&config.backup_folder).map(|n| n.min(target_size));
    if let (Some(free), Some(need)) = (crate::os::available_space(&stage_dir), written) {
        if free < need {
            anyhow::bail!(
                "{} has {} free but the restore stages {}; pick a scratch folder with room",
                stage_dir.display(),
                disk_core::display::format_size(free),
                disk_core::display::format_size(need)
            );
        }
    }
    let mut builder = tempfile::Builder::new();
    builder.prefix("rb-restore-").suffix(".img");
    let staging = match scratch_dir {
        Some(d) => builder.tempfile_in(d),
        None => builder.tempfile(),
    }
    .context("creating restore staging file")?;
    let staging_path = staging.path().to_path_buf();

    config.target_path = staging_path.clone();
    config.target_is_device = false;
    log(
        &progress,
        LogLevel::Info,
        format!(
            "Restoring to a local staging image before pushing to {remote_path} \
             ({target_size} bytes)..."
        ),
    );
    run_restore(config, Arc::clone(&progress)).context("local restore (staging) failed")?;

    if is_cancelled(&progress) {
        bail!("restore cancelled");
    }

    // The staging image is at most `target_size`; push exactly what was written
    // (a sparse tail beyond the last data is logically zero — the remote image
    // file is created zero-filled, and a device's tail is left as-is, matching a
    // local restore which also doesn't write past the data extent).
    let push_len = staging_path
        .metadata()
        .with_context(|| format!("stat staging image {}", staging_path.display()))?
        .len()
        .min(target_size);

    // --- 2. Open the remote write target. ---
    set_operation(&progress, "Opening remote target...");
    let (handle, remote_size) = {
        let mut c = lock(&conn)?;
        c.open_write_target(remote_path, is_device, target_size, overwrite_existing)?
    };
    if push_len > remote_size {
        let _ = close_block(&conn, handle);
        bail!(
            "remote target is {remote_size} bytes, smaller than the {push_len}-byte restore image"
        );
    }

    // --- 3. Push the staging image to the remote target. ---
    set_operation(&progress, "Pushing image to remote target...");
    // A drive restore is laid out to the drive's size; only the partitions and the table tail are
    // sent, not the unallocated zeros between (a 4 GB backup onto a 256 GB card sent 256 GB).
    let skip = if is_device {
        unallocated_gap(&staging_path, push_len)
    } else {
        None
    };
    let push_result = push_image(&staging_path, &conn, handle, push_len, skip, &progress);

    // --- 4. Flush + close the remote handle regardless of outcome. ---
    if push_result.is_ok() {
        if let Ok(mut c) = lock(&conn) {
            let _ = c.flush_block(handle);
        }
    }
    let _ = close_block(&conn, handle);

    push_result?;
    // `staging` drops here, deleting the staging image.
    log(&progress, LogLevel::Info, "Remote restore complete");
    Ok(())
}

/// Stream the first `total` bytes of `path` to the remote write `handle` in
/// `PUSH_CHUNK`-sized `WriteBlock` frames, reporting progress and honouring
/// cancellation at chunk boundaries.
fn push_image(
    path: &Path,
    conn: &Arc<Mutex<RemoteConnection>>,
    handle: u64,
    total: u64,
    skip: Option<(u64, u64)>,
    progress: &Arc<Mutex<RestoreProgress>>,
) -> Result<()> {
    use std::io::{Seek, SeekFrom};
    let mut f = File::open(path).with_context(|| format!("opening staging image {path:?}"))?;
    set_progress_bytes(progress, 0, total);
    let mut buf = vec![0u8; PUSH_CHUNK];
    let mut offset = 0u64;
    while offset < total {
        if is_cancelled(progress) {
            bail!("restore cancelled");
        }
        if let Some((_, to)) = skip.filter(|&(from, to)| offset >= from && offset < to) {
            offset = to;
            f.seek(SeekFrom::Start(offset))?;
            set_progress_bytes(progress, offset, total);
            continue;
        }
        let end = match skip {
            Some((from, _)) if offset < from => from.min(total),
            _ => total,
        };
        let want = PUSH_CHUNK.min((end - offset) as usize);
        f.read_exact(&mut buf[..want])
            .with_context(|| format!("reading staging image at offset {offset}"))?;
        {
            let mut c = lock(conn)?;
            c.write_block(handle, offset, &buf[..want])
                .with_context(|| format!("pushing {want} bytes at offset {offset}"))?;
        }
        offset += want as u64;
        set_progress_bytes(progress, offset, total);
    }
    Ok(())
}

/// The unallocated stretch of a staged drive image, from the last partition's end to the final
/// MiB (where a GPT keeps its backup header), MiB-aligned; `None` when there is none worth skipping.
fn unallocated_gap(path: &Path, total: u64) -> Option<(u64, u64)> {
    const MIB: u64 = 1 << 20;
    let mut f = File::open(path).ok()?;
    let table = crate::partition::PartitionTable::detect(&mut f).ok()?;
    let end = table
        .partitions()
        .iter()
        .map(|p| p.byte_offset().saturating_add(p.size_bytes))
        .max()?;
    let from = end.div_ceil(MIB) * MIB;
    let to = total.saturating_sub(MIB) / MIB * MIB;
    (to > from + MIB).then_some((from, to))
}

fn lock(
    conn: &Arc<Mutex<RemoteConnection>>,
) -> Result<std::sync::MutexGuard<'_, RemoteConnection>> {
    conn.lock()
        .map_err(|_| anyhow!("remote connection lock poisoned"))
}

fn close_block(conn: &Arc<Mutex<RemoteConnection>>, handle: u64) -> Result<()> {
    lock(conn)?.close_block(handle)
}

fn log(progress: &Arc<Mutex<RestoreProgress>>, level: LogLevel, message: impl Into<String>) {
    if let Ok(mut p) = progress.lock() {
        p.log_messages.push_back(LogMessage {
            level,
            message: message.into(),
        });
    }
}

fn set_operation(progress: &Arc<Mutex<RestoreProgress>>, op: impl Into<String>) {
    if let Ok(mut p) = progress.lock() {
        p.operation = op.into();
    }
}

fn set_progress_bytes(progress: &Arc<Mutex<RestoreProgress>>, current: u64, total: u64) {
    if let Ok(mut p) = progress.lock() {
        p.current_bytes = current;
        p.total_bytes = total;
    }
}

fn is_cancelled(progress: &Arc<Mutex<RestoreProgress>>) -> bool {
    progress.lock().map(|p| p.cancel_requested).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 64 MiB drive image holding one 8 MiB partition: only the gap after it is skipped.
    #[test]
    fn the_unallocated_tail_is_skipped_but_not_the_last_mib() {
        let mbr = part_tables::mbr::build_minimal_mbr(1, &[(0x0b, 2048, 16384, false)], 255, 63);
        let f = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(f.path(), &mbr[..]).unwrap();
        let total = 64u64 << 20;
        f.as_file().set_len(total).unwrap();
        let (from, to) = unallocated_gap(f.path(), total).expect("a gap");
        assert_eq!(from, 9 << 20, "the partition ends at 9 MiB");
        assert_eq!(to, total - (1 << 20), "the last MiB is always sent");
    }
}
