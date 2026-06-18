//! Resolve a partition inside a *backup folder* (a `partition-N.{zst,raw}`
//! governed by `metadata.json`) as an editable / readable image, so the CLI
//! can `put` / `rm` / `get` / `ls` into a backup the same way the GUI's
//! archive-edit flow does.
//!
//! A backup partition isn't seekable in its compressed form, so — exactly like
//! the GUI — we decompress it to a temp flat, operate on that, and (for writes)
//! recompress it back over the original + rewrite the `metadata.json` checksum
//! on commit. The decompress / recompress / metadata-update logic is the shared
//! [`crate::model::archive_edit`] core (`extract_partition_sync` /
//! `recompress_partition_sync`), so the byte semantics match the GUI exactly.
//!
//! Single-file-CHD backups are NOT handled here — that layout stores one `.chd`
//! the user edits directly (the CHD path in [`crate::cli::resolve`] already
//! covers it).

use anyhow::{anyhow, bail, Context, Result};
use std::path::Path;

use crate::backup::metadata::{
    update_partition_checksum, BackupLayout, BackupMetadata, PartitionMetadata,
};
use crate::cli::io::open_image_rw;
use crate::cli::logging::log_stderr;
use crate::cli::resolve::{PartitionContext, RwCommit};
use crate::model::archive_edit::{
    compute_file_checksum, extract_partition_sync, recompress_partition_sync, ArchiveEditContext,
};
use crate::rbformats::{BoxReadSeek, BoxRwSeek};

/// A backup folder is a directory containing a `metadata.json`.
pub fn is_backup_folder(path: &Path) -> bool {
    path.is_dir() && path.join("metadata.json").exists()
}

/// Carried by [`RwCommit::BackupArchive`]: on commit, persists the edit back
/// into the backup and updates `metadata.json`.
pub struct BackupArchiveCommit {
    ectx: ArchiveEditContext,
    mode: CommitMode,
}

/// How a backup partition's edit is persisted.
enum CommitMode {
    /// Compressed (zstd / woz): the edit was made to a decompressed temp flat
    /// (held here to keep it alive); recompress it back over the archive.
    Recompress(tempfile::NamedTempFile),
    /// Uncompressed (none / raw): the partition file was edited in place; just
    /// recompute its checksum (no recompress, no rename).
    RawInPlace,
}

impl BackupArchiveCommit {
    /// Persist the edit and update `metadata.json`. Consumes the commit.
    pub fn commit(self) -> Result<()> {
        match self.mode {
            CommitMode::Recompress(temp) => {
                log_stderr(format!(
                    "Saving backup partition: recompressing into {} ({})...",
                    self.ectx.archive_path.display(),
                    self.ectx.compression_type
                ));
                let mut last_logged_mb: u64 = 0;
                let mut progress_cb = |bytes: u64| {
                    let mb = bytes / (1024 * 1024);
                    if mb >= last_logged_mb + 256 {
                        last_logged_mb = mb;
                        log_stderr(format!("  recompressed {mb} MB so far..."));
                    }
                };
                let cancel = || false;
                let mut log_cb = |msg: &str| log_stderr(format!("  {msg}"));
                recompress_partition_sync(
                    &self.ectx,
                    temp.path(),
                    &mut progress_cb,
                    &cancel,
                    &mut log_cb,
                )
                .map_err(|e| anyhow!("recompressing edited backup partition: {e:#}"))?;
                log_stderr("Backup partition saved (metadata.json updated).");
            }
            CommitMode::RawInPlace => {
                // The raw partition file was edited directly; recompute its
                // checksum and refresh metadata.json (compressed_files unchanged).
                let checksum =
                    compute_file_checksum(&self.ectx.archive_path, &self.ectx.checksum_type)
                        .map_err(|e| {
                            anyhow!("computing {} checksum: {e}", self.ectx.checksum_type)
                        })?;
                update_partition_checksum(
                    &self.ectx.metadata_path,
                    self.ectx.partition_index,
                    &checksum,
                    None,
                )
                .context("updating metadata.json after in-place edit")?;
                log_stderr("Backup partition saved (metadata.json checksum updated).");
            }
        }
        Ok(())
    }
}

/// Open a backup-folder partition read-write: decompress it to a temp flat and
/// hand back a `Read + Write + Seek` handle over that flat, plus a
/// [`RwCommit::BackupArchive`] that recompresses + rewrites metadata on commit.
pub fn open_backup_partition_rw(
    folder: &Path,
    selector: Option<u32>,
) -> Result<(BoxRwSeek, PartitionContext, RwCommit)> {
    let (meta, part) = load_and_select(folder, selector)?;
    let ectx = build_edit_ctx(folder, &meta, &part)?;
    let pctx = partition_context(&part, ectx.original_size);

    if is_uncompressed(&meta.compression_type) {
        // The partition file is already a raw flat image — edit it in place
        // (matches the GUI's in-place backup edit: no decompress / recompress /
        // filename churn). A compacted raw partition is stored shorter than its
        // geometry, so an in-place edit would be unsafe — refuse it.
        if part.compacted {
            bail!(
                "backup partition {} is a compacted raw partition; editing it in \
                 place is unsafe. Restore it to an image, edit, then re-backup.",
                part.index
            );
        }
        log_stderr(format!(
            "Editing backup partition {} (raw, in place): {}",
            part.index,
            ectx.archive_path.display()
        ));
        let file = open_image_rw(&ectx.archive_path)
            .with_context(|| format!("opening {}", ectx.archive_path.display()))?;
        return Ok((
            Box::new(file),
            pctx,
            RwCommit::BackupArchive(BackupArchiveCommit {
                ectx,
                mode: CommitMode::RawInPlace,
            }),
        ));
    }

    // Compressed (zstd / woz): decompress to a temp flat, edit that, recompress
    // back over the archive on commit.
    let temp = tempfile::Builder::new()
        .prefix(".rb-backup-edit-")
        .tempfile()
        .context("creating backup-edit tempfile")?;
    log_stderr(format!(
        "Editing backup partition {} ({}): decompressing {} ({} MB)...",
        part.index,
        meta.compression_type,
        ectx.archive_path.display(),
        ectx.original_size / (1024 * 1024)
    ));
    extract_to_temp(&ectx, temp.path())?;

    let file = open_image_rw(temp.path())?;
    Ok((
        Box::new(file),
        pctx,
        RwCommit::BackupArchive(BackupArchiveCommit {
            ectx,
            mode: CommitMode::Recompress(temp),
        }),
    ))
}

/// Whether a backup's `compression_type` stores each partition as a raw flat
/// file (editable in place) rather than a compressed stream.
fn is_uncompressed(compression_type: &str) -> bool {
    matches!(compression_type, "none" | "raw")
}

/// Open a backup-folder partition read-only: decompress it to a temp flat and
/// hand back a `Read + Seek` reader that owns (and cleans up) the temp.
pub fn open_backup_partition_ro(
    folder: &Path,
    selector: Option<u32>,
) -> Result<(BoxReadSeek, PartitionContext)> {
    let (meta, part) = load_and_select(folder, selector)?;
    let ectx = build_edit_ctx(folder, &meta, &part)?;
    let pctx = partition_context(&part, ectx.original_size);

    // Uncompressed and full-size: read the partition file directly — no temp
    // copy needed (a compacted raw partition still needs the zero-pad below).
    if is_uncompressed(&meta.compression_type) && !part.compacted {
        let file = std::fs::File::open(&ectx.archive_path)
            .with_context(|| format!("opening {}", ectx.archive_path.display()))?;
        return Ok((Box::new(file), pctx));
    }

    let temp = tempfile::Builder::new()
        .prefix(".rb-backup-read-")
        .tempfile()
        .context("creating backup-read tempfile")?;
    log_stderr(format!(
        "Reading backup partition {} ({}): decompressing {}...",
        part.index,
        meta.compression_type,
        ectx.archive_path.display()
    ));
    extract_to_temp(&ectx, temp.path())?;

    // Hold the temp alive for the lifetime of the reader (deleted on drop).
    let path = temp.into_temp_path();
    let file = std::fs::File::open(&path).context("reopening decompressed backup partition")?;
    Ok((Box::new(TempBackedReader { file, _temp: path }), pctx))
}

/// Decompress `ectx` to `temp_path`, logging coarse progress to stderr (the
/// decompress of a multi-GB partition is otherwise silent).
fn extract_to_temp(ectx: &ArchiveEditContext, temp_path: &Path) -> Result<()> {
    let mut last_logged_mb: u64 = 0;
    let mut progress_cb = |bytes: u64| {
        let mb = bytes / (1024 * 1024);
        if mb >= last_logged_mb + 256 {
            last_logged_mb = mb;
            log_stderr(format!("  decompressed {mb} MB so far..."));
        }
    };
    let cancel = || false;
    extract_partition_sync(ectx, temp_path, &mut progress_cb, &cancel)
        .map_err(|e| anyhow!("decompressing backup partition: {e:#}"))
}

/// Load `metadata.json`, validate the layout / compression is editable, and
/// select the requested partition (1-based `FOLDER@N`; defaulted when the
/// backup has exactly one partition).
fn load_and_select(
    folder: &Path,
    selector: Option<u32>,
) -> Result<(BackupMetadata, PartitionMetadata)> {
    let meta_path = folder.join("metadata.json");
    let text = std::fs::read_to_string(&meta_path)
        .with_context(|| format!("reading {}", meta_path.display()))?;
    let meta: BackupMetadata =
        serde_json::from_str(&text).with_context(|| format!("parsing {}", meta_path.display()))?;

    if matches!(meta.layout, BackupLayout::SingleFileChd) {
        bail!(
            "{} is a single-file-CHD backup; edit the .chd inside it directly \
             (point rb-cli at the .chd, e.g. `rb-cli put DIR/<name>.chd@N ...`)",
            folder.display()
        );
    }
    match meta.compression_type.as_str() {
        "zstd" | "none" | "raw" => {}
        other => bail!(
            "editing a '{other}'-compressed backup partition is not supported \
             (supported: zstd, raw / none)"
        ),
    }

    if meta.partitions.is_empty() {
        bail!("backup {} has no partitions", folder.display());
    }
    let part = match selector {
        Some(idx) => {
            let i = idx as usize;
            if i == 0 || i > meta.partitions.len() {
                bail!(
                    "backup partition index {idx} out of range (backup has {} partition(s))",
                    meta.partitions.len()
                );
            }
            meta.partitions[i - 1].clone()
        }
        None => {
            if meta.partitions.len() == 1 {
                meta.partitions[0].clone()
            } else {
                bail!(
                    "backup has {} partitions; specify which with FOLDER@N (1-based)",
                    meta.partitions.len()
                );
            }
        }
    };
    Ok((meta, part))
}

/// Build the shared [`ArchiveEditContext`] for a selected partition.
fn build_edit_ctx(
    folder: &Path,
    meta: &BackupMetadata,
    part: &PartitionMetadata,
) -> Result<ArchiveEditContext> {
    let archive_rel = part.compressed_files.first().ok_or_else(|| {
        anyhow!(
            "backup partition {} has no compressed file listed in metadata.json",
            part.index
        )
    })?;
    Ok(ArchiveEditContext {
        archive_path: folder.join(archive_rel),
        compression_type: meta.compression_type.clone(),
        original_size: part.original_size_bytes,
        compacted: part.compacted,
        metadata_path: folder.join("metadata.json"),
        partition_index: part.index,
        checksum_type: meta.checksum_type.clone(),
    })
}

/// The decompressed backup partition is a raw flat filesystem at byte 0.
fn partition_context(part: &PartitionMetadata, size: u64) -> PartitionContext {
    PartitionContext {
        offset: 0,
        type_byte: part.partition_type_byte,
        type_string: part.partition_type_string.clone(),
        size,
        label: format!(
            "Backup partition {} ({} bytes){}",
            part.index,
            size,
            part.partition_type_string
                .as_deref()
                .map(|s| format!(" [{s}]"))
                .unwrap_or_default()
        ),
    }
}

/// A `Read + Seek` reader that owns the temp file it reads from, deleting it
/// when dropped. Lets [`open_backup_partition_ro`] return a boxed reader whose
/// backing temp lives exactly as long as the reader.
struct TempBackedReader {
    file: std::fs::File,
    _temp: tempfile::TempPath,
}

impl std::io::Read for TempBackedReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        std::io::Read::read(&mut self.file, buf)
    }
}

impl std::io::Seek for TempBackedReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        std::io::Seek::seek(&mut self.file, pos)
    }
}
