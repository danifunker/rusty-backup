//! Background extract/recompress orchestration for editing partitions
//! inside a compressed backup archive (decompress → edit raw → recompress).
//!
//! [`start_extract`] and [`start_compress`] each spawn a worker thread and
//! return a shared `Arc<Mutex<ArchiveEditProgress>>` that the GUI polls for
//! completion. View-side state mutations (toggling edit mode, clearing
//! caches, re-opening the filesystem) live with the GUI; only the thread
//! plumbing and disk I/O orchestration live here.
//!
//! Extracted from `gui/browse_view.rs` per §5 of `docs/codecleanup.md`.

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::backup::metadata as backup_metadata;
use crate::rbformats::{compress_file_to_archive, decompress_partition_to_file};

/// Static description of the partition being edited inside an archive.
#[derive(Clone)]
pub struct ArchiveEditContext {
    /// Path to the compressed archive file (e.g. partition-0.zst or partition-0.chd).
    pub archive_path: PathBuf,
    /// Compression type string ("zstd", "chd", etc.).
    pub compression_type: String,
    /// Original uncompressed partition size in bytes.
    pub original_size: u64,
    /// Whether the partition was compacted (stream < original_size).
    pub compacted: bool,
    /// Path to metadata.json for updating checksums (empty path skips the update).
    pub metadata_path: PathBuf,
    /// Partition index in metadata.
    pub partition_index: usize,
    /// Checksum type ("sha256" or "crc32").
    pub checksum_type: String,
}

/// Live progress for an extract or compress operation. The GUI polls this
/// each frame; the worker thread sets `finished = true` (and possibly
/// `error`) when done.
pub struct ArchiveEditProgress {
    /// "Extracting" or "Compressing".
    pub phase: String,
    pub current: u64,
    pub total: u64,
    pub finished: bool,
    pub error: Option<String>,
    pub cancel_requested: bool,
    /// Path to the temp file (set when extraction completes).
    pub temp_path: Option<PathBuf>,
}

/// Begin a background decompress of `ctx.archive_path` into `temp_path`.
/// Returns a progress handle the caller polls for completion.
pub fn start_extract(
    ctx: &ArchiveEditContext,
    temp_path: PathBuf,
) -> Arc<Mutex<ArchiveEditProgress>> {
    let archive_path = ctx.archive_path.clone();
    let compression_type = ctx.compression_type.clone();
    let original_size = ctx.original_size;
    let compacted = ctx.compacted;

    let progress = Arc::new(Mutex::new(ArchiveEditProgress {
        phase: "Extracting".to_string(),
        current: 0,
        total: original_size,
        finished: false,
        error: None,
        cancel_requested: false,
        temp_path: Some(temp_path.clone()),
    }));

    let progress_thread = Arc::clone(&progress);
    std::thread::spawn(move || {
        let cancel = {
            let p = Arc::clone(&progress_thread);
            move || p.lock().map(|g| g.cancel_requested).unwrap_or(false)
        };
        let result = decompress_partition_to_file(
            &archive_path,
            &compression_type,
            &temp_path,
            original_size,
            compacted,
            &mut |bytes| {
                if let Ok(mut p) = progress_thread.lock() {
                    p.current = bytes;
                }
            },
            &cancel,
        );
        if let Ok(mut p) = progress_thread.lock() {
            p.finished = true;
            if let Err(e) = result {
                p.error = Some(format!("{e:#}"));
                // Clean up temp file on error
                let _ = std::fs::remove_file(&temp_path);
            }
        }
    });

    progress
}

/// Begin a background recompress of `temp_path` back into `ctx.archive_path`.
/// On success, the temp file is removed and metadata.json is updated with
/// the new checksum + file list (when `ctx.metadata_path` is non-empty).
/// Returns a progress handle the caller polls for completion.
pub fn start_compress(
    ctx: &ArchiveEditContext,
    temp_path: PathBuf,
) -> Arc<Mutex<ArchiveEditProgress>> {
    let archive_path = ctx.archive_path.clone();
    let compression_type = ctx.compression_type.clone();
    let metadata_path = ctx.metadata_path.clone();
    let partition_index = ctx.partition_index;
    let checksum_type = ctx.checksum_type.clone();

    let input_size = std::fs::metadata(&temp_path).map(|m| m.len()).unwrap_or(0);

    let progress = Arc::new(Mutex::new(ArchiveEditProgress {
        phase: "Compressing".to_string(),
        current: 0,
        total: input_size,
        finished: false,
        error: None,
        cancel_requested: false,
        temp_path: None,
    }));

    let progress_thread = Arc::clone(&progress);
    std::thread::spawn(move || {
        let cancel = {
            let p = Arc::clone(&progress_thread);
            move || p.lock().map(|g| g.cancel_requested).unwrap_or(false)
        };

        // Compute checksum of the temp file before compressing.
        let checksum_result = compute_file_checksum(&temp_path, &checksum_type);

        let archive_base = archive_path.with_extension("");
        log::info!(
            "Compressing {} -> {} (type={})",
            temp_path.display(),
            archive_base.display(),
            compression_type
        );
        let result = compress_file_to_archive(
            &temp_path,
            &archive_base,
            &compression_type,
            &mut |bytes| {
                if let Ok(mut p) = progress_thread.lock() {
                    p.current = bytes;
                }
            },
            &cancel,
            &mut |msg| log::info!("{}", msg),
        );

        if let Ok(mut p) = progress_thread.lock() {
            p.finished = true;
            match result {
                Ok(new_files) => {
                    log::info!("Compress succeeded: {:?}", new_files);
                    // Remove old archive file if extension changed.
                    if archive_path.exists() {
                        let new_path = archive_path
                            .parent()
                            .unwrap_or(Path::new("."))
                            .join(&new_files[0]);
                        if new_path != archive_path {
                            let _ = std::fs::remove_file(&archive_path);
                        }
                    }

                    // Update metadata checksum (skip for standalone container
                    // files where metadata_path is empty).
                    if !metadata_path.as_os_str().is_empty() {
                        if let Ok(checksum) = checksum_result {
                            if let Err(e) = backup_metadata::update_partition_checksum(
                                &metadata_path,
                                partition_index,
                                &checksum,
                                Some(&new_files),
                            ) {
                                p.error = Some(format!(
                                    "Saved archive but failed to update metadata: {e}"
                                ));
                            }
                        }
                    }

                    let _ = std::fs::remove_file(&temp_path);
                }
                Err(e) => {
                    p.error = Some(format!("{e:#}"));
                    // Keep temp file on error so user can retry.
                }
            }
        }
    });

    progress
}

/// SHA-256 or CRC-32 checksum of a file, formatted as a lowercase hex string.
pub fn compute_file_checksum(
    path: &Path,
    checksum_type: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let mut file = File::open(path)?;
    let mut buf = vec![0u8; 256 * 1024];

    match checksum_type {
        "sha256" => {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            loop {
                let n = file.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            Ok(format!("{:x}", hasher.finalize()))
        }
        "crc32" => {
            let mut hasher = crc32fast::Hasher::new();
            loop {
                let n = file.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            Ok(format!("{:08x}", hasher.finalize()))
        }
        other => Err(format!("unsupported checksum type: {other}").into()),
    }
}
