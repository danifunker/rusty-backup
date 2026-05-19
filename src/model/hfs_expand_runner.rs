//! HFS expand-block-size worker — clone a classic-HFS volume into a
//! fresh blank volume of a chosen size + allocation block size, wrap
//! the result in a new APM disk image.
//!
//! Shared between the GUI's "Expand HFS Volume…" dialog
//! (`gui/expand_hfs_dialog.rs`) and `rb-cli expand`. The GUI's source-
//! summary helper and `ExpandSource` shape live here too so both
//! surfaces validate inputs the same way.

use std::io::{Cursor, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::fs::hfs::{create_blank_hfs_sized, HfsFilesystem, HFS_MAX_BTREE_FILE_SIZE};
use crate::fs::hfs_clone::{clone_hfs_volume, emit_apm_disk_with_hfs, CloneReport, EmitReport};
use crate::model::source_reader::open_read;
use crate::model::status::ExpandStatus;

/// Allocation block sizes the user can pick. Each maps to a 2 GB-class
/// ceiling defined by HFS's 65535-block u16.
pub const BLOCK_SIZE_CHOICES: &[u32] = &[4096, 8192, 16384, 32768, 65536];

/// Maximum HFS volume size for a given allocation block size.
pub fn max_volume_for_block_size(bs: u32) -> u64 {
    65535u64 * bs as u64
}

/// Suggest the smallest block size whose ceiling can hold `target_bytes`.
/// Falls back to the largest available size if nothing fits.
pub fn suggest_block_size(target_bytes: u64) -> u32 {
    for &bs in BLOCK_SIZE_CHOICES {
        if max_volume_for_block_size(bs) >= target_bytes {
            return bs;
        }
    }
    *BLOCK_SIZE_CHOICES.last().unwrap()
}

/// Source-volume summary used to validate inputs and shape the clone.
#[derive(Debug, Clone)]
pub struct ExpandSource {
    pub source_path: PathBuf,
    pub partition_offset: u64,
    pub partition_size: u64,
    pub source_block_size: u32,
    pub source_catalog_size: u32,
    pub source_extents_size: u32,
    pub used_bytes: u64,
    pub file_count: u64,
    pub dir_count: u64,
    pub volume_name: String,
}

/// Quickly summarise a source HFS volume.
pub fn summarize_source(
    source_path: &Path,
    partition_offset: u64,
    partition_size: u64,
) -> anyhow::Result<ExpandSource> {
    let reader = open_read(source_path)?;
    let summary = HfsFilesystem::open(reader, partition_offset)
        .map_err(|e| anyhow::anyhow!("open HFS: {e}"))?
        .volume_summary();
    Ok(ExpandSource {
        source_path: source_path.to_path_buf(),
        partition_offset,
        partition_size,
        source_block_size: summary.block_size,
        source_catalog_size: summary.catalog_file_size,
        source_extents_size: summary.extents_file_size,
        used_bytes: summary.used_bytes,
        file_count: summary.file_count as u64,
        dir_count: summary.folder_count as u64,
        volume_name: summary.volume_name,
    })
}

/// Spawn the expand worker. Returns the shared status handle.
pub fn start_hfs_expand(
    source: ExpandSource,
    target_size_bytes: u64,
    target_block_size: u32,
    output_path: PathBuf,
) -> Arc<Mutex<ExpandStatus>> {
    let status = Arc::new(Mutex::new(ExpandStatus {
        finished: false,
        error: None,
        log_messages: Vec::new(),
        current_step: "Starting…".to_string(),
        clone_report: None,
        emit_report: None,
    }));
    let status_thread = Arc::clone(&status);
    std::thread::spawn(move || {
        let _wake = crate::os::wakelock::acquire("Rusty Backup: expand HFS volume");
        let result = run_expand(
            source,
            target_size_bytes,
            target_block_size,
            output_path,
            &status_thread,
        );
        let mut s = status_thread.lock().expect("expand status lock");
        s.finished = true;
        match result {
            Ok((clone, emit)) => {
                s.clone_report = Some(clone);
                s.emit_report = Some(emit);
                s.current_step = "Done.".into();
            }
            Err(e) => {
                s.error = Some(e.to_string());
                s.current_step = "Failed.".into();
            }
        }
    });
    status
}

/// Worker body. Returns the clone+emit reports on success.
fn run_expand(
    source: ExpandSource,
    target_size_bytes: u64,
    target_block_size: u32,
    output_path: PathBuf,
    status: &Arc<Mutex<ExpandStatus>>,
) -> anyhow::Result<(CloneReport, EmitReport)> {
    let push = |s: &str| {
        if let Ok(mut g) = status.lock() {
            g.log_messages.push(s.to_string());
        }
    };
    let step = |s: &str| {
        if let Ok(mut g) = status.lock() {
            g.current_step = s.to_string();
        }
    };

    step("Building blank target volume…");
    let catalog_min = source
        .source_catalog_size
        .saturating_mul(3)
        .saturating_div(2)
        .min(HFS_MAX_BTREE_FILE_SIZE);
    let extents_min = source.source_extents_size.min(HFS_MAX_BTREE_FILE_SIZE);
    let mut target_buf = create_blank_hfs_sized(
        target_size_bytes,
        target_block_size,
        &source.volume_name,
        extents_min,
        catalog_min,
    )
    .map_err(|e| anyhow::anyhow!("create_blank_hfs failed: {e}"))?;
    push(&format!(
        "Target B-trees sized: catalog ≥ {} KiB (source {} KiB), extents-overflow ≥ {} KiB (source {} KiB)",
        catalog_min / 1024,
        source.source_catalog_size / 1024,
        extents_min / 1024,
        source.source_extents_size / 1024
    ));
    push(&format!(
        "Allocated {} MiB target image at {} KiB blocks",
        target_buf.len() / (1024 * 1024),
        target_block_size / 1024
    ));

    step("Reading source catalog…");
    let mut source_buffered =
        open_read(&source.source_path).map_err(|e| anyhow::anyhow!("open source: {e}"))?;
    let source_disk_size = source_buffered
        .seek(SeekFrom::End(0))
        .map_err(|e| anyhow::anyhow!("source size: {e}"))?;
    source_buffered
        .seek(SeekFrom::Start(0))
        .map_err(|e| anyhow::anyhow!("source rewind: {e}"))?;

    let mut source_fs = HfsFilesystem::open(source_buffered, source.partition_offset)
        .map_err(|e| anyhow::anyhow!("open source HFS: {e}"))?;

    let source_issue_codes: std::collections::HashSet<String> = source_fs
        .fsck()
        .map(|r| r.errors.iter().map(|e| e.code.clone()).collect())
        .unwrap_or_default();
    if !source_issue_codes.is_empty() {
        push(&format!(
            "Source has pre-existing fsck issue codes: {:?} (will be ignored on target)",
            source_issue_codes
        ));
    }

    step("Cloning files…");
    let target_cursor = Cursor::new(&mut target_buf);
    let mut target_fs = HfsFilesystem::open(target_cursor, 0)
        .map_err(|e| anyhow::anyhow!("open target HFS: {e}"))?;
    let clone_report = clone_hfs_volume(&mut source_fs, &mut target_fs)
        .map_err(|e| anyhow::anyhow!("clone failed: {e}"))?;
    push(&format!(
        "Copied {} file(s) and {} directory(ies); {} bytes data + {} bytes resource fork",
        clone_report.files_copied,
        clone_report.dirs_copied,
        clone_report.data_bytes_copied,
        clone_report.rsrc_bytes_copied
    ));

    step("Verifying target with fsck…");
    {
        let mut verify_fs = HfsFilesystem::open(Cursor::new(&target_buf), 0)
            .map_err(|e| anyhow::anyhow!("reopen target for fsck: {e}"))?;
        let result = verify_fs
            .fsck()
            .map_err(|e| anyhow::anyhow!("fsck error: {e}"))?;
        let (new_issues, inherited): (Vec<_>, Vec<_>) = result
            .errors
            .iter()
            .partition(|i| !source_issue_codes.contains(&i.code));
        for issue in &inherited {
            push(&format!(
                "fsck (inherited from source): [{}] {}",
                issue.code, issue.message
            ));
        }
        for issue in &new_issues {
            push(&format!("fsck: [{}] {}", issue.code, issue.message));
        }
        if !new_issues.is_empty() {
            let codes: Vec<String> = new_issues.iter().map(|e| e.code.clone()).collect();
            anyhow::bail!(
                "target fsck reported {} new error(s) not present in source: {:?}",
                codes.len(),
                codes
            );
        }
    }

    step("Writing new APM disk image…");
    let output_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&output_path)
        .map_err(|e| anyhow::anyhow!("create output: {e}"))?;
    let mut source_for_apm =
        open_read(&source.source_path).map_err(|e| anyhow::anyhow!("reopen source: {e}"))?;
    let emit_report = {
        let mut writer = output_file;
        emit_apm_disk_with_hfs(
            &mut source_for_apm,
            source_disk_size,
            &target_buf,
            &mut writer,
        )
        .map_err(|e| anyhow::anyhow!("emit APM: {e}"))?
    };
    push(&format!(
        "Wrote {} MiB output: {} driver(s) preserved, HFS at block {} ({} blocks)",
        emit_report.total_bytes / (1024 * 1024),
        emit_report.drivers_copied,
        emit_report.hfs_start_block,
        emit_report.hfs_block_count
    ));

    Ok((clone_report, emit_report))
}
