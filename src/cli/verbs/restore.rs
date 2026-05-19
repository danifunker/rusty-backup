//! `rb-cli restore BACKUP_DIR TARGET` — restore a backup folder to a
//! target file or device. Thin headless front-end over
//! [`crate::restore::run_restore`].

use anyhow::{bail, Context, Result};
use clap::{Args, ValueEnum};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::cli::logging::log_stderr;
use crate::restore::{
    run_restore, RestoreAlignment, RestoreConfig, RestoreProgress, RestoreSizeChoice,
};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum SizeMode {
    /// Use each partition's original size from the backup metadata.
    Original,
    /// Use the imaged minimum size from the backup metadata.
    Minimum,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum AlignmentMode {
    /// Use the alignment recorded in the backup metadata.
    Original,
    /// Modern 1 MB / LBA 2048 alignment.
    Modern1mb,
}

#[derive(Debug, Args)]
pub struct RestoreArgs {
    /// Source backup folder (the directory containing `metadata.json`).
    pub backup_dir: PathBuf,

    /// Target image file or block-device path.
    pub target: PathBuf,

    /// Target size in bytes (defaults to the original disk size from
    /// the backup metadata).
    #[arg(long = "target-size")]
    pub target_size: Option<u64>,

    /// Per-partition size policy.
    #[arg(long, value_enum, default_value = "original")]
    pub size: SizeMode,

    /// Partition alignment policy.
    #[arg(long, value_enum, default_value = "original")]
    pub alignment: AlignmentMode,

    /// Treat `TARGET` as a block device (enables sector-aligned writes
    /// and device-write safety summaries). Phase C: enforced via this
    /// flag; Phase D's auto-detection lands in src/cli/device_safety.rs.
    #[arg(long)]
    pub device: bool,

    /// Confirm destructive write to the target (required for device
    /// targets). For file targets the flag is a no-op.
    #[arg(long)]
    pub yes: bool,

    /// Write zeros to unused filesystem space.
    #[arg(long = "write-zeros-to-unused")]
    pub write_zeros_to_unused: bool,
}

pub fn run(args: RestoreArgs) -> Result<()> {
    if args.device && !args.yes {
        bail!(
            "--device target requires --yes (this will overwrite {}). \
             Phase C safety check; see docs/cli-todo.md § Device-write safety.",
            args.target.display()
        );
    }

    let target_size = match args.target_size {
        Some(n) => n,
        None => read_source_size_from_metadata(&args.backup_dir)?,
    };
    let alignment = match args.alignment {
        AlignmentMode::Original => RestoreAlignment::Original,
        AlignmentMode::Modern1mb => RestoreAlignment::Modern1MB,
    };
    let _size_choice = match args.size {
        SizeMode::Original => RestoreSizeChoice::Original,
        SizeMode::Minimum => RestoreSizeChoice::Minimum,
    };

    let config = RestoreConfig {
        backup_folder: args.backup_dir,
        target_path: args.target,
        target_is_device: args.device,
        target_size,
        alignment,
        // Phase C: per-partition size selection ships from --size (uniform
        // across all partitions). Phase D's batch + per-partition flag map
        // lets the user vary per partition.
        partition_sizes: Vec::new(),
        write_zeros_to_unused: args.write_zeros_to_unused,
    };

    let progress = Arc::new(Mutex::new(RestoreProgress::default()));
    spawn_progress_pump(progress.clone());

    log_stderr(format!(
        "rb-cli restore: {} -> {}",
        config.backup_folder.display(),
        config.target_path.display()
    ));

    run_restore(config, progress).context("restore failed")
}

fn read_source_size_from_metadata(backup_dir: &std::path::Path) -> Result<u64> {
    let meta_path = backup_dir.join("metadata.json");
    let text = std::fs::read_to_string(&meta_path)
        .with_context(|| format!("reading {}", meta_path.display()))?;
    let meta: crate::backup::metadata::BackupMetadata =
        serde_json::from_str(&text).with_context(|| format!("parsing {}", meta_path.display()))?;
    Ok(meta.source_size_bytes)
}

fn spawn_progress_pump(progress: Arc<Mutex<RestoreProgress>>) {
    use std::time::Duration;
    std::thread::spawn(move || {
        let mut last_op = String::new();
        let mut last_pct: i32 = -1;
        loop {
            std::thread::sleep(Duration::from_millis(200));
            let snapshot = match progress.lock() {
                Ok(mut p) => {
                    let drained: Vec<crate::restore::LogMessage> =
                        p.log_messages.drain(..).collect();
                    (
                        p.operation.clone(),
                        p.current_bytes,
                        p.total_bytes,
                        p.finished,
                        drained,
                    )
                }
                Err(_) => return,
            };
            let (op, cur, total, finished, logs) = snapshot;
            for log in logs {
                log_stderr(format!("[{:?}] {}", log.level, log.message));
            }
            if op != last_op {
                last_op = op.clone();
                last_pct = -1;
                if !op.is_empty() {
                    log_stderr(format!("status: {op}"));
                }
            }
            if total > 0 {
                let pct = ((cur as f64 / total as f64) * 100.0) as i32;
                if pct / 5 != last_pct / 5 {
                    log_stderr(format!(
                        "  progress: {:>3}% ({}/{})",
                        pct,
                        crate::partition::format_size(cur),
                        crate::partition::format_size(total),
                    ));
                    last_pct = pct;
                }
            }
            if finished {
                return;
            }
        }
    });
    // Silence unused-import / unused-result warnings in stripped builds.
    let _ = anyhow::Ok::<()>(());
}
