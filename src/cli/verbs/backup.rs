//! `rb-cli backup SOURCE DEST` — back up a disk image / device to a
//! backup folder. Thin headless front-end over [`crate::backup::run_backup`].
//!
//! Phase C scope: the marquee knobs (compression, checksum, name, split
//! size, sector-by-sector, partition filter). The full GUI knob set
//! (per-partition defrag, per-partition target sizes, CHD codec/hunk
//! tuning, shrink-to-minimum) is reachable through the lower-level
//! flags below, but the most common shapes need only a few arguments.

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, ValueEnum};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::backup::{run_backup, BackupConfig, BackupProgress, ChecksumType, CompressionType};
use crate::cli::logging::log_stderr;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum BackupFormat {
    /// CHD with single-file layout (the canonical shape).
    Chd,
    /// DVD-flavor CHD (different default codec).
    Dvd,
    /// VHD (per-partition or whole-disk).
    Vhd,
    /// Zstd-compressed raw per-partition.
    Zstd,
    /// Uncompressed raw per-partition.
    Raw,
}

impl From<BackupFormat> for CompressionType {
    fn from(f: BackupFormat) -> Self {
        match f {
            BackupFormat::Chd => CompressionType::Chd,
            BackupFormat::Dvd => CompressionType::Dvd,
            BackupFormat::Vhd => CompressionType::Vhd,
            BackupFormat::Zstd => CompressionType::Zstd,
            BackupFormat::Raw => CompressionType::None,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ChecksumKind {
    Sha256,
    Crc32,
}

impl From<ChecksumKind> for ChecksumType {
    fn from(c: ChecksumKind) -> Self {
        match c {
            ChecksumKind::Sha256 => ChecksumType::Sha256,
            ChecksumKind::Crc32 => ChecksumType::Crc32,
        }
    }
}

#[derive(Debug, Args)]
pub struct BackupArgs {
    /// Source: an image file or a block-device path.
    pub source: PathBuf,

    /// Destination directory. The backup is written under
    /// `DEST/<name>/`. The directory is created if it doesn't exist.
    pub dest: PathBuf,

    /// Backup name (the subdirectory under `DEST`). Defaults to the
    /// source file's stem with a date suffix.
    #[arg(long)]
    pub name: Option<String>,

    /// Output format.
    #[arg(long, value_enum, default_value = "chd")]
    pub format: BackupFormat,

    /// Checksum to record per file.
    #[arg(long, value_enum, default_value = "sha256")]
    pub checksum: ChecksumKind,

    /// Skip filesystem-aware compaction; copy every sector verbatim.
    #[arg(long = "sector-by-sector")]
    pub sector_by_sector: bool,

    /// Per-partition filter — comma-separated 1-based indices to
    /// include (e.g. `1,3,4`). Default is "all partitions".
    #[arg(long)]
    pub partitions: Option<String>,

    /// Split each output stream after this many MiB (Zstd / Raw only).
    #[arg(long = "split-size")]
    pub split_size_mib: Option<u32>,
}

pub fn run(args: BackupArgs) -> Result<()> {
    let name = args
        .name
        .clone()
        .unwrap_or_else(|| default_name(&args.source));
    let partition_filter = match args.partitions {
        Some(list) => Some(parse_indices(&list)?),
        None => None,
    };

    let config = BackupConfig {
        source_path: args.source,
        destination_dir: args.dest,
        backup_name: name,
        compression: args.format.into(),
        checksum: args.checksum.into(),
        split_size_mib: args.split_size_mib,
        sector_by_sector: args.sector_by_sector,
        partition_filter,
        chd_options: None,
        size_policy: None,
        partition_target_sizes: None,
        shrink_to_minimum: false,
        precomputed_minimum_sizes: None,
        defrag_partition_indices: None,
    };

    let progress = Arc::new(Mutex::new(BackupProgress::default()));
    spawn_progress_pump(progress.clone());

    log_stderr(format!(
        "rb-cli backup: {} -> {}/{}",
        config.source_path.display(),
        config.destination_dir.display(),
        config.backup_name
    ));

    run_backup(config, progress).context("backup failed")
}

fn default_name(source: &std::path::Path) -> String {
    let stem = source
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("backup");
    let date = chrono::Local::now().format("%Y%m%d-%H%M%S");
    format!("{stem}-{date}")
}

fn parse_indices(s: &str) -> Result<Vec<usize>> {
    s.split(',')
        .map(|p| {
            let trimmed = p.trim();
            trimmed
                .parse::<usize>()
                .map_err(|_| anyhow!("invalid partition index {trimmed:?}"))
                .and_then(|n| {
                    if n == 0 {
                        bail!("partition indices are 1-based")
                    } else {
                        Ok(n)
                    }
                })
        })
        .collect()
}

/// Background thread that prints progress + log messages to stderr.
/// Runs until the worker sets `finished = true`. Drains
/// `log_messages` so each line surfaces exactly once.
fn spawn_progress_pump(progress: Arc<Mutex<BackupProgress>>) {
    use std::time::Duration;
    std::thread::spawn(move || {
        let mut last_op = String::new();
        let mut last_pct: i32 = -1;
        loop {
            std::thread::sleep(Duration::from_millis(200));
            let snapshot = match progress.lock() {
                Ok(mut p) => {
                    let drained: Vec<crate::backup::LogMessage> =
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
}
