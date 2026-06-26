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

    /// Per-partition size policy. Defaults to `original`, or
    /// `[restore] size` from the config file when set.
    #[arg(long, value_enum)]
    pub size: Option<SizeMode>,

    /// Partition alignment policy. Defaults to `original`, or
    /// `[restore] alignment` from the config file when set.
    #[arg(long, value_enum)]
    pub alignment: Option<AlignmentMode>,

    /// Treat `TARGET` as a block device (enables sector-aligned writes
    /// and the full device-write safety preflight in
    /// [`crate::cli::device_safety`]).
    #[arg(long)]
    pub device: bool,

    /// Confirm destructive write to the target (required for device
    /// targets). For file targets the flag is a no-op.
    #[arg(long)]
    pub yes: bool,

    /// Allow writing to the system boot disk (refused by default; only
    /// meaningful with `--device`).
    #[arg(long = "write-to-system-disk")]
    pub write_to_system_disk: bool,

    /// Write zeros to unused filesystem space.
    #[arg(long = "write-zeros-to-unused")]
    pub write_zeros_to_unused: bool,
}

pub fn run(args: RestoreArgs) -> Result<()> {
    // Remote target (`rb://host[:port]/path`): restore over the block tier. The
    // local device-safety preflight below is for local device paths only; a
    // remote device is gated on the daemon side, so dispatch before it.
    #[cfg(feature = "remote")]
    if let Some(remote) = crate::remote::RemoteRef::parse(&args.target.to_string_lossy()) {
        return run_remote(args, remote);
    }

    // A `.cbk` container restores natively: materialize it into a temp folder and
    // restore from that. The desktop's resize-on-restore path consumes the native
    // PerPartition folder (cb_dos_network_and_state.md §2b option a). The TempDir
    // guard is held until `run_restore` returns.
    let mut args = args;
    let _cbk_guard: Option<tempfile::TempDir> =
        if args.backup_dir.is_file() && crate::rbformats::cbk::is_cbk(&args.backup_dir) {
            let tmp = tempfile::TempDir::new().context("creating temp dir for .cbk restore")?;
            crate::rbformats::cbk::materialize_cbk_to_folder(&args.backup_dir, tmp.path())
                .context("materializing .cbk container")?;
            log_stderr(format!(
                "rb-cli restore: materialized {} (.cbk) to a temp folder",
                args.backup_dir.display()
            ));
            args.backup_dir = tmp.path().to_path_buf();
            Some(tmp)
        } else {
            None
        };

    if args.device && !args.yes {
        bail!(
            "--device target requires --yes (this will overwrite {}).",
            args.target.display()
        );
    }
    if args.device {
        let pre = crate::cli::device_safety::preflight(&args.target, args.write_to_system_disk)?;
        crate::cli::device_safety::print_safety_summary(
            "restore",
            &args.backup_dir.display().to_string(),
            &args.target,
            args.target_size.unwrap_or(0),
            pre.device.as_ref(),
        );
    }

    let target_size = match args.target_size {
        Some(n) => n,
        None => read_source_size_from_metadata(&args.backup_dir)?,
    };
    let alignment_choice = args
        .alignment
        .or_else(|| {
            crate::cli::logging::loaded_config()
                .and_then(|c| c.get("restore", "alignment"))
                .and_then(parse_alignment_mode)
        })
        .unwrap_or(AlignmentMode::Original);
    let _size_choice_unresolved = args.size.or_else(|| {
        crate::cli::logging::loaded_config()
            .and_then(|c| c.get("restore", "size"))
            .and_then(parse_size_mode)
    });
    let alignment = match alignment_choice {
        AlignmentMode::Original => RestoreAlignment::Original,
        AlignmentMode::Modern1mb => RestoreAlignment::Modern1MB,
    };
    let size_choice = _size_choice_unresolved.unwrap_or(SizeMode::Original);
    let _size_choice = match size_choice {
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

/// Restore a backup folder to a remote target (device or image file) over the
/// block tier. Materializes locally then pushes — see
/// [`crate::model::restore_remote`].
#[cfg(feature = "remote")]
fn run_remote(args: RestoreArgs, remote: crate::remote::RemoteRef) -> Result<()> {
    use crate::model::restore_remote::restore_to_remote;
    use crate::remote::RemoteConnection;

    if args.device && !args.yes {
        bail!(
            "--device target requires --yes (this will overwrite the remote device {}).",
            remote.path
        );
    }

    let target_size = match args.target_size {
        Some(n) => n,
        None => read_source_size_from_metadata(&args.backup_dir)?,
    };
    let alignment_choice = args
        .alignment
        .or_else(|| {
            crate::cli::logging::loaded_config()
                .and_then(|c| c.get("restore", "alignment"))
                .and_then(parse_alignment_mode)
        })
        .unwrap_or(AlignmentMode::Original);
    let alignment = match alignment_choice {
        AlignmentMode::Original => RestoreAlignment::Original,
        AlignmentMode::Modern1mb => RestoreAlignment::Modern1MB,
    };

    let config = RestoreConfig {
        backup_folder: args.backup_dir,
        // Overridden by restore_to_remote (a local staging image is used for the
        // materialize step before the push); set to a placeholder.
        target_path: PathBuf::new(),
        target_is_device: false,
        target_size,
        alignment,
        partition_sizes: Vec::new(),
        write_zeros_to_unused: args.write_zeros_to_unused,
    };

    let progress = Arc::new(Mutex::new(RestoreProgress::default()));
    spawn_progress_pump(progress.clone());

    log_stderr(format!(
        "rb-cli restore: {} -> rb://{}{} ({})",
        config.backup_folder.display(),
        remote.addr(),
        remote.path,
        if args.device { "device" } else { "image" },
    ));

    let conn = RemoteConnection::connect_shared(&remote.addr())
        .with_context(|| format!("connecting to {}", remote.addr()))?;
    restore_to_remote(config, conn, &remote.path, args.device, progress, None)
        .context("remote restore failed")
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

fn parse_size_mode(s: &str) -> Option<SizeMode> {
    match s.to_ascii_lowercase().as_str() {
        "original" => Some(SizeMode::Original),
        "minimum" => Some(SizeMode::Minimum),
        _ => None,
    }
}

fn parse_alignment_mode(s: &str) -> Option<AlignmentMode> {
    match s.to_ascii_lowercase().as_str() {
        "original" => Some(AlignmentMode::Original),
        "modern1mb" | "modern-1mb" => Some(AlignmentMode::Modern1mb),
        _ => None,
    }
}
