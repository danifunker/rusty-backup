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
    /// Gzip-compressed raw per-partition (`partition-N.gz`). The codec
    /// shared with crusty-backup (`cb-dos`) — the desktop restores and
    /// resizes it exactly like a `.zst` member.
    Gzip,
    /// LZ4-compressed raw per-partition (`partition-N.lz4`). The other codec
    /// shared with crusty-backup (`cb-dos` `/CODEC:LZ4`) — faster than gzip on
    /// a slow CPU, lower ratio; restored/resized exactly like a `.gz` member.
    Lz4,
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
            BackupFormat::Gzip => CompressionType::Gzip,
            BackupFormat::Lz4 => CompressionType::Lz4,
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
    /// Source: an image file or a block-device path. A container whose data
    /// does not start at offset 0 (CHD, dynamic VHD, QCOW2, sparse VMDK) is
    /// decoded to a scratch file in the destination first.
    pub source: PathBuf,

    /// Destination directory. The backup is written under
    /// `DEST/<name>/`. The directory is created if it doesn't exist.
    pub dest: PathBuf,

    /// Backup name (the subdirectory under `DEST`). Defaults to the
    /// source file's stem with a date suffix.
    #[arg(long)]
    pub name: Option<String>,

    /// Output format. Defaults to `chd`, or the `[backup] format` value
    /// from the config file when set.
    #[arg(long, value_enum)]
    pub format: Option<BackupFormat>,

    /// Checksum to record per file. Defaults to `sha256`, or the
    /// `[backup] checksum` value from the config file when set.
    #[arg(long, value_enum)]
    pub checksum: Option<ChecksumKind>,

    /// Skip filesystem-aware compaction; copy every sector verbatim.
    #[arg(long = "sector-by-sector")]
    pub sector_by_sector: bool,

    /// Defragment FAT partitions: relocate each file's clusters contiguously
    /// (boot files first) before imaging. Same output size as ordinary
    /// compaction — the restored disk is just defragmented. Non-FAT
    /// filesystems are unaffected. (The desktop sibling of cb-dos `/DEFRAG`.)
    #[arg(long)]
    pub defrag: bool,

    /// Per-partition filter — comma-separated selectors to include, in any of
    /// the `IMG@…` forms: `1,3` (the `idx` column of `inspect`), `s6,s7` (the
    /// table's own slots) or `DH0,DH1`. Default is "all partitions".
    #[arg(long)]
    pub partitions: Option<String>,

    /// Split the output after this many MiB. Raw (`--format raw`) only: a
    /// `.chd` is a single self-contained container and refuses to be split,
    /// and the compressed codecs currently ignore this.
    #[arg(long = "split-size")]
    pub split_size_mib: Option<u32>,

    /// Image swap/page files verbatim instead of excluding them. By default a
    /// FAT volume's swap/page files (`386SPART.PAR`, `WIN386.SWP`, `PAGEFILE.SYS`,
    /// `HIBERFIL.SYS`, `SWAPPER.DAT`) are kept full-size but their content is
    /// zeroed (they reinitialize on boot), which the codec crushes; `--keep-swap`
    /// images them as-is. (The desktop sibling of cb-dos `/KEEPSWAP`.)
    #[arg(long = "keep-swap")]
    pub keep_swap: bool,
}

pub fn run(args: BackupArgs) -> Result<()> {
    let name = args
        .name
        .clone()
        .unwrap_or_else(|| default_name(&args.source));
    let partition_filter = match &args.partitions {
        Some(list) => Some(resolve_partition_filter(&args.source, list)?),
        None => None,
    };

    let format = args
        .format
        .or_else(|| {
            crate::cli::logging::loaded_config()
                .and_then(|c| c.get("backup", "format"))
                .and_then(parse_format)
        })
        .unwrap_or(BackupFormat::Chd);
    let checksum = args
        .checksum
        .or_else(|| {
            crate::cli::logging::loaded_config()
                .and_then(|c| c.get("backup", "checksum"))
                .and_then(parse_checksum)
        })
        .unwrap_or(ChecksumKind::Sha256);

    let config = BackupConfig {
        source_path: args.source,
        destination_dir: args.dest,
        backup_name: name,
        compression: format.into(),
        checksum: checksum.into(),
        split_size_mib: args.split_size_mib,
        sector_by_sector: args.sector_by_sector,
        partition_filter,
        chd_options: None,
        size_policy: None,
        partition_target_sizes: None,
        shrink_to_minimum: false,
        precomputed_minimum_sizes: None,
        defrag_partition_indices: None,
        defrag_fat: args.defrag,
        keep_swap: args.keep_swap,
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

fn parse_format(s: &str) -> Option<BackupFormat> {
    match s.to_ascii_lowercase().as_str() {
        "chd" => Some(BackupFormat::Chd),
        "dvd" => Some(BackupFormat::Dvd),
        "vhd" => Some(BackupFormat::Vhd),
        "zstd" => Some(BackupFormat::Zstd),
        "gzip" | "gz" => Some(BackupFormat::Gzip),
        "lz4" => Some(BackupFormat::Lz4),
        "raw" => Some(BackupFormat::Raw),
        _ => None,
    }
}

fn parse_checksum(s: &str) -> Option<ChecksumKind> {
    match s.to_ascii_lowercase().as_str() {
        "sha256" => Some(ChecksumKind::Sha256),
        "crc32" => Some(ChecksumKind::Crc32),
        _ => None,
    }
}

/// Resolve `--partitions` to the table slots `partition_filter` matches against
/// `PartitionInfo::index`. Each element accepts any `IMG@…` form.
///
/// Resolving here rather than at parse time means an entry that names nothing
/// is an error, not an empty backup: the old parser turned `--partitions 1`
/// into slot 0, which on an APM disk is the partition map, and produced a
/// backup folder with no partitions in it and no complaint.
fn resolve_partition_filter(source: &std::path::Path, list: &str) -> Result<Vec<usize>> {
    let mut probe = crate::model::source_reader::open_peeled_read_with_entry(source, None, None)
        .with_context(|| format!("opening {} to read its partition table", source.display()))?;
    let table = crate::partition::PartitionTable::detect(&mut probe)
        .map_err(|e| anyhow!("detecting the partition table on {}: {e}", source.display()))?;
    let partitions = table.partitions();

    list.split(',')
        .map(|item| {
            let trimmed = item.trim();
            if trimmed.is_empty() {
                bail!("empty entry in --partitions");
            }
            let sel = crate::cli::img_at::ImageRef::parse(&format!("x@{trimmed}"))?
                .partition
                .ok_or_else(|| anyhow!("empty partition selector {trimmed:?}"))?;
            let part = crate::cli::resolve::select_partition(&table, &partitions, &sel)?;
            Ok(part.index)
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

#[cfg(test)]
mod tests {
    use super::resolve_partition_filter;

    /// Builds a 3-partition MBR disk and checks each selector form lands on the
    /// slot `partition_filter` matches, and that a miss is an error rather than
    /// an empty backup.
    #[test]
    fn partitions_flag_accepts_every_selector_form() {
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("d.img");
        let size = 64 * 1024 * 1024;
        let f = std::fs::File::create(&img).unwrap();
        f.set_len(size).unwrap();
        drop(f);
        use crate::partition::provision::{place, write_table, Geometry, PartSpec};
        let kind = crate::partition::type_catalog::TableKind::Mbr;
        let spec = |mb: u64| PartSpec {
            size: Some(mb * 1024 * 1024),
            type_text: Some("0b".into()),
            name: None,
        };
        let placed = place(
            &[spec(16), spec(16)],
            kind,
            size,
            2048 * 512,
            Default::default(),
        )
        .unwrap();
        let mut w = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&img)
            .unwrap();
        write_table(&mut w, kind, &placed, size, Geometry::default()).unwrap();
        drop(w);

        assert_eq!(resolve_partition_filter(&img, "1").unwrap(), vec![0]);
        assert_eq!(resolve_partition_filter(&img, "1,2").unwrap(), vec![0, 1]);
        assert_eq!(
            resolve_partition_filter(&img, " 2 , 1 ").unwrap(),
            vec![1, 0]
        );
        // MBR slots are fdisk's 1-4, so `s1` is the same partition as `@1`.
        assert_eq!(resolve_partition_filter(&img, "s1").unwrap(), vec![0]);

        assert!(resolve_partition_filter(&img, "0").is_err());
        assert!(resolve_partition_filter(&img, "99").is_err());
        assert!(resolve_partition_filter(&img, "1,,2").is_err());
    }
}
