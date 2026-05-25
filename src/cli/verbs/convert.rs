//! `rb-cli convert IN OUT --format FMT` — convert one or more disk images
//! into a chosen output format. Thin headless front-end over
//! [`crate::model::bulk_convert_runner::start_bulk_convert`], the same
//! worker the GUI's Bulk Convert dialog uses.
//!
//! `IN` may be a single file or a folder. When it's a folder, every regular
//! file the chosen format can consume is converted (same scan rules as the
//! dialog — see [`scan_source_folder`]). `OUT` is always a folder; it's
//! created if it doesn't exist. Output filenames reuse the source stem
//! with the format's default extension (overridable with `--extension`).

use anyhow::{bail, Context, Result};
use clap::{Args, ValueEnum};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::cli::logging::log_stderr;
use crate::model::bulk_convert_runner::{
    fmt_bytes, scan_source_folder, start_bulk_convert, ScannedFile,
};
use crate::model::status::{BulkConvertLogLevel, BulkConvertStatus};
use crate::rbformats::chd_options::{ChdOptions, ChdProfile};
use crate::rbformats::export::ExportFormat;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ConvertFormat {
    /// MAME CHD, hard-disk profile (default).
    Chd,
    /// MAME CHD, DVD profile.
    ChdDvd,
    /// MAME CHD, CD profile (input must be .iso or .cue).
    ChdCd,
    /// BIN/CUE extracted from a CD CHD.
    Bincue,
    /// Fixed VHD.
    Vhd,
    /// Dynamic (sparse) VHD — all-zero blocks omitted; same .vhd extension.
    VhdDynamic,
    /// QCOW2 v3 (uncompressed). Sparse: zero clusters omitted.
    Qcow2,
    /// VMDK flat (`monolithicFlat`) — descriptor + sibling `-flat.vmdk` raw extent.
    VmdkFlat,
    /// VMDK sparse (`monolithicSparse`) — single self-contained .vmdk; zero grains omitted.
    VmdkSparse,
    /// Raw disk image (no header).
    Raw,
    /// 2MG (Apple II).
    Twomg,
    /// WOZ 2.0 (Apple II floppy).
    Woz,
    /// DiskCopy 4.2 (Mac / Apple IIgs floppy).
    Dc42,
}

impl From<ConvertFormat> for ExportFormat {
    fn from(f: ConvertFormat) -> Self {
        match f {
            ConvertFormat::Chd => ExportFormat::Chd,
            ConvertFormat::ChdDvd => ExportFormat::ChdDvd,
            ConvertFormat::ChdCd => ExportFormat::ChdCd,
            ConvertFormat::Bincue => ExportFormat::BinCue,
            ConvertFormat::Vhd => ExportFormat::Vhd,
            ConvertFormat::VhdDynamic => ExportFormat::VhdDynamic,
            ConvertFormat::Qcow2 => ExportFormat::Qcow2,
            ConvertFormat::VmdkFlat => ExportFormat::VmdkFlat,
            ConvertFormat::VmdkSparse => ExportFormat::VmdkSparse,
            ConvertFormat::Raw => ExportFormat::Raw,
            ConvertFormat::Twomg => ExportFormat::TwoMg,
            ConvertFormat::Woz => ExportFormat::Woz,
            ConvertFormat::Dc42 => ExportFormat::Dc42,
        }
    }
}

#[derive(Debug, Args)]
pub struct ConvertArgs {
    /// Source file or folder. When a folder, every convertible file is processed.
    #[arg(value_name = "IN")]
    pub source: PathBuf,

    /// Destination folder. Created if absent.
    #[arg(value_name = "OUT")]
    pub dest: PathBuf,

    /// Output format.
    #[arg(long, value_enum, default_value = "chd")]
    pub format: ConvertFormat,

    /// Output extension (no leading dot). Defaults to the format's natural
    /// extension (chd, vhd, img, …).
    #[arg(long)]
    pub extension: Option<String>,

    /// For BIN/CUE output, write one .bin per track instead of a single
    /// concatenated .bin. No effect for other formats.
    #[arg(long = "bincue-multi-bin")]
    pub bincue_multi_bin: bool,

    /// Overwrite destination files that already exist. Without this,
    /// existing outputs are skipped with a warning.
    #[arg(long)]
    pub overwrite: bool,
}

pub fn run(args: ConvertArgs) -> Result<()> {
    let export_format: ExportFormat = args.format.into();
    let extension = args
        .extension
        .clone()
        .unwrap_or_else(|| export_format.extension().to_string());

    let files = collect_inputs(&args.source, export_format)?;
    if files.is_empty() {
        bail!("no convertible files found in {}", args.source.display());
    }

    let (files, skipped_existing) = if args.overwrite {
        (files, Vec::new())
    } else {
        skip_existing(&files, &args.dest, &extension)
    };
    for path in &skipped_existing {
        log_stderr(format!(
            "skipping {} — destination already exists (pass --overwrite to replace)",
            path.display()
        ));
    }
    if files.is_empty() {
        bail!("nothing to do — every destination exists; pass --overwrite to replace");
    }

    let total_bytes: u64 = files
        .iter()
        .map(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0))
        .sum();
    log_stderr(format!(
        "rb-cli convert: {} file(s), {} total -> {} (format: {:?}, ext: .{})",
        files.len(),
        fmt_bytes(total_bytes),
        args.dest.display(),
        args.format,
        extension,
    ));

    let chd_options = match export_format {
        ExportFormat::Chd => Some(ChdOptions::defaults_for(ChdProfile::Hd)),
        ExportFormat::ChdDvd => Some(ChdOptions::defaults_for(ChdProfile::Dvd)),
        ExportFormat::ChdCd => Some(ChdOptions::defaults_for(ChdProfile::Cd)),
        _ => None,
    };

    let status = start_bulk_convert(
        files,
        args.dest.clone(),
        export_format,
        extension,
        chd_options,
        args.bincue_multi_bin,
    );
    drain(status).context("convert failed")
}

/// Resolve the input argument to a list of source files.
fn collect_inputs(source: &std::path::Path, format: ExportFormat) -> Result<Vec<PathBuf>> {
    let meta =
        std::fs::metadata(source).with_context(|| format!("cannot stat {}", source.display()))?;
    if meta.is_file() {
        Ok(vec![source.to_path_buf()])
    } else if meta.is_dir() {
        let scan: Vec<ScannedFile> = scan_source_folder(source, format)
            .with_context(|| format!("scanning {}", source.display()))?;
        Ok(scan.into_iter().map(|s| s.path).collect())
    } else {
        bail!(
            "{} is neither a regular file nor a directory",
            source.display()
        );
    }
}

/// Partition `files` into (will-convert, skipped-because-dest-exists). Mirrors
/// the GUI's conflict-resolution flow but without the interactive prompt.
fn skip_existing(
    files: &[PathBuf],
    dest_dir: &std::path::Path,
    extension: &str,
) -> (Vec<PathBuf>, Vec<PathBuf>) {
    let mut keep = Vec::new();
    let mut skip = Vec::new();
    for path in files {
        let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("disk");
        let dest = dest_dir.join(format!("{stem}.{extension}"));
        if dest.exists() {
            skip.push(path.clone());
        } else {
            keep.push(path.clone());
        }
    }
    (keep, skip)
}

/// Block until the worker finishes, mirroring its log/progress to stderr.
fn drain(status: Arc<Mutex<BulkConvertStatus>>) -> Result<()> {
    let mut last_index: usize = 0;
    let mut last_pct: i32 = -1;
    loop {
        std::thread::sleep(Duration::from_millis(200));
        let (logs, idx, total, cur_bytes, cur_total, finished, succeeded, failed) =
            match status.lock() {
                Ok(mut s) => {
                    let drained: Vec<(BulkConvertLogLevel, String)> =
                        s.log_messages.drain(..).collect();
                    (
                        drained,
                        s.current_index,
                        s.total_files,
                        s.current_bytes,
                        s.current_total_bytes,
                        s.finished,
                        s.succeeded,
                        s.failed,
                    )
                }
                Err(_) => bail!("convert worker poisoned its status mutex"),
            };
        for (level, message) in logs {
            let tag = match level {
                BulkConvertLogLevel::Info => "INFO",
                BulkConvertLogLevel::Warn => "WARN",
                BulkConvertLogLevel::Error => "ERROR",
            };
            log_stderr(format!("[{tag}] {message}"));
        }
        if idx != last_index {
            last_index = idx;
            last_pct = -1;
        }
        if cur_total > 0 {
            let pct = ((cur_bytes as f64 / cur_total as f64) * 100.0) as i32;
            if pct / 5 != last_pct / 5 {
                log_stderr(format!(
                    "  [{}/{}] {:>3}% ({}/{})",
                    idx,
                    total,
                    pct,
                    fmt_bytes(cur_bytes),
                    fmt_bytes(cur_total),
                ));
                last_pct = pct;
            }
        }
        if finished {
            if failed > 0 {
                bail!(
                    "convert finished with {} failure(s) ({} succeeded)",
                    failed,
                    succeeded
                );
            }
            return Ok(());
        }
    }
}
