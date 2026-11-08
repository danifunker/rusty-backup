//! `rb-cli convert IN OUT` — convert one image, or every image in a folder, to other formats.
//!
//! Drives `controller::convert`, the Convert tab's controller (docs/convert_tab.md): each file is
//! read as a hard disk, floppy or disc, and each kind has its own format. `--format` sets every
//! kind that offers it (so `--format chd-cd` still converts a folder of ISOs, and `--format raw`
//! hard disks and floppies alike); `--floppy-to` and `--disc-to` override one kind. `OUT` is a
//! folder, created if absent; a folder source keeps its subfolders there.

use anyhow::{bail, Context, Result};
use clap::Args;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::cli::logging::log_stderr;
use crate::controller::convert::{
    purpose, ChdChoices, ChdProfile, ConvertAction, ConvertController, Extra, ExtraUnit,
    FloppySize, Format, ImageKind, SizeChoice, Sizing,
};
use crate::controller::{Controller as _, Effect};
use crate::model::job::NoNotify;
use crate::rbformats::chd_options::ChdOptions;

#[derive(Debug, Args)]
pub struct ConvertArgs {
    /// Source image or folder. A folder converts every image it holds.
    #[arg(value_name = "IN")]
    pub source: PathBuf,

    /// Destination folder. Created if absent.
    #[arg(value_name = "OUT")]
    pub dest: PathBuf,

    /// Output format, for every kind that offers it: raw, vhd, vhd-dynamic, qcow2, vmdk-flat,
    /// vmdk-sparse, 2mg, chd (hard disks); raw, 2mg, woz, moof, dc42, xdf, hdm, dim, d88
    /// (floppies); chd-cd, chd-dvd, bincue, iso (discs).
    #[arg(long, default_value = "chd")]
    pub format: String,

    /// The floppies' format, overriding --format for them.
    #[arg(long = "floppy-to")]
    pub floppy_to: Option<String>,

    /// The discs' format, overriding --format for them.
    #[arg(long = "disc-to")]
    pub disc_to: Option<String>,

    /// Output extension (no leading dot) for the kinds --format sets. Defaults to the format's.
    #[arg(long)]
    pub extension: Option<String>,

    /// Also convert the images in every subfolder; the output keeps the same folders.
    #[arg(long)]
    pub recursive: bool,

    /// Hard disk size: keep, minimum, minimum+64MB or minimum+10% (the extra goes on every
    /// partition, and never past the partition's original size).
    #[arg(long, default_value = "keep")]
    pub size: String,

    /// Also measure filesystems that need a full read to find their minimum (HFS, ext, UFS,
    /// EFS). Without it those partitions keep their size.
    #[arg(long = "measure-slow")]
    pub measure_slow: bool,

    /// Floppy size: same, or refit to 360k, 720k, 800k, 880k, 1.2m, 1.44m, 1.76m or 2.88m
    /// (a blank disk of that size with every file copied across).
    #[arg(long = "floppy-size", default_value = "same")]
    pub floppy_size: String,

    /// Read each output back and compare it with its source.
    #[arg(long)]
    pub verify: bool,

    /// For BIN/CUE output, write one .bin per track instead of a single concatenated .bin.
    #[arg(long = "bincue-multi-bin")]
    pub bincue_multi_bin: bool,

    /// Overwrite destination files that already exist. Without this they are skipped.
    #[arg(long)]
    pub overwrite: bool,
}

fn format_arg(id: &str) -> Result<Format> {
    // The old spellings stay accepted.
    let id = match id.to_ascii_lowercase().as_str() {
        "twomg" => "2mg".to_string(),
        other => other.to_string(),
    };
    Format::from_id(id.as_str()).with_context(|| format!("unknown format '{id}'"))
}

pub fn run(args: ConvertArgs) -> Result<()> {
    let format = format_arg(args.format.as_str())?;
    let floppy_to = args.floppy_to.as_deref().map(format_arg).transpose()?;
    let disc_to = args.disc_to.as_deref().map(format_arg).transpose()?;
    let sizing = Sizing::parse(args.size.as_str()).map_err(anyhow::Error::msg)?;
    let floppy_size = FloppySize::parse(args.floppy_size.as_str()).map_err(anyhow::Error::msg)?;
    if !args.source.exists() {
        bail!("{} does not exist", args.source.display());
    }
    std::fs::create_dir_all(&args.dest)
        .with_context(|| format!("creating {}", args.dest.display()))?;

    let mut ctl = ConvertController::new(Arc::new(NoNotify));
    ctl.apply(ConvertAction::SetRecursive(args.recursive));
    ctl.apply(ConvertAction::SetSource(args.source.clone()));
    ctl.wait_idle();
    ctl.tick();
    if let Some(e) = ctl.scan_error() {
        bail!("{e}");
    }

    for kind in ImageKind::ALL {
        let chosen = match kind {
            ImageKind::Floppy => floppy_to,
            ImageKind::Disc => disc_to,
            ImageKind::HardDisk => None,
        };
        let pick = chosen.or_else(|| {
            crate::controller::convert::formats_for(kind)
                .contains(&format)
                .then_some(format)
        });
        if let Some(f) = pick {
            if let Some(why) = f.unavailable() {
                bail!("{}: {why}", f.label());
            }
            ctl.apply(ConvertAction::SetFormat(kind, f));
            if let (Some(ext), true) = (&args.extension, f == format) {
                ctl.apply(ConvertAction::SetExtension(kind, ext.clone()));
            }
        }
    }
    let (choice, extra) = match sizing {
        Sizing::Keep => (SizeChoice::Keep, None),
        Sizing::Minimum => (SizeChoice::Minimum, None),
        Sizing::Extra(Extra::Mib(n)) => (SizeChoice::Extra, Some((n, ExtraUnit::Mb))),
        Sizing::Extra(Extra::Percent(n)) => (SizeChoice::Extra, Some((n, ExtraUnit::Percent))),
    };
    ctl.apply(ConvertAction::SetSizing(choice));
    if let Some((n, unit)) = extra {
        ctl.apply(ConvertAction::SetExtraAmount(n));
        ctl.apply(ConvertAction::SetExtraUnit(unit));
    }
    ctl.apply(ConvertAction::SetMeasureSlow(args.measure_slow));
    ctl.apply(ConvertAction::SetFloppySize(floppy_size));
    ctl.apply(ConvertAction::SetVerify(args.verify));
    ctl.apply(ConvertAction::SetMultiBin(args.bincue_multi_bin));

    // A one-file source names its output inside OUT.
    let output = if ctl.source_is_folder() {
        args.dest.clone()
    } else {
        let kind = ctl
            .items()
            .first()
            .and_then(|i| i.found.as_ref().ok())
            .map(|c| c.kind)
            .unwrap_or(ImageKind::HardDisk);
        let stem = args
            .source
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "disk".to_string());
        args.dest.join(format!("{stem}.{}", ctl.extension(kind)))
    };
    let purpose = if ctl.source_is_folder() {
        purpose::OUTPUT_FOLDER
    } else {
        purpose::OUTPUT_FILE
    };
    ctl.apply(ConvertAction::Picked {
        purpose,
        path: Some(output),
    });

    for row in ctl.rows() {
        let note = row.note.map(|n| format!("  ({n})")).unwrap_or_default();
        let line = format!(
            "{}: {} [{}] {}{note}",
            row.name, row.kind, row.detail, row.outcome
        );
        log_stderr(if row.skipped {
            format!("[WARN] {line}")
        } else {
            line
        });
    }
    if let Some(problem) = ctl.start_problem() {
        bail!("{problem}");
    }

    let chd = ChdChoices {
        hd: Some(ChdOptions::defaults_for(ChdProfile::Hd)),
        cd: Some(ChdOptions::defaults_for(ChdProfile::Cd)),
        dvd: Some(ChdOptions::defaults_for(ChdProfile::Dvd)),
    };
    for fx in ctl.apply(ConvertAction::Start(chd)) {
        if let Effect::Choose(q) = fx {
            log_stderr(format!(
                "[WARN] {}",
                q.message.lines().next().unwrap_or_default()
            ));
            let index = if args.overwrite {
                1
            } else {
                log_stderr("Skipping them; pass --overwrite to replace.");
                0
            };
            ctl.apply(ConvertAction::Chose {
                purpose: q.purpose,
                index,
            });
        }
    }
    if !ctl.is_running() {
        bail!("nothing to do: every output already exists; pass --overwrite to replace");
    }
    let mut last_pct = -1i32;
    while ctl.is_running() {
        std::thread::sleep(Duration::from_millis(200));
        ctl.tick();
        for (level, line) in ctl.take_log() {
            log_stderr(format!("[{level:?}] {line}"));
        }
        if let Some(crate::model::job::Progress::Determinate { done, total, .. }) =
            ctl.run_snapshot().map(|s| s.progress)
        {
            let pct = (done as f64 / total.max(1) as f64 * 100.0) as i32;
            if pct / 10 != last_pct / 10 {
                log_stderr(format!("  {pct:>3}%"));
                last_pct = pct;
            }
        }
    }
    ctl.tick();
    for (level, line) in ctl.take_log() {
        log_stderr(format!("[{level:?}] {line}"));
    }
    match ctl.last_result() {
        Some(Ok(r)) if r.failed > 0 => bail!(
            "convert finished with {} failure(s) ({} converted)",
            r.failed,
            r.succeeded
        ),
        Some(Ok(_)) => Ok(()),
        Some(Err(e)) => bail!("convert failed: {e}"),
        None => bail!("convert did not run"),
    }
}
