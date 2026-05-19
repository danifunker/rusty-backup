//! `rb-cli fsck IMG[@N]` — check (and optionally repair) a filesystem.
//!
//! Three modes (mutually exclusive):
//! - default: scan + report. Phase B emits the report and exits 1 if
//!   issues were found. Interactive prompt + auto-repair are layered
//!   in once every editable filesystem exposes its `repair()` impl
//!   (lands as the trait surface fills in).
//! - `--checkonly`: scan + report only. Non-zero exit if issues found.
//! - `--repair`: scan + repair without prompting. Falls back to
//!   "Unsupported" when the filesystem hasn't surfaced a repair method.

use anyhow::{anyhow, bail, Result};
use clap::Args;

use crate::cli::exit;
use crate::cli::img_at::ImageRef;
use crate::cli::logging::{log_stderr, out_stdout};
use crate::cli::resolve::{resolve_partition_ro, resolve_partition_rw};

#[derive(Debug, Args)]
pub struct FsckArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Scan only. Never prompt, never repair. Exits non-zero on issues.
    #[arg(long, conflicts_with = "repair")]
    pub checkonly: bool,

    /// Auto-repair detected issues without prompting.
    #[arg(long, conflicts_with = "checkonly")]
    pub repair: bool,

    /// Seconds to wait for an interactive repair confirmation before
    /// resolving to "No" (default 30). `0` waits indefinitely (TTY only).
    #[arg(long = "prompt-timeout", default_value = "30")]
    pub prompt_timeout: u64,
}

pub fn run(args: FsckArgs) -> Result<()> {
    if args.repair {
        return repair_mode(&args.image);
    }
    check_mode(&args.image)
}

fn check_mode(image: &ImageRef) -> Result<()> {
    let (file, ctx) = resolve_partition_ro(&image.path, image.partition)?;
    log_stderr(&ctx.label);
    let mut fs =
        crate::fs::open_filesystem(file, ctx.offset, ctx.type_byte, ctx.type_string.as_deref())
            .map_err(|e| anyhow!("opening filesystem: {e}"))?;

    let report = match fs.fsck() {
        Some(r) => r.map_err(|e| anyhow!("fsck: {e}"))?,
        None => {
            log_stderr("fsck: not supported for this filesystem");
            return Err(anyhow::anyhow!("fsck not supported")
                .context(format!("exit code {}", exit::GENERIC_FAILURE)));
        }
    };

    print_report(&report);

    if report.is_clean() {
        Ok(())
    } else {
        bail!(
            "fsck: {} error(s), {} warning(s){}",
            report.errors.len(),
            report.warnings.len(),
            if report.repairable {
                " (some repairable; re-run with --repair)"
            } else {
                ""
            }
        )
    }
}

fn repair_mode(image: &ImageRef) -> Result<()> {
    let (file, ctx) = resolve_partition_rw(&image.path, image.partition)?;
    log_stderr(&ctx.label);
    let mut fs = crate::fs::open_editable_filesystem(
        file,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening filesystem for repair: {e}"))?;

    let report = match fs.fsck() {
        Some(r) => r.map_err(|e| anyhow!("fsck: {e}"))?,
        None => bail!("fsck not supported for this filesystem"),
    };
    print_report(&report);
    if report.is_clean() {
        out_stdout("fsck: clean, nothing to repair");
        return Ok(());
    }
    if !report.repairable {
        bail!("fsck: no repairable errors found");
    }

    let repair = fs.repair().map_err(|e| anyhow!("repair: {e}"))?;
    out_stdout(format!(
        "Repaired: {} fix(es) applied, {} failed, {} unrepairable",
        repair.fixes_applied.len(),
        repair.fixes_failed.len(),
        repair.unrepairable_count,
    ));
    for f in &repair.fixes_applied {
        out_stdout(format!("  + {f}"));
    }
    for f in &repair.fixes_failed {
        out_stdout(format!("  ! {f}"));
    }
    Ok(())
}

fn print_report(r: &crate::fs::fsck::FsckResult) {
    out_stdout(format!(
        "fsck: {} files / {} dirs checked",
        r.stats.files_checked, r.stats.directories_checked
    ));
    for e in &r.errors {
        if !e.debug {
            out_stdout(format!("  ERROR  [{}] {}", e.code, e.message));
        }
    }
    for w in &r.warnings {
        if !w.debug {
            out_stdout(format!("  WARN   [{}] {}", w.code, w.message));
        }
    }
    for o in &r.orphaned_entries {
        out_stdout(format!(
            "  ORPH   id={} parent_missing={} name={:?} dir={}",
            o.id, o.missing_parent_id, o.name, o.is_directory
        ));
    }
}
