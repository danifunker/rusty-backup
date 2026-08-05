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
//!
//! Output:
//! - `--format text` (default): human-readable lines on stdout, log
//!   chatter on stderr.
//! - `--format json` / `--format yaml`: a single envelope on stdout,
//!   matching the schema used by `inspect` / `show *` / `locate`.
//! - `--format csv|tsv`: rejected — `FsckResult` is nested.

use anyhow::{anyhow, bail, Result};
use clap::Args;
use serde::Serialize;

use crate::cli::exit;
use crate::cli::img_at::ImageRef;
use crate::cli::logging::{log_stderr, out_stdout};
use crate::cli::output::{emit_envelope, require_non_flat, Envelope, OutputFormat};
use crate::cli::resolve::{
    resolve_partition_rw_forced, resolve_partition_streaming_forced, FsDispatchOverride,
};
use crate::fs::alto::{self, bfs::BfsFilesystem, FsFamily};
use crate::fs::filesystem::{EditableFilesystem, Filesystem};
use crate::fs::fsck::{FsckResult, RepairReport};

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
    /// resolving to "No" (default 30; or `[fsck] prompt-timeout` from
    /// the config file when set). `0` waits indefinitely (TTY only).
    #[arg(long = "prompt-timeout")]
    pub prompt_timeout: Option<u64>,

    /// Output format. `text` (default) emits the human-readable report;
    /// `json` / `yaml` emit a status-wrapped envelope mirroring the
    /// other read-only verbs. `csv` / `tsv` are rejected — the report
    /// is nested.
    #[arg(long, default_value_t = OutputFormat::Text, value_enum)]
    pub format: OutputFormat,

    /// Force a filesystem dispatch (`--fs-type`). Required for signatureless
    /// images: CP/M has no on-disk magic, so `--fs-type cpm:<preset>` selects
    /// the disk-parameter block (e.g. `cpm:amstrad_data`).
    #[command(flatten)]
    pub fs_override: FsDispatchOverride,
}

/// Structured payload for the `--format json|yaml` envelope. Wraps the
/// raw `FsckResult` plus the `clean` summary so script consumers can
/// branch on a single boolean without re-deriving it from `errors`.
#[derive(Debug, Serialize)]
struct FsckPayload<'a> {
    clean: bool,
    report: &'a FsckResult,
}

#[derive(Debug, Serialize)]
struct RepairPayload<'a> {
    clean_before: bool,
    report: &'a FsckResult,
    repair: Option<&'a RepairReport>,
}

pub fn run(args: FsckArgs) -> Result<()> {
    let _timeout = args
        .prompt_timeout
        .or_else(|| {
            crate::cli::logging::loaded_config()
                .and_then(|c| c.get("fsck", "prompt-timeout"))
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or(30u64);
    // Note: the interactive prompt is still on the roadmap; the flag is
    // resolved here so callers / scripts get a deterministic value once
    // it lands. The current `check_mode` / `repair_mode` paths don't
    // prompt — they exit immediately.

    // fsck's payload is the nested FsckResult, so CSV/TSV doesn't apply.
    require_non_flat(args.format, "fsck")?;

    if args.repair {
        return repair_mode(&args.image, args.format, &args.fs_override);
    }
    check_mode(&args.image, args.format, &args.fs_override)
}

fn check_mode(
    image: &ImageRef,
    format: OutputFormat,
    fs_override: &FsDispatchOverride,
) -> Result<()> {
    // Alto disk packs open through the container parser, not the block factory.
    if let Some((disk, _is_pdi)) = read_alto_disk(&image.path)? {
        return alto_check(disk, image, format);
    }

    // `resolve_partition_streaming_forced` peels container / image wrappers
    // (.atr, .d88, CHD, GHO, …) to a flat stream — the same read path ls / get
    // / inspect use — so `fsck` (check) works on a wrapped image, matching the
    // read-write repair path. The `--fs-type` override makes a signatureless
    // image (CP/M) dispatchable when detection alone can't identify it.
    let (file, mut ctx) = resolve_partition_streaming_forced(
        &image.path,
        image.partition.clone(),
        None,
        fs_override.fs_type.as_deref(),
    )?;
    fs_override.apply(&mut ctx);
    log_stderr(&ctx.label);
    let mut fs =
        crate::fs::open_filesystem(file, ctx.offset, ctx.type_byte, ctx.type_string.as_deref())
            .map_err(|e| anyhow!("opening filesystem: {e}"))?;

    let report = match fs.fsck() {
        Some(r) => r.map_err(|e| anyhow!("fsck: {e}"))?,
        None => {
            // Surface a structured "unsupported" envelope when JSON/YAML
            // was asked for, so scripts can pivot on `status.error`
            // without parsing the stderr text. Plain-text mode keeps the
            // existing log line + non-zero exit.
            return fsck_unsupported(format);
        }
    };

    emit_check_report(&report, format)
}

/// Emit an `fsck` (check-mode) report through the text or structured path and
/// return the correct exit status. Shared by the block-factory path and the
/// Alto-pack path.
fn emit_check_report(report: &FsckResult, format: OutputFormat) -> Result<()> {
    if format.is_structured() {
        let payload = FsckPayload {
            clean: report.is_clean(),
            report,
        };
        let env = Envelope::ok(payload);
        emit_envelope(format, &env)?;
        return if report.is_clean() {
            Ok(())
        } else {
            // Non-zero exit even in structured mode so shell `$?`
            // branching still works alongside `status.error`.
            Err(anyhow!("fsck: {} error(s)", report.errors.len()))
        };
    }

    print_report(report);
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

fn repair_mode(
    image: &ImageRef,
    format: OutputFormat,
    fs_override: &FsDispatchOverride,
) -> Result<()> {
    // Alto disk packs open through the container parser + persist as PDI, not
    // through the block-factory / commit path.
    if let Some((disk, is_pdi)) = read_alto_disk(&image.path)? {
        return alto_repair(disk, is_pdi, image, format);
    }

    let (file, mut ctx, commit) = resolve_partition_rw_forced(
        &image.path,
        image.partition.clone(),
        fs_override.fs_type.as_deref(),
    )?;
    fs_override.apply(&mut ctx);
    log_stderr(&ctx.label);
    let mut fs = ctx
        .open_editable(file)
        .map_err(|e| anyhow!("opening filesystem for repair: {e}"))?;

    let report = match fs.fsck() {
        Some(r) => r.map_err(|e| anyhow!("fsck: {e}"))?,
        None => return fsck_unsupported(format),
    };

    if report.is_clean() {
        return emit_repair_clean(&report, format);
    }
    if !report.repairable {
        return emit_repair_unrepairable(&report, format);
    }

    let repair = fs.repair().map_err(|e| anyhow!("repair: {e}"))?;
    // Persist: re-encode the temp flat back into the container (no-op for raw
    // images). The clean / unrepairable paths above return before mutating, so
    // they leave the container untouched.
    drop(fs);
    commit.commit()?;

    emit_repair_done(&report, &repair, format)
}

/// Clean-before-repair: nothing to do. Shared emit path.
fn emit_repair_clean(report: &FsckResult, format: OutputFormat) -> Result<()> {
    if format.is_structured() {
        let env = Envelope::ok(RepairPayload {
            clean_before: true,
            report,
            repair: None,
        });
        emit_envelope(format, &env)?;
    } else {
        print_report(report);
        out_stdout("fsck: clean, nothing to repair");
    }
    Ok(())
}

/// Errors found but none are auto-repairable. Shared emit path (non-zero exit).
fn emit_repair_unrepairable(report: &FsckResult, format: OutputFormat) -> Result<()> {
    if format.is_structured() {
        let env = Envelope::error(
            exit::GENERIC_FAILURE,
            "fsck: no repairable errors found",
            Some(RepairPayload {
                clean_before: false,
                report,
                repair: None,
            }),
        );
        emit_envelope(format, &env)?;
        bail!("fsck: no repairable errors found");
    }
    print_report(report);
    bail!("fsck: no repairable errors found");
}

/// A repair ran; report what it did. Shared emit path.
fn emit_repair_done(
    report: &FsckResult,
    repair: &RepairReport,
    format: OutputFormat,
) -> Result<()> {
    if format.is_structured() {
        let env = Envelope::ok(RepairPayload {
            clean_before: false,
            report,
            repair: Some(repair),
        });
        emit_envelope(format, &env)?;
        return Ok(());
    }

    print_report(report);
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

/// Emit a structured "unsupported" response when the filesystem can't
/// be fsck'd. Text mode keeps the existing stderr log + non-zero exit.
fn fsck_unsupported(format: OutputFormat) -> Result<()> {
    if format.is_structured() {
        let env: Envelope<FsckPayload<'_>> = Envelope::error(
            exit::GENERIC_FAILURE,
            "fsck not supported for this filesystem",
            None,
        );
        emit_envelope(format, &env)?;
        return Err(anyhow!("fsck not supported"));
    }
    log_stderr("fsck: not supported for this filesystem");
    Err(anyhow::anyhow!("fsck not supported")
        .context(format!("exit code {}", exit::GENERIC_FAILURE)))
}

fn print_report(r: &FsckResult) {
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

/// If `path` is an Alto / Pilot disk pack, decode it into an in-memory `Disk`;
/// otherwise `Ok(None)` so the caller falls through to the block factory. These
/// packs (`.pdi` / `.bfs` / CopyDisk / Salto / ContrAlto / Trident / `.zdisk`)
/// carry out-of-band sector labels and can't be represented as a flat sector
/// stream, so they open through `alto::open_pack` rather than `open_filesystem`.
///
/// Returns `(disk, is_pdi)`; `is_pdi` gates in-place `--repair` (repair rebuilds
/// the pack as a PDI, which would silently change a non-PDI container's format).
/// A cheap magic/size pre-check gates the full read so an unrelated large image
/// is never slurped just to be rejected.
fn read_alto_disk(path: &std::path::Path) -> Result<Option<(alto::Disk, bool)>> {
    use std::io::Read as _;
    let len = match std::fs::metadata(path) {
        Ok(m) if m.is_file() => m.len() as usize,
        _ => return Ok(None),
    };
    // The largest legitimate Alto pack is a Trident T-300; skip anything bigger.
    if len == 0 || len > alto::trident::T300_BYTES {
        return Ok(None);
    }
    let mut magic = [0u8; 8];
    let n = std::fs::File::open(path)?.read(&mut magic).unwrap_or(0);
    if !alto::looks_like_pack(&magic[..n], len) {
        return Ok(None);
    }
    let bytes = std::fs::read(path)?;
    match alto::open_pack(&bytes) {
        Ok(disk) => {
            let is_pdi = bytes.len() >= alto::pdi::MAGIC.len()
                && &bytes[..alto::pdi::MAGIC.len()] == alto::pdi::MAGIC;
            Ok(Some((disk, is_pdi)))
        }
        // Gate passed but it isn't actually a pack (e.g. an unrelated image of
        // exactly the Salto/Trident size) — fall through to the normal path.
        Err(_) => Ok(None),
    }
}

fn alto_family_label(disk: &alto::Disk) -> &'static str {
    match disk.geometry.family {
        FsFamily::Diablo => "Alto BFS (Diablo)",
        FsFamily::Trident => "Alto TFS (Trident)",
        FsFamily::Pilot => "Pilot/Cedar",
    }
}

/// `fsck` (check-mode) on an Alto BFS/TFS pack. Pilot/Cedar volumes have no
/// checker, so they surface the same "unsupported" response as any other FS.
fn alto_check(disk: alto::Disk, image: &ImageRef, format: OutputFormat) -> Result<()> {
    if image.partition.is_some() {
        bail!("Alto disk packs have no partition table; drop the `@N` suffix");
    }
    log_stderr(format!("Alto pack: {}", alto_family_label(&disk)));
    if disk.geometry.family == FsFamily::Pilot {
        return fsck_unsupported(format);
    }
    let mut fs = BfsFilesystem::open(disk);
    let report = fs
        .fsck()
        .expect("BFS implements fsck")
        .map_err(|e| anyhow!("fsck: {e}"))?;
    emit_check_report(&report, format)
}

/// `fsck --repair` on an Alto BFS/TFS pack. Repair rebuilds the volume and
/// writes it back as a PARC Disk Image, so it is only allowed in place for a
/// `.pdi` input — any other container would have its format silently changed.
fn alto_repair(
    disk: alto::Disk,
    is_pdi: bool,
    image: &ImageRef,
    format: OutputFormat,
) -> Result<()> {
    if image.partition.is_some() {
        bail!("Alto disk packs have no partition table; drop the `@N` suffix");
    }
    if disk.geometry.family == FsFamily::Pilot {
        return fsck_unsupported(format);
    }
    if !is_pdi {
        bail!(
            "Alto --repair rebuilds the volume and writes it back as a PARC Disk Image (PDI); \
             the input is not a .pdi, and repairing it in place would change the container \
             format. Copy/convert it to a .pdi first, or repair it in the GUI (which saves \
             the fix to a chosen PDI path)."
        );
    }
    log_stderr(format!("Alto pack: {}", alto_family_label(&disk)));
    let mut fs = BfsFilesystem::open_editable(disk, image.path.clone());
    let report = fs
        .fsck()
        .expect("BFS implements fsck")
        .map_err(|e| anyhow!("fsck: {e}"))?;
    if report.is_clean() {
        return emit_repair_clean(&report, format);
    }
    if !report.repairable {
        return emit_repair_unrepairable(&report, format);
    }
    // BfsFilesystem::repair() rebuilds the DiskDescriptor and writes the PDI to
    // the save path (the original file) itself — no container commit step.
    let repair = fs.repair().map_err(|e| anyhow!("repair: {e}"))?;
    emit_repair_done(&report, &repair, format)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::output::Envelope;
    use crate::fs::fsck::{FsckIssue, FsckResult, FsckStats, OrphanedEntry};

    fn sample_result() -> FsckResult {
        FsckResult {
            errors: vec![FsckIssue {
                code: "BadSignature".into(),
                message: "MDB signature mismatch".into(),
                repairable: true,
                debug: false,
            }],
            warnings: vec![FsckIssue {
                code: "MinorDrift".into(),
                message: "trivial counter delta".into(),
                repairable: false,
                debug: false,
            }],
            stats: FsckStats {
                files_checked: 7,
                directories_checked: 3,
                extra: vec![("volume".into(), "Macintosh HD".into())],
            },
            repairable: true,
            orphaned_entries: vec![OrphanedEntry {
                id: 42,
                name: "ghost".into(),
                is_directory: false,
                missing_parent_id: 100,
            }],
        }
    }

    #[test]
    fn json_payload_carries_clean_and_report() {
        let r = sample_result();
        let payload = FsckPayload {
            clean: r.is_clean(),
            report: &r,
        };
        let env = Envelope::ok(payload);
        let s = serde_json::to_string(&env).expect("serialize ok envelope");
        assert!(s.contains("\"schema_version\":1"));
        assert!(s.contains("\"error\":false"));
        assert!(s.contains("\"clean\":false"));
        assert!(s.contains("\"BadSignature\""));
        assert!(s.contains("\"MinorDrift\""));
        assert!(s.contains("\"files_checked\":7"));
        assert!(s.contains("\"orphaned_entries\""));
        assert!(s.contains("\"name\":\"ghost\""));
    }

    #[test]
    fn json_clean_volume_serializes_with_empty_lists() {
        let r = FsckResult {
            errors: vec![],
            warnings: vec![],
            stats: FsckStats {
                files_checked: 0,
                directories_checked: 0,
                extra: vec![],
            },
            repairable: false,
            orphaned_entries: vec![],
        };
        let payload = FsckPayload {
            clean: r.is_clean(),
            report: &r,
        };
        let s = serde_json::to_string(&Envelope::ok(payload)).unwrap();
        assert!(s.contains("\"clean\":true"));
        assert!(s.contains("\"errors\":[]"));
        assert!(s.contains("\"orphaned_entries\":[]"));
    }

    #[test]
    #[cfg(feature = "yaml")]
    fn yaml_envelope_renders_top_level_status_and_result() {
        let r = sample_result();
        let env = Envelope::ok(FsckPayload {
            clean: r.is_clean(),
            report: &r,
        });
        let s = serde_yml::to_string(&env).expect("serialize yaml");
        assert!(s.contains("schema_version: 1"));
        assert!(s.contains("status:"));
        assert!(s.contains("error: false"));
        assert!(s.contains("clean: false"));
        assert!(s.contains("code: BadSignature"));
    }

    #[test]
    fn require_non_flat_rejects_csv_and_tsv() {
        assert!(require_non_flat(OutputFormat::Csv, "fsck").is_err());
        assert!(require_non_flat(OutputFormat::Tsv, "fsck").is_err());
        assert!(require_non_flat(OutputFormat::Json, "fsck").is_ok());
        assert!(require_non_flat(OutputFormat::Yaml, "fsck").is_ok());
        assert!(require_non_flat(OutputFormat::Text, "fsck").is_ok());
    }

    #[test]
    fn unsupported_envelope_carries_exit_code() {
        let env: Envelope<FsckPayload<'_>> = Envelope::error(
            exit::GENERIC_FAILURE,
            "fsck not supported for this filesystem",
            None,
        );
        let s = serde_json::to_string(&env).unwrap();
        assert!(s.contains("\"error\":true"));
        assert!(s.contains(&format!("\"code\":{}", exit::GENERIC_FAILURE)));
        assert!(s.contains("\"result\":null"));
    }
}
