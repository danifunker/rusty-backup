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
use crate::cli::resolve::{resolve_partition_ro, resolve_partition_rw};
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
        return repair_mode(&args.image, args.format);
    }
    check_mode(&args.image, args.format)
}

fn check_mode(image: &ImageRef, format: OutputFormat) -> Result<()> {
    let (file, ctx) = resolve_partition_ro(&image.path, image.partition)?;
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

    if format.is_structured() {
        let payload = FsckPayload {
            clean: report.is_clean(),
            report: &report,
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

fn repair_mode(image: &ImageRef, format: OutputFormat) -> Result<()> {
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
        None => return fsck_unsupported(format),
    };

    if report.is_clean() {
        if format.is_structured() {
            let env = Envelope::ok(RepairPayload {
                clean_before: true,
                report: &report,
                repair: None,
            });
            emit_envelope(format, &env)?;
        } else {
            print_report(&report);
            out_stdout("fsck: clean, nothing to repair");
        }
        return Ok(());
    }
    if !report.repairable {
        if format.is_structured() {
            let env = Envelope::error(
                exit::GENERIC_FAILURE,
                "fsck: no repairable errors found",
                Some(RepairPayload {
                    clean_before: false,
                    report: &report,
                    repair: None,
                }),
            );
            emit_envelope(format, &env)?;
            bail!("fsck: no repairable errors found");
        }
        print_report(&report);
        bail!("fsck: no repairable errors found");
    }

    let repair = fs.repair().map_err(|e| anyhow!("repair: {e}"))?;

    if format.is_structured() {
        let env = Envelope::ok(RepairPayload {
            clean_before: false,
            report: &report,
            repair: Some(&repair),
        });
        emit_envelope(format, &env)?;
        return Ok(());
    }

    print_report(&report);
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
