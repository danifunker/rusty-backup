//! Structured output for query verbs.
//!
//! The CLI's read-only verbs (`inspect`, `show partmap`, `show fs-info`,
//! `show chd-info`, `show devices`, `ls`, `fsck` report form) can emit
//! their results in one of five formats:
//!
//! - **text** (default) — human-readable tabular/paragraph form.
//! - **json** — pretty-printed JSON object with the schema below.
//! - **yaml** — same shape as JSON, serialized to YAML. Behind the `yaml`
//!   feature (on by default); the PowerPC mrustc build drops it because
//!   `serde_yml`'s backend hits an mrustc macro-expansion gap.
//! - **csv** — flat tabular outputs only.
//! - **tsv** — same scope as CSV.
//!
//! ## JSON/YAML schema
//!
//! All structured responses are wrapped in a top-level envelope:
//!
//! ```json
//! {
//!   "schema_version": 1,
//!   "status": {
//!     "error": false,
//!     "code": 0,
//!     "message": null
//!   },
//!   "result": { ... per-verb payload ... }
//! }
//! ```
//!
//! On errors, `status.error` is `true`, `status.code` carries the exit
//! code (see [`crate::cli::exit`]), `status.message` carries a short
//! human description, and `result` is `null` (or a partial payload for
//! verbs that can still emit useful data on failure).
//!
//! ## CSV/TSV scope
//!
//! These formats only apply to *flat* tabular results — `ls`, `show
//! partmap`, `show devices`, `fsck` issue lists. Nested-result verbs
//! (`inspect`, `show fs-info`, `show chd-info`) error out with
//! [`crate::cli::exit::USAGE_ERROR`] when CSV/TSV is requested.

use anyhow::Result;
use serde::Serialize;
use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};

/// Output format selected via `--format`. Default is [`OutputFormat::Text`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum OutputFormat {
    /// Human-readable text (default).
    #[default]
    Text,
    /// Pretty-printed JSON, status-wrapped.
    Json,
    /// YAML, same structure as JSON.
    ///
    /// Hidden from `--format` when the `yaml` feature is off (the PowerPC
    /// mrustc build; see docs/build-ppc-mrustc.md). The variant itself stays so
    /// every `Json | Yaml` match arm in the verbs keeps compiling - only the
    /// serializer and the CLI's acceptance of the value go away.
    #[cfg_attr(not(feature = "yaml"), value(skip))]
    Yaml,
    /// CSV. Flat tabular outputs only.
    Csv,
    /// TSV. Flat tabular outputs only.
    Tsv,
}

impl fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Text => "text",
            Self::Json => "json",
            Self::Yaml => "yaml",
            Self::Csv => "csv",
            Self::Tsv => "tsv",
        })
    }
}

impl OutputFormat {
    /// Whether this format carries nested data (JSON/YAML) or only flat
    /// tabular rows (text/CSV/TSV). Verbs with nested results check this
    /// before serializing.
    pub fn is_structured(self) -> bool {
        matches!(self, Self::Json | Self::Yaml)
    }

    /// Whether this format is flat-tabular only.
    pub fn is_flat_only(self) -> bool {
        matches!(self, Self::Csv | Self::Tsv)
    }

    fn as_u8(self) -> u8 {
        match self {
            Self::Text => 0,
            Self::Json => 1,
            Self::Yaml => 2,
            Self::Csv => 3,
            Self::Tsv => 4,
        }
    }

    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Text),
            1 => Some(Self::Json),
            2 => Some(Self::Yaml),
            3 => Some(Self::Csv),
            4 => Some(Self::Tsv),
            _ => None,
        }
    }
}

/// The `--format` the running verb was given, or `FORMAT_UNSET`.
///
/// `--format` is a per-verb argument, so the error path in `main` has no other
/// way to learn that the caller asked for JSON (R-005).
static ACTIVE_FORMAT: AtomicU8 = AtomicU8::new(FORMAT_UNSET);

const FORMAT_UNSET: u8 = u8::MAX;

/// Record the format the invoked verb parsed. Called once from the `rb-cli`
/// entry point, before dispatch.
pub fn record_active_format(format: Option<OutputFormat>) {
    ACTIVE_FORMAT.store(
        format.map_or(FORMAT_UNSET, OutputFormat::as_u8),
        Ordering::Relaxed,
    );
}

/// The recorded format, if the invoked verb has a `--format` at all.
pub fn active_format() -> Option<OutputFormat> {
    OutputFormat::from_u8(ACTIVE_FORMAT.load(Ordering::Relaxed))
}

/// Dig the `--format` value out of parsed clap matches, walking to the deepest
/// subcommand so `show partmap --format json` finds the inner verb's flag.
///
/// Reading clap's own matches rather than rescanning `argv` means a file named
/// `--format` can't be mistaken for the flag, and a verb added later is covered
/// without touching this function.
pub fn format_from_matches(matches: &clap::ArgMatches) -> Option<OutputFormat> {
    let mut level = matches;
    let mut found = None;
    loop {
        if let Ok(Some(f)) = level.try_get_one::<OutputFormat>("format") {
            found = Some(*f);
        }
        match level.subcommand() {
            Some((_, sub)) => level = sub,
            None => return found,
        }
    }
}

/// Emit an error envelope for a failure on its way out of `main`, when the
/// caller asked for a structured format. Returns whether anything was written.
pub fn emit_error_envelope_for(err: &anyhow::Error) -> bool {
    let Some(format) = active_format() else {
        return false;
    };
    if !format.is_structured() {
        return false;
    }
    let env: Envelope<()> =
        Envelope::error(crate::cli::exit::code_for(err), format!("{err:#}"), None);
    emit_envelope(format, &env).is_ok()
}

/// Top-level envelope for JSON/YAML payloads. Verbs construct one of
/// these via [`Envelope::ok`] or [`Envelope::error`] and pass it to
/// [`emit_envelope`].
#[derive(Debug, Serialize)]
pub struct Envelope<T: Serialize> {
    pub schema_version: u32,
    pub status: Status,
    pub result: Option<T>,
}

#[derive(Debug, Serialize)]
pub struct Status {
    pub error: bool,
    pub code: i32,
    pub message: Option<String>,
}

impl<T: Serialize> Envelope<T> {
    /// Build a success envelope with a `result` payload.
    pub fn ok(result: T) -> Self {
        Self {
            schema_version: 1,
            status: Status {
                error: false,
                code: 0,
                message: None,
            },
            result: Some(result),
        }
    }

    /// Build an error envelope. `result` may be `None` (typical) or a
    /// partial payload for verbs that produce useful data even on failure.
    pub fn error(code: i32, message: impl Into<String>, result: Option<T>) -> Self {
        Self {
            schema_version: 1,
            status: Status {
                error: true,
                code,
                message: Some(message.into()),
            },
            result,
        }
    }
}

/// Emit an envelope as JSON or YAML to stdout, followed by a trailing
/// newline. The caller decides whether to flush.
pub fn emit_envelope<T: Serialize>(format: OutputFormat, env: &Envelope<T>) -> Result<()> {
    match format {
        OutputFormat::Json => {
            let s = serde_json::to_string_pretty(env)?;
            println!("{s}");
            Ok(())
        }
        #[cfg(feature = "yaml")]
        OutputFormat::Yaml => {
            let s = serde_yml::to_string(env)?;
            // serde_yml already appends a trailing newline.
            print!("{s}");
            Ok(())
        }
        // Unreachable in practice - `value(skip)` stops clap accepting
        // `--format yaml` in this configuration - but a caller that builds the
        // variant directly deserves a straight answer rather than the
        // "non-structured format" message below.
        #[cfg(not(feature = "yaml"))]
        OutputFormat::Yaml => anyhow::bail!(
            "this build has no YAML output support (built without the `yaml` feature); \
             use --format json"
        ),
        // Text / CSV / TSV are emitted by per-verb code; this function is
        // only meant for structured envelopes.
        _ => anyhow::bail!(
            "internal: emit_envelope called with non-structured format {format}; \
             verbs must call the format-specific emitter directly"
        ),
    }
}

/// Emit flat rows as CSV or TSV, header included. Shared by every verb in the
/// flat-tabular scope (`ls`, `show partmap`, `show devices`, `fsck` issues).
pub fn emit_csv_or_tsv<T: Serialize>(format: OutputFormat, rows: &[T]) -> Result<()> {
    let delim = if format == OutputFormat::Tsv {
        b'\t'
    } else {
        b','
    };
    let mut wtr = csv::WriterBuilder::new()
        .delimiter(delim)
        .from_writer(std::io::stdout().lock());
    for row in rows {
        wtr.serialize(row)?;
    }
    wtr.flush()?;
    Ok(())
}

/// Reject `--format csv|tsv` for nested-result verbs. Verbs whose result
/// shape doesn't flatten into rows call this at the top of their
/// dispatcher; on error returns [`crate::cli::exit::USAGE_ERROR`] via
/// `anyhow`.
pub fn require_non_flat(format: OutputFormat, verb_name: &str) -> Result<()> {
    if format.is_flat_only() {
        // Don't advertise a format this build can't produce.
        #[cfg(feature = "yaml")]
        let suggestion = "Use --format json or --format yaml instead.";
        #[cfg(not(feature = "yaml"))]
        let suggestion = "Use --format json instead.";
        // A usage rejection, and it must exit 2 — this file has documented that
        // since it was written, while `anyhow::bail!` sent 1 (R-004).
        return Err(crate::cli::exit::usage(format!(
            "{verb_name} returns nested data; --format {format} only supports flat tabular \
             results. {suggestion}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ok_envelope_round_trips_via_json() {
        let env = Envelope::ok(serde_json::json!({"hello": "world"}));
        let s = serde_json::to_string(&env).unwrap();
        assert!(s.contains("\"schema_version\":1"));
        assert!(s.contains("\"error\":false"));
        assert!(s.contains("\"hello\":\"world\""));
    }

    #[test]
    fn error_envelope_keeps_code_and_message() {
        let env: Envelope<()> = Envelope::error(3, "not found", None);
        let s = serde_json::to_string(&env).unwrap();
        assert!(s.contains("\"error\":true"));
        assert!(s.contains("\"code\":3"));
        assert!(s.contains("\"message\":\"not found\""));
        assert!(s.contains("\"result\":null"));
    }

    #[test]
    #[cfg(feature = "yaml")]
    fn yaml_serializer_works() {
        let env = Envelope::ok(serde_json::json!({"a": 1}));
        let s = serde_yml::to_string(&env).unwrap();
        assert!(s.contains("schema_version: 1"));
        assert!(s.contains("error: false"));
    }

    #[test]
    fn structured_vs_flat_classification() {
        assert!(OutputFormat::Json.is_structured());
        assert!(OutputFormat::Yaml.is_structured());
        assert!(!OutputFormat::Csv.is_structured());
        assert!(OutputFormat::Csv.is_flat_only());
        assert!(OutputFormat::Tsv.is_flat_only());
        assert!(!OutputFormat::Text.is_flat_only());
        assert!(!OutputFormat::Json.is_flat_only());
    }

    #[test]
    fn require_non_flat_rejects_csv_for_nested() {
        // Asserting only `is_err()` is what let R-004 sit here unnoticed: the
        // message said usage error while the process exited 1.
        for f in [OutputFormat::Csv, OutputFormat::Tsv] {
            let e = require_non_flat(f, "inspect").expect_err("must reject");
            assert_eq!(
                crate::cli::exit::code_for(&e),
                crate::cli::exit::USAGE_ERROR,
                "{f} rejection must exit 2"
            );
        }
        assert!(require_non_flat(OutputFormat::Json, "inspect").is_ok());
        assert!(require_non_flat(OutputFormat::Text, "inspect").is_ok());
    }
}
