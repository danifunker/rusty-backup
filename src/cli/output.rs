//! Structured output for query verbs.
//!
//! The CLI's read-only verbs (`inspect`, `show partmap`, `show fs-info`,
//! `show chd-info`, `show devices`, `ls`, `fsck` report form) can emit
//! their results in one of five formats:
//!
//! - **text** (default) — human-readable tabular/paragraph form.
//! - **json** — pretty-printed JSON object with the schema below.
//! - **yaml** — same shape as JSON, serialized to YAML.
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

/// Output format selected via `--format`. Default is [`OutputFormat::Text`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum OutputFormat {
    /// Human-readable text (default).
    #[default]
    Text,
    /// Pretty-printed JSON, status-wrapped.
    Json,
    /// YAML, same structure as JSON.
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
        OutputFormat::Yaml => {
            let s = serde_yml::to_string(env)?;
            // serde_yml already appends a trailing newline.
            print!("{s}");
            Ok(())
        }
        // Text / CSV / TSV are emitted by per-verb code; this function is
        // only meant for structured envelopes.
        _ => anyhow::bail!(
            "internal: emit_envelope called with non-structured format {format}; \
             verbs must call the format-specific emitter directly"
        ),
    }
}

/// Reject `--format csv|tsv` for nested-result verbs. Verbs whose result
/// shape doesn't flatten into rows call this at the top of their
/// dispatcher; on error returns [`crate::cli::exit::USAGE_ERROR`] via
/// `anyhow`.
pub fn require_non_flat(format: OutputFormat, verb_name: &str) -> Result<()> {
    if format.is_flat_only() {
        anyhow::bail!(
            "{verb_name} returns nested data; --format {format} only supports flat tabular \
             results. Use --format json or --format yaml instead."
        );
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
        assert!(require_non_flat(OutputFormat::Csv, "inspect").is_err());
        assert!(require_non_flat(OutputFormat::Tsv, "inspect").is_err());
        assert!(require_non_flat(OutputFormat::Json, "inspect").is_ok());
        assert!(require_non_flat(OutputFormat::Text, "inspect").is_ok());
    }
}
