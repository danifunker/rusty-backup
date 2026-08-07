//! The `rb-cli` structured-output envelope.
//!
//! Every structured verb wraps its payload the same way (see
//! `src/cli/output.rs`):
//!
//! ```json
//! {
//!   "schema_version": 1,
//!   "status": { "error": false, "code": 0, "message": null },
//!   "result": { ... }
//! }
//! ```
//!
//! Validating this shape on every structured invocation the harness makes is
//! deliberate: an envelope regression then surfaces across the entire matrix
//! at once instead of hiding until someone happens to write a test for it.

use serde_json::Value;

#[derive(Debug)]
pub struct Parsed {
    /// Parsed and named because it is the envelope contract's version field;
    /// no assertion reads it yet.
    #[allow(dead_code)]
    pub schema_version: u64,
    pub status_error: bool,
    pub status_code: i64,
    pub status_message: Option<String>,
    pub result: Value,
}

/// The envelope version this harness understands. A bump upstream should be a
/// deliberate, visible event — every structured case reports it rather than
/// silently coping.
pub const EXPECTED_SCHEMA_VERSION: u64 = 1;

pub fn parse(stdout: &str) -> Result<Parsed, String> {
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        return Err("no output to parse".to_string());
    }

    let root: Value = serde_json::from_str(trimmed)
        .map_err(|e| format!("not valid JSON: {}", e))?;

    let obj = root
        .as_object()
        .ok_or_else(|| "top level is not a JSON object".to_string())?;

    let schema_version = obj
        .get("schema_version")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| "missing or non-numeric `schema_version`".to_string())?;

    if schema_version != EXPECTED_SCHEMA_VERSION {
        return Err(format!(
            "envelope schema_version is {}, this harness understands {}",
            schema_version, EXPECTED_SCHEMA_VERSION
        ));
    }

    let status = obj
        .get("status")
        .and_then(|v| v.as_object())
        .ok_or_else(|| "missing `status` object".to_string())?;

    let status_error = status
        .get("error")
        .and_then(|v| v.as_bool())
        .ok_or_else(|| "missing or non-boolean `status.error`".to_string())?;

    let status_code = status.get("code").and_then(|v| v.as_i64()).unwrap_or(0);

    let status_message = status
        .get("message")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // `result` is legitimately null on an error envelope, so its absence is
    // not itself a parse failure.
    let result = obj.get("result").cloned().unwrap_or(Value::Null);

    Ok(Parsed {
        schema_version,
        status_error,
        status_code,
        status_message,
        result,
    })
}

/// Resolve a dotted selector against a value.
///
/// Segments are object keys, except that an all-digits segment indexes an
/// array: `partitions.0.fs_type`. Deliberately not full JSONPath — the
/// manifests only ever need this much, and a small selector language is one
/// fewer thing that can be subtly wrong in a thousand case files.
pub fn select<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = root;
    for segment in path.split('.') {
        if segment.is_empty() {
            return None;
        }
        cur = if segment.bytes().all(|b| b.is_ascii_digit()) {
            let idx: usize = segment.parse().ok()?;
            cur.as_array()?.get(idx)?
        } else {
            cur.as_object()?.get(segment)?
        };
    }
    Some(cur)
}

/// Read the `clean` flag from an `fsck --format json` payload.
///
/// `FsckPayload` (src/cli/verbs/fsck.rs) exposes `clean` alongside the raw
/// report precisely so consumers can branch on one boolean instead of
/// re-deriving it from the error list. Use it.
///
/// Note the more robust assertion for a plain "is it clean" check is
/// `fsck --checkonly` with `expect_exit = 0`, which needs no JSON at all.
pub fn fsck_clean_flag(result: &Value) -> Option<bool> {
    select(result, "clean").and_then(|v| v.as_bool())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_well_formed_envelope() {
        let text = r#"{"schema_version":1,
                       "status":{"error":false,"code":0,"message":null},
                       "result":{"partitions":[{"fs_type":"FAT16"}]}}"#;
        let p = parse(text).expect("should parse");
        assert!(!p.status_error);
        assert_eq!(
            select(&p.result, "partitions.0.fs_type").and_then(|v| v.as_str()),
            Some("FAT16")
        );
    }

    #[test]
    fn rejects_an_unexpected_schema_version() {
        let text = r#"{"schema_version":2,"status":{"error":false,"code":0},"result":null}"#;
        assert!(parse(text).is_err());
    }

    #[test]
    fn missing_selector_is_none_not_panic() {
        let v: Value = serde_json::from_str(r#"{"a":{"b":[1,2]}}"#).unwrap();
        assert!(select(&v, "a.b.9").is_none());
        assert!(select(&v, "a.zzz").is_none());
        assert_eq!(select(&v, "a.b.1").and_then(|x| x.as_u64()), Some(2));
    }
}
