//! Assertion evaluation.
//!
//! Every assertion produces either nothing (it held) or a [`FailedAssertion`]
//! carrying expected-vs-observed. Assertions never panic and never abort the
//! run; a step evaluates all of its assertions so one failure does not mask
//! the next.
//!
//! The operator set is deliberately closed — see COVERAGE.md § Assertion
//! vocabulary. A case that needs something outside it is a signal to add a
//! first-class operator here, not to smuggle in a shell escape.

use crate::envelope;
use crate::exec::Output;
use crate::manifest::Step;
use serde::Serialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Serialize)]
pub struct FailedAssertion {
    pub op: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selector: Option<String>,
    pub expected: String,
    pub observed: String,
}

impl FailedAssertion {
    fn new(op: &str, expected: impl Into<String>, observed: impl Into<String>) -> Self {
        FailedAssertion {
            op: op.to_string(),
            selector: None,
            expected: expected.into(),
            observed: observed.into(),
        }
    }

    fn with_selector(mut self, selector: &str) -> Self {
        self.selector = Some(selector.to_string());
        self
    }
}

/// Evaluate every assertion on `step` against `out`.
///
/// `resolve` expands `{scratch}` / `{fixture}` style placeholders in path
/// arguments; the caller owns the substitution table.
pub fn evaluate(
    step: &Step,
    out: &Output,
    resolve: &dyn Fn(&str) -> String,
) -> Vec<FailedAssertion> {
    let mut failures = Vec::new();

    if out.timed_out {
        failures.push(FailedAssertion::new(
            "timeout",
            "completes within budget",
            "killed after exceeding the wall-clock budget",
        ));
        // A killed process has no meaningful output; further assertions would
        // just produce noise on top of the real finding.
        return failures;
    }

    if let Some(expected) = step.expect_exit {
        let observed = out.exit_code;
        if observed != Some(expected) {
            failures.push(FailedAssertion::new(
                "exit",
                expected.to_string(),
                match observed {
                    Some(c) => c.to_string(),
                    None => "killed by signal".to_string(),
                },
            ));
        }
    }

    // Parse the envelope once if anything needs it.
    let needs_envelope = step.expect_envelope_ok.is_some()
        || !step.json_equals.is_empty()
        || !step.json_exists.is_empty()
        || step.fsck_clean;

    let parsed = if needs_envelope {
        match envelope::parse(&out.stdout) {
            Ok(e) => Some(e),
            Err(msg) => {
                failures.push(FailedAssertion::new(
                    "envelope",
                    "output parses as the rb-cli JSON envelope",
                    msg,
                ));
                None
            }
        }
    } else {
        None
    };

    if let (Some(want_ok), Some(env)) = (step.expect_envelope_ok, parsed.as_ref()) {
        let observed_ok = !env.status_error;
        if observed_ok != want_ok {
            failures.push(FailedAssertion::new(
                "envelope",
                format!("status.error == {}", !want_ok),
                format!(
                    "status.error == {} (code {}, message {:?})",
                    env.status_error, env.status_code, env.status_message
                ),
            ));
        }
    }

    for needle in &step.stdout_contains {
        let needle = resolve(needle);
        if !out.stdout.contains(&needle) {
            failures.push(FailedAssertion::new(
                "stdout_contains",
                needle,
                truncate(&out.stdout, 400),
            ));
        }
    }

    for pattern in &step.stdout_matches {
        match regex::Regex::new(pattern) {
            Ok(re) => {
                if !re.is_match(&out.stdout) {
                    failures.push(FailedAssertion::new(
                        "stdout_matches",
                        pattern.clone(),
                        truncate(&out.stdout, 400),
                    ));
                }
            }
            Err(e) => failures.push(FailedAssertion::new(
                "stdout_matches",
                pattern.clone(),
                format!("manifest bug: invalid regex: {}", e),
            )),
        }
    }

    if step.stderr_empty && !out.stderr.trim().is_empty() {
        failures.push(FailedAssertion::new(
            "stderr_empty",
            "empty stderr",
            truncate(&out.stderr, 400),
        ));
    }

    if let Some(env) = parsed.as_ref() {
        for eq in &step.json_equals {
            match envelope::select(&env.result, &eq.path) {
                Some(observed) => {
                    if observed != &eq.value {
                        failures.push(
                            FailedAssertion::new(
                                "json_equals",
                                eq.value.to_string(),
                                observed.to_string(),
                            )
                            .with_selector(&eq.path),
                        );
                    }
                }
                None => failures.push(
                    FailedAssertion::new("json_equals", eq.value.to_string(), "selector not found")
                        .with_selector(&eq.path),
                ),
            }
        }

        for path in &step.json_exists {
            if envelope::select(&env.result, path).is_none() {
                failures.push(
                    FailedAssertion::new("json_exists", "present", "selector not found")
                        .with_selector(path),
                );
            }
        }

        if step.fsck_clean {
            match envelope::fsck_clean_flag(&env.result) {
                Some(true) => {}
                Some(false) => failures.push(FailedAssertion::new(
                    "fsck_clean",
                    "result.clean == true",
                    format!(
                        "result.clean == false; report: {}",
                        truncate(
                            &envelope::select(&env.result, "report")
                                .map(|v| v.to_string())
                                .unwrap_or_else(|| "<absent>".to_string()),
                            600
                        )
                    ),
                )),
                None => failures.push(FailedAssertion::new(
                    "fsck_clean",
                    "a boolean `clean` in the fsck result payload",
                    "no `clean` flag found",
                )),
            }
        }
    }

    for raw in &step.files_exist {
        let path = resolve(raw);
        if !Path::new(&path).exists() {
            failures.push(FailedAssertion::new("file_exists", path, "missing"));
        }
    }

    for pair in &step.files_identical {
        if pair.len() != 2 {
            continue; // rejected at load time
        }
        let a = resolve(&pair[0]);
        let b = resolve(&pair[1]);
        match compare_files(Path::new(&a), Path::new(&b)) {
            Ok(true) => {}
            Ok(false) => failures.push(FailedAssertion::new(
                "files_identical",
                format!("{} == {}", a, b),
                "contents differ".to_string(),
            )),
            Err(e) => failures.push(FailedAssertion::new(
                "files_identical",
                format!("{} == {}", a, b),
                format!("could not compare: {}", e),
            )),
        }
    }

    for want in &step.file_sha256 {
        let path = resolve(&want.path);
        match sha256_file(Path::new(&path)) {
            Ok(got) => {
                if !got.eq_ignore_ascii_case(&want.sha256) {
                    failures.push(
                        FailedAssertion::new("file_sha256", want.sha256.clone(), got)
                            .with_selector(&path),
                    );
                }
            }
            Err(e) => failures.push(
                FailedAssertion::new("file_sha256", want.sha256.clone(), format!("unreadable: {}", e))
                    .with_selector(&path),
            ),
        }
    }

    failures
}

fn truncate(s: &str, max: usize) -> String {
    let trimmed = s.trim();
    if trimmed.chars().count() <= max {
        return trimmed.to_string();
    }
    let head: String = trimmed.chars().take(max).collect();
    format!("{}... [{} bytes total]", head, s.len())
}

/// Byte-compare two files. Sizes are checked first so a length mismatch is
/// cheap, then contents stream in blocks — these can be multi-gigabyte disk
/// images and must never be loaded whole.
fn compare_files(a: &Path, b: &Path) -> std::io::Result<bool> {
    use std::io::Read;

    let ma = fs::metadata(a)?;
    let mb = fs::metadata(b)?;
    if ma.len() != mb.len() {
        return Ok(false);
    }

    let mut fa = fs::File::open(a)?;
    let mut fb = fs::File::open(b)?;
    let mut ba = vec![0u8; 256 * 1024];
    let mut bb = vec![0u8; 256 * 1024];

    loop {
        let na = read_full(&mut fa, &mut ba)?;
        let nb = read_full(&mut fb, &mut bb)?;
        if na != nb {
            return Ok(false);
        }
        if na == 0 {
            return Ok(true);
        }
        if ba[..na] != bb[..nb] {
            return Ok(false);
        }
    }

    fn read_full(f: &mut fs::File, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut filled = 0;
        while filled < buf.len() {
            match f.read(&mut buf[filled..])? {
                0 => break,
                n => filled += n,
            }
        }
        Ok(filled)
    }
}

pub fn sha256_file(path: &Path) -> std::io::Result<String> {
    use sha2::{Digest, Sha256};
    use std::io::Read;

    let mut f = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}
