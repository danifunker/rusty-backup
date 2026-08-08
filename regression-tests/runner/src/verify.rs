//! `verify` — hand produced artifacts to third-party tools and record what
//! they say.
//!
//! This is the half of the split that is OS-specific. It walks the artifact
//! tree as a **queue** and does not care which OS produced what: a Mac verifies
//! Windows-produced HFS, a Linux box verifies Mac-produced ext4, and nothing
//! coordinates. Whatever this host has no oracle for is recorded with a reason,
//! not dropped — a verification tree that silently omits what it could not
//! check reads exactly like one where everything passed.
//!
//! What an oracle *is* comes from `oracles.toml`. What it *runs* comes from the
//! `check` field on each `verifies` row, which is to `oracles.toml` what
//! `produce.toml` is to `formats.toml`: the registry knows a tool proves a
//! format, the check knows the command. `evidence` strings like
//! `qemu-img info -> raw` describe a check; they are not one.
//!
//! Only `package` and `mount` oracles can be automated. The emulator, MiSTer
//! and round-trip oracles need a preconfigured guest or real hardware and
//! resolve to `skip-manual` — 43 of the 93 declared pairs. That is a real
//! ceiling on what this command can ever claim, so it is reported rather than
//! buried.

use serde::Serialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::exec;
use crate::produce::ArtifactMeta;
use crate::registry::Registry;

const VERIFY_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, Serialize, PartialEq)]
pub enum Verdict {
    /// The oracle ran and agreed.
    Pass,
    /// The oracle ran and disagreed. This is a finding.
    Fail { reason: String },
    /// The oracle is not on this host.
    SkipUnavailable { reason: String },
    /// The oracle needs an emulator, a MiSTer core or a full round-trip.
    SkipManual { reason: String },
    /// The (oracle, format) pair has no `check` in oracles.toml yet.
    SkipNoCheck,
    /// The check could not be launched, or timed out.
    Error { reason: String },
}

impl Verdict {
    fn label(&self) -> &'static str {
        match self {
            Verdict::Pass => "pass",
            Verdict::Fail { .. } => "FAIL",
            Verdict::SkipUnavailable { .. } => "skip-unavailable",
            Verdict::SkipManual { .. } => "skip-manual",
            Verdict::SkipNoCheck => "skip-no-check",
            Verdict::Error { .. } => "error",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Record {
    pub format: String,
    pub producer_os: String,
    pub producer_host: String,
    pub verifier_os: String,
    pub verifier_host: String,
    pub oracle: String,
    pub strength: String,
    /// The artifact's sha256 as recorded by `produce`, so a verdict can be tied
    /// to the exact bytes that were checked.
    pub artifact_sha256: String,
    pub argv: Vec<String>,
    pub verdict: Verdict,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stdout_head: Option<String>,
}

pub struct Report {
    pub records: Vec<Record>,
    /// Formats present in the artifact tree that no oracle claims at all.
    pub unclaimed: Vec<String>,
}

/// Resolve an oracle's executable on this host.
///
/// PATH first, then the `path_hint` recorded for this platform — qemu-img and
/// chdman are both installed here but neither is on PATH, and treating that as
/// "oracle absent" would silently drop the two best container checks we have.
fn resolve_program(
    reg: &Registry,
    oracle_id: &str,
    program: &str,
    platform: &str,
    regression_dir: &Path,
) -> Option<PathBuf> {
    // Platform availability first. Without this, a repo-shipped helper script
    // resolves on every host that has the checkout — so Windows found
    // oracles/fsck_hfs_image.sh, tried to execute a shell script, and produced
    // nine `error` records where the honest answer is "this oracle is macOS
    // only". Errors that should be skips train people to ignore the report.
    if reg
        .availability
        .iter()
        .any(|a| a.oracle == oracle_id && a.platform == platform && a.status == "absent")
    {
        return None;
    }

    // A repo-relative helper, e.g. "oracles/fsck_hfs_image.sh". Some oracles
    // need setup and teardown around the actual check — fsck_hfs wants a block
    // device, so its image has to be attached and detached — and a shipped
    // script keeps that reviewable and versioned instead of buried in a TOML
    // string. Checked first so the repo's copy always wins over a stray one.
    if program.contains('/') && !Path::new(program).is_absolute() {
        let local = regression_dir.join(program);
        if local.is_file() {
            return Some(local);
        }
    }
    if exec::tool_available(program) {
        return Some(PathBuf::from(program));
    }
    for a in reg.availability.iter().filter(|a| a.oracle == oracle_id) {
        if a.platform != platform {
            continue;
        }
        let hint = match &a.path_hint {
            Some(h) => h,
            None => continue,
        };
        // A hint may name a directory ("C:/Tools/chdman") or the binary itself.
        let base = PathBuf::from(hint);
        let exts: &[&str] = if cfg!(windows) { &["", ".exe"] } else { &[""] };
        for ext in exts {
            let direct = PathBuf::from(format!("{}{}", base.display(), ext));
            if direct.is_file() {
                return Some(direct);
            }
            let inside = base.join(format!("{}{}", program, ext));
            if inside.is_file() {
                return Some(inside);
            }
        }
    }
    None
}

fn expand(args: &[String], artifact: &Path) -> Vec<String> {
    let dir = artifact
        .parent()
        .map(|p| p.display().to_string())
        .unwrap_or_default();
    args.iter()
        .map(|a| {
            a.replace("{artifact}", &artifact.display().to_string())
                .replace("{dir}", &dir)
        })
        .collect()
}

/// Every artifact in the tree, across all producer OSes.
fn walk_artifacts(root: &Path) -> Vec<(ArtifactMeta, PathBuf)> {
    let mut out = Vec::new();
    let os_dirs = match fs::read_dir(root) {
        Ok(e) => e,
        Err(_) => return out,
    };
    for os_entry in os_dirs.flatten() {
        if !os_entry.path().is_dir() {
            continue;
        }
        let fmt_dirs = match fs::read_dir(os_entry.path()) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for f in fmt_dirs.flatten() {
            let dir = f.path();
            if !dir.is_dir() {
                continue;
            }
            let text = match fs::read_to_string(dir.join("meta.json")) {
                Ok(t) => t,
                Err(_) => continue,
            };
            if let Ok(meta) = serde_json::from_str::<ArtifactMeta>(&text) {
                let image = dir.join(&meta.file);
                if image.is_file() {
                    out.push((meta, image));
                }
            }
        }
    }
    out
}

#[allow(clippy::too_many_arguments)]
pub fn verify(
    reg: &Registry,
    artifacts_root: &Path,
    out_dir: &Path,
    verifier_host: &str,
    filter: Option<&str>,
    regression_dir: &Path,
) -> Result<Report, String> {
    let platform = exec::platform_token();
    fs::create_dir_all(out_dir).map_err(|e| format!("{}: {}", out_dir.display(), e))?;

    let artifacts_root = &exec::absolutise(artifacts_root);
    let artifacts = walk_artifacts(artifacts_root);
    if artifacts.is_empty() {
        return Err(format!(
            "no artifacts under {} — run `produce` first",
            artifacts_root.display()
        ));
    }

    let oracles: BTreeMap<&str, &crate::registry::Oracle> =
        reg.oracles.iter().map(|o| (o.id.as_str(), o)).collect();

    let mut records = Vec::new();
    let mut unclaimed = Vec::new();

    for (meta, image) in &artifacts {
        if let Some(f) = filter {
            if !meta.format.contains(f) {
                continue;
            }
        }
        // Only write-direction rows: this artifact is something rb-cli wrote,
        // so a read-direction oracle row says nothing about it.
        let claims: Vec<&crate::registry::Verification> = reg
            .verifications
            .iter()
            .filter(|v| v.format == meta.format && v.direction == "write")
            .collect();

        if claims.is_empty() {
            if !unclaimed.contains(&meta.format) {
                unclaimed.push(meta.format.clone());
            }
            continue;
        }

        for claim in claims {
            let oracle = match oracles.get(claim.oracle.as_str()) {
                Some(o) => *o,
                None => continue,
            };
            let program = oracle.program.as_deref().unwrap_or(&oracle.tool);

            let (verdict, argv, stdout_head) = if !matches!(
                oracle.kind.as_str(),
                "package" | "mount"
            ) {
                (
                    Verdict::SkipManual {
                        reason: format!(
                            "{} is a {} oracle — needs a preconfigured guest or real hardware",
                            oracle.id, oracle.kind
                        ),
                    },
                    Vec::new(),
                    None,
                )
            } else if claim.check.is_none() {
                (Verdict::SkipNoCheck, Vec::new(), None)
            } else {
                match resolve_program(reg, &oracle.id, program, platform, regression_dir) {
                    None => (
                        Verdict::SkipUnavailable {
                            reason: format!("{} not on PATH or at its path_hint", program),
                        },
                        Vec::new(),
                        None,
                    ),
                    Some(exe) => {
                        let argv = expand(claim.check.as_ref().unwrap(), image);
                        let cwd = image.parent().unwrap_or(artifacts_root);
                        match exec::run(&exe, &argv, cwd, VERIFY_TIMEOUT) {
                            Err(e) => (
                                Verdict::Error {
                                    reason: e.to_string(),
                                },
                                argv,
                                None,
                            ),
                            Ok(r) if r.timed_out => (
                                Verdict::Error {
                                    reason: "timed out".to_string(),
                                },
                                argv,
                                None,
                            ),
                            Ok(r) => {
                                let head = head_of(&r.stdout, &r.stderr);
                                (evaluate(claim, &r), argv, Some(head))
                            }
                        }
                    }
                }
            };

            records.push(Record {
                format: meta.format.clone(),
                producer_os: meta.producer_os.clone(),
                producer_host: meta.producer_host.clone(),
                verifier_os: platform.to_string(),
                verifier_host: verifier_host.to_string(),
                oracle: oracle.id.clone(),
                strength: claim.strength.clone(),
                artifact_sha256: meta.sha256.clone(),
                argv,
                verdict,
                stdout_head,
            });
        }
    }

    // One file per (format, producer). A verifier writes only into its own
    // directory, so several hosts can fill one tree with no locking.
    let mut by_key: BTreeMap<String, Vec<&Record>> = BTreeMap::new();
    for r in &records {
        by_key
            .entry(format!("{}.{}", r.format, r.producer_os))
            .or_default()
            .push(r);
    }
    for (key, rs) in by_key {
        let json = serde_json::to_string_pretty(&rs).map_err(|e| e.to_string())?;
        fs::write(out_dir.join(format!("{}.json", key)), json).map_err(|e| e.to_string())?;
    }

    Ok(Report { records, unclaimed })
}

/// Compare a check's output against what the row said to expect. An expectation
/// that was not stated is not checked — a `check` with no expectations at all
/// is an exit-code check, which is the common case.
fn evaluate(claim: &crate::registry::Verification, r: &exec::Output) -> Verdict {
    let want_exit = claim.expect_exit.unwrap_or(0);
    if r.exit_code != Some(want_exit) {
        return Verdict::Fail {
            reason: format!(
                "exit {:?}, expected {}: {}",
                r.exit_code,
                want_exit,
                first_line(&r.stderr, &r.stdout)
            ),
        };
    }
    // Tools differ on which stream carries the verdict, so match either.
    let combined = format!("{}\n{}", r.stdout, r.stderr);
    if let Some(want) = &claim.expect_stdout {
        if !combined.contains(want.as_str()) {
            return Verdict::Fail {
                reason: format!("output did not contain {:?}", want),
            };
        }
    }
    if let Some(unwanted) = &claim.expect_stdout_not {
        if combined.contains(unwanted.as_str()) {
            return Verdict::Fail {
                reason: format!("output contained {:?}", unwanted),
            };
        }
    }
    Verdict::Pass
}

fn first_line(stderr: &str, stdout: &str) -> String {
    for s in [stderr, stdout] {
        if let Some(l) = s.lines().find(|l| !l.trim().is_empty()) {
            return l.trim().chars().take(160).collect();
        }
    }
    String::new()
}

fn head_of(stdout: &str, stderr: &str) -> String {
    let combined = format!("{}\n{}", stdout.trim(), stderr.trim());
    combined.trim().chars().take(400).collect()
}

pub fn render(report: &Report, out_dir: &Path) -> String {
    let mut s = String::new();
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();

    for r in &report.records {
        *counts.entry(r.verdict.label()).or_insert(0) += 1;
        // Skips are counted, not listed one by one — there are dozens and they
        // are the expected state, not news. Failures always print.
        match &r.verdict {
            Verdict::Fail { reason } => s.push_str(&format!(
                "FAIL  {:<20} {:<14} via {:<12} {}\n",
                r.format, r.producer_os, r.oracle, reason
            )),
            Verdict::Error { reason } => s.push_str(&format!(
                "error {:<20} {:<14} via {:<12} {}\n",
                r.format, r.producer_os, r.oracle, reason
            )),
            Verdict::Pass => s.push_str(&format!(
                "pass  {:<20} {:<14} via {:<12} ({})\n",
                r.format, r.producer_os, r.oracle, r.strength
            )),
            _ => {}
        }
    }

    s.push('\n');
    for (label, n) in &counts {
        s.push_str(&format!("{:<18} {}\n", label, n));
    }
    s.push_str(&format!("\nverifications: {}\n", out_dir.display()));

    // The ceiling, restated every run: a tree full of passes still says nothing
    // about the formats nobody claims.
    if !report.unclaimed.is_empty() {
        s.push_str(&format!(
            "\n{} produced format(s) that NO oracle claims — nothing can verify these:\n",
            report.unclaimed.len()
        ));
        for f in &report.unclaimed {
            s.push_str(&format!("  {}\n", f));
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::Verification;

    fn claim(exit: Option<i32>, want: Option<&str>, not: Option<&str>) -> Verification {
        Verification {
            oracle: "o".into(),
            format: "f".into(),
            direction: "write".into(),
            strength: "structural".into(),
            status: "proven".into(),
            evidence: None,
            check: Some(vec![]),
            expect_exit: exit,
            expect_stdout: want.map(|s| s.to_string()),
            expect_stdout_not: not.map(|s| s.to_string()),
        }
    }

    fn out(code: i32, stdout: &str, stderr: &str) -> exec::Output {
        exec::Output {
            exit_code: Some(code),
            stdout: stdout.into(),
            stderr: stderr.into(),
            duration: Duration::from_secs(0),
            timed_out: false,
        }
    }

    #[test]
    fn exit_zero_with_no_expectations_passes() {
        assert_eq!(evaluate(&claim(None, None, None), &out(0, "", "")), Verdict::Pass);
    }

    #[test]
    fn nonzero_exit_fails() {
        assert!(matches!(
            evaluate(&claim(None, None, None), &out(1, "", "boom")),
            Verdict::Fail { .. }
        ));
    }

    #[test]
    fn expected_text_is_matched_on_either_stream() {
        // fsck tools routinely put the verdict on stderr; requiring stdout
        // would fail a clean check.
        let c = claim(None, Some("No errors were found"), None);
        assert_eq!(
            evaluate(&c, &out(0, "", "No errors were found on the image")),
            Verdict::Pass
        );
    }

    #[test]
    fn forbidden_text_fails_even_on_exit_zero() {
        // The case this exists for: a tool that reports corruption and still
        // exits 0. Exit code alone would call that a pass.
        let c = claim(None, None, Some("corrupt"));
        assert!(matches!(
            evaluate(&c, &out(0, "image is corrupt", "")),
            Verdict::Fail { .. }
        ));
    }
}
