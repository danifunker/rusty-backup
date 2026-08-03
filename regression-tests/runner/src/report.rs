//! Verdicts and the run report bundle.
//!
//! Format is specified in REPORTING.md. Two properties matter most:
//!
//! 1. `results.jsonl` is appended as the run proceeds, so an interrupted run
//!    still leaves usable data. A first full regression is expected to be
//!    long and may well get killed.
//! 2. A failing case captures everything needed to reproduce it *without*
//!    the harness. If a reader has to reconstruct the command from the
//!    manifest, the capture is incomplete.

use crate::assertion::FailedAssertion;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Verdict {
    Pass,
    Fail,
    SkipFixture,
    SkipPlatform,
    SkipTool,
    SkipHardware,
    Error,
}

impl Verdict {
    pub fn label(self) -> &'static str {
        match self {
            Verdict::Pass => "pass",
            Verdict::Fail => "fail",
            Verdict::SkipFixture => "skip-fixture",
            Verdict::SkipPlatform => "skip-platform",
            Verdict::SkipTool => "skip-tool",
            Verdict::SkipHardware => "skip-hardware",
            Verdict::Error => "error",
        }
    }
}

#[derive(Debug, Serialize)]
pub struct CaseResult {
    pub case_id: String,
    pub group: String,
    pub tier: u8,
    pub verdict: Verdict,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fixture_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skip_reason: Option<String>,
    pub duration_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_step: Option<usize>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub failed_assertions: Vec<FailedAssertion>,
    pub platform: String,

    // --- provenance, stamped by Bundle::record ------------------------------
    //
    // Every line carries its own identity so that concatenating results from
    // six hosts is lossless: `cat regressions/*/results/*.jsonl` yields a file
    // where every verdict still knows which build and which run produced it.
    // Without these, a merged file is a pile of verdicts with no way to
    // separate this month's from last month's, or Windows' from the Mac's.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub run_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub git_sha: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub rb_version: String,
}

/// Identity of the thing under test, stamped onto every result line.
#[derive(Debug, Clone, Default)]
pub struct RunIdentity {
    pub run_id: String,
    pub git_sha: String,
    pub rb_version: String,
}

pub struct Bundle {
    pub dir: PathBuf,
    pub identity: RunIdentity,
    results: File,
    pub missing_fixtures: BTreeMap<String, Vec<String>>,
    pub tool_skips: BTreeMap<String, Vec<String>>,
    pub results_seen: Vec<(String, String, u8, Verdict)>,
}

impl Bundle {
    pub fn create(
        report_root: &Path,
        host: &str,
        platform: &str,
        stamp: &str,
        identity: RunIdentity,
    ) -> std::io::Result<Bundle> {
        let dir = report_root.join(format!("{}-{}-{}", stamp, host, platform));
        fs::create_dir_all(dir.join("failures"))?;
        fs::create_dir_all(dir.join("checklists"))?;

        let results = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("results.jsonl"))?;

        Ok(Bundle {
            dir,
            identity,
            results,
            missing_fixtures: BTreeMap::new(),
            tool_skips: BTreeMap::new(),
            results_seen: Vec::new(),
        })
    }

    pub fn record(&mut self, r: &CaseResult) -> std::io::Result<()> {
        // Stamp provenance here rather than at every construction site, so a
        // new result type cannot forget it.
        let mut r = CaseResult {
            run_id: self.identity.run_id.clone(),
            git_sha: self.identity.git_sha.clone(),
            rb_version: self.identity.rb_version.clone(),
            ..clone_result(r)
        };
        let r = &mut r;
        let line = serde_json::to_string(r)
            .unwrap_or_else(|e| format!("{{\"case_id\":\"{}\",\"serialize_error\":\"{}\"}}", r.case_id, e));
        writeln!(self.results, "{}", line)?;
        self.results.flush()?;
        self.results_seen
            .push((r.case_id.clone(), r.group.clone(), r.tier, r.verdict));
        Ok(())
    }

    pub fn note_missing_fixture(&mut self, fixture_id: &str, case_id: &str) {
        self.missing_fixtures
            .entry(fixture_id.to_string())
            .or_default()
            .push(case_id.to_string());
    }

    pub fn note_tool_skip(&mut self, tool: &str, case_id: &str) {
        self.tool_skips
            .entry(tool.to_string())
            .or_default()
            .push(case_id.to_string());
    }

    pub fn failure_dir(&self, case_id: &str) -> PathBuf {
        self.dir.join("failures").join(sanitise(case_id))
    }

    /// Write the reproduction bundle for one failing case. Everything here
    /// exists so a human can reproduce without the harness.
    #[allow(clippy::too_many_arguments)]
    pub fn write_failure(
        &self,
        case_id: &str,
        rb_cli: &Path,
        args: &[String],
        cwd: &Path,
        exit_code: Option<i32>,
        expected_exit: Option<i32>,
        stdout: &str,
        stderr: &str,
        assertions: &[FailedAssertion],
        fixture: Option<(&str, &Path)>,
    ) -> std::io::Result<()> {
        let dir = self.failure_dir(case_id);
        fs::create_dir_all(&dir)?;

        // One argument per line so paths containing spaces stay unambiguous,
        // with a ready-to-paste single line underneath.
        let mut cmd = String::new();
        cmd.push_str("# argv, one element per line\n");
        cmd.push_str(&format!("{}\n", rb_cli.display()));
        for a in args {
            cmd.push_str(&format!("{}\n", a));
        }
        cmd.push_str("\n# working directory\n");
        cmd.push_str(&format!("{}\n", cwd.display()));
        cmd.push_str("\n# copy-paste form\n");
        cmd.push_str(&format!("\"{}\"", rb_cli.display()));
        for a in args {
            cmd.push_str(&format!(" \"{}\"", a));
        }
        cmd.push('\n');
        fs::write(dir.join("cmd.txt"), cmd)?;

        fs::write(
            dir.join("exit.txt"),
            format!(
                "observed: {}\nexpected: {}\n",
                exit_code.map(|c| c.to_string()).unwrap_or_else(|| "killed".into()),
                expected_exit.map(|c| c.to_string()).unwrap_or_else(|| "(unasserted)".into()),
            ),
        )?;
        fs::write(dir.join("stdout.txt"), stdout)?;
        fs::write(dir.join("stderr.txt"), stderr)?;
        fs::write(
            dir.join("assertions.json"),
            serde_json::to_string_pretty(assertions).unwrap_or_else(|_| "[]".into()),
        )?;

        if let Some((id, path)) = fixture {
            // Record the fixture by identity, never by copying it — the
            // corpus already holds the bytes.
            fs::write(
                dir.join("fixture.txt"),
                format!("id:   {}\npath: {}\n", id, path.display()),
            )?;
        }

        Ok(())
    }

    pub fn write_env(&self, env: &serde_json::Value) -> std::io::Result<()> {
        fs::write(
            self.dir.join("env.json"),
            serde_json::to_string_pretty(env).unwrap_or_else(|_| "{}".into()),
        )
    }

    pub fn write_missing_fixtures(&self) -> std::io::Result<()> {
        let mut s = String::from("# Missing fixtures\n\n");
        if self.missing_fixtures.is_empty() {
            s.push_str("Every fixture referenced by a case in this run resolved.\n");
        } else {
            s.push_str(
                "Fixture IDs referenced by at least one case but not resolvable on this\n\
                 host. This is the shopping list, not a failure. See FIXTURES.md.\n\n",
            );
            for (fixture, cases) in &self.missing_fixtures {
                s.push_str(&format!("## `{}`\n\n", fixture));
                s.push_str(&format!("Blocks {} case(s):\n\n", cases.len()));
                for c in cases {
                    s.push_str(&format!("- {}\n", c));
                }
                s.push('\n');
            }
        }
        fs::write(self.dir.join("missing-fixtures.md"), s)
    }

    pub fn write_tool_skips(&self) -> std::io::Result<()> {
        let mut s = String::from("# Absent external tools\n\n");
        if self.tool_skips.is_empty() {
            s.push_str("Every external oracle a case asked for was available.\n");
        } else {
            for (tool, cases) in &self.tool_skips {
                s.push_str(&format!("## `{}` — {} case(s) skipped\n\n", tool, cases.len()));
                for c in cases {
                    s.push_str(&format!("- {}\n", c));
                }
                s.push('\n');
            }
        }
        fs::write(self.dir.join("oracle-skips.md"), s)
    }

    /// The top sheet. Ordered for triage, not for completeness — see
    /// REPORTING.md.
    pub fn write_summary(&self, env: &serde_json::Value, wall_ms: u128) -> std::io::Result<()> {
        let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();
        for (_, _, _, v) in &self.results_seen {
            *counts.entry(v.label()).or_insert(0) += 1;
        }

        let mut s = String::new();
        s.push_str("# Regression run summary\n\n");
        s.push_str(&format!(
            "- platform: `{}`\n- host: `{}`\n- rb-cli: `{}`\n- wall clock: {:.1}s\n- cases: {}\n\n",
            env.get("platform").and_then(|v| v.as_str()).unwrap_or("?"),
            env.get("host").and_then(|v| v.as_str()).unwrap_or("?"),
            env.get("rb_cli_version").and_then(|v| v.as_str()).unwrap_or("?"),
            wall_ms as f64 / 1000.0,
            self.results_seen.len(),
        ));

        s.push_str("## Verdicts\n\n| verdict | count |\n|---------|------:|\n");
        for (label, n) in &counts {
            s.push_str(&format!("| {} | {} |\n", label, n));
        }
        s.push('\n');

        // Cluster failures by group. Forty failures that all say "exFAT" are
        // one bug, and the summary should say so rather than making a human
        // notice it.
        let mut by_group: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for (id, group, _, v) in &self.results_seen {
            if *v == Verdict::Fail || *v == Verdict::Error {
                by_group.entry(group.as_str()).or_default().push(id.as_str());
            }
        }

        s.push_str("## Failures by group\n\n");
        if by_group.is_empty() {
            s.push_str("No failures.\n\n");
        } else {
            s.push_str("Clustered so a single root cause reads as one entry.\n\n");
            let mut groups: Vec<_> = by_group.iter().collect();
            groups.sort_by(|a, b| b.1.len().cmp(&a.1.len()));
            for (group, ids) in groups {
                s.push_str(&format!("### `{}` — {} failing\n\n", group, ids.len()));
                for id in ids {
                    s.push_str(&format!("- `{}` -> `failures/{}/`\n", id, sanitise(id)));
                }
                s.push('\n');
            }
        }

        s.push_str("## Skips\n\n");
        s.push_str(&format!(
            "- fixture gaps: {} distinct IDs (see `missing-fixtures.md`)\n",
            self.missing_fixtures.len()
        ));
        s.push_str(&format!(
            "- absent oracles: {} tools (see `oracle-skips.md`)\n\n",
            self.tool_skips.len()
        ));

        s.push_str("## Next\n\n");
        s.push_str(
            "Triage per REPORTING.md: read the clustered failures above, reproduce one\n\
             representative per cluster from its `cmd.txt`, and promote confirmed bugs to\n\
             issues by hand.\n",
        );

        fs::write(self.dir.join("summary.md"), s)
    }
}

/// Case IDs are dotted and safe, but never trust one into a path.
fn sanitise(id: &str) -> String {
    id.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// `CaseResult` holds a `Vec<FailedAssertion>`, which is not `Clone`, so the
/// stamping step rebuilds it field by field rather than deriving `Clone` on a
/// type that is otherwise move-only.
fn clone_result(r: &CaseResult) -> CaseResult {
    CaseResult {
        case_id: r.case_id.clone(),
        group: r.group.clone(),
        tier: r.tier,
        verdict: r.verdict,
        fixture_id: r.fixture_id.clone(),
        skip_reason: r.skip_reason.clone(),
        duration_ms: r.duration_ms,
        failed_step: r.failed_step,
        failed_assertions: r
            .failed_assertions
            .iter()
            .map(|a| FailedAssertion {
                op: a.op.clone(),
                selector: a.selector.clone(),
                expected: a.expected.clone(),
                observed: a.observed.clone(),
            })
            .collect(),
        platform: r.platform.clone(),
        run_id: r.run_id.clone(),
        git_sha: r.git_sha.clone(),
        rb_version: r.rb_version.clone(),
    }
}
