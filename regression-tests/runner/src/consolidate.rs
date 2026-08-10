//! Consolidation — how far has a regression actually got?
//!
//! A regression is not one run. It is spread across six hosts and, in
//! practice, several days: the Mac gets borrowed, the MiSTer gets used for
//! actual gaming, the Linux box is up whenever it is up. So the question
//! "how far are we?" has to be answerable from partial, asynchronous results.
//!
//! The mechanism is deliberately the dumbest thing that works: **concatenate
//! the `results.jsonl` files**. That only works because every line carries its
//! own provenance (`run_id`, `git_sha`, `rb_version`) — see `report.rs`. With
//! that, merging is `cat` and every rollup is a group-by. No results
//! database, no merge protocol, no coordination.
//!
//! The build sha matters more than it looks. Consolidating results from two
//! different builds produces a number that describes no program that ever
//! existed, so results are grouped by sha and a mixed set is called out rather
//! than silently averaged.

use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
struct Line {
    case_id: String,
    #[serde(default)]
    group: String,
    #[serde(default)]
    tier: u8,
    verdict: String,
    #[serde(default)]
    platform: String,
    #[serde(default)]
    run_id: String,
    #[serde(default)]
    git_sha: String,
    #[serde(default)]
    rb_version: String,
}

#[derive(Debug, Default)]
pub struct Consolidated {
    pub lines: usize,
    pub files: usize,
    pub by_verdict: BTreeMap<String, usize>,
    pub by_platform: BTreeMap<String, BTreeMap<String, usize>>,
    pub by_group: BTreeMap<String, BTreeMap<String, usize>>,
    pub shas: BTreeSet<String>,
    pub builds: BTreeSet<String>,
    pub runs: BTreeSet<String>,
    pub cases: BTreeSet<String>,
    /// Cases that failed, with the platforms they failed on — the triage list.
    pub failures: BTreeMap<String, BTreeSet<String>>,
    /// (case, platform, verdict, run_id) for every line, so coverage skew can
    /// be computed from each platform's *latest* run rather than from all of
    /// history at once.
    seen: Vec<(String, String, String, String)>,
    pub unstamped: usize,
}

/// A case that ran somewhere and was skipped for a missing fixture elsewhere.
#[derive(Debug)]
pub struct CoverageSkew {
    pub case_id: String,
    pub skipped_on: BTreeSet<String>,
    pub ran_on: BTreeSet<String>,
}

impl Consolidated {
    /// Cases covered on some hosts and skipped on others for want of a fixture.
    ///
    /// Invisible in the pass/fail columns: an unresolved fixture is
    /// `skip-fixture`, not a failure, so a host missing part of the corpus
    /// still reports zero failures — just a quietly smaller pass count. Three
    /// hosts sat at 257/257/259 that way, all reporting 0 fail.
    ///
    /// Computed from each platform's **latest run only**. `consolidate` reads
    /// every `results.jsonl` ever written, so comparing across all of history
    /// reports skew that was fixed weeks ago — the first version of this did
    /// exactly that, naming eleven optical cases as missing on Windows when
    /// Windows had had them for weeks. Run ids are timestamp-prefixed, so the
    /// lexical maximum per platform is that platform's most recent run.
    pub fn coverage_skew(&self) -> Vec<CoverageSkew> {
        let mut latest: BTreeMap<&str, &str> = BTreeMap::new();
        for (_, platform, _, run_id) in &self.seen {
            if run_id.is_empty() {
                continue;
            }
            let e = latest.entry(platform.as_str()).or_insert(run_id.as_str());
            if run_id.as_str() > *e {
                *e = run_id.as_str();
            }
        }

        let mut ran_on: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
        let mut skipped_on: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
        for (case, platform, verdict, run_id) in &self.seen {
            if latest.get(platform.as_str()) != Some(&run_id.as_str()) {
                continue;
            }
            ran_on
                .entry(case.as_str())
                .or_default()
                .insert(platform.as_str());
            if verdict == "skip-fixture" {
                skipped_on
                    .entry(case.as_str())
                    .or_default()
                    .insert(platform.as_str());
            }
        }

        let mut out = Vec::new();
        for (case_id, skipped) in &skipped_on {
            let ran: BTreeSet<String> = ran_on
                .get(case_id)
                .map(|all| {
                    all.difference(skipped)
                        .map(|s| (*s).to_string())
                        .collect()
                })
                .unwrap_or_default();
            if !ran.is_empty() {
                out.push(CoverageSkew {
                    case_id: (*case_id).to_string(),
                    skipped_on: skipped.iter().map(|s| (*s).to_string()).collect(),
                    ran_on: ran,
                });
            }
        }
        out
    }
}

/// Find every `results.jsonl` under `root`, at any depth.
fn find_results(root: &Path, out: &mut Vec<PathBuf>) {
    let entries = match fs::read_dir(root) {
        Ok(e) => e,
        Err(_) => return,
    };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            find_results(&p, out);
        } else if p.file_name().map(|n| n == "results.jsonl").unwrap_or(false) {
            out.push(p);
        }
    }
}

pub fn consolidate(root: &Path) -> Result<Consolidated, String> {
    let mut files = Vec::new();
    find_results(root, &mut files);
    if files.is_empty() {
        return Err(format!("no results.jsonl found under {}", root.display()));
    }

    let mut c = Consolidated {
        files: files.len(),
        ..Default::default()
    };

    for f in &files {
        let text = match fs::read_to_string(f) {
            Ok(t) => t,
            Err(_) => continue,
        };
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let l: Line = match serde_json::from_str(line) {
                Ok(l) => l,
                Err(_) => continue,
            };
            c.lines += 1;
            *c.by_verdict.entry(l.verdict.clone()).or_insert(0) += 1;
            *c.by_platform
                .entry(if l.platform.is_empty() { "?".into() } else { l.platform.clone() })
                .or_default()
                .entry(l.verdict.clone())
                .or_insert(0) += 1;
            *c.by_group
                .entry(if l.group.is_empty() { format!("tier{}", l.tier) } else { l.group.clone() })
                .or_default()
                .entry(l.verdict.clone())
                .or_insert(0) += 1;
            c.cases.insert(l.case_id.clone());

            if l.git_sha.is_empty() {
                // Written before provenance stamping existed, or by a build
                // that could not reach git. Counted, not silently folded in.
                c.unstamped += 1;
            } else {
                c.shas.insert(l.git_sha.clone());
            }
            if !l.rb_version.is_empty() {
                c.builds.insert(l.rb_version.clone());
            }
            if !l.run_id.is_empty() {
                c.runs.insert(l.run_id.clone());
            }

            let platform = if l.platform.is_empty() {
                "?".to_string()
            } else {
                l.platform.clone()
            };
            c.seen.push((
                l.case_id.clone(),
                platform.clone(),
                l.verdict.clone(),
                l.run_id.clone(),
            ));

            if l.verdict == "fail" || l.verdict == "error" {
                c.failures.entry(l.case_id).or_default().insert(platform);
            }
        }
    }

    Ok(c)
}

pub fn render(c: &Consolidated, root: &Path) -> String {
    let mut s = String::new();
    s.push_str(&format!("consolidated {} file(s) under {}\n", c.files, root.display()));
    s.push_str(&format!(
        "  {} result line(s), {} distinct case(s), {} run(s)\n\n",
        c.lines,
        c.cases.len(),
        c.runs.len()
    ));

    // Build identity first: a mixed set makes every number below suspect.
    if c.shas.len() > 1 {
        s.push_str(&format!(
            "WARNING: results span {} different builds. A consolidated number\n\
             across builds describes no program that ever existed — filter to one\n\
             sha before drawing conclusions.\n",
            c.shas.len()
        ));
        for sha in &c.shas {
            s.push_str(&format!("    {}\n", &sha[..sha.len().min(12)]));
        }
        s.push('\n');
    } else if let Some(sha) = c.shas.iter().next() {
        s.push_str(&format!("build: {}\n", &sha[..sha.len().min(12)]));
        for b in &c.builds {
            s.push_str(&format!("       {}\n", b));
        }
        s.push('\n');
    }
    if c.unstamped > 0 {
        s.push_str(&format!(
            "note: {} line(s) carry no build provenance (pre-dating result stamping)\n\n",
            c.unstamped
        ));
    }

    let total = c.lines.max(1);
    let passed = c.by_verdict.get("pass").copied().unwrap_or(0);
    s.push_str("verdicts\n");
    for (v, n) in &c.by_verdict {
        s.push_str(&format!("  {:<14} {:>5}  {:>5.1}%\n", v, n, *n as f64 * 100.0 / total as f64));
    }
    s.push_str(&format!("\n  progress: {}/{} passing ({:.1}%)\n\n", passed, c.lines,
                        passed as f64 * 100.0 / total as f64));

    s.push_str("by platform\n");
    for (p, m) in &c.by_platform {
        let t: usize = m.values().sum();
        let ok = m.get("pass").copied().unwrap_or(0);
        s.push_str(&format!("  {:<14} {:>4}/{:<4} passing", p, ok, t));
        let notable: Vec<String> = m
            .iter()
            .filter(|(k, _)| k.as_str() != "pass")
            .map(|(k, v)| format!("{} {}", v, k))
            .collect();
        if !notable.is_empty() {
            s.push_str(&format!("   ({})", notable.join(", ")));
        }
        s.push('\n');
    }

    s.push_str("\nby group (worst first)\n");
    let mut groups: Vec<(&String, usize, usize)> = c
        .by_group
        .iter()
        .map(|(g, m)| {
            let t: usize = m.values().sum();
            (g, m.get("pass").copied().unwrap_or(0), t)
        })
        .collect();
    groups.sort_by(|a, b| {
        let ra = a.1 as f64 / a.2.max(1) as f64;
        let rb = b.1 as f64 / b.2.max(1) as f64;
        ra.partial_cmp(&rb).unwrap_or(std::cmp::Ordering::Equal)
    });
    for (g, ok, t) in groups.iter().take(15) {
        s.push_str(&format!(
            "  {:<28} {:>4}/{:<4}  {:>5.1}%\n",
            g, ok, t, *ok as f64 * 100.0 / (*t).max(1) as f64
        ));
    }

    if !c.failures.is_empty() {
        s.push_str(&format!("\nfailing cases ({})\n", c.failures.len()));
        for (case, plats) in c.failures.iter().take(30) {
            let mut ps: Vec<&str> = plats.iter().map(|s| s.as_str()).collect();
            ps.sort_unstable();
            s.push_str(&format!("  {:<44} {}\n", case, ps.join(", ")));
        }
        if c.failures.len() > 30 {
            s.push_str(&format!("  ... and {} more\n", c.failures.len() - 30));
        }
    }

    // Coverage skew is the failure mode the pass/fail columns cannot show: a
    // host missing a fixture reports skip-fixture, not a failure, so it stays
    // green while covering less than its peers.
    let skew = c.coverage_skew();
    if !skew.is_empty() {
        s.push_str(&format!(
            "\nCOVERAGE SKEW ({}) - ran on some hosts, skipped for a missing fixture on others\n\
             Comparing each host's LATEST run. Those hosts are green on less than their\n\
             peers. Sync the corpus AND the catalogue row (gitignored, so a pull does not\n\
             bring it) before comparing pass counts.\n",
            skew.len()
        ));
        for k in skew.iter().take(30) {
            let miss: Vec<&str> = k.skipped_on.iter().map(|s| s.as_str()).collect();
            let has: Vec<&str> = k.ran_on.iter().map(|s| s.as_str()).collect();
            s.push_str(&format!(
                "  {:<44} missing on {} (ran on {})\n",
                k.case_id,
                miss.join(", "),
                has.join(", ")
            ));
        }
        if skew.len() > 30 {
            s.push_str(&format!("  ... and {} more\n", skew.len() - 30));
        }
    }

    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seen(c: &mut Consolidated, case: &str, platform: &str, verdict: &str, run: &str) {
        c.seen.push((
            case.into(),
            platform.into(),
            verdict.into(),
            run.into(),
        ));
    }

    #[test]
    fn a_case_skipped_on_one_host_and_run_on_another_is_skew() {
        let mut c = Consolidated::default();
        seen(&mut c, "read.hpfs.os2-warp45", "windows", "pass", "200-win");
        seen(&mut c, "read.hpfs.os2-warp45", "linux", "skip-fixture", "200-lin");
        seen(&mut c, "read.hpfs.os2-warp45", "macos", "skip-fixture", "200-mac");

        let skew = c.coverage_skew();
        assert_eq!(skew.len(), 1);
        assert_eq!(skew[0].case_id, "read.hpfs.os2-warp45");
        assert!(skew[0].skipped_on.contains("linux"));
        assert!(skew[0].ran_on.contains("windows"));
    }

    #[test]
    fn a_case_skipped_everywhere_is_not_skew() {
        // Nobody holds the fixture. That is a corpus gap the inventory already
        // reports; it is not one host being quietly less covered than another.
        let mut c = Consolidated::default();
        seen(&mut c, "read.something", "windows", "skip-fixture", "200-win");
        seen(&mut c, "read.something", "linux", "skip-fixture", "200-lin");
        assert!(c.coverage_skew().is_empty());
    }

    #[test]
    fn a_case_that_ran_everywhere_is_not_skew() {
        let mut c = Consolidated::default();
        seen(&mut c, "read.something", "windows", "pass", "200-win");
        seen(&mut c, "read.something", "linux", "xfail", "200-lin");
        assert!(c.coverage_skew().is_empty());
    }

    #[test]
    fn skew_fixed_in_a_later_run_is_not_reported() {
        // The whole point of scoping to the latest run per platform. The first
        // version compared all of history and named eleven optical cases as
        // missing on Windows that Windows had held for weeks.
        let mut c = Consolidated::default();
        seen(&mut c, "read.optical.udf", "windows", "skip-fixture", "100-win");
        seen(&mut c, "read.optical.udf", "linux", "pass", "100-lin");
        // Later runs: Windows got the fixture.
        seen(&mut c, "read.optical.udf", "windows", "pass", "300-win");
        seen(&mut c, "read.optical.udf", "linux", "pass", "300-lin");
        assert!(
            c.coverage_skew().is_empty(),
            "stale skew from an older run must not be reported"
        );
    }

    #[test]
    fn skew_still_present_in_the_latest_run_is_reported() {
        let mut c = Consolidated::default();
        seen(&mut c, "read.optical.udf", "windows", "pass", "100-win");
        seen(&mut c, "read.optical.udf", "linux", "pass", "100-lin");
        seen(&mut c, "read.optical.udf", "windows", "pass", "300-win");
        seen(&mut c, "read.optical.udf", "linux", "skip-fixture", "300-lin");
        let skew = c.coverage_skew();
        assert_eq!(skew.len(), 1);
        assert!(skew[0].skipped_on.contains("linux"));
    }
}
