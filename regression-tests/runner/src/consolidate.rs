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
    pub unstamped: usize,
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

            if l.verdict == "fail" || l.verdict == "error" {
                c.failures
                    .entry(l.case_id)
                    .or_default()
                    .insert(if l.platform.is_empty() { "?".into() } else { l.platform });
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

    s
}
