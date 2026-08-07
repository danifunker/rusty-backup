//! The planner — "regression maker".
//!
//! Maps requirements onto the machines that actually exist. Given
//!
//!   - what rb-cli reads and writes      (`data/formats.toml`)
//!   - what verifies it, and where       (`data/oracles.toml`)
//!   - which machines are available      (`data/hosts.toml`)
//!
//! it computes a staged job graph: who produces each artifact, who verifies
//! it, and which artifacts have to cross a machine boundary to get there.
//!
//! The motivating case: only Windows has the native NTFS write path, but the
//! oracle that can judge the result lives on Linux. So Windows produces, the
//! artifact ships, Linux verifies, and the verdict comes back. One Linux box
//! ends up doing all Linux-side filesystem validation on inputs produced
//! elsewhere.
//!
//! Anything that cannot be routed is a **gap**, reported explicitly. A plan
//! that silently omits a format would be worse than no plan, because it would
//! read as coverage.

use crate::registry::Registry;
use std::collections::BTreeMap;
use std::path::Path;


/// One unit of work on one machine.
#[derive(Debug)]
pub struct Job {
    pub stage: u8,
    pub host: String,
    pub kind: JobKind,
    pub format: String,
    pub detail: String,
    /// Other host/oracle pairs that could do this job. Half our oracles run on
    /// more than one platform, so a plan that named only the winner would read
    /// as though each check were pinned to one machine. Recording the
    /// alternatives makes the redundancy visible and lets a run fall back
    /// without re-planning when a host is unreachable.
    pub alternatives: Vec<String>,
}

#[derive(Debug, PartialEq)]
pub enum JobKind {
    /// rb-cli builds an artifact.
    Produce,
    /// The artifact moves to another machine.
    Transfer,
    /// An oracle judges the artifact.
    Verify,
    /// rb-cli reads a third-party reference fixture.
    Read,
}

impl JobKind {
    pub fn label(&self) -> &'static str {
        match self {
            JobKind::Produce => "produce",
            JobKind::Transfer => "transfer",
            JobKind::Verify => "verify",
            JobKind::Read => "read",
        }
    }
}

#[derive(Debug)]
pub struct Gap {
    pub format: String,
    /// Classifies the gap; `render` groups by section instead, for now.
    #[allow(dead_code)]
    pub kind: String,
    pub reason: String,
}

#[derive(Debug)]
pub struct Plan {
    pub jobs: Vec<Job>,
    pub gaps: Vec<Gap>,
    pub hosts: Vec<String>,
}

/// Rank oracles so the planner picks the best available judge, not the first.
fn strength_rank(s: &str) -> i32 {
    match s {
        "authoritative" => 3,
        "structural" => 2,
        _ => 1,
    }
}

fn availability_rank(s: &str) -> i32 {
    match s {
        "verified" => 3,
        "expected" => 2,
        _ => 1, // install
    }
}

pub fn build_plan(regression_dir: &Path) -> Result<Plan, String> {
    let reg = Registry::load(regression_dir)?;
    if reg.hosts.is_empty() {
        return Err("no hosts configured; copy data/hosts.toml.example".into());
    }

    let hosts: Vec<String> = reg.hosts.iter().map(|h| h.id.clone()).collect();

    // Prefer a local producer so we avoid a needless network hop.
    let mut producers: Vec<&crate::registry::Host> =
        reg.hosts.iter().filter(|h| h.can_produce).collect();
    producers.sort_by_key(|h| match h.transport.as_str() {
        "local" => 0,
        "wsl" => 1,
        _ => 2,
    });
    let producer = producers
        .first()
        .map(|h| h.id.clone())
        .unwrap_or_else(|| "?".to_string());

    let mut jobs = Vec::new();
    let mut gaps = Vec::new();

    // --- write paths -------------------------------------------------------
    for f in reg.formats.iter().filter(|f| f.we_write) {
        let mut candidates: Vec<(String, String, String, String, i32)> = Vec::new();
        for h in reg.hosts.iter().filter(|h| h.can_verify) {
            for a in reg.availability.iter().filter(|a| a.platform == h.platform) {
                if !matches!(a.status.as_str(), "verified" | "expected" | "install") {
                    continue;
                }
                for v in reg
                    .verifications
                    .iter()
                    .filter(|v| v.oracle == a.oracle && v.format == f.id && v.direction == "write")
                {
                    let mut score =
                        strength_rank(&v.strength) * 10 + availability_rank(&a.status) * 3;
                    if h.transport == "local" {
                        score += 1;
                    }
                    candidates.push((
                        h.id.clone(),
                        v.oracle.clone(),
                        v.strength.clone(),
                        a.status.clone(),
                        score,
                    ));
                }
            }
        }

        // Highest score wins; the rest are recorded as fallbacks.
        candidates.sort_by(|a, b| b.4.cmp(&a.4).then(a.0.cmp(&b.0)));
        let best = candidates.first().cloned();
        let alternatives: Vec<String> = candidates
            .iter()
            .skip(1)
            .map(|c| format!("{} ({})", c.0, c.1))
            .collect();

        match best {
            None => gaps.push(Gap {
                format: f.id.clone(),
                kind: f.kind.clone(),
                reason: match &f.builder {
                    Some(b) => format!("`{}` produces it, but no oracle on any configured host", b),
                    None => "we write it, but no oracle on any configured host".into(),
                },
            }),
            Some((vhost, oracle, strength, avail, _)) => {
                jobs.push(Job {
                    stage: 1,
                    host: producer.clone(),
                    kind: JobKind::Produce,
                    format: f.id.clone(),
                    detail: match &f.builder {
                        Some(b) => format!("rb-cli {}", b),
                        None => "rb-cli write path".into(),
                    },
                    alternatives: Vec::new(),
                });
                if producer != vhost {
                    jobs.push(Job {
                        stage: 2,
                        host: format!("{} -> {}", producer, vhost),
                        kind: JobKind::Transfer,
                        format: f.id.clone(),
                        detail: "ship artifact to the host holding the oracle".into(),
                        alternatives: Vec::new(),
                    });
                }
                jobs.push(Job {
                    stage: 3,
                    host: vhost,
                    kind: JobKind::Verify,
                    format: f.id.clone(),
                    detail: format!("{} ({}, {})", oracle, strength, avail),
                    alternatives,
                });
            }
        }
    }

    // --- read paths --------------------------------------------------------
    for f in reg.formats.iter().filter(|f| f.we_read) {
        let n = reg
            .fixtures
            .iter()
            .filter(|x| x.formats.iter().any(|c| c == &f.id))
            .count();
        if n == 0 {
            gaps.push(Gap {
                format: f.id.clone(),
                kind: f.kind.clone(),
                reason: "we read it, but no third-party reference fixture exists".into(),
            });
        } else {
            jobs.push(Job {
                stage: 0,
                host: producer.clone(),
                kind: JobKind::Read,
                format: f.id.clone(),
                detail: format!("{} reference fixture(s)", n),
                alternatives: Vec::new(),
            });
        }
    }

    jobs.sort_by(|a, b| a.stage.cmp(&b.stage).then(a.format.cmp(&b.format)));
    Ok(Plan { jobs, gaps, hosts })
}

/// Human-readable plan. Deliberately terse — it is meant to be scanned before
/// a monthly run to see what will happen and what will not.
pub fn render(plan: &Plan) -> String {
    let mut s = String::new();
    s.push_str(&format!("hosts: {}\n\n", plan.hosts.join(", ")));

    let stage_name = |n: u8| match n {
        0 => "STAGE 0 - read reference fixtures",
        1 => "STAGE 1 - produce artifacts",
        2 => "STAGE 2 - transfer",
        _ => "STAGE 3 - verify",
    };

    let mut by_stage: BTreeMap<u8, Vec<&Job>> = BTreeMap::new();
    for j in &plan.jobs {
        by_stage.entry(j.stage).or_default().push(j);
    }
    for (stage, jobs) in &by_stage {
        s.push_str(&format!("{} ({} jobs)\n", stage_name(*stage), jobs.len()));
        // Group by host so it reads as "this machine does this list".
        let mut by_host: BTreeMap<&str, Vec<&Job>> = BTreeMap::new();
        for j in jobs {
            by_host.entry(j.host.as_str()).or_default().push(j);
        }
        for (host, hj) in &by_host {
            s.push_str(&format!("  {:<22} {} job(s)\n", host, hj.len()));
            for j in hj.iter().take(6) {
                s.push_str(&format!(
                    "    {:<8} {:<22} {}\n",
                    j.kind.label(),
                    j.format,
                    j.detail
                ));
                // Half our oracles run on more than one platform. Showing the
                // fallbacks stops the plan reading as though every check were
                // pinned to a single machine.
                if !j.alternatives.is_empty() {
                    s.push_str(&format!(
                        "             {:<21} or: {}\n",
                        "",
                        j.alternatives.join(", ")
                    ));
                }
            }
            if hj.len() > 6 {
                s.push_str(&format!("    ... and {} more\n", hj.len() - 6));
            }
        }
        s.push('\n');
    }

    s.push_str(&format!("GAPS - cannot be planned ({})\n", plan.gaps.len()));
    for g in &plan.gaps {
        s.push_str(&format!("  {:<22} {}\n", g.format, g.reason));
    }
    s
}
