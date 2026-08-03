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

use rusqlite::{params, Connection};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize)]
struct HostsFile {
    #[serde(default, rename = "host")]
    hosts: Vec<HostDef>,
}

#[derive(Debug, Deserialize)]
struct HostDef {
    id: String,
    platform: String,
    transport: String,
    #[serde(default = "yes")]
    can_produce: bool,
    #[serde(default = "yes")]
    can_verify: bool,
    #[serde(default)]
    notes: Option<String>,
}

fn yes() -> bool {
    true
}

/// One unit of work on one machine.
#[derive(Debug)]
pub struct Job {
    pub stage: u8,
    pub host: String,
    pub kind: JobKind,
    pub format: String,
    pub detail: String,
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
    pub kind: String,
    pub reason: String,
}

#[derive(Debug)]
pub struct Plan {
    pub jobs: Vec<Job>,
    pub gaps: Vec<Gap>,
    pub hosts: Vec<String>,
}

/// Load `data/hosts.toml` into the database, replacing whatever was there.
pub fn load_hosts(conn: &Connection, regression_dir: &Path) -> Result<usize, String> {
    let p = regression_dir.join("data").join("hosts.toml");
    let p = if p.is_file() {
        p
    } else {
        // Fall back to the template so `plan` demonstrates something useful
        // on a fresh checkout rather than reporting an empty inventory.
        regression_dir.join("data").join("hosts.toml.example")
    };
    let text = fs::read_to_string(&p).map_err(|e| format!("cannot read {}: {}", p.display(), e))?;
    let f: HostsFile = toml::from_str(&text).map_err(|e| format!("{}: {}", p.display(), e))?;

    conn.execute("DELETE FROM host", []).map_err(|e| e.to_string())?;
    for h in &f.hosts {
        conn.execute(
            "INSERT OR REPLACE INTO host (id,platform,transport,can_produce,can_verify,notes)
             VALUES (?1,?2,?3,?4,?5,?6)",
            params![
                h.id,
                h.platform,
                h.transport,
                h.can_produce as i32,
                h.can_verify as i32,
                h.notes
            ],
        )
        .map_err(|e| format!("host {}: {}", h.id, e))?;
    }
    Ok(f.hosts.len())
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

pub fn build_plan(db_path: &Path, regression_dir: &Path) -> Result<Plan, String> {
    let conn = Connection::open(db_path).map_err(|e| e.to_string())?;
    load_hosts(&conn, regression_dir)?;

    let mut hosts: Vec<String> = Vec::new();
    {
        let mut st = conn
            .prepare("SELECT id FROM host ORDER BY id")
            .map_err(|e| e.to_string())?;
        let rows = st.query_map([], |r| r.get::<_, String>(0)).map_err(|e| e.to_string())?;
        for r in rows.flatten() {
            hosts.push(r);
        }
    }
    if hosts.is_empty() {
        return Err("no hosts configured; copy data/hosts.toml.example".into());
    }

    // Producers, preferring a local transport so we avoid a needless hop.
    let mut producers: Vec<(String, String, String)> = Vec::new(); // id, platform, transport
    {
        let mut st = conn
            .prepare(
                "SELECT id, platform, transport FROM host WHERE can_produce = 1
                 ORDER BY CASE transport WHEN 'local' THEN 0 WHEN 'wsl' THEN 1 ELSE 2 END, id",
            )
            .map_err(|e| e.to_string())?;
        let rows = st
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .map_err(|e| e.to_string())?;
        for r in rows.flatten() {
            producers.push(r);
        }
    }

    let mut jobs = Vec::new();
    let mut gaps = Vec::new();

    // --- write paths -------------------------------------------------------
    let mut st = conn
        .prepare(
            "SELECT id, kind, name, COALESCE(builder,'') FROM format
             WHERE we_write = 1 ORDER BY kind, id",
        )
        .map_err(|e| e.to_string())?;
    let formats: Vec<(String, String, String, String)> = st
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .map_err(|e| e.to_string())?
        .flatten()
        .collect();

    for (fid, fkind, _fname, builder) in &formats {
        // Best host/oracle pair that can judge this write.
        let mut best: Option<(String, String, String, String, i32)> = None; // host, oracle, strength, avail, score
        let mut vs = conn
            .prepare(
                "SELECT host, oracle, strength, availability, transport
                 FROM v_host_capability
                 WHERE format_id = ?1 AND direction = 'write'",
            )
            .map_err(|e| e.to_string())?;
        let rows = vs
            .query_map(params![fid], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                ))
            })
            .map_err(|e| e.to_string())?;
        for (host, oracle, strength, avail, transport) in rows.flatten() {
            let mut score = strength_rank(&strength) * 10 + availability_rank(&avail) * 3;
            if transport == "local" {
                score += 1; // tie-break toward no network hop
            }
            if best.as_ref().map(|b| score > b.4).unwrap_or(true) {
                best = Some((host, oracle, strength, avail, score));
            }
        }

        match best {
            None => gaps.push(Gap {
                format: fid.clone(),
                kind: fkind.clone(),
                reason: if builder.is_empty() {
                    "we write it, but no oracle on any configured host".into()
                } else {
                    format!("`{}` produces it, but no oracle on any configured host", builder)
                },
            }),
            Some((vhost, oracle, strength, avail, _)) => {
                // Produce on the preferred producer. A future refinement is
                // platform-pinned production (raw device paths only exist on
                // their own OS); for now the first producer wins.
                let (phost, _pplat, _ptrans) = producers
                    .first()
                    .cloned()
                    .unwrap_or_else(|| ("?".into(), "?".into(), "?".into()));

                jobs.push(Job {
                    stage: 1,
                    host: phost.clone(),
                    kind: JobKind::Produce,
                    format: fid.clone(),
                    detail: if builder.is_empty() {
                        "rb-cli write path".into()
                    } else {
                        format!("rb-cli {}", builder)
                    },
                });

                if phost != vhost {
                    jobs.push(Job {
                        stage: 2,
                        host: format!("{} -> {}", phost, vhost),
                        kind: JobKind::Transfer,
                        format: fid.clone(),
                        detail: "ship artifact to the host holding the oracle".into(),
                    });
                }

                jobs.push(Job {
                    stage: 3,
                    host: vhost,
                    kind: JobKind::Verify,
                    format: fid.clone(),
                    detail: format!("{} ({}, {})", oracle, strength, avail),
                });
            }
        }
    }

    // --- read paths --------------------------------------------------------
    let mut rs = conn
        .prepare(
            "SELECT f.id, f.kind,
                    (SELECT COUNT(*) FROM fixture_format ff WHERE ff.format_id = f.id)
             FROM format f WHERE f.we_read = 1 ORDER BY f.kind, f.id",
        )
        .map_err(|e| e.to_string())?;
    let reads: Vec<(String, String, i64)> = rs
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .map_err(|e| e.to_string())?
        .flatten()
        .collect();

    for (fid, fkind, nfix) in &reads {
        if *nfix == 0 {
            gaps.push(Gap {
                format: fid.clone(),
                kind: fkind.clone(),
                reason: "we read it, but no third-party reference fixture exists".into(),
            });
        } else {
            let (phost, _, _) = producers
                .first()
                .cloned()
                .unwrap_or_else(|| ("?".into(), "?".into(), "?".into()));
            jobs.push(Job {
                stage: 0,
                host: phost,
                kind: JobKind::Read,
                format: fid.clone(),
                detail: format!("{} reference fixture(s)", nfix),
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
