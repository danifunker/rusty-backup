//! The regression database.
//!
//! The matrix is relational data — formats x oracles x platforms x fixtures x
//! runs — and it used to live as prose tables across several markdown files.
//! That is unqueryable, drifts silently against the code, and cannot be
//! executed. Here it is a SQLite file that anything can read.
//!
//! Three layers, and the direction of flow matters:
//!
//! 1. **Declarative input, in git**: `data/formats.toml`, `data/oracles.toml`,
//!    `fixture-map*.tsv`, `cases/**.toml`. Hand-edited, reviewable in a diff.
//! 2. **This database**, built from layer 1 plus run results. Regenerable, so
//!    never hand-edited and never precious.
//! 3. **Generated markdown** for humans. Outputs, not inputs.
//!
//! SQLite is compiled in (`rusqlite/bundled`), so no test host needs a
//! `sqlite3` package, and the resulting `.db` is still openable by any
//! standard tool if someone wants to poke at it directly.

use rusqlite::{params, Connection};
use serde::Deserialize;
use std::fs;
use std::path::Path;

/// Schema lives in a real `.sql` file so it is readable and diffable, but is
/// embedded at compile time so the binary is self-contained.
const SCHEMA: &str = include_str!("../../db/schema.sql");

#[derive(Debug, Deserialize)]
struct FormatsFile {
    #[serde(default, rename = "format")]
    formats: Vec<FormatDef>,
}

#[derive(Debug, Deserialize)]
struct FormatDef {
    id: String,
    kind: String,
    name: String,
    #[serde(default)]
    we_read: bool,
    #[serde(default)]
    we_write: bool,
    #[serde(default)]
    builder: Option<String>,
    #[serde(default)]
    notes: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OraclesFile {
    #[serde(default, rename = "oracle")]
    oracles: Vec<OracleDef>,
}

#[derive(Debug, Deserialize)]
struct OracleDef {
    id: String,
    tool: String,
    kind: String,
    #[serde(default)]
    notes: Option<String>,
    #[serde(default)]
    availability: Vec<AvailDef>,
    #[serde(default)]
    verifies: Vec<VerifyDef>,
}

#[derive(Debug, Deserialize)]
struct AvailDef {
    platform: String,
    status: String,
    #[serde(default)]
    path_hint: Option<String>,
    #[serde(default)]
    verified_on: Option<String>,
}

#[derive(Debug, Deserialize)]
struct VerifyDef {
    format: String,
    direction: String,
    strength: String,
    #[serde(default = "default_status")]
    status: String,
    #[serde(default)]
    evidence: Option<String>,
}

fn default_status() -> String {
    "untested".to_string()
}

pub struct BuildReport {
    pub formats: usize,
    pub oracles: usize,
    pub verifications: usize,
    pub fixtures: usize,
    pub runs: usize,
    pub results: usize,
    /// Non-fatal problems — a verification naming an unknown format, say.
    /// Reported rather than aborting, consistent with the rest of the harness.
    pub warnings: Vec<String>,
}

/// Build (or rebuild) the database from the declarative sources.
///
/// Destructive by design: the database is derived, so a rebuild is always
/// safe and always the fastest way to fix a stale row. Run history is the one
/// exception and is preserved by ingesting bundles afterwards.
pub fn build(regression_dir: &Path, db_path: &Path) -> Result<BuildReport, String> {
    if let Some(parent) = db_path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let conn = Connection::open(db_path).map_err(|e| format!("cannot open {}: {}", db_path.display(), e))?;
    conn.execute_batch(SCHEMA)
        .map_err(|e| format!("applying schema: {}", e))?;

    let mut rep = BuildReport {
        formats: 0,
        oracles: 0,
        verifications: 0,
        fixtures: 0,
        runs: 0,
        results: 0,
        warnings: Vec::new(),
    };

    // Declarative rows are replaced wholesale; runs and results are not.
    conn.execute_batch(
        "DELETE FROM verification; DELETE FROM oracle_availability;
         DELETE FROM oracle; DELETE FROM fixture_format; DELETE FROM fixture;
         DELETE FROM format;",
    )
    .map_err(|e| format!("clearing declarative tables: {}", e))?;

    // --- formats -----------------------------------------------------------
    let fpath = regression_dir.join("data").join("formats.toml");
    let ftext = fs::read_to_string(&fpath)
        .map_err(|e| format!("cannot read {}: {}", fpath.display(), e))?;
    let ffile: FormatsFile =
        toml::from_str(&ftext).map_err(|e| format!("{}: {}", fpath.display(), e))?;
    for f in &ffile.formats {
        conn.execute(
            "INSERT OR REPLACE INTO format (id,kind,name,we_read,we_write,builder,notes)
             VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![
                f.id,
                f.kind,
                f.name,
                f.we_read as i32,
                f.we_write as i32,
                f.builder,
                f.notes
            ],
        )
        .map_err(|e| format!("inserting format {}: {}", f.id, e))?;
        rep.formats += 1;
    }

    // --- oracles -----------------------------------------------------------
    let opath = regression_dir.join("data").join("oracles.toml");
    let otext = fs::read_to_string(&opath)
        .map_err(|e| format!("cannot read {}: {}", opath.display(), e))?;
    let ofile: OraclesFile =
        toml::from_str(&otext).map_err(|e| format!("{}: {}", opath.display(), e))?;
    for o in &ofile.oracles {
        conn.execute(
            "INSERT OR REPLACE INTO oracle (id,tool,kind,notes) VALUES (?1,?2,?3,?4)",
            params![o.id, o.tool, o.kind, o.notes],
        )
        .map_err(|e| format!("inserting oracle {}: {}", o.id, e))?;
        rep.oracles += 1;

        for a in &o.availability {
            conn.execute(
                "INSERT OR REPLACE INTO oracle_availability
                   (oracle_id,platform,status,path_hint,verified_on)
                 VALUES (?1,?2,?3,?4,?5)",
                params![o.id, a.platform, a.status, a.path_hint, a.verified_on],
            )
            .map_err(|e| format!("availability {}/{}: {}", o.id, a.platform, e))?;
        }

        for v in &o.verifies {
            // A verification naming a format that does not exist is a data
            // bug worth surfacing, not a reason to abandon the build.
            let known: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM format WHERE id = ?1",
                    params![v.format],
                    |r| r.get(0),
                )
                .unwrap_or(0);
            if known == 0 {
                rep.warnings.push(format!(
                    "oracle '{}' claims to verify unknown format '{}'",
                    o.id, v.format
                ));
                continue;
            }
            conn.execute(
                "INSERT OR REPLACE INTO verification
                   (oracle_id,format_id,direction,strength,status,evidence)
                 VALUES (?1,?2,?3,?4,?5,?6)",
                params![o.id, v.format, v.direction, v.strength, v.status, v.evidence],
            )
            .map_err(|e| format!("verification {}/{}: {}", o.id, v.format, e))?;
            rep.verifications += 1;
        }
    }

    // --- fixtures ----------------------------------------------------------
    for (name, location) in [
        ("fixture-map.repo.tsv", "repo"),
        ("fixture-map.tsv", "corpus"),
    ] {
        let p = regression_dir.join(name);
        if !p.is_file() {
            continue;
        }
        let text = match fs::read_to_string(&p) {
            Ok(t) => t,
            Err(e) => {
                rep.warnings.push(format!("cannot read {}: {}", p.display(), e));
                continue;
            }
        };
        for (i, line) in text.lines().enumerate() {
            if i == 0 || line.trim().is_empty() || line.starts_with('#') {
                continue;
            }
            let c: Vec<&str> = line.split('\t').collect();
            if c.len() < 2 {
                continue;
            }
            let get = |n: usize| c.get(n).map(|s| s.trim().to_string()).unwrap_or_default();
            let loc = if get(1).starts_with("repo:") {
                "repo"
            } else if get(1).contains("fixtures-large") {
                "annex"
            } else {
                location
            };
            conn.execute(
                "INSERT OR REPLACE INTO fixture
                   (id,relpath,bytes,sha256,origin,redistributable,location,notes)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    get(0),
                    get(1),
                    get(2).parse::<i64>().unwrap_or(0),
                    get(3),
                    get(5),
                    get(6),
                    loc,
                    get(8)
                ],
            )
            .map_err(|e| format!("fixture {}: {}", get(0), e))?;
            rep.fixtures += 1;

            // Fixture IDs encode their format: fs.fat16.base.hd -> fs.fat16.
            let id = get(0);
            let parts: Vec<&str> = id.split('.').collect();
            if parts.len() >= 2 {
                let fmt = format!("{}.{}", parts[0], parts[1]);
                let known: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM format WHERE id = ?1",
                        params![fmt],
                        |r| r.get(0),
                    )
                    .unwrap_or(0);
                if known > 0 {
                    let _ = conn.execute(
                        "INSERT OR IGNORE INTO fixture_format (fixture_id,format_id)
                         VALUES (?1,?2)",
                        params![id, fmt],
                    );
                }
            }
        }
    }

    let (runs, results) = ingest_runs(&conn, &regression_dir.join("runs"), &mut rep.warnings);
    rep.runs = runs;
    rep.results = results;

    Ok(rep)
}

/// Pull historical run bundles into the database so trends survive.
fn ingest_runs(conn: &Connection, runs_dir: &Path, warnings: &mut Vec<String>) -> (usize, usize) {
    let mut nruns = 0;
    let mut nresults = 0;
    let entries = match fs::read_dir(runs_dir) {
        Ok(e) => e,
        Err(_) => return (0, 0),
    };
    for entry in entries.flatten() {
        let dir = entry.path();
        let jsonl = dir.join("results.jsonl");
        if !jsonl.is_file() {
            continue;
        }
        let bundle = dir.file_name().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
        // Already ingested? Bundle names are unique per run.
        let seen: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM run WHERE bundle_path = ?1",
                params![bundle],
                |r| r.get(0),
            )
            .unwrap_or(0);
        if seen > 0 {
            continue;
        }

        let env: serde_json::Value = fs::read_to_string(dir.join("env.json"))
            .ok()
            .and_then(|t| serde_json::from_str(&t).ok())
            .unwrap_or(serde_json::Value::Null);
        let sv = |k: &str| env.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string();

        if conn
            .execute(
                "INSERT INTO run (started_utc,platform,host,rb_version,bundle_path)
                 VALUES (?1,?2,?3,?4,?5)",
                params![sv("stamp"), sv("platform"), sv("host"), sv("rb_cli_version"), bundle],
            )
            .is_err()
        {
            continue;
        }
        let run_id = conn.last_insert_rowid();
        nruns += 1;

        let text = match fs::read_to_string(&jsonl) {
            Ok(t) => t,
            Err(e) => {
                warnings.push(format!("{}: {}", jsonl.display(), e));
                continue;
            }
        };
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let v: serde_json::Value = match serde_json::from_str(line) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let s = |k: &str| v.get(k).and_then(|x| x.as_str()).map(|x| x.to_string());
            let n = |k: &str| v.get(k).and_then(|x| x.as_i64());
            if conn
                .execute(
                    "INSERT OR IGNORE INTO result
                       (run_id,case_id,tier,group_name,verdict,duration_ms,fixture_id,detail)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                    params![
                        run_id,
                        s("case_id").unwrap_or_default(),
                        n("tier"),
                        s("group"),
                        s("verdict").unwrap_or_else(|| "error".into()),
                        n("duration_ms"),
                        s("fixture_id"),
                        s("skip_reason")
                    ],
                )
                .is_ok()
            {
                nresults += 1;
            }
        }
    }
    (nruns, nresults)
}

/// Run a query and return rows as strings — used by `db query` and by the
/// markdown generators.
pub fn query(db_path: &Path, sql: &str) -> Result<(Vec<String>, Vec<Vec<String>>), String> {
    let conn = Connection::open(db_path).map_err(|e| e.to_string())?;
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let cols: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
    let ncols = cols.len();
    let mut out = Vec::new();
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        let mut r = Vec::with_capacity(ncols);
        for i in 0..ncols {
            let v: rusqlite::types::Value = row.get(i).unwrap_or(rusqlite::types::Value::Null);
            r.push(match v {
                rusqlite::types::Value::Null => String::new(),
                rusqlite::types::Value::Integer(i) => i.to_string(),
                rusqlite::types::Value::Real(f) => f.to_string(),
                rusqlite::types::Value::Text(t) => t,
                rusqlite::types::Value::Blob(_) => "<blob>".to_string(),
            });
        }
        out.push(r);
    }
    Ok((cols, out))
}

/// Named queries, so the common questions do not need SQL at the call site.
pub fn named_query(name: &str) -> Option<&'static str> {
    Some(match name {
        "unverified-writes" => "SELECT id, kind, name, COALESCE(builder,'') AS builder FROM v_unverified_writes",
        "unfixtured-reads" => "SELECT id, kind, name FROM v_unfixtured_reads",
        "coverage" => "SELECT id, kind, we_read, we_write, fixtures, write_oracles, \
                       COALESCE(has_authoritative,0) AS authoritative FROM v_coverage",
        "platform-pins" => "SELECT format_id, direction, only_platform FROM v_platform_pins",
        "oracle-reach" => "SELECT oracle, platform, status, COALESCE(format_id,'') AS fmt, \
                           COALESCE(direction,'') AS dir FROM v_oracle_reach ORDER BY oracle, platform",
        "latest" => "SELECT platform, verdict, COUNT(*) AS n FROM v_latest_result \
                     GROUP BY platform, verdict ORDER BY platform, verdict",
        "fixtures" => "SELECT location, COUNT(*) AS n, SUM(bytes)/1048576 AS mb \
                       FROM fixture GROUP BY location",
        _ => return None,
    })
}

pub const QUERY_NAMES: &[&str] = &[
    "unverified-writes",
    "unfixtured-reads",
    "coverage",
    "platform-pins",
    "oracle-reach",
    "latest",
    "fixtures",
];
