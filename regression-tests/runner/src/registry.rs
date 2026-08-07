//! The registry — the matrix, held in memory.
//!
//! Replaces an earlier SQLite layer. The reasoning, recorded so nobody
//! reintroduces it by accident:
//!
//! The whole matrix is ~52 KB of TOML — 121 formats, 42 oracles, 6 hosts.
//! Every question the planner asks is a handful of iterator chains over that.
//! The database was larger than the data it held, needed a C-compiled
//! dependency, produced a binary artifact nobody could review in a diff, and
//! introduced a materialisation step that went stale twice in one afternoon
//! (a schema edit silently ignored because of CREATE TABLE IF NOT EXISTS, and
//! a plan computed from a database that predated the TOML it came from).
//!
//! History did not save the argument either: a year of monthly runs across six
//! hosts is about 11 MB across 72 JSONL files, which loads in well under a
//! second.
//!
//! So: TOML is the source of truth, this is the in-memory view, and nothing is
//! materialised in between. `export` writes a normalised JSON snapshot for
//! other tools, but nothing here depends on it.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

// ---------------------------------------------------------------------------
// Entities. Field names are shared with the JSON export, so the on-disk shape
// and the in-memory shape cannot drift.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Format {
    pub id: String,
    pub kind: String,
    pub name: String,
    #[serde(default)]
    pub we_read: bool,
    #[serde(default)]
    pub we_write: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub builder: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Oracle {
    pub id: String,
    pub tool: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Availability {
    pub oracle: String,
    pub platform: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_hint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified_on: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Verification {
    pub oracle: String,
    pub format: String,
    pub direction: String,
    pub strength: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Host {
    pub id: String,
    pub platform: String,
    pub transport: String,
    pub can_produce: bool,
    pub can_verify: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fixture {
    pub id: String,
    pub relpath: String,
    #[serde(default)]
    pub bytes: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sha256: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub origin: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub redistributable: String,
    pub location: String,
    /// Formats this fixture is a reference for. Derived from the ID prefix.
    pub formats: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub notes: String,
}

// ---------------------------------------------------------------------------
// TOML input shapes
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct FormatsFile {
    #[serde(default, rename = "format")]
    formats: Vec<Format>,
}

#[derive(Deserialize)]
struct OraclesFile {
    #[serde(default, rename = "oracle")]
    oracles: Vec<OracleDef>,
}

#[derive(Deserialize)]
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

#[derive(Deserialize)]
struct AvailDef {
    platform: String,
    status: String,
    #[serde(default)]
    path_hint: Option<String>,
    #[serde(default)]
    verified_on: Option<String>,
}

#[derive(Deserialize)]
struct VerifyDef {
    format: String,
    direction: String,
    strength: String,
    #[serde(default = "untested")]
    status: String,
    #[serde(default)]
    evidence: Option<String>,
}

fn untested() -> String {
    "untested".to_string()
}

#[derive(Deserialize)]
struct HostsFile {
    #[serde(default, rename = "host")]
    hosts: Vec<HostDef>,
}

#[derive(Deserialize)]
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

// ---------------------------------------------------------------------------

#[derive(Debug, Default, Serialize)]
pub struct Registry {
    pub schema_version: u32,
    pub formats: Vec<Format>,
    pub oracles: Vec<Oracle>,
    pub availability: Vec<Availability>,
    pub verifications: Vec<Verification>,
    pub hosts: Vec<Host>,
    pub fixtures: Vec<Fixture>,
    /// Problems found while loading. Surfaced, never fatal — the same
    /// report-never-abort rule the rest of the harness follows.
    #[serde(skip)]
    pub warnings: Vec<String>,
}

pub const SCHEMA_VERSION: u32 = 1;

impl Registry {
    pub fn load(regression_dir: &Path) -> Result<Registry, String> {
        let mut r = Registry {
            schema_version: SCHEMA_VERSION,
            ..Default::default()
        };
        let data = regression_dir.join("data");

        // --- formats -------------------------------------------------------
        let p = data.join("formats.toml");
        let text = fs::read_to_string(&p).map_err(|e| format!("{}: {}", p.display(), e))?;
        let ff: FormatsFile = toml::from_str(&text).map_err(|e| format!("{}: {}", p.display(), e))?;
        r.formats = ff.formats;

        let known: BTreeSet<String> = r.formats.iter().map(|f| f.id.clone()).collect();
        if known.len() != r.formats.len() {
            r.warnings.push("duplicate format ids in formats.toml".to_string());
        }

        // --- oracles -------------------------------------------------------
        let p = data.join("oracles.toml");
        let text = fs::read_to_string(&p).map_err(|e| format!("{}: {}", p.display(), e))?;
        let of: OraclesFile = toml::from_str(&text).map_err(|e| format!("{}: {}", p.display(), e))?;
        for o in of.oracles {
            for a in &o.availability {
                r.availability.push(Availability {
                    oracle: o.id.clone(),
                    platform: a.platform.clone(),
                    status: a.status.clone(),
                    path_hint: a.path_hint.clone(),
                    verified_on: a.verified_on.clone(),
                });
            }
            for v in &o.verifies {
                // The check the FK used to do, now with a better message.
                if !known.contains(&v.format) {
                    r.warnings.push(format!(
                        "oracle '{}' claims to verify unknown format '{}'",
                        o.id, v.format
                    ));
                    continue;
                }
                r.verifications.push(Verification {
                    oracle: o.id.clone(),
                    format: v.format.clone(),
                    direction: v.direction.clone(),
                    strength: v.strength.clone(),
                    status: v.status.clone(),
                    evidence: v.evidence.clone(),
                });
            }
            r.oracles.push(Oracle {
                id: o.id,
                tool: o.tool,
                kind: o.kind,
                notes: o.notes,
            });
        }

        // --- hosts ---------------------------------------------------------
        let p = data.join("hosts.toml");
        let p = if p.is_file() { p } else { data.join("hosts.toml.example") };
        if let Ok(text) = fs::read_to_string(&p) {
            match toml::from_str::<HostsFile>(&text) {
                Ok(hf) => {
                    for h in hf.hosts {
                        r.hosts.push(Host {
                            id: h.id,
                            platform: h.platform,
                            transport: h.transport,
                            can_produce: h.can_produce,
                            can_verify: h.can_verify,
                            notes: h.notes,
                        });
                    }
                }
                Err(e) => r.warnings.push(format!("{}: {}", p.display(), e)),
            }
        }

        // --- fixtures ------------------------------------------------------
        let mut seen: BTreeMap<String, Fixture> = BTreeMap::new();
        for (name, default_loc) in [
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
                    r.warnings.push(format!("{}: {}", p.display(), e));
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
                let g = |n: usize| c.get(n).map(|s| s.trim().to_string()).unwrap_or_default();
                let relpath = g(1);
                let location = if relpath.starts_with("repo:") {
                    "repo"
                } else if relpath.contains("fixtures-large") {
                    "annex"
                } else {
                    default_loc
                };
                let id = g(0);
                // Fixture IDs encode their format: fs.fat16.base.hd -> fs.fat16
                let parts: Vec<&str> = id.split('.').collect();
                let mut formats = Vec::new();
                if parts.len() >= 2 {
                    let fmt = format!("{}.{}", parts[0], parts[1]);
                    if known.contains(&fmt) {
                        formats.push(fmt);
                    } else {
                        r.warnings.push(format!(
                            "fixture '{}' implies format '{}', which is not in formats.toml",
                            id, fmt
                        ));
                    }
                }
                seen.insert(
                    id.clone(),
                    Fixture {
                        id,
                        relpath,
                        bytes: g(2).parse().unwrap_or(0),
                        sha256: g(3),
                        origin: g(5),
                        redistributable: g(6),
                        location: location.to_string(),
                        formats,
                        notes: g(8),
                    },
                );
            }
        }
        r.fixtures = seen.into_values().collect();

        Ok(r)
    }

    /// Formats we can write with no independent oracle behind them.
    pub fn unverified_writes(&self) -> Vec<&Format> {
        self.formats
            .iter()
            .filter(|f| f.we_write)
            .filter(|f| {
                !self
                    .verifications
                    .iter()
                    .any(|v| v.format == f.id && v.direction == "write")
            })
            .collect()
    }

    /// Formats we read with no third-party reference fixture.
    pub fn unfixtured_reads(&self) -> Vec<&Format> {
        self.formats
            .iter()
            .filter(|f| f.we_read)
            .filter(|f| !self.fixtures.iter().any(|x| x.formats.iter().any(|c| c == &f.id)))
            .collect()
    }

    /// Which oracles can run on a host, by matching its platform.
    pub fn oracle_reach(&self) -> Vec<(&Host, &Availability, Vec<&Verification>)> {
        let mut out = Vec::new();
        for h in &self.hosts {
            if !h.can_verify {
                continue;
            }
            for a in &self.availability {
                if a.platform != h.platform {
                    continue;
                }
                if !matches!(a.status.as_str(), "verified" | "expected" | "install") {
                    continue;
                }
                let vs: Vec<&Verification> =
                    self.verifications.iter().filter(|v| v.oracle == a.oracle).collect();
                out.push((h, a, vs));
            }
        }
        out
    }

    /// Formats verifiable on exactly one platform — what decides which
    /// machines the suite genuinely requires.
    pub fn platform_pins(&self) -> Vec<(String, String, String)> {
        let mut by_fmt: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();
        for (h, _a, vs) in self.oracle_reach() {
            for v in vs {
                by_fmt
                    .entry((v.format.clone(), v.direction.clone()))
                    .or_default()
                    .insert(h.platform.clone());
            }
        }
        by_fmt
            .into_iter()
            .filter(|(_, plats)| plats.len() == 1)
            .map(|((f, d), plats)| (f, d, plats.into_iter().next().unwrap_or_default()))
            .collect()
    }

    pub fn counts(&self) -> BTreeMap<&'static str, usize> {
        let mut m = BTreeMap::new();
        m.insert("formats", self.formats.len());
        m.insert("oracles", self.oracles.len());
        m.insert("availability", self.availability.len());
        m.insert("verifications", self.verifications.len());
        m.insert("hosts", self.hosts.len());
        m.insert("fixtures", self.fixtures.len());
        m
    }

    /// Normalised JSON snapshot: flat entity arrays, references by ID.
    ///
    /// Normalised rather than nested because format<->oracle is many-to-many —
    /// nesting would duplicate every oracle under each format it verifies, and
    /// the duplicates would drift.
    ///
    /// Generated, and committed so coverage changes show up in a diff. Nothing
    /// reads it back; the planner uses the TOML directly.
    pub fn export_json(&self) -> Result<String, String> {
        serde_json::to_string_pretty(self).map_err(|e| e.to_string())
    }
}
