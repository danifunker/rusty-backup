//! Case manifest schema and loader.
//!
//! A manifest is a TOML file under `cases/` describing one or more regression
//! cases. Cases are data, never code: the whole point is that the matrix can
//! grow to thousands of entries without the harness changing.
//!
//! Minimal single-command case:
//!
//! ```toml
//! [meta]
//! tier = 0
//! group = "cli.basics"
//!
//! [[case]]
//! id = "cli.basics.version"
//! description = "--version exits 0 and prints something version-shaped"
//! args = ["--version"]
//! expect_exit = 0
//! stdout_matches = ["[0-9]+\\.[0-9]+"]
//! ```
//!
//! Multi-step case (setup, act, verify) — the shape tiers 3 through 5 need:
//!
//! ```toml
//! [[case]]
//! id = "fs.fat16.put-roundtrip"
//! fixture = "fs.fat16.dos622.hd"
//!
//! [[case.step]]
//! args = ["put", "{scratch}/payload.txt", "{fixture_copy}:/PAYLOAD.TXT"]
//! expect_exit = 0
//!
//! [[case.step]]
//! args = ["get", "{fixture_copy}:/PAYLOAD.TXT", "{scratch}/back.txt"]
//! expect_exit = 0
//! files_identical = [["{scratch}/payload.txt", "{scratch}/back.txt"]]
//!
//! [[case.step]]
//! args = ["fsck", "{fixture_copy}", "--format", "json"]
//! expect_exit = 0
//! fsck_clean = true
//! ```

use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

/// Top-level shape of one `.toml` file under `cases/`.
#[derive(Debug, Deserialize)]
pub struct Manifest {
    pub meta: Meta,
    #[serde(default, rename = "case")]
    pub cases: Vec<Case>,
}

#[derive(Debug, Deserialize)]
pub struct Meta {
    /// Default tier for every case in the file. A case may override.
    pub tier: u8,
    /// Human-facing grouping, used to cluster failures in the summary.
    pub group: String,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Case {
    /// Stable, globally unique. Renaming one breaks run-to-run comparison,
    /// so treat it as an API.
    pub id: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub tier: Option<u8>,

    /// Logical fixture ID, resolved through the catalogue. A case naming a
    /// fixture that is not in the catalogue resolves to `skip-fixture` —
    /// never an error. See FIXTURES.md.
    #[serde(default)]
    pub fixture: Option<String>,

    /// Restrict to these platforms (`windows`, `linux`, `macos`, ...).
    /// Empty means "runs everywhere".
    #[serde(default)]
    pub platforms: Vec<String>,

    /// External tools that must be on PATH. Absent tool -> `skip-tool`.
    #[serde(default)]
    pub requires: Vec<String>,

    /// Case writes to physical media; requires `--allow-hardware`.
    #[serde(default)]
    pub hardware: bool,

    /// Wall-clock budget for the whole case.
    #[serde(default)]
    pub timeout_ms: Option<u64>,

    // ---- single-step sugar -------------------------------------------------
    // A case that is just one command can skip the [[case.step]] table.
    #[serde(default)]
    pub args: Option<Vec<String>>,
    #[serde(default)]
    pub expect_exit: Option<i32>,

    #[serde(default, rename = "step")]
    pub steps: Vec<Step>,
}

/// One `rb-cli` invocation plus the assertions that apply to its result.
#[derive(Debug, Default, Deserialize)]
pub struct Step {
    pub args: Vec<String>,

    /// Expected process exit code. Values come from `src/cli/exit.rs`:
    /// 0 success, 1 generic failure, 2 usage error, 3 not found,
    /// 4 permission denied, 5 user declined.
    #[serde(default)]
    pub expect_exit: Option<i32>,

    /// Output must parse as the JSON envelope and `status.error` must equal
    /// this. Validating the envelope on every structured call means an
    /// envelope regression shows up across the whole matrix at once rather
    /// than in one hand-written test.
    #[serde(default)]
    pub expect_envelope_ok: Option<bool>,

    #[serde(default)]
    pub stdout_contains: Vec<String>,
    #[serde(default)]
    pub stdout_matches: Vec<String>,
    #[serde(default)]
    pub stderr_empty: bool,

    /// `result`-relative dotted selectors, e.g. `partitions.0.fs_type`.
    #[serde(default)]
    pub json_equals: Vec<JsonEquals>,
    #[serde(default)]
    pub json_exists: Vec<String>,

    #[serde(default)]
    pub files_exist: Vec<String>,
    #[serde(default)]
    pub files_identical: Vec<Vec<String>>,
    #[serde(default)]
    pub file_sha256: Vec<FileSha>,

    /// A `fsck --format json` step whose report must carry no issues.
    #[serde(default)]
    pub fsck_clean: bool,
}

#[derive(Debug, Deserialize)]
pub struct JsonEquals {
    pub path: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct FileSha {
    pub path: String,
    pub sha256: String,
}

#[derive(Debug)]
pub struct LoadError {
    pub path: PathBuf,
    pub message: String,
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.path.display(), self.message)
    }
}

/// Recursively load every `.toml` under `dir`.
///
/// Returns the manifests that parsed *and* the errors for those that did not.
/// A malformed manifest must not stop a run — it is reported as a harness
/// error against the file and the rest of the matrix still executes. This is
/// the prime directive applied to the harness's own inputs.
pub fn load_dir(dir: &Path) -> (Vec<(PathBuf, Manifest)>, Vec<LoadError>) {
    let mut out = Vec::new();
    let mut errs = Vec::new();
    walk(dir, &mut out, &mut errs);
    out.sort_by(|a, b| a.0.cmp(&b.0));
    (out, errs)
}

fn walk(dir: &Path, out: &mut Vec<(PathBuf, Manifest)>, errs: &mut Vec<LoadError>) {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) => {
            errs.push(LoadError {
                path: dir.to_path_buf(),
                message: format!("cannot read directory: {}", e),
            });
            return;
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk(&path, out, errs);
        } else if path.extension().map(|e| e == "toml").unwrap_or(false) {
            match load_file(&path) {
                Ok(m) => out.push((path, m)),
                Err(e) => errs.push(e),
            }
        }
    }
}

fn load_file(path: &Path) -> Result<Manifest, LoadError> {
    let text = fs::read_to_string(path).map_err(|e| LoadError {
        path: path.to_path_buf(),
        message: format!("cannot read: {}", e),
    })?;

    let manifest: Manifest = toml::from_str(&text).map_err(|e| LoadError {
        path: path.to_path_buf(),
        message: format!("malformed manifest: {}", e),
    })?;

    validate(path, &manifest)?;
    Ok(manifest)
}

fn validate(path: &Path, m: &Manifest) -> Result<(), LoadError> {
    for case in &m.cases {
        let has_sugar = case.args.is_some();
        let has_steps = !case.steps.is_empty();
        if has_sugar && has_steps {
            return Err(LoadError {
                path: path.to_path_buf(),
                message: format!(
                    "case '{}' sets both `args` and `[[case.step]]`; use one or the other",
                    case.id
                ),
            });
        }
        if !has_sugar && !has_steps {
            return Err(LoadError {
                path: path.to_path_buf(),
                message: format!("case '{}' has no `args` and no `[[case.step]]`", case.id),
            });
        }
        for pair in case.steps.iter().flat_map(|s| s.files_identical.iter()) {
            if pair.len() != 2 {
                return Err(LoadError {
                    path: path.to_path_buf(),
                    message: format!(
                        "case '{}': files_identical takes exactly two paths, got {}",
                        case.id,
                        pair.len()
                    ),
                });
            }
        }
    }
    Ok(())
}

impl Case {
    /// Normalise the single-step sugar into the same `Step` list every case
    /// executes through, so the runner has exactly one code path.
    pub fn resolved_steps(&self) -> Vec<Step> {
        if let Some(args) = &self.args {
            vec![Step {
                args: args.clone(),
                expect_exit: Some(self.expect_exit.unwrap_or(0)),
                ..Default::default()
            }]
        } else {
            self.steps
                .iter()
                .map(|s| Step {
                    args: s.args.clone(),
                    expect_exit: s.expect_exit,
                    expect_envelope_ok: s.expect_envelope_ok,
                    stdout_contains: s.stdout_contains.clone(),
                    stdout_matches: s.stdout_matches.clone(),
                    stderr_empty: s.stderr_empty,
                    json_equals: s
                        .json_equals
                        .iter()
                        .map(|j| JsonEquals {
                            path: j.path.clone(),
                            value: j.value.clone(),
                        })
                        .collect(),
                    json_exists: s.json_exists.clone(),
                    files_exist: s.files_exist.clone(),
                    files_identical: s.files_identical.clone(),
                    file_sha256: s
                        .file_sha256
                        .iter()
                        .map(|f| FileSha {
                            path: f.path.clone(),
                            sha256: f.sha256.clone(),
                        })
                        .collect(),
                    fsck_clean: s.fsck_clean,
                })
                .collect()
        }
    }
}
