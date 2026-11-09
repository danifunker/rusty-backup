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
    /// Read by humans in the manifest; no report renders it yet.
    #[allow(dead_code)]
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
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

    // ---- flow tags (FLOWS.md) ----------------------------------------------
    /// `smoke` or `full`; `--level smoke` runs only smoke cases.
    #[serde(default)]
    pub level: Option<String>,
    /// The `fs` values that count as smoke; empty means every one does.
    #[serde(default)]
    pub smoke_fs: Vec<String>,
    /// Areas of the app this case exercises (`backup`, `inspect`, ...), for `--area`.
    #[serde(default)]
    pub area: Vec<String>,
    /// Filesystem matrix: the case runs once per value, with `{fs}` filled in.
    #[serde(default)]
    pub fs: Vec<String>,
    /// Front-ends the `ui` steps run on; one case per shell.
    #[serde(default)]
    pub shells: Vec<String>,
    /// Rows of docs/capabilities.md this case proves.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Set by matrix expansion: the shell and filesystem this instance runs.
    #[serde(skip)]
    pub shell: Option<String>,
    #[serde(skip)]
    pub fs_value: Option<String>,

    /// Extra environment for every step, e.g. `TZ = "UTC"` so local-time exports hash the same anywhere.
    #[serde(default)]
    pub env: std::collections::BTreeMap<String, String>,

    // ---- single-step sugar -------------------------------------------------
    // A case that is just one command can skip the [[case.step]] table.
    #[serde(default)]
    pub args: Option<Vec<String>>,
    #[serde(default)]
    pub expect_exit: Option<i32>,

    #[serde(default, rename = "step")]
    pub steps: Vec<Step>,
}

/// One `rb-cli` invocation plus the assertions that apply to its result, or a run of `ui` ops.
#[derive(Debug, Default, Clone, Deserialize)]
pub struct Step {
    #[serde(default)]
    pub args: Vec<String>,

    /// Front-end ops, run in the case's shell instead of an `rb-cli` invocation.
    #[serde(default)]
    pub ui: Vec<UiOp>,

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
    /// Text that must NOT appear. There is no way to express this with a
    /// regex — Rust's engine has no look-around — and "the output must not
    /// contain this" is a normal thing to assert.
    #[serde(default)]
    pub stdout_not_contains: Vec<String>,
    /// Diagnostics go to stderr, so a negative case asserting the *right*
    /// failure — not merely a non-zero exit — has nowhere else to look.
    #[serde(default)]
    pub stderr_contains: Vec<String>,
    #[serde(default)]
    pub stderr_not_contains: Vec<String>,
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

/// One front-end op; exactly one action field is set. FLOWS.md lists them.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UiOp {
    /// Switch to the named screen.
    pub tab: Option<String>,
    /// Move the focus to the control labelled this.
    pub focus: Option<String>,
    /// Step the focused control's value until it shows this.
    pub choose: Option<String>,
    /// Press one named key (`Enter`, `Down`, `Ctrl+K`, `s`).
    pub key: Option<String>,
    /// Press these keys in order.
    pub keys: Option<Vec<String>>,
    /// Type this text.
    #[serde(rename = "type")]
    pub text: Option<String>,
    /// Clear the focused text field, then type this.
    pub fill: Option<String>,
    /// Wait until the screen shows this.
    pub wait: Option<String>,
    /// The screen must show this now (after a short settle).
    pub expect: Option<String>,
    /// The screen must not show this.
    pub expect_not: Option<String>,
    /// Budget for `wait`, `focus` and `choose`.
    pub timeout_ms: Option<u64>,
    /// Run only on this shell; a shell-specific step (a TUI-only key, say).
    pub only: Option<String>,
}

impl UiOp {
    pub fn action_count(&self) -> usize {
        [
            self.tab.is_some(),
            self.focus.is_some(),
            self.choose.is_some(),
            self.key.is_some(),
            self.keys.is_some(),
            self.text.is_some(),
            self.fill.is_some(),
            self.wait.is_some(),
            self.expect.is_some(),
            self.expect_not.is_some(),
        ]
        .iter()
        .filter(|b| **b)
        .count()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct JsonEquals {
    pub path: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
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

    let mut manifest: Manifest = toml::from_str(&text).map_err(|e| LoadError {
        path: path.to_path_buf(),
        message: format!("malformed manifest: {}", e),
    })?;

    validate(path, &manifest)?;
    manifest.cases = manifest.cases.iter().flat_map(|c| c.expand()).collect();
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
        for (i, st) in case.steps.iter().enumerate() {
            let bad = if st.args.is_empty() == st.ui.is_empty() {
                Some("needs exactly one of `args` and `ui`".to_string())
            } else {
                st.ui
                    .iter()
                    .find(|o| o.action_count() != 1)
                    .map(|o| format!("ui op {:?} must set exactly one action", o))
            };
            if let Some(why) = bad {
                return Err(LoadError {
                    path: path.to_path_buf(),
                    message: format!("case '{}' step {}: {}", case.id, i, why),
                });
            }
        }
        let has_ui = case.steps.iter().any(|s| !s.ui.is_empty());
        if has_ui && case.shells.is_empty() {
            return Err(LoadError {
                path: path.to_path_buf(),
                message: format!("case '{}' has `ui` steps but no `shells`", case.id),
            });
        }
        if let Some(l) = &case.level {
            if l != "smoke" && l != "full" {
                return Err(LoadError {
                    path: path.to_path_buf(),
                    message: format!("case '{}': level must be smoke or full, not '{}'", case.id, l),
                });
            }
        }
        if let Some(f) = case.smoke_fs.iter().find(|f| !case.fs.contains(f)) {
            return Err(LoadError {
                path: path.to_path_buf(),
                message: format!("case '{}': smoke_fs '{}' is not in fs", case.id, f),
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
            self.steps.clone()
        }
    }

    /// The case as it runs: one instance per filesystem and shell, `{fs}` filled in.
    pub fn expand(&self) -> Vec<Case> {
        let fss: Vec<Option<&String>> = if self.fs.is_empty() {
            vec![None]
        } else {
            self.fs.iter().map(Some).collect()
        };
        let has_ui = self.steps.iter().any(|s| !s.ui.is_empty());
        let shells: Vec<Option<&String>> = if has_ui {
            self.shells.iter().map(Some).collect()
        } else {
            vec![None]
        };
        let mut out = Vec::new();
        for f in &fss {
            for sh in &shells {
                let mut c = self.clone();
                if let Some(f) = f {
                    c.id = format!("{}.{}", c.id, f);
                    let fill = |t: &str| t.replace("{fs}", f);
                    c.fixture = c.fixture.as_deref().map(fill);
                    c.description = c.description.as_deref().map(fill);
                    for st in &mut c.steps {
                        st.fill(&fill);
                    }
                    if c.level.as_deref() == Some("smoke")
                        && !self.smoke_fs.is_empty()
                        && !self.smoke_fs.contains(f)
                    {
                        c.level = Some("full".to_string());
                    }
                    c.fs_value = Some(f.to_string());
                }
                if let Some(sh) = sh {
                    c.id = format!("{}.{}", c.id, sh);
                    c.shell = Some(sh.to_string());
                }
                out.push(c);
            }
        }
        out
    }
}

impl Step {
    fn fill(&mut self, f: &dyn Fn(&str) -> String) {
        let each = |v: &mut Vec<String>| {
            for x in v.iter_mut() {
                *x = f(x);
            }
        };
        each(&mut self.args);
        each(&mut self.stdout_contains);
        each(&mut self.stdout_matches);
        each(&mut self.stdout_not_contains);
        each(&mut self.files_exist);
        for pair in &mut self.files_identical {
            each(pair);
        }
        for op in &mut self.ui {
            for v in [
                &mut op.tab,
                &mut op.focus,
                &mut op.choose,
                &mut op.key,
                &mut op.text,
                &mut op.fill,
                &mut op.wait,
                &mut op.expect,
                &mut op.expect_not,
            ]
            .into_iter()
            .flatten()
            {
                *v = f(v);
            }
            if let Some(ks) = &mut op.keys {
                each(ks);
            }
        }
    }
}
