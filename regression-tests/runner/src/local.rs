//! `local.toml` — the one gitignored file that names this network.
//!
//! Everything private lives here and nowhere else: the corpus path, the
//! machines, their ssh targets. It is the only file that has to be carried
//! between systems, and the only place a real address may appear. This is a
//! public repository; a committed path or hostname is a leak, not a
//! convenience. `local.toml.example` is the committed template.
//!
//! It replaced three files that each held a piece — `data/hosts.toml`,
//! `scripts/hosts.local` and an earlier fixture-root-only `local.toml`. Three
//! gitignored files meant three chances to leave one behind when moving to a
//! new machine, and the capability half of the inventory drifted from the
//! connection half.

use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Default, Deserialize)]
pub struct LocalConfig {
    /// Where a run reads fixtures. **Local disk.** A run that reads a network
    /// share is slower, fails differently when the share is not mounted, and
    /// makes the share's address a runtime dependency of the suite — which on
    /// a public repository is how a private path ends up in a doc.
    pub fixture_root: Option<String>,

    /// Where `fixtures --sync` fetches from: the NAS, an external disk, another
    /// machine. Used for that one copy and nothing else. Optional — a host
    /// that already has its corpus needs no source at all.
    pub corpus_source: Option<String>,
    /// Not read yet — `--report-root` is the only way in today. Declared so a
    /// local.toml carrying it does not fail to parse.
    #[allow(dead_code)]
    pub report_root: Option<String>,
    #[serde(default, rename = "host")]
    pub hosts: Vec<LocalHost>,
}

/// One machine. The first five fields are what the planner needs; `ssh`,
/// `repo` and `shell` are what `scripts/regress-all.sh` needs to drive it.
#[derive(Debug, Deserialize)]
pub struct LocalHost {
    pub id: String,
    pub platform: String,
    pub transport: String,
    #[serde(default = "yes")]
    pub can_produce: bool,
    #[serde(default = "yes")]
    pub can_verify: bool,
    #[serde(default)]
    pub notes: Option<String>,

    /// `user@host`. Absent for a host the driver cannot reach over ssh.
    #[serde(default)]
    pub ssh: Option<String>,
    /// Repository path on that host, relative to its home directory.
    #[serde(default)]
    pub repo: Option<String>,
    /// `bash`, or a login-shell invocation for a host whose toolchain is not
    /// on the non-interactive PATH. macOS needs `zsh -lc`.
    #[serde(default)]
    pub shell: Option<String>,
}

fn yes() -> bool {
    true
}

/// Load `local.toml`, falling back to the committed example so a fresh clone
/// still plans and validates. Returns the path actually read.
pub fn load(regression_dir: &Path) -> (LocalConfig, PathBuf, Option<String>) {
    let real = regression_dir.join("local.toml");
    let path = if real.is_file() {
        real
    } else {
        regression_dir.join("local.toml.example")
    };
    match fs::read_to_string(&path) {
        Ok(text) => match toml::from_str::<LocalConfig>(&text) {
            Ok(c) => (c, path, None),
            Err(e) => (
                LocalConfig::default(),
                path.clone(),
                Some(format!("{}: {}", path.display(), e)),
            ),
        },
        Err(e) => (
            LocalConfig::default(),
            path.clone(),
            Some(format!("{}: {}", path.display(), e)),
        ),
    }
}

/// The distribution source, if this host has one configured.
///
/// Never answers from `local.toml.example`: its value is a placeholder, and
/// syncing from a placeholder is a confusing failure rather than a helpful
/// default. Same reason `query hosts` refuses to emit the example's addresses.
pub fn corpus_source(regression_dir: &Path) -> Option<PathBuf> {
    let (cfg, from, _) = load(regression_dir);
    if from.extension().is_some_and(|e| e == "example") {
        return None;
    }
    cfg.corpus_source
        .filter(|s| !s.trim().is_empty())
        .map(|s| resolve(&s, regression_dir))
}

/// A relative path in `local.toml` is relative to the file itself, not to
/// whatever directory the command happened to be run from. `fixture_root =
/// "fixtures"` has to mean the same thing from anywhere.
fn resolve(value: &str, regression_dir: &Path) -> PathBuf {
    let p = PathBuf::from(value);
    if p.is_absolute() || value.starts_with("//") || value.starts_with("\\\\") {
        p
    } else {
        regression_dir.join(p)
    }
}

/// Fixture root from, in order: an explicit flag, the environment, then
/// `local.toml`. Never hardcodes a path — see FIXTURES.md.
pub fn discover_root(explicit: Option<PathBuf>, regression_dir: &Path) -> Option<PathBuf> {
    if let Some(p) = explicit {
        return Some(p);
    }
    if let Some(env) = std::env::var_os("RB_FIXTURE_ROOT") {
        return Some(PathBuf::from(env));
    }
    let (cfg, _, _) = load(regression_dir);
    cfg.fixture_root
        .filter(|s| !s.trim().is_empty())
        .map(|s| resolve(&s, regression_dir))
}
