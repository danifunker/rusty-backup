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
    pub fixture_root: Option<String>,
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
        .map(PathBuf::from)
}
