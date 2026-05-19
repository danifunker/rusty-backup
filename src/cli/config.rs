//! Simple INI-shaped config file for `rb-cli`.
//!
//! Loaded once at startup; supplies *defaults* for flags. Explicit
//! CLI flags always win. See `docs/cli-todo.md` § "Config file" for
//! the rationale.
//!
//! ## Format
//!
//! ```ini
//! # comments start with # or ;
//! [defaults]
//! log-level = warn
//! progress = auto
//!
//! [backup]
//! checksum = sha256
//! format = chd
//!
//! [fsck]
//! prompt-timeout = 30
//! ```
//!
//! ## Locations (first match wins)
//!
//! - `--config PATH` (CLI override)
//! - `$XDG_CONFIG_HOME/rb-cli/rbcli.conf` (Linux/macOS)
//! - `~/.config/rb-cli/rbcli.conf` (Linux/macOS fallback)
//! - `%APPDATA%\rb-cli\rbcli.conf` (Windows)
//!
//! ## Phase G scope
//!
//! Phase G ships the loader + the standard locations + a thin
//! `rb-cli config init` to drop a commented template. Wiring every
//! flag to consult its config-file default is the long tail; the
//! initial set is `[defaults] log-level/quiet/progress/color` because
//! those are the cross-cutting flags every verb shares. Per-verb
//! sections (`[backup]`, `[restore]`) parse but don't influence
//! verb behavior yet — they'll be threaded through as the wiring
//! lands per verb in follow-up commits.

use anyhow::{Context, Result};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// In-memory config representation. Section names are lower-cased.
#[derive(Debug, Default, Clone)]
pub struct Config {
    pub sections: BTreeMap<String, BTreeMap<String, String>>,
}

impl Config {
    /// Parse a `key = value` INI-shaped file.
    pub fn parse(text: &str) -> Result<Self> {
        let mut cfg = Config::default();
        let mut current = "defaults".to_string();
        for (lineno, raw) in text.lines().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
                continue;
            }
            if let Some(rest) = line.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
                current = rest.trim().to_lowercase();
                continue;
            }
            let (k, v) = line.split_once('=').ok_or_else(|| {
                anyhow::anyhow!(
                    "line {} {raw:?}: expected `key = value` or `[section]`",
                    lineno + 1
                )
            })?;
            cfg.sections
                .entry(current.clone())
                .or_default()
                .insert(k.trim().to_lowercase(), v.trim().to_string());
        }
        Ok(cfg)
    }

    /// Load the config from `path` if it exists, otherwise return an
    /// empty config. Path-not-found is never an error here — the user
    /// just hasn't created it yet.
    pub fn load(path: &Path) -> Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(text) => Self::parse(&text).with_context(|| format!("parsing {}", path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(anyhow::Error::new(e).context(format!("reading {}", path.display()))),
        }
    }

    pub fn get(&self, section: &str, key: &str) -> Option<&str> {
        self.sections
            .get(&section.to_lowercase())
            .and_then(|m| m.get(&key.to_lowercase()))
            .map(|s| s.as_str())
    }
}

/// Canonical config-file location for the host OS. `None` if neither
/// `$HOME` nor `%APPDATA%` is set.
pub fn default_path() -> Option<PathBuf> {
    if cfg!(windows) {
        std::env::var_os("APPDATA").map(|app| PathBuf::from(app).join("rb-cli/rbcli.conf"))
    } else {
        let base = std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .or_else(|| dirs::home_dir().map(|h| h.join(".config")));
        base.map(|b| b.join("rb-cli/rbcli.conf"))
    }
}

/// Commented template that `rb-cli config init` writes.
pub const TEMPLATE: &str = "\
# rb-cli configuration file.
#
# Lines starting with `#` or `;` are comments. Section headers use
# `[section]`; keys are `key = value` on their own line. Per-section
# keys mirror the CLI flag spellings of that verb (without the leading
# `--`).
#
# Explicit CLI flags ALWAYS win over config-file values. The config
# only supplies defaults for flags the user didn't specify.

[defaults]
# log-level = warn        # error|warn|info|debug|trace
# quiet = false
# progress = auto         # auto|always|never
# color = auto            # auto|always|never

[backup]
# format = chd            # chd|dvd|vhd|zstd|raw
# checksum = sha256       # sha256|crc32

[restore]
# size = original         # original|minimum
# alignment = original    # original|modern1mb

[fsck]
# prompt-timeout = 30     # seconds; 0 = wait indefinitely

[put]
# type = BINA             # 4-char HFS type code
# creator = ????          # 4-char HFS creator code

[optical]
# resource-forks = appledouble  # data-only|native|appledouble|separate-rsrc|macbinary
";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_sections() {
        let text = "\
[defaults]
log-level = info
quiet = true

[backup]
format = zstd
";
        let cfg = Config::parse(text).unwrap();
        assert_eq!(cfg.get("defaults", "log-level"), Some("info"));
        assert_eq!(cfg.get("defaults", "quiet"), Some("true"));
        assert_eq!(cfg.get("backup", "format"), Some("zstd"));
        assert_eq!(cfg.get("backup", "missing"), None);
    }

    #[test]
    fn skips_comments_and_blank_lines() {
        let text = "\
# top comment
[defaults]

; another comment
log-level = warn
";
        let cfg = Config::parse(text).unwrap();
        assert_eq!(cfg.get("defaults", "log-level"), Some("warn"));
    }

    #[test]
    fn case_insensitive_lookup() {
        let text = "[Defaults]\nLog-Level = debug\n";
        let cfg = Config::parse(text).unwrap();
        assert_eq!(cfg.get("DEFAULTS", "log-level"), Some("debug"));
        assert_eq!(cfg.get("defaults", "LOG-LEVEL"), Some("debug"));
    }

    #[test]
    fn missing_file_yields_empty_config() {
        let cfg = Config::load(Path::new("/nonexistent/path/rbcli.conf")).unwrap();
        assert!(cfg.sections.is_empty());
    }
}
