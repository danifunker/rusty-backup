//! Logging, progress, and color flag handling for `rb-cli`.
//!
//! The CLI distinguishes **results** (verb payloads — go to stdout) from
//! **logs and progress** (advisory — go to stderr). The split is strict
//! and survives `--quiet` and `--format json`. The two helpers exposed
//! here, [`out_stdout`] and [`log_stderr`], are the only places verbs
//! should print user-facing text from.
//!
//! Implementation is intentionally thin: most of the work delegates to
//! `env_logger` (for the `log` crate output) and `std::io::IsTerminal`
//! (for TTY detection). The few helpers wrap that with a CLI-shaped
//! configuration.

use anyhow::{Context, Result};
use clap::ValueEnum;
use std::fmt;
use std::io::{IsTerminal, Write};
use std::sync::OnceLock;

/// Verbosity tiers for `--log-level`. Order matches `log::Level` so the
/// mapping below is monotonic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum LogLevel {
    /// Only errors.
    Error,
    /// Warnings + errors (default).
    #[default]
    Warn,
    /// Info-level progress + warnings + errors.
    Info,
    /// Per-step diagnostic output. Verbose.
    Debug,
    /// Maximum detail, including library-level traces.
    Trace,
}

impl LogLevel {
    fn to_log_filter(self) -> log::LevelFilter {
        match self {
            Self::Error => log::LevelFilter::Error,
            Self::Warn => log::LevelFilter::Warn,
            Self::Info => log::LevelFilter::Info,
            Self::Debug => log::LevelFilter::Debug,
            Self::Trace => log::LevelFilter::Trace,
        }
    }
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        })
    }
}

/// `--progress` modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum ProgressMode {
    /// Auto: progress bars on TTY stderr, suppressed when piped (default).
    #[default]
    Auto,
    /// Force progress bars on even when stderr isn't a TTY.
    Always,
    /// Suppress progress bars entirely.
    Never,
}

/// `--color` modes. Honors `NO_COLOR` env var when set to anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum ColorMode {
    /// Auto: color when stdout/stderr is a TTY and `NO_COLOR` is unset (default).
    #[default]
    Auto,
    /// Force color on.
    Always,
    /// Suppress color entirely.
    Never,
}

/// Global flags shared across every verb. Parsed via clap at the root.
#[derive(Debug, Clone, clap::Args)]
pub struct GlobalFlags {
    /// Diagnostic verbosity for stderr logs.
    #[arg(long, value_enum, default_value_t = LogLevel::Warn, global = true)]
    pub log_level: LogLevel,

    /// Suppress all stderr output except errors and the final result.
    /// Mutually exclusive with `--log-level debug|trace`.
    #[arg(long, short = 'q', global = true, conflicts_with = "log_level")]
    pub quiet: bool,

    /// Progress bar behavior. `auto` is the default; `never` is the
    /// safest setting inside CI / cron / wrapper scripts.
    #[arg(long, value_enum, default_value_t = ProgressMode::Auto, global = true)]
    pub progress: ProgressMode,

    /// ANSI color usage. Honors the `NO_COLOR` env var when set.
    #[arg(long, value_enum, default_value_t = ColorMode::Auto, global = true)]
    pub color: ColorMode,

    /// Mirror full trace-level log output to PATH regardless of
    /// `--log-level`. Useful on Windows cmd where redirection is awkward.
    #[arg(long, global = true)]
    pub log_file: Option<std::path::PathBuf>,
}

impl Default for GlobalFlags {
    fn default() -> Self {
        Self {
            log_level: LogLevel::default(),
            quiet: false,
            progress: ProgressMode::default(),
            color: ColorMode::default(),
            log_file: None,
        }
    }
}

/// Effective runtime configuration after parsing flags and inspecting
/// the environment (TTY state, `NO_COLOR`). Set once via [`install`]; the
/// helpers below read from it.
#[derive(Debug, Clone)]
pub struct EffectiveLogging {
    pub log_level: LogLevel,
    pub quiet: bool,
    pub progress_enabled: bool,
    pub color_enabled: bool,
    pub log_file: Option<std::path::PathBuf>,
}

static EFFECTIVE: OnceLock<EffectiveLogging> = OnceLock::new();

/// Install the global logging configuration. Idempotent; the first
/// caller wins. Initializes `env_logger` as a side effect.
pub fn install(flags: &GlobalFlags) -> Result<&'static EffectiveLogging> {
    let stderr_is_tty = std::io::stderr().is_terminal();
    let stdout_is_tty = std::io::stdout().is_terminal();
    let no_color_env = std::env::var_os("NO_COLOR").is_some();

    let progress_enabled = match flags.progress {
        ProgressMode::Always => true,
        ProgressMode::Never => false,
        ProgressMode::Auto => stderr_is_tty,
    } && !flags.quiet;

    let color_enabled = match flags.color {
        ColorMode::Always => true,
        ColorMode::Never => false,
        ColorMode::Auto => !no_color_env && (stderr_is_tty || stdout_is_tty),
    };

    let log_level = if flags.quiet {
        LogLevel::Error
    } else {
        flags.log_level
    };

    let mut builder = env_logger::Builder::new();
    builder.filter_level(log_level.to_log_filter());
    builder.format_timestamp_millis();
    builder.target(env_logger::Target::Stderr);
    builder.write_style(if color_enabled {
        env_logger::WriteStyle::Always
    } else {
        env_logger::WriteStyle::Never
    });
    // `init()` panics if a logger is already installed; `try_init()` makes
    // the call site idempotent (matters when the GUI binary and the CLI
    // share library code that touches `log::*`).
    let _ = builder.try_init();

    // Open log file once and stash the path. Per-line mirror writes happen
    // through `log_stderr()` (best-effort — failures swallowed; the user
    // already has stderr).
    if let Some(path) = &flags.log_file {
        // Sanity check we can open; surface errors at install time rather
        // than on the first log line.
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("opening log file {}", path.display()))?;
    }

    let effective = EffectiveLogging {
        log_level,
        quiet: flags.quiet,
        progress_enabled,
        color_enabled,
        log_file: flags.log_file.clone(),
    };

    Ok(EFFECTIVE.get_or_init(|| effective))
}

/// Return the effective configuration if `install` has been called.
pub fn effective() -> Option<&'static EffectiveLogging> {
    EFFECTIVE.get()
}

/// Write a user-facing message to stdout (the "results" stream). Use this
/// when emitting verb payloads under `--format text`.
pub fn out_stdout(msg: impl AsRef<str>) {
    let mut stdout = std::io::stdout().lock();
    let _ = writeln!(stdout, "{}", msg.as_ref());
}

/// Write an advisory message to stderr (the "logs/progress" stream).
/// Honors `--quiet` (drops the message unless it's an error). Also
/// mirrors to `--log-file` when configured.
pub fn log_stderr(msg: impl AsRef<str>) {
    let eff = match effective() {
        Some(e) => e,
        None => {
            // Pre-install fallback: write directly so we don't lose early
            // errors during argument parsing failure paths.
            let mut stderr = std::io::stderr().lock();
            let _ = writeln!(stderr, "{}", msg.as_ref());
            return;
        }
    };
    if eff.quiet {
        return;
    }
    {
        let mut stderr = std::io::stderr().lock();
        let _ = writeln!(stderr, "{}", msg.as_ref());
    }
    if let Some(path) = &eff.log_file {
        if let Ok(mut f) = std::fs::OpenOptions::new().append(true).open(path) {
            let _ = writeln!(f, "{}", msg.as_ref());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_level_maps_monotonically() {
        // Smaller filter value = quieter. Asserting basic ordering.
        assert!(LogLevel::Error.to_log_filter() < LogLevel::Warn.to_log_filter());
        assert!(LogLevel::Warn.to_log_filter() < LogLevel::Info.to_log_filter());
        assert!(LogLevel::Info.to_log_filter() < LogLevel::Debug.to_log_filter());
        assert!(LogLevel::Debug.to_log_filter() < LogLevel::Trace.to_log_filter());
    }

    #[test]
    fn defaults() {
        let g = GlobalFlags::default();
        assert_eq!(g.log_level, LogLevel::Warn);
        assert!(!g.quiet);
        assert_eq!(g.progress, ProgressMode::Auto);
        assert_eq!(g.color, ColorMode::Auto);
        assert!(g.log_file.is_none());
    }
}
