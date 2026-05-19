//! `rb-cli completions <shell>` — emit a shell-completion script.
//! `rb-cli install-completions [--shell SHELL]` — write the script to
//! the user-scoped canonical location for the detected (or specified)
//! shell.
//!
//! See `docs/cli-todo.md` § "Shell-completion install flow" for the
//! full per-shell path table and re-run semantics.

use anyhow::{Context, Result};
use clap::{Args, CommandFactory, Subcommand, ValueEnum};
use clap_complete::{generate, Shell};
use std::io::Write;
use std::path::PathBuf;

use crate::cli::logging::log_stderr;

/// Subset of clap_complete::Shell, exposed for our own help text. Maps
/// 1:1 to that enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ShellKind {
    Bash,
    Zsh,
    Fish,
    PowerShell,
    Elvish,
}

impl From<ShellKind> for Shell {
    fn from(s: ShellKind) -> Self {
        match s {
            ShellKind::Bash => Shell::Bash,
            ShellKind::Zsh => Shell::Zsh,
            ShellKind::Fish => Shell::Fish,
            ShellKind::PowerShell => Shell::PowerShell,
            ShellKind::Elvish => Shell::Elvish,
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum CompletionsCommand {
    /// Emit a shell completion script to stdout for the given shell.
    /// Packagers and sysadmins use this; ordinary users should reach
    /// for `rb-cli install-completions` instead.
    #[command(name = "completions")]
    Emit(EmitArgs),
}

#[derive(Debug, Args)]
pub struct EmitArgs {
    /// Shell to generate completions for.
    pub shell: ShellKind,
}

#[derive(Debug, Args)]
pub struct InstallArgs {
    /// Force the shell instead of auto-detecting from `$SHELL` /
    /// `$PSModulePath`.
    #[arg(long, value_enum)]
    pub shell: Option<ShellKind>,

    /// Override the install prefix (rarely needed). When set, the file
    /// is written under `PREFIX/<canonical-subdir>`.
    #[arg(long)]
    pub prefix: Option<PathBuf>,

    /// Print the script to stdout instead of writing to disk.
    #[arg(long, conflicts_with = "uninstall")]
    pub print: bool,

    /// Remove the installed completion file. No-op if it doesn't exist.
    #[arg(long, conflicts_with = "print")]
    pub uninstall: bool,
}

pub fn run_emit(args: EmitArgs) -> Result<()> {
    let shell: Shell = args.shell.into();
    let mut cmd = super::super::Cli::command();
    let bin = cmd.get_name().to_string();
    generate(shell, &mut cmd, bin, &mut std::io::stdout());
    Ok(())
}

pub fn run_install(args: InstallArgs) -> Result<()> {
    let shell = match args.shell {
        Some(s) => s,
        None => detect_shell()?,
    };

    if args.print {
        return run_emit(EmitArgs { shell });
    }

    let dest = canonical_completion_path(shell, args.prefix.as_deref())?;
    if args.uninstall {
        return uninstall(&dest);
    }

    install(shell, &dest)
}

fn install(shell: ShellKind, dest: &std::path::Path) -> Result<()> {
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let mut file =
        std::fs::File::create(dest).with_context(|| format!("creating {}", dest.display()))?;
    let mut cmd = super::super::Cli::command();
    let bin = cmd.get_name().to_string();
    generate(Shell::from(shell), &mut cmd, bin, &mut file);
    file.flush()?;
    log_stderr(format!(
        "Installed {shell:?} completions to {}",
        dest.display()
    ));
    log_stderr("Restart your shell (or `source` your rc file) to activate.");
    Ok(())
}

fn uninstall(dest: &std::path::Path) -> Result<()> {
    match std::fs::remove_file(dest) {
        Ok(()) => {
            log_stderr(format!("Removed {}", dest.display()));
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            log_stderr(format!("Nothing to remove at {}", dest.display()));
            Ok(())
        }
        Err(e) => Err(anyhow::Error::new(e).context(format!("removing {}", dest.display()))),
    }
}

fn detect_shell() -> Result<ShellKind> {
    // PowerShell on Windows: `PSModulePath` is set inside a pwsh session.
    if std::env::var_os("PSModulePath").is_some() {
        return Ok(ShellKind::PowerShell);
    }
    // Unix-ish: `$SHELL` is the login shell, used here as a best-effort
    // signal for "what is the user running."
    let shell = std::env::var("SHELL").unwrap_or_default();
    let base = std::path::Path::new(&shell)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    match base {
        "bash" => Ok(ShellKind::Bash),
        "zsh" => Ok(ShellKind::Zsh),
        "fish" => Ok(ShellKind::Fish),
        // Fall back to bash on unknown $SHELL — bash completions usually
        // work well enough in posh-shell wrappers.
        _ => Ok(ShellKind::Bash),
    }
}

/// Canonical user-scoped install path for `shell`. Prefix override slots
/// in front of the standard subdir when supplied.
fn canonical_completion_path(
    shell: ShellKind,
    prefix: Option<&std::path::Path>,
) -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("can't locate home directory"))?;
    let path = match shell {
        ShellKind::Bash => {
            let base = prefix.map(PathBuf::from).unwrap_or_else(|| {
                std::env::var_os("XDG_DATA_HOME")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| home.join(".local/share"))
            });
            base.join("bash-completion/completions/rb-cli")
        }
        ShellKind::Zsh => {
            let base = prefix
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join(".zsh"));
            base.join("completions/_rb-cli")
        }
        ShellKind::Fish => {
            let base = prefix.map(PathBuf::from).unwrap_or_else(|| {
                std::env::var_os("XDG_CONFIG_HOME")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| home.join(".config"))
            });
            base.join("fish/completions/rb-cli.fish")
        }
        ShellKind::PowerShell => {
            // Conservative default — write a profile-loadable .ps1 next
            // to the user's profile. Users source it from $PROFILE.
            let base = prefix
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join("Documents/PowerShell"));
            base.join("rb-cli-completions.ps1")
        }
        ShellKind::Elvish => {
            let base = prefix.map(PathBuf::from).unwrap_or_else(|| {
                std::env::var_os("XDG_CONFIG_HOME")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| home.join(".config"))
            });
            base.join("elvish/lib/rb-cli.elv")
        }
    };
    Ok(path)
}
