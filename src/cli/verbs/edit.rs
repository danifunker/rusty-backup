//! `rb-cli edit IMG@N /path` — open a text file from inside an image in
//! `$EDITOR` and write the result back.
//!
//! The editor never sees the file in its on-disk form. A DOS text file is
//! CP437 with CRLF endings, a classic-Mac one is MacRoman with bare CRs, and an
//! editor handed either of those either mangles it or silently rewrites it as
//! UTF-8/LF on save. So the round trip converts on the way out and restores on
//! the way back — see [`crate::model::text_edit`], which owns those rules and
//! is shared with the in-app editors.
//!
//! The write goes through [`crate::fs::replace`], so the edited file keeps its
//! permissions, owner, timestamps and type/creator: fixing one line of a config
//! file should not reset who is allowed to read it.

use anyhow::{anyhow, bail, Context, Result};
use clap::Args;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;
use crate::cli::resolve::{resolve_partition_rw_forced, FsDispatchOverride};
use crate::fs::filesystem::CreateFileOptions;
use crate::model::text_edit::{decode_for_edit, encode_after_edit, TextEditError, TextEncoding};

#[derive(Debug, Args)]
pub struct EditArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Path of the text file inside the filesystem.
    pub path: String,

    /// Editor to run. Defaults to `$VISUAL`, then `$EDITOR`, then `vi`
    /// (`notepad` on Windows).
    #[arg(long)]
    pub editor: Option<String>,

    /// Force the file's character encoding instead of inferring it.
    ///
    /// One of `utf8`, `cp437` (aliases `dos`, `oem`), `macroman` (`mac`),
    /// `latin1` (`amiga`), `shiftjis` (`sjis`, `x68000`). Inference uses the
    /// filesystem's convention, except that content which is already valid
    /// UTF-8 is taken at face value.
    #[arg(long)]
    pub encoding: Option<String>,

    /// Write a best-effort replacement for characters the file's encoding
    /// cannot represent, instead of refusing.
    ///
    /// Off by default: silently substituting is how a vintage text file quietly
    /// stops saying what it said.
    #[arg(long = "force-substitute")]
    pub force_substitute: bool,

    /// Write the file with these line endings instead of the ones it has.
    ///
    /// One of `lf` (`unix`), `crlf` (`dos`) or `cr` (`mac`). Without this the
    /// file keeps whatever it was found with — endings are read from the
    /// content, never assumed from the filesystem, because a DOS-formatted
    /// volume holds LF-only files all the time.
    ///
    /// Applies to the whole file, so it also repairs one with mixed endings.
    #[arg(long = "line-endings")]
    pub line_endings: Option<String>,

    /// Convert without opening an editor.
    ///
    /// For repairing files in bulk: `--line-endings crlf --no-edit` rewrites
    /// the endings and changes nothing else.
    #[arg(long = "no-edit")]
    pub no_edit: bool,

    /// Edit the file as text even if it looks binary. Almost never what you
    /// want — a round trip through an editor will not preserve arbitrary bytes.
    #[arg(long)]
    pub force_binary: bool,

    #[command(flatten)]
    pub fs_override: FsDispatchOverride,
}

pub fn run(args: EditArgs) -> Result<()> {
    let target_endings = match args.line_endings.as_deref() {
        Some(name) => Some(
            crate::model::text_edit::LineEnding::parse(name)
                .ok_or_else(|| anyhow!("unknown --line-endings value: {name}"))?,
        ),
        None => None,
    };
    if args.no_edit && target_endings.is_none() {
        bail!("--no-edit needs something to change; pass --line-endings too");
    }
    let forced = match args.encoding.as_deref() {
        Some(name) => Some(
            TextEncoding::parse(name).ok_or_else(|| anyhow!("unknown --encoding value: {name}"))?,
        ),
        None => None,
    };

    let (file, mut ctx, commit) = resolve_partition_rw_forced(
        &args.image.path,
        args.image.partition,
        args.fs_override.fs_type.as_deref(),
    )?;
    args.fs_override.apply(&mut ctx);
    let mut fs = ctx
        .open_editable(file)
        .map_err(|e| anyhow!("opening filesystem for write: {e}"))?;

    let fs_type = fs.fs_type().to_string();
    let (parent, name) = super::ls::resolve_parent(fs.as_filesystem_mut(), &args.path)?;
    let entry = super::ls::resolve_path(fs.as_filesystem_mut(), &args.path)?;
    if entry.is_directory() {
        bail!("{} is a directory", args.path);
    }

    let original = fs
        .read_file(&entry, usize::MAX)
        .map_err(|e| anyhow!("reading {}: {e}", args.path))?;

    let decoded = match decode_for_edit(&original, &fs_type, forced) {
        Ok(d) => d,
        Err(TextEditError::NotText { reason }) if !args.force_binary => {
            bail!(
                "{}: {reason}. Editing it as text would corrupt it; pass \
                 --force-binary only if you are certain",
                args.path
            );
        }
        // --force-binary: fall back to a lossless-as-possible Latin-1 view,
        // where every byte has a character, so at least nothing is dropped on
        // the way out.
        Err(_) => decode_for_edit(&original, &fs_type, Some(TextEncoding::Latin1))
            .map_err(|e| anyhow!("{}: {e}", args.path))?,
    };

    log_stderr(format!(
        "{}: {} bytes, {} endings, {}{}",
        args.path,
        original.len(),
        decoded.shape.ending.label(),
        decoded.shape.encoding.label(),
        if decoded.shape.mixed_endings {
            " (mixed endings - the dominant one will be used throughout)"
        } else {
            ""
        },
    ));

    // A real file on the host, so the editor behaves normally: a name it can
    // syntax-highlight, and a directory it can write a swapfile into.
    // The shape actually written: the file's own, with the requested endings
    // substituted when asked.
    let mut out_shape = decoded.shape;
    if let Some(target) = target_endings {
        if target != decoded.shape.ending {
            log_stderr(format!(
                "converting line endings {} -> {}",
                decoded.shape.ending.label(),
                target.label()
            ));
        }
        out_shape.ending = target;
    }

    if args.no_edit {
        // Nothing to edit, but there is still something to write: re-encode the
        // untouched text under the new shape.
        let bytes = encode_after_edit(&decoded.text, &out_shape, args.force_substitute)
            .map_err(|e| anyhow!("{}: {e}", args.path))?;
        let mut reader = std::io::Cursor::new(bytes.clone());
        crate::fs::replace::create_or_replace(
            fs.as_mut(),
            &parent,
            &name,
            &mut reader,
            bytes.len() as u64,
            &CreateFileOptions::default(),
            crate::fs::replace::ReplacePolicy::replace(),
        )
        .map_err(|e| anyhow!("writing {}: {e}", args.path))?;
        fs.sync_metadata()
            .map_err(|e| anyhow!("sync_metadata: {e}"))?;
        drop(fs);
        commit.commit()?;
        log_stderr(format!(
            "wrote {} bytes back to {} as {} with {} endings",
            bytes.len(),
            args.path,
            out_shape.encoding.label(),
            out_shape.ending.label(),
        ));
        return Ok(());
    }

    let tmp = tempfile::Builder::new()
        .prefix("rb-edit-")
        .tempdir()
        .context("creating a temp directory for the edit")?;
    let host_path = tmp.path().join(&name);
    std::fs::write(&host_path, decoded.text.as_bytes())
        .with_context(|| format!("writing {}", host_path.display()))?;
    let before = decoded.text.clone();

    launch_editor(args.editor.as_deref(), &host_path)?;

    let edited = std::fs::read_to_string(&host_path)
        .with_context(|| format!("reading back {}", host_path.display()))?;
    if edited == before && out_shape.ending == decoded.shape.ending {
        log_stderr("No changes; nothing written");
        return Ok(());
    }

    let bytes = match encode_after_edit(&edited, &out_shape, args.force_substitute) {
        Ok(b) => b,
        Err(e @ TextEditError::Unrepresentable { .. }) => {
            bail!(
                "cannot write {} as {}\n  {e}\n  nothing was written; edit the character \
                 or pass --force-substitute",
                args.path,
                out_shape.encoding.label(),
            );
        }
        Err(e) => bail!("{}: {e}", args.path),
    };

    let mut reader = std::io::Cursor::new(bytes.clone());
    let outcome = crate::fs::replace::create_or_replace(
        fs.as_mut(),
        &parent,
        &name,
        &mut reader,
        bytes.len() as u64,
        &CreateFileOptions::default(),
        crate::fs::replace::ReplacePolicy::replace(),
    )
    .map_err(|e| anyhow!("writing {}: {e}", args.path))?;

    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;
    drop(fs);
    commit.commit()?;

    let kept = outcome
        .preserved
        .as_ref()
        .filter(|p| !p.is_empty())
        .map(|p| format!(" ({} preserved)", p.summary()))
        .unwrap_or_default();
    log_stderr(format!(
        "wrote {} bytes back to {} as {} with {} endings{kept}",
        bytes.len(),
        args.path,
        out_shape.encoding.label(),
        out_shape.ending.label(),
    ));
    if outcome.unsafe_fallback {
        log_stderr(
            "Note: this filesystem cannot stage a replace (no rename), so the original \
             was removed before the new contents were written",
        );
    }
    Ok(())
}

/// Run the user's editor on `path`, waiting for it to exit.
fn launch_editor(explicit: Option<&str>, path: &std::path::Path) -> Result<()> {
    let chosen = explicit
        .map(|s| s.to_string())
        .or_else(|| {
            std::env::var("VISUAL")
                .ok()
                .filter(|s| !s.trim().is_empty())
        })
        .or_else(|| {
            std::env::var("EDITOR")
                .ok()
                .filter(|s| !s.trim().is_empty())
        })
        .unwrap_or_else(|| {
            if cfg!(windows) {
                "notepad".to_string()
            } else {
                "vi".to_string()
            }
        });

    // Split so `EDITOR="code --wait"` works, which is common enough that not
    // handling it looks like a bug.
    let mut parts = split_editor_command(&chosen)?;
    if parts.is_empty() {
        bail!("editor command is empty");
    }
    let program = parts.remove(0);

    log_stderr(format!("launching {chosen} ..."));
    let status = std::process::Command::new(&program)
        .args(&parts)
        .arg(path)
        .status()
        .with_context(|| format!("launching editor {program:?}"))?;
    if !status.success() {
        bail!("editor {program:?} exited with {status}; nothing was written");
    }
    Ok(())
}

/// Split an editor command into program + arguments.
///
/// POSIX word-splitting is wrong on Windows, where `\` is the path separator
/// and not an escape character: `shell_words::split` turns the perfectly
/// ordinary `EDITOR=C:\Windows\notepad.exe` into `C:Windowsnotepad.exe`, and the
/// spawn then fails with a "not found" naming a path the user never typed.
///
/// So the two platforms get the rule that matches their own shell. Unix keeps
/// POSIX splitting. Windows splits on whitespace, honours double quotes for a
/// path that contains spaces, and leaves every backslash alone — which is what
/// `cmd.exe` itself does.
///
/// On Windows an unquoted string that names an existing file is taken whole,
/// so `EDITOR=C:\Program Files\Vim\vim.exe` works without the user having to
/// know to quote it. That guess is only made when the file is actually there,
/// so it can never swallow a real trailing argument.
fn split_editor_command(chosen: &str) -> Result<Vec<String>> {
    #[cfg(not(windows))]
    {
        shell_words::split(chosen)
            .map_err(|e| anyhow!("cannot parse editor command {chosen:?}: {e}"))
    }
    #[cfg(windows)]
    {
        let trimmed = chosen.trim();
        if !trimmed.starts_with('"') && std::path::Path::new(trimmed).is_file() {
            return Ok(vec![trimmed.to_string()]);
        }
        let mut out = Vec::new();
        let mut cur = String::new();
        let mut in_quotes = false;
        let mut has_word = false;
        for c in trimmed.chars() {
            match c {
                '"' => {
                    in_quotes = !in_quotes;
                    has_word = true;
                }
                c if c.is_whitespace() && !in_quotes => {
                    if has_word {
                        out.push(std::mem::take(&mut cur));
                        has_word = false;
                    }
                }
                c => {
                    cur.push(c);
                    has_word = true;
                }
            }
        }
        if in_quotes {
            bail!("cannot parse editor command {chosen:?}: unbalanced quote");
        }
        if has_word {
            out.push(cur);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::split_editor_command;

    /// An editor with arguments has to keep working on both platforms — this is
    /// the `EDITOR="code --wait"` shape, which is common enough that breaking it
    /// reads as a bug.
    #[test]
    fn an_editor_with_arguments_splits_into_program_and_args() {
        assert_eq!(
            split_editor_command("code --wait").unwrap(),
            vec!["code".to_string(), "--wait".to_string()]
        );
    }

    #[test]
    fn a_quoted_path_with_spaces_stays_one_word() {
        assert_eq!(
            split_editor_command("\"/usr/local/my editor\" --wait").unwrap(),
            vec!["/usr/local/my editor".to_string(), "--wait".to_string()]
        );
    }

    /// The regression this splitter exists for. POSIX splitting eats the
    /// backslashes in a Windows path and the spawn fails naming a mangled path
    /// the user never typed.
    #[cfg(windows)]
    #[test]
    fn a_windows_path_keeps_its_backslashes() {
        assert_eq!(
            split_editor_command(r"C:\Windows\notepad.exe").unwrap(),
            vec![r"C:\Windows\notepad.exe".to_string()]
        );
        assert_eq!(
            split_editor_command(r#""C:\Program Files\Vim\vim.exe" --nofork"#).unwrap(),
            vec![
                r"C:\Program Files\Vim\vim.exe".to_string(),
                "--nofork".to_string()
            ]
        );
    }

    #[cfg(windows)]
    #[test]
    fn an_unbalanced_quote_is_reported_not_silently_accepted() {
        assert!(split_editor_command("\"C:\\bin\\ed.exe").is_err());
    }
}
