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

    /// Edit the file as text even if it looks binary. Almost never what you
    /// want — a round trip through an editor will not preserve arbitrary bytes.
    #[arg(long)]
    pub force_binary: bool,

    #[command(flatten)]
    pub fs_override: FsDispatchOverride,
}

pub fn run(args: EditArgs) -> Result<()> {
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
    if edited == before {
        log_stderr("No changes; nothing written");
        return Ok(());
    }

    let bytes = match encode_after_edit(&edited, &decoded.shape, args.force_substitute) {
        Ok(b) => b,
        Err(e @ TextEditError::Unrepresentable { .. }) => {
            bail!(
                "cannot write {} as {}\n  {e}\n  nothing was written; edit the character \
                 or pass --force-substitute",
                args.path,
                decoded.shape.encoding.label(),
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
        decoded.shape.encoding.label(),
        decoded.shape.ending.label(),
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
    let mut parts = shell_words::split(&chosen)
        .map_err(|e| anyhow!("cannot parse editor command {chosen:?}: {e}"))?;
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
