//! `rb-cli mkdir IMG[@N] PATH` — create a directory inside a filesystem.
//! Generic across every EditableFilesystem.

use anyhow::{anyhow, bail, Result};
use clap::Args;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;
#[cfg(feature = "remote")]
use crate::cli::parse::split_mac_path;
use crate::cli::resolve::resolve_partition_rw;
use crate::fs::filesystem::CreateDirectoryOptions;

#[derive(Debug, Args)]
pub struct MkdirArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Directory path to create. The parent must exist (no `-p`-style
    /// auto-creation in Phase B). A literal `/` in the new name is written
    /// `\/`; on HFS / HFS+ a `:`-separated path also works.
    pub path: String,

    /// Accepted for consistency with `ls`/`get`/`rm`; `mkdir` always treats
    /// the path as an exact literal path (it never globs), so glob
    /// metacharacters in a name are used verbatim with or without it.
    #[arg(short = 'L', long = "literal", alias = "no-glob")]
    pub literal: bool,
}

pub fn run(args: MkdirArgs) -> Result<()> {
    // Remote: `rb-cli mkdir rb://host:port/img@N /NEWDIR` — stage + apply.
    // Remote addressing is slash-only (the daemon resolves the path itself).
    #[cfg(feature = "remote")]
    if let Some(rref) = crate::remote::RemoteRef::parse(&args.image.path.to_string_lossy()) {
        let (parent_path, name) = split_mac_path(&args.path)?;
        if name.is_empty() {
            bail!("directory path has no basename");
        }
        let mut session = crate::remote::RemoteSession::connect(&rref.addr())?;
        let sid = session.open_session(&rref.path, args.image.partition)?;
        session.stage_mkdir(sid, &parent_path, &name)?;
        session.apply(sid)?;
        session.close_session(sid)?;
        return Ok(());
    }

    let (file, ctx, commit) = resolve_partition_rw(&args.image.path, args.image.partition)?;
    log_stderr(&ctx.label);
    let mut fs = crate::fs::open_editable_filesystem(
        file,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening filesystem for write: {e}"))?;

    // Resolve parent + leaf with the shared escape / colon grammar so a new
    // directory whose name contains a literal `/` can be created.
    let (parent, name) = super::ls::resolve_parent(&mut *fs, &args.path)?;
    if name.is_empty() {
        bail!("directory path has no basename");
    }
    if !parent.is_directory() {
        bail!("parent is not a directory: {}", args.path);
    }

    // Early duplicate check so the user-facing message names the path.
    let already = fs
        .list_directory(&parent)
        .map_err(|e| anyhow!("list_directory: {e}"))?
        .into_iter()
        .any(|e| e.name == name);
    if already {
        bail!("{} already exists", args.path);
    }

    fs.create_directory(&parent, &name, &CreateDirectoryOptions::default())
        .map_err(|e| anyhow!("create_directory: {e}"))?;
    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;
    drop(fs);
    commit.commit()?;
    Ok(())
}
