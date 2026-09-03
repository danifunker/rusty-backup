//! `rb-cli mv IMG[@N] SRC DST` — rename a file or directory in place, the
//! scriptable twin of the browse view's Rename. Generic across every
//! EditableFilesystem that implements `rename`.

use anyhow::{anyhow, bail, Result};
use clap::Args;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::{log_stderr, out_stdout};
use crate::cli::resolve::{resolve_partition_rw_forced, FsDispatchOverride};
use crate::fs::entry::FileEntry;
use crate::fs::filesystem::Filesystem;

#[derive(Debug, Args)]
pub struct MvArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Path of the entry to rename. Always literal (never a glob). A literal
    /// `/` in a name is written `\/`; on HFS / HFS+ a `:`-separated path
    /// also works.
    pub src: String,

    /// The new name, or a full path inside the same directory as SRC. The
    /// entry keeps its contents, dates and attributes; only its name
    /// changes. Moving between directories is not supported.
    pub dst: String,

    /// Accepted for consistency with `ls`/`get`/`rm`; `mv` always treats
    /// both paths as exact literal paths (it never globs).
    #[arg(short = 'L', long = "literal", alias = "no-glob")]
    pub literal: bool,

    #[command(flatten)]
    pub fs_override: FsDispatchOverride,
}

pub fn run(args: MvArgs) -> Result<()> {
    // The daemon has no rename staging yet; say so instead of failing later.
    #[cfg(feature = "remote")]
    if crate::remote::RemoteRef::parse(&args.image.path.to_string_lossy()).is_some() {
        bail!("mv is not available over a remote session yet; run it on the daemon host");
    }

    let (file, mut ctx, commit) = resolve_partition_rw_forced(
        &args.image.path,
        args.image.partition,
        args.fs_override.fs_type.as_deref(),
    )?;
    args.fs_override.apply(&mut ctx);
    log_stderr(&ctx.label);
    let mut fs = ctx
        .open_editable(file)
        .map_err(|e| crate::cli::resolve::write_open_error("opening filesystem for write", e))?;

    let (parent, old_name) = super::ls::resolve_parent(fs.as_filesystem_mut(), &args.src)?;
    if old_name.is_empty() {
        bail!("source path has no basename");
    }
    let new_name = new_name_in_parent(fs.as_filesystem_mut(), &parent, &args.dst)?;
    if new_name.is_empty() {
        bail!("destination has no basename");
    }
    if old_name == new_name {
        bail!("source and destination are the same name");
    }
    fs.validate_name(&new_name)
        .map_err(|e| anyhow!("invalid name {new_name:?}: {e}"))?;

    let children = fs
        .list_directory(&parent)
        .map_err(|e| anyhow!("list_directory: {e}"))?;
    let entry = children
        .iter()
        .find(|c| c.name == old_name)
        .cloned()
        .ok_or_else(|| anyhow!("not found: {}", args.src))?;
    // A case-only rename on a case-folding volume is the entry itself, not a clash.
    let fold = fs.case_insensitive_lookup();
    let taken = children.iter().any(|c| {
        c.name != old_name
            && (c.name == new_name || (fold && c.name.eq_ignore_ascii_case(&new_name)))
    });
    if taken {
        bail!("{} already exists", args.dst);
    }

    fs.rename(&parent, &entry, &new_name)
        .map_err(|e| anyhow!("rename: {e}"))?;
    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;
    drop(fs);
    commit.commit()?;
    out_stdout(format!("Renamed {} -> {}", args.src, new_name));
    Ok(())
}

/// The new leaf name: `dst` as a bare name, or a path whose parent is `parent`.
fn new_name_in_parent(fs: &mut dyn Filesystem, parent: &FileEntry, dst: &str) -> Result<String> {
    let colon = super::ls::colon_mode(fs, dst);
    let (dst_parent, name) = crate::cli::parse::split_image_parent(dst, colon);
    if dst_parent.is_empty() && !dst.starts_with('/') {
        return Ok(name);
    }
    let (resolved, name) = super::ls::resolve_parent(fs, dst)?;
    if resolved.path != parent.path {
        bail!(
            "mv renames within one directory; {dst} is not in {}",
            if parent.path.is_empty() {
                "/"
            } else {
                parent.path.as_str()
            }
        );
    }
    Ok(name)
}
