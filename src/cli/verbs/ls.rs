//! `rb-cli ls IMG[@N] [PATH]` — list a directory inside a filesystem.
//!
//! Generic across every read-only filesystem the engine layer
//! supports — FAT12/16/32, NTFS, exFAT, HFS, HFS+, ext2/3/4, btrfs,
//! XFS, ProDOS, ISO9660, AFFS, PFS3, SFS, EFS, and any superfloppy
//! the magic-byte sniffer recognizes.

use anyhow::{anyhow, bail, Result};
use clap::Args;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::{log_stderr, out_stdout};
use crate::cli::resolve::resolve_partition_ro;
use crate::fs::filesystem::Filesystem;

#[derive(Debug, Args)]
pub struct LsArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Path inside the filesystem (use `/` as the separator). Defaults to
    /// the volume root.
    #[arg(default_value = "/")]
    pub path: String,
}

pub fn run(args: LsArgs) -> Result<()> {
    let (file, ctx) = resolve_partition_ro(&args.image.path, args.image.partition)?;
    log_stderr(&ctx.label);
    let mut fs =
        crate::fs::open_filesystem(file, ctx.offset, ctx.type_byte, ctx.type_string.as_deref())
            .map_err(|e| anyhow!("opening filesystem: {e}"))?;

    let entry = resolve_path(&mut *fs, &args.path)?;
    if !entry.is_directory() {
        bail!("not a directory: {}", args.path);
    }
    let children = fs
        .list_directory(&entry)
        .map_err(|e| anyhow!("list_directory: {e}"))?;
    for c in children {
        let kind = if c.is_directory() { "DIR " } else { "FILE" };
        // Type/creator are HFS-specific; ignore on other filesystems.
        let t = c.type_code.as_deref().unwrap_or("    ");
        let cr = c.creator_code.as_deref().unwrap_or("    ");
        out_stdout(format!("{kind}  {:>10}  {t} {cr}  {}", c.size, c.name));
    }
    Ok(())
}

/// Walk `path` inside a generic filesystem, one component at a time.
pub fn resolve_path(fs: &mut dyn Filesystem, path: &str) -> Result<crate::fs::entry::FileEntry> {
    let mut current = fs.root().map_err(|e| anyhow!("root: {e}"))?;
    let trimmed = path.trim_start_matches('/').trim_end_matches('/');
    if trimmed.is_empty() {
        return Ok(current);
    }
    for component in trimmed.split('/') {
        if component.is_empty() {
            continue;
        }
        let children = fs
            .list_directory(&current)
            .map_err(|e| anyhow!("list_directory: {e}"))?;
        let next = children
            .into_iter()
            .find(|c| c.name == component)
            .ok_or_else(|| anyhow!("path component not found: {component}"))?;
        current = next;
    }
    Ok(current)
}
