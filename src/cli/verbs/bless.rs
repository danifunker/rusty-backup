//! `rb-cli bless <set|show|pick> IMG@N [...]` — inspect or set the
//! bootable System Folder. HFS / HFS+ only; other filesystems return
//! `Unsupported`. Wraps [`EditableFilesystem::set_blessed_folder`] and
//! [`Filesystem::blessed_system_folder`].

use anyhow::{anyhow, bail, Result};
use clap::{Args, Subcommand};

use crate::cli::img_at::ImageRef;
use crate::cli::logging::{log_stderr, out_stdout};
use crate::cli::resolve::{resolve_partition_rw, resolve_partition_streaming};
use crate::fs::filesystem::Filesystem;

#[derive(Debug, Subcommand)]
pub enum BlessCommand {
    /// Bless the folder at PATH (mark it as the bootable System Folder).
    Set(BlessSetArgs),
    /// Print the volume's current blessed System Folder.
    Show(BlessShowArgs),
    /// Interactively browse the volume's folders and pick one to bless.
    Pick(BlessPickArgs),
}

#[derive(Debug, Args)]
pub struct BlessSetArgs {
    /// Image reference (`path` or `path@N`).
    pub image: ImageRef,
    /// Absolute Mac path of the folder to bless (e.g. `/System Folder`).
    pub path: String,
}

#[derive(Debug, Args)]
pub struct BlessShowArgs {
    /// Image reference (`path` or `path@N`).
    pub image: ImageRef,
}

#[derive(Debug, Args)]
pub struct BlessPickArgs {
    /// Image reference (`path` or `path@N`).
    pub image: ImageRef,
}

pub fn run(cmd: BlessCommand) -> Result<()> {
    match cmd {
        BlessCommand::Set(args) => apply_bless(&args.image, &args.path),
        BlessCommand::Show(args) => run_show(args),
        BlessCommand::Pick(args) => super::bless_pick::run(args),
    }
}

/// Open the partition read-write, bless the folder at `path`, and commit.
/// Shared by `bless set` and the interactive picker's confirm step.
pub fn apply_bless(image: &ImageRef, path: &str) -> Result<()> {
    let (file, ctx, commit) = resolve_partition_rw(&image.path, image.partition)?;
    log_stderr(&ctx.label);
    let mut fs = crate::fs::open_editable_filesystem(
        file,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening filesystem for write: {e}"))?;

    let entry = super::ls::resolve_path(&mut *fs, path)?;
    if !entry.is_directory() {
        bail!("bless: {path} is not a directory");
    }
    fs.set_blessed_folder(&entry)
        .map_err(|e| anyhow!("set_blessed_folder: {e}"))?;
    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;
    drop(fs);
    commit.commit()?;
    log_stderr(format!("blessed {path}"));
    Ok(())
}

fn run_show(args: BlessShowArgs) -> Result<()> {
    let (reader, ctx) = resolve_partition_streaming(&args.image.path, args.image.partition)?;
    log_stderr(&ctx.label);
    let mut fs = crate::fs::open_filesystem(
        reader,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening filesystem: {e}"))?;

    match blessed_info(&mut *fs) {
        Some(info) => out_stdout(format_blessed(&info)),
        None => out_stdout("No blessed System Folder set."),
    }
    Ok(())
}

/// The volume's blessed System Folder, resolved for display.
pub struct BlessedInfo {
    /// Catalog node ID of the blessed folder.
    pub cnid: u64,
    /// The folder's own (leaf) name.
    pub name: String,
    /// Full Mac path (`/a/b/c`), when it could be reconstructed by walking
    /// the directory tree. `None` if the CNID couldn't be located (e.g. a
    /// stale blessing pointing at a deleted folder).
    pub path: Option<String>,
}

/// Read the blessed System Folder (HFS / HFS+ only) and try to reconstruct
/// its full path. Returns `None` when the volume has no blessing or the
/// filesystem doesn't support blessing at all.
pub fn blessed_info(fs: &mut dyn Filesystem) -> Option<BlessedInfo> {
    let (cnid, name) = fs.blessed_system_folder()?;
    let path = find_dir_path_by_cnid(fs, cnid);
    Some(BlessedInfo { cnid, name, path })
}

/// Render a [`BlessedInfo`] as a one-line human string, e.g.
/// `"/System Folder (CNID 17)"` or `"System Folder (CNID 17)"` when the
/// path couldn't be rebuilt.
pub fn format_blessed(info: &BlessedInfo) -> String {
    match &info.path {
        Some(p) => format!("{p} (CNID {})", info.cnid),
        None => format!("{} (CNID {})", info.name, info.cnid),
    }
}

/// Depth-first walk of the directory tree from the root, returning the full
/// `/a/b/c` path of the directory whose CNID (`FileEntry::location`) matches
/// `target`. Bounded by a visited-node budget so a pathological volume can't
/// spin forever; returns `None` if not found within the budget.
fn find_dir_path_by_cnid(fs: &mut dyn Filesystem, target: u64) -> Option<String> {
    let root = fs.root().ok()?;
    let mut stack = vec![(root, String::new())];
    let mut budget: usize = 200_000;
    while let Some((dir, prefix)) = stack.pop() {
        let children = match fs.list_directory(&dir) {
            Ok(c) => c,
            Err(_) => continue,
        };
        for child in children {
            if !child.is_directory() {
                continue;
            }
            if budget == 0 {
                return None;
            }
            budget -= 1;
            let path = format!("{prefix}/{}", child.name);
            if child.location == target {
                return Some(path);
            }
            stack.push((child, path));
        }
    }
    None
}
