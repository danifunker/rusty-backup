//! `rb-cli xattr <list|set|rm> IMG[@N] PATH [...]` — read and edit the
//! extended attributes on a file inside an image.
//!
//! The scriptable half of the File Info panel the GUI and TUI already offer.
//! Until this verb existed, xattrs could only be edited by hand in a window,
//! which is the wrong shape for the thing they are actually used for: an
//! appliance-image build step that has to put `security.capability` back on a
//! binary it replaced.
//!
//! Wraps [`Filesystem::list_xattrs`], [`EditableFilesystem::set_xattr`] and
//! [`EditableFilesystem::remove_xattr`]. Filesystems that store no xattrs say
//! so rather than silently doing nothing.

use anyhow::{anyhow, bail, Result};
use clap::{Args, Subcommand};

use crate::cli::img_at::ImageRef;
use crate::cli::logging::{log_stderr, out_stdout};
use crate::cli::resolve::{resolve_partition_rw, resolve_partition_streaming};

#[derive(Debug, Subcommand)]
pub enum XattrCommand {
    /// List the extended attributes on a file.
    List(XattrListArgs),
    /// Set (or replace) one extended attribute.
    Set(XattrSetArgs),
    /// Remove one extended attribute.
    Rm(XattrRmArgs),
}

#[derive(Debug, Args)]
pub struct XattrListArgs {
    /// Image reference (`path` or `path@N`).
    pub image: ImageRef,
    /// Absolute path of the file inside the filesystem.
    pub path: String,
}

#[derive(Debug, Args)]
pub struct XattrSetArgs {
    /// Image reference (`path` or `path@N`).
    pub image: ImageRef,
    /// Absolute path of the file inside the filesystem.
    pub path: String,
    /// Fully-qualified attribute name, including its namespace prefix —
    /// `user.` / `trusted.` / `security.` / `system.` (e.g.
    /// `security.capability`). A name without one cannot be stored and is
    /// refused.
    pub name: String,
    /// Attribute value. A `0x`-prefixed string is decoded as raw hex bytes
    /// (what a capability struct needs); anything else is stored as its UTF-8
    /// bytes. Mutually exclusive with `--value-file`.
    pub value: Option<String>,
    /// Read the value verbatim from a host file instead, for a binary value
    /// too awkward to hex-encode on a command line.
    #[arg(long, conflicts_with = "value")]
    pub value_file: Option<std::path::PathBuf>,
}

#[derive(Debug, Args)]
pub struct XattrRmArgs {
    /// Image reference (`path` or `path@N`).
    pub image: ImageRef,
    /// Absolute path of the file inside the filesystem.
    pub path: String,
    /// Fully-qualified attribute name to remove.
    pub name: String,
}

pub fn run(cmd: XattrCommand) -> Result<()> {
    match cmd {
        XattrCommand::List(args) => run_list(args),
        XattrCommand::Set(args) => run_set(args),
        XattrCommand::Rm(args) => run_rm(args),
    }
}

fn run_list(args: XattrListArgs) -> Result<()> {
    let (reader, ctx) = resolve_partition_streaming(&args.image.path, args.image.partition)?;
    log_stderr(&ctx.label);
    let mut fs = ctx
        .open_ro(reader, None)
        .map_err(|e| anyhow!("opening filesystem: {e}"))?;

    if !fs.supports_xattrs() {
        bail!(
            "{} does not store extended attributes",
            fs.as_ref().fs_type()
        );
    }
    let entry = super::ls::resolve_path(fs.as_mut(), &args.path)?;
    let attrs = fs
        .list_xattrs(&entry)
        .map_err(|e| anyhow!("list_xattrs: {e}"))?;
    if attrs.is_empty() {
        log_stderr(format!("{}: no extended attributes", args.path));
        return Ok(());
    }
    for a in &attrs {
        out_stdout(format!("{} = {}", a.name, a.value_display()));
    }
    Ok(())
}

fn run_set(args: XattrSetArgs) -> Result<()> {
    // Parse the value before touching the image: a bad hex string should cost
    // nothing.
    let value = match (&args.value, &args.value_file) {
        (Some(v), None) => crate::fs::xattr::parse_value(v)
            .map_err(|e| anyhow!("parsing value for {}: {e}", args.name))?,
        (None, Some(p)) => {
            std::fs::read(p).map_err(|e| anyhow!("reading value from {}: {e}", p.display()))?
        }
        (None, None) => bail!("xattr set: pass a VALUE or --value-file"),
        // clap's `conflicts_with` rejects both; belt and braces.
        (Some(_), Some(_)) => bail!("xattr set: pass either VALUE or --value-file, not both"),
    };

    let (file, ctx, commit) = resolve_partition_rw(&args.image.path, args.image.partition)?;
    log_stderr(&ctx.label);
    let mut fs = ctx
        .open_editable(file)
        .map_err(|e| crate::cli::resolve::write_open_error("opening filesystem for write", e))?;
    if !fs.as_filesystem().supports_xattrs() {
        bail!(
            "{} does not store extended attributes",
            fs.as_filesystem().fs_type()
        );
    }

    let entry = super::ls::resolve_path(fs.as_filesystem_mut(), &args.path)?;
    fs.set_xattr(&entry, &args.name, &value)
        .map_err(|e| anyhow!("set_xattr: {e}"))?;
    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;
    drop(fs);
    commit.commit()?;
    log_stderr(format!(
        "{}: set {} ({} byte(s))",
        args.path,
        args.name,
        value.len()
    ));
    Ok(())
}

fn run_rm(args: XattrRmArgs) -> Result<()> {
    let (file, ctx, commit) = resolve_partition_rw(&args.image.path, args.image.partition)?;
    log_stderr(&ctx.label);
    let mut fs = ctx
        .open_editable(file)
        .map_err(|e| crate::cli::resolve::write_open_error("opening filesystem for write", e))?;
    if !fs.as_filesystem().supports_xattrs() {
        bail!(
            "{} does not store extended attributes",
            fs.as_filesystem().fs_type()
        );
    }

    let entry = super::ls::resolve_path(fs.as_filesystem_mut(), &args.path)?;
    // Removing an absent attribute is a no-op at the driver level (matching
    // `removexattr` without flags), so say what happened rather than implying a
    // change that didn't occur.
    let had = fs
        .as_filesystem_mut()
        .list_xattrs(&entry)
        .map(|v| v.iter().any(|a| a.name == args.name))
        .unwrap_or(false);
    fs.remove_xattr(&entry, &args.name)
        .map_err(|e| anyhow!("remove_xattr: {e}"))?;
    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;
    drop(fs);
    commit.commit()?;
    if had {
        log_stderr(format!("{}: removed {}", args.path, args.name));
    } else {
        log_stderr(format!("{}: {} was not set", args.path, args.name));
    }
    Ok(())
}
