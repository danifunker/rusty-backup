//! `rb-cli chmeta IMG@N PATH [--type TYPE] [--creator CREATOR]` — change
//! the type and/or creator code on an existing file. HFS / HFS+ /
//! ProDOS (`--type` only).

use anyhow::{anyhow, bail, Result};
use clap::Args;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;
use crate::cli::resolve::resolve_partition_rw;

#[derive(Debug, Args)]
pub struct ChmetaArgs {
    /// Image reference (`path` or `path@N`).
    pub image: ImageRef,
    /// Absolute Mac path of the file to update.
    pub path: String,
    /// New 4-character type code.
    #[arg(long = "type")]
    pub type_code: Option<String>,
    /// New 4-character creator code (HFS / HFS+ only).
    #[arg(long)]
    pub creator: Option<String>,
}

pub fn run(args: ChmetaArgs) -> Result<()> {
    if args.type_code.is_none() && args.creator.is_none() {
        bail!("chmeta: pass at least one of --type or --creator");
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

    let entry = super::ls::resolve_path(&mut *fs, &args.path)?;
    let new_type = args
        .type_code
        .as_deref()
        .or(entry.type_code.as_deref())
        .unwrap_or("BINA");
    let new_creator = args
        .creator
        .as_deref()
        .or(entry.creator_code.as_deref())
        .unwrap_or("????");
    fs.set_type_creator(&entry, new_type, new_creator)
        .map_err(|e| anyhow!("set_type_creator: {e}"))?;
    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;
    drop(fs);
    commit.commit()?;
    log_stderr(format!(
        "{}: type={new_type} creator={new_creator}",
        args.path
    ));
    Ok(())
}
