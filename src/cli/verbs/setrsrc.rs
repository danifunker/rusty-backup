//! `rb-cli setrsrc IMG@N PATH --from-file FILE` — write the resource
//! fork of an existing HFS / HFS+ file from host bytes. Pass an empty
//! file (or `/dev/null` with `--size 0`) to clear the fork.

use anyhow::{anyhow, Context, Result};
use clap::Args;
use std::fs::File;
use std::path::PathBuf;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;
use crate::cli::resolve::resolve_partition_rw;

#[derive(Debug, Args)]
pub struct SetRsrcArgs {
    /// Image reference (`path` or `path@N`).
    pub image: ImageRef,
    /// Absolute Mac path of the file whose resource fork should be replaced.
    pub path: String,
    /// Host file whose contents become the new resource fork.
    #[arg(long = "from-file")]
    pub from_file: PathBuf,
}

pub fn run(args: SetRsrcArgs) -> Result<()> {
    let (file, ctx) = resolve_partition_rw(&args.image.path, args.image.partition)?;
    log_stderr(&ctx.label);
    let mut fs = crate::fs::open_editable_filesystem(
        file,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening filesystem for write: {e}"))?;

    let entry = super::ls::resolve_path(&mut *fs, &args.path)?;
    let meta = std::fs::metadata(&args.from_file)
        .with_context(|| format!("stat {}", args.from_file.display()))?;
    let len = meta.len();
    let mut hf = File::open(&args.from_file)
        .with_context(|| format!("open {}", args.from_file.display()))?;
    fs.write_resource_fork(&entry, &mut hf, len)
        .map_err(|e| anyhow!("write_resource_fork: {e}"))?;
    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;
    log_stderr(format!("{}: wrote {} byte resource fork", args.path, len));
    Ok(())
}
