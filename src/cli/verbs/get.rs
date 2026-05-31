//! `rb-cli get IMG[@N] SRC HOST` — extract a file from a filesystem to
//! the host.
//!
//! Generic across every read-only filesystem the engine layer supports.
//! Phase B scope is single-file extract; recursive extract +
//! `--no-rsrc` / `--applesingle` / `--preserve-dates` lands in Phase D
//! alongside the batch verb.

use anyhow::{anyhow, bail, Result};
use clap::Args;
use std::path::PathBuf;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;
use crate::cli::resolve::resolve_partition_streaming_with_password;

#[derive(Debug, Args)]
pub struct GetArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Source path inside the filesystem.
    pub src: String,

    /// Destination path on the host.
    pub dst: PathBuf,

    /// Password for encrypted containers (currently: WinImage IMZ).
    #[arg(long)]
    pub password: Option<String>,
}

pub fn run(args: GetArgs) -> Result<()> {
    let pw_bytes = args.password.as_deref().map(|s| s.as_bytes());
    let (reader, ctx) = resolve_partition_streaming_with_password(
        &args.image.path,
        args.image.partition,
        pw_bytes,
    )?;
    log_stderr(&ctx.label);
    let mut fs = crate::fs::open_filesystem(
        reader,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening filesystem: {e}"))?;
    let entry = super::ls::resolve_path(&mut *fs, &args.src)?;
    if entry.is_directory() {
        bail!(
            "{src} is a directory; recursive extract lands in Phase D",
            src = args.src
        );
    }
    let mut out = std::fs::File::create(&args.dst)
        .map_err(|e| anyhow!("creating {}: {e}", args.dst.display()))?;
    fs.write_file_to(&entry, &mut out)
        .map_err(|e| anyhow!("write_file_to: {e}"))?;
    Ok(())
}
