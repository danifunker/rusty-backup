//! `rb-cli export IMG[@N] OUT [--format F]` — archive a whole volume to one file, the CLI side of
//! the browse view's Export Volume. Zip, tar (plain, gzip, zstd) on every filesystem; Mac
//! Archive, StuffIt and StuffIt-in-BinHex on a Mac volume, where they keep resource forks.
//!
//! The format is `--format`, else OUT's extension, else the filesystem's own: tar.gz for a Unix
//! filesystem (permissions, symlinks), Mac Archive for HFS / MFS, Zip for the rest. The list and
//! the default are `controller::commander::{volume_export_formats, default_volume_export}`, the
//! same the GUIs and the TUI offer; `rb-cli tar` stays for a subtree and its tar-only options.

#[cfg(feature = "rust173-polyfill")]
use crate::rust173_compat::IntIsMultipleOf as _;
use anyhow::{anyhow, bail, Context, Result};
use clap::Args;
use std::path::PathBuf;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;
use crate::cli::resolve::{resolve_partition_streaming_forced_inside, FsDispatchOverride};
use crate::controller::commander::{
    default_volume_export, export_format_named, export_format_of_path, volume_export_formats,
};
use crate::fs::export_selection::export_to_file;

#[derive(Debug, Args)]
pub struct ExportArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Output archive path.
    pub out: PathBuf,

    /// The archive format: zip, tar, tar.gz (tgz), tar.zst, and on a Mac volume mar, sit or
    /// sit.hqx. Without it, OUT's extension decides, then the filesystem.
    #[arg(long, value_name = "FORMAT")]
    pub format: Option<String>,

    /// Overwrite OUT if it already exists.
    #[arg(long)]
    pub force: bool,

    /// Password for encrypted containers or an encrypted filesystem (APFS FileVault).
    #[arg(long)]
    pub password: Option<String>,

    /// For a `.zip` holding more than one disk image, the archive entry to open.
    #[arg(long = "inside", value_name = "NAME")]
    pub inside: Option<String>,

    #[command(flatten)]
    pub fs_override: FsDispatchOverride,
}

pub fn run(args: ExportArgs) -> Result<()> {
    if args.out.exists() && !args.force {
        bail!(
            "output exists: {} (pass --force to overwrite)",
            args.out.display()
        );
    }
    let asked = match &args.format {
        Some(name) => Some(export_format_named(name).ok_or_else(|| {
            anyhow!("unknown format {name:?}: use zip, tar, tar.gz, tar.zst, mar, sit or sit.hqx")
        })?),
        None => None,
    };

    let pw_bytes = args.password.as_deref().map(|s| s.as_bytes());
    let (reader, mut ctx) = resolve_partition_streaming_forced_inside(
        &args.image.path,
        args.image.partition,
        pw_bytes,
        args.fs_override.fs_type.as_deref(),
        args.inside.as_deref(),
    )?;
    args.fs_override.apply(&mut ctx);
    log_stderr(&ctx.label);
    let mut fs = ctx
        .open_ro(reader, args.password.as_deref())
        .map_err(|e| anyhow!("opening filesystem: {e}"))?;

    let fs_type = fs.fs_type().to_string();
    let offered = volume_export_formats(&fs_type);
    let format = asked
        .or_else(|| export_format_of_path(&args.out))
        .unwrap_or_else(|| default_volume_export(&fs_type));
    if !offered.contains(&format) {
        bail!(
            "{} is not offered for a {fs_type} volume (it keeps resource forks, which only a Mac \
             volume has); use zip or a tar format",
            format.label()
        );
    }

    let root = fs.root().map_err(|e| anyhow!("reading the root: {e}"))?;
    let entries = fs
        .list_directory(&root)
        .map_err(|e| anyhow!("listing the root: {e}"))?;
    let progress = |path: &str, files: usize, _bytes: u64| {
        if files > 0 && files.is_multiple_of(200) {
            log_stderr(format!("  archiving... {files} files ({path})"));
        }
    };
    let summary = export_to_file(&mut *fs, &entries, &args.out, format, &progress, &|| false)
        .with_context(|| format!("exporting to {}", args.out.display()))?;
    log_stderr(format!(
        "Wrote {} ({}): {} files, {} bytes",
        args.out.display(),
        format.label(),
        summary.files,
        summary.bytes
    ));
    Ok(())
}
