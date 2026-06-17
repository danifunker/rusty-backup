//! `rb-cli untar IMG[@N] ARCHIVE [DEST]` — import a `.tar.gz` / `.tar.zst`
//! / `.tar` archive's contents INTO a filesystem in a disk image. The
//! inverse of `tar`.
//!
//! Compression is auto-detected from the archive's magic. The directory
//! tree is recreated under DEST (default `/`), files are streamed in, and
//! symlinks are recreated where the target filesystem supports them
//! (skipped + counted where it can't — e.g. FAT/HFS). Unix modes are
//! applied best-effort.
//!
//! Conflicts: by default an existing destination name is an error; `--force`
//! overwrites, `--skip-existing` skips.

use anyhow::{anyhow, bail, Result};
use clap::Args;
use std::path::PathBuf;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;
use crate::cli::resolve::{resolve_partition_rw_forced, FsDispatchOverride};
use crate::fs::tar_import::{
    import_tar_from_path, ImportConflict, TarImportOptions, TarImportStats,
};

#[derive(Debug, Args)]
pub struct UntarArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Host archive to import (`.tar.gz` / `.tar.zst` / `.tar`; the
    /// compression is detected from the file's contents, not its name).
    pub archive: PathBuf,

    /// Destination directory inside the filesystem. Defaults to the root.
    #[arg(default_value = "/")]
    pub dest: String,

    /// Overwrite entries that already exist at the destination. Mutually
    /// exclusive with `--skip-existing`.
    #[arg(long, conflicts_with = "skip_existing")]
    pub force: bool,

    /// Skip entries that already exist at the destination. Mutually
    /// exclusive with `--force`.
    #[arg(long = "skip-existing", conflicts_with = "force")]
    pub skip_existing: bool,

    /// Do not apply archived Unix permission bits (mode) to imported files.
    #[arg(long = "no-permissions")]
    pub no_permissions: bool,

    /// Import macOS AppleDouble sidecars (`._*`) too. By default they are
    /// skipped as Mac metadata cruft.
    #[arg(long = "include-appledouble")]
    pub include_appledouble: bool,

    #[command(flatten)]
    pub fs_override: FsDispatchOverride,
}

pub fn run(args: UntarArgs) -> Result<()> {
    if !args.archive.is_file() {
        bail!("archive not found: {}", args.archive.display());
    }

    let (file, mut ctx, commit) = resolve_partition_rw_forced(
        &args.image.path,
        args.image.partition,
        args.fs_override.fs_type.as_deref(),
    )?;
    args.fs_override.apply(&mut ctx);
    log_stderr(&ctx.label);

    let mut fs = crate::fs::open_editable_filesystem(
        file,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening filesystem for writing: {e}"))?;

    let dest = super::ls::resolve_path(&mut *fs, &args.dest)
        .map_err(|e| anyhow!("resolving destination {:?}: {e}", args.dest))?;
    if !dest.is_directory() {
        bail!("destination {:?} is not a directory", args.dest);
    }

    let conflict = match (args.force, args.skip_existing) {
        (true, _) => ImportConflict::Overwrite,
        (_, true) => ImportConflict::Skip,
        _ => ImportConflict::Error,
    };
    let opts = TarImportOptions {
        conflict,
        apply_permissions: !args.no_permissions,
        skip_appledouble: !args.include_appledouble,
    };

    let progress = |s: &TarImportStats| {
        let done = s.files + s.dirs_created + s.symlinks;
        if done > 0 && done.is_multiple_of(200) {
            log_stderr(format!(
                "  importing... {} files, {} dirs",
                s.files, s.dirs_created
            ));
        }
    };

    let stats = import_tar_from_path(&mut *fs, &dest, &args.archive, &opts, &progress)
        .map_err(|e| anyhow!("importing {}: {e}", args.archive.display()))?;

    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;
    commit.commit()?;

    summarize(&args.archive, &args.dest, &stats);
    Ok(())
}

fn summarize(archive: &std::path::Path, dest: &str, stats: &TarImportStats) {
    log_stderr(format!(
        "Imported {} into {dest}: {} files, {} dirs, {} symlinks, {} bytes",
        archive.display(),
        stats.files,
        stats.dirs_created,
        stats.symlinks,
        stats.total_bytes
    ));
    if stats.symlinks_skipped > 0 {
        log_stderr(format!(
            "  {} symlink(s) skipped (this filesystem can't store symlinks)",
            stats.symlinks_skipped
        ));
    }
    if stats.appledouble_skipped > 0 {
        log_stderr(format!(
            "  {} macOS AppleDouble (._*) sidecar(s) skipped",
            stats.appledouble_skipped
        ));
    }
    if stats.invalid_names_skipped > 0 {
        log_stderr(format!(
            "  {} entr(ies) skipped (name not valid on this filesystem, e.g. macOS ._ cruft)",
            stats.invalid_names_skipped
        ));
    }
    if stats.skipped_existing > 0 {
        log_stderr(format!(
            "  {} entr(ies) skipped (already existed)",
            stats.skipped_existing
        ));
    }
    if stats.overwritten > 0 {
        log_stderr(format!("  {} entr(ies) overwritten", stats.overwritten));
    }
    if stats.other_skipped > 0 {
        log_stderr(format!(
            "  {} entr(ies) skipped (hardlinks / devices / not representable)",
            stats.other_skipped
        ));
    }
}
