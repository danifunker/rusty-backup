//! `rb-cli tar IMG[@N] [SRC] OUT` — archive a filesystem (or a subtree)
//! from a disk image to a single `.tar.gz` / `.tar.zst` / `.tar`, streamed
//! from one read pass (no host tree is staged).
//!
//! The point versus `get`: a tar archive preserves exact, case-sensitive
//! names and real symlinks. Extracting a case-sensitive source filesystem
//! (EFS, ext, XFS, HFSX, AFFS, …) directly onto a case-insensitive host
//! (macOS APFS, Windows NTFS) silently merges names that differ only in
//! case (`Makefile` vs `makefile`); the archive sidesteps that — the user
//! extracts it on a case-sensitive volume, or just inspects it.
//!
//! v1 archives the data fork only (HFS/HFS+ resource forks are dropped and
//! counted). Compression is inferred from OUT's extension (`.tar` = none,
//! `.tar.zst` = zstd, otherwise gzip) unless forced with `--gzip` /
//! `--zstd` / `--no-compress`.

use anyhow::{anyhow, bail, Context, Result};
use clap::Args;
use std::path::PathBuf;

use crate::cli::copy_paths::base_name_of;
use crate::cli::glob::compile_patterns;
use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;
use crate::cli::resolve::{resolve_partition_streaming_forced_inside, FsDispatchOverride};
use crate::fs::tar_export::{export_tar, TarCompression, TarExportOptions, TarExportStats};

#[derive(Debug, Args)]
pub struct TarArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Source path inside the filesystem to archive — a directory (archived
    /// recursively) or a single file. Use `/` for the whole volume.
    pub src: String,

    /// Output archive path. Compression is inferred from the extension
    /// (`.tar` = none, `.tar.zst` = zstd, otherwise gzip) unless one of
    /// `--gzip` / `--zstd` / `--no-compress` is given.
    pub out: PathBuf,

    /// Exclude entries whose path matches this glob (a directory match prunes
    /// its whole subtree). Repeatable.
    #[arg(long = "exclude")]
    pub exclude: Vec<String>,

    /// Force gzip (`.tar.gz`).
    #[arg(long, conflicts_with_all = ["zstd", "no_compress"])]
    pub gzip: bool,

    /// Force zstd (`.tar.zst`).
    #[arg(long, conflicts_with_all = ["gzip", "no_compress"])]
    pub zstd: bool,

    /// Force no compression (`.tar`).
    #[arg(long = "no-compress", conflicts_with_all = ["gzip", "zstd"])]
    pub no_compress: bool,

    /// Overwrite OUT if it already exists.
    #[arg(long)]
    pub force: bool,

    /// Match `--exclude` globs case-insensitively (default follows the
    /// filesystem's native rule).
    #[arg(long, conflicts_with = "case_sensitive")]
    pub ignore_case: bool,

    /// Match `--exclude` globs case-sensitively (default follows the
    /// filesystem's native rule).
    #[arg(long, conflicts_with = "ignore_case")]
    pub case_sensitive: bool,

    /// Password for encrypted containers (currently: WinImage IMZ, and
    /// password-protected `.zip` disks).
    #[arg(long)]
    pub password: Option<String>,

    /// For a `.zip` holding more than one disk image, the archive entry to
    /// open (e.g. `--inside backup.img`). Ignored for non-zip sources.
    #[arg(long = "inside", value_name = "NAME")]
    pub inside: Option<String>,

    #[command(flatten)]
    pub fs_override: FsDispatchOverride,
}

pub fn run(args: TarArgs) -> Result<()> {
    if args.out.exists() && !args.force {
        bail!(
            "output exists: {} (pass --force to overwrite)",
            args.out.display()
        );
    }

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
    let mut fs = crate::fs::open_filesystem(
        reader,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening filesystem: {e}"))?;

    // Resolve the source path to an entry.
    let entry = super::ls::resolve_path(&mut *fs, &args.src)
        .with_context(|| format!("resolving {:?}", args.src))?;

    // Compression: explicit flag wins, else infer from the output extension.
    let compression = if args.no_compress {
        TarCompression::None
    } else if args.zstd {
        TarCompression::Zstd
    } else if args.gzip {
        TarCompression::Gzip
    } else {
        TarCompression::infer_from_path(&args.out)
    };

    // Build the exclude matcher from the compiled glob patterns.
    let case_insensitive = match (args.ignore_case, args.case_sensitive) {
        (true, _) => true,
        (_, true) => false,
        _ => true,
    };
    let mut excludes = Vec::new();
    for ex in &args.exclude {
        excludes.extend(compile_patterns(ex, case_insensitive)?);
    }
    let exclude_fn = move |path: &str| excludes.iter().any(|p| p.is_match(path));

    // Whole-volume export (`/`) lays entries at the archive top level; a
    // subtree export roots them under the source basename (like `tar`).
    let archive_prefix = if entry.is_directory() && args.src != "/" && !args.src.is_empty() {
        base_name_of(&args.src)
    } else {
        String::new()
    };

    let out_file = std::fs::File::create(&args.out)
        .with_context(|| format!("creating {}", args.out.display()))?;
    let out = std::io::BufWriter::new(out_file);

    let opts = TarExportOptions {
        exclude: &exclude_fn,
        ..Default::default()
    };

    // Lightweight progress: a periodic stderr heartbeat so a large volume
    // doesn't look hung. ASCII only (no glyphs) per project UI rules.
    let progress = |s: &TarExportStats| {
        let done = s.files + s.dirs + s.symlinks;
        if done > 0 && done.is_multiple_of(200) {
            log_stderr(format!("  archiving... {} files, {} dirs", s.files, s.dirs));
        }
    };

    let stats = export_tar(
        &mut *fs,
        &entry,
        &archive_prefix,
        out,
        compression,
        &opts,
        &progress,
    )
    .with_context(|| format!("exporting {} to {}", args.src, args.out.display()))?;

    summarize(&args.out, compression, &stats);
    Ok(())
}

fn summarize(out: &std::path::Path, compression: TarCompression, stats: &TarExportStats) {
    let kind = match compression {
        TarCompression::Gzip => "tar.gz",
        TarCompression::Zstd => "tar.zst",
        TarCompression::None => "tar",
    };
    log_stderr(format!(
        "Wrote {} ({kind}): {} files, {} dirs, {} symlinks, {} bytes",
        out.display(),
        stats.files,
        stats.dirs,
        stats.symlinks,
        stats.total_bytes
    ));
    if stats.specials_skipped > 0 {
        log_stderr(format!(
            "  {} special file(s) skipped (devices/fifo/socket)",
            stats.specials_skipped
        ));
    }
    if stats.resource_forks_dropped > 0 {
        log_stderr(format!(
            "  {} resource fork(s) dropped (data fork only)",
            stats.resource_forks_dropped
        ));
    }
    if stats.excluded > 0 {
        log_stderr(format!("  {} entr(ies) excluded", stats.excluded));
    }
    if stats.case_collisions > 0 {
        log_stderr(format!(
            "  Note: {} case-only name collision(s) preserved in the archive \
             (these would have clobbered each other on a case-insensitive host)",
            stats.case_collisions
        ));
    }
}
