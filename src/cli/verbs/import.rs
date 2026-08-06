//! `rb-cli import IMG[@N] DIR [DEST]` — copy a host directory tree INTO a
//! filesystem in a disk image. The bulk counterpart to `put` (one file) and
//! the no-tarball-needed counterpart to `untar`.
//!
//! The tree is recreated under DEST (default `/`), files are streamed in, and
//! symlinks are recreated where the target filesystem supports them (skipped +
//! counted where it can't — e.g. FAT/HFS). Unix modes are applied
//! best-effort. Entries are visited in sorted order so the same tree always
//! produces the same on-disk layout.
//!
//! `--expand-archives` unpacks tar archives found in the tree (detected by
//! content, so IRIX `.tardist` files count) into a directory named after each,
//! instead of copying them in as opaque files. Off by default — a
//! software-distribution disc usually wants its archives left intact.
//!
//! Conflicts: by default an existing destination name is an error; `--force`
//! overwrites, `--skip-existing` skips.

#[cfg(feature = "rust173-polyfill")]
use crate::rust173_compat::IntIsMultipleOf as _;
use anyhow::{anyhow, bail, Result};
use clap::Args;
use std::path::PathBuf;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;
use crate::cli::resolve::{resolve_partition_rw_forced, FsDispatchOverride};
use crate::fs::dir_import::{import_dir, DirImportOptions};
use crate::fs::import_sink::{ImportConflict, ImportOptions, ImportStats};

#[derive(Debug, Args)]
pub struct ImportArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Host directory whose contents are copied in. The directory itself is
    /// not created inside the image — its *contents* land under DEST.
    pub dir: PathBuf,

    /// Destination directory inside the filesystem. Defaults to the root.
    #[arg(default_value = "/")]
    pub dest: String,

    /// Unpack archives found in the tree into a directory named after each,
    /// instead of copying them in verbatim. Two families: tar (detected by the
    /// `ustar` magic, so IRIX `.tardist` and oddly-named archives are found and
    /// a gzipped disk image is not mistaken for one) and classic Mac
    /// (`.sit` / `.sea` / `.cpt` / `.hqx` / `.mar`, which land with both forks
    /// and their Finder type/creator intact).
    #[arg(long = "expand-archives")]
    pub expand_archives: bool,

    /// With `--expand-archives`: unpack each archive into the directory that
    /// held it rather than into a subdirectory named after it, so every
    /// archive shares one root.
    ///
    /// This is the shape an IRIX `inst` distribution wants — `.tardist` files
    /// carry flat product images and `inst` is pointed at a single directory
    /// holding all of them. Because archives are then expected to overlap
    /// (SGI freeware tardists all ship the same `fw_common*` product), this
    /// defaults to skipping entries that already exist; pass `--force` to
    /// overwrite instead.
    #[arg(long = "flatten-folders", requires = "expand_archives")]
    pub flatten_folders: bool,

    /// Overwrite entries that already exist at the destination. Mutually
    /// exclusive with `--skip-existing`.
    #[arg(long, conflicts_with = "skip_existing")]
    pub force: bool,

    /// Skip entries that already exist at the destination. Mutually
    /// exclusive with `--force`.
    #[arg(long = "skip-existing", conflicts_with = "force")]
    pub skip_existing: bool,

    /// Ignore the host's Unix mode and ownership. Imported entries then
    /// inherit uid/gid from the directory they land in and take the
    /// filesystem's default mode, the same rule `put` follows.
    #[arg(long = "no-permissions")]
    pub no_permissions: bool,

    /// Import macOS AppleDouble sidecars (`._*`) too. By default they are
    /// skipped as Mac metadata cruft.
    #[arg(long = "include-appledouble")]
    pub include_appledouble: bool,

    #[command(flatten)]
    pub fs_override: FsDispatchOverride,
}

/// Build the shared import options from the conflict/permission flags.
///
/// `flatten` shifts the default conflict policy from Error to Skip. Merging
/// several archives into one root makes overlap the norm rather than a
/// mistake — the four SGI freeware tardists in a typical set all carry the
/// same `fw_common*` product — so erroring out would make the flag unusable
/// on exactly the trees it exists for. An explicit `--force` still wins, and
/// the summary reports how many entries were skipped.
pub(crate) fn shared_options(
    force: bool,
    skip_existing: bool,
    no_permissions: bool,
    include_appledouble: bool,
    flatten: bool,
) -> ImportOptions {
    ImportOptions {
        conflict: match (force, skip_existing) {
            (true, _) => ImportConflict::Overwrite,
            (_, true) => ImportConflict::Skip,
            _ if flatten => ImportConflict::Skip,
            _ => ImportConflict::Error,
        },
        apply_permissions: !no_permissions,
        skip_appledouble: !include_appledouble,
    }
}

/// Progress ticker shared by every folder-import surface.
pub(crate) fn progress_cb(s: &ImportStats) {
    let done = s.files + s.dirs_created + s.symlinks;
    if done > 0 && done.is_multiple_of(200) {
        log_stderr(format!(
            "  importing... {} files, {} dirs",
            s.files, s.dirs_created
        ));
    }
}

pub fn run(args: ImportArgs) -> Result<()> {
    if !args.dir.is_dir() {
        bail!("not a directory: {}", args.dir.display());
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
        .map_err(|e| anyhow!("opening filesystem for writing: {e}"))?;

    let dest = super::ls::resolve_path(fs.as_filesystem_mut(), &args.dest)
        .map_err(|e| anyhow!("resolving destination {:?}: {e}", args.dest))?;
    if !dest.is_directory() {
        bail!("destination {:?} is not a directory", args.dest);
    }

    let opts = DirImportOptions {
        shared: shared_options(
            args.force,
            args.skip_existing,
            args.no_permissions,
            args.include_appledouble,
            args.flatten_folders,
        ),
        expand_archives: args.expand_archives,
        flatten_archives: args.flatten_folders,
    };

    let stats = import_dir(&mut *fs, &dest, &args.dir, &opts, &progress_cb)
        .map_err(|e| anyhow!("importing {}: {e}", args.dir.display()))?;

    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;
    commit.commit()?;

    summarize(&args.dir, &args.dest, &stats, args.expand_archives);
    Ok(())
}

/// Report what landed. Also nudges toward `--expand-archives` when the tree
/// held archives and the flag was off — the choice is a real one, and finding
/// out after burning a disc is the wrong time to learn the option exists.
pub(crate) fn summarize(dir: &std::path::Path, dest: &str, stats: &ImportStats, expanded: bool) {
    log_stderr(format!(
        "Imported {} into {dest}: {} files, {} dirs, {} symlinks, {} bytes",
        dir.display(),
        stats.files,
        stats.dirs_created,
        stats.symlinks,
        stats.total_bytes
    ));
    if stats.archives_expanded > 0 {
        log_stderr(format!(
            "  {} archive(s) unpacked into the image",
            stats.archives_expanded
        ));
    } else if !expanded {
        let archives = count_archives(dir);
        if archives > 0 {
            log_stderr(format!(
                "  Info: {archives} archive(s) were copied in as files. \
                 Pass --expand-archives to unpack them into the image instead."
            ));
        }
    }
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
            "  {} entr(ies) skipped (name not valid on this filesystem)",
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

/// Shallow count of expandable archives (tar or classic Mac) directly under
/// `dir`, for the hint above. Deliberately not recursive: this is a nudge, not
/// an audit, and sniffing every file in a deep tree after the import has
/// already run is wasted work.
fn count_archives(dir: &std::path::Path) -> usize {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return 0;
    };
    rd.flatten()
        .filter(|d| d.path().is_file())
        .filter(|d| {
            crate::fs::tar_import::looks_like_tar_archive(&d.path())
                || crate::fs::mac_archive_import::looks_like_mac_archive(&d.path())
        })
        .count()
}
