//! `rb-cli cp SRCIMG SRC DSTIMG DST` — copy files / directory trees
//! between two disk images without staging through the host.
//!
//! Mirrors the `get` / `put` argument shape: positions 1 & 3 are image
//! references (`path` or `path@N`), positions 2 & 4 are paths inside the
//! respective filesystems. The source path (position 2) may be a glob;
//! the destination (position 4) follows `cp` semantics (copy into an
//! existing directory, or rename to a literal target).
//!
//! The actual copy mechanics — metadata translation, the spooled-temp
//! data bridge, name-fitting, free-space preflight — live in the
//! filesystem-agnostic [`crate::fs::copy`] engine so the GUI browse view
//! and a future `batch` `from_image` op can reuse them.

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, ValueEnum};

use crate::cli::copy_paths::{compute_glob_root, has_glob_chars, strip_root_prefix};
use crate::cli::glob::{collect_matches, compile_patterns};
use crate::cli::img_at::ImageRef;
use crate::cli::logging::{log_stderr, out_stdout};
use crate::cli::parse::split_mac_path;
use crate::cli::resolve::{
    resolve_partition_rw_forced, resolve_partition_streaming_forced, FsDispatchOverride,
};
use crate::fs::copy::{self, AttrPolicy, ConflictMode, CopyOptions, CopyStats, NamePolicy};
use crate::fs::entry::FileEntry;
use crate::fs::filesystem::{CreateDirectoryOptions, EditableFilesystem, Filesystem};
use crate::model::source_reader;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum NamePolicyArg {
    /// Truncate + `~N` uniquify names the destination rejects (default).
    Truncate,
    /// Skip entries whose name the destination rejects.
    Skip,
    /// Hard error on a name the destination rejects.
    Error,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum AttrPolicyArg {
    /// Carry every FS-specific attribute the destination supports (default).
    Preserve,
    /// Drop FS-specific attributes (NTFS files then inherit parent ACL).
    Skip,
}

#[derive(Debug, Args)]
pub struct CpArgs {
    /// Source image reference (`path` or `path@N` for the 1-based partition index).
    pub src_image: ImageRef,

    /// Source path or glob inside the source filesystem. Patterns
    /// containing `*`, `?`, `[`, or `{` walk the volume and copy every match.
    pub src: String,

    /// Destination image reference (`path` or `path@N`).
    pub dst_image: ImageRef,

    /// Destination path inside the destination filesystem. Copying into an
    /// existing directory (or a path ending in `/`) keeps the source
    /// basename; otherwise the destination is the literal target name.
    pub dst: String,

    /// Recursively copy directories. Without this, directory sources /
    /// matches are skipped with a warning.
    #[arg(long, short = 'r')]
    pub recursive: bool,

    /// Overwrite existing destination entries. Mutually exclusive with
    /// `--skip-existing`.
    #[arg(long, conflicts_with = "skip_existing")]
    pub force: bool,

    /// Skip when a destination entry already exists. Mutually exclusive
    /// with `--force`. Without either, an existing destination is an error.
    #[arg(long = "skip-existing", conflicts_with = "force")]
    pub skip_existing: bool,

    /// Exclude source paths matching this glob. Repeatable. Exclude wins.
    #[arg(long = "exclude")]
    pub exclude: Vec<String>,

    /// Match the source case-insensitively regardless of its native rule.
    #[arg(long, conflicts_with = "case_sensitive")]
    pub ignore_case: bool,

    /// Match the source case-sensitively regardless of its native rule.
    #[arg(long, conflicts_with = "ignore_case")]
    pub case_sensitive: bool,

    /// Policy for source names the destination filesystem rejects
    /// (too long / illegal characters). Default: truncate.
    #[arg(long = "names", value_enum, default_value = "truncate")]
    pub names: NamePolicyArg,

    /// Whether to carry FS-specific attributes (type/creator, Unix perms,
    /// DOS attribute bits, Amiga bits). Default: preserve.
    #[arg(long = "attrs", value_enum, default_value = "preserve")]
    pub attrs: AttrPolicyArg,

    /// Collapse a source tree into the destination directory when the
    /// destination filesystem has no subdirectories (CP/M, DFS, CBM, …).
    #[arg(long)]
    pub flatten: bool,

    /// Auto-create missing destination parent directories.
    #[arg(long)]
    pub parents: bool,

    /// Password for an encrypted source container (currently: WinImage IMZ).
    #[arg(long)]
    pub password: Option<String>,

    /// Force a specific filesystem dispatch for the SOURCE (e.g.
    /// `cpm:amstrad_data`). See `get --fs-type`.
    #[arg(long = "src-fs-type", value_name = "TYPE")]
    pub src_fs_type: Option<String>,

    /// Force a specific filesystem dispatch for the DESTINATION.
    #[arg(long = "dst-fs-type", value_name = "TYPE")]
    pub dst_fs_type: Option<String>,

    /// Scan the entire source image for recoverable text in the synthetic
    /// carve view (NDOS disks). Source-side only.
    #[arg(long = "carve-full")]
    pub carve_full: bool,
}

pub fn run(args: CpArgs) -> Result<()> {
    // On-device remote->remote copy: both images are rb:// refs on the same
    // daemon, so the file data never round-trips through the desktop.
    #[cfg(feature = "remote")]
    {
        let src_remote = crate::remote::RemoteRef::parse(&args.src_image.path.to_string_lossy());
        let dst_remote = crate::remote::RemoteRef::parse(&args.dst_image.path.to_string_lossy());
        match (src_remote, dst_remote) {
            (Some(s), Some(d)) => return remote_cp(&s, &d, &args),
            (Some(_), None) | (None, Some(_)) => bail!(
                "mixed local/remote cp over rb:// isn't supported yet; Phase 2 does on-device \
                 remote->remote (both images on the same daemon). Use `get` + `put` to bridge."
            ),
            (None, None) => {}
        }
    }

    // --- Guards -----------------------------------------------------------
    if same_file(&args.src_image.path, &args.dst_image.path) {
        bail!(
            "source and destination are the same image ({}); same-image partition-to-partition \
             copy isn't supported yet - copy to a different file",
            args.src_image.path.display()
        );
    }
    if source_reader::is_container_path(&args.dst_image.path)
        && !source_reader::is_editable_container_path(&args.dst_image.path)
    {
        bail!(
            "destination {} is a read-only container format; convert it to a writable image first \
             (rb-cli convert ...)",
            args.dst_image.path.display()
        );
    }

    let opts = CopyOptions {
        recursive: args.recursive,
        conflict: match (args.force, args.skip_existing) {
            (true, _) => ConflictMode::Force,
            (_, true) => ConflictMode::SkipExisting,
            _ => ConflictMode::Error,
        },
        names: match args.names {
            NamePolicyArg::Truncate => NamePolicy::Truncate,
            NamePolicyArg::Skip => NamePolicy::Skip,
            NamePolicyArg::Error => NamePolicy::Error,
        },
        attrs: match args.attrs {
            AttrPolicyArg::Preserve => AttrPolicy::Preserve,
            AttrPolicyArg::Skip => AttrPolicy::Skip,
        },
        flatten: args.flatten,
    };
    let case_insensitive = !args.case_sensitive; // default-insensitive, like get

    // --- Open source (read-only, streaming) ------------------------------
    let pw = args.password.as_deref().map(|s| s.as_bytes());
    let (reader, mut src_ctx) = resolve_partition_streaming_forced(
        &args.src_image.path,
        args.src_image.partition,
        pw,
        args.src_fs_type.as_deref(),
    )?;
    FsDispatchOverride {
        fs_type: args.src_fs_type.clone(),
        carve_full: args.carve_full,
    }
    .apply(&mut src_ctx);
    log_stderr(format!("source: {}", src_ctx.label));
    let mut src = crate::fs::open_filesystem(
        reader,
        src_ctx.offset,
        src_ctx.type_byte,
        src_ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening source filesystem: {e}"))?;

    // --- Open destination (read+write) -----------------------------------
    let (file, mut dst_ctx, commit) = resolve_partition_rw_forced(
        &args.dst_image.path,
        args.dst_image.partition,
        args.dst_fs_type.as_deref(),
    )?;
    if let Some(t) = &args.dst_fs_type {
        dst_ctx.type_string = Some(t.clone());
        dst_ctx.type_byte = 0;
        dst_ctx.label = format!("{} [--dst-fs-type {t}]", dst_ctx.label);
    }
    log_stderr(format!("destination: {}", dst_ctx.label));
    let mut dst = crate::fs::open_editable_filesystem(
        file,
        dst_ctx.offset,
        dst_ctx.type_byte,
        dst_ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening destination filesystem for write: {e}"))?;

    // --- Selection + copy ------------------------------------------------
    let stats = if has_glob_chars(&args.src) || !args.exclude.is_empty() {
        copy_glob(&mut *src, &mut *dst, &args, &opts, case_insensitive)?
    } else {
        copy_literal(&mut *src, &mut *dst, &args, &opts)?
    };

    dst.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;
    drop(dst);
    drop(src);
    commit.commit()?;

    summarize(&stats);
    Ok(())
}

/// On-device remote→remote copy: read SRC from one image on the daemon and
/// write it into another image on the **same** daemon, with no desktop data
/// round-trip. Phase 2: a single literal file (globs / `-r` deferred).
#[cfg(feature = "remote")]
fn remote_cp(
    src: &crate::remote::RemoteRef,
    dst: &crate::remote::RemoteRef,
    args: &CpArgs,
) -> Result<()> {
    if src.host != dst.host || src.port != dst.port {
        bail!(
            "both images must be on the same daemon for an on-device copy (src {}, dst {})",
            src.addr(),
            dst.addr()
        );
    }
    if has_glob_chars(&args.src) || !args.exclude.is_empty() {
        bail!("glob/exclude source isn't supported over rb:// yet (Phase 2 copies a single file)");
    }
    if args.recursive {
        bail!("recursive (-r) directory copy isn't supported over rb:// yet");
    }
    if args.skip_existing {
        bail!("--skip-existing isn't supported over rb:// yet (use --force to overwrite)");
    }

    // Destination: split into (parent, name); a trailing '/' (empty name) keeps
    // the source basename — cp-into-directory semantics.
    let (parent, mut name) = split_mac_path(&args.dst)?;
    if name.is_empty() {
        name = src_basename(&args.src).to_string();
    }

    let mut session = crate::remote::RemoteSession::connect(&dst.addr())?;
    let sid = session.open_session(&dst.path, args.dst_image.partition)?;
    session.stage_copy_local(
        sid,
        &src.path,
        args.src_image.partition,
        &args.src,
        &parent,
        &name,
        args.force,
    )?;
    let n = session.apply(sid)?;
    session.close_session(sid)?;
    out_stdout(format!(
        "Copied {name} on-device ({n} edit applied over rb://)"
    ));
    Ok(())
}

/// Last `/`-separated component of a source path.
#[cfg(feature = "remote")]
fn src_basename(src: &str) -> &str {
    src.trim_end_matches('/').rsplit('/').next().unwrap_or(src)
}

/// Literal (non-glob) source: a single file or a directory tree.
fn copy_literal(
    src: &mut dyn Filesystem,
    dst: &mut dyn EditableFilesystem,
    args: &CpArgs,
    opts: &CopyOptions,
) -> Result<CopyStats> {
    let entry = super::ls::resolve_path(src, &args.src)
        .with_context(|| format!("resolving source {:?}", args.src))?;
    if entry.is_directory() && !args.recursive {
        bail!(
            "{} is a directory; pass -r / --recursive to copy the tree",
            args.src
        );
    }

    // Free-space preflight before touching the destination.
    let need = copy::project(
        src,
        &entry,
        &copy::Capabilities::infer(dst.fs_type()),
        dst.allocation_unit(),
        args.recursive,
    )?;
    preflight_free_space(dst, need, args)?;

    let mut stats = CopyStats::default();
    if entry.path == "/" || entry.name == "/" {
        // Whole-volume source: there's no named directory to recreate, so
        // copy the root's children straight into the destination directory
        // (`cp -r src/. dst/` semantics — what consolidating a floppy wants).
        let dest_dir = resolve_or_create_dest_dir(dst, &args.dst, args.parents)?;
        for child in src
            .list_directory(&entry)
            .map_err(|e| anyhow!("listing source root: {e}"))?
        {
            copy::copy_into(
                src,
                &child,
                dst,
                &dest_dir,
                &child.name,
                opts,
                &mut stats,
                &logger,
            )?;
        }
        return Ok(stats);
    }

    let (parent, name) = decide_dest_target(dst, &args.dst, &entry, args.parents)?;
    copy::copy_into(src, &entry, dst, &parent, &name, opts, &mut stats, &logger)?;
    Ok(stats)
}

/// Glob source: walk the volume, lay each match under DST rooted at the
/// pattern's longest non-glob prefix.
fn copy_glob(
    src: &mut dyn Filesystem,
    dst: &mut dyn EditableFilesystem,
    args: &CpArgs,
    opts: &CopyOptions,
    case_insensitive: bool,
) -> Result<CopyStats> {
    let includes = compile_patterns(&args.src, case_insensitive)?;
    let mut excludes = Vec::new();
    for ex in &args.exclude {
        excludes.extend(compile_patterns(ex, case_insensitive)?);
    }
    let mut matches = collect_matches(src, &includes, &excludes)?;
    if matches.is_empty() {
        bail!("no matches for {:?}", args.src);
    }
    // Shallowest-first so parent directories are created before their files.
    matches.sort_by_key(|(_, _, full)| full.matches('/').count());

    // Free-space preflight. Project each match non-recursively (so `**`
    // file matches are counted once); recursion undercounts directory-only
    // matches, which the DiskFull backstop catches.
    let dst_caps = copy::Capabilities::infer(dst.fs_type());
    let unit = dst.allocation_unit();
    let mut need = 0u64;
    for (_, entry, _) in &matches {
        need = need.saturating_add(copy::project(src, entry, &dst_caps, unit, false)?);
    }
    preflight_free_space(dst, need, args)?;

    let glob_root = compute_glob_root(&args.src);
    let dst_root = args.dst.trim_end_matches('/').to_string();
    let mut stats = CopyStats::default();
    for (_, entry, full) in matches {
        let rel = strip_root_prefix(&full, &glob_root);
        let (rel_dir, base) = match rel.rsplit_once('/') {
            Some((d, b)) => (d.to_string(), b.to_string()),
            None => (
                String::new(),
                if rel.is_empty() {
                    entry.name.clone()
                } else {
                    rel.clone()
                },
            ),
        };
        let parent_path = join_fs_path(&dst_root, &rel_dir);
        let parent = ensure_dir_path(dst, &parent_path)?;
        copy::copy_into(src, &entry, dst, &parent, &base, opts, &mut stats, &logger)?;
    }
    Ok(stats)
}

/// Decide the destination parent directory + final name for a literal copy,
/// following `cp` conventions.
fn decide_dest_target(
    dst: &mut dyn EditableFilesystem,
    dst_path: &str,
    src_entry: &FileEntry,
    parents: bool,
) -> Result<(FileEntry, String)> {
    let trailing = dst_path.ends_with('/');
    match super::ls::resolve_path(dst, dst_path) {
        Ok(e) if e.is_directory() => Ok((e, src_entry.name.clone())),
        Ok(_) => {
            // Destination exists as a file.
            if src_entry.is_directory() {
                bail!(
                    "cannot overwrite file {dst_path} with directory {}",
                    src_entry.name
                );
            }
            let (parent_path, name) = split_mac_path(dst_path)?;
            Ok((resolve_existing_dir(dst, &parent_path)?, name))
        }
        Err(_) => {
            // Destination doesn't exist.
            if trailing {
                let dir = if parents {
                    ensure_dir_path(dst, dst_path)?
                } else {
                    bail!("destination directory {dst_path} does not exist (pass --parents)");
                };
                return Ok((dir, src_entry.name.clone()));
            }
            let (parent_path, name) = split_mac_path(dst_path)?;
            if name.is_empty() {
                bail!("destination path {dst_path:?} has no filename");
            }
            let parent = if parents {
                ensure_dir_path(dst, &parent_path)?
            } else {
                resolve_existing_dir(dst, &parent_path)?
            };
            Ok((parent, name))
        }
    }
}

/// Resolve the destination directory for a whole-volume / contents copy:
/// an existing directory, or (with `--parents`) one created on demand.
fn resolve_or_create_dest_dir(
    dst: &mut dyn EditableFilesystem,
    path: &str,
    parents: bool,
) -> Result<FileEntry> {
    match super::ls::resolve_path(dst, path) {
        Ok(e) if e.is_directory() => Ok(e),
        Ok(_) => bail!("destination {path} is a file, not a directory"),
        Err(_) => {
            if parents {
                ensure_dir_path(dst, path)
            } else {
                bail!("destination directory {path} does not exist (pass --parents)")
            }
        }
    }
}

/// Resolve an existing directory on the destination, erroring if it's a
/// file or missing.
fn resolve_existing_dir(dst: &mut dyn EditableFilesystem, path: &str) -> Result<FileEntry> {
    let e = super::ls::resolve_path(dst, path)
        .with_context(|| format!("resolving destination directory {path:?}"))?;
    if !e.is_directory() {
        bail!("destination parent {path} is not a directory");
    }
    Ok(e)
}

/// `mkdir -p` on the destination filesystem; returns the leaf directory.
fn ensure_dir_path(dst: &mut dyn EditableFilesystem, path: &str) -> Result<FileEntry> {
    let mut cur = dst
        .root()
        .map_err(|e| anyhow!("opening destination root: {e}"))?;
    for comp in path.split('/').filter(|c| !c.is_empty()) {
        let existing = dst
            .list_directory(&cur)
            .map_err(|e| anyhow!("list_directory {}: {e}", cur.path))?
            .into_iter()
            .find(|e| e.name == comp);
        cur = match existing {
            Some(e) if e.is_directory() => e,
            Some(_) => bail!("{comp} exists as a file, not a directory"),
            None => dst
                .create_directory(&cur, comp, &CreateDirectoryOptions::default())
                .map_err(|e| anyhow!("create_directory {comp}: {e}"))?,
        };
    }
    Ok(cur)
}

/// Join a `/`-separated relative path onto an in-image base path.
fn join_fs_path(base: &str, rel: &str) -> String {
    let base = base.trim_end_matches('/');
    let rel = rel.trim_matches('/');
    match (base.is_empty(), rel.is_empty()) {
        (true, true) => "/".to_string(),
        (true, false) => format!("/{rel}"),
        (false, true) => base.to_string(),
        (false, false) => format!("{base}/{rel}"),
    }
}

/// Compare the projected need against the destination's free space and
/// error out (before any write) with an actionable message if it won't fit.
fn preflight_free_space(dst: &mut dyn EditableFilesystem, need: u64, args: &CpArgs) -> Result<()> {
    let free = dst
        .free_space()
        .map_err(|e| anyhow!("querying destination free space: {e}"))?;
    if need <= free {
        return Ok(());
    }
    let fs_type = dst.fs_type().to_string();
    bail!(
        "copy needs ~{} but the {} volume on {} has only {} free.\n  \
         - free up space on the destination, or\n  \
         - grow it:  rb-cli resize {}{} --size <larger>\n    \
         (FAT/NTFS/exFAT/HFS+/ext/btrfs/SFS/PFS3/AFFS/EFS are resizable; you may first need \
         `rb-cli grow` + `rb-cli partmap resize` if the partition has no trailing slack)",
        crate::partition::format_size(need),
        fs_type,
        args.dst_image.path.display(),
        crate::partition::format_size(free),
        args.dst_image.path.display(),
        args.dst_image
            .partition
            .map(|n| format!("@{n}"))
            .unwrap_or_default(),
    )
}

/// Engine log sink: route warnings / per-entry notes to stderr.
fn logger(msg: &str) {
    log_stderr(msg);
}

fn same_file(a: &std::path::Path, b: &std::path::Path) -> bool {
    match (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
        (Ok(ca), Ok(cb)) => ca == cb,
        // If either can't be canonicalized (e.g. doesn't exist), fall back
        // to a literal comparison.
        _ => a == b,
    }
}

fn summarize(stats: &CopyStats) {
    let mut parts = Vec::new();
    if stats.files > 0 {
        parts.push(format!("{} file(s)", stats.files));
    }
    if stats.dirs > 0 {
        parts.push(format!("{} dir(s)", stats.dirs));
    }
    if stats.symlinks > 0 {
        parts.push(format!("{} symlink(s)", stats.symlinks));
    }
    if stats.renamed > 0 {
        parts.push(format!("{} renamed", stats.renamed));
    }
    if stats.skipped_existing > 0 {
        parts.push(format!("{} skipped (exists)", stats.skipped_existing));
    }
    if stats.skipped_names > 0 {
        parts.push(format!("{} skipped (name)", stats.skipped_names));
    }
    if stats.skipped_special > 0 {
        parts.push(format!("{} special skipped", stats.skipped_special));
    }
    if parts.is_empty() {
        parts.push("nothing".to_string());
    }
    out_stdout(format!(
        "Copied {} ({})",
        parts.join(", "),
        crate::partition::format_size(stats.bytes)
    ));

    // Fidelity-loss summary (only when something was dropped).
    let mut dropped = Vec::new();
    if stats.dropped_forks > 0 {
        dropped.push(format!("{} resource fork(s)", stats.dropped_forks));
    }
    if stats.dropped_type_creator > 0 {
        dropped.push(format!("{} type/creator", stats.dropped_type_creator));
    }
    if stats.dropped_perms > 0 {
        dropped.push(format!("{} Unix perms", stats.dropped_perms));
    }
    if stats.dropped_attrs > 0 {
        dropped.push(format!("{} DOS attrs", stats.dropped_attrs));
    }
    if stats.dropped_amiga > 0 {
        dropped.push(format!("{} Amiga metadata", stats.dropped_amiga));
    }
    if !dropped.is_empty() {
        out_stdout(format!(
            "Note: destination couldn't store: {} (see warnings above)",
            dropped.join(", ")
        ));
    }
}
