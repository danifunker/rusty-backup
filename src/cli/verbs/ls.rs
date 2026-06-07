//! `rb-cli ls IMG[@N] [PATH]` — list a directory inside a filesystem.
//!
//! Generic across every read-only filesystem the engine layer
//! supports — FAT12/16/32, NTFS, exFAT, HFS, HFS+, ext2/3/4, btrfs,
//! XFS, ProDOS, ISO9660, AFFS, PFS3, SFS, EFS, and any superfloppy
//! the magic-byte sniffer recognizes.

use anyhow::{anyhow, bail, Result};
use clap::Args;

use crate::cli::glob::{collect_matches, compile_patterns};
use crate::cli::img_at::ImageRef;
use crate::cli::logging::{log_stderr, out_stdout};
use crate::cli::resolve::{resolve_partition_streaming_with_password, FsDispatchOverride};
use crate::fs::filesystem::Filesystem;

#[derive(Debug, Args)]
pub struct LsArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Path or glob pattern inside the filesystem (use `/` as the
    /// separator). A plain path lists that directory's contents;
    /// patterns containing `*`, `?`, `[`, or `{` walk the volume and
    /// emit one line per match.
    #[arg(default_value = "/")]
    pub path: String,

    /// Exclude paths matching this glob. Repeatable.
    /// Exclude always wins over `--include` / a positional path.
    #[arg(long = "exclude")]
    pub exclude: Vec<String>,

    /// Treat case-insensitively, regardless of the target's native rule.
    #[arg(long, conflicts_with = "case_sensitive")]
    pub ignore_case: bool,

    /// Treat case-sensitively, regardless of the target's native rule.
    #[arg(long, conflicts_with = "ignore_case")]
    pub case_sensitive: bool,

    /// Password for encrypted containers (currently: WinImage IMZ).
    #[arg(long)]
    pub password: Option<String>,

    #[command(flatten)]
    pub fs_override: FsDispatchOverride,
}

pub fn run(args: LsArgs) -> Result<()> {
    let pw_bytes = args.password.as_deref().map(|s| s.as_bytes());
    let (reader, mut ctx) = resolve_partition_streaming_with_password(
        &args.image.path,
        args.image.partition,
        pw_bytes,
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

    // Default case rule: insensitive on classic case-insensitive
    // filesystems, sensitive elsewhere. Phase B is conservative and
    // simply leaves the default insensitive on every filesystem because
    // it's the safer match for retro disks; CLI flags override.
    let case_insensitive = match (args.ignore_case, args.case_sensitive) {
        (true, _) => true,
        (_, true) => false,
        _ => true,
    };

    let is_glob = has_glob_chars(&args.path);

    if is_glob || !args.exclude.is_empty() {
        // Glob path — walk the volume.
        let includes = compile_patterns(&args.path, case_insensitive)?;
        let mut excludes = Vec::new();
        for ex in &args.exclude {
            excludes.extend(compile_patterns(ex, case_insensitive)?);
        }
        let matches = collect_matches(&mut *fs, &includes, &excludes)?;
        for (_, entry, full) in matches {
            print_entry(&entry, &full);
        }
        return Ok(());
    }

    // Literal path: directory listing.
    let entry = resolve_path(&mut *fs, &args.path)?;
    if !entry.is_directory() {
        bail!("not a directory: {}", args.path);
    }
    let children = fs
        .list_directory(&entry)
        .map_err(|e| anyhow!("list_directory: {e}"))?;
    for c in children {
        print_entry(&c, &c.name);
    }
    Ok(())
}

fn print_entry(entry: &crate::fs::entry::FileEntry, display_name: &str) {
    let kind = if entry.is_directory() { "DIR " } else { "FILE" };
    let t = entry.type_code.as_deref().unwrap_or("    ");
    let cr = entry.creator_code.as_deref().unwrap_or("    ");
    out_stdout(format!(
        "{kind}  {:>10}  {t} {cr}  {display_name}",
        entry.size
    ));
}

fn has_glob_chars(s: &str) -> bool {
    s.chars().any(|c| matches!(c, '*' | '?' | '[' | '{'))
}

/// Walk `path` inside a generic filesystem, one component at a time.
pub fn resolve_path(fs: &mut dyn Filesystem, path: &str) -> Result<crate::fs::entry::FileEntry> {
    let mut current = fs.root().map_err(|e| anyhow!("root: {e}"))?;
    let trimmed = path.trim_start_matches('/').trim_end_matches('/');
    if trimmed.is_empty() {
        return Ok(current);
    }
    for component in trimmed.split('/') {
        if component.is_empty() {
            continue;
        }
        let children = fs
            .list_directory(&current)
            .map_err(|e| anyhow!("list_directory: {e}"))?;
        let next = children
            .into_iter()
            .find(|c| c.name == component)
            .ok_or_else(|| anyhow!("path component not found: {component}"))?;
        current = next;
    }
    Ok(current)
}
