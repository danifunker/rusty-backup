//! `rb-cli rm IMG[@N] PATH` — delete a file or directory from a
//! filesystem. Generic across every EditableFilesystem.

use anyhow::{anyhow, bail, Result};
use clap::Args;

use crate::cli::glob::{collect_matches, compile_patterns};
use crate::cli::img_at::ImageRef;
use crate::cli::logging::{log_stderr, out_stdout};
use crate::cli::resolve::{resolve_partition_rw_forced, FsDispatchOverride};

#[derive(Debug, Args)]
pub struct RmArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Path or glob pattern inside the filesystem. Patterns containing
    /// `*`, `?`, `[`, or `{` walk the volume and delete every match.
    /// Pass `--literal` to delete a single path verbatim when its name
    /// contains those characters. A literal `/` in a name is written `\/`
    /// (or use a `:`-separated path on HFS / HFS+, which also forces literal).
    pub path: String,

    /// Recursively delete directories (matches will include directories
    /// without this flag, but they get rejected unless --recursive).
    #[arg(long, short = 'r')]
    pub recursive: bool,

    /// Exclude paths matching this glob from deletion. Repeatable.
    /// Exclude always wins over the positional pattern.
    #[arg(long = "exclude")]
    pub exclude: Vec<String>,

    /// Treat the path as an exact, literal path: never interpret `*`, `?`,
    /// `[`, `]`, `{`, `}` as glob metacharacters. Use for names that contain
    /// those characters. Conflicts with `--exclude`.
    #[arg(
        short = 'L',
        long = "literal",
        alias = "no-glob",
        conflicts_with = "exclude"
    )]
    pub literal: bool,

    /// Match case-insensitively regardless of the target's native rule.
    #[arg(long, conflicts_with = "case_sensitive")]
    pub ignore_case: bool,

    /// Match case-sensitively regardless of the target's native rule.
    #[arg(long, conflicts_with = "ignore_case")]
    pub case_sensitive: bool,

    #[command(flatten)]
    pub fs_override: FsDispatchOverride,
}

pub fn run(args: RmArgs) -> Result<()> {
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
    .map_err(|e| anyhow!("opening filesystem for write: {e}"))?;

    let case_insensitive = match (args.ignore_case, args.case_sensitive) {
        (true, _) => true,
        (_, true) => false,
        _ => true,
    };

    // `--literal` (or a colon-grammar path on HFS / HFS+) forces the exact
    // single-path delete even for names that contain glob metacharacters.
    let colon = super::ls::colon_mode(&*fs, &args.path);
    if !args.literal && !colon && (has_glob_chars(&args.path) || !args.exclude.is_empty()) {
        // Glob path — collect everything, sort deepest-first so we delete
        // children before parents, then apply.
        let includes = compile_patterns(&args.path, case_insensitive)?;
        let mut excludes = Vec::new();
        for ex in &args.exclude {
            excludes.extend(compile_patterns(ex, case_insensitive)?);
        }
        let mut matches = collect_matches(&mut *fs, &includes, &excludes)?;
        // Deepest-first: more slashes = deeper.
        matches.sort_by_key(|m| std::cmp::Reverse(m.2.matches('/').count()));
        if matches.is_empty() {
            bail!("no matches for {:?}", args.path);
        }
        let mut count_files = 0u64;
        let mut count_dirs = 0u64;
        for (parent, entry, full) in matches {
            if entry.is_directory() {
                if !args.recursive {
                    out_stdout(format!("  SKIP {full} (directory; pass --recursive)"));
                    continue;
                }
                fs.delete_recursive(&parent, &entry)
                    .map_err(|e| anyhow!("delete_recursive {full}: {e}"))?;
                count_dirs += 1;
            } else {
                fs.delete_entry(&parent, &entry)
                    .map_err(|e| anyhow!("delete_entry {full}: {e}"))?;
                count_files += 1;
            }
        }
        fs.sync_metadata()
            .map_err(|e| anyhow!("sync_metadata: {e}"))?;
        drop(fs);
        commit.commit()?;
        out_stdout(format!(
            "Removed {count_files} file(s) and {count_dirs} directory tree(s)"
        ));
        return Ok(());
    }

    // Literal-path single delete. Resolve parent + leaf with the shared escape
    // / colon grammar so a name containing a literal `/` is addressable.
    let (parent, name) = super::ls::resolve_parent(&mut *fs, &args.path)?;
    if name.is_empty() {
        bail!("path has no basename");
    }
    let children = fs
        .list_directory(&parent)
        .map_err(|e| anyhow!("list_directory: {e}"))?;
    let entry = children
        .into_iter()
        .find(|c| c.name == name)
        .ok_or_else(|| anyhow!("not found: {}", args.path))?;

    if entry.is_directory() {
        if !args.recursive {
            bail!(
                "{} is a directory; pass -r / --recursive to delete it and its contents",
                args.path
            );
        }
        fs.delete_recursive(&parent, &entry)
            .map_err(|e| anyhow!("delete_recursive: {e}"))?;
    } else {
        fs.delete_entry(&parent, &entry)
            .map_err(|e| anyhow!("delete_entry: {e}"))?;
    }
    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;
    drop(fs);
    commit.commit()?;
    Ok(())
}

fn has_glob_chars(s: &str) -> bool {
    s.chars().any(|c| matches!(c, '*' | '?' | '[' | '{'))
}
