//! `rb-cli get IMG[@N] SRC HOST` — extract a file or directory tree
//! from a filesystem to the host.
//!
//! Generic across every read-only filesystem the engine layer supports.
//!
//! Three shapes, dispatched on the source argument:
//!
//!  1. **Single literal file** — `get IMG /foo/a.txt out.txt` writes the
//!     file to the exact host path.
//!  2. **Literal directory** — `get IMG /foo out/ -r` walks `/foo`
//!     recursively, mirroring the tree under `out/foo/...`. Without
//!     `-r` the verb refuses.
//!  3. **Glob** — `get IMG '/foo/*.txt' out/` walks the volume,
//!     collects every match, and lays them out under `out/` rooted at
//!     the longest non-glob prefix (so `/foo/a.txt` becomes
//!     `out/a.txt`, and `/foo/sub/a.txt` under `/foo/**/*.txt` becomes
//!     `out/sub/a.txt`).
//!
//! Glob syntax follows `ls` / `rm` / `put`: `*`, `?`, `[abc]`, `**`,
//! `{a,b}`, plus `--exclude` (exclude-wins). Case sensitivity follows
//! the filesystem's native rule with `--ignore-case` /
//! `--case-sensitive` overrides.
//!
//! Conflicts on the host: default is error; `--force` overwrites,
//! `--skip-existing` skips silently. Symlinks land as plain text files
//! containing the target — lossy but cross-platform safe; the original
//! `get-binhex` / `get-applesingle` paths still handle Mac resource
//! forks.

use anyhow::{anyhow, bail, Context, Result};
use clap::Args;
use std::path::{Path, PathBuf};

use crate::cli::glob::{collect_matches, compile_patterns};
use crate::cli::img_at::ImageRef;
use crate::cli::logging::{log_stderr, out_stdout};
use crate::cli::resolve::{resolve_partition_streaming_forced, FsDispatchOverride};
use crate::fs::entry::FileEntry;
use crate::fs::filesystem::Filesystem;

#[derive(Debug, Args)]
pub struct GetArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Source path or glob inside the filesystem. Patterns containing
    /// `*`, `?`, `[`, or `{` walk the volume and extract every match.
    pub src: String,

    /// Destination path on the host. Single-match: the literal target
    /// file. Multi-match or directory source: a directory under which
    /// matched entries are laid out (created if it doesn't exist).
    pub dst: PathBuf,

    /// Recursively extract directories (literal dir source or glob
    /// match against a directory). Without this flag, matched
    /// directories are skipped with a warning.
    #[arg(long, short = 'r')]
    pub recursive: bool,

    /// Exclude paths matching this glob. Repeatable.
    /// Exclude always wins over `--include` / the positional source.
    #[arg(long = "exclude")]
    pub exclude: Vec<String>,

    /// Match case-insensitively regardless of the target's native rule.
    #[arg(long, conflicts_with = "case_sensitive")]
    pub ignore_case: bool,

    /// Match case-sensitively regardless of the target's native rule.
    #[arg(long, conflicts_with = "ignore_case")]
    pub case_sensitive: bool,

    /// Overwrite existing host files. Mutually exclusive with `--skip-existing`.
    #[arg(long, conflicts_with = "skip_existing")]
    pub force: bool,

    /// Skip silently when a host file already exists. Mutually
    /// exclusive with `--force`. Without either flag, an existing
    /// destination is a hard error.
    #[arg(long = "skip-existing", conflicts_with = "force")]
    pub skip_existing: bool,

    /// Password for encrypted containers (currently: WinImage IMZ).
    #[arg(long)]
    pub password: Option<String>,

    #[command(flatten)]
    pub fs_override: FsDispatchOverride,
}

/// How to handle a host destination that already exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictMode {
    Error,
    Force,
    SkipExisting,
}

impl ConflictMode {
    fn from_flags(force: bool, skip_existing: bool) -> Self {
        match (force, skip_existing) {
            (true, _) => Self::Force,
            (_, true) => Self::SkipExisting,
            _ => Self::Error,
        }
    }
}

pub fn run(args: GetArgs) -> Result<()> {
    let pw_bytes = args.password.as_deref().map(|s| s.as_bytes());
    let (reader, mut ctx) = resolve_partition_streaming_forced(
        &args.image.path,
        args.image.partition,
        pw_bytes,
        args.fs_override.fs_type.as_deref(),
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

    let case_insensitive = match (args.ignore_case, args.case_sensitive) {
        (true, _) => true,
        (_, true) => false,
        _ => true,
    };
    let conflict = ConflictMode::from_flags(args.force, args.skip_existing);

    // Decide the dispatch shape. Globs and exclude lists always go
    // through the glob walker; a literal source goes through resolve_path
    // and is treated as a single file or a recursive directory dump.
    if has_glob_chars(&args.src) || !args.exclude.is_empty() {
        return run_glob(
            &mut *fs,
            &args.src,
            &args.dst,
            &args.exclude,
            case_insensitive,
            args.recursive,
            conflict,
        );
    }

    // Literal path.
    let entry = super::ls::resolve_path(&mut *fs, &args.src)
        .with_context(|| format!("resolving {:?}", args.src))?;
    if entry.is_directory() {
        if !args.recursive {
            bail!(
                "{src} is a directory; pass -r / --recursive to extract the tree",
                src = args.src
            );
        }
        // Directory dump: lay the tree under DST/<source-basename>/...,
        // matching cp/rsync convention. Implemented as a recursive walk
        // over the live FS — no need to spin up the glob matcher for a
        // single-rooted recursion.
        let base_name = base_name_of(&args.src);
        let target_root = if base_name.is_empty() {
            args.dst.clone()
        } else {
            args.dst.join(&base_name)
        };
        ensure_dir(&target_root)?;
        let mut stats = ExtractStats::default();
        extract_directory_recursive(&mut *fs, &entry, &target_root, conflict, &mut stats)?;
        summarize(&stats);
        return Ok(());
    }
    // Single file.
    let host_target = decide_single_host_target(&args.dst, &entry);
    extract_file_to_host(&mut *fs, &entry, &host_target, conflict)
        .map(|_written| ())
        .with_context(|| format!("extracting {} to {}", args.src, host_target.display()))
}

fn run_glob(
    fs: &mut dyn Filesystem,
    src: &str,
    dst: &Path,
    exclude: &[String],
    case_insensitive: bool,
    recursive: bool,
    conflict: ConflictMode,
) -> Result<()> {
    let includes = compile_patterns(src, case_insensitive)?;
    let mut excludes = Vec::new();
    for ex in exclude {
        excludes.extend(compile_patterns(ex, case_insensitive)?);
    }
    let mut matches = collect_matches(fs, &includes, &excludes)?;
    if matches.is_empty() {
        bail!("no matches for {:?}", src);
    }

    // Sort shallowest-first so parent dirs land before their files.
    // Counting `/` is a cheap proxy for depth and matches the order
    // we'd need for `mkdir -p` to win.
    matches.sort_by_key(|(_, _, full)| full.matches('/').count());

    let glob_root = compute_glob_root(src);
    ensure_dir(dst)?;

    let mut stats = ExtractStats::default();
    for (_parent, entry, full) in matches {
        let rel = strip_root_prefix(&full, &glob_root);
        let host_target = if rel.is_empty() {
            // The glob matched the root itself (rare); fall back to the
            // entry's name under DST.
            dst.join(&entry.name)
        } else {
            join_relative(dst, &rel)
        };

        match entry.entry_type {
            crate::fs::entry::EntryType::Directory => {
                if !recursive {
                    log_stderr(format!("skip dir (no -r): {full}"));
                    stats.skipped_dirs += 1;
                    continue;
                }
                ensure_dir(&host_target)?;
                extract_directory_recursive(fs, &entry, &host_target, conflict, &mut stats)?;
            }
            crate::fs::entry::EntryType::File
            | crate::fs::entry::EntryType::Symlink
            | crate::fs::entry::EntryType::Special => {
                if let Some(parent) = host_target.parent() {
                    ensure_dir(parent)?;
                }
                let _ = extract_entry_to_host(fs, &entry, &host_target, conflict, &mut stats)
                    .with_context(|| format!("extracting {full} to {}", host_target.display()))?;
            }
        }
    }
    summarize(&stats);
    Ok(())
}

/// Walk a directory and copy its contents into `host_target`. Used by
/// both the literal-directory path and matched-directory entries in
/// the glob path. `host_target` should already exist.
fn extract_directory_recursive(
    fs: &mut dyn Filesystem,
    dir: &FileEntry,
    host_target: &Path,
    conflict: ConflictMode,
    stats: &mut ExtractStats,
) -> Result<()> {
    let children = fs
        .list_directory(dir)
        .map_err(|e| anyhow!("list_directory {}: {e}", dir.path))?;
    for child in children {
        let host_child = host_target.join(&child.name);
        match child.entry_type {
            crate::fs::entry::EntryType::Directory => {
                ensure_dir(&host_child)?;
                extract_directory_recursive(fs, &child, &host_child, conflict, stats)?;
            }
            crate::fs::entry::EntryType::File
            | crate::fs::entry::EntryType::Symlink
            | crate::fs::entry::EntryType::Special => {
                extract_entry_to_host(fs, &child, &host_child, conflict, stats).with_context(
                    || format!("extracting {} to {}", child.path, host_child.display()),
                )?;
            }
        }
    }
    Ok(())
}

/// Dispatch a single non-directory entry (file / symlink / special) to
/// the host. Returns `Ok(true)` if a file was written, `Ok(false)` if
/// skipped.
fn extract_entry_to_host(
    fs: &mut dyn Filesystem,
    entry: &FileEntry,
    host_target: &Path,
    conflict: ConflictMode,
    stats: &mut ExtractStats,
) -> Result<bool> {
    match entry.entry_type {
        crate::fs::entry::EntryType::File => {
            let wrote = extract_file_to_host(fs, entry, host_target, conflict)?;
            if wrote {
                stats.files += 1;
            } else {
                stats.skipped_existing += 1;
            }
            Ok(wrote)
        }
        crate::fs::entry::EntryType::Symlink => {
            // Cross-platform safe fallback: write the target as a plain
            // text file. Lossy but doesn't fail on Windows without
            // developer mode. Add `--symlinks=native` in a follow-up if
            // demand surfaces.
            let target = entry.symlink_target.clone().unwrap_or_default();
            let body = if target.is_empty() {
                String::new()
            } else {
                format!("{target}\n")
            };
            let wrote = write_bytes_to_host(host_target, body.as_bytes(), conflict)?;
            if wrote {
                stats.symlinks += 1;
                log_stderr(format!(
                    "  symlink as text: {} -> {target} (use platform tools to recreate the link)",
                    host_target.display()
                ));
            } else {
                stats.skipped_existing += 1;
            }
            Ok(wrote)
        }
        crate::fs::entry::EntryType::Special => {
            stats.skipped_special += 1;
            log_stderr(format!(
                "  skip special file: {} ({})",
                entry.path,
                entry.special_type.as_deref().unwrap_or("unknown kind")
            ));
            Ok(false)
        }
        crate::fs::entry::EntryType::Directory => {
            // Caller is responsible for directory dispatch.
            unreachable!("extract_entry_to_host called on a directory");
        }
    }
}

/// Extract a single regular file. Returns `Ok(true)` if the file was
/// written, `Ok(false)` if it was skipped due to a conflict.
fn extract_file_to_host(
    fs: &mut dyn Filesystem,
    entry: &FileEntry,
    host_target: &Path,
    conflict: ConflictMode,
) -> Result<bool> {
    if host_target.exists() {
        match conflict {
            ConflictMode::Force => {}
            ConflictMode::SkipExisting => {
                log_stderr(format!("  skip (exists): {}", host_target.display()));
                return Ok(false);
            }
            ConflictMode::Error => bail!(
                "destination exists: {} (pass --force or --skip-existing)",
                host_target.display()
            ),
        }
    }
    if let Some(parent) = host_target.parent() {
        ensure_dir(parent)?;
    }
    let mut out = std::fs::File::create(host_target)
        .with_context(|| format!("creating {}", host_target.display()))?;
    fs.write_file_to(entry, &mut out)
        .map_err(|e| anyhow!("write_file_to({}): {e}", entry.path))?;
    Ok(true)
}

/// Write raw bytes to the host with conflict-handling. Used for
/// symlink-as-text payloads.
fn write_bytes_to_host(host_target: &Path, bytes: &[u8], conflict: ConflictMode) -> Result<bool> {
    if host_target.exists() {
        match conflict {
            ConflictMode::Force => {}
            ConflictMode::SkipExisting => return Ok(false),
            ConflictMode::Error => bail!(
                "destination exists: {} (pass --force or --skip-existing)",
                host_target.display()
            ),
        }
    }
    if let Some(parent) = host_target.parent() {
        ensure_dir(parent)?;
    }
    std::fs::write(host_target, bytes)
        .with_context(|| format!("writing {}", host_target.display()))?;
    Ok(true)
}

/// Decide where a single-file extract should land on the host.
///
///  * If `dst` is an existing directory **or** the user wrote it with
///    a trailing separator, the target is `dst/<entry.name>`.
///  * Otherwise the target is `dst` verbatim.
fn decide_single_host_target(dst: &Path, entry: &FileEntry) -> PathBuf {
    let dst_str = dst.to_string_lossy();
    let trailing_sep = dst_str.ends_with('/') || dst_str.ends_with('\\');
    if dst.is_dir() || trailing_sep {
        dst.join(&entry.name)
    } else {
        dst.to_path_buf()
    }
}

/// `mkdir -p` with a clean error message. No-op on a path that already
/// exists as a directory; errors if the path exists as a non-directory.
fn ensure_dir(p: &Path) -> Result<()> {
    if p.as_os_str().is_empty() {
        return Ok(());
    }
    if p.exists() {
        if !p.is_dir() {
            bail!(
                "destination already exists and is not a directory: {}",
                p.display()
            );
        }
        return Ok(());
    }
    std::fs::create_dir_all(p).with_context(|| format!("creating directory {}", p.display()))
}

/// Compute the longest leading prefix of a glob pattern that contains
/// no glob characters. Used to decide which segments of a match path
/// to strip when laying entries under DST.
///
/// Examples:
///  * `/foo/*.txt` → `/foo`
///  * `/foo/**/*.txt` → `/foo` (since `**` is a glob char)
///  * `*.txt` → `/`
///  * `/foo/bar` → `/foo/bar` (all literal; rare in this codepath)
fn compute_glob_root(src: &str) -> String {
    let trimmed = src.trim_start_matches('/').trim_end_matches('/');
    if trimmed.is_empty() {
        return "/".to_string();
    }
    let mut root_parts: Vec<&str> = Vec::new();
    for part in trimmed.split('/') {
        if part.is_empty() {
            continue;
        }
        if has_glob_chars(part) {
            break;
        }
        root_parts.push(part);
    }
    if root_parts.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", root_parts.join("/"))
    }
}

/// Strip `glob_root` from the front of `full_path` and return the
/// remainder (without a leading `/`). Returns an empty string when
/// `full_path == glob_root` exactly.
fn strip_root_prefix(full_path: &str, glob_root: &str) -> String {
    if glob_root == "/" {
        return full_path.trim_start_matches('/').to_string();
    }
    if let Some(rest) = full_path.strip_prefix(glob_root) {
        return rest.trim_start_matches('/').to_string();
    }
    // Fallback: shouldn't happen — if glob_root isn't a prefix, the
    // glob walker wouldn't have matched. Use the basename as a safe
    // fallback rather than panicking.
    full_path.rsplit('/').next().unwrap_or("").to_string()
}

/// Join a slash-separated relative path onto a host base. Splits on
/// `/` rather than passing the whole string in one shot so Windows
/// host paths don't get confused.
fn join_relative(base: &Path, rel: &str) -> PathBuf {
    let mut out = base.to_path_buf();
    for component in rel.split('/').filter(|c| !c.is_empty()) {
        out.push(component);
    }
    out
}

/// Basename of a filesystem-internal path, e.g. `/foo/bar` → `bar`.
fn base_name_of(path: &str) -> String {
    path.trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("")
        .to_string()
}

fn has_glob_chars(s: &str) -> bool {
    s.chars().any(|c| matches!(c, '*' | '?' | '[' | '{'))
}

#[derive(Debug, Default)]
struct ExtractStats {
    files: u64,
    symlinks: u64,
    skipped_dirs: u64,
    skipped_existing: u64,
    skipped_special: u64,
}

fn summarize(stats: &ExtractStats) {
    if stats.files == 0
        && stats.symlinks == 0
        && stats.skipped_dirs == 0
        && stats.skipped_existing == 0
        && stats.skipped_special == 0
    {
        return;
    }
    let mut parts = Vec::new();
    if stats.files > 0 {
        parts.push(format!("{} file(s)", stats.files));
    }
    if stats.symlinks > 0 {
        parts.push(format!("{} symlink(s)", stats.symlinks));
    }
    if stats.skipped_dirs > 0 {
        parts.push(format!("{} dir(s) skipped (no -r)", stats.skipped_dirs));
    }
    if stats.skipped_existing > 0 {
        parts.push(format!("{} skipped existing", stats.skipped_existing));
    }
    if stats.skipped_special > 0 {
        parts.push(format!("{} special skipped", stats.skipped_special));
    }
    out_stdout(format!("Extracted: {}", parts.join(", ")));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_root_strips_globbed_tail() {
        assert_eq!(compute_glob_root("/foo/*.txt"), "/foo");
        assert_eq!(compute_glob_root("/foo/bar/*.txt"), "/foo/bar");
        assert_eq!(compute_glob_root("/foo/**/*.txt"), "/foo");
        assert_eq!(compute_glob_root("/foo/*/baz/*.txt"), "/foo");
    }

    #[test]
    fn glob_root_unanchored_pattern_uses_root() {
        assert_eq!(compute_glob_root("*.txt"), "/");
        assert_eq!(compute_glob_root("**/*.txt"), "/");
    }

    #[test]
    fn glob_root_literal_pattern_is_whole_path() {
        assert_eq!(compute_glob_root("/foo/bar"), "/foo/bar");
        assert_eq!(compute_glob_root("/foo/bar/"), "/foo/bar");
    }

    #[test]
    fn glob_root_brace_in_segment_stops_walk() {
        // `{a,b}` is a glob char (in `has_glob_chars`); the prefix
        // truncates at the brace segment.
        assert_eq!(compute_glob_root("/foo/{a,b}/c"), "/foo");
    }

    #[test]
    fn strip_root_returns_relative_remainder() {
        assert_eq!(strip_root_prefix("/foo/a.txt", "/foo"), "a.txt");
        assert_eq!(strip_root_prefix("/foo/sub/a.txt", "/foo"), "sub/a.txt");
        assert_eq!(strip_root_prefix("/a.txt", "/"), "a.txt");
    }

    #[test]
    fn strip_root_handles_exact_match() {
        assert_eq!(strip_root_prefix("/foo", "/foo"), "");
    }

    #[test]
    fn join_relative_splits_on_slashes() {
        let base = PathBuf::from("/tmp/out");
        assert_eq!(
            join_relative(&base, "a.txt"),
            PathBuf::from("/tmp/out/a.txt")
        );
        assert_eq!(
            join_relative(&base, "sub/a.txt"),
            PathBuf::from("/tmp/out/sub/a.txt")
        );
        // Empty components are dropped.
        assert_eq!(
            join_relative(&base, "/foo//bar"),
            PathBuf::from("/tmp/out/foo/bar")
        );
    }

    #[test]
    fn base_name_of_strips_trailing_slash() {
        assert_eq!(base_name_of("/foo/bar"), "bar");
        assert_eq!(base_name_of("/foo/bar/"), "bar");
        assert_eq!(base_name_of("/foo"), "foo");
        assert_eq!(base_name_of("/"), "");
    }

    #[test]
    fn conflict_mode_precedence() {
        assert_eq!(ConflictMode::from_flags(false, false), ConflictMode::Error);
        assert_eq!(ConflictMode::from_flags(true, false), ConflictMode::Force);
        assert_eq!(
            ConflictMode::from_flags(false, true),
            ConflictMode::SkipExisting
        );
        // `clap` rejects `--force --skip-existing` at parse time; if both
        // somehow set, --force wins (matches the cli behaviour shown by
        // run flow before clap conflicts kick in).
        assert_eq!(ConflictMode::from_flags(true, true), ConflictMode::Force);
    }

    #[test]
    fn has_glob_chars_recognises_each_metacharacter() {
        assert!(has_glob_chars("*"));
        assert!(has_glob_chars("?"));
        assert!(has_glob_chars("[abc]"));
        assert!(has_glob_chars("{a,b}"));
        assert!(!has_glob_chars("/literal/path.txt"));
    }

    #[test]
    fn decide_single_target_with_trailing_slash_descends() {
        let entry = FileEntry::new_file("a.txt".into(), "/a.txt".into(), 0, 0);
        // Trailing slash forces directory semantics.
        let target = decide_single_host_target(&PathBuf::from("out/"), &entry);
        assert_eq!(target, PathBuf::from("out/").join("a.txt"));
    }

    #[test]
    fn decide_single_target_literal_keeps_path() {
        let entry = FileEntry::new_file("a.txt".into(), "/a.txt".into(), 0, 0);
        let target = decide_single_host_target(&PathBuf::from("out.txt"), &entry);
        assert_eq!(target, PathBuf::from("out.txt"));
    }
}
