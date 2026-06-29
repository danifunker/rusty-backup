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
use crate::cli::resolve::{resolve_partition_streaming_forced_inside, FsDispatchOverride};
use crate::fs::filesystem::Filesystem;

#[derive(Debug, Args)]
pub struct LsArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Path or glob pattern inside the filesystem (use `/` as the
    /// separator). A plain path lists that directory's contents;
    /// patterns containing `*`, `?`, `[`, or `{` walk the volume and
    /// emit one line per match. Pass `--literal` to address a path
    /// verbatim when its name contains those characters.
    ///
    /// A literal `/` inside a name (classic-Mac volumes allow it) is written
    /// `\/` (and a literal `\` as `\\`). On HFS / HFS+ you may instead use `:`
    /// as the separator — the native Mac convention — so `/` is just data:
    /// `:System Folder:Oxyd b/w`.
    #[arg(default_value = "/")]
    pub path: String,

    /// Exclude paths matching this glob. Repeatable.
    /// Exclude always wins over `--include` / a positional path.
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

    /// Treat case-insensitively, regardless of the target's native rule.
    #[arg(long, conflicts_with = "case_sensitive")]
    pub ignore_case: bool,

    /// Treat case-sensitively, regardless of the target's native rule.
    #[arg(long, conflicts_with = "ignore_case")]
    pub case_sensitive: bool,

    /// Password for encrypted containers (currently: WinImage IMZ, and
    /// password-protected `.zip` disks).
    #[arg(long)]
    pub password: Option<String>,

    /// For a `.zip` holding more than one disk image, the archive entry to
    /// open (e.g. `--inside backup.img`). Matched by exact name, then case-
    /// insensitively, then by basename. Ignored for non-zip sources.
    #[arg(long = "inside", value_name = "NAME")]
    pub inside: Option<String>,

    #[command(flatten)]
    pub fs_override: FsDispatchOverride,
}

pub fn run(args: LsArgs) -> Result<()> {
    // Remote source: `rb-cli ls rb://host:port/img@N /path`. The daemon parses
    // the filesystem and returns the listing; we never pull raw blocks.
    #[cfg(feature = "remote")]
    if let Some(rref) = crate::remote::RemoteRef::parse(&args.image.path.to_string_lossy()) {
        return remote_ls(&rref, args.image.partition, &args.path);
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

    // Volume-level advisory: surface the blessed System Folder (HFS / HFS+)
    // so users see what's currently bootable without a separate `bless show`.
    // Goes to stderr to keep stdout parse-clean. No-op on filesystems that
    // don't support blessing or volumes with none set.
    if let Some(info) = super::bless::blessed_info(&mut *fs) {
        log_stderr(format!(
            "Blessed System Folder: {}",
            super::bless::format_blessed(&info)
        ));
    }

    // Default case rule: insensitive on classic case-insensitive
    // filesystems, sensitive elsewhere. Phase B is conservative and
    // simply leaves the default insensitive on every filesystem because
    // it's the safer match for retro disks; CLI flags override.
    let case_insensitive = match (args.ignore_case, args.case_sensitive) {
        (true, _) => true,
        (_, true) => false,
        _ => true,
    };

    // `--literal` forces the exact-path branch even when the name contains
    // glob metacharacters. A colon-grammar path (HFS / HFS+ with `:`) is always
    // an exact path — it addresses a literal `/`-bearing name — so it never globs.
    let use_glob = !args.literal
        && !colon_mode(&*fs, &args.path)
        && (has_glob_chars(&args.path) || !args.exclude.is_empty());

    if use_glob {
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

/// Remote directory listing over an `rb://` reference. Lists either the
/// daemon's host filesystem (`rb://host/` or a host directory) or the contents
/// of an image on it (`rb://host/img@N`), auto-detected. Literal paths only —
/// globbing would need server-side volume walking, deferred.
#[cfg(feature = "remote")]
fn remote_ls(rref: &crate::remote::RemoteRef, partition: Option<u32>, path: &str) -> Result<()> {
    if has_glob_chars(path) {
        bail!("glob patterns aren't supported over rb:// yet (literal paths only)");
    }
    let mut session = crate::remote::RemoteSession::connect(&rref.addr())?;

    // With no `@N` the rb:// path could be a host directory to browse or a
    // (superfloppy) image file to open — ask the daemon which it is.
    if partition.is_none() {
        let (exists, is_dir) = session.host_stat(&rref.path)?;
        if !exists {
            bail!("no such path on the remote: {}", rref.path);
        }
        if is_dir {
            for entry in session.list_host_dir(&rref.path)? {
                print_wire_entry(&entry);
            }
            return Ok(());
        }
    }

    let opened = session.open_image(&rref.path, partition)?;
    log_stderr(opened.label);
    for entry in session.list_dir(opened.handle, path)? {
        print_wire_entry(&entry);
    }
    Ok(())
}

/// Mirror of [`print_entry`] for a [`crate::remote::protocol::WireEntry`].
#[cfg(feature = "remote")]
fn print_wire_entry(entry: &crate::remote::protocol::WireEntry) {
    let kind = if entry.is_dir() { "DIR " } else { "FILE" };
    let t = crate::fs::entry::display_file_type(entry.type_code.as_ref(), entry.prodos_file_type);
    let cr = entry
        .creator_code
        .as_ref()
        .map(crate::fs::hfs_common::decode_ostype);
    let t = t.as_deref().unwrap_or("    ");
    let cr = cr.as_deref().unwrap_or("    ");
    out_stdout(format!(
        "{kind}  {:>10}  {t} {cr}  {}",
        entry.size, entry.name
    ));
}

fn print_entry(entry: &crate::fs::entry::FileEntry, display_name: &str) {
    let kind = if entry.is_directory() { "DIR " } else { "FILE" };
    let t = entry.type_code_display();
    let cr = entry.creator_code_display();
    let t = t.as_deref().unwrap_or("    ");
    let cr = cr.as_deref().unwrap_or("    ");
    out_stdout(format!(
        "{kind}  {:>10}  {t} {cr}  {display_name}",
        entry.size
    ));
}

fn has_glob_chars(s: &str) -> bool {
    s.chars().any(|c| matches!(c, '*' | '?' | '[' | '{'))
}

/// True when `path` should be parsed with the colon grammar: only on a
/// filesystem that reserves `:` (HFS / HFS+) and only when the path actually
/// uses a `:` (so a plain `/`-path on HFS keeps working, with `\/` available
/// for a slash inside a name).
pub fn colon_mode(fs: &dyn Filesystem, path: &str) -> bool {
    fs.uses_colon_paths() && path.contains(':')
}

/// Walk `path` inside a generic filesystem, one component at a time.
///
/// The path is tokenised with [`crate::cli::parse::split_image_path`], so a
/// literal `/` inside a name can be addressed as `\/` (slash grammar) or by
/// using `:` separators on HFS / HFS+ (colon grammar). See that function.
pub fn resolve_path(fs: &mut dyn Filesystem, path: &str) -> Result<crate::fs::entry::FileEntry> {
    let colon = colon_mode(fs, path);
    let components = crate::cli::parse::split_image_path(path, colon);
    resolve_components(fs, &components)
}

/// Walk a path given as already-decoded components (no further splitting).
pub fn resolve_components(
    fs: &mut dyn Filesystem,
    components: &[String],
) -> Result<crate::fs::entry::FileEntry> {
    let mut current = fs.root().map_err(|e| anyhow!("root: {e}"))?;
    for component in components {
        let children = fs
            .list_directory(&current)
            .map_err(|e| anyhow!("list_directory: {e}"))?;
        let next = children
            .into_iter()
            .find(|c| &c.name == component)
            .ok_or_else(|| anyhow!("path component not found: {component}"))?;
        current = next;
    }
    Ok(current)
}

/// Resolve the parent directory of `path` and return `(parent_entry, basename)`,
/// decoding with the same escape / colon rules as [`resolve_path`]. Used by the
/// write verbs (`put`, `mkdir`, `rm`) so the leaf name they create / delete is
/// the decoded component (e.g. a literal `Oxyd b/w`).
pub fn resolve_parent(
    fs: &mut dyn Filesystem,
    path: &str,
) -> Result<(crate::fs::entry::FileEntry, String)> {
    let colon = colon_mode(fs, path);
    let (parent_components, name) = crate::cli::parse::split_image_parent(path, colon);
    let parent = resolve_components(fs, &parent_components)?;
    Ok((parent, name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::filesystem::{CreateDirectoryOptions, CreateFileOptions, EditableFilesystem};
    use crate::fs::hfs::{create_blank_hfs, HfsFilesystem};
    use std::io::Cursor;

    /// Build a blank HFS volume holding `/Oxyd 3.6/Oxyd b/w` — a folder with a
    /// child file whose name contains a literal `/` (legal on classic-Mac
    /// volumes). Returns the opened filesystem.
    fn hfs_with_slash_named_file() -> HfsFilesystem<Cursor<Vec<u8>>> {
        let img = create_blank_hfs(8 * 1024 * 1024, 4096, "Test").unwrap();
        let mut fs = HfsFilesystem::open(Cursor::new(img), 0).unwrap();
        let root = fs.root().unwrap();
        let folder = fs
            .create_directory(&root, "Oxyd 3.6", &CreateDirectoryOptions::default())
            .unwrap();
        let data = b"app";
        let mut reader: &[u8] = data;
        fs.create_file(
            &folder,
            "Oxyd b/w",
            &mut reader,
            data.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        fs.sync_metadata().unwrap();
        fs
    }

    #[test]
    fn resolve_slash_name_via_backslash_escape() {
        let mut fs = hfs_with_slash_named_file();
        let entry = resolve_path(&mut fs, r"/Oxyd 3.6/Oxyd b\/w").unwrap();
        assert_eq!(entry.name, "Oxyd b/w");
        assert!(!entry.is_directory());
    }

    #[test]
    fn resolve_slash_name_via_colon_grammar() {
        let mut fs = hfs_with_slash_named_file();
        let entry = resolve_path(&mut fs, ":Oxyd 3.6:Oxyd b/w").unwrap();
        assert_eq!(entry.name, "Oxyd b/w");
        assert!(!entry.is_directory());
    }

    #[test]
    fn unescaped_slash_name_mis_splits_and_is_not_found() {
        // Without an escape or colon grammar, the `/` is a separator: the path
        // resolves through a (nonexistent) folder `Oxyd b`, so it isn't found.
        let mut fs = hfs_with_slash_named_file();
        assert!(resolve_path(&mut fs, "/Oxyd 3.6/Oxyd b/w").is_err());
    }

    #[test]
    fn resolve_parent_returns_decoded_slash_basename() {
        let mut fs = hfs_with_slash_named_file();
        let (parent, name) = resolve_parent(&mut fs, r"/Oxyd 3.6/Oxyd b\/w").unwrap();
        assert!(parent.is_directory());
        assert_eq!(parent.name, "Oxyd 3.6");
        assert_eq!(name, "Oxyd b/w");
    }
}
