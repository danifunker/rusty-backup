//! Import a host directory tree INTO a disk image's filesystem — the
//! `mkisofs -graft-points`-shaped counterpart to [`crate::fs::tar_import`],
//! without needing to stage a tarball first.
//!
//! Only the source differs: this walks the host with `std::fs` and hands each
//! entry to the shared [`crate::fs::import_sink`], which owns path-traversal
//! guarding, mkdir -p, the conflict policy, and attribute inheritance. So a
//! directory import and a tar import behave identically once an entry has been
//! classified — including which failures are "skip and count" rather than
//! "abort" (a tree with symlinks imports fine onto FAT, dropping them).
//!
//! Entries are visited in sorted order at every level, so an import is
//! reproducible: the same tree produces the same on-disk layout rather than
//! whatever order the host filesystem happened to hand back.
//!
//! The caller owns the sync/commit lifecycle: like every other
//! [`EditableFilesystem`] mutation, callers MUST call `sync_metadata()` (and,
//! for a container, `commit`) after import returns.

use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};

use crate::fs::attrs::AttrOverrides;
use crate::fs::entry::FileEntry;
use crate::fs::filesystem::EditableFilesystem;
use crate::fs::import_sink::{
    is_appledouble, ImportItem, ImportOptions, ImportPreflight, ImportStats, Importer,
};

pub use crate::fs::import_sink::{
    ImportConflict, ImportPreflight as DirImportPreflight, ImportStats as DirImportStats,
};

/// Knobs for [`import_dir`].
#[derive(Default)]
pub struct DirImportOptions {
    pub shared: ImportOptions,
    /// Unpack tar archives found in the tree (`.tar`, `.tar.gz`, `.tgz`,
    /// `.tar.zst`, and anything else carrying the `ustar` magic — IRIX
    /// `.tardist` files included) into a directory named after the archive,
    /// instead of copying the archive in as an opaque file.
    ///
    /// **Off by default.** Which one you want is a genuine judgement call: a
    /// software-distribution disc usually wants the archives left intact so
    /// the target system's own installer can consume them, while a "just give
    /// me the files" disc wants them unpacked. Detection is by content sniff,
    /// not extension, so an oddly-named archive is still found and a `.gz`
    /// disk image is not mistaken for one.
    pub expand_archives: bool,

    /// With [`Self::expand_archives`]: unpack each archive's contents into the
    /// directory that held the archive, rather than into a per-archive
    /// subdirectory. Every archive alongside each other then shares one root.
    ///
    /// This is what an IRIX `inst` distribution wants: `.tardist` files carry
    /// flat product images (`tgc_bash`, `tgc_bash.idb`, `tgc_bash.sw`, ...),
    /// and `inst` is pointed at ONE directory holding all of them. A
    /// subdirectory per archive would mean re-pointing `inst` 55 times.
    ///
    /// Overlapping entries are expected here rather than exceptional —
    /// SGI freeware tardists all ship the same shared `fw_common*` product —
    /// so callers generally want a non-fatal conflict policy alongside this.
    pub flatten_archives: bool,
}

/// Directory name for an expanded archive: the file name with its archive
/// extension(s) stripped. `foo-1.2.tar.gz` -> `foo-1.2`, `bar.tardist` ->
/// `bar`. A name that strips to nothing keeps the original.
fn expanded_dir_name(file_name: &str) -> String {
    let lower = file_name.to_ascii_lowercase();
    for suffix in [
        ".tar.gz",
        ".tar.zst",
        ".tar.zstd",
        ".tar.bz2",
        ".tar.xz",
        ".tgz",
        ".tzst",
        ".tbz2",
        ".txz",
        ".tardist",
        ".tar",
    ] {
        if lower.ends_with(suffix) {
            let stem = &file_name[..file_name.len() - suffix.len()];
            if !stem.is_empty() {
                return stem.to_string();
            }
        }
    }
    file_name.to_string()
}

/// One host entry, flattened out of the recursive walk.
struct HostEntry {
    /// Path components relative to the import root.
    comps: Vec<String>,
    host: PathBuf,
    kind: HostKind,
}

enum HostKind {
    Dir,
    File { size: u64 },
    Symlink { target: String },
    Other,
}

/// Walk `root` depth-first, yielding directories before their contents so the
/// sink can create parents first. Symlinks are reported as symlinks and never
/// followed — following them risks importing the same subtree repeatedly, or
/// escaping `root` entirely.
fn walk(root: &Path) -> Result<Vec<HostEntry>> {
    let mut out = Vec::new();
    walk_into(root, &mut Vec::new(), &mut out)?;
    Ok(out)
}

fn walk_into(dir: &Path, prefix: &mut Vec<String>, out: &mut Vec<HostEntry>) -> Result<()> {
    let mut children: Vec<_> = std::fs::read_dir(dir)
        .with_context(|| format!("reading {}", dir.display()))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("reading {}", dir.display()))?;
    // Deterministic order, so the same tree always lays out the same way.
    children.sort_by_key(|d| d.file_name());

    for dent in children {
        let name = dent.file_name().to_string_lossy().into_owned();
        let host = dent.path();
        // `symlink_metadata` so a symlink is classified as one rather than as
        // whatever it points at.
        let meta = match host.symlink_metadata() {
            Ok(m) => m,
            // A file that vanished mid-walk is not worth aborting a
            // multi-gigabyte import over.
            Err(_) => continue,
        };
        let ft = meta.file_type();
        prefix.push(name);
        if ft.is_symlink() {
            let target = std::fs::read_link(&host)
                .map(|t| t.to_string_lossy().into_owned())
                .unwrap_or_default();
            out.push(HostEntry {
                comps: prefix.clone(),
                host: host.clone(),
                kind: HostKind::Symlink { target },
            });
        } else if ft.is_dir() {
            out.push(HostEntry {
                comps: prefix.clone(),
                host: host.clone(),
                kind: HostKind::Dir,
            });
            walk_into(&host, prefix, out)?;
        } else if ft.is_file() {
            out.push(HostEntry {
                comps: prefix.clone(),
                host: host.clone(),
                kind: HostKind::File { size: meta.len() },
            });
        } else {
            // Devices, fifos, sockets.
            out.push(HostEntry {
                comps: prefix.clone(),
                host: host.clone(),
                kind: HostKind::Other,
            });
        }
        prefix.pop();
    }
    Ok(())
}

/// The mode / ownership a host entry asks for, or nothing when the caller
/// turned that off (`--no-permissions`), in which case the shared resolver
/// falls back to the replaced entry then the parent directory.
///
/// Unix-only: on Windows there is no mode to read, so every entry takes the
/// resolver's inherit-from-parent default.
#[cfg(unix)]
fn host_overrides(meta: &std::fs::Metadata, apply: bool) -> AttrOverrides {
    use std::os::unix::fs::MetadataExt;
    if !apply {
        return AttrOverrides::default();
    }
    AttrOverrides {
        mode: Some(meta.mode() & 0o7777),
        uid: Some(meta.uid()),
        gid: Some(meta.gid()),
    }
}

#[cfg(not(unix))]
fn host_overrides(_meta: &std::fs::Metadata, _apply: bool) -> AttrOverrides {
    AttrOverrides::default()
}

fn overrides_for(host: &Path, apply: bool) -> AttrOverrides {
    match host.symlink_metadata() {
        Ok(m) => host_overrides(&m, apply),
        Err(_) => AttrOverrides::default(),
    }
}

/// Import everything under `root` into `dest`.
///
/// Brackets the work in [`EditableFilesystem::begin_bulk`] /
/// [`end_bulk`](EditableFilesystem::end_bulk). The importer aborts on the first
/// hard error (every create is `?`-propagated) and callers discard the volume
/// on `Err`, so per-operation rollback is redundant here; bulk mode lets HFS
/// skip cloning its whole catalog on every entry. `end_bulk` runs even on
/// error so the filesystem is never left stuck in bulk mode.
pub fn import_dir(
    efs: &mut dyn EditableFilesystem,
    dest: &FileEntry,
    root: &Path,
    opts: &DirImportOptions,
    progress: &dyn Fn(&ImportStats),
) -> Result<ImportStats> {
    if !root.is_dir() {
        return Err(anyhow!("{} is not a directory", root.display()));
    }
    efs.begin_bulk();
    let result = import_dir_inner(efs, dest, root, opts, progress);
    efs.end_bulk();
    result
}

fn import_dir_inner(
    efs: &mut dyn EditableFilesystem,
    dest: &FileEntry,
    root: &Path,
    opts: &DirImportOptions,
    progress: &dyn Fn(&ImportStats),
) -> Result<ImportStats> {
    let entries = walk(root)?;
    let mut sink = Importer::new(dest);

    for e in entries {
        let display = e.host.display().to_string();
        let overrides = overrides_for(&e.host, opts.shared.apply_permissions);
        match e.kind {
            HostKind::Dir => {
                sink.push(
                    efs,
                    &e.comps,
                    ImportItem::Dir,
                    &overrides,
                    &opts.shared,
                    &display,
                )?;
            }
            HostKind::Symlink { target } => {
                sink.push(
                    efs,
                    &e.comps,
                    ImportItem::Symlink { target },
                    &overrides,
                    &opts.shared,
                    &display,
                )?;
            }
            HostKind::File { size } => {
                if opts.expand_archives
                    && crate::fs::tar_import::looks_like_tar_archive(&e.host)
                    && expand_archive(efs, &mut sink, &e, opts, progress)?
                {
                    progress(&sink.stats);
                    continue;
                }
                let mut f = File::open(&e.host).with_context(|| format!("opening {display}"))?;
                sink.push(
                    efs,
                    &e.comps,
                    ImportItem::File { size, data: &mut f },
                    &overrides,
                    &opts.shared,
                    &display,
                )?;
            }
            HostKind::Other => {
                sink.push(
                    efs,
                    &e.comps,
                    ImportItem::Unsupported,
                    &overrides,
                    &opts.shared,
                    &display,
                )?;
            }
        }
        progress(&sink.stats);
    }
    Ok(sink.stats)
}

/// Unpack one archive: into a directory named after it, or — with
/// `flatten_archives` — straight into the directory that held it, so sibling
/// archives share one root. Returns `false` when the archive should be copied
/// in verbatim after all (its directory name is unusable on this filesystem),
/// so the caller falls back to a plain copy rather than silently dropping the
/// file.
///
/// Bulk mode stays with the enclosing [`import_dir`] — see
/// [`crate::fs::tar_import::import_tar_into`] for why this can't just call
/// `import_tar`.
fn expand_archive(
    efs: &mut dyn EditableFilesystem,
    sink: &mut Importer,
    e: &HostEntry,
    opts: &DirImportOptions,
    progress: &dyn Fn(&ImportStats),
) -> Result<bool> {
    let file_name = match e.comps.last() {
        Some(n) => n.clone(),
        None => return Ok(false),
    };
    // Flattened: drop the archive's own name, landing its contents in the
    // directory that held it (the destination root for a top-level archive).
    // Otherwise: replace the archive's name with the stripped-extension form
    // and unpack under that.
    let mut comps = e.comps.clone();
    let last = comps.len() - 1;
    if opts.flatten_archives {
        comps.truncate(last);
    } else {
        comps[last] = expanded_dir_name(&file_name);
    }

    let dir = match sink.ensure_dir_at(efs, &comps)? {
        Some(d) => d,
        None => return Ok(false),
    };
    // The nested import's tally is reported against the parent's totals, so
    // the caller's progress callback keeps counting up rather than restarting.
    let base = sink.stats.clone();
    let nested = crate::fs::tar_import::import_tar_from_path_into(
        efs,
        &dir,
        &e.host,
        &opts.shared,
        &|s: &ImportStats| {
            let mut merged = base.clone();
            merged.merge(s);
            progress(&merged);
        },
    )
    // `{e:#}` not `{e}`: the CLI flattens this error into a message with
    // `{e}` further up, which keeps only the outermost layer. Without the
    // alternate form the real cause — which file, which filesystem limit —
    // is silently dropped and the user gets a bare archive name.
    .map_err(|err| anyhow!("expanding {}: {err:#}", e.host.display()))?;
    sink.stats.merge(&nested);
    sink.stats.archives_expanded += 1;
    Ok(true)
}

/// Read-only scan of a host tree against a target filesystem: what would be
/// skipped or dropped, and how many content bytes the import would write.
/// Mirrors [`import_dir`]'s classification but performs no mutations.
///
/// `total_bytes` is what sizing a fresh volume to fit the tree keys off — see
/// [`projected_volume_bytes`].
pub fn preflight_dir(
    efs: &dyn EditableFilesystem,
    root: &Path,
    opts: &DirImportOptions,
) -> Result<ImportPreflight> {
    let supports_symlinks = efs.supports_symlinks();
    let mut pf = ImportPreflight::default();
    for e in walk(root)? {
        let name_invalid = e.comps.iter().any(|c| efs.validate_name(c).is_err());
        let appledouble = opts.shared.skip_appledouble
            && e.comps.last().map(|c| is_appledouble(c)).unwrap_or(false);
        if appledouble {
            pf.appledouble += 1;
            continue;
        }
        match e.kind {
            HostKind::Dir => {
                pf.dirs += 1;
                if name_invalid {
                    pf.invalid_names += 1;
                }
            }
            _ if name_invalid => pf.invalid_names += 1,
            HostKind::Symlink { .. } => {
                pf.symlinks += 1;
                if !supports_symlinks {
                    pf.symlinks_dropped += 1;
                }
            }
            HostKind::File { size } => {
                if opts.expand_archives && crate::fs::tar_import::looks_like_tar_archive(&e.host) {
                    // Count what the archive becomes, not the archive.
                    let (f, d, b) = crate::fs::tar_import::measure_tar_expanded(&e.host)?;
                    pf.files += f;
                    // Plus the directory it unpacks into, unless flattened —
                    // then it shares the one that already exists.
                    pf.dirs += d + u64::from(!opts.flatten_archives);
                    pf.total_bytes += b;
                } else {
                    pf.files += 1;
                    pf.total_bytes += size;
                }
            }
            HostKind::Other => pf.other_unsupported += 1,
        }
    }
    Ok(pf)
}

/// Scan a host tree with no target filesystem in hand — used to size a volume
/// *before* formatting it, where there is nothing to validate names against
/// yet. Returns (file count, directory count, total content bytes).
///
/// With `expand_archives` on, archives are measured by what they unpack to.
/// That distinction is the whole point for sizing: a tree of `.tar.gz` can
/// easily triple on the way in, and a disc sized off the compressed total
/// would run out partway through the copy.
/// `flatten` matches [`DirImportOptions::flatten_archives`]: it drops the
/// per-archive directory from the count. It does not try to predict how many
/// entries several archives share once merged, so a flattened estimate errs
/// high — the safe direction for sizing.
pub fn measure_dir(root: &Path, expand_archives: bool, flatten: bool) -> Result<(u64, u64, u64)> {
    let mut files = 0u64;
    let mut dirs = 0u64;
    let mut bytes = 0u64;
    for e in walk(root)? {
        match e.kind {
            HostKind::Dir => dirs += 1,
            HostKind::File { size } => {
                if expand_archives && crate::fs::tar_import::looks_like_tar_archive(&e.host) {
                    let (f, d, b) = crate::fs::tar_import::measure_tar_expanded(&e.host)?;
                    files += f;
                    dirs += d + u64::from(!flatten);
                    bytes += b;
                } else {
                    files += 1;
                    bytes += size;
                }
            }
            _ => {}
        }
    }
    Ok((files, dirs, bytes))
}

/// Project the volume size needed to hold a tree of `content_bytes` across
/// `files` files and `dirs` directories, for `--size auto`.
///
/// Every filesystem rounds each file up to a block and spends further blocks on
/// its own metadata, so the raw content total always undershoots. This adds a
/// per-entry block allowance plus a proportional slack margin and rounds up to
/// a whole mebibyte. Deliberately generous: overshooting costs a few MB of
/// empty space, undershooting costs the user a failed import most of the way
/// through a long copy.
pub fn projected_volume_bytes(files: u64, dirs: u64, content_bytes: u64, block_size: u64) -> u64 {
    let block = block_size.max(512);
    // Per-entry: one block of internal fragmentation on average, plus a block
    // of directory/inode overhead.
    let per_entry = (files + dirs).saturating_mul(block.saturating_mul(2));
    let base = content_bytes
        .saturating_add(per_entry)
        // Filesystem-level metadata (bitmaps, inode tables, superblocks) plus
        // headroom, as a fraction of the content.
        .saturating_add(content_bytes / 8);
    // Never propose something uselessly tiny.
    let floor = 4 * 1024 * 1024;
    let mib = 1024 * 1024;
    base.max(floor).div_ceil(mib).saturating_mul(mib)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::filesystem::Filesystem;

    fn blank_fat() -> Vec<u8> {
        crate::fs::fat::create_blank_fat(16 * 1024 * 1024, Some("DIRIMP")).expect("format")
    }

    fn tree(root: &Path) {
        std::fs::create_dir_all(root.join("sub/deeper")).unwrap();
        std::fs::write(root.join("a.txt"), b"alpha").unwrap();
        std::fs::write(root.join("sub/b.txt"), b"bravo").unwrap();
        std::fs::write(root.join("sub/deeper/c.bin"), vec![7u8; 5000]).unwrap();
    }

    #[test]
    fn imports_a_host_tree_preserving_structure_and_content() {
        let tmp = tempfile::tempdir().unwrap();
        tree(tmp.path());

        let img = blank_fat();
        let mut fs =
            crate::fs::fat::FatFilesystem::open(std::io::Cursor::new(img), 0).expect("open");
        let root = Filesystem::root(&mut fs).expect("root");
        let stats = import_dir(
            &mut fs,
            &root,
            tmp.path(),
            &DirImportOptions::default(),
            &|_| {},
        )
        .expect("import");

        assert_eq!(stats.files, 3);
        assert_eq!(stats.dirs_created, 2);
        assert_eq!(stats.total_bytes, 5 + 5 + 5000);

        // Content survives the trip, including the nested one.
        let entries = fs.list_directory(&root).expect("list");
        let a = entries.iter().find(|e| e.name == "a.txt").expect("a.txt");
        assert_eq!(fs.read_file(a, usize::MAX).unwrap(), b"alpha");

        let sub = entries.iter().find(|e| e.name == "sub").expect("sub");
        let sub_kids = fs.list_directory(sub).expect("list sub");
        let b = sub_kids.iter().find(|e| e.name == "b.txt").expect("b.txt");
        assert_eq!(fs.read_file(b, usize::MAX).unwrap(), b"bravo");
    }

    /// The walk must not follow symlinks — following them can import a
    /// subtree repeatedly or escape the import root entirely.
    #[cfg(unix)]
    #[test]
    fn symlinks_are_recorded_not_followed() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("real")).unwrap();
        std::fs::write(tmp.path().join("real/file.txt"), b"data").unwrap();
        std::os::unix::fs::symlink("real", tmp.path().join("link")).unwrap();

        let entries = walk(tmp.path()).unwrap();
        let link = entries
            .iter()
            .find(|e| e.comps == vec!["link".to_string()])
            .expect("link present");
        assert!(matches!(link.kind, HostKind::Symlink { .. }));
        // "real/file.txt" appears exactly once — not again under "link/".
        let count = entries
            .iter()
            .filter(|e| e.comps.last().map(|s| s.as_str()) == Some("file.txt"))
            .count();
        assert_eq!(count, 1, "symlink was followed");
    }

    /// A tree with symlinks must still import onto a filesystem that can't
    /// store them — dropped and counted, not fatal.
    #[cfg(unix)]
    #[test]
    fn symlinks_are_dropped_not_fatal_on_fat() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("real.txt"), b"data").unwrap();
        std::os::unix::fs::symlink("real.txt", tmp.path().join("link.txt")).unwrap();

        let img = blank_fat();
        let mut fs =
            crate::fs::fat::FatFilesystem::open(std::io::Cursor::new(img), 0).expect("open");
        let root = Filesystem::root(&mut fs).expect("root");
        let stats = import_dir(
            &mut fs,
            &root,
            tmp.path(),
            &DirImportOptions::default(),
            &|_| {},
        )
        .expect("import must not abort on an unsupported symlink");
        assert_eq!(stats.files, 1);
        assert_eq!(stats.symlinks_skipped, 1);

        let pf = preflight_dir(&fs, tmp.path(), &DirImportOptions::default()).expect("preflight");
        assert_eq!(pf.symlinks_dropped, 1);
        assert!(pf.has_warnings());
    }

    #[test]
    fn walk_order_is_deterministic() {
        let tmp = tempfile::tempdir().unwrap();
        for n in ["zebra", "alpha", "middle"] {
            std::fs::write(tmp.path().join(n), b"x").unwrap();
        }
        let names: Vec<String> = walk(tmp.path())
            .unwrap()
            .into_iter()
            .map(|e| e.comps.join("/"))
            .collect();
        assert_eq!(names, vec!["alpha", "middle", "zebra"]);
    }

    fn write_tgz(path: &Path, entries: &[(&str, &[u8])]) {
        let f = std::fs::File::create(path).unwrap();
        let enc = flate2::write::GzEncoder::new(f, flate2::Compression::default());
        let mut b = tar::Builder::new(enc);
        for (name, data) in entries {
            let mut h = tar::Header::new_gnu();
            h.set_size(data.len() as u64);
            h.set_mode(0o644);
            h.set_cksum();
            b.append_data(&mut h, name, *data).unwrap();
        }
        b.into_inner().unwrap().finish().unwrap();
    }

    /// Default is to copy an archive in verbatim; `--expand-archives` unpacks
    /// it into a directory named after it. Both must be reachable, because
    /// which one is right depends on what the disc is for.
    #[test]
    fn archives_are_copied_verbatim_unless_expansion_is_requested() {
        let tmp = tempfile::tempdir().unwrap();
        write_tgz(
            &tmp.path().join("pkg-1.0.tar.gz"),
            &[("inner/one.txt", b"one"), ("inner/two.txt", b"two")],
        );

        // Default: one opaque file, no expansion.
        let img = blank_fat();
        let mut fs =
            crate::fs::fat::FatFilesystem::open(std::io::Cursor::new(img), 0).expect("open");
        let root = Filesystem::root(&mut fs).expect("root");
        let stats = import_dir(
            &mut fs,
            &root,
            tmp.path(),
            &DirImportOptions::default(),
            &|_| {},
        )
        .expect("import");
        assert_eq!(stats.files, 1);
        assert_eq!(stats.archives_expanded, 0);
        let names: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(names.iter().any(|n| n == "pkg-1.0.tar.gz"));

        // Opt in: the archive becomes a directory of its contents.
        let img = blank_fat();
        let mut fs =
            crate::fs::fat::FatFilesystem::open(std::io::Cursor::new(img), 0).expect("open");
        let root = Filesystem::root(&mut fs).expect("root");
        let opts = DirImportOptions {
            expand_archives: true,
            ..Default::default()
        };
        let stats = import_dir(&mut fs, &root, tmp.path(), &opts, &|_| {}).expect("import");
        assert_eq!(stats.archives_expanded, 1);
        assert_eq!(stats.files, 2, "both archive members should land");

        let names: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(
            !names.iter().any(|n| n.contains("tar.gz")),
            "the archive itself should not be copied: {names:?}"
        );
        // pkg-1.0/inner/one.txt
        let dir = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name.eq_ignore_ascii_case("pkg-1.0"))
            .expect("expanded directory");
        let inner = fs
            .list_directory(&dir)
            .unwrap()
            .into_iter()
            .find(|e| e.name.eq_ignore_ascii_case("inner"))
            .expect("inner dir");
        let kids = fs.list_directory(&inner).unwrap();
        let one = kids
            .iter()
            .find(|e| e.name.eq_ignore_ascii_case("one.txt"))
            .expect("one.txt");
        assert_eq!(fs.read_file(one, usize::MAX).unwrap(), b"one");
    }

    /// Flattening drops the per-archive wrapper directory so sibling archives
    /// share one root — the shape IRIX `inst` wants from a `.tardist` set.
    #[test]
    fn flatten_merges_every_archive_into_one_root() {
        let tmp = tempfile::tempdir().unwrap();
        write_tgz(
            &tmp.path().join("pkg-a.tardist"),
            &[("a_prod", b"A"), ("a_prod.sw", b"A")],
        );
        write_tgz(
            &tmp.path().join("pkg-b.tardist"),
            &[("b_prod", b"B"), ("b_prod.sw", b"B")],
        );

        let img = blank_fat();
        let mut fs =
            crate::fs::fat::FatFilesystem::open(std::io::Cursor::new(img), 0).expect("open");
        let root = Filesystem::root(&mut fs).expect("root");
        let opts = DirImportOptions {
            expand_archives: true,
            flatten_archives: true,
            ..Default::default()
        };
        let stats = import_dir(&mut fs, &root, tmp.path(), &opts, &|_| {}).expect("import");
        assert_eq!(stats.archives_expanded, 2);
        assert_eq!(stats.files, 4);
        // No wrapper directories at all — every product image sits at the root.
        assert_eq!(
            stats.dirs_created, 0,
            "flatten should create no wrapper dirs"
        );

        let mut names: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name.to_ascii_lowercase())
            .collect();
        names.sort();
        assert_eq!(names, vec!["a_prod", "a_prod.sw", "b_prod", "b_prod.sw"]);
    }

    /// The case a real IRIX freeware set hits: several tardists ship the same
    /// shared `fw_common*` product. Flattening must merge them rather than
    /// abort, or the flag is unusable on exactly the trees it exists for.
    #[test]
    fn flatten_tolerates_the_shared_product_archives_have_in_common() {
        let tmp = tempfile::tempdir().unwrap();
        for pkg in ["fw_one", "fw_two", "fw_three"] {
            write_tgz(
                &tmp.path().join(format!("{pkg}.tardist")),
                &[
                    (pkg, b"unique"),
                    // Every freeware tardist carries this same product.
                    ("fw_common", b"shared"),
                    ("fw_common.idb", b"shared"),
                ],
            );
        }

        let img = blank_fat();
        let mut fs =
            crate::fs::fat::FatFilesystem::open(std::io::Cursor::new(img), 0).expect("open");
        let root = Filesystem::root(&mut fs).expect("root");
        let opts = DirImportOptions {
            // What the CLI builds for --flatten-folders: Skip, not Error.
            shared: ImportOptions {
                conflict: ImportConflict::Skip,
                ..Default::default()
            },
            expand_archives: true,
            flatten_archives: true,
        };
        let stats = import_dir(&mut fs, &root, tmp.path(), &opts, &|_| {})
            .expect("a shared product across archives must not abort the import");

        // 3 unique products + fw_common + fw_common.idb written once each.
        assert_eq!(stats.files, 5);
        assert_eq!(stats.skipped_existing, 4, "the 2nd/3rd copies are skipped");

        let mut names: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name.to_ascii_lowercase())
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec!["fw_common", "fw_common.idb", "fw_one", "fw_three", "fw_two"]
        );
    }

    /// Without flattening the same set stays segregated per archive, so the
    /// shared product is duplicated rather than merged and nothing collides.
    #[test]
    fn without_flatten_each_archive_keeps_its_own_folder() {
        let tmp = tempfile::tempdir().unwrap();
        for pkg in ["fw_one", "fw_two"] {
            write_tgz(
                &tmp.path().join(format!("{pkg}.tardist")),
                &[(pkg, b"unique"), ("fw_common", b"shared")],
            );
        }
        let img = blank_fat();
        let mut fs =
            crate::fs::fat::FatFilesystem::open(std::io::Cursor::new(img), 0).expect("open");
        let root = Filesystem::root(&mut fs).expect("root");
        let opts = DirImportOptions {
            expand_archives: true,
            ..Default::default()
        };
        let stats = import_dir(&mut fs, &root, tmp.path(), &opts, &|_| {}).expect("import");
        assert_eq!(stats.dirs_created, 2, "one wrapper dir per archive");
        assert_eq!(stats.files, 4, "fw_common lands once per archive");
        assert_eq!(stats.skipped_existing, 0);
    }

    /// Sizing has to key off what an archive *becomes*, not its compressed
    /// size, or `--size auto` under-provisions and the copy dies partway in.
    #[test]
    fn expansion_changes_the_measured_size() {
        let tmp = tempfile::tempdir().unwrap();
        // Highly compressible payload: on disk it is tiny, expanded it is not.
        write_tgz(
            &tmp.path().join("big.tar.gz"),
            &[("blob", &vec![0u8; 200_000])],
        );

        let (_, _, packed) = measure_dir(tmp.path(), false, false).unwrap();
        let (_, _, expanded) = measure_dir(tmp.path(), true, false).unwrap();
        assert!(
            expanded > packed * 4,
            "expanded ({expanded}) should dwarf the compressed size ({packed})"
        );
        assert!(expanded >= 200_000);
    }

    #[test]
    fn expanded_dir_name_strips_archive_suffixes() {
        assert_eq!(expanded_dir_name("foo-1.2.tar.gz"), "foo-1.2");
        assert_eq!(expanded_dir_name("bar.tardist"), "bar");
        assert_eq!(expanded_dir_name("baz.tgz"), "baz");
        assert_eq!(expanded_dir_name("qux.TAR.GZ"), "qux");
        // Not an archive suffix -> untouched.
        assert_eq!(expanded_dir_name("plain.txt"), "plain.txt");
        // Stripping to nothing keeps the original.
        assert_eq!(expanded_dir_name(".tar"), ".tar");
    }

    #[test]
    fn measure_and_projection_leave_room_for_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        tree(tmp.path());
        let (files, dirs, bytes) = measure_dir(tmp.path(), false, false).unwrap();
        assert_eq!(files, 3);
        assert_eq!(dirs, 2);
        assert_eq!(bytes, 5010);
        // The projection must exceed raw content, and land on a whole MiB.
        let projected = projected_volume_bytes(files, dirs, bytes, 512);
        assert!(projected > bytes);
        assert_eq!(projected % (1024 * 1024), 0);
    }
}
