//! Export a filesystem (or a subtree) from a disk image to a single
//! `.tar.gz` / `.tar.zst` / `.tar` archive, streamed straight to a writer —
//! no host tree is staged.
//!
//! **Why a tar archive.** Case-sensitive source filesystems (EFS, ext, XFS,
//! HFSX, AFFS, …) routinely hold names that differ only in case (`Makefile`
//! vs `makefile`). Extracting those directly onto a case-insensitive host
//! (macOS APFS, Windows NTFS) silently merges them. A tar archive preserves
//! exact names, paths, and symlinks; the user extracts on a case-sensitive
//! volume — or just inspects the archive. It also avoids macOS xattr /
//! resource-fork cruft and preserves Unix mode / uid / gid losslessly.
//!
//! **v1 scope.** Data fork only — HFS/HFS+ resource forks are dropped (and
//! counted). Real symlinks are preserved (unlike `get`, which degrades them
//! to text files). Unix mode/uid/gid are carried when the source FS provides
//! them; otherwise sane defaults (0644 file / 0755 dir / 0777 symlink). File
//! mtimes are not yet preserved across all FS families, so entries are
//! stamped epoch-0 for now.
//!
//! Shared by the CLI `tar` verb and the GUI "Export to .tar.gz…" action.

use std::collections::HashSet;
use std::io::{Seek, SeekFrom, Write};

use anyhow::{anyhow, Context, Result};

use crate::fs::entry::{EntryType, FileEntry};
use crate::fs::filesystem::Filesystem;

/// Container compression for the tar stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TarCompression {
    /// `.tar.gz` — gzip (the default).
    Gzip,
    /// `.tar.zst` — zstandard.
    Zstd,
    /// `.tar` — uncompressed.
    None,
}

impl TarCompression {
    /// Infer the compression from an output path's extension: `.tar` → none,
    /// `.tar.zst` / `.tzst` → zstd, anything else (`.tgz`, `.tar.gz`, …) →
    /// gzip.
    pub fn infer_from_path(path: &std::path::Path) -> Self {
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        if name.ends_with(".tar.zst") || name.ends_with(".tzst") {
            TarCompression::Zstd
        } else if name.ends_with(".tar") {
            TarCompression::None
        } else {
            TarCompression::Gzip
        }
    }
}

/// Knobs for [`export_tar`].
pub struct TarExportOptions<'a> {
    /// Return `true` to skip an entry — and, for a directory, its whole
    /// subtree. The argument is the source FS path (e.g. `/etc/hosts`).
    pub exclude: &'a dyn Fn(&str) -> bool,
    /// Emit special files (block/char device, fifo, socket) as zero-length
    /// entries instead of skipping them. Default `false`.
    pub include_special: bool,
    /// Files larger than this are spilled to a tempfile while their tar entry
    /// is built, instead of being buffered in RAM. Default 64 MiB.
    pub spill_threshold: u64,
}

impl Default for TarExportOptions<'_> {
    fn default() -> Self {
        Self {
            exclude: &|_| false,
            include_special: false,
            spill_threshold: 64 * 1024 * 1024,
        }
    }
}

/// Tally of what the export produced. Surfaced to the user as a summary.
#[derive(Default, Debug, Clone)]
pub struct TarExportStats {
    pub files: u64,
    pub dirs: u64,
    pub symlinks: u64,
    pub specials_skipped: u64,
    pub excluded: u64,
    /// HFS/HFS+ files whose resource fork was dropped (data fork only in v1).
    pub resource_forks_dropped: u64,
    /// Directories that contained two children differing only in case — the
    /// exact situation a tar archive protects against on a case-insensitive
    /// host. Informational.
    pub case_collisions: u64,
    /// Total data-fork bytes written (uncompressed).
    pub total_bytes: u64,
}

/// Archive `root` (a directory or a single file) into `out` as a tar stream
/// with the chosen `compression`. `archive_prefix` is the path prefix inside
/// the archive (`""` for a whole-volume top-level layout, or e.g. `"usr"`
/// for a subtree so entries land under `usr/…`). `progress` is invoked after
/// each entry is appended, with the running stats, for UI updates.
pub fn export_tar<W: Write>(
    fs: &mut dyn Filesystem,
    root: &FileEntry,
    archive_prefix: &str,
    out: W,
    compression: TarCompression,
    opts: &TarExportOptions,
    progress: &dyn Fn(&TarExportStats),
) -> Result<TarExportStats> {
    let mut stats = TarExportStats::default();
    match compression {
        TarCompression::Gzip => {
            let enc = flate2::write::GzEncoder::new(out, flate2::Compression::default());
            let mut builder = tar::Builder::new(enc);
            export_into(
                fs,
                root,
                archive_prefix,
                &mut builder,
                opts,
                &mut stats,
                progress,
            )?;
            let enc = builder.into_inner().context("finishing tar stream")?;
            enc.finish().context("finishing gzip stream")?;
        }
        TarCompression::Zstd => {
            // Level 0 = zstd's default (currently 3).
            let enc = crate::rbformats::zstd_compat::ZstdEncoder::new(out, 0)
                .context("init zstd encoder")?;
            let mut builder = tar::Builder::new(enc);
            export_into(
                fs,
                root,
                archive_prefix,
                &mut builder,
                opts,
                &mut stats,
                progress,
            )?;
            let enc = builder.into_inner().context("finishing tar stream")?;
            enc.finish().context("finishing zstd stream")?;
        }
        TarCompression::None => {
            let mut builder = tar::Builder::new(out);
            export_into(
                fs,
                root,
                archive_prefix,
                &mut builder,
                opts,
                &mut stats,
                progress,
            )?;
            builder.into_inner().context("finishing tar stream")?;
        }
    }
    Ok(stats)
}

fn export_into<W: Write>(
    fs: &mut dyn Filesystem,
    root: &FileEntry,
    archive_prefix: &str,
    builder: &mut tar::Builder<W>,
    opts: &TarExportOptions,
    stats: &mut TarExportStats,
    progress: &dyn Fn(&TarExportStats),
) -> Result<()> {
    match root.entry_type {
        EntryType::Directory => {
            // For a subtree export, emit the root directory itself so the
            // archive carries `usr/`, `usr/bin/…`. For a whole-volume export
            // (`archive_prefix == ""`) there is no top entry.
            if !archive_prefix.is_empty() {
                append_dir(builder, archive_prefix, root, stats)?;
                progress(stats);
            }
            walk(fs, root, archive_prefix, builder, opts, stats, progress)?;
        }
        EntryType::File => {
            let arch = if archive_prefix.is_empty() {
                root.name.clone()
            } else {
                archive_prefix.to_string()
            };
            append_file(fs, builder, &arch, root, opts, stats)?;
            progress(stats);
        }
        EntryType::Symlink => {
            let arch = if archive_prefix.is_empty() {
                root.name.clone()
            } else {
                archive_prefix.to_string()
            };
            append_symlink(builder, &arch, root, stats)?;
            progress(stats);
        }
        EntryType::Special => {
            // A bare special-file root is a no-op unless explicitly included.
            if opts.include_special {
                let arch = if archive_prefix.is_empty() {
                    root.name.clone()
                } else {
                    archive_prefix.to_string()
                };
                append_special(builder, &arch, root)?;
                progress(stats);
            }
        }
    }
    Ok(())
}

fn walk<W: Write>(
    fs: &mut dyn Filesystem,
    dir: &FileEntry,
    prefix: &str,
    builder: &mut tar::Builder<W>,
    opts: &TarExportOptions,
    stats: &mut TarExportStats,
    progress: &dyn Fn(&TarExportStats),
) -> Result<()> {
    let children = fs
        .list_directory(dir)
        .map_err(|e| anyhow!("list_directory {}: {e}", dir.path))?;

    // Case-collision tally — the very situation tar export defends against.
    let mut seen_ci: HashSet<String> = HashSet::with_capacity(children.len());
    for c in &children {
        if !seen_ci.insert(c.name.to_ascii_lowercase()) {
            stats.case_collisions += 1;
        }
    }

    for child in children {
        if (opts.exclude)(&child.path) {
            stats.excluded += 1;
            continue;
        }
        let arch = if prefix.is_empty() {
            child.name.clone()
        } else {
            format!("{prefix}/{}", child.name)
        };
        match child.entry_type {
            EntryType::Directory => {
                append_dir(builder, &arch, &child, stats)?;
                progress(stats);
                walk(fs, &child, &arch, builder, opts, stats, progress)?;
            }
            EntryType::File => {
                append_file(fs, builder, &arch, &child, opts, stats)?;
                progress(stats);
            }
            EntryType::Symlink => {
                append_symlink(builder, &arch, &child, stats)?;
                progress(stats);
            }
            EntryType::Special => {
                if opts.include_special {
                    append_special(builder, &arch, &child)?;
                    progress(stats);
                } else {
                    stats.specials_skipped += 1;
                }
            }
        }
    }
    Ok(())
}

/// Build a base tar header carrying the entry's Unix metadata, falling back
/// to `default_mode` when the source FS has no mode concept.
fn base_header(entry: &FileEntry, default_mode: u32) -> tar::Header {
    let mut h = tar::Header::new_gnu();
    let mode = entry.mode.map(|m| m & 0o7777).unwrap_or(default_mode);
    h.set_mode(mode);
    h.set_uid(entry.uid.unwrap_or(0) as u64);
    h.set_gid(entry.gid.unwrap_or(0) as u64);
    // v1: mtimes aren't uniformly available across FS families; stamp 0.
    h.set_mtime(0);
    h
}

fn append_dir<W: Write>(
    builder: &mut tar::Builder<W>,
    arch: &str,
    entry: &FileEntry,
    stats: &mut TarExportStats,
) -> Result<()> {
    let mut h = base_header(entry, 0o755);
    h.set_entry_type(tar::EntryType::Directory);
    h.set_size(0);
    // tar directory members conventionally end with '/'.
    let path = format!("{arch}/");
    builder
        .append_data(&mut h, &path, std::io::empty())
        .with_context(|| format!("append dir {arch}"))?;
    stats.dirs += 1;
    Ok(())
}

fn append_symlink<W: Write>(
    builder: &mut tar::Builder<W>,
    arch: &str,
    entry: &FileEntry,
    stats: &mut TarExportStats,
) -> Result<()> {
    let target = entry.symlink_target.clone().unwrap_or_default();
    let mut h = base_header(entry, 0o777);
    h.set_entry_type(tar::EntryType::Symlink);
    h.set_size(0);
    builder
        .append_link(&mut h, arch, &target)
        .with_context(|| format!("append symlink {arch} -> {target}"))?;
    stats.symlinks += 1;
    Ok(())
}

/// Special files (devices/fifo/socket) have no portable content; emit a
/// zero-length regular file as a placeholder so the path is preserved.
fn append_special<W: Write>(
    builder: &mut tar::Builder<W>,
    arch: &str,
    entry: &FileEntry,
) -> Result<()> {
    let mut h = base_header(entry, 0o644);
    h.set_entry_type(tar::EntryType::Regular);
    h.set_size(0);
    builder
        .append_data(&mut h, arch, std::io::empty())
        .with_context(|| format!("append special placeholder {arch}"))?;
    Ok(())
}

fn append_file<W: Write>(
    fs: &mut dyn Filesystem,
    builder: &mut tar::Builder<W>,
    arch: &str,
    entry: &FileEntry,
    opts: &TarExportOptions,
    stats: &mut TarExportStats,
) -> Result<()> {
    if entry.resource_fork_size.unwrap_or(0) > 0 {
        stats.resource_forks_dropped += 1;
    }

    // We must know the exact data-fork length for the tar header, and
    // write_file_to is push-only, so buffer the content. Small files go to
    // RAM; large ones spill to a tempfile to keep memory bounded.
    if entry.size <= opts.spill_threshold {
        let mut buf: Vec<u8> = Vec::with_capacity(entry.size.min(1 << 20) as usize);
        let n = fs
            .write_file_to(entry, &mut buf)
            .map_err(|e| anyhow!("write_file_to {}: {e}", entry.path))?;
        let mut h = base_header(entry, 0o644);
        h.set_size(n);
        builder
            .append_data(&mut h, arch, &buf[..])
            .with_context(|| format!("append file {arch}"))?;
        stats.total_bytes += n;
    } else {
        let mut tmp = tempfile::tempfile().context("create spill tempfile")?;
        let n = fs
            .write_file_to(entry, &mut tmp)
            .map_err(|e| anyhow!("write_file_to {}: {e}", entry.path))?;
        tmp.flush().ok();
        tmp.seek(SeekFrom::Start(0))
            .context("rewind spill tempfile")?;
        let mut h = base_header(entry, 0o644);
        h.set_size(n);
        builder
            .append_data(&mut h, arch, &mut tmp)
            .with_context(|| format!("append file {arch}"))?;
        stats.total_bytes += n;
    }
    stats.files += 1;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::filesystem::EditableFilesystem;
    use std::collections::HashMap;
    use std::io::Read;

    /// Create a file with `data` via the editable FS API (handles the
    /// `&mut dyn Read` + explicit length the trait wants).
    fn put_file(efs: &mut dyn EditableFilesystem, parent: &FileEntry, name: &str, data: &[u8]) {
        let mut r: &[u8] = data;
        efs.create_file(parent, name, &mut r, data.len() as u64, &Default::default())
            .unwrap();
    }

    /// (path, entry_type, link_target) for one archive member.
    type TarMeta = (String, tar::EntryType, Option<String>);

    /// Decompress + parse a tar.gz blob into a map of path -> contents, plus
    /// a list of per-member [`TarMeta`].
    fn read_targz(blob: &[u8]) -> (HashMap<String, Vec<u8>>, Vec<TarMeta>) {
        let gz = flate2::read::GzDecoder::new(blob);
        let mut ar = tar::Archive::new(gz);
        let mut files = HashMap::new();
        let mut meta = Vec::new();
        for e in ar.entries().unwrap() {
            let mut e = e.unwrap();
            let path = e.path().unwrap().to_string_lossy().to_string();
            let et = e.header().entry_type();
            let link = e
                .link_name()
                .ok()
                .flatten()
                .map(|p| p.to_string_lossy().to_string());
            let mut data = Vec::new();
            if et.is_file() {
                e.read_to_end(&mut data).unwrap();
            }
            files.insert(path.clone(), data);
            meta.push((path, et, link));
        }
        (files, meta)
    }

    fn export_blank_fat_volume() -> (HashMap<String, Vec<u8>>, Vec<TarMeta>, TarExportStats) {
        // Build a FAT volume with a couple of files + a subdir via the
        // editable FS API, then tar-export the whole volume.
        let flat = crate::fs::fat::create_blank_fat(2 * 1024 * 1024, Some("TARTEST")).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("v.img");
        std::fs::write(&img, &flat).unwrap();

        // Populate via the editable filesystem.
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&img)
                .unwrap();
            let mut efs = crate::fs::open_editable_filesystem(file, 0, 0, None).unwrap();
            let root = efs.root().unwrap();
            put_file(&mut *efs, &root, "HELLO.TXT", b"hello world");
            let data: Vec<u8> = (0u8..200).collect();
            put_file(&mut *efs, &root, "DATA.BIN", &data);
            let sub = efs
                .create_directory(&root, "SUB", &Default::default())
                .unwrap();
            put_file(&mut *efs, &sub, "INNER.TXT", b"nested file");
            efs.sync_metadata().unwrap();
        }

        let file = std::fs::File::open(&img).unwrap();
        let mut fs = crate::fs::open_filesystem(file, 0, 0, None).unwrap();
        let root = fs.root().unwrap();
        let mut out = Vec::new();
        let opts = TarExportOptions::default();
        let stats = export_tar(
            &mut *fs,
            &root,
            "",
            &mut out,
            TarCompression::Gzip,
            &opts,
            &|_| {},
        )
        .unwrap();
        let (files, meta) = read_targz(&out);
        (files, meta, stats)
    }

    #[test]
    fn exports_files_and_dirs_with_contents() {
        let (files, meta, stats) = export_blank_fat_volume();
        assert!(files.contains_key("HELLO.TXT"), "paths: {:?}", files.keys());
        assert_eq!(files["HELLO.TXT"], b"hello world");
        assert_eq!(files["DATA.BIN"], (0u8..200).collect::<Vec<u8>>());
        assert_eq!(files["SUB/INNER.TXT"], b"nested file");
        // The subdir is present as a directory member.
        assert!(meta
            .iter()
            .any(|(p, t, _)| p == "SUB/" && *t == tar::EntryType::Directory));
        assert_eq!(stats.files, 3);
        assert_eq!(stats.dirs, 1);
    }

    #[test]
    fn infer_compression_from_extension() {
        use std::path::Path;
        assert_eq!(
            TarCompression::infer_from_path(Path::new("out.tar")),
            TarCompression::None
        );
        assert_eq!(
            TarCompression::infer_from_path(Path::new("out.tar.zst")),
            TarCompression::Zstd
        );
        assert_eq!(
            TarCompression::infer_from_path(Path::new("out.tgz")),
            TarCompression::Gzip
        );
        assert_eq!(
            TarCompression::infer_from_path(Path::new("out.tar.gz")),
            TarCompression::Gzip
        );
    }

    #[test]
    fn exclude_prunes_matching_paths() {
        let flat = crate::fs::fat::create_blank_fat(2 * 1024 * 1024, Some("EXCL")).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("v.img");
        std::fs::write(&img, &flat).unwrap();
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&img)
                .unwrap();
            let mut efs = crate::fs::open_editable_filesystem(file, 0, 0, None).unwrap();
            let root = efs.root().unwrap();
            put_file(&mut *efs, &root, "KEEP.TXT", b"keep");
            put_file(&mut *efs, &root, "DROP.LOG", b"drop");
            efs.sync_metadata().unwrap();
        }
        let file = std::fs::File::open(&img).unwrap();
        let mut fs = crate::fs::open_filesystem(file, 0, 0, None).unwrap();
        let root = fs.root().unwrap();
        let mut out = Vec::new();
        let exclude = |p: &str| p.ends_with(".LOG");
        let opts = TarExportOptions {
            exclude: &exclude,
            ..Default::default()
        };
        let stats = export_tar(
            &mut *fs,
            &root,
            "",
            &mut out,
            TarCompression::Gzip,
            &opts,
            &|_| {},
        )
        .unwrap();
        let (files, _) = read_targz(&out);
        assert!(files.contains_key("KEEP.TXT"));
        assert!(!files.contains_key("DROP.LOG"));
        assert_eq!(stats.excluded, 1);
        assert_eq!(stats.files, 1);
    }
}
