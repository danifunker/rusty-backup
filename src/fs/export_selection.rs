//! Export a Commander selection out of a source filesystem to the host in one
//! of several output shapes — the engine behind the "Export as" control.
//!
//! Two destination shapes, chosen by [`ExportFormat::is_single_file`]:
//! - **folder** outputs ([`export_to_folder`]) — loose files (resource forks in
//!   the chosen [`ResourceForkMode`] container) or each file's data fork
//!   individually gzip/zstd-compressed;
//! - **single archive file** outputs ([`export_to_file`]) — a tar (optionally
//!   gzip/zstd-compressed), a zip (data fork + AppleDouble `._name` sidecar for
//!   Mac forks), or a classic StuffIt `.sit` (forks + type/creator native).
//!
//! All formats reuse dependencies already in the tree (`tar`, `zip`, `flate2`,
//! the zstd backend selected by the `native-zstd` / `pure-zstd` feature, and the
//! in-crate StuffIt writer), so nothing here is feature-gated.
//!
//! GUI-free and generic over the [`Filesystem`] trait, so it drives an image
//! volume, a backup, an inline wrapper mount, or a remote image identically.

use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};

use crate::fs::entry::{EntryType, FileEntry};
use crate::fs::filesystem::Filesystem;
use crate::fs::fork_export::{export_file_with_fork, safe_name};
use crate::fs::resource_fork::{build_appledouble, MacFileDates, ResourceForkMode};
use crate::fs::tar_export::{export_tar_multi, TarCompression, TarExportOptions};
use crate::macarchive::stuffit::{build_archive_tree, StuffItInput, StuffItInputNode, WriteMethod};

/// One user-selectable export output shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    /// Loose files into a folder; resource forks preserved via the chosen
    /// [`ResourceForkMode`] container (MacBinary / BinHex / AppleDouble / …).
    LooseFiles,
    /// Each file's data fork gzip-compressed to `<name>.gz` in a mirrored
    /// folder tree. Resource forks are not preserved (use loose / tar / sit).
    GzipPerFile,
    /// Each file's data fork zstd-compressed to `<name>.zst` in a mirrored
    /// folder tree. Resource forks are not preserved.
    ZstdPerFile,
    /// A single uncompressed `.tar`. Data forks only; preserves *nix symlinks.
    Tar,
    /// A single gzip-compressed `.tar.gz`.
    TarGz,
    /// A single zstd-compressed `.tar.zst`.
    TarZstd,
    /// One BinHex 4.0 `.hqx` text file per file (both forks + Finder info),
    /// into a mirrored folder tree.
    BinHex,
    /// A single `.zip`; Mac resource forks ride along as `__MACOSX/._name`
    /// AppleDouble members (the convention macOS itself uses).
    Zip,
    /// A single classic StuffIt `.sit`; forks + Finder type/creator native.
    StuffIt,
    /// A single MacArchive `.mar`; forks + Finder type/creator native, same
    /// input tree as StuffIt.
    MacArchive,
}

impl ExportFormat {
    /// Every format, in menu order (folder outputs first, then archives).
    pub const ALL: [ExportFormat; 10] = [
        ExportFormat::LooseFiles,
        ExportFormat::GzipPerFile,
        ExportFormat::ZstdPerFile,
        ExportFormat::BinHex,
        ExportFormat::Tar,
        ExportFormat::TarGz,
        ExportFormat::TarZstd,
        ExportFormat::Zip,
        ExportFormat::StuffIt,
        ExportFormat::MacArchive,
    ];

    /// Short menu / dropdown label.
    pub fn label(self) -> &'static str {
        match self {
            ExportFormat::LooseFiles => "Loose files",
            ExportFormat::GzipPerFile => "Gzip each file (.gz)",
            ExportFormat::ZstdPerFile => "Zstd each file (.zst)",
            ExportFormat::BinHex => "BinHex each file (.hqx)",
            ExportFormat::Tar => "Tar (.tar)",
            ExportFormat::TarGz => "Tar + gzip (.tar.gz)",
            ExportFormat::TarZstd => "Tar + zstd (.tar.zst)",
            ExportFormat::Zip => "Zip (.zip)",
            ExportFormat::StuffIt => "StuffIt (.sit)",
            ExportFormat::MacArchive => "Mac Archive (.mar)",
        }
    }

    /// True when the output is a single archive *file* (the caller prompts for a
    /// filename); false when it writes loose outputs into a *folder*.
    pub fn is_single_file(self) -> bool {
        matches!(
            self,
            ExportFormat::Tar
                | ExportFormat::TarGz
                | ExportFormat::TarZstd
                | ExportFormat::Zip
                | ExportFormat::StuffIt
                | ExportFormat::MacArchive
        )
    }

    /// The extension (no dot) for the single-file formats, for the save dialog.
    pub fn file_extension(self) -> Option<&'static str> {
        match self {
            ExportFormat::Tar => Some("tar"),
            ExportFormat::TarGz => Some("tar.gz"),
            ExportFormat::TarZstd => Some("tar.zst"),
            ExportFormat::Zip => Some("zip"),
            ExportFormat::StuffIt => Some("sit"),
            ExportFormat::MacArchive => Some("mar"),
            _ => None,
        }
    }
}

/// Tally of what an export produced, for the completion status line.
#[derive(Debug, Default, Clone, Copy)]
pub struct ExportSummary {
    pub files: usize,
    pub bytes: u64,
}

/// Progress callback: `(current_file_path, files_done, bytes_done)`, fired after
/// each file. `bytes` counts source data-fork bytes read.
pub type Progress<'a> = dyn Fn(&str, usize, u64) + 'a;

/// Cancel gate: return `true` to abort at the next file boundary.
pub type Cancelled<'a> = dyn Fn() -> bool + 'a;

fn cancelled_err() -> anyhow::Error {
    anyhow::anyhow!("export cancelled")
}

/// The directory part of an absolute entry path, "" for a top-level entry.
fn parent_dir(path: &str) -> &str {
    match path.trim_end_matches('/').rsplit_once('/') {
        Some((dir, _)) => dir,
        None => "",
    }
}

/// The deepest directory containing every selected entry, "" at the volume root.
///
/// Selections are named relative to this, so picking several files out of one
/// folder still yields bare names (what it always did) while a selection that
/// spans folders keeps enough path to stay unique. Without it every root was
/// named by `archive_name()` alone, so `/a/notes.txt` and `/b/notes.txt` both
/// became `notes.txt` and the second silently replaced the first.
pub fn common_parent(entries: &[FileEntry]) -> String {
    let mut it = entries.iter();
    let Some(first) = it.next() else {
        return String::new();
    };
    let mut base: Vec<&str> = parent_dir(&first.path)
        .split('/')
        .filter(|c| !c.is_empty())
        .collect();
    for e in it {
        let these: Vec<&str> = parent_dir(&e.path)
            .split('/')
            .filter(|c| !c.is_empty())
            .collect();
        let keep = base
            .iter()
            .zip(these.iter())
            .take_while(|(a, b)| a == b)
            .count();
        base.truncate(keep);
        if base.is_empty() {
            break;
        }
    }
    if base.is_empty() {
        String::new()
    } else {
        format!("/{}", base.join("/"))
    }
}

/// `entry`'s own directory relative to `base`; "" when it sits directly in it.
/// Always uses `/` separators — callers targeting the host filesystem convert.
pub fn relative_dir(entry: &FileEntry, base: &str) -> String {
    let dir = parent_dir(&entry.path);
    let rel = dir.strip_prefix(base).unwrap_or(dir);
    rel.trim_matches('/').to_string()
}

/// Export `entries` from `fs` as loose outputs into the directory `dest_dir`.
/// Only [`ExportFormat::LooseFiles`] / [`GzipPerFile`](ExportFormat::GzipPerFile)
/// / [`ZstdPerFile`](ExportFormat::ZstdPerFile) are valid here.
pub fn export_to_folder(
    fs: &mut dyn Filesystem,
    entries: &[FileEntry],
    dest_dir: &Path,
    format: ExportFormat,
    fork_mode: ResourceForkMode,
    progress: &Progress,
    cancelled: &Cancelled,
) -> Result<ExportSummary> {
    let mut summary = ExportSummary::default();
    let base = common_parent(entries);
    for e in entries {
        // Re-create the entry's own folder under `dest_dir` so two same-named
        // files picked from different folders no longer overwrite each other.
        let rel = relative_dir(e, &base);
        let target = if rel.is_empty() {
            dest_dir.to_path_buf()
        } else {
            let t = dest_dir.join(rel.replace('/', std::path::MAIN_SEPARATOR_STR));
            std::fs::create_dir_all(&t).with_context(|| format!("creating {}", t.display()))?;
            t
        };
        folder_recurse(
            fs,
            std::slice::from_ref(e),
            &target,
            format,
            fork_mode,
            progress,
            cancelled,
            &mut summary,
        )?;
    }
    Ok(summary)
}

#[allow(clippy::too_many_arguments)]
fn folder_recurse(
    fs: &mut dyn Filesystem,
    entries: &[FileEntry],
    dest_dir: &Path,
    format: ExportFormat,
    fork_mode: ResourceForkMode,
    progress: &Progress,
    cancelled: &Cancelled,
    summary: &mut ExportSummary,
) -> Result<()> {
    for e in entries {
        if cancelled() {
            return Err(cancelled_err());
        }
        match e.entry_type {
            EntryType::Directory => {
                let children = fs
                    .list_directory(e)
                    .with_context(|| format!("listing '{}'", e.name))?;
                // A volume root has no name of its own — extract its contents
                // straight into `dest_dir`. Joining its literal "/" name would
                // discard `dest_dir` entirely and target the host root.
                let sub = match e.archive_name() {
                    Some(name) => {
                        let sub = dest_dir.join(name);
                        std::fs::create_dir_all(&sub)
                            .with_context(|| format!("creating {}", sub.display()))?;
                        sub
                    }
                    None => dest_dir.to_path_buf(),
                };
                folder_recurse(
                    fs, &children, &sub, format, fork_mode, progress, cancelled, summary,
                )?;
            }
            EntryType::File => {
                match format {
                    // BinHex is a fixed fork container, independent of the loose
                    // "Forks as:" choice.
                    ExportFormat::LooseFiles | ExportFormat::BinHex => {
                        let mode = if format == ExportFormat::BinHex {
                            ResourceForkMode::BinHex
                        } else {
                            fork_mode
                        };
                        export_file_with_fork(fs, e, dest_dir, &safe_name(e), mode)
                            .with_context(|| format!("exporting '{}'", e.name))?;
                    }
                    ExportFormat::GzipPerFile | ExportFormat::ZstdPerFile => {
                        export_compressed_file(fs, e, dest_dir, format)
                            .with_context(|| format!("compressing '{}'", e.name))?;
                    }
                    _ => anyhow::bail!("{:?} is not a folder-output format", format),
                }
                summary.files += 1;
                summary.bytes = summary.bytes.saturating_add(e.size);
                progress(&e.path, summary.files, summary.bytes);
            }
            EntryType::Symlink | EntryType::Special => {
                // Not representable as a loose host file here; skip.
            }
        }
    }
    Ok(())
}

/// Read a file's data fork fully into memory.
fn read_data_fork(fs: &mut dyn Filesystem, e: &FileEntry) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(e.size as usize);
    fs.write_file_to(e, &mut buf)
        .with_context(|| format!("reading '{}'", e.name))?;
    Ok(buf)
}

/// Read a file's resource fork (empty when it has none).
///
/// Only asked of entries that claim one: the trait default is `Ok(0)`, so an
/// error here means a fork that exists could not be decoded — an unsupported
/// StuffIt method, a bad CRC, a truncated archive. Swallowing that wrote a
/// silently fork-less `.zip` / `.sit` and called it a success (R-041).
fn read_resource_fork(fs: &mut dyn Filesystem, e: &FileEntry) -> Result<Vec<u8>> {
    if e.resource_fork_size.unwrap_or(0) == 0 {
        return Ok(Vec::new());
    }
    let mut buf = Vec::new();
    fs.write_resource_fork_to(e, &mut buf)
        .with_context(|| format!("reading resource fork of '{}'", e.name))?;
    Ok(buf)
}

/// Gzip/zstd one file's data fork to `<dest_dir>/<name>.<ext>`.
fn export_compressed_file(
    fs: &mut dyn Filesystem,
    e: &FileEntry,
    dest_dir: &Path,
    format: ExportFormat,
) -> Result<()> {
    let data = read_data_fork(fs, e)?;
    let ext = if format == ExportFormat::GzipPerFile {
        "gz"
    } else {
        "zst"
    };
    let out_path = dest_dir.join(format!("{}.{ext}", safe_name(e)));
    let out = std::fs::File::create(&out_path)
        .with_context(|| format!("creating {}", out_path.display()))?;
    match format {
        ExportFormat::GzipPerFile => {
            let mut enc = flate2::write::GzEncoder::new(out, flate2::Compression::default());
            enc.write_all(&data)?;
            enc.finish().context("finishing gzip stream")?;
        }
        ExportFormat::ZstdPerFile => {
            let mut enc = crate::rbformats::zstd_compat::ZstdEncoder::new(out, 0)
                .context("init zstd encoder")?;
            enc.write_all(&data)?;
            enc.finish().context("finishing zstd stream")?;
        }
        _ => unreachable!("caller guarantees a per-file format"),
    }
    Ok(())
}

/// Export `entries` from `fs` into the single archive file `out_path`. Only the
/// [`ExportFormat::is_single_file`] formats are valid here.
pub fn export_to_file(
    fs: &mut dyn Filesystem,
    entries: &[FileEntry],
    out_path: &Path,
    format: ExportFormat,
    progress: &Progress,
    cancelled: &Cancelled,
) -> Result<ExportSummary> {
    match format {
        ExportFormat::Tar | ExportFormat::TarGz | ExportFormat::TarZstd => {
            export_tar_file(fs, entries, out_path, format, progress)
        }
        ExportFormat::Zip => export_zip_file(fs, entries, out_path, progress, cancelled),
        ExportFormat::StuffIt | ExportFormat::MacArchive => {
            export_mac_archive_file(fs, entries, out_path, format, progress, cancelled)
        }
        _ => anyhow::bail!("{:?} is not a single-file format", format),
    }
}

fn export_tar_file(
    fs: &mut dyn Filesystem,
    entries: &[FileEntry],
    out_path: &Path,
    format: ExportFormat,
    progress: &Progress,
) -> Result<ExportSummary> {
    let compression = match format {
        ExportFormat::Tar => TarCompression::None,
        ExportFormat::TarGz => TarCompression::Gzip,
        ExportFormat::TarZstd => TarCompression::Zstd,
        _ => unreachable!(),
    };
    let out = std::fs::File::create(out_path)
        .with_context(|| format!("creating {}", out_path.display()))?;
    let opts = TarExportOptions::default();
    let stats = export_tar_multi(fs, entries, out, compression, &opts, &|s| {
        progress("", s.files as usize, s.total_bytes);
    })?;
    Ok(ExportSummary {
        files: stats.files as usize,
        bytes: stats.total_bytes,
    })
}

fn export_zip_file(
    fs: &mut dyn Filesystem,
    entries: &[FileEntry],
    out_path: &Path,
    progress: &Progress,
    cancelled: &Cancelled,
) -> Result<ExportSummary> {
    let out = std::fs::File::create(out_path)
        .with_context(|| format!("creating {}", out_path.display()))?;
    let mut zw = zip::ZipWriter::new(out);
    let opts: zip::write::FileOptions<'_, ()> =
        zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Deflated);
    let mut summary = ExportSummary::default();
    let base = common_parent(entries);
    for e in entries {
        // Each root keeps its folder relative to the common parent, so a
        // cross-folder selection cannot collide on a bare name.
        let rel = relative_dir(e, &base);
        zip_recurse(
            fs,
            std::slice::from_ref(e),
            &rel,
            &mut zw,
            opts,
            progress,
            cancelled,
            &mut summary,
        )?;
    }
    zw.finish().context("finishing zip archive")?;
    Ok(summary)
}

#[allow(clippy::too_many_arguments)]
fn zip_recurse<W: Write + std::io::Seek>(
    fs: &mut dyn Filesystem,
    entries: &[FileEntry],
    prefix: &str,
    zw: &mut zip::ZipWriter<W>,
    opts: zip::write::FileOptions<'static, ()>,
    progress: &Progress,
    cancelled: &Cancelled,
    summary: &mut ExportSummary,
) -> Result<()> {
    for e in entries {
        if cancelled() {
            return Err(cancelled_err());
        }
        // A volume root contributes its children at the current level; zip, like
        // tar, has no member name for it (its literal name is "/").
        let member = e.archive_name();
        let arch = match (prefix.is_empty(), member) {
            (_, None) => prefix.to_string(),
            (true, Some(n)) => n.to_string(),
            (false, Some(n)) => format!("{prefix}/{n}"),
        };
        match e.entry_type {
            EntryType::Directory => {
                if member.is_some() {
                    zw.add_directory(format!("{arch}/"), opts)
                        .with_context(|| format!("zip dir '{arch}'"))?;
                }
                let children = fs
                    .list_directory(e)
                    .with_context(|| format!("listing '{}'", e.name))?;
                zip_recurse(fs, &children, &arch, zw, opts, progress, cancelled, summary)?;
            }
            EntryType::File => {
                let data = read_data_fork(fs, e)?;
                zw.start_file(&arch, opts)
                    .with_context(|| format!("zip file '{arch}'"))?;
                zw.write_all(&data)?;
                // Preserve a Mac resource fork / Finder info as the AppleDouble
                // sidecar macOS' own Archive Utility writes into a zip.
                let rsrc = read_resource_fork(fs, e)?;
                if !rsrc.is_empty() || e.type_code.is_some() {
                    let ad = build_appledouble(
                        &e.type_code.unwrap_or([0; 4]),
                        &e.creator_code.unwrap_or([0; 4]),
                        mac_dates(e),
                        &rsrc,
                    );
                    let sidecar = match arch.rsplit_once('/') {
                        Some((dir, base)) => format!("__MACOSX/{dir}/._{base}"),
                        None => format!("__MACOSX/._{arch}"),
                    };
                    zw.start_file(&sidecar, opts)
                        .with_context(|| format!("zip sidecar '{sidecar}'"))?;
                    zw.write_all(&ad)?;
                }
                summary.files += 1;
                summary.bytes = summary.bytes.saturating_add(data.len() as u64);
                progress(&e.path, summary.files, summary.bytes);
            }
            EntryType::Symlink | EntryType::Special => {}
        }
    }
    Ok(())
}

/// Build a `.sit` (StuffIt) or `.mar` (MacArchive) from the selection — both use
/// the same [`StuffItInputNode`] tree, differing only in the container writer.
fn export_mac_archive_file(
    fs: &mut dyn Filesystem,
    entries: &[FileEntry],
    out_path: &Path,
    format: ExportFormat,
    progress: &Progress,
    cancelled: &Cancelled,
) -> Result<ExportSummary> {
    let mut summary = ExportSummary::default();
    let base = common_parent(entries);
    let mut nodes: Vec<StuffItInputNode> = Vec::new();
    for e in entries {
        let produced = sit_nodes(
            fs,
            std::slice::from_ref(e),
            progress,
            cancelled,
            &mut summary,
        )?;
        // Rebuild the entry's folders inside the archive, merging into any
        // folder an earlier root already created at the same path.
        let rel = relative_dir(e, &base);
        sit_insert(&mut nodes, &rel, produced);
    }
    let bytes = if format == ExportFormat::MacArchive {
        let root = out_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("archive");
        crate::macarchive::mar::build_archive(root, &nodes).context("building MacArchive (.mar)")?
    } else {
        build_archive_tree(&nodes, WriteMethod::Rle).context("building StuffIt archive")?
    };
    std::fs::write(out_path, &bytes).with_context(|| format!("writing {}", out_path.display()))?;
    Ok(summary)
}

/// Splice `produced` into `nodes` under `rel`, reusing a folder node that is
/// already there so two roots from the same source folder share one entry.
fn sit_insert(nodes: &mut Vec<StuffItInputNode>, rel: &str, produced: Vec<StuffItInputNode>) {
    let Some((head, tail)) = rel.trim_matches('/').split_once('/').or_else(|| {
        let t = rel.trim_matches('/');
        (!t.is_empty()).then_some((t, ""))
    }) else {
        nodes.extend(produced);
        return;
    };
    if let Some(StuffItInputNode::Folder { children, .. }) = nodes
        .iter_mut()
        .find(|n| matches!(n, StuffItInputNode::Folder { name, .. } if name == head))
    {
        sit_insert(children, tail, produced);
        return;
    }
    let mut children = Vec::new();
    sit_insert(&mut children, tail, produced);
    nodes.push(StuffItInputNode::Folder {
        name: head.to_string(),
        finder_flags: 0,
        create_date: 0,
        mod_date: 0,
        children,
    });
}

fn sit_nodes(
    fs: &mut dyn Filesystem,
    entries: &[FileEntry],
    progress: &Progress,
    cancelled: &Cancelled,
    summary: &mut ExportSummary,
) -> Result<Vec<StuffItInputNode>> {
    let mut nodes = Vec::new();
    for e in entries {
        if cancelled() {
            return Err(cancelled_err());
        }
        match e.entry_type {
            EntryType::Directory => {
                let children = fs
                    .list_directory(e)
                    .with_context(|| format!("listing '{}'", e.name))?;
                let (create, modify) = sit_dates(e);
                let inner = sit_nodes(fs, &children, progress, cancelled, summary)?;
                match e.archive_name() {
                    // A volume root becomes the archive's own top level rather
                    // than a folder literally named "/".
                    None => nodes.extend(inner),
                    Some(name) => nodes.push(StuffItInputNode::Folder {
                        name: name.to_string(),
                        finder_flags: e.finder_flags.unwrap_or(0),
                        create_date: create,
                        mod_date: modify,
                        children: inner,
                    }),
                }
            }
            EntryType::File => {
                let data = read_data_fork(fs, e)?;
                let rsrc = read_resource_fork(fs, e)?;
                let (create, modify) = sit_dates(e);
                summary.files += 1;
                summary.bytes = summary.bytes.saturating_add(data.len() as u64);
                progress(&e.path, summary.files, summary.bytes);
                nodes.push(StuffItInputNode::File(StuffItInput {
                    name: e.name.clone(),
                    type_code: e.type_code.unwrap_or(*b"????"),
                    creator_code: e.creator_code.unwrap_or(*b"????"),
                    finder_flags: e.finder_flags.unwrap_or(0),
                    create_date: create,
                    mod_date: modify,
                    data_fork: data,
                    resource_fork: rsrc,
                }));
            }
            EntryType::Symlink | EntryType::Special => {}
        }
    }
    Ok(nodes)
}

/// Mac create/modify seconds (1904 epoch) from an entry's dates, 0 when unknown.
fn sit_dates(e: &FileEntry) -> (u32, u32) {
    match e.mac_dates {
        Some((create, modify, _backup)) => (create, modify),
        None => (0, 0),
    }
}

fn mac_dates(e: &FileEntry) -> MacFileDates {
    let (created, modified) = sit_dates(e);
    MacFileDates { created, modified }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::filesystem::EditableFilesystem;
    use std::io::Read;

    fn put_file(efs: &mut dyn EditableFilesystem, parent: &FileEntry, name: &str, data: &[u8]) {
        let mut r: &[u8] = data;
        efs.create_file(parent, name, &mut r, data.len() as u64, &Default::default())
            .unwrap();
    }

    /// A blank FAT volume holding `HELLO.TXT`, `DATA.BIN`, and `SUB/INNER.TXT`.
    /// Returns the open (read-only) filesystem and its selected root entries.
    fn fixture() -> (Box<dyn Filesystem>, Vec<FileEntry>, tempfile::TempDir) {
        let flat = crate::fs::fat::create_blank_fat(2 * 1024 * 1024, Some("EXP")).unwrap();
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
            put_file(&mut *efs, &root, "HELLO.TXT", b"hello world");
            let data: Vec<u8> = (0u8..250).collect();
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
        let entries = fs.list_directory(&root).unwrap();
        (fs, entries, dir)
    }

    fn noprog(_: &str, _: usize, _: u64) {}
    fn never() -> bool {
        false
    }

    #[test]
    fn loose_files_written_to_folder() {
        let (mut fs, entries, _g) = fixture();
        let out = tempfile::tempdir().unwrap();
        let s = export_to_folder(
            &mut *fs,
            &entries,
            out.path(),
            ExportFormat::LooseFiles,
            ResourceForkMode::Native,
            &noprog,
            &never,
        )
        .unwrap();
        assert_eq!(s.files, 3);
        assert_eq!(
            std::fs::read(out.path().join("HELLO.TXT")).unwrap(),
            b"hello world"
        );
        assert_eq!(
            std::fs::read(out.path().join("SUB/INNER.TXT")).unwrap(),
            b"nested file"
        );
    }

    /// Exporting a *selected volume root* — what "select everything, export"
    /// produces — must lay the root's children at the top of the archive.
    ///
    /// Every driver names its root `"/"`, and taking that literally broke each
    /// format differently: tar and zip reject an absolute member outright, and
    /// the loose-file path was worse than an error — `Path::join("/")` discards
    /// the destination and yields the **host** root, so the export would have
    /// aimed at `/`.
    #[test]
    fn a_selected_volume_root_exports_its_children_in_every_format() {
        let (mut fs, _entries, _g) = fixture();
        let root = fs.root().unwrap();
        assert_eq!(root.name, "/", "fixture assumption: roots are named '/'");
        let sel = std::slice::from_ref(&root);
        let out = tempfile::tempdir().unwrap();

        for (fmt, ext) in [
            (ExportFormat::Tar, "tar"),
            (ExportFormat::TarGz, "tar.gz"),
            (ExportFormat::Zip, "zip"),
            (ExportFormat::MacArchive, "mar"),
            (ExportFormat::StuffIt, "sit"),
        ] {
            let path = out.path().join(format!("whole.{ext}"));
            let s = export_to_file(&mut *fs, sel, &path, fmt, &noprog, &never)
                .unwrap_or_else(|e| panic!("{fmt:?} failed on a selected root: {e}"));
            assert_eq!(s.files, 3, "{fmt:?} should archive all three files");
        }

        // Loose files: the contents land in the chosen folder, and nothing is
        // created at the host root.
        let loose = tempfile::tempdir().unwrap();
        let s = export_to_folder(
            &mut *fs,
            sel,
            loose.path(),
            ExportFormat::LooseFiles,
            ResourceForkMode::Native,
            &noprog,
            &never,
        )
        .expect("loose export of a selected root");
        assert_eq!(s.files, 3);
        assert_eq!(
            std::fs::read(loose.path().join("HELLO.TXT")).unwrap(),
            b"hello world"
        );
        assert_eq!(
            std::fs::read(loose.path().join("SUB/INNER.TXT")).unwrap(),
            b"nested file"
        );
    }

    #[test]
    fn export_nested_directory_all_formats() {
        // folder-in-folder: SUB/SUBSUB/DEEP.TXT. Every other test only exercises
        // one directory level; real folders nest, so exercise a marked folder
        // that contains a subfolder across every single-file archive format.
        let flat = crate::fs::fat::create_blank_fat(2 * 1024 * 1024, Some("EXP")).unwrap();
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
            let sub = efs
                .create_directory(&root, "SUB", &Default::default())
                .unwrap();
            let subsub = efs
                .create_directory(&sub, "SUBSUB", &Default::default())
                .unwrap();
            put_file(&mut *efs, &subsub, "DEEP.TXT", b"deep");
            efs.sync_metadata().unwrap();
        }
        let file = std::fs::File::open(&img).unwrap();
        let mut fs = crate::fs::open_filesystem(file, 0, 0, None).unwrap();
        let root = fs.root().unwrap();
        let sub = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "SUB")
            .expect("SUB");
        let out = tempfile::tempdir().unwrap();
        for (fmt, ext) in [
            (ExportFormat::MacArchive, "mar"),
            (ExportFormat::StuffIt, "sit"),
            (ExportFormat::Zip, "zip"),
            (ExportFormat::Tar, "tar"),
        ] {
            let path = out.path().join(format!("nested.{ext}"));
            let s = export_to_file(
                &mut *fs,
                std::slice::from_ref(&sub),
                &path,
                fmt,
                &noprog,
                &never,
            )
            .unwrap_or_else(|e| panic!("{fmt:?} failed on a nested folder: {e}"));
            assert_eq!(s.files, 1, "{fmt:?} should archive DEEP.TXT");
        }
    }

    #[test]
    fn export_lone_directory_via_fresh_fs_recurses() {
        // Mirrors the GUI: a directory entry is captured from the live browse
        // fs, then export opens a *fresh* fs instance (take_or_open_fs) and hands
        // it that captured entry. Exporting the folder alone must still recurse.
        let (_fs, entries, g) = fixture();
        let sub = entries
            .iter()
            .find(|e| e.name == "SUB")
            .cloned()
            .expect("SUB dir");
        assert!(sub.is_directory());

        let file = std::fs::File::open(g.path().join("v.img")).unwrap();
        let mut fs2 = crate::fs::open_filesystem(file, 0, 0, None).unwrap();
        let out = tempfile::tempdir().unwrap();
        let path = out.path().join("folder.mar");
        let s = export_to_file(
            &mut *fs2,
            &[sub],
            &path,
            ExportFormat::MacArchive,
            &noprog,
            &never,
        )
        .unwrap();
        assert_eq!(s.files, 1, "the folder's INNER.TXT should be archived");
    }

    #[test]
    fn gzip_per_file_round_trips() {
        let (mut fs, entries, _g) = fixture();
        let out = tempfile::tempdir().unwrap();
        export_to_folder(
            &mut *fs,
            &entries,
            out.path(),
            ExportFormat::GzipPerFile,
            ResourceForkMode::Native,
            &noprog,
            &never,
        )
        .unwrap();
        let gz = std::fs::read(out.path().join("HELLO.TXT.gz")).unwrap();
        let mut dec = flate2::read::GzDecoder::new(&gz[..]);
        let mut got = Vec::new();
        dec.read_to_end(&mut got).unwrap();
        assert_eq!(got, b"hello world");
    }

    #[test]
    fn tar_gz_contains_all_files() {
        let (mut fs, entries, _g) = fixture();
        let out = tempfile::tempdir().unwrap();
        let path = out.path().join("out.tar.gz");
        let s = export_to_file(
            &mut *fs,
            &entries,
            &path,
            ExportFormat::TarGz,
            &noprog,
            &never,
        )
        .unwrap();
        assert_eq!(s.files, 3);
        let f = std::fs::File::open(&path).unwrap();
        let dec = flate2::read::GzDecoder::new(f);
        let mut ar = tar::Archive::new(dec);
        let names: Vec<String> = ar
            .entries()
            .unwrap()
            .map(|e| e.unwrap().path().unwrap().to_string_lossy().into_owned())
            .collect();
        assert!(names.iter().any(|n| n == "HELLO.TXT"), "{names:?}");
        assert!(names.iter().any(|n| n == "SUB/INNER.TXT"), "{names:?}");
    }

    #[test]
    fn zip_contains_all_files() {
        let (mut fs, entries, _g) = fixture();
        let out = tempfile::tempdir().unwrap();
        let path = out.path().join("out.zip");
        export_to_file(
            &mut *fs,
            &entries,
            &path,
            ExportFormat::Zip,
            &noprog,
            &never,
        )
        .unwrap();
        let f = std::fs::File::open(&path).unwrap();
        let mut zip = zip::ZipArchive::new(f).unwrap();
        let mut hello = zip.by_name("HELLO.TXT").unwrap();
        let mut got = Vec::new();
        hello.read_to_end(&mut got).unwrap();
        assert_eq!(got, b"hello world");
        drop(hello);
        assert!(zip.by_name("SUB/INNER.TXT").is_ok());
    }

    #[test]
    fn stuffit_round_trips_through_parser() {
        let (mut fs, entries, _g) = fixture();
        let out = tempfile::tempdir().unwrap();
        let path = out.path().join("out.sit");
        let s = export_to_file(
            &mut *fs,
            &entries,
            &path,
            ExportFormat::StuffIt,
            &noprog,
            &never,
        )
        .unwrap();
        assert_eq!(s.files, 3);
        let bytes = std::fs::read(&path).unwrap();
        let archive = crate::macarchive::stuffit::parse(&bytes).unwrap();
        let hello = archive
            .entries
            .iter()
            .find(|e| e.name == "HELLO.TXT")
            .expect("HELLO.TXT in sit");
        let data =
            crate::macarchive::stuffit::decompress_fork(&bytes, hello.data.as_ref().unwrap())
                .unwrap();
        assert_eq!(data, b"hello world");
    }

    #[test]
    fn binhex_per_file_round_trips() {
        let (mut fs, entries, _g) = fixture();
        let out = tempfile::tempdir().unwrap();
        export_to_folder(
            &mut *fs,
            &entries,
            out.path(),
            ExportFormat::BinHex,
            ResourceForkMode::Native,
            &noprog,
            &never,
        )
        .unwrap();
        let hqx = std::fs::read(out.path().join("HELLO.TXT.hqx")).unwrap();
        let bh = crate::fs::binhex::parse_binhex(&hqx).unwrap();
        assert_eq!(bh.data_fork, b"hello world");
    }

    #[test]
    fn mac_archive_round_trips_through_parser() {
        let (mut fs, entries, _g) = fixture();
        let out = tempfile::tempdir().unwrap();
        let path = out.path().join("out.mar");
        let s = export_to_file(
            &mut *fs,
            &entries,
            &path,
            ExportFormat::MacArchive,
            &noprog,
            &never,
        )
        .unwrap();
        assert_eq!(s.files, 3);
        let raw = std::fs::read(&path).unwrap();
        let (bytes, archive) = crate::macarchive::mar::parse(&raw).unwrap();
        let hello = archive
            .entries
            .iter()
            .find(|e| e.name == "HELLO.TXT")
            .expect("HELLO.TXT in mar");
        let data =
            crate::macarchive::stuffit::decompress_fork(&bytes, hello.data.as_ref().unwrap())
                .unwrap();
        assert_eq!(data, b"hello world");
    }

    #[test]
    fn cancel_stops_export() {
        let (mut fs, entries, _g) = fixture();
        let out = tempfile::tempdir().unwrap();
        let err = export_to_folder(
            &mut *fs,
            &entries,
            out.path(),
            ExportFormat::LooseFiles,
            ResourceForkMode::Native,
            &noprog,
            &|| true,
        )
        .unwrap_err();
        assert!(err.to_string().contains("cancelled"), "{err}");
    }
}

#[cfg(test)]
mod selection_path_tests {
    use super::*;
    use crate::fs::entry::EntryType;

    fn f(path: &str) -> FileEntry {
        let mut e = FileEntry::root();
        e.name = path.rsplit('/').next().unwrap_or(path).to_string();
        e.path = path.to_string();
        e.entry_type = EntryType::File;
        e
    }

    #[test]
    fn one_folder_selection_keeps_bare_names() {
        // The common case, and the behaviour that must not change.
        let v = vec![f("/docs/a.txt"), f("/docs/b.txt")];
        let base = common_parent(&v);
        assert_eq!(base, "/docs");
        assert_eq!(relative_dir(&v[0], &base), "");
        assert_eq!(relative_dir(&v[1], &base), "");
    }

    #[test]
    fn cross_folder_selection_keeps_the_folders_apart() {
        // The bug: both were named "notes.txt" and one overwrote the other.
        let v = vec![f("/docs/notes.txt"), f("/backup/notes.txt")];
        let base = common_parent(&v);
        assert_eq!(base, "");
        assert_eq!(relative_dir(&v[0], &base), "docs");
        assert_eq!(relative_dir(&v[1], &base), "backup");
    }

    #[test]
    fn common_parent_is_the_deepest_shared_folder() {
        let v = vec![f("/a/b/x.txt"), f("/a/c/y.txt")];
        let base = common_parent(&v);
        assert_eq!(base, "/a");
        assert_eq!(relative_dir(&v[0], &base), "b");
        assert_eq!(relative_dir(&v[1], &base), "c");
    }

    #[test]
    fn mixed_depth_under_one_root() {
        let v = vec![f("/a/x.txt"), f("/a/b/y.txt")];
        let base = common_parent(&v);
        assert_eq!(base, "/a");
        assert_eq!(relative_dir(&v[0], &base), "");
        assert_eq!(relative_dir(&v[1], &base), "b");
    }

    #[test]
    fn volume_root_files_stay_flat() {
        let v = vec![f("/x.txt"), f("/y.txt")];
        let base = common_parent(&v);
        assert_eq!(base, "");
        assert_eq!(relative_dir(&v[0], &base), "");
    }

    #[test]
    fn empty_selection_is_not_a_panic() {
        assert_eq!(common_parent(&[]), "");
    }
}
