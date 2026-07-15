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
    /// A single `.zip`; Mac resource forks ride along as `__MACOSX/._name`
    /// AppleDouble members (the convention macOS itself uses).
    Zip,
    /// A single classic StuffIt `.sit`; forks + Finder type/creator native.
    StuffIt,
}

impl ExportFormat {
    /// Every format, in menu order (folder outputs first, then archives).
    pub const ALL: [ExportFormat; 8] = [
        ExportFormat::LooseFiles,
        ExportFormat::GzipPerFile,
        ExportFormat::ZstdPerFile,
        ExportFormat::Tar,
        ExportFormat::TarGz,
        ExportFormat::TarZstd,
        ExportFormat::Zip,
        ExportFormat::StuffIt,
    ];

    /// Short menu / dropdown label.
    pub fn label(self) -> &'static str {
        match self {
            ExportFormat::LooseFiles => "Loose files",
            ExportFormat::GzipPerFile => "Gzip each file (.gz)",
            ExportFormat::ZstdPerFile => "Zstd each file (.zst)",
            ExportFormat::Tar => "Tar (.tar)",
            ExportFormat::TarGz => "Tar + gzip (.tar.gz)",
            ExportFormat::TarZstd => "Tar + zstd (.tar.zst)",
            ExportFormat::Zip => "Zip (.zip)",
            ExportFormat::StuffIt => "StuffIt (.sit)",
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
    folder_recurse(
        fs,
        entries,
        dest_dir,
        format,
        fork_mode,
        progress,
        cancelled,
        &mut summary,
    )?;
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
                let sub = dest_dir.join(&e.name);
                std::fs::create_dir_all(&sub)
                    .with_context(|| format!("creating {}", sub.display()))?;
                let children = fs
                    .list_directory(e)
                    .with_context(|| format!("listing '{}'", e.name))?;
                folder_recurse(
                    fs, &children, &sub, format, fork_mode, progress, cancelled, summary,
                )?;
            }
            EntryType::File => {
                match format {
                    ExportFormat::LooseFiles => {
                        export_file_with_fork(fs, e, dest_dir, &safe_name(e), fork_mode)
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
fn read_resource_fork(fs: &mut dyn Filesystem, e: &FileEntry) -> Vec<u8> {
    let mut buf = Vec::new();
    let _ = fs.write_resource_fork_to(e, &mut buf);
    buf
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
        ExportFormat::StuffIt => export_sit_file(fs, entries, out_path, progress, cancelled),
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
    zip_recurse(
        fs,
        entries,
        "",
        &mut zw,
        opts,
        progress,
        cancelled,
        &mut summary,
    )?;
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
        let arch = if prefix.is_empty() {
            e.name.clone()
        } else {
            format!("{prefix}/{}", e.name)
        };
        match e.entry_type {
            EntryType::Directory => {
                zw.add_directory(format!("{arch}/"), opts)
                    .with_context(|| format!("zip dir '{arch}'"))?;
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
                let rsrc = read_resource_fork(fs, e);
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

fn export_sit_file(
    fs: &mut dyn Filesystem,
    entries: &[FileEntry],
    out_path: &Path,
    progress: &Progress,
    cancelled: &Cancelled,
) -> Result<ExportSummary> {
    let mut summary = ExportSummary::default();
    let nodes = sit_nodes(fs, entries, progress, cancelled, &mut summary)?;
    let bytes = build_archive_tree(&nodes, WriteMethod::Rle).context("building StuffIt archive")?;
    std::fs::write(out_path, &bytes).with_context(|| format!("writing {}", out_path.display()))?;
    Ok(summary)
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
                nodes.push(StuffItInputNode::Folder {
                    name: e.name.clone(),
                    finder_flags: e.finder_flags.unwrap_or(0),
                    create_date: create,
                    mod_date: modify,
                    children: inner,
                });
            }
            EntryType::File => {
                let data = read_data_fork(fs, e)?;
                let rsrc = read_resource_fork(fs, e);
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
