//! Archives at the edge of a browsed volume.
//!
//! What a host pick or drop is (a tar, a Mac archive, or a plain item); a Mac archive decoded into
//! host files to stage (expanded to AppleDouble, or only its BinHex wrapper stripped); a decoded
//! archive extracted to a host folder; and a tar imported straight into the volume (a preflight
//! first, which asks when the volume cannot keep everything). The browser pane runs these as jobs.

use std::path::{Path, PathBuf};

use arc_mac::detect::{detect_mac_archive, MacArchiveKind};
use arc_mac::extract::{self as mac_extract, ForkFormat};
use arc_mac::stuffit::StuffItArchive;
use disk_core::entry::FileEntry;
use fs_tar::tar_import::{self, ImportConflict, TarImportOptions, TarImportStats};

use crate::model::browse_session::BrowseSession;
use crate::model::file_types::MAC_ARCHIVE_EXTS;
use crate::model::job::JobCtx;

/// What a host file or folder picked or dropped onto a volume is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostPick {
    /// Staged as it is.
    Plain(PathBuf),
    /// A tar: its contents can be imported instead.
    Tar(PathBuf),
    /// A Mac archive (only looked for on a Mac volume), with the bytes read to detect it.
    Mac {
        path: PathBuf,
        kind: MacArchiveKind,
        raw: Vec<u8>,
    },
}

/// Classify one pick; anything unreadable is plain, so the staging reports it.
pub fn classify(path: &Path, mac_volume: bool) -> HostPick {
    let plain = || HostPick::Plain(path.to_path_buf());
    if !path.is_file() {
        return plain();
    }
    if tar_import::looks_like_tar_archive(path) {
        return HostPick::Tar(path.to_path_buf());
    }
    if !mac_volume {
        return plain();
    }
    // Detection needs the whole file; past 64 MiB only a Mac-archive extension earns that read.
    let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    let named = path
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| MAC_ARCHIVE_EXTS.iter().any(|a| a.eq_ignore_ascii_case(e)));
    if size > fs_tar::mem_archive::INLINE_MAX && !named {
        return plain();
    }
    let Ok(raw) = std::fs::read(path) else {
        return plain();
    };
    match detect_mac_archive(&raw) {
        Some(kind) => HostPick::Mac {
            path: path.to_path_buf(),
            kind,
            raw,
        },
        None => plain(),
    }
}

/// Decode a Mac archive into `dir` as AppleDouble (forks and type kept); returns what to stage.
pub fn expand_mac_archive(raw: Vec<u8>, dir: &Path) -> Result<Vec<PathBuf>, String> {
    let (bytes, archive) = mac_extract::open_bytes(raw).map_err(|e| format!("decode: {e}"))?;
    std::fs::create_dir_all(dir).map_err(|e| format!("create folder: {e}"))?;
    let format = ForkFormat::AppleDouble;
    mac_extract::extract_all(&bytes, &archive, dir, format, |_, _, _| {}, |_| {})
        .map_err(|e| format!("extract: {e}"))?;
    // The `._name` sidecars ride along: staging pairs them with their file.
    let children = std::fs::read_dir(dir).map_err(|e| format!("read folder: {e}"))?;
    Ok(children.filter_map(|r| r.ok()).map(|e| e.path()).collect())
}

/// Strip a BinHex wrapper into `dir`; returns the inner archive's path.
pub fn unwrap_binhex(raw: &[u8], dir: &Path) -> Result<PathBuf, String> {
    let bh = fs_mac::binhex::parse_binhex(raw).map_err(|e| format!("decode BinHex: {e}"))?;
    std::fs::create_dir_all(dir).map_err(|e| format!("create folder: {e}"))?;
    let name = match bh.name.is_empty() {
        true => "archive.sit".to_string(),
        false => bh.name.replace(['/', '\\'], "_"),
    };
    let inner = dir.join(name);
    std::fs::write(&inner, &bh.data_fork).map_err(|e| format!("write inner archive: {e}"))?;
    Ok(inner)
}

/// One entry of an archive listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveRow {
    pub path: String,
    pub is_dir: bool,
    /// `TYPE / CRTR`.
    pub type_creator: String,
    /// The data fork's size, else the resource fork's.
    pub size: u64,
    pub method: &'static str,
}

/// The archive's entries, in order.
pub fn archive_rows(archive: &StuffItArchive) -> Vec<ArchiveRow> {
    let row = |e: &arc_mac::stuffit::StuffItEntry| {
        let (size, method) = e
            .data
            .as_ref()
            .filter(|f| f.uncompressed_len > 0)
            .or(e.rsrc.as_ref())
            .map(|f| (u64::from(f.uncompressed_len), f.method_name()))
            .unwrap_or((0, "None"));
        ArchiveRow {
            path: e.display_path(),
            is_dir: e.is_dir,
            type_creator: format!(
                "{} / {}",
                String::from_utf8_lossy(&e.type_code),
                String::from_utf8_lossy(&e.creator_code)
            ),
            size,
            method,
        }
    };
    archive.entries.iter().map(row).collect()
}

/// Extract a decoded archive (every entry, or those `keep` marks) into `dest`; returns a status line.
pub fn extract_to_host(
    bytes: &[u8],
    archive: &StuffItArchive,
    dest: &Path,
    format: ForkFormat,
    keep: Option<&[bool]>,
) -> Result<String, String> {
    let keep = |i: usize| match keep {
        Some(k) => k.get(i).copied().unwrap_or(true),
        None => true,
    };
    let stats =
        mac_extract::extract_filtered(bytes, archive, dest, format, keep, |_, _, _| {}, |_| {})
            .map_err(|e| format!("Extract failed: {e}"))?;
    let skipped = match stats.skipped {
        0 => String::new(),
        n => format!(" ({n} skipped)"),
    };
    Ok(format!(
        "Extracted {} file(s) to {} as {}{skipped}",
        stats.files,
        dest.display(),
        format.label()
    ))
}

/// What a tar import's preflight found that the volume cannot keep.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TarPreview {
    pub archive: PathBuf,
    pub warnings: Vec<String>,
    /// macOS `._*` sidecars that are skipped.
    pub sidecars: u64,
    pub files: u64,
    pub dirs: u64,
}

/// How a tar-import job ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TarStep {
    /// The preflight found content that would be skipped or dropped; ask before importing.
    Confirm(TarPreview),
    /// The status line, and whether anything was written.
    Finished(String, bool),
}

/// Import `archive` into `dest` (existing names skipped), after a preflight when `check`.
pub fn tar_import_step(
    session: &BrowseSession,
    dest: &FileEntry,
    archive: PathBuf,
    check: bool,
    job: &JobCtx,
) -> TarStep {
    let opts = TarImportOptions {
        conflict: ImportConflict::Skip,
        ..Default::default()
    };
    if check {
        job.set_phase("Checking the archive...");
        let (efs, _commit) = match session.open_editable() {
            Ok(pair) => pair,
            Err(e) => return TarStep::Finished(format!("Error opening filesystem: {e}"), false),
        };
        let pf = match tar_import::preflight_tar_from_path(&*efs, &archive, &opts) {
            Ok(pf) => pf,
            Err(e) => return TarStep::Finished(format!("Could not read archive: {e:#}"), false),
        };
        if pf.has_warnings() {
            return TarStep::Confirm(TarPreview {
                warnings: pf.warnings(),
                sidecars: pf.appledouble,
                files: pf.files,
                dirs: pf.dirs,
                archive,
            });
        }
    }
    job.set_phase("Importing...");
    let (mut efs, commit) = match session.open_editable() {
        Ok(pair) => pair,
        Err(e) => return TarStep::Finished(format!("Error opening filesystem: {e}"), false),
    };
    let report = |st: &TarImportStats| {
        job.set_phase(format!(
            "Importing... {} file(s), {} folder(s)",
            st.files, st.dirs_created
        ));
    };
    let stats = match tar_import::import_tar_from_path(&mut *efs, dest, &archive, &opts, &report) {
        Ok(s) => s,
        Err(e) => return TarStep::Finished(format!("Import failed: {e:#}"), false),
    };
    if let Err(e) = efs.sync_metadata() {
        return TarStep::Finished(format!("Error saving to disk: {e}"), false);
    }
    drop(efs);
    if let Err(e) = commit.commit() {
        return TarStep::Finished(format!("Error writing container: {e}"), false);
    }
    TarStep::Finished(import_summary(&stats, dest), true)
}

fn import_summary(stats: &TarImportStats, dest: &FileEntry) -> String {
    let into = if dest.path == "/" { "" } else { &dest.name };
    let mut msg = format!(
        "Imported {} files, {} dirs, {} symlinks into /{into}",
        stats.files, stats.dirs_created, stats.symlinks
    );
    let notes = [
        (stats.symlinks_skipped, "symlink(s) skipped"),
        (stats.appledouble_skipped, "._ sidecar(s) skipped"),
        (
            stats.appledouble_paired,
            "._ sidecar(s) joined to their file",
        ),
        (stats.invalid_names_skipped, "bad-name entr(ies) skipped"),
        (stats.skipped_existing, "already existed"),
    ];
    for (n, what) in notes {
        if n > 0 {
            msg.push_str(&format!("; {n} {what}"));
        }
    }
    msg
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A BinHex document of one file named `name` holding `data`.
    pub(crate) fn hqx(name: &str, data: &[u8]) -> String {
        fs_mac::binhex::build_binhex(&fs_mac::binhex::BinHexFile {
            name: name.to_string(),
            type_code: *b"TEXT",
            creator_code: *b"ttxt",
            flags: 0,
            data_fork: data.to_vec(),
            resource_fork: Vec::new(),
        })
    }

    #[test]
    fn a_plain_file_and_a_folder_classify_as_plain() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("a.txt");
        std::fs::write(&f, b"hello").unwrap();
        assert_eq!(classify(&f, true), HostPick::Plain(f.clone()));
        assert_eq!(
            classify(dir.path(), true),
            HostPick::Plain(dir.path().to_path_buf())
        );
    }

    #[test]
    fn a_binhex_file_is_a_mac_archive_only_on_a_mac_volume() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("x.hqx");
        std::fs::write(&f, hqx("X", b"data")).unwrap();
        assert!(matches!(classify(&f, true), HostPick::Mac { .. }));
        assert_eq!(classify(&f, false), HostPick::Plain(f.clone()));
        let inner = unwrap_binhex(&std::fs::read(&f).unwrap(), &dir.path().join("u")).unwrap();
        assert_eq!(std::fs::read(inner).unwrap(), b"data");
    }
}
