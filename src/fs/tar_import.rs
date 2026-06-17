//! Import a `.tar.gz` / `.tar.zst` / `.tar` archive's contents INTO a disk
//! image's filesystem — the inverse of [`crate::fs::tar_export`].
//!
//! Reads the archive (compression auto-detected from magic), recreates the
//! directory tree, streams each file into the target via the
//! [`EditableFilesystem`] API, and recreates symlinks where the target FS
//! supports them. Filesystems that can't store a symlink or a Unix mode are
//! handled gracefully — those entries are skipped/ignored and counted, so a
//! `tar -> FAT` import doesn't abort on the first symlink. (Symmetric with
//! the export, which reports dropped resource forks.)
//!
//! The caller owns the sync/commit lifecycle: like every other
//! `EditableFilesystem` mutation, callers MUST call `sync_metadata()` (and,
//! for a container, `commit`) after import returns.

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, Path};

use anyhow::{anyhow, bail, Context, Result};

use crate::fs::entry::FileEntry;
use crate::fs::filesystem::{EditableFilesystem, FilesystemError};

/// What to do when an entry's destination name already exists in the image.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImportConflict {
    /// Abort the import (default).
    Error,
    /// Delete the existing entry and write the archived one.
    Overwrite,
    /// Leave the existing entry; skip the archived one.
    Skip,
}

/// Knobs for [`import_tar`].
pub struct TarImportOptions {
    pub conflict: ImportConflict,
    /// Best-effort: apply each entry's Unix mode via `set_permissions`
    /// (silently ignored on filesystems that don't support it).
    pub apply_permissions: bool,
    /// Skip macOS AppleDouble sidecars (`._*`) — resource-fork/metadata cruft
    /// a Mac adds to archives. On by default; almost never wanted inside a
    /// disk image.
    pub skip_appledouble: bool,
}

impl Default for TarImportOptions {
    fn default() -> Self {
        Self {
            conflict: ImportConflict::Error,
            apply_permissions: true,
            skip_appledouble: true,
        }
    }
}

/// True when `name` is a macOS AppleDouble sidecar (`._something`).
fn is_appledouble(name: &str) -> bool {
    name.starts_with("._")
}

/// Tally of what the import produced.
#[derive(Default, Debug, Clone)]
pub struct TarImportStats {
    pub files: u64,
    pub dirs_created: u64,
    pub symlinks: u64,
    /// Symlinks the target filesystem can't store (skipped).
    pub symlinks_skipped: u64,
    pub skipped_existing: u64,
    pub overwritten: u64,
    pub perms_applied: u64,
    /// macOS AppleDouble (`._*`) sidecars skipped.
    pub appledouble_skipped: u64,
    /// Entries whose name the target filesystem can't store (e.g. a
    /// trailing-dot name on FAT).
    pub invalid_names_skipped: u64,
    /// Entries we don't represent (hardlinks, devices, fifos, …).
    pub other_skipped: u64,
    pub total_bytes: u64,
}

fn is_unsupported(e: &FilesystemError) -> bool {
    matches!(e, FilesystemError::Unsupported(_))
}

/// Import from a host archive path, auto-detecting gzip / zstd / plain tar
/// from the leading magic bytes.
pub fn import_tar_from_path(
    efs: &mut dyn EditableFilesystem,
    dest: &FileEntry,
    path: &Path,
    opts: &TarImportOptions,
    progress: &dyn Fn(&TarImportStats),
) -> Result<TarImportStats> {
    let mut file =
        File::open(path).with_context(|| format!("opening archive {}", path.display()))?;
    let mut magic = [0u8; 4];
    let n = file.read(&mut magic).unwrap_or(0);
    file.seek(SeekFrom::Start(0)).context("rewind archive")?;

    if n >= 2 && magic[0] == 0x1f && magic[1] == 0x8b {
        let dec = flate2::read::GzDecoder::new(file);
        import_tar(efs, dest, dec, opts, progress)
    } else if n >= 4 && magic == [0x28, 0xb5, 0x2f, 0xfd] {
        let dec = zstd::Decoder::new(file).context("init zstd decoder")?;
        import_tar(efs, dest, dec, opts, progress)
    } else {
        import_tar(efs, dest, file, opts, progress)
    }
}

/// Import an (already-decompressed, or plain) tar stream into `dest`.
pub fn import_tar<R: Read>(
    efs: &mut dyn EditableFilesystem,
    dest: &FileEntry,
    archive: R,
    opts: &TarImportOptions,
    progress: &dyn Fn(&TarImportStats),
) -> Result<TarImportStats> {
    let mut stats = TarImportStats::default();
    let mut ar = tar::Archive::new(archive);
    // archive-relative dir path -> the image FileEntry for that dir.
    let mut dir_cache: HashMap<String, FileEntry> = HashMap::new();
    dir_cache.insert(String::new(), dest.clone());

    for entry in ar.entries().context("reading tar entries")? {
        let mut entry = entry.context("reading tar entry")?;
        let raw_path = entry.path().context("entry path")?.into_owned();
        let comps = match safe_components(&raw_path) {
            Some(c) if !c.is_empty() => c,
            // Skip empty paths and anything with `..` / absolute roots.
            _ => continue,
        };
        // Drop macOS AppleDouble sidecars (`._*`) first — they're Mac
        // resource-fork/metadata cruft, almost never wanted in the image.
        if opts.skip_appledouble && comps.last().map(|c| is_appledouble(c)).unwrap_or(false) {
            stats.appledouble_skipped += 1;
            progress(&stats);
            continue;
        }

        // Skip (don't abort on) any path component the target filesystem
        // can't store — e.g. a trailing-dot name on FAT.
        if comps.iter().any(|c| efs.validate_name(c).is_err()) {
            stats.invalid_names_skipped += 1;
            progress(&stats);
            continue;
        }

        let etype = entry.header().entry_type();
        let mode = entry.header().mode().ok().map(|m| m & 0o7777);

        if etype.is_dir() {
            ensure_dir(efs, &mut dir_cache, &comps, &mut stats)?;
            progress(&stats);
            continue;
        }

        let (parent_comps, leaf) = comps.split_at(comps.len() - 1);
        let name = &leaf[0];
        let parent = ensure_dir(efs, &mut dir_cache, parent_comps, &mut stats)?;

        // Conflict handling.
        if let Some(existing) = find_child(efs, &parent, name)? {
            match opts.conflict {
                ImportConflict::Error => bail!(
                    "{} already exists in the image (pass --force or --skip-existing)",
                    raw_path.display()
                ),
                ImportConflict::Skip => {
                    stats.skipped_existing += 1;
                    progress(&stats);
                    continue;
                }
                ImportConflict::Overwrite => {
                    efs.delete_entry(&parent, &existing)
                        .map_err(|e| anyhow!("overwrite delete {}: {e}", raw_path.display()))?;
                    stats.overwritten += 1;
                }
            }
        }

        if etype.is_symlink() {
            let target = entry
                .link_name()
                .ok()
                .flatten()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default();
            match efs.create_symlink(&parent, name, &target, &Default::default()) {
                Ok(_) => stats.symlinks += 1,
                Err(ref e) if is_unsupported(e) => stats.symlinks_skipped += 1,
                Err(e) => return Err(anyhow!("create_symlink {}: {e}", raw_path.display())),
            }
            progress(&stats);
            continue;
        }

        if etype.is_file() {
            let size = entry.size();
            let new_entry = efs
                .create_file(&parent, name, &mut entry, size, &Default::default())
                .map_err(|e| anyhow!("create_file {}: {e}", raw_path.display()))?;
            stats.files += 1;
            stats.total_bytes += size;
            if opts.apply_permissions {
                if let Some(m) = mode {
                    match efs.set_permissions(&new_entry, m) {
                        Ok(()) => stats.perms_applied += 1,
                        Err(ref e) if is_unsupported(e) => {}
                        Err(e) => {
                            return Err(anyhow!("set_permissions {}: {e}", raw_path.display()))
                        }
                    }
                }
            }
            progress(&stats);
            continue;
        }

        // Hardlinks, char/block devices, fifos, sockets — not representable.
        stats.other_skipped += 1;
        progress(&stats);
    }
    Ok(stats)
}

/// Read-only scan of an archive against a target filesystem, computing what
/// *would* be skipped or dropped — without writing anything. The GUI uses
/// this to warn (and prompt) before a potentially-lossy import; the CLI
/// skips the prompt and just imports.
#[derive(Default, Debug, Clone)]
pub struct TarImportPreflight {
    pub files: u64,
    pub dirs: u64,
    pub symlinks: u64,
    /// AppleDouble (`._*`) entries that will be skipped.
    pub appledouble: u64,
    /// Entries whose name the target filesystem can't store (will be skipped).
    pub invalid_names: u64,
    /// Symlinks that will be DROPPED because the target FS can't store them.
    pub symlinks_dropped: u64,
    /// Hardlinks / devices / fifos that aren't representable (skipped).
    pub other_unsupported: u64,
}

impl TarImportPreflight {
    /// True when the import will skip or drop something the user might care
    /// about — i.e. the GUI should confirm before proceeding.
    pub fn has_warnings(&self) -> bool {
        self.symlinks_dropped > 0 || self.invalid_names > 0 || self.other_unsupported > 0
    }

    /// Human-readable warning lines (ASCII only). Empty when lossless.
    pub fn warnings(&self) -> Vec<String> {
        let mut w = Vec::new();
        if self.symlinks_dropped > 0 {
            w.push(format!(
                "{} symlink(s) will be DROPPED - this filesystem can't store symbolic links.",
                self.symlinks_dropped
            ));
        }
        if self.invalid_names > 0 {
            w.push(format!(
                "{} entr(ies) have names this filesystem can't store and will be skipped.",
                self.invalid_names
            ));
        }
        if self.other_unsupported > 0 {
            w.push(format!(
                "{} entr(ies) (hardlinks / devices) aren't representable and will be skipped.",
                self.other_unsupported
            ));
        }
        w
    }
}

/// Preflight an archive on the host against `efs`, auto-detecting compression.
pub fn preflight_tar_from_path(
    efs: &dyn EditableFilesystem,
    path: &Path,
    opts: &TarImportOptions,
) -> Result<TarImportPreflight> {
    let mut file =
        File::open(path).with_context(|| format!("opening archive {}", path.display()))?;
    let mut magic = [0u8; 4];
    let n = file.read(&mut magic).unwrap_or(0);
    file.seek(SeekFrom::Start(0)).context("rewind archive")?;

    if n >= 2 && magic[0] == 0x1f && magic[1] == 0x8b {
        preflight_tar(efs, flate2::read::GzDecoder::new(file), opts)
    } else if n >= 4 && magic == [0x28, 0xb5, 0x2f, 0xfd] {
        preflight_tar(
            efs,
            zstd::Decoder::new(file).context("init zstd decoder")?,
            opts,
        )
    } else {
        preflight_tar(efs, file, opts)
    }
}

/// Read-only preflight scan of a tar stream. Mirrors [`import_tar`]'s
/// classification but performs no mutations.
pub fn preflight_tar<R: Read>(
    efs: &dyn EditableFilesystem,
    archive: R,
    opts: &TarImportOptions,
) -> Result<TarImportPreflight> {
    let supports_symlinks = efs.supports_symlinks();
    let mut pf = TarImportPreflight::default();
    let mut ar = tar::Archive::new(archive);
    for entry in ar.entries().context("reading tar entries")? {
        let entry = entry.context("reading tar entry")?;
        let raw = entry.path().context("entry path")?.into_owned();
        let comps = match safe_components(&raw) {
            Some(c) if !c.is_empty() => c,
            _ => continue,
        };
        if opts.skip_appledouble && comps.last().map(|c| is_appledouble(c)).unwrap_or(false) {
            pf.appledouble += 1;
            continue;
        }
        let name_invalid = comps.iter().any(|c| efs.validate_name(c).is_err());
        let etype = entry.header().entry_type();
        if etype.is_dir() {
            pf.dirs += 1;
            if name_invalid {
                pf.invalid_names += 1;
            }
        } else if name_invalid {
            pf.invalid_names += 1;
        } else if etype.is_symlink() {
            pf.symlinks += 1;
            if !supports_symlinks {
                pf.symlinks_dropped += 1;
            }
        } else if etype.is_file() {
            pf.files += 1;
        } else {
            pf.other_unsupported += 1;
        }
    }
    Ok(pf)
}

/// Return the `Normal` path components as strings, or `None` if the path is
/// absolute or contains a `..` component (tar path-traversal guard).
fn safe_components(p: &Path) -> Option<Vec<String>> {
    let mut out = Vec::new();
    for c in p.components() {
        match c {
            Component::Normal(s) => out.push(s.to_string_lossy().into_owned()),
            Component::CurDir => {}
            // RootDir, ParentDir, Prefix -> reject (escape attempt).
            _ => return None,
        }
    }
    Some(out)
}

/// Ensure every directory named by `comps` exists under the import root,
/// creating missing ones (mkdir -p). Returns the deepest directory's entry.
fn ensure_dir(
    efs: &mut dyn EditableFilesystem,
    cache: &mut HashMap<String, FileEntry>,
    comps: &[String],
    stats: &mut TarImportStats,
) -> Result<FileEntry> {
    let mut key = String::new();
    let mut parent = cache.get("").expect("root cached").clone();
    for comp in comps {
        let next_key = if key.is_empty() {
            comp.clone()
        } else {
            format!("{key}/{comp}")
        };
        if let Some(e) = cache.get(&next_key) {
            parent = e.clone();
            key = next_key;
            continue;
        }
        let entry = match find_child(efs, &parent, comp)? {
            Some(e) if e.is_directory() => e,
            Some(_) => bail!("path component {comp:?} exists but is not a directory"),
            None => {
                let e = efs
                    .create_directory(&parent, comp, &Default::default())
                    .map_err(|err| anyhow!("create_directory {comp:?}: {err}"))?;
                stats.dirs_created += 1;
                e
            }
        };
        cache.insert(next_key.clone(), entry.clone());
        parent = entry;
        key = next_key;
    }
    Ok(parent)
}

fn find_child(
    efs: &mut dyn EditableFilesystem,
    parent: &FileEntry,
    name: &str,
) -> Result<Option<FileEntry>> {
    let children = efs
        .list_directory(parent)
        .map_err(|e| anyhow!("list_directory {}: {e}", parent.path))?;
    Ok(children.into_iter().find(|c| c.name == name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::tar_export::{export_tar, TarCompression, TarExportOptions};

    fn put_file(efs: &mut dyn EditableFilesystem, parent: &FileEntry, name: &str, data: &[u8]) {
        let mut r: &[u8] = data;
        efs.create_file(parent, name, &mut r, data.len() as u64, &Default::default())
            .unwrap();
    }

    /// Round-trip: build a populated FAT volume, export it to a .tar.gz, then
    /// import that archive into a fresh blank FAT volume and confirm the tree
    /// + contents survive.
    #[test]
    fn round_trip_export_then_import() {
        let dir = tempfile::tempdir().unwrap();

        // Source volume.
        let src_img = dir.path().join("src.img");
        std::fs::write(
            &src_img,
            crate::fs::fat::create_blank_fat(4 * 1024 * 1024, Some("SRC")).unwrap(),
        )
        .unwrap();
        {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&src_img)
                .unwrap();
            let mut efs = crate::fs::open_editable_filesystem(f, 0, 0, None).unwrap();
            let root = efs.root().unwrap();
            put_file(&mut *efs, &root, "HELLO.TXT", b"hello world");
            let payload: Vec<u8> = (0u8..255).collect();
            put_file(&mut *efs, &root, "DATA.BIN", &payload);
            let sub = efs
                .create_directory(&root, "SUB", &Default::default())
                .unwrap();
            put_file(&mut *efs, &sub, "INNER.TXT", b"nested file");
            efs.sync_metadata().unwrap();
        }

        // Export to a .tar.gz.
        let tgz = dir.path().join("out.tar.gz");
        {
            let f = std::fs::File::open(&src_img).unwrap();
            let mut fs = crate::fs::open_filesystem(f, 0, 0, None).unwrap();
            let root = fs.root().unwrap();
            let out = std::fs::File::create(&tgz).unwrap();
            export_tar(
                &mut *fs,
                &root,
                "",
                out,
                TarCompression::Gzip,
                &TarExportOptions::default(),
                &|_| {},
            )
            .unwrap();
        }

        // Fresh blank target volume; import the archive into it.
        let dst_img = dir.path().join("dst.img");
        std::fs::write(
            &dst_img,
            crate::fs::fat::create_blank_fat(4 * 1024 * 1024, Some("DST")).unwrap(),
        )
        .unwrap();
        let stats = {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&dst_img)
                .unwrap();
            let mut efs = crate::fs::open_editable_filesystem(f, 0, 0, None).unwrap();
            let root = efs.root().unwrap();
            let stats = import_tar_from_path(
                &mut *efs,
                &root,
                &tgz,
                &TarImportOptions::default(),
                &|_| {},
            )
            .unwrap();
            efs.sync_metadata().unwrap();
            stats
        };
        assert_eq!(stats.files, 3, "stats: {stats:?}");
        assert_eq!(stats.dirs_created, 1);

        // Verify the target volume's contents.
        let f = std::fs::File::open(&dst_img).unwrap();
        let mut fs = crate::fs::open_filesystem(f, 0, 0, None).unwrap();
        let root = fs.root().unwrap();
        let names: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(names.iter().any(|n| n == "HELLO.TXT"), "names: {names:?}");
        assert!(names.iter().any(|n| n == "SUB"), "names: {names:?}");

        let hello = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "HELLO.TXT")
            .unwrap();
        let mut buf = Vec::new();
        fs.write_file_to(&hello, &mut buf).unwrap();
        assert_eq!(buf, b"hello world");

        let sub = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "SUB")
            .unwrap();
        let inner = fs
            .list_directory(&sub)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "INNER.TXT")
            .unwrap();
        let mut buf2 = Vec::new();
        fs.write_file_to(&inner, &mut buf2).unwrap();
        assert_eq!(buf2, b"nested file");
    }

    #[test]
    fn skip_existing_does_not_error() {
        let dir = tempfile::tempdir().unwrap();
        // Build a one-file tar.gz in memory by exporting a tiny volume.
        let src = dir.path().join("s.img");
        std::fs::write(
            &src,
            crate::fs::fat::create_blank_fat(2 * 1024 * 1024, Some("S")).unwrap(),
        )
        .unwrap();
        {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&src)
                .unwrap();
            let mut efs = crate::fs::open_editable_filesystem(f, 0, 0, None).unwrap();
            let root = efs.root().unwrap();
            put_file(&mut *efs, &root, "A.TXT", b"one");
            efs.sync_metadata().unwrap();
        }
        let tgz = dir.path().join("a.tar.gz");
        {
            let f = std::fs::File::open(&src).unwrap();
            let mut fs = crate::fs::open_filesystem(f, 0, 0, None).unwrap();
            let root = fs.root().unwrap();
            let out = std::fs::File::create(&tgz).unwrap();
            export_tar(
                &mut *fs,
                &root,
                "",
                out,
                TarCompression::Gzip,
                &TarExportOptions::default(),
                &|_| {},
            )
            .unwrap();
        }
        // Target already containing A.TXT -> skip mode imports nothing new.
        let dst = dir.path().join("d.img");
        std::fs::write(
            &dst,
            crate::fs::fat::create_blank_fat(2 * 1024 * 1024, Some("D")).unwrap(),
        )
        .unwrap();
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&dst)
            .unwrap();
        let mut efs = crate::fs::open_editable_filesystem(f, 0, 0, None).unwrap();
        let root = efs.root().unwrap();
        put_file(&mut *efs, &root, "A.TXT", b"preexisting");
        let opts = TarImportOptions {
            conflict: ImportConflict::Skip,
            ..Default::default()
        };
        let stats = import_tar_from_path(&mut *efs, &root, &tgz, &opts, &|_| {}).unwrap();
        assert_eq!(stats.skipped_existing, 1, "stats: {stats:?}");
        assert_eq!(stats.files, 0);
    }
}
