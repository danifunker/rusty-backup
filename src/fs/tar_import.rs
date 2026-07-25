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

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, Path};

use anyhow::{anyhow, bail, Context, Result};

use crate::fs::attrs::AttrOverrides;
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
    /// Apply each entry's archived Unix mode and ownership (uid/gid).
    /// Filesystems that don't store them ignore the values.
    ///
    /// When off, entries inherit uid/gid from the directory they land in
    /// and take the filesystem's default mode — the precedence in
    /// [`crate::fs::attrs`], not a blanket `root:root 0644`.
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

/// Cheap content sniff: does `path` look like a tar archive — plain, gzip-, or
/// zstd-compressed? Only a small prefix is (de)compressed to check for the tar
/// `ustar` magic at offset 257, so a gzip *disk image* (`.adz` / `.hdz`) is
/// **not** mistaken for a tarball just because it's gzip. Used by the GUI to
/// auto-route a dropped/added tar archive into the import flow.
pub fn looks_like_tar_archive(path: &Path) -> bool {
    let Ok(mut f) = File::open(path) else {
        return false;
    };
    let mut magic = [0u8; 4];
    let n = f.read(&mut magic).unwrap_or(0);
    if f.seek(SeekFrom::Start(0)).is_err() {
        return false;
    }
    // Need to see at least the first tar header (512 B) -> read ~600 of the
    // (decompressed) stream so offset 257..262 is covered.
    let prefix = if n >= 2 && magic[0] == 0x1f && magic[1] == 0x8b {
        read_prefix(flate2::read::GzDecoder::new(f), 600)
    } else if n >= 4 && magic == [0x28, 0xb5, 0x2f, 0xfd] {
        match crate::rbformats::zstd_compat::decoder(f) {
            Ok(d) => read_prefix(d, 600),
            Err(_) => return false,
        }
    } else {
        read_prefix(f, 600)
    };
    // ustar magic: "ustar\0" (POSIX) or "ustar  " (GNU) — both start "ustar".
    prefix.len() >= 262 && &prefix[257..262] == b"ustar"
}

fn read_prefix(mut r: impl Read, n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    let mut filled = 0;
    while filled < n {
        match r.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(k) => filled += k,
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        }
    }
    buf.truncate(filled);
    buf
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
        let dec = crate::rbformats::zstd_compat::decoder(file).context("init zstd decoder")?;
        import_tar(efs, dest, dec, opts, progress)
    } else {
        import_tar(efs, dest, file, opts, progress)
    }
}

/// Import an (already-decompressed, or plain) tar stream into `dest`.
///
/// Brackets the work in [`EditableFilesystem::begin_bulk`] /
/// [`end_bulk`](EditableFilesystem::end_bulk). The importer aborts on the first
/// hard error (every create is `?`-propagated) and callers discard the volume
/// on `Err`, so per-operation rollback is redundant here; bulk mode lets HFS
/// skip cloning its whole catalog on every entry. `end_bulk` runs even on
/// error so the filesystem is never left stuck in bulk mode.
pub fn import_tar<R: Read>(
    efs: &mut dyn EditableFilesystem,
    dest: &FileEntry,
    archive: R,
    opts: &TarImportOptions,
    progress: &dyn Fn(&TarImportStats),
) -> Result<TarImportStats> {
    efs.begin_bulk();
    let result = import_tar_inner(efs, dest, archive, opts, progress);
    efs.end_bulk();
    result
}

fn import_tar_inner<R: Read>(
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
    // image dir path -> set of child names known to exist, so the per-entry
    // conflict check is O(1) instead of listing the (growing) directory every
    // time — otherwise a large single-directory import is O(n^2). Seeded lazily
    // from the on-disk listing the first time we touch a directory.
    let mut dir_children: HashMap<String, HashSet<String>> = HashMap::new();

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
        // What the archive says this entry's mode and ownership should be.
        // Empty when `apply_permissions` is off, in which case the shared
        // resolver falls back to the replaced entry / parent directory —
        // the same precedence `rb-cli put` uses.
        let overrides = archived_overrides(entry.header(), opts.apply_permissions);

        if etype.is_dir() {
            // A directory can already exist here either because the image
            // had it or because an earlier entry auto-created it as an
            // implicit parent. Either way its own archive entry is the
            // authority on its mode, so stamp it after the fact.
            let existed = dir_cache.contains_key(&comps.join("/"));
            let dir = ensure_dir(efs, &mut dir_cache, &comps, &mut stats, &overrides)?;
            if existed {
                apply_attrs_after_create(efs, &dir, &overrides, &mut stats)?;
            } else if !overrides.is_empty() {
                stats.perms_applied += 1;
            }
            progress(&stats);
            continue;
        }

        let (parent_comps, leaf) = comps.split_at(comps.len() - 1);
        let name = &leaf[0];
        let parent = ensure_dir(
            efs,
            &mut dir_cache,
            parent_comps,
            &mut stats,
            &AttrOverrides::default(),
        )?;

        // Conflict handling. Seed this directory's existing-names set once
        // (from the on-disk listing — empty for a directory we just created),
        // then consult/update it per entry so the check is O(1).
        let parent_key = parent.path.clone();
        // Both populated only when this entry overwrites an existing one.
        let mut inherited_xattrs = Vec::new();
        let mut replaced: Option<FileEntry> = None;
        if !dir_children.contains_key(&parent_key) {
            let existing: HashSet<String> = efs
                .list_directory(&parent)
                .map_err(|e| anyhow!("list_directory {}: {e}", parent.path))?
                .into_iter()
                .map(|c| c.name)
                .collect();
            dir_children.insert(parent_key.clone(), existing);
        }
        if dir_children[&parent_key].contains(name) {
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
                    if let Some(existing) = find_child(efs, &parent, name)? {
                        // Before the delete: a replacement inherits the
                        // extended attributes of what it displaces, the same
                        // way it inherits mode and ownership.
                        inherited_xattrs = crate::fs::attrs::inherited_xattrs(
                            efs.as_filesystem_mut(),
                            Some(&existing),
                        );
                        // Kept for the same reason: with `--no-permissions`
                        // the replacement inherits the displaced file's mode
                        // and ownership rather than dropping to a default.
                        replaced = Some(existing.clone());
                        efs.delete_entry(&parent, &existing)
                            .map_err(|e| anyhow!("overwrite delete {}: {e}", raw_path.display()))?;
                    }
                    dir_children
                        .get_mut(&parent_key)
                        .expect("seeded above")
                        .remove(name);
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
            // A symlink's own mode is nearly always 0777 and rarely load-
            // bearing, but its ownership is — and the drivers OR in their
            // own S_IFLNK, so bare permission bits are what they want here.
            let attrs =
                crate::fs::attrs::resolve_attrs(&overrides, None, Some(&parent), None, 0o777);
            let link_opts = crate::fs::filesystem::CreateFileOptions {
                mode: Some(attrs.mode & 0o7777),
                uid: Some(attrs.uid),
                gid: Some(attrs.gid),
                ..Default::default()
            };
            match efs.create_symlink(&parent, name, &target, &link_opts) {
                Ok(_) => {
                    stats.symlinks += 1;
                    dir_children
                        .get_mut(&parent_key)
                        .expect("seeded above")
                        .insert(name.clone());
                }
                Err(ref e) if is_unsupported(e) => stats.symlinks_skipped += 1,
                Err(e) => return Err(anyhow!("create_symlink {}: {e}", raw_path.display())),
            }
            progress(&stats);
            continue;
        }

        if etype.is_file() {
            let size = entry.size();
            // Mode and ownership go in through `create_file`, not a chmod
            // afterwards: every driver that stores them honours these
            // fields, while `set_permissions` is implemented by only two,
            // so the old post-create chmod silently dropped the archive's
            // mode on EFS, UFS, Minix and the rest.
            let attrs = crate::fs::attrs::resolve_attrs(
                &overrides,
                replaced.as_ref(),
                Some(&parent),
                None,
                0o644,
            );
            let create_opts = crate::fs::filesystem::CreateFileOptions {
                mode: Some(attrs.file_mode()),
                uid: Some(attrs.uid),
                gid: Some(attrs.gid),
                xattrs: inherited_xattrs,
                ..Default::default()
            };
            efs.create_file(&parent, name, &mut entry, size, &create_opts)
                .map_err(|e| anyhow!("create_file {}: {e}", raw_path.display()))?;
            dir_children
                .get_mut(&parent_key)
                .expect("seeded above")
                .insert(name.clone());
            stats.files += 1;
            stats.total_bytes += size;
            if !overrides.is_empty() {
                stats.perms_applied += 1;
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
                // Deliberately about the *driver*, not the format: several
                // filesystems we can read symlinks from we cannot yet write
                // them to, and claiming the format can't hold them sends the
                // user looking for a problem that isn't there.
                "{} symlink(s) will be DROPPED - writing symbolic links is not \
                 supported for this filesystem.",
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
            crate::rbformats::zstd_compat::decoder(file).context("init zstd decoder")?,
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

/// The mode / ownership an archived entry asks for, or nothing when the
/// caller turned that off (`--no-permissions`), in which case the shared
/// resolver falls back to the replaced entry then the parent directory.
///
/// `tar_export` writes all three fields on every header it emits, so
/// reading only `mode` here — and only for files — is what made a
/// `tar` -> `untar` round-trip lose ownership outright and directory
/// modes with it.
fn archived_overrides(header: &tar::Header, apply: bool) -> crate::fs::attrs::AttrOverrides {
    if !apply {
        return crate::fs::attrs::AttrOverrides::default();
    }
    crate::fs::attrs::AttrOverrides {
        mode: header.mode().ok().map(|m| m & 0o7777),
        uid: header.uid().ok().map(|v| v as u32),
        gid: header.gid().ok().map(|v| v as u32),
    }
}

/// Stamp mode / ownership onto an entry that already existed, so it can't
/// be set at creation time. Only the filesystems implementing these two
/// hooks can honour it; others report `Unsupported` and are left alone.
fn apply_attrs_after_create(
    efs: &mut dyn EditableFilesystem,
    entry: &FileEntry,
    overrides: &AttrOverrides,
    stats: &mut TarImportStats,
) -> Result<()> {
    let mut applied = false;
    if let Some(m) = overrides.mode {
        match efs.set_permissions(entry, m) {
            Ok(()) => applied = true,
            Err(ref e) if is_unsupported(e) => {}
            Err(e) => return Err(anyhow!("set_permissions {}: {e}", entry.path)),
        }
    }
    if let (Some(u), Some(g)) = (overrides.uid, overrides.gid) {
        match efs.set_owner(entry, u, g) {
            Ok(()) => applied = true,
            Err(ref e) if is_unsupported(e) => {}
            Err(e) => return Err(anyhow!("set_owner {}: {e}", entry.path)),
        }
    }
    if applied {
        stats.perms_applied += 1;
    }
    Ok(())
}

/// Ensure every directory named by `comps` exists under the import root,
/// creating missing ones (mkdir -p). Returns the deepest directory's entry.
///
/// `leaf_overrides` applies to the LAST component only — that is the one
/// the archive has an entry for. Intermediate components are implicit
/// parents the archive never described, so they take the resolver's
/// inherit-from-parent default.
fn ensure_dir(
    efs: &mut dyn EditableFilesystem,
    cache: &mut HashMap<String, FileEntry>,
    comps: &[String],
    stats: &mut TarImportStats,
    leaf_overrides: &AttrOverrides,
) -> Result<FileEntry> {
    let mut key = String::new();
    let mut parent = cache.get("").expect("root cached").clone();
    let last = comps.len().saturating_sub(1);
    for (i, comp) in comps.iter().enumerate() {
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
                let overrides = if i == last {
                    *leaf_overrides
                } else {
                    AttrOverrides::default()
                };
                let attrs = crate::fs::attrs::resolve_dir_attrs(&overrides, None, Some(&parent));
                let dir_opts = crate::fs::filesystem::CreateDirectoryOptions {
                    mode: Some(attrs.dir_mode()),
                    uid: Some(attrs.uid),
                    gid: Some(attrs.gid),
                    ..Default::default()
                };
                let e = efs
                    .create_directory(&parent, comp, &dir_opts)
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
    fn looks_like_tar_detects_tar_and_rejects_gzip_image() {
        use std::io::Write as _;
        let dir = tempfile::tempdir().unwrap();

        // A real .tar.gz built by exporting a tiny FAT volume.
        let img = dir.path().join("s.img");
        std::fs::write(
            &img,
            crate::fs::fat::create_blank_fat(2 * 1024 * 1024, Some("S")).unwrap(),
        )
        .unwrap();
        {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&img)
                .unwrap();
            let mut efs = crate::fs::open_editable_filesystem(f, 0, 0, None).unwrap();
            let root = efs.root().unwrap();
            put_file(&mut *efs, &root, "A.TXT", b"hi");
            efs.sync_metadata().unwrap();
        }
        let tgz = dir.path().join("a.tar.gz");
        {
            let f = std::fs::File::open(&img).unwrap();
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
        assert!(
            looks_like_tar_archive(&tgz),
            "real .tar.gz should be detected"
        );

        // A plain (uncompressed) tar.
        let plain = dir.path().join("a.tar");
        {
            let f = std::fs::File::open(&img).unwrap();
            let mut fs = crate::fs::open_filesystem(f, 0, 0, None).unwrap();
            let root = fs.root().unwrap();
            let out = std::fs::File::create(&plain).unwrap();
            export_tar(
                &mut *fs,
                &root,
                "",
                out,
                TarCompression::None,
                &TarExportOptions::default(),
                &|_| {},
            )
            .unwrap();
        }
        assert!(
            looks_like_tar_archive(&plain),
            "plain .tar should be detected"
        );

        // A gzip stream that is NOT a tar (stand-in for a .adz disk image) —
        // must NOT be mistaken for a tarball.
        let adz = dir.path().join("disk.adz");
        {
            let f = std::fs::File::create(&adz).unwrap();
            let mut enc = flate2::write::GzEncoder::new(f, flate2::Compression::default());
            enc.write_all(&vec![0xE9u8; 4096]).unwrap(); // boot-sector-ish bytes
            enc.finish().unwrap();
        }
        assert!(
            !looks_like_tar_archive(&adz),
            "gzip disk image must not look like tar"
        );
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

    /// Build a tar in memory carrying explicit modes and ownership on a
    /// file, an executable, and a directory.
    fn tar_with_permissions() -> Vec<u8> {
        let mut b = tar::Builder::new(Vec::new());

        let mut dh = tar::Header::new_gnu();
        dh.set_entry_type(tar::EntryType::Directory);
        dh.set_size(0);
        dh.set_mode(0o700);
        dh.set_uid(1000);
        dh.set_gid(100);
        dh.set_cksum();
        b.append_data(&mut dh, "private/", std::io::empty())
            .unwrap();

        let mut fh = tar::Header::new_gnu();
        fh.set_entry_type(tar::EntryType::Regular);
        fh.set_size(6);
        fh.set_mode(0o600);
        fh.set_uid(1000);
        fh.set_gid(100);
        fh.set_cksum();
        b.append_data(&mut fh, "private/key.txt", &b"secret"[..])
            .unwrap();

        let mut xh = tar::Header::new_gnu();
        xh.set_entry_type(tar::EntryType::Regular);
        xh.set_size(3);
        xh.set_mode(0o755);
        xh.set_uid(1000);
        xh.set_gid(100);
        xh.set_cksum();
        b.append_data(&mut xh, "run.sh", &b"#!\n"[..]).unwrap();

        b.into_inner().unwrap()
    }

    /// An import has to carry the archive's mode AND ownership onto files
    /// AND directories.
    ///
    /// It used to read only `mode`, apply it only to files, and only via
    /// `set_permissions` — a hook just two drivers implement — so on every
    /// other Unix filesystem an executable came back 0644, a 0600 secret
    /// came back world-readable, directory modes were dropped everywhere,
    /// and uid/gid were never read from the header at all. `tar_export`
    /// writes all three fields, so a `tar` -> `untar` round-trip lost them.
    #[test]
    fn import_applies_archived_mode_and_ownership() {
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("ext.img");
        std::fs::write(
            &img,
            crate::fs::ext_format::create_blank_ext2(16 * 1024 * 1024, "PERMS").unwrap(),
        )
        .unwrap();
        let archive = dir.path().join("perms.tar");
        std::fs::write(&archive, tar_with_permissions()).unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&img)
            .unwrap();
        let mut efs = crate::fs::open_editable_filesystem(f, 0, 0, None).unwrap();
        let root = efs.root().unwrap();
        import_tar_from_path(
            &mut *efs,
            &root,
            &archive,
            &TarImportOptions::default(),
            &|_| {},
        )
        .unwrap();
        efs.sync_metadata().unwrap();

        let find =
            |efs: &mut dyn EditableFilesystem, parent: &FileEntry, name: &str| -> FileEntry {
                efs.list_directory(parent)
                    .unwrap()
                    .into_iter()
                    .find(|e| e.name == name)
                    .unwrap_or_else(|| panic!("{name} missing after import"))
            };

        let root = efs.root().unwrap();
        let private = find(&mut *efs, &root, "private");
        assert_eq!(
            private.mode.map(|m| m & 0o7777),
            Some(0o700),
            "directory mode came from the driver default, not the archive"
        );
        assert_eq!((private.uid, private.gid), (Some(1000), Some(100)));

        let run = find(&mut *efs, &root, "run.sh");
        assert_eq!(
            run.mode.map(|m| m & 0o7777),
            Some(0o755),
            "executable bit did not survive the import"
        );
        assert_eq!((run.uid, run.gid), (Some(1000), Some(100)));

        let key = find(&mut *efs, &private, "key.txt");
        assert_eq!(
            key.mode.map(|m| m & 0o7777),
            Some(0o600),
            "a 0600 file must not come back more permissive than the archive"
        );
        assert_eq!((key.uid, key.gid), (Some(1000), Some(100)));
    }

    /// `--no-permissions` means "ignore what the archive says", not "fall
    /// back to root:root 0644": new entries inherit ownership from the
    /// directory they land in, the same rule `rb-cli put` follows.
    #[test]
    fn no_permissions_inherits_from_the_parent_directory() {
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("ext.img");
        std::fs::write(
            &img,
            crate::fs::ext_format::create_blank_ext2(16 * 1024 * 1024, "PERMS").unwrap(),
        )
        .unwrap();
        let archive = dir.path().join("perms.tar");
        std::fs::write(&archive, tar_with_permissions()).unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&img)
            .unwrap();
        let mut efs = crate::fs::open_editable_filesystem(f, 0, 0, None).unwrap();
        let root = efs.root().unwrap();
        // Import into a directory with a distinctive owner to inherit.
        // (Built through `create_directory` rather than `set_owner`, which
        // only SquashFS implements today.)
        efs.create_directory(
            &root,
            "dest",
            &crate::fs::filesystem::CreateDirectoryOptions {
                mode: Some(0o40755),
                uid: Some(42),
                gid: Some(43),
                ..Default::default()
            },
        )
        .unwrap();
        // Re-read it the way a caller would, so it carries its owner.
        let dest = efs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "dest")
            .unwrap();
        assert_eq!((dest.uid, dest.gid), (Some(42), Some(43)));

        let opts = TarImportOptions {
            apply_permissions: false,
            ..Default::default()
        };
        let stats = import_tar_from_path(&mut *efs, &dest, &archive, &opts, &|_| {}).unwrap();
        assert_eq!(stats.perms_applied, 0, "nothing archived should be applied");
        efs.sync_metadata().unwrap();

        let run = efs
            .list_directory(&dest)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "run.sh")
            .expect("run.sh missing");
        assert_ne!(
            run.mode.map(|m| m & 0o7777),
            Some(0o755),
            "the archive's mode should have been ignored"
        );
        assert_eq!(
            (run.uid, run.gid),
            (Some(42), Some(43)),
            "ownership should come from the parent directory"
        );
    }
}
