//! The shared write half of every "import a tree into a disk image" flow.
//!
//! [`crate::fs::tar_import`] and [`crate::fs::dir_import`] differ only in where
//! their entries come from — a tar stream versus a host directory walk. Every
//! decision *after* that point is identical: path-traversal guarding, mkdir -p
//! of implicit parents, the existing-name conflict policy, xattr/mode/ownership
//! inheritance when a replacement displaces a file, and which failures are
//! "skip and count" rather than "abort".
//!
//! That logic is fiddly and easy to let drift, so it lives here once and both
//! drivers push into it. A source driver's whole job is to classify each entry
//! into an [`ImportItem`] and hand it to [`Importer::push`].
//!
//! The caller owns the sync/commit lifecycle: like every other
//! [`EditableFilesystem`] mutation, callers MUST call `sync_metadata()` (and,
//! for a container, `commit`) after an import returns.

use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::{Component, Path};

use anyhow::{anyhow, bail, Result};

use crate::fs::attrs::AttrOverrides;
use crate::fs::entry::FileEntry;
use crate::fs::filesystem::{EditableFilesystem, FilesystemError};

/// What to do when an entry's destination name already exists in the image.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImportConflict {
    /// Abort the import (default).
    Error,
    /// Delete the existing entry and write the incoming one.
    Overwrite,
    /// Leave the existing entry; skip the incoming one.
    Skip,
}

/// Knobs shared by every importer.
pub struct ImportOptions {
    pub conflict: ImportConflict,
    /// Apply each entry's source Unix mode and ownership (uid/gid).
    /// Filesystems that don't store them ignore the values.
    ///
    /// When off, entries inherit uid/gid from the directory they land in
    /// and take the filesystem's default mode — the precedence in
    /// [`crate::fs::attrs`], not a blanket `root:root 0644`.
    pub apply_permissions: bool,
    /// Skip macOS AppleDouble sidecars (`._*`) — resource-fork/metadata cruft
    /// a Mac adds to trees and archives. On by default; almost never wanted
    /// inside a disk image.
    pub skip_appledouble: bool,
}

impl Default for ImportOptions {
    fn default() -> Self {
        Self {
            conflict: ImportConflict::Error,
            apply_permissions: true,
            skip_appledouble: true,
        }
    }
}

/// Tally of what an import produced.
#[derive(Default, Debug, Clone)]
pub struct ImportStats {
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
    /// Archives unpacked into the image instead of copied in verbatim
    /// (`--expand-archives`). Their contents are counted in the fields above.
    pub archives_expanded: u64,
}

impl ImportStats {
    /// Fold a nested import's tally into this one. Used when an archive is
    /// expanded mid-walk: its contents belong in the parent import's totals.
    pub fn merge(&mut self, other: &ImportStats) {
        self.files += other.files;
        self.dirs_created += other.dirs_created;
        self.symlinks += other.symlinks;
        self.symlinks_skipped += other.symlinks_skipped;
        self.skipped_existing += other.skipped_existing;
        self.overwritten += other.overwritten;
        self.perms_applied += other.perms_applied;
        self.appledouble_skipped += other.appledouble_skipped;
        self.invalid_names_skipped += other.invalid_names_skipped;
        self.other_skipped += other.other_skipped;
        self.total_bytes += other.total_bytes;
        self.archives_expanded += other.archives_expanded;
    }
}

/// Read-only projection of what an import *would* do, without writing.
/// The GUI uses this to warn (and prompt) before a potentially-lossy
/// import; the CLI generally skips the prompt and just imports.
#[derive(Default, Debug, Clone)]
pub struct ImportPreflight {
    pub files: u64,
    pub dirs: u64,
    pub symlinks: u64,
    /// AppleDouble (`._*`) entries that will be skipped.
    pub appledouble: u64,
    /// Entries whose names the target filesystem can't store (will be skipped).
    pub invalid_names: u64,
    /// Symlinks that will be DROPPED because the target FS can't store them.
    pub symlinks_dropped: u64,
    /// Hardlinks / devices / fifos that aren't representable (skipped).
    pub other_unsupported: u64,
    /// Total bytes of regular-file content the import will write. Drivers
    /// that can cheaply stat their source fill this in; it is what sizing a
    /// fresh volume to fit the content keys off.
    pub total_bytes: u64,
}

impl ImportPreflight {
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

/// One thing to write, already classified by the source driver.
pub enum ImportItem<'a> {
    Dir,
    Symlink {
        target: String,
    },
    File {
        size: u64,
        data: &'a mut dyn Read,
        /// A Mac resource fork + Finder info the source driver resolved from
        /// the host containers, for targets that can store one. `data` is
        /// already the *data* fork, so a whole-file wrapper is unwrapped by the
        /// driver rather than here (see [`crate::fs::dir_import`]).
        mac_fork: Option<&'a crate::fs::resource_fork::ImportedResourceFork>,
    },
    /// Hardlinks, char/block devices, fifos, sockets — not representable.
    Unsupported,
}

/// True when `name` is a macOS AppleDouble sidecar (`._something`).
pub fn is_appledouble(name: &str) -> bool {
    name.starts_with("._")
}

pub fn is_unsupported(e: &FilesystemError) -> bool {
    matches!(e, FilesystemError::Unsupported(_))
}

/// Return the `Normal` path components as strings, or `None` if the path is
/// absolute or contains a `..` component (path-traversal guard).
pub fn safe_components(p: &Path) -> Option<Vec<String>> {
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

/// Drives writes into a target filesystem, holding the caches that keep a
/// large import from going quadratic.
pub struct Importer {
    /// Source-relative dir path -> the image `FileEntry` for that dir.
    dir_cache: HashMap<String, FileEntry>,
    /// Image dir path -> set of child names known to exist, so the per-entry
    /// conflict check is O(1) instead of listing the (growing) directory every
    /// time. Seeded lazily from the on-disk listing the first time we touch a
    /// directory.
    dir_children: HashMap<String, HashSet<String>>,
    pub stats: ImportStats,
}

impl Importer {
    pub fn new(dest: &FileEntry) -> Self {
        let mut dir_cache = HashMap::new();
        dir_cache.insert(String::new(), dest.clone());
        Self {
            dir_cache,
            dir_children: HashMap::new(),
            stats: ImportStats::default(),
        }
    }

    /// Write one classified entry. `comps` is the source-relative path split
    /// into components; `display` is used only in error messages.
    ///
    /// Entries the target can't represent are counted and skipped rather than
    /// aborting the run — a `tar -> FAT` import shouldn't die on the first
    /// symlink. Genuine I/O and filesystem errors still propagate.
    pub fn push(
        &mut self,
        efs: &mut dyn EditableFilesystem,
        comps: &[String],
        item: ImportItem<'_>,
        overrides: &AttrOverrides,
        opts: &ImportOptions,
        display: &str,
    ) -> Result<()> {
        if comps.is_empty() {
            return Ok(());
        }
        // Drop macOS AppleDouble sidecars (`._*`) first — Mac resource-fork
        // cruft, almost never wanted in the image.
        if opts.skip_appledouble && comps.last().map(|c| is_appledouble(c)).unwrap_or(false) {
            self.stats.appledouble_skipped += 1;
            return Ok(());
        }
        // Skip (don't abort on) any path component the target filesystem
        // can't store — e.g. a trailing-dot name on FAT.
        if comps.iter().any(|c| efs.validate_name(c).is_err()) {
            self.stats.invalid_names_skipped += 1;
            return Ok(());
        }

        if let ImportItem::Dir = item {
            // A directory can already exist here either because the image had
            // it or because an earlier entry auto-created it as an implicit
            // parent. Either way its own source entry is the authority on its
            // mode, so stamp it after the fact.
            let existed = self.dir_cache.contains_key(&comps.join("/"));
            let dir = self.ensure_dir(efs, comps, overrides)?;
            if existed {
                let mut applied = false;
                apply_attrs_after_create(efs, &dir, overrides, &mut applied)?;
                if applied {
                    self.stats.perms_applied += 1;
                }
            } else if !overrides.is_empty() {
                self.stats.perms_applied += 1;
            }
            return Ok(());
        }

        let (parent_comps, leaf) = comps.split_at(comps.len() - 1);
        let name = &leaf[0];
        let parent = self.ensure_dir(efs, parent_comps, &AttrOverrides::default())?;

        // Conflict handling. Both of these are populated only when this entry
        // overwrites an existing one.
        let parent_key = parent.path.clone();
        let mut inherited_xattrs = Vec::new();
        let mut replaced: Option<FileEntry> = None;
        if !self.dir_children.contains_key(&parent_key) {
            let existing: HashSet<String> = efs
                .list_directory(&parent)
                .map_err(|e| anyhow!("list_directory {}: {e}", parent.path))?
                .into_iter()
                .map(|c| c.name)
                .collect();
            self.dir_children.insert(parent_key.clone(), existing);
        }
        if self.dir_children[&parent_key].contains(name) {
            match opts.conflict {
                ImportConflict::Error => {
                    bail!("{display} already exists in the image (pass --force or --skip-existing)")
                }
                ImportConflict::Skip => {
                    self.stats.skipped_existing += 1;
                    return Ok(());
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
                            .map_err(|e| anyhow!("overwrite delete {display}: {e}"))?;
                    }
                    self.dir_children
                        .get_mut(&parent_key)
                        .expect("seeded above")
                        .remove(name);
                    self.stats.overwritten += 1;
                }
            }
        }

        match item {
            ImportItem::Dir => unreachable!("handled above"),
            ImportItem::Symlink { target } => {
                // A symlink's own mode is nearly always 0777 and rarely load-
                // bearing, but its ownership is — and the drivers OR in their
                // own S_IFLNK, so bare permission bits are what they want here.
                let attrs =
                    crate::fs::attrs::resolve_attrs(overrides, None, Some(&parent), None, 0o777);
                let link_opts = crate::fs::filesystem::CreateFileOptions {
                    mode: Some(attrs.mode & 0o7777),
                    uid: Some(attrs.uid),
                    gid: Some(attrs.gid),
                    unix_times: overrides.unix_times,
                    ..Default::default()
                };
                match efs.create_symlink(&parent, name, &target, &link_opts) {
                    Ok(_) => {
                        self.stats.symlinks += 1;
                        self.dir_children
                            .get_mut(&parent_key)
                            .expect("seeded above")
                            .insert(name.clone());
                    }
                    Err(ref e) if is_unsupported(e) => self.stats.symlinks_skipped += 1,
                    Err(e) => return Err(anyhow!("create_symlink {display}: {e}")),
                }
            }
            ImportItem::File {
                size,
                data,
                mac_fork,
            } => {
                // Mode and ownership go in through `create_file`, not a chmod
                // afterwards: every driver that stores them honours these
                // fields, while `set_permissions` is implemented by only two,
                // so a post-create chmod silently drops the mode on EFS, UFS,
                // Minix and the rest.
                let attrs = crate::fs::attrs::resolve_attrs(
                    overrides,
                    replaced.as_ref(),
                    Some(&parent),
                    None,
                    0o644,
                );
                let mut create_opts = crate::fs::filesystem::CreateFileOptions {
                    mode: Some(attrs.file_mode()),
                    uid: Some(attrs.uid),
                    gid: Some(attrs.gid),
                    xattrs: inherited_xattrs,
                    // Preserve source mtime end-to-end (host stat / tar Header
                    // / stage_copy source inode); the driver falls back to now
                    // when this is None (new blank file from rb-cli).
                    unix_times: overrides.unix_times,
                    ..Default::default()
                };
                // Raw OSType bytes rather than the text form, so high-bit codes
                // survive; the extension dictionary still fills an absent one.
                if let Some(imp) = mac_fork {
                    if !imp.data.is_empty() {
                        create_opts.resource_fork = Some(
                            crate::fs::filesystem::ResourceForkSource::Data(imp.data.clone()),
                        );
                    }
                    create_opts.os_type = imp.type_code;
                    create_opts.os_creator = imp.creator_code;
                }
                efs.create_file(&parent, name, data, size, &create_opts)
                    .map_err(|e| anyhow!("create_file {display}: {e}"))?;
                self.dir_children
                    .get_mut(&parent_key)
                    .expect("seeded above")
                    .insert(name.clone());
                self.stats.files += 1;
                self.stats.total_bytes += size;
                if !overrides.is_empty() {
                    self.stats.perms_applied += 1;
                }
            }
            ImportItem::Unsupported => self.stats.other_skipped += 1,
        }
        Ok(())
    }

    /// Create (or resolve) the directory at `comps` and hand back its entry,
    /// so a caller can run a nested import rooted there — what expanding an
    /// archive into the image needs. Name validation matches [`Self::push`],
    /// so a name the target can't store is reported rather than half-created.
    pub fn ensure_dir_at(
        &mut self,
        efs: &mut dyn EditableFilesystem,
        comps: &[String],
    ) -> Result<Option<FileEntry>> {
        if comps.iter().any(|c| efs.validate_name(c).is_err()) {
            self.stats.invalid_names_skipped += 1;
            return Ok(None);
        }
        let dir = self.ensure_dir(efs, comps, &AttrOverrides::default())?;
        Ok(Some(dir))
    }

    /// Ensure every directory named by `comps` exists under the import root,
    /// creating missing ones (mkdir -p). Returns the deepest directory's entry.
    ///
    /// `leaf_overrides` applies to the LAST component only — that is the one
    /// the source has an entry for. Intermediate components are implicit
    /// parents the source never described, so they take the resolver's
    /// inherit-from-parent default.
    fn ensure_dir(
        &mut self,
        efs: &mut dyn EditableFilesystem,
        comps: &[String],
        leaf_overrides: &AttrOverrides,
    ) -> Result<FileEntry> {
        let mut key = String::new();
        let mut parent = self.dir_cache.get("").expect("root cached").clone();
        let last = comps.len().saturating_sub(1);
        for (i, comp) in comps.iter().enumerate() {
            let next_key = if key.is_empty() {
                comp.clone()
            } else {
                format!("{key}/{comp}")
            };
            if let Some(e) = self.dir_cache.get(&next_key) {
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
                    let attrs =
                        crate::fs::attrs::resolve_dir_attrs(&overrides, None, Some(&parent));
                    let dir_opts = crate::fs::filesystem::CreateDirectoryOptions {
                        mode: Some(attrs.dir_mode()),
                        uid: Some(attrs.uid),
                        gid: Some(attrs.gid),
                        unix_times: overrides.unix_times,
                        ..Default::default()
                    };
                    let e = efs
                        .create_directory(&parent, comp, &dir_opts)
                        .map_err(|err| anyhow!("create_directory {comp:?}: {err}"))?;
                    self.stats.dirs_created += 1;
                    e
                }
            };
            self.dir_cache.insert(next_key.clone(), entry.clone());
            parent = entry;
            key = next_key;
        }
        Ok(parent)
    }
}

/// Stamp mode / ownership onto an entry that already existed, so it can't
/// be set at creation time. Only the filesystems implementing these two
/// hooks can honour it; others report `Unsupported` and are left alone.
fn apply_attrs_after_create(
    efs: &mut dyn EditableFilesystem,
    entry: &FileEntry,
    overrides: &AttrOverrides,
    applied: &mut bool,
) -> Result<()> {
    if let Some(m) = overrides.mode {
        match efs.set_permissions(entry, m) {
            Ok(()) => *applied = true,
            Err(ref e) if is_unsupported(e) => {}
            Err(e) => return Err(anyhow!("set_permissions {}: {e}", entry.path)),
        }
    }
    if let (Some(u), Some(g)) = (overrides.uid, overrides.gid) {
        match efs.set_owner(entry, u, g) {
            Ok(()) => *applied = true,
            Err(ref e) if is_unsupported(e) => {}
            Err(e) => return Err(anyhow!("set_owner {}: {e}", entry.path)),
        }
    }
    Ok(())
}

pub fn find_child(
    efs: &mut dyn EditableFilesystem,
    parent: &FileEntry,
    name: &str,
) -> Result<Option<FileEntry>> {
    let children = efs
        .list_directory(parent)
        .map_err(|e| anyhow!("list_directory {}: {e}", parent.path))?;
    Ok(children.into_iter().find(|c| c.name == name))
}
