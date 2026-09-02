//! Staged-edit queue for the filesystem browser.
//!
//! GUI staging code pushes [`StagedEdit`] values into a queue while the user
//! makes changes; nothing touches disk until the user clicks "Apply Edits",
//! which feeds the queue to [`apply_edit`] in order against an open
//! [`EditableFilesystem`]. Keeping the enum + dispatch here means future GUIs
//! (and tests) can stage and apply edits without depending on the egui view.
//!
//! Extracted from `gui/browse_view.rs` per §5 of `docs/codecleanup.md`.

use std::fs::File;
use std::io::Cursor;
use std::path::PathBuf;

use crate::fs::entry::FileEntry;
use crate::fs::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, FilesystemError,
    ResourceForkSource,
};
use crate::fs::resource_fork::{self, ImportedResourceFork};

/// Original timestamps captured from a source file so a cross-image copy can
/// reproduce them on the destination instead of stamping the current time.
/// Each filesystem family uses its own date representation; the unused halves
/// stay `None`. Amiga dates are applied via `CreateFileOptions::amiga_dates`
/// (honored by AFFS `create_file`); HFS dates via `set_dates` after creation;
/// Unix mtimes via `CreateFileOptions::unix_times` (honoured by every Unix-
/// family driver — ext, UFS, EFS, XFS, minix, squashfs, …).
#[derive(Debug, Clone, Default)]
pub struct PreservedDates {
    /// AmigaDOS `(days, minutes, ticks)` since 1978-01-01.
    pub amiga: Option<(i32, i32, i32)>,
    /// HFS/HFS+ `(create, modify, backup)` in Mac-epoch seconds.
    pub mac: Option<(u32, u32, u32)>,
    /// Unix mtime in seconds since 1970-01-01, the numeric `FileEntry.
    /// modified_unix` captured from any per-second-dated source (Unix
    /// inode / Mac dates / Amiga datestamp / tar Header / host stat).
    pub unix_mtime: Option<u64>,
}

impl PreservedDates {
    /// Capture whatever raw dates a source `FileEntry` carries. Returns
    /// `None` when the entry has no timestamp at all — the copy then just
    /// stamps the current time. `unix_mtime` is populated whenever the
    /// entry carries one, so a cross-fs copy from any per-second-dated
    /// source (ext → EFS, AFFS → ext, tar → xfs, …) preserves the date
    /// through Unix-family drivers' new `CreateFileOptions.unix_times`.
    pub fn from_entry(entry: &FileEntry) -> Option<Self> {
        if entry.mac_dates.is_none() && entry.amiga_date.is_none() && entry.modified_unix.is_none()
        {
            return None;
        }
        Some(Self {
            amiga: entry.amiga_date,
            mac: entry.mac_dates,
            unix_mtime: entry.modified_unix,
        })
    }
}

/// A single edit operation queued by the GUI, applied later against an
/// editable filesystem in insertion order.
#[derive(Debug, Clone)]
pub enum StagedEdit {
    AddFile {
        parent: FileEntry,
        name: String,
        host_path: PathBuf,
        size: u64,
        /// ProDOS-specific overrides. None means "auto-detect from the host
        /// filename extension at apply time".
        prodos_type: Option<u8>,
        prodos_aux: Option<u16>,
        /// Resource fork data detected from the host (HFS/HFS+ only).
        resource_fork: Option<resource_fork::ImportedResourceFork>,
        /// HFS/HFS+ type/creator overrides set by the user before Apply.
        /// `None` means "let create_file pick the default" (FInfo from the
        /// resource_fork sidecar if any, else the extension dictionary).
        hfs_type_override: Option<[u8; 4]>,
        hfs_creator_override: Option<[u8; 4]>,
        /// Original timestamps captured from the source file, applied to the
        /// created copy so dates survive a cross-image copy (the "keep original
        /// dates" option). `None` lets `create_file` stamp the current time.
        dates: Option<PreservedDates>,
        /// What to do if the destination name is already taken.
        ///
        /// Defaults to [`OnConflict::Fail`], which is what every caller did
        /// before a replace was possible here: the driver rejected the
        /// duplicate name and the batch errored. The GUI and Commander set this
        /// from the conflict review the user is shown before applying, so the
        /// decision is made once, up front, rather than by a modal interrupting
        /// a half-applied batch.
        on_conflict: crate::fs::replace::OnConflict,
    },
    CreateDirectory {
        parent: FileEntry,
        name: String,
    },
    DeleteEntry {
        parent: FileEntry,
        entry: FileEntry,
    },
    DeleteRecursive {
        parent: FileEntry,
        entry: FileEntry,
    },
    /// Rename an entry in place (keeps its identity / contents). Applied via
    /// [`EditableFilesystem::rename`]; the filesystem must support it (gated in
    /// the GUI by `fs::supports_rename`).
    Rename {
        parent: FileEntry,
        entry: FileEntry,
        new_name: String,
    },
    SetProdosType {
        entry: FileEntry,
        type_byte: u8,
        aux_type: u16,
    },
    /// Set the ProDOS access byte on an existing entry (file or subdir).
    /// `0xC3` = unlocked; `0x21` = locked. ProDOS-only — applied via
    /// `EditableFilesystem::set_prodos_access`.
    SetProdosAccess {
        entry: FileEntry,
        access: u8,
    },
    BlessFolder {
        entry: FileEntry,
    },
    /// Write the 1024-byte HFS boot-block region (sectors 0–1) verbatim,
    /// captured from a donor disk at staging time. Makes a classic-HFS
    /// volume bootable; HFS/HFS+ only.
    WriteBootBlocks {
        blocks: Box<[u8; 1024]>,
    },
    /// Set HFS/HFS+ type and creator codes on an existing on-disk file.
    SetTypeCreator {
        entry: FileEntry,
        type_code: [u8; 4],
        creator_code: [u8; 4],
    },
    /// Set Unix permission bits (`st_mode`) on an existing entry. Applied via
    /// [`EditableFilesystem::set_permissions`].
    SetPermissions {
        entry: FileEntry,
        mode: u32,
    },
    /// Set the owning uid/gid on an existing entry. Applied via
    /// [`EditableFilesystem::set_owner`]; every Unix filesystem supports it.
    SetOwner {
        entry: FileEntry,
        uid: u32,
        gid: u32,
    },
    /// Create or replace an extended attribute. Applied via
    /// [`EditableFilesystem::set_xattr`]; ext / XFS / SquashFS only.
    SetXattr {
        entry: FileEntry,
        name: String,
        value: Vec<u8>,
    },
    /// Remove an extended attribute. Applied via
    /// [`EditableFilesystem::remove_xattr`].
    RemoveXattr {
        entry: FileEntry,
        name: String,
    },
    /// Set HFS/HFS+ creation/modification/backup dates on an existing entry.
    /// Values are Mac epoch seconds (since 1904-01-01 UTC); applied via
    /// [`EditableFilesystem::set_dates`].
    SetDates {
        entry: FileEntry,
        create: u32,
        modify: u32,
        backup: u32,
    },
}

/// True when `edit` is a set/remove xattr op for `path` + `name` — the
/// supersede predicate shared by [`EditQueue::replace_set_xattr`] and
/// [`EditQueue::replace_remove_xattr`].
fn xattr_edit_targets(edit: &StagedEdit, path: &str, name: &str) -> bool {
    match edit {
        StagedEdit::SetXattr { entry, name: n, .. }
        | StagedEdit::RemoveXattr { entry, name: n } => entry.path == path && n == name,
        _ => false,
    }
}

/// Walk an editable filesystem from the root to the directory at `path`,
/// returning its live [`FileEntry`].
///
/// Staged edits capture a `parent` `FileEntry` at staging time, but for a
/// pending-add directory the `location` (CNID/cluster) field is a placeholder
/// because the directory does not yet exist on disk. Re-resolving by path at
/// apply time picks up the real identifier assigned when the earlier
/// `CreateDirectory` edit ran.
pub fn resolve_dir_by_path(
    efs: &mut dyn EditableFilesystem,
    path: &str,
) -> Result<FileEntry, FilesystemError> {
    let mut current = efs.root()?;
    if path == "/" || path.is_empty() {
        return Ok(current);
    }
    for component in path.trim_start_matches('/').split('/') {
        if component.is_empty() {
            continue;
        }
        let children = efs.list_directory(&current)?;
        current = children
            .into_iter()
            .find(|e| e.is_directory() && e.name == component)
            .ok_or_else(|| {
                FilesystemError::NotFound(format!(
                    "directory '{component}' not found while resolving '{path}'"
                ))
            })?;
    }
    Ok(current)
}

/// Apply a single staged edit to `efs`. Pure dispatch — does not call
/// `sync_metadata`, which the caller is responsible for after the full batch.
pub fn apply_edit(
    efs: &mut dyn EditableFilesystem,
    edit: &StagedEdit,
) -> Result<(), FilesystemError> {
    match edit {
        StagedEdit::AddFile {
            parent,
            name,
            host_path,
            size,
            prodos_type,
            prodos_aux,
            resource_fork: rsrc_import,
            hfs_type_override,
            hfs_creator_override,
            dates,
            on_conflict,
        } => {
            let mut opts = CreateFileOptions {
                type_code: prodos_type.map(|t| format!("${:02X}", t)),
                aux_type: *prodos_aux,
                // Amiga dates round-trip via create_file (AFFS honors this);
                // HFS dates are applied with set_dates after creation below;
                // Unix mtime rides through every Unix-family driver via
                // create_file's `unix_times` (falls back to the source's
                // native scheme when both are set, since the driver picks
                // whichever it can honour).
                amiga_dates: dates.as_ref().and_then(|d| d.amiga),
                unix_times: dates
                    .as_ref()
                    .and_then(|d| d.unix_mtime)
                    .map(crate::fs::times::UnixTimes::mtime_only),
                ..Default::default()
            };

            if let Some(imp) = rsrc_import {
                if !imp.data.is_empty() {
                    opts.resource_fork = Some(ResourceForkSource::Data(imp.data.clone()));
                }
                // Type/creator from container overrides auto-detect, but not
                // explicit ProDOS overrides. Carried as raw `os_type` bytes so
                // high-bit OSTypes survive (no lossy text round-trip).
                if opts.os_type.is_none() {
                    opts.os_type = imp.type_code;
                }
                if opts.os_creator.is_none() {
                    opts.os_creator = imp.creator_code;
                }
            }

            // Per-staged-file HFS overrides (from the inline editor) win over
            // both AppleDouble FInfo and the dictionary.
            if let Some(tc) = hfs_type_override {
                opts.os_type = Some(*tc);
            }
            if let Some(cc) = hfs_creator_override {
                opts.os_creator = Some(*cc);
            }

            // For MacBinary imports, use the extracted data fork instead of
            // the raw .bin file.
            if let Some(imp) = rsrc_import {
                if let Some(ref data_fork) = imp.data_fork {
                    let mut cursor = Cursor::new(data_fork);
                    let df_size = data_fork.len() as u64;
                    let resolved_parent = resolve_dir_by_path(efs, &parent.path)?;
                    crate::fs::replace::create_or_replace(
                        efs,
                        &resolved_parent,
                        name,
                        &mut cursor,
                        df_size,
                        &opts,
                        crate::fs::replace::ReplacePolicy {
                            on_conflict: *on_conflict,
                            ..Default::default()
                        },
                    )?;
                    return Ok(());
                }
            }

            let mut file = File::open(host_path).map_err(FilesystemError::Io)?;
            let resolved_parent = resolve_dir_by_path(efs, &parent.path)?;
            // Routed through the shared helper so the GUI and Commander get the
            // same replace semantics as the CLI: metadata carried over from the
            // file being replaced, and the swap staged so a failure mid-write
            // leaves the original intact.
            let outcome = crate::fs::replace::create_or_replace(
                efs,
                &resolved_parent,
                name,
                &mut file,
                *size,
                &opts,
                crate::fs::replace::ReplacePolicy {
                    on_conflict: *on_conflict,
                    ..Default::default()
                },
            )?;
            let Some(created) = outcome.created else {
                // Skipped by the user's conflict choice; nothing further to do.
                return Ok(());
            };
            // HFS/HFS+ dates aren't a create_file option, so apply them after
            // the fact. Best-effort: filesystems without set_dates return
            // Unsupported, which we ignore (the copy keeps its create-time stamp).
            if let Some((c, m, b)) = dates.as_ref().and_then(|d| d.mac) {
                let _ = efs.set_dates(&created, c, m, b);
            }
            Ok(())
        }
        StagedEdit::CreateDirectory { parent, name } => {
            let resolved_parent = resolve_dir_by_path(efs, &parent.path)?;
            // A folder that is already there is the outcome asked for; only a
            // file in the way is a real collision (the pre-scan skips folders).
            if let Some(existing) = efs
                .list_directory(&resolved_parent)?
                .into_iter()
                .find(|e| &e.name == name)
            {
                if existing.is_directory() {
                    return Ok(());
                }
                return Err(FilesystemError::AlreadyExists(name.clone()));
            }
            efs.create_directory(&resolved_parent, name, &CreateDirectoryOptions::default())?;
            Ok(())
        }
        StagedEdit::DeleteEntry { parent, entry } => efs.delete_entry(parent, entry),
        StagedEdit::DeleteRecursive { parent, entry } => efs.delete_recursive(parent, entry),
        StagedEdit::Rename {
            parent,
            entry,
            new_name,
        } => {
            let resolved_parent = resolve_dir_by_path(efs, &parent.path)?;
            efs.rename(&resolved_parent, entry, new_name)
        }
        StagedEdit::SetProdosType {
            entry,
            type_byte,
            aux_type,
        } => efs.set_prodos_type(entry, *type_byte, *aux_type),
        StagedEdit::SetProdosAccess { entry, access } => efs.set_prodos_access(entry, *access),
        StagedEdit::BlessFolder { entry } => efs.set_blessed_folder(entry),
        StagedEdit::WriteBootBlocks { blocks } => efs.write_boot_blocks(blocks),
        StagedEdit::SetTypeCreator {
            entry,
            type_code,
            creator_code,
        } => efs.set_type_creator(
            entry,
            &String::from_utf8_lossy(type_code),
            &String::from_utf8_lossy(creator_code),
        ),
        StagedEdit::SetPermissions { entry, mode } => efs.set_permissions(entry, *mode),
        StagedEdit::SetOwner { entry, uid, gid } => efs.set_owner(entry, *uid, *gid),
        StagedEdit::SetXattr { entry, name, value } => efs.set_xattr(entry, name, value),
        StagedEdit::RemoveXattr { entry, name } => efs.remove_xattr(entry, name),
        StagedEdit::SetDates {
            entry,
            create,
            modify,
            backup,
        } => efs.set_dates(entry, *create, *modify, *backup),
    }
}

/// Net free-space impact of a staged batch.
#[derive(Debug, Clone, Copy, Default)]
pub struct SpaceDelta {
    /// Bytes that will be consumed once `AddFile` edits run.
    pub added: u64,
    /// Bytes that will be reclaimed once `Delete*` edits run.
    pub freed: u64,
}

/// Staged-edit queue with the predicates and mutations the GUI needs while the
/// user is staging changes. The queue is "dumb" — applying edits is still done
/// via [`apply_edit`]; this type only owns the list and answers questions
/// about it.
#[derive(Debug, Default)]
pub struct EditQueue {
    edits: Vec<StagedEdit>,
}

impl EditQueue {
    pub fn new() -> Self {
        Self { edits: Vec::new() }
    }

    pub fn len(&self) -> usize {
        self.edits.len()
    }

    /// One plain-language line per staged edit, in apply order — for a
    /// "pending edits" review list (the GUI shows these before Apply).
    pub fn describe(&self) -> Vec<String> {
        self.edits
            .iter()
            .map(|e| match e {
                StagedEdit::AddFile { parent, name, .. } => {
                    format!("Add file: {}", Self::pending_path(&parent.path, name))
                }
                StagedEdit::CreateDirectory { parent, name } => {
                    format!("New folder: {}", Self::pending_path(&parent.path, name))
                }
                StagedEdit::DeleteEntry { entry, .. } => format!("Delete: {}", entry.path),
                StagedEdit::DeleteRecursive { entry, .. } => {
                    format!("Delete (recursive): {}", entry.path)
                }
                StagedEdit::Rename {
                    entry, new_name, ..
                } => format!("Rename: {} -> {}", entry.path, new_name),
                StagedEdit::SetProdosType {
                    entry,
                    type_byte,
                    aux_type,
                } => format!(
                    "ProDOS type: {} -> ${type_byte:02X}/${aux_type:04X}",
                    entry.path
                ),
                StagedEdit::SetProdosAccess { entry, access } => {
                    format!("ProDOS access: {} -> ${access:02X}", entry.path)
                }
                StagedEdit::BlessFolder { entry } => format!("Bless folder: {}", entry.path),
                StagedEdit::WriteBootBlocks { .. } => "Write boot blocks".to_string(),
                StagedEdit::SetTypeCreator {
                    entry,
                    type_code,
                    creator_code,
                } => format!(
                    "Type/Creator: {} -> {}/{}",
                    entry.path,
                    String::from_utf8_lossy(type_code),
                    String::from_utf8_lossy(creator_code),
                ),
                StagedEdit::SetPermissions { entry, mode } => {
                    format!("Permissions: {} -> {:o}", entry.path, mode & 0o7777)
                }
                StagedEdit::SetOwner { entry, uid, gid } => {
                    format!("Owner: {} -> {uid}:{gid}", entry.path)
                }
                StagedEdit::SetXattr { entry, name, .. } => {
                    format!("Xattr set: {} {name}", entry.path)
                }
                StagedEdit::RemoveXattr { entry, name } => {
                    format!("Xattr remove: {} {name}", entry.path)
                }
                StagedEdit::SetDates { entry, .. } => format!("Dates: {}", entry.path),
            })
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.edits.is_empty()
    }

    pub fn clear(&mut self) {
        self.edits.clear();
    }

    pub fn push(&mut self, edit: StagedEdit) {
        self.edits.push(edit);
    }

    pub fn iter(&self) -> std::slice::Iter<'_, StagedEdit> {
        self.edits.iter()
    }

    pub fn drain(&mut self) -> std::vec::Drain<'_, StagedEdit> {
        self.edits.drain(..)
    }

    /// Staged additions whose destination name is already taken, as
    /// `(full path, file name)`.
    ///
    /// Answered before applying, on purpose. The alternative — discovering each
    /// collision mid-batch — means interrupting the user file by file and
    /// leaving a half-applied queue behind if they change their mind at file 7
    /// of 12. Staging exists precisely so the questions can be asked once.
    pub fn conflicting_adds(&self, efs: &mut dyn EditableFilesystem) -> Vec<(String, String)> {
        // Replay the queue in order over each directory's names, so an earlier
        // rename or delete frees (or takes) a name the way the apply will see it.
        type Occupied = std::collections::HashMap<String, Option<Vec<String>>>;
        fn names_in<'a>(
            occupied: &'a mut Occupied,
            parent: &FileEntry,
            efs: &mut dyn EditableFilesystem,
        ) -> Option<&'a mut Vec<String>> {
            occupied
                .entry(parent.path.clone())
                .or_insert_with(|| {
                    let dir = resolve_dir_by_path(efs, &parent.path).ok()?;
                    let children = efs.list_directory(&dir).ok()?;
                    Some(children.into_iter().map(|e| e.name).collect())
                })
                .as_mut()
        }
        let mut occupied = Occupied::new();
        let mut out = Vec::new();
        for edit in &self.edits {
            match edit {
                StagedEdit::AddFile { parent, name, .. } => {
                    if let Some(names) = names_in(&mut occupied, parent, efs) {
                        if names.iter().any(|n| n == name) {
                            out.push((Self::pending_path(&parent.path, name), name.clone()));
                        } else {
                            names.push(name.clone());
                        }
                    }
                }
                StagedEdit::CreateDirectory { parent, name } => {
                    if let Some(names) = names_in(&mut occupied, parent, efs) {
                        if !names.iter().any(|n| n == name) {
                            names.push(name.clone());
                        }
                    }
                }
                StagedEdit::DeleteEntry { parent, entry }
                | StagedEdit::DeleteRecursive { parent, entry } => {
                    if let Some(names) = names_in(&mut occupied, parent, efs) {
                        names.retain(|n| n != &entry.name);
                    }
                }
                StagedEdit::Rename {
                    parent,
                    entry,
                    new_name,
                } => {
                    if let Some(names) = names_in(&mut occupied, parent, efs) {
                        names.retain(|n| n != &entry.name);
                        names.push(new_name.clone());
                    }
                }
                _ => {}
            }
        }
        out
    }

    /// Apply a conflict decision to one staged addition, keyed by the full path
    /// [`conflicting_adds`] reported.
    pub fn set_conflict_for(&mut self, path: &str, on: crate::fs::replace::OnConflict) {
        for edit in &mut self.edits {
            if let StagedEdit::AddFile {
                parent,
                name,
                on_conflict,
                ..
            } = edit
            {
                if Self::pending_path(&parent.path, name) == path {
                    *on_conflict = on;
                }
            }
        }
    }

    /// Apply one decision to every staged addition.
    pub fn set_all_conflicts(&mut self, on: crate::fs::replace::OnConflict) {
        for edit in &mut self.edits {
            if let StagedEdit::AddFile { on_conflict, .. } = edit {
                *on_conflict = on;
            }
        }
    }

    /// Full path for an `AddFile` / `CreateDirectory` edit, anchored at root.
    fn pending_path(parent_path: &str, name: &str) -> String {
        if parent_path == "/" {
            format!("/{name}")
        } else {
            format!("{parent_path}/{name}")
        }
    }

    /// True if any `Delete*` edit targets `entry_path`.
    pub fn is_pending_delete(&self, entry_path: &str) -> bool {
        self.edits.iter().any(|edit| match edit {
            StagedEdit::DeleteEntry { entry: e, .. }
            | StagedEdit::DeleteRecursive { entry: e, .. } => e.path == entry_path,
            _ => false,
        })
    }

    /// True if `entry_path` is a pending add (file or directory).
    pub fn is_pending_add(&self, entry_path: &str) -> bool {
        self.edits.iter().any(|edit| match edit {
            StagedEdit::AddFile { parent, name, .. }
            | StagedEdit::CreateDirectory { parent, name, .. } => {
                Self::pending_path(&parent.path, name) == entry_path
            }
            _ => false,
        })
    }

    /// True when a metadata edit (type/creator, ProDOS type/access, dates, or
    /// permissions) is staged for `entry_path` — drives the "changed" row tint
    /// so the user sees which existing files have pending metadata edits.
    pub fn has_pending_metadata(&self, entry_path: &str) -> bool {
        self.edits.iter().any(|edit| match edit {
            StagedEdit::SetTypeCreator { entry, .. }
            | StagedEdit::SetProdosType { entry, .. }
            | StagedEdit::SetProdosAccess { entry, .. }
            | StagedEdit::SetDates { entry, .. }
            | StagedEdit::SetOwner { entry, .. }
            | StagedEdit::SetXattr { entry, .. }
            | StagedEdit::RemoveXattr { entry, .. }
            | StagedEdit::SetPermissions { entry, .. } => entry.path == entry_path,
            _ => false,
        })
    }

    /// Remove any `Delete*` edit targeting `entry_path` (the "undelete" action).
    /// Returns `true` if a matching edit was removed.
    pub fn remove_pending_delete(&mut self, entry_path: &str) -> bool {
        let before = self.edits.len();
        self.edits.retain(|edit| {
            !matches!(
                edit,
                StagedEdit::DeleteEntry { entry, .. } | StagedEdit::DeleteRecursive { entry, .. }
                    if entry.path == entry_path
            )
        });
        self.edits.len() < before
    }

    /// New name of a pending `Rename` targeting `entry_path`, if any (the most
    /// recently staged one wins).
    pub fn pending_rename_for(&self, entry_path: &str) -> Option<&str> {
        self.edits.iter().rev().find_map(|edit| match edit {
            StagedEdit::Rename {
                entry, new_name, ..
            } if entry.path == entry_path => Some(new_name.as_str()),
            _ => None,
        })
    }

    /// Remove any pending `Rename` targeting `entry_path`. Returns `true` if a
    /// matching edit was removed.
    pub fn remove_pending_rename(&mut self, entry_path: &str) -> bool {
        let before = self.edits.len();
        self.edits.retain(
            |edit| !matches!(edit, StagedEdit::Rename { entry, .. } if entry.path == entry_path),
        );
        self.edits.len() < before
    }

    /// Synthesize `FileEntry`s for pending adds whose parent is `parent_path`.
    pub fn pending_adds_for(&self, parent_path: &str) -> Vec<FileEntry> {
        self.edits
            .iter()
            .filter_map(|edit| match edit {
                StagedEdit::AddFile {
                    parent, name, size, ..
                } if parent.path == parent_path => {
                    let path = Self::pending_path(&parent.path, name);
                    Some(FileEntry::new_file(name.clone(), path, *size, 0))
                }
                StagedEdit::CreateDirectory { parent, name, .. } if parent.path == parent_path => {
                    let path = Self::pending_path(&parent.path, name);
                    Some(FileEntry::new_directory(name.clone(), path, 0))
                }
                _ => None,
            })
            .collect()
    }

    /// Remove the `AddFile` / `CreateDirectory` edit at `entry_path`. Returns
    /// `true` if a matching edit was removed.
    pub fn remove_pending_add(&mut self, entry_path: &str) -> bool {
        let before = self.edits.len();
        self.edits.retain(|edit| match edit {
            StagedEdit::AddFile { parent, name, .. }
            | StagedEdit::CreateDirectory { parent, name, .. } => {
                Self::pending_path(&parent.path, name) != entry_path
            }
            _ => true,
        });
        self.edits.len() < before
    }

    /// Remove the pending add at `entry_path` plus every pending edit nested
    /// underneath it (used when the user unstages a pending directory — its
    /// staged children would otherwise become orphans whose `parent.path` no
    /// longer resolves at apply time). Returns the number of edits removed.
    pub fn remove_pending_subtree(&mut self, entry_path: &str) -> usize {
        let prefix = if entry_path == "/" {
            "/".to_string()
        } else {
            format!("{entry_path}/")
        };
        let before = self.edits.len();
        self.edits.retain(|edit| match edit {
            StagedEdit::AddFile { parent, name, .. }
            | StagedEdit::CreateDirectory { parent, name, .. } => {
                let path = Self::pending_path(&parent.path, name);
                path != entry_path && !path.starts_with(&prefix)
            }
            _ => true,
        });
        before - self.edits.len()
    }

    /// Imported resource fork attached to the pending `AddFile` at `entry_path`.
    pub fn pending_resource_fork_for(&self, entry_path: &str) -> Option<&ImportedResourceFork> {
        self.edits.iter().find_map(|edit| match edit {
            StagedEdit::AddFile {
                parent,
                name,
                resource_fork: Some(imp),
                ..
            } if Self::pending_path(&parent.path, name) == entry_path => Some(imp),
            _ => None,
        })
    }

    /// Net space impact of the staged batch.
    pub fn space_delta(&self) -> SpaceDelta {
        let mut delta = SpaceDelta::default();
        for edit in &self.edits {
            match edit {
                StagedEdit::AddFile { size, .. } => delta.added += size,
                StagedEdit::DeleteEntry { entry, .. }
                | StagedEdit::DeleteRecursive { entry, .. }
                    if !entry.is_directory() =>
                {
                    delta.freed += entry.size;
                }
                _ => {}
            }
        }
        delta
    }

    /// Resolve the effective HFS/HFS+ type/creator codes for `entry`,
    /// considering pending overrides, imported AppleDouble FInfo, on-disk
    /// catalog values, and the extension dictionary. Returns `[0;4]` for
    /// either half if nothing is known.
    pub fn resolved_hfs_type_creator(&self, entry: &FileEntry) -> ([u8; 4], [u8; 4]) {
        for edit in &self.edits {
            if let StagedEdit::AddFile {
                parent,
                name,
                resource_fork,
                hfs_type_override,
                hfs_creator_override,
                ..
            } = edit
            {
                if Self::pending_path(&parent.path, name) != entry.path {
                    continue;
                }
                let imp_t = resource_fork.as_ref().and_then(|i| i.type_code);
                let imp_c = resource_fork.as_ref().and_then(|i| i.creator_code);
                // Mirror `apply` exactly: it hands `create_file` an `os_type` of
                // override-or-import, and the create path then resolves blanks /
                // the extension dictionary. Going through the same resolver here
                // is what keeps this preview honest -- an import carrying a blank
                // FInfo must display the dictionary value that will actually be
                // written, not a blank.
                return crate::fs::hfs_common::resolve_create_type_creator(
                    name,
                    hfs_type_override.or(imp_t),
                    hfs_creator_override.or(imp_c),
                    None,
                    None,
                );
            }
        }
        // A staged SetTypeCreator on an existing file wins over the on-disk
        // value, so the editor/detail rows reflect the pending change.
        if let Some((t, c)) = self.edits.iter().rev().find_map(|e| match e {
            StagedEdit::SetTypeCreator {
                entry: e2,
                type_code,
                creator_code,
            } if e2.path == entry.path => Some((*type_code, *creator_code)),
            _ => None,
        }) {
            return (t, c);
        }
        let t = entry.type_code.unwrap_or([0; 4]);
        let c = entry.creator_code.unwrap_or([0; 4]);
        (t, c)
    }

    /// Set the per-entry HFS type/creator override on a pending `AddFile`.
    /// Returns `true` if an entry at `entry_path` was found.
    pub fn set_pending_hfs_override(
        &mut self,
        entry_path: &str,
        type_code: [u8; 4],
        creator_code: [u8; 4],
    ) -> bool {
        for edit in self.edits.iter_mut() {
            if let StagedEdit::AddFile {
                parent,
                name,
                hfs_type_override,
                hfs_creator_override,
                ..
            } = edit
            {
                if Self::pending_path(&parent.path, name) == entry_path {
                    *hfs_type_override = Some(type_code);
                    *hfs_creator_override = Some(creator_code);
                    return true;
                }
            }
        }
        false
    }

    /// Set the per-entry ProDOS type/aux override on a pending `AddFile`.
    /// Returns `true` if an entry at `entry_path` was found.
    pub fn set_pending_prodos_override(
        &mut self,
        entry_path: &str,
        type_byte: u8,
        aux_type: u16,
    ) -> bool {
        for edit in self.edits.iter_mut() {
            if let StagedEdit::AddFile {
                parent,
                name,
                prodos_type,
                prodos_aux,
                ..
            } = edit
            {
                if Self::pending_path(&parent.path, name) == entry_path {
                    *prodos_type = Some(type_byte);
                    *prodos_aux = Some(aux_type);
                    return true;
                }
            }
        }
        false
    }

    /// Push a `SetProdosType` edit, replacing any prior one targeting the
    /// same on-disk path.
    pub fn replace_set_prodos_type(&mut self, entry: &FileEntry, type_byte: u8, aux_type: u16) {
        let path = entry.path.clone();
        self.edits.retain(|e| match e {
            StagedEdit::SetProdosType { entry: e2, .. } => e2.path != path,
            _ => true,
        });
        self.edits.push(StagedEdit::SetProdosType {
            entry: entry.clone(),
            type_byte,
            aux_type,
        });
    }

    /// Push a `SetProdosAccess` edit, replacing any prior one targeting the
    /// same on-disk path.
    pub fn replace_set_prodos_access(&mut self, entry: &FileEntry, access: u8) {
        let path = entry.path.clone();
        self.edits.retain(|e| match e {
            StagedEdit::SetProdosAccess { entry: e2, .. } => e2.path != path,
            _ => true,
        });
        self.edits.push(StagedEdit::SetProdosAccess {
            entry: entry.clone(),
            access,
        });
    }

    /// Return the access byte the user has staged for `entry_path`, if any.
    /// Used by the GUI to render the staged access value back into the lock
    /// toggle before the user clicks Apply.
    pub fn pending_prodos_access_for(&self, entry_path: &str) -> Option<u8> {
        self.edits.iter().rev().find_map(|edit| match edit {
            StagedEdit::SetProdosAccess { entry, access } if entry.path == entry_path => {
                Some(*access)
            }
            _ => None,
        })
    }

    /// Push a `SetTypeCreator` edit, replacing any prior one targeting the
    /// same on-disk path.
    pub fn replace_set_type_creator(
        &mut self,
        entry: &FileEntry,
        type_code: [u8; 4],
        creator_code: [u8; 4],
    ) {
        let path = entry.path.clone();
        self.edits.retain(|e| match e {
            StagedEdit::SetTypeCreator { entry: e2, .. } => e2.path != path,
            _ => true,
        });
        self.edits.push(StagedEdit::SetTypeCreator {
            entry: entry.clone(),
            type_code,
            creator_code,
        });
    }

    /// Push a `SetPermissions` edit, replacing any prior one targeting the
    /// same on-disk path.
    pub fn replace_set_permissions(&mut self, entry: &FileEntry, mode: u32) {
        let path = entry.path.clone();
        self.edits.retain(|e| match e {
            StagedEdit::SetPermissions { entry: e2, .. } => e2.path != path,
            _ => true,
        });
        self.edits.push(StagedEdit::SetPermissions {
            entry: entry.clone(),
            mode,
        });
    }

    /// Return the permission bits the user has staged for `entry_path`, if any.
    pub fn pending_permissions_for(&self, entry_path: &str) -> Option<u32> {
        self.edits.iter().rev().find_map(|edit| match edit {
            StagedEdit::SetPermissions { entry, mode } if entry.path == entry_path => Some(*mode),
            _ => None,
        })
    }

    /// Push a `SetOwner` edit, replacing any prior one for the same path.
    pub fn replace_set_owner(&mut self, entry: &FileEntry, uid: u32, gid: u32) {
        let path = entry.path.clone();
        self.edits.retain(|e| match e {
            StagedEdit::SetOwner { entry: e2, .. } => e2.path != path,
            _ => true,
        });
        self.edits.push(StagedEdit::SetOwner {
            entry: entry.clone(),
            uid,
            gid,
        });
    }

    /// Return the (uid, gid) the user has staged for `entry_path`, if any.
    pub fn pending_owner_for(&self, entry_path: &str) -> Option<(u32, u32)> {
        self.edits.iter().rev().find_map(|edit| match edit {
            StagedEdit::SetOwner { entry, uid, gid } if entry.path == entry_path => {
                Some((*uid, *gid))
            }
            _ => None,
        })
    }

    /// Stage a `SetXattr` (create/replace), superseding any pending set/remove of
    /// the same name on the same path so the queue holds one op per (path, name).
    pub fn replace_set_xattr(&mut self, entry: &FileEntry, name: &str, value: Vec<u8>) {
        let path = entry.path.clone();
        self.edits.retain(|e| !xattr_edit_targets(e, &path, name));
        self.edits.push(StagedEdit::SetXattr {
            entry: entry.clone(),
            name: name.to_string(),
            value,
        });
    }

    /// Stage a `RemoveXattr`, superseding any pending set/remove of the same
    /// name on the same path.
    pub fn replace_remove_xattr(&mut self, entry: &FileEntry, name: &str) {
        let path = entry.path.clone();
        self.edits.retain(|e| !xattr_edit_targets(e, &path, name));
        self.edits.push(StagedEdit::RemoveXattr {
            entry: entry.clone(),
            name: name.to_string(),
        });
    }

    /// The pending xattr edits for `entry_path`: `(name, Some(value))` for a
    /// staged set, `(name, None)` for a staged remove. Lets the UI overlay the
    /// on-disk xattrs with what the user has queued.
    pub fn pending_xattrs_for(&self, entry_path: &str) -> Vec<(String, Option<Vec<u8>>)> {
        self.edits
            .iter()
            .filter_map(|edit| match edit {
                StagedEdit::SetXattr { entry, name, value } if entry.path == entry_path => {
                    Some((name.clone(), Some(value.clone())))
                }
                StagedEdit::RemoveXattr { entry, name } if entry.path == entry_path => {
                    Some((name.clone(), None))
                }
                _ => None,
            })
            .collect()
    }

    /// Push a `SetDates` edit, replacing any prior one targeting the same
    /// on-disk path. Dates are Mac epoch seconds.
    pub fn replace_set_dates(&mut self, entry: &FileEntry, create: u32, modify: u32, backup: u32) {
        let path = entry.path.clone();
        self.edits.retain(|e| match e {
            StagedEdit::SetDates { entry: e2, .. } => e2.path != path,
            _ => true,
        });
        self.edits.push(StagedEdit::SetDates {
            entry: entry.clone(),
            create,
            modify,
            backup,
        });
    }

    /// Return the (create, modify, backup) Mac-epoch dates the user has staged
    /// for `entry_path`, if any.
    pub fn pending_dates_for(&self, entry_path: &str) -> Option<(u32, u32, u32)> {
        self.edits.iter().rev().find_map(|edit| match edit {
            StagedEdit::SetDates {
                entry,
                create,
                modify,
                backup,
            } if entry.path == entry_path => Some((*create, *modify, *backup)),
            _ => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::filesystem::Filesystem;
    use crate::fs::hfs::{create_blank_hfs, HfsFilesystem};
    use std::io::Cursor;

    const MIB: u64 = 1024 * 1024;

    /// Commander's delete-toggle round trip: staging a delete makes the entry
    /// pending, and `remove_pending_delete` (undelete) clears it without
    /// disturbing an unrelated staged delete.
    #[test]
    fn remove_pending_delete_undeletes_one_entry() {
        let parent = FileEntry::new_directory("dir".into(), "/dir".into(), 0);
        let a = FileEntry::new_file("a.txt".into(), "/dir/a.txt".into(), 10, 0);
        let b = FileEntry::new_directory("sub".into(), "/dir/sub".into(), 0);

        let mut q = EditQueue::new();
        q.push(StagedEdit::DeleteEntry {
            parent: parent.clone(),
            entry: a.clone(),
        });
        q.push(StagedEdit::DeleteRecursive {
            parent: parent.clone(),
            entry: b.clone(),
        });
        assert!(q.is_pending_delete("/dir/a.txt"));
        assert!(q.is_pending_delete("/dir/sub"));

        assert!(q.remove_pending_delete("/dir/a.txt"));
        assert!(!q.is_pending_delete("/dir/a.txt"));
        // The recursive delete of the sibling directory is untouched.
        assert!(q.is_pending_delete("/dir/sub"));
        assert_eq!(q.len(), 1);

        // Removing a non-pending path is a no-op.
        assert!(!q.remove_pending_delete("/dir/a.txt"));
    }

    /// The full GUI "Boot Blocks..." mechanism: a `WriteBootBlocks` staged
    /// edit, dispatched through `apply_edit` + `sync_metadata`, lands the
    /// 1024-byte region at sector 0 of a previously-bare HFS volume.
    #[test]
    fn write_boot_blocks_staged_edit_reaches_sector_zero() {
        // A freshly-built HFS volume has zeroed boot blocks.
        let img = create_blank_hfs(8 * MIB, 4096, "NoBoot").unwrap();
        assert_eq!(&img[0..2], &[0x00, 0x00]);

        let mut blocks = Box::new([0u8; 1024]);
        blocks[0] = 0x4C; // 'L'
        blocks[1] = 0x4B; // 'K'
        blocks[2] = 0x60;

        let mut buf = img.clone();
        {
            let mut efs = HfsFilesystem::open(Cursor::new(&mut buf), 0).unwrap();
            apply_edit(
                &mut efs,
                &StagedEdit::WriteBootBlocks {
                    blocks: blocks.clone(),
                },
            )
            .unwrap();
            efs.sync_metadata().unwrap();
        }

        // Sector 0 now carries the staged boot blocks, and the volume still
        // opens as a valid HFS volume.
        assert_eq!(&buf[0..3], &[0x4C, 0x4B, 0x60]);
        let fs = HfsFilesystem::open(Cursor::new(buf), 0).unwrap();
        assert_eq!(fs.fs_type(), "HFS");
    }

    /// Full-disk (partitioned) case: the same staged edit, applied to an HFS
    /// volume opened at a NON-zero partition offset (as on an APM disk — e.g.
    /// an infinite-mac "device image" with DDR + map + drivers ahead of the
    /// Apple_HFS partition), writes the boot blocks at the partition's first
    /// sector and leaves the preceding bytes (DDR / partition map / drivers)
    /// untouched. This is what makes "Boot Blocks..." work on full disks, not
    /// just flat HFVs.
    #[test]
    fn write_boot_blocks_honors_partition_offset() {
        // 0xC000 = 49152, the HFS offset of infinite-mac's SCSI-4.3 device
        // image header.
        const OFFSET: usize = 0xC000;
        let hfs = create_blank_hfs(8 * MIB, 4096, "InPart").unwrap();

        // Sentinel-filled leading region stands in for DDR + APM + drivers;
        // it must never be overwritten by a partition-scoped boot write.
        let mut disk = vec![0xABu8; OFFSET];
        disk.extend_from_slice(&hfs);

        let mut blocks = Box::new([0u8; 1024]);
        blocks[0] = 0x4C;
        blocks[1] = 0x4B;

        {
            let mut efs = HfsFilesystem::open(Cursor::new(&mut disk), OFFSET as u64).unwrap();
            apply_edit(
                &mut efs,
                &StagedEdit::WriteBootBlocks {
                    blocks: blocks.clone(),
                },
            )
            .unwrap();
            efs.sync_metadata().unwrap();
        }

        // Boot blocks landed at the partition offset...
        assert_eq!(&disk[OFFSET..OFFSET + 2], &[0x4C, 0x4B]);
        // ...and the leading DDR / partition-map region is pristine.
        assert!(disk[..OFFSET].iter().all(|&b| b == 0xAB));
    }

    /// `replace_set_permissions` keeps a single staged edit per on-disk path
    /// (the latest value wins) and `pending_permissions_for` reports it back.
    #[test]
    fn replace_set_permissions_dedups_and_reports() {
        let a = FileEntry::new_file("a".into(), "/a".into(), 0, 0);
        let b = FileEntry::new_file("b".into(), "/b".into(), 0, 0);

        let mut q = EditQueue::new();
        q.replace_set_permissions(&a, 0o644);
        q.replace_set_permissions(&b, 0o600);
        // Restaging the same path replaces, not appends.
        q.replace_set_permissions(&a, 0o755);

        assert_eq!(q.len(), 2);
        assert_eq!(q.pending_permissions_for("/a"), Some(0o755));
        assert_eq!(q.pending_permissions_for("/b"), Some(0o600));
        assert_eq!(q.pending_permissions_for("/c"), None);
    }

    /// `replace_set_dates` keeps a single staged edit per on-disk path and
    /// `pending_dates_for` reports the (create, modify, backup) triple back.
    #[test]
    fn replace_set_dates_dedups_and_reports() {
        let a = FileEntry::new_file("a".into(), "/a".into(), 0, 0);

        let mut q = EditQueue::new();
        q.replace_set_dates(&a, 100, 200, 300);
        q.replace_set_dates(&a, 111, 222, 333);

        assert_eq!(q.len(), 1);
        assert_eq!(q.pending_dates_for("/a"), Some((111, 222, 333)));
        assert_eq!(q.pending_dates_for("/missing"), None);
    }

    /// `has_pending_metadata` flags exactly the entries with a staged metadata
    /// edit — drives the blue "changed" row tint.
    #[test]
    fn has_pending_metadata_flags_metadata_edits() {
        let a = FileEntry::new_file("a".into(), "/a".into(), 0, 0);
        let b = FileEntry::new_file("b".into(), "/b".into(), 0, 0);
        let dir = FileEntry::new_directory("d".into(), "/d".into(), 0);

        let mut q = EditQueue::new();
        q.replace_set_dates(&a, 1, 2, 3);
        q.replace_set_permissions(&b, 0o644);
        // A delete is not a metadata edit.
        q.push(StagedEdit::DeleteEntry {
            parent: dir.clone(),
            entry: FileEntry::new_file("c".into(), "/d/c".into(), 0, 0),
        });

        assert!(q.has_pending_metadata("/a"));
        assert!(q.has_pending_metadata("/b"));
        assert!(!q.has_pending_metadata("/d/c")); // staged delete, not metadata
        assert!(!q.has_pending_metadata("/missing"));
    }

    /// A staged `SetTypeCreator` on an existing file is reflected by
    /// `resolved_hfs_type_creator` (so the editor/detail rows show the change).
    #[test]
    fn resolved_hfs_type_creator_reflects_staged_set() {
        let mut e = FileEntry::new_file("doc".into(), "/doc".into(), 0, 0);
        e.type_code = Some(*b"TEXT");
        e.creator_code = Some(*b"ttxt");

        let mut q = EditQueue::new();
        // Before staging: the on-disk values.
        assert_eq!(q.resolved_hfs_type_creator(&e), (*b"TEXT", *b"ttxt"));

        q.replace_set_type_creator(&e, *b"PICT", *b"8BIM");
        assert_eq!(q.resolved_hfs_type_creator(&e), (*b"PICT", *b"8BIM"));
    }

    /// `describe` renders one plain-language line per staged edit, in order.
    #[test]
    fn describe_lists_edits_in_order() {
        let dir = FileEntry::new_directory("d".into(), "/d".into(), 0);
        let f = FileEntry::new_file("f".into(), "/d/f".into(), 0, 0);

        let mut q = EditQueue::new();
        q.replace_set_permissions(&f, 0o600);
        q.push(StagedEdit::DeleteEntry {
            parent: dir,
            entry: f.clone(),
        });

        let lines = q.describe();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].starts_with("Permissions: /d/f -> 600"));
        assert!(lines[1].starts_with("Delete: /d/f"));
    }

    /// X5: a staged "new folder" whose name already exists as a folder is
    /// done, not a mid-batch error; a file in the way still is one.
    #[test]
    fn creating_a_directory_that_exists_is_a_no_op_but_a_file_in_the_way_fails() {
        use crate::fs::fat::{create_blank_fat, FatFilesystem};
        use crate::fs::filesystem::{CreateDirectoryOptions, CreateFileOptions, Filesystem};
        use std::io::Cursor;

        let img = create_blank_fat(2 * 1024 * 1024, Some("T")).unwrap();
        let mut fs = FatFilesystem::open(Cursor::new(img), 0).unwrap();
        let root = fs.root().unwrap();
        fs.create_directory(&root, "DOCS", &CreateDirectoryOptions::default())
            .unwrap();
        let mut d = Cursor::new(b"x".to_vec());
        fs.create_file(&root, "NOTE", &mut d, 1, &CreateFileOptions::default())
            .unwrap();

        let again = StagedEdit::CreateDirectory {
            parent: root.clone(),
            name: "DOCS".to_string(),
        };
        apply_edit(&mut fs, &again).expect("existing folder is a no-op");
        let dirs = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .filter(|e| e.name == "DOCS")
            .count();
        assert_eq!(dirs, 1, "no duplicate folder");

        let over_file = StagedEdit::CreateDirectory {
            parent: root.clone(),
            name: "NOTE".to_string(),
        };
        assert!(matches!(
            apply_edit(&mut fs, &over_file),
            Err(FilesystemError::AlreadyExists(_))
        ));
    }

    /// X7: the pre-scan looked at the disk only, so a rename or delete earlier
    /// in the same batch made it report the wrong conflicts.
    #[test]
    fn conflict_scan_replays_pending_renames_and_deletes() {
        use crate::fs::fat::{create_blank_fat, FatFilesystem};
        use crate::fs::filesystem::{CreateFileOptions, Filesystem};
        use crate::fs::replace::OnConflict;
        use std::io::Cursor;

        let img = create_blank_fat(2 * 1024 * 1024, Some("T")).unwrap();
        let mut fs = FatFilesystem::open(Cursor::new(img), 0).unwrap();
        let root = fs.root().unwrap();
        let mut d = Cursor::new(b"old".to_vec());
        fs.create_file(&root, "A.TXT", &mut d, 3, &CreateFileOptions::default())
            .unwrap();
        let a = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "A.TXT")
            .unwrap();
        let host = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(host.path(), b"new").unwrap();
        let add = |name: &str| StagedEdit::AddFile {
            parent: root.clone(),
            name: name.to_string(),
            host_path: host.path().to_path_buf(),
            size: 3,
            prodos_type: None,
            prodos_aux: None,
            resource_fork: None,
            hfs_type_override: None,
            hfs_creator_override: None,
            dates: None,
            on_conflict: OnConflict::Fail,
        };
        let names = |q: &EditQueue, fs: &mut FatFilesystem<Cursor<Vec<u8>>>| -> Vec<String> {
            q.conflicting_adds(fs).into_iter().map(|(_, n)| n).collect()
        };

        // Rename frees A.TXT and takes B.TXT.
        let mut q = EditQueue::new();
        q.push(StagedEdit::Rename {
            parent: root.clone(),
            entry: a.clone(),
            new_name: "B.TXT".to_string(),
        });
        q.push(add("A.TXT"));
        q.push(add("B.TXT"));
        assert_eq!(names(&q, &mut fs), vec!["B.TXT"]);

        // Delete frees A.TXT.
        let mut q = EditQueue::new();
        q.push(StagedEdit::DeleteEntry {
            parent: root.clone(),
            entry: a.clone(),
        });
        q.push(add("A.TXT"));
        assert!(names(&q, &mut fs).is_empty());

        // Order matters: an add before the rename still collides on disk.
        let mut q = EditQueue::new();
        q.push(add("A.TXT"));
        q.push(StagedEdit::Rename {
            parent: root.clone(),
            entry: a.clone(),
            new_name: "C.TXT".to_string(),
        });
        assert_eq!(names(&q, &mut fs), vec!["A.TXT"]);
    }

    /// Conflicts are found before anything is applied, so the user answers once
    /// instead of being interrupted per file mid-batch.
    #[test]
    fn conflicting_adds_are_detected_up_front_and_decided_per_file() {
        use crate::fs::fat::{create_blank_fat, FatFilesystem};
        use crate::fs::filesystem::{CreateFileOptions, Filesystem};
        use crate::fs::replace::OnConflict;
        use std::io::Cursor;

        let img = create_blank_fat(2 * 1024 * 1024, Some("T")).unwrap();
        let mut fs = FatFilesystem::open(Cursor::new(img), 0).unwrap();
        let root = fs.root().unwrap();
        // Two of the three names are already taken.
        for name in ["A.TXT", "B.TXT"] {
            let mut d = Cursor::new(b"old".to_vec());
            fs.create_file(&root, name, &mut d, 3, &CreateFileOptions::default())
                .unwrap();
        }

        let host = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(host.path(), b"new").unwrap();
        let mut q = EditQueue::new();
        for name in ["A.TXT", "B.TXT", "C.TXT"] {
            q.push(StagedEdit::AddFile {
                parent: root.clone(),
                name: name.to_string(),
                host_path: host.path().to_path_buf(),
                size: 3,
                prodos_type: None,
                prodos_aux: None,
                resource_fork: None,
                hfs_type_override: None,
                hfs_creator_override: None,
                dates: None,
                on_conflict: OnConflict::Fail,
            });
        }

        let conflicts = q.conflicting_adds(&mut fs);
        let names: Vec<&str> = conflicts.iter().map(|(_, n)| n.as_str()).collect();
        assert_eq!(names, vec!["A.TXT", "B.TXT"], "C.TXT is free");

        // Decisions are per file: replace one, skip the other.
        q.set_conflict_for(&conflicts[0].0, OnConflict::Replace);
        q.set_conflict_for(&conflicts[1].0, OnConflict::Skip);
        for edit in q.iter() {
            if let StagedEdit::AddFile {
                name, on_conflict, ..
            } = edit
            {
                let want = match name.as_str() {
                    "A.TXT" => OnConflict::Replace,
                    "B.TXT" => OnConflict::Skip,
                    _ => OnConflict::Fail,
                };
                assert_eq!(*on_conflict, want, "{name}");
            }
        }

        // And the blanket answer covers everything at once.
        q.set_all_conflicts(OnConflict::Replace);
        assert!(q.iter().all(|e| matches!(
            e,
            StagedEdit::AddFile {
                on_conflict: OnConflict::Replace,
                ..
            }
        )));
    }

    /// The GUI and Commander go through `apply_edit`, which used to call
    /// `create_file` with no existence check - so a name collision was simply
    /// an error from the driver, mid-batch. With `OnConflict::Replace` the same
    /// path now replaces, and carries the previous file's metadata across.
    #[test]
    fn apply_edit_can_replace_and_keeps_the_old_files_metadata() {
        use crate::fs::fat::{create_blank_fat, FatFilesystem};
        use crate::fs::filesystem::{CreateFileOptions, Filesystem};
        use std::io::Cursor;

        let img = create_blank_fat(2 * 1024 * 1024, Some("T")).unwrap();
        let mut fs = FatFilesystem::open(Cursor::new(img), 0).unwrap();

        // An existing file carrying attribute bits worth keeping.
        let root = fs.root().unwrap();
        let mut original = Cursor::new(b"original".to_vec());
        fs.create_file(
            &root,
            "A.TXT",
            &mut original,
            8,
            &CreateFileOptions {
                dos_attributes: Some(0x01), // read-only
                ..Default::default()
            },
        )
        .unwrap();

        let host = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(host.path(), b"replacement").unwrap();

        let edit = StagedEdit::AddFile {
            parent: fs.root().unwrap(),
            name: "A.TXT".to_string(),
            host_path: host.path().to_path_buf(),
            size: 11,
            prodos_type: None,
            prodos_aux: None,
            resource_fork: None,
            hfs_type_override: None,
            hfs_creator_override: None,
            dates: None,
            on_conflict: crate::fs::replace::OnConflict::Replace,
        };
        apply_edit(&mut fs, &edit).expect("replace should succeed");

        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert_eq!(entries.len(), 1, "no staging leftovers: {entries:?}");
        let e = &entries[0];
        assert_eq!(fs.read_file(e, usize::MAX).unwrap(), b"replacement");
        assert_eq!(
            e.dos_attributes.map(|a| a & 0x01),
            Some(0x01),
            "the read-only bit should have survived the replace"
        );
    }

    /// The default stays `Fail`, so every caller that has not been taught about
    /// conflicts keeps refusing rather than silently overwriting.
    #[test]
    fn apply_edit_still_refuses_a_collision_by_default() {
        use crate::fs::fat::{create_blank_fat, FatFilesystem};
        use crate::fs::filesystem::{CreateFileOptions, Filesystem};
        use std::io::Cursor;

        let img = create_blank_fat(2 * 1024 * 1024, Some("T")).unwrap();
        let mut fs = FatFilesystem::open(Cursor::new(img), 0).unwrap();
        let root = fs.root().unwrap();
        let mut original = Cursor::new(b"original".to_vec());
        fs.create_file(
            &root,
            "A.TXT",
            &mut original,
            8,
            &CreateFileOptions::default(),
        )
        .unwrap();

        let host = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(host.path(), b"replacement").unwrap();
        let edit = StagedEdit::AddFile {
            parent: fs.root().unwrap(),
            name: "A.TXT".to_string(),
            host_path: host.path().to_path_buf(),
            size: 11,
            prodos_type: None,
            prodos_aux: None,
            resource_fork: None,
            hfs_type_override: None,
            hfs_creator_override: None,
            dates: None,
            on_conflict: crate::fs::replace::OnConflict::default(),
        };
        assert!(apply_edit(&mut fs, &edit).is_err(), "default must refuse");

        let root = fs.root().unwrap();
        let e = &fs.list_directory(&root).unwrap()[0];
        assert_eq!(
            fs.read_file(e, usize::MAX).unwrap(),
            b"original",
            "the original must be untouched after a refused collision"
        );
    }

    /// "Keep original dates": an AddFile carrying a source Amiga datestamp
    /// reproduces it on the destination (via CreateFileOptions::amiga_dates)
    /// instead of stamping the current time — end to end through apply_edit.
    #[test]
    fn add_file_preserves_amiga_dates() {
        use crate::fs::affs::{create_blank_affs, AffsFilesystem};
        use crate::fs::filesystem::Filesystem;
        use std::io::Cursor;

        let img = create_blank_affs(880 * 1024, 1, "DATES").unwrap();
        let mut buf = img.clone();

        let host = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(host.path(), b"vintage payload").unwrap();
        let target = (1234i32, 56i32, 78i32); // (days, mins, ticks) since 1978

        {
            let mut efs = AffsFilesystem::open(Cursor::new(&mut buf), 0).unwrap();
            let root = efs.root().unwrap();
            apply_edit(
                &mut efs,
                &StagedEdit::AddFile {
                    parent: root,
                    name: "DATED".to_string(),
                    host_path: host.path().to_path_buf(),
                    size: 15,
                    prodos_type: None,
                    prodos_aux: None,
                    resource_fork: None,
                    hfs_type_override: None,
                    hfs_creator_override: None,
                    dates: Some(PreservedDates {
                        amiga: Some(target),
                        mac: None,
                        unix_mtime: None,
                    }),
                    on_conflict: crate::fs::replace::OnConflict::Fail,
                },
            )
            .unwrap();
            efs.sync_metadata().unwrap();
        }

        let mut fs = AffsFilesystem::open(Cursor::new(&mut buf), 0).unwrap();
        let root = fs.root().unwrap();
        let entry = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "DATED")
            .expect("DATED present");
        assert_eq!(
            entry.amiga_date,
            Some(target),
            "copied file kept the source's Amiga datestamp"
        );
    }
}
