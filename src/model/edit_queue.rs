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
    /// [`EditableFilesystem::set_permissions`]; ext-only today (other Unix
    /// filesystems return `Unsupported`).
    SetPermissions {
        entry: FileEntry,
        mode: u32,
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
        } => {
            let mut opts = CreateFileOptions {
                type_code: prodos_type.map(|t| format!("${:02X}", t)),
                aux_type: *prodos_aux,
                ..Default::default()
            };

            if let Some(imp) = rsrc_import {
                if !imp.data.is_empty() {
                    opts.resource_fork = Some(ResourceForkSource::Data(imp.data.clone()));
                }
                // Type/creator from container overrides auto-detect, but not
                // explicit ProDOS overrides.
                if opts.type_code.is_none() {
                    if let Some(tc) = imp.type_code {
                        opts.type_code = Some(String::from_utf8_lossy(&tc).to_string());
                    }
                }
                if opts.creator_code.is_none() {
                    if let Some(cc) = imp.creator_code {
                        opts.creator_code = Some(String::from_utf8_lossy(&cc).to_string());
                    }
                }
            }

            // Per-staged-file HFS overrides (from the inline editor) win over
            // both AppleDouble FInfo and the dictionary.
            if let Some(tc) = hfs_type_override {
                opts.type_code = Some(String::from_utf8_lossy(tc).to_string());
            }
            if let Some(cc) = hfs_creator_override {
                opts.creator_code = Some(String::from_utf8_lossy(cc).to_string());
            }

            // For MacBinary imports, use the extracted data fork instead of
            // the raw .bin file.
            if let Some(imp) = rsrc_import {
                if let Some(ref data_fork) = imp.data_fork {
                    let mut cursor = Cursor::new(data_fork);
                    let df_size = data_fork.len() as u64;
                    let resolved_parent = resolve_dir_by_path(efs, &parent.path)?;
                    efs.create_file(&resolved_parent, name, &mut cursor, df_size, &opts)?;
                    return Ok(());
                }
            }

            let mut file = File::open(host_path).map_err(FilesystemError::Io)?;
            let resolved_parent = resolve_dir_by_path(efs, &parent.path)?;
            efs.create_file(&resolved_parent, name, &mut file, *size, &opts)?;
            Ok(())
        }
        StagedEdit::CreateDirectory { parent, name } => {
            let resolved_parent = resolve_dir_by_path(efs, &parent.path)?;
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
                let dict = crate::fs::hfs_common::type_creator_for_extension(
                    name.rsplit('.').next().unwrap_or(""),
                );
                let t = hfs_type_override
                    .or(imp_t)
                    .or(dict.map(|(t, _)| t))
                    .unwrap_or([0; 4]);
                let c = hfs_creator_override
                    .or(imp_c)
                    .or(dict.map(|(_, c)| c))
                    .unwrap_or([0; 4]);
                return (t, c);
            }
        }
        let t = entry
            .type_code
            .as_deref()
            .map(crate::fs::hfs_common::encode_fourcc)
            .unwrap_or([0; 4]);
        let c = entry
            .creator_code
            .as_deref()
            .map(crate::fs::hfs_common::encode_fourcc)
            .unwrap_or([0; 4]);
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
}
