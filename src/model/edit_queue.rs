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
use crate::fs::resource_fork;

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
    SetProdosType {
        entry: FileEntry,
        type_byte: u8,
        aux_type: u16,
    },
    BlessFolder {
        entry: FileEntry,
    },
    /// Set HFS/HFS+ type and creator codes on an existing on-disk file.
    SetTypeCreator {
        entry: FileEntry,
        type_code: [u8; 4],
        creator_code: [u8; 4],
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
        StagedEdit::SetProdosType {
            entry,
            type_byte,
            aux_type,
        } => efs.set_prodos_type(entry, *type_byte, *aux_type),
        StagedEdit::BlessFolder { entry } => efs.set_blessed_folder(entry),
        StagedEdit::SetTypeCreator {
            entry,
            type_code,
            creator_code,
        } => efs.set_type_creator(
            entry,
            &String::from_utf8_lossy(type_code),
            &String::from_utf8_lossy(creator_code),
        ),
    }
}
