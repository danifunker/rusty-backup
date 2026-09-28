//! Rename staged copy-ins whose names the destination filesystem can't hold.
//!
//! A cross-volume copy stages `CreateDirectory` / `AddFile` edits carrying the
//! source names verbatim. [`legalize_staged_names`] runs each name through
//! [`legalize_name`] against the destination's `validate_name`, picks a free
//! name with [`unique_name`] when the repaired name is already used in that
//! folder, and rewrites the `parent` path of every descendant so the subtree
//! still resolves under the renamed folder. The returned [`NameChange`] list is
//! what the UI shows the user (with the `.mar` export tip).

use std::collections::HashMap;

use crate::fs::entry::FileEntry;
use crate::fs::filesystem::FilesystemError;
use crate::fs::name_legalize::{legalize_name, unique_name, NameValidator};
use crate::model::edit_queue::StagedEdit;

/// One staged name that was rewritten for the destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NameChange {
    /// Destination folder path (after any renamed ancestors).
    pub parent: String,
    pub from: String,
    pub to: String,
}

/// What [`legalize_staged_names`] did to a batch.
#[derive(Debug, Default)]
pub struct LegalizeReport {
    pub renamed: Vec<NameChange>,
    /// Items with no legal name at all, dropped with their subtree: `(source path, reason)`.
    pub dropped: Vec<(String, String)>,
}

/// Legalize the names in `edits` (a freshly staged copy batch) in place.
///
/// `existing` lists the names already in a destination folder by path; it is
/// only asked for folders where a rename happened, so it may be slow.
pub fn legalize_staged_names(
    edits: &mut Vec<StagedEdit>,
    validate: &NameValidator<'_>,
    fold_case: bool,
    existing: &mut dyn FnMut(&str) -> Vec<String>,
) -> LegalizeReport {
    let mut report = LegalizeReport::default();
    // Old full folder path -> new full folder path, for re-parenting descendants.
    let mut moved: HashMap<String, String> = HashMap::new();
    // Folder path -> names already present or claimed by this batch.
    let mut claimed: HashMap<String, (bool, Vec<String>)> = HashMap::new();
    let mut dropped_dirs: Vec<String> = Vec::new();
    let mut keep = vec![true; edits.len()];

    for (i, edit) in edits.iter_mut().enumerate() {
        let (parent, name, is_dir) = match edit {
            StagedEdit::AddFile { parent, name, .. } => (parent, name, false),
            StagedEdit::CreateDirectory { parent, name } => (parent, name, true),
            _ => continue,
        };
        let old_parent = parent.path.clone();
        let old_full = join(&old_parent, name);
        if dropped_dirs.contains(&old_parent) {
            keep[i] = false;
            if is_dir {
                dropped_dirs.push(old_full);
            }
            continue;
        }
        if let Some(new_parent) = moved.get(&old_parent) {
            *parent = FileEntry::new_directory(leaf(new_parent), new_parent.clone(), 0);
        }
        let new_name = match legalize_name(validate, name) {
            Ok(n) => n,
            Err(e) => {
                report.dropped.push((old_full.clone(), describe(&e)));
                keep[i] = false;
                if is_dir {
                    dropped_dirs.push(old_full);
                }
                continue;
            }
        };
        let (seeded, taken) = claimed.entry(parent.path.clone()).or_default();
        let final_name = if new_name == *name {
            new_name
        } else {
            if !*seeded {
                taken.extend(existing(&parent.path));
                *seeded = true;
            }
            let clash = |n: &str| {
                taken
                    .iter()
                    .any(|t| t == n || (fold_case && t.eq_ignore_ascii_case(n)))
            };
            unique_name(validate, &new_name, &clash).unwrap_or(new_name)
        };
        taken.push(final_name.clone());
        if final_name != *name {
            report.renamed.push(NameChange {
                parent: parent.path.clone(),
                from: name.clone(),
                to: final_name.clone(),
            });
            *name = final_name;
        }
        if is_dir {
            let new_full = join(&parent.path, name);
            if new_full != old_full {
                moved.insert(old_full, new_full);
            }
        }
    }
    let mut it = keep.into_iter();
    edits.retain(|_| it.next().unwrap_or(true));
    report
}

fn join(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{name}")
    } else {
        format!("{parent}/{name}")
    }
}

/// The last component of a folder path; a slash-bearing name only affects the display label.
fn leaf(path: &str) -> String {
    path.rsplit('/').next().unwrap_or("").to_string()
}

fn describe(e: &FilesystemError) -> String {
    match e {
        FilesystemError::InvalidData(s) | FilesystemError::Unsupported(s) => s.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::name_legalize::validate_amiga_name;
    use std::path::PathBuf;

    fn dir(parent: &str, name: &str) -> StagedEdit {
        StagedEdit::CreateDirectory {
            parent: FileEntry::new_directory(leaf(parent), parent.into(), 0),
            name: name.into(),
        }
    }

    fn file(parent: &str, name: &str) -> StagedEdit {
        StagedEdit::AddFile {
            parent: FileEntry::new_directory(leaf(parent), parent.into(), 0),
            name: name.into(),
            host_path: PathBuf::new(),
            size: 0,
            prodos_type: None,
            prodos_aux: None,
            resource_fork: None,
            hfs_type_override: None,
            hfs_creator_override: None,
            dates: None,
            on_conflict: crate::fs::replace::OnConflict::Fail,
        }
    }

    fn target(e: &StagedEdit) -> (String, String) {
        match e {
            StagedEdit::AddFile { parent, name, .. }
            | StagedEdit::CreateDirectory { parent, name } => (parent.path.clone(), name.clone()),
            _ => unreachable!(),
        }
    }

    #[test]
    fn renamed_folders_carry_their_subtree() {
        let v = |n: &str| validate_amiga_name(n, 30, "SFS");
        let mut edits = vec![
            dir("/", "Acquire/Export"),
            dir("/Acquire/Export", "Sub"),
            file("/Acquire/Export/Sub", "a:b"),
            file("/Acquire/Export", "ok"),
        ];
        let report = legalize_staged_names(&mut edits, &v, true, &mut |_| Vec::new());
        let got: Vec<_> = edits.iter().map(target).collect();
        assert_eq!(
            got,
            [
                ("/".into(), "Acquire_Export".into()),
                ("/Acquire_Export".into(), "Sub".into()),
                ("/Acquire_Export/Sub".into(), "a_b".into()),
                ("/Acquire_Export".into(), "ok".into()),
            ]
        );
        assert_eq!(report.renamed.len(), 2);
        assert!(report.dropped.is_empty());
    }

    #[test]
    fn repaired_names_never_collide() {
        let v = |n: &str| validate_amiga_name(n, 30, "SFS");
        let mut edits = vec![file("/", "a:b"), file("/", "a/b")];
        legalize_staged_names(&mut edits, &v, true, &mut |_| vec!["A_B".into()]);
        let names: Vec<_> = edits.iter().map(|e| target(e).1).collect();
        assert_eq!(names, ["a_b_1", "a_b_2"]);
    }

    #[test]
    fn unrepairable_items_drop_with_their_subtree() {
        let v = |n: &str| match n {
            "keep" => Ok(()),
            _ => Err(FilesystemError::InvalidData("read-only".into())),
        };
        let mut edits = vec![dir("/", "x"), file("/x", "inner"), file("/", "keep")];
        let report = legalize_staged_names(&mut edits, &v, false, &mut |_| Vec::new());
        assert_eq!(edits.len(), 1);
        assert_eq!(target(&edits[0]).1, "keep");
        assert_eq!(report.dropped.len(), 1);
    }
}
