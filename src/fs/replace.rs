//! Creating a file that may already exist, without losing what was there.
//!
//! Every filesystem here implements "replace" as delete-then-create, which has
//! two consequences worth handling in one place rather than three:
//!
//! * The old file's metadata is gone the instant it is deleted, so anything
//!   worth keeping has to be captured first ([`super::attrs::PreservedMeta`]).
//! * If the write fails after the delete, the original is gone and the
//!   replacement is incomplete. On a config file being edited in place that is
//!   the worst possible outcome.
//!
//! So a replace is staged instead: write the new contents under a temporary
//! name, move the original aside, put the new file in place, and only then drop
//! the original. Every step before the final move is reversible, and the step
//! that decides whether the filesystem can do this at all happens while the
//! original is still untouched. Drivers without a working `rename` fall back to
//! delete-then-create, which is what the CLI did unconditionally before.
//!
//! Shared deliberately: the CLI's `put`, the staged-edit applier behind the GUI
//! and Commander, and the TUI's import all need identical semantics, and
//! `CONTRIBUTING.md` is explicit that one operation gets one implementation.

use super::attrs::{preserved_meta, PreservedMeta};
use super::entry::FileEntry;
use super::filesystem::{CreateFileOptions, EditableFilesystem, FilesystemError};

/// What to do when the destination name is already taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnConflict {
    /// Refuse, leaving the existing file alone. The safe default, and what
    /// every caller did before replacing was possible.
    #[default]
    Fail,
    /// Replace the existing file, keeping its metadata unless
    /// `preserve_meta` is false.
    Replace,
    /// Leave the existing file alone and report that nothing was written.
    Skip,
}

/// How a write should behave when the destination already exists.
#[derive(Debug, Clone, Copy)]
pub struct ReplacePolicy {
    pub on_conflict: OnConflict,
    /// Carry the replaced file's metadata onto the replacement. On by default,
    /// because a replace changes contents, not who may read the file.
    pub preserve_meta: bool,
}

impl Default for ReplacePolicy {
    fn default() -> Self {
        Self {
            on_conflict: OnConflict::Fail,
            preserve_meta: true,
        }
    }
}

impl ReplacePolicy {
    /// Replace an existing file, keeping what it carried.
    pub fn replace() -> Self {
        Self {
            on_conflict: OnConflict::Replace,
            ..Self::default()
        }
    }

    /// Replace an existing file, starting its metadata fresh.
    pub fn replace_fresh() -> Self {
        Self {
            on_conflict: OnConflict::Replace,
            preserve_meta: false,
        }
    }
}

/// What [`create_or_replace`] actually did.
#[derive(Debug)]
pub struct ReplaceOutcome {
    /// The entry now at that name. `None` when the write was skipped.
    pub created: Option<FileEntry>,
    /// An existing file was replaced.
    pub replaced: bool,
    /// The destination existed and [`OnConflict::Skip`] was in force.
    pub skipped: bool,
    /// What was carried over from the replaced file, for the caller to report.
    pub preserved: Option<PreservedMeta>,
    /// The replace could not be staged (no usable `rename`) and fell back to
    /// delete-then-create, so a failure mid-write would have lost the original.
    /// Worth saying out loud on a filesystem where it applies.
    pub unsafe_fallback: bool,
}

/// Create `name` in `parent`, honouring `on_conflict` if it already exists.
///
/// `opts` is used as given for a fresh file. For a replace, the preserved
/// metadata fills in only the fields the caller left unset — an explicit
/// `--type` or `--mode` is an instruction and outranks whatever the old file
/// carried.
pub fn create_or_replace(
    efs: &mut dyn EditableFilesystem,
    parent: &FileEntry,
    name: &str,
    data: &mut dyn std::io::Read,
    data_len: u64,
    opts: &CreateFileOptions,
    policy: ReplacePolicy,
) -> Result<ReplaceOutcome, FilesystemError> {
    let ReplacePolicy {
        on_conflict,
        preserve_meta,
    } = policy;
    let fold_case = efs.case_insensitive_lookup();
    let siblings = efs.list_directory(parent)?;
    let existing = crate::fs::copy::select_child(&siblings, fold_case, name).cloned();

    let Some(existing) = existing else {
        let created = efs.create_file(parent, name, data, data_len, opts)?;
        return Ok(ReplaceOutcome {
            created: Some(created),
            replaced: false,
            skipped: false,
            preserved: None,
            unsafe_fallback: false,
        });
    };

    match on_conflict {
        OnConflict::Fail => {
            return Err(FilesystemError::InvalidData(format!(
                "{name} already exists"
            )))
        }
        OnConflict::Skip => {
            return Ok(ReplaceOutcome {
                created: None,
                replaced: false,
                skipped: true,
                preserved: None,
                unsafe_fallback: false,
            })
        }
        OnConflict::Replace => {}
    }

    // Capture before anything is destroyed; afterwards there is nothing to ask.
    let preserved = if preserve_meta {
        preserved_meta(efs.as_filesystem_mut(), Some(&existing))
    } else {
        PreservedMeta::default()
    };
    let mut opts = opts.clone();
    preserved.apply_to_options(&mut opts);

    let taken: Vec<String> = siblings.iter().map(|e| e.name.clone()).collect();
    let staged = staged_replace(efs, parent, name, &existing, data, data_len, &opts, &taken)?;

    // Whether the crash-safe path was available is known here and nowhere else:
    // `staged_replace` returns `None` precisely when it declined.
    let unsafe_fallback = staged.is_none();
    let created = match staged {
        Some(entry) => entry,
        None => {
            // No usable rename: the original has to go first. This is the old
            // behaviour, and the window where a failure loses the file.
            efs.delete_entry(parent, &existing)?;
            efs.create_file(parent, name, data, data_len, &opts)?
        }
    };

    preserved.reapply_dates(efs, &created);

    Ok(ReplaceOutcome {
        created: Some(created),
        replaced: true,
        skipped: false,
        preserved: Some(preserved),
        unsafe_fallback,
    })
}

/// Try the crash-safe path. `Ok(None)` means this filesystem cannot stage a
/// replace and the caller should fall back, with the original still intact.
///
/// Order matters, and this order is the point:
/// 1. write the new contents under a temp name — the long, failure-prone step,
///    with the original still present and untouched;
/// 2. move the original aside. **This is where an unsupported `rename` is
///    discovered**, while nothing has been lost yet;
/// 3. move the new file into place;
/// 4. drop the original.
///
/// A failure at step 3 puts the original back before returning.
#[allow(clippy::too_many_arguments)]
fn staged_replace(
    efs: &mut dyn EditableFilesystem,
    parent: &FileEntry,
    name: &str,
    existing: &FileEntry,
    data: &mut dyn std::io::Read,
    data_len: u64,
    opts: &CreateFileOptions,
    taken: &[String],
) -> Result<Option<FileEntry>, FilesystemError> {
    let Some(tmp_new) = temp_name(taken, 0) else {
        return Ok(None);
    };
    let Some(tmp_old) = temp_name(taken, 1) else {
        return Ok(None);
    };

    // 1. New contents under a temp name. A driver that rejects the name (an
    // 8.3-only volume with an awkward rule, say) is a reason to fall back, not
    // to fail: nothing has been touched yet.
    let staged = match efs.create_file(parent, &tmp_new, data, data_len, opts) {
        Ok(e) => e,
        Err(_) => return Ok(None),
    };

    // 2. Original aside. Unsupported rename is discovered here, with the
    // original still in place and only a temp file to clean up.
    if efs.rename(parent, existing, &tmp_old).is_err() {
        let _ = efs.delete_entry(parent, &staged);
        return Ok(None);
    }

    // 3. New file into place. Re-read the staged entry first: a driver's
    // `rename` re-stamps attributes from the entry it is given, and the handle
    // `create_file` returns does not necessarily carry them (FAT's does not),
    // so renaming the stale handle silently reset the read-only / hidden bits
    // the replace had just preserved. The directory listing has them.
    let staged = current_entry(efs, parent, &tmp_new).unwrap_or(staged);
    // If this fails the original goes back, so the caller is no worse off.
    if let Err(e) = efs.rename(parent, &staged, name) {
        let restored = current_entry(efs, parent, &tmp_old);
        if let Some(orig) = restored {
            let _ = efs.rename(parent, &orig, name);
        }
        let _ = current_entry(efs, parent, &tmp_new).map(|s| efs.delete_entry(parent, &s));
        return Err(e);
    }

    // 4. The original is now redundant. Failing to remove it leaves a stray
    // temp file, which is untidy but not lossy, so it is not worth failing the
    // whole write over.
    if let Some(old) = current_entry(efs, parent, &tmp_old) {
        let _ = efs.delete_entry(parent, &old);
    }

    Ok(current_entry(efs, parent, name).or(Some(staged)))
}

/// Re-read an entry by name; a rename invalidates the handle we were holding.
fn current_entry(
    efs: &mut dyn EditableFilesystem,
    parent: &FileEntry,
    name: &str,
) -> Option<FileEntry> {
    efs.list_directory(parent)
        .ok()?
        .into_iter()
        .find(|e| e.name == name)
}

/// A temp name that will survive the strictest filesystem here.
///
/// Six uppercase alphanumerics with no extension: valid as a FAT short name, a
/// CP/M name, and everything in between. `slot` separates the two temporaries a
/// staged replace needs at once.
fn temp_name(taken: &[String], slot: usize) -> Option<String> {
    for n in 0..10 {
        let cand = format!("RBT{slot}{n:02}");
        if !taken.iter().any(|t| t.eq_ignore_ascii_case(&cand)) {
            return Some(cand);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::fat::{create_blank_fat, FatFilesystem};
    use crate::fs::filesystem::Filesystem;
    use std::io::Cursor;

    fn blank() -> FatFilesystem<Cursor<Vec<u8>>> {
        let img = create_blank_fat(4 * 1024 * 1024, Some("T")).expect("blank");
        FatFilesystem::open(Cursor::new(img), 0).expect("open")
    }

    fn put(
        fs: &mut FatFilesystem<Cursor<Vec<u8>>>,
        name: &str,
        body: &[u8],
        on: OnConflict,
    ) -> ReplaceOutcome {
        let root = fs.root().expect("root");
        let mut data = Cursor::new(body.to_vec());
        create_or_replace(
            fs,
            &root,
            name,
            &mut data,
            body.len() as u64,
            &CreateFileOptions::default(),
            ReplacePolicy {
                on_conflict: on,
                ..Default::default()
            },
        )
        .expect("create_or_replace")
    }

    fn read_back(fs: &mut FatFilesystem<Cursor<Vec<u8>>>, name: &str) -> Vec<u8> {
        let root = fs.root().unwrap();
        let entry = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name.eq_ignore_ascii_case(name))
            .expect("entry present");
        fs.read_file(&entry, usize::MAX).expect("read")
    }

    #[test]
    fn a_fresh_name_is_just_created() {
        let mut fs = blank();
        let out = put(&mut fs, "NEW.TXT", b"one", OnConflict::Fail);
        assert!(!out.replaced && !out.skipped);
        assert_eq!(read_back(&mut fs, "NEW.TXT"), b"one");
    }

    #[test]
    fn fail_refuses_and_leaves_the_original_alone() {
        let mut fs = blank();
        put(&mut fs, "A.TXT", b"original", OnConflict::Fail);
        let root = fs.root().unwrap();
        let mut data = Cursor::new(b"replacement".to_vec());
        let err = create_or_replace(
            &mut fs,
            &root,
            "A.TXT",
            &mut data,
            11,
            &CreateFileOptions::default(),
            ReplacePolicy::default(),
        )
        .expect_err("must refuse");
        assert!(err.to_string().contains("already exists"), "{err}");
        assert_eq!(read_back(&mut fs, "A.TXT"), b"original");
    }

    #[test]
    fn skip_leaves_the_original_and_reports_it() {
        let mut fs = blank();
        put(&mut fs, "A.TXT", b"original", OnConflict::Fail);
        let out = put(&mut fs, "A.TXT", b"replacement", OnConflict::Skip);
        assert!(out.skipped && !out.replaced);
        assert!(out.created.is_none());
        assert_eq!(read_back(&mut fs, "A.TXT"), b"original");
    }

    #[test]
    fn replace_swaps_the_contents_and_leaves_no_temp_files() {
        let mut fs = blank();
        put(&mut fs, "A.TXT", b"original", OnConflict::Fail);
        let out = put(&mut fs, "A.TXT", b"replacement", OnConflict::Replace);
        assert!(out.replaced && !out.skipped);
        assert_eq!(read_back(&mut fs, "A.TXT"), b"replacement");

        // The staging temporaries must not survive: a stray RBT000 next to the
        // file would be the visible symptom of a half-finished replace.
        let root = fs.root().unwrap();
        let names: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(
            !names.iter().any(|n| n.to_uppercase().starts_with("RBT")),
            "staging temporaries left behind: {names:?}"
        );
        assert_eq!(names.len(), 1, "exactly the one file: {names:?}");
    }

    /// The replaced file's metadata must come across - this is what makes
    /// fixing one line of a config file safe.
    #[test]
    fn replace_carries_the_previous_files_dos_attributes() {
        let mut fs = blank();
        let root = fs.root().unwrap();
        let mut data = Cursor::new(b"original".to_vec());
        let opts = CreateFileOptions {
            // Read-only + hidden.
            dos_attributes: Some(0x03),
            ..Default::default()
        };
        create_or_replace(
            &mut fs,
            &root,
            "A.TXT",
            &mut data,
            8,
            &opts,
            ReplacePolicy::default(),
        )
        .expect("create");

        let out = put(&mut fs, "A.TXT", b"replacement", OnConflict::Replace);
        let preserved = out.preserved.expect("something was preserved");
        assert_eq!(
            preserved.dos_attributes,
            Some(0x03),
            "DOS attribute bits should have been captured before the delete"
        );
        assert_eq!(read_back(&mut fs, "A.TXT"), b"replacement");

        // Capturing them is not the same as landing them, and asserting only
        // the captured struct is what let a real bug through: a driver's
        // `rename` re-stamps attributes from the entry it is handed, so the
        // staged file went in with the default Archive bit and the read-only
        // bit was quietly lost. Assert the file on disk.
        let root = fs.root().unwrap();
        let landed = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name.eq_ignore_ascii_case("A.TXT"))
            .expect("the replacement is there");
        assert_eq!(
            landed.dos_attributes.map(|a| a & 0x03),
            Some(0x03),
            "read-only + hidden must be set on the file that is actually on disk"
        );
    }

    #[test]
    fn temp_names_avoid_collisions_and_stay_short() {
        let taken: Vec<String> = vec!["RBT000".into(), "RBT001".into()];
        let n = temp_name(&taken, 0).expect("a free slot");
        assert_eq!(n, "RBT002");
        assert!(n.len() <= 8, "must fit an 8.3 short name: {n}");
        assert!(n.chars().all(|c| c.is_ascii_alphanumeric()));
        // The two staging names never collide with each other.
        assert_ne!(temp_name(&[], 0), temp_name(&[], 1));
    }
}
