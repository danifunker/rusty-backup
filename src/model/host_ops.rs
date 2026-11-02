//! Immediate writes to a host folder: delete, rename, make a folder.
//!
//! Host panes never stage; these run as jobs straight against the OS. Each reports
//! plainly so a controller can word the outcome and re-list the folder.

use std::io;
use std::path::{Path, PathBuf};

/// What [`delete_paths`] did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DeleteOutcome {
    pub removed: usize,
    /// `(path, reason)` for each item that could not be removed.
    pub failed: Vec<(String, String)>,
}

/// Remove each path (a folder with its contents), carrying on past failures.
pub fn delete_paths(paths: &[PathBuf], mut on_item: impl FnMut(usize, &Path)) -> DeleteOutcome {
    let mut out = DeleteOutcome::default();
    for (i, path) in paths.iter().enumerate() {
        on_item(i, path);
        // symlink_metadata so a link to a folder is unlinked, never followed.
        let res = match std::fs::symlink_metadata(path) {
            Ok(m) if m.is_dir() => std::fs::remove_dir_all(path),
            Ok(_) => std::fs::remove_file(path),
            Err(e) => Err(e),
        };
        match res {
            Ok(()) => out.removed += 1,
            Err(e) => out.failed.push((path.display().to_string(), e.to_string())),
        }
    }
    out
}

/// Rename `old` within its folder; refuses to replace an existing name.
pub fn rename_entry(old: &Path, new_name: &str) -> io::Result<PathBuf> {
    check_name(new_name)?;
    let new = old.with_file_name(new_name);
    if new.symlink_metadata().is_ok() {
        return Err(exists(new_name));
    }
    std::fs::rename(old, &new)?;
    Ok(new)
}

/// Create `name` inside `parent`.
pub fn make_dir(parent: &Path, name: &str) -> io::Result<PathBuf> {
    check_name(name)?;
    let dir = parent.join(name);
    if dir.symlink_metadata().is_ok() {
        return Err(exists(name));
    }
    std::fs::create_dir(&dir)?;
    Ok(dir)
}

/// Why `name` cannot be a single host path component, if it cannot.
pub fn host_name_error(name: &str) -> Option<&'static str> {
    if name.is_empty() {
        Some("The name is empty.")
    } else if name == "." || name == ".." {
        Some("That name is reserved.")
    } else if name.contains('/') || name.contains('\0') || (cfg!(windows) && name.contains('\\')) {
        Some("The name contains a path separator.")
    } else {
        None
    }
}

fn check_name(name: &str) -> io::Result<()> {
    match host_name_error(name) {
        Some(why) => Err(io::Error::new(io::ErrorKind::InvalidInput, why)),
        None => Ok(()),
    }
}

fn exists(name: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!("'{name}' already exists"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn make_dir_creates_and_refuses_an_existing_name() {
        let d = tempfile::tempdir().unwrap();
        let made = make_dir(d.path(), "new").unwrap();
        assert!(made.is_dir());
        let err = make_dir(d.path(), "new").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        assert!(make_dir(d.path(), "a/b").is_err());
        assert!(make_dir(d.path(), "..").is_err());
    }

    #[test]
    fn rename_moves_within_the_folder_and_never_replaces() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(d.path().join("a.txt"), b"a").unwrap();
        std::fs::write(d.path().join("b.txt"), b"b").unwrap();
        let err = rename_entry(&d.path().join("a.txt"), "b.txt").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(d.path().join("b.txt")).unwrap(), b"b");
        let new = rename_entry(&d.path().join("a.txt"), "c.txt").unwrap();
        assert_eq!(new, d.path().join("c.txt"));
        assert!(!d.path().join("a.txt").exists());
    }

    #[test]
    fn delete_removes_files_and_folders_and_reports_the_rest() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(d.path().join("f"), b"x").unwrap();
        std::fs::create_dir_all(d.path().join("dir").join("inner")).unwrap();
        std::fs::write(d.path().join("dir").join("inner").join("g"), b"y").unwrap();
        let paths = vec![
            d.path().join("f"),
            d.path().join("dir"),
            d.path().join("missing"),
        ];
        let mut seen = 0;
        let out = delete_paths(&paths, |_, _| seen += 1);
        assert_eq!(seen, 3);
        assert_eq!(out.removed, 2);
        assert_eq!(out.failed.len(), 1);
        assert!(out.failed[0].0.ends_with("missing"));
        assert!(!d.path().join("f").exists() && !d.path().join("dir").exists());
    }
}
