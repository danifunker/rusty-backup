//! Tree-view listing for any filesystem implementing the `Filesystem` trait.
//!
//! Produces output similar to the GNU `tree` utility. Two variants:
//! - `format_tree` — file/directory names with sizes
//! - `format_tree_with_ids` — same, plus filesystem IDs (CNID for HFS, inode for ext, etc.)

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};
use crate::partition::format_size;

/// Format a GNU tree-style listing of all files and directories.
///
/// File sizes include the resource fork (if any). Directories show no size.
/// The output ends with a summary line: `N directories, M files`.
pub fn format_tree(fs: &mut dyn Filesystem) -> Result<String, FilesystemError> {
    let root = fs.root()?;
    let label = fs.volume_label().unwrap_or("/").to_owned();
    let mut out = String::new();
    out.push_str(&label);
    out.push('\n');
    let mut dir_count: u64 = 0;
    let mut file_count: u64 = 0;
    walk_tree(
        fs,
        &root,
        "",
        &mut out,
        false,
        &mut dir_count,
        &mut file_count,
    )?;
    out.push_str(&format!(
        "\n{} directories, {} files\n",
        dir_count, file_count
    ));
    Ok(out)
}

/// Like `format_tree`, but each entry also shows its filesystem ID
/// (e.g. CNID for HFS/HFS+, inode for ext, cluster for FAT).
pub fn format_tree_with_ids(fs: &mut dyn Filesystem) -> Result<String, FilesystemError> {
    let root = fs.root()?;
    let label = fs.volume_label().unwrap_or("/").to_owned();
    let mut out = String::new();
    out.push_str(&format!("{label}  (ID: {})", root.location));
    out.push('\n');
    let mut dir_count: u64 = 0;
    let mut file_count: u64 = 0;
    walk_tree(
        fs,
        &root,
        "",
        &mut out,
        true,
        &mut dir_count,
        &mut file_count,
    )?;
    out.push_str(&format!(
        "\n{} directories, {} files\n",
        dir_count, file_count
    ));
    Ok(out)
}

fn walk_tree(
    fs: &mut dyn Filesystem,
    dir: &FileEntry,
    prefix: &str,
    out: &mut String,
    show_ids: bool,
    dir_count: &mut u64,
    file_count: &mut u64,
) -> Result<(), FilesystemError> {
    let children = fs.list_directory(dir)?;
    let count = children.len();

    for (i, child) in children.iter().enumerate() {
        let is_last = i == count - 1;
        let connector = if is_last { "└── " } else { "├── " };

        out.push_str(prefix);
        out.push_str(connector);
        out.push_str(&child.name);

        if child.is_file() || child.is_symlink() {
            let total_size = child.size + child.resource_fork_size.unwrap_or(0);
            out.push_str(&format!("  [{}]", format_size(total_size)));
        }

        if show_ids {
            out.push_str(&format!("  (ID: {})", child.location));
        }

        out.push('\n');

        if child.is_directory() {
            *dir_count += 1;
            let child_prefix = if is_last {
                format!("{prefix}    ")
            } else {
                format!("{prefix}│   ")
            };
            walk_tree(
                fs,
                child,
                &child_prefix,
                out,
                show_ids,
                dir_count,
                file_count,
            )?;
        } else {
            *file_count += 1;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::entry::EntryType;

    struct MockFs;

    impl Filesystem for MockFs {
        fn root(&mut self) -> Result<FileEntry, FilesystemError> {
            Ok(FileEntry {
                name: "/".into(),
                path: "/".into(),
                entry_type: EntryType::Directory,
                size: 0,
                location: 2,
                modified: None,
                type_code: None,
                creator_code: None,
                symlink_target: None,
                special_type: None,
                mode: None,
                uid: None,
                gid: None,
                resource_fork_size: None,
            })
        }

        fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
            match entry.path.as_str() {
                "/" => Ok(vec![
                    FileEntry {
                        name: "Documents".into(),
                        path: "/Documents".into(),
                        entry_type: EntryType::Directory,
                        size: 0,
                        location: 10,
                        modified: None,
                        type_code: None,
                        creator_code: None,
                        symlink_target: None,
                        special_type: None,
                        mode: None,
                        uid: None,
                        gid: None,
                        resource_fork_size: None,
                    },
                    FileEntry {
                        name: "ReadMe".into(),
                        path: "/ReadMe".into(),
                        entry_type: EntryType::File,
                        size: 1024,
                        location: 20,
                        modified: None,
                        type_code: None,
                        creator_code: None,
                        symlink_target: None,
                        special_type: None,
                        mode: None,
                        uid: None,
                        gid: None,
                        resource_fork_size: Some(512),
                    },
                ]),
                "/Documents" => Ok(vec![FileEntry {
                    name: "Report.doc".into(),
                    path: "/Documents/Report.doc".into(),
                    entry_type: EntryType::File,
                    size: 2048,
                    location: 30,
                    modified: None,
                    type_code: None,
                    creator_code: None,
                    symlink_target: None,
                    special_type: None,
                    mode: None,
                    uid: None,
                    gid: None,
                    resource_fork_size: None,
                }]),
                _ => Ok(vec![]),
            }
        }

        fn read_file(
            &mut self,
            _entry: &FileEntry,
            _max_bytes: usize,
        ) -> Result<Vec<u8>, FilesystemError> {
            Ok(vec![])
        }

        fn volume_label(&self) -> Option<&str> {
            Some("TestVolume")
        }

        fn fs_type(&self) -> &str {
            "Mock"
        }

        fn total_size(&self) -> u64 {
            10_000_000
        }

        fn used_size(&self) -> u64 {
            5_000_000
        }
    }

    #[test]
    fn test_format_tree() {
        let mut fs = MockFs;
        let result = format_tree(&mut fs).unwrap();
        let expected = "\
TestVolume
├── Documents
│   └── Report.doc  [2.0 KiB]
└── ReadMe  [1.5 KiB]

1 directories, 2 files\n";
        assert_eq!(result, expected);
    }

    #[test]
    #[ignore] // Run manually: cargo test test_tree_real_hfs -- --ignored --nocapture
    fn test_tree_real_hfs() {
        use std::io::BufReader;
        let path = std::env::var("HFS_IMAGE").unwrap_or_else(|_| {
            let home = std::env::var("HOME").unwrap();
            format!("{home}/Documents/HD40_imagedPowerMac6100.hda")
        });
        let f = std::fs::File::open(&path).expect("cannot open image");
        let reader = BufReader::new(f);
        // APM partition offset: block 96 * 512 = 49152
        let mut fs = crate::fs::hfs::HfsFilesystem::open(reader, 49152).expect("cannot open HFS");
        let tree = format_tree(&mut fs).unwrap();
        println!("{tree}");
        let tree_ids = format_tree_with_ids(&mut fs).unwrap();
        println!("{tree_ids}");
    }

    #[test]
    fn test_format_tree_with_ids() {
        let mut fs = MockFs;
        let result = format_tree_with_ids(&mut fs).unwrap();
        assert!(result.contains("(ID: 2)"), "root should show ID");
        assert!(result.contains("Documents  (ID: 10)"));
        assert!(result.contains("ReadMe  [1.5 KiB]  (ID: 20)"));
        assert!(result.contains("Report.doc  [2.0 KiB]  (ID: 30)"));
    }
}
