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

/// Replace ASCII control characters with Unicode Control Pictures (U+2400–U+241F)
/// so they render as visible glyphs instead of disrupting the tree layout.
fn display_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_control() {
                // Unicode Control Pictures block: U+2400 + codepoint
                char::from_u32(0x2400 + c as u32).unwrap_or('\u{FFFD}')
            } else {
                c
            }
        })
        .collect()
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
        out.push_str(&display_name(&child.name));

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
                aux_type: None,
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
                        aux_type: None,
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
                        aux_type: None,
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
                    aux_type: None,
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
    #[ignore] // Run: cargo test test_repair_diag -- --ignored --nocapture
    fn test_repair_diag() {
        use std::io::BufReader;

        let orig = std::env::var("HFS_IMAGE").unwrap_or_else(|_| {
            let home = std::env::var("HOME").unwrap();
            format!("{home}/Documents/HD40_imagedPowerMac6100.hda")
        });
        let work = "/tmp/HD40_repair_test.hda";
        let offset: u64 = 49152;

        std::fs::copy(&orig, work).expect("copy failed");
        println!("Working copy: {work}");

        // --- BEFORE ---
        println!("\n=== BEFORE REPAIR ===");
        {
            let f = BufReader::new(std::fs::File::open(work).unwrap());
            let mut fs = crate::fs::hfs::HfsFilesystem::open(f, offset).expect("cannot open HFS");
            let tree = format_tree_with_ids(&mut fs).unwrap();
            std::fs::write("/tmp/tree_before.txt", &tree).unwrap();
            println!("Tree saved to /tmp/tree_before.txt");
            if let Some((cnid, name)) =
                <crate::fs::hfs::HfsFilesystem<_> as crate::fs::filesystem::Filesystem>::blessed_system_folder(&mut fs)
            {
                println!("Blessed folder: {name} (CNID {cnid})");
            } else {
                println!("Blessed folder: NONE");
            }
        }

        // --- FSCK BEFORE ---
        println!("\n=== FSCK BEFORE ===");
        {
            let f = BufReader::new(std::fs::File::open(work).unwrap());
            let mut fs = crate::fs::hfs::HfsFilesystem::open(f, offset).unwrap();
            let result = fs.fsck().unwrap();
            print_fsck(&result);
            std::fs::write("/tmp/fsck_before.txt", format_fsck(&result)).unwrap();
        }

        // --- REPAIR ---
        println!("\n=== RUNNING REPAIR ===");
        {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(work)
                .unwrap();
            let mut fs = crate::fs::hfs::HfsFilesystem::open(f, offset).unwrap();
            use crate::fs::filesystem::EditableFilesystem;
            match fs.repair() {
                Ok(report) => {
                    for fix in &report.fixes_applied {
                        println!("  + {fix}");
                    }
                    for fix in &report.fixes_failed {
                        println!("  - FAILED: {fix}");
                    }
                    println!("Unrepairable: {}", report.unrepairable_count);
                }
                Err(e) => println!("Repair error: {e}"),
            }
        }

        // --- AFTER ---
        println!("\n=== AFTER REPAIR ===");
        {
            let f = BufReader::new(std::fs::File::open(work).unwrap());
            let mut fs = crate::fs::hfs::HfsFilesystem::open(f, offset).unwrap();
            let tree = format_tree_with_ids(&mut fs).unwrap();
            std::fs::write("/tmp/tree_after.txt", &tree).unwrap();
            println!("Tree saved to /tmp/tree_after.txt");
            if let Some((cnid, name)) =
                <crate::fs::hfs::HfsFilesystem<_> as crate::fs::filesystem::Filesystem>::blessed_system_folder(&mut fs)
            {
                println!("Blessed folder: {name} (CNID {cnid})");
            } else {
                println!("Blessed folder: NONE");
            }
        }

        // --- FSCK AFTER ---
        println!("\n=== FSCK AFTER ===");
        {
            let f = BufReader::new(std::fs::File::open(work).unwrap());
            let mut fs = crate::fs::hfs::HfsFilesystem::open(f, offset).unwrap();
            let result = fs.fsck().unwrap();
            print_fsck(&result);
            std::fs::write("/tmp/fsck_after.txt", format_fsck(&result)).unwrap();
        }

        println!("\n=== FILES SAVED ===");
        println!("/tmp/tree_before.txt  /tmp/tree_after.txt");
        println!("/tmp/fsck_before.txt  /tmp/fsck_after.txt");
        println!("\nRun:  diff /tmp/tree_before.txt /tmp/tree_after.txt");
    }

    fn print_fsck(r: &crate::fs::FsckResult) {
        println!(
            "Clean: {}  Files: {}  Dirs: {}",
            r.is_clean(),
            r.stats.files_checked,
            r.stats.directories_checked
        );
        for (k, v) in &r.stats.extra {
            println!("  {k}: {v}");
        }
        for e in &r.errors {
            println!(
                "  ERROR [{}] {} (repairable: {})",
                e.code, e.message, e.repairable
            );
        }
        for w in &r.warnings {
            println!("  WARN [{}] {}", w.code, w.message);
        }
        for o in &r.orphaned_entries {
            println!(
                "  ORPHAN ID:{} name:{:?} is_dir:{} missing_parent:{}",
                o.id, o.name, o.is_directory, o.missing_parent_id
            );
        }
    }

    fn format_fsck(r: &crate::fs::FsckResult) -> String {
        let mut s = format!(
            "Clean: {}\nFiles: {}  Dirs: {}\n",
            r.is_clean(),
            r.stats.files_checked,
            r.stats.directories_checked
        );
        for (k, v) in &r.stats.extra {
            s.push_str(&format!("  {k}: {v}\n"));
        }
        for e in &r.errors {
            s.push_str(&format!(
                "ERROR [{}] {} (repairable: {})\n",
                e.code, e.message, e.repairable
            ));
        }
        for w in &r.warnings {
            s.push_str(&format!("WARN [{}] {}\n", w.code, w.message));
        }
        for o in &r.orphaned_entries {
            s.push_str(&format!(
                "ORPHAN ID:{} name:{:?} is_dir:{} missing_parent:{}\n",
                o.id, o.name, o.is_directory, o.missing_parent_id
            ));
        }
        s
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
