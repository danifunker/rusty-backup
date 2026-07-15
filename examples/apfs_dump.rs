//! Developer tool: dump APFS container structure for driver bring-up.
//!
//! Usage: `cargo run --example apfs_dump -- <raw-apfs-container.img>`
//!
//! Not shipped or wired into the app — it exists to validate the
//! `src/fs/apfs.rs` parser against real macOS-formatted images while the
//! read-only browse driver is being built (see docs/apfs_support_plan.md).

use std::io::Cursor;

use rusty_backup::fs::apfs::ApfsFilesystem;
use rusty_backup::fs::filesystem::Filesystem;

fn main() {
    let path = std::env::args().nth(1).expect("usage: apfs_dump <image>");
    let bytes = std::fs::read(&path).expect("read image");
    println!("image: {} ({} bytes)", path, bytes.len());

    let mut fs = ApfsFilesystem::open(Cursor::new(bytes), 0).expect("open apfs");
    println!("summary      = {}", fs.debug_summary());
    println!("fs_type      = {}", fs.fs_type());
    println!("volume_label = {:?}", fs.volume_label());
    println!("total_size   = {}", fs.total_size());
    println!("used_size    = {}", fs.used_size());

    // Walk the tree if browsing is implemented (later phases).
    fn walk(fs: &mut dyn Filesystem, dir: &rusty_backup::fs::entry::FileEntry, depth: usize) {
        let children = match fs.list_directory(dir) {
            Ok(c) => c,
            Err(e) => {
                println!("{}<list error: {}>", "  ".repeat(depth), e);
                return;
            }
        };
        for child in children {
            let kind = if child.is_directory() {
                "DIR "
            } else if child.is_symlink() {
                "LINK"
            } else {
                "FILE"
            };
            println!(
                "{}{} {:>10}  {}",
                "  ".repeat(depth),
                kind,
                child.size,
                child.name
            );
            if child.is_directory() {
                walk(fs, &child, depth + 1);
            }
        }
    }

    match fs.root() {
        Ok(root) => {
            println!("--- tree ---");
            walk(&mut fs, &root, 0);
        }
        Err(e) => println!("root error: {e}"),
    }
}
