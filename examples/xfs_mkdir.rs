//! Create a directory under the root of a v4 XFS image (short-form parents
//! only). Used to oracle-validate the XFS write primitives against
//! `xfs_repair -n`.
//!
//!   cargo run --example xfs_mkdir -- <image> <name> [partition-offset-bytes]

use std::fs::OpenOptions;
use std::process::ExitCode;

use rusty_backup::fs::filesystem::{CreateDirectoryOptions, EditableFilesystem, Filesystem};
use rusty_backup::fs::xfs::XfsFilesystem;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (Some(path), Some(name)) = (args.first().cloned(), args.get(1).cloned()) else {
        eprintln!("usage: xfs_mkdir <image> <name> [partition-offset-bytes]");
        return ExitCode::FAILURE;
    };
    let offset: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);

    let file = match OpenOptions::new().read(true).write(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("cannot open {path} read-write: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut fs = match XfsFilesystem::open(file, offset) {
        Ok(fs) => fs,
        Err(e) => {
            eprintln!("not an XFS volume at offset {offset}: {e}");
            return ExitCode::FAILURE;
        }
    };
    let root = match fs.root() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("cannot read root: {e}");
            return ExitCode::FAILURE;
        }
    };
    match fs.create_directory(&root, &name, &CreateDirectoryOptions::default()) {
        Ok(entry) => {
            let _ = fs.sync_metadata();
            println!("created /{name} at inode {}", entry.location);
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("create_directory failed: {e}");
            ExitCode::FAILURE
        }
    }
}
