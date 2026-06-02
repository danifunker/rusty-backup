//! Create a regular file with `size` bytes of test data under the root of a v4
//! XFS image (short-form parents, single-extent files). Oracle-validation tool.
//!
//!   cargo run --example xfs_mkfile -- <image> <name> <size-bytes> [offset-bytes]

use std::fs::OpenOptions;
use std::io::Cursor;
use std::process::ExitCode;

use rusty_backup::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
use rusty_backup::fs::xfs::XfsFilesystem;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (Some(path), Some(name), Some(size_s)) = (
        args.first().cloned(),
        args.get(1).cloned(),
        args.get(2).cloned(),
    ) else {
        eprintln!("usage: xfs_mkfile <image> <name> <size-bytes> [offset-bytes]");
        return ExitCode::FAILURE;
    };
    let size: u64 = size_s.parse().unwrap_or(0);
    let offset: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(0);

    // Deterministic, non-trivial payload so the written data is easy to verify.
    let mut payload = vec![0u8; size as usize];
    for (i, b) in payload.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }

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
    let root = fs.root().expect("root");
    let mut data = Cursor::new(payload);
    match fs.create_file(&root, &name, &mut data, size, &CreateFileOptions::default()) {
        Ok(entry) => {
            let _ = fs.sync_metadata();
            println!("created /{name} ({size} bytes) at inode {}", entry.location);
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("create_file failed: {e}");
            ExitCode::FAILURE
        }
    }
}
